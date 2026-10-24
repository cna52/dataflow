from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from dataflow.config import PG_DSN, RAW, SOURCES, TABLE_KEYS, ensure_dirs
from dataflow.util import safe_run_id, sha256, utcnow, write_json_atomic


def _extract_table(connection: psycopg.Connection, table: str, target: Path, cursor_value: tuple[datetime, int] | None) -> dict:
    key = TABLE_KEYS[table]
    lower_ts, lower_id = cursor_value or (datetime(1970, 1, 1, tzinfo=timezone.utc), 0)
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(f"SELECT max(updated_at) AS upper FROM source.{table}")
        upper = cursor.fetchone()["upper"]
        if upper is None:
            target.touch()
            return {"rows": 0, "updated_at": lower_ts.isoformat(), "last_id": lower_id}
        count = 0
        with target.open("w", encoding="utf-8") as output:
            while True:
                cursor.execute(
                    f"SELECT * FROM source.{table} WHERE (updated_at,{key}) > (%s,%s) "
                    f"AND updated_at <= %s ORDER BY updated_at,{key} LIMIT 5000",
                    (lower_ts, lower_id, upper),
                )
                rows = cursor.fetchall()
                if not rows:
                    break
                for row in rows:
                    output.write(json.dumps(row, default=str, sort_keys=True) + "\n")
                count += len(rows)
                lower_ts, lower_id = rows[-1]["updated_at"], rows[-1][key]
        return {"rows": count, "updated_at": lower_ts.isoformat(), "last_id": lower_id}


def ingest(run_id: str, full: bool = False) -> Path:
    """Write an immutable batch and advance source cursors after durable publication."""
    safe_run_id(run_id)
    ensure_dirs()
    final = RAW / run_id
    if (final / "manifest.json").exists():
        with psycopg.connect(PG_DSN) as connection:
            _commit_manifest(connection, json.loads((final / "manifest.json").read_text(encoding="utf-8")), final)
        return final / "manifest.json"
    pending = RAW / f".{run_id}.pending"
    if pending.exists():
        shutil.rmtree(pending)
    pending.mkdir(parents=True)
    manifest: dict = {"run_id": run_id, "created_at": utcnow().isoformat(), "mode": "full" if full else "incremental", "tables": {}, "files": []}
    with psycopg.connect(PG_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT source_name,updated_at,last_id FROM control.watermarks")
            watermarks = {name: (ts, row_id) for name, ts, row_id in cursor.fetchall()}
            cursor.execute("SELECT source_path,sha256 FROM control.file_versions")
            seen_files = set(cursor.fetchall())
        if not watermarks:
            full = True
            manifest["mode"] = "full"
        for table in TABLE_KEYS:
            target = pending / f"{table}.jsonl"
            result = _extract_table(connection, table, target, None if full else watermarks.get(table))
            result["sha256"] = sha256(target)
            manifest["tables"][table] = result
        for source in sorted((SOURCES / "catalog").glob("*.csv")) + sorted((SOURCES / "events").glob("*.jsonl")):
            digest = sha256(source)
            relative = str(source.relative_to(SOURCES))
            if full or (relative, digest) not in seen_files:
                target = pending / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
                manifest["files"].append({"source_path": relative, "sha256": digest, "bytes": target.stat().st_size})
        write_json_atomic(pending / "manifest.json", manifest)
        os.replace(pending, final)
        _commit_manifest(connection, manifest, final)
    return final / "manifest.json"


def _commit_manifest(connection: psycopg.Connection, manifest: dict, final: Path) -> None:
    run_id = manifest["run_id"]
    with connection.cursor() as cursor:
        for table, value in manifest["tables"].items():
            cursor.execute(
                "INSERT INTO control.watermarks VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (source_name) DO UPDATE SET updated_at=excluded.updated_at,last_id=excluded.last_id,run_id=excluded.run_id "
                "WHERE (excluded.updated_at,excluded.last_id) > (control.watermarks.updated_at,control.watermarks.last_id)",
                (table, value["updated_at"], value["last_id"], run_id),
            )
        for value in manifest["files"]:
            cursor.execute(
                "INSERT INTO control.file_versions VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (value["source_path"], value["sha256"], run_id),
            )
        cursor.execute(
            "INSERT INTO control.runs(run_id,status,details) VALUES (%s,'ingested',%s) "
            "ON CONFLICT (run_id) DO UPDATE SET status=excluded.status,details=excluded.details",
            (run_id, json.dumps({"manifest": str(final / "manifest.json")})),
        )


def manifests() -> list[Path]:
    return sorted(RAW.glob("*/manifest.json"))


def validate_raw(run_id: str) -> None:
    path = RAW / safe_run_id(run_id) / "manifest.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    base = path.parent
    for table, value in data["tables"].items():
        target = base / f"{table}.jsonl"
        if sha256(target) != value["sha256"]:
            raise ValueError(f"Raw checksum mismatch: {target}")
        with target.open(encoding="utf-8") as handle:
            if sum(1 for _ in handle) != value["rows"]:
                raise ValueError(f"Raw row count mismatch: {target}")
    for value in data["files"]:
        target = base / value["source_path"]
        if sha256(target) != value["sha256"]:
            raise ValueError(f"Raw checksum mismatch: {target}")
