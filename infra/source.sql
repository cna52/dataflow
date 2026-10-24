CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS source.customers (
  customer_id BIGINT PRIMARY KEY,
  email TEXT NOT NULL,
  region TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS source.orders (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL REFERENCES source.customers(customer_id),
  ordered_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending','paid','cancelled','refunded')),
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS source.order_items (
  order_item_id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES source.orders(order_id),
  product_id BIGINT NOT NULL,
  quantity INTEGER NOT NULL,
  unit_price_cents BIGINT NOT NULL,
  currency TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS source.transactions (
  transaction_id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES source.orders(order_id),
  kind TEXT NOT NULL CHECK (kind IN ('payment','refund')),
  status TEXT NOT NULL CHECK (status IN ('pending','settled','failed')),
  amount_cents BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_customers_changed ON source.customers(updated_at, customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_changed ON source.orders(updated_at, order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_changed ON source.order_items(updated_at, order_item_id);
CREATE INDEX IF NOT EXISTS idx_transactions_changed ON source.transactions(updated_at, transaction_id);

CREATE TABLE IF NOT EXISTS control.watermarks (
  source_name TEXT PRIMARY KEY,
  updated_at TIMESTAMPTZ NOT NULL,
  last_id BIGINT NOT NULL,
  run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS control.file_versions (
  source_path TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  run_id TEXT NOT NULL,
  PRIMARY KEY (source_path, sha256)
);
CREATE TABLE IF NOT EXISTS control.runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  details JSONB NOT NULL DEFAULT '{}'::jsonb
);
