FROM apache/airflow:3.1.7-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java \
    && python -m venv /opt/dataflow-venv \
    && chown -R airflow:0 /opt/dataflow-venv \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java
USER airflow
COPY --chown=airflow:0 pyproject.toml /opt/dataflow/pyproject.toml
COPY --chown=airflow:0 requirements.lock /opt/dataflow/requirements.lock
RUN /opt/dataflow-venv/bin/pip install --no-cache-dir -r /opt/dataflow/requirements.lock
COPY --chown=airflow:0 dataflow /opt/dataflow/dataflow
RUN /opt/dataflow-venv/bin/pip install --no-deps -e '/opt/dataflow[test]'
WORKDIR /opt/dataflow
ENV PATH=/opt/dataflow-venv/bin:${PATH}
ENV PYTHONPATH=/opt/dataflow
