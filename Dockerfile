FROM apache/airflow:2.10.4-python3.12

USER root
RUN mkdir -p /opt/sentinel/data /opt/sentinel/reports && chown -R airflow:root /opt/sentinel
USER airflow

RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres==1.8.2"

COPY --chown=airflow:root pyproject.toml README.md /opt/sentinel/
COPY --chown=airflow:root src /opt/sentinel/src
RUN pip install --no-cache-dir "/opt/sentinel" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"

COPY --chown=airflow:root sql /opt/sentinel/sql
COPY --chown=airflow:root dbt /opt/sentinel/dbt
COPY --chown=airflow:root dags /opt/airflow/dags

ENV SENTINEL_DBT_EXECUTABLE=/opt/airflow/dbt-venv/bin/dbt \
    SENTINEL_SCHEMA_PATH=/opt/sentinel/sql/schema.sql \
    SENTINEL_DBT_PROJECT_DIR=/opt/sentinel/dbt \
    SENTINEL_LANDING_DIR=/opt/sentinel/data/landing \
    SENTINEL_STATE_DIR=/opt/sentinel/data/state \
    SENTINEL_REPORTS_DIR=/opt/sentinel/reports
