# Built from the repository root (see docker-compose.yml), because the image
# needs the requirement files of the Spark jobs and of dbt.
FROM python:3.12-slim-bookworm

ARG AIRFLOW_VERSION=3.3.2

# Spark runs on the JVM. procps provides `ps`, which Spark's launch scripts call.
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AIRFLOW_HOME=/opt/airflow \
    LAKEHOUSE_IVY_DIR=/opt/ivy \
    GX_ANALYTICS_ENABLED=false

# Airflow, with the constraint file its maintainers publish for this exact
# version and Python: the supported way to get a reproducible install.
RUN pip install --no-cache-dir "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

# The Spark jobs and dbt each get a virtual environment of their own. Airflow
# pins many libraries; keeping the jobs apart means their dependencies never
# have to agree with Airflow's.
COPY pipelines/requirements.txt pipelines/constraints.txt /tmp/pipelines/
RUN python -m venv /opt/venvs/lakehouse \
    && /opt/venvs/lakehouse/bin/pip install --no-cache-dir \
       -r /tmp/pipelines/requirements.txt -c /tmp/pipelines/constraints.txt

COPY warehouse/requirements.txt /tmp/warehouse/
RUN python -m venv /opt/venvs/dbt \
    && /opt/venvs/dbt/bin/pip install --no-cache-dir -r /tmp/warehouse/requirements.txt

# Download the Delta Lake and Kafka connector jars at build time (same step
# as in the pipelines image).
COPY pipelines/lakehouse/__init__.py pipelines/lakehouse/config.py pipelines/lakehouse/session.py \
     /tmp/warmup/lakehouse/
RUN cd /tmp/warmup \
    && /opt/venvs/lakehouse/bin/python -c "from lakehouse.config import Config; from lakehouse.session import build_session; build_session('warmup', Config.from_env()).stop()" \
    && rm -rf /tmp/warmup

# The project code (DAGs, Spark jobs, dbt project) is mounted by
# docker-compose under /opt/project, so edits are picked up without a rebuild.
WORKDIR /opt/airflow

# One container running the scheduler, the DAG processor, the API server and
# the web interface: right for a laptop. A production setup runs them apart.
CMD ["airflow", "standalone"]
