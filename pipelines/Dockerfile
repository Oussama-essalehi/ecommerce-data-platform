FROM python:3.12-slim-bookworm

# Spark runs on the JVM. procps provides `ps`, which Spark's launch scripts call.
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# GX_ANALYTICS_ENABLED=false: Great Expectations sends no usage statistics.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LAKEHOUSE_IVY_DIR=/opt/ivy \
    GX_ANALYTICS_ENABLED=false

WORKDIR /app

COPY requirements.txt requirements-dev.txt constraints.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt -c constraints.txt

# Download the Delta Lake and Kafka connector jars at build time, so jobs do
# not fetch them on every start. Only the three files the session needs are
# copied first: editing a job does not invalidate this (slow) layer.
COPY lakehouse/__init__.py lakehouse/config.py lakehouse/session.py ./lakehouse/
RUN python -c "from lakehouse.config import Config; from lakehouse.session import build_session; build_session('warmup', Config.from_env()).stop()"

COPY pytest.ini ./
COPY lakehouse ./lakehouse
COPY tests ./tests

ENTRYPOINT ["python", "-m", "lakehouse"]
CMD ["--help"]
