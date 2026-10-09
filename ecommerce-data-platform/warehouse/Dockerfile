FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/usr/app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The project itself is not copied in: docker-compose mounts ./warehouse
# here, so a model edited on your machine is used by the next dbt command.
WORKDIR /usr/app

ENTRYPOINT ["dbt"]
CMD ["build"]
