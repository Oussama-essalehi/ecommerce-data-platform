from __future__ import annotations

import dataclasses
import os
import time

import pytest

from lakehouse.bronze.marketplace import SOURCE_COLUMNS
from lakehouse.config import Config
from lakehouse.session import build_session

# PySpark hands timestamps back in the machine's local time. Pin it to UTC
# so the tests read the same on a laptop in Paris and on a CI runner.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

ENV = Config.from_env()
HEADER = ";".join(SOURCE_COLUMNS)


def pytest_collection_modifyitems(config, items):
    if not ENV.offline:
        return
    skip = pytest.mark.skip(reason="LAKEHOUSE_OFFLINE=1: Delta Lake jars are not loaded")
    for item in items:
        if "delta" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    config = dataclasses.replace(ENV, spark_master="local[2]", driver_memory="1g")
    session = build_session("tests", config)
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()


@pytest.fixture()
def config(tmp_path) -> Config:
    """A lake and a landing zone of their own for each test."""
    return dataclasses.replace(
        ENV,
        lake_root=tmp_path / "lake",
        landing_root=tmp_path / "landing",
        quality_root=tmp_path / "quality",
    )


@pytest.fixture()
def write_export(config):
    """Write a marketplace export file for a day into the landing zone."""

    def write(day: str, rows: list[str], header: str = HEADER):
        folder = config.marketplace_landing
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"marketplace_orders_{day}.csv"
        path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
        return path

    return write


class FakeApi:
    """Stands in for ShopApiClient: serves records from memory, filtered like the API."""

    def __init__(self, records: dict[str, list[dict]]):
        self.records = records
        self.retries = 0
        self.calls: list[tuple[str, str | None]] = []

    def iter_records(self, entity: str, updated_since: str | None = None):
        self.calls.append((entity, updated_since))
        for record in sorted(self.records[entity], key=lambda r: r["updated_at"]):
            if updated_since is None or record["updated_at"] >= updated_since:
                yield record


@pytest.fixture()
def fake_api():
    return FakeApi
