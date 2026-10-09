"""Runtime configuration, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    lake_root: Path = Path("data/lake")
    landing_root: Path = Path("data/landing")
    # Where the data-quality reports (Great Expectations) are written.
    quality_root: Path = Path("data/quality")
    api_url: str = "http://localhost:8000"
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "shop.orders.v1"
    spark_master: str = "local[*]"
    driver_memory: str = "2g"
    # Where Spark caches the jars it downloads (Delta, Kafka connector).
    ivy_dir: str | None = None
    # True: start Spark without Delta and Kafka jars. Only the unit tests of
    # the transformations work in that mode; it exists for machines that
    # cannot reach Maven Central.
    offline: bool = False
    # The PostgreSQL data warehouse that gold is loaded into.
    warehouse_host: str = "localhost"
    warehouse_port: int = 5432
    warehouse_db: str = "warehouse"
    warehouse_user: str = "warehouse"
    warehouse_password: str = "warehouse"

    @classmethod
    def from_env(cls) -> "Config":
        env = os.environ
        default = cls()
        return cls(
            lake_root=Path(env.get("LAKE_ROOT", default.lake_root)),
            landing_root=Path(env.get("LANDING_ROOT", default.landing_root)),
            quality_root=Path(env.get("QUALITY_ROOT", default.quality_root)),
            api_url=env.get("SHOP_API_URL", default.api_url),
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP_SERVERS", default.kafka_bootstrap),
            kafka_topic=env.get("KAFKA_TOPIC", default.kafka_topic),
            spark_master=env.get("SPARK_MASTER", default.spark_master),
            driver_memory=env.get("SPARK_DRIVER_MEMORY", default.driver_memory),
            ivy_dir=env.get("LAKEHOUSE_IVY_DIR") or None,
            offline=env.get("LAKEHOUSE_OFFLINE", "") not in ("", "0", "false"),
            warehouse_host=env.get("WAREHOUSE_HOST", default.warehouse_host),
            warehouse_port=int(env.get("WAREHOUSE_PORT", default.warehouse_port)),
            warehouse_db=env.get("WAREHOUSE_DB", default.warehouse_db),
            warehouse_user=env.get("WAREHOUSE_USER", default.warehouse_user),
            warehouse_password=env.get("WAREHOUSE_PASSWORD", default.warehouse_password),
        )

    def table(self, layer: str, name: str) -> str:
        """Absolute path of a Delta table, e.g. <lake>/bronze/customers."""
        return str((self.lake_root / layer / name).resolve())

    def checkpoint(self, layer: str, name: str) -> str:
        """Absolute path of a streaming checkpoint. Kept outside the table folders."""
        return str((self.lake_root / "_checkpoints" / layer / name).resolve())

    @property
    def marketplace_landing(self) -> Path:
        return (self.landing_root / "marketplace").resolve()

    def api_landing(self, entity: str) -> Path:
        return (self.landing_root / "api" / entity).resolve()
