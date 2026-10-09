"""Bronze layer: the sources, as they arrived.

Rules shared by every bronze table:

- Nothing is cleaned, typed or filtered. Business columns stay strings, so a
  postal code keeps its leading zero and a decimal comma stays a comma.
  Fixing the data is the job of the silver layer.
- Every row carries where it came from and when it was loaded
  (`_source_file` or Kafka coordinates, `_ingested_at`, `_run_id`).
- Loads are idempotent: running a job twice does not duplicate rows.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

TABLES = ["customers", "products", "marketplace_orders", "order_events"]


def new_run_id() -> str:
    """Identifies one execution of a job, e.g. 20261008T211503Z-3f9a1c."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:6]}"
