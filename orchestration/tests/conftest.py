"""Point Airflow at a throwaway home before it is imported anywhere."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-tests-"))
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS_FOLDER)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

# The DAG files import their helpers as `common.*`, as they do inside Airflow.
sys.path.insert(0, str(DAGS_FOLDER))
