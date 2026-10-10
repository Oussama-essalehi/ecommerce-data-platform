"""Fail when the documentation of the published tables has drifted from the warehouse.

Run after `dbt docs generate`. For every model of the `core` and `marts`
schemas, the columns described in the YAML files are compared with the
columns PostgreSQL really has:

- a column without a description means someone added it and did not say
  what it is;
- a described column that no longer exists means the docs promise something
  the table does not deliver.

The staging views are left out: they only rename source columns.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PUBLISHED_SCHEMAS = {"core", "marts"}


def problems(manifest: dict, catalog: dict) -> list[str]:
    found = []
    for unique_id, node in sorted(manifest["nodes"].items()):
        if node["resource_type"] != "model" or node["schema"] not in PUBLISHED_SCHEMAS:
            continue
        table = f"{node['schema']}.{node['name']}"
        if not node.get("description", "").strip():
            found.append(f"{table}: the model has no description")
        if unique_id not in catalog["nodes"]:
            found.append(f"{table}: not in the warehouse, run `dbt build` first")
            continue
        actual = {name.lower() for name in catalog["nodes"][unique_id]["columns"]}
        described = {
            name.lower() for name, column in node["columns"].items()
            if column.get("description", "").strip()
        }
        listed = {name.lower() for name in node["columns"]}
        for column in sorted(actual - described):
            found.append(f"{table}.{column}: no description")
        for column in sorted(listed - actual):
            found.append(f"{table}.{column}: described, but the table has no such column")
    return found


def main(target: Path) -> int:
    manifest = json.loads((target / "manifest.json").read_text())
    catalog = json.loads((target / "catalog.json").read_text())
    found = problems(manifest, catalog)
    for line in found:
        print(line)
    if found:
        print(f"\n{len(found)} documentation problem(s).")
        return 1
    tables = sum(
        1 for node in manifest["nodes"].values()
        if node["resource_type"] == "model" and node["schema"] in PUBLISHED_SCHEMAS
    )
    print(f"Documentation complete: every column of the {tables} published tables is described.")
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("target")))
