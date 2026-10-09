from __future__ import annotations

import json

import pytest

from lakehouse import lake
from lakehouse.bronze import api

CUSTOMERS = api.ENTITIES["customers"]
PRODUCTS = api.ENTITIES["products"]


def customer(number: int, updated_at: str, **changes) -> dict:
    record = {
        "customer_id": f"C{number:07d}",
        "first_name": "Inès",
        "last_name": "Lefèvre",
        "email": f"ines.lefevre{number}@courriel.example",
        "phone": "0639981234",
        "city": "Nice",
        "postal_code": "06000",
        "country": "FR",
        "created_at": "2026-01-05T08:00:00.000Z",
        "updated_at": updated_at,
    }
    record.update(changes)
    return record


def product(number: int, updated_at: str, **changes) -> dict:
    record = {
        "product_id": f"P{number:04d}",
        "name": "Jeu d'échecs en bois Le Fou Blanc",
        "category": "Livres & Jeux",
        "brand": "Le Fou Blanc",
        "unit_price": 48.9,
        "currency": "EUR",
        "is_active": True,
        "created_at": "2025-03-01T00:00:00.000Z",
        "updated_at": updated_at,
    }
    record.update(changes)
    return record


# --- extract: API -> landing file -------------------------------------------


def test_extract_writes_one_json_line_per_record(config, fake_api):
    records = [customer(1, "2026-10-01T10:00:00.000Z"), customer(2, "2026-10-02T10:00:00.000Z")]
    landing = config.api_landing("customers")
    path, count = api.extract(fake_api({"customers": records}), CUSTOMERS, None, landing, "run-1")

    assert count == 2 and path == landing / "customers_run-1.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == records
    assert "Inès" in lines[0]  # accents are written as-is, not as \u escapes
    assert not list(landing.glob("*.tmp"))


def test_extract_with_nothing_new_leaves_no_file(config, fake_api):
    landing = config.api_landing("customers")
    path, count = api.extract(fake_api({"customers": []}), CUSTOMERS, None, landing, "run-1")
    assert (path, count) == (None, 0)
    assert list(landing.iterdir()) == []


def test_a_failed_extraction_leaves_no_file_behind(config):
    class Broken:
        retries = 0

        def iter_records(self, entity, updated_since=None):
            yield customer(1, "2026-10-01T10:00:00.000Z")
            raise RuntimeError("API went down mid-way")

    landing = config.api_landing("customers")
    with pytest.raises(RuntimeError, match="mid-way"):
        api.extract(Broken(), CUSTOMERS, None, landing, "run-1")
    assert list(landing.iterdir()) == []


# --- load: landing file -> rows ----------------------------------------------


def _land(config, entity, records, run_id="run-1"):
    folder = config.api_landing(entity.name)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{entity.name}_{run_id}.jsonl"
    path.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records), encoding="utf-8"
    )
    return path


def test_fields_become_text_columns_and_the_raw_line_is_kept(spark, config):
    record = product(12, "2026-10-01T10:00:00.000Z", brand=None, warranty_months=24)
    path = _land(config, PRODUCTS, [record])
    df = api.read_landing(spark, str(path), PRODUCTS, "run-1")

    assert df.columns == [*PRODUCTS.columns, "_raw", "_source_file", "_ingested_at", "_run_id"]
    assert all(dict(df.dtypes)[column] == "string" for column in PRODUCTS.columns)
    row = df.first()
    assert row["product_id"] == "P0012"
    assert row["unit_price"] == "48.9"      # a number in JSON, text in bronze
    assert row["is_active"] == "true"
    assert row["brand"] is None
    assert row["name"] == "Jeu d'échecs en bois Le Fou Blanc"
    assert row["_source_file"] == "products_run-1.jsonl"
    assert row["_run_id"] == "run-1"
    # A field the table has no column for is still there, in the raw line.
    assert json.loads(row["_raw"]) == record
    assert json.loads(row["_raw"])["warranty_months"] == 24


def test_postal_codes_keep_their_leading_zero(spark, config):
    path = _land(config, CUSTOMERS, [customer(1, "2026-10-01T10:00:00.000Z")])
    assert api.read_landing(spark, str(path), CUSTOMERS, "r").first()["postal_code"] == "06000"


def test_an_unreadable_line_is_kept_with_empty_columns(spark, config):
    path = _land(config, CUSTOMERS, [customer(1, "2026-10-01T10:00:00.000Z")])
    with open(path, "a", encoding="utf-8") as handle:
        handle.write('{"customer_id": "C0000002", "first_na\n')
    rows = api.read_landing(spark, str(path), CUSTOMERS, "r").collect()
    broken = [r for r in rows if r["customer_id"] is None]
    assert len(rows) == 2 and len(broken) == 1
    assert broken[0]["_raw"] == '{"customer_id": "C0000002", "first_na'


def test_only_new_versions_drops_what_bronze_already_has(spark, config):
    old = customer(1, "2026-10-01T10:00:00.000Z")
    moved = customer(1, "2026-10-05T09:00:00.000Z", city="Lyon", postal_code="69003")
    new = customer(2, "2026-10-05T11:00:00.000Z")
    existing = api.read_landing(spark, str(_land(config, CUSTOMERS, [old], "a")), CUSTOMERS, "a")
    incoming = api.read_landing(
        spark, str(_land(config, CUSTOMERS, [old, moved, new], "b")), CUSTOMERS, "b"
    )
    kept = api.only_new_versions(incoming, existing, "customer_id").collect()
    assert sorted((r["customer_id"], r["city"]) for r in kept) == [
        ("C0000001", "Lyon"),
        ("C0000002", "Nice"),
    ]


# --- the job, against real Delta tables --------------------------------------


@pytest.mark.delta
def test_first_run_loads_everything_then_only_changes(spark, config, fake_api):
    source = fake_api(
        {
            "customers": [
                customer(1, "2026-10-01T10:00:00.000Z"),
                customer(2, "2026-10-02T10:00:00.000Z"),
            ],
            "products": [product(1, "2026-09-01T00:00:00.000Z")],
        }
    )
    first = {s["entity"]: s for s in api.run(spark, config, client=source)}
    assert first["customers"]["updated_since"] is None
    assert (first["customers"]["rows"], first["products"]["rows"]) == (2, 1)

    # Customer 1 moves, customer 3 signs up.
    source.records["customers"] += [
        customer(1, "2026-10-06T08:30:00.000Z", city="Lyon", postal_code="69003"),
        customer(3, "2026-10-06T09:00:00.000Z"),
    ]
    second = {s["entity"]: s for s in api.run(spark, config, client=source)}

    # The watermark is the newest updated_at in bronze minus the 10-minute margin.
    assert second["customers"]["updated_since"] == "2026-10-02T09:50:00.000Z"
    # Customer 2 falls inside the margin: fetched again, but not stored again.
    assert second["customers"]["extracted"] == 3
    assert second["customers"]["rows"] == 2
    assert second["customers"]["already_known"] == 1
    assert second["products"]["rows"] == 0

    table = lake.read(spark, config.table("bronze", "customers"))
    assert table.count() == 4
    versions = sorted(
        (r["updated_at"], r["city"]) for r in table.where("customer_id = 'C0000001'").collect()
    )
    assert versions == [
        ("2026-10-01T10:00:00.000Z", "Nice"),
        ("2026-10-06T08:30:00.000Z", "Lyon"),
    ]


@pytest.mark.delta
def test_running_again_with_no_change_adds_nothing(spark, config, fake_api):
    source = fake_api({"customers": [customer(1, "2026-10-01T10:00:00.000Z")], "products": []})
    api.run(spark, config, entities=["customers"], client=source)
    again = api.run(spark, config, entities=["customers"], client=source)[0]
    assert again["extracted"] == 1 and again["rows"] == 0
    assert lake.read(spark, config.table("bronze", "customers")).count() == 1


@pytest.mark.delta
def test_full_reload_does_not_duplicate(spark, config, fake_api):
    source = fake_api(
        {"customers": [customer(n, f"2026-10-0{n}T10:00:00.000Z") for n in (1, 2, 3)],
         "products": []}
    )
    api.run(spark, config, entities=["customers"], client=source)
    full = api.run(spark, config, entities=["customers"], client=source, full=True)[0]
    assert full["updated_since"] is None
    assert full["extracted"] == 3 and full["rows"] == 0
    assert lake.read(spark, config.table("bronze", "customers")).count() == 3


@pytest.mark.delta
def test_an_empty_source_creates_no_table(spark, config, fake_api):
    summary = api.run(spark, config, entities=["products"],
                      client=fake_api({"customers": [], "products": []}))[0]
    assert summary["rows"] == 0 and summary["landing_file"] is None
    assert not lake.exists(spark, config.table("bronze", "products"))
