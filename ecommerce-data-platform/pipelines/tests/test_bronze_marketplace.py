from __future__ import annotations

from datetime import date

import pytest

from lakehouse import lake
from lakehouse.bronze import marketplace

ROW_A = "MKP-20261006-00001;06/10/2026 09:12:44;06/10/2026 09:14:02;payée;C0000042;1;P0007;2;49,90;10;0,00;CB;Nice;06000"
ROW_B = "MKP-20261006-00002;06/10/2026 10:01:10;06/10/2026 10:01:10;PAYÉE ;;1;P0011;-1;19,90;0;4,90;PayPal;Lyon;69003"
ROW_C = "MKP-20261006-00001;06/10/2026 09:12:44;07/10/2026 15:30:00;expédiée;C0000042;1;P0007;2;49,90;10;0,00;CB;Nice;06000"


# --- choosing files (no Spark needed) -------------------------------------


def test_available_files_ignores_anything_that_is_not_an_export(config, write_export):
    write_export("2026-10-06", [ROW_A])
    write_export("2026-10-07", [ROW_C])
    landing = config.marketplace_landing
    (landing / "marketplace_orders_2026-10-08.csv.tmp").write_text("partial")
    (landing / "notes.txt").write_text("hello")
    files = marketplace.available_files(landing)
    assert list(files) == [date(2026, 10, 6), date(2026, 10, 7)]
    assert files[date(2026, 10, 6)].name == "marketplace_orders_2026-10-06.csv"


def test_available_files_on_a_missing_folder(config):
    assert marketplace.available_files(config.marketplace_landing) == {}


def test_select_files_reports_days_without_a_file(config, write_export):
    write_export("2026-10-05", [ROW_A])
    write_export("2026-10-07", [ROW_A])
    available = marketplace.available_files(config.marketplace_landing)
    selected, missing = marketplace.select_files(available, date(2026, 10, 5), date(2026, 10, 8))
    assert list(selected) == [date(2026, 10, 5), date(2026, 10, 7)]
    assert missing == [date(2026, 10, 6), date(2026, 10, 8)]


def test_days_predicate():
    assert (
        marketplace.days_predicate([date(2026, 10, 6), date(2026, 10, 7)])
        == "_file_date IN (DATE'2026-10-06', DATE'2026-10-07')"
    )


# --- reading files ---------------------------------------------------------


def _read(spark, paths):
    return marketplace.add_metadata(marketplace.read_files(spark, [str(p) for p in paths]), "run-1")


def test_values_are_kept_exactly_as_in_the_file(spark, write_export):
    path = write_export("2026-10-06", [ROW_A, ROW_B])
    rows = {r["numero_commande"]: r for r in _read(spark, [path]).collect()}
    assert len(rows) == 2

    clean = rows["MKP-20261006-00001"]
    assert clean["code_postal"] == "06000"       # leading zero survives
    assert clean["prix_unitaire"] == "49,90"     # decimal comma is not interpreted
    assert clean["date_commande"] == "06/10/2026 09:12:44"
    assert clean["statut"] == "payée"
    assert clean["_corrupt_record"] is None

    dirty = rows["MKP-20261006-00002"]
    assert dirty["statut"] == "PAYÉE "           # not trimmed, not lower-cased
    assert dirty["id_client"] is None            # empty field
    assert dirty["quantite"] == "-1"             # bad value is kept for silver to judge


def test_every_business_column_is_a_string(spark, write_export):
    df = _read(spark, [write_export("2026-10-06", [ROW_A])])
    types = dict(df.dtypes)
    assert all(types[column] == "string" for column in marketplace.SOURCE_COLUMNS)
    assert types["_file_date"] == "date"
    assert types["_ingested_at"] == "timestamp"


def test_metadata_says_where_each_row_comes_from(spark, write_export):
    paths = [write_export("2026-10-06", [ROW_A, ROW_B]), write_export("2026-10-07", [ROW_C])]
    rows = _read(spark, paths).collect()
    by_file = {}
    for row in rows:
        by_file.setdefault(row["_source_file"], []).append(row)
    assert {name: len(group) for name, group in by_file.items()} == {
        "marketplace_orders_2026-10-06.csv": 2,
        "marketplace_orders_2026-10-07.csv": 1,
    }
    assert {row["_file_date"] for row in by_file["marketplace_orders_2026-10-07.csv"]} == {
        date(2026, 10, 7)
    }
    assert {row["_run_id"] for row in rows} == {"run-1"}
    assert all(row["_ingested_at"] is not None for row in rows)


def test_a_row_with_missing_fields_is_kept_and_flagged(spark, write_export):
    short = "MKP-20261006-00003;06/10/2026 11:00:00;06/10/2026 11:00:00;payée"
    path = write_export("2026-10-06", [ROW_A, short])
    rows = {r["numero_commande"]: r for r in _read(spark, [path]).collect()}
    assert rows["MKP-20261006-00001"]["_corrupt_record"] is None
    assert rows["MKP-20261006-00003"]["_corrupt_record"] == short
    assert rows["MKP-20261006-00003"]["code_postal"] is None


def test_a_renamed_column_stops_the_load(spark, write_export):
    drifted = ";".join(marketplace.SOURCE_COLUMNS).replace("id_client", "client_id")
    path = write_export("2026-10-06", [ROW_A], header=drifted)
    with pytest.raises(Exception, match="(?s)id_client.*client_id|client_id.*id_client"):
        _read(spark, [path]).collect()


def test_an_empty_file_gives_no_rows(spark, write_export):
    assert _read(spark, [write_export("2026-10-06", [])]).count() == 0


# --- the job, against a real Delta table -----------------------------------


@pytest.mark.delta
def test_loading_the_same_day_twice_does_not_duplicate(spark, config, write_export):
    write_export("2026-10-06", [ROW_A, ROW_B])
    day = date(2026, 10, 6)
    first = marketplace.run(spark, config, start=day, end=day)
    second = marketplace.run(spark, config, start=day, end=day)

    assert first["rows"] == second["rows"] == 2
    table = lake.read(spark, config.table("bronze", "marketplace_orders"))
    assert table.count() == 2
    assert {r["_run_id"] for r in table.collect()} == {second["run_id"]}


@pytest.mark.delta
def test_a_corrected_file_replaces_its_day_and_only_its_day(spark, config, write_export):
    write_export("2026-10-06", [ROW_A, ROW_B])
    write_export("2026-10-07", [ROW_C])
    marketplace.run(spark, config, all_files=True)

    write_export("2026-10-06", [ROW_A])  # the partner re-sends a corrected file
    summary = marketplace.run(spark, config, start=date(2026, 10, 6), end=date(2026, 10, 6))

    assert summary["rows"] == 1
    table = lake.read(spark, config.table("bronze", "marketplace_orders"))
    counts = {r["_file_date"]: r["count"] for r in table.groupBy("_file_date").count().collect()}
    assert counts == {date(2026, 10, 6): 1, date(2026, 10, 7): 1}


@pytest.mark.delta
def test_all_files_loads_the_whole_landing_zone(spark, config, write_export):
    write_export("2026-10-05", [ROW_A])
    write_export("2026-10-06", [ROW_A, ROW_B])
    write_export("2026-10-07", [ROW_C])
    summary = marketplace.run(spark, config, all_files=True)
    assert summary["files"] == 3 and summary["rows"] == 4
    assert (summary["first_day"], summary["last_day"]) == ("2026-10-05", "2026-10-07")


@pytest.mark.delta
def test_a_missing_day_fails_the_run_unless_told_otherwise(spark, config, write_export):
    write_export("2026-10-06", [ROW_A])
    with pytest.raises(marketplace.MissingFilesError, match="2026-10-07"):
        marketplace.run(spark, config, start=date(2026, 10, 6), end=date(2026, 10, 7))
    assert not lake.exists(spark, config.table("bronze", "marketplace_orders"))

    summary = marketplace.run(
        spark, config, start=date(2026, 10, 6), end=date(2026, 10, 7), skip_missing=True
    )
    assert summary["rows"] == 1 and summary["missing_days"] == ["2026-10-07"]


@pytest.mark.delta
def test_nothing_to_load_is_not_an_error(spark, config):
    summary = marketplace.run(spark, config, all_files=True)
    assert summary["files"] == 0 and summary["rows"] == 0
