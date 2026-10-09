from __future__ import annotations

import csv
from collections import Counter
from datetime import date, datetime, timedelta

from shop_sim.csv_export import (
    COLUMNS,
    STATUS_LABELS,
    export_range,
    file_name,
    rows_for_day,
    write_day,
)
from shop_sim.orders import MAX_LIFECYCLE_DAYS
from shop_sim.settings import PARIS

DAY = date(2026, 3, 14)


def _read(path):
    with open(path, encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def test_file_uses_the_french_export_format(clean_book, tmp_path):
    result = write_day(clean_book, DAY, tmp_path)
    assert result.written and result.path.name == "marketplace_orders_2026-03-14.csv"
    header = result.path.read_text(encoding="utf-8").splitlines()[0]
    assert header == ";".join(COLUMNS)

    rows = _read(result.path)
    assert len(rows) == result.rows > 0
    for row in rows:
        assert row["numero_commande"].startswith("MKP-")
        datetime.strptime(row["date_commande"], "%d/%m/%Y %H:%M:%S")
        datetime.strptime(row["date_maj"], "%d/%m/%Y %H:%M:%S")
        assert "," in row["prix_unitaire"] and "." not in row["prix_unitaire"]
        assert float(row["prix_unitaire"].replace(",", ".")) > 0
        assert row["statut"] in STATUS_LABELS.values()
        assert len(row["code_postal"]) == 5


def test_file_holds_orders_created_or_updated_that_day(clean_book):
    rows = rows_for_day(clean_book, DAY)
    assert all(
        datetime.strptime(r["date_maj"], "%d/%m/%Y %H:%M:%S").date() == DAY for r in rows
    )
    created_dates = {
        datetime.strptime(r["date_commande"], "%d/%m/%Y %H:%M:%S").date() for r in rows
    }
    assert DAY in created_dates
    assert min(created_dates) < DAY  # older orders that shipped or were delivered today
    # One row per order line, no repetition, in a clean file.
    keys = [(r["numero_commande"], r["ligne"]) for r in rows]
    assert len(set(keys)) == len(keys)


def test_an_order_progresses_across_daily_files(clean_book):
    statuses: dict[str, list[str]] = {}
    for offset in range(25):
        for row in rows_for_day(clean_book, DAY + timedelta(days=offset)):
            if row["numero_commande"].startswith(f"MKP-{DAY:%Y%m%d}") and row["ligne"] == "1":
                statuses.setdefault(row["numero_commande"], []).append(row["statut"])
    order_rank = list(STATUS_LABELS.values())
    assert statuses
    for history in statuses.values():
        ranks = [order_rank.index(status) for status in history]
        assert ranks == sorted(ranks)
    assert any(history[-1] == "livrée" and len(history) >= 2 for history in statuses.values())


def test_every_marketplace_order_reaches_the_files(clean_book):
    expected = {o.order_id for o in clean_book.day(DAY) if o.channel == "marketplace"}
    exported = {r["numero_commande"] for r in rows_for_day(clean_book, DAY)}
    assert expected <= exported


def test_files_report_every_status_change_up_to_the_final_one(clean_book):
    """Each order is exported on exactly the days it changed, and ends in its true status."""
    orders = {o.order_id: o for o in clean_book.day(DAY) if o.channel == "marketplace"}
    exported_on: dict[str, set[date]] = {order_id: set() for order_id in orders}
    last_status: dict[str, str] = {}
    for offset in range(MAX_LIFECYCLE_DAYS + 2):
        day = DAY + timedelta(days=offset)
        for row in rows_for_day(clean_book, day):
            if row["numero_commande"] in orders:
                exported_on[row["numero_commande"]].add(day)
                last_status[row["numero_commande"]] = row["statut"]

    for order_id, order in orders.items():
        changes = [order.created_at] + [change.ts for change in order.lifecycle]
        assert exported_on[order_id] == {ts.astimezone(PARIS).date() for ts in changes}
        final = order.lifecycle[-1].status if order.lifecycle else "created"
        assert last_status[order_id] == STATUS_LABELS[final]
    assert any(len(order.lifecycle) == 4 for order in orders.values())  # a returned order


def test_export_is_idempotent_and_does_not_overwrite(book, tmp_path):
    first = export_range(book, DAY, DAY + timedelta(days=2), tmp_path)
    assert [r.written for r in first] == [True, True, True]
    snapshot = {r.path.name: r.path.read_bytes() for r in first}

    second = export_range(book, DAY, DAY + timedelta(days=2), tmp_path)
    assert [r.written for r in second] == [False, False, False]

    forced = export_range(book, DAY, DAY + timedelta(days=2), tmp_path, force=True)
    assert {r.path.name: r.path.read_bytes() for r in forced} == snapshot
    assert not list(tmp_path.glob("*.tmp"))
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        file_name(DAY + timedelta(days=i)) for i in range(3)
    ]


def test_defects_are_injected_in_the_files(book, clean_book, world):
    dirty, clean = [], []
    for offset in range(30):
        dirty.extend(rows_for_day(book, DAY + timedelta(days=offset)))
        clean.extend(rows_for_day(clean_book, DAY + timedelta(days=offset)))

    n = len(clean)
    assert n > 2_000
    counts = Counter()
    for row in dirty:
        if row["id_client"] == "":
            counts["missing_customer"] += 1
        if int(row["quantite"]) <= 0:
            counts["bad_quantity"] += 1
        if row["ref_produit"] not in world.products_by_id:
            counts["unknown_product"] += 1
        if row["statut"] not in STATUS_LABELS.values():
            counts["dirty_status"] += 1
            assert row["statut"].strip().lower() in STATUS_LABELS.values()
    counts["duplicate_rows"] = len(dirty) - n

    assert set(counts) == {
        "missing_customer", "bad_quantity", "unknown_product", "dirty_status", "duplicate_rows",
    }
    assert all(0 < value < 0.02 * n for value in counts.values()), counts
