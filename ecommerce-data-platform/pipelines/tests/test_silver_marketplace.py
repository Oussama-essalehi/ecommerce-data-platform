from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from helpers import export_row, marketplace_bronze, product_ids

from lakehouse.silver import ORDER_LINE_COLUMNS, marketplace

ORDER = "MKP-20261006-00001"


def _build(spark, rows, known=("P0007", "P0011")):
    lines, rejects = marketplace.build(marketplace_bronze(spark, rows), product_ids(spark, *known))
    return lines.collect(), rejects.collect()


# --- typing ------------------------------------------------------------------------


def test_text_becomes_typed_columns(spark):
    lines, rejects = _build(spark, [export_row()])
    assert rejects == []
    (line,) = lines
    assert line["order_id"] == ORDER
    assert line["order_ts"] == datetime(2026, 10, 6, 7, 12, 44)   # 09:12 in Paris, summer time
    assert line["status"] == "paid" and line["status_ts"] == datetime(2026, 10, 6, 7, 14, 2)
    assert line["quantity"] == 2 and line["line_number"] == 1
    assert line["unit_price"] == Decimal("49.90")                  # decimal comma parsed
    assert line["discount_pct"] == 10
    assert line["shipping_fee"] == Decimal("0.00")
    assert line["payment_method"] == "card"
    assert line["shipping_postal_code"] == "06000"                 # still text, zero kept
    assert (line["source"], line["channel"]) == ("marketplace", "marketplace")
    assert line["source_total_amount"] is None


def test_winter_time_is_one_hour_from_utc(spark):
    (line,), _ = _build(spark, [export_row(
        "2026-01-15", date_commande="15/01/2026 09:12:44", date_maj="15/01/2026 09:14:02")])
    assert line["order_ts"] == datetime(2026, 1, 15, 8, 12, 44)


def test_french_labels_are_mapped_whatever_their_case_and_spacing(spark):
    (line,), rejects = _build(spark, [export_row(statut="EXPÉDIÉE ", mode_paiement=" paiement 3X")])
    assert rejects == []
    assert (line["status"], line["payment_method"]) == ("shipped", "installments")


def test_output_has_the_shared_order_line_columns(spark):
    lines, _ = marketplace.build(marketplace_bronze(spark, [export_row()]), product_ids(spark, "P0007"))
    assert lines.columns == ORDER_LINE_COLUMNS


# --- one row per line, latest status -------------------------------------------------


def test_exact_duplicate_rows_are_dropped_silently(spark):
    lines, rejects = _build(spark, [export_row(), export_row()])
    assert len(lines) == 1 and rejects == []


def test_the_latest_export_gives_the_status_and_each_status_keeps_its_date(spark):
    lines, _ = _build(spark, [
        export_row("2026-10-06", statut="payée", date_maj="06/10/2026 09:14:02"),
        export_row("2026-10-08", statut="livrée", date_maj="08/10/2026 16:45:10"),
        export_row("2026-10-07", statut="expédiée", date_maj="07/10/2026 11:30:00"),
    ])
    (line,) = lines
    assert line["status"] == "delivered"
    assert line["status_ts"] == datetime(2026, 10, 8, 14, 45, 10)
    assert line["paid_at"] == datetime(2026, 10, 6, 7, 14, 2)
    assert line["shipped_at"] == datetime(2026, 10, 7, 9, 30)
    assert line["delivered_at"] == datetime(2026, 10, 8, 14, 45, 10)
    assert line["cancelled_at"] is None and line["returned_at"] is None


def test_an_order_keeps_all_its_lines(spark):
    lines, _ = _build(spark, [
        export_row(ligne="1", ref_produit="P0007"),
        export_row(ligne="2", ref_produit="P0011", quantite="1", prix_unitaire="19,90"),
    ])
    assert sorted((r["line_number"], r["product_id"]) for r in lines) == [(1, "P0007"), (2, "P0011")]
    assert {r["status"] for r in lines} == {"paid"}


# --- defects -------------------------------------------------------------------------


def test_a_missing_customer_is_recovered_from_another_export(spark):
    lines, rejects = _build(spark, [
        export_row("2026-10-06", id_client="C0000042"),
        export_row("2026-10-07", id_client="", statut="expédiée", date_maj="07/10/2026 11:30:00"),
    ])
    assert rejects == []                       # a missing customer is not a reason to reject
    assert lines[0]["customer_id"] == "C0000042" and lines[0]["status"] == "shipped"


def test_a_customer_missing_everywhere_stays_null(spark):
    lines, rejects = _build(spark, [export_row(id_client="")])
    assert lines[0]["customer_id"] is None and rejects == []


def test_a_line_corrupted_in_the_latest_export_is_repaired_by_an_earlier_one(spark):
    lines, rejects = _build(spark, [
        export_row("2026-10-06", quantite="2"),
        export_row("2026-10-07", quantite="-1", statut="expédiée", date_maj="07/10/2026 11:30:00"),
    ])
    (line,) = lines
    assert line["quantity"] == 2               # from the valid export
    assert line["status"] == "shipped"         # yet the status is the latest one
    assert [(r["source"], r["reason"]) for r in rejects] == [("marketplace_orders", "invalid_quantity")]
    assert '"quantite":"-1"' in rejects[0]["record"]
    assert rejects[0]["source_ref"] == "marketplace_orders_2026-10-07.csv"


def test_a_line_that_is_never_valid_is_left_out_but_the_order_survives(spark):
    lines, rejects = _build(spark, [
        export_row(ligne="1", ref_produit="P0007"),
        export_row(ligne="2", ref_produit="P9123"),      # not in the catalogue
    ])
    assert [r["line_number"] for r in lines] == [1]
    assert [r["reason"] for r in rejects] == ["unknown_product"]


def test_each_kind_of_bad_row_gets_its_reason(spark):
    _, rejects = _build(spark, [
        export_row(numero_commande="MKP-1", corrupt="MKP-1;06/10/2026 11:00:00;payée"),
        export_row(numero_commande="MKP-2", date_commande="2026-10-06"),
        export_row(numero_commande="MKP-3", statut="en transit"),
        export_row(numero_commande="MKP-4", quantite="0"),
        export_row(numero_commande="MKP-5", quantite="deux"),
        export_row(numero_commande="MKP-6", ref_produit="P9999"),
        export_row(numero_commande="MKP-7", remise_pct="150"),
        export_row(numero_commande="MKP-8", prix_unitaire="gratuit"),
        export_row(numero_commande="MKP-9"),
    ])
    reasons = sorted((r["record"].split('"')[3], r["reason"]) for r in rejects)
    assert reasons == [
        ("MKP-1", "malformed_row"),
        ("MKP-2", "invalid_format"),
        ("MKP-3", "unknown_status"),
        ("MKP-4", "invalid_quantity"),
        ("MKP-5", "invalid_quantity"),
        ("MKP-6", "unknown_product"),
        ("MKP-7", "invalid_amount"),
        ("MKP-8", "invalid_amount"),
    ]
