"""Daily CSV export of the marketplace partner.

One file per business day, one row per order line, containing every
marketplace order that was created *or changed status* that day. An order
therefore shows up in several files as it moves from "payée" to "livrée":
downstream, the latest `date_maj` wins.

The file looks like a typical French back-office export: semicolon
separator, decimal comma, dd/mm/yyyy dates in local time, French labels.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

from .orders import MAX_LIFECYCLE_DAYS, OrderBook
from .settings import PARIS, UTC, Settings, rng_for

COLUMNS = [
    "numero_commande", "date_commande", "date_maj", "statut", "id_client",
    "ligne", "ref_produit", "quantite", "prix_unitaire", "remise_pct",
    "frais_port", "mode_paiement", "ville_livraison", "code_postal",
]

STATUS_LABELS = {
    "created": "en attente",
    "paid": "payée",
    "shipped": "expédiée",
    "delivered": "livrée",
    "cancelled": "annulée",
    "returned": "retournée",
}

PAYMENT_LABELS = {
    "card": "CB",
    "paypal": "PayPal",
    "apple_pay": "Apple Pay",
    "installments": "Paiement 3x",
    "bank_transfer": "Virement",
}

# Injected file defects, as probabilities per row (scaled by defect_rate).
P_MISSING_CUSTOMER = 0.003
P_BAD_QUANTITY = 0.002
P_UNKNOWN_PRODUCT = 0.002
P_DIRTY_STATUS = 0.005
P_DUPLICATE_ROW = 0.005


@dataclass(frozen=True)
class ExportResult:
    day: date
    path: Path
    rows: int
    written: bool  # False when the file already existed and was left untouched


def file_name(day: date) -> str:
    return f"marketplace_orders_{day.isoformat()}.csv"


def _decimal(value: float) -> str:
    return f"{value:.2f}".replace(".", ",")


def _local(ts: datetime) -> str:
    return ts.astimezone(PARIS).strftime("%d/%m/%Y %H:%M:%S")


def rows_for_day(book: OrderBook, day: date) -> list[dict[str, str]]:
    """Rows of the export for one business day, defects included."""
    settings = book.settings
    if day < settings.start_date:
        return []
    start = datetime.combine(day, time.min, tzinfo=PARIS).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=PARIS).astimezone(UTC)
    last_instant = end - timedelta(microseconds=1)

    rows: list[dict[str, str]] = []
    first_day = max(settings.start_date, day - timedelta(days=MAX_LIFECYCLE_DAYS))
    for order in book.days(first_day, day):
        if order.channel != "marketplace" or not order.touched_between(start, end):
            continue
        status, since = order.status_at(last_instant)
        for line in order.lines:
            rows.append(
                {
                    "numero_commande": order.order_id,
                    "date_commande": _local(order.created_at),
                    "date_maj": _local(since),
                    "statut": STATUS_LABELS[status],
                    "id_client": order.customer_id,
                    "ligne": str(line.line_number),
                    "ref_produit": line.product_id,
                    "quantite": str(line.quantity),
                    "prix_unitaire": _decimal(line.unit_price),
                    "remise_pct": str(line.discount_pct),
                    "frais_port": _decimal(order.shipping_fee),
                    "mode_paiement": PAYMENT_LABELS[order.payment_method],
                    "ville_livraison": order.shipping_city,
                    "code_postal": order.shipping_postal_code,
                }
            )

    rate = settings.defect_rate
    if rate <= 0:
        return rows
    rng = rng_for(settings.seed, "csv-defects", day.isoformat())
    dirty: list[dict[str, str]] = []
    for row in rows:
        roll = rng.random()
        if roll < P_MISSING_CUSTOMER * rate:
            row["id_client"] = ""
        elif roll < (P_MISSING_CUSTOMER + P_BAD_QUANTITY) * rate:
            row["quantite"] = str(rng.choice([0, -1]))
        elif roll < (P_MISSING_CUSTOMER + P_BAD_QUANTITY + P_UNKNOWN_PRODUCT) * rate:
            row["ref_produit"] = f"P9{rng.randrange(1000):03d}"
        elif roll < (P_MISSING_CUSTOMER + P_BAD_QUANTITY + P_UNKNOWN_PRODUCT + P_DIRTY_STATUS) * rate:
            row["statut"] = row["statut"].upper() + " "
        dirty.append(row)
        if rng.random() < P_DUPLICATE_ROW * rate:
            dirty.append(dict(row))
    return dirty


def write_day(book: OrderBook, day: date, out_dir: Path, force: bool = False) -> ExportResult:
    """Write the export of one day. Existing files are kept unless `force`."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / file_name(day)
    if path.exists() and not force:
        return ExportResult(day, path, rows=0, written=False)
    rows = rows_for_day(book, day)
    # Write to a temporary name, then rename: a reader never sees a half-written file.
    tmp_path = path.with_suffix(".csv.tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, delimiter=";", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)
    return ExportResult(day, path, rows=len(rows), written=True)


def export_range(
    book: OrderBook, start: date, end: date, out_dir: Path, force: bool = False
) -> list[ExportResult]:
    """Write one file per day from start to end, both included."""
    results = []
    day = start
    while day <= end:
        results.append(write_day(book, day, out_dir, force=force))
        day += timedelta(days=1)
    return results
