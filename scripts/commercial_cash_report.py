"""Reconcile one USD subscription price to exported Stripe balance activity."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from collections.abc import Collection, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

_INVOICE_COLUMNS = (
    "invoice_id",
    "customer_id",
    "price_ids",
    "livemode",
    "status",
    "currency",
    "amount_paid_cents",
    "tax_cents",
)
_BALANCE_COLUMNS = (
    "balance_transaction_id",
    "created_utc",
    "currency",
    "gross",
    "fee",
    "net",
    "reporting_category",
    "invoice_id",
    "charge_id",
)
_REFUNDS = {"refund", "refund_failure"}
_DISPUTES = {"dispute", "dispute_reversal"}
_REVERSAL_DEBITS = {"refund_failure": "refund", "dispute_reversal": "dispute"}
_ACTIVITY = {"charge"} | _REFUNDS | _DISPUTES


@dataclass(frozen=True)
class Invoice:
    customer_id: str
    prices: tuple[str, ...]
    livemode: bool
    status: str
    currency: str
    amount_paid_cents: int
    tax_cents: int


@dataclass(frozen=True)
class BalanceRow:
    created_at: datetime
    category: str
    invoice_id: str
    charge_id: str
    gross: int
    fee: int
    net: int
    source_id: str


@dataclass(frozen=True)
class CashMatch:
    invoice_id: str
    invoice: Invoice
    charge: BalanceRow
    activity: tuple[BalanceRow, ...]


@dataclass
class Month:
    first_observed_paying_customers: int = 0
    paid_invoices: int = 0
    gross_cents: int = 0
    refund_net_cents: int = 0
    dispute_net_cents: int = 0
    stripe_fees_cents: int = 0
    net_stripe_cash_cents: int = 0

    def add(self, row: BalanceRow) -> None:
        if row.category == "charge":
            self.paid_invoices += 1
            self.gross_cents += row.gross
        elif row.category in _REFUNDS:
            self.refund_net_cents -= row.gross
        else:
            self.dispute_net_cents -= row.gross
        self.stripe_fees_cents += row.fee
        self.net_stripe_cash_cents += row.net


def _rows(
    path: Path, columns: tuple[str, ...], label: str, *, optional: tuple[str, ...] = ()
) -> Iterator[tuple[int, dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        fields = reader.fieldnames or []
        if len(set(fields)) != len(fields) or not set(columns).issubset(fields):
            raise ValueError(f"{label} CSV must contain {', '.join(columns)}")
        selected = columns + tuple(column for column in optional if column in fields)
        for row in reader:
            if None in row or any(row[column] is None for column in selected):
                raise ValueError(f"Malformed {label} CSV row {reader.line_num}")
            yield reader.line_num, {column: row[column].strip() for column in selected}


def _invoices(path: Path) -> dict[str, Invoice]:
    invoices: dict[str, Invoice] = {}
    for line, row in _rows(path, _INVOICE_COLUMNS, "invoice"):
        invoice_id, customer_id = row["invoice_id"], row["customer_id"]
        livemode = row["livemode"].lower()
        amount, tax = row["amount_paid_cents"], row["tax_cents"]
        if (
            not invoice_id.startswith("in_")
            or not customer_id.startswith("cus_")
            or livemode not in {"true", "false"}
            or not re.fullmatch(r"[0-9]+", amount)
            or not re.fullmatch(r"[0-9]+", tax)
        ):
            raise ValueError(f"Invalid invoice register row {line}")
        if invoice_id in invoices:
            raise ValueError(f"Duplicate invoice in register at row {line}")
        invoices[invoice_id] = Invoice(
            customer_id,
            tuple(part.strip() for part in row["price_ids"].split("|")),
            livemode == "true",
            row["status"].lower(),
            row["currency"].lower(),
            int(amount),
            int(tax),
        )
    return invoices


def _cents(value: str) -> int:
    if not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]{1,2})?", value):
        raise ValueError("Expected a decimal USD amount with at most two places")
    whole, _, fraction = value.lstrip("+-").partition(".")
    cents = int(whole) * 100 + int(fraction.ljust(2, "0"))
    return -cents if value.startswith("-") else cents


def _balance(path: Path) -> tuple[list[BalanceRow], int]:
    balance: dict[str, BalanceRow] = {}
    duplicates = 0
    for line, row in _rows(path, _BALANCE_COLUMNS, "balance", optional=("source_id",)):
        try:
            if row["currency"].lower() != "usd" or not row["balance_transaction_id"].startswith(
                "txn_"
            ):
                raise ValueError
            created = datetime.fromisoformat(row["created_utc"])
            if created.tzinfo is None:
                if ":" not in row["created_utc"]:
                    raise ValueError
                created = created.replace(tzinfo=timezone.utc)
            parsed = BalanceRow(
                created.astimezone(timezone.utc),
                row["reporting_category"].lower(),
                row["invoice_id"],
                row["charge_id"],
                _cents(row["gross"]),
                _cents(row["fee"]),
                _cents(row["net"]),
                row.get("source_id", ""),
            )
            if parsed.gross - parsed.fee != parsed.net or not parsed.category:
                raise ValueError
            if (
                parsed.category in {"charge", "refund_failure", "dispute_reversal"}
                and parsed.gross < 0
            ):
                raise ValueError
            if parsed.category in {"refund", "dispute"} and parsed.gross > 0:
                raise ValueError
        except (ValueError, OverflowError):
            raise ValueError(f"Invalid USD balance activity at CSV row {line}") from None
        transaction_id = row["balance_transaction_id"]
        previous = balance.get(transaction_id)
        if previous is None:
            balance[transaction_id] = parsed
        elif previous == parsed:
            duplicates += 1
        else:
            raise ValueError(f"Conflicting duplicate balance transaction at CSV row {line}")
    # Stripe timestamps can tie, so debit rows go first and transaction IDs break remaining ties.
    ordered = sorted(
        balance.items(),
        key=lambda item: (item[1].created_at, item[1].category in _REVERSAL_DEBITS, item[0]),
    )
    return [row for _, row in ordered], duplicates


def match_paid_invoices(
    invoices_path: Path,
    balance_path: Path,
    *,
    price_id: str,
    exclude_customer_ids: Collection[str] = (),
) -> tuple[list[CashMatch], dict[str, int]]:
    """Match eligible invoices to a single actual Stripe charge."""
    if not re.fullmatch(r"price_[A-Za-z0-9_]+", price_id):
        raise ValueError("A Stripe price ID is required")
    invoices = _invoices(invoices_path)
    balance, duplicates = _balance(balance_path)
    audit = {
        "duplicate_balance_rows": duplicates,
        "unreconciled_paid_invoices": 0,
        "unsupported_target_invoices": 0,
        "unattributed_balance_rows": 0,
        "unsupported_linked_balance_rows": 0,
        "unallocated_fee_rows": 0,
        "unmatched_charge_adjustments": 0,
        "unreconciled_adjustment_credits": 0,
    }

    charge_invoices: dict[str, str] = {}
    for row in balance:
        if row.category == "charge" and row.charge_id and row.invoice_id:
            if charge_invoices.setdefault(row.charge_id, row.invoice_id) != row.invoice_id:
                raise ValueError("Balance export links a charge to multiple invoices")

    linked: dict[str, list[BalanceRow]] = defaultdict(list)
    for row in balance:
        charge_invoice = charge_invoices.get(row.charge_id)
        if row.invoice_id and charge_invoice and row.invoice_id != charge_invoice:
            raise ValueError("Balance export has inconsistent invoice and charge links")
        invoice_id = row.invoice_id or charge_invoice
        if invoice_id in invoices:
            linked[invoice_id].append(row)
        elif row.category in _ACTIVITY:
            audit["unattributed_balance_rows"] += 1
        elif row.category == "fee":
            audit["unallocated_fee_rows"] += 1

    matches = []
    for invoice_id, invoice in invoices.items():
        if (
            not invoice.livemode
            or invoice.status != "paid"
            or invoice.amount_paid_cents <= 0
            or price_id not in invoice.prices
            or invoice.customer_id in exclude_customer_ids
        ):
            continue
        if invoice.prices != (price_id,) or invoice.tax_cents != 0 or invoice.currency != "usd":
            audit["unsupported_target_invoices"] += 1
            continue
        rows = linked[invoice_id]
        charges = [row for row in rows if row.category == "charge"]
        if len(charges) != 1 or charges[0].gross != invoice.amount_paid_cents:
            audit["unreconciled_paid_invoices"] += 1
            continue

        matches.append(CashMatch(invoice_id, invoice, charges[0], tuple(rows)))
    return matches, audit


def build_report(
    invoices_path: Path,
    balance_path: Path,
    *,
    price_id: str,
    exclude_customer_ids: Collection[str] = (),
) -> dict:
    """Measure matched cash and its first observed payers, grouped by posting month."""
    matches, audit = match_paid_invoices(
        invoices_path,
        balance_path,
        price_id=price_id,
        exclude_customer_ids=exclude_customer_ids,
    )
    months: dict[str, Month] = defaultdict(Month)
    first_paid_at: dict[str, datetime] = {}
    for match in matches:
        invoice, charge = match.invoice, match.charge
        first_paid_at[invoice.customer_id] = min(
            charge.created_at, first_paid_at.get(invoice.customer_id, charge.created_at)
        )
        months[charge.created_at.strftime("%Y-%m")].add(charge)
        unreversed: dict[tuple[str, str], tuple[int, int]] = {}
        for row in match.activity:
            if row is charge:
                continue
            if row.category not in _ACTIVITY:
                audit["unsupported_linked_balance_rows"] += 1
                continue
            if (
                not row.charge_id
                or row.charge_id != charge.charge_id
                or row.created_at < charge.created_at
            ):
                audit["unmatched_charge_adjustments"] += 1
                continue

            debit_category = _REVERSAL_DEBITS.get(row.category, row.category)
            key = (debit_category, row.source_id)
            principal, fees = unreversed.get(key, (0, 0))
            if row.gross > principal or -row.fee > fees:
                audit["unreconciled_adjustment_credits"] += 1
                continue
            unreversed[key] = (principal - row.gross, fees + row.fee)
            months[row.created_at.strftime("%Y-%m")].add(row)

    for paid_at in first_paid_at.values():
        months[paid_at.strftime("%Y-%m")].first_observed_paying_customers += 1
    return {
        "currency": "USD",
        "months": {month: vars(totals) for month, totals in sorted(months.items())},
        "audit": audit,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--invoices", type=Path, required=True, help="Normalized invoice register CSV"
    )
    parser.add_argument(
        "--balance", type=Path, required=True, help="Itemized USD Stripe Balance CSV"
    )
    parser.add_argument("--price-id", required=True)
    parser.add_argument("--exclude-customer-id", action="append", default=[])
    args = parser.parse_args()
    try:
        result = build_report(
            args.invoices,
            args.balance,
            price_id=args.price_id,
            exclude_customer_ids=args.exclude_customer_id,
        )
    except (ValueError, csv.Error) as exc:
        parser.exit(1, f"{exc}\n")
    except (OSError, UnicodeError):
        parser.exit(1, "Could not read the invoice register or balance export\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
