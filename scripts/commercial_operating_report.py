"""Reconcile one complete USD posting month to documented operating costs."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.commercial_cash_report import (
    _ACTIVITY,
    BalanceRow,
    Month,
    _load_cash_matches,
    _reconciled_activity,
    _rows,
)

_COST_COLUMNS = ("entry_id", "posted_at_utc", "category", "currency", "amount_cents")
_VARIABLE_COSTS = ("acquisition", "variable_support")
_FIXED_COSTS = (
    "hosting",
    "odds_data",
    "auth",
    "backups",
    "domains",
    "monitoring",
    "fixed_labor",
    "other_fixed",
)
_COST_CATEGORIES = {*_VARIABLE_COSTS, *_FIXED_COSTS, "stripe_fee", "owner_labor"}
_PAYOUT_TRANSFERS = {"payout", "payout_reversal"}


@dataclass(frozen=True)
class Cost:
    posted_at: datetime
    category: str
    amount_cents: int


def _utc(value: datetime) -> datetime:
    if value.utcoffset() is None:
        raise ValueError("Operating report timestamps must include a UTC offset")
    return value.astimezone(timezone.utc)


def _timestamp(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value))


def _month_bounds(month: str) -> tuple[datetime, datetime]:
    if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("Operating report month must be YYYY-MM")
    start = datetime.fromisoformat(f"{month}-01T00:00:00Z")
    return start, (start + timedelta(days=32)).replace(day=1)


def _costs(path: Path) -> dict[str, Cost]:
    costs: dict[str, Cost] = {}
    for line, row in _rows(path, _COST_COLUMNS, "operating cost"):
        entry_id, category = row["entry_id"], row["category"]
        try:
            if (
                not entry_id
                or category not in _COST_CATEGORIES
                or row["currency"].lower() != "usd"
                or not re.fullmatch(r"[+-]?[0-9]+", row["amount_cents"])
                or entry_id.startswith("txn_") != (category == "stripe_fee")
            ):
                raise ValueError
            cost = Cost(_timestamp(row["posted_at_utc"]), category, int(row["amount_cents"]))
        except (ValueError, OverflowError):
            raise ValueError(f"Invalid USD operating cost at CSV row {line}") from None
        if entry_id in costs:
            raise ValueError(f"Duplicate operating cost ID at CSV row {line}")
        costs[entry_id] = cost
    return costs


def build_operating_report(
    invoices_path: Path,
    balance_path: Path,
    costs_path: Path,
    *,
    price_id: str,
    month: str,
    stripe_history_start: datetime,
    stripe_observed_through: datetime,
    costs_observed_from: datetime,
    costs_observed_through: datetime,
    confirm_dedicated_account: bool = False,
    confirm_no_paid_operating_costs: bool = False,
) -> dict:
    """Return cash-based totals only after both source scopes reconcile."""
    start, end = _month_bounds(month)
    stripe_start, stripe_end, cost_start, cost_end = map(
        _utc,
        (
            stripe_history_start,
            stripe_observed_through,
            costs_observed_from,
            costs_observed_through,
        ),
    )
    if stripe_start > stripe_end or cost_start > cost_end:
        raise ValueError("Source coverage start must precede its end")
    now = datetime.now(timezone.utc)
    matches, balance, cash_audit = _load_cash_matches(
        invoices_path, balance_path, price_id=price_id
    )
    costs = _costs(costs_path)

    totals = Month()
    accounted: set[str] = set()
    for match in matches:
        for row in _reconciled_activity(match, cash_audit):
            if start <= row.created_at < end:
                totals.add(row)
                accounted.add(row.transaction_id)

    audit = {
        "dedicated_account_unconfirmed": int(not confirm_dedicated_account),
        "stripe_coverage_incomplete": int(not stripe_start <= start < end <= stripe_end <= now),
        "cost_coverage_incomplete": int(not cost_start <= start < end <= cost_end <= now),
        "unreconciled_paid_invoices": cash_audit["unreconciled_paid_invoices"],
        "unsupported_target_invoices": cash_audit["unsupported_target_invoices"],
        "unreconciled_month_cash_rows": 0,
        "unsupported_month_balance_rows": 0,
        "missing_stripe_fee_entries": 0,
        "mismatched_stripe_fee_entries": 0,
    }
    fees: dict[str, BalanceRow] = {}
    for row in balance:
        if not start <= row.created_at < end or row.transaction_id in accounted:
            continue
        if row.category == "fee":
            fees[row.transaction_id] = row
        elif not (row.gross or row.fee or row.net):
            continue
        elif row.category in _ACTIVITY:
            audit["unreconciled_month_cash_rows"] += 1
        elif not (
            row.category in _PAYOUT_TRANSFERS and not (row.fee or row.invoice_id or row.charge_id)
        ):
            audit["unsupported_month_balance_rows"] += 1

    standalone_fees = 0
    for transaction_id, row in fees.items():
        cost = costs.get(transaction_id)
        if cost is None:
            audit["missing_stripe_fee_entries"] += 1
        elif (cost.category, cost.posted_at, cost.amount_cents) != (
            "stripe_fee",
            row.created_at,
            -row.net,
        ):
            audit["mismatched_stripe_fee_entries"] += 1
        else:
            standalone_fees += cost.amount_cents

    month_costs: dict[str, int] = defaultdict(int)
    has_paid_cost_entries = False
    audit["extra_stripe_fee_entries"] = 0
    for entry_id, cost in costs.items():
        if not start <= cost.posted_at < end:
            continue
        if cost.category == "stripe_fee":
            audit["extra_stripe_fee_entries"] += int(entry_id not in fees)
        else:
            month_costs[cost.category] += cost.amount_cents
            has_paid_cost_entries = has_paid_cost_entries or (
                cost.category != "owner_labor" and cost.amount_cents != 0
            )

    audit["zero_paid_operating_costs_unconfirmed"] = int(
        not has_paid_cost_entries and not confirm_no_paid_operating_costs
    )
    variable_costs = sum(month_costs.get(category, 0) for category in _VARIABLE_COSTS)
    fixed_costs = sum(month_costs.get(category, 0) for category in _FIXED_COSTS)
    owner_labor = month_costs.get("owner_labor")
    complete = not any(audit.values())
    contribution = (
        totals.net_stripe_cash_cents - standalone_fees - variable_costs if complete else None
    )
    operating_result = contribution - fixed_costs if contribution is not None else None
    return {
        "status": "complete" if complete else "incomplete",
        "currency": "USD",
        "month_utc": month,
        "price_id": price_id,
        "stripe_history_start": stripe_start.isoformat(),
        "stripe_observed_through": stripe_end.isoformat(),
        "costs_observed_from": cost_start.isoformat(),
        "costs_observed_through": cost_end.isoformat(),
        "gross_collected_cents": totals.gross_cents,
        "refund_net_cents": totals.refund_net_cents,
        "dispute_net_cents": totals.dispute_net_cents,
        "attached_stripe_fees_cents": totals.stripe_fees_cents,
        "net_stripe_cash_cents": totals.net_stripe_cash_cents,
        "standalone_stripe_fees_cents": standalone_fees,
        "variable_costs_cents": variable_costs,
        "paid_fixed_costs_cents": fixed_costs,
        "owner_labor_cents": owner_labor,
        "contribution_cents": contribution,
        "operating_cash_result_cents": operating_result,
        "owner_labor_adjusted_result_cents": (
            operating_result - owner_labor
            if operating_result is not None and owner_labor is not None
            else None
        ),
        "audit": audit,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invoices", dest="invoices_path", type=Path, required=True)
    parser.add_argument("--balance", dest="balance_path", type=Path, required=True)
    parser.add_argument("--costs", dest="costs_path", type=Path, required=True)
    parser.add_argument("--price-id", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--stripe-history-start", type=_timestamp, required=True)
    parser.add_argument("--stripe-observed-through", type=_timestamp, required=True)
    parser.add_argument("--costs-observed-from", type=_timestamp, required=True)
    parser.add_argument("--costs-observed-through", type=_timestamp, required=True)
    parser.add_argument("--confirm-dedicated-account", action="store_true")
    parser.add_argument("--confirm-no-paid-operating-costs", action="store_true")
    args = parser.parse_args()
    try:
        report = build_operating_report(**vars(args))
    except (ValueError, OverflowError, csv.Error) as exc:
        parser.exit(1, f"{exc}\n")
    except OSError:
        parser.exit(1, "Could not read the invoice, balance, or operating cost export\n")
    print(json.dumps(report, indent=2))
    if report["status"] == "incomplete":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
