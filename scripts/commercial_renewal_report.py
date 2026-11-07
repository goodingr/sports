"""Measure first and second renewals from private Stripe export evidence."""

from __future__ import annotations

import argparse
import csv
import json
from calendar import monthrange
from collections import defaultdict
from collections.abc import Callable, Collection
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.commercial_cash_report import CashMatch, _rows, match_paid_invoices

_CYCLE_COLUMNS = (
    "invoice_id",
    "subscription_id",
    "billing_reason",
    "period_start_utc",
    "period_end_utc",
    "proration",
    "renewal_1_due_utc",
    "renewal_2_due_utc",
)


@dataclass(frozen=True)
class Cycle:
    subscription_id: str
    billing_reason: str
    starts_at: datetime
    ends_at: datetime
    proration: bool
    first_due: datetime | None
    second_due: datetime | None


@dataclass
class Renewal:
    not_due: int = 0
    in_grace: int = 0
    mature: int = 0
    paid_within_grace: int = 0
    late_recovered: int = 0
    no_payment: int = 0
    rate_within_grace: float | None = None

    def observe(
        self, due: datetime, paid: datetime | None, as_of: datetime, grace: timedelta
    ) -> None:
        cutoff = due + grace
        if as_of < due:
            self.not_due += 1
        elif as_of < cutoff:
            self.in_grace += 1
        else:
            self.mature += 1
            if paid is None:
                self.no_payment += 1
            elif paid <= cutoff:
                self.paid_within_grace += 1
            else:
                self.late_recovered += 1


@dataclass
class Cohort:
    first_payers: int = 0
    renewal_1: Renewal = field(default_factory=Renewal)
    renewal_2: Renewal = field(default_factory=Renewal)


def _aware_utc(value: datetime) -> datetime:
    if value.utcoffset() is None:
        raise ValueError("Renewal timestamps must include a UTC offset")
    return value.astimezone(timezone.utc)


def _timestamp(value: str) -> datetime:
    return _aware_utc(datetime.fromisoformat(value))


def _cycles(path: Path) -> dict[str, Cycle]:
    cycles = {}
    for line, row in _rows(path, _CYCLE_COLUMNS, "invoice cycle"):
        try:
            starts, ends = _timestamp(row["period_start_utc"]), _timestamp(row["period_end_utc"])
            first, second = row["renewal_1_due_utc"], row["renewal_2_due_utc"]
            proration = row["proration"].lower()
            if (
                not row["invoice_id"].startswith("in_")
                or not row["subscription_id"].startswith("sub_")
                or starts >= ends
                or proration not in {"true", "false"}
                or not row["billing_reason"]
            ):
                raise ValueError
            cycle = Cycle(
                row["subscription_id"],
                row["billing_reason"].lower(),
                starts,
                ends,
                proration == "true",
                _timestamp(first) if first else None,
                _timestamp(second) if second else None,
            )
        except (ValueError, OverflowError):
            raise ValueError(f"Invalid invoice cycle CSV row {line}") from None
        if row["invoice_id"] in cycles:
            raise ValueError(f"Duplicate invoice in cycle register at row {line}")
        cycles[row["invoice_id"]] = cycle
    return cycles


def _monthly_boundary(anchor: datetime, months: int) -> datetime:
    year, month_index = divmod(anchor.year * 12 + anchor.month - 1 + months, 12)
    month = month_index + 1
    return anchor.replace(year=year, month=month, day=min(anchor.day, monthrange(year, month)[1]))


def _original_schedule(cycle: Cycle | None) -> tuple[datetime, datetime, datetime] | None:
    if cycle is None or cycle.proration or cycle.billing_reason != "subscription_create":
        return None
    try:
        first = _monthly_boundary(cycle.starts_at, 1)
        second = _monthly_boundary(cycle.starts_at, 2)
        third = _monthly_boundary(cycle.starts_at, 3)
    except ValueError:
        return None
    declared_due = (cycle.first_due, cycle.second_due)
    if cycle.ends_at != first or declared_due not in ((None, None), (first, second)):
        return None
    return first, second, third


def _measure_renewals(
    by_customer: dict[str, list[CashMatch]],
    cycles: dict[str, Cycle],
    *,
    as_of: datetime,
    grace: timedelta,
    cohort_for: Callable[[CashMatch], str],
) -> tuple[dict[str, Cohort], dict[str, int]]:
    """Measure original renewal slots for customers with known collected payments."""
    audit = {
        "missing_cycle_records": 0,
        "invalid_origin_schedules": 0,
        "ambiguous_first_payments": 0,
        "unsupported_linked_cycle_records": 0,
        "extra_collected_invoices_for_slot": 0,
    }

    cohorts: dict[str, Cohort] = defaultdict(Cohort)
    for payments in by_customer.values():
        payments.sort(key=lambda item: (item.charge.created_at, item.invoice_id))
        missing_cycles = sum(payment.invoice_id not in cycles for payment in payments)
        audit["missing_cycle_records"] += missing_cycles
        first = payments[0]
        cohort = cohorts[cohort_for(first)]
        cohort.first_payers += 1
        if len(payments) > 1 and payments[1].charge.created_at == first.charge.created_at:
            audit["ambiguous_first_payments"] += 1
            continue
        original = cycles.get(first.invoice_id)
        schedule = _original_schedule(original)
        if original is None or schedule is None:
            audit["invalid_origin_schedules"] += 1
            continue
        if missing_cycles:
            continue

        first_due, second_due, third_due = schedule
        slots = ((first_due, second_due), (second_due, third_due))
        slot_payments: dict[datetime, list[datetime]] = defaultdict(list)
        unsupported_slots: set[datetime] = set()
        for payment in payments[1:]:
            cycle = cycles[payment.invoice_id]
            if cycle.subscription_id != original.subscription_id:
                continue
            if (
                cycle.billing_reason == "subscription_cycle"
                and not cycle.proration
                and (cycle.starts_at, cycle.ends_at) in slots
            ):
                slot_payments[cycle.starts_at].append(payment.charge.created_at)
                continue
            overlaps = {due for due, end in slots if cycle.starts_at < end and due < cycle.ends_at}
            if overlaps:
                audit["unsupported_linked_cycle_records"] += 1
                unsupported_slots.update(overlaps)

        for (due, _), renewal in zip(slots, (cohort.renewal_1, cohort.renewal_2), strict=True):
            received = slot_payments[due]
            if len(received) > 1:
                audit["extra_collected_invoices_for_slot"] += len(received) - 1
                continue
            if due not in unsupported_slots:
                renewal.observe(due, received[0] if received else None, as_of, grace)

    return dict(cohorts), audit


def _renewals_have_evidence(audit: dict[str, int]) -> bool:
    return not (
        audit["missing_cycle_records"]
        or audit["invalid_origin_schedules"]
        or audit["ambiguous_first_payments"]
        or audit["unsupported_linked_cycle_records"]
        or audit["extra_collected_invoices_for_slot"]
    )


def build_renewal_report(
    invoices_path: Path,
    balance_path: Path,
    cycles_path: Path,
    *,
    price_id: str,
    history_start: datetime,
    as_of: datetime,
    observed_through: datetime,
    grace_days: int = 7,
    exclude_customer_ids: Collection[str] = (),
) -> dict:
    """Count elapsed original renewal slots against their verified collected charges."""
    history_start, as_of, observed_through = map(
        _aware_utc, (history_start, as_of, observed_through)
    )
    if not history_start <= as_of <= observed_through:
        raise ValueError("The observed export history must cover the as-of time")
    if observed_through > datetime.now(timezone.utc):
        raise ValueError("Observed export coverage cannot extend past the current time")
    if type(grace_days) is not int or grace_days < 0:
        raise ValueError("Retry grace days must be a nonnegative integer")
    grace = timedelta(days=grace_days)
    cycles = _cycles(cycles_path)
    matches, cash_audit = match_paid_invoices(
        invoices_path,
        balance_path,
        price_id=price_id,
        exclude_customer_ids=exclude_customer_ids,
    )
    by_customer: dict[str, list[CashMatch]] = defaultdict(list)
    for match in matches:
        paid = match.charge.created_at
        if paid < history_start:
            raise ValueError("A matched payment predates the declared history start")
        if paid <= as_of:
            by_customer[match.invoice.customer_id].append(match)

    cohorts, audit = _measure_renewals(
        by_customer, cycles, as_of=as_of, grace=grace,
        cohort_for=lambda first: first.charge.created_at.strftime("%Y-%m"),
    )
    complete = _renewals_have_evidence(audit) and not (
        cash_audit["unreconciled_paid_invoices"] or cash_audit["unsupported_target_invoices"]
    )
    if complete:
        for cohort in cohorts.values():
            for renewal in (cohort.renewal_1, cohort.renewal_2):
                if renewal.mature:
                    renewal.rate_within_grace = renewal.paid_within_grace / renewal.mature
    return {
        "status": "complete" if complete else "incomplete",
        "history_start": history_start.isoformat(),
        "as_of": as_of.isoformat(),
        "observed_through": observed_through.isoformat(),
        "retry_grace_days": grace_days,
        "cohorts": {month: asdict(cohort) for month, cohort in sorted(cohorts.items())},
        "audit": audit,
        "cash_audit": cash_audit,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invoices", type=Path, required=True)
    parser.add_argument("--balance", type=Path, required=True)
    parser.add_argument("--cycles", type=Path, required=True)
    parser.add_argument("--price-id", required=True)
    parser.add_argument("--history-start", type=_timestamp, required=True)
    parser.add_argument("--as-of", type=_timestamp, required=True)
    parser.add_argument("--observed-through", type=_timestamp, required=True)
    parser.add_argument("--grace-days", type=int, default=7)
    parser.add_argument("--exclude-customer-id", action="append", default=[])
    args = parser.parse_args()
    try:
        result = build_renewal_report(
            args.invoices,
            args.balance,
            args.cycles,
            price_id=args.price_id,
            history_start=args.history_start,
            as_of=args.as_of,
            observed_through=args.observed_through,
            grace_days=args.grace_days,
            exclude_customer_ids=args.exclude_customer_id,
        )
    except (ValueError, OverflowError, csv.Error) as exc:
        parser.exit(1, f"{exc}\n")
    except OSError:
        parser.exit(1, "Could not read the invoice, balance, or cycle export\n")
    print(json.dumps(result, indent=2))
    if result["status"] == "incomplete":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
