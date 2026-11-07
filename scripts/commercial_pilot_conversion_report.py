"""Attribute verified first payments and renewals to a frozen qualified pilot cohort."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from scripts.commercial_cash_report import CashMatch, match_paid_invoices
from scripts.commercial_interest_report import OFFER_VERSION, SOURCES
from scripts.commercial_renewal_report import (
    Cohort,
    _cycles,
    _measure_renewals,
    _renewals_have_evidence,
    _timestamp,
)

USER_ID = re.compile(r"user_[A-Za-z0-9_-]+")
CUSTOMER_ID = re.compile(r"cus_[A-Za-z0-9_-]+")
CASH_EXCEPTIONS = ("unsupported_target_invoices", "unreconciled_paid_invoices", "unattributed_balance_rows")


@dataclass(frozen=True)
class Window:
    history_start: datetime
    viewed_from: datetime
    viewed_before: datetime
    cohort_frozen_at: datetime
    paid_opened_at: datetime
    conversion_before: datetime
    as_of: datetime
    observed_through: datetime
    bindings_observed_at: datetime


@dataclass(frozen=True)
class Member:
    user_id: str
    source: str
    first_intent: bool


@dataclass(frozen=True)
class Customer:
    owners: frozenset[str]
    live: bool


@dataclass
class Conversion:
    qualified_viewers: int = 0
    first_price_intents: int = 0
    no_checkout_binding: int = 0
    unresolved_binding: int = 0
    linked_without_matched_payment: int = 0
    already_paid_before_open: int = 0
    first_paid_after_window: int = 0
    first_payers: int = 0
    first_payers_with_price_intent: int = 0


def _records(evidence: dict, name: str) -> list[dict]:
    rows = evidence.get(name)
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"Evidence must include a {name} array of objects")
    return rows


def _identifier(value: object, pattern: re.Pattern[str], *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError("Invalid identifier in the private cohort or binding evidence")
    return value


def _window(evidence: dict) -> Window:
    try:
        window = Window(*(_timestamp(evidence[field]) for field in Window.__dataclass_fields__))
    except (KeyError, TypeError, ValueError):
        raise ValueError("Evidence dates must be ISO 8601 timestamps with offsets") from None
    now = datetime.now(timezone.utc)
    if not (
        window.history_start <= window.viewed_from < window.viewed_before <= window.cohort_frozen_at
        <= window.paid_opened_at < window.conversion_before <= window.as_of
        <= window.observed_through <= now
        and window.as_of <= window.bindings_observed_at <= now
    ):
        raise ValueError("Cohort, paid window and export coverage dates are inconsistent or in the future")
    return window


def _members(evidence: dict, window: Window) -> list[Member]:
    members = []
    seen = set()
    for row in _records(evidence, "cohort"):
        user = _identifier(row.get("user_id"), USER_ID)
        source = row.get("source")
        try:
            viewed = _timestamp(row["viewed_at"])
            joined_at = row["joined_at"]
            joined = _timestamp(joined_at) if joined_at is not None else None
        except (KeyError, TypeError, ValueError):
            raise ValueError("Invalid frozen cohort date") from None
        if (
            user is None or user in seen or row.get("offer_version") != OFFER_VERSION
            or source not in SOURCES or not window.viewed_from <= viewed < window.viewed_before
            or (joined is not None and not viewed <= joined <= window.cohort_frozen_at)
        ):
            raise ValueError("Frozen cohort rows must be unique and match the declared offer and window")
        seen.add(user)
        members.append(Member(user, source, joined is not None and joined < window.viewed_before))
    return members


class Bindings:
    def __init__(self, evidence: dict) -> None:
        self.clerk: dict[str, list[str | None]] = defaultdict(list)
        self.clerk_owners: dict[str, set[str]] = defaultdict(set)
        for row in _records(evidence, "clerk_bindings"):
            user = _identifier(row.get("user_id"), USER_ID)
            if "stripe_customer_id" not in row:
                raise ValueError("Clerk binding rows require a customer ID or explicit null")
            customer = _identifier(row["stripe_customer_id"], CUSTOMER_ID, optional=True)
            assert user is not None
            self.clerk[user].append(customer)
            if customer is not None:
                self.clerk_owners[customer].add(user)

        self.stripe: dict[str, list[Customer]] = defaultdict(list)
        self.stripe_claims: dict[str, set[str]] = defaultdict(set)
        for row in _records(evidence, "stripe_customers"):
            customer = _identifier(row.get("id"), CUSTOMER_ID)
            metadata = row.get("metadata")
            if (
                not isinstance(metadata, dict) or type(row.get("livemode")) is not bool
                or type(row.get("deleted", False)) is not bool
            ):
                raise ValueError("Invalid Stripe customer export row")
            owners = frozenset(
                user for key in ("userId", "clerkUserId")
                if (user := _identifier(metadata.get(key), USER_ID, optional=True)) is not None
            )
            live = row["livemode"] and not row.get("deleted", False)
            assert customer is not None
            self.stripe[customer].append(Customer(owners, live))
            if row["livemode"]:
                for user in owners:
                    self.stripe_claims[user].add(customer)

    def live_owner(self, customer: str) -> str | None:
        rows = self.stripe.get(customer, [])
        if len(rows) == 1 and rows[0].live and len(rows[0].owners) == 1:
            return next(iter(rows[0].owners))
        return None

    def resolve(self, user: str) -> tuple[str, str | None]:
        clerk_rows = self.clerk.get(user, [])
        stripe_claims = self.stripe_claims.get(user, set())
        if len(clerk_rows) != 1:
            return "unresolved_binding", None
        customer = clerk_rows[0]
        if customer is None:
            return ("unresolved_binding" if stripe_claims else "no_checkout_binding"), None
        if (
            self.live_owner(customer) != user or stripe_claims != {customer}
            or self.clerk_owners[customer] != {user}
        ):
            return "unresolved_binding", None
        return "bound", customer


def _conversion_summary(rows: Iterable[Conversion], complete: bool) -> dict[str, Any]:
    fields = dict.fromkeys(Conversion.__dataclass_fields__, 0)
    for row in rows:
        for key, count in asdict(row).items():
            fields[key] += count
    viewers, intents = fields["qualified_viewers"], fields["first_price_intents"]
    return {
        **fields,
        "viewer_to_first_payment_rate": fields["first_payers"] / viewers if complete and viewers else None,
        "intent_to_first_payment_rate": (
            fields["first_payers_with_price_intent"] / intents if complete and intents else None
        ),
    }


def _renewal_summary(cohorts: Iterable[Cohort], complete: bool) -> dict[str, Any]:
    rows = list(cohorts)
    result: dict[str, Any] = {"first_payers": sum(row.first_payers for row in rows)}
    for slot in ("renewal_1", "renewal_2"):
        counts = dict.fromkeys(("not_due", "in_grace", "mature", "paid_within_grace", "late_recovered", "no_payment"), 0)
        for row in rows:
            renewal = getattr(row, slot)
            for key in counts:
                counts[key] += getattr(renewal, key)
        rate = counts["paid_within_grace"] / counts["mature"] if complete and counts["mature"] else None
        result[slot] = {**counts, "rate_within_grace": rate}
    return result


def build_conversion_report(evidence: dict, invoices: Path, balance: Path, cycles: Path) -> dict[str, Any]:
    if evidence.get("confirm_complete_same_instance_exports") is not True:
        raise ValueError("Confirm complete live exports from the same Clerk and Stripe instances")
    window = _window(evidence)
    grace_days = evidence.get("grace_days", 7)
    if type(grace_days) is not int or grace_days < 0:
        raise ValueError("Retry grace days must be a nonnegative integer")
    price = evidence.get("price_id")
    if not isinstance(price, str):
        raise ValueError("A Stripe price ID is required")
    members, bindings = _members(evidence, window), Bindings(evidence)
    matches, cash_audit = match_paid_invoices(invoices, balance, price_id=price)
    payments: dict[str, list[CashMatch]] = defaultdict(list)
    for match in matches:
        paid = match.charge.created_at
        if not window.history_start <= paid <= window.observed_through:
            raise ValueError("A matched payment lies outside the declared export history")
        if paid <= window.as_of:
            payments[match.invoice.customer_id].append(match)

    conversions = {source: Conversion() for source in SOURCES}
    converted_sources: dict[str, str] = {}
    for member in members:
        counts = conversions[member.source]
        counts.qualified_viewers += 1
        counts.first_price_intents += member.first_intent
        binding, customer = bindings.resolve(member.user_id)
        if binding == "unresolved_binding":
            counts.unresolved_binding += 1
        elif customer is None:
            counts.no_checkout_binding += 1
        elif customer not in payments:
            counts.linked_without_matched_payment += 1
        else:
            paid_at = min(payment.charge.created_at for payment in payments[customer])
            if paid_at < window.paid_opened_at:
                counts.already_paid_before_open += 1
            elif paid_at >= window.conversion_before:
                counts.first_paid_after_window += 1
            else:
                counts.first_payers += 1
                counts.first_payers_with_price_intent += member.first_intent
                converted_sources[customer] = member.source

    unverified_payers = 0
    for customer in payments:
        owner = bindings.live_owner(customer)
        if owner is None or bindings.resolve(owner) != ("bound", customer):
            unverified_payers += 1
    unresolved = sum(counts.unresolved_binding for counts in conversions.values())
    conversion_complete = not (unresolved or unverified_payers or any(cash_audit[key] for key in CASH_EXCEPTIONS))
    renewal_cohorts, renewal_audit = _measure_renewals(
        {customer: payments[customer] for customer in converted_sources},
        _cycles(cycles),
        as_of=window.as_of,
        grace=timedelta(days=grace_days),
        cohort_for=lambda first: converted_sources[first.invoice.customer_id],
    )
    renewal_complete = conversion_complete and _renewals_have_evidence(renewal_audit)
    return {
        "status": "complete" if renewal_complete else "incomplete",
        "offer_version": OFFER_VERSION,
        "price_id": price,
        "window": {key: value.isoformat() for key, value in asdict(window).items()},
        "retry_grace_days": grace_days,
        "conversion_complete": conversion_complete,
        "renewal_complete": renewal_complete,
        "totals": _conversion_summary(conversions.values(), conversion_complete),
        "by_source": {source: _conversion_summary([counts], conversion_complete) for source, counts in conversions.items()},
        "renewals": _renewal_summary(renewal_cohorts.values(), renewal_complete),
        "renewals_by_source": {
            source: _renewal_summary([renewal_cohorts[source]] if source in renewal_cohorts else [], renewal_complete)
            for source in SOURCES
        },
        "audit": {"unresolved_pilot_bindings": unresolved, "unverified_paid_customer_identities": unverified_payers},
        "cash_audit": cash_audit,
        "renewal_audit": renewal_audit,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--invoices", type=Path, required=True)
    parser.add_argument("--balance", type=Path, required=True)
    parser.add_argument("--cycles", type=Path, required=True)
    args = parser.parse_args()
    try:
        evidence = json.loads(args.evidence.read_text(encoding="utf-8-sig"))
        if not isinstance(evidence, dict):
            raise ValueError("Private evidence must be a JSON object")
        result = build_conversion_report(evidence, args.invoices, args.balance, args.cycles)
    except (ValueError, OverflowError, csv.Error) as exc:
        parser.exit(1, f"{exc}\n")
    except OSError:
        parser.exit(1, "Could not read the private evidence exports\n")
    print(json.dumps(result, indent=2))
    if result["status"] == "incomplete":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
