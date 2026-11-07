"""Count first opt-ins and current interest in the $29 monthly offer."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Iterable, Iterator, Sequence, Set
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import dotenv_values

# Support direct invocation as well as python -m scripts.commercial_interest_report.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.data.config import _load_dotenv_if_available  # noqa: E402

OFFER_VERSION = "premium-29-usd-monthly-v1"
SOURCES = ("direct", "community", "referral", "email", "other")
METRICS = (
    "viewed", "active", "withdrawn", "never_opted_in", "first_opt_ins_in_window",
    "first_price_intents_in_window", "contact_consent_active",
)
CLERK_USERS_URL = "https://api.clerk.com/v1/users"
PAGE_SIZE = 500


class ReportError(Exception):
    """A sanitized error suitable for display without account data."""


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _cohort_timestamp(value: str) -> datetime:
    parsed = _parse_timestamp(value)
    if parsed is None:
        raise argparse.ArgumentTypeError("expected an ISO 8601 timestamp with a UTC offset")
    return parsed


def _read_clerk_key(dotenv_path: Path | None, expected_mode: str) -> str:
    try:
        if dotenv_path is None:
            _load_dotenv_if_available()
            value = os.environ.get("CLERK_SECRET_KEY")
        else:
            with dotenv_path.open(encoding="utf-8") as stream:
                values = dotenv_values(stream=stream, interpolate=False)
            value = values.get("CLERK_SECRET_KEY")
    except (OSError, ValueError):
        raise ReportError("Clerk credential configuration could not be read.") from None

    secret_key = (value or "").strip()
    if not secret_key:
        raise ReportError("CLERK_SECRET_KEY is required.")
    prefix = f"sk_{expected_mode}_"
    if not secret_key.startswith(prefix) or secret_key == prefix:
        raise ReportError(f"CLERK_SECRET_KEY does not match the expected {expected_mode} mode.")
    return secret_key


def _read_qualified_user_ids(path: Path) -> set[str]:
    try:
        user_ids = {line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines()}
    except (OSError, ValueError):
        raise ReportError("Qualified user ID file could not be read.") from None
    user_ids.discard("")
    if not user_ids or any(
        not re.fullmatch(r"user_[A-Za-z0-9_-]+", user_id) for user_id in user_ids
    ):
        raise ReportError("Qualified user ID file must contain one Clerk user ID per nonblank line.")
    return user_ids


def fetch_users(secret_key: str) -> Iterator[dict[str, Any]]:
    """Page through the Clerk REST API in ascending creation order."""
    with requests.Session() as session:
        session.headers.update(
            {"Authorization": f"Bearer {secret_key}", "Accept": "application/json"}
        )
        offset = 0
        while True:
            params: dict[str, str | int] = {
                "limit": PAGE_SIZE,
                "offset": offset,
                "order_by": "+created_at",
            }
            try:
                response = session.get(
                    CLERK_USERS_URL,
                    params=params,
                    timeout=20,
                    allow_redirects=False,
                )
                if response.status_code != 200:
                    raise ReportError(f"Clerk user request returned HTTP {response.status_code}.")
                users = response.json()
            except (requests.RequestException, ValueError):
                raise ReportError("Clerk user request or response could not be read.") from None

            if not isinstance(users, list) or len(users) > PAGE_SIZE:
                raise ReportError("Clerk returned an unexpected user list format.")
            yield from users
            if len(users) < PAGE_SIZE:
                return
            offset += len(users)


def _totals(by_source: dict[str, dict[str, int]]) -> dict[str, int]:
    return {metric: sum(row[metric] for row in by_source.values()) for metric in METRICS}


def _contact_state(
    record: dict[str, Any], joined_at: datetime | None, withdrawn_at: datetime | None
) -> tuple[datetime | None, bool]:
    # Older joins required consent, so their first join is also the first contact permission.
    if "contactConsent" not in record:
        return joined_at, joined_at is not None and withdrawn_at is None

    consent = record["contactConsent"]
    if not isinstance(consent, dict) or not isinstance(consent.get("active"), bool):
        raise ValueError("Invalid contact consent")
    active = consent["active"]
    first = _parse_timestamp(consent.get("firstGrantedAt"))
    granted = _parse_timestamp(consent.get("grantedAt"))
    if (
        (first is None and consent.get("firstGrantedAt") is not None)
        or (first is not None and (joined_at is None or first < joined_at))
        or (first is not None and withdrawn_at is not None and first > withdrawn_at)
        or (active and (withdrawn_at is not None or first is None or granted is None or granted < first))
        or (not active and consent.get("grantedAt") is not None)
    ):
        raise ValueError("Invalid contact consent")
    return first, active


def build_report(
    users: Iterable[dict[str, Any]],
    *,
    viewed_from: datetime | None = None,
    viewed_before: datetime | None = None,
    exclude_user_ids: Set[str] = frozenset(),
    qualified_user_ids: Set[str] | None = None,
) -> dict[str, Any]:
    """Aggregate price intent and contact consent for a half-open first-view cohort."""
    by_source = {source: dict.fromkeys(METRICS, 0) for source in SOURCES}
    qualified_by_source = {source: dict.fromkeys(METRICS, 0) for source in SOURCES}
    eligible_ids = qualified_user_ids - exclude_user_ids if qualified_user_ids is not None else None
    seen: set[str] = set()
    invalid_offer_records = 0
    for user in users:
        if not isinstance(user, dict) or not isinstance(user.get("id"), str) or not user["id"]:
            raise ReportError("Clerk returned a user without a valid ID.")
        user_id = user["id"]
        if user_id in exclude_user_ids or user_id in seen:
            continue
        seen.add(user_id)

        metadata = user.get("private_metadata")
        record = metadata.get("commercialPriceInterest") if isinstance(metadata, dict) else None
        if not isinstance(record, dict) or record.get("offerVersion") != OFFER_VERSION:
            continue

        viewed_at = _parse_timestamp(record.get("viewedAt"))
        if viewed_at is None:
            invalid_offer_records += 1
            continue
        if (viewed_from is not None and viewed_at < viewed_from) or (
            viewed_before is not None and viewed_at >= viewed_before
        ):
            continue

        source = record.get("source")
        joined_at = _parse_timestamp(record.get("joinedAt"))
        withdrawn_at = _parse_timestamp(record.get("withdrawnAt"))
        if (
            source not in SOURCES
            or (joined_at is None and record.get("joinedAt") is not None)
            or (joined_at is not None and joined_at < viewed_at)
            or (withdrawn_at is None and record.get("withdrawnAt") is not None)
            or (withdrawn_at is not None and (joined_at is None or withdrawn_at < joined_at))
        ):
            invalid_offer_records += 1
            continue
        try:
            first_contact, contact_active = _contact_state(record, joined_at, withdrawn_at)
        except ValueError:
            invalid_offer_records += 1
            continue

        if withdrawn_at is not None:
            state = "withdrawn"
        elif joined_at is not None:
            state = "active"
        else:
            state = "never_opted_in"
        # Valid intent and consent are at or after the cohort-eligible first view.
        counts = {
            "viewed": 1,
            state: 1,
            "first_opt_ins_in_window": int(
                first_contact is not None and (viewed_before is None or first_contact < viewed_before)
            ),
            "first_price_intents_in_window": int(
                joined_at is not None and (viewed_before is None or joined_at < viewed_before)
            ),
            "contact_consent_active": int(contact_active),
        }
        groups = [by_source[source]]
        if eligible_ids is not None and user_id in eligible_ids:
            groups.append(qualified_by_source[source])
        for group in groups:
            for metric, count in counts.items():
                group[metric] += count

    result: dict[str, Any] = {
        "offer_version": OFFER_VERSION,
        "population": "signed_in_price_offer_viewers",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "viewed_from": viewed_from.isoformat() if viewed_from else None,
        "viewed_before": viewed_before.isoformat() if viewed_before else None,
        "totals": _totals(by_source),
        "by_source": by_source,
        "invalid_offer_records": invalid_offer_records,
    }
    if eligible_ids is not None:
        result["qualified_cohort"] = {
            "eligible_roster_accounts": len(eligible_ids),
            "matched_accounts": len(seen & eligible_ids),
            "totals": _totals(qualified_by_source),
            "by_source": qualified_by_source,
        }
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dotenv", type=Path, help="read CLERK_SECRET_KEY only from this .env file"
    )
    parser.add_argument(
        "--expected-mode",
        choices=("live", "test"),
        default="live",
        help="required Clerk credential mode (default: live)",
    )
    parser.add_argument(
        "--viewed-from", type=_cohort_timestamp, help="inclusive first-view timestamp"
    )
    parser.add_argument(
        "--viewed-before", type=_cohort_timestamp, help="exclusive first-view timestamp"
    )
    parser.add_argument(
        "--qualified-user-ids-file",
        type=Path,
        help="private UTF-8 file containing one screened Clerk user ID per nonblank line",
    )
    parser.add_argument(
        "--exclude-user-id", action="append", default=[], help="omit a team or test user"
    )
    args = parser.parse_args(argv)
    if args.viewed_from and args.viewed_before and args.viewed_from >= args.viewed_before:
        parser.error("--viewed-from must be earlier than --viewed-before")

    try:
        qualified_user_ids = (
            _read_qualified_user_ids(args.qualified_user_ids_file)
            if args.qualified_user_ids_file is not None
            else None
        )
        secret_key = _read_clerk_key(args.dotenv, args.expected_mode)
        report = build_report(
            fetch_users(secret_key),
            viewed_from=args.viewed_from,
            viewed_before=args.viewed_before,
            exclude_user_ids=set(args.exclude_user_id),
            qualified_user_ids=qualified_user_ids,
        )
    except ReportError as exc:
        print(exc, file=sys.stderr)
        return 1
    report["clerk_mode"] = args.expected_mode
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
