"""Synthetic coverage for the offline Clerk to Stripe cohort report."""

import csv
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

from scripts.commercial_interest_report import OFFER_VERSION
from scripts.commercial_pilot_conversion_report import build_conversion_report

PRICE = "price_demo_pilot"
STAMP = "T00:00:00Z"


def _write(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header.split(","))
        writer.writerows(rows)
    return path


@pytest.fixture
def private_exports(tmp_path):
    members = [
        ("community", "community", "2026-04-20"),
        ("referral", "referral", None),
        ("prior", "direct", "2026-04-22"),
        ("after", "direct", "2026-04-22"),
        ("unbound", "direct", "2026-04-22"),
        ("no_cash", "email", "2026-05-01"),
    ]
    evidence = {
        "confirm_complete_same_instance_exports": True,
        "history_start": "2026-01-01" + STAMP,
        "viewed_from": "2026-04-01" + STAMP,
        "viewed_before": "2026-05-01" + STAMP,
        "cohort_frozen_at": "2026-05-01T06:00:00Z",
        "paid_opened_at": "2026-05-02" + STAMP,
        "conversion_before": "2026-06-01" + STAMP,
        "as_of": "2026-07-10" + STAMP,
        "observed_through": "2026-07-11" + STAMP,
        "bindings_observed_at": "2026-07-12" + STAMP,
        "price_id": PRICE,
        "cohort": [
            {
                "user_id": f"user_{user}",
                "offer_version": OFFER_VERSION,
                "source": source,
                "viewed_at": "2026-04-10" + STAMP,
                "joined_at": joined + STAMP if joined else None,
            }
            for user, source, joined in members
        ],
        "clerk_bindings": [
            {"user_id": f"user_{user}", "stripe_customer_id": None if user == "unbound" else f"cus_{user}"}
            for user, _, _ in members
        ],
        "stripe_customers": [
            {
                "id": f"cus_{user}",
                "livemode": True,
                "metadata": {"clerkUserId" if user == "community" else "userId": f"user_{user}"},
            }
            for user, _, _ in members if user != "unbound"
        ],
    }
    # Collection times differ from service periods for the first renewal.
    payments = [
        ("community_origin", "community", "2026-05-02", "2026-05-02", "2026-06-02", "2026-07-02"),
        ("community_r1", "community", "2026-06-05", "2026-06-02", "2026-07-02", None),
        ("referral_origin", "referral", "2026-05-31", "2026-05-31", "2026-06-30", "2026-07-31"),
        ("prior_origin", "prior", "2026-05-01", "2026-05-01", "2026-06-01", "2026-07-01"),
        ("after_origin", "after", "2026-06-01", "2026-06-01", "2026-07-01", "2026-08-01"),
    ]
    invoices = _write(
        tmp_path / "invoices.csv",
        "invoice_id,customer_id,price_ids,livemode,status,currency,amount_paid_cents,tax_cents",
        [(f"in_{label}", f"cus_{user}", PRICE, "true", "paid", "usd", "2900", "0")
         for label, user, *_ in payments],
    )
    balance = _write(
        tmp_path / "balance.csv",
        "balance_transaction_id,created_utc,currency,gross,fee,net,reporting_category,invoice_id,charge_id",
        [(f"txn_{label}", paid + STAMP, "usd", "29.00", "1.34", "27.66", "charge", f"in_{label}", f"ch_{label}")
         for label, _, paid, *_ in payments],
    )
    cycles = _write(
        tmp_path / "cycles.csv",
        "invoice_id,subscription_id,billing_reason,period_start_utc,period_end_utc,proration,"
        "renewal_1_due_utc,renewal_2_due_utc",
        [(f"in_{label}", f"sub_{user}", "subscription_create" if second else "subscription_cycle",
          start + STAMP, end + STAMP, "false", end + STAMP if second else "", second + STAMP if second else "")
         for label, user, _, start, end, second in payments],
    )
    return evidence, invoices, balance, cycles


def test_attributes_first_cash_and_renewals_by_original_pilot_source(private_exports):
    report = build_conversion_report(*private_exports)
    assert report["status"] == "complete"
    totals = report["totals"]
    assert totals["qualified_viewers"] == 6
    assert totals["first_price_intents"] == 4  # One intent exactly at close stays out.
    assert totals["first_payers"] == 2
    assert totals["first_payers_with_price_intent"] == 1
    assert totals["viewer_to_first_payment_rate"] == pytest.approx(1 / 3)
    assert totals["intent_to_first_payment_rate"] == 0.25
    assert totals["already_paid_before_open"] == totals["first_paid_after_window"] == 1
    assert totals["no_checkout_binding"] == totals["linked_without_matched_payment"] == 1
    assert report["by_source"]["community"]["first_payers"] == 1
    assert report["by_source"]["referral"]["first_payers_with_price_intent"] == 0
    assert report["renewals"]["renewal_1"]["mature"] == 2
    assert report["renewals"]["renewal_1"]["rate_within_grace"] == 0.5
    assert report["renewals"]["renewal_2"]["no_payment"] == 1
    assert report["renewals"]["renewal_2"]["not_due"] == 1
    # The June 1 first charge is outside conversion, while the June 5 renewal
    # of a May converter still counts for the original service period.
    assert report["renewals_by_source"]["community"]["renewal_1"]["paid_within_grace"] == 1
    assert report["renewals_by_source"]["referral"]["renewal_2"]["rate_within_grace"] is None
    assert all(prefix not in json.dumps(report) for prefix in ("user_community", "cus_community", "in_community"))


@pytest.mark.parametrize(
    "problem", ["missing_clerk", "clerk_null", "missing_stripe", "test_mode", "conflicting_legacy",
                "duplicate_stripe", "duplicate_owner", "shared_clerk_customer"]
)
def test_ambiguous_or_one_sided_bindings_withhold_rates(private_exports, problem):
    evidence, *_ = private_exports
    clerk, stripe = evidence["clerk_bindings"], evidence["stripe_customers"]
    if problem == "missing_clerk":
        clerk.pop(0)
    elif problem == "clerk_null":
        clerk[0]["stripe_customer_id"] = None
    elif problem == "missing_stripe":
        stripe.pop(0)
    elif problem == "test_mode":
        stripe[0]["livemode"] = False
    elif problem == "conflicting_legacy":
        stripe[0]["metadata"]["userId"] = "user_different"
    elif problem == "duplicate_stripe":
        stripe.append(dict(stripe[0]))
    elif problem == "duplicate_owner":
        stripe.append({**stripe[0], "id": "cus_second"})
    else:
        clerk.append({"user_id": "user_different", "stripe_customer_id": "cus_community"})
    report = build_conversion_report(*private_exports)
    assert report["status"] == "incomplete"
    assert report["totals"]["unresolved_binding"] >= 1
    assert report["totals"]["viewer_to_first_payment_rate"] is None
    assert report["renewals"]["renewal_1"]["rate_within_grace"] is None


@pytest.mark.parametrize("problem", [None, "missing_clerk", "clerk_null", "shared_clerk_customer", "missing_stripe"])
def test_verifies_live_payer_outside_the_pilot_cohort(private_exports, problem):
    evidence, *_ = private_exports
    evidence["cohort"] = [row for row in evidence["cohort"] if row["user_id"] != "user_referral"]
    clerk, stripe = evidence["clerk_bindings"], evidence["stripe_customers"]
    if problem == "missing_clerk":
        clerk[:] = [row for row in clerk if row["user_id"] != "user_referral"]
    elif problem == "clerk_null":
        next(row for row in clerk if row["user_id"] == "user_referral")["stripe_customer_id"] = None
    elif problem == "shared_clerk_customer":
        clerk.append({"user_id": "user_other", "stripe_customer_id": "cus_referral"})
    elif problem == "missing_stripe":
        stripe[:] = [row for row in stripe if row["id"] != "cus_referral"]

    report = build_conversion_report(*private_exports)
    assert report["audit"]["unresolved_pilot_bindings"] == 0
    assert report["totals"]["qualified_viewers"] == 5
    assert report["totals"]["first_payers"] == 1
    if problem is None:
        assert report["status"] == "complete"
        assert report["audit"]["unverified_paid_customer_identities"] == 0
        assert report["totals"]["viewer_to_first_payment_rate"] == pytest.approx(1 / 5)
    else:
        assert report["status"] == "incomplete"
        assert report["audit"]["unverified_paid_customer_identities"] == 1
        assert report["totals"]["viewer_to_first_payment_rate"] is None
        assert report["totals"]["intent_to_first_payment_rate"] is None
        assert report["renewals"]["renewal_1"]["rate_within_grace"] is None
    output = json.dumps(report)
    private_ids = ("user_referral", "user_other", "cus_referral", "in_referral_origin", "ch_referral_origin")
    assert all(identifier not in output for identifier in private_ids)


def test_missing_cycle_withholds_only_renewal_rates(private_exports):
    _, _, _, cycles = private_exports
    lines = cycles.read_text(encoding="utf-8").splitlines()
    cycles.write_text("\n".join(line for line in lines if not line.startswith("in_community_origin,")), encoding="utf-8")
    report = build_conversion_report(*private_exports)
    assert report["status"] == "incomplete"
    assert report["conversion_complete"] is True
    assert report["totals"]["first_payers"] == 2
    assert report["totals"]["viewer_to_first_payment_rate"] == pytest.approx(1 / 3)
    assert report["renewal_audit"]["missing_cycle_records"] == 1
    assert report["renewals"]["renewal_1"]["rate_within_grace"] is None


def test_conflicting_linked_cycle_preserves_verified_pilot_conversion(private_exports):
    _, _, _, cycles = private_exports
    lines = cycles.read_text(encoding="utf-8").splitlines()
    cycles.write_text(
        "\n".join(
            line.replace("2026-07-02T00:00:00Z", "2026-07-01T00:00:00Z")
            if line.startswith("in_community_r1,") else line
            for line in lines
        ),
        encoding="utf-8",
    )
    report = build_conversion_report(*private_exports)

    assert report["status"] == "incomplete"
    assert report["conversion_complete"] is True
    assert report["totals"]["first_payers"] == 2
    assert report["totals"]["viewer_to_first_payment_rate"] == pytest.approx(1 / 3)
    assert report["renewal_complete"] is False
    assert report["renewal_audit"]["unsupported_linked_cycle_records"] == 1
    first = report["renewals_by_source"]["community"]["renewal_1"]
    assert first["mature"] == first["paid_within_grace"] == first["no_payment"] == 0
    assert first["rate_within_grace"] is None


def test_unsupported_target_cash_is_not_zero_conversion(private_exports):
    _, invoices, *_ = private_exports
    rows = invoices.read_text(encoding="utf-8").splitlines()
    rows[1] = rows[1].removesuffix(",0") + ",100"
    invoices.write_text("\n".join(rows), encoding="utf-8")
    report = build_conversion_report(*private_exports)
    assert report["cash_audit"]["unsupported_target_invoices"] == 1
    assert report["conversion_complete"] is False
    assert report["totals"]["viewer_to_first_payment_rate"] is None
    assert report["totals"]["intent_to_first_payment_rate"] is None
    assert report["renewals"]["renewal_1"]["rate_within_grace"] is None


@pytest.mark.parametrize(
    "key,value",
    [
        ("history_start", "2026-04-02T00:00:00Z"),
        ("conversion_before", "2026-08-01T00:00:00Z"),
        ("bindings_observed_at", "2026-07-01T00:00:00Z"),
        ("observed_through", (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()),
        ("confirm_complete_same_instance_exports", False),
    ],
)
def test_invalid_coverage_fails_closed(private_exports, key, value):
    evidence, *_ = private_exports
    evidence[key] = value
    with pytest.raises(ValueError):
        build_conversion_report(*private_exports)


def test_missing_export_field_cannot_mean_no_checkout(private_exports):
    evidence, *_ = private_exports
    del evidence["clerk_bindings"][4]["stripe_customer_id"]
    with pytest.raises(ValueError, match="explicit null"):
        build_conversion_report(*private_exports)


def test_cli_incomplete_is_aggregate_only(private_exports, tmp_path):
    evidence, invoices, balance, cycles = private_exports
    evidence["clerk_bindings"].pop(0)
    path = tmp_path / "private-evidence.json"
    path.write_text(json.dumps(evidence), encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-B", "-m", "scripts.commercial_pilot_conversion_report",
         "--evidence", str(path), "--invoices", str(invoices), "--balance", str(balance), "--cycles", str(cycles)],
        capture_output=True, text=True, timeout=20, check=False,
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["status"] == "incomplete"
    assert "user_community" not in result.stdout + result.stderr
