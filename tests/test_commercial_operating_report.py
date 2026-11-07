import csv
import json
import sys
from datetime import datetime

import pytest

from scripts.commercial_operating_report import build_operating_report, main

MONTH = "2025-01"
PRICE = "price_test"
START = datetime.fromisoformat("2025-01-01T00:00:00Z")
END = datetime.fromisoformat("2025-02-01T00:00:00Z")
POSTED = "2025-01-14T15:00:00Z"
FIXED = ("receipt_host", POSTED, "hosting", "USD", 2400)
CHARGE = ("txn_charge", POSTED, "usd", "29.00", "1.34", "27.66", "charge", "in_sale", "ch_sale")


def write_exports(tmp_path, *, balance, costs, extra_invoices=()):
    invoices = [("in_sale", "cus_sale", PRICE, "true", "paid", "usd", "2900", "0"), *extra_invoices]
    inputs = (
        (
            "invoices",
            "invoice_id,customer_id,price_ids,livemode,status,currency,amount_paid_cents,tax_cents",
            invoices,
        ),
        (
            "balance",
            "balance_transaction_id,created_utc,currency,gross,fee,net,reporting_category,invoice_id,charge_id",
            balance,
        ),
        ("costs", "entry_id,posted_at_utc,category,currency,amount_cents", costs),
    )
    paths = []
    for name, header, rows in inputs:
        path = tmp_path / f"{name}.csv"
        with path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(header.split(","))
            writer.writerows(rows)
        paths.append(path)
    return paths


def operating_report(paths, **overrides):
    parameters = {
        "price_id": PRICE,
        "month": MONTH,
        "stripe_history_start": START,
        "stripe_observed_through": END,
        "costs_observed_from": START,
        "costs_observed_through": END,
        "confirm_dedicated_account": True,
        **overrides,
    }
    return build_operating_report(*paths, **parameters)


@pytest.mark.parametrize(
    ("fee_usd", "fee_cents", "expected"), [("-3.00", 300, 66), ("-4.00", 400, -34)]
)
def test_standalone_fee_can_flip_operating_result(tmp_path, fee_usd, fee_cents, expected):
    fee = ("txn_fee", POSTED, "usd", fee_usd, "0", fee_usd, "fee", "", "")
    paths = write_exports(
        tmp_path,
        balance=[CHARGE, fee, fee],
        costs=[FIXED, ("txn_fee", "2025-01-14T10:00:00-05:00", "stripe_fee", "USD", fee_cents)],
    )

    result = operating_report(paths)

    assert result["status"] == "complete"
    assert result["net_stripe_cash_cents"] == 2766
    assert result["attached_stripe_fees_cents"] == 134
    assert result["standalone_stripe_fees_cents"] == fee_cents
    assert result["contribution_cents"] == 2766 - fee_cents
    assert result["operating_cash_result_cents"] == expected
    assert "txn_fee" not in json.dumps(result)


def test_separates_credits_variable_fixed_and_imputed_owner_labor(tmp_path):
    paths = write_exports(
        tmp_path,
        balance=[
            CHARGE,
            ("txn_refund", POSTED, "usd", "-5.00", "0.20", "-5.20", "refund", "in_sale", "ch_sale"),
            ("txn_fee", POSTED, "usd", "-4.00", "0", "-4.00", "fee", "in_sale", ""),
            ("txn_credit", POSTED, "usd", "1.00", "0", "1.00", "fee", "", ""),
            ("txn_payout", POSTED, "usd", "-15.00", "0", "-15.00", "payout", "", ""),
        ],
        costs=[
            FIXED,
            ("txn_fee", POSTED, "stripe_fee", "USD", 400),
            ("txn_credit", POSTED, "stripe_fee", "USD", -100),
            ("campaign", POSTED, "acquisition", "USD", 100),
            ("ticket_hours", POSTED, "variable_support", "USD", 50),
            ("owner_timesheet", POSTED, "owner_labor", "USD", 500),
            ("next_month", "2025-02-01T00:00:00Z", "hosting", "USD", 500),
        ],
    )

    result = operating_report(paths)

    assert result["status"] == "complete"
    assert result["refund_net_cents"] == 500
    assert result["attached_stripe_fees_cents"] == 154
    assert result["net_stripe_cash_cents"] == 2246
    assert result["standalone_stripe_fees_cents"] == 300
    assert result["variable_costs_cents"] == 150
    assert result["paid_fixed_costs_cents"] == 2400
    assert result["contribution_cents"] == 1796
    assert result["operating_cash_result_cents"] == -604
    assert result["owner_labor_adjusted_result_cents"] == -1104


@pytest.mark.parametrize(
    ("fee_cost", "audit_key"),
    [
        (None, "missing_stripe_fee_entries"),
        (
            ("txn_fee", "2025-01-14T15:00:01Z", "stripe_fee", "USD", 300),
            "mismatched_stripe_fee_entries",
        ),
        (("txn_fee", POSTED, "stripe_fee", "USD", 299), "mismatched_stripe_fee_entries"),
        (("txn_charge", POSTED, "stripe_fee", "USD", 134), "extra_stripe_fee_entries"),
    ],
)
def test_missing_wrong_or_attached_fee_evidence_suppresses_result(tmp_path, fee_cost, audit_key):
    fee = ("txn_fee", POSTED, "usd", "-3.00", "0", "-3.00", "fee", "", "")
    costs = [FIXED, fee_cost] if fee_cost is not None else [FIXED]
    paths = write_exports(tmp_path, balance=[CHARGE, fee], costs=costs)

    result = operating_report(paths)

    assert result["status"] == "incomplete"
    assert result["audit"][audit_key] == 1
    assert result["contribution_cents"] is None
    assert result["operating_cash_result_cents"] is None


def test_cost_coverage_and_explicit_zero_paid_costs_are_required(tmp_path):
    paths = write_exports(tmp_path, balance=[CHARGE], costs=[])
    partial = operating_report(
        paths,
        costs_observed_through=datetime.fromisoformat("2025-01-31T23:59:59Z"),
        confirm_no_paid_operating_costs=True,
    )
    assert partial["audit"]["cost_coverage_incomplete"] == 1
    assert partial["operating_cash_result_cents"] is None

    no_evidence = operating_report(paths)
    assert no_evidence["audit"]["zero_paid_operating_costs_unconfirmed"] == 1
    assert no_evidence["operating_cash_result_cents"] is None
    assert (
        operating_report(paths, confirm_no_paid_operating_costs=True)["operating_cash_result_cents"]
        == 2766
    )


def test_unreconciled_invoice_and_unknown_balance_activity_suppress_result(tmp_path):
    paths = write_exports(
        tmp_path,
        balance=[
            CHARGE,
            ("txn_unknown", POSTED, "usd", "-1.00", "0", "-1.00", "adjustment", "", ""),
        ],
        costs=[FIXED],
        extra_invoices=[
            ("in_unmatched", "cus_unmatched", PRICE, "true", "paid", "usd", "2900", "0")
        ],
    )

    result = operating_report(paths)

    assert result["audit"]["unreconciled_paid_invoices"] == 1
    assert result["audit"]["unsupported_month_balance_rows"] == 1
    assert result["operating_cash_result_cents"] is None


def test_cost_ids_are_unique_and_stripe_ids_cannot_be_booked_as_expenses(tmp_path):
    with pytest.raises(ValueError, match="Duplicate operating cost ID"):
        operating_report(write_exports(tmp_path, balance=[CHARGE], costs=[FIXED, FIXED]))
    with pytest.raises(ValueError, match="Invalid USD operating cost"):
        operating_report(
            write_exports(
                tmp_path, balance=[CHARGE], costs=[("txn_charge", POSTED, "hosting", "USD", 134)]
            )
        )


def test_cli_outputs_incomplete_and_returns_nonzero_without_costs(tmp_path, monkeypatch, capsys):
    invoices, balance, costs = write_exports(tmp_path, balance=[CHARGE], costs=[])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "commercial_operating_report",
            "--invoices",
            str(invoices),
            "--balance",
            str(balance),
            "--costs",
            str(costs),
            "--price-id",
            PRICE,
            "--month",
            MONTH,
            "--confirm-dedicated-account",
            "--stripe-history-start",
            START.isoformat(),
            "--stripe-observed-through",
            END.isoformat(),
            "--costs-observed-from",
            START.isoformat(),
            "--costs-observed-through",
            END.isoformat(),
        ],
    )

    with pytest.raises(SystemExit) as exit_info:
        main()

    assert exit_info.value.code == 2
    assert json.loads(capsys.readouterr().out)["status"] == "incomplete"
