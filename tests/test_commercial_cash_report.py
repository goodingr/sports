import csv
from pathlib import Path

import pytest

from scripts.commercial_cash_report import build_report

PRICE_ID = "price_pro_monthly"
INVOICE_FIELDS = (
    "invoice_id",
    "customer_id",
    "price_ids",
    "livemode",
    "status",
    "currency",
    "amount_paid_cents",
    "tax_cents",
)
BALANCE_FIELDS = (
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


def invoice_row(
    invoice_id: str,
    customer_id: str,
    *,
    prices: str = PRICE_ID,
    livemode: str = "true",
    amount_paid: str = "2900",
    tax: str = "0",
) -> tuple[str, ...]:
    return (invoice_id, customer_id, prices, livemode, "paid", "usd", amount_paid, tax)


def balance_row(
    transaction_id: str,
    invoice_id: str,
    charge_id: str,
    *,
    at: str = "2026-11-30 23:59:59",
    gross: str = "29.00",
    fee: str = "1.34",
    net: str = "27.66",
    category: str = "charge",
) -> tuple[str, ...]:
    return (transaction_id, at, "usd", gross, fee, net, category, invoice_id, charge_id)


def write_inputs(
    tmp_path: Path,
    invoices: list[tuple[str, ...]],
    balance: list[tuple[str, ...]],
    *,
    include_source_id: bool = False,
) -> tuple[Path, Path]:
    invoices_path = tmp_path / "invoices.csv"
    balance_path = tmp_path / "balance.csv"
    balance_fields = (*BALANCE_FIELDS, "source_id") if include_source_id else BALANCE_FIELDS
    for path, fields, rows in (
        (invoices_path, INVOICE_FIELDS, invoices),
        (balance_path, balance_fields, balance),
    ):
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(fields)
            writer.writerows(rows)
    return invoices_path, balance_path


def test_reconciles_collected_cash_and_posts_later_refund_once(tmp_path):
    charge = balance_row("txn_paid", "in_paid", "ch_paid")
    paths = write_inputs(
        tmp_path,
        invoices=[
            invoice_row("in_paid", "cus_paid"),
            invoice_row("in_renewal", "cus_paid"),
            invoice_row("in_out_of_band", "cus_oob"),
            invoice_row("in_comp", "cus_comp", amount_paid="0"),
            invoice_row("in_other", "cus_other", prices="price_other"),
            invoice_row("in_test", "cus_test", livemode="false"),
        ],
        balance=[
            charge,
            charge,
            balance_row(
                "txn_refund",
                "",
                "ch_paid",
                at="2026-12-01 00:00:00",
                gross="-10.00",
                fee="0.00",
                net="-10.00",
                category="refund",
            ),
            balance_row("txn_renewal", "in_renewal", "ch_renewal", at="2027-01-01 00:00:00"),
            balance_row("txn_other", "in_other", "ch_other"),
            balance_row("txn_test", "in_test", "ch_test"),
        ],
    )

    report = build_report(*paths, price_id=PRICE_ID)

    assert report["currency"] == "USD"
    assert report["audit"]["duplicate_balance_rows"] == 1
    assert report["audit"]["unreconciled_paid_invoices"] == 1
    assert report["months"] == {
        "2026-11": {
            "first_observed_paying_customers": 1,
            "paid_invoices": 1,
            "gross_cents": 2900,
            "refund_net_cents": 0,
            "dispute_net_cents": 0,
            "stripe_fees_cents": 134,
            "net_stripe_cash_cents": 2766,
        },
        "2026-12": {
            "first_observed_paying_customers": 0,
            "paid_invoices": 0,
            "gross_cents": 0,
            "refund_net_cents": 1000,
            "dispute_net_cents": 0,
            "stripe_fees_cents": 0,
            "net_stripe_cash_cents": -1000,
        },
        "2027-01": {
            "first_observed_paying_customers": 0,
            "paid_invoices": 1,
            "gross_cents": 2900,
            "refund_net_cents": 0,
            "dispute_net_cents": 0,
            "stripe_fees_cents": 134,
            "net_stripe_cash_cents": 2766,
        },
    }


def test_rejects_conflicting_rows_for_same_balance_transaction(tmp_path):
    charge = balance_row("txn_paid", "in_paid", "ch_paid")
    conflicting_charge = balance_row("txn_paid", "in_paid", "ch_paid", fee="1.35", net="27.65")
    paths = write_inputs(
        tmp_path, [invoice_row("in_paid", "cus_paid")], [charge, conflicting_charge]
    )

    with pytest.raises(ValueError):
        build_report(*paths, price_id=PRICE_ID)


def test_invoice_id_does_not_attach_adjustments_without_the_matched_charge(tmp_path):
    paths = write_inputs(
        tmp_path,
        invoices=[invoice_row("in_paid", "cus_paid")],
        balance=[
            balance_row("txn_paid", "in_paid", "ch_paid"),
            balance_row(
                "txn_other_charge",
                "in_paid",
                "ch_unknown",
                at="2026-12-01 00:00:00",
                gross="-10",
                fee="0",
                net="-10",
                category="refund",
            ),
            balance_row(
                "txn_missing_charge",
                "in_paid",
                "",
                at="2026-12-01 00:00:00",
                gross="-29",
                fee="15",
                net="-44",
                category="dispute",
            ),
            balance_row(
                "txn_before_charge",
                "in_paid",
                "ch_paid",
                at="2026-11-01 00:00:00",
                gross="-10",
                fee="0",
                net="-10",
                category="refund",
            ),
        ],
    )

    report = build_report(*paths, price_id=PRICE_ID)

    assert report["audit"]["unmatched_charge_adjustments"] == 3
    assert list(report["months"]) == ["2026-11"]
    assert report["months"]["2026-11"]["net_stripe_cash_cents"] == 2766


@pytest.mark.parametrize(
    ("debit", "reversal", "other_debit", "net_field"),
    [
        ("refund", "refund_failure", "dispute", "refund_net_cents"),
        ("dispute", "dispute_reversal", "refund", "dispute_net_cents"),
    ],
)
def test_reversal_credits_cannot_outspend_prior_matching_debits(
    tmp_path, debit, reversal, other_debit, net_field
):
    paths = write_inputs(
        tmp_path,
        invoices=[invoice_row("in_paid", "cus_paid")],
        balance=[
            balance_row("txn_paid", "in_paid", "ch_paid"),
            balance_row(
                "txn_oversized_reversal",
                "",
                "ch_paid",
                at="2027-01-02 00:00:00",
                gross="5",
                fee="0",
                net="5",
                category=reversal,
            ),
            balance_row(
                "txn_valid_reversal",
                "",
                "ch_paid",
                at="2027-01-01 00:00:00",
                gross="6",
                fee="-2",
                net="8",
                category=reversal,
            ),
            balance_row(
                "txn_valid_debit",
                "",
                "ch_paid",
                at="2026-12-15 00:00:00",
                gross="-10",
                fee="2",
                net="-12",
                category=debit,
            ),
            balance_row(
                "txn_wrong_category",
                "",
                "ch_paid",
                at="2026-12-01 00:00:00",
                gross="-10",
                fee="2",
                net="-12",
                category=other_debit,
            ),
            balance_row(
                "txn_early_reversal",
                "",
                "ch_paid",
                at="2026-12-02 00:00:00",
                gross="10",
                fee="0",
                net="10",
                category=reversal,
            ),
            balance_row(
                "txn_excess_fee_credit",
                "",
                "ch_paid",
                at="2027-01-03 00:00:00",
                gross="0",
                fee="-1",
                net="1",
                category=reversal,
            ),
        ],
    )

    report = build_report(*paths, price_id=PRICE_ID)

    assert report["audit"]["unreconciled_adjustment_credits"] == 3
    assert report["months"]["2026-12"][net_field] == 1000
    assert report["months"]["2026-12"]["net_stripe_cash_cents"] == -2400
    assert report["months"]["2027-01"][net_field] == -600
    assert report["months"]["2027-01"]["stripe_fees_cents"] == -200
    assert report["months"]["2027-01"]["net_stripe_cash_cents"] == 800


def test_exported_source_id_keeps_separate_disputes_on_one_charge_apart(tmp_path):
    paths = write_inputs(
        tmp_path,
        invoices=[invoice_row("in_paid", "cus_paid")],
        balance=[
            (*balance_row("txn_paid", "in_paid", "ch_paid"), "ch_paid"),
            (
                *balance_row(
                    "txn_debit",
                    "",
                    "ch_paid",
                    at="2026-12-01 00:00:00",
                    gross="-10",
                    fee="0",
                    net="-10",
                    category="dispute",
                ),
                "du_paid",
            ),
            (
                *balance_row(
                    "txn_wrong_reversal",
                    "",
                    "ch_paid",
                    at="2027-01-01 00:00:00",
                    gross="10",
                    fee="0",
                    net="10",
                    category="dispute_reversal",
                ),
                "du_other",
            ),
            (
                *balance_row(
                    "txn_reversal",
                    "",
                    "ch_paid",
                    at="2027-02-01 00:00:00",
                    gross="10",
                    fee="0",
                    net="10",
                    category="dispute_reversal",
                ),
                "du_paid",
            ),
        ],
        include_source_id=True,
    )

    report = build_report(*paths, price_id=PRICE_ID)

    assert report["audit"]["unreconciled_adjustment_credits"] == 1
    assert "2027-01" not in report["months"]
    assert report["months"]["2027-02"]["net_stripe_cash_cents"] == 1000


def test_posts_dispute_and_reversal_in_their_own_months_and_flags_unknown_adjustment(tmp_path):
    paths = write_inputs(
        tmp_path,
        invoices=[invoice_row("in_paid", "cus_paid")],
        balance=[
            balance_row("txn_paid", "in_paid", "ch_paid"),
            balance_row(
                "txn_dispute",
                "in_paid",
                "ch_paid",
                at="2026-12-10 12:00:00",
                gross="-29.00",
                fee="15.00",
                net="-44.00",
                category="dispute",
            ),
            balance_row(
                "txn_reversal",
                "in_paid",
                "ch_paid",
                at="2027-01-10 12:00:00",
                gross="29.00",
                fee="-15.00",
                net="44.00",
                category="dispute_reversal",
            ),
            balance_row(
                "txn_adjustment",
                "in_paid",
                "ch_paid",
                at="2027-01-12 12:00:00",
                gross="1.00",
                fee="0.00",
                net="1.00",
                category="adjustment",
            ),
        ],
    )

    report = build_report(*paths, price_id=PRICE_ID)

    assert report["audit"]["unsupported_linked_balance_rows"] == 1
    for month, dispute, fees, net in (
        ("2026-12", 2900, 1500, -4400),
        ("2027-01", -2900, -1500, 4400),
    ):
        assert report["months"][month] == {
            "first_observed_paying_customers": 0,
            "paid_invoices": 0,
            "gross_cents": 0,
            "refund_net_cents": 0,
            "dispute_net_cents": dispute,
            "stripe_fees_cents": fees,
            "net_stripe_cash_cents": net,
        }


def test_parses_exact_cents_and_excludes_mixed_taxed_and_internal_invoices(tmp_path):
    paths = write_inputs(
        tmp_path,
        invoices=[
            invoice_row("in_small", "cus_small", amount_paid="29"),
            invoice_row("in_taxed", "cus_taxed", amount_paid="3000", tax="100"),
            invoice_row("in_mixed", "cus_mixed", prices=f"{PRICE_ID}|price_other"),
            invoice_row("in_internal", "cus_internal"),
        ],
        balance=[
            balance_row("txn_small", "in_small", "ch_small", gross="0.29", fee="0.03", net="0.26"),
            balance_row(
                "txn_taxed", "in_taxed", "ch_taxed", gross="30.00", fee="1.37", net="28.63"
            ),
            balance_row("txn_mixed", "in_mixed", "ch_mixed"),
            balance_row("txn_internal", "in_internal", "ch_internal"),
        ],
    )

    report = build_report(*paths, price_id=PRICE_ID, exclude_customer_ids={"cus_internal"})

    assert report["audit"]["unsupported_target_invoices"] == 2
    november = report["months"]["2026-11"]
    assert november["first_observed_paying_customers"] == 1
    assert november["paid_invoices"] == 1
    assert november["gross_cents"] == 29
    assert november["stripe_fees_cents"] == 3
    assert november["net_stripe_cash_cents"] == 26
