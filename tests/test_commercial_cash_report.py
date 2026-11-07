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
) -> tuple[Path, Path]:
    invoices_path = tmp_path / "invoices.csv"
    balance_path = tmp_path / "balance.csv"
    for path, fields, rows in (
        (invoices_path, INVOICE_FIELDS, invoices),
        (balance_path, BALANCE_FIELDS, balance),
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
