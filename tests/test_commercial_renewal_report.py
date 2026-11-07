import csv
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from scripts import commercial_renewal_report
from scripts.commercial_renewal_report import Cycle, _original_schedule, build_renewal_report

PRICE_ID = "price_pro_monthly"
HISTORY_START = datetime.fromisoformat("2027-01-01T00:00:00Z")
AS_OF = datetime.fromisoformat("2027-04-08T15:00:00Z")
OBSERVED_THROUGH = datetime.fromisoformat("2027-04-09T15:00:00Z")
NOW = datetime.fromisoformat("2027-04-10T15:00:00Z")


@pytest.fixture(autouse=True)
def freeze_report_clock(monkeypatch):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz)

    monkeypatch.setattr(commercial_renewal_report, "datetime", FrozenDateTime)


def write_rows(path: Path, header: str, rows: list[tuple[str, ...]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header.split(","))
        writer.writerows(rows)


def sample_exports(
    tmp_path: Path, *, missing: str = "", duplicate_second: bool = False
) -> tuple[Path, Path, Path]:
    invoices: list[tuple[str, ...]] = []
    balance: list[tuple[str, ...]] = []
    cycles: list[tuple[str, ...]] = []

    def stamp(day: str) -> str:
        return f"{day}T15:00:00Z"

    def payment(
        label: str,
        customer: str,
        subscription: str,
        period_start: str,
        period_end: str,
        *,
        collected: str | None = None,
        reason: str = "subscription_cycle",
        second_due: str | None = None,
    ) -> None:
        invoice = f"in_{label}"
        invoices.append((invoice, f"cus_{customer}", PRICE_ID, "true", "paid", "usd", "2900", "0"))
        balance.append(
            (
                f"txn_{label}",
                stamp(collected or period_start),
                "usd",
                "29.00",
                "1.34",
                "27.66",
                "charge",
                invoice,
                f"ch_{label}",
            )
        )
        if label == "canceled_origin" and missing == "record":
            return
        first_due = stamp(period_end) if second_due else ""
        if label == "canceled_origin" and missing == "schedule":
            second_due = None
        cycles.append(
            (
                invoice,
                f"sub_{subscription}",
                reason,
                stamp(period_start),
                stamp(period_end),
                "false",
                first_due,
                stamp(second_due) if second_due else "",
            )
        )

    jan, feb, mar, apr, may = "2027-01-31", "2027-02-28", "2027-03-31", "2027-04-30", "2027-05-31"
    for customer in ("canceled", "retried", "late"):
        payment(
            f"{customer}_origin",
            customer,
            customer,
            jan,
            feb,
            reason="subscription_create",
            second_due=mar,
        )

    # February's invoice succeeds in March without becoming the March service period.
    payment("retry_r1", "retried", "retried", feb, mar, collected="2027-03-03")
    balance.append(balance[-1])
    payment("retry_r2", "retried", "retried", mar, apr, collected="2027-04-03")
    if duplicate_second:
        payment("extra_r2", "retried", "retried", mar, apr, collected="2027-04-04")
    payment("late_r1", "late", "late", feb, mar, collected="2027-03-20")
    payment("restart", "late", "replacement", mar, apr, reason="subscription_create")
    payment(
        "younger_origin",
        "younger",
        "younger",
        mar,
        apr,
        reason="subscription_create",
        second_due=may,
    )

    invoices_path = tmp_path / "invoices.csv"
    balance_path = tmp_path / "balance.csv"
    cycles_path = tmp_path / "cycles.csv"
    write_rows(
        invoices_path,
        "invoice_id,customer_id,price_ids,livemode,status,currency,amount_paid_cents,tax_cents",
        invoices,
    )
    write_rows(
        balance_path,
        "balance_transaction_id,created_utc,currency,gross,fee,net,reporting_category,invoice_id,charge_id",
        balance,
    )
    write_rows(
        cycles_path,
        "invoice_id,subscription_id,billing_reason,period_start_utc,period_end_utc,proration,"
        "renewal_1_due_utc,renewal_2_due_utc",
        cycles,
    )
    return invoices_path, balance_path, cycles_path


def report_at(
    paths: tuple[Path, Path, Path],
    *,
    as_of: datetime = AS_OF,
    observed_through: datetime = OBSERVED_THROUGH,
) -> dict:
    return build_renewal_report(
        *paths,
        price_id=PRICE_ID,
        history_start=HISTORY_START,
        as_of=as_of,
        observed_through=observed_through,
        grace_days=7,
    )


def test_mature_slots_keep_cancels_and_distinguish_retries_late_cash_and_restarts(tmp_path):
    paths = sample_exports(tmp_path)
    report = report_at(paths)

    assert report["status"] == "complete"
    assert set(report["cohorts"]) == {"2027-01", "2027-03"}
    january = report["cohorts"]["2027-01"]
    assert january["first_payers"] == 3
    for ordinal, late_recovered, no_payment in ((1, 1, 1), (2, 0, 2)):
        assert january[f"renewal_{ordinal}"] == {
            "not_due": 0,
            "in_grace": 0,
            "mature": 3,
            "paid_within_grace": 1,
            "late_recovered": late_recovered,
            "no_payment": no_payment,
            "rate_within_grace": pytest.approx(1 / 3),
        }
    younger = report["cohorts"]["2027-03"]
    assert younger["first_payers"] == 1
    for slot in (younger["renewal_1"], younger["renewal_2"]):
        assert slot["not_due"] == 1
        assert slot["mature"] == 0
        assert slot["rate_within_grace"] is None

    earlier = report_at(paths, as_of=datetime.fromisoformat("2027-04-02T15:00:00Z"))
    second = earlier["cohorts"]["2027-01"]["renewal_2"]
    assert second["in_grace"] == 3
    assert second["mature"] == 0
    assert second["paid_within_grace"] == 0
    assert second["rate_within_grace"] is None


def test_rejects_review_beyond_declared_source_coverage(tmp_path):
    paths = sample_exports(tmp_path)

    with pytest.raises(ValueError):
        report_at(paths, observed_through=datetime.fromisoformat("2027-04-07T15:00:00Z"))


@pytest.mark.parametrize("future_as_of", [False, True])
def test_rejects_export_coverage_in_the_future(tmp_path, future_as_of):
    paths = sample_exports(tmp_path)
    after_now = NOW + timedelta(microseconds=1)

    assert report_at(paths, observed_through=NOW)["status"] == "complete"
    with pytest.raises(ValueError, match="past the current time"):
        report_at(
            paths, as_of=after_now if future_as_of else AS_OF, observed_through=after_now
        )


@pytest.mark.parametrize(
    ("missing", "audit_key"),
    [("record", "missing_cycle_records"), ("schedule", "invalid_origin_schedules")],
)
def test_missing_canceled_origin_evidence_suppresses_rates(tmp_path, missing, audit_key):
    report = report_at(sample_exports(tmp_path, missing=missing))

    assert report["status"] == "incomplete"
    assert report["audit"][audit_key] == 1
    assert report["cohorts"]["2027-01"]["first_payers"] == 3
    for cohort in report["cohorts"].values():
        assert cohort["renewal_1"]["rate_within_grace"] is None
        assert cohort["renewal_2"]["rate_within_grace"] is None


@pytest.mark.parametrize(
    ("year", "day", "february_day", "april_day"),
    [(2027, 29, 28, 29), (2027, 30, 28, 30), (2027, 31, 28, 30), (2028, 31, 29, 30)],
)
def test_origin_uses_original_monthly_anchor_through_february(year, day, february_day, april_day):
    origin = datetime.fromisoformat(f"{year}-01-{day}T15:23:01Z")
    first = origin.replace(month=2, day=february_day)
    second = origin.replace(month=3)
    third = origin.replace(month=4, day=april_day)
    cycle = Cycle("sub_test", "subscription_create", origin, first, False, first, second)

    assert _original_schedule(cycle) == (first, second, third)


def change_cycle(path: Path, invoice_id: str, **values: str) -> None:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames
        rows = list(reader)
    assert fields is not None
    for row in rows:
        if row["invoice_id"] == invoice_id:
            row.update(values)
    write_rows(path, ",".join(fields), [tuple(row[field] for field in fields) for row in rows])


@pytest.mark.parametrize(
    "values",
    [
        {"billing_reason": "subscription_cycle"},
        {"period_end_utc": "2027-02-27T15:00:00Z", "renewal_1_due_utc": "2027-02-27T15:00:00Z"},
        {"renewal_2_due_utc": "2027-03-28T15:00:00Z"},
        {"renewal_1_due_utc": ""},
    ],
)
def test_unsupported_first_collected_invoice_cannot_prove_original_renewals(tmp_path, values):
    paths = sample_exports(tmp_path)
    change_cycle(paths[2], "in_canceled_origin", **values)
    report = report_at(paths)

    assert report["status"] == "incomplete"
    assert report["audit"]["invalid_origin_schedules"] == 1
    january = report["cohorts"]["2027-01"]
    assert january["first_payers"] == 3
    assert january["renewal_1"]["mature"] == january["renewal_2"]["mature"] == 2
    assert january["renewal_1"]["rate_within_grace"] is None
    assert january["renewal_2"]["rate_within_grace"] is None


def test_proven_origin_can_derive_both_due_dates(tmp_path):
    paths = sample_exports(tmp_path)
    expected = report_at(paths)
    for customer in ("canceled", "retried", "late", "younger"):
        change_cycle(paths[2], f"in_{customer}_origin", renewal_1_due_utc="", renewal_2_due_utc="")

    assert report_at(paths) == expected


@pytest.mark.parametrize(
    ("invoice_id", "values", "slot", "unpaid"),
    [
        ("in_retry_r1", {"period_start_utc": "2027-03-01T15:00:00Z"}, "renewal_1", 1),
        ("in_retry_r1", {"proration": "true"}, "renewal_1", 1),
        ("in_retry_r2", {"billing_reason": "subscription_update"}, "renewal_2", 2),
        ("in_retry_r2", {"period_end_utc": "2027-04-29T15:00:00Z"}, "renewal_2", 2),
    ],
)
def test_unsupported_linked_period_is_neither_renewal_nor_churn(tmp_path, invoice_id, values, slot, unpaid):
    paths = sample_exports(tmp_path)
    change_cycle(paths[2], invoice_id, **values)
    report = report_at(paths)

    assert report["status"] == "incomplete"
    assert report["audit"]["unsupported_linked_cycle_records"] == 1
    january = report["cohorts"]["2027-01"]
    assert january["first_payers"] == 3
    assert january[slot]["mature"] == 2
    assert january[slot]["paid_within_grace"] == 0
    assert january[slot]["no_payment"] == unpaid
    assert january[slot]["rate_within_grace"] is None


def test_multiple_collected_invoices_for_one_slot_withhold_renewal_rates(tmp_path):
    report = report_at(sample_exports(tmp_path, duplicate_second=True))

    assert report["status"] == "incomplete"
    assert report["audit"]["extra_collected_invoices_for_slot"] == 1
    second = report["cohorts"]["2027-01"]["renewal_2"]
    assert second["mature"] == second["no_payment"] == 2
    assert second["paid_within_grace"] == 0
    assert second["rate_within_grace"] is None
