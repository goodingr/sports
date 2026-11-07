import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest
import requests

from scripts import commercial_interest_report as report

FIRST_VIEW = "2026-11-01T00:00:00Z"
JOIN = "2026-11-02T12:00:00Z"
WITHDRAW = "2026-11-06T12:00:00Z"


def user(user_id, *, source="direct", viewed=FIRST_VIEW, joined=None, withdrawn=None):
    return {
        "id": user_id,
        "email_addresses": [{"email_address": "private@example.test"}],
        "private_metadata": {
            "commercialPriceInterest": {
                "offerVersion": report.OFFER_VERSION,
                "source": source,
                "viewedAt": viewed,
                "joinedAt": joined,
                "withdrawnAt": withdrawn,
            }
        },
    }


def test_aggregates_current_states_for_unique_users_in_first_view_cohort():
    active = user("active", source="community", joined=JOIN)
    old_offer = user("old_offer")
    old_offer["private_metadata"]["commercialPriceInterest"]["offerVersion"] = "old-version"
    result = report.build_report(
        [
            active,
            active,
            user("withdrawn", source="community", joined=JOIN, withdrawn=WITHDRAW),
            user("viewed"),
            user("excluded", source="email", joined=JOIN),
            user("too_early", viewed="2026-10-31T23:59:59Z"),
            user("at_cutoff", viewed="2026-11-02T00:00:00Z"),
            user("bad_source", source="arbitrary account data"),
            user("invalid_state", withdrawn=WITHDRAW),
            old_offer,
            {"id": "no_offer", "private_metadata": {}},
        ],
        viewed_from=datetime.fromisoformat(FIRST_VIEW),
        viewed_before=datetime.fromisoformat("2026-11-02T00:00:00Z"),
        exclude_user_ids={"excluded"},
    )

    assert result["population"] == "signed_in_price_offer_viewers"
    assert "qualified_cohort" not in result
    assert result["totals"] == {
        "viewed": 3,
        "active": 1,
        "withdrawn": 1,
        "never_opted_in": 1,
        "first_opt_ins_in_window": 0,
    }
    assert result["by_source"]["community"] == {
        "viewed": 2,
        "active": 1,
        "withdrawn": 1,
        "never_opted_in": 0,
        "first_opt_ins_in_window": 0,
    }
    assert result["by_source"]["email"]["viewed"] == 0
    assert result["invalid_offer_records"] == 2
    assert "qualified_cohort" not in result
    serialized = json.dumps(result)
    assert "private@example.test" not in serialized
    assert "arbitrary account data" not in serialized


def test_first_window_opt_ins_stay_fixed_after_late_join_and_withdrawal():
    window = {
        "viewed_from": datetime.fromisoformat(FIRST_VIEW),
        "viewed_before": datetime.fromisoformat("2026-12-01T00:00:00Z"),
        "qualified_user_ids": {"user_first", "user_last", "user_late", "user_outside"},
    }
    final_in_window = "2026-12-01T00:59:59.999999+01:00"
    at_exclusive_end = "2026-11-30T19:00:00-05:00"
    stable_records = [
        user("user_first", source="referral", joined=FIRST_VIEW),
        user("user_unscreened", source="email", joined=JOIN),
        user("user_outside", source="email", viewed=at_exclusive_end, joined=at_exclusive_end),
    ]
    before = report.build_report(
        [
            *stable_records,
            user("user_last", source="referral", joined=final_in_window),
            user("user_late", source="community"),
        ],
        **window,
    )
    after = report.build_report(
        [
            *stable_records,
            user(
                "user_last",
                source="referral",
                joined=final_in_window,
                withdrawn="2026-12-02T12:00:00Z",
            ),
            user("user_late", source="community", joined=at_exclusive_end),
        ],
        **window,
    )

    for result in (before, after):
        assert result["totals"]["viewed"] == 4
        assert result["totals"]["first_opt_ins_in_window"] == 3
        assert result["by_source"]["email"]["first_opt_ins_in_window"] == 1
        qualified = result["qualified_cohort"]
        assert qualified["totals"]["viewed"] == 3
        assert qualified["totals"]["first_opt_ins_in_window"] == 2
        assert qualified["by_source"]["referral"]["first_opt_ins_in_window"] == 2
        assert qualified["by_source"]["community"]["first_opt_ins_in_window"] == 0
        assert qualified["by_source"]["email"]["first_opt_ins_in_window"] == 0

    assert before["qualified_cohort"]["by_source"]["community"]["never_opted_in"] == 1
    assert after["qualified_cohort"]["by_source"]["community"]["active"] == 1
    assert before["qualified_cohort"]["by_source"]["referral"]["active"] == 2
    assert after["qualified_cohort"]["by_source"]["referral"]["withdrawn"] == 1


def test_cli_paginates_and_prints_only_aggregates(monkeypatch, capsys):
    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_live_mock-secret")
    monkeypatch.setattr(report, "_load_dotenv_if_available", lambda: None)
    monkeypatch.setattr(report, "PAGE_SIZE", 2)
    session = MagicMock()
    session.get.side_effect = [
        MagicMock(status_code=200, json=lambda: [user("mock-one"), user("mock-two")]),
        MagicMock(status_code=200, json=lambda: [user("mock-three", joined=JOIN)]),
    ]
    monkeypatch.setattr(
        report.requests, "Session", lambda: MagicMock(__enter__=lambda self: session)
    )

    assert report.main(["--exclude-user-id", "mock-two"]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert "mock-secret" not in output.out and "mock-one" not in output.out
    assert json.loads(output.out)["clerk_mode"] == "live"
    assert json.loads(output.out)["totals"]["viewed"] == 2
    assert json.loads(output.out)["totals"]["active"] == 1
    assert json.loads(output.out)["totals"]["first_opt_ins_in_window"] == 1
    assert [call.kwargs["params"]["offset"] for call in session.get.call_args_list] == [0, 2]
    assert all(call.args[0] == report.CLERK_USERS_URL for call in session.get.call_args_list)


def test_cli_reports_screened_accounts_separately_from_raw_exposure(monkeypatch, capsys, tmp_path):
    roster = tmp_path / "private roster.txt"
    roster.write_text(
        "user_screened\nuser_screened\n\n user_withdrawn \nuser_excluded\n"
        "user_outside\nuser_missing\n",
        encoding="utf-8-sig",
    )
    screened = user("user_screened", source="referral", joined=JOIN)
    monkeypatch.setattr(report, "_read_clerk_key", lambda *_: "sk_live_mock-secret")
    monkeypatch.setattr(
        report,
        "fetch_users",
        lambda _: iter(
            [
                screened,
                screened,
                user("user_unscreened", source="referral", joined=JOIN),
                user("user_withdrawn", source="referral", joined=JOIN, withdrawn=WITHDRAW),
                user("user_excluded", source="referral", joined=JOIN),
                user("user_outside", viewed="2026-10-31T23:59:59Z"),
            ]
        ),
    )

    assert report.main(
        [
            "--qualified-user-ids-file", str(roster),
            "--viewed-from", FIRST_VIEW,
            "--viewed-before", "2026-12-01T00:00:00Z",
            "--exclude-user-id", "user_excluded",
        ]
    ) == 0
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["totals"] == {
        "viewed": 3,
        "active": 2,
        "withdrawn": 1,
        "never_opted_in": 0,
        "first_opt_ins_in_window": 3,
    }
    qualified = result["qualified_cohort"]
    assert qualified["eligible_roster_accounts"] == 4
    assert qualified["matched_accounts"] == 3
    assert qualified["totals"] == {
        "viewed": 2,
        "active": 1,
        "withdrawn": 1,
        "never_opted_in": 0,
        "first_opt_ins_in_window": 2,
    }
    assert qualified["by_source"]["referral"] == qualified["totals"]
    assert qualified["by_source"]["direct"]["viewed"] == 0
    assert all(
        private not in output.out
        for private in ("user_screened", "user_unscreened", "user_missing", roster.name, "@example.test")
    )
    assert output.err == ""


@pytest.mark.parametrize(
    ("contents", "expected_error"),
    [
        (None, "Qualified user ID file could not be read.\n"),
        (b"\xff", "Qualified user ID file could not be read.\n"),
        (b"\n  \n", "Qualified user ID file must contain one Clerk user ID per nonblank line.\n"),
        (
            b"user_screened\nprivate@example.test\n",
            "Qualified user ID file must contain one Clerk user ID per nonblank line.\n",
        ),
    ],
)
def test_invalid_roster_fails_before_clerk_access(monkeypatch, capsys, tmp_path, contents, expected_error):
    read_key = MagicMock()
    fetch_users = MagicMock()
    monkeypatch.setattr(report, "_read_clerk_key", read_key)
    monkeypatch.setattr(report, "fetch_users", fetch_users)
    roster = tmp_path / "private roster.txt"
    if contents is not None:
        roster.write_bytes(contents)

    assert report.main(["--qualified-user-ids-file", str(roster)]) == 1
    read_key.assert_not_called()
    fetch_users.assert_not_called()
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == expected_error


def test_failed_page_emits_no_partial_report_or_private_error(monkeypatch, capsys):
    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_live_mock-secret")
    monkeypatch.setattr(report, "_load_dotenv_if_available", lambda: None)
    monkeypatch.setattr(report, "PAGE_SIZE", 1)
    session = MagicMock()
    session.get.side_effect = [
        MagicMock(status_code=200, json=lambda: [user("mock-one")]),
        requests.ConnectionError("mock-secret private@example.test"),
    ]
    monkeypatch.setattr(
        report.requests, "Session", lambda: MagicMock(__enter__=lambda self: session)
    )

    assert report.main([]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "Clerk user request or response could not be read.\n"


def test_test_credential_rejected_by_default_before_any_request(monkeypatch, capsys):
    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_test_mock-secret")
    monkeypatch.setattr(report, "_load_dotenv_if_available", lambda: None)
    fetch_users = MagicMock()
    monkeypatch.setattr(report, "fetch_users", fetch_users)

    assert report.main([]) == 1
    fetch_users.assert_not_called()
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "CLERK_SECRET_KEY does not match the expected live mode.\n"


def test_explicit_dotenv_selects_test_key_without_overwriting_inherited_key(
    monkeypatch, capsys, tmp_path
):
    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_live_inherited-secret")
    default_loader = MagicMock()
    monkeypatch.setattr(report, "_load_dotenv_if_available", default_loader)
    fetch_users = MagicMock(return_value=[])
    monkeypatch.setattr(report, "fetch_users", fetch_users)
    dotenv_path = tmp_path / "pilot.env"
    dotenv_path.write_text("CLERK_SECRET_KEY='sk_test_file-secret'\n", encoding="utf-8")

    assert report.main(["--dotenv", str(dotenv_path), "--expected-mode", "test"]) == 0
    fetch_users.assert_called_once_with("sk_test_file-secret")
    default_loader.assert_not_called()
    output = capsys.readouterr()
    assert json.loads(output.out)["clerk_mode"] == "test"
    assert output.err == ""
    assert "file-secret" not in output.out
    assert report.os.environ["CLERK_SECRET_KEY"] == "sk_live_inherited-secret"


@pytest.mark.parametrize(
    ("dotenv_content", "expected_error"),
    [
        (None, "Clerk credential configuration could not be read.\n"),
        ("OTHER_KEY=example\n", "CLERK_SECRET_KEY is required.\n"),
        (
            "CLERK_SECRET_KEY=${CLERK_SECRET_KEY}\n",
            "CLERK_SECRET_KEY does not match the expected live mode.\n",
        ),
    ],
)
def test_explicit_dotenv_never_falls_back_or_exposes_credential_path(
    monkeypatch, capsys, tmp_path, dotenv_content, expected_error
):
    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_live_inherited-secret")
    fetch_users = MagicMock()
    monkeypatch.setattr(report, "fetch_users", fetch_users)
    dotenv_path = tmp_path / "private credential filename.env"
    if dotenv_content is not None:
        dotenv_path.write_text(dotenv_content, encoding="utf-8")

    assert report.main(["--dotenv", str(dotenv_path)]) == 1
    fetch_users.assert_not_called()
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == expected_error
