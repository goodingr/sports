from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
import yaml

from src.dashboard import app as dashboard
from src.data.audit_nba_schedule import save_schedule_audit
from src.predict.paper_ledger import SCHEMA_PATH, load_trial

NOW = datetime(2026, 11, 7, 4, 0, tzinfo=timezone.utc)


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz)


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    path = tmp_path / "paper.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO paper_trials VALUES (?, ?, ?)",
            (
                "nba",
                (NOW - timedelta(days=1)).isoformat(),
                json.dumps(
                    {
                        "league": "NBA",
                        "market": "totals",
                        "book": "draftkings",
                        "review_at": (NOW + timedelta(days=30)).isoformat(),
                        "min_decisions": 150,
                        "min_roi": 0.05,
                        "min_clv_coverage": 0.8,
                    }
                ),
            ),
        )
    monkeypatch.setattr(dashboard, "resolve_db_path", lambda: path)
    monkeypatch.setattr(dashboard, "datetime", FixedClock)
    monkeypatch.setattr(dashboard, "DEFAULT_RULES", tmp_path / "paper_trial.yml")
    return path


def _text(component):
    if isinstance(component, (list, tuple)):
        return " ".join(_text(child) for child in component)
    if component is None:
        return ""
    if hasattr(component, "children"):
        return _text(component.children)
    return str(component)


def _record_run(path, fixtures=(), *, status="evaluated"):
    with sqlite3.connect(path) as conn:
        cursor = conn.execute(
            "INSERT INTO paper_capture_runs (trial_id, observed_at, status) VALUES (?, ?, ?)",
            ("nba", (NOW - timedelta(minutes=5)).isoformat(), status),
        )
        conn.executemany(
            "INSERT INTO paper_fixture_observations VALUES (?, ?, ?, ?, ?)",
            [
                (cursor.lastrowid, game_id, (NOW + timedelta(hours=1)).isoformat(), outcome, None)
                for game_id, outcome in fixtures
            ],
        )


def test_trial_card_shows_distinct_coverage_and_attempt_reasons(ledger):
    _record_run(
        ledger,
        [
            ("first", "below_edge_threshold"),
            ("second", "recorded"),
            ("third", "stale_quote"),
        ],
    )
    _record_run(ledger, [("first", "below_edge_threshold")])
    text = _text(dashboard.show_paper_trials(None))
    assert "Last capture attempt Nov 06, 2026 10:55 PM EST" in text
    assert "evaluated · Capture attempts 2" in text
    assert "Fully evaluated 2 / 3 observed fixtures · 1 below the edge threshold" in text
    assert "Below edge threshold 2" in text
    assert "Stale quote 1" in text
    assert "Recorded 1" in text


def test_trial_card_distinguishes_missing_history_from_empty_capture_window(ledger):
    _record_run(ledger, status="no_predictions")
    text = _text(dashboard.show_paper_trials(None))
    assert "no predictions · Capture attempts 1" in text
    assert "No fixtures were observed inside the capture window." in text
    assert "No capture history is available." not in text

    with sqlite3.connect(ledger) as conn:
        conn.execute("DROP TABLE paper_fixture_observations")
        conn.execute("DROP TABLE paper_capture_runs")
    text = _text(dashboard.show_paper_trials(None))
    assert "No capture history is available." in text
    assert "No fixtures were observed" not in text
    assert "Fully evaluated 0 / 0" not in text


def test_trial_card_shows_no_attempts_when_coverage_is_initialized(ledger):
    text = _text(dashboard.show_paper_trials(None))
    assert "No capture attempts have been recorded." in text
    assert "No fixtures were observed" not in text
    assert "No capture history is available." not in text


def test_configured_registration_comes_first_and_retained_review_remains_visible(ledger):
    config_path = ledger.with_name("paper_trial.yml")
    specification = {
        "trial_id": "nba_v2",
        "league": "NBA",
        "market": "totals",
        "model_type": "random_forest",
        "model_sha256": "a" * 64,
        "book": "draftkings",
        "odds_source": "the-odds-api",
        "min_edge": 0.2,
        "max_quote_age_minutes": 10,
        "min_minutes_before_start": 15,
        "max_hours_before_start": 12,
        "min_decisions": 180,
        "min_roi": 0.05,
        "min_clv_coverage": 0.8,
        "review_at": (NOW + timedelta(days=120)).isoformat(),
    }
    config_path.write_text(yaml.safe_dump(specification), encoding="utf-8")
    with sqlite3.connect(ledger) as conn:
        conn.execute(
            "UPDATE paper_trials SET specification = json_set(specification, '$.review_at', ?) "
            "WHERE trial_id = 'nba'",
            ((NOW - timedelta(days=14)).isoformat(),),
        )
        conn.execute(
            "INSERT INTO paper_trials VALUES (?, ?, ?)",
            ("nba_v2", NOW.isoformat(), load_trial(config_path).specification),
        )

    configured, retained = map(_text, dashboard.show_paper_trials(None))
    assert "Configured capture trial Collecting" in configured
    assert "0 / 180" in configured
    assert "Trial nba_v2" in configured
    assert "Retained registration Complete" in retained
    assert "0 / 150" in retained
    assert "Trial nba · Review verdict fail" in retained

    specification["min_edge"] = 0.3
    config_path.write_text(yaml.safe_dump(specification), encoding="utf-8")
    text = _text(dashboard.show_paper_trials(None))
    assert "No configured capture trial matches the ledger." in text
    assert text.count("Retained registration") == 2
    assert "Configured capture trial" not in text


@pytest.mark.parametrize("config_content", [None, "trial_id: [", "trial_id: nba"])
def test_missing_or_invalid_config_leaves_the_ledger_usable(ledger, config_content):
    if config_content is not None:
        ledger.with_name("paper_trial.yml").write_text(config_content, encoding="utf-8")

    text = _text(dashboard.show_paper_trials(None))
    assert "No configured capture trial matches the ledger." in text
    assert "Retained registration Collecting" in text
    assert "Trial nba" in text


def test_daily_capture_panel_checks_the_exact_receipt_and_shows_later_attempt(ledger, monkeypatch):
    rules = ledger.with_name("paper_trial.yml")
    rules.write_bytes((dashboard.PROJECT_ROOT / "config/paper_trial.yml").read_bytes())
    trial = load_trial(rules)
    tick = NOW + timedelta(hours=19, minutes=4)
    fetched = tick + timedelta(minutes=2)
    captured = fetched + timedelta(seconds=4)
    raw_dir = ledger.parent / "raw" / "odds" / "basketball_nba"
    raw_dir.mkdir(parents=True)
    receipt = raw_dir / fetched.strftime("odds_%Y-%m-%dT%H-%M-%SZ.json")
    receipt.write_text(json.dumps({
        "fetched_at": fetched.isoformat(),
        "request": {
            "url": "https://api.the-odds-api.com/v4/sports/basketball_nba/odds",
            "params": {"regions": "us", "markets": "h2h,totals", "oddsFormat": "american"},
        },
        "results": [{
            "id": "provider-fixture", "sport_key": "basketball_nba",
            "commence_time": (tick + timedelta(hours=2)).isoformat(),
        }],
    }), encoding="utf-8")
    with sqlite3.connect(ledger) as conn:
        conn.executescript("""
            CREATE TABLE sports (sport_id, league);
            CREATE TABLE games (game_id, sport_id, odds_api_id, home_team_id, away_team_id);
            CREATE TABLE odds_snapshots (snapshot_id, sport_id, source, fetched_at_utc, raw_path);
            INSERT INTO sports VALUES (1, 'NBA');
            INSERT INTO games VALUES ('game', 1, 'provider-fixture', 1, 2);
        """)
        conn.execute("INSERT INTO paper_trials VALUES (?, ?, ?)", (trial.trial_id, NOW.isoformat(), trial.specification))
        run = conn.execute(
            "INSERT INTO paper_capture_runs (trial_id, observed_at, status) VALUES (?, ?, 'evaluated')",
            (trial.trial_id, captured.isoformat()),
        ).lastrowid
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('snapshot', 1, 'the-odds-api', ?, ?)",
            (fetched.isoformat(), str(receipt)),
        )
        conn.execute(
            "INSERT INTO paper_fixture_observations VALUES (?, 'game', ?, 'below_edge_threshold', 'snapshot')",
            (run, (tick + timedelta(hours=2)).isoformat()),
        )
        conn.execute(
            "INSERT INTO paper_capture_runs (trial_id, observed_at, status) VALUES (?, ?, 'failed')",
            (trial.trial_id, (captured + timedelta(minutes=5)).isoformat()),
        )

    assert "Daily NBA capture" in _text(dashboard.render_page("/paper"))
    assert "Waiting for the first daily capture window" in _text(dashboard.show_daily_capture(None))

    class AfterGrace(datetime):
        @classmethod
        def now(cls, tz=None):
            return (tick + timedelta(minutes=31)).astimezone(tz)

    monkeypatch.setattr(dashboard, "datetime", AfterGrace)
    text = _text(dashboard.show_daily_capture(1))
    assert "Daily receipt and evaluation verified" in text
    assert "Expected Nov 07, 06:04:00 PM EST · Grace ends Nov 07, 06:34:00 PM EST" in text
    assert "Evaluated 1 / 1 eligible fixtures · 1 below the edge threshold" in text
    assert "Later attempt Nov 07, 06:11:04 PM EST · failed" in text
    assert receipt.name in text

    receipt.unlink()
    assert "Daily odds receipt is missing" in _text(dashboard.show_daily_capture(2))


def _schedule_report(*, audited_at=NOW, observed_at=NOW, linked=False):
    day = NOW.astimezone(dashboard.DISPLAY_TIMEZONE).date() + timedelta(days=1)
    return {
        "report_version": 1,
        "trial_id": "nba_test",
        "eastern_date": day.isoformat(),
        "warehouse_audited_at_utc": audited_at.isoformat(),
        "official_source": {"observed_at_utc": observed_at.isoformat()},
        "scheduled_regular_season_games": 1,
        "provider_observed": int(linked),
        "provider_linked": int(linked),
        "warehouse_matched": int(linked),
        "provider_evidence": {"archives_checked": 1, "archives_unavailable_or_invalid": 0},
        "fixtures": [
            {
                "away": "DEN", "home": "IND", "early_entry": True,
                "official_start_utc": (NOW + timedelta(hours=21)).isoformat(),
                "warehouse_start_utc": (NOW + timedelta(hours=21, minutes=10)).isoformat() if linked else None,
                "decision_deadline_utc": (NOW + timedelta(hours=20, minutes=45)).isoformat(),
                "warehouse_matched": linked, "provider_linked": linked,
                "status": "linked" if linked else "missing_warehouse_fixture",
                "deadline_passed": False,
            },
        ],
    }


def test_schedule_panel_reads_latest_saved_audit_and_explains_early_gaps(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "AUDIT_REPORTS_DIR", tmp_path)
    monkeypatch.setattr(dashboard, "datetime", FixedClock)
    earlier = NOW - timedelta(hours=1)
    old_path = save_schedule_audit(_schedule_report(audited_at=earlier, observed_at=earlier, linked=True), tmp_path)
    latest_path = save_schedule_audit(_schedule_report(), tmp_path)
    assert old_path.exists() and latest_path.exists() and old_path < latest_path

    text = _text(dashboard.show_schedule_coverage(None))
    assert "No saved audit for Nov 06" in text
    assert "Verified provider links 0 of 1" in text
    assert "Provider observed 0 of 1" in text
    assert "Early starts without a verified link 1" in text
    assert "DEN at IND" in text
    assert "Missing warehouse fixture · Early start" in text
    assert "Official schedule observed Nov 06, 11:00 PM EST" in text
    assert "Entry deadline" in text
    assert "Nov 07, 07:45 PM EST" in text


def test_schedule_panel_marks_elapsed_deadline_without_updating_saved_evidence(tmp_path):
    path = save_schedule_audit(_schedule_report(), tmp_path)
    original = path.read_bytes()
    text = _text(dashboard.schedule_coverage_cards(tmp_path, NOW + timedelta(hours=21)))
    assert "Nov 07, 07:45 PM EST · Passed since audit" in text
    assert "Provider evidence at audit" in text
    assert "Missing warehouse fixture · Early start" in text
    assert path.read_bytes() == original


def test_schedule_panel_checks_archived_official_response(tmp_path):
    report = _schedule_report(linked=True)
    raw = b"official schedule fixture"
    digest = hashlib.sha256(raw).hexdigest()
    relative = f"raw/schedules/nba/{report['eastern_date']}/{digest}.body"
    report["official_source"].update(raw_path=relative, sha256=digest)
    audit_dir = tmp_path / "audits"
    data_dir = tmp_path / "data"
    archive = data_dir / relative
    archive.parent.mkdir(parents=True)
    archive.write_bytes(raw)
    save_schedule_audit(report, audit_dir)

    text = _text(dashboard.schedule_coverage_cards(audit_dir, NOW, data_dir=data_dir))
    assert "Official response checksum verified." in text
    assert "Warehouse tipoff" in text
    assert "Nov 07, 08:00 PM EST Nov 07, 08:10 PM EST" in text
    archive.write_bytes(b"different input")
    text = _text(dashboard.schedule_coverage_cards(audit_dir, NOW, data_dir=data_dir))
    assert "Archived official response is unavailable or invalid." in text
    assert "Official response checksum verified." not in text


def test_schedule_panel_flags_old_or_unusable_audit(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "AUDIT_REPORTS_DIR", tmp_path)
    monkeypatch.setattr(dashboard, "datetime", FixedClock)
    old = NOW - timedelta(hours=7)
    report = _schedule_report(audited_at=old, observed_at=old)
    report["provider_evidence"]["archives_unavailable_or_invalid"] = 1
    path = save_schedule_audit(report, tmp_path)
    text = _text(dashboard.show_schedule_coverage(None))
    assert "Saved audit is more than six hours old" in text
    assert "Provider archives unavailable or invalid · 1" in text

    path.write_text("{}", encoding="utf-8")
    text = _text(dashboard.show_schedule_coverage(None))
    assert "Saved audit for Nov 07 is unavailable" in text
    assert "Verified provider links" not in text
