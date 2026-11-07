"""Exercise the official schedule audit against saved inputs and a read-only warehouse."""

import hashlib
import json
import sys
from datetime import date, datetime, timedelta, timezone

import pytest
import requests

from scripts.backup_evidence import EVIDENCE_ZIP, create_evidence_zip, read_raw_evidence
from src.data import audit_nba_schedule as audit
from src.db.core import connect, initialize

DAY = date(2026, 11, 8)
NOW = datetime(2026, 11, 8, 20, 20, tzinfo=timezone.utc)
TEAMS = [
    (1, "IND", "Indiana Pacers"),
    (2, "DEN", "Denver Nuggets"),
    (3, "ATL", "Atlanta Hawks"),
    (4, "BKN", "Brooklyn Nets"),
    (5, "BOS", "Boston Celtics"),
    (6, "NYK", "New York Knicks"),
    (7, "MIA", "Miami Heat"),
    (8, "ORL", "Orlando Magic"),
    (9, "LAL", "Los Angeles Lakers"),
    (10, "DET", "Detroit Pistons"),
    (11, "SAC", "Sacramento Kings"),
    (12, "MIL", "Milwaukee Bucks"),
    (13, "PHI", "Philadelphia 76ers"),
    (14, "CHI", "Chicago Bulls"),
    (15, "NOP", "New Orleans Pelicans"),
    (16, "LAC", "LA Clippers"),
]


def _game_id(i):
    return f"0022600{i:03d}"


def _event_id(i):
    return f"{i:032x}"


def _card(i, start, home, away, *, status=1):
    return {
        "cardType": "game",
        "cardData": {
            "gameId": _game_id(i),
            "leagueId": "00",
            "seasonType": "Regular Season",
            "gameStatus": status,
            "gameTimeUtc": start.isoformat(),
            "homeTeam": {"teamId": 1610612700 + home, "teamTricode": TEAMS[home - 1][1]},
            "awayTeam": {"teamId": 1610612700 + away, "teamTricode": TEAMS[away - 1][1]},
        },
    }


def _official(cards, *, html=True, count=None, selected=DAY.isoformat()):
    page = {
        "selectedDate": selected,
        "hasError": False,
        "allGamesInCurrentYear": {
            "2026": {DAY.isoformat(): len(cards) if count is None else count}
        },
        "gameCardFeed": {"modules": [{"cards": cards}] if cards else []},
    }
    if html:
        body = json.dumps({"props": {"pageProps": page}})
        return f'<html><script id="__NEXT_DATA__" type="application/json">{body}</script></html>'.encode()
    return json.dumps({"pageProps": page}).encode()


def _event(i, start, home, away):
    return {
        "id": _event_id(i),
        "sport_key": "basketball_nba",
        "commence_time": start.isoformat(),
        "home_team": TEAMS[home - 1][2],
        "away_team": TEAMS[away - 1][2],
    }


def _unexpected(*args, **kwargs):
    pytest.fail("An offline audit must not call the network")


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz)

    path = tmp_path / "audit.db"
    monkeypatch.setenv("SPORTS_DB_PATH", str(path))
    monkeypatch.setattr(audit, "load_env", lambda: None)
    monkeypatch.setattr(audit, "datetime", Clock)
    monkeypatch.setattr(requests, "get", _unexpected)
    initialize(path)
    with connect(path) as conn:
        conn.execute(
            "INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Basketball', 'NBA', 'moneyline')"
        )
        conn.executemany(
            "INSERT INTO teams (team_id, sport_id, code, name) VALUES (?, 1, ?, ?)", TEAMS
        )
    return path


def _archive(path, name, captured, events, *, discovery=False, bad_hash=False):
    raw_path = path.parent / f"{name}.json"
    payload = {
        "fetched_at": captured.isoformat(),
        "results": events,
        "request": {"url": audit._URL if discovery else audit._ODDS_URL},
    }
    if discovery:
        payload.update(source=audit._SOURCE, request_credits=0)
    raw_path.write_text(json.dumps(payload), encoding="utf-8")
    with connect(path) as conn:
        if discovery:
            conn.execute(
                "INSERT OR IGNORE INTO data_sources (source_id, source_key, name, category) VALUES (1, ?, 'Events', 'schedule')",
                (audit._SOURCE,),
            )
            digest = "invalid" if bad_hash else hashlib.sha256(raw_path.read_bytes()).hexdigest()
            conn.execute(
                "INSERT INTO source_files (source_id, captured_at, storage_path, hash) VALUES (1, ?, ?, ?)",
                (captured.isoformat(), str(raw_path), digest),
            )
        else:
            conn.execute(
                "INSERT INTO odds_snapshots VALUES (?, ?, 1, 'the-odds-api', ?)",
                (name, captured.isoformat(), str(raw_path)),
            )
    return raw_path


def test_offline_cli_distinguishes_linked_late_missing_and_ambiguous(
    warehouse, monkeypatch, capsys
):
    start = NOW + timedelta(minutes=10)
    cards = [_card(i, start, 2 * i - 1, 2 * i) for i in range(1, 9)]
    official = warehouse.parent / "official.html"
    official.write_bytes(_official(cards))
    with connect(warehouse) as conn:
        conn.executemany(
            """
            INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id)
            VALUES (?, 1, ?, ?, ?, ?)
            """,
            [
                ("linked", (start + timedelta(minutes=10)).isoformat(), 1, 2, _event_id(1)),
                ("late", start.isoformat(), 3, 4, _event_id(2)),
                ("reversed", start.isoformat(), 8, 7, _event_id(4)),
                ("unchecked", start.isoformat(), 9, 10, _event_id(5)),
                ("unlinked", start.isoformat(), 11, 12, None),
                ("competing IDs", start.isoformat(), 13, 14, _event_id(7)),
                (
                    "clock conflict",
                    (start - timedelta(minutes=10)).isoformat(),
                    15,
                    16,
                    _event_id(8),
                ),
            ],
        )
    verified_at = NOW - timedelta(minutes=5)
    archived = _archive(
        warehouse,
        "events",
        verified_at,
        [
            _event(1, start + timedelta(minutes=10), 1, 2),
            _event(3, start, 5, 6),
            _event(6, start, 11, 12),
            _event(7, start, 13, 14),
            _event(8, start + timedelta(minutes=10), 15, 16),
        ],
        discovery=True,
    )
    _archive(warehouse, "ordinary", NOW, [_event(2, start, 3, 4), _event(9, start, 13, 14)])
    _archive(warehouse, "bad_hash", NOW, [_event(5, start, 9, 10)], discovery=True, bad_hash=True)
    _archive(warehouse, "future", NOW + timedelta(seconds=1), [_event(5, start, 9, 10)])
    with connect(warehouse) as conn:
        before = list(conn.iterdump())

    observed = NOW - timedelta(hours=3)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "audit",
            "--date",
            DAY.isoformat(),
            "--fixture",
            str(official),
            "--fixture-observed-at",
            observed.isoformat(),
        ],
    )
    audit.main()
    result = json.loads(capsys.readouterr().out)
    assert result["by_status"] == {
        "linked": 1,
        "late": 1,
        "missing_warehouse_fixture": 1,
        "ambiguous": 3,
        "unverified_provider_id": 1,
        "unlinked_provider_id": 1,
    }
    assert result["warehouse_matched"] == 6
    assert result["warehouse_missing"] == 1
    assert result["provider_linked"] == 2
    assert result["provider_observed_late"] == 1
    assert result["provider_evidence"]["archives_checked"] == 3
    assert result["provider_evidence"]["archives_unavailable_or_invalid"] == 1
    assert result["official_source"]["observed_at_utc"] == observed.isoformat()
    assert result["official_source"]["sha256"] == hashlib.sha256(official.read_bytes()).hexdigest()
    assert result["warehouse_audited_at_utc"] == NOW.isoformat()
    rows = {row["nba_game_id"]: row for row in result["fixtures"]}
    assert rows[_game_id(1)]["warehouse_start_utc"] == (start + timedelta(minutes=10)).isoformat()
    assert rows[_game_id(1)]["first_retained_provider_observation_utc"] == verified_at.isoformat()
    assert rows[_game_id(1)]["first_retained_provider_observation_path"] == str(archived)
    assert rows[_game_id(2)]["provider_observed_by_deadline"] is False
    assert rows[_game_id(3)]["verified_provider_ids"] == [_event_id(3)]
    assert rows[_game_id(4)]["warehouse_candidates"] == ["reversed"]
    assert rows[_game_id(7)]["verified_provider_ids"] == [_event_id(7), _event_id(9)]
    with connect(warehouse) as conn:
        assert list(conn.iterdump()) == before


@pytest.mark.parametrize(
    "defect", ["missing_json", "wrong_date", "wrong_count", "duplicate_id", "naive_tipoff"]
)
def test_malformed_official_input_fails_closed_before_reading_warehouse(
    tmp_path, defect, monkeypatch
):
    monkeypatch.setattr(requests, "get", _unexpected)
    fixture = tmp_path / "invalid.html"
    card = _card(1, NOW, 1, 2)
    if defect == "missing_json":
        raw = b"<html>no structured schedule</html>"
    elif defect == "wrong_date":
        raw = _official([card], selected="2026-11-09")
    elif defect == "wrong_count":
        raw = _official([card], count=2)
    elif defect == "duplicate_id":
        raw = _official([card, card])
    else:
        card["cardData"]["gameTimeUtc"] = "2026-11-08T20:00:00"
        raw = _official([card])
    fixture.write_bytes(raw)
    with pytest.raises(ValueError, match="Invalid or incomplete official NBA games feed"):
        audit.audit_schedule(
            DAY, fixture_path=fixture, fixture_observed_at=NOW, db_path=tmp_path / "absent.db"
        )


def test_provider_id_observed_for_different_matchup_is_ambiguous(warehouse):
    start = NOW + timedelta(hours=1)
    fixture = warehouse.parent / "next.json"
    fixture.write_bytes(_official([_card(1, start, 1, 2)], html=False))
    with connect(warehouse) as conn:
        conn.execute(
            "INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id) VALUES ('game', 1, ?, 1, 2, ?)",
            (start.isoformat(), _event_id(1)),
        )
    _archive(warehouse, "earlier", NOW - timedelta(minutes=20), [_event(1, start, 3, 4)])
    _archive(warehouse, "later", NOW, [_event(1, start, 1, 2)])
    result = audit.audit_schedule(DAY, fixture_path=fixture, fixture_observed_at=NOW)
    assert result["by_status"] == {"ambiguous": 1}
    assert result["provider_observed"] == result["provider_linked"] == 0


def test_explicit_zero_game_day_and_observation_clock(warehouse):
    fixture = warehouse.parent / "next.json"
    fixture.write_bytes(_official([], html=False))
    result = audit.audit_schedule(DAY, fixture_path=fixture, fixture_observed_at=NOW)
    assert result["official_calendar_games"] == 0
    assert result["fixtures"] == []
    with pytest.raises(ValueError, match="observation cannot be in the future"):
        audit.audit_schedule(
            DAY, fixture_path=fixture, fixture_observed_at=NOW + timedelta(seconds=1)
        )
    with pytest.raises(ValueError, match="requires a timezone-aware observation time"):
        audit.audit_schedule(DAY, fixture_path=fixture)


def test_fetch_uses_only_official_public_url_and_preserves_observation_metadata(
    warehouse, monkeypatch
):
    calls = []
    start = NOW + timedelta(hours=1)
    raw = _official([_card(1, start, 1, 2), _card(2, NOW, 3, 4, status=3)])

    def get(url, **kwargs):
        calls.append((url, kwargs))
        response = requests.Response()
        response.status_code = 200
        response.headers["ETag"] = '"official-etag"'
        response._content = raw
        return response

    monkeypatch.setattr(requests, "get", get)
    result = audit.audit_schedule(DAY, fetch=True)
    assert calls == [
        (
            "https://www.nba.com/games?date=2026-11-08",
            {"headers": audit._NBA_HEADERS, "timeout": 15, "allow_redirects": False},
        )
    ]
    assert result["official_calendar_games"] == 2
    assert result["scheduled_regular_season_games"] == 1
    assert result["by_status"] == {"missing_warehouse_fixture": 1}
    assert result["official_source"]["etag"] == '"official-etag"'


def test_saved_audit_retains_original_bytes_for_replay_after_restore(warehouse, monkeypatch):
    data_dir = warehouse.parent
    audits = data_dir / "audits"
    monkeypatch.setattr(audit, "AUDIT_REPORTS_DIR", audits)
    raw = _official([_card(1, NOW + timedelta(hours=1), 1, 2)])
    original = data_dir / "official.html"
    original.write_bytes(raw)

    for _ in range(2):
        result = audit.audit_schedule(DAY, fixture_path=original, fixture_observed_at=NOW, save=True)

    source = result["official_source"]
    assert source["raw_path"].startswith(f"raw/schedules/nba/{DAY}/")
    assert (data_dir / source["raw_path"]).read_bytes() == raw
    assert len(list((data_dir / "raw").rglob("*.body"))) == 1
    saved = sorted((audits / DAY.isoformat()).glob("*.json"))
    assert len(saved) == 2

    restored = data_dir / "restored"
    restored.mkdir()
    create_evidence_zip(restored / EVIDENCE_ZIP, [("raw", data_dir / "raw")])
    restored_report = json.loads(read_raw_evidence(
        f"raw/schedules/nba/audits/{DAY}/{saved[-1].name}", restored
    ))
    assert restored_report == json.loads(saved[-1].read_bytes())
    assert audit.read_official_schedule_evidence(DAY, restored_report["official_source"], restored) == raw
    (data_dir / source["raw_path"]).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="checksum mismatch"):
        audit.audit_schedule(DAY, fixture_path=original, fixture_observed_at=NOW, save=True)
    assert len(list((audits / DAY.isoformat()).glob("*.json"))) == 2
