"""Exercise the frozen archive and model boundaries without production outcomes."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from reports.research.ncaab_imbalance_offset import experiment

FETCHED = datetime(2026, 11, 5, 22, tzinfo=timezone.utc)
TIP = FETCHED + timedelta(hours=2)
EVENT_ID = "a" * 32
TEAMS = {"home_team": "Kentucky Wildcats", "away_team": "Duke Blue Devils"}


@pytest.fixture
def protocol():
    return json.loads(experiment.PROTOCOL.read_text(encoding="utf-8"))


def test_zero_and_lower_bounds_are_exact():
    offsets = np.zeros(12)
    imbalance = np.full(12, 2.0)
    assert experiment._fit_beta(offsets, imbalance, np.ones(12), -1, 0, 5) == 0
    assert experiment._fit_beta(offsets, imbalance, np.zeros(12), -1, 0, 0.1) == -1


def test_interior_optimum_penalizes_summed_loss():
    offsets = np.zeros(12)
    imbalance = np.full(12, 2.0)
    labels = np.r_[np.ones(4), np.zeros(8)]
    beta = experiment._fit_beta(offsets, imbalance, labels, -1, 0, 5)
    assert -1 < beta < 0
    p = 1 / (1 + np.exp(-2 * beta))
    assert abs((p * 12 - 4) * 2 + 5 * beta) < 1e-10


@pytest.mark.parametrize("age", [timedelta(seconds=-1), None, timedelta(minutes=10, seconds=1)])
def test_book_timestamp_cannot_substitute_for_market_timestamp(age):
    book = {
        "last_update": FETCHED.isoformat(),
        "markets": [{
            "key": "totals",
            "last_update": (FETCHED - age).isoformat() if age is not None else None,
            "outcomes": [
                {"name": "Over", "point": 142.5, "price": -110},
                {"name": "Under", "point": 142.5, "price": -110},
            ],
        }],
    }
    assert experiment._market(book, "totals", ("over", "under"), FETCHED, timedelta(minutes=10)) is None
    book["markets"][0]["last_update"] = (FETCHED - timedelta(minutes=10)).isoformat()
    assert experiment._market(book, "totals", ("over", "under"), FETCHED, timedelta(minutes=10)) == (0.5, 142.5)


def _final(**changes):
    return {
        "id": EVENT_ID,
        "sport_key": "basketball_ncaab",
        "commence_time": TIP.isoformat(),
        "completed": True,
        "scores": [
            {"name": TEAMS["home_team"], "score": "78"},
            {"name": TEAMS["away_team"], "score": "68"},
        ],
        **TEAMS,
        **changes,
    }


def _archive(tmp_path: Path, observed: datetime, event: dict):
    name = observed.strftime("scores_%Y-%m-%dT%H-%M-%SZ.json")
    path = tmp_path / name
    path.write_text(json.dumps([event]), encoding="utf-8")
    return path


def _cohort():
    teams = experiment._teams(TEAMS, "NCAAB")
    assert teams is not None
    quote = experiment.Quote(EVENT_ID, teams, TIP, FETCHED, 142.5, 0.5, 0.75)
    return {quote.event_id: quote}


def test_later_scores_file_is_never_opened(tmp_path, monkeypatch, protocol):
    cutoff = TIP + timedelta(hours=8)
    _archive(tmp_path, TIP + timedelta(hours=4), _final())
    forbidden = _archive(tmp_path, cutoff, _final())
    read_bytes = Path.read_bytes

    def read_before_cutoff(path):
        assert path != forbidden
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", read_before_cutoff)
    grades, counts = experiment._score_evidence(tmp_path, _cohort(), protocol, FETCHED, cutoff)
    assert grades == {EVENT_ID: 146}
    assert counts["development_score_receipts"] == 1


def test_conflicting_and_wrong_identity_grades_stay_unresolved(tmp_path, protocol):
    _archive(tmp_path, TIP + timedelta(hours=3), _final(home_team=TEAMS["away_team"], away_team=TEAMS["home_team"]))
    grades, counts = experiment._score_evidence(
        tmp_path, _cohort(), protocol, FETCHED, TIP + timedelta(hours=8)
    )
    assert not grades
    assert counts["missing_score_fixtures"] == 1
    _archive(tmp_path, TIP + timedelta(hours=4), _final())
    _archive(tmp_path, TIP + timedelta(hours=5), _final(scores=[
        {"name": TEAMS["home_team"], "score": "80"},
        {"name": TEAMS["away_team"], "score": "68"},
    ]))
    grades, counts = experiment._score_evidence(
        tmp_path, _cohort(), protocol, FETCHED, TIP + timedelta(hours=8)
    )
    assert not grades
    assert counts["ambiguous_score_fixtures"] == 1
