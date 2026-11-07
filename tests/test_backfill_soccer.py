from datetime import datetime, timedelta, timezone

from src.data import backfill_soccer
from src.data.team_mappings import normalize_team_code


def test_espn_soccer_uses_only_final_scores(monkeypatch):
    now = datetime.now(timezone.utc)
    past = (now - timedelta(days=1)).isoformat()
    future = (now + timedelta(days=1)).isoformat()

    def event(event_id, kickoff, status, home_score):
        return {
            "id": event_id,
            "competitions": [{
                "date": kickoff,
                "status": {"type": {"name": status}},
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Arsenal"}, "score": home_score},
                    {"homeAway": "away", "team": {"displayName": "Chelsea"}, "score": "0"},
                ],
            }],
        }

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"events": [
                event("scheduled", future, "STATUS_SCHEDULED", "0"),
                event("live", past, "STATUS_IN_PROGRESS", "1"),
                event("future_final", future, "STATUS_FULL_TIME", "0"),
                event("final_draw", past, "STATUS_FULL_TIME", "0"),
                event("invalid", past, "STATUS_FULL_TIME", "--"),
            ]}

    monkeypatch.setattr(backfill_soccer.requests, "get", lambda *args, **kwargs: Response())
    monkeypatch.setattr(backfill_soccer, "save_raw_json", lambda *args: None)
    monkeypatch.setattr(backfill_soccer.time, "sleep", lambda *args: None)

    games = backfill_soccer.fetch_from_espn(["EPL"]).set_index("event_id")
    assert games.loc[["scheduled", "live", "future_final"], ["home_score", "away_score"]].isna().all().all()
    assert games.loc["final_draw", "home_score"] == games.loc["final_draw", "away_score"] == 0
    assert games.loc["invalid", ["home_score", "away_score"]].isna().all()


def test_team_aliases_join_espn_to_odds_api_names():
    provider_pairs = [
        ("BUNDESLIGA", "SC Paderborn 07", "SC Paderborn"),
        ("BUNDESLIGA", "SV Elversberg", "Elversberg"),
        ("BUNDESLIGA", "FC Augsburg", "Augsburg"),
        ("BUNDESLIGA", "1. FC Union Berlin", "Union Berlin"),
        ("EPL", "AFC Bournemouth", "Bournemouth"),
        ("LALIGA", "Racing Santander", "Real Racing Club de Santander"),
        ("LALIGA", "Deportivo", "Deportivo La Coruña"),
        ("LALIGA", "Athletic Club", "Athletic Bilbao"),
        ("LIGUE1", "Stade Rennais", "Rennes"),
        ("LIGUE1", "Le Mans", "Le Mans FC"),
    ]
    for league, espn, odds_api in provider_pairs:
        assert normalize_team_code(league, espn) == normalize_team_code(league, odds_api)
