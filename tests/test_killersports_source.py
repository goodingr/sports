from __future__ import annotations

from pathlib import Path

from src.data.sources import killersports

FIXTURE = Path(__file__).resolve().parent / "data" / "killersports_nhl_sample.html"


def test_extract_rows_parses_moneyline_and_totals() -> None:
    html = FIXTURE.read_text(encoding="utf-8")
    df = killersports._extract_rows(html, "NHL")  # type: ignore[attr-defined]

    assert len(df) == 3
    future_row = df[df["is_future"]].iloc[0]
    assert future_row["team"] == "Blue Jackets"
    assert future_row["moneyline"] == 155
    assert future_row["total"] == 6.5
    assert future_row["side_result"] is None

    past_row = df[~df["is_future"]].iloc[0]
    assert past_row["team"] == "Ducks"
    assert past_row["opponent"] == "Mammoth"
    assert past_row["moneyline"] == -105
    assert past_row["total_result"] == "U"
    assert past_row["team_score"] == 3
    assert past_row["opponent_score"] == 2
