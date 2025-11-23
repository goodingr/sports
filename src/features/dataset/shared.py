"""Shared utilities and constants for dataset generation."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd

from src.data.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from src.data.team_mappings import normalize_team_code
from src.db.core import connect

LOGGER = logging.getLogger(__name__)

ROLLING_WINDOWS = (3, 5)
DEFAULT_REST_DAYS = 7
PBP_COLUMNS = [
    "game_id",
    "season",
    "week",
    "season_type",
    "play_id",
    "posteam",
    "defteam",
    "epa",
    "success",
    "pass",
    "rush",
    "play_type",
]
INJURY_TEAM_COLUMNS = ["team", "club_code", "club"]
INJURY_WEEK_COLUMNS = ["week", "game_week", "gameWeek"]
INJURY_STATUS_COLUMNS = [
    "injury_game_status_desc",
    "injury_game_status",
    "injury_status",
    "status",
    "report_status",
]
INJURY_PRACTICE_COLUMNS = ["practice_status", "practice_participation"]
INJURY_PLAYER_COLUMNS = [
    "player_display_name",
    "player_name",
    "display_name",
    "name",
    "gsis_id",
]
TEAM_SKILL_POSITIONS = {"QB", "WR", "RB", "TE"}
NBA_SKILL_POSITIONS = {"G", "F", "C", "G-F", "F-G", "F-C", "C-F", "G-C"}
PRECIP_KEYWORDS = re.compile("rain|snow|sleet|storm|shower|drizzle|hail|precip", re.IGNORECASE)
SOCCER_LEAGUES = {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}
SOCCER_SEASON_MIN = 2021
SOCCER_SEASON_MAX = 2025


@dataclass
class DatasetPaths:
    seasons: Tuple[int, int]
    league: str = "NFL"

    @property
    def raw_schedules(self) -> Path:
        start, end = self.seasons
        return RAW_DATA_DIR / "results" / f"schedules_{start}_{end}.parquet"

    @property
    def raw_betting(self) -> Path:
        start, end = self.seasons
        return RAW_DATA_DIR / "results" / f"betting_{start}_{end}.parquet"

    @property
    def raw_pbp(self) -> Path:
        start, end = self.seasons
        return RAW_DATA_DIR / "results" / f"pbp_{start}_{end}.parquet"

    @property
    def raw_injuries(self) -> Path:
        start, end = self.seasons
        return RAW_DATA_DIR / "results" / f"injuries_{start}_{end}.parquet"

    @property
    def derived_dir(self) -> Path:
        return PROCESSED_DATA_DIR / "intermediate"

    @property
    def team_metrics(self) -> Path:
        start, end = self.seasons
        return self.derived_dir / f"team_metrics_{start}_{end}.parquet"

    @property
    def processed(self) -> Path:
        start, end = self.seasons
        league_tag = self.league.lower()
        return PROCESSED_DATA_DIR / "model_input" / f"moneyline_{league_tag}_{start}_{end}.parquet"


def first_present_column(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def coalesce_columns(df: pd.DataFrame, candidates: List[str], default: float | str | None = np.nan) -> pd.Series:
    for col in candidates:
        if col in df.columns:
            return df[col]
    return pd.Series(default, index=df.index)


def normalize_advanced_team_codes(df: pd.DataFrame, league: str) -> pd.DataFrame:
    if df.empty or "team" not in df.columns:
        return df
    df = df.copy()
    df["team"] = df["team"].apply(lambda name: normalize_team_code(league, str(name)))
    df = df[df["team"].astype(bool)]
    return df


def status_category_from_text(text: str) -> str:
    lowered = text.lower()
    if any(keyword in lowered for keyword in ["injured reserve", "out", "suspended", "physically unable", "non-football", "covid"]):
        return "out"
    if "doubt" in lowered:
        return "doubtful"
    if "question" in lowered or "probable" in lowered:
        return "questionable"
    return "other"


def practice_status_to_category(text: str) -> str:
    lowered = text.lower()
    if lowered.startswith("dnp") or "did not" in lowered:
        return "questionable"
    if "limited" in lowered:
        return "questionable"
    return "other"


def running_streak(values: pd.Series, active_value: int = 1) -> pd.Series:
    streak: List[int] = []
    current = 0
    for value in values.astype(int):
        if value == active_value:
            current += 1
        else:
            current = 0
        streak.append(current)
    return pd.Series(streak, index=values.index)


def win_loss_streak(outcomes: pd.Series) -> pd.Series:
    streak: List[int] = []
    current = 0
    for result in outcomes.astype(float):
        if result == 1:
            current = current + 1 if current >= 0 else 1
        elif result == 0:
            current = current - 1 if current <= 0 else -1
        else:
            current = 0
        streak.append(current)
    return pd.Series(streak, index=outcomes.index)


def mirror_game_feature(df: pd.DataFrame, column: str) -> pd.Series:
    def _swap(series: pd.Series) -> np.ndarray:
        if len(series) == 2:
            return series.iloc[::-1].to_numpy()
        return np.repeat(np.nan, len(series))

    return df.groupby("game_id")[column].transform(_swap)


def merge_opponent_features(dataset: pd.DataFrame, features: pd.DataFrame, prefix: str) -> pd.DataFrame:
    feature_cols = [col for col in features.columns if col not in {"game_id", "team"}]
    if not feature_cols:
        return dataset
    renamed = features.rename(
        columns={"team": "opponent", **{col: f"{prefix}{col}" for col in feature_cols}}
    )
    return dataset.merge(renamed, on=["game_id", "opponent"], how="left")


def add_opponent_feature_mirrors(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    updated = df.copy()
    for column in columns:
        if column in updated.columns:
            updated[f"opponent_{column}"] = mirror_game_feature(updated, column)
    return updated


def normalize_score_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "score_home" in df.columns and "score_away" in df.columns:
        return df.rename(columns={"score_home": "home_score", "score_away": "away_score"})
    if "home_score" not in df.columns or "away_score" not in df.columns:
        raise KeyError("Betting dataset missing score columns; update ingestion or rename columns accordingly")
    return df


def normalize_moneyline_columns(df: pd.DataFrame) -> pd.DataFrame:
    candidates = {
        "home_moneyline": ["home_moneyline", "moneyline_home", "ml_home"],
        "away_moneyline": ["away_moneyline", "moneyline_away", "ml_away"],
    }
    rename_map = {}
    for canonical, options in candidates.items():
        for option in options:
            if option in df.columns:
                rename_map[option] = canonical
                break
        if canonical not in rename_map.values():
            raise KeyError(f"Betting dataset missing {canonical} column")
    return df.rename(columns=rename_map)


def normalize_team_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    if "team_home" not in df.columns:
        if "home_team" in df.columns:
            rename_map["home_team"] = "team_home"
        else:
            raise KeyError("Betting dataset missing team_home column")
    if "team_away" not in df.columns:
        if "away_team" in df.columns:
            rename_map["away_team"] = "team_away"
        else:
            raise KeyError("Betting dataset missing team_away column")
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def implied_probability(moneyline: pd.Series) -> pd.Series:
    ml = moneyline.astype(float)
    implied = np.where(
        ml < 0,
        (-ml) / ((-ml) + 100.0),
        100.0 / (ml + 100.0),
    )
    return implied


def seasons_tuple(seasons: Iterable[int | str]) -> Tuple[int, int]:
    parsed = sorted({int(season) for season in seasons})
    if not parsed:
        raise ValueError("At least one season is required")
    return parsed[0], parsed[-1]


def load_injuries_from_db_league(league: str, seasons: Iterable[int]) -> pd.DataFrame:
    season_range = seasons_tuple(seasons)
    league_code = league.upper()
    with connect() as conn:
        injuries = pd.read_sql_query(
            """
            SELECT team_code,
                   team_id,
                   player_name,
                   position,
                   status,
                   practice_status,
                   report_date,
                   game_date,
                   season,
                   week
            FROM injury_reports
            WHERE league = ? AND (season IS NULL OR season BETWEEN ? AND ?)
            """,
            conn,
            params=(league_code, season_range[0], season_range[1]),
        )

    if injuries.empty:
        return injuries

    injuries["team"] = injuries.get("team_code", "").astype(str)
    injuries["report_date"] = pd.to_datetime(injuries.get("report_date"), errors="coerce")
    injuries["game_date"] = pd.to_datetime(injuries.get("game_date"), errors="coerce")

    season_guess = injuries.get("season")
    if season_guess is not None:
        injuries["season"] = pd.to_numeric(season_guess, errors="coerce")
    else:
        injuries["season"] = np.nan

    missing_season = injuries["season"].isna()
    if missing_season.any():
        fallback_dates = injuries.loc[missing_season, "game_date"].fillna(injuries.loc[missing_season, "report_date"])
        injuries.loc[missing_season, "season"] = fallback_dates.dt.year

    injuries = injuries.dropna(subset=["season"])
    injuries["season"] = injuries["season"].astype(int)
    injuries = injuries[injuries["season"].between(season_range[0], season_range[1])]
    return injuries


def summarize_injuries_by_date(injuries: pd.DataFrame, league: str) -> pd.DataFrame:
    if injuries.empty:
        return pd.DataFrame(
            columns=[
                "season",
                "game_date",
                "team",
                "injuries_out",
                "injuries_doubtful",
                "injuries_questionable",
                "injuries_total",
                "injuries_qb_out",
                "injuries_skill_out",
            ]
        )

    df = injuries.copy()
    df["team"] = df.get("team", df.get("team_code", "")).astype(str).str.upper()

    date_series = df.get("game_date")
    if date_series is None:
        date_series = df.get("report_date")
    df["game_date"] = pd.to_datetime(date_series, errors="coerce")
    fallback_date = df.get("report_date")
    if fallback_date is not None:
        fallback_date = pd.to_datetime(fallback_date, errors="coerce")
        df.loc[df["game_date"].isna(), "game_date"] = fallback_date[df["game_date"].isna()]
    df = df[df["game_date"].notna()]
    df["game_date"] = df["game_date"].dt.date

    status_series = df.get("status", "").fillna("").astype(str)
    practice_series = df.get("practice_status", "").fillna("").astype(str)
    df["status_category"] = status_series.apply(status_category_from_text)
    practice_categories = practice_series.apply(practice_status_to_category)
    mask_other = df["status_category"] == "other"
    df.loc[mask_other, "status_category"] = practice_categories[mask_other]

    df["position"] = df.get("position", "").fillna("").astype(str).str.upper()
    skill_positions = TEAM_SKILL_POSITIONS if league.upper() == "NFL" else NBA_SKILL_POSITIONS

    df["is_qb_severe"] = (
        (df["position"] == "QB") & df["status_category"].isin({"out", "doubtful"})
    ).astype(int)
    df["is_skill_severe"] = (
        df["position"].isin(skill_positions) & df["status_category"].isin({"out", "doubtful"})
    ).astype(int)

    summary = (
        df.groupby(["season", "game_date", "team"], dropna=False)
        .agg(
            injuries_out=("status_category", lambda s: int((s == "out").sum())),
            injuries_doubtful=("status_category", lambda s: int((s == "doubtful").sum())),
            injuries_questionable=("status_category", lambda s: int((s == "questionable").sum())),
            injuries_total=("status_category", lambda s: int((s != "other").sum())),
            injuries_qb_out=("is_qb_severe", "sum"),
            injuries_skill_out=("is_skill_severe", "sum"),
        )
        .reset_index()
    )

    return summary


def add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    updated = df.copy()
    temperature_series = coalesce_columns(updated, ["temperature_raw", "temperature", "temp", "temp_f"], np.nan)
    wind_series = coalesce_columns(updated, ["wind_raw", "wind", "wind_speed"], np.nan)
    weather_desc = coalesce_columns(updated, ["weather_detail", "weather"], "").astype(str)
    roof_series = coalesce_columns(updated, ["roof"], "").astype(str)

    updated["game_temperature_f"] = pd.to_numeric(temperature_series, errors="coerce")
    updated["game_wind_mph"] = pd.to_numeric(wind_series, errors="coerce")
    updated["is_weather_dome"] = roof_series.str.lower().isin({"closed", "dome", "indoors", "retractable"}).astype(int)
    updated["is_weather_precip"] = weather_desc.apply(lambda value: int(bool(PRECIP_KEYWORDS.search(value))))
    updated["is_weather_windy"] = (
        (updated["game_wind_mph"].fillna(0) >= 15)
        | weather_desc.str.contains("wind", case=False, na=False)
    ).astype(int)

    return updated


def add_rest_travel_features(df: pd.DataFrame) -> pd.DataFrame:
    updated = df.sort_values(["team", "game_datetime"]).copy()
    updated["team_rest_days"] = (
        updated.groupby("team")["game_datetime"].diff().dt.days.fillna(DEFAULT_REST_DAYS)
    )
    updated["team_rest_days"] = updated["team_rest_days"].clip(lower=0)
    updated["team_rest_days"] = updated["team_rest_days"].replace(0, DEFAULT_REST_DAYS)

    updated["is_short_week"] = (updated["team_rest_days"] <= 5).astype(int)
    updated["is_long_rest"] = (updated["team_rest_days"] >= 10).astype(int)
    updated["is_post_bye"] = (updated["team_rest_days"] >= 13).astype(int)

    updated["opponent_rest_days"] = mirror_game_feature(updated, "team_rest_days")
    updated["rest_diff"] = updated["team_rest_days"] - updated["opponent_rest_days"]

    updated["_road_indicator"] = (~updated["is_home"]).astype(int)
    updated["_home_indicator"] = updated["is_home"].astype(int)
    updated["road_trip_length"] = updated.groupby("team")["_road_indicator"].transform(running_streak)
    updated["home_stand_length"] = updated.groupby("team")["_home_indicator"].transform(running_streak)
    updated["road_trip_length_entering"] = (updated["road_trip_length"] - updated["_road_indicator"]).clip(lower=0)
    updated["home_stand_length_entering"] = (updated["home_stand_length"] - updated["_home_indicator"]).clip(lower=0)
    updated.drop(columns=["_road_indicator", "_home_indicator"], inplace=True)

    return updated.sort_values(["game_datetime", "team"])


def add_team_form_features(df: pd.DataFrame) -> pd.DataFrame:
    updated = df.copy()
    updated["points_for"] = np.where(updated["is_home"], updated["home_score"], updated["away_score"])
    updated["points_against"] = np.where(updated["is_home"], updated["away_score"], updated["home_score"])
    updated["point_diff"] = updated["points_for"] - updated["points_against"]

    for window in ROLLING_WINDOWS:
        updated[f"rolling_win_pct_{window}"] = (
            updated.groupby("team")["win"].transform(lambda s: s.shift(1).rolling(window, min_periods=1).mean())
        )
        updated[f"rolling_point_diff_{window}"] = (
            updated.groupby("team")["point_diff"].transform(lambda s: s.shift(1).rolling(window, min_periods=1).mean())
        )

    return updated


def latest_source_directory(league: str, source_subdir: str) -> Path | None:
    base = RAW_DATA_DIR / "sources" / league.lower() / source_subdir
    if not base.exists():
        return None
    candidates = sorted([path for path in base.iterdir() if path.is_dir()])
    return candidates[-1] if candidates else None


def load_latest_csv(league: str, source_subdir: str, filename: str) -> pd.DataFrame:
    directory = latest_source_directory(league, source_subdir)
    if not directory:
        return pd.DataFrame()
    path = directory / filename
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def load_latest_parquet(league: str, source_subdir: str, filename: str) -> pd.DataFrame:
    directory = latest_source_directory(league, source_subdir)
    if not directory:
        return pd.DataFrame()
    path = directory / filename
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def convert_line_to_float(value: str | float | None) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = value.strip().lower().replace("o", "").replace("u", "").replace("+", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def merge_espn_odds(dataset: pd.DataFrame, league: str) -> pd.DataFrame:
    odds = load_latest_csv(league, "espn_odds", "odds.csv")
    if odds.empty:
        for column in [
            "espn_moneyline_open",
            "espn_moneyline_close",
            "espn_spread_open",
            "espn_spread_close",
            "espn_total_open",
            "espn_total_close",
        ]:
            if column not in dataset.columns:
                dataset[column] = np.nan
        return dataset

    odds["team"] = odds["team"].astype(str).str.upper()
    odds["game_datetime"] = pd.to_datetime(odds["start_time"], utc=True, errors="coerce").dt.tz_convert(None)
    odds["espn_moneyline_open"] = odds["moneyline_open"].apply(convert_line_to_float)
    odds["espn_moneyline_close"] = odds["moneyline_close"].apply(convert_line_to_float)
    odds["espn_spread_open"] = odds["spread_open"].apply(convert_line_to_float)
    odds["espn_spread_close"] = odds["spread_close"].apply(convert_line_to_float)
    odds["espn_total_open"] = odds["total_open"].apply(convert_line_to_float)
    odds["espn_total_close"] = odds["total_close"].apply(convert_line_to_float)

    subset = odds[
        [
            "team",
            "game_datetime",
            "espn_moneyline_open",
            "espn_moneyline_close",
            "espn_spread_open",
            "espn_spread_close",
            "espn_total_open",
            "espn_total_close",
        ]
    ].dropna(subset=["game_datetime"])

    dataset["game_datetime"] = pd.to_datetime(dataset["game_datetime"], errors="coerce", utc=True).dt.tz_convert(None)
    merged = dataset.merge(subset, on=["team", "game_datetime"], how="left")
    for column in [
        "espn_moneyline_open",
        "espn_moneyline_close",
        "espn_spread_open",
        "espn_spread_close",
        "espn_total_open",
        "espn_total_close",
    ]:
        if column not in merged.columns:
            merged[column] = np.nan
    return merged


def build_base_dataset(seasons: Iterable[int], league: str) -> pd.DataFrame:
    from src.data.config import ensure_directories
    ensure_directories()
    league_code = league.upper()
    season_list = sorted({int(season) for season in seasons})
    if not season_list:
        LOGGER.warning("No seasons provided for %s", league_code)
        return pd.DataFrame()
    if league_code in SOCCER_LEAGUES:
        season_list = [
            season
            for season in season_list
            if SOCCER_SEASON_MIN <= season <= SOCCER_SEASON_MAX
        ]
        if not season_list:
            LOGGER.warning(
                "Requested seasons fall outside supported soccer window %s-%s",
                SOCCER_SEASON_MIN,
                SOCCER_SEASON_MAX,
            )
            return pd.DataFrame()
    season_range = (season_list[0], season_list[-1])

    with connect() as conn:
        sport_row = conn.execute(
            "SELECT sport_id FROM sports WHERE league = ?",
            (league_code,),
        ).fetchone()
        if not sport_row:
            raise RuntimeError(f"No sport data found in database for league {league_code}")
        sport_id = sport_row[0]

        games = pd.read_sql_query(
            """
            SELECT g.game_id,
                   g.season,
                   g.week,
                   g.game_type,
                   g.start_time_utc,
                   ht.code AS home_team,
                   at.code AS away_team,
                   r.home_score,
                   r.away_score,
                   r.home_moneyline_close,
                   r.away_moneyline_close,
                   r.spread_close,
                   r.total_close
            FROM games g
            JOIN teams ht ON ht.team_id = g.home_team_id
            JOIN teams at ON at.team_id = g.away_team_id
            LEFT JOIN game_results r ON r.game_id = g.game_id
            WHERE g.sport_id = ? AND g.season BETWEEN ? AND ?
            """,
            conn,
            params=(sport_id, season_range[0], season_range[1]),
        )

        games_lookup = games[["game_id", "season", "start_time_utc", "home_team", "away_team"]].copy()

        odds = pd.read_sql_query(
            """
            SELECT o.game_id,
                   o.outcome,
                   o.price_american,
                   s.fetched_at_utc
            FROM odds o
            JOIN odds_snapshots s ON s.snapshot_id = o.snapshot_id
            JOIN games g ON g.game_id = o.game_id
            WHERE g.sport_id = ? AND o.market = 'h2h'
            """,
            conn,
            params=(sport_id,),
        )

    if games.empty:
        LOGGER.warning("No games found for %s seasons %s", league_code, season_range)
        return pd.DataFrame()

    games["start_time_utc"] = pd.to_datetime(games["start_time_utc"], errors="coerce", utc=True)

    if not odds.empty:
        odds["fetched_at_utc"] = pd.to_datetime(odds["fetched_at_utc"], errors="coerce", utc=True)
        odds = odds.dropna(subset=["fetched_at_utc", "price_american"])
        odds.sort_values("fetched_at_utc", inplace=True)

        games_index = games.set_index("game_id")
        closing_map: dict[tuple[str, str], float] = {}
        for (game_id, outcome), group in odds.groupby(["game_id", "outcome"]):
            outcome_key = str(outcome).lower()
            start_time = games_index.loc[game_id, "start_time_utc"] if game_id in games_index.index else None
            valid = group
            if pd.notna(start_time):
                valid = group[group["fetched_at_utc"] <= start_time]
                if valid.empty:
                    valid = group
            closing_map[(game_id, outcome_key)] = valid.iloc[-1]["price_american"]

        for outcome_key, column in (("home", "home_moneyline_close"), ("away", "away_moneyline_close")):
            mask = games[column].isna()
            if mask.any():
                fill_values = games.loc[mask, "game_id"].map(
                    lambda gid: closing_map.get((gid, outcome_key))
                )
                fill_values = pd.to_numeric(fill_values, errors="coerce")
                games.loc[mask, column] = fill_values

    # For NBA, match odds from database by game_id
    if league_code == "NBA" and not odds.empty:
        odds_by_game = {}
        for (game_id, outcome), group in odds.groupby(["game_id", "outcome"]):
            outcome_key = str(outcome).lower()
            if outcome_key in ("home", "away"):
                latest_price = group.iloc[-1]["price_american"]
                if game_id not in odds_by_game:
                    odds_by_game[game_id] = {}
                odds_by_game[game_id][outcome_key] = latest_price
        
        for outcome_key, column in (("home", "home_moneyline_close"), ("away", "away_moneyline_close")):
            mask = games[column].isna()
            if mask.any():
                fill_values = games.loc[mask, "game_id"].map(
                    lambda gid: odds_by_game.get(gid, {}).get(outcome_key) if pd.notna(gid) else None
                )
                fill_values = pd.to_numeric(fill_values, errors="coerce")
                games.loc[mask, column] = fill_values

    games = games.dropna(subset=["home_score", "away_score"])
    games["game_datetime"] = games["start_time_utc"]
    games["game_type"] = games.get("game_type", "REG").fillna("REG")

    home = games.assign(
        team=games["home_team"],
        opponent=games["away_team"],
        moneyline=games["home_moneyline_close"],
        implied_prob=implied_probability(games["home_moneyline_close"]),
        is_home=True,
        win=(games["home_score"] > games["away_score"]).astype(int),
        spread_line=games.get("spread_close"),
        total_line=games.get("total_close"),
        league=league_code,
    )

    away = games.assign(
        team=games["away_team"],
        opponent=games["home_team"],
        moneyline=games["away_moneyline_close"],
        implied_prob=implied_probability(games["away_moneyline_close"]),
        is_home=False,
        win=(games["away_score"] > games["home_score"]).astype(int),
        spread_line=games.get("spread_close"),
        total_line=games.get("total_close"),
        league=league_code,
    )

    dataset = pd.concat([home, away], ignore_index=True)
    dataset = dataset[
        [
            "game_id",
            "game_datetime",
            "season",
            "week",
            "game_type",
            "team",
            "opponent",
            "is_home",
            "moneyline",
            "implied_prob",
            "spread_line",
            "total_line",
            "win",
            "league",
            "home_score",
            "away_score",
        ]
    ].copy()

    dataset.sort_values(["game_datetime", "season", "team"], inplace=True)

    injuries = load_injuries_from_db_league(league_code, season_list)
    if not injuries.empty:
        injury_summary = summarize_injuries_by_date(injuries, league_code)
        dataset["game_date"] = pd.to_datetime(dataset["game_datetime"], errors="coerce").dt.date
        dataset = dataset.merge(
            injury_summary,
            on=["season", "game_date", "team"],
            how="left",
        )
        for col in [
            "injuries_out",
            "injuries_doubtful",
            "injuries_questionable",
            "injuries_total",
            "injuries_qb_out",
            "injuries_skill_out",
        ]:
            if col in dataset.columns:
                dataset[col] = dataset[col].fillna(0)
        dataset["game_date"].fillna(pd.NaT, inplace=True)
    else:
        dataset["game_date"] = pd.to_datetime(dataset["game_datetime"], errors="coerce").dt.date

    return dataset
