"""Transform betting data into a modeling dataset for NFL and NBA."""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]

try:
    import nfl_data_py as nfl  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "nfl_data_py is required for feature generation. Install it with `poetry add nfl-data-py`."
    ) from exc

from src.data.config import PROCESSED_DATA_DIR, RAW_DATA_DIR, ensure_directories
from src.data.nfl import get_team_conference, get_team_division
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


def _first_present_column(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _coalesce_columns(df: pd.DataFrame, candidates: List[str], default: float | str | None = np.nan) -> pd.Series:
    for col in candidates:
        if col in df.columns:
            return df[col]
    return pd.Series(default, index=df.index)


def _status_category_from_text(text: str) -> str:
    lowered = text.lower()
    if any(keyword in lowered for keyword in ["injured reserve", "out", "suspended", "physically unable", "non-football", "covid"]):
        return "out"
    if "doubt" in lowered:
        return "doubtful"
    if "question" in lowered or "probable" in lowered:
        return "questionable"
    return "other"


def _practice_status_to_category(text: str) -> str:
    lowered = text.lower()
    if lowered.startswith("dnp") or "did not" in lowered:
        return "questionable"
    if "limited" in lowered:
        return "questionable"
    return "other"


def _running_streak(values: pd.Series, active_value: int = 1) -> pd.Series:
    streak: List[int] = []
    current = 0
    for value in values.astype(int):
        if value == active_value:
            current += 1
        else:
            current = 0
        streak.append(current)
    return pd.Series(streak, index=values.index)


def _win_loss_streak(outcomes: pd.Series) -> pd.Series:
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


def _mirror_game_feature(df: pd.DataFrame, column: str) -> pd.Series:
    def _swap(series: pd.Series) -> np.ndarray:
        if len(series) == 2:
            return series.iloc[::-1].to_numpy()
        return np.repeat(np.nan, len(series))

    return df.groupby("game_id")[column].transform(_swap)


def _normalize_score_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "score_home" in df.columns and "score_away" in df.columns:
        return df.rename(columns={"score_home": "home_score", "score_away": "away_score"})
    if "home_score" not in df.columns or "away_score" not in df.columns:
        raise KeyError("Betting dataset missing score columns; update ingestion or rename columns accordingly")
    return df


def _normalize_moneyline_columns(df: pd.DataFrame) -> pd.DataFrame:
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


def _normalize_team_columns(df: pd.DataFrame) -> pd.DataFrame:
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


def _implied_probability(moneyline: pd.Series) -> pd.Series:
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


def _load_schedules(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    if paths.raw_schedules.exists():
        LOGGER.info("Loading cached schedules data from %s", paths.raw_schedules)
        schedules = pd.read_parquet(paths.raw_schedules)
    else:
        LOGGER.info("Cached schedules not found. Downloading via nfl_data_py.import_schedules")
        schedules = nfl.import_schedules(list(seasons))
        paths.raw_schedules.parent.mkdir(parents=True, exist_ok=True)
        schedules.to_parquet(paths.raw_schedules, index=False)

    if not paths.raw_betting.exists():
        subset_columns = [
            "season",
            "game_type",
            "week",
            "gameday",
            "weekday",
            "gametime",
            "away_team",
            "home_team",
            "away_score",
            "home_score",
            "away_moneyline",
            "home_moneyline",
            "spread_line",
            "away_spread_odds",
            "home_spread_odds",
            "total_line",
            "under_odds",
            "over_odds",
            "game_id",
            "gsis",
            "pfr",
        ]
        betting_snapshot = schedules[list(set(subset_columns) & set(schedules.columns))].copy()
        paths.raw_betting.parent.mkdir(parents=True, exist_ok=True)
        betting_snapshot.to_parquet(paths.raw_betting, index=False)

    return schedules


def _load_pbp(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    if paths.raw_pbp.exists():
        LOGGER.info("Loading cached play-by-play data from %s", paths.raw_pbp)
        return pd.read_parquet(paths.raw_pbp)

    LOGGER.info("Downloading play-by-play data for seasons %s", seasons)
    pbp_frames: List[pd.DataFrame] = []
    for season in seasons:
        season_data = nfl.import_pbp_data([season])
        pbp_frames.append(season_data)
    pbp = pd.concat(pbp_frames, ignore_index=True)
    pbp = pbp[[col for col in PBP_COLUMNS if col in pbp.columns]].copy()
    paths.raw_pbp.parent.mkdir(parents=True, exist_ok=True)
    pbp.to_parquet(paths.raw_pbp, index=False)
    return pbp


def _team_game_metrics(pbp: pd.DataFrame) -> pd.DataFrame:
    if pbp.empty:
        return pd.DataFrame(
            columns=[
                "season",
                "week",
                "game_id",
                "team",
                "off_epa_per_play",
                "off_success_rate",
                "def_epa_per_play",
                "def_success_rate",
            ]
        )

    plays = pbp.copy()
    if "pass" in plays.columns and "rush" in plays.columns:
        pass_mask = plays["pass"].fillna(0).astype(int) == 1
        rush_mask = plays["rush"].fillna(0).astype(int) == 1
        plays = plays[pass_mask | rush_mask]
    if "posteam" in plays.columns:
        plays = plays[plays["posteam"].notna()]

    plays["success"] = pd.to_numeric(plays.get("success"), errors="coerce")
    plays["epa"] = pd.to_numeric(plays.get("epa"), errors="coerce")

    offense_group = (
        plays.groupby(["season", "week", "game_id", "posteam"], dropna=False)
        .agg(
            off_plays=("play_id", "count"),
            off_total_epa=("epa", "sum"),
            off_success_rate=("success", "mean"),
            off_pass_attempts=("pass", "sum") if "pass" in plays.columns else ("play_id", "count"),
        )
        .reset_index()
    )
    offense_group.rename(columns={"posteam": "team"}, inplace=True)
    offense_group["off_epa_per_play"] = offense_group["off_total_epa"] / offense_group["off_plays"].replace(0, np.nan)
    if "off_pass_attempts" in offense_group.columns:
        offense_group["off_pass_rate"] = offense_group["off_pass_attempts"] / offense_group["off_plays"].replace(0, np.nan)
        offense_group.drop(columns=["off_pass_attempts"], inplace=True)
    offense_group.drop(columns=["off_total_epa"], inplace=True)

    defense_group = (
        plays.groupby(["season", "week", "game_id", "defteam"], dropna=False)
        .agg(
            def_plays=("play_id", "count"),
            def_total_epa=("epa", "sum"),
            def_success_allowed=("success", "mean"),
        )
        .reset_index()
    )
    defense_group.rename(columns={"defteam": "team"}, inplace=True)
    defense_group["def_epa_per_play"] = -defense_group["def_total_epa"] / defense_group["def_plays"].replace(0, np.nan)
    defense_group["def_success_rate"] = 1 - defense_group["def_success_allowed"]
    defense_group.drop(columns=["def_total_epa", "def_success_allowed"], inplace=True)

    metrics = offense_group.merge(
        defense_group,
        on=["season", "week", "game_id", "team"],
        how="outer",
    )

    metrics.sort_values(["season", "week", "game_id", "team"], inplace=True)
    return metrics


def _load_team_metrics(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    if paths.team_metrics.exists():
        LOGGER.info("Loading cached team metrics from %s", paths.team_metrics)
        return pd.read_parquet(paths.team_metrics)

    pbp = _load_pbp(paths, seasons)
    metrics = _team_game_metrics(pbp)
    paths.team_metrics.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_parquet(paths.team_metrics, index=False)
    return metrics


def _load_injuries_from_db_nfl(seasons: Iterable[int]) -> pd.DataFrame:
    season_range = seasons_tuple(seasons)
    with connect() as conn:
        injuries = pd.read_sql_query(
            """
            SELECT season,
                   week,
                   team_code AS team,
                   player_name,
                   position,
                   status,
                   practice_status,
                   report_date,
                   game_date
            FROM injury_reports
            WHERE league = 'NFL' AND season BETWEEN ? AND ?
            """,
            conn,
            params=(season_range[0], season_range[1]),
        )
    return injuries


def _load_injuries_from_db_league(league: str, seasons: Iterable[int]) -> pd.DataFrame:
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


def _load_injuries(paths: DatasetPaths, seasons: Iterable[int], league: str = "NFL") -> pd.DataFrame:
    league_code = league.upper()
    if league_code == "NFL":
        injuries = _load_injuries_from_db_nfl(seasons)
        if not injuries.empty:
            return injuries

    if paths.raw_injuries.exists():
        LOGGER.info("Loading cached injury reports from %s", paths.raw_injuries)
        return pd.read_parquet(paths.raw_injuries)

    LOGGER.info("Downloading injury reports for seasons %s", seasons)
    injuries = nfl.import_injuries(list(seasons))
    injuries = injuries.copy()
    paths.raw_injuries.parent.mkdir(parents=True, exist_ok=True)
    injuries.to_parquet(paths.raw_injuries, index=False)
    return injuries


def _summarize_weekly_injuries(injuries: pd.DataFrame) -> pd.DataFrame:
    if injuries.empty:
        return pd.DataFrame(
            columns=[
                "season",
                "week",
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

    team_col = _first_present_column(df, INJURY_TEAM_COLUMNS)
    if team_col is None:
        raise KeyError("Injury dataset missing team column")
    df = df.rename(columns={team_col: "team"})

    week_col = _first_present_column(df, INJURY_WEEK_COLUMNS)
    if week_col is None:
        raise KeyError("Injury dataset missing week column")
    df["week"] = pd.to_numeric(df[week_col], errors="coerce")
    df = df[df["week"].notna()]

    if "season" in df.columns:
        df["season"] = pd.to_numeric(df["season"], errors="coerce")
        df = df[df["season"].notna()]

    status_col = _first_present_column(df, INJURY_STATUS_COLUMNS)
    status_series = df[status_col].fillna("").astype(str) if status_col else pd.Series("", index=df.index)

    practice_col = _first_present_column(df, INJURY_PRACTICE_COLUMNS)
    practice_series = df[practice_col].fillna("").astype(str) if practice_col else pd.Series("", index=df.index)

    df["status_category"] = status_series.apply(_status_category_from_text)
    practice_categories = practice_series.apply(_practice_status_to_category)
    df.loc[df["status_category"] == "other", "status_category"] = practice_categories[df["status_category"] == "other"]

    player_col = _first_present_column(df, INJURY_PLAYER_COLUMNS)
    if player_col is None:
        player_col = "player_id"
    df.rename(columns={player_col: "player_identifier"}, inplace=True)

    df["position"] = df.get("position", "").astype(str).str.upper()
    df["severity_rank"] = df["status_category"].map({"out": 3, "doubtful": 2, "questionable": 1, "other": 0})

    df = df.sort_values("severity_rank", ascending=False)
    df = df.drop_duplicates(subset=["season", "week", "team", "player_identifier"], keep="first")

    df["is_qb_severe"] = ((df["position"] == "QB") & df["status_category"].isin({"out", "doubtful"})).astype(int)
    df["is_skill_severe"] = (
        df["position"].isin(TEAM_SKILL_POSITIONS)
        & df["status_category"].isin({"out", "doubtful", "questionable"})
    ).astype(int)

    summary = (
        df.groupby(["season", "week", "team"], dropna=False)
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

    summary["season"] = summary["season"].astype(int)
    summary["week"] = summary["week"].astype(int)

    return summary


def _summarize_injuries_by_date(injuries: pd.DataFrame, league: str) -> pd.DataFrame:
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
    df["status_category"] = status_series.apply(_status_category_from_text)
    practice_categories = practice_series.apply(_practice_status_to_category)
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


def _latest_source_directory(league: str, source_subdir: str) -> Path | None:
    base = RAW_DATA_DIR / "sources" / league.lower() / source_subdir
    if not base.exists():
        return None
    candidates = sorted([path for path in base.iterdir() if path.is_dir()])
    return candidates[-1] if candidates else None


def _load_latest_csv(league: str, source_subdir: str, filename: str) -> pd.DataFrame:
    directory = _latest_source_directory(league, source_subdir)
    if not directory:
        return pd.DataFrame()
    path = directory / filename
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _load_latest_parquet(league: str, source_subdir: str, filename: str) -> pd.DataFrame:
    directory = _latest_source_directory(league, source_subdir)
    if not directory:
        return pd.DataFrame()
    path = directory / filename
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _convert_line_to_float(value: str | float | None) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = value.strip().lower().replace("o", "").replace("u", "").replace("+", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _merge_espn_odds(dataset: pd.DataFrame, league: str) -> pd.DataFrame:
    odds = _load_latest_csv(league, "espn_odds", "odds.csv")
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
    odds["espn_moneyline_open"] = odds["moneyline_open"].apply(_convert_line_to_float)
    odds["espn_moneyline_close"] = odds["moneyline_close"].apply(_convert_line_to_float)
    odds["espn_spread_open"] = odds["spread_open"].apply(_convert_line_to_float)
    odds["espn_spread_close"] = odds["spread_close"].apply(_convert_line_to_float)
    odds["espn_total_open"] = odds["total_open"].apply(_convert_line_to_float)
    odds["espn_total_close"] = odds["total_close"].apply(_convert_line_to_float)

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

    dataset["game_datetime"] = pd.to_datetime(dataset["game_datetime"], errors="coerce")
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


def _merge_team_metrics(dataset: pd.DataFrame, league: str) -> pd.DataFrame:
    if league.upper() == "NBA":
        metrics = _load_latest_parquet("nba", "team_metrics", "team_metrics.parquet")
        if metrics.empty:
            return dataset
        if "team" in metrics.columns:
            metrics["team"] = metrics["team"].astype(str)
        elif "TEAM_ABBREVIATION" in metrics.columns:
            metrics["team"] = metrics["TEAM_ABBREVIATION"].astype(str)
        else:
            return dataset
        metrics = metrics[["season", "team", "E_OFF_RATING", "E_DEF_RATING", "E_NET_RATING", "E_PACE"]]
        return dataset.merge(metrics, on=["season", "team"], how="left")

    if league.upper() == "NFL":
        metrics = _load_latest_parquet("nfl", "team_metrics", "team_metrics.parquet")
        if metrics.empty:
            return dataset
        metrics["team"] = metrics["team"].astype(str)
        metrics = metrics[["season", "team", "off_epa_per_play", "off_success_rate", "def_epa_per_play", "def_success_rate"]]
        metrics = metrics.rename(
            columns={
                "off_epa_per_play": "season_off_epa_per_play",
                "off_success_rate": "season_off_success_rate",
                "def_epa_per_play": "season_def_epa_per_play",
                "def_success_rate": "season_def_success_rate",
            }
        )
        return dataset.merge(metrics, on=["season", "team"], how="left")

    return dataset


def _add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    updated = df.copy()
    temperature_series = _coalesce_columns(updated, ["temperature_raw", "temperature", "temp", "temp_f"], np.nan)
    wind_series = _coalesce_columns(updated, ["wind_raw", "wind", "wind_speed"], np.nan)
    weather_desc = _coalesce_columns(updated, ["weather_detail", "weather"], "").astype(str)
    roof_series = _coalesce_columns(updated, ["roof"], "").astype(str)

    updated["game_temperature_f"] = pd.to_numeric(temperature_series, errors="coerce")
    updated["game_wind_mph"] = pd.to_numeric(wind_series, errors="coerce")
    updated["is_weather_dome"] = roof_series.str.lower().isin({"closed", "dome", "indoors", "retractable"}).astype(int)
    updated["is_weather_precip"] = weather_desc.apply(lambda value: int(bool(PRECIP_KEYWORDS.search(value))))
    updated["is_weather_windy"] = (
        (updated["game_wind_mph"].fillna(0) >= 15)
        | weather_desc.str.contains("wind", case=False, na=False)
    ).astype(int)

    return updated


def _add_rest_travel_features(df: pd.DataFrame) -> pd.DataFrame:
    updated = df.sort_values(["team", "game_datetime"]).copy()
    updated["team_rest_days"] = (
        updated.groupby("team")["game_datetime"].diff().dt.days.fillna(DEFAULT_REST_DAYS)
    )
    updated["team_rest_days"] = updated["team_rest_days"].clip(lower=0)
    updated["team_rest_days"] = updated["team_rest_days"].replace(0, DEFAULT_REST_DAYS)

    updated["is_short_week"] = (updated["team_rest_days"] <= 5).astype(int)
    updated["is_long_rest"] = (updated["team_rest_days"] >= 10).astype(int)
    updated["is_post_bye"] = (updated["team_rest_days"] >= 13).astype(int)

    updated["opponent_rest_days"] = _mirror_game_feature(updated, "team_rest_days")
    updated["rest_diff"] = updated["team_rest_days"] - updated["opponent_rest_days"]

    updated["_road_indicator"] = (~updated["is_home"]).astype(int)
    updated["_home_indicator"] = updated["is_home"].astype(int)
    updated["road_trip_length"] = updated.groupby("team")["_road_indicator"].transform(_running_streak)
    updated["home_stand_length"] = updated.groupby("team")["_home_indicator"].transform(_running_streak)
    updated["road_trip_length_entering"] = (updated["road_trip_length"] - updated["_road_indicator"]).clip(lower=0)
    updated["home_stand_length_entering"] = (updated["home_stand_length"] - updated["_home_indicator"]).clip(lower=0)
    updated.drop(columns=["_road_indicator", "_home_indicator"], inplace=True)

    return updated.sort_values(["game_datetime", "team"])


def _add_team_form_features(df: pd.DataFrame) -> pd.DataFrame:
    updated = df.copy()
    updated["points_for"] = np.where(updated["is_home"], updated["home_score"], updated["away_score"])
    updated["points_against"] = np.where(updated["is_home"], updated["away_score"], updated["home_score"])
    updated["point_diff"] = updated["points_for"] - updated["points_against"]

    updated["rolling_win_pct_3"] = (
        updated.groupby("team")["win"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    )
    updated["rolling_win_pct_5"] = (
        updated.groupby("team")["win"].transform(lambda s: s.shift(1).rolling(5, min_periods=1).mean())
    )
    updated["rolling_point_diff_3"] = (
        updated.groupby("team")["point_diff"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    )
    updated["rolling_point_diff_5"] = (
        updated.groupby("team")["point_diff"].transform(lambda s: s.shift(1).rolling(5, min_periods=1).mean())
    )
    updated["win_streak"] = (
        updated.groupby("team")["win"].transform(_win_loss_streak).shift(1).fillna(0)
    )

    return updated


def _add_rolling_performance_features(df: pd.DataFrame) -> pd.DataFrame:
    updated = df.copy()
    metric_columns = [
        "off_epa_per_play",
        "off_success_rate",
        "off_pass_rate",
        "def_epa_per_play",
        "def_success_rate",
    ]
    for column in metric_columns:
        if column not in updated.columns:
            continue
        updated[f"{column}_prev"] = updated.groupby("team")[column].shift(1)
        for window in ROLLING_WINDOWS:
            updated[f"{column}_rolling_{window}"] = (
                updated.groupby("team")[column]
                .transform(lambda s: s.shift(1).rolling(window, min_periods=1).mean())
            )
    return updated


def _add_opponent_feature_mirrors(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    updated = df.copy()
    for column in columns:
        if column in updated.columns:
            updated[f"opponent_{column}"] = _mirror_game_feature(updated, column)
    return updated


def build_dataset(seasons: Iterable[int], league: str = "NFL") -> pd.DataFrame:
    if league.upper() != "NFL":
        return _build_dataset_generic(seasons, league)

    ensure_directories()
    season_range = seasons_tuple(seasons)
    season_list = list(range(season_range[0], season_range[1] + 1))
    paths = DatasetPaths(season_range, league="NFL")

    schedules = _load_schedules(paths, season_list)
    schedules = _normalize_score_columns(schedules)
    schedules = _normalize_moneyline_columns(schedules)
    schedules = _normalize_team_columns(schedules)

    schedules = schedules.dropna(subset=["home_moneyline", "away_moneyline"]).reset_index(drop=True)

    if "game_id" not in schedules.columns:
        if "gsis_id" in schedules.columns:
            schedules["game_id"] = schedules["gsis_id"]
        else:
            schedules["game_id"] = (
                schedules["season"].astype(str)
                + "_"
                + schedules["week"].astype(str)
                + "_"
                + schedules["team_home"].astype(str)
            )

    if "schedule_date" in schedules.columns:
        schedules["game_datetime"] = pd.to_datetime(schedules["schedule_date"])
    elif "gameday" in schedules.columns:
        schedules["game_datetime"] = pd.to_datetime(schedules["gameday"])
    else:
        schedules["game_datetime"] = pd.NaT

    game_type_series = _coalesce_columns(schedules, ["game_type", "season_type"], "REG").astype(str).str.upper()
    weekday_series = _coalesce_columns(schedules, ["weekday"], "")
    stadium_series = _coalesce_columns(schedules, ["stadium", "stadium_name"], "")
    surface_series = _coalesce_columns(schedules, ["surface"], "")
    roof_series = _coalesce_columns(schedules, ["roof"], "")
    weather_series = _coalesce_columns(schedules, ["weather", "weather_detail"], "")
    temperature_series = _coalesce_columns(schedules, ["temperature", "temp", "temp_f"], np.nan)
    wind_series = _coalesce_columns(schedules, ["wind", "wind_speed"], np.nan)
    humidity_series = _coalesce_columns(schedules, ["humidity"], np.nan)
    gametime_series = _coalesce_columns(schedules, ["gametime", "game_time"], "")

    home = schedules.assign(
        team=schedules["team_home"],
        opponent=schedules["team_away"],
        moneyline=schedules["home_moneyline"],
        implied_prob=_implied_probability(schedules["home_moneyline"]),
        is_home=True,
        win=(schedules["home_score"] > schedules["away_score"]).astype(int),
        game_type=game_type_series,
        weekday=weekday_series,
        stadium=stadium_series,
        surface=surface_series,
        roof=roof_series,
        weather_detail=weather_series,
        temperature_raw=temperature_series,
        wind_raw=wind_series,
        humidity_raw=humidity_series,
        game_time=gametime_series,
    )

    away = schedules.assign(
        team=schedules["team_away"],
        opponent=schedules["team_home"],
        moneyline=schedules["away_moneyline"],
        implied_prob=_implied_probability(schedules["away_moneyline"]),
        is_home=False,
        win=(schedules["away_score"] > schedules["home_score"]).astype(int),
        game_type=game_type_series,
        weekday=weekday_series,
        stadium=stadium_series,
        surface=surface_series,
        roof=roof_series,
        weather_detail=weather_series,
        temperature_raw=temperature_series,
        wind_raw=wind_series,
        humidity_raw=humidity_series,
        game_time=gametime_series,
    )

    dataset = pd.concat([home, away], ignore_index=True)
    dataset["season"] = pd.to_numeric(dataset["season"], errors="coerce").astype(int)
    dataset["week"] = pd.to_numeric(dataset["week"], errors="coerce").astype(int)
    dataset["league"] = "NFL"

    optional_columns = {
        "spread_line": ["spread_line", "spread_close"],
        "total_line": ["total_line", "total_close", "over_under_line"],
    }
    for canonical, options in optional_columns.items():
        for option in options:
            if option in dataset.columns:
                dataset[canonical] = dataset[option]
                break
        if canonical not in dataset.columns:
            dataset[canonical] = np.nan

    metrics = _load_team_metrics(paths, season_list)
    dataset = dataset.merge(metrics, on=["season", "week", "game_id", "team"], how="left")

    injuries = _load_injuries(paths, season_list, league="NFL")
    injury_summary = _summarize_weekly_injuries(injuries)
    dataset = dataset.merge(injury_summary, on=["season", "week", "team"], how="left")

    for injury_col in [
        "injuries_out",
        "injuries_doubtful",
        "injuries_questionable",
        "injuries_total",
        "injuries_qb_out",
        "injuries_skill_out",
    ]:
        if injury_col in dataset.columns:
            dataset[injury_col] = dataset[injury_col].fillna(0.0)
        else:
            dataset[injury_col] = 0.0

    dataset = _merge_espn_odds(dataset, "NFL")
    dataset = _merge_team_metrics(dataset, "NFL")
    dataset = _add_team_form_features(dataset)
    dataset = _add_rest_travel_features(dataset)
    dataset = _add_weather_features(dataset)
    dataset = _add_rolling_performance_features(dataset)

    mirror_columns = [
        "off_epa_per_play",
        "off_epa_per_play_rolling_3",
        "off_epa_per_play_rolling_5",
        "off_success_rate",
        "off_success_rate_rolling_3",
        "off_success_rate_rolling_5",
        "off_pass_rate",
        "def_epa_per_play_rolling_3",
        "def_epa_per_play_rolling_5",
        "def_epa_per_play",
        "def_success_rate",
        "def_success_rate_rolling_3",
        "def_success_rate_rolling_5",
        "rolling_win_pct_3",
        "rolling_win_pct_5",
        "rolling_point_diff_3",
        "rolling_point_diff_5",
        "win_streak",
        "is_short_week",
        "is_post_bye",
        "injuries_out",
        "injuries_qb_out",
        "injuries_skill_out",
    ]
    dataset = _add_opponent_feature_mirrors(dataset, mirror_columns)

    dataset["game_type"] = dataset.get("game_type", "REG").fillna("REG").astype(str).str.upper()
    playoff_mask = ~dataset["game_type"].isin({"REG"})
    dataset["is_playoff"] = playoff_mask.astype(int)
    dataset["is_regular_season"] = (~playoff_mask).astype(int)

    team_divisions = dataset["team"].map(get_team_division)
    opponent_divisions = dataset["opponent"].map(get_team_division)
    dataset["is_division_game"] = (
        (team_divisions.notna()) & (team_divisions == opponent_divisions)
    ).astype(int)

    team_conferences = dataset["team"].map(get_team_conference)
    opponent_conferences = dataset["opponent"].map(get_team_conference)
    dataset["is_conference_game"] = (
        (team_conferences.notna()) & (team_conferences == opponent_conferences)
    ).astype(int)
    dataset["is_interconference_game"] = (1 - dataset["is_conference_game"]).astype(int)

    boolean_columns = [
        "is_home",
        "is_short_week",
        "is_long_rest",
        "is_post_bye",
        "is_weather_dome",
        "is_weather_precip",
        "is_weather_windy",
        "is_playoff",
        "is_regular_season",
        "is_division_game",
        "is_conference_game",
        "is_interconference_game",
    ]
    for column in boolean_columns:
        if column in dataset.columns:
            dataset[column] = dataset[column].astype(int)

    column_order = [
        "game_id",
        "game_datetime",
        "season",
        "week",
        "game_type",
        "league",
        "team",
        "opponent",
        "is_home",
        "moneyline",
        "implied_prob",
        "spread_line",
        "total_line",
        "espn_moneyline_open",
        "espn_moneyline_close",
        "espn_spread_open",
        "espn_spread_close",
        "espn_total_open",
        "espn_total_close",
        "win",
        "home_score",
        "away_score",
        "points_for",
        "points_against",
        "point_diff",
        "off_epa_per_play",
        "off_epa_per_play_prev",
        "off_epa_per_play_rolling_3",
        "off_epa_per_play_rolling_5",
        "off_success_rate",
        "off_success_rate_prev",
        "off_success_rate_rolling_3",
        "off_success_rate_rolling_5",
        "off_pass_rate",
        "off_pass_rate_prev",
        "off_pass_rate_rolling_3",
        "off_pass_rate_rolling_5",
        "def_epa_per_play",
        "def_epa_per_play_prev",
        "def_epa_per_play_rolling_3",
        "def_epa_per_play_rolling_5",
        "def_success_rate",
        "def_success_rate_prev",
        "def_success_rate_rolling_3",
        "def_success_rate_rolling_5",
        "team_rest_days",
        "opponent_rest_days",
        "rest_diff",
        "is_short_week",
        "is_long_rest",
        "is_post_bye",
        "road_trip_length_entering",
        "home_stand_length_entering",
        "season_off_epa_per_play",
        "season_off_success_rate",
        "season_def_epa_per_play",
        "season_def_success_rate",
        "rolling_win_pct_3",
        "rolling_win_pct_5",
        "rolling_point_diff_3",
        "rolling_point_diff_5",
        "win_streak",
        "injuries_out",
        "injuries_doubtful",
        "injuries_questionable",
        "injuries_total",
        "injuries_qb_out",
        "injuries_skill_out",
        "game_temperature_f",
        "game_wind_mph",
        "is_weather_precip",
        "is_weather_windy",
        "is_weather_dome",
        "is_playoff",
        "is_regular_season",
        "is_division_game",
        "is_conference_game",
        "is_interconference_game",
        "weekday",
        "stadium",
        "surface",
        "roof",
        "weather_detail",
        "game_time",
    ]

    for opponent_column in [col for col in dataset.columns if col.startswith("opponent_")]:
        if opponent_column not in column_order:
            column_order.append(opponent_column)

    existing_columns = [col for col in column_order if col in dataset.columns]
    dataset = dataset[existing_columns].copy()

    dataset.sort_values(["game_datetime", "season", "week", "team"], inplace=True)

    output_path = paths.processed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output_path, index=False)
    LOGGER.info("Wrote processed dataset to %s", output_path)

    return dataset


def _build_dataset_generic(seasons: Iterable[int], league: str) -> pd.DataFrame:
    ensure_directories()
    season_range = seasons_tuple(seasons)
    league_code = league.upper()

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

    games["start_time_utc"] = pd.to_datetime(games["start_time_utc"], errors="coerce")

    if not odds.empty:
        odds["fetched_at_utc"] = pd.to_datetime(odds["fetched_at_utc"], errors="coerce")
        odds = odds.dropna(subset=["fetched_at_utc", "price_american"])
        odds.sort_values("fetched_at_utc", inplace=True)

        games_index = games.set_index("game_id")
        closing_map: Dict[tuple[str, str], float] = {}
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
                games.loc[mask, column] = games.loc[mask, "game_id"].map(
                    lambda gid: closing_map.get((gid, outcome_key))
                )

    # For NBA, match odds from database by game_id
    # First check if moneylines are already in game_results (from Kaggle or other sources)
    # Then fill missing ones from odds table
    if league_code == "NBA" and not odds.empty:
        # Build mapping from odds table for games missing moneylines
        odds_by_game = {}
        for (game_id, outcome), group in odds.groupby(["game_id", "outcome"]):
            outcome_key = str(outcome).lower()
            if outcome_key in ("home", "away"):
                # Get the most recent odds for this game/outcome
                latest_price = group.iloc[-1]["price_american"]
                if game_id not in odds_by_game:
                    odds_by_game[game_id] = {}
                odds_by_game[game_id][outcome_key] = latest_price
        
        # Fill in missing moneylines from odds table BEFORE filtering
        for outcome_key, column in (("home", "home_moneyline_close"), ("away", "away_moneyline_close")):
            mask = games[column].isna()
            if mask.any():
                games.loc[mask, column] = games.loc[mask, "game_id"].map(
                    lambda gid: odds_by_game.get(gid, {}).get(outcome_key) if pd.notna(gid) else None
                )

    games = games.dropna(subset=["home_moneyline_close", "away_moneyline_close", "home_score", "away_score"])
    games["game_datetime"] = games["start_time_utc"]
    games["game_type"] = games.get("game_type", "REG").fillna("REG")

    home = games.assign(
        team=games["home_team"],
        opponent=games["away_team"],
        moneyline=games["home_moneyline_close"],
        implied_prob=_implied_probability(games["home_moneyline_close"]),
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
        implied_prob=_implied_probability(games["away_moneyline_close"]),
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
        ]
    ].copy()

    # For NBA, also try ESPN CSV odds as fallback if moneyline is still missing
    if league_code == "NBA":
        dataset = _merge_espn_odds(dataset, league_code)
        # Use ESPN close odds if moneyline is missing
        mask = dataset["moneyline"].isna() & dataset["espn_moneyline_close"].notna()
        if mask.any():
            dataset.loc[mask, "moneyline"] = dataset.loc[mask, "espn_moneyline_close"]
            dataset.loc[mask, "implied_prob"] = _implied_probability(dataset.loc[mask, "espn_moneyline_close"])
    
    dataset = dataset.dropna(subset=["moneyline"])
    dataset.sort_values(["game_datetime", "season", "team"], inplace=True)

    injuries = _load_injuries_from_db_league(league_code, seasons)
    if not injuries.empty:
        injury_summary = _summarize_injuries_by_date(injuries, league_code)
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

    # Merge ESPN odds again after injury merge (in case it wasn't done before)
    if league_code != "NBA":
        dataset = _merge_espn_odds(dataset, league_code)
    dataset = _merge_team_metrics(dataset, league_code)

    output_path = PROCESSED_DATA_DIR / "model_input" / (
        f"moneyline_{league_code.lower()}_{season_range[0]}_{season_range[1]}.parquet"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output_path, index=False)
    LOGGER.info("Wrote %s dataset to %s", league_code, output_path)

    return dataset


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build processed dataset for moneyline modeling")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=list(range(1999, 2024)),
        help="NFL seasons to include",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    parser.add_argument(
        "--league",
        default="NFL",
        choices=["NFL", "NBA"],
        help="League to build the dataset for",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    seasons = [int(season) for season in args.seasons]
    build_dataset(seasons, league=args.league)


if __name__ == "__main__":
    main()

