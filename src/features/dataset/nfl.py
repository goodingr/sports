"""NFL-specific dataset generation logic."""
from __future__ import annotations

import logging
from typing import Iterable, List

import numpy as np
import pandas as pd

try:
    import nfl_data_py as nfl  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "nfl_data_py is required for feature generation. Install it with `poetry add nfl-data-py`."
    ) from exc

from src.data.config import ensure_directories
from src.data.nfl import get_team_conference, get_team_division
from src.db.core import connect
from .shared import (
    DatasetPaths,
    INJURY_PLAYER_COLUMNS,
    INJURY_PRACTICE_COLUMNS,
    INJURY_STATUS_COLUMNS,
    INJURY_TEAM_COLUMNS,
    INJURY_WEEK_COLUMNS,
    PBP_COLUMNS,
    ROLLING_WINDOWS,
    TEAM_SKILL_POSITIONS,
    add_opponent_feature_mirrors,
    add_rest_travel_features,
    add_team_form_features,
    add_weather_features,
    coalesce_columns,
    first_present_column,
    implied_probability,
    load_injuries_from_db_league,
    merge_espn_odds,
    normalize_moneyline_columns,
    normalize_score_columns,
    normalize_team_columns,
    practice_status_to_category,
    seasons_tuple,
    status_category_from_text,
)

LOGGER = logging.getLogger(__name__)


def load_schedules(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
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


def load_pbp(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    if paths.raw_pbp.exists():
        LOGGER.info("Loading cached play-by-play data from %s", paths.raw_pbp)
        return pd.read_parquet(paths.raw_pbp)

    LOGGER.info("Downloading play-by-play data for seasons %s", seasons)
    pbp_frames: List[pd.DataFrame] = []
    skipped: List[int] = []
    for season in seasons:
        try:
            season_data = nfl.import_pbp_data([season])
        except Exception as exc:  # pragma: no cover - external dependency guard
            LOGGER.warning("Failed to download play-by-play for season %s: %s. Skipping.", season, exc)
            skipped.append(season)
            continue
        pbp_frames.append(season_data)

    if not pbp_frames:
        raise RuntimeError(
            f"Unable to download play-by-play data for seasons {list(seasons)}. "
            "nfl_data_py may not have released the current season yet."
        )

    if skipped:
        LOGGER.warning(
            "Play-by-play data unavailable for seasons %s; continuing with %s",
            skipped,
            [s for s in seasons if s not in skipped],
        )

    pbp = pd.concat(pbp_frames, ignore_index=True)
    pbp = pbp[[col for col in PBP_COLUMNS if col in pbp.columns]].copy()
    paths.raw_pbp.parent.mkdir(parents=True, exist_ok=True)
    pbp.to_parquet(paths.raw_pbp, index=False)
    return pbp


def team_game_metrics(pbp: pd.DataFrame) -> pd.DataFrame:
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


def load_team_metrics(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    if paths.team_metrics.exists():
        LOGGER.info("Loading cached team metrics from %s", paths.team_metrics)
        return pd.read_parquet(paths.team_metrics)

    pbp = load_pbp(paths, seasons)
    metrics = team_game_metrics(pbp)
    paths.team_metrics.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_parquet(paths.team_metrics, index=False)
    return metrics


def load_injuries_from_db_nfl(seasons: Iterable[int]) -> pd.DataFrame:
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


def load_injuries(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    injuries = load_injuries_from_db_nfl(seasons)
    if not injuries.empty:
        return injuries

    if paths.raw_injuries.exists():
        LOGGER.info("Loading cached injury reports from %s", paths.raw_injuries)
        return pd.read_parquet(paths.raw_injuries)

    LOGGER.info("Downloading injury reports for seasons %s", seasons)
    injury_frames: List[pd.DataFrame] = []
    missing: List[int] = []
    for season in seasons:
        try:
            injury_frames.append(nfl.import_injuries([season]))
        except Exception as exc:  # pragma: no cover - external dependency guard
            LOGGER.warning("Failed to download injuries for season %s: %s. Skipping.", season, exc)
            missing.append(season)
            continue

    if not injury_frames:
        raise RuntimeError(
            f"Unable to download injury reports for seasons {list(seasons)}. "
            "nfl_data_py may not publish in-progress seasons yet."
        )

    if missing:
        LOGGER.warning(
            "Injury data unavailable for seasons %s; continuing with %s",
            missing,
            [s for s in seasons if s not in missing],
        )

    injuries = pd.concat(injury_frames, ignore_index=True)
    paths.raw_injuries.parent.mkdir(parents=True, exist_ok=True)
    injuries.to_parquet(paths.raw_injuries, index=False)
    return injuries


def summarize_weekly_injuries(injuries: pd.DataFrame) -> pd.DataFrame:
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

    team_col = first_present_column(df, INJURY_TEAM_COLUMNS)
    if team_col is None:
        raise KeyError("Injury dataset missing team column")
    df = df.rename(columns={team_col: "team"})

    week_col = first_present_column(df, INJURY_WEEK_COLUMNS)
    if week_col is None:
        raise KeyError("Injury dataset missing week column")
    df["week"] = pd.to_numeric(df[week_col], errors="coerce")
    df = df[df["week"].notna()]

    if "season" in df.columns:
        df["season"] = pd.to_numeric(df["season"], errors="coerce")
        df = df[df["season"].notna()]

    status_col = first_present_column(df, INJURY_STATUS_COLUMNS)
    status_series = df[status_col].fillna("").astype(str) if status_col else pd.Series("", index=df.index)

    practice_col = first_present_column(df, INJURY_PRACTICE_COLUMNS)
    practice_series = df[practice_col].fillna("").astype(str) if practice_col else pd.Series("", index=df.index)

    df["status_category"] = status_series.apply(status_category_from_text)
    practice_categories = practice_series.apply(practice_status_to_category)
    df.loc[df["status_category"] == "other", "status_category"] = practice_categories[df["status_category"] == "other"]

    player_col = first_present_column(df, INJURY_PLAYER_COLUMNS)
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


def merge_team_metrics(dataset: pd.DataFrame) -> pd.DataFrame:
    metrics = pd.read_parquet(DatasetPaths(seasons_tuple(dataset["season"].unique()), "NFL").team_metrics)
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


def add_rolling_performance_features(df: pd.DataFrame) -> pd.DataFrame:
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


def build_dataset(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    ensure_directories()
    season_list = list(seasons)

    schedules = load_schedules(paths, season_list)
    schedules = normalize_score_columns(schedules)
    schedules = normalize_moneyline_columns(schedules)
    schedules = normalize_team_columns(schedules)

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

    game_type_series = coalesce_columns(schedules, ["game_type", "season_type"], "REG").astype(str).str.upper()
    weekday_series = coalesce_columns(schedules, ["weekday"], "")
    stadium_series = coalesce_columns(schedules, ["stadium", "stadium_name"], "")
    surface_series = coalesce_columns(schedules, ["surface"], "")
    roof_series = coalesce_columns(schedules, ["roof"], "")
    weather_series = coalesce_columns(schedules, ["weather", "weather_detail"], "")
    temperature_series = coalesce_columns(schedules, ["temperature", "temp", "temp_f"], np.nan)
    wind_series = coalesce_columns(schedules, ["wind", "wind_speed"], np.nan)
    humidity_series = coalesce_columns(schedules, ["humidity"], np.nan)
    gametime_series = coalesce_columns(schedules, ["gametime", "game_time"], "")

    home = schedules.assign(
        team=schedules["team_home"],
        opponent=schedules["team_away"],
        moneyline=schedules["home_moneyline"],
        implied_prob=implied_probability(schedules["home_moneyline"]),
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
        implied_prob=implied_probability(schedules["away_moneyline"]),
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

    metrics = load_team_metrics(paths, season_list)
    dataset = dataset.merge(metrics, on=["season", "week", "game_id", "team"], how="left")

    injuries = load_injuries(paths, season_list)
    injury_summary = summarize_weekly_injuries(injuries)
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

    dataset = merge_espn_odds(dataset, "NFL")
    dataset = merge_team_metrics(dataset)
    dataset = add_team_form_features(dataset)
    dataset = add_rest_travel_features(dataset)
    dataset = add_weather_features(dataset)
    dataset = add_rolling_performance_features(dataset)

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
    dataset = add_opponent_feature_mirrors(dataset, mirror_columns)

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
