"""Soccer-specific feature helpers leveraging Understat and Football-Data feeds."""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd

from src.data.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from src.data.team_mappings import normalize_team_code

LOGGER = logging.getLogger(__name__)

SOCCER_LEAGUES = {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}
SOCCER_ALLOWED_SEASONS = range(2021, 2026)

UNDERSTAT_PROCESSED_DIR = {
    "EPL": "EPL",
    "LALIGA": "La_liga",
    "BUNDESLIGA": "Bundesliga",
    "SERIEA": "Serie_A",
    "LIGUE1": "Ligue_1",
}

FOOTBALL_DATA_DIR = {
    "EPL": "premier-league",
    "LALIGA": "la-liga",
    "BUNDESLIGA": "bundesliga",
    "SERIEA": "serie-a",
    "LIGUE1": "ligue-1",
}

SET_PIECE_SITUATIONS = {"DirectFreekick", "SetPiece", "Corner", "FreeKick", "Penalty"}

PITCH_LENGTH_METERS = 105.0
PITCH_WIDTH_METERS = 68.0

SOCCER_NAME_KEY_OVERRIDES = {
    "afc bournemouth": "bournemouth",
    "brighton and hove albion": "brighton",
    "brighton hove albion": "brighton",
    "manchester united": "manchester united",
    "man united": "manchester united",
    "manchester city": "manchester city",
    "man city": "manchester city",
    "newcastle united": "newcastle",
    "nottingham forest": "nottingham forest",
    "nottingham": "nottingham forest",
    "nottm forest": "nottingham forest",
    "west ham united": "west ham",
    "leeds united": "leeds",
    "borussia monchengladbach": "borussia monchengladbach",
    "borussia m gladbach": "borussia monchengladbach",
    "rb leipzig": "rb leipzig",
    "rasenballsport leipzig": "rb leipzig",
    "mainz 05": "mainz",
    "1 fc union berlin": "union berlin",
    "fc union berlin": "union berlin",
    "union berlin": "union berlin",
    "1 fc heidenheim": "heidenheim",
    "fc heidenheim": "heidenheim",
    "fc st pauli": "st pauli",
    "st pauli": "st pauli",
    "1 fc koln": "cologne",
    "fc cologne": "cologne",
    "fc koln": "cologne",
    "tsg hoffenheim": "hoffenheim",
    "real oviedo": "oviedo",
    "rayo vallecano": "rayo vallecano",
    "atletico madrid": "atletico madrid",
    "athletic club": "athletic club",
    "real sociedad": "real sociedad",
    "real betis": "real betis",
    "rc lens": "lens",
    "paris fc": "paris fc",
    "paris saint germain": "paris saint germain",
}


def _filter_seasons(seasons: Iterable[int]) -> List[int]:
    filtered = sorted({int(season) for season in seasons if 2021 <= int(season) <= 2025})
    return filtered


def _soccer_team_name_key(name: str | None) -> str:
    if not name:
        return ""
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = " ".join(text.split())
    return SOCCER_NAME_KEY_OVERRIDES.get(text, text)


def _warehouse_team_column(games: pd.DataFrame, side: str) -> pd.Series:
    code_column = f"{side}_team_code"
    team_column = f"{side}_team"
    if code_column in games.columns:
        return games[code_column].astype(str)
    return games[team_column].astype(str)


def _prepare_games_for_match(league: str, games_df: pd.DataFrame) -> pd.DataFrame:
    if games_df.empty:
        return pd.DataFrame()
    games = games_df.copy()
    games["start_time_utc"] = pd.to_datetime(games["start_time_utc"], utc=True, errors="coerce")
    games.dropna(subset=["start_time_utc"], inplace=True)
    if games.empty:
        return games
    games["match_date"] = games["start_time_utc"].dt.date
    games["home_output_team"] = _warehouse_team_column(games, "home")
    games["away_output_team"] = _warehouse_team_column(games, "away")

    if "home_team_name" in games.columns and "away_team_name" in games.columns:
        games["home_match_key"] = games["home_team_name"].apply(_soccer_team_name_key)
        games["away_match_key"] = games["away_team_name"].apply(_soccer_team_name_key)
        games["match_strategy"] = "name"
    else:
        games["home_match_key"] = (
            games["home_team"].astype(str).apply(lambda value: normalize_team_code(league, value))
        )
        games["away_match_key"] = (
            games["away_team"].astype(str).apply(lambda value: normalize_team_code(league, value))
        )
        games["match_strategy"] = "code"
    return games


def _latest_understat_matches_dir(league: str | None = None) -> Path | None:
    base = RAW_DATA_DIR / "sources" / "soccer" / "understat_matches"
    if not base.exists():
        return None
    runs = sorted([path for path in base.iterdir() if path.is_dir()])
    if not runs:
        return None
    if league is None:
        return runs[-1]
    for directory in reversed(runs):
        meta_path = directory / "match_metadata.parquet"
        if not meta_path.exists():
            continue
        try:
            leagues = pd.read_parquet(meta_path, columns=["league"])["league"].unique()
        except Exception:  # noqa: BLE001
            continue
        if any(entry == league for entry in leagues):
            return directory
    return None


def _load_understat_match_metadata(league: str, seasons: Iterable[int]) -> pd.DataFrame:
    directory = _latest_understat_matches_dir(league)
    if not directory:
        LOGGER.warning("Understat match payloads have not been ingested")
        return pd.DataFrame()

    path = directory / "match_metadata.parquet"
    if not path.exists():
        LOGGER.warning("Understat match metadata missing at %s", path)
        return pd.DataFrame()

    df = pd.read_parquet(path)
    df = df[(df["league"] == league) & (df["season"].isin(seasons))].copy()
    if df.empty:
        return df
    df["match_datetime"] = pd.to_datetime(df["match_datetime"], utc=True, errors="coerce")
    df["match_date"] = df["match_datetime"].dt.date
    df["home_code"] = df["home_team"].apply(lambda name: normalize_team_code(league, str(name)))
    df["away_code"] = df["away_team"].apply(lambda name: normalize_team_code(league, str(name)))
    df["home_match_key"] = df["home_team"].apply(_soccer_team_name_key)
    df["away_match_key"] = df["away_team"].apply(_soccer_team_name_key)
    df.dropna(subset=["home_code", "away_code"], inplace=True)
    return df


def _map_matches_to_games(
    league: str,
    match_meta: pd.DataFrame,
    games_df: pd.DataFrame,
) -> pd.DataFrame:
    if match_meta.empty or games_df.empty:
        return pd.DataFrame()

    games = _prepare_games_for_match(league, games_df)
    if games.empty:
        return pd.DataFrame()

    name_games = games[games["match_strategy"] == "name"].copy()
    frames: List[pd.DataFrame] = []
    if not name_games.empty and {"home_match_key", "away_match_key"}.issubset(match_meta.columns):
        name_merged = match_meta.merge(
            name_games,
            on=["match_date", "home_match_key", "away_match_key"],
            how="inner",
            suffixes=("_understat", "_game"),
        )
        if not name_merged.empty:
            name_merged["home_feature_key"] = name_merged["home_match_key"]
            name_merged["away_feature_key"] = name_merged["away_match_key"]
            name_merged["feature_key_kind"] = "name"
            frames.append(name_merged)

    code_games = games[games["match_strategy"] == "code"].copy()
    if not code_games.empty:
        code_merged = match_meta.merge(
            code_games,
            left_on=["match_date", "home_code", "away_code"],
            right_on=["match_date", "home_match_key", "away_match_key"],
            how="inner",
            suffixes=("_understat", "_game"),
        )
        if not code_merged.empty:
            code_merged["home_feature_key"] = code_merged["home_code"]
            code_merged["away_feature_key"] = code_merged["away_code"]
            code_merged["feature_key_kind"] = "code"
            frames.append(code_merged)

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values("match_datetime")
    merged = merged.drop_duplicates(subset=["match_id"])
    return merged


def _load_understat_team_history(
    league: str,
    seasons: Iterable[int],
    *,
    use_name_keys: bool = False,
) -> pd.DataFrame:
    folder_name = UNDERSTAT_PROCESSED_DIR.get(league)
    if not folder_name:
        return pd.DataFrame()
    base = PROCESSED_DATA_DIR / "external" / "understat" / folder_name
    frames: List[pd.DataFrame] = []
    for season in seasons:
        path = base / f"{season}_teams.parquet"
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        df["season"] = int(season)
        df["match_datetime"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
        df["team_title"] = df["team_title"].astype(str)
        if use_name_keys:
            df["team_code"] = df["team_title"].apply(_soccer_team_name_key)
        else:
            df["team_code"] = df["team_title"].apply(lambda name: normalize_team_code(league, name))
        df["opponent_side"] = df["h_a"].map({"h": "away", "a": "home"})
        frames.append(df)
    if not frames:
        LOGGER.warning("Missing Understat team history for %s seasons %s", league, seasons)
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_processed_understat_match_metadata(league: str, seasons: Iterable[int]) -> pd.DataFrame:
    folder_name = UNDERSTAT_PROCESSED_DIR.get(league)
    if not folder_name:
        return pd.DataFrame()
    base = PROCESSED_DATA_DIR / "external" / "understat" / folder_name
    frames: List[pd.DataFrame] = []
    for season in seasons:
        path = base / f"{season}_dates.parquet"
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        if df.empty:
            continue
        df = df.copy()
        df["league"] = league
        df["season"] = int(season)
        df["match_id"] = df["id"].astype(str)
        df["match_datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        df["home_team"] = df["h"].apply(
            lambda value: value.get("title") if isinstance(value, dict) else None
        )
        df["away_team"] = df["a"].apply(
            lambda value: value.get("title") if isinstance(value, dict) else None
        )
        df["match_date"] = df["match_datetime"].dt.date
        df["home_code"] = df["home_team"].apply(lambda name: normalize_team_code(league, str(name)))
        df["away_code"] = df["away_team"].apply(lambda name: normalize_team_code(league, str(name)))
        df["home_match_key"] = df["home_team"].apply(_soccer_team_name_key)
        df["away_match_key"] = df["away_team"].apply(_soccer_team_name_key)
        frames.append(
            df[
                [
                    "league",
                    "season",
                    "match_id",
                    "match_datetime",
                    "match_date",
                    "home_team",
                    "away_team",
                    "home_code",
                    "away_code",
                    "home_match_key",
                    "away_match_key",
                ]
            ].dropna(subset=["match_datetime", "home_team", "away_team"])
        )
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _expand_timeline(mapped_matches: pd.DataFrame) -> pd.DataFrame:
    long_rows: List[Dict[str, object]] = []
    for row in mapped_matches.itertuples():
        long_rows.append(
            {
                "match_id": row.match_id,
                "game_id": row.game_id,
                "league": row.league,
                "season": row.season_understat,
                "match_datetime": row.match_datetime,
                "team_code": row.home_feature_key,
                "opponent_code": row.away_feature_key,
                "team_output": row.home_output_team,
                "team_side": "h",
            }
        )
        long_rows.append(
            {
                "match_id": row.match_id,
                "game_id": row.game_id,
                "league": row.league,
                "season": row.season_understat,
                "match_datetime": row.match_datetime,
                "team_code": row.away_feature_key,
                "opponent_code": row.home_feature_key,
                "team_output": row.away_output_team,
                "team_side": "a",
            }
        )
    timeline = pd.DataFrame(long_rows)
    if "team_output" not in timeline.columns:
        timeline["team_output"] = timeline["team_code"]
    timeline.dropna(subset=["team_code"], inplace=True)
    return timeline


def _compute_team_metrics(
    league: str,
    timeline: pd.DataFrame,
    team_history: pd.DataFrame,
) -> pd.DataFrame:
    if timeline.empty or team_history.empty:
        return pd.DataFrame()

    history = team_history.copy()
    history["ppda_att"] = history.apply(
        lambda row: (
            float(row["ppda"]["att"]) / float(row["ppda"]["def"])
            if isinstance(row["ppda"], dict) and row["ppda"].get("def")
            else np.nan
        ),
        axis=1,
    )
    history["ppda_allowed_att"] = history.apply(
        lambda row: (
            float(row["ppda_allowed"]["att"]) / float(row["ppda_allowed"]["def"])
            if isinstance(row["ppda_allowed"], dict) and row["ppda_allowed"].get("def")
            else np.nan
        ),
        axis=1,
    )
    for column in [
        "xG",
        "xGA",
        "deep",
        "deep_allowed",
        "scored",
        "missed",
        "xpts",
        "ppda_att",
        "ppda_allowed_att",
    ]:
        history[column] = pd.to_numeric(history[column], errors="coerce")
    history.sort_values(["team_code", "season", "match_datetime"], inplace=True)

    records: List[Dict[str, object]] = []
    for row in timeline.itertuples(index=False):
        prior = history[
            (history["team_code"] == row.team_code)
            & (history["season"] == row.season)
            & (history["match_datetime"] < row.match_datetime)
        ]
        record: Dict[str, object] = {"match_id": row.match_id, "team_code": row.team_code}
        for metric, source, window in (
            ("ust_team_xg_avg_l3", "xG", 3),
            ("ust_team_xg_avg_l5", "xG", 5),
            ("ust_team_xga_avg_l3", "xGA", 3),
            ("ust_team_xga_avg_l5", "xGA", 5),
            ("ust_team_ppda_att_l3", "ppda_att", 3),
            ("ust_team_ppda_allowed_att_l3", "ppda_allowed_att", 3),
            ("ust_team_deep_entries_l3", "deep", 3),
            ("ust_team_deep_allowed_l3", "deep_allowed", 3),
            ("ust_team_goals_for_avg_l5", "scored", 5),
            ("ust_team_goals_against_avg_l5", "missed", 5),
            ("ust_team_xpts_avg_l5", "xpts", 5),
        ):
            recent = prior[source].dropna().tail(window)
            record[metric] = float(recent.mean()) if not recent.empty else np.nan
        records.append(record)

    return pd.DataFrame.from_records(records)


def _load_match_shots(league: str, seasons: Iterable[int]) -> pd.DataFrame:
    directory = _latest_understat_matches_dir(league)
    if not directory:
        return pd.DataFrame()
    path = directory / "match_shots.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty or "league" not in df.columns:
        return pd.DataFrame()
    return df[(df["league"] == league) & (df["season"].isin(seasons))].copy()


def _compute_shot_metrics(
    league: str,
    seasons: Iterable[int],
    timeline: pd.DataFrame,
    *,
    use_name_keys: bool = False,
) -> pd.DataFrame:
    shots = _load_match_shots(league, seasons)
    if shots.empty:
        return pd.DataFrame()

    if use_name_keys:
        shots["team_code"] = shots["team_title"].apply(_soccer_team_name_key)
    else:
        shots["team_code"] = shots["team_title"].apply(
            lambda name: normalize_team_code(league, str(name))
        )
    shots = shots.merge(
        timeline[["match_id", "team_code"]].drop_duplicates(),
        on=["match_id", "team_code"],
        how="inner",
    )
    if shots.empty:
        return pd.DataFrame()

    shots["shot_distance"] = np.sqrt(
        ((1.0 - shots["x"]) * PITCH_LENGTH_METERS) ** 2
        + ((0.5 - shots["y"]) * PITCH_WIDTH_METERS) ** 2
    )
    shots["is_open_play"] = (shots["situation"] == "OpenPlay").astype(int)
    shots["is_set_piece"] = shots["situation"].isin(SET_PIECE_SITUATIONS).astype(int)

    agg = (
        shots.groupby(["match_id", "team_code"])
        .agg(
            shots_total=("shot_id", "count"),
            open_play=("is_open_play", "sum"),
            set_piece=("is_set_piece", "sum"),
            avg_distance=("shot_distance", "mean"),
        )
        .reset_index()
    )
    agg["open_play_share"] = np.where(
        agg["shots_total"] > 0, agg["open_play"] / agg["shots_total"], np.nan
    )
    agg["set_piece_share"] = np.where(
        agg["shots_total"] > 0, agg["set_piece"] / agg["shots_total"], np.nan
    )

    merged = timeline.merge(agg, on=["match_id", "team_code"], how="left")
    if merged.empty:
        return pd.DataFrame()
    merged.sort_values(["team_code", "match_datetime"], inplace=True)
    group = merged.groupby(["team_code", "season"], sort=True)
    merged["ust_team_shot_open_play_share_l5"] = group["open_play_share"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=1).mean()
    )
    merged["ust_team_shot_set_piece_share_l5"] = group["set_piece_share"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=1).mean()
    )
    merged["ust_team_avg_shot_distance_l5"] = group["avg_distance"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=1).mean()
    )

    return merged[
        [
            "match_id",
            "team_code",
            "ust_team_shot_open_play_share_l5",
            "ust_team_shot_set_piece_share_l5",
            "ust_team_avg_shot_distance_l5",
        ]
    ]


def _load_match_players(league: str, seasons: Iterable[int]) -> pd.DataFrame:
    directory = _latest_understat_matches_dir(league)
    if not directory:
        return pd.DataFrame()
    path = directory / "match_players.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    return df[(df["league"] == league) & (df["season"].isin(seasons))].copy()


def _compute_lineup_features(
    league: str,
    seasons: Iterable[int],
    timeline: pd.DataFrame,
    *,
    use_name_keys: bool = False,
) -> pd.DataFrame:
    rosters = _load_match_players(league, seasons)
    if rosters.empty:
        return pd.DataFrame()

    rosters["position_order"] = pd.to_numeric(rosters.get("position_order"), errors="coerce")
    rosters["is_starter"] = rosters["position_order"] < 17
    if use_name_keys:
        rosters["team_code"] = rosters["team_title"].apply(_soccer_team_name_key)
    else:
        rosters["team_code"] = rosters["team_title"].apply(
            lambda name: normalize_team_code(league, str(name))
        )
    starters = rosters[rosters["is_starter"]].copy()
    starters = starters.merge(
        timeline[["match_id", "team_code", "season", "match_datetime"]].drop_duplicates(),
        on=["match_id", "team_code"],
        how="inner",
        suffixes=("", "_timeline"),
    )
    if "season_timeline" in starters.columns:
        starters["season"] = starters["season_timeline"]
        starters.drop(columns=["season_timeline"], inplace=True)
    if starters.empty:
        return pd.DataFrame()

    starters.sort_values(["team_code", "season", "match_datetime"], inplace=True)

    records: List[Dict[str, object]] = []
    for (team_code, season), group in starters.groupby(["team_code", "season"], sort=True):
        player_history: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {
                "minutes": 0.0,
                "xg": 0.0,
                "xa": 0.0,
                "shots": 0.0,
                "key_passes": 0.0,
                "starts": 0,
            }
        )
        prev_lineup: set[str] = set()
        recent_lineups: deque[set[str]] = deque(maxlen=3)

        for match_id, match_rows in group.groupby("match_id", sort=False):
            lineup_ids = set(match_rows["player_id"].astype(str))
            starter_count = max(len(lineup_ids), 1)

            zero_min = 0
            prior_minutes_total = 0.0
            xg_per90: List[float] = []
            xa_per90: List[float] = []
            shots_per90: List[float] = []
            key_passes_per90: List[float] = []

            for row in match_rows.itertuples():
                player_id = str(row.player_id)
                stats = player_history[player_id]
                minutes = stats["minutes"]
                prior_minutes_total += minutes
                if minutes == 0:
                    zero_min += 1
                factor = (90.0 / minutes) if minutes > 0 else 0.0
                xg_per90.append(stats["xg"] * factor)
                xa_per90.append(stats["xa"] * factor)
                shots_per90.append(stats["shots"] * factor)
                key_passes_per90.append(stats["key_passes"] * factor)

            recent_counts = Counter(pid for lineup in recent_lineups for pid in lineup)
            returning_prev = len(lineup_ids & prev_lineup) / starter_count
            returning_last3 = (
                len({pid for pid in lineup_ids if recent_counts.get(pid, 0) >= 2}) / starter_count
                if recent_counts
                else 0.0
            )

            records.append(
                {
                    "match_id": match_id,
                    "team_code": team_code,
                    "ust_xi_prior_minutes_total": prior_minutes_total,
                    "ust_xi_prior_minutes_avg": prior_minutes_total / starter_count,
                    "ust_xi_prior_xg_per90_avg": float(np.mean(xg_per90)) if xg_per90 else 0.0,
                    "ust_xi_prior_xa_per90_avg": float(np.mean(xa_per90)) if xa_per90 else 0.0,
                    "ust_xi_prior_shots_per90_avg": (
                        float(np.mean(shots_per90)) if shots_per90 else 0.0
                    ),
                    "ust_xi_prior_key_passes_per90_avg": (
                        float(np.mean(key_passes_per90)) if key_passes_per90 else 0.0
                    ),
                    "ust_xi_share_zero_min": zero_min / starter_count,
                    "ust_xi_returning_starters_prev_match": returning_prev,
                    "ust_xi_returning_starters_last3": returning_last3,
                }
            )

            for row in match_rows.itertuples():
                player_id = str(row.player_id)
                stats = player_history[player_id]
                stats["minutes"] += float(row.minutes or 0.0)
                stats["xg"] += float(row.xg or 0.0)
                stats["xa"] += float(row.xa or 0.0)
                stats["shots"] += float(row.shots or 0.0)
                stats["key_passes"] += float(row.key_passes or 0.0)
                stats["starts"] += 1

            recent_lineups.append(lineup_ids)
            prev_lineup = lineup_ids

    return pd.DataFrame.from_records(records)


def build_understat_features(
    league: str,
    seasons: Iterable[int],
    games_df: pd.DataFrame,
) -> pd.DataFrame:
    filtered_seasons = _filter_seasons(seasons)
    if not filtered_seasons:
        return pd.DataFrame()

    match_meta = _load_understat_match_metadata(league, filtered_seasons)
    mapped = (
        _map_matches_to_games(league, match_meta, games_df)
        if not match_meta.empty
        else pd.DataFrame()
    )
    if mapped.empty:
        processed_meta = _load_processed_understat_match_metadata(league, filtered_seasons)
        mapped = (
            _map_matches_to_games(league, processed_meta, games_df)
            if not processed_meta.empty
            else pd.DataFrame()
        )
    if mapped.empty:
        LOGGER.warning("Unable to match Understat games to warehouse games for %s", league)
        return pd.DataFrame()

    timeline = _expand_timeline(mapped)
    use_name_keys = "feature_key_kind" in mapped.columns and bool(
        (mapped["feature_key_kind"] == "name").any()
    )
    team_history = _load_understat_team_history(
        league,
        filtered_seasons,
        use_name_keys=use_name_keys,
    )
    team_metrics = _compute_team_metrics(league, timeline, team_history)
    shot_metrics = _compute_shot_metrics(
        league,
        filtered_seasons,
        timeline,
        use_name_keys=use_name_keys,
    )
    lineup_metrics = _compute_lineup_features(
        league,
        filtered_seasons,
        timeline,
        use_name_keys=use_name_keys,
    )

    features = timeline[["match_id", "team_code", "team_output", "game_id"]].drop_duplicates()
    for frame in (team_metrics, shot_metrics, lineup_metrics):
        if not frame.empty:
            features = features.merge(frame, on=["match_id", "team_code"], how="left")

    features = features.drop(columns=["match_id", "team_code"]).rename(
        columns={"team_output": "team"}
    )
    return features


def _decimal_to_prob(value: float | None) -> float | np.nan:
    if not value or value <= 1.0:
        return np.nan
    return 1.0 / value


def _decimal_to_american(value: float | None) -> float | np.nan:
    if not value or value <= 1.0:
        return np.nan
    if value >= 2.0:
        return round((value - 1.0) * 100.0, 2)
    return round(-100.0 / (value - 1.0), 2)


def load_football_data_odds(
    league: str,
    seasons: Iterable[int],
    games_df: pd.DataFrame,
) -> pd.DataFrame:
    folder = FOOTBALL_DATA_DIR.get(league)
    if not folder:
        return pd.DataFrame()
    league_dir = PROCESSED_DATA_DIR / "external" / "football_data" / folder
    if not league_dir.exists():
        LOGGER.warning("Football-Data directory missing for %s at %s", league, league_dir)
        return pd.DataFrame()

    filtered_seasons = _filter_seasons(seasons)
    frames: List[pd.DataFrame] = []
    for path in league_dir.glob("*.parquet"):
        df = pd.read_parquet(path)
        if "match_date" not in df.columns:
            df["match_date"] = pd.to_datetime(df["Date"], dayfirst=True, utc=True, errors="coerce")
        else:
            df["match_date"] = pd.to_datetime(df["match_date"], utc=True, errors="coerce")
        df.dropna(subset=["match_date"], inplace=True)
        df["season_year"] = df["match_date"].dt.year
        df.loc[df["match_date"].dt.month < 7, "season_year"] -= 1
        df = df[df["season_year"].isin(filtered_seasons)]
        if df.empty:
            continue
        df["match_date"] = df["match_date"].dt.date
        df["home_code"] = df["HomeTeam"].apply(lambda name: normalize_team_code(league, str(name)))
        df["away_code"] = df["AwayTeam"].apply(lambda name: normalize_team_code(league, str(name)))
        df.dropna(subset=["home_code", "away_code"], inplace=True)
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    odds = pd.concat(frames, ignore_index=True)

    games = games_df[["game_id", "season", "start_time_utc", "home_team", "away_team"]].copy()
    games["start_time_utc"] = pd.to_datetime(games["start_time_utc"], utc=True, errors="coerce")
    games.dropna(subset=["start_time_utc"], inplace=True)
    games["match_date"] = games["start_time_utc"].dt.date

    merged = odds.merge(
        games,
        left_on=["match_date", "home_code", "away_code"],
        right_on=["match_date", "home_team", "away_team"],
        how="inner",
    )
    if merged.empty:
        LOGGER.warning("Unable to align Football-Data odds for %s", league)
        return pd.DataFrame()

    def _coerce(col: str) -> pd.Series:
        return pd.to_numeric(merged[col], errors="coerce")

    merged["B365H"] = _coerce("B365H")
    merged["B365A"] = _coerce("B365A")
    merged["B365D"] = _coerce("B365D")
    merged["PSH"] = _coerce("PSH")
    merged["PSA"] = _coerce("PSA")
    merged["PSD"] = _coerce("PSD")

    rows: List[Dict[str, object]] = []
    for row in merged.itertuples():
        for team, opp, home_flag in (
            (row.home_code, row.away_code, True),
            (row.away_code, row.home_code, False),
        ):
            b365 = row.B365H if home_flag else row.B365A
            ps = row.PSH if home_flag else row.PSA
            entry = {
                "game_id": row.game_id,
                "team": team,
                "fd_b365_ml_decimal": b365,
                "fd_b365_ml_american": _decimal_to_american(b365),
                "fd_b365_implied": _decimal_to_prob(b365),
                "fd_ps_ml_decimal": ps,
                "fd_ps_ml_american": _decimal_to_american(ps),
                "fd_ps_implied": _decimal_to_prob(ps),
                "fd_b365_draw_decimal": row.B365D,
                "fd_b365_draw_implied": _decimal_to_prob(row.B365D),
                "fd_ps_draw_decimal": row.PSD,
                "fd_ps_draw_implied": _decimal_to_prob(row.PSD),
            }
            decimals = [val for val in (b365, ps) if val and val > 1.0]
            entry["fd_avg_ml_decimal"] = float(np.mean(decimals)) if decimals else np.nan
            entry["fd_avg_implied"] = _decimal_to_prob(entry["fd_avg_ml_decimal"])
            rows.append(entry)
    return pd.DataFrame.from_records(rows)


def build_football_data_form_features(
    league: str,
    seasons: Iterable[int],
    games_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build shifted team form from Football-Data match stats.

    Football-Data has final match stats, so every rolling calculation is shifted
    before being attached to the target game.
    """
    folder = FOOTBALL_DATA_DIR.get(league)
    if not folder:
        return pd.DataFrame()
    league_dir = PROCESSED_DATA_DIR / "external" / "football_data" / folder
    if not league_dir.exists():
        return pd.DataFrame()

    filtered_seasons = _filter_seasons(seasons)
    frames: List[pd.DataFrame] = []
    for path in league_dir.glob("*.parquet"):
        df = pd.read_parquet(path)
        if "match_date" not in df.columns:
            df["match_date"] = pd.to_datetime(df["Date"], dayfirst=True, utc=True, errors="coerce")
        else:
            df["match_date"] = pd.to_datetime(df["match_date"], utc=True, errors="coerce")
        df.dropna(subset=["match_date"], inplace=True)
        df["season_year"] = df["match_date"].dt.year
        df.loc[df["match_date"].dt.month < 7, "season_year"] -= 1
        df = df[df["season_year"].isin(filtered_seasons)]
        if df.empty:
            continue
        df["match_date"] = df["match_date"].dt.date
        df["home_code"] = df["HomeTeam"].apply(lambda name: normalize_team_code(league, str(name)))
        df["away_code"] = df["AwayTeam"].apply(lambda name: normalize_team_code(league, str(name)))
        df["home_match_key"] = df["HomeTeam"].apply(_soccer_team_name_key)
        df["away_match_key"] = df["AwayTeam"].apply(_soccer_team_name_key)
        df.dropna(subset=["home_code", "away_code"], inplace=True)
        frames.append(df)

    if not frames or games_df.empty:
        return pd.DataFrame()

    stats = pd.concat(frames, ignore_index=True)
    games = _prepare_games_for_match(league, games_df)
    frames = []
    name_games = games[games["match_strategy"] == "name"].copy()
    if not name_games.empty:
        name_merged = stats.merge(
            name_games,
            on=["match_date", "home_match_key", "away_match_key"],
            how="inner",
            suffixes=("_football_data", "_game"),
        )
        frames.append(name_merged)
    code_games = games[games["match_strategy"] == "code"].copy()
    if not code_games.empty:
        code_merged = stats.merge(
            code_games,
            left_on=["match_date", "home_code", "away_code"],
            right_on=["match_date", "home_match_key", "away_match_key"],
            how="inner",
            suffixes=("_football_data", "_game"),
        )
        frames.append(code_merged)
    non_empty_frames = [frame for frame in frames if not frame.empty]
    if not non_empty_frames:
        return pd.DataFrame()
    merged = pd.concat(non_empty_frames, ignore_index=True)

    def _num(column: str) -> pd.Series:
        if column not in merged.columns:
            return pd.Series(np.nan, index=merged.index)
        return pd.to_numeric(merged[column], errors="coerce")

    merged["FTHG"] = _num("FTHG")
    merged["FTAG"] = _num("FTAG")
    merged["HS"] = _num("HS")
    merged["AS"] = _num("AS")
    merged["HST"] = _num("HST")
    merged["AST"] = _num("AST")

    rows: List[Dict[str, object]] = []
    for row in merged.itertuples():
        rows.append(
            {
                "game_id": row.game_id,
                "season": row.season,
                "start_time_utc": row.start_time_utc,
                "team": row.home_output_team,
                "is_home": True,
                "goals_for": row.FTHG,
                "goals_against": row.FTAG,
                "shots_for": row.HS,
                "shots_against": row.AS,
                "shots_on_target_for": row.HST,
                "shots_on_target_against": row.AST,
            }
        )
        rows.append(
            {
                "game_id": row.game_id,
                "season": row.season,
                "start_time_utc": row.start_time_utc,
                "team": row.away_output_team,
                "is_home": False,
                "goals_for": row.FTAG,
                "goals_against": row.FTHG,
                "shots_for": row.AS,
                "shots_against": row.HS,
                "shots_on_target_for": row.AST,
                "shots_on_target_against": row.HST,
            }
        )

    timeline = pd.DataFrame.from_records(rows)
    if timeline.empty:
        return pd.DataFrame()
    timeline.sort_values(["team", "season", "start_time_utc"], inplace=True)
    metrics = [
        "goals_for",
        "goals_against",
        "shots_for",
        "shots_against",
        "shots_on_target_for",
        "shots_on_target_against",
    ]
    group = timeline.groupby(["team", "season"], sort=True)
    for metric in metrics:
        timeline[f"fd_{metric}_l5"] = group[metric].transform(
            lambda s: s.shift(1).rolling(5, min_periods=1).mean()
        )

    venue_group = timeline.groupby(["team", "season", "is_home"], sort=True)
    for metric in ["goals_for", "goals_against", "shots_for", "shots_against"]:
        timeline[f"fd_same_venue_{metric}_l5"] = venue_group[metric].transform(
            lambda s: s.shift(1).rolling(5, min_periods=1).mean()
        )

    feature_cols = [column for column in timeline.columns if column.startswith("fd_")]
    return timeline[["game_id", "team", *feature_cols]].drop_duplicates(["game_id", "team"])
