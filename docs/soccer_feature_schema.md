# Soccer Feature Schema (2021–2025)

This document codifies the soccer-specific feature set that will be produced for EPL, La Liga, Bundesliga, Serie A, and Ligue 1 between the 2021 and 2025 seasons. It defines the exact Understat and Football-Data attributes that feed the moneyline dataset as well as the rules for matching events across data sources.

## Data Inputs

- **Understat league payloads** (`datesData`, `teamsData`, `playersData`) for match schedules and per-team match histories.
- **Understat match payloads** (`rostersData`, `shotsData`) fetched from `https://understat.com/match/<id>` for every league match; used to reconstruct starting XIs and per-player rolling form.
- **Football-Data odds CSVs** (Bet365, Pinnacle, William Hill, BetVictor, Max/Avg books) converted to Parquet via `src.data.ingest_football_data`.
- **Internal warehouse tables** (`games`, `game_results`, `odds`, `teams`, `sports`) that anchor canonical team codes and match identifiers.

All features are computed strictly with information that would be available before kickoff (or immediately after official lineups are published).

## Match Mapping Strategy

1. **Normalize team names** – Every external team string is mapped through `normalize_team_code(league, name)` so that EPL teams always reduce to codes such as `ARS`, `MCI`, `ITO`, etc. Missing aliases must be added to `src/data/team_mappings.py`.
2. **Create Understat match keys** – For each Understat row build `match_key = (league, match_date_utc, home_code, away_code, match_id)`. `match_date_utc` is derived from the Understat `datetime` stamp after coercing to UTC and dropping the time component.
3. **Create Football-Data match keys** – Football-Data rows use the same `(league, match_date_utc, home_code, away_code)` key. Season is inferred by soccer convention (July→June).
4. **Join to warehouse games** – Pull `games` + `teams` + `sports` for the target league and seasons ∈ [2021, 2025]. Convert `start_time_utc` to a date-only column and match on `(league, game_date, home_code, away_code)`. When multiple candidates exist (rare reschedules), prefer the closest kickoff timestamp.
5. **Persist the mapping** – The resulting dataframe provides `understat_match_id`, `football_data_match_id`, and `game_id`. All downstream merges (features, odds) use `game_id` + `team` to keep alignment unambiguous.

## Understat Team Aggregates

Understat’s per-team match histories are transformed into rolling aggregates that describe attacking, defensive, and possession quality entering a match. Every metric is computed on the matches prior to the target game (i.e., shifted by one).

| Feature | Definition | Window |
| --- | --- | --- |
| `ust_team_xg_avg_l3` | Mean expected goals for over the previous 3 matches | last 3 |
| `ust_team_xg_avg_l5` | Mean expected goals for over the previous 5 matches | last 5 |
| `ust_team_xga_avg_l3` | Mean expected goals allowed over the previous 3 matches | last 3 |
| `ust_team_xga_avg_l5` | Mean expected goals allowed over the previous 5 matches | last 5 |
| `ust_team_ppda_att_l3` | Average PPDA attacking value (`ppda.att/ppda.def`) as a pressing proxy | last 3 |
| `ust_team_ppda_allowed_att_l3` | Average PPDA allowed (`ppda_allowed.att/ppda_allowed.def`) | last 3 |
| `ust_team_deep_entries_l3` | Mean number of deep touches (`deep`) | last 3 |
| `ust_team_deep_allowed_l3` | Mean `deep_allowed` | last 3 |
| `ust_team_goals_for_avg_l5` | Mean actual goals scored | last 5 |
| `ust_team_goals_against_avg_l5` | Mean actual goals conceded | last 5 |
| `ust_team_xpts_avg_l5` | Mean Understat expected points | last 5 |
| `ust_team_shot_open_play_share_l5` | Share of shots from open play (open-play shots ÷ total shots) derived from `shotsData` | last 5 |
| `ust_team_shot_set_piece_share_l5` | Share of shots from set pieces | last 5 |
| `ust_team_avg_shot_distance_l5` | Average shot distance (meters) using Understat XY coordinates | last 5 |

These metrics are stored per (`game_id`, `team`) so they can be merged for both the team and its opponent.

## Starting XI Metrics

Lineup strength and continuity are extracted from `rostersData`. For every match we capture the 11 players whose `positionOrder < 17` as the starters, look up their cumulative contributions from prior matches, and compute lineup-level features:

| Feature | Definition |
| --- | --- |
| `ust_xi_prior_minutes_total` | Sum of minutes logged by the announced starters across all previous matches that season |
| `ust_xi_prior_minutes_avg` | Average minutes per starter (`total ÷ 11`) |
| `ust_xi_prior_xg_per90_avg` | Mean of each starter’s rolling xG per 90 (using cumulative pre-match xG & minutes) |
| `ust_xi_prior_xa_per90_avg` | Mean of starters’ rolling xA per 90 |
| `ust_xi_prior_shots_per90_avg` | Mean rolling shots per 90 across starters |
| `ust_xi_prior_key_passes_per90_avg` | Mean rolling key passes per 90 |
| `ust_xi_share_zero_min` | Fraction of starters who have logged zero minutes before this match (debut/returning from long layoff) |
| `ust_xi_returning_starters_prev_match` | Share of the XI that also started the immediately previous league match |
| `ust_xi_returning_starters_last3` | Share of the XI that appeared in at least 2 of the last 3 matches |

All lineup metrics are captured before ingesting the current match data to avoid label leakage.

## Football-Data Odds Columns

Football-Data provides closing 3-way odds from multiple bookmakers. These are normalized into decimal, American, and implied probability formats so we can both backfill missing moneylines and expose bookmaker-specific signals.

| Column Prefix | Description |
| --- | --- |
| `fd_b365_ml_decimal` | Bet365 decimal price for the current team (home ⇒ `B365H`, away ⇒ `B365A`) |
| `fd_b365_ml_american` | Bet365 price converted to American format |
| `fd_b365_implied` | Implied probability from Bet365 decimal odds |
| `fd_b365_draw_decimal` / `fd_b365_draw_implied` | Bet365 draw price/implied probability (shared by both team rows) |
| `fd_ps_ml_decimal` | Pinnacle (PSH/PSA) decimal odds |
| `fd_ps_ml_american` | Pinnacle odds converted to American format |
| `fd_ps_implied` | Pinnacle implied probability |
| `fd_ps_draw_decimal` / `fd_ps_draw_implied` | Pinnacle draw price/implied probability |
| `fd_avg_ml_decimal` | Arithmetic mean of Bet365 & Pinnacle decimal odds for the team |
| `fd_avg_implied` | Implied probability from `fd_avg_ml_decimal` |

Whenever `game_results.home_moneyline_close` / `away_moneyline_close` are missing for soccer, Bet365 prices are converted to American format and used to populate the core `moneyline` column so training rows are no longer dropped.

## Season Limits

All soccer feature engineering and model training is constrained to the 2021–2025 seasons (inclusive). Requests for seasons outside that range will be clipped to that window, ensuring the Understat + Football-Data data completeness assumptions remain valid.
