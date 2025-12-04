# Training Data Quality Review & Ranking

This document provides a review of the training data used for each league and ranks them based on the quality, depth, and predictive potential of the available features.

## Ranking Summary

| Rank | League | Data Quality | Key Strengths | Key Weaknesses |
| :--- | :--- | :--- | :--- | :--- |
| 1 | **NFL** | ⭐⭐⭐⭐⭐ (Excellent) | Play-by-play EPA/Success Rate, granular injury data, weather/travel context. | None significant. |
| 2 | **NBA** | ⭐⭐⭐⭐ (Very Good) | "Four Factors" efficiency ratings, player-level features, extensive rolling windows. | Relies on aggregated team stats vs. raw tracking data. |
| 3 | **Soccer** | ⭐⭐⭐⭐ (Good) | **Understat xG/xGA** integration is a powerful predictive signal. | Limited to leagues covered by Understat. |
| 4 | **CFB** | ⭐⭐⭐ (Moderate) | Some advanced stats available, but less granular than NFL. | High variance in team quality, less reliable player data. |
| 5 | **NCAAB** | ⭐⭐ (Basic) | Basic box scores & team form only. | **Missing efficiency metrics** (KenPom/Torvik style) which are standard for CBB. |
| 6 | **NHL** | ⭐⭐ (Basic) | Basic box scores & team form only. | **Missing possession metrics** (Corsi/Fenwick) and shot quality (xG). |

---

## Detailed Analysis

### 1. NFL (National Football League)
**Status**: 🟢 **Gold Standard**
- **Data Source**: `nfl_data_py` (Play-by-play data).
- **Features**:
    - **Advanced Metrics**: Expected Points Added (EPA) per play (Offense/Defense), Success Rate.
    - **Context**: Detailed weather (Temp, Wind, Precip, Dome), Rest Days, Travel Distance.
    - **Injuries**: Highly specific injury flags (QB Out, Skill Position Out) derived from official reports.
    - **Form**: Rolling averages (3, 5 games) of EPA and Success Rate.
- **Verdict**: The model uses state-of-the-art metrics. EPA is widely considered the best predictor for NFL outcomes.

### 2. NBA (National Basketball Association)
**Status**: 🟢 **Strong**
- **Data Source**: Aggregated team and player stats.
- **Features**:
    - **Efficiency**: Offensive, Defensive, and Net Ratings (points per 100 possessions).
    - **Pace**: Possessions per 48 minutes.
    - **Player Impact**: Aggregated player features, allowing the model to account for roster changes (to some extent).
    - **Trends**: Extensive rolling windows (3, 5, 10, 15, 20 games) to capture streaks and slumps.
- **Verdict**: Strong foundation based on Dean Oliver's "Four Factors" principles. Adding lineup data or "minutes weighted" player stats could be a future enhancement.

### 3. Soccer (EPL, La Liga, Bundesliga, Serie A, Ligue 1)
**Status**: 🟢 **Strong**
- **Data Source**: `football-data.co.uk` (Odds) + `Understat` (Advanced Stats).
- **Features**:
    - **Expected Goals (xG)**: The holy grail of soccer analytics. Measures the quality of chances created/conceded.
    - **xPTS**: Expected points based on xG performance.
    - **Market Data**: Historical odds from multiple bookmakers (Bet365, Pinnacle).
- **Verdict**: The inclusion of xG data places this model well ahead of basic form-based models. It captures "performance vs. results" discrepancies very well.

### 4. CFB (College Football)
**Status**: 🟡 **Moderate**
- **Data Source**: Basic box scores + some advanced stats.
- **Features**:
    - **Stats**: Includes some efficiency metrics, but lacks the play-by-play granularity of the NFL pipeline.
    - **Variance**: CFB has high roster turnover and massive skill gaps, making team-level aggregates less stable than in pros.
- **Verdict**: Decent, but could be improved by incorporating recruiting rankings (talent composite) or returning production metrics to better handle year-over-year changes.

### 5. NCAAB (College Basketball)
**Status**: 🔴 **Basic**
- **Data Source**: Kaggle / March Madness compact results.
- **Features**:
    - **Limited**: Relies primarily on wins/losses, scores, and simple rolling form.
    - **Missing**: No possession-adjusted efficiency metrics (KenPom/Torvik), which are essential for comparing teams across different tempos.
- **Verdict**: **High priority for upgrade.** Without efficiency ratings, the model cannot accurately compare a slow-paced defensive team vs. a fast-paced offensive team.

### 6. NHL (National Hockey League)
**Status**: 🔴 **Basic**
- **Data Source**: Basic box scores.
- **Features**:
    - **Limited**: Goals, shots, and recent form.
    - **Missing**: No Corsi/Fenwick (shot attempts), High-Danger Chances, or xG.
- **Verdict**: Hockey is high-variance. Predicting it without shot quality or possession metrics is extremely difficult. This model is likely essentially a "hot hand" detector.

## Recommendations

1.  **Upgrade NCAAB**: Integrate possession-adjusted efficiency metrics (Off/Def Rating, Tempo). Even calculating raw "Points per Possession" from box scores would be a significant upgrade over raw scores.
2.  **Upgrade NHL**: Calculate or source basic possession metrics (Shot Attempts % instead of just Goal %) to better measure ice tilt.
3.  **Maintain NFL/NBA/Soccer**: These pipelines are robust. Focus on model tuning rather than new data features for now.
