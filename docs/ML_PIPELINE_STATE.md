# Machine Learning Pipeline State

Last reviewed: 2026-04-27

This report summarizes the current betting ML pipeline, where it is strong,
where it is weak, and what should happen next to move predictions toward
beating the market. It is based on the current local DB quality report generated
at `2026-04-27T16:53:12Z`.

## Executive Summary

The pipeline is now structurally pointed in the right direction: it has
leakage-safe model inputs, explicit market baselines, rolling-origin validation,
feature contract coverage reporting, CLV reporting, and a fail-closed publishing
gate. That is the correct foundation for finding real betting edges.

The current predictions should not be treated as market-beating yet. There are
no approved rules, no passing approved rules, and no publishable paid bet list.
The best candidates show encouraging ROI and Brier improvement, especially in
LALIGA totals, but they fail the strict gate because sample sizes are still too
small and CLV win rates are poor.

The next major objective is to stop optimizing around apparent ROI alone and
make CLV the main development target. A model that does not consistently beat
the closing market is unlikely to have durable edge.

## Current Pipeline Shape

The current quality report evaluates settled predictions made before game start:

| Area | Current value |
| --- | ---: |
| Totals prediction rows | 5,676 |
| Moneyline prediction rows | 5,676 |
| Expanded totals bet rows | 10,984 |
| Expanded moneyline bet rows | 11,352 |
| Passing approved rules | 0 |
| Publishable paid list | false |
| CLV summary groups | 1,591 |

The launch gate remains intentionally strict:

- minimum narrow-rule sample: 150 bets
- minimum multi-league sample: 300 bets
- minimum ROI: 5%
- bootstrap ROI lower bound: above 0%
- model Brier must beat no-vig market Brier
- average CLV must be positive
- CLV win rate must be above 50%
- rules must be explicitly approved before paid publishing

Current approved rules remain empty in `config/published_rules.yml`.

## Strong Areas

### 1. Leakage controls are much better than before

The canonical builders use only pre-game market snapshots and shifted historical
team features. Soccer Understat rolling features are shifted before the target
match. The quality report evaluates only settled predictions made before game
start.

This is a major strength. It means bad results are more likely to be real signal
weakness rather than hidden leakage or accidental target contamination.

### 2. The pipeline is market-aware

Models are evaluated against no-vig market probability, not just against raw game
outcomes. The training and benchmark path supports residual-style models around
market probabilities, which is the right framing. The model should answer:
"where is the market wrong?", not "what happens in the game?"

### 3. Publishing is fail-closed

The product layer does not publish paid recommendations unless approved rules
pass the quality gate. This prevents optimistic backtest artifacts from leaking
into subscriber-facing picks.

### 4. Feature contracts now expose real signals

The training contracts now include canonical feature columns rather than only a
small base feature subset.

| Market | Training rows | Feature count |
| --- | ---: | ---: |
| Totals | 2,259 | 149 |
| Moneyline | 4,524 | 146 |

Feature groups now eligible for training include:

- market features
- rest and back-to-back features
- home/away splits
- NBA rolling advanced metrics
- soccer Understat and Football-Data form metrics
- future availability features when the data is populated

### 5. Soccer feature density has improved

Soccer model inputs now include shifted Understat team metrics for the current
season. This produced usable density for xG/xGA/PPDA/deep-entry style features
across the release soccer leagues. That is the strongest non-market feature area
currently available.

### 6. CLV is now visible

The report now includes CLV summaries and candidate rule rankings with CLV
metrics. This gives the pipeline a way to distinguish real edge from noisy ROI.

## Weak Areas

### 1. No current rule is market-beating by the launch standard

There are no passing approved rules and no publishable paid bet list. The
pipeline should continue to block paid picks.

Top current candidates:

| Rank | Rule | Bets | ROI | Brier delta vs market | Avg CLV | CLV win rate | Failure reasons |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `laliga_totals_random_forest_edge_004` | 128 | 19.93% | +0.0082 | +0.0129 | 14.66% | sample below 150, CLV win rate below 50% |
| 2 | `laliga_totals_ensemble_edge_004` | 110 | 20.97% | +0.0039 | +0.0230 | 17.35% | sample below 150, CLV win rate below 50% |
| 3 | `laliga_totals_gradient_boosting_edge_004` | 110 | 20.97% | +0.0039 | +0.0230 | 17.35% | sample below 150, CLV win rate below 50% |
| 4 | `nba_totals_random_forest_edge_020` | 120 | 17.86% | -0.0104 | +0.0598 | 23.93% | sample below 150, Brier worse than market, CLV win rate below 50% |

The ROI numbers are attractive, but the CLV win rates are weak. That is the
clearest warning that these are not durable market-beating rules yet.

### 2. Availability data is effectively missing

Feature contract coverage shows:

| Market | Availability feature count | Availability non-null |
| --- | ---: | ---: |
| Totals | 0 | 0.0% |
| Moneyline | 0 | 0.0% |

There is ESPN NBA injury ingestion code in the repo, but the current DB
`injury_reports` table is empty. This removes one of the highest-leverage data
sources for NBA totals and moneyline.

### 3. Existing DB predictions may not reflect the expanded contracts

The latest quality report evaluates predictions already stored in the DB. Some
of those predictions likely predate the expanded training contracts, so the
current candidate quality should be treated as a baseline, not as the final read
on the richer feature set.

The next benchmark run is important because it will test models after the
expanded feature eligibility.

### 4. Most total-score models still do not beat the sportsbook line

On absolute total error, the sportsbook line is still better in most groups.
The only current narrow exception is very small:

| League | Model | Games | Model MAE | Line MAE | Delta |
| --- | --- | ---: | ---: | ---: | ---: |
| LALIGA | random_forest | 158 | 1.1818 | 1.1883 | -0.0064 |

That is not enough to claim a durable edge. It is a weak positive hint, not a
product signal.

### 5. CLV is the main blocker

The top candidates show positive average CLV but low CLV win rate. That can
happen when a few larger line moves dominate the average while most individual
bets do not beat close. For a durable betting product, the model needs to
produce edges that consistently survive into closing prices.

### 6. Current feature density is uneven

Feature coverage is improved, but still uneven:

| Market | Soccer features | NBA advanced | Availability | Rest | Market |
| --- | ---: | ---: | ---: | ---: | ---: |
| Totals | 25.60% | 25.19% | 0.00% | 98.70% | 100.00% |
| Moneyline | 25.57% | 25.20% | 0.00% | 98.65% | 100.00% |

The low soccer/NBA percentages are partly because the release set spans multiple
leagues, but the zero availability coverage is a true hole.

## What To Do Next

### 1. Run the full portfolio benchmark after the contract expansion

This is the next most important step. The pipeline now exposes far more features
to training, but the current quality report mainly evaluates existing stored DB
predictions.

Run:

```powershell
poetry run python -m src.models.train_betting --benchmark `
  --benchmark-config config/betting_benchmark.yml `
  --benchmark-output-dir reports/betting_benchmarks
```

Acceptance criteria:

- benchmark emits `candidate_rule_rankings`
- best candidates include Brier, ROI, bootstrap, CLV, drawdown, and bet count
- no candidate is promoted automatically
- paid publishing remains fail-closed

### 2. Populate NBA injury and availability data

NBA availability is the largest obvious missing signal. Use the existing ESPN
NBA injury ingestion path and verify that `injury_reports` is populated.

Recommended workflow:

```powershell
poetry run python -m src.data.ingest_injuries
poetry run python -m src.models.prediction_quality `
  --rules config/published_rules.yml `
  --output reports/prediction_quality/latest_quality_report.json
```

Acceptance criteria:

- `injury_reports` has current NBA rows
- `availability_feature_count` becomes non-zero
- availability non-null coverage becomes non-zero for NBA rows
- NBA candidate Brier and CLV are re-evaluated

### 3. Make CLV the primary model-selection target

Do not select candidates by ROI alone. The top candidates currently fail CLV win
rate. Candidate ranking should continue to prioritize:

1. Brier beats no-vig market
2. positive bootstrap ROI lower bound
3. positive average CLV
4. CLV win rate above 50%
5. acceptable drawdown
6. enough bets

The working assumption should be: if a rule does not beat close, ROI is probably
noise.

### 4. Improve price selection and odds timing

The model can only beat the market if the pipeline evaluates bets at realistic
available prices. Next improvements should include:

- freshness buckets by hours before start
- exclusion of stale odds near game time
- book-level CLV reporting
- edge decay by time window
- candidate rankings by best book vs selected book

The goal is to find whether any model edge is available at real prices before
the market moves.

### 5. Focus feature work by league

Prioritized feature investments:

1. NBA: injury/availability, player impact aggregates, recent pace, offensive
   rating, defensive rating, rest and travel.
2. Soccer: continue improving Understat density, add lineup/player strength only
   when known pre-game, and avoid actual lineup leakage.
3. NHL: rest, back-to-back, home/away form, recent goals for/against, and goalie
   proxy only if a stable free pre-game source exists.

Avoid fragile scraping unless the source is stable enough to run daily without
manual repair.

### 6. Keep promotion manual and strict

No automatic approval should be added. Promotion should remain explicit:

- candidate appears in benchmark/quality report
- sample size passes threshold
- ROI and bootstrap lower bound pass
- Brier beats market
- CLV passes
- rule is manually moved into `approved_rules`

Until that happens, the app should show analytics but not paid picks.

## Recommended Next Engineering Pass

The next implementation pass should do these in order:

1. Run the full benchmark using the expanded contracts.
2. Add a short benchmark summary report under `reports/betting_benchmarks`.
3. Run NBA injury ingestion and verify DB coverage.
4. Rebuild quality report and compare NBA totals before/after availability.
5. Add book/time-window CLV slices to candidate rankings.
6. Only then decide whether any candidate deserves more focused tuning.

## Current Bottom Line

The pipeline is now robust enough to find real edges, but it has not found a
publishable market-beating rule yet. The strongest signal is LALIGA totals, but
it is below sample size and does not beat the CLV win-rate gate. NBA totals has
some ROI signal but fails Brier vs market and CLV win rate.

The path to market-beating predictions is clear: fill the availability data gap,
run the expanded-feature portfolio benchmark, and optimize candidates against
closing-line performance instead of historical ROI alone.
