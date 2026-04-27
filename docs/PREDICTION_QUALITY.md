# Prediction Quality Gates

Paid picks are blocked until a predeclared betting rule passes the quality report.
The current quality workflow is intentionally conservative: rules in
`config/published_rules.yml` are candidates until enough out-of-sample evidence
exists.

## Run The Report

```powershell
poetry run python -m src.models.prediction_quality `
  --rules config/published_rules.yml `
  --output reports/prediction_quality/latest_quality_report.json
```

The report uses only settled predictions made before game start. For each rule it
returns ROI, win rate, Brier score, market baseline Brier score, bootstrap ROI
confidence intervals, calibration bins, max drawdown, and losing streak.
The report includes `publishable_profitable_list_exists` and
`passing_approved_rule_ids`; paid picks are blocked unless at least one approved
rule is listed there.

Candidate validation artifacts can be evaluated with the same strict gate:

```powershell
poetry run python -m src.models.prediction_quality `
  --rules config/published_rules.yml `
  --benchmark-predictions reports/prediction_quality/totals_logistic_nba_validation_predictions.parquet `
  --output reports/prediction_quality/latest_quality_report.json
```

DB-backed settled predictions are reported as the `existing_predictions` source.
Each benchmark file is reported as a `candidate_benchmark_output` source. Both
sources call the same rule evaluator and the same launch-gate thresholds.

## Build Model Inputs

Use the canonical builders when training new betting models. They construct one
leakage-safe row per settled game from odds snapshots and results.

```powershell
poetry run python -m src.features.betting_model_input `
  --market totals `
  --leagues NBA,NHL,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1 `
  --output reports/prediction_quality/totals_model_input.parquet
```

Totals rows include opening line, latest pre-game line, line movement, no-vig
probabilities, market hold, line dispersion, best available side prices, rolling
prior team scoring/allowed form, rest, and back-to-back flags. Moneyline exports
are one row per bettable side/team. The date filter is strictly before game start
for market snapshots and strictly before the target game for team-history
features.

## Train And Validate Candidate Models

Train betting-outcome models with rolling-origin validation:

```powershell
poetry run python -m src.models.train_betting `
  --market totals `
  --model-type logistic `
  --leagues NBA `
  --output-dir reports/prediction_quality `
  --models-dir reports/prediction_quality/models
```

Supported `--model-type` values are `market_only`, `line_movement`, `logistic`,
`gradient_boosting`, `random_forest`, and `xgboost`. Start every league with
`market_only` and `line_movement`; candidate models must beat those baselines
out of sample before their rules can move toward approval.

The trainer writes:

- a model bundle with the feature contract;
- row-level validation predictions;
- a quality JSON with Brier/log-loss versus market, fold metrics, ROI, bootstrap
  ROI interval, calibration, drawdown, and losing streak for the validation rule.

The current NBA totals logistic smoke test is not launchable: it trained and
validated successfully, but underperformed the market baseline and produced
negative validation ROI. Treat that as a working pipeline check, not a product
signal.

## Launch Gate

A subscriber-facing rule must satisfy all of these:

- narrow league rule: at least 150 settled pre-game bets;
- multi-league rule: at least 300 settled pre-game bets;
- ROI at least 5%;
- bootstrap 95% ROI lower bound above 0%;
- model Brier score beats the no-vig market baseline;
- rule is predeclared as `approved` in `config/published_rules.yml`.

Candidate rules are monitored but should not be shown as paid recommendations.

## Publish The Paid List

`config/published_rules.yml` is the single source of truth for
subscriber-facing rules. The publish command reads approved rules from that file,
rebuilds the quality report, and applies only currently passing approved rules to
current pre-game predictions.

```powershell
poetry run python -m src.predict.publishable_bets `
  --rules config/published_rules.yml `
  --output reports/publishable_bets/latest_publishable_bets.json `
  --quality-output reports/publishable_bets/latest_quality_report.json
```

The command is fail-closed. If no approved rule currently passes the launch gate,
or if no current prediction matches a passing approved rule, it exits non-zero and
removes any stale paid bet-list file at `--output`.

When it succeeds, the JSON list contains rule id, market, league, game id,
teams, side, American odds, edge, model probability, no-vig market probability,
and the historical quality summary that justified publishing the rule.

## Promotion Workflow

Promotion is explicit and gated. A candidate rule can move into `approved_rules`
only when it passes at least one strict report source:

```powershell
poetry run python -m src.predict.publishable_bets promote `
  --rule-id nba_totals_candidate `
  --rules config/published_rules.yml `
  --quality-report reports/prediction_quality/latest_quality_report.json
```

If the candidate does not pass, the command exits non-zero and does not edit the
rules file. Approved rules that later fail the gate produce no paid bets; add
`disabled: true` or set `status: disabled` in `config/published_rules.yml` to
turn one off intentionally.

## Current Intended Use

Use this report as the quality gate in front of any recommendation feed. The app
can still show historical analytics, but paid over/under picks should be hidden
unless an approved rule passes the gate.
