# Prediction Quality Gates

Paid picks require an approved rule backed by a completed prospective paper
trial. Candidate quality reports support research and selection of future trials.
The publisher evaluates the frozen paper review against the current launch
thresholds and its exact model and execution contract.

## Run The Report

```powershell
poetry run python -m src.models.prediction_quality `
  --rules config/published_rules.yml `
  --output reports/prediction_quality/latest_quality_report.json
```

The report uses only settled predictions made before game start. For each rule it
returns ROI, win rate, Brier score, market baseline Brier score, bootstrap ROI
confidence intervals, calibration bins, max drawdown, losing streak, CLV, and
the odds-timing filter used to exclude stale snapshots.
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
are one row per side and game. Quotes must precede kickoff, and team form and
injury features use the selected quote time as their cutoff. Training excludes
three-way soccer moneylines and historical NBA and soccer enrichments that lack
quote-time provenance.

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

Each validation fold keeps all rows for a game together. Model fitting and
calibration use outcomes available before the earliest quote in the fold.
Historical results lack observed settlement times, so the benchmark assumes
they become available six hours after kickoff.

The trainer writes:

- a model bundle with the feature contract;
- row-level validation predictions;
- a quality JSON with Brier/log-loss versus market, fold metrics, ROI, bootstrap
  ROI interval, calibration, drawdown, and losing streak for the validation rule.

The current NBA totals logistic smoke test is not launchable: it trained and
validated successfully, but underperformed the market baseline and produced
negative validation ROI. Treat that as a working pipeline check, not a product
signal.

For the full paid-picks promotion grid, run the predeclared rolling-origin
benchmark:

```powershell
poetry run python -m src.models.train_betting `
  --benchmark `
  --benchmark-config config/betting_benchmark.yml `
  --benchmark-output-dir reports/betting_benchmarks
```

The benchmark ranks only rules declared in `config/betting_benchmark.yml`, adds
book/timing CLV slices to candidate rankings, and never auto-promotes a rule.
For a chronological holdout, pass `--holdout-start YYYY-MM-DD` to
`python -m src.models.betting_benchmark`. The benchmark ranks rules using
development outcomes available before that UTC boundary, then evaluates only
the top candidate meeting the sample floor on quotes at or after it.
Each run also writes `reports/betting_benchmarks/triage_<timestamp>.json`. The
triage artifact groups candidate failures by league, market, model, and strict
failure reason, reports timing/CLV data gaps, and lists the failing candidate
rules closest to passing with the remaining failed gates and shortfall metrics.

For the NBA totals research lane, generate a focused report from the benchmark:

```powershell
poetry run python -m src.models.nba_totals_research `
  --benchmark reports/betting_benchmarks/betting_benchmark_<timestamp>.json `
  --output reports/betting_benchmarks/nba_totals_research_<timestamp>.json
```

This report summarizes NBA totals strict-gate failures, best candidate cohorts,
book/timing CLV slices, and whether the current artifact contains a true
with-versus-without availability feature comparison.

## Audit Odds Coverage

When strict benchmark rules produce too few qualifying bets, run the odds
coverage audit before changing any rule or model setting:

```powershell
poetry run python -m src.data.odds_coverage `
  --leagues NBA,NHL,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1 `
  --output reports/odds_coverage/latest.json
```

The JSON is designed for daily tracking. It reports settled games, usable
pre-game odds pairs, opening/current/closing odds availability, CLV availability,
stale odds excluded by the `max_hours_before_start` timing filter, book coverage,
hours-before-start buckets, and selected-book versus best-book availability by
league and market. This is diagnostic infrastructure only; it does not change
promotion rules, approved rules, or paid publish behavior.

## Audit CLV Lineage

When a market shows positive ROI but weak or inconsistent CLV, audit whether the
current odds row and closing line are traceable to compatible sources before
tuning models:

```powershell
poetry run python -m src.data.clv_lineage_audit `
  --league NBA `
  --market totals `
  --output reports/data_quality/clv_lineage_nba_totals_latest.json
```

The report flags stale selected snapshots, missing `total_close`, close lines
that are not observed in pre-game odds snapshots, inferred close candidates that
are older than the selected current snapshot, and selected-book rows that do not
match the inferred close-line source. It is diagnostic only and does not change
promotion rules, approved rules, or paid publish behavior. The summary separates
odds-backed settled games from historical games without pre-game odds snapshots;
strict benchmark CLV should be trusted only for the odds-backed row set.

For existing rows, backfill stored `total_close` provenance only where the close
line maps to a single latest pre-game snapshot/book candidate:

```powershell
poetry run python -m src.data.backfill_total_close_provenance `
  --league NBA `
  --dry-run `
  --output reports/data_quality/total_close_provenance_backfill_dry_run_latest.json

poetry run python -m src.data.backfill_total_close_provenance `
  --league NBA `
  --write `
  --output reports/data_quality/total_close_provenance_backfill_write_latest.json
```

If the stored scalar `total_close` itself needs to be rebuilt from odds history,
use the latest pre-game strategy. This rewrites only games with pre-game totals
odds and chooses the latest pre-game snapshot using the standard book priority:

```powershell
poetry run python -m src.data.backfill_total_close_provenance `
  --league NBA `
  --strategy latest-pregame `
  --dry-run `
  --output reports/data_quality/total_close_provenance_latest_pregame_dry_run_latest.json

poetry run python -m src.data.backfill_total_close_provenance `
  --league NBA `
  --strategy latest-pregame `
  --write `
  --output reports/data_quality/total_close_provenance_latest_pregame_write_latest.json
```

The totals benchmark counts same-book closing quotes captured after the decision
and within 90 minutes before kickoff. Legacy `game_results.total_close` is retained
for lineage auditing.

The historical odds backfill uses the provider's archived snapshot time and only
attaches pregame odds to uniquely matched final fixtures. It preserves existing
schedule, score, and close records. The [historical odds API](https://the-odds-api.com/liveapi/guides/v4/#get-historical-odds)
requires a paid provider plan.

## Repair NBA Availability IDs

If NBA availability coverage reports missing player mappings, preview the safe
backfill before writing:

```powershell
poetry run python -m src.data.backfill_injury_player_ids `
  --league NBA `
  --dry-run `
  --output reports/data_quality/injury_player_id_backfill_dry_run.json
```

The command uses high-confidence exact name/team evidence from existing injury
rows, NBA player stats, and ESPN active injury athlete references. It reports
resolvable, ambiguous, and unresolved rows. Apply only non-ambiguous matches with:

```powershell
poetry run python -m src.data.backfill_injury_player_ids `
  --league NBA `
  --write `
  --output reports/data_quality/injury_player_id_backfill_write.json
```

## Launch Gate

A subscriber-facing rule must satisfy all of these:

- narrow league rule: at least 150 settled pre-game bets;
- multi-league rule: at least 300 settled pre-game bets;
- ROI at least 5%;
- bootstrap 95% ROI lower bound above 0%;
- model Brier score beats the no-vig market baseline;
- at least 80% closing-line coverage, positive average closing-line value, and
  positive closing-line value on more than half of matched bets;
- latest available odds snapshot is not older than the configured
  `max_hours_before_start`;
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
Automation may pass `--allow-empty` so this valid fail-closed state exits zero
without publishing stale picks.

When it succeeds, the JSON list contains rule id, market, league, game id,
teams, side, American odds, edge, model probability, no-vig market probability,
and the historical quality summary that justified publishing the rule.

## Promotion Workflow

Promotion reads the registered prospective trial and its frozen review directly
from the warehouse. A completed passing review must satisfy the current launch
thresholds. Approval carries the pinned model SHA-256, book, source, edge,
quote age, decision window, and model-input provenance requirements into live
publication.

```powershell
poetry run python -m src.predict.publishable_bets promote-paper `
  --trial-id nba_totals_random_forest_draftkings_v1 `
  --rules config/published_rules.yml
```

An incomplete or failing review exits non-zero before changing the rules file.
Historical report files cannot promote a rule. Approved rules that later fail
the current thresholds produce no paid bets; add
`disabled: true` or set `status: disabled` in `config/published_rules.yml` to
turn one off intentionally.

## Issued Pick Record

The publisher commits the active release and the first issued pick per fixture
and market to SQLite in one transaction. The API reads this committed release.
The JSON output remains an export for operations.

A paid pick requires model artifact identity and retained quote provenance.
The observed quote must be an intact pair from the same book and provider
snapshot, must match the predicted line and price, and must be at most 15 minutes
old when issued. Untraced legacy rows cannot be issued. Quarter-point totals
are excluded because the current grader supports integer and half-point totals.

The original price and line remain fixed in the record. Changed prices or
opposite signals cannot replace an issued selection. Provider aliases representing
the same ordered teams and nearby start cannot create a second issued bet.
An active pick expires when its quoted price is stale or superseded.

The publisher grades final results at one unit per issued pick, includes pushes
in total stake, and excludes voids from ROI. Reschedules, fixture identity
changes, and quarantined fixtures are voided. Corrected official results update
the public return while retaining the previous grade in the audit history.

The public `/record` page and `GET /api/bets/record` endpoint use this issued
ledger. The model-results history remains separate. To revoke the active release
while preserving previously issued picks, run
`poetry run python -m src.predict.publishable_bets revoke`.

If an official result is withdrawn or becomes invalid, its current settlement
returns to pending and leaves the public return calculations. The original
grade and the retraction remain in the audit history.

## Prospective NBA Paper Trial

Totals regressors trained by `src.models.train_totals` now use separate
chronological fitting, uncertainty-calibration, and final test dates. A six-hour
result-availability assumption determines the purge before each later forecast
boundary.
The probability scale is the RMS error on the calibration dates. Training
residual spread is retained as a diagnostic.

Use a separate output directory for research artifacts.

```powershell
poetry run python -m src.models.train_totals --league NBA --model-type random_forest --input reports/research/totals_calibration/nba_draftkings_point_in_time_20261107T042053Z.parquet --models-dir reports/research/totals_calibration/shadow_models --metrics-dir reports/research/totals_calibration/shadow_metrics
```

The audited first experiment contains 104 quoted fixtures across 40 UTC dates.
On its 41 later test fixtures, held-out scaling produced Brier 0.2856 versus
0.3239 with training residual spread and 0.2503 for the same quoted no-vig
market. These are exploratory research measurements. The registered live pilot
continues to use the SHA-pinned artifact below.

The private trial in `config/paper_trial.yml` tests full-game NBA totals at
DraftKings. It pins the random forest artifact by SHA-256 and records the first
qualifying decision per fixture. A decision requires a model edge of at least
20 percentage points against the two-sided no-vig market and positive modeled
value at the quoted price. The model's two moneyline inputs must match the
snapshot that supplied the totals pair. The pinned model has zero dependence on
its spread input.

The capture window runs from 12 hours to 15 minutes before scheduled tipoff.
The latest totals snapshot for the fixture must have been fetched within 10 minutes of
capture. This measures the quote the feed returned, without assuming sportsbook
acceptance or recording any wager.

The scheduled pipeline captures eligible decisions immediately after a successful
NBA odds download, reusing that download. It runs settlement updates after data
ingestion. To run the same steps locally,
use these commands from the repository root.

```powershell
poetry run python -m src.predict.paper_ledger report
poetry run python -m src.predict.paper_ledger update
poetry run python -m src.data.ingest_odds --league NBA --market h2h,totals --region us --force-refresh
poetry run python -m src.predict.runner --leagues NBA --model-type random_forest --paper
```

The update command registers the trial on its first run and appends available
settlements. The report command reads the existing ledger. The refresh command
consumes live odds quota. The warehouse backup includes trial registrations,
decisions, and grades.

The report also shows retained capture runs and observations for scheduled
fixtures inside the decision window. It separates fully evaluated fixtures,
below-threshold opportunities, missing predictions, stale quotes, and inference
failures. Empty runs retain their timestamps. This coverage starts with the first
retained run and covers observations made since that time. Upgrading the worker
adds these tables without reconstructing earlier opportunities.

Known future fixture schedules are also retained prospectively. The report
separately counts known capture windows that elapsed without an observation,
including a fixture that never appeared in an in-window run. Large schedule or
identity changes are reported as ambiguous. These counts use retained schedule
observations rather than reconstructing arrival times from current game rows.

Settlement uses full-game final scores, including overtime, at one paper unit
per decision. A push returns the stake. Cancelled games and starts that moved
more than 10 minutes are voided. A small start-time correction uses the earlier
start for lead time and closing eligibility. A decision with less than 15 minutes
of lead time against the corrected start is voided. Closing line value uses the latest
paired totals quote from the same book and feed within 90 minutes before tipoff
and strictly after the decision. A missing close stays missing in the report.

Ledger updates re-evaluate source results and retain grade revisions, including
official score corrections and retracted final scores. Reports select the latest
grade observed by their evidence cutoff. Material outcome, profit, or CLV
corrections after the final review are flagged separately and block subscriber
approval. The original frozen review stays available for audit.

Preview the closing collector with
`poetry run python -m src.predict.paper_close_poll`. Add `--collect` to allow
one batched request when pending decisions need a close and have 10 to 60
minutes until tipoff. The collector checks a 200-credit reserve, spaces requests
at least 15 minutes apart, and allows up to three attempts per UTC day. A preview
only reads the warehouse. For collection, free quota probes check the active key
and then distinct configured keys. A qualifying key is used locally for that
request, under the shared attempt cap and cooldown.

Capture ends February 1, 2027 at midnight Eastern time. The final report freezes
grades one week later and requires at least 150 graded decisions, 5% flat-unit
ROI, a positive lower 95% date-clustered bootstrap bound, a lower Brier score than
the quoted no-vig market, and at least 80% same-book closing coverage with
positive average CLV and positive CLV on more than half the matched decisions.
Unresolved decisions at the grading cutoff fail review. This verdict remains
internal to the trial. The paid feed uses the separate approval workflow above.

## Current Intended Use

Use this report as the quality gate in front of any recommendation feed. The app
can still show historical analytics, but paid over/under picks should be hidden
unless an approved rule passes the gate.

Any public claim about ROI, win rate, beating the market, CLV, or profitability
must be listed in `docs/CLAIM_EVIDENCE.md` with a current report artifact.
