# Claim Evidence Register

Last updated: 2026-11-07 UTC

This register is the required substantiation file for any public claim about
paid picks, ROI, win rate, model edge, or beating the market. Do not publish a
claim unless it is listed here with a current evidence artifact.

## Policy

- Objective claims must be supported before they are used in marketing, pricing
  pages, emails, screenshots, social posts, or affiliate copy.
- Subscriber approval requires a completed, passing frozen prospective trial
  from `src.predict.paper_report`. `src.predict.paper_approval` verifies the
  original trial contract and current launch thresholds before promotion into
  `config/published_rules.yml`. Historical research reports remain diagnostic.
- A rule is sellable only after it appears in `approved_rules` and passes the
  current launch gate: sample size, ROI, bootstrap ROI lower bound, Brier versus
  no-vig market, closing-line coverage, positive average CLV, and CLV win rate.
  `src.predict.publishable_bets` rechecks that evidence when generating a release.
- Public issued-pick results come from the immutable original quotes and revised
  grades in `src.predict.published_ledger`, exposed at `/api/bets/record` and
  `/record`. Research backtests and the internal paper trial are separate records.
- FTC advertising substantiation reference:
  https://www.ftc.gov/legal-library/browse/ftc-policy-statement-regarding-advertising-substantiation
- Responsible-gaming reference:
  https://www.ncpgambling.org/responsible-gambling/internet-standards/

## Current Approved Claims

| Claim | Status | Evidence |
| --- | --- | --- |
| "Paid picks are quality-gated and hidden unless an approved rule passes." | Allowed | `config/published_rules.yml`, `src.predict.publishable_bets`, API tests |
| "No paid picks are currently approved." | Allowed until changed | `approved_rules: []` in `config/published_rules.yml` |
| "Historical candidates are being evaluated against closing-line value." | Allowed | `src.models.prediction_quality.build_clv_summary`, `src.models.betting_benchmark.rank_predeclared_rules` |

The November 7 research snapshot after raw-archive recovery found zero passing
rules among 3,024 evaluated configurations (669 met the sample threshold).
Evidence:
`reports/betting_benchmarks/after_raw_recovery/betting_benchmark_20261107T044956Z.json`.
Subsequent frozen NHL moneyline and soccer totals market-offset experiments also
failed to establish a profitable edge; their evidence is in
`reports/research/nhl_market_offset/result.json` and
`reports/research/soccer_poisson_offset/result.json`. The college basketball
moneyline consensus holdout produced one win across 32 decisions. That +5000
payout accounted for its entire gain. Its date-clustered ROI interval included
losses, its Brier score was worse than DraftKings, and it had zero qualifying
closing quotes. See `reports/research/ncaab_moneyline_consensus/result.json`.
The BetRivers soccer draw replay used early, same-snapshot moneyline and totals
quotes. On 456 later fixtures, the paired log-loss difference missed the frozen
gate. Its 165 selected draws returned −2.30% and had zero verified closes.
See `reports/research/soccer_draw_cross_market/result.json`.
The college football peer-movement replay missed its frozen log-loss gate on
305 later fixtures. It selected four bets, with one verified closing quote.
See `reports/research/cfb_peer_continuation/result.json`.
The college football archive audit found 540 later nightly DraftKings moneyline
entries and 121 possible same-book closes. It also identified 2,564 odds rows
attached to 2025 fixtures from a CollegeFootballData archive covering 2014–2024.
These rows were isolated from the local odds table with a verified recovery
copy. CollegeFootballData imports now require a unique matching kickoff and
ordered teams, preserve schedule and result fields, and stay out of betting
inputs. See `reports/research/non_nba_nightly_quote_diagnostic_20261107/result.json`.
The fixed soccer 1X2 power de-vig replay missed its log-loss gate on 514 later
fixtures. Its nine Bet365 selections returned −63.33%. The one-hour pre-tip
snapshots serve as historical closing-price proxies. See
`reports/research/soccer_power_devig/result.json`.
The frozen lower-division soccer replay found a zero short-away-turnaround
coefficient. It missed the accuracy gate on 620 later fixtures and returned
−6.11% on its 465 selections. See
`reports/research/soccer_away_turnaround/result.json`.
The NBA prospective trial is collecting evidence and its
scheduled final review is April 19, 2027 at midnight Eastern time.
The frozen [college basketball totals forecast](../reports/research/ncaab_imbalance_offset/README.md)
uses 1,111 verified development fixtures. Its first eligible capture is
November 8, 2026 Eastern time, and final review opens March 22, 2027 Eastern
time. This experiment measures forecast accuracy and selects no wagers.
Two archive screens stopped before scoring. The [NFL wind screen](../reports/research/nfl_forecast_wind/availability.md)
found no verified same-book closes on 94 later entries. The [soccer cross-book screen](../reports/research/soccer_hourly_cross_book/availability.md)
found seven later price opportunities across six dates, below its sample floor.

## Blocked Claims

These are blocked until this file is updated with passing report evidence:

- Guaranteed profit, guaranteed wins, "locks", or risk-free language.
- Any income, earnings, or replacement-income claim.
- "Beats the market", "profitable", "positive ROI", or specific ROI/win-rate
  language for paid picks unless backed by a current passing approved rule.
- Testimonials or influencer claims that imply typical customer profit unless
  the typicality and material connection disclosures have legal approval.

## Evidence Template

Use this template when a claim becomes supportable:

```text
Claim:
Status:
Audience/channel:
Evidence artifact:
Generated at:
Rule id(s):
Market/league:
Sample size:
ROI:
Bootstrap ROI lower bound:
Brier delta versus no-vig market:
Average CLV:
CLV win rate:
Approved by:
Expiration/review date:
```
