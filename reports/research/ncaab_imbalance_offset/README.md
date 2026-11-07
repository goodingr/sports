# NCAA basketball totals and moneyline imbalance

This is a frozen forecast experiment. It tests whether DraftKings moneyline
imbalance improves its full-game totals probability on the same fixture.
The model uses one negative-only coefficient and no intercept. The frozen
protocol specifies the quote clock, result evidence and final review.

The development archive had 1,520 eligible quote fixtures. Of these, 1,111
had exact original final-score evidence across 83 Eastern dates and 409
had no matching score receipt. The first retained score receipt was
December 4, 2025, after the kickoff of 83 unmatched fixtures. Another 45
fixtures had conflicting archived quote identities and were excluded before
grading. The fitted coefficient is −0.03450546975128979. A stricter check
for kickoff conflicts in earlier partial receipts excluded 12 fixtures from
the initial replay, including two that had matching final scores.

`frozen_fit.json` records the cohort hash, cutoff, source-tree commit and
coefficient. Run the experiment from the commit that introduced that
artifact so its imported evidence and pricing helpers also match.

```powershell
$artifactCommit = git log -1 --format=%H -- reports/research/ncaab_imbalance_offset/frozen_fit.json
git worktree add --detach <review-worktree> $artifactCommit
Set-Location <review-worktree>
poetry run python -B -m reports.research.ncaab_imbalance_offset.experiment --db <warehouse-path>
poetry run python -B -m reports.research.ncaab_imbalance_offset.review --db <warehouse-path>
```

The review stays closed until March 22, 2027 at midnight Eastern. It uses
ordinary provider captures from November 8 through the March 15 capture
cutoff and requires 400 non-push fixtures across at least 30 Eastern dates.
The analysis is a replay on quotes archived after the model freeze. It
provides forecast evidence without an issued-pick or wagering record.

## Quote coverage before review

Run this read-only preview from the current project checkout during collection.

```powershell
poetry run python -B -m scripts.ncaab_quote_coverage --db <warehouse-path>
```

It applies the frozen quote selector to the current archive and reports aggregate
receipt counts, eligible fixtures and Eastern dates, and ambiguity. An archived
quote can qualify before its fixture tips off. The preview reads no scores.
Run the final review separately at the scheduled cutoff.
