#!/usr/bin/env bash
#
# Linux/container equivalent of scripts/pipeline.ps1.
# Orchestrates ingest -> data hygiene -> paper grading -> prediction refresh
# and paper capture -> paid-picks publish gate -> quality benchmark.
#
# Defaults: full pipeline (with quality benchmark).
# Flags:
#   --skip-training    skip the benchmark step (use for hourly cadence)
#   --skip-benchmark   alias for --skip-training
#   --skip-prediction  skip legacy current prediction refresh
#   --skip-publish     skip the paid-picks publish gate
#   --soccer-only      only process soccer leagues
#   --skip-odds        skip odds + scores fetches (use cached data)
#   --skip-paper       skip the private paper ledger
#
# Each step's stdout/stderr is appended to logs/pipeline_<ts>.log.
#
# Failure model:
#   Individual sub-steps are tolerated — the pipeline continues on a single
#   failed league/model so a flaky odds API doesn't take down predict for
#   the entire portfolio. But we *count* every WARN, write a structured
#   summary to logs/pipeline_summary.json, and exit non-zero if a critical
#   step fails (e.g. predict failed for *every* league/model).
#   The non-zero exit is what scripts/run_job.sh turns into a fail heartbeat
#   for the container healthcheck.

set -uo pipefail

cd "$(dirname "$0")/.."

SKIP_TRAINING=0
SKIP_PREDICTION=0
SKIP_PUBLISH=0
SOCCER_ONLY=0
SKIP_ODDS=0
SKIP_PAPER=0

for arg in "$@"; do
	case "$arg" in
		--skip-training) SKIP_TRAINING=1 ;;
		--skip-benchmark) SKIP_TRAINING=1 ;;
		--skip-prediction) SKIP_PREDICTION=1 ;;
		--skip-publish) SKIP_PUBLISH=1 ;;
		--soccer-only)   SOCCER_ONLY=1 ;;
		--skip-odds)     SKIP_ODDS=1 ;;
		--skip-paper)    SKIP_PAPER=1 ;;
		*) echo "unknown arg: $arg" >&2; exit 64 ;;
	esac
done

mkdir -p logs
TS=$(date -u +%Y%m%d_%H%M%S)
LOG_FILE="logs/pipeline_${TS}.log"
SUMMARY_FILE="logs/pipeline_summary.json"
PAID_OUTPUT="reports/publishable_bets/latest_publishable_bets.json"
exec > >(tee -a "$LOG_FILE") 2>&1

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

# --- Failure tracking ----------------------------------------------------------
FAILURES=()
PREDICT_OK=0
PREDICT_FAIL=0
INGEST_OK=0
INGEST_FAIL=0
TRAIN_OK=0
TRAIN_FAIL=0
QUALITY_OK=0
QUALITY_FAIL=0
PUBLISH_OK=0
PUBLISH_FAIL=0
PAPER_OK=0
PAPER_FAIL=0
PAPER_ENABLED=0

if (( ! SKIP_PAPER )) && [[ -f config/paper_trial.yml ]]; then
	PAPER_ENABLED=1
fi

note_failure() {
	local step=$1; shift
	local detail=$*
	FAILURES+=("${step}: ${detail}")
	log "WARN: ${step} failed -> ${detail}"
}

# Run a sub-command and increment success/failure counters for the named step.
#   bucket_run <bucket> <description> -- <command...>
# Bucket is one of: ingest, train, predict, quality, publish, paper.
bucket_run() {
	local bucket=$1 desc=$2
	shift 2
	if [[ "$1" != "--" ]]; then
		echo "bucket_run: missing -- separator before command" >&2
		return 64
	fi
	shift
	if "$@"; then
		case "$bucket" in
			ingest)  INGEST_OK=$((INGEST_OK + 1)) ;;
			train)   TRAIN_OK=$((TRAIN_OK + 1)) ;;
			predict) PREDICT_OK=$((PREDICT_OK + 1)) ;;
			quality) QUALITY_OK=$((QUALITY_OK + 1)) ;;
			publish) PUBLISH_OK=$((PUBLISH_OK + 1)) ;;
			paper)   PAPER_OK=$((PAPER_OK + 1)) ;;
		esac
		return 0
	else
		local rc=$?
	fi
	case "$bucket" in
		ingest)  INGEST_FAIL=$((INGEST_FAIL + 1)) ;;
		train)   TRAIN_FAIL=$((TRAIN_FAIL + 1)) ;;
		predict) PREDICT_FAIL=$((PREDICT_FAIL + 1)) ;;
		quality) QUALITY_FAIL=$((QUALITY_FAIL + 1)) ;;
		publish) PUBLISH_FAIL=$((PUBLISH_FAIL + 1)) ;;
		paper)   PAPER_FAIL=$((PAPER_FAIL + 1)) ;;
	esac
	note_failure "$bucket" "$desc (exit=$rc)"
	return "$rc"
}

CORE_LEAGUES=(NFL NBA CFB NCAAB NHL)
SOCCER_LEAGUES=(EPL LALIGA BUNDESLIGA SERIEA LIGUE1)
DEFAULT_PAID_RELEASE_LEAGUES=(NBA NHL EPL LALIGA BUNDESLIGA SERIEA LIGUE1)

csv_to_array() {
	local raw=$1
	local -n out_ref=$2
	IFS=',' read -r -a out_ref <<< "$raw"
}

if (( SOCCER_ONLY )); then
	LEAGUES=("${SOCCER_LEAGUES[@]}")
else
	LEAGUES=("${CORE_LEAGUES[@]}" "${SOCCER_LEAGUES[@]}")
fi

if [[ -n "${PAID_RELEASE_LEAGUES:-}" ]]; then
	csv_to_array "$PAID_RELEASE_LEAGUES" PAID_LEAGUES
else
	PAID_LEAGUES=("${DEFAULT_PAID_RELEASE_LEAGUES[@]}")
fi

if (( SOCCER_ONLY )); then
	FILTERED_PAID_LEAGUES=()
	for league in "${PAID_LEAGUES[@]}"; do
		if printf '%s\n' "${SOCCER_LEAGUES[@]}" | grep -qx "$league"; then
			FILTERED_PAID_LEAGUES+=("$league")
		fi
	done
	PAID_LEAGUES=("${FILTERED_PAID_LEAGUES[@]}")
fi

PAID_LEAGUES_CSV=$(IFS=,; printf '%s' "${PAID_LEAGUES[*]}")

log "============================================="
log "pipeline start (skip_benchmark=$SKIP_TRAINING skip_prediction=$SKIP_PREDICTION skip_publish=$SKIP_PUBLISH soccer_only=$SOCCER_ONLY skip_odds=$SKIP_ODDS paper_enabled=$PAPER_ENABLED)"
log "leagues: ${LEAGUES[*]}"
log "paid release leagues: ${PAID_LEAGUES[*]}"
log "============================================="

# ----------------------------------------------------------------------------
# Pre-flight backup. Cheap insurance — if anything below corrupts the DB,
# we have a snapshot from seconds ago.
# ----------------------------------------------------------------------------
log "step: pre-flight backup"
if ! python scripts/backup_data.py; then
	note_failure "preflight_backup" "backup_data.py exited non-zero"
fi

# ----------------------------------------------------------------------------
# Step 1: ingest
# ----------------------------------------------------------------------------
CAPTURE_PAPER_FIRST=0
if (( PAPER_ENABLED && ! SKIP_PREDICTION && ! SKIP_ODDS )) && printf '%s\n' "${LEAGUES[@]}" | grep -qx NBA; then
	CAPTURE_PAPER_FIRST=1
	log "step: fetch NBA odds for paper capture before history ingestion"
	if bucket_run ingest "odds[NBA]" -- \
		python -m src.data.ingest_odds --league NBA --market h2h,totals --require-fresh; then
		bucket_run paper "NBA/random_forest capture" -- \
			python -m src.predict.runner --leagues NBA --model-type random_forest --paper-only
	else
		PAPER_FAIL=$((PAPER_FAIL + 1))
		note_failure "paper" "NBA odds refresh failed; paper capture could not run"
	fi
fi

log "step 1: smart history ingest"
bucket_run ingest "ingest_manager" -- \
	python -m src.data.ingest_manager --leagues "${LEAGUES[@]}"

if printf '%s\n' "${LEAGUES[@]}" | grep -qx NBA; then
	log "step 1a: nba rolling metrics"
	bucket_run ingest "nba_rolling_metrics" -- \
		python -m src.data.sources.nba_rolling_metrics --seasons 2024 2025

	log "step 1b: nba injury availability"
	bucket_run ingest "nba_injuries" -- \
		python -m src.data.ingest_injuries
fi

if (( ! SKIP_ODDS )); then
	log "step 2: live odds"
	for league in "${LEAGUES[@]}"; do
		if (( CAPTURE_PAPER_FIRST )) && [[ "$league" == NBA ]]; then continue; fi
		log "  fetching odds: $league"
		bucket_run ingest "odds[$league]" -- \
			python -m src.data.ingest_odds --league "$league" --market h2h,totals --force-refresh
	done

	log "step 3: live scores"
	# Bash's IFS+expansion form: assignment is local to this command line.
	CSV_LEAGUES=$(IFS=,; printf '%s' "${LEAGUES[*]}")
	bucket_run ingest "ingest_scores" -- \
		python -m src.data.ingest_scores --leagues "$CSV_LEAGUES"
else
	log "skipping odds + scores (--skip-odds)"
fi

# ----------------------------------------------------------------------------
# Step 1b: paid-release data hygiene.
# ----------------------------------------------------------------------------
if (( ${#PAID_LEAGUES[@]} > 0 )); then
	log "step 3a: paid-release data hygiene"
	bucket_run quality "prune_orphan_results" -- \
		python -m src.data.quality \
			--prune-orphans \
			--warn-only \
			--leagues "$PAID_LEAGUES_CSV"

	bucket_run quality "resolve_stale_scores" -- \
		python -m src.data.score_backfill \
			--resolve-stale \
			--lookback-days "${SCORE_BACKFILL_LOOKBACK_DAYS:-14}" \
			--leagues "$PAID_LEAGUES_CSV"

	bucket_run quality "close_unresolved_stale_games" -- \
		python -m src.data.quality \
			--finalize-scored \
			--close-unresolved-stale \
			--warn-only \
			--leagues "$PAID_LEAGUES_CSV"

	bucket_run quality "data_quality_warn" -- \
		python -m src.data.quality \
			--warn-only \
			--leagues "$PAID_LEAGUES_CSV"
fi

if (( PAPER_ENABLED )); then
	log "step 3b: update paper ledger"
	bucket_run paper "paper_ledger_update" -- python -m src.predict.paper_ledger update
fi

# ----------------------------------------------------------------------------
# Step 2: quality-first benchmark. This replaces legacy training as the
# promotion path for paid picks. It never auto-approves a rule.
# ----------------------------------------------------------------------------
RELEASE_QUALITY_PASSED=1
if (( ! SKIP_TRAINING || ! SKIP_PUBLISH )); then
	log "step 4: paid-picks data-quality gate"
	if [[ -n "$PAID_LEAGUES_CSV" ]] && python -m src.data.quality --leagues "$PAID_LEAGUES_CSV"; then
		QUALITY_OK=$((QUALITY_OK + 1))
	else
		RELEASE_QUALITY_PASSED=0
		QUALITY_FAIL=$((QUALITY_FAIL + 1))
		note_failure "quality" "paid-release data quality failed"
		if (( ! SKIP_PUBLISH )) && ! python -m src.predict.publishable_bets revoke \
			--output "$PAID_OUTPUT" --reason data_quality_failed; then
			PUBLISH_FAIL=$((PUBLISH_FAIL + 1))
			note_failure "publish" "could not revoke the previous paid output"
		fi
	fi
fi

# ----------------------------------------------------------------------------
# Step 3: current prediction refresh. These predictions are not subscriber-
# facing unless src.predict.publishable_bets gates them through an approved,
# passing rule in the final publish step.
# ----------------------------------------------------------------------------
if (( ! SKIP_PREDICTION )); then
	log "step 5: current prediction refresh"
	MODEL_TYPES=(ensemble random_forest gradient_boosting)
	for league in "${LEAGUES[@]}"; do
		for mt in "${MODEL_TYPES[@]}"; do
			log "  predicting: $league / $mt"
			bucket_run predict "predict[$league/$mt]" -- \
				python -m src.predict.runner \
					--leagues "$league" \
					--model-type "$mt" \
					--log-level INFO
		done
	done
else
	log "skipping current prediction refresh (--skip-prediction)"
fi

# ----------------------------------------------------------------------------
# Step 4: paid-picks publish gate. Empty output is a valid fail-closed state.
# ----------------------------------------------------------------------------
if (( ! SKIP_PUBLISH && RELEASE_QUALITY_PASSED )); then
	log "step 6: paid-picks publish gate"
	bucket_run publish "publishable_bets" -- \
		python -m src.predict.publishable_bets publish \
			--rules config/published_rules.yml \
			--output "$PAID_OUTPUT" \
			--quality-output reports/publishable_bets/latest_quality_report.json \
			--leagues "$PAID_LEAGUES_CSV" \
			--allow-empty
else
	log "skipping paid-picks publish gate"
fi

# Research runs after the prediction refresh and cannot delay the paid feed.
if (( ! SKIP_TRAINING && RELEASE_QUALITY_PASSED )); then
	log "step 7: rolling-origin paid-picks benchmark"
	bucket_run train "betting_benchmark" -- \
		python -m src.models.train_betting \
			--benchmark \
			--benchmark-config config/betting_benchmark.yml \
			--benchmark-output-dir reports/betting_benchmarks
else
	log "skipping paid-picks benchmark"
fi

# ----------------------------------------------------------------------------
# Summary + exit code
# ----------------------------------------------------------------------------
TOTAL_FAIL=${#FAILURES[@]}

# Critical-failure rules:
#   - predict produced zero successes in this run -> critical (no fresh
#     predictions for users).
#   - ingest had failures and zero successes -> critical (we ran on stale
#     data only and didn't even know it).
CRITICAL=0
if (( PREDICT_FAIL > 0 && PREDICT_OK == 0 )); then
	CRITICAL=1
fi
if (( INGEST_FAIL > 0 && INGEST_OK == 0 )); then
	CRITICAL=1
fi
if (( ! SKIP_TRAINING && QUALITY_FAIL > 0 )); then
	CRITICAL=1
fi
if (( ! RELEASE_QUALITY_PASSED )); then
	CRITICAL=1
fi
if (( ! SKIP_TRAINING && TRAIN_FAIL > 0 )); then
	CRITICAL=1
fi
if (( ! SKIP_PUBLISH && PUBLISH_FAIL > 0 )); then
	CRITICAL=1
fi
if (( PAPER_FAIL > 0 )); then
	CRITICAL=1
fi

# Write a JSON summary the rest of the system can read without parsing logs.
{
	printf '{\n'
	printf '  "ran_at": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '  "log_file": "%s",\n' "$LOG_FILE"
	printf '  "skip_benchmark": %s,\n' "$SKIP_TRAINING"
	printf '  "skip_prediction": %s,\n' "$SKIP_PREDICTION"
	printf '  "skip_publish": %s,\n' "$SKIP_PUBLISH"
	printf '  "skip_odds": %s,\n' "$SKIP_ODDS"
	printf '  "leagues": "%s",\n' "${LEAGUES[*]}"
	printf '  "paid_release_leagues": "%s",\n' "${PAID_LEAGUES[*]}"
	printf '  "ingest": { "ok": %s, "fail": %s },\n' "$INGEST_OK" "$INGEST_FAIL"
	printf '  "quality":{ "ok": %s, "fail": %s },\n' "$QUALITY_OK" "$QUALITY_FAIL"
	printf '  "benchmark": { "ok": %s, "fail": %s },\n' "$TRAIN_OK" "$TRAIN_FAIL"
	printf '  "predict":{ "ok": %s, "fail": %s },\n' "$PREDICT_OK" "$PREDICT_FAIL"
	printf '  "publish":{ "ok": %s, "fail": %s },\n' "$PUBLISH_OK" "$PUBLISH_FAIL"
	printf '  "paper": { "enabled": %s, "ok": %s, "fail": %s },\n' "$PAPER_ENABLED" "$PAPER_OK" "$PAPER_FAIL"
	printf '  "total_failures": %s,\n' "$TOTAL_FAIL"
	printf '  "critical": %s\n' "$([[ $CRITICAL -eq 1 ]] && printf true || printf false)"
	printf '}\n'
} > "$SUMMARY_FILE"

log "============================================="
log "pipeline complete: ingest=${INGEST_OK}/${INGEST_FAIL} quality=${QUALITY_OK}/${QUALITY_FAIL} benchmark=${TRAIN_OK}/${TRAIN_FAIL} predict=${PREDICT_OK}/${PREDICT_FAIL} publish=${PUBLISH_OK}/${PUBLISH_FAIL} paper=${PAPER_OK}/${PAPER_FAIL} (total_failures=${TOTAL_FAIL})"
if (( TOTAL_FAIL > 0 )); then
	log "failures:"
	for f in "${FAILURES[@]}"; do log "  - $f"; done
fi
log "summary written to $SUMMARY_FILE"
log "============================================="

if (( CRITICAL == 1 )); then
	log "CRITICAL: one or more required pipeline steps failed"
	exit 1
fi

exit 0
