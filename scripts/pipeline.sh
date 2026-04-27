#!/usr/bin/env bash
#
# Linux/container equivalent of scripts/pipeline.ps1.
# Orchestrates ingest -> (train) -> predict.
#
# Defaults: full pipeline (with training).
# Flags:
#   --skip-training    skip the training step (use for hourly cadence)
#   --soccer-only      only process soccer leagues
#   --skip-odds        skip odds + scores fetches (use cached data)
#
# Each step's stdout/stderr is appended to logs/pipeline_<ts>.log.

set -uo pipefail

cd "$(dirname "$0")/.."

SKIP_TRAINING=0
SOCCER_ONLY=0
SKIP_ODDS=0

for arg in "$@"; do
	case "$arg" in
		--skip-training) SKIP_TRAINING=1 ;;
		--soccer-only)   SOCCER_ONLY=1 ;;
		--skip-odds)     SKIP_ODDS=1 ;;
		*) echo "unknown arg: $arg" >&2; exit 64 ;;
	esac
done

mkdir -p logs
TS=$(date -u +%Y%m%d_%H%M%S)
LOG_FILE="logs/pipeline_${TS}.log"
exec > >(tee -a "$LOG_FILE") 2>&1

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

CORE_LEAGUES=(NFL NBA CFB NCAAB NHL)
SOCCER_LEAGUES=(EPL LALIGA BUNDESLIGA SERIEA LIGUE1)

if (( SOCCER_ONLY )); then
	LEAGUES=("${SOCCER_LEAGUES[@]}")
else
	LEAGUES=("${CORE_LEAGUES[@]}" "${SOCCER_LEAGUES[@]}")
fi

log "============================================="
log "pipeline start (skip_training=$SKIP_TRAINING soccer_only=$SOCCER_ONLY skip_odds=$SKIP_ODDS)"
log "leagues: ${LEAGUES[*]}"
log "============================================="

# ----------------------------------------------------------------------------
# Pre-flight backup. Cheap insurance — if anything below corrupts the DB,
# we have a snapshot from seconds ago.
# ----------------------------------------------------------------------------
log "step: pre-flight backup"
python scripts/backup_data.py || log "WARN: pre-flight backup failed, continuing"

# ----------------------------------------------------------------------------
# Step 1: ingest
# ----------------------------------------------------------------------------
log "step 1: smart history ingest"
python -m src.data.ingest_manager --leagues "${LEAGUES[@]}" \
	|| log "WARN: ingest_manager failed (continuing)"

if printf '%s\n' "${LEAGUES[@]}" | grep -qx NBA; then
	log "step 1a: nba rolling metrics"
	python -m src.data.sources.nba_rolling_metrics --seasons 2024 2025 \
		|| log "WARN: nba_rolling_metrics failed (continuing)"
fi

if (( ! SKIP_ODDS )); then
	log "step 2: live odds"
	for league in "${LEAGUES[@]}"; do
		log "  fetching odds: $league"
		python -m src.data.ingest_odds \
			--league "$league" \
			--market h2h,totals \
			--force-refresh \
			|| log "  WARN: odds fetch failed for $league"
	done

	log "step 3: live scores"
	# Bash's IFS+expansion form: assignment is local to this command line.
	CSV_LEAGUES=$(IFS=,; printf '%s' "${LEAGUES[*]}")
	python -m src.data.ingest_scores --leagues "$CSV_LEAGUES" \
		|| log "WARN: ingest_scores failed"
else
	log "skipping odds + scores (--skip-odds)"
fi

# ----------------------------------------------------------------------------
# Step 2: train
# ----------------------------------------------------------------------------
if (( ! SKIP_TRAINING )); then
	log "step 4: training"
	START_YEAR=2015
	END_YEAR=$(date -u +%Y)
	SEASONS=()
	for ((y=START_YEAR; y<=END_YEAR; y++)); do SEASONS+=("$y"); done

	MODEL_TYPES=(ensemble random_forest gradient_boosting)

	for league in "${LEAGUES[@]}"; do
		log "  building dataset: $league ($START_YEAR-$END_YEAR)"
		python -m src.features.moneyline_dataset \
			--league "$league" --seasons "${SEASONS[@]}" \
			|| log "  WARN: dataset build failed for $league"

		for mt in "${MODEL_TYPES[@]}"; do
			log "  training: $league / $mt"
			python -m src.models.train --league "$league" --model-type "$mt" \
				|| log "  WARN: train failed: $league / $mt"

			if [[ "$mt" == "gradient_boosting" || "$mt" == "random_forest" ]]; then
				log "  training totals: $league / $mt"
				python -m src.models.train_totals --league "$league" --model-type "$mt" \
					|| log "  WARN: train_totals failed: $league / $mt"
			fi
		done
	done
else
	log "skipping training (--skip-training)"
fi

# ----------------------------------------------------------------------------
# Step 3: predict
# ----------------------------------------------------------------------------
log "step 5: predict"
MODEL_TYPES=(ensemble random_forest gradient_boosting)
for league in "${LEAGUES[@]}"; do
	for mt in "${MODEL_TYPES[@]}"; do
		log "  predicting: $league / $mt"
		python -m src.predict.runner \
			--leagues "$league" \
			--model-type "$mt" \
			--log-level INFO \
			|| log "  WARN: predict failed: $league / $mt"
	done
done

log "============================================="
log "pipeline complete"
log "============================================="
