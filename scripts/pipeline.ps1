#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Master pipeline script. Orchestrates ingest, data hygiene, quality
    benchmarking, current prediction refresh, and paid-picks publishing.
#>

param(
    [switch]$SkipTraining = $false,
    [switch]$SkipBenchmark = $false,
    [switch]$SkipPrediction = $false,
    [switch]$SkipPublish = $false,
    [switch]$SoccerOnly = $false,
    [switch]$SkipOdds = $false
)

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = "logs/pipeline_${timestamp}.log"
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

# Start Transcript to capture all output
Start-Transcript -Path $logFile -Append

$paidReleaseLeagues = if ($env:PAID_RELEASE_LEAGUES) {
    $env:PAID_RELEASE_LEAGUES
} elseif ($SoccerOnly) {
    "EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1"
} else {
    "NBA,NHL,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1"
}



Write-Host "=== Starting Data Backup ==="
python "$scriptDir/backup_data.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Data backup failed. Proceeding with caution..."
} else {
    Write-Host "Data backup completed."
}

# 1. Ingest All Data
Write-Host "=== Starting Data Ingestion ==="
& "$scriptDir/ingest_all.ps1" -SoccerOnly:$SoccerOnly -SkipOdds:$SkipOdds
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Data ingestion failed."
    exit $LASTEXITCODE
}

# 1b. Paid-release data hygiene
Write-Host "=== Starting Paid-Release Data Hygiene ==="
python -m src.data.quality --prune-orphans --warn-only --leagues $paidReleaseLeagues
python -m src.data.score_backfill --resolve-stale --lookback-days 14 --leagues $paidReleaseLeagues
python -m src.data.quality --finalize-scored --close-unresolved-stale --warn-only --leagues $paidReleaseLeagues
python -m src.data.quality --warn-only --leagues $paidReleaseLeagues

# 2. Quality-first benchmark (Default, unless skipped)
if (-not $SkipTraining -and -not $SkipBenchmark) {
    Write-Host "=== Starting Paid-Picks Data Quality Gate ==="
    python -m src.data.quality --leagues $paidReleaseLeagues
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Paid-release data quality failed. Skipping benchmark."
        exit $LASTEXITCODE
    }

    Write-Host "=== Starting Rolling-Origin Betting Benchmark ==="
    python -m src.models.train_betting `
        --benchmark `
        --benchmark-config config/betting_benchmark.yml `
        --benchmark-output-dir reports/betting_benchmarks
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Betting benchmark failed. Aborting."
        exit $LASTEXITCODE
    }
} else {
    Write-Host "=== Skipping Paid-Picks Benchmark ==="
}

# 3. Predict. These current predictions are not subscriber-facing unless the
# publish gate matches them to an approved, passing rule.
if (-not $SkipPrediction) {
    Write-Host "=== Starting Current Prediction Refresh ==="
    & "$scriptDir/predict.ps1" -SoccerOnly:$SoccerOnly
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Prediction pipeline failed."
        exit $LASTEXITCODE
    }
} else {
    Write-Host "=== Skipping Current Prediction Refresh ==="
}

# 4. Paid-picks publish gate. --allow-empty keeps fail-closed no-pick days from
# failing automation.
if (-not $SkipPublish) {
    Write-Host "=== Starting Paid-Picks Publish Gate ==="
    python -m src.predict.publishable_bets publish `
        --rules config/published_rules.yml `
        --output reports/publishable_bets/latest_publishable_bets.json `
        --quality-output reports/publishable_bets/latest_quality_report.json `
        --leagues $paidReleaseLeagues `
        --allow-empty
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Paid-picks publish gate failed."
        exit $LASTEXITCODE
    }
} else {
    Write-Host "=== Skipping Paid-Picks Publish Gate ==="
}

Write-Host "=== Master Pipeline Completed ==="

Stop-Transcript
