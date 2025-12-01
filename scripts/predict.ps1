#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs the prediction pipeline: Odds Snapshot -> TeamRankings -> Predictions -> Result Sync.
    This script is intended to be run frequently (e.g., hourly).
#>

param(
    [switch]$SoccerOnly = $false,
    [switch]$SkipOdds = $false
)

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$logFile = "logs/predict_pipeline_{0}.log" -f (Get-Date -Format 'yyyyMMdd_HHmmss')
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

$coreLeagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL")
$soccerLeagues = @("EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
$allLeagues = $coreLeagues + $soccerLeagues
$targetCoreLeagues = if ($SoccerOnly) { @() } else { $coreLeagues }
$targetSoccerLeagues = $soccerLeagues
$targetLeagues = $targetCoreLeagues + $targetSoccerLeagues

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[{0}] {1}" -f $timestamp, $Message
    Write-Host $logMessage
    Add-Content -Path $logFile -Value $logMessage
}



Write-Log "========================================="
Write-Log "Starting PREDICTION pipeline"
Write-Log "========================================="

if (-not $targetLeagues.Count) {
    Write-Log "ERROR: No leagues selected for processing. Exiting."
    exit 1
}

if ($SoccerOnly) {
    Write-Log ("Soccer-only mode enabled. Processing leagues: " + ($targetLeagues -join ", "))
} else {
    Write-Log ("Processing leagues: " + ($targetLeagues -join ", "))
}

# Step 0: Backups
Write-Log "Step 0: Creating backups..."
try {
    Write-Log "Backing up database..."
    & poetry run python scripts/backup_db.py
    
    Write-Log "Backing up prediction files..."
    & poetry run python scripts/backup_predictions.py
} catch {
    Write-Log "WARNING: Backup step failed: $_"
    # Continue anyway as this is not critical for prediction generation
}

# Step 1: Fetch Odds API snapshots
Write-Log "Step 1: Fetching The Odds API snapshots..."
$theOddsLeagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
foreach ($league in $targetLeagues) {
    if ($theOddsLeagues -notcontains $league) {
        continue
    }
    if (-not $SkipOdds) {
        try {
            Write-Log "Requesting Odds API snapshot for $league (moneyline + totals)..."
            & poetry run python -m src.data.ingest_odds --league $league --market h2h,totals --force-refresh | Out-Null
        } catch {
            Write-Log ("WARNING: Odds API snapshot failed for {0}: {1}" -f $league, $_)
        }
    } else {
        Write-Log "Skipping Odds API snapshot for $league (SkipOdds enabled)"
    }
}



# Step 3: Generate predictions
Write-Log "Step 3: Generating predictions..."
try {
    if (-not $targetLeagues -or -not $targetLeagues.Count) {
        Write-Log "WARNING: No leagues configured for prediction step"
    } else {
        $modelTypes = @("ensemble", "random_forest", "gradient_boosting")
        foreach ($league in $targetLeagues) {
            foreach ($modelType in $modelTypes) {
                Write-Log "Forward testing $league with $modelType model..."
                # Always use --use-db-odds because we just fetched the odds in Step 1 (or skipped if SkipOdds is set)
                & poetry run python -m src.models.forward_test predict --league $league --model-type $modelType --dotenv .env --log-level INFO --use-db-odds
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Forward test failed for $league ($modelType)"
                    continue
                }
                Write-Log "Updating completed results for $league ($modelType)..."
                & poetry run python -m src.models.forward_test update --league $league --model-type $modelType --dotenv .env --log-level INFO
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Result update failed for $league ($modelType)"
                }
            }
        }
    }
    Write-Log "Prediction generation complete"
} catch {
    Write-Log "ERROR: Prediction generation step failed: $_"
}

# Step 4: Sync results
Write-Log "Step 4: Syncing results across model types..."
try {
    & poetry run python scripts/copy_results.py
    if ($LASTEXITCODE -eq 0) {
        Write-Log "Results synced successfully across all model types"
    } else {
        Write-Log "WARNING: Result sync failed"
    }
} catch {
    Write-Log "ERROR: Result sync step failed: $_"
}

Write-Log "========================================="
Write-Log "PREDICTION pipeline completed"
Write-Log "========================================="
