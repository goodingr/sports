#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs the prediction pipeline: Predictions -> Result Sync.
    This script is intended to be run frequently (e.g., hourly).
#>

param(
    [string]$League = "",
    [switch]$SoccerOnly = $false
)

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$logFile = "logs/predict_pipeline_{0}.log" -f (Get-Date -Format 'yyyyMMdd_HHmmss')
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

$coreLeagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL")
$soccerLeagues = @("EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")

if ($League) {
    $targetLeagues = @($League.ToUpper())
} elseif ($SoccerOnly) {
    $targetLeagues = $soccerLeagues
} else {
    $targetLeagues = $coreLeagues + $soccerLeagues
}

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

# Step 1: Generate predictions
Write-Log "Step 1: Generating predictions..."
try {
    if (-not $targetLeagues -or -not $targetLeagues.Count) {
        Write-Log "WARNING: No leagues configured for prediction step"
    } else {
        $modelTypes = @("ensemble", "random_forest", "gradient_boosting")
        foreach ($league in $targetLeagues) {
            foreach ($modelType in $modelTypes) {
                Write-Log "Generating predictions for $league with $modelType model..."
                # Use new runner
                & poetry run python -m src.predict.runner --leagues $league --model-type $modelType --log-level INFO 2>&1 | ForEach-Object { "$_" }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Prediction failed for $league ($modelType)"
                    continue
                }
            }
        }
    }
    Write-Log "Prediction generation complete"
} catch {
    Write-Log "ERROR: Prediction generation step failed: $_"
}

# Step 2: Sync results (REMOVED - Handled by Ingestion)
Write-Log "Step 2: Result syncing is now handled by ingestion pipeline."

Write-Log "========================================="
Write-Log "PREDICTION pipeline completed"
Write-Log "========================================="
