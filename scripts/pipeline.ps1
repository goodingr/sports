#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Master pipeline script. Orchestrates training and prediction workflows.
    
    Default behavior: Runs prediction pipeline only (fast, hourly).
    With -Train: Runs training pipeline first, then prediction pipeline (slow, daily).
#>

param(
    [switch]$SkipTraining = $false,
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

# 2. Train (Default, unless skipped)
if (-not $SkipTraining) {
    Write-Host "=== Starting Training Pipeline ==="
    & "$scriptDir/train.ps1" -SoccerOnly:$SoccerOnly
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Training pipeline failed. Aborting."
        exit $LASTEXITCODE
    }
} else {
    Write-Host "=== Skipping Training Pipeline ==="
}

# 3. Predict
Write-Host "=== Starting Prediction Pipeline ==="
# Skip history and odds since we just did them
& "$scriptDir/predict.ps1" -SoccerOnly:$SoccerOnly
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Prediction pipeline failed."
    exit $LASTEXITCODE
}

Write-Host "=== Master Pipeline Completed ==="

Stop-Transcript
