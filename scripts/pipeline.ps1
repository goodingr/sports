#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Master pipeline script. Orchestrates training and prediction workflows.
    
    Default behavior: Runs prediction pipeline only (fast, hourly).
    With -Train: Runs training pipeline first, then prediction pipeline (slow, daily).
#>

param(
    [switch]$Train = $false,
    [switch]$SoccerOnly = $false,
    [switch]$SkipOdds = $false
)

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot



Write-Host "=== Starting Data Backup ==="
python "$scriptDir/backup_data.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Data backup failed. Proceeding with caution..."
} else {
    Write-Host "Data backup completed."
}

if ($Train) {
    Write-Host "=== Starting Training Pipeline ==="
    & "$scriptDir/train.ps1" -SoccerOnly:$SoccerOnly
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Training pipeline failed. Aborting."
        exit $LASTEXITCODE
    }
}

Write-Host "=== Starting Prediction Pipeline ==="
& "$scriptDir/predict.ps1" -SoccerOnly:$SoccerOnly -SkipOdds:$SkipOdds
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Prediction pipeline failed."
    exit $LASTEXITCODE
}

Write-Host "=== Master Pipeline Completed ==="
