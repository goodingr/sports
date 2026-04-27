#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs the training pipeline: Dataset Build -> Model Training.
    Logs all output to logs/train_pipeline_YYYYMMDD_HHmmss.log.
#>

param(
    [switch]$SoccerOnly = $false
)

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

# Setup Logging
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = "logs/train_pipeline_${timestamp}.log"
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

# Start Transcript to capture all output (verbose, errors, native commands)
Start-Transcript -Path $logFile -Append

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[{0}] {1}" -f $ts, $Message
    Write-Host $logMessage
}

Write-Log "========================================="
Write-Log "Starting TRAINING pipeline"
Write-Log "========================================="

$coreLeagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL")
$soccerLeagues = @("EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
$targetCoreLeagues = if ($SoccerOnly) { @() } else { $coreLeagues }
$targetSoccerLeagues = $soccerLeagues
$targetLeagues = $targetCoreLeagues + $targetSoccerLeagues

if (-not $targetLeagues.Count) {
    Write-Log "ERROR: No leagues selected for processing. Exiting."
    exit 1
}

Write-Log ("Processing leagues: " + ($targetLeagues -join ", "))

# Step 1: Build Datasets
Write-Log "Step 1: Building Datasets..."
try {
    # Define seasons to include in training
    # Using a broad range to ensure historical data is captured
    $currentYear = (Get-Date).Year
    $startYear = 2015
    $seasons = $startYear..$currentYear
    $seasonArgs = $seasons -join " "
    
    foreach ($league in $targetLeagues) {
        Write-Log "Building dataset for $league (Seasons: $startYear-$currentYear)..."
        
        & poetry run python -m src.features.moneyline_dataset --league $league --seasons $seasons 2>&1 | ForEach-Object { "$_" }
        if ($LASTEXITCODE -ne 0) { 
            Write-Log "WARNING: Dataset build failed for $league"
        }
    }
} catch {
    Write-Log "ERROR: Dataset building step failed: $_"
    exit 1
}

# Step 2: Train Models
Write-Log "Step 2: Training Models..."
try {
    $modelTypes = @("ensemble", "random_forest", "gradient_boosting")
    
    foreach ($league in $targetLeagues) {
        foreach ($modelType in $modelTypes) {
            Write-Log "Training $league $modelType model..."
            & poetry run python -m src.models.train --league $league --model-type $modelType --seasons $seasons 2>&1 | ForEach-Object { "$_" }
            if ($LASTEXITCODE -ne 0) {
                Write-Log "WARNING: Training failed for $league ($modelType)"
            }
            
            # Train totals for GB and RF
            if ($modelType -in @("gradient_boosting", "random_forest")) {
                Write-Log "Training $league $modelType totals model..."
                & poetry run python -m src.models.train_totals --league $league --model-type $modelType 2>&1 | ForEach-Object { "$_" }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Totals training failed for $league ($modelType)"
                }
            }
        }
    }
} catch {
    Write-Log "ERROR: Model training step failed: $_"
    exit 1
}

Write-Log "========================================="
Write-Log "TRAINING pipeline completed"
Write-Log "========================================="

Stop-Transcript
