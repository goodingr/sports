#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs all data ingestion steps: Smart History + Live Odds + Live Scores.
#>

param(
    [switch]$SoccerOnly = $false,
    [switch]$SkipOdds = $false,
    [switch]$Paper = $false
)

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$coreLeagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL")
$soccerLeagues = @("EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
$targetCoreLeagues = if ($SoccerOnly) { @() } else { $coreLeagues }
$targetSoccerLeagues = $soccerLeagues
$targetLeagues = $targetCoreLeagues + $targetSoccerLeagues
$paperFailed = $false

Write-Host "=== Starting Data Ingestion ==="

# 1. Smart History Ingestion
Write-Host "Step 1: Smart History Ingestion..."
try {
    $leagueArgs = $targetLeagues -join " "
    & poetry run python -m src.data.ingest_manager --leagues $targetLeagues 2>&1 | ForEach-Object { "$_" }
    
    if ($targetLeagues -contains "NBA") {
        Write-Host "Calculating NBA rolling metrics..."
        & poetry run python -m src.data.sources.nba_rolling_metrics --seasons 2024 2025 2>&1 | ForEach-Object { "$_" }
    }
} catch {
    Write-Host "WARNING: Smart ingestion failed: $_"
}

# 2. Live Odds
if (-not $SkipOdds) {
    Write-Host "Step 2: Fetching Live Odds..."
    $theOddsLeagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
    foreach ($league in $targetLeagues) {
        if ($theOddsLeagues -notcontains $league) { continue }
        try {
            Write-Host "Fetching odds for $league..."
            & poetry run python -m src.data.ingest_odds --league $league --market h2h,totals --force-refresh 2>&1 | ForEach-Object { "$_" }
            $oddsExitCode = $LASTEXITCODE
            if ($Paper -and $league -eq "NBA") {
                if ($oddsExitCode -eq 0) {
                    Write-Host "Capturing NBA paper decisions from the fresh odds snapshot..."
                    try {
                        & poetry run python -m src.predict.runner --leagues NBA --model-type random_forest --paper 2>&1 | ForEach-Object { "$_" }
                        if ($LASTEXITCODE -ne 0) {
                            Write-Warning "NBA paper capture failed (exit=$LASTEXITCODE)."
                            $paperFailed = $true
                        }
                    } catch {
                        Write-Warning "NBA paper capture failed."
                        $paperFailed = $true
                    }
                } else {
                    Write-Warning "NBA odds refresh failed. Paper capture could not run."
                    $paperFailed = $true
                }
                $global:LASTEXITCODE = $oddsExitCode
            }
        } catch {
            Write-Host ("WARNING: Odds fetch failed for {0}: {1}" -f $league, $_)
            if ($Paper -and $league -eq "NBA") {
                $paperFailed = $true
            }
        }
    }
} else {
    Write-Host "Skipping Live Odds (SkipOdds enabled)"
}

# 3. Live Scores
if (-not $SkipOdds) {
    Write-Host "Step 3: Fetching Live Scores..."
    try {
        $commaLeagues = $targetLeagues -join ","
        & poetry run python -m src.data.ingest_scores --leagues $commaLeagues --dotenv .env --resolve-stale --stale-lookback-days 14 2>&1 | ForEach-Object { "$_" }
        
    } catch {
        Write-Host "WARNING: Score ingestion failed: $_"
    }
} else {
    Write-Host "Skipping Live Scores (SkipOdds enabled)"
}

Write-Host "=== Data Ingestion Completed ==="
$ingestExitCode = $LASTEXITCODE
if ($ingestExitCode -ne 0) {
    Write-Warning "The final ingestion command failed (exit=$ingestExitCode)."
    exit 1
}
if ($paperFailed) { exit 2 }
exit 0
