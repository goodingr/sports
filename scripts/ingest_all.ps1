#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs all data ingestion steps: Smart History + Live Odds + Live Scores.
#>

param(
    [switch]$SoccerOnly = $false,
    [switch]$SkipOdds = $false
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

Write-Host "=== Starting Data Ingestion ==="

# 1. Smart History Ingestion
Write-Host "Step 1: Smart History Ingestion..."
try {
    $leagueArgs = $targetLeagues -join " "
    & poetry run python -m src.data.ingest_manager --leagues $targetLeagues 2>&1 | ForEach-Object { "$_" }
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
        } catch {
            Write-Host ("WARNING: Odds fetch failed for {0}: {1}" -f $league, $_)
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
        & poetry run python -m src.data.ingest_scores --leagues $commaLeagues --dotenv .env 2>&1 | ForEach-Object { "$_" }
    } catch {
        Write-Host "WARNING: Score ingestion failed: $_"
    }
} else {
    Write-Host "Skipping Live Scores (SkipOdds enabled)"
}

Write-Host "=== Data Ingestion Completed ==="
