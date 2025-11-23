#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs the full ingestion → dataset → training → prediction pipeline for every supported league.
#>

param(
    [switch]$SoccerOnly = $false,
    [switch]$SkipOdds = $false
)

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$logFile = "logs/hourly_pipeline_{0}.log" -f (Get-Date -Format 'yyyyMMdd_HHmmss')
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

$coreLeagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL")
$soccerLeagues = @("EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
$allLeagues = $coreLeagues + $soccerLeagues
$targetCoreLeagues = if ($SoccerOnly) { @() } else { $coreLeagues }
$targetSoccerLeagues = $soccerLeagues
$targetLeagues = $targetCoreLeagues + $targetSoccerLeagues
$targetLeaguesJson = ConvertTo-Json $targetLeagues -Compress
$targetCoreLeaguesJson = ConvertTo-Json $targetCoreLeagues -Compress
$targetSoccerLeaguesJson = ConvertTo-Json $targetSoccerLeagues -Compress
$datasetReady = @()
$trainedLeagues = @()

function New-DefaultSeasonRange {
    param(
        [int]$LastCompleted,
        [int]$Years = 5
    )
    $seasons = @()
    for ($year = $LastCompleted - ($Years - 1); $year -le $LastCompleted; $year++) {
        $seasons += $year
    }
    return $seasons
}

function Get-LeagueSeasons {
    param(
        [string]$League,
        [int[]]$DefaultSeasons,
        [int[]]$SoccerSeasons
    )
    switch ($League.ToUpper()) {
        "EPL" { return $SoccerSeasons }
        "LALIGA" { return $SoccerSeasons }
        "BUNDESLIGA" { return $SoccerSeasons }
        "SERIEA" { return $SoccerSeasons }
        "LIGUE1" { return $SoccerSeasons }
        default { return $DefaultSeasons }
    }
}

$currentYear = (Get-Date).Year
$activeSeasonYear = $currentYear
if ((Get-Date).Month -lt 3) {
    # Treat January/February as part of previous season for leagues that span calendar years
    $activeSeasonYear -= 1
}
$defaultSeasons = New-DefaultSeasonRange -LastCompleted $activeSeasonYear -Years 5
$soccerPrimarySeasons = New-DefaultSeasonRange -LastCompleted $activeSeasonYear -Years 4
$soccerSupplementSeasons = New-DefaultSeasonRange -LastCompleted ($activeSeasonYear - 4) -Years 4
$soccerSeasonYears = @($soccerSupplementSeasons + $soccerPrimarySeasons) | Sort-Object -Unique
$soccerDaysBack = 7
$soccerDaysForward = 2

$soccerSeasonMap = @{}
$understatLeagueMap = @{
    "EPL"       = "EPL"
    "LALIGA"    = "La_liga"
    "BUNDESLIGA"= "Bundesliga"
    "SERIEA"    = "Serie_A"
    "LIGUE1"    = "Ligue_1"
}
$footballDataLeagueMap = @{
    "EPL"       = "premier-league"
    "LALIGA"    = "la-liga"
    "BUNDESLIGA"= "bundesliga"
    "SERIEA"    = "serie-a"
    "LIGUE1"    = "ligue-1"
}
$soccerRecentSeasonStart = 2021
$soccerRecentSeasonEnd = [Math]::Min($activeSeasonYear + 1, 2025)
$soccerRecentSeasons = @()
for ($season = $soccerRecentSeasonStart; $season -le $soccerRecentSeasonEnd; $season++) {
    $soccerRecentSeasons += $season
}
$soccerMatchPayloadSeasons = @(
    $soccerSeasonYears |
        Sort-Object -Descending |
        Select-Object -First 2 |
        Sort-Object
)
if (-not $soccerMatchPayloadSeasons -or $soccerMatchPayloadSeasons.Count -eq 0) {
    $soccerMatchPayloadSeasons = @(
        $soccerRecentSeasons |
            Sort-Object -Descending |
            Select-Object -First 2 |
            Sort-Object
    )
}

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[{0}] {1}" -f $timestamp, $Message
    Write-Host $logMessage
    Add-Content -Path $logFile -Value $logMessage
}

Write-Log "========================================="
Write-Log "Starting hourly pipeline"
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

if ($targetSoccerLeagues.Count) {
    Write-Log "Detecting available soccer seasons from data/raw/results..."
    $pythonSeasonScript = @"
import json
from pathlib import Path
import pandas as pd

base = Path('data/raw/results')
result = {}
for league in ['EPL', 'LALIGA', 'BUNDESLIGA', 'SERIEA', 'LIGUE1']:
    years = set()
    for file in base.glob(f'schedules_{league.lower()}*.parquet'):
        try:
            df = pd.read_parquet(file, columns=['gameday'])
        except Exception:
            continue
        years |= {
            ts.year
            for ts in pd.to_datetime(df['gameday'], errors='coerce')
            if not pd.isna(ts)
        }
    if years:
        result[league] = sorted(years)

print(json.dumps(result))
"@
    $detectedSeasonsJson = $pythonSeasonScript | poetry run python -
    if ($LASTEXITCODE -eq 0 -and $detectedSeasonsJson) {
        $parsed = $detectedSeasonsJson | ConvertFrom-Json
        foreach ($prop in $parsed.PSObject.Properties) {
            $soccerSeasonMap[$prop.Name] = @($prop.Value)
        }
        $seasonSummary = @()
        foreach ($key in $soccerSeasonMap.Keys) {
            $values = @($soccerSeasonMap[$key])
            if (-not $values -or -not $values.Count) {
                continue
            }
            $tailCount = [Math]::Min(5, $values.Count)
            $tailValues = ($values | Select-Object -Last $tailCount) -join ","
            $seasonSummary += "${key}:`t$tailValues"
        }
        if ($seasonSummary.Count) {
            Write-Log ("Detected soccer seasons: " + ($seasonSummary -join " | "))
        } else {
            Write-Log "WARNING: Soccer season detection returned an empty list"
        }
    } else {
        Write-Log "WARNING: Failed to detect soccer seasons dynamically; falling back to defaults"
    }
} else {
    Write-Log "Skipping soccer season detection (no soccer leagues selected)..."
}

# Step 1: ingest odds, injuries, and latest results
Write-Log "Step 1: Ingesting odds, injuries, and recent results..."
try {
    & poetry run python -c "
import datetime
import json
import logging

from src.data.sources import espn_odds, nba_injuries_espn
from src.data.ingest_results import run as ingest_nfl_results
from src.data.ingest_results_nba import run as ingest_nba_results
from src.data.ingest_results_cfb import run as ingest_cfb_results
from src.data.ingest_results_soccer import run as ingest_soccer_results

logging.basicConfig(level=logging.INFO)

target_leagues = json.loads('''$targetLeaguesJson''')
core_leagues = json.loads('''$targetCoreLeaguesJson''')
soccer_leagues = json.loads('''$targetSoccerLeaguesJson''')

odds_funcs = {
    'NFL': espn_odds.ingest_nfl,
    'NBA': espn_odds.ingest_nba,
    'CFB': espn_odds.ingest_cfb,
    'EPL': espn_odds.ingest_epl,
    'LALIGA': espn_odds.ingest_laliga,
    'BUNDESLIGA': espn_odds.ingest_bundesliga,
    'SERIEA': espn_odds.ingest_seriea,
    'LIGUE1': espn_odds.ingest_ligue1,
}

for label, func in odds_funcs.items():
    if label not in target_leagues:
        continue
    try:
        print(f'Ingesting ESPN odds for {label}...')
        func()
    except Exception as exc:  # noqa: BLE001
        print(f'ESPN odds ingestion failed for {label}: {exc}')

if 'NBA' in core_leagues:
    try:
        print('Ingesting NBA injuries via ESPN...')
        nba_injuries_espn.ingest()
    except Exception as exc:  # noqa: BLE001
        print(f'NBA injuries ingestion failed: {exc}')

now = datetime.datetime.utcnow()
recent_days = 14
current_year = now.year
if now.month < 3:  # allow seasons that span calendar years (NBA/CFB/NFL)
    current_year -= 1
seasons = list(range(current_year - 1, current_year + 1))
soccer_days_back = $soccerDaysBack
soccer_days_forward = $soccerDaysForward

def _safe_run(label, func, *args, **kwargs):
    try:
        print(f'Running {label}...')
        func(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        print(f'{label} failed: {exc}')

if 'NFL' in core_leagues:
    _safe_run('NFL results', ingest_nfl_results, seasons)
if 'NBA' in core_leagues:
    _safe_run('NBA results', ingest_nba_results, seasons, days_back=recent_days)
if 'CFB' in core_leagues:
    _safe_run('CFB results', ingest_cfb_results, seasons)
if soccer_leagues:
    _safe_run(
        'Soccer results',
        ingest_soccer_results,
        leagues=soccer_leagues,
        use_espn=True,
        use_database=False,
        days_back=soccer_days_back,
        days_forward=soccer_days_forward,
        seasons=None,
    )
"
    Write-Log "Ingestion complete"
} catch {
    Write-Log "ERROR: Ingestion step failed: $_"
}

Write-Log "Step 1b: Fetching The Odds API snapshots..."
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

if ($targetSoccerLeagues.Count) {
    $understatTargets = @($targetSoccerLeagues | Where-Object { $understatLeagueMap.ContainsKey($_) } | ForEach-Object { $understatLeagueMap[$_] })
    if ($understatTargets.Count) {
        $leagueArg = ($understatTargets -join ',')
        $seasonArg = ($soccerRecentSeasons -join ',')
        Write-Log "Syncing Understat archives for leagues: $leagueArg (seasons: $seasonArg)..."
        try {
            & poetry run python -m src.data.ingest_understat --leagues $leagueArg --seasons $seasonArg | Out-Null
        } catch {
            Write-Log "WARNING: Understat ingestion failed: $_"
        }
        $matchSeasonArg = ($soccerMatchPayloadSeasons -join ',')
        Write-Log "Fetching Understat match payloads for leagues: $leagueArg (seasons: $matchSeasonArg)..."
        try {
            & poetry run python -m src.data.sources.understat_match_payloads --leagues $leagueArg --seasons $matchSeasonArg | Out-Null
        } catch {
            Write-Log "WARNING: Understat match payload ingestion failed: $_"
        }
    }

    $footballTargets = @($targetSoccerLeagues | Where-Object { $footballDataLeagueMap.ContainsKey($_) } | ForEach-Object { $footballDataLeagueMap[$_] })
    if ($footballTargets.Count) {
        $leagueArg = ($footballTargets -join ',')
        Write-Log "Syncing football-data odds archives for leagues: $leagueArg..."
        try {
            & poetry run python -m src.data.ingest_football_data --leagues $leagueArg | Out-Null
        } catch {
            Write-Log "WARNING: Football-data ingestion failed: $_"
        }

        Write-Log "Updating soccer totals from football-data for leagues: $leagueArg..."
        try {
            & poetry run python scripts/run_external_loader.py --source football-data --leagues $footballTargets | Out-Null
        } catch {
            Write-Log "WARNING: football-data loader failed: $_"
        }
    }
}

$teamRankingsEligible = @($targetLeagues | Where-Object { $_ -in @("NFL", "NBA", "CFB") })
if ($teamRankingsEligible.Count) {
    $leagueArg = ($teamRankingsEligible -join ',')
    Write-Log "Fetching TeamRankings over/under picks for leagues: $leagueArg..."
    foreach ($league in $teamRankingsEligible) {
        try {
            & poetry run python -m src.data.sources.teamrankings_over_under --league $league | Out-Null
        } catch {
            Write-Log ("WARNING: TeamRankings ingestion failed for {0}: {1}" -f $league, $_)
        }
    }
}

$teamRankingsEligible = @($targetLeagues | Where-Object { $_ -in @("NFL", "NBA", "CFB") })
if ($teamRankingsEligible.Count) {
    $leagueArg = ($teamRankingsEligible -join ',')
    Write-Log "Updating TeamRankings picks for leagues: $leagueArg..."
    try {
        & poetry run python scripts/run_external_loader.py --source teamrankings --leagues $teamRankingsEligible | Out-Null
    } catch {
        Write-Log "WARNING: TeamRankings loader failed: $_"
    }
}

# Step 2: advanced stats / rolling metrics
Write-Log "Step 2: Refreshing advanced stats..."
try {
    $cfbSeasonList = ($defaultSeasons -join ', ')
    $soccerSeasonList = ($soccerSeasonYears -join ', ')
    & poetry run python -c "
import datetime
import json

from src.data.sources import nba_rolling_metrics, cfbd_advanced_stats, soccer_advanced_stats

target_leagues = json.loads('''$targetLeaguesJson''')
core_leagues = json.loads('''$targetCoreLeaguesJson''')
soccer_leagues = json.loads('''$targetSoccerLeaguesJson''')

current_year = datetime.datetime.now().year
last_completed = current_year - 1
cfb_seasons = [$cfbSeasonList]
soccer_historical_seasons = [$soccerSeasonList]

def _safe(label, func, *args, **kwargs):
    try:
        print(f'Running {label}...')
        func(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        print(f'{label} failed: {exc}')

if 'NBA' in core_leagues:
    _safe('NBA rolling metrics', nba_rolling_metrics.ingest)
if 'CFB' in core_leagues:
    _safe('CFB advanced stats', cfbd_advanced_stats.ingest, seasons=cfb_seasons)
if soccer_leagues:
    _safe('Soccer advanced stats', soccer_advanced_stats.ingest, seasons=soccer_historical_seasons, leagues=soccer_leagues)
"
    Write-Log "Advanced stats ingestion complete"
} catch {
    Write-Log "ERROR: Advanced stats step failed: $_"
}

# Step 3: rebuild datasets
Write-Log "Step 3: Rebuilding datasets..."
try {
    $datasetReady = @()
    $leagueSeasonPlan = @{}
    foreach ($league in $targetLeagues) {
        $leagueSeasons = Get-LeagueSeasons -League $league -DefaultSeasons $defaultSeasons -SoccerSeasons $soccerSeasonYears
        $supplementSeasons = @()
        if ($league -in $soccerLeagues -and $soccerSeasonMap.ContainsKey($league.ToUpper())) {
            $availableSeasons = $soccerSeasonMap[$league.ToUpper()] | Sort-Object
            $primaryCutoff = $activeSeasonYear - 3
            $primarySeasons = @($availableSeasons | Where-Object { $_ -ge $primaryCutoff })
            if (-not $primarySeasons -or $primarySeasons.Count -lt 4) {
                $primarySeasons = @($availableSeasons | Select-Object -Last ([Math]::Min(4, $availableSeasons.Count)))
            }
            if ($primarySeasons.Count) {
                $leagueSeasons = $primarySeasons
            }
            $supplementSeasons = @()
            $primaryEarliest = $null
            if ($leagueSeasons -and $leagueSeasons.Count) {
                $primaryEarliest = ($leagueSeasons | Sort-Object | Select-Object -First 1)
            } elseif ($availableSeasons.Count) {
                $primaryEarliest = ($availableSeasons | Sort-Object | Select-Object -First 1)
            }
            if ($primaryEarliest -ne $null) {
                $historicalSeasons = @($availableSeasons | Where-Object { $_ -lt $primaryEarliest })
                if ($historicalSeasons.Count) {
                    $supplementSeasons = @($historicalSeasons | Select-Object -Last ([Math]::Min(4, $historicalSeasons.Count)))
                }
            }
        } else {
            $supplementSeasons = @()
        }
        if (-not $leagueSeasons -or -not $leagueSeasons.Count) {
            if ($supplementSeasons -and $supplementSeasons.Count) {
                $leagueSeasons = $supplementSeasons
                $supplementSeasons = @()
            } else {
                Write-Log "WARNING: No seasons configured for $league; skipping dataset build"
                continue
            }
        }
        $seasonArgs = @($leagueSeasons | Sort-Object -Unique)
        if ($league -in $soccerLeagues -and $supplementSeasons -and $supplementSeasons.Count) {
            $seasonArgs = @($seasonArgs + $supplementSeasons | Sort-Object -Unique)
            Write-Log "INFO: Augmenting $league dataset with historical seasons: $($seasonArgs -join ', ')"
        }
        Write-Log "Building dataset for $league (seasons: $($seasonArgs -join ', '))..."
        & poetry run python -m src.features.moneyline_dataset --league $league --seasons $seasonArgs
        $datasetPath = "data/processed/model_input/moneyline_$($league.ToLower())_$($seasonArgs[0])_$($seasonArgs[-1]).parquet"
        if ($LASTEXITCODE -eq 0 -and (Test-Path $datasetPath)) {
            $datasetReady += $league
            $leagueSeasonPlan[$league] = $seasonArgs
        } else {
            Write-Log "WARNING: Dataset build failed for $league"
        }
    }
    if (-not $datasetReady.Count) {
        Write-Log "WARNING: No datasets built successfully; downstream steps will be skipped"
    }
    Write-Log "Dataset rebuild complete"
} catch {
    Write-Log "ERROR: Dataset rebuild step failed: $_"
}

# Step 4: train models
Write-Log "Step 4: Training models..."
try {
    $trainedLeagues = @()
    if (-not $datasetReady -or -not $datasetReady.Count) {
        Write-Log "WARNING: Skipping training step because no datasets are ready"
    } else {
        $modelTypes = @("ensemble", "random_forest", "gradient_boosting")
        foreach ($league in $datasetReady) {
            if ($leagueSeasonPlan.ContainsKey($league)) {
                $leagueSeasons = $leagueSeasonPlan[$league]
            } else {
                $leagueSeasons = Get-LeagueSeasons -League $league -DefaultSeasons $defaultSeasons -SoccerSeasons $soccerSeasonYears
            }
            foreach ($modelType in $modelTypes) {
                Write-Log "Training $league $modelType model..."
                & poetry run python -m src.models.train --league $league --seasons $leagueSeasons --model-type $modelType
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Training failed for $league ($modelType)"
                } else {
                    if ($trainedLeagues -notcontains $league) {
                        $trainedLeagues += $league
                    }
                }
            }
        }
    }
    Write-Log "Model training complete"
} catch {
    Write-Log "ERROR: Model training step failed: $_"
}

# Step 5: generate predictions
Write-Log "Step 5: Generating predictions..."
try {
    if (-not $targetLeagues -or -not $targetLeagues.Count) {
        Write-Log "WARNING: No leagues configured for prediction step"
    } else {
        $modelTypes = @("ensemble", "random_forest", "gradient_boosting")
        foreach ($league in $targetLeagues) {
            foreach ($modelType in $modelTypes) {
                Write-Log "Forward testing $league with $modelType model..."
                & poetry run python -m src.models.forward_test predict --league $league --model-type $modelType --dotenv .env --log-level INFO
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

Write-Log "========================================="
Write-Log "Hourly pipeline completed"
Write-Log "========================================="
