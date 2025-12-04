#!/usr/bin/env pwsh
<#
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
                
                # Train totals model if applicable (only for GB and RF)
                if ($modelType -in @("gradient_boosting", "random_forest")) {
                    Write-Log "Training $league $modelType totals model..."
                    & poetry run python -m src.models.train_totals --league $league --model-type $modelType
                    if ($LASTEXITCODE -ne 0) {
                        Write-Log "WARNING: Totals training failed for $league ($modelType)"
                    }
                }
            }
        }
    }
    Write-Log "Model training complete"
} catch {
    Write-Log "ERROR: Model training step failed: $_"
}

Write-Log "========================================="
Write-Log "TRAINING pipeline completed"
Write-Log "========================================="
