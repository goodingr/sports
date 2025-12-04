# Script to update forward test results (called by Task Scheduler)
# This script is designed to run automatically and log results

$ErrorActionPreference = "Continue"

# Find project root by looking for pyproject.toml
$WorkingDir = $PSScriptRoot
$MaxDepth = 5
$Depth = 0
while ($Depth -lt $MaxDepth -and -not (Test-Path (Join-Path $WorkingDir "pyproject.toml"))) {
    $WorkingDir = Split-Path -Parent $WorkingDir
    if (-not $WorkingDir -or $WorkingDir -eq (Split-Path -Parent $WorkingDir)) {
        # Reached root, use script location
        $WorkingDir = Split-Path -Parent $PSScriptRoot
        break
    }
    $Depth++
}

# Ensure we're in the project root
Set-Location $WorkingDir
Write-Host "Working directory: $WorkingDir"
Write-Host "pyproject.toml exists: $(Test-Path 'pyproject.toml')"

$LogDir = Join-Path $WorkingDir "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$LogFile = Join-Path $LogDir "forward_test_update_$(Get-Date -Format 'yyyyMMdd').log"

function Write-Log {
    param([string]$Message)
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMessage = "[$Timestamp] $Message"
    Add-Content -Path $LogFile -Value $LogMessage
    Write-Host $LogMessage
}

Write-Log "Starting forward test update..."

# Discover supported leagues dynamically so tasks stay in sync with the codebase
try {
    $LeaguesJson = (& poetry run python -c "import json; from src.models.forward_test import SUPPORTED_LEAGUES; print(json.dumps(SUPPORTED_LEAGUES))" 2>&1)
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR: Failed to discover supported leagues. Output: $LeaguesJson"
        exit $LASTEXITCODE
    }

    if ($LeaguesJson -is [System.Array]) {
        $LeaguesJson = ($LeaguesJson -join "")
    }

    $Leagues = $LeaguesJson | ConvertFrom-Json
    if (-not $Leagues) {
        Write-Log "ERROR: No leagues discovered from forward_test module"
        exit 1
    }
} catch {
    Write-Log "ERROR: Unable to resolve supported leagues: $_"
    exit 1
}

try {
    # First, try to load recent game results if needed
    # This ensures we have the latest data
    $CurrentSeason = (Get-Date).Year
    $OverallExitCode = 0

    foreach ($League in $Leagues) {
        switch ($League) {
            "NBA" {
                Write-Log "Loading recent NBA game results..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_nba --seasons $CurrentSeason 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: NBA results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "NFL" {
                Write-Log "Loading recent NFL game results..."
                $ResultOutput = & poetry run python -m src.data.ingest_results --seasons $CurrentSeason 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: NFL results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "CFB" {
                Write-Log "Loading recent CFB game results..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_cfb $CurrentSeason 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: CFB results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "MLB" {
                Write-Log "Loading recent MLB game results..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_mlb --seasons $CurrentSeason 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: MLB results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "EPL" {
                Write-Log "Loading recent EPL game results from ESPN API..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_soccer --leagues EPL --days-ahead 7 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: EPL results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "LALIGA" {
                Write-Log "Loading recent La Liga game results from ESPN API..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_soccer --leagues LALIGA --days-ahead 7 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: La Liga results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "BUNDESLIGA" {
                Write-Log "Loading recent Bundesliga game results from ESPN API..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_soccer --leagues BUNDESLIGA --days-ahead 7 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Bundesliga results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "SERIEA" {
                Write-Log "Loading recent Serie A game results from ESPN API..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_soccer --leagues SERIEA --days-ahead 7 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Serie A results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            "LIGUE1" {
                Write-Log "Loading recent Ligue 1 game results from ESPN API..."
                $ResultOutput = & poetry run python -m src.data.ingest_results_soccer --leagues LIGUE1 --days-ahead 7 2>&1
                $ResultOutput | ForEach-Object { Write-Log $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARNING: Ligue 1 results ingestion exited with $LASTEXITCODE"
                    if ($OverallExitCode -eq 0) { $OverallExitCode = $LASTEXITCODE }
                }
            }
            default {
                Write-Log "No automated results ingestion implemented for $League; skipping"
            }
        }
    }

    # Run the update command once after ingestion attempts
    Write-Log "Updating predictions with results across all leagues..."
    $Output = & poetry run python -m src.models.forward_test update 2>&1
    $Output | ForEach-Object { Write-Log $_ }

    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR: Forward test update failed with exit code $LASTEXITCODE"
        $OverallExitCode = $LASTEXITCODE
    } else {
        Write-Log "Forward test update completed successfully"
    }

    if ($OverallExitCode -eq 0) {
        exit 0
    } else {
        Write-Log "Completed with warnings/errors (exit code $OverallExitCode)"
        exit $OverallExitCode
    }
} catch {
    Write-Log "ERROR: Exception occurred: $_"
    exit 1
}

