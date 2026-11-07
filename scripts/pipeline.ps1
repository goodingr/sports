#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Master pipeline script. Orchestrates ingest, data hygiene, quality
    benchmarking, current prediction refresh, paper tracking, and paid-picks publishing.
.PARAMETER ResearchProviderLeagues
    Restrict live Odds API odds and scores to NBA and NCAAB.
#>

param(
    [switch]$SkipTraining = $false,
    [switch]$SkipBenchmark = $false,
    [switch]$SkipPrediction = $false,
    [switch]$SkipPublish = $false,
    [switch]$SoccerOnly = $false,
    [switch]$SkipOdds = $false,
    [switch]$SkipPaper = $false,
    [switch]$ResearchProviderLeagues = $false
)

if ($ResearchProviderLeagues -and $SoccerOnly) {
    Write-Error "ResearchProviderLeagues cannot be combined with SoccerOnly."
    exit 64
}

$ErrorActionPreference = "Continue"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

if (-not $SoccerOnly -and (Test-Path -LiteralPath "config/research_provider_leagues.local" -PathType Leaf)) {
    $ResearchProviderLeagues = $true
}

$paperEnabled = -not $SkipPaper -and (Test-Path -LiteralPath "config/paper_trial.yml" -PathType Leaf)
$paperFailed = $false
$runBenchmark = -not $SkipTraining -and -not $SkipBenchmark
$releaseQualityPassed = $true
$paidOutput = "reports/publishable_bets/latest_publishable_bets.json"
$nbaQuoteReceipt = ""
$paidGateStarted = ""
$paidGateFinished = ""
if ($paperEnabled -and -not ($SkipPrediction -or $SkipOdds -or $SoccerOnly)) {
    $nbaQuoteReceipt = Join-Path ([IO.Path]::GetTempPath()) ("sports-nba-receipt-" + [guid]::NewGuid())
}

function Write-NbaQuoteTiming {
    if (-not $nbaQuoteReceipt) { return }
    $previousExitCode = $global:LASTEXITCODE
    $timingArgs = @("--receipt-file", $nbaQuoteReceipt)
    if ($paidGateStarted) { $timingArgs += @("--paid-started-at", $paidGateStarted) }
    if ($paidGateFinished) { $timingArgs += @("--paid-finished-at", $paidGateFinished) }
    try {
        & python -m scripts.nba_quote_timing @timingArgs
        if ($LASTEXITCODE -ne 0) { Write-Warning "NBA quote timing could not run." }
    } catch {
        Write-Warning "NBA quote timing could not run."
    } finally {
        Remove-Item -LiteralPath $nbaQuoteReceipt -ErrorAction SilentlyContinue
        $global:LASTEXITCODE = $previousExitCode
    }
}

function Complete-NbaEvidence {
    Write-NbaQuoteTiming
    if (-not $runBenchmark -or -not $paperEnabled -or $SkipOdds -or $SoccerOnly) { return }

    Write-Host "=== Saving Next-Day NBA Schedule Audit ==="
    $previousExitCode = $global:LASTEXITCODE
    try {
        & poetry run python -m src.data.audit_nba_schedule --next-eastern-day --fetch --save 2>&1 |
            ForEach-Object { "$_" }
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Next-day NBA schedule audit could not run."
        }
    } catch {
        Write-Warning "Next-day NBA schedule audit could not run."
    } finally {
        $global:LASTEXITCODE = $previousExitCode
    }
}

$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = "logs/pipeline_${timestamp}.log"
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

# Start Transcript to capture all output
Start-Transcript -Path $logFile -Append

$configuredPaidLeagues = $env:PAID_RELEASE_LEAGUES
$paidScopeFile = "config/paid_release_leagues.local"
if (-not $configuredPaidLeagues -and -not $SoccerOnly -and (Test-Path -LiteralPath $paidScopeFile -PathType Leaf)) {
    $knownLeagues = "NFL|NBA|CFB|NCAAB|NHL|EPL|LALIGA|BUNDESLIGA|SERIEA|LIGUE1"
    try {
        $selection = ([string](Get-Content -LiteralPath $paidScopeFile -Raw -ErrorAction Stop)).Trim()
        if ($selection -cmatch "^($knownLeagues)(,($knownLeagues))*$") {
            $configuredPaidLeagues = $selection
        } else {
            Write-Warning "Invalid workspace paid-release scope. Using the standard scope."
        }
    } catch {
        Write-Warning "Workspace paid-release scope could not be read. Using the standard scope."
    }
}
$paidReleaseLeagues = if ($configuredPaidLeagues) {
    $configuredPaidLeagues
} elseif ($SoccerOnly) {
    "EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1"
} else {
    "NBA,NHL,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1"
}
Write-Host "Paid release leagues: $paidReleaseLeagues"



Write-Host "=== Starting Data Backup ==="
python "$scriptDir/backup_data.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Data backup failed. Proceeding with caution..."
} else {
    Write-Host "Data backup completed."
}

# 1. Ingest All Data
Write-Host "=== Starting Data Ingestion ==="
& "$scriptDir/ingest_all.ps1" -SoccerOnly:$SoccerOnly -SkipOdds:$SkipOdds `
    -Paper:($paperEnabled -and -not $SkipPrediction) -NbaQuoteReceipt $nbaQuoteReceipt `
    -ResearchProviderLeagues:$ResearchProviderLeagues
if ($LASTEXITCODE -eq 2) {
    Write-Warning "The immediate NBA paper capture failed. Data ingestion continued."
    $paperFailed = $true
} elseif ($LASTEXITCODE -ne 0) {
    $ingestExitCode = $LASTEXITCODE
    Write-Host "ERROR: Data ingestion failed."
    Complete-NbaEvidence
    exit $ingestExitCode
}

# 1b. Paid-release data hygiene
Write-Host "=== Starting Paid-Release Data Hygiene ==="
python -m src.data.quality --prune-orphans --warn-only --leagues $paidReleaseLeagues
python -m src.data.score_backfill --resolve-stale --lookback-days 14 --leagues $paidReleaseLeagues
python -m src.data.quality --finalize-scored --close-unresolved-stale --warn-only --leagues $paidReleaseLeagues
if ($runBenchmark -or -not $SkipPublish) {
    python -m src.data.quality --leagues $paidReleaseLeagues
    $releaseQualityPassed = $LASTEXITCODE -eq 0
    if (-not $releaseQualityPassed) {
        Write-Warning "Paid-release data quality failed."
        if (-not $SkipPublish) {
            python -m src.predict.publishable_bets revoke --output $paidOutput --reason data_quality_failed
            if ($LASTEXITCODE -ne 0) {
                $revokeExitCode = $LASTEXITCODE
                Write-Error "Failed to revoke the active paid release."
                Complete-NbaEvidence
                exit $revokeExitCode
            }
        }
    }
}

if ($paperEnabled) {
    Write-Host "=== Updating Paper Ledger ==="
    & poetry run python -m src.predict.paper_ledger update
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Paper ledger update failed (exit=$LASTEXITCODE)."
        $paperFailed = $true
    }
}

# 2. Predict. These current predictions are not subscriber-facing unless the
# publish gate matches them to an approved, passing rule.
if (-not $SkipPrediction) {
    Write-Host "=== Starting Current Prediction Refresh ==="
    & "$scriptDir/predict.ps1" -SoccerOnly:$SoccerOnly
    if ($LASTEXITCODE -eq 2) {
        Write-Warning "The NBA paper capture run failed. Other predictions completed."
        $paperFailed = $true
    } elseif ($LASTEXITCODE -ne 0) {
        $predictionExitCode = $LASTEXITCODE
        Write-Host "ERROR: Prediction pipeline failed."
        Complete-NbaEvidence
        exit $predictionExitCode
    }
} else {
    Write-Host "=== Skipping Current Prediction Refresh ==="
}

# 3. Paid-picks publish gate. --allow-empty keeps fail-closed no-pick days from
# failing automation.
if (-not $SkipPublish -and $releaseQualityPassed) {
    Write-Host "=== Starting Paid-Picks Publish Gate ==="
    if ($nbaQuoteReceipt) { $paidGateStarted = [DateTimeOffset]::UtcNow.ToString("o") }
    python -m src.predict.publishable_bets publish `
        --rules config/published_rules.yml `
        --output $paidOutput `
        --quality-output reports/publishable_bets/latest_quality_report.json `
        --leagues $paidReleaseLeagues `
        --allow-empty
    if ($LASTEXITCODE -ne 0) {
        $publishExitCode = $LASTEXITCODE
        Write-Host "ERROR: Paid-picks publish gate failed."
        Complete-NbaEvidence
        exit $publishExitCode
    }
    if ($nbaQuoteReceipt) { $paidGateFinished = [DateTimeOffset]::UtcNow.ToString("o") }
} else {
    Write-Host "=== Skipping Paid-Picks Publish Gate ==="
}
Complete-NbaEvidence

# 4. Benchmark after publishing so research does not delay the prediction refresh.
if ($runBenchmark -and $releaseQualityPassed) {
    Write-Host "=== Starting Rolling-Origin Betting Benchmark ==="
    python -m src.models.train_betting `
        --benchmark `
        --benchmark-config config/betting_benchmark.yml `
        --benchmark-output-dir reports/betting_benchmarks
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Betting benchmark failed."
        exit $LASTEXITCODE
    }
} else {
    Write-Host "=== Skipping Paid-Picks Benchmark ==="
}

Write-Host "=== Master Pipeline Completed ==="

Stop-Transcript
if ($paperFailed -or -not $releaseQualityPassed) {
    exit 1
}
