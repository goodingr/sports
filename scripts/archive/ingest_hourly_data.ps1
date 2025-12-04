Param(
    [string[]]$Leagues = @("NBA", "NFL", "CFB", "MLB", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

try {
    $poetryExe = (Get-Command poetry -ErrorAction Stop).Source
} catch {
    throw "Poetry executable not found on PATH. Ensure Poetry is installed and available before running this script."
}

function Invoke-PoetryCommand {
    Param(
        [Parameter(Mandatory = $true)]
        [string[]]$ArgumentList
    )

    Write-Host "Running: poetry run $($ArgumentList -join ' ')" -ForegroundColor Cyan
    & $poetryExe 'run' @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Command failed with exit code $LASTEXITCODE"
    }
}

# Source handlers for hourly ingestion
$hourlySources = @{
    "NBA" = @(
        @{handler = "src.data.sources.espn_odds:ingest_nba"; name = "ESPN NBA odds"},
        @{handler = "src.data.sources.nba_rolling_metrics:ingest"; name = "NBA rolling metrics"},
        @{handler = "src.data.sources.nba_injuries_espn:ingest"; name = "NBA injuries (ESPN)"}
    )
    "NFL" = @(
        @{handler = "src.data.sources.espn_odds:ingest_nfl"; name = "ESPN NFL odds"}
    )
    "CFB" = @(
        @{handler = "src.data.sources.espn_odds:ingest_cfb"; name = "ESPN CFB odds"},
        @{handler = "src.data.sources.cfbd_advanced_stats:ingest"; name = "CFBD advanced team stats"}
    )
    "MLB" = @(
        @{handler = "src.data.sources.espn_odds:ingest_mlb"; name = "ESPN MLB odds"},
        @{handler = "src.data.sources.mlb_advanced_stats:ingest"; name = "MLB advanced stats"}
    )
    "EPL" = @(
        @{handler = "src.data.sources.espn_odds:ingest_epl"; name = "ESPN EPL odds"}
    )
    "LALIGA" = @(
        @{handler = "src.data.sources.espn_odds:ingest_laliga"; name = "ESPN La Liga odds"}
    )
    "BUNDESLIGA" = @(
        @{handler = "src.data.sources.espn_odds:ingest_bundesliga"; name = "ESPN Bundesliga odds"}
    )
    "SERIEA" = @(
        @{handler = "src.data.sources.espn_odds:ingest_seriea"; name = "ESPN Serie A odds"}
    )
    "LIGUE1" = @(
        @{handler = "src.data.sources.espn_odds:ingest_ligue1"; name = "ESPN Ligue 1 odds"}
    )
}

$soccerAdvancedHandled = $false

$blockages = @()

foreach ($league in $Leagues) {
    if (-not $hourlySources.ContainsKey($league)) {
        Write-Warning "No hourly sources configured for league '$league'"
        continue
    }

    Write-Host "`n=== Processing $league ===" -ForegroundColor Green

    foreach ($source in $hourlySources[$league]) {
        Write-Host "Ingesting: $($source.name)" -ForegroundColor Yellow
        
        try {
            $handlerParts = $source.handler -split ":"
            $module = $handlerParts[0]
            $function = $handlerParts[1]
            
            $expression = if ($source.ContainsKey("expression") -and $null -ne $source.expression) {
                $source.expression
            } else {
                "from $module import $function; $function()"
            }

            $args = @("python", "-c", $expression)
            
            Invoke-PoetryCommand $args
            
            if ($LASTEXITCODE -ne 0) {
                $blockage = @{
                    League = $league
                    Source = $source.name
                    Handler = $source.handler
                    Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                    Error = "Exit code $LASTEXITCODE"
                }
                $blockages += $blockage
                Write-Warning "Failed to ingest $($source.name) for $league"
            }
        } catch {
            $blockage = @{
                League = $league
                Source = $source.name
                Handler = $source.handler
                Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                Error = $_.Exception.Message
            }
            $blockages += $blockage
            Write-Warning "Error ingesting $($source.name) for $league : $_"
        }
    }

    if (($league -in @("EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")) -and -not $soccerAdvancedHandled) {
        Write-Host "Ingesting: Understat soccer advanced stats" -ForegroundColor Yellow
        try {
            $args = @(
                "python",
                "-c",
                "from src.data.sources.soccer_advanced_stats import ingest; ingest()"
            )
            Invoke-PoetryCommand $args
            if ($LASTEXITCODE -ne 0) {
                $blockages += @{
                    League = "Soccer"
                    Source = "Understat advanced stats"
                    Handler = "src.data.sources.soccer_advanced_stats:ingest"
                    Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                    Error = "Exit code $LASTEXITCODE"
                }
                Write-Warning "Failed to ingest soccer advanced stats"
            } else {
                $soccerAdvancedHandled = $true
            }
        } catch {
            $blockages += @{
                League = "Soccer"
                Source = "Understat advanced stats"
                Handler = "src.data.sources.soccer_advanced_stats:ingest"
                Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                Error = $_.Exception.Message
            }
            Write-Warning "Error ingesting soccer advanced stats : $_"
        }
    }
}

# Log blockages if any
if ($blockages.Count -gt 0) {
    Write-Host "`n=== Blockages Encountered ===" -ForegroundColor Red
    $blockages | Format-Table -AutoSize
    
    # Append to blockages log file
    $blockagesFile = Join-Path $repoRoot "docs" "scraping_blockages.md"
    $blockagesDir = Split-Path $blockagesFile -Parent
    if (-not (Test-Path $blockagesDir)) {
        New-Item -ItemType Directory -Path $blockagesDir -Force | Out-Null
    }
    
    $logEntry = "`n## Blockages Logged: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`n`n"
    foreach ($blockage in $blockages) {
        $logEntry += "- **$($blockage.League) - $($blockage.Source)**: $($blockage.Error) (Handler: $($blockage.Handler))`n"
    }
    $logEntry += "`n"
    
    Add-Content -Path $blockagesFile -Value $logEntry
    Write-Host "Blockages logged to $blockagesFile" -ForegroundColor Yellow
} else {
    Write-Host "`n=== All sources ingested successfully ===" -ForegroundColor Green
}

Write-Host "`nHourly ingestion completed." -ForegroundColor Green

