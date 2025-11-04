# Script to update College Football forward test results (called by Task Scheduler)
# Mirrors the NBA/NFL script but calls CFB ingestion/update commands

$ErrorActionPreference = "Continue"

# Find project root by looking for pyproject.toml
$WorkingDir = $PSScriptRoot
$MaxDepth = 5
$Depth = 0
while ($Depth -lt $MaxDepth -and -not (Test-Path (Join-Path $WorkingDir "pyproject.toml"))) {
    $WorkingDir = Split-Path -Parent $WorkingDir
    if (-not $WorkingDir -or $WorkingDir -eq (Split-Path -Parent $WorkingDir)) {
        $WorkingDir = Split-Path -Parent $PSScriptRoot
        break
    }
    $Depth++
}

Set-Location $WorkingDir
Write-Host "Working directory: $WorkingDir"
Write-Host "pyproject.toml exists: $(Test-Path 'pyproject.toml')"

$LogDir = Join-Path $WorkingDir "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$LogFile = Join-Path $LogDir "forward_test_update_cfb_$(Get-Date -Format 'yyyyMMdd').log"

function Write-Log {
    param([string]$Message)
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMessage = "[$Timestamp] $Message"
    Add-Content -Path $LogFile -Value $LogMessage
    Write-Host $LogMessage
}

Write-Log "Starting CFB forward test update..."

try {
    # Refresh recent CFB results via CFBD
    Write-Log "Loading recent CFB game results..."
    $ResultOutput = poetry run python -m src.data.ingest_results_cfb --seasons $((Get-Date).Year) 2>&1
    $ResultOutput | ForEach-Object { Write-Log $_ }

    # Run the update command (league CFB)
    Write-Log "Updating CFB predictions with results..."
    $Output = poetry run python -m src.models.forward_test update --league CFB 2>&1

    Write-Log "Command output:"
    $Output | ForEach-Object { Write-Log $_ }

    if ($LASTEXITCODE -eq 0) {
        Write-Log "CFB update completed successfully"
        exit 0
    } else {
        Write-Log "ERROR: CFB update failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
} catch {
    Write-Log "ERROR: Exception occurred: $_"
    exit 1
}
