# Script to run forward test predictions (called by Task Scheduler)
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

$LogFile = Join-Path $LogDir "forward_test_predict_$(Get-Date -Format 'yyyyMMdd').log"

function Write-Log {
    param([string]$Message)
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMessage = "[$Timestamp] $Message"
    Add-Content -Path $LogFile -Value $LogMessage
    Write-Host $LogMessage
}

Write-Log "Starting forward test predictions..."

# Discover supported leagues from the Python module so new leagues are picked up automatically
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
    # Load environment variables
    $EnvFile = Join-Path $WorkingDir ".env"
    if (Test-Path $EnvFile) {
        Get-Content $EnvFile | ForEach-Object {
            if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
                $name = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($name, $value, "Process")
            }
        }
    }

    $OverallExitCode = 0

    foreach ($League in $Leagues) {
        Write-Log "Running predictions for league: $League"

        $Output = & poetry run python -m src.models.forward_test predict --league $League --dotenv .env 2>&1

        Write-Log "Command output for ${League}:"
        $Output | ForEach-Object { Write-Log $_ }

        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR: Predictions failed for $League with exit code $LASTEXITCODE"
            $OverallExitCode = $LASTEXITCODE
        } else {
            Write-Log "Predictions completed successfully for $League"
        }
    }

    if ($OverallExitCode -eq 0) {
        Write-Log "All league predictions completed successfully"
        exit 0
    } else {
        Write-Log "One or more league predictions failed"
        exit $OverallExitCode
    }
} catch {
    Write-Log "ERROR: Exception occurred: $_"
    exit 1
}

