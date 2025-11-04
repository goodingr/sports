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

    # Run the prediction command
    $Output = poetry run python -m src.models.forward_test predict --dotenv .env 2>&1
    
    Write-Log "Command output:"
    $Output | ForEach-Object { Write-Log $_ }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Log "Predictions completed successfully"
        exit 0
    } else {
        Write-Log "ERROR: Predictions failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
} catch {
    Write-Log "ERROR: Exception occurred: $_"
    exit 1
}

