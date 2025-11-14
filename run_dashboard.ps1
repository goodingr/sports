param(
    [Alias('Host')]
    [string]$DashboardHost = '0.0.0.0',
    [Alias('Port')]
    [int]$DashboardPort = 8050,
    [switch]$DashDebug,
    [switch]$NoDebug,
    [switch]$Help,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$DashArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Show-Help {
    @'
Run the Dash-based forward testing dashboard.

Usage:
  .\run_dashboard.ps1 [-DashboardHost 0.0.0.0] [-DashboardPort 8050] [-DashDebug] [-NoDebug] [-- <extra dash args>]

Options:
  -DashboardHost <value>    Host/interface to bind (default: 0.0.0.0)
  -DashboardPort <value>    Port to serve (default: 8050)
  -DashDebug       Enable Dash debug/reload mode
  -NoDebug         Explicitly disable debug mode (ignores env defaults)
  -Help            Show this help text
  --               Pass remaining arguments directly to `python -m src.dashboard`
'@ | Write-Host
}

if ($Help) {
    Show-Help
    exit 0
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$poetryCmd = Get-Command poetry -ErrorAction SilentlyContinue
if (-not $poetryCmd) {
    Write-Error "Poetry is required but was not found. Install it from https://python-poetry.org/docs/ or add it to PATH."
    exit 1
}

$predictionsPath = 'data/forward_test/predictions_master.parquet'
if (-not (Test-Path $predictionsPath)) {
    Write-Warning "$predictionsPath not found. The dashboard will still start but metrics will be empty."
}

$cmdArgs = @('--host', $DashboardHost, '--port', $DashboardPort.ToString())
if ($DashDebug.IsPresent) {
    $cmdArgs += '--debug'
}
elseif ($NoDebug.IsPresent) {
    $cmdArgs += '--no-debug'
}

if ($DashArgs) {
    $cmdArgs += $DashArgs
}

Write-Host "Running: poetry run python -m src.dashboard $($cmdArgs -join ' ')"

& poetry run python -m src.dashboard @cmdArgs
