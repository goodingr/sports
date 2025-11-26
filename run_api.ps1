param(
    [Alias('Host')]
    [string]$ApiHost = '0.0.0.0',
    [Alias('Port')]
    [int]$ApiPort = 8000,
    [switch]$Reload,
    [switch]$Help,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$UvicornArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Show-Help {
    @'
Run the Sports Betting FastAPI Backend.

Usage:
  .\run_api.ps1 [-ApiHost 0.0.0.0] [-ApiPort 8000] [-Reload] [-- <extra uvicorn args>]

Options:
  -ApiHost <value>    Host/interface to bind (default: 0.0.0.0)
  -ApiPort <value>    Port to serve (default: 8000)
  -Reload             Enable auto-reload (for development)
  -Help               Show this help text
  --                  Pass remaining arguments directly to uvicorn
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

$cmdArgs = @('run', 'uvicorn', 'src.api.main:app', '--host', $ApiHost, '--port', $ApiPort.ToString())

if ($Reload.IsPresent) {
    $cmdArgs += '--reload'
}

if ($UvicornArgs) {
    $cmdArgs += $UvicornArgs
}

Write-Host "Running: poetry $($cmdArgs -join ' ')"

& poetry @cmdArgs
