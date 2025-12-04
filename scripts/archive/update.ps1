#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Convenience wrapper to run the forward test update action for a specific league.

.DESCRIPTION
    Executes `poetry run python -m src.models.forward_test update --league <League>`
    from the project root. Optionally accepts a custom .env path that will be passed
    through to the Python command if it exists on disk.
#>

param(
    [ValidateSet("NBA", "NFL", "CFB", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1", "ALL")]
    [string]$League = "ALL",

    [string]$DotEnvPath = ".env"
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$resolvedEnv = $null
if ($DotEnvPath -and (Test-Path $DotEnvPath)) {
    $resolvedEnv = (Resolve-Path $DotEnvPath).Path
}

if (-not $PSBoundParameters.ContainsKey("League")) {
    $League = "ALL"
}

Write-Host ("[{0}] Updating results for league: {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $League)

$poetryArgs = @(
    "run", "python", "-m", "src.models.forward_test", "update",
    "--league", $League
)

if ($resolvedEnv) {
    Write-Host ("Using env file: {0}" -f $resolvedEnv)
    $poetryArgs += @("--dotenv", $resolvedEnv)
} elseif ($DotEnvPath -and $DotEnvPath -ne "") {
    Write-Warning ("Dotenv file not found at '{0}'. Continuing without it." -f $DotEnvPath)
}

try {
    & poetry @poetryArgs
    Write-Host "Forward test update completed."
} catch {
    Write-Error "Forward test update failed: $_"
    exit 1
}
