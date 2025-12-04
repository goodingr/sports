#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Set up a single hourly Windows Task Scheduler job for the entire pipeline
.DESCRIPTION
    Creates a scheduled task that runs the comprehensive hourly pipeline script every hour
#>

$ErrorActionPreference = "Stop"

# Get the script directory and project root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

# Task configuration
$TaskName = "SportsAnalyticsHourly"
$TaskDescription = "Hourly sports betting analytics pipeline (data ingestion, training, predictions)"
$ScriptPath = Join-Path $projectRoot "scripts\pipeline.ps1"

# Verify the script exists
if (!(Test-Path $ScriptPath)) {
    Write-Error "Pipeline script not found at: $ScriptPath"
    exit 1
}

# Check if task already exists and remove it
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task: $TaskName"
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the action - run PowerShell with the pipeline script
$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`"" `
    -WorkingDirectory $projectRoot

# Create the trigger - every hour
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddHours((Get-Date).Hour + 1) -RepetitionInterval (New-TimeSpan -Hours 1)

# Create the settings
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -MultipleInstances IgnoreNew

# Create the principal (run with user privileges)
# Note: If you want to run with highest privileges, you must run this setup script as Administrator
$Principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType S4U `
    -RunLevel Limited

# Register the task
Write-Host "Registering scheduled task: $TaskName"
Write-Host "  Script: $ScriptPath"
Write-Host "  Frequency: Every hour"
Write-Host "  Next run: $((Get-Date).Date.AddHours((Get-Date).Hour + 1))"

Register-ScheduledTask `
    -TaskName $TaskName `
    -Description $TaskDescription `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Force

Write-Host ""
Write-Host "Task registered successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "To manage the task:"
Write-Host "  View:    Get-ScheduledTask -TaskName '$TaskName'"
Write-Host "  Run now: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "  Stop:    Stop-ScheduledTask -TaskName '$TaskName'"
Write-Host "  Remove:  Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
Write-Host ""
Write-Host "Logs will be saved to: $projectRoot\logs\"

