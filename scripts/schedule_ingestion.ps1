# Windows Task Scheduler script for automated data ingestion
# Run this to set up scheduled tasks for hourly/daily source ingestion

param(
    [string]$TaskName = "SportsBetting-Ingestion",
    [string]$Frequency = "Hourly"  # Hourly, Daily
)

$ScriptPath = Join-Path $PSScriptRoot ".." "src" "data" "ingest_sources.py"
$PoetryPath = "poetry"
$WorkingDir = Split-Path -Parent $PSScriptRoot

if ($Frequency -eq "Hourly") {
    $Trigger = New-ScheduledTaskTrigger -At (Get-Date) -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 365)
    $Action = New-ScheduledTaskAction -Execute $PoetryPath -Argument "run python -m src.data.ingest_sources --league nfl --league nba" -WorkingDirectory $WorkingDir
} else {
    $Trigger = New-ScheduledTaskTrigger -Daily -At "02:00"
    $Action = New-ScheduledTaskAction -Execute $PoetryPath -Argument "run python -m src.data.ingest_sources" -WorkingDirectory $WorkingDir
}

$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings -Description "Automated sports betting data ingestion"

Write-Host "Scheduled task '$TaskName' created successfully"
Write-Host "View tasks: Get-ScheduledTask -TaskName $TaskName"
Write-Host "Remove task: Unregister-ScheduledTask -TaskName $TaskName -Confirm:`$false"

