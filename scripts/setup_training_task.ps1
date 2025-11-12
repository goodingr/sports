Param(
    [string]$TaskName = "SportsModelTraining",
    [datetime]$StartTime = [datetime]::Today.AddHours(3).AddMinutes(30),
    [string]$TaskUser = "$env:USERNAME",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scriptPath = (Resolve-Path (Join-Path $repoRoot "scripts\train_daily_models.ps1")).Path

if (-not (Test-Path $scriptPath)) {
    throw "Training script not found at $scriptPath."
}

try {
    $poetryExe = (Get-Command poetry -ErrorAction Stop).Source
} catch {
    throw "Poetry executable not found on PATH. Ensure Poetry is installed before registering the task."
}

# Build the action to run the training script via PowerShell
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""

$trigger = New-ScheduledTaskTrigger -Daily -At $StartTime

$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

$principal = New-ScheduledTaskPrincipal -UserId $TaskUser -LogonType Interactive -RunLevel Limited

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    if (-not $Force) {
        throw "A scheduled task named '$TaskName' already exists. Use -Force to overwrite it."
    }

    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal

Write-Host "Registered scheduled task '$TaskName' to run daily at $($StartTime.ToShortTimeString())." -ForegroundColor Green
