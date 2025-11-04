# Schedule Forward Testing Tasks - Simple Version
# Uses command-line schtasks instead of XML for better compatibility

param(
    [switch]$Remove,
    [string]$WorkingDir = (Get-Location).Path
)

$ErrorActionPreference = "Continue"

Write-Host ("=" * 60)
Write-Host "Forward Testing Task Scheduler Setup"
Write-Host ("=" * 60)
Write-Host ""

# Convert to absolute path
$WorkingDir = (Resolve-Path $WorkingDir -ErrorAction SilentlyContinue).Path
if (-not $WorkingDir) {
    $WorkingDir = (Get-Location).Path
}

Write-Host "Working Directory: $WorkingDir"
Write-Host ""

if ($Remove) {
    Write-Host "Removing scheduled tasks..."
    schtasks /Delete /TN "NBA Forward Test - Predict" /F 2>$null
    schtasks /Delete /TN "NBA Forward Test - Update" /F 2>$null
    Write-Host "[OK] Tasks removed"
    exit 0
}

# Get PowerShell executable path
$PowerShellExe = (Get-Command powershell.exe).Source

# Task 1: Make Predictions (Daily at 6:00 PM EST / 11:00 PM UTC)
Write-Host "Creating task: Make Predictions (Daily 6:00 PM EST)..."
$predictScript = Join-Path $WorkingDir "scripts\run_forward_test_predict.ps1"

schtasks /Create `
    /TN "NBA Forward Test - Predict" `
    /TR "$PowerShellExe -ExecutionPolicy Bypass -File `"$predictScript`"" `
    /SC DAILY `
    /ST 23:00 `
    /RU "$env:USERDOMAIN\$env:USERNAME" `
    /RP "" `
    /F `
    2>&1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Task created: NBA Forward Test - Predict"
} else {
    Write-Host "[X] Failed to create task (may already exist)"
}

# Task 2: Update Results (Daily at 2:00 AM EST / 7:00 AM UTC)
Write-Host "Creating task: Update Results (Daily 2:00 AM EST)..."
$updateScript = Join-Path $WorkingDir "scripts\run_forward_test_update.ps1"

schtasks /Create `
    /TN "NBA Forward Test - Update" `
    /TR "$PowerShellExe -ExecutionPolicy Bypass -File `"$updateScript`"" `
    /SC DAILY `
    /ST 07:00 `
    /RU "$env:USERDOMAIN\$env:USERNAME" `
    /RP "" `
    /F `
    2>&1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Task created: NBA Forward Test - Update"
} else {
    Write-Host "[X] Failed to create task (may already exist)"
}

Write-Host ""
Write-Host ("=" * 60)
Write-Host "Setup Complete!"
Write-Host ("=" * 60)
Write-Host ""
Write-Host "Task 1: NBA Forward Test - Predict"
Write-Host "  Schedule: Daily at 11:00 PM UTC (6:00 PM EST)"
Write-Host "  Action: Makes predictions on live games"
Write-Host ""
Write-Host "Task 2: NBA Forward Test - Update"
Write-Host "  Schedule: Daily at 7:00 AM UTC (2:00 AM EST)"
Write-Host "  Action: Updates predictions with game results"
Write-Host ""
Write-Host "To verify tasks:"
Write-Host "  schtasks /Query /TN `"NBA Forward Test - Predict`""
Write-Host "  schtasks /Query /TN `"NBA Forward Test - Update`""
Write-Host ""
Write-Host "To run manually:"
Write-Host "  schtasks /Run /TN `"NBA Forward Test - Predict`""
Write-Host "  schtasks /Run /TN `"NBA Forward Test - Update`""
Write-Host ""
Write-Host "To remove tasks:"
Write-Host "  .\scripts\schedule_forward_test_simple.ps1 -Remove"
Write-Host ""


