# Schedule Forward Testing Tasks for Windows Task Scheduler
# This script creates scheduled tasks to automatically run forward testing

param(
    [switch]$Remove,
    [string]$WorkingDir = (Get-Location).Path
)

$ErrorActionPreference = "Stop"

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

# Task 1: Make Predictions (Daily at 6:00 PM EST / 11:00 PM UTC)
Write-Host "Creating task: Make Predictions (Daily 6:00 PM EST)..."
$predictScript = Join-Path $WorkingDir "scripts\run_forward_test_predict.ps1"
$predictCommand = "powershell.exe -ExecutionPolicy Bypass -File `"$predictScript`""

$xml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss")</Date>
    <Author>NBA Forward Testing System</Author>
    <Description>Makes predictions on live NBA games before they start</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>$(Get-Date -Format "yyyy-MM-dd")T23:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>$env:USERDOMAIN\$env:USERNAME</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>$predictCommand</Command>
      <WorkingDirectory>$WorkingDir</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

$xmlPath = Join-Path $env:TEMP "forward_test_predict.xml"
# Write XML with UTF-16 encoding (required for Task Scheduler)
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($xmlPath, $xml, $utf8NoBom)

schtasks /Create /TN "NBA Forward Test - Predict" /XML $xmlPath /F
Remove-Item $xmlPath
Write-Host "[OK] Task created: NBA Forward Test - Predict"

# Task 2: Update Results (Daily at 2:00 AM EST / 7:00 AM UTC)
Write-Host "Creating task: Update Results (Daily 2:00 AM EST)..."
$updateScript = Join-Path $WorkingDir "scripts\run_forward_test_update.ps1"
$updateCommand = "powershell.exe -ExecutionPolicy Bypass -File `"$updateScript`""

$xml2 = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>$(Get-Date -Format "yyyy-MM-ddTHH:mm:ss")</Date>
    <Author>NBA Forward Testing System</Author>
    <Description>Updates forward test predictions with actual game results</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>$(Get-Date -Format "yyyy-MM-dd")T07:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>$env:USERDOMAIN\$env:USERNAME</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>$updateCommand</Command>
      <WorkingDirectory>$WorkingDir</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

$xmlPath2 = Join-Path $env:TEMP "forward_test_update.xml"
# Write XML with UTF-16 encoding (required for Task Scheduler)
[System.IO.File]::WriteAllText($xmlPath2, $xml2, $utf8NoBom)

schtasks /Create /TN "NBA Forward Test - Update" /XML $xmlPath2 /F
Remove-Item $xmlPath2
Write-Host "[OK] Task created: NBA Forward Test - Update"

Write-Host ""
Write-Host ("=" * 60)
Write-Host "Tasks Scheduled Successfully!"
Write-Host ("=" * 60)
Write-Host ""
Write-Host "Task 1: NBA Forward Test - Predict"
Write-Host "  Schedule: Daily at 6:00 PM EST (11:00 PM UTC)"
Write-Host "  Action: Makes predictions on live games"
Write-Host ""
Write-Host "Task 2: NBA Forward Test - Update"
Write-Host "  Schedule: Daily at 2:00 AM EST (7:00 AM UTC)"
Write-Host "  Action: Updates predictions with game results"
Write-Host ""
Write-Host "To view tasks:"
Write-Host "  schtasks /Query /TN `"NBA Forward Test - Predict`""
Write-Host "  schtasks /Query /TN `"NBA Forward Test - Update`""
Write-Host ""
Write-Host "To run manually:"
Write-Host "  schtasks /Run /TN `"NBA Forward Test - Predict`""
Write-Host "  schtasks /Run /TN `"NBA Forward Test - Update`""
Write-Host ""
Write-Host "To remove tasks:"
Write-Host "  .\scripts\schedule_forward_test.ps1 -Remove"
Write-Host ""

