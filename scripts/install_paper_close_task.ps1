# Preview a paper task. Use -Entry or -Discover for the supplemental modes.
param(
    [string]$PythonPath = "",
    [switch]$Entry = $false,
    [switch]$Discover = $false,
    [switch]$Apply = $false
)

$ErrorActionPreference = "Stop"
if ($Entry -and $Discover) { throw "Choose one supplemental mode." }
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot
$taskName = if ($Discover) {
    "SportsPaperNBAEvents"
} elseif ($Entry) {
    "SportsPaperEarlyEntry"
} else {
    "SportsPaperClosingQuotes"
}
$taskPath = "\"
$wrapperPath = Join-Path $PSScriptRoot "paper_close_poll.ps1"
$powershellPath = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$description = if ($Discover) {
    "Sports paper NBA event discovery owned by $projectRoot"
} elseif ($Entry) {
    "Sports supplemental paper entry owned by $projectRoot"
} else {
    "Sports paper closing quotes owned by $projectRoot"
}

if (-not $PythonPath) {
    $poetry = Get-Command poetry -CommandType Application -ErrorAction Stop
    $ErrorActionPreference = "Continue"
    $resolvedPython = & $poetry.Source env info --executable 2>&1
    $resolveExitCode = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($resolveExitCode -ne 0 -or @($resolvedPython).Count -ne 1) {
        throw "Could not resolve the existing Poetry interpreter."
    }
    $PythonPath = $resolvedPython.ToString().Trim()
}
if ($PythonPath -notmatch '^(?:[A-Za-z]:[\\/]|[\\/]{2}[^\\/]+[\\/][^\\/]+[\\/])') {
    throw "PythonPath must be an absolute interpreter path."
}
$python = Get-Item -LiteralPath $PythonPath -ErrorAction Stop
if ($python.PSIsContainer) {
    throw "PythonPath must name an interpreter file."
}
if (-not (Test-Path -LiteralPath $wrapperPath -PathType Leaf)) {
    throw "The project paper polling wrapper is missing."
}

$metadataScript = @'
import json
import sqlite3
from contextlib import closing
from datetime import datetime
from src.db.core import resolve_db_path
from src.predict.paper_ledger import load_trial
from src.predict.paper_report import GRADING_GRACE
trial = load_trial()
path = resolve_db_path().resolve()
if path.is_file():
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as conn:
        exists = conn.execute(
            'SELECT 1 FROM sqlite_master WHERE type=? AND name=?', ('table', 'paper_trials')
        ).fetchone()
        if exists:
            registered = conn.execute(
                'SELECT specification FROM paper_trials WHERE trial_id=?', (trial.trial_id,)
            ).fetchone()
            if registered and registered[0] != trial.specification:
                raise ValueError('The trial configuration differs from its registered specification')
review = datetime.fromisoformat(trial.review_at)
print(json.dumps({'trial_id': trial.trial_id, 'review_at': review.isoformat(),
                  'end_boundary': (review + GRADING_GRACE).isoformat()}))
'@
$ErrorActionPreference = "Continue"
$metadataOutput = & $python.FullName -B -c $metadataScript 2>&1
$metadataExitCode = $LASTEXITCODE
$ErrorActionPreference = "Stop"
if ($metadataExitCode -ne 0) {
    throw "Could not read the trial deadline with the selected interpreter."
}
$metadata = ($metadataOutput -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
$deadline = if ($Entry -or $Discover) { $metadata.review_at } else { $metadata.end_boundary }
$expiresAt = [DateTimeOffset]::Parse($deadline).ToLocalTime()
$offsetMinutes = if ($Discover) { 10 } elseif ($Entry) { 5 } else { 0 }
$intervalMinutes = if ($Discover) { 60 } else { 15 }
$phaseTicks = [TimeSpan]::FromMinutes($offsetMinutes).Ticks
$interval = [TimeSpan]::FromMinutes($intervalMinutes)
$startsAt = [DateTimeOffset]::UtcNow.AddMinutes(1)
$phaseRemainder = ($startsAt.Ticks - $phaseTicks) % $interval.Ticks
$startsAt = $startsAt.AddTicks($interval.Ticks - $phaseRemainder).ToLocalTime()
if ($expiresAt -le $startsAt.Add($interval)) {
    throw "The trial deadline does not allow another $taskName interval."
}

$daily = Get-ScheduledTask -TaskName "DailyPipelineRun" -TaskPath $taskPath -ErrorAction Stop
if ([string]$daily.Principal.LogonType -ne "Interactive" -or [string]$daily.Principal.RunLevel -ne "Limited") {
    throw "DailyPipelineRun must use an Interactive, Limited principal."
}
$arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}" -Collect -PythonPath "{1}"' -f $wrapperPath, $python.FullName
if ($Entry) { $arguments += ' -Entry' }
if ($Discover) { $arguments += ' -Discover' }
$existing = Get-ScheduledTask -TaskPath $taskPath -ErrorAction Stop |
    Where-Object { $_.TaskName -eq $taskName }
if ($existing) {
    $actions = @($existing.Actions)
    if (
        $existing.Description -ne $description -or
        $actions.Count -ne 1 -or
        $actions[0].Execute -ne $powershellPath -or
        $actions[0].Arguments -ne $arguments -or
        $actions[0].WorkingDirectory -ne $projectRoot -or
        $existing.Principal.UserId -ne $daily.Principal.UserId -or
        [string]$existing.Principal.LogonType -ne "Interactive" -or
        [string]$existing.Principal.RunLevel -ne "Limited"
    ) {
        throw "A conflicting $taskName task already exists."
    }
}

$plan = [ordered]@{
    mode = if ($Apply) { "registered" } else { "preview" }
    task_name = $taskName
    task_path = $taskPath
    trial_id = $metadata.trial_id
    python_path = $python.FullName
    execute = $powershellPath
    arguments = $arguments
    working_directory = $projectRoot
    principal = $daily.Principal.UserId
    logon_type = "Interactive"
    run_level = "Limited"
    first_start = $startsAt.ToString("o")
    end_boundary = $expiresAt.ToString("yyyy-MM-ddTHH:mm:sszzz")
    repetition_minutes = $intervalMinutes
    offset_minutes = $offsetMinutes
    execution_limit_minutes = if ($Entry) { 5 } else { 2 }
    window_style = "Hidden"
    task_hidden = $false
    start_when_available = $true
    multiple_instances = "IgnoreNew"
    wake_to_run = $false
}
if ($Apply) {
    $action = New-ScheduledTaskAction -Execute $powershellPath -Argument $arguments -WorkingDirectory $projectRoot
    $principal = New-ScheduledTaskPrincipal -UserId $daily.Principal.UserId -LogonType Interactive -RunLevel Limited
    $trigger = New-ScheduledTaskTrigger -Once -At $startsAt.LocalDateTime -RepetitionInterval $interval -RepetitionDuration ($expiresAt - $startsAt)
    $trigger.EndBoundary = $plan.end_boundary
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable:$plan.start_when_available -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::FromMinutes($plan.execution_limit_minutes)) -WakeToRun:$false
    $definition = New-ScheduledTask -Action $action -Principal $principal -Trigger $trigger -Settings $settings -Description $description
    Register-ScheduledTask -TaskName $taskName -TaskPath $taskPath -InputObject $definition -Force:([bool]$existing) -ErrorAction Stop | Out-Null
}
$plan | ConvertTo-Json -Depth 4
