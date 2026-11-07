# Run one closing-quote inspection, or allow a bounded collection with -Collect.
param(
    [Parameter(Mandatory = $true)]
    [string]$PythonPath,
    [switch]$Collect = $false
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot
if ($PythonPath -notmatch '^(?:[A-Za-z]:[\\/]|[\\/]{2}[^\\/]+[\\/][^\\/]+[\\/])') {
    throw "PythonPath must be an absolute interpreter path."
}
$python = Get-Item -LiteralPath $PythonPath -ErrorAction Stop
if ($python.PSIsContainer) {
    throw "PythonPath must name an interpreter file."
}

$logDirectory = Join-Path $projectRoot "logs"
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
$startedAt = [DateTime]::UtcNow
$logPath = Join-Path $logDirectory ("paper_close_{0}.log" -f $startedAt.ToString("yyyyMMdd"))
$pythonArgs = @("-B", "-m", "src.predict.paper_close_poll")
if ($Collect) {
    $pythonArgs += "--collect"
}

# Windows PowerShell 5.1 represents redirected native stderr as ErrorRecord.
# Capture it under Continue so stderr cannot interrupt native exit-code handling.
$ErrorActionPreference = "Continue"
$global:LASTEXITCODE = 1
$output = & $python.FullName @pythonArgs 2>&1
$nativeExitCode = $LASTEXITCODE
$ErrorActionPreference = "Stop"
$mode = if ($Collect) { "collect" } else { "preview" }
$lines = @("[{0}] mode={1}" -f $startedAt.ToString("o"), $mode)
foreach ($line in $output) {
    $lines += "[{0}] {1}" -f [DateTime]::UtcNow.ToString("o"), $line.ToString()
    Write-Output $line.ToString()
}
$lines += "[{0}] exit={1}" -f [DateTime]::UtcNow.ToString("o"), $nativeExitCode
Add-Content -LiteralPath $logPath -Value $lines -Encoding UTF8
exit $nativeExitCode
