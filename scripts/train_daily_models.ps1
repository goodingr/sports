Param(
    [string[]]$Leagues = @(
        "NBA",
        "NFL",
        "CFB",
        "MLB",
        "EPL",
        "LALIGA",
        "BUNDESLIGA",
        "SERIEA",
        "LIGUE1"
    )
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

try {
    $poetryExe = (Get-Command poetry -ErrorAction Stop).Source
} catch {
    throw "Poetry executable not found on PATH. Ensure Poetry is installed and available before running this script."
}

function Invoke-PoetryCommand {
    Param(
        [Parameter(Mandatory = $true)]
        [string[]]$ArgumentList
    )

    Write-Host "Running: poetry run $($ArgumentList -join ' ')" -ForegroundColor Cyan
    & $poetryExe 'run' @ArgumentList
}

$seasonMap = @{
    "NBA"        = @(2016..2024)
    "NFL"        = @(2016..2024)
    "CFB"        = @(2016..2024)
    "MLB"        = @(2016..2024)
    "EPL"        = @(2008..2016)
    "LALIGA"     = @(2008..2016)
    "BUNDESLIGA" = @(2008..2016)
    "SERIEA"     = @(2008..2016)
    "LIGUE1"     = @(2008..2016)
}

foreach ($league in $Leagues) {
    if (-not $seasonMap.ContainsKey($league)) {
        Write-Warning "Skipping unsupported league '$league'"
        continue
    }

    Write-Host "=== Processing $league ===" -ForegroundColor Yellow

    $seasonArgs = $seasonMap[$league] | ForEach-Object { $_.ToString() }

    # Build dataset
    $datasetArgs = @(
        "python",
        "-m",
        "src.features.moneyline_dataset",
        "--league",
        $league,
        "--seasons"
    ) + $seasonArgs
    Invoke-PoetryCommand $datasetArgs

    # Train calibrated gradient boosting model
    $trainArgs = @(
        "python",
        "-m",
        "src.models.train",
        "--league",
        $league,
        "--seasons"
    ) + $seasonArgs + @(
        "--model-type",
        "gradient_boosting",
        "--calibration",
        "sigmoid"
    )
    Invoke-PoetryCommand $trainArgs
}

Write-Host "Training pipeline completed successfully." -ForegroundColor Green
