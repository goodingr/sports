# Daily Forward Testing Workflow Script
# Run this before games to make predictions, and after games to update results

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("predict", "update", "report", "full")]
    [string]$Action = "full"
)

Write-Host "=" * 60
Write-Host "Forward Testing Workflow"
Write-Host "=" * 60
Write-Host ""

if ($Action -eq "predict" -or $Action -eq "full") {
    Write-Host "[1/3] Making predictions on live games..."
    poetry run python -m src.models.forward_test predict --dotenv .env
    Write-Host ""
}

if ($Action -eq "update" -or $Action -eq "full") {
    Write-Host "[2/3] Updating predictions with game results..."
    poetry run python -m src.models.forward_test update
    Write-Host ""
}

if ($Action -eq "report" -or $Action -eq "full") {
    Write-Host "[3/3] Generating performance report..."
    poetry run python -m src.models.forward_test report
    Write-Host ""
}

Write-Host "Done!"


