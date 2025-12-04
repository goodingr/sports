# Retrain Random Forest models with tuned hyperparameters for all leagues
# This script trains Random Forest models which will automatically use the tuned parameters

param(
    [string[]]$Leagues = @("NFL", "NBA", "CFB", "NCAAB", "NHL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
)

$ErrorActionPreference = "Continue"

Write-Host "========================================="
Write-Host "Retraining Random Forest models with tuned hyperparameters"
Write-Host "Leagues: $($Leagues -join ', ')"
Write-Host "========================================="
Write-Host ""

foreach ($league in $Leagues) {
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Training Random Forest for $league..."
    
    try {
        & poetry run python -m src.models.train `
            --league $league `
            --model-type random_forest `
            --seasons 2021 2022 2023 2024 2025 `
            --calibration sigmoid
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Success: $league Random Forest trained" -ForegroundColor Green
        } else {
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Failed: $league training" -ForegroundColor Red
        }
    } catch {
        Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Error: $league training failed: $_" -ForegroundColor Red
    }
    
    Write-Host ""
}

Write-Host "========================================="
Write-Host "Training complete"
Write-Host "========================================="
Write-Host ""
Write-Host "Models saved to: models/{league}_random_forest_calibrated_moneyline.pkl"
