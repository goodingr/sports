# Tune Random Forest models for all leagues
# This script runs hyperparameter tuning using Optuna

param(
    [int]$NTrials = 50
)

$ErrorActionPreference = "Continue"

$leagues = @("NFL", "CFB", "NCAAB", "NHL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")

Write-Host "========================================="
Write-Host "Starting hyperparameter tuning for all leagues"
Write-Host "Trials per league: $NTrials"
Write-Host "========================================="
Write-Host ""

foreach ($league in $leagues) {
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Tuning Random Forest for $league..."
    
    try {
        & poetry run python -m src.models.tune `
            --league $league `
            --model-type random_forest `
            --n-trials $NTrials `
            --seasons 2021 2022 2023 2024 2025
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Success: $league tuning completed" -ForegroundColor Green
        } else {
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Failed: $league tuning" -ForegroundColor Red
        }
    } catch {
        Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Error: $league tuning failed: $_" -ForegroundColor Red
    }
    
    Write-Host ""
}

Write-Host "========================================="
Write-Host "Tuning complete for all leagues"
Write-Host "========================================="
Write-Host ""
Write-Host "Tuned parameters saved to: config/tuned_params/"
Write-Host "Visualizations saved to: reports/tuning/"
