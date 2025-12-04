# Script to generate predictions for all model types across all leagues
# This populates the new model-specific prediction directories

$Leagues = @("NBA", "NCAAB", "NFL", "NHL", "CFB", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
$ModelTypes = @("ensemble", "random_forest", "gradient_boosting")

Write-Host "========================================="
Write-Host "Generating Predictions for All Models"
Write-Host "========================================="
Write-Host ""

foreach ($ModelType in $ModelTypes) {
    Write-Host "Processing Model Type: $ModelType" -ForegroundColor Cyan
    Write-Host "-----------------------------------------"
    
    foreach ($League in $Leagues) {
        Write-Host "  Generating predictions for $League..." -NoNewline
        
        try {
            # Run forward_test.py for specific league and model type
            $Output = & poetry run python -m src.models.forward_test predict --league $League --model-type $ModelType 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host " Done" -ForegroundColor Green
            } else {
                Write-Host " Failed" -ForegroundColor Red
                Write-Host "    Error: $Output" -ForegroundColor Gray
            }
        } catch {
            Write-Host " Error: $_" -ForegroundColor Red
        }
    }
    Write-Host ""
}

Write-Host "========================================="
Write-Host "Generation Complete"
Write-Host "========================================="
