# Test the new --model-type functionality in forward_test.py
# This script tests predictions with different model types

Write-Host "========================================="
Write-Host "Testing Model Type Functionality"
Write-Host "========================================="
Write-Host ""

# Test with NBA (assuming models exist)
$league = "NBA"

Write-Host "Testing $league predictions with different model types..."
Write-Host ""

# Test Ensemble (default)
Write-Host "[1/3] Testing Ensemble model..."
try {
    & poetry run python -m src.models.forward_test predict --league $league --model-type ensemble
    Write-Host "  Success: Ensemble predictions generated" -ForegroundColor Green
} catch {
    Write-Host "  Failed: Ensemble predictions - $_" -ForegroundColor Red
}
Write-Host ""

# Test Random Forest (with tuned params)
Write-Host "[2/3] Testing Random Forest model (tuned)..."
try {
    & poetry run python -m src.models.forward_test predict --league $league --model-type random_forest
    Write-Host "  Success: Random Forest predictions generated" -ForegroundColor Green
} catch {
    Write-Host "  Failed: Random Forest predictions - $_" -ForegroundColor Red
}
Write-Host ""

# Test Gradient Boosting
Write-Host "[3/3] Testing Gradient Boosting model..."
try {
    & poetry run python -m src.models.forward_test predict --league $league --model-type gradient_boosting
    Write-Host "  Success: Gradient Boosting predictions generated" -ForegroundColor Green
} catch {
    Write-Host "  Failed: Gradient Boosting predictions - $_" -ForegroundColor Red
}
Write-Host ""

Write-Host "========================================="
Write-Host "Test Complete"
Write-Host "========================================="
Write-Host ""
Write-Host "Check predictions in: data/forward_test/predictions_master.parquet"
