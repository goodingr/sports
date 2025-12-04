# Implementation Summary: Alternative NBA Injuries & Feature Importance

## 1. Alternative NBA Injury Source

### Created: `src/data/sources/nba_injuries_espn.py`

**Status**: ⚠️ PARTIAL IMPLEMENTATION

**Findings**:
- ESPN roster API (`/teams/{id}/roster`) does NOT include injury data
- Athlete objects contain basic info (name, position, stats) but no injury status
- Need alternative approach

**Alternative Options Identified**:
1. **Scrape ESPN Injury Reports Page**: `https://www.espn.com/nba/injuries`
   - Requires web scraping with BeautifulSoup/Selenium
   - May be rate-limited or blocked
   - Most accessible free option

2. **Paid APIs**:
   - SportsDataIO: Comprehensive NBA data including injuries
   - Sportradar: Official NBA data provider
   - STATS API: Historical and real-time data

3. **Python Package**: `nbainjuries` (PyPI)
   - Requires Java runtime
   - May have limitations

**Recommendation**: Implement scraping from ESPN injury reports page as next step.

### Updated: `src/models/feature_loader.py`
- Now checks `injuries_espn` source first for NBA
- Falls back to regular injuries source if ESPN source is empty

### Updated: `config/sources.yml`
- Added `nba_injuries_espn` source (disabled original `nba_injuries` due to 403)
- Documented blockage in notes

## 2. Feature Importance Analysis

### Created: `src/models/feature_importance.py`

**Analysis Method**: Missing Feature Impact Analysis
- Tests how much predictions change when each feature is set to 0/NaN
- More direct than permutation importance for identifying critical missing features

### Results for NBA Model

**Critical Features (High Impact When Missing)**:
1. `moneyline`: 16.2% mean impact (79.2% of predictions change significantly)
2. `implied_prob`: 6.9% mean impact (37.5% of predictions change significantly)
3. `is_home`: 0.6% mean impact (4.2% of predictions change significantly)

**Zero Impact Features**:
- All ESPN odds features (open/close)
- `spread_line` and `total_line`

**Key Finding**: 
The NBA model was trained with only 5 basic features:
- `is_home`, `moneyline`, `implied_prob`, `spread_line`, `total_line`

**This explains the large edges** - the model lacks advanced features (rolling metrics, team efficiency, injuries) that would improve predictions.

### Recommendations

1. **Retrain NBA Model with Advanced Features**:
   ```bash
   poetry run python -m src.features.moneyline_dataset --league NBA --seasons 2016 2017 2018 2019 2020 2021 2022 2023 2024
   poetry run python -m src.models.train --league NBA --seasons 2016 2017 2018 2019 2020 2021 2022 2023 2024 --model-type gradient_boosting --calibration sigmoid
   ```

2. **Priority Features to Ensure**:
   - ✅ `moneyline` (CRITICAL - always available from odds API)
   - ✅ `implied_prob` (CRITICAL - calculated from moneyline)
   - ✅ Rolling metrics (AVAILABLE - 4,920 rows ingested)
   - ⚠️ Injuries (NEEDS ALTERNATIVE SOURCE)

3. **Next Steps for Injuries**:
   - Implement ESPN injury reports page scraping
   - Or integrate paid API (SportsDataIO/Sportradar)
   - Or use `nbainjuries` package if Java is available

## Files Created/Modified

### New Files:
- `src/data/sources/nba_injuries_espn.py` - ESPN-based injury source (placeholder)
- `src/models/feature_importance.py` - Feature importance analysis tool
- `reports/feature_importance_nba.txt` - NBA feature importance report
- `FEATURE_IMPORTANCE_SUMMARY.md` - Analysis summary
- `IMPLEMENTATION_SUMMARY.md` - This file

### Modified Files:
- `src/models/feature_loader.py` - Added ESPN injuries fallback
- `config/sources.yml` - Added ESPN injuries source, disabled blocked source
- `docs/scraping_blockages.md` - Documented NBA injuries blockage

## Usage

### Run Feature Importance Analysis:
```bash
poetry run python -m src.models.feature_importance --league NBA --output reports/feature_importance_nba.txt
```

### Test ESPN Injuries (when implemented):
```bash
poetry run python -c "from src.data.sources.nba_injuries_espn import ingest; ingest()"
```

## Conclusion

1. ✅ Feature importance analysis implemented and working
2. ⚠️ ESPN injuries source created but needs scraping implementation
3. ✅ Identified that NBA model needs retraining with advanced features
4. ✅ Rolling metrics available and working
5. ⚠️ Injuries still need alternative source (scraping or paid API)

