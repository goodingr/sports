# Scraping Blockages Documentation

This document tracks sources that cannot be scraped due to blockages, rate limiting, or other issues.

## Overview

When a data source blocks scraping attempts or returns errors, those issues are logged here for future resolution. This may require:
- Residential proxies
- Different scraping strategies
- Alternative data sources
- API access (if available)

## Blockage Log

### Format

Each blockage entry includes:
- **League**: The sport/league affected
- **Source**: The data source name
- **Handler**: The Python handler function
- **Timestamp**: When the blockage was encountered
- **Error**: The error message or exit code

---

## Notes

- Blockages are automatically logged by `scripts/ingest_hourly_data.ps1`
- Manual entries can be added below
- When a blockage is resolved, mark it as resolved with a timestamp

---

## Manual Entries

### NBA Injuries API - 403 Forbidden
- **League**: NBA
- **Source**: NBA Injuries API (cdn.nba.com)
- **Handler**: `src.data.sources.nba_injuries:ingest`
- **Error**: `403 Client Error: Forbidden for url: https://cdn.nba.com/static/json/liveData/injuries/injuries_00.json`
- **Timestamp**: 2025-11-08
- **Status**: Blocked - API requires authentication or has changed access policy
- **Workaround**: 
  - ESPN roster API doesn't provide injury data
  - Alternative options:
    1. Scrape ESPN injury reports page (https://www.espn.com/nba/injuries)
    2. Use SportsDataIO API (paid service)
    3. Use Sportradar API (paid service)
    4. Use `nbainjuries` Python package (requires Java)
    5. Manual data entry or wait for NBA API access

