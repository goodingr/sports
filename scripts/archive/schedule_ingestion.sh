#!/bin/bash
# Linux/macOS cron example for automated data ingestion
# Add entries to crontab with: crontab -e

# Example crontab entries (add to ~/.crontab or system crontab):
#
# Hourly odds ingestion (NFL and NBA)
# 0 * * * * cd /path/to/sports && poetry run python -m src.data.ingest_sources --league nfl --league nba --source espn_odds_nfl --source espn_odds_nba
#
# Daily full ingestion (all enabled sources)
# 0 2 * * * cd /path/to/sports && poetry run python -m src.data.ingest_sources
#
# Weekly team metrics (Sunday at 3 AM)
# 0 3 * * 0 cd /path/to/sports && poetry run python -m src.data.ingest_sources --source nfl_team_metrics --source nba_team_metrics
#
# Health check and alert (every 6 hours)
# 0 */6 * * * cd /path/to/sports && poetry run python -m src.data.monitor_sources check --hours 24 --min-success-rate 80 || echo "Source health check failed" | mail -s "Data Ingestion Alert" admin@example.com

echo "Cron examples for data ingestion:"
echo ""
echo "Hourly odds:"
echo "  0 * * * * cd $(pwd) && poetry run python -m src.data.ingest_sources --league nfl --league nba --source espn_odds_nfl --source espn_odds_nba"
echo ""
echo "Daily full:"
echo "  0 2 * * * cd $(pwd) && poetry run python -m src.data.ingest_sources"
echo ""
echo "Weekly metrics:"
echo "  0 3 * * 0 cd $(pwd) && poetry run python -m src.data.ingest_sources --source nfl_team_metrics --source nba_team_metrics"
echo ""
echo "Add to crontab with: crontab -e"

