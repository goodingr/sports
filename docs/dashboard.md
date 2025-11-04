# Forward Testing Dashboard

Interactive monitoring UI built with Dash (Plotly) to visualize forward testing outputs and surface actionable bets in real time.

## Prerequisites

- Install project dependencies via Poetry (`poetry install`). The dashboard relies on:
  - `dash`
  - `dash-bootstrap-components`
  - Plotly (bundled with Dash)
- Forward testing predictions saved under `data/forward_test/predictions_master.parquet`.
  - Populate this file by running the forward testing pipeline:
    - `poetry run python -m src.models.forward_test predict --league <NBA|NFL|CFB>`
    - `poetry run python -m src.models.forward_test update --league <NBA|NFL|CFB>`
  - Scheduled scripts keep the file fresh automatically:
    - NBA/NFL: `scripts/run_forward_test_predict.ps1`, `scripts/run_forward_test_update.ps1`
    - CFB: `scripts/run_forward_test_predict_cfb.ps1`, `scripts/run_forward_test_update_cfb.ps1`

## Running the Dashboard

```bash
poetry run python -m src.dashboard --host 0.0.0.0 --port 8050 --debug
```

Arguments:

- `--host`: Interface to bind (defaults to `0.0.0.0` for LAN access).
- `--port`: Port to serve the UI (default `8050`).
- `--debug`: Enables Dash's reloader and verbose logs for local development.

Once running, open `http://localhost:8050` in a browser.

## Layout Overview

- **Controls** (top):
  - Manual Refresh button reloads `predictions_master.parquet` on demand.
  - Date range picker filters by game commence date.
  - Edge threshold slider (0–20%) filters recommendation logic across widgets.
  - Performance period dropdown adjusts aggregation for the profit-by-period bar chart.
- **Tabs**:
  1. **Overview** – KPI cards + cumulative profit line.
  2. **Performance** – ROI, win rate, and profit by period charts.
  3. **Recent Predictions** – Sortable/filterable table of the latest prediction rows (home/away sides).
  4. **Recommended Bets** – Upcoming bets above the edge threshold with predicted probability, edge, and moneyline.
  5. **Edge Analysis** – ROI by edge bucket chart plus summary table (bets, wins, net profit, ROI).
  6. **Calendar** – Date-organized view of pending recommendations for quick scanning of today's slate.

## Data Logic

- Reads the master predictions snapshot and expands each game into home/away betting rows.
- Summary metrics respect the configured edge threshold and assume a $100 flat stake for ROI calculations.
- ROI/time-series charts only consider settled bets (games with results). Pending bets remain visible in tables but are excluded from ROI.
- Calendar and recommendations reflect only upcoming (result unset) bets above the edge threshold.

## Customisation Tips

- Change default edge threshold by editing `DEFAULT_EDGE_THRESHOLD` in `src/dashboard/data.py`.
- Adjust stake assumptions (`DEFAULT_STAKE`) to align ROI with bankroll strategy.
- To add new charts or metrics, add helper functions to `src/dashboard/data.py` and expose corresponding components in `src/dashboard/components.py`.
- Multi-league filtering is handled via the League dropdown (All/NBA/NFL/CFB); ensure predictions include the `league` column when adding new sports.

## Troubleshooting

- **Blank dashboard**: Ensure `predictions_master.parquet` exists. The header will show `Last updated: —` if the file is missing.
- **Out-of-date metrics**: Click the Manual Refresh button after new predictions or results are written.
- **Port already in use**: Specify another port via `--port` (e.g., `--port 8051`).
- **Dash not installed**: Re-run `poetry install` to sync the new dependencies.

## Next Steps

- Add authentication (Dash Enterprise or reverse-proxy level) before exposing externally.
- Integrate automated refresh (e.g., `dcc.Interval`) for near real-time updates once data is pushed continuously.
- Capture user-selected filters or favorite views and persist them for quick recall.


