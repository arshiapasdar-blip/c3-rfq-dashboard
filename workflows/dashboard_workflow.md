# Dashboard Workflow

## Objective
Run the C3 Analytics Dashboard — a two-section Streamlit web app showing Sales and Sourcing KPIs from the C3_Web SQL Server database.

## Prerequisites
- macOS: `brew install freetds` (required by pymssql for SQL Server connectivity)
- VPN or local network access to `j2azdb01.j2sourcing.local`
- Python 3.10+

## Setup (First Time Only)

```bash
cd "C3 test Data Dashboard"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If pymssql fails to install on Apple Silicon:
```bash
pip install pymssql --no-binary :all:
```

## Running the Dashboard

```bash
source .venv/bin/activate
streamlit run dashboard.py
```

Opens at: http://localhost:8501

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| "Cannot connect to database" | Not on VPN/network | Connect to J2 VPN and retry |
| Empty charts | Date range too narrow / no data | Widen the date range in sidebar |
| `ModuleNotFoundError: pymssql` | Not installed | Run `pip install -r requirements.txt` |
| `brew: command not found` | Homebrew not installed | Install from https://brew.sh |
| Slow queries | Large date range | Narrow date range; queries cache for 5 min |

## Adding New Metrics

1. Add a query function to `tools/sales_data.py` or `tools/sourcing_data.py`
2. Add a cached wrapper in `dashboard.py` following the `@st.cache_data(ttl=300)` pattern
3. Add the chart/table to the appropriate tab section in `dashboard.py`

## Updating the MPN Blocklist

Dirty test MPNs are listed in `tools/db.py` as `DIRTY_MPNS`. Add new entries as needed:
```python
DIRTY_MPNS = {
    'test', 'test1', ...  # add new entries here
}
```

## Architecture

```
dashboard.py           ← Streamlit UI, caching, layout
tools/
  db.py               ← DB connection, run_query(), MPN blocklist
  sales_data.py       ← All Sales tab query functions
  sourcing_data.py    ← All Sourcing tab query functions
.env                  ← DB credentials (never commit this)
```

## Data Notes

- All CustomerRfqs queries filter `Deleted=0` — this is critical
- Date range defaults to 2024-01-01 → today; pre-2024 data contains synthetic test records
- Currency values are shown in native currency — no FX conversion applied
- SourcingStatus codes: 0=Not Started, 30=Sourced, 40=Quoted, 50=Won, 60=Closed
