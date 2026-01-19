cd .. /cpi_app
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip wheel
pip install -r requirements.txt

# run the updater script (logs to ./logs/cpi.log)
./run_cpi.sh

# one-time fetch to populate SQLite (and you can backfill too)
python -m jobs.fetch_all
# optional:
python -m jobs.backfill_cpi --start 2005-01 --end 2025-08 --overwrite
python -m jobs.backfill_wages --start 2005-01 --end 2025-08 --overwrite
