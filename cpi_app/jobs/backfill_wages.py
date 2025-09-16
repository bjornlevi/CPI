# jobs/backfill_wages.py — create historical Wage forecast runs (per month, all categories)
# Usage examples:
#   python -m jobs.backfill_wages --months-predict 12 --window 24
#   python -m jobs.backfill_wages --start 2005-01 --end 2025-08 --overwrite
import os, sys, argparse
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import select, func
from sqlalchemy.orm import Session

# allow "from models import ..." when run as a module or script
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from models import (
    engine, Base,
    WageActual, WageForecastRun, WageForecastPoint
)
from pipelines.wages import compute_forecast as wages_forecast

def log(*a): print(*a, flush=True)

def daterange_months(start: pd.Timestamp, end: pd.Timestamp):
    m = start.to_period("M"); e = end.to_period("M")
    while m <= e:
        yield m.to_timestamp("M")
        m += 1

def backfill(months_predict=12, window=24, start=None, end=None, overwrite=False, only_cats=None):
    """
    months_predict: forecast horizon per historical month
    window: how many most-recent months to fit on (per category)
    only_cats: optional list of categories to restrict (e.g., ["TOTAL","ALM"])
    """
    Base.metadata.create_all(engine)  # ensure tables exist

    with Session(engine) as s:
        # Load all wage actuals
        acts = s.scalars(select(WageActual).order_by(WageActual.category, WageActual.date)).all()
        if not acts:
            raise RuntimeError("No Wage actuals found. Run your fetch job first.")

        # Build a tidy frame: date, category, value
        df = pd.DataFrame({
            "date": [pd.Timestamp(a.date) for a in acts],
            "category": [a.category for a in acts],
            "value": [a.index_value for a in acts],
        }).sort_values(["category","date"]).reset_index(drop=True)

        # Category filter if requested
        all_cats = sorted(df["category"].unique().tolist())
        cats = [c for c in (only_cats or all_cats) if c in all_cats]
        if not cats:
            raise RuntimeError("No categories match the available data.")

        # Date range for backfill
        min_date = df["date"].min()
        max_date = df["date"].max()
        start_ts = (pd.Timestamp(start) if start else min_date + pd.DateOffset(months=window-1)).to_period("M").to_timestamp("M")
        end_ts   = (pd.Timestamp(end)   if end   else max_date).to_period("M").to_timestamp("M")

        log(f"Wage backfill | cats={cats} | range {start_ts.date()} → {end_ts.date()} | window={window} | horizon={months_predict} | overwrite={overwrite}")

        runs_created = 0
        for i, cut in enumerate(daterange_months(start_ts, end_ts), 1):
            # Per-month run (one run for all categories)
            existing = s.scalars(
                select(WageForecastRun).where(func.strftime("%Y-%m", WageForecastRun.created_at) == cut.strftime("%Y-%m"))
                .order_by(WageForecastRun.created_at.desc())
            ).first()

            if existing and not overwrite:
                log(f"[{i}] {cut.date()} exists → skip")
                continue

            if existing and overwrite:
                s.query(WageForecastPoint).filter(WageForecastPoint.run_id == existing.id).delete()
                run = existing
                log(f"[{i}] {cut.date()} overwrite → refresh points")
            else:
                run = WageForecastRun(
                    created_at=datetime(cut.year, cut.month, 1, 12, 0, 0, tzinfo=timezone.utc),
                    months_predict=months_predict,
                    notes=f"wage-backfill_window{window}"
                )
                s.add(run); s.flush()
                log(f"[{i}] {cut.date()} new run id={run.id}")

            # For each category, fit on data available up to 'cut'
            for cat in cats:
                sub = df[(df["category"] == cat) & (df["date"] <= cut)].copy()
                if len(sub) < max(2, window):
                    continue
                if len(sub) > window:
                    sub = sub.tail(window).reset_index(drop=True)

                # Series indexed by date for compute_forecast
                ser = sub.set_index("date")["value"]

                # wages_forecast returns list[(date, value)] in our pipeline.
                # If your version differs, adapt here.
                pairs = wages_forecast(ser, months=months_predict)
                for d, yhat in pairs:
                    s.add(WageForecastPoint(
                        run_id=run.id,
                        date=pd.Timestamp(d).date(),
                        category=cat,
                        predicted_index=float(yhat)
                    ))

            runs_created += 1

        s.commit()
        log(f"✅ Wage backfill complete. Runs created/refreshed: {runs_created}")
        return runs_created

def main():
    ap = argparse.ArgumentParser(description="Backfill Wage forecast runs month-by-month.")
    ap.add_argument("--months-predict", type=int, default=12, help="months to forecast each run (default 12)")
    ap.add_argument("--window", type=int, default=24, help="training window in months (default 24)")
    ap.add_argument("--start", type=str, help="YYYY-MM (first month to simulate)")
    ap.add_argument("--end", type=str, help="YYYY-MM (last month to simulate)")
    ap.add_argument("--overwrite", action="store_true", help="refresh forecasts for months that already have a run")
    ap.add_argument("--cats", nargs="*", help="Optional list of categories to include (e.g. TOTAL ALM)")
    args = ap.parse_args()
    backfill(args.months_predict, args.window, args.start, args.end, args.overwrite, args.cats)

if __name__ == "__main__":
    main()
