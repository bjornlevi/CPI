# backfill_cpi.py — create historical CPI forecast runs
# Usage examples:
#   python -m jobs.backfill_cpi --months-predict 6 --window 24
#   python -m jobs.backfill_cpi --start 2005-01 --end 2020-12 --overwrite
import os, sys, argparse, math
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import select, func
from sqlalchemy.orm import Session

# allow "from models import ..." when run as a script
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from models import engine, CPIActual, ForecastRun, ForecastPoint
from pipelines.cpi import compute_trend as cpi_trend

def normalize_trend_output(res):
    """Accept (model, pairs) or (dates, preds) or (model, dates, preds) -> list[(date, value)]."""
    if not isinstance(res, tuple):
        raise TypeError(f"Unexpected compute_trend() output: {type(res)}")
    if len(res) == 2 and hasattr(res[0], "predict"):
        return list(res[1])  # (model, pairs)
    if len(res) == 2:
        dates, preds = res
        return list(zip(dates, preds))
    if len(res) == 3:
        _, dates, preds = res
        return list(zip(dates, preds))
    raise TypeError(f"Unexpected compute_trend() tuple len={len(res)}")

def daterange_months(start: pd.Timestamp, end: pd.Timestamp):
    m = start.to_period("M")
    e = end.to_period("M")
    while m <= e:
        yield m.to_timestamp("M")  # end-of-month
        m += 1

def backfill(months_predict=6, window=24, start=None, end=None, overwrite=False):
    with Session(engine) as s:
        actuals = s.scalars(select(CPIActual).order_by(CPIActual.date)).all()
        if not actuals:
            raise RuntimeError("No CPI actuals found. Run your fetch job first.")
        df = pd.DataFrame({
            "date": [pd.Timestamp(a.date) for a in actuals],
            "CPI":  [a.cpi for a in actuals],
        }).sort_values("date").reset_index(drop=True)

        min_date = df["date"].min()
        max_date = df["date"].max()

        start_ts = pd.Timestamp(start) if start else min_date + pd.DateOffset(months=window-1)
        end_ts   = pd.Timestamp(end)   if end   else max_date

        # ensure month-end timestamps for consistency
        start_ts = start_ts.to_period("M").to_timestamp("M")
        end_ts   = end_ts.to_period("M").to_timestamp("M")

        runs_created = 0
        for cut in daterange_months(start_ts, end_ts):
            # historical "now" is the end of 'cut' month; include data up to that date
            hist_df = df[df["date"] <= cut].copy()
            if len(hist_df) < max(2, window):
                continue
            if window and len(hist_df) > window:
                hist_df = hist_df.tail(window).reset_index(drop=True)

            # idempotency per month: either skip or refresh
            if not overwrite:
                existing = s.scalars(
                    select(ForecastRun).where(func.strftime("%Y-%m", ForecastRun.created_at) == cut.strftime("%Y-%m"))
                    .order_by(ForecastRun.created_at.desc())
                ).first()
                if existing:
                    continue

            # create or reuse run for this month (overwrite mode deletes points)
            run = s.scalars(
                select(ForecastRun).where(func.strftime("%Y-%m", ForecastRun.created_at) == cut.strftime("%Y-%m"))
            ).first()
            if run and overwrite:
                s.query(ForecastPoint).filter(ForecastPoint.run_id == run.id).delete()
            if not run:
                run = ForecastRun(
                    created_at=datetime(cut.year, cut.month, 1, 12, 0, 0, tzinfo=timezone.utc),
                    months_predict=months_predict,
                    notes=f"backfill_window{window}"
                )
                s.add(run)
                s.flush()

            # compute forecast using only data up to 'cut'
            # build df24-like input that your compute_trend expects: columns ["date","CPI"]
            pairs = normalize_trend_output(cpi_trend(hist_df.reset_index(drop=True), months_predict=months_predict))

            # insert forecast points
            for d, yhat in pairs:
                s.add(ForecastPoint(run_id=run.id, date=pd.Timestamp(d).date(), predicted_cpi=float(yhat)))

            runs_created += 1

        s.commit()
        return runs_created

def main():
    ap = argparse.ArgumentParser(description="Backfill CPI forecast runs month-by-month.")
    ap.add_argument("--months-predict", type=int, default=6, help="months to forecast each run (default 6)")
    ap.add_argument("--window", type=int, default=24, help="training window in months (default 24)")
    ap.add_argument("--start", type=str, help="YYYY-MM (first month to simulate)")
    ap.add_argument("--end", type=str, help="YYYY-MM (last month to simulate)")
    ap.add_argument("--overwrite", action="store_true", help="refresh forecasts for months that already have a run")
    args = ap.parse_args()
    n = backfill(args.months_predict, args.window, args.start, args.end, args.overwrite)
    print(f"✅ Backfill complete. Runs created/refreshed: {n}")

if __name__ == "__main__":
    main()
