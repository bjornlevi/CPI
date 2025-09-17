# cpi_app/jobs/backfill_cpi.py
from __future__ import annotations

import argparse
from datetime import datetime, timezone, date
from typing import Iterator

from sqlalchemy.orm import Session
from sqlalchemy import select, delete

from ..models import (
    Base, engine, SessionLocal,
    CPIActual, ForecastRun, ForecastPoint,
)
from ..pipelines.cpi import fetch_cpi_data, parse_data as parse_cpi, compute_trend

def parse_ym(s: str) -> date:
    return datetime.strptime(s, "%Y-%m").date()

def month_iter(start: date, end: date) -> Iterator[date]:
    y, m = start.year, start.month
    while True:
        d = date(y, m, 1)
        if d > end:
            break
        yield d
        # next month
        m += 1
        if m == 13:
            m = 1
            y += 1

def upsert_cpi_actual(s: Session, d: date, cpi_value: float, monthly_change: float | None) -> None:
    obj = s.query(CPIActual).filter_by(date=d).one_or_none()
    if obj:
        obj.cpi = float(cpi_value)
        obj.monthly_change = monthly_change if monthly_change is None else float(monthly_change)
    else:
        s.add(CPIActual(date=d, cpi=float(cpi_value),
                        monthly_change=monthly_change if monthly_change is None else float(monthly_change)))

def delete_backfill_run(s: Session, anchor_ym: str) -> None:
    # Any run created by this script uses notes=f"backfill:{anchor_ym}:..."
    runs = s.scalars(select(ForecastRun).where(ForecastRun.notes.like(f"backfill:{anchor_ym}:%"))).all()
    for r in runs:
        s.execute(delete(ForecastPoint).where(ForecastPoint.run_id == r.id))
        s.delete(r)

def main():
    ap = argparse.ArgumentParser(description="Backfill CPI actuals + as-of forecasts by month.")
    ap.add_argument("--start", required=True, help="YYYY-MM (inclusive)")
    ap.add_argument("--end",   required=True, help="YYYY-MM (inclusive)")
    ap.add_argument("--months", type=int, default=6, help="Forecast horizon (default: 6)")
    ap.add_argument("--window", type=int, default=24, help="Regression window in months (default: 24)")
    ap.add_argument("--overwrite", action="store_true", help="Delete any existing backfill run at same anchor month")
    args = ap.parse_args()

    start = parse_ym(args.start)
    end   = parse_ym(args.end)

    # Make sure tables exist
    Base.metadata.create_all(bind=engine)

    # Load full CPI data once
    src = fetch_cpi_data()
    df_all = parse_cpi(src)  # columns: date (datetime), CPI, Monthly Change
    if df_all.empty:
        print("No CPI data available from source.")
        return

    # Normalize dates to date()
    df_all["date"] = df_all["date"].dt.date

    count_runs = 0
    with SessionLocal() as s:
        for anchor in month_iter(start, end):
            # find the row for this month
            row = df_all.loc[df_all["date"] == anchor]
            if row.empty:
                # skip months not in source
                continue

            cpi_val = float(row["CPI"].iloc[0])
            mc = row["Monthly Change"].iloc[0]
            monthly_change = None if (mc != mc) else float(mc)  # NaN-safe

            # upsert actual for anchor month
            upsert_cpi_actual(s, anchor, cpi_val, monthly_change)

            # windowed training data: <= anchor, last N
            df_hist = df_all[df_all["date"] <= anchor].tail(args.window).reset_index(drop=True)
            if len(df_hist) < 2:
                # need at least 2 points for regression
                continue

            # forecast and store as a run
            anchor_ym = f"{anchor.year:04d}-{anchor.month:02d}"
            if args.overwrite:
                delete_backfill_run(s, anchor_ym)

            run = ForecastRun(
                months_predict=args.months,
                notes=f"backfill:{anchor_ym}:linear_reg_{args.window}m"
            )
            s.add(run); s.flush()

            _model, future = compute_trend(df_hist, months_predict=args.months)
            for d, yhat in future:
                s.add(ForecastPoint(
                    run_id=run.id,
                    date=d.date(),
                    predicted_cpi=float(yhat),
                ))

            count_runs += 1
            # commit per anchor to avoid big transactions
            s.commit()
            print(f"✓ {anchor_ym}: stored actual + forecast ({args.months}m)")

    print(f"Done. Created {count_runs} forecast runs.")

if __name__ == "__main__":
    main()
