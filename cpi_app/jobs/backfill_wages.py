# cpi_app/jobs/backfill_wages.py
from __future__ import annotations

import argparse
from datetime import datetime, date
from typing import Iterator, List

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, delete

from ..models import (
    Base, engine, SessionLocal,
    WageActual, WageForecastRun, WageForecastPoint,
)
from ..pipelines.wages import fetch_wage_series, compute_forecast

def parse_ym(s: str) -> date:
    return datetime.strptime(s, "%Y-%m").date()

def month_iter(start: date, end: date) -> Iterator[date]:
    y, m = start.year, start.month
    while True:
        d = date(y, m, 1)
        if d > end:
            break
        yield d
        m += 1
        if m == 13:
            m = 1
            y += 1

def upsert_wage_actual(s: Session, d: date, category: str, value: float) -> None:
    obj = s.query(WageActual).filter_by(date=d, category=category).one_or_none()
    if obj:
        obj.index_value = float(value)
    else:
        s.add(WageActual(date=d, category=category, index_value=float(value)))

def delete_backfill_run(s: Session, anchor_ym: str, category: str) -> None:
    # runs created here use notes=f"backfill:{category}:{anchor_ym}:..."
    runs = s.scalars(
        select(WageForecastRun).where(WageForecastRun.notes.like(f"backfill:{category}:{anchor_ym}:%"))
    ).all()
    for r in runs:
        s.execute(delete(WageForecastPoint).where(WageForecastPoint.run_id == r.id))
        s.delete(r)

def main():
    ap = argparse.ArgumentParser(description="Backfill wage index actuals + as-of forecasts by month.")
    ap.add_argument("--start", required=True, help="YYYY-MM (inclusive)")
    ap.add_argument("--end",   required=True, help="YYYY-MM (inclusive)")
    ap.add_argument("--categories", default="TOTAL",
                    help="Comma-separated wage categories (default: TOTAL). Example: TOTAL,ALM,OPI,OPI_R,OPI_L")
    ap.add_argument("--months", type=int, default=12, help="Forecast horizon (default: 12)")
    ap.add_argument("--window", type=int, default=24, help="Regression window in months (default: 24)")
    ap.add_argument("--overwrite", action="store_true", help="Delete any existing backfill run at same anchor/category")
    args = ap.parse_args()

    start = parse_ym(args.start)
    end   = parse_ym(args.end)
    cats: List[str] = [c.strip() for c in args.categories.split(",") if c.strip()]

    # Ensure tables
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as s:
        total_runs = 0
        for cat in cats:
            # load full series for this category once
            series = fetch_wage_series(cat)  # pandas Series indexed by Timestamp
            if series.empty:
                print(f"Category {cat}: no data. Skipping.")
                continue
            # normalize index to date()
            series.index = pd.to_datetime(series.index).date

            for anchor in month_iter(start, end):
                if anchor not in series.index:
                    # no actual for this month
                    continue

                # upsert actual
                upsert_wage_actual(s, anchor, cat, float(series.loc[anchor]))

                # windowed history ≤ anchor
                hist = series[series.index <= anchor]
                if len(hist) < 2:
                    continue
                if len(hist) > args.window:
                    hist = hist[-args.window:]

                # overwrite if requested
                anchor_ym = f"{anchor.year:04d}-{anchor.month:02d}"
                if args.overwrite:
                    delete_backfill_run(s, anchor_ym, cat)

                # create run and store forecast
                run = WageForecastRun(
                    months_predict=args.months,
                    notes=f"backfill:{cat}:{anchor_ym}:linear_reg_{args.window}m",
                )
                s.add(run); s.flush()

                fut = compute_forecast(hist, months=args.months, window=len(hist))
                for d, yhat in fut:
                    s.add(WageForecastPoint(
                        run_id=run.id,
                        category=cat,
                        date=d.date(),
                        predicted_index=float(yhat),
                    ))

                s.commit()
                total_runs += 1
                print(f"✓ {cat} {anchor_ym}: stored actual + forecast ({args.months}m)")

        print(f"Done. Created {total_runs} wage forecast runs.")

if __name__ == "__main__":
    main()
