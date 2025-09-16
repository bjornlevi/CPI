# allow running this script directly: python jobs/fetch_all.py
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# jobs/fetch_all.py (and backfills)
from ..models import (SessionLocal, Base, engine,
                      CPIActual, ForecastRun, ForecastPoint,
                      WageActual, WageForecastRun, WageForecastPoint)
from ..pipelines.cpi import fetch_cpi_data, parse_data as parse_cpi, compute_trend as cpi_trend
from ..pipelines.wages import fetch_wage_data, parse_data as parse_wages, compute_forecast as wages_forecast

import pandas as pd

def upsert_cpi(s, df):
    for _, r in df.iterrows():
        obj = s.query(CPIActual).filter_by(date=r["date"]).one_or_none()
        if obj:
            obj.cpi = float(r["CPI"])
            obj.monthly_change = None if pd.isna(r.get("Monthly Change")) else float(r.get("Monthly Change"))
        else:
            s.add(CPIActual(
                date=r["date"],
                cpi=float(r["CPI"]),
                monthly_change=None if pd.isna(r.get("Monthly Change")) else float(r.get("Monthly Change")),
            ))

def save_cpi_forecast(s, df24, months=6):
    run = ForecastRun(months_predict=months, notes="linear_reg_24m")
    s.add(run); s.flush()
    futures = cpi_trend(df24, months_predict=months)[1]
    for d, yhat in futures:
        s.add(ForecastPoint(run_id=run.id, date=d, predicted_cpi=float(yhat)))

def upsert_wages(s, df):
    for (d, cat, val) in df.itertuples(index=False):
        obj = s.query(WageActual).filter_by(date=d, category=cat).one_or_none()
        if obj: obj.index_value = float(val)
        else:   s.add(WageActual(date=d, category=cat, index_value=float(val)))

def save_wage_forecast(s, df, months=12):
    run = WageForecastRun(months_predict=months, notes="linear_reg_24m")
    s.add(run); s.flush()
    for cat, sub in df.groupby("category"):
        ser = sub.set_index("date")["value"]
        for d, yhat in wages_forecast(ser, months=months):
            s.add(WageForecastPoint(run_id=run.id, date=d, category=cat, predicted_index=float(yhat)))

def main():
    # create tables if needed
    Base.metadata.create_all(engine)
    s = SessionLocal()
    try:
        # CPI
        cpi_js = fetch_cpi_data()
        cpi_df = parse_cpi(cpi_js)
        upsert_cpi(s, cpi_df)
        save_cpi_forecast(s, cpi_df.tail(24).reset_index(drop=True), months=6)
        # Wages
        w_js = fetch_wage_data()
        w_df = parse_wages(w_js)
        upsert_wages(s, w_df)
        save_wage_forecast(s, w_df, months=12)
        s.commit()
        print("✅ Stored CPI + wages + forecasts")
    except Exception:
        s.rollback(); raise
    finally:
        s.close()

if __name__ == "__main__":
    main()
