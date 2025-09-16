# fetch_and_store.py
import os
import pandas as pd
from datetime import datetime
from models import SessionLocal, init_db, CPIActual, ForecastRun, ForecastPoint
from cpi_pipeline import fetch_cpi_data, parse_data, compute_trend

def upsert_actuals(session, df):
    for _, row in df.iterrows():
        obj = session.query(CPIActual).filter_by(date=row["date"]).one_or_none()
        if obj:
            obj.cpi = float(row["CPI"])
            obj.monthly_change = None if pd.isna(row.get("Monthly Change")) else float(row.get("Monthly Change"))
        else:
            session.add(CPIActual(
                date=row["date"],
                cpi=float(row["CPI"]),
                monthly_change=None if pd.isna(row.get("Monthly Change")) else float(row.get("Monthly Change")),
            ))

def save_forecast(session, df, months_predict=6, notes=None):
    run = ForecastRun(months_predict=months_predict, notes=notes or "linear_reg")
    session.add(run)
    session.flush()  # get run.id
    futures = compute_trend(df, months_predict=months_predict)
    for d, val in futures:
        session.add(ForecastPoint(run_id=run.id, date=d, predicted_cpi=float(val)))
    return run.id

def main():
    os.makedirs("data", exist_ok=True)
    init_db()

    print(f"[{datetime.utcnow().isoformat()}] Fetching CPI…")
    js = fetch_cpi_data()
    df = parse_data(js)              # columns: date, CPI, Monthly Change

    session = SessionLocal()
    try:
        upsert_actuals(session, df)
        # use last 24 months to fit
        tail = df.tail(24).reset_index(drop=True)
        run_id = save_forecast(session, tail, months_predict=6)
        session.commit()
        print(f"Saved {len(df)} actuals. Forecast run_id={run_id}.")
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
