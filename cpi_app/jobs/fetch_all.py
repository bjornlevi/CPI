# jobs/fetch_all.py

# allow running this script directly: python jobs/fetch_all.py
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timezone
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..models import (
    SessionLocal, Base, engine,
    CPIActual, ForecastRun, ForecastPoint,
    WageActual, WageForecastRun, WageForecastPoint,
)

from ..pipelines.cpi import (
    fetch_cpi_data,            # returns CPI source object (Hagstofan-backed)
    parse_data as parse_cpi,   # -> DataFrame: ['date', 'CPI', 'Monthly Change']
    compute_trend as cpi_trend # -> (model, [(date, yhat), ...])
)

from ..pipelines.wages import (
    fetch_wage_series,         # -> pandas Series (DatetimeIndex) for a category
    compute_forecast as wages_forecast
)

# ---------- CPI helpers ----------

def upsert_cpi(s: Session, df: pd.DataFrame) -> None:
    for _, r in df.iterrows():
        obj = s.query(CPIActual).filter_by(date=r["date"].date()).one_or_none()
        monthly_change = None if pd.isna(r.get("Monthly Change")) else float(r.get("Monthly Change"))
        if obj:
            obj.cpi = float(r["CPI"])
            obj.monthly_change = monthly_change
        else:
            s.add(CPIActual(
                date=r["date"].date(),
                cpi=float(r["CPI"]),
                monthly_change=monthly_change,
            ))

def save_cpi_forecast(s: Session, df24: pd.DataFrame, months: int = 6) -> None:
    run = ForecastRun(months_predict=months, notes="linear_reg_24m")
    s.add(run); s.flush()
    futures = cpi_trend(df24, months_predict=months)[1]
    for d, yhat in futures:
        s.add(ForecastPoint(run_id=run.id, date=d.date(), predicted_cpi=float(yhat)))

# ---------- Wages helpers (TOTAL) ----------

def make_wage_df_for_category(category: str = "TOTAL") -> pd.DataFrame:
    """
    Returns a tidy DataFrame with columns: ['date','category','value'] for one category.
    If the series is empty (category not present), returns empty DataFrame.
    """
    s = fetch_wage_series(category)  # pandas Series with DatetimeIndex
    if s.empty:
        return pd.DataFrame(columns=["date","category","value"])
    df = s.reset_index()
    df.columns = ["date", "value"]
    df["category"] = category
    return df[["date", "category", "value"]]

def upsert_wages(s: Session, df: pd.DataFrame) -> None:
    # expects columns: date (Timestamp), category (str), value (float)
    for d, cat, val in df.itertuples(index=False):
        d = d.date() if hasattr(d, "date") else d
        obj = s.query(WageActual).filter_by(date=d, category=cat).one_or_none()
        if obj:
            obj.index_value = float(val)
        else:
            s.add(WageActual(date=d, category=cat, index_value=float(val)))

def save_wage_forecast(s: Session, df: pd.DataFrame, months: int = 12) -> None:
    """
    For each category present in df, fit a 24-month linear model (anchored) and store 12 future points.
    """
    if df.empty:
        return
    run = WageForecastRun(months_predict=months, notes="linear_reg_24m")
    s.add(run); s.flush()
    for cat, sub in df.groupby("category"):
        ser = sub.set_index("date")["value"].astype(float)
        fut = wages_forecast(ser, months=months)  # -> [(Timestamp, yhat), ...]
        for d, yhat in fut:
            s.add(WageForecastPoint(
                run_id=run.id,
                date=d.date(),
                category=cat,
                predicted_index=float(yhat),
            ))

# ---------- main ----------

def main():
    # create tables if needed
    Base.metadata.create_all(engine)

    s = SessionLocal()
    try:
        # --- CPI ---
        cpi_src = fetch_cpi_data()
        cpi_df  = parse_cpi(cpi_src)
        upsert_cpi(s, cpi_df)
        save_cpi_forecast(s, cpi_df.tail(24).reset_index(drop=True), months=6)

        # --- Wages (multiple categories) ---
        cats = ["TOTAL", "ALM"]  # add "OPI", "OPI_R", "OPI_L" if you want
        frames = [make_wage_df_for_category(c) for c in cats]
        w_df = pd.concat([f for f in frames if not f.empty], ignore_index=True)

        upsert_wages(s, w_df)
        save_wage_forecast(s, w_df, months=12)

        s.commit()
        print("✅ Stored CPI + wages (TOTAL) + forecasts")

    except Exception:
        s.rollback()
        raise
    finally:
        s.close()

if __name__ == "__main__":
    main()
