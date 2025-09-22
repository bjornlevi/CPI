# jobs/fetch_all.py

# allow running this script directly: python jobs/fetch_all.py
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timezone, date
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select
from dateutil.relativedelta import relativedelta
from ..models import CPISubMetric

from ..models import (
    SessionLocal, Base, engine,
    CPIActual, ForecastRun, ForecastPoint,
    WageActual, WageForecastRun, WageForecastPoint,
)

from ..models import (
  # existing…
  BCIActual, BCIForecastRun, BCIForecastPoint,
  PPIActual, PPIForecastRun, PPIForecastPoint,
)
from ..pipelines.bci import fetch_bci_series as fetch_bci, compute_forecast as bci_forecast
from ..pipelines.ppi import fetch_ppi_series as fetch_ppi, compute_forecast as ppi_forecast


from ..pipelines.cpi import (
    fetch_cpi_data,            # returns CPI source object (Hagstofan-backed)
    parse_data as parse_cpi,   # -> DataFrame: ['date', 'CPI', 'Monthly Change']
    compute_trend as cpi_trend, # -> (model, [(date, yhat), ...])
    list_isnr, isnr_label, get_isnr_series    
)

from ..pipelines.wages import (
    fetch_wage_series,         # -> pandas Series (DatetimeIndex) for a category
    compute_forecast as wages_forecast
)

# ---------- CPI helpers ----------

# Curated ISNR codes to always surface
CPI_CURATED = [
    "IS011",      # Matur
    "IS041",      # Greidd húsaleiga
    "IS042",      # Reiknuð húsaleiga
    "IS0451",     # Rafmagn
    "IS0455",     # Hiti
    "IS06",       # Heilsa
    "IS0722",     # Bensín og olíur
    "IS111",      # Veitingar
]

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

def _pct(curr, prev):
    if curr is None or prev in (None, 0):
        return None
    return (curr / prev - 1.0) * 100.0

def _yyyymm(dt):
    return dt.strftime("%Y-%m")

def upsert_latest_cpi_sub_metrics(session):
    """
    For the latest CPI month in CPIActual, compute MoM/YoY and deltas vs total CPI
    for all top-level IS codes (IS01.., IS02..) and curated fine-grained codes.
    Upsert into cpi_sub_metrics (one row per code for that month).
    """
    # 1) What is the latest month we have in CPIActual?
    from ..models import CPIActual
    latest = session.query(CPIActual).order_by(CPIActual.date.desc()).first()
    if not latest:
        return
    last_dt = latest.date
    prev_dt = last_dt - relativedelta(months=1)
    prev12_dt = last_dt - relativedelta(months=12)

    # Build quick dict of total CPI values by YYYY-MM for MoM/YoY deltas
    all_actuals = session.query(CPIActual).order_by(CPIActual.date).all()
    total_by_key = {_yyyymm(a.date): a.cpi for a in all_actuals}

    last_key, prev_key, prev12_key = _yyyymm(last_dt), _yyyymm(prev_dt), _yyyymm(prev12_dt)
    total_mom = _pct(total_by_key.get(last_key), total_by_key.get(prev_key))
    total_yoy = _pct(total_by_key.get(last_key), total_by_key.get(prev12_key))

    # 2) Pull Hagstofan CPI source once
    src = fetch_cpi_data()
    all_codes = list_isnr(src)

    # pick: top-level "ISxx" (len==4, not IS00) + your curated set
    top_level = sorted([c for c in all_codes if c.startswith("IS") and len(c) == 4 and c != "IS00"])
    target_codes = sorted(set(top_level).union(CPI_CURATED))

    # 3) For each code, get its series, compute value/MoM/YoY for latest month, upsert
    for code in target_codes:
        df = get_isnr_series(src, code)  # columns: date, value, Monthly Change (maybe)
        by_key = {_yyyymm(d): float(v) for d, v in zip(df["date"], df["value"])}

        val = by_key.get(last_key)
        mom = _pct(by_key.get(last_key), by_key.get(prev_key))
        yoy = _pct(by_key.get(last_key), by_key.get(prev12_key))

        delta_mom = None if (mom is None or total_mom is None) else (mom - total_mom)
        delta_yoy = None if (yoy is None or total_yoy is None) else (yoy - total_yoy)

        label = isnr_label(code) or code

        # UPSERT row (date+code unique)
        obj = session.query(CPISubMetric).filter_by(date=last_dt, code=code).one_or_none()
        if obj:
            obj.label = label
            obj.value = val
            obj.mom = mom
            obj.yoy = yoy
            obj.delta_mom_vs_total = delta_mom
            obj.delta_yoy_vs_total = delta_yoy
        else:
            session.add(CPISubMetric(
                date=last_dt, code=code, label=label,
                value=val, mom=mom, yoy=yoy,
                delta_mom_vs_total=delta_mom, delta_yoy_vs_total=delta_yoy
            ))

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

def upsert_bci(s, df):
    # df: date, category, value
    for d, cat, val in df.itertuples(index=False):
        obj = s.query(BCIActual).filter_by(date=d, category=cat).one_or_none()
        if obj: obj.index_value = float(val)
        else:   s.add(BCIActual(date=d, category=cat, index_value=float(val)))

def save_bci_forecast(s, df, months=6):
    run = BCIForecastRun(months_predict=months, notes="linear_reg_24m")
    s.add(run); s.flush()
    for cat, sub in df.groupby("category"):
        ser = sub.set_index("date")["value"]
        for d, yhat in bci_forecast(ser, months=months):
            s.add(BCIForecastPoint(run_id=run.id, date=d, category=cat, predicted_index=float(yhat)))

def upsert_ppi(s, df):
    for d, cat, val in df.itertuples(index=False):
        obj = s.query(PPIActual).filter_by(date=d, category=cat).one_or_none()
        if obj: obj.index_value = float(val)
        else:   s.add(PPIActual(date=d, category=cat, index_value=float(val)))

def save_ppi_forecast(s, df, months=6):
    run = PPIForecastRun(months_predict=months, notes="linear_reg_24m")
    s.add(run); s.flush()
    for cat, sub in df.groupby("category"):
        ser = sub.set_index("date")["value"]
        for d, yhat in ppi_forecast(ser, months=months):
            s.add(PPIForecastPoint(run_id=run.id, date=d, category=cat, predicted_index=float(yhat)))


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

        # --- Sub-CPI metrics for latest month (fast) ---
        upsert_latest_cpi_sub_metrics(s)

        # --- BCI ---
        bci_df = fetch_bci(categories=["BCI"])  # add more cats later if desired
        upsert_bci(s, bci_df)
        save_bci_forecast(s, bci_df, months=6)

        # --- PPI ---
        ppi_df = fetch_ppi(categories=["PPI"])
        upsert_ppi(s, ppi_df)
        save_ppi_forecast(s, ppi_df, months=6)

        s.commit()
        print("✅ Stored CPI + wages (TOTAL) + PPI + BCI + forecasts")

    except Exception:
        s.rollback()
        raise
    finally:
        s.close()

if __name__ == "__main__":
    main()
