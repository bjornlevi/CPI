# backfill_ppi_bci.py
from datetime import datetime
import pandas as pd

from sqlalchemy.orm import Session
from cpi_app.models import (
    Base, engine,
    BCIActual, BCIForecastRun, BCIForecastPoint,
    PPIActual, PPIForecastRun, PPIForecastPoint,
)
from cpi_app.scripts.Hagstofan.api_client import APIClient
from cpi_app.scripts.Hagstofan.economy.construction_price_index import ConstructionPriceIndex
from cpi_app.scripts.Hagstofan.economy.production_price_index import ProductionPriceIndex
from cpi_app.pipelines.bci import compute_forecast as bci_forecast
from cpi_app.pipelines.ppi import compute_forecast as ppi_forecast

DATE_FMT = "%YM%m"   # e.g. 2025M07
FORECAST_MONTHS = 6
FORECAST_TOTAL_ONLY = True  # set False if you want forecasts for *every* category

def _parse_date(ym: str):
    try:
        return datetime.strptime(ym, DATE_FMT).date()
    except ValueError:
        return None

def backfill_bci(session: Session):
    client = APIClient(base_url="https://px.hagstofa.is:443/pxis/api/v1")
    ds = ConstructionPriceIndex(client)
    cats = ds.list_categories() or ["BCI"]

    # ---- upsert ALL historical actuals ----
    for cat in cats:
        for ym, v in ds.get_historical_values(cat, months=10000):
            d = _parse_date(ym)
            if d is None or v is None:
                continue
            obj = session.query(BCIActual).filter_by(date=d, category=cat).one_or_none()
            if obj: obj.index_value = float(v)
            else:   session.add(BCIActual(date=d, category=cat, index_value=float(v)))

    # ---- create a single forecast run ----
    run = BCIForecastRun(months_predict=FORECAST_MONTHS, notes="backfill linear_reg_24m")
    session.add(run); session.flush()

    forecast_cats = ["BCI"] if FORECAST_TOTAL_ONLY else cats
    for cat in forecast_cats:
        # build a pandas Series from DB (guarantees dedupe/sorted)
        rows = session.query(BCIActual).filter_by(category=cat).order_by(BCIActual.date).all()
        if len(rows) < 2:  # need at least 2 points
            continue
        s = pd.Series([r.index_value for r in rows], index=[r.date for r in rows])
        for d, yhat in bci_forecast(s, months=FORECAST_MONTHS):
            session.add(BCIForecastPoint(run_id=run.id, date=d, category=cat, predicted_index=float(yhat)))

def backfill_ppi(session: Session):
    client = APIClient(base_url="https://px.hagstofa.is:443/pxis/api/v1")
    ds = ProductionPriceIndex(client)
    cats = ds.list_categories() or ["PPI"]

    # ---- upsert ALL historical actuals ----
    for cat in cats:
        for ym, v in ds.get_historical_values(cat, months=10000):
            d = _parse_date(ym)
            if d is None or v is None:
                continue
            obj = session.query(PPIActual).filter_by(date=d, category=cat).one_or_none()
            if obj: obj.index_value = float(v)
            else:   session.add(PPIActual(date=d, category=cat, index_value=float(v)))

    # ---- create a single forecast run ----
    run = PPIForecastRun(months_predict=FORECAST_MONTHS, notes="backfill linear_reg_24m")
    session.add(run); session.flush()

    forecast_cats = ["PPI"] if FORECAST_TOTAL_ONLY else cats
    for cat in forecast_cats:
        rows = session.query(PPIActual).filter_by(category=cat).order_by(PPIActual.date).all()
        if len(rows) < 2:
            continue
        s = pd.Series([r.index_value for r in rows], index=[r.date for r in rows])
        for d, yhat in ppi_forecast(s, months=FORECAST_MONTHS):
            session.add(PPIForecastPoint(run_id=run.id, date=d, category=cat, predicted_index=float(yhat)))

def main():
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        try:
            backfill_bci(s)
            backfill_ppi(s)
            s.commit()
            print("✅ Backfilled BCI & PPI (actuals) and created 6-month forecasts")
        except Exception:
            s.rollback()
            raise

if __name__ == "__main__":
    main()
