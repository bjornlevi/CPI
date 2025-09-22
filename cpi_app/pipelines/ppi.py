from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from cpi_app.scripts.Hagstofan.api_client import APIClient
from cpi_app.scripts.Hagstofan.economy.production_price_index import ProductionPriceIndex

DATE_FMT = "%YM%m"

def fetch_ppi_series(categories=None) -> pd.DataFrame:
    client = APIClient(base_url="https://px.hagstofa.is:443/pxis/api/v1")
    ds = ProductionPriceIndex(client)
    cats = categories or ["PPI"]  # total by default
    rows = []
    for cat in cats:
        for ym, v in ds.get_historical_values(cat, months=10000):
            try:
                d = datetime.strptime(ym, DATE_FMT).date()
            except ValueError:
                continue
            rows.append((d, cat, float(v)))
    df = pd.DataFrame(rows, columns=["date", "category", "value"]).sort_values("date")
    return df

def compute_forecast(series: pd.Series, months: int = 6, window: int = 24):
    s = series.dropna()
    if len(s) < 2:
        return []
    y = s.values[-window:] if len(s) > window else s.values
    X = np.arange(len(y)).reshape(-1, 1)
    model = LinearRegression().fit(X, y)
    future_X = np.arange(len(y), len(y) + months).reshape(-1, 1)
    preds = model.predict(future_X)
    last_date = s.index[-1]
    future_dates = pd.date_range(last_date, periods=months+1, freq="MS")[1:]
    return list(zip([d.date() for d in future_dates], preds))
