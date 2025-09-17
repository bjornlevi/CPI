# cpi_app/pipelines/wages.py
from __future__ import annotations
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from matplotlib.dates import date2num
from typing import Iterable, List, Tuple

# Try both import paths (depending on where you keep the module)
from cpi_app.scripts.Hagstofan.api_client import APIClient
from cpi_app.scripts.Hagstofan.community.wage_index import WageIndex


def fetch_wage_series(category: str = "TOTAL"):
    """
    Return a pandas.Series indexed by month (DatetimeIndex) for a given category
    using Hagstofan Wages (LAU04000) with Eining=index.
    """
    client = APIClient(base_url="https://px.hagstofa.is:443/pxis/api/v1")
    src = WageIndex(client)
    rows = src.get_series(category)
    if not rows:
        # fall back to first available category if requested not present
        cats = src.list_categories()
        if not cats:
            return pd.Series(dtype=float)
        rows = src.get_series(cats[0])
    dates, values = zip(*rows) if rows else ([], [])
    return pd.Series(values, index=pd.to_datetime(dates), dtype=float).sort_index()


def compute_forecast(series: pd.Series, months: int = 12, window: int = 24) -> List[Tuple[pd.Timestamp, float]]:
    """
    Linear regression on the level, anchored to the last observed point.
    Returns list of (future_date, predicted_value) for the next `months`.
    """
    s = series.dropna().sort_index()
    if s.empty:
        return []

    if len(s) > window:
        s = s.iloc[-window:]

    x = date2num(s.index).reshape(-1, 1)
    y = s.values.astype(float)

    model = LinearRegression().fit(x, y)

    # Anchor to last observed level to avoid a visual jump
    last_x = date2num(s.index[-1])
    last_hat = model.predict([[last_x]])[0]
    bias = y[-1] - last_hat

    future_dates = [s.index[-1] + pd.DateOffset(months=i) for i in range(1, months + 1)]
    future_x = date2num(future_dates).reshape(-1, 1)
    preds = (model.predict(future_x) + bias).tolist()

    return list(zip(future_dates, preds))
