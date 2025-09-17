# cpi_app/pipelines/cpi.py
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression
from datetime import datetime

# --- Import your Hagstofan module (works whether you import as "Hagstofan" or via package path) ---
from cpi_app.scripts.Hagstofan.api_client import APIClient
from cpi_app.scripts.Hagstofan.economy.cpi import CPI as _CPI
from cpi_app.scripts.Hagstofan.economy.isnr_labels import ISNRLabels

# ---------- Public API (keeps old function names) ----------

def fetch_cpi_data() -> _CPI:
    """
    Backwards-compatible replacement for the old 'fetch_cpi_data' that used requests directly.
    Returns a CPI data-source object backed by your Hagstofan module, already loaded with:
      - overall CPI and all ISNR sub-categories (B1997 index)
      - weights (from VIS01305)
    """
    client = APIClient(base_url="https://px.hagstofa.is:443/pxis/api/v1")
    return _CPI(client)


def parse_data(source: "_CPI") -> pd.DataFrame:
    """
    Backwards-compatible replacement for the old 'parse_data(js)'.

    Now accepts the CPI source object returned by fetch_cpi_data()
    and returns a DataFrame with columns: ['date', 'CPI', 'Monthly Change'] where
    CPI is IS00 and 'Monthly Change' is pct change vs previous month.
    """
    if not isinstance(source, _CPI):
        raise TypeError("parse_data() now expects the CPI object returned by fetch_cpi_data().")

    # Build a tidy series for IS00 (overall CPI)
    rows: List[Tuple[datetime, float]] = []
    for (ym, isnr), val in source.index.items():
        if isnr == "IS00" and isinstance(val, (int, float)):
            try:
                dt = datetime.strptime(ym, "%YM%m")
            except ValueError:
                continue
            rows.append((dt, float(val)))

    if not rows:
        return pd.DataFrame(columns=["date", "CPI", "Monthly Change"])

    rows.sort(key=lambda t: t[0])
    df = pd.DataFrame(rows, columns=["date", "CPI"]).sort_values("date").reset_index(drop=True)
    # Month-over-month pct change
    df["Monthly Change"] = df["CPI"].pct_change(periods=1) * 100.0
    return df


def compute_trend(df: pd.DataFrame, months_predict: int = 6) -> Tuple[LinearRegression, List[Tuple[datetime, float]]]:
    """
    Linear trend on CPI level with ANCHOR to last observed value (no visual jump).
    Returns (model, [(future_date, predicted_cpi), ...]).
    """
    if df.empty:
        return LinearRegression(), []

    X = np.arange(len(df)).reshape(-1, 1)
    y = df["CPI"].astype(float).values
    model = LinearRegression()
    model.fit(X, y)

    # bias-correct to anchor at last actual
    last_hat = model.predict([[len(df) - 1]])[0]
    bias = y[-1] - last_hat

    future_X = np.arange(len(df), len(df) + months_predict).reshape(-1, 1)
    preds = model.predict(future_X) + bias

    last_date = pd.to_datetime(df["date"].iloc[-1])
    future_dates = [last_date + relativedelta(months=i) for i in range(1, months_predict + 1)]
    return model, list(zip([d.to_pydatetime() for d in future_dates], preds.tolist()))


def compute_annual_cpi(df: pd.DataFrame, end_index: int) -> Optional[float]:
    """
    Annual % change for CPI at a given index (vs t-12).
    """
    if end_index < 12 or df.empty:
        return None
    current = float(df.loc[end_index, "CPI"])
    prior = float(df.loc[end_index - 12, "CPI"])
    if prior == 0:
        return None
    return (current / prior - 1.0) * 100.0


# ---------- New helpers leveraging sub-categories (ISNR) ----------

def list_isnr(source: "_CPI") -> List[str]:
    """Sorted list of available ISNR codes (e.g., IS0112)."""
    return source.list_is_nr_values()


def isnr_label(code: str) -> Optional[str]:
    """Human label for an ISNR code."""
    return ISNRLabels.get(code)


def get_isnr_series(source: "_CPI", isnr: str) -> pd.DataFrame:
    """
    Tidy monthly series for a single ISNR:
    columns: ['date', 'value', 'Monthly Change']  (value is the B1997 index)
    """
    rows: List[Tuple[datetime, float]] = []
    for (ym, code), val in source.index.items():
        if code != isnr:
            continue
        try:
            dt = datetime.strptime(ym, "%YM%m")
        except ValueError:
            continue
        if isinstance(val, (int, float)):
            rows.append((dt, float(val)))

    rows.sort(key=lambda t: t[0])
    df = pd.DataFrame(rows, columns=["date", "value"])
    if df.empty:
        return df
    df["Monthly Change"] = df["value"].pct_change(periods=1) * 100.0
    return df


def latest_weights(source: "_CPI") -> Dict[str, float]:
    """
    Returns a dict {ISNR: weight} for the latest month where weights exist.
    (Uses VIS01305.px that your module already loads.)
    """
    if not source.weights:
        return {}
    # Find latest weight month
    last_month = max(ym for (ym, _isnr) in source.weights.keys())
    # Collect weights for that month
    out: Dict[str, float] = {}
    for (ym, code), w in source.weights.items():
        if ym == last_month:
            try:
                out[code] = float(w)
            except Exception:
                continue
    return out


def contribution_table(source: "_CPI", months_back: int = 1, top_k: Optional[int] = 10) -> pd.DataFrame:
    """
    (Nice for the UI) Build a table of latest sub-category contributions.

    For each ISNR:
      - label
      - latest value
      - MoM % (vs t-1)
      - YoY % (vs t-12)
      - latest weight (if available)

    Returns a DataFrame sorted by |MoM %| descending (optionally top_k rows).
    """
    rows = []
    weights = latest_weights(source)  # per-ISNR
    for code in list_isnr(source):
        s = get_isnr_series(source, code)
        if s.empty or len(s) < 13:
            continue
        last = s.iloc[-1]
        prev_m = s.iloc[-2]
        prev_y = s.iloc[-13]
        try:
            mom = (last["value"] / prev_m["value"] - 1.0) * 100.0
            yoy = (last["value"] / prev_y["value"] - 1.0) * 100.0
        except ZeroDivisionError:
            mom = float("nan"); yoy = float("nan")
        rows.append({
            "isnr": code,
            "label": ISNRLabels.get(code) or code,
            "date": last["date"].strftime("%Y-%m"),
            "index": last["value"],
            "mom_pct": mom,
            "yoy_pct": yoy,
            "weight": weights.get(code),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(by=df["mom_pct"].abs().sort_values(ascending=False).index)
    if top_k:
        df = df.head(top_k)
    return df.reset_index(drop=True)
