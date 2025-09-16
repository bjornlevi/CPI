# cpi_pipeline.py
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression

PX_URL = "https://px.hagstofa.is:443/pxis/api/v1/is/Efnahagur/visitolur/1_vnv/1_vnv/VIS01000.px"

def fetch_cpi_data():
    payload = {
        "query": [
            {"code": "Vísitala", "selection": {"filter": "item", "values": ["CPI"]}},
            {"code": "Liður", "selection": {"filter": "item", "values": ["index", "change_M"]}}
        ],
        "response": {"format": "json"}
    }
    r = requests.post(PX_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def parse_data(js):
    recs = []
    for entry in js["data"]:
        date_str, _, measure_type = entry["key"]
        val = entry["values"][0]
        if val == ".":
            continue
        try:
            d = datetime.strptime(date_str, "%YM%m")
        except ValueError:
            continue
        recs.append({"date": d.date(), "type": measure_type, "value": float(val)})
    df = pd.DataFrame(recs)
    wide = df.pivot(index="date", columns="type", values="value").sort_index()
    wide = wide.rename(columns={"index": "CPI", "change_M": "Monthly Change"})
    wide = wide.reset_index().rename(columns={"index": "date"})
    return wide  # columns: date, CPI, Monthly Change

def compute_trend(df, months_predict=6):
    # df has columns ["date","CPI"]
    X = np.arange(len(df)).reshape(-1, 1)
    y = df["CPI"].values

    model = LinearRegression()
    model.fit(X, y)

    future_X = np.arange(len(df), len(df) + months_predict).reshape(-1, 1)

    # --- anchor: make the model match the last observed value ---
    last_hat = model.predict([[len(df) - 1]])[0]
    bias = y[-1] - last_hat
    preds = model.predict(future_X) + bias

    last_date = df["date"].iloc[-1]
    future_dates = [last_date + relativedelta(months=i) for i in range(1, months_predict + 1)]
    return model, list(zip(future_dates, preds))

def compute_annual_cpi(series, idx):
    # series is a 1D array-like of CPI values
    if idx < 12:
        return None
    return (series[idx] / series[idx - 12] - 1) * 100.0
