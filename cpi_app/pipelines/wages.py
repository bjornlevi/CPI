import requests, pandas as pd, numpy as np
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression
from matplotlib.dates import date2num

WAGE_URL = "https://px.hagstofa.is/pxis/api/v1/is/Samfelag/launogtekjur/2_lvt/1_manadartolur/LAU04003.px"

def get_px_meta(url=WAGE_URL):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def _var(meta, pred):
    for v in meta["variables"]:
        if pred(v):
            return v
    raise KeyError("Variable not found")

def build_wage_payload(meta, groups=("TOTAL","ALM","OPI","OPI_R","OPI_L"), months_back=240):
    visitala = _var(meta, lambda v: v.get("text","").lower().find("vísitala")!=-1 or v["code"].lower().startswith("v"))
    eining   = _var(meta, lambda v: v.get("text","").lower().find("eining")  !=-1 or v["code"].lower().startswith("eini"))
    hopur    = _var(meta, lambda v: v.get("text","").lower().find("hópur")   !=-1 or v["code"].lower().startswith(("hop","cat")))
    timi     = _var(meta, lambda v: v.get("time") is True)

    allowed_groups = set(hopur["values"])
    want_groups = [g for g in groups if g in allowed_groups] or [hopur["values"][0]]

    def ensure_value(var, desired):
        return desired if desired in var["values"] else var["values"][0]

    visitala_val = ensure_value(visitala, "LVT")
    eining_val   = ensure_value(eining, "index")

    return {
        "query": [
            {"code": visitala["code"], "selection": {"filter": "item", "values": [visitala_val]}},
            {"code": eining["code"],   "selection": {"filter": "item", "values": [eining_val]}},
            {"code": hopur["code"],    "selection": {"filter": "item", "values": want_groups}},
            {"code": timi["code"],     "selection": {"filter": "top",  "values": [str(240)]}},
        ],
        "response": {"format": "json"}
    }

def fetch_wage_data():
    meta = get_px_meta(WAGE_URL)
    payload = build_wage_payload(meta)
    r = requests.post(WAGE_URL, json=payload, timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        raise RuntimeError(f"PXWeb error {r.status_code}\nPayload:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\nResponse:\n{r.text[:1000]}")
    return r.json()

def parse_data(js) -> pd.DataFrame:
    """Return tidied rows: date (date), category (str), value (float)."""
    rows = []
    for entry in js["data"]:
        d_str, _, _, cat = entry["key"]  # date, Vísitala, Eining, Hópur
        v = entry["values"][0]
        if v == ".": 
            continue
        try:
            d = datetime.strptime(d_str, "%YM%m").date()
        except ValueError:
            continue
        rows.append((d, cat, float(v)))
    df = pd.DataFrame(rows, columns=["date", "category", "value"]).sort_values(["category","date"])
    return df

def compute_forecast(series, months=12, window=24):
    """
    series: pd.Series indexed by monthly DatetimeIndex, values are the wage index.
    returns: list[(date, value)]
    """
    s = series.sort_index()
    if len(s) > window:
        s = s.iloc[-window:]

    x = date2num(s.index).reshape(-1, 1)
    y = s.values

    model = LinearRegression()
    model.fit(x, y)

    # future months
    last_date = s.index[-1]
    future_dates = [last_date + pd.DateOffset(months=i) for i in range(1, months + 1)]
    future_x = date2num(future_dates).reshape(-1, 1)

    # --- anchor to last observed level ---
    last_hat = model.predict([[date2num(last_date)]])[0]
    bias = y[-1] - last_hat
    preds = model.predict(future_x) + bias

    return list(zip(future_dates, preds))

