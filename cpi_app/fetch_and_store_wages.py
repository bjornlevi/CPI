from models import init_db, SessionLocal, WageActual, WageForecastRun, WageForecastPoint
from wage_pipeline import fetch_wage_data, parse_data, compute_forecast

def upsert_wage_actuals(s, df):
    for (d, cat, val) in df.itertuples(index=False):
        obj = s.query(WageActual).filter_by(date=d, category=cat).one_or_none()
        if obj: obj.index_value = float(val)
        else:   s.add(WageActual(date=d, category=cat, index_value=float(val)))

def save_wage_forecast(s, df, months=12, notes="linear_reg_24m"):
    run = WageForecastRun(months_predict=months, notes=notes)
    s.add(run); s.flush()
    for cat, sub in df.groupby("category"):
        ser = sub.set_index("date")["value"]
        for d, yhat in compute_forecast(ser, months=months):
            s.add(WageForecastPoint(run_id=run.id, date=d, category=cat, predicted_index=float(yhat)))
    return run.id

def main():
    init_db()
    js = fetch_wage_data()
    df = parse_data(js)
    s = SessionLocal()
    try:
        upsert_wage_actuals(s, df)
        save_wage_forecast(s, df, months=12)
        s.commit()
        print(f"Stored {len(df)} wage rows and a forecast run.")
    except:
        s.rollback(); raise
    finally:
        s.close()

if __name__ == "__main__":
    main()
