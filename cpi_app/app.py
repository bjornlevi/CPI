import os
from flask import Flask, render_template, request
from sqlalchemy.orm import Session
from sqlalchemy import select
from .models import (
    engine,
    CPIActual, ForecastRun, ForecastPoint,
    WageActual, WageForecastRun, WageForecastPoint,
)
from werkzeug.middleware.proxy_fix import ProxyFix

def create_app():
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)   
    app.config["SITE_NAME"] = os.environ.get("SITE_NAME", "Efnahagur")

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/")
    def index():
        run_id = request.args.get("run_id", type=int)
        cat_req = request.args.get("cat")  # e.g. ?cat=TOTAL

        # ---- CPI (same as you have now) ----
        with Session(engine) as s:
            cpi_actuals = s.scalars(select(CPIActual).order_by(CPIActual.date)).all()
            cpi_run = s.get(ForecastRun, run_id) if run_id else s.scalars(
                select(ForecastRun).order_by(ForecastRun.created_at.desc())
            ).first()
            cpi_future = s.scalars(
                select(ForecastPoint).where(ForecastPoint.run_id == cpi_run.id).order_by(ForecastPoint.date)
            ).all() if cpi_run else []

        cpi_labels = [a.date.strftime("%Y-%m") for a in cpi_actuals[-24:]] if cpi_actuals else []
        cpi_values = [a.cpi for a in cpi_actuals[-24:]] if cpi_actuals else []
        cpi_fut_labels = [p.date.strftime("%Y-%m") for p in cpi_future]
        cpi_fut_values = [p.predicted_cpi for p in cpi_future]
        cpi_updated = cpi_actuals[-1].date.strftime("%Y-%m") if cpi_actuals else "N/A"

        # ---- Wages: pick a real category ----
        with Session(engine) as s:
            # what categories exist?
            cats = s.execute(select(WageActual.category).group_by(WageActual.category).order_by(WageActual.category)).scalars().all()
            # choose category
            if cat_req in cats:
                cat = cat_req
            elif "TOTAL" in cats:
                cat = "TOTAL"
            else:
                cat = cats[0] if cats else None

            w_actuals = []
            w_future = []
            if cat:
                w_actuals = s.scalars(
                    select(WageActual).where(WageActual.category == cat).order_by(WageActual.date)
                ).all()
                w_run = s.scalars(select(WageForecastRun).order_by(WageForecastRun.created_at.desc())).first()
                if w_run:
                    w_future = s.scalars(
                        select(WageForecastPoint)
                        .where(WageForecastPoint.run_id == w_run.id, WageForecastPoint.category == cat)
                        .order_by(WageForecastPoint.date)
                    ).all()

        wages_labels = [a.date.strftime("%Y-%m") for a in w_actuals[-24:]] if w_actuals else []
        wages_values = [a.index_value for a in w_actuals[-24:]] if w_actuals else []
        wages_fut_labels = [p.date.strftime("%Y-%m") for p in w_future]
        wages_fut_values = [p.predicted_index for p in w_future]

        return render_template(
            "index.html",
            site_name=app.config["SITE_NAME"],
            # CPI
            labels=cpi_labels, values=cpi_values,
            fut_labels=cpi_fut_labels, fut_values=cpi_fut_values,
            updated=cpi_updated,
            # Wages
            wages_labels=wages_labels, wages_values=wages_values,
            wages_fut_labels=wages_fut_labels, wages_fut_values=wages_fut_values,
            wage_category=cat or "", wage_categories=cats,
        )


    return app

if __name__ == "__main__":
    create_app().run(debug=True)
