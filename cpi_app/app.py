import os
from flask import Flask, render_template, request
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from werkzeug.middleware.proxy_fix import ProxyFix

from .models import (
    engine,
    CPIActual, ForecastRun, ForecastPoint,
    WageActual, WageForecastRun, WageForecastPoint,
)

# Hagstofan CPI helpers
from .pipelines.cpi import (
    fetch_cpi_data, list_isnr, isnr_label, get_isnr_series
)

FORECAST_MONTHS = 6  # UI cap (also set months=6 in your jobs/backfills)


def create_app():
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.config["SITE_NAME"] = os.environ.get("SITE_NAME", "Efnahagur")

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/")
    def index():
        # ---------- CPI (totals + forecast) ----------
        with Session(engine) as s:
            cpi_actuals = s.scalars(
                select(CPIActual).order_by(CPIActual.date)
            ).all()

            # Choose run with the most recent forecast point (avoid old backfills)
            best_run_id = s.scalar(
                select(ForecastPoint.run_id)
                .group_by(ForecastPoint.run_id)
                .order_by(func.max(ForecastPoint.date).desc())
                .limit(1)
            )

            cpi_future = []
            if best_run_id:
                cpi_future = s.scalars(
                    select(ForecastPoint)
                    .where(ForecastPoint.run_id == best_run_id)
                    .order_by(ForecastPoint.date)
                ).all()

        # Last 24 months as the x-axis
        cpi_labels = [a.date.strftime("%Y-%m") for a in cpi_actuals[-24:]]
        cpi_values = [a.cpi for a in cpi_actuals[-24:]]

        cpi_future = cpi_future[:FORECAST_MONTHS]
        cpi_fut_labels = [p.date.strftime("%Y-%m") for p in cpi_future]
        cpi_fut_values = [p.predicted_cpi for p in cpi_future]
        cpi_updated = cpi_actuals[-1].date.strftime("%Y-%m") if cpi_actuals else "N/A"

        # ---------- CPI subcategories (toggle overlays) ----------
        # Fetch once per request. (If this endpoint gets heavy, add a small cache.)
        cpi_src = fetch_cpi_data()
        all_codes = list_isnr(cpi_src)

        # "Main" categories: IS00 is total; keep IS01, IS02, ... (two digits only)
        main_codes = sorted([c for c in all_codes if c.startswith("IS") and len(c) == 4 and c != "IS00"])

        # Build aligned series for each subcategory (to the last-24 CPI labels)
        def aligned_series(code: str):
            df = get_isnr_series(cpi_src, code)  # columns: date, value, Monthly Change
            m = {d.strftime("%Y-%m"): float(v) for d, v in zip(df["date"], df["value"])}
            return [m.get(lbl) for lbl in cpi_labels]

        cpi_sub_meta = [{"code": c, "label": isnr_label(c) or c} for c in main_codes]
        cpi_sub_series = {c: aligned_series(c) for c in main_codes}

        # ---------- WAGES (actuals + newest forecast for selected category) ----------
        cat = request.args.get("cat")

        with Session(engine) as s:
            cats = s.scalars(
                select(WageActual.category).distinct().order_by(WageActual.category)
            ).all() or ["TOTAL", "ALM", "OPI", "OPI_R", "OPI_L"]

            if not cat or cat not in cats:
                cat = cats[0]

            w_actuals = s.scalars(
                select(WageActual)
                .where(WageActual.category == cat)
                .order_by(WageActual.date)
            ).all()

            best_wage_run_id = s.scalar(
                select(WageForecastPoint.run_id)
                .where(WageForecastPoint.category == cat)
                .group_by(WageForecastPoint.run_id)
                .order_by(func.max(WageForecastPoint.date).desc())
                .limit(1)
            )

            w_future = []
            if best_wage_run_id:
                w_future = s.scalars(
                    select(WageForecastPoint)
                    .where(
                        WageForecastPoint.run_id == best_wage_run_id,
                        WageForecastPoint.category == cat,
                    )
                    .order_by(WageForecastPoint.date)
                ).all()

        wages_labels = [a.date.strftime("%Y-%m") for a in w_actuals[-24:]]
        wages_values = [a.index_value for a in w_actuals[-24:]]

        w_future = w_future[:FORECAST_MONTHS]
        wages_fut_labels = [p.date.strftime("%Y-%m") for p in w_future]
        wages_fut_values = [p.predicted_index for p in w_future]

        return render_template(
            "index.html",
            site_name=app.config.get("SITE_NAME", "Efnahagur"),
            # CPI totals
            labels=cpi_labels,
            values=cpi_values,
            fut_labels=cpi_fut_labels,
            fut_values=cpi_fut_values,
            updated=cpi_updated,
            # CPI subcategories (toggle)
            cpi_sub_meta=cpi_sub_meta,         # [{code,label}, ...]
            cpi_sub_series=cpi_sub_series,     # { code: [values aligned to labels] }
            # Wages
            wages_labels=wages_labels,
            wages_values=wages_values,
            wages_fut_labels=wages_fut_labels,
            wages_fut_values=wages_fut_values,
            wage_category=cat,
            wage_categories=cats,
        )

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
