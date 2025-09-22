import os
from flask import Flask, render_template, request
from sqlalchemy.orm import Session
from sqlalchemy import select, func, desc
from werkzeug.middleware.proxy_fix import ProxyFix
from typing import List
from statistics import mean, median, stdev

from .models import (
    engine,
    CPIActual, ForecastRun, ForecastPoint,
    WageActual, WageForecastRun, WageForecastPoint,
    CPISubMetric
)

from .models import (
    engine,
    # existing…
    BCIActual, BCIForecastRun, BCIForecastPoint,
    PPIActual, PPIForecastRun, PPIForecastPoint,
)

# CPI helpers (from your pipelines)
from .pipelines.cpi import (
    fetch_cpi_data,         # fetches Hagstofan CPI source
    list_isnr,              # returns all ISxx codes
    isnr_label,             # pretty label for a code
    get_isnr_series,        # returns df(date,value,Monthly Change) for a code
)

FORECAST_MONTHS = 6   # UI cap; also set months=6 in your jobs/backfills

CURATED_ISNR = os.environ.get("CPI_CURATED_CODES",
    "IS011,IS041,IS042,IS0451,IS0455,IS06,IS0722,IS111"
).split(",")
CURATED_ISNR = [c.strip() for c in CURATED_ISNR if c.strip()]


def _cpi_context():
    """Build context for CPI total + forecast + subcategory overlays."""
    with Session(engine) as s:
        cpi_actuals = s.scalars(select(CPIActual).order_by(CPIActual.date)).all()

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

    # last 24 actuals for the x-axis base
    labels = [a.date.strftime("%Y-%m") for a in cpi_actuals[-24:]]
    values = [a.cpi for a in cpi_actuals[-24:]]
    cpi_stats = _series_stats(values)

    cpi_future = cpi_future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in cpi_future]
    fut_values = [p.predicted_cpi for p in cpi_future]
    updated = cpi_actuals[-1].date.strftime("%Y-%m") if cpi_actuals else "N/A"
    cpi_table = _structured_change_table(values, fut_values, len(labels), len(fut_labels))

    # ---------- Sub-CPI overlays ----------

    # 1) Curated set (always include)
    cpi_src = fetch_cpi_data()
    def series_for(code: str):
        df = get_isnr_series(cpi_src, code)
        m = {d.strftime("%Y-%m"): float(v) for d, v in zip(df["date"], df["value"])}
        return [m.get(lbl) for lbl in labels]

    curated_meta = [{"code": c, "label": isnr_label(c) or c} for c in CURATED_ISNR]
    curated_series = {c: series_for(c) for c in CURATED_ISNR}

    # 2) Top movers from DB (latest month), by |delta_yoy_vs_total| (fall back to |delta_mom_vs_total|)
    with Session(engine) as s2:
        latest_date = s2.scalar(select(func.max(CPISubMetric.date)))
        top_meta, top_series = [], {}
        if latest_date:
            rows = s2.execute(
                select(CPISubMetric)
                .where(CPISubMetric.date == latest_date)
            ).scalars().all()

            # rank by YoY deviation, fallback to MoM deviation if YoY missing
            scored = []
            for r in rows:
                if r.code in CURATED_ISNR:
                    continue
                score = abs(r.delta_yoy_vs_total) if r.delta_yoy_vs_total is not None else (
                        abs(r.delta_mom_vs_total) if r.delta_mom_vs_total is not None else None)
                if score is not None:
                    scored.append((score, r))
            scored.sort(key=lambda t: t[0], reverse=True)
            picked = [r for _, r in scored[:6]]  # top 6 movers

            top_meta = [{"code": r.code, "label": r.label} for r in picked]
            for r in picked:
                top_series[r.code] = series_for(r.code)

    # combine curated + top movers (dedupe by code)
    seen = set()
    cpi_sub_meta = []
    cpi_sub_series = {}
    for meta_list, series_map in ((curated_meta, curated_series), (top_meta, top_series)):
        for m in meta_list:
            if m["code"] in seen:
                continue
            seen.add(m["code"])
            cpi_sub_meta.append(m)
            cpi_sub_series[m["code"]] = series_map[m["code"]]

    # ... after you have `rows` and `picked` ...
    rows_by_code = {r.code: r for r in rows}

    # Curated first (if available this month)
    curated_data = []
    for code in CURATED_ISNR:
        r = rows_by_code.get(code)
        if not r:
            continue
        curated_data.append({
            "code": code,
            "label": r.label or code,
            "mom": r.mom, "yoy": r.yoy,
            "d_mom": r.delta_mom_vs_total, "d_yoy": r.delta_yoy_vs_total,
        })

    # Then the top movers you picked (ranked by delta vs total), excluding duplicates
    top_data = []
    seen = {d["code"] for d in curated_data}
    for r in picked:
        if r.code in seen:
            continue
        top_data.append({
            "code": r.code,
            "label": r.label or r.code,
            "mom": r.mom, "yoy": r.yoy,
            "d_mom": r.delta_mom_vs_total, "d_yoy": r.delta_yoy_vs_total,
        })

    cpi_movers = curated_data + top_data

    return dict(
        labels=labels, values=values,
        fut_labels=fut_labels, fut_values=fut_values,
        updated=updated,
        cpi_sub_meta=cpi_sub_meta, cpi_sub_series=cpi_sub_series,
        cpi_table=cpi_table,
        cpi_movers=cpi_movers,
    )


def _wages_context(requested_cat: str | None):
    """Build context for wages chart for a chosen category with newest forecast."""
    with Session(engine) as s:
        cats = s.scalars(
            select(WageActual.category).distinct().order_by(WageActual.category)
        ).all() or ["TOTAL", "ALM", "OPI", "OPI_R", "OPI_L"]

        cat = requested_cat if requested_cat in cats else (requested_cat or cats[0])

        w_actuals = s.scalars(
            select(WageActual)
            .where(WageActual.category == cat)
            .order_by(WageActual.date)
        ).all()

        best_run_id = s.scalar(
            select(WageForecastPoint.run_id)
            .where(WageForecastPoint.category == cat)
            .group_by(WageForecastPoint.run_id)
            .order_by(func.max(WageForecastPoint.date).desc())
            .limit(1)
        )

        w_future = []
        if best_run_id:
            w_future = s.scalars(
                select(WageForecastPoint)
                .where(
                    WageForecastPoint.run_id == best_run_id,
                    WageForecastPoint.category == cat,
                )
                .order_by(WageForecastPoint.date)
            ).all()

    labels = [a.date.strftime("%Y-%m") for a in w_actuals[-24:]]
    values = [a.index_value for a in w_actuals[-24:]]
    wage_stats = _series_stats(values)

    w_future = w_future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in w_future]
    fut_values = [p.predicted_index for p in w_future]
    updated = w_actuals[-1].date.strftime("%Y-%m") if w_actuals else "N/A"
    wage_table = _structured_change_table(values, fut_values, len(labels), len(fut_labels))

    # YoY gap vs TOTAL and identical check
    gap_vs_total = None
    identical_to_total = False
    if cat != "TOTAL":
        # fetch TOTAL actuals for alignment
        total_vals = None
        with Session(engine) as s2:
            tot = s2.scalars(
                select(WageActual).where(WageActual.category == "TOTAL").order_by(WageActual.date)
            ).all()
            total_vals = [a.index_value for a in tot[-len(values):]] if tot else None

        if total_vals:
            # identical if all overlapping values match (within tiny epsilon)
            eps = 1e-6
            identical_to_total = all(
                (a is not None and b is not None and abs(a - b) < eps) for a, b in zip(values[-len(total_vals):], total_vals)
            )
            # YoY gap = selected YoY − TOTAL YoY (current point only, if defined)
            sel_yoy = wage_stats.get("curr_yoy")
            tot_stats = _series_stats(total_vals)
            tot_yoy = tot_stats.get("curr_yoy")
            if sel_yoy is not None and tot_yoy is not None:
                gap_vs_total = sel_yoy - tot_yoy

    # Real wage YoY = wage YoY - CPI YoY (using CPI totals)
    with Session(engine) as s3:
        cpi_all = s3.scalars(select(CPIActual).order_by(CPIActual.date)).all()
        cpi_vals = [a.cpi for a in cpi_all]
    cpi_stats = _series_stats(cpi_vals)
    real_wage_yoy = None
    if wage_stats.get("curr_yoy") is not None and cpi_stats.get("curr_yoy") is not None:
        real_wage_yoy = wage_stats["curr_yoy"] - cpi_stats["curr_yoy"]

    return dict(
        wages_labels=labels, wages_values=values,
        wages_fut_labels=fut_labels, wages_fut_values=fut_values,
        wages_updated=updated,
        wage_category=cat, wage_categories=cats,
        wage_stats=wage_stats,
        real_wage_yoy=real_wage_yoy,
        wage_gap_vs_total=gap_vs_total,
        wage_identical_to_total=identical_to_total,
        wage_table=wage_table,
    )

def _pick_best_cat(session, model_actual, preferred: str | None):
    """Return a category that actually has rows. Prefer `preferred` if present,
    else the category with the most recent data, else the first alphabetically."""
    cats = session.scalars(
        select(model_actual.category).distinct().order_by(model_actual.category)
    ).all() or []

    if preferred and preferred in cats:
        return preferred, cats

    # category with the latest observation
    best = session.execute(
        select(model_actual.category, func.max(model_actual.date).label("m"))
        .group_by(model_actual.category)
        .order_by(func.max(model_actual.date).desc())
        .limit(1)
    ).first()
    if best:
        return best[0], cats

    # fallback
    return (cats[0] if cats else None), cats

def _bci_context(requested_cat: str | None):
    with Session(engine) as s:
        # prefer "BCI" (total); otherwise pick the category with newest data
        cat, cats = _pick_best_cat(s, BCIActual, preferred="BCI")
        if requested_cat and requested_cat in cats:
            cat = requested_cat

        actuals = s.scalars(
            select(BCIActual).where(BCIActual.category == cat).order_by(BCIActual.date)
        ).all()

        # Use the forecast run that has the latest point for *this* category (may be none)
        best_run_id = s.scalar(
            select(BCIForecastPoint.run_id)
            .where(BCIForecastPoint.category == cat)
            .group_by(BCIForecastPoint.run_id)
            .order_by(func.max(BCIForecastPoint.date).desc())
            .limit(1)
        )
        future = s.scalars(
            select(BCIForecastPoint)
            .where(BCIForecastPoint.run_id == best_run_id,
                   BCIForecastPoint.category == cat)
            .order_by(BCIForecastPoint.date)
        ).all() if best_run_id else []

    labels     = [a.date.strftime("%Y-%m") for a in actuals[-24:]]
    values     = [a.index_value for a in actuals[-24:]]
    future     = future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in future]
    fut_values = [p.predicted_index for p in future]
    updated    = actuals[-1].date.strftime("%Y-%m") if actuals else "N/A"

    return dict(
        bci_labels=labels, bci_values=values,
        bci_fut_labels=fut_labels, bci_fut_values=fut_values,
        bci_updated=updated,
        bci_category=cat, bci_categories=cats,
    )

def _ppi_context(requested_cat: str | None):
    with Session(engine) as s:
        cat, cats = _pick_best_cat(s, PPIActual, preferred="PPI")
        if requested_cat and requested_cat in cats:
            cat = requested_cat

        actuals = s.scalars(
            select(PPIActual).where(PPIActual.category == cat).order_by(PPIActual.date)
        ).all()

        best_run_id = s.scalar(
            select(PPIForecastPoint.run_id)
            .where(PPIForecastPoint.category == cat)
            .group_by(PPIForecastPoint.run_id)
            .order_by(func.max(PPIForecastPoint.date).desc())
            .limit(1)
        )
        future = s.scalars(
            select(PPIForecastPoint)
            .where(PPIForecastPoint.run_id == best_run_id,
                   PPIForecastPoint.category == cat)
            .order_by(PPIForecastPoint.date)
        ).all() if best_run_id else []

    labels     = [a.date.strftime("%Y-%m") for a in actuals[-24:]]
    values     = [a.index_value for a in actuals[-24:]]
    future     = future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in future]
    fut_values = [p.predicted_index for p in future]
    updated    = actuals[-1].date.strftime("%Y-%m") if actuals else "N/A"

    return dict(
        ppi_labels=labels, ppi_values=values,
        ppi_fut_labels=fut_labels, ppi_fut_values=fut_values,
        ppi_updated=updated,
        ppi_category=cat, ppi_categories=cats,
    )

def _pct_change(curr: float, prev: float) -> float | None:
    if curr is None or prev in (None, 0):
        return None
    return (curr / prev - 1.0) * 100.0

def _series_stats(values: list[float]) -> dict:
    """Compute historical MoM% and YoY% series + current snapshots and simple stats."""
    # monthly changes (from second point onward)
    mom = []
    for i in range(1, len(values)):
        a, b = values[i], values[i-1]
        if a is None or b in (None, 0): 
            continue
        mom.append((a / b - 1.0) * 100.0)

    # year-over-year (from 12th onward)
    yoy = []
    for i in range(12, len(values)):
        a, b = values[i], values[i-12]
        if a is None or b in (None, 0):
            continue
        yoy.append((a / b - 1.0) * 100.0)

    def safe_mean(xs):   return mean(xs)   if xs else None
    def safe_median(xs): return median(xs) if xs else None

    # current snapshots
    curr = values[-1] if values else None
    prev = values[-2] if len(values) >= 2 else None
    prev12 = values[-13] if len(values) >= 13 else None

    return {
        "curr": curr,
        "curr_mom": _pct_change(curr, prev),
        "curr_yoy": _pct_change(curr, prev12),
        "hist_mom_mean": safe_mean(mom),
        "hist_mom_median": safe_median(mom),
        "hist_yoy_mean": safe_mean(yoy),
        "hist_yoy_median": safe_median(yoy),
        # optional extras:
        "hist_mom_std": (stdev(mom) if len(mom) > 1 else None),
        "hist_yoy_std": (stdev(yoy) if len(yoy) > 1 else None),
    }

def _pct(curr: float | None, prev: float | None) -> float | None:
    if curr is None or prev in (None, 0):
        return None
    return (curr / prev - 1.0) * 100.0

def _changes_mom(values: list[float]) -> list[float]:
    out = []
    for i in range(1, len(values)):
        out.append(_pct(values[i], values[i-1]))
    return [x for x in out if x is not None]

def _changes_yoy(values: list[float]) -> list[float]:
    out = []
    for i in range(12, len(values)):
        out.append(_pct(values[i], values[i-12]))
    return [x for x in out if x is not None]

def _structured_change_table(values: list[float], fut_values: list[float],
                             label_count: int, fut_count: int) -> dict:
    """
    Build a table-like dict with:
      monthly: historic avg/median, current, projected avg/median (next horizon)
      yearly:  historic avg/median, current, projected (YoY at last forecast month)
    """
    values = list(values or [])
    fut_values = list(fut_values or [])
    combined = values + fut_values

    # Historic based only on actuals
    hist_mom = _changes_mom(values)
    hist_yoy = _changes_yoy(values)

    # Current snapshots (last actual)
    curr = values[-1] if values else None
    curr_mom = _pct(curr, values[-2] if len(values) >= 2 else None)
    curr_yoy = _pct(curr, values[-13] if len(values) >= 13 else None)

    # Projected on the *current track* (using provided forecast path)
    proj_moms = []
    if fut_count > 0 and len(combined) >= 2:
        start = len(values)  # first forecast index in combined
        for i in range(start, start + fut_count):
            prev = combined[i-1]
            currf = combined[i]
            proj = _pct(currf, prev)
            if proj is not None:
                proj_moms.append(proj)

    # Projected YoY at *last* forecast month (if we can reference t-12)
    proj_yoy_last = None
    if fut_count > 0:
        last = len(combined) - 1
        prev12_idx = last - 12
        if prev12_idx >= 0:
            proj_yoy_last = _pct(combined[last], combined[prev12_idx])

    def m(x):   return (mean(x)   if x else None)
    def med(x): return (median(x) if x else None)

    return {
        "monthly": {
            "historic_avg":   m(hist_mom),
            "historic_med":   med(hist_mom),
            "current":        curr_mom,
            "projected_avg":  m(proj_moms),
            "projected_med":  med(proj_moms),
            "horizon":        fut_count,
        },
        "yearly": {
            "historic_avg":   m(hist_yoy),
            "historic_med":   med(hist_yoy),
            "current":        curr_yoy,
            "projected":      proj_yoy_last,
        }
    }


def create_app():
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.config["SITE_NAME"] = os.environ.get("SITE_NAME", "Efnahagur")

    @app.get("/health")
    def health():
        return {"ok": True}

    # ---------- Home ----------
    @app.get("/")
    def index():
        cpi_ctx   = _cpi_context()
        wages_ctx = _wages_context(request.args.get("cat"))
        bci_ctx   = _bci_context(None)
        ppi_ctx   = _ppi_context(None)
        return render_template(
            "index.html",
            site_name=app.config["SITE_NAME"],
            **cpi_ctx, **wages_ctx, **bci_ctx, **ppi_ctx
        )


    # ---------- CPI detail ----------
    @app.get("/cpi")
    def cpi_page():
        cpi_ctx = _cpi_context()
        return render_template(
            "cpi.html",
            site_name=app.config["SITE_NAME"],
            **cpi_ctx,
        )

    # ---------- Wages detail ----------
    @app.get("/wages")
    def wages_page():
        wages_ctx = _wages_context(request.args.get("cat"))
        return render_template(
            "wages.html",
            site_name=app.config["SITE_NAME"],
            **wages_ctx,
        )

    @app.get("/bci")
    def bci_page():
        ctx = _bci_context(request.args.get("cat"))
        return render_template("bci.html", site_name=app.config["SITE_NAME"], **ctx)

    @app.get("/ppi")
    def ppi_page():
        ctx = _ppi_context(request.args.get("cat"))
        return render_template("ppi.html", site_name=app.config["SITE_NAME"], **ctx)

    return app

if __name__ == "__main__":
    create_app().run(debug=True)
