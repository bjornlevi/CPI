import os
from statistics import mean, median, stdev
from typing import Optional, Tuple, List, Dict, Any

from flask import Flask, render_template, request
from werkzeug.middleware.proxy_fix import ProxyFix
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from .models import (
    engine,
    # CPI
    CPIActual, ForecastRun, ForecastPoint, CPISubMetric,
    # Wages
    WageActual, WageForecastRun, WageForecastPoint,
    # BCI
    BCIActual, BCIForecastRun, BCIForecastPoint,
    # PPI
    PPIActual, PPIForecastRun, PPIForecastPoint,
)

# CPI helpers from your pipelines
from .pipelines.cpi import (
    fetch_cpi_data,   # fetches Hagstofan CPI source
    isnr_label,       # pretty label for ISNR code
    get_isnr_series,  # returns DataFrame with columns: date, value, (maybe Monthly Change)
)

# -----------------------------------------------------------------------------
# Configuration / constants
# -----------------------------------------------------------------------------
FORECAST_MONTHS = 6  # UI cap; keep your jobs/backfills producing 6 months

CURATED_ISNR = os.environ.get(
    "CPI_CURATED_CODES",
    "IS011,IS041,IS042,IS0451,IS0455,IS06,IS0722,IS111"
).split(",")
CURATED_ISNR = [c.strip() for c in CURATED_ISNR if c.strip()]


# -----------------------------------------------------------------------------
# Utility functions (stats/changes/tables)
# -----------------------------------------------------------------------------
def _pct(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
    if curr is None or prev in (None, 0):
        return None
    return (curr / prev - 1.0) * 100.0


def _changes_mom(values: list[float]) -> list[float]:
    out = []
    for i in range(1, len(values)):
        out.append(_pct(values[i], values[i - 1]))
    return [x for x in out if x is not None]


def _changes_yoy(values: list[float]) -> list[float]:
    out = []
    for i in range(12, len(values)):
        out.append(_pct(values[i], values[i - 12]))
    return [x for x in out if x is not None]


def _series_stats(values: list[float]) -> dict:
    """Compute MoM% & YoY% series + current snapshots and simple stats for a level series."""
    # MoM series
    mom = []
    for i in range(1, len(values)):
        a, b = values[i], values[i - 1]
        if a is None or b in (None, 0):
            continue
        mom.append((a / b - 1.0) * 100.0)

    # YoY series
    yoy = []
    for i in range(12, len(values)):
        a, b = values[i], values[i - 12]
        if a is None or b in (None, 0):
            continue
        yoy.append((a / b - 1.0) * 100.0)

    def safe_mean(xs):   return mean(xs)   if xs else None
    def safe_median(xs): return median(xs) if xs else None

    curr = values[-1] if values else None
    prev = values[-2] if len(values) >= 2 else None
    prev12 = values[-13] if len(values) >= 13 else None

    return {
        "curr": curr,
        "curr_mom": _pct(curr, prev),
        "curr_yoy": _pct(curr, prev12),
        "hist_mom_mean": safe_mean(mom),
        "hist_mom_median": safe_median(mom),
        "hist_yoy_mean": safe_mean(yoy),
        "hist_yoy_median": safe_median(yoy),
        "hist_mom_std": (stdev(mom) if len(mom) > 1 else None),
        "hist_yoy_std": (stdev(yoy) if len(yoy) > 1 else None),
    }


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

    # Historic based on actuals only
    hist_mom = _changes_mom(values)
    hist_yoy = _changes_yoy(values)

    # Current snapshots (last actual)
    curr = values[-1] if values else None
    curr_mom = _pct(curr, values[-2] if len(values) >= 2 else None)
    curr_yoy = _pct(curr, values[-13] if len(values) >= 13 else None)

    # Projected MoM on the forecast path
    proj_moms = []
    if fut_count > 0 and len(combined) >= 2:
        start = len(values)  # first forecast index in combined
        for i in range(start, start + fut_count):
            prev = combined[i - 1]
            currf = combined[i]
            proj = _pct(currf, prev)
            if proj is not None:
                proj_moms.append(proj)

    # Projected YoY at last forecast month
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


# -----------------------------------------------------------------------------
# Context builders
# -----------------------------------------------------------------------------
from typing import Tuple, List, Dict, Any

def _cpi_context() -> dict:
    """Build context for CPI: totals, forecast, full-length sub-series, movers, table."""
    with Session(engine) as s:
        # full history from DB
        cpi_actuals = s.scalars(select(CPIActual).order_by(CPIActual.date)).all()
        full_labels = [a.date.strftime("%Y-%m") for a in cpi_actuals]
        full_values = [a.cpi for a in cpi_actuals]

        # latest forecast run (points are only future months)
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

    # short 24-month window used on the homepage
    labels_24 = full_labels[-24:]
    values_24 = full_values[-24:]

    # forecast (cap to UI horizon)
    cpi_future = cpi_future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in cpi_future]
    fut_values = [p.predicted_cpi for p in cpi_future]

    updated = full_labels[-1] if full_labels else "N/A"
    cpi_table = _structured_change_table(values_24, fut_values, len(labels_24), len(fut_labels))

    # ---------- build full-length sub-series ----------
    # We fetch raw CPI from Hagstofan and map it onto full_labels.
    cpi_src = fetch_cpi_data()

    def series_for(code: str, on_labels: list[str]) -> list[float | None]:
        df = get_isnr_series(cpi_src, code)  # df has columns: date, value, Monthly Change
        lookup = {d.strftime("%Y-%m"): float(v) for d, v in zip(df["date"], df["value"])}
        return [lookup.get(lbl) for lbl in on_labels]

    curated_meta = [{"code": c, "label": isnr_label(c) or c} for c in CURATED_ISNR]
    # FULL history for sub-series (this is what the range control needs)
    curated_series_full = {c: series_for(c, full_labels) for c in CURATED_ISNR}

    # ---------- movers (latest month deltas vs total) ----------
    rows = []
    picked = []
    with Session(engine) as s2:
        latest_date = s2.scalar(select(func.max(CPISubMetric.date)))
        if latest_date:
            rows = s2.execute(
                select(CPISubMetric).where(CPISubMetric.date == latest_date)
            ).scalars().all()
            scored = []
            for r in rows:
                if r.code in CURATED_ISNR:
                    continue
                score = (
                    abs(r.delta_yoy_vs_total) if r.delta_yoy_vs_total is not None
                    else (abs(r.delta_mom_vs_total) if r.delta_mom_vs_total is not None else None)
                )
                if score is not None:
                    scored.append((score, r))
            scored.sort(key=lambda t: t[0], reverse=True)
            picked = [r for _, r in scored[:6]]

    top_meta = [{"code": r.code, "label": r.label} for r in picked]
    top_series_full = {r.code: series_for(r.code, full_labels) for r in picked}

    # merge curated + top (dedupe by code)
    seen = set()
    cpi_sub_meta: list[dict] = []
    cpi_sub_series_full: dict[str, list[float | None]] = {}
    for meta_list, series_map in ((curated_meta, curated_series_full), (top_meta, top_series_full)):
        for m in meta_list:
            if m["code"] in seen:
                continue
            seen.add(m["code"])
            cpi_sub_meta.append(m)
            cpi_sub_series_full[m["code"]] = series_map[m["code"]]

    # movers table rows (curated first, then top picks)
    rows_by_code = {r.code: r for r in rows}
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

    seen = {d["code"] for d in curated_data}
    top_data = []
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
        # short window (homepage)
        labels=labels_24, values=values_24,
        # full history (detail page + range control)
        full_labels=full_labels, full_values=full_values,
        # forecast
        fut_labels=fut_labels, fut_values=fut_values,
        updated=updated,
        # sub-series (FULL history aligned to full_labels)
        cpi_sub_meta=cpi_sub_meta,
        cpi_sub_series=cpi_sub_series_full,
        # tables
        cpi_table=cpi_table,
        cpi_movers=cpi_movers,
    )

def build_cpi_subseries(label_list: List[str]) -> Tuple[List[dict], Dict[str, List[float]], List[dict]]:
    """
    Returns:
      - sub_meta:   [{code, label}, ...]
      - sub_series: { code: [values aligned to label_list], ... }
      - movers:     [{code, label, mom, yoy, d_mom, d_yoy}, ...] (latest month snapshot)
    """
    # 1) Pull source once
    src = fetch_cpi_data()

    def series_for(code: str, labels: List[str]) -> List[float]:
        df = get_isnr_series(src, code)  # expects columns date, value
        mapping = {d.strftime("%Y-%m"): float(v) for d, v in zip(df["date"], df["value"])}
        return [mapping.get(lbl) for lbl in labels]

    # 2) Curated set
    curated_meta   = [{"code": c, "label": isnr_label(c) or c} for c in CURATED_ISNR]
    curated_series = {c: series_for(c, label_list) for c in CURATED_ISNR}

    # 3) Top movers from DB (latest month)
    rows = []
    with Session(engine) as s:
        latest_date = s.scalar(select(func.max(CPISubMetric.date)))
        if latest_date:
            rows = s.scalars(
                select(CPISubMetric).where(CPISubMetric.date == latest_date)
            ).all()

    top_meta, top_series, picked = [], {}, []
    if rows:
        scored: List[tuple[float, CPISubMetric]] = []
        for r in rows:
            if r.code in CURATED_ISNR:
                continue
            score = (
                abs(r.delta_yoy_vs_total) if r.delta_yoy_vs_total is not None
                else (abs(r.delta_mom_vs_total) if r.delta_mom_vs_total is not None else None)
            )
            if score is not None:
                scored.append((score, r))

        scored.sort(key=lambda t: t[0], reverse=True)
        picked = [r for _, r in scored[:6]]

        top_meta = [{"code": r.code, "label": r.label or r.code} for r in picked]
        for r in picked:
            top_series[r.code] = series_for(r.code, label_list)

    # 4) Merge curated + top movers (dedupe by code)
    seen = set()
    sub_meta: List[dict] = []
    sub_series: Dict[str, List[float]] = {}

    for meta_list, series_map in ((curated_meta, curated_series), (top_meta, top_series)):
        for m in meta_list:
            code = m["code"]
            if code in seen:
                continue
            seen.add(code)
            sub_meta.append(m)
            sub_series[code] = series_map.get(code, series_for(code, label_list))

    # 5) Movers snapshot (if we had rows for latest month)
    movers: List[dict] = []
    if rows:
        rows_by_code = {r.code: r for r in rows}

        # curated first (only those present in this month)
        for code in CURATED_ISNR:
            r = rows_by_code.get(code)
            if not r:
                continue
            movers.append({
                "code": code,
                "label": r.label or code,
                "mom": r.mom, "yoy": r.yoy,
                "d_mom": r.delta_mom_vs_total, "d_yoy": r.delta_yoy_vs_total,
            })

        # then top picked (excluding already added)
        already = {m["code"] for m in movers}
        for r in picked:
            if r.code in already:
                continue
            movers.append({
                "code": r.code,
                "label": r.label or r.code,
                "mom": r.mom, "yoy": r.yoy,
                "d_mom": r.delta_mom_vs_total, "d_yoy": r.delta_yoy_vs_total,
            })

    return sub_meta, sub_series, movers

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

        # ---- FULL HISTORY (for range switcher) ----
        wages_full_labels = [a.date.strftime("%Y-%m") for a in w_actuals]
        wages_full_values = [a.index_value for a in w_actuals]

        # last 24 for initial/default small view (keep existing behavior)
        labels = wages_full_labels[-24:]
        values = wages_full_values[-24:]

        # latest forecast run that has points for this category
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

    wage_stats = _series_stats(values)
    w_future   = w_future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in w_future]
    fut_values = [p.predicted_index for p in w_future]
    updated    = wages_full_labels[-1] if wages_full_labels else "N/A"
    wage_table = _structured_change_table(values, fut_values, len(labels), len(fut_labels))

    # YoY gap vs TOTAL and identical check
    gap_vs_total = None
    identical_to_total = False
    if cat != "TOTAL":
        with Session(engine) as s2:
            tot = s2.scalars(
                select(WageActual).where(WageActual.category == "TOTAL").order_by(WageActual.date)
            ).all()
        total_vals = [a.index_value for a in tot[-len(values):]] if tot else None
        if total_vals:
            eps = 1e-6
            identical_to_total = all(
                (a is not None and b is not None and abs(a - b) < eps)
                for a, b in zip(values[-len(total_vals):], total_vals)
            )
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
        # FULL history for range switcher
        wages_full_labels=wages_full_labels,
        wages_full_values=wages_full_values,
        # 24m default view (kept for other uses)
        wages_labels=labels, wages_values=values,
        # forecast
        wages_fut_labels=fut_labels, wages_fut_values=fut_values,
        wages_updated=updated,
        # meta / stats
        wage_category=cat, wage_categories=cats,
        wage_stats=wage_stats,
        real_wage_yoy=real_wage_yoy,
        wage_gap_vs_total=gap_vs_total,
        wage_identical_to_total=identical_to_total,
        wage_table=wage_table,
    )

def _pick_best_cat(session, model_actual, preferred: Optional[str]):
    """Return a category that actually has rows. Prefer `preferred` if present,
    else the category with the most recent data, else the first alphabetically."""
    cats = session.scalars(
        select(model_actual.category).distinct().order_by(model_actual.category)
    ).all() or []

    if preferred and preferred in cats:
        return preferred, cats

    best = session.execute(
        select(model_actual.category, func.max(model_actual.date).label("m"))
        .group_by(model_actual.category)
        .order_by(func.max(model_actual.date).desc())
        .limit(1)
    ).first()
    if best:
        return best[0], cats

    return (cats[0] if cats else None), cats


def _bci_context(requested_cat: str | None):
    with Session(engine) as s:
        cat, cats = _pick_best_cat(s, BCIActual, preferred="BCI")
        if requested_cat and requested_cat in cats:
            cat = requested_cat

        actuals = s.scalars(
            select(BCIActual).where(BCIActual.category == cat).order_by(BCIActual.date)
        ).all()

        # ---- FULL HISTORY ----
        bci_full_labels = [a.date.strftime("%Y-%m") for a in actuals]
        bci_full_values = [a.index_value for a in actuals]

        # last 24 for initial/default
        labels = bci_full_labels[-24:]
        values = bci_full_values[-24:]

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

    # Build overlay sub-series (actuals only) for ALL categories, aligned to full labels
    with Session(engine) as s2:
        all_cats = s2.scalars(
            select(BCIActual.category).distinct().order_by(BCIActual.category)
        ).all()

        def series_for(cat_code: str):
            rows = s2.scalars(
                select(BCIActual).where(BCIActual.category == cat_code).order_by(BCIActual.date)
            ).all()
            mp = {r.date.strftime("%Y-%m"): r.index_value for r in rows}
            return [mp.get(lbl) for lbl in bci_full_labels]

        bci_sub_meta = [{"code": c, "label": c} for c in all_cats]
        bci_sub_series_full = {c: series_for(c) for c in all_cats}

    future     = future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in future]
    fut_values = [p.predicted_index for p in future]
    updated    = bci_full_labels[-1] if bci_full_labels else "N/A"

    return dict(
        bci_full_labels=bci_full_labels, bci_full_values=bci_full_values,
        bci_labels=labels, bci_values=values,
        bci_fut_labels=fut_labels, bci_fut_values=fut_values,
        bci_updated=updated,
        bci_category=cat, bci_categories=cats,
        bci_sub_meta=bci_sub_meta, bci_sub_series_full=bci_sub_series_full,
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

    full_labels = [a.date.strftime("%Y-%m") for a in actuals]
    full_values = [a.index_value for a in actuals]

    # keep 24m for quick summary if you need it elsewhere
    labels = full_labels[-24:]
    values = full_values[-24:]

    with Session(engine) as s2:
        all_cats = s2.scalars(
            select(PPIActual.category).distinct().order_by(PPIActual.category)
        ).all()

        def series_for(cat_code: str):
            rows = s2.scalars(
                select(PPIActual).where(PPIActual.category == cat_code).order_by(PPIActual.date)
            ).all()
            mp = {r.date.strftime("%Y-%m"): r.index_value for r in rows}
            return [mp.get(lbl) for lbl in full_labels]

        ppi_sub_meta = [{"code": c, "label": c} for c in all_cats]
        ppi_sub_series_full = {c: series_for(c) for c in all_cats}

    future     = future[:FORECAST_MONTHS]
    fut_labels = [p.date.strftime("%Y-%m") for p in future]
    fut_values = [p.predicted_index for p in future]
    updated    = full_labels[-1] if full_labels else "N/A"

    return dict(
        ppi_full_labels=full_labels, ppi_full_values=full_values,
        ppi_labels=labels, ppi_values=values,
        ppi_fut_labels=fut_labels, ppi_fut_values=fut_values,
        ppi_updated=updated,
        ppi_category=cat, ppi_categories=cats,
        ppi_sub_meta=ppi_sub_meta, ppi_sub_series_full=ppi_sub_series_full,
    )

# -----------------------------------------------------------------------------
# Flask app / routes
# -----------------------------------------------------------------------------
def create_app():
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.config["SITE_NAME"] = os.environ.get("SITE_NAME", "Efnahagur")

    @app.get("/health")
    def health():
        return {"ok": True}

    # Home: four cards (CPI, Wages, BCI, PPI)
    @app.get("/")
    def index():
        cpi_ctx = _cpi_context()
        wages_ctx = _wages_context(request.args.get("cat"))
        bci_ctx = _bci_context(None)
        ppi_ctx = _ppi_context(None)
        return render_template(
            "index.html",
            site_name=app.config["SITE_NAME"],
            **cpi_ctx, **wages_ctx, **bci_ctx, **ppi_ctx
        )

    # CPI detail (with sub-series)
    @app.get("/cpi")
    def cpi_page():
        ctx = _cpi_context()  # contains: full_labels/full_values, fut_*, cpi_sub_meta, cpi_sub_series (FULL), tables, movers
        return render_template(
            "cpi.html",
            site_name=app.config["SITE_NAME"],
            **ctx
        )

    # Wages detail
    @app.get("/wages")
    def wages_page():
        wages_ctx = _wages_context(request.args.get("cat"))
        return render_template(
            "wages.html",
            site_name=app.config["SITE_NAME"],
            **wages_ctx,
        )

    # BCI detail
    @app.get("/bci")
    def bci_page():
        ctx = _bci_context(request.args.get("cat"))
        return render_template("bci.html", site_name=app.config["SITE_NAME"], **ctx)

    # PPI detail
    @app.get("/ppi")
    def ppi_page():
        ctx = _ppi_context(request.args.get("cat"))
        return render_template("ppi.html", site_name=app.config["SITE_NAME"], **ctx)

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
