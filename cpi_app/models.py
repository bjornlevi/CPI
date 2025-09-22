import os
from sqlalchemy import create_engine, Column, Integer, Float, String, Date, DateTime, ForeignKey, UniqueConstraint, select
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_FILE = os.environ.get("CPI_DB", os.path.join(DATA_DIR, "cpi.sqlite"))
engine = create_engine(f"sqlite:///{DB_FILE}", future=True)
SessionLocal = sessionmaker(bind=engine, future=True)
Base = declarative_base()

# --- CPI ---
class CPIActual(Base):
    __tablename__ = "cpi_actuals"
    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True, index=True, nullable=False)
    cpi = Column(Float, nullable=False)
    monthly_change = Column(Float)

class ForecastRun(Base):
    __tablename__ = "forecast_runs"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    months_predict = Column(Integer)
    notes = Column(String)

class ForecastPoint(Base):
    __tablename__ = "forecast_points"
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("forecast_runs.id"), nullable=False)
    date = Column(Date, nullable=False)
    predicted_cpi = Column(Float, nullable=False)

class CPISubMetric(Base):
    __tablename__ = "cpi_sub_metrics"
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True, nullable=False)          # month (e.g. 2025-08-01)
    code = Column(String(16), index=True, nullable=False)    # IS011, IS041, ...
    label = Column(String(128), nullable=False)

    value = Column(Float)             # index level for that month
    mom = Column(Float)               # % vs previous month (same code)
    yoy = Column(Float)               # % vs same month a year earlier (same code)

    delta_mom_vs_total = Column(Float)  # mom - total_cpi_mom
    delta_yoy_vs_total = Column(Float)  # yoy - total_cpi_yoy

    __table_args__ = (
        UniqueConstraint("date", "code", name="uq_cpi_sub_metric_date_code"),
    )

# --- Wages ---
class WageActual(Base):
    __tablename__ = "wage_actuals"
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True, nullable=False)
    category = Column(String(16), index=True, nullable=False)  # e.g. TOTAL
    index_value = Column(Float, nullable=False)
    __table_args__ = (UniqueConstraint("date", "category", name="uq_wage_date_cat"),)

class WageForecastRun(Base):
    __tablename__ = "wage_forecast_runs"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    months_predict = Column(Integer)
    notes = Column(String)

class WageForecastPoint(Base):
    __tablename__ = "wage_forecast_points"
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("wage_forecast_runs.id"), nullable=False)
    date = Column(Date, nullable=False)
    category = Column(String(16), nullable=False)
    predicted_index = Column(Float, nullable=False)

# --- Construction Price Index (BCI) ---
class BCIActual(Base):
    __tablename__ = "bci_actuals"
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True, nullable=False)
    category = Column(String(32), index=True, nullable=False, default="BCI")
    index_value = Column(Float, nullable=False)
    __table_args__ = (UniqueConstraint("date", "category", name="uq_bci_actual"),)

class BCIForecastRun(Base):
    __tablename__ = "bci_forecast_runs"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    months_predict = Column(Integer, nullable=False)
    notes = Column(String(200))

class BCIForecastPoint(Base):
    __tablename__ = "bci_forecast_points"
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("bci_forecast_runs.id", ondelete="CASCADE"), index=True, nullable=False)
    date = Column(Date, index=True, nullable=False)
    category = Column(String(32), index=True, nullable=False, default="BCI")
    predicted_index = Column(Float, nullable=False)

# --- Production Price Index (PPI) ---
class PPIActual(Base):
    __tablename__ = "ppi_actuals"
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True, nullable=False)
    category = Column(String(32), index=True, nullable=False, default="PPI")
    index_value = Column(Float, nullable=False)
    __table_args__ = (UniqueConstraint("date", "category", name="uq_ppi_actual"),)

class PPIForecastRun(Base):
    __tablename__ = "ppi_forecast_runs"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    months_predict = Column(Integer, nullable=False)
    notes = Column(String(200))

class PPIForecastPoint(Base):
    __tablename__ = "ppi_forecast_points"
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ppi_forecast_runs.id", ondelete="CASCADE"), index=True, nullable=False)
    date = Column(Date, index=True, nullable=False)
    category = Column(String(32), index=True, nullable=False, default="PPI")
    predicted_index = Column(Float, nullable=False)
