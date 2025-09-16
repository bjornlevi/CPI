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
