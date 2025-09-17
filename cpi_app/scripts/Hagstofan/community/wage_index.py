# cpi_app/scripts/Hagstofan/economy/wages.py
from __future__ import annotations
from ..base_data_source import BaseDataSource
from datetime import datetime
import re
from typing import Dict, Iterable, List, Optional, Tuple

MONTH_RE = re.compile(r"^\d{4}M\d{2}$")

class WageIndex(BaseDataSource):
    """
    Wages index (Launavísitala) via LAU04000.px.
    Stores a level index (Eining = 'index') for all months and categories found.

    Internal store:
      self.index[(month_str, category)] = float_value
      self.categories = set([...])

    Category is inferred from non-month dimensions in the PX 'key'.
    If multiple non-month dims exist, they are joined with ':' (e.g. 'TOTAL:MEN').
    """

    def __init__(self, client):
        super().__init__(client, "is/Samfelag/launogtekjur/2_lvt/1_manadartolur/LAU04000.px")

        # Fetch *all* months/categories but restrict to Eining=index
        body = {
            "query": [
                {
                    "code": "Eining",
                    "selection": { "filter": "item", "values": ["index"] }
                }
                # Intentionally do NOT filter other dimensions: we want everything
            ],
            "response": { "format": "json" }
        }

        raw = self.get_data(body)

        self.index: Dict[Tuple[str, str], float] = {}
        self.categories: set[str] = set()

        for entry in raw.get("data", []):
            keys: List[str] = entry.get("key", [])
            vals: List[str] = entry.get("values", [])
            if not keys or not vals:
                continue
            # find the month position
            month_idx = next((i for i, k in enumerate(keys) if MONTH_RE.match(k)), None)
            if month_idx is None:
                # sometimes month can be the *first* dimension named 'Mánuður' but we match by value anyway
                continue
            date_str = keys[month_idx]
            # category is every other dimension (joined). Try to keep common codes ('TOTAL','ALM',...) if present.
            non_month = [k for i, k in enumerate(keys) if i != month_idx]

            # Prefer a single "nice" category if it looks like a known code
            nice = next((k for k in non_month if k in {"TOTAL", "ALM", "OPI", "OPI_R", "OPI_L"}), None)
            category = nice or (non_month[0] if len(non_month) == 1 else ":".join(non_month)) or "TOTAL"

            try:
                val = float(vals[0])
            except (TypeError, ValueError, IndexError):
                continue

            self.index[(date_str, category)] = val
            self.categories.add(category)

    # ------------ Convenience API ------------

    def list_categories(self) -> List[str]:
        return sorted(self.categories)

    def months(self) -> List[str]:
        """All YYYYMmm that appear in the data, sorted."""
        return sorted({m for (m, _c) in self.index.keys()})

    def get_series(self, category: str) -> List[Tuple[datetime, float]]:
        """List of (datetime, value) for one category, sorted by month."""
        rows: List[Tuple[datetime, float]] = []
        for (m, c), v in self.index.items():
            if c != category:
                continue
            try:
                dt = datetime.strptime(m, "%YM%m")
            except ValueError:
                continue
            rows.append((dt, float(v)))
        rows.sort(key=lambda t: t[0])
        return rows

    def latest(self, category: str) -> Optional[Tuple[str, float]]:
        """('YYYYMmm', value) for most recent month in this category."""
        ms = [m for (m, c) in self.index.keys() if c == category]
        if not ms:
            return None
        last = max(ms)
        return last, self.index[(last, category)]

    def as_pandas(self, category: str):
        """Return a pandas.DataFrame with columns ['date','value'] for a category."""
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("pandas is required for as_pandas()")
        data = self.get_series(category)
        return pd.DataFrame(data, columns=["date", "value"])
