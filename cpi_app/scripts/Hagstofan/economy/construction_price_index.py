from ..base_data_source import BaseDataSource
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

class ConstructionPriceIndex(BaseDataSource):
    def __init__(self, client):
        super().__init__(client, 'is/Efnahagur/visitolur/2_byggingarvisitala/byggingarvisitala/VIS13302.px')

        body = {
            "query": [
                {
                    "code": "Liður",
                    "selection": {
                        "filter": "item",
                        "values": ["index"]
                    }
                }
            ],
            "response": {
                "format": "json"
            }
        }

        raw_data = self.get_data(body)

        self.index = {}  # {(date, category): value}
        self.categories = set()

        self.category_labels = {
            "Metal": "Blikk- og járnsmíði",
            "Carp": "Húsasmíði",
            "Carp_mat": "Húsasmíði, efnishluti",
            "Carp_lab": "Húsasmíði, vinnuhluti",
            "Concret": "Múrverk",
            "Design": "Hönnun",
            "Floor": "Lagning gólfefna",
            "Paint": "Málun",
            "Brick": "Múrverk",
            "Plumb": "Pípulögn",
            "Elec": "Raflögn",
            "Elec_mat": "Raflögn, efnishluti",
            "Elec_lab": "Raflögn, vinnuhluti",
            "Mach": "Vélavinna, akstur, uppfylling",
            "Man": "Verkstjórn, eftirlit, verkamannavinna",
            "BCI": "Vísitala byggingarkostnaðar",
            "DesCost": "Vísitala hönnunarkostnaðar"
        }

        for entry in raw_data.get("data", []):
            key = entry.get("key", [])
            if len(key) < 3:
                continue
            date_str, _, category = key
            try:
                value = float(entry["values"][0])
            except (ValueError, IndexError):
                continue
            self.index[(date_str, category)] = value
            self.categories.add(category)

    def get_label_for_category(self, category: str) -> str:
        """
        Returns the label for a given construction category.

        Args:
            category (str): Category code such as 'Carp', 'Super', etc.

        Returns:
            str: Human-readable label or fallback to the category code.
        """
        return self.category_labels.get(category, category)

    def list_categories(self):
        return sorted(self.categories)

    def get_value_for(self, year_month: str, category: str):
        value = self.index.get((year_month, category))
        if value is None:
            return {"error": f"No value found for {year_month} and category '{category}'"}
        return value

    def get_historical_values(self, category: str, months: int = 12):
        """
        Returns the last X months of values for a given category.
        Returns a list of (month_str, value) tuples.
        """
        dates = sorted([d for (d, c) in self.index if c == category])
        if len(dates) == 0:
            return []
        recent_dates = dates[-months:]
        return [(d, self.index.get((d, category))) for d in recent_dates if self.index.get((d, category)) is not None]


    def __str__(self):
        return f"Construction Price Index with {len(self.index)} entries across {len(self.categories)} categories."
