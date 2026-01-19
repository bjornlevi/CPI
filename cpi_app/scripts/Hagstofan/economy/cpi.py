from ..base_data_source import BaseDataSource
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .isnr_labels import ISNRLabels
import re
import statistics
from requests.exceptions import HTTPError

class CPI(BaseDataSource):
    def __init__(self, client, endpoint: str | None = None, weight_endpoint: str | None = None):
        endpoint = endpoint or 'is/Efnahagur/visitolur/1_vnv/2_undirvisitolur/VIS01302.px'
        weight_endpoint = weight_endpoint or 'is/Efnahagur/visitolur/1_vnv/2_undirvisitolur/VIS01306.px'
        super().__init__(client, endpoint)

        index_var_code = "Liður"
        index_code = "index_B1997"
        body = {
            "query": [
                {
                    "code": index_var_code,
                    "selection": {
                        "filter": "item",
                        "values": [index_code]
                    }
                }
            ],
            "response": {
                "format": "json"
            }
        }
        try:
            raw_data = self.get_data(body)
        except HTTPError as exc:
            raw_data = None
            selector = self._discover_index_selector(client)
            if selector:
                discovered_var_code, discovered_value = selector
                if discovered_var_code and discovered_value:
                    if not (discovered_var_code == index_var_code and discovered_value == index_code):
                        body["query"][0]["code"] = discovered_var_code
                        body["query"][0]["selection"]["values"] = [discovered_value]
                        try:
                            raw_data = self.get_data(body)
                        except HTTPError:
                            raw_data = self._fetch_with_meta_query(client)
                    else:
                        raw_data = self._fetch_with_meta_query(client)
                else:
                    raw_data = self._fetch_with_meta_query(client)
            else:
                raw_data = self._fetch_with_meta_query(client)

            if raw_data is None:
                raise exc

        if not raw_data.get("data"):
            raw_data = self._fetch_with_meta_query(client) or raw_data

        self.raw_data = raw_data
        self.index = {}  # {(date, isnr): value}
        self.isnr_values = set()

        for entry in raw_data.get("data", []):
            key = entry.get("key", [])
            if not key:
                continue
            date_str = next((k for k in key if re.match(r"^\d{4}M\d{2}$", k)), None)
            isnr_value = next((k for k in key if re.match(r"^(IS|CP)\d+$", k)), None)
            if not date_str or not isnr_value:
                continue
            try:
                value = float(entry["values"][0])
            except (ValueError, IndexError, TypeError):
                continue

            self.index[(date_str, isnr_value)] = value
            self.isnr_values.add(isnr_value)

        # Load weight data from the secondary source
        self.weights = {}  # {(date, isnr): weight}
        if weight_endpoint:
            weight_source = BaseDataSource(client, weight_endpoint)
            weight_body = {
                "query": [],
                "response": {
                    "format": "json"
                }
            }
            raw_weights = weight_source.get_data(weight_body)
            for entry in raw_weights.get("data", []):
                key = entry.get("key", [])
                if not key:
                    continue
                date_str = next((k for k in key if re.match(r"^\d{4}M\d{2}$", k)), None)
                isnr_value = next((k for k in key if re.match(r"^(IS|CP)\d+$", k)), None)
                if not date_str or not isnr_value:
                    continue
                try:
                    value = float(entry["values"][0])
                except (ValueError, IndexError, TypeError):
                    continue
                self.weights[(date_str, isnr_value)] = value

    def get_current(self, is_nr: str):
        dates = [d for (d, i) in self.index if i == is_nr]
        if not dates:
            return {"error": f"No data found for ISO '{is_nr}'"}
        latest = max(dates)
        return {"month": latest, "value": self.index.get((latest, is_nr))}

    def get_12_month_change(self, is_nr: str):
        dates = [d for (d, i) in self.index if i == is_nr]
        if not dates:
            return {"error": f"No data found for IS_NR '{is_nr}'"}

        latest_month_str = max(dates)
        try:
            latest_date = datetime.strptime(latest_month_str, "%YM%m")
        except ValueError:
            return {"error": "Invalid date format."}

        previous_date = latest_date - relativedelta(months=12)
        previous_month_str = previous_date.strftime("%YM%m")

        latest_value = self.index.get((latest_month_str, is_nr))
        previous_value = self.index.get((previous_month_str, is_nr))

        if latest_value is None or previous_value is None:
            return {"error": "Insufficient data for 12-month comparison."}

        change = ((latest_value - previous_value) / previous_value) * 100
        return {
            "from": previous_month_str,
            "to": latest_month_str,
            "change_percent": round(change, 2)
        }

    def get_cpi(self):
        return self.get_12_month_change("IS00")

    def list_is_nr_values(self):
        return sorted(self.isnr_values)

    def get_value_for(self, year_month: str, is_nr: str):
        value = self.index.get((year_month, is_nr))
        if value is None:
            return {"error": f"No value found for {year_month} and IS_NR '{is_nr}'"}
        return value

    def get_label_for_is_nr(self, is_nr: str):
        return ISNRLabels.get(is_nr)

    def get_weight(self, year_month: str, is_nr: str):
        """
        Returns the weight of the given ISNR for the specified year and month.

        Args:
            year_month (str): The date in format "YYYYMmm", e.g., "2024M01".
            is_nr (str): The ISNR code, e.g., "IS0112".

        Returns:
            float: The weight value.

        Raises:
            ValueError: If no weight data is found for the specified combination.
        """
        try:
            return self.weights[(year_month, is_nr)]
        except KeyError:
            return None

    def get_increase_over_months(self, n_months: int):
        """
        Calculates the % increase in CPI value over the past n_months for each ISNR.

        Args:
            n_months (int): Number of months back to calculate change from.

        Returns:
            dict: Mapping from ISNR to % change (float), or error message if data is missing.
        """
        result = {}
        for isnr in self.isnr_values:
            dates = [d for (d, i) in self.index if i == isnr]
            if not dates:
                continue

            latest_date_str = max(dates)
            try:
                latest_date = datetime.strptime(latest_date_str, "%YM%m")
            except ValueError:
                continue

            prev_date = latest_date - relativedelta(months=n_months)
            prev_date_str = prev_date.strftime("%YM%m")

            latest_val = self.index.get((latest_date_str, isnr))
            prev_val = self.index.get((prev_date_str, isnr))

            if latest_val is not None and prev_val is not None and prev_val != 0:
                change = ((latest_val - prev_val) / prev_val) * 100
                result[isnr] = round(change, 2)

        return result

    def _discover_index_selector(self, client):
        try:
            meta = client.get(self.endpoint)
        except Exception:
            return None

        variables = meta.get("variables", [])
        candidates = []

        for var in variables:
            values = var.get("values") or []
            for value in values:
                match = re.search(r"index_B(\d{4})", value, re.IGNORECASE)
                if match:
                    candidates.append((int(match.group(1)), var.get("code"), value))

        if not candidates:
            for var in variables:
                code = var.get("code", "")
                text = var.get("text", "")
                if re.search(r"lið|liður|lidur", code, re.IGNORECASE) or re.search(r"lið|liður|lidur", text, re.IGNORECASE):
                    values = var.get("values") or []
                    texts = var.get("valueTexts") or []
                    for idx, value in enumerate(values):
                        text = texts[idx] if idx < len(texts) else ""
                        if re.search(r"index", value, re.IGNORECASE) or re.search(r"index|vísitala", text, re.IGNORECASE):
                            match = re.search(r"index_B(\d{4})", value, re.IGNORECASE)
                            year = int(match.group(1)) if match else 0
                            candidates.append((year, var.get("code"), value))

        if not candidates:
            return None

        candidates.sort()
        _year, var_code, value = candidates[-1]
        return var_code, value

    def _fetch_with_meta_query(self, client):
        try:
            data = self.get_data({"query": [], "response": {"format": "json"}})
            if data and data.get("data"):
                return data
        except HTTPError:
            pass

        body = self._build_query_from_meta(client, use_all_wildcard=True)
        if not body:
            return None
        try:
            return self.get_data(body)
        except HTTPError:
            body = self._build_query_from_meta(client, use_all_wildcard=False)
            if not body:
                return None
            return self.get_data(body)

    def _build_query_from_meta(self, client, use_all_wildcard: bool):
        try:
            meta = client.get(self.endpoint)
        except Exception:
            return None

        variables = meta.get("variables", [])
        if not variables:
            return None

        query = []
        for var in variables:
            code = var.get("code")
            values = var.get("values") or []
            if not code or not values:
                continue

            selection = self._selection_for_variable(values, use_all_wildcard)
            query.append({"code": code, "selection": selection})

        if not query:
            return None

        return {"query": query, "response": {"format": "json"}}

    def _selection_for_variable(self, values, use_all_wildcard: bool):
        index_value = self._latest_index_value(values)
        if index_value:
            return {"filter": "item", "values": [index_value]}

        if any(re.match(r"^IS\d+$", v) for v in values):
            if use_all_wildcard:
                return {"filter": "all", "values": ["*"]}
            return {"filter": "item", "values": values}

        if any(re.match(r"^\d{4}M\d{2}$", v) for v in values):
            if use_all_wildcard:
                return {"filter": "all", "values": ["*"]}
            return {"filter": "item", "values": values}

        if any(str(v).lower() == "index" for v in values):
            return {"filter": "item", "values": ["index"]}

        return {"filter": "item", "values": [values[0]]}

    def _latest_index_value(self, values):
        candidates = []
        for value in values:
            match = re.search(r"index_B(\d{4})", str(value), re.IGNORECASE)
            if match:
                candidates.append((int(match.group(1)), value))
        if not candidates:
            return None
        candidates.sort()
        return candidates[-1][1]

    def get_average_and_median_change(self, is_nr: str, n_months: int):
        """
        Computes average and median monthly % change for a given ISNR over the past n_months.

        Args:
            is_nr (str): The ISNR code to compute stats for.
            n_months (int): Number of recent months to include.

        Returns:
            dict: {"average": float, "median": float} or {"error": str}
        """
        dates = sorted([d for (d, i) in self.index if i == is_nr], reverse=True)
        if len(dates) < n_months + 1:
            return {"error": f"Not enough data for ISNR '{is_nr}'"}

        percent_changes = []
        for i in range(n_months):
            d1_str, d2_str = dates[i + 1], dates[i]
            val1 = self.index.get((d1_str, is_nr))
            val2 = self.index.get((d2_str, is_nr))
            if val1 is not None and val2 is not None and val1 != 0:
                pct_change = ((val2 - val1) / val1) * 100
                percent_changes.append(pct_change)

        if not percent_changes:
            return {"error": f"No valid change data for ISNR '{is_nr}'"}

        return {
            "average": round(statistics.mean(percent_changes), 2),
            "median": round(statistics.median(percent_changes), 2)
        }

    def __str__(self):
        total_items = len(self.index)
        unique_isnr = len(self.isnr_values)
        return f"CPI Data Source with {total_items} entries across {unique_isnr} unique ISNR codes."
