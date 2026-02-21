from ..base_data_source import BaseDataSource
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .isnr_labels import ISNRLabels
import re
import statistics
from requests.exceptions import HTTPError


class CPI(BaseDataSource):
    def __init__(self, client, endpoint: str | None = None, weight_endpoint: str | None = None):
        endpoint = endpoint or "is/Efnahagur/visitolur/1_vnv/2_undirvisitolur/VIS01300.px"
        weight_endpoint = weight_endpoint or "is/Efnahagur/visitolur/1_vnv/2_undirvisitolur/VIS01306.px"
        super().__init__(client, endpoint)

        raw_data = self._fetch_with_meta_query(client)
        if raw_data is None:
            raise HTTPError(f"Failed to load CPI data from {self.endpoint}")

        self.raw_data = raw_data
        self.index = {}  # {(date, isnr): value}
        self.isnr_values = set()

        for entry in raw_data.get("data", []):
            key = entry.get("key", [])
            if not key:
                continue
            date_str = next((k for k in key if re.match(r"^\d{4}M\d{2}$", str(k))), None)
            code_value = next((k for k in key if re.match(r"^(IS|CP)\d+$", str(k)) or k == "CPI"), None)
            if not date_str or not code_value:
                continue
            isnr_value = "IS" + code_value[2:] if str(code_value).startswith("CP") else str(code_value)
            if not re.match(r"^(IS|CP)\d+$", isnr_value):
                continue
            try:
                value = float(entry["values"][0])
            except (ValueError, IndexError, TypeError):
                continue

            self.index[(date_str, isnr_value)] = value
            self.isnr_values.add(isnr_value)

        # Pull headline CPI and rebase to previous month = 100.
        headline_raw = self._fetch_from_endpoint(
            client, "is/Efnahagur/visitolur/1_vnv/1_vnv/VIS01000.px", use_all_wildcard=True
        )
        headline = []
        for entry in (headline_raw or {}).get("data", []):
            key = entry.get("key", [])
            if len(key) < 2:
                continue
            if not any(str(item).lower() == "index" for item in key):
                continue
            if not any(item in ("CPI", "CP00", "IS00") for item in key):
                continue

            date_str = next((k for k in key if re.match(r"^\d{4}M\d{2}$", str(k))), None)
            if not date_str:
                continue
            try:
                val = float(entry["values"][0])
            except (ValueError, IndexError, TypeError):
                continue
            headline.append((date_str, val))

        headline.sort(key=lambda t: t[0])
        if headline:
            base_val = headline[-2][1] if len(headline) >= 2 else headline[-1][1]
            for ym, val in headline:
                rebased = val / base_val * 100.0 if base_val else val
                self.index[(ym, "IS00")] = rebased
            self.isnr_values.add("IS00")

        # Load weight data from the secondary source.
        self.weights = {}  # {(date, isnr): weight}
        if weight_endpoint:
            raw_weights = self._fetch_from_endpoint(client, weight_endpoint, use_all_wildcard=True)
            for entry in (raw_weights or {}).get("data", []):
                key = entry.get("key", [])
                if len(key) < 2:
                    continue
                code_value = next((k for k in key if re.match(r"^(IS|CP)\d+$", str(k)) or k == "CPI"), None)
                date_str = next((k for k in key if re.match(r"^\d{4}M\d{2}$", str(k))), None)
                if not date_str or not code_value:
                    continue
                isnr_value = "IS" + code_value[2:] if str(code_value).startswith("CP") else str(code_value)
                if not re.match(r"^(IS|CP)\d+$", isnr_value):
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
            "change_percent": round(change, 2),
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
        try:
            return self.weights[(year_month, is_nr)]
        except KeyError:
            return None

    def get_increase_over_months(self, n_months: int):
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

    def _fetch_with_meta_query(self, client):
        return self._fetch_from_endpoint(client, self.endpoint, use_all_wildcard=True)

    def _fetch_from_endpoint(self, client, endpoint: str, use_all_wildcard: bool):
        body = self._build_query_from_endpoint(client, endpoint, use_all_wildcard)
        if not body:
            return None
        try:
            return client.post(endpoint, body)
        except HTTPError:
            if use_all_wildcard:
                fallback = self._build_query_from_endpoint(client, endpoint, use_all_wildcard=False)
                if fallback:
                    return client.post(endpoint, fallback)
            return None

    def _build_query_from_meta(self, client, use_all_wildcard: bool):
        return self._build_query_from_endpoint(client, self.endpoint, use_all_wildcard)

    def _build_query_from_endpoint(self, client, endpoint: str, use_all_wildcard: bool):
        try:
            meta = client.get(endpoint)
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
        if any(str(v).lower() == "index" for v in values):
            return {"filter": "item", "values": [next(v for v in values if str(v).lower() == "index")]}

        if any(re.match(r"^(IS|CP)\d+$", str(v)) for v in values):
            if use_all_wildcard:
                return {"filter": "all", "values": ["*"]}
            return {"filter": "item", "values": values}

        if any(self._is_time_value(v) for v in values):
            if use_all_wildcard:
                return {"filter": "all", "values": ["*"]}
            return {"filter": "item", "values": values}

        latest_index = self._latest_index_value(values)
        if latest_index:
            return {"filter": "item", "values": [latest_index]}

        return {"filter": "item", "values": [values[0]]}

    def _is_time_value(self, value):
        return bool(re.match(r"^\d{4}M\d{2}$", str(value)))

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
            "median": round(statistics.median(percent_changes), 2),
        }

    def __str__(self):
        total_items = len(self.index)
        unique_isnr = len(self.isnr_values)
        return f"CPI Data Source with {total_items} entries across {unique_isnr} unique ISNR codes."
