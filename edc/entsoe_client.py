from __future__ import annotations
import os, requests, xmltodict
from datetime import datetime
from tenacity import retry, wait_exponential, stop_after_attempt

BASE_URL = "https://web-api.tp.entsoe.eu/api"
TOKEN = os.environ.get("ENTSOE_API_TOKEN")

# Minimal mapping for convenience (BZN codes)
EIC_BY_ALPHA = {
    "ES": "10YES-REE------0",  # Spain
    "PT": "10YPT-REN------W",  # Portugal
    "FR": "10YFR-RTE------C",  # France
    "DE": "10Y1001A1001A83F",  # Germany (BZN Germany-Lux)
    "IT": "10YIT-GRTN-----B",  # Italy (BZN IT)
}

def _normalize_zone(zone: str) -> str:
    z = zone.strip().upper()
    # allow either raw EIC (10Y...) or 2-letter shorthand
    if z.startswith("10Y"):
        return z
    if z in EIC_BY_ALPHA:
        return EIC_BY_ALPHA[z]
    raise ValueError(f"Unknown bidding zone '{zone}'. "
                     f"Use an EIC code (starts with 10Y...) or one of {sorted(EIC_BY_ALPHA)}")

class EntsoeClient:
    def __init__(self, token: str | None = None):
        self.token = token or TOKEN
        if not self.token:
            raise RuntimeError("ENTSOE_API_TOKEN missing")

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_after_attempt(5))
    def _get(self, params: dict) -> dict:
        # IMPORTANT: token must be passed as 'securityToken' in GET
        full_params = {"securityToken": self.token, **params}
        r = requests.get(BASE_URL, params=full_params, timeout=60, headers={"Accept": "application/xml"})
        r.raise_for_status()
        return xmltodict.parse(r.text)

    @staticmethod
    def yyyymmddhhmm(dt: datetime) -> str:
        return dt.strftime("%Y%m%d%H%M")

    def day_ahead_prices(self, bidding_zone: str, start_utc: datetime, end_utc: datetime) -> list[dict]:
        """
        A44 Price Document (Day‑ahead prices).
        ENTSO‑E requires EIC codes for in_Domain/out_Domain. We accept 'ES','PT','FR', etc. and map to EIC.
        """
        bz = _normalize_zone(bidding_zone)
        params = dict(
            documentType="A44",
            processType="A01",               # add this
            in_Domain=bz,
            out_Domain=bz,
            periodStart=self.yyyymmddhhmm(start_utc),
            periodEnd=self.yyyymmddhhmm(end_utc),
        )

        doc = self._get(params)
        series = []
        try:
            timeseries = doc["Publication_MarketDocument"]["TimeSeries"]
            if isinstance(timeseries, dict):
                timeseries = [timeseries]
            for ts in timeseries:
                area = ts["in_Domain"]["@v"]
                period = ts["Period"]
                if isinstance(period, dict):
                    period = [period]
                from datetime import timedelta
                import re
                for p in period:
                    res = p["resolution"]  # e.g., PT60M
                    interval_start = p["timeInterval"]["start"]
                    interval = datetime.fromisoformat(interval_start.replace("Z","+00:00"))
                    m = re.match(r"PT(\d+)M", res)
                    minutes = int(m.group(1)) if m else 60
                    for pt in p["Point"]:
                        pos = int(pt["position"])
                        price = float(pt["price"]["#text"])
                        ts_utc = interval + timedelta(minutes=minutes*(pos-1))
                        series.append({"ts_utc": ts_utc.isoformat(), "price_eur_mwh": price, "area": area})
        except KeyError:
            pass
        return series
