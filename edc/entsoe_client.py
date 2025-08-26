# edc/entsoe_client.py
from __future__ import annotations
import os, requests, xmltodict, re
from datetime import datetime, timedelta
import pytz
from tenacity import retry, wait_exponential, stop_after_attempt

BASE_URL = "https://web-api.tp.entsoe.eu/api"
TOKEN = os.environ.get("ENTSOE_API_TOKEN")

EIC_BY_ALPHA = {
    "ES": "10YES-REE------0",
    "PT": "10YPT-REN------W",
    "FR": "10YFR-RTE------C",
    "DE": "10Y1001A1001A83F",
    "IT": "10YIT-GRTN-----B",
}

BRUSSELS_TZ = pytz.timezone("Europe/Brussels")


def _normalize_zone(zone: str) -> str:
    z = zone.strip().upper()
    if z.startswith("10Y"):
        return z
    if z in EIC_BY_ALPHA:
        return EIC_BY_ALPHA[z]
    raise ValueError(
        f"Unknown bidding zone '{zone}'. Use an EIC code (10Y...) or one of {sorted(EIC_BY_ALPHA)}"
    )


class EntsoeClient:
    def __init__(self, token: str | None = None):
        self.token = token or TOKEN
        if not self.token:
            raise RuntimeError("ENTSOE_API_TOKEN missing")

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_after_attempt(5), reraise=True)
    def _get(self, params: dict) -> dict:
        full_params = {"securityToken": self.token, **params}
        r = requests.get(
            BASE_URL,
            params=full_params,
            timeout=60,
            headers={"Accept": "application/xml"},
        )
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            # surface API error body for debugging
            body = (r.text or "")[:2000]
            raise requests.HTTPError(f"{e} | body: {body}") from e
        return xmltodict.parse(r.text)

    @staticmethod
    def yyyymmddhhmm(dt: datetime) -> str:
        return dt.strftime("%Y%m%d%H%M")

    def _utc_to_brussels(self, dt_utc: datetime) -> datetime:
        if dt_utc.tzinfo is None:
            dt_utc = pytz.UTC.localize(dt_utc)
        return dt_utc.astimezone(BRUSSELS_TZ)

    def _snap_daily_local_midnight(self, start_utc: datetime, end_utc: datetime) -> tuple[datetime, datetime]:
        """
        For A44 (day-ahead), ENTSO-E expects local CET/CEST midnights.
        We snap the requested UTC window to [local_midnight, next_local_midnight).
        """
        start_loc = self._utc_to_brussels(start_utc)
        # take the LOCAL date and set to 00:00 local
        local_midnight = BRUSSELS_TZ.localize(
            datetime(start_loc.year, start_loc.month, start_loc.day, 0, 0)
        )
        return local_midnight, local_midnight + timedelta(days=1)

    def _expected_utc_range_for_local_day(self, start_ce_local: datetime, end_ce_local: datetime) -> list[datetime]:
        """
        Build the expected sequence of UTC datetimes corresponding to each local
        hour in [start_ce_local, end_ce_local). Length is 24 normally, 23/25 on DST days.
        """
        assert start_ce_local.tzinfo is not None and end_ce_local.tzinfo is not None
        total_hours = int((end_ce_local - start_ce_local).total_seconds() // 3600)
        expected_utc = []
        for h in range(total_hours):
            dt_local = start_ce_local + timedelta(hours=h)
            expected_utc.append(dt_local.astimezone(pytz.UTC))
        return expected_utc

    def day_ahead_prices(self, bidding_zone: str, start_utc: datetime, end_utc: datetime) -> list[dict]:
        """
        Day-ahead Prices (documentType=A44)

        - Accepts UTC window, snaps to the corresponding *local Brussels* day.
        - Queries with in_Domain=out_Domain=<zone EIC>.
        - Returns rows strictly on the local-day grid, **in UTC**:
            [{"ts_utc": ISO8601, "price_eur_mwh": float|None, "area": <EIC>}]
          guaranteeing 24 rows (or 23/25 on DST days). Missing hours -> price None.
        """
        bz = _normalize_zone(bidding_zone)

        # Snap to local (Brussels) day
        start_ce, end_ce = self._snap_daily_local_midnight(start_utc, end_utc)

        params = dict(
            documentType="A44",
            # processType is optional for A44; keep or drop. Keeping is harmless, but we omit for clarity.
            in_Domain=bz,
            out_Domain=bz,
            periodStart=self.yyyymmddhhmm(start_ce),
            periodEnd=self.yyyymmddhhmm(end_ce),
        )

        doc = self._get(params)

        # --- Parse all points into a map {utc_datetime: price} -----------------
        price_by_utc: dict[datetime, float] = {}
        parsed_area = bz  # fallback

        try:
            tss = doc["Publication_MarketDocument"]["TimeSeries"]
            if isinstance(tss, dict):
                tss = [tss]

            for ts in tss:
                # Try to read area back from payload; fall back to requested bz.
                parsed_area = (
                    (isinstance(ts.get("outBiddingZone_Domain"), dict) and ts["outBiddingZone_Domain"].get("@v"))
                    or (isinstance(ts.get("inBiddingZone_Domain"), dict) and ts["inBiddingZone_Domain"].get("@v"))
                    or (isinstance(ts.get("out_Domain"), dict) and ts["out_Domain"].get("@v"))
                    or (isinstance(ts.get("in_Domain"), dict) and ts["in_Domain"].get("@v"))
                    or bz
                )

                periods = ts.get("Period", [])
                if isinstance(periods, dict):
                    periods = [periods]

                for p in periods:
                    # Resolution is typically PT60M for A44, but we infer start+position anyway.
                    res = p.get("resolution", "PT60M")
                    m = re.match(r"PT(\d+)M", res or "")
                    step_min = int(m.group(1)) if m else 60

                    start_iso = datetime.fromisoformat(
                        p["timeInterval"]["start"].replace("Z", "+00:00")
                    )

                    pts = p.get("Point", [])
                    if isinstance(pts, dict):
                        pts = [pts]

                    for pt in pts:
                        pos = int(pt["position"])
                        price_node = pt.get("price") or pt.get("price.amount")
                        price = float(price_node["#text"] if isinstance(price_node, dict) else price_node)
                        ts_utc = start_iso + timedelta(minutes=step_min * (pos - 1))
                        # Keep last-seen if duplicates
                        price_by_utc[ts_utc.replace(tzinfo=pytz.UTC)] = price
        except KeyError:
            # If structure doesn't match expected, leave map empty; reindexing below will yield None values.
            pass

        # --- Reindex onto the *local-day* hourly grid, expressed in UTC -------
        expected_utc = self._expected_utc_range_for_local_day(start_ce, end_ce)

        rows: list[dict] = []
        for ts in expected_utc:
            price = price_by_utc.get(ts, None)
            rows.append(
                {
                    "ts_utc": ts.isoformat(),            # UTC, tz-aware
                    "price_eur_mwh": price,              # None if missing
                    "area": parsed_area,
                }
            )

        return rows
