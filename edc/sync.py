# edc/sync.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Dict, List, Optional
from pathlib import Path
from datetime import datetime, timedelta
import os

import pandas as pd
import pytz

from .entsoe_client import EntsoeClient, _normalize_zone
from . import io_local  # <-- use centralized I/O helpers

BRUSSELS_TZ = pytz.timezone("Europe/Brussels")

@dataclass
class SyncConfig:
    std_root: Path                     # e.g. Path("data/standard")
    series_name: str = "entsoe_day_ahead_price"
    start_ymd: str = "2020-01-01"
    area_files_prefix: str = ""

    def path_for_area(self, area_eic: str) -> Path:
        fname = f"{self.area_files_prefix}{self.series_name}_{area_eic}.parquet"
        return self.std_root / fname

def _expected_utc_grid_for_local_day(local_day: datetime) -> List[datetime]:
    assert local_day.tzinfo is not None
    start = local_day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    total_hours = int((end - start).total_seconds() // 3600)
    return [(start + timedelta(hours=h)).astimezone(pytz.UTC) for h in range(total_hours)]

def _find_last_complete_local_day(df: pd.DataFrame) -> Optional[datetime]:
    if df.empty:
        return None
    ts_utc = pd.to_datetime(df["timestamp_utc"], utc=True)
    local = ts_utc.dt.tz_convert(BRUSSELS_TZ)
    df = df.assign(_local_date=local.dt.date)

    last_complete: Optional[datetime] = None
    for d, g in df.groupby("_local_date"):
        local_midnight = BRUSSELS_TZ.localize(datetime(d.year, d.month, d.day))
        expected = len(_expected_utc_grid_for_local_day(local_midnight))
        if len(g) == expected and g["value"].isna().sum() == 0:
            last_complete = local_midnight
    return last_complete

def _latest_available_local_day() -> datetime:
    # keep this simple – availability is verified during fetch
    return datetime.now(BRUSSELS_TZ).replace(hour=0, minute=0, second=0, microsecond=0)

def _day_range(start_ymd: str, end_ymd: str) -> List[str]:
    s = datetime.strptime(start_ymd, "%Y-%m-%d")
    e = datetime.strptime(end_ymd, "%Y-%m-%d")
    out, cur = [], s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out

def _normalize_rows_to_df(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["timestamp_utc", "value", "area"])
    df = pd.DataFrame(rows)
    if "price_eur_mwh" in df.columns:
        df = df.rename(columns={"price_eur_mwh": "value"})
    ts_col = "ts_utc" if "ts_utc" in df.columns else "timestamp_utc"
    df["timestamp_utc"] = pd.to_datetime(df[ts_col], utc=True)
    return df[["timestamp_utc", "value", "area"]].sort_values("timestamp_utc").reset_index(drop=True)

def _is_all_nan_day(df_day: pd.DataFrame) -> bool:
    return df_day["value"].isna().all() if not df_day.empty else True

def _to_relpath(absolute_path: Path) -> str:
    """
    Convert an absolute path under EDC_OUTPUT_DIR into a relative path
    so io_local can read/write it. If std_root is outside OUTPUT_DIR, fall back
    to a relative string without leading slash (still usable by io_local).
    """
    output_dir = Path(os.environ.get("EDC_OUTPUT_DIR", "./data")).resolve()
    try:
        rel = absolute_path.resolve().relative_to(output_dir)
        return str(rel.as_posix())
    except ValueError:
        # outside OUTPUT_DIR – store under a 'standard' prefix relative path
        return str(absolute_path.name)

def update_single_file_for_area(area: str, cfg: SyncConfig) -> Path:
    area_eic = _normalize_zone(area)
    path = cfg.path_for_area(area_eic)
    client = EntsoeClient()

    # ---- read existing via io_local -----------------------------------------
    relpath = _to_relpath(path)
    try:
        existing = io_local.read_parquet(relpath)
    except FileNotFoundError:
        existing = pd.DataFrame(columns=["timestamp_utc", "value", "area"])

    last_complete = _find_last_complete_local_day(existing)
    start_date = cfg.start_ymd if last_complete is None else (last_complete + timedelta(days=1)).date().strftime("%Y-%m-%d")
    today_local = _latest_available_local_day().date().strftime("%Y-%m-%d")

    new_chunks: List[pd.DataFrame] = []

    def _fetch_one(ymd: str) -> Optional[pd.DataFrame]:
        day_local = BRUSSELS_TZ.localize(datetime.strptime(ymd, "%Y-%m-%d"))
        start_utc = day_local.astimezone(pytz.UTC)
        end_utc = (day_local + timedelta(days=1)).astimezone(pytz.UTC)
        rows = client.day_ahead_prices(area_eic, start_utc, end_utc)
        df = _normalize_rows_to_df(rows)
        return None if _is_all_nan_day(df) else df

    # re-fetch incomplete last day (if any)
    if last_complete is not None:
        last_ts_local = pd.to_datetime(existing["timestamp_utc"], utc=True).dt.tz_convert(BRUSSELS_TZ).max()
        last_day_local = last_ts_local.replace(hour=0, minute=0, second=0, microsecond=0)
        if last_day_local > last_complete:
            df = _fetch_one(last_day_local.date().strftime("%Y-%m-%d"))
            if df is not None:
                new_chunks.append(df)

    # fetch forward until unpublished day
    for ymd in _day_range(start_date, today_local):
        df = _fetch_one(ymd)
        if df is None:
            break
        new_chunks.append(df)

    if not new_chunks and last_complete is not None:
        return path  # nothing to do

    incoming = pd.concat(new_chunks, ignore_index=True) if new_chunks else pd.DataFrame(columns=["timestamp_utc","value","area"])

    merged = (
        pd.concat([existing, incoming], ignore_index=True)
          .drop_duplicates(subset=["timestamp_utc", "area"], keep="last")
          .sort_values("timestamp_utc")
          .reset_index(drop=True)
    )

    # ---- write via io_local --------------------------------------------------
    io_local.write_parquet(merged, relpath)
    return path

def sync_single_files(
    areas: Iterable[str],
    std_root: str | Path,
    start_ymd: str = "2020-01-01",
    series_name: str = "entsoe_day_ahead_price",
    area_files_prefix: str = "",
) -> Dict[str, str]:
    cfg = SyncConfig(std_root=Path(std_root), series_name=series_name, start_ymd=start_ymd, area_files_prefix=area_files_prefix)
    out: Dict[str, str] = {}
    for a in areas:
        p = update_single_file_for_area(a, cfg)
        out[_normalize_zone(a)] = str(p)
    return out
