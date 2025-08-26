# dags/entsoe_sync.py
from __future__ import annotations
import os, pendulum
from typing import List, Dict
from airflow.decorators import dag, task

from edc.sync import sync_single_files
from edc.entsoe_client import _normalize_zone

ZONES = [z.strip() for z in os.getenv("EDC_BIDDING_ZONES", "ES,PT,FR").split(",") if z.strip()]
STD_ROOT = os.getenv("EDC_STD_OUTPUT_DIR", "/opt/airflow/data/standard")
START_YMD = os.getenv("EDC_START_DATE", "2020-01-01")
SERIES = os.getenv("EDC_SERIES_NAME", "entsoe_day_ahead_price")

@dag(
    dag_id="entsoe_sync",
    schedule="@hourly",                 # was "@daily"
    start_date=pendulum.datetime(2020, 1, 1, tz="Europe/Madrid"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3},
    tags=["entsoe","sync","singlefile"],
)

def entsoe_sync():
    @task
    def resolve_areas() -> List[str]:
        return [_normalize_zone(z) for z in ZONES]

    @task
    def run_sync(areas: List[str]) -> Dict[str, str]:
        return sync_single_files(areas=areas, std_root=STD_ROOT, start_ymd=START_YMD, series_name=SERIES)

    run_sync(resolve_areas())

entsoe_sync()
