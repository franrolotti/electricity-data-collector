from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.decorators import task
from edc.entsoe_client import EntsoeClient
from edc.io_local import write_parquet
from edc.transforms import normalize_prices

TZ = os.environ.get("EDC_TIMEZONE", "Europe/Madrid")
BIDDING_ZONES = os.environ.get("EDC_BIDDING_ZONES", "ES,PT").split(",")

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="entsoe_day_ahead_prices",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 13 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["entsoe","prices","dayahead"],
) as dag:

    @task
    def fetch_prices(execution_date=None):
        ds = execution_date or dag.get_dagrun().logical_date
        start_utc = (ds - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_utc   = start_utc + timedelta(days=1)

        client = EntsoeClient()
        all_rows = []
        for zone in BIDDING_ZONES:
            rows = client.day_ahead_prices(zone.strip(), start_utc, end_utc)
            all_rows.extend(rows)

        df = normalize_prices(all_rows, target_tz=TZ)
        date_str = (start_utc.date()).isoformat()
        relpath = f"raw/day_ahead_prices/date={date_str}/prices.parquet"
        write_parquet(df, relpath)
        return relpath

    fetch_prices()
