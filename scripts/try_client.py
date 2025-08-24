# scripts/try_client.py
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os, sys
sys.path.append(".")

from edc.entsoe_client import EntsoeClient

# Choose the delivery day in *local* time (yesterday in Madrid)
madrid = ZoneInfo("Europe/Madrid")
today_local = datetime.now(madrid).date()
delivery_day_local = today_local - timedelta(days=1)

# Build exact local day bounds, then convert to UTC for the API
start_local = datetime.combine(delivery_day_local, datetime.min.time(), tzinfo=madrid)
end_local   = start_local + timedelta(days=1)
start_utc = start_local.astimezone(timezone.utc)
end_utc   = end_local.astimezone(timezone.utc)

c = EntsoeClient(os.getenv("ENTSOE_API_TOKEN"))
rows = c.day_ahead_prices("ES", start_utc, end_utc)
print(f"Fetched {len(rows)} points for ES, {delivery_day_local} (local) "
      f"→ [{start_utc:%Y-%m-%d %H:%M}Z, {end_utc:%Y-%m-%d %H:%M}Z)")
for r in rows[:5]:
    print(r)
