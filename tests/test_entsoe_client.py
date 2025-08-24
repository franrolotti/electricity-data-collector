import os
import pytest
from datetime import datetime, timezone, timedelta
from edc.entsoe_client import EntsoeClient

@pytest.mark.skipif(not os.environ.get("ENTSOE_API_TOKEN"), reason="Need token")
def test_day_ahead_prices_smoke():
    c = EntsoeClient()
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    end = start + timedelta(days=1)
    rows = c.day_ahead_prices("ES", start, end)
    assert isinstance(rows, list)
