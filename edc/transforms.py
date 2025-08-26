import pandas as pd

def normalize_prices(rows, target_tz="Europe/Madrid") -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df["ts_local"] = df["ts_utc"].dt.tz_convert(target_tz)
    cols = ["ts_utc","ts_local","area","price_eur_mwh"]
    return df[cols].sort_values(["area","ts_utc"]).reset_index(drop=True)
