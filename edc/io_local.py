import os, pandas as pd
from pathlib import Path

OUTPUT_DIR = os.environ.get("EDC_OUTPUT_DIR", "./data")

def write_parquet(df: pd.DataFrame, relpath: str):
    path = Path(OUTPUT_DIR) / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def read_parquet(relpath: str) -> pd.DataFrame:
    path = Path(OUTPUT_DIR) / relpath
    return pd.read_parquet(path)
