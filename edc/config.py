from pathlib import Path
from dotenv import load_dotenv

def load_env():
    # repo root assumed two levels up from this file: .../electricity-data-collector/edc/config.py
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env", override=False)
