import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if load_dotenv is not None:
    load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = Path(os.getenv("TFL_DATA_DIR", PROJECT_ROOT / "data")).resolve()
TFL_API_KEY = os.getenv("TFL_API_KEY")


def tfl_request_params(**params) -> dict:
    if TFL_API_KEY:
        params["app_key"] = TFL_API_KEY
    return params
