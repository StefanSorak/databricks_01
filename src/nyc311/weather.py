import time
from datetime import date

import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060
DAILY_VARIABLES = ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]
REQUEST_TIMEOUT = 30
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 5
RETRY_STATUS = {429, 500, 502, 503, 504}

_SESSION = requests.Session()


def _get(params: dict) -> dict:
    """GET with retry/backoff on timeouts and 5xx, mirroring api_client._get; fails fast on 4xx."""
    failure = {}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.get(ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as exc:
            failure = exc
        else:
            if resp.status_code not in RETRY_STATUS:
                resp.raise_for_status()
                return resp.json()
            failure = requests.HTTPError(f"HTTP {resp.status_code} from Open-Meteo")

        if attempt < MAX_ATTEMPTS:
            delay = BACKOFF_SECONDS * 2 ** (attempt - 1)
            print(f"  request failed ({failure}); retrying in {delay}s")
            time.sleep(delay)

    raise failure


def rows_from_daily(daily: dict) -> list[dict]:
    """Transpose Open-Meteo's per-variable arrays into one dict per day."""
    dates = daily["time"]
    variables = [k for k in daily if k != "time"]
    return [
        {"date": dates[i], **{var: daily[var][i] for var in variables}}
        for i in range(len(dates))
    ]


def fetch_daily_weather(
    start_date: str,
    end_date: str,
    latitude: float = NYC_LATITUDE,
    longitude: float = NYC_LONGITUDE,
) -> list[dict]:
    """Fetch one row per day in [start_date, end_date]; end_date is clamped to today since the API 400s past it."""
    end_date = min(end_date, date.today().isoformat())
    payload = _get(
        {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": "America/New_York",
        }
    )
    return rows_from_daily(payload["daily"])
