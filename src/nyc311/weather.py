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
    """
    GET one response from Open-Meteo, retrying transient failures with exponential
    backoff. Mirrors api_client._get: only timeouts, connection errors and the
    retryable status codes get another attempt — a 4xx (e.g. end_date out of the
    allowed 1940..today range) means the query itself is wrong and won't self-heal.
    """
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
    """
    Pure reshape: Open-Meteo returns one array per variable, all indexed by the same
    `time` array. Transpose into one dict per day, keyed `date` plus each variable.
    """
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
    """
    Fetch one row per day in [start_date, end_date] (both YYYY-MM-DD, inclusive).

    A single call covers the whole range — unlike Socrata, Open-Meteo's daily
    aggregates aren't row-paginated, so a full backfill-scale request (~350 days)
    is one lightweight request, not hundreds.

    end_date is clamped to today: the API 400s on any end_date past its own
    "today" (measured in its server's UTC clock), so a caller computing "now" a
    few hours off from that clock would otherwise fail the whole run for one day
    of data it can't serve yet anyway.
    """
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
