import json
import gzip
import os
import time
from datetime import datetime, timedelta, timezone

import requests

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000
CHUNK_DAYS = 1
REQUEST_TIMEOUT = 120
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 5
RETRY_STATUS = {429, 500, 502, 503, 504}

# Reused across a backfill's hundreds of requests to avoid a TLS handshake each time.
_SESSION = requests.Session()


def read_watermark(watermark_path: str, default: str) -> str:
    """Last date landed, or default - resets if config's start_date changed, so widening it re-backfills."""
    try:
        with open(watermark_path) as f:
            state = json.load(f)
    except FileNotFoundError:
        return default

    if state.get("start_date") != default:
        return default
    return state["max_created_date"]


def write_watermark(watermark_path: str, value: str, start_date: str) -> None:
    os.makedirs(os.path.dirname(watermark_path), exist_ok=True)
    with open(watermark_path, "w") as f:
        json.dump({"max_created_date": value, "start_date": start_date}, f)


def _get(params: dict) -> list[dict]:
    """GET one page with retry/backoff; fails fast on 4xx since a bad query won't self-heal."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as exc:
            failure = exc
        else:
            if resp.status_code not in RETRY_STATUS:
                resp.raise_for_status()
                return resp.json()
            failure = requests.HTTPError(f"HTTP {resp.status_code} from Socrata")

        if attempt == MAX_ATTEMPTS:
            raise failure
        delay = BACKOFF_SECONDS * 2 ** (attempt - 1)
        print(f"  request failed ({failure}); retrying in {delay}s")
        time.sleep(delay)


def iter_chunks(since: str, until: str):
    """Yield (start, end] timestamp pairs, CHUNK_DAYS at a time, so chunks never overlap or gap."""
    start = datetime.fromisoformat(since).replace(microsecond=0)
    stop = datetime.fromisoformat(until).replace(microsecond=0)

    while start < stop:
        boundary = (start + timedelta(days=CHUNK_DAYS)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end = min(boundary, stop)
        yield start.isoformat(), end.isoformat()
        start = end


def fetch_pages(start: str, end: str):
    """Page by created_date, not :id (Socrata can't index that alongside a date filter and times out)."""
    offset = 0
    while True:
        records = _get(
            {
                "$where": f"created_date > '{start}' AND created_date <= '{end}'",
                "$order": "created_date,:id",
                "$limit": PAGE_SIZE,
                "$offset": offset,
            }
        )
        if not records:
            return
        yield records
        if len(records) < PAGE_SIZE:
            return  # short page means last page; skip the empty confirming request
        offset += PAGE_SIZE


def write_ndjson_gz(records: list[dict], out_dir: str, page: int, run_id: str) -> str:
    """Lands one page as gzipped NDJSON; run_id avoids same-day filename collisions across reruns."""
    os.makedirs(out_dir, exist_ok=True)
    path = f"{out_dir}/run_{run_id}_page_{page:04d}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return path


def run_ingestion(landing_root: str, default_start: str, verbose: bool = True) -> dict:
    watermark_path = f"{landing_root}/_watermark/watermark.json"
    since = read_watermark(watermark_path, default_start)
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    out_dir = f"{landing_root}/ingest_date={now.date().isoformat()}"
    until = now.replace(tzinfo=None, microsecond=0).isoformat()

    max_seen, total, pages, chunks = since, 0, 0, 0

    for chunk_start, chunk_end in iter_chunks(since, until):
        chunk_total = 0
        for records in fetch_pages(chunk_start, chunk_end):
            write_ndjson_gz(records, out_dir, pages, run_id)
            pages += 1
            total += len(records)
            chunk_total += len(records)
            batch_max = max(r["created_date"] for r in records if "created_date" in r)
            max_seen = max(max_seen, batch_max)
        chunks += 1

        # Safe to advance now since pages arrive in created_date order - a crash costs one day, not the run.
        if total > 0:
            write_watermark(watermark_path, max_seen, default_start)
        if verbose:
            print(f"  {chunk_start[:10]}  {chunk_total:>6} records  (total {total:,})")

    return {
        "run_id": run_id,
        "since": since,
        "until": until,
        "records": total,
        "pages": pages,
        "chunks": chunks,
        "new_watermark": max_seen,
    }
