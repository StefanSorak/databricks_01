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

# One session for the whole run: a backfill is hundreds of requests and reusing the
# connection avoids a TLS handshake on every one.
_SESSION = requests.Session()


def read_watermark(watermark_path: str, default: str) -> str:
    """
    Returns the last date landed, or default if there is no usable watermark.

    The file records the start_date the run was seeded from, so widening start_date in
    config invalidates it and re-backfills from the new date. Without that check a
    widened start_date would be silently ignored: the stored high-water mark is
    always later than any earlier start_date, so it would always win.
    """
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
    """
    GET one page from Socrata, retrying transient failures with exponential backoff.

    A backfill is hundreds of requests, so a single blip must not end the run. Only
    timeouts, connection errors and the retryable status codes get another attempt;
    a 4xx means the query itself is wrong and will fail identically forever.
    """
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
    """
    Yield (start, end) timestamp pairs covering (since, until], CHUNK_DAYS at a time.

    Each pair is exclusive of start and inclusive of end, so consecutive chunks
    neither overlap nor leave gaps. The first chunk runs from the watermark to the
    next midnight boundary; the rest are whole days.
    """
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
    """
    Yield lists of records with start < created_date <= end, page by page.

    Two Socrata characteristics shape this query, both measured against the ~3.7M-row
    2025-26 winter window:

    - "$order=:id" is a system column Socrata cannot serve from an index alongside a
      created_date filter. It sorted the entire filtered set before returning page 0
      and read-timed out at 90s for even 1,000 rows. Ordering by created_date - the
      same column being filtered - streams in index order: 50k rows in ~12s. ":id"
      stays as the tiebreaker so rows sharing a created_date still page stably.
    - Deep "$offset" degrades badly regardless of ordering: 50k rows at offset 2M
      timed out at 90s. That is why the caller slices the window into days rather
      than paging through it. The busiest day in the window holds ~23k records, well
      under PAGE_SIZE, so offset stays 0 in practice - the loop below is a safety net
      for a freak day, and its offsets stay shallow enough to serve.
    """
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
    """
    Land one page as gzipped NDJSON at run_{run_id}_page_{page}.json.gz.

    The run_id keeps two runs on the same day from colliding: page numbering restarts
    at 0 every run, so a bare page_0000.json.gz would be overwritten by the next run
    into the same ingest_date= folder. Auto Loader's checkpoint has already recorded
    that path as ingested, so the overwritten content would never reach bronze - a
    silent gap rather than a duplicate.
    """
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

        # The chunk's whole date range is drained before we get here, so advancing now
        # cannot skip an unfetched record - which is only true because pages arrive in
        # created_date order. Under the old ":id" ordering this would have been a data
        # loss bug. It makes the backfill resumable: a crash costs one day, not the run.
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
