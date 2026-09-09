import json
import gzip
import os
from datetime import datetime, timezone

import requests

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000


def read_watermark(watermark_path: str, default: str) -> str:
    """
    Returns the last date landed, or default if there is no usable watermark.

    The file records the start_date it was seeded from, so widening start_date in
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


def fetch_pages(since: str):
    """Yield lists of records with created_date > since, page by page."""
    offset = 0
    while True:
        params = {
            "$where": f"created_date > '{since}'",
            "$order": ":id",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        resp = requests.get(API_URL, params=params, timeout=120)
        resp.raise_for_status()
        records = resp.json()
        if not records:
            return
        yield records
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


def run_ingestion(landing_root: str, default_start: str) -> dict:
    watermark_path = f"{landing_root}/_watermark/watermark.json"
    since = read_watermark(watermark_path, default_start)
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    out_dir = f"{landing_root}/ingest_date={now.date().isoformat()}"

    max_seen, total, pages = since, 0, 0
    for page_num, records in enumerate(fetch_pages(since)):
        write_ndjson_gz(records, out_dir, page_num, run_id)
        total += len(records)
        pages += 1
        batch_max = max(r["created_date"] for r in records if "created_date" in r)
        max_seen = max(max_seen, batch_max)

    if total > 0:
        write_watermark(watermark_path, max_seen, default_start)
    return {
        "run_id": run_id,
        "since": since,
        "records": total,
        "pages": pages,
        "new_watermark": max_seen,
    }
