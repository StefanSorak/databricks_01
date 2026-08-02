import json
import gzip
import os
from datetime import date

import requests

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000


def read_watermark(watermark_path: str, default: str) -> str:
    """
    Returns the last date or default if the file doesn't exist
    """
    try:
        with open(watermark_path) as f:
            return json.load(f)["max_created_date"]
    except FileNotFoundError:
        return default


def write_watermark(watermark_path: str, value: str) -> None:
    os.makedirs(os.path.dirname(watermark_path), exist_ok=True)
    with open(watermark_path, "w") as f:
        json.dump({"max_created_date": value}, f)


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


def write_ndjson_gz(records: list[dict], out_dir: str, page: int) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = f"{out_dir}/page_{page:04d}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return path


def run_ingestion(landing_root: str, default_start: str) -> dict:
    watermark_path = f"{landing_root}/_watermark/watermark.json"
    since = read_watermark(watermark_path, default_start)
    out_dir = f"{landing_root}/ingest_date={date.today().isoformat()}"
    

    max_seen, total, pages = since, 0, 0
    for page_num, records in enumerate(fetch_pages(since)):
        write_ndjson_gz(records, out_dir, page_num)
        total += len(records)
        pages += 1
        batch_max = max(r["created_date"] for r in records if "created_date" in r)
        max_seen = max(max_seen, batch_max)

    if total > 0:
        write_watermark(watermark_path, max_seen)
    return {"records": total, "pages": pages, "new_watermark": max_seen}