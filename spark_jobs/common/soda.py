"""Socrata SODA API client for the Chicago Taxi Trips dataset (wrvz-psew).

Pages a single date window (filtered on trip_start_timestamp) and yields raw CSV
text per page. Column projection ($select) keeps payloads small; an app token
(X-App-Token) avoids throttling; requests retry with backoff on 429/5xx.

Docs: https://dev.socrata.com/docs/queries/  |  https://dev.socrata.com/docs/app-tokens.html
"""
import csv
import io
import os
import time
from typing import Iterator, Tuple

import requests

from common.obs import get_logger

log = get_logger("soda")

# Only the columns we need - projection dramatically reduces payload size.
SELECT_COLS = [
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
]

DEFAULT_PAGE_SIZE = 50000
# The Chicago SODA portal can have transient 503 bursts / slow responses; retry
# generously with capped exponential backoff so a blip doesn't fail a whole month.
MAX_RETRIES = 8
BACKOFF_CAP = 60
REQUEST_TIMEOUT = 300


def _base_url() -> str:
    base = os.environ.get("TAXI_API_BASE", "https://data.cityofchicago.org/resource").rstrip("/")
    dataset = os.environ.get("TAXI_DATASET_ID", "wrvz-psew")
    return f"{base}/{dataset}.csv"


def _headers() -> dict:
    token = os.environ.get("CHICAGO_DATA_PORTAL_TOKEN", "").strip()
    return {"X-App-Token": token} if token else {}


def _request_with_retry(url: str, params: dict, headers: dict) -> str:
    """GET with exponential backoff on transient (429/5xx/network) errors."""
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"transient {resp.status_code} {resp.reason}")
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as err:
            last_err = err
            backoff = min(2 ** attempt, BACKOFF_CAP)
            log.warning(
                "SODA request failed (attempt %d/%d): %s - retrying in %ds",
                attempt + 1, MAX_RETRIES, err, backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"SODA request failed after {MAX_RETRIES} retries: {last_err}")


def _count_data_rows(csv_text: str) -> int:
    """Count data rows (excludes header), CSV-aware so quoted fields are safe."""
    reader = csv.reader(io.StringIO(csv_text))
    total = sum(1 for _ in reader)
    return max(total - 1, 0)


def fetch_pages(
    start_iso: str, end_iso: str, page_size: int = DEFAULT_PAGE_SIZE
) -> Iterator[Tuple[int, str, int]]:
    """Yield (page_index, csv_text, data_row_count) for one [start, end) window.

    Each page's CSV includes its own header row, so pages can be written as
    independent part files that Spark reads with header=true.
    """
    url = _base_url()
    headers = _headers()
    has_token = bool(os.environ.get("CHICAGO_DATA_PORTAL_TOKEN", "").strip())
    log.info(
        "SODA fetch window [%s, %s) page_size=%d app_token=%s",
        start_iso, end_iso, page_size, "yes" if has_token else "no",
    )
    where = (
        f"trip_start_timestamp >= '{start_iso}' AND trip_start_timestamp < '{end_iso}'"
    )
    offset = 0
    page_index = 0
    while True:
        params = {
            "$select": ",".join(SELECT_COLS),
            "$where": where,
            "$order": "trip_start_timestamp,trip_id",
            "$limit": page_size,
            "$offset": offset,
        }
        text = _request_with_retry(url, params, headers)
        rows = _count_data_rows(text)
        if rows == 0:
            break
        log.info("SODA page %d fetched rows=%d offset=%d", page_index, rows, offset)
        yield page_index, text, rows
        page_index += 1
        offset += page_size
        if rows < page_size:
            break
    log.info("SODA fetch complete: %d page(s)", page_index)
