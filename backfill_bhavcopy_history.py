"""
Backfills historical BSE Bhavcopy (daily EOD OHLCV) into an Azure Table
Storage table, one entity per (scrip_code, trade_date). Meant to run as a
standalone GitHub Actions job, decoupled from the bse-trader Function App's
own live nightly sync (signalfeeed_bhavcopy_sync in function_app.py), which
only writes to Supabase `bhavcopy_daily` going forward.

Schema: PartitionKey=scrip_code, RowKey=trade_date ("YYYY-MM-DD", sortable)
— optimized for "give me one company's full price history" reads (the
company-page price chart's actual access pattern). One side effect of this
choice: a single day's ~5000 companies each land in a DIFFERENT partition,
so Table Storage's same-partition batch API can't be used for a daily
backfill — writes are issued concurrently instead (see WRITE_CONCURRENCY).

Only BSE_BHAVCOPY_URL_TEMPLATE's current UDiFF format (post ~2024) is
verified. Do not extend START_DATE before that era without first confirming
the legacy URL/column format the same way the current one was (browser
DevTools), per the existing NOTE in bse-trader's function_app.py.

Required env var: AZURE_STORAGE_CONNECTION_STRING (secret, never log it).
Optional env vars: BACKFILL_START_DATE (default 2026-01-01), BACKFILL_END_DATE
(default today, UTC), BHAVCOPY_HISTORY_TABLE_NAME (default BhavcopyHistory),
BHAVCOPY_URL_TEMPLATE, REQUEST_SLEEP_SECONDS (default 1 — throttles only the
BSE HTTP fetches, one per trading day, to avoid bot-blocking a shared
GitHub-runner IP), WRITE_CONCURRENCY (default 16).
"""
import csv
import gzip
import logging
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from io import BytesIO, StringIO

import requests
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import HttpResponseError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bhavcopy_backfill")

BHAVCOPY_URL_TEMPLATE = os.environ.get(
    "BHAVCOPY_URL_TEMPLATE",
    "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{yyyymmdd}_F_0000.CSV",
).strip()
TABLE_NAME = os.environ.get("BHAVCOPY_HISTORY_TABLE_NAME", "BhavcopyHistory").strip()
REQUEST_SLEEP_SECONDS = float(os.environ.get("REQUEST_SLEEP_SECONDS", "1"))
WRITE_CONCURRENCY = int(os.environ.get("WRITE_CONCURRENCY", "16"))
REQUEST_TIMEOUT_SECONDS = 30
# A real browser UA lowers the odds of a shared GitHub-runner IP being
# challenged/blocked by BSE's edge (Cloudflare etc.) — best-effort only.
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

# Same aliases as bse-trader's function_app.py BHAVCOPY_COLUMN_ALIASES —
# kept in sync manually since this script is intentionally standalone.
BHAVCOPY_COLUMN_ALIASES = {
    "scrip_code": ["FinInstrmId", "SC_CODE", "SCRIP_CD", "SCRIP CODE", "SECURITY CODE"],
    "open": ["OpnPric", "OPEN", "OPEN_PRICE", "OPEN PRICE"],
    "high": ["HghPric", "HIGH", "HIGH_PRICE", "HIGH PRICE"],
    "low": ["LwPric", "LOW", "LOW_PRICE", "LOW PRICE"],
    "close": ["ClsPric", "CLOSE", "CLOSE_PRICE", "CLOSE PRICE"],
    "prev_close": ["PrvsClsgPric", "PREVCLOSE", "PREV_CLOSE", "PREVIOUS CLOSE"],
    "volume": ["TtlTradgVol", "NO_OF_SHRS", "TTL_TRD_QNTY", "NET_TURNOV", "TOTAL TRADED QUANTITY"],
    "week52_high": ["HIGH52", "52WK_HIGH", "52 WEEK HIGH"],
    "week52_low": ["LOW52", "52WK_LOW", "52 WEEK LOW"],
}


def _parse_date(value: str, fallback: date) -> date:
    if not value:
        return fallback
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def _to_number(value):
    try:
        cleaned = (value or "").strip().replace(",", "")
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def _decode_bhavcopy_bytes(raw_bytes: bytes) -> str:
    """Handles plain CSV, .zip, or .gz — BSE has used all three."""
    try:
        with zipfile.ZipFile(BytesIO(raw_bytes)) as zf:
            csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), zf.namelist()[0])
            return zf.read(csv_name).decode("utf-8", errors="replace")
    except zipfile.BadZipFile:
        try:
            return gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
        except (OSError, gzip.BadGzipFile):
            return raw_bytes.decode("utf-8", errors="replace")


def _resolve_bhavcopy_columns(header_row: list):
    normalized = {h.strip().upper().replace(" ", "").replace("_", ""): h for h in header_row}
    resolved = {}
    for field, aliases in BHAVCOPY_COLUMN_ALIASES.items():
        for alias in aliases:
            key = alias.upper().replace(" ", "").replace("_", "")
            if key in normalized:
                resolved[field] = normalized[key]
                break
    if "scrip_code" not in resolved or "close" not in resolved:
        return None
    return resolved


def _parse_bhavcopy_csv(raw_bytes: bytes) -> list:
    csv_text = _decode_bhavcopy_bytes(raw_bytes)
    reader = csv.DictReader(StringIO(csv_text))
    if not reader.fieldnames:
        return []
    columns = _resolve_bhavcopy_columns(reader.fieldnames)
    if not columns:
        log.error("Could not recognize any expected column in header: %s", reader.fieldnames)
        return []

    rows = []
    for r in reader:
        scrip_code = (r.get(columns["scrip_code"]) or "").strip()
        close = _to_number(r.get(columns["close"]))
        if not scrip_code or close is None:
            continue
        rows.append({
            "scrip_code": scrip_code,
            "open": _to_number(r.get(columns.get("open"))) if columns.get("open") else None,
            "high": _to_number(r.get(columns.get("high"))) if columns.get("high") else None,
            "low": _to_number(r.get(columns.get("low"))) if columns.get("low") else None,
            "close": close,
            "prev_close": _to_number(r.get(columns.get("prev_close"))) if columns.get("prev_close") else None,
            "volume": int(_to_number(r.get(columns.get("volume"))) or 0) if columns.get("volume") else None,
            "week52_high": _to_number(r.get(columns.get("week52_high"))) if columns.get("week52_high") else None,
            "week52_low": _to_number(r.get(columns.get("week52_low"))) if columns.get("week52_low") else None,
        })
    return rows


class HolidayOrMissingFile(Exception):
    """Non-retryable: BSE returned a non-200 for this date (weekend/holiday/no file yet)."""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=20),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)
def _fetch_bhavcopy(trade_date: date) -> bytes:
    url = BHAVCOPY_URL_TEMPLATE.format(yyyymmdd=trade_date.strftime("%Y%m%d"))
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
    if resp.status_code != 200:
        raise HolidayOrMissingFile(f"{url} -> HTTP {resp.status_code}")
    return resp.content


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(HttpResponseError),
    reraise=True,
)
def _upsert_row(table_client, trade_date: date, row: dict):
    entity = {
        "PartitionKey": row["scrip_code"],
        "RowKey": trade_date.isoformat(),
        "Open": row["open"],
        "High": row["high"],
        "Low": row["low"],
        "Close": row["close"],
        "PrevClose": row["prev_close"],
        "Volume": row["volume"],
        "Week52High": row["week52_high"],
        "Week52Low": row["week52_low"],
    }
    entity = {k: v for k, v in entity.items() if v is not None or k in ("PartitionKey", "RowKey")}
    table_client.upsert_entity(entity, mode=UpdateMode.MERGE)


def backfill_one_day(table_client, trade_date: date) -> int:
    try:
        raw_bytes = _fetch_bhavcopy(trade_date)
    except HolidayOrMissingFile as e:
        log.info("Skipping %s (weekend/holiday/no file): %s", trade_date, e)
        return 0
    except requests.exceptions.RequestException as e:
        log.warning("Giving up on %s after retries: %s", trade_date, e)
        return 0

    rows = _parse_bhavcopy_csv(raw_bytes)
    if not rows:
        log.warning("Downloaded %s but found 0 usable rows — column mapping may need updating.", trade_date)
        return 0

    written = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=WRITE_CONCURRENCY) as pool:
        futures = {pool.submit(_upsert_row, table_client, trade_date, row): row for row in rows}
        for future in as_completed(futures):
            try:
                future.result()
                written += 1
            except HttpResponseError as e:
                failed += 1
                log.error("Write failed for %s: %s", futures[future]["scrip_code"], e)

    log.info("%s: upserted %d / %d rows (%d failed)", trade_date, written, len(rows), failed)
    return written


def main():
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        log.error("AZURE_STORAGE_CONNECTION_STRING is not set. Aborting.")
        sys.exit(1)

    start = _parse_date(os.environ.get("BACKFILL_START_DATE"), date(2026, 1, 1))
    end = _parse_date(os.environ.get("BACKFILL_END_DATE"), date.today())
    if start > end:
        log.error("BACKFILL_START_DATE (%s) is after BACKFILL_END_DATE (%s). Aborting.", start, end)
        sys.exit(1)

    service = TableServiceClient.from_connection_string(connection_string)
    table_client = service.create_table_if_not_exists(TABLE_NAME)

    total_written = 0
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon-Fri only; Sat/Sun never have a file
            total_written += backfill_one_day(table_client, current)
            time.sleep(REQUEST_SLEEP_SECONDS)
        current += timedelta(days=1)

    log.info("Backfill complete: %s -> %s, %d rows written total.", start, end, total_written)


if __name__ == "__main__":
    main()
