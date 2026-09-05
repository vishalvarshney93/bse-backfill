"""Select an idempotent FilingForge backfill batch for GitHub Actions."""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from filingforge_poc import AzureStore


SCRIP_CODE_PATTERN = re.compile(r"^\d{6}$")


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def select_company_specs(
    companies: list[dict[str, Any]],
    state_rows: list[dict[str, Any]],
    batch_size: int,
    refresh_days: int,
    now: datetime | None = None,
) -> list[str]:
    current_time = now or datetime.now(timezone.utc)
    refresh_cutoff = current_time - timedelta(days=refresh_days)
    retry_cutoff = current_time - timedelta(hours=24)
    states: dict[str, dict[str, Any]] = {}
    for row in state_rows:
        match = re.search(r"-(\d{6})$", str(row.get("RowKey") or ""))
        if match:
            states[match.group(1)] = row

    candidates = []
    for company in companies:
        scrip_code = str(company.get("scrip_code") or "").strip()
        company_name = re.sub(
            r"\s+",
            " ",
            re.sub(r"[^A-Za-z0-9 .&()'/-]+", " ", str(company.get("company_name") or "")),
        ).strip()
        if not SCRIP_CODE_PATTERN.fullmatch(scrip_code) or not company_name:
            continue
        state = states.get(scrip_code)
        updated_at = _parse_timestamp(state.get("UpdatedAt")) if state else None
        status = str(state.get("Status") or "") if state else ""
        if status == "partial":
            if updated_at and updated_at > retry_cutoff:
                continue
            priority = (0, updated_at or datetime.min.replace(tzinfo=timezone.utc), scrip_code)
        elif state is None:
            priority = (1, datetime.min.replace(tzinfo=timezone.utc), scrip_code)
        elif status != "complete":
            if updated_at and updated_at > retry_cutoff:
                continue
            priority = (2, updated_at or datetime.min.replace(tzinfo=timezone.utc), scrip_code)
        elif updated_at is None or updated_at < refresh_cutoff:
            priority = (3, updated_at or datetime.min.replace(tzinfo=timezone.utc), scrip_code)
        else:
            continue
        candidates.append((priority, f"{company_name}|{scrip_code}"))

    candidates.sort(key=lambda item: item[0])
    return [spec for _priority, spec in candidates[:batch_size]]


def fetch_company_universe() -> list[dict[str, Any]]:
    base_url = os.environ.get("SIGNALFEED_APP_BASE_URL", "").strip().rstrip("/")
    if not base_url or not (
        base_url.startswith("https://")
        or base_url.startswith("http://localhost")
        or base_url.startswith("http://127.0.0.1")
    ):
        raise RuntimeError("SIGNALFEED_APP_BASE_URL must be the deployed HTTPS website URL")
    companies: list[dict[str, Any]] = []
    cursor = ""
    while True:
        response = requests.get(
            f"{base_url}/api/companies/directory",
            params={"after": cursor, "limit": "1000"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        page = payload.get("companies") if isinstance(payload, dict) else None
        if not isinstance(page, list):
            raise RuntimeError("Company directory returned an unexpected response")
        companies.extend(page)
        if len(page) < 1000:
            return companies
        next_cursor = str(payload.get("next_cursor") or "")
        if not SCRIP_CODE_PATTERN.fullmatch(next_cursor) or next_cursor <= cursor:
            raise RuntimeError("Company directory returned an invalid pagination cursor")
        cursor = next_cursor


def claim_company_specs(
    store: AzureStore,
    specs: list[str],
    batch_size: int,
    lease_hours: int,
    now: datetime | None = None,
) -> list[str]:
    current_time = now or datetime.now(timezone.utc)
    for claim in store.state.query_entities("PartitionKey eq 'FILINGFORGE_ROTATION'"):
        expires_at = _parse_timestamp(claim.get("ExpiresAt"))
        if expires_at is None or expires_at > current_time:
            continue
        try:
            store.state.delete_entity("FILINGFORGE_ROTATION", str(claim["RowKey"]))
        except ResourceNotFoundError:
            pass

    claimed = []
    for spec in specs:
        _name, separator, scrip_code = spec.rpartition("|")
        if not separator or not SCRIP_CODE_PATTERN.fullmatch(scrip_code):
            continue
        try:
            store.state.create_entity({
                "PartitionKey": "FILINGFORGE_ROTATION",
                "RowKey": scrip_code,
                "ClaimedAt": current_time.isoformat(),
                "ExpiresAt": (current_time + timedelta(hours=lease_hours)).isoformat(),
                "RunId": os.environ.get("GITHUB_RUN_ID", "local")[:100],
            })
            claimed.append(spec)
            if len(claimed) >= batch_size:
                break
        except ResourceExistsError:
            continue
    return claimed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--refresh-days", type=int, default=30)
    parser.add_argument("--lease-hours", type=int, default=8)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 25:
        raise SystemExit("--batch-size must be between 1 and 25")
    if not 1 <= args.refresh_days <= 365:
        raise SystemExit("--refresh-days must be between 1 and 365")
    if not 7 <= args.lease_hours <= 24:
        raise SystemExit("--lease-hours must be between 7 and 24")

    store = AzureStore()
    state_rows = list(store.state.query_entities(
        "PartitionKey eq 'FILINGFORGE_POC'",
        select=["RowKey", "Status", "UpdatedAt"],
    ))
    candidates = select_company_specs(
        fetch_company_universe(),
        state_rows,
        10_000,
        args.refresh_days,
    )
    specs = claim_company_specs(store, candidates, args.batch_size, args.lease_hours)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as output_file:
            output_file.write(f"companies={','.join(specs)}\n")
            output_file.write(f"count={len(specs)}\n")
    print(f"Selected {len(specs)} company specification(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())