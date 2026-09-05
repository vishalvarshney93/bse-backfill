"""Validate and publish downloaded FilingForge snapshots without re-running AI."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from filingforge_poc import (
    AzureStore,
    SnapshotPublicationError,
    build_published_projection,
    utc_now,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot_dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    files = sorted(args.snapshot_dir.resolve().glob("*.json"))
    if not files:
        raise SystemExit("No JSON snapshots found")
    schema_path = Path(__file__).with_name("schemas") / "company-research.schema.json"
    if not schema_path.is_file():
        raise SystemExit(
            f"Missing required schema: {schema_path}. Upload/clone the complete repository, not only the Python files."
        )

    planned = []
    failures = 0
    for path in files:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        company_key = str(snapshot.get("company_key") or "")
        try:
            projection = build_published_projection(snapshot)
            status = {
                "status": "published",
                "company_key": company_key,
                "generated_at": projection["generated_at"],
                "published_at": utc_now(),
                "warnings": projection["publication"]["warnings"],
            }
            print(f"PUBLISH {company_key}: {projection['validated_evidence_count']} evidence items")
        except SnapshotPublicationError as exc:
            projection = None
            status = {
                "status": "quarantined",
                "company_key": company_key,
                "generated_at": snapshot.get("generated_at"),
                "published_at": utc_now(),
                "reasons": str(exc).split("; "),
            }
            failures += 1
            print(f"QUARANTINE {company_key}: {exc}")
        planned.append((company_key, projection, status))

    # Do not connect to or mutate Azure until every local file has completed
    # deterministic classification. Unexpected operational errors therefore
    # cannot leave a partially-published or falsely-quarantined batch.
    store = None if args.dry_run else AzureStore()
    if store:
        store.preflight()
    for company_key, projection, status in planned:
        if store:
            store.publish_projection(company_key, projection, status)
    print(f"Processed {len(files)} snapshot(s): {len(files) - failures} published, {failures} quarantined")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())