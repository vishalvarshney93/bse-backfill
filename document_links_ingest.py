"""Ingest the existing SignalFeed document archive into the research corpus.

This deliberately skips FilingForge discovery: `company_documents` is the
approved document queue. PDFs are transient runner inputs; only Markdown and
the existing evidence/retrieval artifacts are published.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import logging
import os
import re
import socket
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import fitz
import requests
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import UpdateMode

from filingforge_poc import (
    AzureStore,
    NvidiaClient,
    claim_cache_name,
    discover_documents,
    document_windows,
    load_cached_claims,
    process_company,
    upload_prepared_company,
    utc_now,
    write_cached_claims,
)


DOCUMENT_TYPES = {
    "ANNUAL_REPORT": "annual-reports",
    "QUARTERLY_RESULT": "quarterly",
    "INVESTOR_PRESENTATION": "investor-ppts",
    "EARNINGS_TRANSCRIPT": "concalls",
}
STATE_PARTITION = "DOCUMENT_LINKS"
LEASE_PARTITION = "DOCUMENT_LINKS_LEASE"
LEASE_DURATION = timedelta(hours=6)
SCRIP_PATTERN = re.compile(r"^\d{6}$")
MAX_PDF_BYTES = 40 * 1024 * 1024
MIN_EXTRACTED_CHARS = 200
SPARSE_PAGE_CHAR_THRESHOLD = 30
ANALYSIS_WINDOW_CHARS = max(
    4_000,
    min(int(os.environ.get("NVIDIA_NIM_ANALYSIS_WINDOW_CHARS", "12000")), 24_000),
)
MAX_WINDOWS_PER_DOCUMENT = max(
    1,
    min(int(os.environ.get("NVIDIA_NIM_MAX_WINDOWS_PER_DOCUMENT", "8")), 16),
)
EVIDENCE_PASSAGE_CHARS = 1_800
log = logging.getLogger("document_links_ingest")


class DocumentUnavailableError(ValueError):
    pass


class DocumentExtractionError(ValueError):
    pass


@dataclass(frozen=True)
class DocumentFailure:
    document_id: str
    status: str
    detail: str


def public_document_url(value: str) -> str:
    """Validate a curated source URL before the runner requests it."""
    parsed = urlparse(str(value or "").strip())
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("document URL must be a credential-free HTTPS URL")
    if parsed.hostname.lower() in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("document URL must not target localhost")
    return parsed.geturl()


def requestable_document_url(value: str) -> str:
    url = public_document_url(value)
    hostname = urlparse(url).hostname
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)}
    except socket.gaierror as exc:
        raise ValueError("document URL host could not be resolved") from exc
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise ValueError("document URL host resolves to a non-public address")
    return url


def is_screener_document_url(value: str) -> bool:
    try:
        hostname = (urlparse(value).hostname or "").lower()
        return hostname == "screener.in" or hostname.endswith(".screener.in")
    except ValueError:
        return False


def company_key(document: dict[str, Any]) -> str:
    scrip_code = str(document.get("scrip_code") or "").strip()
    if not SCRIP_PATTERN.fullmatch(scrip_code):
        raise ValueError("document has an invalid BSE scrip code")
    name = re.sub(r"[^A-Z0-9&-]+", "-", str(document.get("company_name") or "BSE").upper()).strip("-")
    return f"{name[:60] or 'BSE'}-{scrip_code}"


def source_set_hash(documents: list[dict[str, Any]]) -> str:
    values = [
        "|".join((str(row.get("id") or ""), str(row.get("pdf_url") or "")))
        for row in sorted(documents, key=lambda row: str(row.get("id") or ""))
    ]
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()


def fetch_documents() -> list[dict[str, Any]]:
    base_url = os.environ.get("SUPABASE_URL", os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")).rstrip("/")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not base_url or not service_key:
        raise RuntimeError("SUPABASE_URL/NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    headers = {"apikey": service_key, "Authorization": f"Bearer {service_key}"}
    select = "id,scrip_code,company_name,doc_type,doc_period_end_date,pdf_url"
    encoded_types = ",".join(quote(value) for value in sorted(DOCUMENT_TYPES))
    result: list[dict[str, Any]] = []
    for offset in range(0, 200_000, 1000):
        response = requests.get(
            f"{base_url}/rest/v1/company_documents",
            params={"select": select, "doc_type": f"in.({encoded_types})", "order": "scrip_code.asc,id.asc", "offset": offset, "limit": 1000},
            headers=headers,
            timeout=45,
        )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list):
            raise RuntimeError("company_documents returned an unexpected response")
        result.extend(row for row in page if isinstance(row, dict))
        if len(page) < 1000:
            return result
    raise RuntimeError("company_documents pagination exceeded the safety limit")


def persist_resolved_document_url(document: dict[str, Any], resolved_url: str | None) -> bool:
    original_url = str(document.get("pdf_url") or "")
    if resolved_url == original_url:
        return True
    base_url = os.environ.get("SUPABASE_URL", os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")).rstrip("/")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not base_url or not service_key:
        return False
    try:
        response = requests.patch(
            f"{base_url}/rest/v1/company_documents",
            params={"id": f"eq.{document['id']}"},
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json={"pdf_url": resolved_url},
            timeout=30,
        )
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        log.warning("Could not persist resolved URL for %s: %s", document.get("id"), type(exc).__name__)
        return False


def resolve_stored_document_url(raw_url: str) -> str | None:
    if not is_screener_document_url(raw_url):
        return public_document_url(raw_url)
    current = requestable_document_url(raw_url)
    try:
        for _ in range(4):
            response = requests.get(
                current,
                timeout=(10, 30),
                allow_redirects=False,
                stream=True,
                headers={"User-Agent": "TickerVectorDocuments/1.0"},
            )
            try:
                if response.is_redirect and response.headers.get("Location"):
                    current = requestable_document_url(requests.compat.urljoin(current, response.headers["Location"]))
                    continue
                response.raise_for_status()
                return None if is_screener_document_url(current) else current
            finally:
                response.close()
    except (ValueError, requests.RequestException):
        return None
    return None


def canonicalize_stored_document_urls(
    documents: list[dict[str, Any]],
    limit: int,
    delay_seconds: float,
) -> tuple[int, int, int]:
    candidates = [
        document for document in documents
        if is_screener_document_url(str(document.get("pdf_url") or ""))
    ][:limit]
    resolved = 0
    removed = 0
    failed = 0
    for index, document in enumerate(candidates):
        direct_url = resolve_stored_document_url(str(document.get("pdf_url") or ""))
        if not persist_resolved_document_url(document, direct_url):
            failed += 1
            continue
        if direct_url:
            resolved += 1
        else:
            removed += 1
        if index < len(candidates) - 1:
            time.sleep(delay_seconds)
    print(f"Canonicalized {resolved} direct document URL(s); removed {removed} unresolved URL(s); {failed} update(s) failed.")
    return resolved, removed, failed


def group_documents(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("doc_type") not in DOCUMENT_TYPES:
            continue
        try:
            grouped[company_key(row)].append(row)
        except ValueError:
            continue
    return dict(grouped)


def select_companies(
    grouped: dict[str, list[dict[str, Any]]], state_rows: list[dict[str, Any]], batch_size: int, now: datetime | None = None,
) -> list[str]:
    prior = {str(row.get("RowKey")): row for row in state_rows}
    candidates: list[str] = []
    for key in grouped:
        state = prior.get(key)
        if not state or state.get("Status") not in {"enabled", "not_enabled"}:
            candidates.append(key)
    return sorted(candidates)[:batch_size]


def markdown_from_pdf(
    document: dict[str, Any],
    content: bytes,
    resolved_source_url: str | None = None,
) -> str:
    if not content.startswith(b"%PDF-"):
        raise DocumentExtractionError("response is not a PDF")
    if len(content) > MAX_PDF_BYTES:
        raise DocumentExtractionError("PDF exceeds size limit")
    pdf = fitz.open(stream=content, filetype="pdf")
    try:
        pages = [page.get_text("text").replace("\x00", "").strip() for page in pdf]
        sparse_pages = [
            index for index, page in enumerate(pdf)
            if len(pages[index]) < SPARSE_PAGE_CHAR_THRESHOLD and page.get_images()
        ]
        if sparse_pages:
            try:
                import pytesseract
                from PIL import Image

                for index in sparse_pages:
                    pixmap = pdf[index].get_pixmap(matrix=fitz.Matrix(150 / 72, 150 / 72), alpha=False)
                    image = Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
                    pages[index] = pytesseract.image_to_string(image).replace("\x00", "").strip()
                    image.close()
            except Exception as exc:
                if len("\n".join(pages)) < MIN_EXTRACTED_CHARS:
                    raise DocumentExtractionError(
                        f"PDF text extraction was too thin and OCR failed: {type(exc).__name__}: {exc}"
                    ) from exc
    finally:
        pdf.close()
    text = "\n\n".join(page for page in pages if page)
    if len(text) < MIN_EXTRACTED_CHARS:
        raise DocumentExtractionError("PDF text extraction was too thin after OCR")
    title = re.sub(r"\s+", " ", str(document.get("company_name") or document["scrip_code"])).strip()
    period = str(document.get("doc_period_end_date") or "unknown")
    source_url = public_document_url(resolved_source_url or str(document["pdf_url"]))
    return (
        "---\n"
        f"news_id: direct-{document['id']}\n"
        f"source_pdf: {source_url}\n"
        f"document_type: {document['doc_type']}\n"
        f"period_end_date: {period}\n"
        "extracted: ok\n"
        "---\n\n"
        f"# {title} {document['doc_type'].replace('_', ' ').title()} ({period})\n\n{text}\n"
    )


def download_markdown(document: dict[str, Any]) -> str:
    original_url = str(document.get("pdf_url") or "")
    try:
        source_url = requestable_document_url(original_url)
        for _ in range(4):
            response = requests.get(source_url, timeout=(10, 90), allow_redirects=False, headers={"User-Agent": "TickerVectorResearch/1.0"})
            if not response.is_redirect:
                response.raise_for_status()
                break
            location = response.headers.get("Location")
            if not location:
                raise DocumentUnavailableError("redirect response is missing a location")
            source_url = requestable_document_url(requests.compat.urljoin(source_url, location))
        else:
            raise DocumentUnavailableError("document URL redirected too many times")
    except DocumentUnavailableError:
        raise
    except (ValueError, requests.RequestException) as exc:
        raise DocumentUnavailableError(f"{type(exc).__name__}: {exc}") from exc
    if response.content.startswith(b"%PDF-"):
        persist_resolved_document_url(document, source_url)
    return markdown_from_pdf(document, response.content, source_url)


def prepare_company(key: str, documents: list[dict[str, Any]], library_root: Path) -> list[DocumentFailure]:
    errors: list[DocumentFailure] = []
    for document in documents:
        category = DOCUMENT_TYPES[str(document["doc_type"])]
        period = str(document.get("doc_period_end_date") or "undated")
        destination = library_root / key / category / period[:4] / f"{period}__direct-{document['id']}.md"
        try:
            markdown = download_markdown(document)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists() or destination.read_text(encoding="utf-8") != markdown:
                destination.write_text(markdown, encoding="utf-8")
        except DocumentUnavailableError as exc:
            errors.append(DocumentFailure(str(document["id"]), "unavailable", str(exc)))
        except Exception as exc:
            errors.append(DocumentFailure(
                str(document["id"]), "extraction_error", f"{type(exc).__name__}: {exc}",
            ))
    return errors


def build_deterministic_evidence(record: Any, markdown: str) -> list[dict[str, Any]]:
    evidence = []
    seen = set()
    for source_offset, source in document_windows(
        markdown,
        max_chars=ANALYSIS_WINDOW_CHARS,
        max_windows=MAX_WINDOWS_PER_DOCUMENT,
    ):
        text = source.strip()
        if source_offset == 0 and text.startswith("---"):
            _prefix, separator, remainder = text.partition("\n---")
            if separator:
                text = remainder.lstrip("-\n ")
        if len(text) > EVIDENCE_PASSAGE_CHARS:
            end = max(
                text.rfind("\n\n", 900, EVIDENCE_PASSAGE_CHARS),
                text.rfind(". ", 900, EVIDENCE_PASSAGE_CHARS),
            )
            text = text[:end + 1 if end >= 900 else EVIDENCE_PASSAGE_CHARS].strip()
        if len(text) < 12:
            continue
        identity = hashlib.sha256(re.sub(r"\s+", " ", text).lower().encode("utf-8")).hexdigest()
        if identity in seen:
            continue
        seen.add(identity)
        headings = [match.group(2).strip() for match in re.finditer(r"^(#{1,4})\s+(.+?)\s*$", markdown[:source_offset + 1], re.MULTILINE)]
        evidence.append({
            "claim_type": "business_fact",
            "statement": text,
            "metric": None,
            "target": None,
            "target_period": None,
            "citation": {
                "document_id": record.document_id,
                "content_sha256": record.content_sha256,
                "source_pdf": record.source_pdf,
                "filing_date": record.filing_date,
                "title": record.title,
                "heading": headings[-1] if headings else None,
                "quote": text,
            },
        })
    return evidence


def seed_deterministic_evidence_caches(
    key: str,
    records: list[Any],
    paths: dict[str, Path],
    output_root: Path,
) -> int:
    claims_root = output_root / key / "claims"
    seeded = 0
    for record in records:
        if record.company_key != key:
            continue
        cache_path = claims_root / claim_cache_name(record)
        if load_cached_claims(cache_path, record) is not None:
            continue
        markdown = paths[record.document_id].read_text(encoding="utf-8", errors="replace")
        write_cached_claims(cache_path, record, build_deterministic_evidence(record, markdown))
        seeded += 1
    return seeded


def state_rows(store: AzureStore) -> list[dict[str, Any]]:
    return list(store.state.query_entities(f"PartitionKey eq '{STATE_PARTITION}'"))


def record_direct_state(
    store: AzureStore,
    key: str,
    documents: list[dict[str, Any]],
    status: str,
    detail: str,
    usable_count: int = 0,
    unavailable_count: int = 0,
    failed_count: int = 0,
) -> None:
    store.state.upsert_entity({
        "PartitionKey": STATE_PARTITION,
        "RowKey": key,
        "Status": status,
        "AskAiEnabled": status == "enabled",
        "DocumentCount": len(documents),
        "UsableDocumentCount": usable_count,
        "UnavailableDocumentCount": unavailable_count,
        "FailedDocumentCount": failed_count,
        "SourceSetHash": source_set_hash(documents),
        "UpdatedAt": utc_now(),
        "Detail": detail[:1000],
    }, mode=UpdateMode.MERGE)


def initialize_company_audit(store: AzureStore, grouped: dict[str, list[dict[str, Any]]], existing: list[dict[str, Any]]) -> None:
    known = {str(row.get("RowKey") or "") for row in existing}
    pending = []
    for key, documents in sorted(grouped.items()):
        if key in known:
            continue
        pending.append(("create", {
            "PartitionKey": STATE_PARTITION,
            "RowKey": key,
            "Status": "pending",
            "AskAiEnabled": False,
            "DocumentCount": len(documents),
            "UsableDocumentCount": 0,
            "UnavailableDocumentCount": 0,
            "FailedDocumentCount": 0,
            "SourceSetHash": source_set_hash(documents),
            "UpdatedAt": utc_now(),
            "Detail": "awaiting Ask AI enablement",
        }))
    for start in range(0, len(pending), 100):
        store.state.submit_transaction(pending[start:start + 100])


def record_document_failures(store: AzureStore, documents: list[dict[str, Any]], errors: list[DocumentFailure]) -> None:
    failures = {error.document_id: error for error in errors}
    for document in documents:
        document_id = str(document.get("id") or "")
        failure = failures.get(document_id)
        if failure is None:
            continue
        store.state.upsert_entity({
            "PartitionKey": STATE_PARTITION,
            "RowKey": f"DOCUMENT|{document_id}",
            "Status": failure.status,
            "SourceUrl": str(document.get("pdf_url") or "")[:1000],
            "UpdatedAt": utc_now(),
            "Detail": failure.detail[:1000],
        }, mode=UpdateMode.MERGE)


def available_documents(documents: list[dict[str, Any]], state_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unavailable = {
        str(row.get("RowKey"))[9:]
        for row in state_rows
        if str(row.get("RowKey") or "").startswith("DOCUMENT|") and row.get("Status") == "unavailable"
    }
    return [document for document in documents if str(document.get("id") or "") not in unavailable]


def claim_company_lease(store: AzureStore, key: str, now: datetime | None = None) -> bool:
    claimed_at = now or datetime.now(timezone.utc)
    entity = {
        "PartitionKey": LEASE_PARTITION,
        "RowKey": key,
        "ClaimedAt": claimed_at.isoformat(),
        "ExpiresAt": (claimed_at + LEASE_DURATION).isoformat(),
    }
    try:
        store.state.create_entity(entity)
        return True
    except ResourceExistsError:
        try:
            existing = store.state.get_entity(partition_key=LEASE_PARTITION, row_key=key)
            expires_at = datetime.fromisoformat(str(existing.get("ExpiresAt") or "").replace("Z", "+00:00"))
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at > claimed_at:
                return False
            store.state.delete_entity(partition_key=LEASE_PARTITION, row_key=key)
        except (ResourceNotFoundError, ValueError):
            try:
                store.state.delete_entity(partition_key=LEASE_PARTITION, row_key=key)
            except ResourceNotFoundError:
                pass
        try:
            store.state.create_entity(entity)
            return True
        except ResourceExistsError:
            return False


def release_company_lease(store: AzureStore, key: str) -> None:
    try:
        store.state.delete_entity(partition_key=LEASE_PARTITION, row_key=key)
    except ResourceNotFoundError:
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--company-keys", default="", help="Optional comma-separated company keys")
    parser.add_argument("--library-root", default="DirectDocumentLibrary")
    parser.add_argument("--output-root", default="direct-document-output")
    parser.add_argument("--skip-analysis", action="store_true")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--upload-only", action="store_true")
    parser.add_argument("--seed-from-azure", action="store_true")
    parser.add_argument("--canonicalize-only", action="store_true")
    parser.add_argument("--canonicalize-limit", type=int, default=800)
    parser.add_argument("--canonicalize-delay-seconds", type=float, default=6.5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.prepare_only and args.upload_only:
        raise SystemExit("--prepare-only and --upload-only cannot be combined")
    if not 1 <= args.batch_size <= 10:
        raise SystemExit("--batch-size must be between 1 and 10")
    if not 1 <= args.canonicalize_limit <= 2000:
        raise SystemExit("--canonicalize-limit must be between 1 and 2000")
    if args.canonicalize_delay_seconds < 1:
        raise SystemExit("--canonicalize-delay-seconds must be at least 1")
    library_root = Path(args.library_root).resolve()
    output_root = Path(args.output_root).resolve()
    library_root.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    store = AzureStore()

    if args.canonicalize_only:
        _resolved, _removed, failed = canonicalize_stored_document_urls(
            fetch_documents(), args.canonicalize_limit, args.canonicalize_delay_seconds,
        )
        return 1 if failed else 0

    if args.upload_only:
        records, paths = discover_documents(library_root)
        for key in sorted({record.company_key for record in records}):
            upload_prepared_company(key, records, paths, output_root, store)
        return 0

    all_grouped = group_documents(fetch_documents())
    existing_state = state_rows(store)
    initialize_company_audit(store, all_grouped, existing_state)
    grouped = {key: available_documents(documents, existing_state) for key, documents in all_grouped.items()}
    selected = [key for key in args.company_keys.split(",") if key] or select_companies(grouped, existing_state, args.batch_size)
    if not selected:
        print("No document-link companies require ingestion")
        return 0
    nvidia = None if args.skip_analysis else NvidiaClient()
    failures = []
    for key in selected:
        documents = grouped.get(key)
        all_documents = all_grouped.get(key)
        if all_documents is None:
            failures.append(key)
            continue
        if not documents:
            record_direct_state(
                store, key, all_documents, "not_enabled", "all stored document links are unavailable",
                usable_count=0, unavailable_count=len(all_documents), failed_count=len(all_documents),
            )
            continue
        if not claim_company_lease(store, key):
            print(f"Skipping {key}: already leased by another ingestion run")
            continue
        try:
            record_direct_state(
                store, key, all_documents, "processing", "downloading stored document links",
                unavailable_count=len(all_documents) - len(documents),
            )
            if args.seed_from_azure:
                store.hydrate_company_library(key, library_root / key, output_root / key / "claims")
            errors = prepare_company(key, documents, library_root)
            record_document_failures(store, documents, errors)
            records, paths = discover_documents(library_root)
            seeded_evidence = seed_deterministic_evidence_caches(key, records, paths, output_root)
            print(f"Seeded deterministic evidence for {seeded_evidence} document(s) in {key}")
            failed_ids = {error.document_id for error in errors}
            successful_documents = [
                document for document in documents
                if str(document.get("id") or "") not in failed_ids
            ]
            error_detail = "; ".join(
                f"{error.document_id}: {error.status}: {error.detail}" for error in errors
            )
            unavailable_count = sum(error.status == "unavailable" for error in errors)
            if not successful_documents:
                record_direct_state(
                    store, key, all_documents, "not_enabled", error_detail,
                    usable_count=0, unavailable_count=unavailable_count, failed_count=len(errors),
                )
                continue
            try:
                process_company(
                    key,
                    records,
                    paths,
                    output_root,
                    None,
                    nvidia,
                    0,
                    ANALYSIS_WINDOW_CHARS,
                    MAX_WINDOWS_PER_DOCUMENT,
                )
                if not args.prepare_only:
                    published = upload_prepared_company(key, records, paths, output_root, store)
                    if not published:
                        failures.append(key)
                        record_direct_state(
                            store, key, all_documents, "error",
                            "No publishable research snapshot was produced; the company will retry.",
                            usable_count=len(successful_documents),
                            unavailable_count=unavailable_count,
                            failed_count=len(errors),
                        )
                        continue
                    record_direct_state(
                        store, key, all_documents, "enabled", error_detail or "ok",
                        usable_count=len(successful_documents),
                        unavailable_count=unavailable_count,
                        failed_count=len(errors),
                    )
            except Exception as exc:
                failures.append(key)
                log.exception("Ask AI ingestion failed for %s", key)
                if not args.prepare_only:
                    record_direct_state(
                        store, key, all_documents, "error", f"processing failed: {type(exc).__name__}: {exc}",
                        usable_count=len(successful_documents),
                        unavailable_count=unavailable_count,
                        failed_count=len(errors),
                    )
        finally:
            release_company_lease(store, key)
    if failures:
        print(f"Ask AI ingestion failed for: {', '.join(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())