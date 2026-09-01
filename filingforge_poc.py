"""FilingForge -> Azure Blob -> cited NVIDIA company-research POC.

The POC intentionally stores Markdown only. FilingForge PDFs are temporary
conversion inputs and are deleted before the job exits. Each logical document
gets a stable manifest identity so physical Blob packing can change later
without breaking citations.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from azure.core.exceptions import ResourceExistsError
from azure.data.tables import TableServiceClient, UpdateMode
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from jsonschema import Draft202012Validator, FormatChecker
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("filingforge_poc")

DATE_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2})")
HEADING = re.compile(r"^(#{1,4})\s+(.+?)\s*$", re.MULTILINE)
WHITESPACE = re.compile(r"\s+")
CURATED_RESEARCH_CATEGORIES = {"annual-reports", "quarterly", "investor-ppts", "concalls"}
RESEARCH_CATEGORY_PRIORITY = {
    "concalls": 0,
    "investor-ppts": 1,
    "annual-reports": 2,
    "quarterly": 3,
}


@dataclass(frozen=True)
class DocumentRecord:
    document_id: str
    source_news_id: str | None
    source_pdf: str | None
    extraction_status: str
    converter_version: str
    company_key: str
    category: str
    filing_date: str | None
    title: str
    relative_path: str
    blob_name: str
    pack_key: str
    content_sha256: str
    byte_length: int
    headings: list[str]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: str) -> str:
    return WHITESPACE.sub(" ", html.unescape(value or "")).strip().lower()


def parse_frontmatter(text: str) -> dict[str, str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    values: dict[str, str] = {}
    for line in lines[1:]:
        if line.strip() == "---":
            return values
        key, separator, value = line.partition(":")
        if separator:
            values[key.strip()] = value.strip()
    return {}


def parse_json_object(value: str) -> dict[str, Any]:
    text = (value or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE)
    decoder = json.JSONDecoder()
    for start, character in enumerate(text):
        if character != "{":
            continue
        try:
            parsed, _ = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise NvidiaResponseError("NVIDIA response did not contain a complete JSON object")


class NvidiaResponseError(RuntimeError):
    pass


def build_document_record(company_dir: Path, markdown_path: Path) -> DocumentRecord:
    relative = markdown_path.relative_to(company_dir).as_posix()
    parts = Path(relative).parts
    category = parts[0] if len(parts) > 1 else "other"
    raw = markdown_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    filing_date_match = DATE_PREFIX.match(markdown_path.stem)
    filing_date = filing_date_match.group(1) if filing_date_match else None
    title_start = 11 if filing_date else 0
    title = markdown_path.stem[title_start:].split("__", 1)[0].replace("_", " ").strip()
    text = raw.decode("utf-8", errors="replace")
    frontmatter = parse_frontmatter(text)
    source_news_id = frontmatter.get("news_id") or None
    if not source_news_id and "__" in markdown_path.stem:
        source_news_id = markdown_path.stem.rsplit("__", 1)[-1]
    stable_source = source_news_id or relative
    headings = [match.group(2).strip() for match in HEADING.finditer(text)][:100]
    company_key = company_dir.name
    year = filing_date[:4] if filing_date else "undated"
    identity_hash = hashlib.sha256(f"{company_key}|{stable_source}".encode("utf-8")).hexdigest()
    document_id = f"ff-{identity_hash[:24]}"
    return DocumentRecord(
        document_id=document_id,
        source_news_id=source_news_id,
        source_pdf=frontmatter.get("source_pdf") or None,
        extraction_status=frontmatter.get("extracted") or "unknown",
        converter_version=os.environ.get("FILINGFORGE_VERSION", "unknown"),
        company_key=company_key,
        category=category,
        filing_date=filing_date,
        title=title or markdown_path.stem,
        relative_path=relative,
        blob_name=f"companies/{company_key}/documents/{category}/{year}/{document_id}/{digest}.md",
        pack_key=f"companies/{company_key}/packs/{year}/{category}.jsonl.zst",
        content_sha256=digest,
        byte_length=len(raw),
        headings=headings,
    )


def discover_documents(library_root: Path) -> tuple[list[DocumentRecord], dict[str, Path]]:
    records: list[DocumentRecord] = []
    paths: dict[str, Path] = {}
    for company_dir in sorted(path for path in library_root.iterdir() if path.is_dir() and not path.name.startswith("_")):
        for markdown_path in sorted(company_dir.rglob("*.md")):
            if markdown_path.name == "INDEX.md" or "research_report" in markdown_path.parts:
                continue
            record = build_document_record(company_dir, markdown_path)
            records.append(record)
            paths[record.document_id] = markdown_path
    return records, paths


def run_filingforge(company: str, library_root: Path, years: int) -> None:
    command = [sys.executable, "-m", "engine", company, str(library_root), "--years", str(years)]
    log.info("Pulling %s with FilingForge (%d years)", company, years)
    subprocess.run(command, check=True)


def remove_temporary_pdfs(library_root: Path) -> int:
    removed = 0
    for pdf_path in library_root.rglob("*.pdf"):
        pdf_path.unlink()
        removed += 1
    return removed


class AzureStore:
    def __init__(self) -> None:
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip()
        account_name = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
        if connection_string:
            self.blobs = BlobServiceClient.from_connection_string(connection_string)
            self.tables = TableServiceClient.from_connection_string(connection_string)
        elif account_name:
            credential = DefaultAzureCredential(exclude_managed_identity_credential=True)
            self.blobs = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net", credential=credential
            )
            self.tables = TableServiceClient(
                endpoint=f"https://{account_name}.table.core.windows.net", credential=credential
            )
        else:
            raise RuntimeError("Set AZURE_STORAGE_ACCOUNT for OIDC/Azure CLI, or AZURE_STORAGE_CONNECTION_STRING")

        self.markdown = self.blobs.get_container_client(
            os.environ.get("FILINGS_CONTAINER", "filings-md")
        )
        self.snapshots = self.blobs.get_container_client(
            os.environ.get("RESEARCH_CONTAINER", "research-snapshots")
        )
        table_name = os.environ.get("RESEARCH_STATE_TABLE", "ResearchIngestionState")
        self.state = self.tables.get_table_client(table_name)

    @staticmethod
    def _upload(container, name: str, data: bytes, content_type: str, overwrite: bool = True) -> None:
        try:
            container.upload_blob(
                name,
                data,
                overwrite=overwrite,
                content_settings=ContentSettings(content_type=f"{content_type}; charset=utf-8"),
            )
        except ResourceExistsError:
            if overwrite:
                raise

    def upload_document(self, record: DocumentRecord, path: Path) -> None:
        self._upload(self.markdown, record.blob_name, path.read_bytes(), "text/markdown", overwrite=False)

    def upload_manifest(self, company_key: str, manifest: dict[str, Any]) -> None:
        payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._upload(self.markdown, f"companies/{company_key}/manifest.json", payload, "application/json")

    def upload_snapshot(self, company_key: str, snapshot: dict[str, Any]) -> None:
        payload = json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
        self._upload(self.snapshots, f"companies/{company_key}/latest.json", payload, "application/json")
        version = snapshot["generated_at"].replace(":", "-")
        self._upload(self.snapshots, f"companies/{company_key}/history/{version}.json", payload, "application/json")

    def record_state(self, company_key: str, status: str, document_count: int, detail: str = "") -> None:
        safe_key = re.sub(r"[/\\#?]", "-", company_key)[:1024]
        self.state.upsert_entity(
            {
                "PartitionKey": "FILINGFORGE_POC",
                "RowKey": safe_key,
                "Status": status,
                "DocumentCount": document_count,
                "UpdatedAt": utc_now(),
                "Detail": detail[:1000],
            },
            mode=UpdateMode.MERGE,
        )


class NvidiaClient:
    def __init__(self) -> None:
        self.api_key = os.environ.get("NVIDIA_NIM_API_KEY", "").strip()
        self.model = (
            os.environ.get("NVIDIA_NIM_MODEL") or "nvidia/nvidia-nemotron-nano-9b-v2"
        ).strip()
        self.base_url = os.environ.get(
            "NVIDIA_NIM_BASE_URL", "https://integrate.api.nvidia.com/v1"
        ).rstrip("/")
        if not self.api_key:
            raise RuntimeError("NVIDIA_NIM_API_KEY is required unless --skip-analysis is used")
        self.session = requests.Session()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=20),
        retry=retry_if_exception_type((requests.RequestException, NvidiaResponseError)),
        reraise=True,
    )
    def json_completion(self, system: str, user: str, max_tokens: int = 6000) -> dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json={
                "model": self.model,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "temperature": 0.1,
                "max_tokens": max_tokens,
            },
            timeout=180,
        )
        response.raise_for_status()
        choice = response.json()["choices"][0]
        if choice.get("finish_reason") == "length":
            raise NvidiaResponseError("NVIDIA response was truncated at the output-token limit")
        content = choice["message"]["content"]
        return parse_json_object(content)


def select_research_documents(records: list[DocumentRecord], limit: int) -> list[DocumentRecord]:
    categories = sorted(RESEARCH_CATEGORY_PRIORITY, key=RESEARCH_CATEGORY_PRIORITY.get)
    by_category = {
        category: sorted(
            (
                record for record in records
                if record.category == category and record.extraction_status in {"ok", "unknown"}
            ),
            key=lambda record: record.filing_date or "",
            reverse=True,
        )
        for category in categories
    }
    selected: list[DocumentRecord] = []
    offset = 0
    while len(selected) < limit:
        added = False
        for category in categories:
            category_records = by_category[category]
            if offset < len(category_records):
                selected.append(category_records[offset])
                added = True
                if len(selected) == limit:
                    break
        if not added:
            break
        offset += 1
    return selected


def document_windows(markdown: str, max_chars: int, max_windows: int) -> list[tuple[int, str]]:
    if max_chars <= 0 or max_windows <= 0:
        return []
    if len(markdown) <= max_chars:
        return [(0, markdown)]
    if max_windows == 1:
        return [(0, markdown[:max_chars])]

    last_start = len(markdown) - max_chars
    starts = sorted({round(last_start * index / (max_windows - 1)) for index in range(max_windows)})
    return [(start, markdown[start : start + max_chars]) for start in starts]


def extract_supported_claims(
    client: NvidiaClient, record: DocumentRecord, source: str, source_offset: int = 0
) -> list[dict[str, Any]]:
    system = """You extract auditable equity-research evidence from one official company filing.
Return JSON only. Do not infer missing numbers or dates. Every claim must contain a short verbatim quote
copied from SOURCE. Allowed claim_type values: business_fact, positive, risk, guidance, outcome.
Guidance means a forward-looking management promise, target, milestone or expectation. Outcome means
later evidence about delivery. Use null for unknown metric/target/target_period."""
    user = f"""DOCUMENT_ID: {record.document_id}
CATEGORY: {record.category}
FILING_DATE: {record.filing_date}
TITLE: {record.title}
SOURCE_CHARACTER_OFFSET: {source_offset}

SOURCE:
{source}

Return:
{{"claims":[{{"claim_type":"guidance","statement":"...","metric":null,"target":null,
"target_period":null,"quote":"exact source text","heading":null}}]}}"""
    result = client.json_completion(system, user)
    supported: list[dict[str, Any]] = []
    normalized_source = normalize_text(source)
    for claim in result.get("claims", []):
        if not isinstance(claim, dict) or claim.get("claim_type") not in {
            "business_fact", "positive", "risk", "guidance", "outcome"
        }:
            continue
        quote = str(claim.get("quote") or "").strip()
        if len(quote) < 12 or normalize_text(quote) not in normalized_source:
            continue
        supported.append(
            {
                "claim_type": claim["claim_type"],
                "statement": str(claim.get("statement") or "").strip(),
                "metric": claim.get("metric"),
                "target": claim.get("target"),
                "target_period": claim.get("target_period"),
                "citation": {
                    "document_id": record.document_id,
                    "content_sha256": record.content_sha256,
                    "source_pdf": record.source_pdf,
                    "filing_date": record.filing_date,
                    "title": record.title,
                    "heading": claim.get("heading"),
                    "quote": quote,
                },
            }
        )
    return supported


def synthesize_company_research(
    client: NvidiaClient, company_key: str, claims: list[dict[str, Any]]
) -> dict[str, Any]:
    system = """You are producing a cited, detailed Indian-equity company research snapshot from a closed
set of validated evidence claims. Return JSON only. Never introduce a fact, number, target or date absent
from EVIDENCE. Every positive, risk, guidance item, deliverable and walk-the-talk assessment must cite one
or more document_id values present in EVIDENCE. Pending future guidance is not a miss. Use unverifiable when
evidence is insufficient. Separate fact from interpretation."""
    user = f"""COMPANY_KEY: {company_key}
EVIDENCE:
{json.dumps(claims, ensure_ascii=False)}

Return this shape:
{{
    "overview": {{"sections":[{{"heading":"Business model", "text":"", "document_ids":[]}}]}},
  "positives": [{{"point":"", "why_it_matters":"", "document_ids":[]}}],
  "risks": [{{"point":"", "why_it_matters":"", "document_ids":[]}}],
  "management_guidance": [{{"guidance_id":"G1", "statement":"", "metric":null,
    "target":null, "target_period":null, "document_ids":[]}}],
  "key_deliverables": [{{"deliverable":"", "metric_or_milestone":"", "due_period":null,
    "status":"pending", "document_ids":[]}}],
  "walk_the_talk": [{{"guidance_id":"G1", "status":"achieved|partially_achieved|missed|pending|unverifiable",
    "assessment":"", "guidance_document_ids":[], "outcome_document_ids":[]}}]
}}"""
    result = client.json_completion(system, user, max_tokens=12000)
    valid_ids = {
        claim["citation"]["document_id"]
        for claim in claims
        if isinstance(claim.get("citation"), dict) and claim["citation"].get("document_id")
    }
    for collection in ("positives", "risks", "management_guidance", "key_deliverables"):
        result[collection] = [
            item for item in result.get(collection, [])
            if isinstance(item, dict)
            and item.get("document_ids")
            and set(item["document_ids"]).issubset(valid_ids)
        ]
    overview = result.get("overview") if isinstance(result.get("overview"), dict) else {}
    overview["sections"] = [
        item for item in overview.get("sections", [])
        if isinstance(item, dict)
        and item.get("document_ids")
        and set(item["document_ids"]).issubset(valid_ids)
    ]
    result["overview"] = overview
    result["walk_the_talk"] = [
        item for item in result.get("walk_the_talk", [])
        if isinstance(item, dict)
        and set(item.get("guidance_document_ids", [])).issubset(valid_ids)
        and set(item.get("outcome_document_ids", [])).issubset(valid_ids)
    ]
    return result


def build_manifest(company_key: str, records: list[DocumentRecord]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "company_key": company_key,
        "generated_at": utc_now(),
        "document_count": len(records),
        "documents": [asdict(record) for record in records],
    }


def validate_snapshot(snapshot: dict[str, Any]) -> None:
    schema_path = Path(__file__).with_name("schemas") / "company-research.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(snapshot)


def process_company(
    company_key: str,
    records: list[DocumentRecord],
    paths: dict[str, Path],
    output_root: Path,
    store: AzureStore | None,
    nvidia: NvidiaClient | None,
    max_analysis_documents: int,
    max_chars_per_document: int,
    max_windows_per_document: int,
) -> None:
    company_records = [record for record in records if record.company_key == company_key]
    manifest = build_manifest(company_key, company_records)
    company_output = output_root / company_key
    company_output.mkdir(parents=True, exist_ok=True)
    (company_output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if store:
        for record in company_records:
            store.upload_document(record, paths[record.document_id])
        store.upload_manifest(company_key, manifest)

    snapshot: dict[str, Any] | None = None
    if nvidia:
        claims: list[dict[str, Any]] = []
        selected = select_research_documents(company_records, max_analysis_documents)
        for record in selected:
            markdown = paths[record.document_id].read_text(encoding="utf-8", errors="replace")
            for source_offset, source in document_windows(
                markdown, max_chars_per_document, max_windows_per_document
            ):
                claims.extend(extract_supported_claims(nvidia, record, source, source_offset))
        if not claims:
            raise NvidiaResponseError(f"No validated evidence was extracted for {company_key}")
        research = synthesize_company_research(nvidia, company_key, claims)
        snapshot = {
            "schema_version": 1,
            "company_key": company_key,
            "generated_at": utc_now(),
            "source_document_count": len(selected),
            "validated_evidence_count": len(claims),
            "research": research,
            "evidence": claims,
        }
        validate_snapshot(snapshot)
        (company_output / "research.json").write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        if store:
            store.upload_snapshot(company_key, snapshot)

    if store:
        store.record_state(company_key, "complete", len(company_records), f"analysis={bool(snapshot)}")
    log.info("%s: %d Markdown documents, analysis=%s", company_key, len(company_records), bool(snapshot))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--companies", default="SHILPAMED,MARUTI,HDFCBANK")
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument("--library-root")
    parser.add_argument("--output-root", default="poc-output")
    parser.add_argument("--skip-pull", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Write local outputs without Azure")
    parser.add_argument("--max-analysis-documents", type=int, default=10)
    parser.add_argument("--max-chars-per-document", type=int, default=30000)
    parser.add_argument("--max-windows-per-document", type=int, default=2)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    companies = [company.strip() for company in args.companies.split(",") if company.strip()]
    if not companies:
        raise SystemExit("--companies must contain at least one company")

    temporary_root: tempfile.TemporaryDirectory[str] | None = None
    if args.library_root:
        library_root = Path(args.library_root).resolve()
        library_root.mkdir(parents=True, exist_ok=True)
    else:
        temporary_root = tempfile.TemporaryDirectory(prefix="filingforge-poc-")
        library_root = Path(temporary_root.name)

    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    store = None if args.dry_run else AzureStore()
    nvidia = None if args.skip_analysis else NvidiaClient()
    failures: list[str] = []

    try:
        if not args.skip_pull:
            for company in companies:
                try:
                    run_filingforge(company, library_root, args.years)
                except subprocess.CalledProcessError as exc:
                    failures.append(company)
                    log.exception("FilingForge pull failed for %s; continuing with other companies", company)
                    if store:
                        store.record_state(f"request-{company}", "pull_failed", 0, str(exc))

        records, paths = discover_documents(library_root)
        company_keys = sorted({record.company_key for record in records})
        if not company_keys:
            raise RuntimeError("FilingForge produced no Markdown documents")

        for company_key in company_keys:
            try:
                process_company(
                    company_key,
                    records,
                    paths,
                    output_root,
                    store,
                    nvidia,
                    args.max_analysis_documents,
                    args.max_chars_per_document,
                    args.max_windows_per_document,
                )
            except Exception as exc:
                failures.append(company_key)
                log.exception("Processing failed for %s; continuing with other companies", company_key)
                if store:
                    try:
                        store.record_state(company_key, "processing_failed", 0, str(exc))
                    except Exception:
                        log.exception("Could not record failure state for %s", company_key)
        removed = remove_temporary_pdfs(library_root)
        log.info("Removed %d temporary PDFs; Markdown is the only retained filing format", removed)
        if failures:
            log.error("POC completed with failures: %s", ", ".join(failures))
            return 1
        return 0
    finally:
        if temporary_root:
            temporary_root.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())