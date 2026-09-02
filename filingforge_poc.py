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
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from jsonschema import Draft202012Validator, FormatChecker
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


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


class NvidiaRequestError(NvidiaResponseError):
    def __init__(self, model: str, status_code: int) -> None:
        self.status_code = status_code
        if status_code in {401, 403}:
            guidance = "verify NVIDIA_NIM_API_KEY has hosted-inference access"
        elif status_code == 404:
            guidance = "verify the model supports hosted chat completions"
        else:
            guidance = "check the NVIDIA API status and model configuration"
        super().__init__(
            f"NVIDIA model {model!r} returned HTTP {status_code} from /chat/completions; {guidance}"
        )


def is_retryable_nvidia_error(exc: BaseException) -> bool:
    if isinstance(exc, NvidiaRequestError):
        return exc.status_code in {408, 429} or exc.status_code >= 500
    return isinstance(exc, (requests.RequestException, NvidiaResponseError))


def validate_nvidia_model_id(model: str) -> str:
    model = str(model or "").strip()
    if "/" not in model:
        suffix = model[len("nvidia"):] if model.lower().startswith("nvidia") else model
        suggestion = f"nvidia/{suffix.lstrip('/-')}"
        raise ValueError(
            f"NVIDIA_NIM_MODEL must include its publisher prefix, for example {suggestion!r}; got {model!r}"
        )
    return model


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


def parse_company_spec(company_spec: str) -> tuple[str, str | None]:
    query, separator, scrip_code = company_spec.partition("|")
    query = query.strip()
    scrip_code = scrip_code.strip() if separator else None
    if not query:
        raise ValueError(f"Invalid company specification: {company_spec!r}")
    return query, scrip_code or None


def resolve_company_spec(company_spec: str, client, resolver=None) -> tuple[str, str, str]:
    query, expected_scrip_code = parse_company_spec(company_spec)
    if expected_scrip_code and not re.fullmatch(r"\d{6}", expected_scrip_code):
        raise ValueError(f"Pinned BSE scrip code must be exactly six digits: {expected_scrip_code!r}")

    if resolver is None:
        from engine import resolve as resolver

    candidates = []
    try:
        candidates = resolver(query, client)
    except Exception as exc:
        if not expected_scrip_code:
            raise
        log.warning(
            "%r did not resolve by name (%s); using pinned BSE scrip code %s",
            query,
            exc,
            expected_scrip_code,
        )

    if expected_scrip_code:
        chosen = next(
            (candidate for candidate in candidates if str(candidate.scrip_code) == expected_scrip_code),
            None,
        )
        company_name = chosen.company if chosen else query
        scrip_code = expected_scrip_code
    else:
        if not candidates:
            raise RuntimeError(f"{query!r} did not resolve to a BSE company")
        chosen = next((candidate for candidate in candidates if candidate.is_primary), candidates[0])
        company_name = chosen.company
        scrip_code = str(chosen.scrip_code)

    ticker_prefix = re.sub(r"[^A-Z0-9]+", "", company_name.split()[0].upper()) or scrip_code
    return company_name, scrip_code, f"{ticker_prefix}-{scrip_code}"


def run_filingforge(company_spec: str, library_root: Path, years: float) -> str:
    from engine import BSEClient, build_library

    client = BSEClient()
    try:
        company_name, scrip_code, ticker = resolve_company_spec(company_spec, client)
        log.info("Pulling %s (%s) with FilingForge (%g years)", company_name, scrip_code, years)
        result = build_library(
            scrip_code,
            ticker,
            library_root,
            [],
            years,
            client,
            everything=True,
        )
        log.info(
            "%s: %d new, %d already present, %d failed, %d pending",
            ticker,
            len(result.downloaded),
            len(result.skipped),
            len(result.failed),
            len(result.pending),
        )
        return ticker
    finally:
        client.close()


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
            credential = AzureCliCredential(process_timeout=30)
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

    def preflight(self) -> None:
        self.markdown.get_container_properties()
        self.snapshots.get_container_properties()
        next(
            self.state.query_entities(
                "PartitionKey eq 'FILINGFORGE_POC'",
                results_per_page=1,
            ),
            None,
        )
        log.info("Azure preflight passed for both Blob containers and the state table")

    def hydrate_company_library(self, company_key: str, company_dir: Path) -> int:
        manifest_name = f"companies/{company_key}/manifest.json"
        try:
            manifest = json.loads(
                self.markdown.get_blob_client(manifest_name).download_blob().readall()
            )
        except ResourceNotFoundError:
            log.info("%s: no remote manifest; starting a new FilingForge library", company_key)
            return 0

        company_root = company_dir.resolve()
        hydrated = 0
        seen_ids: set[str] = set()
        for document in manifest.get("documents", []):
            relative_path = str(document.get("relative_path") or "")
            blob_name = str(document.get("blob_name") or "")
            expected_hash = str(document.get("content_sha256") or "")
            if not relative_path or not blob_name or not expected_hash:
                raise RuntimeError(f"Remote manifest contains an incomplete document for {company_key}")
            destination = (company_root / Path(relative_path)).resolve()
            try:
                destination.relative_to(company_root)
            except ValueError as exc:
                raise RuntimeError(f"Unsafe remote manifest path for {company_key}: {relative_path}") from exc
            markdown = self.markdown.get_blob_client(blob_name).download_blob().readall()
            if hashlib.sha256(markdown).hexdigest() != expected_hash:
                raise RuntimeError(f"Remote Markdown hash mismatch for {blob_name}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(markdown)
            source_news_id = document.get("source_news_id")
            if source_news_id:
                seen_ids.add(str(source_news_id))
            hydrated += 1

        company_root.mkdir(parents=True, exist_ok=True)
        (company_root / ".filingforge_index.json").write_text(
            json.dumps(sorted(seen_ids)),
            encoding="utf-8",
        )
        log.info(
            "%s: hydrated %d verified Markdown documents and %d FilingForge identities from Azure",
            company_key,
            hydrated,
            len(seen_ids),
        )
        return hydrated

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

    def upload_analysis_status(self, company_key: str, status: dict[str, Any]) -> None:
        payload = json.dumps(status, ensure_ascii=False, indent=2).encode("utf-8")
        self._upload(
            self.snapshots,
            f"companies/{company_key}/analysis-status.json",
            payload,
            "application/json",
        )

    @staticmethod
    def _state_row_key(company_key: str) -> str:
        return re.sub(r"[/\\#?]", "-", company_key)[:1024]

    def record_state(self, company_key: str, status: str, document_count: int, detail: str = "") -> bool:
        try:
            self.state.upsert_entity(
                {
                    "PartitionKey": "FILINGFORGE_POC",
                    "RowKey": self._state_row_key(company_key),
                    "Status": status,
                    "DocumentCount": document_count,
                    "UpdatedAt": utc_now(),
                    "Detail": detail[:1000],
                },
                mode=UpdateMode.MERGE,
            )
            return True
        except Exception as exc:
            log.warning("Could not record Azure Table state for %s: %s", company_key, exc)
            return False

    def verify_company_upload(
        self,
        company_key: str,
        records: list[DocumentRecord],
        has_analysis: bool,
        has_analysis_status: bool,
    ) -> None:
        manifest_name = f"companies/{company_key}/manifest.json"
        remote_manifest = json.loads(
            self.markdown.get_blob_client(manifest_name).download_blob().readall()
        )
        remote_documents = {
            (document["document_id"], document["content_sha256"])
            for document in remote_manifest.get("documents", [])
        }
        expected_documents = {
            (record.document_id, record.content_sha256) for record in records
        }
        if remote_manifest.get("document_count") != len(records) or remote_documents != expected_documents:
            raise RuntimeError(f"Azure manifest verification failed for {company_key}")

        for record in records:
            remote_markdown = self.markdown.get_blob_client(record.blob_name).download_blob().readall()
            remote_hash = hashlib.sha256(remote_markdown).hexdigest()
            if len(remote_markdown) != record.byte_length or remote_hash != record.content_sha256:
                raise RuntimeError(f"Azure Markdown verification failed for {record.document_id}")

        if has_analysis:
            self.snapshots.get_blob_client(
                f"companies/{company_key}/latest.json"
            ).get_blob_properties()
        if has_analysis_status:
            self.snapshots.get_blob_client(
                f"companies/{company_key}/analysis-status.json"
            ).get_blob_properties()

        for container in (self.markdown, self.snapshots):
            pdfs = [
                blob.name
                for blob in container.list_blobs(name_starts_with=f"companies/{company_key}/")
                if blob.name.lower().endswith(".pdf")
            ]
            if pdfs:
                raise RuntimeError(f"Unexpected PDF blobs for {company_key}: {', '.join(pdfs[:5])}")

        state = self.state.get_entity(
            partition_key="FILINGFORGE_POC",
            row_key=self._state_row_key(company_key),
        )
        expected_detail = f"analysis={has_analysis}"
        if (
            state.get("Status") != "complete"
            or int(state.get("DocumentCount", -1)) != len(records)
            or state.get("Detail") != expected_detail
        ):
            raise RuntimeError(f"Azure Table state verification failed for {company_key}: {dict(state)}")
        log.info(
            "%s: verified Azure manifest, %d Markdown hashes, analysis=%s, table state, and zero PDFs",
            company_key,
            len(records),
            has_analysis,
        )


class NvidiaClient:
    def __init__(self) -> None:
        self.api_key = os.environ.get("NVIDIA_NIM_API_KEY", "").strip()
        self.model = (
            os.environ.get("NVIDIA_NIM_MODEL") or "nvidia/nemotron-3.5-lightning-30b-a3b"
        ).strip()
        validate_nvidia_model_id(self.model)
        self.extraction_model = (
            os.environ.get("NVIDIA_NIM_EXTRACTION_MODEL") or self.model
        ).strip()
        validate_nvidia_model_id(self.extraction_model)
        self.base_url = os.environ.get(
            "NVIDIA_NIM_BASE_URL", "https://integrate.api.nvidia.com/v1"
        ).rstrip("/")
        self.extraction_timeout = float(os.environ.get("NVIDIA_NIM_EXTRACTION_TIMEOUT_SECONDS", "60"))
        self.synthesis_timeout = float(os.environ.get("NVIDIA_NIM_SYNTHESIS_TIMEOUT_SECONDS", "180"))
        if not self.api_key:
            raise RuntimeError("NVIDIA_NIM_API_KEY is required unless --skip-analysis is used")
        self.session = requests.Session()

    def preflight(self) -> None:
        response = self.session.get(
            f"{self.base_url}/models",
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30,
        )
        response.raise_for_status()
        available_models = {
            str(model.get("id"))
            for model in response.json().get("data", [])
            if isinstance(model, dict) and model.get("id")
        }
        unavailable = [
            model for model in {self.model, self.extraction_model}
            if available_models and model not in available_models
        ]
        if unavailable:
            raise RuntimeError(f"NVIDIA model(s) not available to this API key: {', '.join(sorted(unavailable))}")

        probed_models: set[str] = set()
        for model, configured_timeout in (
            (self.extraction_model, self.extraction_timeout),
            (self.model, self.synthesis_timeout),
        ):
            if model in probed_models:
                continue
            result = self.json_completion(
                "Return JSON only.",
                'Return exactly {"ok": true}.',
                max_tokens=32,
                model=model,
                timeout_seconds=min(configured_timeout, 30),
            )
            if result.get("ok") is not True:
                raise NvidiaResponseError(f"NVIDIA preflight returned an unexpected response for {model}")
            probed_models.add(model)
        log.info(
            "NVIDIA completion preflight passed for synthesis=%s, extraction=%s",
            self.model,
            self.extraction_model,
        )

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=2, min=2, max=20),
        retry=retry_if_exception(is_retryable_nvidia_error),
        reraise=True,
    )
    def json_completion(
        self,
        system: str,
        user: str,
        max_tokens: int = 2000,
        model: str | None = None,
        timeout_seconds: float | None = None,
        temperature: float = 0.1,
        top_p: float = 0.95,
        enable_thinking: bool = False,
        reasoning_budget: int | None = None,
        stream: bool = False,
    ) -> dict[str, Any]:
        requested_model = model or self.model
        payload: dict[str, Any] = {
            "model": requested_model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
            "chat_template_kwargs": {"enable_thinking": enable_thinking},
            "stream": stream,
        }
        if reasoning_budget is not None:
            payload["reasoning_budget"] = reasoning_budget
        response = self.session.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=timeout_seconds or self.synthesis_timeout,
            stream=stream,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise NvidiaRequestError(requested_model, response.status_code) from exc
        if stream:
            content_parts: list[str] = []
            finish_reason = None
            for line in response.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                event = json.loads(data)
                choices = event.get("choices") or []
                if not choices:
                    continue
                choice = choices[0]
                finish_reason = choice.get("finish_reason") or finish_reason
                delta = choice.get("delta") or {}
                if delta.get("content"):
                    content_parts.append(delta["content"])
            content = "".join(content_parts)
        else:
            choice = response.json()["choices"][0]
            finish_reason = choice.get("finish_reason")
            content = choice["message"]["content"]
        if finish_reason == "length":
            raise NvidiaResponseError("NVIDIA response was truncated at the output-token limit")
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
    result = client.json_completion(
        system,
        user,
        max_tokens=4096,
        model=client.extraction_model,
        timeout_seconds=client.extraction_timeout,
    )
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
    result = client.json_completion(
        system,
        user,
        max_tokens=32768,
        model=client.model,
        timeout_seconds=client.synthesis_timeout,
        temperature=0.2,
        top_p=0.95,
        enable_thinking=True,
        reasoning_budget=8192,
        stream=True,
    )
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
        failed_windows = 0
        failure_messages: list[str] = []
        selected = select_research_documents(company_records, max_analysis_documents)
        for record in selected:
            markdown = paths[record.document_id].read_text(encoding="utf-8", errors="replace")
            for source_offset, source in document_windows(
                markdown, max_chars_per_document, max_windows_per_document
            ):
                try:
                    claims.extend(extract_supported_claims(nvidia, record, source, source_offset))
                except (requests.RequestException, NvidiaResponseError, ValueError) as exc:
                    failed_windows += 1
                    failure_messages.append(f"{record.document_id}@{source_offset}: {type(exc).__name__}")
                    log.warning(
                        "%s: skipping timed-out/invalid NVIDIA window %s@%d: %s",
                        company_key,
                        record.document_id,
                        source_offset,
                        exc,
                    )
                    if failed_windows >= 2:
                        log.warning("%s: stopping NVIDIA extraction after %d failed windows", company_key, failed_windows)
                        break
            if failed_windows >= 2:
                break

        if claims:
            try:
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
            except (requests.RequestException, NvidiaResponseError, ValueError) as exc:
                failure_messages.append(f"synthesis: {type(exc).__name__}")
                log.warning("%s: NVIDIA synthesis unavailable; Markdown will still upload: %s", company_key, exc)
        else:
            failure_messages.append("no validated evidence extracted")

        if snapshot is None:
            analysis_status = {
                "status": "unavailable",
                "generated_at": utc_now(),
                "failed_windows": failed_windows,
                "details": failure_messages,
            }
            (company_output / "analysis-status.json").write_text(
                json.dumps(analysis_status, indent=2),
                encoding="utf-8",
            )
            if store:
                store.upload_analysis_status(company_key, analysis_status)
        elif store:
            store.upload_analysis_status(
                company_key,
                {"status": "available", "generated_at": snapshot["generated_at"]},
            )

    if store:
        store.record_state(company_key, "complete", len(company_records), f"analysis={bool(snapshot)}")
    log.info("%s: %d Markdown documents, analysis=%s", company_key, len(company_records), bool(snapshot))


def upload_prepared_company(
    company_key: str,
    records: list[DocumentRecord],
    paths: dict[str, Path],
    output_root: Path,
    store: AzureStore,
) -> None:
    company_records = [record for record in records if record.company_key == company_key]
    company_output = output_root / company_key
    manifest_path = company_output / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(f"Prepared manifest is missing for {company_key}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    prepared = {
        (document["document_id"], document["content_sha256"])
        for document in manifest.get("documents", [])
    }
    discovered = {(record.document_id, record.content_sha256) for record in company_records}
    if prepared != discovered:
        raise RuntimeError(f"Prepared manifest does not match current Markdown library for {company_key}")

    for record in company_records:
        store.upload_document(record, paths[record.document_id])
    store.upload_manifest(company_key, manifest)

    research_path = company_output / "research.json"
    analysis_status_path = company_output / "analysis-status.json"
    has_analysis = research_path.exists()
    if has_analysis:
        snapshot = json.loads(research_path.read_text(encoding="utf-8"))
        validate_snapshot(snapshot)
        store.upload_snapshot(company_key, snapshot)
        store.upload_analysis_status(
            company_key,
            {"status": "available", "generated_at": snapshot["generated_at"]},
        )
    elif analysis_status_path.exists():
        store.upload_analysis_status(
            company_key,
            json.loads(analysis_status_path.read_text(encoding="utf-8")),
        )
    if not store.record_state(company_key, "complete", len(company_records), f"analysis={has_analysis}"):
        raise RuntimeError(f"Could not persist Azure Table completion state for {company_key}")
    store.verify_company_upload(
        company_key,
        company_records,
        has_analysis,
        analysis_status_path.exists() or has_analysis,
    )
    log.info("%s: uploaded %d Markdown documents, analysis=%s", company_key, len(company_records), has_analysis)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--companies",
        default="SHILPAMED|530549,Maruti Suzuki India|532500,HDFC Bank|500180",
        help="Comma-separated names, optionally pinned as name|BSE-scrip-code",
    )
    parser.add_argument("--years", type=float, default=2.0)
    parser.add_argument("--library-root")
    parser.add_argument("--output-root", default="poc-output")
    parser.add_argument("--skip-pull", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Write local outputs without Azure")
    parser.add_argument("--prepare-only", action="store_true", help="Pull/analyze locally without Azure")
    parser.add_argument("--upload-only", action="store_true", help="Upload an already prepared library/output")
    parser.add_argument("--preflight", action="store_true", help="Validate Azure and NVIDIA access, then exit")
    parser.add_argument(
        "--seed-from-azure",
        action="store_true",
        help="Hydrate existing Markdown and FilingForge dedup state from Azure before pulling",
    )
    parser.add_argument("--max-analysis-documents", type=int, default=10)
    parser.add_argument("--max-chars-per-document", type=int, default=12000)
    parser.add_argument("--max-windows-per-document", type=int, default=2)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.preflight:
        if not args.skip_analysis:
            NvidiaClient().preflight()
        AzureStore().preflight()
        return 0
    if args.prepare_only and args.upload_only:
        raise SystemExit("--prepare-only and --upload-only cannot be combined")
    if args.upload_only and not args.library_root:
        raise SystemExit("--upload-only requires --library-root")
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
    store = AzureStore() if args.upload_only or (not args.dry_run and not args.prepare_only) else None
    nvidia = None if args.skip_analysis or args.upload_only else NvidiaClient()
    failures: list[str] = []

    try:
        if args.seed_from_azure and not args.upload_only:
            seed_store = AzureStore()
            for company in companies:
                try:
                    from engine import BSEClient

                    client = BSEClient()
                    try:
                        _company_name, _scrip_code, company_key = resolve_company_spec(company, client)
                    finally:
                        client.close()
                    seed_store.hydrate_company_library(company_key, library_root / company_key)
                except Exception:
                    failures.append(company)
                    log.exception("Azure hydration failed for %s", company)

        if not args.skip_pull and not args.upload_only:
            for company in companies:
                if company in failures:
                    continue
                try:
                    run_filingforge(company, library_root, args.years)
                except Exception as exc:
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
                if args.upload_only:
                    upload_prepared_company(company_key, records, paths, output_root, store)
                else:
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
