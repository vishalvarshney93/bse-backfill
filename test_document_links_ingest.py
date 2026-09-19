from io import BytesIO
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from PIL import Image

import document_links_ingest as ingest


class DocumentLinkIngestTests(unittest.TestCase):
    def test_markdown_table_preserves_financial_layout(self):
        rendered = ingest.markdown_table([
            ["Particulars", "FY24", "FY25"],
            ["Revenue", "680.99", "804.09"],
            ["EBITDA margin", "12.75%", "12.11%"],
        ], 1)
        self.assertIn("| Particulars | FY24 | FY25 |", rendered)
        self.assertIn("| Revenue | 680.99 | 804.09 |", rendered)
        self.assertIn("| EBITDA margin | 12.75% | 12.11% |", rendered)

    def test_reclaims_expired_company_lease_after_cancelled_run(self):
        now = datetime(2026, 9, 18, 14, 0, tzinfo=timezone.utc)

        class FakeState:
            def __init__(self):
                self.entity = {
                    "PartitionKey": ingest.LEASE_PARTITION,
                    "RowKey": "ISSUER-500001",
                    "ExpiresAt": (now - timedelta(minutes=1)).isoformat(),
                }

            def create_entity(self, entity):
                if self.entity is not None:
                    from azure.core.exceptions import ResourceExistsError
                    raise ResourceExistsError("exists")
                self.entity = entity

            def get_entity(self, **_kwargs):
                return self.entity

            def delete_entity(self, **_kwargs):
                self.entity = None

        store = type("Store", (), {"state": FakeState()})()
        self.assertTrue(ingest.claim_company_lease(store, "ISSUER-500001", now))
        self.assertGreater(datetime.fromisoformat(store.state.entity["ExpiresAt"]), now)

    def test_accepts_public_https_company_url(self):
        self.assertEqual(
            ingest.public_document_url("https://investor.example.com/results/q1.pdf"),
            "https://investor.example.com/results/q1.pdf",
        )

    def test_rejects_non_https_or_localhost_url(self):
        for url in ("http://example.com/report.pdf", "https://localhost/report.pdf", "file:///tmp/report.pdf"):
            with self.assertRaises(ValueError):
                ingest.public_document_url(url)

    def test_queues_each_company_once_for_enablement(self):
        documents = [{"id": "a", "pdf_url": "https://issuer.example/a.pdf"}]
        current_hash = ingest.source_set_hash(documents)
        self.assertEqual(ingest.select_companies({"ISSUER-500001": documents}, [], 1), ["ISSUER-500001"])
        state = [{"RowKey": "ISSUER-500001", "Status": "enabled", "CorpusVersion": ingest.CORPUS_VERSION, "SourceSetHash": current_hash, "UpdatedAt": "2026-09-18T00:00:00+00:00"}]
        self.assertEqual(ingest.select_companies({"ISSUER-500001": documents}, state, 1), [])
        changed = [{"id": "a", "pdf_url": "https://issuer.example/new-a.pdf"}]
        self.assertEqual(ingest.select_companies({"ISSUER-500001": changed}, state, 1), [])

    def test_requeues_enabled_company_when_corpus_version_is_stale(self):
        documents = [{"id": "a", "pdf_url": "https://issuer.example/a.pdf"}]
        state = [{"RowKey": "ISSUER-500001", "Status": "enabled", "CorpusVersion": ingest.CORPUS_VERSION - 1}]
        self.assertEqual(ingest.select_companies({"ISSUER-500001": documents}, state, 1), ["ISSUER-500001"])

    def test_retries_company_processing_failure_without_delay(self):
        documents = [{"id": "a", "pdf_url": "https://issuer.example/a.pdf"}]
        state = [{"RowKey": "ISSUER-500001", "Status": "error", "UpdatedAt": "2026-09-18T00:00:00+00:00"}]
        self.assertEqual(
            ingest.select_companies({"ISSUER-500001": documents}, state, 1),
            ["ISSUER-500001"],
        )

    def test_company_selection_shards_do_not_overlap(self):
        grouped = {
            f"ISSUER-{scrip_code}": [{"id": scrip_code}]
            for scrip_code in ("500001", "500002", "500003", "500004")
        }
        first = ingest.select_companies(grouped, [], 2, shard_index=0, shard_count=2)
        second = ingest.select_companies(grouped, [], 2, shard_index=1, shard_count=2)
        self.assertEqual(first, ["ISSUER-500001", "ISSUER-500003"])
        self.assertEqual(second, ["ISSUER-500002", "ISSUER-500004"])
        self.assertFalse(set(first) & set(second))

    def test_omits_previously_unavailable_document_links(self):
        documents = [{"id": "good"}, {"id": "broken"}]
        state = [
            {"RowKey": "DOCUMENT|broken", "Status": "unavailable"},
            {"RowKey": "DOCUMENT|good", "Status": "extraction_error"},
        ]
        self.assertEqual(ingest.available_documents(documents, state), [{"id": "good"}])

    def test_extraction_failure_is_not_blacklisted_as_unavailable(self):
        document = {
            "id": "scan", "scrip_code": "500001", "company_name": "Issuer",
            "doc_type": "ANNUAL_REPORT", "doc_period_end_date": "2026-03-31",
            "pdf_url": "https://issuer.example/scan.pdf",
        }
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(
            ingest, "download_markdown", side_effect=ingest.DocumentExtractionError("OCR failed"),
        ):
            failures = ingest.prepare_company("ISSUER-500001", [document], Path(temp))
        self.assertEqual(failures[0].status, "extraction_error")
        self.assertEqual(
            ingest.available_documents([document], [{"RowKey": "DOCUMENT|scan", "Status": "extraction_error"}]),
            [document],
        )

    def test_keeps_malformed_stored_link_for_failure_audit(self):
        row = {"id": "bad", "scrip_code": "500001", "company_name": "Issuer", "doc_type": "ANNUAL_REPORT", "pdf_url": "not-a-url"}
        self.assertEqual(ingest.group_documents([row]), {"ISSUER-500001": [row]})

    def test_canonicalization_updates_only_screener_urls(self):
        documents = [
            {"id": "redirect", "pdf_url": "https://www.screener.in/company/source/report/"},
            {"id": "direct", "pdf_url": "https://www.bseindia.com/report.pdf"},
        ]
        with (
            mock.patch.object(ingest, "resolve_stored_document_url", return_value="https://www.bseindia.com/resolved.pdf"),
            mock.patch.object(ingest, "persist_resolved_document_url", return_value=True) as persist,
        ):
            result = ingest.canonicalize_stored_document_urls(documents, limit=10, delay_seconds=1)
        self.assertEqual(result, (1, 0, 0))
        persist.assert_called_once_with(documents[0], "https://www.bseindia.com/resolved.pdf")

    def test_markdown_conversion_keeps_provenance(self):
        document = {"id": "doc-1", "scrip_code": "500001", "company_name": "Issuer Ltd", "doc_type": "QUARTERLY_RESULT", "doc_period_end_date": "2026-06-30", "pdf_url": "https://issuer.example/q1.pdf"}
        pdf = ingest.fitz.open()
        page = pdf.new_page()
        source_text = " ".join(["Quarterly revenue increased materially and management expects demand to remain strong."] * 4)
        page.insert_textbox((72, 72, 500, 500), source_text, fontsize=10)
        data = pdf.tobytes()
        pdf.close()
        markdown = ingest.markdown_from_pdf(document, data)
        self.assertIn("news_id: direct-doc-1", markdown)
        self.assertIn("title: Issuer Ltd Quarterly Result (2026-06-30)", markdown)
        self.assertIn("extracted: ok", markdown)
        self.assertIn("source_pdf: https://issuer.example/q1.pdf", markdown)
        self.assertIn("Quarterly revenue increased", markdown)

    def test_scanned_pdf_uses_ocr_and_resolved_source_url(self):
        document = {"id": "scan", "scrip_code": "500001", "company_name": "Issuer Ltd", "doc_type": "ANNUAL_REPORT", "doc_period_end_date": "2026-03-31", "pdf_url": "https://www.screener.in/company/source/report/"}
        image_buffer = BytesIO()
        Image.new("RGB", (400, 300), "white").save(image_buffer, format="PNG")
        pdf = ingest.fitz.open()
        page = pdf.new_page()
        page.insert_image(page.rect, stream=image_buffer.getvalue())
        data = pdf.tobytes()
        pdf.close()
        ocr_text = " ".join(["Revenue and operating margin improved during the reported period."] * 5)
        resolved_url = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/report.pdf"
        with mock.patch("pytesseract.image_to_string", return_value=ocr_text):
            markdown = ingest.markdown_from_pdf(document, data, resolved_url)
        self.assertIn(f"source_pdf: {resolved_url}", markdown)
        self.assertIn("Revenue and operating margin improved", markdown)

    def test_deterministic_evidence_cache_bypasses_extraction(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            company = root / "library" / "ISSUER-500001"
            filing = company / "quarterly" / "2026" / "2026-06-30__direct-doc.md"
            filing.parent.mkdir(parents=True)
            filing.write_text(
                "---\nnews_id: direct-doc\nsource_pdf: https://issuer.example/report.pdf\n"
                "extracted: ok\n---\n\n# Results\n\nRevenue and margins improved during the quarter.",
                encoding="utf-8",
            )
            records, paths = ingest.discover_documents(root / "library")
            seeded = ingest.seed_deterministic_evidence_caches(
                "ISSUER-500001", records, paths, root / "output",
            )
            cache_path = root / "output" / "ISSUER-500001" / "claims" / ingest.claim_cache_name(records[0])
            cached = ingest.load_cached_claims(cache_path, records[0])
            self.assertEqual(seeded, 1)
            self.assertTrue(cached)
            self.assertEqual(cached[0]["citation"]["document_id"], records[0].document_id)

    def test_deterministic_evidence_covers_deep_document_facts(self):
        record = type("Record", (), {
            "document_id": "ff-" + "a" * 24,
            "content_sha256": "b" * 64,
            "source_pdf": "https://issuer.example/annual-report.pdf",
            "filing_date": "2025-03-31",
            "title": "FY25 Annual Report",
        })()
        markdown = "# Annual Report\n\n" + "\n\n".join(
            f"Section {index}: operations and financial discussion for business area {index}."
            for index in range(2_000)
        )
        insertion = 54_321
        markdown = markdown[:insertion] + " Established in 1987, the company manufactures minerals. " + markdown[insertion:]
        evidence = ingest.build_deterministic_evidence(record, markdown)
        self.assertGreater(len(evidence), 8)
        self.assertTrue(any("Established in 1987" in item["statement"] for item in evidence))
        self.assertEqual(len({item["evidence_id"] for item in evidence}), len(evidence))
        self.assertTrue(all(item["citation"]["evidence_id"] == item["evidence_id"] for item in evidence))


if __name__ == "__main__":
    unittest.main()