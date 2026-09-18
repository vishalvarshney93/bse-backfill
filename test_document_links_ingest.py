import unittest

import document_links_ingest as ingest


class DocumentLinkIngestTests(unittest.TestCase):
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
        state = [{"RowKey": "ISSUER-500001", "Status": "enabled", "SourceSetHash": current_hash, "UpdatedAt": "2026-09-18T00:00:00+00:00"}]
        self.assertEqual(ingest.select_companies({"ISSUER-500001": documents}, state, 1), [])
        changed = [{"id": "a", "pdf_url": "https://issuer.example/new-a.pdf"}]
        self.assertEqual(ingest.select_companies({"ISSUER-500001": changed}, state, 1), [])

    def test_retries_company_processing_failure_without_delay(self):
        documents = [{"id": "a", "pdf_url": "https://issuer.example/a.pdf"}]
        state = [{"RowKey": "ISSUER-500001", "Status": "error", "UpdatedAt": "2026-09-18T00:00:00+00:00"}]
        self.assertEqual(
            ingest.select_companies({"ISSUER-500001": documents}, state, 1),
            ["ISSUER-500001"],
        )

    def test_omits_previously_unavailable_document_links(self):
        documents = [{"id": "good"}, {"id": "broken"}]
        state = [{"RowKey": "DOCUMENT|broken", "Status": "unavailable"}]
        self.assertEqual(ingest.available_documents(documents, state), [{"id": "good"}])

    def test_keeps_malformed_stored_link_for_failure_audit(self):
        row = {"id": "bad", "scrip_code": "500001", "company_name": "Issuer", "doc_type": "ANNUAL_REPORT", "pdf_url": "not-a-url"}
        self.assertEqual(ingest.group_documents([row]), {"ISSUER-500001": [row]})

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
        self.assertIn("source_pdf: https://issuer.example/q1.pdf", markdown)
        self.assertIn("Quarterly revenue increased", markdown)


if __name__ == "__main__":
    unittest.main()