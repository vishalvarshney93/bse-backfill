import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from filingforge_poc import (
    AzureStore,
    build_document_record,
    build_manifest,
    document_windows,
    extract_supported_claims,
    parse_json_object,
    parse_company_spec,
    select_research_documents,
    synthesize_company_research,
    upload_prepared_company,
    validate_nvidia_model_id,
    validate_snapshot,
)


class FakeNvidiaClient:
    def __init__(self):
        self.calls = 0

    def json_completion(self, system, user, max_tokens=6000):
        self.calls += 1
        if self.calls == 1:
            return {
                "claims": [
                    {
                        "claim_type": "guidance",
                        "statement": "Management expects Plant B commissioning in FY27.",
                        "metric": "Plant B commissioning",
                        "target": "commissioned",
                        "target_period": "FY27",
                        "quote": "Plant B is expected to be commissioned during FY27.",
                        "heading": "Expansion plan",
                    },
                    {
                        "claim_type": "positive",
                        "statement": "Unsupported invented claim.",
                        "quote": "This quote is absent from the filing.",
                    },
                ]
            }
        return {
            "overview": {
                "sections": [
                    {
                        "heading": "Business model",
                        "text": "Specialty manufacturer.",
                        "document_ids": ["DOCUMENT_ID"],
                    }
                ]
            },
            "positives": [],
            "risks": [],
            "management_guidance": [
                {
                    "guidance_id": "G1",
                    "statement": "Plant B commissioning",
                    "metric": "Plant B commissioning",
                    "target": "commissioned",
                    "target_period": "FY27",
                    "document_ids": ["DOCUMENT_ID"],
                }
            ],
            "key_deliverables": [
                {
                    "deliverable": "Commission Plant B",
                    "metric_or_milestone": "commissioning",
                    "due_period": "FY27",
                    "status": "pending",
                    "document_ids": ["DOCUMENT_ID"],
                }
            ],
            "walk_the_talk": [
                {
                    "guidance_id": "G1",
                    "status": "pending",
                    "assessment": "Target period has not concluded.",
                    "guidance_document_ids": ["DOCUMENT_ID"],
                    "outcome_document_ids": [],
                }
            ],
        }


class FakeAzureStore:
    def __init__(self):
        self.documents = []
        self.manifests = []
        self.snapshots = []
        self.states = []

    def upload_document(self, record, path):
        self.documents.append((record.document_id, path.read_bytes()))

    def upload_manifest(self, company_key, manifest):
        self.manifests.append((company_key, manifest))

    def upload_snapshot(self, company_key, snapshot):
        self.snapshots.append((company_key, snapshot))

    def record_state(self, company_key, status, document_count, detail=""):
        self.states.append((company_key, status, document_count, detail))


class FailingTableClient:
    def upsert_entity(self, *args, **kwargs):
        raise RuntimeError("simulated expired credential")


class FakeContainerClient:
    def __init__(self):
        self.checked = False

    def get_container_properties(self):
        self.checked = True


class FakeReadableTableClient:
    def __init__(self):
        self.query_filter = None

    def query_entities(self, query_filter, **kwargs):
        self.query_filter = query_filter
        return iter([])


class FilingForgePocTests(unittest.TestCase):
    def test_azure_preflight_checks_both_containers_and_table(self):
        store = object.__new__(AzureStore)
        store.markdown = FakeContainerClient()
        store.snapshots = FakeContainerClient()
        store.state = FakeReadableTableClient()
        store.preflight()
        self.assertTrue(store.markdown.checked)
        self.assertTrue(store.snapshots.checked)
        self.assertEqual(store.state.query_filter, "PartitionKey eq 'FILINGFORGE_POC'")

    def test_nvidia_model_id_requires_publisher_prefix(self):
        self.assertEqual(
            validate_nvidia_model_id("nvidia/nemotron-3.5-lightning-30b-a3b"),
            "nvidia/nemotron-3.5-lightning-30b-a3b",
        )
        with self.assertRaisesRegex(ValueError, "nvidia/nemotron-3.5-lightning-30b-a3b"):
            validate_nvidia_model_id("nvidianemotron-3.5-lightning-30b-a3b")

    def test_state_write_failure_does_not_raise(self):
        store = object.__new__(AzureStore)
        store.state = FailingTableClient()
        self.assertFalse(store.record_state("MARUTI-532500", "complete", 1))

    def test_company_specs_can_pin_ambiguous_names_to_bse_scrip_code(self):
        self.assertEqual(parse_company_spec("Maruti Suzuki India|532500"), ("Maruti Suzuki India", "532500"))
        self.assertEqual(parse_company_spec("SHILPAMED"), ("SHILPAMED", None))

    def test_manifest_and_cited_research_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            company = Path(temp) / "SHILPA-530549"
            filing = company / "concalls" / "2026-06-30_Q1_FY27_Earnings_Call.md"
            filing.parent.mkdir(parents=True)
            filing.write_text(
                "---\nnews_id: filing-123\nsource_pdf: source.pdf\nextracted: ok\n---\n\n"
                "# Earnings Call\n\n## Expansion plan\n\n"
                "Plant B is expected to be commissioned during FY27.\n",
                encoding="utf-8",
            )

            record = build_document_record(company, filing)
            self.assertEqual(record.company_key, "SHILPA-530549")
            self.assertEqual(record.category, "concalls")
            self.assertEqual(record.filing_date, "2026-06-30")
            self.assertEqual(record.source_news_id, "filing-123")
            self.assertEqual(record.extraction_status, "ok")
            self.assertIn("/packs/2026/concalls.jsonl.zst", record.pack_key)
            self.assertIn(record.content_sha256, record.blob_name)
            self.assertEqual(select_research_documents([record], 10), [record])

            manifest = build_manifest(record.company_key, [record])
            self.assertEqual(manifest["document_count"], 1)
            self.assertEqual(manifest["documents"][0]["document_id"], record.document_id)

            client = FakeNvidiaClient()
            claims = extract_supported_claims(client, record, filing.read_text(encoding="utf-8"))
            self.assertEqual(len(claims), 1)
            self.assertEqual(claims[0]["citation"]["document_id"], record.document_id)
            self.assertEqual(claims[0]["citation"]["content_sha256"], record.content_sha256)

            original_id = record.document_id
            client_result = client.json_completion

            def synthesis_with_real_id(system, user, max_tokens=6000):
                result = client_result(system, user, max_tokens)
                for collection in ("management_guidance", "key_deliverables"):
                    for item in result.get(collection, []):
                        item["document_ids"] = [original_id]
                for item in result.get("overview", {}).get("sections", []):
                    item["document_ids"] = [original_id]
                for item in result.get("walk_the_talk", []):
                    item["guidance_document_ids"] = [original_id]
                return result

            client.json_completion = synthesis_with_real_id
            research = synthesize_company_research(client, record.company_key, claims)
            self.assertEqual(research["overview"]["sections"][0]["heading"], "Business model")
            self.assertEqual(research["management_guidance"][0]["guidance_id"], "G1")
            self.assertEqual(research["walk_the_talk"][0]["status"], "pending")
            validate_snapshot({
                "schema_version": 1,
                "company_key": record.company_key,
                "generated_at": "2026-09-01T12:00:00+00:00",
                "source_document_count": 1,
                "validated_evidence_count": len(claims),
                "research": research,
                "evidence": claims,
            })

    def test_json_parser_ignores_surrounding_text_and_rejects_truncation(self):
        self.assertEqual(parse_json_object("analysis\n{\"ok\": true}\ntrailing"), {"ok": True})
        with self.assertRaises(RuntimeError):
            parse_json_object("analysis\n{\"ok\":")

    def test_long_documents_are_sampled_across_their_full_length(self):
        markdown = "A" * 100 + "B" * 100 + "C" * 100
        windows = document_windows(markdown, max_chars=100, max_windows=3)
        self.assertEqual([offset for offset, _ in windows], [0, 100, 200])
        self.assertEqual([text[0] for _, text in windows], ["A", "B", "C"])

    def test_cli_dry_run_writes_manifest_and_removes_pdf(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            library = root / "library"
            filing = library / "MARUTI-532500" / "annual-reports" / "2026-03-31_Annual_Report.md"
            filing.parent.mkdir(parents=True)
            filing.write_text(
                "---\nnews_id: annual-123\nsource_pdf: source.pdf\nextracted: ok\n---\n\n"
                "# Annual Report\n\nOfficial filing text.\n",
                encoding="utf-8",
            )
            pdf = filing.with_suffix(".pdf")
            pdf.write_bytes(b"temporary-pdf")
            output = root / "output"

            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("filingforge_poc.py")),
                    "--library-root", str(library),
                    "--output-root", str(output),
                    "--skip-pull",
                    "--skip-analysis",
                    "--dry-run",
                ],
                check=True,
            )

            manifest = json.loads((output / "MARUTI-532500" / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["document_count"], 1)
            self.assertFalse(pdf.exists())

    def test_upload_only_uses_prepared_outputs_without_reanalysis(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            company = root / "library" / "MARUTI-532500"
            filing = company / "quarterly" / "2026-06-30_Results.md"
            filing.parent.mkdir(parents=True)
            filing.write_text(
                "---\nnews_id: result-123\nsource_pdf: source.pdf\nextracted: ok\n---\n\nResults text.",
                encoding="utf-8",
            )
            record = build_document_record(company, filing)
            output = root / "output" / record.company_key
            output.mkdir(parents=True)
            manifest = build_manifest(record.company_key, [record])
            (output / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            store = FakeAzureStore()
            upload_prepared_company(
                record.company_key,
                [record],
                {record.document_id: filing},
                root / "output",
                store,
            )

            self.assertEqual(len(store.documents), 1)
            self.assertEqual(store.manifests[0][0], "MARUTI-532500")
            self.assertEqual(store.states[0][1:3], ("complete", 1))
            self.assertEqual(store.snapshots, [])


if __name__ == "__main__":
    unittest.main()
