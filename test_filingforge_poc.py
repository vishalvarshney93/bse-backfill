import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import requests

from filingforge_poc import (
    AzureStore,
    NvidiaClient,
    NvidiaRequestError,
    build_document_record,
    build_manifest,
    document_windows,
    extract_supported_claims,
    parse_json_object,
    parse_company_spec,
    parse_args,
    process_company,
    resolve_company_spec,
    select_research_documents,
    synthesize_company_research,
    upload_prepared_company,
    validate_nvidia_model_id,
    validate_snapshot,
)


class FakeNvidiaClient:
    def __init__(self):
        self.calls = 0
        self.requests = []
        self.model = "nvidia/test-synthesis"
        self.extraction_model = "nvidia/test-extraction"
        self.extraction_timeout = 1
        self.synthesis_timeout = 1

    def json_completion(self, system, user, max_tokens=6000, model=None, timeout_seconds=None, **kwargs):
        self.calls += 1
        self.requests.append(
            {
                "max_tokens": max_tokens,
                "model": model,
                "timeout_seconds": timeout_seconds,
                **kwargs,
            }
        )
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


class TimingOutNvidiaClient:
    model = "nvidia/test-synthesis"
    extraction_model = "nvidia/test-extraction"
    extraction_timeout = 1
    synthesis_timeout = 1

    def json_completion(self, *args, **kwargs):
        raise requests.ReadTimeout("simulated NVIDIA timeout")


class FakeAzureStore:
    def __init__(self):
        self.documents = []
        self.manifests = []
        self.snapshots = []
        self.analysis_statuses = []
        self.states = []
        self.verifications = []

    def upload_document(self, record, path):
        self.documents.append((record.document_id, path.read_bytes()))

    def upload_manifest(self, company_key, manifest):
        self.manifests.append((company_key, manifest))

    def upload_snapshot(self, company_key, snapshot):
        self.snapshots.append((company_key, snapshot))

    def upload_analysis_status(self, company_key, status):
        self.analysis_statuses.append((company_key, status))

    def record_state(self, company_key, status, document_count, detail=""):
        self.states.append((company_key, status, document_count, detail))
        return True

    def verify_company_upload(self, company_key, records, has_analysis, has_analysis_status):
        self.verifications.append(
            (company_key, len(records), has_analysis, has_analysis_status)
        )


class FailingStateAzureStore(FakeAzureStore):
    def record_state(self, company_key, status, document_count, detail=""):
        return False


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
    def test_cli_accepts_six_month_window(self):
        with mock.patch.object(sys, "argv", ["filingforge_poc.py", "--years", "0.5"]):
            self.assertEqual(parse_args().years, 0.5)

    def test_azure_preflight_checks_both_containers_and_table(self):
        store = object.__new__(AzureStore)
        store.markdown = FakeContainerClient()
        store.snapshots = FakeContainerClient()
        store.state = FakeReadableTableClient()
        store.preflight()
        self.assertTrue(store.markdown.checked)
        self.assertTrue(store.snapshots.checked)
        self.assertEqual(store.state.query_filter, "PartitionKey eq 'FILINGFORGE_POC'")

    def test_azure_upload_verifier_reads_manifest_markdown_snapshot_and_table(self):
        with tempfile.TemporaryDirectory() as temp:
            company = Path(temp) / "SHILPA-530549"
            filing = company / "quarterly" / "2026-06-30_Results.md"
            filing.parent.mkdir(parents=True)
            filing.write_text(
                "---\nnews_id: result-123\nextracted: ok\n---\n\nResults text.",
                encoding="utf-8",
            )
            record = build_document_record(company, filing)
            manifest = json.dumps(build_manifest(record.company_key, [record])).encode("utf-8")

            manifest_blob = mock.Mock()
            manifest_blob.download_blob.return_value.readall.return_value = manifest
            markdown_blob = mock.Mock()
            markdown_blob.download_blob.return_value.readall.return_value = filing.read_bytes()
            markdown_container = mock.Mock()
            markdown_container.get_blob_client.side_effect = lambda name: (
                manifest_blob if name.endswith("manifest.json") else markdown_blob
            )
            markdown_container.list_blobs.return_value = []
            snapshot_container = mock.Mock()
            snapshot_container.list_blobs.return_value = []
            state = mock.Mock()
            state.get_entity.return_value = {
                "Status": "complete",
                "DocumentCount": 1,
                "Detail": "analysis=True",
            }
            store = object.__new__(AzureStore)
            store.markdown = markdown_container
            store.snapshots = snapshot_container
            store.state = state

            store.verify_company_upload("SHILPA-530549", [record], True, True)

            snapshot_names = [call.args[0] for call in snapshot_container.get_blob_client.call_args_list]
            self.assertEqual(
                snapshot_names,
                [
                    "companies/SHILPA-530549/latest.json",
                    "companies/SHILPA-530549/analysis-status.json",
                ],
            )
            state.get_entity.assert_called_once_with(
                partition_key="FILINGFORGE_POC",
                row_key="SHILPA-530549",
            )

    def test_azure_hydration_restores_markdown_and_filingforge_dedup_ledger(self):
        with tempfile.TemporaryDirectory() as temp:
            company = Path(temp) / "SHILPA-530549"
            markdown = b"---\nnews_id: filing-123\nextracted: ok\n---\n\nVerified filing."
            digest = hashlib.sha256(markdown).hexdigest()
            manifest = {
                "document_count": 1,
                "documents": [
                    {
                        "source_news_id": "filing-123",
                        "relative_path": "quarterly/2026/2026-06-30_Results.md",
                        "blob_name": f"companies/SHILPA-530549/documents/quarterly/2026/doc/{digest}.md",
                        "content_sha256": digest,
                    }
                ],
            }
            manifest_blob = mock.Mock()
            manifest_blob.download_blob.return_value.readall.return_value = json.dumps(manifest).encode("utf-8")
            markdown_blob = mock.Mock()
            markdown_blob.download_blob.return_value.readall.return_value = markdown
            store = object.__new__(AzureStore)
            store.markdown = mock.Mock()
            store.markdown.get_blob_client.side_effect = [manifest_blob, markdown_blob]

            hydrated = store.hydrate_company_library("SHILPA-530549", company)

            self.assertEqual(hydrated, 1)
            self.assertEqual(
                (company / "quarterly" / "2026" / "2026-06-30_Results.md").read_bytes(),
                markdown,
            )
            self.assertEqual(
                json.loads((company / ".filingforge_index.json").read_text(encoding="utf-8")),
                ["filing-123"],
            )

    def test_nvidia_preflight_sends_completion_to_each_configured_model(self):
        catalog_response = mock.Mock()
        catalog_response.json.return_value = {
            "data": [
                {"id": "nvidia/test-extraction"},
                {"id": "nvidia/test-synthesis"},
            ]
        }
        client = object.__new__(NvidiaClient)
        client.api_key = "test-key"
        client.base_url = "https://example.invalid/v1"
        client.extraction_model = "nvidia/test-extraction"
        client.model = "nvidia/test-synthesis"
        client.extraction_timeout = 60
        client.synthesis_timeout = 180
        client.session = mock.Mock()
        client.session.get.return_value = catalog_response
        client.json_completion = mock.Mock(return_value={"ok": True})

        client.preflight()

        self.assertEqual(client.json_completion.call_count, 2)
        self.assertEqual(
            [call.kwargs["model"] for call in client.json_completion.call_args_list],
            ["nvidia/test-extraction", "nvidia/test-synthesis"],
        )
        self.assertTrue(all(call.kwargs["max_tokens"] == 32 for call in client.json_completion.call_args_list))
        self.assertTrue(all(call.kwargs["timeout_seconds"] == 30 for call in client.json_completion.call_args_list))

    def test_blank_extraction_model_reuses_synthesis_model(self):
        with mock.patch.dict(
            "os.environ",
            {
                "NVIDIA_NIM_API_KEY": "test-key",
                "NVIDIA_NIM_MODEL": "nvidia/test-synthesis",
                "NVIDIA_NIM_EXTRACTION_MODEL": "",
            },
            clear=True,
        ):
            client = NvidiaClient()
        self.assertEqual(client.extraction_model, "nvidia/test-synthesis")

    def test_permanent_nvidia_404_names_model_without_retrying(self):
        response = mock.Mock(status_code=404)
        response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        client = object.__new__(NvidiaClient)
        client.api_key = "test-key"
        client.base_url = "https://example.invalid/v1"
        client.model = "nvidia/test-model"
        client.synthesis_timeout = 180
        client.session = mock.Mock()
        client.session.post.return_value = response

        with self.assertRaisesRegex(NvidiaRequestError, "nvidia/test-model.*HTTP 404"):
            client.json_completion("Return JSON only.", "Return an object.")

        client.session.post.assert_called_once()

    def test_nvidia_streaming_payload_matches_reasoning_profile(self):
        response = mock.Mock(status_code=200)
        response.iter_lines.return_value = [
            'data: {"choices":[{"delta":{"reasoning_content":"checking"},"finish_reason":null}]}',
            'data: {"choices":[{"delta":{"content":"{\\"ok\\":"},"finish_reason":null}]}',
            'data: {"choices":[{"delta":{"content":"true}"},"finish_reason":"stop"}]}',
            "data: [DONE]",
        ]
        client = object.__new__(NvidiaClient)
        client.api_key = "test-key"
        client.base_url = "https://example.invalid/v1"
        client.model = "nvidia/nemotron-3.5-lightning-30b-a3b"
        client.synthesis_timeout = 180
        client.session = mock.Mock()
        client.session.post.return_value = response

        result = client.json_completion(
            "Return JSON only.",
            "Return an object.",
            max_tokens=32768,
            temperature=0.2,
            top_p=0.95,
            enable_thinking=True,
            reasoning_budget=8192,
            stream=True,
        )

        self.assertEqual(result, {"ok": True})
        request = client.session.post.call_args
        self.assertTrue(request.kwargs["stream"])
        self.assertEqual(
            request.kwargs["json"],
            {
                "model": "nvidia/nemotron-3.5-lightning-30b-a3b",
                "messages": [
                    {"role": "system", "content": "Return JSON only."},
                    {"role": "user", "content": "Return an object."},
                ],
                "temperature": 0.2,
                "top_p": 0.95,
                "max_tokens": 32768,
                "chat_template_kwargs": {"enable_thinking": True},
                "stream": True,
                "reasoning_budget": 8192,
            },
        )

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

    def test_nvidia_timeouts_preserve_manifest_and_mark_analysis_unavailable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            company = root / "library" / "MARUTI-532500"
            filing = company / "concalls" / "2026-06-30_Earnings_Call.md"
            filing.parent.mkdir(parents=True)
            filing.write_text(
                "---\nnews_id: call-123\nsource_pdf: source.pdf\nextracted: ok\n---\n\n"
                + ("Management expects capacity to double during FY27. " * 600),
                encoding="utf-8",
            )
            record = build_document_record(company, filing)
            output_root = root / "output"

            process_company(
                record.company_key,
                [record],
                {record.document_id: filing},
                output_root,
                None,
                TimingOutNvidiaClient(),
                10,
                12_000,
                2,
            )

            company_output = output_root / record.company_key
            self.assertTrue((company_output / "manifest.json").exists())
            self.assertTrue((company_output / "analysis-status.json").exists())
            self.assertFalse((company_output / "research.json").exists())
            status = json.loads((company_output / "analysis-status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["failed_windows"], 2)

    def test_company_specs_can_pin_ambiguous_names_to_bse_scrip_code(self):
        self.assertEqual(parse_company_spec("Maruti Suzuki India|532500"), ("Maruti Suzuki India", "532500"))
        self.assertEqual(parse_company_spec("SHILPAMED"), ("SHILPAMED", None))

    def test_pinned_larsen_code_survives_name_resolver_failure(self):
        def failing_resolver(_query, _client):
            raise RuntimeError("no BSE match for 'Larsen & Toubro'")

        self.assertEqual(
            resolve_company_spec("Larsen & Toubro|500510", object(), resolver=failing_resolver),
            ("Larsen & Toubro", "500510", "LARSEN-500510"),
        )

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
            self.assertEqual(client.requests[0]["max_tokens"], 4096)
            self.assertEqual(claims[0]["citation"]["document_id"], record.document_id)
            self.assertEqual(claims[0]["citation"]["content_sha256"], record.content_sha256)

            original_id = record.document_id
            client_result = client.json_completion

            def synthesis_with_real_id(system, user, max_tokens=6000, **kwargs):
                result = client_result(system, user, max_tokens, **kwargs)
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
            self.assertEqual(
                client.requests[1],
                {
                    "max_tokens": 32768,
                    "model": "nvidia/test-synthesis",
                    "timeout_seconds": 1,
                    "temperature": 0.2,
                    "top_p": 0.95,
                    "enable_thinking": True,
                    "reasoning_budget": 8192,
                    "stream": True,
                },
            )
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
            (output / "analysis-status.json").write_text(
                json.dumps({"status": "unavailable", "details": ["simulated timeout"]}),
                encoding="utf-8",
            )

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
            self.assertEqual(store.analysis_statuses[0][1]["status"], "unavailable")
            self.assertEqual(
                store.verifications,
                [("MARUTI-532500", 1, False, True)],
            )

    def test_upload_only_fails_when_table_state_cannot_be_persisted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            company = root / "library" / "MARUTI-532500"
            filing = company / "quarterly" / "2026-06-30_Results.md"
            filing.parent.mkdir(parents=True)
            filing.write_text(
                "---\nnews_id: result-123\nextracted: ok\n---\n\nResults text.",
                encoding="utf-8",
            )
            record = build_document_record(company, filing)
            output = root / "output" / record.company_key
            output.mkdir(parents=True)
            (output / "manifest.json").write_text(
                json.dumps(build_manifest(record.company_key, [record])),
                encoding="utf-8",
            )
            store = FailingStateAzureStore()

            with self.assertRaisesRegex(RuntimeError, "Could not persist Azure Table"):
                upload_prepared_company(
                    record.company_key,
                    [record],
                    {record.document_id: filing},
                    root / "output",
                    store,
                )

            self.assertEqual(store.verifications, [])


if __name__ == "__main__":
    unittest.main()

