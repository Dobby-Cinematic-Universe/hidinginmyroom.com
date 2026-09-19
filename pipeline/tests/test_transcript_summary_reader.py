"""Citation-free reading copies preserve the private synthesis evidence chain."""
from __future__ import annotations

import io
import hashlib
import json
import os
from pathlib import Path
import stat
import unittest
from unittest.mock import patch

from pipeline import transcript_summary as runner
from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_reader as reader
from pipeline.tests import test_transcript_summary as existing
from pipeline.tests import test_transcript_summary_sonnet as sonnet


TEXTS = {
    "summary": ("A walk followed a library visit [1].", "reported_statement"),
    "topics": ("Reading and local travel; a note is marked [2020].", "reported_statement"),
    "events": ("An unnamed participant alleged a dispute; the account is unverified.", "reported_allegation"),
    "uncertainties": ('Who spoke is unclear.\nKeep “these words”, 日本語🙂 and https://example.invalid/[2].',
                      "uncertainty"),
}


def reader_payload(job):
    sections = {}
    for section, (text, classification) in TEXTS.items():
        evidence_id = next("e" + str(index) for index, item in enumerate(job["evidence"], 1)
                           if item["classification"] in (None, classification))
        sections[section] = [{"text": text, "classification": classification,
                              "evidence_ids": [evidence_id]}]
    return sections


class TranscriptReaderTests(unittest.TestCase):
    setUp = sonnet.SonnetRunnerTests.setUp
    file = sonnet.SonnetRunnerTests.file
    source = sonnet.SonnetRunnerTests.source
    plan = sonnet.SonnetRunnerTests.plan
    submit = sonnet.SonnetRunnerTests.submit
    finish = sonnet.SonnetRunnerTests.finish

    def prepare(self, phase):
        prepared = runner.prepare_plan(*self.args, phase=phase)
        self.assertIn(prepared["state"], ("prepared", "already_prepared"))
        folder = Path(self.request["state_root"]) / "waves" / prepared["wave_id"]
        wave = runner.read(runner.binding(folder / "wave.json"))
        service = (sonnet.AnthropicService(wave) if wave["provider"] == "anthropic"
                   else existing.Service(wave, self.creation["plan_id"]))
        return wave, service, folder

    def complete_transcripts(self):
        folders = []
        for _ in range(8):
            if runner.status_plan(*self.args, phase="transcripts")["transcript_phase_complete"]:
                return folders
            wave, service, folder = self.prepare("transcripts")
            for job, row in zip(wave["jobs"], service.rows):
                row["response"] = existing.response_body(wave["provider"], reader_payload(job))
            self.assertEqual(self.finish(wave, service)["completed"], len(wave["jobs"]))
            folders.append(folder)
        raise AssertionError("Transcript fixture did not finish")

    def finish_synthesis(self, wave, service):
        for job, row in zip(wave["jobs"], service.rows):
            row["result"]["message"]["content"][0]["text"] = json.dumps(reader_payload(job))
        self.assertEqual(self.finish(wave, service)["completed"], len(wave["jobs"]))

    def snapshot(self):
        root = Path(self.request["state_root"])
        return {str(path.relative_to(root)): (path.read_bytes(), stat.S_IMODE(path.stat().st_mode))
                for path in root.rglob("*") if path.is_file()
                and not path.is_relative_to(root / "reader-exports")}

    def test_reader_hides_evidence_metadata_and_preserves_exact_prose_and_classification(self):
        self.plan()
        self.complete_transcripts()
        exported = reader.export_reader(*self.args)
        document = runner.read(exported["artifact"])
        self.assertEqual(exported["state"], "exported_private_reader")
        self.assertEqual(exported["transcript_summaries"], 1)
        self.assertEqual(document["kind"], "himr_private_summary_reader_export")
        self.assertEqual(document["schema_version"], 1)
        self.assertEqual(document["phase"], "transcripts")
        self.assertTrue(document["phase_complete"])
        self.assertFalse(document["complete"])
        record = document["records"][0]
        self.assertEqual(record["recording_id"], "rec-1")
        self.assertEqual(record["title"], self.request["sources"][0]["title"])
        self.assertEqual(record["date"], {"value": "2026-08-20", "kind": "published"})
        self.assertEqual(record["sections"], {section: [{"text": text, "classification": classification}]
                                              for section, (text, classification) in TEXTS.items()})
        forbidden = {"item_id", "evidence_ids", "citations", "source_ref", "path", "transcript_sha256"}

        def check_keys(value):
            if isinstance(value, dict):
                self.assertTrue(forbidden.isdisjoint(value))
                for child in value.values():
                    check_keys(child)
            elif isinstance(value, list):
                for child in value:
                    check_keys(child)

        check_keys(document)
        self.assertNotIn(str(self.root), json.dumps(document))
        for key, value in runner.SEMANTICS.items():
            self.assertEqual(document["semantics"][key], value)
        self.assertFalse(document["semantics"]["publication_authority"])
        files = document["transcript_files"]
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["source_id"], record["source_id"])
        package = Path(exported["artifact"]["path"]).parent
        transcript = package / files[0]["href"]
        raw = transcript.read_bytes()
        self.assertEqual(raw, b"The speaker reports a walk.")
        self.assertEqual(files[0]["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(files[0]["href"], "transcripts/" + record["source_id"] + ".txt")

    def test_private_evidence_export_and_sonnet_continuation_are_unchanged(self):
        self.plan()
        self.complete_transcripts()
        canonical = runner.export_plan(*self.args, phase="transcripts")
        before = self.snapshot()
        plan, sources = runner.load_plan(*self.args)
        state = runner.load_state(plan, sources)
        expected = core.next_jobs(sources, state["jobs"], {item["job_id"]: item for item in state["results"]},
                                  self.request["config"], stages=runner.PHASE_STAGES["synthesis"])
        read_copy = reader.export_reader(*self.args)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(runner.export_plan(*self.args, phase="transcripts")["artifact"], canonical["artifact"])
        canonical_document = runner.read(canonical["artifact"])
        self.assertTrue(canonical_document["results"][0]["sections"]["summary"][0]["citations"])
        self.assertTrue(canonical_document["results"][0]["sections"]["summary"][0]["evidence_ids"])
        next_wave, service, _ = self.prepare("synthesis")
        self.assertEqual(next_wave["provider"], "anthropic")
        self.assertEqual(next_wave["jobs"], expected)
        for job in next_wave["jobs"]:
            self.assertTrue(job["prompt"]["input"]["source_excerpts"])
        self.finish_synthesis(next_wave, service)
        for _ in range(10):
            if runner.status_plan(*self.args)["state"] == "completed":
                break
            wave, service, _ = self.prepare("synthesis")
            self.finish_synthesis(wave, service)
        self.assertEqual(runner.status_plan(*self.args)["state"], "completed")
        finished = reader.export_reader(*self.args)
        self.assertTrue(finished["complete"])
        self.assertEqual(len(runner.read(finished["artifact"])["records"]), 1)
        self.assertNotEqual(read_copy["artifact"]["path"], finished["artifact"]["path"])
        self.assertFalse(runner.read(read_copy["artifact"])["complete"])

    def test_incomplete_export_does_not_promote_chunks_to_transcript_summaries(self):
        self.plan(approved=False)
        first = reader.export_reader(*self.args)
        self.assertFalse(first["phase_complete"])
        self.assertFalse(first["complete"])
        self.assertEqual(runner.read(first["artifact"])["records"], [])
        self.plan()
        chunk, service, _ = self.prepare("transcripts")
        self.assertEqual({job["stage"] for job in chunk["jobs"]}, {"chunk"})
        self.finish(chunk, service)
        second = reader.export_reader(*self.args)
        self.assertEqual(second["transcript_summaries"], 0)
        self.assertFalse(second["phase_complete"])
        self.assertEqual(runner.read(second["artifact"])["records"], [])

    def test_empty_sources_complete_without_manufacturing_reading_records(self):
        for selected in ([], [self.source("rec-1", text=None)], [self.source("rec-1", text=" \n\t")]):
            with self.subTest(sources=len(selected)):
                self.plan(selected, approved=False, broader=False)
                exported = reader.export_reader(*self.args)
                self.assertTrue(exported["phase_complete"])
                self.assertTrue(exported["complete"])
                self.assertEqual(exported["transcript_summaries"], 0)
                self.assertEqual(runner.read(exported["artifact"])["records"], [])

    def test_reader_exports_are_deterministic_private_and_never_overwritten(self):
        self.plan()
        self.complete_transcripts()
        exported = reader.export_reader(*self.args)
        self.assertEqual(reader.export_reader(*self.args), exported)
        path = Path(exported["artifact"]["path"])
        self.assertEqual(path.parent.parent, Path(self.request["state_root"]) / "reader-exports")
        self.assertEqual(path.name, "index.json")
        self.assertRegex(path.parent.name, r"^reader-[0-9a-f]{32}$")
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o400)
        self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
        path.chmod(0o600)
        path.write_bytes(b"tampered reader copy")
        with self.assertRaises(runner.Error):
            reader.export_reader(*self.args)
        self.assertEqual(path.read_bytes(), b"tampered reader copy")

    def test_synthesis_links_only_cited_transcripts_deduplicated_without_timestamps(self):
        self.plan([self.source("rec-1", text="First source [1].\nIts final sentence."),
                   self.source("rec-2", text="Uncited material should not be linked.")], broader=False)
        self.complete_transcripts()
        wave, service, _ = self.prepare("synthesis")
        self.assertEqual(len(wave["jobs"]), 1)
        job = wave["jobs"][0]
        self.assertEqual(job["stage"], "timeline")
        self.assertEqual(len(job["scope"]["source_ids"]), 2)
        chosen_source = job["evidence"][0]["citations"][0]["source_id"]
        references = ["e" + str(index) for index, item in enumerate(job["evidence"], 1)
                      if item["classification"] == "reported_statement"
                      and {c["source_id"] for c in item["citations"]} == {chosen_source}]
        self.assertGreaterEqual(len(references), 2)
        payload = {"summary": [{"text": "Two related points from one recording [1].",
                                "classification": "reported_statement", "evidence_ids": references[:2]}],
                   "topics": [], "events": [], "uncertainties": []}
        service.rows[0]["result"]["message"]["content"][0]["text"] = json.dumps(payload)
        self.assertEqual(self.finish(wave, service)["completed"], 1)
        exported = reader.export_reader(*self.args, phase="synthesis")
        document = runner.read(exported["artifact"])
        self.assertEqual(document["phase"], "synthesis")
        self.assertEqual(document["records"], [])
        self.assertEqual(len(document["synthesis"]), 1)
        item = document["synthesis"][0]["sections"]["summary"][0]
        self.assertEqual(item["text"], payload["summary"][0]["text"])
        self.assertEqual(set(item), {"text", "classification", "sources"})
        self.assertEqual(len(item["sources"]), 1)
        link = item["sources"][0]
        self.assertEqual(set(link), {"source_id", "recording_id", "title", "href"})
        self.assertEqual(link["source_id"], chosen_source)
        self.assertEqual(link["href"], "transcripts/" + chosen_source + ".txt")
        self.assertNotIn("#", link["href"])
        self.assertEqual([file["source_id"] for file in document["transcript_files"]], [chosen_source])
        _, sources = runner.load_plan(*self.args)
        source = next(source for source in sources if source["source_id"] == chosen_source)
        expected = "\n".join(segment["text"] for segment in source["segments"]).encode("utf-8")
        target = Path(exported["artifact"]["path"]).parent / link["href"]
        self.assertEqual(target.read_bytes(), expected)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o400)
        for key in ("start_ms", "end_ms", "char_start", "char_end", "timing_basis", "evidence_ids", "citations"):
            self.assertNotIn('"' + key + '"', json.dumps(document))
        combined = runner.read(reader.export_reader(*self.args, phase="all")["artifact"])
        self.assertEqual(len(combined["records"]), 2)
        self.assertEqual(len(combined["synthesis"]), 1)
        self.assertEqual(len(combined["transcript_files"]), 2)

    def test_bound_transcript_text_file_is_not_silently_replaced_after_tampering(self):
        self.plan()
        self.complete_transcripts()
        exported = reader.export_reader(*self.args)
        document = runner.read(exported["artifact"])
        path = Path(exported["artifact"]["path"]).parent / document["transcript_files"][0]["href"]
        path.chmod(0o600)
        path.write_bytes(b"tampered transcript reading copy")
        with self.assertRaises(runner.Error):
            reader.export_reader(*self.args)
        self.assertEqual(path.read_bytes(), b"tampered transcript reading copy")

    def test_cli_is_offline_without_api_client_or_env_file_loading(self):
        self.plan(approved=False)
        with patch.dict(os.environ, {}, clear=True), patch.object(
                runner, "api_client", side_effect=AssertionError("Reader must not create an API client")), patch.object(
                runner.env_module, "api_key", side_effect=AssertionError("Reader must not load credentials")), patch(
                "sys.stdout", new_callable=io.StringIO) as output:
            code = reader.main(["--manifest", self.args[0], "--expected-sha256", self.args[1]])
        self.assertEqual(code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["state"], "exported_private_reader")
        self.assertFalse(result["phase_complete"])

    def test_reader_module_is_outside_sealed_implementation_and_plan_still_replays(self):
        self.plan()
        plan = runner.read(self.ref)
        before = runner.implementation()
        self.assertNotIn("transcript_summary_reader.py", before)
        self.assertNotIn("transcript_summary_reader.py", runner.NAMES)
        self.assertNotIn("transcript-summary-reader", before)
        reader.export_reader(*self.args)
        self.assertEqual(runner.implementation(), before)
        self.assertEqual(plan["implementation"], before)
        self.assertEqual(runner.load_plan(*self.args)[0], plan)

    def test_invalid_source_or_capture_is_rejected_before_reader_artifact_creation(self):
        for mode in ("source", "capture"):
            with self.subTest(mode=mode):
                self.plan()
                folders = self.complete_transcripts()
                path = (Path(self.request["sources"][0]["transcript"]["path"]) if mode == "source"
                        else folders[-1] / "capture.json")
                path.chmod(0o600)
                path.write_bytes(b'{"tampered":true}\n')
                with self.assertRaises(RuntimeError):
                    reader.export_reader(*self.args)
                self.assertFalse((Path(self.request["state_root"]) / "reader-exports").exists())


if __name__ == "__main__":
    unittest.main()
