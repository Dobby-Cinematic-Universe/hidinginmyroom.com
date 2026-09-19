"""Synthetic metadata-only archive selection; no transcript parsing or network."""
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import transcript_summary as runner
from pipeline import transcript_summary_archive_select as select


class ArchiveSelectionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.old, self.new, self.short = (self.root / name for name in ("old", "new", "short"))
        self.inventory = self.root / "inventory.json"
        self.canary = "himrlongjob_canary"
        self.media = [character * 64 for character in "abc"]
        self.kwargs = {"inventory": self.inventory, "longform_roots": (self.old, self.new),
                       "normalized_roots": (self.short,), "canary_job_id": self.canary,
                       "expected_audio_count": 3, "shard_size": 2}
        self.file(self.inventory, {"kind": "himr_private_speaker_screen_archive_inventory", "schema_version": 1,
            "records": [{"audio_stream_count": 1, "recording": {"sha256": value}} for value in self.media]
                       + [{"audio_stream_count": 0, "recording": {"sha256": "d" * 64}}]})
        self.longform(self.old, "himrlongjob_old", self.media[0])
        self.longform(self.new, self.canary, self.media[0])
        self.longform(self.old, "himrlongjob_queue", self.media[1], queued=True)
        self.normalized("overlap", self.media[1])
        self.normalized("empty", self.media[2], empty=True)

    def file(self, path, value):
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        raw = value if isinstance(value, bytes) else runner.canonical(value)
        path.write_bytes(raw)
        return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}

    def preprocess(self, label, media):
        # Neither raw media nor the preprocessed audio exists: selection must not open either.
        audio = {"artifact_id": "audio_" + label, "path": str(self.root / (label + ".flac")),
                 "sha256": "f" * 64, "duration_ms": 1000}
        ref = self.file(self.root / (label + "-preprocess.json"), {
            "status": "completed", "input": {"sha256": media}, "artifacts": [deepcopy(audio)]})
        return ref, audio

    def longform(self, root, job_id, media, *, queued=False):
        folder = root / "jobs" / job_id
        raw = b"Deliberately not parsed as transcript JSON."
        transcript = self.file(folder / "recording-transcript.json", raw)
        if queued:
            pre, audio = self.preprocess(job_id, media)
            source = {"preprocess_result": pre, "audio": audio}
        else:
            source = {"source_media": {"sha256": media, "duration_ms": 1000}}
        self.file(folder / "job.json", {"kind": "himr_longform_asr_campaign_job", "job_id": job_id,
                                       "source": source})
        self.file(folder / "completion.json", {"kind": "himr_longform_asr_campaign_completion",
            "job_id": job_id, "runner": {"status": "completed"},
            "assembler": {"status": "completed", "coverage_complete": True, "segment_count": 2},
            "transcript": {**transcript, "byte_count": len(raw)}})

    def normalized(self, label, media, *, empty=False):
        pre, audio = self.preprocess(label, media)
        work_id = "gpuasrwo5_" + label
        self.file(self.short / "gpu-work-orders/work-orders" / (work_id + ".json"), {
            "work_order_id": work_id, "identity_sha256": "e" * 64,
            "input": {"artifact_id": audio["artifact_id"], "path": audio["path"],
                      "expected_sha256": audio["sha256"]},
            "source_lineage": {"preprocess_result": pre}})
        folder = self.short / "gpu-results/asr/faster-whisper-gpu-v5/sha256/ff" / label / "results/result"
        raw = b"Not read; empty-content status is receipt metadata only."
        artifact = self.file(folder / "transcript.normalized.json", raw)
        self.file(folder / "result.json", {"kind": "himr_faster_whisper_gpu_result", "status": "completed",
            "work_order": {"work_order_id": work_id, "identity_sha256": "e" * 64},
            "input": {"artifact_id": audio["artifact_id"], "sha256": audio["sha256"],
                      "timeline_offset_ms": 0, "duration_ms": 1000},
            "artifacts": [{**artifact, "byte_count": len(raw), "artifact_kind": "transcript_normalized_json"}],
            "transcript": {"segment_count": 0 if empty else 2, "text_character_count": 0 if empty else 20},
            "execution": {"completed_at": "2026-09-12T10:00:00Z"}})

    def test_dedup_uses_original_media_native_longform_ids_and_retains_empty_sources(self):
        with patch.object(runner.sources_module, "read_json", side_effect=AssertionError("no transcript parsing")), \
                patch.object(runner, "api_client", side_effect=AssertionError("no API")):
            value = select.build_selection(**self.kwargs)
        self.assertEqual(value, select.build_selection(**self.kwargs))
        self.assertEqual(value["counts"]["selected_sources"], 3)
        self.assertEqual(value["counts"]["formats"], {"longform": 2, "normalized": 1})
        self.assertEqual(value["counts"]["duplicate_candidates"], 2)
        self.assertEqual(value["counts"]["metadata_empty_normalized_sources"], 1)
        self.assertEqual([len(shard) for shard in value["shards"]], [1, 2])
        self.assertEqual(value["sources"][0]["recording_id"], self.canary)
        self.assertEqual(value["sources"][1]["recording_id"], "himrlongjob_queue")
        self.assertEqual(value["sources"][2]["recording_id"], "media_sha256_" + self.media[2])
        self.assertTrue(all(source["title"] is None and source["date"] is None for source in value["sources"]))
        self.assertTrue(all(source["completion"] is None for source in value["sources"][:2]))
        self.assertIn(value["sources"][2]["completion"], value["proof_bindings"])
        self.assertEqual([row["original_media_sha256"] for row in value["recordings"]], self.media)
        self.assertFalse(value["semantics"]["transcript_content_verified"])
        self.assertFalse(value["semantics"]["paid_requests_started"])
        self.assertFalse(value["semantics"]["campaign_manifest_sealed"])

    def test_changed_preprocess_proof_is_rejected_before_output(self):
        path = self.root / "empty-preprocess.json"
        self.file(path, {"status": "completed", "input": {"sha256": "a" * 64}, "artifacts": []})
        output = self.root / "selection"
        with self.assertRaisesRegex(select.SelectionError, "metadata binding differs"):
            select.write_selection(output, **self.kwargs)
        self.assertFalse(output.exists())

    def test_missing_audio_or_exact_canary_fails_without_partial_selection(self):
        for changes in ({"normalized_roots": ()}, {"canary_job_id": "himrlongjob_not_selected"}):
            with self.subTest(changes=changes), self.assertRaises(select.SelectionError):
                select.build_selection(**{**self.kwargs, **changes})

    def test_selection_is_private_immutable_and_cli_has_no_paid_option(self):
        output = self.root / "selection"
        result = select.write_selection(output, **self.kwargs)
        self.assertEqual(result["state"], "prepared_offline_selection")
        self.assertEqual(os.stat(output).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(result["artifact"]["path"]).st_mode & 0o777, 0o400)
        self.assertEqual(runner.binding(result["artifact"]["path"]), result["artifact"])
        with self.assertRaisesRegex(select.SelectionError, "new private leaf"):
            select.write_selection(output, **self.kwargs)
        with patch.object(select, "write_selection", return_value=result) as write, \
                patch("builtins.print"), patch.object(runner, "api_client", side_effect=AssertionError("no API")):
            self.assertEqual(select.main(["--output", str(self.root / "other")]), 0)
            write.assert_called_once_with(str(self.root / "other"))


if __name__ == "__main__":
    unittest.main()
