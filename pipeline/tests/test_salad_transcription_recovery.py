"""Synthetic, offline crash-window regressions for the Salad runtime."""

from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid
import wave

from pipeline import salad_transcription as runtime
from pipeline import salad_transcription_contract as contract
from pipeline.tests.test_salad_transcription_contract import recording


class CloudRecoveryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="salad-recovery-fixture-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        source = self.root / "source"
        source.mkdir(mode=0o700)
        row = recording(seconds=1)
        row["manifest"]["path"] = str(source / "recording-input.json")
        row["audio"]["path"] = str(source / "audio.flac")
        # No actual source media, runtime, credentials, or FFmpeg are needed.
        self.plan = contract.build_plan(
            [row], organization="example-org", output_root=str(self.root / "cloud"),
            ffmpeg={"path": "/usr/bin/ffmpeg", "sha256": "f" * 64},
            rate_usd_per_hour="0.20", max_estimated_cost_usd="1",
            chunk_seconds=1, overlap_seconds=0,
        )
        self.workspace = runtime.Workspace(self.plan)
        self.chunk_id, self.chunk = next(iter(self.workspace.chunks.items()))
        self.job_id = str(uuid.UUID(int=1))
        self.url = "https://storage-api.salad.com/organizations/example-org/files/audio.wav" + "?token=synthetic"
        self.output = {"text": "Synthetic transcript.", "sentence_level_timestamps": [
            {"text": "Synthetic transcript.", "start": 0, "end": 1}
        ]}
        self.client = mock.Mock()

    @staticmethod
    def digest(value):
        return hashlib.sha256(contract.canonical_bytes(value)).hexdigest()

    @staticmethod
    def write_wav(path: Path, samples: int):
        with wave.open(str(path), "wb") as handle:
            handle.setparams((1, 2, 16_000, samples, "NONE", "not compressed"))
            handle.writeframes(b"\x00\x00" * samples)
        path.chmod(0o600)

    def prepared(self):
        directory = self.workspace.directory(self.chunk_id)
        path = directory / "audio.wav"
        self.write_wav(path, self.chunk["analysis"]["end_sample"] - self.chunk["analysis"]["start_sample"])
        receipt = {
            "kind": "himr_salad_prepared_audio", "schema_version": 1,
            "plan_id": self.plan["plan_id"], "chunk_id": self.chunk_id,
            "path": str(path), **self.workspace._verify_wav(path, self.chunk),
        }
        runtime._write_json(directory / "prepared.json", receipt)
        self.workspace.row(self.chunk_id)["status"] = "prepared"
        self.workspace.save()
        return path, receipt

    def intent(self, status="submitting"):
        intent = self.workspace._intent(self.chunk_id, self.url)
        row = self.workspace.row(self.chunk_id)
        row.update(status=status, intent=intent, intent_sha256=self.digest(intent))
        if status in {"pending", "running", "succeeded", "completed", "failed", "cancelled"}:
            row["provider_job_id"] = self.job_id
        receipt = {"plan_id": self.plan["plan_id"], "chunk_id": self.chunk_id,
                   "intent": intent, "intent_sha256": self.digest(intent)}
        runtime._write_json(self.workspace.directory(self.chunk_id) / "submission-intent.json", receipt)
        if status != "completed":
            self.workspace.save()
        return row

    def job(self, *, output=None, status="succeeded"):
        row = self.workspace.row(self.chunk_id)
        return {
            "id": self.job_id, "status": status,
            "organization_name": self.plan["provider"]["organization"],
            "inference_endpoint_name": self.plan["provider"]["engine"],
            "input": copy.deepcopy(row["intent"]["input"]),
            "metadata": copy.deepcopy(row["intent"]["metadata"]),
            "output": copy.deepcopy(self.output if output is None else output),
        }

    def test_missing_runtime_state_with_submission_evidence_never_restarts(self):
        with self.workspace.locked():
            self.intent()
        state_path = self.workspace.root / "state.json"
        state_path.unlink()
        with self.assertRaisesRegex(runtime.CloudPipelineError, "state is missing"):
            with runtime.Workspace(self.plan).locked() as restarted:
                restarted.run_cycle(self.client)
        self.client.assert_not_called()
        self.client.submit.assert_not_called()

    def test_rolled_back_state_cannot_erase_immutable_submission_intent(self):
        with self.workspace.locked():
            self.intent()
            saved = copy.deepcopy(self.workspace.state)
        for status in (None, "planned", "prepared", "uploaded"):
            stale = copy.deepcopy(saved)
            stale["chunks"] = {} if status is None else {self.chunk_id: {"status": status}}
            runtime._write_json(self.workspace.root / "state.json", stale, replace=True)
            with self.subTest(status=status), self.assertRaisesRegex(runtime.CloudPipelineError, "evidence is ahead"):
                with runtime.Workspace(self.plan).locked() as restarted:
                    restarted.run_cycle(self.client)
        self.client.submit.assert_not_called()

    def test_immutable_intent_receipt_must_match_runtime_intent(self):
        with self.workspace.locked():
            row = self.intent()
            row["intent"] = self.workspace._intent(self.chunk_id, self.url + "-different")
            row["intent_sha256"] = self.digest(row["intent"])
            with self.assertRaisesRegex(runtime.CloudPipelineError, "intent receipt differs"):
                self.workspace._validate_state()

    def test_completed_state_rejects_null_missing_or_malformed_hashes(self):
        with self.workspace.locked():
            row = self.intent(status="completed")
            keys = ("transcript_sha256", "provider_job_sha256", "provider_output_sha256")
            for key in keys:
                for bad in (None, "not-a-digest", "A" * 64, 123):
                    row.update({name: "a" * 64 for name in keys})
                    row[key] = bad
                    with self.subTest(key=key, bad=bad), self.assertRaises(runtime.CloudPipelineError):
                        self.workspace._validate_state()
                row.update({name: "a" * 64 for name in keys})
                row.pop(key)
                with self.subTest(key=key, missing=True), self.assertRaises(runtime.CloudPipelineError):
                    self.workspace._validate_state()

    def test_truncated_wav_with_intact_header_is_not_admitted(self):
        with self.workspace.locked():
            directory = self.workspace.directory(self.chunk_id)
            path = directory / "audio.wav"
            self.write_wav(path, 16_000)
            complete_bytes = path.read_bytes()
            path.write_bytes(complete_bytes[:-2])
            with wave.open(str(path), "rb") as handle:
                self.assertEqual(handle.getnframes(), 16_000)  # Header still lies.
                self.assertEqual(len(handle.readframes(16_000)), 31_998)
            with self.assertRaises(runtime.CloudPipelineError):
                self.workspace._verify_wav(path, self.chunk)

    def test_same_inode_orphan_wav_link_recovers_without_removing_other_files(self):
        with self.workspace.locked():
            path, receipt = self.prepared()
            orphan = path.parent / ".salad-wav-interrupted"
            unrelated = path.parent / ".salad-wav-unrelated"
            os.link(path, orphan)
            unrelated.write_bytes(b"unrelated fixture remains untouched")
            self.assertEqual(path.stat().st_nlink, 2)
            with mock.patch.object(self.workspace, "_render_wav", side_effect=AssertionError("unnecessary render")):
                replayed = self.workspace.prepare(self.chunk_id)
            self.assertEqual(replayed, receipt)
            self.assertEqual(path.stat().st_nlink, 1)
            self.assertFalse(orphan.exists())
            self.assertEqual(unrelated.read_bytes(), b"unrelated fixture remains untouched")

    def test_unrecognized_wav_hardlink_is_not_silently_removed(self):
        with self.workspace.locked():
            path, _receipt = self.prepared()
            unknown = path.parent / "someone-elses-audio.wav"
            os.link(path, unknown)
            with self.assertRaises(runtime.CloudPipelineError):
                self.workspace.prepare(self.chunk_id)
            self.assertTrue(unknown.exists())
            self.assertEqual(path.stat().st_nlink, 2)

    def test_cached_inline_output_must_match_provider_job(self):
        with self.workspace.locked():
            self.intent()
            path = self.workspace.directory(self.chunk_id) / "provider-output.json"
            runtime._write_json(path, {"text": "Unrelated cached text."})
            with self.assertRaisesRegex(runtime.CloudPipelineError, "immutable output differs"):
                self.workspace._accept_job(self.chunk_id, self.job(), self.client)
            self.assertEqual(self.workspace.row(self.chunk_id)["status"], "succeeded")
            self.assertFalse((path.parent / "transcript.json").exists())
        self.client.download_output.assert_not_called()

    def test_interrupted_url_output_is_redownloaded_before_reuse(self):
        with self.workspace.locked():
            self.intent(status="succeeded")
            job = self.job(output={"url": self.url})
            directory = self.workspace.directory(self.chunk_id)
            runtime._write_json(directory / "provider-job.json", job)
            runtime._write_json(directory / "provider-output.json", self.output)
            self.client.download_output.return_value = copy.deepcopy(self.output)
            self.workspace._accept_job(self.chunk_id, job, self.client)
            self.assertEqual(self.workspace.row(self.chunk_id)["status"], "completed")
        self.client.download_output.assert_called_once_with(self.url)

    def test_changed_interrupted_url_output_is_not_blessed(self):
        with self.workspace.locked():
            self.intent(status="succeeded")
            job = self.job(output={"url": self.url})
            directory = self.workspace.directory(self.chunk_id)
            runtime._write_json(directory / "provider-job.json", job)
            runtime._write_json(directory / "provider-output.json", {"text": "Altered cached output."})
            self.client.download_output.return_value = copy.deepcopy(self.output)
            with self.assertRaisesRegex(runtime.CloudPipelineError, "immutable output differs"):
                self.workspace._accept_job(self.chunk_id, job, self.client)
            self.assertNotEqual(self.workspace.row(self.chunk_id)["status"], "completed")

    def test_collection_requires_saved_provider_job_to_have_succeeded(self):
        with self.workspace.locked():
            self.intent()
            self.workspace._accept_job(self.chunk_id, self.job(), self.client)
            path = self.workspace.directory(self.chunk_id) / "provider-job.json"
            value = runtime.read_json(path)
            value["status"] = "pending"
            runtime._write_json(path, value, replace=True)
            self.workspace.row(self.chunk_id)["provider_job_sha256"] = runtime.file_sha256(path)
            self.workspace.save()
            with self.assertRaisesRegex(runtime.CloudPipelineError, "successful provider job"):
                self.workspace.collect()

    def test_runtime_supplies_exact_prepared_hash_and_size_to_upload(self):
        with self.workspace.locked():
            path, receipt = self.prepared()
            self.client.upload_file.return_value = self.url

            def accept(_engine, payload):
                row = runtime.read_json(self.workspace.root / "state.json")["chunks"][self.chunk_id]
                self.assertEqual(row["status"], "submitting")
                self.assertEqual(row["intent"], payload)
                return self.job(status="pending")

            self.client.submit.side_effect = accept
            self.workspace.submit(self.chunk_id, self.client)
            self.assertEqual(self.workspace.row(self.chunk_id)["status"], "pending")
        self.client.upload_file.assert_called_once_with(
            path, f"himr/{self.plan['plan_id']}/{self.chunk_id}.wav", expires_seconds=259200,
            expected_sha256=receipt["sha256"], expected_byte_count=receipt["byte_count"],
        )


if __name__ == "__main__":
    unittest.main()
