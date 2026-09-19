from __future__ import annotations

from contextlib import redirect_stdout, redirect_stderr
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import uuid
from unittest import mock
import wave

from pipeline import salad_transcription as runtime
from pipeline.salad_transcription_contract import build_plan, canonical_bytes, load_recording_input


class CloudCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="salad-offline-")
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir(mode=0o700)
        self.output = self.root / "cloud"
        self.audio = self.source / "audio.flac"
        self.manifest = self.source / "recording-input.json"
        self.audio.write_bytes(b"placeholder not decoded by metadata-only operations")
        self.write_manifest()
        self.selection = self.root / "selection.json"
        self.selection.write_bytes(canonical_bytes({"kind": "himr_salad_input_selection", "schema_version": 1,
                                                   "recordings": [{"recording_input": str(self.manifest), "sha256": runtime.file_sha256(self.manifest)}]}))
        self.plan_path = self.root / "cloud-plan.json"

    def tearDown(self):
        self.temporary.cleanup()

    def write_manifest(self):
        value = {"kind": "himr_longform_recording_input_manifest", "schema_version": 1, "boundary_candidates": [],
                 "recording": {"recording_id": "recording-synthetic", "media_id": "media-synthetic",
                               "input": {"path": str(self.audio), "sha256": runtime.file_sha256(self.audio),
                                         "byte_count": self.audio.stat().st_size, "sample_rate_hz": 16000,
                                         "total_samples": 20_000, "duration_ms": 1250, "channels": 1,
                                         "artifact_id": "audio-synthetic"}}}
        self.manifest.write_bytes(canonical_bytes(value))

    def plan(self):
        row = load_recording_input(self.manifest, runtime.file_sha256(self.manifest))
        value = build_plan([row], organization="example-org", output_root=str(self.output),
                           ffmpeg={"path": "/usr/bin/ffmpeg", "sha256": runtime.file_sha256(Path("/usr/bin/ffmpeg"))},
                           rate_usd_per_hour="0.20", max_estimated_cost_usd="1", chunk_seconds=1, overlap_seconds=0)
        self.plan_path.write_bytes(canonical_bytes(value))
        return value

    def invoke(self, arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = runtime.main(arguments)
        return code, stdout.getvalue(), stderr.getvalue()

    def bound_args(self, command):
        return [command, "--plan", str(self.plan_path), "--plan-sha256", runtime.file_sha256(self.plan_path)]

    def test_plan_validate_status_are_offline_and_do_not_read_audio(self):
        with mock.patch.object(runtime, "SaladClient", side_effect=AssertionError("network client constructed")):
            code, stdout, stderr = self.invoke(["plan", "--selection", str(self.selection), "--organization", "example-org",
                                               "--output-root", str(self.output), "--ffmpeg", "/usr/bin/ffmpeg",
                                               "--rate-usd-per-hour", "0.20", "--max-estimated-cost-usd", "1",
                                               "--output", str(self.plan_path)])
            self.assertEqual(code, 0, stderr)
            self.assertIn('"plan_sha256"', stdout)
            self.assertEqual(self.plan_path.stat().st_mode & 0o777, 0o600)
            self.audio.unlink()  # Metadata inspection must still work.
            self.assertEqual(self.invoke(self.bound_args("validate"))[0], 0)
            self.assertEqual(self.invoke(self.bound_args("status"))[0], 0)
            self.assertFalse(self.output.exists())

    def test_cloud_gate_runs_before_workspace_or_client(self):
        self.plan()
        with mock.patch.object(runtime, "SaladClient", side_effect=AssertionError("unexpected network client")):
            code, _, stderr = self.invoke(self.bound_args("run-cycle"))
        self.assertEqual(code, 2)
        self.assertIn("--allow-cloud", stderr)
        self.assertFalse(self.output.exists())

    def test_plan_diarization_and_summary_flags_are_sealed_without_cloud_access(self):
        arguments = ["plan", "--selection", str(self.selection), "--organization", "example-org",
                     "--output-root", str(self.output), "--ffmpeg", "/usr/bin/ffmpeg",
                     "--rate-usd-per-hour", "0.20", "--max-estimated-cost-usd", "1", "--output", str(self.plan_path)]
        with mock.patch.object(runtime, "SaladClient", side_effect=AssertionError("unexpected cloud client")):
            code, stdout, stderr = self.invoke(arguments + ["--diarization", "both", "--summary-words", "200"])
        self.assertEqual(code, 0, stderr)
        options = runtime.read_json(self.plan_path)["transcription_options"]
        self.assertEqual(options, {"language_code": "en", "sentence_level_timestamps": True, "word_level_timestamps": True,
                                   "diarization": True, "sentence_diarization": True, "summarize": 200})
        self.assertEqual(json.loads(stdout)["transcription_options"], options)
        self.assertFalse(self.output.exists())

    def test_lite_summary_rejected_before_plan_creation(self):
        code, _, stderr = self.invoke(["plan", "--selection", str(self.selection), "--organization", "example-org",
                                       "--output-root", str(self.output), "--ffmpeg", "/usr/bin/ffmpeg",
                                       "--rate-usd-per-hour", "0.20", "--max-estimated-cost-usd", "1",
                                       "--engine", "transcription-lite", "--summary-words", "200", "--output", str(self.plan_path)])
        self.assertEqual(code, 2, stderr)
        self.assertFalse(self.plan_path.exists())
        self.assertFalse(self.output.exists())

    def test_large_whole_recording_uses_temp_without_forwarding_api_credentials(self):
        row = load_recording_input(self.manifest, runtime.file_sha256(self.manifest))
        row["audio"].update(total_samples=7200 * 16000, duration_ms=7_200_000)
        plan = build_plan([row], organization="example-org", output_root=str(self.output),
                           ffmpeg={"path": "/usr/bin/ffmpeg", "sha256": "f" * 64},
                           rate_usd_per_hour="0.20", max_estimated_cost_usd="1")
        workspace = runtime.Workspace(plan)
        self.assertEqual(len(workspace.chunks), 1)
        chunk_id = next(iter(workspace.chunks))
        self.assertEqual(workspace.chunks[chunk_id]["upload_provider"], "temp_sh")
        client = mock.Mock()
        client.submit.side_effect = lambda engine, payload: {
            "id": str(uuid.UUID(int=1)), "organization_name": "example-org", "inference_endpoint_name": engine,
            "input": copy.deepcopy(payload["input"]), "metadata": copy.deepcopy(payload["metadata"]), "status": "pending"}
        receipt = {"path": str(self.audio), "sha256": runtime.file_sha256(self.audio), "byte_count": self.audio.stat().st_size}
        with workspace.locked(), mock.patch.object(workspace, "prepare", return_value=receipt), \
                mock.patch.object(runtime, "upload_temp_file", return_value="https://temp.sh/fixture/audio.wav") as upload:
            workspace.submit(chunk_id, client)
            self.assertEqual(workspace.row(chunk_id)["upload_provider"], "temp_sh")
        client.upload_file.assert_not_called()
        upload.assert_called_once_with(self.audio, expected_sha256=receipt["sha256"], expected_byte_count=receipt["byte_count"])
        self.assertEqual(client.submit.call_args.args[1]["input"]["url"], "https://temp.sh/fixture/audio.wav")

    def test_s4_failure_does_not_switch_hosts_or_submit_paid_job(self):
        workspace = runtime.Workspace(self.plan())
        client = mock.Mock()
        client.upload_file.side_effect = runtime.CloudClientError("synthetic S4 rejection", status_code=403)
        receipt = {"path": str(self.audio), "sha256": runtime.file_sha256(self.audio), "byte_count": self.audio.stat().st_size}
        with workspace.locked(), mock.patch.object(workspace, "prepare", return_value=receipt), \
                mock.patch.object(runtime, "upload_temp_file") as upload:
            with self.assertRaises(runtime.CloudClientError):
                workspace.submit(next(iter(workspace.chunks)), client)
        upload.assert_not_called()
        client.submit.assert_not_called()

    def test_plan_hash_change_rejected(self):
        self.plan()
        arguments = self.bound_args("validate")
        with self.plan_path.open("ab") as handle:
            handle.write(b"\n")
        code, _, error = self.invoke(arguments)
        self.assertEqual(code, 2)
        self.assertIn("SHA-256 differs", error)

    def test_cli_entrypoint_works_outside_repository(self):
        entrypoint = Path(runtime.__file__).parent / "bin" / "salad-transcription"
        result = subprocess.run([str(entrypoint), "--help"], cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("run-cycle", result.stdout)

    def test_no_symlink_ancestors_in_private_workspace_or_input(self):
        link = self.root / "linked"
        link.symlink_to(self.source, target_is_directory=True)
        with self.assertRaises(OSError):
            runtime.read_json(link / self.manifest.name)
        plan = self.plan()
        plan = build_plan([{key: plan["recordings"][0][key] for key in ("manifest", "recording_id", "media_id", "audio")}],
                           organization="example-org", output_root=str(link / "new-work"), ffmpeg=plan["ffmpeg"],
                           rate_usd_per_hour="0.20", max_estimated_cost_usd="1")
        with self.assertRaises(runtime.CloudPipelineError):
            with runtime.Workspace(plan).locked():
                pass
        self.assertFalse((self.source / "new-work").exists())

    def test_workspace_is_single_plan_bound_and_exclusively_locked(self):
        plan = self.plan()
        with runtime.Workspace(plan).locked():
            with self.assertRaisesRegex(runtime.CloudPipelineError, "another command"):
                with runtime.Workspace(plan).locked():
                    pass
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.output / "state.json").stat().st_mode & 0o777, 0o600)

    def test_missing_state_with_chunk_evidence_does_not_restart_paid_work(self):
        plan = self.plan()
        with runtime.Workspace(plan).locked() as workspace:
            workspace.directory(next(iter(workspace.chunks)))
        (self.output / "state.json").unlink()
        with self.assertRaisesRegex(runtime.CloudPipelineError, "state is missing"):
            with runtime.Workspace(plan).locked():
                pass

    def test_duplicate_nonfinite_and_fifo_json_rejected(self):
        path = self.root / "bad.json"
        for body in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}'):
            path.write_bytes(body)
            with self.assertRaises(runtime.CloudPipelineError):
                runtime.read_json(path)
        path.unlink()
        os.mkfifo(path)
        with self.assertRaises(runtime.CloudPipelineError):
            runtime.read_json(path)

    @unittest.skipUnless(shutil.which("ffmpeg"), "requires FFmpeg")
    def test_real_synthetic_flac_chunks_exact_samples_and_resume(self):
        command = ["/usr/bin/ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
                   "sine=frequency=440:sample_rate=16000:duration=1.25", "-ac", "1", "-ar", "16000", "-c:a", "flac", str(self.audio)]
        result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.write_manifest()
        plan = self.plan()
        original_audio = self.audio.read_bytes()
        with runtime.Workspace(plan).locked() as workspace:
            for chunk_id in workspace.chunks:
                receipt = workspace.prepare(chunk_id)
                with wave.open(receipt["path"], "rb") as wav:
                    chunk = workspace.chunks[chunk_id]
                    self.assertEqual(wav.getnframes(), chunk["analysis"]["end_sample"] - chunk["analysis"]["start_sample"])
        with runtime.Workspace(plan).locked() as workspace:
            with mock.patch.object(workspace, "_render_wav", side_effect=AssertionError("repeated rendering")):
                for chunk_id in workspace.chunks:
                    workspace.prepare(chunk_id)
        self.assertEqual(self.audio.read_bytes(), original_audio)

    def test_metadata_mismatch_prevents_preparation_before_ffmpeg(self):
        plan = self.plan()
        self.audio.write_bytes(b"different")
        with runtime.Workspace(plan).locked() as workspace:
            with mock.patch.object(runtime.subprocess, "run", side_effect=AssertionError("FFmpeg called")):
                with self.assertRaisesRegex(runtime.CloudPipelineError, "source audio hash or size differs"):
                    workspace.prepare(next(iter(workspace.chunks)))


if __name__ == "__main__":
    unittest.main()
