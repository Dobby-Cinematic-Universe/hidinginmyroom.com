"""Synthetic-only CPU preparation tests; no archive or cloud access."""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock
import wave

from pipeline import hybrid_audio_prepare as prepare
from pipeline import salad_transcription_contract as contract


class HybridAudioPrepareTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="hybrid-cpu-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        inputs = self.root / "inputs"
        inputs.mkdir(mode=0o700)
        self.input = inputs / "original.wav"
        with wave.open(str(self.input), "wb") as handle:
            handle.setparams((2, 2, 48000, 60000, "NONE", "not compressed"))
            handle.writeframes(b"\0\0\1\0" * 60000)
        self.input.chmod(0o600)
        self.original = self.input.read_bytes()
        self.source = {"path": str(self.input), "sha256": self.sha(self.original),
                       "byte_count": len(self.original), "media_id": "media:original-identity"}
        tools = self.root / "tools"
        tools.mkdir(mode=0o700)
        self.tools = {}
        for name in ("ffmpeg", "ffprobe"):
            path = tools / name
            path.write_bytes(("synthetic never executed " + name).encode())
            path.chmod(0o700)
            self.tools[name] = {"path": str(path), "sha256": self.sha(path.read_bytes())}
        self.output = self.root / "normalized"
        self.source_probe = {"streams": [{"codec_name": "pcm_s16le", "sample_fmt": "s16",
                            "sample_rate": "48000", "channels": 2, "duration": "1.250000",
                            "duration_ts": 60000, "time_base": "1/48000"}], "format": {"duration": "1.250000"}}
        self.audio_probe = {"streams": [{"codec_name": "flac", "sample_fmt": "s16",
                           "sample_rate": "16000", "channels": 1, "duration": "1.250000",
                           "duration_ts": 20000, "time_base": "1/16000", "bits_per_raw_sample": "16"}]}
        self.encoded = b"fLaCsynthetic normalized sample data"

    @staticmethod
    def sha(body):
        return hashlib.sha256(body).hexdigest()

    def invoke(self, **overrides):
        kwargs = {"output_root": self.output, **self.tools}
        kwargs.update(overrides)
        return prepare.prepare_audio(self.source, **kwargs)

    def encode(self, command, descriptors, deadline, *, capture=False):
        self.assertFalse(capture)
        output_fd = int(command[-1].rsplit("/", 1)[1])
        self.assertIn(output_fd, descriptors)
        os.write(output_fd, self.encoded)

    def mocked(self, **overrides):
        with mock.patch.object(prepare, "_probe", side_effect=[self.source_probe, self.audio_probe]), \
                mock.patch.object(prepare, "_run", side_effect=self.encode) as run:
            result = self.invoke(**overrides)
        return result, run

    def test_real_contract_manifest_and_cpu_only_command(self):
        result, run = self.mocked()
        self.assertFalse(result["reused"])
        self.assertEqual(result["media_id"], self.source["media_id"])
        self.assertEqual(result["audio"]["total_samples"], 20000)
        self.assertEqual(result["audio"]["duration_ms"], 1250)
        manifest_path = Path(result["recording_input"])
        self.assertEqual(self.sha(manifest_path.read_bytes()), result["sha256"])
        manifest = json.loads(manifest_path.read_text())
        self.assertEqual(manifest["boundary_candidates"], [])
        self.assertEqual(set(manifest), {"kind", "schema_version", "boundary_candidates", "recording"})
        self.assertEqual(manifest["recording"]["input"]["channels"], 1)
        recording = contract.load_recording_input(manifest_path, result["sha256"])
        plan = contract.build_plan([recording], organization="test-org", output_root=str(self.root / "cloud"),
                                   ffmpeg=self.tools["ffmpeg"], rate_usd_per_hour="0.20", max_estimated_cost_usd="1")
        self.assertEqual(plan["recordings"][0]["media_id"], self.source["media_id"])
        self.assertEqual(self.input.read_bytes(), self.original)
        self.assertEqual(Path(result["audio"]["path"]).stat().st_mode & 0o777, 0o400)
        self.assertEqual(manifest_path.parent.stat().st_mode & 0o777, 0o700)
        command = run.call_args.args[0]
        for flag, value in (("-hwaccel", "none"), ("-sample_fmt", "s16"), ("-c:a", "flac"),
                            ("-map", "0:a:0"), ("-protocol_whitelist", "file,pipe")):
            self.assertEqual(command[command.index(flag) + 1], value)
        self.assertEqual(command.count("-i"), 1)
        self.assertIn("-vn", command)
        self.assertNotIn("http", " ".join(command))
        receipt = Path(result["receipt"])
        self.assertEqual(self.sha(receipt.read_bytes()), result["receipt_sha256"])

    def test_idempotent_reuse_verifies_audio_without_reencoding(self):
        first, _ = self.mocked()
        with mock.patch.object(prepare, "_probe", return_value=self.audio_probe), \
                mock.patch.object(prepare, "_run") as run:
            second = self.invoke()
        run.assert_not_called()
        self.assertTrue(second["reused"])
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertEqual(first["receipt_sha256"], second["receipt_sha256"])

    def test_reuse_rejects_truncated_audio_without_repair_or_overwrite(self):
        first, _ = self.mocked()
        audio = Path(first["audio"]["path"])
        audio.chmod(0o600)
        audio.write_bytes(b"fLaCtruncated")
        with mock.patch.object(prepare, "_probe", return_value=self.audio_probe), \
                self.assertRaises(prepare.AudioPreparationError):
            self.invoke()
        self.assertEqual(audio.read_bytes(), b"fLaCtruncated")

    def test_reuse_rejects_truncated_receipt_and_duplicate_json(self):
        first, _ = self.mocked()
        receipt = Path(first["receipt"])
        for body in (b'{"kind":', b'{"kind":"one","kind":"two"}', b'{"bad":NaN}'):
            with self.subTest(body=body):
                receipt.chmod(0o600)
                receipt.write_bytes(body)
                with self.assertRaises(prepare.AudioPreparationError):
                    self.invoke()
                self.assertEqual(receipt.read_bytes(), body)

    def test_hash_size_and_tool_binding_fail_before_any_execution(self):
        original = copy.deepcopy(self.source)
        for field, value in (("sha256", "0" * 64), ("byte_count", self.source["byte_count"] - 1)):
            with self.subTest(field=field), mock.patch.object(prepare, "_run") as run:
                self.source = {**original, field: value}
                with self.assertRaises(prepare.AudioPreparationError):
                    self.invoke()
                run.assert_not_called()
                self.assertFalse(self.output.exists())
        self.source = original
        with mock.patch.object(prepare, "_run") as run, self.assertRaises(prepare.AudioPreparationError):
            self.invoke(ffmpeg={**self.tools["ffmpeg"], "sha256": "0" * 64})
        run.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_symlink_leaf_ancestor_tools_and_hardlinks_rejected(self):
        link = self.root / "input-link.wav"
        link.symlink_to(self.input)
        real = self.source["path"]
        self.source["path"] = str(link)
        with self.assertRaises(prepare.AudioPreparationError):
            self.invoke()
        parent_link = self.root / "linked-inputs"
        parent_link.symlink_to(self.input.parent, target_is_directory=True)
        self.source["path"] = str(parent_link / self.input.name)
        with self.assertRaises(prepare.AudioPreparationError):
            self.invoke()
        self.source["path"] = real
        tool_link = self.root / "ffmpeg-link"
        tool_link.symlink_to(self.tools["ffmpeg"]["path"])
        with self.assertRaises(prepare.AudioPreparationError):
            self.invoke(ffmpeg={**self.tools["ffmpeg"], "path": str(tool_link)})
        os.link(self.input, self.root / "hardlink.wav")
        with self.assertRaises(prepare.AudioPreparationError):
            self.invoke()

    def test_encode_failure_or_timeout_removes_only_current_staging(self):
        for failure in (prepare.AudioPreparationError("synthetic failure"), subprocess.TimeoutExpired("ffmpeg", 1)):
            with self.subTest(failure=type(failure).__name__), \
                    mock.patch.object(prepare, "_probe", return_value=self.source_probe), \
                    mock.patch.object(prepare, "_run", side_effect=failure), \
                    self.assertRaises(prepare.AudioPreparationError):
                self.invoke()
            self.assertEqual([path.name for path in self.output.iterdir()], [".prepare.lock"])
            self.assertEqual(self.input.read_bytes(), self.original)

    def test_interrupted_staging_is_never_reused_or_deleted(self):
        self.output.mkdir(mode=0o700)
        orphan = self.output / ".hybridaudio-interrupted"
        orphan.mkdir(mode=0o700)
        partial = orphan / "audio.flac"
        partial.write_bytes(b"unfinished from a prior process")
        result, _ = self.mocked()
        self.assertNotEqual(Path(result["audio"]["path"]).parent, orphan)
        self.assertEqual(partial.read_bytes(), b"unfinished from a prior process")

    def test_source_duration_and_normalized_sample_profile_bounds(self):
        for duration in (None, "NaN", "0", "86400.001"):
            with self.subTest(duration=duration):
                bad = copy.deepcopy(self.source_probe)
                bad["streams"][0]["duration"] = duration
                with mock.patch.object(prepare, "_probe", return_value=bad), \
                        mock.patch.object(prepare, "_run") as run, self.assertRaises(prepare.AudioPreparationError):
                    self.invoke()
                run.assert_not_called()
        for field, value in (("time_base", "1/1000"), ("duration_ts", 0), ("duration_ts", True),
                             ("duration_ts", 86400 * 16000 + 1), ("channels", 2),
                             ("sample_fmt", "s32"), ("bits_per_raw_sample", "24")):
            with self.subTest(field=field, value=value):
                bad = copy.deepcopy(self.audio_probe)
                bad["streams"][0][field] = value
                with mock.patch.object(prepare, "_probe", side_effect=[self.source_probe, bad]), \
                        mock.patch.object(prepare, "_run", side_effect=self.encode), \
                        self.assertRaises(prepare.AudioPreparationError):
                    self.invoke()

    def test_truncation_detected_from_source_duration(self):
        source = copy.deepcopy(self.source_probe)
        source["streams"][0]["duration"] = "50"
        with mock.patch.object(prepare, "_probe", side_effect=[source, self.audio_probe]), \
                mock.patch.object(prepare, "_run", side_effect=self.encode), self.assertRaisesRegex(prepare.AudioPreparationError, "shorter"):
            self.invoke()

    def test_no_space_and_busy_lock_do_not_encode(self):
        with mock.patch.object(prepare, "_probe", return_value=self.source_probe), \
                mock.patch.object(prepare.shutil, "disk_usage", return_value=mock.Mock(free=1024)), \
                mock.patch.object(prepare, "_run") as run, self.assertRaisesRegex(prepare.AudioPreparationError, "space"):
            self.invoke()
        run.assert_not_called()
        with (self.output / ".prepare.lock").open("r+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(prepare.AudioPreparationError, "locked"):
                self.invoke()

    def test_invalid_paths_limits_and_nonprivate_output_rejected(self):
        original = copy.deepcopy(self.source)
        for field, value in (("path", "https://example.test/video.mp4"),
                             ("byte_count", prepare.MAX_SOURCE_BYTES + 1), ("byte_count", True)):
            with self.subTest(field=field):
                self.source = {**original, field: value}
                with self.assertRaises(prepare.AudioPreparationError):
                    self.invoke()
        self.source = original
        for output in (self.input.parent, self.root, Path.cwd()):
            with self.subTest(output=str(output)), self.assertRaises(prepare.AudioPreparationError):
                self.invoke(output_root=output)
        for timeout in (True, 0, 7201, 1.5):
            with self.subTest(timeout=timeout), self.assertRaises(prepare.AudioPreparationError):
                self.invoke(timeout_seconds=timeout)
        self.output.mkdir(mode=0o755)
        with self.assertRaises(prepare.AudioPreparationError):
            self.invoke()
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o755)

    def test_mutation_during_encoding_is_rejected(self):
        def mutate(command, descriptors, deadline, *, capture=False):
            self.encode(command, descriptors, deadline, capture=capture)
            with self.input.open("r+b") as handle:
                handle.write(b"changed")
        with mock.patch.object(prepare, "_probe", side_effect=[self.source_probe, self.audio_probe]), \
                mock.patch.object(prepare, "_run", side_effect=mutate), self.assertRaisesRegex(prepare.AudioPreparationError, "changed"):
            self.invoke()
        self.assertEqual([path.name for path in self.output.iterdir()], [".prepare.lock"])

    def test_caller_and_output_leases_inherited_not_closed(self):
        lease = os.open(self.root / "caller.lock", os.O_RDWR | os.O_CREAT, 0o600)
        self.addCleanup(os.close, lease)
        with mock.patch.object(prepare, "_probe", side_effect=[self.source_probe, self.audio_probe]) as probe, \
                mock.patch.object(prepare, "_run", side_effect=self.encode) as run:
            self.invoke(lease_fds=(lease,))
        os.fstat(lease)
        inherited = run.call_args.args[1]
        self.assertIn(lease, inherited)
        for call in probe.call_args_list:
            self.assertIn(lease, call.kwargs["lease_fds"])
            self.assertEqual(len(call.kwargs["lease_fds"]), 2)
        for leases in ([lease], (True,), (-1,), (999999,)):
            with self.subTest(leases=leases), self.assertRaises(prepare.AudioPreparationError):
                self.invoke(lease_fds=leases)

    def test_media_subprocess_invocation_passes_descriptors_and_total_timeout(self):
        with mock.patch.object(prepare.subprocess, "run", return_value=mock.Mock(returncode=0, stdout=b"{}")) as run:
            prepare._run(["/proc/self/fd/12"], [12, 13, 14], prepare.time.monotonic() + 5, capture=True)
        self.assertEqual(run.call_args.kwargs["pass_fds"], (12, 13, 14))
        self.assertGreater(run.call_args.kwargs["timeout"], 0)
        self.assertLessEqual(run.call_args.kwargs["timeout"], 5)
        self.assertEqual(run.call_args.kwargs["stdin"], subprocess.DEVNULL)
        self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertEqual(run.call_args.kwargs["env"], {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"})

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "synthetic smoke needs installed FFmpeg")
    def test_real_synthetic_ffmpeg_smoke_and_reuse(self):
        tools = {}
        for name in ("ffmpeg", "ffprobe"):
            path = Path(shutil.which(name)).resolve()
            tools[name] = {"path": str(path), "sha256": self.sha(path.read_bytes())}
        first = self.invoke(**tools, timeout_seconds=30)
        self.assertEqual(first["audio"]["total_samples"], 20000)
        self.assertEqual(first["audio"]["duration_ms"], 1250)
        self.assertEqual(Path(first["audio"]["path"]).read_bytes()[:4], b"fLaC")
        second = self.invoke(**tools, timeout_seconds=30)
        self.assertTrue(second["reused"])
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertEqual(self.input.read_bytes(), self.original)


if __name__ == "__main__":
    unittest.main()
