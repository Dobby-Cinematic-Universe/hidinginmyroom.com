from __future__ import annotations

from contextlib import redirect_stderr
import hashlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


from pipeline.longform_asr_input import (
    LongformInputError,
    build_manifest,
    main,
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg is required")
class LongformAsrInputTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="longform-input-")
        self.root = Path(self.temporary.name).resolve()
        self.audio = self.root / "audio-16khz-mono.flac"
        completed = subprocess.run(
            [
                shutil.which("ffmpeg") or "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=16000:duration=1.25",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "flac",
                str(self.audio),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode(errors="replace"))
        self.routing = self.root / "routing.json"
        routing_value = {
            "schema_version": 1,
            "silence_intervals": [
                {"start_ms": 250, "end_ms": 750, "duration_ms": 500},
            ],
        }
        self.routing.write_text(json.dumps(routing_value), encoding="utf-8")
        self.result = self.root / "result.json"
        result_value = {
            "schema_version": 1,
            "status": "completed",
            "dry_run": False,
            "input": {
                "media_id": "media_sha256_" + "a" * 64,
            },
            "artifacts": [
                {
                    "artifact_id": "artifact_normalized_test",
                    "artifact_kind": "audio_16khz_mono_flac",
                    "path": str(self.audio),
                    "sha256": file_sha256(self.audio),
                    "byte_count": self.audio.stat().st_size,
                },
                {
                    "artifact_id": "artifact_routing_test",
                    "artifact_kind": "scene_silence_routing_json",
                    "path": str(self.routing),
                    "sha256": file_sha256(self.routing),
                    "byte_count": self.routing.stat().st_size,
                },
            ],
        }
        self.result.write_text(json.dumps(result_value), encoding="utf-8")
        self.ffprobe = Path(shutil.which("ffprobe") or "").resolve()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_completed_preprocess_result_becomes_exact_sample_manifest(self) -> None:
        value = build_manifest(
            self.result,
            recording_id="recording_test",
            media_id=None,
            ffprobe=self.ffprobe,
            expected_ffprobe_sha256=file_sha256(self.ffprobe),
            include_routing=True,
        )
        self.assertEqual(value["recording"]["input"]["total_samples"], 20_000)
        self.assertEqual(value["recording"]["input"]["duration_ms"], 1_250)
        self.assertEqual(value["recording"]["input"]["sample_rate_hz"], 16_000)
        self.assertEqual(value["recording"]["input"]["channels"], 1)
        self.assertEqual(
            value["boundary_candidates"],
            [
                {
                    "confidence_millionths": 0,
                    "kind": "silence_midpoint",
                    "sample": 8_000,
                }
            ],
        )

    def test_audio_tamper_and_tool_pin_fail_closed(self) -> None:
        self.audio.write_bytes(self.audio.read_bytes() + b"tamper")
        with self.assertRaisesRegex(LongformInputError, "differ"):
            build_manifest(
                self.result,
                recording_id="recording_test",
                media_id=None,
                ffprobe=self.ffprobe,
                expected_ffprobe_sha256=file_sha256(self.ffprobe),
                include_routing=False,
            )

        # Restore the artifact binding before exercising the independent tool pin.
        result = json.loads(self.result.read_text(encoding="utf-8"))
        result["artifacts"][0]["sha256"] = file_sha256(self.audio)
        result["artifacts"][0]["byte_count"] = self.audio.stat().st_size
        self.result.write_text(json.dumps(result), encoding="utf-8")
        with self.assertRaisesRegex(LongformInputError, "ffprobe differs"):
            build_manifest(
                self.result,
                recording_id="recording_test",
                media_id=None,
                ffprobe=self.ffprobe,
                expected_ffprobe_sha256="0" * 64,
                include_routing=False,
            )

    def test_ffprobe_pin_is_mandatory_for_programmatic_callers(self) -> None:
        with self.assertRaisesRegex(LongformInputError, "ffprobe SHA-256"):
            build_manifest(
                self.result,
                recording_id="recording_test",
                media_id=None,
                ffprobe=self.ffprobe,
                expected_ffprobe_sha256=None,  # type: ignore[arg-type]
                include_routing=False,
            )

    def test_cli_requires_ffprobe_pin(self) -> None:
        output = self.root / "must-not-exist.json"
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                main(
                    [
                        "--preprocess-result",
                        str(self.result),
                        "--recording-id",
                        "recording_test",
                        "--ffprobe",
                        str(self.ffprobe),
                        "--output",
                        str(output),
                    ]
                )
        self.assertEqual(raised.exception.code, 2)
        self.assertFalse(output.exists())

    def test_probe_uses_retained_proc_descriptors(self) -> None:
        actual_run = subprocess.run
        observed: dict[str, object] = {}

        def inspect_run(command: list[str], *args: object, **kwargs: object) -> object:
            observed["command"] = command
            observed["pass_fds"] = kwargs.get("pass_fds")
            for descriptor in kwargs.get("pass_fds", ()):
                os.fstat(descriptor)
            return actual_run(command, *args, **kwargs)

        with mock.patch("pipeline.longform_asr_input.subprocess.run", side_effect=inspect_run):
            build_manifest(
                self.result,
                recording_id="recording_test",
                media_id=None,
                ffprobe=self.ffprobe,
                expected_ffprobe_sha256=file_sha256(self.ffprobe),
                include_routing=False,
            )

        command = observed["command"]
        self.assertIsInstance(command, list)
        self.assertRegex(command[0], r"^/proc/self/fd/[0-9]+$")
        self.assertRegex(command[-1], r"^/proc/self/fd/[0-9]+$")
        passed = observed["pass_fds"]
        self.assertEqual(
            passed,
            (int(command[0].rsplit("/", 1)[1]), int(command[-1].rsplit("/", 1)[1])),
        )

    def test_post_probe_audio_mutation_fails_closed(self) -> None:
        actual_run = subprocess.run

        def mutate_after_probe(command: list[str], *args: object, **kwargs: object) -> object:
            completed = actual_run(command, *args, **kwargs)
            with self.audio.open("ab") as handle:
                handle.write(b"changed-after-probe")
            return completed

        with mock.patch(
            "pipeline.longform_asr_input.subprocess.run",
            side_effect=mutate_after_probe,
        ):
            with self.assertRaisesRegex(LongformInputError, "probe input verification failed"):
                build_manifest(
                    self.result,
                    recording_id="recording_test",
                    media_id=None,
                    ffprobe=self.ffprobe,
                    expected_ffprobe_sha256=file_sha256(self.ffprobe),
                    include_routing=False,
                )

    def test_cli_writes_once_without_touching_media(self) -> None:
        output = self.root / "manifest.json"
        arguments = [
            "--preprocess-result",
            str(self.result),
            "--recording-id",
            "recording_test",
            "--ffprobe",
            str(self.ffprobe),
            "--ffprobe-sha256",
            file_sha256(self.ffprobe),
            "--output",
            str(output),
        ]
        self.assertEqual(main(arguments), 0)
        before = file_sha256(self.audio)
        self.assertEqual(main(arguments), 2)
        self.assertEqual(file_sha256(self.audio), before)
        self.assertEqual(json.loads(output.read_text())["kind"], "himr_longform_recording_input_manifest")


if __name__ == "__main__":
    unittest.main()
