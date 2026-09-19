from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "transform_calibration_v2.py"
LEGACY_PROGRAM = PIPELINE_ROOT / "transform_calibration.py"
LEGACY_PROGRAM_SHA256 = "7a6c1f1c7db417ddc7507731fee8230d471691b249627cf0c9fb34c3bcd1d969"
ACQUIRE = REPOSITORY_ROOT / "acquisition" / "acquire.py"
SCHEMA = PIPELINE_ROOT / "schemas" / "transform-calibration-receipt.schema.json"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / f"transform-calibration-{os.getpid()}"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=check,
    )


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def make_writable(root: Path) -> None:
    if not root.exists():
        return
    for path in [root, *root.rglob("*")]:
        try:
            path.chmod(path.stat().st_mode | 0o700)
        except OSError:
            pass


@unittest.skipUnless(
    shutil.which("ffmpeg") and shutil.which("ffprobe"),
    "FFmpeg and FFprobe are required",
)
class TransformCalibrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import numpy  # noqa: F401
            import scipy  # noqa: F401
        except ImportError as error:
            raise unittest.SkipTest("NumPy and SciPy are required") from error
        TEST_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        make_writable(TEST_ROOT)
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, exist_ok=True)
        self.finalized_source = (self.case / "finalized-source.mp4").resolve()
        self.earlier_source = (self.case / "earlier-source.mp4").resolve()
        run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=160x90:rate=10:duration=7",
                "-f",
                "lavfi",
                "-i",
                "aevalsrc=sin(2*PI*(300*t+30*t*t)):s=16000:d=7",
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "10",
                "-c:a",
                "aac",
                "-movflags",
                "+faststart",
                str(self.finalized_source),
            ]
        )
        run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-i",
                str(self.finalized_source),
                "-t",
                "6",
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-c",
                "copy",
                str(self.earlier_source),
            ]
        )
        self.fake_ytdlp = (self.case / "fake-yt-dlp").resolve()
        self.fake_ytdlp.write_text(
            "#!/usr/bin/env python3\n"
            "import json, shutil, sys\n"
            "from pathlib import Path\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-transform-calibration-2026.08.27')\n"
            "    raise SystemExit(0)\n"
            "target = Path(sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4'))\n"
            f"earlier = Path({str(self.earlier_source)!r})\n"
            f"finalized = Path({str(self.finalized_source)!r})\n"
            "is_earlier = 'calibration-old-001' in str(target)\n"
            "shutil.copyfile(earlier if is_earlier else finalized, target)\n"
            "print(json.dumps({\n"
            "  'id': 'fixtureCal01', 'title': 'Calibration fixture',\n"
            "  'webpage_url': sys.argv[-1], 'original_url': sys.argv[-1],\n"
            "  'extractor': 'youtube', 'extractor_key': 'Youtube',\n"
            "  'ext': 'mp4', 'format_id': '396+140', 'width': 160, 'height': 90,\n"
            "  'fps': 10, 'vcodec': 'h264', 'acodec': 'aac',\n"
            "  'duration': 3 if is_earlier else 7, 'availability': 'public',\n"
            "  'live_status': 'post_live' if is_earlier else 'was_live',\n"
            "  'timestamp': 1787830505, 'upload_date': '20260827'\n"
            "}))\n",
            encoding="utf-8",
        )
        self.fake_ytdlp.chmod(0o755)
        self.acquired = (self.case / "acquired").resolve()
        self.old_order, self.old_result = self.acquire_side(
            "calibration-old-001", "old-work-order.json"
        )
        self.finalized_order, self.finalized_result = self.acquire_side(
            "calibration-finalized-001", "finalized-work-order.json"
        )

    def tearDown(self) -> None:
        make_writable(self.case)

    def acquire_side(self, job_id: str, filename: str) -> tuple[Path, Path]:
        url = "https://www.youtube.com/watch?v=fixtureCal01"
        order = {
            "schema_version": 1,
            "job_id": job_id,
            "adapter": "yt_dlp",
            "source": {
                "platform": "youtube",
                "source_kind": "youtube_video",
                "native_id": "fixtureCal01",
                "canonical_url": url,
                "title": "Calibration fixture",
                "published_at": None,
                "access_state": "public",
            },
            "adapter_config": {
                "url": url,
                "executable": str(self.fake_ytdlp),
                "expected_executable_sha256": digest(self.fake_ytdlp),
                "format_selector": "396+140",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(self.acquired)},
            "limits": {
                "max_job_bytes": 32 * 1024 * 1024,
                "global_cache_cap_bytes": 128 * 1024 * 1024,
                "free_space_floor_bytes": 0,
            },
        }
        order_path = (self.case / filename).resolve()
        order_path.write_text(json.dumps(order, indent=2) + "\n", encoding="utf-8")
        completed = run(
            [
                "python3",
                str(ACQUIRE),
                "run",
                "--work-order",
                str(order_path),
            ],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        return order_path, Path(result["result_path"])

    def produce_arguments(self, output: Path) -> list[str]:
        return [
            "python3",
            str(PROGRAM),
            "produce",
            "--old-work-order",
            str(self.old_order),
            "--old-result",
            str(self.old_result),
            "--finalized-work-order",
            str(self.finalized_order),
            "--finalized-result",
            str(self.finalized_result),
            "--ffmpeg",
            str(Path(shutil.which("ffmpeg") or "").resolve()),
            "--ffprobe",
            str(Path(shutil.which("ffprobe") or "").resolve()),
            "--identity-candidate-id",
            "srtc_fixture_identity_001",
            "--scaled-candidate-id",
            "srtc_fixture_scaled_001",
            "--declared-recording-duration-ms",
            "3000",
            "--checkpoint-ms",
            "0",
            "--checkpoint-ms",
            "2500",
            "--checkpoint-ms",
            "5400",
            "--visual-window-ms",
            "300",
            "--audio-window-ms",
            "500",
            "--identity-audio-search-radius-ms",
            "200",
            "--scaled-audio-search-radius-ms",
            "100",
            "--audio-sample-rate-hz",
            "2000",
            "--minimum-identity-ssim",
            "0.98",
            "--minimum-identity-audio-correlation",
            "0.95",
            "--minimum-visual-margin",
            "0.03",
            "--minimum-audio-margin",
            "0.10",
            "--output",
            str(output),
        ]

    def test_exact_private_receipt_replays_and_satisfies_schema(self) -> None:
        output = (self.case / "private" / "receipt.json").resolve()
        completed = run(self.produce_arguments(output), check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        summary = json.loads(completed.stdout)
        receipt = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(summary["receipt_id"], receipt["receipt_id"])
        self.assertEqual(output.stat().st_mode & 0o777, 0o400)
        self.assertTrue(receipt["conclusions"]["shared_prefix_identity_mapping_supported"])
        self.assertTrue(receipt["conclusions"]["scaled_candidate_contradicted"])
        accounting = receipt["duration_accounting"]
        tail = accounting["uncovered_finalized_video_tail"]
        self.assertGreater(tail["duration_ms"], 0)
        self.assertEqual(
            tail["duration_ms"],
            accounting["finalized_video_duration_ms"]
            - accounting["earlier_video_duration_ms"],
        )
        self.assertFalse(receipt["safety"]["catalog_opened"])
        self.assertIsNone(receipt["conclusions"]["catalog_transform_decision"])
        self.assertIsNone(receipt["conclusions"]["publication_decision"])
        validated = run(
            ["python3", str(PROGRAM), "validate", "--receipt", str(output)],
            check=False,
        )
        self.assertEqual(validated.returncode, 0, validated.stderr)
        self.assertEqual(json.loads(validated.stdout), summary)
        contract = run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(SCHEMA),
                str(output),
            ],
            check=False,
        )
        self.assertEqual(contract.returncode, 0, contract.stdout + contract.stderr)
        threshold_fields = {
            "minimum_identity_ssim": 0.97,
            "minimum_identity_audio_correlation": 0.94,
            "minimum_visual_margin": 0.02,
            "minimum_audio_margin": 0.09,
        }
        for field, value in threshold_fields.items():
            invalid = json.loads(output.read_text(encoding="utf-8"))
            invalid["recipe"]["thresholds"][field] = value
            invalid_path = (self.case / "private" / f"invalid-{field}.json").resolve()
            invalid_path.write_text(
                json.dumps(invalid, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            rejected_contract = run(
                [
                    "python3",
                    str(CONTRACT_VALIDATOR),
                    "--validate",
                    str(SCHEMA),
                    str(invalid_path),
                ],
                check=False,
            )
            self.assertEqual(
                rejected_contract.returncode,
                1,
                rejected_contract.stdout + rejected_contract.stderr,
            )
        open_stream = json.loads(output.read_text(encoding="utf-8"))
        open_stream["acquisitions"]["earlier"]["acquisition_probe"]["streams"][0][
            "secret_like"
        ] = "must-not-pass"
        open_stream_path = (
            self.case / "private" / "invalid-open-probe-stream.json"
        ).resolve()
        open_stream_path.write_text(
            json.dumps(open_stream, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        rejected_stream = run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(SCHEMA),
                str(open_stream_path),
            ],
            check=False,
        )
        self.assertEqual(
            rejected_stream.returncode,
            1,
            rejected_stream.stdout + rejected_stream.stderr,
        )

    def test_receipt_tampering_fails_before_replay_acceptance(self) -> None:
        output = (self.case / "private" / "receipt.json").resolve()
        completed = run(self.produce_arguments(output), check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        output.chmod(0o600)
        receipt = json.loads(output.read_text(encoding="utf-8"))
        receipt["conclusions"]["scaled_contradicting_checkpoint_count"] += 1
        output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        output.chmod(0o400)
        rejected = run(
            ["python3", str(PROGRAM), "validate", "--receipt", str(output)],
            check=False,
        )
        self.assertEqual(rejected.returncode, 2)
        self.assertIn("semantic identity", rejected.stderr)

    def test_nonignored_output_is_rejected_without_writing(self) -> None:
        output = (REPOSITORY_ROOT / "public" / "transform-calibration-test.json").resolve()
        self.assertFalse(output.exists())
        rejected = run(self.produce_arguments(output), check=False)
        self.assertEqual(rejected.returncode, 2)
        self.assertIn("Git-ignored private path", rejected.stderr)
        self.assertFalse(output.exists())

    def test_approved_threshold_floors_cannot_be_weakened(self) -> None:
        cases = {
            "--minimum-identity-ssim": "0.979999",
            "--minimum-identity-audio-correlation": "0.949999",
            "--minimum-visual-margin": "0.029999",
            "--minimum-audio-margin": "0.099999",
        }
        for flag, value in cases.items():
            output = (self.case / "private" / f"{flag[2:]}.json").resolve()
            arguments = self.produce_arguments(output)
            arguments[arguments.index(flag) + 1] = value
            rejected = run(arguments, check=False)
            self.assertEqual(rejected.returncode, 2, rejected.stderr)
            self.assertIn("approved range", rejected.stderr)
            self.assertFalse(output.exists())

        legacy_output = (self.case / "private" / "legacy-weak.json").resolve()
        legacy_arguments = self.produce_arguments(legacy_output)
        legacy_arguments[1] = str(LEGACY_PROGRAM)
        legacy_arguments[
            legacy_arguments.index("--minimum-identity-ssim") + 1
        ] = "0.97"
        legacy_produced = run(legacy_arguments, check=False)
        self.assertEqual(legacy_produced.returncode, 0, legacy_produced.stderr)
        rejected_legacy = run(
            [
                "python3",
                str(PROGRAM),
                "validate",
                "--receipt",
                str(legacy_output),
            ],
            check=False,
        )
        self.assertEqual(rejected_legacy.returncode, 2, rejected_legacy.stderr)
        self.assertIn("approved range", rejected_legacy.stderr)

    def test_symlinked_output_parent_and_destination_are_rejected(self) -> None:
        real_parent = (self.case / "real-private-parent").resolve()
        real_parent.mkdir()
        linked_parent = self.case / "linked-private-parent"
        linked_parent.symlink_to(real_parent, target_is_directory=True)
        linked_output = (linked_parent / "receipt.json").absolute()
        rejected_parent = run(self.produce_arguments(linked_output), check=False)
        self.assertEqual(rejected_parent.returncode, 2, rejected_parent.stderr)
        self.assertIn("symlink components", rejected_parent.stderr)
        self.assertFalse((real_parent / "receipt.json").exists())

        direct_parent = (self.case / "direct-private-parent").resolve()
        direct_parent.mkdir()
        target = (self.case / "symlink-target").resolve()
        target.write_text("unchanged\n", encoding="utf-8")
        linked_destination = direct_parent / "receipt.json"
        linked_destination.symlink_to(target)
        rejected_destination = run(
            self.produce_arguments(linked_destination), check=False
        )
        self.assertEqual(rejected_destination.returncode, 2, rejected_destination.stderr)
        self.assertIn("must not be a symlink", rejected_destination.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "unchanged\n")

    def test_program_has_no_database_or_promotion_interface(self) -> None:
        self.assertEqual(digest(LEGACY_PROGRAM), LEGACY_PROGRAM_SHA256)
        source = PROGRAM.read_text(encoding="utf-8")
        self.assertNotIn("import sqlite3", source)
        help_result = run(["python3", str(PROGRAM), "--help"])
        self.assertNotIn("--db", help_result.stdout)
        self.assertNotIn("promote", help_result.stdout.lower())
        self.assertNotIn("publish", help_result.stdout.lower())


if __name__ == "__main__":
    unittest.main()
