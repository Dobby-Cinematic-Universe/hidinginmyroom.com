from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "visual_fingerprint.py"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / "visual-fingerprint"


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


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def make_writable(root: Path) -> None:
    if not root.exists():
        return
    for path in [root, *root.rglob("*")]:
        try:
            path.chmod(path.stat().st_mode | 0o700)
        except OSError:
            pass


def validate_schema(instance: object, name: str) -> None:
    try:
        import jsonschema
    except ImportError:
        return
    schema = json.loads((PIPELINE_ROOT / "schemas" / name).read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(instance, schema)


@unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg is required")
class VisualFingerprintTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        inspected = run(["python3", str(PROGRAM), "inspect-engine"])
        cls.engine_pin = json.loads(inspected.stdout)

    @classmethod
    def tearDownClass(cls) -> None:
        make_writable(TEST_ROOT)
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, exist_ok=True)
        ffmpeg = self.engine_pin["executable"]
        self.original = (self.case / "original.mkv").resolve()
        self.transcoded = (self.case / "transcoded.mp4").resolve()
        self.other = (self.case / "other.mkv").resolve()
        run(
            [
                ffmpeg,
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=160x90:rate=10:duration=3",
                "-c:v",
                "ffv1",
                str(self.original),
            ]
        )
        run(
            [
                ffmpeg,
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-i",
                str(self.original),
                "-c:v",
                "mpeg4",
                "-q:v",
                "8",
                str(self.transcoded),
            ]
        )
        run(
            [
                ffmpeg,
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "smptebars=size=160x90:rate=10:duration=3",
                "-c:v",
                "ffv1",
                str(self.other),
            ]
        )
        for path in (self.original, self.transcoded, self.other):
            path.chmod(0o444)

    def tearDown(self) -> None:
        make_writable(self.case)

    def order(
        self,
        media: Path | None = None,
        *,
        output_name: str = "output",
        samples: list[dict] | None = None,
    ) -> dict:
        media = media or self.original
        media_sha = digest(media)
        return {
            "schema_version": 1,
            "job_id": f"visual-{output_name}",
            "input": {
                "path": str(media),
                "expected_sha256": media_sha,
                "expected_byte_count": media.stat().st_size,
                "media_id": f"media_sha256_{media_sha}",
                "artifact_id": f"artifact_{output_name}",
                "parent_processing_run_id": f"run_preprocess_{output_name}",
                "duration_ms": 3000,
                "timeline_origin_ms": 0,
                "video_stream_selector": "0:v:0",
                "sealed": True,
            },
            "engine": self.engine_pin,
            "extraction": {
                "algorithm": "fixed_q20_dct_phash_8x8_v1",
                "pixel_format": "gray",
                "width": 32,
                "height": 32,
                "scale_flags": "bilinear",
                "threads": 1,
                "timeout_seconds_per_frame": 30,
                "max_timestamp_drift_ms": 100,
                "samples": samples
                or [
                    {
                        "sample_id": "sample-0500",
                        "window_id": "window-0000-1000",
                        "start_ms": 0,
                        "end_ms": 1000,
                        "requested_timestamp_ms": 500,
                        "timestamp_kind": "explicit",
                    }
                ],
            },
            "catalog_context": None,
            "output": {"root": str((self.case / output_name).resolve())},
        }

    def execute(self, order: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        path = self.case / f"order-{len(list(self.case.glob('order-*.json')))}.json"
        write_json(path, order)
        return run(
            ["python3", str(PROGRAM), "run", "--work-order", str(path), *arguments],
            check=False,
        )

    def extract(self, order: dict) -> dict:
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def test_exact_replay_is_byte_stable_sealed_and_schema_valid(self) -> None:
        order = self.order()
        first = self.extract(order)
        validate_schema(first, "visual-fingerprint-result.schema.json")
        result_path = Path(first["result_path"])
        first_bytes = result_path.read_bytes()
        second = self.extract(order)
        self.assertEqual(first, second)
        self.assertEqual(result_path.read_bytes(), first_bytes)
        frame = first["frames"][0]
        self.assertRegex(frame["phash_hex"], r"^[0-9a-f]{16}$")
        self.assertEqual(frame["algorithm"], "fixed_q20_dct_phash_8x8_v1")
        self.assertEqual(frame["artifact"]["byte_count"], 1024)
        self.assertEqual(Path(frame["artifact"]["path"]).stat().st_mode & 0o222, 0)
        self.assertEqual(result_path.stat().st_mode & 0o222, 0)

    def test_dry_run_writes_nothing(self) -> None:
        order = self.order(output_name="dry")
        completed = self.execute(order, "--dry-run")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "visual-fingerprint-result.schema.json")
        self.assertEqual(result["status"], "planned")
        self.assertFalse(Path(order["output"]["root"]).exists())

    def compare_order(
        self,
        query: dict,
        candidate: dict,
        *,
        maximum_hamming: int,
        name: str,
    ) -> dict:
        return {
            "schema_version": 1,
            "job_id": f"compare-{name}",
            "method": "minimum_pairwise_phash_hamming_v1",
            "query": {
                "role": "query",
                "result_path": query["result_path"],
                "expected_sha256": digest(Path(query["result_path"])),
                "frame_ids": [query["frames"][0]["fingerprint_id"]],
            },
            "candidate": {
                "role": "candidate",
                "result_path": candidate["result_path"],
                "expected_sha256": digest(Path(candidate["result_path"])),
                "frame_ids": [candidate["frames"][0]["fingerprint_id"]],
            },
            "threshold": {
                "maximum_hamming_distance": maximum_hamming,
                "top_k": 10,
                "max_pairwise_comparisons": 16,
            },
            "catalog_context": None,
            "output": {"root": str((self.case / f"comparisons-{name}").resolve())},
        }

    def execute_compare(self, order: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        path = self.case / f"compare-{len(list(self.case.glob('compare-*.json')))}.json"
        write_json(path, order)
        return run(
            ["python3", str(PROGRAM), "compare", "--work-order", str(path), *arguments],
            check=False,
        )

    def test_transcode_tolerance_routes_candidate_without_relationship_claim(self) -> None:
        query = self.extract(self.order(self.original, output_name="query"))
        candidate = self.extract(self.order(self.transcoded, output_name="transcoded"))
        order = self.compare_order(query, candidate, maximum_hamming=8, name="transcode")
        planned = self.execute_compare(order, "--dry-run")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        validate_schema(json.loads(planned.stdout), "visual-fingerprint-compare-result.schema.json")
        completed = self.execute_compare(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "visual-fingerprint-compare-result.schema.json")
        evidence = result["comparison"]
        self.assertLessEqual(evidence["best_hamming_distance"], 8)
        self.assertTrue(evidence["candidate_emitted"])
        self.assertEqual(evidence["calibration_state"], "not_calibrated")
        self.assertIsNone(evidence["calibrated_probability"])
        self.assertTrue(evidence["requires_human_review"])
        self.assertTrue(all(value is False for value in evidence["assertions"].values()))

        # A shape-valid edit to a completed comparison is not accepted as new
        # evidence: replay recomputes the complete comparison from the sealed sides.
        result_path = Path(result["result_path"])
        result_path.chmod(0o644)
        altered = json.loads(result_path.read_text(encoding="utf-8"))
        altered["comparison"]["warning"] = "shape-valid but forged"
        write_json(result_path, altered)
        result_path.chmod(0o444)
        tampered = self.execute_compare(order)
        self.assertNotEqual(tampered.returncode, 0)
        self.assertIn("evidence", tampered.stderr)

    def test_below_threshold_is_not_an_unrelated_claim(self) -> None:
        query = self.extract(self.order(self.original, output_name="query-other"))
        candidate = self.extract(self.order(self.other, output_name="other"))
        order = self.compare_order(query, candidate, maximum_hamming=0, name="other")
        completed = self.execute_compare(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        evidence = json.loads(completed.stdout)["comparison"]
        self.assertGreater(evidence["best_hamming_distance"], 0)
        self.assertFalse(evidence["candidate_emitted"])
        self.assertEqual(evidence["threshold_state"], "does_not_meet_configured_threshold")
        self.assertFalse(evidence["assertions"]["unrelated"])

    def test_tamper_symlink_caps_and_half_open_time_fail_closed(self) -> None:
        result = self.extract(self.order(output_name="tamper"))
        artifact = Path(result["frames"][0]["artifact"]["path"])
        artifact.chmod(0o644)
        body = bytearray(artifact.read_bytes())
        body[0] ^= 1
        artifact.write_bytes(body)
        artifact.chmod(0o444)
        replay = self.execute(self.order(output_name="tamper"))
        self.assertNotEqual(replay.returncode, 0)
        self.assertIn("digest", replay.stderr)

        alias = self.case / "alias.mkv"
        alias.symlink_to(self.original)
        symlink_order = self.order(output_name="symlink")
        symlink_order["input"]["path"] = str(alias)
        rejected = self.execute(symlink_order)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("symlink", rejected.stderr)

        cap_order = self.order(output_name="cap")
        cap_order["extraction"]["samples"] = [
            {
                "sample_id": f"sample-{index}",
                "window_id": "window-cap",
                "start_ms": 0,
                "end_ms": 3000,
                "requested_timestamp_ms": index % 3000,
                "timestamp_kind": "explicit",
            }
            for index in range(4097)
        ]
        capped = self.execute(cap_order)
        self.assertNotEqual(capped.returncode, 0)
        self.assertIn("1..4096", capped.stderr)

        window_order = self.order(
            output_name="window",
            samples=[
                {
                    "sample_id": "narrow",
                    "window_id": "narrow-window",
                    "start_ms": 550,
                    "end_ms": 551,
                    "requested_timestamp_ms": 550,
                    "timestamp_kind": "explicit",
                }
            ],
        )
        outside = self.execute(window_order)
        self.assertNotEqual(outside.returncode, 0)
        self.assertIn("half-open window", outside.stderr)

        unknown = self.order(output_name="unknown")
        unknown["extraction"]["samples"][0]["identity"] = "forbidden"
        rejected_unknown = self.execute(unknown)
        self.assertNotEqual(rejected_unknown.returncode, 0)
        self.assertIn("unknown", rejected_unknown.stderr)


if __name__ == "__main__":
    unittest.main()
