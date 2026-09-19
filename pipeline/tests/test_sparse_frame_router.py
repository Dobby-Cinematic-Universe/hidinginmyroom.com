from __future__ import annotations

import hashlib
import fcntl
import json
import os
import shutil
import subprocess
import unittest
from fractions import Fraction
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "sparse_frame_router.py"
WORK_ORDER_SCHEMA = PIPELINE_ROOT / "schemas" / "sparse-frame-work-order.schema.json"
RESULT_SCHEMA = PIPELINE_ROOT / "schemas" / "sparse-frame-result.schema.json"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
# Agents and CI shards may run the suite concurrently in one checkout.  A
# process-scoped root prevents one run's tamper test from corrupting another.
TEST_ROOT = PIPELINE_ROOT / f".test-sparse-frames-{os.getpid()}"


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


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def stable_preprocess_artifact_id(run_id: str, sha256: str) -> str:
    identity = {
        "processing_run_id": run_id,
        "kind": "low_resolution_cfr_proxy",
        "sha256": sha256,
    }
    return "artifact_" + hashlib.sha256(canonical_bytes(identity)).hexdigest()[:32]


def ffmpeg_identity() -> dict[str, str]:
    path = Path(shutil.which("ffmpeg") or "").resolve(strict=True)
    version = run([str(path), "-version"]).stdout.strip()
    return {
        "path": str(path),
        "expected_sha256": digest(path),
        "expected_version_output_sha256": hashlib.sha256(version.encode()).hexdigest(),
    }


def remove_test_tree(path: Path) -> None:
    """Remove test-only sealed trees without hiding cleanup failures."""

    if not path.exists():
        return
    for child in sorted(path.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        try:
            child.chmod(0o755 if child.is_dir() else 0o644)
        except FileNotFoundError:
            pass
    path.chmod(0o755)
    shutil.rmtree(path)


def create_preprocess_fixture(
    root: Path,
    *,
    scene_timestamps_ms: list[int] | None = None,
    duration_seconds: int = 6,
) -> Path:
    run_id = "run_preprocess_0123456789abcdef0123456789abcdef"
    run_dir = root / "preprocess" / "executions" / run_id
    proxy = run_dir / "artifacts" / "proxy-320x180-25fps.mp4"
    proxy.parent.mkdir(parents=True)
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
            f"color=c=black:s=320x180:r=25:d={duration_seconds}",
            "-an",
            "-map_metadata",
            "-1",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            "-fps_mode",
            "cfr",
            str(proxy),
        ]
    )
    proxy.chmod(0o444)
    proxy_sha256 = digest(proxy)
    artifact_id = stable_preprocess_artifact_id(run_id, proxy_sha256)
    duration_ms = duration_seconds * 1000
    normalized_probe = {
        "schema_version": 1,
        "media": {
            "media_id": f"media_sha256_{proxy_sha256}",
            "sha256": proxy_sha256,
            "byte_count": proxy.stat().st_size,
            "basename": proxy.name,
        },
        "tool": {"name": "ffprobe", "version": "fixture"},
        "format": {
            "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
            "format_long_name": "QuickTime / MOV",
            "duration_ms": duration_ms,
            "start_ms": 0,
            "bit_rate_bps": None,
            "probe_score": 100,
            "tags": {},
        },
        "primary_streams": {"video_index": 0, "audio_index": None},
        "streams": [
            {
                "index": 0,
                "codec_type": "video",
                "codec_name": "h264",
                "codec_long_name": "H.264",
                "profile": "Constrained Baseline",
                "duration_ms": duration_ms,
                "start_ms": 0,
                "bit_rate_bps": None,
                "time_base": "1/12800",
                "disposition": {"default": 1, "forced": 0, "attached_pic": 0},
                "tags": {},
                "side_data": [],
                "video": {
                    "width": 320,
                    "height": 180,
                    "pixel_format": "yuv420p",
                    "sample_aspect_ratio": "1:1",
                    "display_aspect_ratio": "16:9",
                    "average_frame_rate": {
                        "text": "25/1",
                        "numerator": 25,
                        "denominator": 1,
                        "decimal": 25.0,
                    },
                    "reported_frame_rate": {
                        "text": "25/1",
                        "numerator": 25,
                        "denominator": 1,
                        "decimal": 25.0,
                    },
                    "frame_count": duration_seconds * 25,
                },
            }
        ],
        "chapters": [],
    }
    scenes = [
        {"timestamp_ms": value, "score_percent": 25.0}
        for value in (scene_timestamps_ms or [])
    ]
    result_path = run_dir / "result.json"
    result = {
        "schema_version": 1,
        "job_id": "preprocess-fixture",
        "status": "completed",
        "dry_run": False,
        "duration_ms": 1,
        "processing_run": {
            "processing_run_id": run_id,
            "stage": "media_preprocess",
            "implementation_version": "0.3.1",
            "parameters_json": {},
            "environment_json": {
                "tool_paths": {"ffmpeg": ffmpeg_identity()["path"]}
            },
            "started_at": "2026-08-26T00:00:00Z",
            "completed_at": "2026-08-26T00:00:01Z",
            "status": "completed",
        },
        "input": {},
        "layout": {"run_dir": str(run_dir)},
        "steps": [],
        "artifacts": [
            {
                "artifact_id": artifact_id,
                "processing_run_id": run_id,
                "artifact_kind": "low_resolution_cfr_proxy",
                "storage_uri": proxy.as_uri(),
                "path": str(proxy),
                "sha256": proxy_sha256,
                "byte_count": proxy.stat().st_size,
                "schema_version": 1,
                "visibility": "private",
                "media_kind": "video",
                "mime_type": "video/mp4",
                "normalized_probe": normalized_probe,
            }
        ],
        "routing": {
            "schema_version": 1,
            "coverage": {
                "duration_ms": duration_ms,
                "has_video": True,
                "has_audio": False,
            },
            "scene_changes": scenes,
        },
        "reuse": {},
        "catalog_records": {},
        "errors": [],
        "result_path": str(result_path),
    }
    result_path.write_text(
        json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    result_path.chmod(0o444)
    return result_path


def work_order(preprocess_result: Path, output_root: Path) -> dict:
    return {
        "schema_version": 1,
        "job_id": "sparse-frame-fixture-001",
        "preprocess_result": {
            "path": str(preprocess_result),
            "expected_sha256": digest(preprocess_result),
        },
        "ffmpeg": ffmpeg_identity(),
        "sampling": {
            "include_recording_start": True,
            "scene_changes": {"enabled": True, "max_frames": 3, "offset_ms": 0},
            "periodic": {"enabled": True, "interval_ms": 2000, "max_frames": 2},
            "min_separation_ms": 100,
        },
        "limits": {
            "max_frames": 6,
            "max_media_duration_ms": 60_000,
            "max_input_pixels": 320 * 180,
            "max_frame_bytes": 2 * 1024 * 1024,
            "max_timestamp_drift_ms": 1000,
            "timeout_seconds_per_frame": 30,
        },
        "output": {"root": str(output_root)},
    }


class SparseFrameRouterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not shutil.which("ffmpeg"):
            raise unittest.SkipTest("ffmpeg is required")
        remove_test_tree(TEST_ROOT)
        TEST_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        remove_test_tree(TEST_ROOT)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, exist_ok=True)

    def write_work_order(self, value: dict, name: str = "work-order.json") -> Path:
        path = self.case / name
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return path

    def execute(self, value: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        path = self.write_work_order(value)
        return run(
            [
                "python3",
                str(PROGRAM),
                "run",
                "--work-order",
                str(path),
                *arguments,
            ],
            check=False,
        )

    def assert_contract(self, schema: Path, value: dict, name: str) -> None:
        instance = self.case / name
        instance.write_text(json.dumps(value), encoding="utf-8")
        completed = run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(schema),
                str(instance),
            ],
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            f"Generated sparse-frame value failed contract:\n{completed.stdout}{completed.stderr}",
        )

    def test_static_video_uses_start_and_periodic_fallback_with_exact_pts(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        output_root = self.case / "derived"
        value = work_order(preprocess, output_root)
        self.assert_contract(WORK_ORDER_SCHEMA, value, "work-order.contract.json")

        dry = self.execute(value, "--dry-run")
        self.assertEqual(dry.returncode, 0, dry.stderr)
        plan = json.loads(dry.stdout)
        self.assert_contract(RESULT_SCHEMA, plan, "planned-result.contract.json")
        self.assertFalse(output_root.exists())
        self.assertEqual(
            [frame["requested_timestamp_ms"] for frame in plan["selection"]["planned_frames"]],
            [0, 2000, 4000],
        )
        self.assertEqual(plan["selection"]["candidate_counts"]["preprocess_scene_changes"], 0)

        completed = self.execute(value)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_contract(RESULT_SCHEMA, result, "completed-result.contract.json")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(result["frames"]), 3)
        for frame, artifact in zip(result["frames"], result["artifacts"], strict=True):
            timestamp = frame["timestamp"]
            exact_ms = round(
                Fraction(
                    timestamp["pts"] * timestamp["time_base_numerator"] * 1000,
                    timestamp["time_base_denominator"],
                )
            )
            self.assertEqual(timestamp["timestamp_ms"], exact_ms)
            self.assertLessEqual(frame["timestamp_drift_ms"], 1000)
            self.assertEqual(frame["ocr_routing"]["evaluation_state"], "not_evaluated")
            self.assertEqual(frame["ocr_routing"]["text_presence"], "unknown")
            self.assertNotIn("text", frame["ocr_routing"])
            self.assertNotIn("identity", frame)
            path = Path(artifact["path"])
            self.assertEqual(path.stat().st_mode & 0o222, 0)
            self.assertEqual(digest(path), artifact["sha256"])
        self.assertEqual(Path(result["result_path"]).stat().st_mode & 0o222, 0)

    def test_generator_pins_preprocess_and_ffmpeg_identities(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        output_root = self.case / "derived"
        generated = run(
            [
                "python3",
                str(PROGRAM),
                "create-work-order",
                "--job-id",
                "generated-sparse-001",
                "--preprocess-result",
                str(preprocess),
                "--output-root",
                str(output_root),
            ],
            check=False,
        )
        self.assertEqual(generated.returncode, 0, generated.stderr)
        value = json.loads(generated.stdout)
        self.assert_contract(WORK_ORDER_SCHEMA, value, "generated-work-order.contract.json")
        self.assertEqual(value["preprocess_result"]["expected_sha256"], digest(preprocess))
        self.assertEqual(value["ffmpeg"]["expected_sha256"], digest(Path(value["ffmpeg"]["path"])))
        self.assertFalse(output_root.exists())

    def test_scene_and_nearby_periodic_reasons_merge_without_dense_sampling(self) -> None:
        preprocess = create_preprocess_fixture(
            self.case, scene_timestamps_ms=[1950, 3500]
        )
        completed = self.execute(work_order(preprocess, self.case / "derived"), "--dry-run")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        planned = result["selection"]["planned_frames"]
        self.assertEqual([row["requested_timestamp_ms"] for row in planned], [0, 1950, 3500, 4000])
        merged = next(row for row in planned if row["requested_timestamp_ms"] == 1950)
        self.assertEqual(
            merged["selection_reason_codes"],
            ["FRAME_SCENE_CHANGE", "FRAME_PERIODIC_COVERAGE"],
        )
        self.assertIn("NEARBY_CANDIDATES_MERGED", result["selection"]["limit_reason_codes"])
        self.assertEqual(result["selection"]["candidate_counts"]["planned_frames"], 4)

    def test_scene_candidates_are_uniformly_capped_and_total_is_bounded(self) -> None:
        preprocess = create_preprocess_fixture(
            self.case, scene_timestamps_ms=list(range(100, 5900, 50))
        )
        value = work_order(preprocess, self.case / "derived")
        value["sampling"]["min_separation_ms"] = 0
        completed = self.execute(value, "--dry-run")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        selection = json.loads(completed.stdout)["selection"]
        self.assertEqual(selection["candidate_counts"]["retained_scene_candidates"], 3)
        self.assertLessEqual(selection["candidate_counts"]["planned_frames"], 6)
        self.assertIn("SCENE_CANDIDATES_UNIFORMLY_CAPPED", selection["limit_reason_codes"])

    def test_repeated_execution_returns_identical_sealed_result(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        value = work_order(preprocess, self.case / "derived")
        first = self.execute(value)
        self.assertEqual(first.returncode, 0, first.stderr)
        first_result = json.loads(first.stdout)
        result_path = Path(first_result["result_path"])
        before_stat = result_path.stat()
        before_digest = digest(result_path)
        second = self.execute(value)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(before_digest, digest(result_path))
        self.assertEqual(before_stat.st_mtime_ns, result_path.stat().st_mtime_ns)

    def test_result_lock_rejects_concurrent_writer_without_partial_output(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        value = work_order(preprocess, self.case / "derived")
        planned = self.execute(value, "--dry-run")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        plan = json.loads(planned.stdout)
        run_dir = Path(plan["result_path"]).parent
        run_dir.parent.mkdir(parents=True)
        lock_path = run_dir.parent / f".{plan['result_key']}.lock"
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            blocked = self.execute(value)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("holds the result lock", json.loads(blocked.stderr)["error"]["message"])
        self.assertFalse(run_dir.exists())

    def test_bitexact_png_hashes_are_stable_across_distinct_result_keys(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        first_value = work_order(preprocess, self.case / "derived")
        first = self.execute(first_value)
        self.assertEqual(first.returncode, 0, first.stderr)
        second_value = json.loads(json.dumps(first_value))
        second_value["job_id"] = "sparse-frame-fixture-002"
        second = self.execute(second_value)
        self.assertEqual(second.returncode, 0, second.stderr)
        first_hashes = [row["sha256"] for row in json.loads(first.stdout)["artifacts"]]
        second_hashes = [row["sha256"] for row in json.loads(second.stdout)["artifacts"]]
        self.assertEqual(first_hashes, second_hashes)

    def test_tampered_completed_frame_fails_closed_instead_of_reextracting(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        value = work_order(preprocess, self.case / "derived")
        first = self.execute(value)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        frame_path = Path(result["artifacts"][0]["path"])
        frame_path.chmod(0o644)
        with frame_path.open("r+b") as handle:
            handle.seek(-1, os.SEEK_END)
            original = handle.read(1)
            handle.seek(-1, os.SEEK_END)
            handle.write(bytes([original[0] ^ 1]))
        frame_path.chmod(0o444)
        blocked = self.execute(value)
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("artifact metadata/hash", json.loads(blocked.stderr)["error"]["message"])
        self.assertEqual(len(list((self.case / "derived").rglob("result.json"))), 1)

    def test_tampered_envelope_cannot_add_an_ocr_text_claim(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        value = work_order(preprocess, self.case / "derived")
        first = self.execute(value)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        result_path = Path(result["result_path"])
        result["frames"][0]["ocr_text"] = "fabricated"
        result_path.parent.chmod(0o755)
        result_path.chmod(0o644)
        result_path.write_text(
            json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        result_path.chmod(0o444)
        result_path.parent.chmod(0o555)

        blocked = self.execute(value)
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("unsupported keys: ocr_text", json.loads(blocked.stderr)["error"]["message"])

    def test_tampered_preprocess_proxy_is_rejected_before_output(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        raw = json.loads(preprocess.read_text(encoding="utf-8"))
        proxy = Path(raw["artifacts"][0]["path"])
        proxy.chmod(0o644)
        with proxy.open("r+b") as handle:
            handle.seek(-1, os.SEEK_END)
            original = handle.read(1)
            handle.seek(-1, os.SEEK_END)
            handle.write(bytes([original[0] ^ 1]))
        proxy.chmod(0o444)
        output_root = self.case / "derived"
        blocked = self.execute(work_order(preprocess, output_root))
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("preprocess proxy SHA-256 mismatch", json.loads(blocked.stderr)["error"]["message"])
        self.assertFalse(output_root.exists())

    def test_tampered_preprocess_envelope_hash_is_rejected_before_output(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        value = work_order(preprocess, self.case / "derived")
        preprocess.chmod(0o644)
        preprocess.write_text(preprocess.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        preprocess.chmod(0o444)
        blocked = self.execute(value)
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("media-preprocess result SHA-256 mismatch", json.loads(blocked.stderr)["error"]["message"])
        self.assertFalse((self.case / "derived").exists())

    def test_adversarial_work_orders_fail_before_creating_output(self) -> None:
        preprocess = create_preprocess_fixture(self.case)
        output_root = self.case / "derived"
        extra = work_order(preprocess, output_root)
        extra["unexpected"] = True
        blocked = self.execute(extra, "--dry-run")
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("unsupported keys", json.loads(blocked.stderr)["error"]["message"])

        underfunded = work_order(preprocess, output_root)
        underfunded["limits"]["max_frames"] = 5
        blocked = self.execute(underfunded, "--dry-run")
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("must cover recording start", json.loads(blocked.stderr)["error"]["message"])

        url = work_order(preprocess, output_root)
        url["preprocess_result"]["path"] = "https://example.invalid/result.json"
        blocked = self.execute(url, "--dry-run")
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("absolute local file path", json.loads(blocked.stderr)["error"]["message"])
        self.assertFalse(output_root.exists())


if __name__ == "__main__":
    unittest.main()
