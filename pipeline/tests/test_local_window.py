from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROGRAM = REPOSITORY_ROOT / "pipeline" / "local_window.py"
CONTRACT = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
WORK_SCHEMA = REPOSITORY_ROOT / "pipeline" / "schemas" / "local-window-work-order.schema.json"
BUNDLE_SCHEMA = REPOSITORY_ROOT / "pipeline" / "schemas" / "local-window-bundle-manifest.schema.json"
RESULT_SCHEMA = REPOSITORY_ROOT / "pipeline" / "schemas" / "local-window-result.schema.json"
TEST_ROOT = REPOSITORY_ROOT / "pipeline" / ".test-work"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def command(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(arguments, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)


class LocalWindowTests(unittest.TestCase):
    def setUp(self) -> None:
        if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
            self.skipTest("ffmpeg and ffprobe are required")
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="local-window-", dir=TEST_ROOT)
        self.root = Path(self.temporary.name)
        self.source = self.root / "parent.mp4"
        created = command(
            [
                shutil.which("ffmpeg") or "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
                "-i", "testsrc2=size=320x180:rate=25:duration=4.2", "-f", "lavfi", "-i",
                "sine=frequency=440:sample_rate=48000:duration=4.2", "-c:v", "libx264", "-preset",
                "ultrafast", "-c:a", "aac", "-shortest", str(self.source),
            ]
        )
        self.assertEqual(created.returncode, 0, created.stderr)
        self.ffmpeg = Path(shutil.which("ffmpeg") or "").resolve()
        self.ffprobe = Path(shutil.which("ffprobe") or "").resolve()
        probe = command([str(self.ffprobe), "-v", "error", "-show_entries", "format=duration", "-of", "json", str(self.source)])
        self.duration_ms = round(float(json.loads(probe.stdout)["format"]["duration"]) * 1000)
        media_sha = sha256(self.source)
        self.acquisition = self.root / "acquisition-result.json"
        result = {
            "schema_version": 1, "job_id": "fixture", "adapter": "local_file", "status": "completed",
            "dry_run": False, "reused": False, "work_order_sha256": "a" * 64,
            "started_at": "2026-08-26T00:00:00Z", "completed_at": "2026-08-26T00:00:01Z",
            "duration_ms": 1000,
            "source": {"platform": "youtube", "source_kind": "youtube_video", "native_id": "8AbFGYob9SU", "canonical_url": "https://www.youtube.com/watch?v=8AbFGYob9SU", "title": "fixture", "published_at": None, "access_state": "unknown"},
            "limits": {}, "capacity_before": {}, "capacity_after": {}, "commands": [], "source_observation": {}, "selected_remote_metadata": {},
            "admission": {"media_id": f"media_sha256_{media_sha}", "sha256": media_sha, "byte_count": self.source.stat().st_size, "path": str(self.source), "storage_uri": self.source.as_uri(), "normalized_probe": {"format": {"duration_ms": self.duration_ms}}},
            "catalog_records": {}, "result_path": str(self.acquisition), "errors": [],
        }
        self.acquisition.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.bundles = self.root / "private-bundles"
        self.outputs = self.root / "private-windows"

    def tearDown(self) -> None:
        for child in sorted(self.root.rglob("*"), reverse=True):
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        self.temporary.cleanup()

    def materialize(self, *extra: str) -> subprocess.CompletedProcess[str]:
        return command(
            [
                "python3", str(PROGRAM), "materialize", "--acquisition-result", str(self.acquisition),
                "--bundle-root", str(self.bundles), "--window-output-root", str(self.outputs),
                "--ffmpeg", str(self.ffmpeg), "--ffmpeg-sha256", sha256(self.ffmpeg),
                "--ffprobe", str(self.ffprobe), "--ffprobe-sha256", sha256(self.ffprobe),
                "--chunk-duration-ms", "2000", "--max-windows", "8",
                "--max-window-output-bytes", str(64 * 1024 * 1024), "--free-space-floor-bytes", "0",
                "--timeout-seconds", "120", *extra,
            ]
        )

    def assert_schema(self, schema: Path, instance: Path) -> None:
        checked = command(["python3", str(CONTRACT), "--validate", str(schema), str(instance)])
        self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)

    def test_partition_execute_replay_and_contracts(self) -> None:
        materialized = self.materialize()
        self.assertEqual(materialized.returncode, 0, materialized.stderr)
        replay = self.materialize()
        self.assertEqual(replay.stdout, materialized.stdout)
        manifest = json.loads(materialized.stdout)
        windows = manifest["work_orders"]
        self.assertEqual([(w["start_ms"], w["end_ms"]) for w in windows], [(0, 2000), (2000, 4000), (4000, self.duration_ms)])
        self.assertEqual(windows[-1]["is_partial_tail"], True)
        bundle = self.bundles / manifest["bundle_relative_path"]
        self.assert_schema(BUNDLE_SCHEMA, bundle / "manifest.json")
        order_path = bundle / windows[0]["path"]
        self.assert_schema(WORK_SCHEMA, order_path)

        dry = command(["python3", str(PROGRAM), "run", "--work-order", str(order_path), "--dry-run"])
        self.assertEqual(dry.returncode, 0, dry.stderr)
        self.assertEqual(json.loads(dry.stdout)["time_mapping"]["boundary"], "half_open")
        self.assertFalse(self.outputs.exists())

        completed = command(["python3", str(PROGRAM), "run", "--work-order", str(order_path)])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        result_path = Path(result["result_path"])
        self.assert_schema(RESULT_SCHEMA, result_path)
        again = command(["python3", str(PROGRAM), "run", "--work-order", str(order_path)])
        self.assertEqual(again.stdout, completed.stdout)
        self.assertEqual({a["artifact_kind"] for a in result["artifacts"]}, {"window_audio_16khz_mono_flac", "window_low_resolution_cfr_proxy"})
        self.assertFalse(result["time_mapping"]["byte_exact_source_fragment"])

    def test_hash_tamper_and_window_count_fail_closed(self) -> None:
        too_many = self.materialize("--max-windows", "2")
        self.assertEqual(too_many.returncode, 2)
        self.assertIn("exceeds", json.loads(too_many.stderr)["error"]["message"])

        materialized = self.materialize()
        manifest = json.loads(materialized.stdout)
        order = self.bundles / manifest["bundle_relative_path"] / "work-orders/000001.json"
        self.source.write_bytes(self.source.read_bytes() + b"tamper")
        refused = command(["python3", str(PROGRAM), "run", "--work-order", str(order)])
        self.assertEqual(refused.returncode, 2)
        self.assertIn("SHA-256", json.loads(refused.stderr)["error"]["message"])
        self.assertFalse(self.outputs.exists())

    def test_shape_valid_but_semantically_tampered_replay_is_rejected(self) -> None:
        manifest = json.loads(self.materialize().stdout)
        order = self.bundles / manifest["bundle_relative_path"] / "work-orders/000001.json"
        completed = command(["python3", str(PROGRAM), "run", "--work-order", str(order)])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result_path = Path(json.loads(completed.stdout)["result_path"])
        value = json.loads(result_path.read_text())
        value["job_id"] = "wrong-but-shape-valid"
        result_path.chmod(0o600)
        result_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        result_path.chmod(0o400)
        refused = command(["python3", str(PROGRAM), "run", "--work-order", str(order)])
        self.assertEqual(refused.returncode, 2)
        self.assertIn("job_id", json.loads(refused.stderr)["error"]["message"])

    def test_final_output_burst_cannot_escape_window_byte_cap(self) -> None:
        materialized = self.materialize("--max-window-output-bytes", "14500")
        self.assertEqual(materialized.returncode, 0, materialized.stderr)
        manifest = json.loads(materialized.stdout)
        order = (
            self.bundles
            / manifest["bundle_relative_path"]
            / "work-orders/000001.json"
        )
        refused = command(["python3", str(PROGRAM), "run", "--work-order", str(order)])
        self.assertEqual(refused.returncode, 2)
        self.assertIn(
            "max_window_output_bytes",
            json.loads(refused.stderr)["error"]["message"],
        )
        expected_final = (
            self.outputs
            / "windows"
            / sha256(self.source)[:2]
            / sha256(self.source)
            / manifest["bundle_id"]
            / "window_000001"
        )
        self.assertFalse(expected_final.exists())


if __name__ == "__main__":
    unittest.main()
