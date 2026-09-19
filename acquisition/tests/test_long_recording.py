from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from acquisition.tests.test_materialize_queue import fixture_plan


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "acquisition"))
import materialize_long_recording as long_recording  # noqa: E402

PROGRAM = REPOSITORY_ROOT / "acquisition" / "materialize_long_recording.py"
SCHEMA = REPOSITORY_ROOT / "acquisition" / "schemas" / "long-recording-bundle-manifest.schema.json"
CONTRACT = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
TEST_ROOT = REPOSITORY_ROOT / "acquisition" / ".test-work"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class LongRecordingMaterializerTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="long-recording-", dir=TEST_ROOT)
        self.root = Path(self.temporary.name)
        self.plan = self.root / "plan.json"
        self.bundles = self.root / "bundles-private"
        self.media = self.root / "media-private"
        self.ytdlp = self.root / "yt-dlp"
        self.marker = self.root / "must-not-run"
        self.ytdlp.write_text(f"#!/bin/sh\ntouch {self.marker!s}\nexit 99\n", encoding="utf-8")
        self.ytdlp.chmod(0o755)

    def tearDown(self) -> None:
        for child in sorted(self.root.rglob("*"), reverse=True):
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        self.temporary.cleanup()

    def youtube_plan(self) -> dict:
        plan = fixture_plan()
        candidate = plan["candidates"][2]
        candidate.update(
            {
                "platform": "youtube",
                "source_kind": "youtube_video",
                "native_id": "8AbFGYob9SU",
                "canonical_url": "https://www.youtube.com/watch?v=8AbFGYob9SU",
                "adapter": "yt_dlp",
                "expected_byte_count": None,
                "expected_sha256": None,
                "source_class": None,
                "mapping_roles": ["current_platform_listing"],
                "priority": 30,
                "priority_tier": "current_public_upload",
                "reason_codes": ["current_platform_listing", "exceeds_single_job_policy"],
                "estimated_bytes": 8_000_000 * 500_000 // 1000 + 64 * 1024**2,
                "estimate_basis": "duration_conservative_rate",
            }
        )
        # Reuse the fixture helpers to restore summary and plan identity.
        from acquisition.tests.test_materialize_queue import select_and_summarize

        plan["candidates"].sort(
            key=lambda item: (
                item["priority"],
                item["duration_ms"] if item["duration_ms"] is not None else 2**63,
                item["recording_id"],
                item["source_id"],
            )
        )
        return select_and_summarize(plan)

    def execute(self, *extra: str) -> subprocess.CompletedProcess[str]:
        arguments = [
            "python3", str(PROGRAM), "--plan", str(self.plan), "--candidate-id", "8AbFGYob9SU",
            "--bundle-root", str(self.bundles), "--media-output-root", str(self.media),
            "--yt-dlp-executable", str(self.ytdlp), "--yt-dlp-sha256", digest(self.ytdlp),
            "--full-source-max-bytes", "6000000000", "--global-cache-cap-bytes", "12000000000",
            "--free-space-floor-bytes", "0", *extra,
        ]
        return subprocess.run(arguments, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)

    def test_explicit_long_source_bundle_is_offline_immutable_and_schema_valid(self) -> None:
        self.plan.write_text(json.dumps(self.youtube_plan()), encoding="utf-8")
        first = self.execute()
        self.assertEqual(first.returncode, 0, first.stderr)
        second = self.execute()
        self.assertEqual(second.stdout, first.stdout)
        manifest = json.loads(first.stdout)
        self.assertFalse(self.marker.exists())
        self.assertEqual(manifest["candidate"]["queue_state"], "requires_chunking")
        self.assertFalse(manifest["safety"]["normal_queue_semantics_changed"])
        self.assertFalse(manifest["safety"]["remote_time_sections_allowed"])
        self.assertEqual(
            manifest["safety"]["access_challenge_policy"],
            "retryable_without_credentials_or_admission",
        )
        bundle = self.bundles / manifest["bundle_relative_path"]
        completed = subprocess.run(
            ["python3", str(CONTRACT), "--validate", str(SCHEMA), str(bundle / "manifest.json")],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        order = json.loads((bundle / "work-orders/000001.json").read_text())
        self.assertEqual(order["limits"]["max_job_bytes"], 6_000_000_000)
        self.assertNotIn("cookie", json.dumps(order).lower())

    def test_underestimated_cap_and_non_chunk_candidate_fail_without_bundle(self) -> None:
        plan = self.youtube_plan()
        self.plan.write_text(json.dumps(plan), encoding="utf-8")
        rejected = self.execute("--full-source-max-bytes", "4000000000")
        self.assertEqual(rejected.returncode, 2)
        self.assertIn("below", json.loads(rejected.stderr)["error"]["message"])

        candidate = next(item for item in plan["candidates"] if item["native_id"] == "8AbFGYob9SU")
        candidate["queue_state"] = "ready"
        candidate["defer_reason"] = None
        candidate["queue_ordinal"] = 3
        from acquisition.tests.test_materialize_queue import select_and_summarize

        self.plan.write_text(json.dumps(select_and_summarize(plan)), encoding="utf-8")
        wrong_queue = self.execute()
        self.assertEqual(wrong_queue.returncode, 2)
        self.assertIn("inconsistent", json.loads(wrong_queue.stderr)["error"]["message"])

    def test_long_parent_can_target_only_a_dedicated_cold_descendant(self) -> None:
        cold_cas = (
            long_recording.COLD_ARCHIVE_ROOT
            / "corpus"
            / "raw"
            / "long-recording-parents"
        )
        self.assertEqual(
            "sealed_work_order_media_output_root_write_only",
            long_recording.storage_access_policy(cold_cas),
        )
        self.assertEqual(
            "forbidden", long_recording.storage_access_policy(self.media)
        )
        with self.assertRaisesRegex(
            long_recording.LongRecordingError, "dedicated descendant"
        ):
            long_recording.storage_access_policy(
                long_recording.COLD_ARCHIVE_ROOT
            )


if __name__ == "__main__":
    unittest.main()
