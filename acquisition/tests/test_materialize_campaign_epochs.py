from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from acquisition.tests.test_materialize_queue import (
    fixture_plan,
    select_and_summarize,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
PROGRAM = ACQUISITION_ROOT / "materialize_campaign_epochs.py"
TEST_PARENT = ACQUISITION_ROOT / ".test-work"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
CAMPAIGN_SCHEMA = ACQUISITION_ROOT / "schemas" / "campaign-epoch-manifest.schema.json"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def archive_plan(sizes: list[int], *, explicit: bool = True) -> dict:
    base = fixture_plan()
    archive = copy.deepcopy(base["candidates"][1])
    candidates = []
    for index, size in enumerate(sizes, 1):
        candidate = copy.deepcopy(archive)
        candidate["recording_id"] = f"rec_archive_{index:03d}"
        candidate["source_id"] = f"src_archive_{index:03d}"
        candidate["native_id"] = f"item-one/movie-{index:03d}.mp4"
        candidate["canonical_url"] = (
            f"https://archive.org/download/item-one/movie-{index:03d}.mp4"
        )
        candidate["title"] = f"Public Archive fixture {index}"
        candidate["duration_ms"] = 90_000 + index
        candidate["estimated_bytes"] = size
        candidate["expected_byte_count"] = size
        if explicit:
            candidate["priority"] = 10
            candidate["priority_tier"] = "explicit_selection"
            candidate["reason_codes"] = [
                "bounded_single_job",
                "explicit_selection",
                "explicit_source_id",
                "provider_original",
            ]
        candidate["queue_ordinal"] = index
        candidate["defer_reason"] = None
        candidates.append(candidate)
    base["candidates"] = candidates
    base["selection_basis"] = {
        "purpose": "sealed Archive campaign fixture",
        "manifest_sha256": None,
        "youtube_video_ids": [],
        "source_ids": sorted(c["source_id"] for c in candidates) if explicit else [],
        "recording_ids": [],
        "requested_identifiers_already_acquired": [],
        "requested_identifiers_not_eligible": [],
    }
    base["limits"]["selection_only"] = explicit
    base["limits"]["max_items"] = max(1, len(candidates))
    base["limits"]["plan_budget_bytes"] = max(1, sum(sizes))
    base["summary"]["supported_unacquired_sources"] = len(candidates)
    base["summary"]["already_acquired_recordings"] = 0
    base["summary"]["withheld_access_sources"] = 0
    return select_and_summarize(base)


class CampaignEpochMaterializerTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_PARENT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="campaign-epochs-", dir=TEST_PARENT
        )
        self.root = Path(self.temporary.name).resolve()
        self.parent_plan = self.root / "parent-plan.json"
        self.campaign_root = self.root / "campaign-control"
        self.media_root = self.root / "media-output"
        self.fake_ytdlp = self.root / "fake-yt-dlp"
        self.invocation_marker = self.root / "network-tool-was-invoked"
        self.fake_ytdlp.write_text(
            "#!/bin/sh\n"
            f"touch {str(self.invocation_marker)!r}\n"
            "exit 99\n",
            encoding="utf-8",
        )
        self.fake_ytdlp.chmod(0o755)

    def tearDown(self) -> None:
        for child in sorted(self.root.rglob("*"), reverse=True):
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        try:
            self.root.chmod(0o700)
        except OSError:
            pass
        self.temporary.cleanup()

    def write_parent(self, plan: dict, *, mode: int = 0o400) -> str:
        self.parent_plan.write_text(
            json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        self.parent_plan.chmod(mode)
        return file_sha256(self.parent_plan)

    def arguments(
        self,
        expected_sha256: str,
        *,
        max_items: int = 3,
        max_bytes: int = 10_000_000,
    ) -> list[str]:
        return [
            "--parent-plan",
            str(self.parent_plan),
            "--expected-parent-plan-sha256",
            expected_sha256,
            "--campaign-root",
            str(self.campaign_root),
            "--media-output-root",
            str(self.media_root),
            "--yt-dlp-executable",
            str(self.fake_ytdlp),
            "--yt-dlp-sha256",
            file_sha256(self.fake_ytdlp),
            "--global-cache-cap-bytes",
            str(50 * 1024**3),
            "--free-space-floor-bytes",
            str(64 * 1024**3),
            "--max-epoch-items",
            str(max_items),
            "--max-epoch-estimated-bytes",
            str(max_bytes),
        ]

    def execute(self, arguments: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(PROGRAM), *arguments],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def load_manifest(self, result: subprocess.CompletedProcess[str]) -> tuple[dict, dict]:
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(result.stdout)
        path = Path(receipt["campaign_manifest_path"])
        self.assertEqual(file_sha256(path), receipt["campaign_manifest_sha256"])
        validated = subprocess.run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(CAMPAIGN_SCHEMA),
                str(path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(validated.returncode, 0, validated.stdout + validated.stderr)
        return receipt, json.loads(path.read_text(encoding="utf-8"))

    def test_partitions_all_selected_candidates_and_proves_exact_coverage(self) -> None:
        digest = self.write_parent(
            archive_plan([4_000_000, 4_000_000, 4_000_000, 7_000_000, 3_000_000])
        )
        receipt, manifest = self.load_manifest(self.execute(self.arguments(digest)))
        self.assertEqual(receipt["epoch_count"], 3)
        self.assertEqual(receipt["selected_count"], 5)
        self.assertEqual(
            [row["selected_count"] for row in manifest["epochs"]], [2, 1, 2]
        )
        self.assertEqual(
            [row["selected_estimated_bytes"] for row in manifest["epochs"]],
            [8_000_000, 4_000_000, 10_000_000],
        )
        proof = manifest["coverage_proof"]
        self.assertEqual(proof["parent_selected_count"], 5)
        self.assertEqual(proof["epoch_union_count"], 5)
        self.assertEqual(proof["epoch_unique_count"], 5)
        self.assertEqual(proof["overlap_count"], 0)
        self.assertEqual(proof["missing_count"], 0)
        self.assertEqual(proof["unexpected_count"], 0)
        self.assertEqual(
            proof["parent_selected_member_sha256"],
            proof["epoch_union_member_sha256"],
        )
        self.assertTrue(proof["ordered_union_identical"])
        self.assertTrue(proof["local_ordinals_contiguous"])
        self.assertFalse(self.invocation_marker.exists())

        observed_parent_ordinals = []
        for row in manifest["epochs"]:
            epoch_plan_path = Path(row["epoch_plan"]["path"])
            bundle_path = Path(row["bundle"]["manifest_path"])
            self.assertEqual(file_sha256(epoch_plan_path), row["epoch_plan"]["sha256"])
            self.assertEqual(file_sha256(bundle_path), row["bundle"]["manifest_sha256"])
            self.assertEqual(epoch_plan_path.stat().st_mode & 0o777, 0o400)
            self.assertEqual(bundle_path.stat().st_mode & 0o777, 0o400)
            epoch_plan = json.loads(epoch_plan_path.read_text(encoding="utf-8"))
            self.assertEqual(
                [candidate["queue_ordinal"] for candidate in epoch_plan["candidates"]],
                list(range(1, row["selected_count"] + 1)),
            )
            self.assertLessEqual(len(epoch_plan["selection_basis"]["source_ids"]), 3)
            bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
            self.assertEqual(bundle["work_order_count"], row["selected_count"])
            self.assertEqual(bundle["policy"]["media_output_root"], str(self.media_root))
            observed_parent_ordinals.extend(
                range(
                    row["parent_queue_ordinal_first"],
                    row["parent_queue_ordinal_last"] + 1,
                )
            )
        self.assertEqual(observed_parent_ordinals, list(range(1, 6)))

    def test_exact_replay_is_idempotent_and_tampering_fails_closed(self) -> None:
        digest = self.write_parent(archive_plan([1_000_000, 2_000_000, 3_000_000]))
        arguments = self.arguments(digest, max_items=2)
        first = self.execute(arguments)
        second = self.execute(arguments)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(first.stdout, second.stdout)
        _receipt, manifest = self.load_manifest(first)
        plan_path = Path(manifest["epochs"][0]["epoch_plan"]["path"])
        plan_path.chmod(0o600)
        plan_path.write_text("{}\n", encoding="utf-8")
        refused = self.execute(arguments)
        self.assertEqual(refused.returncode, 2)
        self.assertIn("immutable", json.loads(refused.stderr)["error"]["message"])
        self.assertFalse(self.invocation_marker.exists())

    def test_parent_digest_mode_and_archive_only_boundary_fail_closed(self) -> None:
        digest = self.write_parent(archive_plan([1_000_000]))
        bad_hash = self.execute(self.arguments("0" * 64))
        self.assertEqual(bad_hash.returncode, 2)
        self.assertIn("does not match", json.loads(bad_hash.stderr)["error"]["message"])

        self.parent_plan.chmod(0o600)
        bad_mode = self.execute(self.arguments(digest))
        self.assertEqual(bad_mode.returncode, 2)
        self.assertIn("0400", json.loads(bad_mode.stderr)["error"]["message"])

        mixed = fixture_plan()
        mixed["candidates"] = mixed["candidates"][:2]
        mixed["summary"]["supported_unacquired_sources"] = 2
        mixed = select_and_summarize(mixed)
        mixed_digest = self.write_parent(mixed)
        non_archive = self.execute(self.arguments(mixed_digest))
        self.assertEqual(non_archive.returncode, 2)
        self.assertIn("non-Archive", json.loads(non_archive.stderr)["error"]["message"])
        self.assertFalse(self.invocation_marker.exists())

    def test_single_candidate_larger_than_epoch_budget_is_rejected(self) -> None:
        digest = self.write_parent(archive_plan([11_000_000]))
        refused = self.execute(self.arguments(digest, max_bytes=10_000_000))
        self.assertEqual(refused.returncode, 2)
        self.assertIn("exceeds the epoch byte cap", json.loads(refused.stderr)["error"]["message"])
        self.assertFalse(self.invocation_marker.exists())

    def test_nonexplicit_archive_plan_derives_valid_compact_epochs(self) -> None:
        digest = self.write_parent(
            archive_plan([1_000_000, 2_000_000, 3_000_000], explicit=False)
        )
        _receipt, manifest = self.load_manifest(
            self.execute(self.arguments(digest, max_items=2))
        )
        for row in manifest["epochs"]:
            plan = json.loads(Path(row["epoch_plan"]["path"]).read_text())
            self.assertFalse(plan["limits"]["selection_only"])
            self.assertEqual(plan["selection_basis"]["source_ids"], [])


if __name__ == "__main__":
    unittest.main()
