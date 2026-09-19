from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ACQUISITION_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ACQUISITION_ROOT))

import acquire  # noqa: E402
import background_producer  # noqa: E402
import materialize_campaign_epochs  # noqa: E402
import materialize_campaign_schedule_set as schedule_set  # noqa: E402
import materialize_queue  # noqa: E402
from acquisition.tests.test_materialize_campaign_epochs import archive_plan
from acquisition.tests.test_materialize_queue import select_and_summarize


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TEST_PARENT = REPOSITORY_ROOT / "acquisition" / ".test-work"
SET_SCHEMA = (
    REPOSITORY_ROOT
    / "acquisition/schemas/campaign-background-schedule-set-manifest.schema.json"
)
SCHEDULE_SCHEMA = (
    REPOSITORY_ROOT / "acquisition/schemas/background-producer-schedule.schema.json"
)
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts/validate-json-contracts.py"
PROGRAM = REPOSITORY_ROOT / "acquisition/materialize_campaign_schedule_set.py"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def role_plan(role: str, sizes: list[int]) -> dict:
    plan = copy.deepcopy(archive_plan(sizes))
    for index, candidate in enumerate(plan["candidates"], 1):
        candidate["recording_id"] = f"rec_{role}_{index:03d}"
        candidate["source_id"] = f"src_{role}_{index:03d}"
        candidate["native_id"] = f"{role}-item/movie-{index:03d}.mp4"
        candidate["canonical_url"] = (
            f"https://archive.org/download/{role}-item/movie-{index:03d}.mp4"
        )
        candidate["title"] = f"{role} fixture {index}"
    plan["selection_basis"]["source_ids"] = sorted(
        candidate["source_id"] for candidate in plan["candidates"]
    )
    return select_and_summarize(plan)


class CampaignScheduleSetMaterializerTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_PARENT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="campaign-schedule-set-", dir=TEST_PARENT
        )
        self.root = Path(self.temporary.name).resolve()
        self.executable = self.root / "fake-yt-dlp"
        self.invocation_marker = self.root / "network-tool-was-invoked"
        self.executable.write_text(
            "#!/bin/sh\n"
            f"touch {str(self.invocation_marker)!r}\n"
            "exit 97\n",
            encoding="utf-8",
        )
        self.executable.chmod(0o700)
        self.executable_pin = {
            "executable": str(self.executable),
            "sha256": file_sha256(self.executable),
            "byte_count": self.executable.stat().st_size,
        }
        self.control_root = self.root / "schedule-control"
        self.normal = self.materialize_campaign(
            "normal", [1_000_000] * 33
        )
        self.cold = self.materialize_campaign(
            "cold", [2_000_000] * 9
        )

    def tearDown(self) -> None:
        if self.root.exists() and not self.root.is_symlink():
            for current, directories, files in os.walk(self.root, topdown=False):
                current_path = Path(current)
                for filename in files:
                    path = current_path / filename
                    if not path.is_symlink():
                        path.chmod(0o600)
                for dirname in directories:
                    path = current_path / dirname
                    if not path.is_symlink():
                        path.chmod(0o700)
                current_path.chmod(0o700)
        self.temporary.cleanup()

    def write_plan(self, role: str, plan: dict) -> Path:
        path = self.root / f"{role}-parent-plan.json"
        path.write_bytes(materialize_queue.pretty_bytes(plan))
        path.chmod(0o400)
        return path

    def materialize_campaign(
        self,
        role: str,
        sizes: list[int],
        *,
        media_root: Path = schedule_set.COLD_MEDIA_ROOT,
        plan: dict | None = None,
        max_epoch_items: int | None = None,
        max_epoch_estimated_bytes: int | None = None,
    ) -> dict:
        parent = self.write_plan(role, role_plan(role, sizes) if plan is None else plan)
        default_role = (
            schedule_set.NORMAL_ROLE
            if role == "normal"
            else schedule_set.COLD_ONLY_ROLE
        )
        cap = schedule_set.ROLE_EPOCH_CAPS[default_role]
        manifest, path = materialize_campaign_epochs.materialize_campaign(
            parent_plan_path=parent,
            expected_parent_plan_sha256=file_sha256(parent),
            campaign_root=self.root / f"{role}-campaign-control",
            media_output_root=media_root,
            executable_pin=self.executable_pin,
            max_epoch_items=(
                cap["max_epoch_items"]
                if max_epoch_items is None
                else max_epoch_items
            ),
            max_epoch_estimated_bytes=(
                cap["max_epoch_estimated_bytes"]
                if max_epoch_estimated_bytes is None
                else max_epoch_estimated_bytes
            ),
            global_cache_cap_bytes=16 * 1024**3,
            free_space_floor_bytes=512 * 1024**3,
            format_selector=materialize_queue.DEFAULT_FORMAT_SELECTOR,
            http_timeout_seconds=30,
        )
        return {"manifest": manifest, "path": path, "sha256": file_sha256(path)}

    def materialize(self) -> tuple[dict, Path]:
        return schedule_set.materialize_schedule_set(
            normal_campaign_manifest_path=self.normal["path"],
            expected_normal_campaign_manifest_sha256=self.normal["sha256"],
            cold_only_campaign_manifest_path=self.cold["path"],
            expected_cold_only_campaign_manifest_sha256=self.cold["sha256"],
            control_root=self.control_root,
        )

    def validate_instance(self, schema: Path, instance: Path) -> None:
        result = subprocess.run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(schema),
                str(instance),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_materializes_exact_ordered_schedules_and_union_proof_offline(self) -> None:
        with (
            mock.patch.object(
                background_producer,
                "build_schedule",
                wraps=background_producer.build_schedule,
            ) as builder,
            mock.patch.object(
                background_producer,
                "_write_immutable",
                wraps=background_producer._write_immutable,
            ) as writer,
            mock.patch.object(acquire, "run_acquisition") as acquisition,
            mock.patch.object(background_producer, "run_producer") as run_producer,
        ):
            manifest, manifest_path = self.materialize()
        self.assertEqual(builder.call_count, 4)
        self.assertEqual(writer.call_count, 5)
        acquisition.assert_not_called()
        run_producer.assert_not_called()
        self.assertFalse(self.invocation_marker.exists())
        self.assertEqual(manifest_path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(file_sha256(manifest_path), file_sha256(manifest_path))
        self.validate_instance(SET_SCHEMA, manifest_path)

        schedules = manifest["schedules"]
        self.assertEqual(
            [row["role"] for row in schedules],
            [
                schedule_set.NORMAL_ROLE,
                schedule_set.NORMAL_ROLE,
                schedule_set.COLD_ONLY_ROLE,
                schedule_set.COLD_ONLY_ROLE,
            ],
        )
        self.assertEqual(
            [row["schedule_ordinal"] for row in schedules], [1, 2, 3, 4]
        )
        self.assertEqual(
            len({row["preprocess_state_root"] for row in schedules}), 4
        )
        for row in schedules:
            path = Path(row["schedule_path"])
            self.assertEqual(path.stat().st_mode & 0o777, 0o400)
            self.assertEqual(file_sha256(path), row["schedule_sha256"])
            self.assertFalse(Path(row["preprocess_state_root"]).exists())
            self.assertTrue(path.is_relative_to(self.control_root))
            self.assertTrue(
                Path(row["preprocess_state_root"]).is_relative_to(self.control_root)
            )
            schedule = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(schedule["schedule_id"], row["schedule_id"])
            self.assertEqual(
                schedule["queue"]["manifest_path"],
                row["source_epoch"]["bundle_manifest_path"],
            )
            self.assertEqual(
                schedule["policy"]["dispatch_order"],
                "sealed_ordinal_sequential_bounded_failure_isolation",
            )
            self.assertEqual(
                schedule["safety"],
                background_producer.COLD_PRIMARY_SCHEDULE_SAFETY,
            )
            expected = schedule_set.ROLE_POLICIES[row["role"]]
            for key, value in expected.items():
                self.assertEqual(schedule["policy"][key], value)
            epoch_cap = schedule_set.ROLE_EPOCH_CAPS[row["role"]]
            self.assertLessEqual(
                row["source_epoch"]["selected_count"],
                epoch_cap["max_epoch_items"],
            )
            self.assertLessEqual(
                row["source_epoch"]["selected_estimated_bytes"],
                epoch_cap["max_epoch_estimated_bytes"],
            )
            self.validate_instance(SCHEDULE_SCHEMA, path)

        proof = manifest["coverage_proof"]
        self.assertEqual(proof["source_campaign_count"], 2)
        self.assertEqual(proof["source_epoch_count"], 4)
        self.assertEqual(proof["schedule_count"], 4)
        self.assertEqual(proof["source_selected_count"], 42)
        self.assertEqual(proof["schedule_selected_count"], 42)
        self.assertEqual(proof["overlap_count"], 0)
        self.assertEqual(proof["missing_count"], 0)
        self.assertEqual(proof["unexpected_count"], 0)
        self.assertEqual(
            proof["source_epoch_union_sha256"],
            proof["schedule_epoch_union_sha256"],
        )
        self.assertEqual(
            proof["source_member_union_sha256"],
            proof["schedule_member_union_sha256"],
        )
        self.assertTrue(proof["ordered_epoch_union_identical"])
        self.assertTrue(proof["ordered_member_union_identical"])

        core = {
            key: value
            for key, value in manifest.items()
            if key not in {"schedule_set_id", "identity_sha256"}
        }
        identity = schedule_set.sha256_bytes(schedule_set.canonical_bytes(core))
        self.assertEqual(manifest["identity_sha256"], identity)
        self.assertEqual(
            manifest["schedule_set_id"], f"bgacqscheduleset_{identity[:32]}"
        )

    def test_normal_and_cold_only_policy_defaults_are_role_specific(self) -> None:
        manifest, _path = self.materialize()
        normal, cold = manifest["role_policies"]
        self.assertEqual(normal["role"], schedule_set.NORMAL_ROLE)
        self.assertEqual(normal["max_epoch_items"], 32)
        self.assertEqual(normal["max_epoch_estimated_bytes"], 16 * 1024**3)
        self.assertEqual(normal["ready_high_items"], 64)
        self.assertEqual(normal["ready_low_items"], 32)
        self.assertEqual(normal["ready_high_bytes"], 64 * 1024**3)
        self.assertEqual(normal["ready_low_bytes"], 32 * 1024**3)
        self.assertEqual(normal["maximum_dispatch_items_per_run"], 8)
        self.assertEqual(cold["role"], schedule_set.COLD_ONLY_ROLE)
        self.assertEqual(cold["max_epoch_items"], 8)
        self.assertEqual(cold["max_epoch_estimated_bytes"], 128 * 1024**3)
        self.assertEqual(cold["ready_high_items"], 1000)
        self.assertEqual(cold["ready_low_items"], 999)
        self.assertEqual(cold["ready_high_bytes"], 4 * 1024**4)
        self.assertEqual(cold["ready_low_bytes"], 3 * 1024**4)
        self.assertEqual(cold["maximum_dispatch_items_per_run"], 8)
        self.assertEqual(
            normal["maximum_dispatch_bytes_per_run"], 64 * 1024**3
        )
        self.assertEqual(
            cold["maximum_dispatch_bytes_per_run"], 128 * 1024**3
        )
        for policy in (normal, cold):
            self.assertEqual(policy["maximum_run_seconds"], 14_400)
            self.assertEqual(policy["free_space_floor_bytes"], 512 * 1024**3)

    def test_exact_replay_is_idempotent_and_cli_emits_only_a_control_receipt(self) -> None:
        first_manifest, first_path = self.materialize()
        second_manifest, second_path = self.materialize()
        self.assertEqual(first_manifest, second_manifest)
        self.assertEqual(first_path, second_path)
        result = subprocess.run(
            [
                "python3",
                str(PROGRAM),
                "--normal-processing-campaign-manifest",
                str(self.normal["path"]),
                "--expected-normal-processing-campaign-manifest-sha256",
                self.normal["sha256"],
                "--cold-acquisition-only-requires-chunking-campaign-manifest",
                str(self.cold["path"]),
                "--expected-cold-acquisition-only-requires-chunking-campaign-manifest-sha256",
                self.cold["sha256"],
                "--control-root",
                str(self.control_root),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(result.stdout)
        self.assertEqual(receipt["status"], "materialized")
        self.assertEqual(receipt["schedule_set_manifest_path"], str(first_path))
        self.assertEqual(receipt["schedule_set_id"], first_manifest["schedule_set_id"])
        self.assertFalse(self.invocation_marker.exists())

    def test_digest_mode_symlink_and_control_root_boundaries_fail_closed(self) -> None:
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "expected SHA-256"
        ):
            schedule_set.materialize_schedule_set(
                normal_campaign_manifest_path=self.normal["path"],
                expected_normal_campaign_manifest_sha256="0" * 64,
                cold_only_campaign_manifest_path=self.cold["path"],
                expected_cold_only_campaign_manifest_sha256=self.cold["sha256"],
                control_root=self.control_root,
            )
        self.assertFalse(self.control_root.exists())

        self.normal["path"].chmod(0o600)
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "0400"
        ):
            self.materialize()
        self.normal["path"].chmod(0o400)

        alias = self.root / "normal-campaign-alias.json"
        alias.symlink_to(self.normal["path"])
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "symlink"
        ):
            schedule_set.materialize_schedule_set(
                normal_campaign_manifest_path=alias,
                expected_normal_campaign_manifest_sha256=self.normal["sha256"],
                cold_only_campaign_manifest_path=self.cold["path"],
                expected_cold_only_campaign_manifest_sha256=self.cold["sha256"],
                control_root=self.control_root,
            )
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "disjoint"
        ):
            schedule_set.materialize_schedule_set(
                normal_campaign_manifest_path=self.normal["path"],
                expected_normal_campaign_manifest_sha256=self.normal["sha256"],
                cold_only_campaign_manifest_path=self.cold["path"],
                expected_cold_only_campaign_manifest_sha256=self.cold["sha256"],
                control_root=schedule_set.COLD_MEDIA_ROOT / "controls",
            )

    def test_campaign_overlap_and_noncanonical_media_root_are_rejected(self) -> None:
        overlap = self.materialize_campaign(
            "overlap",
            [1_000_000] * 33,
            plan=role_plan("normal", [1_000_000] * 33),
        )
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "overlap"
        ):
            schedule_set.materialize_schedule_set(
                normal_campaign_manifest_path=self.normal["path"],
                expected_normal_campaign_manifest_sha256=self.normal["sha256"],
                cold_only_campaign_manifest_path=overlap["path"],
                expected_cold_only_campaign_manifest_sha256=overlap["sha256"],
                control_root=self.control_root,
            )
        self.assertFalse(self.control_root.exists())

        wrong_root = self.materialize_campaign(
            "hot-root", [7_000_000], media_root=self.root / "wrong-media-root"
        )
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "cold-primary media root"
        ):
            schedule_set.materialize_schedule_set(
                normal_campaign_manifest_path=self.normal["path"],
                expected_normal_campaign_manifest_sha256=self.normal["sha256"],
                cold_only_campaign_manifest_path=wrong_root["path"],
                expected_cold_only_campaign_manifest_sha256=wrong_root["sha256"],
                control_root=self.control_root,
            )
        self.assertFalse(self.invocation_marker.exists())

    def test_role_epoch_cap_substitutions_are_rejected(self) -> None:
        wrong_normal = self.materialize_campaign(
            "wrong-normal-cap",
            [1_000_000, 2_000_000],
            max_epoch_items=31,
            max_epoch_estimated_bytes=16 * 1024**3,
        )
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "exact role epoch cap"
        ):
            schedule_set.materialize_schedule_set(
                normal_campaign_manifest_path=wrong_normal["path"],
                expected_normal_campaign_manifest_sha256=wrong_normal["sha256"],
                cold_only_campaign_manifest_path=self.cold["path"],
                expected_cold_only_campaign_manifest_sha256=self.cold["sha256"],
                control_root=self.control_root,
            )

        wrong_cold = self.materialize_campaign(
            "wrong-cold-cap",
            [3_000_000, 4_000_000],
            max_epoch_items=8,
            max_epoch_estimated_bytes=64 * 1024**3,
        )
        with self.assertRaisesRegex(
            schedule_set.ScheduleSetMaterializationError, "exact role epoch cap"
        ):
            schedule_set.materialize_schedule_set(
                normal_campaign_manifest_path=self.normal["path"],
                expected_normal_campaign_manifest_sha256=self.normal["sha256"],
                cold_only_campaign_manifest_path=wrong_cold["path"],
                expected_cold_only_campaign_manifest_sha256=wrong_cold["sha256"],
                control_root=self.control_root,
            )
        self.assertFalse(self.control_root.exists())


if __name__ == "__main__":
    unittest.main()
