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
import materialize_campaign_schedule_set as source_set  # noqa: E402
import materialize_composite_campaign_schedule_set as composite  # noqa: E402
import materialize_queue  # noqa: E402
from acquisition.tests.test_materialize_campaign_epochs import archive_plan  # noqa: E402
from acquisition.tests.test_materialize_queue import select_and_summarize  # noqa: E402


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TEST_PARENT = REPOSITORY_ROOT / "acquisition" / ".test-work"
SCHEMA = (
    REPOSITORY_ROOT
    / "acquisition/schemas/composite-campaign-schedule-set-manifest.schema.json"
)
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts/validate-json-contracts.py"
PROGRAM = (
    REPOSITORY_ROOT
    / "acquisition/bin/materialize-composite-campaign-schedule-set"
)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def role_plan(component_name: str, role_name: str, sizes: list[int]) -> dict:
    plan = copy.deepcopy(archive_plan(sizes))
    prefix = f"{component_name}-{role_name}"
    for index, candidate in enumerate(plan["candidates"], 1):
        candidate["recording_id"] = f"rec_{prefix}_{index:03d}"
        candidate["source_id"] = f"src_{prefix}_{index:03d}"
        candidate["native_id"] = f"{prefix}/movie-{index:03d}.mp4"
        candidate["canonical_url"] = (
            f"https://archive.org/download/{prefix}/movie-{index:03d}.mp4"
        )
        candidate["title"] = f"{prefix} fixture {index}"
    plan["selection_basis"]["source_ids"] = sorted(
        candidate["source_id"] for candidate in plan["candidates"]
    )
    return select_and_summarize(plan)


class CompositeCampaignScheduleSetTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_PARENT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="composite-campaign-schedule-set-", dir=TEST_PARENT
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
        self.campaigns: dict[tuple[str, str], dict] = {}
        self.components: dict[str, dict] = {}
        for component_name in composite.COMPONENT_ORDER:
            normal = self.materialize_campaign(
                component_name, source_set.NORMAL_ROLE, [1_000_000, 1_100_000]
            )
            cold = self.materialize_campaign(
                component_name,
                source_set.COLD_ONLY_ROLE,
                [2_000_000, 2_100_000],
            )
            self.campaigns[(component_name, source_set.NORMAL_ROLE)] = normal
            self.campaigns[(component_name, source_set.COLD_ONLY_ROLE)] = cold
            self.components[component_name] = self.materialize_source_set(
                component_name, normal, cold
            )
        self.control_root = self.root / "composite-control"

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

    def write_plan(self, component_name: str, role: str, plan: dict) -> Path:
        slug = "normal" if role == source_set.NORMAL_ROLE else "cold"
        path = self.root / f"{component_name}-{slug}-parent-plan.json"
        path.write_bytes(materialize_queue.pretty_bytes(plan))
        path.chmod(0o400)
        return path

    def materialize_campaign(
        self,
        component_name: str,
        role: str,
        sizes: list[int],
        *,
        plan: dict | None = None,
        root_suffix: str = "",
    ) -> dict:
        slug = "normal" if role == source_set.NORMAL_ROLE else "cold"
        selected = (
            role_plan(component_name, slug, sizes) if plan is None else plan
        )
        parent = self.write_plan(component_name + root_suffix, role, selected)
        caps = source_set.ROLE_EPOCH_CAPS[role]
        manifest, path = materialize_campaign_epochs.materialize_campaign(
            parent_plan_path=parent,
            expected_parent_plan_sha256=file_sha256(parent),
            campaign_root=(
                self.root / f"{component_name}-{slug}-campaign{root_suffix}"
            ),
            media_output_root=source_set.COLD_MEDIA_ROOT,
            executable_pin=self.executable_pin,
            max_epoch_items=caps["max_epoch_items"],
            max_epoch_estimated_bytes=caps["max_epoch_estimated_bytes"],
            global_cache_cap_bytes=16 * 1024**3,
            free_space_floor_bytes=512 * 1024**3,
            format_selector=materialize_queue.DEFAULT_FORMAT_SELECTOR,
            http_timeout_seconds=30,
        )
        return {"manifest": manifest, "path": path, "sha256": file_sha256(path)}

    def materialize_source_set(
        self,
        component_name: str,
        normal: dict,
        cold: dict,
        *,
        suffix: str = "",
    ) -> dict:
        manifest, path = source_set.materialize_schedule_set(
            normal_campaign_manifest_path=normal["path"],
            expected_normal_campaign_manifest_sha256=normal["sha256"],
            cold_only_campaign_manifest_path=cold["path"],
            expected_cold_only_campaign_manifest_sha256=cold["sha256"],
            control_root=self.root / f"{component_name}-source-set{suffix}",
        )
        return {"manifest": manifest, "path": path, "sha256": file_sha256(path)}

    def materialize(self) -> tuple[dict, Path]:
        predecessor = self.components[composite.PREDECESSOR]
        addendum = self.components[composite.ADDENDUM]
        return composite.materialize_composite_schedule_set(
            predecessor_schedule_set_manifest_path=predecessor["path"],
            expected_predecessor_schedule_set_manifest_sha256=predecessor[
                "sha256"
            ],
            addendum_schedule_set_manifest_path=addendum["path"],
            expected_addendum_schedule_set_manifest_sha256=addendum["sha256"],
            control_root=self.control_root,
        )

    def validate_instance(self, instance: Path) -> None:
        result = subprocess.run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(SCHEMA),
                str(instance),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_reference_only_exact_flatten_and_union_proofs(self) -> None:
        source_schedules = {
            Path(row["schedule_path"]): (
                Path(row["schedule_path"]).read_bytes(),
                Path(row["schedule_path"]).stat().st_mtime_ns,
            )
            for component_row in self.components.values()
            for row in component_row["manifest"]["schedules"]
        }
        with (
            mock.patch.object(acquire, "run_acquisition") as run_acquisition,
            mock.patch.object(background_producer, "run_producer") as run_producer,
        ):
            manifest, path = self.materialize()
        run_acquisition.assert_not_called()
        run_producer.assert_not_called()
        self.assertFalse(self.invocation_marker.exists())
        self.assertEqual(path.stat().st_mode & 0o777, 0o400)
        self.validate_instance(path)

        self.assertEqual(
            [(row["component"], row["role"]) for row in manifest["schedules"]],
            [
                (composite.PREDECESSOR, source_set.NORMAL_ROLE),
                (composite.ADDENDUM, source_set.NORMAL_ROLE),
                (composite.PREDECESSOR, source_set.COLD_ONLY_ROLE),
                (composite.ADDENDUM, source_set.COLD_ONLY_ROLE),
            ],
        )
        self.assertEqual(
            [row["schedule_ordinal"] for row in manifest["schedules"]],
            [1, 2, 3, 4],
        )
        self.assertTrue(
            all(
                not Path(row["schedule_path"]).is_relative_to(self.control_root)
                for row in manifest["schedules"]
            )
        )
        self.assertEqual(
            {Path(row["schedule_path"]) for row in manifest["schedules"]},
            set(source_schedules),
        )
        for schedule_path, (before_body, before_mtime) in source_schedules.items():
            self.assertEqual(schedule_path.read_bytes(), before_body)
            self.assertEqual(schedule_path.stat().st_mtime_ns, before_mtime)
        self.assertEqual(
            [p for p in self.control_root.rglob("*") if p.is_file()], [path]
        )

        proof = manifest["coverage_proof"]
        self.assertEqual(proof["component_count"], 2)
        self.assertEqual(proof["source_campaign_count"], 4)
        self.assertEqual(proof["schedule_count"], 4)
        self.assertEqual(proof["source_selected_count"], 8)
        self.assertEqual(proof["unique_source_count"], 8)
        self.assertEqual(proof["unique_recording_count"], 8)
        self.assertEqual(proof["unique_native_count"], 8)
        self.assertEqual(proof["source_overlap_count"], 0)
        self.assertEqual(proof["recording_overlap_count"], 0)
        self.assertEqual(proof["native_overlap_count"], 0)
        self.assertTrue(proof["ordered_schedule_union_identical"])
        self.assertTrue(proof["ordered_member_union_identical"])
        self.assertTrue(proof["source_schedule_files_referenced_not_copied"])
        core = {
            key: value
            for key, value in manifest.items()
            if key not in {"composite_schedule_set_id", "identity_sha256"}
        }
        identity = composite.sha256_bytes(composite.canonical_bytes(core))
        self.assertEqual(manifest["identity_sha256"], identity)
        self.assertEqual(
            manifest["composite_schedule_set_id"],
            f"bgacqcompositeset_{identity[:32]}",
        )

    def test_exact_replay_is_idempotent(self) -> None:
        first, first_path = self.materialize()
        before = first_path.read_bytes()
        before_stat = first_path.stat()
        second, second_path = self.materialize()
        self.assertEqual(second, first)
        self.assertEqual(second_path, first_path)
        self.assertEqual(second_path.read_bytes(), before)
        self.assertEqual(second_path.stat().st_ino, before_stat.st_ino)
        self.assertEqual(second_path.stat().st_mtime_ns, before_stat.st_mtime_ns)
        self.assertFalse(self.invocation_marker.exists())

    def test_cross_component_identity_overlap_is_rejected(self) -> None:
        overlapping = self.materialize_source_set(
            "overlap-addendum",
            self.campaigns[(composite.PREDECESSOR, source_set.NORMAL_ROLE)],
            self.campaigns[(composite.ADDENDUM, source_set.COLD_ONLY_ROLE)],
        )
        predecessor = self.components[composite.PREDECESSOR]
        with self.assertRaisesRegex(
            composite.CompositeScheduleSetError,
            "source, recording, or native identities overlap",
        ):
            composite.materialize_composite_schedule_set(
                predecessor_schedule_set_manifest_path=predecessor["path"],
                expected_predecessor_schedule_set_manifest_sha256=predecessor[
                    "sha256"
                ],
                addendum_schedule_set_manifest_path=overlapping["path"],
                expected_addendum_schedule_set_manifest_sha256=overlapping["sha256"],
                control_root=self.root / "overlap-composite-control",
            )
        self.assertFalse((self.root / "overlap-composite-control").exists())

    def test_mutable_component_and_same_component_are_rejected(self) -> None:
        predecessor = self.components[composite.PREDECESSOR]
        addendum = self.components[composite.ADDENDUM]
        predecessor["path"].chmod(0o600)
        try:
            with self.assertRaisesRegex(
                composite.CompositeScheduleSetError, "must have mode 0400"
            ):
                self.materialize()
        finally:
            predecessor["path"].chmod(0o400)
        with self.assertRaisesRegex(
            composite.CompositeScheduleSetError,
            "must be distinct sealed schedule sets",
        ):
            composite.materialize_composite_schedule_set(
                predecessor_schedule_set_manifest_path=addendum["path"],
                expected_predecessor_schedule_set_manifest_sha256=addendum["sha256"],
                addendum_schedule_set_manifest_path=addendum["path"],
                expected_addendum_schedule_set_manifest_sha256=addendum["sha256"],
                control_root=self.root / "same-component-control",
            )

    def test_cli_emits_one_strict_json_receipt(self) -> None:
        predecessor = self.components[composite.PREDECESSOR]
        addendum = self.components[composite.ADDENDUM]
        result = subprocess.run(
            [
                str(PROGRAM),
                "--predecessor-schedule-set-manifest",
                str(predecessor["path"]),
                "--expected-predecessor-schedule-set-manifest-sha256",
                predecessor["sha256"],
                "--addendum-schedule-set-manifest",
                str(addendum["path"]),
                "--expected-addendum-schedule-set-manifest-sha256",
                addendum["sha256"],
                "--control-root",
                str(self.root / "cli-composite-control"),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertEqual(receipt["status"], "materialized")
        self.assertEqual(receipt["component_count"], 2)
        self.assertEqual(receipt["schedule_count"], 4)
        self.assertEqual(result.stderr, "")
        self.assertFalse(self.invocation_marker.exists())


if __name__ == "__main__":
    unittest.main()
