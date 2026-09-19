"""Synthetic setup tests: no deployed campaign, media, GPU, or service access."""

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

from autonomous_controller import setup_archive_incremental as setup
from autonomous_controller.config import REPLACEMENT_COLD_MOUNT_UUID, build_config, canonical_bytes, sha256_bytes
from autonomous_controller.tests.test_controller import config_core


class IncrementalSetupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.campaign = self.root / "campaign"
        self.campaign.mkdir(mode=0o700)
        self.operational_parent = self.root / "operational"
        self.operational_parent.mkdir(mode=0o700)
        self.controls = self.root / "new-controls"
        self.controls.mkdir(mode=0o700)
        old_root = self.root / "old-operational"
        core = config_core(old_root)
        gpu = core["gpu_readiness"]
        refs = {}
        for key in ("root_registration", "production_profile"):
            path = self.root / (key + ".json")
            digest = self._write(path, {"fixture": key})
            gpu[key], gpu[key + "_sha256"] = str(path), digest
            refs[key] = {"path": str(path), "sha256": digest}
        source = self.root / "template.json"
        source_sha = self._write(source, build_config(core))
        self.original_body = source.read_bytes()
        launcher = self.controls / "trusted-launcher-v2"
        launcher.write_bytes(b"fixture executable, never executed\n")
        launcher.chmod(0o500)
        profile = self.controls / "launcher-profile-v2.json"
        profile_sha = self._write(profile, {"fixture": "profile"})
        runtime = {"status": "candidate", "identity_sha256": "d" * 64,
                   "trusted_install": {
                       "launcher": {"path": str(launcher), "sha256": sha256_bytes(launcher.read_bytes())},
                       "launcher_profile": {"path": str(profile), "sha256": profile_sha}}}
        runtime_path = self.controls / "runtime-candidate-v2.json"
        runtime_sha = self._write(runtime_path, runtime)
        readiness = {"status": "passed", "mode": "local-private-production",
                     "runtime_admission": {"path": str(runtime_path), "sha256": runtime_sha,
                                           "identity_sha256": runtime["identity_sha256"]},
                     "launcher": {**runtime["trusted_install"]["launcher"],
                                  "profile": runtime["trusted_install"]["launcher_profile"]}, **refs}
        readiness_sha = self._write(self.controls / "readiness-v1.json", readiness)
        totals = {"candidate_count": 4, "ready_selected_count": 1, "parked_requires_chunking_count": 3}
        inventory = {"kind": "himr_known_archive_collection_inventory", "schema_version": 1,
                     "scope": setup.INVENTORY_SCOPE, "totals": totals,
                     "collections": [{"identifier": "699994", **totals}]}
        inventory_path = self.campaign / "inventory.json"
        inventory_sha = self._write(inventory_path, inventory)
        schedules = []
        for ordinal, (role, count) in enumerate(((setup.NORMAL, 1), (setup.COLD, 3)), 1):
            folder = self.campaign / f"bundle-{ordinal}"
            folder.mkdir(mode=0o700)
            (folder / "work-orders").mkdir(mode=0o700)
            members = []
            for item in range(1, count + 1):
                order = {"job_id": f"job-{ordinal}-{item}", "source": {
                    "platform": "internet_archive", "source_kind": "archive_media_file",
                    "access_state": "public", "native_id": f"699994/video-{ordinal}-{item}.mp4"}}
                relative = f"work-orders/{item:06d}.json"
                digest = self._write(folder / relative, order)
                members.append({"path": relative, "sha256": digest, "job_id": order["job_id"]})
            bundle = {"bundle_id": f"bundle-{ordinal}", "work_order_count": count, "work_orders": members}
            bundle_path = folder / "manifest.json"
            bundle_sha = self._write(bundle_path, bundle)
            schedule = {"schedule_id": "bgacqsched_" + str(ordinal) * 32,
                        "queue": {"bundle_id": bundle["bundle_id"], "manifest_path": str(bundle_path),
                                  "manifest_sha256": bundle_sha}}
            schedule_path = self.campaign / f"schedule-{ordinal}.json"
            schedule_sha = self._write(schedule_path, schedule)
            schedules.append({"schedule_id": schedule["schedule_id"], "schedule_path": str(schedule_path),
                              "schedule_sha256": schedule_sha, "schedule_ordinal": ordinal, "role": role,
                              "source_epoch": {"bundle_id": bundle["bundle_id"], "bundle_manifest_path": str(bundle_path),
                                               "bundle_manifest_sha256": bundle_sha, "selected_count": count}})
        schedule_set_path = self.campaign / "schedule-set.json"
        schedule_set_sha = self._write(schedule_set_path, {"schedule_set_kind": setup.SCHEDULE_KIND,
            "schedule_set_id": "bgacqscheduleset_" + "e" * 32, "schedules": schedules})
        self.arguments = dict(source_config=source, source_config_sha256=source_sha,
            inventory=inventory_path, inventory_sha256=inventory_sha, schedule_set=schedule_set_path,
            schedule_set_sha256=schedule_set_sha, gpu_control_root=self.controls,
            runtime_admission_sha256=runtime_sha, readiness_sha256=readiness_sha,
            operational_root=self.operational_parent / "new-campaign", output=self.campaign / "controller-config.json",
            cold_mount_uuid=REPLACEMENT_COLD_MOUNT_UUID, expected_normal_count=1, expected_cold_count=3)

    @staticmethod
    def _write(path, value):
        if path.exists():
            path.chmod(0o600)
        body = canonical_bytes(value)
        path.write_bytes(body)
        path.chmod(0o400)
        return sha256_bytes(body)

    def _change(self, path_key, digest_key, mutate):
        path = self.arguments[path_key]
        value = json.loads(path.read_bytes())
        mutate(value)
        self.arguments[digest_key] = self._write(path, value)

    def _assert_no_setup_writes(self):
        self.assertFalse(self.arguments["operational_root"].exists())
        self.assertFalse(self.arguments["output"].exists())
        self.assertEqual(self.original_body, self.arguments["source_config"].read_bytes())

    def test_plan_is_read_only_with_exact_new_roots_and_counts(self):
        with mock.patch("subprocess.run", side_effect=AssertionError("no processes")):
            result = setup.setup_archive_incremental(**self.arguments)
        self._assert_no_setup_writes()
        document = result["config_document"]
        self.assertEqual("planned", result["status"])
        self.assertEqual({setup.NORMAL: 1, setup.COLD: 3}, result["item_counts"])
        self.assertEqual(2, result["configured_schedule_count"])
        self.assertEqual(REPLACEMENT_COLD_MOUNT_UUID, document["safety"]["cold_mount_uuid"])
        self.assertEqual(str(self.controls / "runtime-candidate-v2.json"), document["gpu_readiness"]["runtime_admission"])
        for path in setup._old_writable_roots(document):
            self.assertIn(self.arguments["operational_root"], path.parents)
        self.assertFalse(result["full_campaign_validation_performed"])
        self.assertFalse(result["source_state_copied"])

    def test_seal_creates_only_fresh_private_roots_and_immutable_config(self):
        planned = setup.setup_archive_incremental(**self.arguments)
        result = setup.setup_archive_incremental(**self.arguments, seal=True)
        self.assertEqual(planned["config_sha256"], result["config_sha256"])
        self.assertEqual("configured", result["status"])
        self.assertEqual(0o400, stat.S_IMODE(self.arguments["output"].stat().st_mode))
        root = self.arguments["operational_root"]
        self.assertEqual(set(setup.ROOT_NAMES), {path.name for path in root.iterdir()})
        for path in (root, *root.iterdir(), root / "state/events", root / "state/gpu-children"):
            self.assertEqual(0o700, stat.S_IMODE(path.stat().st_mode))
        self.assertEqual(self.original_body, self.arguments["source_config"].read_bytes())
        with self.assertRaises(setup.SetupError):
            setup.setup_archive_incremental(**self.arguments, seal=True)

    def test_refuses_existing_root_even_if_empty(self):
        self.arguments["operational_root"].mkdir(mode=0o700)
        with self.assertRaisesRegex(setup.SetupError, "must be new"):
            setup.setup_archive_incremental(**self.arguments, seal=True)

    def test_refuses_existing_config_without_creating_state(self):
        self._write(self.arguments["output"], {"existing": True})
        with self.assertRaisesRegex(setup.SetupError, "already exists"):
            setup.setup_archive_incremental(**self.arguments, seal=True)
        self.assertFalse(self.arguments["operational_root"].exists())

    def test_wrong_inventory_sha_refused_before_writes(self):
        self.arguments["inventory_sha256"] = "f" * 64
        with self.assertRaisesRegex(setup.SetupError, "SHA-256"):
            setup.setup_archive_incremental(**self.arguments, seal=True)
        self._assert_no_setup_writes()

    def test_original_all_known_inventory_scope_is_not_reused(self):
        self._change("inventory", "inventory_sha256", lambda value: value.update(scope="all_known"))
        with self.assertRaisesRegex(setup.SetupError, "exact incremental"):
            setup.setup_archive_incremental(**self.arguments, seal=True)
        self._assert_no_setup_writes()

    def test_wrong_counts_and_boolean_counts_refused(self):
        for normal, cold in ((2, 3), (True, 3), (1, 1024)):
            with self.subTest(normal=normal, cold=cold):
                args = {**self.arguments, "expected_normal_count": normal, "expected_cold_count": cold}
                with self.assertRaises(setup.SetupError):
                    setup.setup_archive_incremental(**args, seal=True)
        self._assert_no_setup_writes()

    def test_schedule_order_and_bundle_count_fail_closed(self):
        self._change("schedule_set", "schedule_set_sha256",
                     lambda value: value["schedules"][0]["source_epoch"].update(selected_count=2))
        with self.assertRaisesRegex(setup.SetupError, "cardinality"):
            setup.setup_archive_incremental(**self.arguments, seal=True)
        self._assert_no_setup_writes()

    def test_symlink_inventory_rejected(self):
        linked = self.campaign / "linked-inventory.json"
        linked.symlink_to(self.arguments["inventory"])
        self.arguments["inventory"] = linked
        with self.assertRaisesRegex(setup.SetupError, "symlink"):
            setup.setup_archive_incremental(**self.arguments, seal=True)
        self._assert_no_setup_writes()

    def test_hardlinked_inventory_rejected(self):
        os.link(self.arguments["inventory"], self.campaign / "inventory-copy.json")
        with self.assertRaisesRegex(setup.SetupError, "sealed"):
            setup.setup_archive_incremental(**self.arguments, seal=True)
        self._assert_no_setup_writes()

    def test_readiness_for_another_runtime_rejected(self):
        path = self.controls / "readiness-v1.json"
        value = json.loads(path.read_bytes())
        value["runtime_admission"]["sha256"] = "f" * 64
        self.arguments["readiness_sha256"] = self._write(path, value)
        with self.assertRaisesRegex(setup.SetupError, "another runtime"):
            setup.setup_archive_incremental(**self.arguments, seal=True)
        self._assert_no_setup_writes()

    def test_cli_plan_reports_no_processing_or_files(self):
        argv = ["plan"]
        for key, value in self.arguments.items():
            argv.extend(("--" + key.replace("_", "-"), str(value)))
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(0, setup.main(argv))
        self.assertEqual("planned", json.loads(output.getvalue())["status"])
        self._assert_no_setup_writes()


if __name__ == "__main__":
    unittest.main()
