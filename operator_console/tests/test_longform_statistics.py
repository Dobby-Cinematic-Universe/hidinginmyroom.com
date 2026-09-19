from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from autonomous_controller import config as controller_config
from autonomous_controller.tests import test_controller as controller_fixture
from operator_console import longform_statistics as statistics


class LongformStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.controller_path = self.root / "controller-config.json"
        self.controller = controller_config.build_config(controller_fixture.config_core(self.root))
        self.controller_sha = self.write(self.controller_path, self.controller)
        self.deployment = self.root / "companion-state"
        self.deployment.mkdir(mode=0o700)
        self.status_path = self.deployment / "status.json"
        self.campaign_path = self.root / "longform-config.json"
        self.campaign = self.seal({
            "kind": "himr_longform_asr_campaign_config", "schema_version": 1,
            "source_controller": {"config_id": self.controller["config_id"],
                                  "identity_sha256": self.controller["identity_sha256"],
                                  "path": str(self.controller_path), "physical_sha256": self.controller_sha},
            "deployment": {"root": str(self.deployment), "status_path": str(self.status_path)},
        }, "himrlongcfg_")
        self.campaign_sha = self.write(self.campaign_path, self.campaign)
        registration_core = {
            "kind": "himr_longform_asr_companion_registration", "schema_version": 1,
            "source_controller": {"config_id": self.controller["config_id"], "config_path": str(self.controller_path), "config_sha256": self.controller_sha},
            "companion": {"config_id": self.campaign["config_id"], "config_path": str(self.campaign_path),
                          "config_sha256": self.campaign_sha, "status_path": str(self.status_path),
                          "entrypoint_path": str(self.root / "never-open-this-executable"), "entrypoint_sha256": "c" * 64},
            "supervision": {"poll_interval_milliseconds": 250, "graceful_stop_timeout_seconds": 900},
        }
        self.registration = {**registration_core, "identity_sha256": self.sha(registration_core)}
        self.registration_path = statistics.companion.registration_path_for(self.controller_path)
        self.write(self.registration_path, self.registration)
        self.status = {
            "kind": "himr_longform_asr_campaign_status", "schema_version": 1,
            "source_controller": {"config_id": self.controller["config_id"], "physical_sha256": self.controller_sha},
            "campaign_config": {"config_id": self.campaign["config_id"], "physical_sha256": self.campaign_sha},
            "lifecycle": "running", "expected_cold_backlog": 804,
            "discovered": {"cold_candidates": 548, "queue_candidates": 3007, "total_candidates": 3555},
            "jobs": {"unprepared": 228, "preprocessed": 0, "prepared": 0, "incomplete": 0, "completed": 3327},
            "active_job": "himrlongjob_" + "a" * 32, "updated_at": "2026-09-07T01:43:13Z", "last_error": None,
        }
        self.write_status()

    @staticmethod
    def sha(value):
        return hashlib.sha256(statistics.companion.canonical_bytes(value)).hexdigest()

    def seal(self, value, prefix):
        identity = self.sha(value)
        return {**value, "identity_sha256": identity, "config_id": prefix + identity[:32]}

    def write(self, path, value, mode=0o400):
        if path.exists():
            path.chmod(0o600)
        body = statistics.companion.canonical_bytes(value)
        path.write_bytes(body)
        path.chmod(mode)
        return hashlib.sha256(body).hexdigest()

    def write_status(self):
        self.write(self.status_path, self.status, 0o600)

    def read(self):
        return statistics.read_longform_statistics(self.controller_path, self.controller_sha)

    def assert_unavailable(self, value):
        self.assertEqual(value["state"], "unavailable")
        self.assertIsNone(value["counts"])
        self.assertIsNone(value["completion_percent"])
        self.assertIsNone(value["updated_at"])
        self.assertNotIn(str(self.root), json.dumps(value))

    def test_recording_counts_separate_active_from_remaining_and_unseen_cold(self):
        value = self.read()
        self.assertEqual(value["state"], "available")
        self.assertEqual(value["counts"], {
            "completed_recordings": 3327, "discovered_recordings": 3555,
            "remaining_discovered_recordings": 228, "unprepared_recordings": 228,
            "preprocessed_recordings": 0, "prepared_recordings": 0, "incomplete_recordings": 0,
            "cold_candidates": 548, "queue_candidates": 3007, "expected_cold_backlog": 804,
            "cold_candidates_not_discovered": 256, "active_recordings": 1,
        })
        self.assertAlmostEqual(value["completion_percent"], 100 * 3327 / 3555)
        self.assertEqual(value["basis"], statistics.BASIS)
        self.assertEqual(value["updated_at"], self.status["updated_at"])

    def test_zero_discovered_is_known_zero_not_unknown_or_complete_percent(self):
        self.status["discovered"] = {key: 0 for key in self.status["discovered"]}
        self.status["jobs"] = {key: 0 for key in self.status["jobs"]}
        self.status.update(active_job=None, lifecycle="waiting")
        self.write_status()
        value = self.read()
        self.assertEqual(value["state"], "available")
        self.assertEqual(value["counts"]["completed_recordings"], 0)
        self.assertIsNone(value["completion_percent"])

    def test_all_discovered_complete_still_reports_unseen_cold(self):
        self.status["jobs"].update(unprepared=0, completed=3555)
        self.status.update(active_job=None, lifecycle="waiting")
        self.write_status()
        value = self.read()
        self.assertEqual(value["completion_percent"], 100)
        self.assertEqual(value["counts"]["remaining_discovered_recordings"], 0)
        self.assertEqual(value["counts"]["cold_candidates_not_discovered"], 256)

    def test_active_marker_does_not_override_reported_completion_counts(self):
        # The reporting boundary does not infer when finishing/cleanup releases
        # an active marker or subtract it twice from the producer's counts.
        self.status["jobs"].update(unprepared=0, completed=3555)
        self.write_status()
        value = self.read()
        self.assertEqual(value["state"], "available")
        self.assertEqual(value["counts"]["active_recordings"], 1)
        self.assertEqual(value["counts"]["remaining_discovered_recordings"], 0)

    def test_faulted_status_preserves_reported_counts_and_bounded_error(self):
        self.status.update(active_job=None, lifecycle="faulted", last_error={"type": "CampaignError", "message": "synthetic failure"})
        self.write_status()
        value = self.read()
        self.assertEqual(value["state"], "available")
        self.assertEqual(value["lifecycle"], "faulted")
        self.assertEqual(value["last_error"], self.status["last_error"])

    def test_absent_registration_is_not_registered_not_zero(self):
        self.registration_path.unlink()
        value = self.read()
        self.assertEqual(value["state"], "not_registered")
        self.assertIsNone(value["counts"])
        self.assertIsNone(value["diagnostic"])

    def test_missing_config_campaign_or_status_is_unavailable(self):
        for path in (self.controller_path, self.campaign_path, self.status_path):
            with self.subTest(path=path.name):
                saved = path.with_suffix(".saved")
                path.rename(saved)
                self.assert_unavailable(self.read())
                saved.rename(path)

    def test_external_controller_hash_and_identity_fail_closed(self):
        for digest in (None, "wrong", "f" * 64):
            self.assert_unavailable(statistics.read_longform_statistics(self.controller_path, digest))
        self.controller["identity_sha256"] = "a" * 64
        self.controller_sha = self.write(self.controller_path, self.controller)
        self.assert_unavailable(self.read())

    def test_registration_identity_source_and_campaign_hash_fail_closed(self):
        original = copy.deepcopy(self.registration)
        for field in ("identity", "source", "campaign"):
            with self.subTest(field=field):
                changed = copy.deepcopy(original)
                if field == "identity":
                    changed["identity_sha256"] = "f" * 64
                else:
                    changed["source_controller" if field == "source" else "companion"]["config_sha256"] = "f" * 64
                    changed["identity_sha256"] = self.sha({key: value for key, value in changed.items() if key != "identity_sha256"})
                self.write(self.registration_path, changed)
                self.assert_unavailable(self.read())

    def test_cached_status_cross_binding_is_not_accepted(self):
        for field in ("source_controller", "campaign_config"):
            with self.subTest(field=field):
                original = self.status[field]["physical_sha256"]
                self.status[field]["physical_sha256"] = "f" * 64
                self.write_status()
                self.assert_unavailable(self.read())
                self.status[field]["physical_sha256"] = original

    def test_campaign_source_identity_and_status_path_bindings_fail_closed(self):
        original_campaign = copy.deepcopy(self.campaign)
        original_registration = copy.deepcopy(self.registration)
        for edit in (lambda row: row["source_controller"].update(identity_sha256="f" * 64),
                     lambda row: row["source_controller"].update(path=str(self.root / "other-controller.json")),
                     lambda row: row["deployment"].update(status_path=str(self.root / "other-status.json"))):
            changed = {key: copy.deepcopy(value) for key, value in original_campaign.items() if key not in {"config_id", "identity_sha256"}}
            edit(changed)
            changed = self.seal(changed, "himrlongcfg_")
            changed_sha = self.write(self.campaign_path, changed)
            registered = copy.deepcopy(original_registration)
            registered["companion"].update(config_id=changed["config_id"], config_sha256=changed_sha)
            registered["identity_sha256"] = self.sha({key: value for key, value in registered.items() if key != "identity_sha256"})
            self.write(self.registration_path, registered)
            self.assert_unavailable(self.read())

    def test_malformed_counts_and_boolean_schema_fail_closed(self):
        original = copy.deepcopy(self.status)
        for edit in (lambda row: row["jobs"].update(completed=True),
                     lambda row: row["jobs"].update(completed=-1),
                     lambda row: row["jobs"].update(completed=3328),
                     lambda row: row.update(expected_cold_backlog=1),
                     lambda row: row.update(schema_version=True)):
            self.status = copy.deepcopy(original)
            edit(self.status)
            self.write_status()
            self.assert_unavailable(self.read())

    def test_counters_exceeding_javascript_safe_integer_fail_closed(self):
        total = 2**53
        self.status["discovered"] = {"cold_candidates": 0, "queue_candidates": total, "total_candidates": total}
        self.status["jobs"].update(unprepared=1, completed=total - 1)
        self.write_status()
        self.assert_unavailable(self.read())

    def test_largest_javascript_safe_counter_is_preserved_exactly(self):
        total = statistics.MAX_SAFE_INTEGER
        self.status["discovered"] = {"cold_candidates": 0, "queue_candidates": total, "total_candidates": total}
        self.status["jobs"].update(unprepared=1, completed=total - 1)
        self.write_status()
        value = self.read()
        self.assertEqual(value["state"], "available")
        self.assertEqual(value["counts"]["discovered_recordings"], total)
        self.assertEqual(value["counts"]["completed_recordings"], total - 1)
        self.assertLessEqual(value["completion_percent"], 100)

    def test_unsafe_file_modes_hardlinks_and_symlinks_fail_closed(self):
        for path in (self.controller_path, self.registration_path, self.campaign_path, self.status_path):
            with self.subTest(path=path.name):
                original_mode = path.stat().st_mode & 0o777
                path.chmod(0o644)
                self.assert_unavailable(self.read())
                path.chmod(original_mode)
                alias = self.root / "extra-link"
                os.link(path, alias)
                self.assert_unavailable(self.read())
                alias.unlink()
                path.rename(alias)
                path.symlink_to(alias)
                self.assert_unavailable(self.read())
                path.unlink()
                alias.rename(path)

    def test_nonprivate_status_parent_and_symlink_ancestor_fail_closed(self):
        self.deployment.chmod(0o755)
        self.assert_unavailable(self.read())
        self.deployment.chmod(0o700)
        saved = self.deployment.with_name("saved-deployment")
        self.deployment.rename(saved)
        self.deployment.symlink_to(saved, target_is_directory=True)
        self.assert_unavailable(self.read())

    def test_duplicate_noncanonical_and_oversized_status_fail_closed(self):
        for body in (b'{"kind":1,"kind":2}\n', b'{"value":NaN}\n', b'{"value":1e999}\n', json.dumps(self.status).encode(), b" " * (statistics.MAX_METADATA_BYTES + 1)):
            self.status_path.write_bytes(body)
            self.assert_unavailable(self.read())

    def test_metadata_change_during_status_read_invalidates_entire_snapshot(self):
        original = statistics.companion.read_companion_status
        def changed(registration):
            value = original(registration)
            self.registration_path.chmod(0o600)
            self.registration_path.write_bytes(b"{}\n")
            self.registration_path.chmod(0o400)
            return value
        with mock.patch.object(statistics.companion, "read_companion_status", side_effect=changed):
            value = self.read()
        self.assert_unavailable(value)
        self.assertEqual(value["diagnostic"], "metadata_changed")

    def test_reads_only_four_metadata_documents_without_locks_writes_or_discovery(self):
        allowed = {self.controller_path, self.registration_path, self.campaign_path, self.status_path}
        before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in allowed}
        original = os.open
        reads = set()
        def checked_open(path, flags, *args, **kwargs):
            self.assertFalse(flags & (os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_RDWR))
            if not flags & os.O_DIRECTORY:
                self.assertIn(Path(path).name, {item.name for item in allowed})
                reads.add(Path(path).name)
            return original(path, flags, *args, **kwargs)
        with mock.patch.object(os, "open", side_effect=checked_open), mock.patch.object(os, "scandir", side_effect=AssertionError("no discovery")), mock.patch.object(statistics.companion, "load_registration_if_present", side_effect=AssertionError("no deep replay")), mock.patch.object(statistics.companion, "load_config", side_effect=AssertionError("no execution loader")):
            self.assertEqual(self.read()["state"], "available")
        self.assertEqual(reads, {path.name for path in allowed})
        self.assertEqual(before, {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in allowed})

    def test_unexpected_errors_are_contained_and_diagnostics_do_not_echo_secrets(self):
        with mock.patch.object(statistics.companion, "read_companion_status", side_effect=RuntimeError("SECRET_PRIVATE_DETAIL")):
            value = self.read()
        self.assert_unavailable(value)
        self.assertNotIn("SECRET_PRIVATE_DETAIL", json.dumps(value))
        self.assertEqual(statistics.unavailable_statistics("SECRET_PRIVATE_DETAIL")["diagnostic"], "statistics_unavailable")


if __name__ == "__main__":
    unittest.main()
