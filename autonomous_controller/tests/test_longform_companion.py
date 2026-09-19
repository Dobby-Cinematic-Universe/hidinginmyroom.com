from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from autonomous_controller import longform_companion as companion


def _write(path: Path, body: bytes, mode: int) -> None:
    path.write_bytes(body)
    path.chmod(mode)


class LongformCompanionRegistrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.controller_path = self.root / "controller-config.json"
        self.campaign_path = self.root / "longform-config.json"
        _write(self.controller_path, b"{}\n", 0o400)
        _write(self.campaign_path, b"{}\n", 0o400)
        self.controller = SimpleNamespace(
            config_id="himrautocfg_" + "a" * 32,
            physical_sha256="b" * 64,
            path=self.controller_path,
        )
        self.campaign = SimpleNamespace(
            config_id="himrlongcfg_" + "c" * 32,
            physical_sha256="d" * 64,
            path=self.campaign_path,
            document={
                "source_controller": {
                    "config_id": self.controller.config_id,
                    "physical_sha256": self.controller.physical_sha256,
                    "path": str(self.controller_path),
                },
                "deployment": {"status_path": str(self.root / "status.json")},
            },
        )
        self.entrypoint = (
            Path(companion.__file__).resolve().parents[1]
            / "pipeline"
            / "bin"
            / "longform-asr-campaign"
        )
        entrypoint_digest = hashlib.sha256(self.entrypoint.read_bytes()).hexdigest()
        self.document = companion.build_registration(
            controller=self.controller,
            campaign_config=self.campaign,
            entrypoint_path=self.entrypoint,
            entrypoint_sha256=entrypoint_digest,
            poll_interval_milliseconds=50,
            graceful_stop_timeout_seconds=60,
        )
        self.registration_path = companion.registration_path_for(self.controller_path)
        _write(
            self.registration_path,
            companion.canonical_bytes(self.document),
            0o400,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _load(self) -> companion.LongformCompanionRegistration:
        with (
            mock.patch.object(companion, "load_config", return_value=self.controller),
            mock.patch(
                "pipeline.longform_asr_campaign.load_campaign_config",
                return_value=self.campaign,
            ),
        ):
            observed = companion.load_registration_if_present(
                self.controller_path, self.controller.physical_sha256
            )
        assert observed is not None
        return observed

    def test_absent_registration_preserves_legacy_opt_out(self) -> None:
        self.registration_path.unlink()
        self.assertIsNone(
            companion.load_registration_if_present(
                self.controller_path, self.controller.physical_sha256
            )
        )

    def test_registration_replays_source_campaign_and_fixed_entrypoint(self) -> None:
        observed = self._load()
        self.assertEqual(observed.document, self.document)
        self.assertEqual(observed.campaign_config_path, self.campaign_path)
        self.assertEqual(observed.status_path, self.root / "status.json")

    def test_present_registration_with_bad_identity_fails_closed(self) -> None:
        changed = dict(self.document)
        changed["identity_sha256"] = "0" * 64
        self.registration_path.chmod(0o600)
        _write(self.registration_path, companion.canonical_bytes(changed), 0o400)
        with (
            mock.patch.object(companion, "load_config", return_value=self.controller),
            self.assertRaisesRegex(companion.LongformCompanionError, "identity"),
        ):
            companion.load_registration_if_present(
                self.controller_path, self.controller.physical_sha256
            )

    def test_status_is_canonical_private_and_bound_to_both_configs(self) -> None:
        registration = self._load()
        status = {
            "kind": "himr_longform_asr_campaign_status",
            "schema_version": 1,
            "source_controller": {
                "config_id": self.controller.config_id,
                "physical_sha256": self.controller.physical_sha256,
            },
            "campaign_config": {
                "config_id": self.campaign.config_id,
                "physical_sha256": self.campaign.physical_sha256,
            },
            "lifecycle": "waiting",
            "expected_cold_backlog": 4,
            "discovered": {
                "cold_candidates": 4,
                "queue_candidates": 1,
                "total_candidates": 5,
            },
            "jobs": {
                "unprepared": 1,
                "preprocessed": 0,
                "prepared": 0,
                "incomplete": 1,
                "completed": 3,
            },
            "active_job": None,
            "updated_at": "2026-08-30T12:34:56Z",
            "last_error": None,
        }
        _write(registration.status_path, companion.canonical_bytes(status), 0o600)
        self.assertEqual(companion.read_companion_status(registration), status)

        changed = json.loads(json.dumps(status))
        changed["campaign_config"]["physical_sha256"] = "e" * 64
        _write(registration.status_path, companion.canonical_bytes(changed), 0o600)
        with self.assertRaisesRegex(companion.LongformCompanionError, "binding"):
            companion.read_companion_status(registration)

    def test_status_arithmetic_and_mode_fail_closed(self) -> None:
        registration = self._load()
        status = {
            "kind": "himr_longform_asr_campaign_status",
            "schema_version": 1,
            "source_controller": {
                "config_id": self.controller.config_id,
                "physical_sha256": self.controller.physical_sha256,
            },
            "campaign_config": {
                "config_id": self.campaign.config_id,
                "physical_sha256": self.campaign.physical_sha256,
            },
            "lifecycle": "ready",
            "expected_cold_backlog": 1,
            "discovered": {
                "cold_candidates": 1,
                "queue_candidates": 0,
                "total_candidates": 1,
            },
            "jobs": {
                "unprepared": 0,
                "preprocessed": 0,
                "prepared": 0,
                "incomplete": 0,
                "completed": 0,
            },
            "active_job": None,
            "updated_at": "2026-08-30T12:34:56Z",
            "last_error": None,
        }
        _write(registration.status_path, companion.canonical_bytes(status), 0o600)
        with self.assertRaisesRegex(companion.LongformCompanionError, "do not cover"):
            companion.read_companion_status(registration)
        registration.status_path.chmod(0o644)
        with self.assertRaisesRegex(companion.LongformCompanionError, "unsafe metadata"):
            companion.read_companion_status(registration)


if __name__ == "__main__":
    unittest.main()

