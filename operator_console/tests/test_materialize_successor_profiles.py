from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from autonomous_controller.config import ControllerConfig
from operator_console import registry
from operator_console.materialize_successor_profiles import (
    START_PROFILE_ID,
    STOP_PROFILE_ID,
    SuccessorProfileError,
    build_successor_profile_document,
    materialize_successor_profile_set,
)


class SuccessorProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="himr-operator-successor-profile-"
        )
        self.repo = Path(self.temporary.name).resolve()
        os.chmod(self.repo, 0o700)
        self.workspace = self.repo / "research" / "operator-console"
        self.workspace.mkdir(parents=True, mode=0o700)
        self.entrypoint = (
            self.repo
            / "autonomous_controller"
            / "bin"
            / "himr-autonomous-controller"
        )
        self.entrypoint.parent.mkdir(parents=True)
        self.entrypoint.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        self.entrypoint.chmod(0o700)

        self.predecessor_config_path = (
            self.repo / "research" / "corpus" / "autonomous" / "old" / "controller.json"
        )
        self.successor_config_path = (
            self.repo / "research" / "corpus" / "autonomous" / "new" / "controller.json"
        )
        for path in (self.predecessor_config_path, self.successor_config_path):
            path.parent.mkdir(parents=True)
            path.write_text("{}\n", encoding="utf-8")
            path.chmod(0o400)

        self.predecessor_digest = "1" * 64
        self.successor_digest = "2" * 64
        self.predecessor_state = self.repo / "research" / "operator-state" / "old"
        self.successor_state = self.repo / "research" / "operator-state" / "new"
        for state in (self.predecessor_state, self.successor_state):
            state.mkdir(parents=True, mode=0o700)
            lock = state / "controller.lock"
            lock.touch(mode=0o600)
            lock.chmod(0o600)
        self.predecessor_config = ControllerConfig(
            {
                "config_id": "himrautocfg_" + "1" * 32,
                "state_root": str(self.predecessor_state),
            },
            self.predecessor_config_path,
            self.predecessor_digest,
        )
        self.successor_config = ControllerConfig(
            {
                "config_id": "himrautocfg_" + "2" * 32,
                "state_root": str(self.successor_state),
            },
            self.successor_config_path,
            self.successor_digest,
        )
        self.current_profiles = self.workspace / "profiles.json"
        self._write_profiles(
            self.current_profiles,
            self.predecessor_config_path,
            self.predecessor_digest,
        )
        self.current_digest = hashlib.sha256(
            self.current_profiles.read_bytes()
        ).hexdigest()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_profiles(self, path: Path, config: Path, digest: str) -> None:
        document = build_successor_profile_document(
            successor_config=config,
            successor_config_sha256=digest,
            repo_root=self.repo,
        )
        path.write_text(
            json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        path.chmod(0o400)

    @staticmethod
    def _status(actual_state: str) -> dict[str, object]:
        return {
            "desired_state": "stopped",
            "actual_state": actual_state,
            "execution": {
                "accepting_new_work": False,
                "draining": False,
                "inflight_total": 0,
            },
            "current_stage": None,
            "current_gpu_child": None,
            "lanes": {
                "acquisition": {"active": 0},
                "preprocess": {"active": 0},
                "gpu_readiness": {"active": 0},
                "cold_retention": {"active": 0},
            },
        }

    def _load_config(self, path: Path, digest: str) -> ControllerConfig:
        lookup = {
            (self.predecessor_config_path, self.predecessor_digest): self.predecessor_config,
            (self.successor_config_path, self.successor_digest): self.successor_config,
        }
        try:
            return lookup[(path, digest)]
        except KeyError as error:
            raise AssertionError(f"unexpected config replay: {path}, {digest}") from error

    def _public_status(self, path: Path, digest: str) -> dict[str, object]:
        if (path, digest) == (
            self.predecessor_config_path,
            self.predecessor_digest,
        ):
            return self._status("stopped")
        if (path, digest) == (self.successor_config_path, self.successor_digest):
            return self._status("not_started")
        raise AssertionError(f"unexpected status replay: {path}, {digest}")

    def _patches(self):
        return (
            mock.patch.object(registry, "_validate_systemd_user_tools"),
            mock.patch(
                "operator_console.materialize_successor_profiles.load_config",
                side_effect=self._load_config,
            ),
            mock.patch(
                "operator_console.materialize_successor_profiles.read_control_state",
                return_value={"desired_state": "stopped"},
            ),
            mock.patch(
                "operator_console.materialize_successor_profiles.read_public_status",
                side_effect=self._public_status,
            ),
        )

    def test_build_document_contains_only_successor_start_and_stop(self) -> None:
        document = build_successor_profile_document(
            successor_config=self.successor_config_path,
            successor_config_sha256=self.successor_digest,
            repo_root=self.repo,
        )
        self.assertEqual(1, document["schema_version"])
        self.assertEqual(
            [START_PROFILE_ID, STOP_PROFILE_ID],
            [row["id"] for row in document["profiles"]],
        )
        self.assertEqual(
            {"autonomy.run", "autonomy.request_stop"},
            {row["action"] for row in document["profiles"]},
        )
        for row in document["profiles"]:
            self.assertEqual(
                "$REPO/research/corpus/autonomous/new/controller.json",
                row["parameters"]["config"],
            )
            self.assertEqual(
                self.successor_digest,
                row["parameters"]["expected_config_sha256"],
            )

    def test_materialize_stages_mode_0400_exact_successor_without_activation(self) -> None:
        output = self.workspace / "profiles.successor.json"
        patches = self._patches()
        with patches[0], patches[1], patches[2], patches[3]:
            result = materialize_successor_profile_set(
                repo_root=self.repo,
                current_profiles=self.current_profiles,
                expected_current_profiles_sha256=self.current_digest,
                successor_config=self.successor_config_path,
                expected_successor_config_sha256=self.successor_digest,
                output=output,
            )

        self.assertEqual("successor_profile_set_staged", result["status"])
        self.assertFalse(result["active_profile_set_changed"])
        self.assertFalse(result["processing_started"])
        self.assertEqual("restart_console_with_staged_profile_set", result["activation"])
        self.assertEqual(0o400, output.stat().st_mode & 0o777)
        self.assertEqual(1, output.stat().st_nlink)
        loaded = registry.load_profiles(output)
        self.assertEqual(2, len(loaded.profiles))
        self.assertEqual(self.successor_digest, loaded.profiles[0].parameters["expected_config_sha256"])
        self.assertEqual(
            self.predecessor_digest,
            registry.load_profiles(self.current_profiles)
            .profiles[0]
            .parameters["expected_config_sha256"],
        )

    def test_materialize_rejects_non_stopped_predecessor(self) -> None:
        output = self.workspace / "profiles.successor.json"
        patches = self._patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            mock.patch(
                "operator_console.materialize_successor_profiles.read_public_status",
                side_effect=[self._status("running"), self._status("not_started")],
            ),
        ):
            with self.assertRaisesRegex(
                SuccessorProfileError, "predecessor controller is not quiescent"
            ):
                materialize_successor_profile_set(
                    repo_root=self.repo,
                    current_profiles=self.current_profiles,
                    expected_current_profiles_sha256=self.current_digest,
                    successor_config=self.successor_config_path,
                    expected_successor_config_sha256=self.successor_digest,
                    output=output,
                )
        self.assertFalse(output.exists())

    def test_materialize_rejects_profile_digest_change_and_never_overwrites(self) -> None:
        output = self.workspace / "profiles.successor.json"
        output.write_text("keep\n", encoding="utf-8")
        output.chmod(0o400)
        patches = self._patches()
        with patches[0], patches[1], patches[2], patches[3]:
            with self.assertRaisesRegex(
                SuccessorProfileError, "differs from its reviewed SHA-256"
            ):
                materialize_successor_profile_set(
                    repo_root=self.repo,
                    current_profiles=self.current_profiles,
                    expected_current_profiles_sha256="f" * 64,
                    successor_config=self.successor_config_path,
                    expected_successor_config_sha256=self.successor_digest,
                    output=output,
                )
        self.assertEqual(b"keep\n", output.read_bytes())


if __name__ == "__main__":
    unittest.main()
