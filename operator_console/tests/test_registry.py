from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from operator_console import registry


class RegistryTests(unittest.TestCase):
    def test_local_private_gpu_workflow_is_closed_and_readiness_gated(self) -> None:
        queue = registry.ACTIONS["gpu.preprocess_queue_v1"]
        materialize = registry.ACTIONS["gpu.materialize_local_v1"]
        doctor = registry.ACTIONS["gpu.doctor_local_private"]
        run = registry.ACTIONS["gpu.batch_local_private"]
        self.assertEqual(queue.prefix, ("materialize",))
        self.assertIn("local-private-production", materialize.prefix)
        self.assertEqual(doctor.launcher, "local_private_host")
        self.assertEqual(run.launcher, "local_private_host")
        self.assertEqual(run.supervisor, "systemd_user")
        self.assertEqual(run.memory_max_bytes, 12 * 1024**3)
        self.assertIn("local_readiness", {field.name for field in run.fields})
        self.assertIn(
            "expected_local_readiness_sha256",
            {field.name for field in run.fields},
        )

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-operator-registry-")
        self.repo = Path(self.temporary.name).resolve()
        (self.repo / "research").mkdir()
        (self.repo / "corpus" / "src").mkdir(parents=True)
        self.profile_path = self.repo / "research" / "profiles.json"
        self._write_entrypoint("acquisition/bin/background-producer")
        self._write_entrypoint("acquisition/bin/retain-public-acquisition")
        self._write_entrypoint("acquisition/bin/archive-preprocess-next")
        self._write_entrypoint("acquisition/bin/archive-rolling-pipeline")
        self._write_entrypoint("autonomous_controller/bin/himr-autonomous-controller")
        self._write_entrypoint("pipeline/bin/preprocess-batch")
        self._write_entrypoint("pipeline/bin/preprocess-asr-queue-dispatch")
        self._write_entrypoint("pipeline/bin/preprocess-asr-queue-runner-v03")
        self._write_entrypoint("pipeline/bin/asr-whispercpp-v04-receipt-audit")
        self._write_entrypoint("pipeline/bin/materialize-gpu-asr-batch-v1")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_archive_preprocess_handoff_is_closed_and_bounded(self) -> None:
        action = registry.ACTIONS["archive.preprocess_next"]
        self.assertEqual("preprocess", action.resource)
        self.assertEqual("acquisition/bin/archive-preprocess-next", action.entrypoint)
        self.assertEqual("RUN ARCHIVE PREPROCESS", action.confirmation)
        fields = {field.name: field for field in action.fields}
        self.assertEqual(
            {"schedule", "bundle_root", "processing_output_root", "limit"},
            set(fields),
        )
        self.assertEqual((1, 8), (fields["limit"].minimum, fields["limit"].maximum))
        self.assertNotIn(
            "Acquisition → preprocessing handoff",
            {row["stage"] for row in registry.BLOCKED_CAPABILITIES},
        )

    def test_autonomous_campaign_expands_exact_long_running_envelope(self) -> None:
        config = self.repo / "research" / "autonomy-config.json"
        config.write_text("{}\n", encoding="utf-8")
        digest = "a" * 64
        profiles = self._load_one(
            "autonomy.run",
            {
                "config": "$REPO/research/autonomy-config.json",
                "expected_config_sha256": digest,
            },
            profile_id="autonomy.start",
        )
        with mock.patch.object(registry, "_validate_systemd_user_tools"):
            prepared = registry.prepare_profile(
                profiles, "autonomy.start", self.repo
            )
        entrypoint = (
            self.repo
            / "autonomous_controller"
            / "bin"
            / "himr-autonomous-controller"
        )
        self.assertEqual(
            prepared.argv,
            (
                str(entrypoint),
                "run",
                "--config",
                str(config),
                "--expected-config-sha256",
                digest,
            ),
        )
        self.assertEqual(prepared.action.timeout_seconds, 30 * 24 * 60 * 60)
        self.assertEqual(prepared.action.supervisor, "systemd_user")
        self.assertEqual(prepared.action.memory_max_bytes, 16 * 1024**3)
        self.assertEqual(prepared.action.tasks_max, 256)
        self.assertEqual(prepared.action.file_size_max_bytes, 64 * 1024**3)
        self.assertEqual(prepared.action.stop_timeout_seconds, 30)
        self.assertEqual(prepared.action.memory_swap_max_bytes, 0)

    def test_public_retention_profile_expands_one_exact_object_without_cold_path(self) -> None:
        work_order = self.repo / "research" / "work-order.json"
        work_order.write_text("{}\n", encoding="utf-8")
        acquisition_root = self.repo / "research" / "acquired"
        staging_root = self.repo / "research" / "cold-retention-staging"
        receipt_root = self.repo / "research" / "cold-retention-receipts"
        for directory in (acquisition_root, staging_root, receipt_root):
            directory.mkdir(mode=0o700)
        parameters = {
            "work_order": "$REPO/research/work-order.json",
            "acquisition_root": "$REPO/research/acquired",
            "staging_root": "$REPO/research/cold-retention-staging",
            "receipt_root": "$REPO/research/cold-retention-receipts",
            "expected_work_order_sha256": "1" * 64,
            "expected_result_sha256": "2" * 64,
            "expected_sha256": "3" * 64,
            "expected_byte_count": 123456,
            "free_space_floor_bytes": 107374182400,
        }
        loaded = self._load_one(
            "retention.public_acquisition",
            parameters,
            profile_id="retention.one",
        )
        prepared = registry.prepare_profile(loaded, "retention.one", self.repo)
        entrypoint = self.repo / "acquisition" / "bin" / "retain-public-acquisition"
        self.assertEqual(
            (
                str(entrypoint),
                "run",
                "--work-order",
                str(work_order),
                "--acquisition-root",
                str(acquisition_root),
                "--staging-root",
                str(staging_root),
                "--receipt-root",
                str(receipt_root),
                "--expected-work-order-sha256",
                "1" * 64,
                "--expected-result-sha256",
                "2" * 64,
                "--expected-sha256",
                "3" * 64,
                "--expected-byte-count",
                "123456",
                "--free-space-floor-bytes",
                "107374182400",
            ),
            prepared.argv,
        )
        self.assertEqual("cold_storage", prepared.action.resource)
        self.assertEqual(
            "RETAIN ONE PUBLIC ACQUISITION", prepared.action.confirmation
        )
        self.assertNotIn("/mnt/archive/HIMR", prepared.argv)
        self.assertNotIn(
            "Cold transfer, deletion, publication, and identity",
            {row["stage"] for row in registry.BLOCKED_CAPABILITIES},
        )

        parameters["destination_root"] = "/mnt/archive/HIMR"
        rejected = self._load_one(
            "retention.public_acquisition",
            parameters,
            profile_id="retention.rejected",
        )
        with self.assertRaisesRegex(registry.RegistryError, "unknown parameters"):
            registry.prepare_profile(rejected, "retention.rejected", self.repo)

    def test_archive_preprocess_profile_expands_to_exact_fixed_argv(self) -> None:
        schedule = self.repo / "research" / "schedule.json"
        schedule.write_text("{}\n", encoding="utf-8")
        bundle_root = self.repo / "research" / "archive-preprocess-control"
        output_root = self.repo / "research" / "archive-preprocess-output"
        bundle_root.mkdir(mode=0o700)
        output_root.mkdir(mode=0o700)
        loaded = self._load_one(
            "archive.preprocess_next",
            {
                "schedule": "$REPO/research/schedule.json",
                "bundle_root": "$REPO/research/archive-preprocess-control",
                "processing_output_root": "$REPO/research/archive-preprocess-output",
                "limit": 4,
            },
            profile_id="archive.preprocess",
        )
        prepared = registry.prepare_profile(
            loaded, "archive.preprocess", self.repo
        )
        entrypoint = self.repo / "acquisition" / "bin" / "archive-preprocess-next"
        self.assertEqual(
            (
                str(entrypoint),
                "--schedule",
                str(schedule),
                "--bundle-root",
                str(bundle_root),
                "--processing-output-root",
                str(output_root),
                "--limit",
                "4",
            ),
            prepared.argv,
        )
        self.assertEqual("preprocess", prepared.action.resource)

    def test_rolling_archive_profile_has_fixed_dual_lane_contract(self) -> None:
        schedule = self.repo / "research" / "schedule.json"
        schedule.write_text("{}\n", encoding="utf-8")
        bundle_root = self.repo / "research" / "rolling-control"
        output_root = self.repo / "research" / "rolling-output"
        bundle_root.mkdir(mode=0o700)
        output_root.mkdir(mode=0o700)
        parameters = {
            "schedule": "$REPO/research/schedule.json",
            "bundle_root": "$REPO/research/rolling-control",
            "processing_output_root": "$REPO/research/rolling-output",
            "max_new_items": 4,
            "max_new_bytes": 8589934592,
            "max_run_seconds": 14400,
            "free_space_floor_bytes": 137438953472,
            "max_preprocess_items": 8,
        }
        loaded = self._load_one(
            "archive.rolling_pipeline",
            parameters,
            profile_id="archive.rolling",
        )
        prepared = registry.prepare_profile(loaded, "archive.rolling", self.repo)
        entrypoint = self.repo / "acquisition" / "bin" / "archive-rolling-pipeline"
        self.assertEqual("archive_pipeline", prepared.action.resource)
        self.assertEqual("RUN ROLLING ARCHIVE PIPELINE", prepared.action.confirmation)
        self.assertEqual(
            (
                str(entrypoint),
                "--schedule",
                str(schedule),
                "--bundle-root",
                str(bundle_root),
                "--processing-output-root",
                str(output_root),
                "--max-new-items",
                "4",
                "--max-new-bytes",
                "8589934592",
                "--max-run-seconds",
                "14400",
                "--free-space-floor-bytes",
                "137438953472",
                "--max-preprocess-items",
                "8",
            ),
            prepared.argv,
        )

    def _write_entrypoint(self, relative: str, body: bytes = b"#!/bin/sh\nexit 0\n") -> Path:
        path = self.repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() or path.is_symlink():
            path.unlink()
        path.write_bytes(body)
        path.chmod(0o700)
        return path

    def _write_profiles(
        self,
        profiles: list[dict[str, object]],
        *,
        mode: int = 0o400,
    ) -> tuple[Path, bytes]:
        body = (
            json.dumps(
                {"schema_version": registry.PROFILE_SCHEMA_VERSION, "profiles": profiles},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        return self._write_raw_profile(body, mode=mode), body

    def _write_raw_profile(self, body: bytes, *, mode: int = 0o400) -> Path:
        if self.profile_path.exists() or self.profile_path.is_symlink():
            self.profile_path.unlink()
        self.profile_path.write_bytes(body)
        self.profile_path.chmod(mode)
        return self.profile_path

    @staticmethod
    def _profile(
        profile_id: str,
        action: str,
        parameters: dict[str, object],
    ) -> dict[str, object]:
        return {
            "id": profile_id,
            "label": f"Label {profile_id}",
            "description": f"Description for {profile_id}",
            "action": action,
            "parameters": parameters,
        }

    def _load_one(
        self,
        action: str,
        parameters: dict[str, object],
        *,
        profile_id: str = "profile.one",
    ) -> registry.ProfileSet:
        self._write_profiles([self._profile(profile_id, action, parameters)])
        return registry.load_profiles(self.profile_path)

    def test_strict_profile_json_and_exact_mode(self) -> None:
        path, body = self._write_profiles(
            [
                self._profile(
                    "preprocess.check",
                    "preprocess.validate_bundle",
                    {"bundle": "$REPO/research/bundle"},
                )
            ]
        )
        (self.repo / "research" / "bundle").mkdir()
        loaded = registry.load_profiles(path)
        self.assertEqual(loaded.raw_sha256, hashlib.sha256(body).hexdigest())
        self.assertEqual(loaded.byte_count, len(body))
        self.assertEqual([item.profile_id for item in loaded.profiles], ["preprocess.check"])

        path.chmod(0o600)
        with self.assertRaisesRegex(registry.RegistryError, "exact mode 0400"):
            registry.load_profiles(path)

    def test_duplicate_json_keys_and_nonfinite_values_are_rejected(self) -> None:
        invalid_documents = (
            b'{"schema_version":1,"schema_version":1,"profiles":[]}\n',
            b'{"schema_version":1,"profiles":[{"id":"dup.one","label":"x",'
            b'"description":"","action":"gpu.batch","parameters":{},"parameters":{}}]}\n',
            b'{"schema_version":1,"profiles":[],"extra":NaN}\n',
        )
        for index, body in enumerate(invalid_documents):
            with self.subTest(index=index):
                self._write_raw_profile(body)
                with self.assertRaises(registry.RegistryError):
                    registry.load_profiles(self.profile_path)

    def test_profile_and_action_shapes_are_closed(self) -> None:
        cases = (
            {"schema_version": 2, "profiles": []},
            {"schema_version": 1, "profiles": [], "extra": False},
            {
                "schema_version": 1,
                "profiles": [
                    {
                        **self._profile("unknown.action", "not.registered", {}),
                    }
                ],
            },
            {
                "schema_version": 1,
                "profiles": [
                    {
                        **self._profile("bad.shape", "gpu.batch", {}),
                        "extra": "not allowed",
                    }
                ],
            },
            {
                "schema_version": 1,
                "profiles": [
                    self._profile("duplicate.id", "gpu.batch", {}),
                    self._profile("duplicate.id", "gpu.batch", {}),
                ],
            },
        )
        for index, document in enumerate(cases):
            with self.subTest(index=index):
                self._write_raw_profile((json.dumps(document) + "\n").encode())
                with self.assertRaises(registry.RegistryError):
                    registry.load_profiles(self.profile_path)

    def test_parameter_allowlist_missing_required_and_forbidden_name(self) -> None:
        bundle = self.repo / "research" / "bundle"
        bundle.mkdir()
        cases = (
            {"bundle": "$REPO/research/bundle", "arbitrary": "value"},
            {},
            {"bundle": "$REPO/research/bundle", "api_token": "value"},
        )
        for index, parameters in enumerate(cases):
            with self.subTest(index=index):
                profiles = self._load_one("preprocess.validate_bundle", parameters)
                with self.assertRaises(registry.RegistryError):
                    registry.prepare_profile(profiles, "profile.one", self.repo)

    def test_exact_repo_argv_and_closed_environment(self) -> None:
        bundle = self.repo / "research" / "bundle"
        state = self.repo / "research" / "preprocess-state"
        bundle.mkdir()
        state.mkdir()
        profiles = self._load_one(
            "preprocess.dry_run",
            {
                "bundle": "$REPO/research/bundle",
                "state_root": "$REPO/research/preprocess-state",
                "limit": 3,
                "dry_run": True,
            },
        )
        sentinel = "must-not-be-inherited"
        with mock.patch.dict(
            os.environ,
            {
                "HIMR_SECRET_SENTINEL": sentinel,
                "DISCORD_TOKEN": sentinel,
                "HTTP_PROXY": sentinel,
            },
            clear=False,
        ):
            prepared = registry.prepare_profile(profiles, "profile.one", self.repo)

        entrypoint = self.repo / "pipeline" / "bin" / "preprocess-batch"
        self.assertEqual(
            prepared.argv,
            (
                str(entrypoint),
                "run",
                "--bundle",
                str(bundle),
                "--state-root",
                str(state),
                "--limit",
                "3",
                "--dry-run",
            ),
        )
        self.assertEqual(prepared.cwd, self.repo)
        self.assertEqual(
            dict(prepared.environment),
            {
                "PATH": "/usr/bin:/bin",
                "HOME": "/nonexistent",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "TZ": "UTC",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
                "HF_HUB_OFFLINE": "1",
                "HF_DATASETS_OFFLINE": "1",
                "HF_HUB_DISABLE_TELEMETRY": "1",
                "DO_NOT_TRACK": "1",
            },
        )
        self.assertNotIn("HIMR_SECRET_SENTINEL", prepared.environment)
        self.assertNotIn("DISCORD_TOKEN", prepared.environment)
        self.assertNotIn("HTTP_PROXY", prepared.environment)
        self.assertNotIn(sentinel, prepared.environment.values())
        self.assertEqual(prepared.entrypoint_path, entrypoint)
        self.assertEqual(
            prepared.entrypoint_sha256,
            hashlib.sha256(entrypoint.read_bytes()).hexdigest(),
        )

    def test_exact_corpus_argv_and_environment(self) -> None:
        database = self.repo / "research" / "catalog.sqlite3"
        database.write_bytes(b"not opened by the registry test")
        profiles = self._load_one(
            "catalog.validate",
            {"database": "$REPO/research/catalog.sqlite3"},
        )
        prepared = registry.prepare_profile(profiles, "profile.one", self.repo)
        python = Path(sys.executable).resolve(strict=True)
        self.assertEqual(
            prepared.argv,
            (
                str(python),
                "-B",
                "-m",
                "himr_corpus",
                "validate",
                "--db",
                str(database),
            ),
        )
        self.assertEqual(prepared.entrypoint_path, python)
        self.assertEqual(prepared.environment["PYTHONPATH"], str(self.repo / "corpus" / "src"))
        self.assertEqual(set(prepared.environment), {
            "PATH", "HOME", "LANG", "LC_ALL", "TZ", "PYTHONDONTWRITEBYTECODE",
            "PYTHONNOUSERSITE", "HF_HUB_OFFLINE", "HF_DATASETS_OFFLINE",
            "HF_HUB_DISABLE_TELEMETRY", "DO_NOT_TRACK", "PYTHONPATH",
        })

    def test_lexical_traversal_public_control_tmp_and_archive_paths_are_rejected(self) -> None:
        forbidden = (
            "$REPO/research/../../escape",
            "$REPO/public/input.json",
            "$REPO/src/input.json",
            "$REPO/dist/input.json",
            "$REPO/.git/config",
            "/tmp/himr-operator-input.json",
            "/mnt/archive/HIMR/do-not-inspect/result.json",
        )
        original_lstat = Path.lstat

        def guarded_lstat(path: Path):
            if str(path).startswith("/mnt/archive/HIMR"):
                raise AssertionError("cold archive path was inspected")
            return original_lstat(path)

        with mock.patch.object(Path, "lstat", guarded_lstat):
            for index, value in enumerate(forbidden):
                with self.subTest(index=index, value=value):
                    profiles = self._load_one("acquisition.validate", {"schedule": value})
                    with self.assertRaises(registry.RegistryError):
                        registry.prepare_profile(profiles, "profile.one", self.repo)

    def test_symlink_components_fifo_and_nonresearch_private_roots_are_rejected(self) -> None:
        target = self.repo / "research" / "target"
        target.mkdir()
        (target / "schedule.json").write_text("{}\n")
        link = self.repo / "research" / "linked"
        link.symlink_to(target, target_is_directory=True)
        profiles = self._load_one(
            "acquisition.validate",
            {"schedule": "$REPO/research/linked/schedule.json"},
        )
        with self.assertRaisesRegex(registry.RegistryError, "symlink component"):
            registry.prepare_profile(profiles, "profile.one", self.repo)

        fifo = self.repo / "research" / "schedule.fifo"
        os.mkfifo(fifo, 0o600)
        profiles = self._load_one(
            "acquisition.validate",
            {"schedule": "$REPO/research/schedule.fifo"},
        )
        with self.assertRaisesRegex(registry.RegistryError, "regular file"):
            registry.prepare_profile(profiles, "profile.one", self.repo)

        bundle = self.repo / "research" / "bundle"
        bundle.mkdir()
        nonprivate = self.repo / "operator-state"
        nonprivate.mkdir()
        profiles = self._load_one(
            "preprocess.status",
            {
                "bundle": "$REPO/research/bundle",
                "state_root": "$REPO/operator-state",
            },
        )
        with self.assertRaisesRegex(registry.RegistryError, r"below \$REPO/research"):
            registry.prepare_profile(profiles, "profile.one", self.repo)

    def test_profile_leaf_symlink_and_fifo_are_rejected_without_blocking(self) -> None:
        target = self.repo / "research" / "real-profiles.json"
        target.write_text('{"schema_version":1,"profiles":[]}\n')
        target.chmod(0o400)
        self.profile_path.symlink_to(target)
        with self.assertRaises(registry.RegistryError):
            registry.load_profiles(self.profile_path)

        self.profile_path.unlink()
        os.mkfifo(self.profile_path, 0o400)
        with self.assertRaisesRegex(registry.RegistryError, "bounded regular file"):
            registry.load_profiles(self.profile_path)

    def test_blocked_gpu_action_cannot_be_prepared(self) -> None:
        profiles = self._load_one("gpu.batch", {})
        with self.assertRaisesRegex(registry.RegistryError, "runtime.expected_device"):
            registry.prepare_profile(profiles, "profile.one", self.repo)
        public = {item["action_id"]: item for item in registry.public_actions()}
        self.assertFalse(public["gpu.batch"]["enabled"])
        self.assertIn("reboot-unstable", public["gpu.batch"]["blocked_reason"])

    def test_gpu_v2_action_uses_trusted_launcher_hashes_and_systemd_envelope(self) -> None:
        launcher = self._write_entrypoint("trusted-launcher-v2")
        for name in ("batch.json", "profile.json", "root.json"):
            (self.repo / "research" / name).write_text("{}\n")
        for name in ("results", "events", "locks"):
            (self.repo / "research" / name).mkdir()
        digest = "a" * 64
        parameters = {
            "batch_manifest": "$REPO/research/batch.json",
            "expected_batch_sha256": digest,
            "expected_runtime_admission_sha256": digest,
            "production_profile": "$REPO/research/profile.json",
            "expected_production_profile_sha256": digest,
            "root_registration": "$REPO/research/root.json",
            "expected_root_registration_sha256": digest,
            "expected_launcher_profile_sha256": digest,
            "writable_result_root": "$REPO/research/results",
            "writable_event_root": "$REPO/research/events",
            "writable_lock_root": "$REPO/research/locks",
        }
        original = registry.ACTIONS["gpu.batch_v2"]
        action = registry.ActionSpec(
            **{**original.__dict__, "entrypoint": str(launcher)}
        )
        profiles = self._load_one("gpu.batch_v2", parameters)
        with (
            mock.patch.dict(registry.ACTIONS, {"gpu.batch_v2": action}, clear=False),
            mock.patch.object(registry, "TRUSTED_GPU_LAUNCHER", launcher),
            mock.patch.object(registry, "_validate_trusted_host_entrypoint"),
            mock.patch.object(registry, "_validate_systemd_user_tools"),
        ):
            prepared = registry.prepare_profile(profiles, "profile.one", self.repo)
        self.assertEqual(prepared.argv[0], str(launcher))
        self.assertEqual(prepared.argv[1:9], (
            "run", "--mode", "production",
            "--runtime-admission", "/etc/himr-gpu/runtime-admission-v2.json",
            "--launcher-profile", "/etc/himr-gpu/launcher-profile-v2.json",
            "--batch-manifest",
        ))
        self.assertEqual(prepared.action.supervisor, "systemd_user")
        self.assertEqual(prepared.action.memory_max_bytes, 12 * 1024**3)
        self.assertEqual(prepared.action.confirmation, "RUN GPU ASR")
        self.assertNotIn("PYTHONPATH", prepared.environment)

        parameters["expected_batch_sha256"] = "A" * 64
        invalid = self._load_one("gpu.batch_v2", parameters)
        with (
            mock.patch.dict(registry.ACTIONS, {"gpu.batch_v2": action}, clear=False),
            mock.patch.object(registry, "TRUSTED_GPU_LAUNCHER", launcher),
            mock.patch.object(registry, "_validate_trusted_host_entrypoint"),
            mock.patch.object(registry, "_validate_systemd_user_tools"),
        ):
            with self.assertRaisesRegex(registry.RegistryError, "lowercase SHA-256"):
                registry.prepare_profile(invalid, "profile.one", self.repo)

    def test_gpu_materializer_expands_only_sorted_explicit_ordinals(self) -> None:
        for name in ("queue.json", "root.json", "runtime.json", "profile.json"):
            (self.repo / "research" / name).write_text("{}\n")
        for name in ("orders", "receipts", "results", "batches", "events", "locks"):
            (self.repo / "research" / name).mkdir()
        digest = "a" * 64
        parameters = {
            "queue_manifest": "$REPO/research/queue.json",
            "expected_queue_sha256": digest,
            "root_registration": "$REPO/research/root.json",
            "expected_root_registration_sha256": digest,
            "runtime_admission": "$REPO/research/runtime.json",
            "expected_runtime_admission_sha256": digest,
            "production_profile": "$REPO/research/profile.json",
            "expected_production_profile_sha256": digest,
            "queue_ordinals": [1, 3, 9],
            "work_order_root": "$REPO/research/orders",
            "receipt_root": "$REPO/research/receipts",
            "result_root": "$REPO/research/results",
            "batch_root": "$REPO/research/batches",
            "event_root": "$REPO/research/events",
            "lock_root": "$REPO/research/locks",
        }
        profiles = self._load_one("gpu.materialize_v1", parameters)
        prepared = registry.prepare_profile(profiles, "profile.one", self.repo)
        ordinal_positions = [
            index for index, value in enumerate(prepared.argv) if value == "--queue-ordinal"
        ]
        self.assertEqual(
            [prepared.argv[index + 1] for index in ordinal_positions],
            ["1", "3", "9"],
        )
        self.assertEqual(prepared.action.confirmation, "MATERIALIZE GPU BATCH")
        self.assertEqual(prepared.action.resource, "cpu_asr")

        parameters["queue_ordinals"] = [3, 1]
        invalid = self._load_one("gpu.materialize_v1", parameters)
        with self.assertRaisesRegex(registry.RegistryError, "sorted unique"):
            registry.prepare_profile(invalid, "profile.one", self.repo)

    def test_systemd_supervisor_requires_and_publishes_exact_memory_cap(self) -> None:
        action_id = "gpu.vnext.fixture"
        base = registry.ActionSpec(
            action_id=action_id,
            stage="GPU fixture",
            label="GPU fixture",
            description="Fixture only.",
            effect="execute",
            resource="gpu",
            launcher="repo",
            entrypoint="pipeline/bin/preprocess-batch",
            prefix=(),
            fields=(),
            confirmation="RUN GPU FIXTURE",
            timeout_seconds=900,
            supervisor="systemd_user",
            memory_max_bytes=4 * 1024 * 1024 * 1024,
        )
        with mock.patch.dict(registry.ACTIONS, {action_id: base}, clear=False):
            profiles = self._load_one(action_id, {})
            prepared = registry.prepare_profile(profiles, "profile.one", self.repo)
            self.assertEqual(prepared.action.supervisor, "systemd_user")
            public = {item["action_id"]: item for item in registry.public_actions()}
            self.assertEqual(public[action_id]["supervisor"], "systemd_user")

        missing_cap = registry.ActionSpec(
            **{**base.__dict__, "memory_max_bytes": None}
        )
        with mock.patch.dict(
            registry.ACTIONS, {action_id: missing_cap}, clear=False
        ):
            profiles = self._load_one(action_id, {})
            with self.assertRaisesRegex(registry.RegistryError, "MemoryMax"):
                registry.prepare_profile(profiles, "profile.one", self.repo)

    def test_v03_runner_inspection_is_exact_and_real_dispatch_is_blocked(self) -> None:
        manifest = self.repo / "research" / "queue-v03.json"
        manifest.write_text("{}\n")
        profiles = self._load_one(
            "asr_queue.runner_v03_dry_run",
            {"manifest": "$REPO/research/queue-v03.json", "dry_run": True},
        )
        prepared = registry.prepare_profile(profiles, "profile.one", self.repo)
        entrypoint = self.repo / "pipeline" / "bin" / "preprocess-asr-queue-runner-v03"
        self.assertEqual(
            prepared.argv,
            (str(entrypoint), "run", "--manifest", str(manifest), "--dry-run"),
        )
        self.assertEqual(prepared.action.resource, "cpu_asr")
        self.assertEqual(prepared.action.confirmation, "DRY RUN WHOLE ASR V03")

        blocked = self._load_one(
            "asr_queue.runner_v03_run",
            {"manifest": "$REPO/research/queue-v03.json"},
        )
        with self.assertRaisesRegex(registry.RegistryError, "bounded-prefix"):
            registry.prepare_profile(blocked, "profile.one", self.repo)
        public = {item["action_id"]: item for item in registry.public_actions()}
        self.assertFalse(public["asr_queue.runner_v03_run"]["enabled"])

    def test_integer_caps_and_boolean_type_are_enforced(self) -> None:
        schedule = self.repo / "research" / "schedule.json"
        schedule.write_text("{}\n")
        valid = {
            "schedule": "$REPO/research/schedule.json",
            "max_new_items": 64,
            "max_new_bytes": 2**63 - 1,
            "max_run_seconds": 14_400,
            "free_space_floor_bytes": 2**63 - 1,
        }
        profiles = self._load_one("acquisition.run", valid)
        prepared = registry.prepare_profile(profiles, "profile.one", self.repo)
        self.assertEqual(prepared.argv[-8:], (
            "--max-new-items", "64", "--max-new-bytes", str(2**63 - 1),
            "--max-run-seconds", "14400", "--free-space-floor-bytes", str(2**63 - 1),
        ))

        invalid_variants = (
            {**valid, "max_new_items": 0},
            {**valid, "max_new_items": 65},
            {**valid, "max_new_items": True},
            {**valid, "max_new_bytes": 2**63},
            {**valid, "max_run_seconds": 14_401},
            {**valid, "free_space_floor_bytes": 0},
        )
        for index, parameters in enumerate(invalid_variants):
            with self.subTest(index=index):
                profiles = self._load_one("acquisition.run", parameters)
                with self.assertRaises(registry.RegistryError):
                    registry.prepare_profile(profiles, "profile.one", self.repo)

        bundle = self.repo / "research" / "bundle"
        state = self.repo / "research" / "state"
        bundle.mkdir()
        state.mkdir()
        for value, accepted in ((1, True), (32, True), (33, False), (False, False)):
            with self.subTest(preprocess_limit=value):
                profiles = self._load_one(
                    "preprocess.run",
                    {
                        "bundle": "$REPO/research/bundle",
                        "state_root": "$REPO/research/state",
                        "limit": value,
                    },
                )
                if accepted:
                    self.assertIsInstance(
                        registry.prepare_profile(profiles, "profile.one", self.repo),
                        registry.PreparedCommand,
                    )
                else:
                    with self.assertRaises(registry.RegistryError):
                        registry.prepare_profile(profiles, "profile.one", self.repo)

        profiles = self._load_one(
            "preprocess.dry_run",
            {
                "bundle": "$REPO/research/bundle",
                "state_root": "$REPO/research/state",
                "limit": 1,
                "dry_run": False,
            },
        )
        with self.assertRaisesRegex(registry.RegistryError, "must be true"):
            registry.prepare_profile(profiles, "profile.one", self.repo)

    def test_profile_path_replacement_during_read_is_rejected(self) -> None:
        self._write_profiles([])
        replacement = self.profile_path.with_name("profiles.replacement.json")
        replacement.write_text('{"schema_version":1,"profiles":[]}\n')
        replacement.chmod(0o400)
        original_read = registry.os.read
        replaced = False

        def replacing_read(descriptor: int, count: int) -> bytes:
            nonlocal replaced
            if not replaced:
                os.replace(replacement, self.profile_path)
                replaced = True
            return original_read(descriptor, count)

        with mock.patch.object(registry.os, "read", replacing_read):
            with self.assertRaisesRegex(
                registry.RegistryError, r"changed while being read|pathname changed"
            ):
                registry.load_profiles(self.profile_path)

    def test_entrypoint_replacement_during_read_is_rejected(self) -> None:
        bundle = self.repo / "research" / "bundle"
        bundle.mkdir()
        profiles = self._load_one(
            "preprocess.validate_bundle",
            {"bundle": "$REPO/research/bundle"},
        )
        entrypoint = self.repo / "pipeline" / "bin" / "preprocess-batch"
        replacement = entrypoint.with_name("preprocess-batch.replacement")
        replacement.write_bytes(b"#!/bin/sh\nexit 7\n")
        replacement.chmod(0o700)
        original_read = registry.os.read
        replaced = False

        def replacing_read(descriptor: int, count: int) -> bytes:
            nonlocal replaced
            if not replaced:
                os.replace(replacement, entrypoint)
                replaced = True
            return original_read(descriptor, count)

        with mock.patch.object(registry.os, "read", replacing_read):
            with self.assertRaisesRegex(
                registry.RegistryError, r"changed while being read|pathname changed"
            ):
                registry.prepare_profile(profiles, "profile.one", self.repo)

    def test_entrypoint_symlink_replacement_is_rejected(self) -> None:
        bundle = self.repo / "research" / "bundle"
        bundle.mkdir()
        profiles = self._load_one(
            "preprocess.validate_bundle",
            {"bundle": "$REPO/research/bundle"},
        )
        entrypoint = self.repo / "pipeline" / "bin" / "preprocess-batch"
        target = self.repo / "research" / "replacement-executable"
        target.write_bytes(b"#!/bin/sh\nexit 0\n")
        target.chmod(0o700)
        entrypoint.unlink()
        entrypoint.symlink_to(target)
        with self.assertRaisesRegex(registry.RegistryError, "symlink component"):
            registry.prepare_profile(profiles, "profile.one", self.repo)


if __name__ == "__main__":
    unittest.main()
