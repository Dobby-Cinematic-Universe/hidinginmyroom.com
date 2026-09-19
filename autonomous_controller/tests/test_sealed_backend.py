from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from autonomous_controller.controller import (
    TRANSIENT_PEER_RUNTIME_ARTIFACT,
    AutonomousController,
    StageOutcome,
    StartupRestoreStopRequested,
)
from autonomous_controller.gpu_child import (
    BatchResultStatus,
    ControllerUnitContext,
    GpuChildRecord,
    PrivateGpuChildJournal,
    SystemdGpuChildExecutor,
)
from autonomous_controller.sealed_backend import (
    ACQUISITION_LEGACY_FINALIZATION_WINDOW_MS,
    ACQUISITION_LEGACY_VERSION,
    ACQUISITION_RESULT_TIME_WITNESS_TOLERANCE_MS,
    BACKEND_CHECKPOINT_KIND,
    BACKEND_CHECKPOINT_SCHEMA_VERSION,
    BACKEND_KIND,
    BACKEND_RESTART_POLICY,
    GPU_PACK_RECORD_FORMAT,
    PREPROCESS_RECEIPT_SNAPSHOT_ATTEMPTS,
    QUEUE_RUNTIME_SNAPSHOT_ATTEMPTS,
    QUEUE_RUNTIME_SNAPSHOT_RETRY_SECONDS,
    QUEUE_RUNNER_LEGACY_VERSION,
    BackendError,
    SealedArchiveBackend,
    _checkpoint_fingerprint,
    _checkpoint_fingerprint_matches,
    _install_acquisition_directory_identity_adapter,
    _install_acquisition_result_finalization_adapter,
    _install_preprocess_receipt_snapshot_adapter,
)
from autonomous_controller.config import (
    ControllerConfig,
    build_config,
    canonical_bytes,
    sha256_bytes,
)
from autonomous_controller.state import ControlStore
from autonomous_controller.operational_replay import DeepAuditRequired
from autonomous_controller.tests.test_controller import config_core, make_config
from autonomous_controller.tests.test_operational_replay import QueueFixture
from autonomous_controller.tests.test_gpu_child import (
    CHILD_INVOCATION,
    OUTER_INVOCATION,
    OUTER_UNIT,
    FakeSystemd,
)


class FakeBackground:
    def __init__(self) -> None:
        self.call = None

    def run_producer(self, path, **kwargs):
        self.call = (path, kwargs)
        return {
            "status": "bounded",
            "stop_reason": "max_new_items",
            "ready_after": {
                "completed_acquisition_count": 1,
                "ready_item_count": 1,
                "ready_byte_count": 123,
                "items": [{"ordinal": 1, "media_byte_count": 123}],
            },
            "queue_summary": {"new_item_count": 1, "new_byte_count": 123},
        }

    def _runtime_from_queue_summary(self, schedule, summary, **_kwargs):
        return {}, [{}], {
            "completed_acquisition_count": 1,
            "ready_item_count": 1,
            "ready_byte_count": 123,
            "items": [{"ordinal": 1, "media_byte_count": 123}],
            "zone": "at_or_below_low_water",
        }

    @staticmethod
    def _completed_state(state):
        return isinstance(state, dict) and state.get("queue_state") != "quarantined"

    @staticmethod
    def _quarantined_state(state):
        return isinstance(state, dict) and state.get("queue_state") == "quarantined"


def fake_queue_runner() -> SimpleNamespace:
    class QueueDeadlineError(Exception):
        pass

    return SimpleNamespace(
        QueueDeadlineError=QueueDeadlineError,
        canonical_bytes=lambda value: json.dumps(
            value, sort_keys=True, separators=(",", ":")
        ).encode(),
        sha256_bytes=lambda value: hashlib.sha256(value).hexdigest(),
        _capacity_allows=lambda *_args, **_kwargs: True,
        _dispatch_one=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("fake queue dispatch was not expected")
        ),
        _reservation_bytes=lambda order: order.get("limits", {}).get(
            "max_job_bytes", 0
        ),
    )


class FakeChildJournal:
    def __init__(self, records=()) -> None:
        self.records = tuple(records)

    def list_records(self):
        return self.records


class FakeChildExecutor:
    def __init__(self, records=()) -> None:
        self.context = SimpleNamespace(
            outer_unit="himr-operator-job-" + "1" * 32 + ".service",
            outer_invocation_id="2" * 32,
        )
        self.journal = FakeChildJournal(records)
        self.launched = []
        self.stopped = []

    def launch(self, spec):
        self.launched.append(spec)
        return GpuChildRecord(
            unit_name=(
                "himr-autonomy-gpu-"
                + self.context.outer_invocation_id
                + "-"
                + spec.expected_batch_sha256
                + f"-{spec.attempt_ordinal:06d}.service"
            ),
            outer_unit=self.context.outer_unit,
            outer_invocation_id=self.context.outer_invocation_id,
            child_invocation_id="3" * 32,
            batch_id=spec.batch_id,
            batch_sha256=spec.expected_batch_sha256,
            spec_identity_sha256="4" * 64,
            attempt_ordinal=spec.attempt_ordinal,
            state="running",
            created_at="2026-08-29T00:00:00Z",
            updated_at="2026-08-29T00:00:00Z",
            launch_accepted_at="2026-08-29T00:00:00Z",
            started_at="2026-08-29T00:00:00Z",
            completed_at=None,
            stop_requested_at=None,
            returncode=None,
            systemd_result=None,
            result=None,
            error=None,
        )

    def reconcile(self, _spec):
        raise AssertionError("new-child fixture should launch, not reconcile")

    def stop(self, spec):
        self.stopped.append(spec)
        running = self.launch(spec)
        return GpuChildRecord(
            **{
                **running.__dict__,
                "state": "stopped",
                "completed_at": "2026-08-29T00:00:01Z",
                "stop_requested_at": "2026-08-29T00:00:01Z",
                "child_invocation_id": "3" * 32,
            }
        )


def gpu_ready_record(key: str, suffix: str) -> dict:
    return {
        "record_kind": "ready_batch",
        "batch_key": key,
        "batch": {
            "path": f"/private/batch-{suffix}.json",
            "sha256": suffix * 64,
            "batch_id": "gpuasrbatch2_" + suffix * 32,
        },
    }


def gpu_child_record(
    backend: SealedArchiveBackend,
    record: dict,
    ordinal: int,
    state: str,
    *,
    outer_unit: str = "himr-operator-job-" + "1" * 32 + ".service",
    outer_invocation: str = "2" * 32,
) -> GpuChildRecord:
    terminal = state in {"failed", "reconciliation_required"}
    result = (
        {
            "status": "pending",
            "batch_id": record["batch"]["batch_id"],
            "completed_ordinals": [],
            "absent_ordinals": [1],
            "invalid": [],
        }
        if state == "failed"
        else None
    )
    return GpuChildRecord(
        unit_name=(
            f"himr-autonomy-gpu-{outer_invocation}-"
            f"{record['batch']['sha256']}-{ordinal:06d}.service"
        ),
        outer_unit=outer_unit,
        outer_invocation_id=outer_invocation,
        child_invocation_id=(
            "3" * 32
            if state in {"running", "retiring", "stopping", "failed"}
            else None
        ),
        batch_id=record["batch"]["batch_id"],
        batch_sha256=record["batch"]["sha256"],
        spec_identity_sha256=backend._gpu_launch_spec(
            record, ordinal
        ).identity_sha256,
        attempt_ordinal=ordinal,
        state=state,
        created_at="2026-08-29T00:00:00Z",
        updated_at="2026-08-29T00:00:01Z",
        launch_accepted_at="2026-08-29T00:00:00Z",
        started_at=(
            "2026-08-29T00:00:00Z"
            if state in {"running", "retiring", "stopping", "failed"}
            else None
        ),
        completed_at="2026-08-29T00:00:01Z" if terminal else None,
        stop_requested_at=None,
        returncode=2 if state == "failed" else None,
        systemd_result="exit-code" if state == "failed" else None,
        result=result,
        error="worker failed" if terminal else None,
    )

class SealedBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name).resolve()
        (root / "state").mkdir(mode=0o700)
        self.config = make_config(root)
        self.gpu_uuid = "GPU-01234567-89ab-cdef-0123-456789abcdef"
        Path(self.config.section("gpu_readiness")["lock_root"]).mkdir(mode=0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _bind_gpu_profile(self, backend: SealedArchiveBackend) -> None:
        backend._profile = {"hardware": {"gpu_uuid": self.gpu_uuid}}

    def _gpu_child_journal(self) -> PrivateGpuChildJournal:
        root = Path(self.config.section("gpu_readiness")["child_journal_root"])
        root.mkdir(parents=True, mode=0o700, exist_ok=True)
        return PrivateGpuChildJournal(root)

    def _checkpoint_ready_backend(self):
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(queue_runner=fake_queue_runner()),
        )

        def export_restart_checkpoint(**binding):
            return {"kind": "fixture_queue_restart", **binding}

        store = SimpleNamespace(
            export_restart_checkpoint=mock.Mock(
                side_effect=export_restart_checkpoint
            ),
            prepare_restart_checkpoint=mock.Mock(
                return_value={"prepared": True}
            ),
        )
        backend._operational_replay_store = store
        backend._campaign_coverage_checkpoint = {"sealed": True}
        backend._schedule_set_coverage_checkpoint = {"sealed": True}
        backend._cold_storage_identity_checkpoint = {"sealed": True}
        backend._checkpoint_queue_snapshots = {
            row["schedule_id"]: {
                "generation": 1,
                "state_digest": "d" * 64,
            }
            for row in self.config.section("campaign")["schedules"]
        }
        return backend, store

    def _empty_restart_checkpoint(self):
        backend, store = self._checkpoint_ready_backend()
        with (
            mock.patch.object(
                backend,
                "_checkpoint_timestamp",
                return_value="2026-08-31T12:34:56Z",
            ),
            mock.patch.object(
                backend,
                "_export_preprocess_checkpoint",
                return_value={"failure_attempts": [], "candidates": []},
            ),
            mock.patch.object(
                backend,
                "_export_gpu_checkpoint",
                return_value={"records": []},
            ),
        ):
            document = backend.export_checkpoint()
        return document, store

    @contextmanager
    def _hold_gpu_opportunity(self, backend: SealedArchiveBackend):
        path = backend._gpu_opportunity_path()
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield descriptor
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def test_restart_checkpoint_export_has_bound_canonical_envelope(self) -> None:
        document, store = self._empty_restart_checkpoint()
        campaign = self.config.section("campaign")
        expected_keys = {
            "kind",
            "schema_version",
            "backend_kind",
            "config_id",
            "config_sha256",
            "campaign_id",
            "schedule_set_id",
            "created_at",
            "queue_replay",
            "preprocess",
            "gpu",
            "cold_retention",
            "policy",
            "identity_sha256",
        }
        self.assertEqual(expected_keys, set(document))
        self.assertEqual(BACKEND_CHECKPOINT_KIND, document["kind"])
        self.assertEqual(
            BACKEND_CHECKPOINT_SCHEMA_VERSION, document["schema_version"]
        )
        self.assertEqual(BACKEND_KIND, document["backend_kind"])
        self.assertEqual(self.config.config_id, document["config_id"])
        self.assertEqual(self.config.physical_sha256, document["config_sha256"])
        self.assertEqual(campaign["campaign_id"], document["campaign_id"])
        self.assertEqual(
            campaign["schedule_set"]["schedule_set_id"],
            document["schedule_set_id"],
        )
        self.assertEqual(BACKEND_RESTART_POLICY, document["policy"])
        core = {
            key: value
            for key, value in document.items()
            if key != "identity_sha256"
        }
        self.assertEqual(
            sha256_bytes(canonical_bytes(core)), document["identity_sha256"]
        )

        backend, validation_store = self._checkpoint_ready_backend()
        validated, prepared = backend._validated_backend_checkpoint(document)
        self.assertEqual(document, validated)
        self.assertEqual({"prepared": True}, prepared)
        validation_store.prepare_restart_checkpoint.assert_called_once_with(
            document["queue_replay"]
        )
        store.export_restart_checkpoint.assert_called_once_with(
            config_id=self.config.config_id,
            config_sha256=self.config.physical_sha256,
            campaign_id=campaign["campaign_id"],
            schedule_set_id=campaign["schedule_set"]["schedule_set_id"],
            created_at="2026-08-31T12:34:56Z",
            expected_snapshots=backend._checkpoint_queue_snapshots,
        )

    def test_restart_checkpoint_rejects_identity_and_config_tamper_before_restore(
        self,
    ) -> None:
        document, _store = self._empty_restart_checkpoint()
        identity_tamper = {**document, "identity_sha256": "0" * 64}
        config_tamper = {**document, "config_id": "wrong-config"}
        config_core = {
            key: value
            for key, value in config_tamper.items()
            if key != "identity_sha256"
        }
        config_tamper["identity_sha256"] = sha256_bytes(
            canonical_bytes(config_core)
        )

        for label, candidate in (
            ("identity", identity_tamper),
            ("config", config_tamper),
        ):
            with self.subTest(label=label):
                backend, store = self._checkpoint_ready_backend()
                with (
                    mock.patch.object(
                        backend,
                        "_incremental_schedule_runtime",
                        side_effect=AssertionError(
                            "restore consulted schedule paths before envelope validation"
                        ),
                    ) as schedule_restore,
                    self.assertRaisesRegex(
                        BackendError,
                        "backend restart checkpoint identity is invalid",
                    ),
                ):
                    backend.restore_checkpoint(candidate, [])
                schedule_restore.assert_not_called()
                store.prepare_restart_checkpoint.assert_not_called()

    def test_checkpoint_fingerprint_detects_metadata_change(self) -> None:
        path = Path(self.temporary.name) / "witness.json"
        path.write_bytes(b"{}\n")
        path.chmod(0o600)
        witness = _checkpoint_fingerprint(path, label="test witness")
        self.assertTrue(
            _checkpoint_fingerprint_matches(witness, label="test witness")
        )

        path.chmod(0o400)
        self.assertFalse(
            _checkpoint_fingerprint_matches(witness, label="test witness")
        )

    def test_preprocess_checkpoint_accepts_valid_hardlinked_json_artifact(
        self,
    ) -> None:
        """Bootstrap and restore preserve the producer's JSON-artifact policy."""

        from pipeline import preprocess_batch as producer_preprocess

        root = Path(self.temporary.name)
        bundle_id = "ppbatch_" + "a" * 32
        bundle_path = root / "bundle-root" / "bundles" / bundle_id
        order_path = bundle_path / "orders" / "000001.json"
        state_root = root / "preprocess-state"
        receipt_path = (
            state_root / "runs" / bundle_id / "receipts" / "000001.json"
        )
        processing_root = Path(
            self.config.section("preprocess")["processing_output_root"]
        )
        processing_root.mkdir(parents=True, mode=0o700)
        processing_root.chmod(0o700)
        result_root = processing_root / "items" / "000001"
        result_root.mkdir(parents=True, mode=0o700)
        (processing_root / "items").chmod(0o700)
        result_root.chmod(0o700)
        result_path = result_root / "result.json"
        artifact_path = result_root / "metadata.json"
        media_artifact_path = result_root / "normalized.flac"
        artifact_alias = root / "retained-metadata.json"
        selection_path = root / "selection.json"

        def pretty(value: Any) -> bytes:
            return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()

        def write_sealed(path: Path, value: Any) -> bytes:
            path.parent.mkdir(parents=True, exist_ok=True)
            body = pretty(value)
            path.write_bytes(body)
            path.chmod(0o400)
            return body

        def write_sealed_bytes(path: Path, body: bytes) -> bytes:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            path.chmod(0o400)
            return body

        selection_body = write_sealed(selection_path, {"entries": []})
        order_body = write_sealed(order_path, {"ordinal": 1})
        artifact_body = write_sealed(artifact_path, {"segments": []})
        os.link(artifact_path, artifact_alias)
        media_artifact_body = write_sealed_bytes(
            media_artifact_path, b"fLaC\x00fixture"
        )
        result_body = write_sealed(result_path, {"artifacts": []})
        receipt = {
            "bundle_id": bundle_id,
            "ordinal": 1,
            "preprocess_result": {
                "path": str(result_path),
                "sha256": sha256_bytes(result_body),
            },
            "artifacts": [
                {
                    "path": str(artifact_path),
                    "sha256": sha256_bytes(artifact_body),
                    "mime_type": "application/json",
                },
                {
                    "path": str(media_artifact_path),
                    "sha256": sha256_bytes(media_artifact_body),
                    "mime_type": "audio/flac",
                },
            ],
        }
        write_sealed(receipt_path, receipt)
        manifest = {
            "bundle_id": bundle_id,
            "work_order_count": 1,
            "selection": {
                "path": str(selection_path),
                "sha256": sha256_bytes(selection_body),
            },
            "work_orders": [
                {
                    "path": str(order_path.relative_to(bundle_path)),
                    "sha256": sha256_bytes(order_body),
                }
            ],
        }
        write_sealed(bundle_path / "manifest.json", manifest)

        def strict_control_read(
            path: Path, *, maximum: int, label: str, **_kwargs: Any
        ) -> tuple[bytes, os.stat_result]:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or observed.st_uid != os.getuid()
            ):
                raise RuntimeError(
                    f"{label} must be an owner-controlled single-link regular file"
                )
            body = path.read_bytes()
            if not 1 <= len(body) <= maximum:
                raise RuntimeError(f"{label} exceeds its byte cap")
            return body, observed

        modules = SimpleNamespace(
            queue_runner=SimpleNamespace(_stable_read=strict_control_read),
            preprocess_batch=producer_preprocess,
            background=SimpleNamespace(
                _validate_receipt_digest=lambda _receipt: None,
                pretty_bytes=pretty,
            ),
        )
        backend = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )

        self.assertEqual(2, artifact_path.stat().st_nlink)
        self.assertEqual(
            "items/000001/metadata.json",
            str(artifact_path.relative_to(processing_root)),
        )
        self.assertEqual(
            "items/000001/normalized.flac",
            str(media_artifact_path.relative_to(processing_root)),
        )
        self.assertEqual(
            artifact_body,
            producer_preprocess.readonly_file(
                artifact_path,
                len(artifact_body),
                "valid hardlinked producer artifact",
                allow_hardlinks=True,
            )[1],
        )
        witness = backend._preprocess_candidate_restart_witness(
            bundle_path=bundle_path,
            bundle_id=bundle_id,
            state_root=state_root,
            item_count=1,
        )
        rows_by_path = {row["fingerprint"]["path"]: row for row in witness}
        self.assertEqual(
            {
                str(bundle_path / "manifest.json"),
                str(selection_path),
                str(order_path),
                str(receipt_path),
                str(result_path),
                str(artifact_path),
                str(media_artifact_path),
            },
            set(rows_by_path),
        )
        for strict_path in (
            bundle_path / "manifest.json",
            selection_path,
            order_path,
            receipt_path,
            result_path,
        ):
            self.assertEqual(
                "owner_single_link_json",
                rows_by_path[str(strict_path)]["read_policy"],
            )
        self.assertEqual(
            "preprocess_readonly_json_allow_hardlinks",
            rows_by_path[str(artifact_path)]["read_policy"],
        )
        self.assertEqual(
            "fingerprint_only",
            rows_by_path[str(media_artifact_path)]["read_policy"],
        )
        self.assertEqual(
            [str(artifact_path)],
            [
                path
                for path, row in rows_by_path.items()
                if row["read_policy"]
                == "preprocess_readonly_json_allow_hardlinks"
            ],
        )
        self.assertEqual(
            2, rows_by_path[str(artifact_path)]["fingerprint"]["link_count"]
        )
        self.assertTrue(
            backend._preprocess_restart_witness_matches(
                witness,
                bundle_id=bundle_id,
            )
        )

    def test_gpu_checkpoint_json_witnesses_remain_single_link_only(self) -> None:
        root = Path(self.temporary.name)

        def write_sealed(name: str, value: Any) -> tuple[Path, str]:
            path = root / name
            body = (json.dumps(value, sort_keys=True) + "\n").encode()
            path.write_bytes(body)
            path.chmod(0o400)
            return path, sha256_bytes(body)

        def strict_control_read(
            path: Path, *, maximum: int, label: str, **_kwargs: Any
        ) -> tuple[bytes, os.stat_result]:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or observed.st_uid != os.getuid()
            ):
                raise RuntimeError(
                    f"{label} must be an owner-controlled single-link regular file"
                )
            body = path.read_bytes()
            if not 1 <= len(body) <= maximum:
                raise RuntimeError(f"{label} exceeds its byte cap")
            return body, observed

        queue_path, queue_sha256 = write_sealed("gpu-queue.json", {"items": []})
        receipt_path, receipt_sha256 = write_sealed(
            "gpu-materialization-receipt.json", {"selection": {}}
        )
        batch_path, batch_sha256 = write_sealed("gpu-batch.json", {"items": []})
        record = {
            "batch_key": "fixture:gpu-policy",
            "queue": {"path": str(queue_path), "sha256": queue_sha256},
            "materialization_receipt": {
                "path": str(receipt_path),
                "sha256": receipt_sha256,
            },
            "batch": {"path": str(batch_path), "sha256": batch_sha256},
        }
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(
                queue_runner=SimpleNamespace(_stable_read=strict_control_read)
            ),  # type: ignore[arg-type]
        )

        witness = backend._gpu_record_restart_witness(record, "pending")
        self.assertEqual(3, len(witness))
        self.assertEqual(
            {"owner_single_link_json"},
            {row["read_policy"] for row in witness},
        )

        os.link(queue_path, root / "gpu-queue-alias.json")
        with self.assertRaisesRegex(BackendError, "single-link regular file"):
            backend._gpu_record_restart_witness(record, "pending")

    def test_checkpoint_deep_audit_signal_is_not_downgraded_to_bootstrap(
        self,
    ) -> None:
        reference = dict(self.config.section("campaign")["schedules"][0])
        schedule_body = b"schedule-body"
        manifest_body = b"manifest-body"
        manifest_sha256 = "b" * 64
        bundle_id = "fixture-incremental-bundle"
        schedule = {
            "schedule_id": reference["schedule_id"],
            "queue": {
                "manifest_path": str(
                    Path(self.temporary.name) / "manifest.json"
                ),
                "manifest_sha256": manifest_sha256,
                "bundle_id": bundle_id,
            },
        }
        bundle = {
            "body": manifest_body,
            "manifest": {"bundle_id": bundle_id},
        }
        queue_runner = fake_queue_runner()
        queue_runner._load_bundle = mock.Mock(return_value=bundle)
        modules = SimpleNamespace(
            background=SimpleNamespace(
                load_schedule=mock.Mock(
                    return_value=(
                        schedule,
                        Path(reference["path"]),
                        schedule_body,
                    )
                )
            ),
            queue_runner=queue_runner,
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        store = SimpleNamespace(
            hydrate_restart_checkpoint=mock.Mock(
                side_effect=DeepAuditRequired("target changed during replay")
            ),
            bootstrap_restart_snapshot=mock.Mock(
                side_effect=AssertionError(
                    "a bound checkpoint must not fall back to metadata bootstrap"
                )
            ),
        )
        backend._operational_replay_store = store

        def fake_digest(body: bytes) -> str:
            if body == schedule_body:
                return reference["sha256"]
            if body == manifest_body:
                return manifest_sha256
            raise AssertionError("unexpected digest input")

        checkpoint_token = {"checkpoint": True}
        with (
            mock.patch(
                "autonomous_controller.sealed_backend.sha256_bytes",
                side_effect=fake_digest,
            ),
            mock.patch.object(
                backend, "_queue_replay_binding", return_value="binding"
            ),
            self.assertRaisesRegex(
                BackendError, "incremental schedule restore failed"
            ) as raised,
        ):
            backend._incremental_schedule_runtime(reference, checkpoint_token)
        self.assertIsInstance(raised.exception.__cause__, DeepAuditRequired)
        store.hydrate_restart_checkpoint.assert_called_once_with(
            "binding", bundle, checkpoint_token
        )
        store.bootstrap_restart_snapshot.assert_not_called()

    def test_malformed_preprocess_checkpoint_row_fails_closed(self) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(queue_runner=fake_queue_runner()),
        )
        with self.assertRaisesRegex(
            BackendError, "preprocess checkpoint candidate is malformed"
        ):
            backend._restore_preprocess_checkpoint(
                [{"unexpected": True}], metadata_bootstrap=False
            )
        self.assertEqual([], backend._preprocess_candidates_cache)
        self.assertEqual(set(), backend._preprocess_candidate_keys)

    def test_malformed_gpu_checkpoint_row_fails_closed(self) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(queue_runner=fake_queue_runner()),
        )
        with self.assertRaisesRegex(
            BackendError, "GPU checkpoint row is malformed"
        ):
            backend._restore_gpu_checkpoint(
                [{"unexpected": True}], [], metadata_bootstrap=False
            )
        self.assertEqual({}, backend._gpu_records)
        self.assertEqual({}, backend._gpu_status)

    def test_admitted_gpu_queues_restore_without_materializing_media(self) -> None:
        gpu = self.config.section("gpu_readiness")
        bundle_root = Path(self.config.section("preprocess")["bundle_root"])
        state_root = Path(self.temporary.name) / "preprocess-state"
        existing_id = "ppbatch_" + "a" * 32
        new_id = "ppbatch_" + "b" * 32
        existing_bundle = bundle_root / "bundles" / existing_id
        new_bundle = bundle_root / "bundles" / new_id

        def queue_manifest(bundle_id: str, queue_digit: str) -> dict[str, Any]:
            queue_id = "gpuasrqueue_" + queue_digit * 32
            bundle_path = bundle_root / "bundles" / bundle_id
            return {
                "queue_id": queue_id,
                "origin": {
                    "preprocess_bundle": {
                        "bundle_id": bundle_id,
                        "path": str(bundle_path),
                    },
                    "state_root": str(state_root),
                },
                "output": {"queue_root": gpu["queue_root"]},
                "production_profile": {
                    "reference": {
                        "path": gpu["production_profile"],
                        "physical_sha256": gpu["production_profile_sha256"],
                    }
                },
                "members": [
                    {
                        "ordinal": 1,
                        "resource_disposition": {"state": "ready"},
                    },
                    {
                        "ordinal": 2,
                        "resource_disposition": {"state": "ready"},
                    },
                ],
                "totals": {
                    "member_count": 2,
                    "ready_count": 2,
                    "requires_chunking_count": 0,
                    "explicit_skip_count": 0,
                    "ready_audio_duration_ms": 2_000,
                    "requires_chunking_audio_duration_ms": 0,
                },
            }

        existing = queue_manifest(existing_id, "c")
        new = queue_manifest(new_id, "d")
        existing_path = (
            Path(gpu["queue_root"])
            / "queues"
            / existing["queue_id"]
            / "manifest.json"
        )
        new_path = (
            Path(gpu["queue_root"])
            / "queues"
            / new["queue_id"]
            / "manifest.json"
        )
        existing_reference = {
            "path": str(existing_path),
            "sha256": sha256_bytes(canonical_bytes(existing)),
            "queue_id": existing["queue_id"],
        }
        disposition = {
            "member_count": 2,
            "ready_count": 2,
            "requires_chunking_count": 0,
            "explicit_skip_count": 0,
            "ready_audio_duration_ms": 2_000,
            "requires_chunking_audio_duration_ms": 0,
        }
        loader = mock.Mock(return_value=existing)
        materializer = mock.Mock(return_value=(new, new_path))
        modules = SimpleNamespace(
            gpu_queue=SimpleNamespace(
                canonical_bytes=canonical_bytes,
                load_admitted_queue=loader,
                materialize_queue=materializer,
            )
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        backend._preprocess_candidates_cache = [
            (1, existing_bundle, existing_id, state_root),
            (2, new_bundle, new_id, state_root),
        ]
        backend._preprocess_candidate_keys = {
            (str(state_root), existing_id),
            (str(state_root), new_id),
        }
        backend._preprocess_bundle_item_counts = {
            (str(state_root), existing_id): 2,
            (str(state_root), new_id): 2,
        }
        backend._preprocessed_item_total = 4
        backend._gpu_records = {
            "existing-batch": {
                "record_kind": "ready_batch",
                "batch_key": "existing-batch",
                "preprocess_bundle_id": existing_id,
                "queue": existing_reference,
                "queue_disposition": disposition,
            }
        }

        self.assertEqual(1, backend._hydrate_admitted_gpu_queues())
        restored, restored_path = backend._gpu_queue_for_bundle(
            existing_bundle, state_root
        )
        self.assertEqual(existing, restored)
        self.assertEqual(existing_path, restored_path)
        loader.assert_called_once()
        materializer.assert_not_called()

        materialized, materialized_path = backend._gpu_queue_for_bundle(
            new_bundle, state_root
        )
        self.assertEqual(new, materialized)
        self.assertEqual(new_path, materialized_path)
        materializer.assert_called_once()

        # A restored queue may be only partially claimed.  Readiness must use
        # the admitted manifest to expose the suffix rather than treating the
        # whole historical bundle as complete or rebuilding it from media.
        backend._preprocess_candidates_cache = [
            (1, existing_bundle, existing_id, state_root)
        ]
        backend._preprocess_candidate_keys = {(str(state_root), existing_id)}
        backend._preprocess_bundle_item_counts = {
            (str(state_root), existing_id): 2
        }
        backend._preprocessed_item_total = 2
        backend._gpu_status = {"existing-batch": "completed"}
        backend._gpu_batch_item_counts = {"existing-batch": 1}
        backend._gpu_member_claims = {
            (existing["queue_id"], 1): "existing-batch"
        }
        backend._gpu_queue_dispositions = {
            existing["queue_id"]: disposition
        }
        observed_unclaimed: list[dict[str, Any]] = []

        def select(candidates, **_kwargs):
            observed_unclaimed.extend(candidates)
            return [], list(candidates)

        with (
            mock.patch.object(backend, "_refresh_gpu_status", return_value=0),
            mock.patch.object(
                backend, "_supervise_gpu_child", return_value=(False, None, 0)
            ),
            mock.patch.object(
                backend,
                "_load_profile",
                return_value={
                    "batch_limits": {
                        "ready_batch_high_water": 2,
                        "maximum_items": 32,
                        "maximum_total_audio_ms": 1_000_000,
                        "preferred_total_audio_ms": 900_000,
                    }
                },
            ),
            mock.patch.object(backend, "_gpu_inputs_drained", return_value=False),
            mock.patch.object(
                backend,
                "_gpu_partial_reaches_preferred_duration",
                return_value=False,
            ),
            mock.patch.object(
                backend, "_bounded_gpu_partial_flush", return_value=False
            ),
            mock.patch.object(backend, "_select_gpu_packs", side_effect=select),
        ):
            outcome = backend._run_gpu_readiness()
        self.assertEqual([2], [row["member"]["ordinal"] for row in observed_unclaimed])
        self.assertEqual(1, outcome.monitor["buffered_ready_items"])
        materializer.assert_called_once()

    def test_unchanged_pending_gpu_checkpoint_refreshes_result_without_source_replay(
        self,
    ) -> None:
        """A completed result wins over stale pending and exhausted attempts."""

        record = gpu_ready_record("checkpoint:pending", "a")
        key = record["batch_key"]
        queue_id = "gpuasrqueue_" + "b" * 32
        disposition = {
            "member_count": 1,
            "ready_count": 1,
            "requires_chunking_count": 0,
            "explicit_skip_count": 0,
            "ready_audio_duration_ms": 1_000,
            "requires_chunking_audio_duration_ms": 0,
        }
        claims = [(queue_id, 1)]
        dispositions = [(queue_id, disposition)]
        checkpoint_files = [{"fixture": "pending-checkpoint-witness"}]
        row = {
            "record": record,
            "status": "pending",
            "item_count": 1,
            "claims": [[queue_id, 1]],
            "dispositions": [
                {"queue_id": queue_id, "value": disposition}
            ],
            "parked": None,
            "files": checkpoint_files,
        }
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        backend._gpu_executor = FakeChildExecutor(
            [
                gpu_child_record(backend, record, ordinal, "failed")
                for ordinal in range(1, 4)
            ]
        )

        with (
            mock.patch.object(
                backend,
                "_gpu_checkpoint_derivation",
                return_value=(1, claims, dispositions),
            ),
            mock.patch.object(
                backend, "_gpu_restart_witness_matches", return_value=True
            ),
            mock.patch.object(
                backend,
                "_refresh_admitted_gpu_record_status",
                return_value="completed",
            ) as refresh,
            mock.patch.object(
                backend,
                "_apply_gpu_record",
                side_effect=AssertionError(
                    "unchanged pending checkpoint re-entered deep source replay"
                ),
            ) as source_replay,
        ):
            telemetry = backend._restore_gpu_checkpoint(
                [row], [], metadata_bootstrap=False
            )

        self.assertEqual(
            {
                "fast_reused_records": 1,
                "targeted_revalidated_records": 0,
                "metadata_bootstrap_records": 0,
            },
            telemetry,
        )
        refresh.assert_called_once_with(key, record)
        source_replay.assert_not_called()
        self.assertEqual("completed", backend._gpu_status[key])
        self.assertEqual({}, backend._gpu_parked)
        self.assertNotIn(key, backend._gpu_restart_witnesses)
        self.assertEqual(record, backend._gpu_records[key])
        self.assertEqual(1, backend._gpu_batch_item_counts[key])
        self.assertEqual(key, backend._gpu_member_claims[(queue_id, 1)])
        self.assertEqual(disposition, backend._gpu_queue_dispositions[queue_id])

        completed_files = [{"fixture": "completed-result-witness"}]
        with (
            mock.patch.object(
                backend,
                "_gpu_checkpoint_derivation",
                return_value=(1, claims, dispositions),
            ),
            mock.patch.object(
                backend,
                "_gpu_record_restart_witness",
                return_value=completed_files,
            ) as recapture,
        ):
            exported = backend._export_gpu_checkpoint()
        recapture.assert_called_once_with(record, "completed")
        self.assertEqual(completed_files, exported["records"][0]["files"])

    def test_restart_recovers_gpu_authority_before_preprocess_can_fail(self) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(queue_runner=fake_queue_runner()),
        )
        gpu_evidence = [({"batch_key": "fixture-gpu-batch"}, "pending")]
        restore_order = []

        def schedule_runtime(reference, _checkpoint):
            return (
                {"schedule_id": reference["schedule_id"]},
                {},
                [],
                {"items": []},
                {
                    "generation": 1,
                    "state_digest": "a" * 64,
                },
            )

        def restore_gpu(rows, evidence, *, metadata_bootstrap):
            self.assertEqual([], rows)
            self.assertEqual(gpu_evidence, evidence)
            self.assertTrue(metadata_bootstrap)
            restore_order.append("gpu")
            # Model the exact authority cleanup needs in order to bind a
            # durable child journal entry after a later restart-surface fault.
            backend._gpu_records["fixture-gpu-batch"] = {
                "record_kind": "ready_batch"
            }
            return {
                "fast_reused_records": 0,
                "targeted_revalidated_records": 0,
                "metadata_bootstrap_records": 1,
            }

        def fail_preprocess(_rows, *, metadata_bootstrap):
            self.assertTrue(metadata_bootstrap)
            self.assertIn("fixture-gpu-batch", backend._gpu_records)
            restore_order.append("preprocess")
            raise BackendError("fixture preprocess restore failed")

        with (
            mock.patch.object(
                backend,
                "_incremental_schedule_runtime",
                side_effect=schedule_runtime,
            ),
            mock.patch.object(
                backend,
                "_validate_incremental_campaign_authority",
                return_value=({}, {}, {}),
            ),
            mock.patch.object(
                backend,
                "_journal_recovery_evidence",
                return_value=({}, gpu_evidence, []),
            ),
            mock.patch.object(backend, "_completed_acquisitions", return_value=[]),
            mock.patch.object(
                backend,
                "_restore_gpu_checkpoint",
                side_effect=restore_gpu,
            ),
            mock.patch.object(
                backend,
                "_restore_preprocess_checkpoint",
                side_effect=fail_preprocess,
            ),
            self.assertRaisesRegex(
                BackendError, "fixture preprocess restore failed"
            ),
        ):
            backend._restore_checkpoint_exact(None, [])

        self.assertEqual(["gpu", "preprocess"], restore_order)

    def test_trusted_copy_recovery_requires_reviewed_transition_and_mount(self) -> None:
        backend = SealedArchiveBackend(self.config, modules=SimpleNamespace())
        with (
            mock.patch("autonomous_controller.sealed_backend.reviewed_cold_mount_transition", return_value=None),
            mock.patch.object(backend, "restore_checkpoint") as restore,
        ):
            with self.assertRaisesRegex(BackendError, "reviewed mount migration"):
                backend.restore_rsync_copy_checkpoint({}, ())
            restore.assert_not_called()
        with (
            mock.patch("autonomous_controller.sealed_backend.reviewed_cold_mount_transition", return_value={"reviewed": True}),
            mock.patch.object(backend, "_validate_cold_storage_identity", side_effect=BackendError("wrong mount")),
            mock.patch.object(backend, "restore_checkpoint") as restore,
        ):
            with self.assertRaisesRegex(BackendError, "wrong mount"):
                backend.restore_rsync_copy_checkpoint({}, ())
            restore.assert_not_called()
        self.assertFalse(backend._trust_completed_copy)

    def test_trusted_copy_scope_is_cleared_after_success_and_failure(self) -> None:
        for fail in (False, True):
            backend = SealedArchiveBackend(self.config, modules=SimpleNamespace())

            def replay_copy(*_args):
                self.assertTrue(backend._trust_completed_copy)
                if fail:
                    raise BackendError("fixture recovery failure")
                return {}

            with (
                mock.patch("autonomous_controller.sealed_backend.reviewed_cold_mount_transition", return_value={"reviewed": True}),
                mock.patch.object(backend, "_validate_cold_storage_identity", return_value={}),
                mock.patch.object(backend, "restore_checkpoint", side_effect=replay_copy),
            ):
                if fail:
                    with self.assertRaisesRegex(BackendError, "fixture recovery failure"):
                        backend.restore_rsync_copy_checkpoint({}, ())
                else:
                    result = backend.restore_rsync_copy_checkpoint({}, ())
                    self.assertFalse(result["trusted_copy_migration"]["completed_media_rehashed"])
            self.assertFalse(backend._trust_completed_copy)

    def test_startup_terminal_history_binds_gpu_then_stops_before_queue_restore(
        self,
    ) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(queue_runner=fake_queue_runner()),
        )
        journal = self._gpu_child_journal()
        record = gpu_ready_record("fixture-terminal", "a")
        # Match the current production history size. Terminal records are cheap
        # to classify, but still must bind to recovered batch/spec authority.
        for ordinal in range(1, 29):
            journal.save(gpu_child_record(backend, record, ordinal, "failed"))
        self.assertFalse(backend._startup_gpu_child_authority_is_empty())
        self.assertTrue(
            PrivateGpuChildJournal.prove_quiescent_startup_authority(journal.root)
        )
        order: list[str] = []

        def stop_boundary() -> None:
            raise StartupRestoreStopRequested

        def restore_gpu(
            rows,
            evidence,
            *,
            metadata_bootstrap,
            cancellation_boundary,
        ):
            self.assertEqual([], rows)
            self.assertEqual([], evidence)
            self.assertTrue(metadata_bootstrap)
            # Stop was observed but is deliberately latched until this exact
            # GPU-authority recovery unit completes.
            cancellation_boundary()
            order.append("gpu_bound")
            return {
                "fast_reused_records": 0,
                "targeted_revalidated_records": 0,
                "metadata_bootstrap_records": 28,
            }

        def quiesce() -> None:
            order.append("gpu_quiesced")

        with (
            mock.patch.object(
                backend, "_restore_gpu_checkpoint", side_effect=restore_gpu
            ),
            mock.patch.object(
                backend,
                "_quiesce_startup_restored_gpu_authority",
                side_effect=quiesce,
            ),
            mock.patch.object(
                backend, "_prepare_queue_restart_checkpoint"
            ) as queue_restore,
            mock.patch.object(
                backend, "_incremental_schedule_runtime"
            ) as schedule_restore,
            mock.patch.object(
                backend, "_restore_preprocess_checkpoint"
            ) as preprocess_restore,
            self.assertRaises(StartupRestoreStopRequested),
        ):
            backend.restore_checkpoint_interruptibly(
                None, [], cancellation_boundary=stop_boundary
            )

        self.assertEqual(["gpu_bound", "gpu_quiesced"], order)
        queue_restore.assert_not_called()
        schedule_restore.assert_not_called()
        preprocess_restore.assert_not_called()

    def test_startup_stop_does_not_hide_orphan_terminal_child(self) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(queue_runner=fake_queue_runner()),
        )
        journal = self._gpu_child_journal()
        record = gpu_ready_record("orphan-terminal", "c")
        journal.save(gpu_child_record(backend, record, 1, "failed"))

        def stop_boundary() -> None:
            raise StartupRestoreStopRequested

        with (
            mock.patch.object(
                backend, "_quiesce_startup_restored_gpu_authority"
            ) as quiesce,
            self.assertRaisesRegex(
                BackendError, "records without GPU recovery authority"
            ),
        ):
            backend.restore_checkpoint_interruptibly(
                None, [], cancellation_boundary=stop_boundary
            )
        quiesce.assert_not_called()

    def test_startup_active_child_defers_stop_until_gpu_restore_and_quiesce(
        self,
    ) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(queue_runner=fake_queue_runner()),
        )
        journal = self._gpu_child_journal()
        record = gpu_ready_record("fixture-active", "b")
        journal.save(gpu_child_record(backend, record, 1, "running"))
        self.assertFalse(backend._startup_gpu_child_authority_is_empty())
        order: list[str] = []

        def stop_boundary() -> None:
            raise StartupRestoreStopRequested

        def schedule_runtime(
            reference,
            _checkpoint,
            *,
            cancellation_boundary,
        ):
            cancellation_boundary()
            return (
                {"schedule_id": reference["schedule_id"]},
                {},
                [],
                {"items": []},
                {
                    "generation": 1,
                    "state_digest": "a" * 64,
                },
            )

        def restore_gpu(
            rows,
            evidence,
            *,
            metadata_bootstrap,
            cancellation_boundary,
        ):
            self.assertEqual([], rows)
            self.assertEqual([], evidence)
            self.assertTrue(metadata_bootstrap)
            cancellation_boundary()
            order.append("gpu_restored")
            return {
                "fast_reused_records": 0,
                "targeted_revalidated_records": 0,
                "metadata_bootstrap_records": 0,
            }

        def quiesce() -> None:
            order.append("gpu_quiesced")

        with (
            mock.patch.object(
                backend,
                "_incremental_schedule_runtime",
                side_effect=schedule_runtime,
            ),
            mock.patch.object(
                backend,
                "_validate_incremental_campaign_authority",
                return_value=({}, {}, {}),
            ),
            mock.patch.object(
                backend,
                "_journal_recovery_evidence",
                return_value=({}, [], []),
            ),
            mock.patch.object(backend, "_completed_acquisitions", return_value=[]),
            mock.patch.object(
                backend, "_restore_gpu_checkpoint", side_effect=restore_gpu
            ),
            mock.patch.object(
                backend, "_quiesce_startup_restored_gpu_authority", side_effect=quiesce
            ),
            mock.patch.object(
                backend, "_restore_preprocess_checkpoint"
            ) as preprocess_restore,
            self.assertRaises(StartupRestoreStopRequested),
        ):
            backend.restore_checkpoint_interruptibly(
                None, [], cancellation_boundary=stop_boundary
            )

        self.assertEqual(["gpu_restored", "gpu_quiesced"], order)
        preprocess_restore.assert_not_called()

    def _schedule_runtime_retry_fixture(self):
        class FixtureQueueError(RuntimeError):
            pass

        class ReplaySession:
            def __init__(self, store) -> None:
                self.store = store
                self.delta = None

            def __enter__(self):
                return self

            def __exit__(self, error_type, _error, _traceback) -> bool:
                if error_type is None:
                    self.delta = {
                        "session_state_digest": "a" * 64,
                        "state_digest": "a" * 64,
                        "session_completed_count": 0,
                        "session_pending_count": 0,
                        "completed_count": 0,
                        "pending_count": 0,
                    }
                    self.store.committed_sessions += 1
                return False

        class ReplayStore:
            def __init__(self) -> None:
                self.sessions = []
                self.committed_sessions = 0

            @staticmethod
            def has_snapshot(_binding) -> bool:
                return True

            def operational_session(self, _binding):
                session = ReplaySession(self)
                self.sessions.append(session)
                return session

            @staticmethod
            def deep_capture(_binding):
                raise AssertionError("seeded runtime retry must remain operational")

        reference = dict(self.config.document["campaign"]["schedules"][0])
        schedule_body = b'{"fixture":"queue-runtime-snapshot-retry"}\n'
        reference["sha256"] = hashlib.sha256(schedule_body).hexdigest()
        schedule = {
            "schedule_id": reference["schedule_id"],
            "queue": {
                "manifest_path": str(Path(reference["path"]).parent / "manifest.json"),
                "manifest_sha256": "b" * 64,
                "bundle_id": "fixture-queue-runtime-retry",
            },
        }
        runtime = ({"bundle": "current"}, [], {"items": []}, {"status": "validated"})
        background = SimpleNamespace(
            load_schedule=lambda path: (schedule, Path(path), schedule_body),
            _load_runtime=mock.Mock(return_value=runtime),
        )
        modules = SimpleNamespace(
            background=background,
            queue_runner=SimpleNamespace(QueueRunnerError=FixtureQueueError),
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        replay_store = ReplayStore()
        backend._operational_replay_store = replay_store  # type: ignore[assignment]
        return (
            backend,
            background,
            replay_store,
            reference,
            schedule,
            runtime,
            FixtureQueueError,
        )

    def test_schedule_runtime_retries_exact_offline_state_admission_race(self) -> None:
        (
            backend,
            background,
            replay_store,
            reference,
            schedule,
            runtime,
            queue_error,
        ) = self._schedule_runtime_retry_fixture()
        message = "completed/pending/quarantine state changed during offline validation"
        background._load_runtime.side_effect = [queue_error(message), runtime]

        with mock.patch("autonomous_controller.sealed_backend.time.sleep") as pause:
            observed = backend._schedule_runtime(reference)

        self.assertEqual((schedule, *runtime[:3]), observed)
        self.assertEqual(2, background._load_runtime.call_count)
        self.assertEqual(2, len(replay_store.sessions))
        self.assertEqual(1, replay_store.committed_sessions)
        pause.assert_called_once_with(QUEUE_RUNTIME_SNAPSHOT_RETRY_SECONDS)

    def test_schedule_runtime_bounds_persistent_offline_state_admission_race(self) -> None:
        (
            backend,
            background,
            replay_store,
            reference,
            _schedule,
            _runtime,
            queue_error,
        ) = self._schedule_runtime_retry_fixture()
        message = "completed/pending/quarantine state changed during offline validation"
        persistent = queue_error(message)
        background._load_runtime.side_effect = persistent

        with (
            mock.patch("autonomous_controller.sealed_backend.time.sleep") as pause,
            self.assertRaises(queue_error) as raised,
        ):
            backend._schedule_runtime(reference)

        self.assertIs(persistent, raised.exception)
        self.assertEqual(
            QUEUE_RUNTIME_SNAPSHOT_ATTEMPTS,
            background._load_runtime.call_count,
        )
        self.assertEqual(QUEUE_RUNTIME_SNAPSHOT_ATTEMPTS, len(replay_store.sessions))
        self.assertEqual(0, replay_store.committed_sessions)
        self.assertEqual(QUEUE_RUNTIME_SNAPSHOT_ATTEMPTS - 1, pause.call_count)

    def test_schedule_runtime_does_not_retry_lookalike_offline_state_errors(self) -> None:
        message = "completed/pending/quarantine state changed during offline validation"
        cases = (
            ("wrong-message", lambda error_type: error_type(message + " now")),
            ("wrong-type", lambda _error_type: RuntimeError(message)),
        )
        for label, build_error in cases:
            with self.subTest(label=label):
                (
                    backend,
                    background,
                    replay_store,
                    reference,
                    _schedule,
                    _runtime,
                    queue_error,
                ) = self._schedule_runtime_retry_fixture()
                error = build_error(queue_error)
                background._load_runtime.side_effect = error

                with (
                    mock.patch(
                        "autonomous_controller.sealed_backend.time.sleep"
                    ) as pause,
                    self.assertRaises(type(error)) as raised,
                ):
                    backend._schedule_runtime(reference)

                self.assertIs(error, raised.exception)
                self.assertEqual(1, background._load_runtime.call_count)
                self.assertEqual(1, len(replay_store.sessions))
                self.assertEqual(0, replay_store.committed_sessions)
                pause.assert_not_called()

    def test_restore_wraps_the_complete_exact_replay_in_tool_witness(self) -> None:
        class FixtureBatchError(RuntimeError):
            pass

        chronology: list[str] = []

        @contextmanager
        def witness():
            chronology.append("enter")
            try:
                yield
            finally:
                chronology.append("exit")

        modules = SimpleNamespace(
            preprocess_batch=SimpleNamespace(
                BatchError=FixtureBatchError,
                restore_scoped_tool_provenance_witness=witness,
            )
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]

        def exact(_events):
            chronology.append("restore")
            return {"status": "restored"}

        with mock.patch.object(backend, "_restore_exact", side_effect=exact) as replay:
            returned = backend.restore([{"event_type": "fixture"}])

        self.assertEqual({"status": "restored"}, returned)
        self.assertEqual(["enter", "restore", "exit"], chronology)
        replay.assert_called_once_with([{"event_type": "fixture"}])

    def test_restore_does_not_relabel_an_unrelated_preprocess_batch_error(self) -> None:
        class FixtureBatchError(RuntimeError):
            pass

        class FixtureWitnessError(FixtureBatchError):
            pass

        @contextmanager
        def witness():
            yield

        modules = SimpleNamespace(
            preprocess_batch=SimpleNamespace(
                BatchError=FixtureBatchError,
                RestoreToolProvenanceError=FixtureWitnessError,
                restore_scoped_tool_provenance_witness=witness,
            )
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        unrelated = FixtureBatchError("unrelated bundle validation failed")

        with (
            mock.patch.object(backend, "_restore_exact", side_effect=unrelated),
            self.assertRaisesRegex(
                FixtureBatchError,
                r"unrelated bundle validation failed",
            ) as raised,
        ):
            backend.restore([])

        self.assertIs(unrelated, raised.exception)
        self.assertNotIsInstance(raised.exception, BackendError)

    def test_restore_labels_only_the_dedicated_tool_witness_error(self) -> None:
        class FixtureBatchError(RuntimeError):
            pass

        class FixtureWitnessError(FixtureBatchError):
            pass

        @contextmanager
        def witness():
            yield
            raise FixtureWitnessError("exit provenance changed")

        modules = SimpleNamespace(
            preprocess_batch=SimpleNamespace(
                BatchError=FixtureBatchError,
                RestoreToolProvenanceError=FixtureWitnessError,
                restore_scoped_tool_provenance_witness=witness,
            )
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]

        with (
            mock.patch.object(backend, "_restore_exact", return_value={"ok": True}),
            self.assertRaisesRegex(
                BackendError,
                r"restore tool provenance witness failed: exit provenance changed",
            ),
        ):
            backend.restore([])

    def acquisition_result_adapter_fixture(self):
        repository = Path(__file__).resolve().parents[2]
        acquire = ModuleType(f"acquisition_result_fixture_{id(self)}_{time.time_ns()}")
        acquire.__file__ = str(repository / "acquisition" / "acquire.py")
        acquire.IMPLEMENTATION_VERSION = ACQUISITION_LEGACY_VERSION
        acquire._result_timestamp = lambda value: datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
        acquire.utc_now = lambda: "2026-08-30T20:49:25Z"
        validation_inputs = []
        writes = []

        def strict_validator(
            result,
            _output_root,
            _work_order,
            *,
            result_path=None,
            pins=None,
        ):
            validation_inputs.append(dict(result))
            if result.get("envelope") != "valid":
                return False
            started = acquire._result_timestamp(result["started_at"])
            completed = acquire._result_timestamp(result["completed_at"])
            wall_ms = round((completed - started).total_seconds() * 1_000)
            return abs(result["duration_ms"] - wall_ms) <= 2_000

        def atomic_writer(path, value):
            writes.append((Path(path), json.loads(json.dumps(value))))

        acquire.validate_reusable_result = strict_validator
        acquire.atomic_write_json = atomic_writer
        runner = ModuleType(f"queue_result_fixture_{id(self)}_{time.time_ns()}")
        runner.__file__ = str(repository / "acquisition" / "queue_runner.py")
        runner.IMPLEMENTATION_VERSION = QUEUE_RUNNER_LEGACY_VERSION
        runner.acquire = acquire
        _install_acquisition_result_finalization_adapter(runner)
        return acquire, validation_inputs, writes

    @staticmethod
    def legacy_acquisition_result():
        completed_at = "2026-08-30T20:49:23Z"
        return {
            "schema_version": 1,
            "status": "completed",
            "dry_run": False,
            "result_path": "/archive/jobs/job/result.json",
            "started_at": "2026-08-30T20:49:17Z",
            "completed_at": completed_at,
            "duration_ms": 8_591,
            "envelope": "valid",
            "catalog_records": {
                "sources": [
                    {
                        "observed_at": completed_at,
                        "created_at": completed_at,
                        "updated_at": completed_at,
                    }
                ],
                "media_objects": [{"first_cataloged_at": completed_at}],
                "media_locations": [{"verified_at": completed_at}],
                "media_sources": [{"retrieved_at": completed_at}],
            },
        }

    @staticmethod
    def pinned_result(path: Path, written_at: str):
        timestamp = datetime.fromisoformat(written_at.replace("Z", "+00:00"))
        metadata = SimpleNamespace(
            st_mtime_ns=round(timestamp.timestamp() * 1_000_000_000),
            st_mode=stat.S_IFREG | 0o600,
            st_nlink=1,
            st_uid=os.getuid(),
        )
        return SimpleNamespace(
            files=[
                SimpleNamespace(
                    path=path,
                    label="durable acquisition result",
                    initial_stat=metadata,
                )
            ]
        )

    def test_acquisition_result_adapter_recovers_only_witnessed_legacy_drift(self) -> None:
        acquire, validation_inputs, _writes = self.acquisition_result_adapter_fixture()
        result = self.legacy_acquisition_result()
        path = Path(result["result_path"])
        pins = self.pinned_result(path, "2026-08-30T20:49:25.952Z")

        self.assertTrue(
            acquire.validate_reusable_result(
                result, Path("/archive"), {}, result_path=path, pins=pins
            )
        )
        self.assertEqual(8_591, result["duration_ms"])
        self.assertEqual(6_000, validation_inputs[-1]["duration_ms"])

        # Direct validation has no stable result-file witness and remains strict.
        self.assertFalse(
            acquire.validate_reusable_result(
                result, Path("/archive"), {}, result_path=path
            )
        )
        stale = self.pinned_result(path, "2026-08-30T20:49:40Z")
        self.assertFalse(
            acquire.validate_reusable_result(
                result, Path("/archive"), {}, result_path=path, pins=stale
            )
        )
        negative = dict(result, duration_ms=3_000)
        self.assertFalse(
            acquire.validate_reusable_result(
                negative, Path("/archive"), {}, result_path=path, pins=pins
            )
        )
        tampered = dict(result, envelope="tampered")
        self.assertFalse(
            acquire.validate_reusable_result(
                tampered, Path("/archive"), {}, result_path=path, pins=pins
            )
        )
        self.assertEqual(2_000, ACQUISITION_RESULT_TIME_WITNESS_TOLERANCE_MS)
        self.assertEqual(10_000, ACQUISITION_LEGACY_FINALIZATION_WINDOW_MS)

    def test_acquisition_result_adapter_stamps_future_results_at_write_boundary(self) -> None:
        acquire, _validation_inputs, writes = self.acquisition_result_adapter_fixture()
        result = self.legacy_acquisition_result()
        path = Path(result["result_path"])

        acquire.atomic_write_json(path, result)

        self.assertEqual("2026-08-30T20:49:25Z", result["completed_at"])
        for rows, field in (
            ("sources", "observed_at"),
            ("sources", "created_at"),
            ("sources", "updated_at"),
            ("media_objects", "first_cataloged_at"),
            ("media_locations", "verified_at"),
            ("media_sources", "retrieved_at"),
        ):
            self.assertEqual(
                "2026-08-30T20:49:25Z",
                result["catalog_records"][rows][0][field],
            )
        self.assertEqual(result, writes[-1][1])
        self.assertTrue(
            acquire.validate_reusable_result(result, Path("/archive"), {})
        )

        sidecar = {"schema_version": 1, "status": "partial"}
        acquire.atomic_write_json(Path("/archive/resume.json"), sidecar)
        self.assertEqual(sidecar, writes[-1][1])

    def test_acquisition_result_adapter_is_idempotent_and_detects_replacement(self) -> None:
        acquire, _validation_inputs, _writes = self.acquisition_result_adapter_fixture()
        runner = ModuleType(f"queue_result_repeat_fixture_{time.time_ns()}")
        runner.__file__ = str(
            Path(__file__).resolve().parents[2] / "acquisition" / "queue_runner.py"
        )
        runner.IMPLEMENTATION_VERSION = QUEUE_RUNNER_LEGACY_VERSION
        runner.acquire = acquire
        installed = acquire.validate_reusable_result
        _install_acquisition_result_finalization_adapter(runner)
        self.assertIs(installed, acquire.validate_reusable_result)
        acquire.validate_reusable_result = lambda *_args, **_kwargs: True
        with self.assertRaisesRegex(BackendError, "adapter was replaced"):
            _install_acquisition_result_finalization_adapter(runner)

    def test_acquisition_result_adapter_ignores_unreviewed_source_bytes(self) -> None:
        acquire = ModuleType(f"unreviewed_acquire_fixture_{time.time_ns()}")
        acquire_source = self.config.state_root / "unreviewed-acquire.py"
        acquire_source.write_text("# not the pinned producer\n", encoding="utf-8")
        acquire.__file__ = str(acquire_source)
        acquire.IMPLEMENTATION_VERSION = ACQUISITION_LEGACY_VERSION
        validator = lambda *_args, **_kwargs: False
        writer = lambda *_args, **_kwargs: None
        acquire.validate_reusable_result = validator
        acquire.atomic_write_json = writer
        acquire._result_timestamp = lambda _value: None
        acquire.utc_now = lambda: "2026-08-30T00:00:00Z"
        runner = ModuleType(f"unreviewed_queue_fixture_{time.time_ns()}")
        runner.__file__ = str(
            Path(__file__).resolve().parents[2] / "acquisition" / "queue_runner.py"
        )
        runner.IMPLEMENTATION_VERSION = QUEUE_RUNNER_LEGACY_VERSION
        runner.acquire = acquire

        _install_acquisition_result_finalization_adapter(runner)

        self.assertIs(validator, acquire.validate_reusable_result)
        self.assertIs(writer, acquire.atomic_write_json)

    def acquisition_directory_adapter_fixture(self):
        repository = Path(__file__).resolve().parents[2]
        acquire_path = repository / "acquisition" / "acquire.py"
        spec = importlib.util.spec_from_file_location(
            f"acquisition_directory_fixture_{id(self)}_{time.time_ns()}",
            acquire_path,
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("could not load sealed acquisition fixture")
        acquire = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(acquire)
        runner = ModuleType(f"queue_directory_fixture_{id(self)}_{time.time_ns()}")
        runner.__file__ = str(repository / "acquisition" / "queue_runner.py")
        runner.IMPLEMENTATION_VERSION = QUEUE_RUNNER_LEGACY_VERSION
        runner.acquire = acquire
        _install_acquisition_directory_identity_adapter(runner)
        return acquire, runner

    def test_acquisition_directory_adapter_allows_sibling_publication_only(self) -> None:
        acquire, _runner = self.acquisition_directory_adapter_fixture()
        output_root = Path(self.temporary.name) / "directory-adapter"
        result_path = output_root / "jobs" / "existing-job" / "order" / "result.json"
        result_path.parent.mkdir(parents=True)
        result_path.write_bytes(b"{}\n")

        pinned = acquire.PinnedRegularFile.open(
            result_path,
            root=output_root,
            maximum=1024,
            capture=True,
            label="durable acquisition result",
        )
        try:
            root_before = output_root.stat()
            jobs_before = (output_root / "jobs").stat()
            (output_root / "concurrent-root-sibling").mkdir()
            (output_root / "jobs" / "concurrent-job-sibling").mkdir()
            root_after = output_root.stat()
            jobs_after = (output_root / "jobs").stat()

            legacy = acquire._stat_fingerprint.__wrapped__
            self.assertNotEqual(legacy(root_before), legacy(root_after))
            self.assertNotEqual(legacy(jobs_before), legacy(jobs_after))
            self.assertEqual(
                acquire._stat_fingerprint(root_before),
                acquire._stat_fingerprint(root_after),
            )
            self.assertEqual(
                acquire._stat_fingerprint(jobs_before),
                acquire._stat_fingerprint(jobs_after),
            )
            pinned.verify()

            jobs = output_root / "jobs"
            jobs.rename(output_root / "jobs-replaced")
            jobs.mkdir()
            with self.assertRaisesRegex(
                acquire.AcquisitionError,
                "path identity changed at component jobs",
            ):
                pinned.verify()
        finally:
            pinned.close()

    def test_acquisition_directory_adapter_is_idempotent_and_detects_replacement(
        self,
    ) -> None:
        acquire, runner = self.acquisition_directory_adapter_fixture()
        installed = acquire._stat_fingerprint
        _install_acquisition_directory_identity_adapter(runner)
        self.assertIs(installed, acquire._stat_fingerprint)
        acquire._stat_fingerprint = lambda _value: ()
        with self.assertRaisesRegex(
            BackendError, "directory identity adapter was replaced"
        ):
            _install_acquisition_directory_identity_adapter(runner)

    def test_acquisition_directory_adapter_required_mode_rejects_unreviewed_source(
        self,
    ) -> None:
        acquire = ModuleType(f"unreviewed_directory_fixture_{time.time_ns()}")
        acquire_source = self.config.state_root / "unreviewed-directory-acquire.py"
        acquire_source.write_text("# not the pinned producer\n", encoding="utf-8")
        acquire.__file__ = str(acquire_source)
        acquire.IMPLEMENTATION_VERSION = ACQUISITION_LEGACY_VERSION
        fingerprint = lambda _value: ()
        acquire._stat_fingerprint = fingerprint
        runner = ModuleType(f"unreviewed_directory_runner_{time.time_ns()}")
        runner.__file__ = str(
            Path(__file__).resolve().parents[2] / "acquisition" / "queue_runner.py"
        )
        runner.IMPLEMENTATION_VERSION = QUEUE_RUNNER_LEGACY_VERSION
        runner.acquire = acquire

        self.assertFalse(_install_acquisition_directory_identity_adapter(runner))
        self.assertIs(fingerprint, acquire._stat_fingerprint)
        with self.assertRaisesRegex(
            BackendError,
            "required acquisition directory adapter source binding differs",
        ):
            _install_acquisition_directory_identity_adapter(runner, required=True)

    def test_acquisition_directory_adapter_rejects_rebound_primitive(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        acquire_path = repository / "acquisition" / "acquire.py"
        spec = importlib.util.spec_from_file_location(
            f"rebound_directory_fixture_{id(self)}_{time.time_ns()}",
            acquire_path,
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("could not load sealed acquisition fixture")
        acquire = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(acquire)
        acquire._stat_fingerprint = lambda _value: ()
        runner = ModuleType(f"rebound_directory_runner_{time.time_ns()}")
        runner.__file__ = str(repository / "acquisition" / "queue_runner.py")
        runner.IMPLEMENTATION_VERSION = QUEUE_RUNNER_LEGACY_VERSION
        runner.acquire = acquire

        with self.assertRaisesRegex(
            BackendError,
            "stat fingerprint primitive is not the reviewed source binding",
        ):
            _install_acquisition_directory_identity_adapter(runner, required=True)

    def test_rebuild_from_events_constructs_and_restores_a_fresh_root(self) -> None:
        modules = SimpleNamespace()
        executor = object()
        backend = SealedArchiveBackend(
            self.config, modules=modules, gpu_executor=executor  # type: ignore[arg-type]
        )
        events = ({"event_type": "test"},)
        with mock.patch.object(
            SealedArchiveBackend,
            "restore",
            autospec=True,
            return_value={"restored": 1},
        ) as restore:
            rebuilt, recovery = backend.rebuild_from_events(events)  # type: ignore[arg-type]
        self.assertIsNot(backend, rebuilt)
        self.assertIs(modules, rebuilt.modules)
        self.assertIs(executor, rebuilt._gpu_executor)
        self.assertIsNone(rebuilt._gpu_opportunity_lease)
        self.assertEqual({"restored": 1}, recovery)
        restore.assert_called_once_with(rebuilt, events)
        fork = backend.fork_lane("preprocess")
        with self.assertRaisesRegex(BackendError, "stage-confined"):
            fork.rebuild_from_events(events)  # type: ignore[arg-type]

    def test_receipt_snapshot_adapter_retries_only_the_read_only_scan(self) -> None:
        class ReceiptSnapshotError(RuntimeError):
            pass

        background = ModuleType("receipt_snapshot_transient_fixture")
        background.BackgroundProducerError = ReceiptSnapshotError
        scan_calls = 0
        producer_calls = 0

        def scan(root):
            nonlocal scan_calls
            scan_calls += 1
            if scan_calls == 1:
                raise ReceiptSnapshotError(
                    "preprocess receipts directory has an unsupported entry"
                )
            return [Path(root) / "000001.json"]

        def run_producer(root):
            nonlocal producer_calls
            producer_calls += 1
            return background._preprocess_receipt_paths(root)

        background._preprocess_receipt_paths = scan
        background.run_producer = run_producer
        _install_preprocess_receipt_snapshot_adapter(background)
        installed = background._preprocess_receipt_paths
        with mock.patch(
            "autonomous_controller.sealed_backend.time.sleep"
        ) as pause:
            self.assertEqual(
                [self.config.state_root / "000001.json"],
                background.run_producer(self.config.state_root),
            )
        self.assertEqual(1, producer_calls)
        self.assertEqual(2, scan_calls)
        pause.assert_called_once()
        self.assertIs(scan, installed.__wrapped__)

        _install_preprocess_receipt_snapshot_adapter(background)
        self.assertIs(installed, background._preprocess_receipt_paths)

    def test_receipt_snapshot_adapter_bounds_or_rejects_other_failures(self) -> None:
        class ReceiptSnapshotError(RuntimeError):
            pass

        def module_with(scanner, name):
            background = ModuleType(name)
            background.BackgroundProducerError = ReceiptSnapshotError
            background._preprocess_receipt_paths = scanner
            _install_preprocess_receipt_snapshot_adapter(background)
            return background

        unrelated_calls = 0

        def unrelated(_root):
            nonlocal unrelated_calls
            unrelated_calls += 1
            raise ReceiptSnapshotError("unrelated receipt failure")

        unrelated_background = module_with(
            unrelated, "receipt_snapshot_unrelated_fixture"
        )
        with (
            mock.patch("autonomous_controller.sealed_backend.time.sleep") as pause,
            self.assertRaisesRegex(ReceiptSnapshotError, "unrelated receipt failure"),
        ):
            unrelated_background._preprocess_receipt_paths(self.config.state_root)
        self.assertEqual(1, unrelated_calls)
        pause.assert_not_called()

        persistent_calls = 0

        def persistent(_root):
            nonlocal persistent_calls
            persistent_calls += 1
            raise ReceiptSnapshotError(
                "preprocess receipts directory has an unsupported entry"
            )

        persistent_background = module_with(
            persistent, "receipt_snapshot_persistent_fixture"
        )
        with (
            mock.patch("autonomous_controller.sealed_backend.time.sleep") as pause,
            self.assertRaisesRegex(
                ReceiptSnapshotError,
                "preprocess receipts directory has an unsupported entry",
            ),
        ):
            persistent_background._preprocess_receipt_paths(self.config.state_root)
        self.assertEqual(PREPROCESS_RECEIPT_SNAPSHOT_ATTEMPTS, persistent_calls)
        self.assertEqual(PREPROCESS_RECEIPT_SNAPSHOT_ATTEMPTS - 1, pause.call_count)

        persistent_background._preprocess_receipt_paths = lambda _root: []
        with self.assertRaisesRegex(BackendError, "adapter was replaced"):
            _install_preprocess_receipt_snapshot_adapter(persistent_background)

    def test_operational_replay_integrates_mutating_producer_handoff_and_lane_forks(self) -> None:
        historical = b"historical-media" * 128
        added = b"new-media" * 257
        fixture = QueueFixture(self.config.state_root.parent, "backend-replay", [historical, None])
        queue = fixture.module

        class QueueDeadlineError(Exception):
            pass

        queue.QueueDeadlineError = QueueDeadlineError
        queue._capacity_allows = lambda *_args, **_kwargs: True
        queue._dispatch_one = lambda *_args, **_kwargs: ({}, "completed")
        queue._reservation_bytes = lambda _order: 1

        reference = {
            **self.config.document["campaign"]["schedules"][0],
            "path": str(fixture.root / "schedule.json"),
        }
        schedule_body = b'{"integration":"operational-replay"}\n'
        reference["sha256"] = hashlib.sha256(schedule_body).hexdigest()
        self.config.document["campaign"]["schedules"][0] = reference
        schedule = {
            "schedule_id": reference["schedule_id"],
            "queue": {
                "manifest_path": str(fixture.manifest_path),
                "manifest_sha256": hashlib.sha256(fixture.body).hexdigest(),
                "bundle_id": fixture.bundle_id,
            },
            "consumer": {
                "preprocess_state_root": str(fixture.root / "preprocess-state")
            },
        }

        class ReplayBackground:
            def __init__(inner_self) -> None:
                inner_self.last_states = None

            def load_schedule(inner_self, path):
                return schedule, Path(path), schedule_body

            @staticmethod
            def _completed_state(state):
                return state is not None

            @staticmethod
            def _quarantined_state(_state):
                return False

            def ready(inner_self, states):
                rows = [
                    {
                        "ordinal": ordinal,
                        "media_byte_count": state["byte_count"],
                    }
                    for ordinal, state in enumerate(states, 1)
                    if state is not None
                ]
                return {
                    "completed_acquisition_count": len(rows),
                    "quarantined_acquisition_count": 0,
                    "acknowledged_preprocess_count": 0,
                    "ready_item_count": len(rows),
                    "ready_byte_count": sum(
                        row["media_byte_count"] for row in rows
                    ),
                    "zone": "at_or_below_low_water",
                    "items": rows,
                }

            def _load_runtime(inner_self, _schedule):
                first = queue._scan_results(fixture.bundle)
                second = queue._scan_results(fixture.bundle)
                case.assertEqual(first, second)
                inner_self.last_states = second
                return fixture.bundle, second, inner_self.ready(second), {}

            def run_producer(inner_self, _path, **_limits):
                _bundle, initial, _ready, _summary = inner_self._load_runtime(
                    schedule
                )
                case.assertIsNone(initial[1])
                fixture.publish(2, added)
                queue._inspect_result(fixture.orders[1])
                final = queue._scan_results(fixture.bundle)
                inner_self.last_states = final
                return {
                    "status": "completed",
                    "stop_reason": "all_acquired",
                    "ready_after": inner_self.ready(final),
                    "queue_summary": {
                        "new_item_count": 1,
                        "new_byte_count": len(added),
                        "new_failed_attempt_count": 0,
                        "new_quarantined_count": 0,
                        "retryable_failed_count": 0,
                        "failed_attempt_count": 0,
                    },
                }

            def _runtime_from_queue_summary(
                inner_self, _schedule, _summary, **_kwargs
            ):
                states = list(inner_self.last_states)
                return fixture.bundle, states, inner_self.ready(states)

        class ReplayHandoff:
            def run_handoff(inner_self, _path, **_kwargs):
                for _ in range(4):
                    queue._scan_results(fixture.bundle)
                states = list(background.last_states)
                return {
                    "ready_after": background.ready(states),
                    "processed_items": [],
                    "stop_reason": "fixture-held",
                }

        case = self
        background = ReplayBackground()
        handoff = ReplayHandoff()
        modules = SimpleNamespace(
            background=background,
            queue_runner=queue,
            handoff=handoff,
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        replay_store = backend._operational_replay_store
        self.assertIsNotNone(replay_store)

        runtime = backend._schedule_runtime(reference)
        deep_calls = fixture.payload_hash_calls
        deep_bytes = fixture.payload_hash_bytes
        self.assertEqual(4, deep_calls)
        self.assertEqual(4 * len(historical), deep_bytes)
        self.assertEqual(runtime, backend._schedule_runtime(reference))
        self.assertEqual(deep_calls, fixture.payload_hash_calls)
        backend._runtime_cache[reference["schedule_id"]] = runtime

        preprocess_lane = backend.fork_lane("preprocess")
        self.assertIs(replay_store, preprocess_lane._operational_replay_store)
        selected = (reference, *runtime)
        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[selected]),
            mock.patch.object(backend, "_validate_cold_storage_identity"),
        ):
            acquisition = backend._run_acquisition()
        self.assertTrue(acquisition.progressed)
        self.assertEqual(deep_calls + 2, fixture.payload_hash_calls)
        self.assertEqual(deep_bytes + 2 * len(added), fixture.payload_hash_bytes)
        acquisition_replay = acquisition.artifacts[
            TRANSIENT_PEER_RUNTIME_ARTIFACT
        ]["operational_replay"]
        self.assertEqual([2], acquisition_replay["new_exact_ordinals"])
        self.assertEqual(2, replay_store.snapshot_summary(
            backend._queue_replay_binding(reference, schedule)
        )["completed_count"])

        preprocess_lane.observe_peer_outcome(acquisition)
        replayed_runtime = preprocess_lane._runtime_cache[reference["schedule_id"]]
        before_handoff_calls = fixture.payload_hash_calls
        before_handoff_bytes = fixture.payload_hash_bytes
        with mock.patch.object(
            preprocess_lane,
            "_campaign_runtimes",
            return_value=[(reference, *replayed_runtime)],
        ):
            preprocess = preprocess_lane._run_preprocess()
        self.assertFalse(preprocess.progressed)
        self.assertEqual(before_handoff_calls, fixture.payload_hash_calls)
        self.assertEqual(before_handoff_bytes, fixture.payload_hash_bytes)
        preprocess_replay = preprocess.artifacts[
            TRANSIENT_PEER_RUNTIME_ARTIFACT
        ]["operational_replay"]
        self.assertEqual(8, preprocess_replay["fast_reused_items"])
        backend.observe_peer_outcome(preprocess)

        acquisition_lane = backend.fork_lane("acquisition")
        gpu_lane = backend.fork_lane("gpu_readiness")
        self.assertIs(
            acquisition_lane._operational_replay_store,
            gpu_lane._operational_replay_store,
        )
        barrier = threading.Barrier(2)
        errors = []

        def scan_on_lane(lane):
            try:
                with lane._operational_queue_replay(reference, schedule):
                    barrier.wait(timeout=2)
                    queue._scan_results(fixture.bundle)
            except BaseException as error:
                errors.append(error)

        threads = [
            threading.Thread(target=scan_on_lane, args=(acquisition_lane,)),
            threading.Thread(target=scan_on_lane, args=(gpu_lane,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)
        self.assertEqual([], errors)
        self.assertEqual(before_handoff_calls, fixture.payload_hash_calls)

        with mock.patch.object(
            SealedArchiveBackend,
            "restore",
            autospec=True,
            return_value={"restored": 1},
        ):
            rebuilt, _recovery = backend.rebuild_from_events(())
        self.assertIsNot(
            backend._operational_replay_store,
            rebuilt._operational_replay_store,
        )
        before_rebuild_bytes = fixture.payload_hash_bytes
        rebuilt._schedule_runtime(reference)
        self.assertEqual(
            before_rebuild_bytes + 4 * (len(historical) + len(added)),
            fixture.payload_hash_bytes,
        )

    def test_superseded_acquisition_summary_skips_future_receipt_replay(self) -> None:
        first_payload = b"first-generation-media" * 64
        second_payload = b"second-generation-media" * 96
        fixture = QueueFixture(
            self.config.state_root.parent,
            "stale-peer-acquisition",
            [None, None],
        )
        queue = fixture.module
        reference = {
            **self.config.document["campaign"]["schedules"][0],
            "path": str(fixture.root / "schedule.json"),
        }
        schedule_body = b'{"fixture":"stale-peer-acquisition"}\n'
        reference["sha256"] = hashlib.sha256(schedule_body).hexdigest()
        self.config.document["campaign"]["schedules"][0] = reference
        schedule = {
            "schedule_id": reference["schedule_id"],
            "queue": {
                "manifest_path": str(fixture.manifest_path),
                "manifest_sha256": hashlib.sha256(fixture.body).hexdigest(),
                "bundle_id": fixture.bundle_id,
            },
            "consumer": {
                "preprocess_state_root": str(fixture.root / "preprocess-state")
            },
        }

        class ReceiptConflictError(RuntimeError):
            pass

        class ReceiptAwareBackground:
            BackgroundProducerError = ReceiptConflictError

            def __init__(inner_self) -> None:
                inner_self.peer_summary_calls = 0
                inner_self.runtime_loads = 0
                inner_self.peer_summary_behavior = None

            def load_schedule(inner_self, path):
                return schedule, Path(path), schedule_body

            @staticmethod
            def ready(states):
                rows = [
                    {
                        "ordinal": ordinal,
                        "media_byte_count": state["byte_count"],
                    }
                    for ordinal, state in enumerate(states, 1)
                    if state is not None
                ]
                return {
                    "completed_acquisition_count": len(rows),
                    "quarantined_acquisition_count": 0,
                    "acknowledged_preprocess_count": 1,
                    "ready_item_count": len(rows) - 1,
                    "ready_byte_count": sum(
                        row["media_byte_count"] for row in rows[1:]
                    ),
                    "zone": "at_or_below_low_water",
                    "items": rows[1:],
                }

            def _load_runtime(inner_self, _schedule):
                inner_self.runtime_loads += 1
                states = queue._scan_results(fixture.bundle)
                return fixture.bundle, states, inner_self.ready(states), {}

            def _runtime_from_queue_summary(
                inner_self, _schedule, _summary, **_kwargs
            ):
                inner_self.peer_summary_calls += 1
                if inner_self.peer_summary_behavior is not None:
                    return inner_self.peer_summary_behavior()
                raise ReceiptConflictError(
                    "preprocess receipt acknowledges a non-completed queue result"
                )

        background = ReceiptAwareBackground()
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(background=background, queue_runner=queue),
        )  # type: ignore[arg-type]
        replay_store = backend._operational_replay_store
        self.assertIsNotNone(replay_store)
        binding = backend._queue_replay_binding(reference, schedule)
        with replay_store.deep_capture(binding):
            queue._scan_results(fixture.bundle)
            queue._scan_results(fixture.bundle)

        fixture.publish(1, first_payload)
        with replay_store.operational_session(binding) as historical_session:
            historical_states = queue._scan_results(fixture.bundle)
        historical_delta = historical_session.delta
        self.assertIsNotNone(historical_delta)

        # Hold one returned one-item projection open while another session admits
        # the second item. Its later commit merges to the current generation even
        # though the queue summary it returned remains historical.
        stale_scanned = threading.Event()
        current_committed = threading.Event()
        merged: dict[str, Any] = {}
        merge_errors: list[BaseException] = []

        def commit_returned_stale_projection() -> None:
            try:
                with replay_store.operational_session(binding) as session:
                    merged["states"] = queue._scan_results(fixture.bundle)
                    stale_scanned.set()
                    if not current_committed.wait(timeout=5):
                        raise AssertionError("current projection did not commit")
                merged["delta"] = session.delta
            except BaseException as error:
                merge_errors.append(error)

        stale_worker = threading.Thread(target=commit_returned_stale_projection)
        stale_worker.start()
        self.assertTrue(stale_scanned.wait(timeout=5))

        # This models another acquisition generation becoming visible and then
        # preprocessing durably acknowledging it while the GPU lane still holds
        # the older acquisition outcome.
        fixture.publish(2, second_payload)
        with replay_store.operational_session(binding) as current_session:
            current_states = queue._scan_results(fixture.bundle)
        current_committed.set()
        stale_worker.join(timeout=5)
        self.assertFalse(stale_worker.is_alive())
        self.assertEqual([], merge_errors)
        self.assertGreater(
            current_session.delta["generation"],
            historical_delta["generation"],
        )
        self.assertTrue(all(state is not None for state in current_states))
        merged_delta = merged["delta"]
        self.assertEqual(
            current_session.delta["generation"], merged_delta["generation"]
        )
        self.assertEqual(1, merged_delta["session_completed_count"])
        self.assertEqual(2, merged_delta["completed_count"])

        def outcome(delta):
            return StageOutcome(
                "acquisition",
                "progressed",
                True,
                {"active_schedule_id": reference["schedule_id"]},
                {
                    "backend_kind": "himr_sealed_archive_backend_v1",
                    TRANSIENT_PEER_RUNTIME_ARTIFACT: {
                        "kind": "acquisition_queue_summary_v1",
                        "schedule_id": reference["schedule_id"],
                        "queue_summary": {"generation": "historical"},
                        "operational_replay": delta,
                    },
                },
            )

        stale_runtime = (
            schedule,
            fixture.bundle,
            historical_states,
            background.ready(historical_states),
        )
        backend._runtime_cache[reference["schedule_id"]] = stale_runtime
        invalid_delta = {**historical_delta, "delta_sha256": "0" * 64}
        with self.assertRaisesRegex(BackendError, "delta failed"):
            backend.observe_peer_outcome(outcome(invalid_delta))

        # Equal commit generations are not sufficient authority: the separately
        # bound returned projection proves this queue summary was already stale.
        backend._runtime_cache[reference["schedule_id"]] = stale_runtime
        calls_before_merged = background.peer_summary_calls
        backend.observe_peer_outcome(outcome(merged_delta))
        self.assertEqual(calls_before_merged, background.peer_summary_calls)
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

        # Preprocess peers do not carry replacement runtime objects. The same
        # returned-stale delta must invalidate their local projection so the next
        # finite stage reloads exact merged authority instead of retaining a
        # historical ready count.
        backend._runtime_cache[reference["schedule_id"]] = stale_runtime
        backend._observe_preprocess_runtime(
            {
                "kind": "preprocess_operational_replay_v1",
                "schedule_id": reference["schedule_id"],
                "operational_replay": merged_delta,
            },
            {"active_schedule_id": reference["schedule_id"]},
        )
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

        # A current peer replay generation still does not bind the receiver's
        # independently materialized cache tuple. Invalidate it before exact
        # singleton bundle validation just as for a returned-stale projection.
        backend._runtime_cache[reference["schedule_id"]] = (
            schedule,
            fixture.bundle,
            current_states,
            background.ready(current_states),
        )
        backend._observe_preprocess_runtime(
            {
                "kind": "preprocess_operational_replay_v1",
                "schedule_id": reference["schedule_id"],
                "operational_replay": current_session.delta,
            },
            {"active_schedule_id": reference["schedule_id"]},
        )
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

        backend._runtime_cache[reference["schedule_id"]] = stale_runtime
        before_reload_hashes = fixture.payload_hash_calls
        backend.observe_peer_outcome(outcome(historical_delta))
        self.assertEqual(0, background.peer_summary_calls)
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

        refreshed = backend._schedule_runtime(reference)
        self.assertTrue(all(state is not None for state in refreshed[2]))
        self.assertEqual(1, background.runtime_loads)
        self.assertEqual(before_reload_hashes, fixture.payload_hash_calls)

        def advance_generation(state):
            Path(state["result"]["admission"]["path"]).touch()
            with replay_store.operational_session(binding) as session:
                queue._scan_results(fixture.bundle)
            return session.delta

        # The precheck sees equality. During the summary replay, a concurrent
        # exact session advances authority and the historical receipt replay
        # fails. The observer must re-check, discard, and return normally.
        advanced_deltas = []

        def advance_then_fail():
            advanced_deltas.append(advance_generation(current_states[0]))
            raise ReceiptConflictError(
                "preprocess receipt acknowledges a non-completed queue result"
            )

        background.peer_summary_behavior = advance_then_fail
        backend._runtime_cache[reference["schedule_id"]] = refreshed
        calls_before_race = background.peer_summary_calls
        backend.observe_peer_outcome(outcome(current_session.delta))
        self.assertEqual(calls_before_race + 1, background.peer_summary_calls)
        self.assertGreater(
            advanced_deltas[-1]["generation"],
            current_session.delta["generation"],
        )
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

        # A successful replay is provisional too. Inject another exact advance
        # immediately before it returns and prove those obsolete objects never
        # replace the lane cache.
        successful_runtime = (
            fixture.bundle,
            current_states,
            background.ready(current_states),
        )

        def advance_then_succeed():
            advanced_deltas.append(advance_generation(current_states[1]))
            return successful_runtime

        background.peer_summary_behavior = advance_then_succeed
        backend._runtime_cache[reference["schedule_id"]] = refreshed
        peer_before_success_race = advanced_deltas[-1]
        backend.observe_peer_outcome(outcome(peer_before_success_race))
        self.assertGreater(
            advanced_deltas[-1]["generation"],
            peer_before_success_race["generation"],
        )
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

        # Advance after summary replay succeeds but immediately before guarded
        # cache admission. The atomic admission check must reject those now-stale
        # objects rather than recreating the old post-check/cache-write window.
        background.peer_summary_behavior = lambda: successful_runtime
        backend._runtime_cache[reference["schedule_id"]] = refreshed
        peer_before_admission_race = advanced_deltas[-1]
        guarded_admission = replay_store.admit_if_snapshot_current

        def advance_before_admission(*args, **kwargs):
            advanced_deltas.append(advance_generation(current_states[0]))
            return guarded_admission(*args, **kwargs)

        with mock.patch.object(
            replay_store,
            "admit_if_snapshot_current",
            side_effect=advance_before_admission,
        ):
            backend.observe_peer_outcome(outcome(peer_before_admission_race))
        self.assertGreater(
            advanced_deltas[-1]["generation"],
            peer_before_admission_race["generation"],
        )
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

        # Even with a proven concurrent generation advance, no other source,
        # controller, or programming failure may be relabelled as temporal.
        for invalid_error in (
            ReceiptConflictError("queue summary is malformed"),
            BackendError("malformed summary BackendError"),
            RuntimeError("arbitrary programming failure"),
        ):
            with self.subTest(invalid_error=type(invalid_error).__name__):
                peer_before_invalid_error = advanced_deltas[-1]

                def advance_then_invalid(error=invalid_error):
                    advanced_deltas.append(advance_generation(current_states[0]))
                    raise error

                background.peer_summary_behavior = advance_then_invalid
                backend._runtime_cache[reference["schedule_id"]] = refreshed
                with self.assertRaisesRegex(BackendError, str(invalid_error)):
                    backend.observe_peer_outcome(
                        outcome(peer_before_invalid_error)
                    )
                self.assertGreater(
                    advanced_deltas[-1]["generation"],
                    peer_before_invalid_error["generation"],
                )
                self.assertNotIn(
                    reference["schedule_id"], backend._runtime_cache
                )

        # An error without a proven generation advance is still a real exact
        # replay failure and must never be hidden by the stale-summary handling.
        def fail_without_advance():
            raise RuntimeError("equal-generation summary failure")

        background.peer_summary_behavior = fail_without_advance
        backend._runtime_cache[reference["schedule_id"]] = refreshed
        with self.assertRaisesRegex(
            BackendError, "equal-generation summary failure"
        ):
            backend.observe_peer_outcome(outcome(advanced_deltas[-1]))
        self.assertNotIn(reference["schedule_id"], backend._runtime_cache)

    def test_visible_preprocess_receipt_before_commit_reloads_physical_authority(self) -> None:
        fixture = QueueFixture(
            self.config.state_root.parent,
            "receipt-before-replay-commit",
            [b"historical-media" * 32, None],
        )
        queue = fixture.module
        reference = {
            **self.config.document["campaign"]["schedules"][0],
            "path": str(fixture.root / "schedule.json"),
        }
        schedule_body = b'{"fixture":"receipt-before-replay-commit"}\n'
        reference["sha256"] = hashlib.sha256(schedule_body).hexdigest()
        self.config.document["campaign"]["schedules"][0] = reference
        schedule = {
            "schedule_id": reference["schedule_id"],
            "queue": {
                "manifest_path": str(fixture.manifest_path),
                "manifest_sha256": hashlib.sha256(fixture.body).hexdigest(),
                "bundle_id": fixture.bundle_id,
            },
            "consumer": {
                "preprocess_state_root": str(fixture.root / "preprocess-state")
            },
        }
        receipt_path = fixture.root / "preprocess-state" / "receipt.json"

        class ReceiptConflictError(RuntimeError):
            pass

        class ReceiptAwareBackground:
            BackgroundProducerError = ReceiptConflictError

            def __init__(inner_self) -> None:
                inner_self.peer_summary_calls = 0
                inner_self.runtime_loads = 0

            def load_schedule(inner_self, path):
                return schedule, Path(path), schedule_body

            @staticmethod
            def ready(states):
                completed = sum(state is not None for state in states)
                return {
                    "completed_acquisition_count": completed,
                    "quarantined_acquisition_count": 0,
                    "acknowledged_preprocess_count": int(receipt_path.exists()),
                    "ready_item_count": completed - int(receipt_path.exists()),
                    "ready_byte_count": 0,
                    "zone": "at_or_below_low_water",
                    "items": [],
                }

            def _load_runtime(inner_self, _schedule):
                inner_self.runtime_loads += 1
                if not receipt_path.is_file():
                    raise AssertionError("physical preprocess receipt was not visible")
                states = queue._scan_results(fixture.bundle)
                return fixture.bundle, states, inner_self.ready(states), {}

            def _runtime_from_queue_summary(
                inner_self, _schedule, _summary, **_kwargs
            ):
                inner_self.peer_summary_calls += 1
                raise ReceiptConflictError(
                    "preprocess receipt acknowledges a non-completed queue result"
                )

        background = ReceiptAwareBackground()
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(background=background, queue_runner=queue),
        )  # type: ignore[arg-type]
        replay_store = backend._operational_replay_store
        self.assertIsNotNone(replay_store)
        binding = backend._queue_replay_binding(reference, schedule)
        with replay_store.deep_capture(binding) as initial_session:
            first_states = queue._scan_results(fixture.bundle)
            queue._scan_results(fixture.bundle)
        initial_delta = initial_session.delta
        self.assertEqual(1, initial_delta["completed_count"])

        receipt_visible = threading.Event()
        release_preprocess_commit = threading.Event()
        worker_errors: list[BaseException] = []

        def hold_preprocess_commit() -> None:
            try:
                fixture.publish(2, b"concurrent-media" * 64)
                with replay_store.operational_session(binding):
                    states = queue._scan_results(fixture.bundle)
                    if not all(state is not None for state in states):
                        raise AssertionError("preprocess did not observe completed input")
                    receipt_path.parent.mkdir(mode=0o700)
                    receipt_path.write_text("{}\n", encoding="utf-8")
                    receipt_visible.set()
                    if not release_preprocess_commit.wait(timeout=5):
                        raise AssertionError("test did not release preprocess commit")
            except BaseException as error:
                worker_errors.append(error)

        worker = threading.Thread(target=hold_preprocess_commit)
        worker.start()
        self.assertTrue(receipt_visible.wait(timeout=5))
        # The physical result and receipt are visible, but the held session has
        # not yet advanced shared replay authority.
        self.assertEqual(1, replay_store.snapshot_summary(binding)["completed_count"])

        backend._runtime_cache[reference["schedule_id"]] = (
            schedule,
            fixture.bundle,
            first_states,
            background.ready(first_states),
        )
        outcome = StageOutcome(
            "acquisition",
            "progressed",
            True,
            {"active_schedule_id": reference["schedule_id"]},
            {
                "backend_kind": BACKEND_KIND,
                TRANSIENT_PEER_RUNTIME_ARTIFACT: {
                    "kind": "acquisition_queue_summary_v1",
                    "schedule_id": reference["schedule_id"],
                    "queue_summary": {"generation": "before-receipt"},
                    "operational_replay": initial_delta,
                },
            },
        )
        try:
            backend.observe_peer_outcome(outcome)
            refreshed = backend._runtime_cache[reference["schedule_id"]]
            self.assertTrue(all(state is not None for state in refreshed[2]))
            self.assertEqual(2, replay_store.snapshot_summary(binding)["completed_count"])
            self.assertEqual(1, background.peer_summary_calls)
            self.assertEqual(1, background.runtime_loads)
        finally:
            release_preprocess_commit.set()
            worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertEqual([], worker_errors)

    def test_acquisition_delegates_exact_bounds_to_one_campaign_schedule(self) -> None:
        background = FakeBackground()
        modules = SimpleNamespace(
            background=background, queue_runner=fake_queue_runner()
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        low = {
            "ready_item_count": 0,
            "ready_byte_count": 0,
            "items": [],
            "zone": "at_or_below_low_water",
        }
        references = self.config.document["campaign"]["schedules"]
        pending = (references[0], {}, {}, [None], low)
        other = (references[1], {}, {}, [None], {**low, "zone": "hysteresis_hold"})
        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[pending, other]),
            mock.patch.object(backend, "_validate_cold_storage_identity"),
        ):
            outcome = backend._run_acquisition()
        expected = self.config.document["acquisition"]["normal_processing"]
        self.assertEqual(
            Path(self.config.document["campaign"]["schedules"][0]["path"]),
            background.call[0],
        )
        self.assertEqual(expected, background.call[1])
        self.assertTrue(outcome.progressed)
        self.assertEqual(1, outcome.monitor["new_items"])

    def test_acquisition_returned_stale_projection_reloads_without_redispatch(self) -> None:
        background = FakeBackground()
        background._runtime_from_queue_summary = mock.Mock(
            side_effect=AssertionError("historical producer summary must not be replayed")
        )
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(
                background=background,
                queue_runner=fake_queue_runner(),
            ),
        )  # type: ignore[arg-type]
        reference = self.config.document["campaign"]["schedules"][0]
        schedule: dict[str, Any] = {}
        initial_ready = {
            "ready_item_count": 0,
            "ready_byte_count": 0,
            "items": [],
            "zone": "at_or_below_low_water",
        }
        selected = (reference, schedule, {}, [None], initial_ready)
        refreshed_bundle = {"manifest": "current"}
        refreshed_states = [{"queue_state": "completed"}]
        # Concurrent preprocessing acknowledged the newly acquired item before
        # this producer session committed, so current runnable telemetry is zero
        # even though the producer's returned ready_after says one.
        refreshed_ready = {
            "ready_item_count": 0,
            "ready_byte_count": 0,
            "items": [],
            "zone": "at_or_below_low_water",
        }
        replay_session = SimpleNamespace(delta={"returned": "stale"})
        with (
            mock.patch.object(
                backend, "_campaign_runtimes", return_value=[selected]
            ),
            mock.patch.object(backend, "_validate_cold_storage_identity"),
            mock.patch.object(
                backend,
                "_operational_queue_replay",
                return_value=nullcontext(replay_session),
            ),
            mock.patch.object(
                backend,
                "_replay_delta_returned_projection_is_current",
                return_value=False,
            ),
            mock.patch.object(
                backend,
                "_schedule_runtime",
                return_value=(
                    schedule,
                    refreshed_bundle,
                    refreshed_states,
                    refreshed_ready,
                ),
            ) as reload_runtime,
        ):
            outcome = backend._run_acquisition()
        reload_runtime.assert_called_once_with(reference)
        background._runtime_from_queue_summary.assert_not_called()
        self.assertEqual(1, outcome.monitor["completed"])
        self.assertEqual(0, outcome.monitor["ready_items"])
        self.assertEqual(1, outcome.monitor["new_items"])
        self.assertEqual(
            (schedule, refreshed_bundle, refreshed_states, refreshed_ready),
            backend._runtime_cache[reference["schedule_id"]],
        )

    def test_acquisition_stop_gate_preserves_batch_amortization_and_stops_between_files(self) -> None:
        self.config.document["acquisition"]["normal_processing"]["max_new_items"] = 8
        store = ControlStore(self.config)
        store.set_desired_state("running")
        runner = fake_queue_runner()

        class StopAwareBackground(FakeBackground):
            def run_producer(inner_self, path, **kwargs):
                inner_self.call = (path, kwargs)
                dispatched = 0
                while dispatched < kwargs["max_new_items"]:
                    if not runner._capacity_allows(
                        {},
                        free_space_floor_bytes=kwargs["free_space_floor_bytes"],
                    ):
                        break
                    dispatched += 1
                    if dispatched == 1:
                        store.set_desired_state("stopped")
                ready = {
                    "completed_acquisition_count": dispatched,
                    "ready_item_count": dispatched,
                    "ready_byte_count": dispatched * 123,
                    "items": [
                        {"ordinal": ordinal, "media_byte_count": 123}
                        for ordinal in range(1, dispatched + 1)
                    ],
                    "zone": "at_or_below_low_water",
                }
                return {
                    "status": "bounded",
                    "stop_reason": "free_space_floor",
                    "ready_after": ready,
                    "queue_summary": {
                        "new_item_count": dispatched,
                        "new_byte_count": dispatched * 123,
                    },
                }

            def _runtime_from_queue_summary(inner_self, schedule, summary, **_kwargs):
                count = summary["new_item_count"]
                return {}, [{} for _ in range(count)], {
                    "completed_acquisition_count": count,
                    "ready_item_count": count,
                    "ready_byte_count": count * 123,
                    "items": [
                        {"ordinal": ordinal, "media_byte_count": 123}
                        for ordinal in range(1, count + 1)
                    ],
                    "zone": "at_or_below_low_water",
                }

        background = StopAwareBackground()
        modules = SimpleNamespace(background=background, queue_runner=runner)
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        low = {
            "ready_item_count": 0,
            "ready_byte_count": 0,
            "items": [],
            "zone": "at_or_below_low_water",
        }
        reference = self.config.document["campaign"]["schedules"][0]
        pending = (reference, {}, {}, [None] * 8, low)
        original_gate = runner._capacity_allows
        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[pending]),
            mock.patch.object(backend, "_validate_cold_storage_identity"),
        ):
            outcome = backend._run_acquisition()
        self.assertEqual(
            8,
            background.call[1]["max_new_items"],
            "the expensive producer replay must remain amortized",
        )
        self.assertEqual(1, outcome.monitor["new_items"])
        self.assertTrue(outcome.monitor["durable_stop_observed_between_items"])
        self.assertEqual(
            "durable_stop_requested_between_items", outcome.monitor["stop_reason"]
        )
        self.assertIs(original_gate, runner._capacity_allows)

    def test_acquisition_stop_gate_closes_capacity_to_dispatch_race(self) -> None:
        store = ControlStore(self.config)
        store.set_desired_state("running")
        dispatch_called = False

        class QueueDeadlineError(Exception):
            pass

        order = {"job_id": "job-1", "limits": {"max_job_bytes": 4096}}

        def capacity(*_args, **_kwargs):
            store.set_desired_state("stopped")
            return True

        def dispatch(*_args, **_kwargs):
            nonlocal dispatch_called
            dispatch_called = True
            return {}, "completed"

        runner = SimpleNamespace(
            QueueDeadlineError=QueueDeadlineError,
            canonical_bytes=lambda value: json.dumps(
                value, sort_keys=True, separators=(",", ":")
            ).encode(),
            sha256_bytes=lambda value: hashlib.sha256(value).hexdigest(),
            _capacity_allows=capacity,
            _dispatch_one=dispatch,
            _reservation_bytes=lambda value: value["limits"]["max_job_bytes"],
        )

        class RaceBackground(FakeBackground):
            replayed_summary = None

            def run_producer(inner_self, path, **kwargs):
                inner_self.call = (path, kwargs)
                self.assertTrue(
                    runner._capacity_allows(
                        order,
                        free_space_floor_bytes=kwargs["free_space_floor_bytes"],
                    )
                )
                try:
                    runner._dispatch_one(order, 30.0)
                except QueueDeadlineError:
                    pass
                else:  # pragma: no cover - the stop gate must interrupt here
                    self.fail("dispatch crossed a durable stop boundary")
                work_order_sha = hashlib.sha256(
                    json.dumps(order, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                queue_core = {
                    "status": "bounded",
                    "new_item_count": 0,
                    "new_byte_count": 0,
                    "adapter_invocation_count": 1,
                    "dispatch_reservation_bytes": 4096,
                    "stop_reason": "max_run_seconds",
                    "results": [
                        {
                            "work_order_sha256": work_order_sha,
                            "action": "deadline_interrupted",
                            "adapter_invoked": True,
                        }
                    ],
                }
                queue = {
                    **queue_core,
                    "summary_sha256": hashlib.sha256(
                        json.dumps(
                            queue_core, sort_keys=True, separators=(",", ":")
                        ).encode()
                    ).hexdigest(),
                }
                return {
                    "status": "bounded",
                    "stop_reason": "max_run_seconds",
                    "ready_after": {
                        "completed_acquisition_count": 0,
                        "ready_item_count": 0,
                        "ready_byte_count": 0,
                        "items": [],
                        "zone": "at_or_below_low_water",
                    },
                    "queue_summary": queue,
                }

            def _runtime_from_queue_summary(inner_self, _schedule, summary, **_kwargs):
                inner_self.replayed_summary = summary
                return {}, [None], {
                    "completed_acquisition_count": 0,
                    "ready_item_count": 0,
                    "ready_byte_count": 0,
                    "items": [],
                    "zone": "at_or_below_low_water",
                }

        background = RaceBackground()
        modules = SimpleNamespace(background=background, queue_runner=runner)
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        reference = self.config.document["campaign"]["schedules"][0]
        ready = {
            "ready_item_count": 0,
            "ready_byte_count": 0,
            "items": [],
            "zone": "at_or_below_low_water",
        }
        original_capacity = runner._capacity_allows
        original_dispatch = runner._dispatch_one
        with (
            mock.patch.object(
                backend, "_campaign_runtimes", return_value=[(reference, {}, {}, [None], ready)]
            ),
            mock.patch.object(backend, "_validate_cold_storage_identity"),
        ):
            outcome = backend._run_acquisition()
        self.assertFalse(dispatch_called)
        self.assertEqual(
            "durable_stop_requested_between_items", outcome.monitor["stop_reason"]
        )
        summary = background.replayed_summary
        self.assertEqual(0, summary["adapter_invocation_count"])
        self.assertEqual(0, summary["dispatch_reservation_bytes"])
        self.assertEqual("stop_requested_before_dispatch", summary["results"][0]["action"])
        self.assertFalse(summary["results"][0]["adapter_invoked"])
        self.assertIs(original_capacity, runner._capacity_allows)
        self.assertIs(original_dispatch, runner._dispatch_one)

    def test_acquisition_stop_gate_forbids_concurrent_patch_ownership(self) -> None:
        runner = fake_queue_runner()
        modules = SimpleNamespace(background=FakeBackground(), queue_runner=runner)
        first = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        second = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        original_capacity = runner._capacity_allows
        original_dispatch = runner._dispatch_one
        with first._acquisition_stop_gate():
            with self.assertRaisesRegex(BackendError, "concurrent acquisition"):
                with second._acquisition_stop_gate():  # pragma: no cover
                    pass
        self.assertIs(original_capacity, runner._capacity_allows)
        self.assertIs(original_dispatch, runner._dispatch_one)

    def test_lane_forks_isolate_mutable_state_and_observe_targeted_peer_delta(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        first, second = [
            row["schedule_id"]
            for row in self.config.document["campaign"]["schedules"]
        ]
        backend._runtime_cache = {
            first: ({"first": True}, {}, [], {}),
            second: ({"second": True}, {}, [], {}),
        }
        backend._preprocess_candidates_cache = []
        preprocess = backend.fork_lane("preprocess")
        gpu = backend.fork_lane("gpu_readiness")
        self.assertIsNot(backend._runtime_cache, preprocess._runtime_cache)
        self.assertIsNot(preprocess._runtime_cache, gpu._runtime_cache)
        with self.assertRaisesRegex(BackendError, "cannot execute"):
            gpu.run_stage("preprocess", item_limit=1)

        bundle_id = "ppbatch_" + "1" * 32
        bundle_path = (
            Path(self.config.document["preprocess"]["bundle_root"])
            / "bundles"
            / bundle_id
        )
        outcome = StageOutcome(
            "preprocess",
            "progressed",
            True,
            {"active_schedule_id": first, "processed_items": 1},
            {
                "backend_kind": "himr_sealed_archive_backend_v1",
                "preprocess_failure_attempts": [],
                "preprocess_bundles": [
                    {
                        "queue_ordinal": 1,
                        "bundle_path": str(bundle_path),
                        "bundle_id": bundle_id,
                        "bundle_manifest_sha256": "2" * 64,
                        "campaign_ordinal": 1,
                        "preprocess_state_root": str(self.config.state_root / "pre-state"),
                        "item_count": 1,
                    }
                ],
            },
        )
        peer_runtime = (
            self.config.document["campaign"]["schedules"][0],
            {
                "consumer": {
                    "preprocess_state_root": str(
                        self.config.state_root / "pre-state"
                    )
                }
            },
            {},
            [],
            {},
        )

        def register_peer_artifact(artifact, **_kwargs):
            gpu._register_preprocess_candidate(
                campaign_ordinal=artifact["campaign_ordinal"],
                bundle_path=Path(artifact["bundle_path"]),
                bundle_id=artifact["bundle_id"],
                state_root=Path(artifact["preprocess_state_root"]),
                item_count=artifact["item_count"],
            )

        def current_peer_runtimes():
            self.assertNotIn(first, gpu._runtime_cache)
            return [peer_runtime]

        with (
            mock.patch.object(
                gpu, "_campaign_runtimes", side_effect=current_peer_runtimes
            ),
            mock.patch.object(
                gpu,
                "_observe_preprocess_bundle",
                side_effect=register_peer_artifact,
            ),
        ):
            gpu.observe_peer_outcome(outcome)
        self.assertNotIn(first, gpu._runtime_cache)
        self.assertIn(second, gpu._runtime_cache)
        self.assertEqual(
            [(1, bundle_path, bundle_id, self.config.state_root / "pre-state")],
            gpu._preprocess_candidates_cache,
        )
        self.assertIn(first, backend._runtime_cache)

    def test_peer_preprocess_bundle_is_derived_from_exact_singleton_receipt(self) -> None:
        references = self.config.document["campaign"]["schedules"]
        active_reference = references[1]
        state_root = (self.config.state_root / "peer-preprocess-state").resolve()
        bundle_id = "ppbatch_" + "6" * 32
        bundle_path = (
            Path(self.config.document["preprocess"]["bundle_root"])
            / "bundles"
            / bundle_id
        )
        manifest_sha256 = "7" * 64
        acquisition_paths = [
            str(self.config.state_root / f"acquisition-{ordinal}.json")
            for ordinal in (1, 2)
        ]
        manifest = {
            "bundle_id": bundle_id,
            "manifest_sha256": manifest_sha256,
            "work_order_count": 1,
        }
        selection = {
            "entries": [
                {"acquisition_result": {"path": acquisition_paths[1]}}
            ]
        }
        preprocess_batch = SimpleNamespace(
            validate_bundle=mock.Mock(
                return_value=(manifest, selection, [{"ordinal": 1}])
            ),
            existing_receipts=mock.Mock(return_value={1: {"completed": True}}),
        )
        modules = SimpleNamespace(
            background=SimpleNamespace(
                _completed_state=lambda value: isinstance(value, dict),
            ),
            preprocess_batch=preprocess_batch,
            queue_runner=SimpleNamespace(
                _result_path=lambda order: Path(order["result_path"])
            ),
        )
        prior_runtime = (
            references[0],
            {"consumer": {"preprocess_state_root": str(self.config.state_root / "prior")}},
            {},
            [None, None, None],
            {},
        )
        active_schedule = {
            "consumer": {"preprocess_state_root": str(state_root)}
        }
        class DirectIndexOnly(list):
            def __iter__(self):  # pragma: no cover - regression tripwire
                raise AssertionError("exact ordinal lookup must not scan the schedule")

        active_bundle = {
            "manifest": {
                "work_orders": DirectIndexOnly([
                    {"queue_ordinal": ordinal} for ordinal in (1, 2)
                ])
            },
            "orders": DirectIndexOnly([
                {"result_path": path} for path in acquisition_paths
            ]),
        }
        active_runtime = (
            active_reference,
            active_schedule,
            active_bundle,
            DirectIndexOnly([
                {"queue_state": "completed"},
                {"queue_state": "completed"},
            ]),
            {},
        )
        runtimes = [prior_runtime, active_runtime]
        artifact = {
            "queue_ordinal": 2,
            "bundle_path": str(bundle_path),
            "bundle_id": bundle_id,
            "bundle_manifest_sha256": manifest_sha256,
            "campaign_ordinal": 5,
            "preprocess_state_root": str(state_root),
            "item_count": 1,
        }

        def outcome(value, *, processed_items=1, active_schedule_id=None):
            return StageOutcome(
                "preprocess",
                "progressed",
                True,
                {
                    "active_schedule_id": (
                        active_reference["schedule_id"]
                        if active_schedule_id is None
                        else active_schedule_id
                    ),
                    "processed_items": processed_items,
                },
                {
                    "backend_kind": "himr_sealed_archive_backend_v1",
                    "preprocess_failure_attempts": [],
                    "preprocess_bundles": [value],
                },
            )

        receiver = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        receiver._preprocess_candidates_cache = []
        with mock.patch.object(
            receiver, "_campaign_runtimes", return_value=runtimes
        ):
            receiver.observe_peer_outcome(outcome(artifact))
        self.assertEqual(1, receiver._preprocessed_items_cumulative())
        self.assertEqual(
            [(5, bundle_path, bundle_id, state_root)],
            receiver._preprocess_candidates_cache,
        )

        mutations = {
            "item_count": {"item_count": 2},
            "state_root": {
                "preprocess_state_root": str(
                    self.config.state_root / "forged-state"
                )
            },
            "manifest_digest": {"bundle_manifest_sha256": "8" * 64},
            "queue_ordinal": {"queue_ordinal": 1},
            "campaign_ordinal": {"campaign_ordinal": 4},
            "bundle_path": {
                "bundle_path": str(bundle_path.parent / (bundle_id + "-forged"))
            },
            "bundle_id": {"bundle_id": "ppbatch_" + "9" * 32},
        }
        for label, changed in mutations.items():
            with self.subTest(label=label):
                forged_receiver = SealedArchiveBackend(
                    self.config, modules=modules  # type: ignore[arg-type]
                )
                forged_receiver._preprocess_candidates_cache = []
                with (
                    mock.patch.object(
                        forged_receiver,
                        "_campaign_runtimes",
                        return_value=runtimes,
                    ),
                    self.assertRaises(BackendError),
                ):
                    forged_receiver.observe_peer_outcome(
                        outcome({**artifact, **changed})
                    )
                self.assertEqual(0, forged_receiver._preprocessed_items_cumulative())

        count_receiver = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        count_receiver._preprocess_candidates_cache = []
        with self.assertRaisesRegex(BackendError, "artifacts are inconsistent"):
            count_receiver.observe_peer_outcome(
                outcome(artifact, processed_items=2)
            )

        schedule_receiver = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        schedule_receiver._preprocess_candidates_cache = []
        with self.assertRaisesRegex(BackendError, "artifacts are inconsistent"):
            schedule_receiver.observe_peer_outcome(
                outcome(artifact, active_schedule_id=123)
            )

        missing_receiver = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        missing_receiver._preprocess_candidates_cache = []
        with (
            mock.patch.object(
                missing_receiver, "_campaign_runtimes", return_value=runtimes
            ),
            mock.patch.object(
                preprocess_batch, "existing_receipts", return_value={}
            ),
            self.assertRaisesRegex(BackendError, "singleton receipt replay"),
        ):
            missing_receiver.observe_peer_outcome(outcome(artifact))

    def test_repeated_exact_preprocess_failures_keep_cached_cumulative_o1(self) -> None:
        reference = self.config.document["campaign"]["schedules"][0]
        state_root = (self.config.state_root / "failure-preprocess-state").resolve()
        bundle_root = Path(self.config.document["preprocess"]["bundle_root"])
        receiver = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        receiver._preprocess_candidates_cache = []
        runtime = (
            reference,
            {"consumer": {"preprocess_state_root": str(state_root)}},
            {},
            [None, None, None],
            {},
        )

        def register(artifact, **_kwargs):
            receiver._register_preprocess_candidate(
                campaign_ordinal=artifact["campaign_ordinal"],
                bundle_path=Path(artifact["bundle_path"]),
                bundle_id=artifact["bundle_id"],
                state_root=Path(artifact["preprocess_state_root"]),
                item_count=artifact["item_count"],
            )

        def failure_outcome(ordinal):
            bundle_id = f"ppbatch_{ordinal:032x}"
            return StageOutcome(
                "preprocess",
                "progressed",
                True,
                {
                    "active_schedule_id": reference["schedule_id"],
                    "processed_items": 1,
                    "failed_queue_ordinal": ordinal + 1,
                },
                {
                    "backend_kind": "himr_sealed_archive_backend_v1",
                    "preprocess_failure_attempts": [],
                    "preprocess_bundles": [
                        {
                            "queue_ordinal": ordinal,
                            "bundle_path": str(
                                bundle_root / "bundles" / bundle_id
                            ),
                            "bundle_id": bundle_id,
                            "bundle_manifest_sha256": f"{ordinal:064x}",
                            "campaign_ordinal": ordinal,
                            "preprocess_state_root": str(state_root),
                            "item_count": 1,
                        }
                    ],
                    "preprocess_candidates_rescan": False,
                    "resolved_predecessor_ordinals": [ordinal],
                },
            )

        with (
            mock.patch.object(
                receiver, "_campaign_runtimes", return_value=[runtime]
            ),
            mock.patch.object(
                receiver, "_observe_preprocess_bundle", side_effect=register
            ),
            mock.patch.object(
                receiver,
                "_preprocess_bundle_candidates",
                side_effect=AssertionError("cached totals must not rescan"),
            ) as rescan,
        ):
            receiver.observe_peer_outcome(failure_outcome(1))
            self.assertEqual(1, receiver._preprocessed_items_cumulative())
            receiver.observe_peer_outcome(failure_outcome(2))
            for _ in range(10):
                self.assertEqual(2, receiver._preprocessed_items_cumulative())
        rescan.assert_not_called()
        self.assertIsNotNone(receiver._preprocess_candidates_cache)

    def test_preprocess_candidate_hot_path_appends_without_resorting_history(self) -> None:
        receiver = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )

        class NoSortList(list):
            def sort(self, *args, **kwargs):  # pragma: no cover - regression tripwire
                raise AssertionError("candidate hot path must not sort full history")

        receiver._preprocess_candidates_cache = NoSortList()
        state_root = (self.config.state_root / "candidate-index-state").resolve()
        bundle_root = Path(self.config.document["preprocess"]["bundle_root"])
        for ordinal in range(1, 513):
            bundle_id = f"ppbatch_{ordinal:032x}"
            receiver._register_preprocess_candidate(
                campaign_ordinal=ordinal,
                bundle_path=bundle_root / "bundles" / bundle_id,
                bundle_id=bundle_id,
                state_root=state_root,
                item_count=1,
            )
        self.assertEqual(
            list(range(1, 513)),
            [row[0] for row in receiver._preprocess_candidates_cache],
        )

        # Recovery may discover an earlier ordinal after a later candidate. It
        # uses one bisect insertion and preserves exact ordering without a global
        # re-sort.
        recovered = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        recovered._preprocess_candidates_cache = NoSortList()
        for ordinal in (1, 3, 2):
            bundle_id = f"ppbatch_{ordinal + 1000:032x}"
            recovered._register_preprocess_candidate(
                campaign_ordinal=ordinal,
                bundle_path=bundle_root / "bundles" / bundle_id,
                bundle_id=bundle_id,
                state_root=state_root,
                item_count=1,
            )
        self.assertEqual(
            [1, 2, 3],
            [row[0] for row in recovered._preprocess_candidates_cache],
        )

    def test_preprocess_returned_stale_projection_reloads_current_ready_telemetry(self) -> None:
        reference = self.config.document["campaign"]["schedules"][0]
        state_root = (self.config.state_root / "stale-preprocess-state").resolve()
        schedule = {"consumer": {"preprocess_state_root": str(state_root)}}
        initial_bundle = {
            "manifest": {"work_orders": [{"queue_ordinal": 1}]},
            "orders": [{"result_path": "one"}],
        }
        initial_states = [{"queue_state": "completed"}]
        initial_ready = {
            "ready_item_count": 1,
            "ready_byte_count": 10,
            "items": [
                {"ordinal": 1, "media_byte_count": 10, "job_id": "one"}
            ],
            "zone": "at_or_below_low_water",
        }
        selected = (
            reference,
            schedule,
            initial_bundle,
            initial_states,
            initial_ready,
        )
        refreshed_bundle = {
            "manifest": {
                "work_orders": [
                    {"queue_ordinal": 1},
                    {"queue_ordinal": 2},
                ]
            },
            "orders": [
                {"result_path": "one"},
                {"result_path": "two"},
            ],
        }
        refreshed_states = [
            {"queue_state": "completed"},
            {"queue_state": "completed"},
        ]
        refreshed_ready = {
            "ready_item_count": 1,
            "ready_byte_count": 20,
            "items": [
                {"ordinal": 2, "media_byte_count": 20, "job_id": "two"}
            ],
            "zone": "at_or_below_low_water",
        }
        handoff = SimpleNamespace(
            run_handoff=mock.Mock(
                return_value={
                    # This is the historical projection returned before the
                    # concurrently merged acquisition became visible.
                    "ready_after": {
                        "ready_item_count": 0,
                        "ready_byte_count": 0,
                        "items": [],
                        "zone": "at_or_below_low_water",
                    },
                    "processed_items": [{"queue_ordinal": 1}],
                    "stop_reason": "bounded",
                }
            )
        )
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(
                background=SimpleNamespace(
                    _completed_state=lambda value: isinstance(value, dict),
                    _quarantined_state=lambda _value: False,
                ),
                handoff=handoff,
            ),
        )  # type: ignore[arg-type]
        replay_session = SimpleNamespace(delta={"returned": "stale"})
        with (
            mock.patch.object(
                backend, "_campaign_runtimes", return_value=[selected]
            ),
            mock.patch.object(
                backend,
                "_operational_queue_replay",
                return_value=nullcontext(replay_session),
            ),
            mock.patch.object(
                backend,
                "_replay_delta_returned_projection_is_current",
                return_value=False,
            ),
            mock.patch.object(
                backend,
                "_schedule_runtime",
                return_value=(
                    schedule,
                    refreshed_bundle,
                    refreshed_states,
                    refreshed_ready,
                ),
            ) as reload_runtime,
            mock.patch.object(
                backend,
                "_register_completed_preprocess_rows",
                return_value=[],
            ) as register_rows,
        ):
            outcome = backend._run_preprocess()
        handoff.run_handoff.assert_called_once()
        reload_runtime.assert_called_once_with(reference)
        self.assertEqual(1, outcome.monitor["processed_items"])
        self.assertEqual(1, outcome.monitor["ready_items_after"])
        self.assertEqual(1, outcome.monitor["raw_ready_items"])
        self.assertEqual(
            (schedule, refreshed_bundle, refreshed_states, refreshed_ready),
            backend._runtime_cache[reference["schedule_id"]],
        )
        registered_runtimes = register_rows.call_args.kwargs["all_runtimes"]
        self.assertEqual(refreshed_states, registered_runtimes[0][3])
        self.assertEqual(refreshed_ready, registered_runtimes[0][4])

    def test_preprocess_final_scan_discovery_reloads_mixed_generation_runtime(self) -> None:
        reference = self.config.document["campaign"]["schedules"][0]
        state_root = (self.config.state_root / "mixed-preprocess-state").resolve()
        schedule = {"consumer": {"preprocess_state_root": str(state_root)}}
        bundle = {
            "manifest": {
                "work_orders": [
                    {"queue_ordinal": 1},
                    {"queue_ordinal": 2},
                ]
            },
            "orders": [
                {"result_path": "one"},
                {"result_path": "two"},
            ],
        }
        initial_states = [{"queue_state": "completed"}, None]
        initial_ready = {
            "ready_item_count": 1,
            "ready_byte_count": 10,
            "items": [{"ordinal": 1, "media_byte_count": 10}],
            "zone": "at_or_below_low_water",
        }
        selected = (
            reference,
            schedule,
            bundle,
            initial_states,
            initial_ready,
        )
        # The acquisition lane completes ordinal 2 while ordinal 1 is being
        # preprocessed. The handoff's final scan legitimately returns ordinal 2,
        # even though its replay projection is current at commit time.
        handoff_ready = {
            "ready_item_count": 1,
            "ready_byte_count": 20,
            "items": [{"ordinal": 2, "media_byte_count": 20}],
            "zone": "at_or_below_low_water",
        }
        refreshed_states = [
            {"queue_state": "completed"},
            {"queue_state": "completed"},
        ]
        singleton_id = "ppbatch_" + "9" * 32
        handoff = SimpleNamespace(
            run_handoff=mock.Mock(
                return_value={
                    "ready_after": handoff_ready,
                    "processed_items": [
                        {
                            "queue_ordinal": 1,
                            "preprocess_receipt_count": 1,
                            "bundle": {
                                "path": str(
                                    Path(
                                        self.config.document["preprocess"][
                                            "bundle_root"
                                        ]
                                    )
                                    / "bundles"
                                    / singleton_id
                                ),
                                "bundle_id": singleton_id,
                                "manifest_sha256": "9" * 64,
                            },
                        }
                    ],
                    "stop_reason": "limit_reached",
                }
            )
        )
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(
                background=SimpleNamespace(
                    _completed_state=lambda value: isinstance(value, dict),
                    _quarantined_state=lambda _value: False,
                ),
                handoff=handoff,
            ),
        )  # type: ignore[arg-type]
        replay_session = SimpleNamespace(delta={"projection": "current"})
        with (
            mock.patch.object(
                backend, "_campaign_runtimes", return_value=[selected]
            ),
            mock.patch.object(
                backend,
                "_operational_queue_replay",
                return_value=nullcontext(replay_session),
            ),
            mock.patch.object(
                backend,
                "_replay_delta_returned_projection_is_current",
                return_value=True,
            ),
            mock.patch.object(
                backend,
                "_schedule_runtime",
                return_value=(
                    schedule,
                    bundle,
                    refreshed_states,
                    handoff_ready,
                ),
            ) as reload_runtime,
        ):
            outcome = backend._run_preprocess()

        handoff.run_handoff.assert_called_once()
        reload_runtime.assert_called_once_with(reference)
        self.assertEqual(1, outcome.monitor["processed_items"])
        self.assertEqual(
            (schedule, bundle, refreshed_states, handoff_ready),
            backend._runtime_cache[reference["schedule_id"]],
        )
        self.assertEqual(1, len(outcome.artifacts["preprocess_bundles"]))
        artifact = outcome.artifacts["preprocess_bundles"][0]
        self.assertEqual(1, artifact["queue_ordinal"])
        self.assertEqual(singleton_id, artifact["bundle_id"])
        self.assertEqual(1, artifact["campaign_ordinal"])
        self.assertEqual(1, backend._preprocessed_items_cumulative())

        # The following cycle may now process the ordinal discovered by the
        # first handoff's final scan. Its singleton row must validate against the
        # refreshed cached states rather than the pre-call generation.
        next_singleton_id = "ppbatch_" + "a" * 32
        next_artifacts = backend._register_completed_preprocess_rows(
            [
                {
                    "queue_ordinal": 2,
                    "preprocess_receipt_count": 1,
                    "bundle": {
                        "path": str(
                            Path(
                                self.config.document["preprocess"]["bundle_root"]
                            )
                            / "bundles"
                            / next_singleton_id
                        ),
                        "bundle_id": next_singleton_id,
                        "manifest_sha256": "a" * 64,
                    },
                }
            ],
            reference=reference,
            schedule=schedule,
            all_runtimes=[
                (reference, *backend._runtime_cache[reference["schedule_id"]])
            ],
        )
        self.assertEqual(2, next_artifacts[0]["queue_ordinal"])
        self.assertEqual(2, next_artifacts[0]["campaign_ordinal"])
        self.assertEqual(2, backend._preprocessed_items_cumulative())

    def test_acquisition_peer_delta_updates_cache_without_deep_runtime_replay(self) -> None:
        calls = []

        class DeltaBackground:
            def _runtime_from_queue_summary(self, schedule, summary, **kwargs):
                calls.append((schedule, summary, kwargs))
                return {"replayed": True}, [{"queue_state": "completed"}], {
                    "ready_item_count": 1,
                    "ready_byte_count": 123,
                    "items": [{"ordinal": 1, "media_byte_count": 123}],
                    "zone": "at_or_below_low_water",
                }

            def load_schedule(self, _path):  # pragma: no cover
                raise AssertionError("cached schedule should avoid schedule reload")

        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(background=DeltaBackground()),  # type: ignore[arg-type]
        )
        first, second = self.config.document["campaign"]["schedules"]
        sealed_schedule = {"schedule_id": first["schedule_id"]}
        backend._runtime_cache = {
            first["schedule_id"]: (sealed_schedule, {}, [None], {}),
            second["schedule_id"]: ({"schedule_id": second["schedule_id"]}, {}, [None], {}),
        }
        queue = {"summary_sha256": "1" * 64}
        backend.observe_peer_outcome(
            StageOutcome(
                "acquisition",
                "progressed",
                True,
                {"active_schedule_id": first["schedule_id"]},
                {
                    "backend_kind": "himr_sealed_archive_backend_v1",
                    TRANSIENT_PEER_RUNTIME_ARTIFACT: {
                        "kind": "acquisition_queue_summary_v1",
                        "schedule_id": first["schedule_id"],
                        "queue_summary": queue,
                    },
                },
            )
        )
        self.assertEqual(1, len(calls))
        self.assertIs(sealed_schedule, calls[0][0])
        self.assertEqual({"bounded", "completed", "parked"}, calls[0][2]["expected_statuses"])
        self.assertEqual({"replayed": True}, backend._runtime_cache[first["schedule_id"]][1])
        self.assertIn(second["schedule_id"], backend._runtime_cache)

    def test_normal_ready_backpressure_does_not_block_cold_only_acquisition(self) -> None:
        background = FakeBackground()
        modules = SimpleNamespace(
            background=background, queue_runner=fake_queue_runner()
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        normal_reference = {
            **self.config.document["campaign"]["schedules"][0],
            "role": "normal_processing",
        }
        cold_reference = {
            **self.config.document["campaign"]["schedules"][1],
            "role": "cold_acquisition_only_requires_chunking",
        }
        normal = (
            normal_reference,
            {},
            {},
            [None],
            {
                "ready_item_count": 16,
                "ready_byte_count": 32 * 1024**3,
                "items": [
                    {"ordinal": value, "media_byte_count": 2 * 1024**3}
                    for value in range(1, 17)
                ],
                "zone": "at_or_below_low_water",
            },
        )
        cold = (
            cold_reference,
            {},
            {},
            [None],
            {
                "ready_item_count": 0,
                "ready_byte_count": 0,
                "items": [],
                "zone": "at_or_below_low_water",
            },
        )
        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[normal, cold]),
            mock.patch.object(backend, "_validate_cold_storage_identity"),
        ):
            outcome = backend._run_acquisition()
        self.assertEqual(Path(cold_reference["path"]), background.call[0])
        self.assertEqual(
            self.config.document["acquisition"][
                "cold_acquisition_only_requires_chunking"
            ],
            background.call[1],
        )
        self.assertEqual(
            "cold_acquisition_only_requires_chunking",
            outcome.monitor["active_schedule_role"],
        )

    def test_retention_result_must_preserve_no_delete_no_catalogue_policy(self) -> None:
        row = {
            "item_key": "bgacqsched_" + "a" * 32 + ":1",
            "schedule_id": "bgacqsched_" + "a" * 32,
            "ordinal": 1,
            "job_id": "job",
            "work_order_sha256": "1" * 64,
            "result_sha256": "2" * 64,
            "media_sha256": "3" * 64,
            "media_byte_count": 4,
        }
        result = {
            "status": "completed",
            "cold_transfer_receipt": {
                "status": "completed",
                "transfer_id": "coldtx_" + "4" * 32,
                "receipt_id": "coldreceipt_" + "5" * 32,
                "identity_sha256": "6" * 64,
                "destination": {"relative_path": "media/sha256/33/" + "3" * 64 + "/payload"},
            },
            "policy": {
                "source_deleted": False,
                "source_mutated": False,
                "catalogue_mutated": False,
                "publication_authority": "none",
                "deletion_authority": "none",
            },
        }
        record = SealedArchiveBackend._retention_record(row, result)
        self.assertEqual(row["item_key"], record["item_key"])
        forged = dict(result)
        forged["policy"] = {**result["policy"], "source_deleted": True}
        with self.assertRaisesRegex(BackendError, "unsafe"):
            SealedArchiveBackend._retention_record(row, forged)

    def test_disabled_cold_retention_publishes_exact_drained_source_monitor(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        backend.config.document["cold_retention"]["enabled"] = False

        outcome = backend._run_cold_retention()
        source_monitor = {
            "status": outcome.status,
            "progressed": outcome.progressed,
            **outcome.monitor,
        }

        self.assertEqual(
            {
                "status": "skipped",
                "progressed": False,
                "reason": "disabled_by_sealed_config",
                "retained_items": 0,
                "replay_pending_items": 0,
                "pending_items": 0,
            },
            source_monitor,
        )

    def test_known_campaign_inventory_coverage_is_derived_from_sealed_rows(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        inventory = repository / (
            "research/corpus/acquisition-planning/archive-all-known-2026-08-29/"
            "campaign-inventory.json"
        )
        coverage = SealedArchiveBackend._inventory_coverage(
            json.loads(inventory.read_bytes())
        )
        self.assertEqual(7, coverage["collection_count"])
        self.assertEqual(3_923, coverage["candidate_count"])
        self.assertEqual(3_199, coverage["ready_selected_count"])
        self.assertEqual(724, coverage["parked_requires_chunking_count"])
        self.assertEqual(
            coverage["candidate_count"],
            coverage["ready_selected_count"]
            + coverage["parked_requires_chunking_count"],
        )

        plan = repository / (
            "research/corpus/acquisition-planning/archive-all-known-2026-08-29/"
            "queue-plan.json"
        )
        expected = SealedArchiveBackend._expected_archive_role_identities(
            json.loads(plan.read_bytes()), coverage
        )
        self.assertEqual(3_199, len(expected["normal_processing"]))
        self.assertEqual(
            724,
            len(expected["cold_acquisition_only_requires_chunking"]),
        )

    def test_addendum_inventory_scope_is_supported_without_weakening_legacy_scope(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        inventory_path = repository / (
            "research/corpus/acquisition-planning/archive-all-known-2026-08-29/"
            "campaign-inventory.json"
        )
        inventory = json.loads(inventory_path.read_bytes())
        inventory["scope"] = (
            "public_archive_org_collection_addendum_in_sealed_2026_08_30_inventory"
        )
        coverage = SealedArchiveBackend._inventory_coverage(inventory)
        self.assertEqual(3_923, coverage["candidate_count"])
        inventory["scope"] = (
            "all_known_public_archive_org_items_in_sealed_2026_08_30_inventory"
        )
        coverage = SealedArchiveBackend._inventory_coverage(inventory)
        self.assertEqual(3_923, coverage["candidate_count"])
        inventory["scope"] = "unreviewed_archive_scope"
        with self.assertRaisesRegex(BackendError, "header or safety"):
            SealedArchiveBackend._inventory_coverage(inventory)

    def test_composite_schedule_set_is_deep_replayed_as_one_exact_union(self) -> None:
        from acquisition.tests.test_materialize_composite_campaign_schedule_set import (
            CompositeCampaignScheduleSetTests,
        )

        fixture = CompositeCampaignScheduleSetTests(
            "test_reference_only_exact_flatten_and_union_proofs"
        )
        fixture.setUp()
        try:
            manifest, manifest_path = fixture.materialize()
            core = config_core(self.config.state_root.parent)
            core["campaign"]["schedule_set"] = {
                "kind": manifest["composite_kind"],
                "path": str(manifest_path),
                "sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                "schedule_set_id": manifest["composite_schedule_set_id"],
            }
            core["campaign"]["schedules"] = [
                {
                    "path": row["schedule_path"],
                    "sha256": row["schedule_sha256"],
                    "schedule_id": row["schedule_id"],
                    "role": row["role"],
                }
                for row in manifest["schedules"]
            ]
            campaign_core = {
                key: value
                for key, value in core["campaign"].items()
                if key != "campaign_id"
            }
            core["campaign"]["campaign_id"] = (
                "himrarccampaign_"
                + sha256_bytes(canonical_bytes(campaign_core))[:32]
            )
            document = build_config(core)
            config = ControllerConfig(
                document=document,
                path=self.config.path,
                physical_sha256=sha256_bytes(canonical_bytes(document)),
            )
            backend = SealedArchiveBackend(config)
            result = backend._validate_schedule_set_manifest(
                backend._campaign_runtimes(),
                {
                    "ready_selected_count": 4,
                    "parked_requires_chunking_count": 4,
                },
            )
            self.assertEqual(manifest["composite_schedule_set_id"], result["schedule_set_id"])
            self.assertEqual(4, result["schedule_count"])
            self.assertEqual(4, result["normal_selected_count"])
            self.assertEqual(4, result["cold_only_selected_count"])
            self.assertEqual(8, result["selected_count"])
        finally:
            fixture.tearDown()

    def test_selection_only_archive_projection_excludes_outside_selection_rows(self) -> None:
        coverage = {"collections": [{"identifier": "699993"}]}
        plan = {
            "limits": {"selection_only": True},
            "selection_basis": {
                "source_ids": ["src_new_long", "src_new_ready"],
                "youtube_video_ids": [],
                "recording_ids": [],
            },
            "candidates": [
                {
                    "platform": "internet_archive",
                    "source_kind": "archive_media_file",
                    "source_id": "src_old_outside_selection",
                    "recording_id": "rec_old",
                    "native_id": "69999/old.mp4",
                    "priority_tier": "medium_archive",
                    "queue_state": "ready",
                    "queue_ordinal": None,
                    "defer_reason": "outside_explicit_selection",
                },
                {
                    "platform": "internet_archive",
                    "source_kind": "archive_media_file",
                    "source_id": "src_new_long",
                    "recording_id": "rec_new_long",
                    "native_id": "699993/new-long.mp4",
                    "priority_tier": "explicit_selection",
                    "queue_state": "requires_chunking",
                    "queue_ordinal": None,
                    "defer_reason": "requires_chunking",
                },
                {
                    "platform": "internet_archive",
                    "source_kind": "archive_media_file",
                    "source_id": "src_new_ready",
                    "recording_id": "rec_new_ready",
                    "native_id": "699993/new-ready.mp4",
                    "priority_tier": "explicit_selection",
                    "queue_state": "ready",
                    "queue_ordinal": 1,
                    "defer_reason": None,
                },
            ],
        }
        expected = SealedArchiveBackend._expected_archive_role_identities(
            plan, coverage
        )
        self.assertEqual(
            {("src_new_ready", "rec_new_ready")},
            expected["normal_processing"],
        )
        self.assertEqual(
            {("src_new_long", "rec_new_long")},
            expected["cold_acquisition_only_requires_chunking"],
        )

    def test_selection_only_archive_projection_rejects_out_of_scope_source(self) -> None:
        coverage = {"collections": [{"identifier": "699993"}]}
        plan = {
            "limits": {"selection_only": True},
            "selection_basis": {
                "source_ids": ["src_old"],
                "youtube_video_ids": [],
                "recording_ids": [],
            },
            "candidates": [
                {
                    "platform": "internet_archive",
                    "source_kind": "archive_media_file",
                    "source_id": "src_old",
                    "recording_id": "rec_old",
                    "native_id": "69999/old.mp4",
                    "priority_tier": "explicit_selection",
                    "queue_state": "ready",
                    "queue_ordinal": 1,
                    "defer_reason": None,
                }
            ],
        }
        with self.assertRaisesRegex(BackendError, "outside inventory collections"):
            SealedArchiveBackend._expected_archive_role_identities(plan, coverage)

    def test_selection_only_archive_projection_rejects_malformed_selection(self) -> None:
        coverage = {"collections": [{"identifier": "699993"}]}
        plan = {
            "limits": {"selection_only": True},
            "selection_basis": {
                "source_ids": ["src_new"],
                "youtube_video_ids": [],
                "recording_ids": [],
            },
            "candidates": [
                {
                    "platform": "internet_archive",
                    "source_kind": "archive_media_file",
                    "source_id": "src_new",
                    "recording_id": "rec_new",
                    "native_id": "699993/new.mp4",
                    "priority_tier": "explicit_selection",
                    "queue_state": "ready",
                    "queue_ordinal": 1,
                    "defer_reason": None,
                }
            ],
        }
        malformed_selections = (
            {
                "source_ids": ["src_new", "src_new"],
                "youtube_video_ids": [],
                "recording_ids": [],
            },
            {
                "source_ids": ["src_new"],
                "youtube_video_ids": ["youtube-id-is-not-an-archive-source"],
                "recording_ids": [],
            },
        )
        for selection in malformed_selections:
            with self.subTest(selection=selection):
                candidate_plan = {**plan, "selection_basis": selection}
                with self.assertRaisesRegex(BackendError, "exact source IDs only"):
                    SealedArchiveBackend._expected_archive_role_identities(
                        candidate_plan, coverage
                    )

    def test_preprocessed_cumulative_reanchors_from_multi_item_receipts(self) -> None:
        reference = self.config.document["campaign"]["schedules"][0]
        state_root = (self.config.state_root / "preprocess-recovery").resolve()
        bundle_id = "ppbatch_" + "7" * 32
        receipt_paths = [
            state_root / bundle_id / "receipts" / f"receipt-{ordinal}.json"
            for ordinal in (1, 2)
        ]
        result_paths = ["/sealed/result-1.json", "/sealed/result-2.json"]
        acquisition_bundle = {
            "manifest": {
                "work_orders": [
                    {"queue_ordinal": ordinal} for ordinal in (1, 2)
                ]
            },
            "orders": [
                {"result_path": path} for path in result_paths
            ],
        }
        states = [
            {"queue_state": "completed"},
            {"queue_state": "completed"},
        ]
        ready = {
            "ready_item_count": 0,
            "ready_byte_count": 0,
            "items": [],
            "zone": "at_or_below_low_water",
        }
        schedule = {
            "consumer": {"preprocess_state_root": str(state_root)}
        }
        manifest = {"work_order_count": 2}
        selection = {
            "entries": [
                {"acquisition_result": {"path": path}}
                for path in result_paths
            ]
        }
        modules = SimpleNamespace(
            background=SimpleNamespace(
                _completed_state=lambda value: value is not None,
                _quarantined_state=lambda _value: False,
                _preprocess_receipt_paths=lambda root: (
                    receipt_paths if Path(root) == state_root else []
                ),
            ),
            queue_runner=SimpleNamespace(
                _result_path=lambda order: Path(order["result_path"])
            ),
            preprocess_batch=SimpleNamespace(
                validate_bundle=lambda _path: (manifest, selection, [{}, {}]),
                existing_receipts=lambda *_args, **_kwargs: [{}, {}],
            ),
        )
        backend = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        runtime = (reference, schedule, acquisition_bundle, states, ready)
        with mock.patch.object(
            backend, "_campaign_runtimes", return_value=[runtime]
        ):
            candidates = backend._preprocess_bundle_candidates()
            outcome = backend._run_preprocess()
        self.assertEqual(1, len(candidates))
        self.assertEqual(2, backend._preprocessed_items_cumulative())
        self.assertEqual(2, outcome.monitor["preprocessed_items_cumulative"])
        self.assertEqual(0, outcome.monitor["processed_items"])

    def test_gpu_item_totals_preserve_packed_legacy_and_no_ready_counts(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        backend._gpu_batch_item_counts = {
            "packed:32": 32,
            "packed:25": 25,
            "legacy:singleton": 1,
            "legacy:no-ready": 0,
        }
        backend._gpu_records = {
            "packed:32": {"record_kind": "ready_batch"},
            "packed:25": {"record_kind": "ready_batch"},
            "legacy:singleton": {"record_kind": "ready_batch"},
            "legacy:no-ready": {"record_kind": "no_ready_members"},
        }
        backend._gpu_status = {
            "packed:32": "pending",
            "packed:25": "pending",
            "legacy:singleton": "completed",
            "legacy:no-ready": "not_applicable",
        }
        self.assertEqual(
            {"pending_items": 57, "completed_items": 1, "parked_items": 0},
            backend._gpu_item_status_totals(),
        )

        backend._gpu_status["packed:32"] = "completed"
        completed = backend._gpu_item_status_totals()
        self.assertEqual(33, completed["completed_items"])
        self.assertEqual(25, completed["pending_items"])

        # A fresh lane restored from validated record/count ledgers retains the
        # cumulative completion; completing the remaining pack is monotonic.
        restored = backend.fork_lane("gpu_readiness")
        self.assertEqual(completed, restored._gpu_item_status_totals())
        restored._gpu_status["packed:25"] = "completed"
        final = restored._gpu_item_status_totals()
        self.assertEqual(58, final["completed_items"])
        self.assertGreaterEqual(
            final["completed_items"], completed["completed_items"]
        )

        legacy_record = {
            "record_kind": "ready_batch",
            "batch_key": "legacy:peer-singleton",
            "preprocess_bundle_id": "ppbatch_" + "8" * 32,
            "queue": {
                "path": "/private/queue.json",
                "sha256": "9" * 64,
                "queue_id": "gpuasrqueue1_" + "a" * 32,
            },
            "batch": {
                "path": "/private/batch.json",
                "sha256": "b" * 64,
                "batch_id": "gpuasrbatch2_" + "c" * 32,
            },
            "materialization_receipt": {
                "path": "/private/receipt.json",
                "sha256": "d" * 64,
                "receipt_id": "gpubatchreceipt1_" + "e" * 32,
            },
            "queue_disposition": {
                "member_count": 1,
                "ready_count": 1,
                "requires_chunking_count": 0,
                "explicit_skip_count": 0,
                "ready_audio_duration_ms": 1,
                "requires_chunking_audio_duration_ms": 0,
            },
        }
        receiver = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )

        def validate_legacy(record):
            receiver._gpu_batch_item_counts[record["batch_key"]] = 1
            return "pending"

        with mock.patch.object(
            receiver, "_validate_legacy_gpu_record", side_effect=validate_legacy
        ):
            receiver._observe_gpu_record(legacy_record, "pending")
        self.assertEqual(1, receiver._gpu_item_status_totals()["pending_items"])

    def test_gpu_item_totals_reject_omitted_or_zero_ready_ledgers(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        backend._gpu_records = {
            "ready": {"record_kind": "ready_batch"},
            "empty": {"record_kind": "no_ready_members"},
        }
        backend._gpu_batch_item_counts = {"ready": 1, "empty": 0}
        backend._gpu_status = {"ready": "pending", "empty": "not_applicable"}
        self.assertEqual(
            {"pending_items": 1, "completed_items": 0, "parked_items": 0},
            backend._gpu_item_status_totals(),
        )

        del backend._gpu_batch_item_counts["ready"]
        with self.assertRaisesRegex(BackendError, "ledgers differ"):
            backend._gpu_item_status_totals()
        backend._gpu_batch_item_counts["ready"] = 0
        with self.assertRaisesRegex(BackendError, "ledger is malformed"):
            backend._gpu_item_status_totals()
        backend._gpu_batch_item_counts["ready"] = 1
        backend._gpu_batch_item_counts["empty"] = 1
        with self.assertRaisesRegex(BackendError, "ledger is malformed"):
            backend._gpu_item_status_totals()
        backend._gpu_batch_item_counts["empty"] = 0
        del backend._gpu_status["empty"]
        with self.assertRaisesRegex(BackendError, "ledgers differ"):
            backend._gpu_item_status_totals()

    def test_legacy_missing_gpu_status_derives_no_ready_from_exact_replay(self) -> None:
        manifest = {
            "queue_id": "gpuasrqueue1_" + "a" * 32,
            "members": [],
            "totals": {
                "member_count": 0,
                "ready_count": 0,
                "requires_chunking_count": 0,
                "explicit_skip_count": 0,
                "ready_audio_duration_ms": 0,
                "requires_chunking_audio_duration_ms": 0,
            },
        }
        body = json.dumps(
            manifest, sort_keys=True, separators=(",", ":")
        ).encode()
        record = {
            "record_kind": "no_ready_members",
            "batch_key": manifest["queue_id"] + ":no-ready-members",
            "preprocess_bundle_id": "ppbatch_" + "b" * 32,
            "queue": {
                "path": "/private/legacy-queue.json",
                "sha256": hashlib.sha256(body).hexdigest(),
                "queue_id": manifest["queue_id"],
            },
            "batch": None,
            "materialization_receipt": None,
            "queue_disposition": {
                "member_count": 0,
                "ready_count": 0,
                "requires_chunking_count": 0,
                "explicit_skip_count": 0,
                "ready_audio_duration_ms": 0,
                "requires_chunking_audio_duration_ms": 0,
            },
        }
        modules = SimpleNamespace(
            gpu_queue=SimpleNamespace(
                validate_queue=lambda **_kwargs: manifest,
                canonical_bytes=lambda _value: body,
            )
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]

        backend._restore_gpu_record(record, None)

        self.assertEqual("not_applicable", backend._gpu_status[record["batch_key"]])
        self.assertEqual(0, backend._gpu_batch_item_counts[record["batch_key"]])
        self.assertEqual(
            {"pending_items": 0, "completed_items": 0, "parked_items": 0},
            backend._gpu_item_status_totals(),
        )

    def test_rejected_peer_gpu_event_rolls_back_every_record_and_ledger(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        backend._gpu_records = {
            "existing": {"record_kind": "no_ready_members"}
        }
        backend._gpu_status = {"existing": "not_applicable"}
        backend._gpu_queue_dispositions = {
            "existing-queue": {"sentinel": 1}
        }
        backend._gpu_parked = {"existing": {"item_count": 0}}
        backend._gpu_batch_item_counts = {"existing": 0}
        backend._gpu_member_claims = {("existing-queue", 1): "existing"}
        ledger_names = (
            "_gpu_records",
            "_gpu_status",
            "_gpu_queue_dispositions",
            "_gpu_parked",
            "_gpu_batch_item_counts",
            "_gpu_member_claims",
        )
        before = {name: dict(getattr(backend, name)) for name in ledger_names}

        def record(key: str) -> dict:
            return {
                "record_kind": "ready_batch",
                "batch_key": key,
                "queue": {"queue_id": "queue-" + key},
                "queue_disposition": {"member_count": 1},
            }

        first = record("first")
        second = record("second")

        def validate(value: dict) -> str:
            key = value["batch_key"]
            queue_id = value["queue"]["queue_id"]
            backend._gpu_batch_item_counts[key] = 1
            backend._gpu_member_claims[(queue_id, 1)] = key
            return "pending"

        outcome = StageOutcome(
            "gpu_readiness",
            "progressed",
            True,
            {},
            {
                "backend_kind": BACKEND_KIND,
                "gpu_records": [first, second],
                "gpu_record_statuses": {
                    "first": "pending",
                    "second": "completed",
                },
            },
        )
        with (
            mock.patch.object(backend, "_validate_gpu_record", side_effect=validate),
            self.assertRaisesRegex(BackendError, "exact batch replay"),
        ):
            backend.observe_peer_outcome(outcome)

        for name in ledger_names:
            self.assertEqual(before[name], getattr(backend, name), name)

    def test_gpu_readiness_and_quiesce_publish_exact_item_totals(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        backend._gpu_records = {
            "packed:32": {"record_kind": "ready_batch"},
            "legacy:one": {"record_kind": "ready_batch"},
        }
        backend._gpu_batch_item_counts = {"packed:32": 32, "legacy:one": 1}
        backend._gpu_status = {"packed:32": "pending", "legacy:one": "completed"}
        profile = {
            "batch_limits": {
                "ready_batch_high_water": 1,
                "maximum_items": 32,
                "maximum_total_audio_ms": 1_000_000,
                "preferred_total_audio_ms": 900_000,
            }
        }
        with (
            mock.patch.object(backend, "_refresh_gpu_status", return_value=1),
            mock.patch.object(backend, "_load_profile", return_value=profile),
            mock.patch.object(
                backend, "_supervise_gpu_child", return_value=(False, None, 0)
            ),
        ):
            high_water = backend._run_gpu_readiness()
        self.assertEqual(32, high_water.monitor["pending_items"])
        self.assertEqual(1, high_water.monitor["completed_items"])

        normal_profile = {
            "batch_limits": {**profile["batch_limits"], "ready_batch_high_water": 2}
        }
        with (
            mock.patch.object(backend, "_refresh_gpu_status", return_value=1),
            mock.patch.object(backend, "_load_profile", return_value=normal_profile),
            mock.patch.object(
                backend, "_supervise_gpu_child", return_value=(False, None, 0)
            ),
            mock.patch.object(backend, "_preprocess_bundle_candidates", return_value=[]),
        ):
            normal = backend._run_gpu_readiness()
        self.assertEqual(32, normal.monitor["pending_items"])
        self.assertEqual(1, normal.monitor["completed_items"])

        backend._gpu_buffered_ready_items = 3
        with (
            mock.patch.object(
                backend, "_supervise_gpu_child", return_value=(False, None, 0)
            ),
            mock.patch.object(backend, "_refresh_gpu_status", return_value=1),
        ):
            quiesced = backend.quiesce()
        self.assertEqual(32, quiesced.monitor["pending_items"])
        self.assertEqual(1, quiesced.monitor["completed_items"])
        self.assertEqual(3, quiesced.monitor["buffered_ready_items"])

        self.config.document["gpu_readiness"]["enabled"] = False
        disabled = backend._run_gpu_readiness()
        disabled_quiesce = backend.quiesce()
        for outcome in (disabled, disabled_quiesce):
            self.assertEqual(0, outcome.monitor["pending_items"])
            self.assertEqual(0, outcome.monitor["completed_items"])
            self.assertEqual(0, outcome.monitor["buffered_ready_items"])

    def test_pending_gpu_refresh_polls_only_the_admitted_batch_boundary(self) -> None:
        record = gpu_ready_record("packed:admitted", "a")
        batch_manifest = {
            "batch_id": record["batch"]["batch_id"],
            "totals": {"item_count": 32},
        }
        disposition = {
            "status": "pending",
            "batch_id": record["batch"]["batch_id"],
            "completed_ordinals": [],
            "absent_ordinals": list(range(1, 33)),
            "invalid": [],
            "inference_performed": False,
            "files_written": False,
        }
        load_calls = []
        status_calls = []

        def load_manifest(path, digest, *, profile):
            load_calls.append((path, digest, profile))
            return batch_manifest, b"sealed-batch"

        def batch_status(manifest, profile):
            status_calls.append((manifest, profile))
            return dict(disposition)

        modules = SimpleNamespace(
            gpu_bridge=SimpleNamespace(
                BATCH_V2=SimpleNamespace(
                    load_manifest=load_manifest,
                    batch_status=batch_status,
                )
            )
        )
        backend = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 32}

        with (
            mock.patch.object(backend, "_load_profile", return_value={"sealed": True}),
            mock.patch.object(
                backend,
                "_validate_gpu_record",
                side_effect=RecursionError(
                    "immutable source replay must not run during status polling"
                ),
            ),
        ):
            self.assertEqual(1, backend._refresh_gpu_status())
            self.assertEqual(1, backend._refresh_gpu_status())
            disposition.update(
                status="completed",
                completed_ordinals=list(range(1, 33)),
                absent_ordinals=[],
            )
            self.assertEqual(0, backend._refresh_gpu_status())
            # Terminal authority is stable and does not need another filesystem
            # poll until a fresh process performs full journal recovery.
            self.assertEqual(0, backend._refresh_gpu_status())

        self.assertEqual("completed", backend._gpu_status[record["batch_key"]])
        self.assertEqual(3, len(load_calls))
        self.assertEqual(3, len(status_calls))
        self.assertEqual(
            {"pending_items": 0, "completed_items": 32, "parked_items": 0},
            backend._gpu_item_status_totals(),
        )

    def test_pending_gpu_refresh_parks_invalid_and_fails_closed_on_drift(self) -> None:
        record = gpu_ready_record("legacy:admitted", "b")
        batch_manifest = {
            "batch_id": record["batch"]["batch_id"],
            "totals": {"item_count": 3},
        }
        disposition = {
            "status": "invalid",
            "batch_id": record["batch"]["batch_id"],
            "completed_ordinals": [1],
            "absent_ordinals": [3],
            "invalid": [
                {
                    "ordinal": 2,
                    "error_type": "FixtureError",
                    "message": "invalid exact result",
                }
            ],
            "inference_performed": False,
            "files_written": False,
        }
        modules = SimpleNamespace(
            gpu_bridge=SimpleNamespace(
                BATCH_V2=SimpleNamespace(
                    load_manifest=lambda _path, _digest, profile: (
                        batch_manifest,
                        b"sealed-batch",
                    ),
                    batch_status=lambda _manifest, _profile: disposition,
                )
            )
        )
        backend = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 3}
        with mock.patch.object(backend, "_load_profile", return_value={}):
            self.assertEqual(0, backend._refresh_gpu_status())
        self.assertEqual("parked", backend._gpu_status[record["batch_key"]])
        self.assertEqual(
            {"pending_items": 0, "completed_items": 0, "parked_items": 3},
            backend._gpu_item_status_totals(),
        )

        missing = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        missing._gpu_records = {record["batch_key"]: record}
        missing._gpu_batch_item_counts = {record["batch_key"]: 3}
        with self.assertRaisesRegex(BackendError, "lacks exact status authority"):
            missing._refresh_gpu_status()

        drifted = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        drifted._gpu_records = {record["batch_key"]: record}
        drifted._gpu_status = {record["batch_key"]: "pending"}
        drifted._gpu_batch_item_counts = {record["batch_key"]: 4}
        with (
            mock.patch.object(drifted, "_load_profile", return_value={}),
            self.assertRaisesRegex(BackendError, "differs from admission authority"),
        ):
            drifted._refresh_gpu_status()

        contradictory = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        contradictory._gpu_records = {record["batch_key"]: record}
        contradictory._gpu_status = {record["batch_key"]: "pending"}
        contradictory._gpu_batch_item_counts = {record["batch_key"]: 3}
        contradictory_status = dict(disposition)
        contradictory_status["status"] = "completed"
        with (
            mock.patch.object(contradictory, "_load_profile", return_value={}),
            mock.patch.object(
                modules.gpu_bridge.BATCH_V2,
                "batch_status",
                return_value=contradictory_status,
            ),
            self.assertRaisesRegex(BackendError, "contradictory disposition"),
        ):
            contradictory._refresh_gpu_status()

        load_failed = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        load_failed._gpu_records = {record["batch_key"]: record}
        load_failed._gpu_status = {record["batch_key"]: "pending"}
        load_failed._gpu_batch_item_counts = {record["batch_key"]: 3}
        with (
            mock.patch.object(load_failed, "_load_profile", return_value={}),
            mock.patch.object(
                modules.gpu_bridge.BATCH_V2,
                "load_manifest",
                side_effect=RecursionError("batch loader recursion tripwire"),
            ),
            self.assertRaisesRegex(BackendError, "batch status replay failed"),
        ):
            load_failed._refresh_gpu_status()

        mismatched_key = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        mismatched_key._gpu_records = {"outer-key": record}
        mismatched_key._gpu_status = {"outer-key": "pending"}
        mismatched_key._gpu_batch_item_counts = {"outer-key": 3}
        with self.assertRaisesRegex(BackendError, "not an admitted ready batch"):
            mismatched_key._refresh_gpu_status()

    def test_pending_exact_batch_launches_once_through_closed_gpu_child_spec(self) -> None:
        executor = FakeChildExecutor()
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
            gpu_executor=executor,
        )
        self._bind_gpu_profile(backend)
        key = "queue:1"
        backend._gpu_records[key] = {
            "record_kind": "ready_batch",
            "batch_key": key,
            "preprocess_bundle_id": "ppbundle_" + "5" * 32,
            "queue": {
                "path": "/private/queue.json",
                "sha256": "6" * 64,
                "queue_id": "gpuasrqueue1_" + "7" * 32,
            },
            "batch": {
                "path": "/private/batch.json",
                "sha256": "8" * 64,
                "batch_id": "gpuasrbatch2_" + "9" * 32,
            },
            "materialization_receipt": {
                "path": "/private/receipt.json",
                "sha256": "a" * 64,
                "receipt_id": "gpubatchreceipt1_" + "b" * 32,
            },
            "queue_disposition": {
                "member_count": 1,
                "ready_count": 1,
                "requires_chunking_count": 0,
                "explicit_skip_count": 0,
                "ready_audio_duration_ms": 1,
                "requires_chunking_audio_duration_ms": 0,
            },
        }
        backend._gpu_status[key] = "pending"
        with mock.patch.object(backend, "_refresh_gpu_status", return_value=1):
            changed, current, active = backend._supervise_gpu_child()
        self.assertTrue(changed)
        self.assertEqual(1, active)
        self.assertEqual("running", current["state"])
        self.assertEqual(1, len(executor.launched))
        spec = executor.launched[0]
        gpu = self.config.document["gpu_readiness"]
        self.assertEqual(Path(gpu["launcher_profile"]), spec.launcher_profile)
        self.assertEqual(Path(gpu["local_readiness"]), spec.local_readiness)
        self.assertEqual(Path(gpu["local_launcher"]), spec.local_launcher)
        self.assertEqual(1, spec.attempt_ordinal)

    def test_gpu_opportunity_contention_is_held_before_child_attempt(self) -> None:
        executor = FakeChildExecutor()
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
            gpu_executor=executor,
        )
        self._bind_gpu_profile(backend)
        record = gpu_ready_record("queue:opportunity-held", "a")
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 1}

        with (
            self._hold_gpu_opportunity(backend),
            mock.patch.object(backend, "_refresh_gpu_status", return_value=1),
        ):
            changed, current, active = backend._supervise_gpu_child()

        self.assertFalse(changed)
        self.assertIsNone(current)
        self.assertEqual(0, active)
        self.assertEqual([], executor.launched)
        self.assertEqual({}, backend._gpu_child_records)
        self.assertIsNone(backend._gpu_opportunity_lease)

    def test_active_gpu_child_reacquires_and_retains_opportunity_until_quiesced(
        self,
    ) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        self._bind_gpu_profile(backend)
        record = gpu_ready_record("queue:active-opportunity", "b")
        running = gpu_child_record(backend, record, 1, "running").validated()

        class ActiveExecutor(FakeChildExecutor):
            def __init__(self) -> None:
                super().__init__((running,))
                self.reconciled = []

            def reconcile(self, spec):
                self.reconciled.append(spec)
                return running

            def stop(self, spec):
                self.stopped.append(spec)
                stopped = replace(
                    running,
                    state="stopped",
                    updated_at="2026-08-29T00:00:02Z",
                    completed_at="2026-08-29T00:00:02Z",
                    stop_requested_at="2026-08-29T00:00:02Z",
                ).validated()
                self.journal = FakeChildJournal((stopped,))
                return stopped

        executor = ActiveExecutor()
        backend._gpu_executor = executor
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 1}

        changed, current, active = backend._supervise_gpu_child()
        self.assertFalse(changed)
        self.assertEqual("running", current["state"])
        self.assertEqual(1, active)
        self.assertEqual(1, len(executor.reconciled))
        self.assertIsNotNone(backend._gpu_opportunity_lease)

        path = backend._gpu_opportunity_path()
        contender = os.open(
            path,
            os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

            stopped, stopped_current, stopped_active = backend._supervise_gpu_child(
                stop=True
            )
            self.assertTrue(stopped)
            self.assertIsNone(stopped_current)
            self.assertEqual(0, stopped_active)
            self.assertIsNone(backend._gpu_opportunity_lease)
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            fcntl.flock(contender, fcntl.LOCK_UN)
            os.close(contender)

    def test_active_gpu_child_opportunity_contention_fails_closed(self) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        self._bind_gpu_profile(backend)
        record = gpu_ready_record("queue:active-opportunity-held", "c")
        running = gpu_child_record(backend, record, 1, "running").validated()
        executor = FakeChildExecutor((running,))
        backend._gpu_executor = executor
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 1}

        with (
            self._hold_gpu_opportunity(backend),
            self.assertRaisesRegex(
                BackendError,
                "active GPU child could not reacquire its retained opportunity lease",
            ),
        ):
            backend._supervise_gpu_child()

        self.assertEqual([], executor.launched)
        self.assertIsNone(backend._gpu_opportunity_lease)

    def test_gpu_opportunity_lease_is_confined_to_gpu_fork(self) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
            gpu_executor=FakeChildExecutor(),
        )
        self._bind_gpu_profile(backend)
        gpu_lane = backend.fork_lane("gpu_readiness")
        preprocess_lane = backend.fork_lane("preprocess")

        self.assertTrue(gpu_lane._acquire_gpu_opportunity())
        self.assertIsNotNone(gpu_lane._gpu_opportunity_lease)
        self.assertIsNone(backend._gpu_opportunity_lease)
        self.assertIsNone(preprocess_lane._gpu_opportunity_lease)
        with self.assertRaisesRegex(
            BackendError, "while a GPU opportunity lease is retained"
        ):
            gpu_lane.fork_lane("gpu_readiness")
        gpu_lane._release_gpu_opportunity()
        self.assertIsNone(gpu_lane._gpu_opportunity_lease)

    def test_launch_exception_refreshes_and_quiesces_accepted_timeout(self) -> None:
        class MutableJournal:
            def __init__(self) -> None:
                self.records: list[GpuChildRecord] = []
                self.reads = 0

            def list_records(self):
                self.reads += 1
                return tuple(self.records)

        record = gpu_ready_record("queue:ambiguous", "a")
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        self._bind_gpu_profile(backend)

        class AmbiguousLaunchExecutor(FakeChildExecutor):
            def __init__(self) -> None:
                super().__init__()
                self.journal = MutableJournal()
                self.stop_calls = 0

            def launch(self, spec):
                persisted = gpu_child_record(
                    backend, record, spec.attempt_ordinal, "reconciliation_required"
                )
                persisted = replace(
                    persisted,
                    launch_accepted_at=None,
                    error="systemd-run outcome is ambiguous",
                ).validated()
                self.journal.records.append(persisted)
                raise RuntimeError("systemd launch acknowledgement was ambiguous")

            def stop(self, spec):
                self.stop_calls += 1
                persisted = self.journal.records[0]
                self.assert_exact_binding(persisted, spec)
                stopped = replace(
                    persisted,
                    child_invocation_id="3" * 32,
                    launch_accepted_at="2026-08-29T00:00:02Z",
                    stop_requested_at="2026-08-29T00:00:02Z",
                    updated_at="2026-08-29T00:00:02Z",
                ).validated()
                self.journal.records[0] = stopped
                return stopped

            @staticmethod
            def assert_exact_binding(persisted, spec):
                if (
                    persisted.batch_id != spec.batch_id
                    or persisted.batch_sha256 != spec.expected_batch_sha256
                    or persisted.attempt_ordinal != spec.attempt_ordinal
                ):
                    raise AssertionError("quiesce targeted a different exact child")

        executor = AmbiguousLaunchExecutor()
        backend._gpu_executor = executor
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 32}

        with (
            mock.patch.object(backend, "_refresh_gpu_status", return_value=1),
            self.assertRaisesRegex(BackendError, "exact GPU child launch failed"),
        ):
            backend._supervise_gpu_child()

        self.assertGreaterEqual(executor.journal.reads, 2)
        self.assertEqual(
            "reconciliation_required",
            next(iter(backend._gpu_child_records.values())).state,
        )
        with mock.patch.object(backend, "_refresh_gpu_status", return_value=1):
            quiesced = backend.quiesce()
        self.assertGreaterEqual(executor.journal.reads, 3)
        self.assertEqual(1, executor.stop_calls)
        self.assertEqual(0, quiesced.monitor["active_children"])
        self.assertIsNone(quiesced.monitor["current_gpu_child"])
        persisted = next(iter(backend._gpu_child_records.values()))
        self.assertEqual("reconciliation_required", persisted.state)
        self.assertEqual("3" * 32, persisted.child_invocation_id)
        self.assertIsNotNone(persisted.stop_requested_at)

    def test_real_executor_accepted_timeout_is_exactly_quiesced(self) -> None:
        root = self.config.state_root.parent
        gpu = self.config.document["gpu_readiness"]
        for field in (
            "root_registration",
            "runtime_admission",
            "production_profile",
            "launcher_profile",
            "local_readiness",
        ):
            path = Path(gpu[field])
            body = json.dumps(
                {"fixture": field}, sort_keys=True, separators=(",", ":")
            ).encode() + b"\n"
            path.write_bytes(body)
            path.chmod(0o400)
            gpu[f"{field}_sha256"] = hashlib.sha256(body).hexdigest()
        launcher = Path(gpu["local_launcher"])
        launcher.write_text("#!/usr/bin/python3 -IB\n", encoding="utf-8")
        launcher.chmod(0o500)
        for field in (
            "child_journal_root",
            "result_root",
            "event_root",
            "lock_root",
        ):
            path = Path(gpu[field])
            path.mkdir(parents=True, mode=0o700, exist_ok=True)
            path.chmod(0o700)

        batch_path = root / "accepted-timeout-batch.json"
        batch_body = b'{"fixture":"accepted-timeout"}\n'
        batch_path.write_bytes(batch_body)
        batch_path.chmod(0o400)
        record = gpu_ready_record("queue:accepted-timeout", "a")
        record["batch"]["path"] = str(batch_path)
        record["batch"]["sha256"] = hashlib.sha256(batch_body).hexdigest()

        fake: FakeSystemd

        def accepted_timeout(unit: str) -> None:
            fake.units[unit] = {
                "mode": "hold",
                "invocation": CHILD_INVOCATION,
            }
            raise subprocess.TimeoutExpired(["/usr/bin/systemd-run"], 1)

        fake = FakeSystemd(on_run=accepted_timeout)

        class PendingProbe:
            @staticmethod
            def inspect(spec) -> BatchResultStatus:
                return BatchResultStatus("pending", spec.batch_id, (), (1,), ())

        executor = SystemdGpuChildExecutor(
            ControllerUnitContext(OUTER_UNIT, OUTER_INVOCATION),
            PrivateGpuChildJournal(Path(gpu["child_journal_root"])),
            result_probe=PendingProbe(),
            runner=fake,
            sleep=lambda _seconds: None,
            now=lambda: "2026-08-29T22:00:00Z",
        )
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
            gpu_executor=executor,
        )
        self._bind_gpu_profile(backend)
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 1}

        with (
            mock.patch.object(backend, "_refresh_gpu_status", return_value=1),
            self.assertRaisesRegex(BackendError, "exact GPU child launch failed"),
        ):
            backend._supervise_gpu_child()
        ambiguous = executor.journal.list_records()[0]
        self.assertEqual("reconciliation_required", ambiguous.state)
        self.assertIsNone(ambiguous.launch_accepted_at)
        self.assertEqual(0, len(fake.stop_calls))

        with mock.patch.object(backend, "_refresh_gpu_status", return_value=1):
            quiesced = backend.quiesce()

        self.assertEqual(1, len(fake.stop_calls))
        self.assertEqual(0, quiesced.monitor["active_children"])
        self.assertIsNone(quiesced.monitor["current_gpu_child"])
        stopped = executor.journal.list_records()[0]
        self.assertEqual("reconciliation_required", stopped.state)
        self.assertEqual(CHILD_INVOCATION, stopped.child_invocation_id)
        self.assertIsNotNone(stopped.launch_accepted_at)
        self.assertIsNotNone(stopped.stop_requested_at)

    def test_prior_invocation_active_pack_blocks_32_25_parking_and_launch(self) -> None:
        packed_32 = gpu_ready_record("packed:32", "a")
        packed_25 = gpu_ready_record("packed:25", "b")
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        old_outer = "himr-operator-job-" + "6" * 32 + ".service"
        old_invocation = "5" * 32
        records = [
            gpu_child_record(
                backend,
                packed_32,
                ordinal,
                "running" if ordinal == 3 else "failed",
                outer_unit=old_outer,
                outer_invocation=old_invocation,
            )
            for ordinal in range(1, 4)
        ]
        executor = FakeChildExecutor(records)
        backend._gpu_executor = executor
        backend._gpu_records = {
            packed_32["batch_key"]: packed_32,
            packed_25["batch_key"]: packed_25,
        }
        backend._gpu_status = {
            packed_32["batch_key"]: "pending",
            packed_25["batch_key"]: "pending",
        }
        backend._gpu_batch_item_counts = {
            packed_32["batch_key"]: 32,
            packed_25["batch_key"]: 25,
        }

        with self.assertRaisesRegex(
            BackendError, "prior controller invocation requires manual reconciliation"
        ):
            backend._restore_gpu_attempt_exhaustion()
        with self.assertRaisesRegex(
            BackendError, "prior controller invocation requires manual reconciliation"
        ):
            backend._supervise_gpu_child()
        with self.assertRaisesRegex(
            BackendError, "prior controller invocation requires manual reconciliation"
        ):
            backend._supervise_gpu_child(stop=True)

        self.assertEqual([], executor.launched)
        self.assertEqual([], executor.stopped)
        self.assertEqual({}, backend._gpu_parked)
        self.assertEqual(
            {"pending_items": 57, "completed_items": 0, "parked_items": 0},
            backend._gpu_item_status_totals(),
        )

    def test_non_final_active_gpu_attempt_cannot_be_hidden_by_later_history(self) -> None:
        record = gpu_ready_record("packed:32", "c")
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        executor = FakeChildExecutor(
            [
                gpu_child_record(backend, record, 1, "running"),
                gpu_child_record(backend, record, 2, "failed"),
            ]
        )
        backend._gpu_executor = executor
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 32}

        with self.assertRaisesRegex(BackendError, "non-final active child authority"):
            backend._restore_gpu_attempt_exhaustion()
        self.assertEqual("pending", backend._gpu_status[record["batch_key"]])
        self.assertEqual({}, backend._gpu_parked)

    def test_empty_gpu_restore_is_offline_and_rejects_an_orphan_child(self) -> None:
        child_root = Path(
            self.config.section("gpu_readiness")["child_journal_root"]
        )
        child_root.mkdir(mode=0o700)
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        with mock.patch.object(
            backend,
            "_get_gpu_executor",
            side_effect=AssertionError("offline restore requested systemd context"),
        ):
            backend._restore_gpu_attempt_exhaustion()
        self.assertEqual({}, backend._gpu_child_records)
        self.assertIsNone(backend._gpu_executor)

        orphan_backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        orphan_batch = gpu_ready_record("orphan", "d")
        journal = PrivateGpuChildJournal(child_root)
        journal.save(gpu_child_record(orphan_backend, orphan_batch, 1, "failed"))
        with mock.patch.object(
            orphan_backend,
            "_get_gpu_executor",
            side_effect=AssertionError("offline restore requested systemd context"),
        ):
            with self.assertRaisesRegex(
                BackendError, "records without GPU recovery authority"
            ):
                orphan_backend._restore_gpu_attempt_exhaustion()
        self.assertIsNone(orphan_backend._gpu_child_records)
        self.assertIsNone(orphan_backend._gpu_executor)

    def test_any_gpu_recovery_record_preserves_executor_restore_path(self) -> None:
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        executor = FakeChildExecutor()
        backend._gpu_executor = executor
        backend._gpu_records = {
            "queue:no-ready": {"record_kind": "no_ready_members"}
        }
        backend._restore_gpu_attempt_exhaustion()
        self.assertIs(executor, backend._gpu_executor)
        self.assertEqual({}, backend._gpu_child_records)

    def test_terminal_gpu_history_recovers_offline_with_exact_batch_binding(self) -> None:
        backend = SealedArchiveBackend(self.config, modules=SimpleNamespace())
        record = gpu_ready_record("terminal", "d")
        journal = self._gpu_child_journal()
        journal.save(gpu_child_record(backend, record, 1, "failed"))
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "completed"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 1}
        with mock.patch.object(
            backend, "_get_gpu_executor",
            side_effect=AssertionError("terminal recovery requested launch context"),
        ):
            backend._restore_gpu_attempt_exhaustion()
        self.assertEqual(len(backend._gpu_child_records), 1)
        self.assertIsNone(backend._gpu_executor)

        backend._gpu_child_records = None
        backend._gpu_records = {
            "different": {"record_kind": "no_ready_members"}
        }
        with self.assertRaisesRegex(BackendError, "outside controller recovery"):
            backend._restore_gpu_attempt_exhaustion()

    def test_active_gpu_history_still_requires_live_controller_context(self) -> None:
        backend = SealedArchiveBackend(self.config, modules=SimpleNamespace())
        record = gpu_ready_record("active", "e")
        journal = self._gpu_child_journal()
        journal.save(gpu_child_record(backend, record, 1, "running"))
        backend._gpu_records = {record["batch_key"]: record}
        backend._gpu_status = {record["batch_key"]: "pending"}
        backend._gpu_batch_item_counts = {record["batch_key"]: 1}
        with mock.patch.object(
            backend, "_get_gpu_executor",
            side_effect=BackendError("live context required"),
        ) as executor:
            with self.assertRaisesRegex(BackendError, "live context required"):
                backend._restore_gpu_attempt_exhaustion()
        executor.assert_called_once_with()

    def test_exhausted_gpu_batch_parks_and_next_exact_batch_launches(self) -> None:
        first_sha = "8" * 64
        first_id = "gpuasrbatch2_" + "9" * 32
        attempts = []
        for ordinal in range(1, 4):
            attempts.append(
                GpuChildRecord(
                    unit_name=(
                        "himr-autonomy-gpu-"
                        + "2" * 32
                        + "-"
                        + first_sha
                        + f"-{ordinal:06d}.service"
                    ),
                    outer_unit="himr-operator-job-" + "1" * 32 + ".service",
                    outer_invocation_id="2" * 32,
                    child_invocation_id="3" * 32,
                    batch_id=first_id,
                    batch_sha256=first_sha,
                    spec_identity_sha256="4" * 64,
                    attempt_ordinal=ordinal,
                    state="failed",
                    created_at="2026-08-29T00:00:00Z",
                    updated_at="2026-08-29T00:00:01Z",
                    launch_accepted_at="2026-08-29T00:00:00Z",
                    started_at="2026-08-29T00:00:00Z",
                    completed_at="2026-08-29T00:00:01Z",
                    stop_requested_at=None,
                    returncode=2,
                    systemd_result="exit-code",
                    result={
                        "status": "pending",
                        "batch_id": first_id,
                        "completed_ordinals": [],
                        "absent_ordinals": [1],
                        "invalid": [],
                    },
                    error="worker failed",
                )
            )
        executor = FakeChildExecutor(attempts)
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
            gpu_executor=executor,
        )
        self._bind_gpu_profile(backend)

        def ready(key: str, batch_id: str, digest: str) -> dict:
            return {
                "record_kind": "ready_batch",
                "batch_key": key,
                "preprocess_bundle_id": "ppbundle_" + "5" * 32,
                "queue": {
                    "path": "/private/queue.json",
                    "sha256": "6" * 64,
                    "queue_id": "gpuasrqueue1_" + "7" * 32,
                },
                "batch": {
                    "path": "/private/batch.json",
                    "sha256": digest,
                    "batch_id": batch_id,
                },
                "materialization_receipt": {
                    "path": "/private/receipt.json",
                    "sha256": "a" * 64,
                    "receipt_id": "gpubatchreceipt1_" + "b" * 32,
                },
                "queue_disposition": {
                    "member_count": 1,
                    "ready_count": 1,
                    "requires_chunking_count": 0,
                    "explicit_skip_count": 0,
                    "ready_audio_duration_ms": 1,
                    "requires_chunking_audio_duration_ms": 0,
                },
            }

        second_id = "gpuasrbatch2_" + "c" * 32
        second_sha = "d" * 64
        backend._gpu_records = {
            "queue:1": ready("queue:1", first_id, first_sha),
            "queue:2": ready("queue:2", second_id, second_sha),
        }
        backend._gpu_status = {"queue:1": "pending", "queue:2": "pending"}
        backend._gpu_batch_item_counts = {"queue:1": 1, "queue:2": 1}
        with mock.patch.object(backend, "_refresh_gpu_status", return_value=2):
            changed, current, active = backend._supervise_gpu_child()
        self.assertTrue(changed)
        self.assertEqual("parked", backend._gpu_status["queue:1"])
        self.assertEqual(1, backend._gpu_parked_totals()["parked_item_count"])
        self.assertEqual(1, active)
        self.assertEqual(second_id, current["batch_id"])
        self.assertEqual(second_id, executor.launched[0].batch_id)

    def test_restore_reconstructs_only_true_gpu_attempt_exhaustion(self) -> None:
        def ready_record(suffix: str) -> dict:
            batch_id = "gpuasrbatch2_" + suffix * 32
            return {
                "record_kind": "ready_batch",
                "batch_key": "queue:" + suffix,
                "batch": {
                    "path": f"/private/batch-{suffix}.json",
                    "sha256": suffix * 64,
                    "batch_id": batch_id,
                },
            }

        exhausted = ready_record("a")
        active = ready_record("b")
        backend = SealedArchiveBackend(
            self.config,
            modules=SimpleNamespace(),  # type: ignore[arg-type]
        )
        backend._gpu_records = {
            exhausted["batch_key"]: exhausted,
            active["batch_key"]: active,
        }
        backend._gpu_status = {
            exhausted["batch_key"]: "pending",
            active["batch_key"]: "pending",
        }
        backend._gpu_batch_item_counts = {
            exhausted["batch_key"]: 2,
            active["batch_key"]: 1,
        }

        def child(record: dict, ordinal: int, state: str) -> GpuChildRecord:
            spec = backend._gpu_launch_spec(record, ordinal)
            outer_invocation = "2" * 32
            terminal = state == "failed"
            return GpuChildRecord(
                unit_name=(
                    f"himr-autonomy-gpu-{outer_invocation}-"
                    f"{record['batch']['sha256']}-{ordinal:06d}.service"
                ),
                outer_unit="himr-operator-job-" + "1" * 32 + ".service",
                outer_invocation_id=outer_invocation,
                child_invocation_id="3" * 32,
                batch_id=record["batch"]["batch_id"],
                batch_sha256=record["batch"]["sha256"],
                spec_identity_sha256=spec.identity_sha256,
                attempt_ordinal=ordinal,
                state=state,
                created_at="2026-08-29T00:00:00Z",
                updated_at="2026-08-29T00:00:01Z",
                launch_accepted_at="2026-08-29T00:00:00Z",
                started_at="2026-08-29T00:00:00Z",
                completed_at="2026-08-29T00:00:01Z" if terminal else None,
                stop_requested_at=None,
                returncode=2 if terminal else None,
                systemd_result="exit-code" if terminal else None,
                result=(
                    {
                        "status": "pending",
                        "batch_id": record["batch"]["batch_id"],
                        "completed_ordinals": [],
                        "absent_ordinals": [1],
                        "invalid": [],
                    }
                    if terminal
                    else None
                ),
                error="worker failed" if terminal else None,
            )

        records = [
            *(child(exhausted, ordinal, "failed") for ordinal in range(1, 4)),
            child(active, 1, "failed"),
            child(active, 2, "failed"),
            child(active, 3, "running"),
        ]
        backend._gpu_executor = FakeChildExecutor(records)
        backend._restore_gpu_attempt_exhaustion()

        self.assertEqual("parked", backend._gpu_status[exhausted["batch_key"]])
        self.assertEqual("pending", backend._gpu_status[active["batch_key"]])
        self.assertEqual(
            {"pending_items": 1, "completed_items": 0, "parked_items": 2},
            backend._gpu_item_status_totals(),
        )
        self.assertEqual(
            {
                "parked_batch_count": 1,
                "parked_item_count": 2,
            },
            backend._gpu_parked_totals(),
        )

    def test_restore_validates_each_gpu_map_and_rejects_quiesce_records(self) -> None:
        record = gpu_ready_record("queue:durable", "d")

        def event(
            event_type: str,
            records: list[dict],
            statuses: dict,
            *,
            stage: str = "gpu_readiness",
        ) -> dict:
            return {
                "event_type": event_type,
                "payload": {
                    "outcome": {
                        "stage": stage,
                        "artifacts": {
                            "backend_kind": BACKEND_KIND,
                            "gpu_records": records,
                            "gpu_record_statuses": statuses,
                        },
                    }
                },
            }

        valid = event(
            "stage_finished",
            [record],
            {record["batch_key"]: "pending"},
        )
        legacy = event("stage_finished", [record], {})
        del legacy["payload"]["outcome"]["artifacts"]["gpu_record_statuses"]
        legacy_evidence = SealedArchiveBackend._gpu_event_evidence(
            "stage_finished",
            legacy["payload"]["outcome"],
            legacy["payload"]["outcome"]["artifacts"],
        )
        self.assertEqual([(record, None)], legacy_evidence)
        peer_missing_status = StageOutcome(
            "gpu_readiness",
            "progressed",
            True,
            {},
            {
                "backend_kind": BACKEND_KIND,
                "gpu_records": [record],
            },
        )
        peer = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(BackendError, "record/status ledgers differ"):
            peer.observe_peer_outcome(peer_missing_status)
        outside_gpu_stage = StageOutcome(
            "preprocess",
            "progressed",
            True,
            {},
            {
                "backend_kind": BACKEND_KIND,
                "gpu_records": [record],
                "gpu_record_statuses": {record["batch_key"]: "pending"},
            },
        )
        with self.assertRaisesRegex(BackendError, "outside GPU readiness"):
            peer.observe_peer_outcome(outside_gpu_stage)
        missing_status = event("stage_finished", [record], {})
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(BackendError, "record/status ledgers differ"):
            backend.restore([legacy, valid, missing_status])

        invalid_status = event(
            "stage_finished",
            [record],
            {record["batch_key"]: "unknown"},
        )
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(BackendError, "record/status ledgers differ"):
            backend.restore([invalid_status])

        quiesce_with_record = event(
            "gpu_child_quiesced",
            [record],
            {record["batch_key"]: "pending"},
        )
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(BackendError, "quiesce outcome invents"):
            backend.restore([quiesce_with_record])

    def test_preprocess_operational_failures_park_after_three_exact_attempts(self) -> None:
        class BatchError(Exception):
            pass

        reference = self.config.document["campaign"]["schedules"][0]
        state = {
            "queue_state": "completed",
            "result_sha256": "a" * 64,
            "media_sha256": "b" * 64,
            "byte_count": 123,
        }
        bundle = {
            "path": Path("/sealed/bundle/manifest.json"),
            "manifest": {
                "work_orders": [
                    {
                        "queue_ordinal": 1,
                        "job_id": "job-1",
                        "path": "orders/000001.json",
                        "sha256": "c" * 64,
                    }
                ]
            },
            "orders": [{"result": "/cold/result.json"}],
        }
        ready = {
            "ready_item_count": 1,
            "ready_byte_count": 123,
            "items": [{"ordinal": 1, "media_byte_count": 123}],
        }
        runtime = (reference, {"consumer": {}}, bundle, [state], ready)
        background = SimpleNamespace(
            _completed_state=lambda value: value is state,
            _quarantined_state=lambda _value: False,
        )
        handoff = SimpleNamespace(
            run_handoff=mock.Mock(
                side_effect=BatchError(
                    "media preprocessing failed for item 1: deterministic ffmpeg error"
                )
            )
        )
        modules = SimpleNamespace(
            background=background,
            handoff=handoff,
            preprocess_batch=SimpleNamespace(BatchError=BatchError),
            queue_runner=SimpleNamespace(
                _result_path=lambda order: Path(order["result"])
            ),
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        completed = {
            "item_key": f"{reference['schedule_id']}:1",
            "schedule_id": reference["schedule_id"],
            "ordinal": 1,
            "job_id": "job-1",
            "work_order": Path("/sealed/bundle/orders/000001.json"),
            "work_order_sha256": "c" * 64,
            "result": Path("/cold/result.json"),
            "result_sha256": "a" * 64,
            "media_sha256": "b" * 64,
            "media_byte_count": 123,
        }
        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[runtime]),
            mock.patch.object(
                backend,
                "_schedule_runtime",
                return_value=(runtime[1], runtime[2], runtime[3], runtime[4]),
            ),
            mock.patch.object(
                backend, "_completed_acquisitions", return_value=[completed]
            ),
        ):
            outcomes = [backend._run_preprocess() for _ in range(3)]
            exhausted = backend._run_preprocess()
        self.assertEqual(
            ["retryable", "retryable", "parked"],
            [
                outcome.artifacts["preprocess_failure_attempts"][0]["disposition"]
                for outcome in outcomes
            ],
        )
        self.assertEqual(3, handoff.run_handoff.call_count)
        self.assertEqual(1, exhausted.monitor["parked_items"])
        self.assertEqual("complete", exhausted.status)

    def test_mid_batch_preprocess_failure_forces_exact_gpu_candidate_rescan(self) -> None:
        class BatchError(Exception):
            pass

        self.config.document["preprocess"]["max_items"] = 2
        reference = self.config.document["campaign"]["schedules"][0]
        states = [
            {
                "queue_state": "completed",
                "result_sha256": str(ordinal) * 64,
                "media_sha256": chr(96 + ordinal) * 64,
                "byte_count": 100,
            }
            for ordinal in (1, 2)
        ]
        bundle = {
            "path": Path("/sealed/bundle/manifest.json"),
            "manifest": {
                "work_orders": [
                    {
                        "queue_ordinal": ordinal,
                        "job_id": f"job-{ordinal}",
                        "path": f"orders/{ordinal:06d}.json",
                        "sha256": chr(98 + ordinal) * 64,
                    }
                    for ordinal in (1, 2)
                ]
            },
            "orders": [
                {"result": f"/cold/result-{ordinal}.json"}
                for ordinal in (1, 2)
            ],
        }
        ready_before = {
            "ready_item_count": 2,
            "ready_byte_count": 200,
            "items": [
                {"ordinal": ordinal, "media_byte_count": 100}
                for ordinal in (1, 2)
            ],
        }
        ready_after = {
            "ready_item_count": 1,
            "ready_byte_count": 100,
            "items": [{"ordinal": 2, "media_byte_count": 100}],
        }
        runtime_before = (reference, {"consumer": {}}, bundle, states, ready_before)
        runtime_after = (runtime_before[1], bundle, states, ready_after)
        modules = SimpleNamespace(
            background=SimpleNamespace(
                _completed_state=lambda value: isinstance(value, dict),
                _quarantined_state=lambda _value: False,
            ),
            handoff=SimpleNamespace(
                run_handoff=mock.Mock(
                    side_effect=BatchError(
                        "media preprocessing failed for item 1: deterministic ffmpeg error"
                    )
                )
            ),
            preprocess_batch=SimpleNamespace(BatchError=BatchError),
            queue_runner=SimpleNamespace(
                _result_path=lambda order: Path(order["result"])
            ),
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        backend._preprocess_candidates_cache = [(99, Path("/old"), "old", Path("/state"))]
        completed = [
            {
                "item_key": f"{reference['schedule_id']}:{ordinal}",
                "schedule_id": reference["schedule_id"],
                "ordinal": ordinal,
                "job_id": f"job-{ordinal}",
                "work_order": Path(f"/sealed/bundle/orders/{ordinal:06d}.json"),
                "work_order_sha256": chr(98 + ordinal) * 64,
                "result": Path(f"/cold/result-{ordinal}.json"),
                "result_sha256": str(ordinal) * 64,
                "media_sha256": chr(96 + ordinal) * 64,
                "media_byte_count": 100,
            }
            for ordinal in (1, 2)
        ]
        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[runtime_before]),
            mock.patch.object(backend, "_schedule_runtime", return_value=runtime_after),
            mock.patch.object(backend, "_completed_acquisitions", return_value=completed),
        ):
            outcome = backend._run_preprocess()
        self.assertEqual(1, outcome.monitor["processed_items"])
        self.assertTrue(outcome.artifacts["preprocess_candidates_rescan"])
        self.assertEqual([1], outcome.artifacts["resolved_predecessor_ordinals"])
        self.assertIsNone(backend._preprocess_candidates_cache)

        receiver = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        receiver._preprocess_candidates_cache = [(99, Path("/old"), "old", Path("/state"))]
        predecessor_key = f"{reference['schedule_id']}:1"
        receiver._preprocess_failure_attempts[predecessor_key] = [
            {"disposition": "retryable"}
        ]
        receiver.observe_peer_outcome(outcome)
        self.assertIsNone(receiver._preprocess_candidates_cache)
        self.assertIn(predecessor_key, receiver._preprocess_resolved_after_retry)
        self.assertEqual(
            1,
            receiver._preprocess_failure_totals()["retryable_failed_item_count"],
        )

    def test_mid_batch_failure_uses_actual_lower_ordinal_prefix(self) -> None:
        class BatchError(Exception):
            pass

        self.config.document["preprocess"]["max_items"] = 2
        ControlStore(self.config).set_desired_state("running")
        reference = self.config.document["campaign"]["schedules"][0]
        schedule = {
            "consumer": {
                "preprocess_state_root": str(self.config.state_root / "pre-state")
            }
        }
        states = [
            {
                "queue_state": "completed",
                "result_sha256": str(ordinal) * 64,
                "media_sha256": chr(96 + ordinal) * 64,
                "byte_count": 100,
            }
            for ordinal in (1, 2, 3)
        ]
        bundle = {
            "path": Path("/sealed/bundle/manifest.json"),
            "manifest": {
                "work_orders": [
                    {
                        "queue_ordinal": ordinal,
                        "job_id": f"job-{ordinal}",
                        "path": f"orders/{ordinal:06d}.json",
                        "sha256": chr(98 + ordinal) * 64,
                    }
                    for ordinal in (1, 2, 3)
                ]
            },
            "orders": [
                {"result": f"/cold/result-{ordinal}.json"}
                for ordinal in (1, 2, 3)
            ],
        }
        # Ordinal 1 was still retryable when this lane took its snapshot. The
        # acquisition lane completes it before the handoff's source replay, so
        # the actual bounded selection becomes [1, 2] instead of stale [2, 3].
        ready_before = {
            "ready_item_count": 2,
            "ready_byte_count": 200,
            "items": [
                {"ordinal": 2, "media_byte_count": 100},
                {"ordinal": 3, "media_byte_count": 100},
            ],
        }
        ready_after = {
            "ready_item_count": 2,
            "ready_byte_count": 200,
            "items": [
                {"ordinal": 2, "media_byte_count": 100},
                {"ordinal": 3, "media_byte_count": 100},
            ],
        }
        runtime_before = (reference, schedule, bundle, states, ready_before)
        attempted = []

        def run_one(row, **_kwargs):
            ordinal = row["queue_ordinal"]
            attempted.append(ordinal)
            if ordinal == 2:
                raise BatchError(
                    "media preprocessing failed for item 1: deterministic ffmpeg error"
                )
            bundle_id = f"ppbatch_{ordinal:032x}"
            return {
                "queue_ordinal": ordinal,
                "preprocess_receipt_count": 1,
                "bundle": {
                    "path": str(
                        Path(self.config.document["preprocess"]["bundle_root"])
                        / "bundles"
                        / bundle_id
                    ),
                    "bundle_id": bundle_id,
                    "manifest_sha256": f"{ordinal:064x}",
                },
            }

        handoff = SimpleNamespace(_materialize_and_run_one=run_one)

        def run_handoff(_path, **kwargs):
            self.assertEqual(2, kwargs["limit"])
            actual = [{"queue_ordinal": 1}, {"queue_ordinal": 2}]
            return {
                "processed_items": [
                    handoff._materialize_and_run_one(row) for row in actual
                ]
            }

        handoff.run_handoff = run_handoff
        modules = SimpleNamespace(
            background=SimpleNamespace(
                _completed_state=lambda value: isinstance(value, dict),
                _quarantined_state=lambda _value: False,
            ),
            handoff=handoff,
            preprocess_batch=SimpleNamespace(BatchError=BatchError),
            queue_runner=SimpleNamespace(
                _result_path=lambda order: Path(order["result"])
            ),
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        backend._preprocess_candidates_cache = []
        completed = [
            {
                "item_key": f"{reference['schedule_id']}:{ordinal}",
                "schedule_id": reference["schedule_id"],
                "ordinal": ordinal,
                "job_id": f"job-{ordinal}",
                "work_order": Path(f"/sealed/bundle/orders/{ordinal:06d}.json"),
                "work_order_sha256": chr(98 + ordinal) * 64,
                "result": Path(f"/cold/result-{ordinal}.json"),
                "result_sha256": str(ordinal) * 64,
                "media_sha256": chr(96 + ordinal) * 64,
                "media_byte_count": 100,
            }
            for ordinal in (1, 2, 3)
        ]
        with (
            mock.patch.object(
                backend, "_campaign_runtimes", return_value=[runtime_before]
            ),
            mock.patch.object(
                backend,
                "_schedule_runtime",
                return_value=(schedule, bundle, states, ready_after),
            ) as reconcile,
            mock.patch.object(
                backend, "_completed_acquisitions", return_value=completed
            ),
        ):
            outcome = backend._run_preprocess()

        self.assertEqual([1, 2], attempted)
        reconcile.assert_called_once_with(reference)
        self.assertEqual(2, outcome.monitor["failed_queue_ordinal"])
        self.assertEqual(1, outcome.monitor["processed_items"])
        failure = outcome.artifacts["preprocess_failure_attempts"][0]
        self.assertEqual(2, failure["queue_ordinal"])
        self.assertEqual(
            f"{reference['schedule_id']}:2", failure["item_key"]
        )
        self.assertEqual(
            [1], outcome.artifacts["resolved_predecessor_ordinals"]
        )
        self.assertFalse(outcome.artifacts["preprocess_candidates_rescan"])
        self.assertEqual(
            [1],
            [
                row["queue_ordinal"]
                for row in outcome.artifacts["preprocess_bundles"]
            ],
        )
        self.assertIsNotNone(backend._preprocess_candidates_cache)
        self.assertEqual(1, backend._preprocessed_items_cumulative())
        self.assertIn(
            f"{reference['schedule_id']}:1",
            backend._preprocess_resolved_after_retry,
        )
        self.assertNotIn(
            f"{reference['schedule_id']}:2",
            backend._preprocess_resolved_after_retry,
        )

    def test_preprocess_stop_finishes_exact_current_item_and_preserves_suffix(self) -> None:
        self.config.document["preprocess"]["max_items"] = 2
        store = ControlStore(self.config)
        store.set_desired_state("running")
        reference = self.config.document["campaign"]["schedules"][0]
        schedule = {
            "consumer": {
                "preprocess_state_root": str(self.config.state_root / "pre-state")
            }
        }
        bundle = {
            "manifest": {
                "work_orders": [
                    {"queue_ordinal": ordinal} for ordinal in (1, 2, 3)
                ]
            },
            "orders": [{"ordinal": ordinal} for ordinal in (1, 2, 3)],
        }
        states = [
            {"queue_state": "completed"} for _ordinal in (1, 2, 3)
        ]
        ready_before = {
            "ready_item_count": 2,
            "ready_byte_count": 200,
            "items": [
                {"ordinal": 1, "media_byte_count": 100},
                {"ordinal": 2, "media_byte_count": 100},
            ],
        }
        ready_after = {
            "ready_item_count": 1,
            "ready_byte_count": 100,
            "items": [{"ordinal": 2, "media_byte_count": 100}],
        }
        runtime_before = (reference, schedule, bundle, states, ready_before)
        original_calls = []

        def run_one(row, **_kwargs):
            original_calls.append(row["queue_ordinal"])
            store.set_desired_state("stopped")
            ordinal = row["queue_ordinal"]
            bundle_id = f"ppbatch_{ordinal:032x}"
            return {
                "queue_ordinal": ordinal,
                "preprocess_receipt_count": 1,
                "bundle": {
                    "path": str(
                        Path(self.config.document["preprocess"]["bundle_root"])
                        / "bundles"
                        / bundle_id
                    ),
                    "bundle_id": bundle_id,
                    "manifest_sha256": f"{ordinal:064x}",
                },
            }

        handoff = SimpleNamespace(_materialize_and_run_one=run_one)

        def run_handoff(_path, **kwargs):
            self.assertEqual(2, kwargs["limit"])
            rows = [{"queue_ordinal": 1}, {"queue_ordinal": 2}]
            return {
                "processed_items": [
                    handoff._materialize_and_run_one(row) for row in rows
                ]
            }

        handoff.run_handoff = run_handoff
        modules = SimpleNamespace(
            background=SimpleNamespace(
                _completed_state=lambda value: isinstance(value, dict),
                _quarantined_state=lambda _value: False,
            ),
            handoff=handoff,
            preprocess_batch=SimpleNamespace(BatchError=RuntimeError),
            queue_runner=SimpleNamespace(),
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        original_boundary = handoff._materialize_and_run_one
        with (
            mock.patch.object(
                backend, "_campaign_runtimes", return_value=[runtime_before]
            ),
            mock.patch.object(
                backend,
                "_schedule_runtime",
                return_value=(schedule, bundle, states, ready_after),
            ) as reconcile,
        ):
            outcome = backend._run_preprocess()
        self.assertEqual([1], original_calls)
        self.assertIs(original_boundary, handoff._materialize_and_run_one)
        reconcile.assert_called_once_with(reference)
        self.assertEqual("progressed", outcome.status)
        self.assertTrue(outcome.progressed)
        self.assertEqual(1, outcome.monitor["processed_items"])
        self.assertEqual(1, outcome.monitor["ready_items_after"])
        self.assertTrue(
            outcome.monitor["durable_stop_observed_between_items"]
        )
        self.assertEqual(
            "durable_stop_requested_between_preprocess_items",
            outcome.monitor["stop_reason"],
        )
        self.assertEqual(
            [1],
            [
                row["queue_ordinal"]
                for row in outcome.artifacts["preprocess_bundles"]
            ],
        )

        # Also cover the begin/dispatch race: Stop may win after the lane is
        # admitted but before its first singleton source call.
        original_calls.clear()
        stopped_backend = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        with (
            mock.patch.object(
                stopped_backend,
                "_campaign_runtimes",
                return_value=[runtime_before],
            ),
            mock.patch.object(
                stopped_backend,
                "_schedule_runtime",
                return_value=(schedule, bundle, states, ready_before),
            ) as stopped_reconcile,
        ):
            stopped = stopped_backend._run_preprocess()
        self.assertEqual([], original_calls)
        self.assertIs(original_boundary, handoff._materialize_and_run_one)
        stopped_reconcile.assert_called_once_with(reference)
        self.assertEqual("held", stopped.status)
        self.assertFalse(stopped.progressed)
        self.assertEqual(0, stopped.monitor["processed_items"])
        self.assertEqual(2, stopped.monitor["ready_items_after"])
        self.assertTrue(stopped.monitor["durable_stop_observed_between_items"])
        self.assertEqual([], stopped.artifacts["preprocess_bundles"])

        # Acquisition failure isolation may make a lower ordinal ready after the
        # lane snapshot. The handoff's actual gated prefix, not stale identities,
        # is authoritative at the Stop boundary.
        store.set_desired_state("running")
        original_calls.clear()
        lower_states = [
            {"queue_state": "completed"},
            {"queue_state": "completed"},
            {"queue_state": "completed"},
        ]
        stale_ready = {
            "ready_item_count": 2,
            "ready_byte_count": 200,
            "items": [
                {"ordinal": 2, "media_byte_count": 100},
                {"ordinal": 3, "media_byte_count": 100},
            ],
        }
        after_lower_prefix = {
            "ready_item_count": 2,
            "ready_byte_count": 200,
            "items": [
                {"ordinal": 2, "media_byte_count": 100},
                {"ordinal": 3, "media_byte_count": 100},
            ],
        }
        lower_backend = SealedArchiveBackend(
            self.config, modules=modules  # type: ignore[arg-type]
        )
        with (
            mock.patch.object(
                lower_backend,
                "_campaign_runtimes",
                return_value=[
                    (reference, schedule, bundle, lower_states, stale_ready)
                ],
            ),
            mock.patch.object(
                lower_backend,
                "_schedule_runtime",
                return_value=(
                    schedule,
                    bundle,
                    lower_states,
                    after_lower_prefix,
                ),
            ),
        ):
            lower = lower_backend._run_preprocess()
        self.assertEqual([1], original_calls)
        self.assertEqual("progressed", lower.status)
        self.assertEqual(1, lower.monitor["processed_items"])
        self.assertEqual(2, lower.monitor["ready_items_after"])
        self.assertEqual(
            [1],
            [
                row["queue_ordinal"]
                for row in lower.artifacts["preprocess_bundles"]
            ],
        )

    def test_overlap_keeps_acquisition_on_main_thread_and_replays_only_touched_epoch(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        first, second = [
            row["schedule_id"]
            for row in self.config.document["campaign"]["schedules"]
        ]
        old_first = ({"old": 1}, {}, [], {})
        old_second = ({"old": 2}, {}, [], {})
        backend._runtime_cache = {first: old_first, second: old_second}  # type: ignore[assignment]
        acquisition_started = threading.Event()
        preprocess_started = threading.Event()
        main_ident = threading.get_ident()
        observed_threads: dict[str, int] = {}

        def acquire():
            observed_threads["acquisition"] = threading.get_ident()
            acquisition_started.set()
            self.assertTrue(preprocess_started.wait(2))
            return SimpleNamespace()

        def preprocess():
            observed_threads["preprocess"] = threading.get_ident()
            preprocess_started.set()
            self.assertTrue(acquisition_started.wait(2))
            return SimpleNamespace()

        acquisition_outcome = StageOutcome(
            "acquisition",
            "progressed",
            True,
            {"active_schedule_id": first},
            {},
        )
        preprocess_outcome = StageOutcome(
            "preprocess",
            "held",
            False,
            {},
            {},
        )
        refreshed_first = ({"fresh": 1}, {}, [], {})
        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[]),
            mock.patch.object(
                backend,
                "_run_acquisition",
                side_effect=lambda: (acquire(), acquisition_outcome)[1],
            ),
            mock.patch.object(
                backend,
                "_run_preprocess",
                side_effect=lambda: (preprocess(), preprocess_outcome)[1],
            ),
            mock.patch.object(
                backend, "_schedule_runtime", return_value=refreshed_first
            ) as replay,
        ):
            outcomes = backend.run_parallel_stages()
        self.assertEqual((acquisition_outcome, preprocess_outcome), outcomes)
        self.assertEqual(main_ident, observed_threads["acquisition"])
        self.assertNotEqual(main_ident, observed_threads["preprocess"])
        replay.assert_called_once()
        self.assertIs(refreshed_first, backend._runtime_cache[first])
        self.assertIs(old_second, backend._runtime_cache[second])

    def test_overlap_failure_preserves_untouched_epoch_and_replays_successful_peer(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        first, second = [
            row["schedule_id"]
            for row in self.config.document["campaign"]["schedules"]
        ]
        old_first = ({"old": 1}, {}, [], {})
        old_second = ({"old": 2}, {}, [], {})
        backend._runtime_cache = {first: old_first, second: old_second}  # type: ignore[assignment]
        preprocess_finished = threading.Event()
        refreshed_first = ({"fresh": 1}, {}, [], {})

        def acquire():
            backend._runtime_cache.pop(first, None)
            raise BackendError("finite acquisition failure")

        def preprocess():
            preprocess_finished.set()
            return StageOutcome(
                "preprocess",
                "progressed",
                True,
                {"active_schedule_id": first, "processed_items": 1},
                {},
            )

        with (
            mock.patch.object(backend, "_campaign_runtimes", return_value=[]),
            mock.patch.object(backend, "_run_acquisition", side_effect=acquire),
            mock.patch.object(backend, "_run_preprocess", side_effect=preprocess),
            mock.patch.object(
                backend, "_schedule_runtime", return_value=refreshed_first
            ) as replay,
        ):
            with self.assertRaisesRegex(BackendError, "acquisition lane failed"):
                backend.run_parallel_stages()
        self.assertTrue(preprocess_finished.is_set())
        replay.assert_called_once()
        self.assertIs(refreshed_first, backend._runtime_cache[first])
        self.assertIs(old_second, backend._runtime_cache[second])

    def test_cold_storage_guard_binds_reviewed_uuid_and_rejects_hot_primary(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        # Exercise the production cold-primary mode; the generic fixture keeps
        # retention enabled for its separate adapter tests.
        backend.config.document["cold_retention"]["enabled"] = False
        cold_runtime = (
            {},
            {},
            {
                "manifest": {
                    "policy": {
                        "media_output_root": "/mnt/archive/HIMR/corpus/raw"
                    }
                }
            },
            [],
            {},
        )
        with self._fake_cold_mount("5b5813ad-b1a4-4f52-9960-e762ceac5636"):
            observed = backend._validate_cold_storage_identity([cold_runtime])
        self.assertEqual(
            "5b5813ad-b1a4-4f52-9960-e762ceac5636", observed["uuid"]
        )
        hot_runtime = (
            {},
            {},
            {"manifest": {"policy": {"media_output_root": str(self.config.state_root)}}},
            [],
            {},
        )
        with self._fake_cold_mount("5b5813ad-b1a4-4f52-9960-e762ceac5636"):
            with self.assertRaisesRegex(BackendError, "cold-primary"):
                backend._validate_cold_storage_identity([hot_runtime])

    @contextmanager
    def _fake_cold_mount(self, uuid: str):
        def observed(path, **_kwargs):
            if str(path).startswith("/dev/"):
                device = os.makedev(8, 1)
                if str(path).startswith("/dev/disk/by-uuid/") and path.name != uuid:
                    device = os.makedev(8, 2)
                return SimpleNamespace(st_mode=stat.S_IFBLK | 0o600, st_rdev=device)
            return SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_dev=os.makedev(8, 1))

        with (
            mock.patch.object(Path, "resolve", lambda path, **kwargs: path),
            mock.patch.object(Path, "stat", observed),
            mock.patch.object(
                Path, "read_bytes",
                return_value=b"100 20 8:1 / /mnt/archive rw - xfs /dev/fake-archive rw\n",
            ),
        ):
            yield

    def test_reviewed_archive_replacement_keeps_config_and_requires_new_uuid(self) -> None:
        backend = SealedArchiveBackend(self.config, modules=SimpleNamespace())
        document = dict(self.config.document)
        document["config_id"] = "himrautocfg_cbf42c1ecf0e1c59c221e9711e6f6047"
        backend.config = replace(
            self.config, document=document,
            physical_sha256="5d0b549e91a870874e17724510723b33e557288f507ef41a35ab2849656dc913",
        )
        before = canonical_bytes(backend.config.document)
        with self._fake_cold_mount("af41b7da-a588-41cf-83f8-cd99ef425b74"):
            observed = backend._validate_cold_storage_identity([])
        self.assertEqual(observed["uuid"], "af41b7da-a588-41cf-83f8-cd99ef425b74")
        self.assertEqual(
            observed["reviewed_migration"]["from_uuid"],
            "5b5813ad-b1a4-4f52-9960-e762ceac5636",
        )
        self.assertEqual(before, canonical_bytes(backend.config.document))
        with self._fake_cold_mount("5b5813ad-b1a4-4f52-9960-e762ceac5636"):
            with self.assertRaisesRegex(BackendError, "reviewed UUID"):
                backend._validate_cold_storage_identity([])
        backend.config = replace(backend.config, physical_sha256="f" * 64)
        with self._fake_cold_mount("af41b7da-a588-41cf-83f8-cd99ef425b74"):
            with self.assertRaisesRegex(BackendError, "reviewed UUID"):
                backend._validate_cold_storage_identity([])

    def test_fresh_campaign_binds_replacement_disk_without_historical_migration(self) -> None:
        from autonomous_controller.config import REPLACEMENT_SAFETY

        document = {**self.config.document, "safety": dict(REPLACEMENT_SAFETY)}
        config = replace(self.config, document=document)
        backend = SealedArchiveBackend(config, modules=SimpleNamespace())
        with self._fake_cold_mount(REPLACEMENT_SAFETY["cold_mount_uuid"]):
            observed = backend._validate_cold_storage_identity([])
        self.assertEqual(observed["uuid"], REPLACEMENT_SAFETY["cold_mount_uuid"])
        self.assertNotIn("reviewed_migration", observed)
        with self._fake_cold_mount("5b5813ad-b1a4-4f52-9960-e762ceac5636"):
            with self.assertRaisesRegex(BackendError, "reviewed UUID"):
                backend._validate_cold_storage_identity([])

    def test_cold_only_partial_restart_drains_without_preprocess_ack_and_blocks_on_backlog(self) -> None:
        class ColdResumeBackground:
            def __init__(self) -> None:
                self.states = [
                    {"queue_state": "completed"} if ordinal <= 4 else None
                    for ordinal in range(1, 9)
                ]

            @staticmethod
            def _completed_state(state):
                return isinstance(state, dict) and state.get("queue_state") == "completed"

            @staticmethod
            def _quarantined_state(_state):
                return False

            def ready(self):
                ordinals = [
                    ordinal
                    for ordinal, state in enumerate(self.states, 1)
                    if self._completed_state(state)
                ]
                return {
                    "ready_item_count": len(ordinals),
                    "ready_byte_count": 100 * len(ordinals),
                    "items": [
                        {"ordinal": ordinal, "media_byte_count": 100}
                        for ordinal in ordinals
                    ],
                    "zone": "at_or_below_low_water",
                }

            def run_producer(self, _path, **_limits):
                admitted = 0
                for index, state in enumerate(self.states):
                    if state is None and admitted < 2:
                        self.states[index] = {"queue_state": "completed"}
                        admitted += 1
                return {
                    "status": "completed" if all(self.states) else "bounded",
                    "stop_reason": "all_acquired" if all(self.states) else "max_new_items",
                    "ready_after": self.ready(),
                    "queue_summary": {
                        "new_item_count": admitted,
                        "new_byte_count": admitted * 100,
                        "new_failed_attempt_count": 0,
                        "new_quarantined_count": 0,
                        "retryable_failed_count": 0,
                        "failed_attempt_count": 0,
                    },
                }

            def _runtime_from_queue_summary(self, _schedule, _summary, **_kwargs):
                return cold_bundle, list(self.states), self.ready()

        background = ColdResumeBackground()
        handoff = SimpleNamespace(run_handoff=mock.Mock())
        modules = SimpleNamespace(
            background=background,
            handoff=handoff,
            queue_runner=fake_queue_runner(),
        )
        normal_reference = self.config.document["campaign"]["schedules"][0]
        cold_reference = {
            **self.config.document["campaign"]["schedules"][1],
            "role": "cold_acquisition_only_requires_chunking",
        }
        normal_runtime = (
            normal_reference,
            {},
            {},
            [],
            {
                "ready_item_count": 0,
                "ready_byte_count": 0,
                "items": [],
                "zone": "at_or_below_low_water",
            },
        )
        cold_bundle: dict = {}

        def runtimes():
            return [
                normal_runtime,
                (
                    cold_reference,
                    {},
                    cold_bundle,
                    list(background.states),
                    background.ready(),
                ),
            ]

        first = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        with (
            mock.patch.object(first, "_campaign_runtimes", side_effect=runtimes),
            mock.patch.object(first, "_validate_cold_storage_identity"),
        ):
            partial = first._run_acquisition()
            preprocess_partial = first._run_preprocess()
        self.assertEqual(6, partial.monitor["completed"])
        self.assertEqual(2, partial.monitor["pending"])
        self.assertEqual("cold_acquisition_only_requires_chunking", partial.monitor["active_schedule_role"])
        self.assertEqual("complete", preprocess_partial.status)
        handoff.run_handoff.assert_not_called()

        # A new backend reconstructs the same durable six completed states, then
        # drains the final two without requiring any preprocess acknowledgement.
        resumed = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        with (
            mock.patch.object(resumed, "_campaign_runtimes", side_effect=runtimes),
            mock.patch.object(resumed, "_validate_cold_storage_identity"),
        ):
            resumed_progress = resumed._run_acquisition()
            drained = resumed._run_acquisition()
            preprocess_drained = resumed._run_preprocess()
        self.assertEqual(2, resumed_progress.monitor["new_items"])
        self.assertEqual(0, resumed_progress.monitor["pending"])
        self.assertEqual("complete", drained.status)
        self.assertEqual(8, drained.monitor["cold_only_ready_items"])
        self.assertEqual("complete", preprocess_drained.status)
        handoff.run_handoff.assert_not_called()

        controller = AutonomousController(
            self.config,
            ControlStore(self.config),
            SimpleNamespace(),  # type: ignore[arg-type]
        )
        controller._monitor = {
            "recovery": {
                "campaign_coverage": {"parked_requires_chunking_count": 724}
            },
            "acquisition": {"status": "complete", **drained.monitor},
            "preprocess": {"status": "complete", **preprocess_drained.monitor},
            "gpu_readiness": {
                "status": "complete",
                "pending_batches": 0,
                "active_children": 0,
                "requires_chunking_items_cumulative": 0,
                "parked_items": 0,
            },
            "cold_retention": {
                "status": "skipped",
                "pending_items": 0,
                "replay_pending_items": 0,
            },
        }
        self.assertEqual(
            "primary_pass_drained_with_postprocess_backlog",
            controller._terminal_disposition(),
        )

    def test_gpu_chunking_dispositions_are_cumulative_and_deduplicated_by_queue(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        disposition = {
            "member_count": 32,
            "ready_count": 7,
            "requires_chunking_count": 25,
            "explicit_skip_count": 0,
            "ready_audio_duration_ms": 100,
            "requires_chunking_audio_duration_ms": 900,
        }
        first = {
            "queue": {"queue_id": "gpuasrqueue1_" + "1" * 32},
            "queue_disposition": disposition,
        }
        backend._register_gpu_disposition(first)
        backend._register_gpu_disposition(first)
        totals = backend._gpu_disposition_totals()
        self.assertEqual(25, totals["requires_chunking_count"])
        self.assertEqual(32, totals["member_count"])
        conflicting = {
            **first,
            "queue_disposition": {**disposition, "requires_chunking_count": 24},
        }
        with self.assertRaisesRegex(BackendError, "conflicting"):
            backend._register_gpu_disposition(conflicting)

    @staticmethod
    def _pack_candidate(
        ordinal: int, *, duration_ms: int, audio_sha256: str | None = None
    ) -> dict:
        queue_id = f"gpuasrqueue_{ordinal:032x}"
        return {
            "campaign_ordinal": ordinal,
            "bundle_id": f"ppbatch_{ordinal:032x}",
            "queue": {
                "path": f"/private/queue-{ordinal}.json",
                "sha256": f"{ordinal:064x}",
                "queue_id": queue_id,
            },
            "queue_path": Path(f"/private/queue-{ordinal}.json"),
            "queue_disposition": {
                "member_count": 1,
                "ready_count": 1,
                "requires_chunking_count": 0,
                "explicit_skip_count": 0,
                "ready_audio_duration_ms": duration_ms,
                "requires_chunking_audio_duration_ms": 0,
            },
            "member": {
                "ordinal": 1,
                "preprocess_ordinal": 1,
                "member_id": f"gpuasrmember_{ordinal:032x}",
                "audio": {
                    "sha256": audio_sha256 or f"{ordinal + 1000:064x}",
                    "duration_ms": duration_ms,
                },
                "resource_disposition": {"state": "ready"},
            },
        }

    def test_cross_queue_selector_packs_sixteen_and_balances_fixed_pairs(self) -> None:
        candidates = [
            self._pack_candidate(ordinal, duration_ms=ordinal * 1_000)
            for ordinal in range(1, 17)
        ]
        packs, remaining = SealedArchiveBackend._select_gpu_packs(
            candidates,
            maximum_items=32,
            maximum_total_audio_ms=1_000_000,
            preferred_total_audio_ms=900_000,
            maximum_batches=1,
            allow_partial=False,
        )
        self.assertEqual([], remaining)
        self.assertEqual(1, len(packs))
        self.assertEqual(
            list(range(16, 0, -1)),
            [row["campaign_ordinal"] for row in packs[0]],
        )
        self.assertEqual(
            16,
            len(
                {
                    row["member"]["audio"]["sha256"]
                    for row in packs[0]
                }
            ),
        )

    def test_cross_queue_selector_splits_duplicate_audio_between_batches(self) -> None:
        duplicate = "f" * 64
        candidates = [
            self._pack_candidate(
                ordinal, duration_ms=1_000, audio_sha256=duplicate
            )
            for ordinal in range(1, 17)
        ]
        packs, remaining = SealedArchiveBackend._select_gpu_packs(
            candidates,
            maximum_items=32,
            maximum_total_audio_ms=1_000_000,
            preferred_total_audio_ms=900_000,
            maximum_batches=2,
            allow_partial=False,
        )
        self.assertEqual([1], [len(pack) for pack in packs])
        self.assertEqual(15, len(remaining))
        tail_packs, tail = SealedArchiveBackend._select_gpu_packs(
            remaining,
            maximum_items=32,
            maximum_total_audio_ms=1_000_000,
            preferred_total_audio_ms=900_000,
            maximum_batches=1,
            allow_partial=True,
        )
        self.assertEqual([1], [len(pack) for pack in tail_packs])
        self.assertEqual(14, len(tail))
        self.assertNotEqual(
            packs[0][0]["queue"]["queue_id"],
            tail_packs[0][0]["queue"]["queue_id"],
        )

    def test_partial_gpu_tail_waits_sixty_seconds_without_reset_on_addition(self) -> None:
        backend = SealedArchiveBackend(
            self.config, modules=SimpleNamespace()  # type: ignore[arg-type]
        )
        candidates = [
            self._pack_candidate(ordinal, duration_ms=1_000)
            for ordinal in range(1, 4)
        ]
        with mock.patch(
            "autonomous_controller.sealed_backend.time.monotonic", return_value=100.0
        ):
            self.assertFalse(
                backend._bounded_gpu_partial_flush(candidates, force=False)
            )
        with mock.patch(
            "autonomous_controller.sealed_backend.time.monotonic", return_value=159.9
        ):
            self.assertFalse(
                backend._bounded_gpu_partial_flush(candidates, force=False)
            )
        candidates.append(self._pack_candidate(4, duration_ms=1_000))
        with mock.patch(
            "autonomous_controller.sealed_backend.time.monotonic", return_value=160.0
        ):
            self.assertTrue(
                backend._bounded_gpu_partial_flush(candidates, force=False)
            )
        self.assertTrue(backend._bounded_gpu_partial_flush(candidates, force=True))
        self.assertIsNone(backend._gpu_partial_hold_started_at)

    def test_partial_gpu_pack_can_launch_on_preferred_duration(self) -> None:
        candidates = [
            self._pack_candidate(1, duration_ms=500_000),
            self._pack_candidate(2, duration_ms=400_000),
        ]
        self.assertTrue(
            SealedArchiveBackend._gpu_partial_reaches_preferred_duration(
                candidates,
                maximum_items=32,
                maximum_total_audio_ms=2_000_000,
                preferred_total_audio_ms=900_000,
            )
        )
        self.assertFalse(
            SealedArchiveBackend._gpu_partial_reaches_preferred_duration(
                candidates,
                maximum_items=32,
                maximum_total_audio_ms=2_000_000,
                preferred_total_audio_ms=900_001,
            )
        )

    def test_packed_record_replays_multiple_sources_and_coexists_with_legacy_claim(self) -> None:
        def canonical(value):
            return (
                json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode()

        queue_manifests = {}
        receipts = {}
        work_orders = {}
        sources = []
        batch_members = []
        aggregate_orders = []
        for ordinal in (2, 3):
            queue_id = f"gpuasrqueue_{ordinal:032x}"
            queue_path = f"/private/queue-{ordinal}.json"
            queue = {
                "queue_id": queue_id,
                "totals": {
                    "member_count": 1,
                    "ready_count": 1,
                    "requires_chunking_count": 0,
                    "explicit_skip_count": 0,
                    "ready_audio_duration_ms": ordinal * 1_000,
                    "requires_chunking_audio_duration_ms": 0,
                },
            }
            queue_manifests[queue_path] = queue
            queue_sha256 = hashlib.sha256(canonical(queue)).hexdigest()
            receipt_path = f"/private/receipt-{ordinal}.json"
            order_path = f"/private/work-order-{ordinal}.json"
            order = {
                "work_order_id": f"gpuasrwo5_{ordinal:032x}",
                "identity_sha256": f"{ordinal + 10:064x}",
            }
            work_orders[order_path] = order
            receipt = {
                "receipt_id": f"gpuasrmat1_{ordinal:032x}",
                "source_queue": {
                    "path": queue_path,
                    "sha256": queue_sha256,
                    "queue_id": queue_id,
                },
                "selection": {"queue_ordinals": [1]},
                "work_orders": [
                    {
                        "queue_ordinal": 1,
                        "preprocess_ordinal": 1,
                        "member_id": f"gpuasrmember_{ordinal:032x}",
                        "path": order_path,
                        "sha256": f"{ordinal + 20:064x}",
                        "work_order_id": order["work_order_id"],
                        "identity_sha256": order["identity_sha256"],
                    }
                ],
                "batch": {"execution_class": "local_private_production_asr"},
            }
            receipts[receipt_path] = receipt
            disposition = SealedArchiveBackend._gpu_queue_disposition(queue)
            sources.append(
                {
                    "preprocess_bundle_id": f"ppbatch_{ordinal:032x}",
                    "queue": {
                        "path": queue_path,
                        "sha256": queue_sha256,
                        "queue_id": queue_id,
                    },
                    "queue_disposition": disposition,
                    "queue_ordinals": [1],
                    "materialization_receipt": {
                        "path": receipt_path,
                        "sha256": f"{ordinal + 30:064x}",
                        "receipt_id": receipt["receipt_id"],
                    },
                }
            )
            batch_members.append(
                {
                    "batch_ordinal": len(batch_members) + 1,
                    "queue_id": queue_id,
                    "queue_ordinal": 1,
                    "preprocess_ordinal": 1,
                    "member_id": receipt["work_orders"][0]["member_id"],
                    "work_order_id": order["work_order_id"],
                    "work_order_sha256": receipt["work_orders"][0]["sha256"],
                    "work_order_identity_sha256": order["identity_sha256"],
                }
            )
            aggregate_orders.append(order)

        batch_id = "gpuasrbatch2_" + "a" * 32
        batch = {
            "batch_id": batch_id,
            "execution_class": "local_private_production_asr",
            "totals": {"item_count": 2},
            "items": [{"work_order": order} for order in aggregate_orders],
        }
        bridge = SimpleNamespace(
            load_receipt=lambda path, _digest, replay: receipts[path],
            ASR_V5=SimpleNamespace(
                load_work_order=lambda path, **_kwargs: work_orders[path]
            ),
            BATCH_V2=SimpleNamespace(
                load_manifest=lambda _path, _digest, profile: (batch, b"batch"),
                batch_status=lambda _batch, _profile: {
                    "status": "pending",
                    "batch_id": batch_id,
                    "completed_ordinals": [],
                    "absent_ordinals": [1, 2],
                    "invalid": [],
                    "inference_performed": False,
                    "files_written": False,
                },
            ),
        )
        modules = SimpleNamespace(
            gpu_queue=SimpleNamespace(
                MAX_ITEMS=128,
                validate_queue=lambda manifest_path, **_kwargs: queue_manifests[
                    str(manifest_path)
                ],
                canonical_bytes=canonical,
            ),
            gpu_bridge=bridge,
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        backend._claim_gpu_members("legacy:one", [("gpuasrqueue_" + "1" * 32, 1)])
        record = {
            "record_kind": "ready_batch",
            "record_format": GPU_PACK_RECORD_FORMAT,
            "batch_key": f"packed:{batch_id}",
            "sources": sources,
            "batch_members": batch_members,
            "batch": {
                "path": "/private/aggregate.json",
                "sha256": "e" * 64,
                "batch_id": batch_id,
            },
        }
        with mock.patch.object(backend, "_load_profile", return_value={}):
            self.assertEqual("pending", backend._validate_gpu_record(record))
        self.assertEqual(3, len(backend._gpu_member_claims))
        self.assertEqual(
            f"packed:{batch_id}",
            backend._gpu_member_claims[(sources[0]["queue"]["queue_id"], 1)],
        )
        with self.assertRaisesRegex(BackendError, "multiple batches"):
            backend._claim_gpu_members(
                "another-batch", [(sources[0]["queue"]["queue_id"], 1)]
            )
        tampered = {**record, "batch_members": list(reversed(batch_members))}
        second = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        with (
            mock.patch.object(second, "_load_profile", return_value={}),
            self.assertRaisesRegex(BackendError, "batch ordinals"),
        ):
            second._validate_gpu_record(tampered)

        malformed_peer = {
            **record,
            "batch_members": [batch_members[0], None],
        }
        peer = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        with (
            mock.patch.object(peer, "_load_profile", return_value={}),
            self.assertRaisesRegex(BackendError, "member 2 is malformed"),
        ):
            peer._observe_gpu_record(malformed_peer, "pending")
        self.assertEqual({}, peer._gpu_records)
        self.assertEqual({}, peer._gpu_status)

        missing_status = StageOutcome(
            "gpu_readiness",
            "progressed",
            True,
            {},
            {
                "backend_kind": BACKEND_KIND,
                "gpu_records": [record],
                "gpu_record_statuses": {},
            },
        )
        with self.assertRaisesRegex(BackendError, "record/status ledgers differ"):
            peer.observe_peer_outcome(missing_status)

        exact_peer = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        with mock.patch.object(exact_peer, "_load_profile", return_value={}):
            exact_peer._observe_gpu_record(record, "pending")
        self.assertEqual(2, exact_peer._gpu_batch_item_counts[record["batch_key"]])
        self.assertEqual("pending", exact_peer._gpu_status[record["batch_key"]])
        with (
            mock.patch.object(exact_peer, "_load_profile", return_value={}),
            mock.patch.object(
                modules.gpu_queue,
                "validate_queue",
                side_effect=RecursionError("source queue replay tripwire"),
            ),
            mock.patch.object(
                bridge,
                "load_receipt",
                side_effect=RecursionError("materialization replay tripwire"),
            ),
            mock.patch.object(
                bridge.ASR_V5,
                "load_work_order",
                side_effect=RecursionError("external order replay tripwire"),
            ),
        ):
            self.assertEqual(1, exact_peer._refresh_gpu_status())

        legacy_restore = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        with mock.patch.object(legacy_restore, "_load_profile", return_value={}):
            legacy_restore._restore_gpu_record(record, None)
            legacy_restore._restore_gpu_record(record, "pending")
        self.assertEqual(
            "pending", legacy_restore._gpu_status[record["batch_key"]]
        )
        self.assertEqual(
            2, legacy_restore._gpu_batch_item_counts[record["batch_key"]]
        )

        false_completed = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        false_completed._gpu_records = {
            "existing": {"record_kind": "no_ready_members"}
        }
        false_completed._gpu_status = {"existing": "not_applicable"}
        false_completed._gpu_queue_dispositions = {
            "existing-queue": {"sentinel": 1}
        }
        false_completed._gpu_parked = {
            "existing": {"item_count": 0}
        }
        false_completed._gpu_batch_item_counts = {"existing": 0}
        false_completed._gpu_member_claims = {
            ("existing-queue", 1): "existing"
        }
        ledger_names = (
            "_gpu_records",
            "_gpu_status",
            "_gpu_queue_dispositions",
            "_gpu_parked",
            "_gpu_batch_item_counts",
            "_gpu_member_claims",
        )
        before = {
            name: dict(getattr(false_completed, name)) for name in ledger_names
        }
        durable_evidence = false_completed._gpu_event_evidence(
            "stage_finished",
            {"stage": "gpu_readiness"},
            {
                "gpu_records": [record],
                "gpu_record_statuses": {record["batch_key"]: "completed"},
            },
        )
        with (
            mock.patch.object(false_completed, "_load_profile", return_value={}),
            self.assertRaisesRegex(BackendError, "exact batch replay"),
        ):
            for durable_record, durable_status in durable_evidence:
                false_completed._observe_gpu_record(
                    durable_record, durable_status
                )
        for name in ledger_names:
            self.assertEqual(before[name], getattr(false_completed, name), name)

    def test_new_pack_is_not_launched_before_its_stage_record_is_returned(self) -> None:
        queue = {
            "queue_id": "gpuasrqueue_" + "a" * 32,
            "members": [
                self._pack_candidate(ordinal, duration_ms=1_000)["member"]
                for ordinal in range(1, 17)
            ],
            "totals": {
                "member_count": 16,
                "ready_count": 16,
                "requires_chunking_count": 0,
                "explicit_skip_count": 0,
                "ready_audio_duration_ms": 16_000,
                "requires_chunking_audio_duration_ms": 0,
            },
        }
        modules = SimpleNamespace(
            gpu_queue=SimpleNamespace(
                canonical_bytes=lambda value: json.dumps(
                    value, sort_keys=True
                ).encode()
            )
        )
        backend = SealedArchiveBackend(self.config, modules=modules)  # type: ignore[arg-type]
        packed = {
            "record_kind": "ready_batch",
            "record_format": GPU_PACK_RECORD_FORMAT,
            "batch_key": "packed:gpuasrbatch2_" + "b" * 32,
            "sources": [],
            "batch_members": [],
            "batch": {
                "path": "/private/batch.json",
                "sha256": "c" * 64,
                "batch_id": "gpuasrbatch2_" + "b" * 32,
            },
        }
        supervise = mock.Mock(return_value=(False, None, 0))

        def validate_created(record):
            backend._gpu_batch_item_counts[record["batch_key"]] = 16
            return "pending"

        with (
            mock.patch.object(backend, "_refresh_gpu_status", return_value=0),
            mock.patch.object(backend, "_load_profile", return_value={
                "batch_limits": {
                    "ready_batch_high_water": 2,
                    "maximum_items": 32,
                    "maximum_total_audio_ms": 1_000_000,
                    "preferred_total_audio_ms": 900_000,
                }
            }),
            mock.patch.object(backend, "_supervise_gpu_child", supervise),
            mock.patch.object(
                backend,
                "_preprocess_bundle_candidates",
                return_value=[
                    (1, Path("/private/bundle"), "ppbatch_" + "d" * 32, Path("/private/state"))
                ],
            ),
            mock.patch.object(
                backend,
                "_gpu_queue_for_bundle",
                return_value=(queue, Path("/private/queue.json")),
            ),
            mock.patch.object(backend, "_gpu_packed_record", return_value=packed),
            mock.patch.object(
                backend, "_validate_gpu_record", side_effect=validate_created
            ),
            mock.patch.object(backend, "_register_gpu_disposition"),
        ):
            outcome = backend._run_gpu_readiness()
        self.assertTrue(outcome.progressed)
        self.assertEqual(1, outcome.monitor["new_batches"])
        self.assertEqual(1, supervise.call_count)


if __name__ == "__main__":
    unittest.main()
