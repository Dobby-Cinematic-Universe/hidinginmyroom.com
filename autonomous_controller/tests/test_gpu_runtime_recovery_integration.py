"""Offline backend integration checks for an explicit GPU runtime successor.

Only synthetic batch metadata is read. No runtime/model/media validation,
subprocesses, GPU work, controller state, or deployed configuration is used.
"""

from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

from autonomous_controller import sealed_backend as backend_module
from autonomous_controller.config import canonical_bytes, sha256_bytes
from autonomous_controller.gpu_child import LocalPrivateGpuLaunchSpec
from autonomous_controller.sealed_backend import BackendError, SealedArchiveBackend


class GpuRuntimeRecoveryIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.batch_sequence = 0
        self.old = {
            name: str(self.root / name)
            for name in (
                "runtime_admission", "production_profile", "root_registration",
                "launcher_profile", "local_readiness", "local_launcher",
                "result_root", "event_root", "lock_root", "working_directory",
                "work_order_root", "receipt_root", "batch_root",
            )
        }
        for index, name in enumerate(("runtime_admission", "production_profile",
                                      "root_registration", "launcher_profile", "local_readiness"), 1):
            self.old[name + "_sha256"] = str(index) * 64
        self.old["execution_mode"] = "local-private-production"
        self.old["max_active_children"] = 1
        self.old["max_attempts_per_batch"] = 3
        self.new = dict(self.old)
        for index, name in enumerate(("runtime_admission", "launcher_profile", "local_readiness"), 6):
            self.new[name] = str(self.root / ("successor-" + name))
            self.new[name + "_sha256"] = str(index) * 64
        self.new["local_launcher"] = str(self.root / "successor-launcher")
        self.config = SimpleNamespace(section=self._section)
        self.backend = SealedArchiveBackend(self.config, modules=SimpleNamespace(
            queue_runner=SimpleNamespace(_stable_read=self._read_batch_fixture)))
        self.successor = backend_module.gpu_runtime_successor

    def _read_batch_fixture(self, path, *, maximum, label):
        self.assertEqual(self.root, path.parent)
        body = path.read_bytes()
        self.assertLessEqual(len(body), maximum)
        return body, path.stat()

    def _section(self, name):
        self.assertEqual("gpu_readiness", name)
        return dict(self.old)

    def _runtime(self, gpu):
        return {"receipt_path": gpu["runtime_admission"],
                "receipt_sha256": gpu["runtime_admission_sha256"]}

    def _effective(self, config, *, batch_runtime=None):
        self.assertIs(config, self.config)
        if batch_runtime is None or batch_runtime == self._runtime(self.new):
            return dict(self.new)
        if batch_runtime == self._runtime(self.old):
            return dict(self.old)
        raise ValueError("unknown batch runtime")

    def _batch(self, runtime=None):
        manifest = {"batch_id": "gpuasrbatch2_" + "a" * 32,
                    "runtime_admission": self._runtime(self.old) if runtime is None else runtime}
        body = canonical_bytes(manifest)
        self.batch_sequence += 1
        path = self.root / f"batch-{self.batch_sequence}.json"
        path.write_bytes(body)
        path.chmod(0o400)
        return {"record_kind": "ready_batch", "batch_key": "fixture",
                "batch": {"path": str(path), "sha256": sha256_bytes(body),
                          "batch_id": manifest["batch_id"]}}

    def _expected_spec(self, record, gpu, attempt=1):
        batch = record["batch"]
        return LocalPrivateGpuLaunchSpec(
            batch_id=batch["batch_id"], batch_manifest=Path(batch["path"]),
            expected_batch_sha256=batch["sha256"],
            runtime_admission=Path(gpu["runtime_admission"]),
            expected_runtime_admission_sha256=gpu["runtime_admission_sha256"],
            production_profile=Path(gpu["production_profile"]),
            expected_production_profile_sha256=gpu["production_profile_sha256"],
            root_registration=Path(gpu["root_registration"]),
            expected_root_registration_sha256=gpu["root_registration_sha256"],
            launcher_profile=Path(gpu["launcher_profile"]),
            expected_launcher_profile_sha256=gpu["launcher_profile_sha256"],
            local_readiness=Path(gpu["local_readiness"]),
            expected_local_readiness_sha256=gpu["local_readiness_sha256"],
            local_launcher=Path(gpu["local_launcher"]),
            writable_result_root=Path(gpu["result_root"]),
            writable_event_root=Path(gpu["event_root"]),
            writable_lock_root=Path(gpu["lock_root"]),
            working_directory=Path(gpu["working_directory"]), attempt_ordinal=attempt)

    def test_no_successor_preserves_old_spec_without_reading_batch(self):
        record = self._batch()
        Path(record["batch"]["path"]).unlink()
        with (mock.patch.object(self.successor, "load_successor", return_value=None),
              mock.patch.object(self.successor, "effective_gpu") as effective,
              mock.patch.object(self.backend, "_checkpoint_small_json") as reader):
            actual = self.backend._gpu_launch_spec(record, 2)
        self.assertEqual(self._expected_spec(record, self.old, 2).identity_sha256, actual.identity_sha256)
        reader.assert_not_called()
        effective.assert_not_called()

    def test_successor_keeps_historical_batch_spec_identity_exact(self):
        record = self._batch()
        before = copy.deepcopy(record)
        with (mock.patch.object(self.successor, "load_successor", return_value={"sealed": True}),
              mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective) as effective):
            actual = self.backend._gpu_launch_spec(record, 3)
        self.assertEqual(self._expected_spec(record, self.old, 3).identity_sha256, actual.identity_sha256)
        effective.assert_called_once_with(self.config, batch_runtime=self._runtime(self.old))
        self.assertEqual(before, record)

    def test_successor_new_batch_uses_new_controls_and_preserves_output_roots(self):
        record = self._batch(self._runtime(self.new))
        with (mock.patch.object(self.successor, "load_successor", return_value={"sealed": True}),
              mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective)):
            actual = self.backend._gpu_launch_spec(record, 1)
        self.assertEqual(self._expected_spec(record, self.new).identity_document(), actual.identity_document())
        self.assertNotEqual(self._expected_spec(record, self.old).identity_sha256, actual.identity_sha256)

    def test_successor_refuses_wrong_batch_digest_id_or_missing_runtime(self):
        for change in ("digest", "id", "missing", "not-object"):
            with self.subTest(change=change):
                record = self._batch()
                manifest = {"batch_id": record["batch"]["batch_id"],
                            "runtime_admission": self._runtime(self.old)}
                if change == "id":
                    manifest["batch_id"] = "gpuasrbatch2_" + "b" * 32
                elif change == "missing":
                    del manifest["runtime_admission"]
                elif change == "not-object":
                    manifest["runtime_admission"] = []
                body = canonical_bytes(manifest)
                if change != "digest":
                    record["batch"]["sha256"] = sha256_bytes(body)
                else:
                    record["batch"]["sha256"] = "f" * 64
                with (mock.patch.object(self.successor, "load_successor", return_value={}),
                      mock.patch.object(self.successor, "effective_gpu") as effective,
                      mock.patch.object(self.backend, "_checkpoint_small_json", return_value=(manifest, body))):
                    with self.assertRaisesRegex(BackendError, "exact batch binding"):
                        self.backend._gpu_launch_spec(record, 1)
                effective.assert_not_called()

    def test_unknown_batch_runtime_is_closed_backend_error(self):
        record = self._batch({"receipt_path": "/other/runtime.json", "receipt_sha256": "f" * 64})
        with (mock.patch.object(self.successor, "load_successor", return_value={}),
              mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective)):
            with self.assertRaisesRegex(BackendError, "GPU runtime selection failed"):
                self.backend._gpu_launch_spec(record, 1)

    def test_successor_read_error_does_not_fall_back_to_legacy(self):
        with mock.patch.object(self.successor, "load_successor", side_effect=ValueError("changed successor")):
            with self.assertRaisesRegex(BackendError, "GPU runtime selection failed"):
                self.backend._gpu_launch_spec(self._batch(), 1)

    def test_retired_runtime_spec_is_replayable_but_cannot_launch(self):
        record = self._batch()
        old_spec = self._expected_spec(record, self.old)
        with mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective):
            with self.assertRaisesRegex(BackendError, "retired runtime"):
                self.backend._assert_gpu_launch_runtime_current(old_spec)
            self.backend._assert_gpu_launch_runtime_current(self._expected_spec(record, self.new))

    def test_current_runtime_requires_both_exact_path_and_digest(self):
        spec = self._expected_spec(self._batch(self._runtime(self.new)), self.new)
        with mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective):
            for changed in (replace(spec, runtime_admission=Path(self.old["runtime_admission"])),
                            replace(spec, expected_runtime_admission_sha256=self.old["runtime_admission_sha256"])):
                with self.subTest(changed=changed.runtime_admission):
                    with self.assertRaisesRegex(BackendError, "retired runtime"):
                        self.backend._assert_gpu_launch_runtime_current(changed)

    def test_supervisor_never_invokes_child_for_retired_runtime(self):
        record = self._batch()
        self.backend._gpu_records[record["batch_key"]] = record
        self.backend._gpu_status[record["batch_key"]] = "pending"
        executor = SimpleNamespace(context=SimpleNamespace(outer_unit="fixture.service",
                                  outer_invocation_id="a" * 32), launch=mock.Mock())
        with (mock.patch.object(self.successor, "load_successor", return_value={}),
              mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective),
              mock.patch.object(self.backend, "_get_gpu_executor", return_value=executor),
              mock.patch.object(self.backend, "_load_gpu_child_records", return_value={}) as journal,
              mock.patch.object(self.backend, "_refresh_gpu_status", return_value=1),
              mock.patch.object(self.backend, "_acquire_gpu_opportunity", return_value=True)):
            with self.assertRaisesRegex(BackendError, "retired runtime"):
                self.backend._supervise_gpu_child()
        executor.launch.assert_not_called()
        journal.assert_any_call(executor, refresh=True)

    def test_materialization_successor_error_does_not_fall_back(self):
        with mock.patch.object(self.successor, "effective_gpu", side_effect=ValueError("invalid successor")):
            with self.assertRaisesRegex(BackendError, "GPU runtime successor replay failed"):
                self.backend._gpu_materialization_controls()

    def _materializer(self):
        packed = {"batch_id": "gpuasrbatch2_" + "b" * 32}
        def materialize(**kwargs):
            ordinal = int(Path(kwargs["queue_manifest_path"]).stem.split("-")[-1])
            receipt = {
                "receipt_id": "materialization-" + str(ordinal),
                "selection": {"queue_ordinals": kwargs["queue_ordinals"]},
                "batch": {"path": str(self.root / "batch.json"), "sha256": "a" * 64,
                          "batch_id": "gpuasrbatch2_" + "a" * 32},
                "work_orders": [{"queue_ordinal": selected, "preprocess_ordinal": selected,
                    "path": str(self.root / f"order-{ordinal}-{selected}.json"),
                    "member_id": f"member-{ordinal}-{selected}",
                    "work_order_id": f"order-{ordinal}-{selected}", "sha256": "c" * 64,
                    "identity_sha256": "d" * 64} for selected in kwargs["queue_ordinals"]],
            }
            return receipt, self.root / f"receipt-{ordinal}.json", "created"
        bridge = SimpleNamespace(canonical_bytes=canonical_bytes,
            materialize=mock.Mock(side_effect=materialize),
            BATCH_V2=SimpleNamespace(canonical_bytes=canonical_bytes,
                materialize_batch=mock.Mock(return_value=(packed, self.root / "packed.json"))))
        self.backend.modules = SimpleNamespace(gpu_queue=SimpleNamespace(canonical_bytes=canonical_bytes), gpu_bridge=bridge)
        return bridge

    def _queue(self, ordinal):
        return {"queue_id": "queue-" + str(ordinal), "totals": {
            "member_count": 1, "ready_count": 1, "requires_chunking_count": 0,
            "explicit_skip_count": 0, "ready_audio_duration_ms": 1000,
            "requires_chunking_audio_duration_ms": 0}}

    def _assert_new_materialization(self, bridge, count):
        self.assertEqual(count, bridge.materialize.call_count)
        for call in bridge.materialize.call_args_list:
            self.assertEqual(Path(self.new["runtime_admission"]), call.kwargs["runtime_admission_path"])
            self.assertEqual(self.new["runtime_admission_sha256"], call.kwargs["expected_runtime_admission_sha256"])
            self.assertEqual(Path(self.old["result_root"]), call.kwargs["result_root"])

    def test_future_legacy_materialization_uses_successor(self):
        bridge = self._materializer()
        with mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective):
            result = self.backend._gpu_record(bundle_id="bundle-1", queue=self._queue(1),
                queue_path=self.root / "queue-1.json", ordinals=[1])
        self.assertEqual("ready_batch", result["record_kind"])
        self._assert_new_materialization(bridge, 1)

    def test_empty_queue_does_not_require_or_materialize_a_successor(self):
        bridge = self._materializer()
        with mock.patch.object(self.successor, "effective_gpu") as effective:
            result = self.backend._gpu_record(bundle_id="bundle-1", queue=self._queue(1),
                queue_path=self.root / "queue-1.json", ordinals=[])
        self.assertEqual("no_ready_members", result["record_kind"])
        bridge.materialize.assert_not_called()
        effective.assert_not_called()

    def test_future_packed_materialization_uses_successor_for_every_queue(self):
        bridge = self._materializer()
        selected = []
        for ordinal in (1, 2):
            queue = self._queue(ordinal)
            selected.append({"campaign_ordinal": ordinal, "bundle_id": f"bundle-{ordinal}",
                "queue_path": self.root / f"queue-{ordinal}.json",
                "queue": {"queue_id": queue["queue_id"], "sha256": sha256_bytes(canonical_bytes(queue)),
                          "path": str(self.root / f"queue-{ordinal}.json")},
                "queue_disposition": queue["totals"], "member": {"ordinal": 1}})
        with mock.patch.object(self.successor, "effective_gpu", side_effect=self._effective):
            result = self.backend._gpu_packed_record(selected)
        self.assertEqual(2, len(result["sources"]))
        self._assert_new_materialization(bridge, 2)
        bridge.BATCH_V2.materialize_batch.assert_called_once()

    def test_historical_wrapper_installed_only_for_production_module_bundle(self):
        with mock.patch.object(self.successor, "install_historical_replay") as install:
            SealedArchiveBackend(self.config, modules=SimpleNamespace(gpu_bridge=SimpleNamespace(ASR_V5=object())))
        install.assert_not_called()
        modules = backend_module._Modules(**{
            name: ModuleType("fixture_" + name)
            for name in backend_module._Modules.__dataclass_fields__})
        modules.gpu_bridge.ASR_V5 = object()
        with (mock.patch.object(self.successor, "install_historical_replay") as install,
              mock.patch.object(SealedArchiveBackend, "_configure_replay_store", return_value=None)):
            SealedArchiveBackend(self.config, modules=modules)
        install.assert_called_once_with(modules.gpu_bridge.ASR_V5, self.config)


if __name__ == "__main__":
    unittest.main()
