from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

from autonomous_controller import acquisition_retry_proof as proof
from autonomous_controller.sealed_backend import BackendError, SealedArchiveBackend


def digest(body):
    return hashlib.sha256(body).hexdigest()


class AcquisitionRetryProofTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw"
        self.raw.mkdir(mode=0o700)
        self.job_id = "acq-" + "1" * 32 + "-000001"
        self.order = {
            "schema_version": 1, "adapter": "direct_http", "job_id": self.job_id,
            "source": {"access_state": "public"},
            "adapter_config": {"url": "https://example.test/source.wav", "expected_byte_count": 1234},
            "output": {"root": str(self.raw)},
        }
        self.entry = {"queue_ordinal": 1, "job_id": self.job_id, "adapter": "direct_http",
                      "path": "work-orders/000001.json", "sha256": digest(proof.pretty_bytes(self.order)),
                      "byte_count": len(proof.pretty_bytes(self.order))}
        self.manifest_path = self.root / "manifest.json"
        self.manifest = {"bundle_id": "acqbundle_" + "2" * 32,
                         "policy": {"media_output_root": str(self.raw)}, "work_orders": [self.entry]}
        manifest_body = self.write(self.manifest_path, self.manifest)
        self.bundle = {"path": self.manifest_path, "body": manifest_body,
                       "manifest": self.manifest, "orders": [self.order]}
        self.binding = {"bundle_id": self.manifest["bundle_id"], "manifest_sha256": digest(manifest_body),
                        "ordinal": 1, "job_id": self.job_id,
                        "work_order_sha256": digest(proof.canonical_bytes(self.order))}
        old = {"schema_version": 1, "receipt_kind": "public_acquisition_quarantine",
               "failure_attempt_limit": 3, **self.binding,
               "attempt_receipts": [{"attempt_number": i, "receipt_sha256": str(i) * 64}
                                    for i in range(1, 4)]}
        self.quarantine = {**old, "receipt_sha256": digest(proof.canonical_bytes(old))}
        self.old_path = self.raw / "original-quarantine.json"
        self.old_bytes = self.write(self.old_path, self.quarantine)
        self.result_path = (self.raw / "jobs" / self.job_id
                            / self.binding["work_order_sha256"] / "result.json")
        result = {"result_path": str(self.result_path), "job_id": self.job_id,
                  "status": "completed", "dry_run": False,
                  "work_order_sha256": self.binding["work_order_sha256"],
                  "admission": {"sha256": "3" * 64, "byte_count": 1234},
                  "completed_at": "2026-09-12T16:00:00Z"}
        self.state = {"result": result, "result_sha256": digest(proof.pretty_bytes(result)),
                      "media_sha256": "3" * 64, "byte_count": 1234}
        self.schedule_path = self.root / "schedule.json"
        self.schedule = {"schedule_id": "bgacqsched_" + "4" * 32,
                         "queue": {"manifest_path": str(self.manifest_path),
                                   "manifest_sha256": digest(manifest_body),
                                   "bundle_id": self.manifest["bundle_id"]},
                         "consumer": {"preprocess_state_root": str(self.root / "preprocess")}}
        schedule_body = self.write(self.schedule_path, self.schedule)
        self.reference = {"path": str(self.schedule_path), "sha256": digest(schedule_body),
                          "schedule_id": self.schedule["schedule_id"], "role": "normal_processing"}
        self.config_path = self.root / "controller.json"
        self.config = {"config_id": "himrautocfg_" + "5" * 32,
                       "campaign": {"schedules": [self.reference]}}
        config_body = self.write(self.config_path, self.config)
        self.plan_path = self.root / "plan.json"
        self.plan = {
            "schema_version": 1, "kind": proof.PLAN_KIND,
            "config_path": str(self.config_path), "config_sha256": digest(config_body),
            "config_id": self.config["config_id"], "created_at": "2026-09-12T15:00:00Z",
            "entries": [{**self.binding, "quarantine_receipt_sha256": self.quarantine["receipt_sha256"],
                         "manifest_path": str(self.manifest_path), "result_path": str(self.result_path),
                         "expected_bytes": 1234, "url": self.order["adapter_config"]["url"]}],
            "limits": {"max_attempts_per_item": 3, "retry_backoff_seconds": 120, "max_run_seconds": 86400},
        }
        self.reseal_plan()

    @staticmethod
    def write(path, value):
        body = proof.pretty_bytes(value)
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(body)
        path.chmod(0o400)
        return body

    def reseal_plan(self):
        self.plan.pop("plan_sha256", None)
        self.plan["plan_sha256"] = digest(proof.canonical_bytes(self.plan))
        self.plan_sha = digest(self.write(self.plan_path, self.plan))

    def seal(self):
        return proof.seal_completion(self.bundle, self.entry, self.order, self.quarantine,
                                     self.plan_path, self.plan_sha, self.state)

    def verify(self):
        return proof.verify_completion(self.bundle, self.entry, self.order, self.quarantine, self.state)

    def snapshot(self):
        return {str(path.relative_to(self.root)): (path.stat().st_mode, path.read_bytes())
                for path in self.root.rglob("*") if path.is_file()}

    def test_serialization_matches_legacy(self):
        self.assertEqual(proof.canonical_bytes({"b": 2, "a": "é"}), '{"a":"é","b":2}'.encode())
        self.assertTrue(proof.pretty_bytes({"a": 1}).endswith(b"\n"))

    def test_validate_plan_is_metadata_only_and_read_only(self):
        before = self.snapshot()
        self.assertEqual(proof.validate_plan(self.plan_path, self.plan_sha), self.plan)
        self.assertEqual(before, self.snapshot())
        self.assertFalse(self.result_path.exists())

    def test_seal_verify_and_idempotent_replay_preserve_history(self):
        document = self.seal()
        path = proof.completion_path(self.bundle, self.entry, self.order)
        before = path.stat()
        self.assertEqual(document, self.verify())
        self.assertEqual(document, self.seal())
        self.assertEqual((before.st_ino, before.st_mtime_ns), (path.stat().st_ino, path.stat().st_mtime_ns))
        self.assertEqual(path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(path.stat().st_nlink, 1)
        self.assertEqual(self.old_path.read_bytes(), self.old_bytes)
        self.assertFalse(self.result_path.exists())

    def test_missing_proof_never_creates_state(self):
        before = self.snapshot()
        with self.assertRaises(proof.RecoveryProofError):
            self.verify()
        self.assertEqual(before, self.snapshot())
        self.assertFalse((self.raw / proof.DIRECTORY).exists())

    def test_bad_physical_plan_hash_is_rejected(self):
        with self.assertRaises(proof.RecoveryProofError):
            proof.validate_plan(self.plan_path, "0" * 64)

    def test_plan_strict_fields_types_and_identity(self):
        original = copy.deepcopy(self.plan)
        cases = [lambda p: p.update(extra=1), lambda p: p.update(schema_version=True),
                 lambda p: p["limits"].update(max_attempts_per_item=True),
                 lambda p: p["limits"].update(max_attempts_per_item=4),
                 lambda p: p["limits"].update(max_run_seconds=86401),
                 lambda p: p["entries"].append(copy.deepcopy(p["entries"][0])),
                 lambda p: p["entries"][0].update(ordinal=True),
                 lambda p: p["entries"][0].update(url="https://name:secret@example.test/audio"),
                 lambda p: p["entries"][0].update(manifest_path="/tmp/../manifest.json")]
        for change in cases:
            with self.subTest(change=change):
                self.plan = copy.deepcopy(original)
                change(self.plan)
                self.reseal_plan()
                with self.assertRaises(proof.RecoveryProofError):
                    proof.validate_plan(self.plan_path, self.plan_sha)

    def test_plan_internal_hash_is_verified(self):
        self.plan["plan_sha256"] = "0" * 64
        physical = digest(self.write(self.plan_path, self.plan))
        with self.assertRaises(proof.RecoveryProofError):
            proof.validate_plan(self.plan_path, physical)

    def test_plan_must_enroll_manifest_in_original_campaign(self):
        self.plan["entries"][0]["manifest_sha256"] = "0" * 64
        self.reseal_plan()
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()
        self.assertFalse((self.raw / proof.DIRECTORY).exists())

    def test_plan_config_physical_pin_and_id_are_checked(self):
        self.config["config_id"] = "himrautocfg_" + "9" * 32
        self.write(self.config_path, self.config)
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()
        self.plan["config_sha256"] = digest(self.config_path.read_bytes())
        self.reseal_plan()
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()

    def test_completed_result_hash_and_media_binding_are_required(self):
        for field in ("result_sha256", "media_sha256", "byte_count"):
            with self.subTest(field=field):
                original = self.state[field]
                self.state[field] = 1235 if field == "byte_count" else "0" * 64
                with self.assertRaises(proof.RecoveryProofError):
                    self.seal()
                self.state[field] = original

    def test_wrong_expected_bytes_or_url_cannot_authorize_order(self):
        for key, value in (("expected_bytes", 999), ("url", "https://example.test/other.wav")):
            with self.subTest(key=key):
                old = self.plan["entries"][0][key]
                self.plan["entries"][0][key] = value
                self.reseal_plan()
                with self.assertRaises(proof.RecoveryProofError):
                    self.seal()
                self.plan["entries"][0][key] = old

    def test_quarantine_identity_is_not_replaceable(self):
        self.quarantine["ordinal"] = 2
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()

    def test_work_order_must_match_its_physical_manifest_pin(self):
        self.order["adapter_config"]["url"] = "https://example.test/changed.wav"
        with self.assertRaisesRegex(proof.RecoveryProofError, "work-order bytes"):
            self.seal()

    def test_proof_tamper_and_boolean_ordinal_rejected(self):
        document = self.seal()
        path = proof.completion_path(self.bundle, self.entry, self.order)
        document["ordinal"] = True
        document["receipt_sha256"] = digest(proof.canonical_bytes(
            {k: v for k, v in document.items() if k != "receipt_sha256"}))
        self.write(path, document)
        with self.assertRaises(proof.RecoveryProofError):
            self.verify()

    def test_proof_rejects_noncanonical_and_duplicate_json(self):
        self.seal()
        path = proof.completion_path(self.bundle, self.entry, self.order)
        for body in (b'{"a":1,"a":2}', b'{"a":NaN}', b'[]'):
            path.chmod(0o600)
            path.write_bytes(body)
            path.chmod(0o400)
            with self.assertRaises(proof.RecoveryProofError):
                self.verify()

    def test_plan_modes_hardlinks_and_symlinks_rejected(self):
        self.plan_path.chmod(0o600)
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()
        self.plan_path.chmod(0o400)
        link = self.root / "plan-hardlink.json"
        os.link(self.plan_path, link)
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()
        link.unlink()
        link.symlink_to(self.plan_path)
        with self.assertRaises(proof.RecoveryProofError):
            proof.validate_plan(link, self.plan_sha)
        directory_link = self.root / "linked-root"
        directory_link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(proof.RecoveryProofError):
            proof.validate_plan(directory_link / "plan.json", self.plan_sha)

    def test_nonprivate_proof_parent_rejected(self):
        self.seal()
        path = proof.completion_path(self.bundle, self.entry, self.order)
        path.parent.chmod(0o755)
        with self.assertRaises(proof.RecoveryProofError):
            self.verify()

    def test_owned_group_writable_ancestor_is_compatible_but_leaf_stays_private(self):
        private = self.root / "private"
        private.mkdir(mode=0o700)
        copy = private / "plan.json"
        copy.write_bytes(self.plan_path.read_bytes())
        copy.chmod(0o400)
        self.root.chmod(0o775)
        try:
            # Isolate the path-reader check: the original fixture config lives
            # directly under root, whose mode is intentionally not private now.
            self.assertEqual(proof._read_bytes(copy, proof.MAX_PLAN_BYTES), copy.read_bytes())
            private.chmod(0o775)
            with self.assertRaises(proof.RecoveryProofError):
                proof._read_bytes(copy, proof.MAX_PLAN_BYTES)
            private.chmod(0o700)
            self.root.chmod(0o777)
            with self.assertRaises(proof.RecoveryProofError):
                proof._read_bytes(copy, proof.MAX_PLAN_BYTES)
        finally:
            self.root.chmod(0o700)

    def test_fifo_leaf_is_rejected_without_waiting_for_a_writer(self):
        path = self.root / "fifo"
        os.mkfifo(path, 0o400)
        with self.assertRaises(proof.RecoveryProofError):
            proof._read_bytes(path, 1024)

    def test_existing_proof_is_never_overwritten(self):
        document = self.seal()
        path = proof.completion_path(self.bundle, self.entry, self.order)
        document["completed_at"] = "2026-09-12T17:00:00Z"
        self.write(path, document)
        before = path.read_bytes()
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(path.stat().st_nlink, 1)

    def test_symlink_proof_target_cannot_be_overwritten(self):
        self.seal()
        path = proof.completion_path(self.bundle, self.entry, self.order)
        path.unlink()
        path.symlink_to(self.old_path)
        with self.assertRaises(proof.RecoveryProofError):
            self.seal()
        self.assertTrue(path.is_symlink())
        self.assertEqual(self.old_path.read_bytes(), self.old_bytes)

    def test_failed_write_does_not_publish_partial_proof(self):
        with mock.patch.object(proof.os, "write", side_effect=OSError("fixture write failure")):
            with self.assertRaises(proof.RecoveryProofError):
                self.seal()
        path = proof.completion_path(self.bundle, self.entry, self.order)
        self.assertFalse(path.exists())
        self.assertEqual(list(path.parent.iterdir()), [])

    def fake_runner(self):
        runner = ModuleType("retry_fixture_queue")
        acquire = ModuleType("retry_fixture_acquire")
        runner.__file__ = str(self.root / "fixture-runner.py")
        acquire.__file__ = str(self.root / "fixture-acquire.py")
        runner.IMPLEMENTATION_VERSION = "0.2.0"
        acquire.IMPLEMENTATION_VERSION = "0.3.2"
        runner.acquire = acquire
        runner.QueueRunnerError = RuntimeError
        source = '''def _result_action(*, entry, order, state, failure_state, action, adapter_invoked):
    return {"status": "completed" if state is not None else "quarantined",
            "failure_attempt_count": len(failure_state["attempts"]),
            "quarantine_receipt_sha256": None if failure_state["quarantine"] is None else failure_state["quarantine"]["receipt_sha256"]}
'''
        Path(runner.__file__).write_text(source)
        Path(runner.__file__).chmod(0o600)
        Path(acquire.__file__).write_text("# source-bound fixture\n")
        Path(acquire.__file__).chmod(0o600)
        exec(compile(source, runner.__file__, "exec"), runner.__dict__)
        for name, path in (("LEGACY_RUNNER_SHA256", runner.__file__), ("LEGACY_ACQUIRE_SHA256", acquire.__file__)):
            patcher = mock.patch.object(proof, name, digest(Path(path).read_bytes()))
            patcher.start()
            self.addCleanup(patcher.stop)
        return runner

    def action(self, runner, state=None, quarantine=None):
        return runner._result_action(entry=self.entry, order=self.order, state=state,
                                     failure_state={"quarantine": quarantine, "attempts": [1, 2, 3]},
                                     action="validated_reuse", adapter_invoked=False)

    def test_adapter_preserves_failure_history_and_only_changes_active_projection(self):
        self.seal()
        runner = self.fake_runner()
        self.assertTrue(proof.install_adapter(runner))
        self.assertTrue(proof.install_adapter(runner))
        row = self.action(runner, self.state, self.quarantine)
        self.assertEqual(row["status"], "completed")
        self.assertEqual(row["failure_attempt_count"], 3)
        self.assertIsNone(row["quarantine_receipt_sha256"])
        self.assertEqual(self.old_path.read_bytes(), self.old_bytes)

    def test_adapter_missing_proof_is_fatal_not_quarantine_reset(self):
        runner = self.fake_runner()
        proof.install_adapter(runner)
        with self.assertRaises(RuntimeError):
            self.action(runner, self.state, self.quarantine)
        with mock.patch.object(proof, "_verify_projected", side_effect=AssertionError("unexpected proof read")):
            self.assertEqual(self.action(runner, None, self.quarantine)["quarantine_receipt_sha256"],
                             self.quarantine["receipt_sha256"])
            self.assertIsNone(self.action(runner, self.state, None)["quarantine_receipt_sha256"])

    def test_adapter_source_change_or_replacement_is_rejected(self):
        runner = self.fake_runner()
        Path(runner.__file__).write_text("changed source\n")
        with self.assertRaises(proof.RecoveryProofError):
            proof.install_adapter(runner)
        runner = self.fake_runner()
        proof.install_adapter(runner)
        runner._result_action = lambda **kwargs: {}
        with self.assertRaises(proof.RecoveryProofError):
            proof.install_adapter(runner)

    def backend(self):
        backend = SealedArchiveBackend.__new__(SealedArchiveBackend)
        backend._trust_completed_copy = False
        backend._queue_replay_binding = mock.Mock(return_value=object())
        backend._operational_replay_store = SimpleNamespace(hydrate_restart_checkpoint=mock.Mock(
            return_value={"states": [self.state], "telemetry": {"targeted_revalidated_items": 1}}))
        backend.modules = SimpleNamespace(
            queue_runner=SimpleNamespace(_load_bundle=lambda path: self.bundle,
                                         _scan_failure_states=lambda bundle: [{"quarantine": self.quarantine,
                                                                                "attempts": [1, 2, 3]}]),
            background=SimpleNamespace(load_schedule=lambda path: (self.schedule, self.schedule_path,
                                                                   self.schedule_path.read_bytes()),
                                       _acknowledged_results=lambda *a, **k: set(),
                                       _ready_snapshot=lambda *a: {"ready_item_count": 1}))
        return backend

    def test_incremental_restore_requires_proof_and_counts_original_once(self):
        backend = self.backend()
        with self.assertRaises(BackendError):
            backend._incremental_schedule_runtime(self.reference, object())
        self.seal()
        _schedule, _bundle, states, ready, telemetry = backend._incremental_schedule_runtime(self.reference, object())
        self.assertEqual(len(states), 1)
        self.assertEqual(states[0]["queue_state"], "completed")
        self.assertEqual(states[0]["byte_count"], 1234)
        self.assertEqual(ready["ready_item_count"], 1)
        self.assertEqual(telemetry["targeted_revalidated_items"], 1)


if __name__ == "__main__":
    unittest.main()
