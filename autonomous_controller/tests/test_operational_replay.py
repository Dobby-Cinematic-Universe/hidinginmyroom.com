from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
import threading
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from autonomous_controller import operational_replay as replay


class FakeQueueError(RuntimeError):
    pass


class QueueFixture:
    def __init__(self, root: Path, name: str, payloads: list[bytes | None]):
        self.root = root / name
        self.root.mkdir(mode=0o700)
        (self.root / "jobs").mkdir(mode=0o700)
        self.module = types.ModuleType(f"fake_queue_runner_{name}_{id(self)}")
        self.payload_hash_calls = 0
        self.payload_hash_bytes = 0
        self.after_exact_hook = None
        self.orders: list[dict] = []
        self.entries: list[dict] = []
        for ordinal, payload in enumerate(payloads, 1):
            job_id = f"job-{name}-{ordinal:06d}"
            order = {
                "job_id": job_id,
                "output": {"root": str(self.root)},
                "result_path": "filled-after-hash",
                "ordinal": ordinal,
            }
            identity = self.sha256_bytes(self.canonical_bytes(order))
            order["result_path"] = str(
                self.root / "jobs" / job_id / identity / "result.json"
            )
            # result_path is part of this compact fixture's canonical work order;
            # recompute once after assigning it.
            identity = self.sha256_bytes(self.canonical_bytes(order))
            order["result_path"] = str(
                self.root / "jobs" / job_id / identity / "result.json"
            )
            self.orders.append(order)
            self.entries.append(
                {
                    "queue_ordinal": ordinal,
                    "job_id": job_id,
                    "sha256": f"{ordinal:064x}",
                }
            )
        self.body = replay.canonical_bytes(
            {"fixture": name, "work_order_count": len(self.orders)}
        )
        self.manifest_path = self.root / "control" / "manifest.json"
        self.manifest_path.parent.mkdir(mode=0o700)
        self.manifest_path.write_bytes(self.body)
        self.manifest_path.chmod(0o400)
        self.bundle_id = f"bundle-{name}"
        self.bundle = {
            "path": self.manifest_path,
            "body": self.body,
            "manifest": {
                "bundle_id": self.bundle_id,
                "work_orders": self.entries,
            },
            "orders": self.orders,
        }
        self.binding = replay.QueueBinding(
            schedule_id=f"schedule-{name}",
            role="normal_processing",
            schedule_sha256="a" * 64,
            manifest_path=self.manifest_path,
            manifest_sha256=self.sha256_bytes(self.body),
            bundle_id=self.bundle_id,
        )
        self._install_module_functions()
        for ordinal, payload in enumerate(payloads, 1):
            if payload is not None:
                self.publish(ordinal, payload)

    @staticmethod
    def canonical_bytes(value):
        return replay.canonical_bytes(value)

    @staticmethod
    def sha256_bytes(value):
        return hashlib.sha256(value).hexdigest()

    def result_path(self, order: dict) -> Path:
        return Path(order["result_path"])

    def _safe_existing_result_parents(self, result_path: Path, output_root: Path):
        current = output_root
        for component in result_path.relative_to(output_root).parts[:-1]:
            current = current / component
            if not current.exists() and not current.is_symlink():
                return
            observed = current.lstat()
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                raise FakeQueueError("completed-result path component is unsafe")

    def _inspect_result(self, order: dict):
        path = self.result_path(order)
        self._safe_existing_result_parents(path, self.root)
        try:
            observed = path.lstat()
        except FileNotFoundError:
            if path.parent.exists() or path.parent.is_symlink():
                raise FakeQueueError(
                    f"result directory exists without result.json for {order['job_id']}"
                )
            return None
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) & 0o022
        ):
            raise FakeQueueError("completed result has unsafe metadata")
        if set(entry.name for entry in path.parent.iterdir()) != {"result.json"}:
            raise FakeQueueError("completed result directory has missing or extra entries")
        body = path.read_bytes()
        value = json.loads(body)
        if body != self.canonical_bytes(value):
            raise FakeQueueError("completed result is not canonical")
        payload = Path(value["admission"]["path"])
        payload_body = payload.read_bytes()
        for _ in range(2):
            self.payload_hash_calls += 1
            self.payload_hash_bytes += len(payload_body)
            if self.sha256_bytes(payload_body) != value["admission"]["sha256"]:
                raise FakeQueueError("completed payload hash mismatch")
        identity = self.sha256_bytes(self.canonical_bytes(order))
        if (
            value["work_order_sha256"] != identity
            or value["result_path"] != str(path)
            or value["admission"]["byte_count"] != len(payload_body)
        ):
            raise FakeQueueError("completed result binding mismatch")
        returned = {
            "result": value,
            "result_sha256": self.sha256_bytes(body),
            "media_sha256": value["admission"]["sha256"],
            "byte_count": len(payload_body),
        }
        if self.after_exact_hook is not None:
            self.after_exact_hook(order, returned)
        return returned

    def _scan_results(self, bundle: dict):
        return [self.module._inspect_result(order) for order in bundle["orders"]]

    def _install_module_functions(self):
        self.module.canonical_bytes = self.canonical_bytes
        self.module.sha256_bytes = self.sha256_bytes
        self.module._result_path = self.result_path
        self.module._safe_existing_result_parents = self._safe_existing_result_parents
        self.module._inspect_result = self._inspect_result
        self.module._scan_results = self._scan_results
        self.module.QueueRunnerError = FakeQueueError
        self.module.acquire = SimpleNamespace(
            pretty_json=lambda value: self.canonical_bytes(value).decode("utf-8")
        )

    def publish(self, ordinal: int, payload_body: bytes) -> dict:
        order = self.orders[ordinal - 1]
        digest = self.sha256_bytes(payload_body)
        payload = self.root / "media" / "sha256" / digest[:2] / digest / "payload"
        payload.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if not payload.exists():
            payload.write_bytes(payload_body)
            payload.chmod(0o600)
        result_path = self.result_path(order)
        result_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        result = {
            "work_order_sha256": self.sha256_bytes(self.canonical_bytes(order)),
            "result_path": str(result_path),
            "admission": {
                "path": str(payload),
                "sha256": digest,
                "byte_count": len(payload_body),
            },
        }
        result_path.write_bytes(self.canonical_bytes(result))
        result_path.chmod(0o600)
        return result


class OperationalReplayTests(unittest.TestCase):
    def setUp(self):
        test_root = Path(__file__).resolve().parents[2] / "pipeline/.test-work"
        test_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        test_root.chmod(0o700)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="operational-replay-", dir=test_root
        )
        self.root = Path(self.temporary.name).resolve()

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def deep_seed(fixture: QueueFixture, store: replay.OperationalReplayStore):
        with store.deep_capture(fixture.binding) as session:
            first = fixture.module._scan_results(fixture.bundle)
            second = fixture.module._scan_results(fixture.bundle)
        return session, first, second

    def test_deep_capture_preserves_two_passes_then_operational_reads_zero_payload_bytes(self):
        payloads = [b"one" * 1_000, b"two" * 2_000]
        fixture = QueueFixture(self.root, "reuse", payloads)
        router = replay.install_queue_replay_router(fixture.module)
        self.assertIs(router, replay.install_queue_replay_router(fixture.module))
        store = replay.OperationalReplayStore(router)

        session, first, second = self.deep_seed(fixture, store)
        total = sum(map(len, payloads))
        self.assertEqual(first, second)
        self.assertEqual(4 * total, fixture.payload_hash_bytes)
        self.assertEqual("deep", session.delta["mode"])

        before_calls = fixture.payload_hash_calls
        before_bytes = fixture.payload_hash_bytes
        with store.operational_session(fixture.binding) as operational:
            self.assertEqual(first, fixture.module._scan_results(fixture.bundle))
            self.assertEqual(first, fixture.module._scan_results(fixture.bundle))
        self.assertEqual(before_calls, fixture.payload_hash_calls)
        self.assertEqual(before_bytes, fixture.payload_hash_bytes)
        self.assertEqual(4 * total, operational.delta["avoided_logical_payload_bytes"])
        self.assertEqual(4, operational.delta["fast_reused_items"])

    def test_direct_new_result_is_exact_once_and_reused_by_final_scan(self):
        fixture = QueueFixture(self.root, "new", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        payload = b"new-payload" * 1_000

        with store.operational_session(fixture.binding) as session:
            self.assertEqual([None], fixture.module._scan_results(fixture.bundle))
            fixture.publish(1, payload)
            exact = fixture.module._inspect_result(fixture.orders[0])
            self.assertIsNotNone(exact)
            calls_after_exact = fixture.payload_hash_calls
            self.assertEqual([exact], fixture.module._scan_results(fixture.bundle))
        self.assertEqual(2, calls_after_exact)
        self.assertEqual(calls_after_exact, fixture.payload_hash_calls)
        self.assertEqual([1], session.delta["new_exact_ordinals"])
        self.assertEqual([], session.delta["targeted_revalidated_ordinals"])

    def test_metadata_drift_revalidates_only_changed_ordinal(self):
        fixture = QueueFixture(self.root, "drift", [b"a" * 100, b"b" * 200])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        baseline = fixture.payload_hash_calls
        first_payload = Path(
            fixture._inspect_result(fixture.orders[0])["result"]["admission"]["path"]
        )
        baseline = fixture.payload_hash_calls
        os.utime(first_payload, None)

        with store.operational_session(fixture.binding) as session:
            fixture.module._scan_results(fixture.bundle)
            after_revalidate = fixture.payload_hash_calls
            fixture.module._scan_results(fixture.bundle)
        self.assertEqual(baseline + 2, after_revalidate)
        self.assertEqual(after_revalidate, fixture.payload_hash_calls)
        self.assertEqual([1], session.delta["targeted_revalidated_ordinals"])
        with store.operational_session(fixture.binding):
            fixture.module._scan_results(fixture.bundle)
        self.assertEqual(after_revalidate, fixture.payload_hash_calls)

    def test_unrelated_shared_ancestor_change_refreshes_without_payload_hash(self):
        fixture = QueueFixture(self.root, "ancestor-refresh", [b"historical"])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        before_calls = fixture.payload_hash_calls
        before_bytes = fixture.payload_hash_bytes

        unrelated = fixture.root / "media" / "sha256" / "ff" / ("f" * 64)
        unrelated.mkdir(parents=True, mode=0o700)
        with store.operational_session(fixture.binding) as refreshed:
            fixture.module._scan_results(fixture.bundle)
        self.assertEqual(before_calls, fixture.payload_hash_calls)
        self.assertEqual(before_bytes, fixture.payload_hash_bytes)
        self.assertEqual(1, refreshed.delta["fast_reused_items"])
        self.assertEqual([], refreshed.delta["targeted_revalidated_ordinals"])

        with store.operational_session(fixture.binding):
            fixture.module._scan_results(fixture.bundle)
        self.assertEqual(before_calls, fixture.payload_hash_calls)

    def test_new_result_hashes_only_new_payload_after_shared_ancestor_changes(self):
        historical = b"historical-payload" * 100
        added = b"new-payload" * 250
        fixture = QueueFixture(self.root, "new-sibling", [historical, None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        _deep, first, _second = self.deep_seed(fixture, store)
        historical_state = first[0]
        before_calls = fixture.payload_hash_calls
        before_bytes = fixture.payload_hash_bytes

        fixture.publish(2, added)
        with store.operational_session(fixture.binding) as session:
            exact = fixture.module._inspect_result(fixture.orders[1])
            self.assertEqual(
                [historical_state, exact],
                fixture.module._scan_results(fixture.bundle),
            )
        # The fixture models the source's two pinned payload hashes per exact
        # inspection. The historical sibling never contributes another byte.
        self.assertEqual(before_calls + 2, fixture.payload_hash_calls)
        self.assertEqual(
            before_bytes + 2 * len(added), fixture.payload_hash_bytes
        )
        self.assertEqual([2], session.delta["new_exact_ordinals"])

    def test_noop_replay_keeps_generation_but_witness_refresh_advances_it(self):
        fixture = QueueFixture(self.root, "generation", [b"payload"])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        seeded = store.snapshot_summary(fixture.binding)

        with store.operational_session(fixture.binding) as unchanged:
            fixture.module._scan_results(fixture.bundle)
        self.assertEqual(seeded["generation"], unchanged.delta["generation"])

        state = fixture._inspect_result(fixture.orders[0])
        payload = Path(state["result"]["admission"]["path"])
        os.utime(payload, None)
        with store.operational_session(fixture.binding) as refreshed:
            fixture.module._scan_results(fixture.bundle)
        self.assertEqual(seeded["generation"] + 1, refreshed.delta["generation"])

        with store.operational_session(fixture.binding) as stable_again:
            fixture.module._scan_results(fixture.bundle)
        self.assertEqual(refreshed.delta["generation"], stable_again.delta["generation"])

    def test_snapshot_admission_linearizes_against_concurrent_commit(self):
        fixture = QueueFixture(self.root, "guarded-admission", [b"payload"])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        seeded, _first, _second = self.deep_seed(fixture, store)
        authority = seeded.delta
        state = fixture._inspect_result(fixture.orders[0])
        os.utime(Path(state["result"]["admission"]["path"]), None)

        admission_entered = threading.Event()
        commit_attempted = threading.Event()
        commit_finished = threading.Event()
        ordering: list[str] = []
        cache: dict[str, str] = {}

        def commit_new_generation() -> None:
            admission_entered.wait()
            commit_attempted.set()
            with store.operational_session(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
            ordering.append("commit")
            commit_finished.set()

        worker = threading.Thread(target=commit_new_generation)
        worker.start()

        def admit() -> None:
            # The store invokes this only after its equal-generation check. Start
            # a competing commit in the old post-check/pre-cache-write gap and
            # prove it cannot finish until the cache mutation has linearized.
            admission_entered.set()
            self.assertTrue(commit_attempted.wait(timeout=5))
            self.assertFalse(commit_finished.wait(timeout=0.05))
            cache["runtime"] = "admitted"
            ordering.append("admit")

        admitted = store.admit_if_snapshot_current(
            fixture.binding,
            generation=authority["generation"],
            state_digest=authority["state_digest"],
            completed_count=authority["completed_count"],
            pending_count=authority["pending_count"],
            admit=admit,
        )
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertTrue(admitted)
        self.assertEqual({"runtime": "admitted"}, cache)
        self.assertEqual(["admit", "commit"], ordering)
        self.assertGreater(
            store.snapshot_summary(fixture.binding)["generation"],
            authority["generation"],
        )

    def test_operational_bundle_shape_error_requests_deep_audit(self):
        fixture = QueueFixture(self.root, "bundle-drift", [b"payload"])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        changed = {
            **fixture.bundle,
            "manifest": {
                **fixture.bundle["manifest"],
                "work_orders": [
                    {**fixture.entries[0], "sha256": "not-a-digest"}
                ],
            },
        }
        with self.assertRaises(replay.DeepAuditRequired):
            with store.operational_session(fixture.binding):
                fixture.module._scan_results(changed)

    def test_deep_capture_rejects_same_size_mutation_after_exact_hash(self):
        original = b"verified-payload"
        replacement = b"tampered-payload"
        self.assertEqual(len(original), len(replacement))
        fixture = QueueFixture(self.root, "deep-toctou", [original])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        exact_calls = 0

        def mutate_after_second_hash(_order, state):
            nonlocal exact_calls
            exact_calls += 1
            if exact_calls == 2:
                payload = Path(state["result"]["admission"]["path"])
                observed = payload.stat()
                payload.write_bytes(replacement)
                os.utime(
                    payload,
                    ns=(observed.st_atime_ns, observed.st_mtime_ns),
                )

        fixture.after_exact_hook = mutate_after_second_hash
        with self.assertRaises((FakeQueueError, replay.OperationalReplayError)):
            with store.deep_capture(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
                fixture.module._scan_results(fixture.bundle)
        self.assertFalse(store.has_snapshot(fixture.binding))
        digest = hashlib.sha256(original).hexdigest()
        payload = Path(
            fixture.root
            / "media"
            / "sha256"
            / digest[:2]
            / digest
            / "payload"
        )
        self.assertEqual(replacement, payload.read_bytes())

    def test_direct_exact_admission_rejects_post_hash_mutation(self):
        original = b"direct-original"
        replacement = b"direct-tamper!!"
        self.assertEqual(len(original), len(replacement))
        fixture = QueueFixture(self.root, "direct-toctou", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        before = store.snapshot_summary(fixture.binding)
        fixture.publish(1, original)
        mutated = False

        def mutate_after_hash(_order, state):
            nonlocal mutated
            if not mutated:
                mutated = True
                payload = Path(state["result"]["admission"]["path"])
                observed = payload.stat()
                payload.write_bytes(replacement)
                os.utime(
                    payload,
                    ns=(observed.st_atime_ns, observed.st_mtime_ns),
                )

        fixture.after_exact_hook = mutate_after_hash
        with self.assertRaises((FakeQueueError, replay.OperationalReplayError)):
            with store.operational_session(fixture.binding):
                fixture.module._inspect_result(fixture.orders[0])
        self.assertEqual(before, store.snapshot_summary(fixture.binding))

    def test_deep_capture_rejects_valid_tree_swap_during_exact_hash(self):
        original = b"ancestry-valid"
        replacement = b"ancestry-bad!!"
        self.assertEqual(len(original), len(replacement))
        fixture = QueueFixture(self.root, "deep-ancestry", [original])
        media = fixture.root / "media"
        valid_media = fixture.root / "media-good"
        held_media = fixture.root / "media-held"
        media.rename(valid_media)
        shutil.copytree(valid_media, media)
        digest = hashlib.sha256(original).hexdigest()
        corrupt_payload = media / "sha256" / digest[:2] / digest / "payload"
        observed = corrupt_payload.stat()
        corrupt_payload.write_bytes(replacement)
        os.utime(
            corrupt_payload,
            ns=(observed.st_atime_ns, observed.st_mtime_ns),
        )
        source_inspect = fixture.module._inspect_result

        def swap_only_while_hashing(order):
            media.rename(held_media)
            valid_media.rename(media)
            try:
                return source_inspect(order)
            finally:
                media.rename(valid_media)
                held_media.rename(media)

        fixture.module._inspect_result = swap_only_while_hashing
        fixture.module._scan_results = lambda bundle: [
            fixture.module._inspect_result(order) for order in bundle["orders"]
        ]
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        with self.assertRaises(replay.OperationalReplayError):
            with store.deep_capture(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
                fixture.module._scan_results(fixture.bundle)
        self.assertFalse(store.has_snapshot(fixture.binding))
        self.assertEqual(replacement, corrupt_payload.read_bytes())

    def test_operational_revalidation_rejects_valid_tree_swap(self):
        original = b"operation-valid"
        replacement = b"operation-bad!!"
        self.assertEqual(len(original), len(replacement))
        fixture = QueueFixture(self.root, "operation-ancestry", [original])
        media = fixture.root / "media"
        valid_media = fixture.root / "media-good"
        held_media = fixture.root / "media-held"
        enabled = False
        source_inspect = fixture.module._inspect_result

        def conditional_swap(order):
            if not enabled:
                return source_inspect(order)
            media.rename(held_media)
            valid_media.rename(media)
            try:
                return source_inspect(order)
            finally:
                media.rename(valid_media)
                held_media.rename(media)

        fixture.module._inspect_result = conditional_swap
        fixture.module._scan_results = lambda bundle: [
            fixture.module._inspect_result(order) for order in bundle["orders"]
        ]
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        before = store.snapshot_summary(fixture.binding)

        media.rename(valid_media)
        shutil.copytree(valid_media, media)
        digest = hashlib.sha256(original).hexdigest()
        corrupt_payload = media / "sha256" / digest[:2] / digest / "payload"
        observed = corrupt_payload.stat()
        corrupt_payload.write_bytes(replacement)
        os.utime(
            corrupt_payload,
            ns=(observed.st_atime_ns, observed.st_mtime_ns),
        )
        enabled = True
        with self.assertRaises(replay.OperationalReplayError):
            with store.operational_session(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
        self.assertEqual(before, store.snapshot_summary(fixture.binding))
        self.assertEqual(replacement, corrupt_payload.read_bytes())

    def test_completed_result_disappearance_fails_closed_and_does_not_commit(self):
        fixture = QueueFixture(self.root, "missing", [b"payload"])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        before = store.snapshot_summary(fixture.binding)
        result_path = fixture.result_path(fixture.orders[0])
        result_path.unlink()
        result_path.parent.rmdir()
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "completed acquisition result 1 disappeared"
        ):
            with store.operational_session(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
        self.assertEqual(before, store.snapshot_summary(fixture.binding))

    def test_pending_partial_result_preserves_original_admission_error(self):
        fixture = QueueFixture(self.root, "partial", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        fixture.result_path(fixture.orders[0]).parent.mkdir(
            parents=True, mode=0o700
        )
        with (
            mock.patch("autonomous_controller.operational_replay.time.sleep") as pause,
            self.assertRaisesRegex(
                FakeQueueError, "result directory exists without result.json"
            ),
        ):
            with store.operational_session(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
        self.assertEqual(
            replay.ACQUISITION_RESULT_ADMISSION_ATTEMPTS - 1,
            pause.call_count,
        )

    def test_pending_partial_result_is_admitted_after_atomic_publication(self):
        fixture = QueueFixture(self.root, "partial-completes", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        fixture.result_path(fixture.orders[0]).parent.mkdir(
            parents=True, mode=0o700
        )
        published = False

        def finish_publication(_seconds):
            nonlocal published
            if not published:
                fixture.publish(1, b"newly-published")
                published = True

        with mock.patch(
            "autonomous_controller.operational_replay.time.sleep",
            side_effect=finish_publication,
        ) as pause:
            with store.operational_session(fixture.binding) as session:
                observed = fixture.module._scan_results(fixture.bundle)

        self.assertTrue(published)
        self.assertEqual(1, pause.call_count)
        self.assertIsNotNone(observed[0])
        self.assertEqual([1], session.delta["new_exact_ordinals"])

    def test_pending_to_completed_metadata_bracket_retries_only_exact_race(self):
        fixture = QueueFixture(self.root, "bracket-race", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        with store.operational_session(fixture.binding):
            with (
                mock.patch.object(
                    store,
                    "_coherent_exact_inspection_once",
                    side_effect=[
                        replay.OperationalReplayError(
                            "exact pending result changed before its metadata bracket closed"
                        ),
                        (None, None),
                    ],
                ) as inspect,
                mock.patch(
                    "autonomous_controller.operational_replay.time.sleep"
                ) as pause,
            ):
                self.assertEqual(
                    (None, None),
                    store._coherent_exact_inspection(fixture.orders[0]),
                )
        self.assertEqual(2, inspect.call_count)
        pause.assert_called_once_with(
            replay.ACQUISITION_RESULT_ADMISSION_RETRY_SECONDS
        )

        with store.operational_session(fixture.binding):
            with (
                mock.patch.object(
                    store,
                    "_coherent_exact_inspection_once",
                    side_effect=replay.OperationalReplayError(
                        "completed result failed immutable validation"
                    ),
                ) as inspect,
                mock.patch(
                    "autonomous_controller.operational_replay.time.sleep"
                ) as pause,
                self.assertRaisesRegex(
                    replay.OperationalReplayError,
                    "completed result failed immutable validation",
                ),
            ):
                store._coherent_exact_inspection(fixture.orders[0])
        inspect.assert_called_once()
        pause.assert_not_called()

    def test_publication_retry_rejects_lookalike_errors(self):
        fixture = QueueFixture(self.root, "publication-lookalikes", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        job_id = fixture.orders[0]["job_id"]
        cases = (
            (
                "wrong-job",
                FakeQueueError(
                    "result directory exists without result.json for a-different-job"
                ),
            ),
            (
                "wrong-exception-type",
                replay.OperationalReplayError(
                    f"result directory exists without result.json for {job_id}"
                ),
            ),
        )
        for label, error in cases:
            with self.subTest(label=label), store.operational_session(
                fixture.binding
            ):
                with (
                    mock.patch.object(
                        store,
                        "_coherent_exact_inspection_once",
                        side_effect=error,
                    ) as inspect,
                    mock.patch(
                        "autonomous_controller.operational_replay.time.sleep"
                    ) as pause,
                    self.assertRaises(type(error)),
                ):
                    store._coherent_exact_inspection(fixture.orders[0])
                inspect.assert_called_once()
                pause.assert_not_called()

    def test_thread_local_sessions_do_not_cross_queue_bindings(self):
        first = QueueFixture(self.root, "thread-a", [b"a"])
        second = QueueFixture(self.root, "thread-b", [b"bb"])
        # Both fixtures intentionally share one imported module object for the
        # concurrency test, as production schedules do.
        module = first.module
        second.module = module
        original_result_path = module._result_path
        original_safe = module._safe_existing_result_parents
        original_inspect = module._inspect_result

        def result_path(order):
            return Path(order["result_path"])

        def safe(result, root):
            return (
                first._safe_existing_result_parents(result, root)
                if root == first.root
                else second._safe_existing_result_parents(result, root)
            )

        def inspect(order):
            return (
                first._inspect_result(order)
                if order in first.orders
                else second._inspect_result(order)
            )

        module._result_path = result_path
        module._safe_existing_result_parents = safe
        module._inspect_result = inspect
        module._scan_results = lambda bundle: [
            module._inspect_result(order) for order in bundle["orders"]
        ]
        del original_result_path, original_safe, original_inspect
        store = replay.OperationalReplayStore.for_queue_runner(module)
        self.deep_seed(first, store)
        self.deep_seed(second, store)
        barrier = threading.Barrier(2)
        observed: dict[str, list] = {}
        errors: list[BaseException] = []

        def run(label, fixture):
            try:
                with store.operational_session(fixture.binding):
                    barrier.wait(timeout=2)
                    observed[label] = module._scan_results(fixture.bundle)
            except BaseException as error:
                errors.append(error)

        threads = [
            threading.Thread(target=run, args=("first", first)),
            threading.Thread(target=run, args=("second", second)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)
        self.assertEqual([], errors)
        self.assertEqual(1, len(observed["first"]))
        self.assertEqual(1, len(observed["second"]))
        self.assertNotEqual(
            observed["first"][0]["media_sha256"],
            observed["second"][0]["media_sha256"],
        )

    def test_stale_same_schedule_commit_merges_monotonic_completion(self):
        fixture = QueueFixture(self.root, "merge", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        stale = store.operational_session(fixture.binding)
        completing = store.operational_session(fixture.binding)
        fixture.publish(1, b"merged")
        with completing:
            fixture.module._inspect_result(fixture.orders[0])
        with stale:
            # A coherent older view finishing later must not regress completion.
            pass
        summary = store.snapshot_summary(fixture.binding)
        self.assertEqual(1, summary["completed_count"])
        self.assertEqual(0, summary["pending_count"])

    def test_concurrent_merge_keeps_returned_projection_distinct_from_authority(self):
        fixture = QueueFixture(self.root, "returned-merge", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        first_scanned = threading.Event()
        second_committed = threading.Event()
        observed: dict[str, object] = {}
        errors: list[BaseException] = []

        def return_older_projection() -> None:
            try:
                with store.operational_session(fixture.binding) as session:
                    observed["first_states"] = fixture.module._scan_results(
                        fixture.bundle
                    )
                    first_scanned.set()
                    if not second_committed.wait(timeout=5):
                        raise AssertionError("newer replay did not commit")
                observed["first_delta"] = session.delta
            except BaseException as error:
                errors.append(error)

        def commit_newer_projection() -> None:
            try:
                if not first_scanned.wait(timeout=5):
                    raise AssertionError("older replay did not scan")
                fixture.publish(1, b"concurrently-completed")
                with store.operational_session(fixture.binding) as session:
                    observed["second_states"] = fixture.module._scan_results(
                        fixture.bundle
                    )
                observed["second_delta"] = session.delta
            except BaseException as error:
                errors.append(error)
            finally:
                second_committed.set()

        threads = [
            threading.Thread(target=return_older_projection),
            threading.Thread(target=commit_newer_projection),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=6)
            self.assertFalse(thread.is_alive())
        self.assertEqual([], errors)

        first_delta = observed["first_delta"]
        second_delta = observed["second_delta"]
        self.assertEqual([None], observed["first_states"])
        self.assertIsNotNone(observed["second_states"][0])
        # Both sessions commit against the same final monotonic authority, but
        # only the second session actually returned the completed item.
        self.assertEqual(second_delta["generation"], first_delta["generation"])
        self.assertEqual(1, first_delta["completed_count"])
        self.assertEqual(0, first_delta["session_completed_count"])
        self.assertNotEqual(
            first_delta["session_state_digest"], first_delta["state_digest"]
        )
        self.assertEqual(
            second_delta["session_state_digest"], second_delta["state_digest"]
        )
        self.assertEqual(first_delta, store.verify_peer_delta(first_delta))
        # Returned-stale provenance cannot be reconstructed from the current
        # snapshot after its exact same-process digest ages out. The equal-current
        # peer delta can still use bounded snapshot fallback.
        store._accepted_delta_digests.clear()
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "conflicts with the shared snapshot"
        ):
            store.verify_peer_delta(first_delta)
        self.assertEqual(second_delta, store.verify_peer_delta(second_delta))

    def test_one_pass_deep_capture_and_aborted_session_publish_nothing(self):
        fixture = QueueFixture(self.root, "abort", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "at least two exact scan passes"
        ):
            with store.deep_capture(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
        self.assertFalse(store.has_snapshot(fixture.binding))
        with self.assertRaisesRegex(RuntimeError, "abort fixture"):
            with store.deep_capture(fixture.binding):
                fixture.module._scan_results(fixture.bundle)
                fixture.module._scan_results(fixture.bundle)
                raise RuntimeError("abort fixture")
        self.assertFalse(store.has_snapshot(fixture.binding))

    def test_binding_tamper_router_tamper_and_fork_are_rejected(self):
        fixture = QueueFixture(self.root, "tamper", [None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        changed = {**fixture.bundle, "body": b"changed\n"}
        with self.assertRaises(replay.DeepAuditRequired):
            with store.operational_session(fixture.binding):
                fixture.module._scan_results(changed)

        router = store.router
        fixture.module._scan_results = router.original_scan_results
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "replaced or tampered"
        ):
            store.operational_session(fixture.binding)
        fixture.module._scan_results = router.scan_wrapper
        with mock.patch.object(replay.os, "getpid", return_value=os.getpid() + 1):
            with self.assertRaisesRegex(replay.DeepAuditRequired, "process fork"):
                store.operational_session(fixture.binding)

    def test_peer_delta_is_idempotent_but_cannot_hydrate_fresh_store(self):
        fixture = QueueFixture(self.root, "peer", [b"payload"])
        router = replay.install_queue_replay_router(fixture.module)
        store = replay.OperationalReplayStore(router)
        session, _first, _second = self.deep_seed(fixture, store)
        delta = session.delta
        self.assertEqual(delta, store.verify_peer_delta(delta))
        self.assertEqual(delta, store.verify_peer_delta(delta))

        fresh = replay.OperationalReplayStore(router)
        with self.assertRaisesRegex(replay.DeepAuditRequired, "cannot hydrate"):
            fresh.verify_peer_delta(delta)
        forged = {**delta, "completed_count": delta["completed_count"] + 1}
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "delta digest is invalid"
        ):
            store.verify_peer_delta(forged)

        # A genuinely emitted equal-current delta remains recoverable after the
        # bounded accepted-digest cache evicts it.
        store._accepted_delta_digests.clear()
        self.assertEqual(delta, store.verify_peer_delta(delta))

        def recomputed(changes):
            changed = {**delta, **changes}
            core = {
                key: value
                for key, value in changed.items()
                if key != "delta_sha256"
            }
            changed["delta_sha256"] = hashlib.sha256(
                replay.canonical_bytes(core)
            ).hexdigest()
            return changed

        # The canonical SHA is deliberately unkeyed. Recomputing it cannot mint
        # provenance for session fields which now control stale-summary admission.
        for label, changed in {
            "session_digest": {"session_state_digest": "f" * 64},
            "session_counts": {
                "session_completed_count": 0,
                "session_pending_count": 1,
            },
        }.items():
            with self.subTest(label=label), self.assertRaisesRegex(
                replay.OperationalReplayError,
                "conflicts with the shared snapshot",
            ):
                store.verify_peer_delta(recomputed(changed))

    def test_checkpoint_is_digest_only_canonical_and_contains_no_stat_authority(self):
        fixture = QueueFixture(self.root, "checkpoint", [b"payload", None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        checkpoint = store.export_checkpoint(
            config_id="himrautocfg_" + "1" * 32,
            config_sha256="2" * 64,
            campaign_id="campaign-1",
            schedule_set_id="schedule-set-1",
            created_at="2026-08-30T03:00:00Z",
        )
        core = {
            key: value
            for key, value in checkpoint.items()
            if key != "identity_sha256"
        }
        self.assertEqual(
            hashlib.sha256(replay.canonical_bytes(core)).hexdigest(),
            checkpoint["identity_sha256"],
        )
        self.assertEqual(
            checkpoint,
            json.loads(replay.canonical_bytes(checkpoint)),
        )
        serialized = replay.canonical_bytes(checkpoint).decode("utf-8")
        for forbidden in (
            '"inode"',
            '"device"',
            '"mtime_ns"',
            '"ctime_ns"',
            '"link_count"',
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertFalse(checkpoint["policy"]["cross_process_witness_reuse"])
        self.assertFalse(checkpoint["policy"]["filesystem_witnesses_persisted"])
        self.assertEqual(1, checkpoint["totals"]["completed_count"])
        self.assertEqual(1, checkpoint["totals"]["pending_count"])

    def test_restart_checkpoint_hydrates_unchanged_without_payload_reads(self):
        payload = b"restart-payload" * 1_000
        fixture = QueueFixture(self.root, "restart-fast", [payload, None])
        seeded_store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        _deep, first, _second = self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "1" * 32,
            config_sha256="2" * 64,
            campaign_id="campaign-1",
            schedule_set_id="schedule-set-1",
            created_at="2026-08-31T12:00:00Z",
        )
        self.assertEqual(replay.RESTART_CHECKPOINT_SCHEMA_VERSION, checkpoint["schema_version"])
        serialized = replay.canonical_bytes(checkpoint).decode("utf-8")
        self.assertIn('"inode"', serialized)
        self.assertIn('"result"', serialized)

        baseline_calls = fixture.payload_hash_calls
        baseline_bytes = fixture.payload_hash_bytes
        fresh = replay.OperationalReplayStore(seeded_store.router)
        prepared = fresh.prepare_restart_checkpoint(checkpoint)
        self.assertEqual(checkpoint["identity_sha256"], prepared.identity_sha256)
        self.assertEqual(1, prepared.schedule_count)
        with mock.patch.object(
            fresh,
            "_validated_restart_checkpoint",
            wraps=fresh._validated_restart_checkpoint,
        ) as parse_again:
            hydrated = fresh.hydrate_restart_checkpoint(
                fixture.binding, fixture.bundle, prepared
            )
        parse_again.assert_not_called()
        self.assertEqual(first, hydrated["states"])
        self.assertEqual(baseline_calls, fixture.payload_hash_calls)
        self.assertEqual(baseline_bytes, fixture.payload_hash_bytes)
        telemetry = hydrated["telemetry"]
        self.assertEqual("restart_checkpoint_hydration", telemetry["mode"])
        self.assertEqual(1, telemetry["fast_reused_items"])
        self.assertEqual([], telemetry["targeted_revalidated_ordinals"])
        self.assertEqual(4 * len(payload), telemetry["avoided_logical_payload_bytes"])
        self.assertEqual(
            4 * len(payload), telemetry["legacy_deep_logical_payload_bytes"]
        )
        self.assertEqual(
            0, telemetry["targeted_revalidated_logical_payload_bytes"]
        )
        self.assertGreater(telemetry["result_envelope_bytes_read"], 0)

        with fresh.operational_session(fixture.binding):
            self.assertEqual(first, fixture.module._scan_results(fixture.bundle))
        self.assertEqual(baseline_calls, fixture.payload_hash_calls)

        other_store = replay.OperationalReplayStore(seeded_store.router)
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "another store or process"
        ):
            other_store.prepare_restart_checkpoint(prepared)

    def test_restart_hydration_cancellation_between_ordinals_installs_no_snapshot(
        self,
    ):
        fixture = QueueFixture(
            self.root, "restart-cancel-hydrate", [b"first", b"second"]
        )
        seeded_store = replay.OperationalReplayStore.for_queue_runner(
            fixture.module
        )
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "1" * 32,
            config_sha256="2" * 64,
            campaign_id="campaign-cancel-hydrate",
            schedule_set_id="schedule-set-cancel-hydrate",
            created_at="2026-08-31T12:00:10Z",
        )
        fresh = replay.OperationalReplayStore(seeded_store.router)
        prepared = fresh.prepare_restart_checkpoint(checkpoint)

        class StopAtBoundary(Exception):
            pass

        calls = 0

        def cancellation_boundary() -> None:
            nonlocal calls
            calls += 1
            # One prepared-document boundary plus two complete persisted-witness
            # units precede the first complete live ordinal inspection.
            if calls == 4:
                raise StopAtBoundary

        with self.assertRaises(StopAtBoundary):
            fresh.hydrate_restart_checkpoint(
                fixture.binding,
                fixture.bundle,
                prepared,
                cancellation_boundary=cancellation_boundary,
            )
        self.assertEqual(4, calls)
        self.assertFalse(fresh.has_snapshot(fixture.binding))

    def test_restart_document_parse_cancellation_observes_order_boundary(self):
        fixture = QueueFixture(
            self.root, "restart-cancel-parse", [b"first", b"second"]
        )
        seeded_store = replay.OperationalReplayStore.for_queue_runner(
            fixture.module
        )
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "3" * 32,
            config_sha256="4" * 64,
            campaign_id="campaign-cancel-parse",
            schedule_set_id="schedule-set-cancel-parse",
            created_at="2026-08-31T12:00:20Z",
        )
        fresh = replay.OperationalReplayStore(seeded_store.router)

        class StopAtBoundary(Exception):
            pass

        calls = 0

        def cancellation_boundary() -> None:
            nonlocal calls
            calls += 1
            # Whole-document encoding and identity validation are complete
            # units; the third boundary follows the first decoded order row.
            if calls == 3:
                raise StopAtBoundary

        with self.assertRaises(StopAtBoundary):
            fresh.prepare_restart_checkpoint(
                checkpoint,
                cancellation_boundary=cancellation_boundary,
            )
        self.assertEqual(3, calls)
        self.assertFalse(fresh.has_snapshot(fixture.binding))

    def test_restart_bootstrap_cancellation_between_ordinals_installs_no_snapshot(
        self,
    ):
        fixture = QueueFixture(
            self.root, "restart-cancel-bootstrap", [b"first", b"second"]
        )
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)

        class StopAtBoundary(Exception):
            pass

        calls = 0

        def cancellation_boundary() -> None:
            nonlocal calls
            calls += 1
            raise StopAtBoundary

        with self.assertRaises(StopAtBoundary):
            store.bootstrap_restart_snapshot(
                fixture.binding,
                fixture.bundle,
                cancellation_boundary=cancellation_boundary,
            )
        self.assertEqual(1, calls)
        self.assertFalse(store.has_snapshot(fixture.binding))

    def test_restart_hydration_ignores_only_runtime_device_numbers(self):
        fixture = QueueFixture(self.root, "restart-device", [b"payload"])
        seeded_store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "f" * 32,
            config_sha256="1" * 64,
            campaign_id="campaign-device",
            schedule_set_id="schedule-set-device",
            created_at="2026-08-31T12:00:30Z",
        )
        witness = checkpoint["schedules"][0]["orders"][0]["witness"]
        witness["result_file"]["device"] += 10_000
        witness["result_parent"]["device"] += 10_000
        witness["payload_file"]["device"] += 10_000
        for ancestor in (
            witness["result_ancestors"] + witness["payload_ancestors"]
        ):
            ancestor["fingerprint"]["device"] += 10_000
        core = {
            key: value
            for key, value in checkpoint.items()
            if key != "identity_sha256"
        }
        checkpoint["identity_sha256"] = hashlib.sha256(
            replay.canonical_bytes(core)
        ).hexdigest()
        baseline_calls = fixture.payload_hash_calls
        fresh = replay.OperationalReplayStore(seeded_store.router)
        hydrated = fresh.hydrate_restart_checkpoint(
            fixture.binding, fixture.bundle, checkpoint
        )
        self.assertEqual(baseline_calls, fixture.payload_hash_calls)
        self.assertEqual(1, hydrated["telemetry"]["fast_reused_items"])

    def test_restart_checkpoint_revalidates_only_metadata_changed_ordinal(self):
        payloads = [b"first" * 1_000, b"second" * 1_000]
        fixture = QueueFixture(self.root, "restart-target", payloads)
        seeded_store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "3" * 32,
            config_sha256="4" * 64,
            campaign_id="campaign-target",
            schedule_set_id="schedule-set-target",
            created_at="2026-08-31T12:01:00Z",
        )
        first_state = fixture._inspect_result(fixture.orders[0])
        first_payload = Path(first_state["result"]["admission"]["path"])
        observed = first_payload.stat()
        os.utime(
            first_payload,
            ns=(observed.st_atime_ns, observed.st_mtime_ns + 1),
        )
        baseline_calls = fixture.payload_hash_calls
        baseline_bytes = fixture.payload_hash_bytes

        fresh = replay.OperationalReplayStore(seeded_store.router)
        hydrated = fresh.hydrate_restart_checkpoint(
            fixture.binding, fixture.bundle, checkpoint
        )
        self.assertEqual(baseline_calls + 2, fixture.payload_hash_calls)
        self.assertEqual(
            baseline_bytes + 2 * len(payloads[0]), fixture.payload_hash_bytes
        )
        telemetry = hydrated["telemetry"]
        self.assertEqual([1], telemetry["targeted_revalidated_ordinals"])
        self.assertEqual([2], telemetry["fast_reused_ordinals"])
        self.assertEqual(1, telemetry["targeted_revalidated_items"])
        self.assertEqual(1, telemetry["fast_reused_items"])
        self.assertEqual(
            checkpoint["schedules"][0]["generation"] + 1,
            telemetry["generation"],
        )

    def _copied_restart_fixture(self, name):
        fixture = QueueFixture(self.root, name, [b"previously-completed", None])
        seeded = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, seeded)
        checkpoint = seeded.export_restart_checkpoint(
            config_id="himrautocfg_" + "5" * 32,
            config_sha256="6" * 64,
            campaign_id="campaign-copy",
            schedule_set_id="schedule-set-copy",
            created_at="2026-09-06T23:00:00Z",
        )
        original = self.root / (name + "-original")
        fixture.root.rename(original)
        shutil.copytree(original, fixture.root)
        return fixture, replay.OperationalReplayStore(seeded.router), checkpoint

    def test_trusted_copy_rebinds_completed_but_hashes_new_crash_window_result(self):
        fixture, fresh, checkpoint = self._copied_restart_fixture("trusted-copy")
        new_payload = b"interrupted-crash-window"
        fixture.publish(2, new_payload)
        baseline_bytes = fixture.payload_hash_bytes
        hydrated = fresh.hydrate_restart_checkpoint(
            fixture.binding, fixture.bundle, checkpoint, trust_completed_copy=True
        )
        telemetry = hydrated["telemetry"]
        self.assertEqual([1], telemetry["trusted_copy_ordinals"])
        self.assertEqual([2], telemetry["targeted_revalidated_ordinals"])
        self.assertEqual([2], telemetry["new_exact_ordinals"])
        self.assertEqual(baseline_bytes + 2 * len(new_payload), fixture.payload_hash_bytes)
        self.assertEqual(checkpoint["schedules"][0]["generation"] + 1, telemetry["generation"])
        refreshed = fresh.export_restart_checkpoint(
            config_id=checkpoint["config_id"], config_sha256=checkpoint["config_sha256"],
            campaign_id=checkpoint["campaign_id"], schedule_set_id=checkpoint["schedule_set_id"],
            created_at="2026-09-06T23:01:00Z",
        )
        ordinary = replay.OperationalReplayStore(fresh.router)
        baseline_bytes = fixture.payload_hash_bytes
        second = ordinary.hydrate_restart_checkpoint(fixture.binding, fixture.bundle, refreshed)
        self.assertEqual(2, second["telemetry"]["fast_reused_items"])
        self.assertEqual(0, second["telemetry"]["trusted_copy_items"])
        self.assertEqual(baseline_bytes, fixture.payload_hash_bytes)

    def test_trusted_copy_rejects_metadata_or_envelope_conflicts_without_hash_fallback(self):
        for change in ("size", "permissions", "envelope", "hardlink", "missing"):
            with self.subTest(change=change):
                fixture, fresh, checkpoint = self._copied_restart_fixture("copy-" + change)
                saved = checkpoint["schedules"][0]["orders"][0]["state"]
                payload = Path(saved["result"]["admission"]["path"])
                result = fixture.result_path(fixture.orders[0])
                if change == "size":
                    payload.write_bytes(b"short")
                elif change == "permissions":
                    payload.chmod(0o666)
                elif change == "envelope":
                    result.write_bytes(b"{}\n")
                elif change == "hardlink":
                    os.link(payload, self.root / "extra-link")
                else:
                    result.unlink()
                baseline_bytes = fixture.payload_hash_bytes
                with self.assertRaises(replay.OperationalReplayError):
                    fresh.hydrate_restart_checkpoint(
                        fixture.binding, fixture.bundle, checkpoint, trust_completed_copy=True
                    )
                self.assertEqual(baseline_bytes, fixture.payload_hash_bytes)
                self.assertFalse(fresh.has_snapshot(fixture.binding))

    def test_trusted_copy_still_rejects_checkpoint_tampering(self):
        fixture, fresh, checkpoint = self._copied_restart_fixture("copy-tamper")
        checkpoint["identity_sha256"] = "0" * 64
        with self.assertRaisesRegex(replay.OperationalReplayError, "identity"):
            fresh.hydrate_restart_checkpoint(
                fixture.binding, fixture.bundle, checkpoint, trust_completed_copy=True
            )
        self.assertFalse(fresh.has_snapshot(fixture.binding))

    def test_restart_checkpoint_exactly_admits_new_pending_result(self):
        fixture = QueueFixture(self.root, "restart-new", [None])
        seeded_store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "5" * 32,
            config_sha256="6" * 64,
            campaign_id="campaign-new",
            schedule_set_id="schedule-set-new",
            created_at="2026-08-31T12:02:00Z",
        )
        payload = b"arrived-after-checkpoint" * 100
        fixture.publish(1, payload)
        baseline_calls = fixture.payload_hash_calls

        fresh = replay.OperationalReplayStore(seeded_store.router)
        hydrated = fresh.hydrate_restart_checkpoint(
            fixture.binding, fixture.bundle, checkpoint
        )
        self.assertEqual(baseline_calls + 2, fixture.payload_hash_calls)
        self.assertIsNotNone(hydrated["states"][0])
        self.assertEqual([1], hydrated["telemetry"]["new_exact_ordinals"])
        self.assertEqual([1], hydrated["telemetry"]["targeted_revalidated_ordinals"])

    def test_restart_checkpoint_completed_removal_fails_atomically(self):
        fixture = QueueFixture(self.root, "restart-removed", [b"one", b"two"])
        seeded_store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "7" * 32,
            config_sha256="8" * 64,
            campaign_id="campaign-removed",
            schedule_set_id="schedule-set-removed",
            created_at="2026-08-31T12:03:00Z",
        )
        result_path = fixture.result_path(fixture.orders[1])
        result_path.unlink()
        result_path.parent.rmdir()
        fresh = replay.OperationalReplayStore(seeded_store.router)
        with self.assertRaisesRegex(
            replay.OperationalReplayError,
            "completed acquisition result 2 disappeared",
        ):
            fresh.hydrate_restart_checkpoint(
                fixture.binding, fixture.bundle, checkpoint
            )
        self.assertFalse(fresh.has_snapshot(fixture.binding))

    def test_restart_checkpoint_payload_conflict_fails_atomically(self):
        original = b"restart-original"
        replacement = b"restart-tampered"
        self.assertEqual(len(original), len(replacement))
        fixture = QueueFixture(self.root, "restart-conflict", [original])
        seeded_store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "9" * 32,
            config_sha256="a" * 64,
            campaign_id="campaign-conflict",
            schedule_set_id="schedule-set-conflict",
            created_at="2026-08-31T12:04:00Z",
        )
        state = fixture._inspect_result(fixture.orders[0])
        payload = Path(state["result"]["admission"]["path"])
        observed = payload.stat()
        payload.write_bytes(replacement)
        os.utime(payload, ns=(observed.st_atime_ns, observed.st_mtime_ns))

        fresh = replay.OperationalReplayStore(seeded_store.router)
        with self.assertRaises(FakeQueueError):
            fresh.hydrate_restart_checkpoint(
                fixture.binding, fixture.bundle, checkpoint
            )
        self.assertFalse(fresh.has_snapshot(fixture.binding))

    def test_metadata_bootstrap_is_two_pass_stable_and_reads_no_payload(self):
        payload = b"legacy-payload" * 10_000
        fixture = QueueFixture(self.root, "restart-bootstrap", [payload, None])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.assertEqual(0, fixture.payload_hash_calls)
        bootstrapped = store.bootstrap_restart_snapshot(
            fixture.binding, fixture.bundle
        )
        self.assertEqual(0, fixture.payload_hash_calls)
        self.assertEqual(0, fixture.payload_hash_bytes)
        telemetry = bootstrapped["telemetry"]
        self.assertEqual("metadata_bootstrap", telemetry["mode"])
        self.assertEqual(1, telemetry["metadata_bootstrap_items"])
        self.assertEqual([1], telemetry["metadata_bootstrap_ordinals"])
        self.assertEqual(0, telemetry["payload_bytes_read"])
        self.assertGreater(telemetry["result_envelope_bytes_read"], 0)
        self.assertEqual(4 * len(payload), telemetry["avoided_logical_payload_bytes"])

        checkpoint = store.export_restart_checkpoint(
            config_id="himrautocfg_" + "b" * 32,
            config_sha256="c" * 64,
            campaign_id="campaign-bootstrap",
            schedule_set_id="schedule-set-bootstrap",
            created_at="2026-08-31T12:05:00Z",
        )
        fresh = replay.OperationalReplayStore(store.router)
        hydrated = fresh.hydrate_restart_checkpoint(
            fixture.binding, fixture.bundle, checkpoint
        )
        self.assertEqual(0, fixture.payload_hash_calls)
        self.assertEqual(1, hydrated["telemetry"]["fast_reused_items"])

    def test_restart_export_rejects_root_generation_ahead_of_shared_store(self):
        fixture = QueueFixture(self.root, "restart-boundary", [b"payload"])
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        summary = store.snapshot_summary(fixture.binding)
        expected = {
            fixture.binding.schedule_id: {
                "generation": summary["generation"],
                "state_digest": summary["state_digest"],
            }
        }
        document = store.export_restart_checkpoint(
            config_id="himrautocfg_" + "b" * 32,
            config_sha256="c" * 64,
            campaign_id="campaign-boundary",
            schedule_set_id="schedule-set-boundary",
            created_at="2026-08-31T12:05:30Z",
            expected_snapshots=expected,
        )
        self.assertEqual(1, document["totals"]["schedule_count"])
        ahead = json.loads(json.dumps(expected))
        ahead[fixture.binding.schedule_id]["generation"] += 1
        with self.assertRaisesRegex(
            replay.CheckpointDeferred, "ahead of the journal-observed root"
        ):
            store.export_restart_checkpoint(
                config_id="himrautocfg_" + "b" * 32,
                config_sha256="c" * 64,
                campaign_id="campaign-boundary",
                schedule_set_id="schedule-set-boundary",
                created_at="2026-08-31T12:05:31Z",
                expected_snapshots=ahead,
            )

    def test_restart_export_accepts_witness_only_generation_advance(self):
        fixture = QueueFixture(
            self.root, "restart-witness-boundary", [b"payload"]
        )
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        before = store.snapshot_summary(fixture.binding)
        expected = {
            fixture.binding.schedule_id: {
                "generation": before["generation"],
                "state_digest": before["state_digest"],
            }
        }

        state = fixture._inspect_result(fixture.orders[0])
        payload = Path(state["result"]["admission"]["path"])
        metadata = payload.stat()
        os.utime(
            payload,
            ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000),
        )
        with store.operational_session(fixture.binding):
            fixture.module._scan_results(fixture.bundle)
        after = store.snapshot_summary(fixture.binding)
        self.assertGreater(after["generation"], before["generation"])
        self.assertEqual(after["state_digest"], before["state_digest"])

        checkpoint = store.export_restart_checkpoint(
            config_id="himrautocfg_" + "b" * 32,
            config_sha256="c" * 64,
            campaign_id="campaign-witness-boundary",
            schedule_set_id="schedule-set-witness-boundary",
            created_at="2026-08-31T12:05:32Z",
            expected_snapshots=expected,
        )
        self.assertEqual(
            after["generation"], checkpoint["schedules"][0]["generation"]
        )
        self.assertEqual(
            after["state_digest"], checkpoint["schedules"][0]["state_digest"]
        )

    def test_restart_export_still_defers_on_logical_state_advance(self):
        fixture = QueueFixture(
            self.root, "restart-logical-boundary", [b"first", None]
        )
        store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, store)
        before = store.snapshot_summary(fixture.binding)
        expected = {
            fixture.binding.schedule_id: {
                "generation": before["generation"],
                "state_digest": before["state_digest"],
            }
        }

        fixture.publish(2, b"second")
        with store.operational_session(fixture.binding):
            fixture.module._scan_results(fixture.bundle)
        after = store.snapshot_summary(fixture.binding)
        self.assertGreater(after["generation"], before["generation"])
        self.assertNotEqual(after["state_digest"], before["state_digest"])

        with self.assertRaisesRegex(
            replay.CheckpointDeferred, "ahead of the journal-observed root"
        ):
            store.export_restart_checkpoint(
                config_id="himrautocfg_" + "b" * 32,
                config_sha256="c" * 64,
                campaign_id="campaign-logical-boundary",
                schedule_set_id="schedule-set-logical-boundary",
                created_at="2026-08-31T12:05:33Z",
                expected_snapshots=expected,
            )

    def test_restart_checkpoint_tamper_and_repeated_install_fail_closed(self):
        fixture = QueueFixture(self.root, "restart-tamper", [b"payload"])
        seeded_store = replay.OperationalReplayStore.for_queue_runner(fixture.module)
        self.deep_seed(fixture, seeded_store)
        checkpoint = seeded_store.export_restart_checkpoint(
            config_id="himrautocfg_" + "d" * 32,
            config_sha256="e" * 64,
            campaign_id="campaign-tamper",
            schedule_set_id="schedule-set-tamper",
            created_at="2026-08-31T12:06:00Z",
        )
        forged = json.loads(replay.canonical_bytes(checkpoint))
        forged["totals"]["completed_count"] = 0
        fresh = replay.OperationalReplayStore(seeded_store.router)
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "identity is invalid"
        ):
            fresh.hydrate_restart_checkpoint(
                fixture.binding, fixture.bundle, forged
            )
        self.assertFalse(fresh.has_snapshot(fixture.binding))

        fresh.hydrate_restart_checkpoint(fixture.binding, fixture.bundle, checkpoint)
        with self.assertRaisesRegex(
            replay.OperationalReplayError, "already has replay authority"
        ):
            fresh.hydrate_restart_checkpoint(
                fixture.binding, fixture.bundle, checkpoint
            )


if __name__ == "__main__":
    unittest.main()
