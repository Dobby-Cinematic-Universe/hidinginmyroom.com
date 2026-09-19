from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from autonomous_controller.config import (
    ControllerConfig,
    canonical_bytes,
    sha256_bytes,
)
from autonomous_controller.state import (
    CHECKPOINT_KEYS,
    MUTABLE_TEMP_DIRECTORY,
    ControlStore,
    StateError,
    read_control_state,
    request_start,
)


class CheckpointStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.state_root = self.root / "state"
        self.state_root.mkdir(mode=0o700)
        self.config = ControllerConfig(
            document={
                "config_id": "himrautocfg_" + "1" * 32,
                "state_root": str(self.state_root),
            },
            path=self.root / "controller.json",
            physical_sha256="2" * 64,
        )
        self.store = ControlStore(self.config)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def append(self, event_type: str = "test_event") -> dict[str, object]:
        return self.store.append_event(
            event_type,
            {"value": event_type},
            occurred_at="2026-08-29T12:00:00Z",
        )

    def rewrite_checkpoint(
        self,
        document: dict[str, object],
        *,
        recompute_digest: bool = True,
    ) -> None:
        if recompute_digest:
            core = {
                key: document[key]
                for key in CHECKPOINT_KEYS - {"checkpoint_sha256"}
            }
            document["checkpoint_sha256"] = sha256_bytes(canonical_bytes(core))
        self.store.checkpoint_path.write_bytes(canonical_bytes(document))
        self.store.checkpoint_path.chmod(0o600)

    def test_absent_checkpoint_preserves_full_legacy_replay(self) -> None:
        first = self.append("first")
        second = self.append("second")

        self.assertIsNone(self.store.read_checkpoint())
        view = self.store.recovery_view()

        self.assertIsNone(view.checkpoint)
        self.assertEqual((first, second), view.tail_events)
        self.assertEqual(0, view.anchor_sequence)
        self.assertTrue(view.legacy_full_replay)
        self.assertFalse(self.store.checkpoint_path.exists())

    def test_run_lock_exits_normally_and_can_be_reacquired(self) -> None:
        with self.store.run_lock():
            pass
        with self.store.run_lock():
            pass

    def test_round_trip_returns_only_events_after_the_exact_anchor(self) -> None:
        first = self.append("first")
        checkpoint = self.store.write_checkpoint(
            {"campaign": {"cursor": 17}},
            created_at="2026-08-29T12:00:01Z",
        )
        second = self.append("second")

        self.assertEqual(0o600, self.store.checkpoint_path.stat().st_mode & 0o777)
        self.assertEqual(first["event_sha256"], checkpoint["anchor"]["event_sha256"])
        self.assertEqual(self.config.config_id, checkpoint["config_id"])
        self.assertEqual(self.config.physical_sha256, checkpoint["config_sha256"])
        self.assertEqual(checkpoint, self.store.read_checkpoint())
        self.assertEqual(2, len(tuple(self.store.events_root.iterdir())))

        reopened = ControlStore(self.config)
        view = reopened.recovery_view()
        self.assertEqual(checkpoint, view.checkpoint)
        self.assertEqual((second,), view.tail_events)
        self.assertEqual(1, view.anchor_sequence)
        self.assertFalse(view.legacy_full_replay)

        # Lightweight start/stop state remains usable after checkpoint creation.
        self.assertEqual("running", request_start(self.config)["desired_state"])

    def test_write_requires_an_event_anchor_and_canonical_inputs(self) -> None:
        with self.assertRaisesRegex(StateError, "event-journal anchor"):
            self.store.write_checkpoint({})
        self.append()
        with self.assertRaisesRegex(StateError, "backend state"):
            self.store.write_checkpoint([])  # type: ignore[arg-type]
        with self.assertRaisesRegex(StateError, "canonical UTC"):
            self.store.write_checkpoint({}, created_at="2026-8-29 12:00:00")
        with self.assertRaisesRegex(StateError, "include sequence and hash"):
            self.store.write_checkpoint({}, expected_anchor_sequence=1)
        with self.assertRaisesRegex(StateError, "include sequence and hash"):
            self.store.write_checkpoint({}, expected_anchor_sha256="0" * 64)
        with self.assertRaisesRegex(StateError, "expected anchor is invalid"):
            self.store.write_checkpoint(
                {},
                expected_anchor_sequence=True,  # type: ignore[arg-type]
                expected_anchor_sha256="0" * 64,
            )

    def test_write_accepts_an_exact_expected_journal_head(self) -> None:
        event = self.append("snapshot")

        checkpoint = self.store.write_checkpoint(
            {"cursor": 1},
            created_at="2026-08-29T12:00:01Z",
            expected_anchor_sequence=event["sequence"],
            expected_anchor_sha256=event["event_sha256"],
        )

        self.assertEqual(event["sequence"], checkpoint["anchor"]["sequence"])
        self.assertEqual(
            event["event_sha256"], checkpoint["anchor"]["event_sha256"]
        )

    def test_stale_expected_head_preserves_previous_checkpoint(self) -> None:
        first = self.append("first")
        original = self.store.write_checkpoint(
            {"cursor": 1},
            created_at="2026-08-29T12:00:01Z",
            expected_anchor_sequence=first["sequence"],
            expected_anchor_sha256=first["event_sha256"],
        )
        original_bytes = self.store.checkpoint_path.read_bytes()

        with self.assertRaisesRegex(StateError, "journal head changed"):
            self.store.write_checkpoint(
                {"cursor": 2},
                created_at="2026-08-29T12:00:02Z",
                expected_anchor_sequence=first["sequence"],
                expected_anchor_sha256="0" * 64,
            )
        self.assertEqual(original_bytes, self.store.checkpoint_path.read_bytes())

        second = self.append("second")

        with self.assertRaisesRegex(StateError, "journal head changed"):
            self.store.write_checkpoint(
                {"cursor": 2},
                created_at="2026-08-29T12:00:02Z",
                expected_anchor_sequence=first["sequence"],
                expected_anchor_sha256=first["event_sha256"],
            )

        self.assertEqual(original_bytes, self.store.checkpoint_path.read_bytes())
        view = self.store.recovery_view()
        self.assertEqual(original, view.checkpoint)
        self.assertEqual((second,), view.tail_events)

    def test_digest_configuration_and_anchor_tampering_fail_closed(self) -> None:
        self.append("anchor")
        baseline = self.store.write_checkpoint(
            {"cursor": 1}, created_at="2026-08-29T12:00:01Z"
        )

        digest_tampered = dict(baseline)
        digest_tampered["checkpoint_sha256"] = "0" * 64
        self.rewrite_checkpoint(digest_tampered, recompute_digest=False)
        with self.assertRaisesRegex(StateError, "digest"):
            self.store.read_checkpoint()

        config_tampered = json.loads(json.dumps(baseline))
        config_tampered["config_sha256"] = "3" * 64
        self.rewrite_checkpoint(config_tampered)
        with self.assertRaisesRegex(StateError, "checkpoint is invalid"):
            self.store.read_checkpoint()

        anchor_tampered = json.loads(json.dumps(baseline))
        anchor_tampered["anchor"]["event_type"] = "different_event"
        self.rewrite_checkpoint(anchor_tampered)
        with self.assertRaisesRegex(StateError, "does not match"):
            self.store.recovery_view()

        beyond_journal = json.loads(json.dumps(baseline))
        beyond_journal["anchor"]["sequence"] = 2
        self.rewrite_checkpoint(beyond_journal)
        with self.assertRaisesRegex(StateError, "beyond"):
            self.store.read_checkpoint()

    def test_noncanonical_unsafe_or_symlink_checkpoint_fails_closed(self) -> None:
        self.append()
        checkpoint = self.store.write_checkpoint(
            {"cursor": 1}, created_at="2026-08-29T12:00:01Z"
        )

        self.store.checkpoint_path.write_bytes(canonical_bytes(checkpoint) + b"\n")
        with self.assertRaisesRegex(StateError, "not canonical"):
            self.store.read_checkpoint()

        self.store.checkpoint_path.write_bytes(canonical_bytes(checkpoint))
        self.store.checkpoint_path.chmod(0o644)
        with self.assertRaisesRegex(StateError, "unsafe metadata"):
            self.store.read_checkpoint()

        target = self.root / "checkpoint-target.json"
        target.write_bytes(canonical_bytes(checkpoint))
        target.chmod(0o600)
        self.store.checkpoint_path.unlink()
        self.store.checkpoint_path.symlink_to(target)
        with self.assertRaises(StateError):
            self.store.read_checkpoint()

    def test_failed_atomic_replacement_preserves_previous_checkpoint(self) -> None:
        self.append("first")
        original = self.store.write_checkpoint(
            {"cursor": 1}, created_at="2026-08-29T12:00:01Z"
        )
        original_bytes = self.store.checkpoint_path.read_bytes()
        second = self.append("second")

        with mock.patch(
            "autonomous_controller.state.os.replace",
            side_effect=OSError("injected replace failure"),
        ):
            with self.assertRaises(OSError):
                self.store.write_checkpoint(
                    {"cursor": 2}, created_at="2026-08-29T12:00:02Z"
                )

        self.assertEqual(original_bytes, self.store.checkpoint_path.read_bytes())
        self.assertEqual(
            [],
            list((self.state_root / MUTABLE_TEMP_DIRECTORY).iterdir()),
        )
        view = self.store.recovery_view()
        self.assertEqual(original, view.checkpoint)
        self.assertEqual((second,), view.tail_events)

    def test_status_temp_isolated_from_concurrent_layout_reader(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        failures: list[BaseException] = []

        from autonomous_controller import state as state_module

        original_mkstemp = state_module.tempfile.mkstemp

        def held_mkstemp(*args: object, **kwargs: object) -> tuple[int, str]:
            descriptor, name = original_mkstemp(*args, **kwargs)
            if Path(name).parent == self.state_root / MUTABLE_TEMP_DIRECTORY:
                entered.set()
                if not release.wait(timeout=5):
                    raise AssertionError("test did not release mutable state write")
            return descriptor, name

        status = {
            "kind": "himr_autonomous_controller_status",
            "schema_version": 1,
            "config_id": self.config.config_id,
        }

        def status_writer() -> None:
            try:
                self.store.write_status(status)
            except BaseException as error:  # pragma: no cover - asserted below
                failures.append(error)

        with mock.patch(
            "autonomous_controller.state.tempfile.mkstemp",
            side_effect=held_mkstemp,
        ):
            writer = threading.Thread(target=status_writer)
            writer.start()
            self.assertTrue(entered.wait(timeout=5))
            scratch = self.state_root / MUTABLE_TEMP_DIRECTORY
            self.assertEqual(1, len(list(scratch.iterdir())))
            self.assertEqual(
                "stopped", read_control_state(self.config)["desired_state"]
            )
            reopened = ControlStore(self.config)
            self.assertIsNone(reopened.read_status())
            release.set()
            writer.join(timeout=5)

        self.assertFalse(writer.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(status, self.store.read_status())
        self.assertEqual([], list(scratch.iterdir()))

    def test_mutable_temp_root_must_remain_owner_private_directory(self) -> None:
        scratch = self.state_root / MUTABLE_TEMP_DIRECTORY
        scratch.mkdir(mode=0o755)
        with self.assertRaisesRegex(StateError, "mutable temp root"):
            read_control_state(self.config)

    def test_legacy_top_level_mutable_temp_still_fails_closed(self) -> None:
        unexpected = self.state_root / ".status.json.tmp-not-isolated"
        unexpected.write_bytes(b"")
        unexpected.chmod(0o600)
        with self.assertRaisesRegex(StateError, "unexpected entries"):
            read_control_state(self.config)

    def test_checkpoint_write_and_event_admission_share_one_thread_lock(self) -> None:
        self.append("anchor")
        entered = threading.Event()
        release = threading.Event()
        append_finished = threading.Event()
        failures: list[BaseException] = []

        from autonomous_controller import state as state_module

        original_atomic = state_module._atomic_mutable_json

        def held_atomic(path: Path, value: dict[str, object], *, maximum: int) -> None:
            if path == self.store.checkpoint_path:
                entered.set()
                if not release.wait(timeout=5):
                    raise AssertionError("test did not release checkpoint write")
            original_atomic(path, value, maximum=maximum)

        def checkpoint_writer() -> None:
            try:
                self.store.write_checkpoint(
                    {"cursor": 1}, created_at="2026-08-29T12:00:01Z"
                )
            except BaseException as error:  # pragma: no cover - asserted below
                failures.append(error)

        def event_writer() -> None:
            try:
                self.append("tail")
                append_finished.set()
            except BaseException as error:  # pragma: no cover - asserted below
                failures.append(error)

        with mock.patch(
            "autonomous_controller.state._atomic_mutable_json",
            side_effect=held_atomic,
        ):
            checkpoint_thread = threading.Thread(target=checkpoint_writer)
            checkpoint_thread.start()
            self.assertTrue(entered.wait(timeout=5))
            event_thread = threading.Thread(target=event_writer)
            event_thread.start()
            self.assertFalse(append_finished.wait(timeout=0.1))
            release.set()
            checkpoint_thread.join(timeout=5)
            event_thread.join(timeout=5)

        self.assertFalse(checkpoint_thread.is_alive())
        self.assertFalse(event_thread.is_alive())
        self.assertEqual([], failures)
        view = self.store.recovery_view()
        self.assertEqual(1, view.anchor_sequence)
        self.assertEqual((self.store.events[1],), view.tail_events)


if __name__ == "__main__":
    unittest.main()
