from __future__ import annotations

import threading
import types
import unittest

from autonomous_controller.preprocess_stop import (
    PreprocessStopBoundary,
    PreprocessStopError,
    preprocess_stop_gate,
)


class PreprocessStopGateTests(unittest.TestCase):
    def module(self):
        module = types.ModuleType("test_handoff")

        def run_one(row, **_kwargs):
            return {"queue_ordinal": row["queue_ordinal"], "sealed": True}

        module._materialize_and_run_one = run_one
        return module, run_one

    def test_stop_before_first_item_raises_without_starting_it(self) -> None:
        module, original = self.module()
        with self.assertRaises(PreprocessStopBoundary):
            with preprocess_stop_gate(module, desired_state=lambda: "stopped") as seen:
                module._materialize_and_run_one({"queue_ordinal": 7})
        self.assertEqual([], seen.completed)
        self.assertEqual([], seen.attempted_queue_ordinals)
        self.assertEqual(7, seen.boundary_queue_ordinal)
        self.assertTrue(seen.stop_requested_between_items)
        self.assertIs(original, module._materialize_and_run_one)

    def test_current_item_finishes_and_next_item_observes_stop(self) -> None:
        module, original = self.module()
        states = iter(("running", "stopped"))
        with self.assertRaises(PreprocessStopBoundary):
            with preprocess_stop_gate(module, desired_state=lambda: next(states)) as seen:
                first = module._materialize_and_run_one({"queue_ordinal": 3})
                self.assertTrue(first["sealed"])
                module._materialize_and_run_one({"queue_ordinal": 4})
        self.assertEqual([3], [row["queue_ordinal"] for row in seen.completed])
        self.assertEqual([3], seen.attempted_queue_ordinals)
        self.assertEqual(4, seen.boundary_queue_ordinal)
        self.assertIs(original, module._materialize_and_run_one)

    def test_normal_and_source_error_paths_restore_the_function(self) -> None:
        module, original = self.module()
        with preprocess_stop_gate(module, desired_state=lambda: "running") as seen:
            module._materialize_and_run_one({"queue_ordinal": 1})
        self.assertEqual(1, len(seen.completed))
        self.assertIs(original, module._materialize_and_run_one)

        def failed(_row):
            raise ValueError("ffmpeg failed")

        module._materialize_and_run_one = failed
        with self.assertRaisesRegex(ValueError, "ffmpeg failed"):
            with preprocess_stop_gate(module, desired_state=lambda: "running") as failed_seen:
                module._materialize_and_run_one({"queue_ordinal": 2})
        self.assertEqual([2], failed_seen.attempted_queue_ordinals)
        self.assertEqual([], failed_seen.completed)
        self.assertIs(failed, module._materialize_and_run_one)

    def test_concurrent_owner_and_external_replacement_fail_closed(self) -> None:
        module, original = self.module()
        entered = threading.Event()
        release = threading.Event()
        errors = []

        def owner() -> None:
            try:
                with preprocess_stop_gate(module, desired_state=lambda: "running"):
                    entered.set()
                    release.wait(5)
            except Exception as error:  # pragma: no cover - diagnostic capture
                errors.append(error)

        thread = threading.Thread(target=owner)
        thread.start()
        self.assertTrue(entered.wait(5))
        with self.assertRaisesRegex(PreprocessStopError, "concurrent"):
            with preprocess_stop_gate(module, desired_state=lambda: "running"):
                pass
        release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], errors)
        self.assertIs(original, module._materialize_and_run_one)

        with self.assertRaisesRegex(PreprocessStopError, "changed"):
            with preprocess_stop_gate(module, desired_state=lambda: "running"):
                module._materialize_and_run_one = lambda _row: {}
        self.assertIs(original, module._materialize_and_run_one)

    def test_invalid_state_or_result_fails_closed(self) -> None:
        module, original = self.module()
        with self.assertRaisesRegex(PreprocessStopError, "neither"):
            with preprocess_stop_gate(module, desired_state=lambda: "paused"):
                module._materialize_and_run_one({"queue_ordinal": 1})
        self.assertIs(original, module._materialize_and_run_one)

        module._materialize_and_run_one = lambda _row: {"queue_ordinal": 9}
        with self.assertRaisesRegex(PreprocessStopError, "changed"):
            with preprocess_stop_gate(module, desired_state=lambda: "running"):
                module._materialize_and_run_one({"queue_ordinal": 1})


if __name__ == "__main__":
    unittest.main()
