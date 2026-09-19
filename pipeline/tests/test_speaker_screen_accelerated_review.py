"""Independent regression checks for resident orchestration review findings."""

import copy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import speaker_screen_accelerated as accelerated
from pipeline import speaker_screen_accelerated_engine as engine
from pipeline import speaker_screen_core as core
from pipeline import speaker_screen_paths as paths


class ResidentReviewTests(unittest.TestCase):
    def fixtures(self, directory):
        policy = core.validate_policy(copy.deepcopy(core.DEFAULT_POLICY))
        windows = core.plan_windows(2000, policy)
        models = {"kind": "himr_speaker_screen_models", "schema_version": 1,
                  "silero_vad": {"path": "/private/review-vad", "sha256": "0" * 64},
                  "ecapa_embedding": {"path": "/private/review-ecapa", "sha256": "1" * 64}}
        execution = accelerated.validate_execution({})
        plan = {"plan_id": "residentscreen_review", "order": {"recording": {"duration_ms": 2000},
                "models": models, "policy": policy, "resources": {"early_stop_on_positive": False}},
                "windows": windows, "batches": [windows], "execution": execution,
                "runtime_binding": {"versions": engine.CPU_RUNTIME_PINS, "recipe": engine.model_recipe("cpu"),
                                    "nvidia_driver_version": None}}
        runtime = {"models": models, "recipe": engine.model_recipe("cpu"), "threads": 1,
                   "device": "cpu", "batch_size": 8, "cuda_memory_fraction": None,
                   "runtime_versions": engine.CPU_RUNTIME_PINS, "cuda": None,
                   "requested_gpu_uuid": None, "nvidia_driver_version": None}
        binding = {"review": "synthetic source binding"}
        checkpoint = {"kind": "himr_resident_speaker_screen_checkpoint", "schema_version": 1,
                      "plan_id": plan["plan_id"], "binding_sha256": accelerated.screen.digest(binding),
                      "batch_index": 0, "runtime": runtime,
                      "window_results": [{"window": windows[0], "pcm_sha256": "2" * 64,
                      "observation": {"index": 0, "start_ms": 0, "end_ms": 2000, "speech_ms": 2000,
                                      "embedding": [1.0] + [0.0] * 191}}]}
        return plan, binding, checkpoint

    def replay(self, edit):
        with tempfile.TemporaryDirectory(prefix="resident-review-") as name:
            directory = Path(name)
            plan, binding, checkpoint = self.fixtures(directory)
            edit(checkpoint)
            with paths.retained_directory(directory):
                accelerated.screen.write_immutable(accelerated._checkpoint_path(directory, 0), checkpoint)
                return accelerated._observations_for(plan, directory, binding)

    def test_checkpoint_runtime_must_match_sealed_cpu_recipe(self):
        with self.assertRaises(accelerated.ScreenError):
            self.replay(lambda checkpoint: checkpoint.update(runtime={"device": "cuda"}))

    def test_valid_checkpoint_is_accepted(self):
        observed, _, _, _, completed = self.replay(lambda checkpoint: None)
        self.assertEqual(len(observed), 1)
        self.assertEqual(completed, {0})

    def test_checkpoint_embeddings_must_be_exact_192_dimensions(self):
        with self.assertRaises(accelerated.ScreenError):
            self.replay(lambda checkpoint: checkpoint["window_results"][0]["observation"].update(embedding=[1.0, 0.0]))

    def test_checkpoint_embeddings_must_already_be_unit_normalized(self):
        with self.assertRaises(accelerated.ScreenError):
            self.replay(lambda checkpoint: checkpoint["window_results"][0]["observation"].update(embedding=[10.0] + [0.0] * 191))

    def test_decode_close_attempts_every_child_after_one_cleanup_error(self):
        pool = object.__new__(accelerated.DecodePool)
        first, second = (mock.Mock(), mock.Mock()), (mock.Mock(), mock.Mock())
        pool.workers, pool.active = [first, second], None
        calls = []
        def close(process, connection):
            calls.append((process, connection))
            if (process, connection) == first:
                raise OSError("synthetic first child cleanup failure")
        with mock.patch.object(accelerated, "_close_process", side_effect=close):
            try:
                pool.close()
            except (OSError, accelerated.ScreenError):
                pass
        self.assertEqual(calls, [first, second])

    def test_decode_close_reports_a_surviving_child(self):
        process, connection = mock.Mock(), mock.Mock()
        process.pid = 12345
        process.is_alive.return_value = True
        with mock.patch.object(accelerated.os, "killpg"), self.assertRaises(accelerated.ScreenError):
            accelerated._close_process(process, connection)

    def test_connection_close_error_does_not_suppress_child_termination(self):
        process, connection = mock.Mock(), mock.Mock()
        process.pid = 12345
        process.is_alive.side_effect = [True, False, False]
        connection.close.side_effect = OSError("synthetic connection close failure")
        with mock.patch.object(accelerated.os, "killpg") as terminate:
            try:
                accelerated._close_process(process, connection)
            except (OSError, accelerated.ScreenError):
                pass
        terminate.assert_called_once_with(process.pid, accelerated.signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
