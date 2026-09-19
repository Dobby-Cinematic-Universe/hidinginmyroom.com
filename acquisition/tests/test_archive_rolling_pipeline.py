from __future__ import annotations

import os
import hashlib
import shutil
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
TEST_ROOT = ACQUISITION_ROOT / ".test-work" / f"archive-rolling-{os.getpid()}"
sys.path.insert(0, str(ACQUISITION_ROOT))

import archive_rolling_pipeline as rolling  # noqa: E402


def remove_tree() -> None:
    if not TEST_ROOT.exists() and not TEST_ROOT.is_symlink():
        return
    for current, directories, files in os.walk(TEST_ROOT, topdown=False):
        root = Path(current)
        for name in files:
            path = root / name
            if not path.is_symlink():
                path.chmod(0o600)
        for name in directories:
            path = root / name
            if not path.is_symlink():
                path.chmod(0o700)
        root.chmod(0o700)
    shutil.rmtree(TEST_ROOT)


class ArchiveRollingPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        self.schedule_path = TEST_ROOT / "schedule.json"
        self.schedule_body = b'{"sealed":true}\n'
        self.schedule = {
            "schedule_id": "bgacqsched_" + "a" * 32,
            "identity_sha256": "b" * 64,
            "queue": {
                "bundle_id": "acqbundle_" + "c" * 32,
                "manifest_path": str(TEST_ROOT / "queue" / "manifest.json"),
            },
            "consumer": {"preprocess_state_root": str(TEST_ROOT / "state")},
            "policy": {
                "maximum_dispatch_items_per_run": 4,
                "maximum_dispatch_bytes_per_run": 8 * 1024**3,
                "maximum_run_seconds": 14_400,
                "free_space_floor_bytes": 128 * 1024**3,
            },
        }
        self.bundle_root = TEST_ROOT / "control"
        self.processing_root = TEST_ROOT / "processed"
        self.acquisition_root = TEST_ROOT / "acquired"
        self.producer_bundle = {
            "manifest": {
                "policy": {"media_output_root": str(self.acquisition_root)}
            },
            "orders": [
                {"source": {"platform": "internet_archive"}, "job_id": "one"}
            ],
        }

    def tearDown(self) -> None:
        remove_tree()

    def acquisition_result(
        self,
        *,
        status: str = "completed",
        stop_reason: str = "all_acquired",
        new_items: int = 1,
    ) -> dict[str, object]:
        return {
            "status": status,
            "stop_reason": stop_reason,
            "planned_ordinals": [1] if new_items else [],
            "planned_reservation_bytes": 2 * 1024**3 if new_items else 0,
            "queue_summary": {
                "new_item_count": new_items,
                "new_byte_count": 1234 if new_items else 0,
            }
            if new_items
            else None,
            "ready_before": {"ready_item_count": 0},
            "ready_after": {"ready_item_count": new_items},
        }

    @staticmethod
    def preprocess_result(*, processed: bool = True) -> dict[str, object]:
        return {
            "status": "bounded" if processed else "held",
            "stop_reason": (
                "ready_prefix_processed"
                if processed
                else "no_completed_unacknowledged_ready_results"
            ),
            "selected_queue_ordinals": [1] if processed else [],
            "processed_items": [
                {
                    "queue_ordinal": 1,
                    "bundle": {"bundle_id": "ppbatch_" + "d" * 32},
                    "preprocess_state_sha256": "e" * 64,
                }
            ]
            if processed
            else [],
            "ready_before": {"ready_item_count": 1 if processed else 0},
            "ready_after": {"ready_item_count": 0},
        }

    @staticmethod
    def validation_result() -> dict[str, object]:
        return {
            "status": "validated",
            "summary_sha256": "f" * 64,
            "ready_before": {
                "completed_acquisition_count": 1,
                "quarantined_acquisition_count": 0,
                "acknowledged_preprocess_count": 1,
                "ready_item_count": 0,
            },
            "queue_summary": {"pending_count": 0, "parked_count": 0},
        }

    def base_patches(self):
        return (
            mock.patch.object(
                rolling.background_producer,
                "load_schedule",
                return_value=(self.schedule, self.schedule_path, self.schedule_body),
            ),
            mock.patch.object(
                rolling.queue_runner,
                "_load_bundle",
                return_value=self.producer_bundle,
            ),
            mock.patch.object(
                rolling.background_producer,
                "validate_producer",
                return_value=self.validation_result(),
            ),
        )

    def call(self, **overrides):
        values = {
            "schedule_path": self.schedule_path,
            "bundle_root": self.bundle_root,
            "processing_output_root": self.processing_root,
            "max_new_items": 1,
            "max_new_bytes": 8 * 1024**3,
            "max_run_seconds": 60,
            "free_space_floor_bytes": 128 * 1024**3,
            "max_preprocess_items": 1,
        }
        values.update(overrides)
        return rolling.run_rolling_pipeline(**values)

    def test_workers_genuinely_overlap_with_exact_concurrency_one(self) -> None:
        producer_started = threading.Event()
        preprocess_started = threading.Event()
        active = {"producer": 0, "preprocess": 0}
        peaks = dict(active)
        guard = threading.Lock()

        def producer(*_args, **kwargs):
            self.assertEqual(1, kwargs["max_new_items"])
            with guard:
                active["producer"] += 1
                peaks["producer"] = max(peaks["producer"], active["producer"])
            producer_started.set()
            self.assertTrue(preprocess_started.wait(timeout=2))
            time.sleep(0.03)
            with guard:
                active["producer"] -= 1
            return self.acquisition_result()

        def preprocess(*_args, **kwargs):
            self.assertEqual(1, kwargs["limit"])
            with guard:
                active["preprocess"] += 1
                peaks["preprocess"] = max(
                    peaks["preprocess"], active["preprocess"]
                )
            preprocess_started.set()
            self.assertTrue(producer_started.wait(timeout=2))
            time.sleep(0.03)
            with guard:
                active["preprocess"] -= 1
            return self.preprocess_result()

        patches = self.base_patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            mock.patch.object(
                rolling.background_producer,
                "run_producer",
                side_effect=producer,
            ) as producer_call,
            mock.patch.object(
                rolling.archive_preprocess_handoff,
                "run_handoff",
                side_effect=preprocess,
            ) as preprocess_call,
        ):
            summary = self.call()

        self.assertEqual("bounded", summary["status"])
        self.assertTrue(summary["overlap"]["observed"])
        self.assertGreater(summary["overlap"]["duration_ms"], 0)
        self.assertEqual({"producer": 1, "preprocess": 1}, peaks)
        self.assertEqual(1, producer_call.call_count)
        self.assertEqual(1, preprocess_call.call_count)
        self.assertEqual(1, summary["accounting"]["new_acquisition_items"])
        self.assertEqual(1, summary["accounting"]["preprocessed_items"])
        self.assertEqual(2 * 1024**3, summary["accounting"]["acquisition_reservation_bytes"])
        core = {key: value for key, value in summary.items() if key != "summary_sha256"}
        self.assertEqual(
            rolling.sha256_bytes(rolling.canonical_bytes(core)),
            summary["summary_sha256"],
        )

    def test_worker_failure_reports_durable_partial_progress_without_success(self) -> None:
        producer_started = threading.Event()

        def producer(*_args, **_kwargs):
            producer_started.set()
            time.sleep(0.02)
            return self.acquisition_result()

        def preprocess(*_args, **_kwargs):
            self.assertTrue(producer_started.wait(timeout=2))
            raise rolling.preprocess_batch.BatchError("simulated preprocess failure")

        patches = self.base_patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            mock.patch.object(
                rolling.background_producer, "run_producer", side_effect=producer
            ),
            mock.patch.object(
                rolling.archive_preprocess_handoff,
                "run_handoff",
                side_effect=preprocess,
            ),
            self.assertRaises(rolling.RollingPipelineFailure) as raised,
        ):
            self.call()

        summary = raised.exception.summary
        self.assertEqual("failed", summary["status"])
        self.assertEqual(1, summary["accounting"]["new_acquisition_items"])
        self.assertEqual(0, summary["accounting"]["preprocessed_items"])
        self.assertEqual("preprocess", summary["errors"][0]["stage"])
        self.assertFalse(summary["completion_authority"]["summary_is_authority"])
        self.assertIsNotNone(summary["final_schedule_validation"])

    def test_held_workers_finish_without_busy_unbounded_retries(self) -> None:
        patches = self.base_patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            mock.patch.object(
                rolling.background_producer,
                "run_producer",
                return_value=self.acquisition_result(
                    status="held", stop_reason="max_new_bytes", new_items=0
                ),
            ) as producer,
            mock.patch.object(
                rolling.archive_preprocess_handoff,
                "run_handoff",
                return_value=self.preprocess_result(processed=False),
            ) as preprocess,
        ):
            summary = self.call()
        self.assertEqual("held", summary["status"])
        self.assertEqual(1, producer.call_count)
        # One final receipt replay is allowed after the producer's terminal
        # notification; it proves that no ready result appeared in the race window.
        self.assertLessEqual(preprocess.call_count, 2)

    def test_quarantined_epoch_finishes_parked_without_retry_loop(self) -> None:
        validation = self.validation_result()
        validation["ready_before"]["completed_acquisition_count"] = 0
        validation["ready_before"]["quarantined_acquisition_count"] = 1
        validation["queue_summary"]["parked_count"] = 1
        patches = self.base_patches()
        with (
            patches[0],
            patches[1],
            mock.patch.object(
                rolling.background_producer,
                "validate_producer",
                return_value=validation,
            ),
            mock.patch.object(
                rolling.background_producer,
                "run_producer",
                return_value=self.acquisition_result(
                    status="parked",
                    stop_reason="all_runnable_work_exhausted_with_quarantine",
                    new_items=0,
                ),
            ) as producer,
            mock.patch.object(
                rolling.archive_preprocess_handoff,
                "run_handoff",
                return_value=self.preprocess_result(processed=False),
            ) as preprocess,
        ):
            summary = self.call()
        self.assertEqual("parked", summary["status"])
        self.assertEqual(1, producer.call_count)
        self.assertLessEqual(preprocess.call_count, 2)
        self.assertEqual(
            1,
            summary["final_schedule_validation"]["parked_acquisition_count"],
        )

    def test_schedule_bounds_and_archive_only_queue_fail_closed(self) -> None:
        patches = self.base_patches()
        with patches[0], patches[1], self.assertRaisesRegex(
            rolling.RollingPipelineError, "max-new-items exceeds"
        ):
            self.call(max_new_items=5)

        self.producer_bundle["orders"][0]["source"]["platform"] = "youtube"
        patches = self.base_patches()
        with patches[0], patches[1], self.assertRaisesRegex(
            rolling.RollingPipelineError, "internet_archive-only"
        ):
            self.call()

    def test_cli_exposes_only_closed_limits_and_paths(self) -> None:
        help_text = rolling.build_parser().format_help().lower()
        for forbidden in (
            "credential",
            "cookie",
            "catalog",
            "discover",
            "cold",
            "delete",
            "publish",
            "command",
            "shell",
        ):
            self.assertNotIn(forbidden, help_text)
        self.assertEqual(1, rolling.SAFETY["maximum_network_concurrency"])
        self.assertEqual(1, rolling.SAFETY["maximum_preprocess_concurrency"])
        self.assertFalse(rolling.SAFETY["shell_execution_allowed"])
        wrapper = ACQUISITION_ROOT / "bin" / "archive-rolling-pipeline"
        wrapper_text = wrapper.read_text(encoding="utf-8")
        self.assertTrue(wrapper_text.startswith("#!/usr/bin/python3\n"))
        for name in (
            "archive_rolling_pipeline.py",
            "archive_preprocess_handoff.py",
        ):
            digest = hashlib.sha256((ACQUISITION_ROOT / name).read_bytes()).hexdigest()
            self.assertIn(digest, wrapper_text)


if __name__ == "__main__":
    unittest.main()
