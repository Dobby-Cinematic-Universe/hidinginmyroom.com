from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
TEST_ROOT = ACQUISITION_ROOT / ".test-work" / f"background-producer-{os.getpid()}"
sys.path.insert(0, str(ACQUISITION_ROOT))

import acquire  # noqa: E402
import background_producer as producer  # noqa: E402
import materialize_queue  # noqa: E402
import queue_runner  # noqa: E402


def remove_tree() -> None:
    if not TEST_ROOT.exists() and not TEST_ROOT.is_symlink():
        return
    for current, directories, files in os.walk(TEST_ROOT, topdown=False):
        current_path = Path(current)
        for name in files:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o600)
        for name in directories:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o700)
        current_path.chmod(0o700)
    shutil.rmtree(TEST_ROOT)


class BackgroundProducerTests(unittest.TestCase):
    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        self.output_root = TEST_ROOT / "media-output"
        self.executable = TEST_ROOT / "yt-dlp"
        self.executable.write_bytes(b"#!/bin/sh\nexit 0\n")
        self.executable.chmod(0o700)
        executable_body = self.executable.read_bytes()
        pin = {
            "executable": str(self.executable),
            "sha256": hashlib.sha256(executable_body).hexdigest(),
            "byte_count": len(executable_body),
        }
        plan = {
            "schema_version": 1,
            "planned_at": "2026-08-29T00:00:00Z",
            "limits": {"max_job_bytes": 1024},
            "candidates": [self.candidate(ordinal) for ordinal in range(1, 4)],
        }
        core_sha = queue_runner.sha256_bytes(queue_runner.canonical_bytes(plan))
        plan["plan_id"] = materialize_queue.stable_id("acqplan", core_sha)
        manifest, orders = materialize_queue.build_bundle_manifest(
            plan,
            media_output_root=self.output_root,
            executable_pin=pin,
            global_cache_cap_bytes=1024 * 1024,
            free_space_floor_bytes=100,
            format_selector=materialize_queue.DEFAULT_FORMAT_SELECTOR,
            http_timeout_seconds=30,
        )
        bundle = materialize_queue.admit_bundle(TEST_ROOT / "queues", manifest, orders)
        self.manifest_path = bundle / "manifest.json"
        self.state_root = TEST_ROOT / "preprocess-state"
        self.schedule = producer.build_schedule(
            manifest_path=self.manifest_path,
            preprocess_state_root=self.state_root,
            ready_high_items=3,
            ready_low_items=1,
            ready_high_bytes=4096,
            ready_low_bytes=1024,
            maximum_dispatch_items_per_run=3,
            maximum_dispatch_bytes_per_run=3072,
            maximum_run_seconds=60,
            free_space_floor_bytes=100,
        )
        schedule_parent = TEST_ROOT / "private-control"
        schedule_parent.mkdir(mode=0o700)
        self.schedule_path = schedule_parent / "schedule.json"
        producer._write_immutable(
            self.schedule_path,
            producer.pretty_bytes(self.schedule),
            "fixture schedule",
        )

    def tearDown(self) -> None:
        remove_tree()

    @staticmethod
    def candidate(ordinal: int) -> dict[str, object]:
        return {
            "queue_ordinal": ordinal,
            "queue_state": "ready",
            "recording_id": f"recording_fixture_{ordinal}",
            "source_id": f"source_fixture_{ordinal}",
            "adapter": "direct_http",
            "platform": "internet_archive",
            "source_kind": "archive_media_file",
            "native_id": f"fixture-{ordinal}/video.mp4",
            "canonical_url": f"https://archive.org/download/fixture-{ordinal}/video.mp4",
            "title": f"Fixture {ordinal}",
            "expected_sha256": None,
            "expected_byte_count": None,
        }

    @staticmethod
    def ready(items: int, byte_count: int, zone: str) -> dict[str, object]:
        return {
            "completed_acquisition_count": items,
            "acknowledged_preprocess_count": 0,
            "ready_item_count": items,
            "ready_byte_count": byte_count,
            "zone": zone,
            "items": [],
        }

    def assert_summary_hash(self, value: dict[str, object]) -> None:
        core = {key: item for key, item in value.items() if key != "summary_sha256"}
        self.assertEqual(
            value["summary_sha256"],
            producer.sha256_bytes(producer.canonical_bytes(core)),
        )

    def test_materialized_schedule_is_sealed_deterministic_and_offline_validates(self) -> None:
        self.assertEqual(0o400, self.schedule_path.stat().st_mode & 0o777)
        replay = producer.build_schedule(
            manifest_path=self.manifest_path,
            preprocess_state_root=self.state_root,
            ready_high_items=3,
            ready_low_items=1,
            ready_high_bytes=4096,
            ready_low_bytes=1024,
            maximum_dispatch_items_per_run=3,
            maximum_dispatch_bytes_per_run=3072,
            maximum_run_seconds=60,
            free_space_floor_bytes=100,
        )
        self.assertEqual(self.schedule, replay)
        with (
            mock.patch.object(acquire, "run_acquisition") as adapter,
            mock.patch.object(
                queue_runner,
                "_scan_results",
                wraps=queue_runner._scan_results,
            ) as scans,
        ):
            summary = producer.validate_producer(self.schedule_path)
        adapter.assert_not_called()
        self.assertEqual(2, scans.call_count)
        self.assertEqual("validated", summary["status"])
        self.assertEqual(3, summary["queue_summary"]["pending_count"])
        self.assertEqual("at_or_below_low_water", summary["ready_before"]["zone"])
        self.assert_summary_hash(summary)

    def test_storage_safety_distinguishes_dedicated_cold_primary_root(self) -> None:
        cold_root = producer.COLD_ARCHIVE_ROOT / "corpus" / "raw" / "epoch-000001"
        self.assertEqual(
            producer.COLD_PRIMARY_SCHEDULE_SAFETY,
            producer._queue_storage_safety(cold_root),
        )
        self.assertEqual(
            producer.SCHEDULE_SAFETY,
            producer._queue_storage_safety(self.output_root),
        )
        with self.assertRaisesRegex(
            producer.BackgroundProducerError, "dedicated descendant"
        ):
            producer._queue_storage_safety(producer.COLD_ARCHIVE_ROOT)
        with self.assertRaisesRegex(
            producer.BackgroundProducerError, "contain the cold archive"
        ):
            producer._queue_storage_safety(Path("/mnt/archive"))

    def test_quarantined_ordinal_is_terminal_but_never_ready_or_redispatched(self) -> None:
        with mock.patch.object(
            acquire,
            "run_acquisition",
            side_effect=acquire.AcquisitionError("fixture provider failure"),
        ) as adapter:
            summaries = [
                queue_runner.run_queue(
                    self.manifest_path,
                    max_new_items=1,
                    max_new_bytes=1024,
                    max_run_seconds=60,
                    free_space_floor_bytes=100,
                )
                for _ in range(queue_runner.FAILURE_QUARANTINE_ATTEMPTS)
            ]
        self.assertEqual(queue_runner.FAILURE_QUARANTINE_ATTEMPTS, adapter.call_count)
        self.assertEqual("quarantined", summaries[-1]["results"][0]["status"])

        bundle, states, ready, _queue_summary = producer._load_runtime(self.schedule)
        self.assertTrue(producer._quarantined_state(states[0]))
        self.assertEqual(1, ready["quarantined_acquisition_count"])
        self.assertEqual(0, ready["completed_acquisition_count"])
        self.assertEqual(0, ready["ready_item_count"])
        limits = producer._run_limits(
            self.schedule,
            max_new_items=1,
            max_new_bytes=1024,
            max_run_seconds=60,
            free_space_floor_bytes=100,
        )
        planned, reservation, reason = producer._dispatch_prefix(
            self.schedule, bundle, states, ready, limits
        )
        self.assertEqual([2], planned)
        self.assertEqual(1024, reservation)
        self.assertEqual("max_new_items", reason)

    def test_validated_snapshot_is_bounded_to_one_producer_call(self) -> None:
        with mock.patch.object(
            queue_runner,
            "_scan_results",
            wraps=queue_runner._scan_results,
        ) as scans:
            producer._load_runtime(self.schedule)
            producer._load_runtime(self.schedule)
        self.assertEqual(4, scans.call_count)

    def test_queue_summary_rows_are_rebound_to_the_sealed_manifest(self) -> None:
        summary = queue_runner.validate_queue(self.manifest_path)
        changed = json_clone(summary)
        changed["results"][0]["job_id"] = "job_substituted"
        core = {
            key: value for key, value in changed.items() if key != "summary_sha256"
        }
        changed["summary_sha256"] = producer.sha256_bytes(
            producer.canonical_bytes(core)
        )
        bundle = queue_runner._load_bundle(self.manifest_path)
        with self.assertRaisesRegex(
            producer.BackgroundProducerError, "sealed work order"
        ):
            producer._states_from_queue_summary(
                self.schedule,
                bundle,
                changed,
                expected_mode="validate",
                expected_statuses={"validated"},
            )

    def test_delegated_run_summary_is_reused_without_another_payload_scan(self) -> None:
        summary = queue_runner.run_queue(
            self.manifest_path,
            max_new_items=0,
            max_new_bytes=0,
            max_run_seconds=60,
            free_space_floor_bytes=100,
        )
        with mock.patch.object(queue_runner, "_scan_results") as scans:
            _bundle, states, ready = producer._runtime_from_queue_summary(
                self.schedule,
                summary,
                expected_mode="run",
                expected_statuses={"bounded", "completed"},
            )
        scans.assert_not_called()
        self.assertEqual([None, None, None], states)
        self.assertEqual("at_or_below_low_water", ready["zone"])

    def test_run_dispatches_one_exact_contiguous_prefix_with_existing_runner(self) -> None:
        bundle = queue_runner._load_bundle(self.manifest_path)
        before = self.ready(0, 0, "at_or_below_low_water")
        after = self.ready(2, 100, "hysteresis_hold")
        final_states = [{"result": {}}, {"result": {}}, None]
        queue_summary = {
            "completed_count": 2,
            "pending_count": 1,
            "stop_reason": "max_new_items",
        }
        with (
            mock.patch.object(
                producer,
                "_load_runtime",
                return_value=(bundle, [None, None, None], before, {"mode": "validate"}),
            ) as preflight,
            mock.patch.object(
                producer,
                "_runtime_from_queue_summary",
                return_value=(bundle, final_states, after),
            ) as final_snapshot,
            mock.patch.object(
                queue_runner, "run_queue", return_value=queue_summary
            ) as delegated,
        ):
            summary = producer.run_producer(
                self.schedule_path,
                max_new_items=2,
                max_new_bytes=2048,
                max_run_seconds=60,
                free_space_floor_bytes=100,
            )
        delegated.assert_called_once_with(
            self.manifest_path,
            max_new_items=2,
            max_new_bytes=2048,
            max_run_seconds=60,
            free_space_floor_bytes=100,
        )
        preflight.assert_called_once_with(self.schedule)
        final_snapshot.assert_called_once_with(
            self.schedule,
            queue_summary,
            expected_mode="run",
            expected_statuses={"bounded", "completed", "parked"},
        )
        self.assertEqual([1, 2], summary["planned_ordinals"])
        self.assertEqual("bounded", summary["status"])
        self.assert_summary_hash(summary)

    def test_high_water_and_hysteresis_hold_without_adapter(self) -> None:
        bundle = queue_runner._load_bundle(self.manifest_path)
        for ready in (
            self.ready(3, 1, "at_or_above_high_water"),
            self.ready(2, 1, "hysteresis_hold"),
        ):
            with (
                self.subTest(zone=ready["zone"]),
                mock.patch.object(
                    producer,
                    "_load_runtime",
                    return_value=(
                        bundle,
                        [None, None, None],
                        ready,
                        {"mode": "validate"},
                    ),
                ),
                mock.patch.object(queue_runner, "run_queue") as delegated,
            ):
                summary = producer.run_producer(
                    self.schedule_path,
                    max_new_items=3,
                    max_new_bytes=3072,
                    max_run_seconds=60,
                    free_space_floor_bytes=100,
                )
            delegated.assert_not_called()
            self.assertEqual("held", summary["status"])
            self.assertEqual(ready["zone"], summary["stop_reason"])

    def test_exhausted_queue_is_complete_without_delegated_dispatch(self) -> None:
        bundle = queue_runner._load_bundle(self.manifest_path)
        states = [{"result": {}} for _ in bundle["orders"]]
        ready = self.ready(3, 300, "at_or_above_high_water")
        with (
            mock.patch.object(
                producer,
                "_load_runtime",
                return_value=(bundle, states, ready, {"mode": "validate"}),
            ),
            mock.patch.object(queue_runner, "run_queue") as delegated,
        ):
            summary = producer.run_producer(
                self.schedule_path,
                max_new_items=3,
                max_new_bytes=3072,
                max_run_seconds=60,
                free_space_floor_bytes=100,
            )
        delegated.assert_not_called()
        self.assertEqual("completed", summary["status"])
        self.assertEqual("all_acquired", summary["stop_reason"])

    def test_full_next_order_reservation_is_required_without_skipping(self) -> None:
        bundle = queue_runner._load_bundle(self.manifest_path)
        schedule = dict(self.schedule)
        schedule["policy"] = {
            **self.schedule["policy"],
            "ready_high_bytes": 1500,
            "ready_low_bytes": 100,
        }
        ready = self.ready(0, 600, "at_or_below_low_water")
        selected, reserved, reason = producer._dispatch_prefix(
            schedule,
            bundle,
            [None, None, None],
            ready,
            {
                "max_new_items": 3,
                "max_new_bytes": 3072,
                "max_run_seconds": 60,
                "free_space_floor_bytes": 100,
            },
        )
        self.assertEqual([], selected)
        self.assertEqual(0, reserved)
        self.assertEqual("ready_high_bytes", reason)

    def test_weaker_runtime_floor_and_cold_mutable_state_are_rejected(self) -> None:
        with self.assertRaisesRegex(producer.BackgroundProducerError, "may not weaken"):
            producer.run_producer(
                self.schedule_path,
                max_new_items=1,
                max_new_bytes=1024,
                max_run_seconds=60,
                free_space_floor_bytes=99,
            )
        with self.assertRaisesRegex(producer.BackgroundProducerError, "cold archive"):
            producer.build_schedule(
                manifest_path=self.manifest_path,
                preprocess_state_root=Path("/mnt/archive/HIMR/forbidden-state"),
                ready_high_items=3,
                ready_low_items=1,
                ready_high_bytes=4096,
                ready_low_bytes=1024,
                maximum_dispatch_items_per_run=3,
                maximum_dispatch_bytes_per_run=3072,
                maximum_run_seconds=60,
                free_space_floor_bytes=100,
            )

    def test_schedule_or_receipt_identity_drift_fails_closed(self) -> None:
        changed = json_clone(self.schedule)
        changed["producer"]["source_sha256"] = "0" * 64
        with self.assertRaisesRegex(producer.BackgroundProducerError, "source differs"):
            producer.validate_schedule(changed)

        core = {
            "schema_version": 1,
            "receipt_kind": "completed_private_media_preprocess_batch_item",
            "bundle_id": "ppbatch_" + "a" * 32,
            "bundle_manifest_sha256": "b" * 64,
            "ordinal": 1,
            "entry_id": "pbe_" + "c" * 32,
            "job_id": "preprocess-fixture-1",
            "work_order": {},
            "acquisition_result": {},
            "source_media": {},
            "preprocess_result": {},
            "artifacts": [],
            "safety": {
                "exact_validation_completed": True,
                "network_access_performed": False,
                "publication_performed": False,
                "publication_authority": "none",
            },
        }
        receipt_digest = producer.sha256_bytes(producer.canonical_bytes(core))
        receipt = {
            **core,
            "receipt_id": f"ppreceipt_{receipt_digest[:32]}",
            "receipt_sha256": "0" * 64,
        }
        with self.assertRaisesRegex(producer.BackgroundProducerError, "identity"):
            producer._validate_receipt_digest(receipt)

    def test_exact_preprocess_receipt_releases_one_ready_result(self) -> None:
        bundle = queue_runner._load_bundle(self.manifest_path)
        order = bundle["orders"][0]
        result_path = queue_runner._result_path(order)
        result_path.parent.mkdir(parents=True)
        completed_at = "2026-08-29T00:01:00Z"
        media_sha = "a" * 64
        result = {
            "job_id": order["job_id"],
            "work_order_sha256": producer.sha256_bytes(
                producer.canonical_bytes(order)
            ),
            "completed_at": completed_at,
            "admission": {
                "path": str(TEST_ROOT / "media-fixture"),
                "media_id": f"media_sha256_{media_sha}",
                "sha256": media_sha,
                "byte_count": 123,
                "normalized_probe": {"format": {"duration_ms": 4567}},
            },
        }
        result_body = acquire.pretty_json(result).encode("utf-8")
        result_path.write_bytes(result_body)
        result_path.chmod(0o644)
        state = {
            "result_sha256": producer.sha256_bytes(result_body),
            "media_sha256": media_sha,
            "byte_count": 123,
        }

        preprocess_result = TEST_ROOT / "preprocess-result.json"
        preprocess_body = b'{"status":"completed"}\n'
        preprocess_result.write_bytes(preprocess_body)
        preprocess_result.chmod(0o444)
        receipt_core = {
            "schema_version": 1,
            "receipt_kind": "completed_private_media_preprocess_batch_item",
            "bundle_id": "ppbatch_" + "b" * 32,
            "bundle_manifest_sha256": "c" * 64,
            "ordinal": 1,
            "entry_id": "pbe_" + "d" * 32,
            "job_id": "preprocess-fixture-1",
            "work_order": {"path": "work-orders/000001.json"},
            "acquisition_result": {
                "path": str(result_path),
                "sha256": state["result_sha256"],
                "byte_count": len(result_body),
                "job_id": result["job_id"],
                "work_order_sha256": result["work_order_sha256"],
                "completed_at": completed_at,
            },
            "source_media": {
                "path": result["admission"]["path"],
                "media_id": result["admission"]["media_id"],
                "sha256": media_sha,
                "byte_count": 123,
                "duration_ms": 4567,
                "first_cataloged_at": completed_at,
            },
            "preprocess_result": {
                "path": str(preprocess_result),
                "sha256": producer.sha256_bytes(preprocess_body),
                "byte_count": len(preprocess_body),
                "processing_run_id": "run_preprocess_fixture",
                "recipe_sha256": "e" * 64,
                "reuse_mode": "none",
            },
            "artifacts": [{"artifact_kind": "audio_16khz_mono_flac"}],
            "safety": {
                "exact_validation_completed": True,
                "network_access_performed": False,
                "publication_performed": False,
                "publication_authority": "none",
            },
        }
        digest = producer.sha256_bytes(producer.canonical_bytes(receipt_core))
        receipt = {
            **receipt_core,
            "receipt_id": f"ppreceipt_{digest[:32]}",
            "receipt_sha256": digest,
        }
        receipt_dir = (
            self.state_root
            / "runs"
            / receipt["bundle_id"]
            / "receipts"
        )
        receipt_dir.mkdir(parents=True, mode=0o700)
        for directory in (
            self.state_root,
            self.state_root / "runs",
            receipt_dir.parent,
            receipt_dir,
        ):
            directory.chmod(0o700)
        receipt_path = receipt_dir / "000001.json"
        receipt_path.write_bytes(producer.pretty_bytes(receipt))
        receipt_path.chmod(0o400)
        acknowledgements = producer._acknowledged_results(
            self.state_root,
            bundle=bundle,
            states=[state, None, None],
        )
        self.assertEqual({str(result_path)}, acknowledgements)

    def test_cli_exposes_no_credentials_discovery_or_archive_transfer(self) -> None:
        help_text = producer.build_parser().format_help()
        for forbidden in ("credential", "cookie", "discover", "archive-transfer"):
            self.assertNotIn(forbidden, help_text)
        self.assertEqual(1, producer.RUNTIME_SAFETY["maximum_network_concurrency"])
        self.assertEqual("none", producer.RUNTIME_SAFETY["deletion_authority"])
        self.assertEqual("forbidden", producer.RUNTIME_SAFETY["cold_storage_access"])


def json_clone(value: object) -> object:
    return json.loads(json.dumps(value))


if __name__ == "__main__":
    unittest.main()
