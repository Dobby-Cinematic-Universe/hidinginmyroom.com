from __future__ import annotations

import hashlib
import os
import shutil
import sys
import time
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
TEST_ROOT = ACQUISITION_ROOT / ".test-work" / f"queue-runner-{os.getpid()}"
CANARY_MANIFEST = (
    REPOSITORY_ROOT
    / "research/corpus/acquisition-planning/youtube-next-2026-08-27/bundles/bundles"
    / "acqbundle_54cd7c3187b0771a7f4daa18f8edb143/manifest.json"
)
sys.path.insert(0, str(ACQUISITION_ROOT))

import acquire  # noqa: E402
import materialize_queue  # noqa: E402
import queue_runner as runner  # noqa: E402


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


class AcquisitionQueueRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        self.output_root = TEST_ROOT / "media-output"
        self.sealed_floor = 100
        self.executable = TEST_ROOT / "yt-dlp"
        self.executable.write_bytes(b"#!/bin/sh\nexit 0\n")
        self.executable.chmod(0o700)
        executable_body = self.executable.read_bytes()
        executable_pin = {
            "executable": str(self.executable),
            "sha256": hashlib.sha256(executable_body).hexdigest(),
            "byte_count": len(executable_body),
        }
        plan = {
            "schema_version": 1,
            "planned_at": "2026-08-28T12:00:00Z",
            "limits": {"max_job_bytes": 1024},
            "candidates": [self._candidate(ordinal) for ordinal in range(1, 4)],
        }
        plan_core_sha = runner.sha256_bytes(runner.canonical_bytes(plan))
        plan["plan_id"] = materialize_queue.stable_id("acqplan", plan_core_sha)
        manifest, orders = materialize_queue.build_bundle_manifest(
            plan,
            media_output_root=self.output_root,
            executable_pin=executable_pin,
            global_cache_cap_bytes=1024 * 1024,
            free_space_floor_bytes=self.sealed_floor,
            format_selector=materialize_queue.DEFAULT_FORMAT_SELECTOR,
            http_timeout_seconds=30,
        )
        bundle = materialize_queue.admit_bundle(TEST_ROOT / "queue-root", manifest, orders)
        self.manifest_path = bundle / "manifest.json"
        self.manifest = manifest

    def tearDown(self) -> None:
        remove_tree()

    @staticmethod
    def _candidate(ordinal: int) -> dict[str, object]:
        return {
            "queue_ordinal": ordinal,
            "queue_state": "ready",
            "recording_id": f"recording_fixture_{ordinal}",
            "source_id": f"source_fixture_{ordinal}",
            "adapter": "direct_http",
            "platform": "internet_archive",
            "source_kind": "archive_media_file",
            "native_id": f"fixture-{ordinal}/video.mp4",
            "canonical_url": (
                f"https://archive.org/download/fixture-{ordinal}/video.mp4"
            ),
            "title": f"Fixture {ordinal}",
            "expected_sha256": None,
            "expected_byte_count": None,
        }

    def _orders(self) -> list[dict[str, object]]:
        bundle = runner._load_bundle(self.manifest_path)
        return bundle["orders"]

    def _dummy_result(self, order: dict[str, object]) -> dict[str, object]:
        ordinal = int(str(order["job_id"])[-6:])
        return {
            "admission": {
                "sha256": f"{ordinal:064x}",
                "byte_count": ordinal,
            }
        }

    def _write_dummy_result(self, order: dict[str, object]) -> dict[str, object]:
        value = self._dummy_result(order)
        path = runner._result_path(order)
        path.parent.mkdir(parents=True, mode=0o755)
        path.write_bytes(acquire.pretty_json(value).encode("utf-8"))
        path.chmod(0o644)
        return value

    @staticmethod
    def _dummy_loader(
        path: Path, _output_root: Path, _order: dict[str, object]
    ) -> dict[str, object] | None:
        if not path.exists():
            return None
        return acquire.strict_json_object(path.read_bytes(), "fixture result")

    def _run_with_fake_adapter(
        self,
        *,
        max_new_items: int,
        max_new_bytes: int,
        fail_ordinal: int | None = None,
    ) -> tuple[dict[str, object], list[int]]:
        calls: list[int] = []

        def acquire_one(order: dict[str, object], dry_run: bool) -> dict[str, object]:
            self.assertFalse(dry_run)
            ordinal = int(str(order["job_id"])[-6:])
            calls.append(ordinal)
            if ordinal == fail_ordinal:
                raise acquire.AcquisitionError("fixture acquisition failure")
            return self._write_dummy_result(order)

        with (
            mock.patch.object(acquire, "run_acquisition", side_effect=acquire_one),
            mock.patch.object(
                acquire, "load_reusable_result", side_effect=self._dummy_loader
            ),
            mock.patch.object(runner, "_capacity_allows", return_value=True),
            mock.patch.object(runner, "_hard_deadline", side_effect=lambda _value: nullcontext()),
        ):
            value = runner.run_queue(
                self.manifest_path,
                max_new_items=max_new_items,
                max_new_bytes=max_new_bytes,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor,
            )
        return value, calls

    def assert_summary_hash(self, value: dict[str, object]) -> None:
        core = {key: item for key, item in value.items() if key != "summary_sha256"}
        self.assertEqual(
            value["summary_sha256"],
            runner.sha256_bytes(runner.canonical_bytes(core)),
        )

    def test_validate_is_offline_read_only_and_canonical(self) -> None:
        before = self.manifest_path.read_bytes()
        with mock.patch.object(acquire, "run_acquisition") as adapter:
            value = runner.validate_queue(self.manifest_path)
        adapter.assert_not_called()
        self.assertEqual(value["status"], "validated")
        self.assertEqual(value["completed_count"], 0)
        self.assertEqual(value["pending_count"], 3)
        self.assertEqual(value["adapter_invocation_count"], 0)
        self.assertEqual(
            [row["ordinal"] for row in value["results"]], [1, 2, 3]
        )
        self.assertTrue(
            all(row["action"] == "validated_pending" for row in value["results"])
        )
        self.assertFalse(self.output_root.exists())
        self.assertEqual(self.manifest_path.read_bytes(), before)
        self.assert_summary_hash(value)

    def test_validate_rejects_result_state_change_across_final_replay(self) -> None:
        initial = [None, None, None]
        concurrent = [None, {"result_sha256": "a" * 64}, None]
        with (
            mock.patch.object(
                runner, "_scan_results", side_effect=[initial, concurrent]
            ),
            mock.patch.object(acquire, "run_acquisition") as adapter,
            self.assertRaisesRegex(runner.QueueRunnerError, "state changed"),
        ):
            runner.validate_queue(self.manifest_path)
        adapter.assert_not_called()

    def test_validate_rejects_duplicate_manifest_keys(self) -> None:
        body = self.manifest_path.read_bytes()
        self.manifest_path.chmod(0o600)
        self.manifest_path.write_bytes(body.replace(b"{", b'{"schema_version":1,', 1))
        self.manifest_path.chmod(0o400)
        with self.assertRaisesRegex(runner.QueueRunnerError, "duplicate key"):
            runner.validate_queue(self.manifest_path)

    def test_validate_rejects_writable_or_symlinked_work_order(self) -> None:
        order = self.manifest_path.parent / "work-orders/000001.json"
        order.chmod(0o600)
        with self.assertRaisesRegex(runner.QueueRunnerError, "mode 0400"):
            runner.validate_queue(self.manifest_path)
        order.chmod(0o400)
        body = order.read_bytes()
        work_orders = order.parent
        work_orders.chmod(0o700)
        order.unlink()
        target = TEST_ROOT / "substitute.json"
        target.write_bytes(body)
        target.chmod(0o400)
        order.symlink_to(target)
        work_orders.chmod(0o500)
        with self.assertRaisesRegex(runner.QueueRunnerError, "symlink"):
            runner.validate_queue(self.manifest_path)

    def test_validate_rejects_extra_bundle_entry(self) -> None:
        bundle = self.manifest_path.parent
        bundle.chmod(0o700)
        extra = bundle / "unexpected"
        extra.write_text("unexpected\n", encoding="utf-8")
        extra.chmod(0o400)
        bundle.chmod(0o500)
        with self.assertRaisesRegex(runner.QueueRunnerError, "missing or extra"):
            runner.validate_queue(self.manifest_path)

    def test_invalid_or_writable_completed_result_is_not_pending(self) -> None:
        order = self._orders()[0]
        path = runner._result_path(order)
        path.parent.mkdir(parents=True)
        path.write_text("{}\n", encoding="utf-8")
        path.chmod(0o644)
        with self.assertRaisesRegex(runner.QueueRunnerError, "failed immutable"):
            runner.validate_queue(self.manifest_path)
        path.chmod(0o666)
        with self.assertRaisesRegex(runner.QueueRunnerError, "non-writable by peers"):
            runner.validate_queue(self.manifest_path)

    def test_result_directory_without_result_fails_closed(self) -> None:
        order = self._orders()[0]
        runner._result_path(order).parent.mkdir(parents=True)
        with self.assertRaisesRegex(runner.QueueRunnerError, "without result.json"):
            runner.validate_queue(self.manifest_path)

    def test_strict_completed_result_reuse_is_reported(self) -> None:
        order = self._orders()[1]
        self._write_dummy_result(order)
        with (
            mock.patch.object(
                acquire, "load_reusable_result", side_effect=self._dummy_loader
            ) as loader,
            mock.patch.object(acquire, "run_acquisition") as adapter,
        ):
            value = runner.validate_queue(self.manifest_path)
        adapter.assert_not_called()
        self.assertEqual(loader.call_count, 2)
        self.assertEqual(value["completed_count"], 1)
        self.assertEqual(value["pending_count"], 2)
        self.assertEqual(value["results"][1]["action"], "validated_reuse")

    def test_run_dispatches_only_pending_jobs_in_exact_ordinal_order(self) -> None:
        self._write_dummy_result(self._orders()[1])
        value, calls = self._run_with_fake_adapter(
            max_new_items=2,
            max_new_bytes=2 * 1024,
        )
        self.assertEqual(calls, [1, 3])
        self.assertEqual(value["status"], "completed")
        self.assertEqual(value["stop_reason"], "all_completed")
        self.assertEqual(value["adapter_invocation_count"], 2)
        self.assertEqual(value["completed_before"], 1)
        self.assertEqual(value["new_item_count"], 2)
        self.assertEqual(value["results"][1]["action"], "reused")
        self.assert_summary_hash(value)

    def test_max_items_stops_at_the_earliest_remaining_pending_ordinal(self) -> None:
        value, calls = self._run_with_fake_adapter(
            max_new_items=1,
            max_new_bytes=3 * 1024,
        )
        self.assertEqual(calls, [1])
        self.assertEqual(value["status"], "bounded")
        self.assertEqual(value["stop_reason"], "max_new_items")
        self.assertEqual(value["completed_count"], 1)
        self.assertEqual(value["pending_count"], 2)
        self.assertEqual([row["status"] for row in value["results"]], [
            "completed", "pending", "pending"
        ])

    def test_max_bytes_reserves_the_full_sealed_job_cap(self) -> None:
        with mock.patch.object(acquire, "run_acquisition") as adapter:
            value = runner.run_queue(
                self.manifest_path,
                max_new_items=3,
                max_new_bytes=1023,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor,
            )
        adapter.assert_not_called()
        self.assertEqual(value["status"], "bounded")
        self.assertEqual(value["stop_reason"], "max_new_bytes")
        self.assertEqual(value["dispatch_reservation_bytes"], 0)

    def test_stricter_free_space_floor_stops_before_adapter(self) -> None:
        with (
            mock.patch.object(runner, "_capacity_allows", return_value=False),
            mock.patch.object(acquire, "run_acquisition") as adapter,
        ):
            value = runner.run_queue(
                self.manifest_path,
                max_new_items=3,
                max_new_bytes=3 * 1024,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor + 1,
            )
        adapter.assert_not_called()
        self.assertEqual(value["status"], "bounded")
        self.assertEqual(value["stop_reason"], "free_space_floor")

    def test_hard_deadline_interrupts_the_current_operation(self) -> None:
        with self.assertRaisesRegex(runner.QueueDeadlineError, "elapsed during"):
            with runner._hard_deadline(0.01):
                time.sleep(0.2)

    def test_weaker_or_non_bounding_runtime_limits_are_rejected(self) -> None:
        with self.assertRaisesRegex(runner.QueueRunnerError, "may not weaken"):
            runner.run_queue(
                self.manifest_path,
                max_new_items=3,
                max_new_bytes=3 * 1024,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor - 1,
            )
        with self.assertRaisesRegex(runner.QueueRunnerError, "0 through 3"):
            runner.run_queue(
                self.manifest_path,
                max_new_items=4,
                max_new_bytes=3 * 1024,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor,
            )
        with self.assertRaisesRegex(runner.QueueRunnerError, "0 through 3072"):
            runner.run_queue(
                self.manifest_path,
                max_new_items=3,
                max_new_bytes=3 * 1024 + 1,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor,
            )

    def test_provider_failure_is_bounded_quarantined_and_does_not_wedge_queue(self) -> None:
        calls: list[int] = []

        def acquire_one(order: dict[str, object], dry_run: bool) -> dict[str, object]:
            ordinal = int(str(order["job_id"])[-6:])
            calls.append(ordinal)
            if ordinal == 2:
                raise acquire.AcquisitionError("fixture acquisition failure")
            return self._write_dummy_result(order)

        with (
            mock.patch.object(acquire, "run_acquisition", side_effect=acquire_one),
            mock.patch.object(
                acquire, "load_reusable_result", side_effect=self._dummy_loader
            ),
            mock.patch.object(runner, "_capacity_allows", return_value=True),
            mock.patch.object(runner, "_hard_deadline", side_effect=lambda _value: nullcontext()),
        ):
            first = runner.run_queue(
                self.manifest_path,
                max_new_items=3,
                max_new_bytes=3 * 1024,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor,
            )
            second = runner.run_queue(
                self.manifest_path,
                max_new_items=1,
                max_new_bytes=1024,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor,
            )
            third = runner.run_queue(
                self.manifest_path,
                max_new_items=1,
                max_new_bytes=1024,
                max_run_seconds=60,
                free_space_floor_bytes=self.sealed_floor,
            )
        self.assertEqual(calls, [1, 2, 3, 2, 2])
        self.assertEqual(first["status"], "bounded")
        self.assertEqual(first["completed_count"], 2)
        self.assertEqual(first["pending_count"], 1)
        self.assertEqual(first["retryable_failed_count"], 1)
        self.assertEqual(first["failed_attempt_count"], 1)
        self.assertEqual(first["quarantined_count"], 0)
        self.assertEqual(second["failed_attempt_count"], 2)
        self.assertEqual(second["retryable_failed_count"], 1)
        self.assertEqual(third["status"], "parked")
        self.assertEqual(third["stop_reason"], "all_runnable_work_exhausted_with_quarantine")
        self.assertEqual(third["completed_count"], 2)
        self.assertEqual(third["pending_count"], 0)
        self.assertEqual(third["quarantined_count"], 1)
        self.assertEqual(third["parked_count"], 1)
        self.assertEqual(third["terminal_count"], 3)
        self.assertEqual(third["results"][1]["status"], "quarantined")
        self.assertEqual(third["results"][1]["failure_attempt_count"], 3)
        self.assertIsNotNone(third["results"][1]["quarantine_receipt_sha256"])
        self.assert_summary_hash(first)
        self.assert_summary_hash(second)
        self.assert_summary_hash(third)

        with (
            mock.patch.object(acquire, "run_acquisition") as adapter,
            mock.patch.object(
                acquire, "load_reusable_result", side_effect=self._dummy_loader
            ),
        ):
            replay = runner.validate_queue(self.manifest_path)
        adapter.assert_not_called()
        self.assertEqual(replay["results"][1]["action"], "validated_quarantine")
        self.assertEqual(replay["quarantined_count"], 1)

    def test_failure_receipt_tamper_fails_closed(self) -> None:
        _value, _calls = self._run_with_fake_adapter(
            max_new_items=1,
            max_new_bytes=1024,
            fail_ordinal=1,
        )
        bundle = runner._load_bundle(self.manifest_path)
        receipt = runner._failure_order_root(bundle, 1) / "attempts/000001.json"
        receipt.chmod(0o600)
        with self.assertRaisesRegex(runner.QueueRunnerError, "mode 0400"):
            runner.validate_queue(self.manifest_path)

    def test_no_subset_or_authority_options_exist(self) -> None:
        parser = runner.build_parser()
        help_text = parser.format_help()
        self.assertNotIn("start-ordinal", help_text)
        self.assertNotIn("end-ordinal", help_text)
        self.assertNotIn("credential", help_text)
        self.assertEqual(runner.RUNNER_SAFETY["maximum_concurrency"], 1)
        for key in (
            "identity_authority",
            "event_authority",
            "publication_authority",
            "export_authority",
            "deletion_authority",
        ):
            self.assertEqual(runner.RUNNER_SAFETY[key], "none")

    @unittest.skipUnless(CANARY_MANIFEST.exists(), "ignored 25-order pilot is unavailable")
    def test_existing_25_order_bundle_offline_canary(self) -> None:
        with mock.patch.object(acquire, "run_acquisition") as adapter:
            value = runner.validate_queue(CANARY_MANIFEST)
        adapter.assert_not_called()
        rows = value["results"]
        completed = sum(row["status"] == "completed" for row in rows)
        pending = sum(row["status"] == "pending" for row in rows)
        self.assertEqual(value["job_count"], len(rows))
        self.assertEqual(value["completed_count"], completed)
        self.assertEqual(value["pending_count"], pending)
        self.assertEqual(completed + pending, len(rows))
        self.assertEqual(
            [row["ordinal"] for row in rows],
            list(range(1, len(rows) + 1)),
        )
        self.assert_summary_hash(value)


if __name__ == "__main__":
    unittest.main()
