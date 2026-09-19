from __future__ import annotations

import hashlib
import os
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
TEST_ROOT = ACQUISITION_ROOT / ".test-work" / f"archive-handoff-{os.getpid()}"
sys.path.insert(0, str(ACQUISITION_ROOT))

import archive_preprocess_handoff as handoff  # noqa: E402


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


class ArchivePreprocessHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        self.schedule_path = TEST_ROOT / "schedule.json"
        self.schedule = {
            "schedule_id": "bgacqsched_" + "a" * 32,
            "identity_sha256": "b" * 64,
            "queue": {
                "bundle_id": "acqbundle_" + "c" * 32,
                "manifest_path": str(TEST_ROOT / "queue" / "manifest.json"),
            },
            "consumer": {"preprocess_state_root": str(TEST_ROOT / "state")},
        }
        self.schedule_body = b'{"sealed":true}\n'
        self.output_root = TEST_ROOT / "acquired"
        self.bundle_root = TEST_ROOT / "preprocess-control"
        self.processing_root = TEST_ROOT / "preprocess-output"
        self.orders = [self.order(ordinal) for ordinal in range(1, 5)]
        self.bundle = {
            "manifest": {
                "policy": {"media_output_root": str(self.output_root)},
                "work_orders": [
                    {
                        "queue_ordinal": ordinal,
                        "job_id": self.orders[ordinal - 1]["job_id"],
                    }
                    for ordinal in range(1, 5)
                ],
            },
            "orders": self.orders,
        }
        self.states = [
            {
                "result_sha256": f"{ordinal:064x}",
                "media_sha256": f"{ordinal + 10:064x}",
                "byte_count": ordinal * 100,
            }
            for ordinal in range(1, 5)
        ]

    def tearDown(self) -> None:
        remove_tree()

    def order(self, ordinal: int) -> dict[str, object]:
        return {
            "job_id": f"archive-job-{ordinal}",
            "output": {"root": str(self.output_root)},
            "source": {"ordinal": ordinal, "platform": "internet_archive"},
        }

    def ready(self, ordinals: list[int]) -> dict[str, object]:
        rows = [
            {
                "ordinal": ordinal,
                "job_id": self.orders[ordinal - 1]["job_id"],
                "media_sha256": self.states[ordinal - 1]["media_sha256"],
                "media_byte_count": self.states[ordinal - 1]["byte_count"],
            }
            for ordinal in ordinals
        ]
        return {
            "completed_acquisition_count": len(self.states),
            "acknowledged_preprocess_count": len(self.states) - len(rows),
            "ready_item_count": len(rows),
            "ready_byte_count": sum(row["media_byte_count"] for row in rows),
            "zone": "at_or_below_low_water" if len(rows) <= 1 else "hysteresis_hold",
            "items": rows,
        }

    @staticmethod
    def selection_for(paths: list[Path]) -> dict[str, object]:
        digest = hashlib.sha256(str(paths[0]).encode()).hexdigest()
        return {
            "selection_id": f"pbsel_{digest[:32]}",
            "selection_sha256": digest,
            "entries": [{"path": str(paths[0])}],
        }

    def common_patches(
        self,
        *,
        ready_before: dict[str, object],
        ready_after: dict[str, object],
    ):
        manifests: dict[str, dict[str, object]] = {}

        def materialize(
            selection_path: Path,
            bundle_root: Path,
            processing_root: Path,
            *,
            operation_profile: str,
        ) -> Path:
            self.assertEqual("asr-ready", operation_profile)
            path = bundle_root / "bundles" / f"bundle-{selection_path.stem}"
            manifests[str(path)] = handoff.preprocess_batch.build_selection(
                [
                    Path(
                        handoff.preprocess_batch.write_selection.call_args.args[0][0]
                    )
                ]
            )
            return path

        def validate(path: Path):
            selection = manifests[str(path)]
            digest = hashlib.sha256(str(path).encode()).hexdigest()
            return (
                {
                    "bundle_id": f"ppbatch_{digest[:32]}",
                    "manifest_sha256": digest,
                    "work_order_count": 1,
                    "processing_output_root": str(self.processing_root),
                },
                selection,
                [{"operations": handoff.preprocess_batch.ASR_READY_OPERATIONS}],
            )

        def run(path: Path, state_root: Path, *, limit: int):
            self.assertEqual(Path(self.schedule["consumer"]["preprocess_state_root"]), state_root)
            self.assertEqual(1, limit)
            manifest, _selection, _orders = validate(path)
            return {
                "bundle_id": manifest["bundle_id"],
                "status": "complete",
                "pending_count": 0,
                "completed_count": 1,
                "completed_ordinals": [1],
                "state_sha256": "d" * 64,
            }

        load_schedule = mock.patch.object(
            handoff.background_producer,
            "load_schedule",
            return_value=(self.schedule, self.schedule_path, self.schedule_body),
        )
        load_bundle = mock.patch.object(
            handoff.queue_runner, "_load_bundle", return_value=self.bundle
        )
        load_runtime = mock.patch.object(
            handoff.background_producer,
            "_load_runtime",
            side_effect=[
                (self.bundle, self.states, ready_before, {"status": "validated"}),
                (self.bundle, self.states, ready_after, {"status": "validated"}),
            ],
        )
        build_selection = mock.patch.object(
            handoff.preprocess_batch,
            "build_selection",
            side_effect=self.selection_for,
        )
        write_selection = mock.patch.object(
            handoff.preprocess_batch,
            "write_selection",
            side_effect=lambda paths, _output: self.selection_for(paths),
        )
        materialize_bundle = mock.patch.object(
            handoff.preprocess_batch, "materialize_bundle", side_effect=materialize
        )
        validate_bundle = mock.patch.object(
            handoff.preprocess_batch, "validate_bundle", side_effect=validate
        )
        run_batch = mock.patch.object(
            handoff.preprocess_batch, "run_batch", side_effect=run
        )
        return (
            load_schedule,
            load_bundle,
            load_runtime,
            build_selection,
            write_selection,
            materialize_bundle,
            validate_bundle,
            run_batch,
        )

    def test_selects_only_unacknowledged_ready_ordinals_in_sealed_order(self) -> None:
        before = self.ready([2, 4])
        after = self.ready([])
        patches = self.common_patches(ready_before=before, ready_after=after)
        with patches[0], patches[1], patches[2], patches[3], patches[4] as writes, patches[5], patches[6], patches[7] as runs:
            summary = handoff.run_handoff(
                self.schedule_path,
                bundle_root=self.bundle_root,
                processing_output_root=self.processing_root,
                limit=4,
            )

        self.assertEqual("bounded", summary["status"])
        self.assertEqual([2, 4], summary["selected_queue_ordinals"])
        self.assertEqual(2, runs.call_count)
        selected_results = [
            call.args[0][0] for call in writes.call_args_list
        ]
        self.assertEqual(
            [
                handoff.queue_runner._result_path(self.orders[1]),
                handoff.queue_runner._result_path(self.orders[3]),
            ],
            selected_results,
        )
        self.assertTrue(all("ordinal-00000" in str(call.args[1]) for call in writes.call_args_list))
        core = {key: value for key, value in summary.items() if key != "summary_sha256"}
        self.assertEqual(
            handoff.sha256_bytes(handoff.canonical_bytes(core)),
            summary["summary_sha256"],
        )

    def test_exact_parked_ordinal_is_visible_and_later_ready_work_continues(self) -> None:
        before = self.ready([2, 4])
        after = self.ready([2])
        patches = self.common_patches(ready_before=before, ready_after=after)
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4] as writes,
            patches[5],
            patches[6],
            patches[7] as runs,
        ):
            summary = handoff.run_handoff(
                self.schedule_path,
                bundle_root=self.bundle_root,
                processing_output_root=self.processing_root,
                limit=4,
                parked_queue_ordinals=[2],
            )

        self.assertEqual([2], summary["parked_queue_ordinals"])
        self.assertEqual([4], summary["selected_queue_ordinals"])
        self.assertEqual(1, runs.call_count)
        self.assertEqual(
            handoff.queue_runner._result_path(self.orders[3]),
            writes.call_args.args[0][0],
        )

    def test_returns_held_without_materializing_when_no_result_is_ready(self) -> None:
        ready = self.ready([])
        with (
            mock.patch.object(
                handoff.background_producer,
                "load_schedule",
                return_value=(self.schedule, self.schedule_path, self.schedule_body),
            ),
            mock.patch.object(
                handoff.queue_runner, "_load_bundle", return_value=self.bundle
            ),
            mock.patch.object(
                handoff.background_producer,
                "_load_runtime",
                return_value=(self.bundle, self.states, ready, {"status": "validated"}),
            ),
            mock.patch.object(handoff.preprocess_batch, "materialize_bundle") as materialize,
            mock.patch.object(handoff.preprocess_batch, "run_batch") as run,
        ):
            summary = handoff.run_handoff(
                self.schedule_path,
                bundle_root=self.bundle_root,
                processing_output_root=self.processing_root,
                limit=4,
            )
        materialize.assert_not_called()
        run.assert_not_called()
        self.assertEqual("held", summary["status"])
        self.assertEqual([], summary["selected_queue_ordinals"])

    def test_limit_is_bounded_and_cold_outputs_are_rejected(self) -> None:
        with self.assertRaisesRegex(
            handoff.ArchivePreprocessHandoffError, "1 through 8"
        ):
            handoff.run_handoff(
                self.schedule_path,
                bundle_root=self.bundle_root,
                processing_output_root=self.processing_root,
                limit=9,
            )
        with (
            mock.patch.object(
                handoff.background_producer,
                "load_schedule",
                return_value=(self.schedule, self.schedule_path, self.schedule_body),
            ),
            mock.patch.object(
                handoff.queue_runner, "_load_bundle", return_value=self.bundle
            ),
            self.assertRaisesRegex(
                handoff.ArchivePreprocessHandoffError, "cold archive"
            ),
        ):
            handoff.run_handoff(
                self.schedule_path,
                bundle_root=Path("/mnt/archive/HIMR/forbidden"),
                processing_output_root=self.processing_root,
                limit=1,
            )

    def test_non_archive_work_order_is_rejected(self) -> None:
        self.orders[0]["source"]["platform"] = "youtube"
        ready = self.ready([1])
        with self.assertRaisesRegex(
            handoff.ArchivePreprocessHandoffError, "internet_archive-only"
        ):
            handoff._ready_rows(self.bundle, self.states, ready)

    def test_atomic_result_admission_shape_gets_only_a_bounded_exact_retry(self) -> None:
        ready = self.ready([1])
        expected = (self.bundle, self.states, ready, {"status": "validated"})
        transient_messages = (
            "result directory exists without result.json for archive-job",
            "managed output root identity changed while verifying durable acquisition result",
            "durable acquisition result path identity changed at component jobs",
            "completed/pending/quarantine state changed during offline validation",
        )
        for message in transient_messages:
            with self.subTest(message=message):
                transient = handoff.queue_runner.QueueRunnerError(message)
                with (
                    mock.patch.object(
                        handoff.background_producer,
                        "_load_runtime",
                        side_effect=[transient, expected],
                    ) as replay,
                    mock.patch.object(handoff.time, "sleep") as pause,
                ):
                    observed = handoff._load_runtime_with_admission_retry(self.schedule)
                self.assertEqual(expected, observed)
                self.assertEqual(2, replay.call_count)
                pause.assert_called_once_with(handoff.RESULT_ADMISSION_RETRY_SECONDS)

        transient = handoff.queue_runner.QueueRunnerError(transient_messages[1])
        with (
            mock.patch.object(
                handoff.background_producer,
                "_load_runtime",
                side_effect=transient,
            ) as replay,
            mock.patch.object(handoff.time, "sleep"),
            self.assertRaises(handoff.queue_runner.QueueRunnerError),
        ):
            handoff._load_runtime_with_admission_retry(self.schedule)
        self.assertEqual(handoff.RESULT_ADMISSION_RETRY_LIMIT, replay.call_count)

        integrity_error = handoff.queue_runner.QueueRunnerError(
            "completed result failed immutable reuse: durable acquisition result bytes changed"
        )
        with (
            mock.patch.object(
                handoff.background_producer,
                "_load_runtime",
                side_effect=integrity_error,
            ) as replay,
            mock.patch.object(handoff.time, "sleep") as pause,
            self.assertRaises(handoff.queue_runner.QueueRunnerError),
        ):
            handoff._load_runtime_with_admission_retry(self.schedule)
        self.assertEqual(1, replay.call_count)
        pause.assert_not_called()

    def test_interrupted_item_reconstructs_the_same_selection_and_bundle(self) -> None:
        ordinal = 2
        order = self.orders[ordinal - 1]
        state = self.states[ordinal - 1]
        result_path = handoff.queue_runner._result_path(order)
        row = {
            "queue_ordinal": ordinal,
            "job_id": order["job_id"],
            "result_path": result_path,
            "result_sha256": state["result_sha256"],
            "media_sha256": state["media_sha256"],
            "media_byte_count": state["byte_count"],
        }
        selection = self.selection_for([result_path])
        bundle_path = self.bundle_root / "bundles" / "fixed-bundle"
        manifest = {
            "bundle_id": "ppbatch_" + "e" * 32,
            "manifest_sha256": "f" * 64,
            "work_order_count": 1,
            "processing_output_root": str(self.processing_root),
        }
        successful_run = {
            "bundle_id": manifest["bundle_id"],
            "status": "complete",
            "pending_count": 0,
            "completed_count": 1,
            "completed_ordinals": [1],
            "state_sha256": "1" * 64,
        }
        with (
            mock.patch.object(
                handoff.preprocess_batch, "build_selection", return_value=selection
            ),
            mock.patch.object(
                handoff.preprocess_batch, "write_selection", return_value=selection
            ) as writes,
            mock.patch.object(
                handoff.preprocess_batch,
                "materialize_bundle",
                return_value=bundle_path,
            ) as materializes,
            mock.patch.object(
                handoff.preprocess_batch,
                "validate_bundle",
                return_value=(
                    manifest,
                    selection,
                    [{"operations": handoff.preprocess_batch.ASR_READY_OPERATIONS}],
                ),
            ),
            mock.patch.object(
                handoff.preprocess_batch,
                "run_batch",
                side_effect=[
                    handoff.preprocess_batch.BatchError("simulated interruption"),
                    successful_run,
                ],
            ),
        ):
            with self.assertRaisesRegex(
                handoff.preprocess_batch.BatchError, "simulated interruption"
            ):
                handoff._materialize_and_run_one(
                    row,
                    selection_root=self.bundle_root / "selections",
                    bundle_root=self.bundle_root,
                    processing_output_root=self.processing_root,
                    state_root=Path(self.schedule["consumer"]["preprocess_state_root"]),
                )
            completed = handoff._materialize_and_run_one(
                row,
                selection_root=self.bundle_root / "selections",
                bundle_root=self.bundle_root,
                processing_output_root=self.processing_root,
                state_root=Path(self.schedule["consumer"]["preprocess_state_root"]),
            )

        self.assertEqual(2, writes.call_count)
        self.assertEqual(writes.call_args_list[0].args, writes.call_args_list[1].args)
        self.assertEqual(
            materializes.call_args_list[0].args,
            materializes.call_args_list[1].args,
        )
        self.assertEqual(manifest["bundle_id"], completed["bundle"]["bundle_id"])

    def test_cli_has_no_discovery_catalogue_cold_or_credential_input(self) -> None:
        help_text = handoff.build_parser().format_help().lower()
        for forbidden in (
            "credential",
            "cookie",
            "catalog",
            "discover",
            "cold",
            "delete",
            "publish",
        ):
            self.assertNotIn(forbidden, help_text)
        self.assertEqual("forbidden", handoff.SAFETY["catalog_access"])
        self.assertEqual("forbidden", handoff.SAFETY["cold_storage_access"])
        self.assertEqual("none", handoff.SAFETY["deletion_authority"])
        self.assertEqual("none", handoff.SAFETY["publication_authority"])
        source = ACQUISITION_ROOT / "archive_preprocess_handoff.py"
        wrapper = ACQUISITION_ROOT / "bin" / "archive-preprocess-next"
        wrapper_text = wrapper.read_text(encoding="utf-8")
        self.assertTrue(wrapper_text.startswith("#!/usr/bin/python3\n"))
        self.assertIn(hashlib.sha256(source.read_bytes()).hexdigest(), wrapper_text)

    def test_dedicated_cold_primary_acquisition_is_read_only_capability(self) -> None:
        cold_cas = handoff.COLD_ARCHIVE_ROOT / "corpus" / "raw" / "epoch-000001"
        self.assertEqual(
            "read_exact_sealed_acquisition_payloads_only",
            handoff._acquisition_storage_safety(cold_cas)["cold_storage_access"],
        )
        self.assertEqual(
            handoff.SAFETY,
            handoff._acquisition_storage_safety(self.output_root),
        )
        with self.assertRaisesRegex(
            handoff.ArchivePreprocessHandoffError, "dedicated descendant"
        ):
            handoff._acquisition_storage_safety(handoff.COLD_ARCHIVE_ROOT)

    def test_held_handoff_reports_cold_primary_without_touching_cold_storage(self) -> None:
        self.output_root = (
            handoff.COLD_ARCHIVE_ROOT / "corpus" / "raw" / "epoch-fixture"
        )
        self.bundle["manifest"]["policy"]["media_output_root"] = str(
            self.output_root
        )
        for order in self.orders:
            order["output"]["root"] = str(self.output_root)
        empty = self.ready([])
        patches = self.common_patches(ready_before=empty, ready_after=empty)
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3] as builds,
            patches[4] as writes,
            patches[5] as materializes,
            patches[6],
            patches[7] as runs,
        ):
            summary = handoff.run_handoff(
                self.schedule_path,
                bundle_root=self.bundle_root,
                processing_output_root=self.processing_root,
                limit=1,
            )
        builds.assert_not_called()
        writes.assert_not_called()
        materializes.assert_not_called()
        runs.assert_not_called()
        self.assertEqual("cold_primary", summary["storage_mode"])
        self.assertEqual(
            "read_exact_sealed_acquisition_payloads_only",
            summary["safety"]["cold_storage_access"],
        )
        self.assertEqual(str(self.output_root), summary["roots"]["acquisition_output_root"])


if __name__ == "__main__":
    unittest.main()
