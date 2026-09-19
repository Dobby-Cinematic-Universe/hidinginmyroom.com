from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest import mock

from jsonschema.validators import Draft202012Validator


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / f"preprocess-asr-runner-{os.getpid()}"
sys.path.insert(0, str(PIPELINE_ROOT))

import asr_whispercpp  # noqa: E402
import preprocess_asr_queue as queue  # noqa: E402
import preprocess_asr_queue_runner as runner  # noqa: E402


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def remove_tree() -> None:
    if not TEST_ROOT.exists():
        return
    for current, directories, files in os.walk(TEST_ROOT):
        current_path = Path(current)
        current_path.chmod(0o700)
        for name in directories:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o700)
        for name in files:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o600)
    shutil.rmtree(TEST_ROOT)


class PreprocessASRQueueRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        self.bundle = TEST_ROOT / "preprocess-bundle"
        self.bundle.mkdir(mode=0o700)
        self.state = TEST_ROOT / "preprocess-state"
        self.state.mkdir(mode=0o700)
        self.queue_root = TEST_ROOT / "private-queue"
        self.queue_root.mkdir(mode=0o700)
        self.output_root = TEST_ROOT / "private-results"
        self.engine = TEST_ROOT / "whisper-cli"
        self.engine.write_bytes(b"fixture whisper executable\n")
        self.engine.chmod(0o700)
        self.model = TEST_ROOT / "small.en.bin"
        self.model.write_bytes(b"fixture model\n")
        self.engine_sha = digest(self.engine)
        self.model_sha = digest(self.model)
        self.engine_profile = copy.deepcopy(
            queue.whispercpp_engine_profiles.ENGINE_PROFILES[1]
        )
        self.engine_profile["expected_sha256"] = self.engine_sha
        self.engine_profile["byte_count"] = self.engine.stat().st_size
        self.items = [
            self._item(1, routing_hint=runner.REVIEW_ROUTING_HINT),
            self._item(2, routing_hint=runner.PROCESS_ROUTING_HINT),
            self._item(3, routing_hint=runner.PROCESS_ROUTING_HINT),
            self._item(4, routing_hint=runner.REVIEW_ROUTING_HINT),
        ]
        self.origin = self._origin(len(self.items))

        def match_fixture(engine_sha256: str, byte_count: int) -> dict[str, object]:
            self.assertEqual(engine_sha256, self.engine_sha)
            self.assertEqual(byte_count, self.engine.stat().st_size)
            return copy.deepcopy(self.engine_profile)

        self.patchers = [
            mock.patch.object(
                queue.whispercpp_engine_profiles,
                "match_engine_profile",
                side_effect=match_fixture,
            ),
            mock.patch.object(queue, "EXPECTED_MODEL_SHA256", self.model_sha),
            mock.patch.object(
                queue, "EXPECTED_MODEL_BYTE_COUNT", self.model.stat().st_size
            ),
            mock.patch.object(
                queue,
                "_collect_sealed_items",
                return_value=(self.origin, self.items),
            ),
        ]
        for patcher in self.patchers:
            patcher.start()
        _manifest, self.manifest_path = queue.materialize_queue(
            preprocess_bundle=self.bundle,
            preprocess_state_root=self.state,
            queue_root=self.queue_root,
            asr_output_root=self.output_root,
            engine_path=self.engine,
            model_path=self.model,
        )

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        remove_tree()

    def _item(self, ordinal: int, *, routing_hint: str) -> dict[str, object]:
        source_digest = f"{ordinal + 100:064x}"
        run_id = f"run_preprocess_{ordinal:032x}"
        audio_path = TEST_ROOT / f"fixture-audio-{ordinal}.flac"
        audio_path.write_bytes(f"audio fixture {ordinal}\n".encode())
        audio_path.chmod(0o400)
        audio_digest = digest(audio_path)
        duration = 1_000 + ordinal
        probe = {
            "schema_version": 1,
            "media": {
                "basename": audio_path.name,
                "byte_count": audio_path.stat().st_size,
                "media_id": f"media_sha256_{audio_digest}",
                "sha256": audio_digest,
            },
            "primary_streams": {"audio_index": 0, "video_index": None},
            "format": {
                "format_name": "flac",
                "start_ms": 0,
                "duration_ms": duration,
            },
            "streams": [
                {
                    "index": 0,
                    "codec_type": "audio",
                    "codec_name": "flac",
                    "start_ms": 0,
                    "duration_ms": duration,
                    "audio": {
                        "channel_layout": "mono",
                        "channels": 1,
                        "sample_format": "s16",
                        "sample_rate_hz": 16_000,
                    },
                }
            ],
        }
        result_path = TEST_ROOT / f"fixture-result-{ordinal}.json"
        result_ref = {
            "path": str(result_path),
            "uri": result_path.as_uri(),
            "sha256": f"{ordinal + 200:064x}",
            "byte_count": 1_000 + ordinal,
            "job_id": f"preprocess-fixture-{ordinal}",
            "processing_run_id": run_id,
            "recipe_sha256": f"{ordinal + 300:064x}",
        }
        source_path = TEST_ROOT / f"fixture-source-{ordinal}.mp4"
        source = {
            "media_id": f"media_sha256_{source_digest}",
            "sha256": source_digest,
            "byte_count": 2_000 + ordinal,
            "path": str(source_path),
            "storage_uri": source_path.as_uri(),
            "duration_ms": 2_000 + ordinal,
            "first_cataloged_at": "2026-08-27T00:00:00Z",
        }
        audio = {
            "artifact_id": f"artifact_{ordinal:032x}",
            "artifact_kind": "audio_16khz_mono_flac",
            "processing_run_id": run_id,
            "media_id": f"media_sha256_{audio_digest}",
            "path": str(audio_path),
            "uri": audio_path.as_uri(),
            "sha256": audio_digest,
            "byte_count": audio_path.stat().st_size,
            "duration_ms": duration,
            "normalized_probe": probe,
            "visibility": "private",
        }
        return {
            "result": result_ref,
            "source_media": source,
            "audio": audio,
            "routing_hint": routing_hint,
            "evidence": {
                "mode": "sealed_preprocess_receipt",
                "receipt": {
                    "path": str(TEST_ROOT / f"receipt-{ordinal}.json"),
                    "uri": (TEST_ROOT / f"receipt-{ordinal}.json").as_uri(),
                    "physical_sha256": f"{ordinal + 400:064x}",
                    "receipt_id": f"ppreceipt_{ordinal:032x}",
                    "receipt_sha256": f"{ordinal + 500:064x}",
                    "ordinal": ordinal,
                },
                "preprocess_result": result_ref,
                "source_media": source,
            },
        }

    def _origin(self, count: int) -> dict[str, object]:
        return {
            "mode": "sealed_preprocess_receipts",
            "preprocess_bundle": {
                "path": str(self.bundle),
                "manifest_path": str(self.bundle / "manifest.json"),
                "manifest_physical_sha256": "a" * 64,
                "bundle_id": "ppbatch_" + "b" * 32,
                "identity_sha256": "c" * 64,
                "manifest_sha256": "d" * 64,
            },
            "state_root": str(self.state),
            "receipt_count": count,
            "receipt_state_sha256": "e" * 64,
            "receipt_refs_sha256": "f" * 64,
        }

    def _private_handling(
        self, item: dict[str, object]
    ) -> tuple[dict[str, object], dict[str, object]]:
        ordinal = item["evidence"]["receipt"]["ordinal"]
        policy = {
            "storage_scope": "private_canonical_cache",
            "publication_disposition": "no_publication_authority",
            "publication_authority": "none",
            "basis": "Private fixture with no publication authority.",
        }
        seal = {
            "artifact_root": str(TEST_ROOT / "private-acquisition"),
            "receipt_path": str(TEST_ROOT / "private-acquisition/seals/receipt.json"),
            "receipt_sha256": "6" * 64,
            "receipt_byte_count": 1024,
            "plan_sha256": "7" * 64,
            "validated_at": "2026-08-27T00:00:00Z",
            "result_physical_sha256": "8" * 64,
            "result_canonical_sha256": "9" * 64,
            "work_order_sha256": "a" * 64,
            "media_id": "media_sha256_" + "b" * 64,
            "media_sha256": "b" * 64,
            "media_byte_count": 2048,
            "source": {
                "source_id": "source_" + "c" * 32,
                "platform": "local",
                "source_kind": "livestream_capture",
                "native_id": "local-capture-fixture",
                "access_state": "unknown",
            },
            "source_byte_identity_claimed": False,
        }
        boundary = {
            "handling_policy": policy,
            "private_acquisition_seal": seal,
        }
        control_entry = {
            "ordinal": ordinal,
            "entry_id": f"pbe_{ordinal:032x}",
            "handling_policy": policy,
            "handling_boundary_sha256": queue.sha256_bytes(
                queue.canonical_bytes(boundary)
            ),
            "seal_receipt_sha256": seal["receipt_sha256"],
            "seal_plan_sha256": seal["plan_sha256"],
            "source_byte_identity_claimed": False,
        }
        core = {
            "kind": "v30_private_acquisition_handling_control",
            "private_entry_count": 1,
            "entries": [control_entry],
            "seal_receipt_replay_required": True,
            "handling_policy_propagation_required": True,
            "publication_authority": "none",
        }
        control = {
            **core,
            "identity_sha256": queue.sha256_bytes(queue.canonical_bytes(core)),
        }
        item["handling"] = {
            "preprocess_control_entry": copy.deepcopy(control_entry),
            "handling_boundary": copy.deepcopy(boundary),
        }
        return control, boundary

    def _validate_summary_schema(self, value: dict[str, object]) -> None:
        schema = json.loads(
            (PIPELINE_ROOT / "schemas/preprocess-asr-queue-run.schema.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)
        errors = list(Draft202012Validator(schema).iter_errors(value))
        self.assertEqual(errors, [], [error.message for error in errors])

    def _planned(self, order: dict[str, object]) -> dict[str, object]:
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        entry = next(
            row for row in manifest["work_orders"] if row["job_id"] == order["job_id"]
        )
        identity = runner._expected_adapter_identity(order)
        run_id = f"run_asr_whispercpp_{entry['ordinal']:032x}"
        input_stat = asr_whispercpp.public_file_stat(Path(order["input"]["path"]).stat())
        engine_stat = asr_whispercpp.public_file_stat(self.engine.stat())
        model_stat = asr_whispercpp.public_file_stat(self.model.stat())
        probe = {
            "ffprobe_path": "/usr/bin/ffprobe",
            "codec_name": "flac",
            "sample_format": "s16",
            "sample_rate_hz": 16_000,
            "channels": 1,
            "channel_layout": "mono",
            "format_name": "flac",
            "duration_ms": entry["audio_artifact"]["duration_ms"],
        }
        logical_probe = [
            probe["ffprobe_path"],
            "-v",
            "error",
            "-select_streams",
            "a:0",
            "-show_entries",
            (
                "format=duration,format_name:stream=index,codec_name,sample_fmt,"
                "sample_rate,channels,channel_layout,duration"
            ),
            "-of",
            "json",
            order["input"]["path"],
        ]
        logical_whisper = asr_whispercpp.build_command(
            order,
            identity["run_dir"] / "whisper-output",
            identity["window"],
            None,
        )
        executed_probe = [*logical_probe[:-1], "/proc/self/fd/10"]
        executed_whisper = list(logical_whisper)
        executed_whisper[0] = "/proc/self/fd/11"
        executed_whisper[executed_whisper.index("--model") + 1] = "/proc/self/fd/12"
        executed_whisper[executed_whisper.index("--file") + 1] = "/proc/self/fd/10"
        environment = asr_whispercpp.execution_environment(
            [logical_probe, logical_whisper],
            command_states=["executed", "planned"],
            version=order["engine"]["version_label"],
            version_evidence=order["engine"]["version_evidence"],
        )
        return {
            "schema_version": 1,
            "job_id": order["job_id"],
            "status": "planned",
            "dry_run": True,
            "work_order_sha256": identity["work_order_sha256"],
            "recipe_id": identity["recipe_id"],
            "recipe_sha256": identity["recipe_sha256"],
            "result_key": identity["result_key"],
            "processing_run": {
                "processing_run_id": run_id,
                "stage": asr_whispercpp.STAGE,
                "implementation_version": asr_whispercpp.IMPLEMENTATION_VERSION,
                "model_id": order["model"]["model_id"],
                "glossary_revision_id": None,
                "parameters_json": asr_whispercpp.canonical_json_text(
                    identity["recipe"]
                ),
                "environment_json": asr_whispercpp.canonical_json_text(environment),
                "random_seed": None,
                "started_at": "2026-08-27T00:00:00Z",
                "completed_at": "2026-08-27T00:00:01Z",
                "status": "queued",
                "error_text": None,
            },
            "input": {
                "path": order["input"]["path"],
                "sha256": order["input"]["expected_sha256"],
                "byte_count": entry["audio_artifact"]["byte_count"],
                "stat_before": input_stat,
                "stat_after": input_stat,
                "unchanged": True,
                "media_id": order["input"]["media_id"],
                "artifact_id": order["input"]["artifact_id"],
                "parent_processing_run_id": order["input"][
                    "parent_processing_run_id"
                ],
                "probe": probe,
            },
            "engine": {
                "path": order["engine"]["executable"],
                "sha256": order["engine"]["expected_sha256"],
                "byte_count": self.engine.stat().st_size,
                "stat_before": engine_stat,
                "stat_after": engine_stat,
                "unchanged": True,
                "version": order["engine"]["version_label"],
                "version_evidence": order["engine"]["version_evidence"],
                "build": order["engine"]["build"],
            },
            "model": {
                "path": order["model"]["path"],
                "sha256": order["model"]["expected_sha256"],
                "byte_count": self.model.stat().st_size,
                "stat_before": model_stat,
                "stat_after": model_stat,
                "unchanged": True,
                "model_id": order["model"]["model_id"],
                "name": order["model"]["name"],
                "revision": order["model"]["revision"],
                "source": order["model"]["source"],
                "license_label": order["model"]["license_label"],
            },
            "glossary": None,
            "catalog_context": None,
            "window": identity["window"],
            "commands": [executed_probe, executed_whisper],
            "artifacts": [],
            "transcript": None,
            "catalog_records": None,
            "result_path": str(identity["result_path"]),
            "duration_ms": 1,
            "errors": [],
        }

    def test_validate_routes_near_silent_and_performs_no_writes(self) -> None:
        relative = self.manifest_path.relative_to(REPOSITORY_ROOT)
        with mock.patch.object(asr_whispercpp, "run_asr") as adapter:
            value = runner.validate_dispatcher(relative)
        adapter.assert_not_called()
        self.assertEqual(value["status"], "validated")
        self.assertEqual(value["job_count"], 4)
        self.assertEqual(value["selected_count"], 2)
        self.assertEqual(value["review_count"], 2)
        self.assertEqual(
            [row["ordinal"] for row in value["results"]], [1, 2, 3, 4]
        )
        self.assertEqual(
            sum(row["action"] == "review_required" for row in value["results"]),
            2,
        )
        self.assertFalse(self.output_root.exists())
        self._validate_summary_schema(value)

    def test_dry_run_is_sequential_and_skips_review_route(self) -> None:
        calls: list[str] = []

        def planned(order: dict[str, object], *, dry_run: bool) -> dict[str, object]:
            self.assertTrue(dry_run)
            calls.append(order["job_id"])
            return self._planned(order)

        with mock.patch.object(asr_whispercpp, "run_asr", side_effect=planned):
            value = runner.run_queue(self.manifest_path, dry_run=True)
        expected = [
            row["job_id"]
            for row in json.loads(self.manifest_path.read_text())["work_orders"]
            if row["routing_hint"] == runner.PROCESS_ROUTING_HINT
        ]
        self.assertEqual(calls, expected)
        self.assertEqual(value["adapter_invocation_count"], 2)
        self.assertEqual(value["status"], "planned")
        self.assertFalse(self.output_root.exists())
        self._validate_summary_schema(value)

    def test_policy_bound_operational_dry_run_propagates_every_boundary(self) -> None:
        item = copy.deepcopy(self.items[0])
        item["routing_hint"] = runner.PROCESS_ROUTING_HINT
        control, boundary = self._private_handling(item)
        origin = self._origin(1)
        origin["handling_control"] = control
        policy_queue_root = TEST_ROOT / "private-policy-queue"
        policy_output_root = TEST_ROOT / "private-policy-results"

        def planned(order: dict[str, object], *, dry_run: bool) -> dict[str, object]:
            self.assertTrue(dry_run)
            return self._planned(order)

        original_manifest = self.manifest_path
        try:
            with (
                mock.patch.object(
                    queue, "_collect_sealed_items", return_value=(origin, [item])
                ),
                mock.patch.object(
                    queue.preprocess_batch,
                    "replay_handling_boundary",
                    return_value=boundary,
                ),
            ):
                _manifest, policy_manifest = queue.materialize_queue(
                    preprocess_bundle=self.bundle,
                    preprocess_state_root=self.state,
                    queue_root=policy_queue_root,
                    asr_output_root=policy_output_root,
                    engine_path=self.engine,
                    model_path=self.model,
                )
                self.manifest_path = policy_manifest
                with mock.patch.object(
                    asr_whispercpp, "run_asr", side_effect=planned
                ) as adapter:
                    value = runner.run_queue(policy_manifest, dry_run=True)
        finally:
            self.manifest_path = original_manifest

        self.assertEqual(adapter.call_count, 1)
        self.assertEqual(value["status"], "planned")
        self.assertEqual(value["handling_control"], control)
        self.assertTrue(
            value["safety"]["private_acquisition_seal_replay_required"]
        )
        self.assertTrue(
            value["safety"]["handling_policy_propagation_required"]
        )
        self.assertEqual(value["results"][0]["handling"], item["handling"])
        self.assertFalse(policy_output_root.exists())
        self._validate_summary_schema(value)

    def test_near_silent_override_dispatches_all_in_sealed_order(self) -> None:
        calls: list[str] = []

        def planned(order: dict[str, object], *, dry_run: bool) -> dict[str, object]:
            calls.append(order["job_id"])
            return self._planned(order)

        with mock.patch.object(asr_whispercpp, "run_asr", side_effect=planned):
            value = runner.run_queue(
                self.manifest_path,
                dry_run=True,
                include_near_silent=True,
            )
        expected = [
            row["job_id"]
            for row in json.loads(self.manifest_path.read_text())["work_orders"]
        ]
        self.assertEqual(calls, expected)
        self.assertEqual(value["selected_count"], 4)
        self.assertEqual(value["review_count"], 0)
        self.assertTrue(all(row["action"] == "planned" for row in value["results"]))
        self._validate_summary_schema(value)

    def test_first_adapter_failure_stops_and_replays_the_queue(self) -> None:
        calls: list[str] = []

        def fail_second(order: dict[str, object], *, dry_run: bool) -> dict[str, object]:
            calls.append(order["job_id"])
            if len(calls) == 2:
                raise asr_whispercpp.ASRError("fixture adapter failure")
            return self._planned(order)

        with mock.patch.object(asr_whispercpp, "run_asr", side_effect=fail_second):
            with self.assertRaises(runner.RunnerFailure) as raised:
                runner.run_queue(self.manifest_path, dry_run=True)
        value = raised.exception.result
        self.assertEqual(len(calls), 2)
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["adapter_invocation_count"], 2)
        self.assertEqual(value["failed_job"]["error"]["message"], "fixture adapter failure")
        self.assertFalse(self.output_root.exists())
        self._validate_summary_schema(value)

    def test_catalog_origin_is_rejected_before_validation_or_sqlite(self) -> None:
        path = TEST_ROOT / "catalog-origin.json"
        path.write_bytes(
            queue.pretty_bytes(
                {
                    "materializer": queue.MATERIALIZER_NAME,
                    "origin": {"mode": "catalog_admitted_preprocess_results"},
                    "safety": {"catalog_writes": False},
                }
            )
        )
        path.chmod(0o400)
        with (
            mock.patch.object(queue, "validate_queue") as validate,
            mock.patch.object(sqlite3, "connect") as connect,
            self.assertRaisesRegex(runner.RunnerError, "catalog admission"),
        ):
            runner.validate_dispatcher(path)
        validate.assert_not_called()
        connect.assert_not_called()

    def test_work_order_mode_change_fails_before_adapter(self) -> None:
        work_order = self.manifest_path.parent / "work-orders/000001.json"
        original = runner._stable_dispatch_order

        def tamper_then_read(*args: object, **kwargs: object) -> dict[str, object]:
            work_order.chmod(0o600)
            return original(*args, **kwargs)

        try:
            with (
                mock.patch.object(runner, "_stable_dispatch_order", side_effect=tamper_then_read),
                mock.patch.object(asr_whispercpp, "run_asr") as adapter,
                self.assertRaises(runner.RunnerFailure) as raised,
            ):
                runner.run_queue(self.manifest_path, dry_run=True)
            adapter.assert_not_called()
            self.assertIn(
                "post-failure replay also failed",
                raised.exception.result["failed_job"]["error"]["message"],
            )
            self._validate_summary_schema(raised.exception.result)
        finally:
            work_order.chmod(0o400)

    def test_completed_result_tree_with_extra_entry_is_not_reusable(self) -> None:
        manifest, orders = queue.validate_queue(self.manifest_path)
        process_index = next(
            index
            for index, row in enumerate(manifest["work_orders"])
            if row["routing_hint"] == runner.PROCESS_ROUTING_HINT
        )
        identity = runner._expected_adapter_identity(orders[process_index])
        identity["run_dir"].mkdir(parents=True, mode=0o700)
        identity["run_dir"].chmod(0o700)
        for name in (
            "result.json",
            "whisper.raw.json",
            "transcript.normalized.json",
            "unexpected.txt",
        ):
            (identity["run_dir"] / name).write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(runner.RunnerError, "missing or extra"):
            runner._validate_completed_result_tree(identity)

    def test_real_dispatch_revalidates_existing_then_invokes_adapter_for_reuse(self) -> None:
        processing_run_id = "run_asr_whispercpp_" + "a" * 32
        result_sha = "b" * 64
        sentinel = ({"processing_run": {"processing_run_id": processing_run_id}}, result_sha, {})

        def strict_existing(*args: object, **kwargs: object) -> dict[str, object]:
            return {
                "processing_run_id": processing_run_id,
                "result_sha256": result_sha,
            }

        def completed(*args: object, **kwargs: object) -> dict[str, object]:
            return {
                "processing_run_id": processing_run_id,
                "result_sha256": result_sha,
            }

        with (
            mock.patch.object(runner, "_preexisting_result", return_value=sentinel),
            mock.patch.object(
                runner, "_strict_preexisting_result", side_effect=strict_existing
            ) as strict,
            mock.patch.object(asr_whispercpp, "run_asr", return_value={}) as adapter,
            mock.patch.object(runner, "_validate_adapter_result", side_effect=completed),
        ):
            value = runner.run_queue(self.manifest_path, dry_run=False)
        self.assertEqual(strict.call_count, 2)
        self.assertEqual(adapter.call_count, 2)
        self.assertTrue(
            all(
                row["action"] in {"review_required", "reused"}
                for row in value["results"]
            )
        )
        self.assertEqual(self.output_root.stat().st_mode & 0o777, 0o700)
        self._validate_summary_schema(value)


if __name__ == "__main__":
    unittest.main()
