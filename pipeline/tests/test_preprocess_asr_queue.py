from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import unittest
import wave
from pathlib import Path
from unittest import mock

from jsonschema.validators import Draft202012Validator


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / f"preprocess-asr-queue-{os.getpid()}"
sys.path.insert(0, str(PIPELINE_ROOT))

import preprocess_asr_queue as legacy_queue  # noqa: E402
import preprocess_asr_queue_dispatch as queue_dispatch  # noqa: E402
import preprocess_asr_queue_v03 as queue  # noqa: E402
import asr_whispercpp  # noqa: E402


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


class PreprocessASRQueueTests(unittest.TestCase):
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
        self.output_root.mkdir(mode=0o700)
        self.engine = TEST_ROOT / "whisper-cli"
        self.engine.write_bytes(b"fixture whisper executable\n")
        self.engine.chmod(0o700)
        self.model = TEST_ROOT / "small.en.bin"
        self.model.write_bytes(b"fixture model\n")
        self.engine_sha = digest(self.engine)
        self.model_sha = digest(self.model)
        self.engine_profile = copy.deepcopy(queue.whispercpp_engine_profiles.ENGINE_PROFILES[1])
        self.engine_profile["expected_sha256"] = self.engine_sha
        self.engine_profile["byte_count"] = self.engine.stat().st_size

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
            mock.patch.object(queue, "EXPECTED_MODEL_BYTE_COUNT", self.model.stat().st_size),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        remove_tree()

    def _item(self, ordinal: int, *, digest_value: str | None = None) -> dict[str, object]:
        source_digest = f"{ordinal + 100:064x}"
        run_id = f"run_preprocess_{ordinal:032x}"
        artifact_id = f"artifact_{ordinal:032x}"
        audio_path = TEST_ROOT / f"fixture-audio-{ordinal}.flac"
        audio_path.write_bytes(
            b"shared duplicate audio fixture\n"
            if digest_value is not None
            else f"audio fixture {ordinal}\n".encode()
        )
        audio_path.chmod(0o400)
        audio_digest = digest(audio_path)
        audio_byte_count = audio_path.stat().st_size
        result_path = TEST_ROOT / f"fixture-result-{ordinal}.json"
        source_path = TEST_ROOT / f"fixture-source-{ordinal}.mp4"
        duration = 1_000 + ordinal
        probe = {
            "schema_version": 1,
            "media": {
                "basename": audio_path.name,
                "byte_count": audio_byte_count,
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
        result_ref = {
            "path": str(result_path),
            "uri": result_path.as_uri(),
            "sha256": f"{ordinal + 200:064x}",
            "byte_count": 1_000 + ordinal,
            "job_id": f"preprocess-fixture-{ordinal}",
            "processing_run_id": run_id,
            "recipe_sha256": f"{ordinal + 300:064x}",
        }
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
            "artifact_id": artifact_id,
            "artifact_kind": "audio_16khz_mono_flac",
            "processing_run_id": run_id,
            "media_id": f"media_sha256_{audio_digest}",
            "path": str(audio_path),
            "uri": audio_path.as_uri(),
            "sha256": audio_digest,
            "byte_count": audio_byte_count,
            "duration_ms": duration,
            "normalized_probe": probe,
            "visibility": "private",
        }
        receipt_path = TEST_ROOT / f"receipt-{ordinal}.json"
        return {
            "result": result_ref,
            "source_media": source,
            "audio": audio,
            "asr_eligibility": {
                "eligible": True,
                "reason": None,
                "source_audio_stream_present": True,
                "normalized_audio_operation_enabled": True,
            },
            "routing_hint": "process",
            "evidence": {
                "mode": "sealed_preprocess_receipt",
                "receipt": {
                    "path": str(receipt_path),
                    "uri": receipt_path.as_uri(),
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

    def _acquisition_result(self, source_path: Path, ordinal: int) -> Path:
        media_sha256 = digest(source_path)
        media_id = f"media_sha256_{media_sha256}"
        work_order_sha256 = f"{ordinal:064x}"
        job_id = f"acquisition-asr-ready-fixture-{ordinal:03d}"
        result_path = (
            TEST_ROOT
            / "e2e-acquisition-results"
            / job_id
            / work_order_sha256
            / "result.json"
        )
        result_path.parent.mkdir(parents=True, mode=0o700)
        observed_at = f"2026-08-29T00:00:{ordinal:02d}Z"
        probe = {"format": {"duration_ms": 1_000}}
        result = {
            "schema_version": 1,
            "job_id": job_id,
            "adapter": "local_file",
            "status": "completed",
            "dry_run": False,
            "reused": False,
            "work_order_sha256": work_order_sha256,
            "started_at": "2026-08-29T00:00:00Z",
            "completed_at": observed_at,
            "duration_ms": ordinal,
            "source": {},
            "limits": {},
            "capacity_before": {},
            "capacity_after": {},
            "commands": [],
            "source_observation": {},
            "selected_remote_metadata": {},
            "admission": {
                "media_id": media_id,
                "sha256": media_sha256,
                "byte_count": source_path.stat().st_size,
                "path": str(source_path.resolve()),
                "storage_uri": source_path.resolve().as_uri(),
                "normalized_probe": probe,
            },
            "catalog_records": {
                "sources": [{}],
                "media_objects": [
                    {
                        "media_id": media_id,
                        "sha256": media_sha256,
                        "byte_count": source_path.stat().st_size,
                        "media_kind": "video",
                        "mime_type": None,
                        "container": None,
                        "duration_ms": 1_000,
                        "ffprobe_json": probe,
                        "first_cataloged_at": observed_at,
                        "integrity_state": "verified",
                    }
                ],
                "media_locations": [
                    {
                        "media_id": media_id,
                        "storage_uri": source_path.resolve().as_uri(),
                        "is_primary": 1,
                    }
                ],
                "media_sources": [{}],
            },
            "result_path": str(result_path.resolve()),
            "errors": [],
        }
        result_path.write_bytes(queue.preprocess_batch.pretty_bytes(result))
        return result_path

    def _private_handling(
        self, item: dict[str, object], *, ordinal: int = 1
    ) -> tuple[dict[str, object], dict[str, object]]:
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

    def _build_kwargs(self) -> dict[str, object]:
        return {
            "preprocess_bundle": self.bundle,
            "preprocess_state_root": self.state,
            "queue_root": self.queue_root,
            "asr_output_root": self.output_root,
            "engine_path": self.engine,
            "model_path": self.model,
        }

    def test_historical_v01_v02_source_and_schema_pins_are_immutable(self) -> None:
        source_v01 = PIPELINE_ROOT / "preprocess_asr_queue_v01.py"
        schema_v01 = (
            PIPELINE_ROOT / "schemas/preprocess-asr-queue-manifest-v01.schema.json"
        )
        source = Path(legacy_queue.__file__).resolve()
        schema = PIPELINE_ROOT / "schemas/preprocess-asr-queue-manifest.schema.json"
        self.assertEqual(70_900, source_v01.stat().st_size)
        self.assertEqual(
            "ba93a330484c5f34c65df5498b4417a9baa1e498ad6d96ff46c21b4205b97a2b",
            digest(source_v01),
        )
        self.assertEqual(26_309, schema_v01.stat().st_size)
        self.assertEqual(
            "ab4ee8dfe3b542fe197e5fbf9bae14ac0ea77a38b288e5fd3c959733c6db5077",
            digest(schema_v01),
        )
        self.assertEqual(86_523, source.stat().st_size)
        self.assertEqual(
            "e0fffe5af2f403cd46fff82bde452f81fabb1d165f82dffcb052206d00b0fe87",
            digest(source),
        )
        self.assertEqual(31_540, schema.stat().st_size)
        self.assertEqual(
            "f8b5f1debca171b5dd58f2d5e6bcc007489e389d2e2617c581bcff0eaabfdffa",
            digest(schema),
        )

    def test_dispatch_is_exact_by_version_and_new_materialization_uses_v03(self) -> None:
        expected = {
            "0.2.0": queue_dispatch.queue_v02.main,
            "0.3.0": queue_dispatch.queue_v03.main,
        }
        for suffix, (version, validator) in enumerate(expected.items(), 1):
            manifest = TEST_ROOT / f"dispatch-{suffix}.json"
            manifest.write_bytes(queue.pretty_bytes({"implementation_version": version}))
            manifest.chmod(0o400)
            self.assertIs(
                validator,
                queue_dispatch.select_validator(
                    ["validate", "--manifest", str(manifest)]
                ),
            )

        for suffix, version in enumerate(("0.1.0", "99.0.0"), 3):
            manifest = TEST_ROOT / f"dispatch-{suffix}.json"
            manifest.write_bytes(queue.pretty_bytes({"implementation_version": version}))
            manifest.chmod(0o400)
            with self.assertRaises(queue_dispatch.DispatchError):
                queue_dispatch.select_validator(
                    ["validate", "--manifest", str(manifest)]
                )

        with mock.patch.object(
            queue_dispatch.queue_v03, "main", return_value=17
        ) as current_main:
            self.assertEqual(17, queue_dispatch.main(["materialize", "--help"]))
        current_main.assert_called_once_with(["materialize", "--help"])

    def test_dispatch_rejects_fifo_without_blocking(self) -> None:
        fifo = TEST_ROOT / "manifest.fifo"
        os.mkfifo(fifo, 0o600)
        with self.assertRaisesRegex(
            queue_dispatch.DispatchError, "bounded regular file"
        ):
            queue_dispatch.select_validator(
                ["validate", "--manifest", str(fifo)]
            )

    def test_materialization_is_deterministic_private_and_artifact_local(self) -> None:
        items = [self._item(2), self._item(1)]
        with mock.patch.object(
            queue, "_collect_sealed_items", return_value=(self._origin(2), items)
        ):
            first, manifest_path = queue.materialize_queue(**self._build_kwargs())
            second, replay_path = queue.materialize_queue(**self._build_kwargs())
            validated, orders = queue.validate_queue(manifest_path)
        self.assertEqual(first, second)
        self.assertEqual(first, validated)
        self.assertEqual(manifest_path, replay_path)
        self.assertEqual(first["work_order_count"], 2)
        self.assertEqual(stat_mode(manifest_path), 0o400)
        self.assertEqual(stat_mode(manifest_path.parent), 0o500)
        self.assertTrue(all(order["catalog_context"] is None for order in orders))
        self.assertTrue(all(order["window"]["offset_ms"] == 0 for order in orders))
        self.assertTrue(
            all(
                entry["coordinate_provenance"]["recording_transform_state"]
                == "unresolved"
                for entry in first["work_orders"]
            )
        )
        self.assertEqual(
            [entry["audio_artifact"]["sha256"] for entry in first["work_orders"]],
            sorted(entry["audio_artifact"]["sha256"] for entry in first["work_orders"]),
        )
        schema = json.loads(
            (PIPELINE_ROOT / "schemas/preprocess-asr-queue-manifest-v03.schema.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)
        errors = list(Draft202012Validator(schema).iter_errors(first))
        self.assertEqual(errors, [], [error.message for error in errors])

    def test_asr_ready_batch_receipts_admit_audio_and_explicitly_skip_no_audio(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        sources = TEST_ROOT / "e2e-sources"
        sources.mkdir(mode=0o700)
        audio_source = sources / "speech.wav"
        with wave.open(str(audio_source), "wb") as handle:
            handle.setnchannels(1)
            handle.setsampwidth(2)
            handle.setframerate(16_000)
            handle.writeframes(b"\x00\x00" * 16_000)
        video_source = sources / "video-only.mp4"
        completed = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=black:s=160x120:r=10:d=1",
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-pix_fmt",
                "yuv420p",
                str(video_source),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr.decode(errors="replace"))

        acquisition_results = [
            self._acquisition_result(audio_source, 1),
            self._acquisition_result(video_source, 2),
        ]
        selection_path = TEST_ROOT / "e2e-selection" / "selection.json"
        queue.preprocess_batch.write_selection(acquisition_results, selection_path)
        preprocess_bundle = queue.preprocess_batch.materialize_bundle(
            selection_path,
            TEST_ROOT / "e2e-preprocess-bundles",
            TEST_ROOT / "e2e-preprocess-output",
            operation_profile="asr-ready",
        )
        run_summary = queue.preprocess_batch.run_batch(
            preprocess_bundle,
            self.state,
            limit=2,
        )
        self.assertEqual("complete", run_summary["status"])

        kwargs = self._build_kwargs()
        kwargs["preprocess_bundle"] = preprocess_bundle
        manifest, manifest_path = queue.materialize_queue(**kwargs)
        validated, documents = queue.validate_queue(manifest_path)
        self.assertEqual(manifest, validated)
        self.assertEqual(1, manifest["work_order_count"])
        self.assertEqual(1, len(documents))
        skipped = manifest["origin"]["ineligible_receipts"]
        self.assertEqual(1, len(skipped))
        self.assertEqual("source_has_no_audio", skipped[0]["reason"])
        self.assertFalse(skipped[0]["source_audio_stream_present"])
        self.assertTrue(skipped[0]["normalized_audio_operation_enabled"])
        self.assertEqual(2, manifest["origin"]["receipt_count"])

        schema = json.loads(
            (PIPELINE_ROOT / "schemas/preprocess-asr-queue-manifest-v03.schema.json").read_text(
                encoding="utf-8"
            )
        )
        errors = list(Draft202012Validator(schema).iter_errors(manifest))
        self.assertEqual([], errors, [error.message for error in errors])

        eligible_entry = manifest["work_orders"][0]
        normalized_audio = Path(eligible_entry["audio_artifact"]["path"])
        normalized_audio.chmod(0o600)
        normalized_audio.unlink()
        with self.assertRaisesRegex(
            queue.QueueError, "contract failed|audio artifact"
        ):
            queue._validated_result_item(
                Path(eligible_entry["preprocess_result"]["path"])
            )

    def test_exact_replay_rejects_a_mutated_work_order(self) -> None:
        items = [self._item(1)]
        with mock.patch.object(
            queue, "_collect_sealed_items", return_value=(self._origin(1), items)
        ):
            _manifest, manifest_path = queue.materialize_queue(**self._build_kwargs())
            work_order = manifest_path.parent / "work-orders/000001.json"
            work_order.chmod(0o600)
            body = work_order.read_bytes()
            work_order.write_bytes(body + b"\n")
            work_order.chmod(0o400)
            with self.assertRaises(queue.QueueError):
                queue.validate_queue(manifest_path)

    def test_unsafe_queue_roots_are_rejected_before_any_write(self) -> None:
        items = [self._item(1)]
        kwargs = self._build_kwargs()
        kwargs["queue_root"] = REPOSITORY_ROOT / "public" / "forbidden-asr-queue"
        with mock.patch.object(
            queue, "_collect_sealed_items", return_value=(self._origin(1), items)
        ):
            with self.assertRaises(queue.QueueError):
                queue.build_queue(**kwargs)
        self.assertFalse((REPOSITORY_ROOT / "public" / "forbidden-asr-queue").exists())

        nested = self.bundle / "forbidden-asr-queue"
        kwargs["queue_root"] = nested
        with mock.patch.object(
            queue, "_collect_sealed_items", return_value=(self._origin(1), items)
        ):
            with self.assertRaisesRegex(queue.QueueError, "may not overlap"):
                queue.build_queue(**kwargs)
        self.assertFalse(nested.exists())

    def test_policy_bearing_queue_wraps_and_replays_exact_private_lineage(self) -> None:
        item = self._item(1)
        control, boundary = self._private_handling(item)
        origin = self._origin(1)
        origin["handling_control"] = control
        with (
            mock.patch.object(
                queue, "_collect_sealed_items", return_value=(origin, [item])
            ),
            mock.patch.object(
                queue.preprocess_batch,
                "replay_handling_boundary",
                return_value=boundary,
            ) as replay,
        ):
            manifest, manifest_path = queue.materialize_queue(**self._build_kwargs())
            validated, documents = queue.validate_queue(manifest_path)

        self.assertEqual(validated, manifest)
        self.assertEqual(manifest["handling_control"], control)
        self.assertTrue(manifest["safety"]["private_acquisition_seal_required"])
        entry = manifest["work_orders"][0]
        self.assertEqual(entry["handling"]["handling_boundary"], boundary)
        wrapper = json.loads(
            (manifest_path.parent / entry["path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(wrapper["kind"], queue.PRIVATE_WORK_ORDER_KIND)
        self.assertEqual(wrapper["handling"], entry["handling"])
        self.assertEqual(wrapper, documents[0])
        inner_order = wrapper["asr_work_order"]
        self.assertEqual(
            entry["canonical_sha256"],
            queue.sha256_bytes(queue.canonical_bytes(wrapper)),
        )
        self.assertEqual(
            entry["adapter_work_order_sha256"],
            queue.sha256_bytes(queue.canonical_bytes(inner_order)),
        )
        self.assertNotEqual(
            entry["canonical_sha256"], entry["adapter_work_order_sha256"]
        )
        with self.assertRaises(asr_whispercpp.ASRError):
            asr_whispercpp.validate_work_order(wrapper)
        self.assertGreaterEqual(replay.call_count, 2)

        schema = json.loads(
            (PIPELINE_ROOT / "schemas/preprocess-asr-queue-manifest-v03.schema.json").read_text(
                encoding="utf-8"
            )
        )
        errors = list(Draft202012Validator(schema).iter_errors(manifest))
        self.assertEqual(errors, [], [error.message for error in errors])

    def test_handling_control_is_subset_to_asr_eligible_private_receipts(self) -> None:
        item = self._item(1)
        eligible_control, boundary = self._private_handling(item, ordinal=1)
        skipped_entry = copy.deepcopy(eligible_control["entries"][0])
        skipped_entry["ordinal"] = 2
        skipped_entry["entry_id"] = "pbe_" + "2" * 32
        full_core = {
            "kind": "v30_private_acquisition_handling_control",
            "private_entry_count": 2,
            "entries": [eligible_control["entries"][0], skipped_entry],
            "seal_receipt_replay_required": True,
            "handling_policy_propagation_required": True,
            "publication_authority": "none",
        }
        origin = self._origin(2)
        origin["handling_control"] = {
            **full_core,
            "identity_sha256": queue.sha256_bytes(queue.canonical_bytes(full_core)),
        }
        origin["ineligible_receipts"] = [{"ordinal": 2}]
        with mock.patch.object(
            queue.preprocess_batch,
            "replay_handling_boundary",
            return_value=boundary,
        ):
            subset = queue._queue_handling_control(origin, [item])
        self.assertEqual(eligible_control, subset)

    def test_sealed_collector_carries_receipt_boundary_into_queue_origin(self) -> None:
        item = self._item(1)
        control, boundary = self._private_handling(item)
        item.pop("handling")
        manifest = {
            "bundle_id": "ppbatch_" + "b" * 32,
            "identity_sha256": "c" * 64,
            "manifest_sha256": "d" * 64,
            "work_order_count": 1,
            "handling_control": control,
        }
        manifest_path = self.bundle / "manifest.json"
        manifest_path.write_bytes(queue.pretty_bytes(manifest))
        manifest_path.chmod(0o400)
        result_ref = item["result"]
        audio = item["audio"]
        receipt = {
            "receipt_id": "ppreceipt_" + "1" * 32,
            "receipt_sha256": "2" * 64,
            "entry_id": control["entries"][0]["entry_id"],
            "preprocess_result": {
                key: result_ref[key]
                for key in (
                    "path", "sha256", "byte_count", "processing_run_id",
                    "recipe_sha256",
                )
            },
            "artifacts": [
                {
                    "artifact_kind": "audio_16khz_mono_flac",
                    "artifact_id": audio["artifact_id"],
                    "path": audio["path"],
                    "sha256": audio["sha256"],
                    "byte_count": audio["byte_count"],
                    "storage_uri": audio["uri"],
                }
            ],
            "source_media": item["source_media"],
            "handling_boundary": boundary,
        }
        receipts = {
            1: {"receipt": receipt, "physical_sha256": "3" * 64}
        }
        with (
            mock.patch.object(
                queue.preprocess_batch,
                "validate_bundle",
                return_value=(manifest, {"entries": []}, []),
            ),
            mock.patch.object(
                queue.preprocess_batch, "existing_receipts", return_value=receipts
            ),
            mock.patch.object(queue, "_validated_result_item", return_value=item),
            mock.patch.object(
                queue.preprocess_batch,
                "replay_handling_boundary",
                return_value=boundary,
            ) as replay,
        ):
            origin, collected = queue._collect_sealed_items(self.bundle, self.state)

        self.assertEqual(origin["handling_control"], control)
        self.assertEqual(
            collected[0]["handling"]["handling_boundary"], boundary
        )
        self.assertEqual(
            collected[0]["handling"]["preprocess_control_entry"],
            control["entries"][0],
        )
        replay.assert_called_once_with(boundary)

    def test_policy_bearing_item_cannot_drop_or_weaken_its_control(self) -> None:
        item = self._item(1)
        control, boundary = self._private_handling(item)
        origin = self._origin(1)
        origin["handling_control"] = control
        dropped = copy.deepcopy(item)
        dropped.pop("handling")
        with (
            mock.patch.object(
                queue, "_collect_sealed_items", return_value=(origin, [dropped])
            ),
            self.assertRaisesRegex(queue.QueueError, "drops its private handling"),
        ):
            queue.build_queue(**self._build_kwargs())

        item["handling"]["handling_boundary"]["handling_policy"][
            "publication_disposition"
        ] = "never_publish"
        with (
            mock.patch.object(
                queue, "_collect_sealed_items", return_value=(origin, [item])
            ),
            mock.patch.object(
                queue.preprocess_batch,
                "replay_handling_boundary",
                return_value=boundary,
            ),
            self.assertRaisesRegex(queue.QueueError, "differs from exact seal replay"),
        ):
            queue.build_queue(**self._build_kwargs())

    def test_duplicate_audio_content_fails_closed(self) -> None:
        duplicate = "1" * 64
        items = [self._item(1, digest_value=duplicate), self._item(2, digest_value=duplicate)]
        with mock.patch.object(
            queue, "_collect_sealed_items", return_value=(self._origin(2), items)
        ):
            with self.assertRaisesRegex(queue.QueueError, "duplicate audio content"):
                queue.build_queue(**self._build_kwargs())

    def test_all_ineligible_receipts_fail_with_explicit_skip_reasons(self) -> None:
        origin = self._origin(1)
        origin["ineligible_receipts"] = [
            {
                "reason": "source_has_no_audio",
                "preprocess_result": {"path": str(TEST_ROOT / "no-audio-result.json")},
                "source_media": {"path": str(TEST_ROOT / "no-audio-source.mp4")},
            }
        ]
        with (
            mock.patch.object(
                queue, "_collect_sealed_items", return_value=(origin, [])
            ),
            self.assertRaisesRegex(queue.QueueError, "no ASR-eligible.*explicit skips"),
        ):
            queue.build_queue(**self._build_kwargs())

    def test_new_queue_rejects_a_legacy_replay_only_engine_profile(self) -> None:
        items = [self._item(1)]
        legacy = copy.deepcopy(self.engine_profile)
        legacy["profile_id"] = "fixture-legacy-profile"
        legacy["admission"] = "legacy_manifest_replay_only"
        legacy["output_json_full_utf8_token_boundary_merge"] = False
        with (
            mock.patch.object(
                queue, "_collect_sealed_items", return_value=(self._origin(1), items)
            ),
            mock.patch.object(
                queue.whispercpp_engine_profiles,
                "match_engine_profile",
                return_value=legacy,
            ),
        ):
            with self.assertRaisesRegex(queue.QueueError, "not eligible for new UTF-8-safe"):
                queue.build_queue(**self._build_kwargs())

    def test_catalog_binding_is_exact_and_does_not_grant_coordinates(self) -> None:
        item = self._catalog_item_and_database()
        connection = queue._open_readonly_catalog(self.database)
        try:
            binding = queue._catalog_binding(connection, item)
        finally:
            connection.execute("ROLLBACK")
            connection.close()

        self.assertRegex(binding["binding_sha256"], r"^[0-9a-f]{64}$")
        queue_schema = json.loads(
            (PIPELINE_ROOT / "schemas/preprocess-asr-queue-manifest-v03.schema.json").read_text(
                encoding="utf-8"
            )
        )
        binding_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$ref": "#/$defs/catalogBinding",
            "$defs": queue_schema["$defs"],
        }
        binding_errors = list(Draft202012Validator(binding_schema).iter_errors(binding))
        self.assertEqual(
            binding_errors, [], [error.message for error in binding_errors]
        )
        self.assertEqual(binding["snapshot"]["rendition_count"], 0)
        self.assertEqual(binding["snapshot"]["timeline_span_count"], 0)
        self.assertEqual(
            binding["snapshot"]["source_media"]["mime_type"], "video/mp4"
        )
        self.assertIsNone(
            binding["snapshot"]["producer_source_media"]["mime_type"]
        )
        self.assertEqual(
            binding["snapshot"]["source_location"]["storage_class"],
            "local_hot_cache",
        )
        self.assertEqual(
            binding["snapshot"]["producer_source_location"]["storage_class"],
            "local",
        )

        writable = sqlite3.connect(self.database)
        writable.execute(
            "UPDATE artifacts SET sha256 = ? WHERE artifact_id = ?",
            ("f" * 64, item["audio"]["artifact_id"]),
        )
        writable.commit()
        writable.close()
        connection = queue._open_readonly_catalog(self.database)
        try:
            with self.assertRaisesRegex(queue.QueueError, "catalog artifact differs"):
                queue._catalog_binding(connection, item)
        finally:
            connection.execute("ROLLBACK")
            connection.close()

        writable = sqlite3.connect(self.database)
        writable.execute(
            "UPDATE artifacts SET sha256 = ? WHERE artifact_id = ?",
            (item["audio"]["sha256"], item["audio"]["artifact_id"]),
        )
        writable.execute(
            "UPDATE media_objects SET sha256 = ? WHERE media_id = ?",
            ("e" * 64, item["source_media"]["media_id"]),
        )
        writable.commit()
        writable.close()
        connection = queue._open_readonly_catalog(self.database)
        try:
            with self.assertRaisesRegex(queue.QueueError, "catalog source media differs"):
                queue._catalog_binding(connection, item)
        finally:
            connection.execute("ROLLBACK")
            connection.close()

    def _catalog_item_and_database(self) -> dict[str, object]:
        audio_digest = "1" * 64
        source_digest = "2" * 64
        artifact_id = "artifact_" + "3" * 32
        run_id = "run_preprocess_" + "4" * 32
        audio_media_id = f"media_sha256_{audio_digest}"
        source_media_id = f"media_sha256_{source_digest}"
        audio_path = TEST_ROOT / "catalog-audio.flac"
        source_path = TEST_ROOT / "catalog-source.mp4"
        audio_path.write_bytes(b"catalog audio fixture")
        source_path.write_bytes(b"catalog source fixture")
        audio_path.chmod(0o400)
        source_path.chmod(0o400)
        actual_digest = digest(audio_path)
        audio_digest = actual_digest
        audio_media_id = f"media_sha256_{audio_digest}"
        duration = 1234
        probe = {
            "schema_version": 1,
            "media": {
                "basename": audio_path.name,
                "byte_count": audio_path.stat().st_size,
                "media_id": audio_media_id,
                "sha256": audio_digest,
            },
            "primary_streams": {"audio_index": 0, "video_index": None},
            "format": {"format_name": "flac", "start_ms": 0, "duration_ms": duration},
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
                        "sample_rate_hz": 16000,
                    },
                }
            ],
        }
        run = {
            "processing_run_id": run_id,
            "stage": "media_preprocess",
            "implementation_version": "0.3.3",
            "parameters_json": {"fixture": True},
            "environment_json": {"cpu_only": True},
            "started_at": "2026-08-27T00:00:00Z",
            "completed_at": "2026-08-27T00:00:01Z",
            "status": "completed",
        }
        artifact_row = {
            "artifact_id": artifact_id,
            "processing_run_id": run_id,
            "artifact_kind": "audio_16khz_mono_flac",
            "storage_uri": audio_path.as_uri(),
            "sha256": audio_digest,
            "byte_count": audio_path.stat().st_size,
            "schema_version": 1,
            "visibility": "private",
        }
        media_row = {
            "media_id": audio_media_id,
            "sha256": audio_digest,
            "byte_count": audio_path.stat().st_size,
            "media_kind": "audio",
            "mime_type": "audio/flac",
            "container": "flac",
            "duration_ms": duration,
            "ffprobe_json": probe,
            "first_cataloged_at": "2026-08-27T00:00:00Z",
            "integrity_state": "verified",
        }
        source_probe = {
            "schema_version": 1,
            "media": {
                "basename": source_path.name,
                "byte_count": source_path.stat().st_size,
                "media_id": source_media_id,
                "sha256": source_digest,
            },
            "primary_streams": {"audio_index": 1, "video_index": 0},
            "format": {"format_name": "mp4", "start_ms": 0, "duration_ms": 2_000},
            "streams": [],
        }
        catalog_source_probe = {
            "schema_version": 1,
            "tool": {"name": "ffprobe", "version": "fixture legacy probe"},
            "format": {"format_name": "mp4", "duration_ms": 2_000},
            "streams": [],
        }
        source_media_row = {
            "media_id": source_media_id,
            "sha256": source_digest,
            "byte_count": source_path.stat().st_size,
            "media_kind": "video",
            "mime_type": None,
            "container": "mp4",
            "duration_ms": 2_000,
            "ffprobe_json": source_probe,
            "first_cataloged_at": "2026-08-27T00:00:00Z",
            "integrity_state": "verified",
        }
        source_location_row = {
            "media_location_id": "mlc_source_fixture",
            "media_id": source_media_id,
            "storage_uri": source_path.as_uri(),
            "storage_class": "local",
            "verified_at": "2026-08-27T00:00:01Z",
            "is_primary": 1,
        }
        item = {
            "audio": {
                "artifact_id": artifact_id,
                "processing_run_id": run_id,
                "media_id": audio_media_id,
                "path": str(audio_path),
                "uri": audio_path.as_uri(),
                "sha256": audio_digest,
                "byte_count": audio_path.stat().st_size,
                "duration_ms": duration,
                "normalized_probe": probe,
            },
            "source_media": {
                "media_id": source_media_id,
                "sha256": source_digest,
                "byte_count": source_path.stat().st_size,
                "path": str(source_path),
                "storage_uri": source_path.as_uri(),
                "duration_ms": 2_000,
                "first_cataloged_at": "2026-08-27T00:00:00Z",
            },
            "raw_result": {
                "processing_run": run,
                "catalog_records": {
                    "artifacts": [artifact_row],
                    "media_objects": [source_media_row, media_row],
                    "media_locations": [source_location_row],
                },
            },
        }
        self.database = TEST_ROOT / "catalog.sqlite3"
        connection = sqlite3.connect(self.database)
        connection.executescript(
            """
            CREATE TABLE artifacts(
              artifact_id TEXT PRIMARY KEY, processing_run_id TEXT, artifact_kind TEXT,
              storage_uri TEXT, sha256 TEXT, byte_count INTEGER, schema_version INTEGER,
              visibility TEXT, metadata_json TEXT
            );
            CREATE TABLE processing_runs(
              processing_run_id TEXT PRIMARY KEY, stage TEXT, implementation_version TEXT,
              model_id TEXT, glossary_revision_id TEXT, parameters_json TEXT,
              environment_json TEXT, random_seed INTEGER, started_at TEXT,
              completed_at TEXT, status TEXT, error_text TEXT
            );
            CREATE TABLE run_inputs(
              run_input_id TEXT PRIMARY KEY, processing_run_id TEXT, object_type TEXT,
              object_id TEXT, input_role TEXT, input_sha256 TEXT
            );
            CREATE TABLE media_objects(
              media_id TEXT PRIMARY KEY, sha256 TEXT, byte_count INTEGER, media_kind TEXT,
              mime_type TEXT, container TEXT, duration_ms INTEGER, ffprobe_json TEXT,
              first_cataloged_at TEXT, integrity_state TEXT
            );
            CREATE TABLE media_locations(
              media_location_id TEXT PRIMARY KEY, media_id TEXT, storage_uri TEXT,
              storage_class TEXT, verified_at TEXT, is_primary INTEGER
            );
            CREATE TABLE media_derivations(
              parent_media_id TEXT, child_media_id TEXT, derivation_kind TEXT,
              processing_run_id TEXT, metadata_json TEXT
            );
            CREATE TABLE renditions(
              rendition_id TEXT PRIMARY KEY, recording_id TEXT, media_id TEXT,
              rendition_kind TEXT, label TEXT, review_state TEXT, metadata_json TEXT
            );
            CREATE TABLE timeline_map_spans(
              timeline_map_span_id TEXT PRIMARY KEY, rendition_id TEXT, ordinal INTEGER,
              media_start_ms INTEGER, media_end_ms INTEGER, recording_start_ms INTEGER,
              recording_end_ms INTEGER, confidence_state TEXT
            );
            """
        )
        connection.execute(
            "INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?,?)",
            (*artifact_row.values(), queue.canonical_bytes({
                "media_kind": "audio",
                "mime_type": "audio/flac",
                "normalized_probe": probe,
            }).decode()),
        )
        connection.execute(
            "INSERT INTO processing_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                run_id, "media_preprocess", "0.3.3", None, None,
                queue.canonical_bytes(run["parameters_json"]).decode(),
                queue.canonical_bytes(run["environment_json"]).decode(),
                None, run["started_at"], run["completed_at"], "completed", None,
            ),
        )
        connection.execute(
            "INSERT INTO run_inputs VALUES(?,?,?,?,?,?)",
            ("rin_fixture", run_id, "media", source_media_id, "source_media", source_digest),
        )
        connection.execute(
            "INSERT INTO media_objects VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                source_media_id, source_digest, source_path.stat().st_size, "video",
                "video/mp4", "mp4", 2_000,
                queue.canonical_bytes(catalog_source_probe).decode(),
                source_media_row["first_cataloged_at"], "verified",
            ),
        )
        connection.execute(
            "INSERT INTO media_objects VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                audio_media_id, audio_digest, audio_path.stat().st_size, "audio",
                "audio/flac", "flac", duration, queue.canonical_bytes(probe).decode(),
                media_row["first_cataloged_at"], "verified",
            ),
        )
        connection.execute(
            "INSERT INTO media_locations VALUES(?,?,?,?,?,?)",
            (
                source_location_row["media_location_id"], source_media_id,
                source_path.as_uri(), "local_hot_cache",
                source_location_row["verified_at"], 1,
            ),
        )
        connection.execute(
            "INSERT INTO media_locations VALUES(?,?,?,?,?,?)",
            ("mlc_fixture", audio_media_id, audio_path.as_uri(), "local_derived",
             "2026-08-27T00:00:01Z", 1),
        )
        connection.execute(
            "INSERT INTO media_derivations VALUES(?,?,?,?,?)",
            (source_media_id, audio_media_id, "audio_normalization_16khz_mono_flac",
             run_id, "{}"),
        )
        connection.commit()
        connection.close()
        return item


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


if __name__ == "__main__":
    unittest.main()
