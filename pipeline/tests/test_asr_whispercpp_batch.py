from __future__ import annotations

import hashlib
import copy
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / f"asr-whispercpp-batch-{os.getpid()}"
sys.path.insert(0, str(PIPELINE_ROOT))

import asr_whispercpp_batch as batch  # noqa: E402


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def run(command: list[str]) -> None:
    subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    )


def generate_audio(path: Path, frequency: int) -> None:
    run(
        [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency={frequency}:sample_rate=16000:duration=1",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-sample_fmt",
            "s16",
            "-c:a",
            "flac",
            str(path),
        ]
    )


def stable_artifact_id(bundle_id: str, window_id: str, kind: str, sha256: str) -> str:
    return "artifact_" + hashlib.sha256(
        "\x1f".join((bundle_id, window_id, kind, sha256)).encode("utf-8")
    ).hexdigest()[:32]


def remove_test_tree() -> None:
    if not TEST_ROOT.exists():
        return
    for current, directories, files in os.walk(TEST_ROOT):
        Path(current).chmod(0o700)
        for name in directories:
            (Path(current) / name).chmod(0o700)
        for name in files:
            path = Path(current) / name
            if not path.is_symlink():
                path.chmod(0o600)
    shutil.rmtree(TEST_ROOT)


class WhisperCppBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("ffmpeg and ffprobe are required")
        remove_test_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        os.chmod(TEST_ROOT, 0o700)
        self.engine = TEST_ROOT / "whisper-cli"
        self.engine.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        self.engine.chmod(0o700)
        self.model = TEST_ROOT / "ggml-small.en.bin"
        self.model.write_bytes(b"fixture-small-en-model\n")
        self.engine_sha = digest(self.engine)
        self.model_sha = digest(self.model)
        self.original_engine_profiles = copy.deepcopy(
            batch.whispercpp_engine_profiles.ENGINE_PROFILES
        )
        current_profile = copy.deepcopy(
            next(
                profile
                for profile in self.original_engine_profiles
                if profile["admission"] == "current_new_batch"
            )
        )
        current_profile["expected_sha256"] = self.engine_sha
        current_profile["byte_count"] = self.engine.stat().st_size
        self.patchers = [
            mock.patch.object(
                batch.whispercpp_engine_profiles,
                "ENGINE_PROFILES",
                (current_profile,),
            ),
            mock.patch.object(batch, "EXPECTED_MODEL_SHA256", self.model_sha),
            mock.patch.object(batch, "EXPECTED_MODEL_BYTE_COUNT", self.model.stat().st_size),
        ]
        for patcher in self.patchers:
            patcher.start()

        self.bundle_id = "windowbundle_" + "a" * 32
        self.results = [self._make_result(1, 5_000, 6_000, 440), self._make_result(2, 6_000, 7_000, 550)]
        self.database = TEST_ROOT / "catalog.sqlite3"
        self._create_catalog()
        self.batch_root = TEST_ROOT / "private-work-orders"
        self.output_root = TEST_ROOT / "private-asr-results"

    def tearDown(self) -> None:
        for patcher in reversed(getattr(self, "patchers", [])):
            patcher.stop()
        remove_test_tree()

    def _make_result(self, ordinal: int, start_ms: int, end_ms: int, frequency: int) -> dict[str, object]:
        window_id = f"window_{ordinal:06d}"
        directory = TEST_ROOT / "sealed-windows" / self.bundle_id / window_id
        directory.mkdir(parents=True, mode=0o700)
        audio_path = directory / "audio-16khz-mono.flac"
        generate_audio(audio_path, frequency)
        audio_sha = digest(audio_path)
        audio_bytes = audio_path.stat().st_size
        artifact_id = stable_artifact_id(
            self.bundle_id,
            window_id,
            "window_audio_16khz_mono_flac",
            audio_sha,
        )
        window = {
            "boundary": "half_open",
            "end_ms": end_ms,
            "is_partial_tail": False,
            "ordinal": ordinal,
            "start_ms": start_ms,
            "window_id": window_id,
        }
        mapping = {
            "artifact_zero_maps_to_source_ms": start_ms,
            "boundary": "half_open",
            "byte_exact_source_fragment": False,
            "coordinate_precision": "integer_millisecond_contract",
            "extraction_method": "ffmpeg_accurate_seek_transcode",
            "source_end_ms": end_ms,
            "source_start_ms": start_ms,
        }
        probe = {
            "audio": {
                "channels": 1,
                "codec_name": "flac",
                "sample_format": "s16",
                "sample_rate_hz": 16_000,
            },
            "audio_stream_index": 0,
            "duration_ms": 1_000,
            "video": None,
            "video_stream_index": None,
        }
        result_path = directory / "result.json"
        value = {
            "schema_version": 1,
            "implementation_version": "0.1.0",
            "status": "completed",
            "dry_run": False,
            "job_id": f"local-window-fixture-{ordinal}",
            "bundle_id": self.bundle_id,
            "work_order_sha256": f"{ordinal:064x}",
            "source": {"fixture": True},
            "window": window,
            "tools": {"fixture": True},
            "profile": {"profile_id": "long-window-cpu-v1"},
            "limits": {"timeout_seconds": 7_200},
            "commands": [["ffmpeg", "fixture"]],
            "artifacts": [
                {
                    "artifact_id": artifact_id,
                    "artifact_kind": "window_audio_16khz_mono_flac",
                    "byte_count": audio_bytes,
                    "normalized_probe": probe,
                    "path": str(audio_path),
                    "sha256": audio_sha,
                    "visibility": "private",
                }
            ],
            "time_mapping": mapping,
            "safety": batch.EXPECTED_SAFETY,
            "result_path": str(result_path),
        }
        result_path.write_bytes(batch.pretty_bytes(value))
        audio_path.chmod(0o400)
        result_path.chmod(0o400)
        directory.chmod(0o500)
        return {
            "path": result_path,
            "value": value,
            "result_sha": digest(result_path),
            "audio_path": audio_path,
            "audio_sha": audio_sha,
            "audio_bytes": audio_bytes,
            "artifact_id": artifact_id,
            "probe": probe,
            "window": window,
            "mapping": mapping,
        }

    def _create_catalog(self) -> None:
        connection = sqlite3.connect(self.database)
        connection.executescript(
            """
            CREATE TABLE models (
              model_id TEXT PRIMARY KEY, task TEXT, name TEXT, version TEXT,
              weights_sha256 TEXT, license_label TEXT, configuration_json TEXT
            );
            CREATE TABLE model_registry_manifest_imports (
              manifest_id TEXT PRIMARY KEY, input_sha256 TEXT, schema_version INTEGER,
              manifest_created_at TEXT, imported_at TEXT, registered_by TEXT,
              basis TEXT, model_count INTEGER
            );
            CREATE TABLE model_registry_manifest_models (
              manifest_id TEXT, ordinal INTEGER, model_id TEXT, model_snapshot_json TEXT
            );
            CREATE TABLE artifacts (
              artifact_id TEXT PRIMARY KEY, processing_run_id TEXT, artifact_kind TEXT,
              storage_uri TEXT, sha256 TEXT, byte_count INTEGER, schema_version INTEGER,
              visibility TEXT, metadata_json TEXT
            );
            CREATE TABLE processing_runs (
              processing_run_id TEXT PRIMARY KEY, stage TEXT, implementation_version TEXT,
              model_id TEXT, glossary_revision_id TEXT, parameters_json TEXT,
              environment_json TEXT, random_seed INTEGER, started_at TEXT,
              completed_at TEXT, status TEXT, error_text TEXT
            );
            CREATE TABLE media_objects (
              media_id TEXT PRIMARY KEY, sha256 TEXT, byte_count INTEGER, media_kind TEXT,
              mime_type TEXT, container TEXT, duration_ms INTEGER, ffprobe_json TEXT,
              first_cataloged_at TEXT, integrity_state TEXT
            );
            CREATE TABLE media_locations (
              media_location_id TEXT PRIMARY KEY, media_id TEXT, storage_uri TEXT,
              storage_class TEXT, verified_at TEXT, is_primary INTEGER
            );
            CREATE TABLE renditions (
              rendition_id TEXT PRIMARY KEY, recording_id TEXT, media_id TEXT,
              rendition_kind TEXT, label TEXT, review_state TEXT, metadata_json TEXT
            );
            CREATE TABLE recordings (
              recording_id TEXT PRIMARY KEY, canonical_key TEXT, slug TEXT, title TEXT,
              date_label TEXT, date_year INTEGER, date_basis TEXT, duration_ms INTEGER,
              recording_type TEXT, review_state TEXT, merged_into_recording_id TEXT,
              metadata_json TEXT, created_at TEXT, updated_at TEXT,
              current_metadata_observation_id TEXT
            );
            CREATE TABLE timeline_map_spans (
              timeline_map_span_id TEXT PRIMARY KEY, rendition_id TEXT, ordinal INTEGER,
              media_start_ms INTEGER, media_end_ms INTEGER, recording_start_ms INTEGER,
              recording_end_ms INTEGER, mapping_kind TEXT, confidence_state TEXT
            );
            CREATE TABLE media_derivations (
              child_media_id TEXT, parent_media_id TEXT, derivation_kind TEXT,
              processing_run_id TEXT, metadata_json TEXT
            );
            """
        )
        model_configuration = {"source": batch.EXPECTED_MODEL["source"]}
        connection.execute(
            "INSERT INTO models VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                batch.EXPECTED_MODEL["model_id"],
                "asr",
                batch.EXPECTED_MODEL["name"],
                batch.EXPECTED_MODEL["revision"],
                self.model_sha,
                batch.EXPECTED_MODEL["license_label"],
                json.dumps(model_configuration, sort_keys=True),
            ),
        )
        registry = batch.EXPECTED_REGISTRY
        connection.execute(
            "INSERT INTO model_registry_manifest_imports VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(registry[key] for key in (
                "manifest_id", "input_sha256", "schema_version", "manifest_created_at",
                "imported_at", "registered_by", "basis", "model_count",
            )),
        )
        snapshot = {
            "configuration_json": model_configuration,
            "license_label": batch.EXPECTED_MODEL["license_label"],
            "model_id": batch.EXPECTED_MODEL["model_id"],
            "name": batch.EXPECTED_MODEL["name"],
            "task": "asr",
            "version": batch.EXPECTED_MODEL["revision"],
            "weights_byte_count": self.model.stat().st_size,
            "weights_sha256": self.model_sha,
            "weights_uri": self.model.as_uri(),
        }
        connection.execute(
            "INSERT INTO model_registry_manifest_models VALUES (?, 0, ?, ?)",
            (registry["manifest_id"], batch.EXPECTED_MODEL["model_id"], json.dumps(snapshot, sort_keys=True)),
        )

        for item in self.results:
            ordinal = item["window"]["ordinal"]
            run_id = f"run_fixture_{ordinal}"
            media_id = f"media_sha256_{item['audio_sha']}"
            source_media_id = f"media_sha256_{ordinal:064x}"
            recording_id = "rec_fixture"
            rendition_id = f"rnd_fixture_{ordinal}"
            timestamp = "2026-08-27T00:00:00Z"
            metadata = {
                "contract_version": 1,
                "identity_authority": "none",
                "local_window_result_sha256": item["result_sha"],
                "local_window_result_uri": item["path"].as_uri(),
                "media_id": media_id,
                "normalized_probe": item["probe"],
                "publication_state": "withheld_by_default",
                "representation_is_original_source": False,
                "run_semantics": "catalog_admission_verification_not_extraction_execution",
                "source_media_id": source_media_id,
                "source_time_mapping": item["mapping"],
                "window": item["window"],
            }
            parameters = {
                "bundle_id": self.bundle_id,
                "local_window_result_sha256": item["result_sha"],
                "local_window_result_uri": item["path"].as_uri(),
                "run_semantics": "catalog_admission_verification_not_extraction_execution",
                "time_mapping": item["mapping"],
                "window": item["window"],
                "work_order_sha256": item["value"]["work_order_sha256"],
            }
            environment = {
                "credentials_used": False,
                "identity_claims_allowed": False,
                "network_access_performed": False,
                "publication_authority": "none",
            }
            connection.execute(
                "INSERT INTO artifacts VALUES (?, ?, ?, ?, ?, ?, 1, 'private', ?)",
                (item["artifact_id"], run_id, "window_audio_16khz_mono_flac", item["audio_path"].as_uri(), item["audio_sha"], item["audio_bytes"], json.dumps(metadata, sort_keys=True)),
            )
            connection.execute(
                "INSERT INTO processing_runs VALUES (?, ?, ?, NULL, NULL, ?, ?, NULL, ?, ?, 'completed', NULL)",
                (run_id, "local_window_result_admission", "local-window-catalog-bridge/1", json.dumps(parameters, sort_keys=True), json.dumps(environment, sort_keys=True), timestamp, timestamp),
            )
            connection.execute(
                "INSERT INTO media_objects VALUES (?, ?, ?, 'audio', 'audio/flac', 'flac', 1000, ?, ?, 'verified')",
                (media_id, item["audio_sha"], item["audio_bytes"], json.dumps(item["probe"], sort_keys=True), timestamp),
            )
            connection.execute(
                "INSERT INTO media_locations VALUES (?, ?, ?, 'private_local', ?, 1)",
                (f"mlc_fixture_{ordinal}", media_id, item["audio_path"].as_uri(), timestamp),
            )
            rendition_metadata = {
                "identity_authority": "none",
                "local_window_result_sha256": item["result_sha"],
                "local_window_result_uri": item["path"].as_uri(),
                "publication_state": "withheld_by_default",
                "source_time_mapping": item["mapping"],
            }
            connection.execute(
                "INSERT INTO renditions VALUES (?, ?, ?, ?, ?, 'unreviewed', ?)",
                (rendition_id, recording_id, media_id, f"local_window:fixture:{ordinal}", "Fixture local window", json.dumps(rendition_metadata, sort_keys=True)),
            )
            if ordinal == 1:
                connection.execute(
                    "INSERT INTO recordings VALUES (?, ?, ?, ?, NULL, NULL, 'unknown', NULL, 'video', 'metadata_only', NULL, '{}', ?, ?, NULL)",
                    (recording_id, "youtube:video:fixture0001", "fixture", "Fixture recording", timestamp, timestamp),
                )
            connection.execute(
                "INSERT INTO timeline_map_spans VALUES (?, ?, 0, 0, 1000, NULL, NULL, 'unknown', 'metadata_only')",
                (f"tms_fixture_{ordinal}", rendition_id),
            )
            connection.execute(
                "INSERT INTO media_derivations VALUES (?, ?, ?, ?, ?)",
                (media_id, source_media_id, f"local_window:fixture:{ordinal}", run_id, json.dumps({"local_window_result_sha256": item["result_sha"], "source_time_mapping": item["mapping"]}, sort_keys=True)),
            )
        connection.commit()
        connection.close()

    def _materialize(self, results: list[dict[str, object]] | None = None) -> tuple[dict[str, object], Path]:
        return batch.materialize_batch(
            database_path=self.database,
            result_paths=[item["path"] for item in (results or self.results)],
            batch_root=self.batch_root,
            asr_output_root=self.output_root,
            engine_path=self.engine,
            model_path=self.model,
        )

    def test_deterministic_materialization_replay_and_local_time_contract(self) -> None:
        first, manifest_path = self._materialize(list(reversed(self.results)))
        second, second_path = self._materialize(self.results)
        self.assertEqual(first, second)
        self.assertEqual(manifest_path, second_path)
        self.assertEqual(first["work_order_count"], 2)
        self.assertEqual(first["totals"]["audio_duration_ms"], 2_000)
        rebuilt, orders = batch.validate_batch(manifest_path)
        self.assertEqual(rebuilt, first)
        self.assertEqual([order["window"] for order in orders], [
            {"offset_ms": 0, "duration_ms": 1_000},
            {"offset_ms": 0, "duration_ms": 1_000},
        ])
        self.assertEqual(
            rebuilt["work_orders"][0]["local_window_result"]["source_time_mapping"]["artifact_zero_maps_to_source_ms"],
            5_000,
        )
        self.assertIsNone(orders[0]["glossary"])
        self.assertEqual(orders[0]["model"]["model_id"], batch.EXPECTED_MODEL["model_id"])
        try:
            import jsonschema
        except ImportError:
            return
        schema = json.loads((PIPELINE_ROOT / "schemas/asr-whispercpp-batch-manifest.schema.json").read_text(encoding="utf-8"))
        schema = copy.deepcopy(schema)
        schema["$defs"]["engine_current"]["properties"]["expected_sha256"] = {"const": self.engine_sha}
        schema["$defs"]["engine_current"]["properties"]["byte_count"] = {"const": self.engine.stat().st_size}
        schema["$defs"]["model"]["properties"]["expected_sha256"] = {"const": self.model_sha}
        schema["$defs"]["model"]["properties"]["byte_count"] = {"const": self.model.stat().st_size}
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.validate(first, schema)

    def test_explicit_subset_has_stable_identity_and_preserves_job_identity(self) -> None:
        full, full_path = self._materialize(self.results)
        _full_manifest, full_orders = batch.validate_batch(full_path)
        selected_artifact_id = self.results[1]["artifact_id"]
        full_selected = next(
            order
            for order in full_orders
            if order["input"]["artifact_id"] == selected_artifact_id
        )

        subset, subset_path = self._materialize([self.results[1]])
        replayed_subset, subset_orders = batch.validate_batch(subset_path)
        second_subset, second_subset_path = self._materialize([self.results[1]])

        self.assertNotEqual(full["batch_id"], subset["batch_id"])
        self.assertEqual(replayed_subset, subset)
        self.assertEqual(second_subset, subset)
        self.assertEqual(second_subset_path, subset_path)
        self.assertEqual(subset["work_order_count"], 1)
        self.assertEqual(subset_orders, [full_selected])
        self.assertEqual(
            subset["work_orders"][0]["job_id"],
            full_selected["job_id"],
        )

    def test_shared_engine_profiles_reject_legacy_for_new_batches(self) -> None:
        current = batch.whispercpp_engine_profiles.match_engine_profile(
            self.engine_sha,
            self.engine.stat().st_size,
        )
        self.assertEqual(current["admission"], "current_new_batch")
        self.assertTrue(current["output_json_full_utf8_token_boundary_merge"])

        legacy = copy.deepcopy(next(
            profile
            for profile in self.original_engine_profiles
            if profile["admission"] == "legacy_manifest_replay_only"
        ))
        with mock.patch.object(
            batch.whispercpp_engine_profiles,
            "ENGINE_PROFILES",
            (legacy, current),
        ):
            with self.assertRaisesRegex(
                batch.whispercpp_engine_profiles.EngineProfileError,
                "legacy-manifest validation",
            ):
                batch.whispercpp_engine_profiles.match_engine_profile(
                    legacy["expected_sha256"],
                    legacy["byte_count"],
                )
            replay_profile = batch.whispercpp_engine_profiles.match_engine_profile(
                legacy["expected_sha256"],
                legacy["byte_count"],
                allow_legacy_manifest_replay=True,
            )
        self.assertEqual(replay_profile["admission"], "legacy_manifest_replay_only")
        self.assertFalse(replay_profile["output_json_full_utf8_token_boundary_merge"])

    def test_catalog_is_byte_identical_and_no_journal_is_created(self) -> None:
        before = digest(self.database)
        sidecars_before = sorted(path.name for path in TEST_ROOT.glob("catalog.sqlite3-*") if path.exists())
        self._materialize()
        self.assertEqual(digest(self.database), before)
        self.assertEqual(sorted(path.name for path in TEST_ROOT.glob("catalog.sqlite3-*") if path.exists()), sidecars_before)

    def test_result_and_artifact_tampering_fail_closed(self) -> None:
        item = self.results[0]
        result_path = item["path"]
        result_path.chmod(0o600)
        result_path.write_bytes(result_path.read_bytes() + b" ")
        result_path.chmod(0o400)
        with self.assertRaisesRegex(batch.BatchError, "SHA-256|result"):
            self._materialize([item])

    def test_artifact_content_tampering_fails_closed(self) -> None:
        item = self.results[0]
        audio_path = item["audio_path"]
        audio_path.chmod(0o600)
        with audio_path.open("ab") as handle:
            handle.write(b"tamper")
        audio_path.chmod(0o400)
        with self.assertRaisesRegex(batch.BatchError, "byte count|SHA-256"):
            self._materialize([item])

    def test_catalog_binding_change_invalidates_sealed_batch(self) -> None:
        _manifest, manifest_path = self._materialize()
        connection = sqlite3.connect(self.database)
        connection.execute("UPDATE artifacts SET visibility = 'public' WHERE artifact_id = ?", (self.results[0]["artifact_id"],))
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(batch.BatchError, "catalog artifact"):
            batch.validate_batch(manifest_path)

    def test_model_registry_change_fails_closed(self) -> None:
        connection = sqlite3.connect(self.database)
        connection.execute("UPDATE model_registry_manifest_imports SET registered_by = 'different'")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(batch.BatchError, "registry snapshot"):
            self._materialize()

    def test_symlinked_result_and_overlapping_roots_are_rejected(self) -> None:
        link = TEST_ROOT / "result-link.json"
        link.symlink_to(self.results[0]["path"])
        with self.assertRaisesRegex(batch.BatchError, "normalized and resolved|non-symlink"):
            batch.build_batch(
                database_path=self.database,
                result_paths=[link],
                batch_root=self.batch_root,
                asr_output_root=self.output_root,
                engine_path=self.engine,
                model_path=self.model,
            )
        with self.assertRaisesRegex(batch.BatchError, "disjoint"):
            batch.build_batch(
                database_path=self.database,
                result_paths=[self.results[0]["path"]],
                batch_root=self.batch_root,
                asr_output_root=self.batch_root / "nested",
                engine_path=self.engine,
                model_path=self.model,
            )

    def test_hard_linked_sealed_result_is_rejected(self) -> None:
        hard_link = TEST_ROOT / "second-result-link.json"
        os.link(self.results[0]["path"], hard_link)
        with self.assertRaisesRegex(batch.BatchError, "exactly one hard link"):
            self._materialize([self.results[0]])

    def test_sealed_batch_extra_file_fails_replay(self) -> None:
        _manifest, manifest_path = self._materialize()
        work_orders = manifest_path.parent / "work-orders"
        work_orders.chmod(0o700)
        extra = work_orders / "extra.json"
        extra.write_text("{}\n", encoding="utf-8")
        extra.chmod(0o400)
        work_orders.chmod(0o500)
        with self.assertRaisesRegex(batch.BatchError, "missing or extra"):
            batch.validate_batch(manifest_path)

    def test_runner_dispatches_sealed_ordinal_order_and_replays(self) -> None:
        manifest, manifest_path = self._materialize()
        observed: list[str] = []

        def fake_run(order: dict[str, object], *, dry_run: bool) -> dict[str, object]:
            observed.append(order["job_id"])
            return {
                "job_id": order["job_id"],
                "status": "planned",
                "dry_run": True,
                "work_order_sha256": hashlib.sha256(batch.canonical_bytes(order)).hexdigest(),
                "recipe_id": "recipe_fixture",
                "result_key": "a" * 64,
                "result_path": str(self.output_root / f"{order['job_id']}.json"),
                "processing_run": {"processing_run_id": f"run_{len(observed)}"},
                "catalog_context": order["catalog_context"],
                "glossary": None,
                "window": {"offset_ms": 0, "duration_ms": 1_000, "end_ms": 1_000},
                "input": order["input"],
            }

        with mock.patch.object(batch.asr_whispercpp, "run_asr", side_effect=fake_run):
            result = batch.run_batch(manifest_path, dry_run=True)
        self.assertEqual(observed, [entry["job_id"] for entry in manifest["work_orders"]])
        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["job_count"], 2)
        self.assertFalse(self.output_root.exists())
        try:
            import jsonschema
        except ImportError:
            return
        schema = json.loads((PIPELINE_ROOT / "schemas/asr-whispercpp-batch-run.schema.json").read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.validate(result, schema)

    def test_runner_stops_after_first_failure(self) -> None:
        _manifest, manifest_path = self._materialize()
        observed: list[str] = []
        raw_path = self.output_root / "quarantine" / "raw-output.bin"
        quarantine = {
            "failure_key": "a" * 64,
            "receipt_kind": "whispercpp_output_json_full_invalid_utf8",
            "receipt_path": str(self.output_root / "quarantine" / "receipt.json"),
            "receipt_sha256": "b" * 64,
            "invalid_utf8": {
                "decoder": "utf-8-strict",
                "start_byte": 826_915,
                "end_byte": 826_916,
                "reason": "invalid continuation byte",
            },
            "raw_artifact": {
                "artifact_kind": "whispercpp_output_json_full_quarantine",
                "path": str(raw_path),
                "storage_uri": raw_path.as_uri(),
                "byte_count": 826_916,
                "sha256": "c" * 64,
                "visibility": "private",
            },
        }

        def fail_on_second(order: dict[str, object], *, dry_run: bool) -> dict[str, object]:
            observed.append(order["job_id"])
            if len(observed) == 2:
                raise batch.asr_whispercpp.InvalidUTF8OutputError(quarantine)
            return {
                "job_id": order["job_id"],
                "status": "planned",
                "dry_run": True,
                "work_order_sha256": hashlib.sha256(batch.canonical_bytes(order)).hexdigest(),
                "recipe_id": "recipe_fixture",
                "result_key": "b" * 64,
                "result_path": str(self.output_root / "fixture.json"),
                "processing_run": {"processing_run_id": "run_fixture"},
                "catalog_context": order["catalog_context"],
                "glossary": None,
                "window": {"offset_ms": 0, "duration_ms": 1_000, "end_ms": 1_000},
                "input": order["input"],
            }

        with mock.patch.object(batch.asr_whispercpp, "run_asr", side_effect=fail_on_second):
            with self.assertRaisesRegex(batch.BatchRunFailure, "2/2.*not strict UTF-8") as caught:
                batch.run_batch(manifest_path, dry_run=True)
        self.assertEqual(len(observed), 2)
        failure = caught.exception.result
        self.assertEqual(failure["status"], "failed")
        self.assertEqual(len(failure["results"]), 1)
        self.assertEqual(failure["results"][0]["ordinal"], 1)
        self.assertEqual(failure["failed_job"]["ordinal"], 2)
        self.assertEqual(failure["failed_job"]["quarantine"], quarantine)
        self.assertEqual(
            failure["safety"]["resume_policy"],
            "replay_manifest_reuses_content_addressed_completed_results",
        )
        self.assertEqual(
            failure["safety"]["subset_policy"],
            "materialize_explicit_sealed_input_subset",
        )
        try:
            import jsonschema
        except ImportError:
            return
        schema = json.loads(
            (PIPELINE_ROOT / "schemas/asr-whispercpp-batch-run.schema.json").read_text(
                encoding="utf-8"
            )
        )
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.validate(failure, schema)

    def test_legacy_manifest_dispatch_is_rejected_before_adapter_or_output(self) -> None:
        legacy_manifest = {
            "implementation_version": batch.LEGACY_IMPLEMENTATION_VERSION,
        }
        for dry_run in (True, False):
            with self.subTest(dry_run=dry_run), mock.patch.object(
                batch,
                "validate_batch",
                return_value=(legacy_manifest, [{"job_id": "legacy-fixture"}]),
            ), mock.patch.object(
                batch.asr_whispercpp,
                "run_asr",
            ) as adapter_run, mock.patch.object(
                batch,
                "_ensure_private_root",
            ) as ensure_output:
                with self.assertRaisesRegex(
                    batch.BatchError,
                    "validation-compatible.*dispatch-disabled",
                ):
                    batch.run_batch(TEST_ROOT / "legacy-manifest.json", dry_run=dry_run)
                adapter_run.assert_not_called()
                ensure_output.assert_not_called()

    def test_non_dry_runner_establishes_owner_private_output_boundary(self) -> None:
        _manifest, manifest_path = self._materialize([self.results[0]])

        def fake_completed(order: dict[str, object], *, dry_run: bool) -> dict[str, object]:
            self.assertFalse(dry_run)
            self.assertTrue(self.output_root.is_dir())
            self.assertEqual(self.output_root.stat().st_mode & 0o777, 0o700)
            return {
                "job_id": order["job_id"],
                "status": "completed",
                "dry_run": False,
                "work_order_sha256": hashlib.sha256(batch.canonical_bytes(order)).hexdigest(),
                "recipe_id": "recipe_fixture",
                "result_key": "c" * 64,
                "result_path": str(self.output_root / "fixture.json"),
                "processing_run": {"processing_run_id": "run_fixture"},
                "catalog_context": order["catalog_context"],
                "glossary": None,
                "window": {"offset_ms": 0, "duration_ms": 1_000, "end_ms": 1_000},
                "input": order["input"],
            }

        with mock.patch.object(batch.asr_whispercpp, "run_asr", side_effect=fake_completed):
            result = batch.run_batch(manifest_path, dry_run=False)
        self.assertEqual(result["status"], "completed")

    def test_existing_adapter_accepts_full_window_work_order_in_dry_run(self) -> None:
        _manifest, manifest_path = self._materialize([self.results[0]])
        _rebuilt, orders = batch.validate_batch(manifest_path)
        result = batch.asr_whispercpp.run_asr(orders[0], dry_run=True)
        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["window"], {"offset_ms": 0, "duration_ms": 1_000, "end_ms": 1_000})
        self.assertIsNone(result["glossary"])
        self.assertFalse(self.output_root.exists())

    def test_duplicate_results_and_unsafe_permissions_are_rejected(self) -> None:
        with self.assertRaisesRegex(batch.BatchError, "duplicate"):
            batch.build_batch(
                database_path=self.database,
                result_paths=[self.results[0]["path"], self.results[0]["path"]],
                batch_root=self.batch_root,
                asr_output_root=self.output_root,
                engine_path=self.engine,
                model_path=self.model,
            )
        self.batch_root.mkdir(mode=0o755)
        os.chmod(self.batch_root, 0o755)
        with self.assertRaisesRegex(batch.BatchError, "group or world"):
            self._materialize([self.results[0]])


class SealedLegacyManifestCompatibilityTests(unittest.TestCase):
    def test_real_v0183_manifest_validates_with_historical_software_identity(self) -> None:
        manifest_path = (
            REPOSITORY_ROOT
            / "research"
            / "corpus"
            / "private-asr-work-orders"
            / "batches"
            / "asrbatch_a98bf085d254a67bb30b837b307436f6"
            / "manifest.json"
        )
        if not manifest_path.is_file():
            self.skipTest("sealed v0.1.0 batch fixture is unavailable")
        supplied = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(supplied["implementation_version"], "0.1.0")
        self.assertNotEqual(
            supplied["software"]["materializer"]["sha256"],
            digest(Path(batch.__file__)),
        )
        self.assertNotEqual(
            supplied["software"]["asr_adapter"]["sha256"],
            digest(Path(batch.asr_whispercpp.__file__)),
        )

        rebuilt, orders = batch.validate_batch(manifest_path)

        self.assertEqual(rebuilt, supplied)
        self.assertEqual(len(orders), supplied["work_order_count"])
        self.assertEqual(
            rebuilt["engine"]["expected_sha256"],
            "4831024debb4e60e9433d27967ba6dae033d4b0c770c4ef208c16fc5a8fe77d6",
        )
        work_order_path = manifest_path.parent / rebuilt["work_orders"][0]["path"]
        direct = subprocess.run(
            [
                sys.executable,
                str(PIPELINE_ROOT / "asr_whispercpp.py"),
                "run",
                "--work-order",
                str(work_order_path),
                "--dry-run",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="strict",
            check=False,
        )
        self.assertEqual(direct.returncode, 2)
        self.assertEqual(direct.stdout, "")
        self.assertNotIn("Traceback", direct.stderr)
        direct_failure = json.loads(direct.stderr)
        self.assertEqual(direct_failure["error"]["type"], "ASRError")
        self.assertIn(
            "validation-only and cannot be executed",
            direct_failure["error"]["message"],
        )
        try:
            import jsonschema
        except ImportError:
            return
        schema = json.loads(
            (PIPELINE_ROOT / "schemas/asr-whispercpp-batch-manifest.schema.json").read_text(
                encoding="utf-8"
            )
        )
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.validate(rebuilt, schema)
        result_schema = json.loads(
            (PIPELINE_ROOT / "schemas/asr-whispercpp-result.schema.json").read_text(
                encoding="utf-8"
            )
        )
        jsonschema.validate(direct_failure, result_schema)


if __name__ == "__main__":
    unittest.main()
