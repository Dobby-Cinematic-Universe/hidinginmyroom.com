from __future__ import annotations

import hashlib
import json
import stat
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.faster_whisper_gpu_v3_importer import (  # noqa: E402
    CONFIDENCE_NOTICE,
    IDENTITY_NOTICE,
    OUTPUT_CONTRACT,
    POLICY,
    WORD_TIMING_NOTICE,
    _validate_transcripts,
    build_faster_whisper_gpu_v3_admission_plan,
    import_faster_whisper_gpu_v3_result,
)
from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.media_local_asr_bridge import (  # noqa: E402
    search_media_local_transcripts,
)
from himr_corpus.result_importers import ResultImportError  # noqa: E402
from himr_corpus.validation import validate_database  # noqa: E402


def canonical_bytes(value: object) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def identity_document(core: dict, identifier_key: str, prefix: str) -> dict:
    identity = digest(canonical_bytes(core))
    return {
        **core,
        "identity_sha256": identity,
        identifier_key: f"{prefix}_{identity[:32]}",
    }


def write_sealed(path: Path, value: object, mode: int = 0o400) -> bytes:
    body = canonical_bytes(value)
    path.write_bytes(body)
    path.chmod(mode)
    return body


class FasterWhisperGPUV3ImporterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="gpu-v3-import-")
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        self.fixture = self._make_fixture()
        self._seed_input()

    def tearDown(self) -> None:
        self.connection.close()
        for path in sorted(self.root.rglob("*"), reverse=True):
            if path.is_symlink():
                continue
            try:
                path.chmod(0o700 if path.is_dir() else 0o600)
            except FileNotFoundError:
                pass
        self.temporary.cleanup()

    def _make_fixture(self) -> dict:
        input_path = self.root / "audio.flac"
        input_path.write_bytes(b"synthetic sealed audio fixture\n")
        input_path.chmod(0o444)
        input_sha = digest(input_path.read_bytes())
        media_id = f"media_sha256_{input_sha}"
        input_artifact_id = "artifact_input_gpu_v3_fixture"
        parent_run_id = "run_preprocess_" + "1" * 32
        output_root = self.root / "results"
        work_order_core = {
            "catalog_context": None,
            "gpu": {
                "compute_type": "float16",
                "device_index": 0,
                "expected_uuid": "GPU-synthetic",
            },
            "implementation_version": "0.3.0",
            "inference": {
                "condition_on_previous_text": False,
                "max_result_bytes": 16 * 1024 * 1024,
                "max_segments": 32,
                "max_words": 256,
                "vad_filter": False,
                "word_timestamps": True,
            },
            "input": {
                "artifact_id": input_artifact_id,
                "expected_byte_count": input_path.stat().st_size,
                "expected_duration_ms": 2_000,
                "expected_sha256": input_sha,
                "media_format": {
                    "channels": 1,
                    "codec": "flac",
                    "container": "flac",
                    "sample_format": "s16",
                    "sample_rate_hz": 16_000,
                },
                "media_id": media_id,
                "parent_processing_run_id": parent_run_id,
                "path": str(input_path),
                "sealed_mode": "0444",
                "timeline_offset_ms": 0,
            },
            "job_id": "gpu-v3-synthetic-job",
            "kind": "himr_faster_whisper_gpu_work_order",
            "model": {
                "identity_sha256": "2" * 64,
                "revision": "synthetic-model-revision",
            },
            "output": {"root": str(output_root)},
            "policy": POLICY,
            "runtime": {"packages": {"faster-whisper": "1.2.1"}},
            "schema_version": 3,
        }
        work_order = identity_document(work_order_core, "work_order_id", "gpuasrwo")
        work_order_path = self.root / "work-order.json"
        work_order_body = write_sealed(work_order_path, work_order)

        raw_segment = {
            "average_log_probability_raw": -0.25,
            "compression_ratio_raw": 1.5,
            "end_seconds": 1.5,
            "engine_segment_id": 1,
            "no_speech_probability_raw": 0.02,
            "ordinal": 0,
            "seek": 0,
            "start_seconds": 0.2,
            "temperature_raw": 0.0,
            "text": " synthetic searchable phrase",
            "token_ids": [1, 2],
            "words": [
                {
                    "end_seconds": 0.5,
                    "ordinal": 0,
                    "probability_raw": 0.5,
                    "start_seconds": 0.1,
                    "text": " synthetic",
                },
                {
                    "end_seconds": 1.5,
                    "ordinal": 1,
                    "probability_raw": 0.75,
                    "start_seconds": 0.5,
                    "text": " phrase",
                },
            ],
        }
        raw_core = {
            "duration_after_vad_seconds_raw": 2.0,
            "duration_seconds_raw": 2.0,
            "engine": {
                "library": "faster-whisper",
                "library_version": "1.2.1",
                "model_identity_sha256": "2" * 64,
                "model_revision": "synthetic-model-revision",
            },
            "input": {
                "duration_ms": 2_000,
                "sha256": input_sha,
                "timeline_offset_ms": 0,
            },
            "kind": "himr_faster_whisper_raw_transcript",
            "language": {
                "all_probabilities_raw": [],
                "probability_raw": 0.9,
                "value": "en",
            },
            "policy": POLICY,
            "schema_version": 1,
            "score_semantics": "raw_model_outputs_uncalibrated",
            "segments": [raw_segment],
        }
        raw = identity_document(raw_core, "document_id", "gpuasrraw")

        flag_names = {
            "extends_beyond_segment_end": False,
            "overlaps_previous": False,
            "precedes_segment_start": False,
            "start_regresses_from_previous": False,
        }
        first_flags = {**flag_names, "precedes_segment_start": True}
        normalized_segment = {
            "calibrated_confidence": None,
            "end_ms": 1_500,
            "ordinal": 0,
            "raw_scores": {
                "average_log_probability": -0.25,
                "compression_ratio": 1.5,
                "no_speech_probability": 0.02,
                "temperature": 0.0,
            },
            "source_end_ms": 1_500,
            "source_start_ms": 200,
            "speaker": None,
            "start_ms": 200,
            "text": " synthetic searchable phrase",
            "timing_clipped_to_input": False,
            "word_timing_anomaly_count": 1,
            "word_timing_anomaly_flag_counts": {
                "extends_beyond_segment_end": 0,
                "overlaps_previous": 0,
                "precedes_segment_start": 1,
                "start_regresses_from_previous": 0,
            },
            "words": [
                {
                    "calibrated_probability": None,
                    "end_ms": 500,
                    "ordinal": 0,
                    "raw_probability": 0.5,
                    "source_end_ms": 500,
                    "source_start_ms": 100,
                    "start_ms": 100,
                    "text": " synthetic",
                    "timing_anomaly_flags": first_flags,
                    "timing_clipped_to_input": False,
                },
                {
                    "calibrated_probability": None,
                    "end_ms": 1_500,
                    "ordinal": 1,
                    "raw_probability": 0.75,
                    "source_end_ms": 1_500,
                    "source_start_ms": 500,
                    "start_ms": 500,
                    "text": " phrase",
                    "timing_anomaly_flags": flag_names,
                    "timing_clipped_to_input": False,
                },
            ],
        }
        normalized_core = {
            "confidence_notice": CONFIDENCE_NOTICE,
            "human_reviewed": False,
            "identity_notice": IDENTITY_NOTICE,
            "kind": "himr_machine_transcript",
            "language": {
                "calibrated_probability": None,
                "raw_probability": 0.9,
                "value": "en",
            },
            "machine_generated": True,
            "policy": POLICY,
            "schema_version": 1,
            "scores_calibrated": False,
            "segment_count": 1,
            "segments": [normalized_segment],
            "timeline": {
                "coordinate_system": "media_ms",
                "end_ms": 2_000,
                "source_duration_ms": 2_000,
                "source_offset_ms": 0,
            },
            "verified_quotation": False,
            "word_count": 2,
            "word_timing_anomalies": {
                "anomalous_word_count": 1,
                "flag_counts": {
                    "extends_beyond_segment_end": 0,
                    "overlaps_previous": 0,
                    "precedes_segment_start": 1,
                    "start_regresses_from_previous": 0,
                },
                "notice": WORD_TIMING_NOTICE,
                "total_flag_count": 1,
            },
        }
        normalized = identity_document(
            normalized_core, "document_id", "gpuasrnorm"
        )
        raw_body = canonical_bytes(raw)
        normalized_body = canonical_bytes(normalized)

        result_key = "3" * 64
        result_dir = (
            output_root
            / "asr"
            / "faster-whisper-gpu"
            / "sha256"
            / input_sha[:2]
            / input_sha
            / "results"
            / result_key
        )
        result_dir.mkdir(parents=True)
        raw_path = result_dir / "transcript.raw.json"
        normalized_path = result_dir / "transcript.normalized.json"
        raw_path.write_bytes(raw_body)
        normalized_path.write_bytes(normalized_body)
        raw_path.chmod(0o400)
        normalized_path.chmod(0o400)
        run_id = "run_asr_faster_whisper_gpu_" + "4" * 32
        recipe = {
            "confidence_contract": "raw-model-scores-uncalibrated-v1",
            "contract_version": 3,
            "implementation_version": "0.3.0",
            "output_contract": OUTPUT_CONTRACT,
            "stage": "asr_faster_whisper_gpu",
            "timeline_offset_ms": 0,
        }
        recipe_sha = digest(canonical_bytes(recipe))
        observed = input_path.stat()
        result_core = {
            "artifacts": [
                {
                    "artifact_id": "artifact_gpu_v3_raw_fixture",
                    "artifact_kind": "faster_whisper_raw_transcript_json",
                    "byte_count": len(raw_body),
                    "mime_type": "application/json",
                    "mode": "0400",
                    "path": str(raw_path),
                    "processing_run_id": run_id,
                    "sha256": digest(raw_body),
                    "storage_uri": raw_path.as_uri(),
                    "visibility": "private",
                },
                {
                    "artifact_id": "artifact_gpu_v3_normalized_fixture",
                    "artifact_kind": "transcript_normalized_json",
                    "byte_count": len(normalized_body),
                    "mime_type": "application/json",
                    "mode": "0400",
                    "path": str(normalized_path),
                    "processing_run_id": run_id,
                    "sha256": digest(normalized_body),
                    "storage_uri": normalized_path.as_uri(),
                    "visibility": "private",
                },
            ],
            "catalog_context": None,
            "commands": [],
            "errors": [],
            "gpu_lock": {"held": True},
            "hardware": {"uuid": "GPU-synthetic"},
            "inference": {
                "compute_type": "float16",
                "device": "cuda",
                "device_index": 0,
                "library": "faster-whisper",
                "model_local_files_only": True,
                "parameters": work_order["inference"],
                "task": "transcribe",
            },
            "input": {
                "byte_count": observed.st_size,
                "device": observed.st_dev,
                "inode": observed.st_ino,
                "link_count": observed.st_nlink,
                "mode": stat.S_IMODE(observed.st_mode),
                "path": str(input_path),
                "probe": {"duration_ms": 2_000},
                "sha256": input_sha,
            },
            "job_id": work_order["job_id"],
            "kind": "himr_faster_whisper_gpu_result",
            "model": work_order["model"],
            "policy": POLICY,
            "processing_run": {
                "completed_at": "2026-08-29T12:00:02Z",
                "duration_ms": 2_000,
                "implementation_version": "0.3.0",
                "processing_run_id": run_id,
                "random_seed": None,
                "stage": "asr_faster_whisper_gpu",
                "started_at": "2026-08-29T12:00:00Z",
                "status": "completed",
            },
            "recipe": recipe,
            "recipe_id": f"recipe_gpu_asr_{recipe_sha[:32]}",
            "recipe_sha256": recipe_sha,
            "result_key": result_key,
            "result_path": str(result_dir / "result.json"),
            "runtime": work_order["runtime"],
            "schema_version": 1,
            "status": "completed",
            "transcript": {
                "human_reviewed": False,
                "language": normalized["language"],
                "normalized_identity_sha256": normalized["identity_sha256"],
                "raw_identity_sha256": raw["identity_sha256"],
                "scores_calibrated": False,
                "segment_count": 1,
                "word_count": 2,
            },
            "work_order": {
                "byte_count": len(work_order_body),
                "identity_sha256": work_order["identity_sha256"],
                "path": str(work_order_path),
                "sha256": digest(work_order_body),
                "work_order_id": work_order["work_order_id"],
            },
        }
        result = identity_document(result_core, "result_id", "gpuasrresult")
        result_path = result_dir / "result.json"
        result_body = write_sealed(result_path, result)
        result_dir.chmod(0o500)
        return {
            "input_path": input_path,
            "input_sha": input_sha,
            "media_id": media_id,
            "input_artifact_id": input_artifact_id,
            "parent_run_id": parent_run_id,
            "work_order": work_order,
            "work_order_path": work_order_path,
            "work_order_body": work_order_body,
            "result": result,
            "result_path": result_path,
            "result_body": result_body,
            "result_dir": result_dir,
        }

    def _seed_input(self) -> None:
        item = self.fixture
        self.connection.execute(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version, model_id,
                glossary_revision_id, parameters_json, environment_json,
                random_seed, started_at, completed_at, status, error_text
            ) VALUES(?, 'preprocess', 'synthetic', NULL, NULL, '{}', '{}', NULL,
                     '2026-08-29T11:00:00Z', '2026-08-29T11:00:01Z',
                     'completed', NULL)
            """,
            (item["parent_run_id"],),
        )
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, ffprobe_json, first_cataloged_at, integrity_state
            ) VALUES(?, ?, ?, 'audio', 'audio/flac', 'flac', 2000, '{}',
                     '2026-08-29T11:00:01Z', 'verified')
            """,
            (
                item["media_id"],
                item["input_sha"],
                item["input_path"].stat().st_size,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, processing_run_id, artifact_kind, storage_uri,
                sha256, byte_count, schema_version, visibility, metadata_json
            ) VALUES(?, ?, 'normalized_audio_flac', ?, ?, ?, 1, 'private', '{}')
            """,
            (
                item["input_artifact_id"],
                item["parent_run_id"],
                item["input_path"].as_uri(),
                item["input_sha"],
                item["input_path"].stat().st_size,
            ),
        )

    def _replace_with_empty_transcript(self) -> None:
        result_dir = self.fixture["result_dir"]
        raw_path = result_dir / "transcript.raw.json"
        normalized_path = result_dir / "transcript.normalized.json"
        result_path = self.fixture["result_path"]
        result_dir.chmod(0o700)
        for path in (raw_path, normalized_path, result_path):
            path.chmod(0o600)

        raw = json.loads(raw_path.read_text(encoding="utf-8"))
        raw_core = {
            key: value
            for key, value in raw.items()
            if key not in {"identity_sha256", "document_id"}
        }
        raw_core["segments"] = []
        raw = identity_document(raw_core, "document_id", "gpuasrraw")
        raw_body = canonical_bytes(raw)

        normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
        normalized_core = {
            key: value
            for key, value in normalized.items()
            if key not in {"identity_sha256", "document_id"}
        }
        normalized_core["segments"] = []
        normalized_core["segment_count"] = 0
        normalized_core["word_count"] = 0
        normalized_core["word_timing_anomalies"] = {
            "anomalous_word_count": 0,
            "flag_counts": {
                "extends_beyond_segment_end": 0,
                "overlaps_previous": 0,
                "precedes_segment_start": 0,
                "start_regresses_from_previous": 0,
            },
            "notice": WORD_TIMING_NOTICE,
            "total_flag_count": 0,
        }
        normalized = identity_document(
            normalized_core, "document_id", "gpuasrnorm"
        )
        normalized_body = canonical_bytes(normalized)

        result = json.loads(result_path.read_text(encoding="utf-8"))
        result_core = {
            key: value
            for key, value in result.items()
            if key not in {"identity_sha256", "result_id"}
        }
        for artifact in result_core["artifacts"]:
            if artifact["artifact_kind"] == "faster_whisper_raw_transcript_json":
                artifact["sha256"] = digest(raw_body)
                artifact["byte_count"] = len(raw_body)
            else:
                artifact["sha256"] = digest(normalized_body)
                artifact["byte_count"] = len(normalized_body)
        result_core["transcript"] = {
            "human_reviewed": False,
            "language": normalized["language"],
            "normalized_identity_sha256": normalized["identity_sha256"],
            "raw_identity_sha256": raw["identity_sha256"],
            "scores_calibrated": False,
            "segment_count": 0,
            "word_count": 0,
        }
        result = identity_document(result_core, "result_id", "gpuasrresult")

        raw_path.write_bytes(raw_body)
        normalized_path.write_bytes(normalized_body)
        result_path.write_bytes(canonical_bytes(result))
        for path in (raw_path, normalized_path, result_path):
            path.chmod(0o400)
        result_dir.chmod(0o500)

    def _plan(self) -> dict:
        return build_faster_whisper_gpu_v3_admission_plan(
            self.connection,
            self.fixture["result_path"],
            self.fixture["work_order_path"],
        )

    def test_plan_import_search_and_replay_are_private(self) -> None:
        plan = self._plan()
        serialized = canonical_json(plan)
        self.assertNotIn("synthetic searchable phrase", serialized)
        self.assertFalse(plan["safety"]["scores_calibrated"])
        self.assertEqual(plan["safety"]["speaker_assignment"], "none")
        first = import_faster_whisper_gpu_v3_result(
            self.connection,
            self.fixture["result_path"],
            self.fixture["work_order_path"],
            expected_plan_sha256=plan["plan_sha256"],
        )
        second = import_faster_whisper_gpu_v3_result(
            self.connection,
            self.fixture["result_path"],
            self.fixture["work_order_path"],
            expected_plan_sha256=plan["plan_sha256"],
        )
        self.assertEqual(first, second)
        receipt = self.connection.execute(
            "SELECT * FROM private_gpu_v3_asr_imports"
        ).fetchone()
        self.assertEqual(receipt["execution_mode"], "unasserted")
        self.assertEqual(receipt["anomalous_word_count"], 1)
        self.assertEqual(receipt["speaker_assignment"], "none")
        segment = self.connection.execute(
            "SELECT * FROM media_local_transcript_segments"
        ).fetchone()
        self.assertIsNone(segment["speaker_label"])
        self.assertIsNone(segment["calibrated_probability"])
        hits = search_media_local_transcripts(
            self.connection, "searchable", media_id=self.fixture["media_id"]
        )
        self.assertEqual(hits["result_count"], 1)
        validation = validate_database(self.connection)
        self.assertIsInstance(validation, dict)

    def test_wrong_reviewed_plan_fails_without_rows(self) -> None:
        with self.assertRaisesRegex(ResultImportError, "separately reviewed plan"):
            import_faster_whisper_gpu_v3_result(
                self.connection,
                self.fixture["result_path"],
                self.fixture["work_order_path"],
                expected_plan_sha256="f" * 64,
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_gpu_v3_asr_imports"
            ).fetchone()[0],
            0,
        )

    def test_omitted_batch_completion_does_not_claim_standalone(self) -> None:
        plan = self._plan()
        self.assertEqual(plan["batch"]["execution_mode"], "unasserted")
        self.assertIsNone(plan["batch"]["batch_completion_uri"])
        self.assertNotIn("standalone", canonical_json(plan))
        import_faster_whisper_gpu_v3_result(
            self.connection,
            self.fixture["result_path"],
            self.fixture["work_order_path"],
            expected_plan_sha256=plan["plan_sha256"],
        )
        receipt = self.connection.execute(
            "SELECT execution_mode, batch_completion_id "
            "FROM private_gpu_v3_asr_imports"
        ).fetchone()
        self.assertEqual(receipt["execution_mode"], "unasserted")
        self.assertIsNone(receipt["batch_completion_id"])

    def test_empty_silent_result_imports_as_searchable_empty_revision(self) -> None:
        self._replace_with_empty_transcript()
        plan = self._plan()
        self.assertEqual(plan["transcript"]["segment_count"], 0)
        self.assertEqual(plan["transcript"]["word_count"], 0)
        imported = import_faster_whisper_gpu_v3_result(
            self.connection,
            self.fixture["result_path"],
            self.fixture["work_order_path"],
            expected_plan_sha256=plan["plan_sha256"],
        )
        self.assertEqual(imported["status"], "admitted")
        receipt = self.connection.execute(
            "SELECT * FROM private_gpu_v3_asr_imports"
        ).fetchone()
        self.assertEqual(receipt["segment_count"], 0)
        self.assertEqual(receipt["word_count"], 0)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_revisions"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_segments"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_fts"
            ).fetchone()[0],
            0,
        )
        self.assertIsInstance(validate_database(self.connection), dict)

    def test_zero_duration_segment_has_explicit_schema_representation_error(self) -> None:
        raw_path = self.fixture["result_dir"] / "transcript.raw.json"
        normalized_path = self.fixture["result_dir"] / "transcript.normalized.json"
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
        raw_core = {
            key: value
            for key, value in raw.items()
            if key not in {"identity_sha256", "document_id"}
        }
        raw_core["segments"][0]["end_seconds"] = raw_core["segments"][0][
            "start_seconds"
        ]
        raw = identity_document(raw_core, "document_id", "gpuasrraw")
        normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
        with self.assertRaisesRegex(
            ResultImportError,
            "zero-duration segment cannot be represented.*half-open media-local",
        ):
            _validate_transcripts(raw, normalized, self.fixture["work_order"])

    def test_result_tamper_after_plan_fails_closed(self) -> None:
        plan = self._plan()
        self.fixture["result_dir"].chmod(0o700)
        self.fixture["result_path"].chmod(0o600)
        self.fixture["result_path"].write_bytes(self.fixture["result_body"] + b" ")
        self.fixture["result_path"].chmod(0o400)
        self.fixture["result_dir"].chmod(0o500)
        with self.assertRaises(ResultImportError):
            import_faster_whisper_gpu_v3_result(
                self.connection,
                self.fixture["result_path"],
                self.fixture["work_order_path"],
                expected_plan_sha256=plan["plan_sha256"],
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_gpu_v3_asr_imports"
            ).fetchone()[0],
            0,
        )

    def test_publication_object_type_is_forbidden(self) -> None:
        with self.assertRaisesRegex(
            Exception, "private GPU v3 ASR imports have no publication lane"
        ):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, review_decision_id, decided_at, basis, public_label
                ) VALUES('pd_fixture', 'private_gpu_v3_asr_import', 'gpuv3_fixture',
                         'publish', 'missing_reviewer', NULL,
                         '2026-08-29T12:30:00Z', 'forbidden fixture', NULL)
                """
            )

    def test_cli_exposes_plan_and_import_commands(self) -> None:
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("plan-faster-whisper-gpu-v3-admission", choices)
        self.assertIn("import-faster-whisper-gpu-v3-result", choices)
        self.assertIn(
            "--batch-completion",
            choices["import-faster-whisper-gpu-v3-result"]._option_string_actions,
        )


if __name__ == "__main__":
    unittest.main()
