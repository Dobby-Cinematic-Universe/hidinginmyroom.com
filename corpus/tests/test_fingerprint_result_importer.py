from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "audio_fingerprint.py"
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.fingerprint_result_importer import (  # noqa: E402
    _compare_v2_receipt_values,
    _compare_v2_side_values,
    _insert_compare_v2_generic_candidate,
    _insert_compare_v2_inputs,
    _insert_import_ledger,
    _insert_processing_run_v2,
    import_audio_fingerprint_compare_result,
    import_audio_fingerprint_result,
    validate_audio_fingerprint_compare_result_file,
    validate_audio_fingerprint_result_file,
)
from himr_corpus.result_importers import ResultImportError  # noqa: E402
from himr_corpus.validation import validate_database  # noqa: E402


OBSERVED_AT = "2026-08-26T22:00:00Z"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True, encoding="utf-8",
                          errors="replace", check=check)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


@unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg is required")
class FingerprintResultImporterTests(unittest.TestCase):
    V2_RECEIPT_COLUMNS = (
        "match_candidate_id", "processing_run_id", "result_path",
        "result_byte_count", "recipe_id", "recipe_sha256",
        "query_result_sha256", "candidate_result_sha256",
        "query_recording_id", "query_rendition_id",
        "candidate_recording_id", "candidate_rendition_id", "exact_raw_equal",
        "quality_flags_json", "calibration_state", "visibility",
        "publication_authority",
    )
    V2_SIDE_COLUMNS = (
        "match_candidate_id", "role", "extraction_result_sha256",
        "extraction_result_path", "extraction_result_byte_count",
        "extraction_run_id", "extraction_recipe_id", "extraction_recipe_sha256",
        "extraction_parameters_json", "extraction_environment_json",
        "extraction_fingerprint_count", "input_media_id", "input_path",
        "input_artifact_id", "input_artifact_uri",
        "input_parent_processing_run_id", "input_sha256", "input_byte_count",
        "input_duration_ms", "recording_id", "rendition_id", "fingerprint_id",
        "producer_implementation_version", "window_kind", "fingerprint_start_ms",
        "fingerprint_end_ms", "fingerprint_word_count", "artifact_id",
        "artifact_uri", "artifact_sha256", "artifact_byte_count", "engine_path",
        "engine_sha256", "engine_byte_count", "engine_version_label",
        "engine_version_output_sha256", "engine_build_configuration",
        "engine_muxer_help_sha256", "algorithm", "raw_format", "sample_rate_hz",
        "channels",
    )
    @classmethod
    def setUpClass(cls) -> None:
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        cls.shared = tempfile.TemporaryDirectory(prefix="fingerprint-import-suite-", dir=work_root)
        cls.engine_pin = json.loads(run([sys.executable, str(PROGRAM), "inspect-engine"]).stdout)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.shared.cleanup()

    def setUp(self) -> None:
        work_root = Path(self.shared.name)
        self.temporary = tempfile.TemporaryDirectory(prefix=f"{self._testMethodName}-", dir=work_root)
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "corpus.sqlite3")
        migrate(self.connection)
        self.audio = self.root / "normalized.flac"
        run([self.engine_pin["executable"], "-hide_banner", "-nostdin", "-loglevel", "error",
             "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=35",
             "-ac", "1", "-ar", "16000", "-sample_fmt", "s16", "-c:a", "flac",
             str(self.audio)])
        self.audio.chmod(0o444)
        self.result = self._produce("extract", context=True)
        self.result_path = Path(self.result["result_path"])
        self._seed_dependencies(self.result)

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def _order(self, output_name: str, *, context: bool) -> dict:
        audio_sha = digest(self.audio)
        return {
            "schema_version": 1,
            "job_id": f"fingerprint-import-{output_name}",
            "input": {
                "path": str(self.audio), "expected_sha256": audio_sha,
                "expected_byte_count": self.audio.stat().st_size,
                "media_id": f"media_sha256_{audio_sha}",
                "artifact_id": "artifact_normalized_audio_fixture",
                "parent_processing_run_id": "run_preprocess_fixture",
                "duration_ms": 35_000, "sample_rate_hz": 16_000,
                "channels": 1, "sample_format": "s16",
            },
            "engine": self.engine_pin,
            "fingerprint": {
                "algorithm": 1, "raw_format": "ffmpeg_chromaprint_fp_format_raw",
                "threads": 1, "timeout_seconds": 30,
                "selection": {"mode": "explicit_windows", "windows": [
                    {"window_id": "query", "start_ms": 0, "end_ms": 15_000},
                    {"window_id": "candidate", "start_ms": 15_000, "end_ms": 35_000},
                ]},
            },
            "catalog_context": {"recording_id": "recording_fixture", "rendition_id": "rendition_fixture"} if context else None,
            "output": {"root": str(self.root / output_name)},
        }

    def _produce(self, output_name: str, *, context: bool) -> dict:
        order = self._order(output_name, context=context)
        path = self.root / f"{output_name}.work-order.json"
        write_json(path, order)
        completed = run([sys.executable, str(PROGRAM), "run", "--work-order", str(path)], check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def _seed_dependencies(self, result: dict) -> None:
        input_row = result["input"]
        context = result["catalog_context"]
        self.assertIsNotNone(context)
        self.connection.execute(
            "INSERT INTO media_objects(media_id, sha256, byte_count, media_kind, mime_type, container, duration_ms, first_cataloged_at, integrity_state) VALUES(?, ?, ?, 'audio', 'audio/flac', 'flac', ?, ?, 'verified')",
            (input_row["media_id"], input_row["sha256"], input_row["byte_count"], input_row["duration_ms"], OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO processing_runs(processing_run_id, stage, implementation_version, parameters_json, environment_json, started_at, completed_at, status) VALUES(?, 'media_preprocess', 'fixture', '{}', '{}', ?, ?, 'completed')",
            (input_row["parent_processing_run_id"], OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO artifacts(artifact_id, processing_run_id, artifact_kind, storage_uri, sha256, byte_count, schema_version, visibility, metadata_json) VALUES(?, ?, 'normalized_audio', ?, ?, ?, 1, 'private', '{}')",
            (input_row["artifact_id"], input_row["parent_processing_run_id"], input_row["storage_uri"], input_row["sha256"], input_row["byte_count"]),
        )
        self.connection.execute(
            "INSERT INTO recordings(recording_id, canonical_key, slug, title, duration_ms, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (context["recording_id"], context["recording_id"], context["recording_id"], context["recording_id"], input_row["duration_ms"], OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO renditions(rendition_id, recording_id, media_id, rendition_kind, label, review_state, metadata_json) VALUES(?, ?, ?, 'normalized_audio', ?, 'reviewed', '{}')",
            (context["rendition_id"], context["recording_id"], input_row["media_id"], context["rendition_id"]),
        )

    def _produce_custom(
        self,
        audio: Path,
        output_name: str,
        recording_id: str,
        rendition_id: str,
    ) -> dict:
        order = self._order(output_name, context=True)
        audio_sha = digest(audio)
        order["input"].update({
            "path": str(audio),
            "expected_sha256": audio_sha,
            "expected_byte_count": audio.stat().st_size,
            "media_id": f"media_sha256_{audio_sha}",
            "artifact_id": f"artifact_normalized_audio_{output_name}",
            "parent_processing_run_id": f"run_preprocess_{output_name}",
        })
        order["catalog_context"] = {
            "recording_id": recording_id,
            "rendition_id": rendition_id,
        }
        path = self.root / f"{output_name}.work-order.json"
        write_json(path, order)
        completed = run(
            [sys.executable, str(PROGRAM), "run", "--work-order", str(path)],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def _compare_v2_order(self, query: dict, candidate: dict) -> dict:
        return {
            "schema_version": 2,
            "job_id": "fingerprint-compare-import-v2",
            "method": "exact_raw_bytes_v2",
            "query": {
                "role": "query",
                "result_path": query["result_path"],
                "expected_result_sha256": digest(Path(query["result_path"])),
                "fingerprint_id": query["fingerprints"][0]["fingerprint_id"],
            },
            "candidate": {
                "role": "candidate",
                "result_path": candidate["result_path"],
                "expected_result_sha256": digest(Path(candidate["result_path"])),
                "fingerprint_id": candidate["fingerprints"][0]["fingerprint_id"],
            },
            "output": {"root": str(self.root / "comparisons-v2")},
        }

    def _produce_v2_compare(self, query: dict, candidate: dict) -> dict:
        order_path = self.root / "compare-v2.work-order.json"
        write_json(order_path, self._compare_v2_order(query, candidate))
        completed = run(
            [sys.executable, str(PROGRAM), "compare-v2", "--work-order", str(order_path)],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def _prepare_v2_pair(self, suffix: str) -> tuple[dict, dict]:
        candidate_audio = self.root / f"candidate-{suffix}.flac"
        run([
            self.engine_pin["executable"], "-hide_banner", "-nostdin", "-loglevel", "error",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=35",
            "-metadata", f"title={suffix}", "-ac", "1", "-ar", "16000",
            "-sample_fmt", "s16", "-c:a", "flac", str(candidate_audio),
        ])
        candidate_audio.chmod(0o444)
        candidate = self._produce_custom(
            candidate_audio,
            f"candidate-{suffix}-v2",
            f"recording_candidate_{suffix}",
            f"rendition_candidate_{suffix}",
        )
        self._seed_dependencies(candidate)
        import_audio_fingerprint_result(self.connection, self.result_path)
        import_audio_fingerprint_result(self.connection, Path(candidate["result_path"]))
        compare = self._produce_v2_compare(self.result, candidate)
        return candidate, validate_audio_fingerprint_compare_result_file(
            Path(compare["result_path"])
        )

    def _stage_v2_direct_sql(
        self,
        result: dict,
        *,
        receipt_mutator=None,
        side_mutator=None,
    ) -> None:
        _insert_processing_run_v2(self.connection, result)
        _insert_compare_v2_inputs(self.connection, result)
        _insert_compare_v2_generic_candidate(self.connection, result)
        _insert_import_ledger(self.connection, result, "exact_comparison")
        receipt_values = list(_compare_v2_receipt_values(result))
        if receipt_mutator is not None:
            receipt_mutator(receipt_values)
        self.connection.execute(
            f"INSERT INTO audio_fingerprint_compare_v2_receipts("
            f"comparison_result_sha256, {', '.join(self.V2_RECEIPT_COLUMNS)}) "
            f"VALUES({', '.join('?' for _ in range(len(self.V2_RECEIPT_COLUMNS) + 1))})",
            (result["_result_sha256"], *receipt_values),
        )
        for role in ("query", "candidate"):
            side_values = list(_compare_v2_side_values(result, role))
            if side_mutator is not None:
                side_mutator(role, side_values)
            self.connection.execute(
                f"INSERT INTO audio_fingerprint_compare_v2_sides("
                f"{', '.join(self.V2_SIDE_COLUMNS)}) "
                f"VALUES({', '.join('?' for _ in self.V2_SIDE_COLUMNS)})",
                side_values,
            )

    def _insert_v2_subtype_direct_sql(self, result: dict) -> None:
        self.connection.execute(
            """
            INSERT INTO audio_fingerprint_match_candidates_v2(
                match_candidate_id, processing_run_id, query_extraction_run_id,
                candidate_extraction_run_id, query_result_sha256,
                candidate_result_sha256, query_fingerprint_id,
                candidate_fingerprint_id, comparison_method, score_semantics,
                calibration_state, requires_human_review, relationship_asserted,
                visibility, publication_authority
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'exact_raw_bytes_v2',
                     'boolean_raw_byte_equality_not_probability',
                     'not_calibrated', 1, 0, 'private', 'none')
            """,
            (
                result["comparison"]["match_candidate_id"],
                result["processing_run"]["processing_run_id"],
                result["query"]["extraction_result"]["processing_run_id"],
                result["candidate"]["extraction_result"]["processing_run_id"],
                result["query"]["extraction_result"]["sha256"],
                result["candidate"]["extraction_result"]["sha256"],
                result["query"]["fingerprint"]["fingerprint_id"],
                result["candidate"]["fingerprint"]["fingerprint_id"],
            ),
        )

    def _rewrite_result(self, value: dict, path: Path | None = None) -> Path:
        target = path or self.result_path
        os.chmod(target, 0o644)
        write_json(target, value)
        os.chmod(target, 0o444)
        return target

    def _compare_order(self) -> dict:
        sides = []
        for role, item in zip(("query", "candidate"), self.result["fingerprints"], strict=True):
            sides.append({
                "role": role, "path": item["artifact"]["path"],
                "expected_sha256": item["artifact"]["sha256"],
                "expected_byte_count": item["artifact"]["byte_count"],
                "artifact_id": item["artifact"]["artifact_id"], "fingerprint_id": item["fingerprint_id"],
                "media_id": self.result["input"]["media_id"],
                "implementation_version": item["implementation_version"], "algorithm": item["algorithm"],
                "raw_format": item["raw_format"], "sample_rate_hz": item["sample_rate_hz"],
                "channels": item["channels"], "window_kind": item["window_kind"],
                "start_ms": item["start_ms"], "end_ms": item["end_ms"],
                "fingerprint_word_count": item["fingerprint_word_count"], "quality_flags": item["quality_flags"],
            })
        return {
            "schema_version": 1, "job_id": "fingerprint-compare-import", "method": "exact_raw_bytes_v1",
            "query": sides[0], "candidate": sides[1],
            "catalog_context": {
                "query": {"recording_id": "recording_fixture", "rendition_id": "rendition_fixture"},
                "candidate": {"recording_id": "recording_fixture", "rendition_id": "rendition_fixture"},
            },
            "output": {"root": str(self.root / "comparisons")},
        }

    def test_import_is_transactional_private_and_idempotent(self) -> None:
        first = import_audio_fingerprint_result(self.connection, self.result_path)
        second = import_audio_fingerprint_result(self.connection, self.result_path)
        self.assertEqual(first["result_sha256"], second["result_sha256"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM fingerprints").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM audio_fingerprint_observations").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM observation_scores WHERE calibrated_probability IS NOT NULL").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM publication_decisions").fetchone()[0], 0)
        self.assertTrue(all(row[0] == "private" for row in self.connection.execute("SELECT visibility FROM artifacts WHERE artifact_kind = 'audio_fingerprint_chromaprint_raw'")))

    def test_null_context_imports_provenance_only(self) -> None:
        result = self._produce("no-context", context=False)
        imported = import_audio_fingerprint_result(self.connection, Path(result["result_path"]))
        self.assertTrue(imported["provenance_only"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM fingerprints").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM observations WHERE observation_kind = 'audio_fingerprint'").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM artifacts WHERE artifact_kind = 'audio_fingerprint_chromaprint_raw'").fetchone()[0], 2)

    def test_tampered_artifact_unknown_field_and_unsafe_uri_fail_before_transaction(self) -> None:
        artifact_path = Path(self.result["fingerprints"][0]["artifact"]["path"])
        os.chmod(artifact_path, 0o644)
        artifact_path.write_bytes(artifact_path.read_bytes() + b"tamper")
        os.chmod(artifact_path, 0o444)
        with self.assertRaisesRegex(ResultImportError, "byte_count|SHA-256|word"):
            import_audio_fingerprint_result(self.connection, self.result_path)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM processing_runs WHERE stage = 'audio_fingerprint_chromaprint'").fetchone()[0], 0)

        fresh = self._produce("fresh", context=True)
        fresh_path = Path(fresh["result_path"])
        unknown = copy.deepcopy(fresh); unknown["unexpected"] = True
        self._rewrite_result(unknown, fresh_path)
        with self.assertRaisesRegex(ResultImportError, "unknown"):
            validate_audio_fingerprint_result_file(fresh_path)

        unsafe_result = self._produce("unsafe", context=True)
        unsafe_path = Path(unsafe_result["result_path"])
        unsafe_result["fingerprints"][0]["artifact"]["storage_uri"] += "?alias=1"
        self._rewrite_result(unsafe_result, unsafe_path)
        with self.assertRaisesRegex(ResultImportError, "query or fragment|canonical"):
            validate_audio_fingerprint_result_file(unsafe_path)

        duplicate = copy.deepcopy(self.result)
        duplicate_path = self.result_path.with_name("duplicate-result.json")
        duplicate["result_path"] = str(duplicate_path)
        duplicate_body = json.dumps(duplicate, indent=2) + "\n"
        job_line = f'  "job_id": {json.dumps(duplicate["job_id"])},'
        duplicate_body = duplicate_body.replace(
            job_line, f"{job_line}\n{job_line}", 1
        )
        duplicate_path.write_text(duplicate_body, encoding="utf-8")
        duplicate_path.chmod(0o444)
        with self.assertRaisesRegex(ResultImportError, "duplicate JSON key"):
            validate_audio_fingerprint_result_file(duplicate_path)

    def test_missing_context_dependency_rolls_back_all_rows(self) -> None:
        self.connection.execute("DELETE FROM renditions WHERE rendition_id = 'rendition_fixture'")
        with self.assertRaisesRegex(ResultImportError, "context rendition"):
            import_audio_fingerprint_result(self.connection, self.result_path)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM processing_runs WHERE stage = 'audio_fingerprint_chromaprint'").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM artifacts WHERE artifact_kind = 'audio_fingerprint_chromaprint_raw'").fetchone()[0], 0)

    def test_exact_pair_import_is_candidate_only_and_flags_cross_duration(self) -> None:
        import_audio_fingerprint_result(self.connection, self.result_path)
        order = self._compare_order()
        order_path = self.root / "compare.work-order.json"; write_json(order_path, order)
        completed = run([sys.executable, str(PROGRAM), "compare", "--work-order", str(order_path)], check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        result_path = Path(result["result_path"])
        first = import_audio_fingerprint_compare_result(self.connection, result_path)
        second = import_audio_fingerprint_compare_result(self.connection, result_path)
        self.assertEqual(first["result_sha256"], second["result_sha256"])
        row = self.connection.execute("SELECT raw_score, calibrated_probability, decision_state, metadata_json FROM match_candidates").fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["calibrated_probability"])
        self.assertEqual(row["decision_state"], "candidate")
        metadata = json.loads(row["metadata_json"])
        self.assertFalse(metadata["relationship_asserted"])
        self.assertTrue(metadata["requires_human_review"])
        self.assertIn("cross_duration_windows", metadata["quality_flags"])
        subtype = self.connection.execute("SELECT requires_human_review, relationship_asserted, score_semantics FROM audio_fingerprint_match_candidates").fetchone()
        self.assertEqual((subtype[0], subtype[1]), (1, 0))
        self.assertIn("not_probability", subtype[2])
        run_id = first["processing_run_id"]
        query_id = self.result["fingerprints"][0]["fingerprint_id"]
        candidate_id = self.result["fingerprints"][1]["fingerprint_id"]
        self.connection.execute(
            """
            INSERT INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES('unsafe_calibrated_pair', 'fingerprint', ?, 'fingerprint', ?,
                     'chromaprint_exact_raw_bytes_v1', 1.0, 0.99, 'candidate', '{}')
            """,
            (query_id, candidate_id),
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "invalid audio fingerprint"):
            self.connection.execute(
                """
                INSERT INTO audio_fingerprint_match_candidates(
                    match_candidate_id, processing_run_id, query_fingerprint_id,
                    candidate_fingerprint_id, comparison_method, score_semantics
                ) VALUES('unsafe_calibrated_pair', ?, ?, ?, 'exact_raw_bytes_v1',
                         'boolean_raw_byte_equality_not_probability')
                """,
                (run_id, query_id, candidate_id),
            )

    def test_v2_cross_recording_import_is_private_idempotent_and_has_zero_authority(self) -> None:
        candidate, validated = self._prepare_v2_pair("valid")
        self.assertNotEqual(self.result["recipe_id"], candidate["recipe_id"])
        self.assertEqual(
            self.result["fingerprints"][0]["artifact"]["sha256"],
            candidate["fingerprints"][0]["artifact"]["sha256"],
        )
        compare_path = Path(validated["result_path"])
        self.assertEqual(validated["schema_version"], 2)
        first = import_audio_fingerprint_compare_result(self.connection, compare_path)
        second = import_audio_fingerprint_compare_result(self.connection, compare_path)
        self.assertEqual(first, second)
        self.assertEqual(first["calibration_state"], "not_calibrated")
        self.assertEqual(first["visibility"], "private")
        self.assertEqual(first["publication_authority"], "none")
        self.assertEqual(first["publication_decisions"], 0)
        row = self.connection.execute(
            """
            SELECT comparison_method, score_semantics, calibration_state,
                   requires_human_review, relationship_asserted, visibility,
                   publication_authority, query_result_sha256,
                   candidate_result_sha256
            FROM audio_fingerprint_match_candidates_v2
            """
        ).fetchone()
        self.assertEqual(row["comparison_method"], "exact_raw_bytes_v2")
        self.assertEqual(row["calibration_state"], "not_calibrated")
        self.assertEqual((row["requires_human_review"], row["relationship_asserted"]), (1, 0))
        self.assertEqual((row["visibility"], row["publication_authority"]), ("private", "none"))
        self.assertEqual(row["query_result_sha256"], digest(self.result_path))
        self.assertEqual(row["candidate_result_sha256"], digest(Path(candidate["result_path"])))
        generic = self.connection.execute(
            "SELECT raw_score, calibrated_probability, decision_state, metadata_json FROM match_candidates"
        ).fetchone()
        self.assertEqual(generic["raw_score"], 1.0)
        self.assertIsNone(generic["calibrated_probability"])
        self.assertEqual(generic["decision_state"], "candidate")
        metadata = json.loads(generic["metadata_json"])
        self.assertEqual(metadata["publication_authority"], "none")
        self.assertIn("cross_recording", metadata["quality_flags"])
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM publication_decisions").fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM audio_fingerprint_compare_v2_receipts"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM audio_fingerprint_compare_v2_sides"
            ).fetchone()[0],
            2,
        )
        validate_database(self.connection)
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "generic match evidence is append-only"
        ):
            self.connection.execute(
                """
                UPDATE match_candidates
                SET calibrated_probability = 0.99,
                    decision_state = 'accepted',
                    metadata_json = '{"publication_authority":"full"}'
                WHERE match_candidate_id = ?
                """,
                (validated["comparison"]["match_candidate_id"],),
            )
        immutable = self.connection.execute(
            """
            SELECT calibrated_probability, decision_state, metadata_json
            FROM match_candidates WHERE match_candidate_id = ?
            """,
            (validated["comparison"]["match_candidate_id"],),
        ).fetchone()
        self.assertIsNone(immutable["calibrated_probability"])
        self.assertEqual(immutable["decision_state"], "candidate")
        self.assertEqual(json.loads(immutable["metadata_json"])["publication_authority"], "none")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "run inputs are append-only"):
            self.connection.execute(
                """
                INSERT INTO run_inputs(
                    run_input_id, processing_run_id, object_type, object_id,
                    input_role, input_sha256
                ) VALUES('extra_after_v2_admission', ?, 'unexpected', 'unexpected',
                         'unexpected', NULL)
                """,
                (first["processing_run_id"],),
            )

        source_run = self.connection.execute(
            """
            SELECT stage, implementation_version, model_id, glossary_revision_id,
                   parameters_json, environment_json, random_seed, started_at,
                   completed_at, status, error_text
            FROM processing_runs WHERE processing_run_id = ?
            """,
            (first["processing_run_id"],),
        ).fetchone()
        clone_run_id = "run_v2_with_extra_pre_admission_input"
        self.connection.execute(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version, model_id,
                glossary_revision_id, parameters_json, environment_json, random_seed,
                started_at, completed_at, status, error_text
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (clone_run_id, *tuple(source_run)),
        )
        source_inputs = self.connection.execute(
            """
            SELECT object_type, object_id, input_role, input_sha256
            FROM run_inputs WHERE processing_run_id = ? ORDER BY input_role
            """,
            (first["processing_run_id"],),
        ).fetchall()
        self.assertEqual(len(source_inputs), 4)
        for index, input_row in enumerate(source_inputs):
            self.connection.execute(
                """
                INSERT INTO run_inputs(
                    run_input_id, processing_run_id, object_type, object_id,
                    input_role, input_sha256
                ) VALUES(?, ?, ?, ?, ?, ?)
                """,
                (f"clone_v2_input_{index}", clone_run_id, *tuple(input_row)),
            )
        self.connection.execute(
            """
            INSERT INTO run_inputs(
                run_input_id, processing_run_id, object_type, object_id,
                input_role, input_sha256
            ) VALUES('extra_before_v2_admission', ?, 'unexpected', 'unexpected',
                     'unexpected', NULL)
            """,
            (clone_run_id,),
        )
        source_match = self.connection.execute(
            """
            SELECT left_object_type, left_object_id, right_object_type,
                   right_object_id, match_method, raw_score,
                   calibrated_probability, decision_state, metadata_json
            FROM match_candidates WHERE match_candidate_id = ?
            """,
            (validated["comparison"]["match_candidate_id"],),
        ).fetchone()
        self.connection.execute(
            """
            INSERT INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES('match_v2_with_extra_pre_admission_input', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(source_match),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "invalid canonical audio fingerprint v2"
        ):
            self.connection.execute(
                """
                INSERT INTO audio_fingerprint_match_candidates_v2(
                    match_candidate_id, processing_run_id,
                    query_extraction_run_id, candidate_extraction_run_id,
                    query_result_sha256, candidate_result_sha256,
                    query_fingerprint_id, candidate_fingerprint_id,
                    comparison_method, score_semantics, calibration_state
                ) VALUES('match_v2_with_extra_pre_admission_input', ?, ?, ?, ?, ?, ?, ?,
                         'exact_raw_bytes_v2',
                         'boolean_raw_byte_equality_not_probability',
                         'not_calibrated')
                """,
                (
                    clone_run_id,
                    self.result["processing_run"]["processing_run_id"],
                    candidate["processing_run"]["processing_run_id"],
                    digest(self.result_path),
                    digest(Path(candidate["result_path"])),
                    self.result["fingerprints"][0]["fingerprint_id"],
                    candidate["fingerprints"][0]["fingerprint_id"],
                ),
            )

        run_id = first["processing_run_id"]
        query_id = self.result["fingerprints"][0]["fingerprint_id"]
        candidate_id = candidate["fingerprints"][0]["fingerprint_id"]
        self.connection.execute(
            """
            INSERT INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES('unsafe_v2_publication_authority', 'fingerprint', ?,
                     'fingerprint', ?, 'chromaprint_exact_raw_bytes_v2', 1.0,
                     NULL, 'candidate', '{"publication_authority":"full"}')
            """,
            (candidate_id, query_id),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "invalid canonical audio fingerprint v2"
        ):
            self.connection.execute(
                """
                INSERT INTO audio_fingerprint_match_candidates_v2(
                    match_candidate_id, processing_run_id,
                    query_extraction_run_id, candidate_extraction_run_id,
                    query_result_sha256, candidate_result_sha256,
                    query_fingerprint_id, candidate_fingerprint_id,
                    comparison_method, score_semantics, calibration_state
                ) VALUES('unsafe_v2_publication_authority', ?, ?, ?, ?, ?, ?, ?,
                         'exact_raw_bytes_v2',
                         'boolean_raw_byte_equality_not_probability',
                         'not_calibrated')
                """,
                (
                    run_id,
                    self.result["processing_run"]["processing_run_id"],
                    candidate["processing_run"]["processing_run_id"],
                    digest(self.result_path),
                    digest(Path(candidate["result_path"])),
                    candidate_id,
                    query_id,
                ),
            )

    def test_v2_database_guard_rejects_fabricated_pair_without_receipts_or_lineage(self) -> None:
        media_id = self.result["input"]["media_id"]
        observed = OBSERVED_AT
        for run_id in ("run_forged_query_extract", "run_forged_candidate_extract"):
            self.connection.execute(
                """
                INSERT INTO processing_runs(
                    processing_run_id, stage, implementation_version,
                    parameters_json, environment_json, started_at, completed_at, status
                ) VALUES(?, 'audio_fingerprint_chromaprint', '0.1.0', ?, '{}', ?, ?, 'completed')
                """,
                (run_id, json.dumps({"recipe_id": f"recipe_{run_id}"}), observed, observed),
            )
        self.connection.execute(
            """
            INSERT INTO fingerprints(
                fingerprint_id, media_id, fingerprint_kind, implementation_version,
                start_ms, end_ms, value_text, artifact_uri
            ) VALUES('fingerprint_forged_query', ?, 'chromaprint_raw',
                     'forged-query', 0, 15000, NULL, 'file:///forged-query.raw')
            """,
            (media_id,),
        )
        self.connection.execute(
            """
            INSERT INTO fingerprints(
                fingerprint_id, media_id, fingerprint_kind, implementation_version,
                start_ms, end_ms, value_text, artifact_uri
            ) VALUES('fingerprint_forged_candidate', ?, 'chromaprint_raw',
                     'forged-candidate', 0, 15000, NULL, 'file:///forged-candidate.raw')
            """,
            (media_id,),
        )
        parameters = json.dumps(
            {
                "method": "exact_raw_bytes_v2",
                "compatibility_contract": "engine_build_algorithm_format_normalization_v2",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        engine = {
            "name": "ffmpeg",
            "sha256": "e" * 64,
            "byte_count": 1,
            "version_label": "forged",
            "version_output_sha256": "f" * 64,
            "build_configuration": None,
            "muxer_help_sha256": "d" * 64,
        }
        environment = json.dumps(
            {
                "query_engine": engine,
                "candidate_engine": engine,
                "query_extraction_result_sha256": "a" * 64,
                "candidate_extraction_result_sha256": "b" * 64,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        self.connection.execute(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version,
                parameters_json, environment_json, started_at, completed_at, status
            ) VALUES('run_forged_compare_v2', 'audio_fingerprint_exact_compare_v2',
                     '0.2.0', ?, ?, ?, ?, 'completed')
            """,
            (parameters, environment, observed, observed),
        )
        for row_id, object_type, object_id, role, sha in (
            ("input_forged_query_result", "audio_fingerprint_extraction_result", "run_forged_query_extract", "query_result", "a" * 64),
            ("input_forged_candidate_result", "audio_fingerprint_extraction_result", "run_forged_candidate_extract", "candidate_result", "b" * 64),
            ("input_forged_query", "fingerprint", "fingerprint_forged_query", "query", "c" * 64),
            ("input_forged_candidate", "fingerprint", "fingerprint_forged_candidate", "candidate", "d" * 64),
        ):
            self.connection.execute(
                """
                INSERT INTO run_inputs(
                    run_input_id, processing_run_id, object_type, object_id,
                    input_role, input_sha256
                ) VALUES(?, 'run_forged_compare_v2', ?, ?, ?, ?)
                """,
                (row_id, object_type, object_id, role, sha),
            )
        safe_metadata = json.dumps(
            {
                "calibration_state": "not_calibrated",
                "requires_human_review": True,
                "relationship_asserted": False,
                "visibility": "private",
                "publication_authority": "none",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        self.connection.execute(
            """
            INSERT INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES('match_forged_v2', 'fingerprint', 'fingerprint_forged_query',
                     'fingerprint', 'fingerprint_forged_candidate',
                     'chromaprint_exact_raw_bytes_v2', 1.0, NULL, 'candidate', ?)
            """,
            (safe_metadata,),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "invalid canonical audio fingerprint v2"
        ):
            self.connection.execute(
                """
                INSERT INTO audio_fingerprint_match_candidates_v2(
                    match_candidate_id, processing_run_id,
                    query_extraction_run_id, candidate_extraction_run_id,
                    query_result_sha256, candidate_result_sha256,
                    query_fingerprint_id, candidate_fingerprint_id,
                    comparison_method, score_semantics, calibration_state
                ) VALUES('match_forged_v2', 'run_forged_compare_v2',
                         'run_forged_query_extract', 'run_forged_candidate_extract',
                         ?, ?, 'fingerprint_forged_query',
                         'fingerprint_forged_candidate', 'exact_raw_bytes_v2',
                         'boolean_raw_byte_equality_not_probability',
                         'not_calibrated')
                """,
                ("a" * 64, "b" * 64),
            )

    def test_v2_direct_sql_duplicate_engine_algorithm_and_score_bypasses_fail(self) -> None:
        _, result = self._prepare_v2_pair("direct-sql")

        query_run_id = result["query"]["extraction_result"]["processing_run_id"]
        compare_run_id = result["processing_run"]["processing_run_id"]
        match_id = result["comparison"]["match_candidate_id"]

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            environment = self.connection.execute(
                "SELECT environment_json FROM processing_runs WHERE processing_run_id = ?",
                (query_run_id,),
            ).fetchone()[0]
            engine_sha = result["query"]["engine"]["sha256"]
            duplicate_environment = environment.replace(
                f'"sha256":"{engine_sha}"',
                f'"sha256":"{engine_sha}","sha256":"{engine_sha}"',
                1,
            )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "extraction receipt seals its processing run"
            ):
                self.connection.execute(
                    "UPDATE processing_runs SET environment_json = ? "
                    "WHERE processing_run_id = ?",
                    (duplicate_environment, query_run_id),
                )

            def duplicate_side(role: str, values: list) -> None:
                if role == "query":
                    values[9] = duplicate_environment

            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "duplicate or invalid JSON"
            ):
                self._stage_v2_direct_sql(result, side_mutator=duplicate_side)
        finally:
            self.connection.rollback()

        parameters = self.connection.execute(
            "SELECT parameters_json FROM processing_runs WHERE processing_run_id = ?",
            (query_run_id,),
        ).fetchone()[0]
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "extraction receipt seals its processing run"
        ):
            changed = json.loads(parameters)
            changed["fingerprint"]["threads"] += 1
            self.connection.execute(
                "UPDATE processing_runs SET parameters_json = ? "
                "WHERE processing_run_id = ?",
                (
                    json.dumps(
                        changed,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    query_run_id,
                ),
            )

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            noncanonical = json.dumps(json.loads(parameters), indent=2)

            def noncanonical_side(role: str, values: list) -> None:
                if role == "query":
                    values[8] = noncanonical

            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "duplicate or invalid JSON"
            ):
                self._stage_v2_direct_sql(result, side_mutator=noncanonical_side)
        finally:
            self.connection.rollback()

        for case in (
            "comparison_engine",
            "extraction_algorithm",
            "score_hash",
            "receipt_result_hash",
            "receipt_context",
            "receipt_recipe",
            "receipt_run",
        ):
            with self.subTest(case=case):
                self.connection.execute("BEGIN IMMEDIATE")
                try:
                    def mutate_receipt(values: list) -> None:
                        if case == "receipt_result_hash":
                            values[6] = "f" * 64
                        elif case == "receipt_context":
                            values[8] = result["candidate"]["catalog_context"]["recording_id"]
                        elif case == "receipt_recipe":
                            values[4] = "recipe_audio_fingerprint_compare_v2_" + "0" * 32
                        elif case == "receipt_run":
                            values[1] = query_run_id

                    self._stage_v2_direct_sql(
                        result,
                        receipt_mutator=(
                            mutate_receipt if case.startswith("receipt_") else None
                        ),
                    )
                    if case == "comparison_engine":
                        environment = json.loads(
                            self.connection.execute(
                                "SELECT environment_json FROM processing_runs "
                                "WHERE processing_run_id = ?",
                                (compare_run_id,),
                            ).fetchone()[0]
                        )
                        environment["candidate_engine"]["sha256"] = "0" * 64
                        self.connection.execute(
                            "UPDATE processing_runs SET environment_json = ? "
                            "WHERE processing_run_id = ?",
                            (
                                json.dumps(
                                    environment,
                                    ensure_ascii=False,
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                                compare_run_id,
                            ),
                        )
                    elif case == "extraction_algorithm":
                        parameters = json.loads(
                            self.connection.execute(
                                "SELECT parameters_json FROM processing_runs "
                                "WHERE processing_run_id = ?",
                                (query_run_id,),
                            ).fetchone()[0]
                        )
                        parameters["fingerprint"]["algorithm"] += 1
                        with self.assertRaisesRegex(
                            sqlite3.IntegrityError,
                            "extraction receipt seals its processing run",
                        ):
                            self.connection.execute(
                                "UPDATE processing_runs SET parameters_json = ? "
                                "WHERE processing_run_id = ?",
                                (
                                    json.dumps(
                                        parameters,
                                        ensure_ascii=False,
                                        sort_keys=True,
                                        separators=(",", ":"),
                                    ),
                                    query_run_id,
                                ),
                            )
                        continue
                    elif case == "score_hash":
                        self.connection.execute(
                            "UPDATE match_candidates SET raw_score = 0.0 "
                            "WHERE match_candidate_id = ?",
                            (match_id,),
                        )
                    with self.assertRaisesRegex(
                        sqlite3.IntegrityError,
                        "invalid canonical audio fingerprint v2",
                    ):
                        self._insert_v2_subtype_direct_sql(result)
                finally:
                    self.connection.rollback()

    def test_v2_database_validator_rehashes_and_requires_current_files(self) -> None:
        _, result = self._prepare_v2_pair("nonexistent-uri")
        query_fingerprint = result["query"]["fingerprint"]
        missing_path = (self.root / "does-not-exist.chromaprint.raw").resolve()
        missing_uri = missing_path.as_uri()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                "UPDATE artifacts SET storage_uri = ? WHERE artifact_id = ?",
                (missing_uri, query_fingerprint["artifact"]["artifact_id"]),
            )
            self.connection.execute(
                "UPDATE fingerprints SET artifact_uri = ? WHERE fingerprint_id = ?",
                (missing_uri, query_fingerprint["fingerprint_id"]),
            )

            def replace_query_uri(role: str, values: list) -> None:
                if role == "query":
                    values[28] = missing_uri

            self._stage_v2_direct_sql(result, side_mutator=replace_query_uri)
            self._insert_v2_subtype_direct_sql(result)
            self.assertEqual(
                self.connection.execute(
                    "SELECT count(*) FROM audio_fingerprint_match_candidates_v2"
                ).fetchone()[0],
                1,
            )
            with self.assertRaisesRegex(
                RuntimeError, "current-file validation failed"
            ):
                validate_database(self.connection)
        finally:
            self.connection.rollback()

        import_audio_fingerprint_compare_result(
            self.connection, Path(result["result_path"])
        )
        comparison_path = Path(result["result_path"])
        comparison_path.chmod(0o644)
        comparison_path.write_bytes(comparison_path.read_bytes() + b"\n")
        comparison_path.chmod(0o444)
        with self.assertRaisesRegex(RuntimeError, "current-file validation failed"):
            validate_database(self.connection)

    def test_v2_post_admission_graph_attachments_are_rejected(self) -> None:
        _, result = self._prepare_v2_pair("sealed-graph")
        import_audio_fingerprint_compare_result(
            self.connection, Path(result["result_path"])
        )
        match_id = result["comparison"]["match_candidate_id"]
        compare_run = result["processing_run"]["processing_run_id"]
        query = result["query"]
        candidate = result["candidate"]
        query_run = query["extraction_result"]["processing_run_id"]
        query_media = query["input"]["media_id"]
        candidate_media = candidate["input"]["media_id"]
        query_parent_run = query["input"]["parent_processing_run_id"]
        query_recording = query["catalog_context"]["recording_id"]
        query_rendition = query["catalog_context"]["rendition_id"]
        query_fingerprint = query["fingerprint"]["fingerprint_id"]
        observation = self.connection.execute(
            """
            SELECT observation.observation_id
            FROM observations AS observation
            JOIN audio_fingerprint_observations AS typed
              ON typed.observation_id = observation.observation_id
            WHERE observation.processing_run_id = ?
            ORDER BY observation.observation_id LIMIT 1
            """,
            (query_run,),
        ).fetchone()[0]

        attempts = {
            "comparison input": lambda: self.connection.execute(
                "INSERT INTO run_inputs VALUES('v2_extra_compare_input', ?, "
                "'unexpected', 'unexpected', 'unexpected', NULL)",
                (compare_run,),
            ),
            "extraction input": lambda: self.connection.execute(
                "INSERT INTO run_inputs VALUES('v2_extra_extract_input', ?, "
                "'unexpected', 'unexpected', 'unexpected', NULL)",
                (query_run,),
            ),
            "input parent processing run": lambda: self.connection.execute(
                "UPDATE processing_runs SET stage = 'forged_parent_stage' "
                "WHERE processing_run_id = ?",
                (query_parent_run,),
            ),
            "input parent run input": lambda: self.connection.execute(
                "INSERT INTO run_inputs VALUES('v2_extra_parent_input', ?, "
                "'unexpected', 'unexpected', 'unexpected', NULL)",
                (query_parent_run,),
            ),
            "artifact": lambda: self.connection.execute(
                """
                INSERT INTO artifacts(
                    artifact_id, processing_run_id, artifact_kind, storage_uri,
                    sha256, byte_count, schema_version, visibility, metadata_json
                ) VALUES('v2_extra_artifact', ?, 'audio_fingerprint_chromaprint_raw',
                         'file:///tmp/v2-extra-artifact.raw', ?, 0, 1, 'private', '{}')
                """,
                (query_run, "a" * 64),
            ),
            "observation": lambda: self.connection.execute(
                """
                INSERT INTO observations(
                    observation_id, observation_kind, recording_id, rendition_id,
                    processing_run_id, start_ms, end_ms, visibility, review_state,
                    payload_schema_version, metadata_json, created_at
                ) VALUES('v2_extra_observation', 'audio_fingerprint', ?, ?, ?,
                         0, 1, 'private', 'machine', 1, '{}', ?)
                """,
                (query_recording, query_rendition, query_run, OBSERVED_AT),
            ),
            "typed observation": lambda: self.connection.execute(
                """
                INSERT INTO audio_fingerprint_observations(
                    observation_id, fingerprint_id, artifact_id, window_kind,
                    algorithm, raw_format, sample_rate_hz, channels,
                    fingerprint_word_count, requires_human_review
                ) SELECT observation_id, fingerprint_id, artifact_id, window_kind,
                         algorithm, raw_format, sample_rate_hz, channels,
                         fingerprint_word_count, requires_human_review
                  FROM audio_fingerprint_observations
                 WHERE observation_id = ?
                """,
                (observation,),
            ),
            "fingerprint": lambda: self.connection.execute(
                """
                INSERT INTO fingerprints(
                    fingerprint_id, media_id, fingerprint_kind,
                    implementation_version, start_ms, end_ms, value_text, artifact_uri
                ) VALUES('v2_extra_fingerprint', ?, 'chromaprint_raw', ?,
                         100000, 101000, NULL, 'file:///tmp/v2-extra.raw')
                """,
                (query_media, query["fingerprint"]["producer_implementation_version"]),
            ),
            "score": lambda: self.connection.execute(
                """
                INSERT INTO observation_scores(
                    observation_score_id, observation_id, score_name, raw_score,
                    calibrated_probability, calibration_set_id, quality_flags_json
                ) VALUES('v2_extra_score', ?, 'extra', 0.0, NULL, NULL, '[]')
                """,
                (observation,),
            ),
            "receipt": lambda: self.connection.execute(
                """
                INSERT INTO audio_fingerprint_result_imports(
                    import_batch_id, result_kind, result_sha256, processing_run_id,
                    recipe_id, catalog_context_json, imported_at
                ) VALUES('v2_extra_receipt', 'exact_comparison', ?, ?, ?, ?, ?)
                """,
                (
                    "b" * 64,
                    compare_run,
                    result["recipe_id"],
                    json.dumps(result["catalog_context"], sort_keys=True, separators=(",", ":")),
                    OBSERVED_AT,
                ),
            ),
            "side binding": lambda: self.connection.execute(
                "INSERT INTO audio_fingerprint_compare_v2_sides "
                "SELECT * FROM audio_fingerprint_compare_v2_sides "
                "WHERE match_candidate_id = ? AND role = 'query'",
                (match_id,),
            ),
            "comparison receipt": lambda: self.connection.execute(
                """
                INSERT INTO audio_fingerprint_compare_v2_receipts
                SELECT ?, match_candidate_id, processing_run_id, result_path,
                       result_byte_count, recipe_id, recipe_sha256,
                       query_result_sha256, candidate_result_sha256,
                       query_recording_id, query_rendition_id,
                       candidate_recording_id, candidate_rendition_id,
                       exact_raw_equal, quality_flags_json, calibration_state,
                       visibility, publication_authority
                FROM audio_fingerprint_compare_v2_receipts
                WHERE match_candidate_id = ?
                """,
                ("c" * 64, match_id),
            ),
            "media location": lambda: self.connection.execute(
                "INSERT INTO media_locations(media_location_id, media_id, storage_uri) "
                "VALUES('v2_extra_location', ?, 'file:///tmp/v2-extra-media.flac')",
                (query_media,),
            ),
            "media object": lambda: self.connection.execute(
                "UPDATE media_objects SET sha256 = ? WHERE media_id = ?",
                ("0" * 64, query_media),
            ),
            "media derivation": lambda: self.connection.execute(
                """
                INSERT INTO media_derivations(
                    child_media_id, parent_media_id, derivation_kind,
                    processing_run_id, metadata_json
                ) VALUES(?, ?, 'v2_extra', NULL, '{}')
                """,
                (query_media, candidate_media),
            ),
            "rendition": lambda: self.connection.execute(
                """
                INSERT INTO renditions(
                    rendition_id, recording_id, media_id, rendition_kind,
                    label, review_state, metadata_json
                ) VALUES('v2_extra_rendition', ?, ?, 'alternate', NULL,
                         'unreviewed', '{}')
                """,
                (query_recording, query_media),
            ),
            "timeline": lambda: self.connection.execute(
                """
                INSERT INTO timeline_map_spans(
                    timeline_map_span_id, rendition_id, ordinal, media_start_ms,
                    media_end_ms, recording_start_ms, recording_end_ms,
                    mapping_kind, confidence_state
                ) VALUES('v2_extra_timeline', ?, 999, 0, 1, 0, 1,
                         'exact', 'candidate')
                """,
                (query_rendition,),
            ),
        }
        for label, attempt in attempts.items():
            with self.subTest(attachment=label):
                with self.assertRaisesRegex(sqlite3.IntegrityError, "sealed|append-only"):
                    attempt()

    def test_v2_import_requires_both_admitted_extraction_envelopes(self) -> None:
        candidate_audio = self.root / "candidate-unimported.flac"
        run([
            self.engine_pin["executable"], "-hide_banner", "-nostdin", "-loglevel", "error",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=35",
            "-metadata", "title=unimported", "-ac", "1", "-ar", "16000",
            "-sample_fmt", "s16", "-c:a", "flac", str(candidate_audio),
        ])
        candidate_audio.chmod(0o444)
        candidate = self._produce_custom(
            candidate_audio,
            "candidate-unimported-v2",
            "recording_candidate",
            "rendition_candidate",
        )
        self._seed_dependencies(candidate)
        import_audio_fingerprint_result(self.connection, self.result_path)
        compare = self._produce_v2_compare(self.result, candidate)
        with self.assertRaisesRegex(ResultImportError, "receipt is missing"):
            import_audio_fingerprint_compare_result(
                self.connection, Path(compare["result_path"])
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE stage = ?",
                ("audio_fingerprint_exact_compare_v2",),
            ).fetchone()[0],
            0,
        )

    def test_v2_import_rejects_comparator_side_tamper_even_when_result_is_resealed(self) -> None:
        candidate_audio = self.root / "candidate-tamper.flac"
        run([
            self.engine_pin["executable"], "-hide_banner", "-nostdin", "-loglevel", "error",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=35",
            "-metadata", "title=tamper", "-ac", "1", "-ar", "16000",
            "-sample_fmt", "s16", "-c:a", "flac", str(candidate_audio),
        ])
        candidate_audio.chmod(0o444)
        candidate = self._produce_custom(
            candidate_audio,
            "candidate-tamper-v2",
            "recording_candidate",
            "rendition_candidate",
        )
        compare = self._produce_v2_compare(self.result, candidate)
        compare_path = Path(compare["result_path"])
        compare["candidate"]["engine"]["version_label"] += " forged"
        self._rewrite_result(compare, compare_path)
        with self.assertRaisesRegex(ResultImportError, "exactly mirror"):
            validate_audio_fingerprint_compare_result_file(compare_path)


if __name__ == "__main__":
    unittest.main()
