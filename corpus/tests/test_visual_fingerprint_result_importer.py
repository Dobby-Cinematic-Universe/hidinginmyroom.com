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
PROGRAM = PIPELINE_ROOT / "visual_fingerprint.py"
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.result_importers import ResultImportError  # noqa: E402
from himr_corpus.visual_fingerprint_result_importer import (  # noqa: E402
    import_visual_fingerprint_compare_result,
    import_visual_fingerprint_result,
    validate_visual_fingerprint_compare_result_file,
    validate_visual_fingerprint_result_file,
)
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


OBSERVED_AT = "2026-08-26T22:30:00Z"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=check,
    )


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def write_json(path: Path, value: object, *, producer_canonical: bool = False) -> None:
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=producer_canonical,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def make_writable(root: Path) -> None:
    if not root.exists():
        return
    for path in [root, *root.rglob("*")]:
        try:
            path.chmod(path.stat().st_mode | 0o700)
        except OSError:
            pass


@unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg is required")
class VisualFingerprintResultImporterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        cls.shared = tempfile.TemporaryDirectory(prefix="visual-fingerprint-import-suite-", dir=work_root)
        cls.engine_pin = json.loads(
            run([sys.executable, str(PROGRAM), "inspect-engine"]).stdout
        )

    @classmethod
    def tearDownClass(cls) -> None:
        make_writable(Path(cls.shared.name))
        cls.shared.cleanup()

    def setUp(self) -> None:
        work_root = Path(self.shared.name)
        self.temporary = tempfile.TemporaryDirectory(
            prefix=f"{self._testMethodName}-", dir=work_root
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "corpus.sqlite3")
        migrate(self.connection)
        self.video = (self.root / "proxy.mkv").resolve()
        run(
            [
                self.engine_pin["executable"],
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=160x90:rate=10:duration=3",
                "-c:v",
                "ffv1",
                str(self.video),
            ]
        )
        self.video.chmod(0o444)
        self.result = self._produce("extract", context=True)
        self.result_path = Path(self.result["result_path"])
        self._seed_dependencies(self.result)

    def tearDown(self) -> None:
        self.connection.close()
        make_writable(self.root)
        self.temporary.cleanup()

    def _order(self, output_name: str, *, context: bool) -> dict:
        video_sha = digest(self.video)
        return {
            "schema_version": 1,
            "job_id": f"visual-import-{output_name}",
            "input": {
                "path": str(self.video),
                "expected_sha256": video_sha,
                "expected_byte_count": self.video.stat().st_size,
                "media_id": f"media_sha256_{video_sha}",
                "artifact_id": "artifact_proxy_fixture",
                "parent_processing_run_id": "run_preprocess_fixture",
                "duration_ms": 3_000,
                "timeline_origin_ms": 0,
                "video_stream_selector": "0:v:0",
                "sealed": True,
            },
            "engine": self.engine_pin,
            "extraction": {
                "algorithm": "fixed_q20_dct_phash_8x8_v1",
                "pixel_format": "gray",
                "width": 32,
                "height": 32,
                "scale_flags": "bilinear",
                "threads": 1,
                "timeout_seconds_per_frame": 30,
                "max_timestamp_drift_ms": 100,
                "samples": [
                    {
                        "sample_id": "sample-0500",
                        "window_id": "window-0000-1000",
                        "start_ms": 0,
                        "end_ms": 1_000,
                        "requested_timestamp_ms": 500,
                        "timestamp_kind": "explicit",
                    },
                    {
                        "sample_id": "sample-1500",
                        "window_id": "window-1000-2000",
                        "start_ms": 1_000,
                        "end_ms": 2_000,
                        "requested_timestamp_ms": 1_500,
                        "timestamp_kind": "explicit",
                    },
                ],
            },
            "catalog_context": (
                {"recording_id": "recording_fixture", "rendition_id": "rendition_fixture"}
                if context
                else None
            ),
            "output": {"root": str((self.root / output_name).resolve())},
        }

    def _produce(self, output_name: str, *, context: bool) -> dict:
        order_path = self.root / f"{output_name}.work-order.json"
        write_json(order_path, self._order(output_name, context=context))
        completed = run(
            [sys.executable, str(PROGRAM), "run", "--work-order", str(order_path)],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def _produce_comparison(
        self,
        output_name: str,
        query: dict,
        candidate: dict,
        *,
        query_indexes: tuple[int, ...] = (0,),
        candidate_indexes: tuple[int, ...] = (0,),
        maximum_hamming_distance: int = 0,
        context: bool = True,
    ) -> dict:
        order = {
            "schema_version": 1,
            "job_id": f"visual-compare-import-{output_name}",
            "method": "minimum_pairwise_phash_hamming_v1",
            "query": {
                "role": "query",
                "result_path": query["result_path"],
                "expected_sha256": digest(Path(query["result_path"])),
                "frame_ids": [
                    query["frames"][index]["fingerprint_id"] for index in query_indexes
                ],
            },
            "candidate": {
                "role": "candidate",
                "result_path": candidate["result_path"],
                "expected_sha256": digest(Path(candidate["result_path"])),
                "frame_ids": [
                    candidate["frames"][index]["fingerprint_id"]
                    for index in candidate_indexes
                ],
            },
            "threshold": {
                "maximum_hamming_distance": maximum_hamming_distance,
                "top_k": min(10, len(query_indexes) * len(candidate_indexes)),
                "max_pairwise_comparisons": len(query_indexes) * len(candidate_indexes),
            },
            "catalog_context": (
                {
                    "query": {
                        "recording_id": "recording_fixture",
                        "rendition_id": "rendition_fixture",
                    },
                    "candidate": {
                        "recording_id": "recording_fixture",
                        "rendition_id": "rendition_fixture",
                    },
                }
                if context
                else None
            ),
            "output": {"root": str((self.root / output_name).resolve())},
        }
        order_path = self.root / f"{output_name}.compare-work-order.json"
        write_json(order_path, order)
        completed = run(
            [
                sys.executable,
                str(PROGRAM),
                "compare",
                "--work-order",
                str(order_path),
            ],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def _seed_dependencies(self, result: dict) -> None:
        input_row = result["input"]
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(?, ?, ?, 'video', 'video/x-matroska', 'matroska', ?, ?, 'verified')
            """,
            (
                input_row["media_id"],
                input_row["sha256"],
                input_row["byte_count"],
                input_row["duration_ms"],
                OBSERVED_AT,
            ),
        )
        self.connection.execute(
            "INSERT INTO media_locations(media_location_id, media_id, storage_uri, storage_class, verified_at, is_primary) VALUES('location_proxy_fixture', ?, ?, 'local', ?, 1)",
            (input_row["media_id"], input_row["storage_uri"], OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO processing_runs(processing_run_id, stage, implementation_version, parameters_json, environment_json, started_at, completed_at, status) VALUES('run_preprocess_fixture', 'media_preprocess', 'fixture', '{}', '{}', ?, ?, 'completed')",
            (OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO artifacts(artifact_id, processing_run_id, artifact_kind, storage_uri, sha256, byte_count, schema_version, visibility, metadata_json) VALUES('artifact_proxy_fixture', 'run_preprocess_fixture', 'low_resolution_cfr_proxy', ?, ?, ?, 1, 'private', '{}')",
            (input_row["storage_uri"], input_row["sha256"], input_row["byte_count"]),
        )
        self.connection.execute(
            "INSERT INTO recordings(recording_id, canonical_key, slug, title, duration_ms, created_at, updated_at) VALUES('recording_fixture', 'fixture', 'fixture', 'Fixture', ?, ?, ?)",
            (input_row["duration_ms"], OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO renditions(rendition_id, recording_id, media_id, rendition_kind, label, review_state, metadata_json) VALUES('rendition_fixture', 'recording_fixture', ?, 'low_resolution_cfr_proxy', 'fixture', 'reviewed', '{}')",
            (input_row["media_id"],),
        )

    def _rewrite_result(self, value: dict, path: Path) -> None:
        path.chmod(0o644)
        write_json(path, value, producer_canonical=True)
        path.chmod(0o444)

    def test_import_is_transactional_private_uncalibrated_and_idempotent(self) -> None:
        first = import_visual_fingerprint_result(self.connection, self.result_path)
        second = import_visual_fingerprint_result(self.connection, self.result_path)
        self.assertEqual(first["result_sha256"], second["result_sha256"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM fingerprints").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM visual_fingerprint_observations").fetchone()[0], 2)
        rows = self.connection.execute(
            "SELECT calibration_state, calibrated_probability, requires_human_review, identity_asserted, duplicate_asserted, relationship_asserted FROM visual_fingerprint_observations"
        ).fetchall()
        self.assertTrue(
            all(tuple(row) == ("not_calibrated", None, 1, 0, 0, 0) for row in rows)
        )
        self.assertEqual(validate_database(self.connection)["visual_fingerprint_observations"], 2)
        self.assertTrue(
            all(
                row[0] == "private"
                for row in self.connection.execute(
                    "SELECT visibility FROM artifacts WHERE artifact_kind = 'visual_fingerprint_gray32'"
                )
            )
        )
        for table in (
            "publication_decisions",
            "publication_gate_decisions",
            "identity_assertions",
            "recording_relations",
            "match_candidates",
        ):
            self.assertEqual(self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0)

    def test_null_context_imports_provenance_and_private_artifacts_only(self) -> None:
        result = self._produce("no-context", context=False)
        imported = import_visual_fingerprint_result(
            self.connection, Path(result["result_path"])
        )
        self.assertTrue(imported["provenance_only"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM fingerprints").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM observations WHERE observation_kind = 'visual_fingerprint'").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM artifacts WHERE artifact_kind = 'visual_fingerprint_gray32'").fetchone()[0], 2)

    def test_tampered_frame_unknown_field_and_unsafe_uri_fail_before_transaction(self) -> None:
        frame_path = Path(self.result["frames"][0]["artifact"]["path"])
        frame_path.chmod(0o644)
        body = bytearray(frame_path.read_bytes())
        body[0] ^= 1
        frame_path.write_bytes(body)
        frame_path.chmod(0o444)
        with self.assertRaisesRegex(ResultImportError, "grayscale|pHash|bytes"):
            import_visual_fingerprint_result(self.connection, self.result_path)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM processing_runs WHERE stage = 'visual_fingerprint_extract'").fetchone()[0], 0)

        fresh = self._produce("fresh", context=True)
        fresh_path = Path(fresh["result_path"])
        unknown = copy.deepcopy(fresh)
        unknown["unexpected"] = True
        self._rewrite_result(unknown, fresh_path)
        with self.assertRaisesRegex(ResultImportError, "unknown"):
            validate_visual_fingerprint_result_file(fresh_path)

        unsafe = self._produce("unsafe", context=True)
        unsafe_path = Path(unsafe["result_path"])
        unsafe["frames"][0]["artifact"]["storage_uri"] += "?alias=1"
        self._rewrite_result(unsafe, unsafe_path)
        with self.assertRaisesRegex(ResultImportError, "query or fragment|canonical"):
            validate_visual_fingerprint_result_file(unsafe_path)

    def test_missing_context_dependency_rolls_back_every_new_row(self) -> None:
        self.connection.execute("DELETE FROM renditions WHERE rendition_id = 'rendition_fixture'")
        with self.assertRaisesRegex(ResultImportError, "context rendition"):
            import_visual_fingerprint_result(self.connection, self.result_path)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM processing_runs WHERE stage = 'visual_fingerprint_extract'").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM artifacts WHERE artifact_kind = 'visual_fingerprint_gray32'").fetchone()[0], 0)

    def test_database_guard_rejects_public_or_calibrated_visual_subtype(self) -> None:
        import_visual_fingerprint_result(self.connection, self.result_path)
        frame = self.result["frames"][0]
        run_id = self.result["processing_run"]["processing_run_id"]
        self.connection.execute(
            "INSERT INTO fingerprints(fingerprint_id, media_id, fingerprint_kind, implementation_version, start_ms, end_ms, value_text, artifact_uri) VALUES('unsafe_visual_fingerprint', ?, 'fixed_q20_dct_phash_8x8_v1', 'unsafe', 0, 1000, ?, ?)",
            (self.result["input"]["media_id"], frame["phash_hex"], frame["artifact"]["storage_uri"]),
        )
        self.connection.execute(
            "INSERT INTO observations(observation_id, observation_kind, recording_id, rendition_id, processing_run_id, start_ms, end_ms, visibility, review_state, payload_schema_version, metadata_json, created_at) VALUES('unsafe_visual_observation', 'visual_fingerprint', 'recording_fixture', 'rendition_fixture', ?, 0, 1000, 'public', 'machine', 1, '{}', ?)",
            (run_id, OBSERVED_AT),
        )
        timestamp = frame["decoded_timestamp"]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "invalid visual fingerprint"):
            self.connection.execute(
                """
                INSERT INTO visual_fingerprint_observations(
                    observation_id, fingerprint_id, artifact_id, ordinal, sample_id,
                    window_id, requested_timestamp_ms, timestamp_kind, decoded_pts,
                    decoded_duration_pts, time_base_numerator, time_base_denominator,
                    absolute_timestamp_us, relative_timestamp_us, relative_timestamp_ms,
                    decoded_duration_us, is_keyframe, timestamp_drift_us, algorithm,
                    phash_bits, phash_hex, phash_popcount, exact_gray_sha256,
                    calibration_state, calibrated_probability, requires_human_review,
                    identity_asserted, duplicate_asserted, relationship_asserted,
                    quality_flags_json
                ) VALUES(
                    'unsafe_visual_observation', 'unsafe_visual_fingerprint', ?, 0,
                    'unsafe', 'unsafe', 500, 'explicit', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    'fixed_q20_dct_phash_8x8_v1', 64, ?, ?, ?, 'not_calibrated',
                    NULL, 1, 0, 0, 0, '[]'
                )
                """,
                (
                    frame["artifact"]["artifact_id"],
                    timestamp["pts"],
                    timestamp["duration_pts"],
                    timestamp["time_base_numerator"],
                    timestamp["time_base_denominator"],
                    timestamp["absolute_timestamp_us"],
                    timestamp["relative_timestamp_us"],
                    timestamp["relative_timestamp_ms"],
                    timestamp["duration_us"],
                    int(timestamp["is_keyframe"]),
                    frame["timestamp_drift_us"],
                    frame["phash_hex"],
                    frame["phash_popcount"],
                    frame["exact_gray_sha256"],
                ),
            )

    def test_comparison_candidate_is_private_review_routing_and_idempotent(self) -> None:
        candidate = self._produce("candidate-extract", context=True)
        import_visual_fingerprint_result(self.connection, self.result_path)
        import_visual_fingerprint_result(
            self.connection, Path(candidate["result_path"])
        )
        comparison = self._produce_comparison(
            "candidate-compare", self.result, candidate, maximum_hamming_distance=0
        )
        self.assertTrue(comparison["comparison"]["candidate_emitted"])
        comparison_path = Path(comparison["result_path"])
        validated = validate_visual_fingerprint_compare_result_file(comparison_path)
        first = import_visual_fingerprint_compare_result(
            self.connection, comparison_path
        )
        second = import_visual_fingerprint_compare_result(
            self.connection, comparison_path
        )
        self.assertEqual(first, second)
        self.assertEqual(first["comparison_id"], validated["comparison"]["match_candidate_id"])
        self.assertEqual(first["generic_match_candidates_inserted_or_present"], 1)
        self.assertEqual(first["review_tasks_inserted_or_present"], 1)
        expected_counts = {
            "visual_fingerprint_compare_imports": 1,
            "visual_fingerprint_compare_sides": 2,
            "visual_fingerprint_compare_side_frames": 2,
            "visual_fingerprint_comparisons": 1,
            "visual_fingerprint_compare_top_pairs": 1,
            "visual_fingerprint_compare_completion_receipts": 1,
        }
        for table, expected in expected_counts.items():
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                expected,
            )
        typed = self.connection.execute(
            """
            SELECT candidate_emitted, decision_state, calibration_state,
                   calibrated_probability, requires_human_review,
                   person_identity_asserted, duplicate_asserted, parent_asserted,
                   ownership_asserted, relationship_asserted, unrelated_asserted,
                   visibility, publication_authority
            FROM visual_fingerprint_comparisons
            """
        ).fetchone()
        self.assertEqual(
            tuple(typed),
            (
                1, "candidate_for_human_review", "not_calibrated", None, 1,
                0, 0, 0, 0, 0, 0, "private", "none",
            ),
        )
        match = self.connection.execute(
            "SELECT decision_state, calibrated_probability FROM match_candidates "
            "WHERE match_method = 'visual_phash_minimum_hamming_v1'"
        ).fetchone()
        self.assertEqual(tuple(match), ("candidate", None))
        task = self.connection.execute(
            "SELECT review_task_id, status FROM review_tasks "
            "WHERE task_kind = 'visual_fingerprint_comparison_review'"
        ).fetchone()
        self.assertEqual(task["status"], "open")
        self.connection.execute(
            "UPDATE review_tasks SET status = 'in_progress', updated_at = ? "
            "WHERE review_task_id = ?",
            ("2026-08-27T01:00:00Z", task["review_task_id"]),
        )
        import_visual_fingerprint_compare_result(self.connection, comparison_path)
        self.assertEqual(
            self.connection.execute(
                "SELECT status FROM review_tasks WHERE review_task_id = ?",
                (task["review_task_id"],),
            ).fetchone()[0],
            "in_progress",
        )
        self.assertEqual(
            validate_database(self.connection)["visual_fingerprint_comparisons"], 1
        )
        for table in (
            "recording_relations", "publication_decisions",
            "publication_gate_decisions", "identity_assertions",
        ):
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0
            )

    def test_below_threshold_comparison_is_not_match_rejection_or_review_task(self) -> None:
        candidate = self._produce("below-extract", context=True)
        pairs: list[tuple[int, int, int]] = []
        for query_index, query_frame in enumerate(self.result["frames"]):
            for candidate_index, candidate_frame in enumerate(candidate["frames"]):
                distance = (
                    int(query_frame["phash_hex"], 16)
                    ^ int(candidate_frame["phash_hex"], 16)
                ).bit_count()
                pairs.append((distance, query_index, candidate_index))
        distance, query_index, candidate_index = max(pairs)
        self.assertGreater(distance, 0, "fixture needs one non-identical visual hash")
        import_visual_fingerprint_result(self.connection, self.result_path)
        import_visual_fingerprint_result(
            self.connection, Path(candidate["result_path"])
        )
        comparison = self._produce_comparison(
            "below-compare",
            self.result,
            candidate,
            query_indexes=(query_index,),
            candidate_indexes=(candidate_index,),
            maximum_hamming_distance=distance - 1,
        )
        self.assertFalse(comparison["comparison"]["candidate_emitted"])
        imported = import_visual_fingerprint_compare_result(
            self.connection, Path(comparison["result_path"])
        )
        self.assertEqual(imported["decision_state"], "below_configured_threshold")
        self.assertEqual(imported["generic_match_candidates_inserted_or_present"], 0)
        self.assertEqual(imported["review_tasks_inserted_or_present"], 0)
        typed = self.connection.execute(
            """
            SELECT candidate_emitted, decision_state, threshold_state,
                   match_candidate_id, review_task_id, unrelated_asserted
            FROM visual_fingerprint_comparisons
            """
        ).fetchone()
        self.assertEqual(
            tuple(typed),
            (
                0, "below_configured_threshold",
                "does_not_meet_configured_threshold", None, None, 0,
            ),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM match_candidates WHERE "
                "match_method = 'visual_phash_minimum_hamming_v1'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM review_tasks WHERE "
                "task_kind = 'visual_fingerprint_comparison_review'"
            ).fetchone()[0],
            0,
        )
        comparison_id = imported["comparison_id"]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "invalid visual comparison match"):
            self.connection.execute(
                """
                INSERT INTO match_candidates(
                    match_candidate_id, left_object_type, left_object_id,
                    right_object_type, right_object_id, match_method, raw_score,
                    calibrated_probability, decision_state, metadata_json
                ) VALUES(?, 'visual_fingerprint_extraction_result', 'forged_query',
                         'visual_fingerprint_extraction_result', 'forged_candidate',
                         'visual_phash_minimum_hamming_v1', 0.0, NULL,
                         'candidate', '{}')
                """,
                (comparison_id,),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "invalid visual comparison review"):
            self.connection.execute(
                """
                INSERT INTO review_tasks(
                    review_task_id, task_kind, target_type, target_id, reason,
                    priority, status, created_at, updated_at
                ) VALUES('forged_below_task',
                         'visual_fingerprint_comparison_review',
                         'visual_fingerprint_comparison', ?, 'forged', 75,
                         'open', ?, ?)
                """,
                (comparison_id, OBSERVED_AT, OBSERVED_AT),
            )
        self.assertEqual(
            validate_database(self.connection)["visual_fingerprint_comparisons"], 1
        )

    def test_comparison_tamper_missing_receipt_and_null_context_roll_back(self) -> None:
        candidate = self._produce("tamper-extract", context=True)
        comparison = self._produce_comparison(
            "tamper-compare", self.result, candidate, maximum_hamming_distance=64
        )
        comparison_path = Path(comparison["result_path"])
        tampered = copy.deepcopy(comparison)
        tampered["comparison"]["top_pairs"][0]["hamming_distance"] = 64
        self._rewrite_result(tampered, comparison_path)
        with self.assertRaisesRegex(ResultImportError, "comparison evidence"):
            validate_visual_fingerprint_compare_result_file(comparison_path)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE stage = 'visual_fingerprint_compare'"
            ).fetchone()[0],
            0,
        )

        fresh = self._produce_comparison(
            "missing-receipt-compare", self.result, candidate, maximum_hamming_distance=64
        )
        import_visual_fingerprint_result(self.connection, self.result_path)
        with self.assertRaisesRegex(ResultImportError, "candidate extraction receipt"):
            import_visual_fingerprint_compare_result(
                self.connection, Path(fresh["result_path"])
            )
        for table in (
            "visual_fingerprint_compare_imports", "visual_fingerprint_comparisons",
            "visual_fingerprint_compare_completion_receipts",
        ):
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0
            )

        null_context = self._produce_comparison(
            "null-context-compare",
            self.result,
            candidate,
            maximum_hamming_distance=64,
            context=False,
        )
        self.assertIsNone(
            validate_visual_fingerprint_compare_result_file(
                Path(null_context["result_path"])
            )["catalog_context"]
        )
        with self.assertRaisesRegex(ResultImportError, "requires explicit"):
            import_visual_fingerprint_compare_result(
                self.connection, Path(null_context["result_path"])
            )

    def test_comparison_database_guards_block_mutation_and_publication(self) -> None:
        candidate = self._produce("guard-extract", context=True)
        import_visual_fingerprint_result(self.connection, self.result_path)
        import_visual_fingerprint_result(
            self.connection, Path(candidate["result_path"])
        )
        comparison = self._produce_comparison(
            "guard-compare", self.result, candidate, maximum_hamming_distance=0
        )
        imported = import_visual_fingerprint_compare_result(
            self.connection, Path(comparison["result_path"])
        )
        comparison_id = imported["comparison_id"]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE visual_fingerprint_comparisons SET best_hamming_distance = 1 "
                "WHERE comparison_id = ?",
                (comparison_id,),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE match_candidates SET raw_score = 0.5 WHERE match_candidate_id = ?",
                (comparison_id,),
            )
        register_reviewer_fixture(
            self.connection, "human_guard", "Human guard", "human"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "no publication authority"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('publish_visual_compare', 'visual_fingerprint_comparison', ?,
                         'publish', 'human_guard', ?, 'forbidden')
                """,
                (comparison_id, OBSERVED_AT),
            )


if __name__ == "__main__":
    unittest.main()
