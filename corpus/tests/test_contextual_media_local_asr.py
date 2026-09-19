from __future__ import annotations

import copy
import json
import re
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.contextual_media_local_asr import (  # noqa: E402
    CATALOG_ENVIRONMENT_PROJECTION_VERSION,
    CONTEXTUAL_BATCH_ID,
    CONTEXTUAL_BATCH_CANONICAL_SHA256,
    CONTEXTUAL_BATCH_IDENTITY_SHA256,
    CONTEXTUAL_BATCH_RAW_SHA256,
    GLOSSARY_BYTE_COUNT,
    GLOSSARY_CANONICAL_SHA256,
    GLOSSARY_LANGUAGE,
    GLOSSARY_PROMPT_SHA256,
    GLOSSARY_RAW_SHA256,
    GLOSSARY_REVISION_ID,
    GLOSSARY_REVISION_LABEL,
    GLOSSARY_REVISION_SHA256,
    GLOSSARY_TERM_COUNT,
    DIFF_NUMERIC_PROJECTION_FIELDS,
    _build_admission,
    _catalog_safe_artifacts,
    _catalog_safe_processing_run,
    _media_rows,
    _private_reference,
    _require_no_preexisting_publication,
    build_private_glossary_registration_plan,
    import_private_glossary_registration,
    require_exact_contextual_diff_projection,
)
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.result_importers import ResultImportError  # noqa: E402
from himr_corpus.importers import canonical_json  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


OBSERVED_AT = "2026-08-27T14:10:00Z"
SYNTHETIC_GLOSSARY_TERMS = tuple(
    f"Private Sentinel {ordinal:02d}" for ordinal in range(1, GLOSSARY_TERM_COUNT + 1)
)


class ContextualMediaLocalASRTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="contextual-catalog-")
        self.root = Path(self.temporary.name)
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        self.glossary_path = (self.root / "private-neutral-glossary.json").resolve()
        self.glossary = {
            "path": self.glossary_path,
            "artifact_ref": _private_reference(GLOSSARY_RAW_SHA256),
            "raw_sha256": GLOSSARY_RAW_SHA256,
            "canonical_sha256": GLOSSARY_CANONICAL_SHA256,
            "byte_count": GLOSSARY_BYTE_COUNT,
            "schema_version": 1,
            "glossary_revision_id": GLOSSARY_REVISION_ID,
            "revision_label": GLOSSARY_REVISION_LABEL,
            "revision_sha256": GLOSSARY_REVISION_SHA256,
            "language": GLOSSARY_LANGUAGE,
            "prompt_sha256": GLOSSARY_PROMPT_SHA256,
            "term_count": GLOSSARY_TERM_COUNT,
        }

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def _register_glossary(self) -> dict:
        target = "himr_corpus.contextual_media_local_asr._read_private_glossary"
        with mock.patch(target, return_value=self.glossary):
            plan = build_private_glossary_registration_plan(
                self.glossary_path, observed_at=OBSERVED_AT
            )
            first = import_private_glossary_registration(
                self.connection,
                self.glossary_path,
                observed_at=OBSERVED_AT,
                expected_plan_sha256=plan["plan_sha256"],
            )
            second = import_private_glossary_registration(
                self.connection,
                self.glossary_path,
                observed_at=OBSERVED_AT,
                expected_plan_sha256=plan["plan_sha256"],
            )
        self.assertEqual(first, second)
        return first

    def _synthetic_contextual_result_envelope(self) -> dict:
        environment = {
            "python": "3.14.7",
            "cpu_only": True,
            "network": "not_used",
            "engine_version": "whisper.cpp v1.8.7",
            "engine_version_evidence": "source_revision_plus_executable_sha256",
            "command_provenance": {
                "descriptor_execution_policy": "linux_proc_self_fd_retained_verified_v1",
                "logical_commands": [
                    ["/fixture/bin/probe", "/fixture/input.flac"],
                    [
                        "/fixture/bin/engine",
                        "--file",
                        "/fixture/input.flac",
                        "--prompt",
                        "synthetic spelling prompt",
                    ],
                ],
                "logical_commands_definition": (
                    "deterministic_original_input_paths_and_final_output_path_v1"
                ),
                "result_commands_location": "result.commands",
                "result_commands_definition": "exact_child_facing_argv_v1",
                "result_command_states": ["executed", "executed"],
            },
        }
        run_id = "run_asr_whispercpp_" + "1" * 32
        return {
            "job_id": "asr-contextual-" + "2" * 32,
            "_job_id": "asr-contextual-" + "2" * 32,
            "result_key": "3" * 64,
            "commands": [
                ["/proc/self/fd/10", "/proc/self/fd/11"],
                ["/proc/self/fd/12", "--file", "/proc/self/fd/11"],
            ],
            "processing_run": {
                "processing_run_id": run_id,
                "stage": "asr_whispercpp",
                "implementation_version": "0.3.0",
                "model_id": "model_fixture",
                "glossary_revision_id": GLOSSARY_REVISION_ID,
                "parameters_json": canonical_json(
                    {
                        "glossary": {
                            "glossary_revision_id": GLOSSARY_REVISION_ID,
                            "prompt_sha256": GLOSSARY_PROMPT_SHA256,
                            "revision": GLOSSARY_REVISION_LABEL,
                            "sha256": GLOSSARY_RAW_SHA256,
                        }
                    }
                ),
                "environment_json": canonical_json(environment),
                "random_seed": None,
                "started_at": "2026-08-27T14:00:00Z",
                "completed_at": OBSERVED_AT,
                "status": "completed",
                "error_text": None,
            },
            "run_input": {
                "run_input_id": "run_input_" + "4" * 32,
                "processing_run_id": run_id,
                "object_type": "media",
                "object_id": "media_sha256_" + "5" * 64,
                "input_role": "normalized_audio",
                "input_sha256": "5" * 64,
            },
            "input": {
                "media_id": "media_sha256_" + "5" * 64,
                "artifact_id": "artifact_fixture_input",
                "probe": {"duration_ms": 1_000},
            },
            "glossary": {
                "prompt_sha256": GLOSSARY_PROMPT_SHA256,
            },
            "artifacts": [
                {
                    "artifact_id": "artifact_fixture_raw",
                    "processing_run_id": run_id,
                    "artifact_kind": "whispercpp_output_json_full",
                    "storage_uri": "file:///fixture/private/whisper.raw.json",
                    "sha256": "6" * 64,
                    "byte_count": 123,
                    "schema_version": 1,
                    "visibility": "private",
                    "metadata_json": '{"mime_type":"application/json"}',
                },
                {
                    "artifact_id": "artifact_fixture_normalized",
                    "processing_run_id": run_id,
                    "artifact_kind": "transcript_normalized_json",
                    "storage_uri": "file:///fixture/private/transcript.normalized.json",
                    "sha256": "7" * 64,
                    "byte_count": 456,
                    "schema_version": 1,
                    "visibility": "private",
                    "metadata_json": '{"mime_type":"application/json"}',
                },
            ],
            "transcript": {
                "language": {"detected": "en"},
                "segments": [
                    {
                        "ordinal": 0,
                        "start_ms": 10,
                        "end_ms": 900,
                        "text": "synthetic transcript wording",
                        "metadata_json": "{}",
                        "quality_flags": [],
                        "window_overrun_ms": 0,
                        "tokens": [
                            {
                                "ordinal": 0,
                                "start_ms": 10,
                                "end_ms": 100,
                                "text": " synthetic",
                                "raw_probability": 0.5,
                                "raw_dtw_timestamp": 1.0,
                                "token_id": 8,
                            }
                        ],
                    }
                ],
            },
        }

    def _synthetic_admission(self) -> dict:
        result = self._synthetic_contextual_result_envelope()
        result_raw_sha = "d" * 64
        result_canonical_sha = "c" * 64
        entry = {
            "ordinal": 1,
            "path": "work-orders/000001.json",
            "job_id": result["job_id"],
            "sha256": "b" * 64,
            "canonical_sha256": "a" * 64,
            "pair_projection_sha256": "9" * 64,
            "baseline": {
                "result_path": "/private/baseline/result.json",
                "raw_sha256": "8" * 64,
                "canonical_sha256": "7" * 64,
                "processing_run_id": "run_asr_whispercpp_" + "6" * 32,
            },
        }
        batch = {
            "path": Path("/private/manifest.json"),
            "reference": _private_reference(CONTEXTUAL_BATCH_RAW_SHA256),
            "manifest": {
                "work_orders": [entry],
                "glossary": {},
                "engine": {},
                "model": {},
                "materializer": "himr-contextual-asr-batch",
                "implementation_version": "0.1.0",
            },
            "orders": [{}],
            "batch_id": CONTEXTUAL_BATCH_ID,
            "identity_sha256": CONTEXTUAL_BATCH_IDENTITY_SHA256,
            "raw_sha256": CONTEXTUAL_BATCH_RAW_SHA256,
            "canonical_sha256": CONTEXTUAL_BATCH_CANONICAL_SHA256,
            "byte_count": 60812,
            "work_order_count": 17,
        }
        result_file = {
            "path": Path("/private/contextual/result.json"),
            "reference": _private_reference(result_raw_sha),
            "raw": result,
            "raw_sha256": result_raw_sha,
            "canonical_sha256": result_canonical_sha,
            "byte_count": 123456,
        }
        summary = {
            "total_blocks": 3,
            "changed_blocks": 2,
            "unchanged_blocks": 1,
            "total_character_edit_distance": 14,
            "empty_nonempty_transitions": 0,
            "maximum_absolute_first_token_start_drift_ms": 2,
            "maximum_absolute_last_token_end_drift_ms": 3,
            "baseline_lexical_tokens": 40,
            "contextual_lexical_tokens": 41,
            "baseline_untimed_lexical_tokens": 0,
            "contextual_untimed_lexical_tokens": 0,
            "glossary_term_counts": [],
        }
        diff = {
            "path": Path("/private/diff.json"),
            "reference": _private_reference("f" * 64),
            "raw": {
                "identity_sha256": "e" * 64,
                "alignment": {"block_ms": 30000},
                "summary": summary,
            },
            "raw_sha256": "f" * 64,
            "canonical_sha256": "5" * 64,
            "byte_count": 2345,
        }
        glossary_registration = {
            "private_glossary_registration_id": "pgr_" + "4" * 32,
            "registered_at": OBSERVED_AT,
        }
        baseline = {
            "media_local_revision_id": "mltr_" + "3" * 32,
            "processing_run_id": entry["baseline"]["processing_run_id"],
        }
        adapter = SimpleNamespace(_validate_adapter_result=mock.Mock())
        module = "himr_corpus.contextual_media_local_asr"
        with (
            mock.patch(f"{module}._read_validated_batch", return_value=batch),
            mock.patch(
                f"{module}._read_contextual_result",
                return_value=(result, result_file),
            ),
            mock.patch(f"{module}._read_diff", return_value=diff),
            mock.patch(
                f"{module}._require_registered_glossary",
                return_value=glossary_registration,
            ),
            mock.patch(f"{module}._require_baseline", return_value=baseline),
            mock.patch(f"{module}._require_catalog_dependencies"),
            mock.patch(f"{module}._require_no_preexisting_publication"),
            mock.patch(f"{module}._pipeline_modules", return_value=(None, adapter, None)),
        ):
            return _build_admission(
                self.connection,
                Path("/private/contextual/result.json"),
                Path("/private/manifest.json"),
                Path("/private/diff.json"),
            )

    def test_glossary_registration_is_term_free_idempotent_and_frozen(self) -> None:
        target = "himr_corpus.contextual_media_local_asr._read_private_glossary"
        with mock.patch(target, return_value=self.glossary):
            plan = build_private_glossary_registration_plan(
                self.glossary_path, observed_at=OBSERVED_AT
            )
            with self.assertRaisesRegex(ResultImportError, "expected digest"):
                import_private_glossary_registration(
                    self.connection,
                    self.glossary_path,
                    observed_at=OBSERVED_AT,
                    expected_plan_sha256="0" * 64,
                )
        encoded = json.dumps(plan, ensure_ascii=False)
        for term in SYNTHETIC_GLOSSARY_TERMS:
            self.assertNotIn(term, encoded)
        self.assertFalse(plan["policy"]["terms_in_plan"])
        admitted = self._register_glossary()
        self.assertEqual(admitted["status"], "registered")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_glossary_registrations"
            ).fetchone()[0],
            1,
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE glossary_revisions SET description = 'changed' "
                "WHERE glossary_revision_id = ?",
                (GLOSSARY_REVISION_ID,),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE private_glossary_registrations SET registered_at = ?",
                ("2026-08-28T00:00:00Z",),
            )

    def test_catalog_projection_omits_argv_prompt_and_all_local_paths(self) -> None:
        result = self._synthetic_contextual_result_envelope()
        projected_run = _catalog_safe_processing_run(result)
        environment = json.loads(projected_run["environment_json"])
        self.assertEqual(
            environment["projection_version"],
            CATALOG_ENVIRONMENT_PROJECTION_VERSION,
        )
        encoded = canonical_json(environment)
        for forbidden in (
            '"logical_commands":',
            '"command_provenance":',
            "--prompt",
            "file:///",
            str(REPOSITORY_ROOT),
            "/home/",
            "/tmp/",
        ):
            self.assertNotIn(forbidden, encoded)
        self.assertEqual(len(environment["integrity"]["logical_command_sha256"]), 2)
        self.assertEqual(len(environment["integrity"]["result_command_sha256"]), 2)
        artifacts = _catalog_safe_artifacts(result)
        self.assertEqual(len(artifacts), 2)
        for artifact in artifacts:
            self.assertEqual(
                artifact["storage_uri"], _private_reference(artifact["sha256"])
            )

        for probe in (
            "/tmp/private-python",
            "file:///tmp/private-python",
            "\\\\server\\private-python",
            "--prompt secret",
        ):
            forged = copy.deepcopy(result)
            forged_environment = json.loads(
                forged["processing_run"]["environment_json"]
            )
            forged_environment["python"] = probe
            forged["processing_run"]["environment_json"] = canonical_json(
                forged_environment
            )
            with self.assertRaisesRegex(ResultImportError, "private command data"):
                _catalog_safe_processing_run(forged)

    def test_migration_0025_rejects_raw_run_metadata_and_nonopaque_artifacts(self) -> None:
        self._register_glossary()
        result = self._synthetic_contextual_result_envelope()
        run = result["processing_run"]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "redacted projection"):
            self.connection.execute(
                """
                INSERT INTO processing_runs(
                    processing_run_id, stage, implementation_version, model_id,
                    glossary_revision_id, parameters_json, environment_json,
                    random_seed, started_at, completed_at, status, error_text
                ) VALUES(?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run["processing_run_id"],
                    run["stage"],
                    run["implementation_version"],
                    run["glossary_revision_id"],
                    run["parameters_json"],
                    run["environment_json"],
                    run["random_seed"],
                    run["started_at"],
                    run["completed_at"],
                    run["status"],
                    run["error_text"],
                ),
            )

        projected = _catalog_safe_processing_run(result)
        self.connection.execute(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version, model_id,
                glossary_revision_id, parameters_json, environment_json,
                random_seed, started_at, completed_at, status, error_text
            ) VALUES(?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                projected["processing_run_id"],
                projected["stage"],
                projected["implementation_version"],
                projected["glossary_revision_id"],
                projected["parameters_json"],
                projected["environment_json"],
                projected["random_seed"],
                projected["started_at"],
                projected["completed_at"],
                projected["status"],
                projected["error_text"],
            ),
        )
        artifact = result["artifacts"][0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "opaque private"):
            self.connection.execute(
                """
                INSERT INTO artifacts(
                    artifact_id, processing_run_id, artifact_kind, storage_uri,
                    sha256, byte_count, schema_version, visibility, metadata_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(artifact[key] for key in (
                    "artifact_id", "processing_run_id", "artifact_kind",
                    "storage_uri", "sha256", "byte_count", "schema_version",
                    "visibility", "metadata_json",
                )),
            )
        projected_artifact = _catalog_safe_artifacts(result)[0]
        self.connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, processing_run_id, artifact_kind, storage_uri,
                sha256, byte_count, schema_version, visibility, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(projected_artifact[key] for key in (
                "artifact_id", "processing_run_id", "artifact_kind",
                "storage_uri", "sha256", "byte_count", "schema_version",
                "visibility", "metadata_json",
            )),
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE artifacts SET byte_count = byte_count + 1 WHERE artifact_id = ?",
                (projected_artifact["artifact_id"],),
            )

    def test_all_terms_and_path_roots_are_absent_from_nontranscript_outputs(self) -> None:
        built = self._synthetic_admission()
        target = "himr_corpus.contextual_media_local_asr._read_private_glossary"
        with mock.patch(target, return_value=self.glossary):
            glossary_plan = build_private_glossary_registration_plan(
                self.glossary_path, observed_at=OBSERVED_AT
            )
        nontranscript = {
            "glossary_plan": glossary_plan,
            "admission_plan": built["public"],
            "catalog_run": built["catalog_run"],
            "catalog_artifacts": built["catalog_artifacts"],
            "batch_row": built["batch_row"],
            "import_receipt": built["import_receipt"],
            "pair_row": built["pair_row"],
            "diff_row": built["diff_row"],
            "media_revision": built["media"]["revision"],
        }
        encoded = canonical_json(nontranscript)
        for path_probe in (
            "file:///",
            str(REPOSITORY_ROOT),
            "/home/",
            "/tmp/",
            "/private/",
        ):
            self.assertNotIn(path_probe, encoded)

        glossary_document = {"terms": list(SYNTHETIC_GLOSSARY_TERMS)}
        self.assertEqual(len(glossary_document["terms"]), GLOSSARY_TERM_COUNT)
        # These two lower-case identifiers are frozen by migrations 0023/0024.
        # Remove the exact schema-mandated values before a case-folded term scan;
        # no other exception is permitted.
        scanned = encoded.casefold().replace(GLOSSARY_REVISION_ID.casefold(), "")
        scanned = scanned.replace("himr-contextual-asr-batch", "")
        for term in glossary_document["terms"]:
            expression = rf"(?<![a-z0-9]){re.escape(term.casefold())}(?![a-z0-9])"
            self.assertIsNone(
                re.search(expression, scanned),
                f"non-transcript catalog projection contains glossary term {term!r}",
            )
            self.assertIsNotNone(
                re.search(expression, f"prefix | {term.swapcase()} | suffix".casefold()),
                f"case-folded adversarial scan did not exercise {term!r}",
            )

    def test_forged_diff_identity_policy_and_every_numeric_projection_fail(self) -> None:
        expected = self._synthetic_admission()["diff_row"]
        require_exact_contextual_diff_projection(dict(expected), expected)
        for field in DIFF_NUMERIC_PROJECTION_FIELDS:
            forged = dict(expected)
            forged[field] += 1
            with self.assertRaisesRegex(ResultImportError, "deterministic replay"):
                require_exact_contextual_diff_projection(forged, expected)
        for field, value in (
            ("diff_identity_sha256", "0" * 64),
            ("visibility", "public"),
            ("accuracy_claimed", 1),
            ("preferred_revision_selected", 1),
        ):
            forged = dict(expected)
            forged[field] = value
            with self.assertRaisesRegex(ResultImportError, "deterministic replay"):
                require_exact_contextual_diff_projection(forged, expected)

    def test_closed_batch_guard_rejects_a_lookalike(self) -> None:
        self._register_glossary()
        values = (
            "ctxasrbatch_" + "0" * 32,
            CONTEXTUAL_BATCH_IDENTITY_SHA256,
            _private_reference(CONTEXTUAL_BATCH_RAW_SHA256),
            CONTEXTUAL_BATCH_RAW_SHA256,
            CONTEXTUAL_BATCH_CANONICAL_SHA256,
            60812,
            GLOSSARY_REVISION_ID,
            OBSERVED_AT,
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "closed pilot"):
            self.connection.execute(
                """
                INSERT INTO contextual_asr_batch_registrations(
                    contextual_batch_id, identity_sha256, manifest_uri,
                    manifest_raw_sha256, manifest_canonical_sha256,
                    manifest_byte_count, materializer, materializer_version,
                    work_order_count, glossary_revision_id, visibility,
                    publication_authority, registered_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'himr-contextual-asr-batch', '0.1.0',
                         17, ?, 'private', 'none', ?)
                """,
                values,
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM contextual_asr_batch_registrations"
            ).fetchone()[0],
            0,
        )

    def test_media_rows_preserve_a_separate_uncalibrated_machine_revision(self) -> None:
        result = {
            "processing_run": {
                "processing_run_id": "run_asr_whispercpp_" + "1" * 32,
                "completed_at": OBSERVED_AT,
            },
            "input": {
                "media_id": "media_sha256_" + "2" * 64,
                "artifact_id": "artifact_contextual_fixture",
                "probe": {"duration_ms": 1_000},
            },
            "transcript": {
                "language": {"detected": "en"},
                "segments": [
                    {
                        "ordinal": 0,
                        "start_ms": 100,
                        "end_ms": 1_050,
                        "text": "private fixture wording",
                        "metadata_json": "{}",
                        "quality_flags": ["segment_end_after_requested_window"],
                        "window_overrun_ms": 50,
                        "tokens": [
                            {
                                "ordinal": 0,
                                "start_ms": None,
                                "end_ms": None,
                                "text": " private",
                                "raw_probability": 0.5,
                                "raw_dtw_timestamp": -1.0,
                                "timing_quality_flags": ["token_timing_unavailable"],
                                "timing_state": "unavailable",
                                "token_id": 100,
                            }
                        ],
                    }
                ],
            },
        }
        rows = _media_rows(result, "3" * 64, "4" * 64)
        revision = rows["revision"]
        self.assertEqual(revision["revision_kind"], "contextual_asr")
        self.assertEqual(revision["review_state"], "machine")
        self.assertEqual(revision["glossary_revision_id"], GLOSSARY_REVISION_ID)
        self.assertEqual(rows["input_boundary_overrun_ms"], 50)
        self.assertEqual(rows["null_timed_word_count"], 1)
        self.assertIsNone(rows["segments"][0]["calibrated_probability"])
        self.assertIsNone(rows["words"][0]["calibrated_probability"])
        metadata = json.loads(revision["metadata_json"])
        self.assertFalse(metadata["preference_selected"])

    def test_cli_exposes_separate_plan_and_digest_gated_imports(self) -> None:
        parser = build_parser()
        glossary_plan = parser.parse_args(
            [
                "plan-private-glossary-registration",
                "--glossary", str(self.glossary_path),
                "--observed-at", OBSERVED_AT,
            ]
        )
        self.assertEqual(glossary_plan.command, "plan-private-glossary-registration")
        contextual_plan = parser.parse_args(
            [
                "plan-contextual-media-local-asr-admission",
                "--db", "review.sqlite3",
                "--result", "result.json",
                "--batch-manifest", "manifest.json",
                "--diff", "diff.json",
            ]
        )
        self.assertEqual(
            contextual_plan.command, "plan-contextual-media-local-asr-admission"
        )
        contextual_import = parser.parse_args(
            [
                "import-contextual-media-local-asr-result",
                "--db", "review.sqlite3",
                "--result", "result.json",
                "--batch-manifest", "manifest.json",
                "--diff", "diff.json",
                "--expected-plan-sha256", "0" * 64,
            ]
        )
        self.assertEqual(
            contextual_import.command, "import-contextual-media-local-asr-result"
        )

    def test_preexisting_generic_decision_check_fails_closed(self) -> None:
        register_reviewer_fixture(
            self.connection, "reviewer_contextual_fixture", "Fixture reviewer"
        )
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES('publication_contextual_preexisting',
                     'fixture_private_object', 'fixture_private_id', 'withhold',
                     'reviewer_contextual_fixture', ?, 'fixture')
            """,
            (OBSERVED_AT,),
        )
        with self.assertRaisesRegex(ResultImportError, "generic publication"):
            _require_no_preexisting_publication(
                self.connection,
                [("fixture_private_object", "fixture_private_id")],
            )


if __name__ == "__main__":
    unittest.main()
