from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.contextual_media_local_asr import (  # noqa: E402
    CONTEXTUAL_BATCH_CANONICAL_SHA256,
    CONTEXTUAL_BATCH_ID,
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
)
from himr_corpus import media_local_transcript_projection as projection_module  # noqa: E402
from himr_corpus.db import connect, migrate, utc_now  # noqa: E402
from himr_corpus.media_local_transcript_projection import (  # noqa: E402
    COORDINATE_ATTESTATION,
    MediaLocalTranscriptProjectionError,
    apply_media_local_transcript_projection_plan,
    build_media_local_transcript_projection_plan,
    load_media_local_transcript_projection_manifest,
    require_exact_projection_batch,
)
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


SOURCE_TIME = "2026-08-27T00:00:00Z"
QUEUE_ID = "asrppqueue_66349d9b85c74f2376830edf2a7d4f0c"
QUEUE_IDENTITY = "66349d9b85c74f2376830edf2a7d4f0ccf9d4e093f9c55b8127429259f2948d1"
QUEUE_RAW = "100d142cf459a663dd0f899db74b9666fbe49942d7ff0960280424899d39b5a0"
SEAL_ID = "asrsealreceipt_c0694c5c36586eb433a766490f3dbc01"
SEAL_RAW = "806439f2736dae9b95ffdffd19c3464c9efc39e79a5fbc21f7223b9f2c717b7b"
SEAL_IDENTITY = "9f29212c38a78ff91faaea5dc7d8eb10f3d0405c0075ce8e365d4b33598df524"


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def identifier(prefix: str, label: str) -> str:
    return prefix + digest(label)[:32]


class MediaLocalTranscriptProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="projection-v29-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.connection = connect(self.root / "corpus.sqlite3")
        self.addCleanup(self.connection.close)
        migrate(self.connection)
        # The fixture seeds the already independently tested contextual admission
        # graph directly. Production catalogs retain both guards.
        self.connection.execute("DROP TRIGGER contextual_processing_run_redacted_projection")
        self.connection.execute("DROP TRIGGER contextual_import_projected_graph_guard")
        register_reviewer_fixture(
            self.connection, "reviewer_projection_fixture", "Projection Fixture"
        )
        self._seed_contextual_registry()
        self.entries = [
            self._seed_recording(index, recording_delta)
            for index, recording_delta in enumerate((2, 0, 4, 2, 3))
        ]
        self.reviewed_at = utc_now()
        self.manifest_path = self.root / "projection-manifest.json"

    def _seed_contextual_registry(self) -> None:
        self.connection.execute(
            """
            INSERT INTO glossary_revisions(
                glossary_revision_id, sha256, created_at, description, artifact_uri
            ) VALUES(?, ?, ?, 'fixture', ?)
            """,
            (
                GLOSSARY_REVISION_ID, GLOSSARY_RAW_SHA256, SOURCE_TIME,
                "urn:private:sha256:" + GLOSSARY_RAW_SHA256,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO private_glossary_registrations(
                private_glossary_registration_id, glossary_revision_id,
                artifact_uri, raw_sha256, canonical_sha256, byte_count,
                schema_version, revision_label, revision_sha256, language,
                prompt_sha256, term_count, terms_stored_in_catalog, review_state,
                accuracy_claimed, publication_authority, plan_sha256, registered_at
            ) VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 0,
                     'machine_candidate_unreviewed', 0, 'none', ?, ?)
            """,
            (
                identifier("pgr_", "fixture"), GLOSSARY_REVISION_ID,
                "urn:private:sha256:" + GLOSSARY_RAW_SHA256,
                GLOSSARY_RAW_SHA256, GLOSSARY_CANONICAL_SHA256,
                GLOSSARY_BYTE_COUNT, GLOSSARY_REVISION_LABEL,
                GLOSSARY_REVISION_SHA256, GLOSSARY_LANGUAGE,
                GLOSSARY_PROMPT_SHA256, GLOSSARY_TERM_COUNT,
                digest("glossary-plan"), SOURCE_TIME,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO contextual_asr_batch_registrations(
                contextual_batch_id, identity_sha256, manifest_uri,
                manifest_raw_sha256, manifest_canonical_sha256,
                manifest_byte_count, materializer, materializer_version,
                work_order_count, glossary_revision_id, visibility,
                publication_authority, registered_at
            ) VALUES(?, ?, ?, ?, ?, 60812, 'himr-contextual-asr-batch',
                     '0.1.0', 17, ?, 'private', 'none', ?)
            """,
            (
                CONTEXTUAL_BATCH_ID, CONTEXTUAL_BATCH_IDENTITY_SHA256,
                "urn:private:sha256:" + CONTEXTUAL_BATCH_RAW_SHA256,
                CONTEXTUAL_BATCH_RAW_SHA256, CONTEXTUAL_BATCH_CANONICAL_SHA256,
                GLOSSARY_REVISION_ID, SOURCE_TIME,
            ),
        )

    def _seed_recording(self, index: int, recording_delta: int) -> dict[str, object]:
        connection = self.connection
        tag = f"recording-{index}"
        ids = {
            "seed_batch": identifier("imp_", tag + "-seed"),
            "acquisition_batch": identifier("imp_", tag + "-acquisition"),
            "acquisition_observation": identifier("iob_", tag + "-acquisition"),
            "source_metadata_observation": identifier("smo_", tag + "-public"),
            "raw_batch": identifier("imp_", tag + "-raw"),
            "context_batch": identifier("imp_", tag + "-context"),
            "source": identifier("src_", tag),
            "recording": identifier("rec_", tag),
            "recording_source": identifier("rso_", tag),
            "parent": identifier("media_", tag + "-parent"),
            "normalized": identifier("media_", tag + "-normalized"),
            "media_source": identifier("mso_", tag),
            "rendition": identifier("rnd_", tag),
            "preprocess_run": identifier("run_", tag + "-preprocess"),
            "raw_run": identifier("run_", tag + "-raw"),
            "context_run": identifier("run_", tag + "-context"),
            "artifact": identifier("artifact_", tag + "-audio"),
            "raw_revision": identifier("mltr_", tag + "-raw"),
            "context_revision": identifier("mltr_", tag + "-context"),
            "raw_import": identifier("mlasri_", tag),
            "context_import": identifier("cmlasri_", tag),
            "pair": identifier("casp_", tag),
            "diff": identifier("casdiff_", tag),
        }
        connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status, statistics_json
            ) VALUES(?, 'media_preprocess_result_v1', 'fixture', ?, NULL, ?, ?,
                     'completed', '{}')
            """,
            (
                ids["seed_batch"], digest(tag + "-preprocess-canonical"),
                SOURCE_TIME, SOURCE_TIME,
            ),
        )
        connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status,
                statistics_json
            ) VALUES(?, 'acquisition_result_v1', 'fixture', ?, NULL, ?, ?,
                     'completed', '{}')
            """,
            (
                ids["acquisition_batch"], digest(tag + "-acquisition-input"),
                SOURCE_TIME, SOURCE_TIME,
            ),
        )
        connection.execute(
            """
            INSERT INTO import_observations(
                import_observation_id, import_batch_id, importer_version,
                source_snapshot_date, observed_at, status, completed_at,
                statistics_json
            ) VALUES(?, ?, 'fixture', NULL, ?, 'completed', ?, '{}')
            """,
            (
                ids["acquisition_observation"], ids["acquisition_batch"],
                SOURCE_TIME, SOURCE_TIME,
            ),
        )
        raw_result_canonical = digest(tag + "-raw-result-canonical")
        contextual_result_canonical = digest(tag + "-context-result-canonical")
        connection.executemany(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status, statistics_json
            ) VALUES(?, ?, ?, ?, NULL, ?, ?, 'completed', '{}')
            """,
            [
                (
                    ids["raw_batch"], "media_local_asr_result_v1",
                    "media-local-asr-bridge/1", raw_result_canonical,
                    SOURCE_TIME, SOURCE_TIME,
                ),
                (
                    ids["context_batch"], "contextual_media_local_asr_result_v1",
                    "contextual-media-local-asr-bridge/2",
                    contextual_result_canonical, SOURCE_TIME, SOURCE_TIME,
                ),
            ],
        )
        connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, metadata_json,
                created_by_import_batch_id, created_at, updated_at
            ) VALUES(?, 'internet_archive', 'archive_media_file', ?, ?, ?,
                     'public', 'metadata_only', '{}', ?, ?, ?)
            """,
            (
                ids["source"], tag + ".mp4",
                f"https://archive.org/download/fixture/{tag}.mp4",
                SOURCE_TIME, ids["acquisition_batch"], SOURCE_TIME, SOURCE_TIME,
            ),
        )
        connection.execute(
            """
            INSERT INTO source_metadata_observations(
                source_metadata_observation_id, source_id, import_batch_id,
                import_observation_id, observed_at, quality_rank, quality_basis,
                candidate_sha256, parent_source_id, canonical_url,
                historical_url, title, published_at, access_state, review_state,
                metadata_json
            ) VALUES(?, ?, ?, ?, ?, 700,
                     'acquisition_result_v1: locally verified acquisition result',
                     ?, NULL, ?, NULL, NULL, NULL, 'public', 'metadata_only', '{}')
            """,
            (
                ids["source_metadata_observation"], ids["source"],
                ids["acquisition_batch"], ids["acquisition_observation"],
                SOURCE_TIME, digest(tag + "-public-candidate"),
                f"https://archive.org/download/fixture/{tag}.mp4",
            ),
        )
        connection.execute(
            "UPDATE sources SET current_metadata_observation_id = ? WHERE source_id = ?",
            (ids["source_metadata_observation"], ids["source"]),
        )
        connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, duration_ms,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, 'video', 'metadata_only', '{}', ?, ?)
            """,
            (
                ids["recording"], f"youtube:fixture{index}", tag,
                f"Fixture {index}", 1000 + recording_delta, SOURCE_TIME, SOURCE_TIME,
            ),
        )
        connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(?, ?, ?, 'archive_original_file', 'fixture_exact_id',
                     'metadata_only', '{}')
            """,
            (ids["recording_source"], ids["recording"], ids["source"]),
        )
        connection.executemany(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version, model_id,
                glossary_revision_id, parameters_json, environment_json,
                started_at, completed_at, status
            ) VALUES(?, ?, 'fixture/1', NULL, ?, '{}', '{}', ?, ?, 'completed')
            """,
            [
                (ids["preprocess_run"], "media_preprocess", None, SOURCE_TIME, SOURCE_TIME),
                (ids["raw_run"], "asr_whispercpp", None, SOURCE_TIME, SOURCE_TIME),
                (
                    ids["context_run"], "asr_whispercpp", GLOSSARY_REVISION_ID,
                    SOURCE_TIME, SOURCE_TIME,
                ),
            ],
        )
        connection.executemany(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(?, ?, ?, ?, ?, ?, 1000, ?, 'verified')
            """,
            [
                (
                    ids["parent"], digest(tag + "-parent"), 100, "video",
                    "video/mp4", "mp4", SOURCE_TIME,
                ),
                (
                    ids["normalized"], digest(tag + "-normalized"), 50,
                    "audio", "audio/flac", "flac", SOURCE_TIME,
                ),
            ],
        )
        connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, processing_run_id, artifact_kind, storage_uri,
                sha256, byte_count, schema_version, visibility, metadata_json
            ) VALUES(?, ?, 'audio_16khz_mono_flac', ?, ?, 50, 1, 'private', '{}')
            """,
            (
                ids["artifact"], ids["preprocess_run"],
                f"file:///fixture/{tag}.flac", digest(tag + "-normalized"),
            ),
        )
        connection.execute(
            """
            INSERT INTO media_derivations(
                child_media_id, parent_media_id, derivation_kind,
                processing_run_id, metadata_json
            ) VALUES(?, ?, 'audio_normalization_16khz_mono_flac', ?,
                     '{"channels":1,"sample_rate_hz":16000}')
            """,
            (ids["normalized"], ids["parent"], ids["preprocess_run"]),
        )
        connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(?, ?, ?, 'archive_original', 'fixture', 'unreviewed', '{}')
            """,
            (ids["rendition"], ids["recording"], ids["parent"]),
        )
        connection.execute(
            """
            INSERT INTO media_sources(
                media_source_id, media_id, source_id, retrieved_at,
                retrieval_tool, retrieval_tool_version
            ) VALUES(?, ?, ?, ?, 'fixture', '1')
            """,
            (ids["media_source"], ids["parent"], ids["source"], SOURCE_TIME),
        )
        normalized_sha = digest(tag + "-normalized")
        connection.executemany(
            """
            INSERT INTO run_inputs(
                run_input_id, processing_run_id, object_type, object_id,
                input_role, input_sha256
            ) VALUES(?, ?, 'media', ?, ?, ?)
            """,
            [
                (
                    identifier("rinput_", tag + "-preprocess"),
                    ids["preprocess_run"], ids["parent"],
                    "source_media",
                    digest(tag + "-parent"),
                ),
                (
                    identifier("rinput_", tag + "-raw"), ids["raw_run"],
                    ids["normalized"], "normalized_audio", normalized_sha,
                ),
                (
                    identifier("rinput_", tag + "-context"), ids["context_run"],
                    ids["normalized"], "normalized_audio", normalized_sha,
                ),
            ],
        )
        for variant in ("raw", "context"):
            revision_id = ids[f"{variant}_revision"]
            run_id = ids[f"{variant}_run"]
            kind = "raw_asr" if variant == "raw" else "contextual_asr"
            glossary = None if variant == "raw" else GLOSSARY_REVISION_ID
            connection.execute(
                """
                INSERT INTO media_local_transcript_revisions(
                    media_local_revision_id, media_id, input_artifact_id,
                    processing_run_id, revision_kind, origin, language,
                    glossary_revision_id, review_state, coordinate_system, boundary,
                    input_duration_ms, requested_start_ms, requested_end_ms,
                    max_segment_end_ms, input_boundary_overrun_ms,
                    source_coordinate_state, recording_coordinate_state,
                    created_at, metadata_json
                ) VALUES(?, ?, ?, ?, ?, 'fixture', 'en', ?, 'machine', 'media_ms',
                         'half_open', 1000, 0, 1000, 900, 0,
                         'unasserted_catalog_context_null',
                         'unasserted_catalog_context_null', ?, '{}')
                """,
                (revision_id, ids["normalized"], ids["artifact"], run_id, kind, glossary, SOURCE_TIME),
            )
            segment_one = identifier("mlts_", tag + variant + "-one")
            segment_two = identifier("mlts_", tag + variant + "-two")
            connection.executemany(
                """
                INSERT INTO media_local_transcript_segments(
                    media_local_segment_id, media_local_revision_id, ordinal,
                    media_start_ms, media_end_ms, input_boundary_overrun_ms,
                    text, normalized_text, speaker_label, language,
                    confidence_band, calibrated_probability, metadata_json
                ) VALUES(?, ?, ?, ?, ?, 0, ?, NULL, NULL, 'en', NULL, NULL, '{}')
                """,
                [
                    (segment_one, revision_id, 0, 10, 400, f"private {variant} phrase {index}"),
                    (segment_two, revision_id, 1, 500, 900, f"private {variant} ending {index}"),
                ],
            )
            connection.executemany(
                """
                INSERT INTO media_local_transcript_words(
                    media_local_word_id, media_local_segment_id, ordinal,
                    media_start_ms, media_end_ms, token, normalized_token,
                    asr_log_probability, alignment_score, calibrated_probability,
                    metadata_json
                ) VALUES(?, ?, 0, ?, ?, ?, NULL, -0.2, NULL, NULL, ?)
                """,
                [
                    (
                        identifier("mltw_", tag + variant + "-one"), segment_one,
                        None if variant == "context" else 10,
                        None if variant == "context" else 100,
                        "first", '{"timing":"fixture"}',
                    ),
                    (
                        identifier("mltw_", tag + variant + "-two"), segment_two,
                        600, 600, "zero", '{}',
                    ),
                ],
            )
        raw_raw = digest(tag + "-raw-result")
        connection.execute(
            """
            INSERT INTO media_local_asr_imports(
                media_local_asr_import_id, import_batch_id,
                media_local_revision_id, asr_result_uri, asr_result_raw_sha256,
                asr_result_canonical_sha256, asr_result_byte_count,
                queue_manifest_uri, queue_manifest_raw_sha256,
                queue_identity_sha256, queue_id, queue_ordinal, routing_hint,
                work_order_uri, work_order_raw_sha256,
                work_order_canonical_sha256, preprocess_result_uri,
                preprocess_result_raw_sha256, preprocess_result_canonical_sha256,
                preprocess_import_batch_id, seal_receipt_uri,
                seal_receipt_raw_sha256, seal_receipt_id,
                seal_receipt_identity_sha256, seal_receipt_ordinal,
                sealed_result_directory_uri, input_media_id, input_artifact_id,
                input_duration_ms, max_segment_end_ms, input_boundary_overrun_ms,
                null_timed_word_count, plan_sha256, imported_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, 100, ?, ?, ?, ?, ?, 'process', ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1000, 900, 0, 0, ?, ?, '{}')
            """,
            (
                ids["raw_import"], ids["raw_batch"], ids["raw_revision"],
                f"urn:fixture:{tag}:raw", raw_raw, raw_result_canonical,
                "urn:fixture:queue", QUEUE_RAW, QUEUE_IDENTITY, QUEUE_ID,
                index + 2, f"urn:fixture:{tag}:work", digest(tag + "-work-raw"),
                digest(tag + "-work-canonical"), f"urn:fixture:{tag}:preprocess",
                digest(tag + "-preprocess-raw"), digest(tag + "-preprocess-canonical"),
                ids["seed_batch"], "urn:fixture:seal", SEAL_RAW, SEAL_ID,
                SEAL_IDENTITY, index + 1, f"urn:fixture:{tag}:sealed",
                ids["normalized"], ids["artifact"], digest(tag + "-raw-plan"),
                SOURCE_TIME,
            ),
        )
        context_raw = digest(tag + "-context-result")
        pair_projection = digest(tag + "-pair-projection")
        work_raw = digest(tag + "-context-work")
        connection.execute(
            """
            INSERT INTO contextual_media_local_asr_imports(
                contextual_asr_import_id, import_batch_id,
                media_local_revision_id, contextual_batch_id, batch_ordinal,
                pair_projection_sha256, work_order_uri, work_order_raw_sha256,
                work_order_canonical_sha256, result_uri, result_raw_sha256,
                result_canonical_sha256, result_byte_count, input_media_id,
                input_artifact_id, input_duration_ms, max_segment_end_ms,
                input_boundary_overrun_ms, null_timed_word_count,
                result_filesystem_state, plan_sha256, imported_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 100, ?, ?, 1000, 900,
                     0, 1, 'stable_hash_bound_no_seal_claim', ?, ?, '{}')
            """,
            (
                ids["context_import"], ids["context_batch"], ids["context_revision"],
                CONTEXTUAL_BATCH_ID, index + 1, pair_projection,
                "urn:private:sha256:" + work_raw, work_raw,
                digest(tag + "-context-work-canonical"),
                "urn:private:sha256:" + context_raw, context_raw,
                contextual_result_canonical, ids["normalized"], ids["artifact"],
                digest(tag + "-context-plan"), SOURCE_TIME,
            ),
        )
        connection.execute(
            """
            INSERT INTO contextual_media_local_asr_pairs(
                contextual_pair_id, baseline_media_local_revision_id,
                contextual_media_local_revision_id, contextual_asr_import_id,
                glossary_revision_id, pair_projection_sha256, pair_state,
                input_equal, engine_equal, model_equal, window_equal,
                inference_equal, catalog_context_equal,
                only_glossary_job_output_differ, preferred_revision_id,
                correction_asserted, accuracy_claimed, improvement_claimed,
                human_review_claimed, automatic_merge_allowed,
                publication_authority, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'competing_machine_revisions_no_preference',
                     1, 1, 1, 1, 1, 1, 1, NULL, 0, 0, 0, 0, 0, 'none', ?)
            """,
            (
                ids["pair"], ids["raw_revision"], ids["context_revision"],
                ids["context_import"], GLOSSARY_REVISION_ID, pair_projection,
                SOURCE_TIME,
            ),
        )
        diff_raw = digest(tag + "-diff")
        connection.execute(
            """
            INSERT INTO contextual_asr_text_private_diffs(
                contextual_diff_id, contextual_pair_id, diff_uri,
                diff_raw_sha256, diff_canonical_sha256, diff_byte_count,
                diff_identity_sha256, block_ms, total_blocks, changed_blocks,
                unchanged_blocks, total_character_edit_distance,
                empty_nonempty_transitions,
                maximum_absolute_first_token_start_drift_ms,
                maximum_absolute_last_token_end_drift_ms,
                baseline_lexical_tokens, contextual_lexical_tokens,
                baseline_untimed_lexical_tokens,
                contextual_untimed_lexical_tokens, glossary_term_metric_count,
                transcript_text_stored, decoder_scores_calibrated,
                accuracy_claimed, improvement_claimed,
                preferred_revision_selected, human_review_claimed,
                automatic_merge_allowed, visibility, publication_authority,
                created_at
            ) VALUES(?, ?, ?, ?, ?, 100, ?, 30000, 0, 0, 0, 0, 0, 0, 0,
                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                     'private', 'none', ?)
            """,
            (
                ids["diff"], ids["pair"], "urn:private:sha256:" + diff_raw,
                diff_raw, digest(tag + "-diff-canonical"),
                digest(tag + "-diff-identity"), SOURCE_TIME,
            ),
        )
        return {
            "ordinal": index,
            "recording_id": ids["recording"],
            "canonical_key": f"youtube:fixture{index}",
            "source_id": ids["source"],
            "media_source_id": ids["media_source"],
            "recording_source_id": ids["recording_source"],
            "public_source_metadata_observation_id": ids[
                "source_metadata_observation"
            ],
            "public_source_import_observation_id": ids[
                "acquisition_observation"
            ],
            "public_source_observed_at": SOURCE_TIME,
            "parent_media_id": ids["parent"],
            "parent_rendition_id": ids["rendition"],
            "normalized_media_id": ids["normalized"],
            "input_duration_ms": 1000,
            "parent_duration_ms": 1000,
            "recording_duration_ms": 1000 + recording_delta,
            "raw_revision_id": ids["raw_revision"],
            "raw_import_id": ids["raw_import"],
            "contextual_revision_id": ids["context_revision"],
            "contextual_import_id": ids["context_import"],
            "contextual_pair_id": ids["pair"],
            "contextual_diff_id": ids["diff"],
        }

    def _write_manifest(self, *, approve: bool = False, entries=None) -> tuple[dict, dict]:
        value = {
            "schema_version": 1,
            "manifest_id": "projection-fixture-v1",
            "policy_id": "media_local_full_file_identity_v1",
            "selection": copy.deepcopy(self.entries if entries is None else entries),
            "review": {
                "reviewer_id": "reviewer_projection_fixture",
                "reviewed_at": self.reviewed_at,
                "basis": "Reviewed exact full-file lineage and duration evidence only.",
                "coordinate_attestation": COORDINATE_ATTESTATION,
                "approved_plan_sha256": None,
            },
        }
        self.manifest_path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
        plan = build_media_local_transcript_projection_plan(
            self.connection, self.manifest_path
        )
        if approve:
            value["review"]["approved_plan_sha256"] = plan["plan_sha256"]
            self.manifest_path.write_text(
                json.dumps(value, sort_keys=True), encoding="utf-8"
            )
            self.assertEqual(
                build_media_local_transcript_projection_plan(
                    self.connection, self.manifest_path
                )["plan_sha256"],
                plan["plan_sha256"],
            )
        return value, plan

    def test_plan_is_strict_deterministic_paired_and_text_free(self) -> None:
        _, first = self._write_manifest()
        second = build_media_local_transcript_projection_plan(
            self.connection, self.manifest_path
        )
        self.assertEqual(first, second)
        self.assertEqual(first["recording_count"], 5)
        self.assertEqual(first["projection_count"], 10)
        self.assertEqual(first["pair_count"], 5)
        self.assertTrue(
            all(
                len(item["generated_payload_sha256"]) == 64
                and len(item["target_child_identity_sha256"]) == 64
                for item in first["projections"]
            )
        )
        self.assertEqual(
            [item["normalized_recording_delta_ms"] for item in first["projections"]],
            [2, 2, 0, 0, 4, 4, 2, 2, 3, 3],
        )
        encoded = json.dumps(first, sort_keys=True)
        self.assertNotIn("private raw phrase", encoded)
        self.assertNotIn("private context phrase", encoded)
        self.assertIsNone(first["safety"]["target_rendition_id"])

    def test_apply_deep_replay_idempotency_and_null_rendition(self) -> None:
        _, plan = self._write_manifest(approve=True)
        applied = apply_media_local_transcript_projection_plan(
            self.connection, self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        self.assertEqual(applied["status"], "applied")
        batch_id = applied["projection_batch_id"]
        replay = require_exact_projection_batch(self.connection, batch_id)
        self.assertEqual(replay["plan_sha256"], plan["plan_sha256"])
        revisions = self.connection.execute(
            "SELECT rendition_id FROM transcript_revisions WHERE origin = ?",
            ("media_local_full_file_identity_projection_v1",),
        ).fetchall()
        self.assertEqual(len(revisions), 10)
        self.assertTrue(all(row["rendition_id"] is None for row in revisions))
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_decisions"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_gate_decisions"
            ).fetchone()[0],
            0,
        )
        with mock.patch("himr_corpus.validation._validate_contextual_media_local_files"):
            self.assertEqual(
                validate_database(self.connection)[
                    "media_local_transcript_projection_batches"
                ],
                1,
            )
        repeated = apply_media_local_transcript_projection_plan(
            self.connection, self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        self.assertEqual(repeated["status"], "already_applied")
        self.manifest_path.unlink()
        self.assertEqual(
            require_exact_projection_batch(self.connection, batch_id)["plan_sha256"],
            plan["plan_sha256"],
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "segment set is sealed"):
            self.connection.execute(
                """
                INSERT INTO transcript_segments(
                    segment_id, revision_id, ordinal, start_ms, end_ms, text,
                    metadata_json
                ) VALUES('late_segment', ?, 99, 0, 1, 'late', '{}')
                """,
                (plan["projections"][0]["target_revision_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
            self.connection.execute(
                """
                INSERT OR REPLACE INTO media_local_transcript_projections
                SELECT * FROM media_local_transcript_projections
                WHERE projection_id = ?
                """,
                (plan["projections"][0]["projection_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "one exact input"):
            self.connection.execute(
                """
                INSERT INTO run_inputs(
                    run_input_id, processing_run_id, object_type, object_id,
                    input_role, input_sha256
                ) VALUES('late_source_input', ?, 'media', ?, 'extra', ?)
                """,
                (
                    plan["projections"][0]["source_processing_run_id"],
                    plan["projections"][0]["normalized_media_id"],
                    digest("late-source-input"),
                ),
            )

    def test_unapproved_stale_and_incomplete_manifests_fail_without_writes(self) -> None:
        _, plan = self._write_manifest()
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "approved_plan_sha256"):
            apply_media_local_transcript_projection_plan(
                self.connection, self.manifest_path,
                expected_plan_sha256=plan["plan_sha256"],
            )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "3 to 5"):
            self._write_manifest(entries=self.entries[:2])
        entries = copy.deepcopy(self.entries)
        entries[0]["contextual_revision_id"] = entries[0]["raw_revision_id"]
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "contextual"):
            self._write_manifest(entries=entries)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_projection_batches"
            ).fetchone()[0],
            0,
        )

    def test_historical_replay_uses_sealed_receipts_not_mutable_current_state(self) -> None:
        _, plan = self._write_manifest(approve=True)
        result = apply_media_local_transcript_projection_plan(
            self.connection,
            self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        entry = self.entries[0]
        later_batch = identifier("imp_", "later-removed-acquisition")
        later_import_observation = identifier("iob_", "later-removed-acquisition")
        later_source_observation = identifier("smo_", "later-removed-acquisition")
        later = "2026-08-27T00:00:01Z"
        self.connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status,
                statistics_json
            ) VALUES(?, 'acquisition_result_v1', 'fixture', ?, NULL, ?, ?,
                     'completed', '{}')
            """,
            (later_batch, digest("later-removed-input"), later, later),
        )
        self.connection.execute(
            """
            INSERT INTO import_observations(
                import_observation_id, import_batch_id, importer_version,
                source_snapshot_date, observed_at, status, completed_at,
                statistics_json
            ) VALUES(?, ?, 'fixture', NULL, ?, 'completed', ?, '{}')
            """,
            (later_import_observation, later_batch, later, later),
        )
        self.connection.execute(
            """
            INSERT INTO source_metadata_observations(
                source_metadata_observation_id, source_id, import_batch_id,
                import_observation_id, observed_at, quality_rank, quality_basis,
                candidate_sha256, parent_source_id, canonical_url,
                historical_url, title, published_at, access_state, review_state,
                metadata_json
            ) VALUES(?, ?, ?, ?, ?, 700,
                     'acquisition_result_v1: locally verified acquisition result',
                     ?, NULL, NULL, NULL, NULL, NULL, 'removed',
                     'metadata_only', '{}')
            """,
            (
                later_source_observation, entry["source_id"], later_batch,
                later_import_observation, later, digest("later-removed-candidate"),
            ),
        )
        self.connection.execute(
            """
            UPDATE sources
            SET access_state = 'removed', observed_at = ?, updated_at = ?,
                current_metadata_observation_id = ?
            WHERE source_id = ?
            """,
            (later, later, later_source_observation, entry["source_id"]),
        )
        self.connection.execute(
            "UPDATE recordings SET canonical_key = ?, duration_ms = 1004 WHERE recording_id = ?",
            ("youtube:corrected-fixture0", entry["recording_id"]),
        )
        self.connection.execute(
            "UPDATE recording_sources SET confidence_state = 'disputed' "
            "WHERE recording_source_id = ?",
            (entry["recording_source_id"],),
        )
        self.connection.execute(
            "UPDATE renditions SET label = 'corrected label' WHERE rendition_id = ?",
            (entry["parent_rendition_id"],),
        )
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(?, ?, ?, 'normalized_audio_late_catalog', 'late',
                     'unreviewed', '{}')
            """,
            (
                identifier("rnd_", "late-normalized-rendition"),
                entry["recording_id"], entry["normalized_media_id"],
            ),
        )
        extra_parent = identifier("media_", "late-alternate-parent")
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(?, ?, 100, 'video', 'video/mp4', 'mp4', 1000, ?, 'verified')
            """,
            (extra_parent, digest("late-alternate-parent"), later),
        )
        self.connection.execute(
            """
            INSERT INTO media_derivations(
                child_media_id, parent_media_id, derivation_kind,
                processing_run_id, metadata_json
            ) VALUES(?, ?, 'alternate_late_catalog_lineage', NULL, '{}')
            """,
            (entry["normalized_media_id"], extra_parent),
        )
        replay = require_exact_projection_batch(
            self.connection, result["projection_batch_id"]
        )
        self.assertEqual(replay["plan_sha256"], plan["plan_sha256"])
        repeated = apply_media_local_transcript_projection_plan(
            self.connection,
            self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        self.assertEqual(repeated["status"], "already_applied")
        with mock.patch("himr_corpus.validation._validate_contextual_media_local_files"):
            validate_database(self.connection)

    def test_selected_content_and_lineage_are_sealed_after_closure(self) -> None:
        _, plan = self._write_manifest(approve=True)
        apply_media_local_transcript_projection_plan(
            self.connection,
            self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        entry = self.entries[0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "content identity"):
            self.connection.execute(
                "UPDATE media_objects SET duration_ms = duration_ms + 1 WHERE media_id = ?",
                (entry["parent_media_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "derivation is sealed"):
            self.connection.execute(
                "UPDATE media_derivations SET metadata_json = '{\"changed\":true}' "
                "WHERE child_media_id = ? AND parent_media_id = ?",
                (entry["normalized_media_id"], entry["parent_media_id"]),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "acquisition receipt"):
            self.connection.execute(
                "UPDATE media_sources SET retrieval_tool_version = '2' "
                "WHERE media_source_id = ?",
                (entry["media_source_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "mapping identity"):
            self.connection.execute(
                "UPDATE recording_sources SET mapping_method = 'changed' "
                "WHERE recording_source_id = ?",
                (entry["recording_source_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "source identity"):
            self.connection.execute(
                "UPDATE sources SET native_id = 'changed-native-id' WHERE source_id = ?",
                (entry["source_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "parent-rendition identity"):
            self.connection.execute(
                "UPDATE renditions SET rendition_kind = 'changed-kind' "
                "WHERE rendition_id = ?",
                (entry["parent_rendition_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "acquisition receipt"):
            self.connection.execute(
                "UPDATE import_observations SET statistics_json = '{\"changed\":true}' "
                "WHERE import_observation_id = ?",
                (entry["public_source_import_observation_id"],),
            )
        first = plan["projections"][0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "input artifact is sealed"):
            self.connection.execute(
                "UPDATE artifacts SET sha256 = ? WHERE artifact_id = ?",
                (digest("mutated-input-artifact"), first["input_artifact_id"]),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "producer run is sealed"):
            self.connection.execute(
                "UPDATE processing_runs SET environment_json = '{\"changed\":true}' "
                "WHERE processing_run_id = ?",
                (first["input_artifact_processing_run_id"],),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "producer inputs are sealed"
        ):
            self.connection.execute(
                """
                INSERT INTO run_inputs(
                    run_input_id, processing_run_id, object_type, object_id,
                    input_role, input_sha256
                ) VALUES(?, ?, 'media', ?, 'late_extra', ?)
                """,
                (
                    identifier("rinput_", "late-preprocess-input"),
                    first["input_artifact_processing_run_id"],
                    first["parent_media_id"], digest("late-preprocess-input"),
                ),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "producer artifacts are sealed"
        ):
            self.connection.execute(
                """
                INSERT INTO artifacts(
                    artifact_id, processing_run_id, artifact_kind, storage_uri,
                    sha256, byte_count, schema_version, visibility, metadata_json
                ) VALUES(?, ?, 'late_extra', 'urn:private:late', ?, 1, 1,
                         'private', '{}')
                """,
                (
                    identifier("artifact_", "late-preprocess-artifact"),
                    first["input_artifact_processing_run_id"],
                    digest("late-preprocess-artifact"),
                ),
            )
        preprocess_batch_id = self.connection.execute(
            "SELECT preprocess_import_batch_id FROM media_local_asr_imports "
            "WHERE media_local_asr_import_id = ?",
            (entry["raw_import_id"],),
        ).fetchone()[0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "preprocess import"):
            self.connection.execute(
                "UPDATE import_batches SET statistics_json = '{\"changed\":true}' "
                "WHERE import_batch_id = ?",
                (preprocess_batch_id,),
            )

    def test_plan_rejects_failed_source_run(self) -> None:
        raw_revision = self.entries[0]["raw_revision_id"]
        raw_run = self.connection.execute(
            "SELECT processing_run_id FROM media_local_transcript_revisions "
            "WHERE media_local_revision_id = ?",
            (raw_revision,),
        ).fetchone()[0]
        self.connection.execute(
            "UPDATE processing_runs SET status = 'failed', error_text = 'fixture failure' "
            "WHERE processing_run_id = ?",
            (raw_run,),
        )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "completed"):
            self._write_manifest()

    def test_plan_rejects_wrong_asr_importer(self) -> None:
        raw_batch = self.connection.execute(
            "SELECT import_batch_id FROM media_local_asr_imports "
            "WHERE media_local_asr_import_id = ?",
            (self.entries[0]["raw_import_id"],),
        ).fetchone()[0]
        self.connection.execute(
            "UPDATE import_batches SET importer_name = 'wrong_fixture_importer' "
            "WHERE import_batch_id = ?",
            (raw_batch,),
        )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "import batch"):
            self._write_manifest()

    def test_plan_rejects_non_flac_normalized_media(self) -> None:
        self.connection.execute(
            "UPDATE media_objects SET container = 'wav', mime_type = 'audio/wav' "
            "WHERE media_id = ?",
            (self.entries[0]["normalized_media_id"],),
        )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "audio/flac"):
            self._write_manifest()

    def test_plan_rejects_media_source_with_source_snapshot(self) -> None:
        entry = self.entries[0]
        snapshot_id = identifier("ssn_", "projection-fixture-snapshot")
        self.connection.execute(
            """
            INSERT INTO source_snapshots(
                source_snapshot_id, source_id, observed_at, request_url,
                final_url, http_status, payload_sha256, artifact_path,
                metadata_json, import_batch_id
            ) VALUES(?, ?, ?, NULL, NULL, 200, ?, NULL, '{}', ?)
            """,
            (
                snapshot_id, entry["source_id"], SOURCE_TIME,
                digest("projection-fixture-snapshot"),
                identifier("imp_", "recording-0-acquisition"),
            ),
        )
        self.connection.execute(
            "UPDATE media_sources SET source_snapshot_id = ? "
            "WHERE media_source_id = ?",
            (snapshot_id, entry["media_source_id"]),
        )
        with self.assertRaisesRegex(
            MediaLocalTranscriptProjectionError, "without a mutable source snapshot"
        ):
            self._write_manifest()

    def test_review_must_postdate_source_and_preprocess_runs(self) -> None:
        raw_run_id = self.connection.execute(
            "SELECT processing_run_id FROM media_local_transcript_revisions "
            "WHERE media_local_revision_id = ?",
            (self.entries[0]["raw_revision_id"],),
        ).fetchone()[0]
        preprocess_run_id = self.connection.execute(
            "SELECT processing_run_id FROM artifacts WHERE artifact_id = ?",
            (
                self.connection.execute(
                    "SELECT input_artifact_id FROM media_local_transcript_revisions "
                    "WHERE media_local_revision_id = ?",
                    (self.entries[0]["raw_revision_id"],),
                ).fetchone()[0],
            ),
        ).fetchone()[0]
        for run_id in (raw_run_id, preprocess_run_id):
            with self.subTest(run_id=run_id):
                self.connection.execute(
                    "UPDATE processing_runs SET completed_at = '2099-01-01T00:00:00Z' "
                    "WHERE processing_run_id = ?",
                    (run_id,),
                )
                with self.assertRaisesRegex(
                    MediaLocalTranscriptProjectionError, "coordinate review predates"
                ):
                    self._write_manifest()
                self.connection.execute(
                    "UPDATE processing_runs SET completed_at = ? "
                    "WHERE processing_run_id = ?",
                    (SOURCE_TIME, run_id),
                )

    def test_sql_insert_guard_rejects_equal_count_nonexact_target_copy(self) -> None:
        _, plan = self._write_manifest(approve=True)
        exact_rows = projection_module._expected_target_rows

        def substituted_rows(connection, item):
            segments, words = exact_rows(connection, item)
            if item["selection_ordinal"] == 0 and item["variant"] == "raw":
                segments[0]["text"] = "same count but not the source text"
                words[0]["token"] = "substituted-token"
            return segments, words

        with (
            mock.patch.object(
                projection_module,
                "_expected_target_rows",
                side_effect=substituted_rows,
            ),
            self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "differs from exact admitted source",
            ),
        ):
            apply_media_local_transcript_projection_plan(
                self.connection,
                self.manifest_path,
                expected_plan_sha256=plan["plan_sha256"],
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_projection_batches"
            ).fetchone()[0],
            0,
        )

    def test_generated_metadata_and_run_payload_drift_is_sql_rejected(self) -> None:
        _, plan = self._write_manifest(approve=True)
        approved_payload = {
            key: value for key, value in plan.items() if key != "plan_sha256"
        }
        for helper_name in (
            "_projection_parameters",
            "_projection_environment",
            "_revision_metadata",
            "_projection_metadata",
        ):
            original = getattr(projection_module, helper_name)

            def unsafe_payload(*args, _original=original, _name=helper_name):
                value = json.loads(_original(*args))
                value["unapproved_fixture_field"] = _name
                if _name == "_revision_metadata":
                    value["accuracy_claimed"] = True
                return projection_module.canonical_json(value)

            with (
                self.subTest(helper=helper_name),
                mock.patch.object(
                    projection_module,
                    "_plan_payload",
                    return_value=approved_payload,
                ),
                mock.patch.object(
                    projection_module,
                    helper_name,
                    side_effect=unsafe_payload,
                ),
                self.assertRaisesRegex(
                    sqlite3.IntegrityError, "differs from exact admitted source"
                ),
            ):
                apply_media_local_transcript_projection_plan(
                    self.connection,
                    self.manifest_path,
                    expected_plan_sha256=plan["plan_sha256"],
                )
            self.assertEqual(
                self.connection.execute(
                    "SELECT count(*) FROM media_local_transcript_projection_batches"
                ).fetchone()[0],
                0,
            )

    def test_generated_child_id_drift_fails_deep_replay_and_rolls_back(self) -> None:
        _, plan = self._write_manifest(approve=True)
        exact_rows = projection_module._expected_target_rows

        def changed_ids(connection, item):
            segments, words = exact_rows(connection, item)
            if item["selection_ordinal"] == 0 and item["variant"] == "raw":
                old_segment_id = segments[0]["segment_id"]
                segments[0]["segment_id"] = identifier(
                    "ts_", "unapproved-target-segment-id"
                )
                for word in words:
                    if word["segment_id"] == old_segment_id:
                        word["segment_id"] = segments[0]["segment_id"]
                        word["word_id"] = identifier(
                            "tw_", "unapproved-target-word-id"
                        )
            return segments, words

        with (
            mock.patch.object(
                projection_module,
                "_expected_target_rows",
                side_effect=changed_ids,
            ),
            self.assertRaisesRegex(
                MediaLocalTranscriptProjectionError, "deterministic child IDs"
            ),
        ):
            apply_media_local_transcript_projection_plan(
                self.connection,
                self.manifest_path,
                expected_plan_sha256=plan["plan_sha256"],
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_projection_batches"
            ).fetchone()[0],
            0,
        )

    def test_sql_insert_guard_rejects_preexisting_extra_source_asr_input(self) -> None:
        _, plan = self._write_manifest(approve=True)
        item = plan["projections"][0]
        self.connection.execute(
            """
            INSERT INTO run_inputs(
                run_input_id, processing_run_id, object_type, object_id,
                input_role, input_sha256
            ) VALUES(?, ?, 'media', ?, 'unexpected_second_input', ?)
            """,
            (
                identifier("rinput_", "preexisting-extra-source-input"),
                item["source_processing_run_id"], item["normalized_media_id"],
                digest("preexisting-extra-source-input"),
            ),
        )
        approved_payload = {
            key: value for key, value in plan.items() if key != "plan_sha256"
        }
        with (
            mock.patch.object(
                projection_module,
                "_plan_payload",
                return_value=approved_payload,
            ),
            self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "differs from exact admitted source",
            ),
        ):
            apply_media_local_transcript_projection_plan(
                self.connection,
                self.manifest_path,
                expected_plan_sha256=plan["plan_sha256"],
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_projection_batches"
            ).fetchone()[0],
            0,
        )

    def test_deep_replay_reruns_strict_embedded_manifest_validation(self) -> None:
        _, plan = self._write_manifest(approve=True)
        result = apply_media_local_transcript_projection_plan(
            self.connection,
            self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        batch_id = result["projection_batch_id"]
        raw = json.loads(
            self.connection.execute(
                "SELECT manifest_raw_json FROM media_local_transcript_projection_batches "
                "WHERE projection_batch_id = ?",
                (batch_id,),
            ).fetchone()[0]
        )
        raw["unexpected_embedded_field"] = "must fail strict replay"
        raw_text = json.dumps(raw, sort_keys=True, separators=(",", ":"))
        raw_sha = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        self.connection.execute("DROP TRIGGER media_local_projection_batches_no_update")
        self.connection.execute(
            """
            UPDATE media_local_transcript_projection_batches
            SET manifest_raw_json = ?, manifest_raw_sha256 = ?,
                manifest_byte_count = ?
            WHERE projection_batch_id = ?
            """,
            (raw_text, raw_sha, len(raw_text.encode("utf-8")), batch_id),
        )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "unknown"):
            require_exact_projection_batch(self.connection, batch_id)

    def test_deep_replay_rejects_unknown_top_level_plan_fields(self) -> None:
        _, plan = self._write_manifest(approve=True)
        result = apply_media_local_transcript_projection_plan(
            self.connection,
            self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        batch_id = result["projection_batch_id"]
        stored = json.loads(
            self.connection.execute(
                "SELECT plan_json FROM media_local_transcript_projection_batches "
                "WHERE projection_batch_id = ?",
                (batch_id,),
            ).fetchone()[0]
        )
        stored["transcript_text"] = "forbidden even in a self-described plan"
        self.connection.execute("DROP TRIGGER media_local_projection_batches_no_update")
        self.connection.execute(
            "UPDATE media_local_transcript_projection_batches SET plan_json = ? "
            "WHERE projection_batch_id = ?",
            (json.dumps(stored, sort_keys=True, separators=(",", ":")), batch_id),
        )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "unknown"):
            require_exact_projection_batch(self.connection, batch_id)

    def test_breakfast_shape_is_excluded_even_without_overrun(self) -> None:
        entry = copy.deepcopy(self.entries[0])
        self.connection.execute(
            "UPDATE media_objects SET duration_ms = 1001 WHERE media_id = ?",
            (entry["parent_media_id"],),
        )
        self.connection.execute(
            "UPDATE recordings SET duration_ms = 5592 WHERE recording_id = ?",
            (entry["recording_id"],),
        )
        entry["parent_duration_ms"] = 1001
        entry["recording_duration_ms"] = 5592
        entries = [entry, *copy.deepcopy(self.entries[1:])]
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "parent duration delta"):
            self._write_manifest(entries=entries)

    def test_recording_duration_delta_five_passes_and_six_fails(self) -> None:
        entry = copy.deepcopy(self.entries[0])
        self.connection.execute(
            "UPDATE recordings SET duration_ms = 1005 WHERE recording_id = ?",
            (entry["recording_id"],),
        )
        entry["recording_duration_ms"] = 1005
        entries = [entry, *copy.deepcopy(self.entries[1:])]
        _, plan = self._write_manifest(entries=entries)
        self.assertEqual(plan["projections"][0]["normalized_recording_delta_ms"], 5)
        self.connection.execute(
            "UPDATE recordings SET duration_ms = 1006 WHERE recording_id = ?",
            (entry["recording_id"],),
        )
        entries[0]["recording_duration_ms"] = 1006
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "exceeds 5 ms"):
            self._write_manifest(entries=entries)

    def test_exact_replay_detects_equal_count_content_substitution(self) -> None:
        _, plan = self._write_manifest(approve=True)
        result = apply_media_local_transcript_projection_plan(
            self.connection, self.manifest_path,
            expected_plan_sha256=plan["plan_sha256"],
        )
        target = plan["projections"][0]["target_revision_id"]
        self.connection.execute("DROP TRIGGER transcript_segments_no_update")
        self.connection.execute(
            "UPDATE transcript_segments SET text = 'substituted' WHERE revision_id = ? AND ordinal = 0",
            (target,),
        )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "segment/word"):
            require_exact_projection_batch(
                self.connection, result["projection_batch_id"]
            )
        with (
            mock.patch("himr_corpus.validation._validate_contextual_media_local_files"),
            self.assertRaisesRegex(RuntimeError, "reviewed identity contract"),
        ):
            validate_database(self.connection)

    def test_manifest_loader_rejects_duplicate_keys(self) -> None:
        self.manifest_path.write_text(
            '{"schema_version":1,"schema_version":1}', encoding="utf-8"
        )
        with self.assertRaisesRegex(MediaLocalTranscriptProjectionError, "duplicate"):
            load_media_local_transcript_projection_manifest(self.manifest_path)

    def test_batch_header_cannot_precede_or_replace_children(self) -> None:
        _, plan = self._write_manifest(approve=True)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "incomplete"):
            self.connection.execute(
                """
                INSERT INTO media_local_transcript_projection_batches(
                    projection_batch_id, import_batch_id, manifest_id, manifest_uri,
                    manifest_raw_sha256, manifest_canonical_sha256,
                    manifest_core_sha256, manifest_byte_count,
                    manifest_raw_json, manifest_json,
                    plan_sha256, schema_version, policy_id, plan_json, reviewer_id,
                    reviewed_at, review_basis, coordinate_attestation,
                    projection_count, recording_count, pair_count, segment_count,
                    word_count, applied_at
                ) VALUES('premature', ?, 'premature', 'fixture', ?, ?, ?, 2, '{}', '{}',
                         ?, 1, 'media_local_full_file_identity_v1', '{}',
                         'reviewer_projection_fixture', ?, 'fixture', ?,
                         6, 3, 3, 1, 0, ?)
                """,
                (
                    identifier("imp_", "premature"), digest("premature-raw"),
                    digest("premature-canonical"), digest("premature-core"),
                    plan["plan_sha256"], self.reviewed_at,
                    COORDINATE_ATTESTATION, self.reviewed_at,
                ),
            )


if __name__ == "__main__":
    unittest.main()
