from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.exporter import build_release, export_release, validate_release_shape  # noqa: E402
from himr_corpus.ids import recording_id, source_id, stable_id  # noqa: E402
from himr_corpus.importers import (  # noqa: E402
    approve_public_source_metadata,
    import_archive_url_hints,
    import_current_channel,
    import_internet_archive,
    import_legacy_manifest,
    import_snapshot_bundle,
    import_torrent_manifest,
    import_youtube_discovery_candidates,
    import_ytdlp_info,
    import_ytdlp_infos,
)
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


ARCHIVE_DIR = REPOSITORY_ROOT / "research/archive-snapshots/2026-08-25"
CHANNEL_DIR = REPOSITORY_ROOT / "research/channel-snapshots/2026-08-25"
DISCOVERY_ROOT = REPOSITORY_ROOT / "research/corpus/discovery"
ACQUISITION_ROOT = REPOSITORY_ROOT / "research/corpus/acquisition"


class CorpusFoundationTests(unittest.TestCase):
    def new_database(self):
        temporary = tempfile.TemporaryDirectory()
        connection = connect(Path(temporary.name) / "corpus.sqlite3")
        migrate(connection)
        self.addCleanup(connection.close)
        self.addCleanup(temporary.cleanup)
        return connection, Path(temporary.name)

    def _clear_publication_gates(self, connection, targets=None):
        gate_reviewer = "reviewer_test_gate_clear"
        if connection.execute(
            "SELECT 1 FROM reviewers WHERE reviewer_id = ?", (gate_reviewer,)
        ).fetchone() is None:
            register_reviewer_fixture(
                connection, gate_reviewer, "Test Gate Reviewer", "human"
            )
        if targets is None:
            rows = connection.execute(
                """
                SELECT object_type, object_id, reviewer_id, decided_at
                FROM current_publication_decisions
                WHERE decision = 'publish'
                ORDER BY object_type, object_id
                """
            ).fetchall()
        else:
            rows = []
            for object_type, object_id in targets:
                row = connection.execute(
                    """
                    SELECT object_type, object_id, reviewer_id, decided_at
                    FROM current_publication_decisions
                    WHERE object_type = ? AND object_id = ? AND decision = 'publish'
                    """,
                    (object_type, object_id),
                ).fetchone()
                if row:
                    rows.append(row)
        connection.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                for gate_kind in ("rights", "privacy", "sensitivity"):
                    gate_id = stable_id(
                        "pgt", row["object_type"], row["object_id"], gate_kind, "test-clear"
                    )
                    if connection.execute(
                        """
                        SELECT 1 FROM publication_gate_decisions
                        WHERE publication_gate_decision_id = ?
                        """,
                        (gate_id,),
                    ).fetchone() is not None:
                        continue
                    connection.execute(
                        """
                        INSERT INTO publication_gate_decisions(
                            publication_gate_decision_id, object_type, object_id,
                            gate_kind, decision, reviewer_id, decided_at, basis
                        ) VALUES(?, ?, ?, ?, 'clear', ?, ?, 'synthetic completed gate')
                        """,
                        (
                            gate_id,
                            row["object_type"],
                            row["object_id"],
                            gate_kind,
                            gate_reviewer,
                            row["decided_at"],
                        ),
                    )
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()

    def test_migrations_are_checksummed_and_schema_is_complete(self):
        connection, _ = self.new_database()
        self.assertEqual(migrate(connection), [])
        versions = connection.execute(
            "SELECT version, name, length(sha256) FROM schema_migrations ORDER BY version"
        ).fetchall()
        self.assertEqual(
            [row[0] for row in versions],
            [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                29, 30, 31, 32, 33, 34,
            ],
        )
        self.assertTrue(all(row[2] == 64 for row in versions))
        expected_tables = {
            "sources",
            "import_observations",
            "source_metadata_observations",
            "source_relation_observations",
            "external_id_observations",
            "recordings",
            "recording_metadata_observations",
            "media_objects",
            "processing_runs",
            "transcript_revisions",
            "observations",
            "entities",
            "events",
            "review_tasks",
            "publication_decisions",
            "publication_gate_decisions",
            "publication_manifest_imports",
            "machine_transcript_publication_policy_runs",
            "reviewer_admin_manifest_imports",
            "reviewer_admin_events",
            "model_registry_manifest_imports",
            "model_registry_manifest_models",
            "claim_catalog_links",
            "identity_cluster_versions",
            "identity_cluster_memberships",
            "identity_cannot_link_decisions",
            "biometric_artifacts",
            "identity_assertion_subjects",
            "identity_assertion_decisions",
            "audio_fingerprint_observations",
            "audio_fingerprint_match_candidates",
            "audio_fingerprint_match_candidates_v2",
            "audio_fingerprint_compare_v2_receipts",
            "audio_fingerprint_compare_v2_sides",
            "audio_fingerprint_result_imports",
            "visual_fingerprint_observations",
            "visual_fingerprint_result_imports",
            "visual_fingerprint_compare_imports",
            "visual_fingerprint_compare_sides",
            "visual_fingerprint_compare_side_frames",
            "visual_fingerprint_comparisons",
            "visual_fingerprint_compare_top_pairs",
            "visual_fingerprint_compare_completion_receipts",
            "archive_bracket_reconciliation_imports",
            "archive_bracket_youtube_candidates",
            "archive_bracket_reconciliation_issues",
            "torrent_bracket_reconciliation_imports",
            "torrent_bracket_youtube_candidates",
            "archive_hint_reconciliation_imports",
            "archive_hint_provider_candidates",
            "rendition_local_asr_imports",
            "rendition_local_transcript_revisions",
            "rendition_local_transcript_segments",
            "rendition_local_transcript_words",
            "media_local_asr_imports",
            "private_gpu_v3_asr_imports",
            "media_local_transcript_revisions",
            "media_local_transcript_segments",
            "media_local_transcript_words",
            "private_glossary_registrations",
            "contextual_asr_batch_registrations",
            "contextual_media_local_asr_imports",
            "contextual_media_local_asr_pairs",
            "contextual_asr_text_private_diffs",
            "media_local_transcript_projection_batches",
            "media_local_transcript_projections",
            "media_local_transcript_projection_pairs",
            "source_recording_transform_candidates",
            "transcript_lifecycle_decisions",
            "acquisition_handling_restrictions",
            "event_participant_publication_subjects",
            "solo_voice_manifest_imports",
            "solo_voice_subjects",
            "solo_voice_privacy_reviews",
            "solo_voice_attestation_decisions",
            "private_ocr_import_receipts",
            "private_ocr_frame_admissions",
            "private_ocr_word_admissions",
            "private_ocr_word_fts",
        }
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        self.assertTrue(expected_tables <= tables)
        connection.execute(
            "UPDATE schema_migrations SET sha256 = ? WHERE version = 1", ("0" * 64,)
        )
        with self.assertRaisesRegex(RuntimeError, "append-only"):
            migrate(connection)

    def test_migration_and_ledger_are_atomic_after_injected_ledger_failure(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        connection = connect(Path(temporary.name) / "migration-failure.sqlite3")
        self.addCleanup(connection.close)
        connection.execute(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
                applied_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TRIGGER reject_migration_ledger
            BEFORE INSERT ON schema_migrations
            BEGIN
                SELECT RAISE(ABORT, 'injected ledger failure');
            END
            """
        )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "injected ledger failure"):
            migrate(connection)
        self.assertFalse(connection.in_transaction)
        self.assertEqual(
            connection.execute("SELECT count(*) FROM schema_migrations").fetchone()[0], 0
        )
        self.assertIsNone(
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sources'"
            ).fetchone()
        )

        connection.execute("DROP TRIGGER reject_migration_ledger")
        self.assertEqual(len(migrate(connection)), 34)
        self.assertEqual(
            connection.execute("SELECT count(*) FROM schema_migrations").fetchone()[0],
            34,
        )

    def test_audio_v2_canonical_migration_aborts_on_provisional_existing_row(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        migrations = root / "migrations"
        migrations.mkdir()
        source_migrations = CORPUS_ROOT / "migrations"
        for migration in sorted(source_migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            if int(migration.name[:4]) <= 16:
                shutil.copy2(migration, migrations / migration.name)
        connection = connect(root / "pre-0017.sqlite3")
        self.addCleanup(connection.close)
        with mock.patch("himr_corpus.db.MIGRATIONS_DIR", migrations):
            self.assertEqual(len(migrate(connection)), 16)
            connection.execute("DROP TRIGGER audio_fingerprint_match_candidates_v2_admission")
            connection.execute(
                "DROP TRIGGER audio_fingerprint_match_candidates_v2_exact_input_count"
            )
            for run_id, stage, implementation in (
                ("weak_query_run", "audio_fingerprint_chromaprint", "0.1.0"),
                ("weak_candidate_run", "audio_fingerprint_chromaprint", "0.1.0"),
                ("weak_compare_run", "audio_fingerprint_exact_compare_v2", "0.2.0"),
            ):
                connection.execute(
                    """
                    INSERT INTO processing_runs(
                        processing_run_id, stage, implementation_version,
                        parameters_json, environment_json, started_at,
                        completed_at, status
                    ) VALUES(?, ?, ?, '{}', '{}', ?, ?, 'completed')
                    """,
                    (run_id, stage, implementation, "2026-08-26T00:00:00Z", "2026-08-26T00:00:00Z"),
                )
            for suffix in ("query", "candidate"):
                connection.execute(
                    """
                    INSERT INTO media_objects(
                        media_id, sha256, byte_count, media_kind,
                        first_cataloged_at, integrity_state
                    ) VALUES(?, ?, 1, 'audio', ?, 'verified')
                    """,
                    (
                        f"weak_media_{suffix}",
                        ("a" if suffix == "query" else "b") * 64,
                        "2026-08-26T00:00:00Z",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO fingerprints(
                        fingerprint_id, media_id, fingerprint_kind,
                        implementation_version, start_ms, end_ms, artifact_uri
                    ) VALUES(?, ?, 'chromaprint_raw', ?, 0, 1, ?)
                    """,
                    (
                        f"weak_fingerprint_{suffix}",
                        f"weak_media_{suffix}",
                        f"weak-{suffix}",
                        f"file:///weak-{suffix}.raw",
                    ),
                )
            connection.execute(
                """
                INSERT INTO match_candidates(
                    match_candidate_id, left_object_type, left_object_id,
                    right_object_type, right_object_id, match_method, raw_score,
                    calibrated_probability, decision_state, metadata_json
                ) VALUES('weak_v2_match', 'fingerprint', 'weak_fingerprint_query',
                         'fingerprint', 'weak_fingerprint_candidate',
                         'chromaprint_exact_raw_bytes_v2', 1.0, NULL,
                         'candidate', '{}')
                """
            )
            connection.execute(
                """
                INSERT INTO audio_fingerprint_match_candidates_v2(
                    match_candidate_id, processing_run_id,
                    query_extraction_run_id, candidate_extraction_run_id,
                    query_result_sha256, candidate_result_sha256,
                    query_fingerprint_id, candidate_fingerprint_id,
                    comparison_method, score_semantics, calibration_state
                ) VALUES('weak_v2_match', 'weak_compare_run', 'weak_query_run',
                         'weak_candidate_run', ?, ?, 'weak_fingerprint_query',
                         'weak_fingerprint_candidate', 'exact_raw_bytes_v2',
                         'boolean_raw_byte_equality_not_probability',
                         'not_calibrated')
                """,
                ("c" * 64, "d" * 64),
            )
            migration_0017 = source_migrations / (
                "0017_audio_fingerprint_exact_compare_v2_canonical.sql"
            )
            shutil.copy2(migration_0017, migrations / migration_0017.name)
            with self.assertRaisesRegex(sqlite3.IntegrityError, "row_count"):
                migrate(connection)
            self.assertEqual(
                connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0],
                16,
            )
            self.assertIsNone(
                connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name = 'audio_fingerprint_compare_v2_receipts'"
                ).fetchone()
            )

    def test_publication_gates_access_downgrade_and_same_time_remove_fail_closed(self):
        connection, temporary = self.new_database()
        observed = "2026-08-26T12:00:00Z"
        video_id = "safety00001"
        inventory = temporary / "inventory.json"
        inventory.write_text(
            json.dumps(
                {
                    "expected_channel": {
                        "stable_channel_id": "UC_yIF-9jOge6nNA0z-ScrBQ",
                        "display_name": "Hiding in my room",
                    },
                    "items": [
                        {
                            "video_id": video_id,
                            "type": "video",
                            "observed": {
                                "title": "Publication safety fixture",
                                "publish_date_utc": observed,
                                "duration_seconds": 5,
                                "inferred_access": "public",
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        import_current_channel(
            connection, inventory, snapshot_date="2026-08-26", observed_at=observed
        )
        self.assertEqual(
            approve_public_source_metadata(connection),
            {"public_recordings": 0, "public_sources": 0, "recording_decisions_added": 1,
             "source_decisions_added": 1},
        )
        source = source_id("youtube", "youtube_video", video_id)
        recording = recording_id(f"youtube:video:{video_id}")
        targets = (("source", source), ("recording", recording))
        gate_reviewer = "reviewer_test_gate_clear"
        register_reviewer_fixture(
            connection, gate_reviewer, "Test Gate Reviewer", "human"
        )

        for gate_kind in ("rights", "privacy"):
            for object_type, object_id in targets:
                connection.execute(
                    """
                    INSERT INTO publication_gate_decisions(
                        publication_gate_decision_id, object_type, object_id,
                        gate_kind, decision, reviewer_id, decided_at, basis
                    ) VALUES(?, ?, ?, ?, 'clear', ?, ?, 'synthetic cleared gate')
                    """,
                    (
                        stable_id("pgt", object_type, object_id, gate_kind, "partial"),
                        object_type,
                        object_id,
                        gate_kind,
                        gate_reviewer,
                        observed,
                    ),
                )
        self.assertEqual(connection.execute("SELECT count(*) FROM public_sources").fetchone()[0], 0)
        self.assertEqual(connection.execute("SELECT count(*) FROM public_recordings").fetchone()[0], 0)

        connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id, gate_kind,
                decision, reviewer_id, decided_at, basis
            ) VALUES(?, 'source', ?, 'sensitivity', 'clear', ?, ?, 'source checked')
            """,
            (
                stable_id("pgt", "source", source, "sensitivity", "partial"),
                source,
                gate_reviewer,
                observed,
            ),
        )
        self.assertEqual(connection.execute("SELECT count(*) FROM public_sources").fetchone()[0], 1)
        self.assertEqual(connection.execute("SELECT count(*) FROM public_recordings").fetchone()[0], 0)

        connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id, gate_kind,
                decision, reviewer_id, decided_at, basis
            ) VALUES(?, 'recording', ?, 'sensitivity', 'clear', ?, ?, 'recording checked')
            """,
            (
                stable_id("pgt", "recording", recording, "sensitivity", "partial"),
                recording,
                gate_reviewer,
                observed,
            ),
        )
        self.assertEqual(connection.execute("SELECT count(*) FROM public_recordings").fetchone()[0], 1)

        connection.execute(
            "UPDATE sources SET access_state = 'members_only' WHERE source_id = ?", (source,)
        )
        self.assertEqual(connection.execute("SELECT count(*) FROM public_sources").fetchone()[0], 0)
        self.assertEqual(connection.execute("SELECT count(*) FROM public_recordings").fetchone()[0], 0)
        connection.execute(
            "UPDATE sources SET access_state = 'public' WHERE source_id = ?", (source,)
        )

        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES(?, 'source', ?, 'remove', ?, ?, 'same-time takedown')
            """,
            (
                stable_id("pub", "source", source, "same-time-remove"),
                source,
                gate_reviewer,
                observed,
            ),
        )
        self.assertEqual(
            connection.execute(
                "SELECT decision FROM current_publication_decisions "
                "WHERE object_type = 'source' AND object_id = ?",
                (source,),
            ).fetchone()[0],
            "remove",
        )
        self.assertEqual(connection.execute("SELECT count(*) FROM public_sources").fetchone()[0], 0)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "may not weaken a takedown"):
            connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES(?, 'source', ?, 'publish', ?, ?, 'unsafe same-time republish')
                """,
                (
                    stable_id("pub", "source", source, "same-time-republish"),
                    source,
                    gate_reviewer,
                    observed,
                ),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "UPDATE publication_decisions SET decision = 'publish' "
                "WHERE object_type = 'source' AND object_id = ? AND decision = 'remove'",
                (source,),
            )

        later = "2026-08-26T12:00:01Z"
        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES(?, 'source', ?, 'publish', ?, ?, 'reviewed later republication')
            """,
            (
                stable_id("pub", "source", source, "later-republish"),
                source,
                gate_reviewer,
                later,
            ),
        )
        self.assertEqual(connection.execute("SELECT count(*) FROM public_sources").fetchone()[0], 1)
        self.assertEqual(connection.execute("SELECT count(*) FROM public_recordings").fetchone()[0], 1)
        validate_database(connection)

    def test_real_snapshots_discovery_idempotency_and_public_export(self):
        required = [
            ARCHIVE_DIR / "source-manifest.jsonl",
            ARCHIVE_DIR / "raw/internet-archive-69999.json",
            ARCHIVE_DIR / "raw/internet-archive-699992.json",
            CHANNEL_DIR / "inventory.json",
            DISCOVERY_ROOT / "internet-archive/28766/metadata.json",
            DISCOVERY_ROOT / "youtube/search-himr-daniel-lord/candidates.jsonl",
            DISCOVERY_ROOT / "reddit/1q2sk8g/archive-urls.utf8.txt",
            DISCOVERY_ROOT / "reddit/1pqdsxm/hiding-in-my-room.torrent",
            DISCOVERY_ROOT / "reddit/1pqdsxm/discovery.json",
            ACQUISITION_ROOT / "youtube/h3ySLeBAoXs/h3ySLeBAoXs.info.json",
        ]
        metadata_directory = DISCOVERY_ROOT / "youtube/search-himr-daniel-lord/metadata"
        missing = [path for path in required if not path.is_file()]
        metadata_files = list(metadata_directory.glob("*.info.json"))
        if missing or len(metadata_files) != 50:
            self.skipTest(
                "private integration fixtures are unavailable in this checkout"
            )

        connection, temporary = self.new_database()
        baseline = import_snapshot_bundle(connection, ARCHIVE_DIR, CHANNEL_DIR)
        self.assertEqual(baseline["internet_archive"]["video_files"], 3112)
        self.assertEqual(baseline["internet_archive"]["archive_items"], 2)
        self.assertEqual(baseline["legacy_manifest"]["legacy_entries"], 2227)
        self.assertEqual(baseline["legacy_manifest"]["mapped_entries"], 2043)
        self.assertEqual(baseline["legacy_manifest"]["unresolved_entries"], 184)
        self.assertEqual(baseline["legacy_manifest"]["shared_mapping_groups"], 65)
        self.assertEqual(baseline["legacy_manifest"]["explicit_reconciliations"], 3)
        self.assertEqual(baseline["legacy_manifest"]["old_transcript_revisions_imported"], 0)
        self.assertEqual(baseline["current_channel"]["videos"], 24)
        self.assertEqual(connection.execute("SELECT count(*) FROM sources").fetchone()[0], 5366)
        self.assertEqual(connection.execute("SELECT count(*) FROM recordings").fetchone()[0], 2032)
        self.assertEqual(connection.execute("SELECT count(*) FROM review_tasks").fetchone()[0], 249)
        self.assertEqual(connection.execute("SELECT count(*) FROM transcript_revisions").fetchone()[0], 0)
        self.assertEqual(build_release(connection)["counts"]["recordings"], 0)

        counts_before_repeat = self._core_counts(connection)
        self.assertEqual(import_snapshot_bundle(connection, ARCHIVE_DIR, CHANNEL_DIR), baseline)
        self.assertEqual(self._core_counts(connection), counts_before_repeat)
        self.assertEqual(connection.execute("SELECT count(*) FROM import_batches").fetchone()[0], 3)

        additional_archive = import_internet_archive(
            connection,
            [DISCOVERY_ROOT / "internet-archive/28766/metadata.json"],
            snapshot_date="2026-08-26",
            observed_at="2026-08-26T18:20:00Z",
        )
        self.assertEqual(additional_archive["video_files"], 433)
        self.assertEqual(additional_archive["archive_items"], 1)
        candidates = import_youtube_discovery_candidates(
            connection,
            DISCOVERY_ROOT / "youtube/search-himr-daniel-lord/candidates.jsonl",
            observed_at="2026-08-26T18:23:47Z",
            query_label='"Hiding In My Room" Daniel Lord',
        )
        self.assertEqual(candidates["candidate_rows"], 50)
        self.assertEqual(candidates["new_candidate_sources"], 49)
        self.assertEqual(candidates["deduplicated_existing_sources"], 1)
        h3_source = source_id("youtube", "youtube_video", "h3ySLeBAoXs")
        self.assertEqual(
            tuple(connection.execute(
                "SELECT review_state, access_state FROM sources WHERE source_id = ?",
                (h3_source,),
            ).fetchone()),
            ("unreviewed", "unknown"),
        )
        ytdlp = import_ytdlp_info(
            connection,
            ACQUISITION_ROOT / "youtube/h3ySLeBAoXs/h3ySLeBAoXs.info.json",
            observed_at="2026-08-26T18:30:00Z",
        )
        self.assertEqual(ytdlp, {"videos": 1, "channels": 1})
        self.assertEqual(
            tuple(connection.execute(
                "SELECT review_state, access_state FROM sources WHERE source_id = ?",
                (h3_source,),
            ).fetchone()),
            ("metadata_only", "public"),
        )
        ytdlp_bulk = import_ytdlp_infos(
            connection,
            [DISCOVERY_ROOT / "youtube/search-himr-daniel-lord/metadata"],
            observed_at="2026-08-26T18:30:00Z",
        )
        self.assertEqual(ytdlp_bulk, {
            "channels": 50,
            "info_files": 50,
            "unique_channels": 31,
            "videos": 50,
        })
        hints = import_archive_url_hints(
            connection,
            DISCOVERY_ROOT / "reddit/1q2sk8g/archive-urls.utf8.txt",
            observed_at="2026-08-26T18:20:00Z",
            reddit_post_id="1q2sk8g",
        )
        self.assertEqual(hints["valid_hints"], 731)
        self.assertEqual(hints["resolved_existing_sources"], 510)
        self.assertEqual(hints["new_hint_sources"], 221)
        torrent = import_torrent_manifest(
            connection,
            DISCOVERY_ROOT / "reddit/1pqdsxm/hiding-in-my-room.torrent",
            discovery_metadata_path=DISCOVERY_ROOT / "reddit/1pqdsxm/discovery.json",
            observed_at="2026-08-26T18:20:00Z",
        )
        self.assertEqual(torrent["info_hash_sha1"], "4387cfaa6778205949bc39c91dc139e1eb7edebd")
        self.assertEqual(torrent["file_count"], 4719)
        self.assertEqual(torrent["total_bytes"], 776245980232)
        self.assertEqual(torrent["payload_files_downloaded"], 0)

        self.assertEqual(connection.execute("SELECT count(*) FROM sources").fetchone()[0], 10821)
        self.assertEqual(connection.execute("SELECT count(*) FROM recordings").fetchone()[0], 2513)
        self.assertEqual(connection.execute("SELECT count(*) FROM review_tasks").fetchone()[0], 300)
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM sources WHERE source_kind = 'torrent_file_candidate'"
            ).fetchone()[0],
            4719,
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM transcript_revisions WHERE lower(origin) LIKE 'legacy%'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            connection.execute(
                """
                SELECT count(*) FROM sources
                WHERE instr(metadata_json, '"machine_summary"') > 0
                   OR instr(metadata_json, '"speakers"') > 0
                """
            ).fetchone()[0],
            0,
        )

        publication = approve_public_source_metadata(connection)
        self.assertEqual(publication["public_recordings"], 0)
        self.assertEqual(publication["public_sources"], 0)
        self._clear_publication_gates(connection)
        publication = approve_public_source_metadata(connection)
        self.assertEqual(publication["public_recordings"], 2463)
        self.assertEqual(publication["public_sources"], 3568)
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM public_sources WHERE source_id = ?", (h3_source,)
            ).fetchone()[0],
            0,
        )
        members_only_source = source_id("youtube", "youtube_video", "rKdcv4QvGig")
        self.assertEqual(
            connection.execute(
                "SELECT access_state FROM sources WHERE source_id = ?",
                (members_only_source,),
            ).fetchone()[0],
            "members_only",
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM public_sources WHERE source_id = ?",
                (members_only_source,),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            connection.execute(
                """
                SELECT count(*) FROM public_sources
                WHERE source_kind IN ('torrent_manifest', 'torrent_file_candidate',
                                      'archive_url_discovery_hint')
                """
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            connection.execute(
                """
                SELECT count(*) FROM public_sources
                WHERE json_extract(metadata_json, '$.discovery_state') = 'search_candidate'
                """
            ).fetchone()[0],
            0,
        )
        validate_database(connection)

        first_path = temporary / "release-one.json"
        second_path = temporary / "release-two.json"
        first = export_release(connection, first_path)
        second = export_release(connection, second_path)
        self.assertEqual(first["release_id"], second["release_id"])
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertEqual(first_path.read_bytes(), second_path.read_bytes())
        release = json.loads(first_path.read_text(encoding="utf-8"))
        validate_release_shape(release)
        self.assertEqual(release["counts"], {
            "recordings": 2463,
            "sources": 3568,
            "transcript_revisions": 0,
            "segments": 0,
        })
        self.assertTrue(all(not row["transcript_revisions"] for row in release["recordings"]))
        self.assertFalse(
            any(
                source["platform"] in {"legacy_himr_archive", "bittorrent", "reddit"}
                for recording in release["recordings"]
                for source in recording["sources"]
            )
        )

    def test_synthetic_importers_idempotency_and_publication_boundaries(self):
        connection, temporary = self.new_database()
        observed = "2026-08-26T00:00:00Z"

        archive_path = temporary / "ia-69999.json"
        archive_path.write_text(
            json.dumps({
                "metadata": {"identifier": "69999", "title": "Synthetic archive"},
                "files": [
                    {
                        "name": "archive-video.mp4",
                        "format": "MPEG4",
                        "source": "original",
                        "size": "123",
                        "length": "2.5",
                        "sha1": "a" * 40,
                    },
                    {"name": "ignored.txt", "size": "3"},
                ],
            }),
            encoding="utf-8",
        )
        archive_result = import_internet_archive(
            connection, [archive_path], snapshot_date="2026-08-26", observed_at=observed
        )
        self.assertEqual(archive_result["video_files"], 1)

        legacy_path = temporary / "legacy.jsonl"
        legacy_path.write_text(
            json.dumps({
                "source_record_id": "legacy-synthetic",
                "transcript": {
                    "title": "Legacy title",
                    "machine_summary": "must not enter the catalog",
                    "speakers": ["bad guess"],
                    "word_count": 42,
                },
                "video": {
                    "internet_archive_item": "69999",
                    "internet_archive_filename": "archive-video.mp4",
                    "url": "https://archive.org/download/69999/archive-video.mp4",
                },
                "checks": {"synthetic": True},
            }) + "\n",
            encoding="utf-8",
        )
        legacy_result = import_legacy_manifest(
            connection, legacy_path, snapshot_date="2026-08-26", observed_at=observed
        )
        self.assertEqual(legacy_result["mapped_entries"], 1)
        self.assertEqual(legacy_result["old_transcript_revisions_imported"], 0)

        inventory_path = temporary / "inventory.json"
        inventory_path.write_text(
            json.dumps({
                "expected_channel": {
                    "stable_channel_id": "UC_yIF-9jOge6nNA0z-ScrBQ",
                    "display_name": "Hiding in my room",
                },
                "items": [
                    {
                        "video_id": "ddddddddddd",
                        "type": "video",
                        "observed": {
                            "title": "Public official video",
                            "publish_date_utc": "2026-08-25T00:00:00Z",
                            "duration_seconds": 10,
                            "inferred_access": "public",
                        },
                    },
                    {
                        "video_id": "eeeeeeeeeee",
                        "type": "livestream",
                        "observed": {
                            "title": "Members stream",
                            "inferred_access": "members_only",
                        },
                    },
                ],
            }),
            encoding="utf-8",
        )
        channel_result = import_current_channel(
            connection, inventory_path, snapshot_date="2026-08-26", observed_at=observed
        )
        self.assertEqual(channel_result["access_public"], 1)
        self.assertEqual(channel_result["access_members_only"], 1)

        candidates_path = temporary / "candidates.jsonl"
        candidates_path.write_text(
            json.dumps({
                "id": "bbbbbbbbbbb",
                "title": "Third-party candidate",
                "channel": "Third party",
                "url": "https://www.youtube.com/watch?v=bbbbbbbbbbb",
            }) + "\n",
            encoding="utf-8",
        )
        candidate_result = import_youtube_discovery_candidates(
            connection,
            candidates_path,
            observed_at=observed,
            query_label="synthetic search",
        )
        self.assertEqual(candidate_result["new_candidate_sources"], 1)

        info_directory = temporary / "info"
        info_directory.mkdir()
        info_path = info_directory / "bbbbbbbbbbb.info.json"
        info_path.write_text(
            json.dumps({
                "id": "bbbbbbbbbbb",
                "title": "Validated third-party candidate",
                "channel": "Third party",
                "channel_id": "UC_third_party_synthetic",
                "channel_url": "https://www.youtube.com/channel/UC_third_party_synthetic",
                "webpage_url": "https://www.youtube.com/watch?v=bbbbbbbbbbb",
                "availability": "public",
                "duration": 12.5,
                "upload_date": "20260825",
            }),
            encoding="utf-8",
        )
        ytdlp_result = import_ytdlp_infos(
            connection, [info_directory], observed_at=observed
        )
        self.assertEqual(ytdlp_result, {
            "channels": 1,
            "info_files": 1,
            "unique_channels": 1,
            "videos": 1,
        })

        hints_path = temporary / "archive-urls.txt"
        hints_path.write_text(
            "https://archive.org/download/69999/archive-video.mp4\n"
            "https://archive.org/download/unscoped/missing-video.mp4\n",
            encoding="utf-8",
        )
        hints_result = import_archive_url_hints(
            connection, hints_path, observed_at=observed, reddit_post_id="syntheticpost"
        )
        self.assertEqual(hints_result["resolved_existing_sources"], 1)
        self.assertEqual(hints_result["new_hint_sources"], 1)

        info_value = {
            b"files": [
                {b"length": 3, b"path": [b"clip.mp4"]},
                {b"length": 4, b"path": [b"notes.txt"]},
            ],
            b"name": b"synthetic",
        }
        torrent_path = temporary / "synthetic.torrent"
        torrent_path.write_bytes(self._bencode({b"info": info_value}))
        discovery_path = temporary / "torrent-discovery.json"
        discovery_path.write_text(
            json.dumps({
                "torrent": {
                    "info_hash_sha1": hashlib.sha1(self._bencode(info_value)).hexdigest(),
                    "file_count": 2,
                    "total_bytes": 7,
                    "url": "https://example.invalid/synthetic.torrent",
                }
            }),
            encoding="utf-8",
        )
        torrent_result = import_torrent_manifest(
            connection,
            torrent_path,
            observed_at=observed,
            discovery_metadata_path=discovery_path,
        )
        self.assertEqual(torrent_result["file_count"], 2)
        self.assertEqual(torrent_result["payload_files_downloaded"], 0)

        counts_before_repeat = self._core_counts(connection)
        self.assertEqual(
            import_internet_archive(
                connection, [archive_path], snapshot_date="2026-08-26", observed_at=observed
            ),
            archive_result,
        )
        self.assertEqual(
            import_legacy_manifest(
                connection, legacy_path, snapshot_date="2026-08-26", observed_at=observed
            ),
            legacy_result,
        )
        self.assertEqual(
            import_current_channel(
                connection, inventory_path, snapshot_date="2026-08-26", observed_at=observed
            ),
            channel_result,
        )
        self.assertEqual(
            import_youtube_discovery_candidates(
                connection,
                candidates_path,
                observed_at=observed,
                query_label="synthetic search",
            ),
            candidate_result,
        )
        self.assertEqual(
            import_ytdlp_infos(connection, [info_path], observed_at=observed), ytdlp_result
        )
        self.assertEqual(
            import_archive_url_hints(
                connection, hints_path, observed_at=observed, reddit_post_id="syntheticpost"
            ),
            hints_result,
        )
        self.assertEqual(
            import_torrent_manifest(
                connection,
                torrent_path,
                observed_at=observed,
                discovery_metadata_path=discovery_path,
            ),
            torrent_result,
        )
        self.assertEqual(self._core_counts(connection), counts_before_repeat)

        publication = approve_public_source_metadata(connection)
        self.assertEqual(publication["public_sources"], 0)
        self.assertEqual(publication["public_recordings"], 0)
        self._clear_publication_gates(connection)
        publication = approve_public_source_metadata(connection)
        self.assertEqual(publication["public_sources"], 2)
        self.assertEqual(publication["public_recordings"], 2)
        candidate_source = source_id("youtube", "youtube_video", "bbbbbbbbbbb")
        members_source = source_id("youtube", "youtube_video", "eeeeeeeeeee")
        for withheld_source in (candidate_source, members_source):
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM public_sources WHERE source_id = ?",
                    (withheld_source,),
                ).fetchone()[0],
                0,
            )
        self.assertEqual(approve_public_source_metadata(connection), {
            "public_recordings": 2,
            "public_sources": 2,
        })
        self.assertEqual(connection.execute("SELECT count(*) FROM transcript_revisions").fetchone()[0], 0)
        self.assertEqual(
            connection.execute(
                """
                SELECT count(*) FROM sources
                WHERE instr(metadata_json, 'machine_summary') > 0
                   OR instr(metadata_json, 'speakers') > 0
                """
            ).fetchone()[0],
            0,
        )
        release = build_release(connection)
        self.assertEqual(release["counts"], {
            "recordings": 2,
            "sources": 2,
            "transcript_revisions": 0,
            "segments": 0,
        })
        validate_release_shape(release)
        validate_database(connection)

    def test_synthetic_snapshot_bundle(self):
        connection, temporary = self.new_database()
        archive_dir = temporary / "archive"
        raw_dir = archive_dir / "raw"
        channel_dir = temporary / "channel"
        raw_dir.mkdir(parents=True)
        channel_dir.mkdir()
        (archive_dir / "provenance.json").write_text(
            json.dumps({
                "snapshot_date": "2026-08-26",
                "fetched_at": "2026-08-26T00:00:00Z",
            }),
            encoding="utf-8",
        )
        for item, files in (
            ("69999", [{"name": "one.mp4", "source": "original"}]),
            ("699992", []),
        ):
            (raw_dir / f"internet-archive-{item}.json").write_text(
                json.dumps({
                    "metadata": {"identifier": item, "title": f"Item {item}"},
                    "files": files,
                }),
                encoding="utf-8",
            )
        (archive_dir / "source-manifest.jsonl").write_text("", encoding="utf-8")
        (channel_dir / "provenance.json").write_text(
            json.dumps({
                "snapshot_date": "2026-08-26",
                "created_at": "2026-08-26T00:00:00Z",
            }),
            encoding="utf-8",
        )
        (channel_dir / "inventory.json").write_text(
            json.dumps({
                "expected_channel": {
                    "stable_channel_id": "UC_yIF-9jOge6nNA0z-ScrBQ",
                    "display_name": "Hiding in my room",
                },
                "items": [],
            }),
            encoding="utf-8",
        )
        result = import_snapshot_bundle(
            connection, archive_dir, channel_dir, approve_public_metadata=True
        )
        self.assertEqual(result["internet_archive"]["archive_items"], 2)
        self.assertEqual(result["legacy_manifest"].get("legacy_entries", 0), 0)
        self.assertEqual(result["current_channel"].get("videos", 0), 0)
        self.assertEqual(result["publication"]["public_sources"], 0)
        self.assertEqual(result["publication"]["public_recordings"], 0)
        self._clear_publication_gates(connection)
        publication = approve_public_source_metadata(connection)
        self.assertEqual(publication["public_sources"], 1)
        self.assertEqual(publication["public_recordings"], 1)

    def test_machine_transcript_is_public_after_explicit_policy_and_three_gates(self):
        connection, _ = self.new_database()
        observed = "2026-08-26T00:00:00Z"
        batch = "imp_synthetic"
        connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status, statistics_json
            ) VALUES(?, 'test', '1', ?, '2026-08-26', ?, ?, 'completed', '{}')
            """,
            (batch, "a" * 64, observed, observed),
        )
        source = source_id("youtube", "youtube_video", "abcdefghijk")
        recording = recording_id("youtube:video:abcdefghijk")
        connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url, title,
                observed_at, access_state, review_state, metadata_json,
                created_by_import_batch_id, created_at, updated_at
            ) VALUES(?, 'youtube', 'youtube_video', 'abcdefghijk',
                     'https://www.youtube.com/watch?v=abcdefghijk', 'Synthetic', ?,
                     'public', 'metadata_only', '{}', ?, ?, ?)
            """,
            (source, observed, batch, observed, observed),
        )
        connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_label, date_year,
                date_basis, duration_ms, recording_type, review_state, metadata_json,
                created_at, updated_at
            ) VALUES(?, 'youtube:video:abcdefghijk', 'synthetic-abcdefgh', 'Synthetic',
                     '2026-08-26', 2026, 'test', 10000, 'video', 'reviewed', '{}', ?, ?)
            """,
            (recording, observed, observed),
        )
        connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(?, ?, ?, 'test', 'test', 'reviewed', '{}')
            """,
            (stable_id("rso", recording, source, "test"), recording, source),
        )
        register_reviewer_fixture(
            connection, "reviewer_human", "Human reviewer", "human"
        )
        for object_type, object_id in (("source", source), ("recording", recording)):
            connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES(?, ?, ?, 'publish', 'reviewer_human', ?, 'test approval')
                """,
                (stable_id("pub", object_type, object_id), object_type, object_id, observed),
            )
        self._clear_publication_gates(
            connection, (("source", source), ("recording", recording))
        )

        machine_revision = stable_id("trv", recording, "machine")
        connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, revision_kind, origin, language,
                review_state, created_at, metadata_json
            ) VALUES(?, ?, 'raw_asr', 'faster-whisper', 'en', 'machine', ?, '{}')
            """,
            (machine_revision, recording, observed),
        )
        machine_segment = stable_id("seg", machine_revision, 0)
        connection.execute(
            """
            INSERT INTO transcript_segments(
                segment_id, revision_id, ordinal, start_ms, end_ms, text,
                speaker_label, language, confidence_band, calibrated_probability,
                metadata_json
            ) VALUES(?, ?, 0, 0, 900, 'Machine words', 'Daniel', 'en',
                     NULL, NULL, '{}')
            """,
            (machine_segment, machine_revision),
        )
        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'publish', 'reviewer_human', ?, 'mistaken test approval')
            """,
            (stable_id("pub", "transcript_revision", machine_revision), machine_revision, observed),
        )
        self.assertEqual(build_release(connection)["counts"]["transcript_revisions"], 0)
        self._clear_publication_gates(
            connection, (("transcript_revision", machine_revision),)
        )
        machine_release = build_release(connection)
        self.assertEqual(machine_release["counts"]["transcript_revisions"], 1)
        exported_machine = machine_release["recordings"][0]["transcript_revisions"][0]
        self.assertTrue(exported_machine["machine_generated"])
        self.assertTrue(exported_machine["unreviewed"])
        self.assertFalse(exported_machine["verified_quotation"])
        self.assertEqual(
            exported_machine["disclaimer_code"],
            "machine_generated_unreviewed_not_verified_quotation_v1",
        )
        self.assertEqual(exported_machine["lifecycle_state"], "active")
        self.assertEqual(exported_machine["segments"][0]["text"], "Machine words")
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "transcript removal requires a current human retraction",
        ):
            connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES(?, 'transcript_revision', ?, 'remove', 'reviewer_human',
                         '2026-08-26T00:00:30Z', 'unexplained removal attempt')
                """,
                (stable_id("pub", machine_revision, "unexplained-remove"), machine_revision),
            )

        human_revision = stable_id("trv", recording, "human")
        segment = stable_id("seg", human_revision, 0)
        connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, revision_kind, origin, language,
                review_state, created_at, metadata_json
            ) VALUES(?, ?, 'human_verbatim', 'human-review', 'en',
                     'media_checked', ?, '{}')
            """,
            (human_revision, recording, observed),
        )
        connection.execute(
            """
            INSERT INTO transcript_segments(
                segment_id, revision_id, ordinal, start_ms, end_ms, text,
                speaker_label, language, confidence_band, calibrated_probability,
                metadata_json
            ) VALUES(?, ?, 0, 1000, 2500, 'Checked words', 'Speaker 1', 'en',
                     'human', 1.0, '{}')
            """,
            (segment, human_revision),
        )
        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'publish', 'reviewer_human', ?, 'media checked')
            """,
            (stable_id("pub", "transcript_revision", human_revision), human_revision, observed),
        )
        self._clear_publication_gates(
            connection, (("transcript_revision", human_revision),)
        )
        release = build_release(connection)
        self.assertEqual(release["counts"], {
            "recordings": 1,
            "sources": 1,
            "transcript_revisions": 2,
            "segments": 2,
        })
        exported_revision = next(
            revision
            for revision in release["recordings"][0]["transcript_revisions"]
            if revision["revision_id"] == human_revision
        )
        self.assertEqual(exported_revision["revision_id"], human_revision)
        self.assertEqual(exported_revision["segments"][0]["text"], "Checked words")
        validate_release_shape(release)

        register_reviewer_fixture(
            connection, "reviewer_policy", "Policy bot", "automated_policy"
        )
        automated_review = stable_id("rvd", machine_revision, "automated-retract")
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_policy', 'reject',
                     '2026-08-26T01:00:00Z', 'automated retraction attempt')
            """,
            (automated_review, machine_revision),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "requires matching active human review",
        ):
            connection.execute(
                """
                INSERT INTO transcript_lifecycle_decisions(
                    transcript_lifecycle_decision_id, revision_id,
                    lifecycle_state, reason_code, reviewer_id,
                    review_decision_id, decided_at, basis, public_explanation
                ) VALUES(?, ?, 'retracted', 'transcription_error',
                         'reviewer_policy', ?, '2026-08-26T01:00:01Z',
                         'automated attempt', 'The machine transcript was withdrawn.')
                """,
                (
                    stable_id("tld", machine_revision, "automated-retract"),
                    machine_revision,
                    automated_review,
                ),
            )

        initial_reinstatement_review = stable_id(
            "rvd", machine_revision, "initial-reinstatement"
        )
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_human', 'accept',
                     '2026-08-26T01:10:00Z', 'invalid initial reinstatement test')
            """,
            (initial_reinstatement_review, machine_revision),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "reinstatement requires a prior retraction or dispute",
        ):
            connection.execute(
                """
                INSERT INTO transcript_lifecycle_decisions(
                    transcript_lifecycle_decision_id, revision_id,
                    lifecycle_state, reason_code, reviewer_id,
                    review_decision_id, decided_at, basis, public_explanation
                ) VALUES(?, ?, 'reinstated', 'other', 'reviewer_human', ?,
                         '2026-08-26T01:10:01Z', 'invalid initial state',
                         'This initial reinstatement must not be accepted.')
                """,
                (
                    stable_id("tld", machine_revision, "initial-reinstatement"),
                    machine_revision,
                    initial_reinstatement_review,
                ),
            )

        deferred_review = stable_id("rvd", machine_revision, "deferred-retraction")
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_human', 'defer',
                     '2026-08-26T01:20:00Z', 'no retraction decision yet')
            """,
            (deferred_review, machine_revision),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "requires matching active human review",
        ):
            connection.execute(
                """
                INSERT INTO transcript_lifecycle_decisions(
                    transcript_lifecycle_decision_id, revision_id,
                    lifecycle_state, reason_code, reviewer_id,
                    review_decision_id, decided_at, basis, public_explanation
                ) VALUES(?, ?, 'retracted', 'other', 'reviewer_human', ?,
                         '2026-08-26T01:20:01Z', 'defer is not a retraction',
                         'This deferred review must not retract text.')
                """,
                (
                    stable_id("tld", machine_revision, "deferred-retraction"),
                    machine_revision,
                    deferred_review,
                ),
            )

        invalid_date_review = stable_id("rvd", machine_revision, "invalid-date")
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_human', 'reject',
                     '2026-02-28T23:59:59Z', 'calendar validation test')
            """,
            (invalid_date_review, machine_revision),
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "CHECK constraint failed"):
            connection.execute(
                """
                INSERT INTO transcript_lifecycle_decisions(
                    transcript_lifecycle_decision_id, revision_id,
                    lifecycle_state, reason_code, reviewer_id,
                    review_decision_id, decided_at, basis, public_explanation
                ) VALUES(?, ?, 'retracted', 'transcription_error',
                         'reviewer_human', ?, '2026-02-30T00:00:00Z',
                         'invalid date', 'This row must not be accepted.')
                """,
                (
                    stable_id("tld", machine_revision, "invalid-date"),
                    machine_revision,
                    invalid_date_review,
                ),
            )

        human_review = stable_id("rvd", machine_revision, "human-retract")
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, audio_directly_perceived, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_human', 'reject',
                     '2026-08-26T02:00:00Z', 1, 'human compared transcript to audio')
            """,
            (human_review, machine_revision),
        )
        connection.execute(
            """
            INSERT INTO transcript_lifecycle_decisions(
                transcript_lifecycle_decision_id, revision_id,
                lifecycle_state, reason_code, reviewer_id,
                review_decision_id, decided_at, basis, public_explanation
            ) VALUES(?, ?, 'retracted', 'transcription_error',
                     'reviewer_human', ?, '2026-08-26T02:00:01Z',
                     'human retraction',
                     'A human review found that this machine transcript did not match the audio.')
            """,
            (
                stable_id("tld", machine_revision, "human-retract"),
                machine_revision,
                human_review,
            ),
        )
        intermediate_release = build_release(connection)
        intermediate_tombstone = next(
            revision
            for revision in intermediate_release["recordings"][0]["transcript_revisions"]
            if revision["revision_id"] == machine_revision
        )
        self.assertEqual(intermediate_tombstone["lifecycle_state"], "retracted")
        self.assertEqual(intermediate_tombstone["segments"], [])
        self.assertEqual(
            intermediate_tombstone["lifecycle_history"][-1]["explanation"],
            "A human review found that this machine transcript did not match the audio.",
        )
        self.assertEqual(intermediate_release["generated_at"], "2026-08-26T02:00:01Z")

        dispute_after_retraction = stable_id(
            "rvd", machine_revision, "dispute-after-retraction"
        )
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_human', 'dispute',
                     '2026-08-26T02:00:02Z', 'attempted dispute after retraction')
            """,
            (dispute_after_retraction, machine_revision),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "a retracted transcript may only be reinstated",
        ):
            connection.execute(
                """
                INSERT INTO transcript_lifecycle_decisions(
                    transcript_lifecycle_decision_id, revision_id,
                    lifecycle_state, reason_code, reviewer_id,
                    review_decision_id, decided_at, basis, public_explanation
                ) VALUES(?, ?, 'disputed', 'transcription_error',
                         'reviewer_human', ?, '2026-08-26T02:00:03Z',
                         'illegal transition', 'This must not republish transcript text.')
                """,
                (
                    stable_id("tld", machine_revision, "dispute-after-retraction"),
                    machine_revision,
                    dispute_after_retraction,
                ),
            )
        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, review_decision_id, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'remove', 'reviewer_human', ?,
                     '2026-08-26T02:00:04Z', 'publish human retraction tombstone')
            """,
            (
                stable_id("pub", machine_revision, "human-remove"),
                machine_revision,
                human_review,
            ),
        )
        retracted_release = build_release(connection)
        tombstone = next(
            revision
            for revision in retracted_release["recordings"][0]["transcript_revisions"]
            if revision["revision_id"] == machine_revision
        )
        self.assertEqual(tombstone["lifecycle_state"], "retracted")
        self.assertEqual(tombstone["segments"], [])
        self.assertEqual(
            tombstone["lifecycle_history"][-1]["explanation"],
            "A human review found that this machine transcript did not match the audio.",
        )
        self.assertEqual(retracted_release["counts"]["segments"], 1)
        self.assertEqual(retracted_release["generated_at"], "2026-08-26T02:00:04Z")
        validate_release_shape(retracted_release)

        connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id, gate_kind,
                decision, reviewer_id, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'privacy', 'withhold',
                     'reviewer_human', '2026-08-26T02:00:05Z',
                     'temporarily suppress public explanation')
            """,
            (stable_id("pgt", machine_revision, "privacy-withhold"), machine_revision),
        )
        suppressed_release = build_release(connection)
        self.assertNotIn(
            machine_revision,
            {
                revision["revision_id"]
                for revision in suppressed_release["recordings"][0]["transcript_revisions"]
            },
        )
        self.assertNotEqual(suppressed_release["generated_at"], "2026-08-26T02:00:05Z")

        connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id, gate_kind,
                decision, reviewer_id, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'privacy', 'clear',
                     'reviewer_human', '2026-08-26T02:00:06Z',
                     'public explanation is safe')
            """,
            (stable_id("pgt", machine_revision, "privacy-reclear"), machine_revision),
        )
        restored_tombstone_release = build_release(connection)
        self.assertEqual(
            restored_tombstone_release["generated_at"], "2026-08-26T02:00:06Z"
        )

        reinstatement_review = stable_id("rvd", machine_revision, "human-reinstate")
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_human', 'accept',
                     '2026-08-26T02:00:07Z', 'human approved reinstatement')
            """,
            (reinstatement_review, machine_revision),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "replace transcript removal before reinstatement",
        ):
            connection.execute(
                """
                INSERT INTO transcript_lifecycle_decisions(
                    transcript_lifecycle_decision_id, revision_id,
                    lifecycle_state, reason_code, reviewer_id,
                    review_decision_id, decided_at, basis, public_explanation
                ) VALUES(?, ?, 'reinstated', 'transcription_error',
                         'reviewer_human', ?, '2026-08-26T02:00:08Z',
                         'premature reinstatement', 'The transcript was reinstated.')
                """,
                (
                    stable_id("tld", machine_revision, "premature-reinstate"),
                    machine_revision,
                    reinstatement_review,
                ),
            )
        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, review_decision_id, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'publish', 'reviewer_human', ?,
                     '2026-08-26T02:00:08Z', 'prepare reviewed reinstatement')
            """,
            (
                stable_id("pub", machine_revision, "reinstate-publish"),
                machine_revision,
                reinstatement_review,
            ),
        )
        connection.execute(
            """
            INSERT INTO transcript_lifecycle_decisions(
                transcript_lifecycle_decision_id, revision_id,
                lifecycle_state, reason_code, reviewer_id,
                review_decision_id, decided_at, basis, public_explanation
            ) VALUES(?, ?, 'reinstated', 'transcription_error',
                     'reviewer_human', ?, '2026-08-26T02:00:09Z',
                     'human reinstatement',
                     'A human review reinstated this machine transcript revision.')
            """,
            (
                stable_id("tld", machine_revision, "human-reinstate"),
                machine_revision,
                reinstatement_review,
            ),
        )
        reinstated_release = build_release(connection)
        reinstated = next(
            revision
            for revision in reinstated_release["recordings"][0]["transcript_revisions"]
            if revision["revision_id"] == machine_revision
        )
        self.assertEqual(reinstated["lifecycle_state"], "reinstated")
        self.assertEqual(reinstated["segments"][0]["text"], "Machine words")
        self.assertEqual(
            reinstated["disclaimer_code"],
            "machine_generated_unreviewed_not_verified_quotation_v1",
        )

        private_revision = stable_id("trv", recording, "private-machine")
        connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, revision_kind, origin, language,
                review_state, created_at, metadata_json
            ) VALUES(?, ?, 'raw_asr', 'private-test', 'en', 'machine',
                     '2026-08-26T03:00:00Z', '{}')
            """,
            (private_revision, recording),
        )
        private_review = stable_id("rvd", private_revision, "private-retract")
        connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, 'reviewer_human', 'reject',
                     '2026-08-26T03:00:01Z', 'private-only review')
            """,
            (private_review, private_revision),
        )
        connection.execute(
            """
            INSERT INTO transcript_lifecycle_decisions(
                transcript_lifecycle_decision_id, revision_id,
                lifecycle_state, reason_code, reviewer_id,
                review_decision_id, decided_at, basis, public_explanation
            ) VALUES(?, ?, 'retracted', 'transcription_error',
                     'reviewer_human', ?, '2026-08-26T03:00:02Z',
                     'private-only lifecycle', 'Private-only explanation.')
            """,
            (
                stable_id("tld", private_revision, "private-retract"),
                private_revision,
                private_review,
            ),
        )
        self.assertEqual(build_release(connection)["generated_at"], "2026-08-26T02:00:09Z")

        word = stable_id("wrd", machine_segment, 0)
        connection.execute(
            """
            INSERT INTO transcript_words(
                word_id, segment_id, ordinal, start_ms, end_ms, token
            ) VALUES(?, ?, 0, 0, 900, 'Machine')
            """,
            (word, machine_segment),
        )
        connection.execute(
            """
            INSERT INTO transcript_revision_parents(
                revision_id, parent_revision_id, relation_kind
            ) VALUES(?, ?, 'derived_from')
            """,
            (private_revision, machine_revision),
        )
        correction = stable_id("cor", machine_revision, private_revision)
        connection.execute(
            """
            INSERT INTO corrections(
                correction_id, target_type, target_id, replacement_type,
                replacement_id, reviewer_id, reason, created_at
            ) VALUES(?, 'transcript_revision', ?, 'transcript_revision', ?,
                     'reviewer_human', 'append-only correction fixture',
                     '2026-08-26T03:00:03Z')
            """,
            (correction, machine_revision, private_revision),
        )
        validate_database(connection)
        protected_mutations = (
            (
                "UPDATE transcript_revisions SET origin = 'silent rewrite' WHERE revision_id = ?",
                (machine_revision,),
            ),
            ("DELETE FROM transcript_revisions WHERE revision_id = ?", (machine_revision,)),
            (
                "UPDATE transcript_segments SET text = 'silent rewrite' WHERE segment_id = ?",
                (machine_segment,),
            ),
            ("DELETE FROM transcript_segments WHERE segment_id = ?", (machine_segment,)),
            ("UPDATE transcript_words SET token = 'rewrite' WHERE word_id = ?", (word,)),
            ("DELETE FROM transcript_words WHERE word_id = ?", (word,)),
            (
                "UPDATE transcript_revision_parents SET relation_kind = 'rewrite' "
                "WHERE revision_id = ? AND parent_revision_id = ?",
                (private_revision, machine_revision),
            ),
            (
                "DELETE FROM transcript_revision_parents "
                "WHERE revision_id = ? AND parent_revision_id = ?",
                (private_revision, machine_revision),
            ),
            (
                "UPDATE review_decisions SET basis = 'rewrite' WHERE review_decision_id = ?",
                (human_review,),
            ),
            ("DELETE FROM review_decisions WHERE review_decision_id = ?", (human_review,)),
            ("UPDATE corrections SET reason = 'rewrite' WHERE correction_id = ?", (correction,)),
            ("DELETE FROM corrections WHERE correction_id = ?", (correction,)),
            (
                "UPDATE transcript_lifecycle_decisions SET basis = 'rewrite' "
                "WHERE revision_id = ?",
                (private_revision,),
            ),
            (
                "DELETE FROM transcript_lifecycle_decisions WHERE revision_id = ?",
                (private_revision,),
            ),
        )
        for statement, parameters in protected_mutations:
            with self.subTest(statement=statement):
                with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                    connection.execute(statement, parameters)

    def test_public_metadata_policy_withholds_unscoped_archive_item(self):
        connection, _ = self.new_database()
        observed = "2026-08-26T00:00:00Z"
        batch = "imp_unscoped_ia"
        connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status, statistics_json
            ) VALUES(?, 'internet_archive_metadata', '1', ?, '2026-08-26',
                     ?, ?, 'completed', '{}')
            """,
            (batch, "b" * 64, observed, observed),
        )
        item_source = source_id("internet_archive", "archive_item", "unreviewed-item")
        file_source = source_id(
            "internet_archive", "archive_media_file", "unreviewed-item/video.mp4"
        )
        source_rows = (
            (
                item_source,
                "archive_item",
                "unreviewed-item",
                None,
                "https://archive.org/details/unreviewed-item",
                "Unreviewed item",
            ),
            (
                file_source,
                "archive_media_file",
                "unreviewed-item/video.mp4",
                item_source,
                "https://archive.org/download/unreviewed-item/video.mp4",
                "Unreviewed video",
            ),
        )
        for source, kind, native_id, parent, url, title in source_rows:
            connection.execute(
                """
                INSERT INTO sources(
                    source_id, platform, source_kind, native_id, parent_source_id,
                    canonical_url, title, observed_at, access_state, review_state,
                    metadata_json, created_by_import_batch_id, created_at, updated_at
                ) VALUES(?, 'internet_archive', ?, ?, ?, ?, ?, ?, 'public',
                         'metadata_only', '{}', ?, ?, ?)
                """,
                (source, kind, native_id, parent, url, title, observed, batch, observed, observed),
            )
        recording = recording_id("internet_archive:unreviewed-item/video.mp4")
        connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis, recording_type,
                review_state, metadata_json, created_at, updated_at
            ) VALUES(?, 'internet_archive:unreviewed-item/video.mp4',
                     'unreviewed-item-video', 'Unreviewed video', 'unknown', 'video',
                     'metadata_only', '{}', ?, ?)
            """,
            (recording, observed, observed),
        )
        connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(?, ?, ?, 'archive_file', 'archive_path', 'metadata_only', '{}')
            """,
            (stable_id("rso", recording, file_source, "archive_file"), recording, file_source),
        )
        publication = approve_public_source_metadata(connection)
        self.assertEqual(publication["public_sources"], 0)
        self.assertEqual(publication["public_recordings"], 0)

    def test_public_json_schema_is_valid_json(self):
        schema = json.loads(
            (CORPUS_ROOT / "schemas/public-release.schema.json").read_text(encoding="utf-8")
        )
        self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
        self.assertEqual(schema["properties"]["schema_version"]["const"], 1)

    @staticmethod
    def _core_counts(connection: sqlite3.Connection) -> tuple[int, ...]:
        return tuple(
            connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "import_batches",
                "sources",
                "source_snapshots",
                "source_relations",
                "recordings",
                "recording_sources",
                "review_tasks",
                "transcript_revisions",
            )
        )

    @staticmethod
    def _bencode(value) -> bytes:
        if isinstance(value, int):
            return f"i{value}e".encode("ascii")
        if isinstance(value, bytes):
            return str(len(value)).encode("ascii") + b":" + value
        if isinstance(value, list):
            return b"l" + b"".join(CorpusFoundationTests._bencode(item) for item in value) + b"e"
        if isinstance(value, dict):
            return b"d" + b"".join(
                CorpusFoundationTests._bencode(key)
                + CorpusFoundationTests._bencode(value[key])
                for key in sorted(value)
            ) + b"e"
        raise TypeError(type(value))


if __name__ == "__main__":
    unittest.main()
