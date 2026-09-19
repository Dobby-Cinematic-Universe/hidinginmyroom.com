from __future__ import annotations

import copy
import hashlib
import io
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

from jsonschema.validators import Draft202012Validator

CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus import db as db_module  # noqa: E402
from himr_corpus.cli import build_parser, main as cli_main  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.exporter import build_release  # noqa: E402
from himr_corpus.graph_release import build_graph_release  # noqa: E402
from himr_corpus.solo_voice_attestation_admin import (  # noqa: E402
    DIRECT_AUDIO_ATTESTATION,
    SQLITE_INTEGER_MAX,
    SoloVoiceAttestationManifestError,
    apply_solo_voice_attestation_manifest,
    validate_solo_voice_attestation_manifest,
)
from himr_corpus.validation import (  # noqa: E402
    _validate_private_solo_voice_attestations,
    validate_database,
)
from corpus.tests.reviewer_fixtures import (  # noqa: E402
    register_reviewer_fixture,
    set_reviewer_active_fixture,
)


class SoloVoiceAttestationAdminTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="solo-voice-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "corpus.sqlite3")
        self.addCleanup(self.connection.close)
        migrate(self.connection)
        self._seed_catalog()
        register_reviewer_fixture(
            self.connection, "reviewer_audio", "Fictional Audio Reviewer"
        )
        register_reviewer_fixture(
            self.connection, "reviewer_privacy", "Fictional Privacy Reviewer"
        )
        self.manifest = self._manifest()

    def _seed_catalog(self) -> None:
        timestamp = "2026-01-01T00:00:00Z"
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, created_at, updated_at
            ) VALUES(
                'source_fixture', 'fixture', 'audio_post', 'fixture-audio',
                'https://example.invalid/fictional-audio', ?, 'public',
                'reviewed', ?, ?
            )
            """,
            (timestamp, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis, duration_ms,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(
                'recording_fixture', 'fixture:solo-voice', 'fictional-solo-voice',
                'Fictional solo voice', 'fixture', 10000, 'video', 'reviewed',
                '{}', ?, ?
            )
            """,
            (timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(
                'recording_source_fixture', 'recording_fixture', 'source_fixture',
                'canonical', 'direct_media_review', 'reviewed', '{}'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(
                'media_fixture', ?, 100, 'video', 'video/mp4', 'mp4', 10000,
                ?, 'verified'
            )
            """,
            ("a" * 64, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO media_sources(
                media_source_id, media_id, source_id, retrieved_at,
                retrieval_tool, retrieval_tool_version
            ) VALUES(
                'media_source_fixture', 'media_fixture', 'source_fixture', ?,
                'fixture', '1'
            )
            """,
            (timestamp,),
        )
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(
                'rendition_fixture', 'recording_fixture', 'media_fixture',
                'source', 'Fictional exact rendition', 'reviewed', '{}'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO entities(
                entity_id, entity_type, canonical_label, slug, visibility,
                review_state, metadata_json, created_at
            ) VALUES(
                'entity_fictional_aster', 'person', 'Fictional Aster',
                'fictional-aster-solo-voice', 'private', 'reviewed', '{}', ?
            )
            """,
            (timestamp,),
        )

    def _manifest(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "manifest_id": "fixture-solo-voice-manifest-001",
            "created_at": "2026-01-02T12:10:00Z",
            "authorized_by": "fictional-test-operator",
            "basis": "Fictional direct-audio test fixture.",
            "subjects": [
                {
                    "solo_voice_subject_id": "solo_voice_subject_fixture_001",
                    "entity_id": "entity_fictional_aster",
                    "source_id": "source_fixture",
                    "recording_id": "recording_fixture",
                    "rendition_id": "rendition_fixture",
                    "media_id": "media_fixture",
                    "media_sha256": "a" * 64,
                    "start_ms": 1000,
                    "end_ms": 9000,
                    "coordinate_system": "rendition_media_ms",
                }
            ],
            "privacy_reviews": [
                {
                    "solo_voice_privacy_review_id": "solo_voice_privacy_fixture_001",
                    "review_decision_id": "review_solo_voice_privacy_fixture_001",
                    "solo_voice_subject_id": "solo_voice_subject_fixture_001",
                    "decision": "clear_private_use",
                    "reviewer_id": "reviewer_privacy",
                    "reviewed_at": "2026-01-02T12:00:00Z",
                    "basis": "Fictional independent private-use clearance.",
                    "privacy_attestation": {
                        "named_voice_personal_data_reviewed": True,
                        "private_storage_only": True,
                        "public_export_approved": False,
                        "biometric_artifacts_used": False,
                        "machine_identity_outputs_used": False,
                    },
                }
            ],
            "speaker_decisions": [
                {
                    "solo_voice_attestation_decision_id": "solo_voice_decision_fixture_001",
                    "review_decision_id": "review_solo_voice_audio_fixture_001",
                    "solo_voice_subject_id": "solo_voice_subject_fixture_001",
                    "decision": "assert",
                    "reviewer_id": "reviewer_audio",
                    "decided_at": "2026-01-02T12:05:00Z",
                    "basis": "Fictional direct recognition after complete listening.",
                    "direct_audio_attestation": {
                        "attestation": DIRECT_AUDIO_ATTESTATION,
                        "audio_directly_perceived": True,
                        "reviewed_entire_interval": True,
                        "exactly_one_live_human_speaker": True,
                        "overlap_detected": False,
                        "playback_detected": False,
                        "tts_detected": False,
                        "synthetic_voice_detected": False,
                        "unknown_audio_origin_detected": False,
                        "source_metadata_used_as_identity_evidence": False,
                        "channel_context_used_as_identity_evidence": False,
                        "transcript_text_used_as_identity_evidence": False,
                        "machine_identity_output_used": False,
                        "machine_confidence_used": False,
                        "speaking_face_claimed": False,
                    },
                }
            ],
        }

    def _write(self, value: dict[str, object], name: str = "manifest.json") -> Path:
        path = self.root / name
        path.write_text(json.dumps(value, indent=2), encoding="utf-8")
        return path.resolve()

    def _apply(self, value: dict[str, object], name: str = "manifest.json") -> dict[str, object]:
        path = self._write(value, name)
        return apply_solo_voice_attestation_manifest(
            self.connection,
            path,
            dry_run=False,
            expected_input_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        )

    def test_validates_imports_privately_and_replays_exactly(self) -> None:
        path = self._write(self.manifest)
        validated = validate_solo_voice_attestation_manifest(self.connection, path)
        self.assertTrue(validated["validated"])
        self.assertTrue(validated["dry_run"])
        self.assertFalse(validated["publication_authority"])
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM solo_voice_subjects"
            ).fetchone()[0],
            0,
        )

        first = apply_solo_voice_attestation_manifest(
            self.connection,
            path,
            dry_run=False,
            expected_input_sha256=validated["input_sha256"],
        )
        second = apply_solo_voice_attestation_manifest(
            self.connection,
            path,
            dry_run=False,
            expected_input_sha256=validated["input_sha256"],
        )
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(first["input_sha256"], second["input_sha256"])
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            1,
        )
        row = self.connection.execute(
            "SELECT * FROM current_private_solo_voice_assignments"
        ).fetchone()
        self.assertEqual(row["entity_id"], "entity_fictional_aster")
        self.assertEqual(row["coordinate_system"], "rendition_media_ms")
        self.assertNotIn("confidence", set(row.keys()))
        self.assertNotIn("public_label", set(row.keys()))
        generic = self.connection.execute(
            "SELECT audio_directly_perceived, reviewed_complete_item, "
            "context_start_ms, context_end_ms FROM review_decisions "
            "WHERE review_decision_id = 'review_solo_voice_audio_fixture_001'"
        ).fetchone()
        self.assertEqual(tuple(generic), (1, 1, 1000, 9000))
        status = validate_database(self.connection)
        self.assertEqual(status["solo_voice_subjects"], 1)
        self.assertEqual(status["solo_voice_attestation_decisions"], 1)

    def test_current_assignment_is_not_duplicated_by_multiple_reviewed_source_mappings(
        self,
    ) -> None:
        self._apply(self.manifest)
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(
                'recording_source_fixture_alternate', 'recording_fixture',
                'source_fixture', 'alternate', 'second_direct_media_review',
                'reviewed', '{}'
            )
            """
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            1,
        )
        validate_database(self.connection)

    def test_insert_or_replace_is_blocked_with_recursive_triggers_on_or_off(self) -> None:
        self._apply(self.manifest)
        tables = (
            "solo_voice_manifest_imports",
            "solo_voice_subjects",
            "solo_voice_privacy_reviews",
            "solo_voice_attestation_decisions",
        )
        expected_counts = {
            table: self.connection.execute(
                f"SELECT count(*) FROM {table}"
            ).fetchone()[0]
            for table in (*tables, "review_decisions")
        }
        for recursive in (1, 0):
            with self.subTest(recursive_triggers=recursive):
                self.connection.execute(f"PRAGMA recursive_triggers = {recursive}")
                for table in tables:
                    with self.subTest(table=table), self.assertRaisesRegex(
                        sqlite3.IntegrityError, "append-only"
                    ):
                        self.connection.execute(
                            f"INSERT OR REPLACE INTO {table} "
                            f"SELECT * FROM {table} LIMIT 1"
                        )
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "append-only|immutable"
                ):
                    self.connection.execute(
                        "INSERT OR REPLACE INTO review_decisions "
                        "SELECT * FROM review_decisions "
                        "WHERE review_decision_id = "
                        "'review_solo_voice_audio_fixture_001'"
                    )
                for table, expected in expected_counts.items():
                    self.assertEqual(
                        self.connection.execute(
                            f"SELECT count(*) FROM {table}"
                        ).fetchone()[0],
                        expected,
                        table,
                    )

    def test_apply_requires_the_exact_previously_reviewed_input_digest(self) -> None:
        path = self._write(self.manifest)
        validated = validate_solo_voice_attestation_manifest(self.connection, path)
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "expected_input_sha256 is required"
        ):
            apply_solo_voice_attestation_manifest(
                self.connection, path, dry_run=False
            )
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "lowercase SHA-256"
        ):
            apply_solo_voice_attestation_manifest(
                self.connection,
                path,
                dry_run=False,
                expected_input_sha256="A" * 64,
            )

        changed = copy.deepcopy(self.manifest)
        changed["basis"] = "Fictional bytes changed after validation."
        self._write(changed)
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "stable-read manifest digest"
        ):
            apply_solo_voice_attestation_manifest(
                self.connection,
                path,
                dry_run=False,
                expected_input_sha256=validated["input_sha256"],
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM solo_voice_manifest_imports"
            ).fetchone()[0],
            0,
        )

    def test_assertion_constants_reject_context_models_playback_tts_and_overlap(self) -> None:
        attestation = self.manifest["speaker_decisions"][0]["direct_audio_attestation"]
        for key, bad_value in (
            ("source_metadata_used_as_identity_evidence", True),
            ("channel_context_used_as_identity_evidence", True),
            ("transcript_text_used_as_identity_evidence", True),
            ("machine_identity_output_used", True),
            ("machine_confidence_used", True),
            ("playback_detected", True),
            ("tts_detected", True),
            ("synthetic_voice_detected", True),
            ("unknown_audio_origin_detected", True),
            ("overlap_detected", True),
            ("exactly_one_live_human_speaker", False),
            ("speaking_face_claimed", True),
        ):
            with self.subTest(key=key):
                mutated = copy.deepcopy(self.manifest)
                mutated["speaker_decisions"][0]["direct_audio_attestation"][key] = bad_value
                with self.assertRaisesRegex(
                    SoloVoiceAttestationManifestError, key
                ):
                    validate_solo_voice_attestation_manifest(
                        self.connection, self._write(mutated, f"bad-{key}.json")
                    )
        self.assertEqual(attestation["attestation"], DIRECT_AUDIO_ATTESTATION)

    def test_direct_sql_null_assertions_fail_closed_at_every_layer(self) -> None:
        self._apply(self.manifest)

        def insert_generic(review_id: str, decided_at: str) -> None:
            self.connection.execute(
                """
                INSERT INTO review_decisions(
                    review_decision_id, target_type, target_id, reviewer_id,
                    decision, decided_at, audio_directly_perceived,
                    video_directly_perceived, reviewed_complete_item,
                    context_start_ms, context_end_ms, basis
                ) VALUES(
                    ?, 'solo_voice_subject', 'solo_voice_subject_fixture_001',
                    'reviewer_audio', 'accept', ?, 1, 0, 1, 1000, 9000,
                    'Fictional direct-SQL NULL adversarial review.'
                )
                """,
                (review_id, decided_at),
            )

        def insert_all_null(
            decision_id: str,
            review_id: str,
            decided_at: str,
            *,
            manifest_id: str = "fixture-solo-voice-manifest-001",
            decision_ordinal: int = 90,
        ) -> None:
            self.connection.execute(
                """
                INSERT INTO solo_voice_attestation_decisions(
                    solo_voice_attestation_decision_id, solo_voice_subject_id,
                    manifest_id, decision_ordinal, decision, reviewer_id,
                    review_decision_id, decided_at, basis
                ) VALUES(
                    ?, 'solo_voice_subject_fixture_001',
                    ?, ?, 'assert',
                    'reviewer_audio', ?, ?,
                    'Fictional direct-SQL NULL adversarial review.'
                )
                """,
                (
                    decision_id, manifest_id, decision_ordinal,
                    review_id, decided_at,
                ),
            )

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            insert_generic("review_all_null", "2026-01-02T12:06:00Z")
            with self.assertRaises(sqlite3.IntegrityError):
                insert_all_null(
                    "decision_all_null", "review_all_null",
                    "2026-01-02T12:06:00Z",
                )
        finally:
            self.connection.rollback()

        assertion_fields = (
            "direct_audio_attestation",
            "audio_directly_perceived",
            "reviewed_entire_interval",
            "exactly_one_live_human_speaker",
            "overlap_detected",
            "playback_detected",
            "tts_detected",
            "synthetic_voice_detected",
            "unknown_audio_origin_detected",
            "source_metadata_used_as_identity_evidence",
            "channel_context_used_as_identity_evidence",
            "transcript_text_used_as_identity_evidence",
            "machine_identity_output_used",
            "machine_confidence_used",
            "speaking_face_claimed",
        )
        valid_values: list[object] = [
            DIRECT_AUDIO_ATTESTATION, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        field_sql = ", ".join(assertion_fields)
        value_sql = ", ".join("?" for _ in assertion_fields)
        for index, field in enumerate(assertion_fields):
            with self.subTest(null_field=field):
                decided_at = f"2026-01-02T12:07:{index:02d}Z"
                review_id = f"review_one_null_{field}"
                decision_id = f"decision_one_null_{field}"
                values = list(valid_values)
                values[index] = None
                self.connection.execute("BEGIN IMMEDIATE")
                try:
                    insert_generic(review_id, decided_at)
                    with self.assertRaises(sqlite3.IntegrityError):
                        self.connection.execute(
                            f"""
                            INSERT INTO solo_voice_attestation_decisions(
                                solo_voice_attestation_decision_id,
                                solo_voice_subject_id, manifest_id,
                                decision_ordinal, decision, reviewer_id,
                                review_decision_id, decided_at, basis,
                                {field_sql}
                            ) VALUES(
                                ?, 'solo_voice_subject_fixture_001',
                                'fixture-solo-voice-manifest-001', 91,
                                'assert', 'reviewer_audio', ?, ?,
                                'Fictional direct-SQL NULL adversarial review.',
                                {value_sql}
                            )
                            """,
                            (decision_id, review_id, decided_at, *values),
                        )
                finally:
                    self.connection.rollback()

        # Fault-inject around SQLite's CHECK enforcement to exercise both remaining
        # defenses: the private assignment view filters the corrupt current assert,
        # and whole-database validation treats every NULL as a mismatch.
        self.connection.execute("PRAGMA ignore_check_constraints = ON")
        try:
            self.connection.execute(
                """
                INSERT INTO solo_voice_manifest_imports(
                    manifest_id, input_sha256, schema_version,
                    manifest_created_at, imported_at, authorized_by, basis,
                    subject_count, privacy_review_count, speaker_decision_count
                ) VALUES(
                    'fixture-null-bypass-manifest', ?, 1,
                    '2026-01-02T12:10:00Z', '2026-01-02T12:10:00Z',
                    'fictional-test-operator', 'Fault-injected NULL test.',
                    0, 0, 1
                )
                """,
                ("b" * 64,),
            )
            insert_generic("review_bypassed_null", "2026-01-02T12:08:00Z")
            insert_all_null(
                "decision_bypassed_null", "review_bypassed_null",
                "2026-01-02T12:08:00Z",
                manifest_id="fixture-null-bypass-manifest",
                decision_ordinal=0,
            )
        finally:
            self.connection.execute("PRAGMA ignore_check_constraints = OFF")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            0,
        )
        with self.assertRaisesRegex(
            RuntimeError, "exact direct-audio human lineage"
        ):
            _validate_private_solo_voice_attestations(self.connection)
        with self.assertRaisesRegex(RuntimeError, "integrity check failed"):
            validate_database(self.connection)

    def test_constraint_bypassed_privacy_authority_never_projects_or_authorizes(self) -> None:
        self._apply(self.manifest)
        self.connection.execute("PRAGMA ignore_check_constraints = ON")
        try:
            self.connection.execute(
                """
                INSERT INTO solo_voice_manifest_imports(
                    manifest_id, input_sha256, schema_version,
                    manifest_created_at, imported_at, authorized_by, basis,
                    subject_count, privacy_review_count, speaker_decision_count
                ) VALUES(
                    'fixture-privacy-bypass-manifest', ?, 1,
                    '2026-01-02T12:10:00Z', '2026-01-02T12:10:00Z',
                    'fictional-test-operator', 'Fault-injected privacy test.',
                    0, 1, 0
                )
                """,
                ("c" * 64,),
            )
            self.connection.execute(
                """
                INSERT INTO review_decisions(
                    review_decision_id, target_type, target_id, reviewer_id,
                    decision, decided_at, audio_directly_perceived,
                    video_directly_perceived, reviewed_complete_item,
                    context_start_ms, context_end_ms, basis
                ) VALUES(
                    'review_privacy_bypassed_authority',
                    'solo_voice_privacy_review',
                    'privacy_bypassed_authority', 'reviewer_privacy',
                    'accept', '2026-01-02T12:04:00Z', 0, 0, 1,
                    NULL, NULL, 'Fault-injected unsafe privacy clearance.'
                )
                """
            )
            self.connection.execute(
                """
                INSERT INTO solo_voice_privacy_reviews(
                    solo_voice_privacy_review_id, solo_voice_subject_id,
                    manifest_id, review_ordinal, decision, reviewer_id,
                    review_decision_id, reviewed_at, basis,
                    named_voice_personal_data_reviewed, private_storage_only,
                    public_export_approved, biometric_artifacts_used,
                    machine_identity_outputs_used
                ) VALUES(
                    'privacy_bypassed_authority',
                    'solo_voice_subject_fixture_001',
                    'fixture-privacy-bypass-manifest', 0,
                    'clear_private_use', 'reviewer_privacy',
                    'review_privacy_bypassed_authority',
                    '2026-01-02T12:04:00Z',
                    'Fault-injected unsafe privacy clearance.',
                    1, 1, 1, 0, 0
                )
                """
            )
        finally:
            self.connection.execute("PRAGMA ignore_check_constraints = OFF")

        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            0,
        )
        assertion = copy.deepcopy(self.manifest)
        assertion["manifest_id"] = "fixture-after-unsafe-privacy-manifest"
        assertion["subjects"] = []
        assertion["privacy_reviews"] = []
        decision = assertion["speaker_decisions"][0]
        decision["solo_voice_attestation_decision_id"] = (
            "decision_after_unsafe_privacy"
        )
        decision["review_decision_id"] = "review_after_unsafe_privacy"
        decision["decided_at"] = "2026-01-02T12:08:00Z"
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "violates private"
        ):
            validate_solo_voice_attestation_manifest(
                self.connection,
                self._write(assertion, "after-unsafe-privacy.json"),
            )
        with self.assertRaisesRegex(
            RuntimeError, "privacy review lacks exact governed human lineage"
        ):
            _validate_private_solo_voice_attestations(self.connection)

    def test_anchor_must_be_exact_reviewed_and_in_bounds(self) -> None:
        for key, value, message in (
            ("media_sha256", "b" * 64, "exact confirmed"),
            ("end_ms", 10001, "exact confirmed"),
            ("rendition_id", "rendition_missing", "exact confirmed"),
        ):
            with self.subTest(key=key):
                mutated = copy.deepcopy(self.manifest)
                mutated["subjects"][0][key] = value
                with self.assertRaisesRegex(SoloVoiceAttestationManifestError, message):
                    validate_solo_voice_attestation_manifest(
                        self.connection, self._write(mutated, f"bad-anchor-{key}.json")
                    )
        self.connection.execute(
            "UPDATE recording_sources SET confidence_state = 'candidate'"
        )
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "exact confirmed"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(self.manifest, "bad-mapping.json")
            )

    def test_tracked_schema_example_and_sqlite_integer_bounds(self) -> None:
        schema = json.loads(
            (CORPUS_ROOT / "schemas" /
             "private-solo-voice-attestation-manifest.schema.json").read_text(
                encoding="utf-8"
            )
        )
        example = json.loads(
            (CORPUS_ROOT / "examples" /
             "private-solo-voice-attestation-manifest.example.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(example)
        self.assertEqual(
            schema["$defs"]["subject"]["properties"]["start_ms"]["maximum"],
            SQLITE_INTEGER_MAX,
        )
        for field in ("start_ms", "end_ms"):
            with self.subTest(field=field):
                oversized = copy.deepcopy(self.manifest)
                oversized["subjects"][0][field] = SQLITE_INTEGER_MAX + 1
                with self.assertRaisesRegex(
                    SoloVoiceAttestationManifestError, "through 9223372036854775807"
                ):
                    validate_solo_voice_attestation_manifest(
                        self.connection,
                        self._write(oversized, f"oversized-{field}.json"),
                    )
        empty = copy.deepcopy(self.manifest)
        empty["subjects"][0]["end_ms"] = empty["subjects"][0]["start_ms"]
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "non-empty half-open"
        ):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(empty, "empty-interval.json")
            )

    def test_full_validator_rejects_every_post_import_anchor_downgrade(self) -> None:
        self._apply(self.manifest)
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis, duration_ms,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(
                'recording_merge_target', 'fixture:merge-target',
                'fictional-merge-target', 'Fictional merge target', 'fixture',
                10000, 'video', 'reviewed', '{}',
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
            )
            """
        )
        cases = (
            (
                "entity",
                "UPDATE entities SET review_state = 'disputed' "
                "WHERE entity_id = 'entity_fictional_aster'",
            ),
            (
                "source",
                "UPDATE sources SET review_state = 'disputed' "
                "WHERE source_id = 'source_fixture'",
            ),
            (
                "recording",
                "UPDATE recordings SET review_state = 'disputed' "
                "WHERE recording_id = 'recording_fixture'",
            ),
            (
                "merged-recording",
                "UPDATE recordings SET merged_into_recording_id = "
                "'recording_merge_target' WHERE recording_id = 'recording_fixture'",
            ),
            (
                "rendition",
                "UPDATE renditions SET review_state = 'disputed' "
                "WHERE rendition_id = 'rendition_fixture'",
            ),
            (
                "source-mapping",
                "UPDATE recording_sources SET confidence_state = 'candidate' "
                "WHERE recording_id = 'recording_fixture' "
                "AND source_id = 'source_fixture'",
            ),
        )
        for label, statement in cases:
            with self.subTest(label=label):
                self.connection.execute("SAVEPOINT solo_voice_anchor_downgrade")
                try:
                    self.connection.execute(statement)
                    self.assertEqual(
                        self.connection.execute(
                            "SELECT count(*) "
                            "FROM current_private_solo_voice_assignments"
                        ).fetchone()[0],
                        0,
                    )
                    with self.assertRaisesRegex(
                        RuntimeError, "lost its exact private media anchor"
                    ):
                        validate_database(self.connection)
                finally:
                    self.connection.execute(
                        "ROLLBACK TO SAVEPOINT solo_voice_anchor_downgrade"
                    )
                    self.connection.execute(
                        "RELEASE SAVEPOINT solo_voice_anchor_downgrade"
                    )

    def test_privacy_clearance_is_prior_independent_and_human(self) -> None:
        same_reviewer = copy.deepcopy(self.manifest)
        same_reviewer["speaker_decisions"][0]["reviewer_id"] = "reviewer_privacy"
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "independent"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(same_reviewer, "same-reviewer.json")
            )

        later_clearance = copy.deepcopy(self.manifest)
        later_clearance["privacy_reviews"][0]["reviewed_at"] = "2026-01-02T12:06:00Z"
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "earlier independent"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(later_clearance, "late-clearance.json")
            )

        simultaneous_clearance = copy.deepcopy(self.manifest)
        simultaneous_clearance["privacy_reviews"][0]["reviewed_at"] = (
            simultaneous_clearance["speaker_decisions"][0]["decided_at"]
        )
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "earlier independent"):
            validate_solo_voice_attestation_manifest(
                self.connection,
                self._write(simultaneous_clearance, "simultaneous-clearance.json"),
            )

        withheld = copy.deepcopy(self.manifest)
        withheld["privacy_reviews"][0]["decision"] = "withhold"
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "privacy/biometric"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(withheld, "withheld.json")
            )

        inactive = copy.deepcopy(self.manifest)
        set_reviewer_active_fixture(
            self.connection,
            "reviewer_audio",
            False,
            changed_at="2026-01-02T12:01:00Z",
            sequence_label="deactivate-before-audio-review",
        )
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "active then and now"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(inactive, "inactive.json")
            )

    def test_existing_privacy_clearance_reviewer_must_remain_active(self) -> None:
        privacy_only = copy.deepcopy(self.manifest)
        privacy_only["speaker_decisions"] = []
        self._apply(privacy_only, "privacy-only.json")
        set_reviewer_active_fixture(
            self.connection,
            "reviewer_privacy",
            False,
            changed_at="2026-01-03T00:00:00Z",
            sequence_label="deactivate-after-privacy-clearance",
        )
        assertion = copy.deepcopy(self.manifest)
        assertion["manifest_id"] = "fixture-solo-voice-later-assertion-001"
        assertion["created_at"] = "2026-01-04T12:10:00Z"
        assertion["subjects"] = []
        assertion["privacy_reviews"] = []
        decision = assertion["speaker_decisions"][0]
        decision["solo_voice_attestation_decision_id"] = (
            "solo_voice_decision_fixture_later_001"
        )
        decision["review_decision_id"] = "review_solo_voice_audio_later_001"
        decision["decided_at"] = "2026-01-04T12:05:00Z"
        path = self._write(assertion, "later-assertion.json")
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "active then and now"
        ):
            validate_solo_voice_attestation_manifest(self.connection, path)
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "active then and now"
        ):
            apply_solo_voice_attestation_manifest(
                self.connection,
                path,
                dry_run=False,
                expected_input_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )

    def test_idempotent_replay_requires_exact_generic_review_lineage(self) -> None:
        path = self._write(self.manifest)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        apply_solo_voice_attestation_manifest(
            self.connection,
            path,
            dry_run=False,
            expected_input_sha256=digest,
        )
        # Simulate out-of-band corruption in this disposable database. Production
        # triggers normally make both cited review rows immutable.
        self.connection.execute("DROP TRIGGER review_decisions_no_update")
        self.connection.execute(
            "DROP TRIGGER cited_solo_voice_review_decisions_no_update"
        )
        self.connection.execute(
            "UPDATE review_decisions SET notes = 'corrupt' "
            "WHERE review_decision_id = 'review_solo_voice_audio_fixture_001'"
        )
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "exact generic review"
        ):
            apply_solo_voice_attestation_manifest(
                self.connection,
                path,
                dry_run=False,
                expected_input_sha256=digest,
            )

    def test_current_state_uses_time_and_explicit_sequences_must_append(self) -> None:
        self._apply(self.manifest)

        def insert_generic(
            review_id: str,
            target_type: str,
            target_id: str,
            decision: str,
            decided_at: str,
            *,
            complete: int,
            context: tuple[int | None, int | None],
            basis: str,
        ) -> None:
            self.connection.execute(
                """
                INSERT INTO review_decisions(
                    review_decision_id, target_type, target_id, reviewer_id,
                    decision, decided_at, audio_directly_perceived,
                    video_directly_perceived, reviewed_complete_item,
                    context_start_ms, context_end_ms, basis
                ) VALUES(?, ?, ?, 'reviewer_privacy', ?, ?, 0, 0, ?, ?, ?, ?)
                """,
                (
                    review_id, target_type, target_id, decision, decided_at,
                    complete, context[0], context[1], basis,
                ),
            )

        def insert_late_privacy_with_low_sequence() -> None:
            self.connection.execute(
                """
                INSERT INTO solo_voice_privacy_reviews(
                    privacy_review_sequence, solo_voice_privacy_review_id,
                    solo_voice_subject_id, manifest_id, review_ordinal, decision,
                    reviewer_id, review_decision_id, reviewed_at, basis,
                    named_voice_personal_data_reviewed, private_storage_only,
                    public_export_approved, biometric_artifacts_used,
                    machine_identity_outputs_used
                ) VALUES(
                    0, 'privacy_low_sequence', 'solo_voice_subject_fixture_001',
                    'fixture-solo-voice-manifest-001', 99, 'withhold',
                    'reviewer_privacy', 'review_privacy_low_sequence',
                    '2026-01-02T12:06:00Z', 'Later fictional withhold.',
                    1, 1, 0, 0, 0
                )
                """
            )

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            insert_generic(
                "review_privacy_low_sequence",
                "solo_voice_privacy_review",
                "privacy_low_sequence",
                "reject",
                "2026-01-02T12:06:00Z",
                complete=1,
                context=(None, None),
                basis="Later fictional withhold.",
            )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "sequence must append"):
                insert_late_privacy_with_low_sequence()
        finally:
            self.connection.rollback()

        self.connection.execute(
            "DROP TRIGGER solo_voice_privacy_review_sequence_must_append"
        )
        insert_generic(
            "review_privacy_low_sequence",
            "solo_voice_privacy_review",
            "privacy_low_sequence",
            "reject",
            "2026-01-02T12:06:00Z",
            complete=1,
            context=(None, None),
            basis="Later fictional withhold.",
        )
        insert_late_privacy_with_low_sequence()
        privacy = self.connection.execute(
            "SELECT decision, privacy_review_sequence "
            "FROM current_solo_voice_privacy_reviews"
        ).fetchone()
        self.assertEqual(tuple(privacy), ("withhold", 0))

        def insert_late_withdrawal_with_low_sequence() -> None:
            self.connection.execute(
                """
                INSERT INTO solo_voice_attestation_decisions(
                    speaker_decision_sequence,
                    solo_voice_attestation_decision_id, solo_voice_subject_id,
                    manifest_id, decision_ordinal, decision, reviewer_id,
                    review_decision_id, decided_at, basis
                ) VALUES(
                    0, 'speaker_low_sequence', 'solo_voice_subject_fixture_001',
                    'fixture-solo-voice-manifest-001', 99, 'withdraw',
                    'reviewer_privacy', 'review_speaker_low_sequence',
                    '2026-01-02T12:07:00Z', 'Later fictional withdrawal.'
                )
                """
            )

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            insert_generic(
                "review_speaker_low_sequence",
                "solo_voice_subject",
                "solo_voice_subject_fixture_001",
                "correct",
                "2026-01-02T12:07:00Z",
                complete=0,
                context=(1000, 9000),
                basis="Later fictional withdrawal.",
            )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "sequence must append"):
                insert_late_withdrawal_with_low_sequence()
        finally:
            self.connection.rollback()

        self.connection.execute(
            "DROP TRIGGER solo_voice_attestation_sequence_must_append"
        )
        insert_generic(
            "review_speaker_low_sequence",
            "solo_voice_subject",
            "solo_voice_subject_fixture_001",
            "correct",
            "2026-01-02T12:07:00Z",
            complete=0,
            context=(1000, 9000),
            basis="Later fictional withdrawal.",
        )
        insert_late_withdrawal_with_low_sequence()
        speaker = self.connection.execute(
            "SELECT decision, speaker_decision_sequence "
            "FROM current_solo_voice_attestation_decisions"
        ).fetchone()
        self.assertEqual(tuple(speaker), ("withdraw", 0))
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            0,
        )

    def test_historical_replay_rejects_assert_after_current_withhold(self) -> None:
        self._apply(self.manifest)
        self.connection.execute(
            """
            INSERT INTO solo_voice_manifest_imports(
                manifest_id, input_sha256, schema_version,
                manifest_created_at, imported_at, authorized_by, basis,
                subject_count, privacy_review_count, speaker_decision_count
            ) VALUES(
                'fixture-withhold-before-assert-manifest', ?, 1,
                '2026-01-02T12:10:00Z', '2026-01-02T12:10:00Z',
                'fictional-test-operator', 'Historical replay fault fixture.',
                0, 1, 1
            )
            """,
            ("d" * 64,),
        )
        self.connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, audio_directly_perceived,
                video_directly_perceived, reviewed_complete_item,
                context_start_ms, context_end_ms, basis
            ) VALUES(
                'review_historical_withhold', 'solo_voice_privacy_review',
                'privacy_historical_withhold', 'reviewer_privacy', 'reject',
                '2026-01-02T12:06:00Z', 0, 0, 1, NULL, NULL,
                'Historical fictional withhold.'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO solo_voice_privacy_reviews(
                solo_voice_privacy_review_id, solo_voice_subject_id,
                manifest_id, review_ordinal, decision, reviewer_id,
                review_decision_id, reviewed_at, basis,
                named_voice_personal_data_reviewed, private_storage_only,
                public_export_approved, biometric_artifacts_used,
                machine_identity_outputs_used
            ) VALUES(
                'privacy_historical_withhold',
                'solo_voice_subject_fixture_001',
                'fixture-withhold-before-assert-manifest', 0, 'withhold',
                'reviewer_privacy', 'review_historical_withhold',
                '2026-01-02T12:06:00Z', 'Historical fictional withhold.',
                1, 1, 0, 0, 0
            )
            """
        )
        self.connection.execute(
            "DROP TRIGGER solo_voice_assert_requires_independent_privacy_clearance"
        )
        self.connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, audio_directly_perceived,
                video_directly_perceived, reviewed_complete_item,
                context_start_ms, context_end_ms, basis
            ) VALUES(
                'review_assert_after_withhold', 'solo_voice_subject',
                'solo_voice_subject_fixture_001', 'reviewer_audio', 'accept',
                '2026-01-02T12:08:00Z', 1, 0, 1, 1000, 9000,
                'Historical fictional assertion after withhold.'
            )
            """
        )
        attestation = self.manifest["speaker_decisions"][0]["direct_audio_attestation"]
        self.connection.execute(
            """
            INSERT INTO solo_voice_attestation_decisions(
                solo_voice_attestation_decision_id, solo_voice_subject_id,
                manifest_id, decision_ordinal, decision, reviewer_id,
                review_decision_id, decided_at, basis,
                direct_audio_attestation, audio_directly_perceived,
                reviewed_entire_interval, exactly_one_live_human_speaker,
                overlap_detected, playback_detected, tts_detected,
                synthetic_voice_detected, unknown_audio_origin_detected,
                source_metadata_used_as_identity_evidence,
                channel_context_used_as_identity_evidence,
                transcript_text_used_as_identity_evidence,
                machine_identity_output_used, machine_confidence_used,
                speaking_face_claimed
            ) VALUES(
                'decision_assert_after_withhold',
                'solo_voice_subject_fixture_001',
                'fixture-withhold-before-assert-manifest', 0, 'assert',
                'reviewer_audio', 'review_assert_after_withhold',
                '2026-01-02T12:08:00Z',
                'Historical fictional assertion after withhold.',
                ?, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            )
            """,
            (attestation["attestation"],),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            0,
        )
        with self.assertRaisesRegex(
            RuntimeError, "exact direct-audio human lineage"
        ):
            _validate_private_solo_voice_attestations(self.connection)

    def test_historical_replay_retains_resolved_cross_rendition_conflict(self) -> None:
        self._apply(self.manifest)
        self.connection.execute(
            """
            INSERT INTO entities(
                entity_id, entity_type, canonical_label, slug, visibility,
                review_state, metadata_json, created_at
            ) VALUES(
                'entity_fictional_birch_history', 'person',
                'Fictional Birch History', 'fictional-birch-history',
                'private', 'reviewed', '{}', '2026-01-01T00:00:00Z'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(
                'rendition_history_same_media_alias', 'recording_fixture',
                'media_fixture', 'history-alternate',
                'Fictional historical same-media alias', 'reviewed', '{}'
            )
            """
        )
        subject_manifest = copy.deepcopy(self.manifest)
        subject_manifest["manifest_id"] = "fixture-history-birch-subject-manifest"
        subject_manifest["created_at"] = "2026-01-03T12:10:00Z"
        subject = subject_manifest["subjects"][0]
        subject["solo_voice_subject_id"] = "solo_voice_subject_history_birch"
        subject["entity_id"] = "entity_fictional_birch_history"
        subject["rendition_id"] = "rendition_history_same_media_alias"
        subject["start_ms"] = 2000
        subject["end_ms"] = 8000
        privacy = subject_manifest["privacy_reviews"][0]
        privacy["solo_voice_privacy_review_id"] = "privacy_history_birch"
        privacy["review_decision_id"] = "review_privacy_history_birch"
        privacy["solo_voice_subject_id"] = subject["solo_voice_subject_id"]
        privacy["reviewed_at"] = "2026-01-03T12:00:00Z"
        subject_manifest["speaker_decisions"] = []
        self._apply(subject_manifest, "history-birch-subject.json")

        self.connection.execute(
            """
            INSERT INTO solo_voice_manifest_imports(
                manifest_id, input_sha256, schema_version,
                manifest_created_at, imported_at, authorized_by, basis,
                subject_count, privacy_review_count, speaker_decision_count
            ) VALUES(
                'fixture-history-birch-assert-manifest', ?, 1,
                '2026-01-03T12:10:00Z', '2026-01-03T12:10:00Z',
                'fictional-test-operator', 'Historical conflict fixture.',
                0, 0, 1
            )
            """,
            ("e" * 64,),
        )
        self.connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, audio_directly_perceived,
                video_directly_perceived, reviewed_complete_item,
                context_start_ms, context_end_ms, basis
            ) VALUES(
                'review_history_birch_assert', 'solo_voice_subject',
                'solo_voice_subject_history_birch', 'reviewer_audio', 'accept',
                '2026-01-03T12:05:00Z', 1, 0, 1, 2000, 8000,
                'Historical conflicting fictional assertion.'
            )
            """
        )
        self.connection.execute(
            "DROP TRIGGER solo_voice_assert_no_conflicting_current_identity"
        )
        self.connection.execute(
            """
            INSERT INTO solo_voice_attestation_decisions(
                solo_voice_attestation_decision_id, solo_voice_subject_id,
                manifest_id, decision_ordinal, decision, reviewer_id,
                review_decision_id, decided_at, basis,
                direct_audio_attestation, audio_directly_perceived,
                reviewed_entire_interval, exactly_one_live_human_speaker,
                overlap_detected, playback_detected, tts_detected,
                synthetic_voice_detected, unknown_audio_origin_detected,
                source_metadata_used_as_identity_evidence,
                channel_context_used_as_identity_evidence,
                transcript_text_used_as_identity_evidence,
                machine_identity_output_used, machine_confidence_used,
                speaking_face_claimed
            ) VALUES(
                'decision_history_birch_assert',
                'solo_voice_subject_history_birch',
                'fixture-history-birch-assert-manifest', 0, 'assert',
                'reviewer_audio', 'review_history_birch_assert',
                '2026-01-03T12:05:00Z',
                'Historical conflicting fictional assertion.',
                ?, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            )
            """,
            (DIRECT_AUDIO_ATTESTATION,),
        )
        withdrawal = {
            "schema_version": 1,
            "manifest_id": "fixture-history-aster-withdrawal",
            "created_at": "2026-01-04T12:10:00Z",
            "authorized_by": "fictional-test-operator",
            "basis": "Resolve current state without erasing historical conflict.",
            "subjects": [],
            "privacy_reviews": [],
            "speaker_decisions": [
                {
                    "solo_voice_attestation_decision_id":
                        "decision_history_aster_withdrawal",
                    "review_decision_id": "review_history_aster_withdrawal",
                    "solo_voice_subject_id": "solo_voice_subject_fixture_001",
                    "decision": "withdraw",
                    "reviewer_id": "reviewer_audio",
                    "decided_at": "2026-01-04T12:05:00Z",
                    "basis": "Resolve current fictional conflict.",
                    "direct_audio_attestation": None,
                }
            ],
        }
        self._apply(withdrawal, "history-aster-withdrawal.json")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            1,
        )
        with self.assertRaisesRegex(
            RuntimeError, "history admitted an overlapping conflicting identity"
        ):
            _validate_private_solo_voice_attestations(self.connection)

    def test_withdrawal_is_append_only_and_removes_current_assignment(self) -> None:
        self._apply(self.manifest)
        withdrawal = {
            "schema_version": 1,
            "manifest_id": "fixture-solo-voice-withdrawal-001",
            "created_at": "2026-01-03T12:10:00Z",
            "authorized_by": "fictional-test-operator",
            "basis": "Fictional correction stream.",
            "subjects": [],
            "privacy_reviews": [],
            "speaker_decisions": [
                {
                    "solo_voice_attestation_decision_id": "solo_voice_withdraw_fixture_001",
                    "review_decision_id": "review_solo_voice_withdraw_fixture_001",
                    "solo_voice_subject_id": "solo_voice_subject_fixture_001",
                    "decision": "withdraw",
                    "reviewer_id": "reviewer_audio",
                    "decided_at": "2026-01-03T12:05:00Z",
                    "basis": "Fictional human withdrawal; historical rows remain.",
                    "direct_audio_attestation": None,
                }
            ],
        }
        self._apply(withdrawal, "withdrawal.json")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM current_private_solo_voice_assignments"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM solo_voice_attestation_decisions"
            ).fetchone()[0],
            2,
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE solo_voice_attestation_decisions SET basis = 'rewritten'"
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            self.connection.execute(
                "DELETE FROM review_decisions "
                "WHERE review_decision_id = 'review_solo_voice_audio_fixture_001'"
            )

    def test_conflicting_overlapping_identity_requires_prior_withdrawal(self) -> None:
        self._apply(self.manifest)
        self.connection.execute(
            """
            INSERT INTO entities(
                entity_id, entity_type, canonical_label, slug, visibility,
                review_state, metadata_json, created_at
            ) VALUES(
                'entity_fictional_birch', 'person', 'Fictional Birch',
                'fictional-birch-solo-voice', 'private', 'reviewed', '{}',
                '2026-01-01T00:00:00Z'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(
                'rendition_fixture_same_media_alias', 'recording_fixture',
                'media_fixture', 'alternate', 'Fictional same-media alias',
                'reviewed', '{}'
            )
            """
        )
        conflict = copy.deepcopy(self.manifest)
        conflict["manifest_id"] = "fixture-solo-voice-conflict-001"
        conflict["created_at"] = "2026-01-03T12:10:00Z"
        subject = conflict["subjects"][0]
        subject["solo_voice_subject_id"] = "solo_voice_subject_fixture_birch_001"
        subject["entity_id"] = "entity_fictional_birch"
        subject["rendition_id"] = "rendition_fixture_same_media_alias"
        subject["start_ms"] = 2000
        subject["end_ms"] = 8000
        privacy = conflict["privacy_reviews"][0]
        privacy["solo_voice_privacy_review_id"] = "solo_voice_privacy_fixture_birch_001"
        privacy["review_decision_id"] = "review_solo_voice_privacy_birch_001"
        privacy["solo_voice_subject_id"] = subject["solo_voice_subject_id"]
        privacy["reviewed_at"] = "2026-01-03T12:00:00Z"
        decision = conflict["speaker_decisions"][0]
        decision["solo_voice_attestation_decision_id"] = "solo_voice_decision_fixture_birch_001"
        decision["review_decision_id"] = "review_solo_voice_audio_birch_001"
        decision["solo_voice_subject_id"] = subject["solo_voice_subject_id"]
        decision["decided_at"] = "2026-01-03T12:05:00Z"
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "conflicts"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(conflict, "conflict.json")
            )

    def test_publication_and_gate_clearance_are_database_blocked(self) -> None:
        self._apply(self.manifest)
        serialized_releases = json.dumps(
            {
                "recordings": build_release(self.connection),
                "graph": build_graph_release(self.connection),
            }
        )
        self.assertNotIn("solo_voice_subject_fixture_001", serialized_releases)
        self.assertNotIn("Fictional direct recognition", serialized_releases)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "publication state"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES(
                    'publish_private_solo_voice', 'solo_voice_subject',
                    'solo_voice_subject_fixture_001', 'publish', 'reviewer_audio',
                    '2026-01-03T00:00:00Z', 'must fail'
                )
                """
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "gate state"):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id,
                    gate_kind, decision, reviewer_id, decided_at, basis
                ) VALUES(
                    'clear_private_solo_voice', 'solo_voice_subject',
                    'solo_voice_subject_fixture_001', 'privacy', 'clear',
                    'reviewer_privacy', '2026-01-03T00:00:00Z', 'must fail'
                )
                """
            )

        # Simulate out-of-band removal of the insertion guard. Both the admin path
        # and whole-database validator must still refuse stale reserved state.
        self.connection.execute(
            "DROP TRIGGER publication_decisions_block_solo_voice_private_objects"
        )
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES(
                'stale_private_solo_voice_state', 'solo_voice_subject',
                'future_solo_voice_subject', 'withhold', 'reviewer_audio',
                '2026-01-03T00:00:00Z', 'Injected stale reserved state.'
            )
            """
        )
        path = self._write(self.manifest, "reserved-state.json")
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "reserved solo voice publication state"
        ):
            validate_solo_voice_attestation_manifest(self.connection, path)
        with self.assertRaisesRegex(RuntimeError, "reserved publication state"):
            validate_database(self.connection)

    def test_atomic_failure_does_not_leave_generic_reviews_or_subjects(self) -> None:
        self.connection.execute(
            """
            CREATE TRIGGER reject_solo_voice_test
            BEFORE INSERT ON solo_voice_attestation_decisions
            BEGIN SELECT RAISE(ABORT, 'injected solo voice failure'); END
            """
        )
        with self.assertRaisesRegex(
            SoloVoiceAttestationManifestError, "injected solo voice failure"
        ):
            self._apply(self.manifest)
        for table in (
            "solo_voice_manifest_imports",
            "solo_voice_subjects",
            "solo_voice_privacy_reviews",
            "solo_voice_attestation_decisions",
            "review_decisions",
        ):
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                0,
                table,
            )

    def test_manifest_collision_and_unknown_fields_fail_closed(self) -> None:
        self._apply(self.manifest)
        changed = copy.deepcopy(self.manifest)
        changed["basis"] = "Different bytes under an existing manifest ID."
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "conflicts"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(changed, "changed.json")
            )
        unknown = copy.deepcopy(self.manifest)
        unknown["speaker_decisions"][0]["calibrated_probability"] = 0.99
        with self.assertRaisesRegex(SoloVoiceAttestationManifestError, "unknown"):
            validate_solo_voice_attestation_manifest(
                self.connection, self._write(unknown, "unknown-score.json")
            )

    def test_upgrade_refuses_preexisting_reserved_publication_state(self) -> None:
        migration_root = self.root / "preflight-migrations"
        migration_root.mkdir()
        source_root = CORPUS_ROOT / "migrations"
        for source in sorted(source_root.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            if int(source.name[:4]) <= 31:
                shutil.copy2(source, migration_root / source.name)
        database = self.root / "preflight-v31.sqlite3"
        connection = connect(database)
        self.addCleanup(connection.close)
        with mock.patch.object(db_module, "MIGRATIONS_DIR", migration_root):
            migrate(connection)
        register_reviewer_fixture(
            connection,
            "reviewer_preflight",
            "Fictional Preflight Reviewer",
        )
        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES(
                'preexisting_solo_voice_publication', 'solo_voice_subject',
                'future_subject', 'withhold', 'reviewer_preflight',
                '2026-01-01T00:00:00Z', 'Preexisting restrictive state.'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id,
                gate_kind, decision, reviewer_id, decided_at, basis
            ) VALUES(
                'preexisting_solo_voice_gate',
                'solo_voice_attestation_decision', 'future_decision',
                'privacy', 'withhold', 'reviewer_preflight',
                '2026-01-01T00:00:01Z', 'Preexisting restrictive gate state.'
            )
            """
        )
        shutil.copy2(
            source_root / "0032_private_solo_voice_attestations.sql",
            migration_root / "0032_private_solo_voice_attestations.sql",
        )
        with mock.patch.object(
            db_module, "MIGRATIONS_DIR", migration_root
        ), self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "reserved solo voice publication state already exists",
        ):
            migrate(connection)
        self.assertEqual(
            connection.execute(
                "SELECT max(version) FROM schema_migrations"
            ).fetchone()[0],
            31,
        )
        self.assertIsNone(
            connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'solo_voice_subjects'"
            ).fetchone()
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM publication_decisions "
                "WHERE object_type = 'solo_voice_subject'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM publication_gate_decisions "
                "WHERE object_type = 'solo_voice_attestation_decision'"
            ).fetchone()[0],
            1,
        )

    def test_cli_requires_explicit_apply_flag(self) -> None:
        parser = build_parser()
        dry = parser.parse_args(
            [
                "import-solo-voice-attestation-manifest",
                "--db",
                "fixture.sqlite3",
                "--manifest",
                "fixture.json",
            ]
        )
        applied = parser.parse_args(
            [
                "import-solo-voice-attestation-manifest",
                "--db",
                "fixture.sqlite3",
                "--manifest",
                "fixture.json",
                "--apply",
                "--expected-input-sha256",
                "a" * 64,
            ]
        )
        self.assertFalse(dry.apply)
        self.assertIsNone(dry.expected_input_sha256)
        self.assertTrue(applied.apply)
        self.assertEqual(applied.expected_input_sha256, "a" * 64)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            cli_main(
                [
                    "import-solo-voice-attestation-manifest",
                    "--db",
                    "must-not-be-opened.sqlite3",
                    "--manifest",
                    "must-not-be-read.json",
                    "--apply",
                ]
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
