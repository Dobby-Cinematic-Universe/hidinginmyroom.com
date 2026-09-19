"""Database and release integrity checks."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from .asr_result_importer import (
    _absolute_observed_path,
    _local_file_uri,
    _verify_hash,
)
from .db import verify_migrations
from .exporter import validate_release_shape
from .importers import canonical_json
from .machine_transcript_publication import (
    validate_machine_transcript_publication_policy,
)
from .media_local_transcript_projection import (
    MediaLocalTranscriptProjectionError,
    require_exact_projection_batch,
)
from .private_acquisition import (
    PrivateAcquisitionError,
    assert_no_restricted_publication_state,
    validate_handling_policy,
)
from .ocr_tesseract_result_importer import (
    _assert_exact_replay as _assert_exact_ocr_replay,
    _assert_private_ocr_fts_exact,
    _exact_source_anchors as _exact_ocr_source_anchors,
    _frame_rows as _ocr_frame_rows,
    _read_ocr_result,
)
from .result_importers import ResultImportError


def _validate_audio_fingerprint_compare_v2_files(
    connection: sqlite3.Connection,
) -> None:
    verified: set[tuple[str, str, int]] = set()

    def verify_path(
        path_text: str,
        digest: str,
        byte_count: int,
        label: str,
        *, sealed: bool,
    ) -> Path:
        key = (path_text, digest, byte_count)
        try:
            path = _absolute_observed_path(path_text, label)
            if sealed and path.lstat().st_mode & 0o222:
                raise ResultImportError(f"{label} is not sealed read-only")
            if key not in verified:
                _verify_hash(path, digest, byte_count, label)
                verified.add(key)
            return path
        except ResultImportError as error:
            raise RuntimeError(
                f"Audio fingerprint v2 current-file validation failed: {error}"
            ) from error

    for receipt in connection.execute(
        """
        SELECT comparison_result_sha256, result_path, result_byte_count
        FROM audio_fingerprint_compare_v2_receipts
        ORDER BY comparison_result_sha256
        """
    ):
        verify_path(
            receipt["result_path"],
            receipt["comparison_result_sha256"],
            receipt["result_byte_count"],
            "audio fingerprint v2 comparison result",
            sealed=True,
        )

    for side in connection.execute(
        """
        SELECT match_candidate_id, role, extraction_result_path,
               extraction_result_sha256, extraction_result_byte_count,
               input_path, input_artifact_uri, input_sha256, input_byte_count,
               artifact_uri, artifact_sha256, artifact_byte_count,
               engine_path, engine_sha256, engine_byte_count
        FROM audio_fingerprint_compare_v2_sides
        ORDER BY match_candidate_id, role
        """
    ):
        prefix = f"audio fingerprint v2 {side['role']}"
        verify_path(
            side["extraction_result_path"],
            side["extraction_result_sha256"],
            side["extraction_result_byte_count"],
            f"{prefix} extraction result",
            sealed=True,
        )
        input_path = verify_path(
            side["input_path"],
            side["input_sha256"],
            side["input_byte_count"],
            f"{prefix} normalized input",
            sealed=True,
        )
        try:
            input_uri_path = _local_file_uri(
                side["input_artifact_uri"], f"{prefix} normalized input URI"
            )
            artifact_path = _local_file_uri(
                side["artifact_uri"], f"{prefix} raw artifact URI"
            )
        except ResultImportError as error:
            raise RuntimeError(
                f"Audio fingerprint v2 current-file validation failed: {error}"
            ) from error
        if input_uri_path != input_path:
            raise RuntimeError(
                f"Audio fingerprint v2 {side['role']} input path and URI differ"
            )
        verify_path(
            str(artifact_path),
            side["artifact_sha256"],
            side["artifact_byte_count"],
            f"{prefix} raw artifact",
            sealed=True,
        )
        verify_path(
            side["engine_path"],
            side["engine_sha256"],
            side["engine_byte_count"],
            f"{prefix} FFmpeg executable",
            sealed=False,
        )


def _validate_visual_fingerprint_compare_files(
    connection: sqlite3.Connection,
) -> None:
    # Import locally so the catalog validator and importer do not form a module cycle.
    from .visual_fingerprint_result_importer import (
        validate_visual_fingerprint_compare_result_file,
    )

    for receipt in connection.execute(
        """
        SELECT compare_import.result_path, compare_import.result_sha256,
               compare_import.result_byte_count, compare_import.processing_run_id,
               compare_import.comparison_id
        FROM visual_fingerprint_compare_imports AS compare_import
        JOIN visual_fingerprint_compare_completion_receipts AS completion
          ON completion.import_batch_id = compare_import.import_batch_id
         AND completion.comparison_id = compare_import.comparison_id
        ORDER BY compare_import.result_sha256
        """
    ):
        try:
            result = validate_visual_fingerprint_compare_result_file(
                Path(receipt["result_path"])
            )
        except ResultImportError as error:
            raise RuntimeError(
                f"Visual comparison current-file validation failed: {error}"
            ) from error
        if (
            result["_result_sha256"] != receipt["result_sha256"]
            or result["_result_byte_count"] != receipt["result_byte_count"]
            or result["processing_run"]["processing_run_id"]
            != receipt["processing_run_id"]
            or result["comparison"]["match_candidate_id"]
            != receipt["comparison_id"]
        ):
            raise RuntimeError(
                "Visual comparison current file differs from its catalog receipt"
            )


def _validate_contextual_media_local_files(connection: sqlite3.Connection) -> None:
    """Replay every opaque private receipt and compare its catalog projection."""

    from .contextual_media_local_asr import (
        _build_admission,
        _build_glossary_registration,
        _read_private_glossary,
        _read_validated_batch,
        require_exact_contextual_diff_projection,
        resolve_closed_pilot_private_reference,
    )

    def exact_row(table: str, key_column: str, expected: dict) -> None:
        row = connection.execute(
            f"SELECT * FROM {table} WHERE {key_column} = ?",
            (expected[key_column],),
        ).fetchone()
        if row is None or dict(row) != expected:
            raise RuntimeError(
                f"Contextual media-local {table} differs from deterministic replay"
            )

    try:
        for registration in connection.execute(
            """
            SELECT * FROM private_glossary_registrations
            ORDER BY glossary_revision_id
            """
        ):
            glossary_path = resolve_closed_pilot_private_reference(
                registration["artifact_uri"],
                digest=registration["raw_sha256"],
                byte_count=registration["byte_count"],
                kind="glossary",
            )
            glossary = _read_private_glossary(glossary_path)
            if (
                registration["artifact_uri"] != glossary["artifact_ref"]
                or registration["raw_sha256"] != glossary["raw_sha256"]
                or registration["canonical_sha256"] != glossary["canonical_sha256"]
                or registration["byte_count"] != glossary["byte_count"]
                or registration["prompt_sha256"] != glossary["prompt_sha256"]
                or registration["term_count"] != glossary["term_count"]
            ):
                raise ResultImportError(
                    "private glossary registration differs from current replay"
                )
            glossary_replay = _build_glossary_registration(
                glossary_path, observed_at=registration["registered_at"]
            )
            exact_row(
                "private_glossary_registrations",
                "private_glossary_registration_id",
                glossary_replay["row"],
            )
            exact_row(
                "glossary_revisions",
                "glossary_revision_id",
                {
                    "glossary_revision_id": glossary["glossary_revision_id"],
                    "parent_glossary_revision_id": None,
                    "sha256": glossary["raw_sha256"],
                    "created_at": registration["registered_at"],
                    "description": "Private neutral spelling-only machine-candidate glossary.",
                    "artifact_uri": glossary["artifact_ref"],
                },
            )

        for batch_row in connection.execute(
            """
            SELECT * FROM contextual_asr_batch_registrations
            ORDER BY contextual_batch_id
            """
        ):
            manifest_path = resolve_closed_pilot_private_reference(
                batch_row["manifest_uri"],
                digest=batch_row["manifest_raw_sha256"],
                byte_count=batch_row["manifest_byte_count"],
                kind="manifest",
            )
            batch = _read_validated_batch(manifest_path)
            if (
                batch_row["manifest_uri"] != batch["reference"]
                or batch_row["identity_sha256"] != batch["identity_sha256"]
                or batch_row["manifest_raw_sha256"] != batch["raw_sha256"]
                or batch_row["manifest_canonical_sha256"]
                != batch["canonical_sha256"]
                or batch_row["manifest_byte_count"] != batch["byte_count"]
            ):
                raise ResultImportError(
                    "contextual batch registration differs from current replay"
                )

        receipts = connection.execute(
            """
            SELECT receipt.*, diff.diff_uri, diff.diff_raw_sha256,
                   diff.diff_byte_count
            FROM contextual_media_local_asr_imports AS receipt
            JOIN contextual_media_local_asr_pairs AS pair
              ON pair.contextual_asr_import_id = receipt.contextual_asr_import_id
            JOIN contextual_asr_text_private_diffs AS diff
              ON diff.contextual_pair_id = pair.contextual_pair_id
            ORDER BY receipt.contextual_asr_import_id
            """
        ).fetchall()
        for receipt in receipts:
            result_path = resolve_closed_pilot_private_reference(
                receipt["result_uri"],
                digest=receipt["result_raw_sha256"],
                byte_count=receipt["result_byte_count"],
                kind="result",
            )
            diff_path = resolve_closed_pilot_private_reference(
                receipt["diff_uri"],
                digest=receipt["diff_raw_sha256"],
                byte_count=receipt["diff_byte_count"],
                kind="diff",
            )
            batch_receipt = connection.execute(
                """
                SELECT * FROM contextual_asr_batch_registrations
                WHERE contextual_batch_id = ?
                """,
                (receipt["contextual_batch_id"],),
            ).fetchone()
            if batch_receipt is None:
                raise ResultImportError("contextual receipt lacks its batch")
            manifest_path = resolve_closed_pilot_private_reference(
                batch_receipt["manifest_uri"],
                digest=batch_receipt["manifest_raw_sha256"],
                byte_count=batch_receipt["manifest_byte_count"],
                kind="manifest",
            )
            replay = _build_admission(
                connection, result_path, manifest_path, diff_path
            )
            exact_row(
                "contextual_asr_batch_registrations",
                "contextual_batch_id",
                replay["batch_row"],
            )
            exact_row(
                "contextual_media_local_asr_imports",
                "contextual_asr_import_id",
                replay["import_receipt"],
            )
            exact_row(
                "contextual_media_local_asr_pairs",
                "contextual_pair_id",
                replay["pair_row"],
            )
            # This exact comparison covers identity, block alignment, fail-closed
            # policy flags, and every retained numeric diff projection after the
            # private diff itself has been deterministically rebuilt.
            stored_diff = connection.execute(
                """
                SELECT * FROM contextual_asr_text_private_diffs
                WHERE contextual_diff_id = ?
                """,
                (replay["diff_row"]["contextual_diff_id"],),
            ).fetchone()
            if stored_diff is None:
                raise ResultImportError("contextual diff replay lacks its catalog row")
            require_exact_contextual_diff_projection(
                dict(stored_diff), replay["diff_row"]
            )
            exact_row(
                "contextual_asr_text_private_diffs",
                "contextual_diff_id",
                replay["diff_row"],
            )
            exact_row(
                "processing_runs",
                "processing_run_id",
                replay["catalog_run"],
            )
            run_id = replay["catalog_run"]["processing_run_id"]
            if connection.execute(
                "SELECT count(*) FROM run_inputs WHERE processing_run_id = ?",
                (run_id,),
            ).fetchone()[0] != 1:
                raise ResultImportError("contextual run does not have one exact input")
            exact_row(
                "run_inputs",
                "run_input_id",
                replay["result"]["run_input"],
            )
            if connection.execute(
                "SELECT count(*) FROM artifacts WHERE processing_run_id = ?",
                (run_id,),
            ).fetchone()[0] != 2:
                raise ResultImportError("contextual run does not have two exact artifacts")
            for artifact in replay["catalog_artifacts"]:
                exact_row("artifacts", "artifact_id", artifact)
    except ResultImportError as error:
        raise RuntimeError(
            f"Contextual media-local current-file validation failed: {error}"
        ) from error


def _validate_reviewer_administration(connection: sqlite3.Connection) -> None:
    invalid_enrollment = connection.execute(
        """
        SELECT count(*)
        FROM (
            SELECT reviewer.reviewer_id
            FROM reviewers AS reviewer
            LEFT JOIN reviewer_admin_events AS event
              ON event.reviewer_id = reviewer.reviewer_id
             AND event.event_kind IN ('register', 'legacy_adopt')
            GROUP BY reviewer.reviewer_id
            HAVING count(event.reviewer_admin_event_id) <> 1
        )
        """
    ).fetchone()[0]
    if invalid_enrollment:
        raise RuntimeError("Reviewer administration has missing or duplicate enrollment")

    invalid_current = connection.execute(
        """
        SELECT count(*)
        FROM reviewers AS reviewer
        LEFT JOIN current_reviewer_admin_events AS current
          ON current.reviewer_id = reviewer.reviewer_id
        WHERE current.reviewer_admin_event_id IS NULL
           OR current.display_label IS NOT reviewer.display_label
           OR current.reviewer_kind IS NOT reviewer.reviewer_kind
           OR current.new_active IS NOT reviewer.active
        """
    ).fetchone()[0]
    if invalid_current:
        raise RuntimeError("Reviewer current rows disagree with their audit streams")

    invalid_identity_snapshots = connection.execute(
        """
        SELECT count(*)
        FROM reviewer_admin_events AS event
        JOIN reviewers AS reviewer ON reviewer.reviewer_id = event.reviewer_id
        WHERE event.display_label IS NOT reviewer.display_label
           OR event.reviewer_kind IS NOT reviewer.reviewer_kind
        """
    ).fetchone()[0]
    if invalid_identity_snapshots:
        raise RuntimeError("Reviewer audit identity snapshots disagree with current rows")

    invalid_first_or_order = connection.execute(
        """
        SELECT count(*)
        FROM (
            SELECT event.reviewer_id, event.event_kind,
                   event.previous_active, event.new_active,
                   julianday(event.effective_at) AS effective_time,
                   row_number() OVER (
                       PARTITION BY event.reviewer_id ORDER BY event.event_sequence
                   ) AS stream_ordinal,
                   lag(julianday(event.effective_at)) OVER (
                       PARTITION BY event.reviewer_id ORDER BY event.event_sequence
                   ) AS previous_time,
                   lag(event.new_active) OVER (
                       PARTITION BY event.reviewer_id ORDER BY event.event_sequence
                   ) AS previous_new_active
            FROM reviewer_admin_events AS event
        ) AS ordered
        WHERE (ordered.stream_ordinal = 1
               AND ordered.event_kind NOT IN ('register', 'legacy_adopt'))
           OR (ordered.stream_ordinal > 1
               AND ordered.event_kind <> 'set_active')
           OR (ordered.previous_time IS NOT NULL
               AND ordered.effective_time <= ordered.previous_time)
           OR (ordered.stream_ordinal > 1
               AND ordered.previous_active IS NOT ordered.previous_new_active)
        """
    ).fetchone()[0]
    if invalid_first_or_order:
        raise RuntimeError("Reviewer audit streams have invalid enrollment or chronology")

    invalid_manifests = connection.execute(
        """
        SELECT count(*)
        FROM reviewer_admin_manifest_imports AS manifest
        LEFT JOIN reviewer_admin_events AS event
          ON event.manifest_id = manifest.manifest_id
        GROUP BY manifest.manifest_id
        HAVING manifest.registration_count <>
                   sum(CASE WHEN event.event_kind = 'register' THEN 1 ELSE 0 END)
            OR manifest.state_change_count <>
                   sum(CASE WHEN event.event_kind = 'set_active' THEN 1 ELSE 0 END)
            OR manifest.adoption_count <>
                   sum(CASE WHEN event.event_kind = 'legacy_adopt' THEN 1 ELSE 0 END)
            OR count(event.reviewer_admin_event_id) <>
                   manifest.registration_count + manifest.state_change_count
                   + manifest.adoption_count
            OR min(event.ordinal) <> 0
            OR max(event.ordinal) <> count(event.reviewer_admin_event_id) - 1
            OR count(DISTINCT event.ordinal) <> count(event.reviewer_admin_event_id)
            OR julianday(manifest.manifest_created_at) > julianday(manifest.imported_at)
            OR julianday(manifest.imported_at) > julianday('now')
        """
    ).fetchall()
    if invalid_manifests:
        raise RuntimeError("Reviewer admin manifest counts or ordinals are inconsistent")

    invalid_event_times = connection.execute(
        """
        SELECT count(*)
        FROM reviewer_admin_events AS event
        JOIN reviewer_admin_manifest_imports AS manifest
          ON manifest.manifest_id = event.manifest_id
        WHERE julianday(event.effective_at) > julianday(manifest.manifest_created_at)
           OR julianday(event.effective_at) > julianday(manifest.imported_at)
           OR julianday(event.effective_at) > julianday('now')
        """
    ).fetchone()[0]
    if invalid_event_times:
        raise RuntimeError("Reviewer admin event time exceeds its manifest time")


def _validate_private_acquisition_restrictions(
    connection: sqlite3.Connection,
) -> None:
    """Replay durable policy rows against their append-only source observations."""

    # Historical migration tests deliberately validate a closed, checksummed
    # prefix.  Migration 0030 is not part of such a catalog's manifest, so there is
    # no private-restriction state to replay.  Publication/export code still fails
    # closed when called without the new schema.
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'acquisition_handling_restrictions'"
    ).fetchone() is None:
        return

    restrictions = connection.execute(
        """
        SELECT restriction.*,
               batch.importer_name, batch.input_sha256,
               media.sha256 AS media_sha256,
               observation.metadata_json AS source_metadata_json,
               observation.access_state AS observed_access_state
        FROM acquisition_handling_restrictions AS restriction
        LEFT JOIN import_batches AS batch
          ON batch.import_batch_id = restriction.import_batch_id
        LEFT JOIN media_objects AS media
          ON media.media_id = restriction.media_id
        LEFT JOIN source_metadata_observations AS observation
          ON observation.import_batch_id = restriction.import_batch_id
         AND observation.source_id = restriction.source_id
        ORDER BY restriction.restriction_sequence
        """
    ).fetchall()
    for row in restrictions:
        if (
            row["importer_name"] != "acquisition_result_v1"
            or row["input_sha256"] != row["result_canonical_sha256"]
            or row["media_sha256"] is None
            or row["media_id"] != f"media_sha256_{row['media_sha256']}"
            or row["observed_access_state"] != "unknown"
        ):
            raise RuntimeError(
                "Private acquisition restriction provenance is incomplete"
            )
        try:
            policy = validate_handling_policy(
                json.loads(row["policy_json"]), "catalog handling policy"
            )
            source_metadata = json.loads(row["source_metadata_json"])
        except (PrivateAcquisitionError, TypeError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "Private acquisition restriction JSON is invalid"
            ) from error
        if source_metadata != {
            "acquisition_adapter": "local_file",
            "selected_remote_metadata": source_metadata.get(
                "selected_remote_metadata"
            ),
            "handling_policy": policy,
        }:
            raise RuntimeError(
                "Private acquisition source observation dropped or altered policy"
            )
        if (
            hashlib.sha256(canonical_json(policy).encode("utf-8")).hexdigest()
            != row["policy_sha256"]
            or hashlib.sha256(
                canonical_json(source_metadata).encode("utf-8")
            ).hexdigest()
            != row["source_metadata_sha256"]
            or policy["storage_scope"] != row["storage_scope"]
            or policy["publication_disposition"]
            != row["publication_disposition"]
            or policy["publication_authority"] != row["publication_authority"]
            or policy["basis"] != row["basis"]
        ):
            raise RuntimeError(
                "Private acquisition restriction differs from its exact policy"
            )
    missing_restrictions = connection.execute(
        """
        SELECT count(*)
        FROM source_metadata_observations AS observation
        JOIN import_batches AS batch
          ON batch.import_batch_id = observation.import_batch_id
         AND batch.importer_name = 'acquisition_result_v1'
        LEFT JOIN acquisition_handling_restrictions AS restriction
          ON restriction.import_batch_id = observation.import_batch_id
         AND restriction.source_id = observation.source_id
        WHERE json_type(
                  observation.metadata_json, '$.handling_policy'
              ) = 'object'
          AND restriction.acquisition_handling_restriction_id IS NULL
        """
    ).fetchone()[0]
    if missing_restrictions:
        raise RuntimeError(
            "Policy-bearing acquisition observation lacks a durable restriction"
        )
    try:
        assert_no_restricted_publication_state(connection)
    except PrivateAcquisitionError as error:
        raise RuntimeError(str(error)) from error


def _validate_private_solo_voice_attestations(
    connection: sqlite3.Connection,
) -> None:
    """Replay the private named-voice lane without granting publication authority."""

    # Prefix-migration tests may intentionally validate a historical schema.
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'solo_voice_subjects'"
    ).fetchone() is None:
        return

    invalid_manifests = connection.execute(
        """
        SELECT count(*)
        FROM solo_voice_manifest_imports AS manifest
        WHERE manifest.schema_version <> 1
           OR julianday(manifest.manifest_created_at) > julianday(manifest.imported_at)
           OR julianday(manifest.imported_at) > julianday('now')
           OR manifest.subject_count <> (
                SELECT count(*) FROM solo_voice_subjects AS subject
                WHERE subject.manifest_id = manifest.manifest_id
              )
           OR manifest.privacy_review_count <> (
                SELECT count(*) FROM solo_voice_privacy_reviews AS review
                WHERE review.manifest_id = manifest.manifest_id
              )
           OR manifest.speaker_decision_count <> (
                SELECT count(*) FROM solo_voice_attestation_decisions AS decision
                WHERE decision.manifest_id = manifest.manifest_id
              )
           OR (
                manifest.subject_count > 0
                AND (
                    (SELECT min(subject_ordinal) FROM solo_voice_subjects
                     WHERE manifest_id = manifest.manifest_id) <> 0
                    OR
                    (SELECT max(subject_ordinal) FROM solo_voice_subjects
                     WHERE manifest_id = manifest.manifest_id)
                        <> manifest.subject_count - 1
                )
              )
           OR (
                manifest.privacy_review_count > 0
                AND (
                    (SELECT min(review_ordinal) FROM solo_voice_privacy_reviews
                     WHERE manifest_id = manifest.manifest_id) <> 0
                    OR
                    (SELECT max(review_ordinal) FROM solo_voice_privacy_reviews
                     WHERE manifest_id = manifest.manifest_id)
                        <> manifest.privacy_review_count - 1
                )
              )
           OR (
                manifest.speaker_decision_count > 0
                AND (
                    (SELECT min(decision_ordinal)
                     FROM solo_voice_attestation_decisions
                     WHERE manifest_id = manifest.manifest_id) <> 0
                    OR
                    (SELECT max(decision_ordinal)
                     FROM solo_voice_attestation_decisions
                     WHERE manifest_id = manifest.manifest_id)
                        <> manifest.speaker_decision_count - 1
                )
              )
        """
    ).fetchone()[0]
    if invalid_manifests:
        raise RuntimeError("Solo voice manifest ledger is incomplete or inconsistent")

    invalid_subjects = connection.execute(
        """
        SELECT count(*)
        FROM solo_voice_subjects AS subject
        LEFT JOIN entities AS entity ON entity.entity_id = subject.entity_id
        LEFT JOIN sources AS source ON source.source_id = subject.source_id
        LEFT JOIN recordings AS recording
          ON recording.recording_id = subject.recording_id
        LEFT JOIN renditions AS rendition
          ON rendition.rendition_id = subject.rendition_id
        LEFT JOIN media_objects AS media ON media.media_id = subject.media_id
        WHERE subject.coordinate_system <> 'rendition_media_ms'
           OR subject.visibility <> 'private'
           OR subject.publication_authority <> 'none'
           OR entity.entity_id IS NULL
           OR entity.entity_type NOT IN ('person', 'community_figure')
           OR entity.review_state <> 'reviewed'
           OR source.source_id IS NULL
           OR source.review_state <> 'reviewed'
           OR recording.recording_id IS NULL
           OR recording.review_state <> 'reviewed'
           OR recording.merged_into_recording_id IS NOT NULL
           OR rendition.rendition_id IS NULL
           OR rendition.recording_id <> subject.recording_id
           OR rendition.media_id <> subject.media_id
           OR rendition.review_state <> 'reviewed'
           OR media.media_id IS NULL
           OR media.sha256 <> subject.media_sha256
           OR media.integrity_state <> 'verified'
           OR media.media_kind NOT IN ('audio', 'video')
           OR media.duration_ms IS NULL
           OR subject.end_ms > media.duration_ms
           OR NOT EXISTS (
                SELECT 1 FROM recording_sources AS link
                WHERE link.recording_id = subject.recording_id
                  AND link.source_id = subject.source_id
                  AND link.confidence_state = 'reviewed'
              )
           OR NOT EXISTS (
                SELECT 1 FROM media_sources AS link
                WHERE link.media_id = subject.media_id
                  AND link.source_id = subject.source_id
              )
        """
    ).fetchone()[0]
    if invalid_subjects:
        raise RuntimeError("Solo voice subject lost its exact private media anchor")

    invalid_privacy_reviews = connection.execute(
        """
        SELECT count(*)
        FROM solo_voice_privacy_reviews AS privacy
        LEFT JOIN solo_voice_manifest_imports AS manifest
          ON manifest.manifest_id = privacy.manifest_id
        LEFT JOIN reviewers AS reviewer
          ON reviewer.reviewer_id = privacy.reviewer_id
        LEFT JOIN review_decisions AS review
          ON review.review_decision_id = privacy.review_decision_id
        LEFT JOIN reviewer_admin_events AS active_state
          ON active_state.reviewer_id = privacy.reviewer_id
         AND active_state.event_sequence = (
             SELECT max(candidate.event_sequence)
             FROM reviewer_admin_events AS candidate
             WHERE candidate.reviewer_id = privacy.reviewer_id
               AND julianday(candidate.effective_at) <= julianday(privacy.reviewed_at)
         )
        WHERE manifest.manifest_id IS NULL
           OR julianday(privacy.reviewed_at) > julianday(manifest.manifest_created_at)
           OR julianday(privacy.reviewed_at) > julianday(manifest.imported_at)
           OR julianday(privacy.reviewed_at) > julianday('now')
           OR reviewer.reviewer_kind IS NOT 'human'
           OR active_state.new_active IS NOT 1
           OR review.reviewer_id IS NOT privacy.reviewer_id
           OR review.target_type IS NOT 'solo_voice_privacy_review'
           OR review.target_id IS NOT privacy.solo_voice_privacy_review_id
           OR review.decision IS NOT CASE privacy.decision
                  WHEN 'clear_private_use' THEN 'accept' ELSE 'reject' END
           OR review.decided_at IS NOT privacy.reviewed_at
           OR review.audio_directly_perceived IS NOT 0
           OR review.video_directly_perceived IS NOT 0
           OR review.reviewed_complete_item IS NOT 1
           OR review.context_start_ms IS NOT NULL
           OR review.context_end_ms IS NOT NULL
           OR review.review_task_id IS NOT NULL
           OR review.notes IS NOT NULL
           OR review.basis IS NOT privacy.basis
           OR privacy.named_voice_personal_data_reviewed IS NOT 1
           OR privacy.private_storage_only IS NOT 1
           OR privacy.public_export_approved IS NOT 0
           OR privacy.biometric_artifacts_used IS NOT 0
           OR privacy.machine_identity_outputs_used IS NOT 0
           OR privacy.visibility IS NOT 'private'
           OR privacy.publication_authority IS NOT 'none'
        """
    ).fetchone()[0]
    if invalid_privacy_reviews:
        raise RuntimeError("Solo voice privacy review lacks exact governed human lineage")

    invalid_speaker_decisions = connection.execute(
        """
        SELECT count(*)
        FROM solo_voice_attestation_decisions AS decision
        LEFT JOIN solo_voice_manifest_imports AS manifest
          ON manifest.manifest_id = decision.manifest_id
        LEFT JOIN solo_voice_subjects AS subject
          ON subject.solo_voice_subject_id = decision.solo_voice_subject_id
        LEFT JOIN reviewers AS reviewer
          ON reviewer.reviewer_id = decision.reviewer_id
        LEFT JOIN review_decisions AS review
          ON review.review_decision_id = decision.review_decision_id
        LEFT JOIN reviewer_admin_events AS active_state
          ON active_state.reviewer_id = decision.reviewer_id
         AND active_state.event_sequence = (
             SELECT max(candidate.event_sequence)
             FROM reviewer_admin_events AS candidate
             WHERE candidate.reviewer_id = decision.reviewer_id
               AND julianday(candidate.effective_at) <= julianday(decision.decided_at)
         )
        WHERE manifest.manifest_id IS NULL
           OR subject.solo_voice_subject_id IS NULL
           OR julianday(decision.decided_at) > julianday(manifest.manifest_created_at)
           OR julianday(decision.decided_at) > julianday(manifest.imported_at)
           OR julianday(decision.decided_at) > julianday('now')
           OR reviewer.reviewer_kind IS NOT 'human'
           OR active_state.new_active IS NOT 1
           OR review.reviewer_id IS NOT decision.reviewer_id
           OR review.target_type IS NOT 'solo_voice_subject'
           OR review.target_id IS NOT decision.solo_voice_subject_id
           OR review.decision IS NOT CASE decision.decision
                  WHEN 'assert' THEN 'accept'
                  WHEN 'withdraw' THEN 'correct'
                  WHEN 'reject' THEN 'reject'
                  ELSE 'dispute' END
           OR review.decided_at IS NOT decision.decided_at
           OR review.audio_directly_perceived IS NOT CASE decision.decision
                  WHEN 'assert' THEN 1 ELSE 0 END
           OR review.video_directly_perceived IS NOT 0
           OR review.reviewed_complete_item IS NOT CASE decision.decision
                  WHEN 'assert' THEN 1 ELSE 0 END
           OR review.context_start_ms IS NOT subject.start_ms
           OR review.context_end_ms IS NOT subject.end_ms
           OR review.review_task_id IS NOT NULL
           OR review.notes IS NOT NULL
           OR review.basis IS NOT decision.basis
           OR decision.visibility <> 'private'
           OR decision.publication_authority <> 'none'
           OR (
                decision.decision = 'assert'
                AND (
                    decision.direct_audio_attestation IS NOT
                      'I directly listened to the complete anchored interval and identify exactly one live human voice as the named entity without relying on source or channel context, transcript text, machine identity output, or machine confidence.'
                    OR decision.audio_directly_perceived IS NOT 1
                    OR decision.reviewed_entire_interval IS NOT 1
                    OR decision.exactly_one_live_human_speaker IS NOT 1
                    OR decision.overlap_detected IS NOT 0
                    OR decision.playback_detected IS NOT 0
                    OR decision.tts_detected IS NOT 0
                    OR decision.synthetic_voice_detected IS NOT 0
                    OR decision.unknown_audio_origin_detected IS NOT 0
                    OR decision.source_metadata_used_as_identity_evidence IS NOT 0
                    OR decision.channel_context_used_as_identity_evidence IS NOT 0
                    OR decision.transcript_text_used_as_identity_evidence IS NOT 0
                    OR decision.machine_identity_output_used IS NOT 0
                    OR decision.machine_confidence_used IS NOT 0
                    OR decision.speaking_face_claimed IS NOT 0
                    OR NOT EXISTS (
                        SELECT 1
                        FROM solo_voice_privacy_reviews AS privacy
                        WHERE privacy.solo_voice_subject_id =
                                  decision.solo_voice_subject_id
                          AND privacy.decision = 'clear_private_use'
                          AND privacy.reviewer_id <> decision.reviewer_id
                          AND julianday(privacy.reviewed_at) <
                                  julianday(decision.decided_at)
                          AND NOT EXISTS (
                              SELECT 1
                              FROM solo_voice_privacy_reviews AS later_privacy
                              WHERE later_privacy.solo_voice_subject_id =
                                        privacy.solo_voice_subject_id
                                AND julianday(later_privacy.reviewed_at) <
                                      julianday(decision.decided_at)
                                AND (
                                    julianday(later_privacy.reviewed_at) >
                                          julianday(privacy.reviewed_at)
                                    OR (
                                        julianday(later_privacy.reviewed_at) =
                                              julianday(privacy.reviewed_at)
                                        AND later_privacy.privacy_review_sequence >
                                              privacy.privacy_review_sequence
                                    )
                                )
                          )
                    )
                )
              )
           OR (
                decision.decision <> 'assert'
                AND (
                    decision.direct_audio_attestation IS NOT NULL
                    OR decision.audio_directly_perceived IS NOT NULL
                    OR decision.reviewed_entire_interval IS NOT NULL
                    OR decision.exactly_one_live_human_speaker IS NOT NULL
                    OR decision.overlap_detected IS NOT NULL
                    OR decision.playback_detected IS NOT NULL
                    OR decision.tts_detected IS NOT NULL
                    OR decision.synthetic_voice_detected IS NOT NULL
                    OR decision.unknown_audio_origin_detected IS NOT NULL
                    OR decision.source_metadata_used_as_identity_evidence IS NOT NULL
                    OR decision.channel_context_used_as_identity_evidence IS NOT NULL
                    OR decision.transcript_text_used_as_identity_evidence IS NOT NULL
                    OR decision.machine_identity_output_used IS NOT NULL
                    OR decision.machine_confidence_used IS NOT NULL
                    OR decision.speaking_face_claimed IS NOT NULL
                )
              )
        """
    ).fetchone()[0]
    if invalid_speaker_decisions:
        raise RuntimeError("Solo voice decision lacks exact direct-audio human lineage")

    invalid_stream_order = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM (
             SELECT reviewed_at,
                    lag(julianday(reviewed_at)) OVER (
                        PARTITION BY solo_voice_subject_id
                        ORDER BY privacy_review_sequence
                    ) AS previous_time
             FROM solo_voice_privacy_reviews
           ) WHERE previous_time IS NOT NULL
               AND julianday(reviewed_at) <= previous_time)
          +
          (SELECT count(*) FROM (
             SELECT decided_at,
                    lag(julianday(decided_at)) OVER (
                        PARTITION BY solo_voice_subject_id
                        ORDER BY speaker_decision_sequence
                    ) AS previous_time
             FROM solo_voice_attestation_decisions
           ) WHERE previous_time IS NOT NULL
               AND julianday(decided_at) <= previous_time)
        """
    ).fetchone()[0]
    if invalid_stream_order:
        raise RuntimeError("Solo voice decision streams are not strictly chronological")

    conflicting_current_assignments = connection.execute(
        """
        SELECT count(*)
        FROM solo_voice_subjects AS left_subject
        JOIN current_solo_voice_attestation_decisions AS left_decision
          ON left_decision.solo_voice_subject_id = left_subject.solo_voice_subject_id
         AND left_decision.decision = 'assert'
        JOIN solo_voice_subjects AS right_subject
          ON right_subject.media_id = left_subject.media_id
         AND right_subject.solo_voice_subject_id > left_subject.solo_voice_subject_id
         AND right_subject.start_ms < left_subject.end_ms
         AND left_subject.start_ms < right_subject.end_ms
         AND right_subject.entity_id <> left_subject.entity_id
        JOIN current_solo_voice_attestation_decisions AS right_decision
          ON right_decision.solo_voice_subject_id = right_subject.solo_voice_subject_id
         AND right_decision.decision = 'assert'
        """
    ).fetchone()[0]
    if conflicting_current_assignments:
        raise RuntimeError("Current solo voice identities conflict on an overlapping interval")

    historical_conflicting_assertions = connection.execute(
        """
        SELECT count(*)
        FROM solo_voice_attestation_decisions AS proposed_decision
        JOIN solo_voice_subjects AS proposed_subject
          ON proposed_subject.solo_voice_subject_id =
                proposed_decision.solo_voice_subject_id
        WHERE proposed_decision.decision = 'assert'
          AND EXISTS (
              SELECT 1
              FROM solo_voice_subjects AS existing_subject
              JOIN solo_voice_attestation_decisions AS prior_state
                ON prior_state.solo_voice_subject_id =
                      existing_subject.solo_voice_subject_id
               AND prior_state.speaker_decision_sequence = (
                   SELECT max(candidate.speaker_decision_sequence)
                   FROM solo_voice_attestation_decisions AS candidate
                   WHERE candidate.solo_voice_subject_id =
                             existing_subject.solo_voice_subject_id
                     AND candidate.speaker_decision_sequence <
                           proposed_decision.speaker_decision_sequence
               )
              WHERE existing_subject.solo_voice_subject_id <>
                        proposed_subject.solo_voice_subject_id
                AND existing_subject.media_id = proposed_subject.media_id
                AND existing_subject.start_ms < proposed_subject.end_ms
                AND proposed_subject.start_ms < existing_subject.end_ms
                AND existing_subject.entity_id <> proposed_subject.entity_id
                AND prior_state.decision = 'assert'
          )
        """
    ).fetchone()[0]
    if historical_conflicting_assertions:
        raise RuntimeError(
            "Solo voice assertion history admitted an overlapping conflicting identity"
        )

    publication_intent = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM publication_decisions
           WHERE object_type IN (
                'solo_voice_subject', 'solo_voice_privacy_review',
                'solo_voice_attestation_decision'
             ))
          +
          (SELECT count(*) FROM publication_gate_decisions
           WHERE object_type IN (
                'solo_voice_subject', 'solo_voice_privacy_review',
                'solo_voice_attestation_decision'
             ))
        """
    ).fetchone()[0]
    if publication_intent:
        raise RuntimeError("Private solo voice object has reserved publication state")


def _validate_private_ocr_tesseract(connection: sqlite3.Connection) -> None:
    """Replay private OCR receipts, relational safety, and exact FTS coverage."""

    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'private_ocr_import_receipts'"
    ).fetchone() is None:
        return

    invalid_receipts = connection.execute(
        """
        SELECT count(*)
        FROM private_ocr_import_receipts AS receipt
        LEFT JOIN import_batches AS batch
          ON batch.import_batch_id = receipt.import_batch_id
        LEFT JOIN processing_runs AS run
          ON run.processing_run_id = receipt.processing_run_id
        LEFT JOIN processing_runs AS sparse_run
          ON sparse_run.processing_run_id = receipt.sparse_frame_processing_run_id
        LEFT JOIN media_objects AS proxy
          ON proxy.media_id = receipt.proxy_media_id
        WHERE receipt.coordinate_system IS NOT 'rendition_media_ms'
           OR receipt.boundary IS NOT 'half_open'
           OR receipt.visibility IS NOT 'private'
           OR receipt.human_review IS NOT 'required'
           OR receipt.redaction_state IS NOT 'pending'
           OR receipt.score_calibration IS NOT 'not_calibrated'
           OR receipt.calibrated_probabilities_present IS NOT 0
           OR receipt.publication_authority IS NOT 'none'
           OR receipt.identity_authority IS NOT 'none'
           OR receipt.event_authority IS NOT 'none'
           OR receipt.export_authority IS NOT 'none'
           OR batch.import_batch_id IS NULL
           OR batch.importer_name IS NOT 'private_ocr_tesseract_result_v1'
           OR batch.input_sha256 IS NOT receipt.result_raw_sha256
           OR batch.status IS NOT 'completed'
           OR run.processing_run_id IS NULL
           OR run.stage IS NOT 'ocr_tesseract_tsv'
           OR run.status IS NOT 'completed'
           OR sparse_run.processing_run_id IS NULL
           OR sparse_run.stage IS NOT 'sparse_frame_router'
           OR sparse_run.status IS NOT 'completed'
           OR proxy.media_id IS NULL
           OR proxy.integrity_state IS NOT 'verified'
           OR proxy.media_kind IS NOT 'video'
           OR receipt.admitted_frame_count IS NOT (
                SELECT count(*) FROM private_ocr_frame_admissions AS frame
                WHERE frame.private_ocr_import_receipt_id =
                      receipt.private_ocr_import_receipt_id
              )
           OR receipt.word_count IS NOT (
                SELECT count(*)
                FROM private_ocr_word_admissions AS word
                JOIN private_ocr_frame_admissions AS frame
                  ON frame.private_ocr_frame_admission_id =
                     word.private_ocr_frame_admission_id
                WHERE frame.private_ocr_import_receipt_id =
                      receipt.private_ocr_import_receipt_id
              )
           OR receipt.observation_count IS NOT receipt.word_count
           OR receipt.tsv_artifact_count IS NOT receipt.selected_frame_count
           OR receipt.selected_frame_count <= 0
           OR receipt.admitted_frame_count % receipt.selected_frame_count IS NOT 0
           OR receipt.selected_frame_count IS NOT (
                SELECT count(DISTINCT frame.frame_id)
                FROM private_ocr_frame_admissions AS frame
                WHERE frame.private_ocr_import_receipt_id =
                      receipt.private_ocr_import_receipt_id
              )
           OR receipt.tsv_artifact_count IS NOT (
                SELECT count(DISTINCT frame.tsv_artifact_id)
                FROM private_ocr_frame_admissions AS frame
                WHERE frame.private_ocr_import_receipt_id =
                      receipt.private_ocr_import_receipt_id
              )
        """
    ).fetchone()[0]
    invalid_frames = connection.execute(
        """
        SELECT count(*)
        FROM private_ocr_frame_admissions AS frame
        LEFT JOIN private_ocr_import_receipts AS receipt
          ON receipt.private_ocr_import_receipt_id =
             frame.private_ocr_import_receipt_id
        LEFT JOIN renditions AS rendition
          ON rendition.rendition_id = frame.rendition_id
        LEFT JOIN renditions AS source_rendition
          ON source_rendition.rendition_id = frame.source_rendition_id
        LEFT JOIN media_objects AS media ON media.media_id = frame.media_id
        LEFT JOIN media_objects AS source_media
          ON source_media.media_id = frame.source_media_id
        LEFT JOIN artifacts AS png ON png.artifact_id = frame.frame_artifact_id
        LEFT JOIN artifacts AS tsv ON tsv.artifact_id = frame.tsv_artifact_id
        LEFT JOIN observations AS sparse
          ON sparse.observation_id = frame.sparse_frame_observation_id
        LEFT JOIN sources AS source ON source.source_id = frame.source_id
        LEFT JOIN recordings AS recording
          ON recording.recording_id = frame.recording_id
        WHERE receipt.private_ocr_import_receipt_id IS NULL
           OR frame.coordinate_system IS NOT 'rendition_media_ms'
           OR frame.boundary IS NOT 'half_open'
           OR frame.visibility IS NOT 'private'
           OR frame.human_review IS NOT 'required'
           OR frame.redaction_state IS NOT 'pending'
           OR frame.publication_authority IS NOT 'none'
           OR frame.identity_authority IS NOT 'none'
           OR frame.event_authority IS NOT 'none'
           OR rendition.rendition_id IS NULL
           OR rendition.recording_id IS NOT frame.recording_id
           OR rendition.media_id IS NOT frame.media_id
           OR receipt.proxy_media_id IS NOT frame.media_id
           OR rendition.rendition_kind IS NOT 'low_resolution_cfr_proxy'
           OR rendition.review_state IS 'rejected'
           OR json_extract(rendition.metadata_json,
                           '$.derived_from_rendition_id') IS NOT
              frame.source_rendition_id
           OR source_rendition.rendition_id IS NULL
           OR source_rendition.recording_id IS NOT frame.recording_id
           OR source_rendition.media_id IS NOT frame.source_media_id
           OR source_rendition.review_state IS 'rejected'
           OR source_media.media_id IS NULL
           OR source_media.integrity_state IS NOT 'verified'
           OR source_media.media_kind IS NOT 'video'
           OR media.media_id IS NULL
           OR media.integrity_state IS NOT 'verified'
           OR media.media_kind IS NOT 'video'
           OR media.duration_ms IS NULL
           OR frame.end_ms > media.duration_ms
           OR png.artifact_id IS NULL
           OR png.sha256 IS NOT frame.frame_png_sha256
           OR png.visibility IS NOT 'private'
           OR tsv.artifact_id IS NULL
           OR tsv.artifact_kind IS NOT 'tesseract_tsv'
           OR tsv.processing_run_id IS NOT receipt.processing_run_id
           OR tsv.sha256 IS NOT frame.tsv_sha256
           OR tsv.visibility IS NOT 'private'
           OR sparse.observation_id IS NULL
           OR sparse.observation_kind IS NOT 'sparse_frame_routing_candidate'
           OR sparse.recording_id IS NOT frame.recording_id
           OR sparse.rendition_id IS NOT frame.rendition_id
           OR sparse.processing_run_id IS NOT
              receipt.sparse_frame_processing_run_id
           OR sparse.start_ms IS NOT frame.start_ms
           OR sparse.end_ms IS NOT frame.end_ms
           OR sparse.visibility IS NOT 'private'
           OR sparse.review_state IS NOT 'machine'
           OR json_extract(sparse.metadata_json, '$.frame_id') IS NOT frame.frame_id
           OR json_extract(sparse.metadata_json, '$.artifact_id') IS NOT
              frame.frame_artifact_id
           OR json_extract(sparse.metadata_json, '$.requested_timestamp_ms') IS NOT
              frame.requested_timestamp_ms
           OR source.source_id IS NULL
           OR source.review_state IS 'rejected'
           OR recording.recording_id IS NULL
           OR recording.review_state IS 'rejected'
           OR recording.merged_into_recording_id IS NOT NULL
           OR NOT EXISTS (
                SELECT 1 FROM media_sources AS link
                WHERE link.media_id = frame.source_media_id
                  AND link.source_id = frame.source_id
              )
           OR NOT EXISTS (
                SELECT 1 FROM recording_sources AS link
                WHERE link.recording_id = frame.recording_id
                  AND link.source_id = frame.source_id
                  AND link.confidence_state IS NOT 'rejected'
              )
           OR frame.word_count IS NOT (
                SELECT count(*) FROM private_ocr_word_admissions AS word
                WHERE word.private_ocr_frame_admission_id =
                      frame.private_ocr_frame_admission_id
              )
           OR (frame.text_presence = 'detected') IS NOT (frame.word_count > 0)
        """
    ).fetchone()[0]
    invalid_words = connection.execute(
        """
        SELECT count(*)
        FROM private_ocr_word_admissions AS word
        LEFT JOIN private_ocr_frame_admissions AS frame
          ON frame.private_ocr_frame_admission_id =
             word.private_ocr_frame_admission_id
        LEFT JOIN observations AS observation
          ON observation.observation_id = word.observation_id
        LEFT JOIN ocr_observations AS ocr
          ON ocr.observation_id = word.observation_id
        LEFT JOIN observation_scores AS score
          ON score.observation_id = word.observation_id
         AND score.score_name = 'tesseract_raw_0_100_not_probability'
        WHERE frame.private_ocr_frame_admission_id IS NULL
           OR word.raw_score < 0 OR word.raw_score > 100
           OR word.score_name IS NOT 'tesseract_raw_0_100_not_probability'
           OR word.score_calibration IS NOT 'not_calibrated'
           OR word.probability_interpretation IS NOT 'not_a_probability'
           OR word.calibrated_probability IS NOT NULL
           OR word.calibration_set_id IS NOT NULL
           OR word.visibility IS NOT 'private'
           OR word.review_state IS NOT 'machine'
           OR word.human_review IS NOT 'required'
           OR word.redaction_state IS NOT 'pending'
           OR word.publication_authority IS NOT 'none'
           OR word.identity_authority IS NOT 'none'
           OR word.event_authority IS NOT 'none'
           OR observation.observation_id IS NULL
           OR observation.observation_kind IS NOT 'ocr_tesseract_word_candidate'
           OR observation.recording_id IS NOT frame.recording_id
           OR observation.rendition_id IS NOT frame.rendition_id
           OR observation.processing_run_id IS NOT (
                SELECT receipt.processing_run_id
                FROM private_ocr_import_receipts AS receipt
                WHERE receipt.private_ocr_import_receipt_id =
                      frame.private_ocr_import_receipt_id
              )
           OR observation.start_ms IS NOT frame.start_ms
           OR observation.end_ms IS NOT frame.end_ms
           OR observation.visibility IS NOT 'private'
           OR observation.review_state IS NOT 'machine'
           OR observation.payload_schema_version IS NOT 1
           OR json_extract(observation.metadata_json, '$.coordinate_system')
                IS NOT 'rendition_media_ms'
           OR json_extract(observation.metadata_json, '$.human_review')
                IS NOT 'required'
           OR json_extract(observation.metadata_json, '$.redaction_state')
                IS NOT 'pending'
           OR json_extract(observation.metadata_json, '$.publication_authority')
                IS NOT 'none'
           OR json_extract(observation.metadata_json, '$.identity_authority')
                IS NOT 'none'
           OR json_extract(observation.metadata_json, '$.event_authority')
                IS NOT 'none'
           OR json_extract(observation.metadata_json, '$.frame_id')
                IS NOT frame.frame_id
           OR json_extract(observation.metadata_json, '$.region_id')
                IS NOT word.region_id
           OR json_extract(observation.metadata_json, '$.source_id')
                IS NOT frame.source_id
           OR ocr.observation_id IS NULL
           OR ocr.raw_text IS NOT word.raw_text
           OR ocr.normalized_text IS NOT NULL
           OR ocr.redaction_state IS NOT 'pending'
           OR score.observation_id IS NULL
           OR score.raw_score IS NOT word.raw_score
           OR score.calibrated_probability IS NOT NULL
           OR score.calibration_set_id IS NOT NULL
           OR (SELECT count(*) FROM observation_scores AS every_score
               WHERE every_score.observation_id = word.observation_id) IS NOT 1
        """
    ).fetchone()[0]
    orphan_private_ocr = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM observations AS observation
           WHERE observation.observation_kind = 'ocr_tesseract_word_candidate'
             AND NOT EXISTS (
                 SELECT 1 FROM private_ocr_word_admissions AS word
                 WHERE word.observation_id = observation.observation_id
             ))
          +
          (SELECT count(*) FROM artifacts AS artifact
           WHERE artifact.artifact_kind = 'tesseract_tsv'
             AND NOT EXISTS (
                 SELECT 1 FROM private_ocr_frame_admissions AS frame
                 WHERE frame.tsv_artifact_id = artifact.artifact_id
             ))
          +
          (SELECT count(*) FROM processing_runs AS run
           WHERE run.stage = 'ocr_tesseract_tsv'
             AND NOT EXISTS (
                 SELECT 1 FROM private_ocr_import_receipts AS receipt
                 WHERE receipt.processing_run_id = run.processing_run_id
             ))
        """
    ).fetchone()[0]
    invalid_fts = connection.execute(
        """
        SELECT
            abs((SELECT count(*) FROM private_ocr_word_admissions)
              - (SELECT count(*) FROM private_ocr_word_fts))
          + (SELECT count(*)
             FROM private_ocr_word_admissions AS word
             LEFT JOIN private_ocr_word_fts AS search
               ON search.private_ocr_word_admission_id =
                  word.private_ocr_word_admission_id
              AND search.raw_text = word.raw_text
             WHERE search.private_ocr_word_admission_id IS NULL)
          + (SELECT count(*)
             FROM private_ocr_word_fts AS search
             LEFT JOIN private_ocr_word_admissions AS word
               ON word.private_ocr_word_admission_id =
                  search.private_ocr_word_admission_id
              AND word.raw_text = search.raw_text
             WHERE word.private_ocr_word_admission_id IS NULL)
        """
    ).fetchone()[0]
    authority_leaks = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM publication_decisions AS decision
           WHERE decision.object_type IN (
             'private_ocr_import_receipt', 'private_ocr_frame_admission',
             'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
           )
              OR (decision.object_type = 'observation' AND EXISTS (
                  SELECT 1 FROM observations AS observation
                  WHERE observation.observation_id = decision.object_id
                    AND observation.observation_kind = 'ocr_tesseract_word_candidate'
              ))
              OR (decision.object_type = 'artifact' AND EXISTS (
                  SELECT 1 FROM artifacts AS artifact
                  WHERE artifact.artifact_id = decision.object_id
                    AND artifact.artifact_kind = 'tesseract_tsv'
              )))
          +
          (SELECT count(*) FROM publication_gate_decisions AS decision
           WHERE decision.object_type IN (
             'private_ocr_import_receipt', 'private_ocr_frame_admission',
             'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
           )
              OR (decision.object_type = 'observation' AND EXISTS (
                  SELECT 1 FROM observations AS observation
                  WHERE observation.observation_id = decision.object_id
                    AND observation.observation_kind = 'ocr_tesseract_word_candidate'
              ))
              OR (decision.object_type = 'artifact' AND EXISTS (
                  SELECT 1 FROM artifacts AS artifact
                  WHERE artifact.artifact_id = decision.object_id
                    AND artifact.artifact_kind = 'tesseract_tsv'
              )))
          +
          (SELECT count(*) FROM appearances
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
          +
          (SELECT count(*) FROM identity_cluster_memberships
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
          +
          (SELECT count(*) FROM event_evidence
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
          +
          (SELECT count(*) FROM claim_catalog_links
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
        """
    ).fetchone()[0]
    if (
        invalid_receipts
        or invalid_frames
        or invalid_words
        or orphan_private_ocr
        or invalid_fts
        or authority_leaks
    ):
        raise RuntimeError(
            "Private Tesseract OCR escaped exact private redaction-pending semantics"
        )

    try:
        _assert_private_ocr_fts_exact(connection)
    except ResultImportError as error:
        raise RuntimeError(
            f"Private Tesseract OCR FTS validation failed: {error}"
        ) from error

    for receipt in connection.execute(
        "SELECT * FROM private_ocr_import_receipts ORDER BY receipt_sequence"
    ):
        try:
            path = _local_file_uri(receipt["result_uri"], "private OCR result URI")
            result = _read_ocr_result(path)
            anchors = _exact_ocr_source_anchors(connection, result["_sparse"])
            frame_rows, word_rows = _ocr_frame_rows(
                connection,
                result,
                anchors,
                receipt["private_ocr_import_receipt_id"],
            )
            _assert_exact_ocr_replay(
                connection,
                receipt,
                result=result,
                frame_rows=frame_rows,
                word_rows=word_rows,
                check_fts=False,
            )
        except ResultImportError as error:
            raise RuntimeError(
                f"Private Tesseract OCR current-file validation failed: {error}"
            ) from error


def validate_database(connection: sqlite3.Connection) -> dict:
    # Validation never installs schema. Pending, missing, renamed, or changed
    # migrations require an explicit administrative migrate command.
    verify_migrations(connection)
    integrity = connection.execute("PRAGMA integrity_check").fetchall()
    if [row[0] for row in integrity] != ["ok"]:
        raise RuntimeError(f"SQLite integrity check failed: {integrity}")
    foreign_key_issues = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_key_issues:
        raise RuntimeError(f"Foreign-key violations: {foreign_key_issues[:10]}")
    _validate_reviewer_administration(connection)
    _validate_private_acquisition_restrictions(connection)
    _validate_private_solo_voice_attestations(connection)
    _validate_private_ocr_tesseract(connection)
    future_publication_decisions = connection.execute(
        """
        SELECT
            (SELECT count(*) FROM publication_decisions
             WHERE julianday(decided_at) > julianday('now'))
          + (SELECT count(*) FROM publication_gate_decisions
             WHERE julianday(decided_at) > julianday('now'))
        """
    ).fetchone()[0]
    if future_publication_decisions:
        raise RuntimeError("Publication decisions contain a future effective time")
    validate_machine_transcript_publication_policy(connection)
    _validate_contextual_media_local_files(connection)
    invalid_audio_v2 = connection.execute(
        """
        SELECT count(*)
        FROM audio_fingerprint_match_candidates_v2 AS typed
        LEFT JOIN match_candidates AS candidate
          ON candidate.match_candidate_id = typed.match_candidate_id
        LEFT JOIN audio_fingerprint_compare_v2_receipts AS receipt
          ON receipt.match_candidate_id = typed.match_candidate_id
        LEFT JOIN audio_fingerprint_compare_v2_sides AS query_side
          ON query_side.match_candidate_id = typed.match_candidate_id
         AND query_side.role = 'query'
        LEFT JOIN audio_fingerprint_compare_v2_sides AS candidate_side
          ON candidate_side.match_candidate_id = typed.match_candidate_id
         AND candidate_side.role = 'candidate'
        LEFT JOIN processing_runs AS comparison_run
          ON comparison_run.processing_run_id = typed.processing_run_id
        WHERE candidate.match_candidate_id IS NULL
           OR receipt.comparison_result_sha256 IS NULL
           OR query_side.role IS NULL
           OR candidate_side.role IS NULL
           OR receipt.processing_run_id IS NOT typed.processing_run_id
           OR receipt.query_result_sha256 IS NOT typed.query_result_sha256
           OR receipt.candidate_result_sha256 IS NOT typed.candidate_result_sha256
           OR query_side.extraction_run_id IS NOT typed.query_extraction_run_id
           OR candidate_side.extraction_run_id IS NOT typed.candidate_extraction_run_id
           OR query_side.extraction_result_sha256 IS NOT typed.query_result_sha256
           OR candidate_side.extraction_result_sha256 IS NOT typed.candidate_result_sha256
           OR query_side.fingerprint_id IS NOT typed.query_fingerprint_id
           OR candidate_side.fingerprint_id IS NOT typed.candidate_fingerprint_id
           OR receipt.calibration_state <> 'not_calibrated'
           OR receipt.visibility <> 'private'
           OR receipt.publication_authority <> 'none'
           OR candidate.calibrated_probability IS NOT NULL
           OR candidate.decision_state <> 'candidate'
           OR typed.calibration_state <> 'not_calibrated'
           OR typed.requires_human_review <> 1
           OR typed.relationship_asserted <> 0
           OR typed.visibility <> 'private'
           OR typed.publication_authority <> 'none'
           OR comparison_run.stage <> 'audio_fingerprint_exact_compare_v2'
           OR comparison_run.implementation_version <> '0.2.0'
           OR comparison_run.status <> 'completed'
           OR receipt.exact_raw_equal <> CASE
                  WHEN query_side.artifact_sha256 = candidate_side.artifact_sha256
                   AND query_side.artifact_byte_count = candidate_side.artifact_byte_count
                  THEN 1 ELSE 0 END
           OR candidate.raw_score <> CAST(receipt.exact_raw_equal AS REAL)
           OR (SELECT count(*) FROM audio_fingerprint_compare_v2_sides AS side
               WHERE side.match_candidate_id = typed.match_candidate_id) <> 2
           OR (SELECT count(*) FROM audio_fingerprint_result_imports AS import_receipt
               WHERE import_receipt.result_kind = 'exact_comparison'
                 AND import_receipt.processing_run_id = typed.processing_run_id) <> 1
           OR (SELECT count(*) FROM run_inputs AS input
               WHERE input.processing_run_id = typed.processing_run_id) <> 4
        """
    ).fetchone()[0]
    orphan_audio_v2_bindings = connection.execute(
        """
        SELECT
            (SELECT count(*)
             FROM audio_fingerprint_compare_v2_receipts AS receipt
             LEFT JOIN audio_fingerprint_match_candidates_v2 AS typed
               ON typed.match_candidate_id = receipt.match_candidate_id
             WHERE typed.match_candidate_id IS NULL)
          + (SELECT count(*)
             FROM audio_fingerprint_compare_v2_sides AS side
             LEFT JOIN audio_fingerprint_match_candidates_v2 AS typed
               ON typed.match_candidate_id = side.match_candidate_id
             WHERE typed.match_candidate_id IS NULL)
        """
    ).fetchone()[0]
    invalid_audio_v2_extractions = connection.execute(
        "SELECT count(*) FROM audio_fingerprint_compare_v2_invalid_extraction_sides"
    ).fetchone()[0]
    if invalid_audio_v2 or orphan_audio_v2_bindings or invalid_audio_v2_extractions:
        raise RuntimeError(
            "Audio fingerprint v2 evidence violates canonical sealed-graph invariants"
        )
    _validate_audio_fingerprint_compare_v2_files(connection)
    legacy_transcripts = connection.execute(
        "SELECT count(*) FROM transcript_revisions WHERE lower(origin) LIKE 'legacy%'"
    ).fetchone()[0]
    if legacy_transcripts:
        raise RuntimeError("Legacy transcript revisions were imported")
    contaminated_sources = connection.execute(
        """
        SELECT count(*) FROM sources
        WHERE instr(metadata_json, '"machine_summary"') > 0
           OR instr(metadata_json, '"speakers"') > 0
        """
    ).fetchone()[0]
    contaminated_snapshots = connection.execute(
        """
        SELECT count(*) FROM source_snapshots
        WHERE instr(metadata_json, '"machine_summary"') > 0
           OR instr(metadata_json, '"speakers"') > 0
        """
    ).fetchone()[0]
    if contaminated_sources or contaminated_snapshots:
        raise RuntimeError("Legacy machine summary or speaker keys entered catalog metadata")
    mismatched_source_winners = connection.execute(
        """
        WITH ranked AS (
            SELECT observation.*,
                   row_number() OVER (
                       PARTITION BY source_id
                       ORDER BY quality_rank DESC, julianday(observed_at) DESC,
                                CASE access_state
                                    WHEN 'private' THEN 5
                                    WHEN 'members_only' THEN 4
                                    WHEN 'removed' THEN 3
                                    WHEN 'unavailable' THEN 2
                                    WHEN 'unknown' THEN 1
                                    ELSE 0
                                END DESC,
                                candidate_sha256 DESC, import_batch_id DESC,
                                source_metadata_observation_id DESC
                   ) AS winner_rank
            FROM source_metadata_observations AS observation
        )
        SELECT count(*)
        FROM sources AS source
        JOIN ranked AS winner
          ON winner.source_id = source.source_id AND winner.winner_rank = 1
        WHERE source.current_metadata_observation_id IS NOT
                  winner.source_metadata_observation_id
           OR source.access_state <> winner.access_state
           OR source.metadata_json <> winner.metadata_json
        """
    ).fetchone()[0]
    mismatched_recording_winners = connection.execute(
        """
        WITH ranked AS (
            SELECT observation.*,
                   row_number() OVER (
                       PARTITION BY recording_id
                       ORDER BY quality_rank DESC, julianday(observed_at) DESC,
                                candidate_sha256 DESC, import_batch_id DESC,
                                recording_metadata_observation_id DESC
                   ) AS winner_rank
            FROM recording_metadata_observations AS observation
        )
        SELECT count(*)
        FROM recordings AS recording
        JOIN ranked AS winner
          ON winner.recording_id = recording.recording_id AND winner.winner_rank = 1
        WHERE recording.current_metadata_observation_id IS NOT
                  winner.recording_metadata_observation_id
           OR recording.metadata_json <> winner.metadata_json
        """
    ).fetchone()[0]
    mismatched_relation_winners = connection.execute(
        """
        WITH ranked AS (
            SELECT observation.*,
                   row_number() OVER (
                       PARTITION BY source_relation_id
                       ORDER BY quality_rank DESC, julianday(observed_at) DESC,
                                candidate_sha256 DESC, import_batch_id DESC,
                                source_relation_observation_id DESC
                   ) AS winner_rank
            FROM source_relation_observations AS observation
        )
        SELECT count(*)
        FROM source_relations AS relation
        JOIN ranked AS winner
          ON winner.source_relation_id = relation.source_relation_id
         AND winner.winner_rank = 1
        WHERE relation.current_relation_observation_id IS NOT
                  winner.source_relation_observation_id
           OR relation.basis <> winner.basis
           OR relation.confidence_state <> winner.confidence_state
           OR relation.metadata_json <> winner.metadata_json
           OR relation.import_batch_id <> winner.import_batch_id
        """
    ).fetchone()[0]
    mismatched_external_id_winners = connection.execute(
        """
        WITH ranked AS (
            SELECT observation.*,
                   row_number() OVER (
                       PARTITION BY external_id_id
                       ORDER BY quality_rank DESC, julianday(observed_at) DESC,
                                candidate_sha256 DESC, import_batch_id DESC,
                                external_id_observation_id DESC
                   ) AS winner_rank
            FROM external_id_observations AS observation
        )
        SELECT count(*)
        FROM external_ids AS external_id
        JOIN ranked AS winner
          ON winner.external_id_id = external_id.external_id_id AND winner.winner_rank = 1
        WHERE external_id.current_external_id_observation_id IS NOT
                  winner.external_id_observation_id
           OR external_id.basis <> winner.basis
           OR external_id.confidence_state <> winner.confidence_state
           OR external_id.source_id IS NOT winner.source_id
        """
    ).fetchone()[0]
    if any(
        (
            mismatched_source_winners,
            mismatched_recording_winners,
            mismatched_relation_winners,
            mismatched_external_id_winners,
        )
    ):
        raise RuntimeError("Current metadata winner points to a different catalog object")
    invalid_public_transcripts = connection.execute(
        """
        SELECT count(*)
        FROM public_transcript_revisions
        WHERE review_state NOT IN (
                  'machine', 'human_corrected', 'media_checked', 'disputed'
              )
           OR verified_quotation <> 0
           OR machine_generated NOT IN (0, 1)
           OR machine_generated <> CASE
                  WHEN review_state = 'machine'
                       OR revision_kind IN ('raw_asr', 'contextual_asr')
                      THEN 1
                  ELSE 0
              END
           OR lifecycle_state = 'retracted'
           OR disclaimer_code NOT IN (
                  'machine_generated_unreviewed_not_verified_quotation_v1',
                  'disputed_transcript_not_verified_quotation_v1',
                  'reviewed_transcript_not_fact_checked_v1'
              )
           OR disclaimer_code <> CASE
                  WHEN lifecycle_state = 'disputed' OR review_state = 'disputed'
                      THEN 'disputed_transcript_not_verified_quotation_v1'
                  WHEN review_state = 'machine'
                      THEN 'machine_generated_unreviewed_not_verified_quotation_v1'
                  ELSE 'reviewed_transcript_not_fact_checked_v1'
              END
        """
    ).fetchone()[0]
    if invalid_public_transcripts:
        raise RuntimeError("Invalid transcript revision entered public view")
    invalid_public_transcript_tombstones = connection.execute(
        """
        SELECT count(*)
        FROM public_transcript_revision_tombstones AS tombstone
        WHERE tombstone.lifecycle_state <> 'retracted'
           OR tombstone.verified_quotation <> 0
           OR tombstone.disclaimer_code
                  <> 'retracted_transcript_text_withdrawn_v1'
           OR tombstone.machine_generated <> CASE
                  WHEN tombstone.review_state = 'machine'
                       OR tombstone.revision_kind IN ('raw_asr', 'contextual_asr')
                      THEN 1
                  ELSE 0
              END
           OR EXISTS (
               SELECT 1
               FROM public_transcript_segments AS segment
               WHERE segment.revision_id = tombstone.revision_id
           )
           OR NOT EXISTS (
               SELECT 1
               FROM reviewers AS reviewer
               JOIN review_decisions AS review
                 ON review.reviewer_id = reviewer.reviewer_id
               JOIN current_transcript_lifecycle_decisions AS lifecycle
                 ON lifecycle.review_decision_id = review.review_decision_id
               WHERE lifecycle.revision_id = tombstone.revision_id
                 AND lifecycle.lifecycle_state = 'retracted'
                 AND reviewer.reviewer_kind = 'human'
                 AND review.target_type = 'transcript_revision'
                 AND review.target_id = tombstone.revision_id
           )
        """
    ).fetchone()[0]
    if invalid_public_transcript_tombstones:
        raise RuntimeError("Invalid transcript retraction tombstone entered public view")
    invalid_lifecycle_transitions = connection.execute(
        """
        WITH ordered AS (
            SELECT revision_id, lifecycle_state,
                   lag(lifecycle_state) OVER (
                       PARTITION BY revision_id
                       ORDER BY lifecycle_decision_sequence
                   ) AS prior_state
            FROM transcript_lifecycle_decisions
        )
        SELECT count(*)
        FROM ordered
        WHERE CASE COALESCE(prior_state, 'active')
                  WHEN 'active' THEN lifecycle_state NOT IN ('disputed', 'retracted')
                  WHEN 'disputed' THEN lifecycle_state NOT IN ('retracted', 'reinstated')
                  WHEN 'retracted' THEN lifecycle_state <> 'reinstated'
                  WHEN 'reinstated' THEN lifecycle_state NOT IN ('disputed', 'retracted')
                  ELSE 1
              END
        """
    ).fetchone()[0]
    if invalid_lifecycle_transitions:
        raise RuntimeError("Transcript lifecycle history contains an illegal transition")
    invalid_lifecycle_reviews = connection.execute(
        """
        SELECT count(*)
        FROM transcript_lifecycle_decisions AS lifecycle
        LEFT JOIN reviewers AS reviewer
          ON reviewer.reviewer_id = lifecycle.reviewer_id
        LEFT JOIN review_decisions AS review
          ON review.review_decision_id = lifecycle.review_decision_id
        WHERE reviewer.reviewer_kind IS NOT 'human'
           OR review.reviewer_id IS NOT lifecycle.reviewer_id
           OR review.target_type IS NOT 'transcript_revision'
           OR review.target_id IS NOT lifecycle.revision_id
           OR julianday(review.decided_at) > julianday(lifecycle.decided_at)
           OR CASE lifecycle.lifecycle_state
                  WHEN 'retracted' THEN review.decision NOT IN (
                      'reject', 'correct', 'dispute'
                  )
                  WHEN 'disputed' THEN review.decision <> 'dispute'
                  WHEN 'reinstated' THEN review.decision NOT IN ('accept', 'correct')
                  ELSE 1
              END
        """
    ).fetchone()[0]
    if invalid_lifecycle_reviews:
        raise RuntimeError("Transcript lifecycle decision lacks its human review record")
    unexplained_current_removals = connection.execute(
        """
        SELECT count(*)
        FROM current_publication_decisions AS publication
        LEFT JOIN current_transcript_lifecycle_decisions AS lifecycle
          ON lifecycle.revision_id = publication.object_id
        WHERE publication.object_type = 'transcript_revision'
          AND publication.decision = 'remove'
          AND COALESCE(lifecycle.lifecycle_state, '') <> 'retracted'
        """
    ).fetchone()[0]
    if unexplained_current_removals:
        raise RuntimeError("Transcript removal lacks a current explained retraction")
    unsafe_public_sources = connection.execute(
        "SELECT count(*) FROM public_sources WHERE access_state <> 'public'"
    ).fetchone()[0]
    if unsafe_public_sources:
        raise RuntimeError("Non-public source entered public view")
    public_objects_missing_gates = connection.execute(
        """
        SELECT count(*)
        FROM (
            SELECT 'source' AS object_type, source_id AS object_id FROM public_sources
            UNION ALL
            SELECT 'recording', recording_id FROM public_recordings
            UNION ALL
            SELECT 'transcript_revision', revision_id FROM public_transcript_revisions
            UNION ALL
            SELECT 'entity', entity_id FROM public_entities
            UNION ALL
            SELECT 'event', event_id FROM public_events
            UNION ALL
            SELECT 'identity_assertion', identity_assertion_id
            FROM public_identity_assertions
        ) AS public_object
        LEFT JOIN publication_eligible_objects AS eligible
          ON eligible.object_type = public_object.object_type
         AND eligible.object_id = public_object.object_id
        WHERE eligible.object_id IS NULL
        """
    ).fetchone()[0]
    if public_objects_missing_gates:
        raise RuntimeError("Object without cleared publication gates entered public view")
    public_recordings_without_sources = connection.execute(
        """
        SELECT count(*)
        FROM public_recordings AS recording
        WHERE NOT EXISTS (
            SELECT 1
            FROM recording_sources AS link
            JOIN public_sources AS source ON source.source_id = link.source_id
            WHERE link.recording_id = recording.recording_id
              AND link.confidence_state <> 'rejected'
        )
        """
    ).fetchone()[0]
    if public_recordings_without_sources:
        raise RuntimeError("Public recording has no eligible public source")
    inconsistent_manifest_ledger = connection.execute(
        """
        SELECT count(*)
        FROM publication_manifest_imports AS manifest
        WHERE manifest.publication_decision_count <> (
                  SELECT count(*) FROM publication_decisions AS decision
                  WHERE decision.manifest_id = manifest.manifest_id
              )
           OR manifest.gate_decision_count <> (
                  SELECT count(*) FROM publication_gate_decisions AS gate_decision
                  WHERE gate_decision.manifest_id = manifest.manifest_id
              )
        """
    ).fetchone()[0]
    if inconsistent_manifest_ledger:
        raise RuntimeError("Publication manifest ledger counts do not match decisions")
    unsafe_biometric_artifacts = connection.execute(
        """
        SELECT count(*)
        FROM artifacts AS artifact
        WHERE (
            EXISTS (
                SELECT 1 FROM biometric_artifacts
                WHERE artifact_id = artifact.artifact_id
            )
            OR EXISTS (
                SELECT 1 FROM identity_clusters
                WHERE artifact_id = artifact.artifact_id
            )
        )
          AND (
              artifact.visibility <> 'private'
              OR lower(artifact.storage_uri) LIKE 'http://%'
              OR lower(artifact.storage_uri) LIKE 'https://%'
              OR lower(replace(artifact.storage_uri, char(92), '/')) GLOB 'src/*'
              OR lower(replace(artifact.storage_uri, char(92), '/')) GLOB 'public/*'
              OR lower(replace(artifact.storage_uri, char(92), '/')) GLOB 'dist/*'
              OR instr(lower(replace(artifact.storage_uri, char(92), '/')), '/src/data/corpus/') > 0
              OR instr(lower(replace(artifact.storage_uri, char(92), '/')), '/public/') > 0
              OR instr(lower(replace(artifact.storage_uri, char(92), '/')), '/dist/') > 0
              OR NOT (
                  lower(replace(artifact.storage_uri, char(92), '/')) GLOB 'research/*'
                  OR instr(lower(replace(artifact.storage_uri, char(92), '/')), '/research/') > 0
                  OR lower(artifact.storage_uri) LIKE 'private:%'
              )
          )
        """
    ).fetchone()[0]
    if unsafe_biometric_artifacts:
        raise RuntimeError("Biometric artifact escaped private non-web storage")
    identity_membership_conflicts = connection.execute(
        """
        SELECT count(*)
        FROM current_identity_cluster_versions AS version
        JOIN identity_cluster_memberships AS left_member
          ON left_member.identity_cluster_version_id = version.identity_cluster_version_id
         AND left_member.membership_state = 'member'
        JOIN identity_cluster_memberships AS right_member
          ON right_member.identity_cluster_version_id = version.identity_cluster_version_id
         AND right_member.membership_state = 'member'
         AND left_member.observation_id < right_member.observation_id
        JOIN current_identity_cannot_link_decisions AS cannot_link
          ON cannot_link.left_observation_id = left_member.observation_id
         AND cannot_link.right_observation_id = right_member.observation_id
         AND cannot_link.decision IN ('cannot_link', 'dispute')
        """
    ).fetchone()[0]
    if identity_membership_conflicts:
        raise RuntimeError("Current identity cluster violates a cannot-link decision")
    raw_identity_publication_intent = connection.execute(
        """
        SELECT count(*)
        FROM current_publication_decisions AS decision
        WHERE decision.decision = 'publish'
          AND (
              decision.object_type IN (
                  'identity_cluster', 'identity_cluster_version',
                  'identity_cluster_membership', 'identity_cannot_link',
                  'biometric_artifact', 'legacy_identity_assertion'
              )
              OR (decision.object_type = 'artifact' AND EXISTS (
                  SELECT 1 FROM biometric_artifacts
                  WHERE artifact_id = decision.object_id
              ))
              OR (decision.object_type = 'artifact' AND EXISTS (
                  SELECT 1 FROM identity_clusters
                  WHERE artifact_id = decision.object_id
              ))
              OR (decision.object_type = 'observation' AND EXISTS (
                  SELECT 1 FROM identity_cluster_memberships
                  WHERE observation_id = decision.object_id
              ))
          )
        """
    ).fetchone()[0]
    if raw_identity_publication_intent:
        raise RuntimeError("Raw biometric identity object has publication intent")
    nonhuman_public_identity_assertions = connection.execute(
        """
        SELECT count(*)
        FROM public_identity_assertions AS assertion
        JOIN current_publication_decisions AS decision
          ON decision.object_type = 'identity_assertion'
         AND decision.object_id = assertion.identity_assertion_id
         AND decision.decision = 'publish'
        JOIN reviewers AS reviewer ON reviewer.reviewer_id = decision.reviewer_id
        WHERE reviewer.reviewer_kind <> 'human' OR reviewer.active <> 1
        """
    ).fetchone()[0]
    if nonhuman_public_identity_assertions:
        raise RuntimeError("Public identity assertion lacks active human publication review")
    invalid_visual_fingerprints = connection.execute(
        """
        SELECT count(*)
        FROM visual_fingerprint_observations AS visual
        LEFT JOIN observations AS observation
          ON observation.observation_id = visual.observation_id
        LEFT JOIN processing_runs AS run
          ON run.processing_run_id = observation.processing_run_id
        LEFT JOIN renditions AS rendition
          ON rendition.rendition_id = observation.rendition_id
        LEFT JOIN fingerprints AS fingerprint
          ON fingerprint.fingerprint_id = visual.fingerprint_id
        LEFT JOIN artifacts AS artifact
          ON artifact.artifact_id = visual.artifact_id
        WHERE observation.observation_id IS NULL
           OR observation.observation_kind <> 'visual_fingerprint'
           OR observation.visibility <> 'private'
           OR observation.review_state <> 'machine'
           OR observation.recording_id <> rendition.recording_id
           OR fingerprint.media_id <> rendition.media_id
           OR fingerprint.fingerprint_kind <> 'fixed_q20_dct_phash_8x8_v1'
           OR fingerprint.start_ms <> observation.start_ms
           OR fingerprint.end_ms <> observation.end_ms
           OR fingerprint.value_text <> visual.phash_hex
           OR fingerprint.artifact_uri <> artifact.storage_uri
           OR artifact.processing_run_id <> observation.processing_run_id
           OR artifact.artifact_kind <> 'visual_fingerprint_gray32'
           OR artifact.sha256 <> visual.exact_gray_sha256
           OR artifact.byte_count <> 1024
           OR artifact.visibility <> 'private'
           OR run.stage <> 'visual_fingerprint_extract'
           OR run.status <> 'completed'
           OR json_type(visual.quality_flags_json) <> 'array'
           OR (SELECT count(*) FROM json_each(visual.quality_flags_json))
              <> (SELECT count(DISTINCT value) FROM json_each(visual.quality_flags_json))
           OR EXISTS (
                SELECT 1 FROM json_each(visual.quality_flags_json) AS flag
                WHERE flag.type <> 'text'
                   OR flag.value NOT IN (
                        'low_visual_variance',
                        'decoded_timestamp_differs_from_request',
                        'requested_keyframe_timestamp_decoded_non_keyframe'
                   )
           )
           OR EXISTS (
                SELECT 1 FROM observation_scores AS score
                WHERE score.observation_id = observation.observation_id
                  AND (score.calibrated_probability IS NOT NULL
                       OR score.calibration_set_id IS NOT NULL)
           )
        """
    ).fetchone()[0]
    invalid_visual_imports = connection.execute(
        """
        SELECT count(*)
        FROM visual_fingerprint_result_imports AS result_import
        LEFT JOIN processing_runs AS run
          ON run.processing_run_id = result_import.processing_run_id
        WHERE run.processing_run_id IS NULL
           OR run.stage <> 'visual_fingerprint_extract'
           OR run.status <> 'completed'
        """
    ).fetchone()[0]
    if invalid_visual_fingerprints or invalid_visual_imports:
        raise RuntimeError("Visual fingerprint evidence escaped private uncalibrated admission semantics")
    invalid_visual_comparisons = connection.execute(
        """
        SELECT count(*)
        FROM visual_fingerprint_comparisons AS comparison
        LEFT JOIN visual_fingerprint_compare_imports AS compare_import
          ON compare_import.comparison_id = comparison.comparison_id
        LEFT JOIN visual_fingerprint_compare_completion_receipts AS completion
          ON completion.import_batch_id = compare_import.import_batch_id
         AND completion.comparison_id = comparison.comparison_id
        LEFT JOIN processing_runs AS run
          ON run.processing_run_id = comparison.processing_run_id
        LEFT JOIN visual_fingerprint_compare_sides AS query_side
          ON query_side.comparison_id = comparison.comparison_id
         AND query_side.role = 'query'
        LEFT JOIN visual_fingerprint_compare_sides AS candidate_side
          ON candidate_side.comparison_id = comparison.comparison_id
         AND candidate_side.role = 'candidate'
        LEFT JOIN match_candidates AS match
          ON match.match_candidate_id = comparison.match_candidate_id
        LEFT JOIN review_tasks AS task
          ON task.review_task_id = comparison.review_task_id
        WHERE compare_import.import_batch_id IS NULL
           OR completion.completion_receipt_id IS NULL
           OR run.processing_run_id IS NULL
           OR run.stage <> 'visual_fingerprint_compare'
           OR run.implementation_version <> '0.1.0'
           OR run.status <> 'completed'
           OR query_side.role IS NULL
           OR candidate_side.role IS NULL
           OR comparison.calibration_state <> 'not_calibrated'
           OR comparison.calibrated_probability IS NOT NULL
           OR comparison.requires_human_review <> 1
           OR comparison.person_identity_asserted <> 0
           OR comparison.duplicate_asserted <> 0
           OR comparison.parent_asserted <> 0
           OR comparison.ownership_asserted <> 0
           OR comparison.relationship_asserted <> 0
           OR comparison.unrelated_asserted <> 0
           OR comparison.visibility <> 'private'
           OR comparison.publication_authority <> 'none'
           OR comparison.pairwise_comparisons <> query_side.frame_count * candidate_side.frame_count
           OR comparison.pairwise_comparisons > comparison.max_pairwise_comparisons
           OR (SELECT count(*) FROM visual_fingerprint_compare_side_frames AS frame
               WHERE frame.comparison_id = comparison.comparison_id
                 AND frame.role = 'query') <> query_side.frame_count
           OR (SELECT count(*) FROM visual_fingerprint_compare_side_frames AS frame
               WHERE frame.comparison_id = comparison.comparison_id
                 AND frame.role = 'candidate') <> candidate_side.frame_count
           OR (SELECT count(*) FROM visual_fingerprint_compare_top_pairs AS pair
               WHERE pair.comparison_id = comparison.comparison_id)
              <> min(comparison.top_k, comparison.pairwise_comparisons)
           OR NOT EXISTS (
               SELECT 1 FROM visual_fingerprint_compare_top_pairs AS best
               WHERE best.comparison_id = comparison.comparison_id
                 AND best.rank = 0
                 AND best.hamming_distance = comparison.best_hamming_distance
                 AND best.normalized_hamming_distance = comparison.best_normalized_hamming_distance
                 AND best.raw_similarity = comparison.best_raw_similarity
           )
           OR comparison.exact_gray_pair_count <> (
               SELECT count(*)
               FROM visual_fingerprint_compare_side_frames AS query_frame
               JOIN visual_fingerprint_compare_side_frames AS candidate_frame
                 ON candidate_frame.comparison_id = query_frame.comparison_id
                AND candidate_frame.role = 'candidate'
               WHERE query_frame.comparison_id = comparison.comparison_id
                 AND query_frame.role = 'query'
                 AND query_frame.exact_gray_sha256 = candidate_frame.exact_gray_sha256
           )
           OR (SELECT count(*) FROM run_inputs AS input
               WHERE input.processing_run_id = comparison.processing_run_id
                 AND input.object_type = 'visual_fingerprint_extraction_result'
                 AND input.input_role IN ('query_result', 'candidate_result')) <> 2
           OR (
               comparison.candidate_emitted = 0
               AND (
                   comparison.threshold_state <> 'does_not_meet_configured_threshold'
                   OR comparison.decision_state <> 'below_configured_threshold'
                   OR comparison.match_candidate_id IS NOT NULL
                   OR comparison.review_task_id IS NOT NULL
               )
           )
           OR (
               comparison.candidate_emitted = 1
               AND (
                   comparison.threshold_state <> 'meets_configured_threshold'
                   OR comparison.decision_state <> 'candidate_for_human_review'
                   OR match.match_candidate_id IS NULL
                   OR match.match_method <> 'visual_phash_minimum_hamming_v1'
                   OR match.raw_score <> comparison.best_raw_similarity
                   OR match.calibrated_probability IS NOT NULL
                   OR match.decision_state <> 'candidate'
                   OR task.review_task_id IS NULL
                   OR task.task_kind <> 'visual_fingerprint_comparison_review'
                   OR task.target_type <> 'visual_fingerprint_comparison'
                   OR task.target_id <> comparison.comparison_id
               )
           )
        """
    ).fetchone()[0]
    orphan_visual_comparison_rows = connection.execute(
        """
        SELECT
            (SELECT count(*)
             FROM visual_fingerprint_compare_imports AS compare_import
             LEFT JOIN visual_fingerprint_compare_completion_receipts AS completion
               ON completion.import_batch_id = compare_import.import_batch_id
             WHERE completion.completion_receipt_id IS NULL)
          + (SELECT count(*)
             FROM match_candidates AS match
             LEFT JOIN visual_fingerprint_comparisons AS comparison
               ON comparison.match_candidate_id = match.match_candidate_id
             WHERE match.match_method = 'visual_phash_minimum_hamming_v1'
               AND comparison.comparison_id IS NULL)
          + (SELECT count(*)
             FROM review_tasks AS task
             LEFT JOIN visual_fingerprint_comparisons AS comparison
               ON comparison.review_task_id = task.review_task_id
             WHERE task.task_kind = 'visual_fingerprint_comparison_review'
               AND comparison.comparison_id IS NULL)
        """
    ).fetchone()[0]
    visual_comparison_publication_intent = connection.execute(
        """
        SELECT
            (SELECT count(*) FROM publication_decisions AS decision
             WHERE (decision.object_type = 'visual_fingerprint_comparison'
                    AND EXISTS (
                        SELECT 1 FROM visual_fingerprint_comparisons
                        WHERE comparison_id = decision.object_id
                    ))
                OR (decision.object_type = 'match_candidate'
                    AND EXISTS (
                        SELECT 1 FROM visual_fingerprint_comparisons
                        WHERE match_candidate_id = decision.object_id
                    )))
          + (SELECT count(*) FROM publication_gate_decisions AS gate
             WHERE (gate.object_type = 'visual_fingerprint_comparison'
                    AND EXISTS (
                        SELECT 1 FROM visual_fingerprint_comparisons
                        WHERE comparison_id = gate.object_id
                    ))
                OR (gate.object_type = 'match_candidate'
                    AND EXISTS (
                        SELECT 1 FROM visual_fingerprint_comparisons
                        WHERE match_candidate_id = gate.object_id
                    )))
        """
    ).fetchone()[0]
    if (
        invalid_visual_comparisons
        or orphan_visual_comparison_rows
        or visual_comparison_publication_intent
    ):
        raise RuntimeError(
            "Visual comparison evidence violates private measurement/candidate semantics"
        )
    _validate_visual_fingerprint_compare_files(connection)
    invalid_archive_bracket_candidates = connection.execute(
        """
        SELECT count(*)
        FROM archive_bracket_youtube_candidates AS candidate
        LEFT JOIN match_candidates AS match
          ON match.match_candidate_id = candidate.match_candidate_id
        LEFT JOIN archive_bracket_reconciliation_imports AS receipt
          ON receipt.import_batch_id = candidate.import_batch_id
        LEFT JOIN review_tasks AS task
          ON task.review_task_id = candidate.review_task_id
        WHERE match.match_candidate_id IS NULL
           OR receipt.import_batch_id IS NULL
           OR task.review_task_id IS NULL
           OR task.task_kind <> 'archive_bracket_reconciliation_candidate'
           OR task.target_type <> 'match_candidate'
           OR task.target_id <> candidate.match_candidate_id
           OR match.raw_score IS NOT NULL
           OR match.calibrated_probability IS NOT NULL
           OR match.decision_state <> 'candidate'
           OR candidate.requires_human_review <> 1
           OR candidate.relationship_asserted <> 0
           OR candidate.merge_performed <> 0
        """
    ).fetchone()[0]
    invalid_archive_bracket_issues = connection.execute(
        """
        SELECT count(*)
        FROM archive_bracket_reconciliation_issues AS issue
        LEFT JOIN archive_bracket_reconciliation_imports AS receipt
          ON receipt.import_batch_id = issue.import_batch_id
        LEFT JOIN review_tasks AS task ON task.review_task_id = issue.review_task_id
        WHERE receipt.import_batch_id IS NULL
           OR task.review_task_id IS NULL
           OR task.task_kind <> 'archive_bracket_reconciliation_conflict'
           OR task.target_type <> 'archive_bracket_reconciliation_issue'
           OR task.target_id <> issue.issue_id
           OR issue.requires_human_review <> 1
           OR issue.relationship_asserted <> 0
           OR issue.merge_performed <> 0
           OR issue.filename_youtube_video_id = issue.title_youtube_video_id
        """
    ).fetchone()[0]
    if invalid_archive_bracket_candidates or invalid_archive_bracket_issues:
        raise RuntimeError(
            "Archive bracket reconciliation evidence escaped review-only no-merge semantics"
        )
    invalid_torrent_bracket_candidates = connection.execute(
        """
        SELECT count(*)
        FROM torrent_bracket_youtube_candidates AS candidate
        LEFT JOIN match_candidates AS match
          ON match.match_candidate_id = candidate.match_candidate_id
        LEFT JOIN torrent_bracket_reconciliation_imports AS receipt
          ON receipt.import_batch_id = candidate.import_batch_id
        LEFT JOIN import_batches AS torrent_import
          ON torrent_import.import_batch_id = receipt.torrent_import_batch_id
        LEFT JOIN review_tasks AS task
          ON task.review_task_id = candidate.review_task_id
        LEFT JOIN sources AS manifest
          ON manifest.source_id = candidate.torrent_manifest_source_id
        LEFT JOIN sources AS file_source
          ON file_source.source_id = candidate.torrent_file_source_id
        LEFT JOIN source_metadata_observations AS origin
          ON origin.source_id = candidate.torrent_file_source_id
         AND origin.import_batch_id = receipt.torrent_import_batch_id
        WHERE match.match_candidate_id IS NULL
           OR receipt.import_batch_id IS NULL
           OR torrent_import.import_batch_id IS NULL
           OR torrent_import.importer_name <> 'torrent_manifest_metadata'
           OR torrent_import.status <> 'completed'
           OR task.review_task_id IS NULL
           OR manifest.source_id IS NULL
           OR file_source.source_id IS NULL
           OR task.task_kind <> 'torrent_bracket_reconciliation_candidate'
           OR task.target_type <> 'match_candidate'
           OR task.target_id <> candidate.match_candidate_id
           OR match.left_object_type <> 'source'
           OR match.left_object_id <> candidate.torrent_file_source_id
           OR match.match_method <> 'torrent_terminal_bracket_youtube_locator_v1'
           OR match.raw_score IS NOT NULL
           OR match.calibrated_probability IS NOT NULL
           OR match.decision_state <> 'candidate'
           OR candidate.requires_human_review <> 1
           OR candidate.relationship_asserted <> 0
           OR candidate.merge_performed <> 0
           OR candidate.visibility <> 'private'
           OR candidate.publication_authority <> 'none'
           OR manifest.platform <> 'bittorrent'
           OR manifest.source_kind <> 'torrent_manifest'
           OR manifest.native_id <> receipt.info_hash_sha1
           OR file_source.platform <> 'bittorrent'
           OR file_source.source_kind <> 'torrent_file_candidate'
           OR file_source.parent_source_id <> manifest.source_id
           OR file_source.created_by_import_batch_id <> torrent_import.import_batch_id
           OR file_source.native_id <> receipt.info_hash_sha1 || '/' || candidate.manifest_path
           OR origin.source_id IS NULL
           OR origin.parent_source_id <> manifest.source_id
           OR origin.observed_at <> receipt.observed_at
           OR origin.quality_rank <> 250
           OR origin.quality_basis <> 'torrent_manifest_metadata: locally parsed discovery manifest'
           OR origin.access_state <> 'unknown'
           OR origin.review_state <> 'unreviewed'
           OR json_extract(origin.metadata_json, '$.manifest_path') <> candidate.manifest_path
           OR json_extract(origin.metadata_json, '$.byte_count') <> candidate.byte_count
           OR json_type(origin.metadata_json, '$.payload_downloaded') <> 'false'
           OR json_type(candidate.evidence_json, '$.requires_human_review') <> 'true'
           OR json_type(candidate.evidence_json, '$.relationship_asserted') <> 'false'
           OR json_type(candidate.evidence_json, '$.merge_performed') <> 'false'
           OR json_type(candidate.evidence_json, '$.publication_authority') <> 'false'
           OR json_type(candidate.evidence_json, '$.payload_downloaded_or_read') <> 'false'
           OR EXISTS (
                SELECT 1 FROM publication_decisions AS publication
                WHERE publication.object_type = 'match_candidate'
                  AND publication.object_id = candidate.match_candidate_id
           )
        """
    ).fetchone()[0]
    invalid_torrent_bracket_receipts = connection.execute(
        """
        SELECT count(*)
        FROM torrent_bracket_reconciliation_imports AS receipt
        LEFT JOIN import_batches AS candidate_import
          ON candidate_import.import_batch_id = receipt.import_batch_id
        LEFT JOIN import_batches AS torrent_import
          ON torrent_import.import_batch_id = receipt.torrent_import_batch_id
        LEFT JOIN sources AS manifest
          ON manifest.source_id = receipt.torrent_manifest_source_id
        LEFT JOIN source_metadata_observations AS origin
          ON origin.source_id = receipt.torrent_manifest_source_id
         AND origin.import_batch_id = receipt.torrent_import_batch_id
        WHERE candidate_import.import_batch_id IS NULL
           OR torrent_import.import_batch_id IS NULL
           OR manifest.source_id IS NULL
           OR candidate_import.importer_name <> 'torrent_bracket_reconciliation_v1'
           OR candidate_import.input_sha256 <> receipt.plan_sha256
           OR candidate_import.status <> 'completed'
           OR candidate_import.source_snapshot_date <> substr(receipt.observed_at, 1, 10)
           OR candidate_import.started_at <> receipt.observed_at
           OR candidate_import.completed_at <> receipt.observed_at
           OR candidate_import.statistics_json <> receipt.statistics_json
           OR NOT EXISTS (
                  SELECT 1
                  FROM import_observations AS observation
                  WHERE observation.import_batch_id = candidate_import.import_batch_id
                    AND observation.importer_version = candidate_import.importer_version
                    AND observation.source_snapshot_date = candidate_import.source_snapshot_date
                    AND observation.observed_at = receipt.observed_at
                    AND observation.status = 'completed'
                    AND observation.completed_at = receipt.observed_at
                    AND observation.statistics_json = receipt.statistics_json
              )
           OR torrent_import.importer_name <> 'torrent_manifest_metadata'
           OR torrent_import.input_sha256 <> receipt.combined_import_input_sha256
           OR torrent_import.status <> 'completed'
           OR manifest.platform <> 'bittorrent'
           OR manifest.source_kind <> 'torrent_manifest'
           OR manifest.native_id <> receipt.info_hash_sha1
           OR origin.source_id IS NULL
           OR json_extract(origin.metadata_json, '$.torrent_sha256') <> receipt.torrent_sha256
           OR json_type(origin.metadata_json, '$.payload_downloaded') <> 'false'
           OR receipt.candidate_count <> (
                  SELECT count(*) FROM torrent_bracket_youtube_candidates AS candidate
                  WHERE candidate.import_batch_id = receipt.import_batch_id
              )
           OR receipt.review_task_count <> receipt.candidate_count
           OR json_extract(receipt.statistics_json, '$.payload_files_read') <> 0
           OR json_extract(receipt.statistics_json, '$.payload_bytes_read') <> 0
           OR json_extract(receipt.statistics_json, '$.source_or_recording_mutations') <> 0
           OR json_extract(receipt.statistics_json, '$.source_relations') <> 0
           OR json_extract(receipt.statistics_json, '$.recording_relations') <> 0
           OR json_extract(receipt.statistics_json, '$.recording_merges') <> 0
           OR json_extract(receipt.statistics_json, '$.publication_decisions') <> 0
           OR json_extract(receipt.statistics_json, '$.identity_assertions') <> 0
           OR json_extract(receipt.statistics_json, '$.claims') <> 0
        """
    ).fetchone()[0]
    if invalid_torrent_bracket_candidates or invalid_torrent_bracket_receipts:
        raise RuntimeError(
            "Torrent bracket reconciliation evidence escaped private candidate-only semantics"
        )
    invalid_rendition_local_transcripts = connection.execute(
        """
        SELECT count(*)
        FROM rendition_local_transcript_revisions AS revision
        LEFT JOIN rendition_local_asr_imports AS receipt
          ON receipt.local_revision_id = revision.local_revision_id
        LEFT JOIN import_batches AS batch
          ON batch.import_batch_id = receipt.import_batch_id
        LEFT JOIN renditions AS rendition
          ON rendition.rendition_id = revision.rendition_id
        LEFT JOIN media_objects AS media ON media.media_id = rendition.media_id
        LEFT JOIN timeline_map_spans AS span
          ON span.rendition_id = revision.rendition_id
        WHERE rendition.rendition_id IS NULL
           OR media.media_id IS NULL
           OR span.rendition_id IS NULL
           OR receipt.local_revision_id IS NULL
           OR batch.import_batch_id IS NULL
           OR batch.importer_name <> 'rendition_local_asr_result_v1'
           OR batch.importer_version <> 'rendition-local-asr-bridge/1'
           OR batch.status <> 'completed'
           OR batch.input_sha256 <> receipt.asr_result_canonical_sha256
           OR rendition.recording_id <> revision.recording_id
           OR media.duration_ms <> revision.input_duration_ms
           OR revision.coordinate_system <> 'rendition_media_ms'
           OR revision.boundary <> 'half_open'
           OR revision.source_mapping_state <>
              'integer_contract_not_boundary_calibrated'
           OR revision.recording_transform_state <> 'unresolved'
           OR revision.review_state <> 'machine'
           OR span.ordinal <> 0
           OR span.media_start_ms <> 0
           OR span.media_end_ms <> revision.input_duration_ms
           OR span.recording_start_ms IS NOT NULL
           OR span.recording_end_ms IS NOT NULL
           OR span.mapping_kind <> 'unknown'
           OR span.confidence_state <> 'metadata_only'
           OR EXISTS (
                SELECT 1 FROM transcript_revisions AS recording_revision
                WHERE recording_revision.processing_run_id = revision.processing_run_id
           )
           OR EXISTS (
                SELECT 1
                FROM rendition_local_transcript_segments AS segment
                WHERE segment.local_revision_id = revision.local_revision_id
                  AND (
                       segment.source_start_ms - segment.start_ms <>
                           receipt.source_start_ms
                       OR segment.source_end_ms - segment.end_ms <>
                           receipt.source_start_ms
                       OR segment.source_boundary_overrun_ms <>
                           max(0, segment.source_end_ms - receipt.source_end_ms)
                  )
           )
        """
    ).fetchone()[0]
    invalid_rendition_local_fts = connection.execute(
        """
        SELECT
            abs((SELECT count(*) FROM rendition_local_transcript_segments)
          - (SELECT count(*) FROM rendition_local_transcript_fts))
          + (SELECT count(*)
             FROM rendition_local_transcript_segments AS segment
             LEFT JOIN rendition_local_transcript_fts AS search
               ON search.local_segment_id = segment.local_segment_id
              AND search.text = segment.text
             WHERE search.local_segment_id IS NULL)
        """
    ).fetchone()[0]
    invalid_transform_candidates = connection.execute(
        """
        SELECT count(*)
        FROM source_recording_transform_candidates AS candidate
        LEFT JOIN review_tasks AS task ON task.review_task_id = candidate.review_task_id
        WHERE task.review_task_id IS NULL
           OR task.task_kind <> 'source_recording_transform_review'
           OR task.target_type <> 'source_recording_transform_candidate'
           OR task.target_id <> candidate.transform_candidate_id
           OR candidate.evidence_state <> 'hypothesis_only_unreviewed'
           OR candidate.timeline_application_allowed <> 0
           OR candidate.relationship_asserted <> 0
           OR EXISTS (
                SELECT 1 FROM publication_decisions AS publication
                WHERE publication.object_type IN (
                    'rendition_local_transcript_revision',
                    'rendition_local_transcript_segment',
                    'rendition_local_transcript_word',
                    'rendition_local_asr_import',
                    'source_recording_transform_candidate'
                )
           )
        """
    ).fetchone()[0]
    if (
        invalid_rendition_local_transcripts
        or invalid_rendition_local_fts
        or invalid_transform_candidates
    ):
        raise RuntimeError(
            "Rendition-local ASR escaped private unresolved-coordinate semantics"
        )
    gpu_v3_table = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'private_gpu_v3_asr_imports'
        """
    ).fetchone()
    gpu_v3_exclusion = (
        "AND NOT EXISTS ("
        "SELECT 1 FROM private_gpu_v3_asr_imports AS gpu_v3 "
        "WHERE gpu_v3.media_local_revision_id = revision.media_local_revision_id)"
        if gpu_v3_table is not None
        else ""
    )
    invalid_media_local_transcripts = connection.execute(
        f"""
        SELECT count(*)
        FROM media_local_transcript_revisions AS revision
        LEFT JOIN media_local_asr_imports AS receipt
          ON receipt.media_local_revision_id = revision.media_local_revision_id
        LEFT JOIN import_batches AS batch
          ON batch.import_batch_id = receipt.import_batch_id
        LEFT JOIN import_batches AS preprocess_batch
          ON preprocess_batch.import_batch_id = receipt.preprocess_import_batch_id
        LEFT JOIN media_objects AS media ON media.media_id = revision.media_id
        LEFT JOIN artifacts AS input_artifact
          ON input_artifact.artifact_id = revision.input_artifact_id
        LEFT JOIN processing_runs AS run
          ON run.processing_run_id = revision.processing_run_id
        WHERE revision.revision_kind = 'raw_asr'
          {gpu_v3_exclusion}
          AND (
              receipt.media_local_revision_id IS NULL
           OR batch.import_batch_id IS NULL
           OR batch.importer_name <> 'media_local_asr_result_v1'
           OR batch.importer_version <> 'media-local-asr-bridge/1'
           OR batch.status <> 'completed'
           OR batch.input_sha256 <> receipt.asr_result_canonical_sha256
           OR preprocess_batch.import_batch_id IS NULL
           OR preprocess_batch.importer_name <> 'media_preprocess_result_v1'
           OR preprocess_batch.status <> 'completed'
           OR media.media_id IS NULL
           OR media.media_id <> receipt.input_media_id
           OR media.duration_ms <> revision.input_duration_ms
           OR input_artifact.artifact_id IS NULL
           OR input_artifact.artifact_id <> receipt.input_artifact_id
           OR input_artifact.processing_run_id = revision.processing_run_id
           OR input_artifact.visibility <> 'private'
           OR run.processing_run_id IS NULL
           OR run.stage <> 'asr_whispercpp'
           OR run.status <> 'completed'
           OR revision.coordinate_system <> 'media_ms'
           OR revision.boundary <> 'half_open'
           OR revision.requested_start_ms <> 0
           OR revision.requested_end_ms <> revision.input_duration_ms
           OR revision.source_coordinate_state <>
              'unasserted_catalog_context_null'
           OR revision.recording_coordinate_state <>
              'unasserted_catalog_context_null'
           OR revision.review_state <> 'machine'
           OR receipt.queue_id <>
              'asrppqueue_66349d9b85c74f2376830edf2a7d4f0c'
           OR receipt.queue_identity_sha256 <>
              '66349d9b85c74f2376830edf2a7d4f0ccf9d4e093f9c55b8127429259f2948d1'
           OR receipt.queue_manifest_raw_sha256 <>
              '100d142cf459a663dd0f899db74b9666fbe49942d7ff0960280424899d39b5a0'
           OR receipt.routing_hint <> 'process'
           OR receipt.queue_ordinal IN (1, 13)
           OR receipt.seal_receipt_id <>
              'asrsealreceipt_c0694c5c36586eb433a766490f3dbc01'
           OR receipt.seal_receipt_raw_sha256 <>
              '806439f2736dae9b95ffdffd19c3464c9efc39e79a5fbc21f7223b9f2c717b7b'
           OR receipt.seal_receipt_identity_sha256 <>
              '9f29212c38a78ff91faaea5dc7d8eb10f3d0405c0075ce8e365d4b33598df524'
           OR receipt.input_duration_ms <> revision.input_duration_ms
           OR receipt.max_segment_end_ms <> revision.max_segment_end_ms
           OR receipt.input_boundary_overrun_ms <>
              revision.input_boundary_overrun_ms
           OR revision.max_segment_end_ms <> coalesce((
                SELECT max(segment.media_end_ms)
                FROM media_local_transcript_segments AS segment
                WHERE segment.media_local_revision_id =
                      revision.media_local_revision_id
              ), 0)
           OR receipt.null_timed_word_count <> (
                SELECT count(*)
                FROM media_local_transcript_words AS word
                JOIN media_local_transcript_segments AS segment
                  ON segment.media_local_segment_id = word.media_local_segment_id
                WHERE segment.media_local_revision_id =
                      revision.media_local_revision_id
                  AND word.media_start_ms IS NULL
              )
           OR NOT EXISTS (
                SELECT 1 FROM run_inputs AS input
                WHERE input.processing_run_id = revision.processing_run_id
                  AND input.object_type = 'media'
                  AND input.object_id = revision.media_id
                  AND input.input_role = 'normalized_audio'
                  AND input.input_sha256 = media.sha256
              )
           OR EXISTS (
                SELECT 1 FROM transcript_revisions AS recording_revision
                WHERE recording_revision.processing_run_id = revision.processing_run_id
              )
           OR EXISTS (
                SELECT 1 FROM rendition_local_transcript_revisions AS local_revision
                WHERE local_revision.processing_run_id = revision.processing_run_id
              )
           OR EXISTS (
                SELECT 1
                FROM media_local_transcript_segments AS segment
                WHERE segment.media_local_revision_id = revision.media_local_revision_id
                  AND segment.input_boundary_overrun_ms <>
                      max(0, segment.media_end_ms - revision.requested_end_ms)
              )
          )
        """
    ).fetchone()[0]
    invalid_contextual_media_local = connection.execute(
        """
        SELECT count(*)
        FROM media_local_transcript_revisions AS contextual
        LEFT JOIN contextual_media_local_asr_imports AS receipt
          ON receipt.media_local_revision_id = contextual.media_local_revision_id
        LEFT JOIN import_batches AS import_batch
          ON import_batch.import_batch_id = receipt.import_batch_id
        LEFT JOIN contextual_media_local_asr_pairs AS pair
          ON pair.contextual_asr_import_id = receipt.contextual_asr_import_id
         AND pair.contextual_media_local_revision_id = contextual.media_local_revision_id
        LEFT JOIN media_local_transcript_revisions AS baseline
          ON baseline.media_local_revision_id = pair.baseline_media_local_revision_id
        LEFT JOIN media_local_asr_imports AS baseline_receipt
          ON baseline_receipt.media_local_revision_id = baseline.media_local_revision_id
        LEFT JOIN contextual_asr_text_private_diffs AS diff
          ON diff.contextual_pair_id = pair.contextual_pair_id
        LEFT JOIN contextual_asr_batch_registrations AS contextual_batch
          ON contextual_batch.contextual_batch_id = receipt.contextual_batch_id
        LEFT JOIN private_glossary_registrations AS glossary
          ON glossary.glossary_revision_id = contextual.glossary_revision_id
        LEFT JOIN glossary_revisions AS glossary_registry
          ON glossary_registry.glossary_revision_id = glossary.glossary_revision_id
        LEFT JOIN processing_runs AS run
          ON run.processing_run_id = contextual.processing_run_id
        LEFT JOIN media_objects AS media ON media.media_id = contextual.media_id
        LEFT JOIN artifacts AS input_artifact
          ON input_artifact.artifact_id = contextual.input_artifact_id
        WHERE contextual.revision_kind = 'contextual_asr'
          AND (
              receipt.contextual_asr_import_id IS NULL
           OR import_batch.import_batch_id IS NULL
           OR import_batch.importer_name <> 'contextual_media_local_asr_result_v1'
           OR import_batch.importer_version <> 'contextual-media-local-asr-bridge/2'
           OR import_batch.status <> 'completed'
           OR import_batch.input_sha256 <> receipt.result_canonical_sha256
           OR pair.contextual_pair_id IS NULL
           OR baseline.media_local_revision_id IS NULL
           OR baseline.revision_kind <> 'raw_asr'
           OR baseline.glossary_revision_id IS NOT NULL
           OR baseline.media_id <> contextual.media_id
           OR baseline.input_artifact_id <> contextual.input_artifact_id
           OR baseline.input_duration_ms <> contextual.input_duration_ms
           OR baseline_receipt.media_local_asr_import_id IS NULL
           OR diff.contextual_diff_id IS NULL
           OR contextual_batch.contextual_batch_id IS NULL
           OR contextual_batch.contextual_batch_id <>
              'ctxasrbatch_b696c14db13d7ae51a207e528c5e1d0e'
           OR contextual_batch.identity_sha256 <>
              'b696c14db13d7ae51a207e528c5e1d0eebb9161e1e12ecd1dc017d93c1b47de7'
           OR contextual_batch.manifest_raw_sha256 <>
              '6742125ffb752778ca082c491f26d1c596c36e566f637ed9b9e8452d3708aa6f'
           OR contextual_batch.manifest_canonical_sha256 <>
              '743b1a2dc3e0a10efc100c456f3f7613d68470266803df19437657aea6eee13c'
           OR contextual_batch.work_order_count <> 17
           OR glossary.private_glossary_registration_id IS NULL
           OR glossary.raw_sha256 <>
              '221543ce0a6ef220158d90c00bff95ec8d18aa911b11a40a3ac81e56e2a9b240'
           OR glossary.canonical_sha256 <>
              '6bc1d768581d399c2ba846aba5454291b67ccea1c29efb452a4740ff3c1ab374'
           OR glossary.prompt_sha256 <>
              '4ecd3ddb7e6546d145b260ab42ba64e003224e4b0e70d5d6847179fb01f29f96'
           OR glossary.term_count <> 26
           OR glossary.terms_stored_in_catalog <> 0
           OR glossary.review_state <> 'machine_candidate_unreviewed'
           OR glossary_registry.glossary_revision_id IS NULL
           OR glossary_registry.sha256 <> glossary.raw_sha256
           OR glossary_registry.artifact_uri <> glossary.artifact_uri
           OR contextual.glossary_revision_id <>
              'glossary_himrverse_neutral_en_20260827_v1'
           OR contextual.review_state <> 'machine'
           OR contextual.coordinate_system <> 'media_ms'
           OR contextual.boundary <> 'half_open'
           OR contextual.requested_start_ms <> 0
           OR contextual.requested_end_ms <> contextual.input_duration_ms
           OR contextual.source_coordinate_state <>
              'unasserted_catalog_context_null'
           OR contextual.recording_coordinate_state <>
              'unasserted_catalog_context_null'
           OR run.processing_run_id IS NULL
           OR run.stage <> 'asr_whispercpp'
           OR run.status <> 'completed'
           OR run.glossary_revision_id <> contextual.glossary_revision_id
           OR media.media_id IS NULL
           OR media.duration_ms <> contextual.input_duration_ms
           OR input_artifact.artifact_id IS NULL
           OR input_artifact.visibility <> 'private'
           OR receipt.input_media_id <> contextual.media_id
           OR receipt.input_artifact_id <> contextual.input_artifact_id
           OR receipt.input_duration_ms <> contextual.input_duration_ms
           OR receipt.max_segment_end_ms <> contextual.max_segment_end_ms
           OR receipt.input_boundary_overrun_ms <>
              contextual.input_boundary_overrun_ms
           OR receipt.result_filesystem_state <>
              'stable_hash_bound_no_seal_claim'
           OR contextual.max_segment_end_ms <> coalesce((
                SELECT max(segment.media_end_ms)
                FROM media_local_transcript_segments AS segment
                WHERE segment.media_local_revision_id = contextual.media_local_revision_id
              ), 0)
           OR receipt.null_timed_word_count <> (
                SELECT count(*)
                FROM media_local_transcript_words AS word
                JOIN media_local_transcript_segments AS segment
                  ON segment.media_local_segment_id = word.media_local_segment_id
                WHERE segment.media_local_revision_id = contextual.media_local_revision_id
                  AND word.media_start_ms IS NULL
              )
           OR pair.pair_state <> 'competing_machine_revisions_no_preference'
           OR pair.pair_projection_sha256 <> receipt.pair_projection_sha256
           OR pair.input_equal <> 1 OR pair.engine_equal <> 1
           OR pair.model_equal <> 1 OR pair.window_equal <> 1
           OR pair.inference_equal <> 1 OR pair.catalog_context_equal <> 1
           OR pair.only_glossary_job_output_differ <> 1
           OR pair.preferred_revision_id IS NOT NULL
           OR pair.correction_asserted <> 0 OR pair.accuracy_claimed <> 0
           OR pair.improvement_claimed <> 0 OR pair.human_review_claimed <> 0
           OR pair.automatic_merge_allowed <> 0
           OR pair.publication_authority <> 'none'
           OR diff.transcript_text_stored <> 0
           OR diff.decoder_scores_calibrated <> 0
           OR diff.accuracy_claimed <> 0 OR diff.improvement_claimed <> 0
           OR diff.preferred_revision_selected <> 0
           OR diff.human_review_claimed <> 0 OR diff.automatic_merge_allowed <> 0
           OR diff.visibility <> 'private' OR diff.publication_authority <> 'none'
           OR diff.total_blocks <> diff.changed_blocks + diff.unchanged_blocks
           OR EXISTS (
                SELECT 1 FROM media_local_asr_imports AS raw_receipt
                WHERE raw_receipt.media_local_revision_id = contextual.media_local_revision_id
              )
           OR EXISTS (
                SELECT 1 FROM transcript_revisions AS recording_revision
                WHERE recording_revision.processing_run_id = contextual.processing_run_id
              )
           OR EXISTS (
                SELECT 1 FROM rendition_local_transcript_revisions AS local_revision
                WHERE local_revision.processing_run_id = contextual.processing_run_id
              )
          )
        """
    ).fetchone()[0]
    invalid_media_local_fts = connection.execute(
        """
        SELECT
            abs((SELECT count(*) FROM media_local_transcript_segments)
              - (SELECT count(*) FROM media_local_transcript_fts))
          + (SELECT count(*)
             FROM media_local_transcript_segments AS segment
             LEFT JOIN media_local_transcript_fts AS search
               ON search.media_local_segment_id = segment.media_local_segment_id
              AND search.text = segment.text
             WHERE search.media_local_segment_id IS NULL)
        """
    ).fetchone()[0]
    invalid_media_local_publication = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM publication_decisions
           WHERE object_type IN (
             'media_local_transcript_revision',
             'media_local_transcript_segment',
             'media_local_transcript_word',
             'media_local_asr_import',
             'private_gpu_v3_asr_import',
             'private_glossary_registration',
             'contextual_asr_batch_registration',
             'contextual_media_local_asr_import',
             'contextual_media_local_asr_pair',
             'contextual_asr_text_private_diff'
           ))
          +
          (SELECT count(*) FROM publication_gate_decisions
           WHERE object_type IN (
             'media_local_transcript_revision',
             'media_local_transcript_segment',
             'media_local_transcript_word',
             'media_local_asr_import',
             'private_gpu_v3_asr_import',
             'private_glossary_registration',
             'contextual_asr_batch_registration',
             'contextual_media_local_asr_import',
             'contextual_media_local_asr_pair',
             'contextual_asr_text_private_diff'
           ))
        """
    ).fetchone()[0]
    if (
        invalid_media_local_transcripts
        or invalid_contextual_media_local
        or invalid_media_local_fts
        or invalid_media_local_publication
    ):
        raise RuntimeError(
            "Media-local ASR escaped private unasserted-coordinate semantics"
        )

    if gpu_v3_table is not None:
        invalid_gpu_v3 = connection.execute(
            """
            SELECT count(*)
            FROM private_gpu_v3_asr_imports AS receipt
            LEFT JOIN media_local_transcript_revisions AS revision
              ON revision.media_local_revision_id = receipt.media_local_revision_id
            LEFT JOIN processing_runs AS run
              ON run.processing_run_id = receipt.processing_run_id
            LEFT JOIN artifacts AS raw_artifact
              ON raw_artifact.artifact_id = receipt.raw_transcript_artifact_id
            LEFT JOIN artifacts AS normalized_artifact
              ON normalized_artifact.artifact_id =
                 receipt.normalized_transcript_artifact_id
            WHERE revision.media_local_revision_id IS NULL
               OR revision.processing_run_id <> receipt.processing_run_id
               OR revision.media_id <> receipt.input_media_id
               OR revision.input_artifact_id <> receipt.input_artifact_id
               OR revision.review_state <> 'machine'
               OR revision.coordinate_system <> 'media_ms'
               OR revision.recording_coordinate_state <>
                  'unasserted_catalog_context_null'
               OR revision.source_coordinate_state <>
                  'unasserted_catalog_context_null'
               OR run.stage <> 'asr_faster_whisper_gpu'
               OR run.status <> 'completed'
               OR raw_artifact.processing_run_id <> receipt.processing_run_id
               OR raw_artifact.artifact_kind <>
                  'faster_whisper_raw_transcript_json'
               OR raw_artifact.visibility <> 'private'
               OR normalized_artifact.processing_run_id <>
                  receipt.processing_run_id
               OR normalized_artifact.artifact_kind <>
                  'transcript_normalized_json'
               OR normalized_artifact.visibility <> 'private'
               OR EXISTS (
                    SELECT 1
                    FROM media_local_transcript_segments AS segment
                    WHERE segment.media_local_revision_id =
                          receipt.media_local_revision_id
                      AND (segment.speaker_label IS NOT NULL
                           OR segment.confidence_band IS NOT NULL
                           OR segment.calibrated_probability IS NOT NULL)
                  )
               OR EXISTS (
                    SELECT 1
                    FROM media_local_transcript_words AS word
                    JOIN media_local_transcript_segments AS segment
                      ON segment.media_local_segment_id = word.media_local_segment_id
                    WHERE segment.media_local_revision_id =
                          receipt.media_local_revision_id
                      AND (word.alignment_score IS NOT NULL
                           OR word.calibrated_probability IS NOT NULL)
                  )
               OR (SELECT count(*) FROM media_local_transcript_segments AS segment
                   WHERE segment.media_local_revision_id =
                         receipt.media_local_revision_id) <> receipt.segment_count
               OR (SELECT count(*)
                   FROM media_local_transcript_words AS word
                   JOIN media_local_transcript_segments AS segment
                     ON segment.media_local_segment_id = word.media_local_segment_id
                   WHERE segment.media_local_revision_id =
                         receipt.media_local_revision_id) <> receipt.word_count
            """
        ).fetchone()[0]
        if invalid_gpu_v3:
            raise RuntimeError(
                "GPU v3 ASR escaped private uncalibrated media-local semantics"
            )

    # Historical migration tests intentionally validate catalogs whose local
    # migration manifest ends before 0029. Current catalogs always have this table
    # because verify_migrations rejects a pending current schema.
    if connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table'
          AND name = 'media_local_transcript_projection_batches'
        """
    ).fetchone() is None:
        return _database_status(connection)

    for batch in connection.execute(
        "SELECT projection_batch_id FROM media_local_transcript_projection_batches "
        "ORDER BY projection_batch_id"
    ):
        try:
            require_exact_projection_batch(
                connection, batch["projection_batch_id"]
            )
        except MediaLocalTranscriptProjectionError as error:
            raise RuntimeError(
                "Media-local transcript projection escaped its exact "
                f"reviewed identity contract: {error}"
            ) from error
    return _database_status(connection)


def database_status(connection: sqlite3.Connection) -> dict:
    """Return catalog counts only after a non-mutating exact-ledger check."""

    verify_migrations(connection)
    return _database_status(connection)


def _database_status(connection: sqlite3.Connection) -> dict:
    table_names = [
        "sources",
        "source_snapshots",
        "source_metadata_observations",
        "source_relation_observations",
        "external_id_observations",
        "recordings",
        "recording_metadata_observations",
        "recording_sources",
        "media_objects",
        "processing_runs",
        "transcript_revisions",
        "transcript_segments",
        "rendition_local_asr_imports",
        "rendition_local_transcript_revisions",
        "rendition_local_transcript_segments",
        "rendition_local_transcript_words",
        "media_local_asr_imports",
        "media_local_transcript_revisions",
        "media_local_transcript_segments",
        "media_local_transcript_words",
        "private_glossary_registrations",
        "contextual_asr_batch_registrations",
        "contextual_media_local_asr_imports",
        "contextual_media_local_asr_pairs",
        "contextual_asr_text_private_diffs",
        "source_recording_transform_candidates",
        "observations",
        "entities",
        "entity_aliases",
        "appearances",
        "events",
        "event_dates",
        "event_participants",
        "event_relations",
        "event_evidence",
        "review_tasks",
        "review_decisions",
        "publication_decisions",
        "publication_gate_decisions",
        "publication_manifest_imports",
        "reviewer_admin_manifest_imports",
        "reviewer_admin_events",
        "claim_catalog_links",
        "import_observations",
        "identity_cluster_versions",
        "identity_cluster_memberships",
        "identity_cannot_link_decisions",
        "biometric_artifacts",
        "identity_assertion_subjects",
        "identity_assertion_decisions",
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
    ]
    if connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'private_gpu_v3_asr_imports'
        """
    ).fetchone() is not None:
        table_names.append("private_gpu_v3_asr_imports")
    if connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table'
          AND name = 'machine_transcript_publication_policy_runs'
        """
    ).fetchone() is not None:
        table_names.append("machine_transcript_publication_policy_runs")
    if connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table'
          AND name = 'media_local_transcript_projection_batches'
        """
    ).fetchone() is not None:
        table_names.extend(
            [
                "media_local_transcript_projection_batches",
                "media_local_transcript_projections",
                "media_local_transcript_projection_pairs",
            ]
        )
    if connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'solo_voice_subjects'
        """
    ).fetchone() is not None:
        table_names.extend(
            [
                "solo_voice_manifest_imports",
                "solo_voice_subjects",
                "solo_voice_privacy_reviews",
                "solo_voice_attestation_decisions",
            ]
        )
    if connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'private_ocr_import_receipts'
        """
    ).fetchone() is not None:
        table_names.extend(
            [
                "private_ocr_import_receipts",
                "private_ocr_frame_admissions",
                "private_ocr_word_admissions",
                "private_ocr_word_fts",
            ]
        )
    result = {
        name: connection.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        for name in table_names
    }
    result["public_recordings"] = connection.execute(
        "SELECT count(*) FROM public_recordings"
    ).fetchone()[0]
    result["public_sources"] = connection.execute(
        "SELECT count(*) FROM public_sources"
    ).fetchone()[0]
    result["public_identity_assertions"] = connection.execute(
        "SELECT count(*) FROM public_identity_assertions"
    ).fetchone()[0]
    result["source_kinds"] = {
        f"{row['platform']}:{row['source_kind']}": row["count"]
        for row in connection.execute(
            """
            SELECT platform, source_kind, count(*) AS count
            FROM sources GROUP BY platform, source_kind
            ORDER BY platform, source_kind
            """
        )
    }
    result["import_batches"] = [
        {
            "import_batch_id": row["import_batch_id"],
            "importer_name": row["importer_name"],
            "statistics": json.loads(row["statistics_json"]),
        }
        for row in connection.execute(
            """
            SELECT import_batch_id, importer_name, statistics_json
            FROM import_batches ORDER BY importer_name, import_batch_id
            """
        )
    ]
    return result


def validate_release_file(path: str | Path) -> dict:
    release_path = Path(path)
    release = json.loads(release_path.read_text(encoding="utf-8"))
    if isinstance(release, dict) and release.get("schema_version") == 2:
        from .sharded_release import validate_sharded_release

        return validate_sharded_release(release_path)
    validate_release_shape(release)
    return {
        "path": str(release_path),
        "release_id": release["release_id"],
        **release["counts"],
    }
