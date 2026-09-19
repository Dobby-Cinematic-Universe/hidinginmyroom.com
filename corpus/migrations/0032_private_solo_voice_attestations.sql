-- Human-governed private attribution of one live voice interval to one named
-- entity.  This lane is deliberately separate from anonymous diarization,
-- active-speaker association, source ownership, and biometric identity clusters.

-- The publication tables intentionally accept open object-type text. Reserve this
-- lane only if no earlier row (including restrictive state) has squatted an object
-- type that could later align with a newly created private solo-voice object.
CREATE TABLE migration_0032_solo_voice_publication_preflight_guard(
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1)
);

CREATE TRIGGER migration_0032_solo_voice_publication_preflight_abort
BEFORE INSERT ON migration_0032_solo_voice_publication_preflight_guard
WHEN EXISTS (
        SELECT 1 FROM publication_decisions
        WHERE object_type IN (
            'solo_voice_subject', 'solo_voice_privacy_review',
            'solo_voice_attestation_decision'
        )
    )
 OR EXISTS (
        SELECT 1 FROM publication_gate_decisions
        WHERE object_type IN (
            'solo_voice_subject', 'solo_voice_privacy_review',
            'solo_voice_attestation_decision'
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'reserved solo voice publication state already exists');
END;

INSERT INTO migration_0032_solo_voice_publication_preflight_guard(singleton)
VALUES(1);
DROP TRIGGER migration_0032_solo_voice_publication_preflight_abort;
DROP TABLE migration_0032_solo_voice_publication_preflight_guard;

CREATE TABLE solo_voice_manifest_imports (
    manifest_id TEXT PRIMARY KEY,
    input_sha256 TEXT NOT NULL UNIQUE CHECK(length(input_sha256) = 64),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    manifest_created_at TEXT NOT NULL CHECK(julianday(manifest_created_at) IS NOT NULL),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    authorized_by TEXT NOT NULL,
    basis TEXT NOT NULL,
    subject_count INTEGER NOT NULL CHECK(subject_count >= 0),
    privacy_review_count INTEGER NOT NULL CHECK(privacy_review_count >= 0),
    speaker_decision_count INTEGER NOT NULL CHECK(speaker_decision_count >= 0),
    CHECK(subject_count + privacy_review_count + speaker_decision_count > 0)
);

CREATE TABLE solo_voice_subjects (
    solo_voice_subject_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL REFERENCES entities(entity_id) ON DELETE RESTRICT,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE RESTRICT,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE RESTRICT,
    rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id) ON DELETE RESTRICT,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE RESTRICT,
    media_sha256 TEXT NOT NULL CHECK(length(media_sha256) = 64),
    start_ms INTEGER NOT NULL CHECK(start_ms >= 0),
    end_ms INTEGER NOT NULL CHECK(end_ms > start_ms),
    coordinate_system TEXT NOT NULL
        CHECK(coordinate_system = 'rendition_media_ms'),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL DEFAULT 'none'
        CHECK(publication_authority = 'none'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    manifest_id TEXT NOT NULL
        REFERENCES solo_voice_manifest_imports(manifest_id) ON DELETE RESTRICT,
    subject_ordinal INTEGER NOT NULL CHECK(subject_ordinal >= 0),
    UNIQUE(manifest_id, subject_ordinal),
    UNIQUE(
        entity_id, source_id, recording_id, rendition_id, media_id,
        start_ms, end_ms
    )
);

CREATE INDEX solo_voice_subjects_rendition_time
    ON solo_voice_subjects(rendition_id, start_ms, end_ms);

CREATE TABLE solo_voice_privacy_reviews (
    privacy_review_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    solo_voice_privacy_review_id TEXT NOT NULL UNIQUE,
    solo_voice_subject_id TEXT NOT NULL
        REFERENCES solo_voice_subjects(solo_voice_subject_id) ON DELETE RESTRICT,
    manifest_id TEXT NOT NULL
        REFERENCES solo_voice_manifest_imports(manifest_id) ON DELETE RESTRICT,
    review_ordinal INTEGER NOT NULL CHECK(review_ordinal >= 0),
    decision TEXT NOT NULL CHECK(decision IN ('clear_private_use', 'withhold')),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT,
    review_decision_id TEXT NOT NULL UNIQUE
        REFERENCES review_decisions(review_decision_id) ON DELETE RESTRICT,
    reviewed_at TEXT NOT NULL CHECK(julianday(reviewed_at) IS NOT NULL),
    basis TEXT NOT NULL,
    named_voice_personal_data_reviewed INTEGER NOT NULL
        CHECK(named_voice_personal_data_reviewed = 1),
    private_storage_only INTEGER NOT NULL CHECK(private_storage_only = 1),
    public_export_approved INTEGER NOT NULL CHECK(public_export_approved = 0),
    biometric_artifacts_used INTEGER NOT NULL CHECK(biometric_artifacts_used = 0),
    machine_identity_outputs_used INTEGER NOT NULL
        CHECK(machine_identity_outputs_used = 0),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL DEFAULT 'none'
        CHECK(publication_authority = 'none'),
    UNIQUE(manifest_id, review_ordinal),
    UNIQUE(solo_voice_subject_id, reviewed_at, decision)
);

CREATE INDEX solo_voice_privacy_review_stream
    ON solo_voice_privacy_reviews(solo_voice_subject_id, privacy_review_sequence);

CREATE TABLE solo_voice_attestation_decisions (
    speaker_decision_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    solo_voice_attestation_decision_id TEXT NOT NULL UNIQUE,
    solo_voice_subject_id TEXT NOT NULL
        REFERENCES solo_voice_subjects(solo_voice_subject_id) ON DELETE RESTRICT,
    manifest_id TEXT NOT NULL
        REFERENCES solo_voice_manifest_imports(manifest_id) ON DELETE RESTRICT,
    decision_ordinal INTEGER NOT NULL CHECK(decision_ordinal >= 0),
    decision TEXT NOT NULL CHECK(decision IN ('assert', 'withdraw', 'reject', 'dispute')),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT,
    review_decision_id TEXT NOT NULL UNIQUE
        REFERENCES review_decisions(review_decision_id) ON DELETE RESTRICT,
    decided_at TEXT NOT NULL CHECK(julianday(decided_at) IS NOT NULL),
    basis TEXT NOT NULL,
    direct_audio_attestation TEXT,
    audio_directly_perceived INTEGER CHECK(audio_directly_perceived IN (0, 1)),
    reviewed_entire_interval INTEGER CHECK(reviewed_entire_interval IN (0, 1)),
    exactly_one_live_human_speaker INTEGER
        CHECK(exactly_one_live_human_speaker IN (0, 1)),
    overlap_detected INTEGER CHECK(overlap_detected IN (0, 1)),
    playback_detected INTEGER CHECK(playback_detected IN (0, 1)),
    tts_detected INTEGER CHECK(tts_detected IN (0, 1)),
    synthetic_voice_detected INTEGER CHECK(synthetic_voice_detected IN (0, 1)),
    unknown_audio_origin_detected INTEGER
        CHECK(unknown_audio_origin_detected IN (0, 1)),
    source_metadata_used_as_identity_evidence INTEGER
        CHECK(source_metadata_used_as_identity_evidence IN (0, 1)),
    channel_context_used_as_identity_evidence INTEGER
        CHECK(channel_context_used_as_identity_evidence IN (0, 1)),
    transcript_text_used_as_identity_evidence INTEGER
        CHECK(transcript_text_used_as_identity_evidence IN (0, 1)),
    machine_identity_output_used INTEGER
        CHECK(machine_identity_output_used IN (0, 1)),
    machine_confidence_used INTEGER CHECK(machine_confidence_used IN (0, 1)),
    speaking_face_claimed INTEGER CHECK(speaking_face_claimed IN (0, 1)),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL DEFAULT 'none'
        CHECK(publication_authority = 'none'),
    UNIQUE(manifest_id, decision_ordinal),
    UNIQUE(solo_voice_subject_id, decided_at, decision),
    CHECK(
        (
            decision = 'assert'
            AND direct_audio_attestation IS
                'I directly listened to the complete anchored interval and identify exactly one live human voice as the named entity without relying on source or channel context, transcript text, machine identity output, or machine confidence.'
            AND audio_directly_perceived IS 1
            AND reviewed_entire_interval IS 1
            AND exactly_one_live_human_speaker IS 1
            AND overlap_detected IS 0
            AND playback_detected IS 0
            AND tts_detected IS 0
            AND synthetic_voice_detected IS 0
            AND unknown_audio_origin_detected IS 0
            AND source_metadata_used_as_identity_evidence IS 0
            AND channel_context_used_as_identity_evidence IS 0
            AND transcript_text_used_as_identity_evidence IS 0
            AND machine_identity_output_used IS 0
            AND machine_confidence_used IS 0
            AND speaking_face_claimed IS 0
        )
        OR
        (
            decision <> 'assert'
            AND direct_audio_attestation IS NULL
            AND audio_directly_perceived IS NULL
            AND reviewed_entire_interval IS NULL
            AND exactly_one_live_human_speaker IS NULL
            AND overlap_detected IS NULL
            AND playback_detected IS NULL
            AND tts_detected IS NULL
            AND synthetic_voice_detected IS NULL
            AND unknown_audio_origin_detected IS NULL
            AND source_metadata_used_as_identity_evidence IS NULL
            AND channel_context_used_as_identity_evidence IS NULL
            AND transcript_text_used_as_identity_evidence IS NULL
            AND machine_identity_output_used IS NULL
            AND machine_confidence_used IS NULL
            AND speaking_face_claimed IS NULL
        )
    )
);

CREATE INDEX solo_voice_attestation_decision_stream
    ON solo_voice_attestation_decisions(
        solo_voice_subject_id, speaker_decision_sequence
    );

CREATE VIEW current_solo_voice_privacy_reviews AS
SELECT review.*
FROM solo_voice_privacy_reviews AS review
WHERE NOT EXISTS (
    SELECT 1
    FROM solo_voice_privacy_reviews AS newer
    WHERE newer.solo_voice_subject_id = review.solo_voice_subject_id
      AND (
          julianday(newer.reviewed_at) > julianday(review.reviewed_at)
          OR (
              julianday(newer.reviewed_at) = julianday(review.reviewed_at)
              AND newer.privacy_review_sequence > review.privacy_review_sequence
          )
      )
);

CREATE VIEW current_solo_voice_attestation_decisions AS
SELECT decision.*
FROM solo_voice_attestation_decisions AS decision
WHERE NOT EXISTS (
    SELECT 1
    FROM solo_voice_attestation_decisions AS newer
    WHERE newer.solo_voice_subject_id = decision.solo_voice_subject_id
      AND (
          julianday(newer.decided_at) > julianday(decision.decided_at)
          OR (
              julianday(newer.decided_at) = julianday(decision.decided_at)
              AND newer.speaker_decision_sequence >
                    decision.speaker_decision_sequence
          )
      )
);

-- This remains a private catalog view.  It intentionally contains no machine score,
-- cluster, face-track, transcript, source title, channel label, or publication flag.
CREATE VIEW current_private_solo_voice_assignments AS
SELECT subject.solo_voice_subject_id,
       subject.entity_id,
       subject.source_id,
       subject.recording_id,
       subject.rendition_id,
       subject.media_id,
       subject.media_sha256,
       subject.start_ms,
       subject.end_ms,
       subject.coordinate_system,
       decision.solo_voice_attestation_decision_id,
       decision.reviewer_id,
       decision.decided_at,
       privacy.solo_voice_privacy_review_id,
       privacy.reviewer_id AS privacy_reviewer_id,
       privacy.reviewed_at AS privacy_reviewed_at
FROM solo_voice_subjects AS subject
JOIN current_solo_voice_attestation_decisions AS decision
  ON decision.solo_voice_subject_id = subject.solo_voice_subject_id
 AND decision.decision = 'assert'
 AND decision.visibility IS 'private'
 AND decision.publication_authority IS 'none'
 AND decision.direct_audio_attestation IS
       'I directly listened to the complete anchored interval and identify exactly one live human voice as the named entity without relying on source or channel context, transcript text, machine identity output, or machine confidence.'
 AND decision.audio_directly_perceived IS 1
 AND decision.reviewed_entire_interval IS 1
 AND decision.exactly_one_live_human_speaker IS 1
 AND decision.overlap_detected IS 0
 AND decision.playback_detected IS 0
 AND decision.tts_detected IS 0
 AND decision.synthetic_voice_detected IS 0
 AND decision.unknown_audio_origin_detected IS 0
 AND decision.source_metadata_used_as_identity_evidence IS 0
 AND decision.channel_context_used_as_identity_evidence IS 0
 AND decision.transcript_text_used_as_identity_evidence IS 0
 AND decision.machine_identity_output_used IS 0
 AND decision.machine_confidence_used IS 0
 AND decision.speaking_face_claimed IS 0
JOIN current_solo_voice_privacy_reviews AS privacy
  ON privacy.solo_voice_subject_id = subject.solo_voice_subject_id
 AND privacy.decision = 'clear_private_use'
 AND julianday(privacy.reviewed_at) < julianday(decision.decided_at)
 AND privacy.named_voice_personal_data_reviewed IS 1
 AND privacy.private_storage_only IS 1
 AND privacy.public_export_approved IS 0
 AND privacy.biometric_artifacts_used IS 0
 AND privacy.machine_identity_outputs_used IS 0
 AND privacy.visibility IS 'private'
 AND privacy.publication_authority IS 'none'
JOIN reviewers AS speaker_reviewer
  ON speaker_reviewer.reviewer_id = decision.reviewer_id
 AND speaker_reviewer.reviewer_kind = 'human'
 AND speaker_reviewer.active = 1
JOIN reviewers AS privacy_reviewer
  ON privacy_reviewer.reviewer_id = privacy.reviewer_id
 AND privacy_reviewer.reviewer_kind = 'human'
 AND privacy_reviewer.active = 1
 AND privacy_reviewer.reviewer_id <> speaker_reviewer.reviewer_id
JOIN entities AS entity
  ON entity.entity_id = subject.entity_id
 AND entity.entity_type IN ('person', 'community_figure')
 AND entity.review_state = 'reviewed'
JOIN sources AS source
  ON source.source_id = subject.source_id
 AND source.review_state = 'reviewed'
JOIN recordings AS recording
  ON recording.recording_id = subject.recording_id
 AND recording.review_state = 'reviewed'
 AND recording.merged_into_recording_id IS NULL
JOIN renditions AS rendition
  ON rendition.rendition_id = subject.rendition_id
 AND rendition.recording_id = subject.recording_id
 AND rendition.media_id = subject.media_id
 AND rendition.review_state = 'reviewed'
JOIN media_objects AS media
  ON media.media_id = subject.media_id
 AND media.sha256 = subject.media_sha256
 AND media.integrity_state = 'verified'
 AND media.media_kind IN ('audio', 'video')
 AND media.duration_ms IS NOT NULL
 AND subject.end_ms <= media.duration_ms
JOIN media_sources AS media_source
  ON media_source.media_id = subject.media_id
 AND media_source.source_id = subject.source_id
WHERE subject.coordinate_system IS 'rendition_media_ms'
  AND subject.visibility IS 'private'
  AND subject.publication_authority IS 'none'
  AND EXISTS (
      SELECT 1
      FROM recording_sources AS recording_source
      WHERE recording_source.recording_id = subject.recording_id
        AND recording_source.source_id = subject.source_id
        AND recording_source.confidence_state = 'reviewed'
  );

-- Subject admission proves an exact, currently reviewed catalog anchor.  Source or
-- channel metadata establish only which bytes were reviewed; they do not establish
-- who the voice belongs to.
CREATE TRIGGER solo_voice_subject_anchor_is_confirmed
BEFORE INSERT ON solo_voice_subjects
WHEN NOT EXISTS (
    SELECT 1
    FROM entities AS entity
    JOIN sources AS source ON source.source_id = NEW.source_id
    JOIN recordings AS recording ON recording.recording_id = NEW.recording_id
    JOIN recording_sources AS recording_source
      ON recording_source.recording_id = recording.recording_id
     AND recording_source.source_id = source.source_id
    JOIN renditions AS rendition ON rendition.rendition_id = NEW.rendition_id
    JOIN media_objects AS media ON media.media_id = NEW.media_id
    JOIN media_sources AS media_source
      ON media_source.media_id = media.media_id
     AND media_source.source_id = source.source_id
    WHERE entity.entity_id = NEW.entity_id
      AND entity.entity_type IN ('person', 'community_figure')
      AND entity.review_state = 'reviewed'
      AND source.review_state = 'reviewed'
      AND recording.review_state = 'reviewed'
      AND recording.merged_into_recording_id IS NULL
      AND recording_source.confidence_state = 'reviewed'
      AND rendition.recording_id = recording.recording_id
      AND rendition.media_id = media.media_id
      AND rendition.review_state = 'reviewed'
      AND media.sha256 = NEW.media_sha256
      AND media.integrity_state = 'verified'
      AND media.media_kind IN ('audio', 'video')
      AND media.duration_ms IS NOT NULL
      AND NEW.end_ms <= media.duration_ms
)
BEGIN
    SELECT RAISE(ABORT, 'solo voice subject requires an exact reviewed source/recording/rendition/media anchor');
END;

CREATE TRIGGER solo_voice_manifest_time_is_current
BEFORE INSERT ON solo_voice_manifest_imports
WHEN julianday(NEW.manifest_created_at) >
         julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
  OR julianday(NEW.imported_at) >
         julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
  OR julianday(NEW.manifest_created_at) > julianday(NEW.imported_at)
BEGIN
    SELECT RAISE(ABORT, 'solo voice manifest time must not be in the future');
END;

CREATE TRIGGER solo_voice_privacy_review_human_and_current
BEFORE INSERT ON solo_voice_privacy_reviews
WHEN julianday(NEW.reviewed_at) > julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
 OR NOT EXISTS (
    SELECT 1
    FROM solo_voice_manifest_imports AS manifest
    JOIN reviewers AS reviewer ON reviewer.reviewer_id = NEW.reviewer_id
    JOIN reviewer_admin_events AS state ON state.reviewer_id = reviewer.reviewer_id
    JOIN review_decisions AS review ON review.review_decision_id = NEW.review_decision_id
    WHERE manifest.manifest_id = NEW.manifest_id
      AND reviewer.reviewer_kind = 'human'
      AND reviewer.active = 1
      AND state.event_sequence = (
          SELECT MAX(candidate.event_sequence)
          FROM reviewer_admin_events AS candidate
          WHERE candidate.reviewer_id = reviewer.reviewer_id
            AND julianday(candidate.effective_at) <= julianday(NEW.reviewed_at)
      )
      AND state.new_active = 1
      AND review.reviewer_id = NEW.reviewer_id
      AND review.target_type = 'solo_voice_privacy_review'
      AND review.target_id = NEW.solo_voice_privacy_review_id
      AND review.decision = CASE NEW.decision
              WHEN 'clear_private_use' THEN 'accept' ELSE 'reject' END
      AND review.decided_at = NEW.reviewed_at
      AND review.audio_directly_perceived = 0
      AND review.video_directly_perceived = 0
      AND review.reviewed_complete_item = 1
      AND review.context_start_ms IS NULL
      AND review.context_end_ms IS NULL
      AND review.review_task_id IS NULL
      AND review.notes IS NULL
      AND review.basis = NEW.basis
      AND julianday(NEW.reviewed_at) <= julianday(manifest.manifest_created_at)
      AND julianday(NEW.reviewed_at) <= julianday(manifest.imported_at)
 )
BEGIN
    SELECT RAISE(ABORT, 'solo voice privacy review requires governed active human review');
END;

CREATE TRIGGER solo_voice_privacy_review_chronological
BEFORE INSERT ON solo_voice_privacy_reviews
WHEN EXISTS (
    SELECT 1 FROM solo_voice_privacy_reviews AS existing
    WHERE existing.solo_voice_subject_id = NEW.solo_voice_subject_id
      AND julianday(existing.reviewed_at) >= julianday(NEW.reviewed_at)
)
BEGIN
    SELECT RAISE(ABORT, 'solo voice privacy review must be later than its existing stream');
END;

CREATE TRIGGER solo_voice_attestation_human_and_current
BEFORE INSERT ON solo_voice_attestation_decisions
WHEN julianday(NEW.decided_at) > julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
 OR NOT EXISTS (
    SELECT 1
    FROM solo_voice_manifest_imports AS manifest
    JOIN solo_voice_subjects AS subject
      ON subject.solo_voice_subject_id = NEW.solo_voice_subject_id
    JOIN reviewers AS reviewer ON reviewer.reviewer_id = NEW.reviewer_id
    JOIN reviewer_admin_events AS state ON state.reviewer_id = reviewer.reviewer_id
    JOIN review_decisions AS review ON review.review_decision_id = NEW.review_decision_id
    WHERE manifest.manifest_id = NEW.manifest_id
      AND reviewer.reviewer_kind = 'human'
      AND reviewer.active = 1
      AND state.event_sequence = (
          SELECT MAX(candidate.event_sequence)
          FROM reviewer_admin_events AS candidate
          WHERE candidate.reviewer_id = reviewer.reviewer_id
            AND julianday(candidate.effective_at) <= julianday(NEW.decided_at)
      )
      AND state.new_active = 1
      AND review.reviewer_id = NEW.reviewer_id
      AND review.target_type = 'solo_voice_subject'
      AND review.target_id = NEW.solo_voice_subject_id
      AND review.decision = CASE NEW.decision
              WHEN 'assert' THEN 'accept'
              WHEN 'withdraw' THEN 'correct'
              WHEN 'reject' THEN 'reject'
              ELSE 'dispute' END
      AND review.decided_at = NEW.decided_at
      AND review.audio_directly_perceived = CASE NEW.decision
              WHEN 'assert' THEN 1 ELSE 0 END
      AND review.video_directly_perceived = 0
      AND review.reviewed_complete_item = CASE NEW.decision
              WHEN 'assert' THEN 1 ELSE 0 END
      AND review.context_start_ms = subject.start_ms
      AND review.context_end_ms = subject.end_ms
      AND review.review_task_id IS NULL
      AND review.notes IS NULL
      AND review.basis = NEW.basis
      AND julianday(NEW.decided_at) <= julianday(manifest.manifest_created_at)
      AND julianday(NEW.decided_at) <= julianday(manifest.imported_at)
 )
BEGIN
    SELECT RAISE(ABORT, 'solo voice decision requires governed active human review');
END;

CREATE TRIGGER solo_voice_attestation_chronological
BEFORE INSERT ON solo_voice_attestation_decisions
WHEN EXISTS (
    SELECT 1 FROM solo_voice_attestation_decisions AS existing
    WHERE existing.solo_voice_subject_id = NEW.solo_voice_subject_id
      AND julianday(existing.decided_at) >= julianday(NEW.decided_at)
)
BEGIN
    SELECT RAISE(ABORT, 'solo voice decision must be later than its existing stream');
END;

CREATE TRIGGER solo_voice_assert_requires_independent_privacy_clearance
BEFORE INSERT ON solo_voice_attestation_decisions
WHEN NEW.decision = 'assert'
 AND NOT EXISTS (
     SELECT 1
     FROM current_solo_voice_privacy_reviews AS privacy
     JOIN reviewers AS reviewer ON reviewer.reviewer_id = privacy.reviewer_id
     WHERE privacy.solo_voice_subject_id = NEW.solo_voice_subject_id
       AND privacy.decision = 'clear_private_use'
       AND privacy.named_voice_personal_data_reviewed IS 1
       AND privacy.private_storage_only IS 1
       AND privacy.public_export_approved IS 0
       AND privacy.biometric_artifacts_used IS 0
       AND privacy.machine_identity_outputs_used IS 0
       AND privacy.visibility IS 'private'
       AND privacy.publication_authority IS 'none'
       AND reviewer.reviewer_kind = 'human'
       AND reviewer.active = 1
       AND privacy.reviewer_id <> NEW.reviewer_id
       AND julianday(privacy.reviewed_at) < julianday(NEW.decided_at)
 )
BEGIN
    SELECT RAISE(ABORT, 'solo voice assertion requires prior independent privacy/biometric clearance');
END;

CREATE TRIGGER solo_voice_assert_requires_current_confirmed_anchor
BEFORE INSERT ON solo_voice_attestation_decisions
WHEN NEW.decision = 'assert'
 AND NOT EXISTS (
     SELECT 1
     FROM solo_voice_subjects AS subject
     JOIN entities AS entity ON entity.entity_id = subject.entity_id
     JOIN sources AS source ON source.source_id = subject.source_id
     JOIN recordings AS recording ON recording.recording_id = subject.recording_id
     JOIN recording_sources AS recording_source
       ON recording_source.recording_id = subject.recording_id
      AND recording_source.source_id = subject.source_id
     JOIN renditions AS rendition ON rendition.rendition_id = subject.rendition_id
     JOIN media_objects AS media ON media.media_id = subject.media_id
     JOIN media_sources AS media_source
       ON media_source.media_id = subject.media_id
      AND media_source.source_id = subject.source_id
     WHERE subject.solo_voice_subject_id = NEW.solo_voice_subject_id
       AND entity.entity_type IN ('person', 'community_figure')
       AND entity.review_state = 'reviewed'
       AND source.review_state = 'reviewed'
       AND recording.review_state = 'reviewed'
       AND recording.merged_into_recording_id IS NULL
       AND recording_source.confidence_state = 'reviewed'
       AND rendition.recording_id = subject.recording_id
       AND rendition.media_id = subject.media_id
       AND rendition.review_state = 'reviewed'
       AND media.sha256 = subject.media_sha256
       AND media.integrity_state = 'verified'
       AND media.media_kind IN ('audio', 'video')
       AND media.duration_ms IS NOT NULL
       AND subject.end_ms <= media.duration_ms
 )
BEGIN
    SELECT RAISE(ABORT, 'solo voice assertion requires a still-confirmed exact catalog anchor');
END;

CREATE TRIGGER solo_voice_assert_no_conflicting_current_identity
BEFORE INSERT ON solo_voice_attestation_decisions
WHEN NEW.decision = 'assert'
 AND EXISTS (
     SELECT 1
     FROM solo_voice_subjects AS proposed
     JOIN solo_voice_subjects AS existing
       ON existing.media_id = proposed.media_id
      AND existing.start_ms < proposed.end_ms
      AND proposed.start_ms < existing.end_ms
      AND existing.entity_id <> proposed.entity_id
     JOIN current_solo_voice_attestation_decisions AS current
       ON current.solo_voice_subject_id = existing.solo_voice_subject_id
      AND current.decision = 'assert'
     WHERE proposed.solo_voice_subject_id = NEW.solo_voice_subject_id
 )
BEGIN
    SELECT RAISE(ABORT, 'solo voice assertion conflicts with a current overlapping named identity');
END;

-- BEFORE INSERT guards close every replace path across all UNIQUE targets.
CREATE TRIGGER solo_voice_manifest_no_replace
BEFORE INSERT ON solo_voice_manifest_imports
WHEN EXISTS (
    SELECT 1 FROM solo_voice_manifest_imports AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.manifest_id = NEW.manifest_id
       OR existing.input_sha256 = NEW.input_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'solo voice manifests are append-only; replacement is forbidden');
END;

CREATE TRIGGER solo_voice_subject_no_replace
BEFORE INSERT ON solo_voice_subjects
WHEN EXISTS (
    SELECT 1 FROM solo_voice_subjects AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.solo_voice_subject_id = NEW.solo_voice_subject_id
       OR (existing.manifest_id = NEW.manifest_id
           AND existing.subject_ordinal = NEW.subject_ordinal)
       OR (existing.entity_id = NEW.entity_id
           AND existing.source_id = NEW.source_id
           AND existing.recording_id = NEW.recording_id
           AND existing.rendition_id = NEW.rendition_id
           AND existing.media_id = NEW.media_id
           AND existing.start_ms = NEW.start_ms
           AND existing.end_ms = NEW.end_ms)
)
BEGIN
    SELECT RAISE(ABORT, 'solo voice subjects are append-only; replacement is forbidden');
END;

CREATE TRIGGER solo_voice_privacy_review_no_replace
BEFORE INSERT ON solo_voice_privacy_reviews
WHEN EXISTS (
    SELECT 1 FROM solo_voice_privacy_reviews AS existing
    WHERE existing.privacy_review_sequence = NEW.privacy_review_sequence
       OR existing.solo_voice_privacy_review_id = NEW.solo_voice_privacy_review_id
       OR existing.review_decision_id = NEW.review_decision_id
       OR (existing.manifest_id = NEW.manifest_id
           AND existing.review_ordinal = NEW.review_ordinal)
       OR (existing.solo_voice_subject_id = NEW.solo_voice_subject_id
           AND existing.reviewed_at = NEW.reviewed_at
           AND existing.decision = NEW.decision)
)
BEGIN
    SELECT RAISE(ABORT, 'solo voice privacy reviews are append-only; replacement is forbidden');
END;

CREATE TRIGGER solo_voice_attestation_decision_no_replace
BEFORE INSERT ON solo_voice_attestation_decisions
WHEN EXISTS (
    SELECT 1 FROM solo_voice_attestation_decisions AS existing
    WHERE existing.speaker_decision_sequence = NEW.speaker_decision_sequence
       OR existing.solo_voice_attestation_decision_id =
              NEW.solo_voice_attestation_decision_id
       OR existing.review_decision_id = NEW.review_decision_id
       OR (existing.manifest_id = NEW.manifest_id
           AND existing.decision_ordinal = NEW.decision_ordinal)
       OR (existing.solo_voice_subject_id = NEW.solo_voice_subject_id
           AND existing.decided_at = NEW.decided_at
           AND existing.decision = NEW.decision)
)
BEGIN
    SELECT RAISE(ABORT, 'solo voice decisions are append-only; replacement is forbidden');
END;

-- INTEGER PRIMARY KEY accepts an explicit unused value below the current maximum.
-- Reject that route after SQLite assigns ordinary automatic keys. Current views also
-- derive state from the strictly chronological human-decision time, not caller keys.
CREATE TRIGGER solo_voice_privacy_review_sequence_must_append
AFTER INSERT ON solo_voice_privacy_reviews
WHEN NEW.privacy_review_sequence <>
        (SELECT MAX(privacy_review_sequence) FROM solo_voice_privacy_reviews)
BEGIN
    SELECT RAISE(ABORT, 'solo voice privacy review sequence must append');
END;

CREATE TRIGGER solo_voice_attestation_sequence_must_append
AFTER INSERT ON solo_voice_attestation_decisions
WHEN NEW.speaker_decision_sequence <>
        (SELECT MAX(speaker_decision_sequence)
         FROM solo_voice_attestation_decisions)
BEGIN
    SELECT RAISE(ABORT, 'solo voice decision sequence must append');
END;

CREATE TRIGGER solo_voice_manifests_no_update
BEFORE UPDATE ON solo_voice_manifest_imports
BEGIN SELECT RAISE(ABORT, 'solo voice manifests are append-only'); END;
CREATE TRIGGER solo_voice_manifests_no_delete
BEFORE DELETE ON solo_voice_manifest_imports
BEGIN SELECT RAISE(ABORT, 'solo voice manifests are append-only'); END;
CREATE TRIGGER solo_voice_subjects_no_update
BEFORE UPDATE ON solo_voice_subjects
BEGIN SELECT RAISE(ABORT, 'solo voice subjects are append-only'); END;
CREATE TRIGGER solo_voice_subjects_no_delete
BEFORE DELETE ON solo_voice_subjects
BEGIN SELECT RAISE(ABORT, 'solo voice subjects are append-only'); END;
CREATE TRIGGER solo_voice_privacy_reviews_no_update
BEFORE UPDATE ON solo_voice_privacy_reviews
BEGIN SELECT RAISE(ABORT, 'solo voice privacy reviews are append-only'); END;
CREATE TRIGGER solo_voice_privacy_reviews_no_delete
BEFORE DELETE ON solo_voice_privacy_reviews
BEGIN SELECT RAISE(ABORT, 'solo voice privacy reviews are append-only'); END;
CREATE TRIGGER solo_voice_attestation_decisions_no_update
BEFORE UPDATE ON solo_voice_attestation_decisions
BEGIN SELECT RAISE(ABORT, 'solo voice decisions are append-only'); END;
CREATE TRIGGER solo_voice_attestation_decisions_no_delete
BEFORE DELETE ON solo_voice_attestation_decisions
BEGIN SELECT RAISE(ABORT, 'solo voice decisions are append-only'); END;

CREATE TRIGGER cited_solo_voice_review_decisions_no_update
BEFORE UPDATE ON review_decisions
WHEN EXISTS (
        SELECT 1 FROM solo_voice_privacy_reviews
        WHERE review_decision_id = OLD.review_decision_id
    )
 OR EXISTS (
        SELECT 1 FROM solo_voice_attestation_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
BEGIN
    SELECT RAISE(ABORT, 'review decision is immutable after solo voice citation');
END;

-- Keep cited reviews sealed even for INSERT OR REPLACE with recursive trigger
-- execution disabled. The replacement's BEFORE INSERT path cannot silently delete
-- and recreate a cited generic review.
CREATE TRIGGER cited_solo_voice_review_decisions_no_replace
BEFORE INSERT ON review_decisions
WHEN EXISTS (
    SELECT 1
    FROM review_decisions AS existing
    WHERE (
            existing.rowid = NEW.rowid
            OR existing.review_decision_id = NEW.review_decision_id
          )
      AND (
          EXISTS (
              SELECT 1 FROM solo_voice_privacy_reviews AS privacy
              WHERE privacy.review_decision_id = existing.review_decision_id
          )
          OR EXISTS (
              SELECT 1 FROM solo_voice_attestation_decisions AS decision
              WHERE decision.review_decision_id = existing.review_decision_id
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'review decision is immutable after solo voice citation');
END;

CREATE TRIGGER cited_solo_voice_review_decisions_no_delete
BEFORE DELETE ON review_decisions
WHEN EXISTS (
        SELECT 1 FROM solo_voice_privacy_reviews
        WHERE review_decision_id = OLD.review_decision_id
    )
 OR EXISTS (
        SELECT 1 FROM solo_voice_attestation_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
BEGIN
    SELECT RAISE(ABORT, 'review decision is immutable after solo voice citation');
END;

-- The generic publication tables have open object-type text.  Explicitly deny this
-- private lane even if a future caller attempts to manufacture publication state.
CREATE TRIGGER publication_decisions_block_solo_voice_private_objects
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type IN (
     'solo_voice_subject', 'solo_voice_privacy_review',
     'solo_voice_attestation_decision'
 )
BEGIN
    SELECT RAISE(ABORT, 'private solo voice objects cannot enter publication state');
END;

CREATE TRIGGER publication_gate_decisions_block_solo_voice_private_objects
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type IN (
     'solo_voice_subject', 'solo_voice_privacy_review',
     'solo_voice_attestation_decision'
 )
BEGIN
    SELECT RAISE(ABORT, 'private solo voice objects cannot enter gate state');
END;
