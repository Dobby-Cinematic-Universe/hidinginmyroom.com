-- Private, rendition-local transcript admission for ASR over analysis windows.
--
-- Ordinary transcript_segments are recording-scoped.  These tables deliberately
-- keep artifact/rendition time in a separate lane until a human-reviewed transform
-- exists.  Nothing here is projected by the public views.

CREATE TABLE rendition_local_transcript_revisions (
    local_revision_id TEXT PRIMARY KEY,
    producer_revision_id TEXT NOT NULL,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id),
    processing_run_id TEXT NOT NULL UNIQUE REFERENCES processing_runs(processing_run_id),
    revision_kind TEXT NOT NULL
        CHECK(revision_kind IN ('raw_asr', 'contextual_asr')),
    origin TEXT NOT NULL,
    language TEXT NOT NULL,
    glossary_revision_id TEXT REFERENCES glossary_revisions(glossary_revision_id),
    review_state TEXT NOT NULL DEFAULT 'machine'
        CHECK(review_state IN ('machine', 'disputed', 'rejected')),
    coordinate_system TEXT NOT NULL
        CHECK(coordinate_system = 'rendition_media_ms'),
    boundary TEXT NOT NULL CHECK(boundary = 'half_open'),
    input_duration_ms INTEGER NOT NULL CHECK(input_duration_ms > 0),
    requested_start_ms INTEGER NOT NULL CHECK(requested_start_ms >= 0),
    requested_end_ms INTEGER NOT NULL CHECK(requested_end_ms > requested_start_ms),
    max_segment_end_ms INTEGER NOT NULL CHECK(max_segment_end_ms >= 0),
    input_boundary_overrun_ms INTEGER NOT NULL CHECK(input_boundary_overrun_ms >= 0),
    source_mapping_state TEXT NOT NULL
        CHECK(source_mapping_state = 'integer_contract_not_boundary_calibrated'),
    recording_transform_state TEXT NOT NULL
        CHECK(recording_transform_state = 'unresolved'),
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(rendition_id, producer_revision_id),
    CHECK(requested_end_ms - requested_start_ms = input_duration_ms),
    CHECK(input_boundary_overrun_ms = max(0, max_segment_end_ms - requested_end_ms))
);

CREATE INDEX rendition_local_revisions_recording_idx
    ON rendition_local_transcript_revisions(recording_id, rendition_id, created_at);

CREATE TABLE rendition_local_transcript_segments (
    local_segment_id TEXT PRIMARY KEY,
    local_revision_id TEXT NOT NULL
        REFERENCES rendition_local_transcript_revisions(local_revision_id) ON DELETE CASCADE,
    producer_segment_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    start_ms INTEGER NOT NULL CHECK(start_ms >= 0),
    end_ms INTEGER NOT NULL CHECK(end_ms > start_ms),
    source_start_ms INTEGER NOT NULL CHECK(source_start_ms >= 0),
    source_end_ms INTEGER NOT NULL CHECK(source_end_ms > source_start_ms),
    source_boundary_overrun_ms INTEGER NOT NULL CHECK(source_boundary_overrun_ms >= 0),
    text TEXT NOT NULL,
    normalized_text TEXT,
    speaker_label TEXT,
    language TEXT,
    confidence_band TEXT
        CHECK(confidence_band IS NULL OR confidence_band IN ('low', 'medium', 'high', 'human')),
    calibrated_probability REAL
        CHECK(calibrated_probability IS NULL OR
              (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(local_revision_id, ordinal),
    UNIQUE(local_revision_id, producer_segment_id),
    CHECK(source_end_ms - source_start_ms = end_ms - start_ms)
);

CREATE INDEX rendition_local_segments_time_idx
    ON rendition_local_transcript_segments(local_revision_id, start_ms, end_ms);

CREATE TABLE rendition_local_transcript_words (
    local_word_id TEXT PRIMARY KEY,
    local_segment_id TEXT NOT NULL
        REFERENCES rendition_local_transcript_segments(local_segment_id) ON DELETE CASCADE,
    producer_word_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    start_ms INTEGER CHECK(start_ms IS NULL OR start_ms >= 0),
    end_ms INTEGER CHECK(end_ms IS NULL OR end_ms >= 0),
    source_start_ms INTEGER CHECK(source_start_ms IS NULL OR source_start_ms >= 0),
    source_end_ms INTEGER CHECK(source_end_ms IS NULL OR source_end_ms >= 0),
    token TEXT NOT NULL,
    normalized_token TEXT,
    asr_log_probability REAL,
    alignment_score REAL,
    calibrated_probability REAL
        CHECK(calibrated_probability IS NULL OR
              (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    UNIQUE(local_segment_id, ordinal),
    UNIQUE(local_segment_id, producer_word_id),
    CHECK((start_ms IS NULL) = (end_ms IS NULL)),
    CHECK((source_start_ms IS NULL) = (source_end_ms IS NULL)),
    CHECK(end_ms IS NULL OR end_ms >= start_ms),
    CHECK(source_end_ms IS NULL OR source_end_ms >= source_start_ms),
    CHECK(start_ms IS NULL OR source_end_ms - source_start_ms = end_ms - start_ms)
);

CREATE TABLE source_recording_transform_candidates (
    transform_candidate_id TEXT PRIMARY KEY,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    source_rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id),
    candidate_kind TEXT NOT NULL
        CHECK(candidate_kind IN (
            'identity_from_acquired_media_duration_hypothesis',
            'linear_scale_to_declared_recording_duration_hypothesis'
        )),
    source_start_ms INTEGER NOT NULL CHECK(source_start_ms = 0),
    source_end_ms INTEGER NOT NULL CHECK(source_end_ms > source_start_ms),
    proposed_recording_start_ms INTEGER NOT NULL CHECK(proposed_recording_start_ms = 0),
    proposed_recording_end_ms INTEGER NOT NULL
        CHECK(proposed_recording_end_ms > proposed_recording_start_ms),
    scale_numerator INTEGER NOT NULL CHECK(scale_numerator > 0),
    scale_denominator INTEGER NOT NULL CHECK(scale_denominator > 0),
    declared_recording_duration_ms INTEGER NOT NULL CHECK(declared_recording_duration_ms > 0),
    acquired_media_duration_ms INTEGER NOT NULL CHECK(acquired_media_duration_ms > 0),
    duration_delta_ms INTEGER NOT NULL,
    evidence_state TEXT NOT NULL CHECK(evidence_state = 'hypothesis_only_unreviewed'),
    timeline_application_allowed INTEGER NOT NULL DEFAULT 0
        CHECK(timeline_application_allowed = 0),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    review_task_id TEXT NOT NULL UNIQUE REFERENCES review_tasks(review_task_id),
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(recording_id, source_id, source_media_id, candidate_kind),
    CHECK(source_end_ms = acquired_media_duration_ms),
    CHECK(duration_delta_ms = acquired_media_duration_ms - declared_recording_duration_ms)
);

CREATE TABLE rendition_local_asr_imports (
    local_asr_import_id TEXT PRIMARY KEY,
    import_batch_id TEXT NOT NULL UNIQUE REFERENCES import_batches(import_batch_id),
    local_revision_id TEXT NOT NULL UNIQUE
        REFERENCES rendition_local_transcript_revisions(local_revision_id),
    asr_result_uri TEXT NOT NULL,
    asr_result_raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(asr_result_raw_sha256) = 64),
    asr_result_canonical_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(asr_result_canonical_sha256) = 64),
    asr_result_byte_count INTEGER NOT NULL CHECK(asr_result_byte_count > 0),
    local_window_result_uri TEXT NOT NULL,
    local_window_result_sha256 TEXT NOT NULL CHECK(length(local_window_result_sha256) = 64),
    input_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    source_start_ms INTEGER NOT NULL CHECK(source_start_ms >= 0),
    source_end_ms INTEGER NOT NULL CHECK(source_end_ms > source_start_ms),
    artifact_duration_ms INTEGER NOT NULL CHECK(artifact_duration_ms > 0),
    artifact_source_duration_delta_ms INTEGER NOT NULL,
    max_segment_source_end_ms INTEGER NOT NULL CHECK(max_segment_source_end_ms >= 0),
    source_boundary_overrun_ms INTEGER NOT NULL CHECK(source_boundary_overrun_ms >= 0),
    plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256) = 64),
    imported_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    CHECK(artifact_source_duration_delta_ms =
          artifact_duration_ms - (source_end_ms - source_start_ms)),
    CHECK(source_boundary_overrun_ms = max(0, max_segment_source_end_ms - source_end_ms))
);

-- The full-text index is private operational state.  The authoritative text and
-- coordinates remain in rendition_local_transcript_segments.
CREATE VIRTUAL TABLE rendition_local_transcript_fts USING fts5(
    local_segment_id UNINDEXED,
    text,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER rendition_local_segments_fts_insert
AFTER INSERT ON rendition_local_transcript_segments
BEGIN
    INSERT INTO rendition_local_transcript_fts(local_segment_id, text)
    VALUES(NEW.local_segment_id, NEW.text);
END;

-- Evidence, machine transcript rows, and transform hypotheses are append-only.
CREATE TRIGGER rendition_local_revisions_no_update
BEFORE UPDATE ON rendition_local_transcript_revisions
BEGIN SELECT RAISE(ABORT, 'rendition-local transcript revisions are append-only'); END;
CREATE TRIGGER rendition_local_revisions_no_delete
BEFORE DELETE ON rendition_local_transcript_revisions
BEGIN SELECT RAISE(ABORT, 'rendition-local transcript revisions are append-only'); END;
CREATE TRIGGER rendition_local_segments_no_update
BEFORE UPDATE ON rendition_local_transcript_segments
BEGIN SELECT RAISE(ABORT, 'rendition-local transcript segments are append-only'); END;
CREATE TRIGGER rendition_local_segments_no_delete
BEFORE DELETE ON rendition_local_transcript_segments
BEGIN SELECT RAISE(ABORT, 'rendition-local transcript segments are append-only'); END;
CREATE TRIGGER rendition_local_words_no_update
BEFORE UPDATE ON rendition_local_transcript_words
BEGIN SELECT RAISE(ABORT, 'rendition-local transcript words are append-only'); END;
CREATE TRIGGER rendition_local_words_no_delete
BEFORE DELETE ON rendition_local_transcript_words
BEGIN SELECT RAISE(ABORT, 'rendition-local transcript words are append-only'); END;
CREATE TRIGGER source_recording_transform_candidates_no_update
BEFORE UPDATE ON source_recording_transform_candidates
BEGIN SELECT RAISE(ABORT, 'source/recording transform candidates are append-only'); END;
CREATE TRIGGER source_recording_transform_candidates_no_delete
BEFORE DELETE ON source_recording_transform_candidates
BEGIN SELECT RAISE(ABORT, 'source/recording transform candidates are append-only'); END;
CREATE TRIGGER rendition_local_asr_imports_no_update
BEFORE UPDATE ON rendition_local_asr_imports
BEGIN SELECT RAISE(ABORT, 'rendition-local ASR imports are append-only'); END;
CREATE TRIGGER rendition_local_asr_imports_no_delete
BEFORE DELETE ON rendition_local_asr_imports
BEGIN SELECT RAISE(ABORT, 'rendition-local ASR imports are append-only'); END;

-- These private coordinate objects can never be made public by a generic decision.
CREATE TRIGGER rendition_local_publication_forbidden
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type IN (
    'rendition_local_transcript_revision',
    'rendition_local_transcript_segment',
    'rendition_local_transcript_word',
    'rendition_local_asr_import',
    'source_recording_transform_candidate'
)
BEGIN
    SELECT RAISE(ABORT, 'rendition-local coordinate objects have no publication lane');
END;

CREATE TRIGGER rendition_local_publication_gate_forbidden
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type IN (
    'rendition_local_transcript_revision',
    'rendition_local_transcript_segment',
    'rendition_local_transcript_word',
    'rendition_local_asr_import',
    'source_recording_transform_candidate'
)
BEGIN
    SELECT RAISE(ABORT, 'rendition-local coordinate objects have no publication lane');
END;
