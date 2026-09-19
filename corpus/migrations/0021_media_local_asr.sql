-- Private media-local transcript admission for catalog-free ASR results.
--
-- These results intentionally have no recording/rendition context.  Their times
-- belong only to the normalized input media and must never enter the recording-
-- scoped transcript or timeline tables without a separate reviewed translation.

CREATE TABLE media_local_transcript_revisions (
    media_local_revision_id TEXT PRIMARY KEY,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    input_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    processing_run_id TEXT NOT NULL UNIQUE REFERENCES processing_runs(processing_run_id),
    revision_kind TEXT NOT NULL
        CHECK(revision_kind IN ('raw_asr', 'contextual_asr')),
    origin TEXT NOT NULL,
    language TEXT NOT NULL,
    glossary_revision_id TEXT REFERENCES glossary_revisions(glossary_revision_id),
    review_state TEXT NOT NULL DEFAULT 'machine'
        CHECK(review_state IN ('machine', 'disputed', 'rejected')),
    coordinate_system TEXT NOT NULL CHECK(coordinate_system = 'media_ms'),
    boundary TEXT NOT NULL CHECK(boundary = 'half_open'),
    input_duration_ms INTEGER NOT NULL CHECK(input_duration_ms > 0),
    requested_start_ms INTEGER NOT NULL CHECK(requested_start_ms = 0),
    requested_end_ms INTEGER NOT NULL CHECK(requested_end_ms > 0),
    max_segment_end_ms INTEGER NOT NULL CHECK(max_segment_end_ms >= 0),
    input_boundary_overrun_ms INTEGER NOT NULL CHECK(input_boundary_overrun_ms >= 0),
    source_coordinate_state TEXT NOT NULL
        CHECK(source_coordinate_state = 'unasserted_catalog_context_null'),
    recording_coordinate_state TEXT NOT NULL
        CHECK(recording_coordinate_state = 'unasserted_catalog_context_null'),
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(media_id, input_artifact_id, processing_run_id),
    CHECK(requested_end_ms - requested_start_ms = input_duration_ms),
    CHECK(input_boundary_overrun_ms = max(0, max_segment_end_ms - requested_end_ms))
);

CREATE INDEX media_local_revisions_media_idx
    ON media_local_transcript_revisions(media_id, input_artifact_id, created_at);

CREATE TABLE media_local_transcript_segments (
    media_local_segment_id TEXT PRIMARY KEY,
    media_local_revision_id TEXT NOT NULL
        REFERENCES media_local_transcript_revisions(media_local_revision_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    media_start_ms INTEGER NOT NULL CHECK(media_start_ms >= 0),
    media_end_ms INTEGER NOT NULL CHECK(media_end_ms > media_start_ms),
    input_boundary_overrun_ms INTEGER NOT NULL CHECK(input_boundary_overrun_ms >= 0),
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
    UNIQUE(media_local_revision_id, ordinal)
);

CREATE INDEX media_local_segments_time_idx
    ON media_local_transcript_segments(
        media_local_revision_id, media_start_ms, media_end_ms
    );

CREATE TABLE media_local_transcript_words (
    media_local_word_id TEXT PRIMARY KEY,
    media_local_segment_id TEXT NOT NULL
        REFERENCES media_local_transcript_segments(media_local_segment_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    media_start_ms INTEGER CHECK(media_start_ms IS NULL OR media_start_ms >= 0),
    media_end_ms INTEGER CHECK(media_end_ms IS NULL OR media_end_ms >= 0),
    token TEXT NOT NULL,
    normalized_token TEXT,
    asr_log_probability REAL,
    alignment_score REAL,
    calibrated_probability REAL
        CHECK(calibrated_probability IS NULL OR
              (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(media_local_segment_id, ordinal),
    CHECK((media_start_ms IS NULL) = (media_end_ms IS NULL)),
    CHECK(media_end_ms IS NULL OR media_end_ms >= media_start_ms)
);

CREATE TABLE media_local_asr_imports (
    media_local_asr_import_id TEXT PRIMARY KEY,
    import_batch_id TEXT NOT NULL UNIQUE REFERENCES import_batches(import_batch_id),
    media_local_revision_id TEXT NOT NULL UNIQUE
        REFERENCES media_local_transcript_revisions(media_local_revision_id),
    asr_result_uri TEXT NOT NULL,
    asr_result_raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(asr_result_raw_sha256) = 64),
    asr_result_canonical_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(asr_result_canonical_sha256) = 64),
    asr_result_byte_count INTEGER NOT NULL CHECK(asr_result_byte_count > 0),
    queue_manifest_uri TEXT NOT NULL,
    queue_manifest_raw_sha256 TEXT NOT NULL CHECK(length(queue_manifest_raw_sha256) = 64),
    queue_identity_sha256 TEXT NOT NULL CHECK(length(queue_identity_sha256) = 64),
    queue_id TEXT NOT NULL,
    queue_ordinal INTEGER NOT NULL CHECK(queue_ordinal > 0),
    routing_hint TEXT NOT NULL CHECK(routing_hint = 'process'),
    work_order_uri TEXT NOT NULL,
    work_order_raw_sha256 TEXT NOT NULL CHECK(length(work_order_raw_sha256) = 64),
    work_order_canonical_sha256 TEXT NOT NULL
        CHECK(length(work_order_canonical_sha256) = 64),
    preprocess_result_uri TEXT NOT NULL,
    preprocess_result_raw_sha256 TEXT NOT NULL
        CHECK(length(preprocess_result_raw_sha256) = 64),
    preprocess_result_canonical_sha256 TEXT NOT NULL
        CHECK(length(preprocess_result_canonical_sha256) = 64),
    preprocess_import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    seal_receipt_uri TEXT NOT NULL,
    seal_receipt_raw_sha256 TEXT NOT NULL CHECK(length(seal_receipt_raw_sha256) = 64),
    seal_receipt_id TEXT NOT NULL,
    seal_receipt_identity_sha256 TEXT NOT NULL
        CHECK(length(seal_receipt_identity_sha256) = 64),
    seal_receipt_ordinal INTEGER NOT NULL CHECK(seal_receipt_ordinal > 0),
    sealed_result_directory_uri TEXT NOT NULL,
    input_media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    input_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    input_duration_ms INTEGER NOT NULL CHECK(input_duration_ms > 0),
    max_segment_end_ms INTEGER NOT NULL CHECK(max_segment_end_ms >= 0),
    input_boundary_overrun_ms INTEGER NOT NULL CHECK(input_boundary_overrun_ms >= 0),
    null_timed_word_count INTEGER NOT NULL CHECK(null_timed_word_count >= 0),
    plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256) = 64),
    imported_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(queue_id, queue_identity_sha256, queue_ordinal),
    CHECK(input_boundary_overrun_ms = max(0, max_segment_end_ms - input_duration_ms))
);

CREATE VIRTUAL TABLE media_local_transcript_fts USING fts5(
    media_local_segment_id UNINDEXED,
    text,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER media_local_segments_fts_insert
AFTER INSERT ON media_local_transcript_segments
BEGIN
    INSERT INTO media_local_transcript_fts(media_local_segment_id, text)
    VALUES(NEW.media_local_segment_id, NEW.text);
END;

CREATE TRIGGER media_local_revisions_no_update
BEFORE UPDATE ON media_local_transcript_revisions
BEGIN SELECT RAISE(ABORT, 'media-local transcript revisions are append-only'); END;
CREATE TRIGGER media_local_revisions_no_delete
BEFORE DELETE ON media_local_transcript_revisions
BEGIN SELECT RAISE(ABORT, 'media-local transcript revisions are append-only'); END;
CREATE TRIGGER media_local_segments_no_update
BEFORE UPDATE ON media_local_transcript_segments
BEGIN SELECT RAISE(ABORT, 'media-local transcript segments are append-only'); END;
CREATE TRIGGER media_local_segments_no_delete
BEFORE DELETE ON media_local_transcript_segments
BEGIN SELECT RAISE(ABORT, 'media-local transcript segments are append-only'); END;
CREATE TRIGGER media_local_words_no_update
BEFORE UPDATE ON media_local_transcript_words
BEGIN SELECT RAISE(ABORT, 'media-local transcript words are append-only'); END;
CREATE TRIGGER media_local_words_no_delete
BEFORE DELETE ON media_local_transcript_words
BEGIN SELECT RAISE(ABORT, 'media-local transcript words are append-only'); END;
CREATE TRIGGER media_local_asr_imports_no_update
BEFORE UPDATE ON media_local_asr_imports
BEGIN SELECT RAISE(ABORT, 'media-local ASR imports are append-only'); END;
CREATE TRIGGER media_local_asr_imports_no_delete
BEFORE DELETE ON media_local_asr_imports
BEGIN SELECT RAISE(ABORT, 'media-local ASR imports are append-only'); END;

CREATE TRIGGER media_local_publication_forbidden
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type IN (
    'media_local_transcript_revision',
    'media_local_transcript_segment',
    'media_local_transcript_word',
    'media_local_asr_import'
)
BEGIN
    SELECT RAISE(ABORT, 'media-local coordinate objects have no publication lane');
END;

CREATE TRIGGER media_local_publication_gate_forbidden
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type IN (
    'media_local_transcript_revision',
    'media_local_transcript_segment',
    'media_local_transcript_word',
    'media_local_asr_import'
)
BEGIN
    SELECT RAISE(ABORT, 'media-local coordinate objects have no publication lane');
END;
