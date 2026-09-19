-- Additive admission receipts for sealed faster-whisper GPU v3 results.
--
-- Transcript text continues to live only in the existing private media-local lane.
-- This table records the exact producer/work-order/artifact ancestry without
-- creating recording coordinates, identity assertions, event evidence, or a
-- publication object type.

CREATE TABLE migration_0034_gpu_v3_publication_preflight_guard(
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1)
);

CREATE TRIGGER migration_0034_gpu_v3_publication_preflight_abort
BEFORE INSERT ON migration_0034_gpu_v3_publication_preflight_guard
WHEN EXISTS (
        SELECT 1 FROM publication_decisions
        WHERE object_type = 'private_gpu_v3_asr_import'
    )
 OR EXISTS (
        SELECT 1 FROM publication_gate_decisions
        WHERE object_type = 'private_gpu_v3_asr_import'
    )
BEGIN
    SELECT RAISE(ABORT, 'reserved private GPU v3 ASR publication state already exists');
END;

INSERT INTO migration_0034_gpu_v3_publication_preflight_guard(singleton)
VALUES(1);
DROP TRIGGER migration_0034_gpu_v3_publication_preflight_abort;
DROP TABLE migration_0034_gpu_v3_publication_preflight_guard;

CREATE TABLE private_gpu_v3_asr_imports (
    receipt_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    private_gpu_v3_asr_import_id TEXT NOT NULL UNIQUE,
    import_batch_id TEXT NOT NULL UNIQUE
        REFERENCES import_batches(import_batch_id) ON DELETE RESTRICT,
    media_local_revision_id TEXT NOT NULL UNIQUE
        REFERENCES media_local_transcript_revisions(media_local_revision_id)
        ON DELETE RESTRICT,
    processing_run_id TEXT NOT NULL UNIQUE
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,

    result_uri TEXT NOT NULL,
    result_raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(result_raw_sha256) = 64),
    result_identity_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(result_identity_sha256) = 64),
    result_byte_count INTEGER NOT NULL CHECK(result_byte_count > 0),
    result_id TEXT NOT NULL UNIQUE,
    result_key TEXT NOT NULL UNIQUE CHECK(length(result_key) = 64),

    work_order_uri TEXT NOT NULL,
    work_order_raw_sha256 TEXT NOT NULL CHECK(length(work_order_raw_sha256) = 64),
    work_order_identity_sha256 TEXT NOT NULL
        CHECK(length(work_order_identity_sha256) = 64),
    work_order_byte_count INTEGER NOT NULL CHECK(work_order_byte_count > 0),
    work_order_id TEXT NOT NULL,

    raw_transcript_artifact_id TEXT NOT NULL UNIQUE
        REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
    raw_transcript_identity_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(raw_transcript_identity_sha256) = 64),
    normalized_transcript_artifact_id TEXT NOT NULL UNIQUE
        REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
    normalized_transcript_identity_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(normalized_transcript_identity_sha256) = 64),

    input_media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE RESTRICT,
    input_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
    input_duration_ms INTEGER NOT NULL CHECK(input_duration_ms > 0),
    segment_count INTEGER NOT NULL CHECK(segment_count >= 0),
    word_count INTEGER NOT NULL CHECK(word_count >= 0),
    anomalous_word_count INTEGER NOT NULL
        CHECK(anomalous_word_count >= 0 AND anomalous_word_count <= word_count),
    timing_anomaly_flag_count INTEGER NOT NULL
        CHECK(timing_anomaly_flag_count >= anomalous_word_count),

    execution_mode TEXT NOT NULL CHECK(execution_mode IN ('unasserted', 'batch')),
    batch_completion_uri TEXT,
    batch_completion_raw_sha256 TEXT
        CHECK(batch_completion_raw_sha256 IS NULL OR length(batch_completion_raw_sha256) = 64),
    batch_completion_identity_sha256 TEXT
        CHECK(batch_completion_identity_sha256 IS NULL OR length(batch_completion_identity_sha256) = 64),
    batch_completion_id TEXT,
    gpu_batch_id TEXT,
    gpu_batch_ordinal INTEGER CHECK(gpu_batch_ordinal IS NULL OR gpu_batch_ordinal > 0),

    coordinate_system TEXT NOT NULL CHECK(coordinate_system = 'media_ms'),
    boundary TEXT NOT NULL CHECK(boundary = 'half_open'),
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    review_state TEXT NOT NULL CHECK(review_state = 'machine'),
    human_review TEXT NOT NULL CHECK(human_review = 'required'),
    score_calibration TEXT NOT NULL CHECK(score_calibration = 'not_calibrated'),
    speaker_assignment TEXT NOT NULL CHECK(speaker_assignment = 'none'),
    identity_authority TEXT NOT NULL CHECK(identity_authority = 'none'),
    biometric_authority TEXT NOT NULL CHECK(biometric_authority = 'none'),
    event_authority TEXT NOT NULL CHECK(event_authority = 'none'),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    wiki_authority TEXT NOT NULL CHECK(wiki_authority = 'none'),
    export_authority TEXT NOT NULL CHECK(export_authority = 'none'),
    recording_coordinate_state TEXT NOT NULL
        CHECK(recording_coordinate_state = 'unasserted_catalog_context_null'),
    source_coordinate_state TEXT NOT NULL
        CHECK(source_coordinate_state = 'unasserted_catalog_context_null'),
    plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256) = 64),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),

    CHECK(
        (execution_mode = 'unasserted'
         AND batch_completion_uri IS NULL
         AND batch_completion_raw_sha256 IS NULL
         AND batch_completion_identity_sha256 IS NULL
         AND batch_completion_id IS NULL
         AND gpu_batch_id IS NULL
         AND gpu_batch_ordinal IS NULL)
        OR
        (execution_mode = 'batch'
         AND batch_completion_uri IS NOT NULL
         AND batch_completion_raw_sha256 IS NOT NULL
         AND batch_completion_identity_sha256 IS NOT NULL
         AND batch_completion_id IS NOT NULL
         AND gpu_batch_id IS NOT NULL
         AND gpu_batch_ordinal IS NOT NULL)
    )
);

CREATE INDEX private_gpu_v3_asr_input_idx
    ON private_gpu_v3_asr_imports(input_media_id, imported_at);

-- A receipt can only bind the exact private media-local revision and artifacts
-- inserted in the same producer run.
CREATE TRIGGER private_gpu_v3_asr_receipt_exact_lineage
BEFORE INSERT ON private_gpu_v3_asr_imports
WHEN NOT EXISTS (
    SELECT 1
    FROM media_local_transcript_revisions AS revision
    JOIN artifacts AS raw_artifact
      ON raw_artifact.artifact_id = NEW.raw_transcript_artifact_id
    JOIN artifacts AS normalized_artifact
      ON normalized_artifact.artifact_id = NEW.normalized_transcript_artifact_id
    WHERE revision.media_local_revision_id = NEW.media_local_revision_id
      AND revision.processing_run_id = NEW.processing_run_id
      AND revision.media_id = NEW.input_media_id
      AND revision.input_artifact_id = NEW.input_artifact_id
      AND revision.coordinate_system = 'media_ms'
      AND revision.source_coordinate_state = 'unasserted_catalog_context_null'
      AND revision.recording_coordinate_state = 'unasserted_catalog_context_null'
      AND revision.review_state = 'machine'
      AND raw_artifact.processing_run_id = NEW.processing_run_id
      AND raw_artifact.artifact_kind = 'faster_whisper_raw_transcript_json'
      AND raw_artifact.visibility = 'private'
      AND normalized_artifact.processing_run_id = NEW.processing_run_id
      AND normalized_artifact.artifact_kind = 'transcript_normalized_json'
      AND normalized_artifact.visibility = 'private'
      AND (SELECT count(*) FROM media_local_transcript_segments
           WHERE media_local_revision_id = NEW.media_local_revision_id)
          = NEW.segment_count
      AND (SELECT count(*)
           FROM media_local_transcript_words AS word
           JOIN media_local_transcript_segments AS segment
             ON segment.media_local_segment_id = word.media_local_segment_id
           WHERE segment.media_local_revision_id = NEW.media_local_revision_id)
          = NEW.word_count
)
BEGIN
    SELECT RAISE(ABORT, 'private GPU v3 ASR receipt lineage is not exact');
END;

CREATE TRIGGER private_gpu_v3_asr_imports_no_update
BEFORE UPDATE ON private_gpu_v3_asr_imports
BEGIN SELECT RAISE(ABORT, 'private GPU v3 ASR imports are append-only'); END;

CREATE TRIGGER private_gpu_v3_asr_imports_no_delete
BEFORE DELETE ON private_gpu_v3_asr_imports
BEGIN SELECT RAISE(ABORT, 'private GPU v3 ASR imports are append-only'); END;

CREATE TRIGGER private_gpu_v3_asr_publication_forbidden
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type = 'private_gpu_v3_asr_import'
BEGIN
    SELECT RAISE(ABORT, 'private GPU v3 ASR imports have no publication lane');
END;

CREATE TRIGGER private_gpu_v3_asr_publication_gate_forbidden
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type = 'private_gpu_v3_asr_import'
BEGIN
    SELECT RAISE(ABORT, 'private GPU v3 ASR imports have no publication lane');
END;
