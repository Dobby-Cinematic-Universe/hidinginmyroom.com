CREATE TABLE models (
    model_id TEXT PRIMARY KEY,
    task TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    weights_sha256 TEXT CHECK(weights_sha256 IS NULL OR length(weights_sha256) = 64),
    license_label TEXT,
    configuration_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(configuration_json)),
    UNIQUE(task, name, version, weights_sha256)
);

CREATE TABLE glossary_revisions (
    glossary_revision_id TEXT PRIMARY KEY,
    parent_glossary_revision_id TEXT REFERENCES glossary_revisions(glossary_revision_id),
    sha256 TEXT NOT NULL UNIQUE CHECK(length(sha256) = 64),
    created_at TEXT NOT NULL,
    description TEXT,
    artifact_uri TEXT NOT NULL
);

CREATE TABLE processing_runs (
    processing_run_id TEXT PRIMARY KEY,
    stage TEXT NOT NULL,
    implementation_version TEXT NOT NULL,
    model_id TEXT REFERENCES models(model_id),
    glossary_revision_id TEXT REFERENCES glossary_revisions(glossary_revision_id),
    parameters_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(parameters_json)),
    environment_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(environment_json)),
    random_seed INTEGER,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    error_text TEXT
);

CREATE TABLE run_inputs (
    run_input_id TEXT PRIMARY KEY,
    processing_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id) ON DELETE CASCADE,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    input_role TEXT NOT NULL,
    input_sha256 TEXT CHECK(input_sha256 IS NULL OR length(input_sha256) = 64),
    UNIQUE(processing_run_id, object_type, object_id, input_role)
);

CREATE TABLE artifacts (
    artifact_id TEXT PRIMARY KEY,
    processing_run_id TEXT REFERENCES processing_runs(processing_run_id),
    artifact_kind TEXT NOT NULL,
    storage_uri TEXT NOT NULL,
    sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
    byte_count INTEGER NOT NULL CHECK(byte_count >= 0),
    schema_version INTEGER NOT NULL DEFAULT 1,
    visibility TEXT NOT NULL DEFAULT 'private'
        CHECK(visibility IN ('private', 'review', 'public_candidate', 'public')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(storage_uri, sha256)
);

CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    stage TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'blocked')),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK(max_attempts >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(stage, target_type, target_id)
);

CREATE TABLE job_attempts (
    job_attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL CHECK(attempt_number >= 1),
    processing_run_id TEXT REFERENCES processing_runs(processing_run_id),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'cancelled')),
    error_text TEXT,
    UNIQUE(job_id, attempt_number)
);

CREATE TABLE transcript_revisions (
    revision_id TEXT PRIMARY KEY,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    rendition_id TEXT REFERENCES renditions(rendition_id),
    processing_run_id TEXT REFERENCES processing_runs(processing_run_id),
    revision_kind TEXT NOT NULL
        CHECK(revision_kind IN ('raw_asr', 'contextual_asr', 'human_verbatim', 'readability_edit')),
    origin TEXT NOT NULL,
    language TEXT NOT NULL,
    glossary_revision_id TEXT REFERENCES glossary_revisions(glossary_revision_id),
    review_state TEXT NOT NULL DEFAULT 'machine'
        CHECK(review_state IN ('machine', 'human_corrected', 'media_checked', 'disputed', 'rejected')),
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json))
);

CREATE INDEX transcript_revisions_recording_idx ON transcript_revisions(recording_id, created_at);

CREATE TABLE transcript_revision_parents (
    revision_id TEXT NOT NULL REFERENCES transcript_revisions(revision_id) ON DELETE CASCADE,
    parent_revision_id TEXT NOT NULL REFERENCES transcript_revisions(revision_id),
    relation_kind TEXT NOT NULL DEFAULT 'derived_from',
    PRIMARY KEY(revision_id, parent_revision_id),
    CHECK(revision_id <> parent_revision_id)
);

CREATE TABLE transcript_segments (
    segment_id TEXT PRIMARY KEY,
    revision_id TEXT NOT NULL REFERENCES transcript_revisions(revision_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    start_ms INTEGER NOT NULL CHECK(start_ms >= 0),
    end_ms INTEGER NOT NULL CHECK(end_ms > start_ms),
    text TEXT NOT NULL,
    normalized_text TEXT,
    speaker_label TEXT,
    language TEXT,
    confidence_band TEXT CHECK(confidence_band IS NULL OR confidence_band IN ('low', 'medium', 'high', 'human')),
    calibrated_probability REAL CHECK(calibrated_probability IS NULL OR (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(revision_id, ordinal)
);

CREATE INDEX transcript_segments_time_idx ON transcript_segments(revision_id, start_ms, end_ms);

CREATE TABLE transcript_words (
    word_id TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL REFERENCES transcript_segments(segment_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    start_ms INTEGER CHECK(start_ms IS NULL OR start_ms >= 0),
    end_ms INTEGER CHECK(end_ms IS NULL OR end_ms >= 0),
    token TEXT NOT NULL,
    normalized_token TEXT,
    asr_log_probability REAL,
    alignment_score REAL,
    calibrated_probability REAL CHECK(calibrated_probability IS NULL OR (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    CHECK(end_ms IS NULL OR start_ms IS NULL OR end_ms >= start_ms),
    UNIQUE(segment_id, ordinal)
);

