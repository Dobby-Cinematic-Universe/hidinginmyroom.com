-- Private, machine-generated audio-fingerprint evidence.  These subtype tables do
-- not feed any public view and deliberately cannot encode an accepted identity.

CREATE TABLE audio_fingerprint_observations (
    observation_id TEXT PRIMARY KEY REFERENCES observations(observation_id) ON DELETE CASCADE,
    fingerprint_id TEXT NOT NULL REFERENCES fingerprints(fingerprint_id),
    artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    window_kind TEXT NOT NULL
        CHECK(window_kind IN ('full_track', 'explicit_window', 'fixed_chunk', 'partial_tail_chunk')),
    algorithm INTEGER NOT NULL CHECK(algorithm >= 0),
    raw_format TEXT NOT NULL CHECK(raw_format = 'ffmpeg_chromaprint_fp_format_raw'),
    sample_rate_hz INTEGER NOT NULL CHECK(sample_rate_hz = 16000),
    channels INTEGER NOT NULL CHECK(channels = 1),
    fingerprint_word_count INTEGER NOT NULL CHECK(fingerprint_word_count >= 0),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    UNIQUE(observation_id, fingerprint_id)
);

CREATE TABLE audio_fingerprint_match_candidates (
    match_candidate_id TEXT PRIMARY KEY
        REFERENCES match_candidates(match_candidate_id) ON DELETE CASCADE,
    processing_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    query_fingerprint_id TEXT NOT NULL REFERENCES fingerprints(fingerprint_id),
    candidate_fingerprint_id TEXT NOT NULL REFERENCES fingerprints(fingerprint_id),
    comparison_method TEXT NOT NULL CHECK(comparison_method = 'exact_raw_bytes_v1'),
    score_semantics TEXT NOT NULL
        CHECK(score_semantics = 'boolean_raw_byte_equality_not_probability'),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    CHECK(query_fingerprint_id <> candidate_fingerprint_id),
    UNIQUE(processing_run_id, query_fingerprint_id, candidate_fingerprint_id)
);

CREATE TABLE audio_fingerprint_result_imports (
    import_batch_id TEXT PRIMARY KEY,
    result_kind TEXT NOT NULL CHECK(result_kind IN ('extraction', 'exact_comparison')),
    result_sha256 TEXT NOT NULL CHECK(length(result_sha256) = 64),
    processing_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    recipe_id TEXT NOT NULL,
    catalog_context_json TEXT NOT NULL CHECK(json_valid(catalog_context_json)),
    imported_at TEXT NOT NULL,
    UNIQUE(result_sha256, result_kind)
);

CREATE INDEX audio_fingerprint_observation_fingerprint_idx
    ON audio_fingerprint_observations(fingerprint_id);
CREATE INDEX audio_fingerprint_match_pair_idx
    ON audio_fingerprint_match_candidates(query_fingerprint_id, candidate_fingerprint_id);

-- Imported machine evidence is append-only.  Human review is expressed in separate
-- decision/review rows, never by rewriting raw evidence.
CREATE TRIGGER audio_fingerprint_observations_no_update
BEFORE UPDATE ON audio_fingerprint_observations
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint observations are append-only');
END;

CREATE TRIGGER audio_fingerprint_observations_no_delete
BEFORE DELETE ON audio_fingerprint_observations
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint observations are append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_no_update
BEFORE UPDATE ON audio_fingerprint_match_candidates
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint match evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_no_delete
BEFORE DELETE ON audio_fingerprint_match_candidates
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint match evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_result_imports_no_update
BEFORE UPDATE ON audio_fingerprint_result_imports
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint result imports are append-only');
END;

CREATE TRIGGER audio_fingerprint_result_imports_no_delete
BEFORE DELETE ON audio_fingerprint_result_imports
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint result imports are append-only');
END;
