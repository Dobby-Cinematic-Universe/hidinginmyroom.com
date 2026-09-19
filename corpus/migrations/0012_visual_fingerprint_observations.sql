-- Private sparse visual-fingerprint evidence.  A 64-bit perceptual hash is a
-- review-routing descriptor, not an identity, duplicate, parent, ownership, or
-- recording-relationship decision.  Nothing in this migration feeds a public view.

CREATE TABLE visual_fingerprint_observations (
    observation_id TEXT PRIMARY KEY
        REFERENCES observations(observation_id) ON DELETE CASCADE,
    fingerprint_id TEXT NOT NULL UNIQUE REFERENCES fingerprints(fingerprint_id),
    artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    sample_id TEXT NOT NULL,
    window_id TEXT NOT NULL,
    requested_timestamp_ms INTEGER NOT NULL CHECK(requested_timestamp_ms >= 0),
    timestamp_kind TEXT NOT NULL CHECK(timestamp_kind IN ('explicit', 'keyframe')),
    decoded_pts INTEGER NOT NULL CHECK(decoded_pts >= 0),
    decoded_duration_pts INTEGER NOT NULL CHECK(decoded_duration_pts > 0),
    time_base_numerator INTEGER NOT NULL CHECK(time_base_numerator > 0),
    time_base_denominator INTEGER NOT NULL CHECK(time_base_denominator > 0),
    absolute_timestamp_us INTEGER NOT NULL CHECK(absolute_timestamp_us >= 0),
    relative_timestamp_us INTEGER NOT NULL CHECK(relative_timestamp_us >= 0),
    relative_timestamp_ms INTEGER NOT NULL CHECK(relative_timestamp_ms >= 0),
    decoded_duration_us INTEGER NOT NULL CHECK(decoded_duration_us > 0),
    is_keyframe INTEGER NOT NULL CHECK(is_keyframe IN (0, 1)),
    timestamp_drift_us INTEGER NOT NULL CHECK(timestamp_drift_us >= 0),
    algorithm TEXT NOT NULL CHECK(algorithm = 'fixed_q20_dct_phash_8x8_v1'),
    phash_bits INTEGER NOT NULL CHECK(phash_bits = 64),
    phash_hex TEXT NOT NULL
        CHECK(length(phash_hex) = 16)
        CHECK(phash_hex = lower(phash_hex))
        CHECK(phash_hex NOT GLOB '*[^0-9a-f]*'),
    phash_popcount INTEGER NOT NULL CHECK(phash_popcount BETWEEN 0 AND 64),
    exact_gray_sha256 TEXT NOT NULL
        CHECK(length(exact_gray_sha256) = 64)
        CHECK(exact_gray_sha256 = lower(exact_gray_sha256))
        CHECK(exact_gray_sha256 NOT GLOB '*[^0-9a-f]*'),
    calibration_state TEXT NOT NULL CHECK(calibration_state = 'not_calibrated'),
    calibrated_probability REAL CHECK(calibrated_probability IS NULL),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    identity_asserted INTEGER NOT NULL DEFAULT 0 CHECK(identity_asserted = 0),
    duplicate_asserted INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_asserted = 0),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    quality_flags_json TEXT NOT NULL CHECK(json_valid(quality_flags_json)),
    UNIQUE(observation_id, fingerprint_id)
);

CREATE TABLE visual_fingerprint_result_imports (
    import_batch_id TEXT PRIMARY KEY,
    result_sha256 TEXT NOT NULL
        CHECK(length(result_sha256) = 64)
        CHECK(result_sha256 = lower(result_sha256))
        CHECK(result_sha256 NOT GLOB '*[^0-9a-f]*'),
    processing_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    recipe_id TEXT NOT NULL,
    catalog_context_json TEXT NOT NULL CHECK(json_valid(catalog_context_json)),
    imported_at TEXT NOT NULL,
    UNIQUE(result_sha256)
);

CREATE INDEX visual_fingerprint_observation_fingerprint_idx
    ON visual_fingerprint_observations(fingerprint_id);
CREATE INDEX visual_fingerprint_observation_phash_idx
    ON visual_fingerprint_observations(phash_hex);

-- Raw machine evidence and its import receipt are immutable.  Human review must
-- create separate review/decision rows; it must never rewrite these measurements.
CREATE TRIGGER visual_fingerprint_observations_no_update
BEFORE UPDATE ON visual_fingerprint_observations
BEGIN
    SELECT RAISE(ABORT, 'visual fingerprint observations are append-only');
END;

CREATE TRIGGER visual_fingerprint_observations_no_delete
BEFORE DELETE ON visual_fingerprint_observations
BEGIN
    SELECT RAISE(ABORT, 'visual fingerprint observations are append-only');
END;

CREATE TRIGGER visual_fingerprint_result_imports_no_update
BEFORE UPDATE ON visual_fingerprint_result_imports
BEGIN
    SELECT RAISE(ABORT, 'visual fingerprint result imports are append-only');
END;

CREATE TRIGGER visual_fingerprint_result_imports_no_delete
BEFORE DELETE ON visual_fingerprint_result_imports
BEGIN
    SELECT RAISE(ABORT, 'visual fingerprint result imports are append-only');
END;

CREATE TRIGGER visual_fingerprint_result_imports_admission
BEFORE INSERT ON visual_fingerprint_result_imports
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM processing_runs AS run
        WHERE run.processing_run_id = NEW.processing_run_id
          AND run.stage = 'visual_fingerprint_extract'
          AND run.status = 'completed'
          AND NEW.recipe_id GLOB 'recipe_visual_fingerprint_[0-9a-f]*'
    ) THEN RAISE(ABORT, 'invalid visual fingerprint import dependency') END;
END;

-- The subtype can only wrap a completed private machine observation whose exact
-- rendition/media, fingerprint value, sealed frame artifact, run, and timing agree.
CREATE TRIGGER visual_fingerprint_observations_admission
BEFORE INSERT ON visual_fingerprint_observations
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM observations AS observation
        JOIN processing_runs AS run
          ON run.processing_run_id = observation.processing_run_id
        JOIN renditions AS rendition
          ON rendition.rendition_id = observation.rendition_id
        JOIN fingerprints AS fingerprint
          ON fingerprint.fingerprint_id = NEW.fingerprint_id
        JOIN artifacts AS artifact
          ON artifact.artifact_id = NEW.artifact_id
        WHERE observation.observation_id = NEW.observation_id
          AND observation.observation_kind = 'visual_fingerprint'
          AND observation.visibility = 'private'
          AND observation.review_state = 'machine'
          AND observation.recording_id = rendition.recording_id
          AND fingerprint.media_id = rendition.media_id
          AND fingerprint.fingerprint_kind = 'fixed_q20_dct_phash_8x8_v1'
          AND fingerprint.start_ms = observation.start_ms
          AND fingerprint.end_ms = observation.end_ms
          AND fingerprint.value_text = NEW.phash_hex
          AND fingerprint.artifact_uri = artifact.storage_uri
          AND artifact.processing_run_id = observation.processing_run_id
          AND artifact.artifact_kind = 'visual_fingerprint_gray32'
          AND artifact.sha256 = NEW.exact_gray_sha256
          AND artifact.byte_count = 1024
          AND artifact.visibility = 'private'
          AND run.stage = 'visual_fingerprint_extract'
          AND run.status = 'completed'
    ) THEN RAISE(ABORT, 'invalid visual fingerprint observation dependency') END;
END;
