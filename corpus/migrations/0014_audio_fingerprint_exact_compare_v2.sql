-- Version 2 is a separate append-only admission lane.  The historical v1 subtype,
-- its single-method CHECK, and its admission trigger remain unchanged.

CREATE TABLE audio_fingerprint_match_candidates_v2 (
    match_candidate_id TEXT PRIMARY KEY
        REFERENCES match_candidates(match_candidate_id) ON DELETE CASCADE,
    processing_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    query_extraction_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    candidate_extraction_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    query_result_sha256 TEXT NOT NULL CHECK(length(query_result_sha256) = 64),
    candidate_result_sha256 TEXT NOT NULL CHECK(length(candidate_result_sha256) = 64),
    query_fingerprint_id TEXT NOT NULL REFERENCES fingerprints(fingerprint_id),
    candidate_fingerprint_id TEXT NOT NULL REFERENCES fingerprints(fingerprint_id),
    comparison_method TEXT NOT NULL CHECK(comparison_method = 'exact_raw_bytes_v2'),
    score_semantics TEXT NOT NULL
        CHECK(score_semantics = 'boolean_raw_byte_equality_not_probability'),
    calibration_state TEXT NOT NULL CHECK(calibration_state = 'not_calibrated'),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL DEFAULT 'none' CHECK(publication_authority = 'none'),
    CHECK(query_fingerprint_id <> candidate_fingerprint_id),
    UNIQUE(processing_run_id, query_fingerprint_id, candidate_fingerprint_id)
);

CREATE INDEX audio_fingerprint_match_v2_pair_idx
    ON audio_fingerprint_match_candidates_v2(
        query_fingerprint_id, candidate_fingerprint_id
    );

CREATE TRIGGER audio_fingerprint_match_candidates_v2_no_update
BEFORE UPDATE ON audio_fingerprint_match_candidates_v2
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 match evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_no_delete
BEFORE DELETE ON audio_fingerprint_match_candidates_v2
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 match evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_admission
BEFORE INSERT ON audio_fingerprint_match_candidates_v2
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM match_candidates AS candidate
        JOIN processing_runs AS run
          ON run.processing_run_id = NEW.processing_run_id
        WHERE candidate.match_candidate_id = NEW.match_candidate_id
          AND candidate.left_object_type = 'fingerprint'
          AND candidate.left_object_id = NEW.query_fingerprint_id
          AND candidate.right_object_type = 'fingerprint'
          AND candidate.right_object_id = NEW.candidate_fingerprint_id
          AND candidate.match_method = 'chromaprint_exact_raw_bytes_v2'
          AND candidate.raw_score IN (0.0, 1.0)
          AND candidate.calibrated_probability IS NULL
          AND candidate.decision_state = 'candidate'
          AND json_extract(candidate.metadata_json, '$.calibration_state') = 'not_calibrated'
          AND json_extract(candidate.metadata_json, '$.requires_human_review') = 1
          AND json_extract(candidate.metadata_json, '$.relationship_asserted') = 0
          AND json_extract(candidate.metadata_json, '$.visibility') = 'private'
          AND json_extract(candidate.metadata_json, '$.publication_authority') = 'none'
          AND run.stage = 'audio_fingerprint_exact_compare_v2'
          AND run.status = 'completed'
          AND EXISTS (
              SELECT 1 FROM run_inputs AS query_result
              WHERE query_result.processing_run_id = NEW.processing_run_id
                AND query_result.object_type = 'audio_fingerprint_extraction_result'
                AND query_result.object_id = NEW.query_extraction_run_id
                AND query_result.input_role = 'query_result'
                AND query_result.input_sha256 = NEW.query_result_sha256
          )
          AND EXISTS (
              SELECT 1 FROM run_inputs AS candidate_result
              WHERE candidate_result.processing_run_id = NEW.processing_run_id
                AND candidate_result.object_type = 'audio_fingerprint_extraction_result'
                AND candidate_result.object_id = NEW.candidate_extraction_run_id
                AND candidate_result.input_role = 'candidate_result'
                AND candidate_result.input_sha256 = NEW.candidate_result_sha256
          )
    ) THEN RAISE(ABORT, 'invalid audio fingerprint v2 match candidate dependency') END;
END;
