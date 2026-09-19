-- Close direct-SQL and post-admission mutation gaps in the v2 lane.  Migration 0014
-- remains immutable; this migration replaces only its admission trigger and adds
-- guards around the generic/upstream rows on which the v2 subtype depends.

DROP TRIGGER audio_fingerprint_match_candidates_v2_admission;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_admission
BEFORE INSERT ON audio_fingerprint_match_candidates_v2
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM match_candidates AS candidate
        JOIN processing_runs AS comparison_run
          ON comparison_run.processing_run_id = NEW.processing_run_id
        JOIN audio_fingerprint_result_imports AS query_receipt
          ON query_receipt.result_kind = 'extraction'
         AND query_receipt.result_sha256 = NEW.query_result_sha256
         AND query_receipt.processing_run_id = NEW.query_extraction_run_id
        JOIN processing_runs AS query_run
          ON query_run.processing_run_id = NEW.query_extraction_run_id
        JOIN fingerprints AS query_fingerprint
          ON query_fingerprint.fingerprint_id = NEW.query_fingerprint_id
        JOIN audio_fingerprint_observations AS query_typed
          ON query_typed.fingerprint_id = NEW.query_fingerprint_id
        JOIN observations AS query_observation
          ON query_observation.observation_id = query_typed.observation_id
        JOIN artifacts AS query_artifact
          ON query_artifact.artifact_id = query_typed.artifact_id
        JOIN renditions AS query_rendition
          ON query_rendition.rendition_id = query_observation.rendition_id
        JOIN audio_fingerprint_result_imports AS candidate_receipt
          ON candidate_receipt.result_kind = 'extraction'
         AND candidate_receipt.result_sha256 = NEW.candidate_result_sha256
         AND candidate_receipt.processing_run_id = NEW.candidate_extraction_run_id
        JOIN processing_runs AS candidate_run
          ON candidate_run.processing_run_id = NEW.candidate_extraction_run_id
        JOIN fingerprints AS candidate_fingerprint
          ON candidate_fingerprint.fingerprint_id = NEW.candidate_fingerprint_id
        JOIN audio_fingerprint_observations AS candidate_typed
          ON candidate_typed.fingerprint_id = NEW.candidate_fingerprint_id
        JOIN observations AS candidate_observation
          ON candidate_observation.observation_id = candidate_typed.observation_id
        JOIN artifacts AS candidate_artifact
          ON candidate_artifact.artifact_id = candidate_typed.artifact_id
        JOIN renditions AS candidate_rendition
          ON candidate_rendition.rendition_id = candidate_observation.rendition_id
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
          AND comparison_run.stage = 'audio_fingerprint_exact_compare_v2'
          AND comparison_run.implementation_version = '0.2.0'
          AND comparison_run.status = 'completed'
          AND json_extract(comparison_run.parameters_json, '$.method') = 'exact_raw_bytes_v2'
          AND json_extract(
                  comparison_run.parameters_json, '$.compatibility_contract'
              ) = 'engine_build_algorithm_format_normalization_v2'
          AND json_extract(
                  comparison_run.environment_json,
                  '$.query_extraction_result_sha256'
              ) = NEW.query_result_sha256
          AND json_extract(
                  comparison_run.environment_json,
                  '$.candidate_extraction_result_sha256'
              ) = NEW.candidate_result_sha256
          AND json_extract(
                  comparison_run.environment_json, '$.query_engine'
              ) = json_extract(
                  comparison_run.environment_json, '$.candidate_engine'
              )
          AND query_run.stage = 'audio_fingerprint_chromaprint'
          AND query_run.status = 'completed'
          AND query_receipt.recipe_id = json_extract(
                  query_run.parameters_json, '$.recipe_id'
              )
          AND query_fingerprint.fingerprint_kind = 'chromaprint_raw'
          AND query_fingerprint.artifact_uri = query_artifact.storage_uri
          AND query_artifact.processing_run_id = NEW.query_extraction_run_id
          AND query_artifact.artifact_kind = 'audio_fingerprint_chromaprint_raw'
          AND query_artifact.visibility = 'private'
          AND query_observation.processing_run_id = NEW.query_extraction_run_id
          AND query_observation.recording_id = query_rendition.recording_id
          AND query_observation.visibility = 'private'
          AND query_observation.review_state = 'machine'
          AND query_fingerprint.media_id = query_rendition.media_id
          AND query_fingerprint.start_ms = query_observation.start_ms
          AND query_fingerprint.end_ms = query_observation.end_ms
          AND query_observation.recording_id = json_extract(
                  query_receipt.catalog_context_json, '$.recording_id'
              )
          AND query_observation.rendition_id = json_extract(
                  query_receipt.catalog_context_json, '$.rendition_id'
              )
          AND candidate_run.stage = 'audio_fingerprint_chromaprint'
          AND candidate_run.status = 'completed'
          AND candidate_receipt.recipe_id = json_extract(
                  candidate_run.parameters_json, '$.recipe_id'
              )
          AND candidate_fingerprint.fingerprint_kind = 'chromaprint_raw'
          AND candidate_fingerprint.artifact_uri = candidate_artifact.storage_uri
          AND candidate_artifact.processing_run_id = NEW.candidate_extraction_run_id
          AND candidate_artifact.artifact_kind = 'audio_fingerprint_chromaprint_raw'
          AND candidate_artifact.visibility = 'private'
          AND candidate_observation.processing_run_id = NEW.candidate_extraction_run_id
          AND candidate_observation.recording_id = candidate_rendition.recording_id
          AND candidate_observation.visibility = 'private'
          AND candidate_observation.review_state = 'machine'
          AND candidate_fingerprint.media_id = candidate_rendition.media_id
          AND candidate_fingerprint.start_ms = candidate_observation.start_ms
          AND candidate_fingerprint.end_ms = candidate_observation.end_ms
          AND candidate_observation.recording_id = json_extract(
                  candidate_receipt.catalog_context_json, '$.recording_id'
              )
          AND candidate_observation.rendition_id = json_extract(
                  candidate_receipt.catalog_context_json, '$.rendition_id'
              )
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
          AND EXISTS (
              SELECT 1 FROM run_inputs AS query_input
              WHERE query_input.processing_run_id = NEW.processing_run_id
                AND query_input.object_type = 'fingerprint'
                AND query_input.object_id = NEW.query_fingerprint_id
                AND query_input.input_role = 'query'
                AND query_input.input_sha256 = query_artifact.sha256
          )
          AND EXISTS (
              SELECT 1 FROM run_inputs AS candidate_input
              WHERE candidate_input.processing_run_id = NEW.processing_run_id
                AND candidate_input.object_type = 'fingerprint'
                AND candidate_input.object_id = NEW.candidate_fingerprint_id
                AND candidate_input.input_role = 'candidate'
                AND candidate_input.input_sha256 = candidate_artifact.sha256
          )
    ) THEN RAISE(ABORT, 'invalid audio fingerprint v2 match candidate dependency') END;
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_parent_no_update
BEFORE UPDATE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE typed.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 generic match evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_parent_no_delete
BEFORE DELETE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE typed.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 generic match evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_run_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE OLD.processing_run_id IN (
        typed.processing_run_id,
        typed.query_extraction_run_id,
        typed.candidate_extraction_run_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 run evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_run_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE OLD.processing_run_id IN (
        typed.processing_run_id,
        typed.query_extraction_run_id,
        typed.candidate_extraction_run_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 run evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE typed.processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 run inputs are append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE typed.processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 run inputs are append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_fingerprint_no_update
BEFORE UPDATE ON fingerprints
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE OLD.fingerprint_id IN (
        typed.query_fingerprint_id, typed.candidate_fingerprint_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 fingerprint evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_fingerprint_no_delete
BEFORE DELETE ON fingerprints
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE OLD.fingerprint_id IN (
        typed.query_fingerprint_id, typed.candidate_fingerprint_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 fingerprint evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_artifact_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_match_candidates_v2 AS pair
    JOIN audio_fingerprint_observations AS typed
      ON typed.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE typed.artifact_id = OLD.artifact_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 artifact evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_artifact_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_match_candidates_v2 AS pair
    JOIN audio_fingerprint_observations AS typed
      ON typed.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE typed.artifact_id = OLD.artifact_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 artifact evidence is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_observation_no_update
BEFORE UPDATE ON observations
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_observations AS typed
    JOIN audio_fingerprint_match_candidates_v2 AS pair
      ON typed.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE typed.observation_id = OLD.observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation lineage is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_observation_no_delete
BEFORE DELETE ON observations
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_observations AS typed
    JOIN audio_fingerprint_match_candidates_v2 AS pair
      ON typed.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE typed.observation_id = OLD.observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation lineage is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_rendition_no_update
BEFORE UPDATE ON renditions
WHEN EXISTS (
    SELECT 1
    FROM observations AS observation
    JOIN audio_fingerprint_observations AS typed
      ON typed.observation_id = observation.observation_id
    JOIN audio_fingerprint_match_candidates_v2 AS pair
      ON typed.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE observation.rendition_id = OLD.rendition_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 rendition lineage is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_rendition_no_delete
BEFORE DELETE ON renditions
WHEN EXISTS (
    SELECT 1
    FROM observations AS observation
    JOIN audio_fingerprint_observations AS typed
      ON typed.observation_id = observation.observation_id
    JOIN audio_fingerprint_match_candidates_v2 AS pair
      ON typed.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE observation.rendition_id = OLD.rendition_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 rendition lineage is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_no_update
BEFORE UPDATE ON media_objects
WHEN EXISTS (
    SELECT 1
    FROM fingerprints AS fingerprint
    JOIN audio_fingerprint_match_candidates_v2 AS pair
      ON fingerprint.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE fingerprint.media_id = OLD.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media lineage is append-only');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_no_delete
BEFORE DELETE ON media_objects
WHEN EXISTS (
    SELECT 1
    FROM fingerprints AS fingerprint
    JOIN audio_fingerprint_match_candidates_v2 AS pair
      ON fingerprint.fingerprint_id IN (
          pair.query_fingerprint_id, pair.candidate_fingerprint_id
      )
    WHERE fingerprint.media_id = OLD.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media lineage is append-only');
END;
