CREATE TRIGGER audio_fingerprint_observations_admission
BEFORE INSERT ON audio_fingerprint_observations
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM observations AS observation
        JOIN fingerprints AS fingerprint
          ON fingerprint.fingerprint_id = NEW.fingerprint_id
        JOIN artifacts AS artifact
          ON artifact.artifact_id = NEW.artifact_id
        JOIN renditions AS rendition
          ON rendition.rendition_id = observation.rendition_id
        WHERE observation.observation_id = NEW.observation_id
          AND observation.observation_kind = 'audio_fingerprint'
          AND observation.visibility = 'private'
          AND observation.review_state = 'machine'
          AND observation.processing_run_id IS NOT NULL
          AND observation.recording_id = rendition.recording_id
          AND fingerprint.media_id = rendition.media_id
          AND fingerprint.start_ms = observation.start_ms
          AND fingerprint.end_ms = observation.end_ms
          AND artifact.processing_run_id = observation.processing_run_id
          AND artifact.visibility = 'private'
          AND artifact.artifact_kind = 'audio_fingerprint_chromaprint_raw'
    ) THEN RAISE(ABORT, 'invalid audio fingerprint observation dependency') END;
END;

CREATE TRIGGER audio_fingerprint_match_candidates_admission
BEFORE INSERT ON audio_fingerprint_match_candidates
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
          AND candidate.match_method = 'chromaprint_exact_raw_bytes_v1'
          AND candidate.raw_score IN (0.0, 1.0)
          AND candidate.calibrated_probability IS NULL
          AND candidate.decision_state = 'candidate'
          AND run.stage = 'audio_fingerprint_exact_compare'
          AND run.status = 'completed'
    ) THEN RAISE(ABORT, 'invalid audio fingerprint match candidate dependency') END;
END;
