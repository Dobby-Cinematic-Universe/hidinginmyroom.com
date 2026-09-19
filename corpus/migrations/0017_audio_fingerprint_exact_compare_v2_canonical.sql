-- Canonical typed evidence bindings for audio exact-compare v2.
--
-- Rows admitted under the earlier provisional guards do not contain enough typed
-- information to backfill safely.  Fail closed if any exist.  The production catalog
-- had zero v2 rows when this migration was authored.

CREATE TABLE audio_fingerprint_v2_preflight_guard (
    row_count INTEGER NOT NULL CHECK(row_count = 0)
);
INSERT INTO audio_fingerprint_v2_preflight_guard(row_count)
SELECT count(*) FROM audio_fingerprint_match_candidates_v2;
DROP TABLE audio_fingerprint_v2_preflight_guard;

DROP TRIGGER audio_fingerprint_match_candidates_v2_admission;
DROP TRIGGER audio_fingerprint_match_candidates_v2_exact_input_count;

CREATE TABLE audio_fingerprint_compare_v2_receipts (
    comparison_result_sha256 TEXT PRIMARY KEY
        CHECK(length(comparison_result_sha256) = 64
              AND comparison_result_sha256 NOT GLOB '*[^0-9a-f]*'),
    match_candidate_id TEXT NOT NULL UNIQUE
        REFERENCES match_candidates(match_candidate_id) ON DELETE CASCADE,
    processing_run_id TEXT NOT NULL UNIQUE REFERENCES processing_runs(processing_run_id),
    result_path TEXT NOT NULL,
    result_byte_count INTEGER NOT NULL CHECK(result_byte_count > 0),
    recipe_id TEXT NOT NULL,
    recipe_sha256 TEXT NOT NULL
        CHECK(length(recipe_sha256) = 64 AND recipe_sha256 NOT GLOB '*[^0-9a-f]*'),
    query_result_sha256 TEXT NOT NULL
        CHECK(length(query_result_sha256) = 64
              AND query_result_sha256 NOT GLOB '*[^0-9a-f]*'),
    candidate_result_sha256 TEXT NOT NULL
        CHECK(length(candidate_result_sha256) = 64
              AND candidate_result_sha256 NOT GLOB '*[^0-9a-f]*'),
    query_recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    query_rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id),
    candidate_recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    candidate_rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id),
    exact_raw_equal INTEGER NOT NULL CHECK(exact_raw_equal IN (0, 1)),
    quality_flags_json TEXT NOT NULL CHECK(json_valid(quality_flags_json)),
    calibration_state TEXT NOT NULL CHECK(calibration_state = 'not_calibrated'),
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none')
);

CREATE TABLE audio_fingerprint_compare_v2_sides (
    match_candidate_id TEXT NOT NULL
        REFERENCES match_candidates(match_candidate_id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK(role IN ('query', 'candidate')),
    extraction_result_sha256 TEXT NOT NULL
        CHECK(length(extraction_result_sha256) = 64
              AND extraction_result_sha256 NOT GLOB '*[^0-9a-f]*'),
    extraction_result_path TEXT NOT NULL,
    extraction_result_byte_count INTEGER NOT NULL CHECK(extraction_result_byte_count > 0),
    extraction_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    extraction_recipe_id TEXT NOT NULL,
    extraction_recipe_sha256 TEXT NOT NULL
        CHECK(length(extraction_recipe_sha256) = 64
              AND extraction_recipe_sha256 NOT GLOB '*[^0-9a-f]*'),
    extraction_parameters_json TEXT NOT NULL CHECK(json_valid(extraction_parameters_json)),
    extraction_environment_json TEXT NOT NULL CHECK(json_valid(extraction_environment_json)),
    extraction_fingerprint_count INTEGER NOT NULL CHECK(extraction_fingerprint_count > 0),
    input_media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    input_path TEXT NOT NULL,
    input_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    input_artifact_uri TEXT NOT NULL,
    input_parent_processing_run_id TEXT NOT NULL REFERENCES processing_runs(processing_run_id),
    input_sha256 TEXT NOT NULL
        CHECK(length(input_sha256) = 64 AND input_sha256 NOT GLOB '*[^0-9a-f]*'),
    input_byte_count INTEGER NOT NULL CHECK(input_byte_count > 0),
    input_duration_ms INTEGER NOT NULL CHECK(input_duration_ms > 0),
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id),
    fingerprint_id TEXT NOT NULL REFERENCES fingerprints(fingerprint_id),
    producer_implementation_version TEXT NOT NULL,
    window_kind TEXT NOT NULL
        CHECK(window_kind IN ('full_track', 'explicit_window', 'fixed_chunk', 'partial_tail_chunk')),
    fingerprint_start_ms INTEGER NOT NULL CHECK(fingerprint_start_ms >= 0),
    fingerprint_end_ms INTEGER NOT NULL CHECK(fingerprint_end_ms > fingerprint_start_ms),
    fingerprint_word_count INTEGER NOT NULL CHECK(fingerprint_word_count >= 0),
    artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    artifact_uri TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL
        CHECK(length(artifact_sha256) = 64
              AND artifact_sha256 NOT GLOB '*[^0-9a-f]*'),
    artifact_byte_count INTEGER NOT NULL CHECK(artifact_byte_count >= 0),
    engine_path TEXT NOT NULL,
    engine_sha256 TEXT NOT NULL
        CHECK(length(engine_sha256) = 64 AND engine_sha256 NOT GLOB '*[^0-9a-f]*'),
    engine_byte_count INTEGER NOT NULL CHECK(engine_byte_count > 0),
    engine_version_label TEXT NOT NULL,
    engine_version_output_sha256 TEXT NOT NULL
        CHECK(length(engine_version_output_sha256) = 64
              AND engine_version_output_sha256 NOT GLOB '*[^0-9a-f]*'),
    engine_build_configuration TEXT,
    engine_muxer_help_sha256 TEXT NOT NULL
        CHECK(length(engine_muxer_help_sha256) = 64
              AND engine_muxer_help_sha256 NOT GLOB '*[^0-9a-f]*'),
    algorithm INTEGER NOT NULL CHECK(algorithm >= 0),
    raw_format TEXT NOT NULL CHECK(raw_format = 'ffmpeg_chromaprint_fp_format_raw'),
    sample_rate_hz INTEGER NOT NULL CHECK(sample_rate_hz = 16000),
    channels INTEGER NOT NULL CHECK(channels = 1),
    PRIMARY KEY(match_candidate_id, role),
    UNIQUE(match_candidate_id, fingerprint_id)
);

-- This view validates the complete catalog graph for each extraction envelope, not
-- just the selected fingerprint.  It is also reused by validate_database as a
-- defense-in-depth check after admission.
CREATE VIEW audio_fingerprint_compare_v2_invalid_extraction_sides AS
SELECT side.match_candidate_id, side.role
FROM audio_fingerprint_compare_v2_sides AS side
WHERE side.input_media_id <> 'media_sha256_' || side.input_sha256
   OR NOT EXISTS (
       SELECT 1
       FROM media_objects AS input_media
       WHERE input_media.media_id = side.input_media_id
         AND input_media.sha256 = side.input_sha256
         AND input_media.byte_count = side.input_byte_count
         AND input_media.duration_ms = side.input_duration_ms
         AND input_media.media_kind = 'audio'
   )
   OR (SELECT count(*) FROM artifacts
       WHERE processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR (SELECT count(*) FROM observations
       WHERE processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR (SELECT count(*)
       FROM audio_fingerprint_observations AS typed
       JOIN observations AS observation
         ON observation.observation_id = typed.observation_id
       WHERE observation.processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR (SELECT count(DISTINCT typed.artifact_id)
       FROM audio_fingerprint_observations AS typed
       JOIN observations AS observation
         ON observation.observation_id = typed.observation_id
       WHERE observation.processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR (SELECT count(DISTINCT typed.fingerprint_id)
       FROM audio_fingerprint_observations AS typed
       JOIN observations AS observation
         ON observation.observation_id = typed.observation_id
       WHERE observation.processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR (SELECT count(*)
       FROM audio_fingerprint_observations AS typed
       JOIN fingerprints AS fingerprint
         ON fingerprint.fingerprint_id = typed.fingerprint_id
       WHERE fingerprint.media_id = side.input_media_id
         AND fingerprint.implementation_version = side.producer_implementation_version)
          <> side.extraction_fingerprint_count
   OR (SELECT count(*)
       FROM audio_fingerprint_observations AS typed
       JOIN artifacts AS artifact
         ON artifact.artifact_id = typed.artifact_id
       WHERE artifact.processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR (SELECT count(*) FROM fingerprints
       WHERE media_id = side.input_media_id
         AND implementation_version = side.producer_implementation_version)
          <> side.extraction_fingerprint_count
   OR (SELECT count(*)
       FROM observation_scores AS score
       JOIN observations AS observation
         ON observation.observation_id = score.observation_id
       WHERE observation.processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR (SELECT count(DISTINCT score.observation_id)
       FROM observation_scores AS score
       JOIN observations AS observation
         ON observation.observation_id = score.observation_id
       WHERE observation.processing_run_id = side.extraction_run_id)
          <> side.extraction_fingerprint_count
   OR EXISTS (
       SELECT 1
       FROM audio_fingerprint_observations AS typed
       JOIN observations AS observation
         ON observation.observation_id = typed.observation_id
       LEFT JOIN artifacts AS artifact
         ON artifact.artifact_id = typed.artifact_id
       LEFT JOIN fingerprints AS fingerprint
         ON fingerprint.fingerprint_id = typed.fingerprint_id
       LEFT JOIN renditions AS rendition
         ON rendition.rendition_id = observation.rendition_id
       WHERE observation.processing_run_id = side.extraction_run_id
         AND (
             observation.observation_kind <> 'audio_fingerprint'
             OR observation.recording_id <> side.recording_id
             OR observation.rendition_id <> side.rendition_id
             OR observation.visibility <> 'private'
             OR observation.review_state <> 'machine'
             OR rendition.recording_id IS NOT side.recording_id
             OR rendition.media_id IS NOT side.input_media_id
             OR artifact.artifact_id IS NULL
             OR artifact.processing_run_id IS NOT side.extraction_run_id
             OR artifact.artifact_kind <> 'audio_fingerprint_chromaprint_raw'
             OR artifact.visibility <> 'private'
             OR artifact.schema_version <> 1
             OR fingerprint.fingerprint_id IS NULL
             OR fingerprint.media_id IS NOT side.input_media_id
             OR fingerprint.fingerprint_kind <> 'chromaprint_raw'
             OR fingerprint.implementation_version IS NOT side.producer_implementation_version
             OR fingerprint.start_ms IS NOT observation.start_ms
             OR fingerprint.end_ms IS NOT observation.end_ms
             OR fingerprint.value_text IS NOT NULL
             OR fingerprint.artifact_uri IS NOT artifact.storage_uri
             OR typed.algorithm <> side.algorithm
             OR typed.raw_format <> side.raw_format
             OR typed.sample_rate_hz <> side.sample_rate_hz
             OR typed.channels <> side.channels
             OR typed.requires_human_review <> 1
             OR artifact.byte_count <> typed.fingerprint_word_count * 4
         )
   )
   OR EXISTS (
       SELECT 1
       FROM observations AS observation
       LEFT JOIN audio_fingerprint_observations AS typed
         ON typed.observation_id = observation.observation_id
       LEFT JOIN observation_scores AS score
         ON score.observation_id = observation.observation_id
       WHERE observation.processing_run_id = side.extraction_run_id
         AND (
             typed.observation_id IS NULL
             OR score.observation_score_id IS NULL
             OR score.score_name <> 'fingerprint_word_count'
             OR score.raw_score IS NOT CAST(typed.fingerprint_word_count AS REAL)
             OR score.calibrated_probability IS NOT NULL
             OR score.calibration_set_id IS NOT NULL
             OR score.quality_flags_json <> json(score.quality_flags_json)
             OR json_type(score.quality_flags_json) <> 'array'
             OR EXISTS (
                 SELECT 1 FROM json_each(score.quality_flags_json) AS flag
                 WHERE flag.type <> 'text'
                    OR flag.value NOT IN (
                        'short_window_under_10s', 'short_window_under_30s',
                        'partial_tail_chunk', 'empty_raw_fingerprint'
                    )
             )
             OR (SELECT count(*) FROM json_each(score.quality_flags_json)) <>
                (SELECT count(DISTINCT value) FROM json_each(score.quality_flags_json))
             OR EXISTS (
                 SELECT 1
                 FROM json_each(score.quality_flags_json) AS later
                 JOIN json_each(score.quality_flags_json) AS earlier
                   ON CAST(earlier.key AS INTEGER) < CAST(later.key AS INTEGER)
                  AND earlier.value > later.value
             )
             OR (EXISTS (
                 SELECT 1 FROM json_each(score.quality_flags_json)
                 WHERE value = 'short_window_under_10s'
             )) <> ((observation.end_ms - observation.start_ms) < 10000)
             OR (EXISTS (
                 SELECT 1 FROM json_each(score.quality_flags_json)
                 WHERE value = 'short_window_under_30s'
             )) <> ((observation.end_ms - observation.start_ms) < 30000)
             OR (EXISTS (
                 SELECT 1 FROM json_each(score.quality_flags_json)
                 WHERE value = 'partial_tail_chunk'
             )) <> (typed.window_kind = 'partial_tail_chunk')
             OR (EXISTS (
                 SELECT 1 FROM json_each(score.quality_flags_json)
                 WHERE value = 'empty_raw_fingerprint'
             )) <> (typed.fingerprint_word_count = 0)
         )
   );

CREATE TRIGGER audio_fingerprint_compare_v2_receipts_no_update
BEFORE UPDATE ON audio_fingerprint_compare_v2_receipts
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 comparison receipts are append-only');
END;

CREATE TRIGGER audio_fingerprint_compare_v2_receipts_no_delete
BEFORE DELETE ON audio_fingerprint_compare_v2_receipts
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 comparison receipts are append-only');
END;

CREATE TRIGGER audio_fingerprint_compare_v2_sides_no_update
BEFORE UPDATE ON audio_fingerprint_compare_v2_sides
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 side bindings are append-only');
END;

CREATE TRIGGER audio_fingerprint_compare_v2_sides_no_delete
BEFORE DELETE ON audio_fingerprint_compare_v2_sides
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 side bindings are append-only');
END;

CREATE TRIGGER audio_fingerprint_compare_v2_receipts_admission
BEFORE INSERT ON audio_fingerprint_compare_v2_receipts
BEGIN
    SELECT CASE WHEN
        NEW.quality_flags_json <> json(NEW.quality_flags_json)
        OR json_type(NEW.quality_flags_json) <> 'array'
        OR EXISTS (
            SELECT 1 FROM json_each(NEW.quality_flags_json) AS flag
            WHERE flag.type <> 'text'
               OR flag.value NOT IN (
                   'exact_comparison_only_no_alignment',
                   'cross_duration_windows',
                   'short_query_window_under_10s',
                   'short_candidate_window_under_10s',
                   'empty_query_fingerprint',
                   'empty_candidate_fingerprint',
                   'cross_recording',
                   'cross_rendition',
                   'cross_input_media',
                   'cross_extraction_recipe'
               )
        )
        OR (SELECT count(*) FROM json_each(NEW.quality_flags_json)) <>
           (SELECT count(DISTINCT value) FROM json_each(NEW.quality_flags_json))
        OR EXISTS (
            SELECT 1
            FROM json_each(NEW.quality_flags_json) AS later
            JOIN json_each(NEW.quality_flags_json) AS earlier
              ON CAST(earlier.key AS INTEGER) < CAST(later.key AS INTEGER)
             AND earlier.value > later.value
        )
        OR NOT EXISTS (
            SELECT 1 FROM json_each(NEW.quality_flags_json)
            WHERE value = 'exact_comparison_only_no_alignment'
        )
    THEN RAISE(ABORT, 'invalid canonical audio fingerprint v2 comparison receipt') END;
END;

CREATE TRIGGER audio_fingerprint_compare_v2_sides_json_admission
BEFORE INSERT ON audio_fingerprint_compare_v2_sides
BEGIN
    SELECT CASE WHEN
        NEW.extraction_parameters_json <> json(NEW.extraction_parameters_json)
        OR NEW.extraction_environment_json <> json(NEW.extraction_environment_json)
        OR json_type(NEW.extraction_parameters_json) <> 'object'
        OR json_type(NEW.extraction_environment_json) <> 'object'
        OR EXISTS (
            SELECT 1 FROM (
                SELECT parent, key, count(*) AS copies
                FROM json_tree(NEW.extraction_parameters_json)
                WHERE key IS NOT NULL GROUP BY parent, key HAVING copies > 1
            )
        )
        OR EXISTS (
            SELECT 1 FROM (
                SELECT parent, key, count(*) AS copies
                FROM json_tree(NEW.extraction_environment_json)
                WHERE key IS NOT NULL GROUP BY parent, key HAVING copies > 1
            )
        )
    THEN RAISE(ABORT, 'duplicate or invalid JSON in audio fingerprint v2 side binding') END;
END;

-- An extraction import receipt is the immutable catalog anchor for the complete
-- canonical parameters/environment blobs.  Without this guard, direct SQL could
-- rewrite a completed extraction run and stage the same altered JSON in its v2 side
-- binding, making two mutable copies appear to corroborate one another.
CREATE TRIGGER audio_fingerprint_extraction_receipt_run_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_result_imports AS receipt
    WHERE receipt.result_kind = 'extraction'
      AND receipt.processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint extraction receipt seals its processing run');
END;

CREATE TRIGGER audio_fingerprint_extraction_receipt_run_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_result_imports AS receipt
    WHERE receipt.result_kind = 'extraction'
      AND receipt.processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint extraction receipt seals its processing run');
END;

-- The normalized input artifact binds its producing run by ID.  Seal that row and
-- its input attachments once a v2 pair depends on it so the meaning of the bound
-- upstream provenance cannot be changed after admission.
CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_parent_run_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_parent_processing_run_id IN (
        OLD.processing_run_id, NEW.processing_run_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 input parent processing run is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_parent_run_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_parent_processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 input parent processing run is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_parent_input_no_insert
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_parent_processing_run_id = NEW.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 input parent run inputs are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_parent_input_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_parent_processing_run_id IN (
        OLD.processing_run_id, NEW.processing_run_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 input parent run inputs are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_parent_input_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_parent_processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 input parent run inputs are sealed');
END;

CREATE TRIGGER audio_fingerprint_compare_v2_receipts_no_insert_after_admission
BEFORE INSERT ON audio_fingerprint_compare_v2_receipts
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE typed.match_candidate_id = NEW.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 comparison receipt is sealed');
END;

CREATE TRIGGER audio_fingerprint_compare_v2_sides_no_insert_after_admission
BEFORE INSERT ON audio_fingerprint_compare_v2_sides
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE typed.match_candidate_id = NEW.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 side bindings are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_canonical_admission
BEFORE INSERT ON audio_fingerprint_match_candidates_v2
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM match_candidates AS candidate
        JOIN audio_fingerprint_compare_v2_receipts AS receipt
          ON receipt.match_candidate_id = NEW.match_candidate_id
         AND receipt.processing_run_id = NEW.processing_run_id
         AND receipt.query_result_sha256 = NEW.query_result_sha256
         AND receipt.candidate_result_sha256 = NEW.candidate_result_sha256
        JOIN audio_fingerprint_compare_v2_sides AS query_side
          ON query_side.match_candidate_id = NEW.match_candidate_id
         AND query_side.role = 'query'
         AND query_side.extraction_result_sha256 = NEW.query_result_sha256
         AND query_side.extraction_run_id = NEW.query_extraction_run_id
         AND query_side.fingerprint_id = NEW.query_fingerprint_id
        JOIN audio_fingerprint_compare_v2_sides AS candidate_side
          ON candidate_side.match_candidate_id = NEW.match_candidate_id
         AND candidate_side.role = 'candidate'
         AND candidate_side.extraction_result_sha256 = NEW.candidate_result_sha256
         AND candidate_side.extraction_run_id = NEW.candidate_extraction_run_id
         AND candidate_side.fingerprint_id = NEW.candidate_fingerprint_id
        JOIN processing_runs AS comparison_run
          ON comparison_run.processing_run_id = NEW.processing_run_id
        JOIN processing_runs AS query_run
          ON query_run.processing_run_id = NEW.query_extraction_run_id
        JOIN processing_runs AS candidate_run
          ON candidate_run.processing_run_id = NEW.candidate_extraction_run_id
        JOIN audio_fingerprint_result_imports AS comparison_import
          ON comparison_import.result_kind = 'exact_comparison'
         AND comparison_import.result_sha256 = receipt.comparison_result_sha256
         AND comparison_import.processing_run_id = NEW.processing_run_id
         AND comparison_import.recipe_id = receipt.recipe_id
        JOIN audio_fingerprint_result_imports AS query_import
          ON query_import.result_kind = 'extraction'
         AND query_import.result_sha256 = NEW.query_result_sha256
         AND query_import.processing_run_id = NEW.query_extraction_run_id
         AND query_import.recipe_id = query_side.extraction_recipe_id
        JOIN audio_fingerprint_result_imports AS candidate_import
          ON candidate_import.result_kind = 'extraction'
         AND candidate_import.result_sha256 = NEW.candidate_result_sha256
         AND candidate_import.processing_run_id = NEW.candidate_extraction_run_id
         AND candidate_import.recipe_id = candidate_side.extraction_recipe_id
        JOIN fingerprints AS query_fingerprint
          ON query_fingerprint.fingerprint_id = NEW.query_fingerprint_id
        JOIN fingerprints AS candidate_fingerprint
          ON candidate_fingerprint.fingerprint_id = NEW.candidate_fingerprint_id
        JOIN artifacts AS query_artifact
          ON query_artifact.artifact_id = query_side.artifact_id
        JOIN artifacts AS candidate_artifact
          ON candidate_artifact.artifact_id = candidate_side.artifact_id
        JOIN artifacts AS query_input_artifact
          ON query_input_artifact.artifact_id = query_side.input_artifact_id
        JOIN artifacts AS candidate_input_artifact
          ON candidate_input_artifact.artifact_id = candidate_side.input_artifact_id
        JOIN audio_fingerprint_observations AS query_typed
          ON query_typed.fingerprint_id = NEW.query_fingerprint_id
         AND query_typed.artifact_id = query_side.artifact_id
        JOIN observations AS query_observation
          ON query_observation.observation_id = query_typed.observation_id
        JOIN audio_fingerprint_observations AS candidate_typed
          ON candidate_typed.fingerprint_id = NEW.candidate_fingerprint_id
         AND candidate_typed.artifact_id = candidate_side.artifact_id
        JOIN observations AS candidate_observation
          ON candidate_observation.observation_id = candidate_typed.observation_id
        JOIN renditions AS query_rendition
          ON query_rendition.rendition_id = query_side.rendition_id
        JOIN renditions AS candidate_rendition
          ON candidate_rendition.rendition_id = candidate_side.rendition_id
        JOIN media_objects AS query_media
          ON query_media.media_id = query_side.input_media_id
        JOIN media_objects AS candidate_media
          ON candidate_media.media_id = candidate_side.input_media_id
        WHERE candidate.match_candidate_id = NEW.match_candidate_id
          AND candidate.left_object_type = 'fingerprint'
          AND candidate.left_object_id = NEW.query_fingerprint_id
          AND candidate.right_object_type = 'fingerprint'
          AND candidate.right_object_id = NEW.candidate_fingerprint_id
          AND candidate.match_method = 'chromaprint_exact_raw_bytes_v2'
          AND candidate.calibrated_probability IS NULL
          AND candidate.decision_state = 'candidate'
          AND candidate.metadata_json = json_object(
              'calibration_state', 'not_calibrated',
              'catalog_context', json(json_object(
                  'candidate', json(json_object(
                      'recording_id', candidate_side.recording_id,
                      'rendition_id', candidate_side.rendition_id
                  )),
                  'query', json(json_object(
                      'recording_id', query_side.recording_id,
                      'rendition_id', query_side.rendition_id
                  ))
              )),
              'exact_raw_equal', json(CASE receipt.exact_raw_equal WHEN 1 THEN 'true' ELSE 'false' END),
              'publication_authority', 'none',
              'quality_flags', json(receipt.quality_flags_json),
              'relationship_asserted', json('false'),
              'requires_human_review', json('true'),
              'score_semantics', 'boolean_raw_byte_equality_not_probability',
              'visibility', 'private'
          )
          AND receipt.quality_flags_json = json(receipt.quality_flags_json)
          AND json_type(receipt.quality_flags_json) = 'array'
          AND NOT EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json) AS flag
              WHERE flag.type <> 'text'
                 OR flag.value NOT IN (
                     'exact_comparison_only_no_alignment',
                     'cross_duration_windows',
                     'short_query_window_under_10s',
                     'short_candidate_window_under_10s',
                     'empty_query_fingerprint',
                     'empty_candidate_fingerprint',
                     'cross_recording',
                     'cross_rendition',
                     'cross_input_media',
                     'cross_extraction_recipe'
                 )
          )
          AND (SELECT count(*) FROM json_each(receipt.quality_flags_json)) =
              (SELECT count(DISTINCT value) FROM json_each(receipt.quality_flags_json))
          AND NOT EXISTS (
              SELECT 1
              FROM json_each(receipt.quality_flags_json) AS later
              JOIN json_each(receipt.quality_flags_json) AS earlier
                ON CAST(earlier.key AS INTEGER) < CAST(later.key AS INTEGER)
               AND earlier.value > later.value
          )
          AND EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'exact_comparison_only_no_alignment'
          )
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'cross_duration_windows'
          )) = ((query_side.fingerprint_end_ms - query_side.fingerprint_start_ms)
                 <> (candidate_side.fingerprint_end_ms - candidate_side.fingerprint_start_ms))
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'short_query_window_under_10s'
          )) = ((query_side.fingerprint_end_ms - query_side.fingerprint_start_ms) < 10000)
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'short_candidate_window_under_10s'
          )) = ((candidate_side.fingerprint_end_ms - candidate_side.fingerprint_start_ms) < 10000)
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'empty_query_fingerprint'
          )) = (query_side.artifact_byte_count = 0)
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'empty_candidate_fingerprint'
          )) = (candidate_side.artifact_byte_count = 0)
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'cross_recording'
          )) = (query_side.recording_id <> candidate_side.recording_id)
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'cross_rendition'
          )) = (query_side.rendition_id <> candidate_side.rendition_id)
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'cross_input_media'
          )) = (query_side.input_media_id <> candidate_side.input_media_id)
          AND (EXISTS (
              SELECT 1 FROM json_each(receipt.quality_flags_json)
              WHERE value = 'cross_extraction_recipe'
          )) = (query_side.extraction_recipe_id <> candidate_side.extraction_recipe_id)
          AND receipt.recipe_id = 'recipe_audio_fingerprint_compare_v2_' || substr(receipt.recipe_sha256, 1, 32)
          AND receipt.query_recording_id = query_side.recording_id
          AND receipt.query_rendition_id = query_side.rendition_id
          AND receipt.candidate_recording_id = candidate_side.recording_id
          AND receipt.candidate_rendition_id = candidate_side.rendition_id
          AND comparison_run.stage = 'audio_fingerprint_exact_compare_v2'
          AND comparison_run.implementation_version = '0.2.0'
          AND comparison_run.status = 'completed'
          AND comparison_run.parameters_json = json_object(
              'compatibility_contract', 'engine_build_algorithm_format_normalization_v2',
              'method', 'exact_raw_bytes_v2',
              'recipe_id', receipt.recipe_id,
              'recipe_sha256', receipt.recipe_sha256
          )
          AND comparison_run.environment_json = json_object(
              'candidate_engine', json(json_object(
                  'build_configuration', candidate_side.engine_build_configuration,
                  'byte_count', candidate_side.engine_byte_count,
                  'muxer_help_sha256', candidate_side.engine_muxer_help_sha256,
                  'name', 'ffmpeg',
                  'sha256', candidate_side.engine_sha256,
                  'version_label', candidate_side.engine_version_label,
                  'version_output_sha256', candidate_side.engine_version_output_sha256
              )),
              'candidate_extraction_result_sha256', candidate_side.extraction_result_sha256,
              'comparison_runtime', 'python-standard-library',
              'implementation_version', '0.2.0',
              'query_engine', json(json_object(
                  'build_configuration', query_side.engine_build_configuration,
                  'byte_count', query_side.engine_byte_count,
                  'muxer_help_sha256', query_side.engine_muxer_help_sha256,
                  'name', 'ffmpeg',
                  'sha256', query_side.engine_sha256,
                  'version_label', query_side.engine_version_label,
                  'version_output_sha256', query_side.engine_version_output_sha256
              )),
              'query_extraction_result_sha256', query_side.extraction_result_sha256
          )
          AND comparison_import.catalog_context_json = json_object(
              'candidate', json(json_object(
                  'recording_id', candidate_side.recording_id,
                  'rendition_id', candidate_side.rendition_id
              )),
              'query', json(json_object(
                  'recording_id', query_side.recording_id,
                  'rendition_id', query_side.rendition_id
              ))
          )
          AND query_import.catalog_context_json = json_object(
              'recording_id', query_side.recording_id,
              'rendition_id', query_side.rendition_id
          )
          AND candidate_import.catalog_context_json = json_object(
              'recording_id', candidate_side.recording_id,
              'rendition_id', candidate_side.rendition_id
          )
          AND query_run.stage = 'audio_fingerprint_chromaprint'
          AND query_run.implementation_version = '0.1.0'
          AND query_run.status = 'completed'
          AND query_run.parameters_json = query_side.extraction_parameters_json
          AND query_run.environment_json = query_side.extraction_environment_json
          AND candidate_run.stage = 'audio_fingerprint_chromaprint'
          AND candidate_run.implementation_version = '0.1.0'
          AND candidate_run.status = 'completed'
          AND candidate_run.parameters_json = candidate_side.extraction_parameters_json
          AND candidate_run.environment_json = candidate_side.extraction_environment_json
          AND query_side.extraction_recipe_id =
              'recipe_audio_fingerprint_' || substr(query_side.extraction_recipe_sha256, 1, 32)
          AND candidate_side.extraction_recipe_id =
              'recipe_audio_fingerprint_' || substr(candidate_side.extraction_recipe_sha256, 1, 32)
          AND NOT EXISTS (
              SELECT 1 FROM (
                  SELECT parent, key, count(*) AS copies
                  FROM json_tree(query_run.parameters_json)
                  WHERE key IS NOT NULL GROUP BY parent, key HAVING copies > 1
              )
          )
          AND NOT EXISTS (
              SELECT 1 FROM (
                  SELECT parent, key, count(*) AS copies
                  FROM json_tree(query_run.environment_json)
                  WHERE key IS NOT NULL GROUP BY parent, key HAVING copies > 1
              )
          )
          AND NOT EXISTS (
              SELECT 1 FROM (
                  SELECT parent, key, count(*) AS copies
                  FROM json_tree(candidate_run.parameters_json)
                  WHERE key IS NOT NULL GROUP BY parent, key HAVING copies > 1
              )
          )
          AND NOT EXISTS (
              SELECT 1 FROM (
                  SELECT parent, key, count(*) AS copies
                  FROM json_tree(candidate_run.environment_json)
                  WHERE key IS NOT NULL GROUP BY parent, key HAVING copies > 1
              )
          )
          AND json_extract(query_run.parameters_json, '$.recipe_id') = query_side.extraction_recipe_id
          AND json_extract(query_run.parameters_json, '$.recipe_sha256') = query_side.extraction_recipe_sha256
          AND json_extract(query_run.parameters_json, '$.fingerprint.algorithm') = query_side.algorithm
          AND json_extract(query_run.parameters_json, '$.fingerprint.raw_format') = query_side.raw_format
          AND json_extract(query_run.parameters_json, '$.fingerprint.sample_rate_hz') = query_side.sample_rate_hz
          AND json_extract(query_run.parameters_json, '$.fingerprint.channels') = query_side.channels
          AND json_array_length(query_run.parameters_json, '$.expanded_windows') = query_side.extraction_fingerprint_count
          AND json_extract(candidate_run.parameters_json, '$.recipe_id') = candidate_side.extraction_recipe_id
          AND json_extract(candidate_run.parameters_json, '$.recipe_sha256') = candidate_side.extraction_recipe_sha256
          AND json_extract(candidate_run.parameters_json, '$.fingerprint.algorithm') = candidate_side.algorithm
          AND json_extract(candidate_run.parameters_json, '$.fingerprint.raw_format') = candidate_side.raw_format
          AND json_extract(candidate_run.parameters_json, '$.fingerprint.sample_rate_hz') = candidate_side.sample_rate_hz
          AND json_extract(candidate_run.parameters_json, '$.fingerprint.channels') = candidate_side.channels
          AND json_array_length(candidate_run.parameters_json, '$.expanded_windows') = candidate_side.extraction_fingerprint_count
          AND json_extract(query_run.environment_json, '$.engine.name') = 'ffmpeg'
          AND json_extract(query_run.environment_json, '$.engine.path') = query_side.engine_path
          AND json_extract(query_run.environment_json, '$.engine.sha256') = query_side.engine_sha256
          AND json_extract(query_run.environment_json, '$.engine.byte_count') = query_side.engine_byte_count
          AND json_extract(query_run.environment_json, '$.engine.version_label') = query_side.engine_version_label
          AND json_extract(query_run.environment_json, '$.engine.version_output_sha256') = query_side.engine_version_output_sha256
          AND json_extract(query_run.environment_json, '$.engine.build_configuration') IS query_side.engine_build_configuration
          AND json_extract(query_run.environment_json, '$.engine.muxer_help_sha256') = query_side.engine_muxer_help_sha256
          AND json_extract(candidate_run.environment_json, '$.engine.name') = 'ffmpeg'
          AND json_extract(candidate_run.environment_json, '$.engine.path') = candidate_side.engine_path
          AND json_extract(candidate_run.environment_json, '$.engine.sha256') = candidate_side.engine_sha256
          AND json_extract(candidate_run.environment_json, '$.engine.byte_count') = candidate_side.engine_byte_count
          AND json_extract(candidate_run.environment_json, '$.engine.version_label') = candidate_side.engine_version_label
          AND json_extract(candidate_run.environment_json, '$.engine.version_output_sha256') = candidate_side.engine_version_output_sha256
          AND json_extract(candidate_run.environment_json, '$.engine.build_configuration') IS candidate_side.engine_build_configuration
          AND json_extract(candidate_run.environment_json, '$.engine.muxer_help_sha256') = candidate_side.engine_muxer_help_sha256
          AND query_side.engine_sha256 = candidate_side.engine_sha256
          AND query_side.engine_byte_count = candidate_side.engine_byte_count
          AND query_side.engine_version_label = candidate_side.engine_version_label
          AND query_side.engine_version_output_sha256 = candidate_side.engine_version_output_sha256
          AND query_side.engine_build_configuration IS candidate_side.engine_build_configuration
          AND query_side.engine_muxer_help_sha256 = candidate_side.engine_muxer_help_sha256
          AND query_side.algorithm = candidate_side.algorithm
          AND query_side.raw_format = candidate_side.raw_format
          AND query_side.sample_rate_hz = candidate_side.sample_rate_hz
          AND query_side.channels = candidate_side.channels
          AND query_media.sha256 = query_side.input_sha256
          AND query_media.byte_count = query_side.input_byte_count
          AND query_media.duration_ms = query_side.input_duration_ms
          AND query_media.media_kind = 'audio'
          AND candidate_media.sha256 = candidate_side.input_sha256
          AND candidate_media.byte_count = candidate_side.input_byte_count
          AND candidate_media.duration_ms = candidate_side.input_duration_ms
          AND candidate_media.media_kind = 'audio'
          AND query_input_artifact.processing_run_id = query_side.input_parent_processing_run_id
          AND query_input_artifact.artifact_kind = 'normalized_audio'
          AND query_input_artifact.storage_uri = query_side.input_artifact_uri
          AND query_input_artifact.sha256 = query_side.input_sha256
          AND query_input_artifact.byte_count = query_side.input_byte_count
          AND query_input_artifact.visibility = 'private'
          AND candidate_input_artifact.processing_run_id = candidate_side.input_parent_processing_run_id
          AND candidate_input_artifact.artifact_kind = 'normalized_audio'
          AND candidate_input_artifact.storage_uri = candidate_side.input_artifact_uri
          AND candidate_input_artifact.sha256 = candidate_side.input_sha256
          AND candidate_input_artifact.byte_count = candidate_side.input_byte_count
          AND candidate_input_artifact.visibility = 'private'
          AND query_rendition.recording_id = query_side.recording_id
          AND query_rendition.media_id = query_side.input_media_id
          AND candidate_rendition.recording_id = candidate_side.recording_id
          AND candidate_rendition.media_id = candidate_side.input_media_id
          AND query_fingerprint.media_id = query_side.input_media_id
          AND query_fingerprint.fingerprint_kind = 'chromaprint_raw'
          AND query_fingerprint.implementation_version = query_side.producer_implementation_version
          AND query_side.producer_implementation_version =
              'ffmpeg-chromaprint/0.1.0/' || query_side.extraction_recipe_id
          AND query_fingerprint.start_ms = query_side.fingerprint_start_ms
          AND query_fingerprint.end_ms = query_side.fingerprint_end_ms
          AND query_fingerprint.artifact_uri = query_side.artifact_uri
          AND candidate_fingerprint.media_id = candidate_side.input_media_id
          AND candidate_fingerprint.fingerprint_kind = 'chromaprint_raw'
          AND candidate_fingerprint.implementation_version = candidate_side.producer_implementation_version
          AND candidate_side.producer_implementation_version =
              'ffmpeg-chromaprint/0.1.0/' || candidate_side.extraction_recipe_id
          AND candidate_fingerprint.start_ms = candidate_side.fingerprint_start_ms
          AND candidate_fingerprint.end_ms = candidate_side.fingerprint_end_ms
          AND candidate_fingerprint.artifact_uri = candidate_side.artifact_uri
          AND query_artifact.processing_run_id = query_side.extraction_run_id
          AND query_artifact.artifact_kind = 'audio_fingerprint_chromaprint_raw'
          AND query_artifact.storage_uri = query_side.artifact_uri
          AND query_artifact.sha256 = query_side.artifact_sha256
          AND query_artifact.byte_count = query_side.artifact_byte_count
          AND query_artifact.visibility = 'private'
          AND candidate_artifact.processing_run_id = candidate_side.extraction_run_id
          AND candidate_artifact.artifact_kind = 'audio_fingerprint_chromaprint_raw'
          AND candidate_artifact.storage_uri = candidate_side.artifact_uri
          AND candidate_artifact.sha256 = candidate_side.artifact_sha256
          AND candidate_artifact.byte_count = candidate_side.artifact_byte_count
          AND candidate_artifact.visibility = 'private'
          AND query_observation.processing_run_id = query_side.extraction_run_id
          AND query_observation.recording_id = query_side.recording_id
          AND query_observation.rendition_id = query_side.rendition_id
          AND query_observation.visibility = 'private'
          AND query_observation.review_state = 'machine'
          AND query_observation.start_ms = query_side.fingerprint_start_ms
          AND query_observation.end_ms = query_side.fingerprint_end_ms
          AND candidate_observation.processing_run_id = candidate_side.extraction_run_id
          AND candidate_observation.recording_id = candidate_side.recording_id
          AND candidate_observation.rendition_id = candidate_side.rendition_id
          AND candidate_observation.visibility = 'private'
          AND candidate_observation.review_state = 'machine'
          AND candidate_observation.start_ms = candidate_side.fingerprint_start_ms
          AND candidate_observation.end_ms = candidate_side.fingerprint_end_ms
          AND query_typed.window_kind = query_side.window_kind
          AND query_typed.algorithm = query_side.algorithm
          AND query_typed.raw_format = query_side.raw_format
          AND query_typed.sample_rate_hz = query_side.sample_rate_hz
          AND query_typed.channels = query_side.channels
          AND query_typed.fingerprint_word_count = query_side.fingerprint_word_count
          AND candidate_typed.window_kind = candidate_side.window_kind
          AND candidate_typed.algorithm = candidate_side.algorithm
          AND candidate_typed.raw_format = candidate_side.raw_format
          AND candidate_typed.sample_rate_hz = candidate_side.sample_rate_hz
          AND candidate_typed.channels = candidate_side.channels
          AND candidate_typed.fingerprint_word_count = candidate_side.fingerprint_word_count
          AND query_side.artifact_byte_count = query_side.fingerprint_word_count * 4
          AND candidate_side.artifact_byte_count = candidate_side.fingerprint_word_count * 4
          AND receipt.exact_raw_equal = CASE
              WHEN query_side.artifact_sha256 = candidate_side.artifact_sha256
               AND query_side.artifact_byte_count = candidate_side.artifact_byte_count
              THEN 1 ELSE 0 END
          AND candidate.raw_score = CAST(receipt.exact_raw_equal AS REAL)
          AND (SELECT count(*) FROM audio_fingerprint_result_imports
               WHERE result_kind = 'exact_comparison'
                 AND processing_run_id = NEW.processing_run_id) = 1
          AND (SELECT count(*) FROM audio_fingerprint_result_imports
               WHERE result_kind = 'extraction'
                 AND processing_run_id = query_side.extraction_run_id) = 1
          AND (SELECT count(*) FROM audio_fingerprint_result_imports
               WHERE result_kind = 'extraction'
                 AND processing_run_id = candidate_side.extraction_run_id) = 1
          AND (SELECT count(*) FROM run_inputs
               WHERE processing_run_id = NEW.processing_run_id) = 4
          AND EXISTS (
              SELECT 1 FROM run_inputs
              WHERE processing_run_id = NEW.processing_run_id
                AND object_type = 'audio_fingerprint_extraction_result'
                AND object_id = query_side.extraction_run_id
                AND input_role = 'query_result'
                AND input_sha256 = query_side.extraction_result_sha256
          )
          AND EXISTS (
              SELECT 1 FROM run_inputs
              WHERE processing_run_id = NEW.processing_run_id
                AND object_type = 'audio_fingerprint_extraction_result'
                AND object_id = candidate_side.extraction_run_id
                AND input_role = 'candidate_result'
                AND input_sha256 = candidate_side.extraction_result_sha256
          )
          AND EXISTS (
              SELECT 1 FROM run_inputs
              WHERE processing_run_id = NEW.processing_run_id
                AND object_type = 'fingerprint'
                AND object_id = query_side.fingerprint_id
                AND input_role = 'query'
                AND input_sha256 = query_side.artifact_sha256
          )
          AND EXISTS (
              SELECT 1 FROM run_inputs
              WHERE processing_run_id = NEW.processing_run_id
                AND object_type = 'fingerprint'
                AND object_id = candidate_side.fingerprint_id
                AND input_role = 'candidate'
                AND input_sha256 = candidate_side.artifact_sha256
          )
          AND (SELECT count(*) FROM run_inputs
               WHERE processing_run_id = query_side.extraction_run_id) = 1
          AND (SELECT count(*) FROM run_inputs
               WHERE processing_run_id = candidate_side.extraction_run_id) = 1
          AND EXISTS (
              SELECT 1 FROM run_inputs
              WHERE processing_run_id = query_side.extraction_run_id
                AND object_type = 'media_object'
                AND object_id = query_side.input_media_id
                AND input_role = 'normalized_audio'
                AND input_sha256 = query_side.input_sha256
          )
          AND EXISTS (
              SELECT 1 FROM run_inputs
              WHERE processing_run_id = candidate_side.extraction_run_id
                AND object_type = 'media_object'
                AND object_id = candidate_side.input_media_id
                AND input_role = 'normalized_audio'
                AND input_sha256 = candidate_side.input_sha256
          )
          AND (SELECT count(*) FROM artifacts
               WHERE processing_run_id = query_side.extraction_run_id) = query_side.extraction_fingerprint_count
          AND (SELECT count(*) FROM artifacts
               WHERE processing_run_id = candidate_side.extraction_run_id) = candidate_side.extraction_fingerprint_count
          AND NOT EXISTS (
              SELECT 1
              FROM audio_fingerprint_compare_v2_invalid_extraction_sides AS invalid
              WHERE invalid.match_candidate_id = NEW.match_candidate_id
          )
          AND (SELECT count(*) FROM artifacts
               WHERE processing_run_id = NEW.processing_run_id) = 0
          AND (SELECT count(*) FROM observations
               WHERE processing_run_id = NEW.processing_run_id) = 0
    ) THEN RAISE(ABORT, 'invalid canonical audio fingerprint v2 match candidate dependency') END;
END;

-- Seal all pair-scoped attachments after the final subtype is admitted.
CREATE TRIGGER audio_fingerprint_match_candidates_v2_extraction_input_no_insert
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id = NEW.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 extraction inputs are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_artifact_no_insert
BEFORE INSERT ON artifacts
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id = NEW.processing_run_id
       OR typed.processing_run_id = NEW.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 artifact graph is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_observation_no_insert
BEFORE INSERT ON observations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id = NEW.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation graph is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_typed_observation_no_insert
BEFORE INSERT ON audio_fingerprint_observations
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    LEFT JOIN observations AS observation
      ON observation.observation_id = NEW.observation_id
    LEFT JOIN fingerprints AS fingerprint
      ON fingerprint.fingerprint_id = NEW.fingerprint_id
    LEFT JOIN artifacts AS artifact
      ON artifact.artifact_id = NEW.artifact_id
    WHERE observation.processing_run_id = side.extraction_run_id
       OR artifact.processing_run_id = side.extraction_run_id
       OR (
           fingerprint.media_id = side.input_media_id
           AND fingerprint.implementation_version = side.producer_implementation_version
       )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 typed observations are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_fingerprint_no_insert
BEFORE INSERT ON fingerprints
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id = NEW.media_id
      AND side.producer_implementation_version = NEW.implementation_version
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 fingerprint set is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_score_no_insert
BEFORE INSERT ON observation_scores
WHEN EXISTS (
    SELECT 1
    FROM observations AS observation
    JOIN audio_fingerprint_compare_v2_sides AS side
      ON side.extraction_run_id = observation.processing_run_id
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE observation.observation_id = NEW.observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation scores are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_import_receipt_no_insert
BEFORE INSERT ON audio_fingerprint_result_imports
WHEN EXISTS (
    SELECT 1
    FROM audio_fingerprint_match_candidates_v2 AS typed
    JOIN audio_fingerprint_compare_v2_sides AS side
      ON side.match_candidate_id = typed.match_candidate_id
    WHERE NEW.processing_run_id IN (typed.processing_run_id, side.extraction_run_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 import receipts are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_location_no_insert
BEFORE INSERT ON media_locations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id = NEW.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media locations are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_derivation_no_insert
BEFORE INSERT ON media_derivations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id IN (NEW.child_media_id, NEW.parent_media_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media derivations are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_rendition_no_insert
BEFORE INSERT ON renditions
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id = NEW.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 rendition lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_timeline_no_insert
BEFORE INSERT ON timeline_map_spans
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.rendition_id = NEW.rendition_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 timeline lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_extraction_input_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE OLD.processing_run_id IN (side.extraction_run_id, typed.processing_run_id)
       OR NEW.processing_run_id IN (side.extraction_run_id, typed.processing_run_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 extraction inputs are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_extraction_input_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 extraction inputs are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_artifact_graph_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
       OR typed.processing_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
       OR side.input_artifact_id IN (OLD.artifact_id, NEW.artifact_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 artifact graph is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_artifact_graph_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id = OLD.processing_run_id
       OR typed.processing_run_id = OLD.processing_run_id
       OR side.input_artifact_id = OLD.artifact_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 artifact graph is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_observation_graph_no_update
BEFORE UPDATE ON observations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation graph is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_observation_graph_no_delete
BEFORE DELETE ON observations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.extraction_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation graph is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_fingerprint_graph_no_update
BEFORE UPDATE ON fingerprints
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE (side.input_media_id = OLD.media_id
           AND side.producer_implementation_version = OLD.implementation_version)
       OR (side.input_media_id = NEW.media_id
           AND side.producer_implementation_version = NEW.implementation_version)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 fingerprint set is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_fingerprint_graph_no_delete
BEFORE DELETE ON fingerprints
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id = OLD.media_id
      AND side.producer_implementation_version = OLD.implementation_version
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 fingerprint set is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_score_no_update
BEFORE UPDATE ON observation_scores
WHEN EXISTS (
    SELECT 1
    FROM observations AS observation
    JOIN audio_fingerprint_compare_v2_sides AS side
      ON side.extraction_run_id = observation.processing_run_id
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE observation.observation_id IN (OLD.observation_id, NEW.observation_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation scores are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_score_no_delete
BEFORE DELETE ON observation_scores
WHEN EXISTS (
    SELECT 1
    FROM observations AS observation
    JOIN audio_fingerprint_compare_v2_sides AS side
      ON side.extraction_run_id = observation.processing_run_id
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE observation.observation_id = OLD.observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 observation scores are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_location_no_update
BEFORE UPDATE ON media_locations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id IN (OLD.media_id, NEW.media_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media locations are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_location_no_delete
BEFORE DELETE ON media_locations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id = OLD.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media locations are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_derivation_no_update
BEFORE UPDATE ON media_derivations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id IN (
        OLD.child_media_id, OLD.parent_media_id,
        NEW.child_media_id, NEW.parent_media_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media derivations are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_derivation_no_delete
BEFORE DELETE ON media_derivations
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id IN (OLD.child_media_id, OLD.parent_media_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media derivations are sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_rendition_graph_no_update
BEFORE UPDATE ON renditions
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.rendition_id IN (OLD.rendition_id, NEW.rendition_id)
       OR side.input_media_id = NEW.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 rendition lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_rendition_graph_no_delete
BEFORE DELETE ON renditions
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.rendition_id = OLD.rendition_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 rendition lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_timeline_no_update
BEFORE UPDATE ON timeline_map_spans
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.rendition_id IN (OLD.rendition_id, NEW.rendition_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 timeline lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_timeline_no_delete
BEFORE DELETE ON timeline_map_spans
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.rendition_id = OLD.rendition_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 timeline lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_source_no_insert
BEFORE INSERT ON media_sources
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id = NEW.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media source lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_source_no_update
BEFORE UPDATE ON media_sources
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id IN (OLD.media_id, NEW.media_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media source lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_media_source_no_delete
BEFORE DELETE ON media_sources
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.input_media_id = OLD.media_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 media source lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_recording_source_no_insert
BEFORE INSERT ON recording_sources
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.recording_id = NEW.recording_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 recording source lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_recording_source_no_update
BEFORE UPDATE ON recording_sources
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.recording_id IN (OLD.recording_id, NEW.recording_id)
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 recording source lineage is sealed');
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_recording_source_no_delete
BEFORE DELETE ON recording_sources
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_compare_v2_sides AS side
    JOIN audio_fingerprint_match_candidates_v2 AS typed
      ON typed.match_candidate_id = side.match_candidate_id
    WHERE side.recording_id = OLD.recording_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 recording source lineage is sealed');
END;
