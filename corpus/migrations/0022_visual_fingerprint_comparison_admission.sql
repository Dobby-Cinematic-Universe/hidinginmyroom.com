-- Private admission for deterministic sparse visual-fingerprint comparisons.
-- A comparison is routing evidence only.  In particular, a below-threshold result
-- is not a rejection or an assertion that two recordings are unrelated.

CREATE TABLE visual_fingerprint_compare_imports (
    import_batch_id TEXT PRIMARY KEY,
    comparison_id TEXT NOT NULL UNIQUE,
    result_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(result_sha256) = 64)
        CHECK(result_sha256 = lower(result_sha256))
        CHECK(result_sha256 NOT GLOB '*[^0-9a-f]*'),
    result_path TEXT NOT NULL,
    result_byte_count INTEGER NOT NULL CHECK(result_byte_count > 0),
    processing_run_id TEXT NOT NULL UNIQUE
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    recipe_id TEXT NOT NULL,
    recipe_sha256 TEXT NOT NULL
        CHECK(length(recipe_sha256) = 64)
        CHECK(recipe_sha256 = lower(recipe_sha256))
        CHECK(recipe_sha256 NOT GLOB '*[^0-9a-f]*'),
    result_key TEXT NOT NULL UNIQUE
        CHECK(length(result_key) = 64)
        CHECK(result_key = lower(result_key))
        CHECK(result_key NOT GLOB '*[^0-9a-f]*'),
    implementation_sha256 TEXT NOT NULL
        CHECK(length(implementation_sha256) = 64)
        CHECK(implementation_sha256 = lower(implementation_sha256))
        CHECK(implementation_sha256 NOT GLOB '*[^0-9a-f]*'),
    implementation_byte_count INTEGER NOT NULL CHECK(implementation_byte_count > 0),
    query_result_sha256 TEXT NOT NULL
        CHECK(length(query_result_sha256) = 64)
        CHECK(query_result_sha256 = lower(query_result_sha256))
        CHECK(query_result_sha256 NOT GLOB '*[^0-9a-f]*'),
    candidate_result_sha256 TEXT NOT NULL
        CHECK(length(candidate_result_sha256) = 64)
        CHECK(candidate_result_sha256 = lower(candidate_result_sha256))
        CHECK(candidate_result_sha256 NOT GLOB '*[^0-9a-f]*'),
    catalog_context_json TEXT NOT NULL CHECK(json_valid(catalog_context_json)),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL DEFAULT 'none'
        CHECK(publication_authority = 'none'),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    CHECK(query_result_sha256 <> candidate_result_sha256),
    CHECK(recipe_id GLOB 'recipe_visual_fingerprint_compare_[0-9a-f]*'),
    CHECK(lower(result_path) NOT LIKE 'http://%'),
    CHECK(lower(result_path) NOT LIKE 'https://%'),
    CHECK(lower(replace(result_path, char(92), '/')) NOT GLOB 'src/*'),
    CHECK(lower(replace(result_path, char(92), '/')) NOT GLOB 'public/*'),
    CHECK(lower(replace(result_path, char(92), '/')) NOT GLOB 'dist/*'),
    CHECK(instr(lower(replace(result_path, char(92), '/')), '/src/data/corpus/') = 0),
    CHECK(instr(lower(replace(result_path, char(92), '/')), '/public/') = 0),
    CHECK(instr(lower(replace(result_path, char(92), '/')), '/dist/') = 0),
    CHECK(
        lower(replace(result_path, char(92), '/')) GLOB 'research/*'
        OR instr(lower(replace(result_path, char(92), '/')), '/research/') > 0
        OR instr(lower(replace(result_path, char(92), '/')), '/corpus/work/') > 0
        OR instr(lower(replace(result_path, char(92), '/')), '/corpus/private/') > 0
        OR instr(lower(replace(result_path, char(92), '/')), '/corpus/artifacts/') > 0
        OR lower(result_path) LIKE 'private:%'
    )
);

CREATE TABLE visual_fingerprint_compare_sides (
    comparison_id TEXT NOT NULL
        REFERENCES visual_fingerprint_compare_imports(comparison_id) ON DELETE RESTRICT,
    role TEXT NOT NULL CHECK(role IN ('query', 'candidate')),
    extraction_run_id TEXT NOT NULL
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    extraction_import_batch_id TEXT NOT NULL
        REFERENCES visual_fingerprint_result_imports(import_batch_id) ON DELETE RESTRICT,
    extraction_result_sha256 TEXT NOT NULL
        CHECK(length(extraction_result_sha256) = 64),
    extraction_result_path TEXT NOT NULL,
    extraction_result_byte_count INTEGER NOT NULL CHECK(extraction_result_byte_count > 0),
    extraction_result_key TEXT NOT NULL CHECK(length(extraction_result_key) = 64),
    extraction_recipe_id TEXT NOT NULL,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE RESTRICT,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE RESTRICT,
    rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id) ON DELETE RESTRICT,
    frame_count INTEGER NOT NULL CHECK(frame_count BETWEEN 1 AND 256),
    algorithm TEXT NOT NULL CHECK(algorithm = 'fixed_q20_dct_phash_8x8_v1'),
    phash_bits INTEGER NOT NULL CHECK(phash_bits = 64),
    unchanged INTEGER NOT NULL CHECK(unchanged = 1),
    PRIMARY KEY(comparison_id, role),
    UNIQUE(comparison_id, extraction_run_id),
    UNIQUE(comparison_id, extraction_result_sha256),
    CHECK(extraction_result_sha256 = lower(extraction_result_sha256)),
    CHECK(extraction_result_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(extraction_result_key = lower(extraction_result_key)),
    CHECK(extraction_result_key NOT GLOB '*[^0-9a-f]*'),
    CHECK(extraction_recipe_id GLOB 'recipe_visual_fingerprint_[0-9a-f]*')
);

CREATE TABLE visual_fingerprint_compare_side_frames (
    comparison_id TEXT NOT NULL,
    role TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 255),
    fingerprint_id TEXT NOT NULL REFERENCES fingerprints(fingerprint_id) ON DELETE RESTRICT,
    sample_id TEXT NOT NULL,
    requested_timestamp_ms INTEGER NOT NULL CHECK(requested_timestamp_ms >= 0),
    decoded_relative_timestamp_ms INTEGER NOT NULL CHECK(decoded_relative_timestamp_ms >= 0),
    phash_hex TEXT NOT NULL
        CHECK(length(phash_hex) = 16)
        CHECK(phash_hex = lower(phash_hex))
        CHECK(phash_hex NOT GLOB '*[^0-9a-f]*'),
    exact_gray_sha256 TEXT NOT NULL
        CHECK(length(exact_gray_sha256) = 64)
        CHECK(exact_gray_sha256 = lower(exact_gray_sha256))
        CHECK(exact_gray_sha256 NOT GLOB '*[^0-9a-f]*'),
    quality_flags_json TEXT NOT NULL CHECK(json_valid(quality_flags_json)),
    PRIMARY KEY(comparison_id, role, ordinal),
    UNIQUE(comparison_id, role, fingerprint_id),
    FOREIGN KEY(comparison_id, role)
        REFERENCES visual_fingerprint_compare_sides(comparison_id, role)
        ON DELETE RESTRICT
);

CREATE TABLE visual_fingerprint_comparisons (
    comparison_id TEXT PRIMARY KEY
        REFERENCES visual_fingerprint_compare_imports(comparison_id) ON DELETE RESTRICT,
    processing_run_id TEXT NOT NULL UNIQUE
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    method TEXT NOT NULL CHECK(method = 'minimum_pairwise_phash_hamming_v1'),
    algorithm TEXT NOT NULL CHECK(algorithm = 'fixed_q20_dct_phash_8x8_v1'),
    phash_bits INTEGER NOT NULL CHECK(phash_bits = 64),
    pairwise_comparisons INTEGER NOT NULL CHECK(pairwise_comparisons BETWEEN 1 AND 65536),
    best_hamming_distance INTEGER NOT NULL CHECK(best_hamming_distance BETWEEN 0 AND 64),
    best_normalized_hamming_distance REAL NOT NULL
        CHECK(best_normalized_hamming_distance BETWEEN 0.0 AND 1.0),
    best_raw_similarity REAL NOT NULL CHECK(best_raw_similarity BETWEEN 0.0 AND 1.0),
    exact_gray_pair_count INTEGER NOT NULL
        CHECK(exact_gray_pair_count BETWEEN 0 AND pairwise_comparisons),
    maximum_hamming_distance INTEGER NOT NULL CHECK(maximum_hamming_distance BETWEEN 0 AND 64),
    top_k INTEGER NOT NULL CHECK(top_k BETWEEN 1 AND 100),
    max_pairwise_comparisons INTEGER NOT NULL
        CHECK(max_pairwise_comparisons BETWEEN 1 AND 65536),
    threshold_state TEXT NOT NULL
        CHECK(threshold_state IN (
            'meets_configured_threshold', 'does_not_meet_configured_threshold'
        )),
    candidate_emitted INTEGER NOT NULL CHECK(candidate_emitted IN (0, 1)),
    decision_state TEXT NOT NULL
        CHECK(decision_state IN ('candidate_for_human_review', 'below_configured_threshold')),
    score_semantics TEXT NOT NULL
        CHECK(score_semantics = 'raw_64_bit_phash_hamming_not_probability'),
    calibration_state TEXT NOT NULL CHECK(calibration_state = 'not_calibrated'),
    calibrated_probability REAL CHECK(calibrated_probability IS NULL),
    requires_human_review INTEGER NOT NULL CHECK(requires_human_review = 1),
    person_identity_asserted INTEGER NOT NULL CHECK(person_identity_asserted = 0),
    duplicate_asserted INTEGER NOT NULL CHECK(duplicate_asserted = 0),
    parent_asserted INTEGER NOT NULL CHECK(parent_asserted = 0),
    ownership_asserted INTEGER NOT NULL CHECK(ownership_asserted = 0),
    relationship_asserted INTEGER NOT NULL CHECK(relationship_asserted = 0),
    unrelated_asserted INTEGER NOT NULL CHECK(unrelated_asserted = 0),
    match_candidate_id TEXT UNIQUE
        REFERENCES match_candidates(match_candidate_id)
        DEFERRABLE INITIALLY DEFERRED,
    review_task_id TEXT UNIQUE
        REFERENCES review_tasks(review_task_id)
        DEFERRABLE INITIALLY DEFERRED,
    warning TEXT NOT NULL,
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    CHECK(pairwise_comparisons <= max_pairwise_comparisons),
    CHECK(abs(best_normalized_hamming_distance - round(best_hamming_distance / 64.0, 6)) < 0.000000001),
    CHECK(abs(best_raw_similarity - round((64 - best_hamming_distance) / 64.0, 6)) < 0.000000001),
    CHECK(
        (best_hamming_distance <= maximum_hamming_distance
         AND threshold_state = 'meets_configured_threshold'
         AND candidate_emitted = 1
         AND decision_state = 'candidate_for_human_review'
         AND match_candidate_id = comparison_id
         AND review_task_id IS NOT NULL)
        OR
        (best_hamming_distance > maximum_hamming_distance
         AND threshold_state = 'does_not_meet_configured_threshold'
         AND candidate_emitted = 0
         AND decision_state = 'below_configured_threshold'
         AND match_candidate_id IS NULL
         AND review_task_id IS NULL)
    )
);

CREATE TABLE visual_fingerprint_compare_top_pairs (
    comparison_id TEXT NOT NULL
        REFERENCES visual_fingerprint_comparisons(comparison_id) ON DELETE RESTRICT,
    rank INTEGER NOT NULL CHECK(rank BETWEEN 0 AND 99),
    query_fingerprint_id TEXT NOT NULL
        REFERENCES fingerprints(fingerprint_id) ON DELETE RESTRICT,
    candidate_fingerprint_id TEXT NOT NULL
        REFERENCES fingerprints(fingerprint_id) ON DELETE RESTRICT,
    hamming_distance INTEGER NOT NULL CHECK(hamming_distance BETWEEN 0 AND 64),
    normalized_hamming_distance REAL NOT NULL
        CHECK(normalized_hamming_distance BETWEEN 0.0 AND 1.0),
    raw_similarity REAL NOT NULL CHECK(raw_similarity BETWEEN 0.0 AND 1.0),
    exact_gray_equal INTEGER NOT NULL CHECK(exact_gray_equal IN (0, 1)),
    PRIMARY KEY(comparison_id, rank),
    UNIQUE(comparison_id, query_fingerprint_id, candidate_fingerprint_id),
    CHECK(abs(normalized_hamming_distance - round(hamming_distance / 64.0, 6)) < 0.000000001),
    CHECK(abs(raw_similarity - round((64 - hamming_distance) / 64.0, 6)) < 0.000000001)
);

CREATE TABLE visual_fingerprint_compare_completion_receipts (
    completion_receipt_id TEXT PRIMARY KEY,
    import_batch_id TEXT NOT NULL UNIQUE
        REFERENCES visual_fingerprint_compare_imports(import_batch_id) ON DELETE RESTRICT,
    comparison_id TEXT NOT NULL UNIQUE
        REFERENCES visual_fingerprint_comparisons(comparison_id) ON DELETE RESTRICT,
    completed_at TEXT NOT NULL CHECK(julianday(completed_at) IS NOT NULL)
);

CREATE INDEX visual_fingerprint_compare_frames_fingerprint_idx
    ON visual_fingerprint_compare_side_frames(fingerprint_id, comparison_id);
CREATE INDEX visual_fingerprint_compare_top_query_idx
    ON visual_fingerprint_compare_top_pairs(query_fingerprint_id, comparison_id);
CREATE INDEX visual_fingerprint_compare_top_candidate_idx
    ON visual_fingerprint_compare_top_pairs(candidate_fingerprint_id, comparison_id);

CREATE TRIGGER visual_fingerprint_compare_imports_admission
BEFORE INSERT ON visual_fingerprint_compare_imports
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM processing_runs AS run
        WHERE run.processing_run_id = NEW.processing_run_id
          AND run.stage = 'visual_fingerprint_compare'
          AND run.implementation_version = '0.1.0'
          AND run.model_id IS NULL
          AND run.glossary_revision_id IS NULL
          AND run.status = 'completed'
          AND run.completed_at IS NOT NULL
    ) THEN RAISE(ABORT, 'invalid visual comparison processing run') END;
END;

CREATE TRIGGER visual_fingerprint_compare_sides_admission
BEFORE INSERT ON visual_fingerprint_compare_sides
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM visual_fingerprint_compare_imports AS compare_import
        JOIN visual_fingerprint_result_imports AS extraction_import
          ON extraction_import.import_batch_id = NEW.extraction_import_batch_id
        JOIN processing_runs AS extraction_run
          ON extraction_run.processing_run_id = NEW.extraction_run_id
        JOIN renditions AS rendition
          ON rendition.rendition_id = NEW.rendition_id
        WHERE compare_import.comparison_id = NEW.comparison_id
          AND extraction_import.processing_run_id = NEW.extraction_run_id
          AND extraction_import.result_sha256 = NEW.extraction_result_sha256
          AND extraction_import.recipe_id = NEW.extraction_recipe_id
          AND json_extract(extraction_import.catalog_context_json, '$.recording_id') = NEW.recording_id
          AND json_extract(extraction_import.catalog_context_json, '$.rendition_id') = NEW.rendition_id
          AND extraction_run.stage = 'visual_fingerprint_extract'
          AND extraction_run.status = 'completed'
          AND rendition.recording_id = NEW.recording_id
          AND rendition.media_id = NEW.media_id
          AND NEW.extraction_result_sha256 = CASE NEW.role
              WHEN 'query' THEN compare_import.query_result_sha256
              ELSE compare_import.candidate_result_sha256 END
    ) THEN RAISE(ABORT, 'invalid visual comparison extraction side') END;
END;

CREATE TRIGGER visual_fingerprint_compare_side_frames_admission
BEFORE INSERT ON visual_fingerprint_compare_side_frames
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM visual_fingerprint_compare_sides AS side
        JOIN visual_fingerprint_observations AS visual
          ON visual.fingerprint_id = NEW.fingerprint_id
        JOIN observations AS observation
          ON observation.observation_id = visual.observation_id
        JOIN fingerprints AS fingerprint
          ON fingerprint.fingerprint_id = visual.fingerprint_id
        WHERE side.comparison_id = NEW.comparison_id
          AND side.role = NEW.role
          AND observation.processing_run_id = side.extraction_run_id
          AND observation.recording_id = side.recording_id
          AND observation.rendition_id = side.rendition_id
          AND observation.visibility = 'private'
          AND observation.review_state = 'machine'
          AND fingerprint.media_id = side.media_id
          AND visual.sample_id = NEW.sample_id
          AND visual.requested_timestamp_ms = NEW.requested_timestamp_ms
          AND visual.relative_timestamp_ms = NEW.decoded_relative_timestamp_ms
          AND visual.phash_hex = NEW.phash_hex
          AND visual.exact_gray_sha256 = NEW.exact_gray_sha256
          AND visual.quality_flags_json = NEW.quality_flags_json
    ) THEN RAISE(ABORT, 'invalid visual comparison selected frame') END;
END;

CREATE TRIGGER visual_fingerprint_comparisons_admission
BEFORE INSERT ON visual_fingerprint_comparisons
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM visual_fingerprint_compare_imports AS compare_import
        JOIN processing_runs AS run
          ON run.processing_run_id = NEW.processing_run_id
        WHERE compare_import.comparison_id = NEW.comparison_id
          AND compare_import.processing_run_id = NEW.processing_run_id
          AND run.stage = 'visual_fingerprint_compare'
          AND run.status = 'completed'
          AND (SELECT count(*) FROM visual_fingerprint_compare_sides AS side
               WHERE side.comparison_id = NEW.comparison_id) = 2
          AND NEW.pairwise_comparisons =
              (SELECT frame_count FROM visual_fingerprint_compare_sides
               WHERE comparison_id = NEW.comparison_id AND role = 'query')
              *
              (SELECT frame_count FROM visual_fingerprint_compare_sides
               WHERE comparison_id = NEW.comparison_id AND role = 'candidate')
    ) THEN RAISE(ABORT, 'invalid visual comparison summary') END;
END;

CREATE TRIGGER visual_fingerprint_compare_top_pairs_admission
BEFORE INSERT ON visual_fingerprint_compare_top_pairs
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM visual_fingerprint_comparisons AS comparison
        JOIN visual_fingerprint_compare_side_frames AS query_frame
          ON query_frame.comparison_id = comparison.comparison_id
         AND query_frame.role = 'query'
         AND query_frame.fingerprint_id = NEW.query_fingerprint_id
        JOIN visual_fingerprint_compare_side_frames AS candidate_frame
          ON candidate_frame.comparison_id = comparison.comparison_id
         AND candidate_frame.role = 'candidate'
         AND candidate_frame.fingerprint_id = NEW.candidate_fingerprint_id
        WHERE comparison.comparison_id = NEW.comparison_id
          AND NEW.rank < comparison.top_k
          AND NEW.exact_gray_equal = CASE
              WHEN query_frame.exact_gray_sha256 = candidate_frame.exact_gray_sha256
              THEN 1 ELSE 0 END
    ) THEN RAISE(ABORT, 'invalid visual comparison top pair') END;
    SELECT CASE WHEN NEW.rank > 0 AND NOT EXISTS (
        SELECT 1 FROM visual_fingerprint_compare_top_pairs AS previous
        WHERE previous.comparison_id = NEW.comparison_id
          AND previous.rank = NEW.rank - 1
          AND (
              previous.hamming_distance < NEW.hamming_distance
              OR (previous.hamming_distance = NEW.hamming_distance
                  AND previous.query_fingerprint_id < NEW.query_fingerprint_id)
              OR (previous.hamming_distance = NEW.hamming_distance
                  AND previous.query_fingerprint_id = NEW.query_fingerprint_id
                  AND previous.candidate_fingerprint_id < NEW.candidate_fingerprint_id)
          )
    ) THEN RAISE(ABORT, 'visual comparison top pairs must be contiguous and sorted') END;
END;

CREATE TRIGGER visual_fingerprint_match_candidate_admission
BEFORE INSERT ON match_candidates
WHEN NEW.match_method = 'visual_phash_minimum_hamming_v1'
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM visual_fingerprint_comparisons AS comparison
        JOIN visual_fingerprint_compare_sides AS query_side
          ON query_side.comparison_id = comparison.comparison_id
         AND query_side.role = 'query'
        JOIN visual_fingerprint_compare_sides AS candidate_side
          ON candidate_side.comparison_id = comparison.comparison_id
         AND candidate_side.role = 'candidate'
        WHERE comparison.comparison_id = NEW.match_candidate_id
          AND comparison.candidate_emitted = 1
          AND comparison.match_candidate_id = NEW.match_candidate_id
          AND NEW.left_object_type = 'visual_fingerprint_extraction_result'
          AND NEW.left_object_id = query_side.extraction_run_id
          AND NEW.right_object_type = 'visual_fingerprint_extraction_result'
          AND NEW.right_object_id = candidate_side.extraction_run_id
          AND NEW.raw_score = comparison.best_raw_similarity
          AND NEW.calibrated_probability IS NULL
          AND NEW.decision_state = 'candidate'
          AND json_extract(NEW.metadata_json, '$.calibration_state') = 'not_calibrated'
          AND json_extract(NEW.metadata_json, '$.candidate_emitted') = 1
          AND json_extract(NEW.metadata_json, '$.requires_human_review') = 1
          AND json_extract(NEW.metadata_json, '$.person_identity_asserted') = 0
          AND json_extract(NEW.metadata_json, '$.duplicate_asserted') = 0
          AND json_extract(NEW.metadata_json, '$.parent_asserted') = 0
          AND json_extract(NEW.metadata_json, '$.ownership_asserted') = 0
          AND json_extract(NEW.metadata_json, '$.relationship_asserted') = 0
          AND json_extract(NEW.metadata_json, '$.unrelated_asserted') = 0
          AND json_extract(NEW.metadata_json, '$.visibility') = 'private'
          AND json_extract(NEW.metadata_json, '$.publication_authority') = 'none'
    ) THEN RAISE(ABORT, 'invalid visual comparison match candidate') END;
END;

CREATE TRIGGER visual_fingerprint_compare_review_task_admission
BEFORE INSERT ON review_tasks
WHEN NEW.task_kind = 'visual_fingerprint_comparison_review'
   OR NEW.target_type = 'visual_fingerprint_comparison'
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM visual_fingerprint_comparisons AS comparison
        WHERE comparison.comparison_id = NEW.target_id
          AND comparison.candidate_emitted = 1
          AND comparison.review_task_id = NEW.review_task_id
          AND NEW.task_kind = 'visual_fingerprint_comparison_review'
          AND NEW.target_type = 'visual_fingerprint_comparison'
          AND NEW.status = 'open'
    ) THEN RAISE(ABORT, 'invalid visual comparison review task') END;
END;

CREATE TRIGGER visual_fingerprint_compare_completion_admission
BEFORE INSERT ON visual_fingerprint_compare_completion_receipts
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM visual_fingerprint_compare_imports AS compare_import
        JOIN visual_fingerprint_comparisons AS comparison
          ON comparison.comparison_id = compare_import.comparison_id
        WHERE compare_import.import_batch_id = NEW.import_batch_id
          AND comparison.comparison_id = NEW.comparison_id
          AND (SELECT count(*) FROM visual_fingerprint_compare_sides AS side
               WHERE side.comparison_id = comparison.comparison_id) = 2
          AND NOT EXISTS (
              SELECT 1 FROM visual_fingerprint_compare_sides AS side
              WHERE side.comparison_id = comparison.comparison_id
                AND side.frame_count <> (
                    SELECT count(*) FROM visual_fingerprint_compare_side_frames AS frame
                    WHERE frame.comparison_id = side.comparison_id
                      AND frame.role = side.role
                )
          )
          AND (SELECT count(*) FROM visual_fingerprint_compare_top_pairs AS pair
               WHERE pair.comparison_id = comparison.comparison_id)
              = min(comparison.top_k, comparison.pairwise_comparisons)
          AND EXISTS (
              SELECT 1 FROM visual_fingerprint_compare_top_pairs AS best
              WHERE best.comparison_id = comparison.comparison_id
                AND best.rank = 0
                AND best.hamming_distance = comparison.best_hamming_distance
                AND best.normalized_hamming_distance = comparison.best_normalized_hamming_distance
                AND best.raw_similarity = comparison.best_raw_similarity
          )
          AND comparison.exact_gray_pair_count = (
              SELECT count(*)
              FROM visual_fingerprint_compare_side_frames AS query_frame
              JOIN visual_fingerprint_compare_side_frames AS candidate_frame
                ON candidate_frame.comparison_id = query_frame.comparison_id
               AND candidate_frame.role = 'candidate'
              WHERE query_frame.comparison_id = comparison.comparison_id
                AND query_frame.role = 'query'
                AND query_frame.exact_gray_sha256 = candidate_frame.exact_gray_sha256
          )
          AND (
              (comparison.candidate_emitted = 0
               AND comparison.match_candidate_id IS NULL
               AND comparison.review_task_id IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM match_candidates AS match
                   WHERE match.match_candidate_id = comparison.comparison_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM review_tasks AS task
                   WHERE task.target_type = 'visual_fingerprint_comparison'
                     AND task.target_id = comparison.comparison_id
               ))
              OR
              (comparison.candidate_emitted = 1
               AND EXISTS (
                   SELECT 1 FROM match_candidates AS match
                   WHERE match.match_candidate_id = comparison.match_candidate_id
                     AND match.match_method = 'visual_phash_minimum_hamming_v1'
                     AND match.decision_state = 'candidate'
               )
               AND EXISTS (
                   SELECT 1 FROM review_tasks AS task
                   WHERE task.review_task_id = comparison.review_task_id
                     AND task.task_kind = 'visual_fingerprint_comparison_review'
                     AND task.target_type = 'visual_fingerprint_comparison'
                     AND task.target_id = comparison.comparison_id
               ))
          )
    ) THEN RAISE(ABORT, 'visual comparison graph is incomplete or inconsistent') END;
END;

-- Raw comparison evidence is append-only.  Human conclusions are separate review
-- decisions and never rewrite a measurement or its selected-frame graph.
CREATE TRIGGER visual_fingerprint_compare_imports_no_update
BEFORE UPDATE ON visual_fingerprint_compare_imports BEGIN
    SELECT RAISE(ABORT, 'visual comparison imports are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_imports_no_delete
BEFORE DELETE ON visual_fingerprint_compare_imports BEGIN
    SELECT RAISE(ABORT, 'visual comparison imports are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_sides_no_update
BEFORE UPDATE ON visual_fingerprint_compare_sides BEGIN
    SELECT RAISE(ABORT, 'visual comparison sides are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_sides_no_delete
BEFORE DELETE ON visual_fingerprint_compare_sides BEGIN
    SELECT RAISE(ABORT, 'visual comparison sides are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_side_frames_no_update
BEFORE UPDATE ON visual_fingerprint_compare_side_frames BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected frames are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_side_frames_no_delete
BEFORE DELETE ON visual_fingerprint_compare_side_frames BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected frames are append-only');
END;
CREATE TRIGGER visual_fingerprint_comparisons_no_update
BEFORE UPDATE ON visual_fingerprint_comparisons BEGIN
    SELECT RAISE(ABORT, 'visual comparisons are append-only');
END;
CREATE TRIGGER visual_fingerprint_comparisons_no_delete
BEFORE DELETE ON visual_fingerprint_comparisons BEGIN
    SELECT RAISE(ABORT, 'visual comparisons are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_top_pairs_no_update
BEFORE UPDATE ON visual_fingerprint_compare_top_pairs BEGIN
    SELECT RAISE(ABORT, 'visual comparison top pairs are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_top_pairs_no_delete
BEFORE DELETE ON visual_fingerprint_compare_top_pairs BEGIN
    SELECT RAISE(ABORT, 'visual comparison top pairs are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_completion_no_update
BEFORE UPDATE ON visual_fingerprint_compare_completion_receipts BEGIN
    SELECT RAISE(ABORT, 'visual comparison completion receipts are append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_completion_no_delete
BEFORE DELETE ON visual_fingerprint_compare_completion_receipts BEGIN
    SELECT RAISE(ABORT, 'visual comparison completion receipts are append-only');
END;

CREATE TRIGGER visual_fingerprint_compare_match_no_update
BEFORE UPDATE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_comparisons
    WHERE match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison match evidence is append-only');
END;
CREATE TRIGGER visual_fingerprint_compare_match_no_delete
BEFORE DELETE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_comparisons
    WHERE match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison match evidence is append-only');
END;

CREATE TRIGGER visual_fingerprint_compare_run_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_compare_imports
    WHERE processing_run_id = OLD.processing_run_id
    UNION ALL
    SELECT 1 FROM visual_fingerprint_compare_sides
    WHERE extraction_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison cited processing run is immutable');
END;
CREATE TRIGGER visual_fingerprint_compare_run_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_compare_imports
    WHERE processing_run_id = OLD.processing_run_id
    UNION ALL
    SELECT 1 FROM visual_fingerprint_compare_sides
    WHERE extraction_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison cited processing run is immutable');
END;

CREATE TRIGGER visual_fingerprint_compare_input_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_compare_imports
    WHERE processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison run inputs are immutable');
END;
CREATE TRIGGER visual_fingerprint_compare_input_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_compare_imports
    WHERE processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison run inputs are immutable');
END;

CREATE TRIGGER visual_fingerprint_compare_fingerprint_no_update
BEFORE UPDATE ON fingerprints
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_compare_side_frames
    WHERE fingerprint_id = OLD.fingerprint_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected fingerprint is immutable');
END;
CREATE TRIGGER visual_fingerprint_compare_fingerprint_no_delete
BEFORE DELETE ON fingerprints
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_compare_side_frames
    WHERE fingerprint_id = OLD.fingerprint_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected fingerprint is immutable');
END;

CREATE TRIGGER visual_fingerprint_compare_observation_no_update
BEFORE UPDATE ON observations
WHEN EXISTS (
    SELECT 1
    FROM visual_fingerprint_observations AS visual
    JOIN visual_fingerprint_compare_side_frames AS frame
      ON frame.fingerprint_id = visual.fingerprint_id
    WHERE visual.observation_id = OLD.observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected observation is immutable');
END;
CREATE TRIGGER visual_fingerprint_compare_observation_no_delete
BEFORE DELETE ON observations
WHEN EXISTS (
    SELECT 1
    FROM visual_fingerprint_observations AS visual
    JOIN visual_fingerprint_compare_side_frames AS frame
      ON frame.fingerprint_id = visual.fingerprint_id
    WHERE visual.observation_id = OLD.observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected observation is immutable');
END;

CREATE TRIGGER visual_fingerprint_compare_artifact_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM visual_fingerprint_observations AS visual
    JOIN visual_fingerprint_compare_side_frames AS frame
      ON frame.fingerprint_id = visual.fingerprint_id
    WHERE visual.artifact_id = OLD.artifact_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected artifact is immutable');
END;
CREATE TRIGGER visual_fingerprint_compare_artifact_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM visual_fingerprint_observations AS visual
    JOIN visual_fingerprint_compare_side_frames AS frame
      ON frame.fingerprint_id = visual.fingerprint_id
    WHERE visual.artifact_id = OLD.artifact_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison selected artifact is immutable');
END;

CREATE TRIGGER visual_fingerprint_compare_review_task_identity_no_update
BEFORE UPDATE ON review_tasks
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_comparisons
    WHERE review_task_id = OLD.review_task_id
)
AND (
    NEW.review_task_id <> OLD.review_task_id
    OR NEW.task_kind <> OLD.task_kind
    OR NEW.target_type <> OLD.target_type
    OR NEW.target_id <> OLD.target_id
    OR NEW.reason <> OLD.reason
    OR NEW.priority <> OLD.priority
    OR NEW.created_at <> OLD.created_at
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison review-task identity is immutable');
END;
CREATE TRIGGER visual_fingerprint_compare_review_task_no_delete
BEFORE DELETE ON review_tasks
WHEN EXISTS (
    SELECT 1 FROM visual_fingerprint_comparisons
    WHERE review_task_id = OLD.review_task_id
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison review task cannot be deleted');
END;

CREATE TRIGGER visual_fingerprint_compare_publication_forbidden
BEFORE INSERT ON publication_decisions
WHEN (
    NEW.object_type = 'visual_fingerprint_comparison'
    AND EXISTS (
        SELECT 1 FROM visual_fingerprint_comparisons
        WHERE comparison_id = NEW.object_id
    )
)
OR (
    NEW.object_type = 'match_candidate'
    AND EXISTS (
        SELECT 1 FROM visual_fingerprint_comparisons
        WHERE match_candidate_id = NEW.object_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison evidence has no publication authority');
END;

CREATE TRIGGER visual_fingerprint_compare_publication_gate_forbidden
BEFORE INSERT ON publication_gate_decisions
WHEN (
    NEW.object_type = 'visual_fingerprint_comparison'
    AND EXISTS (
        SELECT 1 FROM visual_fingerprint_comparisons
        WHERE comparison_id = NEW.object_id
    )
)
OR (
    NEW.object_type = 'match_candidate'
    AND EXISTS (
        SELECT 1 FROM visual_fingerprint_comparisons
        WHERE match_candidate_id = NEW.object_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'visual comparison evidence cannot receive publication gates');
END;
