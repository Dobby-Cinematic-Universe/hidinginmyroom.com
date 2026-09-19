-- Private admission and full-text search for the completed offline Tesseract TSV
-- producer.  Machine OCR is searchable operational evidence, not reviewed text,
-- identity evidence, event evidence, or publication material.

-- Publication object types are open text in the older tables.  Refuse to reserve
-- this lane over any state that predates the migration.
CREATE TABLE migration_0033_private_ocr_publication_preflight_guard(
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1)
);

CREATE TRIGGER migration_0033_private_ocr_publication_preflight_abort
BEFORE INSERT ON migration_0033_private_ocr_publication_preflight_guard
WHEN EXISTS (
        SELECT 1 FROM publication_decisions
        WHERE object_type IN (
            'private_ocr_import_receipt', 'private_ocr_frame_admission',
            'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
        )
    )
 OR EXISTS (
        SELECT 1 FROM publication_gate_decisions
        WHERE object_type IN (
            'private_ocr_import_receipt', 'private_ocr_frame_admission',
            'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
        )
    )
 OR EXISTS (
        SELECT 1 FROM observations
        WHERE observation_kind = 'ocr_tesseract_word_candidate'
    )
 OR EXISTS (
        SELECT 1 FROM artifacts WHERE artifact_kind = 'tesseract_tsv'
    )
 OR EXISTS (
        SELECT 1 FROM processing_runs WHERE stage = 'ocr_tesseract_tsv'
    )
BEGIN
    SELECT RAISE(ABORT, 'reserved private OCR state already exists');
END;

INSERT INTO migration_0033_private_ocr_publication_preflight_guard(singleton)
VALUES(1);
DROP TRIGGER migration_0033_private_ocr_publication_preflight_abort;
DROP TABLE migration_0033_private_ocr_publication_preflight_guard;

CREATE TABLE private_ocr_import_receipts (
    receipt_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    private_ocr_import_receipt_id TEXT NOT NULL UNIQUE,
    import_batch_id TEXT NOT NULL UNIQUE
        REFERENCES import_batches(import_batch_id) ON DELETE RESTRICT,
    result_uri TEXT NOT NULL,
    result_raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(result_raw_sha256) = 64),
    result_byte_count INTEGER NOT NULL CHECK(result_byte_count > 0),
    result_key TEXT NOT NULL UNIQUE CHECK(length(result_key) = 64),
    processing_run_id TEXT NOT NULL UNIQUE
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    sparse_frame_result_uri TEXT NOT NULL,
    sparse_frame_result_sha256 TEXT NOT NULL
        CHECK(length(sparse_frame_result_sha256) = 64),
    sparse_frame_result_key TEXT NOT NULL CHECK(length(sparse_frame_result_key) = 64),
    sparse_frame_processing_run_id TEXT NOT NULL
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    proxy_media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE RESTRICT,
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    selected_frame_count INTEGER NOT NULL CHECK(selected_frame_count > 0),
    admitted_frame_count INTEGER NOT NULL CHECK(admitted_frame_count > 0),
    word_count INTEGER NOT NULL CHECK(word_count >= 0),
    observation_count INTEGER NOT NULL CHECK(observation_count = word_count),
    tsv_artifact_count INTEGER NOT NULL
        CHECK(tsv_artifact_count = selected_frame_count),
    coordinate_system TEXT NOT NULL
        CHECK(coordinate_system = 'rendition_media_ms'),
    boundary TEXT NOT NULL CHECK(boundary = 'half_open'),
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    human_review TEXT NOT NULL CHECK(human_review = 'required'),
    redaction_state TEXT NOT NULL CHECK(redaction_state = 'pending'),
    score_calibration TEXT NOT NULL CHECK(score_calibration = 'not_calibrated'),
    calibrated_probabilities_present INTEGER NOT NULL
        CHECK(calibrated_probabilities_present = 0),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    identity_authority TEXT NOT NULL CHECK(identity_authority = 'none'),
    event_authority TEXT NOT NULL CHECK(event_authority = 'none'),
    export_authority TEXT NOT NULL CHECK(export_authority = 'none')
);

CREATE TABLE private_ocr_frame_admissions (
    frame_admission_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    private_ocr_frame_admission_id TEXT NOT NULL UNIQUE,
    private_ocr_import_receipt_id TEXT NOT NULL
        REFERENCES private_ocr_import_receipts(private_ocr_import_receipt_id)
        ON DELETE RESTRICT,
    frame_ordinal INTEGER NOT NULL CHECK(frame_ordinal >= 0),
    frame_id TEXT NOT NULL,
    sparse_frame_observation_id TEXT NOT NULL
        REFERENCES observations(observation_id) ON DELETE RESTRICT,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE RESTRICT,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE RESTRICT,
    source_rendition_id TEXT NOT NULL
        REFERENCES renditions(rendition_id) ON DELETE RESTRICT,
    source_media_id TEXT NOT NULL
        REFERENCES media_objects(media_id) ON DELETE RESTRICT,
    rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id) ON DELETE RESTRICT,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE RESTRICT,
    -- These are local PTS-derived coordinates on NEW.rendition_id/media_id, the
    -- low-resolution CFR proxy. source_* columns bind provenance only: they do not
    -- translate this interval onto the original rendition or recording timeline.
    start_ms INTEGER NOT NULL CHECK(start_ms >= 0),
    end_ms INTEGER NOT NULL CHECK(end_ms > start_ms),
    coordinate_system TEXT NOT NULL
        CHECK(coordinate_system = 'rendition_media_ms'),
    boundary TEXT NOT NULL CHECK(boundary = 'half_open'),
    requested_timestamp_ms INTEGER NOT NULL CHECK(requested_timestamp_ms >= 0),
    frame_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
    frame_png_sha256 TEXT NOT NULL CHECK(length(frame_png_sha256) = 64),
    tsv_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
    tsv_sha256 TEXT NOT NULL CHECK(length(tsv_sha256) = 64),
    text_presence TEXT NOT NULL CHECK(text_presence IN ('detected', 'not_detected')),
    word_count INTEGER NOT NULL CHECK(word_count >= 0),
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    human_review TEXT NOT NULL CHECK(human_review = 'required'),
    redaction_state TEXT NOT NULL CHECK(redaction_state = 'pending'),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    identity_authority TEXT NOT NULL CHECK(identity_authority = 'none'),
    event_authority TEXT NOT NULL CHECK(event_authority = 'none'),
    UNIQUE(private_ocr_import_receipt_id, rendition_id, frame_ordinal),
    UNIQUE(private_ocr_import_receipt_id, rendition_id, frame_id),
    CHECK((text_presence = 'detected') = (word_count > 0))
);

CREATE INDEX private_ocr_frames_rendition_time
    ON private_ocr_frame_admissions(rendition_id, start_ms, end_ms);
CREATE INDEX private_ocr_frames_source_filter_proxy_time
    ON private_ocr_frame_admissions(source_id, start_ms, end_ms);

CREATE TABLE private_ocr_word_admissions (
    word_admission_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    private_ocr_word_admission_id TEXT NOT NULL UNIQUE,
    private_ocr_frame_admission_id TEXT NOT NULL
        REFERENCES private_ocr_frame_admissions(private_ocr_frame_admission_id)
        ON DELETE RESTRICT,
    observation_id TEXT NOT NULL UNIQUE
        REFERENCES observations(observation_id) ON DELETE RESTRICT,
    region_id TEXT NOT NULL,
    word_ordinal INTEGER NOT NULL CHECK(word_ordinal >= 0),
    raw_text TEXT NOT NULL CHECK(length(raw_text) BETWEEN 1 AND 16384),
    raw_score REAL NOT NULL CHECK(raw_score >= 0 AND raw_score <= 100),
    raw_score_text TEXT NOT NULL,
    score_name TEXT NOT NULL
        CHECK(score_name = 'tesseract_raw_0_100_not_probability'),
    score_calibration TEXT NOT NULL CHECK(score_calibration = 'not_calibrated'),
    probability_interpretation TEXT NOT NULL
        CHECK(probability_interpretation = 'not_a_probability'),
    calibrated_probability REAL CHECK(calibrated_probability IS NULL),
    calibration_set_id TEXT CHECK(calibration_set_id IS NULL),
    rectangle_json TEXT NOT NULL CHECK(json_valid(rectangle_json)),
    reading_order_json TEXT NOT NULL CHECK(json_valid(reading_order_json)),
    language_choice_json TEXT NOT NULL CHECK(json_valid(language_choice_json)),
    script_choice_json TEXT NOT NULL CHECK(json_valid(script_choice_json)),
    engine_provenance_json TEXT NOT NULL CHECK(json_valid(engine_provenance_json)),
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    review_state TEXT NOT NULL CHECK(review_state = 'machine'),
    human_review TEXT NOT NULL CHECK(human_review = 'required'),
    redaction_state TEXT NOT NULL CHECK(redaction_state = 'pending'),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    identity_authority TEXT NOT NULL CHECK(identity_authority = 'none'),
    event_authority TEXT NOT NULL CHECK(event_authority = 'none'),
    UNIQUE(private_ocr_frame_admission_id, word_ordinal),
    UNIQUE(private_ocr_frame_admission_id, region_id)
);

-- Private operational index. The append-only admission row remains authoritative.
-- SQLite FTS5 virtual tables cannot own ordinary DML-blocking triggers. Therefore
-- direct INSERT/UPDATE/DELETE here is never authoritative; exact-replay, search, and
-- full database validation compare both directions and fail closed on any drift.
CREATE VIRTUAL TABLE private_ocr_word_fts USING fts5(
    private_ocr_word_admission_id UNINDEXED,
    raw_text,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER private_ocr_words_fts_insert
AFTER INSERT ON private_ocr_word_admissions
BEGIN
    INSERT INTO private_ocr_word_fts(private_ocr_word_admission_id, raw_text)
    VALUES(NEW.private_ocr_word_admission_id, NEW.raw_text);
END;

-- The exact source/recording/rendition/media and already-admitted sparse-frame
-- observation must still exist when a frame crosses the OCR admission boundary.
CREATE TRIGGER private_ocr_frame_anchor_is_exact
BEFORE INSERT ON private_ocr_frame_admissions
WHEN NOT EXISTS (
    SELECT 1
    FROM renditions AS proxy_rendition
    JOIN private_ocr_import_receipts AS receipt
      ON receipt.private_ocr_import_receipt_id =
         NEW.private_ocr_import_receipt_id
    JOIN media_objects AS proxy_media
      ON proxy_media.media_id = proxy_rendition.media_id
    JOIN renditions AS source_rendition
      ON source_rendition.rendition_id = NEW.source_rendition_id
     AND source_rendition.recording_id = proxy_rendition.recording_id
    JOIN media_objects AS source_media
      ON source_media.media_id = source_rendition.media_id
    JOIN media_sources AS media_source
      ON media_source.media_id = source_media.media_id
     AND media_source.source_id = NEW.source_id
    JOIN recording_sources AS recording_source
      ON recording_source.recording_id = proxy_rendition.recording_id
     AND recording_source.source_id = NEW.source_id
    JOIN sources AS source ON source.source_id = NEW.source_id
    JOIN recordings AS recording
      ON recording.recording_id = proxy_rendition.recording_id
    JOIN observations AS sparse
      ON sparse.observation_id = NEW.sparse_frame_observation_id
    JOIN artifacts AS frame_artifact
      ON frame_artifact.artifact_id = NEW.frame_artifact_id
    JOIN artifacts AS tsv_artifact
      ON tsv_artifact.artifact_id = NEW.tsv_artifact_id
    WHERE proxy_rendition.rendition_id = NEW.rendition_id
      AND proxy_rendition.recording_id = NEW.recording_id
      AND proxy_rendition.media_id = NEW.media_id
      AND receipt.proxy_media_id = NEW.media_id
      AND receipt.sparse_frame_processing_run_id = sparse.processing_run_id
      AND receipt.processing_run_id = tsv_artifact.processing_run_id
      AND receipt.coordinate_system = 'rendition_media_ms'
      AND receipt.boundary = 'half_open'
      AND (
          SELECT count(*) FROM private_ocr_frame_admissions AS existing_frame
          WHERE existing_frame.private_ocr_import_receipt_id =
                receipt.private_ocr_import_receipt_id
      ) < receipt.admitted_frame_count
      AND proxy_rendition.rendition_kind = 'low_resolution_cfr_proxy'
      AND proxy_rendition.review_state <> 'rejected'
      AND json_extract(proxy_rendition.metadata_json,
                       '$.derived_from_rendition_id') = NEW.source_rendition_id
      AND source_rendition.media_id = NEW.source_media_id
      AND source_rendition.review_state <> 'rejected'
      AND source_media.integrity_state = 'verified'
      AND source_media.media_kind = 'video'
      AND source.review_state <> 'rejected'
      AND recording.review_state <> 'rejected'
      AND recording.merged_into_recording_id IS NULL
      AND recording_source.confidence_state <> 'rejected'
      AND proxy_media.integrity_state = 'verified'
      AND proxy_media.media_kind = 'video'
      AND proxy_media.duration_ms IS NOT NULL
      AND NEW.end_ms <= proxy_media.duration_ms
      AND sparse.observation_kind = 'sparse_frame_routing_candidate'
      AND sparse.recording_id = NEW.recording_id
      AND sparse.rendition_id = NEW.rendition_id
      AND sparse.start_ms = NEW.start_ms
      AND sparse.end_ms = NEW.end_ms
      AND sparse.processing_run_id = receipt.sparse_frame_processing_run_id
      AND sparse.visibility = 'private'
      AND sparse.review_state = 'machine'
      AND json_extract(sparse.metadata_json, '$.frame_id') = NEW.frame_id
      AND json_extract(sparse.metadata_json, '$.artifact_id') = NEW.frame_artifact_id
      AND json_extract(sparse.metadata_json, '$.requested_timestamp_ms') =
          NEW.requested_timestamp_ms
      AND frame_artifact.sha256 = NEW.frame_png_sha256
      AND frame_artifact.visibility = 'private'
      AND tsv_artifact.artifact_kind = 'tesseract_tsv'
      AND tsv_artifact.sha256 = NEW.tsv_sha256
      AND tsv_artifact.visibility = 'private'
)
BEGIN
    SELECT RAISE(ABORT, 'private OCR frame requires exact source/rendition/sparse-frame lineage');
END;

CREATE TRIGGER private_ocr_word_observation_is_private
BEFORE INSERT ON private_ocr_word_admissions
WHEN NOT EXISTS (
    SELECT 1
    FROM private_ocr_frame_admissions AS frame
    JOIN private_ocr_import_receipts AS receipt
      ON receipt.private_ocr_import_receipt_id =
         frame.private_ocr_import_receipt_id
    JOIN observations AS observation
      ON observation.observation_id = NEW.observation_id
    JOIN ocr_observations AS ocr
      ON ocr.observation_id = observation.observation_id
    JOIN observation_scores AS score
      ON score.observation_id = observation.observation_id
    WHERE frame.private_ocr_frame_admission_id =
          NEW.private_ocr_frame_admission_id
      AND observation.observation_kind = 'ocr_tesseract_word_candidate'
      AND observation.recording_id = frame.recording_id
      AND observation.rendition_id = frame.rendition_id
      AND observation.processing_run_id = receipt.processing_run_id
      AND observation.start_ms = frame.start_ms
      AND observation.end_ms = frame.end_ms
      AND observation.visibility = 'private'
      AND observation.review_state = 'machine'
      AND json_extract(observation.metadata_json, '$.coordinate_system') =
          'rendition_media_ms'
      AND json_extract(observation.metadata_json, '$.human_review') = 'required'
      AND json_extract(observation.metadata_json, '$.redaction_state') = 'pending'
      AND json_extract(observation.metadata_json, '$.publication_authority') = 'none'
      AND json_extract(observation.metadata_json, '$.identity_authority') = 'none'
      AND json_extract(observation.metadata_json, '$.event_authority') = 'none'
      AND json_extract(observation.metadata_json, '$.frame_id') = frame.frame_id
      AND json_extract(observation.metadata_json, '$.region_id') = NEW.region_id
      AND json_extract(observation.metadata_json, '$.source_id') = frame.source_id
      AND ocr.raw_text = NEW.raw_text
      AND ocr.normalized_text IS NULL
      AND ocr.redaction_state = 'pending'
      AND score.score_name = 'tesseract_raw_0_100_not_probability'
      AND score.raw_score = NEW.raw_score
      AND score.calibrated_probability IS NULL
      AND score.calibration_set_id IS NULL
      AND (
          SELECT count(*) FROM private_ocr_word_admissions AS existing_word
          WHERE existing_word.private_ocr_frame_admission_id =
                frame.private_ocr_frame_admission_id
      ) < frame.word_count
      AND NOT EXISTS (
          SELECT 1 FROM appearances
          WHERE observation_id = NEW.observation_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM identity_cluster_memberships
          WHERE observation_id = NEW.observation_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM event_evidence
          WHERE observation_id = NEW.observation_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM claim_catalog_links
          WHERE observation_id = NEW.observation_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM publication_decisions
          WHERE object_type = 'observation'
            AND object_id = NEW.observation_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM publication_gate_decisions
          WHERE object_type = 'observation'
            AND object_id = NEW.observation_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'private OCR word requires an uncalibrated private OCR observation');
END;

-- Receipt and OCR admission ledgers are immutable. Exact replay matches existing
-- rows; it never rewrites them or reinserts FTS content.
-- BEFORE INSERT collision guards cover every PRIMARY KEY/rowid and UNIQUE target.
-- SQLite can otherwise implement INSERT OR REPLACE by deleting the conflicting row
-- without running DELETE triggers when recursive_triggers is disabled.
CREATE TRIGGER private_ocr_receipts_no_replace
BEFORE INSERT ON private_ocr_import_receipts
WHEN EXISTS (
    SELECT 1 FROM private_ocr_import_receipts AS existing
    WHERE existing.receipt_sequence = NEW.receipt_sequence
       OR existing.private_ocr_import_receipt_id =
          NEW.private_ocr_import_receipt_id
       OR existing.import_batch_id = NEW.import_batch_id
       OR existing.result_raw_sha256 = NEW.result_raw_sha256
       OR existing.result_key = NEW.result_key
       OR existing.processing_run_id = NEW.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'private OCR receipt replacement is forbidden'); END;

CREATE TRIGGER private_ocr_frames_no_replace
BEFORE INSERT ON private_ocr_frame_admissions
WHEN EXISTS (
    SELECT 1 FROM private_ocr_frame_admissions AS existing
    WHERE existing.frame_admission_sequence = NEW.frame_admission_sequence
       OR existing.private_ocr_frame_admission_id =
          NEW.private_ocr_frame_admission_id
       OR (existing.private_ocr_import_receipt_id =
              NEW.private_ocr_import_receipt_id
           AND existing.rendition_id = NEW.rendition_id
           AND existing.frame_ordinal = NEW.frame_ordinal)
       OR (existing.private_ocr_import_receipt_id =
              NEW.private_ocr_import_receipt_id
           AND existing.rendition_id = NEW.rendition_id
           AND existing.frame_id = NEW.frame_id)
)
BEGIN SELECT RAISE(ABORT, 'private OCR frame replacement is forbidden'); END;

CREATE TRIGGER private_ocr_words_no_replace
BEFORE INSERT ON private_ocr_word_admissions
WHEN EXISTS (
    SELECT 1 FROM private_ocr_word_admissions AS existing
    WHERE existing.word_admission_sequence = NEW.word_admission_sequence
       OR existing.private_ocr_word_admission_id =
          NEW.private_ocr_word_admission_id
       OR existing.observation_id = NEW.observation_id
       OR (existing.private_ocr_frame_admission_id =
              NEW.private_ocr_frame_admission_id
           AND existing.word_ordinal = NEW.word_ordinal)
       OR (existing.private_ocr_frame_admission_id =
              NEW.private_ocr_frame_admission_id
           AND existing.region_id = NEW.region_id)
)
BEGIN SELECT RAISE(ABORT, 'private OCR word replacement is forbidden'); END;

CREATE TRIGGER private_ocr_import_batches_no_replace
BEFORE INSERT ON import_batches
WHEN EXISTS (
    SELECT 1 FROM import_batches AS existing
    WHERE (
            existing.rowid = NEW.rowid
         OR existing.import_batch_id = NEW.import_batch_id
         OR (existing.importer_name = NEW.importer_name
             AND existing.input_sha256 = NEW.input_sha256)
          )
      AND (
            existing.importer_name = 'private_ocr_tesseract_result_v1'
         OR NEW.importer_name = 'private_ocr_tesseract_result_v1'
         OR EXISTS (
                SELECT 1 FROM private_ocr_import_receipts AS receipt
                WHERE receipt.import_batch_id = existing.import_batch_id
            )
          )
)
BEGIN SELECT RAISE(ABORT, 'private OCR import-batch replacement is forbidden'); END;

CREATE TRIGGER private_ocr_processing_runs_no_replace
BEFORE INSERT ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM processing_runs AS existing
    WHERE (existing.rowid = NEW.rowid
           OR existing.processing_run_id = NEW.processing_run_id)
      AND (existing.stage = 'ocr_tesseract_tsv'
           OR NEW.stage = 'ocr_tesseract_tsv')
)
BEGIN SELECT RAISE(ABORT, 'private OCR processing-run replacement is forbidden'); END;

CREATE TRIGGER private_ocr_run_inputs_no_replace
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM run_inputs AS existing
    WHERE (
            existing.rowid = NEW.rowid
         OR existing.run_input_id = NEW.run_input_id
         OR (existing.processing_run_id = NEW.processing_run_id
             AND existing.object_type = NEW.object_type
             AND existing.object_id = NEW.object_id
             AND existing.input_role = NEW.input_role)
          )
      AND (
            EXISTS (
                SELECT 1 FROM processing_runs AS run
                WHERE run.processing_run_id = existing.processing_run_id
                  AND run.stage = 'ocr_tesseract_tsv'
            )
         OR EXISTS (
                SELECT 1 FROM processing_runs AS run
                WHERE run.processing_run_id = NEW.processing_run_id
                  AND run.stage = 'ocr_tesseract_tsv'
            )
          )
)
BEGIN SELECT RAISE(ABORT, 'private OCR run-input replacement is forbidden'); END;

CREATE TRIGGER private_ocr_artifacts_no_replace
BEFORE INSERT ON artifacts
WHEN EXISTS (
    SELECT 1 FROM artifacts AS existing
    WHERE (
            existing.rowid = NEW.rowid
         OR existing.artifact_id = NEW.artifact_id
         OR (existing.storage_uri = NEW.storage_uri
             AND existing.sha256 = NEW.sha256)
          )
      AND (
            existing.artifact_kind = 'tesseract_tsv'
         OR NEW.artifact_kind = 'tesseract_tsv'
         OR EXISTS (
                SELECT 1 FROM processing_runs AS run
                WHERE run.processing_run_id IN (
                    existing.processing_run_id, NEW.processing_run_id
                )
                  AND run.stage = 'ocr_tesseract_tsv'
            )
          )
)
BEGIN SELECT RAISE(ABORT, 'private OCR artifact replacement is forbidden'); END;

CREATE TRIGGER private_ocr_observations_no_replace
BEFORE INSERT ON observations
WHEN EXISTS (
    SELECT 1 FROM observations AS existing
    WHERE (existing.rowid = NEW.rowid
           OR existing.observation_id = NEW.observation_id)
      AND (existing.observation_kind = 'ocr_tesseract_word_candidate'
           OR NEW.observation_kind = 'ocr_tesseract_word_candidate')
)
BEGIN SELECT RAISE(ABORT, 'private OCR observation replacement is forbidden'); END;

CREATE TRIGGER private_ocr_details_no_replace
BEFORE INSERT ON ocr_observations
WHEN EXISTS (
    SELECT 1 FROM ocr_observations AS existing
    WHERE (existing.rowid = NEW.rowid
           OR existing.observation_id = NEW.observation_id)
      AND (
            EXISTS (
                SELECT 1 FROM observations AS observation
                WHERE observation.observation_id = existing.observation_id
                  AND observation.observation_kind =
                      'ocr_tesseract_word_candidate'
            )
         OR EXISTS (
                SELECT 1 FROM observations AS observation
                WHERE observation.observation_id = NEW.observation_id
                  AND observation.observation_kind =
                      'ocr_tesseract_word_candidate'
            )
          )
)
BEGIN SELECT RAISE(ABORT, 'private OCR detail replacement is forbidden'); END;

CREATE TRIGGER private_ocr_scores_no_replace
BEFORE INSERT ON observation_scores
WHEN EXISTS (
    SELECT 1 FROM observation_scores AS existing
    WHERE (
            existing.rowid = NEW.rowid
         OR existing.observation_score_id = NEW.observation_score_id
         OR (existing.observation_id = NEW.observation_id
             AND existing.score_name = NEW.score_name)
          )
      AND (
            EXISTS (
                SELECT 1 FROM observations AS observation
                WHERE observation.observation_id = existing.observation_id
                  AND observation.observation_kind =
                      'ocr_tesseract_word_candidate'
            )
         OR EXISTS (
                SELECT 1 FROM observations AS observation
                WHERE observation.observation_id = NEW.observation_id
                  AND observation.observation_kind =
                      'ocr_tesseract_word_candidate'
            )
          )
)
BEGIN SELECT RAISE(ABORT, 'private OCR score replacement is forbidden'); END;

CREATE TRIGGER private_ocr_receipts_no_update
BEFORE UPDATE ON private_ocr_import_receipts
BEGIN SELECT RAISE(ABORT, 'private OCR receipts are append-only'); END;
CREATE TRIGGER private_ocr_receipts_no_delete
BEFORE DELETE ON private_ocr_import_receipts
BEGIN SELECT RAISE(ABORT, 'private OCR receipts are append-only'); END;
CREATE TRIGGER private_ocr_frames_no_update
BEFORE UPDATE ON private_ocr_frame_admissions
BEGIN SELECT RAISE(ABORT, 'private OCR frame admissions are append-only'); END;
CREATE TRIGGER private_ocr_frames_no_delete
BEFORE DELETE ON private_ocr_frame_admissions
BEGIN SELECT RAISE(ABORT, 'private OCR frame admissions are append-only'); END;
CREATE TRIGGER private_ocr_words_no_update
BEFORE UPDATE ON private_ocr_word_admissions
BEGIN SELECT RAISE(ABORT, 'private OCR word admissions are append-only'); END;
CREATE TRIGGER private_ocr_words_no_delete
BEFORE DELETE ON private_ocr_word_admissions
BEGIN SELECT RAISE(ABORT, 'private OCR word admissions are append-only'); END;

CREATE TRIGGER private_ocr_import_batches_no_update
BEFORE UPDATE ON import_batches
WHEN EXISTS (
    SELECT 1 FROM private_ocr_import_receipts
    WHERE import_batch_id IN (OLD.import_batch_id, NEW.import_batch_id)
)
BEGIN SELECT RAISE(ABORT, 'private OCR import provenance is append-only'); END;
CREATE TRIGGER private_ocr_import_batches_no_delete
BEFORE DELETE ON import_batches
WHEN EXISTS (
    SELECT 1 FROM private_ocr_import_receipts
    WHERE import_batch_id = OLD.import_batch_id
)
BEGIN SELECT RAISE(ABORT, 'private OCR import provenance is append-only'); END;

CREATE TRIGGER private_ocr_processing_runs_no_update
BEFORE UPDATE ON processing_runs
WHEN OLD.stage = 'ocr_tesseract_tsv' OR NEW.stage = 'ocr_tesseract_tsv'
BEGIN SELECT RAISE(ABORT, 'private OCR processing provenance is append-only'); END;
CREATE TRIGGER private_ocr_processing_runs_no_delete
BEFORE DELETE ON processing_runs
WHEN OLD.stage = 'ocr_tesseract_tsv'
BEGIN SELECT RAISE(ABORT, 'private OCR processing provenance is append-only'); END;

CREATE TRIGGER private_ocr_run_inputs_no_insert_after_receipt
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM private_ocr_import_receipts
    WHERE processing_run_id = NEW.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'private OCR processing inputs are sealed'); END;
CREATE TRIGGER private_ocr_run_inputs_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM processing_runs
    WHERE stage = 'ocr_tesseract_tsv'
      AND processing_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
)
BEGIN SELECT RAISE(ABORT, 'private OCR processing inputs are append-only'); END;
CREATE TRIGGER private_ocr_run_inputs_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM processing_runs
    WHERE stage = 'ocr_tesseract_tsv'
      AND processing_run_id = OLD.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'private OCR processing inputs are append-only'); END;

CREATE TRIGGER private_ocr_observations_no_update
BEFORE UPDATE ON observations
WHEN OLD.observation_kind = 'ocr_tesseract_word_candidate'
  OR NEW.observation_kind = 'ocr_tesseract_word_candidate'
BEGIN SELECT RAISE(ABORT, 'private OCR observations are append-only'); END;
CREATE TRIGGER private_ocr_observations_no_delete
BEFORE DELETE ON observations
WHEN OLD.observation_kind = 'ocr_tesseract_word_candidate'
BEGIN SELECT RAISE(ABORT, 'private OCR observations are append-only'); END;
CREATE TRIGGER private_ocr_details_no_update
BEFORE UPDATE ON ocr_observations
WHEN EXISTS (
    SELECT 1 FROM observations AS observation
    WHERE observation.observation_id IN (OLD.observation_id, NEW.observation_id)
      AND observation.observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR details are append-only'); END;
CREATE TRIGGER private_ocr_details_no_delete
BEFORE DELETE ON ocr_observations
WHEN EXISTS (
    SELECT 1 FROM observations AS observation
    WHERE observation.observation_id = OLD.observation_id
      AND observation.observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR details are append-only'); END;
CREATE TRIGGER private_ocr_scores_no_update
BEFORE UPDATE ON observation_scores
WHEN EXISTS (
    SELECT 1 FROM observations AS observation
    WHERE observation.observation_id IN (OLD.observation_id, NEW.observation_id)
      AND observation.observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR scores are append-only'); END;

CREATE TRIGGER private_ocr_observation_insert_policy
BEFORE INSERT ON observations
WHEN NEW.observation_kind = 'ocr_tesseract_word_candidate'
 AND (
      NEW.rendition_id IS NULL
      OR NEW.processing_run_id IS NULL
      OR NEW.visibility IS NOT 'private'
      OR NEW.review_state IS NOT 'machine'
      OR NEW.payload_schema_version IS NOT 1
      OR json_extract(NEW.metadata_json, '$.coordinate_system')
            IS NOT 'rendition_media_ms'
      OR json_extract(NEW.metadata_json, '$.human_review') IS NOT 'required'
      OR json_extract(NEW.metadata_json, '$.redaction_state') IS NOT 'pending'
      OR json_extract(NEW.metadata_json, '$.publication_authority') IS NOT 'none'
      OR json_extract(NEW.metadata_json, '$.identity_authority') IS NOT 'none'
      OR json_extract(NEW.metadata_json, '$.event_authority') IS NOT 'none'
      OR NOT EXISTS (
          SELECT 1
          FROM private_ocr_frame_admissions AS frame
          JOIN private_ocr_import_receipts AS receipt
            ON receipt.private_ocr_import_receipt_id =
               frame.private_ocr_import_receipt_id
          WHERE receipt.processing_run_id = NEW.processing_run_id
            AND frame.recording_id = NEW.recording_id
            AND frame.rendition_id = NEW.rendition_id
            AND frame.start_ms = NEW.start_ms
            AND frame.end_ms = NEW.end_ms
            AND frame.frame_id =
                json_extract(NEW.metadata_json, '$.frame_id')
            AND json_type(NEW.metadata_json, '$.region_id') = 'text'
            AND length(json_extract(NEW.metadata_json, '$.region_id')) > 0
            AND (
                SELECT count(*)
                FROM private_ocr_word_admissions AS existing_word
                WHERE existing_word.private_ocr_frame_admission_id =
                      frame.private_ocr_frame_admission_id
            ) < frame.word_count
      )
      OR EXISTS (
          SELECT 1 FROM publication_decisions
          WHERE object_type = 'observation' AND object_id = NEW.observation_id
      )
      OR EXISTS (
          SELECT 1 FROM publication_gate_decisions
          WHERE object_type = 'observation' AND object_id = NEW.observation_id
      )
 )
BEGIN SELECT RAISE(ABORT, 'private OCR observation policy is fixed'); END;

CREATE TRIGGER private_ocr_detail_insert_policy
BEFORE INSERT ON ocr_observations
WHEN EXISTS (
    SELECT 1 FROM observations AS observation
    WHERE observation.observation_id = NEW.observation_id
      AND observation.observation_kind = 'ocr_tesseract_word_candidate'
)
 AND (
      NEW.normalized_text IS NOT NULL
      OR NEW.redaction_state <> 'pending'
 )
BEGIN SELECT RAISE(ABORT, 'private OCR detail must remain raw and redaction-pending'); END;

CREATE TRIGGER private_ocr_score_insert_policy
BEFORE INSERT ON observation_scores
WHEN EXISTS (
    SELECT 1 FROM observations AS observation
    WHERE observation.observation_id = NEW.observation_id
      AND observation.observation_kind = 'ocr_tesseract_word_candidate'
)
 AND (
      NEW.score_name <> 'tesseract_raw_0_100_not_probability'
      OR NEW.raw_score IS NULL
      OR NEW.raw_score < 0
      OR NEW.raw_score > 100
      OR NEW.calibrated_probability IS NOT NULL
      OR NEW.calibration_set_id IS NOT NULL
 )
BEGIN SELECT RAISE(ABORT, 'private OCR score is raw and never a probability'); END;

CREATE TRIGGER private_ocr_tsv_artifacts_no_update
BEFORE UPDATE ON artifacts
WHEN OLD.artifact_kind = 'tesseract_tsv' OR NEW.artifact_kind = 'tesseract_tsv'
  OR EXISTS (
      SELECT 1 FROM private_ocr_import_receipts
      WHERE processing_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
  )
BEGIN SELECT RAISE(ABORT, 'private OCR TSV artifacts are append-only'); END;
CREATE TRIGGER private_ocr_tsv_artifacts_no_delete
BEFORE DELETE ON artifacts
WHEN OLD.artifact_kind = 'tesseract_tsv'
  OR EXISTS (
      SELECT 1 FROM private_ocr_import_receipts
      WHERE processing_run_id = OLD.processing_run_id
  )
BEGIN SELECT RAISE(ABORT, 'private OCR TSV artifacts are append-only'); END;
CREATE TRIGGER private_ocr_artifacts_no_insert_after_receipt
BEFORE INSERT ON artifacts
WHEN EXISTS (
    SELECT 1 FROM private_ocr_import_receipts
    WHERE processing_run_id = NEW.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'private OCR processing artifacts are sealed'); END;
CREATE TRIGGER private_ocr_tsv_artifact_insert_policy
BEFORE INSERT ON artifacts
WHEN NEW.artifact_kind = 'tesseract_tsv'
 AND (
      NEW.processing_run_id IS NULL
      OR NEW.visibility IS NOT 'private'
      OR EXISTS (
          SELECT 1 FROM publication_decisions
          WHERE object_type = 'artifact' AND object_id = NEW.artifact_id
      )
      OR EXISTS (
          SELECT 1 FROM publication_gate_decisions
          WHERE object_type = 'artifact' AND object_id = NEW.artifact_id
      )
 )
BEGIN SELECT RAISE(ABORT, 'private OCR TSV artifact policy is fixed'); END;
CREATE TRIGGER private_ocr_scores_no_delete
BEFORE DELETE ON observation_scores
WHEN EXISTS (
    SELECT 1 FROM observations AS observation
    WHERE observation.observation_id = OLD.observation_id
      AND observation.observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR scores are append-only'); END;

-- Machine OCR never becomes identity, event, claim, publication, gate, or export
-- authority. A reviewer may create a separate, human-governed object later.
CREATE TRIGGER private_ocr_publication_forbidden
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type IN (
        'private_ocr_import_receipt', 'private_ocr_frame_admission',
        'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
     )
  OR (NEW.object_type = 'observation' AND EXISTS (
        SELECT 1 FROM observations
        WHERE observation_id = NEW.object_id
          AND observation_kind = 'ocr_tesseract_word_candidate'
     ))
  OR (NEW.object_type = 'artifact' AND EXISTS (
        SELECT 1 FROM artifacts
        WHERE artifact_id = NEW.object_id
          AND artifact_kind = 'tesseract_tsv'
     ))
BEGIN SELECT RAISE(ABORT, 'private OCR objects have no publication lane'); END;
CREATE TRIGGER private_ocr_publication_gate_forbidden
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type IN (
        'private_ocr_import_receipt', 'private_ocr_frame_admission',
        'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
     )
  OR (NEW.object_type = 'observation' AND EXISTS (
        SELECT 1 FROM observations
        WHERE observation_id = NEW.object_id
          AND observation_kind = 'ocr_tesseract_word_candidate'
     ))
  OR (NEW.object_type = 'artifact' AND EXISTS (
        SELECT 1 FROM artifacts
        WHERE artifact_id = NEW.object_id
          AND artifact_kind = 'tesseract_tsv'
     ))
BEGIN SELECT RAISE(ABORT, 'private OCR objects have no publication-gate lane'); END;
CREATE TRIGGER private_ocr_appearance_forbidden
BEFORE INSERT ON appearances
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no identity authority'); END;
CREATE TRIGGER private_ocr_identity_membership_forbidden
BEFORE INSERT ON identity_cluster_memberships
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no identity-cluster authority'); END;
CREATE TRIGGER private_ocr_event_evidence_forbidden
BEFORE INSERT ON event_evidence
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no event authority'); END;
CREATE TRIGGER private_ocr_claim_evidence_forbidden
BEFORE INSERT ON claim_catalog_links
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no claim authority'); END;

CREATE TRIGGER private_ocr_appearance_update_forbidden
BEFORE UPDATE ON appearances
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
 OR OLD.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no identity authority'); END;
CREATE TRIGGER private_ocr_identity_membership_update_forbidden
BEFORE UPDATE ON identity_cluster_memberships
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
 OR OLD.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no identity-cluster authority'); END;
CREATE TRIGGER private_ocr_event_evidence_update_forbidden
BEFORE UPDATE ON event_evidence
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
 OR OLD.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no event authority'); END;
CREATE TRIGGER private_ocr_claim_evidence_update_forbidden
BEFORE UPDATE ON claim_catalog_links
WHEN NEW.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
 OR OLD.observation_id IN (
    SELECT observation_id FROM observations
    WHERE observation_kind = 'ocr_tesseract_word_candidate'
)
BEGIN SELECT RAISE(ABORT, 'private OCR has no claim authority'); END;
