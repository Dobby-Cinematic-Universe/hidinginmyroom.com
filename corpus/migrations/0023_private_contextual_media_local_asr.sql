-- Private neutral-glossary registration and paired contextual media-local ASR.
--
-- This lane retains a second machine hypothesis next to an already admitted raw
-- media-local revision.  Pair and diff rows are measurements only: they cannot
-- select, correct, review, or publish either transcript.

CREATE TABLE private_glossary_registrations (
    private_glossary_registration_id TEXT PRIMARY KEY,
    glossary_revision_id TEXT NOT NULL UNIQUE
        REFERENCES glossary_revisions(glossary_revision_id) ON DELETE RESTRICT,
    artifact_uri TEXT NOT NULL UNIQUE,
    raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(raw_sha256) = 64),
    canonical_sha256 TEXT NOT NULL UNIQUE CHECK(length(canonical_sha256) = 64),
    byte_count INTEGER NOT NULL CHECK(byte_count > 0),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    revision_label TEXT NOT NULL,
    revision_sha256 TEXT NOT NULL CHECK(length(revision_sha256) = 64),
    language TEXT NOT NULL,
    prompt_sha256 TEXT NOT NULL CHECK(length(prompt_sha256) = 64),
    term_count INTEGER NOT NULL CHECK(term_count BETWEEN 1 AND 128),
    terms_stored_in_catalog INTEGER NOT NULL CHECK(terms_stored_in_catalog = 0),
    review_state TEXT NOT NULL CHECK(review_state = 'machine_candidate_unreviewed'),
    accuracy_claimed INTEGER NOT NULL CHECK(accuracy_claimed = 0),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    plan_sha256 TEXT NOT NULL UNIQUE CHECK(length(plan_sha256) = 64),
    registered_at TEXT NOT NULL CHECK(julianday(registered_at) IS NOT NULL)
);

CREATE TABLE contextual_asr_batch_registrations (
    contextual_batch_id TEXT PRIMARY KEY,
    identity_sha256 TEXT NOT NULL UNIQUE CHECK(length(identity_sha256) = 64),
    manifest_uri TEXT NOT NULL UNIQUE,
    manifest_raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(manifest_raw_sha256) = 64),
    manifest_canonical_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(manifest_canonical_sha256) = 64),
    manifest_byte_count INTEGER NOT NULL CHECK(manifest_byte_count > 0),
    materializer TEXT NOT NULL CHECK(materializer = 'himr-contextual-asr-batch'),
    materializer_version TEXT NOT NULL,
    work_order_count INTEGER NOT NULL CHECK(work_order_count > 0),
    glossary_revision_id TEXT NOT NULL
        REFERENCES private_glossary_registrations(glossary_revision_id)
        ON DELETE RESTRICT,
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    registered_at TEXT NOT NULL CHECK(julianday(registered_at) IS NOT NULL)
);

CREATE TABLE contextual_media_local_asr_imports (
    contextual_asr_import_id TEXT PRIMARY KEY,
    import_batch_id TEXT NOT NULL UNIQUE REFERENCES import_batches(import_batch_id),
    media_local_revision_id TEXT NOT NULL UNIQUE
        REFERENCES media_local_transcript_revisions(media_local_revision_id)
        ON DELETE RESTRICT,
    contextual_batch_id TEXT NOT NULL
        REFERENCES contextual_asr_batch_registrations(contextual_batch_id)
        ON DELETE RESTRICT,
    batch_ordinal INTEGER NOT NULL CHECK(batch_ordinal > 0),
    pair_projection_sha256 TEXT NOT NULL CHECK(length(pair_projection_sha256) = 64),
    work_order_uri TEXT NOT NULL,
    work_order_raw_sha256 TEXT NOT NULL CHECK(length(work_order_raw_sha256) = 64),
    work_order_canonical_sha256 TEXT NOT NULL CHECK(length(work_order_canonical_sha256) = 64),
    result_uri TEXT NOT NULL UNIQUE,
    result_raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(result_raw_sha256) = 64),
    result_canonical_sha256 TEXT NOT NULL UNIQUE CHECK(length(result_canonical_sha256) = 64),
    result_byte_count INTEGER NOT NULL CHECK(result_byte_count > 0),
    input_media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    input_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    input_duration_ms INTEGER NOT NULL CHECK(input_duration_ms > 0),
    max_segment_end_ms INTEGER NOT NULL CHECK(max_segment_end_ms >= 0),
    input_boundary_overrun_ms INTEGER NOT NULL CHECK(input_boundary_overrun_ms >= 0),
    null_timed_word_count INTEGER NOT NULL CHECK(null_timed_word_count >= 0),
    result_filesystem_state TEXT NOT NULL
        CHECK(result_filesystem_state = 'stable_hash_bound_no_seal_claim'),
    plan_sha256 TEXT NOT NULL UNIQUE CHECK(length(plan_sha256) = 64),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(contextual_batch_id, batch_ordinal),
    CHECK(input_boundary_overrun_ms = max(0, max_segment_end_ms - input_duration_ms))
);

CREATE TABLE contextual_media_local_asr_pairs (
    contextual_pair_id TEXT PRIMARY KEY,
    baseline_media_local_revision_id TEXT NOT NULL
        REFERENCES media_local_transcript_revisions(media_local_revision_id)
        ON DELETE RESTRICT,
    contextual_media_local_revision_id TEXT NOT NULL UNIQUE
        REFERENCES media_local_transcript_revisions(media_local_revision_id)
        ON DELETE RESTRICT,
    contextual_asr_import_id TEXT NOT NULL UNIQUE
        REFERENCES contextual_media_local_asr_imports(contextual_asr_import_id)
        ON DELETE RESTRICT,
    glossary_revision_id TEXT NOT NULL
        REFERENCES private_glossary_registrations(glossary_revision_id)
        ON DELETE RESTRICT,
    pair_projection_sha256 TEXT NOT NULL CHECK(length(pair_projection_sha256) = 64),
    pair_state TEXT NOT NULL
        CHECK(pair_state = 'competing_machine_revisions_no_preference'),
    input_equal INTEGER NOT NULL CHECK(input_equal = 1),
    engine_equal INTEGER NOT NULL CHECK(engine_equal = 1),
    model_equal INTEGER NOT NULL CHECK(model_equal = 1),
    window_equal INTEGER NOT NULL CHECK(window_equal = 1),
    inference_equal INTEGER NOT NULL CHECK(inference_equal = 1),
    catalog_context_equal INTEGER NOT NULL CHECK(catalog_context_equal = 1),
    only_glossary_job_output_differ INTEGER NOT NULL
        CHECK(only_glossary_job_output_differ = 1),
    preferred_revision_id TEXT CHECK(preferred_revision_id IS NULL),
    correction_asserted INTEGER NOT NULL CHECK(correction_asserted = 0),
    accuracy_claimed INTEGER NOT NULL CHECK(accuracy_claimed = 0),
    improvement_claimed INTEGER NOT NULL CHECK(improvement_claimed = 0),
    human_review_claimed INTEGER NOT NULL CHECK(human_review_claimed = 0),
    automatic_merge_allowed INTEGER NOT NULL CHECK(automatic_merge_allowed = 0),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    CHECK(baseline_media_local_revision_id <> contextual_media_local_revision_id)
);

CREATE TABLE contextual_asr_text_private_diffs (
    contextual_diff_id TEXT PRIMARY KEY,
    contextual_pair_id TEXT NOT NULL UNIQUE
        REFERENCES contextual_media_local_asr_pairs(contextual_pair_id)
        ON DELETE RESTRICT,
    diff_uri TEXT NOT NULL UNIQUE,
    diff_raw_sha256 TEXT NOT NULL UNIQUE CHECK(length(diff_raw_sha256) = 64),
    diff_canonical_sha256 TEXT NOT NULL UNIQUE CHECK(length(diff_canonical_sha256) = 64),
    diff_byte_count INTEGER NOT NULL CHECK(diff_byte_count > 0),
    diff_identity_sha256 TEXT NOT NULL UNIQUE CHECK(length(diff_identity_sha256) = 64),
    block_ms INTEGER NOT NULL CHECK(block_ms BETWEEN 5000 AND 120000),
    total_blocks INTEGER NOT NULL CHECK(total_blocks >= 0),
    changed_blocks INTEGER NOT NULL CHECK(changed_blocks >= 0),
    unchanged_blocks INTEGER NOT NULL CHECK(unchanged_blocks >= 0),
    total_character_edit_distance INTEGER NOT NULL CHECK(total_character_edit_distance >= 0),
    empty_nonempty_transitions INTEGER NOT NULL CHECK(empty_nonempty_transitions >= 0),
    maximum_absolute_first_token_start_drift_ms INTEGER NOT NULL CHECK(maximum_absolute_first_token_start_drift_ms >= 0),
    maximum_absolute_last_token_end_drift_ms INTEGER NOT NULL CHECK(maximum_absolute_last_token_end_drift_ms >= 0),
    baseline_lexical_tokens INTEGER NOT NULL CHECK(baseline_lexical_tokens >= 0),
    contextual_lexical_tokens INTEGER NOT NULL CHECK(contextual_lexical_tokens >= 0),
    baseline_untimed_lexical_tokens INTEGER NOT NULL CHECK(baseline_untimed_lexical_tokens >= 0),
    contextual_untimed_lexical_tokens INTEGER NOT NULL CHECK(contextual_untimed_lexical_tokens >= 0),
    glossary_term_metric_count INTEGER NOT NULL CHECK(glossary_term_metric_count >= 0),
    transcript_text_stored INTEGER NOT NULL CHECK(transcript_text_stored = 0),
    decoder_scores_calibrated INTEGER NOT NULL CHECK(decoder_scores_calibrated = 0),
    accuracy_claimed INTEGER NOT NULL CHECK(accuracy_claimed = 0),
    improvement_claimed INTEGER NOT NULL CHECK(improvement_claimed = 0),
    preferred_revision_selected INTEGER NOT NULL CHECK(preferred_revision_selected = 0),
    human_review_claimed INTEGER NOT NULL CHECK(human_review_claimed = 0),
    automatic_merge_allowed INTEGER NOT NULL CHECK(automatic_merge_allowed = 0),
    visibility TEXT NOT NULL CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    CHECK(total_blocks = changed_blocks + unchanged_blocks)
);

CREATE INDEX contextual_media_local_pair_baseline_idx
    ON contextual_media_local_asr_pairs(baseline_media_local_revision_id);
CREATE INDEX contextual_media_local_import_batch_idx
    ON contextual_media_local_asr_imports(contextual_batch_id, batch_ordinal);

CREATE TRIGGER private_glossary_registrations_no_update
BEFORE UPDATE ON private_glossary_registrations
BEGIN SELECT RAISE(ABORT, 'private glossary registrations are append-only'); END;
CREATE TRIGGER private_glossary_registrations_no_delete
BEFORE DELETE ON private_glossary_registrations
BEGIN SELECT RAISE(ABORT, 'private glossary registrations are append-only'); END;
CREATE TRIGGER contextual_asr_batches_no_update
BEFORE UPDATE ON contextual_asr_batch_registrations
BEGIN SELECT RAISE(ABORT, 'contextual ASR batch registrations are append-only'); END;
CREATE TRIGGER contextual_asr_batches_no_delete
BEFORE DELETE ON contextual_asr_batch_registrations
BEGIN SELECT RAISE(ABORT, 'contextual ASR batch registrations are append-only'); END;
CREATE TRIGGER contextual_media_imports_no_update
BEFORE UPDATE ON contextual_media_local_asr_imports
BEGIN SELECT RAISE(ABORT, 'contextual media-local ASR imports are append-only'); END;
CREATE TRIGGER contextual_media_imports_no_delete
BEFORE DELETE ON contextual_media_local_asr_imports
BEGIN SELECT RAISE(ABORT, 'contextual media-local ASR imports are append-only'); END;
CREATE TRIGGER contextual_media_pairs_no_update
BEFORE UPDATE ON contextual_media_local_asr_pairs
BEGIN SELECT RAISE(ABORT, 'contextual media-local ASR pairs are append-only'); END;
CREATE TRIGGER contextual_media_pairs_no_delete
BEFORE DELETE ON contextual_media_local_asr_pairs
BEGIN SELECT RAISE(ABORT, 'contextual media-local ASR pairs are append-only'); END;
CREATE TRIGGER contextual_asr_diffs_no_update
BEFORE UPDATE ON contextual_asr_text_private_diffs
BEGIN SELECT RAISE(ABORT, 'contextual ASR diff evidence is append-only'); END;
CREATE TRIGGER contextual_asr_diffs_no_delete
BEFORE DELETE ON contextual_asr_text_private_diffs
BEGIN SELECT RAISE(ABORT, 'contextual ASR diff evidence is append-only'); END;

CREATE TRIGGER contextual_private_publication_forbidden
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type IN (
    'private_glossary_registration',
    'contextual_asr_batch_registration',
    'contextual_media_local_asr_import',
    'contextual_media_local_asr_pair',
    'contextual_asr_text_private_diff'
)
BEGIN SELECT RAISE(ABORT, 'contextual ASR evidence has no publication lane'); END;

CREATE TRIGGER contextual_private_publication_gate_forbidden
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type IN (
    'private_glossary_registration',
    'contextual_asr_batch_registration',
    'contextual_media_local_asr_import',
    'contextual_media_local_asr_pair',
    'contextual_asr_text_private_diff'
)
BEGIN SELECT RAISE(ABORT, 'contextual ASR evidence has no publication lane'); END;
