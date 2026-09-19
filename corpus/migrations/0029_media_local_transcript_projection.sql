-- Reviewed append-only projection of exact full-file media-local ASR into
-- ordinary recording coordinates. The review is limited to pinned media lineage
-- and the identity time transform. It does not review wording, identify a
-- speaker, prefer a machine hypothesis, clear a publication gate, or publish.

CREATE TABLE media_local_transcript_projection_batches (
    projection_batch_id TEXT PRIMARY KEY,
    import_batch_id TEXT NOT NULL UNIQUE REFERENCES import_batches(import_batch_id),
    manifest_id TEXT NOT NULL UNIQUE,
    manifest_uri TEXT NOT NULL,
    manifest_raw_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(manifest_raw_sha256) = 64)
        CHECK(manifest_raw_sha256 NOT GLOB '*[^0-9a-f]*'),
    manifest_canonical_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(manifest_canonical_sha256) = 64)
        CHECK(manifest_canonical_sha256 NOT GLOB '*[^0-9a-f]*'),
    manifest_core_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(manifest_core_sha256) = 64)
        CHECK(manifest_core_sha256 NOT GLOB '*[^0-9a-f]*'),
    manifest_byte_count INTEGER NOT NULL CHECK(manifest_byte_count > 0),
    manifest_raw_json TEXT NOT NULL CHECK(json_valid(manifest_raw_json)),
    manifest_json TEXT NOT NULL CHECK(json_valid(manifest_json)),
    plan_sha256 TEXT NOT NULL UNIQUE
        CHECK(length(plan_sha256) = 64)
        CHECK(plan_sha256 NOT GLOB '*[^0-9a-f]*'),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    policy_id TEXT NOT NULL
        CHECK(policy_id = 'media_local_full_file_identity_v1'),
    plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT,
    reviewed_at TEXT NOT NULL
        CHECK(julianday(reviewed_at) IS NOT NULL)
        CHECK(reviewed_at GLOB '????-??-??T??:??:??Z')
        CHECK(strftime('%Y-%m-%dT%H:%M:%SZ', reviewed_at) = reviewed_at),
    review_basis TEXT NOT NULL CHECK(length(trim(review_basis)) BETWEEN 1 AND 4096),
    coordinate_attestation TEXT NOT NULL CHECK(
        coordinate_attestation =
        'reviewed_exact_full_file_identity_no_wording_or_speaker_claim_v1'
    ),
    projection_count INTEGER NOT NULL CHECK(projection_count BETWEEN 6 AND 10),
    recording_count INTEGER NOT NULL CHECK(recording_count BETWEEN 3 AND 5),
    pair_count INTEGER NOT NULL CHECK(pair_count BETWEEN 3 AND 5),
    segment_count INTEGER NOT NULL CHECK(segment_count > 0),
    word_count INTEGER NOT NULL CHECK(word_count >= 0),
    applied_at TEXT NOT NULL
        CHECK(julianday(applied_at) IS NOT NULL)
        CHECK(applied_at GLOB '????-??-??T??:??:??Z')
        CHECK(strftime('%Y-%m-%dT%H:%M:%SZ', applied_at) = applied_at),
    CHECK(projection_count = recording_count * 2),
    CHECK(pair_count = recording_count),
    CHECK(julianday(reviewed_at) <= julianday(applied_at))
);

CREATE TABLE media_local_transcript_projections (
    projection_id TEXT PRIMARY KEY,
    projection_batch_id TEXT NOT NULL
        REFERENCES media_local_transcript_projection_batches(projection_batch_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    projection_ordinal INTEGER NOT NULL CHECK(projection_ordinal BETWEEN 0 AND 9),
    variant TEXT NOT NULL CHECK(variant IN ('raw', 'contextual')),
    media_local_revision_id TEXT NOT NULL UNIQUE
        REFERENCES media_local_transcript_revisions(media_local_revision_id)
        ON DELETE RESTRICT,
    media_local_asr_import_id TEXT UNIQUE
        REFERENCES media_local_asr_imports(media_local_asr_import_id)
        ON DELETE RESTRICT,
    contextual_asr_import_id TEXT UNIQUE
        REFERENCES contextual_media_local_asr_imports(contextual_asr_import_id)
        ON DELETE RESTRICT,
    source_receipt_sha256 TEXT NOT NULL
        CHECK(length(source_receipt_sha256) = 64)
        CHECK(source_receipt_sha256 NOT GLOB '*[^0-9a-f]*'),
    source_transcript_sha256 TEXT NOT NULL
        CHECK(length(source_transcript_sha256) = 64)
        CHECK(source_transcript_sha256 NOT GLOB '*[^0-9a-f]*'),
    input_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    input_artifact_sha256 TEXT NOT NULL
        CHECK(length(input_artifact_sha256) = 64)
        CHECK(input_artifact_sha256 NOT GLOB '*[^0-9a-f]*'),
    input_artifact_processing_run_id TEXT NOT NULL
        REFERENCES processing_runs(processing_run_id),
    revision_id TEXT NOT NULL UNIQUE
        REFERENCES transcript_revisions(revision_id) ON DELETE RESTRICT,
    projection_processing_run_id TEXT NOT NULL UNIQUE
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    normalized_media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    normalized_media_sha256 TEXT NOT NULL
        CHECK(length(normalized_media_sha256) = 64)
        CHECK(normalized_media_sha256 NOT GLOB '*[^0-9a-f]*'),
    parent_media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    parent_media_sha256 TEXT NOT NULL
        CHECK(length(parent_media_sha256) = 64)
        CHECK(parent_media_sha256 NOT GLOB '*[^0-9a-f]*'),
    parent_rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id),
    parent_rendition_identity_sha256 TEXT NOT NULL
        CHECK(length(parent_rendition_identity_sha256) = 64)
        CHECK(parent_rendition_identity_sha256 NOT GLOB '*[^0-9a-f]*'),
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    qualifying_source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_identity_sha256 TEXT NOT NULL
        CHECK(length(source_identity_sha256) = 64)
        CHECK(source_identity_sha256 NOT GLOB '*[^0-9a-f]*'),
    qualifying_media_source_id TEXT NOT NULL REFERENCES media_sources(media_source_id),
    qualifying_recording_source_id TEXT NOT NULL
        REFERENCES recording_sources(recording_source_id),
    media_derivation_sha256 TEXT NOT NULL
        CHECK(length(media_derivation_sha256) = 64)
        CHECK(media_derivation_sha256 NOT GLOB '*[^0-9a-f]*'),
    media_source_sha256 TEXT NOT NULL
        CHECK(length(media_source_sha256) = 64)
        CHECK(media_source_sha256 NOT GLOB '*[^0-9a-f]*'),
    recording_source_mapping_sha256 TEXT NOT NULL
        CHECK(length(recording_source_mapping_sha256) = 64)
        CHECK(recording_source_mapping_sha256 NOT GLOB '*[^0-9a-f]*'),
    public_source_metadata_observation_id TEXT NOT NULL
        REFERENCES source_metadata_observations(source_metadata_observation_id),
    public_source_import_batch_id TEXT NOT NULL
        REFERENCES import_batches(import_batch_id),
    public_source_import_observation_id TEXT NOT NULL
        REFERENCES import_observations(import_observation_id),
    public_source_observed_at TEXT NOT NULL
        CHECK(julianday(public_source_observed_at) IS NOT NULL)
        CHECK(public_source_observed_at GLOB '????-??-??T??:??:??Z')
        CHECK(strftime('%Y-%m-%dT%H:%M:%SZ', public_source_observed_at)
              = public_source_observed_at),
    public_source_evidence_sha256 TEXT NOT NULL
        CHECK(length(public_source_evidence_sha256) = 64)
        CHECK(public_source_evidence_sha256 NOT GLOB '*[^0-9a-f]*'),
    projection_policy TEXT NOT NULL
        CHECK(projection_policy = 'media_local_full_file_identity_v1'),
    transform_expression TEXT NOT NULL CHECK(transform_expression = 'recording_ms=media_ms'),
    boundary TEXT NOT NULL CHECK(boundary = 'half_open'),
    mapping_basis TEXT NOT NULL CHECK(
        mapping_basis =
        'human_reviewed_timestamp_preserving_normalization_and_direct_public_file_lineage'
    ),
    source_mapping_confidence_state TEXT NOT NULL
        CHECK(source_mapping_confidence_state IN ('metadata_only', 'candidate', 'reviewed')),
    input_duration_ms INTEGER NOT NULL CHECK(input_duration_ms > 0),
    parent_duration_ms INTEGER NOT NULL CHECK(parent_duration_ms > 0),
    recording_duration_ms INTEGER NOT NULL CHECK(recording_duration_ms > 0),
    normalized_parent_delta_ms INTEGER NOT NULL CHECK(normalized_parent_delta_ms = 0),
    normalized_recording_delta_ms INTEGER NOT NULL
        CHECK(normalized_recording_delta_ms BETWEEN 0 AND 5),
    maximum_recording_duration_delta_ms INTEGER NOT NULL
        CHECK(maximum_recording_duration_delta_ms = 5),
    max_segment_end_ms INTEGER NOT NULL CHECK(max_segment_end_ms >= 0),
    segment_count INTEGER NOT NULL CHECK(segment_count > 0),
    word_count INTEGER NOT NULL CHECK(word_count >= 0),
    target_child_identity_sha256 TEXT NOT NULL
        CHECK(length(target_child_identity_sha256) = 64)
        CHECK(target_child_identity_sha256 NOT GLOB '*[^0-9a-f]*'),
    coordinate_review_state TEXT NOT NULL CHECK(
        coordinate_review_state = 'human_reviewed_exact_identity_projection'
    ),
    wording_reviewed INTEGER NOT NULL CHECK(wording_reviewed = 0),
    speaker_identity_asserted INTEGER NOT NULL CHECK(speaker_identity_asserted = 0),
    accuracy_claimed INTEGER NOT NULL CHECK(accuracy_claimed = 0),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    projected_at TEXT NOT NULL
        CHECK(julianday(projected_at) IS NOT NULL)
        CHECK(projected_at GLOB '????-??-??T??:??:??Z')
        CHECK(strftime('%Y-%m-%dT%H:%M:%SZ', projected_at) = projected_at),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(projection_batch_id, projection_ordinal),
    CHECK(normalized_media_id <> parent_media_id),
    CHECK(normalized_parent_delta_ms = abs(input_duration_ms - parent_duration_ms)),
    CHECK(normalized_recording_delta_ms = abs(input_duration_ms - recording_duration_ms)),
    CHECK(max_segment_end_ms <= input_duration_ms),
    CHECK(max_segment_end_ms <= parent_duration_ms),
    CHECK(max_segment_end_ms <= recording_duration_ms),
    CHECK(
        (variant = 'raw' AND media_local_asr_import_id IS NOT NULL
         AND contextual_asr_import_id IS NULL)
        OR
        (variant = 'contextual' AND media_local_asr_import_id IS NULL
         AND contextual_asr_import_id IS NOT NULL)
    )
);

CREATE INDEX media_local_transcript_projections_recording_idx
    ON media_local_transcript_projections(recording_id, projection_id);

CREATE TABLE media_local_transcript_projection_pairs (
    projection_pair_id TEXT PRIMARY KEY,
    projection_batch_id TEXT NOT NULL
        REFERENCES media_local_transcript_projection_batches(projection_batch_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    recording_ordinal INTEGER NOT NULL CHECK(recording_ordinal BETWEEN 0 AND 4),
    contextual_pair_id TEXT NOT NULL UNIQUE
        REFERENCES contextual_media_local_asr_pairs(contextual_pair_id)
        ON DELETE RESTRICT,
    contextual_diff_id TEXT NOT NULL UNIQUE
        REFERENCES contextual_asr_text_private_diffs(contextual_diff_id)
        ON DELETE RESTRICT,
    baseline_projection_id TEXT NOT NULL UNIQUE
        REFERENCES media_local_transcript_projections(projection_id)
        ON DELETE RESTRICT,
    contextual_projection_id TEXT NOT NULL UNIQUE
        REFERENCES media_local_transcript_projections(projection_id)
        ON DELETE RESTRICT,
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
    wording_reviewed INTEGER NOT NULL CHECK(wording_reviewed = 0),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    created_at TEXT NOT NULL
        CHECK(julianday(created_at) IS NOT NULL)
        CHECK(created_at GLOB '????-??-??T??:??:??Z')
        CHECK(strftime('%Y-%m-%dT%H:%M:%SZ', created_at) = created_at),
    UNIQUE(projection_batch_id, recording_ordinal),
    CHECK(baseline_projection_id <> contextual_projection_id)
);

-- Replacement guards make append-only behavior independent of INSERT OR REPLACE.
CREATE TRIGGER media_local_projection_batches_no_replace
BEFORE INSERT ON media_local_transcript_projection_batches
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projection_batches AS existing
    WHERE existing.projection_batch_id = NEW.projection_batch_id
       OR existing.import_batch_id = NEW.import_batch_id
       OR existing.manifest_id = NEW.manifest_id
       OR existing.manifest_raw_sha256 = NEW.manifest_raw_sha256
       OR existing.manifest_canonical_sha256 = NEW.manifest_canonical_sha256
       OR existing.manifest_core_sha256 = NEW.manifest_core_sha256
       OR existing.plan_sha256 = NEW.plan_sha256
)
BEGIN SELECT RAISE(ABORT, 'media-local projection batch replacement is forbidden'); END;

CREATE TRIGGER media_local_transcript_projections_no_replace
BEFORE INSERT ON media_local_transcript_projections
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections AS existing
    WHERE existing.projection_id = NEW.projection_id
       OR existing.media_local_revision_id = NEW.media_local_revision_id
       OR existing.revision_id = NEW.revision_id
       OR existing.projection_processing_run_id = NEW.projection_processing_run_id
       OR (existing.projection_batch_id = NEW.projection_batch_id
           AND existing.projection_ordinal = NEW.projection_ordinal)
       OR (NEW.media_local_asr_import_id IS NOT NULL
           AND existing.media_local_asr_import_id = NEW.media_local_asr_import_id)
       OR (NEW.contextual_asr_import_id IS NOT NULL
           AND existing.contextual_asr_import_id = NEW.contextual_asr_import_id)
)
BEGIN SELECT RAISE(ABORT, 'media-local transcript projection replacement is forbidden'); END;

CREATE TRIGGER media_local_transcript_projection_pairs_no_replace
BEFORE INSERT ON media_local_transcript_projection_pairs
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projection_pairs AS existing
    WHERE existing.projection_pair_id = NEW.projection_pair_id
       OR existing.contextual_pair_id = NEW.contextual_pair_id
       OR existing.contextual_diff_id = NEW.contextual_diff_id
       OR existing.baseline_projection_id = NEW.baseline_projection_id
       OR existing.contextual_projection_id = NEW.contextual_projection_id
       OR (existing.projection_batch_id = NEW.projection_batch_id
           AND existing.recording_ordinal = NEW.recording_ordinal)
)
BEGIN SELECT RAISE(ABORT, 'media-local projection pair replacement is forbidden'); END;

CREATE TRIGGER media_local_projection_batches_no_update
BEFORE UPDATE ON media_local_transcript_projection_batches
BEGIN SELECT RAISE(ABORT, 'media-local transcript projection batches are append-only'); END;
CREATE TRIGGER media_local_projection_batches_no_delete
BEFORE DELETE ON media_local_transcript_projection_batches
BEGIN SELECT RAISE(ABORT, 'media-local transcript projection batches are append-only'); END;
CREATE TRIGGER media_local_transcript_projections_no_update
BEFORE UPDATE ON media_local_transcript_projections
BEGIN SELECT RAISE(ABORT, 'media-local transcript projections are append-only'); END;
CREATE TRIGGER media_local_transcript_projections_no_delete
BEFORE DELETE ON media_local_transcript_projections
BEGIN SELECT RAISE(ABORT, 'media-local transcript projections are append-only'); END;
CREATE TRIGGER media_local_transcript_projection_pairs_no_update
BEFORE UPDATE ON media_local_transcript_projection_pairs
BEGIN SELECT RAISE(ABORT, 'media-local transcript projection pairs are append-only'); END;
CREATE TRIGGER media_local_transcript_projection_pairs_no_delete
BEFORE DELETE ON media_local_transcript_projection_pairs
BEGIN SELECT RAISE(ABORT, 'media-local transcript projection pairs are append-only'); END;

-- The header is inserted last. These checks close the deferred child graph.
CREATE TRIGGER media_local_projection_batch_complete_insert
BEFORE INSERT ON media_local_transcript_projection_batches
WHEN NOT EXISTS (
    SELECT 1
    FROM reviewers AS reviewer
    JOIN reviewer_admin_events AS state ON state.reviewer_id = reviewer.reviewer_id
    WHERE reviewer.reviewer_id = NEW.reviewer_id
      AND reviewer.reviewer_kind = 'human'
      AND state.event_sequence = (
          SELECT max(candidate.event_sequence)
          FROM reviewer_admin_events AS candidate
          WHERE candidate.reviewer_id = reviewer.reviewer_id
            AND julianday(candidate.effective_at) <= julianday(NEW.reviewed_at)
      )
      AND state.new_active = 1
)
OR julianday(NEW.applied_at) > julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
OR (SELECT count(*) FROM media_local_transcript_projections
    WHERE projection_batch_id = NEW.projection_batch_id) <> NEW.projection_count
OR (SELECT count(DISTINCT recording_id) FROM media_local_transcript_projections
    WHERE projection_batch_id = NEW.projection_batch_id) <> NEW.recording_count
OR (SELECT coalesce(sum(segment_count), 0) FROM media_local_transcript_projections
    WHERE projection_batch_id = NEW.projection_batch_id) <> NEW.segment_count
OR (SELECT coalesce(sum(word_count), 0) FROM media_local_transcript_projections
    WHERE projection_batch_id = NEW.projection_batch_id) <> NEW.word_count
OR (SELECT count(*) FROM media_local_transcript_projection_pairs
    WHERE projection_batch_id = NEW.projection_batch_id) <> NEW.pair_count
OR (SELECT min(projection_ordinal) FROM media_local_transcript_projections
    WHERE projection_batch_id = NEW.projection_batch_id) <> 0
OR (SELECT max(projection_ordinal) FROM media_local_transcript_projections
    WHERE projection_batch_id = NEW.projection_batch_id) <> NEW.projection_count - 1
OR (SELECT min(recording_ordinal) FROM media_local_transcript_projection_pairs
    WHERE projection_batch_id = NEW.projection_batch_id) <> 0
OR (SELECT max(recording_ordinal) FROM media_local_transcript_projection_pairs
    WHERE projection_batch_id = NEW.projection_batch_id) <> NEW.recording_count - 1
OR EXISTS (
    SELECT 1 FROM media_local_transcript_projections AS projection
    WHERE projection.projection_batch_id = NEW.projection_batch_id
    GROUP BY projection.recording_id
    HAVING count(*) <> 2 OR sum(projection.variant = 'raw') <> 1
       OR sum(projection.variant = 'contextual') <> 1
)
OR EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN import_batches AS public_batch
      ON public_batch.import_batch_id = projection.public_source_import_batch_id
    JOIN import_observations AS public_receipt
      ON public_receipt.import_observation_id =
           projection.public_source_import_observation_id
    LEFT JOIN media_local_asr_imports AS raw_receipt
      ON raw_receipt.media_local_asr_import_id = projection.media_local_asr_import_id
    LEFT JOIN contextual_media_local_asr_imports AS contextual_receipt
      ON contextual_receipt.contextual_asr_import_id = projection.contextual_asr_import_id
    JOIN import_batches AS source_asr_batch
      ON source_asr_batch.import_batch_id = coalesce(
           raw_receipt.import_batch_id, contextual_receipt.import_batch_id
         )
    JOIN media_local_transcript_revisions AS source_revision
      ON source_revision.media_local_revision_id = projection.media_local_revision_id
    JOIN processing_runs AS source_run
      ON source_run.processing_run_id = source_revision.processing_run_id
    JOIN processing_runs AS preprocess_run
      ON preprocess_run.processing_run_id =
           projection.input_artifact_processing_run_id
    LEFT JOIN import_batches AS preprocess_batch
      ON preprocess_batch.import_batch_id = raw_receipt.preprocess_import_batch_id
    WHERE projection.projection_batch_id = NEW.projection_batch_id
      AND (
          julianday(projection.public_source_observed_at) > julianday(NEW.reviewed_at)
          OR julianday(public_batch.completed_at) IS NULL
          OR julianday(public_batch.completed_at) > julianday(NEW.reviewed_at)
          OR julianday(public_receipt.completed_at) IS NULL
          OR julianday(public_receipt.completed_at) > julianday(NEW.reviewed_at)
          OR julianday(coalesce(raw_receipt.imported_at,
                                contextual_receipt.imported_at)) IS NULL
          OR julianday(coalesce(raw_receipt.imported_at,
                                contextual_receipt.imported_at))
               > julianday(NEW.reviewed_at)
          OR julianday(source_asr_batch.completed_at) IS NULL
          OR julianday(source_asr_batch.completed_at) > julianday(NEW.reviewed_at)
          OR julianday(source_run.completed_at) IS NULL
          OR julianday(source_run.completed_at) > julianday(NEW.reviewed_at)
          OR julianday(preprocess_run.completed_at) IS NULL
          OR julianday(preprocess_run.completed_at) > julianday(NEW.reviewed_at)
          OR (projection.variant = 'raw'
              AND (julianday(preprocess_batch.completed_at) IS NULL
                   OR julianday(preprocess_batch.completed_at)
                        > julianday(NEW.reviewed_at)))
      )
)
OR EXISTS (
    SELECT 1
    FROM media_local_transcript_projection_pairs AS pair
    JOIN contextual_media_local_asr_pairs AS source_pair
      ON source_pair.contextual_pair_id = pair.contextual_pair_id
    JOIN contextual_asr_text_private_diffs AS source_diff
      ON source_diff.contextual_diff_id = pair.contextual_diff_id
    WHERE pair.projection_batch_id = NEW.projection_batch_id
      AND (
          julianday(source_pair.created_at) IS NULL
          OR julianday(source_pair.created_at) > julianday(NEW.reviewed_at)
          OR julianday(source_diff.created_at) IS NULL
          OR julianday(source_diff.created_at) > julianday(NEW.reviewed_at)
      )
)
OR EXISTS (
    SELECT 1
    FROM media_local_transcript_projection_pairs AS pair
    JOIN media_local_transcript_projections AS baseline
      ON baseline.projection_id = pair.baseline_projection_id
    JOIN media_local_transcript_projections AS contextual
      ON contextual.projection_id = pair.contextual_projection_id
    WHERE pair.projection_batch_id = NEW.projection_batch_id
      AND (baseline.projection_batch_id <> NEW.projection_batch_id
           OR contextual.projection_batch_id <> NEW.projection_batch_id
           OR baseline.recording_id <> contextual.recording_id
           OR baseline.variant <> 'raw' OR contextual.variant <> 'contextual')
)
BEGIN SELECT RAISE(ABORT, 'media-local projection batch is incomplete or lacks active human coordinate review'); END;

-- The exact normalized ASR media has no rendition. parent_rendition_id is
-- evidence only, while the generated transcript revision has rendition_id NULL.
CREATE TRIGGER media_local_transcript_projection_exact_insert
BEFORE INSERT ON media_local_transcript_projections
WHEN NOT EXISTS (
    SELECT 1
    FROM media_local_transcript_revisions AS local_revision
    JOIN media_objects AS normalized ON normalized.media_id = local_revision.media_id
     AND normalized.sha256 = NEW.normalized_media_sha256
     AND normalized.media_kind = 'audio'
     AND normalized.mime_type = 'audio/flac'
     AND normalized.container = 'flac'
    JOIN artifacts AS input_artifact
      ON input_artifact.artifact_id = NEW.input_artifact_id
     AND input_artifact.artifact_id = local_revision.input_artifact_id
     AND input_artifact.sha256 = NEW.input_artifact_sha256
     AND input_artifact.sha256 = normalized.sha256
     AND input_artifact.byte_count = normalized.byte_count
     AND input_artifact.artifact_kind = 'audio_16khz_mono_flac'
     AND input_artifact.visibility = 'private'
     AND input_artifact.processing_run_id = NEW.input_artifact_processing_run_id
    JOIN processing_runs AS preprocess_run
      ON preprocess_run.processing_run_id = input_artifact.processing_run_id
     AND preprocess_run.stage = 'media_preprocess'
     AND preprocess_run.status = 'completed'
     AND preprocess_run.completed_at IS NOT NULL
     AND preprocess_run.error_text IS NULL
    JOIN media_derivations AS derivation
      ON derivation.child_media_id = normalized.media_id
     AND derivation.parent_media_id = NEW.parent_media_id
     AND derivation.derivation_kind = 'audio_normalization_16khz_mono_flac'
     AND derivation.processing_run_id = preprocess_run.processing_run_id
     AND json_extract(derivation.metadata_json, '$.channels') = 1
     AND json_extract(derivation.metadata_json, '$.sample_rate_hz') = 16000
    JOIN media_objects AS parent ON parent.media_id = derivation.parent_media_id
     AND parent.sha256 = NEW.parent_media_sha256
     AND parent.media_kind = 'video'
    JOIN renditions AS parent_rendition
      ON parent_rendition.rendition_id = NEW.parent_rendition_id
     AND parent_rendition.media_id = parent.media_id
     AND parent_rendition.recording_id = NEW.recording_id
    JOIN recordings AS recording ON recording.recording_id = NEW.recording_id
    JOIN transcript_revisions AS target
      ON target.revision_id = NEW.revision_id
     AND target.recording_id = NEW.recording_id AND target.rendition_id IS NULL
     AND target.processing_run_id = NEW.projection_processing_run_id
     AND target.revision_kind = local_revision.revision_kind
     AND target.origin = 'media_local_full_file_identity_projection_v1'
     AND target.language = local_revision.language
     AND target.glossary_revision_id IS local_revision.glossary_revision_id
     AND target.review_state = 'machine' AND target.created_at = NEW.projected_at
     AND target.metadata_json = json_object(
           'accuracy_claimed', json('false'),
           'coordinate_projection', json_object(
             'boundary', 'half_open',
             'parent_rendition_id', NEW.parent_rendition_id,
             'projection_id', NEW.projection_id,
             'source_media_local_revision_id', NEW.media_local_revision_id,
             'source_transcript_sha256', NEW.source_transcript_sha256,
             'target_rendition_id', NULL,
             'transform_expression', 'recording_ms=media_ms'
           ),
           'preferred_revision_selected', json('false'),
           'speaker_identity_asserted', json('false'),
           'wording_reviewed', json('false')
         )
    JOIN processing_runs AS projection_run
      ON projection_run.processing_run_id = NEW.projection_processing_run_id
     AND projection_run.stage = 'media_local_transcript_identity_projection'
     AND projection_run.implementation_version = 'media-local-transcript-projection/2'
     AND projection_run.model_id IS NULL AND projection_run.glossary_revision_id IS NULL
     AND projection_run.parameters_json = json_object(
           'boundary', 'half_open',
           'maximum_normalized_recording_delta_ms', 5,
           'normalized_parent_delta_ms', 0,
           'projection_id', NEW.projection_id,
           'source_processing_run_id', local_revision.processing_run_id,
           'transform_expression', 'recording_ms=media_ms'
         )
     AND projection_run.environment_json = json_object(
           'catalog_only', json('true'),
           'network_access_performed', json('false'),
           'timestamp_repair_performed', json('false'),
           'wording_inference_performed', json('false')
         )
     AND projection_run.random_seed IS NULL
     AND projection_run.status = 'completed'
     AND projection_run.error_text IS NULL
     AND projection_run.started_at = NEW.projected_at
     AND projection_run.completed_at = NEW.projected_at
    JOIN processing_runs AS source_run
      ON source_run.processing_run_id = local_revision.processing_run_id
     AND source_run.stage = 'asr_whispercpp'
     AND source_run.status = 'completed'
     AND source_run.completed_at IS NOT NULL
     AND source_run.error_text IS NULL
    JOIN run_inputs AS projection_input
      ON projection_input.processing_run_id = projection_run.processing_run_id
     AND projection_input.object_type = 'media_local_transcript_revision'
     AND projection_input.object_id = local_revision.media_local_revision_id
     AND projection_input.input_role = 'identity_projection_source'
     AND projection_input.input_sha256 = NEW.source_transcript_sha256
    JOIN media_sources AS acquired
      ON acquired.media_source_id = NEW.qualifying_media_source_id
     AND acquired.media_id = parent.media_id
     AND acquired.source_id = NEW.qualifying_source_id
     AND acquired.source_snapshot_id IS NULL
    JOIN sources AS source
      ON source.source_id = acquired.source_id AND source.access_state = 'public'
    JOIN source_metadata_observations AS public_observation
      ON public_observation.source_metadata_observation_id =
           NEW.public_source_metadata_observation_id
     AND public_observation.source_id = source.source_id
     AND public_observation.import_batch_id = NEW.public_source_import_batch_id
     AND public_observation.import_observation_id =
           NEW.public_source_import_observation_id
     AND public_observation.observed_at = NEW.public_source_observed_at
     AND public_observation.quality_rank = 700
     AND public_observation.quality_basis =
           'acquisition_result_v1: locally verified acquisition result'
     AND public_observation.access_state = 'public'
     AND public_observation.review_state = 'metadata_only'
    JOIN import_batches AS public_import_batch
      ON public_import_batch.import_batch_id = NEW.public_source_import_batch_id
     AND public_import_batch.importer_name = 'acquisition_result_v1'
     AND public_import_batch.status = 'completed'
     AND public_import_batch.completed_at IS NOT NULL
    JOIN import_observations AS public_import_observation
      ON public_import_observation.import_observation_id =
           NEW.public_source_import_observation_id
     AND public_import_observation.import_batch_id = public_import_batch.import_batch_id
     AND public_import_observation.importer_version =
           public_import_batch.importer_version
     AND public_import_observation.observed_at = public_observation.observed_at
     AND public_import_observation.status = 'completed'
     AND public_import_observation.completed_at IS NOT NULL
    JOIN recording_sources AS recording_source
      ON recording_source.recording_source_id = NEW.qualifying_recording_source_id
     AND recording_source.recording_id = recording.recording_id
     AND recording_source.source_id = source.source_id
     AND recording_source.confidence_state = NEW.source_mapping_confidence_state
    WHERE local_revision.media_local_revision_id = NEW.media_local_revision_id
      AND local_revision.review_state = 'machine'
      AND local_revision.coordinate_system = 'media_ms'
      AND local_revision.boundary = 'half_open'
      AND local_revision.requested_start_ms = 0
      AND local_revision.requested_end_ms = local_revision.input_duration_ms
      AND local_revision.input_boundary_overrun_ms = 0
      AND local_revision.media_id = NEW.normalized_media_id
      AND normalized.duration_ms = local_revision.input_duration_ms
      AND local_revision.input_duration_ms = NEW.input_duration_ms
      AND parent.duration_ms = NEW.parent_duration_ms
      AND recording.duration_ms = NEW.recording_duration_ms
      AND local_revision.max_segment_end_ms = NEW.max_segment_end_ms
      AND NEW.metadata_json = json_object(
            'input_artifact_id', NEW.input_artifact_id,
            'input_artifact_processing_run_id',
              NEW.input_artifact_processing_run_id,
            'input_artifact_sha256', NEW.input_artifact_sha256,
            'media_derivation_sha256', NEW.media_derivation_sha256,
            'media_source_sha256', NEW.media_source_sha256,
            'parent_rendition_identity_sha256',
              NEW.parent_rendition_identity_sha256,
            'public_source_evidence_sha256', NEW.public_source_evidence_sha256,
            'qualifying_media_source_id', NEW.qualifying_media_source_id,
            'qualifying_recording_source_id', NEW.qualifying_recording_source_id,
            'recording_source_mapping_sha256',
              NEW.recording_source_mapping_sha256,
            'source_identity_sha256', NEW.source_identity_sha256,
            'source_receipt_sha256', NEW.source_receipt_sha256,
            'source_transcript_sha256', NEW.source_transcript_sha256,
            'target_child_identity_sha256', NEW.target_child_identity_sha256,
            'target_rendition_id', NULL
          )
      AND (SELECT count(*) FROM renditions AS normalized_rendition
           WHERE normalized_rendition.media_id = normalized.media_id) = 0
      AND (SELECT count(*) FROM media_derivations AS all_derivations
           WHERE all_derivations.child_media_id = normalized.media_id) = 1
      AND (SELECT count(*) FROM renditions AS all_parent_renditions
           WHERE all_parent_renditions.media_id = parent.media_id) = 1
      AND (SELECT count(*) FROM run_inputs AS source_input
           WHERE source_input.processing_run_id = local_revision.processing_run_id) = 1
      AND EXISTS (
          SELECT 1 FROM run_inputs AS source_input
          WHERE source_input.processing_run_id = local_revision.processing_run_id
            AND source_input.object_type = 'media'
            AND source_input.object_id = local_revision.media_id
            AND source_input.input_role = 'normalized_audio'
            AND source_input.input_sha256 = normalized.sha256
      )
      AND (SELECT count(*) FROM run_inputs AS preprocess_input
           WHERE preprocess_input.processing_run_id = preprocess_run.processing_run_id) = 1
      AND EXISTS (
          SELECT 1 FROM run_inputs AS preprocess_input
          WHERE preprocess_input.processing_run_id = preprocess_run.processing_run_id
            AND preprocess_input.object_type = 'media'
            AND preprocess_input.object_id = parent.media_id
            AND preprocess_input.input_role = 'source_media'
            AND preprocess_input.input_sha256 = parent.sha256
      )
      AND (SELECT count(*) FROM run_inputs AS all_inputs
           WHERE all_inputs.processing_run_id = projection_run.processing_run_id) = 1
      AND (SELECT count(*) FROM artifacts AS artifact
           WHERE artifact.processing_run_id = projection_run.processing_run_id) = 0
      AND (
          (NEW.variant = 'raw' AND local_revision.revision_kind = 'raw_asr'
           AND local_revision.glossary_revision_id IS NULL AND EXISTS (
               SELECT 1 FROM media_local_asr_imports AS receipt
               JOIN import_batches AS source_batch
                 ON source_batch.import_batch_id = receipt.import_batch_id
                AND source_batch.importer_name = 'media_local_asr_result_v1'
                AND source_batch.input_sha256 = receipt.asr_result_canonical_sha256
                AND source_batch.status = 'completed'
                AND source_batch.completed_at IS NOT NULL
               JOIN import_batches AS preprocess_batch
                 ON preprocess_batch.import_batch_id = receipt.preprocess_import_batch_id
                AND preprocess_batch.importer_name = 'media_preprocess_result_v1'
                AND preprocess_batch.input_sha256 =
                      receipt.preprocess_result_canonical_sha256
                AND preprocess_batch.status = 'completed'
                AND preprocess_batch.completed_at IS NOT NULL
               WHERE receipt.media_local_asr_import_id = NEW.media_local_asr_import_id
                 AND receipt.media_local_revision_id = local_revision.media_local_revision_id
                 AND receipt.input_media_id = local_revision.media_id
                 AND receipt.input_artifact_id = local_revision.input_artifact_id
                 AND receipt.input_duration_ms = local_revision.input_duration_ms
                 AND receipt.max_segment_end_ms = local_revision.max_segment_end_ms
                 AND receipt.input_boundary_overrun_ms = 0))
          OR
          (NEW.variant = 'contextual'
           AND local_revision.revision_kind = 'contextual_asr'
           AND local_revision.glossary_revision_id IS NOT NULL AND EXISTS (
               SELECT 1 FROM contextual_media_local_asr_imports AS receipt
               JOIN import_batches AS source_batch
                 ON source_batch.import_batch_id = receipt.import_batch_id
                AND source_batch.importer_name =
                      'contextual_media_local_asr_result_v1'
                AND source_batch.input_sha256 = receipt.result_canonical_sha256
                AND source_batch.status = 'completed'
                AND source_batch.completed_at IS NOT NULL
               WHERE receipt.contextual_asr_import_id = NEW.contextual_asr_import_id
                 AND receipt.media_local_revision_id = local_revision.media_local_revision_id
                 AND receipt.input_media_id = local_revision.media_id
                 AND receipt.input_artifact_id = local_revision.input_artifact_id
                 AND receipt.input_duration_ms = local_revision.input_duration_ms
                 AND receipt.max_segment_end_ms = local_revision.max_segment_end_ms
                 AND receipt.input_boundary_overrun_ms = 0))
      )
)
OR (SELECT count(*) FROM transcript_segments WHERE revision_id = NEW.revision_id) <> NEW.segment_count
OR (SELECT count(*) FROM transcript_words AS word
    JOIN transcript_segments AS segment ON segment.segment_id = word.segment_id
    WHERE segment.revision_id = NEW.revision_id) <> NEW.word_count
OR (SELECT count(*) FROM media_local_transcript_segments
    WHERE media_local_revision_id = NEW.media_local_revision_id) <> NEW.segment_count
OR (SELECT count(*) FROM media_local_transcript_words AS word
    JOIN media_local_transcript_segments AS segment
      ON segment.media_local_segment_id = word.media_local_segment_id
    WHERE segment.media_local_revision_id = NEW.media_local_revision_id) <> NEW.word_count
OR EXISTS (
    SELECT 1
    FROM transcript_segments AS target_segment
    LEFT JOIN media_local_transcript_segments AS source_segment
      ON source_segment.media_local_revision_id = NEW.media_local_revision_id
     AND source_segment.ordinal = target_segment.ordinal
    WHERE target_segment.revision_id = NEW.revision_id
      AND (
          source_segment.media_local_segment_id IS NULL
          OR target_segment.start_ms IS NOT source_segment.media_start_ms
          OR target_segment.end_ms IS NOT source_segment.media_end_ms
          OR target_segment.text IS NOT source_segment.text
          OR target_segment.normalized_text IS NOT source_segment.normalized_text
          OR target_segment.speaker_label IS NOT source_segment.speaker_label
          OR target_segment.language IS NOT source_segment.language
          OR target_segment.confidence_band IS NOT source_segment.confidence_band
          OR target_segment.calibrated_probability
               IS NOT source_segment.calibrated_probability
          OR json_type(source_segment.metadata_json, '$.coordinate_projection')
               IS NOT NULL
          OR json_type(target_segment.metadata_json, '$.coordinate_projection')
               IS NOT 'object'
          OR (SELECT count(*) FROM json_each(target_segment.metadata_json)
              WHERE key = 'coordinate_projection') <> 1
          OR (SELECT count(*)
              FROM json_each(target_segment.metadata_json,
                             '$.coordinate_projection')) <> 3
          OR json_extract(target_segment.metadata_json,
                          '$.coordinate_projection.projection_id')
               IS NOT NEW.projection_id
          OR json_extract(target_segment.metadata_json,
                          '$.coordinate_projection.source_media_local_segment_id')
               IS NOT source_segment.media_local_segment_id
          OR json_extract(target_segment.metadata_json,
                          '$.coordinate_projection.transform_expression')
               IS NOT 'recording_ms=media_ms'
          OR json_remove(target_segment.metadata_json, '$.coordinate_projection')
               IS NOT source_segment.metadata_json
      )
)
OR EXISTS (
    SELECT 1
    FROM media_local_transcript_segments AS source_segment
    LEFT JOIN transcript_segments AS target_segment
      ON target_segment.revision_id = NEW.revision_id
     AND target_segment.ordinal = source_segment.ordinal
    WHERE source_segment.media_local_revision_id = NEW.media_local_revision_id
      AND target_segment.segment_id IS NULL
)
OR EXISTS (
    SELECT 1
    FROM transcript_words AS target_word
    JOIN transcript_segments AS target_segment
      ON target_segment.segment_id = target_word.segment_id
    LEFT JOIN media_local_transcript_segments AS source_segment
      ON source_segment.media_local_revision_id = NEW.media_local_revision_id
     AND source_segment.ordinal = target_segment.ordinal
    LEFT JOIN media_local_transcript_words AS source_word
      ON source_word.media_local_segment_id = source_segment.media_local_segment_id
     AND source_word.ordinal = target_word.ordinal
    WHERE target_segment.revision_id = NEW.revision_id
      AND (
          source_word.media_local_word_id IS NULL
          OR target_word.start_ms IS NOT source_word.media_start_ms
          OR target_word.end_ms IS NOT source_word.media_end_ms
          OR target_word.token IS NOT source_word.token
          OR target_word.normalized_token IS NOT source_word.normalized_token
          OR target_word.asr_log_probability IS NOT source_word.asr_log_probability
          OR target_word.alignment_score IS NOT source_word.alignment_score
          OR target_word.calibrated_probability
               IS NOT source_word.calibrated_probability
      )
)
OR EXISTS (
    SELECT 1
    FROM media_local_transcript_words AS source_word
    JOIN media_local_transcript_segments AS source_segment
      ON source_segment.media_local_segment_id = source_word.media_local_segment_id
    LEFT JOIN transcript_segments AS target_segment
      ON target_segment.revision_id = NEW.revision_id
     AND target_segment.ordinal = source_segment.ordinal
    LEFT JOIN transcript_words AS target_word
      ON target_word.segment_id = target_segment.segment_id
     AND target_word.ordinal = source_word.ordinal
    WHERE source_segment.media_local_revision_id = NEW.media_local_revision_id
      AND target_word.word_id IS NULL
)
OR (SELECT coalesce(max(media_end_ms), 0) FROM media_local_transcript_segments
    WHERE media_local_revision_id = NEW.media_local_revision_id) <> NEW.max_segment_end_ms
OR EXISTS (
    SELECT 1 FROM media_local_transcript_segments AS segment
    WHERE segment.media_local_revision_id = NEW.media_local_revision_id
      AND (segment.media_end_ms > min(NEW.input_duration_ms,
                                      NEW.parent_duration_ms,
                                      NEW.recording_duration_ms)
           OR segment.speaker_label IS NOT NULL)
)
OR EXISTS (
    SELECT 1 FROM media_local_transcript_words AS word
    JOIN media_local_transcript_segments AS segment
      ON segment.media_local_segment_id = word.media_local_segment_id
    WHERE segment.media_local_revision_id = NEW.media_local_revision_id
      AND word.media_start_ms IS NOT NULL
      AND (word.media_start_ms < segment.media_start_ms
           OR word.media_end_ms > segment.media_end_ms
           OR word.media_end_ms > min(NEW.input_duration_ms,
                                      NEW.parent_duration_ms,
                                      NEW.recording_duration_ms))
)
OR EXISTS (SELECT 1 FROM publication_decisions
           WHERE object_type = 'transcript_revision' AND object_id = NEW.revision_id)
OR EXISTS (SELECT 1 FROM publication_gate_decisions
           WHERE object_type = 'transcript_revision' AND object_id = NEW.revision_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection differs from exact admitted source or reviewed identity bounds'); END;

CREATE TRIGGER media_local_transcript_projection_pair_exact_insert
BEFORE INSERT ON media_local_transcript_projection_pairs
WHEN NOT EXISTS (
    SELECT 1
    FROM contextual_media_local_asr_pairs AS source_pair
    JOIN contextual_asr_text_private_diffs AS source_diff
      ON source_diff.contextual_diff_id = NEW.contextual_diff_id
     AND source_diff.contextual_pair_id = source_pair.contextual_pair_id
    JOIN media_local_transcript_projections AS baseline
      ON baseline.projection_id = NEW.baseline_projection_id
     AND baseline.media_local_revision_id = source_pair.baseline_media_local_revision_id
     AND baseline.variant = 'raw'
    JOIN media_local_transcript_projections AS contextual
      ON contextual.projection_id = NEW.contextual_projection_id
     AND contextual.media_local_revision_id = source_pair.contextual_media_local_revision_id
     AND contextual.variant = 'contextual'
    WHERE source_pair.contextual_pair_id = NEW.contextual_pair_id
      AND baseline.projection_batch_id = NEW.projection_batch_id
      AND contextual.projection_batch_id = NEW.projection_batch_id
      AND baseline.recording_id = contextual.recording_id
      AND baseline.projection_ordinal = NEW.recording_ordinal * 2
      AND contextual.projection_ordinal = NEW.recording_ordinal * 2 + 1
      AND baseline.normalized_media_id = contextual.normalized_media_id
      AND baseline.normalized_media_sha256 = contextual.normalized_media_sha256
      AND baseline.input_artifact_id = contextual.input_artifact_id
      AND baseline.input_artifact_sha256 = contextual.input_artifact_sha256
      AND baseline.input_artifact_processing_run_id =
            contextual.input_artifact_processing_run_id
      AND baseline.parent_media_id = contextual.parent_media_id
      AND baseline.parent_media_sha256 = contextual.parent_media_sha256
      AND baseline.parent_rendition_id = contextual.parent_rendition_id
      AND baseline.parent_rendition_identity_sha256 =
            contextual.parent_rendition_identity_sha256
      AND baseline.qualifying_source_id = contextual.qualifying_source_id
      AND baseline.source_identity_sha256 = contextual.source_identity_sha256
      AND baseline.qualifying_media_source_id =
            contextual.qualifying_media_source_id
      AND baseline.qualifying_recording_source_id =
            contextual.qualifying_recording_source_id
      AND baseline.media_derivation_sha256 = contextual.media_derivation_sha256
      AND baseline.media_source_sha256 = contextual.media_source_sha256
      AND baseline.recording_source_mapping_sha256 =
            contextual.recording_source_mapping_sha256
      AND baseline.public_source_metadata_observation_id =
            contextual.public_source_metadata_observation_id
      AND baseline.public_source_import_batch_id =
            contextual.public_source_import_batch_id
      AND baseline.public_source_import_observation_id =
            contextual.public_source_import_observation_id
      AND baseline.public_source_observed_at = contextual.public_source_observed_at
      AND baseline.public_source_evidence_sha256 =
            contextual.public_source_evidence_sha256
      AND baseline.source_mapping_confidence_state =
            contextual.source_mapping_confidence_state
      AND baseline.input_duration_ms = contextual.input_duration_ms
      AND baseline.parent_duration_ms = contextual.parent_duration_ms
      AND baseline.recording_duration_ms = contextual.recording_duration_ms
      AND source_pair.pair_state = NEW.pair_state
      AND source_pair.input_equal = NEW.input_equal
      AND source_pair.engine_equal = NEW.engine_equal
      AND source_pair.model_equal = NEW.model_equal
      AND source_pair.window_equal = NEW.window_equal
      AND source_pair.inference_equal = NEW.inference_equal
      AND source_pair.catalog_context_equal = NEW.catalog_context_equal
      AND source_pair.only_glossary_job_output_differ = NEW.only_glossary_job_output_differ
      AND source_pair.preferred_revision_id IS NULL
      AND source_pair.correction_asserted = 0 AND source_pair.accuracy_claimed = 0
      AND source_pair.improvement_claimed = 0 AND source_pair.human_review_claimed = 0
      AND source_pair.automatic_merge_allowed = 0
      AND source_pair.publication_authority = 'none'
      AND source_diff.accuracy_claimed = 0 AND source_diff.improvement_claimed = 0
      AND source_diff.preferred_revision_selected = 0
      AND source_diff.human_review_claimed = 0
      AND source_diff.automatic_merge_allowed = 0
      AND source_diff.visibility = 'private' AND source_diff.publication_authority = 'none'
)
BEGIN SELECT RAISE(ABORT, 'projected pair differs from exact no-preference source pair and diff'); END;

-- Seal every generated or selected immutable row after its projection receipt.
CREATE TRIGGER media_local_projection_import_batches_no_update
BEFORE UPDATE ON import_batches
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projection_batches
             WHERE import_batch_id = OLD.import_batch_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection import provenance is append-only'); END;
CREATE TRIGGER media_local_projection_import_batches_no_delete
BEFORE DELETE ON import_batches
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projection_batches
             WHERE import_batch_id = OLD.import_batch_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection import provenance is append-only'); END;
CREATE TRIGGER media_local_projection_source_import_batches_no_update
BEFORE UPDATE ON import_batches
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    LEFT JOIN media_local_asr_imports AS raw_receipt
      ON raw_receipt.media_local_asr_import_id = projection.media_local_asr_import_id
    LEFT JOIN contextual_media_local_asr_imports AS contextual_receipt
      ON contextual_receipt.contextual_asr_import_id = projection.contextual_asr_import_id
    WHERE OLD.import_batch_id IN (
        raw_receipt.import_batch_id, contextual_receipt.import_batch_id
    )
)
BEGIN SELECT RAISE(ABORT, 'projected source import provenance is append-only'); END;
CREATE TRIGGER media_local_projection_source_import_batches_no_delete
BEFORE DELETE ON import_batches
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    LEFT JOIN media_local_asr_imports AS raw_receipt
      ON raw_receipt.media_local_asr_import_id = projection.media_local_asr_import_id
    LEFT JOIN contextual_media_local_asr_imports AS contextual_receipt
      ON contextual_receipt.contextual_asr_import_id = projection.contextual_asr_import_id
    WHERE OLD.import_batch_id IN (
        raw_receipt.import_batch_id, contextual_receipt.import_batch_id
    )
)
BEGIN SELECT RAISE(ABORT, 'projected source import provenance is append-only'); END;
CREATE TRIGGER media_local_projection_preprocess_import_batches_no_update
BEFORE UPDATE ON import_batches
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_asr_imports AS raw_receipt
      ON raw_receipt.media_local_asr_import_id = projection.media_local_asr_import_id
    WHERE raw_receipt.preprocess_import_batch_id IN (
        OLD.import_batch_id, NEW.import_batch_id
    )
)
BEGIN SELECT RAISE(ABORT, 'projected preprocess import provenance is sealed'); END;
CREATE TRIGGER media_local_projection_preprocess_import_batches_no_delete
BEFORE DELETE ON import_batches
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_asr_imports AS raw_receipt
      ON raw_receipt.media_local_asr_import_id = projection.media_local_asr_import_id
    WHERE raw_receipt.preprocess_import_batch_id = OLD.import_batch_id
)
BEGIN SELECT RAISE(ABORT, 'projected preprocess import provenance is sealed'); END;

-- Public-at-review evidence is an immutable acquisition observation, not the
-- mutable current sources.access_state projection.  Seal its exact receipts.
CREATE TRIGGER media_local_projection_public_source_import_batches_no_update
BEFORE UPDATE ON import_batches
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE public_source_import_batch_id IN (OLD.import_batch_id, NEW.import_batch_id)
)
BEGIN SELECT RAISE(ABORT, 'projected public-source acquisition batch is sealed'); END;
CREATE TRIGGER media_local_projection_public_source_import_batches_no_delete
BEFORE DELETE ON import_batches
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE public_source_import_batch_id = OLD.import_batch_id
)
BEGIN SELECT RAISE(ABORT, 'projected public-source acquisition batch is sealed'); END;
CREATE TRIGGER media_local_projection_public_source_import_observations_no_update
BEFORE UPDATE ON import_observations
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE public_source_import_observation_id IN (
        OLD.import_observation_id, NEW.import_observation_id
    )
)
BEGIN SELECT RAISE(ABORT, 'projected public-source acquisition receipt is sealed'); END;
CREATE TRIGGER media_local_projection_public_source_import_observations_no_delete
BEFORE DELETE ON import_observations
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE public_source_import_observation_id = OLD.import_observation_id
)
BEGIN SELECT RAISE(ABORT, 'projected public-source acquisition receipt is sealed'); END;

-- Content identity and the exact selected lineage are sealed.  Current source
-- availability, recording metadata, mapping confidence, rendition labels, and
-- integrity/location projections remain free to evolve.
CREATE TRIGGER media_local_projection_media_identity_no_update
BEFORE UPDATE ON media_objects
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE normalized_media_id = OLD.media_id OR parent_media_id = OLD.media_id
)
AND (
    NEW.media_id IS NOT OLD.media_id
    OR NEW.sha256 IS NOT OLD.sha256
    OR NEW.byte_count IS NOT OLD.byte_count
    OR NEW.media_kind IS NOT OLD.media_kind
    OR NEW.mime_type IS NOT OLD.mime_type
    OR NEW.container IS NOT OLD.container
    OR NEW.duration_ms IS NOT OLD.duration_ms
)
BEGIN SELECT RAISE(ABORT, 'projected media content identity is sealed'); END;
CREATE TRIGGER media_local_projection_media_identity_no_delete
BEFORE DELETE ON media_objects
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE normalized_media_id = OLD.media_id OR parent_media_id = OLD.media_id
)
BEGIN SELECT RAISE(ABORT, 'projected media content identity is sealed'); END;
CREATE TRIGGER media_local_projection_source_identity_no_update
BEFORE UPDATE ON sources
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE qualifying_source_id = OLD.source_id
)
AND (
    NEW.source_id IS NOT OLD.source_id
    OR NEW.platform IS NOT OLD.platform
    OR NEW.source_kind IS NOT OLD.source_kind
    OR NEW.native_id IS NOT OLD.native_id
)
BEGIN SELECT RAISE(ABORT, 'projected source identity is sealed'); END;
CREATE TRIGGER media_local_projection_source_identity_no_delete
BEFORE DELETE ON sources
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE qualifying_source_id = OLD.source_id
)
BEGIN SELECT RAISE(ABORT, 'projected source identity is sealed'); END;
CREATE TRIGGER media_local_projection_derivations_no_update
BEFORE UPDATE ON media_derivations
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections AS projection
    WHERE projection.normalized_media_id = OLD.child_media_id
      AND projection.parent_media_id = OLD.parent_media_id
      AND OLD.derivation_kind = 'audio_normalization_16khz_mono_flac'
)
BEGIN SELECT RAISE(ABORT, 'projected normalized-media derivation is sealed'); END;
CREATE TRIGGER media_local_projection_derivations_no_delete
BEFORE DELETE ON media_derivations
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections AS projection
    WHERE projection.normalized_media_id = OLD.child_media_id
      AND projection.parent_media_id = OLD.parent_media_id
      AND OLD.derivation_kind = 'audio_normalization_16khz_mono_flac'
)
BEGIN SELECT RAISE(ABORT, 'projected normalized-media derivation is sealed'); END;
CREATE TRIGGER media_local_projection_media_sources_no_update
BEFORE UPDATE ON media_sources
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE qualifying_media_source_id IN (OLD.media_source_id, NEW.media_source_id)
)
BEGIN SELECT RAISE(ABORT, 'projected media-source acquisition receipt is sealed'); END;
CREATE TRIGGER media_local_projection_media_sources_no_delete
BEFORE DELETE ON media_sources
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE qualifying_media_source_id = OLD.media_source_id
)
BEGIN SELECT RAISE(ABORT, 'projected media-source acquisition receipt is sealed'); END;
CREATE TRIGGER media_local_projection_recording_source_mapping_no_update
BEFORE UPDATE ON recording_sources
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE qualifying_recording_source_id = OLD.recording_source_id
)
AND (
    NEW.recording_source_id IS NOT OLD.recording_source_id
    OR NEW.recording_id IS NOT OLD.recording_id
    OR NEW.source_id IS NOT OLD.source_id
    OR NEW.mapping_role IS NOT OLD.mapping_role
    OR NEW.source_start_ms IS NOT OLD.source_start_ms
    OR NEW.source_end_ms IS NOT OLD.source_end_ms
    OR NEW.recording_start_ms IS NOT OLD.recording_start_ms
    OR NEW.recording_end_ms IS NOT OLD.recording_end_ms
    OR NEW.mapping_method IS NOT OLD.mapping_method
)
BEGIN SELECT RAISE(ABORT, 'projected recording-source mapping identity is sealed'); END;
CREATE TRIGGER media_local_projection_recording_sources_no_delete
BEFORE DELETE ON recording_sources
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE qualifying_recording_source_id = OLD.recording_source_id
)
BEGIN SELECT RAISE(ABORT, 'projected recording-source mapping identity is sealed'); END;
CREATE TRIGGER media_local_projection_parent_rendition_identity_no_update
BEFORE UPDATE ON renditions
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE parent_rendition_id = OLD.rendition_id
)
AND (
    NEW.rendition_id IS NOT OLD.rendition_id
    OR NEW.recording_id IS NOT OLD.recording_id
    OR NEW.media_id IS NOT OLD.media_id
    OR NEW.rendition_kind IS NOT OLD.rendition_kind
)
BEGIN SELECT RAISE(ABORT, 'projected parent-rendition identity is sealed'); END;
CREATE TRIGGER media_local_projection_parent_renditions_no_delete
BEFORE DELETE ON renditions
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE parent_rendition_id = OLD.rendition_id
)
BEGIN SELECT RAISE(ABORT, 'projected parent-rendition identity is sealed'); END;
CREATE TRIGGER media_local_projection_processing_runs_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE projection_processing_run_id = OLD.processing_run_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection processing provenance is append-only'); END;
CREATE TRIGGER media_local_projection_processing_runs_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE projection_processing_run_id = OLD.processing_run_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection processing provenance is append-only'); END;
CREATE TRIGGER media_local_projection_source_runs_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
)
BEGIN SELECT RAISE(ABORT, 'projected source processing provenance is append-only'); END;
CREATE TRIGGER media_local_projection_source_runs_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id = OLD.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected source processing provenance is append-only'); END;
CREATE TRIGGER media_local_projection_input_artifact_runs_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id IN (
        OLD.processing_run_id, NEW.processing_run_id
    )
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer run is sealed'); END;
CREATE TRIGGER media_local_projection_input_artifact_runs_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id = OLD.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer run is sealed'); END;
CREATE TRIGGER media_local_projection_input_artifact_run_inputs_no_insert
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id = NEW.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer inputs are sealed'); END;
CREATE TRIGGER media_local_projection_input_artifact_run_inputs_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id IN (
        OLD.processing_run_id, NEW.processing_run_id
    )
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer inputs are sealed'); END;
CREATE TRIGGER media_local_projection_input_artifact_run_inputs_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id = OLD.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer inputs are sealed'); END;
CREATE TRIGGER media_local_projection_run_inputs_no_insert
BEFORE INSERT ON run_inputs
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE projection_processing_run_id = NEW.processing_run_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection processing run has one exact input'); END;
CREATE TRIGGER media_local_projection_run_inputs_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE projection_processing_run_id IN (OLD.processing_run_id, NEW.processing_run_id))
BEGIN SELECT RAISE(ABORT, 'media-local projection run inputs are append-only'); END;
CREATE TRIGGER media_local_projection_run_inputs_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE projection_processing_run_id = OLD.processing_run_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection run inputs are append-only'); END;
CREATE TRIGGER media_local_projection_source_run_inputs_no_insert
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id = NEW.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected source run has one exact input'); END;
CREATE TRIGGER media_local_projection_source_run_inputs_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
)
BEGIN SELECT RAISE(ABORT, 'projected source run inputs are append-only'); END;
CREATE TRIGGER media_local_projection_source_run_inputs_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id = OLD.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected source run inputs are append-only'); END;
CREATE TRIGGER media_local_projection_artifacts_no_insert
BEFORE INSERT ON artifacts
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE projection_processing_run_id = NEW.processing_run_id)
BEGIN SELECT RAISE(ABORT, 'media-local projection run has no artifacts'); END;
CREATE TRIGGER media_local_projection_source_artifacts_no_insert
BEFORE INSERT ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id = NEW.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected source artifact set is sealed'); END;
CREATE TRIGGER media_local_projection_source_artifacts_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id IN (OLD.processing_run_id, NEW.processing_run_id)
)
BEGIN SELECT RAISE(ABORT, 'projected source artifacts are append-only'); END;
CREATE TRIGGER media_local_projection_source_artifacts_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM media_local_transcript_projections AS projection
    JOIN media_local_transcript_revisions AS source
      ON source.media_local_revision_id = projection.media_local_revision_id
    WHERE source.processing_run_id = OLD.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected source artifacts are append-only'); END;
CREATE TRIGGER media_local_projection_input_artifact_run_artifacts_no_insert
BEFORE INSERT ON artifacts
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id = NEW.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer artifacts are sealed'); END;
CREATE TRIGGER media_local_projection_input_artifact_run_artifacts_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id IN (
        OLD.processing_run_id, NEW.processing_run_id
    )
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer artifacts are sealed'); END;
CREATE TRIGGER media_local_projection_input_artifact_run_artifacts_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_processing_run_id = OLD.processing_run_id
)
BEGIN SELECT RAISE(ABORT, 'projected input-artifact producer artifacts are sealed'); END;
CREATE TRIGGER media_local_projection_input_artifacts_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_id IN (OLD.artifact_id, NEW.artifact_id)
)
BEGIN SELECT RAISE(ABORT, 'projected normalized input artifact is sealed'); END;
CREATE TRIGGER media_local_projection_input_artifacts_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_projections
    WHERE input_artifact_id = OLD.artifact_id
)
BEGIN SELECT RAISE(ABORT, 'projected normalized input artifact is sealed'); END;
CREATE TRIGGER media_local_projection_source_segments_no_insert
BEFORE INSERT ON media_local_transcript_segments
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE media_local_revision_id = NEW.media_local_revision_id)
BEGIN SELECT RAISE(ABORT, 'a projected media-local segment set is sealed'); END;
CREATE TRIGGER media_local_projection_source_words_no_insert
BEFORE INSERT ON media_local_transcript_words
WHEN EXISTS (
    SELECT 1 FROM media_local_transcript_segments AS segment
    JOIN media_local_transcript_projections AS projection
      ON projection.media_local_revision_id = segment.media_local_revision_id
    WHERE segment.media_local_segment_id = NEW.media_local_segment_id
)
BEGIN SELECT RAISE(ABORT, 'a projected media-local word set is sealed'); END;
CREATE TRIGGER media_local_projection_target_segments_no_insert
BEFORE INSERT ON transcript_segments
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE revision_id = NEW.revision_id)
BEGIN SELECT RAISE(ABORT, 'a projected transcript segment set is sealed'); END;
CREATE TRIGGER media_local_projection_target_words_no_insert
BEFORE INSERT ON transcript_words
WHEN EXISTS (
    SELECT 1 FROM transcript_segments AS segment
    JOIN media_local_transcript_projections AS projection
      ON projection.revision_id = segment.revision_id
    WHERE segment.segment_id = NEW.segment_id
)
BEGIN SELECT RAISE(ABORT, 'a projected transcript word set is sealed'); END;
CREATE TRIGGER media_local_projection_target_parents_no_insert
BEFORE INSERT ON transcript_revision_parents
WHEN EXISTS (SELECT 1 FROM media_local_transcript_projections
             WHERE revision_id = NEW.revision_id)
BEGIN SELECT RAISE(ABORT, 'a projected transcript has no inferred parent revision'); END;

CREATE TRIGGER media_local_projection_publication_forbidden
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type IN (
    'media_local_transcript_projection_batch',
    'media_local_transcript_projection',
    'media_local_transcript_projection_pair'
)
BEGIN SELECT RAISE(ABORT, 'coordinate-projection evidence has no publication lane'); END;
CREATE TRIGGER media_local_projection_gate_forbidden
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type IN (
    'media_local_transcript_projection_batch',
    'media_local_transcript_projection',
    'media_local_transcript_projection_pair'
)
BEGIN SELECT RAISE(ABORT, 'coordinate-projection evidence has no publication lane'); END;
