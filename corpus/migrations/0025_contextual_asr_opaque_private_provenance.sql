-- Opaque private provenance and redacted-run guards for the closed contextual pilot.
-- Migrations 0023 and 0024 are already checksummed and remain untouched.

CREATE TRIGGER private_glossary_registration_opaque_reference
BEFORE INSERT ON private_glossary_registrations
WHEN NEW.artifact_uri <> 'urn:private:sha256:' || NEW.raw_sha256
BEGIN
    SELECT RAISE(ABORT, 'private glossary must use its digest-addressed opaque reference');
END;

CREATE TRIGGER contextual_batch_opaque_reference
BEFORE INSERT ON contextual_asr_batch_registrations
WHEN NEW.manifest_uri <> 'urn:private:sha256:' || NEW.manifest_raw_sha256
BEGIN
    SELECT RAISE(ABORT, 'contextual batch must use its digest-addressed opaque reference');
END;

CREATE TRIGGER contextual_import_opaque_references
BEFORE INSERT ON contextual_media_local_asr_imports
WHEN NEW.work_order_uri <> 'urn:private:sha256:' || NEW.work_order_raw_sha256
  OR NEW.result_uri <> 'urn:private:sha256:' || NEW.result_raw_sha256
BEGIN
    SELECT RAISE(ABORT, 'contextual import must use digest-addressed opaque references');
END;

CREATE TRIGGER contextual_diff_opaque_reference
BEFORE INSERT ON contextual_asr_text_private_diffs
WHEN NEW.diff_uri <> 'urn:private:sha256:' || NEW.diff_raw_sha256
BEGIN
    SELECT RAISE(ABORT, 'contextual diff must use its digest-addressed opaque reference');
END;

-- The producer result remains the exact private replay source.  SQLite receives a
-- deterministic environment projection: execution facts and integrity digests,
-- never argv, the prompt, or local paths.
CREATE TRIGGER contextual_processing_run_redacted_projection
BEFORE INSERT ON processing_runs
WHEN NEW.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
 AND (
       NEW.stage <> 'asr_whispercpp'
    OR NEW.status <> 'completed'
    OR NEW.error_text IS NOT NULL
    OR NEW.environment_json <> json(NEW.environment_json)
    OR json_type(NEW.environment_json) <> 'object'
    OR (SELECT count(*) FROM json_each(NEW.environment_json)) <> 3
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.environment_json)
        WHERE key NOT IN ('projection_version', 'execution', 'integrity')
    )
    OR json_extract(NEW.environment_json, '$.projection_version')
       IS NOT 'contextual_asr_environment_redacted_v1'
    OR json_type(NEW.environment_json, '$.execution') <> 'object'
    OR (SELECT count(*) FROM json_each(NEW.environment_json, '$.execution')) <> 9
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.environment_json, '$.execution')
        WHERE key NOT IN (
            'cpu_only', 'network', 'engine_version', 'engine_version_evidence',
            'python', 'descriptor_execution_policy', 'logical_command_count',
            'result_command_states', 'prompt_present'
        )
    )
    OR json_extract(NEW.environment_json, '$.execution.cpu_only') IS NOT 1
    OR json_extract(NEW.environment_json, '$.execution.network') IS NOT 'not_used'
    OR json_extract(NEW.environment_json, '$.execution.engine_version')
       IS NOT 'whisper.cpp v1.8.7'
    OR json_extract(NEW.environment_json, '$.execution.engine_version_evidence')
       IS NOT 'source_revision_plus_executable_sha256'
    OR json_extract(NEW.environment_json, '$.execution.python') IS NOT '3.14.7'
    OR json_extract(NEW.environment_json, '$.execution.descriptor_execution_policy')
       IS NOT 'linux_proc_self_fd_retained_verified_v1'
    OR json_extract(NEW.environment_json, '$.execution.logical_command_count') IS NOT 2
    OR json_extract(NEW.environment_json, '$.execution.prompt_present') IS NOT 1
    OR json_type(NEW.environment_json, '$.execution.result_command_states') <> 'array'
    OR json_array_length(
           json_extract(NEW.environment_json, '$.execution.result_command_states')
       ) <> 2
    OR json_extract(NEW.environment_json, '$.execution.result_command_states[0]')
       IS NOT 'executed'
    OR json_extract(NEW.environment_json, '$.execution.result_command_states[1]')
       IS NOT 'executed'
    OR json_type(NEW.environment_json, '$.integrity') <> 'object'
    OR (SELECT count(*) FROM json_each(NEW.environment_json, '$.integrity')) <> 7
    OR EXISTS (
        SELECT 1 FROM json_each(NEW.environment_json, '$.integrity')
        WHERE key NOT IN (
            'source_environment_json_sha256', 'parameters_json_sha256',
            'logical_commands_sha256', 'logical_command_sha256',
            'result_commands_sha256', 'result_command_sha256', 'prompt_sha256'
        )
    )
    OR json_type(
           NEW.environment_json, '$.integrity.source_environment_json_sha256'
       ) <> 'text'
    OR json_type(
           NEW.environment_json, '$.integrity.parameters_json_sha256'
       ) <> 'text'
    OR json_type(
           NEW.environment_json, '$.integrity.logical_commands_sha256'
       ) <> 'text'
    OR json_type(
           NEW.environment_json, '$.integrity.result_commands_sha256'
       ) <> 'text'
    OR json_type(NEW.environment_json, '$.integrity.prompt_sha256') <> 'text'
    OR json_type(NEW.environment_json, '$.integrity.logical_command_sha256') <> 'array'
    OR json_array_length(
           json_extract(NEW.environment_json, '$.integrity.logical_command_sha256')
       ) <> 2
    OR json_type(NEW.environment_json, '$.integrity.result_command_sha256') <> 'array'
    OR json_array_length(
           json_extract(NEW.environment_json, '$.integrity.result_command_sha256')
       ) <> 2
    OR EXISTS (
        SELECT 1
        FROM json_each(
            NEW.environment_json, '$.integrity.logical_command_sha256'
        )
        WHERE type <> 'text'
           OR length(value) <> 64
           OR value GLOB '*[^0-9a-f]*'
    )
    OR EXISTS (
        SELECT 1
        FROM json_each(
            NEW.environment_json, '$.integrity.result_command_sha256'
        )
        WHERE type <> 'text'
           OR length(value) <> 64
           OR value GLOB '*[^0-9a-f]*'
    )
    OR EXISTS (
        SELECT 1
        FROM json_tree(NEW.environment_json)
        WHERE type = 'text'
          AND (
               atom LIKE '/%'
            OR lower(atom) LIKE 'file:%'
            OR instr(atom, '--prompt') > 0
            OR atom GLOB '[A-Za-z]:\\*'
          )
    )
    OR EXISTS (
        SELECT 1
        FROM json_tree(NEW.environment_json, '$.integrity')
        WHERE key LIKE '%sha256%'
          AND type = 'text'
          AND (length(atom) <> 64 OR atom GLOB '*[^0-9a-f]*')
    )
    OR json_type(NEW.environment_json, '$.command_provenance') IS NOT NULL
    OR json_type(NEW.environment_json, '$.logical_commands') IS NOT NULL
    OR json_type(NEW.environment_json, '$.prompt') IS NOT NULL
    OR json_type(NEW.parameters_json, '$.glossary') IS NOT 'object'
    OR json_extract(NEW.parameters_json, '$.glossary.glossary_revision_id')
       IS NOT NEW.glossary_revision_id
    OR json_extract(NEW.parameters_json, '$.glossary.sha256')
       IS NOT '221543ce0a6ef220158d90c00bff95ec8d18aa911b11a40a3ac81e56e2a9b240'
    OR json_extract(NEW.parameters_json, '$.glossary.prompt_sha256')
       IS NOT '4ecd3ddb7e6546d145b260ab42ba64e003224e4b0e70d5d6847179fb01f29f96'
    OR json_extract(NEW.environment_json, '$.integrity.prompt_sha256')
       IS NOT '4ecd3ddb7e6546d145b260ab42ba64e003224e4b0e70d5d6847179fb01f29f96'
 )
BEGIN
    SELECT RAISE(ABORT, 'contextual processing run is not a safe redacted projection');
END;

CREATE TRIGGER contextual_artifact_opaque_reference
BEFORE INSERT ON artifacts
WHEN EXISTS (
    SELECT 1 FROM processing_runs AS run
    WHERE run.processing_run_id = NEW.processing_run_id
      AND run.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
)
 AND (
       NEW.storage_uri <> 'urn:private:sha256:' || NEW.sha256
    OR NEW.visibility <> 'private'
    OR NEW.artifact_kind NOT IN (
        'whispercpp_output_json_full', 'transcript_normalized_json'
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'contextual artifact must use opaque private provenance');
END;

CREATE TRIGGER contextual_import_projected_graph_guard
BEFORE INSERT ON contextual_media_local_asr_imports
WHEN NOT EXISTS (
    SELECT 1
    FROM media_local_transcript_revisions AS revision
    JOIN processing_runs AS run
      ON run.processing_run_id = revision.processing_run_id
    JOIN private_glossary_registrations AS glossary
      ON glossary.glossary_revision_id = run.glossary_revision_id
    WHERE revision.media_local_revision_id = NEW.media_local_revision_id
      AND run.environment_json = json(run.environment_json)
      AND json_extract(run.environment_json, '$.projection_version')
          = 'contextual_asr_environment_redacted_v1'
      AND json_extract(run.environment_json, '$.integrity.prompt_sha256')
          = glossary.prompt_sha256
      AND (SELECT count(*) FROM artifacts AS artifact
           WHERE artifact.processing_run_id = run.processing_run_id) = 2
      AND (SELECT count(*) FROM artifacts AS artifact
           WHERE artifact.processing_run_id = run.processing_run_id
             AND artifact.storage_uri = 'urn:private:sha256:' || artifact.sha256
             AND artifact.visibility = 'private'
             AND artifact.artifact_kind IN (
                 'whispercpp_output_json_full', 'transcript_normalized_json'
             )) = 2
      AND (SELECT count(DISTINCT artifact.artifact_kind)
           FROM artifacts AS artifact
           WHERE artifact.processing_run_id = run.processing_run_id) = 2
      AND (SELECT count(*) FROM run_inputs AS input
           WHERE input.processing_run_id = run.processing_run_id) = 1
      AND EXISTS (
          SELECT 1 FROM run_inputs AS input
          WHERE input.processing_run_id = run.processing_run_id
            AND input.object_type = 'media'
            AND input.object_id = NEW.input_media_id
            AND input.input_role = 'normalized_audio'
            AND input.input_sha256 = (
                SELECT media.sha256 FROM media_objects AS media
                WHERE media.media_id = NEW.input_media_id
            )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'contextual import lacks its exact redacted private graph');
END;

CREATE TRIGGER contextual_artifact_no_insert_after_receipt
BEFORE INSERT ON artifacts
WHEN EXISTS (
    SELECT 1
    FROM contextual_media_local_asr_imports AS receipt
    JOIN media_local_transcript_revisions AS revision
      ON revision.media_local_revision_id = receipt.media_local_revision_id
    WHERE revision.processing_run_id = NEW.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'admitted contextual processing run has an exact artifact set');
END;

CREATE TRIGGER contextual_run_input_no_insert_after_receipt
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1
    FROM contextual_media_local_asr_imports AS receipt
    JOIN media_local_transcript_revisions AS revision
      ON revision.media_local_revision_id = receipt.media_local_revision_id
    WHERE revision.processing_run_id = NEW.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'admitted contextual processing run has one exact input');
END;

CREATE TRIGGER contextual_processing_run_no_update
BEFORE UPDATE ON processing_runs
WHEN OLD.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
  OR NEW.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
BEGIN
    SELECT RAISE(ABORT, 'contextual processing-run provenance is append-only');
END;

CREATE TRIGGER contextual_processing_run_no_delete
BEFORE DELETE ON processing_runs
WHEN OLD.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
BEGIN
    SELECT RAISE(ABORT, 'contextual processing-run provenance is append-only');
END;

CREATE TRIGGER contextual_artifact_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM processing_runs AS run
    WHERE run.processing_run_id = OLD.processing_run_id
      AND run.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
)
OR EXISTS (
    SELECT 1 FROM processing_runs AS run
    WHERE run.processing_run_id = NEW.processing_run_id
      AND run.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
)
BEGIN
    SELECT RAISE(ABORT, 'contextual artifact provenance is append-only');
END;

CREATE TRIGGER contextual_artifact_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
    SELECT 1 FROM processing_runs AS run
    WHERE run.processing_run_id = OLD.processing_run_id
      AND run.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
)
BEGIN
    SELECT RAISE(ABORT, 'contextual artifact provenance is append-only');
END;

CREATE TRIGGER contextual_run_input_no_update
BEFORE UPDATE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM processing_runs AS run
    WHERE run.processing_run_id = OLD.processing_run_id
      AND run.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
)
OR EXISTS (
    SELECT 1 FROM processing_runs AS run
    WHERE run.processing_run_id = NEW.processing_run_id
      AND run.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
)
BEGIN
    SELECT RAISE(ABORT, 'contextual run-input provenance is append-only');
END;

CREATE TRIGGER contextual_run_input_no_delete
BEFORE DELETE ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM processing_runs AS run
    WHERE run.processing_run_id = OLD.processing_run_id
      AND run.glossary_revision_id = 'glossary_himrverse_neutral_en_20260827_v1'
)
BEGIN
    SELECT RAISE(ABORT, 'contextual run-input provenance is append-only');
END;
