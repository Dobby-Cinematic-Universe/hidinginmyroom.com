-- Additive admission guards for migration 0023's private contextual-ASR lane.
-- Migration 0023 is already checksummed and remains untouched.

-- A registration must describe the exact legacy glossary-registry row that it
-- protects.  Once registered, that legacy row's mutable provenance columns are
-- frozen as well.
CREATE TRIGGER private_glossary_registration_registry_match
BEFORE INSERT ON private_glossary_registrations
WHEN NOT EXISTS (
    SELECT 1
    FROM glossary_revisions AS glossary
    WHERE glossary.glossary_revision_id = NEW.glossary_revision_id
      AND glossary.sha256 = NEW.raw_sha256
      AND glossary.artifact_uri = NEW.artifact_uri
)
BEGIN
    SELECT RAISE(ABORT, 'private glossary registration differs from glossary registry');
END;

CREATE TRIGGER registered_private_glossary_no_update
BEFORE UPDATE ON glossary_revisions
WHEN EXISTS (
    SELECT 1 FROM private_glossary_registrations AS registration
    WHERE registration.glossary_revision_id = OLD.glossary_revision_id
)
BEGIN
    SELECT RAISE(ABORT, 'registered private glossary provenance is append-only');
END;

CREATE TRIGGER registered_private_glossary_no_delete
BEFORE DELETE ON glossary_revisions
WHEN EXISTS (
    SELECT 1 FROM private_glossary_registrations AS registration
    WHERE registration.glossary_revision_id = OLD.glossary_revision_id
)
BEGIN
    SELECT RAISE(ABORT, 'registered private glossary provenance is append-only');
END;

-- Migration 0023 is a deliberately closed pilot.  Reject lookalike batch or
-- glossary receipts even if their individual columns satisfy generic shape checks.
CREATE TRIGGER private_glossary_registration_closed_pilot
BEFORE INSERT ON private_glossary_registrations
WHEN NEW.glossary_revision_id <> 'glossary_himrverse_neutral_en_20260827_v1'
  OR NEW.raw_sha256 <> '221543ce0a6ef220158d90c00bff95ec8d18aa911b11a40a3ac81e56e2a9b240'
  OR NEW.canonical_sha256 <> '6bc1d768581d399c2ba846aba5454291b67ccea1c29efb452a4740ff3c1ab374'
  OR NEW.byte_count <> 613
  OR NEW.revision_label <> '2026-08-27.1-machine-candidate'
  OR NEW.revision_sha256 <> '69dcc99a3598677ce56e27d4b2916c5b1c9f91d36d8e69c8f7f3b7778370a61a'
  OR NEW.language <> 'en'
  OR NEW.prompt_sha256 <> '4ecd3ddb7e6546d145b260ab42ba64e003224e4b0e70d5d6847179fb01f29f96'
  OR NEW.term_count <> 26
BEGIN
    SELECT RAISE(ABORT, 'private glossary is outside the closed contextual pilot');
END;

CREATE TRIGGER contextual_asr_batch_closed_pilot
BEFORE INSERT ON contextual_asr_batch_registrations
WHEN NEW.contextual_batch_id <> 'ctxasrbatch_b696c14db13d7ae51a207e528c5e1d0e'
  OR NEW.identity_sha256 <> 'b696c14db13d7ae51a207e528c5e1d0eebb9161e1e12ecd1dc017d93c1b47de7'
  OR NEW.manifest_raw_sha256 <> '6742125ffb752778ca082c491f26d1c596c36e566f637ed9b9e8452d3708aa6f'
  OR NEW.manifest_canonical_sha256 <> '743b1a2dc3e0a10efc100c456f3f7613d68470266803df19437657aea6eee13c'
  OR NEW.manifest_byte_count <> 60812
  OR NEW.materializer_version <> '0.1.0'
  OR NEW.work_order_count <> 17
  OR NEW.glossary_revision_id <> 'glossary_himrverse_neutral_en_20260827_v1'
BEGIN
    SELECT RAISE(ABORT, 'contextual ASR batch is outside the closed pilot');
END;

-- Receipt insertion occurs after the transcript revision and batch receipt inside
-- the importer transaction, so these cross-table checks can fail closed without
-- requiring mutable staging rows.
CREATE TRIGGER contextual_media_import_admission_guard
BEFORE INSERT ON contextual_media_local_asr_imports
WHEN NOT EXISTS (
    SELECT 1
    FROM media_local_transcript_revisions AS revision
    JOIN contextual_asr_batch_registrations AS batch
      ON batch.contextual_batch_id = NEW.contextual_batch_id
    WHERE revision.media_local_revision_id = NEW.media_local_revision_id
      AND revision.revision_kind = 'contextual_asr'
      AND revision.glossary_revision_id = batch.glossary_revision_id
      AND revision.review_state = 'machine'
      AND revision.coordinate_system = 'media_ms'
      AND revision.source_coordinate_state = 'unasserted_catalog_context_null'
      AND revision.recording_coordinate_state = 'unasserted_catalog_context_null'
      AND revision.media_id = NEW.input_media_id
      AND revision.input_artifact_id = NEW.input_artifact_id
      AND revision.input_duration_ms = NEW.input_duration_ms
      AND revision.max_segment_end_ms = NEW.max_segment_end_ms
      AND revision.input_boundary_overrun_ms = NEW.input_boundary_overrun_ms
)
BEGIN
    SELECT RAISE(ABORT, 'contextual ASR import differs from revision or batch');
END;

CREATE TRIGGER contextual_media_pair_admission_guard
BEFORE INSERT ON contextual_media_local_asr_pairs
WHEN NOT EXISTS (
    SELECT 1
    FROM media_local_transcript_revisions AS baseline
    JOIN media_local_asr_imports AS baseline_import
      ON baseline_import.media_local_revision_id = baseline.media_local_revision_id
    JOIN media_local_transcript_revisions AS contextual
      ON contextual.media_local_revision_id = NEW.contextual_media_local_revision_id
    JOIN contextual_media_local_asr_imports AS contextual_import
      ON contextual_import.contextual_asr_import_id = NEW.contextual_asr_import_id
     AND contextual_import.media_local_revision_id = contextual.media_local_revision_id
    WHERE baseline.media_local_revision_id = NEW.baseline_media_local_revision_id
      AND baseline.revision_kind = 'raw_asr'
      AND baseline.glossary_revision_id IS NULL
      AND baseline.review_state = 'machine'
      AND contextual.revision_kind = 'contextual_asr'
      AND contextual.glossary_revision_id = NEW.glossary_revision_id
      AND contextual.review_state = 'machine'
      AND baseline.media_id = contextual.media_id
      AND baseline.input_artifact_id = contextual.input_artifact_id
      AND baseline.input_duration_ms = contextual.input_duration_ms
      AND contextual_import.pair_projection_sha256 = NEW.pair_projection_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'contextual ASR pair lacks an exact admitted raw baseline');
END;

CREATE TRIGGER contextual_diff_admission_guard
BEFORE INSERT ON contextual_asr_text_private_diffs
WHEN NOT EXISTS (
    SELECT 1
    FROM contextual_media_local_asr_pairs AS pair
    WHERE pair.contextual_pair_id = NEW.contextual_pair_id
      AND pair.pair_state = 'competing_machine_revisions_no_preference'
      AND pair.preferred_revision_id IS NULL
      AND pair.accuracy_claimed = 0
      AND pair.improvement_claimed = 0
      AND pair.human_review_claimed = 0
      AND pair.automatic_merge_allowed = 0
      AND pair.publication_authority = 'none'
)
BEGIN
    SELECT RAISE(ABORT, 'contextual ASR diff lacks a fail-closed pair');
END;

-- Registration itself may not proceed if a generic decision was pre-created for
-- the deterministic object ID.  The migration-0023 triggers already block future
-- publication and gate decisions.
CREATE TRIGGER contextual_private_no_preexisting_publication
BEFORE INSERT ON private_glossary_registrations
WHEN EXISTS (
    SELECT 1 FROM publication_decisions AS decision
    WHERE decision.object_type = 'private_glossary_registration'
      AND decision.object_id = NEW.private_glossary_registration_id
)
OR EXISTS (
    SELECT 1 FROM publication_gate_decisions AS gate_decision
    WHERE gate_decision.object_type = 'private_glossary_registration'
      AND gate_decision.object_id = NEW.private_glossary_registration_id
)
BEGIN
    SELECT RAISE(ABORT, 'private glossary already has a generic publication decision');
END;
