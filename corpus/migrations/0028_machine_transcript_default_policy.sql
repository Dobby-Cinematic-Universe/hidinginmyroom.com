-- A machine transcript in ordinary recording coordinates may receive its initial
-- publication decision from one closed automated policy after the three independent
-- publication gates have current human clear decisions.  The policy cannot clear a
-- gate, review wording, alter lifecycle state, correct text, or override any existing
-- publication stream.

-- Reserve every identity and prefix before creating the capability.  Migration 0027
-- allowed arbitrary automated-policy identities to make restrictive decisions; a
-- preexisting lookalike must not be silently adopted as this new built-in policy.
CREATE TABLE migration_0028_machine_policy_preflight_guard(singleton INTEGER);

CREATE TRIGGER migration_0028_machine_policy_preflight_abort
BEFORE INSERT ON migration_0028_machine_policy_preflight_guard
BEGIN
    SELECT RAISE(ABORT, 'reserved machine transcript policy provenance already exists');
END;

INSERT INTO migration_0028_machine_policy_preflight_guard(singleton)
SELECT 1
WHERE EXISTS (
        SELECT 1 FROM reviewers
        WHERE reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
    )
 OR EXISTS (
        SELECT 1 FROM reviewer_admin_manifest_imports
        WHERE manifest_id = 'reviewer-admin-machine-transcript-default-policy-v1'
    )
 OR EXISTS (
        SELECT 1 FROM reviewer_admin_events
        WHERE reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
           OR reviewer_admin_event_id IN (
               'register_machine_transcript_default_policy_v1',
               'activate_machine_transcript_default_policy_v1'
           )
    )
 OR EXISTS (
        SELECT 1 FROM publication_manifest_imports
        WHERE manifest_id GLOB 'machine-transcript-default-policy-v1:*'
    )
 OR EXISTS (
        SELECT 1 FROM publication_decisions
        WHERE reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
           OR publication_decision_id GLOB 'machine-transcript-default-v1:*'
    )
 OR EXISTS (
        SELECT 1 FROM publication_gate_decisions
        WHERE reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
           OR publication_gate_decision_id GLOB 'machine-transcript-default-v1:*'
    )
 OR EXISTS (
        SELECT 1 FROM review_decisions
        WHERE reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
    )
 OR EXISTS (
        SELECT 1 FROM corrections
        WHERE reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
    )
 OR EXISTS (
        SELECT 1 FROM transcript_lifecycle_decisions
        WHERE reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
    );

DROP TABLE migration_0028_machine_policy_preflight_guard;

CREATE TABLE machine_transcript_publication_policy_runs (
    plan_sha256 TEXT PRIMARY KEY CHECK(length(plan_sha256) = 64),
    policy_id TEXT NOT NULL
        CHECK(policy_id = 'machine_transcript_default_publication_v1'),
    manifest_id TEXT NOT NULL UNIQUE
        CHECK(manifest_id = 'machine-transcript-default-policy-v1:' || plan_sha256)
        REFERENCES publication_manifest_imports(manifest_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    reviewer_id TEXT NOT NULL
        CHECK(reviewer_id = 'reviewer_machine_transcript_default_policy_v1')
        REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT,
    plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),
    eligible_revision_count INTEGER NOT NULL CHECK(eligible_revision_count > 0),
    new_decision_count INTEGER NOT NULL CHECK(new_decision_count > 0),
    existing_policy_count INTEGER NOT NULL CHECK(existing_policy_count = 0),
    protected_existing_count INTEGER NOT NULL CHECK(protected_existing_count = 0),
    applied_at TEXT NOT NULL
        CHECK(julianday(applied_at) IS NOT NULL)
        CHECK(applied_at GLOB '????-??-??T??:??:??Z')
        CHECK(strftime('%Y-%m-%dT%H:%M:%SZ', applied_at) = applied_at),
    CHECK(eligible_revision_count = new_decision_count)
);

CREATE TRIGGER machine_transcript_policy_runs_no_update
BEFORE UPDATE ON machine_transcript_publication_policy_runs
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy runs are append-only');
END;

CREATE TRIGGER machine_transcript_policy_runs_no_delete
BEFORE DELETE ON machine_transcript_publication_policy_runs
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy runs are append-only');
END;

CREATE TRIGGER machine_transcript_policy_runs_no_replace
BEFORE INSERT ON machine_transcript_publication_policy_runs
WHEN EXISTS (
    SELECT 1
    FROM machine_transcript_publication_policy_runs AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.plan_sha256 = NEW.plan_sha256
       OR existing.manifest_id = NEW.manifest_id
)
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy run replacement is forbidden');
END;

-- The dedicated transaction inserts the tightly validated run first, followed by
-- its reserved publication manifest. The deferred FK prevents a run from committing
-- without that manifest; this trigger prevents ordinary publication administration
-- from squatting the namespace before a run exists.
CREATE TRIGGER machine_transcript_policy_manifest_namespace
BEFORE INSERT ON publication_manifest_imports
WHEN NEW.manifest_id GLOB 'machine-transcript-default-policy-v1:*'
 AND NOT EXISTS (
    SELECT 1
    FROM machine_transcript_publication_policy_runs AS run
    WHERE run.manifest_id = NEW.manifest_id
      AND run.plan_sha256 = NEW.input_sha256
      AND NEW.schema_version = 1
      AND NEW.publication_decision_count = run.new_decision_count
      AND NEW.gate_decision_count = 0
      AND NEW.imported_at = run.applied_at
 )
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy manifest namespace is dedicated');
END;

CREATE TRIGGER machine_transcript_policy_decision_namespace
BEFORE INSERT ON publication_decisions
WHEN NEW.publication_decision_id GLOB 'machine-transcript-default-v1:*'
 AND NEW.reviewer_id <> 'reviewer_machine_transcript_default_policy_v1'
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy decision namespace is dedicated');
END;

CREATE TRIGGER machine_transcript_policy_gate_decision_namespace
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.publication_gate_decision_id GLOB 'machine-transcript-default-v1:*'
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy decision namespace is dedicated');
END;

-- A run is itself a closed, text-free authorization receipt.  Every planned
-- revision must still be in the exact scope at run insertion, and all plan fields
-- used to reconstruct the authorization are pinned to that scope.  Publication
-- decisions are inserted later in the same BEGIN IMMEDIATE transaction.
CREATE TRIGGER machine_transcript_policy_run_scope_exact
BEFORE INSERT ON machine_transcript_publication_policy_runs
WHEN julianday(NEW.applied_at) >
       julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
  OR NOT EXISTS (
      SELECT 1
      FROM reviewers AS reviewer
      WHERE reviewer.reviewer_id = NEW.reviewer_id
        AND reviewer.reviewer_kind = 'automated_policy'
        AND reviewer.active = 1
  )
  OR NOT EXISTS (
      SELECT 1
      FROM reviewer_admin_manifest_imports AS manifest
      JOIN reviewer_admin_events AS registration
        ON registration.manifest_id = manifest.manifest_id
       AND registration.reviewer_admin_event_id =
           'register_machine_transcript_default_policy_v1'
       AND registration.ordinal = 0
       AND registration.event_kind = 'register'
       AND registration.reviewer_id = NEW.reviewer_id
       AND registration.display_label =
           'Machine transcript default publication policy v1'
       AND registration.reviewer_kind = 'automated_policy'
       AND registration.previous_active IS NULL
       AND registration.new_active = 0
       AND registration.basis =
           'Register the exact built-in machine transcript publication policy inactive.'
      JOIN reviewer_admin_events AS activation
        ON activation.manifest_id = manifest.manifest_id
       AND activation.reviewer_admin_event_id =
           'activate_machine_transcript_default_policy_v1'
       AND activation.ordinal = 1
       AND activation.event_kind = 'set_active'
       AND activation.reviewer_id = NEW.reviewer_id
       AND activation.display_label =
           'Machine transcript default publication policy v1'
       AND activation.reviewer_kind = 'automated_policy'
       AND activation.previous_active = 0
       AND activation.new_active = 1
       AND activation.basis =
           'Activate only the gate-dependent machine transcript publication policy.'
      WHERE manifest.manifest_id =
            'reviewer-admin-machine-transcript-default-policy-v1'
        AND manifest.schema_version = 1
        AND manifest.authorized_by =
            'builtin_machine_transcript_default_policy_v1'
        AND manifest.basis =
            'Constrained built-in publication policy for ordinary-coordinate machine transcripts after independent human gate clearances.'
        AND manifest.registration_count = 1
        AND manifest.state_change_count = 1
        AND manifest.adoption_count = 0
        AND (SELECT count(*) FROM reviewer_admin_events AS policy_event
             WHERE policy_event.reviewer_id = NEW.reviewer_id
                OR policy_event.manifest_id = manifest.manifest_id) = 2
  )
  OR NOT EXISTS (
      SELECT 1
      FROM reviewer_admin_events AS event
      WHERE event.reviewer_id = NEW.reviewer_id
        AND julianday(event.effective_at) <= julianday(NEW.applied_at)
        AND event.new_active = 1
        AND NOT EXISTS (
            SELECT 1
            FROM reviewer_admin_events AS later
            WHERE later.reviewer_id = event.reviewer_id
              AND julianday(later.effective_at) <= julianday(NEW.applied_at)
              AND later.event_sequence > event.event_sequence
        )
  )
  OR json_type(NEW.plan_json, '$') IS NOT 'object'
  OR json_extract(NEW.plan_json, '$.schema_version') IS NOT 1
  OR json_extract(NEW.plan_json, '$.policy_id')
       IS NOT 'machine_transcript_default_publication_v1'
  OR json_extract(NEW.plan_json, '$.reviewer_id')
       IS NOT 'reviewer_machine_transcript_default_policy_v1'
  OR json_extract(NEW.plan_json, '$.public_label')
       IS NOT 'machine transcript (unreviewed)'
  OR json_extract(NEW.plan_json, '$.basis') IS NOT
       'Closed automated policy v1: the ordinary recording-coordinate machine transcript has current human clear decisions for each independent rights, privacy, and sensitivity gate; no wording review was performed.'
  OR json_extract(NEW.plan_json, '$.disclaimer_code') IS NOT
       'machine_generated_unreviewed_not_verified_quotation_v1'
  OR json_extract(NEW.plan_json, '$.disclaimer') IS NOT
       'Machine-generated and unreviewed; may be wrong; not a verified quotation.'
  OR json_extract(NEW.plan_json, '$.eligible_revision_count')
       IS NOT NEW.eligible_revision_count
  OR json_type(NEW.plan_json, '$.revisions') IS NOT 'array'
  OR (
      SELECT count(*) FROM json_each(NEW.plan_json)
  ) IS NOT 9
  OR EXISTS (
      SELECT 1 FROM json_each(NEW.plan_json) AS top_level
      WHERE top_level.key NOT IN (
          'schema_version', 'policy_id', 'reviewer_id', 'public_label',
          'basis', 'disclaimer_code', 'disclaimer',
          'eligible_revision_count', 'revisions'
      )
  )
  OR json_array_length(NEW.plan_json, '$.revisions')
       IS NOT NEW.eligible_revision_count
  OR NEW.eligible_revision_count IS NOT (
      SELECT count(*) FROM machine_transcript_policy_publish_scope
  )
  OR (
      SELECT count(DISTINCT json_extract(item.value, '$.revision_id'))
      FROM json_each(NEW.plan_json, '$.revisions') AS item
  ) IS NOT NEW.eligible_revision_count
  OR EXISTS (
      SELECT 1
      FROM json_each(NEW.plan_json, '$.revisions') AS item
      LEFT JOIN machine_transcript_policy_publish_scope AS scope
        ON scope.revision_id = json_extract(item.value, '$.revision_id')
       AND scope.recording_id = json_extract(item.value, '$.recording_id')
       AND scope.revision_kind = json_extract(item.value, '$.revision_kind')
       AND json_extract(item.value, '$.review_state') = 'machine'
       AND scope.language = json_extract(item.value, '$.language')
       AND scope.created_at = json_extract(item.value, '$.created_at')
       AND julianday(scope.created_at) <= julianday(NEW.applied_at)
       AND scope.segment_count = json_extract(item.value, '$.segment_count')
       AND scope.rights_gate_decision_id = json_extract(
            item.value, '$.gates.rights.publication_gate_decision_id'
       )
       AND scope.rights_reviewer_id = json_extract(
            item.value, '$.gates.rights.reviewer_id'
       )
       AND scope.rights_decided_at = json_extract(
            item.value, '$.gates.rights.decided_at'
       )
       AND julianday(scope.rights_decided_at) <= julianday(NEW.applied_at)
       AND scope.privacy_gate_decision_id = json_extract(
            item.value, '$.gates.privacy.publication_gate_decision_id'
       )
       AND scope.privacy_reviewer_id = json_extract(
            item.value, '$.gates.privacy.reviewer_id'
       )
       AND scope.privacy_decided_at = json_extract(
            item.value, '$.gates.privacy.decided_at'
       )
       AND julianday(scope.privacy_decided_at) <= julianday(NEW.applied_at)
       AND scope.sensitivity_gate_decision_id = json_extract(
            item.value, '$.gates.sensitivity.publication_gate_decision_id'
       )
       AND scope.sensitivity_reviewer_id = json_extract(
            item.value, '$.gates.sensitivity.reviewer_id'
       )
       AND scope.sensitivity_decided_at = json_extract(
            item.value, '$.gates.sensitivity.decided_at'
       )
       AND julianday(scope.sensitivity_decided_at) <= julianday(NEW.applied_at)
      WHERE json_type(item.value, '$') IS NOT 'object'
         OR (SELECT count(*) FROM json_each(item.value)) IS NOT 8
         OR EXISTS (
             SELECT 1 FROM json_each(item.value) AS revision_field
             WHERE revision_field.key NOT IN (
                 'revision_id', 'recording_id', 'revision_kind',
                 'review_state', 'language', 'created_at', 'segment_count',
                 'gates'
             )
         )
         OR json_type(item.value, '$.gates') IS NOT 'object'
         OR (SELECT count(*) FROM json_each(item.value, '$.gates')) IS NOT 3
         OR EXISTS (
             SELECT 1 FROM json_each(item.value, '$.gates') AS gate_entry
             WHERE gate_entry.key NOT IN ('rights', 'privacy', 'sensitivity')
                OR json_type(gate_entry.value, '$') IS NOT 'object'
                OR (SELECT count(*) FROM json_each(gate_entry.value)) IS NOT 3
                OR EXISTS (
                    SELECT 1 FROM json_each(gate_entry.value) AS gate_field
                    WHERE gate_field.key NOT IN (
                        'publication_gate_decision_id', 'reviewer_id',
                        'decided_at'
                    )
                )
         )
         OR scope.revision_id IS NULL
  )
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy run is outside its exact initial-publication scope');
END;

-- The intermediate view names each current gate clearance separately.  A gate is
-- eligible only when its current decision is a human clear made inside a recorded
-- active interval.  This deliberately excludes pre-governance automated clear rows.
CREATE VIEW machine_transcript_policy_human_gate_scope AS
SELECT gate.publication_gate_decision_id,
       gate.object_id AS revision_id,
       gate.gate_kind,
       gate.reviewer_id,
       gate.decided_at
FROM current_publication_gate_decisions AS gate
JOIN reviewers AS reviewer
  ON reviewer.reviewer_id = gate.reviewer_id
WHERE gate.object_type = 'transcript_revision'
  AND gate.decision = 'clear'
  AND reviewer.reviewer_kind = 'human'
  AND EXISTS (
      SELECT 1
      FROM reviewer_admin_events AS event
      WHERE event.reviewer_id = gate.reviewer_id
        AND julianday(event.effective_at) <= julianday(gate.decided_at)
        AND event.new_active = 1
        AND NOT EXISTS (
            SELECT 1
            FROM reviewer_admin_events AS later
            WHERE later.reviewer_id = event.reviewer_id
              AND julianday(later.effective_at) <= julianday(gate.decided_at)
              AND later.event_sequence > event.event_sequence
        )
  );

-- This is the entire positive capability.  It contains no transcript wording and
-- cannot include rendition-local or media-local hypotheses because those live in
-- separate private tables.  Any lifecycle history removes a revision from the
-- default lane; a human must handle a dispute, retraction, or reinstatement.
CREATE VIEW machine_transcript_policy_publish_scope AS
SELECT revision.revision_id,
       revision.recording_id,
       revision.revision_kind,
       revision.language,
       revision.created_at,
       count(DISTINCT segment.segment_id) AS segment_count,
       max(CASE WHEN gate.gate_kind = 'rights'
                THEN gate.publication_gate_decision_id END)
           AS rights_gate_decision_id,
       max(CASE WHEN gate.gate_kind = 'rights' THEN gate.reviewer_id END)
           AS rights_reviewer_id,
       max(CASE WHEN gate.gate_kind = 'rights' THEN gate.decided_at END)
           AS rights_decided_at,
       max(CASE WHEN gate.gate_kind = 'privacy'
                THEN gate.publication_gate_decision_id END)
           AS privacy_gate_decision_id,
       max(CASE WHEN gate.gate_kind = 'privacy' THEN gate.reviewer_id END)
           AS privacy_reviewer_id,
       max(CASE WHEN gate.gate_kind = 'privacy' THEN gate.decided_at END)
           AS privacy_decided_at,
       max(CASE WHEN gate.gate_kind = 'sensitivity'
                THEN gate.publication_gate_decision_id END)
           AS sensitivity_gate_decision_id,
       max(CASE WHEN gate.gate_kind = 'sensitivity' THEN gate.reviewer_id END)
           AS sensitivity_reviewer_id,
       max(CASE WHEN gate.gate_kind = 'sensitivity' THEN gate.decided_at END)
           AS sensitivity_decided_at,
       'machine transcript (unreviewed)' AS public_label,
       'Closed automated policy v1: the ordinary recording-coordinate machine transcript has current human clear decisions for each independent rights, privacy, and sensitivity gate; no wording review was performed.' AS basis,
       'machine_generated_unreviewed_not_verified_quotation_v1' AS disclaimer_code,
       'Machine-generated and unreviewed; may be wrong; not a verified quotation.' AS disclaimer_text
FROM transcript_revisions AS revision
JOIN transcript_segments AS segment
  ON segment.revision_id = revision.revision_id
JOIN machine_transcript_policy_human_gate_scope AS gate
  ON gate.revision_id = revision.revision_id
WHERE revision.review_state = 'machine'
  AND revision.revision_kind IN ('raw_asr', 'contextual_asr')
  AND julianday(revision.created_at) <=
      julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
  AND julianday(gate.decided_at) >= julianday(revision.created_at)
  AND NOT EXISTS (
      SELECT 1
      FROM transcript_segments AS named_segment
      WHERE named_segment.revision_id = revision.revision_id
        AND named_segment.speaker_label IS NOT NULL
  )
  AND NOT EXISTS (
      SELECT 1
      FROM transcript_lifecycle_decisions AS lifecycle
      WHERE lifecycle.revision_id = revision.revision_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM publication_decisions AS publication
      WHERE publication.object_type = 'transcript_revision'
        AND publication.object_id = revision.revision_id
  )
GROUP BY revision.revision_id, revision.recording_id, revision.revision_kind,
         revision.language, revision.created_at
HAVING count(DISTINCT gate.gate_kind) = 3;

-- Replace migration 0027's reviewer capability triggers without widening the
-- existing public-metadata policy.  Ordinary automated policies remain restrictive
-- only.  The new built-in policy can publish solely through a recorded policy run
-- and the exact scope, label, basis, warning, and null wording-review reference.
DROP TRIGGER reviewer_admin_publication_reviewer_scope;

CREATE TRIGGER reviewer_admin_publication_reviewer_scope
BEFORE INSERT ON publication_decisions
WHEN EXISTS (
    SELECT 1
    FROM reviewers AS reviewer
    WHERE reviewer.reviewer_id = NEW.reviewer_id
      AND (
          reviewer.reviewer_kind = 'imported_legacy'
          OR (
              reviewer.reviewer_id = 'reviewer_public_metadata_policy_v1'
              AND NOT (
                  NEW.decision = 'publish'
                  AND EXISTS (
                      SELECT 1
                      FROM public_metadata_policy_publish_scope AS scope
                      WHERE scope.object_type = NEW.object_type
                        AND scope.object_id = NEW.object_id
                        AND scope.public_label = NEW.public_label
                        AND scope.basis = NEW.basis
                  )
              )
          )
          OR (
              reviewer.reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
              AND NOT (
                  NEW.object_type = 'transcript_revision'
                  AND NEW.decision = 'publish'
                  AND NEW.review_decision_id IS NULL
                  AND NEW.publication_decision_id =
                      'machine-transcript-default-v1:' || NEW.object_id
                  AND NEW.manifest_id IS NOT NULL
                  AND EXISTS (
                      SELECT 1
                      FROM machine_transcript_publication_policy_runs AS run
                      JOIN json_each(run.plan_json, '$.revisions') AS item
                      JOIN machine_transcript_policy_publish_scope AS scope
                        ON scope.revision_id =
                           json_extract(item.value, '$.revision_id')
                       AND scope.recording_id =
                           json_extract(item.value, '$.recording_id')
                       AND scope.revision_kind =
                           json_extract(item.value, '$.revision_kind')
                       AND json_extract(item.value, '$.review_state') = 'machine'
                       AND scope.language =
                           json_extract(item.value, '$.language')
                       AND scope.created_at =
                           json_extract(item.value, '$.created_at')
                       AND scope.segment_count =
                           json_extract(item.value, '$.segment_count')
                       AND scope.rights_gate_decision_id = json_extract(
                            item.value,
                            '$.gates.rights.publication_gate_decision_id'
                       )
                       AND scope.rights_reviewer_id = json_extract(
                            item.value, '$.gates.rights.reviewer_id'
                       )
                       AND scope.rights_decided_at = json_extract(
                            item.value, '$.gates.rights.decided_at'
                       )
                       AND scope.privacy_gate_decision_id = json_extract(
                            item.value,
                            '$.gates.privacy.publication_gate_decision_id'
                       )
                       AND scope.privacy_reviewer_id = json_extract(
                            item.value, '$.gates.privacy.reviewer_id'
                       )
                       AND scope.privacy_decided_at = json_extract(
                            item.value, '$.gates.privacy.decided_at'
                       )
                       AND scope.sensitivity_gate_decision_id = json_extract(
                            item.value,
                            '$.gates.sensitivity.publication_gate_decision_id'
                       )
                       AND scope.sensitivity_reviewer_id = json_extract(
                            item.value, '$.gates.sensitivity.reviewer_id'
                       )
                       AND scope.sensitivity_decided_at = json_extract(
                            item.value, '$.gates.sensitivity.decided_at'
                       )
                      WHERE run.manifest_id = NEW.manifest_id
                        AND run.reviewer_id = NEW.reviewer_id
                        AND run.applied_at = NEW.decided_at
                        AND scope.revision_id = NEW.object_id
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM machine_transcript_policy_publish_scope AS scope
                      WHERE scope.revision_id = NEW.object_id
                        AND scope.public_label = NEW.public_label
                        AND scope.basis = NEW.basis
                        AND scope.disclaimer_text = NEW.notes
                  )
              )
          )
          OR (
              reviewer.reviewer_kind = 'automated_policy'
              AND NEW.decision = 'publish'
              AND reviewer.reviewer_id NOT IN (
                  'reviewer_public_metadata_policy_v1',
                  'reviewer_machine_transcript_default_policy_v1'
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'reviewer kind is not authorized for this publication decision');
END;

DROP TRIGGER reviewer_admin_gate_reviewer_scope;

CREATE TRIGGER reviewer_admin_gate_reviewer_scope
BEFORE INSERT ON publication_gate_decisions
WHEN EXISTS (
    SELECT 1
    FROM reviewers AS reviewer
    WHERE reviewer.reviewer_id = NEW.reviewer_id
      AND (
          reviewer.reviewer_kind = 'imported_legacy'
          OR reviewer.reviewer_id IN (
              'reviewer_public_metadata_policy_v1',
              'reviewer_machine_transcript_default_policy_v1'
          )
          OR (reviewer.reviewer_kind = 'automated_policy' AND NEW.decision = 'clear')
      )
)
BEGIN
    SELECT RAISE(ABORT, 'reviewer kind is not authorized for this publication gate decision');
END;

-- This reviewer has publication authority only.  These guards make the prohibition
-- on automated wording review, correction, dispute, retraction, and reinstatement
-- explicit in addition to migration 0026's general human-lifecycle requirement.
CREATE TRIGGER machine_transcript_policy_no_review_decisions
BEFORE INSERT ON review_decisions
WHEN NEW.reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
BEGIN
    SELECT RAISE(ABORT, 'machine transcript publication policy cannot review wording or create disputes');
END;

CREATE TRIGGER machine_transcript_policy_no_corrections
BEFORE INSERT ON corrections
WHEN NEW.reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
BEGIN
    SELECT RAISE(ABORT, 'machine transcript publication policy cannot create corrections');
END;

CREATE TRIGGER machine_transcript_policy_no_lifecycle_decisions
BEFORE INSERT ON transcript_lifecycle_decisions
WHEN NEW.reviewer_id = 'reviewer_machine_transcript_default_policy_v1'
BEGIN
    SELECT RAISE(ABORT, 'machine transcript publication policy cannot dispute, retract, or reinstate transcripts');
END;

-- Migration 0026 made these evidence and human-lifecycle tables append-only with
-- UPDATE/DELETE guards.  SQLite's INSERT OR REPLACE may perform an implicit delete
-- without firing DELETE triggers when recursive_triggers is disabled, so every
-- UNIQUE conflict route also needs an explicit BEFORE INSERT guard.
CREATE TRIGGER transcript_revisions_no_replace
BEFORE INSERT ON transcript_revisions
WHEN EXISTS (
    SELECT 1 FROM transcript_revisions AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.revision_id = NEW.revision_id
)
BEGIN
    SELECT RAISE(ABORT, 'transcript revisions are append-only; replacement is forbidden');
END;

CREATE TRIGGER transcript_segments_no_replace
BEFORE INSERT ON transcript_segments
WHEN EXISTS (
    SELECT 1 FROM transcript_segments AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.segment_id = NEW.segment_id
       OR (existing.revision_id = NEW.revision_id
           AND existing.ordinal = NEW.ordinal)
)
BEGIN
    SELECT RAISE(ABORT, 'transcript segments are append-only; replacement is forbidden');
END;

CREATE TRIGGER transcript_words_no_replace
BEFORE INSERT ON transcript_words
WHEN EXISTS (
    SELECT 1 FROM transcript_words AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.word_id = NEW.word_id
       OR (existing.segment_id = NEW.segment_id
           AND existing.ordinal = NEW.ordinal)
)
BEGIN
    SELECT RAISE(ABORT, 'transcript words are append-only; replacement is forbidden');
END;

CREATE TRIGGER transcript_revision_parents_no_replace
BEFORE INSERT ON transcript_revision_parents
WHEN EXISTS (
    SELECT 1 FROM transcript_revision_parents AS existing
    WHERE existing.rowid = NEW.rowid
       OR (existing.revision_id = NEW.revision_id
           AND existing.parent_revision_id = NEW.parent_revision_id)
)
BEGIN
    SELECT RAISE(ABORT, 'transcript revision parents are append-only; replacement is forbidden');
END;

CREATE TRIGGER review_decisions_no_replace
BEFORE INSERT ON review_decisions
WHEN EXISTS (
    SELECT 1 FROM review_decisions AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.review_decision_id = NEW.review_decision_id
)
BEGIN
    SELECT RAISE(ABORT, 'review decisions are append-only; replacement is forbidden');
END;

CREATE TRIGGER corrections_no_replace
BEFORE INSERT ON corrections
WHEN EXISTS (
    SELECT 1 FROM corrections AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.correction_id = NEW.correction_id
)
BEGIN
    SELECT RAISE(ABORT, 'corrections are append-only; replacement is forbidden');
END;

CREATE TRIGGER transcript_lifecycle_decisions_no_replace
BEFORE INSERT ON transcript_lifecycle_decisions
WHEN EXISTS (
    SELECT 1 FROM transcript_lifecycle_decisions AS existing
    WHERE existing.lifecycle_decision_sequence =
          NEW.lifecycle_decision_sequence
       OR existing.transcript_lifecycle_decision_id =
          NEW.transcript_lifecycle_decision_id
       OR existing.review_decision_id = NEW.review_decision_id
       OR (existing.revision_id = NEW.revision_id
           AND existing.decided_at = NEW.decided_at
           AND existing.lifecycle_state = NEW.lifecycle_state)
)
BEGIN
    SELECT RAISE(ABORT, 'transcript lifecycle decisions are append-only; replacement is forbidden');
END;

-- A policy plan authorizes the exact segment set counted at apply time.  Once its
-- publication row exists, neither an ordinary INSERT nor INSERT OR REPLACE may add
-- or substitute public transcript content.  These BEFORE INSERT guards do not rely
-- on recursive_triggers, and historical policy publication remains sealing even if a
-- later human decision withholds or removes the revision.
CREATE TRIGGER machine_transcript_policy_segments_sealed
BEFORE INSERT ON transcript_segments
WHEN EXISTS (
    SELECT 1
    FROM publication_decisions AS publication
    JOIN machine_transcript_publication_policy_runs AS run
      ON run.manifest_id = publication.manifest_id
     AND run.reviewer_id = publication.reviewer_id
    WHERE publication.object_type = 'transcript_revision'
      AND publication.object_id = NEW.revision_id
      AND publication.decision = 'publish'
      AND publication.reviewer_id =
          'reviewer_machine_transcript_default_policy_v1'
)
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy publication seals its exact segment set');
END;

-- Words are not in the current public release, but they are content-bearing children
-- of the sealed revision and must not become a later unreviewed projection escape.
CREATE TRIGGER machine_transcript_policy_words_sealed
BEFORE INSERT ON transcript_words
WHEN EXISTS (
    SELECT 1
    FROM transcript_segments AS segment
    JOIN publication_decisions AS publication
      ON publication.object_type = 'transcript_revision'
     AND publication.object_id = segment.revision_id
    JOIN machine_transcript_publication_policy_runs AS run
      ON run.manifest_id = publication.manifest_id
     AND run.reviewer_id = publication.reviewer_id
    WHERE segment.segment_id = NEW.segment_id
      AND publication.decision = 'publish'
      AND publication.reviewer_id =
          'reviewer_machine_transcript_default_policy_v1'
)
BEGIN
    SELECT RAISE(ABORT, 'machine transcript policy publication seals transcript words');
END;
