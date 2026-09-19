-- Reviewer identities grant authority to review and publication workflows. New
-- identities start inactive and every state transition is represented by an
-- immutable event before it changes the current row. Rows already present when
-- this migration runs are explicitly adopted; no unlogged post-migration INSERT
-- compatibility path remains open.

-- Current-publication views do not implement scheduling. Refuse to upgrade a
-- catalog that already contains a future decision instead of silently preserving
-- authority that would take effect before its claimed decision time.
CREATE TABLE migration_0027_no_future_decision_guard (
    must_be_zero INTEGER NOT NULL CHECK(must_be_zero = 0)
);
INSERT INTO migration_0027_no_future_decision_guard(must_be_zero)
SELECT 1
FROM publication_decisions
WHERE julianday(decided_at) > julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
LIMIT 1;
INSERT INTO migration_0027_no_future_decision_guard(must_be_zero)
SELECT 1
FROM publication_gate_decisions
WHERE julianday(decided_at) > julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
LIMIT 1;
DROP TABLE migration_0027_no_future_decision_guard;

CREATE TABLE reviewer_admin_manifest_imports (
    manifest_id TEXT PRIMARY KEY,
    input_sha256 TEXT NOT NULL UNIQUE CHECK(length(input_sha256) = 64),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    manifest_created_at TEXT NOT NULL CHECK(julianday(manifest_created_at) IS NOT NULL),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    authorized_by TEXT NOT NULL,
    basis TEXT NOT NULL,
    registration_count INTEGER NOT NULL CHECK(registration_count >= 0),
    state_change_count INTEGER NOT NULL CHECK(state_change_count >= 0),
    adoption_count INTEGER NOT NULL CHECK(adoption_count >= 0),
    CHECK(registration_count + state_change_count + adoption_count > 0)
);

CREATE TABLE reviewer_admin_events (
    event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    reviewer_admin_event_id TEXT NOT NULL UNIQUE,
    manifest_id TEXT NOT NULL
        REFERENCES reviewer_admin_manifest_imports(manifest_id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    event_kind TEXT NOT NULL
        CHECK(event_kind IN ('register', 'set_active', 'legacy_adopt')),
    reviewer_id TEXT NOT NULL
        REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    display_label TEXT NOT NULL,
    reviewer_kind TEXT NOT NULL
        CHECK(reviewer_kind IN ('human', 'automated_policy', 'imported_legacy')),
    previous_active INTEGER CHECK(previous_active IS NULL OR previous_active IN (0, 1)),
    new_active INTEGER NOT NULL CHECK(new_active IN (0, 1)),
    effective_at TEXT NOT NULL CHECK(julianday(effective_at) IS NOT NULL),
    basis TEXT NOT NULL,
    UNIQUE(manifest_id, ordinal),
    CHECK(
        (event_kind = 'register' AND previous_active IS NULL AND new_active = 0)
        OR
        (event_kind = 'legacy_adopt' AND previous_active IS NULL)
        OR
        (event_kind = 'set_active' AND previous_active IS NOT NULL
         AND previous_active <> new_active)
    )
);

-- Preserve rows created under migrations 0005--0026 through one explicit,
-- deterministic migration-only adoption ledger. This runs before the guards below
-- are installed. It does not create or activate any reviewer.
INSERT INTO reviewer_admin_manifest_imports(
    manifest_id, input_sha256, schema_version, manifest_created_at, imported_at,
    authorized_by, basis, registration_count, state_change_count, adoption_count
)
SELECT
    'reviewer-admin-migration-0027-legacy-adoption',
    '0000000000000000000000000000000000000000000000000000000000000027',
    1,
    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
    'schema_migration_0027',
    'Snapshot of reviewer rows that predate governed reviewer administration.',
    0,
    0,
    (SELECT COUNT(*) FROM reviewers)
WHERE EXISTS (SELECT 1 FROM reviewers);

INSERT INTO reviewer_admin_events(
    reviewer_admin_event_id, manifest_id, ordinal, event_kind, reviewer_id,
    display_label, reviewer_kind, previous_active, new_active, effective_at, basis
)
SELECT
    'legacy-adopt:' || reviewer.reviewer_id,
    'reviewer-admin-migration-0027-legacy-adoption',
    ROW_NUMBER() OVER (ORDER BY reviewer.reviewer_id) - 1,
    'legacy_adopt',
    reviewer.reviewer_id,
    reviewer.display_label,
    reviewer.reviewer_kind,
    NULL,
    reviewer.active,
    (
        SELECT manifest.imported_at
        FROM reviewer_admin_manifest_imports AS manifest
        WHERE manifest.manifest_id =
              'reviewer-admin-migration-0027-legacy-adoption'
    ),
    'Exact migration-time snapshot of a preexisting reviewer row.'
FROM reviewers AS reviewer
ORDER BY reviewer.reviewer_id;

CREATE UNIQUE INDEX reviewer_admin_one_enrollment_per_reviewer
    ON reviewer_admin_events(reviewer_id)
    WHERE event_kind IN ('register', 'legacy_adopt');

CREATE INDEX reviewer_admin_events_reviewer_stream
    ON reviewer_admin_events(reviewer_id, event_sequence);

-- BEFORE INSERT conflict guards make append-only semantics independent of
-- connection-local recursive_triggers behavior. They reject every UNIQUE target
-- that INSERT OR REPLACE could otherwise satisfy by implicitly deleting old rows.
CREATE TRIGGER reviewer_admin_manifest_imports_no_replace
BEFORE INSERT ON reviewer_admin_manifest_imports
WHEN EXISTS (
    SELECT 1
    FROM reviewer_admin_manifest_imports AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.manifest_id = NEW.manifest_id
       OR existing.input_sha256 = NEW.input_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin manifest imports are append-only; replacement is forbidden');
END;

CREATE TRIGGER reviewer_admin_manifest_time_is_not_future
BEFORE INSERT ON reviewer_admin_manifest_imports
WHEN julianday(NEW.imported_at) > julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
 OR julianday(NEW.manifest_created_at) > julianday(NEW.imported_at)
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin manifest time must not be in the future');
END;

CREATE TRIGGER reviewer_admin_events_no_replace
BEFORE INSERT ON reviewer_admin_events
WHEN EXISTS (
    SELECT 1
    FROM reviewer_admin_events AS existing
    WHERE existing.event_sequence = NEW.event_sequence
       OR existing.reviewer_admin_event_id = NEW.reviewer_admin_event_id
       OR (existing.manifest_id = NEW.manifest_id AND existing.ordinal = NEW.ordinal)
       OR (
           NEW.event_kind IN ('register', 'legacy_adopt')
           AND existing.reviewer_id = NEW.reviewer_id
           AND existing.event_kind IN ('register', 'legacy_adopt')
       )
)
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin events are append-only; replacement is forbidden');
END;

CREATE TRIGGER reviewer_admin_event_time_is_not_future
BEFORE INSERT ON reviewer_admin_events
WHEN NOT EXISTS (
    SELECT 1
    FROM reviewer_admin_manifest_imports AS manifest
    WHERE manifest.manifest_id = NEW.manifest_id
      AND julianday(NEW.effective_at) <= julianday(manifest.manifest_created_at)
      AND julianday(NEW.effective_at) <= julianday(manifest.imported_at)
      AND julianday(NEW.effective_at) <=
          julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
)
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin event time must not be in the future');
END;

CREATE TRIGGER reviewer_admin_manifest_imports_no_update
BEFORE UPDATE ON reviewer_admin_manifest_imports
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin manifest imports are append-only');
END;

CREATE TRIGGER reviewer_admin_manifest_imports_no_delete
BEFORE DELETE ON reviewer_admin_manifest_imports
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin manifest imports are append-only');
END;

CREATE TRIGGER reviewer_admin_events_no_update
BEFORE UPDATE ON reviewer_admin_events
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin events are append-only');
END;

CREATE TRIGGER reviewer_admin_events_no_delete
BEFORE DELETE ON reviewer_admin_events
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin events are append-only');
END;

CREATE TRIGGER reviewer_admin_legacy_adoption_is_migration_only
BEFORE INSERT ON reviewer_admin_events
WHEN NEW.event_kind = 'legacy_adopt'
BEGIN
    SELECT RAISE(ABORT, 'legacy reviewer adoption is migration-only');
END;

-- Registration is event-first. Its deferred reviewer FK is satisfied only when
-- the exact inactive row is inserted in the same successful transaction.
CREATE TRIGGER reviewer_admin_registration_is_first
BEFORE INSERT ON reviewer_admin_events
WHEN NEW.event_kind = 'register'
 AND (
    EXISTS (SELECT 1 FROM reviewers WHERE reviewer_id = NEW.reviewer_id)
    OR EXISTS (
        SELECT 1 FROM reviewer_admin_events
        WHERE reviewer_id = NEW.reviewer_id
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'reviewer registration must be the first event for a new reviewer');
END;

CREATE TRIGGER reviewer_admin_reviewer_insert_requires_registration
BEFORE INSERT ON reviewers
WHEN EXISTS (
    SELECT 1 FROM reviewers WHERE reviewer_id = NEW.reviewer_id
)
 OR EXISTS (
    SELECT 1 FROM reviewers WHERE rowid = NEW.rowid
 )
 OR NOT EXISTS (
    SELECT 1
    FROM reviewer_admin_events AS event
    WHERE event.reviewer_id = NEW.reviewer_id
      AND event.event_kind = 'register'
      AND event.display_label = NEW.display_label
      AND event.reviewer_kind = NEW.reviewer_kind
      AND event.previous_active IS NULL
      AND event.new_active = 0
      AND NEW.active = 0
      AND event.event_sequence = (
          SELECT MAX(latest.event_sequence)
          FROM reviewer_admin_events AS latest
          WHERE latest.reviewer_id = NEW.reviewer_id
      )
 )
BEGIN
    SELECT RAISE(ABORT, 'reviewer insert requires an exact prior registration event');
END;

-- State changes snapshot the identity and exact prior state. The AFTER trigger
-- applies only the event it has just admitted, keeping audit and current state in
-- the same SQLite statement and transaction.
CREATE TRIGGER reviewer_admin_state_snapshot_matches
BEFORE INSERT ON reviewer_admin_events
WHEN NEW.event_kind = 'set_active'
 AND NOT EXISTS (
    SELECT 1
    FROM reviewers AS reviewer
    WHERE reviewer.reviewer_id = NEW.reviewer_id
      AND reviewer.display_label = NEW.display_label
      AND reviewer.reviewer_kind = NEW.reviewer_kind
      AND reviewer.active = NEW.previous_active
 )
BEGIN
    SELECT RAISE(ABORT, 'reviewer state event does not match current reviewer state');
END;

CREATE TRIGGER reviewer_admin_event_stream_is_strictly_chronological
BEFORE INSERT ON reviewer_admin_events
WHEN EXISTS (
    SELECT 1
    FROM reviewer_admin_events AS existing
    WHERE existing.reviewer_id = NEW.reviewer_id
      AND julianday(existing.effective_at) >= julianday(NEW.effective_at)
 )
BEGIN
    SELECT RAISE(ABORT, 'reviewer admin event must be later than its existing stream');
END;

CREATE TRIGGER reviewer_admin_apply_state_event
AFTER INSERT ON reviewer_admin_events
WHEN NEW.event_kind = 'set_active'
BEGIN
    UPDATE reviewers
    SET active = NEW.new_active
    WHERE reviewer_id = NEW.reviewer_id;
END;

-- Every row is governed after the migration-time adoption above. There is no
-- conditional compatibility carve-out for post-migration direct inserts.
CREATE TRIGGER reviewer_admin_identity_no_update
BEFORE UPDATE OF reviewer_id, display_label, reviewer_kind ON reviewers
BEGIN
    SELECT RAISE(ABORT, 'reviewer identity is immutable');
END;

CREATE TRIGGER reviewer_admin_active_requires_event
BEFORE UPDATE OF active ON reviewers
WHEN OLD.active <> NEW.active
 AND NOT EXISTS (
    SELECT 1
    FROM reviewer_admin_events AS event
    WHERE event.event_sequence = (
        SELECT MAX(latest.event_sequence)
        FROM reviewer_admin_events AS latest
        WHERE latest.reviewer_id = OLD.reviewer_id
    )
      AND event.event_kind = 'set_active'
      AND event.previous_active = OLD.active
      AND event.new_active = NEW.active
 )
BEGIN
    SELECT RAISE(ABORT, 'reviewer active state requires a new admin event');
END;

CREATE TRIGGER reviewer_admin_reviewer_no_delete
BEFORE DELETE ON reviewers
BEGIN
    SELECT RAISE(ABORT, 'reviewers cannot be deleted');
END;

-- This view is the single database-level capability granted to the built-in
-- metadata policy. Publication administration, the importer, and the insertion
-- trigger all consult the same rows, labels, and bases so dry-run and apply cannot
-- disagree about that policy's authority.
CREATE VIEW public_metadata_policy_publish_scope AS
SELECT DISTINCT
    'source' AS object_type,
    source.source_id AS object_id,
    'source metadata' AS public_label,
    'Public URL, creator/platform title label, access label, and stable native ID only; no legacy transcript content' AS basis
FROM sources AS source
JOIN sources AS parent
  ON parent.source_id = source.parent_source_id
JOIN source_metadata_observations AS policy_metadata
  ON policy_metadata.source_id = source.source_id
JOIN import_batches AS origin
  ON origin.import_batch_id = policy_metadata.import_batch_id
WHERE source.review_state = 'metadata_only'
  AND source.access_state = 'public'
  AND (
      (source.platform = 'internet_archive'
       AND source.source_kind = 'archive_media_file'
       AND parent.platform = 'internet_archive'
       AND parent.source_kind = 'archive_item'
       AND parent.native_id IN ('28766', '69999', '699992'))
      OR
      (source.platform = 'youtube'
       AND source.source_kind = 'youtube_video'
       AND parent.platform = 'youtube'
       AND parent.source_kind = 'channel'
       AND parent.native_id = 'UC_yIF-9jOge6nNA0z-ScrBQ'
       AND origin.importer_name = 'current_youtube_channel_inventory')
  )
UNION
SELECT DISTINCT
    'recording' AS object_type,
    recording.recording_id AS object_id,
    'metadata only' AS public_label,
    'Recording identity assembled from public Archive.org or YouTube metadata; contents remain unreviewed' AS basis
FROM recordings AS recording
JOIN recording_sources AS link
  ON link.recording_id = recording.recording_id
JOIN sources AS source
  ON source.source_id = link.source_id
JOIN sources AS parent
  ON parent.source_id = source.parent_source_id
JOIN source_metadata_observations AS policy_metadata
  ON policy_metadata.source_id = source.source_id
JOIN import_batches AS origin
  ON origin.import_batch_id = policy_metadata.import_batch_id
WHERE source.review_state = 'metadata_only'
  AND source.access_state = 'public'
  AND (
      (source.platform = 'internet_archive'
       AND source.source_kind = 'archive_media_file'
       AND parent.platform = 'internet_archive'
       AND parent.source_kind = 'archive_item'
       AND parent.native_id IN ('28766', '69999', '699992'))
      OR
      (source.platform = 'youtube'
       AND source.source_kind = 'youtube_video'
       AND parent.platform = 'youtube'
       AND parent.source_kind = 'channel'
       AND parent.native_id = 'UC_yIF-9jOge6nNA0z-ScrBQ'
       AND origin.importer_name = 'current_youtube_channel_inventory')
  );

-- Publication insertions must be authorized both now and at their claimed decision
-- instant. This protects direct SQL as well as the manifest administrator.
CREATE TRIGGER reviewer_admin_publication_id_no_replace
BEFORE INSERT ON publication_decisions
WHEN EXISTS (
    SELECT 1
    FROM publication_decisions AS existing
    WHERE existing.decision_sequence = NEW.decision_sequence
       OR existing.publication_decision_id = NEW.publication_decision_id
       OR (
           existing.object_type = NEW.object_type
           AND existing.object_id = NEW.object_id
           AND existing.decided_at = NEW.decided_at
           AND existing.decision = NEW.decision
       )
)
BEGIN
    SELECT RAISE(ABORT, 'publication decisions are append-only; replacement is forbidden');
END;

CREATE TRIGGER reviewer_admin_gate_id_no_replace
BEFORE INSERT ON publication_gate_decisions
WHEN EXISTS (
    SELECT 1
    FROM publication_gate_decisions AS existing
    WHERE existing.gate_decision_sequence = NEW.gate_decision_sequence
       OR existing.publication_gate_decision_id = NEW.publication_gate_decision_id
       OR (
           existing.object_type = NEW.object_type
           AND existing.object_id = NEW.object_id
           AND existing.gate_kind = NEW.gate_kind
           AND existing.decided_at = NEW.decided_at
           AND existing.decision = NEW.decision
       )
)
BEGIN
    SELECT RAISE(ABORT, 'publication gate decisions are append-only; replacement is forbidden');
END;

CREATE TRIGGER reviewer_admin_publication_manifest_no_replace
BEFORE INSERT ON publication_manifest_imports
WHEN EXISTS (
    SELECT 1
    FROM publication_manifest_imports AS existing
    WHERE existing.rowid = NEW.rowid
       OR existing.manifest_id = NEW.manifest_id
       OR existing.input_sha256 = NEW.input_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'publication manifest imports are append-only; replacement is forbidden');
END;

-- Publication views take the newest row immediately, so future timestamps are not
-- schedules: without these guards they would grant present authority while claiming
-- a decision that has not happened yet.
CREATE TRIGGER reviewer_admin_publication_decision_time_is_not_future
BEFORE INSERT ON publication_decisions
WHEN julianday(NEW.decided_at) >
     julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
BEGIN
    SELECT RAISE(ABORT, 'publication decision time must not be in the future');
END;

CREATE TRIGGER reviewer_admin_gate_decision_time_is_not_future
BEFORE INSERT ON publication_gate_decisions
WHEN julianday(NEW.decided_at) >
     julianday(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
BEGIN
    SELECT RAISE(ABORT, 'publication gate decision time must not be in the future');
END;

CREATE TRIGGER reviewer_admin_publication_reviewer_must_be_active
BEFORE INSERT ON publication_decisions
WHEN (
 NOT EXISTS (
    SELECT 1
    FROM reviewers AS reviewer
    WHERE reviewer.reviewer_id = NEW.reviewer_id
      AND reviewer.active = 1
 )
 OR NOT EXISTS (
    SELECT 1
    FROM reviewer_admin_events AS event
    WHERE event.reviewer_id = NEW.reviewer_id
      AND julianday(event.effective_at) <= julianday(NEW.decided_at)
      AND event.new_active = 1
      AND NOT EXISTS (
          SELECT 1
          FROM reviewer_admin_events AS later
          WHERE later.reviewer_id = event.reviewer_id
            AND julianday(later.effective_at) <= julianday(NEW.decided_at)
            AND later.event_sequence > event.event_sequence
      )
 )
)
BEGIN
    SELECT RAISE(ABORT, 'publication reviewer was not active at decision time');
END;

CREATE TRIGGER reviewer_admin_gate_reviewer_must_be_active
BEFORE INSERT ON publication_gate_decisions
WHEN (
 NOT EXISTS (
    SELECT 1
    FROM reviewers AS reviewer
    WHERE reviewer.reviewer_id = NEW.reviewer_id
      AND reviewer.active = 1
 )
 OR NOT EXISTS (
    SELECT 1
    FROM reviewer_admin_events AS event
    WHERE event.reviewer_id = NEW.reviewer_id
      AND julianday(event.effective_at) <= julianday(NEW.decided_at)
      AND event.new_active = 1
      AND NOT EXISTS (
          SELECT 1
          FROM reviewer_admin_events AS later
          WHERE later.reviewer_id = event.reviewer_id
            AND julianday(later.effective_at) <= julianday(NEW.decided_at)
            AND later.event_sequence > event.event_sequence
      )
 )
)
BEGIN
    SELECT RAISE(ABORT, 'publication gate reviewer was not active at decision time');
END;

-- Reviewer kind is an authority boundary, not merely a display label. Imported
-- legacy identities cannot create new decisions. Automated policies may restrict;
-- only the exact built-in metadata policy may publish, and only inside its closed
-- source/recording allowlist. Automated policies never clear publication gates.
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
              reviewer.reviewer_kind = 'automated_policy'
              AND NEW.decision = 'publish'
              AND NOT (
                  reviewer.reviewer_id = 'reviewer_public_metadata_policy_v1'
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
      )
)
BEGIN
    SELECT RAISE(ABORT, 'reviewer kind is not authorized for this publication decision');
END;

CREATE TRIGGER reviewer_admin_gate_reviewer_scope
BEFORE INSERT ON publication_gate_decisions
WHEN EXISTS (
    SELECT 1
    FROM reviewers AS reviewer
    WHERE reviewer.reviewer_id = NEW.reviewer_id
      AND (
          reviewer.reviewer_kind = 'imported_legacy'
          OR reviewer.reviewer_id = 'reviewer_public_metadata_policy_v1'
          OR (reviewer.reviewer_kind = 'automated_policy' AND NEW.decision = 'clear')
      )
)
BEGIN
    SELECT RAISE(ABORT, 'reviewer kind is not authorized for this publication gate decision');
END;

CREATE VIEW current_reviewer_admin_events AS
SELECT event.*
FROM reviewer_admin_events AS event
JOIN (
    SELECT reviewer_id, MAX(event_sequence) AS event_sequence
    FROM reviewer_admin_events
    GROUP BY reviewer_id
) AS current
  ON current.reviewer_id = event.reviewer_id
 AND current.event_sequence = event.event_sequence;
