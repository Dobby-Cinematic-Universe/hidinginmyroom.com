CREATE TABLE reviewers (
    reviewer_id TEXT PRIMARY KEY,
    display_label TEXT NOT NULL,
    reviewer_kind TEXT NOT NULL CHECK(reviewer_kind IN ('human', 'automated_policy', 'imported_legacy')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1))
);

CREATE TABLE review_tasks (
    review_task_id TEXT PRIMARY KEY,
    task_kind TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK(status IN ('open', 'in_progress', 'completed', 'deferred', 'cancelled')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(task_kind, target_type, target_id)
);

CREATE TABLE review_decisions (
    review_decision_id TEXT PRIMARY KEY,
    review_task_id TEXT REFERENCES review_tasks(review_task_id),
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id),
    decision TEXT NOT NULL
        CHECK(decision IN ('accept', 'reject', 'correct', 'split', 'merge', 'dispute', 'defer')),
    decided_at TEXT NOT NULL,
    audio_directly_perceived INTEGER NOT NULL DEFAULT 0 CHECK(audio_directly_perceived IN (0, 1)),
    video_directly_perceived INTEGER NOT NULL DEFAULT 0 CHECK(video_directly_perceived IN (0, 1)),
    reviewed_complete_item INTEGER NOT NULL DEFAULT 0 CHECK(reviewed_complete_item IN (0, 1)),
    context_start_ms INTEGER CHECK(context_start_ms IS NULL OR context_start_ms >= 0),
    context_end_ms INTEGER CHECK(context_end_ms IS NULL OR context_end_ms >= 0),
    basis TEXT NOT NULL,
    notes TEXT,
    CHECK(context_end_ms IS NULL OR context_start_ms IS NULL OR context_end_ms >= context_start_ms)
);

CREATE TABLE publication_decisions (
    decision_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_decision_id TEXT NOT NULL UNIQUE,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('publish', 'withhold', 'remove')),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id),
    review_decision_id TEXT REFERENCES review_decisions(review_decision_id),
    decided_at TEXT NOT NULL CHECK(julianday(decided_at) IS NOT NULL),
    basis TEXT NOT NULL,
    public_label TEXT,
    UNIQUE(object_type, object_id, decided_at, decision)
);

CREATE INDEX publication_decisions_object_idx
    ON publication_decisions(object_type, object_id, decision_sequence);

-- Decision streams are append-only. The database-assigned sequence is authoritative
-- for ordering; caller-selected opaque IDs and second-resolution timestamps are not.
CREATE TRIGGER publication_decisions_no_update
BEFORE UPDATE ON publication_decisions
BEGIN
    SELECT RAISE(ABORT, 'publication decisions are append-only');
END;

CREATE TRIGGER publication_decisions_no_delete
BEFORE DELETE ON publication_decisions
BEGIN
    SELECT RAISE(ABORT, 'publication decisions are append-only');
END;

-- A less restrictive decision must carry a genuinely later timestamp than every
-- applicable takedown already in the stream. julianday() compares equivalent UTC
-- instants correctly even when one source timestamp includes fractional seconds.
CREATE TRIGGER publication_decisions_takedown_fail_closed
BEFORE INSERT ON publication_decisions
WHEN EXISTS (
    SELECT 1
    FROM publication_decisions AS existing
    WHERE existing.object_type = NEW.object_type
      AND existing.object_id = NEW.object_id
      AND julianday(NEW.decided_at) <= julianday(existing.decided_at)
      AND CASE existing.decision
              WHEN 'remove' THEN 2
              WHEN 'withhold' THEN 1
              ELSE 0
          END
          > CASE NEW.decision
                WHEN 'remove' THEN 2
                WHEN 'withhold' THEN 1
                ELSE 0
            END
)
BEGIN
    SELECT RAISE(ABORT, 'same-time publication decision may not weaken a takedown');
END;

CREATE TABLE publication_gate_decisions (
    gate_decision_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_gate_decision_id TEXT NOT NULL UNIQUE,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    gate_kind TEXT NOT NULL CHECK(gate_kind IN ('rights', 'privacy', 'sensitivity')),
    decision TEXT NOT NULL CHECK(decision IN ('clear', 'withhold')),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id),
    review_decision_id TEXT REFERENCES review_decisions(review_decision_id),
    decided_at TEXT NOT NULL CHECK(julianday(decided_at) IS NOT NULL),
    basis TEXT NOT NULL,
    UNIQUE(object_type, object_id, gate_kind, decided_at, decision)
);

CREATE INDEX publication_gate_decisions_object_idx
    ON publication_gate_decisions(
        object_type, object_id, gate_kind, gate_decision_sequence
    );

CREATE TRIGGER publication_gate_decisions_no_update
BEFORE UPDATE ON publication_gate_decisions
BEGIN
    SELECT RAISE(ABORT, 'publication gate decisions are append-only');
END;

CREATE TRIGGER publication_gate_decisions_no_delete
BEFORE DELETE ON publication_gate_decisions
BEGIN
    SELECT RAISE(ABORT, 'publication gate decisions are append-only');
END;

CREATE TRIGGER publication_gate_decisions_withhold_fail_closed
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.decision = 'clear'
 AND EXISTS (
    SELECT 1
    FROM publication_gate_decisions AS existing
    WHERE existing.object_type = NEW.object_type
      AND existing.object_id = NEW.object_id
      AND existing.gate_kind = NEW.gate_kind
      AND julianday(NEW.decided_at) <= julianday(existing.decided_at)
      AND existing.decision = 'withhold'
)
BEGIN
    SELECT RAISE(ABORT, 'same-time publication gate may not weaken a withhold');
END;

CREATE TABLE corrections (
    correction_id TEXT PRIMARY KEY,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    replacement_type TEXT,
    replacement_id TEXT,
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id),
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE sensitivity_assessments (
    sensitivity_assessment_id TEXT PRIMARY KEY,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    living_private_person INTEGER NOT NULL DEFAULT 0 CHECK(living_private_person IN (0, 1)),
    personal_data INTEGER NOT NULL DEFAULT 0 CHECK(personal_data IN (0, 1)),
    medical_or_mental_health INTEGER NOT NULL DEFAULT 0 CHECK(medical_or_mental_health IN (0, 1)),
    sexual_content INTEGER NOT NULL DEFAULT 0 CHECK(sexual_content IN (0, 1)),
    wrongdoing_allegation INTEGER NOT NULL DEFAULT 0 CHECK(wrongdoing_allegation IN (0, 1)),
    notes TEXT,
    assessed_by TEXT NOT NULL REFERENCES reviewers(reviewer_id),
    assessed_at TEXT NOT NULL
);

CREATE TABLE claim_catalog_links (
    claim_catalog_link_id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL,
    evidence_index INTEGER NOT NULL CHECK(evidence_index >= 0),
    source_id TEXT REFERENCES sources(source_id),
    recording_id TEXT REFERENCES recordings(recording_id),
    rendition_id TEXT REFERENCES renditions(rendition_id),
    transcript_revision_id TEXT REFERENCES transcript_revisions(revision_id),
    observation_id TEXT REFERENCES observations(observation_id),
    start_ms INTEGER CHECK(start_ms IS NULL OR start_ms >= 0),
    end_ms INTEGER CHECK(end_ms IS NULL OR end_ms >= 0),
    link_state TEXT NOT NULL DEFAULT 'candidate'
        CHECK(link_state IN ('candidate', 'reviewed', 'disputed', 'rejected')),
    basis TEXT NOT NULL,
    CHECK(end_ms IS NULL OR start_ms IS NULL OR end_ms >= start_ms),
    CHECK(source_id IS NOT NULL OR recording_id IS NOT NULL OR rendition_id IS NOT NULL OR transcript_revision_id IS NOT NULL OR observation_id IS NOT NULL),
    UNIQUE(claim_id, evidence_index, source_id, recording_id, transcript_revision_id, observation_id)
);

CREATE TABLE claim_import_issues (
    claim_import_issue_id TEXT PRIMARY KEY,
    claim_id TEXT,
    evidence_index INTEGER,
    legacy_source_record_id TEXT,
    issue_kind TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(details_json)),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open', 'resolved', 'wont_fix')),
    created_at TEXT NOT NULL
);

CREATE VIEW current_publication_decisions AS
SELECT decision.*
FROM publication_decisions AS decision
JOIN (
    SELECT object_type, object_id, MAX(decision_sequence) AS decision_sequence
    FROM publication_decisions
    GROUP BY object_type, object_id
) AS current
  ON current.object_type = decision.object_type
 AND current.object_id = decision.object_id
 AND current.decision_sequence = decision.decision_sequence;

CREATE VIEW current_publication_gate_decisions AS
SELECT gate.*
FROM publication_gate_decisions AS gate
JOIN (
    SELECT object_type, object_id, gate_kind,
           MAX(gate_decision_sequence) AS gate_decision_sequence
    FROM publication_gate_decisions
    GROUP BY object_type, object_id, gate_kind
) AS current
  ON current.object_type = gate.object_type
 AND current.object_id = gate.object_id
 AND current.gate_kind = gate.gate_kind
 AND current.gate_decision_sequence = gate.gate_decision_sequence;

CREATE VIEW publication_eligible_objects AS
SELECT decision.object_type, decision.object_id
FROM current_publication_decisions AS decision
JOIN (
    SELECT object_type, object_id
    FROM current_publication_gate_decisions
    WHERE decision = 'clear'
    GROUP BY object_type, object_id
    HAVING COUNT(DISTINCT gate_kind) = 3
) AS cleared
  ON cleared.object_type = decision.object_type
 AND cleared.object_id = decision.object_id
WHERE decision.decision = 'publish'
;

CREATE VIEW public_sources AS
SELECT source.*
FROM sources AS source
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = 'source'
 AND eligible.object_id = source.source_id
WHERE source.access_state = 'public'
  AND source.review_state <> 'rejected';

CREATE VIEW public_recordings AS
SELECT recording.*
FROM recordings AS recording
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = 'recording'
 AND eligible.object_id = recording.recording_id
WHERE recording.merged_into_recording_id IS NULL
  AND recording.review_state <> 'rejected'
  AND EXISTS (
      SELECT 1
      FROM recording_sources AS link
      JOIN public_sources AS source ON source.source_id = link.source_id
      WHERE link.recording_id = recording.recording_id
        AND link.confidence_state <> 'rejected'
  );

CREATE VIEW public_transcript_revisions AS
SELECT revision.*
FROM transcript_revisions AS revision
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = 'transcript_revision'
 AND eligible.object_id = revision.revision_id
JOIN public_recordings AS recording
  ON recording.recording_id = revision.recording_id
WHERE revision.review_state IN ('human_corrected', 'media_checked');

CREATE VIEW public_transcript_segments AS
SELECT segment.*
FROM transcript_segments AS segment
JOIN public_transcript_revisions AS revision ON revision.revision_id = segment.revision_id;

CREATE VIEW public_entities AS
SELECT entity.*
FROM entities AS entity
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = 'entity'
 AND eligible.object_id = entity.entity_id
WHERE entity.visibility IN ('public_candidate', 'public')
  AND entity.review_state = 'reviewed';

CREATE VIEW public_events AS
SELECT event.*
FROM events AS event
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = 'event'
 AND eligible.object_id = event.event_id
WHERE event.visibility IN ('public_candidate', 'public')
  AND event.review_state = 'reviewed';
