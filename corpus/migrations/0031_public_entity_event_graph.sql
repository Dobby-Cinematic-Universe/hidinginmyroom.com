-- Publication-safe entity/event graph projection.
--
-- Migration 0004 stores a broad private working graph and migration 0005 exposes
-- only gated entity/event roots.  This additive layer deliberately does not widen
-- either contract.  Every graph object and edge needs its own complete human review,
-- explicit public label, publish decision, and current human rights/privacy/
-- sensitivity clearances before it can enter these narrow views.

-- event_participants predates stable per-edge IDs.  Bind the importer's deterministic
-- review target to the composite row without rewriting that historical table.
CREATE TABLE event_participant_publication_subjects (
    event_participant_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    participant_role TEXT NOT NULL,
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    FOREIGN KEY(event_id, entity_id, participant_role)
        REFERENCES event_participants(event_id, entity_id, participant_role)
        ON DELETE RESTRICT,
    UNIQUE(event_id, entity_id, participant_role)
);

CREATE TRIGGER event_participant_publication_subjects_no_update
BEFORE UPDATE ON event_participant_publication_subjects
BEGIN
    SELECT RAISE(ABORT, 'event participant publication subjects are append-only');
END;

CREATE TRIGGER event_participant_publication_subjects_no_delete
BEFORE DELETE ON event_participant_publication_subjects
BEGIN
    SELECT RAISE(ABORT, 'event participant publication subjects are append-only');
END;

CREATE TRIGGER mapped_event_participants_no_update
BEFORE UPDATE ON event_participants
WHEN EXISTS (
    SELECT 1
    FROM event_participant_publication_subjects AS subject
    WHERE subject.event_id = OLD.event_id
      AND subject.entity_id = OLD.entity_id
      AND subject.participant_role = OLD.participant_role
)
BEGIN
    SELECT RAISE(ABORT, 'mapped event participants are immutable');
END;

CREATE TRIGGER mapped_event_participants_no_delete
BEFORE DELETE ON event_participants
WHEN EXISTS (
    SELECT 1
    FROM event_participant_publication_subjects AS subject
    WHERE subject.event_id = OLD.event_id
      AND subject.entity_id = OLD.entity_id
      AND subject.participant_role = OLD.participant_role
)
BEGIN
    SELECT RAISE(ABORT, 'mapped event participants are immutable');
END;

-- A permissive graph decision written outside the administrator still needs an
-- active human and an exact, complete acceptance/correction review.  Edge labels in
-- the private map are not public labels; publication must supply one explicitly.
CREATE TRIGGER graph_publication_decisions_human_review
BEFORE INSERT ON publication_decisions
-- Entity/event publication streams predate this migration and may serve a separate
-- public-identity workflow.  Do not retroactively narrow that write contract; the
-- human_public_graph_objects view below independently requires reviewed roots.
WHEN NEW.object_type IN (
        'appearance', 'event_date', 'event_participant', 'event_relation',
        'event_evidence'
     )
 AND NEW.decision = 'publish'
 AND (
        NEW.public_label IS NULL
        OR length(trim(NEW.public_label)) = 0
        OR NEW.review_decision_id IS NULL
        OR NOT EXISTS (
            SELECT 1
            FROM review_decisions AS review
            JOIN reviewers AS reviewer
              ON reviewer.reviewer_id = review.reviewer_id
             AND reviewer.reviewer_kind = 'human'
             AND reviewer.active = 1
            WHERE review.review_decision_id = NEW.review_decision_id
              AND review.target_type = NEW.object_type
              AND review.target_id = NEW.object_id
              AND review.reviewer_id = NEW.reviewer_id
              AND review.decision IN ('accept', 'correct')
              AND review.reviewed_complete_item = 1
              AND julianday(review.decided_at) <= julianday(NEW.decided_at)
              AND (
                    NEW.object_type NOT IN ('appearance', 'event_evidence')
                    OR review.audio_directly_perceived = 1
                    OR review.video_directly_perceived = 1
              )
        )
     )
BEGIN
    SELECT RAISE(ABORT, 'graph publication requires an explicit label and complete human review');
END;

-- Gate clearances are separate human acts.  A graph gate cannot inherit an
-- automated metadata policy, an imported legacy reviewer, or an unrelated review.
CREATE TRIGGER graph_publication_gate_decisions_human_review
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type IN (
        'appearance', 'event_date', 'event_participant', 'event_relation',
        'event_evidence'
     )
 AND NEW.decision = 'clear'
 AND (
        NEW.review_decision_id IS NULL
        OR NOT EXISTS (
            SELECT 1
            FROM review_decisions AS review
            JOIN reviewers AS reviewer
              ON reviewer.reviewer_id = review.reviewer_id
             AND reviewer.reviewer_kind = 'human'
             AND reviewer.active = 1
            WHERE review.review_decision_id = NEW.review_decision_id
              AND review.target_type = NEW.object_type
              AND review.target_id = NEW.object_id
              AND review.reviewer_id = NEW.reviewer_id
              AND review.decision IN ('accept', 'correct')
              AND review.reviewed_complete_item = 1
              AND julianday(review.decided_at) <= julianday(NEW.decided_at)
        )
     )
BEGIN
    SELECT RAISE(ABORT, 'graph gate clearance requires a complete human review');
END;

-- This view is intentionally stricter than publication_eligible_objects.  It
-- requires the publication review plus each current gate to be attributable to an
-- active human and a matching complete review decision.  The ordinary publication
-- view remains unchanged for recording/transcript releases.
CREATE VIEW human_public_graph_objects AS
SELECT publication.object_type,
       publication.object_id,
       publication.public_label,
       publication.decided_at AS publication_decided_at,
       publication.review_decision_id,
       publication_review.audio_directly_perceived,
       publication_review.video_directly_perceived
FROM current_publication_decisions AS publication
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = publication.object_type
 AND eligible.object_id = publication.object_id
JOIN reviewers AS publication_reviewer
  ON publication_reviewer.reviewer_id = publication.reviewer_id
 AND publication_reviewer.reviewer_kind = 'human'
 AND publication_reviewer.active = 1
JOIN review_decisions AS publication_review
  ON publication_review.review_decision_id = publication.review_decision_id
 AND publication_review.target_type = publication.object_type
 AND publication_review.target_id = publication.object_id
 AND publication_review.reviewer_id = publication.reviewer_id
 AND publication_review.decision IN ('accept', 'correct')
 AND publication_review.reviewed_complete_item = 1
WHERE publication.object_type IN (
        'entity', 'event', 'appearance', 'event_date', 'event_participant',
        'event_relation', 'event_evidence'
      )
  AND publication.decision = 'publish'
  AND publication.public_label IS NOT NULL
  AND length(trim(publication.public_label)) > 0
  AND julianday(publication_review.decided_at) <= julianday(publication.decided_at)
  AND (
        publication.object_type NOT IN ('appearance', 'event_evidence')
        OR publication_review.audio_directly_perceived = 1
        OR publication_review.video_directly_perceived = 1
      )
  AND 3 = (
      SELECT count(DISTINCT gate.gate_kind)
      FROM current_publication_gate_decisions AS gate
      JOIN reviewers AS gate_reviewer
        ON gate_reviewer.reviewer_id = gate.reviewer_id
       AND gate_reviewer.reviewer_kind = 'human'
       AND gate_reviewer.active = 1
      JOIN review_decisions AS gate_review
        ON gate_review.review_decision_id = gate.review_decision_id
       AND gate_review.target_type = gate.object_type
       AND gate_review.target_id = gate.object_id
       AND gate_review.reviewer_id = gate.reviewer_id
       AND gate_review.decision IN ('accept', 'correct')
       AND gate_review.reviewed_complete_item = 1
      WHERE gate.object_type = publication.object_type
        AND gate.object_id = publication.object_id
        AND gate.decision = 'clear'
        AND gate.gate_kind IN ('rights', 'privacy', 'sensitivity')
        AND julianday(gate_review.decided_at) <= julianday(gate.decided_at)
  );

CREATE VIEW public_graph_entities AS
SELECT entity.entity_id,
       entity.slug,
       eligible.public_label,
       entity.entity_type
FROM public_entities AS entity
JOIN human_public_graph_objects AS eligible
  ON eligible.object_type = 'entity'
 AND eligible.object_id = entity.entity_id;

CREATE VIEW public_graph_events AS
SELECT event.event_id,
       event.slug,
       eligible.public_label
FROM public_events AS event
JOIN human_public_graph_objects AS eligible
  ON eligible.object_type = 'event'
 AND eligible.object_id = event.event_id;

-- Exactly one reviewed entity/event-map anchor must resolve to public, reviewed
-- source/recording/rendition state.  Private paths, artifacts, observations,
-- transcript text, aliases, metadata, and basis strings are not columns here.
CREATE VIEW reviewed_public_graph_catalog_anchors AS
SELECT link.claim_id,
       link.source_id,
       source.platform AS source_platform,
       source.canonical_url AS source_url,
       source.native_id AS source_native_id,
       link.recording_id,
       recording.slug AS recording_slug,
       recording.title AS recording_title,
       link.rendition_id,
       media.duration_ms AS rendition_duration_ms,
       link.transcript_revision_id,
       link.start_ms,
       link.end_ms
FROM claim_catalog_links AS link
JOIN public_sources AS source
  ON source.source_id = link.source_id
 AND source.review_state = 'reviewed'
 AND source.canonical_url IS NOT NULL
JOIN public_recordings AS recording
  ON recording.recording_id = link.recording_id
 AND recording.review_state = 'reviewed'
JOIN recording_sources AS recording_source
  ON recording_source.recording_id = recording.recording_id
 AND recording_source.source_id = source.source_id
 AND recording_source.confidence_state = 'reviewed'
JOIN renditions AS rendition
  ON rendition.rendition_id = link.rendition_id
 AND rendition.recording_id = recording.recording_id
 AND rendition.review_state = 'reviewed'
JOIN media_objects AS media
  ON media.media_id = rendition.media_id
 AND media.integrity_state = 'verified'
WHERE link.evidence_index = 0
  AND link.link_state = 'reviewed'
  AND link.source_id IS NOT NULL
  AND link.recording_id IS NOT NULL
  AND link.rendition_id IS NOT NULL
  AND (
        (link.start_ms IS NULL AND link.end_ms IS NULL)
        OR (
            link.start_ms IS NOT NULL
            AND link.end_ms IS NOT NULL
            AND link.end_ms > link.start_ms
            AND media.duration_ms IS NOT NULL
            AND link.end_ms <= media.duration_ms
        )
      )
  AND NOT EXISTS (
      SELECT 1
      FROM claim_catalog_links AS other
      WHERE other.claim_id = link.claim_id
        AND other.evidence_index = link.evidence_index
        AND other.claim_catalog_link_id <> link.claim_catalog_link_id
  );

CREATE VIEW public_graph_appearances AS
SELECT appearance.appearance_id,
       appearance.entity_id,
       eligible.public_label,
       appearance.start_ms,
       appearance.end_ms,
       anchor.recording_id,
       anchor.recording_slug,
       anchor.recording_title,
       anchor.rendition_id,
       anchor.source_id,
       anchor.source_platform,
       anchor.source_url,
       anchor.source_native_id
FROM appearances AS appearance
JOIN public_graph_entities AS entity
  ON entity.entity_id = appearance.entity_id
JOIN human_public_graph_objects AS eligible
  ON eligible.object_type = 'appearance'
 AND eligible.object_id = appearance.appearance_id
JOIN reviewed_public_graph_catalog_anchors AS anchor
  ON anchor.claim_id = 'entity_event_map:appearance:' || appearance.appearance_id
 AND anchor.recording_id = appearance.recording_id
 AND anchor.start_ms IS appearance.start_ms
 AND anchor.end_ms IS appearance.end_ms
WHERE appearance.review_state = 'reviewed';

CREATE VIEW public_graph_event_dates AS
SELECT date.event_date_id,
       date.event_id,
       eligible.public_label,
       date.value_start,
       date.value_end,
       date.precision,
       date.certainty
FROM event_dates AS date
JOIN public_graph_events AS event ON event.event_id = date.event_id
JOIN human_public_graph_objects AS eligible
  ON eligible.object_type = 'event_date'
 AND eligible.object_id = date.event_date_id;

CREATE VIEW public_graph_event_participants AS
SELECT subject.event_participant_id,
       participant.event_id,
       participant.entity_id,
       eligible.public_label
FROM event_participant_publication_subjects AS subject
JOIN event_participants AS participant
  ON participant.event_id = subject.event_id
 AND participant.entity_id = subject.entity_id
 AND participant.participant_role = subject.participant_role
JOIN public_graph_events AS event ON event.event_id = participant.event_id
JOIN public_graph_entities AS entity ON entity.entity_id = participant.entity_id
JOIN human_public_graph_objects AS eligible
  ON eligible.object_type = 'event_participant'
 AND eligible.object_id = subject.event_participant_id
WHERE participant.review_state = 'reviewed';

CREATE VIEW public_graph_event_relations AS
SELECT relation.event_relation_id,
       relation.from_event_id,
       relation.to_event_id,
       eligible.public_label
FROM event_relations AS relation
JOIN public_graph_events AS from_event
  ON from_event.event_id = relation.from_event_id
JOIN public_graph_events AS to_event
  ON to_event.event_id = relation.to_event_id
JOIN human_public_graph_objects AS eligible
  ON eligible.object_type = 'event_relation'
 AND eligible.object_id = relation.event_relation_id;

CREATE VIEW public_graph_event_evidence AS
SELECT evidence.event_evidence_id,
       evidence.event_id,
       eligible.public_label,
       evidence.support_kind,
       evidence.start_ms,
       evidence.end_ms,
       anchor.recording_id,
       anchor.recording_slug,
       anchor.recording_title,
       anchor.rendition_id,
       anchor.source_id,
       anchor.source_platform,
       anchor.source_url,
       anchor.source_native_id
FROM event_evidence AS evidence
JOIN public_graph_events AS event ON event.event_id = evidence.event_id
JOIN human_public_graph_objects AS eligible
  ON eligible.object_type = 'event_evidence'
 AND eligible.object_id = evidence.event_evidence_id
JOIN reviewed_public_graph_catalog_anchors AS anchor
  ON anchor.claim_id =
     'entity_event_map:event_evidence:' || evidence.event_evidence_id
 AND anchor.recording_id = evidence.recording_id
 AND anchor.source_id = evidence.source_id
 AND anchor.transcript_revision_id IS evidence.transcript_revision_id
 AND anchor.start_ms IS evidence.start_ms
 AND anchor.end_ms IS evidence.end_ms
-- V1 graph evidence is media/source anchored only.  Transcript-backed edges stay
-- private until a later graph schema can pin and account for transcript lifecycle
-- dependencies without exporting machine-only wording or identity labels.
WHERE evidence.transcript_revision_id IS NULL;
