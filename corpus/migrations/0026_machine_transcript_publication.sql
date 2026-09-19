-- Machine transcripts may enter the public corpus without a wording review, but
-- only through the existing explicit publication decision plus rights, privacy,
-- and sensitivity gates.  Retractions are a separate, human-only append-only
-- lifecycle stream; they never rewrite or delete transcript evidence.

CREATE TRIGGER transcript_revisions_no_update
BEFORE UPDATE ON transcript_revisions
BEGIN
    SELECT RAISE(ABORT, 'transcript revisions are append-only');
END;

CREATE TRIGGER transcript_revisions_no_delete
BEFORE DELETE ON transcript_revisions
BEGIN
    SELECT RAISE(ABORT, 'transcript revisions are append-only');
END;

CREATE TRIGGER transcript_segments_no_update
BEFORE UPDATE ON transcript_segments
BEGIN
    SELECT RAISE(ABORT, 'transcript segments are append-only');
END;

CREATE TRIGGER transcript_segments_no_delete
BEFORE DELETE ON transcript_segments
BEGIN
    SELECT RAISE(ABORT, 'transcript segments are append-only');
END;

CREATE TRIGGER transcript_words_no_update
BEFORE UPDATE ON transcript_words
BEGIN
    SELECT RAISE(ABORT, 'transcript words are append-only');
END;

CREATE TRIGGER transcript_words_no_delete
BEFORE DELETE ON transcript_words
BEGIN
    SELECT RAISE(ABORT, 'transcript words are append-only');
END;

CREATE TRIGGER transcript_revision_parents_no_update
BEFORE UPDATE ON transcript_revision_parents
BEGIN
    SELECT RAISE(ABORT, 'transcript revision parents are append-only');
END;

CREATE TRIGGER transcript_revision_parents_no_delete
BEFORE DELETE ON transcript_revision_parents
BEGIN
    SELECT RAISE(ABORT, 'transcript revision parents are append-only');
END;

CREATE TRIGGER review_decisions_no_update
BEFORE UPDATE ON review_decisions
BEGIN
    SELECT RAISE(ABORT, 'review decisions are append-only');
END;

CREATE TRIGGER review_decisions_no_delete
BEFORE DELETE ON review_decisions
BEGIN
    SELECT RAISE(ABORT, 'review decisions are append-only');
END;

CREATE TRIGGER corrections_no_update
BEFORE UPDATE ON corrections
BEGIN
    SELECT RAISE(ABORT, 'corrections are append-only');
END;

CREATE TRIGGER corrections_no_delete
BEFORE DELETE ON corrections
BEGIN
    SELECT RAISE(ABORT, 'corrections are append-only');
END;

CREATE TABLE transcript_lifecycle_decisions (
    lifecycle_decision_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    transcript_lifecycle_decision_id TEXT NOT NULL UNIQUE,
    revision_id TEXT NOT NULL REFERENCES transcript_revisions(revision_id),
    lifecycle_state TEXT NOT NULL
        CHECK(lifecycle_state IN ('retracted', 'disputed', 'reinstated')),
    reason_code TEXT NOT NULL CHECK(reason_code IN (
        'transcription_error', 'speaker_misattribution', 'source_mismatch',
        'privacy', 'rights', 'sensitivity', 'editorial_decision', 'other'
    )),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id),
    review_decision_id TEXT NOT NULL UNIQUE
        REFERENCES review_decisions(review_decision_id),
    decided_at TEXT NOT NULL
        CHECK(julianday(decided_at) IS NOT NULL)
        CHECK(decided_at GLOB '????-??-??T??:??:??Z')
        CHECK(strftime('%Y-%m-%dT%H:%M:%SZ', decided_at) = decided_at),
    basis TEXT NOT NULL CHECK(length(trim(basis)) BETWEEN 1 AND 4096),
    public_explanation TEXT NOT NULL
        CHECK(length(trim(public_explanation)) BETWEEN 1 AND 2048),
    notes TEXT CHECK(notes IS NULL OR length(notes) <= 16384),
    UNIQUE(revision_id, decided_at, lifecycle_state)
);

CREATE INDEX transcript_lifecycle_revision_idx
    ON transcript_lifecycle_decisions(revision_id, lifecycle_decision_sequence);

CREATE TRIGGER transcript_lifecycle_decisions_no_update
BEFORE UPDATE ON transcript_lifecycle_decisions
BEGIN
    SELECT RAISE(ABORT, 'transcript lifecycle decisions are append-only');
END;

CREATE TRIGGER transcript_lifecycle_decisions_no_delete
BEFORE DELETE ON transcript_lifecycle_decisions
BEGIN
    SELECT RAISE(ABORT, 'transcript lifecycle decisions are append-only');
END;

CREATE TRIGGER transcript_lifecycle_decisions_human_review
BEFORE INSERT ON transcript_lifecycle_decisions
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM reviewers AS reviewer
        JOIN review_decisions AS review
          ON review.reviewer_id = reviewer.reviewer_id
        WHERE reviewer.reviewer_id = NEW.reviewer_id
          AND reviewer.reviewer_kind = 'human'
          AND reviewer.active = 1
          AND review.review_decision_id = NEW.review_decision_id
          AND review.target_type = 'transcript_revision'
          AND review.target_id = NEW.revision_id
          AND julianday(review.decided_at) <= julianday(NEW.decided_at)
          AND (
              (NEW.lifecycle_state = 'retracted'
               AND review.decision IN ('reject', 'correct', 'dispute'))
              OR (NEW.lifecycle_state = 'disputed'
                  AND review.decision = 'dispute')
              OR (NEW.lifecycle_state = 'reinstated'
                  AND review.decision IN ('accept', 'correct'))
          )
    ) THEN RAISE(ABORT, 'transcript lifecycle decision requires matching active human review') END;

    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM transcript_lifecycle_decisions AS existing
        WHERE existing.revision_id = NEW.revision_id
          AND julianday(NEW.decided_at) <= julianday(existing.decided_at)
    ) THEN RAISE(ABORT, 'transcript lifecycle decisions must be strictly chronological') END;

    SELECT CASE WHEN NEW.lifecycle_state = 'reinstated' AND NOT EXISTS (
        SELECT 1
        FROM transcript_lifecycle_decisions AS existing
        WHERE existing.revision_id = NEW.revision_id
          AND existing.lifecycle_decision_sequence = (
              SELECT max(latest.lifecycle_decision_sequence)
              FROM transcript_lifecycle_decisions AS latest
              WHERE latest.revision_id = NEW.revision_id
          )
          AND existing.lifecycle_state IN ('retracted', 'disputed')
    ) THEN RAISE(ABORT, 'reinstatement requires a prior retraction or dispute') END;

    SELECT CASE WHEN EXISTS (
        SELECT 1
        FROM transcript_lifecycle_decisions AS existing
        WHERE existing.revision_id = NEW.revision_id
          AND existing.lifecycle_decision_sequence = (
              SELECT max(latest.lifecycle_decision_sequence)
              FROM transcript_lifecycle_decisions AS latest
              WHERE latest.revision_id = NEW.revision_id
          )
          AND existing.lifecycle_state = 'retracted'
          AND NEW.lifecycle_state <> 'reinstated'
    ) THEN RAISE(ABORT, 'a retracted transcript may only be reinstated') END;

    -- `remove` is the durable publication state paired with a retraction.  A
    -- reinstatement must first replace it with either `publish` (public text may
    -- return after this lifecycle insert) or `withhold` (text stays private).
    SELECT CASE WHEN NEW.lifecycle_state = 'reinstated' AND EXISTS (
        SELECT 1
        FROM current_publication_decisions AS publication
        WHERE publication.object_type = 'transcript_revision'
          AND publication.object_id = NEW.revision_id
          AND publication.decision = 'remove'
    ) THEN RAISE(ABORT, 'replace transcript removal before reinstatement') END;

    SELECT CASE WHEN EXISTS (
        SELECT 1
        FROM transcript_lifecycle_decisions AS existing
        WHERE existing.revision_id = NEW.revision_id
          AND existing.lifecycle_decision_sequence = (
              SELECT max(latest.lifecycle_decision_sequence)
              FROM transcript_lifecycle_decisions AS latest
              WHERE latest.revision_id = NEW.revision_id
          )
          AND existing.lifecycle_state = NEW.lifecycle_state
    ) THEN RAISE(ABORT, 'transcript lifecycle state must change') END;
END;

CREATE VIEW current_transcript_lifecycle_decisions AS
SELECT decision.*
FROM transcript_lifecycle_decisions AS decision
JOIN (
    SELECT revision_id, max(lifecycle_decision_sequence) AS decision_sequence
    FROM transcript_lifecycle_decisions
    GROUP BY revision_id
) AS current
  ON current.revision_id = decision.revision_id
 AND current.decision_sequence = decision.lifecycle_decision_sequence;

-- `remove` has a narrow public meaning for transcripts: it records the durable
-- publication half of an already explained human retraction.  Rights, privacy,
-- or sensitivity suppression uses `withhold` or a gate withhold instead.
CREATE TRIGGER transcript_publication_remove_requires_retraction
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type = 'transcript_revision'
 AND NEW.decision = 'remove'
 AND NOT EXISTS (
     SELECT 1
     FROM current_transcript_lifecycle_decisions AS lifecycle
     WHERE lifecycle.revision_id = NEW.object_id
       AND lifecycle.lifecycle_state = 'retracted'
 )
BEGIN
    SELECT RAISE(ABORT, 'transcript removal requires a current human retraction');
END;

DROP VIEW public_transcript_segments;
DROP VIEW public_transcript_revisions;

CREATE VIEW public_transcript_revisions AS
SELECT revision.*,
       CASE
           WHEN revision.review_state = 'machine'
               THEN 1
           WHEN revision.revision_kind IN ('raw_asr', 'contextual_asr')
               THEN 1
           ELSE 0
       END AS machine_generated,
       0 AS verified_quotation,
       CASE
           WHEN lifecycle.lifecycle_state = 'disputed'
               OR revision.review_state = 'disputed'
               THEN 'disputed_transcript_not_verified_quotation_v1'
           WHEN revision.review_state = 'machine'
               THEN 'machine_generated_unreviewed_not_verified_quotation_v1'
           ELSE 'reviewed_transcript_not_fact_checked_v1'
       END AS disclaimer_code,
       COALESCE(lifecycle.lifecycle_state, 'active') AS lifecycle_state
FROM transcript_revisions AS revision
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = 'transcript_revision'
 AND eligible.object_id = revision.revision_id
JOIN public_recordings AS recording
  ON recording.recording_id = revision.recording_id
LEFT JOIN current_transcript_lifecycle_decisions AS lifecycle
  ON lifecycle.revision_id = revision.revision_id
WHERE revision.review_state IN (
        'machine', 'human_corrected', 'media_checked', 'disputed'
      )
  AND COALESCE(lifecycle.lifecycle_state, 'active') <> 'retracted';

CREATE VIEW public_transcript_segments AS
SELECT segment.*
FROM transcript_segments AS segment
JOIN public_transcript_revisions AS revision
  ON revision.revision_id = segment.revision_id;

-- A retracted transcript exposes a text-free tombstone immediately while its
-- publication state is still `publish`, and continues to expose it after the
-- durable `remove` decision.  This prevents an unexplained gap between the human
-- lifecycle decision and its publication update.  A publication `withhold` or a
-- privacy/rights/sensitivity gate withhold suppresses even the explanation.
CREATE VIEW public_transcript_revision_tombstones AS
SELECT revision.revision_id,
       revision.recording_id,
       revision.revision_kind,
       revision.origin,
       revision.language,
       revision.review_state,
       revision.created_at,
       lifecycle.lifecycle_state,
       lifecycle.decided_at,
       lifecycle.reason_code,
       lifecycle.public_explanation,
       CASE
           WHEN revision.review_state = 'machine'
               OR revision.revision_kind IN ('raw_asr', 'contextual_asr')
               THEN 1
           ELSE 0
       END AS machine_generated,
       0 AS verified_quotation,
       'retracted_transcript_text_withdrawn_v1' AS disclaimer_code
FROM transcript_revisions AS revision
JOIN public_recordings AS recording
  ON recording.recording_id = revision.recording_id
JOIN current_transcript_lifecycle_decisions AS lifecycle
  ON lifecycle.revision_id = revision.revision_id
 AND lifecycle.lifecycle_state = 'retracted'
JOIN current_publication_decisions AS publication
  ON publication.object_type = 'transcript_revision'
 AND publication.object_id = revision.revision_id
 AND publication.decision IN ('publish', 'remove')
JOIN (
    SELECT object_type, object_id
    FROM current_publication_gate_decisions
    WHERE decision = 'clear'
    GROUP BY object_type, object_id
    HAVING count(DISTINCT gate_kind) = 3
) AS cleared
  ON cleared.object_type = 'transcript_revision'
 AND cleared.object_id = revision.revision_id
WHERE revision.review_state IN (
        'machine', 'human_corrected', 'media_checked', 'disputed'
      );
