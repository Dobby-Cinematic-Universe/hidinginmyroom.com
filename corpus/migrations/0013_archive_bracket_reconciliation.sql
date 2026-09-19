-- Private, review-only evidence derived from terminal bracketed YouTube IDs in a
-- sealed Archive.org metadata snapshot.  These rows are discovery/reconciliation
-- candidates, never recording merges or publication/identity assertions.

CREATE TABLE archive_bracket_reconciliation_imports (
    import_batch_id TEXT PRIMARY KEY REFERENCES import_batches(import_batch_id),
    snapshot_id TEXT NOT NULL,
    snapshot_sha256 TEXT NOT NULL
        CHECK(length(snapshot_sha256) = 64)
        CHECK(snapshot_sha256 = lower(snapshot_sha256))
        CHECK(snapshot_sha256 NOT GLOB '*[^0-9a-f]*'),
    plan_sha256 TEXT NOT NULL
        CHECK(length(plan_sha256) = 64)
        CHECK(plan_sha256 = lower(plan_sha256))
        CHECK(plan_sha256 NOT GLOB '*[^0-9a-f]*'),
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    candidate_count INTEGER NOT NULL CHECK(candidate_count >= 0),
    issue_count INTEGER NOT NULL CHECK(issue_count >= 0),
    review_task_count INTEGER NOT NULL CHECK(review_task_count >= 0),
    statistics_json TEXT NOT NULL CHECK(json_valid(statistics_json)),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    CHECK(json_type(statistics_json, '$.candidates_total') = 'integer'
          AND json_extract(statistics_json, '$.candidates_total') = candidate_count),
    CHECK(json_type(statistics_json, '$.issues_conflicting_terminal_bracket_ids') = 'integer'
          AND json_extract(statistics_json, '$.issues_conflicting_terminal_bracket_ids') = issue_count),
    CHECK(json_type(statistics_json, '$.review_tasks_total') = 'integer'
          AND json_extract(statistics_json, '$.review_tasks_total') = review_task_count),
    CHECK(json_type(statistics_json, '$.recording_merges') = 'integer'
          AND json_extract(statistics_json, '$.recording_merges') = 0),
    CHECK(json_type(statistics_json, '$.recording_relations') = 'integer'
          AND json_extract(statistics_json, '$.recording_relations') = 0),
    CHECK(json_type(statistics_json, '$.source_or_recording_mutations') = 'integer'
          AND json_extract(statistics_json, '$.source_or_recording_mutations') = 0),
    CHECK(json_type(statistics_json, '$.publication_decisions') = 'integer'
          AND json_extract(statistics_json, '$.publication_decisions') = 0),
    CHECK(json_type(statistics_json, '$.identity_assertions') = 'integer'
          AND json_extract(statistics_json, '$.identity_assertions') = 0),
    CHECK(json_type(statistics_json, '$.claims') = 'integer'
          AND json_extract(statistics_json, '$.claims') = 0),
    UNIQUE(snapshot_id, plan_sha256)
);

CREATE TABLE archive_bracket_youtube_candidates (
    match_candidate_id TEXT PRIMARY KEY
        REFERENCES match_candidates(match_candidate_id) ON DELETE RESTRICT,
    import_batch_id TEXT NOT NULL
        REFERENCES archive_bracket_reconciliation_imports(import_batch_id),
    review_task_id TEXT NOT NULL UNIQUE REFERENCES review_tasks(review_task_id),
    candidate_kind TEXT NOT NULL CHECK(candidate_kind IN (
        'archive_source_to_youtube_locator',
        'archive_recording_to_youtube_recording',
        'intra_archive_repeat'
    )),
    youtube_video_id TEXT NOT NULL
        CHECK(length(youtube_video_id) = 11)
        CHECK(youtube_video_id NOT GLOB '*[^A-Za-z0-9_-]*'),
    archive_source_id TEXT REFERENCES sources(source_id),
    archive_recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    youtube_source_id TEXT REFERENCES sources(source_id),
    youtube_recording_id TEXT REFERENCES recordings(recording_id),
    comparison_archive_recording_id TEXT REFERENCES recordings(recording_id),
    evidence_basis TEXT NOT NULL CHECK(evidence_basis IN (
        'filename_terminal_bracket',
        'title_terminal_bracket',
        'filename_and_title_terminal_bracket',
        'shared_terminal_bracket_id'
    )),
    resolution_state TEXT NOT NULL CHECK(resolution_state IN (
        'unique_native_recording',
        'native_source_without_unique_recording',
        'missing_native_source',
        'intra_archive_repeat'
    )),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    merge_performed INTEGER NOT NULL DEFAULT 0 CHECK(merge_performed = 0),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    CHECK(candidate_kind <> 'archive_recording_to_youtube_recording'
          OR archive_recording_id <> youtube_recording_id),
    CHECK(candidate_kind <> 'intra_archive_repeat'
          OR archive_recording_id <> comparison_archive_recording_id),
    CHECK(
        (resolution_state = 'unique_native_recording'
         AND youtube_source_id IS NOT NULL
         AND youtube_recording_id IS NOT NULL)
        OR
        (resolution_state = 'native_source_without_unique_recording'
         AND youtube_source_id IS NOT NULL
         AND youtube_recording_id IS NULL)
        OR
        (resolution_state = 'missing_native_source'
         AND youtube_source_id IS NULL
         AND youtube_recording_id IS NULL)
        OR
        (resolution_state = 'intra_archive_repeat'
         AND youtube_source_id IS NULL
         AND youtube_recording_id IS NULL)
    ),
    CHECK(
        (candidate_kind = 'archive_source_to_youtube_locator'
         AND archive_source_id IS NOT NULL
         AND comparison_archive_recording_id IS NULL)
        OR
        (candidate_kind = 'archive_recording_to_youtube_recording'
         AND archive_source_id IS NULL
         AND youtube_source_id IS NOT NULL
         AND youtube_recording_id IS NOT NULL
         AND comparison_archive_recording_id IS NULL
         AND resolution_state = 'unique_native_recording')
        OR
        (candidate_kind = 'intra_archive_repeat'
         AND archive_source_id IS NULL
         AND youtube_source_id IS NULL
         AND youtube_recording_id IS NULL
         AND comparison_archive_recording_id IS NOT NULL
         AND resolution_state = 'intra_archive_repeat')
    )
);

CREATE TABLE archive_bracket_reconciliation_issues (
    issue_id TEXT PRIMARY KEY,
    import_batch_id TEXT NOT NULL
        REFERENCES archive_bracket_reconciliation_imports(import_batch_id),
    review_task_id TEXT NOT NULL UNIQUE REFERENCES review_tasks(review_task_id),
    issue_kind TEXT NOT NULL CHECK(issue_kind = 'conflicting_terminal_bracket_ids'),
    archive_source_id TEXT NOT NULL REFERENCES sources(source_id),
    archive_recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    filename_youtube_video_id TEXT NOT NULL
        CHECK(length(filename_youtube_video_id) = 11)
        CHECK(filename_youtube_video_id NOT GLOB '*[^A-Za-z0-9_-]*'),
    title_youtube_video_id TEXT NOT NULL
        CHECK(length(title_youtube_video_id) = 11)
        CHECK(title_youtube_video_id NOT GLOB '*[^A-Za-z0-9_-]*'),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    merge_performed INTEGER NOT NULL DEFAULT 0 CHECK(merge_performed = 0),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    CHECK(filename_youtube_video_id <> title_youtube_video_id)
);

CREATE INDEX archive_bracket_candidates_video_idx
    ON archive_bracket_youtube_candidates(youtube_video_id, candidate_kind);
CREATE INDEX archive_bracket_candidates_archive_source_idx
    ON archive_bracket_youtube_candidates(archive_source_id);
CREATE INDEX archive_bracket_issues_archive_source_idx
    ON archive_bracket_reconciliation_issues(archive_source_id);

-- Raw reconciliation evidence and receipts are append-only.  Review progress is
-- stored on the associated review task/decision, not by editing these rows.
CREATE TRIGGER archive_bracket_reconciliation_imports_no_update
BEFORE UPDATE ON archive_bracket_reconciliation_imports
BEGIN
    SELECT RAISE(ABORT, 'archive bracket reconciliation imports are append-only');
END;

CREATE TRIGGER archive_bracket_reconciliation_imports_no_delete
BEFORE DELETE ON archive_bracket_reconciliation_imports
BEGIN
    SELECT RAISE(ABORT, 'archive bracket reconciliation imports are append-only');
END;

CREATE TRIGGER archive_bracket_youtube_candidates_no_update
BEFORE UPDATE ON archive_bracket_youtube_candidates
BEGIN
    SELECT RAISE(ABORT, 'archive bracket candidates are append-only');
END;

CREATE TRIGGER archive_bracket_youtube_candidates_no_delete
BEFORE DELETE ON archive_bracket_youtube_candidates
BEGIN
    SELECT RAISE(ABORT, 'archive bracket candidates are append-only');
END;

CREATE TRIGGER archive_bracket_reconciliation_issues_no_update
BEFORE UPDATE ON archive_bracket_reconciliation_issues
BEGIN
    SELECT RAISE(ABORT, 'archive bracket issues are append-only');
END;

CREATE TRIGGER archive_bracket_reconciliation_issues_no_delete
BEFORE DELETE ON archive_bracket_reconciliation_issues
BEGIN
    SELECT RAISE(ABORT, 'archive bracket issues are append-only');
END;

-- The generic candidate projection for this lane is immutable too.  It can only be
-- adjudicated by a separate review decision and can never be rewritten into a merge.
CREATE TRIGGER archive_bracket_match_candidates_no_update
BEFORE UPDATE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM archive_bracket_youtube_candidates AS candidate
    WHERE candidate.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive bracket match candidates are append-only');
END;

CREATE TRIGGER archive_bracket_match_candidates_no_delete
BEFORE DELETE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM archive_bracket_youtube_candidates AS candidate
    WHERE candidate.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive bracket match candidates are append-only');
END;

-- Fail closed if a caller tries to wrap an unrelated generic row or if catalog
-- dependencies do not agree with the sealed candidate description.
CREATE TRIGGER archive_bracket_youtube_candidates_admission
BEFORE INSERT ON archive_bracket_youtube_candidates
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM match_candidates AS match
        JOIN review_tasks AS task ON task.review_task_id = NEW.review_task_id
        WHERE match.match_candidate_id = NEW.match_candidate_id
          AND match.raw_score IS NULL
          AND match.calibrated_probability IS NULL
          AND match.decision_state = 'candidate'
          AND json_extract(match.metadata_json, '$.youtube_video_id') = NEW.youtube_video_id
          AND json_type(match.metadata_json, '$.requires_human_review') = 'true'
          AND json_type(match.metadata_json, '$.relationship_asserted') = 'false'
          AND json_type(match.metadata_json, '$.merge_performed') = 'false'
          AND json_type(match.metadata_json, '$.publication_authority') = 'false'
          AND json_type(NEW.evidence_json, '$.requires_human_review') = 'true'
          AND json_type(NEW.evidence_json, '$.relationship_asserted') = 'false'
          AND json_type(NEW.evidence_json, '$.merge_performed') = 'false'
          AND task.task_kind = 'archive_bracket_reconciliation_candidate'
          AND task.target_type = 'match_candidate'
          AND task.target_id = NEW.match_candidate_id
          AND (
            (NEW.candidate_kind = 'archive_source_to_youtube_locator'
             AND EXISTS (
                 SELECT 1 FROM sources AS archive_source
                 WHERE archive_source.source_id = NEW.archive_source_id
                   AND archive_source.platform = 'internet_archive'
                   AND archive_source.source_kind = 'archive_media_file'
             )
             AND EXISTS (
                 SELECT 1 FROM recording_sources AS link
                 WHERE link.source_id = NEW.archive_source_id
                   AND link.recording_id = NEW.archive_recording_id
                   AND link.mapping_role IN ('archive_original_file', 'archive_derivative_file')
             )
             AND match.left_object_type = 'source'
             AND match.left_object_id = NEW.archive_source_id
             AND match.match_method = 'archive_bracketed_youtube_locator_v1'
             AND (
                 (NEW.youtube_source_id IS NULL
                  AND match.right_object_type = 'youtube_video_id'
                  AND match.right_object_id = NEW.youtube_video_id)
                 OR
                 (NEW.youtube_source_id IS NOT NULL
                  AND match.right_object_type = 'source'
                  AND match.right_object_id = NEW.youtube_source_id)
             ))
            OR
            (NEW.candidate_kind = 'archive_recording_to_youtube_recording'
             AND match.left_object_type = 'recording'
             AND match.left_object_id = NEW.archive_recording_id
             AND match.right_object_type = 'recording'
             AND match.right_object_id = NEW.youtube_recording_id
             AND match.match_method = 'archive_bracketed_native_recording_v1')
            OR
            (NEW.candidate_kind = 'intra_archive_repeat'
             AND match.left_object_type = 'recording'
             AND match.left_object_id = NEW.archive_recording_id
             AND match.right_object_type = 'recording'
             AND match.right_object_id = NEW.comparison_archive_recording_id
             AND match.match_method = 'archive_bracketed_intra_archive_repeat_v1')
          )
    ) THEN RAISE(ABORT, 'invalid archive bracket reconciliation candidate') END;

    SELECT CASE WHEN NEW.youtube_source_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM sources AS source
        WHERE source.source_id = NEW.youtube_source_id
          AND source.platform = 'youtube'
          AND source.source_kind = 'youtube_video'
          AND source.native_id = NEW.youtube_video_id
    ) THEN RAISE(ABORT, 'archive bracket candidate YouTube source differs') END;

    SELECT CASE WHEN NEW.youtube_recording_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM recordings AS recording
        JOIN recording_sources AS link
          ON link.recording_id = recording.recording_id
        WHERE recording.recording_id = NEW.youtube_recording_id
          AND recording.canonical_key = 'youtube:video:' || NEW.youtube_video_id
          AND link.source_id = NEW.youtube_source_id
    ) THEN RAISE(ABORT, 'archive bracket candidate YouTube recording differs') END;
END;

CREATE TRIGGER archive_bracket_reconciliation_issues_admission
BEFORE INSERT ON archive_bracket_reconciliation_issues
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM review_tasks AS task
        JOIN sources AS source ON source.source_id = NEW.archive_source_id
        JOIN recording_sources AS link
          ON link.source_id = NEW.archive_source_id
         AND link.recording_id = NEW.archive_recording_id
        WHERE task.review_task_id = NEW.review_task_id
          AND task.task_kind = 'archive_bracket_reconciliation_conflict'
          AND task.target_type = 'archive_bracket_reconciliation_issue'
          AND task.target_id = NEW.issue_id
          AND source.platform = 'internet_archive'
          AND source.source_kind = 'archive_media_file'
          AND link.mapping_role IN ('archive_original_file', 'archive_derivative_file')
          AND json_type(NEW.evidence_json, '$.requires_human_review') = 'true'
          AND json_type(NEW.evidence_json, '$.relationship_asserted') = 'false'
          AND json_type(NEW.evidence_json, '$.merge_performed') = 'false'
    ) THEN RAISE(ABORT, 'invalid archive bracket reconciliation issue') END;
END;
