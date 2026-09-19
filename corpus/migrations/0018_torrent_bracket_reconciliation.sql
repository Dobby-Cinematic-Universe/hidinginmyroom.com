-- Private, candidate-only locator evidence derived from exact torrent-manifest
-- paths.  No row in this lane is a relationship, merge, identity assertion, or
-- publication decision.

CREATE TABLE torrent_bracket_reconciliation_imports (
    import_batch_id TEXT PRIMARY KEY REFERENCES import_batches(import_batch_id),
    torrent_import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    torrent_manifest_source_id TEXT NOT NULL REFERENCES sources(source_id),
    info_hash_sha1 TEXT NOT NULL
        CHECK(length(info_hash_sha1) = 40)
        CHECK(info_hash_sha1 = lower(info_hash_sha1))
        CHECK(info_hash_sha1 NOT GLOB '*[^0-9a-f]*'),
    torrent_sha256 TEXT NOT NULL
        CHECK(length(torrent_sha256) = 64)
        CHECK(torrent_sha256 = lower(torrent_sha256))
        CHECK(torrent_sha256 NOT GLOB '*[^0-9a-f]*'),
    discovery_sha256 TEXT NOT NULL
        CHECK(length(discovery_sha256) = 64)
        CHECK(discovery_sha256 = lower(discovery_sha256))
        CHECK(discovery_sha256 NOT GLOB '*[^0-9a-f]*'),
    combined_import_input_sha256 TEXT NOT NULL
        CHECK(length(combined_import_input_sha256) = 64)
        CHECK(combined_import_input_sha256 = lower(combined_import_input_sha256))
        CHECK(combined_import_input_sha256 NOT GLOB '*[^0-9a-f]*'),
    plan_sha256 TEXT NOT NULL
        CHECK(length(plan_sha256) = 64)
        CHECK(plan_sha256 = lower(plan_sha256))
        CHECK(plan_sha256 NOT GLOB '*[^0-9a-f]*'),
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    scoped_directory_labels_json TEXT NOT NULL
        CHECK(json_valid(scoped_directory_labels_json))
        CHECK(json_type(scoped_directory_labels_json) = 'array')
        CHECK(json_array_length(scoped_directory_labels_json) = 4)
        CHECK(json_extract(scoped_directory_labels_json, '$[0]') = 'YouTube Videos')
        CHECK(json_extract(scoped_directory_labels_json, '$[1]') = 'Old YouTube Livestreams')
        CHECK(json_extract(scoped_directory_labels_json, '$[2]') = 'New YouTube Livestreams')
        CHECK(json_extract(scoped_directory_labels_json, '$[3]') = 'New New YouTube Livestreams'),
    scoped_file_count INTEGER NOT NULL CHECK(scoped_file_count >= 0),
    candidate_count INTEGER NOT NULL CHECK(candidate_count >= 0),
    review_task_count INTEGER NOT NULL CHECK(review_task_count = candidate_count),
    statistics_json TEXT NOT NULL CHECK(json_valid(statistics_json)),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    CHECK(json_type(statistics_json, '$.scoped_file_records') = 'integer'
          AND json_extract(statistics_json, '$.scoped_file_records') = scoped_file_count),
    CHECK(json_type(statistics_json, '$.terminal_bracket_candidates') = 'integer'
          AND json_extract(statistics_json, '$.terminal_bracket_candidates') = candidate_count),
    CHECK(json_type(statistics_json, '$.review_tasks_total') = 'integer'
          AND json_extract(statistics_json, '$.review_tasks_total') = review_task_count),
    CHECK(json_type(statistics_json, '$.payload_files_read') = 'integer'
          AND json_extract(statistics_json, '$.payload_files_read') = 0),
    CHECK(json_type(statistics_json, '$.payload_bytes_read') = 'integer'
          AND json_extract(statistics_json, '$.payload_bytes_read') = 0),
    CHECK(json_type(statistics_json, '$.source_or_recording_mutations') = 'integer'
          AND json_extract(statistics_json, '$.source_or_recording_mutations') = 0),
    CHECK(json_type(statistics_json, '$.source_relations') = 'integer'
          AND json_extract(statistics_json, '$.source_relations') = 0),
    CHECK(json_type(statistics_json, '$.recording_relations') = 'integer'
          AND json_extract(statistics_json, '$.recording_relations') = 0),
    CHECK(json_type(statistics_json, '$.recording_merges') = 'integer'
          AND json_extract(statistics_json, '$.recording_merges') = 0),
    CHECK(json_type(statistics_json, '$.publication_decisions') = 'integer'
          AND json_extract(statistics_json, '$.publication_decisions') = 0),
    CHECK(json_type(statistics_json, '$.identity_assertions') = 'integer'
          AND json_extract(statistics_json, '$.identity_assertions') = 0),
    CHECK(json_type(statistics_json, '$.claims') = 'integer'
          AND json_extract(statistics_json, '$.claims') = 0),
    UNIQUE(torrent_sha256, discovery_sha256, plan_sha256)
);

CREATE TABLE torrent_bracket_youtube_candidates (
    match_candidate_id TEXT PRIMARY KEY
        REFERENCES match_candidates(match_candidate_id) ON DELETE RESTRICT,
    import_batch_id TEXT NOT NULL
        REFERENCES torrent_bracket_reconciliation_imports(import_batch_id),
    review_task_id TEXT NOT NULL UNIQUE REFERENCES review_tasks(review_task_id),
    candidate_kind TEXT NOT NULL CHECK(candidate_kind = 'torrent_file_to_youtube_locator'),
    torrent_manifest_source_id TEXT NOT NULL REFERENCES sources(source_id),
    torrent_file_source_id TEXT NOT NULL REFERENCES sources(source_id),
    torrent_file_index INTEGER NOT NULL CHECK(torrent_file_index >= 0),
    directory_label TEXT NOT NULL CHECK(directory_label IN (
        'YouTube Videos',
        'Old YouTube Livestreams',
        'New YouTube Livestreams',
        'New New YouTube Livestreams'
    )),
    manifest_path TEXT NOT NULL CHECK(length(manifest_path) BETWEEN 1 AND 16384),
    manifest_path_components_base64_json TEXT NOT NULL
        CHECK(json_valid(manifest_path_components_base64_json))
        CHECK(json_type(manifest_path_components_base64_json) = 'array')
        CHECK(json_array_length(manifest_path_components_base64_json) BETWEEN 1 AND 32),
    manifest_path_sha256 TEXT NOT NULL
        CHECK(length(manifest_path_sha256) = 64)
        CHECK(manifest_path_sha256 = lower(manifest_path_sha256))
        CHECK(manifest_path_sha256 NOT GLOB '*[^0-9a-f]*'),
    byte_count INTEGER NOT NULL CHECK(byte_count >= 0),
    youtube_video_id TEXT NOT NULL
        CHECK(length(youtube_video_id) = 11)
        CHECK(youtube_video_id NOT GLOB '*[^A-Za-z0-9_-]*'),
    youtube_source_id TEXT REFERENCES sources(source_id),
    youtube_recording_id TEXT REFERENCES recordings(recording_id),
    evidence_basis TEXT NOT NULL
        CHECK(evidence_basis = 'terminal_filename_bracket_before_video_extension'),
    resolution_state TEXT NOT NULL CHECK(resolution_state IN (
        'missing_native_source',
        'native_source_without_unique_recording',
        'unique_native_recording'
    )),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    merge_performed INTEGER NOT NULL DEFAULT 0 CHECK(merge_performed = 0),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL DEFAULT 'none' CHECK(publication_authority = 'none'),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    CHECK(
        (resolution_state = 'missing_native_source'
         AND youtube_source_id IS NULL
         AND youtube_recording_id IS NULL)
        OR
        (resolution_state = 'native_source_without_unique_recording'
         AND youtube_source_id IS NOT NULL
         AND youtube_recording_id IS NULL)
        OR
        (resolution_state = 'unique_native_recording'
         AND youtube_source_id IS NOT NULL
         AND youtube_recording_id IS NOT NULL)
    ),
    UNIQUE(import_batch_id, torrent_file_source_id, youtube_video_id)
);

CREATE INDEX torrent_bracket_candidates_video_idx
    ON torrent_bracket_youtube_candidates(youtube_video_id, directory_label);
CREATE INDEX torrent_bracket_candidates_file_idx
    ON torrent_bracket_youtube_candidates(torrent_file_source_id);

CREATE TRIGGER torrent_bracket_reconciliation_imports_admission
BEFORE INSERT ON torrent_bracket_reconciliation_imports
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM import_batches AS candidate_import
        JOIN import_batches AS torrent_import
          ON torrent_import.import_batch_id = NEW.torrent_import_batch_id
        JOIN sources AS manifest
          ON manifest.source_id = NEW.torrent_manifest_source_id
        JOIN source_metadata_observations AS origin
          ON origin.source_id = manifest.source_id
         AND origin.import_batch_id = torrent_import.import_batch_id
        WHERE candidate_import.import_batch_id = NEW.import_batch_id
          AND candidate_import.importer_name = 'torrent_bracket_reconciliation_v1'
          AND candidate_import.input_sha256 = NEW.plan_sha256
          AND candidate_import.status = 'running'
          AND candidate_import.started_at = NEW.observed_at
          AND candidate_import.source_snapshot_date = substr(NEW.observed_at, 1, 10)
          AND torrent_import.importer_name = 'torrent_manifest_metadata'
          AND torrent_import.input_sha256 = NEW.combined_import_input_sha256
          AND torrent_import.status = 'completed'
          AND torrent_import.started_at = NEW.observed_at
          AND manifest.platform = 'bittorrent'
          AND manifest.source_kind = 'torrent_manifest'
          AND manifest.native_id = NEW.info_hash_sha1
          AND manifest.created_by_import_batch_id = torrent_import.import_batch_id
          AND origin.observed_at = NEW.observed_at
          AND origin.quality_rank = 250
          AND json_extract(origin.metadata_json, '$.info_hash_sha1') = NEW.info_hash_sha1
          AND json_extract(origin.metadata_json, '$.torrent_sha256') = NEW.torrent_sha256
          AND json_type(origin.metadata_json, '$.payload_downloaded') = 'false'
    ) THEN RAISE(ABORT, 'invalid torrent bracket reconciliation receipt') END;
END;

CREATE TRIGGER torrent_bracket_reconciliation_imports_no_update
BEFORE UPDATE ON torrent_bracket_reconciliation_imports
BEGIN
    SELECT RAISE(ABORT, 'torrent bracket reconciliation imports are append-only');
END;

CREATE TRIGGER torrent_bracket_reconciliation_imports_no_delete
BEFORE DELETE ON torrent_bracket_reconciliation_imports
BEGIN
    SELECT RAISE(ABORT, 'torrent bracket reconciliation imports are append-only');
END;

CREATE TRIGGER torrent_bracket_youtube_candidates_no_update
BEFORE UPDATE ON torrent_bracket_youtube_candidates
BEGIN
    SELECT RAISE(ABORT, 'torrent bracket candidates are append-only');
END;

CREATE TRIGGER torrent_bracket_youtube_candidates_no_delete
BEFORE DELETE ON torrent_bracket_youtube_candidates
BEGIN
    SELECT RAISE(ABORT, 'torrent bracket candidates are append-only');
END;

CREATE TRIGGER torrent_bracket_match_candidates_no_update
BEFORE UPDATE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM torrent_bracket_youtube_candidates AS candidate
    WHERE candidate.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'torrent bracket match candidates are append-only');
END;

CREATE TRIGGER torrent_bracket_match_candidates_no_delete
BEFORE DELETE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM torrent_bracket_youtube_candidates AS candidate
    WHERE candidate.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'torrent bracket match candidates are append-only');
END;

-- The historical source projection may receive later metadata observations, but
-- the identity columns to which this exact raw-path evidence is bound cannot be
-- rewritten after admission.
CREATE TRIGGER torrent_bracket_source_identity_no_update
BEFORE UPDATE OF platform, source_kind, native_id, parent_source_id,
                 created_by_import_batch_id ON sources
WHEN EXISTS (
    SELECT 1
    FROM torrent_bracket_reconciliation_imports AS receipt
    WHERE receipt.torrent_manifest_source_id = OLD.source_id
)
OR EXISTS (
    SELECT 1
    FROM torrent_bracket_youtube_candidates AS candidate
    WHERE candidate.torrent_manifest_source_id = OLD.source_id
       OR candidate.torrent_file_source_id = OLD.source_id
)
BEGIN
    SELECT CASE WHEN NEW.platform IS NOT OLD.platform
        OR NEW.source_kind IS NOT OLD.source_kind
        OR NEW.native_id IS NOT OLD.native_id
        OR NEW.parent_source_id IS NOT OLD.parent_source_id
        OR NEW.created_by_import_batch_id IS NOT OLD.created_by_import_batch_id
    THEN RAISE(ABORT, 'torrent bracket source identity is immutable') END;
END;

-- The subtype row may wrap only one canonical, unscored, private review candidate
-- whose origin is the already-imported torrent file observation.
CREATE TRIGGER torrent_bracket_youtube_candidates_admission
BEFORE INSERT ON torrent_bracket_youtube_candidates
BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM publication_decisions AS publication
        WHERE publication.object_type = 'match_candidate'
          AND publication.object_id = NEW.match_candidate_id
    ) THEN RAISE(ABORT, 'torrent bracket candidate already has publication state') END;

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM match_candidates AS match
        JOIN review_tasks AS task ON task.review_task_id = NEW.review_task_id
        JOIN torrent_bracket_reconciliation_imports AS receipt
          ON receipt.import_batch_id = NEW.import_batch_id
        JOIN import_batches AS candidate_import
          ON candidate_import.import_batch_id = receipt.import_batch_id
        JOIN import_batches AS torrent_import
          ON torrent_import.import_batch_id = receipt.torrent_import_batch_id
        JOIN sources AS manifest
          ON manifest.source_id = NEW.torrent_manifest_source_id
        JOIN sources AS file_source
          ON file_source.source_id = NEW.torrent_file_source_id
        JOIN source_metadata_observations AS origin
          ON origin.source_id = NEW.torrent_file_source_id
         AND origin.import_batch_id = receipt.torrent_import_batch_id
        WHERE match.match_candidate_id = NEW.match_candidate_id
          AND match.left_object_type = 'source'
          AND match.left_object_id = NEW.torrent_file_source_id
          AND match.match_method = 'torrent_terminal_bracket_youtube_locator_v1'
          AND match.raw_score IS NULL
          AND match.calibrated_probability IS NULL
          AND match.decision_state = 'candidate'
          AND json_type(match.metadata_json) = 'object'
          AND (SELECT count(*) FROM json_each(match.metadata_json)) = 10
          AND json_extract(match.metadata_json, '$.schema_version') = 1
          AND json_extract(match.metadata_json, '$.candidate_kind') = NEW.candidate_kind
          AND json_extract(match.metadata_json, '$.evidence_basis') = NEW.evidence_basis
          AND json_extract(match.metadata_json, '$.youtube_video_id') = NEW.youtube_video_id
          AND json_extract(match.metadata_json, '$.resolution_state') = NEW.resolution_state
          AND json_extract(match.metadata_json, '$.calibration_state') = 'not_calibrated'
          AND json_type(match.metadata_json, '$.requires_human_review') = 'true'
          AND json_type(match.metadata_json, '$.relationship_asserted') = 'false'
          AND json_type(match.metadata_json, '$.merge_performed') = 'false'
          AND json_type(match.metadata_json, '$.publication_authority') = 'false'
          AND task.task_kind = 'torrent_bracket_reconciliation_candidate'
          AND task.target_type = 'match_candidate'
          AND task.target_id = NEW.match_candidate_id
          AND candidate_import.importer_name = 'torrent_bracket_reconciliation_v1'
          AND candidate_import.input_sha256 = receipt.plan_sha256
          AND candidate_import.status = 'running'
          AND receipt.torrent_manifest_source_id = NEW.torrent_manifest_source_id
          AND torrent_import.importer_name = 'torrent_manifest_metadata'
          AND torrent_import.status = 'completed'
          AND manifest.platform = 'bittorrent'
          AND manifest.source_kind = 'torrent_manifest'
          AND manifest.native_id = receipt.info_hash_sha1
          AND file_source.platform = 'bittorrent'
          AND file_source.source_kind = 'torrent_file_candidate'
          AND file_source.parent_source_id = manifest.source_id
          AND file_source.created_by_import_batch_id = torrent_import.import_batch_id
          AND file_source.native_id = receipt.info_hash_sha1 || '/' || NEW.manifest_path
          AND substr(NEW.manifest_path, 1, length(NEW.directory_label) + 1)
              = NEW.directory_label || '/'
          AND (
              (lower(substr(NEW.manifest_path, -4)) IN (
                   '.mp4', '.ogv', '.mkv', '.mov', '.m4v'
               )
               AND substr(NEW.manifest_path, -17, 13)
                   = '[' || NEW.youtube_video_id || ']')
              OR
              (lower(substr(NEW.manifest_path, -5)) = '.webm'
               AND substr(NEW.manifest_path, -18, 13)
                   = '[' || NEW.youtube_video_id || ']')
          )
          AND origin.parent_source_id = manifest.source_id
          AND origin.observed_at = receipt.observed_at
          AND origin.quality_rank = 250
          AND origin.quality_basis = 'torrent_manifest_metadata: locally parsed discovery manifest'
          AND origin.access_state = 'unknown'
          AND origin.review_state = 'unreviewed'
          AND json_extract(origin.metadata_json, '$.manifest_path') = NEW.manifest_path
          AND json_extract(origin.metadata_json, '$.byte_count') = NEW.byte_count
          AND json_type(origin.metadata_json, '$.payload_downloaded') = 'false'
          AND json_extract(origin.metadata_json, '$.discovery_state') = 'private_manifest_candidate'
          AND json_type(NEW.evidence_json) = 'object'
          AND (SELECT count(*) FROM json_each(NEW.evidence_json)) = 22
          AND json_type(NEW.evidence_json, '$.requires_human_review') = 'true'
          AND json_type(NEW.evidence_json, '$.relationship_asserted') = 'false'
          AND json_type(NEW.evidence_json, '$.merge_performed') = 'false'
          AND json_type(NEW.evidence_json, '$.publication_authority') = 'false'
          AND json_type(NEW.evidence_json, '$.payload_downloaded_or_read') = 'false'
          AND json_extract(NEW.evidence_json, '$.torrent_file_index') = NEW.torrent_file_index
          AND json_extract(NEW.evidence_json, '$.torrent_manifest_source_id') = NEW.torrent_manifest_source_id
          AND json_extract(NEW.evidence_json, '$.torrent_file_source_id') = NEW.torrent_file_source_id
          AND json_extract(NEW.evidence_json, '$.directory_label') = NEW.directory_label
          AND json_extract(NEW.evidence_json, '$.manifest_path') = NEW.manifest_path
          AND json_extract(NEW.evidence_json, '$.manifest_path_sha256') = NEW.manifest_path_sha256
          AND json_extract(NEW.evidence_json, '$.byte_count') = NEW.byte_count
          AND json_extract(NEW.evidence_json, '$.youtube_video_id') = NEW.youtube_video_id
          AND json_extract(NEW.evidence_json, '$.evidence_basis') = NEW.evidence_basis
          AND json_extract(NEW.evidence_json, '$.resolution_state') = NEW.resolution_state
          AND json_extract(NEW.evidence_json, '$.youtube_source_id') IS NEW.youtube_source_id
          AND json_extract(NEW.evidence_json, '$.youtube_recording_id') IS NEW.youtube_recording_id
          AND json_extract(NEW.evidence_json, '$.torrent_sha256') = receipt.torrent_sha256
          AND json_extract(NEW.evidence_json, '$.discovery_sha256') = receipt.discovery_sha256
          AND json_extract(NEW.evidence_json, '$.info_hash_sha1') = receipt.info_hash_sha1
          AND json(NEW.manifest_path_components_base64_json)
              = json(json_extract(NEW.evidence_json, '$.manifest_path_components_base64'))
          AND (
              (NEW.youtube_source_id IS NULL
               AND match.right_object_type = 'youtube_video_id'
               AND match.right_object_id = NEW.youtube_video_id)
              OR
              (NEW.youtube_source_id IS NOT NULL
               AND match.right_object_type = 'source'
               AND match.right_object_id = NEW.youtube_source_id)
          )
    ) THEN RAISE(ABORT, 'invalid torrent bracket reconciliation candidate') END;

    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM json_each(NEW.manifest_path_components_base64_json) AS component
        WHERE component.type <> 'text'
           OR length(component.value) NOT BETWEEN 1 AND 5464
           OR component.value GLOB '*[^A-Za-z0-9+/=]*'
           OR length(component.value) % 4 <> 0
           OR length(component.value) - length(rtrim(component.value, '=')) > 2
           OR instr(rtrim(component.value, '='), '=') > 0
    ) THEN RAISE(ABORT, 'torrent bracket raw path evidence is invalid') END;

    SELECT CASE WHEN json_type(NEW.evidence_json, '$.mapped_recording_ids') <> 'array'
        OR EXISTS (
            SELECT 1 FROM json_each(NEW.evidence_json, '$.mapped_recording_ids') AS mapped
            WHERE mapped.type <> 'text'
        )
        OR (SELECT count(*) FROM json_each(NEW.evidence_json, '$.mapped_recording_ids'))
           <> (SELECT count(DISTINCT value)
               FROM json_each(NEW.evidence_json, '$.mapped_recording_ids'))
    THEN RAISE(ABORT, 'torrent bracket mapped recording evidence is invalid') END;

    SELECT CASE WHEN NEW.youtube_source_id IS NULL AND (
        json_array_length(json_extract(NEW.evidence_json, '$.mapped_recording_ids')) <> 0
        OR EXISTS (
            SELECT 1 FROM sources AS source
            WHERE source.platform = 'youtube'
              AND source.source_kind = 'youtube_video'
              AND source.native_id = NEW.youtube_video_id
        )
    ) THEN RAISE(ABORT, 'torrent bracket missing-source resolution differs') END;

    SELECT CASE WHEN NEW.youtube_source_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM sources AS source
        WHERE source.source_id = NEW.youtube_source_id
          AND source.platform = 'youtube'
          AND source.source_kind = 'youtube_video'
          AND source.native_id = NEW.youtube_video_id
    ) THEN RAISE(ABORT, 'torrent bracket YouTube source differs') END;

    SELECT CASE WHEN NEW.youtube_source_id IS NOT NULL AND (
        (SELECT count(*) FROM json_each(NEW.evidence_json, '$.mapped_recording_ids'))
            <> (SELECT count(DISTINCT recording_id)
                FROM recording_sources WHERE source_id = NEW.youtube_source_id)
        OR EXISTS (
            SELECT 1 FROM json_each(NEW.evidence_json, '$.mapped_recording_ids') AS mapped
            WHERE NOT EXISTS (
                SELECT 1 FROM recording_sources AS link
                WHERE link.source_id = NEW.youtube_source_id
                  AND link.recording_id = mapped.value
            )
        )
    ) THEN RAISE(ABORT, 'torrent bracket mapped recording set differs') END;

    SELECT CASE WHEN NEW.resolution_state = 'native_source_without_unique_recording'
        AND EXISTS (
            SELECT 1
            FROM recordings AS recording
            JOIN recording_sources AS link
              ON link.recording_id = recording.recording_id
            WHERE link.source_id = NEW.youtube_source_id
              AND recording.canonical_key = 'youtube:video:' || NEW.youtube_video_id
              AND (SELECT count(DISTINCT one.recording_id)
                   FROM recording_sources AS one
                   WHERE one.source_id = NEW.youtube_source_id) = 1
        )
    THEN RAISE(ABORT, 'torrent bracket non-unique resolution differs') END;

    SELECT CASE WHEN NEW.youtube_recording_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM recordings AS recording
        JOIN recording_sources AS link
          ON link.recording_id = recording.recording_id
        WHERE recording.recording_id = NEW.youtube_recording_id
          AND recording.canonical_key = 'youtube:video:' || NEW.youtube_video_id
          AND link.source_id = NEW.youtube_source_id
          AND (SELECT count(DISTINCT one.recording_id)
               FROM recording_sources AS one
               WHERE one.source_id = NEW.youtube_source_id) = 1
    ) THEN RAISE(ABORT, 'torrent bracket YouTube recording differs') END;
END;

-- Seal only a complete typed candidate set.  Receipt insertion happens first so
-- candidates can reference it, but the parent import may not transition to
-- completed until every declared candidate/review task is present.  Candidate
-- admission above is limited to the running phase, so a sealed set cannot grow.
CREATE TRIGGER torrent_bracket_reconciliation_completion_guard
BEFORE UPDATE OF status ON import_batches
WHEN OLD.status <> 'completed'
 AND NEW.status = 'completed'
 AND EXISTS (
     SELECT 1 FROM torrent_bracket_reconciliation_imports AS receipt
     WHERE receipt.import_batch_id = OLD.import_batch_id
 )
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM torrent_bracket_reconciliation_imports AS receipt
        WHERE receipt.import_batch_id = NEW.import_batch_id
          AND NEW.importer_name = 'torrent_bracket_reconciliation_v1'
          AND NEW.input_sha256 = receipt.plan_sha256
          AND NEW.source_snapshot_date = substr(receipt.observed_at, 1, 10)
          AND NEW.completed_at = receipt.observed_at
          AND NEW.statistics_json = receipt.statistics_json
          AND EXISTS (
              SELECT 1
              FROM import_observations AS observation
              WHERE observation.import_batch_id = NEW.import_batch_id
                AND observation.importer_version = NEW.importer_version
                AND observation.source_snapshot_date = NEW.source_snapshot_date
                AND observation.observed_at = receipt.observed_at
                AND observation.status = 'completed'
                AND observation.completed_at = receipt.observed_at
                AND observation.statistics_json = receipt.statistics_json
          )
          AND receipt.candidate_count = (
              SELECT count(*)
              FROM torrent_bracket_youtube_candidates AS candidate
              WHERE candidate.import_batch_id = receipt.import_batch_id
          )
          AND receipt.review_task_count = (
              SELECT count(DISTINCT candidate.review_task_id)
              FROM torrent_bracket_youtube_candidates AS candidate
              WHERE candidate.import_batch_id = receipt.import_batch_id
          )
    ) THEN RAISE(ABORT, 'incomplete torrent bracket reconciliation import') END;
END;

CREATE TRIGGER torrent_bracket_reconciliation_completed_batch_no_reopen
BEFORE UPDATE OF status ON import_batches
WHEN OLD.status = 'completed'
 AND NEW.status <> 'completed'
 AND EXISTS (
     SELECT 1 FROM torrent_bracket_reconciliation_imports AS receipt
     WHERE receipt.import_batch_id = OLD.import_batch_id
 )
BEGIN
    SELECT RAISE(ABORT, 'completed torrent bracket reconciliation import cannot reopen');
END;

-- Candidate objects are private routing records and cannot receive publication
-- decisions.  A later reviewed source/recording relationship needs its own explicit
-- adjudication path rather than publishing this machine-derived row.
CREATE TRIGGER torrent_bracket_candidates_no_publication
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type = 'match_candidate'
 AND EXISTS (
     SELECT 1 FROM torrent_bracket_youtube_candidates AS candidate
     WHERE candidate.match_candidate_id = NEW.object_id
 )
BEGIN
    SELECT RAISE(ABORT, 'torrent bracket candidates have no publication authority');
END;
