-- Private, candidate-only reconciliation of retained Archive URL hint sources to
-- later provider-file sources.  This subtype records provenance alignment only.
-- It cannot merge/delete sources, copy an external ID, create a source relation,
-- mutate an old observation, or encode a publication/review outcome.

CREATE TABLE archive_hint_reconciliation_imports (
    import_batch_id TEXT PRIMARY KEY REFERENCES import_batches(import_batch_id),
    hint_import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    archive_import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    reddit_post_source_id TEXT NOT NULL REFERENCES sources(source_id),
    original_hints_sha256 TEXT NOT NULL
        CHECK(length(original_hints_sha256) = 64)
        CHECK(original_hints_sha256 = lower(original_hints_sha256))
        CHECK(original_hints_sha256 NOT GLOB '*[^0-9a-f]*')
        CHECK(original_hints_sha256 =
              '80fb016eccfd04dfbbf4df6d27f9ac54c5f80bd8e86ef37b0670c7095e04e587'),
    normalized_hints_sha256 TEXT NOT NULL
        CHECK(length(normalized_hints_sha256) = 64)
        CHECK(normalized_hints_sha256 = lower(normalized_hints_sha256))
        CHECK(normalized_hints_sha256 NOT GLOB '*[^0-9a-f]*')
        CHECK(normalized_hints_sha256 =
              '0006f09a7c4e024c2f155e803f22329ca9d94d25e5c8cc105be1a8e9ea754ab7'),
    discovery_sha256 TEXT NOT NULL
        CHECK(length(discovery_sha256) = 64)
        CHECK(discovery_sha256 = lower(discovery_sha256))
        CHECK(discovery_sha256 NOT GLOB '*[^0-9a-f]*')
        CHECK(discovery_sha256 =
              '35bc77da77e4e7249217bdb1d44d7fd55122b99b664a8b59c35c60f82ca9155a'),
    archive_snapshot_id TEXT NOT NULL
        CHECK(archive_snapshot_id GLOB 'iams_[0-9a-f]*')
        CHECK(length(archive_snapshot_id) = 37)
        CHECK(archive_snapshot_id = 'iams_4f920fcb565f826b8352b450247d18d3'),
    archive_snapshot_sha256 TEXT NOT NULL
        CHECK(length(archive_snapshot_sha256) = 64)
        CHECK(archive_snapshot_sha256 = lower(archive_snapshot_sha256))
        CHECK(archive_snapshot_sha256 NOT GLOB '*[^0-9a-f]*')
        CHECK(archive_snapshot_sha256 =
              '45b4aab1690a2c7610844904e1f56f2279f194b420578343beae6d16966874ba'),
    archive_request_id TEXT NOT NULL
        CHECK(archive_request_id GLOB 'iamr_[0-9a-f]*')
        CHECK(length(archive_request_id) = 37)
        CHECK(archive_request_id = 'iamr_e84fa2fd7fbd7bada30691e34329ea51'),
    combined_input_sha256 TEXT NOT NULL
        CHECK(length(combined_input_sha256) = 64)
        CHECK(combined_input_sha256 = lower(combined_input_sha256))
        CHECK(combined_input_sha256 NOT GLOB '*[^0-9a-f]*'),
    catalog_evidence_sha256 TEXT NOT NULL
        CHECK(length(catalog_evidence_sha256) = 64)
        CHECK(catalog_evidence_sha256 = lower(catalog_evidence_sha256))
        CHECK(catalog_evidence_sha256 NOT GLOB '*[^0-9a-f]*'),
    plan_sha256 TEXT NOT NULL
        CHECK(length(plan_sha256) = 64)
        CHECK(plan_sha256 = lower(plan_sha256))
        CHECK(plan_sha256 NOT GLOB '*[^0-9a-f]*'),
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    late_archive_items_json TEXT NOT NULL
        CHECK(json_valid(late_archive_items_json))
        CHECK(json_type(late_archive_items_json) = 'array')
        CHECK(json_array_length(late_archive_items_json) = 3)
        CHECK(json_extract(late_archive_items_json, '$[0]') = 'hidinginmyroom')
        CHECK(json_extract(late_archive_items_json, '$[1]') = 'hidinginmyroom2')
        CHECK(json_extract(late_archive_items_json, '$[2]') = 'hidinginmyroom3'),
    retained_url_count INTEGER NOT NULL CHECK(retained_url_count = 731),
    already_provider_count INTEGER NOT NULL CHECK(already_provider_count = 510),
    candidate_count INTEGER NOT NULL CHECK(candidate_count = 221),
    review_task_count INTEGER NOT NULL CHECK(review_task_count = 221),
    statistics_json TEXT NOT NULL CHECK(json_valid(statistics_json)),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    CHECK(retained_url_count = already_provider_count + candidate_count),
    CHECK(json_type(statistics_json, '$.retained_urls_total') = 'integer'
          AND json_extract(statistics_json, '$.retained_urls_total') = retained_url_count),
    CHECK(json_type(statistics_json, '$.already_provider_at_hint_import') = 'integer'
          AND json_extract(statistics_json, '$.already_provider_at_hint_import') = already_provider_count),
    CHECK(json_type(statistics_json, '$.late_exact_provider_candidates') = 'integer'
          AND json_extract(statistics_json, '$.late_exact_provider_candidates') = candidate_count),
    CHECK(json_type(statistics_json, '$.review_tasks_total') = 'integer'
          AND json_extract(statistics_json, '$.review_tasks_total') = review_task_count),
    CHECK(json_type(statistics_json, '$.media_payloads_read') = 'integer'
          AND json_extract(statistics_json, '$.media_payloads_read') = 0),
    CHECK(json_type(statistics_json, '$.media_payload_bytes_read') = 'integer'
          AND json_extract(statistics_json, '$.media_payload_bytes_read') = 0),
    CHECK(json_type(statistics_json, '$.sources_created') = 'integer'
          AND json_extract(statistics_json, '$.sources_created') = 0),
    CHECK(json_type(statistics_json, '$.sources_updated') = 'integer'
          AND json_extract(statistics_json, '$.sources_updated') = 0),
    CHECK(json_type(statistics_json, '$.sources_deleted') = 'integer'
          AND json_extract(statistics_json, '$.sources_deleted') = 0),
    CHECK(json_type(statistics_json, '$.source_relations_created') = 'integer'
          AND json_extract(statistics_json, '$.source_relations_created') = 0),
    CHECK(json_type(statistics_json, '$.external_ids_created') = 'integer'
          AND json_extract(statistics_json, '$.external_ids_created') = 0),
    CHECK(json_type(statistics_json, '$.recording_relations_created') = 'integer'
          AND json_extract(statistics_json, '$.recording_relations_created') = 0),
    CHECK(json_type(statistics_json, '$.recording_merges') = 'integer'
          AND json_extract(statistics_json, '$.recording_merges') = 0),
    CHECK(json_type(statistics_json, '$.review_decisions') = 'integer'
          AND json_extract(statistics_json, '$.review_decisions') = 0),
    CHECK(json_type(statistics_json, '$.publication_decisions') = 'integer'
          AND json_extract(statistics_json, '$.publication_decisions') = 0),
    CHECK(json_type(statistics_json, '$.public_rows') = 'integer'
          AND json_extract(statistics_json, '$.public_rows') = 0),
    CHECK(json_type(statistics_json, '$.identity_assertions') = 'integer'
          AND json_extract(statistics_json, '$.identity_assertions') = 0),
    CHECK(json_type(statistics_json, '$.claims') = 'integer'
          AND json_extract(statistics_json, '$.claims') = 0),
    UNIQUE(normalized_hints_sha256, archive_snapshot_sha256, plan_sha256)
);

CREATE TABLE archive_hint_provider_candidates (
    match_candidate_id TEXT PRIMARY KEY
        REFERENCES match_candidates(match_candidate_id) ON DELETE RESTRICT,
    import_batch_id TEXT NOT NULL
        REFERENCES archive_hint_reconciliation_imports(import_batch_id),
    review_task_id TEXT NOT NULL UNIQUE REFERENCES review_tasks(review_task_id),
    hint_source_id TEXT NOT NULL REFERENCES sources(source_id),
    provider_source_id TEXT NOT NULL REFERENCES sources(source_id),
    hint_metadata_observation_id TEXT NOT NULL
        REFERENCES source_metadata_observations(source_metadata_observation_id),
    provider_metadata_observation_id TEXT NOT NULL
        REFERENCES source_metadata_observations(source_metadata_observation_id),
    provider_projection_snapshot_id TEXT NOT NULL
        REFERENCES source_snapshots(source_snapshot_id),
    provider_projection_sha256 TEXT NOT NULL
        CHECK(length(provider_projection_sha256) = 64)
        CHECK(provider_projection_sha256 = lower(provider_projection_sha256))
        CHECK(provider_projection_sha256 NOT GLOB '*[^0-9a-f]*'),
    archive_item_capture_snapshot_id TEXT NOT NULL
        REFERENCES source_snapshots(source_snapshot_id),
    reference_relation_id TEXT NOT NULL REFERENCES source_relations(source_relation_id),
    reference_relation_observation_id TEXT NOT NULL
        REFERENCES source_relation_observations(source_relation_observation_id),
    external_id_id TEXT NOT NULL REFERENCES external_ids(external_id_id),
    external_id_observation_id TEXT NOT NULL
        REFERENCES external_id_observations(external_id_observation_id),
    provider_recording_source_id TEXT NOT NULL
        REFERENCES recording_sources(recording_source_id),
    provider_recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
    archive_item TEXT NOT NULL CHECK(archive_item IN (
        'hidinginmyroom', 'hidinginmyroom2', 'hidinginmyroom3'
    )),
    archive_filename TEXT NOT NULL CHECK(length(archive_filename) BETWEEN 1 AND 4096),
    archive_native_id TEXT NOT NULL CHECK(length(archive_native_id) BETWEEN 3 AND 8192),
    archive_url TEXT NOT NULL CHECK(length(archive_url) BETWEEN 20 AND 8192),
    provider_format TEXT NOT NULL CHECK(length(provider_format) BETWEEN 1 AND 256),
    provider_declared_byte_count INTEGER NOT NULL CHECK(provider_declared_byte_count >= 0),
    provider_declared_duration_ms INTEGER NOT NULL CHECK(provider_declared_duration_ms >= 0),
    provider_declared_crc32 TEXT NOT NULL
        CHECK(length(provider_declared_crc32) = 8)
        CHECK(provider_declared_crc32 = lower(provider_declared_crc32))
        CHECK(provider_declared_crc32 NOT GLOB '*[^0-9a-f]*'),
    provider_declared_md5 TEXT NOT NULL
        CHECK(length(provider_declared_md5) = 32)
        CHECK(provider_declared_md5 = lower(provider_declared_md5))
        CHECK(provider_declared_md5 NOT GLOB '*[^0-9a-f]*'),
    provider_declared_sha1 TEXT NOT NULL
        CHECK(length(provider_declared_sha1) = 40)
        CHECK(provider_declared_sha1 = lower(provider_declared_sha1))
        CHECK(provider_declared_sha1 NOT GLOB '*[^0-9a-f]*'),
    match_basis TEXT NOT NULL
        CHECK(match_basis = 'exact_archive_native_id_and_canonical_url'),
    candidate_state TEXT NOT NULL
        CHECK(candidate_state = 'candidate_only_unreviewed'),
    requires_human_review INTEGER NOT NULL DEFAULT 1 CHECK(requires_human_review = 1),
    relationship_asserted INTEGER NOT NULL DEFAULT 0 CHECK(relationship_asserted = 0),
    source_merge_performed INTEGER NOT NULL DEFAULT 0 CHECK(source_merge_performed = 0),
    external_id_copied INTEGER NOT NULL DEFAULT 0 CHECK(external_id_copied = 0),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    publication_authority TEXT NOT NULL DEFAULT 'none' CHECK(publication_authority = 'none'),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    CHECK(hint_source_id <> provider_source_id),
    CHECK(archive_native_id = archive_item || '/' || archive_filename),
    UNIQUE(import_batch_id, hint_source_id, provider_source_id),
    UNIQUE(hint_source_id, provider_source_id)
);

CREATE INDEX archive_hint_candidates_hint_idx
    ON archive_hint_provider_candidates(hint_source_id);
CREATE INDEX archive_hint_candidates_provider_idx
    ON archive_hint_provider_candidates(provider_source_id);
CREATE INDEX archive_hint_candidates_item_idx
    ON archive_hint_provider_candidates(archive_item, archive_native_id);

-- A receipt can wrap only the original completed URL-hint import, a completed
-- Archive metadata import with all three exact response captures, and one running
-- candidate-only parent batch.
CREATE TRIGGER archive_hint_reconciliation_imports_admission
BEFORE INSERT ON archive_hint_reconciliation_imports
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM import_batches AS candidate_import
        JOIN import_batches AS hint_import
          ON hint_import.import_batch_id = NEW.hint_import_batch_id
        JOIN import_batches AS archive_import
          ON archive_import.import_batch_id = NEW.archive_import_batch_id
        JOIN sources AS post ON post.source_id = NEW.reddit_post_source_id
        WHERE candidate_import.import_batch_id = NEW.import_batch_id
          AND candidate_import.importer_name = 'archive_hint_provider_reconciliation_v1'
          AND candidate_import.input_sha256 = NEW.plan_sha256
          AND candidate_import.status = 'running'
          AND candidate_import.started_at = NEW.observed_at
          AND candidate_import.source_snapshot_date = substr(NEW.observed_at, 1, 10)
          AND hint_import.importer_name = 'archive_url_discovery_hints'
          AND hint_import.input_sha256 = NEW.normalized_hints_sha256
          AND hint_import.status = 'completed'
          AND json_extract(hint_import.statistics_json, '$.valid_hints') = NEW.retained_url_count
          AND json_extract(hint_import.statistics_json, '$.resolved_existing_sources') = NEW.already_provider_count
          AND json_extract(hint_import.statistics_json, '$.new_hint_sources') = NEW.candidate_count
          AND archive_import.importer_name = 'internet_archive_metadata'
          AND archive_import.status = 'completed'
          AND post.platform = 'reddit'
          AND post.source_kind = 'post'
          AND post.native_id = '1q2sk8g'
          AND post.created_by_import_batch_id = hint_import.import_batch_id
    ) THEN RAISE(ABORT, 'invalid archive hint reconciliation receipt') END;

    SELECT CASE WHEN (
        SELECT count(DISTINCT item.native_id)
        FROM source_snapshots AS capture
        JOIN sources AS item ON item.source_id = capture.source_id
        WHERE capture.import_batch_id = NEW.archive_import_batch_id
          AND item.platform = 'internet_archive'
          AND item.source_kind = 'archive_item'
          AND item.native_id IN ('hidinginmyroom', 'hidinginmyroom2', 'hidinginmyroom3')
          AND capture.http_status = 200
          AND json_extract(capture.metadata_json, '$.archive_metadata_snapshot_id') = NEW.archive_snapshot_id
          AND json_extract(capture.metadata_json, '$.archive_metadata_snapshot_sha256') = NEW.archive_snapshot_sha256
          AND json_extract(capture.metadata_json, '$.request_id') = NEW.archive_request_id
          AND json_type(capture.metadata_json, '$.provider_fields_are_content_truth') = 'false'
          AND json_type(capture.metadata_json, '$.publication_authority') = 'false'
          AND (
              (item.native_id = 'hidinginmyroom'
               AND capture.source_snapshot_id = 'ssn_8a7b4ba8dd675da3bf616dcd66fad4b7'
               AND capture.payload_sha256 = '36cf322d32392329450dc6cb990017315d2519a37de388464da667dbe1cb2613'
               AND capture.observed_at = '2026-08-26T22:57:25Z')
           OR (item.native_id = 'hidinginmyroom2'
               AND capture.source_snapshot_id = 'ssn_1b62e3753dd95e9d90bca05d055e7a96'
               AND capture.payload_sha256 = '4cfa7215460f53306496ff9b668682ecb64ac972f40d2490a0bbac15a18fee79'
               AND capture.observed_at = '2026-08-26T22:57:28Z')
           OR (item.native_id = 'hidinginmyroom3'
               AND capture.source_snapshot_id = 'ssn_b93b8d883d7c5b059b118d2a87c9828b'
               AND capture.payload_sha256 = 'f41880686253b53ff531702abe02fd77da5c16e92a9593b19738e5ab430cff0d'
               AND capture.observed_at = '2026-08-26T22:57:32Z')
          )
    ) <> 3 THEN RAISE(ABORT, 'archive hint receipt lacks exact Archive captures') END;
END;

CREATE TRIGGER archive_hint_reconciliation_imports_no_update
BEFORE UPDATE ON archive_hint_reconciliation_imports
BEGIN
    SELECT RAISE(ABORT, 'archive hint reconciliation imports are append-only');
END;

CREATE TRIGGER archive_hint_reconciliation_imports_no_delete
BEFORE DELETE ON archive_hint_reconciliation_imports
BEGIN
    SELECT RAISE(ABORT, 'archive hint reconciliation imports are append-only');
END;

CREATE TRIGGER archive_hint_provider_candidates_no_update
BEFORE UPDATE ON archive_hint_provider_candidates
BEGIN
    SELECT RAISE(ABORT, 'archive hint provider candidates are append-only');
END;

CREATE TRIGGER archive_hint_provider_candidates_no_delete
BEFORE DELETE ON archive_hint_provider_candidates
BEGIN
    SELECT RAISE(ABORT, 'archive hint provider candidates are append-only');
END;

CREATE TRIGGER archive_hint_match_candidates_no_update
BEFORE UPDATE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint generic candidates are append-only');
END;

CREATE TRIGGER archive_hint_match_candidates_no_delete
BEFORE DELETE ON match_candidates
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.match_candidate_id = OLD.match_candidate_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint generic candidates are append-only');
END;

-- The typed row must wrap one unscored candidate plus the exact historical hint,
-- relation/external-ID observations and later provider projection.  It deliberately
-- requires no hint-to-provider source relation and no copied external ID.
CREATE TRIGGER archive_hint_provider_candidates_admission
BEFORE INSERT ON archive_hint_provider_candidates
BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM publication_decisions AS publication
        WHERE publication.object_type = 'match_candidate'
          AND publication.object_id = NEW.match_candidate_id
    ) OR EXISTS (
        SELECT 1 FROM review_decisions AS decision
        WHERE decision.target_type = 'match_candidate'
          AND decision.target_id = NEW.match_candidate_id
    ) THEN RAISE(ABORT, 'archive hint candidate already has decision state') END;

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM archive_hint_reconciliation_imports AS receipt
        JOIN import_batches AS candidate_import
          ON candidate_import.import_batch_id = receipt.import_batch_id
        JOIN import_batches AS hint_import
          ON hint_import.import_batch_id = receipt.hint_import_batch_id
        JOIN import_batches AS archive_import
          ON archive_import.import_batch_id = receipt.archive_import_batch_id
        JOIN match_candidates AS match
          ON match.match_candidate_id = NEW.match_candidate_id
        JOIN review_tasks AS task ON task.review_task_id = NEW.review_task_id
        JOIN sources AS hint ON hint.source_id = NEW.hint_source_id
        JOIN sources AS provider ON provider.source_id = NEW.provider_source_id
        JOIN source_metadata_observations AS hint_origin
          ON hint_origin.source_metadata_observation_id = NEW.hint_metadata_observation_id
        JOIN source_metadata_observations AS provider_origin
          ON provider_origin.source_metadata_observation_id = NEW.provider_metadata_observation_id
        JOIN source_snapshots AS provider_snapshot
          ON provider_snapshot.source_snapshot_id = NEW.provider_projection_snapshot_id
        JOIN source_snapshots AS item_capture
          ON item_capture.source_snapshot_id = NEW.archive_item_capture_snapshot_id
        JOIN sources AS archive_item_source
          ON archive_item_source.source_id = item_capture.source_id
        JOIN source_relations AS reference_relation
          ON reference_relation.source_relation_id = NEW.reference_relation_id
        JOIN source_relation_observations AS reference_origin
          ON reference_origin.source_relation_observation_id = NEW.reference_relation_observation_id
        JOIN external_ids AS external_id ON external_id.external_id_id = NEW.external_id_id
        JOIN external_id_observations AS external_origin
          ON external_origin.external_id_observation_id = NEW.external_id_observation_id
        JOIN recording_sources AS recording_link
          ON recording_link.recording_source_id = NEW.provider_recording_source_id
        WHERE receipt.import_batch_id = NEW.import_batch_id
          AND candidate_import.importer_name = 'archive_hint_provider_reconciliation_v1'
          AND candidate_import.input_sha256 = receipt.plan_sha256
          AND candidate_import.status = 'running'
          AND hint_import.importer_name = 'archive_url_discovery_hints'
          AND hint_import.status = 'completed'
          AND archive_import.importer_name = 'internet_archive_metadata'
          AND archive_import.status = 'completed'
          AND match.left_object_type = 'source'
          AND match.left_object_id = NEW.hint_source_id
          AND match.right_object_type = 'source'
          AND match.right_object_id = NEW.provider_source_id
          AND match.match_method = 'archive_hint_exact_native_id_candidate_v1'
          AND match.raw_score IS NULL
          AND match.calibrated_probability IS NULL
          AND match.decision_state = 'candidate'
          AND json_type(match.metadata_json) = 'object'
          AND (SELECT count(*) FROM json_each(match.metadata_json)) = 11
          AND json_extract(match.metadata_json, '$.schema_version') = 1
          AND json_extract(match.metadata_json, '$.candidate_kind') = 'archive_hint_to_provider_source'
          AND json_extract(match.metadata_json, '$.evidence_basis') = NEW.match_basis
          AND json_extract(match.metadata_json, '$.archive_native_id') = NEW.archive_native_id
          AND json_extract(match.metadata_json, '$.calibration_state') = 'not_calibrated'
          AND json_extract(match.metadata_json, '$.candidate_state') = NEW.candidate_state
          AND json_type(match.metadata_json, '$.requires_human_review') = 'true'
          AND json_type(match.metadata_json, '$.relationship_asserted') = 'false'
          AND json_type(match.metadata_json, '$.source_merge_performed') = 'false'
          AND json_type(match.metadata_json, '$.external_id_copied') = 'false'
          AND json_type(match.metadata_json, '$.publication_authority') = 'false'
          AND task.task_kind = 'archive_hint_provider_source_reconciliation_candidate'
          AND task.target_type = 'match_candidate'
          AND task.target_id = NEW.match_candidate_id
          AND hint.platform = 'internet_archive'
          AND hint.source_kind = 'archive_url_discovery_hint'
          AND hint.native_id = NEW.archive_native_id
          AND hint.canonical_url = NEW.archive_url
          AND hint.created_by_import_batch_id = receipt.hint_import_batch_id
          AND provider.platform = 'internet_archive'
          AND provider.source_kind = 'archive_media_file'
          AND provider.native_id = NEW.archive_native_id
          AND provider.canonical_url = NEW.archive_url
          AND provider.parent_source_id = archive_item_source.source_id
          AND provider.created_by_import_batch_id = receipt.archive_import_batch_id
          AND hint_origin.source_id = hint.source_id
          AND hint_origin.import_batch_id = receipt.hint_import_batch_id
          AND hint_origin.canonical_url = NEW.archive_url
          AND hint_origin.access_state = 'unknown'
          AND hint_origin.review_state = 'unreviewed'
          AND json_extract(hint_origin.metadata_json, '$.discovery_state') = 'url_hint_only'
          AND json_extract(hint_origin.metadata_json, '$.reddit_post_id') = '1q2sk8g'
          AND provider_origin.source_id = provider.source_id
          AND provider_origin.import_batch_id = receipt.archive_import_batch_id
          AND provider_origin.parent_source_id = archive_item_source.source_id
          AND provider_origin.canonical_url = NEW.archive_url
          AND provider_origin.access_state = 'public'
          AND provider_origin.review_state = 'metadata_only'
          AND json_extract(provider_origin.metadata_json, '$.internet_archive_item') = NEW.archive_item
          AND json_extract(provider_origin.metadata_json, '$.filename') = NEW.archive_filename
          AND json_extract(provider_origin.metadata_json, '$.format') = NEW.provider_format
          AND json_extract(provider_origin.metadata_json, '$.source_class') = 'original'
          AND json_extract(provider_origin.metadata_json, '$.derivative_of') IS NULL
          AND json_extract(provider_origin.metadata_json, '$.byte_count') = NEW.provider_declared_byte_count
          AND json_extract(provider_origin.metadata_json, '$.duration_ms') = NEW.provider_declared_duration_ms
          AND provider_snapshot.source_id = provider.source_id
          AND provider_snapshot.import_batch_id = receipt.archive_import_batch_id
          AND provider_snapshot.payload_sha256 = NEW.provider_projection_sha256
          AND item_capture.import_batch_id = receipt.archive_import_batch_id
          AND item_capture.http_status = 200
          AND archive_item_source.platform = 'internet_archive'
          AND archive_item_source.source_kind = 'archive_item'
          AND archive_item_source.native_id = NEW.archive_item
          AND (
              (NEW.archive_item = 'hidinginmyroom'
               AND item_capture.source_snapshot_id = 'ssn_8a7b4ba8dd675da3bf616dcd66fad4b7'
               AND item_capture.payload_sha256 = '36cf322d32392329450dc6cb990017315d2519a37de388464da667dbe1cb2613'
               AND item_capture.observed_at = '2026-08-26T22:57:25Z')
           OR (NEW.archive_item = 'hidinginmyroom2'
               AND item_capture.source_snapshot_id = 'ssn_1b62e3753dd95e9d90bca05d055e7a96'
               AND item_capture.payload_sha256 = '4cfa7215460f53306496ff9b668682ecb64ac972f40d2490a0bbac15a18fee79'
               AND item_capture.observed_at = '2026-08-26T22:57:28Z')
           OR (NEW.archive_item = 'hidinginmyroom3'
               AND item_capture.source_snapshot_id = 'ssn_b93b8d883d7c5b059b118d2a87c9828b'
               AND item_capture.payload_sha256 = 'f41880686253b53ff531702abe02fd77da5c16e92a9593b19738e5ab430cff0d'
               AND item_capture.observed_at = '2026-08-26T22:57:32Z')
          )
          AND json_extract(item_capture.metadata_json, '$.archive_metadata_snapshot_id') = receipt.archive_snapshot_id
          AND json_extract(item_capture.metadata_json, '$.archive_metadata_snapshot_sha256') = receipt.archive_snapshot_sha256
          AND json_extract(item_capture.metadata_json, '$.request_id') = receipt.archive_request_id
          AND json_type(item_capture.metadata_json, '$.provider_fields_are_content_truth') = 'false'
          AND json_type(item_capture.metadata_json, '$.publication_authority') = 'false'
          AND reference_relation.from_source_id = receipt.reddit_post_source_id
          AND reference_relation.relation_kind = 'references'
          AND reference_relation.to_source_id = hint.source_id
          AND reference_relation.basis = 'URL present in contributor-linked archive complement list'
          AND reference_relation.confidence_state = 'metadata_only'
          AND json_type(reference_relation.metadata_json, '$.coverage_hint_only') = 'true'
          AND reference_origin.source_relation_id = reference_relation.source_relation_id
          AND reference_origin.import_batch_id = receipt.hint_import_batch_id
          AND reference_origin.basis = reference_relation.basis
          AND reference_origin.confidence_state = 'metadata_only'
          AND json_type(reference_origin.metadata_json, '$.coverage_hint_only') = 'true'
          AND external_id.object_type = 'source'
          AND external_id.object_id = hint.source_id
          AND external_id.namespace = 'reddit_archive_url_hint'
          AND external_id.external_value = NEW.archive_url
          AND external_id.confidence_state = 'metadata_only'
          AND external_id.basis = 'Reddit post 1q2sk8g URL list'
          AND external_id.source_id = receipt.reddit_post_source_id
          AND external_origin.external_id_id = external_id.external_id_id
          AND external_origin.import_batch_id = receipt.hint_import_batch_id
          AND external_origin.confidence_state = 'metadata_only'
          AND external_origin.basis = external_id.basis
          AND external_origin.source_id = receipt.reddit_post_source_id
          AND recording_link.source_id = provider.source_id
          AND recording_link.recording_id = NEW.provider_recording_id
          AND recording_link.mapping_role = 'archive_original_file'
          AND recording_link.source_start_ms IS NULL
          AND recording_link.source_end_ms IS NULL
          AND recording_link.recording_start_ms IS NULL
          AND recording_link.recording_end_ms IS NULL
          AND recording_link.mapping_method = 'archive_filename_platform_id_grouping'
          AND recording_link.confidence_state = 'metadata_only'
          AND json_extract(recording_link.metadata_json, '$.source_class') = 'original'
    ) THEN RAISE(ABORT, 'invalid archive hint provider candidate') END;

    SELECT CASE WHEN (
        SELECT count(*) FROM recording_sources WHERE source_id = NEW.provider_source_id
    ) <> 1 THEN RAISE(ABORT, 'archive hint provider recording projection is not unique') END;

    SELECT CASE WHEN (
        SELECT count(*) FROM source_hashes
        WHERE source_id = NEW.provider_source_id
          AND declared_by = 'internet_archive_metadata'
          AND ((algorithm = 'crc32' AND digest = NEW.provider_declared_crc32)
            OR (algorithm = 'md5' AND digest = NEW.provider_declared_md5)
            OR (algorithm = 'sha1' AND digest = NEW.provider_declared_sha1))
    ) <> 3 OR (
        SELECT count(*) FROM source_hashes WHERE source_id = NEW.provider_source_id
    ) <> 3 THEN RAISE(ABORT, 'archive hint provider declared hashes differ') END;

    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM external_ids
        WHERE object_type = 'source'
          AND object_id = NEW.provider_source_id
          AND namespace = 'reddit_archive_url_hint'
          AND external_value = NEW.archive_url
    ) THEN RAISE(ABORT, 'archive hint external ID was copied to provider') END;

    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM source_relations
        WHERE (from_source_id = NEW.hint_source_id AND to_source_id = NEW.provider_source_id)
           OR (from_source_id = NEW.provider_source_id AND to_source_id = NEW.hint_source_id)
    ) THEN RAISE(ABORT, 'archive hint/provider relation already exists') END;

    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM media_sources WHERE source_id = NEW.provider_source_id
    ) THEN RAISE(ABORT, 'archive hint candidate cannot claim downloaded-byte lineage') END;

    SELECT CASE WHEN json_type(NEW.evidence_json) <> 'object'
        OR (SELECT count(*) FROM json_each(NEW.evidence_json)) <> 33
        OR json_extract(NEW.evidence_json, '$.schema_version') <> 1
        OR json_extract(NEW.evidence_json, '$.archive_snapshot_id') <>
           (SELECT archive_snapshot_id FROM archive_hint_reconciliation_imports
            WHERE import_batch_id = NEW.import_batch_id)
        OR json_extract(NEW.evidence_json, '$.archive_snapshot_sha256') <>
           (SELECT archive_snapshot_sha256 FROM archive_hint_reconciliation_imports
            WHERE import_batch_id = NEW.import_batch_id)
        OR json_extract(NEW.evidence_json, '$.original_hints_sha256') <>
           (SELECT original_hints_sha256 FROM archive_hint_reconciliation_imports
            WHERE import_batch_id = NEW.import_batch_id)
        OR json_extract(NEW.evidence_json, '$.normalized_hints_sha256') <>
           (SELECT normalized_hints_sha256 FROM archive_hint_reconciliation_imports
            WHERE import_batch_id = NEW.import_batch_id)
        OR json_extract(NEW.evidence_json, '$.archive_item') <> NEW.archive_item
        OR json_extract(NEW.evidence_json, '$.archive_filename') <> NEW.archive_filename
        OR json_extract(NEW.evidence_json, '$.archive_native_id') <> NEW.archive_native_id
        OR json_extract(NEW.evidence_json, '$.archive_url') <> NEW.archive_url
        OR json_extract(NEW.evidence_json, '$.match_basis') <> NEW.match_basis
        OR json_extract(NEW.evidence_json, '$.candidate_state') <> NEW.candidate_state
        OR json_extract(NEW.evidence_json, '$.hint_metadata_observation_id') <> NEW.hint_metadata_observation_id
        OR json_extract(NEW.evidence_json, '$.provider_metadata_observation_id') <> NEW.provider_metadata_observation_id
        OR json_extract(NEW.evidence_json, '$.provider_projection_snapshot_id') <> NEW.provider_projection_snapshot_id
        OR json_extract(NEW.evidence_json, '$.provider_projection_sha256') <> NEW.provider_projection_sha256
        OR json_extract(NEW.evidence_json, '$.archive_item_capture_snapshot_id') <> NEW.archive_item_capture_snapshot_id
        OR json_extract(NEW.evidence_json, '$.reference_relation_observation_id') <> NEW.reference_relation_observation_id
        OR json_extract(NEW.evidence_json, '$.external_id_observation_id') <> NEW.external_id_observation_id
        OR json_extract(NEW.evidence_json, '$.provider_recording_source_id') <> NEW.provider_recording_source_id
        OR json_extract(NEW.evidence_json, '$.provider_recording_id') <> NEW.provider_recording_id
        OR json_extract(NEW.evidence_json, '$.provider_format') <> NEW.provider_format
        OR json_extract(NEW.evidence_json, '$.provider_declared_byte_count') <> NEW.provider_declared_byte_count
        OR json_extract(NEW.evidence_json, '$.provider_declared_duration_ms') <> NEW.provider_declared_duration_ms
        OR json_extract(NEW.evidence_json, '$.provider_declared_crc32') <> NEW.provider_declared_crc32
        OR json_extract(NEW.evidence_json, '$.provider_declared_md5') <> NEW.provider_declared_md5
        OR json_extract(NEW.evidence_json, '$.provider_declared_sha1') <> NEW.provider_declared_sha1
        OR json_type(NEW.evidence_json, '$.requires_human_review') <> 'true'
        OR json_type(NEW.evidence_json, '$.relationship_asserted') <> 'false'
        OR json_type(NEW.evidence_json, '$.source_merge_performed') <> 'false'
        OR json_type(NEW.evidence_json, '$.external_id_copied') <> 'false'
        OR json_type(NEW.evidence_json, '$.provider_fields_are_content_truth') <> 'false'
        OR json_type(NEW.evidence_json, '$.payload_downloaded_or_read') <> 'false'
        OR json_type(NEW.evidence_json, '$.publication_authority') <> 'false'
    THEN RAISE(ABORT, 'archive hint candidate evidence differs') END;
END;

-- Once typed evidence exists, later metadata observations may still be appended,
-- but the source, relation and external-ID identities used by this evidence cannot
-- be rewritten in place.
CREATE TRIGGER archive_hint_candidate_source_identity_no_update
BEFORE UPDATE OF platform, source_kind, native_id, parent_source_id,
                 created_by_import_batch_id ON sources
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.hint_source_id = OLD.source_id
       OR candidate.provider_source_id = OLD.source_id
)
BEGIN
    SELECT CASE WHEN NEW.platform IS NOT OLD.platform
        OR NEW.source_kind IS NOT OLD.source_kind
        OR NEW.native_id IS NOT OLD.native_id
        OR NEW.parent_source_id IS NOT OLD.parent_source_id
        OR NEW.created_by_import_batch_id IS NOT OLD.created_by_import_batch_id
    THEN RAISE(ABORT, 'archive hint candidate source identity is immutable') END;
END;

CREATE TRIGGER archive_hint_candidate_snapshots_no_update
BEFORE UPDATE ON source_snapshots
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.provider_projection_snapshot_id = OLD.source_snapshot_id
       OR candidate.archive_item_capture_snapshot_id = OLD.source_snapshot_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint candidate snapshots are append-only');
END;

CREATE TRIGGER archive_hint_candidate_snapshots_no_delete
BEFORE DELETE ON source_snapshots
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.provider_projection_snapshot_id = OLD.source_snapshot_id
       OR candidate.archive_item_capture_snapshot_id = OLD.source_snapshot_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint candidate snapshots are append-only');
END;

CREATE TRIGGER archive_hint_candidate_hashes_no_update
BEFORE UPDATE ON source_hashes
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.provider_source_id = OLD.source_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint candidate provider hashes are append-only');
END;

CREATE TRIGGER archive_hint_candidate_hashes_no_delete
BEFORE DELETE ON source_hashes
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.provider_source_id = OLD.source_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint candidate provider hashes are append-only');
END;

CREATE TRIGGER archive_hint_candidate_recording_source_no_update
BEFORE UPDATE ON recording_sources
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.provider_recording_source_id = OLD.recording_source_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint candidate recording projection is append-only');
END;

CREATE TRIGGER archive_hint_candidate_recording_source_no_delete
BEFORE DELETE ON recording_sources
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.provider_recording_source_id = OLD.recording_source_id
)
BEGIN
    SELECT RAISE(ABORT, 'archive hint candidate recording projection is append-only');
END;

CREATE TRIGGER archive_hint_candidate_relation_identity_no_update
BEFORE UPDATE OF from_source_id, relation_kind, to_source_id ON source_relations
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.reference_relation_id = OLD.source_relation_id
)
BEGIN
    SELECT CASE WHEN NEW.from_source_id IS NOT OLD.from_source_id
        OR NEW.relation_kind IS NOT OLD.relation_kind
        OR NEW.to_source_id IS NOT OLD.to_source_id
    THEN RAISE(ABORT, 'archive hint reference relation identity is immutable') END;
END;

CREATE TRIGGER archive_hint_candidate_external_id_identity_no_update
BEFORE UPDATE OF object_type, object_id, namespace, external_value, source_id ON external_ids
WHEN EXISTS (
    SELECT 1 FROM archive_hint_provider_candidates AS candidate
    WHERE candidate.external_id_id = OLD.external_id_id
)
BEGIN
    SELECT CASE WHEN NEW.object_type IS NOT OLD.object_type
        OR NEW.object_id IS NOT OLD.object_id
        OR NEW.namespace IS NOT OLD.namespace
        OR NEW.external_value IS NOT OLD.external_value
        OR NEW.source_id IS NOT OLD.source_id
    THEN RAISE(ABORT, 'archive hint external ID identity is immutable') END;
END;

-- Receipt insertion precedes its children, but the parent batch cannot seal until
-- all declared candidates and distinct review tasks are present.  Candidate
-- admission is restricted to the running phase, so a sealed set cannot grow.
CREATE TRIGGER archive_hint_reconciliation_completion_guard
BEFORE UPDATE OF status ON import_batches
WHEN OLD.status <> 'completed'
 AND NEW.status = 'completed'
 AND EXISTS (
     SELECT 1 FROM archive_hint_reconciliation_imports AS receipt
     WHERE receipt.import_batch_id = OLD.import_batch_id
 )
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM archive_hint_reconciliation_imports AS receipt
        WHERE receipt.import_batch_id = NEW.import_batch_id
          AND NEW.importer_name = 'archive_hint_provider_reconciliation_v1'
          AND NEW.input_sha256 = receipt.plan_sha256
          AND NEW.source_snapshot_date = substr(receipt.observed_at, 1, 10)
          AND NEW.completed_at = receipt.observed_at
          AND NEW.statistics_json = receipt.statistics_json
          AND EXISTS (
              SELECT 1 FROM import_observations AS observation
              WHERE observation.import_batch_id = NEW.import_batch_id
                AND observation.importer_version = NEW.importer_version
                AND observation.source_snapshot_date = NEW.source_snapshot_date
                AND observation.observed_at = receipt.observed_at
                AND observation.status = 'completed'
                AND observation.completed_at = receipt.observed_at
                AND observation.statistics_json = receipt.statistics_json
          )
          AND receipt.candidate_count = (
              SELECT count(*) FROM archive_hint_provider_candidates AS candidate
              WHERE candidate.import_batch_id = receipt.import_batch_id
          )
          AND receipt.review_task_count = (
              SELECT count(DISTINCT candidate.review_task_id)
              FROM archive_hint_provider_candidates AS candidate
              WHERE candidate.import_batch_id = receipt.import_batch_id
          )
    ) THEN RAISE(ABORT, 'incomplete archive hint reconciliation import') END;
END;

CREATE TRIGGER archive_hint_reconciliation_completed_batch_no_reopen
BEFORE UPDATE OF status ON import_batches
WHEN OLD.status = 'completed'
 AND NEW.status <> 'completed'
 AND EXISTS (
     SELECT 1 FROM archive_hint_reconciliation_imports AS receipt
     WHERE receipt.import_batch_id = OLD.import_batch_id
 )
BEGIN
    SELECT RAISE(ABORT, 'completed archive hint reconciliation import cannot reopen');
END;

CREATE TRIGGER archive_hint_candidates_no_publication
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type = 'match_candidate'
 AND EXISTS (
     SELECT 1 FROM archive_hint_provider_candidates AS candidate
     WHERE candidate.match_candidate_id = NEW.object_id
 )
BEGIN
    SELECT RAISE(ABORT, 'archive hint candidates have no publication authority');
END;
