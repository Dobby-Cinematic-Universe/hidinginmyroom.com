CREATE TABLE import_batches (
    import_batch_id TEXT PRIMARY KEY,
    importer_name TEXT NOT NULL,
    importer_version TEXT NOT NULL,
    input_sha256 TEXT NOT NULL CHECK(length(input_sha256) = 64),
    source_snapshot_date TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed')),
    statistics_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(statistics_json)),
    UNIQUE(importer_name, input_sha256)
);

CREATE TABLE catalog_snapshots (
    catalog_snapshot_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    generator_version TEXT NOT NULL,
    source_cutoff TEXT,
    notes TEXT
);

CREATE TABLE sources (
    source_id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    native_id TEXT NOT NULL,
    parent_source_id TEXT REFERENCES sources(source_id),
    canonical_url TEXT,
    historical_url TEXT,
    title TEXT,
    published_at TEXT,
    observed_at TEXT NOT NULL,
    access_state TEXT NOT NULL DEFAULT 'unknown'
        CHECK(access_state IN ('public', 'members_only', 'private', 'removed', 'unavailable', 'unknown')),
    review_state TEXT NOT NULL DEFAULT 'metadata_only'
        CHECK(review_state IN ('metadata_only', 'unreviewed', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_by_import_batch_id TEXT REFERENCES import_batches(import_batch_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(platform, source_kind, native_id)
);

CREATE INDEX sources_platform_native_idx ON sources(platform, native_id);
CREATE INDEX sources_parent_idx ON sources(parent_source_id);

CREATE TABLE source_snapshots (
    source_snapshot_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    observed_at TEXT NOT NULL,
    request_url TEXT,
    final_url TEXT,
    http_status INTEGER,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64),
    artifact_path TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    UNIQUE(source_id, observed_at, payload_sha256)
);

CREATE INDEX source_snapshots_source_idx ON source_snapshots(source_id, observed_at);

CREATE TABLE source_relations (
    source_relation_id TEXT PRIMARY KEY,
    from_source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    relation_kind TEXT NOT NULL,
    to_source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    basis TEXT NOT NULL,
    confidence_state TEXT NOT NULL DEFAULT 'metadata_only'
        CHECK(confidence_state IN ('metadata_only', 'candidate', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    import_batch_id TEXT REFERENCES import_batches(import_batch_id),
    CHECK(from_source_id <> to_source_id),
    UNIQUE(from_source_id, relation_kind, to_source_id)
);

CREATE INDEX source_relations_to_idx ON source_relations(to_source_id, relation_kind);

CREATE TABLE source_hashes (
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    algorithm TEXT NOT NULL,
    digest TEXT NOT NULL,
    declared_by TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY(source_id, algorithm, digest)
);

CREATE TABLE external_ids (
    external_id_id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    external_value TEXT NOT NULL,
    confidence_state TEXT NOT NULL DEFAULT 'metadata_only'
        CHECK(confidence_state IN ('metadata_only', 'candidate', 'reviewed', 'disputed', 'rejected')),
    basis TEXT NOT NULL,
    source_id TEXT REFERENCES sources(source_id),
    UNIQUE(object_type, namespace, external_value, object_id)
);

CREATE INDEX external_ids_lookup_idx ON external_ids(namespace, external_value);

