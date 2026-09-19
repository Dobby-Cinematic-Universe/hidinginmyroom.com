CREATE TABLE recordings (
    recording_id TEXT PRIMARY KEY,
    canonical_key TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    date_label TEXT,
    date_year INTEGER,
    date_basis TEXT NOT NULL DEFAULT 'unknown',
    duration_ms INTEGER CHECK(duration_ms IS NULL OR duration_ms >= 0),
    recording_type TEXT NOT NULL DEFAULT 'video'
        CHECK(recording_type IN ('video', 'livestream', 'short', 'guest_appearance', 'compilation', 'unknown')),
    review_state TEXT NOT NULL DEFAULT 'metadata_only'
        CHECK(review_state IN ('metadata_only', 'unreviewed', 'reviewed', 'disputed', 'rejected', 'merged')),
    merged_into_recording_id TEXT REFERENCES recordings(recording_id),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK(merged_into_recording_id IS NULL OR merged_into_recording_id <> recording_id)
);

CREATE INDEX recordings_date_idx ON recordings(date_year, date_label);

CREATE TABLE recording_sources (
    recording_source_id TEXT PRIMARY KEY,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    mapping_role TEXT NOT NULL,
    source_start_ms INTEGER CHECK(source_start_ms IS NULL OR source_start_ms >= 0),
    source_end_ms INTEGER CHECK(source_end_ms IS NULL OR source_end_ms >= 0),
    recording_start_ms INTEGER CHECK(recording_start_ms IS NULL OR recording_start_ms >= 0),
    recording_end_ms INTEGER CHECK(recording_end_ms IS NULL OR recording_end_ms >= 0),
    mapping_method TEXT NOT NULL,
    confidence_state TEXT NOT NULL DEFAULT 'metadata_only'
        CHECK(confidence_state IN ('metadata_only', 'candidate', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    CHECK(source_end_ms IS NULL OR source_start_ms IS NULL OR source_end_ms >= source_start_ms),
    CHECK(recording_end_ms IS NULL OR recording_start_ms IS NULL OR recording_end_ms >= recording_start_ms),
    UNIQUE(recording_id, source_id, mapping_role, source_start_ms, recording_start_ms)
);

CREATE INDEX recording_sources_source_idx ON recording_sources(source_id);

CREATE TABLE recording_relations (
    recording_relation_id TEXT PRIMARY KEY,
    from_recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    relation_kind TEXT NOT NULL,
    to_recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    basis TEXT NOT NULL,
    confidence_state TEXT NOT NULL DEFAULT 'candidate'
        CHECK(confidence_state IN ('metadata_only', 'candidate', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    CHECK(from_recording_id <> to_recording_id),
    UNIQUE(from_recording_id, relation_kind, to_recording_id)
);

CREATE TABLE media_objects (
    media_id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL UNIQUE CHECK(length(sha256) = 64),
    byte_count INTEGER NOT NULL CHECK(byte_count >= 0),
    media_kind TEXT NOT NULL CHECK(media_kind IN ('video', 'audio', 'image', 'subtitle', 'document', 'other')),
    mime_type TEXT,
    container TEXT,
    duration_ms INTEGER CHECK(duration_ms IS NULL OR duration_ms >= 0),
    ffprobe_json TEXT CHECK(ffprobe_json IS NULL OR json_valid(ffprobe_json)),
    first_cataloged_at TEXT NOT NULL,
    integrity_state TEXT NOT NULL DEFAULT 'verified'
        CHECK(integrity_state IN ('verified', 'unverified', 'corrupt', 'missing'))
);

CREATE TABLE media_locations (
    media_location_id TEXT PRIMARY KEY,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE CASCADE,
    storage_uri TEXT NOT NULL,
    storage_class TEXT NOT NULL DEFAULT 'local',
    verified_at TEXT,
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0, 1)),
    UNIQUE(media_id, storage_uri)
);

CREATE TABLE media_sources (
    media_source_id TEXT PRIMARY KEY,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE CASCADE,
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    retrieved_at TEXT NOT NULL,
    retrieval_tool TEXT NOT NULL,
    retrieval_tool_version TEXT,
    source_snapshot_id TEXT REFERENCES source_snapshots(source_snapshot_id),
    UNIQUE(media_id, source_id)
);

CREATE TABLE media_derivations (
    child_media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE CASCADE,
    parent_media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE CASCADE,
    derivation_kind TEXT NOT NULL,
    processing_run_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    PRIMARY KEY(child_media_id, parent_media_id, derivation_kind),
    CHECK(child_media_id <> parent_media_id)
);

CREATE TABLE renditions (
    rendition_id TEXT PRIMARY KEY,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id),
    rendition_kind TEXT NOT NULL,
    label TEXT,
    review_state TEXT NOT NULL DEFAULT 'unreviewed'
        CHECK(review_state IN ('unreviewed', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(recording_id, media_id, rendition_kind)
);

CREATE TABLE timeline_map_spans (
    timeline_map_span_id TEXT PRIMARY KEY,
    rendition_id TEXT NOT NULL REFERENCES renditions(rendition_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    media_start_ms INTEGER NOT NULL CHECK(media_start_ms >= 0),
    media_end_ms INTEGER NOT NULL CHECK(media_end_ms > media_start_ms),
    recording_start_ms INTEGER,
    recording_end_ms INTEGER,
    mapping_kind TEXT NOT NULL CHECK(mapping_kind IN ('exact', 'estimated', 'edited', 'gap', 'unknown')),
    confidence_state TEXT NOT NULL DEFAULT 'candidate'
        CHECK(confidence_state IN ('metadata_only', 'candidate', 'reviewed', 'disputed', 'rejected')),
    CHECK(recording_start_ms IS NULL OR recording_start_ms >= 0),
    CHECK(recording_end_ms IS NULL OR recording_start_ms IS NULL OR recording_end_ms > recording_start_ms),
    UNIQUE(rendition_id, ordinal)
);

CREATE TABLE fingerprints (
    fingerprint_id TEXT PRIMARY KEY,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE CASCADE,
    fingerprint_kind TEXT NOT NULL,
    implementation_version TEXT NOT NULL,
    start_ms INTEGER CHECK(start_ms IS NULL OR start_ms >= 0),
    end_ms INTEGER CHECK(end_ms IS NULL OR end_ms >= 0),
    value_text TEXT,
    artifact_uri TEXT,
    CHECK(value_text IS NOT NULL OR artifact_uri IS NOT NULL),
    CHECK(end_ms IS NULL OR start_ms IS NULL OR end_ms > start_ms),
    UNIQUE(media_id, fingerprint_kind, implementation_version, start_ms, end_ms)
);

CREATE TABLE match_candidates (
    match_candidate_id TEXT PRIMARY KEY,
    left_object_type TEXT NOT NULL,
    left_object_id TEXT NOT NULL,
    right_object_type TEXT NOT NULL,
    right_object_id TEXT NOT NULL,
    match_method TEXT NOT NULL,
    raw_score REAL,
    calibrated_probability REAL CHECK(calibrated_probability IS NULL OR (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    decision_state TEXT NOT NULL DEFAULT 'candidate'
        CHECK(decision_state IN ('candidate', 'accepted', 'rejected', 'disputed')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    CHECK(left_object_type <> right_object_type OR left_object_id <> right_object_id)
);
