CREATE TABLE calibration_sets (
    calibration_set_id TEXT PRIMARY KEY,
    task TEXT NOT NULL,
    model_id TEXT REFERENCES models(model_id),
    evaluation_dataset_label TEXT NOT NULL,
    method TEXT NOT NULL,
    fitted_parameters_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(fitted_parameters_json)),
    metrics_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metrics_json)),
    created_at TEXT NOT NULL
);

CREATE TABLE observations (
    observation_id TEXT PRIMARY KEY,
    observation_kind TEXT NOT NULL,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    rendition_id TEXT REFERENCES renditions(rendition_id),
    processing_run_id TEXT REFERENCES processing_runs(processing_run_id),
    start_ms INTEGER NOT NULL CHECK(start_ms >= 0),
    end_ms INTEGER NOT NULL CHECK(end_ms > start_ms),
    visibility TEXT NOT NULL DEFAULT 'private'
        CHECK(visibility IN ('private', 'review', 'public_candidate', 'public')),
    review_state TEXT NOT NULL DEFAULT 'machine'
        CHECK(review_state IN ('machine', 'human_corrected', 'media_checked', 'disputed', 'rejected')),
    payload_schema_version INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL
);

CREATE INDEX observations_recording_time_idx ON observations(recording_id, start_ms, end_ms);

CREATE TABLE observation_scores (
    observation_score_id TEXT PRIMARY KEY,
    observation_id TEXT NOT NULL REFERENCES observations(observation_id) ON DELETE CASCADE,
    score_name TEXT NOT NULL,
    raw_score REAL,
    calibrated_probability REAL CHECK(calibrated_probability IS NULL OR (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    calibration_set_id TEXT REFERENCES calibration_sets(calibration_set_id),
    quality_flags_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(quality_flags_json)),
    UNIQUE(observation_id, score_name)
);

CREATE TABLE speaker_turn_observations (
    observation_id TEXT PRIMARY KEY REFERENCES observations(observation_id) ON DELETE CASCADE,
    speaker_cluster_id TEXT NOT NULL,
    overlap_detected INTEGER NOT NULL DEFAULT 0 CHECK(overlap_detected IN (0, 1))
);

CREATE TABLE face_track_observations (
    observation_id TEXT PRIMARY KEY REFERENCES observations(observation_id) ON DELETE CASCADE,
    face_cluster_id TEXT,
    track_quality REAL,
    sample_artifact_id TEXT REFERENCES artifacts(artifact_id)
);

CREATE TABLE active_speaker_observations (
    observation_id TEXT PRIMARY KEY REFERENCES observations(observation_id) ON DELETE CASCADE,
    speaker_turn_observation_id TEXT REFERENCES speaker_turn_observations(observation_id),
    face_track_observation_id TEXT REFERENCES face_track_observations(observation_id),
    offscreen_or_unknown INTEGER NOT NULL DEFAULT 0 CHECK(offscreen_or_unknown IN (0, 1)),
    CHECK(face_track_observation_id IS NOT NULL OR offscreen_or_unknown = 1)
);

CREATE TABLE ocr_observations (
    observation_id TEXT PRIMARY KEY REFERENCES observations(observation_id) ON DELETE CASCADE,
    polygon_json TEXT NOT NULL CHECK(json_valid(polygon_json)),
    raw_text TEXT NOT NULL,
    normalized_text TEXT,
    language TEXT,
    redaction_state TEXT NOT NULL DEFAULT 'pending'
        CHECK(redaction_state IN ('pending', 'safe', 'redacted', 'suppressed'))
);

CREATE TABLE sound_observations (
    observation_id TEXT PRIMARY KEY REFERENCES observations(observation_id) ON DELETE CASCADE,
    vocabulary_revision TEXT NOT NULL,
    sound_label TEXT NOT NULL
);

CREATE TABLE action_observations (
    observation_id TEXT PRIMARY KEY REFERENCES observations(observation_id) ON DELETE CASCADE,
    vocabulary_revision TEXT NOT NULL,
    action_label TEXT NOT NULL,
    actor_entity_id TEXT
);

CREATE TABLE entities (
    entity_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL
        CHECK(entity_type IN ('person', 'community_figure', 'animal', 'place', 'organization', 'platform', 'term', 'object', 'unknown')),
    canonical_label TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    visibility TEXT NOT NULL DEFAULT 'private'
        CHECK(visibility IN ('private', 'review', 'public_candidate', 'public')),
    review_state TEXT NOT NULL DEFAULT 'unreviewed'
        CHECK(review_state IN ('unreviewed', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL
);

CREATE TABLE entity_aliases (
    entity_alias_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    alias_kind TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private'
        CHECK(visibility IN ('private', 'review', 'public_candidate', 'public')),
    source_id TEXT REFERENCES sources(source_id),
    valid_from TEXT,
    valid_to TEXT,
    UNIQUE(entity_id, alias, alias_kind)
);

CREATE TABLE identity_clusters (
    identity_cluster_id TEXT PRIMARY KEY,
    modality TEXT NOT NULL CHECK(modality IN ('face', 'voice', 'audiovisual')),
    implementation_version TEXT NOT NULL,
    artifact_id TEXT REFERENCES artifacts(artifact_id),
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    created_at TEXT NOT NULL
);

CREATE TABLE identity_assertions (
    identity_assertion_id TEXT PRIMARY KEY,
    identity_cluster_id TEXT NOT NULL REFERENCES identity_clusters(identity_cluster_id) ON DELETE CASCADE,
    entity_id TEXT NOT NULL REFERENCES entities(entity_id),
    assertion_state TEXT NOT NULL DEFAULT 'candidate'
        CHECK(assertion_state IN ('candidate', 'reviewed', 'disputed', 'rejected')),
    basis TEXT NOT NULL,
    calibrated_probability REAL CHECK(calibrated_probability IS NULL OR (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    created_at TEXT NOT NULL,
    UNIQUE(identity_cluster_id, entity_id)
);

CREATE TABLE appearances (
    appearance_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL REFERENCES entities(entity_id),
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    start_ms INTEGER NOT NULL CHECK(start_ms >= 0),
    end_ms INTEGER NOT NULL CHECK(end_ms > start_ms),
    appearance_role TEXT NOT NULL,
    observation_id TEXT REFERENCES observations(observation_id),
    review_state TEXT NOT NULL DEFAULT 'unreviewed'
        CHECK(review_state IN ('unreviewed', 'reviewed', 'disputed', 'rejected'))
);

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    canonical_label TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    event_kind TEXT NOT NULL,
    description TEXT,
    visibility TEXT NOT NULL DEFAULT 'private'
        CHECK(visibility IN ('private', 'review', 'public_candidate', 'public')),
    review_state TEXT NOT NULL DEFAULT 'unreviewed'
        CHECK(review_state IN ('unreviewed', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL
);

CREATE TABLE event_dates (
    event_date_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    date_kind TEXT NOT NULL,
    value_start TEXT,
    value_end TEXT,
    precision TEXT NOT NULL CHECK(precision IN ('day', 'month', 'year', 'range', 'circa', 'unknown')),
    basis TEXT NOT NULL,
    certainty TEXT NOT NULL CHECK(certainty IN ('certain', 'probable', 'uncertain', 'disputed'))
);

CREATE TABLE event_participants (
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    entity_id TEXT NOT NULL REFERENCES entities(entity_id),
    participant_role TEXT NOT NULL,
    review_state TEXT NOT NULL DEFAULT 'unreviewed'
        CHECK(review_state IN ('unreviewed', 'reviewed', 'disputed', 'rejected')),
    PRIMARY KEY(event_id, entity_id, participant_role)
);

CREATE TABLE event_relations (
    event_relation_id TEXT PRIMARY KEY,
    from_event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relation_kind TEXT NOT NULL,
    to_event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    basis TEXT NOT NULL,
    CHECK(from_event_id <> to_event_id),
    UNIQUE(from_event_id, relation_kind, to_event_id)
);

CREATE TABLE event_evidence (
    event_evidence_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    recording_id TEXT REFERENCES recordings(recording_id),
    source_id TEXT REFERENCES sources(source_id),
    transcript_revision_id TEXT REFERENCES transcript_revisions(revision_id),
    observation_id TEXT REFERENCES observations(observation_id),
    start_ms INTEGER CHECK(start_ms IS NULL OR start_ms >= 0),
    end_ms INTEGER CHECK(end_ms IS NULL OR end_ms >= 0),
    support_kind TEXT NOT NULL CHECK(support_kind IN ('direct', 'contextual', 'corroborating', 'contradicting')),
    CHECK(end_ms IS NULL OR start_ms IS NULL OR end_ms >= start_ms),
    CHECK(recording_id IS NOT NULL OR source_id IS NOT NULL OR transcript_revision_id IS NOT NULL OR observation_id IS NOT NULL)
);

