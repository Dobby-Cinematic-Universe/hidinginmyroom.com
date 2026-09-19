-- Import batches identify exact input bytes.  The same bytes can be observed more
-- than once, so observation time must not be folded into (or discarded by) that
-- content identity.
CREATE TABLE import_observations (
    import_observation_id TEXT PRIMARY KEY,
    import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    importer_version TEXT NOT NULL,
    source_snapshot_date TEXT,
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed')),
    completed_at TEXT CHECK(completed_at IS NULL OR julianday(completed_at) IS NOT NULL),
    statistics_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(statistics_json)),
    UNIQUE(import_batch_id, importer_version, observed_at)
);

CREATE INDEX import_observations_batch_time_idx
    ON import_observations(import_batch_id, observed_at);

-- Every metadata assertion remains available even when it is not the current
-- projection.  quality_rank is an importer-policy rank, not a probability.
CREATE TABLE source_metadata_observations (
    source_metadata_observation_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    import_observation_id TEXT REFERENCES import_observations(import_observation_id),
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    quality_rank INTEGER NOT NULL CHECK(quality_rank >= 0),
    quality_basis TEXT NOT NULL,
    candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256) = 64),
    parent_source_id TEXT REFERENCES sources(source_id),
    canonical_url TEXT,
    historical_url TEXT,
    title TEXT,
    published_at TEXT,
    access_state TEXT NOT NULL
        CHECK(access_state IN ('public', 'members_only', 'private', 'removed', 'unavailable', 'unknown')),
    review_state TEXT NOT NULL
        CHECK(review_state IN ('metadata_only', 'unreviewed', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(source_id, import_observation_id, candidate_sha256)
);

CREATE INDEX source_metadata_observations_winner_idx
    ON source_metadata_observations(source_id, quality_rank DESC, observed_at DESC);

CREATE TRIGGER source_metadata_observations_no_update
BEFORE UPDATE ON source_metadata_observations
BEGIN
    SELECT RAISE(ABORT, 'source metadata observations are append-only');
END;

CREATE TRIGGER source_metadata_observations_no_delete
BEFORE DELETE ON source_metadata_observations
BEGIN
    SELECT RAISE(ABORT, 'source metadata observations are append-only');
END;

CREATE TABLE recording_metadata_observations (
    recording_metadata_observation_id TEXT PRIMARY KEY,
    recording_id TEXT NOT NULL REFERENCES recordings(recording_id) ON DELETE CASCADE,
    import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    import_observation_id TEXT REFERENCES import_observations(import_observation_id),
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    quality_rank INTEGER NOT NULL CHECK(quality_rank >= 0),
    quality_basis TEXT NOT NULL,
    candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256) = 64),
    title TEXT NOT NULL,
    date_label TEXT,
    date_basis TEXT NOT NULL,
    duration_ms INTEGER CHECK(duration_ms IS NULL OR duration_ms >= 0),
    recording_type TEXT NOT NULL
        CHECK(recording_type IN ('video', 'livestream', 'short', 'guest_appearance', 'compilation', 'unknown')),
    review_state TEXT NOT NULL
        CHECK(review_state IN ('metadata_only', 'unreviewed', 'reviewed', 'disputed', 'rejected', 'merged')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(recording_id, import_observation_id, candidate_sha256)
);

CREATE INDEX recording_metadata_observations_winner_idx
    ON recording_metadata_observations(recording_id, quality_rank DESC, observed_at DESC);

CREATE TRIGGER recording_metadata_observations_no_update
BEFORE UPDATE ON recording_metadata_observations
BEGIN
    SELECT RAISE(ABORT, 'recording metadata observations are append-only');
END;

CREATE TRIGGER recording_metadata_observations_no_delete
BEFORE DELETE ON recording_metadata_observations
BEGIN
    SELECT RAISE(ABORT, 'recording metadata observations are append-only');
END;

ALTER TABLE sources ADD COLUMN current_metadata_observation_id TEXT
    REFERENCES source_metadata_observations(source_metadata_observation_id);
ALTER TABLE recordings ADD COLUMN current_metadata_observation_id TEXT
    REFERENCES recording_metadata_observations(recording_metadata_observation_id);

-- Cross-importer assertions can converge on the same relation or external ID.  Keep
-- their provenance too, rather than allowing INSERT OR IGNORE call order to choose a
-- basis label silently.
CREATE TABLE source_relation_observations (
    source_relation_observation_id TEXT PRIMARY KEY,
    source_relation_id TEXT NOT NULL REFERENCES source_relations(source_relation_id)
        ON DELETE CASCADE,
    import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    import_observation_id TEXT REFERENCES import_observations(import_observation_id),
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    quality_rank INTEGER NOT NULL CHECK(quality_rank >= 0),
    quality_basis TEXT NOT NULL,
    candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256) = 64),
    basis TEXT NOT NULL,
    confidence_state TEXT NOT NULL
        CHECK(confidence_state IN ('metadata_only', 'candidate', 'reviewed', 'disputed', 'rejected')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(source_relation_id, import_observation_id, candidate_sha256)
);

CREATE INDEX source_relation_observations_winner_idx
    ON source_relation_observations(source_relation_id, quality_rank DESC, observed_at DESC);

CREATE TRIGGER source_relation_observations_no_update
BEFORE UPDATE ON source_relation_observations
BEGIN
    SELECT RAISE(ABORT, 'source relation observations are append-only');
END;

CREATE TRIGGER source_relation_observations_no_delete
BEFORE DELETE ON source_relation_observations
BEGIN
    SELECT RAISE(ABORT, 'source relation observations are append-only');
END;

ALTER TABLE source_relations ADD COLUMN current_relation_observation_id TEXT
    REFERENCES source_relation_observations(source_relation_observation_id);

CREATE TABLE external_id_observations (
    external_id_observation_id TEXT PRIMARY KEY,
    external_id_id TEXT NOT NULL REFERENCES external_ids(external_id_id) ON DELETE CASCADE,
    import_batch_id TEXT NOT NULL REFERENCES import_batches(import_batch_id),
    import_observation_id TEXT REFERENCES import_observations(import_observation_id),
    observed_at TEXT NOT NULL CHECK(julianday(observed_at) IS NOT NULL),
    quality_rank INTEGER NOT NULL CHECK(quality_rank >= 0),
    quality_basis TEXT NOT NULL,
    candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256) = 64),
    basis TEXT NOT NULL,
    confidence_state TEXT NOT NULL
        CHECK(confidence_state IN ('metadata_only', 'candidate', 'reviewed', 'disputed', 'rejected')),
    source_id TEXT REFERENCES sources(source_id),
    UNIQUE(external_id_id, import_observation_id, candidate_sha256)
);

CREATE INDEX external_id_observations_winner_idx
    ON external_id_observations(external_id_id, quality_rank DESC, observed_at DESC);

CREATE TRIGGER external_id_observations_no_update
BEFORE UPDATE ON external_id_observations
BEGIN
    SELECT RAISE(ABORT, 'external ID observations are append-only');
END;

CREATE TRIGGER external_id_observations_no_delete
BEFORE DELETE ON external_id_observations
BEGIN
    SELECT RAISE(ABORT, 'external ID observations are append-only');
END;

ALTER TABLE external_ids ADD COLUMN current_external_id_observation_id TEXT
    REFERENCES external_id_observations(external_id_observation_id);
