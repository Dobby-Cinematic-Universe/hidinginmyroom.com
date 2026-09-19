-- Model registry entries are reproducibility anchors.  Register them through a
-- checksummed private manifest, then freeze both the audit ledger and model rows.

CREATE TABLE model_registry_manifest_imports (
    manifest_id TEXT PRIMARY KEY,
    input_sha256 TEXT NOT NULL UNIQUE CHECK(length(input_sha256) = 64),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    manifest_created_at TEXT NOT NULL CHECK(julianday(manifest_created_at) IS NOT NULL),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    registered_by TEXT NOT NULL,
    basis TEXT NOT NULL,
    model_count INTEGER NOT NULL CHECK(model_count > 0)
);

CREATE TABLE model_registry_manifest_models (
    manifest_id TEXT NOT NULL
        REFERENCES model_registry_manifest_imports(manifest_id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    model_id TEXT NOT NULL REFERENCES models(model_id) ON DELETE RESTRICT,
    model_snapshot_json TEXT NOT NULL CHECK(json_valid(model_snapshot_json)),
    PRIMARY KEY(manifest_id, ordinal),
    UNIQUE(manifest_id, model_id)
);

CREATE TRIGGER model_registry_manifest_imports_no_update
BEFORE UPDATE ON model_registry_manifest_imports
BEGIN
    SELECT RAISE(ABORT, 'model registry manifest imports are append-only');
END;

CREATE TRIGGER model_registry_manifest_imports_no_delete
BEFORE DELETE ON model_registry_manifest_imports
BEGIN
    SELECT RAISE(ABORT, 'model registry manifest imports are append-only');
END;

CREATE TRIGGER model_registry_manifest_models_no_update
BEFORE UPDATE ON model_registry_manifest_models
BEGIN
    SELECT RAISE(ABORT, 'model registry manifest links are append-only');
END;

CREATE TRIGGER model_registry_manifest_models_no_delete
BEFORE DELETE ON model_registry_manifest_models
BEGIN
    SELECT RAISE(ABORT, 'model registry manifest links are append-only');
END;

CREATE TRIGGER registered_models_no_update
BEFORE UPDATE ON models
BEGIN
    SELECT RAISE(ABORT, 'registered models are append-only');
END;

CREATE TRIGGER registered_models_no_delete
BEFORE DELETE ON models
BEGIN
    SELECT RAISE(ABORT, 'registered models are append-only');
END;
