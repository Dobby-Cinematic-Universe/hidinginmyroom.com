-- Publication manifests require a human-readable note in addition to the terse
-- policy/evidence basis. Existing machine-policy decisions predate that workflow,
-- so the catalog columns remain nullable while the strict manifest importer makes
-- both fields mandatory for all new administrative decisions.
ALTER TABLE publication_decisions ADD COLUMN notes TEXT;
ALTER TABLE publication_gate_decisions ADD COLUMN notes TEXT;

CREATE TABLE publication_manifest_imports (
    manifest_id TEXT PRIMARY KEY,
    input_sha256 TEXT NOT NULL UNIQUE CHECK(length(input_sha256) = 64),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    publication_decision_count INTEGER NOT NULL CHECK(publication_decision_count >= 0),
    gate_decision_count INTEGER NOT NULL CHECK(gate_decision_count >= 0),
    imported_at TEXT NOT NULL CHECK(julianday(imported_at) IS NOT NULL),
    CHECK(publication_decision_count + gate_decision_count > 0)
);

CREATE TRIGGER publication_manifest_imports_no_update
BEFORE UPDATE ON publication_manifest_imports
BEGIN
    SELECT RAISE(ABORT, 'publication manifest imports are append-only');
END;

CREATE TRIGGER publication_manifest_imports_no_delete
BEFORE DELETE ON publication_manifest_imports
BEGIN
    SELECT RAISE(ABORT, 'publication manifest imports are append-only');
END;

ALTER TABLE publication_decisions
    ADD COLUMN manifest_id TEXT REFERENCES publication_manifest_imports(manifest_id);
ALTER TABLE publication_gate_decisions
    ADD COLUMN manifest_id TEXT REFERENCES publication_manifest_imports(manifest_id);
