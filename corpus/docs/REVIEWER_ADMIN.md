# Reviewer administration

Reviewer rows are authority-bearing private catalog records. Register and change
them through the strict reviewer-administration manifest, never through publication
manifests or ad hoc SQL.

## Safety model

- A registration supports exactly `human`, `automated_policy`, or
  `imported_legacy` and always creates an **inactive** reviewer.
- Activation or deactivation is a distinct `state_changes` event with its own ID,
  timestamp, and basis. An activation must be later than registration.
- IDs are bounded machine identifiers. Display labels are normalized ASCII text;
  control characters, markup, URLs, surrounding/repeated whitespace, and unknown
  fields fail closed.
- Registrations cannot reuse an existing reviewer ID, even when the proposed row
  looks identical. State changes reject unknown reviewers, no-ops, backdating, and
  conflict with the current database state.
- Every accepted manifest and event is append-only. Reviewer identities cannot be
  edited or deleted, and active flags change only through the newest matching event.
- Migration `0027` records every preexisting reviewer in one explicit migration-only
  adoption ledger at the actual migration time. After that point every direct
  reviewer insert is rejected; there is no ongoing legacy compatibility bypass.

`authorized_by` records the operator responsible for a manifest. It is audit
provenance, not cryptographic authentication; filesystem and database access remain
the outer authorization boundary.

## Migration rehearsal

Migration `0027` deliberately refuses a catalog that already contains a publication
or gate decision dated later than the migration time. Current-publication views take
the newest row immediately, so such a row would be present authority rather than a
scheduled action. The refusal is atomic: the migration ledger remains at `0026` and
no reviewer-administration or temporary guard table remains. A human must investigate
the future row; do not rewrite, delete, or automatically retract it to make migration
pass.

Rehearse the migration on an exact, disposable copy before opening the production
catalog for schema changes. Substitute private absolute paths and keep all three
files outside the repository:

```sh
PRODUCTION_CATALOG=/absolute/private/path/corpus.sqlite3
BACKUP_CATALOG=/absolute/private/backups/corpus-before-0027.sqlite3
REHEARSAL_CATALOG=/absolute/private/rehearsal/corpus-0027.sqlite3

# Stop every writer first. Checkpoint through SQLite, close this process, and verify
# that no live journal state remains before making a byte-for-byte backup.
sqlite3 "$PRODUCTION_CATALOG" 'PRAGMA wal_checkpoint(TRUNCATE);'
test ! -e "${PRODUCTION_CATALOG}-wal"
test ! -e "${PRODUCTION_CATALOG}-shm"
test ! -e "${PRODUCTION_CATALOG}-journal"
cp --preserve=all "$PRODUCTION_CATALOG" "$BACKUP_CATALOG"
cmp --silent "$PRODUCTION_CATALOG" "$BACKUP_CATALOG"
sha256sum "$PRODUCTION_CATALOG" "$BACKUP_CATALOG"

# Rehearse only on a second copy. Never point the rehearsal command at production.
cp --preserve=all "$BACKUP_CATALOG" "$REHEARSAL_CATALOG"
PYTHONPATH=corpus/src python -m himr_corpus migrate --db "$REHEARSAL_CATALOG"
test ! -e "${REHEARSAL_CATALOG}-wal"
test ! -e "${REHEARSAL_CATALOG}-shm"
test ! -e "${REHEARSAL_CATALOG}-journal"
PYTHONPATH=corpus/src python -m himr_corpus validate --db "$REHEARSAL_CATALOG"

# Both pragmas must report clean (`integrity_check` prints `ok` and
# `foreign_key_check` prints no rows). Preserve these count snapshots for review.
sqlite3 -header -column "$BACKUP_CATALOG" \
  "SELECT 'before_reviewers' AS metric, count(*) AS value FROM reviewers
   UNION ALL SELECT 'before_publication_decisions', count(*) FROM publication_decisions
   UNION ALL SELECT 'before_gate_decisions', count(*) FROM publication_gate_decisions;"
sqlite3 -header -column "$REHEARSAL_CATALOG" \
  "PRAGMA integrity_check;
   PRAGMA foreign_key_check;
   SELECT 'after_reviewers' AS metric, count(*) AS value FROM reviewers
   UNION ALL SELECT 'after_publication_decisions', count(*) FROM publication_decisions
   UNION ALL SELECT 'after_gate_decisions', count(*) FROM publication_gate_decisions
   UNION ALL SELECT 'legacy_adoptions', count(*) FROM reviewer_admin_events
             WHERE event_kind = 'legacy_adopt';"
```

The before/after reviewer and publication counts must match. On a catalog upgraded
from `0026`, `legacy_adoptions` must equal the reviewer count observed before the
migration. Retain the untouched backup and the command output until the production
migration and subsequent validation are complete.

Writer commands use the normal writable connection and can automatically install
pending migrations. In particular,
`import-reviewer-admin-manifest --apply` and
`import-publication-manifest` without `--dry-run` must not be used as a migration
rehearsal. Run the explicit `migrate` command first, review its result, close the
writer, and run immutable `validate` before any writer apply command.

## Commands

Apply migration `0027_reviewer_admin.sql` under normal private-catalog maintenance
before using this lane. The validation and default import commands use the immutable
audit connection: the database must be closed, checkpointed, and free of SQLite
sidecars.

```sh
PYTHONPATH=corpus/src python -m himr_corpus validate-reviewer-admin-manifest \
  --db /absolute/path/to/private-corpus.sqlite3 \
  --manifest /absolute/path/to/reviewer-admin.json

# Import is a dry-run unless the operator supplies the explicit capability flag.
PYTHONPATH=corpus/src python -m himr_corpus import-reviewer-admin-manifest \
  --db /absolute/path/to/private-corpus.sqlite3 \
  --manifest /absolute/path/to/reviewer-admin.json

PYTHONPATH=corpus/src python -m himr_corpus import-reviewer-admin-manifest \
  --db /absolute/path/to/private-corpus.sqlite3 \
  --manifest /absolute/path/to/reviewer-admin.json \
  --apply
```

Dry-run performs the same manifest, identity, event-ID, timestamp, and current-state
checks but inserts no reviewer, manifest, or event row. `--apply` rechecks inside one
`BEGIN IMMEDIATE` transaction and rolls back the entire manifest on any conflict.
Future-dated manifests and events are rejected; this lane does not schedule authority.

The current `reviewers.active` value remains the authority gate consumed by
publication administration. Deactivation blocks new publication decisions from that
reviewer; it does not rewrite or delete their historical decisions. New publication
and gate decisions must also fall within a recorded active interval for that reviewer.
The built-in public-metadata policy is registered and activated, at its actual first
use time, inside the same transaction that applies its closed allowlist.
The machine-transcript default policy has a separate reserved reviewer ID,
reviewer-admin manifest ID, and registration/activation event IDs. Generic reviewer
manifests cannot register, activate, deactivate, or otherwise reuse them. Its
dedicated publication command creates the reviewer on first use and verifies the
complete built-in enrollment provenance before accepting an existing row.
