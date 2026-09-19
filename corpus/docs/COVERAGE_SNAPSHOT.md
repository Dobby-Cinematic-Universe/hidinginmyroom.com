# Private catalogue coverage snapshot

The coverage command turns one sealed catalogue checkpoint into deterministic,
aggregate-only progress data. It is intended for weekly operations and backlog
planning, not publication.

It reports source-family counts, acquisition/rendition/transcript coverage,
processing and job states, review queues, match-candidate states, selected public
projection counts, and whether each machine-enrichment table exists. It includes no
source URL or native ID, title, transcript or OCR text, entity/person label, event or
claim wording, review reason, private path, or row identifier.

## Input boundary

The command requires:

- one regular, non-symlink catalogue with exactly one hard link and mode `0400`;
- a lexical input path with no symlinked component;
- an explicit lowercase SHA-256 pin;
- a closed, checkpointed database with no `-wal`, `-shm`, or `-journal` sidecar;
- an applied migration ledger that is a contiguous, hash-matching prefix of the
  migrations in the current source tree.

Pending migrations are reported rather than installed. This is intentional: a
coverage report must not gain schema-administration authority. The catalogue is
opened with `mode=ro&immutable=1` and `PRAGMA query_only=ON`, checked with
`PRAGMA quick_check`, and rehashed through the original pinned file descriptor. On
Linux, SQLite receives `/proc/self/fd/N` for that still-open descriptor rather than
the caller's pathname. Because a SQLite VFS may canonicalize that URI, the producer
also proves that the live SQLite connection retained a second descriptor with the
same device/inode fingerprint as the pin, and repeats that proof after all queries.
A changed file, path substitution, symlinked path component, sidecar, unexpected
mode, or hash mismatch fails closed.

Grouped categories are emitted only from finite producer-owned vocabularies. The
underlying catalogue columns are free text, so an unfamiliar value fails with a
generic error that does not echo it; a new legitimate platform, source kind, stage,
review-task kind, or match method requires an explicit producer update before a
snapshot can expose it.

Never delete or ignore a nonempty WAL merely to satisfy this command. Produce a
normal SQLite backup/checkpoint under separate administrative control, seal that
copy `0400`, and snapshot the copy.

## Usage

Full private aggregate snapshot:

```sh
PYTHONPATH=corpus/src python -m himr_corpus coverage-snapshot \
  --db /durable/private/catalog-checkpoints/corpus.sqlite3 \
  --expected-sha256 <exact-64-character-sha256>
```

Compact operator view:

```sh
PYTHONPATH=corpus/src python -m himr_corpus coverage-snapshot \
  --db /durable/private/catalog-checkpoints/corpus.sqlite3 \
  --expected-sha256 <exact-64-character-sha256> \
  --compact
```

The full snapshot binds the exact producer-source SHA-256 and has a content-derived
`snapshot_id` and `snapshot_sha256`; it has no generated-at field, so identical
catalogue bytes, source code, and SQLite runtime reproduce the same result. The
runtime qualifier matters because the full snapshot records `sqlite_version`. The
compact view retains only the snapshot/catalog hashes, migration state, principal
coverage counts, open-review and candidate totals, and
`publication_authority: none`.

The command writes no output file. Redirect stdout only into an owner-private,
write-once location if a durable receipt is needed, then hash and seal that receipt
under the normal private research policy. Do not install it into the public static
corpus or infer content, identity, completeness, authenticity, or publication
fitness from a count.
