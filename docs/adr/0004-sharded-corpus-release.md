# ADR 0004: Content-address the public corpus release

- **Status:** Accepted; transcript-selection paragraph superseded by ADR 0010
- **Date:** 2026-08-26

## Context

The first public projection was one nested `release.json`. It was useful as a schema
fixture, but every Astro route and the Pagefind builder had to deserialize all
recordings and all transcript segments. A full transcript backfill would make that
file, the landing page, and a single Pagefind process unreasonable build units.

Publication is deny-by-default. A storage migration must not turn discovery metadata,
machine-only transcript revisions, private identity analysis, or an object missing a
rights/privacy/sensitivity gate into public data.

## Decision

Static release schema v2 is a hash tree:

```text
src/data/corpus/manifest.json
  -> releases/<release_id>/catalog/catalog-<ordinal>-<hash>.json
       -> recordings/<recording_id>-<hash>.json
```

- The manifest contains counts, derived facets/statistics, the catalog bound, and
  SHA-256/byte-count descriptors for every catalog shard.
- A catalog shard contains at most 1,000 transcript-free summaries (250 by default).
  Each summary commits to one complete recording/transcript shard by SHA-256 and byte
  count.
- `release_id` is the first 24 hex characters of SHA-256 over canonical manifest
  content excluding `release_id`. Catalog hashes commit to detail hashes, so this ID
  commits to the complete tree without a circular identifier inside the shards.
- Shards use canonical compact UTF-8 JSON plus one newline. Paths are allowlisted,
  relative, non-symlink paths beneath the matching immutable release directory.
- Export writes and fsyncs a temporary release tree, installs the immutable directory,
  validates the complete on-disk hash tree, and only then atomically replaces the
  active manifest. Old release directories may be retained for audit or removed in a
  separate reviewed cleanup.
- The exporter still starts with the same gated `public_*` projection and applies the
  strict v1 record validator. Sharding cannot approve an object. An empty gated
  database deterministically exports the current 462-byte manifest with no catalog
  shards.
- The loader prefers v2. If no manifest exists, it accepts and strictly validates a v1
  `release.json` during migration. The repository boundary rejects having both active
  formats because that would make publication intent ambiguous.

Astro landing and browse routes load only catalog summaries. A recording route verifies
and loads one detail shard. The corpus indexer streams one detail at a time and creates
independent Pagefind indexes in 25,000-record batches. The browser searches all declared
indexes and merges result handles by score. Wiki Pagefind remains physically separate.

ADR 0010 supersedes the original preferred-transcript selection rule. Every
non-retracted published revision is now independently searchable. Revision headings
and search URLs continue to use immutable revision and segment IDs, never array
positions.

## Consequences

- Catalog and transcript payloads can grow without one release JSON or landing-page
  monolith.
- A changed byte, count, summary, path, or manifest field fails the build.
- A crash cannot make a partially written tree active.
- v1 remains readable, but v2 is the only checked-in active fixture.
- Pagefind batching bounds one index's logical record count, but Pagefind still emits
  many small fragment files and its backend has a substantial fixed/working memory
  cost. The measured 100,000-record build passes; a million-record production deploy
  still needs an asset-count and browser-latency gate or a more compact search format.
