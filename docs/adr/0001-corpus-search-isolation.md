# ADR 0001: Isolate corpus search from wiki search

- **Status:** Accepted; transcript-selection paragraphs superseded by ADR 0010
- **Date:** 2026-08-26

## Context

The public corpus contains timestamped machine and human-corrected transcript material.
Its volume, provisional wording, and segment-level results would overwhelm the reviewed wiki
results if both surfaces shared one search index. The site must remain a static Astro deployment.

Starlight generates the wiki index with Pagefind after Astro renders the site. Starlight marks
indexable documentation content with `data-pagefind-body`; Pagefind excludes pages without that
marker once it appears in a site. Pagefind also supports independent indexes made from custom
records.

## Decision

The site publishes two physically separate Pagefind bundles:

| Surface       | Generated bundle    | Record source                                             |
| ------------- | ------------------- | --------------------------------------------------------- |
| Reviewed wiki | `/pagefind/`        | Starlight pages carrying `data-pagefind-body`             |
| Public corpus | `/corpus/pagefind/` | Sanitized transcript segments passed to `addCustomRecord` |

Corpus routes live under `src/pages/corpus/`, not in the Starlight docs collection. Their shared
layout sets `data-pagefind-ignore="all"` and never emits `data-pagefind-body`. The corpus browser
dynamically loads only `/corpus/pagefind/pagefind.js`. Starlight's `mergeIndex` option is not used.

The corpus index builder prefers the integrity-checked v2
`src/data/corpus/manifest.json` tree and retains a strict v1 `release.json` fallback during
migration. It streams one recording shard at a time and creates one custom record for each
non-empty segment of every eligible non-retracted transcript revision. It does not choose a
preferred revision per language. Each result links to the static recording page with seconds plus
immutable revision and segment IDs. Its metadata and facets include provenance, date, language,
speaker label, review state, recording type, and calibrated-confidence information.

At 25,000 records the builder writes and closes the active Pagefind index, then starts another.
`/corpus/search-manifest.json` lists each independent bundle. The browser searches those bundles
in parallel and merges result handles by score. This bounds a single logical Pagefind index and
preserves the separate `/corpus/pagefind/` namespace.

A missing v2 manifest falls back to v1; if neither exists the loader treats the release as empty.
Astro still emits the corpus landing page,
Pagefind still emits an empty bundle, and the interface explains that no records are available.
Malformed or internally inconsistent releases fail the build rather than silently publishing
partial data.

## Build contract

The required production sequence is:

1. Run the normal Astro/Starlight build, which creates `/pagefind/`.
2. Run `node scripts/build-corpus-index.mjs`, which creates sharded bundles beneath
   `/corpus/pagefind/` and
   `/corpus/search-manifest.json`.
3. Run `node scripts/check-search-isolation.mjs`.

The isolation check fails when:

- corpus HTML carries the main Pagefind marker;
- corpus HTML lacks the blanket Pagefind exclusion;
- either index bundle is missing;
- the main-index count differs from the number of rendered marker pages; or
- the corpus-index count differs from its generated manifest.

## Consequences

- Main wiki searches cannot return transcript segments accidentally.
- Corpus filters and result granularity can evolve without changing Starlight search.
- Every deployment remains static and requires no Worker, database, or search service.
- Corpus search is available after a production build, not during an ordinary Astro development
  session unless a previously generated bundle is present.
- ADR 0010 supersedes the original preferred-per-language ranking: the exporter,
  Astro loader, and standalone index builder expose every eligible non-retracted
  revision, and their tests must continue to agree.
