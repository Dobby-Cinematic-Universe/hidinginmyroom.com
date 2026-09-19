# ADR 0003: Admit media through guarded, content-addressed acquisition

- **Status:** Accepted
- **Date:** 2026-08-26

## Context

The corpus inventory is much larger than the current workstation and points to a mix
of local files, Archive.org objects, public YouTube pages, and secondary community
links. Raw media must stay outside Git, originals must remain byte-identical, and a
remote downloader must not silently inherit personal credentials or expand one
reviewed URL into an unbounded crawl.

Acquisition also precedes the normalized SQLite catalog. It therefore needs a durable,
auditable handoff that can be imported without allowing a partial transfer to become
authoritative catalog state.

## Decision

The `acquisition/` program is a Python-standard-library orchestrator with three narrow
adapters:

1. admit one explicit local file;
2. download one explicit public HTTP/HTTPS object, including an Archive.org object;
3. invoke one caller-supplied yt-dlp executable for one explicit public YouTube page.

Each job has a versioned JSON work order and result envelope. It must supply an
absolute durable output root, one source identity, adapter-specific settings, and all
capacity limits. Runtime validation rejects unknown fields, temporary output roots,
remote sources not declared public, credential-bearing URLs, and Reddit discussion
URLs. The orchestrator neither derives Reddit media URLs nor crawls comments.

No adapter accepts cookies, tokens, passwords, usernames, authorization headers,
browser profiles, authentication bypasses, or member-only/private acquisition. The
yt-dlp adapter uses `--ignore-config`, a fresh empty HOME, no playlist expansion, and
no comments, subtitles, thumbnails, or info sidecars. Only selected non-secret remote
metadata is retained. Public YouTube pages must be labeled with the exact `youtube`
platform so a caller-controlled source label cannot bypass the bounded metadata and
native-ID checks. Its executable is hashed and stat-checked before and after use.
Version-1 work orders may optionally pin the expected executable SHA-256; when present,
the adapter enforces it before invoking yt-dlp. The field remains optional so existing
version-1 work orders preserve their canonical identity.

A separate deterministic materializer converts a queue-plan v1 snapshot into a private,
immutable bundle of exact work orders. It revalidates the plan ID and all derivable
planner semantics, reconstructs source URLs, and admits only selected `ready` candidates.
It requires an explicit media root, cache cap, free-space floor, and yt-dlp path plus
digest. A bundle manifest hashes every order and expressly grants no publication
authority. Bundle admission uses a non-waiting writer lock and atomic complete-tree
rename; exact read-only replay is idempotent and any divergence fails closed. This step
does not invoke a downloader or mutate/import the catalog.

## Integrity and storage

Every actual job reserves disk capacity before creating staging. The current default
recommendation is a 50 GiB managed hot-cache cap and an 80 GiB free-space floor; the
per-job default is 10 GiB and should be lowered when the expected object is known.
Every limit is explicit in the work order.

An OS-backed, non-waiting advisory lock permits one actual writer per output root.
Contention fails with bounded holder metadata. The lock file is durable for diagnosis,
but the kernel lock is authoritative: process death releases it, and the next writer
detects and replaces stale on-disk state. Dry runs remain lock-free and read-only.
External schedulers should also serialize each output root as defense in depth and on
storage whose advisory-lock behavior has not been established.

Bytes stage beneath the output root and are bounded during transfer. Optional expected
size and SHA-256 values are enforced. The complete staged object is hashed and probed
with FFprobe. Only then is it hard-linked without overwrite into:

```text
<output-root>/media/sha256/<prefix>/<full-sha256>/payload
```

Staging and canonical media therefore remain on one filesystem, making admission
atomic. An existing destination must rehash correctly before reuse. Local source
metadata is checked before and after copying; the input is never modified. Direct HTTP
resumption requires the exact URL and uses saved ETag or Last-Modified validators with
`Range`/`If-Range`. Unsafe ranges restart instead of concatenating bytes.

yt-dlp runs in its own process group under a live monitor. The monitor accounts for the
whole staging tree and bounded diagnostics, compares it with the job and reserved
global-cache budgets, checks the live filesystem free-space floor, and terminates the
group on a violation. Resource-limit failure removes that yt-dlp staging tree before
the writer lock is released, so oversized bytes cannot be reused or admitted.

A completed work order has a durable result under `jobs/`. Reuse requires rehashing its
canonical payload, matching the normalized work-order identity, and reapplying remote
selected-identity checks. Dry runs create nothing and never access the network; local
dry runs can still hash and probe their read-only source.

## Catalog boundary

The result's `catalog_records` keys align with `sources`, `media_objects`,
`media_locations`, and `media_sources`. Acquisition completion supplies the catalog
observation field `media_objects.first_cataloged_at` and the retrieval field
`media_sources.retrieved_at`; neither is mislabeled as source publication time. The
implemented `himr-corpus import-acquisition-result` boundary validates exact version-1
keys, types, deterministic producer IDs, hashes, timestamps, and cross-references,
maps the producer-local source ID to the catalog UUIDv5 identity, and inserts the
result in one transaction. Before doing so it re-stats and streams SHA-256 over the
current admitted file, then stats it again; a missing, replaced, or tampered payload is
rejected. It creates no publication decision. Acquisition itself never opens SQLite.

## Consequences

- Corrupt, partial, oversized, or unexpectedly changed media cannot become canonical.
- Identical bytes from different public sources converge on one media object while
  retaining separate source relationships.
- Direct HTTP can resume conservatively without trusting an arbitrary partial file.
- Public yt-dlp acquisition is reproducible enough to audit, but exact remote bytes can
  still change unless an expected hash is known.
- The host or future worker supervisor must still control network egress, cross-volume
  scheduling, wall-clock time, and trusted executable distribution.
- Authentication-dependent and access-controlled media require a separate policy
  decision and are intentionally unsupported.
