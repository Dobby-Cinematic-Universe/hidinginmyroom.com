# HIMR Corpus roadmap

This roadmap is organized around exit gates rather than calendar promises. Source
discovery and backfill remain ongoing even after the first usable corpus release.

## Phase 1 — catalogue foundation

- Create checksummed, forward-only database migrations.
- Import all preserved Archive.org file metadata and the legacy index as separate
  provenance lanes.
- Import the current-channel inventory and stable YouTube IDs.
- Create deterministic source, recording, rendition, and external-ID records.
- Prove import counts, idempotency, database integrity, and that the legacy import
  creates zero transcript revisions.
- Generate a deterministic, deny-by-default public metadata release.

Exit gate: the initial source universe is queryable without importing legacy machine
text, and every published record has a stable source URL and provenance basis.

## Phase 2 — acquisition and deduplication

- Add Archive.org, local-file, public YouTube/yt-dlp, and curated Reddit adapters.
- Stage downloads, probe them with `ffprobe`, compute SHA-256, and atomically admit
  valid objects to content-addressed storage.
- Record exact duplicates, transcodes, mirrors, chunks, excerpts, compilations, and
  response or guest-appearance relations.
- Add audio fingerprints and sparse visual hashes for clip-to-parent matching.
- Maintain short-job and long-stream queues with resumable chunks.

Exit gate: a representative acquisition can be repeated without duplicate downloads,
and no secret, signed URL, private path, or access-controlled asset enters a public
export.

## Phase 3 — transcript vertical slice

- Select 12 varied public recordings and freeze a one-hour evaluation subset.
- Normalize audio, run voice-activity detection, baseline multilingual ASR, and word
  alignment.
- Add a versioned HIMRverse glossary and a contextual second pass for uncertain spans.
- Build a synchronized correction view, immutable transcript revisions, and an
  append-only human dispute/retraction/reinstatement lane.
- Measure wall time, CPU/GPU time, storage, word error, name error, alignment error,
  and human-review time.

Exit gate: the pilot produces searchable word timestamps, reproducible run manifests,
a published evaluation report, and machine-output disclaimers that survive export,
search indexing, and deep links without requiring a wording review.

## Phase 4 — speakers and vision

- Route likely solo recordings through a sampled fast path.
- Add overlap-aware diarization for likely multi-speaker media.
- Track faces only in relevant speech scenes and compare active-speaker models.
- Create private face and voice clusters with human-reviewed public identity
  assertions.
- Keep playback, reaction inserts, TTS, synthetic media, and off-screen voices
  distinct.

Exit gate: uncertain speakers and faces remain unknown, every named appearance has a
review decision, and no biometric artifact is public.

## Phase 5 — OCR and bounded enrichment

- Detect shots and text-bearing regions, then run multilingual OCR selectively.
- Track and deduplicate captions, chat, and moving overlays across frames.
- Add candidate entity mentions, appearances, events, and guest recordings with exact
  source intervals.
- Trial a small vocabulary of sound and action candidates only after the core pipeline
  is stable.

Exit gate: OCR and enrichment results retain exact media coordinates, task-specific
confidence, privacy status, and a route to human review.

## Phase 6 — static explorer and wiki loop

- Publish the independent `/corpus/` catalogue and transcript search.
- Add recording, source, entity, event, timeline, and graph routes.
- Validate search-index isolation from the wiki.
- Add the reviewed claim bridge and stable corpus timestamp links.
- Publish correction, human-only transcript lifecycle, removal, model, schema,
  coverage, and release histories.

Exit gate: every search result resolves to a valid static route and source locator;
every wiki-linked result is human reviewed under the editorial policy.

## Backfill priorities

1. Current public uploads and sources already cited by the wiki.
2. Unique or at-risk guest appearances and event sources.
3. Short Archive.org recordings.
4. Medium recordings.
5. Livestreams over two hours, in resumable chunks.
6. The 184 legacy records without mapped media.
7. Reddit clips whose parent recording remains unresolved.

The status dashboard reports records, hours, storage, failures, and review backlog by
stage. It does not present one misleading project-complete percentage. The first
private aggregate data source is now implemented as the deterministic,
catalogue-hash-pinned `coverage-snapshot` command. Sealed public-only acquisition
bundles now also have a bounded, sequential, resumable runner with strict completed-
result replay, but its summaries and the artifact backlog still need their own sealed
dashboard reconciliation before this becomes the complete multi-input view described
here.
