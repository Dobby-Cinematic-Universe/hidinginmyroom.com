# Private Reddit citation-media handoff

This lane admits the exact media bodies preserved by the frozen
`research/reddit-snapshots/2026-08-26` citation snapshot. It is intentionally not a
general Reddit downloader or a claim importer. The fixed boundary contains 17
captures attached to 14 cited targets across 13 posts:

- 10 video-only MP4 files containing one H.264/yuv420p/30fps stream, with a total
  duration of 1,482,600 ms;
- 7 JPEG still images, cataloged as `media_kind = image` even though FFprobe exposes
  each JPEG as a one-frame MJPEG video stream.

The command revalidates 45 relevant raw gzip artifacts in total: 14 oEmbed JSON
responses, 14 embed-page HTML responses, and the 17 declared-media responses. Only
the 17 media bodies are materialized into the handoff bundle.

The materializer requires the exact frozen provenance SHA-256
`325aa22d783ccc3a591c57242cad6bb9d82a04c3e0168e018ac17c57c1e9e508` and
citation-inventory SHA-256
`4da9d184ac72de45a1bffc8f7c04b3509d31a1efdc6214d4dcb22b1d7d380f58`.
Every declared-media, oEmbed JSON, and embed-page HTML gzip used by the 14 admitted
contexts is opened without following its final
path component. Each input must be a single-link regular file, is checked against the
pre-open path identity, retained by descriptor, and rehashed before close. Requested
and resolved paths must still identify that same inode at closing verification. Gzip
validation
requires exactly one complete member, a valid CRC, no trailing bytes, and exact
compressed and uncompressed sizes and hashes. The pinned FFprobe 8.1.2 binary has
SHA-256 `b0303d039d7768418bb3746053b8fea88190df8ae18cb63bf032948c0af04feb`.

The checked-in snapshot files are owner-writable. That is refused by default. Their
known state can be processed only with `--guard-writable-inputs`, which is an explicit
detect-and-fail guard: it holds every input descriptor and repeats identity, metadata,
and full-byte hashing before returning or committing. It does not make same-UID files
immutable.

## Materialize and validate

Choose a private, durable output directory that does not already exist:

```sh
PYTHONPATH=corpus/src python3 -m himr_corpus \
  materialize-reddit-citation-media-handoff \
  --snapshot-dir research/reddit-snapshots/2026-08-26 \
  --out-dir research/corpus/reddit-citation-media-handoff/2026-08-26 \
  --guard-writable-inputs

PYTHONPATH=corpus/src python3 -m himr_corpus \
  validate-reddit-citation-media-handoff \
  --snapshot-dir research/reddit-snapshots/2026-08-26 \
  --manifest research/corpus/reddit-citation-media-handoff/2026-08-26/handoff-manifest.json \
  --guard-writable-inputs
```

Materialization is atomic and no-replace at the output-directory boundary: an empty
destination that appears during processing is not overwritten. The Linux
`renameat2(RENAME_NOREPLACE)` primitive is required; the command fails closed when it
is unavailable. Media paths are
content-addressed under `media/sha256/<prefix>/<sha>.<ext>`. Files and the manifest
are mode `0400`; directories are mode `0700`. Validation independently reopens the
frozen snapshot and the materialized bundle, recomputes every hash and probe, and
requires the manifest to be an exact derivation of those inputs.

Mode `0400` prevents accidental file writes, but an owner-writable parent directory
does not provide long-term immutability against the same UID. Closing verification
detects path replacement during one command; retain the bundle in an appropriately
controlled private location and rerun validation before import or later use.

## Candidate-only catalog import

Preview against a disposable database first. The project intentionally does not run
this command against the live catalog automatically:

```sh
PYTHONPATH=corpus/src python3 -m himr_corpus \
  import-reddit-citation-media-handoff \
  --db /tmp/himr-reddit-citation-review.sqlite3 \
  --snapshot-dir research/reddit-snapshots/2026-08-26 \
  --manifest research/corpus/reddit-citation-media-handoff/2026-08-26/handoff-manifest.json \
  --guard-writable-inputs
```

One exact import creates only:

- 14 unreviewed Reddit post/comment context sources and 17 unreviewed declared-media
  sources;
- 31 exact source snapshots, including the precise source snapshot referenced by
  every `media_sources` row;
- 17 `candidate` context-to-media source relations;
- 17 verified media objects and 17 private, content-addressed, read-only locations;
- 51 open private tasks: context review, OCR routing, and visual routing for each
  media object.

The importer protects every other table with transaction-local write-rejection
triggers plus an independent in-transaction count audit. This catches an in-place
write made by a pre-existing database trigger, not only inserted or deleted rows.
Completed replay rechecks the exact batch/observation, source snapshots and metadata,
candidate relations and observations, media rows, lineage, and task identities. It is
byte-idempotent for the same sealed handoff at the same retained paths. It creates no recordings, renditions,
transcripts, claim/evidence links, entity or event assertions, review decisions,
publication decisions, gate clearances, or public rows.

The no-audio disposition is explicit: MP4 captures are
`not_applicable_no_audio_stream`, while JPEG captures are
`not_applicable_non_audio_media`. OCR and visual work remain
`candidate_only_not_evaluated`. A candidate relation records only that the frozen
official embed and declared-media response matched at capture time; it does not prove
the repost's authorship, completeness, editing history, depicted identity, event, or
underlying allegation.

For comment-bound images, parent-post agreement is insufficient. Admission requires
the cited comment ID to match the official oEmbed canonical target, official embed
comment record, observable-ID record, post ID, subreddit, exact gallery media URL,
declared media type, response MIME type, and body SHA-256. The raw oEmbed JSON is
independently decompressed and parsed, while the raw embed HTML must contain the
frozen post/comment IDs and observed media URL(s); those checks do not merely trust a
precomputed `semantic_match` flag.
