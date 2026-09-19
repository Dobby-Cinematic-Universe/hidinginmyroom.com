# Torrent terminal-bracket YouTube-ID reconciliation

This is a private, review-only lane for terminal bracketed YouTube IDs in the
already-imported BitTorrent file manifest. It is intentionally separate from the
[Archive.org bracket reconciler](ARCHIVE_BRACKET_RECONCILIATION.md): a torrent path
and an Archive.org per-file title are different evidence sources and never substitute
for one another.

The planner performs no network request, media acquisition, payload read, source or
recording mutation, relationship creation, merge, identity assertion, claim, event,
or publication decision. Standalone planning pins all catalogue reads to one SQLite
read transaction so concurrent imports cannot mix states. A filename-derived ID is
only a locator candidate. Direct media review and a separate adjudication would be
required before asserting that two objects contain the same recording.

## Fixed scope and exact grammar

Only files whose first raw path component exactly equals one of these four reviewed
labels are eligible:

1. `YouTube Videos`
2. `Old YouTube Livestreams`
3. `New YouTube Livestreams`
4. `New New YouTube Livestreams`

The final raw path component must end byte-for-byte with:

```text
\[[A-Za-z0-9_-]{11}\]\.(mp4|webm|ogv|mkv|mov|m4v)
```

Extension matching is ASCII case-insensitive. Bare IDs, dash suffixes, bracket IDs
followed by counters or whitespace, middle-of-name brackets, `.ia.mp4`, non-video
extensions, other top-level directories, transcript paths, and malformed strings are
rejected. Parsing the ASCII suffix from raw bytes means an unrelated malformed UTF-8
character elsewhere in a historical filename cannot change the ID. It also avoids
silently treating Unicode replacement characters as the original evidence.

This lane is deliberately not exhaustive. The same four directories also contain
243 recognized-video paths with a systematic `[ID] 480p.<extension>` shape (242
distinct grammar-valid YouTube IDs) and two terminal-ID `.m4a` paths. A separate
[read-only suffix/audio planner](TORRENT_SUFFIX_AUDIO_PLANNING.md) now gives those
shapes distinct evidence, confidence, and review-routing contracts. It has no import
or migration and does not widen this sealed grammar or migration 0018.

## Exact-input boundary

The planner takes the original `.torrent`, its reviewed discovery JSON, and the
existing private SQLite catalogue. It fails closed unless all of the following replay:

- bounded stable reads of two non-symlink regular files;
- canonical, bounded bencode with sorted unique dictionary keys, exact known record
  shapes, safe path components, a 16,384-character decoded catalogue-path cap,
  unique raw paths, and no display-decoding collision;
- SHA-256 of the complete torrent and discovery files, SHA-1 of the exact bencoded
  `info` dictionary, piece count, file count, total bytes, root name, and reviewed
  per-directory summaries;
- strict UTF-8 discovery JSON with duplicate and unknown keys rejected, public
  HTTPS provenance without URL userinfo, fragments, whitespace, or control text,
  `metadata_only_not_downloaded`, unknown rights, and no publication authority;
- the original importer combined-input digest, completed import receipt, manifest
  source, source snapshot, immutable metadata observation, and info-hash external ID;
- all torrent file sources and all immutable origin observations, not just the rows
  that happen to contain a bracketed ID, including proof that every file source was
  created by that exact torrent import.

The exact raw path component bytes are retained as Base64 plus a length-framed path
SHA-256. The deterministic replacement-decoded path is retained only to bind the
candidate back to the historical importer projection. Candidate evidence also fixes
the torrent/discovery hashes, info hash, original import batch, file index, directory,
byte count, parsed video ID, and current exact native-source resolution state. It
never contains transcript text or wording from a media payload.

## Resolution states

Every eligible file produces one source-to-locator candidate:

| Exact catalogue lookup | Candidate target |
| --- | --- |
| No deterministic `(youtube, youtube_video, ID)` source | Literal `youtube_video_id`; `missing_native_source` |
| Exact source does not map exactly and only to canonical `youtube:video:ID` | Exact source; `native_source_without_unique_recording` |
| Exact source maps only to canonical `youtube:video:ID` | Exact source, with canonical recording retained in typed evidence; `unique_native_recording` |

Even the third state is not a match or duplicate finding. No torrent payload was
read, so content identity cannot be inferred. Raw and calibrated scores remain null,
calibration is `not_calibrated`, human review is mandatory, and relationship,
merge, and publication assertions are fixed false.

## Read-only preview

Run the planner before any migration or admission. It opens SQLite in read-only,
query-only mode and prints only aggregate counts unless `--full` is explicitly used:

```bash
PYTHONPATH=corpus/src python3 -m himr_corpus \
  plan-torrent-bracket-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --torrent "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1pqdsxm/discovery.json"
```

The complete private plan conforms to
[`torrent-bracket-reconciliation-plan.schema.json`](../schemas/torrent-bracket-reconciliation-plan.schema.json).
`--full` may be used for a local schema/review workflow, but the output contains raw
manifest path evidence and belongs only in ignored private research storage.

## Optional future candidate admission

Migration `0018_torrent_bracket_reconciliation.sql` is forward-only and checksummed.
It adds:

- `torrent_bracket_reconciliation_imports`, the exact input/plan/count receipt; and
- `torrent_bracket_youtube_candidates`, an append-only private subtype of
  `match_candidates` with one matching review task.

Admission triggers independently bind each row to the original torrent import,
manifest/file source, immutable source observation, generic candidate, review task,
raw evidence flags, and exact YouTube source/recording when present. They reject a
score, accepted state, relationship assertion, merge flag, non-private visibility,
publication authority, or direct publication decision. Candidates can be admitted
only while their import batch is running; the batch cannot seal until the declared
typed-candidate and distinct-review-task counts are complete, and a sealed batch
cannot reopen. The manifest/file source identity columns bound by an admitted row
also become immutable, while later append-only metadata observations remain allowed.
These tables feed no public view.

After reviewing the plan, migration, private backup, and capacity implications, the
candidate-only import command is:

```bash
PYTHONPATH=corpus/src python3 -m himr_corpus \
  import-torrent-bracket-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --torrent "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1pqdsxm/discovery.json"
```

The current milestone intentionally did **not** run that command against the live
catalogue. In disposable tests, a second exact import verifies every receipt,
candidate, typed raw-evidence row, and review-task identity; it neither repairs nor
overwrites tampered evidence.

## 2026-08-27 live dry-run

The read-only planner scanned the exact existing inputs and returned plan
`tbrp_93b6c0d75e751a9b672a69ff823a4661` with SHA-256
`93b6c0d75e751a9b672a69ff823a46610fc69a681890ea393a8b8b82a0dc4aab`.
It observed:

| Measure | Count |
| --- | ---: |
| Complete provider file records checked | 4,719 |
| Files in the four reviewed directories | 1,462 |
| Recognized video-extension files in scope | 1,448 |
| Strict terminal-bracket candidates | 1,202 |
| Distinct candidate YouTube IDs | 1,202 |
| Missing exact YouTube source | 1,189 |
| Exact YouTube source plus one canonical recording | 13 |
| Exact source without one canonical recording | 0 |

Directory candidate counts were 1,147 `YouTube Videos`, 53 `Old YouTube
Livestreams`, one `New YouTube Livestreams`, and one `New New YouTube Livestreams`.
Pre/post SHA-256 checks of the live SQLite database and WAL were identical. No
migration, candidate import, transcript access, payload access, or download occurred.

A logical SQLite backup then provided a disposable admission canary. Migration 0018
applied there; first import added exactly 1,202 generic candidates and 1,202 review
tasks, exact replay returned the same result, database validation passed with 1,202
typed rows, and source, recording, source-mapping, source-relation,
recording-relation, and publication-decision counts all stayed unchanged. The canary
directory was deleted automatically.
