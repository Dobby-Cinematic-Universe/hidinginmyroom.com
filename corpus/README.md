# HIMR corpus foundation

Public Reddit post and clip discovery is documented in
[the acquisition Atom guide](../acquisition/REDDIT_RSS.md). Its importer adds only
unreviewed sources, contextual relations, recording candidates, and review tasks;
public exports remain fail closed.

The small frozen set of exact media bodies attached to the 2026-08-26 Reddit citation
snapshot has a separate sealed, candidate-only path. It authenticates and
content-addresses those files without creating recordings, claims, transcripts, or
publication decisions. See
[Private Reddit citation-media handoff](docs/REDDIT_CITATION_MEDIA_HANDOFF.md).

Exact public Archive.org item-metadata snapshots and deltas are documented in
[the Archive.org metadata guide](../acquisition/ARCHIVE_ORG_METADATA.md). The lane
uses only unauthenticated `/metadata/<identifier>` requests, stores its evidence under
ignored `research/` paths, and treats every provider field as unreviewed.

This package builds a reproducible, private SQLite catalog and a separate static,
publication-safe corpus release. It uses only the Python standard library. Raw media,
embeddings, source snapshots, and the working database remain outside the public site.

The legacy transcript archive is imported only as historical source metadata. Its
machine summaries, speaker guesses, and transcript text never become transcript
revisions. A public transcript revision must be separately created in ordinary
recording coordinates, explicitly approved for publication, and clear all three
policy gates. Its wording does not require human review: machine revisions publish
with an unreviewed/non-quotation disclaimer and remain separate from any later human
revision. The closed, digest-authorized initial-publication workflow is documented in
[Machine-transcript default publication](docs/MACHINE_TRANSCRIPT_PUBLICATION_POLICY.md).
For a first release that must not open or mutate the active SQLite catalog, use the
[owner-private reviewed-release staging path](docs/REVIEWED_RELEASE_STAGING.md).

Catalog-free short-clip ASR can be retained in a private, append-only media-coordinate
lane without inventing recording timestamps. See
[Private media-local ASR admission and search](docs/MEDIA_LOCAL_ASR.md).
Completed sealed faster-whisper GPU v3 results have a separate digest-gated bridge
into that same private coordinate system; see
[Private faster-whisper GPU v3 admission](docs/FASTER_WHISPER_GPU_V3_ADMISSION.md).
The exact neutral-glossary pilot has a separate digest-gated competing-revision lane;
see [Private paired contextual media-local ASR](docs/CONTEXTUAL_MEDIA_LOCAL_ASR.md).
Exact zero-offset full-file hypotheses can enter ordinary recording coordinates only
through the digest-gated, no-authority identity transform documented in
[Full-file media-local transcript projection](docs/MEDIA_LOCAL_TRANSCRIPT_PROJECTION.md).

## Quick start

From the repository root:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-bundle \
  --db research/corpus/corpus.sqlite3 \
  --archive-dir research/archive-snapshots/2026-08-25 \
  --channel-dir research/channel-snapshots/2026-08-25 \
  --approve-public-metadata

PYTHONPATH=corpus/src python -m himr_corpus validate \
  --db research/corpus/corpus.sqlite3

PYTHONPATH=corpus/src python -m himr_corpus export-sharded \
  --db research/corpus/corpus.sqlite3 \
  --out-dir src/data/corpus
```

`status` and `validate` are strictly audit-only. They never install migrations and
fail closed if the on-disk migration set is pending, missing, renamed, noncontiguous,
or hash-mismatched. They also require a closed, checkpointed catalog with no `-wal`,
`-shm`, or `-journal` sidecar, then open the main database through SQLite's immutable
read path so inspection creates no files or timestamps. Run the explicit `migrate`
command under writable administrative control before auditing a pending schema; never
discard a nonempty WAL to satisfy this precondition.

For deterministic backlog reporting while later migrations are still pending, use
the aggregate-only `coverage-snapshot` command against an explicitly hashed,
mode-`0400`, sidecar-free checkpoint. It reports the pending migration names but
never installs them and emits no titles, locators, transcript/OCR text, person labels,
claims, or review reasons. See
[Private catalogue coverage snapshot](docs/COVERAGE_SNAPSHOT.md).

Additional discovery importers accept arbitrary Internet Archive item metadata,
yt-dlp search candidates, validated yt-dlp info JSON, Archive.org URL-hint lists, and
BitTorrent manifests. Candidate and torrent-file rows remain unreviewed and are not
allowlisted by the public-metadata policy.

Imports retain immutable candidate assertions and recompute current source/recording
metadata by an explicit evidence-quality, chronology, and deterministic tie-break
policy. Reobserving identical input later is meaningful; replaying the same input at
the same instant is idempotent. See
[Metadata observation and precedence policy](docs/METADATA_PRECEDENCE.md).

Validated yt-dlp captures can be imported deterministically from repeated files or
a non-recursive directory input:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-ytdlp-info \
  --db research/corpus/corpus.sqlite3 \
  --info research/corpus/discovery/youtube/search-himr-daniel-lord/metadata \
  --observed-at 2026-08-26T18:30:00Z
```

Validation confirms platform metadata, not HIMR relevance. Third-party/search videos
therefore remain withheld until an explicit human publication decision.

Validate and transactionally import one complete sealed Archive.org metadata
snapshot with the dedicated private-catalog boundary:

```sh
himr-corpus validate-archive-metadata-snapshot \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"

himr-corpus import-archive-metadata-snapshot \
  --db "$PWD/research/corpus/corpus.sqlite3" \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
```

Neither command downloads media, makes an identity assertion, authorizes
publication, or clears a publication gate.

Terminal bracketed YouTube-ID suffixes in an admitted Archive.org snapshot have a
separate, review-only reconciliation lane. Preview it before admission; it never
merges recordings or changes sources:

```sh
himr-corpus plan-archive-bracket-reconciliation \
  --db "$PWD/research/corpus/corpus.sqlite3" \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
himr-corpus import-archive-bracket-reconciliation \
  --db "$PWD/research/corpus/corpus.sqlite3" \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
```

See [Archive.org bracketed YouTube-ID reconciliation](docs/ARCHIVE_BRACKET_RECONCILIATION.md)
for the exact suffix grammar, ambiguity handling, immutable evidence contract, and
replay procedure.

The retained `1q2sk8g` Archive complement has a distinct late-provider reconciliation
lane. It emits a sealed 221-candidate private plan and requires that exact plan digest
for candidate-only admission, while preserving every hint and historical external-ID
observation. See
[Archive URL-hint/provider-source reconciliation](docs/ARCHIVE_HINT_PROVIDER_RECONCILIATION.md).

The already-imported BitTorrent manifest has a separate terminal-bracket lane with a
fixed four-directory scope and raw-path-byte provenance. Its default command is a
read-only aggregate preview; torrent payloads and transcript wording are never read:

```sh
himr-corpus plan-torrent-bracket-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --torrent "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1pqdsxm/discovery.json"
```

See [Torrent terminal-bracket YouTube-ID reconciliation](docs/TORRENT_BRACKET_RECONCILIATION.md)
for its strict bencode/discovery replay boundary, plan schema, exact live dry-run
counts, and candidate-only migration. The live candidate import remains intentionally
unrun.

Two disjoint filename shapes excluded by that sealed grammar have their own
read-only, plan-only command. It preserves separate evidence/confidence/routing for
`[ID] 480p.<video extension>` and `[ID].m4a` locators and offers no import path:

```sh
himr-corpus plan-torrent-suffix-audio-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --torrent "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1pqdsxm/discovery.json"
```

See [Torrent format-label video and audio-only locator planning](docs/TORRENT_SUFFIX_AUDIO_PLANNING.md)
for the exact byte grammars, rejection rules, strict schema, and live aggregate counts.

The two reviewed video grammars also feed one strictly read-only selective-acquisition
planner. It excludes exact sealed Archive.org/catalogue locators, chooses the smallest
rendition per remaining ID, and selects no file index until a complete credential-free
yt-dlp no-download probe reports an explicit private, removed, or video-unavailable
state. Sign-in gates remain indeterminate. It never invokes a torrent client:

```sh
PYTHONPATH=corpus/src python -m himr_corpus plan-torrent-selective-acquisition \
  --db "$PWD/research/corpus/private-catalog-backups/CATALOG.sqlite3" \
  --torrent "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1pqdsxm/discovery.json" \
  --archive-snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/SNAPSHOT/snapshot.json"
```

See [Selective torrent acquisition](../docs/TORRENT_SELECTIVE_ACQUISITION.md) for the
atomic resumable producer, probe contract, exact 2026-08-27 upper bound,
malformed-path review lane, and the selective-client/piece-boundary prerequisites.

Completed acquisition, media-preprocessing, offline whisper.cpp, sparse-frame,
audio-fingerprint, and sparse visual-fingerprint results have separate strict
transactional handoff commands:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-acquisition-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/acquired/jobs/<job>/<recipe>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-preprocess-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/processed/media/sha256/<sha>/runs/<recipe>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-local-window-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/windows/<parent>/<bundle>/<window>/result.json \
  --observed-at 2026-08-26T23:40:15Z

PYTHONPATH=corpus/src python -m himr_corpus import-asr-whispercpp-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/asr/whispercpp/sha256/<prefix>/<sha>/results/<key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-sparse-frame-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/vision/sparse-frames/sha256/<prefix>/<sha>/results/<key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-audio-fingerprint-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/fingerprints/.../executions/<run>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-audio-fingerprint-compare-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/fingerprint-comparisons/.../executions/<run>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-visual-fingerprint-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/vision/visual-fingerprints/.../results/<key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-visual-fingerprint-compare-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/vision/visual-fingerprint-comparisons/.../results/<key>/result.json
```

The boundary rejects planned/failed output, unknown contract fields, inconsistent
cross-references, and non-private machine artifacts. It maps acquisition's
producer-local source ID onto the catalog UUIDv5 identity and is idempotent under
reimport. ASR admission also requires pre-registered exact input media/artifact,
model, optional glossary, and recording/rendition context; null context never creates
a recording or transcript. Visual admission independently recomputes every 64-bit
pHash from its sealed 32x32 grayscale evidence and always records it as uncalibrated,
private, human-review-only routing evidence. Comparison admission additionally
recomputes every selected pair and preserves a below-threshold result as a private
measurement with no generic candidate or review task. See
[Result-envelope ingestion](docs/RESULT_INGESTION.md)
for the exact time, rendition, transcript, routing-observation, and publication
semantics, and [Private model-registry manifests](docs/MODEL_REGISTRY.md) for the
required checksummed model-registration workflow. Sparse-frame admission and its
strictly non-OCR semantics are documented in
[Private sparse-frame result ingestion](docs/SPARSE_FRAME_RESULT_INGESTION.md).
Completed Tesseract results then have a separate digest-reviewed, private-only,
redaction-pending admission and FTS lane; see
[Private Tesseract OCR admission and search](docs/OCR_TESSERACT_RESULT_INGESTION.md).
Sealed long-recording derivatives use a distinct admission/verification run because
local-window result v1 has no producer execution timestamp; see
[Private local-window result ingestion](docs/LOCAL_WINDOW_RESULT_INGESTION.md).
Completed local-window ASR has a separate text-free plan and digest-gated private
admission path that keeps timestamps in rendition media coordinates and supplies FTS
search without inventing recording time; see
[Private rendition-local ASR admission and search](docs/RENDITION_LOCAL_ASR.md).

Publication intent and the independent rights, privacy, and sensitivity clearances
use a strict private-manifest workflow. Validate or dry-run the entire manifest before
its one-transaction append; source and result importers never clear these gates. See
[Publication administration manifests](docs/PUBLICATION_ADMIN.md).

After human reviewers have independently cleared those three gates, eligible
ordinary-coordinate machine transcripts can receive their initial publication row
without a wording review through the separate closed policy. Plan it through an
immutable audit connection and apply only the reviewed digest; install migrations in
a separate command first:

```sh
PYTHONPATH=corpus/src python -m himr_corpus plan-machine-transcript-publication \
  --db research/corpus/corpus-v8.sqlite3

PYTHONPATH=corpus/src python -m himr_corpus apply-machine-transcript-publication \
  --db research/corpus/corpus-v8.sqlite3 \
  --expected-plan-sha256 <reviewed-plan-sha256>
```

This v1 lane excludes named-speaker segments, lifecycle history, and every revision
with an existing human or policy publication stream. It cannot clear gates, review
wording, correct text, or dispute, retract, or reinstate a transcript. See
[Machine-transcript default publication](docs/MACHINE_TRANSCRIPT_PUBLICATION_POLICY.md)
for the exact scope, warning, replay, and failure semantics.

Reviewer identities and active state use a separate, append-only administration
manifest. New reviewers are always registered inactive and require a later explicit
activation event. See [Reviewer administration](docs/REVIEWER_ADMIN.md) before
creating a reviewer or changing publication authority.

Private entity, alias, appearance, and event research has a separate source-anchored
manifest workflow. It requires current source/recording/rendition tuples, bounded
half-open intervals, reviewed transcript revisions when cited, explicit unverified-
claim semantics, and privacy review for sensitive private-person handles. It creates
review tasks but no identity assertion or publication clearance. See
[Private entity/event map manifests](docs/ENTITY_EVENT_MAP_ADMIN.md).

Named solo-voice timestamps have a separate private-only manifest lane. It binds an
exact reviewed source/recording/rendition/media interval, requires one human's direct
complete-interval audio attestation and a different human's privacy/biometric-risk
clearance, and rejects playback, overlap, TTS, synthetic/unknown audio, source or
channel shortcuts, transcript wording, model identity, and confidence scores. The
default import is a dry-run; `--apply` is explicit. It creates no public label or
exportable object. Applying also requires `--expected-input-sha256` set to the exact
digest returned by validation. Conflicting identities are checked across rendition
aliases sharing the same media bytes. See
[Private solo-voice attestation administration](docs/SOLO_VOICE_ATTESTATION_ADMIN.md).

The separately reviewed public graph is an additive migration 0031 projection and
an isolated content-addressed release. It preserves recording schemas v1/v2 and emits
only independently human-reviewed, labeled, published, and three-gate-cleared graph
roots and edges. Media edges also require direct-perception review and reviewed
public source/recording/rendition anchors; their times are explicitly rendition-media
milliseconds. Graph v1 excludes transcript-backed evidence and all aliases, private
provenance, observations, paths, biometrics, and machine identity assertions. Build
and validate it separately:

```sh
PYTHONPATH=corpus/src python -m himr_corpus export-graph \
  --db research/corpus/corpus.sqlite3 \
  --out-dir src/data/corpus/graph

PYTHONPATH=corpus/src python -m himr_corpus validate-graph-release \
  --manifest src/data/corpus/graph/manifest.json
```

The tracked graph release is intentionally empty; tests use fictional synthetic
fixtures only. See [ADR 0011](../docs/adr/0011-public-entity-event-graph-release.md).

Run the tests with:

```sh
PYTHONPATH=corpus/src python -m unittest discover -s corpus/tests -v
```

## Boundary

The SQLite catalog is the system of record for private catalog state.
`src/data/corpus/manifest.json` and its content-addressed v2 shards are a generated,
deny-by-default projection. The
exporter reads only `public_*` views,
which require an explicit current `publication_decisions` row plus current `clear`
decisions for all three publication gates. Public metadata approval does not approve
video contents, identity matches, OCR, transcripts, rights, privacy, or sensitivity.

V2 installs and validates a complete temporary hash tree before atomically replacing
the active manifest. `export --out .../release.json` remains available only as a
strict v1 migration/debug format; the public boundary permits exactly one active
format. See [ADR 0004](../docs/adr/0004-sharded-corpus-release.md).
