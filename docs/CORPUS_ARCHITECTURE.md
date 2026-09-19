# HIMR Corpus architecture

The HIMR Corpus is a static research catalogue and transcript explorer that is
deliberately separate from the editorial wiki. It exists to preserve source identity,
make machine-generated observations reviewable, and give every published transcript
segment a stable route back to its media and processing history.

## Boundaries

The project has three data boundaries:

1. **Private evidence workspace** — acquired media, response captures, working
   databases, model artifacts, face and voice embeddings, review queues, and
   credentials. This material is ignored by the public repository.
2. **Publication-safe corpus release** — sanitized source metadata, explicitly
   gated transcript revisions, timestamps, reviewed public identity labels, and
   confidence disclosures. A transcript revision may be unreviewed machine output;
   its machine status and non-quotation disclaimer travel with every segment. The
   site consumes only this deterministic export.
3. **Editorial wiki** — human-reviewed prose and claims. Corpus results may locate
   evidence, but they do not edit or verify wiki claims automatically.

Raw media and mutable databases do not belong in Git. The private database is the
catalogue of record; checked static exports are immutable publication artifacts.

## Current starting point

The preserved 2026-08-25 inputs contain 2,227 legacy transcript-index records, of
which 2,043 map to 1,922 distinct selected Internet Archive files and 184 have no
mapped video. The two raw Internet Archive metadata responses contain a broader
inventory of 3,112 video-file records. These counts describe records and renditions,
not a proven number of distinct recordings.

Legacy titles, date labels, source mappings, reconciliation notes, and audit flags are
useful catalogue leads. Legacy transcript wording, machine summaries, speaker labels,
and transcript-derived timing are not imported as new transcripts.

## Identity model

The catalogue keeps these concepts separate:

- A **source** is a platform-visible object such as a YouTube page, Archive.org file,
  or Reddit post.
- A **recording** is a conceptual continuous recording that can have several sources,
  chunks, mirrors, or renditions.
- A **media object** is an exact acquired byte sequence identified by a locally
  computed SHA-256.
- A **rendition** relates a source or media object to a recording.
- A **segment** uses a half-open millisecond range, `[start_ms, end_ms)`.
- An **observation** is a revisioned machine or human annotation over a segment.
- An **entity** is a public-safe character, account, channel, place, or topic.
- An **identity assertion** is a reviewed association between a private machine
  cluster and a public entity. Clusters are not identities by themselves.

Native platform identifiers and deterministic catalogue IDs outrank titles,
filenames, handles, and dates. External legacy and claim-ledger IDs are aliases, not
assumed foreign keys.

## Processing levels

Every artifact is immutable and records its input hashes, implementation revision,
model and weights revision, parameters, environment, glossary revision, and run ID.

- **Bronze** — untouched machine output.
- **Silver** — normalized, reconciled, and calibrated output.
- **Gold** — human-corrected or media-checked output with an append-only review
  decision.

Raw ASR, contextual ASR, human-verbatim corrections, and readability edits are
different transcript revisions. One never overwrites another.

Confidence is task-specific. ASR, alignment, diarization, active-speaker association,
face or voice matching, OCR, and action classification each retain their own raw
score, calibration set, calibrated probability, quality flags, and review state.
Human verification is a state, not a probability.

## Resource-aware pipeline

All usable media receives hashing, fingerprints, audio extraction, voice-activity
detection, ASR, alignment, shot detection, and a sparse OCR scan. Full diarization is
reserved for likely multi-speaker recordings. Face tracking and active-speaker
detection run only on speech scenes with usable faces. Dense OCR, secondary ASR, and
action or sound analysis are priority-interval jobs.

At corpus scale, immutable evidence and operational scheduling use separate planes.
The accepted design keeps receipts authoritative and adds a rebuildable,
content-addressed checkpoint so normal resume can eventually validate only the
receipt tail instead of re-hashing the complete historical media set. The first
implementation reuses a deep-validated snapshot only within one producer invocation;
durable cross-process checkpoints and stable global hot CAS roots remain follow-up
work. ASR-ready audio extraction is separated from deferred proxy, routing, OCR,
diarization, and visual enrichment so visual work cannot starve the transcript lane.
See
[ADR 0015](adr/0015-scalable-evidence-plane-and-stage-separation.md).

Routine finite work may be launched through a separate loopback-only operator
console. It accepts only pre-registered, owner-reviewed profiles and treats its job
history as a rebuildable observation; stage results and receipts remain authoritative.
The console is not part of Astro or the public release and cannot bypass currently
blocked GPU, catalogue-admission, cold-storage, publication, or identity contracts.
See [ADR 0016](adr/0016-local-operator-console.md).

A one-speaker result normally begins as `unknown_single`. For a source already
confirmed to be Daniel's own recording, one audible or visible speaker may be routed
under a documented `presumed_daniel_solo` rule unless the title, playback, or context
indicates otherwise. That shortcut saves computation; it is not face recognition and
does not authorize a public identity assertion. Multi-person, guest, reaction,
playback, synthetic, and contradictory cases still require direct review. Cross-video
face and voice clusters remain private and pseudonymous until a reviewer accepts an
appropriate public identity anchor. Unknown people remain unknown.

Routing still does not persist a named speaker. The separate migration `0032`
private solo-voice lane can bind one exact `rendition_media_ms` interval to a named
entity only after a human listens to the complete interval and an independent human
clears the personal-data/biometric risk. It expressly excludes source/channel
context, transcript text, machine identity output, machine confidence, overlap,
playback, TTS, and synthetic or unknown-origin audio. That decision remains private
and has no static export path. See [ADR 0012](adr/0012-private-solo-voice-attestations.md).

## Static publication and search

The existing Starlight wiki index remains at `/pagefind/`. Standalone corpus routes
under `/corpus/` never emit `data-pagefind-body` and carry
`data-pagefind-ignore="all"` as an additional guard.

A separate set of bounded Pagefind bundles is generated beneath `/corpus/pagefind/`
from publication-safe custom records. `/corpus/search-manifest.json` declares every
bundle, and the browser merges their scored results. Transcript records deep-link to
exact timestamps using stable revision and segment IDs. The main wiki never merges
these indexes.

The active release is a small, deterministic v2 manifest. It commits by SHA-256 and
byte count to catalog-summary shards of at most 1,000 recordings (250 by default),
and each summary commits to one full recording/transcript shard. Landing and browse
routes never load transcript text; a video route and the indexer load and verify one
detail shard at a time. Export installs and validates the immutable shard tree before
atomically replacing the active manifest. See
[ADR 0004](adr/0004-sharded-corpus-release.md) and the
[scale benchmark](CORPUS_SCALE_BENCHMARK.md).

The reviewed entity/event graph is a second, isolated static release beneath
`src/data/corpus/graph/`; it does not extend recording schemas v1 or v2. Every root
and edge needs an independent complete human review, publication decision, and all
three current human-reviewed gates. Named appearances and event evidence additionally
need direct-media review and a reviewed public source/recording/rendition anchor.
Published edge times explicitly use `rendition_media_ms`. The strict graph loader
checks its content-addressed shard, references, real calendar dates, and acyclic event
relations before any `/corpus/entities/` or `/corpus/events/` route is generated. The
checked release is empty until objects independently satisfy the complete contract.
See [ADR 0011](adr/0011-public-entity-event-graph-release.md).

The release `generated_at` value is derived only from publication decisions, current
gate decisions, and lifecycle decisions attached to objects that actually appear in
that release. A later decision about private-only or currently suppressed material
does not leak its timing through the public manifest.

The recording release contains a source catalogue and may add rights-cleared machine
or reviewed transcript revisions and OCR. The isolated graph release can expose
reviewed public entities and evidence-bearing event edges without broadening that
contract. Eligible machine transcripts are published by
the normal release lane without a wording review. Each revision exposes whether it
is machine-generated and unreviewed, states that it is not a verified quotation, and
remains separate from reviewed revisions. No release process chooses a silent
"best" revision; every non-retracted published revision remains independently
searchable. The site build reads only the checked static release; it never opens the
private database.

Transcript rows, segments, words, parent links, reviews, corrections, and lifecycle
decisions are append-only. A human can mark a revision disputed, retracted, or
reinstated through a separately explained lifecycle decision. Retraction removes the
text from current search and immediately exposes only a text-free public tombstone
when its gates permit one; it never rewrites the historical private evidence. A
retracted revision can return only through explicit reinstatement, and a transcript
publication `remove` cannot substitute for that human lifecycle record. See
[ADR 0010](adr/0010-machine-transcript-publication-and-lifecycle.md).

## Wiki bridge

The corpus can create an editorial review task, not a wiki conclusion:

1. Search locates a candidate segment.
2. A reviewer opens the underlying media and enough surrounding context.
3. The reviewer records what was listened to or watched and creates an atomic claim.
4. The wiki cites the original public source and may deep-link to the corpus segment.
5. Validation confirms that the claim, source, transcript revision, and locator still
   exist in the published release.

The project preserves contradictions and transcript corrections instead of silently
rewriting history.
