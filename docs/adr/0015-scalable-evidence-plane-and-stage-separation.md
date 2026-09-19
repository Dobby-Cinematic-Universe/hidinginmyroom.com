# ADR 0015: Scalable evidence plane and stage separation

- Status: accepted
- Date: 2026-08-29

## Context

The first end-to-end GPU epoch proved that local ASR is no longer the dominant
cost. The resident-model pilot transcribed 793.913 seconds of audio in 45.190
seconds, including 19.290 seconds of CUDA inference. The current full preprocessing
profile, however, decodes every video three times: once for 16 kHz mono FLAC, once
for a review proxy, and once for scene/silence routing. Projected over the known
duration-bearing catalogue, that full profile is the approximately 10.2-day critical
path.

Restart validation also does more work as the corpus grows. Queue validation reopens
and hashes every completed media payload; several foreground commands perform that
same scan more than once. One-item preprocess bundles similarly replay all selected
inputs and completed artifacts around each invocation. Used as the only scheduler
state, correct immutable receipts therefore lead to quadratic aggregate I/O.

There are two additional handoff defects:

- completed GPU-v3 media-local transcripts have no engine-neutral private catalogue
  admission path, so useful sealed text remains outside private FTS; and
- the existing GPU wrappers name expected hashes for preserved Python sources but
  import those sources by mutable pathname before proving the bytes. Existing v1-v3
  results remain evidence and cannot be repaired in place.

## Decision

Keep immutable results and receipts as the authority, but add a separate rebuildable
operational evidence plane and split latency-sensitive work from deferred enrichment.

### 1. Two replay depths

Every worker exposes two explicit validation modes:

- **operational replay** verifies a content-addressed checkpoint, its append-only
  receipt tail, the next input, free space, and the exact result being consumed;
- **deep audit** independently re-hashes every referenced byte and rebuilds the
  checkpoint from authoritative receipts.

An operational checkpoint is never publication, identity, catalogue, or deletion
authority. It contains only stage/job identities, exact input and result digests,
byte counts, completion state, and the preceding checkpoint digest. If it is absent,
inconsistent, or ahead of the immutable receipt set, the worker falls back to a deep
audit. Checkpoint replacement is no-replace/content-addressed; a small pointer to the
latest accepted checkpoint may be rebuilt at any time.

Within one process, a completed deep validation is reused rather than immediately
repeating the same full scan. Cross-process and post-restart reuse requires the
checkpoint contract above; an in-memory cache alone never crosses that boundary.
Persisted filesystem device, inode, and timestamp values are diagnostic evidence, not
durable content authority: mount/device numbering can change across a clean reboot,
and copies or restores can change the other values without changing content. A seal
successor may use descriptor/path device-and-inode equality to prevent replacement
within one retained-file operation, with live metadata comparisons retained for race
detection. Post-restart authority instead uses the sealed path/tree, regular type,
current ownership and mode policy, link count, byte count, exact content hash, and
lineage. A root-relative CAS location is the future relocation boundary; a stable
filesystem UUID is placement evidence only where the physical hot/cold tier matters.
The original v0.4 validator remains frozen and reboot-fragile. Its narrow, read-only
compatibility auditor and the other measured failures and successor rules are
documented in the [restart and archive portability audit](../RESTART_PORTABILITY_AUDIT.md)
and [v0.4 receipt audit](../../pipeline/ASR_WHISPERCPP_V04_RECEIPT_AUDIT.md).

### 2. Stable content-addressed data roots

Use one stable main-drive root for each physical class:

```text
hot/raw/sha256/<prefix>/<digest>/payload
hot/derived/sha256/<prefix>/<digest>/...
hot/asr/sha256/<prefix>/<input-digest>/recipes/<recipe-digest>/...
control/<epoch-or-batch-id>/...
```

Epoch manifests, work orders, receipts, and attempts remain in bounded control roots.
Changing an epoch must not create another physical copy of content already present
in a global CAS. Publication-safe exports and the cold archive remain separate.

### 3. ASR-ready preprocessing before visual enrichment

The latency-sensitive lane produces only the normalized audio representation and its
probe/provenance. A distinct enrichment-only lane produces the probe, proxy, and
scene/silence routing with normalized-audio generation disabled; sparse frames, OCR,
diarization, and visual models consume later enrichment stages. Both lanes bind the
same exact source media ID; neither output is treated as a superseding rendition of
the other.

Audio eligibility is derived from the validated source probe and sealed operation
profile. A legitimate source with no audio stream remains completed preprocessing
evidence but is recorded as an explicit `source_has_no_audio` ASR skip; it cannot
invalidate eligible siblings in the same receipt set. An audio-bearing source whose
enabled FLAC artifact is absent still fails closed. A deliberately enrichment-only
result is similarly ineligible for ASR with the distinct reason
`normalized_audio_operation_disabled`.

Before changing the current recipe, benchmark these independently on a
recording-disjoint sample:

1. current FLAC compression level 8 versus a faster lossless FLAC setting;
2. audio-only decode versus the current three-pass profile;
3. a fused decode/filter graph versus separate outputs; and
4. source-container decoding directly into a bounded PCM/sample-hash stream.

The admitted winner gets a new recipe identity. Existing FLAC/proxy/routing results
are retained and never silently relabelled. Visual enrichment is backpressured by its
own byte/item budget and cannot hold the ASR-ready queue hostage.

### 4. Filesystem-verified hot evidence

The running kernel is built with `CONFIG_FS_VERITY=y` and the main drive is Btrfs,
but `fsverity-utils` is not installed and the current Btrfs sysfs feature probe
reports `0`. Treat fs-verity as unavailable until a disposable main-drive file passes
an explicit enable/measure/read/tamper capability test. If that gate passes, a future
CAS-v2 admission may enable verity only after ordinary SHA-256 verification, `fsync`,
single-link/path checks, and immutable publication. Receipts bind both the ordinary
content digest and the measured verity digest. Operational replay may then measure
verity in constant time while scheduled deep audits continue to recompute ordinary
SHA-256.

Enabling fs-verity is a new, effectively irreversible per-file state change and is
not authorized by this ADR. It requires `fsverity-utils`, a plan over exact files,
and a separate reviewed apply operation. Mutable databases, queues, staging files,
runtime environments, and cold-storage payloads are excluded until they have their
own lifecycle contract.

### 5. GPU successor contracts

Preserve GPU adapter v1-v3 and resident-batch v1 byte-for-byte for replay. A successor
contract must:

- remain unadmitted until an external pre-execution trust anchor verifies the
  wrapper, interpreter, adapter, and any pathname re-execution before those bytes
  run; an in-process runtime-receipt check is not self-authentication, and a sealed
  verified launcher, genuinely immutable read-only snapshot, or proven fs-verity is
  required;
- hash and load preserved Python dependencies from verified bytes before executing
  them;
- distinguish a requested/forced language from detected language and never present
  a forced-language sentinel as a calibrated or detected probability;
- separate the common decode recipe from per-member resource bounds and output
  locators so compatible items can share larger resident batches;
- use a stable ASR result CAS root;
- emit append-only attempt-start and item-complete events so hard deadline or VRAM
  exits are diagnosable after restart; and
- sample utilization, temperature, power, clocks, throttling, and VRAM during soak
  admission.

Glossary-aware, multilingual, and neural-batched decoding remain separate recipes.
They require recording-disjoint accuracy/timestamp comparisons and resource admission
before use.

### 6. Private transcript admission

Add an engine-neutral, media-local private admission boundary for GPU-v3 and later
normalized transcript envelopes. It reuses the append-only
`media_local_transcript_*` tables and private FTS, stores engine-specific provenance
once in an import receipt, leaves calibration and speaker identity null, and retains
all timing anomaly flags. It may not create recording coordinates or a public
transcript. A text-free plan digest is reviewed before each import, and all source,
artifact, result, work-order, and batch-completion bindings replay inside the
transaction.

Bulk import uses bounded staged rows and one transaction per result or finite chunk,
not a `SELECT` before every segment/word insert. Public projection and wiki use stay
behind their existing independent human decisions.

## Initial operating limits

- one public downloader, one ASR-ready audio worker, and one UUID-locked GPU worker;
- one deferred visual-enrichment worker only when measured I/O and CPU headroom allow;
- a single global hot CAS per physical class;
- operational checkpoint after every bounded batch and deep audit on checkpoint
  creation, scheduled scrub, software upgrade, restore, or inconsistency;
- no cold transfer, hot eviction, identity inference, or publication from scheduler
  state.

The next throughput benchmark is an audio-only, recording-disjoint sample followed by
a 30-minute resident-GPU soak. Report source duration, wall/CPU time, bytes read and
written, peak queue depth, GPU inference/load/control time, VRAM, temperature, power,
and any transcript/timestamp difference. Do not extrapolate a corpus completion date
until the ASR-ready and deferred-enrichment rates are measured separately.

## Initial implementation evidence

`preprocess_batch.py` 0.3 adds uniform `--lane asr-ready` and
`--lane enrichment-only` contracts while retaining the old full lane as the default
and replaying sealed 0.1/0.2 full bundles. ASR-ready work orders enable only probe and
normalized FLAC; enrichment-only work orders enable probe, proxy, and routing without
FLAC. Validation rejects arbitrary operation combinations, mixed-lane batches, and
non-full legacy bundles. Focused batch and producer integration tests prove
deterministic materialization, immutable replay, lane-specific artifact sets, and a
complete ASR-ready batch-receipt handoff into queue admission, including an explicit
no-audio skip beside an eligible item.

The corresponding queue change is a versioned successor, not an in-place rewrite.
The v0.1 and v0.2 materializer/schema bytes remain retained under exact regression
hashes, while v0.3 has separate materializer, manifest schema, runner, run schema,
and result-seal entry points. The v0.3 schemas have distinct `$id` values; historical
v0.1 and v0.2 schemas share an old `$id`, so compatibility tooling selects them by
the sealed implementation version and exact byte hash rather than URI alone. The
v0.1 materializer also self-pins its former unversioned `__file__` path. Directly
importing its retained versioned copy is therefore explicitly rejected; exact replay
requires a read-only sandbox overlay at the originally pinned path. V0.2 remains the
production default while the complete v0.3 materializer/runner/sealer chain is kept
as an explicit candidate lane. Focused v0.3 tests replay a skip-bearing manifest
through the matching sequential runner and exercise the v0.5 result sealer that pins
the v0.3 materializer and schema; no unversioned production entry point was moved.
The v0.5 seal replay also makes persisted filesystem object numbers and timestamps
diagnostic-only across restarts while retaining descriptor/path identity and full
metadata/content race checks within each operation. Exact path/tree, type, mode,
link-count, size, content-hash, and lineage checks remain authoritative after restart.

A read-only benchmark used the already processed 403.377-second public ordinal-20
source. Its historical full preprocess result took 21.428 seconds. Re-running only
the exact admitted 16 kHz mono FLAC transform to a null sink took 0.89 seconds at
compression level 8 (audio-stage RTF about 0.00221), while level 1 took 0.88 seconds.
This warm-cache single-item measurement is not a corpus projection, but it confirms
that proxy/routing separation is much more consequential than changing FLAC
compression for this sample. No media, result, catalogue, or cold-storage file was
created or modified by the benchmark.

Within one background-producer invocation, a deep-validated queue summary is now
strictly rebound and reused for the immediate transition, reducing repeated payload
scans while preserving fresh validation after restart, failure, or a new invocation.
The cross-process operational checkpoint and global CAS layout in this ADR are still
design decisions, not implemented authority; current tools must not claim their
scaling benefit across process boundaries.

Migration `0034_private_faster_whisper_gpu_v3.sql` and the digest-gated v3 importer now
implement the private media-local admission boundary described above. The migration is
deliberately unapplied. Read-only plans bind exact result, work-order, media, artifact,
model, and optional batch-completion ancestry; omission of batch ancestry is recorded
as `unasserted`, not `standalone`. No live catalogue import, recording/source
coordinate, identity/event assertion, calibration, or publication authority was
created during this audit.

## Consequences

- After the content-addressed operational checkpoint is implemented, normal resume can
  become proportional to new work rather than all historical media, while deep audit
  remains available and authoritative. The current in-process reuse improves only one
  invocation.
- Searchable private text can follow GPU production without granting publication or
  identity authority.
- ASR can stay fed while expensive visual derivatives are produced later.
- Global CAS roots eliminate epoch-local duplicate payloads and simplify hot/cold
  residency planning.
- Existing sealed results remain reproducible; correctness fixes receive new contract
  identities instead of rewriting history.
