# ASR-blind interval proposals

`evaluation.interval_proposal` prepares a bounded human-review queue from sealed
preprocessing metadata. It is deliberately **not** an interval freeze, reference
manifest, annotation, transcript, or quality result.

## Inputs and authority

The helper accepts only:

- the exact candidate-cohort manifest;
- a closed, checkpointed SQLite catalogue with no `-wal`, `-shm`, or `-journal`
  sidecar, opened with `mode=ro&immutable=1`, `PRAGMA query_only=ON`, and a read
  transaction;
- completed `media_preprocess` results whose input media, raw result-file SHA-256,
  canonical import-envelope SHA-256, recipe SHA-256, run ID, routing artifact,
  derived artifact hashes, and catalogue rows all agree; and
- for an offset window, a completed local-window result whose exact parent bytes,
  half-open source range, zero-point mapping, and analysis artifact agree.

Grouped local-window requests additionally require the catalogue rows written by
the existing local-window admission workflow. The selected derivative must have
one exact `media_derivations` parent edge, admitted artifact row, deterministic
local-window rendition, admission processing run and two non-ASR admission inputs,
and one timeline span. Their metadata must pin the same local-window result SHA-256,
bundle/window identity, acquisition parent, artifact probe, source-time mapping,
and acquired parent rendition. This helper does not create those rows or perform
admission; it only verifies them in a query-only snapshot.
The immutable open is an offline audit contract: the evaluator refuses a catalogue
with any SQLite sidecar and verifies that the main-file identity and sidecar state do
not change while the snapshot is opened. This prevents a nominally read-only audit
from creating WAL shared-memory files or silently ignoring uncheckpointed rows.
The admission context's exact recording-source row may be an archive acquisition
mapping rather than the cohort's public-platform source row; both must belong to
the same parent recording, and each is independently pinned in the catalogue
basis.

Schema v2 binds two deliberately different digests. The request field
`preprocess_result_raw_sha256` pins the exact sealed on-disk bytes, including JSON
formatting. `preprocess_import_envelope_sha256` reproduces the corpus importer's
SHA-256 over the entire parsed envelope serialized as sorted, compact UTF-8 JSON;
`preprocess_import_batch_id` is deterministically derived from that canonical
digest and `media_preprocess_result_v1`. The completed import-batch row's
`input_sha256` must equal the canonical digest. Thus whitespace-only reformatting
changes the raw pin but not the import admission, while a semantic field change
changes both and cannot reuse the batch. Pinning only run and artifact rows is not
sufficient. The current recording must also remain unmerged and in
`metadata_only`, `unreviewed`, or `reviewed` state.

Here, “sealed” requires a regular non-symlink JSON file with no owner, group, or
other write bit (`mode & 0o222 == 0`). Device, inode, size, modification time,
change time, and mode must remain identical across the read, and the no-write-bit
condition is checked both before and after it. This applies to preprocess and local
result inputs and to pinned JSON routing/probe artifacts.
An operator may seal a legacy writable result before invocation by removing every
write bit (for example, `chmod 0444 result.json`). That permission-only transition
does not change the raw-byte SHA-256 or schema-v1 identity; it must be complete
before the evaluator starts because a mode or change-time transition during a read
fails closed.

Unknown result fields fail closed. The only accepted preprocessing artifact kinds
are normalized probe JSON, 16 kHz mono FLAC, low-resolution CFR proxy, and
scene/silence routing JSON. A result from an ASR stage, an ASR/transcript-shaped
input pathname, an unpinned file, an unverified catalogue media row, or a changed
hash is rejected. The helper never queries transcript tables and never opens media
or derived audio/video bytes. For v2, explicit layout, tool, command-argument,
step-output, reuse, artifact, and catalogue file paths are screened as well; a
neutral key cannot hide an ASR/transcript/reference-shaped pathname.

The registered `scene_silence_routing_json` artifact is the canonical, hash-bound
routing input, not an authoritative description of the content. The helper opens
that metadata file as a bounded non-symlink regular file, verifies its byte count and
SHA-256, requires its parsed object to equal the routing object embedded in the
preprocess result, and routes only after strict parameter, enum, count, duration, and
summary-arithmetic checks. The recipe SHA-256 must equal the
canonical processing-run parameters in both the result and catalogue. A preprocess
run must have exactly one catalogue input with `object_type: "media"` and
`input_role: "source_media"`; extra ASR/transcript inputs fail closed.

All six `catalog_records` arrays are capped and validated row-by-row against exact
media-preprocess shapes; arbitrary nested rows or neutral keys are not accepted.
Declared rows are then matched to the query-only catalogue. Producer-local
`run_input_id` and `media_location_id` values are recomputed and matched by their
catalogue natural keys, because import intentionally assigns catalogue-local IDs.
For a source media object already catalogued by acquisition, the database may retain
its acquisition probe and a narrower cache class. In that case the preprocessing
probe must instead exactly equal the registered, byte-counted, SHA-256-pinned
`ffprobe_normalized_json` artifact; every other substantive media/location field
still has to match the catalogue. This prevents free-form producer convenience rows
from becoming an alternate text-input channel.

Neither generation command accepts an output pathname. JSON is emitted to stdout;
capturing stdout in ignored private storage is an operator action outside this
read-only helper. The commands have no database-write, migration, freeze, review,
annotation, or publication authority.

## Deterministic routing

For each recording, candidate half-open windows come from three metadata-only
routes:

1. windows centred on scene-change timestamps;
2. windows beginning at detected-silence end boundaries; and
3. periodic fallback windows.

Candidates are ranked by estimated silence fraction, route kind, and timestamp.
The helper greedily enforces nonoverlap and the requested minimum gap, then applies
per-recording count, global count, and global duration caps. For schema v2, all
candidate intervals from all windows of one parent are ranked together, so
`minimum_gap_ms` and `max_intervals_per_recording` apply once to the parent—not once
per derivative. A round-robin global pass prevents a low cap from assigning all
intervals to the first recording. The v2 pass skips a candidate that cannot fit the
remaining duration budget and continues scanning that parent's queue, so a long
head interval cannot hide a later eligible short tail. Schema-v1 regeneration
retains its established cap behavior.
Everything is integer-millisecond and deterministic. Routing is a workload hint,
not evidence that speech, a language, a speaker, or a useful evaluation passage is
present.

Every emitted interval is `proposal_unreviewed`. `include`, `split`, `stratum_id`,
language tags, code switching, speaker overlap, playback speech, and noise are all
`null`. A human reviewer must inspect the exact parent rendition, decide whether to
include each interval, complete those fields, resolve any timing issue, and use the
separate freeze workflow. Copying proposal intervals into a freeze without that
review violates the evaluation protocol.

## Full-rendition workflow

First emit a strict, hash-pinned request. Repeat `--preprocess-result` once for each
completed cohort recording:

```sh
python3 -m evaluation emit-proposal-request \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --catalog research/corpus/corpus-v8.sqlite3 \
  --created-at 2026-08-26T22:00:00Z \
  --preprocess-result /ABSOLUTE/PATH/TO/RESULT-1.json \
  --preprocess-result /ABSOLUTE/PATH/TO/RESULT-2.json
```

The convenience command resolves each sealed input media object to exactly one
`acquired_source_media` rendition in the cohort and preserves cohort order. It
supports any nonempty subset up to all 12 cohort recordings, including the 10
full-rendition inputs used for the preserved schema-v1 proposal. It refuses duplicate
candidates. Save stdout only
under ignored private evaluation storage or `/tmp`, then validate the request:

```sh
python3 -m evaluation validate-proposal-request \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  /ABSOLUTE/PATH/interval-proposal-request.json
```

Generate and independently regenerate the proposal:

```sh
python3 -m evaluation propose-intervals \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --catalog research/corpus/corpus-v8.sqlite3 \
  /ABSOLUTE/PATH/interval-proposal-request.json

python3 -m evaluation validate-proposal \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --catalog research/corpus/corpus-v8.sqlite3 \
  --request /ABSOLUTE/PATH/interval-proposal-request.json \
  /ABSOLUTE/PATH/interval-proposal.json
```

The request builder defaults to 30-second intervals, at most 12 intervals per
recording, 120 globally, one hour globally, a five-second gap, and a two-minute
periodic stride. All limits are present in the signed request and may be lowered or
changed through the documented CLI options within the hard schema/runtime caps.

## Grouped local-window workflow (schema v2)

`emit-proposal-request` remains the schema-v1 full-rendition command. The grouped
local-window command takes matched repeated arguments: the first
`--local-window-result` is paired with the first `--preprocess-result`, and so on.
Each preprocessing result must route the exact selected local-window artifact.

```sh
python3 -m evaluation emit-local-window-proposal-request \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --catalog research/corpus/corpus-v8.sqlite3 \
  --created-at 2026-08-26T22:00:00Z \
  --local-window-result /ABSOLUTE/WINDOW-1/result.json \
  --local-window-result /ABSOLUTE/WINDOW-2/result.json \
  --preprocess-result /ABSOLUTE/WINDOW-1/media-preprocess-result.json \
  --preprocess-result /ABSOLUTE/WINDOW-2/media-preprocess-result.json
```

The emitter verifies every pair before grouping it. Parent rows follow cohort order;
their `windows` arrays are sorted by exact half-open parent offset. Duplicate result
pins, window IDs/ordinals, derivative media/renditions, or source ranges fail.
Overlapping source windows also fail, even if each result is independently valid.
There may be at most 256
windows under one parent and 384 across a request; aggregate scene/silence rows and
generated routing anchors are each capped at 200,000. Like proposal generation,
the command has no output-path option,
opens SQLite with `mode=ro` plus `PRAGMA query_only=ON`, never queries transcript
tables, and writes only JSON to stdout.

File URI comparison follows the producer's percent-encoded `Path.as_uri()` form,
including paths containing spaces or non-ASCII characters. Existing local-window
admission rows may retain `storage_class: "private_local"` when preprocessing
declares the same source location generically as `local`; proxy media may likewise
retain catalogue container `mp4` when ffprobe declares its equivalent
`mov,mp4,m4a,3gp,3g2,mj2` name. These are narrow source-media compatibility cases;
hashes, byte counts, media identity, duration, location URI, and every other row
field still match exactly.

Validate and propose with the same generic commands shown above. A v2 proposal keeps
one unique parent recording row, a `windows` array with every exact analysis media,
analysis rendition, preprocess raw-file hash, preprocess canonical-envelope hash
and import-batch ID, routing-artifact hash, local-result hash, artifact hash,
admission run and source offset, and one parent-level `intervals` array. Each
interval identifies its window and repeats both coordinate systems:

```text
parent_start_ms = source_offset_ms + local_start_ms
parent_end_ms   = source_offset_ms + local_end_ms
start_ms        = parent_start_ms
end_ms          = parent_end_ms
```

All four boundaries are integer-millisecond half-open coordinates and every
interval remains `proposal_unreviewed`. The sealed local result must independently
attest that artifact time zero maps to `source_offset_ms`; any boundary skew or
source-mapping mismatch fails before routing.

### Compatibility and migration

Schema-v1 request/proposal files and the full-rendition emitter are unchanged. A
legacy, manually constructed one-local-window v1 request remains valid and
regenerates the same v1 proposal shape. Schema v2 is an additive opt-in contract
for two or more (or a grouped single) admitted local windows; it uses a versioned
deterministic request ID and produces a v2 proposal. No corpus migration or live
database write is performed. Operators must first use the existing private
local-window admission and media-preprocess import workflows, then point this
read-only evaluator at that catalogue snapshot. The exact preprocess-envelope
import-batch check uses the existing `import_batches` table and therefore requires
no schema migration.
The v2 preprocess import-batch row is included only in v2 catalogue-basis hashing;
schema-v1 relevant-row material omits the extension entirely, preserving its
historical catalogue hash and deterministic proposal identity.

The corrected v2 field names replace the pre-release ambiguous
`preprocess_result_sha256` / `preprocess_binding.result_sha256` fields. Any v2
request or proposal emitted by that earlier draft must be re-emitted; schema-v1
files and their deterministic regeneration are unaffected.

## Human-review handoff

Once two validated proposals form a disjoint, exact cover of the 12-candidate
cohort, use `emit-selection-review-template` with the two matching repeated
`--request`/`--proposal` arguments. It emits a valid but deliberately incomplete
private template to stdout only. Keep it under ignored private evaluation storage;
`audit-tracked` rejects every tracked `interval_selection_review`, even if someone
removes or corrupts its privacy block.

`validate-selection-review` is an operational completed-review check, not a way to
approve the emitted template. It requires every interval decision, recording-level
split, direct-parent/no-output attestation, tool provenance, complete include flags,
exclude/trim reasons, and the 3,600,000-ms accepted-duration floor. `compile-freeze`
also requires an explicit `--frozen-at` and writes only deterministic freeze-v2 JSON
to stdout. It maps v2 proposals back to acquired parent media and parent time.

Standalone freeze-v2 runtime validation is self-contained for downstream annotation
and therefore cannot prove that claimed external request/proposal hashes name the
operator's supplied private files. Operational `validate-freeze` closes that gap:
for v2 it requires `--review`, `--catalog`, and both request/proposal pairs, then
regenerates and byte-compares the expected freeze. See the main
[`README.md`](./README.md) for the complete review, timestamp, and privacy contract.

## Contract files

- [`interval-proposal-request.schema.json`](./schemas/interval-proposal-request.schema.json)
- [`interval-proposal.schema.json`](./schemas/interval-proposal.schema.json)
- [`interval-proposal-request-v2.schema.json`](./schemas/interval-proposal-request-v2.schema.json)
- [`interval-proposal-v2.schema.json`](./schemas/interval-proposal-v2.schema.json)
- [`interval-selection-review.schema.json`](./schemas/interval-selection-review.schema.json)
- [`interval-freeze-v2.schema.json`](./schemas/interval-freeze-v2.schema.json)

All schemas reject unknown fields. Runtime validation additionally checks the
canonical manifest digests, deterministic IDs, cohort ordering, all catalogue and
preprocess lineage, local offsets, silence arithmetic, interval nonoverlap and
caps, exact null reviewer fields, and every fail-closed safety assertion.
