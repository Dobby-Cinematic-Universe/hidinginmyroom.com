# Private ASR queues from preprocess audio

The production/default command remains the byte-frozen v0.2 implementation in
[`preprocess_asr_queue.py`](preprocess_asr_queue.py). The explicit v0.3 successor in
[`preprocess_asr_queue_v03.py`](preprocess_asr_queue_v03.py) adds evidence-bound
no-audio skips without changing historical replay bytes.

## Version boundary

| Queue | Materializer and schema | Runner and result seal | Status |
|---|---|---|---|
| v0.1 | [`preprocess_asr_queue_v01.py`](preprocess_asr_queue_v01.py) and [`preprocess-asr-queue-manifest-v01.schema.json`](schemas/preprocess-asr-queue-manifest-v01.schema.json) | Historical replay only | Exact bytes retained; use an original-path sandbox overlay, never direct import |
| v0.2 | [`preprocess_asr_queue.py`](preprocess_asr_queue.py) and [`preprocess-asr-queue-manifest.schema.json`](schemas/preprocess-asr-queue-manifest.schema.json) | Default v0.2 runner and seal | Production/default and historical replay |
| v0.3 | [`preprocess_asr_queue_v03.py`](preprocess_asr_queue_v03.py) and [`preprocess-asr-queue-manifest-v03.schema.json`](schemas/preprocess-asr-queue-manifest-v03.schema.json) | Explicit v0.3 runner and v0.5 seal | Candidate lane; tested, but not the production default |

The historical pins are immutable regression contracts:

- v0.1 source: 70,900 bytes, SHA-256 `ba93a330484c5f34c65df5498b4417a9baa1e498ad6d96ff46c21b4205b97a2b`;
- v0.1 schema: 26,309 bytes, SHA-256 `ab4ee8dfe3b542fe197e5fbf9bae14ac0ea77a38b288e5fd3c959733c6db5077`;
- v0.2 source: 86,523 bytes, SHA-256 `e0fffe5af2f403cd46fff82bde452f81fabb1d165f82dffcb052206d00b0fe87`; and
- v0.2 schema: 31,540 bytes, SHA-256 `f8b5f1debca171b5dd58f2d5e6bcc007489e389d2e2617c581bcff0eaabfdffa`.

The v0.1 source self-pins its original unversioned `__file__` path. A direct import
from the retained versioned filename therefore cannot replay a sealed v0.1 queue.
Use a read-only `bwrap` overlay that presents those exact bytes at
`pipeline/preprocess_asr_queue.py`, then invoke that overlaid path. The v0.1 schema
must likewise be selected by the exact hash above; its shared `$id` is not version
authority. The dispatcher rejects v0.1 direct import for this reason.

Both v0.2 and v0.3 seal
[`asr_whispercpp.py`](asr_whispercpp.py) work orders for full normalized audio
artifacts produced by [`media_preprocess.py`](media_preprocess.py). Legacy/public
inputs remain ordinary adapter work orders; v30 private-acquisition inputs use a
policy-bound queue wrapper. It is a queue materializer only: it performs no ASR,
network access, catalog write, publication, identity assignment, or media copy.

This lane is separate from [`asr_whispercpp_batch.py`](asr_whispercpp_batch.py).
That batch binds extracted local windows. This queue binds full-media preprocess
FLACs and therefore uses a different evidence and coordinate contract.

## Admission proofs

Every queued audio artifact must pass exactly one of two proofs.

In both versions, `sealed_preprocess_receipts` replays an immutable preprocess bundle, its selection,
every work order, every completed receipt, each result envelope, and every retained
artifact. The queue is refused if any batch ordinal lacks a valid receipt. This is
the preferred proof before the corresponding results have been admitted to SQLite.
In v0.3 only, an otherwise valid receipt whose probe proves that the source has no audio stream is
not an ASR member: it is retained under the sealed origin's
`ineligible_receipts` with reason `source_has_no_audio`. A receipt produced with the
normalized-audio operation disabled is retained there with reason
`normalized_audio_operation_disabled`. Receipt counts and receipt-set digests still
cover those explicit skips. An audio-bearing receipt whose recipe enables normalized
audio but lacks the expected FLAC remains invalid, and a batch with no eligible FLACs
cannot materialize an empty queue.
If the preprocess manifest carries v30 private-acquisition `handling_control`, the
collector also replays each receipt's complete `handling_boundary` through the
original acquisition-seal validator. The policy, seal-plan hash, seal-receipt hash,
receipt path, source boundary, and explicit `source_byte_identity_claimed: false`
state must all agree. Missing, additional, or weakened policy entries fail before
queue materialization.

`catalog_admitted_preprocess_results` accepts explicit completed `result.json`
paths and opens a specified SQLite file through `mode=ro`, `query_only`, a
write-denying authorizer, and a read transaction. Each result/audio pair must match
one completed `media_preprocess` run, source run input, private artifact, verified
media object and primary `local_derived` location, and exact normalization
derivation. Existing source-media rows use the same conservative replay semantics as
the preprocess importer: content identity must match exactly, while prior probe/MIME
enrichment and the canonical `local_hot_cache` storage class may predate the result.
The manifest binds both the current catalog source rows and the producer-handoff
source rows so this distinction is explicit. The catalog graph is provenance only;
it does not grant recording-time coordinates.

Both modes retain the exact preprocess-result SHA-256, source-media SHA-256,
normalized-audio SHA-256, sizes, paths, processing-run identity, exact normalized
probe digest, and an evidence-specific canonical binding hash. Catalog JSON fields
are represented by canonical SHA-256 projections in the manifest, while the exact
result hash pins their producer envelope. Argument order does not affect a
catalog-mode queue. Duplicate result paths, artifact IDs, or audio content fail
closed.

## Private-acquisition handling boundary

Legacy/public preprocess results keep the existing queue shape: each file under
`work-orders/` is an ordinary `asr_whispercpp` work order, and no handling block is
invented. Catalog-admitted mode likewise cannot introduce private-acquisition
authority or policy.

A sealed-receipt batch with v30 `handling_control` uses an intentionally different,
fail-closed shape. V0.2 retains the exact preprocess control at both the origin and
top level. V0.3 retains the complete control at the origin while its top-level
control is the deterministic subset for ASR-eligible private receipts, so a
validated no-audio skip cannot create a phantom queue member while its complete
provenance remains visible in the origin. Each affected queue entry
retains a full `handling`
descriptor containing the exact preprocess control row, handling policy, complete
private-acquisition seal binding and source boundary, and all hashes needed for
replay.

The corresponding file under `work-orders/` is a
`v30_private_preprocess_asr_queue_work_order` wrapper around the ordinary adapter
order. The generic ASR adapter rejects that wrapper. Only the queue runner may unwrap
it, after replaying the acquisition seal and comparing the wrapper, manifest entry,
top-level control, preprocess receipt, and deterministic queue reconstruction. The
inner ASR job ID also commits to the boundary, seal-receipt, and seal-plan hashes, so
a pre-policy result cannot collide with or qualify as reuse for the policy-bound job.
The entry's `canonical_sha256` pins the complete wrapper, while the separate
`adapter_work_order_sha256` pins the inner order used in the adapter result identity.
Queue validation returns the wrapper—not a naked inner order—for affected entries;
the runner is the only component in this lane that unwraps it for adapter dispatch.

Every runner action—including failures and reuse—echoes the affected `handling`
descriptor. Every runner summary echoes the top-level `handling_control` plus
`private_acquisition_seal_replay_required` and
`handling_policy_propagation_required`. Publication authority remains `none`; this
lane never publishes, imports, grants review authority, or clears a publication
gate.

## Timestamp contract

Every generated work order uses:

- `window.offset_ms: 0`;
- `window.duration_ms` from the exact normalized FLAC probe;
- `catalog_context: null`; and
- artifact-local milliseconds with a half-open `[0, duration_ms)` interval.

The queue manifest separately fixes `recording_transform_state` to `unresolved`,
keeps both recording endpoints null, and says duration equality is not translation
evidence. A rendition, derivation, similar duration, or source path must never be
used to reinterpret an artifact-local ASR timestamp as recording-global. A later
catalog import needs a separately sealed and reviewed coordinate-translation proof.

Preprocessing routing is advisory at **queue admission**. An explicit admitted audio
artifact remains a sealed queue member when its routing hint is
`review_near_silent_candidate`; the hint never changes queue identity or ordinal.
The bounded dispatcher described below applies a separate operational safety default:
it validates these members in place but routes them to review without invoking the
ASR adapter. A reviewer can explicitly override that default without rematerializing
or reordering the queue.

## Engine and model binding

New queues consume the shared exact allowlist in
[`whispercpp_engine_profiles.py`](whispercpp_engine_profiles.py). The executable
must match both the hash and byte count of the current `v1.8.7` profile, whose full
JSON output fixes split UTF-8 token-boundary merging. The legacy `v1.8.3` profile is
available to the existing batch lane only for replay of already-sealed manifests;
it cannot materialize a new preprocess queue.

Queue identity includes:

- the complete selected engine profile and its canonical SHA-256;
- the engine-profile module's exact path, bytes, version, and SHA-256;
- the ASR adapter and queue materializer exact bytes and versions;
- the executable and model hashes, sizes, build/revision metadata, and paths; and
- the complete inference parameters.

The model remains the exact reviewed `small.en` weights. A queue does not infer a
different model from a filename or accept an unpinned executable.

## Production v0.2: materialize the completed 19-item short batch

Run from the repository root with explicit private roots:

```sh
pipeline/bin/preprocess-asr-queue materialize \
  --preprocess-bundle research/corpus/preprocess-batch-control/bundles/ppbatch_a92965b935f16539966cd28d2491f89c \
  --preprocess-state-root research/corpus/preprocess-batch-state \
  --queue-root research/corpus/private-preprocess-asr-work-orders \
  --asr-output-root research/corpus/private-asr-results \
  --engine research/corpus/tools/whisper.cpp-v1.8.7/build-noccache/bin/whisper-cli \
  --model research/corpus/tools/whisper.cpp/models/ggml-small.en.bin
```

The reviewed receipt set yields 19 work orders, 42,685,619 audio bytes, and
3,264,895 audio milliseconds. Two inputs retain a near-silent review hint; all 19
remain explicit queue members. Recompute the printed queue ID after an intentional
source, adapter, engine-profile, model, or contract change rather than copying an ID
from this document.

The immutable layout is:

```text
<queue-root>/
├── .preprocess-asr-queue.lock
└── queues/
    └── asrppqueue_<32 hex>/             # mode 0500
        ├── manifest.json                # mode 0400
        └── work-orders/                 # mode 0500
            ├── 000001.json              # mode 0400
            └── ...
```

Re-running `materialize` performs exact replay and never replaces the existing
queue. It hashes all retained preprocess inputs and artifacts plus the model and
engine before admitting any JSON.

## Explicit v0.3 candidate lane

Use the versioned command only for a v0.3 ASR-ready batch. It records validated
audio-less receipts in `origin.ineligible_receipts`, keeps all receipt-set digests
over the complete batch, and queues only eligible FLAC artifacts:

```sh
pipeline/bin/preprocess-asr-queue-v03 materialize \
  --preprocess-bundle /absolute/private/asr-ready-bundles/bundles/<bundle-id> \
  --preprocess-state-root /absolute/private/asr-ready-state \
  --queue-root /absolute/private/preprocess-asr-v03-work-orders \
  --asr-output-root /absolute/private/asr-v03-results \
  --engine /absolute/private/whisper.cpp-v1.8.7/whisper-cli \
  --model /absolute/private/models/ggml-small.en.bin

pipeline/bin/preprocess-asr-queue-v03 validate \
  --manifest /absolute/private/preprocess-asr-v03-work-orders/queues/asrppqueue_<32-hex>/manifest.json
```

The version-selecting candidate command
[`preprocess-asr-queue-dispatch`](bin/preprocess-asr-queue-dispatch) validates v0.2
or v0.3 strictly from the manifest's exact `implementation_version`; it rejects
unknown versions and v0.1 direct-import replay. Its materialize operation is v0.3.
The unversioned production command remains v0.2.

## Catalog-admitted mode

After reviewed preprocess imports, bind explicit result envelopes to an exact
read-only catalog snapshot:

```sh
pipeline/bin/preprocess-asr-queue materialize \
  --catalog /absolute/private/catalog.sqlite3 \
  --preprocess-result /absolute/private/result-1.json \
  --preprocess-result /absolute/private/result-2.json \
  --queue-root /absolute/private/preprocess-asr-work-orders \
  --asr-output-root /absolute/private/asr-results \
  --engine /absolute/private/whisper.cpp-v1.8.7/whisper-cli \
  --model /absolute/private/models/ggml-small.en.bin
```

The example uses production v0.2. Substitute only the explicit
`preprocess-asr-queue-v03` command when every supplied result is intended for the
v0.3 contract; catalog mode never silently skips an explicitly selected ineligible
result.

Do not use catalog mode merely because a file path appears in a table. Exact
result-to-run-to-artifact-to-media-to-derivation replay is mandatory. A changed or
missing row, non-current derivation run, unverified location, unexpected storage
class, incompatible source enrichment, catalog integrity failure, or modified
result/audio file rejects the queue.

## Validate and dispatch the sealed queue

Replay every evidence, file, model, executable, manifest, and work-order pin without
running ASR:

```sh
pipeline/bin/preprocess-asr-queue validate \
  --manifest research/corpus/private-preprocess-asr-work-orders/queues/asrppqueue_<32-hex>/manifest.json
```

The production example above is v0.2. The success document lists work-order paths
in sealed ordinal order. Use the isolated
[`preprocess_asr_queue_runner.py`](preprocess_asr_queue_runner.py) for bounded
validation and sequential dispatch. It accepts only `sealed_preprocess_receipts`
queues; it rejects catalog-origin queues before calling the queue validator, so this
lane never opens SQLite. It has no network, publication, catalog-write, identity-write,
or runner-state-write operation.

Before dispatch, the runner replays the full queue, then stable-reads each mode-0400,
single-link work order immediately before its turn. Queue replay rehashes the exact
adapter, engine-profile source, executable, model, every normalized FLAC, preprocess
evidence, and queue file. The adapter then retains and rehashes its engine, model, and
FLAC descriptors around each invocation. Dispatch is single-process, ordinal,
sequential, and fail-fast; the runner replays the entire queue again on success and
after a per-item failure.

First validate runner routing and any reusable results without invoking `ffprobe` or
ASR and without creating the output root:

```sh
pipeline/bin/preprocess-asr-queue-runner validate \
  --manifest research/corpus/private-preprocess-asr-work-orders/queues/asrppqueue_66349d9b85c74f2376830edf2a7d4f0c/manifest.json
```

Then run the adapter's dry-run path. This rehashes retained inputs and executes only
the local `ffprobe` probe; it plans whisper.cpp commands but does not execute ASR or
write a result directory:

```sh
pipeline/bin/preprocess-asr-queue-runner run \
  --manifest research/corpus/private-preprocess-asr-work-orders/queues/asrppqueue_66349d9b85c74f2376830edf2a7d4f0c/manifest.json \
  --dry-run
```

The current sealed queue contains 19 members. By default, ordinals 1 and 13 are
reported as `review_required`, and the other 17 are planned or dispatched in their
original ordinal positions. No adapter call is made for either near-silent item.
After explicit human review, add `--include-near-silent` to either runner command to
make all 19 eligible; the flag does not change queue or work-order identity.

For a later real run, after reviewing the validate and dry-run summaries, use exactly:

```sh
pipeline/bin/preprocess-asr-queue-runner run \
  --manifest research/corpus/private-preprocess-asr-work-orders/queues/asrppqueue_66349d9b85c74f2376830edf2a7d4f0c/manifest.json
```

Do not add `--include-near-silent` unless ordinals 1 and 13 have been reviewed. A
retry still calls the adapter. A pre-existing completed result qualifies for reuse
only after exact output-layout checks, the adapter's immutable-reuse check, and the
catalog-free strict ASR result validator. Those checks rehash the source FLAC, engine,
model, raw engine JSON, and normalized transcript, validate all cross-record and
descriptor-command invariants, and require null catalog/recording coordinates. A
matching filename or result key alone is never reusable.

For a v0.3 manifest, use the matching explicit runner for validate, dry-run, and run:

```sh
pipeline/bin/preprocess-asr-queue-runner-v03 validate \
  --manifest /absolute/private/preprocess-asr-v03-work-orders/queues/asrppqueue_<32-hex>/manifest.json

pipeline/bin/preprocess-asr-queue-runner-v03 run \
  --manifest /absolute/private/preprocess-asr-v03-work-orders/queues/asrppqueue_<32-hex>/manifest.json \
  --dry-run
```

Do not pass a v0.3 manifest to the unversioned v0.2 runner. Completed v0.3 results
must use `pipeline/bin/asr-whispercpp-result-store-seal-v03`; its v0.5 implementation
pins the v0.3 queue validator and manifest schema. The unversioned result-seal command
continues to pin v0.2. Production defaults stay on v0.2 until a reviewed real v0.3
queue completes this full candidate chain.

After a v0.3 run, prepare (but do not apply) its exact result-seal plan with:

```sh
pipeline/bin/asr-whispercpp-result-store-seal-v03 plan \
  --source-mode queue-only-v2 \
  --queue-manifest /absolute/private/preprocess-asr-v03-work-orders/queues/asrppqueue_<32-hex>/manifest.json \
  --result /absolute/private/asr-v03-results/objects/<job>/results/<result-key>/result.json \
  --output-directory /absolute/private/asr-v03-results/sealing-control/plans \
  --store-root /absolute/private/asr-v03-results
```

Repeat `--result` for every process-routed queue member and review the prepared plan
before using that same versioned seal command's `apply` operation. V0.5 treats
persisted filesystem device/inode and ctime/mtime numbers as diagnostic evidence,
not cross-restart identity. Later replay instead requires the sealed paths and tree,
regular types, required modes, link counts, byte counts, exact content hashes,
and higher queue/result lineage. During each live operation it still compares the
retained file descriptors to their path objects and rejects any object, byte, size,
mode, link, or timestamp change in that race window.

The runner emits a summary to standard output but persists no control state. ASR
result import remains a distinct reviewed operation; these artifact-local results
must not receive recording context until a separate translation bridge is admitted.
For a policy-bearing queue, retain the complete runner summary with any result
handoff. Discarding it also discards the policy-aware execution handoff, so the raw
adapter result must not advance on its own.

## Private-output boundary

Queue and ASR output roots must be disjoint owner-private paths on durable storage.
Roots under `/tmp`, `/var/tmp`, repository `public/`, `src/`, `dist/`, or `.git/`
are rejected. The materializer writes JSON only under the queue root. It never copies
the FLAC, proxy, source video, or result envelope and never writes into a public site
path. All operational paths under `research/` remain excluded from Git.

The normative v0.2 queue and runner shapes are
[`preprocess-asr-queue-manifest.schema.json`](schemas/preprocess-asr-queue-manifest.schema.json)
and [`preprocess-asr-queue-run.schema.json`](schemas/preprocess-asr-queue-run.schema.json).
The v0.3 shapes are
[`preprocess-asr-queue-manifest-v03.schema.json`](schemas/preprocess-asr-queue-manifest-v03.schema.json)
and [`preprocess-asr-queue-run-v03.schema.json`](schemas/preprocess-asr-queue-run-v03.schema.json).
Select by exact implementation version and schema hash, never by the schema `$id`.
Runtime exact replay and strict completed-result validation are authoritative.

## Focused tests

```sh
python3 -m unittest pipeline.tests.test_preprocess_asr_queue -v
python3 -m unittest pipeline.tests.test_preprocess_asr_queue_runner -v
python3 -m unittest pipeline.tests.test_preprocess_asr_queue_runner_v03 -v
python3 -m unittest pipeline.tests.test_asr_whispercpp_result_store_seal_v05 -v
python3 scripts/validate-json-contracts.py \
  --validate pipeline/schemas/preprocess-asr-queue-manifest-v03.schema.json \
  /absolute/private/queues/asrppqueue_<32-hex>/manifest.json
```

The focused suites cover deterministic immutable replay, artifact-local and null
catalog-context output, shared-engine-profile gating, catalog-row mutation,
duplicate audio, work-order tampering, near-silent review routing, sequential
fail-fast dispatch, strict result-tree reuse, JSON Schema validation, and public-path
rejection. They also cover ASR-ready batch execution through receipt admission,
explicit evidence-bound no-audio skips, all-ineligible refusal, exact v30
seal-boundary propagation, generic-adapter
rejection of the private wrapper, policy-specific result identity, missing/weakened
policy rejection, and a complete policy-bound runner dry-run. Runner tests use only
tiny files and mocked adapter calls; they execute no real ASR and open no catalog.
