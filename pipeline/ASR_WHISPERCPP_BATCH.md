# Private raw whisper.cpp ASR batches

`asr_whispercpp_batch.py` turns completed, sealed local-window results into a
deterministic batch of immutable work orders for the existing
[`asr_whispercpp.py`](asr_whispercpp.py) adapter. It is a private execution-control
layer, not a catalog importer or a publication path.

The materializer is deliberately narrow:

- input is one to 64 completed local-window `result.json` files;
- every result directory must still be mode `0500`, and every result/artifact must be
  a single-link regular file with mode `0400`;
- the materializer, existing ASR adapter, and shared engine-profile source are retained
  by exact path, byte count, implementation version, and SHA-256;
- the exact result and every listed artifact are read through stable descriptors and
  rehashed twice around the catalog snapshot;
- the SQLite catalog is opened with `mode=ro`, `query_only`, a write-denying
  authorizer, and an explicit read transaction;
- every audio artifact must resolve uniquely through its completed catalog-admission
  processing run, verified private media location, rendition, non-merged recording,
  full-media timeline span, and source-media derivation;
- the model must be the existing registered `small.en` row and exact immutable model
  registry manifest, and both the model weights and whisper.cpp executable must match
  their reviewed byte pins; and
- the raw pass is English transcription with a null glossary. It makes no identity
  assertion and has no publication authority.

The manifest retains full selected catalog-row snapshots and their canonical digest.
Validation reconstructs the entire manifest and every ASR work order from the sealed
inputs and current read-only catalog rows. A changed result, artifact, catalog binding,
model registry row, executable, weight file, work order, permission bit, hard-link
count, or directory entry fails closed.

## Time coordinates

A local-window FLAC is already an extracted interval, so every emitted ASR work order
uses `offset_ms: 0`. Its `duration_ms` is the exact duration retained by the sealed
audio artifact's normalized probe, including a partial tail; it is never replaced by
the nominal source-window length and is never left null.

The upstream source offset is **not** copied into the adapter's artifact-local ASR
coordinates. It remains recoverable from all of these sealed bindings:

1. the work order's `artifact_id` and `parent_processing_run_id`;
2. the catalog artifact/rendition/media-derivation metadata; and
3. `work_orders[].local_window_result.source_time_mapping` in the batch manifest.

Downstream catalog admission or transcript mapping must use that lineage when
converting artifact-local timestamps to source time. It must not interpret a local ASR
timestamp as a recording-global timestamp.

## Materialize the eight `gPhrE99xwqI` jobs

Run from the repository root. The long paths are intentionally explicit; a shell glob
or directory scan is not part of the reviewed selection.

```sh
pipeline/bin/asr-whispercpp-batch materialize \
  --db research/corpus/corpus-v8.sqlite3 \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000001/result.json \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000002/result.json \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000003/result.json \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000004/result.json \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000005/result.json \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000006/result.json \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000007/result.json \
  --local-window-result research/corpus/local-window-artifacts/youtube-gPhrE99xwqI/windows/24/24985b3cb1cb2008e8251221401debc9b97d9d0bae5dc52237329e61c53b3281/windowbundle_2ca7e9fec2d52d27d59b5fecd692d7b6/window_000008/result.json \
  --batch-root research/corpus/private-asr-work-orders \
  --asr-output-root research/corpus/private-asr-results \
  --engine research/corpus/tools/whisper.cpp-v1.8.7/build-noccache/bin/whisper-cli \
  --model research/corpus/tools/whisper.cpp/models/ggml-small.en.bin
```

Input order does not affect the batch identity. The materializer sorts resolved result
paths, derives stable job and batch IDs from exact identities, and seals this layout:

```text
<batch-root>/
├── .asr-whispercpp-batch.lock
└── batches/
    └── asrbatch_<32 hex>/                 # mode 0500
        ├── manifest.json                  # mode 0400
        └── work-orders/                   # mode 0500
            ├── 000001.json                # mode 0400
            └── ...
```

New v0.2.0 batches accept only the exact shared `current_new_batch` engine profile:
whisper.cpp v1.8.7 at source revision
`48f628a84833905ee4a0658ee6d4a5c915ce1997`, executable SHA-256
`36be94accd60116933073e8069964c02e888e3681967184ce7fecfd5f980ae1a`, and 1,020,432
bytes. That upstream line includes token-boundary UTF-8 merging for full JSON output.
The old exact v1.8.3 profile remains allowlisted only to reconstruct and validate
already sealed v0.1.0 manifests with their historical materializer/adapter identities;
the runner will not dispatch them. A retry or recovery must materialize a new explicit
input subset under the current profile, preserving old manifests as provenance.

Re-running `materialize` is an exact replay check. It never replaces an existing
batch. The current reviewed eight inputs produce eight work orders totaling
160,111,673 audio bytes and 13,319,825 audio milliseconds. Recompute and record the
printed batch ID after any intentional catalog or contract change instead of relying
on a copied ID.

## Historical v1.8.3 validation only

The sealed v0.1.0/v1.8.3 batch remains reproducibly validation-compatible at its exact
historical path:

```sh
pipeline/bin/asr-whispercpp-batch validate \
  --manifest research/corpus/private-asr-work-orders/batches/asrbatch_a98bf085d254a67bb30b837b307436f6/manifest.json
```

Do not pass that manifest to `run`: both dry and non-dry batch dispatch reject it
before calling the adapter or creating an ASR output root. Direct adapter execution of
its exact v1.8.3 work orders is also validation-only and rejected.

## Validate, plan, and run a current v1.8.7 batch

After materializing a new v0.2.0 manifest with the v1.8.7 engine above, validation
rehashes the model, every local-window result, every audio/proxy artifact, the selected
catalog graph, the manifest, and every work order:

```sh
pipeline/bin/asr-whispercpp-batch validate \
  --manifest research/corpus/private-asr-work-orders/batches/asrbatch_<32-hex>/manifest.json
```

An adapter-level dry run also invokes `ffprobe` on every FLAC and constructs each
exact whisper.cpp command, but creates no ASR result directory. The adapter retains
verified descriptors for the executable, model, FLAC, and optional glossary; FFprobe
and whisper.cpp receive applicable inputs through Linux `/proc/self/fd` plus
`pass_fds`. Batch dispatch therefore fails closed on hosts without that proc transport.
The result `commands` are the exact child-facing descriptor argv, while canonical
processing-run environment provenance retains deterministic logical paths and the
executed/planned state:

```sh
pipeline/bin/asr-whispercpp-batch run \
  --manifest research/corpus/private-asr-work-orders/batches/asrbatch_<32-hex>/manifest.json \
  --dry-run
```

Remove `--dry-run` only when ready for the CPU work:

```sh
pipeline/bin/asr-whispercpp-batch run \
  --manifest research/corpus/private-asr-work-orders/batches/asrbatch_<32-hex>/manifest.json
```

Dispatch is single-process, ordinal, sequential, and fail-fast. A failure produces a
structured batch summary with the validated completed prefix, exact failed ordinal and
work-order digest, error type/message, and an optional invalid-UTF-8 quarantine receipt
summary. The existing ASR adapter retains completed results immutably and reuses a
byte-valid result when the same job is resumed. Replaying the same current manifest is
therefore deterministic after a corrected external resource is installed. To process
only failed or selected windows, materialize an explicit sealed subset of the original
local-window results; stable per-input ASR job identity is preserved while the subset
receives its own batch identity. Silent manifest editing or implicit skipping is not a
resume mechanism. There is no batch-level rollback of already completed ASR output.
Before a non-dry dispatch, the batch runner creates or verifies the ASR output root as
an owner-only `0700` traversal boundary; validation and dry-run do not create it.

Materialization and validation hash roughly 487 MB of model weights plus all selected
window artifacts. The existing adapter hashes the executable, model, and FLAC again
for each job. Expect the validation passes to consume disk bandwidth before CPU ASR
begins. Each job has a 7,200-second adapter timeout, and the runner does not add
parallelism.

## Safety boundary

- URLs, `/tmp` roots, symlinks, writable sealed inputs, extra batch entries, duplicate
  result/artifact identities, ambiguous renditions, disputed/merged recordings, and
  unregistered or changed model weights are rejected.
- The materializer and runner never import ASR results into SQLite, alter a publication
  decision, or copy artifacts into the public site. Any later corpus import is a
  separate reviewed operation.
- The code contains no downloader or network client, and the batch says network use is
  forbidden. This is a contract boundary, **not an operating-system firewall around
  the caller-provisioned executable**. Run on a disconnected host or in an external
  network-denied sandbox when enforcement is required.
- The SQLite connection is read-only, but another process can legitimately append to
  the live catalog. The runner validates the exact selected rows before dispatch and
  again after dispatch. A concurrent change invalidates replay; it never grants the
  private ASR output publication authority.
- Batch and ASR output roots must be disjoint, owner-private paths on durable storage.
  Everything under `research/` is excluded from Git by the repository ignore policy.

The normative schemas are
[`schemas/asr-whispercpp-batch-manifest.schema.json`](schemas/asr-whispercpp-batch-manifest.schema.json)
and
[`schemas/asr-whispercpp-batch-run.schema.json`](schemas/asr-whispercpp-batch-run.schema.json).
Runtime replay is authoritative and does not require `jsonschema`.

## Tests

```sh
PYTHONPATH=pipeline python3 -m unittest \
  pipeline.tests.test_asr_whispercpp_batch -v
python3 scripts/validate-json-contracts.py
pipeline/tests/run.sh
```

The focused suite uses tiny repository-local FLAC/model/executable fixtures. It checks
deterministic ordering and replay, exact artifact-local duration, retained source-time
lineage, existing-adapter dry-run compatibility, byte-identical read-only catalog use,
model-registry and catalog mutation rejection, result/artifact tampering, symlinks,
permissions, duplicate inputs, root overlap, extra immutable entries, explicit subset
identity, schema-valid partial failure summaries, profile admission, and ordinal
fail-fast dispatch without running real ASR. It also validates the real sealed v0.1.0
v1.8.3 manifest against its historical software identity without requiring the current
source files to retain their old hashes.
