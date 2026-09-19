# Private resumable preprocessing batches

[`preprocess_batch.py`](preprocess_batch.py) converts an explicitly reviewed set of
completed acquisition `result.json` files into ordinary
version-1 media-preprocess work orders. New bundles pin the single-link reuse
successor [`media_preprocess_v034.py`](media_preprocess_v034.py); completed legacy
bundles continue to replay against [`media_preprocess.py`](media_preprocess.py). It is a local
control layer: it does not download, publish, open a corpus database, import catalog
rows, create transcripts, infer identities, or make content claims.

No real corpus batch is materialized or executed by the implementation tests. The
tests use tiny synthetic byte files and a fake producer result.

Version 0.2 adds a fail-closed bridge for v30 private local acquisitions. An
acquisition result that carries `handling_policy` is rejected unless
`create-selection` is also given its owner-only artifact root and exact private
acquisition seal receipt. The receipt is replayed against the current work order,
result, media bytes, directory modes, and policy; it is never treated as a bearer
token. The selection pins the full policy and receipt lineage, the immutable batch
manifest exposes a digest-bound `handling_control`, every completed batch receipt
copies the exact handling boundary, and status/dry-run output repeats the control.
Legacy/public acquisition results without a `handling_policy` retain their original
selection and work-order behavior and may not be paired with a private seal receipt.

Version 0.3 adds uniform `asr-ready` and `enrichment-only` operation lanes. They
retain the same exact source, profile, producer, and receipt boundaries. `asr-ready`
creates only the normalized audio and probe needed by ASR; `enrichment-only` creates
the probe, review proxy, and scene/silence routing without regenerating normalized
audio. A batch cannot mix operation lanes; legacy 0.1/0.2 bundles continue to replay
only as the original full profile.

## State model

The workflow has three separately pinned layers:

```text
mode 0400 selection.json
  └─ exact acquisition-result bytes + exact source-media bytes
       └─ mode 0500 bundle / mode 0400 manifest and work orders
            └─ mode 0700 run state / one mode 0400 receipt per completed item
```

The selection is canonical and independent of argument order. Each row records the
absolute acquisition-result path and SHA-256, acquisition job/work-order identity,
and the source media path, media ID, SHA-256, byte count, duration, and catalog time.
Every selection read reopens and hashes the exact acquisition result and entire media
object; writable upstream acquisition files are never treated as immutable merely
because their path is unchanged.

The bundle pins the exact selection file bytes and semantic digest, the complete
tracked `cpu-balanced-v1` profile and file digest, `media_preprocess.py` bytes and
version (or the active single-link successor bytes and version), full
FFmpeg/FFprobe executable and build provenance, and the private output
root. Its work orders are reconstructed during every validation. Re-materializing an
unchanged selection with the same runtime pins and output root returns the same bundle
and requires byte-identical existing files.

A receipt is the only completion marker. It binds one selection row and work order to
the deeply validated immutable producer result and every artifact path, size, and
SHA-256. Status and replay revalidate all acquisition result/media hashes, result and
artifact hashes, producer semantics, and runtime pins. A missing, writable, extra, or
altered receipt leaves no trusted completion state and fails closed.

## Bounds and exclusions

- A selection contains 1–128 unique media objects. Exact duplicate media is rejected.
- Selection JSON is capped at 4 MiB, manifests at 8 MiB, work orders at 256 KiB,
  producer results at 64 MiB, receipts at 4 MiB, and receipt artifacts at 16.
- JSON must be finite UTF-8 with no duplicate keys and bounded 64-bit integers. Local
  paths are absolute and capped at 16,384 characters; URL paths are rejected.
- The tracked four-thread `cpu-balanced-v1` profile is mandatory. The default `full`
  lane runs probe, FLAC, proxy, and routing. The `asr-ready` lane runs probe and FLAC;
  `enrichment-only` runs probe, proxy, and routing with FLAC disabled. Arbitrary
  operation combinations and mixed-lane bundles are rejected.
- A source without an audio stream completes preprocessing with an explicit
  `audio_flac: not_applicable` step and no invented FLAC. The explicit v0.3 ASR-queue
  successor replays that evidence, records the receipt as `source_has_no_audio`, and
  skips only that item. The production/default v0.2 queue remains byte-frozen for
  historical replay and rejects such a batch instead of applying v0.3 semantics. If
  an audio-bearing source has FLAC enabled but the artifact is missing, v0.3 still
  fails closed. A batch containing no eligible normalized audio cannot produce a
  v0.3 ASR queue.
- Execution is sequential. One new item per invocation is the default; `--limit` may
  be 1–128, but never introduces concurrent execution.
- All safety records fix visibility to private, network and credentials to forbidden,
  publication authority to none, identity claims to false, source preservation to
  true, and automatic catalog admission to false.
- A v30 handling policy can only become more restrictive upstream. Batch control
  accepts exactly `no_publication_authority` or `never_publish`, requires authority
  `none`, retains the policy basis verbatim, and requires
  `source_byte_identity_claimed: false`. Dropping, changing, or inventing the policy,
  receipt, plan, source/media pins, or seal digest fails validation before execution.
- Raw `media_preprocess` producer results are not standalone publication or catalog
  authority. For a v30 batch, retain and review its immutable selection, manifest,
  and item receipt together. No catalog import should consume the raw producer result
  independently of the acquisition restriction and batch-control lineage.
  The current legacy `preprocess-asr-queue` lane therefore rejects a manifest with
  `handling_control` unless its policy-aware v0.2 contract can replay and propagate
  every boundary. That lane seals affected adapter orders inside v30 private wrappers,
  gives them policy-specific job identities, and repeats the control in runner output;
  see [`PREPROCESS_ASR_QUEUE.md`](PREPROCESS_ASR_QUEUE.md).
- Child processes are limited to the hash-pinned FFmpeg/FFprobe executables, receive a
  locale-only environment with no inherited credentials, and reject explicit URL
  arguments. The implementation has no socket, downloader, HTTP, publication, or DB
  code. A production worker may additionally use an OS network namespace as defense
  in depth against vulnerabilities or implicit references parsed from hostile media.

## Private roots and exact creation behavior

Selection parents, bundle roots, processing roots, and state roots must be owned by
the current user and have exact mode `0700`. Existing `0755` roots are deliberately
rejected; the program never weakens or silently chmods an existing directory. Missing
root components are created as `0700` under umask `077` only when the corresponding
write command runs.

The materializer creates the bundle root, `bundles/`, and staging directories as
`0700`, then atomically admits a final bundle directory as `0500`; its manifest and
work orders are `0400`, and `work-orders/` is `0500`. It validates but does not create
the processing output root. A nonexistent processing root is allowed; if it already
exists at materialization or bundle-validation time, it must be an exact non-symlink
directory owned by the current user with mode `0700`. A non-dry runner creates a
missing root and its missing private components as `0700`. It creates state
directories as `0700`, locks as `0600`, and receipts as `0400`. Producer artifacts
remain their native sealed read-only mode inside the `0700` processing tree.
Historical v0.3.3 bundles may contain verified run-local hard links; newly
materialized bundles pin the hash-anchored single-link successor, which uses a
distinct reflinked or byte-copied inode for every reused artifact. Completed legacy
bundles remain replayable, while a legacy bundle with pending work fails closed rather
than creating another hard link.

The admitted `bundles/<bundle-id>` path itself and every private root must be exact
non-symlink paths. Validation rejects a same-ID bundle reached through a symlink even
when the target's contents and modes are otherwise valid. Exact replay is confined to
the original `bundles/` admission directory and never follows a substituted final
path.

`validate-*`, `status`, and `run --dry-run` create nothing. Output roots `/`, `/tmp`,
`/var/tmp`, and repository `public/`, `src/`, `dist/`, or `.git/` trees are rejected.
Repository-local private roots under the ignored `research/` tree are allowed.

For example, deliberately create new roots rather than reuse the existing mode-0755
`research/corpus/processed` directory:

```sh
HIMR_REPO=$(pwd -P)
install -d -m 700 "$HIMR_REPO/research/corpus/preprocess-batch-control"
install -d -m 700 "$HIMR_REPO/research/corpus/processed-batches"
install -d -m 700 "$HIMR_REPO/research/corpus/preprocess-batch-state"
```

## 1. Seal an explicitly reviewed selection

Pass every intended completed acquisition result explicitly. Do not use a broad glob:

```sh
pipeline/bin/preprocess-batch create-selection \
  --acquisition-result /absolute/acquired/jobs/job-000001/<work-order-sha256>/result.json \
  --acquisition-result /absolute/acquired/jobs/job-000002/<work-order-sha256>/result.json \
  --output "$HIMR_REPO/research/corpus/preprocess-batch-control/archive-short-001.selection.json"
```

For the currently reviewed 20-job Archive cohort, job
`acq-8ab5bd7b16687ea354cf76e18866eb9d-000018` is the already-processed
`KOl7XwrQr2c` object. Omit that entire job, pass the other 19 exact result paths, and
require `item_count: 19` in the success response. This is an operational exclusion,
not an automatic title/content policy in the generic tool.

Review the sealed job IDs before materialization, then run the full hash pass again:

```sh
jq -r '.entries[] | [.ordinal, .acquisition_result.job_id, .source_media.media_id] | @tsv' \
  "$HIMR_REPO/research/corpus/preprocess-batch-control/archive-short-001.selection.json"

pipeline/bin/preprocess-batch validate-selection \
  --selection "$HIMR_REPO/research/corpus/preprocess-batch-control/archive-short-001.selection.json"
```

For v30 local-file results with a private handling policy, pass the common reviewed
artifact root and one exact receipt for every selected private result. Receipt order
does not matter: each receipt is matched to the selected result path in its sealed
plan. The example intentionally names each file and does not use a glob:

```sh
pipeline/bin/preprocess-batch create-selection \
  --acquisition-result /absolute/private-acquisition/cache/jobs/job-a/<sha>/result.json \
  --acquisition-result /absolute/private-acquisition/cache/jobs/job-b/<sha>/result.json \
  --private-acquisition-root /absolute/private-acquisition \
  --private-seal-receipt /absolute/private-acquisition/seals/receipts/a.json \
  --private-seal-receipt /absolute/private-acquisition/seals/receipts/b.json \
  --output /absolute/private-control/reviewed.selection.json
```

The private acquisition root, every sealed artifact directory, and the receipt's
ancestor chain must remain exact owner-only mode `0700`; sealed acquisition files and
the receipt must remain mode `0600`. Selection validation replays those requirements
and re-hashes the media rather than trusting the stored receipt.

## 2. Materialize immutable work orders

```sh
pipeline/bin/preprocess-batch materialize \
  --selection "$HIMR_REPO/research/corpus/preprocess-batch-control/archive-short-001.selection.json" \
  --bundle-root "$HIMR_REPO/research/corpus/preprocess-batch-control" \
  --processing-output-root "$HIMR_REPO/research/corpus/processed-batches"
```

For the latency-sensitive transcript lane, use a separate output/control epoch and
request only ASR-ready artifacts:

```sh
pipeline/bin/preprocess-batch materialize \
  --selection "$HIMR_REPO/research/corpus/preprocess-batch-control/archive-short-001.selection.json" \
  --bundle-root "$HIMR_REPO/research/corpus/preprocess-asr-ready-control" \
  --processing-output-root "$HIMR_REPO/research/corpus/preprocess-asr-ready-output" \
  --lane asr-ready
```

Run a later enrichment-only bundle against the same exact source selection when
proxy, scene-routing, OCR, or visual work is scheduled:

```sh
pipeline/bin/preprocess-batch materialize \
  --selection "$HIMR_REPO/research/corpus/preprocess-batch-control/archive-short-001.selection.json" \
  --bundle-root "$HIMR_REPO/research/corpus/preprocess-enrichment-control" \
  --processing-output-root "$HIMR_REPO/research/corpus/preprocess-enrichment-output" \
  --lane enrichment-only
```

The ASR-ready and enrichment recipes are independent immutable derivations; neither
silently upgrades or replaces the other, and delayed enrichment does not repeat the
normalized-audio decode. Use the default `full` lane only when one intentionally
wants all artifacts in a single recipe.

The response exposes only the bundle ID and count. The bundle path is
`<bundle-root>/bundles/<bundle-id>`. Repeating the command is an exact replay check;
it does not replace the existing bundle.

Validate the bundle independently before scheduling work:

```sh
pipeline/bin/preprocess-batch validate-bundle \
  --bundle "$HIMR_REPO/research/corpus/preprocess-batch-control/bundles/<bundle-id>"
```

## 3. Validate a no-write run, then execute incrementally

```sh
pipeline/bin/preprocess-batch run \
  --bundle "$HIMR_REPO/research/corpus/preprocess-batch-control/bundles/<bundle-id>" \
  --state-root "$HIMR_REPO/research/corpus/preprocess-batch-state" \
  --dry-run

pipeline/bin/preprocess-batch run \
  --bundle "$HIMR_REPO/research/corpus/preprocess-batch-control/bundles/<bundle-id>" \
  --state-root "$HIMR_REPO/research/corpus/preprocess-batch-state"
```

The second command processes at most one pending ordinal. Repeat it until `status` is
`complete`, or use a cautious larger sequential count such as `--limit 3`. Inspect
without creating or advancing state with:

```sh
pipeline/bin/preprocess-batch status \
  --bundle "$HIMR_REPO/research/corpus/preprocess-batch-control/bundles/<bundle-id>" \
  --state-root "$HIMR_REPO/research/corpus/preprocess-batch-state"
```

These commands intentionally favor integrity over speed: each invocation reads and
hashes all selected source media, and status/replay also re-hashes and semantically
validates every receipted result and artifact. A larger sequential `--limit` reduces
repeated full-batch validation passes without adding concurrency.

No command in this workflow imports the result into the corpus catalog. That remains
a later, explicit, separately reviewed action.

## Resume and failure semantics

The runner takes a nonblocking per-bundle OS lock, revalidates the complete bundle and
all inputs under that lock, skips only valid receipts, and stops at the first failure.
It never emits a success receipt for a failed or planned producer result. Completed
receipts make later invocations state-stable: no producer call occurs and
`state_sha256` stays identical.

There is one intentional crash window. The active media-preprocess producer atomically
seals its result before the batch receipt is admitted. If the process dies between
those two events, the next invocation sees the item as pending and may create a new
producer execution that reuses the previously verified artifacts. The active successor
publishes those reuse artifacts as distinct single-link inodes. This preserves evidence
and does not overwrite anything, but the ultimately receipted processing-run ID can
differ from the abandoned unreceipted execution. Quarantine malformed producer state
instead of deleting or editing it in place.

State-directory and receipt admission are independently crash-resumable. An existing
empty `runs/<bundle-id>/` directory without `receipts/` is treated as an incomplete
initialization: read-only status reports every item pending, and the next non-dry run
creates the missing mode-0700 directory. Any other entry in that incomplete run
directory is rejected.

Receipt admission reserves only names matching
`.NNNNNN.json.tmp-<pid>-<32-hex-nonce>` (and the earlier pid-only form). A crash may
leave a mode-0600 partial temporary before linking, a mode-0400 complete orphan, or a
temporary hard-linked to its final mode-0400 receipt. Read-only status never counts a
temporary as completion; it may validate the final peer of the exact two-link crash
case. Under the bundle writer lock, the next non-dry run removes only bounded,
owner-owned, regular reserved temporaries with safe modes and link topology, fsyncs
the directory, and then resumes. Symlinks, excessive temporaries, unsafe hard links,
and every unrelated extra filename fail closed rather than being ignored or treated
as receipts.

## Contracts and tests

The runtime validators are authoritative. The corresponding strict development
contracts are:

- [`schemas/preprocess-batch-selection.schema.json`](schemas/preprocess-batch-selection.schema.json)
- [`schemas/preprocess-batch-manifest.schema.json`](schemas/preprocess-batch-manifest.schema.json)
- [`schemas/preprocess-batch-receipt.schema.json`](schemas/preprocess-batch-receipt.schema.json)
- [`schemas/preprocess-batch-run-summary.schema.json`](schemas/preprocess-batch-run-summary.schema.json)

Run the focused synthetic suite with:

```sh
python3 -m unittest -v pipeline.tests.test_preprocess_batch
```
