# Contextual-ASR completed-result sealing

`contextual_asr_result_store_seal.py` is a private administrative lane for one
completed, sealed `contextual_asr_batch.py` manifest and the exact result produced by
each of its work orders. It is separate from the historical raw-ASR result sealer and
does not widen or call that implementation.

The lane creates text-free control evidence, then optionally performs a mode-only
transition:

```text
result-key/                         0700 -> 0500
├── result.json                     0600 or 0644 -> 0400
├── transcript.normalized.json      0600 or 0644 -> 0400
└── whisper.raw.json                0600 or 0644 -> 0400
```

Content bytes, file size, inode/device identity, hard-link count, directory entries,
and nanosecond mtime are preserved. A successful receipt records the post-transition
ctime because chmod necessarily changes ctime.

## Closed admission contract

Planning requires one explicit `--result` path for every work order in one contextual
batch manifest. Order of the CLI arguments is irrelevant; results are mapped and
recorded in manifest ordinal order. Planning fails unless:

- the manifest, its batch/work-order directories, and every work-order file replay
  through `contextual_asr_batch.validate_batch` with exact bytes, raw SHA-256,
  canonical SHA-256, modes, single links, ordinals, and closed entries;
- the explicit result set has exactly the manifest work-order count, with no duplicate
  path, duplicate work-order claim, omission, or unselected result;
- every result has the deterministic adapter layout below the manifest's exact private
  ASR output root and its directory contains only the three named files;
- the plan's `store_root` is exactly the manifest-bound private ASR output root, that
  resolved root remains mode `0700`, and plan/receipt paths are the exact derived paths
  beneath its real mode-`0700` control tree;
- directories are real mode `0700`, files are real single-link mode `0600` or `0644`,
  each later validation requires the exact per-file starting mode recorded in the
  plan, paths contain no symlink/traversal component, and retained descriptor/path
  identities survive all validation;
- `contextual_asr_batch._validate_adapter_result` replays the job, input, window,
  glossary, engine, model, recipe, result key, contextual provenance, and manifest
  pairing;
- the corpus importer's catalog-free strict result validator accepts the complete
  envelope and both artifacts; and
- artifact file URIs, byte counts, hashes, private visibility, and the result's raw and
  canonical hashes match the retained files.

All duplicated identity projections are cross-checked rather than trusted separately:
input/job/recipe/result IDs and paths, source ordinals, physical and canonical work
order hashes, glossary revision, processing run, adapter validation, and catalog-free
validation must agree with the retained result and sealed source.

The plan and receipt contain paths, IDs, hashes, byte counts, mode/stat evidence,
counts, and the glossary revision ID. They contain no transcript text, token text,
segment text, glossary terms, preference, accuracy judgment, or human-review claim.
Paths and IDs remain sensitive operational metadata, so control documents stay inside
the private contextual result root. `created_at` and `applied_at` are real calendar
times in the single canonical UTC-second form `YYYY-MM-DDTHH:MM:SSZ`; alternate
offsets, fractional seconds, malformed values, and wrong JSON types fail runtime
validation as well as the schemas.

Separate contextual-diff files are not result artifacts and are outside this lane's
scope.

## No-write dry run

Build an exact argument array from the dedicated result store, then validate without
creating the control tree, plan, receipt, or changing a mode:

```sh
ctx_manifest_path=/absolute/contextual-work-orders/batches/ctxasrbatch_ID/manifest.json
ctx_results_root=/absolute/private-contextual-asr-results
ctx_result_args=()
while IFS= read -r ctx_result_path; do
  ctx_result_args+=(--result "$ctx_result_path")
done < <(find "$ctx_results_root" -type f -name result.json -print | sort)

pipeline/bin/contextual-asr-result-store-seal plan \
  --dry-run \
  --manifest "$ctx_manifest_path" \
  "${ctx_result_args[@]}"
```

The explicit list is intentional. Do not use a root containing unrelated contextual
batches without filtering the list to the named manifest. Exact tuple closure still
rejects an unrelated, missing, or duplicate result.

## Two-phase plan and apply

Remove `--dry-run` to create a canonical mode-`0400` plan under the derived private
path:

```text
<asr-output-root>/contextual-sealing-control/<batch-id>/
├── .contextual-asr-result-seal.lock       # mode 0600
├── plans/ctxasrsealplan_<32 hex>.json
└── receipts/ctxasrsealreceipt_<32 hex>.json
```

```sh
pipeline/bin/contextual-asr-result-store-seal plan \
  --manifest "$ctx_manifest_path" \
  "${ctx_result_args[@]}"
```

Validate that prepared plan independently, then apply it by exact path:

```sh
pipeline/bin/contextual-asr-result-store-seal validate-plan \
  --plan /absolute/.../plans/ctxasrsealplan_ID.json

pipeline/bin/contextual-asr-result-store-seal apply \
  --plan /absolute/.../plans/ctxasrsealplan_ID.json

pipeline/bin/contextual-asr-result-store-seal validate-receipt \
  --receipt /absolute/.../receipts/ctxasrsealreceipt_ID.json
```

Application takes a retained-inode exclusive `flock` before checking for a receipt and
holds it through transition and receipt commit. It retains all source/result
descriptors across revalidation, changes the three file modes before the directory
mode for each result, rehashes and strictly validates everything, and atomically
creates the receipt without replacement. Concurrent cooperating apply processes are
serialized; an existing exact valid receipt makes `apply` an idempotent read-only
replay. If a non-cooperating writer wins the no-replace receipt link, only a fully
validated exact receipt is accepted as terminal evidence; an invalid winner has no
authority and the caller's transition is rolled back.

If every target is already in its exact after mode but no receipt exists—for example,
after interruption immediately before receipt commit—`apply` revalidates all bytes and
may write the missing receipt. A mixed before/after state is never accepted as
success. Neither is an opposite `0600`/`0644` pre-mode accepted merely because it is a
globally supported planning mode: every entry is classified only as its exact recorded
before mode, its exact `0400`/`0500` after mode, or invalid. The tool attempts to
restore every exact mode recorded in the plan and exits without a receipt for any
mixed/invalid state. A transition or pre-receipt validation failure receives the same
best-effort rollback. Because rollback chmod changes ctime, discard the old plan and
create a fresh one before retrying. Any reported rollback failure is an administrative
incident requiring inspection before another run.

## Authority boundary

The CLI has only `plan`, `validate-plan`, `apply`, and `validate-receipt` commands. It
does not execute ASR, compare hypotheses, choose a transcript, grant human review,
open or write a database, admit a catalog row, assign identity, publish, download, or
use a network client. Its validator call is catalog-free. A valid plan or receipt is
only immutable-storage evidence.

## Tests

```sh
PYTHONPATH=pipeline python3 -m unittest \
  pipeline.tests.test_contextual_asr_result_store_seal -v
python3 scripts/validate-json-contracts.py
```

The disposable suite covers schema-valid text-free plans and receipts, exact-set
closure, exact per-entry `0600`/`0644` replay, no-write dry run,
mode/content/mtime preservation, exact idempotent replay, rollback after an injected
chmod failure, invalid aggregate modes, interrupted mixed-state recovery, concurrent
apply serialization, valid and invalid receipt-link races, alternate-root and
duplicated-ID plan forgeries, atomic no-overwrite control writes, path-swap races,
canonical timestamp forgeries, hard links, symlinks, and extra entries. It never opens
the live catalog or applies to private production results.

Normative schemas are
[`schemas/contextual-asr-result-seal-plan.schema.json`](schemas/contextual-asr-result-seal-plan.schema.json)
and
[`schemas/contextual-asr-result-seal-receipt.schema.json`](schemas/contextual-asr-result-seal-receipt.schema.json).
