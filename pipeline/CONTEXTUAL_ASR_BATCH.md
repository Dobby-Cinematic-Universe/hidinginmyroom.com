# Private paired contextual whisper.cpp ASR batches

`contextual_asr_batch.py` materializes and serially dispatches a contextual-ASR
pass paired to already completed raw `asr_whispercpp.py` results. It is an
execution-control and provenance lane. It is not transcript review, accuracy
measurement, catalog admission, or publication.

The intended first use is a bounded full-input pilot: explicitly name each raw
baseline, bind one exact neutral spelling glossary, and preserve every other ASR
variable. Raw and contextual hypotheses remain separate. This lane does not decide
which wording is correct and does not manufacture a human-review state.

## Admission contract

Every `--baseline-result` must be an absolute or resolvable local path to the
current adapter's completed, non-dry, no-glossary `result.json`. Its result directory
must be sealed exactly as follows:

```text
<raw-result-key>/                   # mode 0500, no symlink
├── result.json                     # mode 0400, one hard link
├── transcript.normalized.json      # mode 0400, one hard link
└── whisper.raw.json                # mode 0400, one hard link
```

No other directory entry is accepted. The result must pass the corpus importer's
catalog-free envelope validator while retaining raw-ASR/null-glossary provenance.
Its window must cover the complete normalized input (`offset_ms: 0`, duration equal
to the probed input duration). The normalized FLAC, engine executable, model, raw
result, and both artifacts are re-read and SHA-256 checked. The engine must match the
shared `current_new_batch` profile; a legacy replay-only profile is rejected.

All baselines in one batch must use byte-identical engine, model, and decoding
documents. `inference.language` must be concrete (not `auto`) and equal the glossary
language. Duplicate paths and duplicate contextual pair targets are rejected.

The glossary is an existing neutral-glossary-v1 JSON file outside both output roots.
It must be a resolved, regular, single-link file with exact mode `0400`. Its exact raw
bytes, canonical JSON bytes, revision text (by digest), constructed neutral prompt
(by digest), language, revision ID, and term count are bound. Glossary terms are not
copied into the manifest or run summary.

## Pairing and text-free control metadata

For each accepted raw result, the emitted work order exactly preserves:

- normalized input path, SHA-256, media/artifact IDs, and parent run;
- current engine path, SHA-256, source/build identity, and model byte identity;
- full-input window and every decoding parameter; and
- the nullable catalog context.

Only the deterministic contextual job ID, private output root, and exact glossary
reference differ. The manifest binds the raw and canonical result envelope hashes,
both raw artifact hashes, exact input/engine/model/glossary bytes, paired projection,
and implementation source bytes. Input argument order does not affect the batch ID.

The manifest intentionally contains no transcript text and no glossary term strings.
Work orders contain only the glossary path and digest, so their control JSON also
does not duplicate the terms. Paths and IDs can still be sensitive operational
metadata; all outputs are private.

## Materialize

Run from the repository root. Inputs are deliberately explicit; this command never
scans a directory, database, or catalog:

```sh
pipeline/bin/contextual-asr-batch materialize \
  --baseline-result /absolute/private/raw/result-a/result.json \
  --baseline-result /absolute/private/raw/result-b/result.json \
  --glossary /absolute/private/glossaries/himr-neutral-en-v1.json \
  --batch-root /absolute/private/contextual-work-orders \
  --asr-output-root /absolute/private/contextual-results
```

The materializer creates owner-private roots and atomically seals a
content-addressed layout:

```text
<batch-root>/
├── .contextual-asr-batch.lock       # mode 0600
└── batches/
    └── ctxasrbatch_<32 hex>/         # mode 0500
        ├── manifest.json             # mode 0400, one hard link
        └── work-orders/              # mode 0500
            ├── 000001.json           # mode 0400, one hard link
            └── ...
```

Re-running the same command is an exact replay check. It revalidates all sources and
sealed bytes and never overwrites a batch. An intentional input, glossary, software,
or output-root change creates a different identity.

## Validate and run

Validation reconstructs the full manifest and every paired order from the currently
sealed baselines and glossary:

```sh
pipeline/bin/contextual-asr-batch validate \
  --manifest /absolute/private/contextual-work-orders/batches/ctxasrbatch_<id>/manifest.json
```

An adapter dry run probes every input and validates executable, model, glossary, and
command construction without creating ASR result directories:

```sh
pipeline/bin/contextual-asr-batch run \
  --manifest /absolute/private/contextual-work-orders/batches/ctxasrbatch_<id>/manifest.json \
  --dry-run
```

Remove `--dry-run` to execute whisper.cpp. Dispatch is single-process, ordinal,
sequential, and fail-fast. The runner verifies each raw baseline immediately before
and after its paired adapter call, then replays the full batch after the last job.
Completed content-addressed adapter results may be reused by the adapter on replay.
A failure summary contains only the completed/planned prefix, failed ordinal and
digests, error details, and an optional quarantine receipt. There is no batch-level
rollback of completed private results.

The produced contextual adapter results are deliberately not sealed or admitted to
the catalog by this controller. Comparing raw and contextual hypotheses, measuring a
term-level effect, choosing a preferred transcript, adding a catalog registry, and
recording human review are separate future operations.

## Safety boundary

- The controller has no database code, catalog write, downloader, HTTP client,
  publication operation, identity authority, review authority, or transcript
  preference authority.
- “No network access by controller” is a software contract, not an operating-system
  firewall around caller-provided `ffprobe` or whisper.cpp executables. Use an
  externally network-denied sandbox when enforcement is required.
- `/tmp`, `/var/tmp`, symlinks, writable sealed inputs, multi-link sealed files,
  overlapping roots, unknown immutable entries, changed pins, contextual baselines,
  partial-input baselines, mixed decoding policies, and legacy engines fail closed.
- Manifest and ASR roots must be disjoint, owner-private locations on durable local
  storage. Do not place secrets in job IDs, filenames, or paths.

Normative JSON Schemas are
[`schemas/contextual-asr-batch-manifest.schema.json`](schemas/contextual-asr-batch-manifest.schema.json)
and
[`schemas/contextual-asr-batch-run.schema.json`](schemas/contextual-asr-batch-run.schema.json).
Generated paired work orders also validate against the existing
[`schemas/asr-whispercpp-work-order.schema.json`](schemas/asr-whispercpp-work-order.schema.json).
Runtime exact-byte replay is authoritative and does not depend on `jsonschema`.

## Tests

```sh
PYTHONPATH=pipeline python3 -m unittest \
  pipeline.tests.test_contextual_asr_batch -v
python3 scripts/validate-json-contracts.py
```

The focused suite creates tiny normalized FLAC inputs and genuine completed current
raw adapter envelopes using a pinned fake whisper executable. It covers schema-valid
materialization and run summaries, input-order independence, byte-exact replay,
text-free metadata, paired projection preservation, completed serial execution,
baseline immutability, fail-fast partial summaries, current/raw/full-input admission,
mixed-policy rejection, permission/hard-link/symlink/extra-entry tampering, duplicate
targets, root isolation, and the controller's no-database/no-network import boundary.
