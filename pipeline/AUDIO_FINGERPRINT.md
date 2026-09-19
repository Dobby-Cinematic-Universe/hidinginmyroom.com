# Offline audio fingerprints and clip candidates

`audio_fingerprint.py` extracts private FFmpeg/Chromaprint evidence from an exact,
sealed normalized-audio artifact. It is an offline routing aid for later clip-to-parent
research. It does not download media, identify a recording, merge records, or publish
an artifact. Version 1 uses only Python's standard library and the host FFmpeg
Chromaprint muxer; it does not add a fingerprint-matching package.

## Engine and input pins

Capture the installed engine pins:

```sh
pipeline/bin/audio-fingerprint inspect-engine > /private/work-orders/ffmpeg-pin.json
```

The command resolves and hashes the FFmpeg executable and retains the complete
`-version` and `-h muxer=chromaprint` outputs. A work order pins the executable SHA-256,
byte count, version-output SHA-256, muxer-help SHA-256, and first version line. The
producer rechecks the executable before and after extraction. A changed binary, build
report, or muxer report fails closed.

The input must be a resolved, non-symlink, sealed local file with exact SHA-256, byte
count, media ID, normalized-audio artifact ID, parent preprocessing-run ID, duration,
and the preprocessing contract's 16 kHz/mono/s16 declaration. The catalog importer
independently rehashes the current input and requires the media object and private
artifact rows to match all of those dependencies.

Use
[`schemas/audio-fingerprint-work-order.schema.json`](schemas/audio-fingerprint-work-order.schema.json)
and the illustrative
[`examples/audio-fingerprint-work-order.example.json`](examples/audio-fingerprint-work-order.example.json).
The example hashes are placeholders, not runnable pins.

## Windows and half-open time

Three deterministic selections are supported:

- `full_track` emits `[0, duration_ms)`.
- `explicit_windows` retains the caller's unique window IDs and exact
  `[start_ms, end_ms)` bounds.
- `fixed_chunks` expands from zero using the declared chunk and hop. A final partial
  chunk is retained only when both tail controls allow it.

All bounds satisfy `0 <= start_ms < end_ms <= duration_ms`. At exactly 16 kHz, every
millisecond is 16 samples. FFmpeg therefore receives
`atrim=start_sample=...:end_sample=...`, not floating-point seek timestamps. Expanded
windows and complete command arrays remain in the result.

```sh
pipeline/bin/audio-fingerprint run \
  --work-order /private/work-orders/fingerprint-001.json
```

`--dry-run` performs local file/tool admission and prints the deterministic recipe and
commands without making an output directory.

## Raw artifacts and execution identity

The producer asks FFmpeg for `-f chromaprint -fp_format raw -algorithm <n>` and stores
the returned bytes unchanged. The format label is
`ffmpeg_chromaprint_fp_format_raw`; the algorithm, FFmpeg build, normalization,
half-open window, raw digest, byte count, and word count are explicit. The code does
not decode and re-encode the raw format, so its meaning stays tied to the pinned
FFmpeg/Chromaprint implementation rather than an assumed portable integer encoding.

The deterministic recipe identity covers exact input bytes/duration, engine hash/build
evidence, algorithm/format, selection, expanded windows, and runtime parameters. A
fresh nonce creates a distinct processing-run ID per execution. Artifacts live at:

```text
<output-root>/audio-fingerprint/ffmpeg-chromaprint/
  sha256/<input-prefix>/<input-sha>/recipes/<recipe-sha>/
  executions/<run-id>/artifacts/<window-id>/sha256/<prefix>/<sha>.chromaprint.raw
```

Artifacts and `result.json` are admitted without replacement and sealed read-only.
Two windows may legitimately have identical bytes; their window paths stay distinct
while their content digests agree. Windows under 30 seconds, windows under 10 seconds,
partial tails, and empty fingerprints get explicit quality flags. These are routing
strata, not confidence probabilities.

## Conservative exact comparison v1 (historical contract)

The historical comparison contract only answers whether two caller-described raw artifacts are
byte-for-byte equal. It requires the same implementation, algorithm, raw format,
sample rate, and channels; rejects self-pairs; and retains query/candidate roles.
Because v1's per-fingerprint `implementation_version` includes the extraction
`recipe_id`, its equality check also requires the same extraction recipe. That
historical behavior is preserved for reproducibility; v1 is not suitable for raw
fingerprints made from different input bytes. Cross-duration, short-window, and
empty-fingerprint conditions are flagged.

```sh
pipeline/bin/audio-fingerprint compare \
  --work-order /private/work-orders/fingerprint-pair-001.json
```

The schemas are
[`audio-fingerprint-compare-work-order.schema.json`](schemas/audio-fingerprint-compare-work-order.schema.json)
and
[`audio-fingerprint-compare-result.schema.json`](schemas/audio-fingerprint-compare-result.schema.json).
The score is `0.0` or `1.0` with semantics
`boolean_raw_byte_equality_not_probability`. Every pair remains
`decision_state: candidate`, `requires_human_review: true`, and
`relationship_asserted: false`. Equality is not proof of duplicate, excerpt, parent,
ownership, or chronology. Non-equality is not proof that audio is unrelated: this
helper performs no alignment, offset search, or approximate match.

Approximate clip-to-parent matching is deferred until a matcher can be pinned,
evaluated on a frozen HIMR-specific set, stratified by duration/edit/noise, and
calibrated. Raw similarity must never be relabeled as probability.

## Sealed-envelope cross-recording exact comparison v2

V2 fixes cross-input comparability in a new contract without weakening v1. Its work
order does not copy artifact hashes, engine fields, algorithm fields, windows, or
catalog IDs. Each side contains only an explicit `query`/`candidate` role, an exact
sealed extraction-result path and expected result SHA-256, and the selected
`fingerprint_id`:

```sh
pipeline/bin/audio-fingerprint compare-v2 \
  --work-order /private/work-orders/fingerprint-pair-v2-001.json
```

See
[`audio-fingerprint-compare-work-order-v2.schema.json`](schemas/audio-fingerprint-compare-work-order-v2.schema.json),
[`audio-fingerprint-compare-result-v2.schema.json`](schemas/audio-fingerprint-compare-result-v2.schema.json),
and the
[`v2 example`](examples/audio-fingerprint-compare-work-order-v2.example.json).
`validate-compare-v2` performs the same admission checks without writing output, and
`compare-v2 --dry-run` emits the deterministic comparison recipe without creating an
output directory.

For both sides, v2 independently rehashes and reconstructs the completed extraction
envelope, recipe, run, commands, input, current FFmpeg executable/build evidence,
every declared raw artifact, IDs, windows, metadata, flags, and recording/rendition
context. The selected fingerprints must have identical engine/build identity,
algorithm, raw format, sample rate, and channels. Their extraction input hashes and
recipe IDs may differ. Executable paths may differ when the exact engine/build binding
is otherwise identical. The comparison itself reads and compares only the selected
raw artifact bytes; it performs no alignment, decoding, offset search, or approximate
matching.

V2 derives catalog context from the producer envelopes rather than accepting a new
caller-supplied context. Null-context extraction results are rejected because this
lane requires catalog lineage. Cross-recording, cross-rendition, cross-input,
cross-recipe, duration, short-window, and empty-fingerprint conditions are explicit
quality flags. Every result and comparison is fixed to `visibility: private`,
`publication_authority: none`, `calibration_state: not_calibrated`, null calibrated
probability, candidate-only decision state, mandatory human review, and
`relationship_asserted: false`.

## Catalog admission

Producers never open SQLite. Validate or import sealed completed results with:

```sh
PYTHONPATH=corpus/src python3 -m himr_corpus validate-audio-fingerprint-result \
  --result /private/fingerprints/.../result.json
PYTHONPATH=corpus/src python3 -m himr_corpus import-audio-fingerprint-result \
  --db /private/corpus.sqlite3 --result /private/fingerprints/.../result.json

PYTHONPATH=corpus/src python3 -m himr_corpus validate-audio-fingerprint-compare-result \
  --result /private/fingerprint-comparisons/.../result.json
PYTHONPATH=corpus/src python3 -m himr_corpus import-audio-fingerprint-compare-result \
  --db /private/corpus.sqlite3 \
  --result /private/fingerprint-comparisons/.../result.json
```

With `catalog_context: null`, admission is provenance-only: completed runs, run
inputs, and private artifacts are retained, but no fingerprint observation or match
candidate is created. With context, the exact recording/rendition/media chain must
exist. Extraction creates private machine observations and an uncalibrated raw
word-count score. Exact comparison creates only a generic `match_candidates` row plus
its restrictive subtype; it never creates a recording relation. Imports are
transactional and idempotent, and no path reaches the public exporter.

For v2, import both exact extraction results first. Comparison admission revalidates
both sealed producer envelopes and all current evidence again inside the catalog
transaction, requires their append-only extraction receipts, fingerprints, private
artifacts, observations, and recording/rendition/media lineage, then writes the
separate `audio_fingerprint_match_candidates_v2` subtype. The existing compare-result
CLI dispatches v1 or v2 by `schema_version`.

The comparison import receipt is inserted before the v2 subtype, and one typed receipt
plus exactly two role-keyed side bindings preserve result paths/hashes/counts,
producer-run engine and configuration evidence, normalized inputs, selected raw
artifacts, and catalog context. Admission rejects duplicate JSON keys, derives the
boolean score from raw artifact SHA-256 plus byte count, and seals pair-scoped graph
attachments. Catalog `validate` also rehashes every persisted v2 local path; SQL alone
cannot prove that a URI exists or that its current bytes match a digest.

## Tests

```sh
python3 -m unittest pipeline.tests.test_audio_fingerprint -v
PYTHONPATH=corpus/src python3 -m unittest \
  corpus.tests.test_fingerprint_result_importer -v
```

Fixtures exercise real local FFmpeg extraction, deterministic recipes, unique runs,
full/explicit/fixed windows, partial tails, identical-content windows, exact pair
comparison, cross-recording equal raw bytes from different input/recipe envelopes,
historical v1 rejection, mismatched engines, result/artifact/side tampering, unknown
fields, unsafe paths/URIs, missing receipts and lineage, rollback, provenance-only
admission, idempotency, uncalibrated scores, and mandatory private
human-review/no-relationship/no-publication-authority state.
