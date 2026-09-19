# Offline Tesseract TSV adapter

`ocr_tesseract_adapter.py` is the explicitly selected private execution boundary
between a completed `sparse_frame_router` result and private OCR review/admission.
It does not make the
preflight pass and does not replace the separate host/runtime review. Do not run a
real batch until that gate has passed and its executable, model-data, license, and
runtime-closure evidence has been reviewed.

The adapter has no network or database client, no publication operation, and no
person-identity behavior. Its result grants no publication authority. OCR text and
geometry remain owner-private and require human review because a frame may contain
addresses, credentials, account identifiers, private messages, or unrelated people’s
information.

## Contracts

- `schemas/ocr-tesseract-work-order.schema.json` pins one completed sparse-frame
  `result.json` by absolute path and SHA-256; the Tesseract executable, byte count,
  version label, and version-output SHA-256; every local `.traineddata` model by
  language/path/byte count/SHA-256; the exact language expression and engine
  parameters; an explicit ordered list of upstream frame IDs and its routing basis;
  bounded resource limits; and a specific private output root.
- `schemas/ocr-tesseract-result.schema.json` describes planned, completed, and failed
  envelopes. Completed results include the raw TSV artifacts, word rectangles in
  source-frame pixels, reading order, exact upstream frame locators, explicit
  language/script evidence, complete per-region engine/model/parameter provenance,
  and raw Tesseract scores labeled `not_calibrated` and `not_a_probability`.

Unknown fields, duplicate JSON keys, URL paths, symlinks, dot traversal, writable
upstream seals, noncontiguous frame ordinals, identity/lineage drift, extra frame
files, hash or stat drift, malformed TSV, non-UTF-8 output, missing text/score fields,
zero-area or out-of-frame word boxes, non-finite/out-of-range scores, resource-cap
violations, and non-private result reuse all fail closed.

Sparse-frame router output is only a broad candidate pool. Because most video frames
do not contain useful on-screen text, this adapter never interprets that pool as a
blanket execution request. A separate cheap text-presence stage or reviewer must
first choose frame IDs. The work order then requires `mode: explicit_frame_ids` and a
`basis` of `external_text_presence_candidate` or `reviewer_selected`; empty,
unknown, duplicate, or out-of-upstream-order selections fail closed. This adapter
does not implement the preceding detector.

The adapter rehashes the sparse-frame envelope and every selected PNG before work,
after each PNG is consumed, and again before sealing. It similarly rechecks the
executable, exact version output, and all pinned models. The following upstream data
is copied without dropping or recomputing its coordinate meaning:

- source media, proxy artifact, preprocess run, and sparse-frame run identifiers;
- sparse-frame result key, frame and frame-artifact identifiers, ordinal, and PNG
  SHA-256;
- requested timestamp, exact PTS/duration/time base, upstream rounded timestamp, and
  drift; and
- sparse selection reasons, source scene timestamps, and OCR-routing reasons.

All timestamp fields above retain the sparse router's decoded proxy-media coordinate
meaning. In particular, the producer's legacy field name
`rounded_source_timestamp_ms` is the rounded PTS on the low-resolution CFR proxy; it
is not an original-provider or recording-timeline timestamp. Likewise,
`source-frame pixels` means pixels in that selected proxy PNG. Migration 0033 binds
the original source rendition/media as provenance but deliberately leaves source and
recording time mappings unasserted.

Every word repeats this complete frame locator. A later deduplication or temporal
tracking stage therefore cannot replace the underlying frame observations.

## Why TSV is activated explicitly

The command uses:

```text
-c tessedit_create_tsv=1
```

It deliberately does not append the named `tsv` config. An isolated tessdata bundle
may omit `configs/tsv`; Tesseract can then return status zero while silently writing
plain text. The adapter requires the exact 12-column Tesseract TSV header before it
accepts any output:

```text
level page_num block_num par_num line_num word_num left top width height conf text
```

(The contract uses literal tab separators.) Only level-5 word rows become regions.
No level-5 row may have missing text, a zero-area rectangle, an invalid reading-order
coordinate, or a confidence lexeme outside raw 0–100. A valid header with no word
rows is retained as `not_detected`, not treated as an engine failure.

## CLI

The wrapper is `pipeline/bin/ocr-tesseract-adapter`.

Create a fully pinned work order on an already reviewed offline host:

```bash
pipeline/bin/ocr-tesseract-adapter create-work-order \
  --job-id PILOT_ID \
  --sparse-frame-result /ABSOLUTE/SEALED/result.json \
  --selection-basis external_text_presence_candidate \
  --frame-id frame_0123456789abcdef0123456789abcdef \
  --frame-id frame_fedcba9876543210fedcba9876543210 \
  --tesseract /ABSOLUTE/PINNED/tesseract \
  --tessdata-dir /ABSOLUTE/PINNED/tessdata \
  --model eng=/ABSOLUTE/PINNED/tessdata/eng.traineddata \
  --model jpn=/ABSOLUTE/PINNED/tessdata/jpn.traineddata \
  --model kor=/ABSOLUTE/PINNED/tessdata/kor.traineddata \
  --language eng --language jpn --language kor \
  --output-root /ABSOLUTE/OWNER_PRIVATE/derived
```

The command prints JSON to standard output; save and review it through the normal
workspace process. Validation rehashes every current input and captures the pinned
version output but creates no result:

```bash
pipeline/bin/ocr-tesseract-adapter validate --work-order /ABSOLUTE/work-order.json
```

Dry-run planning also creates no output directory:

```bash
pipeline/bin/ocr-tesseract-adapter run \
  --work-order /ABSOLUTE/work-order.json --dry-run
```

Only after the external preflight and work-order review should execution be allowed:

```bash
pipeline/bin/ocr-tesseract-adapter run --work-order /ABSOLUTE/work-order.json
```

The final layout is content-addressed beneath
`vision/ocr/tesseract/sha256/.../results/RESULT_KEY/`. TSV and JSON files are mode
`0400`; the immutable result and TSV directories are mode `0500`; mutable ancestors
and lock files are owner-only. A complete exact replay returns the existing JSON
byte-for-byte after rehashing its source, runtime pins, TSV artifacts, and parsed word
regions. An incomplete or altered result directory is never repaired or overwritten.

## Current boundary

This implementation has completed synthetic/adversarial contract testing and a small
private pilot. It does not claim OCR accuracy, does not calibrate scores, and does not
detect script or per-word language. A separate migration-0033 importer may admit an
exact completed result only to the private, redaction-pending, no-authority catalog
lane documented in
[`../corpus/docs/OCR_TESSERACT_RESULT_INGESTION.md`](../corpus/docs/OCR_TESSERACT_RESULT_INGESTION.md);
the producer itself still authorizes neither catalog writes nor public release.
`script: unknown` and
`WORK_ORDER_EXPLICIT_NOT_WORD_DETECTED` are intentional evidence labels, not missing
metadata to infer later without review.

PSM 12 is intentionally outside the admitted parameter set because it performs
orientation/script detection and can load `osd.traineddata`, which this version does
not model as an explicit selected-language asset. A later contract may admit it only
with that extra model and its interpretation boundaries pinned.
