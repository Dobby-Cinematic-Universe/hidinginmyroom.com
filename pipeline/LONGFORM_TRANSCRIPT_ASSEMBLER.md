# Long-form transcript assembler

This isolated successor component turns one validated long-form ASR plan and its
per-span native transcript results into one private recording-level machine
transcript. It has no controller, catalog, publication, wiki, network, inference,
media-write, or deletion authority. It does not alter the active archive campaign.

The parent plan is the authority for exact 16 kHz, zero-based, half-open sample
coordinates. Each completed span transcript uses analysis-span-local sample
coordinates. The assembler adds the plan's analysis start, then assigns each timed
word or segment fallback to the unique core containing its midpoint. If any word in
a native segment lacks paired timing, that segment is kept as one fallback with an
empty assembled `words` array; native evidence remains referenced and the assembler
does not fabricate word coordinates. Integer milliseconds are emitted only at
document, coverage-interval, boundary, and segment level for playback convenience.
Words retain only text, exact parent samples, an optional raw model score, and present
anomaly flags.

## Inputs

`--parent-manifest` must be a deterministically replayable
`himr_longform_asr_plan` created by the corpus planner. `--span-results` follows
[`longform-span-transcript-bindings.schema.json`](schemas/longform-span-transcript-bindings.schema.json).
It contains exactly one row per planned span:

- `completed` binds both the executor's `result.json` and `transcript.json` by
  path and SHA-256;
- `failed` carries a machine-safe failure code and no artifacts; and
- `pending` carries neither artifacts nor a failure code.

Before granting decoded coverage, the assembler verifies the completed result's
semantic identity, parent recording binding, parent plan binding, exact analysis
and core intervals, work-order agreement, one-call/no-persistent-chunk execution
claim, complete-coverage declaration, and normalized transcript artifact path,
size, SHA-256, and semantic identity. It rehashes all completed artifacts after
reconciliation. A completed empty transcript therefore means decoded with no
machine-detected speech; a missing, pending, failed, mismatched, or truncated span
does not.

## Deterministic overlap handling

Core ownership is decided before any model score. Adjacent overlap units are paired
deterministically by normalized text and parent timing. A single core-owned copy
wins. If timestamp drift makes multiple copies core-owned, the copy furthest inside
its core wins, followed by stable span/segment/source-array order. Raw scores are
preserved but never used as calibrated confidence or as the ownership tiebreak.

Every adjacent boundary records comparison hashes and one of `consistent`,
`conflict`, or `unassessed`, plus explicit warning flags. These are quality signals,
not publication gates. Source artifacts remain the evidence for every excluded
context or duplicate unit.

## Run

Validation performs all loading and assembly in memory without creating a file:

```sh
pipeline/bin/longform-transcript-assembler \
  --parent-manifest /absolute/path/plan.json \
  --span-results /absolute/path/bindings.json \
  --validate-only
```

Create one new immutable output after validation:

```sh
pipeline/bin/longform-transcript-assembler \
  --parent-manifest /absolute/path/plan.json \
  --span-results /absolute/path/bindings.json \
  --output /absolute/hot-tier/path/recording-transcript.json
```

Existing output paths are never replaced. The recording-level artifact follows
[`longform-recording-transcript.schema.json`](schemas/longform-recording-transcript.schema.json)
and carries the standard disclaimer:

> Machine-generated and unreviewed; may be wrong; not a verified quotation.

It remains private and unreviewed. Catalog ingestion, release policy, explorer
projection, and any later correction/retraction workflow are separate downstream
operations.
