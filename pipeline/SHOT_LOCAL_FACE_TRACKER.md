# Deterministic shot-local face tracker

`shot_local_face_tracker.py` is the anonymous geometry-only tracking foundation from
[ADR 0007](../docs/adr/0007-speaker-and-active-speaker-models.md). It consumes a
sealed, hash-pinned JSON artifact of frame-local face detections and creates tracks
using constant-velocity box prediction plus Hungarian IoU assignment. It never reads
media, runs a detector, computes an embedding, names a person, scores an active
speaker, opens the corpus database, uses the network, or publishes anything.

This adapter is not face recognition. A track means only that a deterministic box
association recipe linked detections inside one declared shot.

## Strict inputs

The three Draft 2020-12 contracts are:

- `schemas/shot-local-face-detections.schema.json` for the sealed detector artifact;
- `schemas/shot-local-face-tracker-work-order.schema.json` for execution; and
- `schemas/shot-local-face-tracker-result.schema.json` for the sealed result.

The detection artifact must declare ordered, non-overlapping half-open shot bounds
and must contain every frame index inside each shot. Each frame carries a strictly
increasing timestamp, dimensions, and zero or more ordinal-stable detections. Width
and height must remain constant inside a shot, but may reset at the next cut. Each
detection preserves its pixel `x/y/width/height` box, five landmarks, raw detector
score, and the derived below-64-pixel flag. Unknown keys fail closed, so embeddings,
names, or ungoverned detector metadata cannot be smuggled through this contract.

The work order pins the exact detection bytes and executing Python bytes/version.
The adapter hashes and parses the same captured detection byte buffer, then rechecks
the immutable path before sealing a result; an atomic path replacement can therefore
neither change the verified parse nor survive the final integrity check. The
detection file must be immutable and owner-private, and the output root must be an
existing owner-private directory without symlink traversal. The ordinary system
Python executable is recorded truthfully as `host_runtime`, not private data. Recipe
parameters are explicit because ADR 0007 deliberately reserves IoU, gap, and
confirmation values for development-data selection. The repository example values
are synthetic fixture values, not promoted thresholds.

The fixed execution envelope accepts at most 64 MiB of JSON, 100,000 frames, 50,000
shots, 64 detections per frame, and 128 simultaneously active tracks. The adapter is
single-threaded and refuses a sealed result larger than 256 MiB. These are safety
bounds rather than target operating sizes; long recordings should be routed in
governed shot-aligned chunks.

## Frozen association behavior

For each shot independently, frames are processed in ascending frame index and
detections in declared ordinal order:

1. A track with one match predicts its last box. With at least two matches, each
   `x/y/width/height` component advances at the velocity between the last two matched
   boxes, divided by their frame-index difference. Extrapolated width and height are
   clamped to a positive epsilon; non-finite predictions fail closed.
2. All prediction/detection IoUs are computed. Eligibility is tested against raw IoU
   before any rounding, and pairs below the inclusive configured minimum are
   forbidden.
3. A square, dummy-augmented Hungarian assignment first maximizes the number of
   eligible matches and then the sum of IoUs quantized to eight decimals. Only the
   eligible score and emitted value are quantized. The implementation scans
   equal-cost rows and columns in ascending stable order, so exact ties replay
   deterministically.
4. Every accepted assignment records the predicted box and assignment IoU. Every
   unmatched active track records a gap, predicted box, and consecutive gap count.
5. A track remains available after at most `max_gap_frames` consecutive misses. The
   next miss closes it as `max_gap_exceeded`; a later detection starts a new track.
6. Every still-active track closes as `shot_end` on the shot's final frame. The
   active-track set is discarded before the next shot, regardless of box similarity.

Track confirmation means only that the configured number of detections was reached.
It is track lifecycle state, not identity confidence. The result retains confirmed
and unconfirmed tracks, their first/last detection, every observation and gap, maximum
gap, closure reason, and frame-level starts/assignments/closures.

The result and every track always carry identity state `unknown`, null identity label
and probability, active-speaker state `unknown`, null active-speaker score, and
publication `false`. Top-level identity, active-speaker, cross-shot-join, and
publication decisions are all `false`; human review remains required.

## Usage

Start from the tracked examples, replace every path/hash/size/runtime value, seal the
detection artifact, and use an owner-private output root:

```sh
pipeline/bin/shot-local-face-tracker \
  --work-order /absolute/private/path/work-order.json \
  --dry-run

pipeline/bin/shot-local-face-tracker \
  --work-order /absolute/private/path/work-order.json
```

The result is staged as an owner-private hidden sibling below
`<output-root>/vision/shot-local-face-tracks/`, with its result file sealed before
the directory itself is sealed. The already-sealed sibling is then atomically renamed
to
`<output-root>/vision/shot-local-face-tracks/<result-key>/result.json`, with result
files mode `0400`, the sealed result directory `0500`, and writable private ancestors
`0700`. There is no fallible chmod or other work after the final rename. A pre-rename
failure cleans the hidden staging sibling; an existing result key is an error, so the
adapter never overwrites or silently reuses an earlier result.
Materialized CLI stdout is a small receipt with counts and the private result path; it
does not duplicate boxes, landmarks, assignments, or gaps into terminal logs.

Validate contracts and a generated result with:

```sh
python3 scripts/validate-json-contracts.py \
  --validate pipeline/schemas/shot-local-face-detections.schema.json /private/detections.json \
  --validate pipeline/schemas/shot-local-face-tracker-work-order.schema.json /private/work-order.json \
  --validate pipeline/schemas/shot-local-face-tracker-result.schema.json /private/result.json

python3 -m unittest pipeline.tests.test_shot_local_face_tracker -v
```

The 15-case adversarial suite covers shot cuts, tolerated and overlong gaps, exact
assignment ties, crossing/overlap ambiguity, incomplete or overlapping bounds,
constant per-shot dimensions, hash mismatch, writable and symlinked detector inputs,
atomic path replacement after capture, raw-IoU rounding boundaries, positive
extrapolated extents and result-schema bounds, pre-rename seal failure, absence of
post-rename chmod, authority defaults, and symlinked output components or
existing-output refusal.

## Synthetic private pilot

The ignored 2026-08-28 pilot contains five synthetic frames across two shots. The
original implementation `0.1.0` result remains sealed but is explicitly quarantined
and superseded because it predates the six audit repairs above. A separate
implementation `0.2.0` result produced two shot-scoped tracks, two existing-track
assignments, and one explicit gap. Repeated dry-run receipts were byte-identical. The
fixed materialized result validates against the strict result schema, and an exact
second run fails closed without changing its hash. Both artifacts remain in the
owner-private audit packet; only the `0.2.0` result is current correctness evidence.
The packet contains no real face or media and grants no detector, tracking, identity,
active-speaker, calibration, or publication performance claim.

The separate synthetic-only
[`yunet_face_detector.py`](yunet_face_detector.py) bridge now proves that the pinned
YuNet runtime can emit this contract and hand it to the tracker, but its procedural
fixture produced no detections and is not a quality result. The next governed step is
to admit an exact dense real frame sequence with registered shot boundaries, then
evaluate face recall, track coverage, and identity switches on the recording-disjoint
benchmark required by ADR 0007. No recipe value should be promoted before that
development/test split is frozen.
