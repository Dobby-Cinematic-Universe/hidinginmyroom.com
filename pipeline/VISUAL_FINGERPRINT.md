# Sparse visual fingerprints and clip candidates

[`visual_fingerprint.py`](visual_fingerprint.py) is an offline, CPU-only foundation
for finding frames that may warrant clip-to-video review. It consumes an exact,
sealed local video or analysis proxy, decodes only caller-selected timestamps, stores
private 32×32 grayscale evidence, and computes a deterministic 64-bit perceptual hash.
It does not identify a person, decide that two recordings are duplicates, choose a
parent, establish ownership, infer chronology, or create a recording relationship.

This stage complements, rather than replaces:

- [`sparse_frame_router.py`](sparse_frame_router.py), which selects bounded scene and
  periodic PNGs for OCR/visual routing from a sealed preprocessing envelope; and
- [`audio_fingerprint.py`](audio_fingerprint.py), whose exact raw Chromaprint evidence
  can route audio-preserving excerpts independently of their pictures.

## Exact input, tool, and implementation pins

The input must be a resolved, non-symlink, read-only regular file. The work order pins
its SHA-256, byte count, media/artifact/preprocessing-run lineage, duration, timeline
origin, and video selector. The source is hashed before and after extraction and its
device, inode, size, and nanosecond modification time must remain unchanged. URLs and
output roots under `/tmp` or `/var/tmp` are rejected.

Capture the local decoder pins with:

```sh
pipeline/bin/visual-fingerprint inspect-engine
```

The pin covers the FFmpeg executable bytes, full version output, and the complete
reported `scale`, `showinfo`, and rawvideo capabilities. Extraction rechecks all of
them after the last frame. Results also retain the SHA-256 and byte count of the
Python implementation itself. There is no network code, model, model download,
NumPy, or SciPy dependency.

The normative contracts and illustrative orders are:

- [`visual-fingerprint-work-order.schema.json`](schemas/visual-fingerprint-work-order.schema.json)
  and [`visual-fingerprint-work-order.example.json`](examples/visual-fingerprint-work-order.example.json);
- [`visual-fingerprint-result.schema.json`](schemas/visual-fingerprint-result.schema.json);
- [`visual-fingerprint-compare-work-order.schema.json`](schemas/visual-fingerprint-compare-work-order.schema.json)
  and [`visual-fingerprint-compare-work-order.example.json`](examples/visual-fingerprint-compare-work-order.example.json);
- [`visual-fingerprint-compare-result.schema.json`](schemas/visual-fingerprint-compare-result.schema.json).

Runtime validation is authoritative and rejects unknown keys.

## Half-open windows and decoded time evidence

Every sample binds one requested millisecond timestamp to an explicit window:

```text
start_ms <= requested_timestamp_ms < end_ms
```

`timestamp_kind` records whether the caller supplied an explicit coverage timestamp
or an upstream keyframe timestamp. It is provenance, not a promise that the decoded
frame is a keyframe. FFmpeg `showinfo` supplies the actual integer PTS, duration,
rational time base, and keyframe bit. The result retains those values plus rounded
absolute/relative microseconds and milliseconds. The exact decoded time must remain
inside the declared half-open window and within `max_timestamp_drift_ms`; otherwise
the job fails closed. A requested keyframe that decodes to a non-keyframe receives a
quality flag.

Work orders are deliberately capped at 4,096 samples. Pair work orders select at most
256 frames per side and at most 65,536 comparisons. Those ceilings prevent an
apparently sparse job from becoming a dense scan.

## Fingerprint representation

FFmpeg emits exactly 1,024 bytes per selected frame after a pinned
`scale=32:32:flags=bilinear,format=gray` transform. Those bytes are retained as a
private, sealed artifact and their SHA-256 is the exact decoded-frame digest.

The perceptual representation is `fixed_q20_dct_phash_8x8_v1`:

1. A committed 8×32 cosine matrix represents the low DCT basis as signed Q20
   integers. Runtime trigonometry and floating-point transforms are not used.
2. A separable integer transform produces the top-left 8×8 coefficients.
3. The DC bit is fixed to zero. Each of the remaining 63 row-major bits is one only
   when its coefficient is strictly greater than the median AC coefficient.
4. Bits are serialized most-significant first as 16 lowercase hexadecimal digits.

The exact FFmpeg build, input, implementation hash, matrix/threshold definition,
sampling plan, and time constraints are all part of the recipe. Exact replays return
the existing byte-identical sealed result only after every grayscale artifact is
rehashed and its perceptual hash is recomputed.

This representation is intentionally small and auditable. It is not invariant to
all edits. Re-encoding or modest color changes may preserve a near hash, while crops,
overlays, borders, rotations, speed changes, frame interpolation, scene-boundary
shifts, and selecting a neighboring frame may produce a large distance. Visually
simple or repeated frames can also collide.

## Conservative comparison

The comparison command reads two pinned, sealed extraction results and a bounded list
of fingerprint IDs:

```sh
pipeline/bin/visual-fingerprint compare \
  --work-order /private/work-orders/visual-pair-001.json
```

It computes the raw 64-bit Hamming distance for every selected cross-pair, retains a
bounded top list, and reports the minimum distance, normalized distance, raw
similarity, and exact-grayscale equality. The configured maximum Hamming distance is
only a routing threshold. Results say either `candidate_for_human_review` or
`below_configured_threshold`; the latter is **not** an assertion that the material is
unrelated.

Every result is explicitly `not_calibrated`, has `calibrated_probability: null`, and
requires human review. Its identity, duplicate, parent, ownership, relationship, and
unrelated assertions are all fixed to false. A permissive threshold can produce many
false candidates; a strict threshold can miss edited/reposted clips. Scores must not
be called probabilities until a frozen, recording-disjoint HIMR evaluation set has
measured them across edit, duration, codec, resolution, crop/overlay, and low-variance
strata.

Audio and visual candidates may be combined later as review-priority evidence, but
agreement between two uncalibrated modalities is still not proof of a relationship.
No current producer opens SQLite or creates a public artifact.

Completed extraction envelopes cross a second, independent trust boundary through
`himr-corpus validate-visual-fingerprint-result` and
`himr-corpus import-visual-fingerprint-result`. The catalog importer rehashes the
sealed result, input, implementation, FFmpeg executable, and every gray artifact;
recomputes the fixed-Q20 pHash and all IDs/timing; and admits only private,
uncalibrated, mandatory-review observations when an exact pre-existing
recording/rendition/media context is supplied. A null context is provenance-only.
The importer creates no match, identity, duplicate, relationship, or publication
decision.

Completed comparison envelopes cross the analogous boundary through
`himr-corpus validate-visual-fingerprint-compare-result` and
`himr-corpus import-visual-fingerprint-compare-result`. Both extraction envelopes
must already have been admitted with the exact non-null contexts named by the
comparison. The importer recomputes every cross-pair and preserves the bounded ranked
evidence privately. A threshold pass creates an uncalibrated generic routing
candidate and direct-media review task; a below-threshold result creates neither and
is never translated into `rejected` or `unrelated`. Neither outcome creates a
recording relation, identity assertion, publication decision, or gate clearance.

## Immutable layout

Extraction results use input and recipe digests:

```text
<output-root>/vision/visual-fingerprints/sha256/<input-sha>/
  recipes/<recipe-sha>/results/<result-key>/
    result.json
    frames/frame-0000-<sample-id>.gray
```

Comparisons use the ordered result pair and their own recipe/result key under
`vision/visual-fingerprint-comparisons/`. Files and completed directories are staged
on the destination filesystem, admitted with an atomic rename under an OS lock, and
sealed read-only. Writable, symlinked, moved, malformed, or tampered evidence fails
closed rather than being silently regenerated.

## Tests

```sh
python3 -m unittest pipeline.tests.test_visual_fingerprint -v
python3 scripts/validate-json-contracts.py
```

The synthetic FFmpeg suite covers exact replay, sealed artifacts, no-write dry runs,
simple transcode-tolerant candidate behavior, a below-threshold observation that does
not become an “unrelated” claim, result/artifact tampering, symlink rejection, sample
and pair caps, half-open time failures, and unknown-field rejection. Synthetic
threshold outcomes validate implementation behavior only; they are not accuracy or
truth labels.
