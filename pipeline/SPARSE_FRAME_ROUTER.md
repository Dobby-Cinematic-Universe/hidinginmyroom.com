# Sparse frames and OCR-candidate routing

[`sparse_frame_router.py`](sparse_frame_router.py) is an offline, CPU-only bridge
from media preprocessing to later OCR and visual analysis. It consumes a completed,
sealed [`media_preprocess`](media_preprocess.py) result, reads its
`low_resolution_cfr_proxy` and scene-change routing, and extracts a small set of
lossless PNG frames. It does **not** run OCR, infer whether text is present, identify
a person, or publish anything.

This boundary is deliberate: frame selection and media decoding can be reproduced and
audited before a language-data or vision-model dependency is admitted.

## Requirements and trust boundary

- Python 3.10 or newer and a caller-provisioned `ffmpeg` executable.
- No network access or model download. The implementation has no downloader or HTTP
  client and invokes only the explicitly pinned local FFmpeg path.
- A completed version-1 preprocessing result and its CFR proxy must be regular,
  non-symlink, sealed read-only files. Their SHA-256 digests, sizes, processing-run
  relationship, artifact ID, file URI, normalized probe, and run-directory containment
  are checked before extraction and again after it.
- FFmpeg is pinned by executable SHA-256 and complete `ffmpeg -version` output
  SHA-256. Both identities are rechecked after the last frame.
- The input proxy must fit the work order's pixel, duration, and byte limits. The
  version-1 global ceilings are 1280×720, seven days, 256 frames, 64 MiB per PNG, and
  ten minutes per frame. Practical work orders should be much smaller.
- Every artifact and result has `visibility: private` semantics. This stage does not
  touch the corpus database and creates no publication decision.

Completed output can be checked and admitted through the separate catalog trust
boundary described in
[`../corpus/docs/SPARSE_FRAME_RESULT_INGESTION.md`](../corpus/docs/SPARSE_FRAME_RESULT_INGESTION.md).
That importer independently rehashes and reconstructs the result, writes only private
routing candidates, and does not convert them into OCR, face, identity, or publication
claims.

The normative work-order and result shapes are
[`schemas/sparse-frame-work-order.schema.json`](schemas/sparse-frame-work-order.schema.json)
and
[`schemas/sparse-frame-result.schema.json`](schemas/sparse-frame-result.schema.json).
Runtime validation is authoritative and rejects unknown keys.

## Create and validate a work order

The generator reads the exact FFmpeg identity recorded by preprocessing and captures
its current executable and version hashes:

```sh
pipeline/bin/sparse-frame-router create-work-order \
  --job-id sparse-pilot-001 \
  --preprocess-result /srv/himr-media/derived/media/sha256/ab/abc.../recipes/def.../executions/run_preprocess_abc.../result.json \
  --output-root /srv/himr-media/derived \
  > /srv/himr-media/work-orders/sparse-pilot-001.json

pipeline/bin/sparse-frame-router validate \
  --work-order /srv/himr-media/work-orders/sparse-pilot-001.json
```

The generated defaults reserve one recording-start frame, at most 64 scene frames,
and at most 64 one-minute periodic frames, for a hard total of 129. Edit those
explicit caps down for short material. The caps for enabled classes plus the required
start frame must fit `limits.max_frames`; the runtime rejects an under-provisioned
order instead of silently changing class allocation.

An illustrative hand-written order is
[`examples/sparse-frame-work-order.example.json`](examples/sparse-frame-work-order.example.json).

## Selection algorithm

Selection is deterministic and independent of image content:

1. Retain proxy-rendition media time zero (`FRAME_RECORDING_START`). This reason-code
   name does not assert a recording-timeline transform.
2. Read scene timestamps already produced by preprocessing, apply the explicit
   non-negative scene offset, and discard only timestamps beyond the proxy duration.
3. If scene candidates exceed their class cap, retain uniformly spaced entries from
   the ordered list, including its first and last entries. The result records
   `SCENE_CANDIDATES_UNIFORMLY_CAPPED`.
4. Generate periodic timestamps at the configured interval. Uniformly cap that list in
   the same way, recording `PERIODIC_CANDIDATES_UNIFORMLY_CAPPED` when applicable.
5. Preserve start and selected scene anchors first. Merge periodic candidates within
   `min_separation_ms` into the nearest anchor, retaining both reason codes. Nearby
   scene candidates are likewise merged deterministically. The result records
   `NEARBY_CANDIDATES_MERGED` and the source scene timestamps.

Periodic samples provide coverage for static videos and long intervals without a
scene transition. There is no dense frame scan, object detector, face detector, OCR
pass, or action detector. A one-hour static recording with a one-minute interval
requests only 60 frames (including the start frame), subject to the explicit cap.

## Dry run and extraction

```sh
pipeline/bin/sparse-frame-router run \
  --work-order /srv/himr-media/work-orders/sparse-pilot-001.json \
  --dry-run

pipeline/bin/sparse-frame-router run \
  --work-order /srv/himr-media/work-orders/sparse-pilot-001.json
```

A dry run hashes and validates the preprocessing handoff and FFmpeg, computes the
complete selection, and prints each planned command. It writes nothing.

Each real frame uses one bounded, single-threaded FFmpeg invocation with input-side
accurate seek, `-copyts`, bit-exact flags, metadata/chapter removal, `rgb24`, and the
lossless PNG encoder at a fixed compression/prediction setting. The exact FFmpeg
binary and proxy bytes are part of the recipe and result identity. This makes the PNG
byte hashes stable for a fixed build and input while avoiding a claim of portability
across different FFmpeg builds.

FFmpeg's `showinfo` filter supplies the decoded frame's integer PTS, integer duration,
and exact rational time base. The envelope retains those authoritative values plus
rounded microsecond and millisecond conveniences and drift from the requested
timestamp. A proxy can have a small nonzero container/video start (for example due to
codec timing); the observed PTS is therefore not overwritten with the requested
timestamp. `max_timestamp_drift_ms` fails closed on an unexpected seek result.

## OCR routing is not OCR

Every selected frame receives a routing object such as:

```json
{
  "route": "queue_candidate",
  "evaluation_state": "not_evaluated",
  "text_presence": "unknown",
  "reason_codes": ["OCR_CANDIDATE_PERIODIC_COVERAGE"],
  "warning": "This is an extraction/routing candidate only. OCR and text-presence evaluation have not run, and no person or content is identified."
}
```

The schema has no OCR-text, face, identity, or content-claim field. A later OCR stage
must use a separate result contract, model/language-data registry entry, confidence
calibration, and review state.

The pinned FFmpeg 8.1.2 build advertises an optional `ocr` video filter with
`datapath`, `language`, `whitelist`, and `blacklist` options. This stage does not call
that filter. A later 2026-08-28 follow-up added a separately pinned standalone
Tesseract TSV producer and migration-0033 private admission boundary; neither changes
this router's `not_evaluated` semantics or grants public-use authority.

The separate [`ocr_preflight.py`](ocr_preflight.py) makes those prerequisites
executable. The repository-default system-tessdata manifest still exits nonzero when
its pinned Japanese/Korean models are absent, while a reviewed owner-private bundle
has cleared the deterministic TSV gate. ADR 0006 preserves the historical deferral
and records the later private-only implementation follow-up.

## Immutable layout and replay

```text
<output-root>/
└── vision/sparse-frames/sha256/ab/<proxy-sha256>/
    └── results/<result-key>/
        ├── result.json
        └── frames/
            ├── frame-0000-000000000000.png
            └── frame-0001-000000060000.png
```

The result key covers the normalized work order, sealed preprocessing-result hash,
proxy hash and artifact lineage, deterministic recipe, and complete selected-frame
plan. Files are staged in the destination filesystem, verified, sealed read-only, and
admitted by an atomic directory rename while a per-result OS lock is held.

An identical replay returns the existing envelope byte-for-byte only after rehashing
all inputs and validating every PNG, path, URI, size, image header, run-scoped ID,
frame relationship, exact timestamp record, and OCR route. A writable, symlinked,
partial, moved, or tampered result fails closed; it is never silently regenerated.

## Tests

```sh
python3 -m unittest pipeline.tests.test_sparse_frame_router -v
pipeline/tests/run.sh
python3 scripts/validate-json-contracts.py
```

The dedicated suite covers static-video fallback, scene/periodic merging, deterministic
uniform caps, exact PTS arithmetic, schema validation, no-write dry runs, bit-stable
PNGs across distinct result keys, byte-identical idempotent replay, sealed outputs,
writer-lock contention, unknown-key/URL/cap attacks, and tampered preprocess envelopes,
proxies, completed frame artifacts, and attempted OCR-claim injection.
