# Private YuNet/SFace face-candidate adapter

[`face_candidate_adapter.py`](face_candidate_adapter.py) is a bounded, offline pilot
for cross-video **review candidates**. It consumes an explicit list of sealed PNG
frames, detects faces with a hash-pinned YuNet artifact, aligns each detection with
OpenCV SFace, and stores the aligned crop and 128-dimensional embedding privately.
It compares only explicitly named frame pairs for which each frame has exactly one
detection.

This stage does not identify anyone. A label attached to a confirmed source remains
source context only: the result fixes `label_forwarded_by_model`, `identity_decision`,
and `human_identity_attestation` to `false`. Raw YuNet scores and SFace cosine
similarities are not calibrated probabilities. The result schema therefore requires
`calibrated_probability: null`, `threshold_decision: null`, and
`identity_label: null`.

## Trust and privacy boundary

- Inputs are absolute, sealed, non-symlink PNGs with exact SHA-256 and byte-count
  pins. Requested and FFmpeg-observed timestamps remain separate.
- The isolated OpenCV and NumPy wheels, their loaded native binaries, Python
  executable, OpenCV build-information output, YuNet/SFace models, and both license
  snapshots are hash-pinned. The adapter has no downloader or network client.
- The official initial YuNet profile is fixed at score/NMS/top-k `0.9 / 0.3 / 5000`.
  It runs with one OpenCV thread and OpenCL disabled. A miss abstains; the pilot does
  not lower the detector threshold to obtain a desired comparison.
- More or fewer than one detection on either side of a requested pair yields
  `abstained_detection_cardinality`. There is no largest-face guess and no forced
  match.
- Crops and little-endian float32 embeddings remain under the ignored owner-private
  research tree. Its pilot root and working ancestors are mode `0700`; sealed result,
  crop, and embedding directories are mode `0500`; and frames, crops, embeddings,
  manifests, and sealed result JSON are mode `0400`. They are absent from Git, the
  public corpus, site search, logs, analytics, and the live SQLite catalogue.
- A result grants no publication, recording-relationship, or identity authority and
  requires human review. No hosted biometric API or open-web face search is used.

The normative contracts are:

- [`schemas/face-candidate-work-order.schema.json`](schemas/face-candidate-work-order.schema.json)
- [`schemas/face-candidate-result.schema.json`](schemas/face-candidate-result.schema.json)
- [`examples/face-candidate-work-order.example.json`](examples/face-candidate-work-order.example.json)

Runtime validation is stricter than JSON Schema: it resolves paths without symlinks,
rehashes every byte before execution, loads OpenCV/NumPy only from the explicit
isolated module root, constructs both pinned networks, and rechecks every runtime,
model, license, and input-frame artifact after inference.

## Commands

Use an absolute work-order path:

```sh
pipeline/bin/face-candidate-adapter validate \
  --work-order /srv/himr-private/work-orders/face-candidate-001.json

pipeline/bin/face-candidate-adapter run \
  --work-order /srv/himr-private/work-orders/face-candidate-001.json \
  --dry-run

pipeline/bin/face-candidate-adapter run \
  --work-order /srv/himr-private/work-orders/face-candidate-001.json
```

Dry-run validation writes nothing. A completed destination is not silently reused:
this pilot fails closed and leaves validation of a retained result to the strict JSON
contract plus independent artifact hashing. Production promotion should add a
separate sealed-result replay validator rather than weakening this behavior.

## 2026-08-28 public-media positive-control pilot

The first ignored private run used four FFmpeg-observed frame pairs between confirmed
main-channel source `gPhrE99xwqI` and public Reddit clip `9g4t2fhoqylh1`. A prior
audio/visual packet had already proposed the four jump-cut coordinates, so this is a
transcode/crop robustness positive control, not independent proof of identity or a
recording relationship.

YuNet returned exactly one detection in all eight frames at the untouched `0.9`
threshold. The four raw SFace values and exact face-bearing timestamps remain only
in the ignored owner-private packet; contributor-facing documentation deliberately
does not reproduce biometric routing artifacts. Every row remains
`candidate_only_single_detection_each` with a null probability, null threshold
decision, null identity label, and false identity/relationship decisions. One
measured run took 1.33 seconds wall time, used one effective CPU thread, peaked at
172,936 KiB RSS, and reported no swaps.

The exact private result key is
`8852eaf29ae5ff34308a74eb0268285d66301261fc70555e8394eb87a03b8ae9`;
its `result.json` SHA-256 is
`634e93c5a3fd860942ae4ade3dfd6ecffa6ab03cfd4e30a5f566ea4b903ec5eb`.

## 2026-08-28 recording-disjoint feasibility pilot

A separate ignored run paired three frames from `gPhrE99xwqI` with three frames
from distinct public main-channel video `s2OW-jRyFrw`. The second source's sealed
acquisition metadata names channel `Hiding in my room` and uploader
`@hidinginmyroom1111`. Public YouTube repost `fku-kaaUStw` was not used because its
metadata names a different uploader. All six selected frames contained exactly one
YuNet detection at the unchanged `0.9` threshold.

The work order explicitly declares two within-recording pairs for each source and
three recording-disjoint pairs. Their seven raw routing values remain only in the
ignored owner-private packet. This is not a threshold experiment: the sample
supplies no negative distribution, identity references, or calibration authority.
The `s2OW-jRyFrw` frames remain deliberately unlabeled; the `Daniel` string is
confined to `gPhrE99xwqI` source context and is never forwarded. Every probability,
threshold decision, identity label, identity/relationship decision, and publication
decision remains null or false.

Result key
`564bd1ead59198f7451df3ca490b6d80de41a8d00b565f8d091ce258ac9e0730`
has `result.json` SHA-256
`25f44d681f3b3d8dbde6fcddc2f1aa88732783c7db9fbfb44bb659f00095859f`.
The one-thread run took 1.21 seconds and peaked at 173,124 KiB RSS without swaps.

## What this does not establish

This pilot does not meet ADR 0007's face-tracking or identity-candidate quality gate.
The two-source feasibility run is recording-disjoint only at its declared comparison
boundary; it is not a recording-disjoint development/calibration/test split. The
pilots have no shot-local tracker, track aggregation, negative pairs,
incidental-person cannot-link annotations, quality-stratum coverage, calibrated
decision rule, or jurisdiction-specific biometric approval.
The exploratory runner is isolated from the application environment but uses host
Python 3.14.7; the ADR's production benchmark still requires a fully hash-locked
Python 3.12 runner. Until those requirements are met, use these results only to
prioritize private human review.

## Tests

```sh
python3 -m unittest pipeline.tests.test_face_candidate_adapter -v
python3 scripts/validate-json-contracts.py
```

Tests cover raw cosine behavior, abstention on invalid detection cardinality, the
ban on query-side source labels, the non-forwarding source-context boundary, and a
no-write offline dry run over pinned synthetic assets. A negative path test rejects
both writable frame input and a symlink alias.
