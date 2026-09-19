# ADR 0002: Use an offline, CPU-first media preprocessing runtime

- **Status:** Accepted
- **Date:** 2026-08-26

## Context

The archive inventory maps 1,922 distinct media URLs covering approximately 2,335
hours and 1.265 TB. The current workstation has about 210 GiB free, and its Radeon Pro
WX 4100 is not exposed to the processing environment through `/dev/dri` or `/dev/kfd`.
Only about 7.7 GiB of RAM was available during the initial audit and swap was nearly
full. The existing active Python 3.14 environment has no usable speech or vision stack.

Preprocessing nevertheless needs to start before model selection. Acquisition,
transcription, and publication must not depend on one mutable workstation environment,
and raw files must never enter Git or be modified in place.

## Decision

The foundation under `pipeline/` is a network-free Python-standard-library program
that invokes the installed FFmpeg and FFprobe executables. It operates on one explicit
local source at a time and emits:

- normalized FFprobe JSON;
- lossless 16 kHz mono signed-16-bit FLAC;
- an aspect-preserving, padded, low-resolution CFR H.264 proxy;
- scene-change and silence-interval routing metadata; and
- a versioned result envelope with commands, hashes, derivations, and catalog-shaped
  records.

The tracked CPU profile gives FFmpeg four threads. Work orders carry the complete
profile rather than referring to an ambient default. Both work orders and results have
versioned JSON Schemas, while runtime validation remains dependency-free.
The audio profile explicitly fixes `audio_sample_format` to `s16`; FFmpeg receives
`-sample_fmt s16`, and the derivative is rejected unless FFprobe reports `s16`.

The output location has no default. It must be absolute and cannot be beneath `/tmp`
or `/var/tmp`. Output paths are addressed first by source SHA-256, then by a
deterministic recipe SHA-256, and finally by a unique execution ID. The recipe covers
parameters, contract and implementation versions, executable byte hashes and sizes,
and full FFmpeg/FFprobe version/build output. Resolved executable paths and a UUID
execution nonce remain execution provenance rather than recipe identity.

Derivatives and results are created atomically without replacement and sealed
read-only. A prior execution is reusable only after its immutable result envelope,
all current artifact paths/sizes/hashes/IDs/probes, and all catalog location and
derivation relationships validate. Reused bytes are exposed through run-local hard
links under a new processing-run ID, with explicit prior-envelope path and digest
lineage. Partial executions have no completed result and cannot authorize reuse;
tampered, stale, writable, or inconsistent completed results fail closed. The input
file's device, inode, byte count, modification time, and content hash are compared
before and after every actual run.

Dry-run mode is read-only: it hashes and probes the source so that paths and commands
are exact, but creates no output directories and executes no derivative or routing
scan.

The preprocessing program does not open SQLite. Its `catalog_records` section aligns
with the catalog's `processing_runs`, `run_inputs`, `media_objects`, `media_locations`,
`media_derivations`, and `artifacts` fields. The implemented
`himr-corpus import-preprocess-result` boundary validates the exact completed envelope
and inserts it transactionally. It creates unreviewed renditions only where recording
context already exists, keeps artifacts and routing observations private, and creates
no publication decision. It also re-stats and hashes the current input and every local
artifact before recording verified media. This keeps filesystem work resumable without
making partial database state authoritative.
Recipe identity and execution identity are intentionally separate. Artifact identity
includes the unique processing-run ID as well as kind and content hash, so byte-identical
outputs from distinct runs cannot collide. Media handoff uses the
catalog-observation field `first_cataloged_at`, preserving an upstream value when one
is supplied and otherwise labeling the preprocessing observation basis; it does not
invent an acquisition time.

## ML container boundary

Later ML stages will run in a Python 3.12 container pinned by immutable image digest
and dependency hashes. The interface reserves read-only `/input` and `/models` mounts,
an explicit read-write `/output` mount, and disabled networking. Models remain external,
checksummed, licensed registry entries rather than image contents. No image or model is
downloaded as part of this decision.

The ML result envelope must link to its parent preprocessing run and input artifacts,
record exact model and image revisions, use integer-millisecond intervals, keep raw and
calibrated scores separate, and emit anonymous track or cluster IDs. Human identity
assertions and publication decisions remain separate catalog operations.

## Consequences

- The pilot can exercise real media now without waiting for a GPU or choosing every
  future model.
- Re-running the same source and recipe does not decode a verified derivative again,
  but still records a distinct, auditable execution and reuse lineage.
- Long AV1 originals can be decoded once; subsequent visual work can use the proxy and
  audio work can use the normalized FLAC.
- The full raw archive still cannot fit on this workstation. Acquisition must remain
  queued/on-demand or use a larger external media store.
- Scene and silence outputs are routing suggestions only. They do not establish
  speaker count, identity, meaning, or publishability.
- Python 3.12 ML image selection, dependency locking, model licensing, benchmarking,
  and calibration remain later decisions behind the documented container boundary.
