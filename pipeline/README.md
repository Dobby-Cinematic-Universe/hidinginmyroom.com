# Offline media preprocessing

This directory contains the first, deliberately small processing layer for the HIMR
corpus. It accepts an existing **local** media file, preserves that file byte-for-byte,
and emits private, content-addressed derivatives and metadata for later transcription,
OCR, diarization, and visual analysis.

The preprocessing program does not download media, call a service, load an ML model,
identify a person, or publish anything. It uses only the Python standard library,
`ffmpeg`, and `ffprobe`.

The separate [`gpu/`](gpu/) profile is a private, synthetic-only RTX 3050 readiness
lane. It uses an exact CPython 3.12.14 `uv` lock, a sealed model snapshot, Python
isolated mode, an actual bubblewrap network namespace, explicit GPU-device bindings,
finite byte/duration/wall limits, and crash-atomic private receipts. Its successful
smoke does not authorize corpus GPU backfill, rerun existing ASR, assign identities,
update the catalogue or wiki, or publish machine output. See
[`gpu/README.md`](gpu/README.md).

The isolated [long-form ASR successor](../docs/LONGFORM_ASR_PIPELINE.md) provides an
opt-in direct-first or adaptive logical-span path, resumable immutable hypotheses, and
one coverage-audited recording transcript without persistent audio chunks. Its
low-level commands remain opt-in; the Archive deployment registers a separately
supervised companion through its adjacent registration rather
than adding an ordinary controller lane. Its long-form thresholds and resource
envelope remain explicitly `candidate_unsoaked`.

The independent [Salad cloud transcription lane](../docs/SALAD_CLOUD_TRANSCRIPTION.md)
provides opt-in, bounded cloud jobs for already-admitted normalized audio. It prefers
whole-recording jobs, uses S4 uploads or temp.sh above S4's size limit, and retains
explicit cost estimates, durable submission holds, and separate
private cloud transcripts. It does not switch the active autonomous campaign;
network access and potentially paid submissions require `--allow-cloud`.

The separate, opt-in [`asr_whispercpp.py`](asr_whispercpp.py) adapter can invoke a
caller-provisioned, hash-pinned whisper.cpp binary and model against the normalized
FLAC. Its stricter work-order, result, glossary, review, and offline boundaries are
documented in [`ASR_WHISPERCPP.md`](ASR_WHISPERCPP.md). No binary, model, or real ASR
run is included in this repository. Invalid UTF-8 engine output is rejected without
replacement decoding and retained only as exact bytes plus a private deterministic
quarantine receipt. Execution is Linux-only and binds the verified executable, model,
audio, and optional glossary through retained `/proc/self/fd` descriptors with final
logical-path tamper checks; deterministic logical argv is recorded separately from the
exact child-facing descriptor argv.

The private [`asr_whispercpp_batch.py`](asr_whispercpp_batch.py) control layer derives
immutable raw-pass ASR work orders from sealed local-window results and exact
read-only catalog lineage. It binds the registered `small.en` weights, uses each full
artifact-local probe duration with offset zero and a null glossary, then replays all
bytes and bindings before and after sequential dispatch. Its shared engine allowlist
admits v1.8.7 for new work while retaining exact v1.8.3 manifests for validation-only
legacy replay. It never writes the catalog or publishes output. See
[`ASR_WHISPERCPP_BATCH.md`](ASR_WHISPERCPP_BATCH.md).

The separate, validate-only
[`asr_whispercpp_batch_archive.py`](asr_whispercpp_batch_archive.py) lane preserves one
superseded v0.1.0/v1.8.3 batch as explicitly non-executable evidence without widening
the active runner's legacy allowlist or changing its pinned source identity. It reads a
private immutable disposition receipt, both exact sealed trees, and narrowly bound
historical evidence; it has no execution, import, publication, database, network, or
write path. See
[`ASR_WHISPERCPP_BATCH_ARCHIVE.md`](ASR_WHISPERCPP_BATCH_ARCHIVE.md).

The separate [`asr_whispercpp_result_store_seal.py`](asr_whispercpp_result_store_seal.py)
administrative lane prepares, applies, and validates chmod-only seals for an explicit
allowlist of completed result directories. It binds sealed batch/queue membership,
exact result/raw/normalized hashes, retained inode identities, and catalog-free
importer validity; application preserves content and mtime while changing the three
files from `0644` to `0400` and the result directory from `0700` to `0500`. It has no
ASR, catalog import, database, network, identity, or publication command. See
[`ASR_WHISPERCPP_RESULT_STORE_SEAL.md`](ASR_WHISPERCPP_RESULT_STORE_SEAL.md).

Historical queue-only v0.4 seal receipts have a separate, strictly read-only
[`asr_whispercpp_v04_receipt_audit.py`](asr_whispercpp_v04_receipt_audit.py)
compatibility lane for restart-induced device-number drift. It accepts only the
exact retained v0.4 sealer and v0.2 queue/source-schema hashes, has no plan, apply,
or write command, and does not authorize the still device-bound media-local bridge.
See [`ASR_WHISPERCPP_V04_RECEIPT_AUDIT.md`](ASR_WHISPERCPP_V04_RECEIPT_AUDIT.md).

Completed paired contextual passes use the independent
[`contextual_asr_result_store_seal.py`](contextual_asr_result_store_seal.py) lane. It
binds exactly one sealed contextual batch manifest to one explicit result per work
order, preserves mixed `0600`/`0644` input modes in its plan, and can later transition
only the three files to `0400` and each result directory to `0500`. Replay binds each
file's exact recorded pre-mode and the manifest-derived control root; a retained
private lock serializes apply through receipt commit. Its plan and receipt are
text-free, its apply operation is rollback-aware and idempotent, and it does not call
or broaden the raw-result sealer. See
[`CONTEXTUAL_ASR_RESULT_STORE_SEAL.md`](CONTEXTUAL_ASR_RESULT_STORE_SEAL.md).

The isolated [`preprocess_asr_queue.py`](preprocess_asr_queue.py) materializer seals
ASR work orders for full-media preprocess FLACs from either complete immutable
preprocess receipts or exact read-only catalog admission. It consumes the shared
current whisper.cpp engine profile, preserves result/source/artifact hashes, fixes
timestamps to artifact-local milliseconds, and leaves catalog context null until a
separate coordinate translation is admitted. See
[`PREPROCESS_ASR_QUEUE.md`](PREPROCESS_ASR_QUEUE.md).

The bounded [`preprocess_asr_queue_runner.py`](preprocess_asr_queue_runner.py)
validates and dispatches only sealed-receipt preprocess queues. It replays every pin,
stable-reads each work order immediately before a single sequential adapter call,
and performs a final replay on success or per-item failure. Completed results are
reused only after the catalog-free strict importer validator rehashes all provenance
and artifact bytes. Near-silent candidates remain in ordinal order but route to
review without ASR by default. The runner has validate and adapter dry-run modes and
never writes the catalog, publication state, identity state, or its own control state.

The offline [`audio_fingerprint.py`](audio_fingerprint.py) adapter uses the host's
hash-pinned FFmpeg Chromaprint muxer to preserve raw full-track/window/chunk
fingerprints. Its conservative comparison helper emits human-review candidates only;
v1 retains its historical same-recipe restriction, while the isolated sealed-envelope
v2 lane supports different input/recipe results under the same exact engine/build and
raw-format binding. Neither performs approximate matching or asserts duplicate/parent
identity. See
[`AUDIO_FINGERPRINT.md`](AUDIO_FINGERPRINT.md).

The offline [`visual_fingerprint.py`](visual_fingerprint.py) stage decodes only
explicit, half-open-window timestamps from a sealed video/proxy, retains exact private
32×32 grayscale evidence, and computes a fixed-integer 64-bit perceptual hash. Its
bounded Hamming comparison emits uncalibrated human-review candidates only and fixes
all identity/duplicate/parent/ownership/relationship assertions to false. See
[`VISUAL_FINGERPRINT.md`](VISUAL_FINGERPRINT.md).

The private [`face_candidate_adapter.py`](face_candidate_adapter.py) pilot consumes
only explicitly sealed PNG frames, detects faces with hash-pinned YuNet, and creates
SFace crops, embeddings, and explicitly requested pairwise review candidates. It
keeps all biometric artifacts owner-private, abstains unless each compared frame has
exactly one detection, and cannot forward a source-context label into an identity
decision. Similarities remain raw and uncalibrated. See
[`FACE_CANDIDATE_ADAPTER.md`](FACE_CANDIDATE_ADAPTER.md).

The separate synthetic-only [`yunet_face_detector.py`](yunet_face_detector.py)
bridge runs the pinned YuNet detector offline over a complete, explicitly
shot-aligned 25 fps PNG fixture and emits the anonymous frame-local detection contract
used by the tracker. It preserves zero/multiple detections and has no crops,
embeddings, identities, active-speaker decisions, database access, network, or
publication authority. Real media remains barred until dense frame and shot lineage
is admitted. See [`YUNET_FACE_DETECTOR.md`](YUNET_FACE_DETECTOR.md).

The geometry-only [`shot_local_face_tracker.py`](shot_local_face_tracker.py) consumes
a sealed, hash-pinned frame-local detection artifact with explicit shot bounds. It
uses deterministic constant-velocity prediction and Hungarian IoU assignment, logs
every assignment and gap, and closes all tracks at cuts. It has no embedding,
identity, active-speaker, database, network, or publication interface. See
[`SHOT_LOCAL_FACE_TRACKER.md`](SHOT_LOCAL_FACE_TRACKER.md).

The separate [`sparse_frame_router.py`](sparse_frame_router.py) stage consumes a
sealed preprocessing result and extracts a resource-bounded set of lossless PNGs from
its CFR proxy at scene and periodic-coverage timestamps. It records exact decoded PTS
and emits explicit OCR-candidate routes while leaving OCR, text presence, content, and
identity unevaluated. Its contract and safety boundary are documented in
[`SPARSE_FRAME_ROUTER.md`](SPARSE_FRAME_ROUTER.md).

The offline [`ocr_preflight.py`](ocr_preflight.py) feasibility gate hashes the current
FFmpeg/Tesseract assets and repeats synthetic English, Japanese, and Korean probes. It
still fails closed against the repository-default system tessdata path when its pinned
Japanese/Korean files are absent. A separately reviewed, ignored Fedora 44 bundle now
passes the complete synthetic gate with a pinned Tesseract CLI and all three language
models; raw scores remain uncalibrated, and FFmpeg's filter still exposes no word
geometry. The gate never processes corpus frames or emits OCR observations. See
[`OCR_PREFLIGHT.md`](OCR_PREFLIGHT.md).

The private [`whispercpp_vad.py`](whispercpp_vad.py) adapter runs the exact reviewed
whisper.cpp v1.8.7/Silero v6.2.0 GGML pair over sealed 16 kHz mono FLAC work units. It
preserves raw centisecond coordinates, normalizes them to artifact-local milliseconds,
records bounded tail clipping, and emits speech-candidate intervals with null scores
and no calibrated probability. It contains an upstream v1.8.7 CLI parser defect by
pinning the source defaults and omitting the broken minimum-silence controls. Its v0.3
execution boundary copies the verified single-link engine/model bytes into write-sealed
Linux descriptors, bounds child pipes while running, retains the full output path with
directory descriptors, and publishes private results by descriptor-relative no-replace
rename. It has no speaker, face, active-speaker, action, catalog, or publication
authority. See
[`WHISPERCPP_VAD.md`](WHISPERCPP_VAD.md).

The separate [`speaker_activity_router.py`](speaker_activity_router.py) stage plans
speaker, face, and active-speaker workloads without running a model or independently
inferring identity. It binds completed preprocessing and ASR results to a hashed
human-review document, reserves `unknown_single` as the default solo label, and can
forward the narrow public label `Daniel` only from a complete-recording human
source/speaker attestation under the explicit
`confirmed_daniel_source_solo_presumption` basis. It preserves overlap and
media-origin distinctions and gates hash-pinned future capabilities against an
explicit CPU/GPU resource envelope. Its contracts and limitations are documented in
[`SPEAKER_ACTIVITY_ROUTING.md`](SPEAKER_ACTIVITY_ROUTING.md).

The offline [`local_window.py`](local_window.py) stage handles admitted long recordings
without remote interval downloads. It binds one completed public acquisition result,
the full parent SHA-256, pinned FFmpeg/FFprobe builds, and a contiguous set of exact
half-open source-time coordinates into an immutable bundle. Each window independently
produces private normalized FLAC and CFR proxy artifacts, so failed windows can be
retried without repeating completed ones. See [`LOCAL_WINDOWS.md`](LOCAL_WINDOWS.md).

The private [`transform_calibration_v2.py`](transform_calibration_v2.py) evidence lane
compares one guarded post-live acquisition with a credential-free finalized
reacquisition of the same exact YouTube video+audio formats. It binds both acquisition
envelopes and media hashes, observed yt-dlp launcher/version evidence, exact
calibration runtime trees and probes, and distributed SSIM and audio-correlation
measurements into a deterministic sealed receipt. It never
opens the catalog or creates a transform, review, relationship, transcript, or
publication decision. See [`TRANSFORM_CALIBRATION.md`](TRANSFORM_CALIBRATION.md).

The private [`preprocess_batch.py`](preprocess_batch.py) control layer seals a reviewed
list of completed acquisition results, materializes deterministic CPU-profile work
orders, and advances them sequentially through immutable per-item receipts. It
re-hashes the exact upstream result and full media bytes on selection, materialization,
status, and execution; it never downloads, publishes, imports a DB record, or grants
completion without a valid receipt. Policy-bearing v30 local acquisitions additionally
require a replayed private-acquisition seal receipt and retain the exact handling
policy and receipt/plan hashes through selection, manifest, item-receipt, and run
control. See
[`PREPROCESS_BATCH.md`](PREPROCESS_BATCH.md).

## Safety and runtime limits

- A work order must contain an absolute source path and an explicit absolute output
  root. There is no default output location.
- Output roots under `/tmp` and `/var/tmp` are rejected. Long media jobs must use a
  durable, capacity-monitored volume.
- URLs are rejected. This program has no network code and never invokes a downloader.
- The source is opened read-only. Its device, inode, size, and nanosecond modification
  time are checked after processing, and its SHA-256 is the parent media identity.
- The tracked CPU profile gives FFmpeg four threads. Run one work order at a time on the
  current host until measured memory and real-time factors justify more concurrency.
- Full frame sequences are not created. The proxy and compact routing JSON are the only
  visual outputs of preprocessing. Sparse frames are created only by the separate,
  explicitly capped router.
- Every produced artifact is private by default. Raw media, derivatives, and work
  directories are ignored by Git.

## Requirements

- Python 3.10 or newer; the future ML environment targets Python 3.12
- `ffmpeg` and `ffprobe` on `PATH`
- An explicit output volume outside temporary directories

No Python packages need to be installed for this stage.

## Work orders

Generate a complete work order from the tracked CPU profile:

```sh
pipeline/bin/media-preprocess create-work-order \
  --job-id pilot-001 \
  --source /srv/himr-media/incoming/video.mp4 \
  --output-root /srv/himr-media/derived \
  > /srv/himr-media/work-orders/pilot-001.json
```

For an already checksummed source, add `--expected-sha256` to fail before any output is
created if the file differs. If the source is already in the normalized catalog, pass
`--source-first-cataloged-at` to preserve that earlier catalog timestamp. Otherwise the
result explicitly uses the preprocessing observation time as `first_cataloged_at` and
states that preprocessing is not claiming an acquisition timestamp. Validate a
hand-edited order with:

```sh
pipeline/bin/media-preprocess validate \
  --work-order /srv/himr-media/work-orders/pilot-001.json
```

The normative JSON shape is in
[`schemas/work-order.schema.json`](schemas/work-order.schema.json). An illustrative
order is in [`examples/work-order.example.json`](examples/work-order.example.json).
Runtime validation is authoritative and does not require a JSON Schema package.

## Dry runs

```sh
pipeline/bin/media-preprocess run \
  --work-order /srv/himr-media/work-orders/pilot-001.json \
  --dry-run
```

A dry run reads and hashes the source and invokes `ffprobe`, then prints the normalized
input identity, recipe hash, content-addressed paths, and exact planned FFmpeg command
arrays. It creates no directories or files. It does not run the derivative or routing
scans.

## Processing

```sh
pipeline/bin/media-preprocess run \
  --work-order /srv/himr-media/work-orders/pilot-001.json
```

Contract version 1 performs four operations:

1. Normalize FFprobe metadata into stable millisecond, stream, frame-rate, language,
   and chapter fields.
2. Extract the primary audio stream as lossless 16 kHz, one-channel, signed-16-bit
   FLAC. Both the FFmpeg command (`-sample_fmt s16`) and the generated artifact probe
   are checked against that contract.
3. Make a padded, aspect-preserving 640×360, 25 fps CFR H.264 review/analysis proxy.
4. Scan for FFmpeg scene changes and silence intervals, then emit conservative routing
   candidates for later ASR, OCR, and visual work. Scene detection uses `sc_pass=0` so
   static sources still feed video packets to the null muxer and audio silence logs
   are retained even when no scene crosses the configured threshold.

Routing values are workload suggestions, not factual annotations. In particular, this
stage never labels a speaker or declares that a recording has only one speaker.
FFmpeg may report a decoded-video scene timestamp or final decoded-audio silence end
slightly beyond the normalized container duration. The producer applies one inclusive
250 ms decoder/container-tail policy to both: a coordinate at most 250 ms beyond
coverage is clipped to the declared endpoint, while a larger discrepancy fails the
run. Scene rows collapsed onto one timestamp are deterministically deduplicated by
retaining the greatest score. Silence rows are sorted, deduplicated, and required to be
nonoverlapping. The producer, prior-result replay validator, and corpus importer each
independently recompute exact row counts, summed/clamped silent duration, and the
six-decimal silent fraction from the primitive routing rows and coverage metadata.

## Content-addressed layout

The source SHA-256 and deterministic recipe SHA-256 select a recipe tree. Every
actual or replayed execution then receives a distinct processing-run ID:

```text
<output-root>/
└── media/sha256/ab/<full-source-sha256>/
    └── recipes/<full-recipe-sha256>/
        └── executions/<unique-processing-run-id>/
            ├── probe.normalized.json
            ├── routing.json
            ├── result.json
            └── artifacts/
                ├── audio-16khz-mono.flac
                └── proxy-640x360-25fps.mp4
```

The recipe digest covers the work-order operations, complete profile, contract and
implementation versions, executable byte hashes and sizes, full FFmpeg/FFprobe build
output and its hash, and the readable build configuration. Resolved executable paths
are retained as execution environment provenance but do not define recipe identity.
Changing a derivation input creates a different recipe tree; repeating one creates a
new execution ID derived from a fresh UUID nonce.

Each result and artifact is admitted atomically without replacement and sealed
read-only. A per-recipe OS lock serializes discovery and admission. Reuse is allowed
only from a completed, sealed prior `result.json`: the runtime validates every result,
artifact path, size, SHA-256, run-scoped ID, normalized probe, catalog handoff, media
location, and derivation relationship. It also validates the prior-result lineage.
Historical v0.3.3 executions then made run-local hard links and emitted a new
immutable result envelope. New bundles use the hash-anchored single-link successor:
it keeps the v0.3.3 result contract, verifies the exact legacy implementation bytes,
and publishes each reused artifact as a distinct inode using Btrfs reflink or a
verified byte-copy fallback. Shape-valid orphan, partial, stale, writable, symlinked,
or tampered files are never reuse authority. An invalid completed envelope fails
closed and requires manual quarantine rather than silent regeneration.
Referenced prior result envelopes form an auditable acyclic chain and must be retained
with the execution; pruning or relocating one intentionally invalidates later reuse.

## Result envelope and catalog handoff

The result written to `result.json` and printed to stdout follows
[`schemas/result.schema.json`](schemas/result.schema.json). Every version-1 envelope,
probe, routing, artifact, and catalog-row object has exact keys and typed values;
unknown properties are rejected. It contains:

- the source media ID, SHA-256, size, URI, and before/after integrity observations;
- the deterministic recipe ID, unique processing-run ID, executable/build provenance,
  full parameters, commands, and any verified prior-result lineage;
- every artifact's SHA-256, byte count, storage URI, visibility, and normalized probe;
- scene/silence observations and bounded routing candidates; and
- `catalog_records`, whose field names intentionally align with the normalized SQLite
  tables `processing_runs`, `run_inputs`, `media_objects`, `media_locations`,
  `media_derivations`, and `artifacts`.

The envelope is an import contract, not a direct database mutation.
`himr-corpus import-preprocess-result` validates its exact completed version-1 shape,
creates catalog-specific IDs where required, and inserts all records in one
transaction. Artifacts and routing observations remain private, and the importer
creates no publication decision. The pipeline never opens the corpus database itself.
Media rows use `first_cataloged_at`, not a fabricated `acquired_at`; when no upstream
catalog time is provided, the input envelope labels the value as a preprocessing
observation and leaves acquisition time unclaimed.

## Future ML boundary

The pinned Python 3.12 container interface is specified in
[`ml-container-interface.md`](ml-container-interface.md). No image, model, weight,
license acceptance, or model cache is downloaded by this preprocessing layer.

## Tests

Run:

```sh
python3 -m pip install -r scripts/requirements-json-contracts.txt
python3 scripts/validate-json-contracts.py
pipeline/tests/run.sh
```

The standard-library test suite makes short FFmpeg `lavfi` fixtures under the explicit
repository-local `pipeline/.test-work/` root. It checks no-write dry runs, source
preservation, 16 kHz mono s16 FLAC, 640×360/25 fps CFR output, abrupt scene detection,
silence detection, audio-only and video-only routing, catalog-shaped records,
run-scoped artifact identity, unique replay execution identity, sealed-envelope reuse,
hard-link identity, writer contention, tampered artifact/envelope rejection,
stale-copy failure, partial execution isolation, hash mismatch failure, and
rejection of `/`, existing-file, and `/tmp` output roots. Routing regressions cover the
exact 250/251 ms decoder-tail boundary, negative and zero coordinates, fallback silence
starts, scene-tail clipping/deduplication, semantic replay tampering, and recipe
separation across implementation versions.
Planned and completed results, including routing and audio-only/video-only variants,
are checked against the result schema. The pinned `jsonschema` package is used only
for development and CI.
The same command also runs the offline fake-engine ASR contract tests described in
[`ASR_WHISPERCPP.md`](ASR_WHISPERCPP.md); it never runs a real model.
The fake-engine VAD suite exercises exact output parsing, private immutable replay,
and the centisecond-to-millisecond contract; it never runs or evaluates a VAD model.
It also runs the sparse-frame contract suite described in
[`SPARSE_FRAME_ROUTER.md`](SPARSE_FRAME_ROUTER.md); that suite invokes only pinned
local FFmpeg and never runs OCR or a vision model.
The visual-fingerprint synthetic suite uses only pinned local FFmpeg and standard
Python integer arithmetic; its transcode and below-threshold fixtures test routing
behavior, not truth or accuracy.
The speaker/active-speaker adversarial suite hashes non-executable fake capability
bytes to test provenance and resource gating, but never performs or claims ML
inference.
The fixtures and all derivatives are removed at the end.
