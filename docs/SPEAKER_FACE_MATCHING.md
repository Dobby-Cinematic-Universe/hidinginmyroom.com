# Anonymous speaker-to-face matching

The separate private stage in
[`speaker_face_matching.py`](../pipeline/speaker_face_matching.py) associates a
diarization speaker with a **shot-local face track in a sampled clip**. It does
not recognize people, assign real names, link identities across recordings or
shots, or change transcripts, screening, acquisition, the catalogue, or the site.

The implemented route is completed diarization → clean speech clips → aligned
audio/video → YuNet face detection and geometric tracking → LR-ASD active-speaker
scores → conservative candidate association or explicit unknown.

## Readiness and limits

The code and model-free tests are implemented, including native FFmpeg tests on
synthetic media. **Real LR-ASD inference and accuracy are not yet validated or
enabled.** No model downloads, runtime installation, inference jobs, services, or
changes to the running archive screen were performed during this build.

This is a private unvalidated pilot under
[ADR 0007](adr/0007-speaker-and-active-speaker-models.md), not production approval.
Before inference, acquire the reviewed model artifacts, register a separate
offline runtime and bundle, and complete a small reviewed real-media pilot with
preprocessing/reference-score parity and accuracy/resource measurements. The
current screening runtime is not the matching runtime. The default resource
envelope is a safety limit, not a demonstrated performance or memory estimate.

This version prepares evidence for review; it does not implement automatic named
speaker labeling. Human-attested names, if added separately, must retain their
own evidence and scope. A candidate association does not authorize publication.

## Finite selection and coverage

Input is the hash-bound immutable plan of the
[separate diarization pipeline](SCREENED_DIARIZATION.md). Planning reads only
verified completed results, never partially written output. `job_ids` optionally
filters the source jobs. The plan snapshots completion at that moment; newly
completed diarization jobs require a new request and workspace.

Ordinary diarization turns determine clean single-speaker intervals. Other-speaker
overlap is excluded, including overlap retained in the ordinary representation
but hidden by the exclusive transcript-alignment view. The default adds 200 ms
boundary guards, samples up to three non-overlapping 2–5 second clips per speaker
distributed over eligible speech, and caps each recording at 96 clips. Speakers
without a long enough clean interval are explicitly recorded, not invented.

The request's `limits.max_clips` independently caps the whole plan, default 256.
Unscheduled clips are counted as deferred. Coverage records preserve ordinary,
clean, eligible, and sampled speech durations. To handle deferred recordings, use
disjoint job filters in subsequent plans. A recording exceeding its own policy cap
requires a separately reviewed policy/new plan. There is no unbounded watcher.

Results apply **only to the sampled intervals**. Even a strong candidate in every
sample does not label the rest of a video, identify off-screen speech, or establish
that a face track in a different shot depicts the same person.

## Timestamp and visual preparation

Each clip keeps source-zero, half-open integer-millisecond timestamps on the
25 fps / 40 ms grid. FFmpeg decodes one bounded clip into 640×360 BGR frames and
16 kHz mono signed 16-bit PCM. A bounded seek preroll preserves partially spanning
audio packets. Video is aspect-preserving and letterboxed. The private raw video
artifact is named `video.rgb` for the worker binding, but its declared pixel order
is **BGR**, not RGB. No whole recording is duplicated.

The timing receipt checks pre-frame-rate-conversion source timestamps, every
output frame, contiguous audio sample timestamps, byte counts, and digests.
Source frame durations above 50 ms, gaps, displaced audio,
truncation, or unsupported timing becomes `needs_review`. There is no invented
audio padding, time stretching, or guessed timestamp repair. Timestamp verification
establishes container alignment, **not that the source itself is lip-synchronized**.
Some valid but unsupported media will conservatively need review.

YuNet runs on CPU. Geometry-only tracks terminate on cuts, missing detections or
ambiguous assignments; no recognition embeddings or cross-shot stitching is used.
LR-ASD accepts only usable contiguous tracks of at least 25 frames with faces at
least 64 pixels in the prepared image. Its track crops are grayscale 112×112.
Small/border-clipped tracks are unscored; clips with detected cuts are not scored.
The cut detector is heuristic and there is no dedicated occlusion detector, so
missed faces, undetected edits, dubbing, occlusion and incidental mouth motion remain
pilot risks. Accepted border or tiny detections remain unscored competitors rather
than disappearing from the evidence and making another face look unambiguous.

The audio features use upstream-compatible 13-coefficient MFCCs at 25 ms windows
and 10 ms steps. Any final feature-row wrap follows the recorded upstream feature
alignment recipe; it is not audio or video padding. This adapter uses one bounded
track context without the upstream multiscale ensemble or score smoothing. Crop
and reference-score parity still need real-model validation.

## Association semantics

Each usable tracked face receives an independent raw positive-class active-speaker logit.
These values are not probabilities or calibrated confidence. The default candidate
gate requires 90% track coverage, positive logits on at least 80% of scored frames,
a strictly positive mean raw logit, and a mean-logit margin of at least 0.5 over a rival.
These are uncalibrated pilot thresholds, not measured HIMR error rates.

Missing evidence, inadequate coverage, ambiguous competing tracks, simultaneous
active tracks, cuts, or unusable visible faces lead to `unknown`. The stage never
forces one of the visible faces to be the speaker. Successful association remains
`candidate_match`, not a verified identity. Labels retain the diarization run,
media SHA, clip, shot and face-track scope. There is no calibrated output mode.

## Model and runtime registration

The engine admits only `Junhua-Liao/LR-ASD` revision
`1b6dcd2d8fc2895683de6508ec6294ec47d388ca`, including
`weight/pretrain_AVA.model`, and the pinned YuNet 2023 model with SHA-256
`8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4`.
The exact ten upstream files, byte counts and Git blob identities are recorded in
`UPSTREAM_FILES` in
[`speaker_face_matching_engine.py`](../pipeline/speaker_face_matching_engine.py).
Do not substitute model files based only on their filenames.

The `himr_lrasd_bundle` manifest binds the source root and files, upstream MIT
license, reviewer attestation, YuNet artifact, and `himr_lrasd_runtime` manifest.
The runtime records an exact Python executable/version, complete installed-file
inventory, and all installed package versions and wheel hashes. Required packages
include PyTorch 2.6 or newer, NumPy, SciPy, `python-speech-features==0.6` and only
the headless OpenCV distribution. Exact installed versions must be registered;
this is not an instruction to upgrade the active environment.

`admit_bundle(binding)` performs read-only admission without importing ML. Worker
startup rechecks the registered runtime before imports, denies network access,
removes inherited credentials from its environment, and uses restricted
`weights_only=True` checkpoint loading with a strict complete tensor state dict.
Only the pinned model/loss modules execute; upstream CLI/training wrappers do not.
No unsafe checkpoint fallback, downloads, training, face recognition, or voice
enrollment is provided. The parent retains ownership of the worker process group,
cancellation, aggregate memory boundary, file-size and wall-time limits.

## Planning and running

Start with [the example request](../pipeline/examples/speaker-face-matching-request.example.json).
Replace illustrative paths and hashes. Use a new private workspace under an owned
0700 parent, separate from source, model/runtime and publication directories, and
keep the request outside the workspace. Leave `python` and `model_bundle` as null
for a blocked setup preview; enabling them later requires a new request/workspace.

```sh
pipeline/bin/speaker-face-matching plan \
  --request /absolute/request.json --expected-sha256 REQUEST_SHA256

pipeline/bin/speaker-face-matching status \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256
```

After approved setup and only when competing work is idle, explicitly launch a
dedicated cgroup with the example's 4 GiB aggregate host ceiling and no swap:

```sh
systemd-run --user --unit=himr-matching-pilot-001 \
  --property=MemoryMax=4G --property=MemorySwapMax=0 \
  --property=OOMPolicy=stop --property=KillMode=control-group \
  --property=TimeoutStopSec=60 --property=RuntimeMaxSec=3660 \
  --property=Restart=no --property=UMask=0077 \
  --property=WorkingDirectory=/srv/himr \
  -- /usr/bin/python3 -B /srv/himr/pipeline/speaker_face_matching.py run \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256
```

CPU is the conservative example default. CUDA is supported only with an explicit
GPU UUID and separately registered compatible runtime. GPU allocator fraction is
not a hard bound on all driver allocations. Do not start a competing GPU campaign
during a clip. `blocking_units` includes the current fast-screen service by default;
execution checks it before launching work, never stops it, and refuses to proceed
while it is active. No service is created by planning or status inspection.

## Recovery, retention and statistics

Per-clip immutable `result.json` binds the completed upstream diarization, clip
scope, decoder receipt, worker request, engine output, model/runtime provenance
and replayable association. Resume validates completed proofs and skips those
clips. Source metadata or implementation changes, I/O errors, corrupt proofs and
worker/setup failures stop execution rather than silently replace evidence.
Raw source videos are not fully rehashed; selection relies on the archive's
immutable content store plus existing SHA and current metadata witness.

Unsupported decode or a decode timeout commits a terminal `needs_review` result
and allows other clips to continue. It is not repeatedly retried by resume. Review
the cause and use a new plan/workspace when inputs or policy legitimately change.
Cancellation kills and reaps the owned child group, preserving committed results.
An interrupted uncommitted clip can be retried in a fresh attempt.

By default only newly generated `video.rgb` and `audio.pcm` in that attempt are
removed on success or failure. Original media and durable proofs are never deleted.
A maximum-size raw clip uses about 82.6 MiB; decoding is sequential. Setting
`retain_clip_media` retains private debug media and makes storage cumulative.
Abrupt host loss or SIGKILL can leave a partial attempt; resume does not recursively
delete old directories. Proofs do not claim to rehash clip media after deletion.

Status reports selected recordings, planned/processed/remaining clips, candidate
matches, unknown associations, review clips, deferred clips, and historical pending
diarization jobs. `completed` means the finite clip plan has terminal results,
not that an archive or all speech has been matched. `completed_with_reviews` keeps
decode exclusions visible; `empty_selection` means no clips were eligible. The
separate `status.json` also records current decode/matching, blocking, failure and
interruption states. No stage changes upstream progress counters or labels.
