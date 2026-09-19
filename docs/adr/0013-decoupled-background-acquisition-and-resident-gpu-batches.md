# ADR 0013: Decoupled background acquisition and resident GPU batches

- Status: accepted
- Date: 2026-08-29

## Context

The first production RTX 3050 corpus run showed that CUDA inference itself is fast:
8.629 seconds for 335.970 seconds of audio, or RTF `0.025684928`. The complete job
took 33.548 seconds because every invocation repeated runtime admission, input and
model replay, process isolation, model load, and result setup. Paying that roughly
24.9-second fixed cost for each of the approximately 54,098 bounded chunks implied by
known catalogue durations would raise the projected GPU stage from about 5.2 days of
inference to about 20.8 days.

Acquisition was also coupled operationally to each corpus item: download one object,
wait for preprocessing and analysis, then request the next object. That leaves the
network idle during CPU/GPU work and leaves the GPU idle while a provider is slow.
Archive.org objects are especially suitable for a credential-free background lane
because the planner can bind their exact public object URLs and provider byte counts.

The existing integrity boundaries remain valuable. Acquisition work orders are
sealed, the acquisition queue has deterministic ordinals, downloaded media becomes
content-addressed only after hashing and probing, preprocessing emits immutable
receipts, and GPU results are independently sealed. A faster design must retain those
properties and the one-GPU UUID lock. It must not turn a mutable database row or a
filename into completion state.

The pinned `faster-whisper` runtime also contains `BatchedInferencePipeline`, but it
is not a transparent performance switch. It introduces a batch-size parameter,
expects short clip lists when VAD is disabled, and does not preserve all of the
currently admitted decoding and fallback behavior. It therefore needs a separate
accuracy, timestamp, VRAM, and throughput admission before use.

## Decision

Adopt a bounded producer/consumer pipeline with three independently restartable
stages:

```text
sealed public acquisition queue
        |
        v
background acquisition producer  -- content hash + probe -->  acquired CAS
        |                                                        |
        | backpressure                                           v
        +<---------------- verified ready budget -------- CPU preprocess worker
                                                                 |
                                                                 v
                                                    sealed normalized-audio queue
                                                                 |
                                                                 v
                                                   resident single-GPU micro-batch
```

The handoff between stages is an exact immutable result or receipt, never an
in-process promise and never an unverified staging path. Each worker derives pending
state by replaying those durable artifacts, so no general-purpose mutable scheduler
database is required for recovery.

### Background acquisition producer

The initial producer keeps maximum network concurrency at one. It runs independently
of preprocessing and GPU work, so concurrency across stages provides the useful
overlap without weakening the existing output-root writer lock or multiplying
provider load. Both Archive.org `direct_http` work orders and public `yt_dlp` work
orders continue to use the guarded acquisition boundary. There are no credentials,
cookies, private-source fallback, discovery, substitution, or URL rewriting in the
worker.

Within a sealed bundle, the producer may dispatch only the earliest strictly
validated pending ordinal. It must not skip a blocked head item merely to improve
throughput. Multiple future bundles may expose only their validated head items to a
source-aware supervisor; any cross-bundle priority policy must itself be sealed and
must not change an existing bundle's order.

The producer stops admitting work whenever any high-water condition is met:

- fewer than the configured free-space-floor bytes would remain;
- the conservative sum of unprocessed acquisition reservations reaches its byte
  budget;
- the number of acquired-but-not-preprocessed objects reaches its item budget; or
- the run-local item, byte, or wall-time bound is exhausted.

The first operating profile uses the existing 128 GiB free-space floor, one network
slot, one object per guarded dispatch, a ready high-water mark of eight objects, and
a 16 GiB conservative ready-byte budget. The low-water mark is four objects or 8 GiB.
These are supervisor bounds in addition to, not replacements for, each immutable
work order's limits. Free-space hysteresis pauses acquisition below 128 GiB and does
not resume it until at least 160 GiB is available. Ready-byte admission counts actual
durable unprocessed payload bytes plus the full reservation of the next work order.
A provider error is item-local and retryable under a sealed maximum-attempt/backoff
policy; exhausting it reports `blocked_head` and stops rather than skipping the
ordinal. It does not erase completed acquisitions or block offline processing of the
already verified ready set.

### CPU preprocessing worker

Run at most one heavy FFmpeg preprocessing job initially, with the admitted
four-thread profile. It consumes only strict completed acquisition results and
publishes the existing normalized FLAC, proxy, routing document, result, and receipt.
Acquisition may continue concurrently because its network slot and the CPU worker
have separate lifecycle boundaries. If disk latency materially degrades either
stage, the supervisor lowers the producer ready budget; it does not create extra
FFmpeg workers.

### Resident GPU micro-batch worker

Use one isolated process, one UUID-keyed advisory lock, and one loaded `small.en`
model. A finite sealed batch manifest binds an ordered list of v3 media-local GPU work
orders, their exact file hashes, the batch-worker source hash, runtime admission,
model identity, GPU UUID, limits, and safety policy. Before loading CUDA, the worker
strictly validates the whole manifest, verifies that all members share the admitted
model/runtime/GPU/inference profile, and classifies each exact result as completed or
pending.

The first worker implementation processes pending items sequentially through the
same `WhisperModel.transcribe` semantics as the admitted v3 adapter. This is model
residency batching, not multi-item neural batching. It is expected to recover most of
the fixed startup cost without changing decoding. The worker:

- holds the GPU UUID lock for the finite batch;
- loads the model exactly once;
- retains and revalidates one sealed input descriptor at a time;
- enforces an item-local hard deadline plus a batch wall-time limit;
- samples process VRAM continuously and fails closed above the admitted ceiling;
- atomically publishes and immediately exact-replays every item's ordinary private
  result before advancing;
- records the common model-load and worker provenance in a sealed batch receipt;
- replays model, runtime, manifest, and completed member bindings before exit; and
- resumes by reusing only independently valid completed results.

A crash may leave earlier item results complete and the current item absent. A
partial result directory remains an error, as it is today. Shared runtime/model/GPU
drift aborts the batch. Invalid input is recorded as an item failure and stops the
first implementation; a later continue-on-item-error policy would require a new
contract.

Initial admission is capped at 32 work orders, 12 hours of source audio, the current
per-item audio/segment/word/result bounds, 4 GiB process VRAM, and one model worker.
The worker must pass short, 30-minute, two-hour, and thermal-soak cases before a
64-item ceiling or a corpus-wide backfill. Long input support should first be tested directly with the
same model. If external chunking is still needed, use exact 16 kHz sample-index
windows with explicit overlap/core ownership and a separately admitted media-local
reassembly transform; never infer recording coordinates from chunk order.

`BatchedInferencePipeline` is deferred. It may be added only as a distinct recipe
after recording-disjoint accuracy and boundary tests show acceptable word/timestamp
behavior and a VRAM/RTF admission establishes a safe batch size on the 6 GiB card.
Running two model processes concurrently is not an accepted optimization.

### Service lifecycle and resource ownership

Workers are foreground, finite commands suitable for a user-level service supervisor.
The supervisor may restart them, but durable result validation remains the source of
truth. It writes only bounded operational logs and a restart-safe aggregate summary;
it cannot publish, import the catalogue, identify a speaker, delete media, or move
data to cold storage.

At most these heavy operations run together on the current host:

- one public network acquisition;
- one four-thread FFmpeg preprocessing job; and
- one four-thread, one-model GPU inference worker.

Hashing and metadata checks should be streamed and scheduled around the heavy FFmpeg
pass. OCR, diarization, visual embeddings, and other CUDA work use the same UUID
scheduler and cannot overlap the ASR worker until separately admitted.

## Consequences

- Downloading, preprocessing, and inference can overlap without downloading two
  copies or weakening evidence boundaries.
- Archive.org can be kept ahead of compute using its existing exact direct-HTTP work
  orders; no archive-specific media downloader is needed inside the GPU worker.
- With 32-item resident batches and a residual 0.25–1.0 seconds of per-item control
  work, the projected GPU lane is roughly 5.8–6.3 days; a separately soaked 64-item
  ceiling projects roughly 5.6–6.0 days. Observed one-worker CPU preprocessing remains
  the likely approximately 10.2-day critical path. These are extrapolations, not a
  completion promise.
- A resident worker is new executable behavior and needs its own source-bound runtime
  admission and soak evidence before corpus-wide use. The current v2 single-item
  adapter remains preserved evidence; the anomaly-aware v3 single-item adapter is the
  production fallback.

## Initial implementation evidence

The bounded producer was implemented as a finite foreground worker and its first
Archive.org epoch completed on 2026-08-29. Eight exact public objects (314,776,598
bytes) were acquired through one network slot while CPU preprocessing independently
drained completed results. Final offline replay reported eight acquisitions, eight
preprocess acknowledgements, zero ready items, and zero adapter calls.

The v3 single-item fallback completed two private corpus runs before resident-batch
admission. Their measured inference RTF values were `0.0230311` and `0.0228627`.

The final resident worker was independently audited, passed 36 CPU-only contract and
smoke tests, and was source-bound by runtime receipt
`receipt-resident-batch-v1-final-source-bound.json` (physical SHA-256
`4f26a1e8c3c4a7f7ac70819dda20355115627190c8c5f4541ae1bdbba4c0f2ab`). Its first
network-isolated two-item batch then completed 793.913 seconds of audio with one model
load. CUDA inference took 19.290 seconds (RTF `0.0242968`); the complete batch took
45.190 seconds (RTF `0.0569206`, 17.57 times real time). Exact replay reported two
completed items, zero pending, a process-VRAM peak of 805,306,368 bytes, and no sampler
error. Both normalized outputs flagged two preserved model words extending beyond a
segment boundary. They remain private, unreviewed, uncalibrated, media-local machine
artifacts with no catalogue, identity, wiki, or publication authority.
- Faster neural batching remains possible, but only behind a different calibrated
  recipe rather than as an invisible configuration change.
