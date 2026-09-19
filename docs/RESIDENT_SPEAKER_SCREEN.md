# Resident CPU/GPU speaker screen

This is a separate, explicit screening path. It does not change the original
[CPU screener](CPU_SPEAKER_SCREEN.md), acquisition, ASR, controllers, source files,
catalogue, publication, or cloud settings. It screens anonymous voice diversity;
it does not identify a person or perform full diarization. No archive campaign
or automatic discovery service is installed or started.

## What changes

One isolated model worker loads the same hash-bound VAD/ECAPA pair once per finite
run and reuses those weights across up to 128 explicitly selected recordings.
Speech/voice evidence and classifier caches remain recording-local. VAD recurrent
state resets for every probe. No previous recording's vectors enter another
recording's classification.

One or two spawned CPU decoders prepare fixed probe batches. Decoding the next
batch overlaps inference on the current batch. CUDA mode keeps VAD and feature
extraction on CPU and runs ECAPA on the selected GPU. Only equal-length feature
tensors are batched; excerpts are not padded or concatenated to force batching.
Float32 inference is used without TF32, autocast, or mixed precision. CUDA
failure does not silently switch a recording to CPU.

Persistent CPU mode has the same model-reuse and decoder-overlap benefits, without
requiring a GPU. GPU mode is opt-in, not an assumption that every workload will
be faster. Input selection, sampling density, thresholds and speech support are
copied from the explicit work orders. Select fast-triage or coverage-preserving
orders with the existing `speaker_screen.py prepare` command first if desired.

## New request and pinned runtime

Start with the inert [request example](../pipeline/examples/speaker-screen-accelerated-request.example.json)
or [CUDA example](../pipeline/examples/speaker-screen-accelerated-cuda-request.example.json),
and [schema](../pipeline/schemas/speaker-screen-accelerated-request.schema.json).
All placeholder paths and hashes need real bindings. Each `work_orders` entry is
an existing CPU work-order file's absolute path and SHA-256 of its actual bytes.
These are read-only input templates: their original output roots are never used.
All new plans/checkpoints live beneath a disjoint new `state_root`, whose parent
must already exist and be owned/private (0700). All orders must select the same
model artifacts. Duplicate recording identities or source paths are rejected.

Execution defaults: CPU, one model thread, batches of eight probes, two decoder
workers, and a one-hour finite run. Bounds are 1–2 model threads, 1–16 probes per
batch, 1–2 decoders and 10–86,400 seconds per run. Each original work order's
per-recording time/window limits still apply; its `threads` field is replaced by
the explicit resident execution thread setting. Use larger per-recording bounds
when preparing new orders to avoid unnecessary bounded pauses.

Use the selected environment's Python for **all** plan/run/status commands:

- CPU: `research/corpus/speaker-screen-runtime-20260912/venv/bin/python`.
- CUDA: `research/corpus/speaker-screen-accelerated-runtime-20260912/venv/bin/python`.

The manifest pins the Python executable/configuration, exact installed main
package versions, model recipe, implementation hashes, all input templates, and
CUDA driver version where applicable. CPU and CUDA have distinct recipes and
plan identities. Do not mix old CPU-v1, resident CPU, and resident CUDA
checkpoints, or edit source modules while a run is active.

```sh
"/ABSOLUTE/ISOLATED/venv/bin/python" -B pipeline/speaker_screen_accelerated.py plan \
  --request "/ABSOLUTE/PRIVATE/request.json" \
  --expected-sha256 "REPLACE_WITH_REQUEST_SHA256" \
  --output "/ABSOLUTE/PRIVATE/resident-state/manifest.json"

"/ABSOLUTE/ISOLATED/venv/bin/python" -B pipeline/speaker_screen_accelerated.py run \
  --manifest "/ABSOLUTE/PRIVATE/resident-state/manifest.json" \
  --expected-sha256 "REPLACE_WITH_MANIFEST_FILE_SHA256"
```

`plan` seals metadata only; it does not decode media or load models. `status`
takes the same arguments as `run` and replays private checkpoints without model
loading. While an active writer holds the workspace lock, status explicitly
returns `state: running`, `snapshot_deferred: true`, and `counts: null` rather
than reporting a potentially inconsistent checkpoint snapshot. Between finite
runs it returns verified counts and coverage. A completed replay starts no model
or decoder workers and does not create another run receipt.

## GPU memory and isolation

For CUDA set `execution.device` to `cuda` and `gpu_uuid` to the explicitly selected
NVIDIA UUID from `nvidia-smi --query-gpu=uuid --format=csv,noheader`. The default
Torch allocator budget is 50% of that GPU's reported memory (allowed 10–75%).
This is not a cap on all CUDA-driver allocations; the GPU is shared with display
and other applications. No exclusive reservation or ASR scheduling is implied.

CUDA requires a hard inherited cgroup-v2 aggregate memory ceiling of at most
4 GiB and swap disabled. A 4 GiB *virtual-address-space* limit is inappropriate
for CUDA's large virtual mappings. The separate wrapper creates a transient
user scope containing only this command and its children:

```sh
pipeline/bin/speaker-screen-accelerated-gpu \
  "/ABSOLUTE/ISOLATED/CUDA/venv/bin/python" run \
  --manifest "/ABSOLUTE/PRIVATE/resident-state/manifest.json" \
  --expected-sha256 "REPLACE_WITH_MANIFEST_FILE_SHA256"
```

It uses `systemd-run --user --scope --property=MemoryMax=4G
--property=MemorySwapMax=0`. CUDA fails closed if those effective inherited limits
cannot be verified. CPU model workers retain the existing 4 GiB address-space
limit; decoders retain 1 GiB limits. Models/decoders have finite CPU and wall-time
budgets, reduced priority, kernel IPv4/IPv6 socket denial and parent-death cleanup.
Models load only from verified local bytes with weights-only deserialization;
there is no remote YAML/model loader. The old environments are not upgraded.

The isolated CUDA environment uses verified official-index wheels, offline
hash-locked installation, Torch/TorchAudio 2.8.0+cu128, SpeechBrain 1.0.3,
NumPy 2.2.6 and CPU ONNX Runtime 1.22.1. Its private setup/provenance and pilot
reports are in `research/corpus/speaker-screen-accelerated-runtime-20260912`.

## Optional metadata-guided mode

The separate [guided screen](GUIDED_SPEAKER_SCREEN.md) adds title/date queue
priority and aligned timestamp probes while preserving baseline coverage. It
uses new manifests/checkpoints and does not modify this resident implementation.

## Measured local comparison (2026-09-12)

The matched pilot used the same sixteen ten-second probes across two retained
recordings, with five embedding-bearing excerpts. Both resident modes used one
model thread, two decoders, and fixed batches of four probes (two batches per
recording, to exercise decoding overlap).

| Path | Measured elapsed time |
| --- | ---: |
| Existing CPU-v1, two recording workers | 7.758 s |
| Resident CPU, one shared model worker | 6.434 s |
| Resident CUDA, one shared model worker | 5.732 s |

In this small test, resident CPU took about 17% less time than the existing
two-worker path; resident CUDA took about 26% less (1.35 times the throughput).
The additional GPU benefit over resident CPU was about 11% less elapsed time.
The sequential CPU-v1 baseline took 14.435 seconds, but comparing only to that
would overstate the improvement over the already available parallel CPU path.
These two recordings are not an archive-wide throughput or accuracy benchmark.

Every decoded PCM hash, VAD/excerpt boundary, source witness, classification,
reason flag, group membership and coverage value matched the original CPU result.
The five resident-CPU vectors were bit-identical; CUDA vectors differed by at
most about 5.8e-7 per coordinate, with minimum matched cosine similarity above
0.999999999997. This is observed consistency, not a guarantee that all CPU/GPU
classifications near a threshold will agree. Both sampled recordings remained
`uncertain`; the pilot is not evidence of accurate speaker counts.

A separate duplicate-excerpt control verified an actual four-item ECAPA forward
pass on `cuda:0`, with GPU events and forward hooks, and consistent singleton/
batch outputs. That synthetic repetition is not a corpus throughput claim.
CUDA's matched-run observed model-child peak RSS was about 1.43 GB and sampled
GPU usage about 222 MiB; neither is a maximum allocation guarantee. Completed
replays took under 0.08 seconds and launched no workers. All pilot workers exited.

For this host, a measured starting configuration is the CUDA example: one model
thread, two decoders, four-probe batches, and a 50% Torch allocator budget in the
separate 4 GiB/no-swap scope. Persistent CPU remains available without GPU work.
The built-in CPU/eight-probe defaults are conservative generic defaults, not a
claim of an optimally tuned batch size. No full-archive screening run was started.

## Checkpoints, stopping and interpretation

Each plan seals fixed probe-batch membership. A whole batch is validated and
published atomically as `batch-NNNN.json`; interruption before publication loses
only that uncommitted batch's computation. Resume uses the original groups,
avoiding changes caused by regrouping a partially saved CUDA batch. Checkpoints
contain private vectors; printed status and final results do not.

Every batch binds the source witness, exact probe coordinates, decoded PCM hashes,
normalized 192-dimensional vectors, runtime/model/device provenance and plan.
Sealed results must replay exactly. Source replacement, missing required evidence,
runtime drift and incompatible recipes fail closed; never edit checkpoints to
bypass a failure. The default source verification remains a metadata witness,
not a new full-source hash. Use exact audio-stream timing to prepare inputs;
[the CPU guide](CPU_SPEAKER_SCREEN.md#source-identity-and-timeline) explains why
rounded container durations can request a nonexistent final sample.

Each explicit run visits each unfinished recording at most once. Work bounds
may leave paused plans; rerun explicitly to continue. Errors stop that finite run
and retain already committed evidence. There is no unlimited retry or CPU/GPU
fallback. SIGINT/SIGTERM terminate/reap the model and all decoders. An unused
prefetched batch is discarded after a supported-positive early stop; it is not
counted as inspected coverage and cannot invalidate the committed decision.

`screening_decision_complete` is separate from a fully completed sampling plan:
early positive results retain paused sampling and explicit remaining coverage.
A sampled negative is never proof that the whole video has one speaker. All
categories are uncalibrated hypotheses needing review. Neither GPU acceleration
nor completed sampling means ASR completion, full diarization, or publication.

Exit 0 can mean a normal bounded pause; inspect the reported state/counts. Errors
return 2 and cancellation returns 130. A completed replay avoids repeated work,
but no full-archive scheduler or multi-batch daemon is supplied.

## Verification

```sh
python3 -m unittest \
  pipeline.tests.test_speaker_screen_accelerated_engine \
  pipeline.tests.test_speaker_screen_accelerated_worker \
  pipeline.tests.test_speaker_screen_accelerated \
  pipeline.tests.test_speaker_screen_accelerated_review
```

These are synthetic inference/metadata checks plus bounded local synthetic-WAV
decoding. Real model performance and accuracy are separate measurements; a tiny
pilot is not an archive-wide ETA or an accuracy calibration.
