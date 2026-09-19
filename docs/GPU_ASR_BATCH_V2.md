# GPU ASR batch v2

`production_asr_batch_v2.py` is the finite execution layer between sealed v5
work orders and private transcript results. It is deliberately separate from
acquisition, corpus import, search, publication, and the wiki.

## Execution contract

- A manifest contains 1–32 work orders of exactly one class: root-admitted
  private preprocess outputs, candidate-runtime local-private preprocess outputs,
  or purpose-built synthetic canaries. Mixing is rejected. The exact classes are
  `production_private_asr`, `local_private_production_asr`, and
  `synthetic_canary`.
- The trusted launcher authenticates root-owned host state, resolves the stable
  GPU UUID, checks the complete SquashFS digest, and creates a networkless
  Bubblewrap sandbox. The worker authenticates that launch attestation again.
- Admission has two sandbox phases. The GPU-less first phase deep-validates each
  preprocess queue/member projection (one typed queue replay per unique queue),
  or the exact synthetic fixture/case, and emits one bounded content-addressed
  lineage attestation. The launcher validates and descriptor-binds it into the
  second phase; `run` exact-replays it and the launch attestation.
- Every input is opened with `O_NOFOLLOW`, copied and hashed once into a bounded
  anonymous `memfd`, then sealed against write/grow/shrink/further-seal changes.
  Authenticated PyAV 18.1.0 and inference read only that sealed memfd. PyAV
  enforces one FLAC audio stream, mono/s16/16 kHz metadata, and exact duration
  before all source descriptors are closed and the nonblocking GPU UUID lock is
  requested. The V5 result keeps the legacy phase name `preflight_ffprobe` for
  schema compatibility; its measured implementation is PyAV, with engine
  evidence in the attempt journal.
- One `WhisperModel` is created with `num_workers=2`. At most two unchanged
  `transcribe` calls consume iterators concurrently. This is model-worker
  concurrency, not neural batching. VAD remains disabled.
- A pair is fully computed and validated before either member is published.
  A `pair-ready` transaction record binds both result hashes, then successful
  members publish in ordinal order using Linux `renameat2` with
  `RENAME_NOREPLACE`; a pair computation failure publishes neither member. A
  crash between the two filesystem commits is recovered by exact replay of the
  first member and a singleton retry of its fixed ordinal pair, never by replace.
- Exact v5 result, artifact identity, and raw-to-normalized replay is required
  after every publication and before an existing result is reused on restart.
- The event journal is a bounded set of immutable per-attempt documents. It
  records setup, iterator consumption, transcript normalization, serialization,
  publication, pair wall time, failures, and final telemetry.
- Process VRAM is guarded at 20 Hz. Utilization, memory-controller utilization,
  temperature, power, clocks, throttle state, and estimated energy are retained
  as bounded 4 Hz histograms. Stale/error/over-limit telemetry fails closed.
- Result, event, and UUID-lock mutations are anchored to retained root
  descriptors. Every component link is replayed immediately around commits;
  no recursive/path-based cleanup is used. Concurrent accidental parent moves
  fail closed. As elsewhere in this private lane, a malicious process with the
  same host UID is outside the confidentiality/publication threat boundary.

## Operator flow

Create a finite manifest outside the sandbox:

```sh
pipeline/bin/asr-faster-whisper-gpu-batch-v2 materialize \
  --work-order /absolute/hot/path/work-order-1.json \
  --work-order /absolute/hot/path/work-order-2.json \
  --production-profile /absolute/hot/path/production-profile-v2.json \
  --expected-production-profile-sha256 SHA256 \
  --batch-root /absolute/hot/path/gpu-batches \
  --event-root /absolute/hot/path/gpu-events \
  --lock-root /absolute/hot/path/gpu-locks
```

Submit the resulting manifest through the root-owned trusted launcher. Do not
invoke `run` directly: the required launch attestation is created only after the
launcher completes host trust, execution-image, phase-one lineage, GPU, resource
envelope, exact-file binding, and namespace checks. In particular, production
manifests are reviewed/root-owned mode `0444`; input and lineage descriptors are
retained before either sandbox starts.

For the unprivileged lane, materialize with
`--execution-mode local-private-production`, then execute the current-user
mode-`0500` launcher with `--mode local-private-production` and the doctor-issued
readiness receipt. That lane still enforces the full resource envelope and all
isolation/input-hash rules, but its attestation explicitly records that another
same-UID process can mutate user-owned controls. It is not an admitted-runtime or
root-production claim.

The launcher calls these worker interfaces internally:

```text
preflight-lineage --batch-manifest PATH --expected-batch-sha256 SHA256
  --production-profile PATH --expected-production-profile-sha256 SHA256
  --root-registration PATH --expected-root-registration-sha256 SHA256
  --lineage-attestation-output PATH

run --batch-manifest PATH --expected-batch-sha256 SHA256
  --runtime-admission PATH --expected-runtime-admission-sha256 SHA256
  --production-profile PATH --expected-production-profile-sha256 SHA256
  --root-registration PATH --expected-root-registration-sha256 SHA256
  --lineage-preflight-attestation PATH
  --expected-lineage-preflight-attestation-sha256 SHA256
  --launch-attestation PATH --expected-launch-attestation-sha256 SHA256
```

The execution image does not package or map a shell wrapper or host `ffprobe`;
the launcher invokes its authenticated Python and worker source directly.
`CUDA_VISIBLE_DEVICES` is the admitted stable UUID, while CUDA/NVML sees that
device as visible index zero. Before model construction the worker rejects
foreign compute-only PIDs and insufficient free VRAM; ordinary desktop graphics
contexts that also appear in NVML's compute list are measured, not misclassified.

`status` performs read-only exact replay. A completed machine transcript remains
private and unreviewed; moving it into the searchable corpus is a separate human
reviewed operation.

## Admission

Passing unit tests or a short canary does not make this lane production-admitted.
The immutable execution-image/profile tuple must pass every accuracy, semantic,
30-minute, 2-hour, 8-hour thermal, 32-item, scheduler, crash-recovery, and launcher
isolation gate. Any source, dependency, image, profile, model, driver-minimum, or
gate change creates a new candidate identity.
