# GPU ASR throughput and production-admission plan

- Audit date: 2026-08-29
- Hardware observed: NVIDIA GeForce RTX 3050, 6 GiB VRAM, compute capability 8.6;
  AMD Ryzen 7 3700X, 8 cores/16 threads; 31 GiB RAM
- Scope: private, offline ASR execution on the hot tier
- Status: design and admission gates; this document is not execution authority

No inference, media read, catalogue mutation, or archive operation was performed for
this audit. The admitted v3 worker and its receipts remain immutable historical
evidence. Throughput changes belong in a successor contract and require new
admission evidence.

Exact model, runtime, profile, and input identities are run-integrity metadata, not
a policy of freezing the technology. A better candidate may replace the current
one whenever a recording-disjoint evaluation shows a better accuracy/efficiency
frontier. Byte-identical rebuilds are useful for detecting packaging mistakes but
are not an optimization objective across candidate generations. Promotion should
be based on reviewed word/name/term error, hallucination and timing failures,
GPU-hours, peak resources, and failure recovery—not release recency alone.

This plan therefore separates **run integrity** from **technology selection**.
Hashes say what produced a result and make a bad run diagnosable; they do not make
that model, runtime, or decoding recipe permanent. Every successor is scored from
scratch and may replace the incumbent as soon as it wins the reviewed frontier.

## Decision

The execution-path optimization is now implemented: one resident model serves a
finite batch from a minimized read-only bundle, with one image integrity pass and
no wheelhouse replay in the inference path. The immediate objective is to start a
private, resumable backfill with the current `small.en` FP16/beam-5 control on inputs
that already meet its limits. Model/decoder bake-offs, long-form chunk expansion,
and transcript post-processing can proceed after that lane is operating. Successor
candidates still use the same held-out references before promotion. Byte equality is
a sensitive infrastructure diagnostic, never a substitute for accuracy and never a
cross-generation promotion requirement.

The production scheduler should use these initial bounds:

| Resource or queue | Initial bound |
| --- | ---: |
| GPU inference processes | 1 |
| CTranslate2 workers in one resident model | 2 |
| Concurrent unchanged per-item transcriptions | 2 |
| Pending input preflight workers | 0 (v2 does not implement prefetch) |
| Input prefetch depth | 0 (benchmark for a successor) |
| Ready GPU batches | 2 |
| Items per sealed batch | 32 maximum |
| Preferred audio per batch | 2 hours or 32 items, whichever comes first |
| Minimum non-urgent audio per batch | 30 minutes |
| Process VRAM ceiling | 4 GiB until a successor benchmark raises it |
| Free VRAM before load | 2 GiB minimum |
| Other CUDA jobs | 0 |

These are conservative launch values, not claims that every larger value is unsafe.
An adaptive profile may replace them only after the matrix below supplies local
evidence.

### Start-now implementation order

The immediate path deliberately separates launch blockers from improvements that can
land while the backfill is running:

1. Make the current control executable through a bounded private local-production
   profile, run readiness checks and synthetic canaries, then start only already
   `ready` queue members. Outputs remain private machine transcripts with no automatic
   import or publication authority.
2. Add timestamp-preserving overlapping chunks for inputs above the current
   420-second/8-MiB item limit, plus boundary reconciliation back into original
   media coordinates. Those long inputs wait; they do not block shorter ready work.
3. Complete and adjudicate the recording-disjoint reference set, then screen model
   generations and decoder choices using the staged matrix below.
4. After choosing a model/precision/beam family, optimize mixed-duration packing,
   bounded decode prefetch, VAD, neural batch size, and hotwords one family at a time.
5. Feed the paired accuracy report and worker-derived GPU measurements into the
   root-admitted successor. Exact transcript equality remains an infrastructure
   diagnostic only.

The current hard-coded profile is intentionally the control, not the generic future
profile. Candidate contracts must express engine/model, precision, decoding, VAD,
language routing, term prompting, and resource bounds without weakening the admitted
control in place.

## Measured baseline and bottleneck

The historical two-item pilot used `Systran/faster-whisper-small.en`, FP16, beam 5,
temperature 0, word timestamps on, VAD off, previous-text conditioning off, four
CPU threads, and one worker.

| Observation | Value |
| --- | ---: |
| Two-item audio duration | 793.913 s |
| Two-item end-to-end time | 45.190 s |
| CUDA inference time | 19.290 s |
| Model load time | 0.544 s |
| End-to-end throughput | 17.57 times real time |
| Inference throughput | 41.16 times real time |
| Process peak VRAM | 805,306,368 bytes |
| Non-inference/non-load time | about 25.36 s |

Only about 42.7% of that pilot wall time was inference. The model is small relative
to the card, but the current admission replay is large relative to the job:

- the installed runtime tree is about 2.7 GiB;
- the build wheelhouse is about 1.5 GiB;
- live receipt replay rebuilds admission evidence and makes multiple complete passes
  over both trees; and
- the resident batch repeats live common-binding replay at the beginning and end.

This can amount to roughly 30 GiB of integrity reads per batch. The wheelhouse is
build evidence, not an execution input. Re-reading it on every inference run spends
I/O and CPU without improving the integrity of bytes actually executed.

On 2026-08-29, the final deterministic image candidate was benchmarked with the
unchanged FP16/beam-5/word-timestamp path and four CPU threads. On four repeated
59.448-second synthetic inputs, one CTranslate2 worker reached 44.28 times real
time, two reached 57.64 times real time, and four reached 59.83 times real time.
Two workers therefore improved aggregate throughput by 30.2% over one, whereas
four improved only 3.8% over two. The two-worker run peaked at 840 MiB of process
VRAM with 4,390.6 MiB minimum device-free memory; four workers peaked at 1,048 MiB
with 4,182.6 MiB free. Every output in the comparison had the same text hash.

The same conclusion held for the 5.945-second fixture: 25.66, 31.88, and 33.88
times real time for one, two, and four workers respectively. The production
candidate consequently binds one resident model, two CTranslate2 workers, and two
ordinary concurrent `transcribe` calls. The exact diagnostic evidence is retained
beside candidate image identity
`27a6dd32f713868435e72dcc974d2284aac675629d7e502de2a6ccaf1cf7af24`.
These synthetic observations select the admission candidate; they do not replace
the recording-disjoint accuracy, packed-batch, or thermal gates.

The minimized candidate contains 2,572 entries and 1,708,578,555 logical bytes,
compressed into a 1,120,018,432-byte SquashFS. Its execution closure excludes the
wheelhouse and unused cuDNN/NVRTC libraries, so ordinary launch integrity work is
one image pass rather than repeated scans over mutable runtime and build trees.

At the measured inference RTF, a full batch containing about three hours of admitted
audio would project to roughly 37.5 times real time even if the fixed 25.36-second
overhead remained. A two-hour batch projects to about 35.9 times real time. These
are arithmetic projections from the two-item pilot, not capacity results; the soak
must measure them.

## Production execution bundle

Create a successor that executes only from one externally authenticated, read-only
bundle containing the interpreter, installed packages, CUDA libraries, model,
adapter, worker, PyAV, and a canonical tree manifest. Suitable implementations
are a verified EROFS/SquashFS image, fs-verity-protected files with a sealed root
manifest, or an equivalent immutable mount whose digest is checked by a launcher
before any bundled code executes.

The concrete external boundary and root-owned staging flow are documented in
[`GPU_TRUSTED_LAUNCHER_V2.md`](GPU_TRUSTED_LAUNCHER_V2.md). That launcher performs
one streaming full-image hash before mounting, then uses a compact sealed
attestation so the worker does not repeat multi-gigabyte runtime/model scans.

The launcher must:

1. be outside the bundle and independently pinned;
2. authenticate the image/root manifest before executing its Python or shell code;
3. enter the no-network namespace and closed Bubblewrap mount profile;
4. resolve the current hot-tier root and enforce live same-filesystem relationships
   without persisting Linux `st_dev` as durable identity;
5. expose only the exact read-only bundle, sealed work orders and inputs, GPU device
   nodes, one UUID lock, and explicit writable result/receipt roots; and
6. record the launcher, bundle, root-registry, namespace, GPU UUID, driver, and
   writable-root bindings in the completion receipt.

Admission should retain the wheelhouse digest and package-to-wheel proof, but the
wheelhouse should not be mounted or traversed by ordinary execution. A scheduled
offline audit can fully rebuild and hash the environment. Per-run replay should
verify the authenticated bundle/root digest plus exact small manifests, not rescan
gigabytes of already immutable bytes.

### Deterministic SquashFS candidate builder

`pipeline/bin/build-gpu-execution-image` now implements the candidate image and
receipt boundary. It has not built the real runtime/model image and grants no mount
or execution authority. Its canonical source specification must explicitly list
every directory and regular file; directories are entries, not recursive inclusion
shortcuts. Every parent image directory must also be listed. Each entry supplies:

- `kind`: `directory` or `regular_file`;
- `source_path`: one normalized absolute hot-tier/system path;
- `image_relative_path`: a traversal-free portable image path; and
- `image_mode`: an admitted read-only file/directory mode.

The specification also binds a deterministic `source_epoch`, an intended host mount
policy path, the private/no-network/no-wheelhouse policy, and bounded logical mapping
rows shaped as:

```json
{
  "name": "python_executable",
  "image_relative_path": "runtime/bin/python",
  "sandbox_path": "/opt/himr-gpu/runtime/bin/python",
  "role": "executable"
}
```

Mapping names, image paths, and sandbox paths must each be unique. The generic
builder does not decide which names a production ASR contract requires; the runtime
admission successor must require the full closed set it consumes.

Build only into existing current-user mode-0700 directories on the explicitly
supplied Btrfs UUID:

```sh
builder=pipeline/bin/build-gpu-execution-image
spec=/absolute/private/execution-image-spec.json
spec_sha256=REVIEWED_CANONICAL_SPEC_SHA256

"$builder" build \
  --spec "$spec" \
  --expected-spec-sha256 "$spec_sha256" \
  --image /absolute/private/images/gpu-runtime.squashfs \
  --receipt /absolute/private/receipts/gpu-runtime.json \
  --filesystem-uuid REVIEWED_BTRFS_UUID \
  --maximum-wall-seconds 7200

"$builder" validate \
  --receipt /absolute/private/receipts/gpu-runtime.json \
  --expected-receipt-sha256 REVIEWED_RECEIPT_SHA256
```

The builder rejects archive paths before filesystem access, symlink components,
special files, hardlinked source files, unlisted parents, duplicate mappings,
unsafe source ownership/modes, and replacement of either output. It copies each
retained source descriptor while performing the full SHA-256 audit, builds through
the exact root-owned `/usr/bin/mksquashfs`, and binds the Btrfs FSID measured through
the retained-descriptor `portable_root` ioctl. SquashFS settings are fixed to four
processors, zstd level 9, 128 KiB blocks, all-root ownership, a canonical sort,
explicit creation/file/root timestamps, and no append, xattrs, exports, recovery
file, or hardlinks.

The canonical receipt includes the complete source projection/tree identity, image
path/SHA-256/size, Btrfs UUID, builder and `portable_root` source hashes,
`mksquashfs` hash/version, deterministic settings, intended mount path, and every
logical mapping. The public import boundary is:

```python
load_receipt(
    path,
    expected_sha256=None,
    verify_image=True,
    expected_image_uid=None,
)
```

Candidate replay requires current-user image ownership by default. The image is
mode 0444 inside a current-user mode-0700 parent, so it remains private during
staging. After a separately reviewed transition, production uses a root-owned mode
0444 image under root-owned non-writable ancestors: the unprivileged systemd-user
launcher can authenticate and mount it but cannot mutate it. The launcher must pass
`expected_image_uid=0`. `verify_image=False` is suitable only when an externally
trusted launch attestation has already bound the launcher-verified image digest; it
must not become a generic performance shortcut. Receipt replay never traverses the
source runtime, model, or wheelhouse and never invokes CUDA.

## Efficient finite-batch lifecycle

Keep batches deterministic, finite, content-addressed, and restartable:

1. Materialize only mutually compatible work orders.
2. Validate and probe every pending input before reserving the GPU. Retain a
   no-follow descriptor through inference, or rely on an admitted fs-verity input
   seal while retaining the descriptor for race checks.
3. Acquire the UUID lock only after the batch is ready and the GPU idle/memory gate
   passes.
4. Load one model and process members in sealed ordinal order.
5. Use one bounded preflight thread to prepare the next descriptor while the current
   member is inferred. Prefetch performs no inference and makes no writes. A failed
   current member discards later preflight state.
6. Publish and exact-replay each ordinary member result before starting inference on
   the next member. This preserves the present fail-stop boundary.
7. Release the model and GPU lock, perform a cheap authenticated-bundle drift check,
   then publish the batch completion receipt.

Do not create an unbounded daemon queue inside the CUDA worker. Two ready batches
are enough to hide materialization latency, while finite manifests preserve replay,
operator control, and bounded failure recovery.

## Instrumentation required before tuning

The successor receipt should include monotonic spans for manifest replay, immutable
bundle verification, input hash/open, PyAV probe/decode, feature preparation,
iterator/GPU decode, normalization, serialization, publication, exact result replay,
model load/unload, and completion sealing. Preserve total wall and audio duration so
each span can be converted to RTF.

Extend the bounded NVML sampler with:

- sample count and interval;
- process and global VRAM peak;
- GPU-utilization mean, p50, p95, and active-time fraction;
- memory-controller utilization mean and p95;
- power mean/peak and energy estimate;
- maximum temperature;
- SM-clock p05/mean; and
- observed thermal, power, or idle throttling reasons where NVML supports them.

Measure setup separately from generator consumption. In pinned faster-whisper,
`WhisperModel.transcribe` performs decode/feature setup before the returned segment
iterator performs most model work; one combined `inference_seconds` value cannot
show whether CPU decode or CUDA decoding is the bottleneck.

## Optimization matrix

Run one variable family at a time on the same recording-disjoint benchmark. Always
retain the current FP16/beam-5, two-worker resident profile as the control.

### Model generations to screen

Use a short viability pass to avoid spending the full reviewed benchmark on weak
or non-fitting candidates. The priority order for the 6 GiB RTX 3050 is:

| Priority | Candidate | Initial configuration | Purpose |
| ---: | --- | --- | --- |
| control | `Systran/faster-whisper-small.en` | FP16, beam 5 | Current measured baseline |
| 1 | `distil-whisper/distil-large-v3.5` | FP16, beam 5, English | Leading English accuracy/throughput challenger |
| 2 | `openai/whisper-large-v3-turbo` | FP16, beam 5, English | Leading long-form quality/efficiency challenger |
| 3 | `openai/whisper-large-v3` | `int8_float16`, beam 5 | Accuracy ceiling and possible selective second pass |
| 4 | `nvidia/parakeet-tdt-0.6b-v2` | native greedy decoder, 30–60 s chunks | Non-Whisper English challenger behind a separate adapter |

The pinned faster-whisper release recognizes Distil-Large-v3.5 and Turbo directly.
Parakeet must live in a separate NeMo/PyTorch image behind an engine-neutral result
contract; do not add PyTorch/NeMo to the small CTranslate2 execution image. Normalize
its text and timing into the comparison schema, while retaining engine-native scores
as uncalibrated diagnostics rather than pretending they are comparable to Whisper
scores.

For each candidate, run one warm-up plus a five-minute mixed-condition viability
cell at batch one. Reject it early on load/OOM, invalid or missing word times,
critical omissions, obvious silence hallucination, or throughput below the incumbent
without a material accuracy gain. Only survivors receive the full reference pass.
Start with FP16; try `int8_float16` when it improves fit or batch capacity, then keep
it only if the paired accuracy result passes. On the two best Whisper survivors,
compare beam 1 and beam 5; add beam 3 only when those endpoints reveal a useful
frontier. Sweep neural batch sizes only after selecting model, precision, and beam.
Stop before OOM or sustained process allocation above about 5.2 GiB so the desktop
and driver retain a practical reserve.

Evaluate a small reviewed HIMRverse hotword list separately after the base model is
chosen. Score both entity recall and false insertion; do not let a prompt-assisted
name improvement hide ordinary-word regressions or confirmation-shaped text.

### Infrastructure-only candidates

1. Immutable-bundle replay plus full 32-item resident batch.
2. Preflight before GPU reservation.
3. One-pair bounded float32 decode prefetch during current inference.
4. CPU thread counts 2, 4, 6, and 8.

These candidates are expected to leave transcript content unchanged when the model
and decoder are unchanged, so byte equality is a useful diagnostic. Still score
them. A difference is investigated and classified; it is rejected when it degrades
reviewed content, timing, or critical-error gates, not merely because bytes differ.

### Output-changing candidates

1. Beam sizes 1, 3, and 5.
2. FP16 versus `int8_float16`.
3. `BatchedInferencePipeline` batch sizes 2, 4, 6, and 8.
4. One versus two CTranslate2 workers with separate-file concurrent calls.
5. VAD-off control versus pinned, explicitly parameterized VAD.

Do not combine neural batching and multiple model workers in the first grid. The
pinned batched pipeline requires VAD or explicit clips for audio at least 30 seconds
long and uses a different chunk/timestamp path. It therefore needs a new transcript
contract, not a runtime flag on v3. VAD must retain original media coordinates and
be evaluated for clipped interjections, distant speech, music/playback, and silence
hallucination. `best_of` is inactive at temperature 0 in the pinned implementation,
so changing it alone is not a throughput experiment.

Select a Pareto profile, not merely the fastest cell. Accuracy has lexical priority:
first discard candidates outside the reviewed non-inferiority bounds or with new
critical failures, then choose the most efficient member of the surviving accuracy
tier. Do not collapse WER and speed into one scalar because a large throughput gain
must not compensate for invented names or omitted consequential speech.

## Benchmark and canary gates

### Dataset

Use a sealed, recording-disjoint set with reviewed references and at least:

- 60 minutes of accuracy audio across clean, noisy, accented, distant, playback,
  music, and silence-heavy speech;
- short clips, ordinary 5–15 minute videos, and at least one 30-minute stream;
- HIMRverse term/name coverage without prompting the reference answer; and
- explicit speech/non-speech boundary marks for the VAD candidates.

The current admitted benchmark has one 5.945-second case and one repetition. It is a
runtime smoke, not sufficient production or accuracy evidence.

### Staged runs

1. Three repetitions of each short matrix cell after one discarded warm-up.
2. A 30-minute mixed-duration canary.
3. A two-hour packed-batch canary.
4. An eight-hour thermal soak with at least one batch-boundary restart.

Randomize matrix order or interleave the control to reduce thermal/order bias. Report
median and p95, not only the best run.

### Acceptance thresholds

- zero failed or missing members;
- no reviewed content/timing regression for infrastructure-only candidates; exact
  equality is recorded as a strong diagnostic when observed, not required by policy;
- a paired 95% confidence interval whose upper regression bound stays within the
  preregistered WER non-inferiority margin for an efficiency promotion (initial
  margin: 0.5 percentage point absolute and 3% relative, requiring both); a candidate
  outside that margin must show a separately reviewed material accuracy improvement
  to remain on the frontier;
- HIMRverse term recall no worse than the control by more than the same reviewed
  practical bound;
- no increase in silence hallucination or speech-clipping rate;
- p95 process VRAM below the admitted ceiling with at least 1 GiB physical reserve;
- no OOM, thermal throttle, deadline exit, partial publication, or completion/member
  mismatch;
- temperature and clock stability across the eight-hour soak;
- successful resume and validation after a clean restart; and
- crash canaries before model load, during each member phase, after member
  publication, and before completion publication, with no duplicate inference
  authority or overwritten result.

An initial throughput target for the infrastructure-only successor is at least 30
times real time end to end on a packed two-hour batch and at least 70% mean GPU
utilization during the measured model-iterator spans. These targets are deliberately
below the 41.16-times inference ceiling measured by the pilot. Revise them only from
local evidence.

## CPU/GPU overlap and host policy

Acquisition and one bounded preprocessing job may overlap CUDA only when the
scheduler has separate CPU, RAM, and hot-tier I/O tokens. Start with four CPU threads
reserved for GPU ASR and at most four for the overlapping job. Pause new hot-tier
bulk reads if GPU iterator utilization falls or input-ready latency rises.

Before model load, sample the GPU long enough to reject an unrelated CUDA workload,
not merely low free memory. Desktop graphics may remain, but the receipt should
record external process-memory totals, baseline utilization, temperature, and free
VRAM. The launch gate should require free VRAM to exceed the admitted p95 process
peak plus a safety margin and at least 1 GiB reserve; checking only a fixed free-byte
floor independently of the selected profile can admit an impossible combination.
Do not alter clocks, overclock, or enable a privileged persistence setting as part
of ordinary pipeline execution; power-limit experiments belong in a separate
operator-reviewed benchmark.

## Production-ready definition

GPU execution is production-ready only when all of the following are true:

- the restart-portable root contract and pre-execution trust anchor are admitted;
- the ASR-ready receipt-to-work-order/batch builder is versioned, resumable, and
  rejects duplicate or mismatched work;
- the operator launches only closed, owner-reviewed profiles through whole-process-
  tree supervision with bounded cancellation;
- the 30-minute, two-hour, and eight-hour gates pass;
- admission records the selected model/runtime/inference/worker/launcher identities
  for run diagnosis and rollback without freezing future candidates;
- completion receipts expose enough timing and NVML evidence to detect regressions;
  and
- one rollback profile retains the current FP16/beam-5 behavior.

Until then, a successful GPU call remains a bounded pilot rather than authority for a
corpus-wide unattended backfill.
