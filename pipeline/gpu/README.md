# Isolated RTX 3050 GPU runtime

This directory defines the bounded private CUDA lane used to prove local RTX 3050
access before any corpus GPU backfill. It does not replace the existing CPU-first
preprocessing or whisper.cpp contracts and does not authorize rerunning completed
ASR. The runtime produces machine-unreviewed private output with no identity, wiki,
catalogue, or publication authority.

## Environment

Use the tracked `pyproject.toml` and `uv.lock` with CPython 3.12.14. Keep the managed
interpreter, virtual environment, wheel cache, model cache, and results under an
ignored main-drive path such as `research/corpus/gpu-runtime/`. Do not create the
environment inside the repository's current Python environment and do not place any
of it on cold storage.

```sh
UV=/absolute/private/bootstrap/venv/bin/uv
GPU_ROOT=/absolute/main-drive/research/corpus/gpu-runtime

UV_PYTHON_INSTALL_DIR="$GPU_ROOT/python" \
UV_CACHE_DIR="$GPU_ROOT/uv-cache" \
  "$UV" python install 3.12.14
```

The lock pins CPython 3.12.14 and all Python packages and artifact hashes. Critical
direct pins are faster-whisper 1.2.1, CTranslate2 4.8.1, cuBLAS 12.9.2.10, cuDNN
9.24.0.43, and nvidia-ml-py 13.610.43. The final ASR execution-image projection
explicitly excludes both cuDNN and NVRTC because the measured CTranslate2 control
does not load them; retaining them in the broader build-environment lock does not
make them executable dependencies. The local driver must expose at least one CUDA
device to CTranslate2. CUDA libraries are loaded from the locked wheels rather than
an ambient toolkit; a host `nvcc` installation is not required.

Use the exact managed interpreter when synchronizing. `--locked` rejects a stale
lock, while `--offline` prevents dependency resolution during replay:

```sh
UV_PROJECT_ENVIRONMENT="$GPU_ROOT/env" \
UV_PYTHON_INSTALL_DIR="$GPU_ROOT/python" \
UV_CACHE_DIR="$GPU_ROOT/uv-cache" \
  "$UV" sync --project /absolute/repository/pipeline/gpu --locked --offline \
  --no-dev \
  --python "$GPU_ROOT/python/cpython-3.12.14-linux-x86_64-gnu/bin/python3.12"
```

## Model acquisition and sealing

Acquire a public model only in a separate, credential-free step at a full upstream
commit. The first synthetic smoke uses `Systran/faster-whisper-tiny.en` at commit
`0d3d19a32d3338f10357c0889762bd8d64bbdeba`. Keep the model outside Git, then seal
every snapshot byte and emit a private manifest:

```sh
research/corpus/gpu-runtime/env/bin/python pipeline/gpu/seal_model_snapshot.py \
  --model-root /absolute/private/model/snapshot \
  --repository Systran/faster-whisper-tiny.en \
  --revision 0d3d19a32d3338f10357c0889762bd8d64bbdeba \
  --license mit \
  --sealed-at 2026-08-28T23:00:00Z \
  --output /absolute/private/model-manifest.json
```

The sealer rejects symlinks, non-regular or multiply linked files, seals files mode
`0400` and directories mode `0500`, rehashes them, and atomically publishes its
manifest without replacement. Review model terms independently before any non-smoke
use. The generic sealer records operator-supplied upstream provenance; a production
model admission still needs a separate credential-free downloader and license receipt.

## Bounded smoke

Use a sealed synthetic-fixture manifest for 16 kHz mono FLAC of no more than 30
seconds and 4 MiB. Arbitrary audio is rejected, so the result cannot silently label
corpus media as synthetic. Pass every mutable input by absolute path and expected
digest. `smoke.py` replays the fixture, sealed model, source, environment lock,
interpreter, FFprobe, bubblewrap, CUDA device, NVML identity, and VRAM use before and
after inference. It rejects non-finite limits, enforces a hard process deadline, uses
Python isolated mode, and atomically publishes its result mode `0400` without
replacement.

Run the process in a fresh bubblewrap network namespace. The repository and runtime
root are read-only except for the explicit result directory; only the four NVIDIA
device nodes needed by this lane are rebound. The smoke verifies that its network
namespace differs from the parent and that `/proc/net/dev` exposes no non-loopback
interface. Hugging Face offline variables remain defense in depth, not the basis for
the network-isolation claim.

```sh
REPO_ROOT=/absolute/repository
GPU_ROOT="$REPO_ROOT/research/corpus/gpu-runtime"
RESULT_DIR="$GPU_ROOT/results"
PARENT_NET_NS=$(readlink /proc/self/ns/net)

bwrap --die-with-parent --new-session --unshare-net --ro-bind / / \
  --dev /dev \
  --dev-bind /dev/nvidia0 /dev/nvidia0 \
  --dev-bind /dev/nvidiactl /dev/nvidiactl \
  --dev-bind /dev/nvidia-uvm /dev/nvidia-uvm \
  --dev-bind /dev/nvidia-uvm-tools /dev/nvidia-uvm-tools \
  --proc /proc --tmpfs /tmp --bind "$RESULT_DIR" "$RESULT_DIR" \
  --clearenv --setenv PATH /usr/bin:/bin --setenv HOME /nonexistent \
  --setenv LANG C.UTF-8 \
  "$GPU_ROOT/env/bin/python" -I "$REPO_ROOT/pipeline/gpu/smoke.py" \
  --audio /absolute/private/synthetic.flac \
  --expected-audio-sha256 SHA256 \
  --fixture-manifest /absolute/private/fixture-manifest.json \
  --expected-fixture-manifest-sha256 SHA256 \
  --model-manifest /absolute/private/model-manifest.json \
  --expected-model-manifest-sha256 SHA256 \
  --pyproject "$REPO_ROOT/pipeline/gpu/pyproject.toml" \
  --expected-pyproject-sha256 SHA256 \
  --lock "$REPO_ROOT/pipeline/gpu/uv.lock" \
  --expected-lock-sha256 SHA256 \
  --ffprobe /usr/bin/ffprobe --expected-ffprobe-sha256 SHA256 \
  --expected-smoke-source-sha256 SHA256 \
  --expected-python-executable-sha256 SHA256 \
  --sandbox-executable /usr/bin/bwrap \
  --expected-sandbox-executable-sha256 SHA256 \
  --expected-parent-network-namespace "$PARENT_NET_NS" \
  --runtime-root "$GPU_ROOT" --expected-runtime-device DEVICE_NUMBER \
  --expected-gpu-uuid GPU_UUID --min-free-vram-bytes 1073741824 \
  --max-audio-seconds 10 --max-audio-bytes 4194304 \
  --max-wall-seconds 60 --output "$RESULT_DIR/new-result.json"
```

Success proves only that this exact local runtime can execute bounded FP16
faster-whisper inference. It does not establish transcript accuracy, calibration,
speaker identity, biometric fitness, corpus-wide scheduling safety, or publication
fitness. An RTX 3050 has only 6 GiB VRAM; later model profiles must be separately
benchmarked and may need quantization or deterministic windows.

## Accepted local smoke profile — 2026-08-28

The accepted schema-v2 private receipt is
`research/corpus/gpu-runtime/results/smoke-20260828T2322Z.json`, physical SHA-256
`6ebdcef91298fb15048df0e18d5ce0cca48fd67054047617292756a1c25c7dd8`, identity
SHA-256 `47c1ae977d14068d03247fd89c6664a643ec9f791736dcdff2c9186a5eecfc26`.
It binds pyproject SHA-256
`7134738cdc056320f2d66881da2969379dc27dfe1eb57d2532a84ca8b092b181`, lock
SHA-256 `5668d413c971f0d7b989151bfab692c77cf4f6416a8e44c4aae5345e97217ae6`,
and smoke-source SHA-256
`37313a7284f92c679e23bc02c3961e3f14a28dd1b14c9348df9fe7beb168b28d`.
The earlier schema-v1 receipt is retained as superseded audit history because its
environment-only network field was too broad; it is not the accepted isolation
receipt.

## Accepted production profile — 2026-08-29

The admitted production inference profile is `Systran/faster-whisper-small.en` at
exact commit `d1d751a5f8271d482d14ca55d9e2deeebbae577f`, FP16, English,
beam/best-of 5, temperature 0, word timestamps enabled, VAD disabled, previous-text
conditioning disabled, four CPU threads, and one model worker. The model admission
identity is
`d1fb86a9ba89db472106c9f171aaf96a29262b7bdabdc7f04804cf65c581c143`.
The isolated runtime and wheelhouse tree identities are respectively
`3897fa135da5fde2e5a06789a845497fd27982b0ac918e1f5a85e1464a9547fa`
and `4067da1331123b030e0bc27a55ce1522e32025e8633140d287470f166605b70d`.

Only one job may hold the UUID-keyed GPU lock. A work order may reserve no more than
4 GiB of process VRAM and must observe at least 2 GiB free before corpus inference.
The current single-item profile has a 900-second wall bound and a 336-second input
bound. CPU acquisition, hashing, and one bounded four-thread preprocess job may run
beside GPU ASR, provided they do not mutate pinned inputs or overload main-drive I/O.
Do not run a second CUDA workload concurrently on the 6 GiB card until that exact
combination has a separate admission.

The v1 runtime receipt is
`research/corpus/gpu-runtime/production-runtime-admissions/receipt-production-v1.json`
(physical SHA-256
`8618dc77c9d1e163321aacb5310f777c4c83dfa2a01a50640c151f1567db1d07`).
It remains the exact replay authority for the first sealed corpus result only.

The first accepted media-local contract was the v2 wrapper
`pipeline/gpu/production_asr_v2.py` (SHA-256
`f95410aa74095347d01c9757c9daf14b61dc7a30e634145db18c85e0cc51e9c9`).
It loads the preserved v1 implementation at exact SHA-256
`bb5a2557e693cea1daeaefa339b1f3aca2634f124ea72e2f29910331c5ba816c`,
rejects nonzero producer timeline offsets, and emits only `media_ms`. Recording
projection remains a separately reviewed operation. Its work-order/result contract
identities are
`901147b6142e0e4eb47679a34bf87cd11866afef4adbffc3e2c66feab707bf83`
and `11ad607d8a9ddd3cac5558f4eae1fd4906bfd02cbcc7313d4d9a4937e1948bb1`.
The accepted v2 runtime receipt is
`research/corpus/gpu-runtime/production-runtime-admissions/receipt-media-local-v2-source-bound.json`
(physical SHA-256
`a32995dcfc3c74e1e980f84e5d79bd59923332d752536348e99ba5e0b4e04fab`,
identity
`95bfc6f951cd5b281aaa3059272f4d45602f7f69f87988e5618f14dfe1bf8c39`).

The v2 admission reuses the exact sealed inference benchmark because v2 changes only
post-inference coordinate normalization; it does not claim a second benchmark run.
The benchmark still binds the same model, inference dictionary, runtime, wheelhouse,
GPU, driver, RTF, and VRAM observations, while its v2 evidence binds the new contract
identities. Fresh lock contention/crash-release evidence binds both the v2 wrapper
and preserved implementation. Changing either source, any runtime/model byte, an
inference parameter, or a resource bound requires new evidence, runtime admission,
specification, and work order.

The current admitted fallback/result contract is the anomaly-aware media-local v3
wrapper `pipeline/gpu/production_asr_v3.py` at SHA-256
`6624dcfa554c38c029090ab0dd7ceefa75c868ed0951cbbfefe059e386dec1a1`.
It preserves finite non-inverted word timings that fall outside their segment or
regress/overlap, labels those observations explicitly, and keeps catalogue context
null. Its work-order/result contract identities are
`1fa8c90f70215d9766ae99ce221591bfd31b06fe011d2a8c27bac844b4e5e71a`
and `a2508422b7baf8d6807bafb1441ab9a96b99ffd719fd51c76cbcb7b17b4c223d`.
The resident worker below emits ordinary v3 results.

## Production work-order lifecycle

Use only absolute normalized paths. The work order binds the physical CPython
executable, not merely the environment symlink; production execution enters through
the admitted wrapper so the virtual-environment packages remain available. Generate
and inspect contract identities before materializing any specification:

```sh
HIMR_REPO=/absolute/path/to/HIMR
adapter="$HIMR_REPO/pipeline/bin/asr-faster-whisper-gpu-adapter-v3"

"$adapter" contracts
"$adapter" create-work-order --spec /absolute/private/spec.json \
  --output /absolute/private/work-order.json
"$adapter" validate --work-order /absolute/private/work-order.json
"$adapter" dry-run --work-order /absolute/private/work-order.json
```

The execution sandbox must expose the root filesystem read-only, a fresh network
namespace with only loopback, `/proc`, a private temporary filesystem, the four
required NVIDIA character devices, exactly one writable result root, and the single
UUID lock file. Invoke the wrapper with `-B -I` already supplied by the wrapper and
pass the parent network-namespace identity:

```sh
parent_net_ns=$(readlink /proc/self/ns/net)
bwrap --die-with-parent --new-session --unshare-net --ro-bind / / \
  --dev /dev \
  --dev-bind /dev/nvidia0 /dev/nvidia0 \
  --dev-bind /dev/nvidiactl /dev/nvidiactl \
  --dev-bind /dev/nvidia-uvm /dev/nvidia-uvm \
  --dev-bind /dev/nvidia-uvm-tools /dev/nvidia-uvm-tools \
  --proc /proc --tmpfs /tmp \
  --bind /absolute/private/result-root /absolute/private/result-root \
  --bind /absolute/private/GPU-UUID.lock /absolute/private/GPU-UUID.lock \
  --clearenv --setenv PATH /usr/bin:/bin --setenv HOME /nonexistent \
  --setenv LANG C.UTF-8 \
  "$adapter" run --work-order /absolute/private/work-order.json \
  --expected-parent-network-namespace "$parent_net_ns"

"$adapter" status --work-order /absolute/private/work-order.json
```

`status` is inference-free and write-free, but it is not currently restart-portable.
It replays the sealed normalized document from the raw artifact and verifies the
result envelope, then also traverses the historical persisted
`runtime.expected_device` binding. A clean reboot changed that numeric device value,
so current v1-v4 `status`, validation, and execution remain blocked until the
filesystem-identity successor described below is admitted.

## First corpus run and coordinate quarantine — ordinal 15

The first v1 production run used normalized input SHA-256
`53114c266984e85ef1cdcaf618526f52569d6015e7a49d938a770d58bd6cddd9`
(335.970 seconds). CUDA inference took 8.629365 seconds, RTF 0.02568493,
with measured process peak VRAM 805,306,368 bytes. The complete isolated job took
33.548 seconds and emitted 71 segments and 1,019 words. The result is private,
machine-unreviewed, uncalibrated, and grants no identity, biometric, catalogue,
wiki, rerun, or publication authority.

An integration audit then found that v1 labeled its null-context timeline
`recording_milliseconds`. That is incompatible with the media-local corpus policy.
The exact v1 result is preserved but is ineligible for ordinary transcript import,
private FTS, public search, or release. Its raw `source_*_ms` fields are artifact
local. Do not alter or delete that result; a future deterministic v2 normalization
must publish a new, explicitly linked superseding artifact without rerunning CUDA.
The text-free disposition packet is
`research/corpus/analysis/u_TprxOlL14/analysis-v1.json`.

## Resident v3 batch lifecycle

The admitted resident worker is `production_asr_batch.py` at SHA-256
`399acf139cbe118d7eb0a73becd992cd8825f1aa37cc49a663299aaaa0e1b64e`; its wrapper is
`pipeline/bin/asr-faster-whisper-gpu-batch` at SHA-256
`37a4914b32e04386ecfe9c91ab90477ba3aee940f4064e731e4e5d78b536b3e4`. Use only the
final source-bound runtime receipt
`research/corpus/gpu-runtime/production-runtime-admissions/receipt-resident-batch-v1-final-source-bound.json`
(physical SHA-256
`4f26a1e8c3c4a7f7ac70819dda20355115627190c8c5f4541ae1bdbba4c0f2ab`). Earlier
batch receipts are sealed superseded lineage and are not execution inputs.

Materialize a finite ordered batch only from ordinary sealed v3 work orders that
share the exact output, model, runtime, GPU, inference, and policy bindings:

```sh
HIMR_REPO=/absolute/path/to/HIMR
batch="$HIMR_REPO/pipeline/bin/asr-faster-whisper-gpu-batch"
"$batch" materialize \
  --work-order /absolute/private/member-000001.json \
  --work-order /absolute/private/member-000002.json \
  --batch-root /absolute/private/batch-root \
  --receipt-root /absolute/private/receipt-root \
  --maximum-batch-wall-seconds 1800

"$batch" validate --manifest /absolute/private/batch/manifest.json
"$batch" dry-run --manifest /absolute/private/batch/manifest.json
"$batch" status --manifest /absolute/private/batch/manifest.json
```

`validate`, `dry-run`, and `status` perform frozen replay without CUDA. Production
`run` additionally requires the same Bubblewrap profile as the single-item lane,
with only the result root, receipt root, and UUID lock bind-mounted writable. The
batch root and every input remain read-only. Bind both writable roots explicitly,
pass the parent network namespace, and invoke the admitted wrapper inside the fresh
loopback-only namespace. One process holds one UUID lock and loads one model; items
are inferred sequentially and each ordinary v3 result is atomically published and
exact-replayed before the next ordinal.

The first two-item pilot processed 793.913 seconds of audio in 45.190 seconds end to
end (17.57 times real time), including one 0.544-second model load. CUDA inference
took 19.290 seconds (RTF `0.0242968`, 41.16 times real time) and process VRAM peaked
at 805,306,368 bytes. This validates finite model residency, restart/replay, and the
initial resource envelope; it is not a 32-item thermal soak or an admission of
`BatchedInferencePipeline` neural batching.

Measured single-item inference is about 8.8 times faster than the aggregate recent
CPU whisper.cpp lane. On this two-item pilot, normalized end-to-end throughput was
1.62 times the two preceding isolated single-item jobs because runtime/model replay
was amortized. Before a corpus-wide backfill, complete the staged 30-minute,
two-hour, and thermal-soak gates. Do not weaken per-input hashes, the one-GPU lock,
resource limits, or post-run drift checks to gain throughput.

## Unadmitted v4 successor

An audit found that v1-v3 declared expected preserved-source digests but loaded the
preserved Python implementation by pathname before proving those bytes. It also found
that the fixed-English recipe exposed faster-whisper's `en/1.0` sentinel as though a
language-detection probability had been measured. Existing adapters and results are
preserved unchanged for replay.

`production_asr_v4.py` is the CPU-tested successor candidate. It bootstraps
`verified_dependency_loader.py` only after a stable retained-descriptor hash check,
then compiles and executes preserved v1 only from exact verified bytes. V1's two
local executable helpers are intercepted by a closed dispatcher: only the exact
pinned `admit_hf_model.py` and `admit_runtime.py` bytes can run, and the latter also
requires the runtime receipt to bind the loader, v1, both helpers, and the v4 wrapper.
The reusable loader registers verified modules with ordinary `sys.modules` semantics
and restores any previous registration if execution fails. Dependency opens are
nonblocking, so a regular-file-to-special-file pathname race fails instead of
hanging before descriptor validation. The only allowed forced language is `en`; its
artifact records that setting as `forced_by_inference_profile`, sets
`detection_performed:false`, and leaves raw/calibrated probability null. `auto` is
rejected until model language capability is an explicit admitted dependency; an
English-only model can otherwise return `en/1.0` without performing detection.
Completed-result replay independently re-reads both transcript artifacts and binds
raw, normalized, result-summary, inference, model, and input language lineage.

Candidate source hashes are:

- loader: `b6250aea1c8baf5ed54e867ffa6cc584220378e989dd1637138b3920130b083c`;
- model-admission helper: `82e1ab544c64de36bf1d40fac287b64ed2546fe41085e84623186c76bfea57a1`;
- runtime-admission helper: `8f20e9efb2d9f293f9415a40845be9412644ad33e59c569b72a32acfca8261a2`;
- v4 adapter: `377d6ab33e970a9bbf40979f4a3832181b022fd31f726e22eef7b47ed0f26b79`;
- v4 wrapper: `9f1b2539a90071fc13b0332e7164e1c0c9beccc433d1778b4f8d02b3dd6d93b4`;
- v4 work-order contract:
  `64e2f53df8a135b96cce91d07942fb402900247b8c182e9d4411844f4e058a63`;
- v4 result contract:
  `a9395e09418812d3d9c3b5e92cd489d309495e403190c69e283ca3bbc1b4e7ec`.

These are development evidence, not an execution admission. Do not create corpus v4
work orders or repoint resident batch v1. A separate runtime receipt must bind the
loader, preserved v1, v4 adapter/wrapper, model, runtime, GPU, and a fresh
accuracy/resource benchmark before one bounded v4 pilot. The receipt must list both
admission helpers and the verified loader as production sources; omission is a
hard replay failure.

V4 also inherits v1's persisted `runtime.expected_device` contract. A clean restart
can change Linux device numbering while the exact runtime remains on the intended
main-drive filesystem, so this field currently causes false replay failures. V4 must
not be admitted until a successor replaces that persisted number with
current-operation same-filesystem checks and stable filesystem-tier/root-relocation
semantics. This is independent of the pre-execution trust-anchor blocker below.

V4 is also blocked on a pre-execution trust anchor. The shell wrapper, managed
Python, and adapter pathname necessarily execute before the adapter can replay its
own runtime receipt; the CUDA library-path bootstrap can re-execute that pathname as
well. An in-process hash cannot authenticate code that is already running. Admission
therefore requires an external sealed verified launcher, a genuinely immutable
read-only runtime/source snapshot, or proven fs-verity enforcement. The ordinary
post-execution receipt is necessary but not sufficient.

## Receipt-bound v0.3 queue to v5 batch bridge

`pipeline/bin/materialize-gpu-asr-batch-v1` is the restart-portable operator bridge
from the validated preprocess-v0.3 GPU handoff to production ASR v5 and resident
batch v2. It accepts only explicitly repeated queue ordinals; it never scans a
directory or guesses a member. The bridge replays the queue, portable-root
registration, exact production profile, admitted runtime-v2 receipt, and receipt
lineage. It does not open an input audio payload. The existing v5 typed projection
creates each work order and the existing batch-v2 materializer creates the batch.

Create all six output roots beforehand as distinct, non-nested, current-user-owned
mode-`0700` directories beneath the registered hot root. Do not place any argument
under `/mnt/archive/HIMR`. Use the queue SHA-256 reported by the GPU handoff
materializer and preserve the intended batch order in the repeated arguments:

```sh
bridge=/absolute/repository/pipeline/bin/materialize-gpu-asr-batch-v1

"$bridge" contract

"$bridge" materialize \
  --queue-manifest /absolute/hot/gpu-queue/queues/QUEUE/manifest.json \
  --expected-queue-sha256 SHA256 \
  --root-registration /absolute/hot/root-registration.json \
  --expected-root-registration-sha256 SHA256 \
  --runtime-admission /absolute/hot/runtime-admission-v2.json \
  --expected-runtime-admission-sha256 SHA256 \
  --production-profile /absolute/hot/production-profile-v2.json \
  --expected-production-profile-sha256 SHA256 \
  --queue-ordinal 3 \
  --queue-ordinal 7 \
  --work-order-root /absolute/hot/gpu-v5-work-orders \
  --receipt-root /absolute/hot/gpu-v5-materialization-receipts \
  --result-root /absolute/hot/gpu-v5-results \
  --batch-root /absolute/hot/gpu-v2-batches \
  --event-root /absolute/hot/gpu-v2-events \
  --lock-root /absolute/hot/gpu-v2-locks

"$bridge" validate \
  --receipt /absolute/hot/gpu-v5-materialization-receipts/materializations/RECEIPT.json \
  --expected-receipt-sha256 SHA256 \
  --replay
```

Rerunning the same command replays byte-identical work orders, the batch manifest,
and the final materialization receipt instead of replacing them. A different
selection or binding produces different content identities. A failed run may leave
valid content-addressed work orders for the next replay, but it cannot publish,
import, archive, delete corpus data, execute inference, or authorize a
`requires_chunking` member.

The materializer output is not trusted-launcher authority by itself. Review the
sealed receipt, work orders, and batch manifest separately. Production launch still
requires the batch manifest and other controls to satisfy the trusted launcher's
root-owned installation/review policy; do not weaken that boundary or treat a
current-user materialization as an admitted execution.
