# Isolated long-form ASR pipeline

This is the recording-first successor for transcribing a complete recording without
creating persistent audio chunks. Its low-level commands remain useful as explicit
operator tools. The registered Archive deployment runs them as an
isolated companion to the ordinary controller without changing the controller config
itself. The companion has separate state and outputs; it does not become an ordinary
controller lane or mutate ordinary queues and receipts.

The initial long-form duration and resource envelope is still an unsoaked candidate,
and no real long-form GPU canary was performed while building it. Work orders and
results therefore record `execution_lineage.longform_scope_status` as
`candidate_unsoaked`. The production profile, runtime receipt and package closure,
and model bundle are hash-bound lineage, but they do not imply that the long-form
envelope itself is production-admitted.

## What it does

The planner selects one of two strategies from the recording's exact 16 kHz sample
count:

- `direct`: the engine receives the verified full normalized FLAC once.
- `adaptive_spans`: one forward-only FFmpeg process decodes logical, overlapping
  sample ranges into a rolling memory buffer. No FLAC or WAV chunks are retained.

The tracked initial candidate policy uses a 30-minute direct ceiling. Longer inputs
use 15- to 30-minute cores, a 20-minute target, up to 15 seconds of context on each
side, a 120-second boundary search budget, and at most 4,096 spans. These values are
not production admission claims. Direct failure does not automatically create an
adaptive replacement plan; automatic failure fallback is deferred.

Each completed span is immutable and resumable. The assembler reconciles overlap,
projects span-local times onto the parent timeline, and creates one recording-level
transcript with an explicit coverage ledger. Empty speech output still counts as
decoded coverage only when the engine completed that span.

## Autonomous companion operation

The operator console still exposes only **Start**, **Stop**, and monitors. When the
controller config has its reviewed adjacent
`longform-asr-companion-registration.json`, Start validates the registration and its
initial status before launching the ordinary controller and long-form campaign as one
supervised operation. A missing registration preserves the legacy ordinary-only
behavior; a present but invalid registration fails closed before either child starts.

Stop remains one durable request against the ordinary controller. The companion
observes that same intent. It starts no new recording after Stop, and an adaptive job
finishes its current logical span before returning resumable `incomplete` status. A
direct job is one bounded span, so its next safe checkpoint is the end of that span.
Start again replays immutable discovery receipts, plans, completed span results, and
completion documents before resuming missing work.

The ordinary GPU lane has scheduling priority, while independent acquisition,
preprocessing, and cold retention continue. Long-form dispatch remains held as
`ordinary_gpu_lane_not_idle` unless the source controller's desired, actual, and
lifecycle states are exactly `running` and every ordinary demand signal is empty:
ready/pending batches, pending items, active/current child, and the partial-pack
buffer.

That projection is paired with a second UUID-bound opportunity flock. The ordinary
controller retains this lease from immediately before child launch through terminal
reconciliation; the long-form runner acquires it nonblockingly and rechecks ordinary
demand before loading the model. A late race therefore becomes a held long-form
cycle, never a failed ordinary child attempt. The original nonblocking GPU-UUID lock
remains the final CUDA exclusion mechanism.

While an adaptive recording runs, the long-form worker checks ordinary demand after
each immutable span. New demand returns
`ordinary_gpu_work_observed_between_spans`, preserves every completed span, unloads
the model, and resumes later. Direct recordings contain one bounded span and yield at
its end. Waiting status heartbeats are refreshed from the already validated small
projection without rescanning discovery receipts.

Before retaining the source or taking either GPU lock, the long-form runner replays
the controller-bound production profile, runtime receipt and installed package
closure, and model bundle. `run-plan` then re-executes once with `LD_LIBRARY_PATH`
replaced by the exact cuBLAS directory from the pinned Python runtime. It requires
and probes both `libcublasLt.so.12` and `libcublas.so.12`. This is expected under the
companion's sanitized subprocess environment and does not inherit arbitrary host
library paths.

Under the locks, the runner enforces the offline environment and admits GPU UUID,
free VRAM, temperature, and absence of foreign CUDA compute. One run-level
`gpu_admission` observation records that check.

The registered mutable status is the owner-only mode-`0600` file at the path bound by
the registration. For the Archive deployment layout it is:

```text
/srv/himr/research/operator-state/longform-asr-archive-all-known-2026-08-30/status.json
```

Its exact fields are:

| Field | Meaning |
| --- | --- |
| `kind`, `schema_version` | `himr_longform_asr_campaign_status`, version 1 |
| `source_controller` | Bound ordinary controller config ID and physical SHA-256 |
| `campaign_config` | Bound companion config ID and physical SHA-256 |
| `lifecycle` | `ready`, `running`, `waiting`, `stopped`, or `faulted` |
| `expected_cold_backlog` | Sealed count of cold-only long recordings |
| `discovered` | `cold_candidates`, `queue_candidates`, and their `total_candidates` |
| `jobs` | `unprepared`, `preprocessed`, `prepared`, `incomplete`, and `completed` |
| `active_job` | Current job ID only while lifecycle is `running`; otherwise null |
| `updated_at` | UTC timestamp at second precision |
| `last_error` | Bounded error type/message for a fault, otherwise null |

The ordinary controller may report full `campaign_drained` only after its own work is
drained and the registered status covers the complete expected cold backlog, has no
active job, and marks every discovered companion job completed. The standalone
read-only replay is:

```sh
HIMR_LONGFORM_CONFIG=/srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/longform-asr-campaign-config.json
HIMR_LONGFORM_SHA=85a60d152e225bf9f09129bd613794d7105c003f398520bbab4c929b7e08a756

pipeline/bin/longform-asr-campaign status \
  --config "$HIMR_LONGFORM_CONFIG" \
  --expected-config-sha256 "$HIMR_LONGFORM_SHA"
```

The sealed setup and one-recording campaign state machine are detailed in
[LONGFORM_ASR_AUTONOMOUS_DEPLOYMENT.md](LONGFORM_ASR_AUTONOMOUS_DEPLOYMENT.md).
The installed deployment has config ID
`himrlongcfg_93be7667e911b9846d2dd711004e9d67`; the command above uses its external
SHA-256 rather than trusting a digest recomputed from an unreviewed file.

`waiting` with no error is normal when acquisition has not produced the next cold
result or the ordinary GPU lane has demand. `faulted` always carries
`last_error` and the companion exits nonzero. A supervised child failure durably
requests ordinary Stop; after the registration's grace period, the supervisor
terminates any remaining child process groups. Atomic outputs make the next Start a
replay-and-resume operation.

Immutable `discovery/queue-receipts/` records envelope-validate an ordinary GPU queue
manifest once. Later status and discovery replay file metadata and the sealed receipt
instead of repeatedly hashing unchanged queue manifests or media. Media bytes are
still verified at recording-input admission, where they become execution authority.

After a cold job has a sealed complete transcript and completion document, cleanup
may remove only that job's derived `preprocess/output` scratch tree. It first renames
the exact tree to a cleanup tombstone, removes it, and seals `cleanup.json`; restart
reconciles either side of that sequence. The scratch tree may contain the normalized
FLAC and other preprocess derivatives; their bound hashes remain in retained evidence.
Raw source media, plans, span results, bindings, transcripts, completion documents,
discovery receipts, and other provenance are never cleanup targets. Queue-origin jobs
have no cold preprocess-scratch cleanup. Do not manually delete or broaden this scope.

## Prerequisites and paths

The remaining sections describe the low-level manual path for diagnostics and
isolated canaries. After registration, normal autonomous operation uses the console
Start/Stop pair and the campaign adapter above. Do not launch a manual runner against
a registered companion job or reuse its deployment/result paths.

Start with a completed media-preprocess `result.json` whose normalized artifact is a
mono 16,000 Hz FLAC. Keep the long-form control and result roots on the hot/main drive.
The runner explicitly rejects output under `/mnt/archive/HIMR`; that volume remains
the cold raw-media tier.

The runner also requires:

- the repository's pinned GPU runtime at
  `research/corpus/gpu-runtime/env/bin/python`;
- a controller configuration with an enabled, valid production GPU profile,
  runtime-admission receipt and exact installed package closure, and local model
  bundle;
- the configuration's exact SHA-256; and
- hash-pinned local `ffprobe` and `ffmpeg` executables. The input adapter requires
  `--ffprobe-sha256`; there is no unpinned admission mode.

The example below uses task-specific shell variables. Replace the preprocess result,
IDs, and run name before executing it. Every manifest and transcript output is
create-only, so use a new run directory instead of overwriting a prior run.

```sh
cd /srv/himr

HIMR_REPO=/srv/himr
HIMR_RUN_ROOT=/srv/himr/research/corpus/longform-asr/manual/example-001
HIMR_PREPROCESS_RESULT=/absolute/path/to/completed-preprocess/result.json
HIMR_RECORDING_ID=recording_example_001
HIMR_MEDIA_ID=media_example_001
HIMR_CONTROLLER_CONFIG="$HIMR_REPO/research/corpus/autonomous/archive-all-known-2026-08-30/controller-config.json"

mkdir -p "$HIMR_RUN_ROOT"

HIMR_CONTROLLER_SHA=$(sha256sum "$HIMR_CONTROLLER_CONFIG" | awk '{print $1}')
HIMR_FFPROBE_SHA=$(sha256sum /usr/bin/ffprobe | awk '{print $1}')
HIMR_FFMPEG_SHA=$(sha256sum /usr/bin/ffmpeg | awk '{print $1}')
```

If the preprocess result already contains the intended media ID, `--media-id` may be
omitted.

## 1. Admit the normalized recording

This rehashes the complete FLAC, reads its exact `duration_ts` at a `1/16000` time
base, and imports silence midpoints from the preprocess routing JSON when available.
The adapter retains the verified FLAC and pinned `ffprobe` file descriptors across
the probe, invokes both through `/proc/self/fd`, and then rechecks descriptor
identity, path identity, and content hashes before admitting the result. It writes
metadata only.

```sh
pipeline/bin/longform-asr-input \
  --preprocess-result "$HIMR_PREPROCESS_RESULT" \
  --recording-id "$HIMR_RECORDING_ID" \
  --media-id "$HIMR_MEDIA_ID" \
  --ffprobe /usr/bin/ffprobe \
  --ffprobe-sha256 "$HIMR_FFPROBE_SHA" \
  --output "$HIMR_RUN_ROOT/recording-input.json"
```

Use `--without-routing-boundaries` only when the existing routing observations should
not influence logical boundary placement.

## 2. Build and validate the plan

```sh
pipeline/bin/longform-asr-plan build \
  --manifest "$HIMR_RUN_ROOT/recording-input.json" \
  --policy "$HIMR_REPO/corpus/examples/longform-asr-policy.initial-candidate.json" \
  --output "$HIMR_RUN_ROOT/plan.json"

pipeline/bin/longform-asr-plan validate \
  --plan "$HIMR_RUN_ROOT/plan.json" >/dev/null
```

Planning is metadata-only and does not open CUDA or modify pipeline state.

## 3. Inspect resumable status

`status` reconstructs work orders and strictly replays any existing results. It does
not load a model, take the GPU lock, or create the result root.

```sh
pipeline/bin/longform-asr-runner-v1 status \
  --plan "$HIMR_RUN_ROOT/plan.json" \
  --output-root "$HIMR_RUN_ROOT/results" \
  --controller-config "$HIMR_CONTROLLER_CONFIG" \
  --controller-config-sha256 "$HIMR_CONTROLLER_SHA" \
  | jq '{status, plan_id, strategy, span_counts, gpu_invoked}'
```

If `--initial-prompt` or `--hotwords-json` will be used for execution, pass the exact
same values to every later `status` and `run-plan` invocation. They are part of each
work-order identity. A hotwords file has the exact shape
`{"hotwords":["HIMR","Pia"]}`.

## 4. Run or resume explicitly

This is the only GPU step. It replays the controller-bound profile, runtime receipt
and package closure, and model lineage; locks and admits the configured GPU UUID;
retains and authenticates the parent FLAC; loads one model; and executes only missing
spans in order. The result remains explicitly `candidate_unsoaked` at long-form scope.

```sh
pipeline/bin/longform-asr-runner-v1 run-plan \
  --plan "$HIMR_RUN_ROOT/plan.json" \
  --output-root "$HIMR_RUN_ROOT/results" \
  --controller-config "$HIMR_CONTROLLER_CONFIG" \
  --controller-config-sha256 "$HIMR_CONTROLLER_SHA" \
  --ffmpeg /usr/bin/ffmpeg \
  --ffmpeg-sha256 "$HIMR_FFMPEG_SHA" \
  --bindings-output "$HIMR_RUN_ROOT/span-bindings.json"
```

On success, `span-bindings.json` is emitted only after all spans replay as complete.
If the process stops, completed result directories remain immutable; rerunning the
same command replays them and processes only missing spans. A command failure is
reported as one strict JSON object, while a span without a complete result remains
`pending`; the current runner does not persist a separate failed-span receipt.

## 5. Assemble one recording transcript

Validate the complete bindings in memory, then create the immutable recording-level
document:

```sh
pipeline/bin/longform-transcript-assembler \
  --parent-manifest "$HIMR_RUN_ROOT/plan.json" \
  --span-results "$HIMR_RUN_ROOT/span-bindings.json" \
  --validate-only

pipeline/bin/longform-transcript-assembler \
  --parent-manifest "$HIMR_RUN_ROOT/plan.json" \
  --span-results "$HIMR_RUN_ROOT/span-bindings.json" \
  --output "$HIMR_RUN_ROOT/recording-transcript.json"
```

Treat `coverage.complete`, the interval ledger, and boundary-conflict counts as the
completion signals. A nonempty `segments` array alone does not prove full coverage.

## Compact word metadata

Word rows intentionally avoid repeated metadata. A persisted span word contains only
`text`, paired half-open `start_sample`/`end_sample` values when the engine supplies
word timing, and, when present, an uncalibrated raw probability, a true clipping
marker, or anomaly flags. If any word in a native segment lacks timing, the assembler
preserves the native evidence but emits one segment-owned unit with `words: []`; it
does not invent exact per-word coordinates from the segment interval. An assembled
timed word is similarly limited to text, parent samples, and optional raw
probability/anomalies. Null probabilities, false clipping markers, and empty anomaly
lists are omitted. Array order supplies word order; milliseconds, source/span
lineage, model provenance, ownership, policy, review state, and coordinate semantics
live once at segment, result, or document scope.

Exact samples are authoritative. Recording segments include rounded milliseconds for
display only. All outputs remain private, machine-generated, unreviewed, not verified
quotations, and have no publication, catalogue, identity, wiki, archive, deletion, or
retraction authority. Corpus import and a user-facing explorer remain downstream
post-processing work.

## Contracts and current validation boundary

The normative contracts are:

- [`longform-asr-recording-input-manifest.schema.json`](../corpus/schemas/longform-asr-recording-input-manifest.schema.json)
- [`longform-asr-planning-policy.schema.json`](../corpus/schemas/longform-asr-planning-policy.schema.json)
- [`longform-asr-plan.schema.json`](../corpus/schemas/longform-asr-plan.schema.json)
- [`longform-asr-span-work-order.schema.json`](../pipeline/schemas/longform-asr-span-work-order.schema.json)
- [`longform-asr-span-transcript.schema.json`](../pipeline/schemas/longform-asr-span-transcript.schema.json)
- [`longform-asr-span-result.schema.json`](../pipeline/schemas/longform-asr-span-result.schema.json)
- [`longform-span-transcript-bindings.schema.json`](../pipeline/schemas/longform-span-transcript-bindings.schema.json)
- [`longform-recording-transcript.schema.json`](../pipeline/schemas/longform-recording-transcript.schema.json)

The design rationale is recorded in
[ADR 0017](adr/0017-direct-first-long-recording-asr.md).

CPU planning, fake-engine execution/resume, exact-sample assembly, schema, and wrapper
tests are appropriate while the autonomous campaign is running. Real CUDA canaries,
soak tests, threshold tuning, and automatic direct-failure fallback remain deferred.
An installed companion registration supplies scheduling and supervision, not
evidence that those performance gates have passed.
