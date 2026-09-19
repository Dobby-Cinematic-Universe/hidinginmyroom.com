# Autonomous Archive campaign controller

This package runs the sealed HIMR Archive.org campaign unattended while keeping the
operator surface to Start, Stop, and monitors. It owns scheduling only: it has no
publication, catalogue, identity, credential, or open-ended discovery authority. The
ordinary controller has no deletion authority; a registered long-form companion has
only the receipt-bound derived-scratch cleanup described below.

## Lifecycle

Start is a two-step durable handshake. The operator service first commits
`request-start`, then admits the foreground `run` process in its user-systemd unit.
`run` consumes an existing running intent and never overwrites it. A Stop accepted
after Start therefore has the later control generation and always wins.

`request-stop` writes one atomic stop intent. Acquisition checks it before every
dispatch and preprocessing checks it before every singleton FFmpeg item. A file
which has started is allowed to finish and seal its receipt, but the next file is
not started. The GPU-owning lane promptly reconciles/stops only its exact journalled
child while slower lanes unwind. SIGINT and SIGTERM use the same graceful path.
Start/Stop/status validate only the small config-bound control or status file; they
do not replay the multi-day event journal.

For a registered long-form deployment, the public `run` wrapper also replays the
mode-`0400` `longform-asr-companion-registration.json` beside the controller config
and validates the companion's initial status before launch. It then supervises the
ordinary controller and long-form campaign together. Stop still writes only the one
ordinary durable intent; both children converge on that same authority. If the
registration is absent, `run` retains the exact ordinary-only behavior. If it is
present but invalid, Start fails closed before child launch.

## Exact production campaign

The canonical mode-`0400` successor config binds the reviewed all-known Archive
inventory and composite schedule set
`bgacqcompositeset_29cb1a0012715281a21d308155698e0c` by path, physical SHA-256,
and identity. The set contains 219 ordered epochs across eight collections:

- 118 `normal_processing` schedules covering exactly 3,680 candidates;
- 101 `cold_acquisition_only_requires_chunking` schedules covering exactly 804
  long candidates; and
- 4,484 unique Archive candidates total, with zero overlap, missing, or unexpected
  identities.

Restore replays every schedule and compares the exact `(source_id, recording_id)`
union by role against the inventory's sealed Archive-only plan. Counts alone are
never accepted. The controller neither discovers URLs nor creates arbitrary work.

All acquisition writes go directly to the shared cold CAS below
`/mnt/archive/HIMR`. Immediately before every acquisition call, and again at
restore, the controller verifies that `/mnt/archive` is the reviewed XFS mount with
UUID `5b5813ad-b1a4-4f52-9960-e762ceac5636` and that every cold output has the same
device identity. Cold retention is disabled for this cold-primary campaign; source
media is never deleted.

## Autonomous pipeline

Four independently scheduled, bounded lanes run without a global cycle barrier:

1. Acquisition remains on the controller's POSIX main thread so the sealed hard
   deadline remains valid. It advances at most one exact schedule with one network
   slot. A schedule call may amortize validation over up to eight files, with a
   durable Stop gate between dispatches.
2. One worker thread runs a bounded preprocess handoff concurrently. It amortizes
   queue validation over as many as eight selected rows but still exposes a durable
   Stop boundary between singleton FFmpeg jobs. No second acquisition or preprocess
   worker exists.
3. GPU queues are packed across preprocess bundles under their common production
   profile/runtime/root bindings. A pack normally waits for 16 members, closes at
   32 members, launches earlier when it reaches the profile's preferred audio
   duration, and flushes a partial tail after 60 seconds or immediately when input
   is drained. New arrivals do not restart the starvation timer. Equal
   normalized-audio hashes are split between batches. Members are ordered
   longest-first so the executor's fixed adjacent two-worker groups contain
   similarly sized inputs. At most one sibling systemd GPU service is active. It
   uses the fixed trusted local launcher, 12-GiB memory envelope, zero swap,
   64-task cap, one-hour runtime cap, closed environment, and exact batch/control
   SHA bindings. Inference never runs in the controller process.
4. The cold-retention adapter is skipped because acquisition already writes to the
   reviewed cold-primary CAS.

Progressed upstream outcomes are delivered to every dependent lane as exact peer
deltas. Per-lane dependency generations prevent a stale downstream `complete`
result from ending the campaign before it has observed new acquisition or
preprocess work. The live status document reports each lane's independent running,
waiting, quiescing, or faulted state.

The safe heavy-work ceiling on the current Ryzen 7 3700X / RTX 3050 / single USB
HDD host is three simultaneous activities: one Archive acquisition, one
four-thread FFmpeg item, and one GPU child. A second acquisition is forbidden by
the shared CAS writer lock, process-global deadline guard, and sealed sequential
queue policy. A second preprocess worker would require process-isolated exact item
claims and a successor contract; threads are unsafe because the pinned processor
uses process-global environment guards. Raising counts in the present controller
would create failure evidence rather than useful parallelism.

Cold-only completions advance independently of preprocess receipts and never enter
the hot-ready cap, preprocessing, or GPU queues.

## Registered long-form companion

The installed adjacent registration deploys a separate recording-first
companion for the 804 cold-only long recordings and any immutable ordinary GPU queue
member classified `requires_chunking`. It is not a fifth controller lane: it has a
distinct mode-`0700` deployment root, discovery receipts, jobs, plans, results,
transcripts, completion documents, and a bounded mutable status projection.

Recordings at or below 30 minutes use one direct model call. Longer recordings use
overlapping logical spans over one verified 16 kHz parent. Adaptive execution uses
one forward-only FFmpeg decode with an in-memory overlap buffer; it never persists
audio chunk files. Word rows retain compact text and exact sample timings with only
present raw probability/anomaly fields, while shared lineage remains at higher scope.
The long-form resource envelope is explicitly `candidate_unsoaked` even though its
production profile, runtime receipt/package closure, and model bundle are hash-bound.

The complete ordinary pipeline has priority. Long-form GPU dispatch is held as
`primary_pipeline_not_fully_drained` until the source desired/actual/lifecycle states
are all `running`; acquisition and preprocessing are drained; ordinary GPU work has
no pending batch or active child; and cold retention is complete or skipped with no
pending replay. This terminal-ready gate prevents a new ordinary batch from appearing
after a point-in-time idle observation. Both lanes still use the exact same
non-blocking GPU-UUID lock as final exclusion. Lock contention is a healthy
`waiting`/held outcome, not a fault. Under the lock the runner also checks GPU UUID,
free VRAM, temperature, and foreign CUDA processes, enforces its offline environment,
and records one run-level `gpu_admission` observation.

The registered status path is `<long-form-deployment-root>/status.json`; the current
layout resolves it to
`research/operator-state/longform-asr-archive-all-known-2026-08-30/status.json`.
It is canonical current-user mode `0600` and contains exact source/campaign config
bindings, lifecycle, expected cold backlog, discovered cold/queue/total counts,
unprepared/preprocessed/prepared/incomplete/completed job counts, active job, UTC
update time, and last error. The ordinary controller refuses `campaign_drained` until
that status covers the expected cold backlog and every discovered companion job is
complete.

Stop is checked before recording dispatch and between adaptive spans. Completed span
results remain immutable; an interrupted recording is `incomplete`, and the next
Start reconstructs completed/pending bindings before resuming. A direct recording is
one bounded span and reaches its safe Stop point when that call completes. Assembly
and the completion seal occur only after every span covers its core interval.

Immutable `discovery/queue-receipts/` bind each ordinary GPU queue envelope once.
Routine status/recovery replays the receipt and manifest metadata rather than hashing
unchanged queue manifests or media repeatedly; the selected media is deeply verified
later by recording-input admission.

For cold-origin jobs, post-completion cleanup is restricted to the derived
`preprocess/output` scratch tree. It is rename-before-delete and receipt-backed so a
restart can reconcile an interrupted cleanup. Source/raw media, plans, span results,
bindings, transcripts, completion evidence, and discovery receipts are never removed.
Queue-origin jobs do not run this cold-scratch cleanup.

The companion persists `lifecycle: faulted` with a bounded `last_error` and exits
nonzero on an operational or validation fault. The outer supervisor then commits
ordinary Stop and gives the other child its registered grace interval before
process-group TERM/KILL. Restart is receipt and immutable-result replay, not manual
state repair. A malformed registration or status fails closed instead of being
silently treated as completion. See
[LONGFORM_ASR_PIPELINE.md](../docs/LONGFORM_ASR_PIPELINE.md) for contracts and manual
diagnostics.

## Durable recovery and failure isolation

The owner-private state root contains atomic `control.json` and `status.json`, a
singleton lock, an immutable hash-chained `events/` journal, and a separate
`gpu-children/` journal. Mutable JSON writers stage their private files in the
same-filesystem mode-`0700` `.mutable-tmp/` directory, so concurrent lane stop checks
never observe a writer's temporary file as a foreign top-level entry. Stage results
and their existing sealed receipts remain completion authority.

Packed batches reuse the existing single-queue materialization receipts as exact
per-source lineage and seal one aggregate v2 batch over their work orders. The
component manifests are never launch candidates. A newly created aggregate is
journalled in the controller event chain before a later supervision tick may start
it, so a crash cannot leave a child whose batch has no controller authority.
Legacy singleton records remain valid: restore claims every `(queue_id, ordinal)`
selected by either record format, rejects conflicting claims, and never repacks a
pending, completed, or parked legacy member.

Full queue, materialization-receipt, and external work-order replay occurs whenever
a GPU record can enter authority: journal restore, peer admission, or local batch
creation. Once admitted, a pending-status poll reloads only the exact SHA-pinned
aggregate batch and replays its mutable result disposition. It verifies the batch
ID, admitted item count, complete ordinal partition, and side-effect-free status
shape before allowing a monotonic transition to completed or parked. This keeps
idle supervision bounded without weakening restart or admission validation.

Acquisition and preprocessing remain independent lanes. Receipt publication briefly
exposes the preprocess writer's reserved temporary filename before its immutable final
hard link is fsynced. If the sealed background reader observes precisely its
`preprocess receipts directory has an unsupported entry` error at that boundary, the
controller retries only that read-only receipt enumeration through a tamper-checked
runtime adapter. It never repeats the enclosing acquisition call. Retries are tightly
bounded; every accepted snapshot still passes the unchanged SHA-pinned producer
validator, while persistent or differently malformed directory entries continue to
fail closed.

Material events grow with items and batches, not idle wall-clock polls. Restart
authority is an atomic config-bound backend checkpoint anchored to one exact event,
plus only the immutable journal tail after that anchor. The first Start without a
checkpoint performs a metadata bootstrap: it validates canonical small envelopes,
receipt topology, schedule coverage, and cold-filesystem identity, then persists
no-follow witnesses without rereading every media payload. Later Starts reread the
small envelopes and reuse unchanged media only when inode, size, nanosecond times,
mode, link count, and owner still match. A new or changed item returns to its original
exact validator and content rehash before it enters authority. Recovery also retains
per-item quarantines, preprocess receipts and failure evidence, GPU
queues/batches/results, child attempts, and cumulative chunking dispositions.

The checkpoint never replaces immutable receipts as completion evidence. It is
bound to the config hash, campaign and schedule-set identities, the reviewed
`/mnt/archive` XFS UUID, and the event hash-chain head. Linux `st_dev` is excluded
from persisted leaf witnesses because it can change across boots. See
[AUTONOMOUS_RESTART_CHECKPOINTS.md](../docs/AUTONOMOUS_RESTART_CHECKPOINTS.md) for
the trust boundary, telemetry, changed-only validation, and offline deep-audit
procedure.

The same restore has a process- and thread-local tool-provenance witness. Identical
bundle pins share only the repeated `ffmpeg -version` and `ffprobe -version` output;
every bundle still resolves the active path and descriptor-hashes/stat-brackets
both executables. Full provenance is executed on first observation and again when
the restore scope closes. The witness is never persisted, cannot cross a fork or
thread, and any mismatch fails the entire restore closed.

If any independent lane fails after another lane has admitted durable progress,
the retry path constructs a fresh backend from the checkpoint and admitted journal
tail. It never continues from a partially updated root. A Stop which wins during
that failure skips the retry-only rebuild and proceeds directly to quiescence.

- Acquisition retries an exact work order three times, then writes its immutable
  quarantine evidence and continues later ordinals.
- Only an operational media-preprocessing failure can enter the controller's exact
  three-attempt ledger. Hash, schema, provenance, and integrity failures still fail
  closed. A parked ordinal remains visible while later ready ordinals continue.
- A GPU batch with invalid exact results or three exhausted child attempts is
  parked durably; supervision advances to the next batch. Ambiguous manager identity
  still fails closed.

Inventory-long items, GPU `requires_chunking` dispositions, acquisition
quarantines, preprocess parks, and GPU parks remain explicit backlog. A supported
ordinary-only pass can stop cleanly as blocked with postprocess/parked work. With a
registered companion, inventory-long and `requires_chunking` work keep the combined
operation alive until companion completion; other quarantines or parks still prevent
full `campaign_drained`.

## Commands

```sh
autonomous_controller/bin/himr-autonomous-controller setup-archive-all-known \
  --output /absolute/private/controller-config.json

# For the sealed predecessor-plus-addendum successor only; see SUCCESSOR_SETUP.md.
autonomous_controller/bin/himr-autonomous-controller setup-archive-successor \
  --composite-schedule-set-manifest /absolute/composite/manifest.json \
  --expected-composite-schedule-set-manifest-sha256 LOWERCASE_SHA256 \
  --expected-inventory-sha256 LOWERCASE_SHA256

autonomous_controller/bin/himr-autonomous-controller request-start \
  --config /absolute/private/controller-config.json \
  --expected-config-sha256 LOWERCASE_SHA256

autonomous_controller/bin/himr-autonomous-controller run \
  --config /absolute/private/controller-config.json \
  --expected-config-sha256 LOWERCASE_SHA256

autonomous_controller/bin/himr-autonomous-controller request-stop \
  --config /absolute/private/controller-config.json \
  --expected-config-sha256 LOWERCASE_SHA256

autonomous_controller/bin/himr-autonomous-controller status \
  --config /absolute/private/controller-config.json \
  --expected-config-sha256 LOWERCASE_SHA256

# Expensive offline maintenance only; durable desired_state must be stopped.
autonomous_controller/bin/himr-autonomous-controller deep-audit-checkpoint \
  --config /absolute/private/controller-config.json \
  --expected-config-sha256 LOWERCASE_SHA256
```

Setup deep-validates the exact production schedule set and GPU controls, creates
only fixed mode-`0700` writable roots, and emits a canonical mode-`0400` config with
no overwrite. It does not start processing. `status` reads only the bounded cached
status plus sealed inventory coverage.

When the reviewed adjacent companion registration exists, the same `run` command is
the automatic paired supervisor entrypoint; no additional operator Start action is
required. A child fault coordinates durable Stop for the pair. The companion's own
read-only command and exact status fields are documented in
[LONGFORM_ASR_PIPELINE.md](../docs/LONGFORM_ASR_PIPELINE.md).

`deep-audit-checkpoint` acquires the controller run lock, deeply replays the full
immutable journal, verifies that the durable intent remains stopped and the journal
head remains unchanged, and atomically replaces only `checkpoint.json`. It launches
no pipeline or companion stage and emits one strict JSON result. It can read the
entire retained corpus, so ordinary restarts should use automatic checkpoint-plus-
tail recovery. Installing the checkpoint-aware code does not require a live restart:
let the current process reach a safe Stop, then load it on the next Start.

The successor setup has a stricter handoff contract: it freezes and verifies the
stopped predecessor, reuses its nonempty preprocess roots, creates fresh
controller/GPU/cold roots, and deep-restores the composite with an empty successor
journal before config admission. See [SUCCESSOR_SETUP.md](SUCCESSOR_SETUP.md).

Run the focused tests with:

```sh
python3 -B -m unittest discover -s autonomous_controller/tests -v
python3 -B -m unittest acquisition.tests.test_archive_preprocess_handoff -v
```
