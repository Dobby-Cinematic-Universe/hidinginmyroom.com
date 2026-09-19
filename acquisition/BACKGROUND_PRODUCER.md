# Bounded background acquisition producer

`background_producer.py` decouples public downloading from CPU/GPU processing without
changing the guarded acquisition boundary. It is a finite foreground worker: a user
service or timer may run it independently while downstream workers consume already
verified results. It does not daemonize, discover candidates, open the catalogue,
access cold storage, or accept credentials.

The producer accepts one immutable schedule. That schedule binds the exact sealed
queue manifest and current producer source, a main-drive preprocess state root, high
and low ready-buffer watermarks, a strengthened free-space floor, and maximum bounds
for one invocation. Every actual adapter call still goes through
`queue_runner.run_queue` with network concurrency one and the queue's exact ordinal
semantics.

## Why one network slot still overlaps work

The useful concurrency is across stages:

```text
public download N+1  ||  FFmpeg preprocess N  ||  resident GPU ASR N-1
```

The current acquisition lock covers the entire download, hash, probe, content-addressed
admission, and result write for one output root. Keeping one network slot preserves
that proven capacity and crash-recovery behavior. Concurrent downloads sharing the
same output root require a future reservation-ledger contract; this producer does not
bypass the lock.

## Materialize and validate a schedule

Use an owner-only control directory on the main drive. The preprocess worker must use
the exact state root named here for its completed batch receipts.

```sh
umask 077
mkdir -p /srv/himr-private/background-acquisition
chmod 0700 /srv/himr-private/background-acquisition

acquisition/bin/background-producer materialize \
  --manifest /srv/himr-private/acquisition/bundles/acqbundle_ID/manifest.json \
  --preprocess-state-root /srv/himr-private/background-acquisition/preprocess-state \
  --free-space-floor-bytes 137438953472 \
  --ready-high-items 8 \
  --ready-low-items 4 \
  --ready-high-bytes 17179869184 \
  --ready-low-bytes 8589934592 \
  --maximum-dispatch-items-per-run 8 \
  --maximum-dispatch-bytes-per-run 17179869184 \
  --maximum-run-seconds 14400 \
  --output /srv/himr-private/background-acquisition/schedule.json

acquisition/bin/background-producer validate \
  --schedule /srv/himr-private/background-acquisition/schedule.json
```

Materialization is offline. It validates the complete sealed queue and writes the
schedule as canonical mode `0400` JSON. Exact replay is idempotent; source, manifest,
delegated runner/acquisition/materializer software, or policy drift fails closed.
Mutable state and reports under `/mnt/archive/HIMR` are explicitly refused without
inspecting that mount.

## Run one foreground cycle

```sh
acquisition/bin/background-producer run \
  --schedule /srv/himr-private/background-acquisition/schedule.json \
  --max-new-items 8 \
  --max-new-bytes 17179869184 \
  --max-run-seconds 14400 \
  --free-space-floor-bytes 137438953472 \
  --report /srv/himr-private/background-acquisition/reports/cycle-000001.json
```

The run replays every completed acquisition and every applicable completed preprocess
receipt before considering the network. A result remains in the ready buffer until a
canonical `completed_private_media_preprocess_batch_item` receipt binds its exact
result path/hash, admitted media, and a still-present hash-matching preprocess result.
Invalid receipts fail closed rather than creating capacity.

Within one producer invocation, the exact queue summary returned by the delegated
runner may be reused after its manifest, work-order, count, row, and result bindings
are revalidated. This avoids immediately hashing the same completed payloads again.
The snapshot is never persisted or reused by a later command: every invocation and
restart begins with a fresh full queue validation, and any failed dispatch triggers a
fresh validation before reporting state.

The producer resumes only when both ready counts are at or below their low-water
marks. It then admits a contiguous pending prefix whose full sealed `max_job_bytes`
reservations fit beneath both runtime limits and high-water marks. It never skips a
blocked head ordinal. Reaching a bound or watermark is a successful `held`/`bounded`
stop. A provider failure is recorded and bounded to the exact ordinal so later
sealed work can continue; a local integrity, replay, or receipt-write failure remains
fail-fast. Earlier durable results remain
valid and the next invocation resumes them. Put retry delay and a finite retry count in
the service supervisor so a persistent unavailable head does not spin.

The optional report is immutable mode `0400`; choose a new path for each cycle. The
queue results themselves remain the authoritative restart state even if no report was
requested.

## Consume the next ready Archive prefix

The tracked handoff command removes the manual result-path step. It replays the exact
schedule and queue, applies the same preprocess-receipt acknowledgement contract used
for producer backpressure, and selects only a bounded completed-but-unacknowledged
prefix in sealed queue order:

```sh
acquisition/bin/archive-preprocess-next \
  --schedule /srv/himr-private/background-acquisition/schedule.json \
  --bundle-root /srv/himr-private/background-acquisition/preprocess-control \
  --processing-output-root /srv/himr-private/background-acquisition/preprocess-output \
  --limit 4
```

The bundle and processing roots must be disjoint owner-only main-drive directories;
the preprocess state root comes from the schedule and cannot be substituted. Each
selected queue ordinal receives a deterministic one-item `asr-ready` selection and
bundle. This makes an interrupted item replay the same immutable controls, while a
valid completion receipt prevents it from being selected again. No ready item returns
`status: held`; the command performs no network, catalogue, cold-storage, deletion, or
publication operation. The hard command limit is eight.

## Run a finite rolling producer and preprocessor

For genuine stage overlap, the rolling supervisor starts exactly one producer worker
and one receipt-bound ASR-ready worker. It invokes the producer one ordinal at a time,
so each newly durable result can feed preprocessing while the next public download is
in flight:

```sh
acquisition/bin/archive-rolling-pipeline \
  --schedule /srv/himr-private/background-acquisition/schedule.json \
  --bundle-root /srv/himr-private/background-acquisition/preprocess-control \
  --processing-output-root /srv/himr-private/background-acquisition/preprocess-output \
  --max-new-items 4 \
  --max-new-bytes 8589934592 \
  --max-run-seconds 14400 \
  --free-space-floor-bytes 137438953472 \
  --max-preprocess-items 8
```

All limits are fixed arguments and are checked against the sealed schedule before a
worker starts. The byte counter uses each dispatched work order's full reservation,
not its smaller eventual payload. The producer retains network concurrency one, the
handoff retains preprocessing concurrency one, and high/low-water receipt accounting
controls when acquisition resumes. A worker failure stops new dispatch, waits for the
other in-flight finite operation, and returns a failed summary containing every
durable partial result or receipt reference observed; the summary itself never grants
completion authority. Restarting reconstructs progress exclusively from queue results
and preprocess receipts.

The exact result reader also tolerates only the acquisition writer's narrow atomic
admission window: if it observes the expected result directory immediately before
`result.json` replacement, it performs at most three complete strict replays. A
persistent malformed directory still fails closed, and a receipt created before a
final replay remains visible in the same handoff summary.

The operator action `archive.rolling_pipeline` owns both console resource claims.
Do not launch standalone or out-of-band acquisition/preprocess commands against the
same roots while it is active.

## Service lifecycle

Run the command as a user-level `Type=oneshot` service and trigger it with a timer or a
larger pipeline supervisor. Recommended service constraints are `Restart=on-failure`,
a bounded restart interval/count, low I/O weight, no supplementary credentials, and a
private umask. A `held` result exits successfully, so a timer can check again after the
CPU worker has emitted more receipts. Do not use shell URL lists or regenerate a queue
inside the service.

## Fresh Archive.org epochs

The existing durable Archive inputs are:

- live catalogue: `research/corpus/corpus-v8.sqlite3`;
- earlier all-source candidate snapshot:
  `research/corpus/acquisition-planning/youtube-next-2026-08-27/queue-plan.json`;
- completed 20-item Archive plan:
  `research/corpus/acquisition-plans/wiki-cited-short-20260827T060000Z.json`;
- completed 20-item bundle:
  `research/corpus/acquisition-queue-bundles/wiki-cited-short-20260827/bundles/`
  `acqbundle_0dcb5d80254d9291189381b2a5f3a9c8/manifest.json`.

That completed bundle must not be rematerialized or reacquired. Before planning a new
epoch, strictly validate it and reconcile every other completed acquisition result
against the live catalogue. The 20 bundle source IDs are currently present in
`media_sources`; any newer completed but unimported result must either be imported
through the existing exact catalogue boundary or explicitly excluded before planning.
The background producer itself has no import authority.

A safe small Archive-only epoch uses two read-only planner passes:

1. Copy the current live catalogue to a disposable owner-only planning copy after the
   reconciliation above; run `plan-queue` without `--selection-only` only to obtain a
   fresh candidate snapshot.
2. From that snapshot, deterministically select a small ordered set of `source_id`
   values where `platform=internet_archive`, `queue_state=ready`, and the recording is
   still unacquired. Seal those IDs in a v1 queue selection.
3. Rerun `plan-queue --selection-manifest ... --selection-only` against the same
   unchanged planning copy, with at most eight items and an explicit byte budget.
4. Materialize that exact plan to a new bundle/output namespace, validate offline,
   then materialize a producer schedule. Never combine it with the completed
   20-item bundle or edit old ordinals.

The 2026-08-27 candidate snapshot contains 3,198 ready Archive.org recordings totaling
433,205,331,096 provider-declared bytes, plus 723 Archive recordings routed to
`requires_chunking`. Those figures are planning evidence, not a download authorization.
They exceed current hot capacity, so small epochs and free-space backpressure are
mandatory until a separately authorized cold-copy/retention design can drain originals.

## Tests

```sh
python3 -B -m unittest acquisition.tests.test_background_producer -v
python3 -B -m unittest acquisition.tests.test_archive_preprocess_handoff -v
python3 -B -m unittest acquisition.tests.test_archive_rolling_pipeline -v
python3 scripts/validate-json-contracts.py
```

The focused suite uses only repository-local fixtures and mocked dispatch. It performs
no real network request and never reads or writes the archive mount.
