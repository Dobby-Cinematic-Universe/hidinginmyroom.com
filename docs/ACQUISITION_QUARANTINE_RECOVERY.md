# Explicit acquisition quarantine recovery

`autonomous_controller.quarantine_recovery` recovers an explicitly sealed set of
quarantined public HTTP work orders. It does not remove quarantines, reset old
attempt counts, change original job identities, replace source URLs, or alter
sealed schedules. There is no catalogue, publication, cloud-upload, or deletion
authority.

The existing acquisition runner and downloader are SHA-pinned by the schedules.
Their source bytes stay unchanged. A narrowly source-bound controller adapter
accepts a completed result with historical quarantine only after validating a new
immutable completion proof. Both ordinary queue projections and incremental
checkpoint restoration require that proof. Missing or mismatched proof fails
closed; merely writing a result beside a quarantine is insufficient.

## Plan and validate

The plan command reads configuration, bundles and failure receipts, not completed
media. It seals exactly the observed quarantines and requires the reviewed count.
Use a new private directory outside the controller's strict `state` directory.

```sh
python3 -B -m autonomous_controller.quarantine_recovery plan \
  --config /absolute/controller-config.json \
  --expected-config-sha256 CONFIG_SHA256 \
  --expected-quarantined-count 13 \
  --output /absolute/private/quarantine-recovery/DATE/plan.json
```

The returned `plan_sha256` is the **physical file hash**, used by the following
commands. The plan also contains a separate internal content identity. Review its
exact URLs, expected sizes and original job IDs before execution.

```sh
python3 -B -m autonomous_controller.quarantine_recovery validate \
  --plan /absolute/private/quarantine-recovery/DATE/plan.json \
  --expected-plan-sha256 PLAN_FILE_SHA256
```

## Execute while the legacy campaign is stopped

Request a graceful controller stop through the existing controller wrapper, wait
for the source controller and long-form companion processes to exit, and confirm
both report stopped with no active jobs. Recovery takes the two existing execution
locks and an additional per-plan mutex. A second invocation cannot overwrite the
active run's status. Do not press Start during recovery.

```sh
python3 -B -m autonomous_controller.quarantine_recovery run \
  --plan /absolute/private/quarantine-recovery/DATE/plan.json \
  --expected-plan-sha256 PLAN_FILE_SHA256
```

Recovery is sequential and retains every original work-order byte, capacity floor
and download byte cap. There are at most three **additional** attempts per item,
with 120 seconds between attempts in a newly generated plan. Each invocation has
a 24-hour work budget; individual acquisition plus targeted result verification
is bounded to four hours or the remaining budget. Previously completed corpus
media are not fully reverified. The original downloader reuses its validated
partial-download staging where possible.

Only explicitly recognized transport failures receive another attempt. Integrity,
unexpected filesystem, authority or receipt errors halt recovery. Exhausted
transport retries remain quarantined; successful items can still be processed.
Do not weaken validation to turn a failed item into a completed one.

An immutable start receipt consumes an attempt before dispatch. The recovery
ledger lives next to the plan under `attempts/JOB_ID/`. Successful retries gain
`OUTPUT/.queue-retry-recovery-v1/BUNDLE/ordinals/ORDINAL/completion.json`, binding
the plan, original work order, original quarantine, and exact completed result.
All old failure receipts and quarantines remain byte-for-byte intact.

## Automatic handoff

`run --resume-campaign` is accepted only inside a correctly admitted systemd user
unit named `himr-operator-job-<32 lowercase hex>.service`, with matching
`HIMR_AUTONOMY_OUTER_UNIT`. After completing the retry pass, it releases the legacy
locks, atomically verifies that the stopped control generation is unchanged,
requests Start, and **execs** the original controller wrapper. This preserves the
unit's MainPID for GPU supervisor admission. Never launch a supervisor as a child
of a still-running recovery coordinator.

Use the normal campaign resource/environment constraints. A new recovery unit
should allow at least 930 seconds for service stop, covering the supervisor's
900-second graceful-stop budget. The controller will hydrate its existing
checkpoint and exact-verify only newly appeared or changed results; neither
`deep-audit-checkpoint` nor `--trust-rsync-copy` is needed.

Even another Stop request while already stopped cancels automatic handoff. Stop
uses bounded-drain semantics: the current download may finish before the request
is observed; no later attempt or automatic restart is allowed. A service SIGTERM
interrupts the current attempt and leaves resumable state. An exec failure restores
stopped intent only if no newer operator request has superseded this run.

A standalone recovery unit is not registered as an operator-console Activity job.
Its service/logs must be managed by its exact recorded unit name. Once handed off,
the normal controller status and durable Stop controls apply.

## Status and interrupted recovery

```sh
python3 -B -m autonomous_controller.quarantine_recovery status \
  --plan /absolute/private/quarantine-recovery/DATE/plan.json \
  --expected-plan-sha256 PLAN_FILE_SHA256
```

Rerun the **same** plan to resume. If a result was durably published before its
completion proof, recovery verifies that exact result, seals the missing proof,
and does not download it again. Do not restart the legacy campaign before this
reconciliation succeeds. An unfinished start without a result consumes its
attempt and receives backoff before the next attempt.

A forced kill inside immutable receipt publication may leave a temporary or
hard-linked file. That ambiguous ledger is intentionally held for manual review;
the runner never deletes evidence or silently resets the attempt budget.

After automatic handoff, recovery status remains `resuming_campaign`; consult the
normal controller and companion status records for live processing progress.

## Offline tests

```sh
python3 -B -m unittest \
  autonomous_controller.tests.test_acquisition_retry_proof \
  autonomous_controller.tests.test_quarantine_recovery \
  autonomous_controller.tests.test_sealed_backend \
  autonomous_controller.tests.test_operational_replay
```
