# Autonomous restart checkpoints

The autonomous Archive controller uses a config-bound backend checkpoint plus the
immutable journal events after that checkpoint. This makes ordinary recovery
proportional to changed/new evidence and the short journal tail instead of to every
media byte already acquired.

## Recovery modes

### First metadata bootstrap

If `checkpoint.json` does not yet exist, the first Start performs a metadata
bootstrap over the full immutable journal. It validates campaign/schedule bindings,
canonical result envelopes, receipt topology, and the reviewed cold-storage
identity, then records no-follow metadata witnesses for the already admitted files.
It does **not** reread every media payload merely to establish this initial cache.
The controller writes the first checkpoint at the next admitted lifecycle boundary.

This is an explicit migration trust boundary: the bootstrap relies on the existing
sealed envelopes and their recorded content identities plus the current filesystem
metadata. Run the manual deep audit below first if a full byte-level verification is
required before accepting that initial checkpoint.

### Checkpoint plus journal tail

Later Starts validate the checkpoint's config hash, campaign ID, schedule-set ID,
self-digest, and exact hash-chain anchor. Only events after that anchor are replayed.
For each completed acquisition, preprocess bundle, and GPU record, recovery rereads
the small control/envelope files and compares the persisted no-follow witness. The
witness contains inode, size, nanosecond modification/change times, mode, link
count, and owner; `st_dev` is deliberately excluded because Linux device numbers
can change across boots.

An unchanged witness avoids reading the associated media payload. A new path or any
metadata mismatch is not accepted from the checkpoint: only that item returns to
its existing exact validator and content rehash. A missing, malformed, conflicting,
or incompatible checkpoint fails closed. Crash-window receipts which exist after
the checkpoint anchor are discovered and validated during reconciliation.

A durable Stop is checked between complete restore units. With an empty GPU-child
journal, emptiness is re-proved under the launch lock before recovery is abandoned.
With any persisted child history, Stop is latched until the exact checkpoint/tail
GPU batch authority has been restored and quiesced; only then does startup publish a
clean stopped lifecycle and return success. Partial in-memory restore state is never
checkpointed. If an external timeout nevertheless kills a child before it can emit
its result, the companion supervisor reports the terminating signal in its own
strict JSON envelope instead of mislabeling the empty stream as malformed JSON.

Checkpoint file witnesses retain the producer's file contract. Controller,
manifest, receipt, result, and GPU JSON remains owner-controlled and single-link.
Only a JSON artifact explicitly named by a preprocess receipt may use the
preprocessor's verified-reuse hardlink policy; it must remain owner-owned,
read-only, stably readable without symlink traversal, confined beneath the exact
private processing-output root, and equal to the receipt's SHA-256. Non-JSON media
is not reread on the ordinary unchanged-witness path.

Unchanged pending GPU records also reuse their checkpointed immutable lineage.
Recovery refreshes only their mutable batch/result/transcript JSON before replaying
the independent child journal and attempt limit; it does not revalidate source
lineage or hash normalized audio. If a pending result has become terminal, recovery
invalidates the pending witness so the next checkpoint binds the terminal result
files. This keeps completed-result authority ahead of attempt-exhaustion parking
without reintroducing a multi-gigabyte startup scan.

GPU readiness also reconstructs its derived queue cache from every queue reference
already admitted by those GPU records. Each queue is loaded from its small canonical
manifest under the recorded SHA-256, portable-root policy, self-identity, configured
profile, deterministic output path, and exact preprocess bundle/state binding. It is
not rebuilt from receipts, source media, or normalized audio. Partially claimed
queues retain their unclaimed suffix; fully claimed queues are skipped in constant
time. A preprocess candidate absent from this durable authority still takes the full
queue materialization and media-validation path. Queue manifests are immutable and
shared between lane forks while each fork receives independent cache indexes, which
avoids multiplying the restored corpus in memory.

The checkpoint is atomically replaced and bound to the exact current journal head.
Normal cadence limits the tail to 64 material events or five minutes; lifecycle,
quiesce, and retry boundaries can force an earlier checkpoint. Immutable result and
receipt files remain completion authority; the checkpoint is only bounded restart
authority and grants no publication, discovery, deletion, or stage-execution power.
If an independent queue lane commits logical shared state just before the matching
journal/root boundary, export is atomically deferred. The prior checkpoint and
pending tail remain valid. A due deferral now raises a bounded scheduler drain:
new finite stage calls pause, already-admitted calls and peer observers finish at
their normal boundaries, and the controller retries under the journal/root
serializer. Parallel dispatch resumes immediately after the exact checkpoint is
written. Exact validation can also refresh filesystem witnesses while leaving the
logical queue-state digest unchanged; a monotonic witness-only generation advance
is safe to include at the current journal anchor and does not require a drain.
Durable Stop and lane faults interrupt the wait; if logical export is still
deferred after every mutator has drained, the controller faults closed instead of
spinning or allowing the journal tail to grow without bound.

## Filesystem identity

Every recovery verifies that `/mnt/archive` is the reviewed XFS filesystem and
verifies the configured cold paths against that mount. Historical configs bind
UUID `5b5813ad-b1a4-4f52-9960-e762ceac5636`. The explicit 2026-09-06 replacement in
`autonomous_controller/cold_mount_migrations.py` binds the exact active 2026-08-30
config ID **and physical SHA-256** to UUID
`af41b7da-a588-41cf-83f8-cd99ef425b74`. That campaign now requires the replacement,
not either UUID. No other config inherits the transition. The recovery monitor
reports the effective UUID and the reviewed transition; historical sealed config
and receipt bytes remain unchanged. The filesystem UUID binds placement; it does not replace
the recorded SHA-256 content identities. This XFS deployment does not provide a
usable `fs-verity` measurement interface, so restart safety does not depend on
`fs-verity`.

## Stopped recovery after a copy or reboot

`recover-checkpoint` performs the same checkpoint-plus-tail validation as Start,
then atomically refreshes the checkpoint without launching pipeline stages:

```sh
autonomous_controller/bin/himr-autonomous-controller recover-checkpoint \
  --config /absolute/private/controller-config.json \
  --expected-config-sha256 LOWERCASE_SHA256
```

It requires an existing checkpoint, the singleton run lock, durable stopped intent,
and an unchanged journal head. It never silently replaces a missing checkpoint with
a metadata bootstrap. Copied raw media has changed metadata witnesses, so this
operation still exactly revalidates those files; it can read the whole migrated
corpus and take hours. A mismatch fails closed. Do not press Start during recovery.
Use `deep-audit-checkpoint` when a full legacy audit rather than targeted recovery
is intended.

For an operator-confirmed rsync copy to the exact reviewed replacement disk,
`recover-checkpoint --trust-rsync-copy` is an explicit alternative. It preserves
the existing checkpoint and journal authority, rereads completed result envelopes,
and requires their exact saved identities plus stable matching sizes, permissions,
owners, link counts, and path topology. It rebinds only copied inode/time/device
witnesses, trusting the operator's copy rather than independently hashing all
previously completed media. New crash-window results still receive exact payload
validation. Conflicting or missing completed evidence fails closed without an
implicit full-scan fallback. This option requires the reviewed config/UUID
transition and stopped maintenance lock; normal Start never enables it. Recovery
reports trusted-copy and exactly revalidated items separately. Subsequent ordinary
recovery again applies the normal strict changed-witness rules.

## Manual offline deep audit

The following maintenance command deliberately performs the legacy full immutable
journal replay and exact payload validation, exports a new backend checkpoint, and
atomically installs it at the unchanged journal head:

```sh
autonomous_controller/bin/himr-autonomous-controller deep-audit-checkpoint \
  --config /absolute/private/controller-config.json \
  --expected-config-sha256 LOWERCASE_SHA256
```

Use it only when the pipeline is durably stopped. The command acquires the same
singleton run lock as `run`, verifies `desired_state: stopped` before and after the
audit, verifies that the journal head did not change, and never launches an
acquisition, preprocess, GPU, cold-retention, or long-form stage. Success and failure
each emit one strict JSON object. The audit may read the entire retained corpus and
can therefore take a long time and generate substantial disk I/O.

Do not press Start while the audit is running. If a Start intent appears before the
checkpoint write, the audit discards its in-memory snapshot and fails without
replacing the prior checkpoint.

## Rollout

Installing these source changes does not require or justify restarting a live
campaign. A running Python process continues with the code it already loaded. Let it
reach a normal safe Stop, then use the next Start to load the checkpoint-aware
recovery path. The ordinary operator console remains Start, Stop, and monitors; the
deep-audit command is an explicit terminal-only maintenance operation.

The recovery monitor exposes the selected mode (`metadata_bootstrap`,
`checkpoint_plus_tail`, or `deep_audit`), fast-reused and targeted-revalidated item
counts, admitted queue manifests loaded, admitted queue count, result-envelope bytes
read, exact media bytes rehashed, and the estimated legacy payload bytes avoided.
