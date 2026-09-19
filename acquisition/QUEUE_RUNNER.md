# Sealed acquisition queue runner

`queue_runner.py` is the narrow sequential execution bridge between an immutable
`himr-queue-materializer` bundle and the existing guarded one-order acquisition
boundary. It accepts exactly one bundle `manifest.json`; it has no candidate search,
credential, fallback, substitution, arbitrary-subset, database, import, identity,
event, publication, export, or deletion interface.

## Offline validation

Run this before permitting network acquisition:

```sh
acquisition/bin/queue-runner validate \
  --manifest /srv/himr-private/acquisition/bundles/acqbundle_ID/manifest.json \
  > /srv/himr-private/reports/acqbundle_ID.validate.json
```

`validate` is offline and read-only. It does not create the media output root, invoke
an adapter, acquire a writer lock, open SQLite, write a runner state file, or alter a
completed result. It performs all of these checks:

- strict duplicate-key/non-finite JSON parsing and canonical serialization;
- the content-derived plan and bundle IDs, supported materializer version, exact
  public-only/no-authority safety block, and exact immutable bundle layout;
- owner-controlled `0500` bundle directories and `0400` single-link manifest/work
  order files, with no missing, extra, writable, hard-linked, or symlink entries;
- contiguous one-based ordinals and the exact generated job/path identities;
- every work order through `acquire.validate_work_order`, followed by an exact check
  against its manifest entry and the bundle's output, capacity, HTTP, and hash-pinned
  yt-dlp policy;
- the currently installed yt-dlp launcher bytes against the bundle pin; and
- every extant durable result through `acquire.load_reusable_result`, including its
  exact envelope, work-order identity, content-addressed payload bytes, SHA-256,
  FFprobe record, source observation, and deterministic catalog projection.

The runner repeats the complete result scan after replaying the bundle, executable,
and software pins. Any concurrent completed/pending transition or replacement makes
validation fail instead of emitting a stale queue-state claim.

A present but invalid result never becomes pending work. It fails validation. A
missing result is pending unless the exact ordinal has a validated quarantine receipt.
Offline validation also replays the append-only failure-attempt ledger and every
quarantine receipt. The canonical JSON summary contains every ordinal in order,
completed/pending/quarantined counts, physical and canonical pins, the acquisition
software hashes, and a `summary_sha256` derived from every other summary field. It
contains no review or publication decision and is not stored by the runner.

## Bounded run

A real run requires four explicit run-local constraints:

```sh
acquisition/bin/queue-runner run \
  --manifest /srv/himr-private/acquisition/bundles/acqbundle_ID/manifest.json \
  --max-new-items 2 \
  --max-new-bytes 4294967296 \
  --max-run-seconds 14400 \
  --free-space-floor-bytes 161061273600 \
  > /srv/himr-private/reports/acqbundle_ID.run.json
```

The item and byte ceilings cannot exceed the remaining pending queue. Bytes are
reserved conservatively at each sealed order's full `max_job_bytes`, rather than an
optimistic remote estimate. The run free-space floor must equal or strengthen the
bundle floor. The time ceiling is a hard POSIX wall-clock deadline: if it expires
during an adapter call, the runner interrupts the call, the existing yt-dlp boundary
terminates its process group, and resumable HTTP staging remains governed by the
existing acquisition boundary. The maximum accepted run duration is seven days.

The runner first validates the entire bundle, every completed result, and the failure
ledger. It then walks the sealed ordinal list with maximum concurrency one. A
completed ordinal is strictly reused; a quarantined ordinal is visibly parked; any
other pending ordinal is dispatched only if all four run bounds admit it. The runner
never jumps over a pending ordinal that a bound refuses. Each actual dispatch calls
only `acquire.run_acquisition(order, dry_run=False)`, then reopens and strictly
validates the durable result and payload before proceeding. Bundle, executable, and
runner/acquisition source pins are replayed around dispatch.

Reaching an item, byte, time, or free-space bound is a successful `bounded` stop and
leaves all remaining ordinals pending. A provider/adapter failure writes one canonical
mode-`0400` failed-attempt receipt, leaves the ordinal retryable, and continues to later
sealed ordinals within the same run. On the third failed invocation for that exact
bundle ordinal, the runner seals a quarantine receipt and parks it. Quarantine is not
completion authority: it never increments `completed_count`, never enters the ready
media buffer, and remains exactly identifiable for a future sealed retry queue or a
future explicit retry policy. Local integrity, ledger-write, bundle-replay, and result
replay failures remain fail-fast because skipping them would hide corruption.

Receipts live below
`OUTPUT/.queue-failure-state-v1/BUNDLE_ID/ordinals/ORDINAL/`. Each one binds the
manifest hash, job ID, canonical work-order hash, exact attempt number, timestamp, and
bounded error. The ledger is append-only, canonical, owner-only, hash-identified, and
strictly replayed; malformed, reordered, writable, symlinked, or substituted state
fails closed. The receipt schema is
`schemas/queue-failure-receipt.schema.json`.

When nothing remains pending, the only non-weakening run ceilings are
`--max-new-items 0` and `--max-new-bytes 0`; the runner returns the completed replay
without invoking the adapter.

## Summary interpretation

- `completed_before` / `pending_before` / `quarantined_before` describe the strict
  preflight replay.
- `new_item_count` and `new_byte_count` describe newly durable results in this call.
- `retryable_failed_count`, `failed_attempt_count`, `quarantined_count`, and
  `parked_count` expose failure isolation without treating it as success.
- `terminal_count` is completed plus quarantined; it is an execution-drain metric,
  not a corpus-completion claim.
- `adapter_invocation_count` counts pending-order calls into the hardened acquisition
  boundary.
- `dispatch_reservation_bytes` is the conservative sum admitted by the byte bound.
- `stop_reason` is `all_completed`, a named run bound,
  `retryable_failures_recorded`, `all_runnable_work_exhausted_with_quarantine`, or an
  integrity failure.
- Per-ordinal actions distinguish validated pending/reuse, run reuse, new completion,
  and the narrowly handled case where another guarded writer completed an ordinal.

The summary is operational evidence only. It cannot admit records to the corpus,
identify a person, assert an event, approve a transcript, authorize publication, or
export media.

## Tests and offline pilot

Run the focused suite without network access:

```sh
python3 -B -m unittest acquisition.tests.test_queue_runner -v
```

When the ignored pilot bundle is present, the suite also performs a read-only canary
over its sealed orders. The canary derives its completed and pending counts from the
currently sealed result set, checks that they partition the manifest exactly, mocks
the adapter, and asserts that it is never invoked.
