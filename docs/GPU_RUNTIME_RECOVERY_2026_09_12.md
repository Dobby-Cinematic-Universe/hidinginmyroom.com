# GPU runtime recovery and exact Archive update

The ordinary GPU lane was blocked by a legitimate host upgrade: bubblewrap
changed to Fedora's verified `0.12.0-1.fc44` package and the booted kernel changed
from `7.1.10-200.fc44.x86_64` to `7.1.13-200.fc44.x86_64`. The sealed execution
image and all 17 host-library files were verified unchanged. This was a runtime
pin mismatch, not a newly observed archive-disk I/O failure.

Fresh local-private controls and a successful doctor receipt are under
`research/corpus/gpu-runtime/portable-v2-local-private-20260829T211303Z/controls-20260912-runtime-recovery-v2`.
The earlier `controls-20260912-bwrap-recovery` candidate failed its kernel check
and is retained as evidence; it is not the execution authority.

## Historical state and new execution

`autonomous_controller.gpu_runtime_successor` authorizes a narrow, immutable
sidecar beside the original controller configuration. It permits only the
reviewed bubblewrap replacement and kernel-platform refresh. The model, image,
production profile, host-library closure, other tools, and launcher remain pinned.
The original configuration, batches, child identities, results, and checkpoints
are not rewritten to impersonate the new runtime.

Historical batches keep their original launch-spec identity. A confined
historical receipt reader accounts for the exact replaced tool; it does not
modify the trusted launcher's executable admission checks. New materializations
and launches bind the fresh controls. Pending batches bound to the retired
runtime are refused, not silently relabeled.

Activation uses `python3 -B -m autonomous_controller.activate_gpu_runtime_successor`.
It requires the expected stopped control generation, both stopped execution
leases, a drained anchored GPU checkpoint, and no orphan materialization receipt.
`--check-only` validates without publishing. Activation never starts a service,
rewrites a checkpoint, or performs a corpus-wide media audit.

## New 699994 originals

The separate, exact incremental admission is under
`research/corpus/autonomous/archive-699994-update-2026-09-12`.
It contains only these four newly added originals, totaling 1,372,755,713 bytes:

- `20260826-I'M BACK-gPhrE99xwqI.mp4`
- `20260904-life updates-KkrkY1hA7iw.mp4`
- `20260910-delicious dinner-YnVyLWbGNos.webm`
- `20260910-life updates-sOA1o10kAFM.mp4`

The short dinner video follows ordinary processing; the other three follow the
long-form lane. Provider SHA-1/MD5 values are retained as evidence in
`admission.json`; acquisition itself enforces the sealed byte bound and computes
its own SHA-256, not an unavailable provider SHA-256.

Fresh setup uses `autonomous_controller.setup_archive_incremental`, with explicit
inventory, schedule, runtime, and readiness hashes and exact normal/cold counts.
It creates new private operational roots and directly seals the reviewed
replacement disk UUID. Historical disk-migration bindings remain unchanged.

Neither this admission nor runtime recovery publishes transcripts, changes the
live catalogue, imports third-party `HIMR-Transcripts` as native ASR, or enables
continuing remote discovery. Subsequent Archive additions require another
explicit admission.

## Active recovery run and queued update

The original campaign resumed at `2026-09-12T16:29:39Z`, control generation 64,
in `himr-operator-job-20260912aabb4c7d9e6f1234567890ab.service`, invocation
`2e6df486b1454a98acaecfb40ba024d6`. Its first new GPU batch completed successfully:
ordinary ASR increased from 668 to 669, with no pending ordinary GPU items.
Checkpoint preflight fast-reused all 4,484 acquisitions and read zero media bytes
for targeted revalidation. This was not a full corpus audit.

`autonomous_controller.campaign_handoff` provides an explicit, one-shot follow-up.
It binds the live predecessor supervisor and its output inode, checks both
campaign control generations while waiting, and requires full source/long-form
completion, successful exact-unit termination, and both old execution locks
before starting the new campaign. A manual Stop, fault, changed identity, or
expired 24-hour wait cancels the handoff; it does not repair or restart the old
campaign. It runs in a separate admitted service and execs the new supervisor.

The live queued handoff is
`himr-operator-job-20260912ccdd4c7d9e6f1234567890ad.service`, invocation
`643086e4e1f049b284c2708a3ae0e500`. Its initial
`waiting_for_predecessor` binding was confirmed, with original generation 64 and
successor generation 0. Inspect this exact unit's journal for cancellation or
handoff; the original campaign must not be manually restarted in parallel.

The complete autonomous-controller test suite passed: 430 tests.

The incremental controller config SHA-256 is
`9f2c6200964a56553028126a4b5588380285fae7b5402a0bbbfe6675d8529bc3`.
Its active long-form config is `longform-asr-campaign-config-v2.json`, SHA-256
`71050d3bb76913a6678d1d19776b52d03f501c8819618cfeddd899e3eb4e12b2`.
It preserves the original campaign's exact initial prompt, all 17 hotwords,
planning policy, and execution limits. The first unstarted long-form config and
its registration were retained as setup history; the earlier waiting handoff
unit ending `90ac` was cancelled before switching to these final bindings.
The independent operational roots are
`research/operator-state/autonomous-archive-699994-update-2026-09-12` and
`research/operator-state/longform-asr-archive-699994-update-2026-09-12-v2`.
Original-campaign UI counters do not include this separate batch.

Do not start both campaign configurations simultaneously: their GPU execution
locks are campaign-local. Before handoff, the original Stop control cancels the
queued transition. Once the incremental campaign starts, its own graceful Stop is:

```sh
autonomous_controller/bin/himr-autonomous-controller request-stop \
  --config /srv/himr/research/corpus/autonomous/archive-699994-update-2026-09-12/controller-config.json \
  --expected-config-sha256 9f2c6200964a56553028126a4b5588380285fae7b5402a0bbbfe6675d8529bc3
```

Issuing this Stop while the update is still queued also cancels its handoff.
No operator-console profile has been silently redirected to the new campaign.
