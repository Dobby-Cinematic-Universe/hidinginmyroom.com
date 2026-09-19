# Unified Archive campaign setup

`setup-archive-successor` admits the 2026-08-30 predecessor-plus-addendum
campaign. It is an offline, no-overwrite setup step; it does not discover URLs,
contact a provider, start the controller, preprocess media, run ASR, publish, or
delete anything.

The production contract is deliberately narrow:

- the predecessor config is fixed by path, SHA-256, config ID, and state root;
- both the predecessor controller lock and control lock must already exist and be
  obtainable exclusively without waiting;
- predecessor control and cached status must agree that desired and actual state
  are `stopped`, with no active lane, in-flight work, or GPU child;
- the successor inventory is the fixed mode-`0400`
  `archive-all-known-2026-08-30/campaign-inventory.json` and requires an external
  SHA-256 pin;
- the externally pinned mode-`0400` composite must be below the fixed successor
  control root and must retain the predecessor schedule-set reference and every
  predecessor schedule exactly and in order;
- the predecessor preprocess-control and preprocess-output roots are reused
  without requiring them to be empty, so existing acquisition/preprocess progress
  remains authoritative;
- controller state, events, GPU queues/work/results, and cold staging/receipts are
  created below a new mode-`0700` successor root and must be empty; and
- the backend deeply replays the composite, both source schedule sets, inventory,
  work orders, acquisition results, preprocess receipts, mount identity, and GPU
  controls from an empty successor event journal before the canonical mode-`0400`
  config is admitted; and
- the empty-journal replay must preserve the frozen predecessor's own acquisition
  and preprocess totals (280 acquisition results and 246 physical preprocess
  receipts at the latest locked validation), while its schedule and role
  cardinalities must equal values derived from the sealed composite (currently
  4,484 total, 3,680 normal, 804 cold-only, and 219 schedules).

The old GPU roots are intentionally not reused. GPU completion and pending-batch
authority is carried by the predecessor's config-bound, hash-chained event journal,
not merely by files in those roots. Copying or re-signing that evidence into a new
config would be a separate migration protocol. Reusing the directories without
that protocol would mix writers while still failing to prove the predecessor's 58
completed and 64 pending items. The successor therefore starts with zero restored
GPU records and may reprocess already-transcribed audio. A future reviewed import
can avoid that work without weakening this admission boundary.

After the combined inventory and composite schedule-set manifest have been sealed,
run:

```sh
autonomous_controller/bin/himr-autonomous-controller \
  setup-archive-successor \
  --composite-schedule-set-manifest \
    /srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/producer-control/composite-schedule-sets/bgacqcompositeset_ID/manifest.json \
  --expected-composite-schedule-set-manifest-sha256 LOWERCASE_SHA256 \
  --expected-inventory-sha256 LOWERCASE_SHA256
```

The output location is fixed at
`research/corpus/autonomous/archive-all-known-2026-08-30/controller-config.json`.
Setup returns strict JSON containing its physical SHA-256 and does not set Start.
If any partial fresh directories remain after a failed attempt, exact replay is
allowed only while every expected fresh directory remains empty; foreign or
nonempty state fails closed.

Run the focused tests with:

```sh
python3 -B -m unittest autonomous_controller.tests.test_setup_successor -v
```
