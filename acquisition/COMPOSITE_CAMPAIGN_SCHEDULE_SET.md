# Offline predecessor/addendum schedule-set composite

`materialize-composite-campaign-schedule-set` binds exactly two already sealed v1
Archive campaign schedule sets: one predecessor and one addendum. It creates no new
producer schedules. Instead, it emits one immutable hot-control manifest whose
schedule entries reference every existing mode-`0400` schedule by its absolute path,
physical SHA-256, byte count, schedule ID, and identity digest.

This is an offline control-plane operation. It does not discover sources, contact
Archive.org, start a process, inspect media, read or write the acquisition CAS, create
preprocess state, run a GPU, mutate the catalogue, or modify either input campaign.

## Validation and ordering

Both input manifests require caller-supplied SHA-256 pins. The materializer deeply
replays each schedule set's:

- canonical v1 identity, storage topology, role policies, totals, and union proof;
- two source campaign manifests, sealed parent plans, deterministic epoch partitions,
  bundle manifests, and every work order;
- Archive-only source and shared-CAS bindings; and
- existing producer schedules, including physical bytes, campaign/epoch references,
  preprocess-state paths, producer source pin, and bounded role policy.

It then proves that source IDs, recording IDs, and `(platform, native_id)` identities
are unique across the predecessor and addendum. A repeated schedule path, schedule
ID, preprocess-state root, schedule set, or member identity fails closed.

The output order is exact:

1. predecessor `normal_processing`;
2. addendum `normal_processing`;
3. predecessor `cold_acquisition_only_requires_chunking`; and
4. addendum `cold_acquisition_only_requires_chunking`.

Ordering inside each component/role remains the original schedule order. Component,
component-role, aggregate-role, schedule-union, and member-union proofs are recorded
independently in the composite manifest. Source and composite projections must have
equal hashes and ordered rows.

## Materialize

Use a new owner-controlled hot path which is disjoint from both component control
roots and from `/mnt/archive/HIMR`. Its parent must already exist and must not be
group/other writable. The root is created with mode `0700`; the manifest is mode
`0400`.

```sh
umask 077

acquisition/bin/materialize-composite-campaign-schedule-set \
  --predecessor-schedule-set-manifest \
    /absolute/predecessor/schedule-sets/bgacqscheduleset_ID/manifest.json \
  --expected-predecessor-schedule-set-manifest-sha256 PREDECESSOR_SHA256 \
  --addendum-schedule-set-manifest \
    /absolute/addendum/schedule-sets/bgacqscheduleset_ID/manifest.json \
  --expected-addendum-schedule-set-manifest-sha256 ADDENDUM_SHA256 \
  --control-root /absolute/new/composite-control
```

The only new object is:

```text
COMPOSITE_CONTROL/
  composite-schedule-sets/
    bgacqcompositeset_ID/
      manifest.json
```

Exact replay is idempotent: the same inputs and control root reproduce and validate
the same manifest. No-replace admission refuses a conflicting existing object.
Changing any component manifest, campaign, epoch, bundle, work order, schedule, mode,
path, or digest fails before composite admission.

The structural contract is
[`schemas/composite-campaign-schedule-set-manifest.schema.json`](schemas/composite-campaign-schedule-set-manifest.schema.json).
JSON Schema supplies structural checks; deep runtime replay remains authoritative for
cross-file relationships and union equality.

This manifest is deliberately not a controller configuration. Controller/backend and
operator-profile support for consuming a composite is a separate reviewed change.

## Focused tests

```sh
python3 -B -m unittest \
  acquisition.tests.test_materialize_composite_campaign_schedule_set -v
```

The fixtures use tiny local JSON controls and a fake executable which leaves a marker
if invoked. They assert reference-only output, exact flattening, schema validity,
idempotent replay, immutable-mode enforcement, and cross-component overlap refusal.
