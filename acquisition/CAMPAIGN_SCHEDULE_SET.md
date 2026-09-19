# Offline Archive campaign schedule set

`materialize-campaign-schedule-set` is the final offline control-plane step between
two reviewed campaign-epoch manifests and the autonomous controller configuration.
It consumes exactly one `normal_processing` campaign and exactly one
`cold_acquisition_only_requires_chunking` campaign. The role is supplied by the
dedicated command-line position and is recorded only in the schedule-set manifest;
the established background-producer schedule schema is not extended with controller
roles.

The command does not start or schedule a producer, contact Archive.org, run a GPU,
inspect acquisition results, create preprocess state directories, or read/write media.
It writes only immutable hot control JSON. In particular, it never stats or enumerates
the cold media root.

## Exact inputs and storage topology

Both input campaign manifests must be canonical mode-`0400` files at already
normalized absolute paths. Their caller-supplied physical SHA-256 digests are checked
through stable, no-symlink reads. The materializer then replays:

- each campaign ID, parent plan, deterministic epoch partition, and campaign coverage
  proof;
- every mode-`0400` epoch plan and its exact derivation from the parent;
- every immutable bundle manifest and work order through the queue runner; and
- the exact Archive.org platform, `archive_media_file` source kind, direct-HTTP route,
  and shared output root on every selected row.

The only accepted media output root is:

```text
/mnt/archive/HIMR/corpus/raw/acquisition-cas
```

All schedule and preprocess-state paths are deterministic strict descendants of one
owner-controlled, mode-`0700` hot `--control-root`. They are disjoint from
`/mnt/archive/HIMR`. A unique preprocess-state path is bound into every epoch
schedule, but that state directory is not created by this command.

## Per-role producer policy

Each epoch receives one schedule built by `background_producer.build_schedule` and
sealed by its immutable writer. Both roles retain network concurrency one, full
sealed-reservation accounting, both-low-water hysteresis, a 14,400-second run bound,
the 512 GiB cold free-space floor, and the current
`sealed_ordinal_sequential_bounded_failure_isolation` dispatch policy.

| Role | Exact epoch cap | Ready items high/low | Ready bytes high/low | Dispatch items/bytes |
| --- | ---: | ---: | ---: | ---: |
| `normal_processing` | 32 / 16 GiB | 64 / 32 | 64 GiB / 32 GiB | 8 / 64 GiB |
| `cold_acquisition_only_requires_chunking` | 8 / 128 GiB | 1000 / 999 | 4 TiB / 3 TiB | 8 / 128 GiB |

The normal schedule's 64/32 item watermarks are deliberately larger than a normal
32-item epoch. Parked preprocess ordinals therefore cannot fill the raw
receipt-accounted schedule watermark and wedge later acquisition. This does not
change the autonomous controller's separate global runnable-ready high watermark of
16 items.

The two input campaigns must seal those role epoch caps exactly, and every replayed
epoch is checked against its role's item and estimated-byte envelope. This prevents a
swapped or repartitioned campaign manifest from silently changing production bounds.

The cold-only role does not hand work to the current normal preprocess/GPU path. Its
large watermarks prevent acquisition-only payloads from being mistaken for normal
ready pressure, while the 8-item/128-GiB dispatch bound matches the reviewed cold
epoch cap. This permits one bounded epoch to drain in one producer run when it also
fits the four-hour limit, avoiding repeated validation reads of already completed
cold payloads. Individual files remain bounded by their sealed work orders.

## Materialize

The parent of a new control root must already exist, be owned by the current user,
contain no symlink traversal, and not be group/other writable.

```sh
umask 077

acquisition/bin/materialize-campaign-schedule-set \
  --normal-processing-campaign-manifest \
    /srv/himr/research/corpus/archive-campaign/normal/campaigns/acqcampaign_ID/manifest.json \
  --expected-normal-processing-campaign-manifest-sha256 NORMAL_CAMPAIGN_SHA256 \
  --cold-acquisition-only-requires-chunking-campaign-manifest \
    /srv/himr/research/corpus/archive-campaign/cold-only/campaigns/acqcampaign_ID/manifest.json \
  --expected-cold-acquisition-only-requires-chunking-campaign-manifest-sha256 \
    COLD_ONLY_CAMPAIGN_SHA256 \
  --control-root \
    /srv/himr/research/corpus/archive-campaign/producer-control
```

The command emits a small receipt to stdout and admits this hot control topology:

```text
CONTROL_ROOT/
  schedules/ROLE/epoch-NNNNNN-acqbundle_ID/schedule.json
  schedule-sets/bgacqscheduleset_ID/manifest.json
  preprocess-state/ROLE/epoch-NNNNNN-acqbundle_ID/  # path binding only; not created
```

Every admitted JSON file has mode `0400`. Exact replay is idempotent; a changed
existing file, digest mismatch, mutable input mode, symlink, unsafe root, role/campaign
overlap, non-Archive order, output-root substitution, source drift, or producer
software drift fails closed.

## Manifest and union proof

The schedule-set manifest orders all normal epochs first and then all cold-only
epochs, preserving each campaign's epoch order. Each schedule entry contains:

- its global schedule ordinal and exact role;
- schedule path, physical SHA-256, byte count, ID, and identity digest;
- the unique hot preprocess-state root;
- source campaign ID/path/SHA-256; and
- source epoch plan and bundle references, selected count/estimated bytes, and member
  digest.

Role totals and the top-level coverage proof independently count source campaigns,
epochs, schedules, selected members, and estimated bytes. The proof rejects duplicate
source, recording, or native identities across roles. It records zero overlap,
missing, and unexpected counts, plus equal ordered epoch-union and member-union
digests projected once from the two campaigns and once from the generated schedule
entries. Runtime replay is authoritative for relationships JSON Schema cannot
express.

The structural contract is
[`schemas/campaign-background-schedule-set-manifest.schema.json`](schemas/campaign-background-schedule-set-manifest.schema.json).
The controller may project each manifest entry's `path`, `sha256`, `schedule_id`, and
`role` into its sealed configuration only after separate review. Materialization does
not edit controller configuration or start the campaign.

## Focused offline tests

```sh
python3 -B -m unittest acquisition.tests.test_materialize_campaign_schedule_set -v
python3 scripts/validate-json-contracts.py
```

The fixtures use tiny hot control files and a fake executable that leaves a marker if
invoked. They never access the network, GPU, or archive mount. They cover exact policy
values, official builder/writer delegation, ordered schedule and member coverage,
schema validation, replay, immutable modes, hash/mode/symlink/root refusal,
cross-campaign overlap, and cold-root substitution.
