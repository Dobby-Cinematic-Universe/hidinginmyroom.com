# Preprocess → GPU ASR queue v1

`preprocess-gpu-asr-queue` creates the deterministic, private handoff between
completed preprocess receipts and the portable GPU v5 lane. It performs no ASR,
chunking, catalogue import, publication, identity work, or GPU execution.

## Inputs

- One immutable preprocess bundle and its completed receipt state root. Receipt,
  result, normalized-audio, and private-handling validation is imported directly
  from `preprocess_asr_queue_v03`; this lane does not maintain a second replay
  implementation.
- One canonical `production_profile_v2` document. It must be a single-link file
  owned by the current user with mode `0400`, or owned by root with mode `0444`.
- One externally SHA-256-anchored portable-root registration for the
  `hot_main_drive` tier. The bundle, receipt state, profile, normalized audio,
  result envelopes, receipts, and pre-created queue root must be descendants of
  that registered Btrfs root. The registration document uses the same exact
  current-user/`0400` or root/`0444` owner-mode policy. The queue root must be
  private mode `0700`.

The materializer accepts at most 128 completed receipts and does not glob for
inputs or query a catalogue.

## Dispositions

Every replayed receipt appears exactly once:

- Audio within the profile's byte and duration ceilings is `ready`.
- Audio exceeding either ceiling is retained as `requires_chunking`, with exact
  reasons and `chunk_plan: null`. A separate admitted contract must create a
  chunk plan.
- A receipt with no normalized audio is retained in `explicit_skips`, including
  the v03 reason and full lineage.

Duplicate receipt paths/IDs/hashes and duplicate audio paths/artifact IDs/content
hashes fail closed. Every preprocess private-handling descriptor—including one
belonging to an explicit skip—is replayed and carried forward.

## Output and replay

The canonical manifest is sealed at:

```text
<queue-root>/queues/gpuasrqueue_<identity>/manifest.json
```

The manifest is mode `0400`; its deterministic directory is mode `0500`. A
single-link mode-`0600` writer lock is acquired with nonblocking `flock`. An
existing byte-identical queue is reused; a conflicting deterministic path fails.
No device number, inode, mount ID, or timestamp is durable queue authority.

Each member carries the exact audio path/hash/bytes/duration/format, retained
sealed mode (`0400` or `0444`), and artifact, preprocess-run, bundle, result,
source, and receipt lineage. The audio descriptor stays retained while the
member and queue identities are constructed and is rechecked before release.
Consumers must independently replay the portable-root registration, production
profile, manifest, input hash, and handling boundary. Only `ready` members can
be proposed to GPU v5 admission.

## CLI

```sh
pipeline/bin/preprocess-gpu-asr-queue materialize \
  --preprocess-bundle /absolute/hot/preprocess/bundles/ppbundle_ID \
  --preprocess-state-root /absolute/hot/preprocess-state \
  --queue-root /absolute/hot/gpu-handoff \
  --production-profile /absolute/hot/control/production-profile-v2.json \
  --root-registration /absolute/control/hot-root-registration.json \
  --root-registration-sha256 LOWERCASE_SHA256

pipeline/bin/preprocess-gpu-asr-queue validate \
  --manifest /absolute/hot/gpu-handoff/queues/gpuasrqueue_ID/manifest.json \
  --root-registration /absolute/control/hot-root-registration.json \
  --root-registration-sha256 LOWERCASE_SHA256
```

Validation requires the external registration digest again and replays all
receipts, handling boundaries, the production profile, every input placement,
and the deterministic sealed layout.
