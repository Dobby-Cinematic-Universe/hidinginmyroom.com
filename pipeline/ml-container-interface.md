# Future Python 3.12 ML container interface

This is an interface reservation, not a runnable image definition. It prevents later
ASR, alignment, diarization, OCR, face tracking, and active-speaker implementations
from coupling themselves to workstation paths or mutating preprocessing output.

## Image and dependency policy

- The runtime will use Python 3.12.
- Production invocations must use an immutable image digest, not a floating tag.
- Python dependencies and system packages must be locked with hashes.
- Each model must be registered separately with task, upstream name, exact revision,
  weight SHA-256, license label, and configuration. Models are not baked into the image.
- Building the production image and admitting production model weights remain future
  operations under this container contract. The separate `pipeline/gpu/` lane now
  seals one public tiny-model snapshot outside Git and runs only a synthetic,
  bubblewrap-isolated RTX 3050 readiness smoke; it is not the production container,
  a corpus work-order adapter, or backfill authorization.

## Mount and process contract

The eventual container receives exactly three mounts:

| Container path | Mode       | Contents                                                 |
| -------------- | ---------- | -------------------------------------------------------- |
| `/input`       | read-only  | One completed preprocessing run, including `result.json` |
| `/output`      | read-write | An explicit durable output root for the ML run           |
| `/models`      | read-only  | Locally provisioned, checksummed registered weights      |

The container runs with networking disabled. A representative future invocation is:

```sh
podman run --rm --network none --cpus 8 --memory 20g \
  -e HIMR_INPUT_RESULT=/input/result.json \
  -e HIMR_OUTPUT_ROOT=/output \
  -e HIMR_MODEL_ROOT=/models \
  -e HIMR_THREADS=8 \
  -v "$PREPROCESS_RUN:/input:ro,Z" \
  -v "$ML_OUTPUT_ROOT:/output:rw,Z" \
  -v "$MODEL_ROOT:/models:ro,Z" \
  IMAGE_NAME@sha256:IMAGE_DIGEST run --work-order /input/ml-work-order.json
```

`IMAGE_NAME`, `IMAGE_DIGEST`, and the ML work-order schema intentionally remain unset
until the pilot benchmarks select implementations. No command should be copied into
production with those placeholders unresolved.

## Input and output rules

1. Validate preprocessing schema version, artifact hashes, and source-media ID before
   inference.
2. Read the normalized FLAC for audio tasks and the CFR proxy for visual tasks unless a
   reviewed exception explicitly calls for the original.
3. Treat `/input` and `/models` as immutable. Never update caches inside either mount.
4. Write an atomic JSON result envelope beneath `/output`, content-addressed by input
   artifact hash, task recipe, model revision, and image digest.
5. Include parent processing-run ID, input artifact IDs, model ID, parameters, random
   seed where applicable, calibrated and raw scores, timestamps in integer
   milliseconds, quality flags, commands, and environment details.
6. Emit anonymous speaker, face-track, and voice-cluster IDs. Identity assertions are
   separate human-reviewed catalog records; biometric embeddings remain private.
7. Never update wiki pages or public release data directly.

The normalized SQLite catalog imports a completed ML envelope through the same pattern
as preprocessing: validate first, assign catalog IDs where required, then transactionally
insert the processing run, inputs, artifacts, and observations.
