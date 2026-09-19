# Gemini scheduling up to 100 active batches

Active service: `himr-cloud-gemini-20260913-hundred-v1.service`, started at
14:01:25 EDT on September 13 after the refined worker's cooperative stop and
the workspace-lock check. The independent cloud-transcription service was not
restarted. A [persistent-cache successor](GEMINI_PERSISTENT_CACHE.md) is staged
but has not replaced this running memory-cache worker.

Use `systemctl --user list-jobs` to distinguish a queued start from a running
worker, and `systemctl --user status himr-cloud-gemini-20260913-hundred-v1.service`
for the eventual service state. An inactive unit with a waiting start job is
not a startup failure.

Runtime, relative to `research/private-transcriptions/cloud-archive-20260913`:
`gemini-hundred-v1/runtime`.

Execution release: `gemini-hundred-v1/execution-release/release.json`, SHA-256
`814dedf125e5c61a7e0ed003aa69fd12cd1b86e88af13fcbc00cc17b8883765c`.

The original `summaries-v2/manifest.json`, record plans, submitted requests,
results and reservations remain in place. This runtime retains the
[incremental caches](GEMINI_INCREMENTAL_REUSE.md), parallel collection, and
[single-label normalization and diarization policy](DIARIZATION_REFINEMENT.md).
Do not edit the runtime or launch an older worker against the same state root.

## Admission and collection

- The target starts at 16. Two consecutive cycles with successful provider
  operations raise it to 32, then 64, then at most 100. Idle cycles do not grow it.
- A 429 halves the target and applies the existing cooldown of at least 60
  seconds, respecting Retry-After. Subsequent healthy recovery is additive by
  two, not doubling. Existing batches are never cancelled to lower occupancy.
- At most 32 new paid waves are attempted per cycle. Each requires its own
  unchanged token and budget checks and durable reservation before submission.
- Pending, ambiguous and orphan-reserved waves all count against the limit.
  Unknown submissions retain their holds and are never automatically reposted.
- The local queued input allowance remains 3,000,000. It is a conservative
  UTF-8-plus-framing allowance, not a measurement of the project's Google quota.
- The worker budget remains $119.040085; historical reservations bring the
  overall authorized Gemini ceiling to $150. More slots do not increase it.
- Four persistent spawned processes collect different recordings concurrently.
  The collector now accepts 100 distinct record groups per cycle, removing the
  previous 64-group guard. Paid submissions stay serialized in the parent.

The ceiling is not a promise of 100 occupied slots or proportional throughput.
The token allowance, spending reservations, eligible work and provider capacity
can bind first. The provider's concurrency limit is project-wide, so unrelated
batch workloads need their own headroom; this worker cannot inventory them.
Batch pricing, model, prompts, timestamp stripping and evidence are unchanged.
No standard-priority requests or automatic paid retries were introduced.

Operational arguments are `--max-active 100 --initial-active 16
--max-new-waves 32 --max-enqueued-tokens 3000000 --poll-workers 4`, with a
20-second interval between cycles and a finite 24-hour runtime. Keys are read
from the explicit repository `.env`, never passed as arguments. The service is
transient, not boot-enabled, and has no automatic restart or forced SIGKILL.

## Validation

175 targeted and existing tests passed in the versioned runtime, including
100-slot admission and collection, a 101st-job hold, ambiguous/orphan accounting,
budget and token gates, growth and 429 recovery, single-label projection,
request reuse, release binding, and safe process-pool draining. Real process
initializers also passed against the original manifest and new release without
provider calls. This was not a full-archive verification or a throughput test.
