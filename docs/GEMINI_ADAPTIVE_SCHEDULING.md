# Adaptive Gemini scheduling and parallel collection

Current launch target: `himr-cloud-gemini-20260913-hundred-v1.service`, using the
[100-slot adaptive release](GEMINI_HUNDRED_SCHEDULING.md). Its start is queued
after the refined worker's cooperative stop. The 16-slot adaptive release below
is retained history; the successor raises the ceiling and accelerates healthy
growth while preserving token, budget and paid-state safeguards.

The versioned runtime is
`research/private-transcriptions/cloud-archive-20260913/summary-adaptive-v1/runtime`.
It uses the original `summaries-v2/manifest.json` and all existing record plans,
requests, receipts, reservations and cached-result policies. No source or paid
plan was resealed, and no shared pinned summary module was edited.

Execution release, relative to the campaign root:
`summary-adaptive-v1/execution-release/release.json`, SHA-256
`161e5b2959079e9627eb43ea8997f41d6aaf59e57f475a6a29b123bf05dba9a5`.

## Earlier 16-slot scheduling

- Starts at eight slots and increases by two after every two healthy cycles,
  up to sixteen. A 429 halves the target and applies an interruptible cooldown
  respecting Retry-After. Existing jobs are never cancelled to reduce the target.
- The operator ceiling is 3,000,000 queued input-token allowances. Allowances
  use the existing conservative UTF-8-plus-framing budgets. This is a local
  ceiling, not confirmation of the project's actual Google quota or other usage.
- Pending, ambiguous and orphan-reserved waves all consume slots and token
  allowances. An unknown paid request is not retried or silently removed.
- The original worker ceiling remains $119.040085 within the total $150 Gemini
  authorization including historical reservations. All reservations remain in
  the same ledger. Temporary reservation pressure keeps collection running;
  unresolved holds still require review. Oversized unreserved candidates do not
  prevent the worker from trying smaller recordings.

## Parallel collection

Four persistent, spawned processes handle existing paid waves for different
recordings. Waves belonging to one recording remain sequential. Each child
loads the same explicit release and cache scope, and reads the repository `.env`
locally; keys are never passed in task messages or command-line arguments.

Only GET polling and result collection run in these processes. The parent keeps
the global worker lock, drains all collection work, then makes budget decisions
and paid submissions serially. Fatal pool failures fully shut down workers before
the parent can release that lock. No automatic paid retry or pool restart occurs.

The service polls between cycles at 20-second intervals (actual cycle time also
includes local work), with a finite 24-hour deadline and cooperative shutdown.
It does not change model, prompts, thinking level, chunk boundaries, evidence,
timestamp stripping, anonymous-speaker holds, or Batch pricing. One-pass summaries
and standard/priority API inference were not enabled.

Logs expose `adaptive_concurrency`, `active_target_used`,
`enqueued_input_token_allowance`, `operator_enqueued_token_ceiling`,
`parallel_collection`, `collection_seconds`, `cycle_seconds`, and explicit
`admission_holds`. Existing request/record/export cache counters remain available.

Validation: 157 focused and existing tests passed, including real spawned worker
tests, 16-slot ramping, token limits before reservation, unknown paid holds,
temporary budget pressure, 429 backoff, oversized-record fairness and fatal pool
draining. Real initializer startup was also checked without provider calls.

The independent cloud transcription and archive-screening services are unchanged.
See [incremental record reuse](GEMINI_INCREMENTAL_REUSE.md) and
[cloud upload recovery](CLOUD_UPLOAD_RECOVERY.md).
