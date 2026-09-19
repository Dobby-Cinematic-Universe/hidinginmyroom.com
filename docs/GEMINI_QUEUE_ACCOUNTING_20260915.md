# Gemini queue accounting and repeated local work

The later, finite recovery of selected Google INTERNAL failures is documented in
[Targeted Gemini recovery](GEMINI_TARGETED_INTERNAL_RECOVERY_20260915.md). It uses
the same queue ceiling and financial ledger; it does not enable general retries.

## Current admission policy: Tier 1, 95% token target

After the operator confirmed Tier 1 and approved using more of its batch capacity,
the opt-in `--queue-policy tier1-95-percent` policy uses cached Google `countTokens`
results without the old per-request 10% + 128-token padding. Its admission ceiling
is **2,850,000 tokens**, retaining a single **150,000-token (5%) reserve** below the
3,000,000 Tier 1 batch limit. The scheduler refuses a higher ceiling in this mode.
If the launcher receives no explicit token ceiling for `run`/`cycle`, it supplies
2,850,000. Legacy mode remains available and preserves the original calculation.

Unresolved submissions still consume their full counted input tokens and active
slots. Missing or invalid counts still fall back to the original conservative
financial input allowance. The count cache identity is unchanged; changing the
admission policy does not recount prompts, change paid requests, retry ambiguous
submissions, or reduce dollar reservations. The 100-batch limit, $119.040085 worker
allocation, and $150 overall Gemini spending cap are unchanged.

Live status now separates tokenizer-counted inputs, fallback allowances,
per-request padding, confirmed-pending tokens, unresolved-submission tokens, and
the operator reserve. These remain **local admission estimates**, not a live
Google quota meter. The dashboard's 28-day peak is also not current occupancy.
Google's RPM/TPM/RPD chart is separate from the Batch API queue/concurrency chart.
See [Google's batch limits](https://ai.google.dev/gemini-api/docs/rate-limits#batch-api-rate-limits).

The earlier accounting policy and its initial activation measurements are retained
below as historical evidence; their 2,958,213 figure includes padding and must not
be presented as Google-measured usage. The follow-up activation audit is under
`summaries-v2/scheduler-efficiency-v1/tier1-95-percent-v2/`.

The follow-up controller started at **15:59:41 EDT on September 15** after a
graceful stop. Its first cycle took 458.589 seconds, including the one-time
in-memory state load, and accepted **two additional batches (29 requests)**.
The queue reached **2,815,715 counted tokens**: 2,780,159 in 80 confirmed-pending
batches and 35,556 retained for the five unresolved submissions. No additional
per-request padding or fallback counts were used. The remaining 34,285 tokens
below the 2,850,000 target did not fit another selected whole batch.

169 focused/regression tests and 14 integration tests against the pinned runtime
passed. Independent Google GETs verified the two additional submission receipts.
All 2,841 pre-activation reservations, 83 active wave requests, and 78 existing
submission receipts were checked unchanged. The first activation cycle reported
no transport errors or rate limits. No original paid request was resubmitted.
These are activation measurements, not permanently current queue statistics.

## Original morning activation

The existing `himr-cloud-gemini-20260913-selective-v1.service` was gracefully
stopped and restarted with `pipeline/gemini_efficient_resume_runner.py` on
September 15 at 10:38 EDT. Its original conservative runtime, execution release,
summary manifest, job plans, model requests, reservations, and paid receipts
remain unchanged. Rev AI recovery and the review feed were not restarted.

## Queue tokens are not financial allowances

The old queue calculation used `job.budget.input_token_allowance`, a UTF-8-byte
ceiling plus 4,096 framing tokens intended for conservative spending guards.
This substantially overstated queue occupancy. Immediately before the restart,
49 pending batches and five ambiguous/orphan holds occupied **2,993,719** of the
3,000,000-token operator ceiling under that accounting.

`pipeline/gemini_queue_tokens.py` now uses Google's
[countTokens endpoint](https://ai.google.dev/api/tokens) on the complete
`generateContentRequest`, including system instructions and the output schema.
Only the non-prompt `store: false` transport control is omitted. No transcript
text, summary prompt, evidence ID, model, or paid request body is rewritten.
The client permits only this counting endpoint, not generation or cancellation.

Each request's scheduling estimate is its returned input count plus 10% and
128 tokens of headroom. This is an estimate, not an exact quota guarantee.
Missing, invalid, corrupt-cache, or unavailable counts retain the full original
allowance; they never become zero. Counting uses at most eight concurrent requests
and backs off after failures/rate limiting. It does not retry paid generation.

Counts are private disposable sidecars under
`summaries-v2/scheduler-efficiency-v1/queue-tokens-v1/`, keyed by model and exact
count-request digest. They survive restarts. Changed text, system instructions,
schema, or model requires a different count. Only active, unresolved, and prepared
work is counted; the completed archive is not submitted for recounting.

The preflight cached all 720 requests in 109 uncollected waves with zero counting
failures and zero new generation requests. Of those, the 121 requests in the
49 pending plus five unknown batches occupy **650,620 estimated queue tokens**,
including headroom, instead of 2,993,719. Prepared but unsubmitted requests do not
occupy the active queue. These are a before-admission snapshot, not a permanent
queue size; subsequent new submissions increase it.

The original dollar reservations, cached-token usage accountant, $119.040085
worker allocation, and overall $150 Gemini cap are not reduced or reset.
Ambiguous/orphan requests still consume active slots, estimated input tokens,
and their original financial holds. The 3,000,000-token operator ceiling and
100-batch maximum remain. The restart retains the previously reached 100-slot
target; subsequent provider 429s still reduce that target. Actual account quota
and other project usage are not claimed to have been verified.

## Local processing

`pipeline/gemini_scheduler_efficiency.py` replaces only controller scheduling:

- Preferred-source selection and speaker/short-input admission run once per
  cycle, reused by its internal snapshots. They refresh on the next cycle so
  newly completed transcripts and saved reviews remain visible. Sealed revisions
  and the existing anonymous-speaker/50-word policies remain authoritative.
- Pending records with no ready or prepared work do not invoke `prepare_plan`
  simply to rediscover that their dependencies are still remote.
- A sealed prepared wave is read and checked against its validated snapshot and
  content-derived identity. Admission does not reconstruct its plan/dependency
  graph again. The original paid submitter still performs full request and
  dependency validation before its one POST.
- A waiting cycle takes two snapshots, not four. Additional snapshots occur
  only when records/waves/reservations change. Collection always gets a fresh
  snapshot afterward, including when a GET/collector returns an error.
- Full cycle status is atomically published to
  `summaries-v2/scheduler-efficiency-v1/status.json`, avoiding dependence on split
  journal lines. It reports queue estimate, original financial token allowance,
  count-cache statistics, selection/snapshot counts, skipped preparations, sealed
  wave reuse, collection time, and total cycle time.

Before the change, the two last measured cycles took about 335 seconds each,
with only 11 seconds in remote collection. This is the baseline, not a claimed
new throughput measurement. A one-time retained-state load is still needed after
restart; it preserves the existing completed-export and metadata-cache reuse
instead of replaying completed summary dependency graphs. Google processing time
remains independent of local scheduler speed.

## Tests and recovery evidence

151 focused/regression tests passed, plus 12 selected integration tests executed
against the exact retained runtime with the extension installed. These include
chunk/reducer completion, no-repeat restart, pending-wave limits, financial caps,
unknown/orphan holds, tampered ledgers, changed sources, model-input stripping,
count-cache restart/mutation/failure handling, and avoiding pending graph rebuilds.
All generation in those tests used mocks. The real token-count preflight did not
purchase summaries.

The private scheduler directory contains `baseline.json`, `previous-service.unit`,
and `activation.json`. The baseline binds all preexisting global reservations;
the activation binds the new launcher/extensions and old/new service settings.
The original runtime/release remain sealed. Do not delete paid reservations,
intents, or responses when clearing a disposable token cache or rolling back.
Never run the old and new controller concurrently on the same worker root.

## Live verification

The first cycle included the one-time retained-state load and 30 newly accepted
batches. It completed successfully with 79 remote batches pending, plus the
unchanged five ambiguous/orphan holds. The active queue then held 461 requests
at 2,958,213 estimated input tokens, including headroom. Their unchanged
financial input allowances sum to 12,841,669; those byte-based allowances no
longer incorrectly prevent this work from entering the queue.

The next warm waiting cycle took **85.140 seconds**, including **30.820 seconds**
in collection. That leaves **54.320 seconds** of local/controller work, versus
about **324.399 seconds** locally in the 335.210-second pre-change cycle. This
observed cycle was approximately four times faster overall and six times faster
locally, while polling 79 batches instead of 49. It selected/gated sources once,
took two snapshots, skipped preparation of all 79 pending recordings, and reused
16 sealed candidate waves. No new generation requests were made in that warm
cycle, and no paid batch was resubmitted. Provider completion latency remains
independent of this measured controller improvement.

All 2,807 pre-handover reservation bindings and the original worker manifest
were checked unchanged. The new live reservations remain in that same ledger
and under its original dollar cap. `first-live-cycle.json` retains the first
saved **warm-cycle measurement**, not the initial cold-start timing.
