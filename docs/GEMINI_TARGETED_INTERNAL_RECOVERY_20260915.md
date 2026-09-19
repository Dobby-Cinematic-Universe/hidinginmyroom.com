# Targeted Gemini INTERNAL-error recovery

The operator approved one additional attempt for **37 requests across 23
recordings** with collected Gemini `provider_error_code: 13` failures. Selection
was frozen at 2026-09-16 02:52:15 UTC (September 15, 22:52:15 EDT). It contains 28
chunk requests and 9 transcript-level requests. The 14 older validation/blocked
failures and five ambiguous/orphan submissions are **not** retry-authorized.

The immutable authority is private:
`research/private-transcriptions/cloud-archive-20260913/summaries-v2/targeted-internal-recovery-20260915/authority.json`.
It binds the existing worker, entries, plans, original waves, collections and
exact job bodies. It does not discover or approve future failures.

## Boundaries

- `pipeline/gemini_targeted_retry.py` permits only these exact failed jobs to
  reach attempt two. Failure or uncertainty on that attempt remains held; there
  is no third attempt, unknown-POST retry, provider switch or full-recording rerun.
- Original request/plan bytes remain unchanged, including their ordinary
  one-attempt policy. The hash-pinned extension supplies a narrowly scoped,
  in-memory validation override and checks every repeated job against the grant.
- Successful chunks, captures, paid receipts and original cost holds are reused.
  After recovered chunks complete, normal downstream transcript reducers can run.
- The same global pre-POST reservation ledger, $119.040085 worker allocation,
  $150 overall Gemini cap, 100 slots and 2,850,000-token queue ceiling apply.
  The retries reserve at most **$4.895629 additional**, conservatively. This is
  not an expected bill and does not include ordinary downstream reducers.
- Cached token counts total **178,508** for the selected retry inputs, with zero
  fallback counts. Retry recordings receive admission priority; submission still
  waits for available queue capacity. Google occupancy is not inferred from a
  local prepared request.
- Source selection, manual speaker-review decisions, short-transcript admission,
  stripped model inputs, internal evidence links and the model remain unchanged.

## Deployment and operation

`gemini_efficient_resume_runner.py` accepts the optional all-or-nothing flags
`--retry-manifest`, `--retry-manifest-sha256` and `--retry-sha256`. The service
`himr-cloud-gemini-20260913-selective-v1.service` was gracefully stopped and
started with those pins at **23:12:36 EDT on September 15**. Other pipeline
services were not restarted. The original execution release remains sealed.

The scheduler prepares the selected retry waves under its existing worker lock
and submits them through the normal paid submitter. Parallel collectors receive
the same hash-bound authority and reviewed-source adapter. Original initial chunks
seed their caches without duplicate coverage from retry waves. Only the 23
selected records require retry-aware graph inspection; completed archive exports
retain their existing receipt-based fast path.

The scheduler status file adds `targeted_recovery`: selected jobs/recordings,
completed, prepared, pending, ambiguous and failed-again job counts. Prepared
means local only; pending requires a provider submission receipt. Normal
`automatic_paid_retries` remains false: this is finite explicit recovery, not an
automatic retry policy. Future status/run/export operations on this worker must
retain the authority extension, including after recovery finishes. Do not roll
back to an unextended launcher once retry waves exist; its one-attempt guard
correctly rejects those waves.

## Checks

169 existing regression tests, 14 pinned-runtime scheduler integration tests and
13 targeted-retry tests passed. The latter cover partial-batch success reuse,
unchanged inputs/receipts, single extra attempts, rejected successful/unknown
jobs, lost POSTs, restart idempotence, budget/token caps, retained-cache reuse,
parallel collection and picklable collector initialization. Generation was mocked.

A read-only preflight of the 23 real selected records passed in 32.46 seconds,
with 23 retained initial-job cache hits and **zero fresh initial-job builds**.
A real spawned collector also loaded the pinned release, reviewed-source adapter
and recovery authority successfully, reused a selected record's original chunks,
and inspected its state with network access explicitly forbidden in the probe.
No full-archive transcript/media verification or regeneration was performed.
The private recovery directory retains the authority, previous service unit and
activation audit. Never delete a reservation, submission intent or provider
response to clear a recovery hold.

## First live cycle

The first cycle, including the retained-state load and retry preparation, took
572.022 seconds. **Nine retry requests were accepted**, while **28** remained
prepared in 14 local waves awaiting queue capacity. The queue reached **2,849,206
tokens** across 90 confirmed-pending batches and the unchanged five unresolved
holds. Three independent provider GETs reported the new batches RUNNING.

All 23 original grant bindings and their 44 successful sibling results remained
unchanged; the 37 prepared retry jobs were exact approved originals without
duplicates. There were no unresolved new retry submissions. One GET for an older
batch returned HTTP 503 and was retained for later polling; it did not interrupt
the controller or cause a paid resubmission. `live-verification.json` records this
snapshot, not a promise that provider processing is already complete.

The next warm cycle completed in **92.375 seconds**, with zero transport errors
and no duplicate submissions. A fresh spawned collector also replayed the real
two-attempt record successfully with zero network calls and zero fresh chunk
builds; the immutable plan still declares its original ordinary one-attempt rule.
