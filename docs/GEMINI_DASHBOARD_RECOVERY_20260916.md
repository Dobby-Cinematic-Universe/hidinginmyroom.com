# Dashboard-managed Gemini spending and targeted recovery

The operator requested removal of the local spend limit on September 16, 2026,
after setting a provider-dashboard cap. This applies to the active Gemini
transcript-summary worker, not AssemblyAI, Rev.ai or Anthropic authorizations.
The last operator-reported Google spend was **$43.85**; it is not a measurement
from the local estimator or an independently verified dashboard balance.

## Spending policy

`pipeline/gemini_dashboard_spend.py` is an explicitly hash-pinned adapter for
the existing immutable worker. The launcher requires `--spend-policy
google-dashboard` and `--spend-policy-sha256`. It removes dollar-based blocking
from snapshot validation, candidate holds, admission, retained reservation replay
and submission. It does not substitute an artificially large budget or zero
the accounting data. The original manifests and requests remain unchanged.

Status reports `local_spend_limit_enforced: false`, the effective maximum as
`null`, and the original amount as `historical_worker_budget_microusd`.
Usage estimates, unsettled reservations and original paid receipts remain
available for accounting. They are not claimed to match Google's invoice.
The old $150 campaign budget and $119.040085 worker allocation no longer stop
this Gemini worker. The dashboard cap itself has not been independently verified.

The unchanged safeguards include 100 concurrent slots, a 2,850,000-token queue,
429 backoff, finite request/attempt limits, source and evidence validation,
stripped model input, short-transcript withholding, and durable intent-before-POST
deduplication. The adapter also reaches spawned collection workers. Original
validation functions are adapted at exact, checked dollar-guard sites; changes
to those sites fail activation closed rather than bypassing a larger validator.

Google warns that experimental project-cap enforcement has latency and batch
jobs can exceed the cap. A cap is not a guarantee against any overspend:
[official Gemini billing documentation](https://ai.google.dev/gemini-api/docs/billing#project-spend-caps).

## Finite recovery

The new private authority is
`research/private-transcriptions/cloud-archive-20260913/summaries-v2/dashboard-recovery-20260916/authority.json`.
It preserves the previous 37-job authority and all successful results, while
selecting **401 currently failed requests** for another attempt: 399 confirmed
INTERNAL errors and two invalid-evidence outputs. Only the 14 jobs with an exact
second-attempt failure proof may reach attempt three; other selected failed
jobs may reach attempt two. No fourth attempt or automatic discovery of future
failures is authorized. The authority contains 424 unique retry jobs because it
also retains 23 already recovered jobs; those 23 are not submitted again.

Twelve additional jobs are recoverable without an API call. The model put text
in the uncertainty section but used a less cautious classification. An explicit
repair grant permits only changing that tag to `uncertainty`. Model text and
evidence links stay intact, and the strict result validator must accept the
corrected result. Original captures and failed collections remain unchanged;
the authority reproduces the correction during dependency-graph replay and
subsequent export. This is not semantic fact-checking or identity verification.

Nine Google `PROHIBITED_CONTENT` outcomes are excluded from paid recovery. There
is no attempt to weaken safety settings or bypass the provider's policy.

## Unknown submissions and stopping

A read-only listing checked 4,142 provider batch operations. Two previously
unknown waves had unique matching names and were adopted through the existing
strict reconciliation path: one completed result was collected and one batch
was still running at Google. Six unmatched submissions remain held: absence
from a batch listing alone does not prove a paid POST was never accepted.
Neither their reservations nor intents are deleted to force resubmission.

The scheduler's graceful-stop boundary now checks for stopping **before** writing
a global reservation. Once a reservation is durable, it finishes the existing
intent/submission boundary. This prevents a graceful stop from creating an
orphan reservation between those two operations. Uncertain network outcomes
still remain held rather than automatically reposted.

## Verification and operation

The private recovery directory preserves the old service unit, paused status,
selection report, spawned-collector smoke test, targeted preflight and
reconciliation receipts. Only affected records receive dependency-graph checks;
there is no full-archive media verification or transcript rebuild.

The targeted preflight checked 115 affected records in 241.59 seconds, reused
115 retained initial-job caches, and performed **zero fresh initial-job builds**.
It replayed all 12 local output repairs. A real spawned collector also replayed
a repaired record with network access forbidden. Regression runs passed: 173
baseline tests and 67 focused pinned-runtime tests (the scheduler tests are
included in both runs).

The same service is used:
`himr-cloud-gemini-20260913-selective-v1.service`.
It restarted successfully at **11:16:05 EDT on September 16**. The speaker
review UI, reviewed-source feed and short-summary filter were not restarted.
During startup, the scheduler status file still describes the last finished
cycle (which can say `paused`). Service liveness and the small
`recovery_inspection_progress` journal events distinguish that retained snapshot
from the running process. The status file changes after the first cycle.
Keep the new authority and both policy/module pins on subsequent starts, since
restoring the old unextended one-attempt runtime would reject recovered records.
Do not cancel Google's accepted batches or delete journals to restart locally.

## First live cycle

The recovery startup and first cycle took **1,615.429 seconds (26m 55s)**.
Google accepted **32 new batches** with **zero transport errors**. Of the 401
selected failed requests, 134 were pending at Google and 267 remained prepared.
All 12 local classification repairs replayed successfully. The 23 previously
recovered requests remained complete and were not resubmitted.

The status now reports `waiting_remote`, `local_spend_limit_enforced: false`
and `max_total_budget_microusd: null`. The local estimate plus conservative holds
was $122.487778, above the historical $119.040085 allocation without blocking
admission; that figure is **not** a Google bill. Queue use was 847,709 estimated
tokens across 33 confirmed pending waves plus six unresolved holds.

The live request audit verified all 95 new retry waves/401 jobs against the exact
approved originals, with zero successful jobs resubmitted. Three independent
provider GETs confirmed new batches were running. The private directory retains
`first-live-cycle-status.json`, `live-request-audit.json`,
`first-provider-confirmation.json` and `live-verification.json`.

## Faster queue refill

On September 16 the operator approved faster refilling within the existing
provider limits. The service now uses `--max-new-waves 100` (previously 32)
and `--poll-seconds 10` (previously 20). These are existing supported settings;
the worker, scheduler, recovery authority and implementation hashes are unchanged.

The scheduler already collects remote completions, refreshes their state and
submits newly ready work in the **same cycle**. Raising the submission cap lets
that existing refill pass use available slots instead of stopping at 32 requests.
It does not add 100 requests regardless of occupancy: confirmed pending batches
and uncertain reservations still count toward the 100-slot ceiling, and the
2,850,000-token ceiling, 429 cooldown and all deduplication rules remain enforced.
Ten seconds is the idle delay **between cycles**, not a promise of a ten-second
collection cadence; local processing and sequential submission still take time.

Deployment uses a graceful stop and restart of only the Gemini worker. Already
accepted Google batches are not cancelled, and no reservations, intents or
results are removed. Cold local caches must warm again on restart; the last
status snapshot may say `paused` until the first new cycle completes. The review
UI, source-feed and short-summary-filter services are not restarted.

Regression coverage explicitly checks same-cycle refill after collection,
preservation of occupied slots with a 100-request refill allowance, and rejection
of allowances above 100, for both queue-accounting policies.

Deployment verification: 73 focused scheduler, dashboard-spending and targeted
retry tests passed; `systemd-analyze --user verify` accepted the unit. The old
worker exited cleanly, and the replacement started at **17:45:12 EDT** on
September 16. Its retained shutdown snapshot had 2,343 eligible summaries
complete and nine confirmed pending batches plus six uncertain reservations.
This is a pre-warm-up snapshot, not a measurement of the new refill throughput.
