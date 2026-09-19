# Gemini recovery and cached-token accounting

The September 15 [queue-accounting and scheduling correction](GEMINI_QUEUE_ACCOUNTING_20260915.md)
builds on this recovery. Its separate hash-pinned launcher keeps the cached-token
financial accountant described here unchanged while correcting queue occupancy
and repeated local planning.

The original conservative runtime, release, manifest, job plans and paid receipts
are unchanged. `pipeline/gemini_resume_runner.py` installs separately hash-pinned
`gemini_recovery_extension.py` and the existing 50-word admission extension.

## Accounting

The previous accountant retained a full maximum-cost reservation whenever a
completed response reported cached input tokens. Valid cached tokens are part of
the reported prompt token count. The recovery accountant therefore accepts a
nonnegative integer cached count no greater than the prompt count, and accounts
for the entire prompt at the existing normal input rate. It deliberately does
not claim a cache discount. This is a conservative usage estimate, not a billing
invoice or a forced match to the user's $24.89 dashboard figure.

Missing/inconsistent usage, unsupported models and tool usage remain held.
Token/cost allowance violations still fail. Pending batches, ambiguous submission
intents and orphan reservations retain their original safeguards. Neither old
reservations nor receipts are deleted. The current worker's $119.040085 allocation
remains unchanged; earlier allocations account for the remainder of the $150 cap.

Reference: [Google token accounting documentation](https://ai.google.dev/gemini-api/docs/generate-content/tokens).

## Startup and completed-record reuse

A live stack sample showed completed summary jobs being reconstructed in
`load_state`/`validate_job`, even in the final snapshot before the first cycle log.
A second sample showed completed cloud source refresh repeatedly loading large
historical screening manifests. CPU activity alone had obscured these delays.

The extension changes these two recovery boundaries:

- Preferred transcript selection consumes the original plan and bound completion
  receipts, checking completed transcript identity and receipt links. It does not
  rerun historical acoustic screening. Normal source/provider validation still
  applies when sources enter unfinished/new summary work.
- Fully collected successful records with one content-addressed final export can
  reuse that export. Plan/request identity, wave identities/order, input hashes,
  cost sums, submission receipts, capture hashes, job coverage and final-result
  equality are checked. Completed dependency graphs are not regenerated and
  evidence semantics are not reverified. Missing/unfinished/failed collections
  fall back to the original inspector; inconsistent retained bindings fail.

Record caches retain their bounded metadata witnesses. Changed records are
reinspected; unfinished job validation has an additional bounded, content-keyed
memory cache with isolated return values. No transcript or summary is edited.
Recovery emits progress every 25 inspected records, instead of remaining silent
until an entire cycle completes. Concurrent cloud receipt changes remain visible
on the next source refresh. Review UI and the output filter run independently.

Tests cover cached usage, invalid usage, allowance overruns, memoization mutation
isolation, equivalence with full accounting on an offline completed fixture,
unfinished-collection fallback, and corrupt capture/export/request/intent rejection. Forty-two tests
passed across recovery, short-summary admission/filtering and campaign regression.

The old worker did not finish its graceful stop while replaying records, so it
was force-stopped before recovery. Durable paid state was preserved. The new
launcher is never run as a paid worker concurrently with the old process.

## Reconciliation before restart

The offline status run completed in approximately 4 minutes 45 seconds:

- 1,179 plans; 1,069 complete, 95 prepared, 10 needs-review, five reconciliation holds.
- 2,940 preferred transcripts; 989 eligible completed summaries, 251 short sources
  withheld across the available archive and 42 additional identity holds.
- No pending remote waves remained. The old worker had collected its pending
  batch before it was stopped, despite not yet having emitted a cycle log.
- Current-worker estimated usage: $22.131331; remaining holds: $1.462890;
  total accounted: $23.594221 against the unchanged $119.040085 allocation.

The user's Google dashboard showed $24.89. That billing figure is not overwritten
or claimed to equal the current-worker-only estimate. Earlier campaign usage and
discounts are outside this reconciliation. Approximately $95.45 of this worker's
allocation became available without increasing the overall cap. No paid requests
were made by the offline status run. The service was then started with paid
submission enabled and the same concurrency/token ceilings as before.

Live verification: the restarted service passed the initial inspection at about
4 minutes 45 seconds and reached paid admission within roughly six minutes.
Confirmed submission receipts increased from 2,155 to 2,157, with 2,155 collected
receipts at that check: two new batches accepted, not duplicate retries. The
service remained active; review UI returned HTTP 200. Future submissions will
increase reservations beyond the pre-restart reconciliation totals above.
