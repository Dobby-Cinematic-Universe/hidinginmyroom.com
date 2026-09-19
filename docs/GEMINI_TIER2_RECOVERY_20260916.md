# Tier 2 queue and held-request recovery

The operator reported a Gemini batch quota of 100 concurrent batches and
400,000,000 enqueued tokens, and approved the 380,000,000-token working ceiling
plus recovery of held work. This quota is operator-reported, not independently
verified through the provider account. No other service's budget is changed.

## Queue configuration

The hash-pinned launcher supports `--queue-policy tier2-95-percent`. This uses
the same countTokens cache and request counting as Tier 1, with a process-local
400M admission maximum and a 380M working ceiling. Status identifies the active
operator-declared tier, provider limit and 20M-token reserve. The legacy
`tier1_batch_token_limit` field remains the historical Tier 1 reference, not the
active Tier 2 limit. Tier 1 remains the default and retains its 2.85M ceiling.

The active service retains 100 concurrent slots, 100 submissions per cycle,
10-second idle polling, rate-limit cooldown, immutable financial accounting,
dashboard-managed spending, strict output validation and durable submission
intents. Raising the token ceiling does not increase the 100-slot ceiling or
remove sequential local-processing costs.

## Held work

Private receipts are under
`research/private-transcriptions/cloud-archive-20260913/summaries-v2/tier2-recovery-20260916/`.
The deployed successor authority is **authority-v2.json**. It preserves all
424 previous retry grants, including third-attempt failure proofs, and all
12 previous classification repairs. No successful requests gain resubmission
authority. It adds:

- **24 exact failed requests**: 22 absent results from a terminal Gemini batch
  with INTERNAL/code 13, one invalid-text output and one invalid-evidence output.
  A missing result alone is insufficient: the batch must be explicitly done,
  failed and have integer error code 13. Original captures stay unchanged.
- **Six classification-only repairs** across five recordings. The original
  wording and evidence are preserved; only an uncertainty-section classification
  is changed to `uncertainty`, and the strict validator must accept the result.
  One repair is on a previously authorized retry wave. Such a repair requires
  that wave and its jobs to match the existing exact retry grant; it does not
  authorize another paid attempt.

The authority therefore has 448 retry-job grants and 18 repair grants in total;
these include historic successes and must not be reported as 448 new retries.
The finite maximum remains three attempts, with a bound failed second attempt
required for any third attempt. Future failures are not automatically admitted.

A fresh read-only listing checked **5,721 provider operations**. None matched
the six unresolved reservations. Five have durable POST intents and remain
held: absence from a listing is not evidence that a POST was never accepted.
The sixth had only `wave.json` and `requests.bin`, and no submission intent,
receipt or capture. Under the stopped worker's lock, its exact existing global
reservation and source eligibility were checked. The original submission was
then completed without removing or replacing its reservation. Google accepted
it as `batches/si664gsur74ecyvq9fh1tg663jtadmx0c9fb`.

**Fourteen provider-blocked requests across thirteen recordings** remain
withheld, with no weakened safety settings or attempted policy bypass. An older
INTERNAL error never overrides a later provider block.

## Verification and operation

107 focused tests passed, including both queue tiers, hard ceilings, occupied
slots, unknown holds, retry proof preservation, classification-only repair of
an authorized retry, and rejection of missing-result recovery without a terminal
INTERNAL failure. The unit passed `systemd-analyze --user verify`.

Targeted live preflight replayed only the **21 failed records**, not the full
archive or media. All six new local repairs replayed successfully, and the 24
new retry jobs were identified as unprepared for the scheduler to admit after
restart. The prior worker exited gracefully; no accepted batches were cancelled.
The review UI, reviewed-source feed and short-summary filter were not restarted.

The first status snapshot after restart can remain `paused` while the local
caches warm. Do not remove the new authority or change pinned modules underneath
the running worker. Preserve the five uncertain POSTs unless an exact provider
receipt or other conclusive reconciliation evidence becomes available.
