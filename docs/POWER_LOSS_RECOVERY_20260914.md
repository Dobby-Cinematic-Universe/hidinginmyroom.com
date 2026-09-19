# Power-loss recovery, 2026-09-14

Targeted recovery only; no raw archive verification or retranscription sweep.
The archive was mounted at its original path with approximately 4.4 TiB free.

Durable pre-restart state:

- Cloud transcription: 519 completions, 34 provider collection review holds,
  two isolated media holds, no submitted-uncollected jobs. The last cloud worker
  finished `cloud_complete_with_review_holds`; it was not restarted unnecessarily.
- Gemini: 1,069 retained summary exports, one submitted wave without collection,
  four submit intents without submission receipts. Ambiguous intents and budget
  reservations were preserved, not retried or released.
- Speaker review decisions were preserved at their original paths.

Restored persistent user service definitions (started manually, not boot-enabled):

- `himr-cloud-gemini-20260913-selective-v1.service`: original conservative runtime,
  release, manifest, budget and 100-slot ceiling. Separately hash-pinned
  `pipeline/short_summary_admission_runner.py` prevents new work on transcripts
  below 50 words while allowing collection of already-paid waves. No retained
  runtime files were edited.
- `himr-speaker-review-ui-20260914.service`: loopback port 8766.
- `himr-summary-length-filter-20260914.service`: independent 60-second consumption
  index. Initial recovered index: 989 eligible exports; 80 short exported records
  withheld without deleting originals, 89 short planned sources in total.

The two media holds remain isolated: `cloudjob_005541298d49c9c37bbdc7043b3c8032`
has a decoded duration roughly 2.63 seconds short but only a 20 ms container audio
offset; `cloudjob_2b62407493b0a525b34e2b066b7405fc` has an audio stream approximately
19.4 seconds shorter than its video. Neither qualifies for the existing narrow
start-offset repair. No silence padding or replacement receipt was fabricated.

Tests: 16 unit tests covering admission boundaries, validation preservation,
output filtering, media isolation and review server passed. UI returned HTTP 200.
Gemini startup rebuilds process-local state; a running service is not proof that
its first collection/admission cycle has completed. Consult its current journal
for cycle progress and budget pauses. Existing ambiguous paid records remain held.
