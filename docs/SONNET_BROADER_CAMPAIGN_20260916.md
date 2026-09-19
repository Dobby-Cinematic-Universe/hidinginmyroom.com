# Claude broader summaries — 2026-09-16

Operator approved monthly summaries, yearly summaries, and an archive overview,
with undated recordings separate and the existing $150 Anthropic cap retained.

## Inputs and scope

The immutable selection contains 2,684 physical recordings: 2,396 dated recordings
across 125 months, and 288 undated recordings. It selects the preferred completed
cloud/third-party transcript summaries, including the 18 integrated recoveries.
Thirty-six older duplicate summary revisions are excluded. One still-pending
transcript summary is explicitly excluded from this snapshot; it is not silently
counted as summarized. Short and review-ineligible recordings remain excluded.
Historical local ASR is not used.

Outputs: 125 dated monthly summaries, one separate undated summary, 12 yearly
summaries, and one selected-archive overview (139 final scopes). Large months and
the undated group use intermediate reductions without truncating input parents.
The overview may discuss undated material but must not give it invented dates.
Retained date metadata is source/report chronology, not verified event chronology.
Conflicting dates remain undated.

Only completed summary text and relevant title/date context are sent to Claude.
No raw transcript, word/segment timestamps, or local paths enter model requests.
Internal evidence follows original summary items through every reduction; reader
output links to transcript-level text, without timestamp citations. Source
transcripts are copied to the private reader bundle only when cited, not sent to
the model. Canonical inputs and transcripts are not modified.

## Run and restart safety

Implementation: `pipeline/sonnet_broader_selection.py` and
`pipeline/sonnet_broader_campaign.py`. Uses the existing Sonnet batch client/model
profile and `.env` credentials. There are two active batch slots, each with up to
16 requests. Monthly results unlock yearly synthesis; all required period results
unlock the overview. Failed individual requests are held without discarding
successful siblings. There are no automatic paid retries or provider fallbacks.

Durable submission intents precede each POST. An ambiguous attempt is held for
reconciliation, never automatically resubmitted. Implementation, selection, wire
requests, jobs, and captured results are bound to their retained content. Restart
the same manifest without editing its pinned implementation. A held prerequisite
blocks only its dependent scopes. Budget holds stop new spending.

Campaign budget is $149.881665 after reserving $0.118335 for the previous Claude
recovery. Reservations are deliberately conservative and are not actual billing.
The 198 initial monthly-level requests reserve at most $24.382254 under the
existing profile; upper-level work is reserved as it becomes ready. This is not a
complete-run quote or a replacement for the provider's billing dashboard.

Service: `himr-sonnet-broader-20260916.service` (user service, finite 24-hour runner).
The Gemini service is unchanged.

Root: `research/private-summaries/sonnet-broader-20260916/`

- `manifest.json`: SHA-256 `824f37aac9bb3626808a5120201f9d4b08c18bb9750452f7b294b51fd3a13170`
- `status.json`: latest scope progress and reserved allowance.
- `reader/index.json`: completed broader summaries and per-transcript links.
- `reader/transcripts/`: linked transcript text, without timestamps.
- `exports/`: canonical results retaining evidence lineage.
- `jobs/`, `batches/`: sealed requests, receipts, captures, and outcomes.

Selection: `research/private-summaries/sonnet-broader-20260916-selection/selection.json`,
SHA-256 `95261568beebcd1db4df0694d33fc0f318cdb9a39f561dd6ee2bd72e89809d60`.

Verification: 12 focused campaign tests plus 26 existing Anthropic client tests
passed; the complete selected-input preview succeeded before submission. No
full-media verification or transcription rebuild was run.

Launch confirmed at 22:57 EDT: two batches accepted, containing 32 requests;
no submission errors or ambiguous receipts. Initial reservation: $3.075223.
