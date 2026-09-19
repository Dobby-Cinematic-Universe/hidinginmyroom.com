# Targeted transcription and summary follow-up

The operator approved recovery of held broader summaries, integrating completed
targeted transcriptions into the local site, and summarizing submitted recordings
when no summary exists. Public deployment remains unauthorized.

## Claude recovery

`pipeline/sonnet_broader_recovery.py` overlays the original sealed campaign without
editing its pinned implementation, original requests, receipts or failed outputs.
Each held job gets at most one corrected replacement. The correction preserves
the complete input and requires request-local evidence IDs and concise output.
The output allowance is 16,384 tokens. All normal evidence and structure checks
remain in force; unknown references are never guessed or silently removed.

The initial 38 replacements reserve at most $6.845454, sharing the original
$149.881665 campaign cap. Newly unlocked descendants run through the existing
campaign. New held descendants can also receive one replacement; failed
replacements remain held. Ambiguous submission intents are never resubmitted.
No completed original work is repurchased.

Service: `himr-sonnet-recovery-20260917`. Recovery receipts/status:
`research/private-summaries/sonnet-broader-recovery-20260917/`.
The original campaign reader receives successful recovered and downstream outputs.

Some replacement outputs still invented hexadecimal references or packed multiple
IDs into one string. `pipeline/sonnet_broader_strict_recovery.py` queues a second,
bounded follow-up only after that first worker releases its lock and all its
requests settle. It constrains evidence references to an enum of the exact IDs in
each request, rather than relying on prompt wording. This is one additional
attempt per still-held job, not an unbounded retry policy. Both recovery layers'
reservations and successes are inherited under the same original campaign cap.
Prior captures remain unchanged. Service: `himr-sonnet-strict-recovery-20260917`;
workspace: `research/private-summaries/sonnet-broader-strict-recovery-20260917/`.

## Targeted transcript summaries

`pipeline/targeted_summary_followup.py` admits six completed non-anonymous
transcripts through a new reconciliation overlay. Original transcripts and
speaker decisions are unchanged. Five substantial recordings lacking a preferred
summary are selected for Gemini. Canonical import copies are replayed against
retained provider JSON, without rereading or hashing the media archive.

- Self Isolation - Day 1-2
- Life Update - Its Getting Worse...
- The saddest day of my life
- I ejaculated in 5 seconds
- My first stream

The Canadian-guy clip remains held for anonymous multi-speaker labeling.
Japan_graveyard is available as raw transcript but is too short for summarization;
provider CJK spacing must not inflate the apparent word count.

Service: `himr-targeted-gemini-20260917`. Workspace:
`research/private-summaries/targeted-followup-20260917/`.
The five independent shard plans are pre-admitted so they can proceed without
waiting for another long-recording canary. Existing accepted work is reused.
Gemini gets only speech text, speaker evidence and local evidence aliases—not
titles, timestamps, dates, paths or provider metadata. Original timestamps and
internal evidence mappings remain local. API keys use the existing .env loader.

## Local site

`release-20260917-v8` adds completed non-anonymous transcripts and current broader
summaries, retaining prior reviewed sources. Full-text search is built for this
snapshot. No media verification, retranscription or public release is performed.

`scripts/refresh-preview-summaries.mjs` accepts an optional targeted run directory
and adds only completed transcript reader exports missing from the base snapshot.
`pipeline/summary_preview_watch.py` watches completed Claude and targeted Gemini
exports for seven days, updates the local overlay, refreshes cached local event
groups, and restarts only the dedicated local Astro service. It makes no paid calls.
