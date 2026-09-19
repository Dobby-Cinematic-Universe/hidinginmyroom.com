# Metadata-only reconciliation — local preview

The operator accepted incomplete mappings and requested speaker review where
anonymous labels remain. No new paid transcription or summarization is authorized
by this offline reconciler. Canonical source files, paid receipts and running
workers remain unchanged.

`pipeline/metadata_reconciliation.py` works from the exact retained source matches,
not fuzzy title similarity. It admits one SRT candidate bound by source ID or
literal archive filename. Multiple physical copies may share that transcript when
their durations agree; different-length copies require a uniquely closer compatible
duration match. A long unmatched variant remains held rather than guessing offsets.

Missing endings are allowed with a prominent partial-coverage note. Valid cues
with nonmonotonic ordering are sorted by their original timestamps, preserving all
words and timestamps and explicitly recording the ordering change. Empty files,
overrun timestamps and ambiguous timeline mappings remain held.

Existing AssemblyAI utterances may be recovered when complete text agrees with
the utterance sequence and utterance timestamps are valid, even if word-level
timing was rejected. Word timestamps are not needed. Empty provider speech results
remain empty; no replacement ASR or fabricated transcript is introduced.

## Outcome

The final reconciliation index is under
`research/private-transcriptions/cloud-archive-20260913/metadata-reconciliation-v3/`.

- 1,041 additional transcript mappings are ready.
- 129 of those are explicitly partial.
- Nine cases were added for speaker review (seven existing and two recovered utterance results).
- 79 other cases remain held: 41 source structure/timeline issues (including empty
  third-party files), 30 empty provider speech results, six without a usable
  completion, one incompatible long variant, and one invalid cloud segment interval.

The review UI is loopback-only at `http://localhost:8766`. Its report remains pinned
to the v2 reconciliation source copies so saved decisions retain exact input hashes.
It contains all 120 earlier records plus nine new cases, preserving prior decisions.
All 129 records subsequently received complete review decisions during this run.
The refreshed preview incorporates those decisions, including explicitly uncertain
labels, without treating uncertainty as unfinished review.
Final local snapshot: 4,063 recordings, 3,984 with transcripts, 79 metadata-only
(down from 1,129), and 129 newly mapped transcripts explicitly marked partial.
There are 2,683 existing transcript summaries; this reconciliation buys no new ones.

Services:

- `himr-metadata-speaker-review-20260916.service`: review UI.
- `himr-metadata-review-feed-20260916.service`: independently refreshes reviewed copies
  under `metadata-reconciliation-v2/reviewed-feed/` every 60 seconds.

Neither service changes the existing Gemini admission feed. Anonymous multi-speaker
copies remain excluded from summaries until reviewed. The new third-party mappings
are not automatically submitted for new summaries.

## Local preview

The corpus preparer accepts `--reconciled` (the v3 `index.json`) and
`--reviewed-feed` (the v2 reconciliation reviewed-feed `index.json`). Its final
snapshot for this operation is `research/corpus/site-previews/release-20260916-v7`.
Activate it with the existing `activate-corpus-preview.mjs` script. Transcript
coverage notes are displayed alongside provenance; source timestamps are not
claimed to be independently audio-aligned or complete.

After new speaker decisions are saved, regenerate a fresh preview snapshot with
the same optional inputs to include newly completed reviewed copies. Restart only
Astro when switching snapshots. Do not restart paid pipelines or erase their caches.
Public deployment gates remain unchanged.
