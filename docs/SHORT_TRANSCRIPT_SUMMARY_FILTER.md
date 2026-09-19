# Short transcript summary filter

Policy: fewer than 50 whitespace-separated words across transcript segment text
means no displayed/generated summary in the filtered consumption view. Titles,
timestamps, speaker labels and metadata do not contribute. Exactly 50 words is
eligible. Original transcripts should be displayed instead for withheld records.
Original transcript and summary exports, including internal evidence, remain intact.

Current filtered view:
`research/private-transcriptions/cloud-archive-20260913/length-filtered-summaries-v1/index.json`.
Consumers should read `eligible_summaries` and use their referenced summary exports.
`withheld_sources` records the transcript reference, word count, reason and preserved
summary references for auditing; those summaries should not be displayed.

The independent `himr-summary-length-filter-20260914.service` refreshes this view
every 60 seconds, incorporating new finished exports. It reads only summary plans,
their bound transcripts and exports, caches unchanged inputs, and does not inspect
raw audio. The index is replaced atomically. It makes no API calls.

Power-loss recovery: the resumed Gemini service now uses the separately hash-pinned
`pipeline/short_summary_admission_runner.py` extension. It blocks new plans and
chunk/reducer submissions for sources below 50 words, without changing the retained
runtime or release. Already-submitted waves are still collected before this gate;
ambiguous submissions and spending reservations remain untouched. Status separates
`short_transcripts_withheld` from speaker identity holds. This was installed while
the worker was already stopped after the outage, not injected into a running process.
The output index remains an independent consumption filter; its
`live_submission_gate_installed: false` describes the filter itself, not the separate
submitter extension. Existing consumers that directly scan the original
export directories must switch to this index to get withholding; those directories
are intentionally not deleted or edited.

Tests: `python3 -m unittest pipeline.tests.test_short_summary_filter` covers the
49/50-word boundary, metadata exclusion, retroactive and newly-arriving exports,
and preservation of source and summary bytes.
