# Refined diarization selection and single-label summary inputs

The future-request routing below is superseded by
[selective title/face/transcript-lead routing](SELECTIVE_CLOUD_DIARIZATION.md).
Historical paid proofs and the single-label summary behavior remain valid.

Current controllers:

- `himr-cloud-transcription-20260913-refined-v1.service`
- `himr-cloud-gemini-20260913-hundred-v1.service` (successor queued after the
  cooperative stop of `himr-cloud-gemini-20260913-refined-v1.service`)
- `himr-cloud-followup-screen-20260913-v1.service` (completed finite local screening)

Cloud runtime: `research/private-transcriptions/cloud-archive-20260913/diarization-refinement-v1/runtime`.
Gemini's successor preserves this policy in its separate
[100-slot runtime](GEMINI_HUNDRED_SCHEDULING.md).
The original cloud plan, summary manifest, budgets, paid intents, receipts,
transcripts and screening files remain unchanged. Both paid controllers use
the repository `.env` and the same existing state roots.

Cloud and predecessor Gemini execution release: `diarization-refinement-v1/execution-release/release.json`,
SHA-256 `d9636459ee3c681c2dd92c4b25c4d6f51d72eac2f61984e4958af1120dc35adb`.

Follow-up policy: `diarization-refinement-v1/followup/policy.json`, SHA-256
`ac487aae99dcec855046fcb8b83f27741fbc236c798ec3dd70efbea25fe0c6a3`.
Paths above are relative to the cloud campaign root.

## Future cloud submissions

Already-paid intents and even orphan reservations keep their original
diarization setting and evidence. They are never reinterpreted or resubmitted.

For an unsubmitted recording:

1. Supported acoustic diversity, an explicit conversational title, an exact
   strong/moderate transcript lead, or a multiple-face cue keeps diarization on.
   These are routing signals, not verified speaker counts or identities.
2. An existing adequate negative without risk cues keeps diarization off.
3. Other uncertain recordings await at most eight new, disjoint audio probes,
   up to ten seconds each, spread across all four quarters of the recording.
   The existing local VAD/embedding models remain resident in a network-denied
   worker; the original archive screen is not rerun or overwritten.
4. Diarization turns off only after usable speech covers all four quarters,
   there are no failed/discontinuous follow-up samples or unresolved visual
   evidence, and the combined audio evidence has no supported diversity or
   exhausted search. Positive or still-inconclusive follow-ups keep it on.

The cloud controller can process other ready recordings while these short
follow-ups run. Their results and checkpoints are independently retained.
`policy-screen.json` binds the effective setting to the original screen and
the new policy/result. Base reasons/evidence remain historical; consult the
linked follow-up decision for the refined evidence. None of these signals
automatically repurchases an existing usable third-party transcript.

## Exactly one anonymous label

The summary worker now treats exactly one distinct anonymous label like
unlabeled/non-diarized input. It preserves text, segment timestamps and original
segment references, clears the normalized `speaker` and `speaker_scope`, and
recomputes derived source/evidence IDs. Gemini still receives timestamp-stripped
text and internal evidence IDs, without a redundant speaker alias.

The original provider transcript, original diarization flag, labels, completion
and raw response remain intact for provenance. The projection does not claim
that the recording has one person or identify the voice as Daniel. Existing
sealed summary plans are preserved, rather than rewritten or bought again.
Results with two or more anonymous labels still wait for speaker identification.

This projection is an explicit runtime scope, also installed in parallel
collection workers. Use the current versioned worker to inspect its derived
summary plans; do not run an older controller against the same roots.

## Validation

244 targeted and existing tests passed. The first three real follow-ups yielded
one adequate negative and two inconclusive results, with no paid API calls.
All thirteen then-existing single-label cloud results normalized successfully
with identical text/timestamps and unchanged raw files. Their old summary plans
did not need migration because they had previously been held before admission.

The initial future-work split was 71 risk-cue recordings, 18 existing negatives,
and 442 follow-ups; 24 already-paid jobs were frozen. Those are launch-snapshot
counts, not a claim that every uncertain recording can now be classified.

All 442 targeted follow-ups subsequently completed: 38 adequate negatives,
2 supported positives and 402 inconclusive results. Inconclusive recordings
retain diarization; the follow-up worker does not need to be restarted.
