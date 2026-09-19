# Targeted missing-speech recovery

Operator approval: “Yes, retranscribe them,” following the targeted recovery
recommendation. This is not a full-archive retranscription or summary rerun.

Active workspace:
`research/private-transcriptions/cloud-archive-20260913/targeted-retranscription-20260917-v3`.
The v1/v2 plans were preparation iterations; neither submitted paid requests.

Service: `himr-targeted-retranscription-20260917.service` (four workers, no automatic
service restart). Credentials come from the repository `.env`; never print it.
The service can be restarted explicitly to resume receipted jobs. Any intent
without a receipt is held for reconciliation rather than submitted again.

## Selection

70 candidates, after merging two compatible source-ID copies, include the four
priority partial recordings: Live Q&A with my Sister; Staying at my viewers house;
Staying at the cheapest hotel in Osaka with my girlfriend; Self Isolation – Day 1-2.
The other candidates come from empty transcripts/results, missing completions and
the longer Life Update variant. The 11 excluded entries are two without audio,
eight third-party timeline overruns and one invalid cloud interval. Those require
no-speech classification or local timing/mapping recovery, not blind paid retries.

Selection checks all three existing cloud lanes for related physical/source-ID
copies. Only explicitly empty, completed provider results may be retried; uncertain
or accepted prior jobs block submission. Different-duration variants are not merged.

## Speech gate and provider routing

The retained Silero ONNX VAD screens short files in full. For long recordings it
checks eight 30-second samples in the suspected missing portion. At least one
sample must contain 1.5 seconds of positive speech frames with a contiguous run
of 256 ms. This is triage, not proof of live dialogue or a calibrated guarantee.
A sampled negative is held as `no_speech_detected_in_probes`, not declared silent.
VAD may mistake music/playback for speech; no participant identities are inferred.

Positive candidates submit the **whole recording** as FLAC, not the sampled clips.
AssemblyAI Universal-3.5 Pro is used within the retained ten-hour limit; the nearly
twelve-hour Osaka recording uses Rev AI. Diarization follows retained selective
decisions or conversational title hints, with the three guest-oriented priority
recordings explicitly diarized. The solo Self Isolation priority stays unlabeled.

The retained rate model estimates at most $7.34 if all candidates pass. This is
an estimate, not a provider quote. A $10 batch allocation fits within the earlier
$150 transcription allocation guard. No Gemini or Anthropic calls are authorized
or made by this worker.

## Outputs and review

Original third-party/cloud transcripts and previous speaker decisions are not
overwritten. Each job retains its speech screen, paid intent, provider receipt,
raw result, normalized transcript, completion or hold, and a small `status.json`.
Word timestamps are not duplicated into canonical transcripts; segment timestamps
are retained. A single returned speaker is normalized to unlabeled text. Multiple
speakers set `requires_speaker_review`; new results are not automatically published
or summarized and must be added to the review report before release.

Inspect per-job status files for live progress (the aggregate status advances in
submission order). A failed recording is held without stopping the remaining queue.
Normalization failures retain original provider output for local recovery; they
never trigger another paid POST automatically. Generated WAV/FLAC files are kept
in this isolated workspace until a deliberate cleanup; original media is untouched.

Focused tests: `python3 -B -m unittest pipeline.tests.test_targeted_retranscription`.

## First-pass findings and media recovery

63 candidates returned no positive speech probe; this is not proof that every
long tail is silent. In particular, ffprobe confirms the long Live Q&A with my
Sister container lasts 12,176.833 seconds but its audio stream ends at 5,524.701
seconds. Its supposed missing 111 minutes are not additional available audio.
The viewers-house and Osaka tail samples decoded normally but had no speech
detected. Self Isolation and the longer Life Update copy passed the speech gate.

Three speech-positive candidates failed strict media preparation before any paid
intent. `pipeline/targeted_retranscription_media_recovery.py` created a separate
`targeted-retranscription-media-20260917` workspace:

- The saddest day of my life: use the compatible source-ID alternate copy, whose
  strict decode succeeds, instead of the first copy that fails near 94 seconds.
- My first stream and I ejaculated in 5 seconds: preserve audio packet timestamps,
  fill timeline gaps and the silent container tail using explicit resampling/padding.
  No speech text is fabricated. Both corrected decodes pass strict duration checks.

Original failures and failed decodes remain untouched in v3. No failed job had a
paid intent; these repairs therefore do not duplicate provider charges. The
separate `himr-targeted-media-recovery-20260917.service` processes the three prepared
copies. Its maximum cost replaces, rather than adds to, those three original
unsubmitted allocations. It does not release old holds or reset any paid job.
