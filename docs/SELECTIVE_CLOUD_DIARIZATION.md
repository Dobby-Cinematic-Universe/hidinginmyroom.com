# Selective diarization for future cloud requests

This supersedes the routing part of [the risk/follow-up policy](DIARIZATION_REFINEMENT.md),
not its historical proofs. The user explicitly retained multiple-face cues and
transcript keyword leads after initially considering a conversation-only policy.

For a cloud job with no paid intent **and no reservation**, request diarization if
any of these existing bound signals is present:

- A conversational title matched by the existing title policy.
- At least one multiple-face sample in the existing screen evidence summary.
- An exact recording match in the prior strong/moderate transcript-lead selection.

Acoustic diversity alone, or an inconclusive screen alone, no longer enables it.
No additional follow-up probes are scheduled. These signals are routing hints,
not verified live participants, calibrated confidence, speaker counts or identities.
Faces can belong to playback; transcript keywords can be quoted. This policy
deliberately retains those broader candidates at the user's request.

## State and summary safety

The original `screen.json` and old `policy-screen.json` remain untouched. New
requests get a separate `selective-screen.json`, bound to the original screen and
the new immutable policy. The original acoustic state stays intact even when the
request's diarization flag is false. Validation replays the exact policy.

Any existing reservation or intent freezes its original screen decision, price,
diarization flag and paid evidence. Old title and follow-up proof validators are
unchanged. No old transcript is repurchased, relabeled, overwritten or newly
admitted because of this policy. Third-party preference and historical local-ASR
exclusion remain unchanged. Both providers retain whole-recording lossless FLAC.

Gemini requests, prompts, internal evidence and timestamp stripping are unchanged.
Exactly one anonymous label can still use the existing unlabeled projection;
multiple anonymous labels still need identity review. There is no bulk label
removal to bypass that hold. The separate [review pilot](TRANSCRIPT_AUDIO_REVIEW.md)
remains outside production. The existing prompt explicitly says missing labels
do not imply one speaker and forbids assigning guest, quoted, clip or ambiguous
speech to Daniel by default. This is a prompt safeguard, not audio-source detection.

## Versioned handover

Campaign root: `research/private-transcriptions/cloud-archive-20260913`.
New runtime: `conservative-diarization-v1/runtime` (directory name predates the
user's clarification; its actual policy retains face and transcript cues).
Release: `conservative-diarization-v1/execution-release/release.json`, SHA-256
`9fac1c7f5603db18f7e8f1e89b89daf72bcdfefe141eec949bf3c432f60e12f7`.

Successor services:

- `himr-cloud-transcription-20260913-selective-v1.service`
- `himr-cloud-gemini-20260913-selective-v1.service`

Both require a cooperative stop of their predecessors. Neither predecessor's
running code is patched in place. No forced kill, cancellation, automatic paid
retry, reboot enablement or restart loop is introduced. Use `systemctl --user
list-jobs` to distinguish a queued handover from an active successor.

The same original plan, summary manifest, `.env`, $150 service spending ceilings,
four active transcription jobs, adaptive 100-slot Gemini ceiling, token gate,
four collectors and finite 24-hour runtime remain. Existing Gemini batch requests
are resumed, not submitted again. Its memory-only local validation cache must
warm again after the process change; no archive-wide media verification is added.

## Metadata preview and validation

The preparation snapshot counted 78 frozen paid jobs, 66 unsubmitted jobs with
diarization and 411 without it. The 66 included 35 title matches and 43 face-cue
matches (overlapping). No additional unsubmitted job in this queue matched the
retained transcript lead list; the transcript-lead rule remains active. Counts
can move while the predecessor drains. This preview reads small existing
metadata; it is not a full-archive proof replay or an accuracy measurement.

166 applicable focused and regression tests passed. New tests cover title/face/keyword retention, acoustic-only/uncertain-only
exclusion, exact recording matching, real paid-gate pricing, normalization,
frozen paid requests and orphan reservations, tamper rejection and read-only
preview. The real original cloud/summary manifests and collector initializer
load under the new release without provider calls.

Run the new tests from the versioned runtime:

```sh
python3 -B -m unittest \
  pipeline.tests.test_cloud_selective_diarization.SelectiveRouteTests \
  pipeline.tests.test_cloud_selective_diarization.SelectiveRuntimeTests -v
```

The pre-FLAC resilient-upload tests assume inline upload retries and are not the
current upload contract; the already-deployed per-record backoff/FLAC tests remain
the applicable transport regression suite. No upload/retry implementation was
changed for this policy release.
