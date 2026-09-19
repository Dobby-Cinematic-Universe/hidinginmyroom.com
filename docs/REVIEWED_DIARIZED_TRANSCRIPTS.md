# Reviewed diarized transcripts and selective replacements

The September 14 replacement lane retranscribes six explicitly selected full
recordings whose preferred source was a third-party transcript without diarization:

- Meeting Sora the Troll (it was weird)
- Livestream With My Wife
- Live Q&A with Mila
- stream with sunny
- Mega Q&A with my Wife – Live Stream
- First Live Stream with my Girlfriend

This is not permission to retranscribe every third-party recording. Selection is
the exact physical media already associated with the retained third-party import;
shared YouTube IDs are not merged across physical files. Originals and the old
cloud plan remain unchanged. The separate lane uses AssemblyAI Universal-3.5 Pro,
whole-recording FLAC uploads, diarization enabled, and segment timestamps. There
is no forced two-speaker limit. No word timestamps are added to canonical copies.

Service: `himr-third-party-diarization-20260914.service`.
Workspace: `research/private-transcriptions/cloud-archive-20260913/diarized-third-party-v1/`.
All six provider requests are terminal, with six diarized outputs listed in
`results.json` (three canonical and three explicitly recovered copies). The
submitter was stopped after completion; the review feed and Gemini remain active.
The current runner is `runner-v2.json` (v1 was replaced before any paid requests).
The selected jobs have a conservative $3.380491 estimate and a $5 lane cap. The
authorization also reserves the entire old plan's maximum, keeping the combined
transcription allowance under the user's $150 limit. Unknown paid requests are
retained for reconciliation, never automatically submitted again. A per-record
media/provider hold does not prevent other selected jobs from continuing.

## Review semantics

`pipeline/reviewed_transcript_feed.py` watches saved decisions and completed
receipts every minute. The UI uses its dynamic `review-index.json`. Only exact
transcript-bound reviews apply; a label from another transcript revision cannot
silently name a new provider label. Segment decisions override whole-label
decisions. Existing explicit user confirmations are also transcript-bound.

- Participant names are reviewer assignments, not automatically inferred identities.
- Labels with the same case-insensitive saved name merge in the derived copy.
- Explicit Uncertain counts as complete but never becomes Daniel by default.
- A mixed/provisional saved name containing `/` or `?` becomes unattributed
  uncertain speech in the derived copy, retaining the saved value in the audit.
- An unnamed Participant counts as reviewed and remains unidentified.
- Playback / game audio / background noise and TTS remain in the full transcript,
  but are excluded from participant-summary inputs.
- Anonymous provider diarization is not presented as confirmed named diarization
  when fewer than two distinct people have explicit non-provisional assignments.
- The 50-word summary minimum is applied to remaining transcript words, not
  speaker prefixes. Unreviewed turns and extremely short inputs remain withheld.

Original text and segment times are preserved in content-addressed copies.
There is no identity matching by face, voice, title, or guessed names in this
stage. API diarization supplies speaker clusters, not reliable character names.
New multi-label results still need explicit review before named summaries.

## Gemini handoff

`pipeline/gemini_reviewed_resume_runner.py` installs the pinned recovery/short-source
extensions and `pipeline/reviewed_summary_adapter.py` over the unchanged retained
runtime. It uses the existing `summaries-v2/manifest.json`, execution release,
global budget ledger, reservations, paid receipts, and parallel collectors. It
does not create a separate unbudgeted paid summary service.

The adapter creates `himr_reviewed_summary_evidence` documents under
`reviewed-transcript-feed-v1/gemini-evidence/`. These are explicitly rendered
evidence, not verbatim provider transcripts: each remaining segment contains a
saved name or an uncertainty/unnamed-participant prefix plus the original text.
References map to the reviewed copy and original segment. Local evidence retains
segment timestamps; Gemini receives only text and request-local evidence IDs,
with no titles, dates, paths, provider details, or timestamp fields.

Every derived source is replayed against its bound manual decisions. Ordinary
provider normalization remains unchanged. The new kind has separate proof checks
and reuses the existing structural/evidence validation; it is never persisted as
if it were an original provider response. This preserves honest character offsets
for citations to rendered evidence and an internal link to the original turn.

Before admission, current completed reviews replace eligible unplanned sources.
Once a summary plan exists, its exact reviewed revision stays sealed. Auto-save
does not repeatedly purchase summaries. A newly reviewed cloud replacement of an
already summarized third-party transcript gets one separate logical revision
keyed to that cloud transcript's digest. The earlier paid summary is retained.
Further review edits do not automatically buy another pass; a deliberate
revision/republication step is needed to update already sealed work.

## Bounded timestamp recovery

`pipeline/diarized_transcript_timing_repair.py` handles the observed provider defect
where an utterance ends just before its retained final word. It only expands up
to ten utterance ends, by at most two seconds each, to their actual retained word
ends; complete provider normalization and audio-duration checks must then pass.
It never changes text, labels, words, original raw receipts, or paid submissions.
Adjusted responses/transcripts are explicitly marked local derivatives.

The Sunny replacement had one 1,247 ms end mismatch. Mega Q&A with my Wife had
two expansions (1,250 and 1,473 ms), and First Live Stream with my Girlfriend had
one (481 ms). All three were recovered without another API request. The review
feed includes their recovery report as its base snapshot. The original cloud
jobs keep their collection-review holds; recovered copies do not pretend that
the original provider documents passed their stricter canonical validator.

Tests cover uncertainty completion, stale review bindings, dynamic UI discovery,
speaker merging, original text/timestamp preservation, metadata stripping,
short-input gating, rendered-evidence tampering, sealed revisions, unchanged
standard cloud validation, collector initialization, and bounded timing repair.

## Additional audited batch: 63 recordings

The user approved the 63 positive recommendations in
`diarization-candidate-audit-20260914/recommendations.json`: 16 longer recordings,
35 additional conversations, and 12 multilingual conversations (70.38 hours).
Reuse-first/overlap cases, brief-exchange leads, negative controls, and the six
previously purchased replacements are excluded. Selection is bound to the exact
physical recording and retained third-party import, not a shared video ID.
Explicit user approval plus the audit authorizes diarization; inconclusive
acoustic results are retained and are **not** relabeled as positive screens.

The independent workspace is `diarized-candidates-v1/` under the same private
cloud campaign. `himr-diarization-candidates-20260914.service` runs four isolated
workers using its pinned `runtime-v2/`. The immutable plan is
`diarized-candidates-v1/transcription/plan.json`, SHA-256
`265fb7ecbf9e7d53d257faeb3f2a67e1bc4d465da5bf575bc6923ea782b82b0a`.

- 61 whole recordings use AssemblyAI Universal-3.5 Pro with language detection
  and diarization. Two recordings over ten hours use Rev AI machine transcription
  with diarization and the existing English configuration.
- No forced speaker count, custom glossary, automatic model fallback, or
  automatic paid retry. Native Japanese is supported; minority Thai speech in
  mixed recordings is not guaranteed to be covered by the selected model.
- The duration-tolerant estimate is **$15.551194**, with a separate **$20** batch
  cap. Reserving the entire original plan, the previous $5 lane, and this $20
  lane gives a $92.315445 combined ceiling within the user's $150 transcription
  cap. These are conservative allocations, not reconciled provider invoices.
- Existing third-party originals, local ASR, saved reviews, old paid receipts,
  and the running Gemini process are unchanged. Only generated WAV/FLAC upload
  copies are removed after durable completion; they can be regenerated.
- Intent and budget reservation are durable before the paid request. An
  ambiguous submission is held for reconciliation, never sent a second time.
  Per-record media/provider failures are isolated so the other workers continue.
- A one-label result is flattened to unattributed segments. Multiple labels
  appear automatically in the review UI; they are not inferred character names.
  They remain ineligible for summaries until the transcript-bound review is
  complete. The review feed was refreshed with this plan; Gemini was not restarted.

The English and Japanese canaries are complete whole recordings from this same
63-record selection, not additional purchased samples. Japanese utterances had
provider-inserted token spaces absent from the full text. Collector release v2
accepts only exact non-whitespace character equality for Japanese/Chinese in
explicit auto-language mode, retaining both original text representations.
English and genuine content mismatches still fail validation. The original
runtime, plan, hold evidence, and both provider receipts remain intact; recovery
did not issue another paid request.

Safe status check (no provider calls or media scan):

```sh
cd /srv/himr/research/private-transcriptions/cloud-archive-20260913/diarized-candidates-v1/runtime-v2
python3 -B -m pipeline.cloud_diarization_batch status \
  --plan /srv/himr/research/private-transcriptions/cloud-archive-20260913/diarized-candidates-v1/transcription/plan.json \
  --expected-sha256 265fb7ecbf9e7d53d257faeb3f2a67e1bc4d465da5bf575bc6923ea782b82b0a
```

After an interruption, `systemctl --user start
himr-diarization-candidates-20260914.service` resumes existing receipts and skips
completed/held jobs. Do not delete intents, reservations, or provider receipts to
force a retry. The review UI remains at `http://127.0.0.1:8766/`.

## Targeted recovery on September 15

The 63-record batch ended with 59 completed jobs and four holds. Recovery is
limited to those four; it does not rebuild the archive, repeat completed paid
requests, change saved labels, or restart Gemini.

### Explicit Rev AI fallback for the final failed upload

The sequential recovery completed `meeting the hongkong guy` and `japanese
woman came to my hotel room`. The remaining `dinner and owl cafe with bloodbucket
[G07oXmwgq3s]` upload returned HTTP 502 after 901.1 and 909.9 seconds, despite a
3,600-second client timeout. This suggests a remote upload timeout; no AssemblyAI
transcription intent, reservation, or paid submission existed for that recording.

The user explicitly approved switching **only**
`cloudjob_002dcd70e78a489b1d3bdd0fc126ffff` to Rev AI. This is a recorded one-off
provider override, not permission for automatic model fallback on other jobs.
`pipeline/cloud_diarization_revai_recovery.py` uses the unchanged, verified
460,376,767-byte whole-recording FLAC (25,092.514 seconds), Rev machine English
transcription, and diarization without a forced speaker count. This changes the
model from the originally planned Universal-3.5 Pro. Canonical segment timestamps
remain; normalized word timestamps remain absent.

Service: `himr-diarization-revai-recovery-20260915.service`.
Bound workspace: `diarized-candidates-v1/revai-recovery-20260915-v1/`.
Manifest SHA-256:
`589d16dc990f6db0af577874468319a4c64f21332dc7ed6dbf7961a85ffad032`.
Pinned runner SHA-256:
`dcc4f97710be85318f6e819fc37cccc3e17bf83d3c0b346417ada6be0bb477fb`.

The $1.395445 conservative reservation replaces the unused $1.604762 planned
AssemblyAI allowance in the **same** $20 batch ledger; it does not establish a
second budget. The combined cap check also includes the retained $0.01 Rev pilot
allowance. Estimates/reservations are not settled provider charges.

- The original plan, job, screen, runtime, third-party source, media, reviews,
  and other 62 completed recordings remain unchanged.
- The original job lock protects both providers. Its original AssemblyAI hold
  stays as retained evidence and prevents the old runner from buying another
  transcription. A durable completion takes precedence over that historical hold.
- Intent and reservation are written before the **single paid multipart POST**.
  The receipt is saved before validation. A reservation without a receipt stops
  for reconciliation, even after an interruption. Only GET collection retries.
- The original job folder receives the actual Rev receipt, raw response, and
  canonical completion, each linked to the explicit provider override. No prior
  paid result is overwritten. Cached aggregate status reports the actual provider;
  the immutable original plan still records its historical AssemblyAI selection.
- Existing receipt discovery admits multiple labels to the review UI and
  withholds them from summaries until reviewed. One label is flattened as usual.
  Neither the review feed nor Gemini needs a restart. No separate summary purchase
  is made by this recovery worker.
- WAV/FLAC copies are retained by this recovery; no original media is deleted.

Use the recovery workspace's `status.json`, `result.json`, and service journal
for this override's live state. Logged upload byte counts are transport progress,
**not** provider acceptance. After a pause with a saved submission, starting the
same service resumes collection. Do not clear a reconciliation hold, delete
intent/reservation files, restart the old upload recovery, or create another
fallback workspace to force another paid attempt.

Before launch, the pinned runtime and retained audio receipts passed preflight,
the Rev account GET authenticated, and 76 focused tests passed, including stream
framing, whole-source preservation, budget enforcement, receipt replay, ambiguous
POST protection, review discovery, and anonymous-speaker summary withholding.

### Three failed uploads

`pipeline/cloud_diarization_upload_recovery.py` runs as
`himr-diarization-upload-recovery-20260915.service`. Its pinned runner, manifest,
per-upload byte progress, attempt receipts, and final result are in
`diarized-candidates-v1/upload-recovery-20260915-v1/`.

The selected jobs are `meeting the hongkong guy`, `japanese woman came to my
hotel room`, and `dinner and owl cafe with bloodbucket`. All three failed before
a paid intent/reservation/submission was created, and their whole-file lossless
FLAC copies were retained. Recovery uploads them one at a time, smallest first,
with a 3,600-second socket timeout and at most two unbillable attempts each.
Progress is logged every 20 seconds. Byte counts describe transport progress,
not provider acceptance; only a saved validated upload receipt establishes that.

After each successful upload, the original hold is retained under an explicitly
resolved name and the unchanged pinned batch handles the first paid submission
and collection in a separate process. Thus collection can overlap the next
upload. Existing paid evidence always takes the collection/reconciliation path,
never another paid POST. The original $20 batch allocation still applies.

While a retry is uploading, its old batch hold remains visible; consult the
recovery service's progress files/logs for current activity. Do not restart the
old four-worker batch while recovery owns these jobs. The recovery service can
be started again after an interruption; durable upload receipts and paid
intents are retained. Exhausted or non-transient failures require inspection.

### Partial recovery of the invalid closing turn

`pipeline/cloud_diarization_tail_recovery.py` recovered `She moved in with me`
without another provider call. The provider returned a final turn containing
256 repetitions of one token, extending 3,105 ms beyond the prepared audio.
This is not the earlier small utterance-end mismatch and was not silently
clamped into valid-looking timestamps.

The recovered copy contains the preceding **244 turns**, with their original
speech text, labels, and segment times unchanged and full strict normalization
passing for that retained prefix. The final **21.995 seconds** are explicitly
excluded from this partial transcript and preserved in
`tail-recovery-20260915-v1/quarantined-tail.json`, alongside the untouched full
provider result and original hold evidence. The audio content of this anomalous
tail has not been confirmed. Original video/audio is not cut or modified.

The review feed uses the derived report in that folder, preserving all earlier
review records and adding a visible warning on the final retained turn. The
copy is marked `partial_recovery: true`; it must not be described as verified
full-media coverage. Normal speaker-review gating still applies, and the
quarantined repetitive text is not sent to Gemini with the recovered prefix.

Only the lightweight review-feed service was refreshed. Its implementation and
the running Gemini process were not changed.
