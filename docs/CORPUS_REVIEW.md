# Corpus review and confidence guide

Machine output is useful for navigation, but it is not automatically reliable speech,
identity, action, or evidence. This guide applies to corpus annotations; wiki
publication must additionally satisfy `EDITORIAL_POLICY.md`.

## Review states

- `machine` — generated and not checked by a person.
- `human_corrected` — wording or annotation was corrected by a reviewer.
- `media_checked` — the reviewer directly listened to or watched the cited interval
  with enough context.
- `disputed` — credible sources or reviewers materially disagree.
- `rejected` — the source does not support the proposed annotation.
- `suppressed` — retained privately but excluded from publication for rights, privacy,
  sensitivity, or safety reasons.

Review records are append-only. They identify the reviewer, date, exact interval,
transcript or observation revision, whether audio was heard, whether video was
watched, the context range, and the decision.

## Machine-transcript publication

Machine transcript wording is not presumed to receive human review. Once an ordinary
recording-coordinate revision has a public source, a publication decision, and clear
rights, privacy, and sensitivity gates, the normal policy is to include it in the next
corpus release. Rendition-local and media-local hypotheses cannot skip coordinate
projection merely because their text is publishable in principle.

Every machine revision and every search result displays: “Machine-generated and
unreviewed; may be wrong; not a verified quotation.” A published transcript is a
navigation aid, not evidence that the speaker said the exact words and never evidence
that the content of a statement is true. Human-corrected, media-checked, and disputed
revisions remain separately labelled; the exporter does not hide one revision behind
an editorial ranking.

An explicit dispute, retraction, or reinstatement requires a matching active human
review and a public explanation. These lifecycle decisions are append-only. A
retraction withdraws transcript text from current search and emits, where the privacy,
rights, and sensitivity gates permit it, only a text-free tombstone and its human
explanation. The tombstone appears as soon as the human retraction is recorded and
remains after its publication state moves from `publish` to `remove`; an unexplained
transcript `remove` is rejected. A retracted revision can only be reinstated, never
changed directly to disputed. Automated reviewers may route or withhold material, but
cannot create a transcript lifecycle decision.

## Confidence

Raw model scores are not probabilities. A displayed probability requires a named,
versioned calibration set and evaluation report. Store confidence separately for:

- speech recognition;
- word alignment;
- speaker diarization;
- active-speaker association;
- face and voice cluster matching;
- identity assertion;
- OCR;
- sound and action classification; and
- source or clip-parent reconciliation.

Public labels should normally use calibrated bands and plain-language limitations.
The precise score remains available in the release metadata when it is genuinely
calibrated. A human decision never becomes `100% confidence`; it remains a review
state with provenance.

## Minimum evaluation set

Begin with at least six stratified hours and expand toward roughly 24 hours. Freeze a
held-out portion before choosing thresholds. Include clean and noisy monologues,
outdoor and car audio, long livestreams, phone or remote audio, overlapping speakers,
music, multilingual speech, reaction playback, short reposts, OCR-heavy screens, and
known synthetic material.

Track word and character error, HIMRverse-name error and recall, word-boundary error,
DER/JER, active-speaker precision and off-screen errors, cluster false merges, OCR
error and temporal duplication, per-action precision, Brier score, calibration error,
and risk/coverage. Report results by media stratum rather than hiding difficult media
inside one average.

## Identity review

Audio turns, visible face tracks, active-speaker links, voice clusters, face clusters,
and named entities are separate records. A model may propose an association; only a
reviewed public anchor may name it.

Do not identify incidental people, search the open web by face or voice, publish
embeddings, or infer sensitive traits. Exclude TTS, media playing inside a video,
altered-speed audio, and AI-generated footage from enrollment. False splits are safer
than false merges.

## Transcript stage directions

Machine sound and action classifications remain separate observations. A stage
direction such as `[laughter]`, `[music starts]`, or `[shows phone to camera]` is added
to a human transcript revision only after a reviewer checks it. Never infer motive,
intent, emotion, intoxication, diagnosis, consent, or relationship status from an
action model.
