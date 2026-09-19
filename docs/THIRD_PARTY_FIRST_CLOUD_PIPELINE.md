# Third-party-first cloud transcription and parallel summaries

This is a separate private pipeline. It does not overwrite the local ASR archive,
restart the old Gemini campaign, change the running archive speaker screen,
publish material, or start Sonnet. Building/preparing the pipeline is not
authorization to submit paid jobs. API calls require an explicit paid flag and
separate transcription and Gemini spending limits.

Local ASR transcripts and their old summaries are historical only: they are never
used as current transcript coverage, cloud inputs, or Gemini evidence. Current
coverage consists only of the supplied third-party transcripts and new cloud
results. Missing current coverage can receive a new cloud transcription. Existing
current transcripts are never automatically purchased again; any separately
authorized replacement for diarization requires a positive multi-speaker screen,
not an uncertain result.

## Source selection

1. Prefer clean, exactly matched files in `research/HIMR-Transcripts`.
2. Reuse any explicitly admitted, already-paid cloud result without another POST.
3. Screen recordings with no usable match before making a new cloud submission.
4. Transcribe the whole recording with AssemblyAI Universal-3.5 Pro when it fits.
5. Route recordings beyond AssemblyAI's duration limit to Rev AI's machine ASR.
6. Hold ambiguous identities, suspect supplied transcripts, unsupported lengths,
   and damaged evidence for review. Never resolve ambiguity by silently spending.

The supplied collection was posted by u/MelatoninHighs in
[HIMR Transcripts: 2015–September 2026 (Excluding Kickstreams)](https://old.reddit.com/r/HIMRFAM2/comments/1weonll/himr_transcripts_2015september_2026_excluding/).
Universal-3.5 Pro is recorded as **the user's confirmation from the author**,
not an independently authenticated provider receipt. Original bytes/hashes,
attribution, supplied timestamps and generic speaker labels are retained locally.
No license, speaker identity, human verification or complete audio coverage is
inferred from the public post or an exact filename match.

Matching prioritizes literal Archive item/filename evidence, then unique literal
export basenames, then exact YouTube IDs. Only deterministic documented date and
Archive derivative filename forms are recognized. There is no fuzzy title
matching. Different physical encodings/edits are not automatically collapsed:
the same YouTube ID sometimes identifies files differing by more than an hour.
Conflicts, empty files, nonmonotonic cues, timestamp overruns and suspiciously
large missing tails remain explicit review holds. Subtitle end times alone do
not prove that a recording is truncated.

## Independent stages

For the running 2026-09-13 campaign, use the
[versioned title-policy release](CLOUD_TITLE_POLICY_RELEASE.md), not a second
copy of the old parallel launcher. Its independent services retain the existing
workspaces and isolate provider failures between stages.

```text
exact third-party import ───────────────────────────→ Gemini transcript summary
missing transcript → local screen → cloud ASR ─────→ Gemini transcript summary
```

Each completed recording can advance without waiting for the whole archive.
The screen worker, cloud controller and Gemini worker use separate locks.
Cloud ASR and Gemini each allow bounded concurrent remote jobs; the local
screening/model slot is intentionally CPU- and memory-bounded. Existing verified
screen results can be reused after replaying their source and result evidence.
New screens use an isolated one-recording workspace, not the active archive's
frame-first queue. Third-party-covered recordings do not require a new screen
or another ASR purchase before summarization.

The parallel launcher defaults to four active cloud transcription jobs and eight
Gemini batches. `--transcription-concurrency` and `--summary-concurrency` each
accept 1–8. These limits do not increase the spending caps or cause a paid retry.

## Diarization policy and limits

The screen produces a triage decision, not a verified speaker count:

| Screen decision | AssemblyAI | Rev AI |
| --- | --- | --- |
| Positive or uncertain | `speaker_labels: true` | `skip_diarization: false` |
| Negative, but a clearly conversational title | `speaker_labels: true` | `skip_diarization: false` |
| Completed, adequately sampled negative without that title signal | `speaker_labels: false` | `skip_diarization: true` |

The versioned title-policy runtime additionally recognizes explicit interviews,
Q&A with another person, calls, guest appearances, and streams with someone.
Mentions, reactions, and talking about someone are not sufficient. This changes
the API option only for an unsubmitted cloud recording: it does not turn a
negative screen into a positive finding, change an existing paid request, or
authorize buying another transcript. Already-covered recordings remain review
leads. Existing intents and reservations retain their exact original decision.

Cloud and Gemini may continue while screening discovers more evidence. An
eligible first-pass summary need not wait for full-archive screening; a later
identified multi-speaker result can be handled separately. The existing hold on
anonymous diarized transcripts still applies. Permission for two passes does
not release unknown paid reservations or enable blind retries.

The text-only search of the supplied transcripts is documented in
[Transcript conversation leads](TRANSCRIPT_CONVERSATION_LEADS.md). Those leads
prioritize audio review; they are not speaker detections or paid routing inputs.

No fixed two-speaker count is supplied. Generic labels can be wrong or split
one person into several labels. Frames with multiple people, posters, gameplay
portraits or playback mirrors do not establish multiple speaking people. A
negative screen means no supported diversity in its samples, not proof that the
entire recording has only one voice.

The initial negative gate requires four independent usable fresh baseline probes,
no evidence or sample failures, no unresolved visual cue and no exhausted acoustic
search. In the 39-recording pilot this gave 3 positive, 3 negative and 33 uncertain
decisions, hence diarization enabled for 36. That is an intentionally conservative
**API decision count**, not 36 multi-speaker recordings. A short successful screen
can remain uncertain because it cannot supply enough independent probes.

Provider limits verified on 2026-09-13:

| Provider | Whole recording duration | Direct upload limit | Selected model/options |
| --- | --- | --- | --- |
| AssemblyAI | 10 hours | 2.2 GB | `speech_models: ["universal-3-5-pro"]`, English, screen-controlled diarization |
| Rev AI | 17 hours | 2 GB including multipart framing | `transcriber: "machine"`, English, screen-controlled diarization |

See [AssemblyAI limits](https://support.assemblyai.com/articles/9208125065-are-there-any-limits-on-file-size-or-file-duration-for-files-submitted-to-the-api)
and [Rev AI limits](https://docs.rev.ai/faq). Longer recordings stop for review;
there is no automatic segmentation or lossy transcript stitching. The fallback
is a preflight compatibility decision, never a retry after an uncertain request.

Only the selected recording is hash-verified and converted to a whole-recording
16 kHz mono, 16-bit PCM WAV for upload. Even 17 hours fits below the conservative
2 GB direct-upload cap. This avoids uploading large video tracks or using a
public temporary-file host. There are no cuts, VAD deletions, glossary prompts,
hotwords, custom vocabularies, translation or provider summaries. Normalized
transcripts retain segment/utterance timestamps, text and generic speaker labels,
not per-word timing arrays. The unmodified provider responses remain private
audit evidence and may contain word timings supplied automatically by the API;
where needed, these are used only to derive the stored segment boundaries.

FFmpeg has bounded CPU/memory/output/time and only local file/pipe protocols.
The original media and transcripts never change. Generated upload WAVs are
removed only after durable, validated provider results and normalized transcripts
exist; they can be regenerated from the retained original. Failed/interrupted
unreceipted audio requires review instead of being silently reused.

## Gemini inputs and source provenance

The current worker holds any transcript containing anonymous `SPEAKER_…` labels
for speaker identification **before** Gemini admission. This applies to both
third-party and cloud transcripts, including a result with just one anonymous
label. It never assumes that an anonymous label means Daniel. Unlabeled sources
can advance while other recordings wait. The hold is reported as
`speaker_identity_pending` / `waiting_for_speaker_identity`, not a pipeline error.
If a source was already submitted, existing provider results can be collected,
but no new reduction or export occurs while its identity hold applies. Resolving
the hold needs an explicit verified speaker-mapping policy; changing a label
string is not a bypass.

The new Gemini worker seals `transcript_input_policy: text_and_speaker_evidence_v1`
into its request contracts. For transcript chunks/reducers it sends text, useful
generic speaker labels, compact evidence IDs and necessary evidence classification.
The projection supports speaker labels, but the worker's admission hold above
prevents anonymous-label sources from reaching that step in the current campaign.
It does **not** send segment or word timestamps, subtitle cue numbers, source paths, filenames,
provider payloads, recording IDs, dates or unrelated metadata as model evidence.
Spoken words that happen to contain dates or numbers are not erased.

Internal evidence mapping, original metadata and provider/source receipts remain
local. Source metadata is preserved for later broader synthesis and timeline
construction. The original summary structural/evidence checks remain strict;
they do not prove the audio was transcribed correctly. Cloud transcripts have
their own honest machine-generated source format and are revalidated against
retained provider responses; they never masquerade as local Whisper or Salad.

The old summary implementation was preserved before extending its source reader
and adding the opt-in stripped-input policy. Old request bytes/configuration
semantics are unchanged, but old manifests intentionally fail their implementation
guard against newly extended code. Do not bypass or reseal that guard. The paused
v6 campaign's exact 14-file code proof is retained at
`research/private-transcriptions/cloud-archive-20260913/legacy-gemini-v6-code-proof.json`
for later reconciliation of its three pending batches. Its completed summaries
and paid receipts were not deleted or admitted as summaries of the new sources.

## Credentials

Add these to the repository `.env` (do not overwrite the existing Gemini or
Anthropic settings):

```dotenv
ASSEMBLYAI_API_KEY=your_assemblyai_key
REVAI_ACCESS_TOKEN=your_rev_ai_access_token
GEMINI_API_KEY=your_gemini_key
```

Use `chmod 600 .env`. `REVAI_API_KEY` is an accepted alias, but conflicting aliases
are rejected. Explicit environment keys take precedence. The parser supports
ordinary single-line dotenv syntax without executing shell code or interpolating
variables. Credentials are read lazily, never embedded in plans, receipts,
process arguments or status output. Provider transports do not follow redirects
or send keys to user-configured hosts.

## Preparation and launch

The canonical archive inventory is already available at
`research/private-transcriptions/cloud-archive-20260913/inventory.json`:
SHA-256 `a1c0a108d4d415635dcd6e6ef1f36b283d8d46e59d4b54678873662027f2e5ac`.
It includes all 4,063 physical recordings, 4,488 source aliases, and the newer
699994 admissions. Preparation reads small metadata/transcripts, not the whole
3.1 TB media archive.

The reusable screening configuration is at the same parent, `screen-config.json`:
SHA-256 `b86ab5b9ac3efca3435bab9ff29135d3c9f794c4a7b7ee9434e256059443670e`.
It binds the existing 4,024-record archive selection plus the retained 39-record
pilot, and allows a maximum 300 seconds per isolated recording.

Create a fresh transcription workspace offline:

```sh
pipeline/bin/cloud-transcription prepare \
  --inventory /absolute/private/inventory.json --expected-sha256 INVENTORY_SHA \
  --third-party-root /srv/himr/research/HIMR-Transcripts \
  --screen-config /absolute/private/screen-config.json --screen-config-sha256 SCREEN_SHA \
  --cloud-admissions /absolute/private/paid-result-catalog.json --cloud-admissions-sha256 ADMISSION_SHA \
  --state-root /absolute/private/new-cloud-campaign
```

The cloud-admission arguments are optional for an entirely new campaign. They
are required when carrying the existing paid canary into this deployment.
`pipeline.cloud_transcription_recovery` publishes an admission only after every
producer paid job is accounted for with successful retained output. It verifies
the original code, request/receipt/input/screen bindings and stopped-workspace
inventory, then normalizes saved output into a new artifact. It never creates a
replacement submission intent. Imported reservations remain inside the combined
ASR cap, and an admitted recording cannot be put back into the paid queue.

Then prepare a separate Gemini worker, with a deliberately chosen total USD cap:

```sh
pipeline/bin/cloud-transcription-summary prepare \
  --cloud-plan /absolute/private/new-cloud-campaign/plan.json --expected-sha256 PLAN_SHA \
  --state-root /absolute/private/new-summary-campaign --budget-usd SUMMARY_USD_CAP
```

The parallel launcher checks that both sealed manifests reference the same cloud
plan, then starts the three finite workers. This is the explicit paid step:

```sh
pipeline/bin/cloud-transcription-parallel \
  --plan /absolute/private/new-cloud-campaign/plan.json --expected-sha256 PLAN_SHA \
  --summary-manifest /absolute/private/new-summary-campaign/manifest.json \
  --summary-expected-sha256 SUMMARY_SHA \
  --transcription-budget-usd TRANSCRIPTION_USD_CAP --allow-paid-api \
  --transcription-concurrency 4 --summary-concurrency 8 \
  --max-runtime-seconds 86400
```

Alternatively run the stage commands independently (`cloud-transcription screen`,
`cloud-transcription run`, and `cloud-transcription-summary run`) with their own
finite limits. Screen preparation and execution are local; no paid flag is needed
for that stage. `status` and `export` are offline. Start commands do not enable
boot-time services or automatic retries.

## Costs, interruptions and review

The user approved $150 for the combined AssemblyAI/Rev AI stage and $150 for
Gemini. A strict offline audit preserved $30.952361 of historical Gemini archive
usage and unsettled holds. The new live Gemini chunk canary subsequently used
$0.007554. Counting both within the $150 leaves **$119.040085** for the running
Gemini worker. This is reserved accounting, not a
claim that all historical holds have been billed. The proof and its exact scope
are retained at
`research/private-transcriptions/cloud-archive-20260913/gemini-budget-audit-v1/historical-reservation.json`.
Earlier synchronous pilots and unrelated account activity are outside that audit.

The ASR reservation rates are $0.21/hour without AssemblyAI diarization,
$0.23/hour with it, and $0.20/hour for Rev AI English machine transcription.
These are the [AssemblyAI](https://www.assemblyai.com/pricing) and
[Rev AI](https://www.rev.ai/pricing) published rates checked on 2026-09-13;
reservations include conservative duration tolerance/rounding. They are local
estimates, not an account-wide billing cap or provider invoice; taxes, unrelated
activity, account-specific rates and previous campaigns are outside this ledger.

The cloud controller writes a separate immutable reservation and paid intent
before every billable POST. Raw responses are retained before validation. Unknown,
failed and missing-result jobs keep their reservations. Missing job directories,
spending-limit proofs, screen decisions or receipts stop processing instead of
turning prior paid work into a new request. The Gemini worker likewise keeps a
global pre-submission ledger across its independent per-recording plans, using
validated terminal usage plus unresolved holds. Neither stage raises its cap.

Graceful SIGTERM lets requests preserve receipts before workers stop. Do not
force-kill or run another controller over the same workspace. The parallel
launcher signals all workers on failure or expiry and does not automatically
restart or SIGKILL them; a worker still stopping is reported for review. A
completed screen stage does not stop the other stages.

An uncertain cloud POST can only be reconciled to an existing remote ID with
`cloud-transcription reconcile`: AssemblyAI must echo the exact uploaded audio
and options; Rev AI must echo the unique request fingerprint. This is GET-only,
not authorization to retry the purchase. A provider error is never used to switch
providers automatically. Do not create a second workspace to bypass existing
reservations; review/recover the original evidence first.

Expected provider failures and normalization-review holds retain their original
reservation and do not block healthy recordings. Their raw responses and review
proof remain intact, and they are never automatically resubmitted. Ambiguous
billable requests, missing receipts and integrity/storage errors still stop paid
work globally. Verification is focused on these safeguards and actual failures;
no additional full-archive verification job is part of the live run.
