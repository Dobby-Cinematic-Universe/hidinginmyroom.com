# Private transcript and timeline summaries

[`transcript_summary.py`](../pipeline/transcript_summary.py) is a separate, finite
Batch API pipeline for completed transcript artifacts. It does not modify the
running acquisition, ASR, speaker screen, diarization, matching, catalogue, or site.
It uploads selected transcript text and prior summary text, **not raw audio/video**;
temporary media hosting such as temp.sh is unnecessary.

The route is completed transcript → bounded cited chunk summaries → whole-transcript
summary → monthly timeline summary. Large reductions use intermediate levels.
Monthly summaries wait for all selected nonempty recordings in their month.
The opt-in [Sonnet broader-synthesis route](SONNET_ARCHIVE_SYNTHESIS.md) adds yearly,
archive-wide and explicitly selected topic summaries, with original supporting
excerpts. Neither route discovers new sources, launches services, or runs an endless watcher.

## Models and readiness

The default `transcript_profile` is `gemini_flash_batch`, using **Gemini 3.8 Flash**
through Google's `generateContent` Batch API. This covers both chunk and
whole-transcript summaries. Gemini Batch currently uses `generateContent`, not the
Interactions API. [Gemini model](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash),
[Batch API](https://ai.google.dev/gemini-api/docs/batch-api).

Gemini requests use the native `responseSchema` format, projected before job
hashing. The local schema still enforces the complete field, length, item-count
and evidence-reference contract. Invalid string arrays or missing evidence are
retained for review, not converted into apparently valid summaries. For an
explicit first-recording review pause, see the
[Gemini-only campaign](GEMINI_TRANSCRIPT_CAMPAIGN.md).

The default `timeline_profile` is `openai_mini_batch`, using the dated
`gpt-5.4-mini-2026-03-17` snapshot through OpenAI Batch `/v1/responses`, structured
JSON output, `reasoning.effort: none`, and `store: false`.
[Model](https://developers.openai.com/api/docs/models/gpt-5.4-mini),
[structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs),
[Batch API](https://developers.openai.com/api/docs/guides/batch).

Set `timeline_profile` to `gemini_flash_batch` in a **new request** to use Gemini
for the original transcript/monthly stages. `anthropic_sonnet_batch` selects Claude
Sonnet 5; see the [Sonnet request example](../pipeline/examples/sonnet-synthesis-request.example.json)
for monthly and optional broader synthesis. Existing defaults are unchanged. Request,
model, instructions, schema, evidence and dependencies are bound into stable job
identities. Provider responses are retained privately for audit.

The implementation has offline tests. No live-model quality evaluation, API call,
credential setup, or paid archive run is implied by code readiness. Start with a
small representative selection and review accuracy, attribution, late-transcript
coverage and usefulness before admitting a larger selection. Account access,
provider quotas and actual service availability still need a live pilot.

## Inputs and provenance

Start from [the request example](../pipeline/examples/transcript-summary-request.example.json).
Every source is explicitly selected by an absolute JSON path and SHA-256; one
transcript revision per recording is allowed. No recursive archive verification
or corpus scan is performed. Inputs are checked again when loading the plan, so
source edits or replacements cannot silently alter an existing campaign.

| `format` | Accepted completed artifact | `completion` |
| --- | --- | --- |
| `longform` | `himr_longform_recording_transcript`, with complete span/coverage accounting | `null`; completion is in the assembly |
| `normalized` | `transcript_normalized` v5 or `himr_machine_transcript` v1 | Hash-bound completed `himr_faster_whisper_gpu_result` that binds this transcript artifact |
| `salad` | `himr_salad_recording_transcript`, with all planned jobs collected | `null`; completion is in the recording transcript |
| `third_party` | Explicit `himr_third_party_transcript_import` wrapper | `null`; wrapper status must be `completed` |

Native semantic identities, counts, bounds and completion evidence are checked.
This is transcript admission, not a fresh verification of every underlying media
file. Third-party provenance is retained without granting rights or treating text
as a verified quotation. The HIMR-Transcripts collection is not assumed to be local
ASR: use the third-party wrapper below for selected material from that collection.

```json
{
  "kind": "himr_third_party_transcript_import",
  "schema_version": 1,
  "recording_id": "RECORDING_ID",
  "status": "completed",
  "provenance": {
    "label": "HIMR-Transcripts",
    "source_url": null,
    "attribution": "REPLACE_WITH_KNOWN_ATTRIBUTION",
    "rights_note": "REPLACE_WITH_KNOWN_RIGHTS_OR_UNCERTAINTY"
  },
  "segments": [
    {"start_ms": null, "end_ms": null, "text": "Exact selected source text.", "speaker": null}
  ]
}
```

Set `format: "third_party"` and bind that wrapper in the source specification.
Use supplied timestamps only when available; both endpoints otherwise remain
`null`. Speaker fields may be `null` or anonymous `SPEAKER_0000`-style labels,
never inferred real names. Native named labels do not become identity evidence.
Salad speaker IDs remain scoped to their original chunk; the normalizer does not
identify the same person across chunks.

Diarization and audio-visual matching are **not prerequisites**. Summaries can be
created from the completed text now. Attribution to a verified person, if wanted
later, requires a separate reviewed workflow and new source revisions.

## Dates and timeline meaning

`date: null` means unknown, not a date guessed from a title, filename, statement,
file modification time, or relative phrase. Unknown-date sources produce a separate
`unknown` timeline, never a fabricated position among dated recordings.

To date a source, set its request field to:

```json
{
  "value": "2020-03-12",
  "kind": "published",
  "evidence": {"path": "/absolute/date-evidence.json", "sha256": "REPLACE_WITH_SHA256"}
}
```

The referenced evidence JSON must contain exactly:

```json
{
  "kind": "himr_summary_date_evidence",
  "schema_version": 1,
  "recording_id": "RECORDING_ID",
  "value": "2020-03-12",
  "date_kind": "published",
  "basis": "operator_supplied_metadata"
}
```

Use `recorded` instead of `published` when that is the supported date kind.
`basis` may also be `direct_catalogue_metadata`. Date values and the recording ID
must agree between request and evidence. These dates organize when recordings were
recorded or published; they **do not establish when recounted events occurred**.
The original date kind remains attached to each source citation.

## Evidence, coverage and review

All characters of a substantive selected transcript are covered by initial jobs;
empty/whitespace-only transcripts require no paid request. Oversized segments are
split at UTF-8 character boundaries and retain character offsets and their original
coarse timestamp interval. There is no guessed intra-segment timing or silent
tail truncation. Missing timing remains missing.

### Compact model inputs

The API receives ordered text with request-local evidence IDs (`e1`, `e2`, …),
short source IDs (`s1`, `s2`, …), and recording/publication dates once per cited
source. The stage and grouping period remain available for chronological summaries.
Existing summary classifications and supported anonymous speaker labels are kept
when present; speaker aliases are scoped to the original recording or chunk.
Missing labels are omitted, not interpreted as one speaker.

Structured timestamps, segment ordinals, character offsets, timing basis, file
paths, provenance records, hashes, dependency IDs and the full selection scope
stay local. Topic requests include only the topic ID and title, not the recording
membership list. Actual transcript text is preserved exactly: literal dates,
timestamps or metadata-looking words spoken in the transcript are not removed.

Sonnet also receives the cited original text in a deduplicated excerpt pool (`x1`,
`x2`, …), linked from the summary evidence and ordered by position within each
source. Deduplication uses original citation
identity, not matching text across different recordings. Models must cite the
request's `eN` IDs; the collector rejects source/excerpt IDs, unknown IDs and full
internal IDs, then resolves valid aliases back to the original local evidence.
The prompt keeps compact IDs out of prose; they are request-local references,
not reader citations or persistent speaker identities.
This reduces input overhead without removing the audit trail or changing reader
links; it is not a guarantee of lower token usage or model accuracy on every job.

This input format changes requests, job IDs and implementation hashes. Use a new
reviewed plan for new work. Existing sealed plans require their original code;
do not modify them or resubmit potentially paid work to bypass a mismatch.

### Retained evidence and validation

Internal results contain `summary`, `topics`, `events` and `uncertainties`. Every item carries
input evidence IDs, and retained results flatten those references back to original
source IDs, transcript SHA-256, segment ordinal, character range, timing basis and
available timestamps. Reducers cannot cite absent evidence or upgrade an inherited
allegation/uncertainty to a plain reported statement.

Citation integrity does **not** prove semantic entailment, quotation accuracy or
historical truth. A transcript can mishear a speaker or contain false allegations;
a model can still misrepresent cited text. All summaries remain private,
machine-generated, unreviewed and unverified. Inputs are treated as untrusted data,
not instructions; no provider is given tools. Prompts prohibit inferred names,
diagnoses, sensitive traits, identities and event dates; structural validation is
not a guarantee of model compliance. No identity or publication authority is granted.
Review before using a summary in a public static site or broader factual narrative.

### Concise prose and speaker attribution

The prompt asks for direct content summaries without routine narrator/source
framing, even on the first mention: avoid "a speaker reported", "Daniel said",
"the transcript discusses" and "according to the source" when they add no meaning.
For example, prefer "Daniel returned two books" to "A speaker reported returning
two books" for an ordinary main-speaker event. Reducers should also remove unnecessary framing
inherited from prior summaries. Needed attribution for allegations, conflicting
accounts and unclear speakers remains; do not turn claims into established facts.

This is a generation instruction, not a string-removal pass over saved text.
Normalization and reader exports preserve the wording they receive, including
meaningful attribution, negations, corrections and literal quoted phrases. Existing
results are not rewritten or rerun; new instructions require a new reviewed plan.
Offline prompt/contract tests do not guarantee that every live response follows
the preferred style.

A UI-level notice can carry the general machine-generated,
unreviewed-source disclaimer; this pipeline does not create that UI notice.
Internal item-level citations and classifications remain mandatory, but reader-facing
transcript summaries have no citation fields. Broader reader summaries link to the
supporting whole transcripts, with no timestamp offsets. Allegations, corrections,
conflicts and uncertainty still need accurate wording and attribution when relevant;
a global notice cannot repair an unsupported claim or a mistaken speaker assignment.
Unsupported optional sections are empty arrays, not omitted JSON fields.

At the user's direction, **Daniel is the default main-speaker label for this
archive**, not an independently verified voice identity. Use his name naturally
for main-speaker actions and views; do not treat every utterance as his. Guest
speech, quoted speech, clips and ambiguous exchanges must not be assigned to Daniel
by default. Other identities must not be guessed from titles, mentioned names or
first-person wording. Attribution still needs review, especially on mixed-speaker
recordings; this instruction does not perform speaker matching or diarization.

Diarization is not required for content/topic summaries. Current long-form ASR
sources enter with `speaker: null`; the user-designated default does not replace
that missing speaker evidence or prove that only one person spoke. Anonymous
labels, when supported by another input format, remain local to their recording or
chunk and are not cross-recording identities. Internal speaker-identity verification
flags remain false, and the default is retained in the sealed job's prompt.

Start with a representative transcript-summary pilot. If "who said what" matters
for a recording flagged as multi-speaker, diarize and review that recording before
summarizing it or relying on it for speaker-specific synthesis. Content-only
summaries of other recordings need not wait. Diarization outputs are not
automatically merged into these immutable transcript sources or existing summaries.
Any later source/prompt change needs a new reviewed plan, and relevant downstream
summaries need regeneration; there is no automatic selective migration or paid rerun.

### Reader-facing summaries and transcript links

Keep the model's private evidence IDs and exact source links for validation and
Sonnet's supporting excerpts. For the UI/review output, use the separate offline
reader exporter:

```sh
pipeline/bin/transcript-summary-reader \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --phase transcripts

# After broader summaries exist, export both stages for the reader:
pipeline/bin/transcript-summary-reader \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --phase all
```

The default phase is `transcripts`; `synthesis` exports only broader results.
The command returns an artifact binding for
`reader-exports/reader-<digest>/index.json`. This is a private, UI-ready data bundle,
not an automatically published website or a new model response.

- `records` holds only completed final transcript summaries. Its four section
  arrays contain `{text, classification}` items, without evidence IDs, citation
  arrays, timestamps or segment references.
- `synthesis` holds completed monthly/yearly/archive/topic summaries. Each item
  also has `sources`: deduplicated links to the whole transcripts actually cited
  by that item, not every recording in the broader scope. Entries contain
  `source_id`, `recording_id`, `title` and `href`.
- Each `href` resolves relative to `index.json` as
  `transcripts/<source_id>.txt`. These text files contain the original normalized
  segment texts joined with newlines; the exporter inserts no timecodes. Their
  hashes are listed in `transcript_files`. It does not invent public site URLs.
- Your UI can use these relative links or map the supplied recording IDs to its
  own transcript pages. No timestamp query, fragment or offset is needed.

Summary wording and classifications are copied exactly, including qualifying
language and uncertainty. The reader removes structured citation metadata, not
text patterns that merely resemble citations, dates or timestamps. Dates in
recording/month/year metadata remain useful for chronology. Citation integrity
is not proof of accuracy; this presentation change does not add factual checking.

The bundle retains private/unreviewed flags and scoped `phase_complete`; `complete`
still describes the whole plan. Only included records and cited sources get text
files. The index is published after its linked files, and repeated exports of the
same results are deterministic. Different snapshots keep separate transcript text
copies, so export at review milestones rather than continuously.

This exporter leaves canonical jobs, provider captures, results, prompts and the
standard evidence-rich export unchanged. Sonnet continues consuming the internal
evidence, never the reader projection. No keys, paid requests or model reruns are
needed. Adding this separate presentation module does not change the implementation
hashes of existing sealed plans. Existing source/prompt changes still follow the
normal immutable-plan rules.

## Offline preparation

Replace all placeholder paths, hashes and recording IDs. Use a new private
workspace leaf under an owned **0700 parent**, separate from source and publication
directories, and keep the request outside that workspace. The example leaves both
cloud approval flags false. These commands need no key or network connection:

```sh
pipeline/bin/transcript-summary plan \
  --request /absolute/request.json --expected-sha256 REQUEST_SHA256

pipeline/bin/transcript-summary prepare \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256

pipeline/bin/transcript-summary status \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256
```

`plan` returns the plan hash and initial reservation estimate. `prepare` seals a
bounded transport wave and returns its `summarywave_…` ID. Repeating preparation
returns an existing unsubmitted wave before creating another. A wave contains only
one provider/model. Initial jobs and intermediate reductions may span multiple
waves without treating a partial level as complete.

For a strict transcript-first workflow, use `--phase transcripts` on `prepare`,
`submit`, `retry`, `status` and review `export`. It covers both chunks and final
whole-transcript reductions. `prepare` returns `phase_complete` when this phase
is finished; `status` also reports `transcript_phase_complete` and the remaining
transcript count. Then explicitly switch preparation and submission to
`--phase synthesis`, which waits for every selected nonempty transcript's final
summary. Both phases reuse the same plan and completed results. Commands without
the flag keep the original `all` behavior; `poll` and `reconcile` address existing
waves and do not start another phase. See the
[staged Gemini → Sonnet commands](SONNET_ARCHIVE_SYNTHESIS.md#finish-transcript-summaries-before-starting-sonnet)
for the review/export pause and recovery details.

Plans are immutable. Source, model/profile, budget or permission changes require a
new request and workspace; do not edit sealed manifests to bypass a check.
Implementation hashes are pinned too: keep the original implementation to resume
an older plan. Do not recreate or resubmit potentially paid work simply to bypass
a code-version mismatch; first reconcile its existing submissions.

## Explicit paid execution

Before planning a paid pilot, set both `cloud.processing_approved` and
`cloud.paid_tier_confirmed` to `true` in the reviewed request. Confirm that you may
send the selected material to the chosen provider and that the selected project is
on the intended paid tier. Do not use a free-tier project as an implicit substitute.

Supply `GEMINI_API_KEY` for Gemini waves, `OPENAI_API_KEY` for OpenAI waves, and
`ANTHROPIC_API_KEY` for Sonnet waves. The CLI first checks the current process's
exported provider variable. If unset, it reads the repository-root `.env` (not an
arbitrary file discovered from the working directory). Only the current wave's
provider key is needed; the Gemini-to-Sonnet preset does not need an OpenAI key.

Using a local editor, add the following entries to
`/srv/himr/.env`, preserving any existing entries:

```dotenv
GEMINI_API_KEY=your_google_key
ANTHROPIC_API_KEY=your_anthropic_key
```

Then restrict access:

```sh
chmod 600 /srv/himr/.env
```

The file must be current-user-owned, private (0600 or 0400), a regular single-link
file, and not reached through symlinks. It is limited to 64 KiB. Never use a
`PUBLIC_` key prefix or place keys in `src/`, `public/`, requests, generated artifacts,
command arguments or source control. `.env` is already git-ignored. A blank
[template](../pipeline/examples/transcript-summary.env.example) is provided; no
populated credential file is created by this feature.

The supported dotenv subset is `KEY=value`, optional `export`, single/double
quotes, blank lines and comments. Values are not shell-sourced or expanded; keys
must be literal, single-line values. Unrelated entries are ignored. An exported
variable wins even when empty, so unset an old/empty export to use its file value.
Duplicate or malformed supported entries produce redacted errors, never key values.

`submit`, `poll` and `reconcile` also accept
`--env-file /absolute/private/summary.env`. It is used only if an API client actually
needs a file-backed key. Offline commands, already-submitted waves and recovery from
cached results do not read the file. Neither values nor the credential-file path
are saved into the plan or wave. A missing default `.env` is tolerated until a key
is needed; an explicitly selected missing file fails when read. No environment
variables are modified, and reading credentials does not authorize paid submission.

```sh
pipeline/bin/transcript-summary submit \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave WAVE_ID --allow-paid-api

pipeline/bin/transcript-summary poll \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave WAVE_ID
```

`submit` is the paid action and requires the explicit flag. `poll` is a separate
read/collection step; it does not submit another paid request. Batch processing is
asynchronous. Gemini documents a target turnaround of 24 hours; OpenAI's chosen
completion window is 24 hours. Provider limits and availability still apply.
[Gemini Batch](https://ai.google.dev/gemini-api/docs/batch-api),
[OpenAI Batch](https://developers.openai.com/api/docs/guides/batch).

After collection, run `prepare` again to seal the next ready work, inspect its
reservation, and explicitly submit that wave. There is no automatic paid retry,
provider fallback or background launcher.

## Cost reservations and limits

The example ceiling is **25,000,000 micro-USD ($25)**, with at most two explicit
attempts per job. Reservations accumulate for submission intents, including
uncertain submissions and retries; they are not automatically refunded after a
short response. This is a local safety allowance, **not actual spend or an invoice
estimate**, and it does not control unrelated usage on your provider account.

Input allowance uses serialized request UTF-8 bytes plus a framing reserve, not
exact tokenization. Although visible output is requested at at most 8,192 tokens,
Gemini reserves its full 65,536-token model output ceiling to conservatively cover
billable thinking. Actual usage may be much lower. The reservation covers the
configured text Batch rates, not taxes or account-specific surcharges.

Rates checked September 12, 2026, per million tokens:

| Profile | Batch input | Batch output, including thinking |
| --- | ---: | ---: |
| Gemini Flash | $0.375 | $1.875 |
| OpenAI Mini | $0.375 | $2.25 |
| Sonnet 5 | $1 | $5 |

The Gemini rates are published through December 31, 2026 and double on January 1,
2027. New submissions are blocked after the pipeline's December 31 pricing-review
cutoff until rates/configuration are reviewed in a new implementation and plan.
[Gemini pricing](https://ai.google.dev/gemini-api/docs/pricing).
OpenAI's Batch discount is 50% of the listed standard model prices.
[OpenAI model pricing](https://developers.openai.com/api/docs/models/gpt-5.4-mini),
[Batch pricing policy](https://developers.openai.com/api/docs/guides/batch).
Sonnet's `max_tokens` allowance includes thinking and visible output; its batch
rates are [documented by Anthropic](https://platform.claude.com/docs/en/about-claude/pricing).

The defaults also bound selected sources, total normalized bytes, total jobs,
waves, requests per wave, request bytes, per-section items and item length. An
oversized/non-shrinking reduction stops for a revised plan rather than discarding
claims. Full-archive costs and quality are not estimated by the small default pilot.

## Interruptions, retries and private export

A durable submission intent is written **before** a provider upload/create action.
If the response is lost, the state becomes `needs_reconciliation`; resubmission is
blocked because the remote operation may already exist. After independently
locating the exact provider batch, explicitly bind it:

```sh
pipeline/bin/transcript-summary reconcile \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave WAVE_ID --remote-id PROVIDER_BATCH_ID
```

Gemini creation allows a 180-second response deadline; GETs retain the configured
timeout (60 seconds by default). A longer deadline reduces avoidable ambiguous
submissions but does not make creation idempotent or enable automatic POST retries.

Gemini/OpenAI reconciliation checks the wave tag and model/endpoint, and OpenAI
also checks the known uploaded input file. Anthropic has no batch-level wave tag:
its lost submissions require an ended batch with every expected wave-specific
request ID and matching successful-response model before adoption. It cannot
adopt an in-progress batch based only on a matching request count.
Reconciliation does not create a new batch. An OpenAI upload that
succeeded without a recoverable batch may require manual provider inspection and
abandonment of that attempt; this CLI does not clear ambiguous intents or blindly
re-upload. Do not start a replacement until the old operation's status is understood.

Confirmed Anthropic HTTP rejections (for example authentication, billing or rate
limits) instead produce `submission_rejected` and a durable rejection record.
After correcting the cause, use the same explicit `retry` command below to prepare
a new attempt. The old wave is never reposted, and its reservation/attempt remains
accounted for. Transport failures and uncertain responses still require reconciliation.

Refusals, incomplete output, missing results, malformed JSON or unsupported
citations become review-required outcomes. Successful jobs are retained. To prepare
only eligible failed jobs from a terminal collected wave:

```sh
pipeline/bin/transcript-summary retry \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave FAILED_WAVE_ID
```

This command is offline; any resulting retry wave still needs a separate paid
`submit`. Already completed jobs and jobs in active attempts are not eligible.

```sh
pipeline/bin/transcript-summary export \
  --manifest /absolute/workspace/plan.json --expected-sha256 PLAN_SHA256
```

Export writes an immutable **private JSON** artifact under the workspace's
`exports/` directory. It contains final transcript/monthly and any enabled broader summaries and a
completion flag; an incomplete export does not claim full selection coverage.
There is no website deployment or publication step.

The workspace retains selected source snapshots, requests, provider captures,
dependency proofs and results. OpenAI input/output file requests use a seven-day
expiration; collect results promptly. `store: false` does not mean all Batch files
or provider logs vanish immediately. Gemini uses inline batches here, so no Gemini
Files API upload is made. Gemini requests also explicitly set `store: false` to
override optional project request logging; this is a privacy control, not a
zero-retention guarantee. [Gemini request logging control](https://ai.google.dev/api/batch-api#GenerateContentRequest).
Anthropic batch results are available for 29 days after batch creation; collect
them promptly. This is not a zero-retention mode. No Anthropic Files API or
temporary hosting service is used. [Anthropic batch limitations](https://platform.claude.com/docs/en/build-with-claude/batch-processing).
The pipeline does not automatically delete remote
resources or purge local evidence. Provider data retention and project policies
remain separate from the private-output flags.

Status distinguishes prepared/pending/ambiguous waves, completed jobs,
review-required jobs, whole-transcript summaries, monthly/yearly/archive/topic summaries, empty
transcripts and reserved budget. A completed chunk is not counted as a completed
whole-transcript summary.
