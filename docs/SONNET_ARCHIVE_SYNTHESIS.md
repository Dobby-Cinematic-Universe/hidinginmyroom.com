# Private Sonnet archive synthesis

The existing [`transcript-summary`](../pipeline/bin/transcript-summary) command can
use Claude Sonnet 5 for monthly timelines and optional yearly, selected-archive and
topic summaries. Gemini Flash remains the default for chunk and whole-transcript
summaries. All work is bounded by explicitly selected completed transcripts, their
hashes, the request limits and the campaign budget. No service, watcher, publication
or media upload is started.

The [Sonnet request example](../pipeline/examples/sonnet-synthesis-request.example.json)
adds `config.broader_synthesis` and sets `config.timeline_profile` to
`anthropic_sonnet_batch`. Its cloud approval flags are false. The existing example
and ordinary summary workflow remain available; the broader stages are optional.
See [transcript admission and date provenance](TRANSCRIPT_SUMMARIZATION.md) for
accepted artifacts and hash-bound metadata.

## Selection and chronology

The optional configuration is:

```json
{
  "profile": "anthropic_sonnet_batch",
  "yearly": true,
  "archive": true,
  "topics": [
    {"id": "walks", "title": "Reported walks and outings", "recording_ids": ["rec-1"]}
  ]
}
```

Each topic names recording IDs already selected by the request. Topic membership
is explicit; a topic title does not search for additional recordings or establish
facts about a person. Empty transcripts do not require a paid request.

Monthly summaries wait for all selected nonempty recordings in their month.
Yearly summaries reduce the selected monthly summaries for that year. The archive
summary reduces completed yearly summaries and the undated monthly summary; when
yearly synthesis is disabled, it uses the monthly summaries directly. Topic
summaries reduce the explicitly selected whole-transcript summaries. Large
reductions may use intermediate levels. "Archive" means this request's selection,
not an automatic claim to cover every file in the archive.

Grouping uses supported recording or publication metadata. A story told in a 2026
recording belongs to that recording's 2026 group even when it describes an earlier
experience. Unknown dates stay `unknown`, contribute to the selected-archive
summary, and are excluded from dated yearly groups. This is a chronology of source
reports, **not an event-date ledger**. The pipeline does not infer actual event
dates, real speaker identities, causal links or continuity between recordings.

## Evidence and model behavior

Sonnet synthesis jobs receive the supplied prior summary items and their cited
original transcript excerpts. The model sees compact request-local evidence/source/
excerpt IDs, exact text, supported anonymous speaker labels, classifications and
recording/publication dates in a per-source table. It does not receive structured
timestamps, segment/character offsets, hashes, paths, provenance records or the full
local selection scope. Topic input contains only the topic ID and title.

Excerpts retain their original source, character range and available timing
**locally**; dependency replay verifies them against the bound normalized sources.
Returned `eN` evidence IDs are strictly resolved to full local citations before
results are retained. Excerpts are ordered by transcript position within each
source and deduplicated by original citation identity. Literal timestamps or
metadata-looking text within the transcript
are preserved. See [compact model inputs](TRANSCRIPT_SUMMARIZATION.md#compact-model-inputs).
The model has no retrieval tools, network access or permission
to add outside knowledge. Inputs, including topic titles and transcript text, are
untrusted source data.

Internally, all stages return `summary`, `topics`, `events` and `uncertainties`. Every item must
cite supplied evidence. Retained citations lead back to the selected transcript's
hash, segment, character range and available timestamp. The validator preserves
inherited allegations and uncertainties. These links establish reference integrity,
not semantic entailment, historical truth or verified speaker identity. Summaries
remain private, unreviewed machine interpretations and require review before use
in a public narrative.

The implemented Anthropic model is `claude-sonnet-5` through Message Batches at
`https://api.anthropic.com/v1/messages/batches`. It uses adaptive thinking; the
[`max_tokens` allowance includes thinking](https://platform.claude.com/docs/en/models/sonnet-5/whats-new-sonnet-5).
Only a completed assistant response
with `stop_reason: end_turn` and one JSON text block is admitted as a candidate
summary. Thinking and redacted-thinking blocks are ignored. Refusals, truncation,
tool use, unexpected models, malformed JSON and unsupported citations require
review. Structural checks cannot guarantee the quality of an otherwise valid
summary. Offline tests do not establish live model quality or account availability.

Prompts request direct content summaries without routine "a speaker reported" or
"the transcript discusses" framing, even once. Sonnet should also remove unnecessary
framing inherited from earlier summaries; needed attribution for allegations,
conflicts and unclear speakers remains. Saved output is never cleaned by deleting
phrases, and existing results are not automatically rewritten or rerun.
Daniel is the user-designated default main speaker for this archive. Prefer direct
wording such as "Daniel returned two books", without "Daniel said" framing unless
the attribution matters. This is not verified voice identification: do not assign
guest speech, quotes, clips or ambiguous exchanges to Daniel by default. Sonnet must
preserve those distinctions and uncertainties from the prior summaries and excerpts.
Your UI can display the general disclaimer once; internal evidence,
allegation/uncertainty wording and ambiguous-speaker handling remain necessary.
Reader-facing transcript summaries omit citation fields. Broader reader summaries
link each item to its supporting whole transcripts, without timestamp offsets.
No UI warning is implemented by this pipeline change.

Diarization is not a prerequisite for transcript content summaries. Start those
with a representative pilot; prioritize diarization/review for multi-speaker
recordings before relying on speaker-specific claims in Sonnet synthesis. Missing
speaker labels do not imply a single speaker, and anonymous diarization labels do
not identify people. See [speaker attribution and later revisions](TRANSCRIPT_SUMMARIZATION.md#concise-prose-and-speaker-attribution).

## Prepare and execute

Replace the example's placeholder paths, hashes and recording IDs. Supply supported
date evidence to obtain dated month/year groups; the example deliberately leaves
the source undated. Use a new dedicated private state directory under an owned
0700 parent, separate from the source and publication directories, and keep the
request outside it. Do not reuse a workspace belonging to another campaign.

Planning, preparation, status, retry preparation and export are offline. No key or
paid request is needed for these commands:

```sh
pipeline/bin/transcript-summary plan \
  --request /absolute/sonnet-request.json --expected-sha256 REQUEST_SHA256

pipeline/bin/transcript-summary prepare \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --phase transcripts

pipeline/bin/transcript-summary status \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --phase transcripts
```

For a paid campaign, both `cloud.processing_approved` and
`cloud.paid_tier_confirmed` must be true in the request before planning. Confirm
that sending the selected material to each chosen provider is authorized. Set
`ANTHROPIC_API_KEY` securely in the command environment for Sonnet waves, and
`GEMINI_API_KEY` for the default transcript waves. Credentials are never placed in
requests, command arguments or tracked repository files. Only the current wave's provider
key is needed.

Alternatively, put both literal keys in the git-ignored repository-root `.env`
with permissions `0600`. Commands read it lazily when an API key is needed;
exported variables take precedence. An alternate private file can be selected with
`--env-file` on `submit`, `poll` and `reconcile`. See the
[dotenv setup and security rules](TRANSCRIPT_SUMMARIZATION.md#explicit-paid-execution)
and [blank example](../pipeline/examples/transcript-summary.env.example). No shell
sourcing or key export is required when using the file.

```sh
pipeline/bin/transcript-summary submit \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave WAVE_ID --phase transcripts --allow-paid-api

pipeline/bin/transcript-summary poll \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave WAVE_ID
```

`submit --allow-paid-api` is the paid operation. `poll` retrieves that existing
batch and collects available results; it does not submit the next wave. Repeat
`prepare --phase transcripts` after collection, inspect the prepared wave and
explicitly submit it with `--phase transcripts`.
There is no automatic retry or provider fallback. Plans are immutable, so source,
model, permissions, topic membership or budget changes require a new plan.

### Finish transcript summaries before starting Sonnet

Use the same Sonnet preset and sealed plan for both phases. `transcripts` includes
chunk summaries and all whole-transcript reduction levels; with this preset they
use Gemini Flash. `synthesis` includes monthly, yearly, selected-archive and topic
summaries, which use Sonnet. Phase selection is based on job stage, not provider.

Repeat the transcript `prepare` → explicit `submit` → `poll` cycle above until
`prepare --phase transcripts` returns `phase_complete`. A `no_ready_jobs` response
alone is **not** completion: use `status --phase transcripts` to check pending
waves or failures, and collect/reconcile/retry them as appropriate. Status exposes
`transcript_phase_complete` and `transcript_summaries_remaining`; blank transcripts
are counted separately and need no model request. Failures do not count as finished.

Export the finished transcript summaries for private review without citation clutter:

```sh
pipeline/bin/transcript-summary-reader \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --phase transcripts
```

The reader export contains only final transcript summaries, copying their text and
classifications without per-item evidence IDs or citations. `phase_complete: true` means
the transcript phase is finished; `complete` still describes the entire plan and
remains false until the broader summaries finish. Export does not release Sonnet
jobs, and this review pause does not rerun or discard the completed Gemini work.

When ready to begin broader synthesis, explicitly switch phases:

```sh
pipeline/bin/transcript-summary prepare \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --phase synthesis

pipeline/bin/transcript-summary submit \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave SONNET_WAVE_ID --phase synthesis --allow-paid-api
```

Poll the returned wave normally, then repeat with `--phase synthesis` on `prepare`
and `submit`. `prepare`, `retry` and new paid `submit` calls with that phase return
`waiting_for_transcripts` without preparing/submitting synthesis work if any
selected nonempty transcript still lacks its final summary. Use
`status --phase synthesis` to follow this phase, then `export` without a phase
filter for the combined final artifact.

For the combined reader-facing version with whole-transcript links on broader items:

```sh
pipeline/bin/transcript-summary-reader \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --phase all
```

The private bundle contains `index.json` and linked full transcript `.txt` files.
Broad-item links are deduplicated from that item's actual internal evidence and
contain no timecodes. Transcript-summary items remain citation-free. This is an
offline presentation step, with no model rerun or change to Sonnet's evidence.
The original `transcript-summary export` remains available for evidence-rich
audit output. See [reader bundle fields and UI integration](TRANSCRIPT_SUMMARIZATION.md#reader-facing-summaries-and-transcript-links).

Include the matching `--phase` on explicit retries too. Phase-limited submission
rejects a wave containing any other stage before saving a paid intent. Preparation
skips unrelated already-prepared waves but refuses an existing wave that mixes the
requested phase with another; it never silently splits or resubmits that wave.
Commands without `--phase` retain the original `all` behavior, which can admit a
month/topic as soon as its own dependencies finish. Therefore keep the phase flag
on preparation and submission throughout a strictly transcript-first campaign.
No command automatically starts the next phase, and phase flags do not cancel
work that was already submitted.

Phase-filtered status scopes its state, ready/failed job counts and wave lists to
that phase. Selection totals, completed-stage counters and budget reservations
still cover the whole plan; use unfiltered `status` when inspecting all recovery
obligations. Phase-filtered exports identify their phase explicitly and never
claim that the omitted stages are included.

The configured [Sonnet Batch rates](https://platform.claude.com/docs/en/about-claude/pricing)
are $1 per million input tokens and $5 per million output tokens. The reservation includes the complete `max_tokens` output
allowance, including thinking. Input reservations use a conservative serialized
UTF-8 byte allowance plus framing reserve; they are not exact token counts.
Reservations accumulate for submission attempts, including ambiguous attempts
and retries. They are budget safety allowances, **not an invoice or a prediction
of actual spend**, and do not control unrelated provider-account activity. The
example ceiling is $25 with at most two explicit attempts per job. Larger selections
may need revised limits after a representative pilot. The implementation's pricing
review cutoff also applies; do not bypass an expired pricing check.

## Recovery and private export

Each Anthropic attempt has wave-specific request IDs. The exact provider body is
sealed in `provider-requests.bin` before submission; job templates remain in
`requests.bin`. A durable intent is saved before the paid POST. If its response is
lost, submission stays `needs_reconciliation` and cannot silently repost.

A confirmed Anthropic HTTP rejection with status 400, 401, 402, 403, 404, 413,
422 or 429 returns `submission_rejected` and stores a private
`submission-rejected.json` proof. Provider error bodies are not retained in that
proof. Status lists the wave in `rejected_waves` and reports `needs_review` when
no other work is ready or pending. Repeating submission does not send another
POST. No batch was accepted, so reconciliation does not apply: correct the cause,
prepare an explicit `retry` of the rejected wave, then submit the new wave with
`--allow-paid-api`. Its request IDs are new, and the original attempt and budget
reservation remain counted. All existing attempt and budget limits still apply.

HTTP 408, 409, 500 and 529, transport failures and uncertain responses remain
`needs_reconciliation`. They do not authorize a retry without establishing the
outcome of the original submission.

After independently locating a candidate batch, use:

```sh
pipeline/bin/transcript-summary reconcile \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave WAVE_ID --remote-id MESSAGE_BATCH_ID
```

Anthropic batches do not carry the pipeline's wave metadata. Ambiguous recovery
therefore waits until the batch has ended, then requires the exact complete set
of attempt-specific request IDs and the expected model in every successful
response. Matching only the number of requests is insufficient. A pending batch
remains `needs_reconciliation`; old retry IDs or foreign IDs cannot bind the
attempt. The result proof is retained privately in `reconciliation-results.json`.
Reconciliation never starts another batch.

After a complete proof has been saved, reconciliation can recover an interrupted
receipt write and polling can collect from that local proof without a provider
key or another download. The requested remote batch ID must still match the
proof. This also permits collection after remote result availability ends.

Normal collection keeps successful jobs when other jobs fail or are missing.
Duplicate or unknown result IDs reject the capture. An explicit retry prepares
only eligible failed or definitively rejected jobs and uses new request IDs;
submitting it remains a separate paid action:

```sh
pipeline/bin/transcript-summary retry \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256 \
  --wave FAILED_WAVE_ID

pipeline/bin/transcript-summary export \
  --manifest /absolute/sonnet-workspace/plan.json --expected-sha256 PLAN_SHA256
```

Status reports separate completed transcript, monthly, yearly, archive and topic
counts. Export writes immutable private JSON under the workspace, with an explicit
completion flag. An incomplete export does not claim full selection coverage.
Local source snapshots, provider captures and evidence remain available for audit;
provider retention is separate from private local output. Anthropic makes batch
results available for 29 days after batch creation, so collect them promptly.
This workflow does not assume zero provider retention.
[Anthropic Batch processing](https://platform.claude.com/docs/en/build-with-claude/batch-processing).

This implementation changes the code hash bound into new plans. An existing
immutable plan requires its original implementation to resume. Do not edit old
manifests, recreate old paid campaigns, or resubmit possibly paid work to work
around an implementation mismatch. Preserve existing state and reconcile any
ambiguous provider action with the implementation that created it. Adding this
feature does not migrate or alter live state. Compact model inputs also change
request/job identities and require a new reviewed plan for new work.
