# Optional Salad cloud transcription lane

This is an opt-in, independent lane for a future cloud pilot. It does not switch
the autonomous campaign, mark local work complete, change controller state, or
import results into the catalogue. Its cloud transcripts are not drop-in
replacements for local, model-pinned ASR results.

The implementation has offline and mocked tests. No production audio was uploaded
and no paid live transcription was used to validate this integration. Account
configuration and a small, explicitly authorized live pilot remain necessary.

## Uploads, limits, and cost

The default is to process each video's entire audio track in one job whenever
it fits Salad's 2.5-hour job limit. The uploaded payload is normalized mono
16 kHz, 16-bit WAV audio, not the original video's pixels or container. A full
2.5-hour WAV is about 288 MB. Longer recordings must still be split: with the
default five-second overlap, core windows are 8,990 seconds with up to five
seconds of context on each side, keeping every submitted analysis window within
9,000 seconds. Salad's separate 3 GB media limit also applies.
[Transcription FAQ](https://docs.salad.com/transcription/reference/faq)

Each chunk's upload destination is sealed into the plan. When its conservative
rendered WAV size is at most `100_000_000` bytes, `upload_provider` is `s4`;
larger WAVs use `temp_sh`. This threshold is based on the WAV to be uploaded,
not the compressed source audio or original video's size. Salad documents S4 as
free for SaladCloud customers, with a 100 MB object limit and 30-day storage
lifetime. Transcription itself is paid.
[Salad product documentation](https://docs.salad.com/)

This is a size-based choice, not an error fallback. An S4 authentication,
authorization, or server failure does not cause a file to be sent to temp.sh.

S4 upload requests create signed download URLs valid for three days. These URLs
are access credentials: anyone receiving one may be able to download that
object. Keep the private runtime directory and its backups private. Three-day
link expiry and 30-day object retention are different things; neither establishes
a retention policy for all transcription processing or derived results.

[temp.sh](https://temp.sh/) advertises 4 GB uploads that expire after three days.
The implemented larger-file path follows the operator's preference to keep
whole-recording jobs together rather than split solely to fit S4. Temp.sh's
4 GB allowance cannot override Salad's 3 GB or 2.5-hour limits. Temp.sh adds a
third-party recipient and public-style download links; its homepage does not
document authenticated downloads or a service-level guarantee. Review this
exposure along with Salad's terms before authorizing a live transfer.

The temp.sh adapter validates a plaintext upload-response URL, then accepts a
direct WAV download or resolves one observed HTML download link. It does not
forward the Salad API key or authentication headers to temp.sh. The exact upload
response contract is not documented on the site's homepage, and the linked
upload script returned 404 during research. Tests use mocked response shapes;
an explicitly authorized live pilot must confirm current upload and download
compatibility. No temp.sh upload was performed while building the pipeline.

Every plan requires an operator-supplied `--rate-usd-per-hour` and
`--max-estimated-cost-usd`. The estimate includes overlap, rounds each job upward
to a hundredth of an hour, and applies a 0.01-hour minimum. Salad documents
nearest-hundredth billing and the same minimum; the planner's upward rounding is
deliberately conservative. Check the current rate for your account and engine
before planning. [Billing documentation](https://docs.salad.com/transcription/explanation/billing)

The budget rejects plans whose estimate exceeds that amount. It is not an actual
price or account-spend cap, does not aggregate other plans, and does not verify
that the entered rate matches your account. Before new submissions, `run-cycle`
fetches endpoint information and saves `last-endpoint-preflight.json`; it does not
compare the live price with the entered rate, block a differing live price, or
reconcile account charges.

Salad advertises its transcription features as included without separate feature
charges. Diarization, timestamps, and optional summaries are requested on the same
Salad job; this integration needs no additional LLM API key or server. Confirm
your account's current terms rather than treating that marketing statement or the
plan estimate as a billing guarantee.
[Salad transcription features](https://salad.com/transcription)

## Input selection

Inputs must already have an admitted `recording-input.json` describing normalized
mono 16 kHz audio, its SHA-256, and exact sample count. Raw acquisition files alone
are not eligible. This first version does not prepare the campaign's unprocessed
cold backlog or admit new local candidates.

The inventory helper reads campaign configuration, discovery receipts, small job
manifests, and completion metadata. It never prepares audio, reads audio or
transcript contents, or mutates the campaign. It excludes completed work after
checking receipt bindings and transcript file size; that is not a fresh transcript
content-hash verification. Malformed completion evidence stops inventory rather
than silently making that job eligible again.

It also excludes partial local jobs by default. `--include-partial-local-work`
explicitly includes those jobs for whole-recording paid retranscription. A cloud
job cannot resume the local runner's saved spans. Inspect `counts`, `excluded`,
and the warning policy in the selection before planning.

Inventory is only a snapshot, not a reservation. Stop competing local processing
of selected recordings before cloud dispatch. The cloud lane does not stop or
coordinate the local runner for you.

## Offline planning and preparation

Examples below are templates, not commands already executed. Run from the
repository root, replace every placeholder, and first create a dedicated private
metadata directory. All file arguments must be normalized absolute paths without
symlink components. Use a different private runtime directory for each plan;
do not select a source, live campaign, workspace root, or another workflow's
directory as `--output-root`.

Generate an inventory into a private file:

```sh
umask 077
python3 -m pipeline.salad_transcription_inventory \
  --campaign-config "/absolute/path/longform-asr-campaign-config.json" \
  --campaign-config-sha256 "<campaign-config-sha256>" \
  > "/absolute/private/salad-pilot/selection.json"
```

For a small pilot, retain only the intended prepared recordings in a reviewed
selection. Do not silently drop a partial-work warning. A minimal manually
reviewed selection has this shape:

```json
{
  "kind": "himr_salad_input_selection",
  "schema_version": 1,
  "recordings": [
    {
      "recording_input": "/absolute/path/recording-input.json",
      "sha256": "<recording-input-file-sha256>"
    }
  ]
}
```

Build the immutable plan. Choose `transcribe` or `transcription-lite`; the default
is `transcribe`. Requests use English and always request sentence and word
timestamps. Diarization and summaries are optional plan settings:

| Plan option | Meaning |
| --- | --- |
| `--diarization none` | Default; no speaker labeling requested. |
| `--diarization sentence` | Request sentence-level speaker labels. |
| `--diarization word` | Request word-level speaker labels. |
| `--diarization both` | Request both; sends both diarization flags to Salad. |
| `--summary-words 0` | Default; no summary requested. |
| `--summary-words 200` | Request a summary of about 200 words for each submitted chunk. |

`--summary-words` accepts integers from `0` through `2000`. A nonzero summary
request requires `--engine transcribe`; planning rejects it with
`transcription-lite`. Feature choices are sealed into the plan identity. Changing
them requires a fresh plan, not editing pending request state; resolve any
existing paid jobs before replacing their plan.

This example keeps whole-recording audio jobs where possible and enables both
forms of diarization and per-job summaries. Omit the two feature options to use
the defaults:

```sh
./pipeline/bin/salad-transcription plan \
  --selection "/absolute/private/salad-pilot/selection.json" \
  --organization "<your-organization>" \
  --engine transcribe \
  --diarization both \
  --summary-words 200 \
  --chunk-seconds 9000 \
  --output-root "/absolute/private/salad-pilot/runtime" \
  --ffmpeg "/usr/bin/ffmpeg" \
  --rate-usd-per-hour "<reviewed-hourly-rate>" \
  --max-estimated-cost-usd "<approved-estimated-budget>" \
  --output "/absolute/private/salad-pilot/plan.json"
```

Planning reads manifests and hashes the FFmpeg executable, not source media.
`--chunk-seconds` defaults to `9000`; `--overlap-seconds` defaults to `5`. A
recording up to 2.5 hours is submitted as one job, with no artificial internal
overlap. Longer recordings use the bounded split described above. Shorter custom
windows remain available, for example `--chunk-seconds 1800` for 30-minute cores.
The plan records the selected inputs, actual chunk coordinates, per-chunk upload
provider, executable hash, and estimate. Save the returned `plan_sha256`; every
subsequent command requires that binding. Do not edit the plan in place. The tool
does not rewrite saved plans to adopt new defaults; changing chunking or upload
choices requires a fresh plan after resolving any already-submitted jobs.

Validate the plan and its manifest bindings, then optionally prepare one local
WAV without contacting Salad:

```sh
./pipeline/bin/salad-transcription validate \
  --plan "/absolute/private/salad-pilot/plan.json" \
  --plan-sha256 "<plan-sha256>"

./pipeline/bin/salad-transcription prepare \
  --plan "/absolute/private/salad-pilot/plan.json" \
  --plan-sha256 "<plan-sha256>" \
  --max-chunks 1
```

`validate` reads no media. `prepare` verifies only selected source audio and the
pinned FFmpeg binary, renders exact planned samples, validates the WAV, and saves
a hash-bound preparation receipt. It does not verify the entire corpus. Staging
requires space for the next chunk plus a 128 MiB free-space floor. Derived WAVs
are retained; there is no automatic local pruning or remote-object deletion in
this lane (each host still applies its retention policy). Budget roughly
115.2 MB of WAV storage per recorded hour, plus overlap, headers, and result
files, if all chunks are retained.

## Explicit cloud execution

Before a live pilot, review the account's rates, credit settings, data-processing
terms, retention, and access policy for the selected recordings, including the
temp.sh recipient for oversized WAVs. Configure
`SALAD_API_KEY` in the process environment through your normal secret-management
mechanism. Never put the key in a command argument, selection, plan, checked-in
file, or shell-history command containing its literal value.

Only `run-cycle` and `reconcile` instantiate the network client, and both require
`--allow-cloud`. The following command authorizes uploads and potentially paid
transcription; do not run it merely to check readiness:

```sh
./pipeline/bin/salad-transcription run-cycle \
  --plan "/absolute/private/salad-pilot/plan.json" \
  --plan-sha256 "<plan-sha256>" \
  --allow-cloud \
  --max-new-jobs 1 \
  --max-inflight 2 \
  --max-polls 10
```

These are the defaults: at most one new submission, two in-flight jobs, and ten
existing-job polls per invocation. It is one finite cycle, not a background
daemon. The command prepares a needed chunk automatically, persists its request
intent before submission, polls existing jobs first, and assembles recordings
whose chunks are all complete. Invoke subsequent cycles only under the same
approved plan. `--max-new-jobs 0` polls and collects without submitting new jobs;
it still accesses Salad and can download result files.

Inspect cached state without network calls:

```sh
./pipeline/bin/salad-transcription status \
  --plan "/absolute/private/salad-pilot/plan.json" \
  --plan-sha256 "<plan-sha256>"
```

`status` reports chunk-state counts and `held_for_reconciliation`; it is not a
fresh remote status query. Do not delete runtime state to clear an error: that
state prevents duplicate paid submissions. Preserve the plan, state, and receipts
together when backing up or migrating a cloud run.

## Interrupted requests and reconciliation

A lost connection after a submission can mean that Salad accepted a job even
though its ID was not received locally. Salad's documented API does not provide a
submission idempotency guarantee. This pipeline therefore never automatically
retries an ambiguous POST. `submitting` after a crash and `submission_unknown`
both hold further submissions for that plan. Even a rejected POST requires
operator inspection; failed and cancelled jobs are not automatically retried.

Inspect Salad's records for the already-submitted job and identify its provider
UUID using the saved request metadata. Do not share private state files or signed
URLs in logs or issue reports. Once the exact existing job has been identified,
attach it without posting a new job:

```sh
./pipeline/bin/salad-transcription reconcile \
  --plan "/absolute/private/salad-pilot/plan.json" \
  --plan-sha256 "<plan-sha256>" \
  --chunk-id "<held-chunk-id>" \
  --provider-job-id "<existing-provider-job-uuid>" \
  --allow-cloud
```

Reconciliation verifies the returned organization, engine, input, and request
metadata against the saved intent before binding the ID. If acceptance cannot be
established, keep the hold and seek provider assistance. There is no CLI to reset
an unknown submission, blindly resubmit it, or cancel remote jobs in this first
version. Do not create a replacement plan as a workaround for an unresolved job.

## Results and trust boundaries

The runtime root is private (0700), and newly created files use 0600. It contains
the bound `plan.json`, resumable `state.json`, per-chunk WAV/preparation receipts,
canonical provider job/output documents, and normalized cloud transcripts.
Provider JSON hashes bind canonical JSON values, not the original HTTP wire
format. Recording-level results appear in `recordings/<recording-id-sha256>.json`.

Collection can also be rerun offline:

```sh
./pipeline/bin/salad-transcription collect \
  --plan "/absolute/private/salad-pilot/plan.json" \
  --plan-sha256 "<plan-sha256>"
```

`collect` verifies saved result bindings and assembles only recordings with every
planned chunk complete. Provider timestamps become approximate
recording-relative milliseconds; overlap ownership uses each timed unit's
midpoint within the nonoverlapping core. Untimed text remains explicitly
unplaced. This is not a guarantee of semantic deduplication or exact decoded
sample coverage.

When diarization is enabled, returned speaker labels remain anonymous and scoped
to their provider job and chunk. `SPEAKER_00` in two different chunks does not
establish that they are the same person. The pipeline preserves those labels
without mapping them to names or stitching identities across chunks.

Requested summaries are retained separately for each submitted job. When a
recording fits one job, its summary covers the entire submitted recording audio.
When a recording is split, summaries remain separate per analysis chunk; there
is no additional synthesized whole-recording summary, and neighboring summaries
can repeat overlapping context. Treat them as machine-generated interpretations
requiring review, never as quotations or replacements for the source transcript.
Enabling summaries does not discard the transcript, timing, or raw provider
evidence.

The output kinds are `himr_salad_chunk_transcript` and
`himr_salad_recording_transcript`. They retain cloud-job provenance and explicitly
require human review. No local model revision, verbatim accuracy, publication
approval, catalogue authority, or local campaign completion is claimed. Salad
states that its transcription is not verbatim and may correct repetitions and
small errors, which matters when evaluating archival evidence.
[Transcription FAQ](https://docs.salad.com/transcription/reference/faq)

## Verification

Run the focused offline suite from the repository root:

```sh
python3 -m unittest \
  pipeline.tests.test_salad_transcription_client \
  pipeline.tests.test_salad_transcription_contract \
  pipeline.tests.test_salad_transcription_inventory \
  pipeline.tests.test_salad_transcription_runtime \
  pipeline.tests.test_salad_transcription_cli \
  pipeline.tests.test_salad_transcription_recovery \
  pipeline.tests.test_salad_transcription_temp_upload
```

Tests use synthetic inputs and mocked provider responses; they do not establish
live account readiness or provider compatibility under all production responses.
Finish activation with a small approved live pilot, inspect timing and text
quality, and confirm actual account charges before expanding the selection.
