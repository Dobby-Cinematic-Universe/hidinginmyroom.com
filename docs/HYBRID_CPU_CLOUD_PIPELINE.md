# Standby-first hybrid CPU/cloud pipeline

This optional sidecar processes explicitly handed-off, finished local media. It
does not replace, restart, reconfigure, or write the running autonomous Archive
campaign. Its own queue, controls, prepared audio, transcripts, and receipts live
in a separate private workspace. Initialization leaves it **stopped**; nothing is
installed or activated by this implementation or the service template below.

The initial routing is:

| Handoff origin | Route | Default processing |
| --- | --- | --- |
| `archive_batch` | Salad cloud | Full audio jobs where possible; timestamps, word and sentence diarization, approximately 200-word summaries |
| `new_video` | Local CPU | Pinned whisper.cpp, four threads, physical 30-minute audio windows |
| `finished_livestream` | Local CPU | Same resumable CPU processing, after recording has actually finished |

The hybrid cycle admits at most eight new handoffs, advances at most one new CPU
window, and permits at most one new cloud job per cycle. Its default global
cloud limit is two in-flight jobs; the supervision interval is 30 seconds.
Completed CPU windows have durable receipts and are verified on resume. The
current window may need to run again after an interruption.

## Scope and readiness

Discovery means automatic scanning of configured **completion-handoff inboxes**,
not scanning all media folders or polling channels. A downloader or operator must
publish a descriptor only after a local file is finished. `--confirm-finished`
is required; an unchanged size alone is not proof that a broadcast has ended.
There is no ongoing livestream capture, new-channel poller, or connected feed
integration in this build. Which channels, feeds, and completion folders should
produce these handoffs remains an operator choice.

The CPU defaults reference the existing Ryzen 7 3700X deployment's pinned
whisper.cpp v1.8.7 executable and English `small.en` model. Existing asset presence
is not a new inference validation. `readiness` reports file presence and size
without rehashing the large model or reading media; selected execution verifies
the pinned bytes. Source normalization was tested with synthetic FFmpeg audio;
CPU orchestration and cloud paths have offline/mocked tests. A new real ASR run
and a small, explicitly authorized cloud pilot remain necessary. No Salad API
credential was supplied for this build, and no live upload, transcription, or
billing occurred.

Both routes first create private mono 16 kHz, 16-bit FLAC and an exact-sample
recording-input manifest. This preparation preserves the original media identity
and source file; it does not create proxy video, perform OCR, or invent existing
preprocessing lineage. It accepts one hash-bound local file, never a live URL.
Limits are 64 GiB source size, 24 hours of source audio, less than 4 GiB normalized
FLAC, and a two-hour preparation deadline. Preparation requires at least 4 GiB
plus a 128 MiB free-space floor before encoding. CPU windows and cloud WAVs need
additional space. There is no automatic source deletion or result publication.

## Initialize an isolated candidate

### Prepared standby workspace on this machine

On 2026-09-06, a separate empty workspace was initialized at
`/srv/himr/research/operator-state/hybrid-cpu-cloud-v1`.
Its control remains `stopped`, generation zero; no service was installed or
started, no source was admitted, and no cost was reserved. Its configuration is:

```text
config: /srv/himr/research/operator-state/hybrid-cpu-cloud-v1/config.json
config_sha256: af5f46d5a06941c563ee5724755a9547d6efe0a178d12fcbd266db0e8875c669
```

Read-only preflight found the CPU binary, model, FFmpeg, and FFprobe present.
Legacy exclusion reports `legacy_control_not_stopped`, as expected while the
original controller remains running. Its controller and long-form configuration
hashes are unchanged. The guard accommodates this machine's owner-private group
permissions without changing any existing permissions.

Cloud organization, reviewed rate, approved estimated budget, and API key are
not configured. No channel/folder completion producer has been connected.
Configure those and arrange the selected live pilots before handover. Do not
activate this empty standby configuration as a replacement discovery pipeline.
The unused initial metadata-only preflight workspace was preserved at sibling
`hybrid-cpu-cloud-v1-preflight`; it contains no jobs and is not an activation
candidate.

### Example for a future reviewed configuration

These are future operator examples, not steps performed while building this
feature. Use a new, dedicated absolute directory, separate from legacy state,
source files, and model/tool directories. Do not reuse a nonempty directory.
The following legacy configuration paths are the existing deployment paths
documented in [the autonomous deployment guide](LONGFORM_ASR_AUTONOMOUS_DEPLOYMENT.md).

```sh
cd /srv/himr

pipeline/bin/hybrid-pipeline init \
  --root /srv/himr/research/operator-state/hybrid-cpu-cloud-candidate \
  --legacy-controller-config /srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/controller-config.json \
  --legacy-companion-config /srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/longform-asr-campaign-config.json
```

This creates private state with `control.desired = "stopped"` and a default
`inbox/` under the new root. It does not start inference, scan source media, or
contact cloud services. Record the returned `config` and `config_sha256`.
Subsequent commands require that exact binding:

```sh
HIMR_HYBRID_CONFIG=/srv/himr/research/operator-state/hybrid-cpu-cloud-candidate/config.json
HIMR_HYBRID_CONFIG_SHA256="<config_sha256 returned by init>"

pipeline/bin/hybrid-pipeline readiness \
  --config "$HIMR_HYBRID_CONFIG" --config-sha256 "$HIMR_HYBRID_CONFIG_SHA256"

pipeline/bin/hybrid-pipeline status \
  --config "$HIMR_HYBRID_CONFIG" --config-sha256 "$HIMR_HYBRID_CONFIG_SHA256"
```

Cloud organization, reviewed hourly rate, and estimated budget are optional at
initialization. Leaving them absent keeps the cloud route undispatched; CPU
readiness does not require an API key. To configure cloud at initial creation,
add these options to `init`, replacing the placeholders with reviewed values:

```text
--cloud-organization <your-organization>
--cloud-rate-usd-per-hour <reviewed-hourly-rate>
--cloud-estimated-budget-usd <approved-cumulative-estimated-budget>
```

The configuration seals software, tool, engine, model, policy, and path
identities. Do not edit it in place, recompute its checksum to bypass a failure,
or reinitialize an existing workspace to clear errors. Configuration changes
require a newly reviewed configuration before queue admission. Once jobs exist,
resolve their ownership and provider state before migrating to a new workspace;
otherwise a fresh ledger could admit duplicate work.

## Publish finished-media handoffs

Use the finished file's known SHA-256 and exact byte count from its trusted
completion receipt, or verify only that selected file. No full-corpus
verification is required. A stable `source-key` identifies the originating video
across copies and handoffs; it is not a download URL.

```sh
pipeline/bin/hybrid-pipeline handoff \
  --source /absolute/finished-media/video.mp4 \
  --source-sha256 "<64-lowercase-hex SHA-256>" \
  --source-bytes "<exact-byte-count>" \
  --source-key "channel:video-id" \
  --origin new_video \
  --output /srv/himr/research/operator-state/hybrid-cpu-cloud-candidate/inbox/video-id.json \
  --confirm-finished
```

The command publishes metadata; it does not read or upload the source. Use
`archive_batch` for selected cloud batch material or `finished_livestream` for a
completed broadcast. `init --inbox /absolute/private/completion-inbox` can be
repeated for external inboxes; those directories must already be private when
scanned. `scan` admits descriptors without inference or network calls, including
while stopped:

```sh
pipeline/bin/hybrid-pipeline scan \
  --config "$HIMR_HYBRID_CONFIG" --config-sha256 "$HIMR_HYBRID_CONFIG_SHA256"
```

Scanning checks source file metadata/size, not its full content hash. Execution
verifies the selected bytes before use. The private ledger deduplicates by media
SHA-256 and source key. The first admitted route owns that source; conflicting
bytes or route requests are recorded privately as conflicts, not dispatched to
both CPU and cloud. Descriptors do not authorize catalogue writes, source
deletion, publication, or person-identity claims.
This deduplication covers this sidecar's ledger only; it does not automatically
import all previously completed legacy work. A future feed must hand off newly
selected work, not replay the whole old corpus into a fresh ledger. Each scan
inspects at most 32 descriptors of at most 64 KiB, with a persisted cursor;
combined inbox and admitted-job inventories are each bounded to 10,000 entries.

## Explicit execution and legacy exclusion

`start` changes only this sidecar's desired state. It refuses when legacy state
is active or uncertain; it never stops the Archive campaign for you. Each work
cycle also requires proven legacy quiescence and holds both pre-existing legacy
execution locks while processing. Those lock leases are inherited by media/ASR
children, so an orphaned child cannot silently release exclusion when its parent
dies. The guard does not rewrite legacy control/status files or remove locks.
Missing, malformed, active, or uncertain legacy evidence holds the sidecar.

Only after separately arranging a reviewed, quiescent handover should an
operator run:

```sh
pipeline/bin/hybrid-pipeline start \
  --config "$HIMR_HYBRID_CONFIG" --config-sha256 "$HIMR_HYBRID_CONFIG_SHA256"

pipeline/bin/hybrid-pipeline cycle \
  --config "$HIMR_HYBRID_CONFIG" --config-sha256 "$HIMR_HYBRID_CONFIG_SHA256" \
  --allow-local-processing
```

That cycle permits CPU processing only. `serve` uses the same execution flags
and repeats bounded cycles; `--max-cycles 1` limits it to one. Add `--allow-cloud`
only after the organization, rate, budget, credentials, upload recipients, and
pilot authorization have been reviewed. It explicitly permits transfers and
potentially paid submissions; it is not a readiness-check flag.

```sh
pipeline/bin/hybrid-pipeline stop \
  --config "$HIMR_HYBRID_CONFIG" --config-sha256 "$HIMR_HYBRID_CONFIG_SHA256"
```

`stop` changes only hybrid control and prevents further dispatch at the next
control check. It is not an immediate cancellation of a running window and does
not cancel an already accepted cloud job. It does not stop or restart the legacy
pipeline. A supervised process stop may interrupt the current window; completed
checkpoint receipts remain reusable.

## Cloud limits, features, and recovery

The batch route uses the existing [Salad lane](SALAD_CLOUD_TRANSCRIPTION.md).
It submits each recording's entire normalized audio track when it fits the
150-minute job limit. Longer recordings use bounded splits with context overlap;
each submitted window still fits 150 minutes. Video pixels are never uploaded.
WAV payloads at or below the conservative 100 MB threshold use signed Salad S4
URLs; larger WAVs use temp.sh. This is a sealed size-based choice, not fallback
after an S4 error. Temp.sh introduces a third-party recipient and public-style
links; private local state does not make uploaded media private. Current live
upload/download compatibility still needs a pilot.

The default full `transcribe` engine requests word and sentence timestamps, both
diarization modes, and a roughly 200-word summary per provider job. Timestamps,
speaker labels, and summaries are machine hypotheses. Speaker IDs are scoped to
one chunk, not linked identities across recordings. For split recordings,
summaries remain per chunk; there is no fabricated whole-recording summary.
The CPU route does not provide Salad diarization or summarization.

Before the first possible paid submission, the hybrid ledger reserves the
estimated cost of the **whole recording**, including all its planned chunks.
Reservations accumulate across this workspace and are not automatically released
after completion, failure, or uncertainty. This is an admission limit on estimates,
not an actual account-charge cap. It does not cover other workspaces or unrelated
account activity, validate live rates against entered rates, or reconcile bills.
Review account-level credit controls and current provider terms separately.

Submission intent is saved before POST. A crash or ambiguous response can mean
that a paid job exists without its ID being saved locally. Such requests are held
for reconciliation, never automatically resubmitted. Inspect the saved
`jobs/<job-id>/cloud-plan.json` and follow the Salad guide's reconciliation
procedure before retrying the hybrid job. After the underlying issue is resolved:

```sh
pipeline/bin/hybrid-pipeline retry \
  --config "$HIMR_HYBRID_CONFIG" --config-sha256 "$HIMR_HYBRID_CONFIG_SHA256" \
  --job-id "<held-hybrid-job-id>"
```

`retry` resets only the hybrid dispatch hold; it preserves cloud submission
intents and cannot safely clear provider uncertainty. Preserve the entire
workspace, including ledger, plans, controls, audio/CPU receipts, cloud state,
and raw provider results. Never delete state to force a retry or roll back to an
older backup that forgets accepted/completed cloud jobs without provider
reconciliation. Recovery must not convert missing local evidence into a new
paid submission.

## Optional service template, not installed

[hybrid-pipeline.service.template](../pipeline/examples/hybrid-pipeline.service.template)
is an uninstalled user-service candidate. Replace every absolute-path and digest
placeholder only after reviewing the configuration and pilot. It is not enabled,
loaded, or started by this build. Service launch does not set hybrid control to
running; the explicit `start` guard still applies.

The template limits the sidecar to four CPU-equivalents and 3 GiB of memory, uses
low scheduling priority, and stops only its own cgroup. A memory limit is not an
ASR performance guarantee; validate it with a selected real pilot. Do not attach
legacy services to this unit or add legacy restart/stop commands. Do not apply a
blanket network denial to the mixed cloud process: cloud needs outbound HTTPS.

Provide `SALAD_API_KEY` through the deployment's normal secret-management path.
The optional service `EnvironmentFile` must be an owned mode-0600 file outside
the repository; its containing directory must be private. No credential file is
created here. Never put the key literal in the unit, arguments, config JSON,
handoff, source control, or shell history. For CPU-only supervision, omit
`--allow-cloud` and the credentials-file directive together.
