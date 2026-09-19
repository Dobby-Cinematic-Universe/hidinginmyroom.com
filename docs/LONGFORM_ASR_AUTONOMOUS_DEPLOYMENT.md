# Autonomous long-form ASR deployment candidate

The long-form campaign adapter is a standalone, finite-stage companion to the
ordinary Archive controller. It is not activated by merely creating its config.
The unified operator supervisor is responsible for starting the ordinary controller
and this adapter together; the adapter watches the ordinary controller's durable
`desired_state` and exits cleanly when it becomes `stopped`.

The adapter covers both sources of long recordings:

- immutable ordinary GPU queue members already classified as
  `requires_chunking`; and
- all 804 work orders in controller-bound
  `cold_acquisition_only_requires_chunking` schedules.

Setup seals the 804 cold result locators once without opening media payloads. Each
continuous cycle then stats only still-pending result paths. A newly appearing small
result envelope supplies a scheduling hint; only the shortest eligible result is
deeply replayed and given an immutable candidate receipt. Restarts replay those
receipts instead of rehashing all earlier acquisition payloads.

Cold admission shares the acquisition handoff's bounded atomic-publication retry
classifier. If strict replay reports one of those exact transient directory/state
race messages, the adapter repeats the entire immutable replay up to 20 times with
0.1 seconds between attempts. It never accepts the failed observation, and digest,
payload, unexpected-shape, or other integrity failures remain immediately fatal.
This lets acquisition publish an unrelated sibling job while a large completed
payload is being hashed without weakening the sealed result or media checks.

Ordinary GPU queue manifests receive a separate immutable discovery receipt after
one bounded envelope replay. Later status/cycle reads compare cheap sealed-file
metadata and replay the receipt; normalized audio is hashed only when the selected
recording-input adapter admits that recording.

A changed queue-manifest device number, inode, or timestamp after a reboot/copy
invalidates only its cheap metadata witness. The adapter rehashes that small sealed
manifest against the receipt's original SHA-256, preserving the historical receipt
and without reading normalized audio. Changed bytes, unsafe permissions, symlinks,
and additional hardlinks still fail closed.

## State machine

```text
sealed cold locator
  -> completed acquisition candidate receipt
  -> isolated one-item ASR-ready preprocess receipt
  -> verified recording input
  -> direct/adaptive plan
  -> immutable span results
  -> complete bindings and recording transcript
  -> completion receipt
  -> derived preprocess scratch cleanup receipt
```

All mutable paths are beneath a separate long-form deployment root. Cold raw source
media, source controller state, plans, span results, transcripts, and receipts are
never removed. After a cold job has a sealed completion document, only that job's
derived `preprocess/output` tree is renamed and removed. A 64-GiB hot-space floor
also gates the creation of one-job preprocessing scratch.

The adapter checks the ordinary public controller status before dispatch. The source
controller must be exactly `running`, but acquisition, preprocessing, and cold
retention may continue while ordinary GPU demand is idle. Dispatch remains held if
any ordinary batch, item, child, current-child record, or partial-pack buffer exists.

A UUID-bound `*.opportunity.lock` makes that idle observation atomic with launch.
The ordinary controller retains the same lease for the full lifetime of its GPU
child; long-form contention therefore creates no child and consumes no ordinary
attempt. The existing GPU-UUID lock remains the final CUDA exclusion authority. If
ordinary work appears during an adaptive recording, the runner finishes and seals
the current logical span, returns resumable `incomplete` status, unloads the model,
and yields the lease. Durable Stop uses the same safe span boundary. Direct jobs are
one bounded span under the candidate direct-duration ceiling, so their only yield
point is completion.

The admitted Faster-Whisper 1.2.1 runtime has a version-scoped compatibility
guard for empty CTranslate2 word alignments. A sub-stride terminal window may
legitimately produce no alignment pairs; in that case the affected segment keeps
its text but carries no word timestamps, matching Faster-Whisper's downstream
contract. Non-empty alignments use the otherwise unchanged 1.2.1 calculation. The
guard lives in the first-party adapter rather than modifying the admitted runtime
installation and can be removed after an upstream release includes
[faster-whisper PR #1460](https://github.com/SYSTRAN/faster-whisper/pull/1460).

## Candidate setup command

Run this only after the successor ordinary controller config and unified supervisor
have passed review. It creates isolated metadata/state, but does not start either
pipeline or invoke CUDA.

```sh
cd /srv/himr

HIMR_CONTROLLER_CONFIG=/srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/controller-config.json
HIMR_LONGFORM_CONFIG=/srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/longform-asr-campaign-config.json
HIMR_LONGFORM_ROOT=/srv/himr/research/operator-state/longform-asr-archive-all-known-2026-08-30
HIMR_POLICY=/srv/himr/corpus/examples/longform-asr-policy.initial-candidate.json

HIMR_CONTROLLER_SHA=$(sha256sum "$HIMR_CONTROLLER_CONFIG" | awk '{print $1}')
HIMR_POLICY_SHA=$(sha256sum "$HIMR_POLICY" | awk '{print $1}')
HIMR_FFMPEG_SHA=$(sha256sum /usr/bin/ffmpeg | awk '{print $1}')
HIMR_FFPROBE_SHA=$(sha256sum /usr/bin/ffprobe | awk '{print $1}')

pipeline/bin/longform-asr-campaign setup \
  --controller-config "$HIMR_CONTROLLER_CONFIG" \
  --controller-config-sha256 "$HIMR_CONTROLLER_SHA" \
  --planning-policy "$HIMR_POLICY" \
  --planning-policy-sha256 "$HIMR_POLICY_SHA" \
  --ffmpeg /usr/bin/ffmpeg \
  --ffmpeg-sha256 "$HIMR_FFMPEG_SHA" \
  --ffprobe /usr/bin/ffprobe \
  --ffprobe-sha256 "$HIMR_FFPROBE_SHA" \
  --deployment-root "$HIMR_LONGFORM_ROOT" \
  --output "$HIMR_LONGFORM_CONFIG" \
  --max-run-seconds 86400
```

The command emits one strict JSON object containing the resulting config SHA-256.
Bind that exact path and digest in the reviewed unified supervisor profile.

## Read-only and finite commands

`status` does not admit newly appearing cold results and does not invoke the GPU:

```sh
pipeline/bin/longform-asr-campaign status \
  --config "$HIMR_LONGFORM_CONFIG" \
  --expected-config-sha256 "$(sha256sum "$HIMR_LONGFORM_CONFIG" | awk '{print $1}')"
```

`prepare-once` and `run-once` execute at most one selected recording. `run` repeats
finite one-recording cycles, uses idle ordinary-GPU opportunities, and emits one
strict JSON object only when it exits or faults. While waiting on ordinary demand it
refreshes the existing small status heartbeat without replaying the candidate corpus.
The owner-private `status.json` under the deployment root supplies lightweight supervisor telemetry:
source config identity, lifecycle, expected cold backlog, discovered candidates,
job counts, active job, update time, and last error.

This lane has no publication, catalogue, identity, wiki, archive, or source-media
deletion authority. Its outputs remain private, machine-generated, unreviewed, and
not verified quotations.

## Reviewed host-tool update during 2026-09-06 recovery

The local-private long-form runner uses its pinned Python, packages, model, FFmpeg,
and GPU directly; it does not execute the historical runtime receipt's bubblewrap
launcher. Fedora's verified `bubblewrap-0.12.0-1.fc44` update changed that unused
tool. `_replay_longform_runtime` permits exactly its reviewed old/new hashes and
root-owned executable metadata while replaying the one SHA-bound candidate receipt
with the one SHA-bound admission helper. Every other validation remains strict.
The compatibility projection preserves the historical receipt, records the
observed transition, and restores the helper after replay, including on failure.

This does **not** authorize the upgraded executable for the separate sealed GPU
launcher or alter its profile. All ordinary GPU jobs in this campaign are already
terminal; remaining campaign execution is in the independent long-form lane. A
future campaign needing new sealed ordinary GPU launches must freshly admit its
updated launcher/toolchain rather than treating this historical replay as launch
authority.
