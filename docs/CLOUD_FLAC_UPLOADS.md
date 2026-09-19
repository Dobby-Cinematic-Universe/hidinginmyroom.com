# Lossless FLAC uploads and per-recording cooldowns

The [both-provider FLAC successor](REVAI_FLAC_TRANSPORT.md) is now queued as
`himr-cloud-transcription-20260913-flac-v2.service`. It extends this transport
to Rev AI while preserving the distinction between unbillable AssemblyAI blob
uploads and paid Rev AI multipart submissions. The v1 details below are retained
as the predecessor's deployment record.

Successor service: `himr-cloud-transcription-20260913-flac-v1.service`.
At handover setup its start job was waiting for the cooperative stop of
`himr-cloud-transcription-20260913-refined-v1.service`. A workspace-lock check
also runs before startup. There is no forced kill, overlapping controller or
automatic service restart. Gemini was not changed or restarted.

Runtime relative to `research/private-transcriptions/cloud-archive-20260913`:
`cloud-flac-v1/runtime`.

Execution release: `cloud-flac-v1/execution-release/release.json`, SHA-256
`aecc355626e66f2fcd1cac52b1a6ecf338775fe72a2819570b6b8106a2af280d`.
The separately pinned runner inside this runtime is
`pipeline/cloud_transcription_resilient.py`, SHA-256
`018005cb644c7a5206fec75e7004641a9f5114a263105738e1fc7ac83b0cd2f8`.
Do not substitute the older shared-root runner, which deliberately remains
unchanged for the draining worker.

## Transport and provenance

Only AssemblyAI blob uploads without an existing upload receipt use FLAC.
Already-uploaded recordings keep their existing URL; already-paid jobs keep
their intents and receipts. Rev AI remains on its tested WAV multipart path.

The original whole-recording 16 kHz mono PCM preparation and `audio.json` stay
unchanged. `audio.flac` is an additional transport copy. Before admission, its
STREAMINFO rate, channels, sample width and frame count must match the WAV, and
decoding it must reproduce the exact SHA-256 of the WAV's PCM samples. The
encoder, source WAV and FLAC bytes are bound in `flac-transport.json`.
Encoding and verification use bounded local FFmpeg processes, without network
input protocols. An interrupted/unreceipted copy is never silently overwritten;
it may be adopted only if full format and lossless checks pass.

The unchanged PCM-backed completion/normalizer contract lets the running Gemini
worker consume new results without a code change. No transcript, source media,
paid request or original plan was resealed. Model, diarization routing, segment
timestamps, third-party preference and the no-automatic-retranscription policy
are unchanged.

On normal successful collection, the WAV and FLAC upload copies are pruned only
after their bindings and durable transcript/provider results are checked. Their
small preparation proofs remain. The copies can be regenerated from retained
source media; original recordings and transcripts are never deleted.

## Failure isolation

An AssemblyAI blob upload gets one attempt per eligible visit. A narrowly
classified transport failure, HTTP 408/429, or HTTP 5xx writes an immutable
per-job `upload-backoff/NNNN.json` record and continues to another recording.
Cooldowns start at 15 minutes, then 30, then 60; longer Retry-After values are
respected. The journal survives restart and binds the exact job folder. Up to
four new upload attempts may occur per cycle, matching the launch admission
bound. After 128 recorded failures a job remains deferred for review.

Authentication, unsafe input, malformed response and provenance failures are
not hidden as transient upload errors. All safe GET retries remain available.
Neither provider's paid transcription POST is automatically retried. An
ambiguous paid outcome still retains its reservation and requires reconciliation.
Logs include `upload_attempts` and `upload_deferrals`; captured upload transport
exception types are retained without raw URLs, response bodies or credentials.

The $150 cloud ceiling, four-active-job limit, explicit repository `.env`,
finite 24-hour runtime and original paid-state workspace remain unchanged.
The separately retained Rev AI pilot reservation still counts toward the
aggregate authorization, as documented in [the pilot](REVAI_API_PILOT.md).

## Validation

162 focused and regression tests passed, including real lossless encoding,
receipt reuse, changed-copy rejection, orphan-copy checks, per-record retry
isolation, persisted backoff, authentication rejection, attempt bounds, paid
submission protection, GET retries, collection cleanup and existing cloud and
summary contracts.

An actual two-minute audio sample shrank from 3,840,078 bytes of WAV to 1,535,941
bytes of FLAC (60% smaller), with bit-identical decoded PCM. AssemblyAI accepted
that FLAC blob upload. No transcription was purchased for this transport test.
Private artifacts are under `cloud-flac-v1/codec-pilot`.

This mitigates long-transfer failures; it does not prove the network fault has
disappeared or establish the compression ratio for every recording. The old
586 MB WAV had failed four times after roughly 16 minutes per attempt, with no
HTTP status captured. No definite server-versus-network cause was established.

Check `systemctl --user list-jobs` for a queued handover and
`systemctl --user status himr-cloud-transcription-20260913-flac-v1.service` for
the successor state. A waiting start job is not an upload or startup failure.
