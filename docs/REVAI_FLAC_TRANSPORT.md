# Rev AI FLAC transport

Successor service: `himr-cloud-transcription-20260913-flac-v2.service`.
Its start is ordered after the cooperative stop of the AssemblyAI-only FLAC
worker. Gemini is untouched. Check `systemctl --user list-jobs` to distinguish
a waiting handover from an active worker.

Runtime, relative to `research/private-transcriptions/cloud-archive-20260913`:
`cloud-flac-v2/runtime`.
Execution release: `cloud-flac-v2/execution-release/release.json`, SHA-256
`25fdbcf63b6492703fa9aa3bd4568d41993da56e9c4d7d3c71fab91f84d34868`.
The runner SHA-256 remains
`018005cb644c7a5206fec75e7004641a9f5114a263105738e1fc7ac83b0cd2f8`.

Both providers now receive verified lossless FLAC for future unsubmitted work.
Rev AI uses the same whole-recording PCM-to-FLAC preparation and exact decoded
sample comparison as [AssemblyAI](CLOUD_FLAC_UPLOADS.md). The FLAC byte count is
checked against Rev AI's upload allowance before any paid intent/reservation,
and the existing multipart client separately checks complete request length.

Original `audio.json`, PCM-based intents, request metadata, duration checks,
diarization options, segment timestamps and normalization remain unchanged.
Already-submitted WAV jobs are collected normally without another purchase.
Both regenerable upload copies are removed after durable successful collection;
original media, transcripts, receipts and lossless proofs remain.

Crucially, Rev AI multipart upload **is a paid transcription submission**.
It does not use AssemblyAI's unbillable-upload cooldown/retry path. A durable
intent and reservation precede the one POST; an ambiguous outcome still stops
for reconciliation. No automatic paid retry, provider switch or repurchase of
an existing third-party transcript is introduced.

170 focused and regression tests passed. New cases exercise a real FLAC through
the multipart encoder with a mocked HTTP transport, metadata/diarization
preservation, PCM-compatible result collection, pre-POST size rejection and
ambiguous-POST protection. The earlier real Rev AI pilot used WAV; no second
paid pilot was submitted for this change, and no full-size live FLAC upload is
claimed to have been tested.

The original cloud plan, $150 budget, four-job concurrency, repository `.env`,
and finite 24-hour worker deadline are preserved. The shared pinned repository
modules were not edited; launch only from the exact versioned runtime/release.
