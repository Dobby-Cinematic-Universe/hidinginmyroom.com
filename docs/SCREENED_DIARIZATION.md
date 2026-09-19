# Diarization after archive speaker screening

The separate private pipeline is implemented in
[`screened_diarization.py`](../pipeline/screened_diarization.py). It selects completed
archive-screen results, normalizes the full recording, invokes a bounded offline
Community-1 worker, and retains independently verifiable anonymous speaker turns.
It does not alter or restart screening, acquisition, ASR, the catalogue, or the site.

## Current readiness

Code, model-free integration tests, native generated-audio normalization tests,
and failure/replay checks are available. **Real Community-1 inference is not yet
validated or enabled.** No model downloads, installations, access-condition
acceptance, credentials, or diarization service were performed during this build.
The current screening environment is not a diarization environment.

Before real inference, accept the Community-1 access conditions yourself, acquire
the exact approved bundle, build a separate pinned offline runtime, register its
files/wheels, and complete a bounded real-media smoke test. Configuration bytes
are gated upstream; the strict parser and restricted checkpoint loader must be
tested against the lawful local bundle. Unsupported configuration or checkpoint
globals fail closed, never fall back to arbitrary imports or unsafe pickle.

The code implements a **private unvalidated pilot**, not an approved production
backfill or an accuracy claim. Follow [ADR 0007](adr/0007-speaker-and-active-speaker-models.md)
for the reviewed corpus benchmark, quality/resource gates and attribution records.
No time or memory estimate here replaces measurements on the RTX 3050.

## Selection and speaker counts

The default queue contains only completed `multiple_speaker_candidate` records.
Set `include_uncertain: true` to include uncertain results. `media_ids` is a filter,
not an override of eligibility. A negative sampled screen is neither a confirmed
solo recording nor a diarization result.

Selection verifies the sealed campaign/batch plan, original screening inputs,
completed result, checkpoint replay, and current source metadata witness. It never
executes the screening model, reads an actively written partial result as complete,
or rehashes full raw videos. It relies on the archive's immutable content store for
the existing raw content SHA. Any corruption or source drift stops the operation.

A plan is a finite snapshot. Newly completed screen results require a new request
and workspace; no watcher silently extends a sealed run. At most 128 selected
recordings enter one plan. Excess eligible records are explicitly counted as
deferred; use explicit media filters for subsequent disjoint plans.

Speaker counting defaults to automatic. “Probably two” is not a hard constraint.
Optional per-source bounds require a direct-media human review, matching media SHA,
reviewer, zoned review time, and an independently hash-bound evidence file:

```json
{
  "media_sha256": "<source SHA-256>",
  "bounds": {
    "parameters": {"min_speakers": 1, "max_speakers": 2},
    "review": {
      "basis": "direct_media_human_review",
      "media_sha256": "<same source SHA-256>",
      "reviewer": "<reviewer>",
      "reviewed_at": "2026-09-12T18:00:00-04:00",
      "evidence": {"path": "/absolute/review.json", "sha256": "<review SHA-256>"}
    }
  }
}
```

For confirmed exactly-two recordings, use `num_speakers: 2` instead of min/max.
The evidence is a human attestation, not a speaker-screen score or title inference.
Silence never produces invented speakers; nonempty model output that violates a
reviewed count is rejected for review.

## Audio and results

Each job processes one whole recording in one model call. It does not independently
diarize chunks and then assume their speaker labels match. Very long inputs that
exceed the declared resource envelope are explicitly blocked, not silently split.

Audio is normalized to 16 kHz mono s16 FLAC and then verified PCM. Timestamp-driven
hard correction fills initial/internal timestamp gaps with silence and trims
timestamp overlaps; this is explicitly disclosed, not claimed to be untouched
audio. FFmpeg uses
`aresample=16000:async=1:first_pts=0:min_comp=0.0000625:min_hard_comp=0.0000625:max_soft_comp=0`.
The correction threshold is one output sample; soft time stretching is disabled.
There is no unconstrained end padding. PCM must match the full
screened audio EOF exactly at integer-millisecond resolution. A mismatch becomes a
review/error, not invented tail audio. Unlike fast triage, full diarization does
not omit the opening 100 ms.

Ordinary turns preserve overlapping speakers. Exclusive turns are a separate
transcript-alignment representation. Both use recording/run-local anonymous labels
and half-open integer-millisecond intervals. Quantization and small boundary
adjustments are recorded and replayable. No named identity, cross-recording voice
matching, embeddings, calibrated confidence, or publication authority is produced.

Per-recording `result.json` binds the selection evidence, audio preparation,
worker request, raw engine-output receipt, actual model/runtime provenance and
normalized turns. Resume verifies these artifacts instead of rerunning completed
jobs. Failed jobs remain incomplete and committed results survive interruption.

Generated PCM and FLAC are removed after success or failure unless
`retain_normalized_audio` is true. Only exact files in this job's newly created
attempt directory are eligible for cleanup; original media is untouched. Proofs
and results remain private and durable. Retained debug audio consumes disk space.

## Model and runtime admission

The engine only accepts `pyannote/speaker-diarization-community-1`, revision
`3533c8cf8e369892e6b79ff1bf80f7b0286a54ee`, with `pyannote.audio==4.0.7` and its
registered wheel SHA. The complete pinned model tree, weights, configuration,
license snapshot, access review, and runtime manifest must match.

[`screened_diarization_engine.py`](../pipeline/screened_diarization_engine.py)
documents the exact bundle/runtime/review contracts. Its `admit_bundle(binding)`
function checks artifacts without importing ML. Worker startup verifies the exact
Python executable, installed distributions and registered files/wheels before
imports. Network access is denied, telemetry is disabled, environment credentials
are not inherited, and checkpoint loading is restricted to reviewed architectures
and safe globals. Models are never downloaded at inference time.

## Planning and running

Start with [the example request](../pipeline/examples/screened-diarization-request.example.json).
Replace all illustrative paths and hashes. Create a **new owned 0700 parent** for
the private workspace. Keep the request outside the workspace. `model_bundle` and
`python` may remain `null` for a clearly blocked setup preview; configure them in a
new request/workspace once model/runtime acquisition is complete.

```sh
pipeline/bin/screened-diarization plan \
  --request /absolute/request.json --expected-sha256 <request-sha256>

pipeline/bin/screened-diarization status \
  --manifest /absolute/workspace/plan.json --expected-sha256 <plan-sha256>
```

The input source can be `campaign` for the full screen campaign's immutable
`campaign/manifest.json`, or `batch` for an explicit archive batch manifest.
Planning before the screen has sealed its manifest is not possible; do not invent
a hash or consume its live preparation-status file as an input manifest.

Execution is explicit and must use a dedicated cgroup with a hard aggregate host
memory ceiling and no swap. For the example's 12 GiB ceiling, after approved setup:

```sh
systemd-run --user --unit=himr-diarization-pilot-001 \
  --property=MemoryMax=12G --property=MemorySwapMax=0 \
  --property=OOMPolicy=stop --property=KillMode=control-group \
  --property=TimeoutStopSec=60 --property=RuntimeMaxSec=28800 \
  --property=Restart=no --property=UMask=0077 \
  --property=WorkingDirectory=/srv/himr \
  -- /usr/bin/python3 -B /srv/himr/pipeline/screened_diarization.py run \
  --manifest /absolute/workspace/plan.json --expected-sha256 <plan-sha256>
```

The supervisor uses the request's separately hash-bound Python for model workers.
The default `blocking_units` includes the current fast-screen service: a run refuses
to launch inference while that service is active. Run one diarization worker at a
time and do not start a competing GPU campaign during a recording. GPU allocator
fraction is not a hard bound on all driver allocations; pilot measurements remain
required.

`status.json` reports setup blocks, active normalization/inference, interruption,
failure, or completed/remaining counts. Stop only the diarization unit to cancel it.
Running the same immutable plan again resumes completed recordings; there is no
automatic retry loop or unbounded background watcher.
