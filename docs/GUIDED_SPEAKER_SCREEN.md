# Metadata-guided speaker screening

This is a **separate opt-in CPU/CUDA screen**, not a change to acquisition, ASR,
the CPU-v1 screen, or the existing resident GPU screen. Existing input work
orders are read-only templates. All plans and checkpoints live in a new private
workspace with distinct guided kinds/IDs. No full-archive screen is started by
installation or planning.

## What guidance changes

| Signal | Effect | What it does not establish |
| --- | --- | --- |
| Acquisition title | Explainable queue priority for interview/collaboration/call/guest cues | That the named person appears or speaks |
| Filename/upload date near an explicitly reviewed event | Small priority increase, with date basis and distance retained | A recording date, shared speakers, or identity |
| Aligned chapter/transcript/reviewed interval | Additional probes near the cue and nearby confirmation opportunities | That the transcript is true or a second voice is audible |
| Missing/generic metadata | Neutral priority, same baseline coverage | A single-speaker recording |

Priority rules are deterministic literal text rules, not model instructions or
calibrated probabilities. Title scores are capped at 60; reviewed-date proximity
adds at most 5; accepted timed cues add 20. Ties preserve original request order.
Dates come from the acquisition source's filename or timezone-qualified publish
timestamp, **never filesystem/rsync times or Archive ingestion dates**. Invalid
filename dates stay unknown. Upload/filename dates remain weak contextual hints.

Every original uniformly dispersed probe is retained unchanged in location.
Guidance adds at most `max_target_windows` probes per recording (default example
16; explicit range 0–64), to the original at-most-512 baseline. Extra probes are
full-width, chronological, and nonoverlapping with both baseline and other
extras. Placement shares the budget among hints and reports existing coverage,
omitted requests, and unprobed portions; no hint is guaranteed exhaustive coverage.
There are at most 32 selected hints per recording. Cue-dense inputs are sampled
across their timeline rather than taking only their first 32 cues.

Speaker decisions still use only recording-local audio embeddings and unchanged
acoustic thresholds. No cross-recording voice cache or named-person inference is
introduced. Supported groups are screening candidates, not person counts.

## Inputs and alignment

Start with the [guidance example](../pipeline/examples/speaker-screen-guidance.example.json).
Its `records` associate existing screen media IDs/SHA-256s with:

- `acquisition_result`: an exact completed acquisition result, or `null`.
  Titles/dates are extracted automatically only after its media ID, SHA-256, and
  byte count match the screen input. Container duration is not substituted for
  the screen's exact audio duration.
- `timed_cues`: an exact normalized timed-cue capsule, or `null`.

Omitted records receive neutral guidance and keep all baseline probes. Extra,
duplicate, or mismatched recording IDs are rejected. There is no catalog scan or
automatic network metadata lookup. All supplied metadata and evidence references
are SHA-256-bound, locally read, and size-limited (16 MiB per file, 128 MiB total).

The [timed-cue example](../pipeline/examples/speaker-screen-timed-cues.example.json)
accepts chronological millisecond segments of kind `transcript`, `chapter`, or
`reviewed_interval`. Literal introduction/conversation cues select transcript
segments; chapter titles also use the stronger title rules. A reviewed interval
can select a timestamp explicitly. Raw SRT/VTT and arbitrary provider formats are
not automatically ingested in this version: prepare a normalized capsule first.

**All timed-cue capsules require an explicit alignment review attestation**
(`reviewed: true` and a hash-bound `review_evidence` file), including local ASR.
This flag is an operator attestation, not a claim that this tool performed a human
review. Do not copy it from the example without an actual alignment check.
Hashing a transcript proves its bytes, not that its text/timing matches a video.
The tool validates capsule structure and mapping bounds; it does not compare the
capsule's text against the referenced transcript or authenticate the reviewer.

`same_media` requires matching exact media SHA-256, exact audio duration, and zero
offset. `reviewed_offset` permits a reviewed constant-offset mapping from another
source timeline. Every mapped segment must fit the exact screen timeline. Edited
or time-stretched transcripts need separate mapping work, not a guessed offset.
This is particularly important for `HIMR-Transcripts` third-party material and
normalized ASR audio versus retained raw containers. Missing alignment evidence
fails closed; no existing transcript is automatically declared aligned.

Optional `events` are file bindings to
[reviewed event records](../pipeline/examples/speaker-screen-reviewed-event.example.json).
They require an explicit reviewer, evidence file, calendar date, and a bounded
0–14-day radius. They affect priority only. No events or review attestations are
invented from neighbouring video titles.

## Planning and running

Use the [guided request example](../pipeline/examples/speaker-screen-guided-request.example.json).
Replace all placeholder paths/digests and the GPU UUID. List 1–128 existing CPU
work-order references and a guidance-file reference. Use a new disjoint
`state_root`, whose parent already exists and is private mode 0700. The new root
must not overlap old output roots, media, models, metadata/evidence, or tools.
The 16 MiB manifest limit may require smaller batches for large sampling plans.

Use the same reviewed isolated interpreter as the resident screen. On this host
the existing GPU interpreter is:

```text
/srv/himr/research/corpus/speaker-screen-accelerated-runtime-20260912/venv/bin/python
```

Plan only (no media decoding, model loading, or background launch):

```sh
pipeline/bin/speaker-screen-guided /ABSOLUTE/ISOLATED/venv/bin/python plan \
  --request /ABSOLUTE/PRIVATE/PATH/guided-request.json \
  --expected-sha256 REPLACE_WITH_REQUEST_FILE_SHA256 \
  --output /ABSOLUTE/PRIVATE/PATH/guided-state/manifest.json
```

Run a finite GPU batch inside its own 4 GiB/no-swap memory scope:

```sh
pipeline/bin/speaker-screen-guided-gpu /ABSOLUTE/ISOLATED/venv/bin/python run \
  --manifest /ABSOLUTE/PRIVATE/PATH/guided-state/manifest.json \
  --expected-sha256 REPLACE_WITH_MANIFEST_FILE_SHA256
```

For CPU use `device: "cpu"`, `gpu_uuid: null`, the existing isolated CPU interpreter,
and `speaker-screen-guided` without the GPU wrapper. CPU/CUDA runtime pins, model
residency, decoding overlap, fixed GPU batches, network denial, and resource
limits are reused unchanged. There is no silent CPU fallback or environment upgrade.

Use `status` in place of `run` for read-only checkpoint status; rerun explicitly
to continue a bounded pause. Changing guidance, target policy, source bytes,
implementation, or runtime requires a fresh plan/workspace. Old resident/CPU
checkpoints are never imported into guided plans.

## Progress, interruption, and cost

Status separates priority reasons from acoustic classification, and reports
baseline/targeted planned, inspected, and remaining probe counts. Checkpoints
contain private vectors; normal status/results do not. Atomic fixed batches,
exclusive leases, source witnesses, and metadata witnesses preserve resumability.
Guidance is hash-checked when loading and at run start, then checked for file
changes before publication without repeatedly rehashing entire transcripts.

Every recording receives at most one bounded invocation per explicit batch run.
If 512 baseline plus extras exceed its 512-probe invocation ceiling, resume with
another explicit run. No automatic full-archive scheduler or infinite retry is
installed. Exit 0 can mean a normal bounded pause: inspect status. Errors return
2 and cancellation 130. An active writer causes status to defer its snapshot.

A positive decision cannot skip unfinished baseline probes. Because this version
executes chronologically and the baseline usually includes an EOF probe, this
normally means completing the whole guided plan even after a positive appears.
It does **not** promise early-stop savings. Queue priority can surface useful
recordings sooner; additive checks can improve where we look but increase work.
The earlier resident-GPU ETA is not a measured ETA for this guided mode. No
accuracy or speed improvement is claimed without a representative evaluation.

The confirmation opportunities here are preplanned around timestamp hints;
audio-triggered adaptive resampling is not implemented. Uniform baseline coverage
remains the protection against missed metadata cues and selection bias.

## Verification

```sh
python3 -B -m unittest \
  pipeline.tests.test_speaker_screen_guidance \
  pipeline.tests.test_speaker_screen_guided_core \
  pipeline.tests.test_speaker_screen_guided \
  pipeline.tests.test_speaker_screen_guided_schema
```

These tests cover metadata provenance, alignment attestations, deterministic
placement, acoustic parity without hints, and synthetic execution/resume. They
are not a corpus accuracy calibration or a full-archive screening run.

The implementation check on 2026-09-12 passed 88 new guided tests, 177 existing
speaker-screen tests, and 430 controller tests (695 total). A 576-probe regression
also exercises the unchanged worker's stricter transport-index validation:
fixed-batch local indices are checked and restored to global checkpoint indices,
including resuming from probe 512 through 575 without changing prior evidence.

Native CPU and CUDA workers each processed three probes of a generated 30-second
silence fixture (one baseline plus two targeted), then replayed with zero new
workers/probes. Both correctly remained `uncertain`; no speech embeddings were
produced. This verifies actual decoding, model loading, IPC and checkpoint paths,
not speaker accuracy or GPU embedding throughput. Real acquisition metadata was
also admitted into unstarted CPU/CUDA preview plans. No archive audio was decoded
for these checks. Details are in
`research/corpus/speaker-screen-guided-preview-20260912/README.md`.
