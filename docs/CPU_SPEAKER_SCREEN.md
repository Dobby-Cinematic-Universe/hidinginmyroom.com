# Standalone CPU speaker screen

For model reuse across recordings, overlapping decoding, and optional CUDA
embeddings, see the separate [resident CPU/GPU screener](RESIDENT_SPEAKER_SCREEN.md).
The original CPU implementation and its checkpoints remain unchanged.

This is an optional, separate screening tool for explicitly supplied local
recordings, run individually or in a bounded parallel batch. It does not join,
start, stop, or change the autonomous pipeline. It
does not alter the console, controller, campaign configuration, catalogue, source
media, transcripts, or the inactive hybrid workspace. There is no daemon,
automatic discovery, acquisition, cloud upload, or scheduling integration.

It samples deterministic short audio windows, applies speech detection and
speaker embeddings on the CPU, and records a private screening result. This is
not transcription, full-recording diarization, named-person identification, or a
guarantee about portions of the recording that were not sampled. A completed
screen means the screening plan finished; it does **not** mean “ASR complete.”

The model adapter, checkpoint resume, faster classification, and parallel batch
runner are implemented. A separate CPU environment and hash-verified models are
installed in `research/corpus/speaker-screen-runtime-20260912`; see its private
`README.md` and `runtime-setup.json` for reproducibility and pilot measurements.
Real decoding, VAD, embeddings, and parallel checkpoint replay passed a bounded
two-recording pilot within the existing offline and 4 GiB address-space limits.
This does not establish screening accuracy or justify a full-archive launch.

## Work order and commands

Start from [the example work order](../pipeline/examples/speaker-screen-work-order.example.json).
Every path and zero-filled digest in it is a placeholder. Replace them with
explicit local inputs before use; the example is not an activation configuration.
The [JSON Schema](../pipeline/schemas/speaker-screen-work-order.schema.json)
checks document structure. Runtime checks additionally enforce file identity,
safe paths, private workspace ownership, and checkpoint consistency.

The work order binds the recording ID, absolute file path, supplied SHA-256,
byte count, and duration; the local FFmpeg binary; both local model artifacts;
screening policy; bounded resource limits; verification mode; and a dedicated
output directory. Empty `policy: {}` selects the core defaults. Omitted policy
fields use those defaults; unknown fields are rejected.

Defaults are ten-second probes at a target density of one per minute, capped at
512 probes. Probes are spread across the recording rather than limited to its
opening portion; a single probe is centered. Usable excerpts require at least
two seconds of contiguous speech, and a supported similarity group requires
evidence from at least two separate probes. Matching and distinct-group cosine
thresholds default to 0.85 and 0.65 respectively. These thresholds are not
calibrated probabilities. Runtime also checks that stride is at least probe width
and the distinct threshold is below the matching threshold; JSON Schema alone
does not express those merged-policy comparisons.

Calculate the SHA-256 of the **work-order file's actual bytes** after editing it.
The commands below contain placeholders; do not run them without real bindings.
Run from the repository root using the separate environment's Python interpreter.
These invoke the Python entrypoint directly. The equivalent
`pipeline/bin/speaker-screen` shell wrapper uses the `python3` selected by your
shell environment; do not pass that shell wrapper to Python as a script.

```sh
"/ABSOLUTE/PRIVATE/PATH/speaker-screen-venv/bin/python" -B \
  "pipeline/speaker_screen.py" plan \
  --work-order "/ABSOLUTE/PRIVATE/PATH/screen-work-order.json" \
  --expected-sha256 "REPLACE_WITH_WORK_ORDER_SHA256"

"/ABSOLUTE/PRIVATE/PATH/speaker-screen-venv/bin/python" -B \
  "pipeline/speaker_screen.py" run \
  --work-order "/ABSOLUTE/PRIVATE/PATH/screen-work-order.json" \
  --expected-sha256 "REPLACE_WITH_WORK_ORDER_SHA256"

"/ABSOLUTE/PRIVATE/PATH/speaker-screen-venv/bin/python" -B \
  "pipeline/speaker_screen.py" status \
  --work-order "/ABSOLUTE/PRIVATE/PATH/screen-work-order.json" \
  --expected-sha256 "REPLACE_WITH_WORK_ORDER_SHA256"
```

`plan` reads the work order and the tool's own source metadata, without opening
the recording or models or creating a workspace. `status` reads bounded metadata
without decoding media or changing checkpoints. `run` is the only processing
command: each explicit invocation does bounded work and resumes completed-window
checkpoints automatically. Invoke it again if the result reports unfinished work.

The example requests one CPU thread, a 120-second window timeout, at most 3,600
seconds per invocation, and at most 512 windows per invocation. This keeps the
model loaded for more probes before a bounded pause. Limits are bounds,
not a throughput promise. The work-order schema permits 1–2 threads, 10–600 seconds
per window, 10–3,600 seconds per run, and 1–512 windows per run. Input bounds are
64 GiB and 24 hours. The engine analyzes at most ten seconds of PCM per probe.
The model child has a fixed 4 GiB address-space ceiling; the decoder has a 1 GiB
ceiling. These are virtual-memory limits, not measured resident-memory usage or
allocation targets. Both run at reduced CPU priority, with CPU-time limits and
parent-death cleanup. Linux `libseccomp` blocks new IPv4/IPv6 sockets, and local
worker communication has deadlines covering both sends and receives. The isolated
runtime passed model loading and a short real-media pilot within these limits;
long-run/corpus-wide throughput still needs measurement.

## Faster presets and parallel batches

`prepare` seals a **new** work order and requires a disjoint new output workspace;
it never processes media or modifies the original work order/checkpoints.

```sh
"/ABSOLUTE/PRIVATE/PATH/speaker-screen-venv/bin/python" -B \
  pipeline/speaker_screen.py prepare \
  --work-order "/ABSOLUTE/PRIVATE/PATH/original-order.json" \
  --expected-sha256 "REPLACE_WITH_ORIGINAL_ORDER_SHA256" \
  --preset throughput \
  --output "/ABSOLUTE/PRIVATE/PATH/faster-order.json" \
  --output-root "/ABSOLUTE/PRIVATE/PATH/faster-screen" \
  --threads 1
```

- `throughput` preserves the original sampling policy and thresholds. It raises
  invocation bounds to 512 probes/3,600 seconds, with early stopping disabled.
  The default policy remains ten seconds per target minute, capped at 512 probes.
- `fast-triage` retains the similarity/speech-support thresholds but uses ten-second
  probes at a target density of one per five minutes, capped at 64. Probes still
  span the whole recording. This is a faster, **less sensitive** first pass; short
  guest appearances may be missed. It enables supported-positive early stopping.
- Both presets accept one or two model CPU threads. No defaults or thresholds in
  an existing sealed work order are silently changed.

For the 4,488 admitted recording durations as of 2026-09-12, the default policy
plans 311,135 probes; `fast-triage` plans 55,512: **82.2% fewer probes** (5.60 times
less probe work), before positive early stopping. These are metadata-based work
counts, not a measured wall-clock speedup, accuracy result, or confirmation that
every source decodes. The earlier four explicit ASR skips still need screening
input validation.

Positive early stopping is optional (`resources.early_stop_on_positive`, default
false). Evidence is checked after geometrically increasing probe counts, and at
the invocation boundary. Only a supported `multiple_speaker_candidate` can end a
screen early; absent/ambiguous speech or a sampled negative cannot. In-memory
similarity caching also speeds checkpoint summaries without changing thresholds,
classifications, or evidence order. Synthetic 256-probe/192-dimensional checks
measured 1.66–3.08 times faster final summaries and 6.14 times faster repeated
summaries; these are Python classification timings, not end-to-end inference.

The separate [batch request example](../pipeline/examples/speaker-screen-batch-request.example.json)
lists 1–128 exact work-order paths and SHA-256 digests. Its
[schema](../pipeline/schemas/speaker-screen-batch-request.schema.json) accepts
concurrency 1–4 (default 2) and a batch wall-clock bound of 10–86,400 seconds
(default 3,600). Use the same isolated interpreter for planning and execution;
the manifest binds its executable, version, virtual-environment configuration,
work orders, and implementation. Each recording and the batch itself must have
disjoint private output roots, outside their inputs/models/tools.

```sh
"/ABSOLUTE/PRIVATE/PATH/speaker-screen-venv/bin/python" -B \
  pipeline/speaker_screen_batch.py plan \
  --request "/ABSOLUTE/PRIVATE/PATH/batch-request.json" \
  --expected-sha256 "REPLACE_WITH_REQUEST_SHA256" \
  --output "/ABSOLUTE/PRIVATE/PATH/batch-state/manifest.json"

"/ABSOLUTE/PRIVATE/PATH/speaker-screen-venv/bin/python" -B \
  pipeline/speaker_screen_batch.py run \
  --manifest "/ABSOLUTE/PRIVATE/PATH/batch-state/manifest.json" \
  --expected-sha256 "REPLACE_WITH_MANIFEST_FILE_SHA256"
```

Batch `plan` seals metadata in its new private workspace but does not open source
media or models. Batch `status` takes the same arguments as `run` and only replays
metadata/checkpoints. Each explicit `run` gives each unfinished recording **at
most one bounded invocation**. Finished decisions are reused; paused recordings
resume on the next explicit run. There is no automatic infinite retry, discovery,
or full-archive scheduler. Cancellation/time limits terminate active workers and
retain committed checkpoints. A private batch lock prevents overlapping runs.
Worker errors return exit status 2, cancellation 130; a normal bounded pause may
return 0, so inspect status rather than treating exit 0 as complete.

Start with two workers and one model thread each. Four workers are an explicit
option, not a promise of linear speedup: archive seeks and memory bandwidth can
become limiting. No batch is connected to acquisition, ASR, or publication.

### Measured local pilot (2026-09-12)

On this Ryzen 7 3700X host, two retained recordings were each tested with the same
eight ten-second probes at one and two model threads. One-thread runs took 6.963
and 6.965 seconds; two-thread runs took 6.910 and 6.654 seconds, including model
startup and checkpoint writes. Both recordings produced real embeddings, their
decoded PCM hashes matched across thread counts, and every planned probe finished.
Two threads reduced these startup-heavy runtimes by only 0.8% and 4.5%; they are
not evidence of a large per-recording speedup. Peak model-child RSS was about
652 MB (not the sum of all processes).

A separate two-worker/one-thread-per-worker batch completed four probes per
recording (eight total, including three embedding-bearing excerpts) in 5.952
seconds. This validates concurrent execution, not a like-for-like parallel speedup
comparison with the eight-probes-per-recording runs. Replaying that finished batch
took 0.0585 seconds and launched no workers or new receipts. All workers exited.
The original failed endpoint diagnostic was preserved; 49 probe attempts in total
were made across the pilot, not a full-archive screen. These two inputs and short
runs do not establish corpus accuracy or an archive-wide ETA. Detailed input,
runtime, and measurement provenance remains in the private runtime directory.

## Reading results

The private `result.json` contains evidence timestamps and coverage, but no raw
embedding vectors. Checkpoints contain private vectors needed for resume.
`state: completed` always means every planned probe finished. With positive early
stopping, a sealed result may instead have `state: paused`,
`screening_decision_complete: true`, and `stop_reason: supported_multiple_speakers`;
its uninspected/remaining coverage is retained explicitly. A completed sampling
plan has `stop_reason: sampling_plan_completed`; an unfinished bounded invocation
has `stop_reason: invocation_limit`. Batch statistics report screening decisions
and completed sampling plans separately. Neither counter is an ASR statistic.

- `multiple_speaker_candidate`: two supported, sufficiently distinct sampled
  voice groups were found; playback and microphone changes still require review.
- `no_second_voice_detected_in_sampled_audio`: one supported group was found
  after completing the sampling plan; unsampled audio may contain another voice.
- `uncertain`: insufficient speech/support, isolated outliers, ambiguous groups,
  or an unfinished plan without positive evidence.

Coverage distinguishes whole probe audio inspected by VAD from the shorter speech
excerpts actually compared. Neither group count nor sampling completion is an
exact person count. Partial results are snapshots and cannot clear the video as
single-speaker. Evidence positions are integer milliseconds, not word-level
speaker annotations. `status` does not recheck current source-file availability.

## Source identity and timeline

The default `source_verification: "metadata_witness"` treats the supplied recording
SHA-256 as provenance, not as a hash newly checked against the entire media file.
It checks the declared byte count and records a filesystem identity witness.
Resume must fail closed if that witness changes. Decoded PCM is separately hashed
and bound to its window checkpoint; a PCM hash does not verify unsampled media.

Use `source_verification: "sha256"` when a full source hash check is wanted. That
adds whole-file reads. Neither mode authorizes rewriting source files or silently
accepting changed media into an existing checkpoint sequence. A copied/replaced
file can have a different witness even when its supplied content hash is unchanged;
do not bypass that mismatch by editing checkpoint files.

An original video/audio file and a long-form normalized recording are different
inputs. For a normalized long-form input, copy the audio path, SHA-256, and byte
count from the explicitly selected `recording.input` manifest entry, while
preserving its recording's `media_id`. Derive the integer screening duration as
`total_samples * 1000 // sample_rate_hz` from that input's exact sample metadata;
do not copy a rounded-up presentation duration. This excludes only the final
fractional millisecond, rather than requesting nonexistent samples. Retain the
exact sample count/rate in input-preparation provenance. The first raw-AAC pilot
exposed this distinction: its last probe was one 16 kHz sample (0.0625 ms) short,
and the screener correctly retained earlier checkpoints without admitting it.
That ID can refer to the original media;
it is not necessarily the normalized audio's content hash. Do not feed the JSON
manifest itself to FFmpeg or regenerate/preprocess an entire recording merely
to screen it.

If the normalized file has already been cleaned up, use the retained original
media as a **new, separately bound input**, not a stale normalized locator. For
the tested MP4/AAC inputs, exact audio-stream `duration_ts` and rational
`time_base` from bounded local FFprobe inspection supply the duration:
`floor(duration_ts * time_base * 1000)`. Use integer/rational arithmetic and retain
that stream metadata as provenance; the rounded container/format duration can
request a nonexistent final sample. Correctly floored raw-stream metadata fixed
the pilot's final-probe failure without padding, source changes, or relaxed
validation. Never apply a normalized input's duration to a different raw timeline.

Reported positions refer to the selected input's audio timeline. A normalized
FLAC starts on its normalized-recording timeline; its manifest does not establish
an exact mapping to an original container's nonzero timestamps or editing history.
Do not relabel those offsets as independently verified original-video timing.

## Models and interpretation

Both model artifacts are explicit local files with SHA-256 bindings. Runtime
loading is offline and CPU-only; missing files or dependencies are errors, not
permission to download a replacement model. There are no model-service keys.

The expected files are the compatible Silero VAD ONNX model and the raw
`embedding_model.ckpt` from `speechbrain/spkrec-ecapa-voxceleb`. The ECAPA adapter
loads weights into its fixed local architecture with CPU, weights-only loading;
it does not execute a downloaded YAML recipe or require a TorchScript export.
No automatic model downloader or model-export helper is supplied. The isolated
2026-09-12 setup records upstream revisions, verified artifact hashes, and retained
MIT/Apache-2.0 license/model-card snapshots in `model-provenance.json`.

The candidate [runtime requirements](../pipeline/speaker-screen-runtime-requirements.txt)
target Python 3.12, PyTorch 2.8.0 CPU,
torchaudio 2.8.0 CPU, SpeechBrain 1.0.3, ONNX Runtime 1.22.1, and NumPy 2.2.6.
Use a new private virtual environment and explicitly selected CPU wheels, not
the environment serving the autonomous pipeline. This short requirements file is
not a complete transitive lock. The installed isolated environment additionally
has `requirements.lock.txt` and `wheel-inventory.json`: 37 wheels verified against
official indexes and installed offline with required hashes; `pip check` passed.
Its copied Python 3.12.14 executable still shares the base Python standard library,
as a normal virtual environment does; this is not a portable deployment image.

The engine selects the longest contiguous speech-positive excerpt of at least
two seconds within a probe, capped at five seconds, taking the first on ties.
Embeddings describe only that selected excerpt. When no qualifying excerpt exists,
the probe has no embedding; it is not evidence that the entire recording contains
no speech. Short/noisy speech, overlapping voices, music, channel changes, and
sampling gaps can affect the result. Treat scores and categories as uncalibrated
screening hypotheses requiring review, not identity evidence or probabilities.

Embeddings and checkpoints remain private to this recording's workspace. They
are not exported as a person-identity index or reused for cross-recording identity
search. A short screening excerpt is not a reviewed, diarization-clean identity
reference.

## Isolation and recovery

Use a new, dedicated output directory outside all current campaign, source,
model, transcript, and hybrid state trees. Runtime requires a private marked
workspace and refuses an unrelated nonempty directory. Its parent must already
exist; the tool creates only the explicitly selected leaf directory. Path
components cannot be symlinks. Private output directories are retained by file
descriptor, and creation/publication is relative to those descriptors so an
ancestor rename cannot redirect writes. Read-only inputs are retained and their
configured hash or source witness is checked. No home-directory permission change
is needed. Do not copy these artifacts
into existing catalogue or ASR completion-receipt locations.

CPU-only does not mean zero impact: FFmpeg decoding uses storage bandwidth and
CPU time. Keep the conservative limits while the autonomous pipeline is running;
there are no measured throughput or “free background capacity” guarantees.

Completed-window checkpoints are resumable, but missing, changed, or inconsistent
evidence is not a fresh screen. Inspect the reported error and preserve the
workspace for diagnosis. Do not reset its state, delete source data, or restart the
running autonomous campaign to recover this separate tool.

Artifacts use atomic, non-overwriting publication (`renameat2` on Linux). A sealed
final result must replay against all its checkpoints before any further work;
missing/corrupt evidence is not silently replaced. Configuration or implementation
changes produce a different plan identity. Runtime-version changes cannot be
mixed into an existing checkpoint sequence.

## Offline contract checks

```sh
python3 -m unittest \
  pipeline.tests.test_speaker_screen_core \
  pipeline.tests.test_speaker_screen_engine \
  pipeline.tests.test_speaker_screen \
  pipeline.tests.test_speaker_screen_schema \
  pipeline.tests.test_speaker_screen_batch
```

These checks use synthetic metadata, synthetic PCM, a real bounded FFmpeg decode,
an isolated model-free silence worker, and mocked model fixtures; they do
not process corpus media, load real model weights, acquire live pipeline locks,
or make network requests. They are not a substitute for the separately authorized
live model/accuracy pilot.
