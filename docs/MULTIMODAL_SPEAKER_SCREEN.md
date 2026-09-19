# Private multimodal speaker triage

This is a separate screening pipeline, not a replacement for existing results or
full diarization. It does not modify acquisition, ASR, Gemini summaries, speaker
identities, publication state, or the old archive screen. It uses existing local
models, makes no paid requests, and does not upload media. A full-archive
continuation started on September 13, 2026 UTC after the title-enriched benchmark
below. Outputs remain review cues, not verified speaker counts.

## Why this approach

The completed-result audit of the September 12 fast screen found 2,678 terminal
results: 2,670 uncertain, eight sampled negatives, and zero multiple-speaker
candidates. Its strict selection kept only the longest uninterrupted VAD-positive
excerpt from each probe. That can lose short replies and speech interrupted by
natural pauses. Its 0.85 complete-link similarity rule also fragmented evidence:
one nearly 12-hour recording had 34 usable embeddings whose maximum pairwise
cosine was only 0.809. This does **not** establish that those voices are the same
person; it demonstrates why the old rule could not join any of those samples.

The new cascade is:

1. Reuse hash-checked, source-bound embeddings from completed old results.
2. Decode sparse frames across each video and run CPU YuNet face detection.
3. Sample extra audio around frames showing multiple faces and previously
   observed speech without usable embeddings, while keeping an independent audio
   baseline for calls, voiceover and off-screen speakers.
4. Use VAD hysteresis and several non-overlapping speech excerpts per audio probe.
5. Rank repeated, separated acoustic groups and visual cues for review. Do not
   force two clusters or assign a speaker count.

YuNet is a lightweight face detector, not an active-speaker or identity model.
This implementation uses the already-pinned 2023 model compatible with the local
OpenCV 4 runtime. See [OpenCV's model documentation](https://github.com/opencv/opencv_zoo/tree/main/models/face_detection_yunet).

The VAD change follows the hysteresis and minimum-silence pattern in
[Silero's implementation](https://github.com/snakers4/silero-vad/blob/master/src/silero_vad/utils_vad.py).
The exact excerpt-selection and acoustic-ranking policy here is a local,
uncalibrated heuristic. Cosine values are not probabilities or universal speaker
verification thresholds; [SpeechBrain's ECAPA model card](https://huggingface.co/speechbrain/spkrec-ecapa-voxceleb)
describes its speaker-embedding purpose and training domain.

For a later shortlist, audiovisual active-speaker detection is more meaningful
than face counting: it models temporal correspondence between speech and faces.
[TalkNet](https://arxiv.org/abs/2107.06592) and
[LR-ASD](https://github.com/Junhua-Liao/LR-ASD) are relevant primary references.
LR-ASD is the preferred next benchmark in the existing private model ADR; it is
not calibrated or invoked by this new screen. Its published benchmark
results do not establish accuracy on this archive. Dense active-speaker analysis
of every video is not necessary for this first triage pass.

## Bounds and interpretation

| Work | Per-recording bound |
| --- | --- |
| Baseline video frames | At least three when possible; one-minute target spacing, capped at 32 and spread across the full hinted interval |
| Visual confirmations | Up to eight extra frames, one second after a multiple-face sample |
| Frame resolution | Aspect-preserving 640 × 360 letterbox; YuNet score at least 0.9; faces smaller than 12 pixels excluded |
| Fresh audio | Up to four uniform 10-second baseline probes, plus cue-adjacent probes up to 12 total; shorter clips use fewer probes |
| Speech excerpts | Up to three real, non-overlapping 2–3-second excerpts per fresh probe |
| Cached audio | At most 64 spread embeddings per recording |
| Decoder | One thread, 20-second wall timeout, 1 GiB address-space limit, bounded output |
| Model worker | One CPU thread, 4 GiB address-space limit, no GPU, denied network |

The video and audio models run in separate, sequential resident workers because
their existing isolated runtimes have different NumPy versions. Loading each
model once avoids per-recording startup costs. Frames and audio waveforms are
ephemeral; private checkpoints retain timestamps, hashes, boxes, and anonymous
voice vectors. These vectors are sensitive evidence and must not be published.

The repeated-acoustic-core heuristic needs two independent, non-overlapping
probes per core and at least four seconds of VAD-positive speech in each core.
Within-core cosine must be at least 0.60, cross-core at most 0.35, with a 0.20
anchor margin. These are **review-routing settings, not calibrated accuracy
claims**. Search is capped at 64 candidate anchor pairs, and the result says when
that cap applies. Isolated outliers cannot establish another voice, and processing
the same audio twice cannot create independent supporting votes.

The VAD can keep real pauses shorter than 256 ms inside an excerpt, but at least
65% of that excerpt must be VAD-positive. It neither removes pauses nor joins
separate utterances into a synthetic waveform. This can still include a speaker
change inside an excerpt; it is not turn-level diarization.

Multiple faces do not prove multiple speakers. Bystanders, mirrors, posters,
reaction videos, and prerecorded playback need review. Identical frame hashes
cannot create repeated corroboration, but changing compression or overlays on a
static image can defeat that narrow guard. Sparse frames can miss side profiles,
small faces, alternating camera shots and short appearances. No faces is never
negative audio evidence. No detected acoustic diversity is never proof of a
single-speaker recording. Titles select/prioritize examples; they are not labels.

`priority` audio refresh skips new audio only when sufficient cached samples have
no supported diversity and no multiple-face samples occur. This is a speed/recall
trade-off, not a high-recall guarantee. `all` always performs the fresh baseline
and applicable cue sampling; use it for evaluation. `none` performs visual checks
and cached-evidence ranking without new audio inference.

## Running and resuming

The wrapper is `pipeline/bin/speaker-screen-multimodal`. Preparation is metadata
only: it verifies acquisition receipts and old completed-result proofs without
rehashing or decoding the entire media files. A source's acquisition content hash
is trusted with its current size/stat witness. This is not full disk verification.

Create a **new**, private state directory under an existing owned mode-0700
parent. The state directory must not already exist. Supply the SHA-256 of the
inventory, explicit local assets/runtime paths, and optionally repeated
`--media-id media_sha256_...` selections:

```sh
pipeline/bin/speaker-screen-multimodal prepare \
  --inventory /absolute/path/to/inventory.json \
  --expected-sha256 INVENTORY_SHA256 \
  --audio-results-root /absolute/path/to/archive-fast-20260912 \
  --face-assets /absolute/path/to/asset-manifest.json \
  --audio-models /absolute/path/to/models.json \
  --audio-python /absolute/path/to/audio-runtime/venv/bin/python \
  --state-root /absolute/private/parent/new-screen \
  --limit 16 --max-seconds 1800 --audio-refresh priority
```

The returned manifest path and SHA are required for both execution and status:

```sh
pipeline/bin/speaker-screen-multimodal run \
  --manifest /absolute/private/parent/new-screen/manifest.json \
  --expected-sha256 RETURNED_MANIFEST_SHA256

pipeline/bin/speaker-screen-multimodal status \
  --manifest /absolute/private/parent/new-screen/manifest.json \
  --expected-sha256 RETURNED_MANIFEST_SHA256
```

The default selection is the first 16 inventory records, not the entire archive.
An explicit selection cannot exceed `--limit`; the hard selection cap is 4,096.
The execution budget is per invocation, not an automatic restart loop. If the
budget expires between samples, the state remains `incomplete`; the same `run`
command resumes committed work. SIGINT preserves completed checkpoints. Inspect
the returned state as well as the exit status. Only one runner may hold the
workspace lock.

Implementation, policy, models, interpreter and decoder executables are pinned.
Changing implementation or policy requires a new manifest/workspace. Do not edit
an old manifest's hashes to force a resume. The running Gemini manifest pins old
speaker modules; this feature deliberately adds new modules without changing
those files.

Individual unsupported samples remain `needs_review`; they are not silently
treated as silence or single-speaker evidence. Storage I/O errors stop the run.
Non-zero container timestamps are retained, and valid short EOF samples are
accepted with their actual duration, without silence padding. Unverified duration
hints remain explicitly marked; no full-file EOF verification is claimed.
When decoded audio timestamps are discontinuous, only the longest verified
continuous subinterval is retained; all discarded samples are counted. No
timestamp relaxation, waveform retiming, interpolation to fill gaps, or joining
discontinuous pieces is used. A fragmented probe without a two-second continuous
interval remains `needs_review`. This is deliberate evidence loss rather than
pretending the entire requested probe was continuous.

Each job keeps a probe receipt, frame and audio checkpoints, `visual.json`, and a
final `result.json`. Status replays completed-result hashes, sampling schedules,
timing receipts and aggregate decisions without decoding media or loading models.
Visual-only completion counts during a run are progress telemetry, not completed
audio/visual decisions. A fully completed rerun also checks current source
witnesses and launches no workers. Unavailable audio/video and failed samples
remain separately visible in counts.

## Validation

The policy and recovery tests are in
`pipeline/tests/test_speaker_screen_multimodal.py`. They cover multiple excerpts,
outlier rejection, repeat support, faces-not-speakers semantics, off-screen audio,
bounded sampling, native non-zero PTS/short-EOF decoding, source changes,
checkpoint integrity and completed replay without model launches.
All 414 speaker-screen regression tests passed, including 39 multimodal tests.

## Title-enriched benchmark and archive continuation (September 13)

The private audit is `research/corpus/speaker-screen-campaigns/title-audit-20260913/`.
It selected 15 likely conversation/interview titles plus six seeded random
recordings in each of four duration bands (24 timing controls). These are
unlabeled recordings: titles are selection cues, not ground truth.

All 39 completed in 446.79 seconds, with five individual sample reviews and one
unavailable audio stream. Results include three acoustic-diversity candidates
and five repeated visual cues, with overlap between those counts. `Live Q&A with
Mila` and `Live Q&A with my Sister` have both repeated acoustic groups and visual
cues. Thirteen sampled frames were inspected separately. The gameplay candidate
includes channel portraits/thumbnails and possible playback, while another
recording shows recursive copies of a webcam view. Neither establishes several
live participants. Masks, obscured faces, camera operators and off-screen calls
remain important limitations. No listening review or numerical speaker-detection
precision/recall is claimed.

A bounded follow-up added 16 non-overlapping-with-baseline ten-second probes to
each of four difficult call/social examples (64 probes, 33.29 seconds). It added
51 usable excerpts without establishing another supported acoustic group.
Thresholds were not relaxed to make expected titles pass. These files remain
uncertain; more samples are not themselves proof of another speaker.

The duration-weighted estimate is 10.27 hours for all 4,063 unique payloads under
the tested `audio_refresh=all` policy. Plan for roughly **10–14 hours**, excluding
human review, diarization and further targeted passes; drive contention or sample
failures can extend it. The small-sample bootstrap interval (9.52–11.08 hours)
does not include systematic caching or workload differences. Exact inputs,
stratum weights and timings are in `report.json` and `execution.json`.

The 39 completed benchmark results are preserved and excluded from the new run.
`archive-partition.json` proves the disjoint 39 + 4,024 selection. The continuation
reuses old audio evidence for 2,654 of its 4,024 recordings and performs fresh
audio screening for all eligible recordings. No full media hashes, new model
downloads, paid APIs, source copying or deletion are involved.

The user service is `himr-speaker-multimodal-archive-20260913.service`, with a
4 GiB aggregate memory ceiling, no swap, low CPU priority and no automatic
restart. Its selected model workers remain CPU-only and network-denied. This
version finishes the archive-wide **frame pass first, then the audio pass**:
`visual_complete` is meaningful progress before final `complete` counts rise.
Both counts exclude the 39 results retained in the separate benchmark.

```sh
systemctl --user status himr-speaker-multimodal-archive-20260913.service --no-pager
pipeline/bin/speaker-screen-multimodal status \
  --manifest /srv/himr/research/corpus/speaker-screen-campaigns/multimodal-archive-20260913-v1/manifest.json \
  --expected-sha256 0f2d833c55ec0b3b261e8136dfc1ad88d4e2740980f3021e42910e0622a30ab4 | jq '.counts'
```

The per-invocation worker deadline is 24 hours. An incomplete run resumes with
the same manifest and code, retaining atomic checkpoints. Do not run another
process against the same workspace or change pinned implementation files while
the service is active.

The initial mixed-media pilot is retained at
`research/corpus/speaker-screen-campaigns/multimodal-pilot-20260913-v1/`, with its
findings in `REVIEW.md`. Its follow-up uses a new sealed workspace,
`research/corpus/speaker-screen-campaigns/multimodal-pilot-20260913-v2/`.
Its selection includes calls, social videos, a short interview, a nearly 12-hour
stream, a 20-second clip, an ordinary long stream, and an Ogg video recording.
Native probing corrected the initial assumption that the Ogg file was audio-only;
a separate native generated-WAV test covers the no-video path.
It uses `audio-refresh all` for a matched-probe comparison of old versus new
speech-excerpt eligibility. More excerpts means more sampled evidence, **not** a
measured improvement in speaker-detection accuracy. The selection is not a
representative or speaker-labeled evaluation set.

The revised pilot completed **12 recordings totaling 19.04 hours in 140.20
seconds**, by sparse sampling. All 212 frames decoded; 63 of 65 audio probes were
usable. It retained 35 new speech excerpts versus 10 eligible under the old rule
on the **same decoded audio**, plus 50 cached embeddings. Nine discontinuous
audio probes were salvaged without retiming; two remained too fragmented and
explicitly need review. It routed one repeated multi-face cue for audio review;
there were zero supported acoustic-diversity candidates. This is improved
evidence collection and recovery, not a demonstrated speaker-detection accuracy
gain. Six recordings still had insufficient audio evidence.

Frame decoding took 94.16 seconds versus 4.72 seconds for face detection. A GPU
face detector would therefore address only a small fraction of this pilot's
runtime. A seven-file keyframe-only experiment saved about 25% of decoder time
but could shift samples more than five seconds earlier; it was not enabled as
the default. Detailed results and limitations are in the v2 `REVIEW.md`.

To inspect the completed revised pilot without starting any models:

```sh
pipeline/bin/speaker-screen-multimodal status \
  --manifest /srv/himr/research/corpus/speaker-screen-campaigns/multimodal-pilot-20260913-v2/manifest.json \
  --expected-sha256 0bc4bb20bf350efe75c8f5f4b0f6e7bc2c9f5abaa68b82f06edede05ac63710c
```
