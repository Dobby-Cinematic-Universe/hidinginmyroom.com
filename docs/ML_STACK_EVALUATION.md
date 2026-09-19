# HIMR Corpus ML stack evaluation

Status: active pilot decision, updated 2026-08-29

This document evaluates the machine-learning stack for the private evidence
workspace described in `CORPUS_ARCHITECTURE.md`. It is an engineering decision,
not a claim that any model output is evidence. Model availability, model cards,
and license terms can change. Exact versions and hashes identify what one run used;
they are run-integrity metadata, not a commitment to keep using old technology.
Every promoted candidate must record its own artifacts and license snapshot and beat
or justify replacing the incumbent on the common held-out evaluation.

## Decision summary

Use two execution lanes and make the inexpensive lane useful by itself:

1. **Local CPU Bronze lane:** deterministic media normalization and hashing,
   Silero VAD, faster-whisper multilingual ASR, sparse scene detection and OCR,
   and audio/video fingerprints. This lane creates searchable candidates without
   attempting to name people.
2. **Local GPU Silver-candidate lane:** higher-quality ASR decoding and
   WhisperX alignment, overlap-aware pyannote diarization, LR-ASD active-speaker
   inference, and approved private face/voice candidate clustering. Jobs are
   interval-routed; the GPU does not scan every frame of every recording.
3. **Human Gold lane:** transcript correction, public speaker/identity assertions,
   stage directions, event interpretation, and wiki claims.

The exact speaker/face/ASD artifacts, offline profiles, recording-disjoint benchmark,
and stop gates are fixed in
[`ADR 0007`](adr/0007-speaker-and-active-speaker-models.md). That ADR is a private
benchmark decision, not authorization for model downloads or corpus-wide backfill.

The recommended pilot stack is:

| Capability               | Pilot choice                                                                     | Lane                                 | Initial disposition                                                                                                  |
| ------------------------ | -------------------------------------------------------------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| Decode and normalization | FFmpeg, 16 kHz mono PCM plus source-preserving video timestamps                  | CPU                                  | Run on every admitted media object                                                                                   |
| VAD                      | Silero VAD ONNX                                                                  | CPU                                  | Run on every audio object                                                                                            |
| Multilingual ASR         | faster-whisper `small`, `medium`, and `turbo` INT8 bake-off                      | CPU                                  | Select one Bronze model after the frozen pilot                                                                       |
| High-quality ASR         | `small.en` control; Distil-Large-v3.5, Turbo, Large-v3 INT8, and Parakeet-v2 bake-off | local GPU                         | Select from the reviewed accuracy/efficiency frontier; selective second pass only if it earns one                     |
| Word alignment           | WhisperX forced alignment with an explicitly pinned aligner per language         | GPU preferred                        | English first; publish segment timing when a language is not validated                                               |
| Diarization and overlap  | pyannote `speaker-diarization-community-1`; compare Streaming Sortformer 4spk-v2 | GPU preferred, measured CPU fallback | Only likely multi-speaker recordings; preserve overlap-aware, exclusive, and raw-activity representations separately |
| Active speaker           | YuNet plus shot-local motion/IoU tracks and LR-ASD; Light-ASD fallback           | GPU                                  | Only routed speech scenes with usable faces; zero, one, or multiple visible active candidates; never identity        |
| Face candidates          | YuNet plus SFace, track-level aggregation                                        | CPU/GPU selective                    | Private, pseudonymous, review queue only                                                                             |
| Voice candidates         | SpeechBrain ECAPA-TDNN on clean diarized turns                                   | GPU preferred                        | Private, pseudonymous, review queue only                                                                             |
| Shots and OCR            | PySceneDetect 0.7.1 plus PP-OCRv6 tiny/small; PP-OCRv5 language fallback         | CPU                                  | Sparse keyframes and text-change frames only                                                                         |
| Sound events             | YAMNet with a small approved class vocabulary                                    | CPU experiment                       | Defer until transcript pilot passes                                                                                  |
| Actions                  | MMAction2 model bake-off on selected intervals                                   | GPU experiment                       | Defer; never generate transcript stage directions automatically                                                      |
| Exact/near duplicates    | SHA-256, Chromaprint, audfprint, and vPDQ                                        | CPU                                  | Run in stages on every recording/rendition                                                                           |
| Calibration              | held-out sigmoid/temperature scaling; isotonic only with enough data             | CPU                                  | No displayed probability until its task-specific calibration gate passes                                             |

Do not install this stack into the catalogue application's current Python
environment. The host has Python 3.14.6, while several ML projects and wheels lag
that release. Build separate, hash-locked Python 3.12 CPU and GPU environments or
containers after the pilot manifests have been approved.

## Resource reality

The local host is an AMD Ryzen 7 3700X with 8 physical cores, 16 threads, AVX2,
and 31 GiB installed RAM. At the time of this evaluation only about 7.4 GiB was
available and the 8 GiB swap was full. There is no usable NVIDIA GPU. The media
filesystem had about 208 GiB free. FFmpeg 8.1.2 is present and its build includes
Chromaprint.

Those facts impose the following operating rules:

- Run one memory-heavy model process at a time. Do not keep ASR, alignment,
  diarization, and OCR models resident together.
- Start CPU ASR with 8 physical-core threads, one worker, and batch size 1. Measure
  8 versus 16 threads and batch sizes 1, 2, and 4; do not assume logical threads or
  batching improve this CPU.
- Reject a local configuration that needs more than 5.5 GiB peak RSS under the
  current pressure. The scheduler must check available memory and swap before
  launch rather than trusting installed RAM.
- Chunk long streams at deterministic, overlap-preserving boundaries and free the
  model between stages. A failed chunk must be independently resumable.
- Keep a bounded media/model cache. Dense frame extraction is prohibited; frames
  are derived from timestamps and deleted after their observations are admitted.
- Treat local benchmark results as authoritative for scheduling. Upstream speed
  tables are orientation only: faster-whisper's published CPU comparison used an
  8-thread i7-12700K, not this host.

The local 6 GiB RTX 3050 is now the primary ASR runner. A future 24 GiB device remains
useful for the combined alignment, diarization, and active-speaker lane, but is no
longer a prerequisite for accurate bulk transcription. Model choice and batch size
must come from same-card measurements rather than VRAM-class assumptions. The runner
has no public serving endpoint and must not receive biometric jobs unless its
operator, location, retention, and data-processing terms have been approved.

### Local GPU follow-up — 2026-08-28

The earlier no-GPU observation above remains the historical state at evaluation
time. A local NVIDIA GeForce RTX 3050 is now accessible with 6 GiB VRAM, compute
capability 8.6, driver 610.57.04, CUDA driver API 13.3, and working NVML/device nodes.
FFmpeg 8.1.2 completed synthetic H.264 NVENC encode and CUDA/NVDEC decode. The
hash-locked `pipeline/gpu/` CPython 3.12.14 profile also completed a bounded FP16
faster-whisper smoke using CTranslate2 4.8.1, cuBLAS 12.9.2.10, and cuDNN 9.24.0.43.

The accepted schema-v2 run used a sealed 5.945-second synthetic fixture, a full-commit
`tiny.en` model snapshot, Python isolated mode, a bubblewrap network namespace with no
non-loopback interface, read-only code/model/audio mounts, an explicit result-only
writable mount, exact source/tool/lock/interpreter hashes, a 1 GiB free-VRAM floor,
and a 60-second hard deadline. It loaded the model in 0.244 seconds and inferred in
0.367 seconds (inference RTF 0.0617), with a 224 MiB sampled process VRAM peak. Receipt
SHA-256 is `6ebdcef91298fb15048df0e18d5ce0cca48fd67054047617292756a1c25c7dd8`.
This establishes local device/runtime readiness only: it is not an accuracy result,
does not approve a production model, and does not authorize corpus-wide GPU work,
diarization, biometrics, identity decisions, or publication. The 6 GiB card also does
not replace the separate 24 GiB target for the full Gate-B benchmark.

The 2026-08-29 successor keeps one model resident and serves two ordinary concurrent
transcriptions. On four repeated 59.448-second synthetic inputs, one worker reached
44.28 times real time, two reached 57.64 times, and four reached 59.83 times; two was
retained because four bought only another 3.8% while widening the resource/failure
group. These numbers measure execution efficiency, not transcript accuracy. The
next gate is the recording-disjoint paired bake-off in
[`GPU_THROUGHPUT_PRODUCTION_PLAN.md`](GPU_THROUGHPUT_PRODUCTION_PLAN.md).

The current production-candidate control is faster-whisper `small.en`, FP16,
beam/best-of 5, with two worker iterators sharing one resident model. It is a safe
measured starting point, not the intended permanent winner. The minimized execution
image retains the cuBLAS libraries actually needed by this path and no longer ships
cuDNN or NVRTC. Candidate bake-offs may replace the model, runtime, compute type,
decoder, or scheduling policy after a paired accuracy/efficiency evaluation; each
candidate gets new per-run integrity metadata rather than inheriting the control's
hashes.

## Evaluation by capability

### Audio normalization and VAD

FFmpeg remains the deterministic boundary: retain the original stream timing,
derive 16 kHz mono PCM for speech models, record the exact command and FFmpeg build,
and validate duration/sample count. Downmixing can erase channel separation, so keep
the original channels addressable and test separate-channel diarization when a
source provides meaningfully different channels.

[Silero VAD](https://github.com/snakers4/silero-vad) is the CPU default. Its project
is MIT-licensed, supplies ONNX artifacts, supports 8/16 kHz audio, recommends one
Torch thread in its example, and describes a roughly 2 MB JIT model. Its published
speed claim is below 1 ms for a 30+ ms chunk on one CPU thread, making it suitable
for an always-on routing pass. Pin the ONNX file hash, not merely `silero-vad`, and
calibrate onset, offset, padding, and minimum-duration rules on HIMR audio.

A first dependency-light feasibility lane is now implemented with whisper.cpp v1.8.7
and the exact Silero v6.2.0 GGML weights. On one sealed 1,799.979-second window it ran
in 6.006 seconds under the frozen v0.3 adapter on one CPU thread and retained 486
uncalibrated speech candidates. The verified engine/model bytes execute from
write-sealed anonymous Linux descriptors; output pipes and result publication are
bounded, the output component chain is retained, and publication is descriptor-relative.
This establishes throughput and coordinate handling, not accuracy or calibration.
Its contract pins the reviewed defaults because the upstream v1.8.7 example CLI has a
minimum-silence option-assignment defect. The planned ONNX comparison and frozen
HIMR-audio calibration set remain required before selecting a corpus-wide profile.

Do not let VAD delete media coordinates. Store speech intervals over the normalized
audio timeline and map them back to the source. Preserve low-score boundary context
for reviewers and ASR padding. Music, playback, breath noise, distant speech, and
short interjections must appear in the VAD evaluation set.

### Multilingual and context-aware ASR

[faster-whisper](https://github.com/SYSTRAN/faster-whisper) is the primary runtime.
It is MIT-licensed, uses CTranslate2, supports CPU INT8, batched inference, word
timestamps, and Silero filtering. Its official 13-minute benchmark reports the
`small` model at 1m42s and 1,477 MB with CPU INT8, and 51s/3,608 MB at batch eight,
on an 8-thread i7-12700K. Those numbers justify a local bake-off but are not a
capacity promise for the Ryzen host.

[whisper.cpp](https://github.com/ggml-org/whisper.cpp) is the contingency and
comparison runtime. It is MIT-licensed, supports AVX CPU inference, quantization,
VAD, and a dependency-light C/C++ deployment. Its published memory table is useful
for constrained operation, and it avoids the Python-wheel problem. Keep it in the
pilot because it may win throughput or operational simplicity even if
faster-whisper remains the richer orchestration API.

The [OpenAI Whisper repository](https://github.com/openai/whisper) is the model and
reference implementation source; its code and weights are MIT-licensed. Do not use
the reference PyTorch runtime for bulk CPU work unless it unexpectedly wins the
local benchmark. Compare these model/runtime pairs on identical decoded audio:

- faster-whisper `small` INT8: throughput baseline;
- faster-whisper `medium` INT8: likely CPU quality candidate;
- faster-whisper `turbo` INT8: speed/quality candidate with a short decoder;
- whisper.cpp quantized `small`, `medium`, and `large-v3-turbo`: operational
  comparison, using the same decoding policy where possible; and
- on the local GPU, keep `small.en` FP16/beam 5 as the control and screen
  [Distil-Large-v3.5](https://huggingface.co/distil-whisper/distil-large-v3.5),
  [Whisper Large-v3 Turbo](https://huggingface.co/openai/whisper-large-v3-turbo),
  and Large-v3 `int8_float16`;
- trial [Parakeet-TDT-0.6B-v2](https://huggingface.co/nvidia/parakeet-tdt-0.6b-v2)
  behind a separate NeMo adapter as the non-Whisper English challenger.

`turbo` is multilingual ASR but is not intended for speech translation. Preserve
the original-language transcript; translation is a separate revision and task.
English-only Distil-Whisper models are not the corpus default because the source
universe includes multilingual and code-switched speech.

Context must improve spelling without rewriting history:

1. Produce a context-free baseline with its complete decoder diagnostics.
2. Detect candidate uncertain spans using disagreement, word/segment scores,
   glossary matches, OCR, and review feedback. A raw Whisper word score is not a
   calibrated probability.
3. Re-run only those intervals with a small, versioned list of relevant names and
   terms. faster-whisper explicitly supports `hotwords`/hint phrases and an
   `initial_prompt`; it also exposes average log probability, no-speech score,
   compression ratio, and word score in its
   [transcription API](https://github.com/SYSTRAN/faster-whisper/blob/master/faster_whisper/transcribe.py).
4. Store the baseline and contextual output as separate immutable transcript
   revisions with glossary revision, prompt, window, and parameters.
5. Prefer the contextual token only when the evaluation policy accepts it or a
   person reviews it. Never prompt with a whole wiki page, allegations, or a list of
   expected events; that invites confirmation-shaped hallucination.

Specify language when source metadata or a reviewed sample establishes it. For
unknown/code-switched media, retain per-window language evidence and evaluate
language changes rather than forcing one language across a long stream. Repetition,
timestamp drift, music hallucinations, and invented glossary terms get explicit
error counters.

### Word alignment

[WhisperX](https://github.com/m-bain/whisperX) is the forced-alignment layer, not the
source of transcript truth. Its code is BSD-2-Clause and it aligns Whisper text with
language-specific wav2vec2/CTC models. The current code has built-in choices for
English, French, German, Spanish, and Italian through torchaudio and maps many other
languages, including Japanese and Korean, to third-party Hugging Face models in
[`alignment.py`](https://github.com/m-bain/whisperX/blob/main/whisperx/alignment.py).

Each aligner is a separate model dependency with its own model card, training-data
terms, vocabulary, revision, and accuracy. “WhisperX supports the language” is not a
license or quality finding. Pilot English first, then Japanese, Korean, and other
observed languages individually. If a language or token cannot be aligned, retain
the enclosing segment time and mark the word time unavailable; do not interpolate a
precise-looking timestamp across an unsupported token.

Run alignment after text selection, retain the original ASR timing, and validate
monotonicity, bounds, coverage, and human-marked word boundaries. GPU is preferred
for the full backfill, but a single English aligner can be tested locally after the
ASR model has exited.

### Diarization, overlap, and transcript speaker assignment

[pyannote.audio](https://github.com/pyannote/pyannote-audio) code is MIT-licensed.
The recommended
[`speaker-diarization-community-1`](https://huggingface.co/pyannote/speaker-diarization-community-1)
pipeline is CC-BY-4.0, requires accepting its access conditions and using a Hugging
Face token for download, and can run offline after acquisition. It emits an
overlap-aware diarization and an “exclusive” version designed to simplify transcript
reconciliation.

Keep both representations. Exclusive assignment is useful for attaching one speaker
label to a word, but it must not erase the fact that two people spoke at once.
Diarization labels such as `SPEAKER_02` are recording-local and are not identities.
The Hugging Face token is a secret and never belongs in a run manifest, log, static
release, or model cache archive.

Use the sampled solo fast path before full diarization. Route a recording to
Community-1 when VAD/ASR/visual sampling suggests another voice, overlap, remote
audio, reaction playback, or uncertain speaker count. Run it on the GPU for long
media. CPU is acceptable for short pilot clips, one process at a time, only if its
measured real-time factor is operationally useful.

[NVIDIA Streaming Sortformer 4spk-v2](https://huggingface.co/nvidia/diar_streaming_sortformer_4spk-v2)
is the GPU/long-form comparison. Unlike the older non-commercial offline v1 weight,
v2 is CC-BY-4.0 and has both a NeMo checkpoint and a Q8 GGUF for NVIDIA's new
[Apache-2.0 NeMo-Speech.cpp runtime](https://github.com/NVIDIA/NeMo-Speech.cpp).
It emits four simultaneous 80 ms speaker-activity streams, but its card explicitly
warns that performance degrades above four speakers, on very long recordings, on
non-English speech, and out of domain. Use the fixed high-latency streaming profile
in ADR 0007 and never run it on an interval whose reviewed lower speaker bound is
above four.

Community-1's final clustered turns do not have a documented calibrated confidence;
store no turn probability. Sortformer's sigmoid activity matrix is a raw model output
and remains uncalibrated on HIMR until a recording-disjoint calibration passes. Both
systems emit recording/run-local anonymous labels only.

### Active-speaker detection

Active-speaker detection links audible speech to a visible face track. It does not
identify the person, and an off-screen voice or video playing inside the recording
must be a valid “no visible speaker” result.

Use lightweight [YuNet](https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_detection_yunet)
face detection, a deterministic shot-local motion/IoU tracker, and
[LR-ASD](https://github.com/Junhua-Liao/LR-ASD) as the primary audiovisual model.
The YuNet directory is MIT-licensed. LR-ASD's repository and included weights are
MIT-licensed; its authors report 94.45% AVA validation mAP and 86.1 average Columbia
F1 with AVA weights, increasing to 96.4 F1 with TalkSet-finetuned weights. The large
change across weights is a warning about domain transfer, not a HIMR accuracy claim.

[Light-ASD](https://github.com/Junhua-Liao/Light-ASD) is the close model fallback.
[TalkNet](https://github.com/TaoRuijie/TalkNet-ASD) is reference parity only: its
official environment starts at Python 3.7.9, requirements are unbounded, and its
weights are fetched outside the Git repository. LR-ASD's inference path returns a
positive-class logit and the demo thresholds it at zero; it is not a probability.
Any adapter must reproduce the authors' reference logits before corpus use.

Run ASD only on VAD-positive intervals that contain a usable track. Store the face
track, audio interval, raw ASD score, calibration revision, visible/off-screen
decision, and diarization-turn link separately. Require high precision and allow
abstention; a wrong visible-speaker link is worse than an unknown speaker. Score each
track independently: off-screen speech may yield zero active faces and visible overlap
may yield more than one. Never force an argmax.

### Cross-video face and voice candidates

Face and voice embeddings are biometric identifiers or biometric templates in many
legal and policy contexts. They remain in the private evidence workspace, encrypted
at rest, access-controlled, deletion-aware, and absent from Git, public releases,
browser search indexes, logs, and analytics. Do not search the open web by face or
voice, infer sensitive traits, or enroll incidental bystanders. Obtain jurisdiction-
appropriate legal/privacy review before operating this stage at scale.

For faces, use YuNet detection and
[OpenCV SFace](https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_recognition_sface)
embeddings for the first license-safe bake-off. The SFace directory, including its
ONNX model, states Apache-2.0. Aggregate several high-quality frames into a
track-level representation; never let one blurry crop join identities. Compare
candidate pairs with source-family-aware held-out positives/negatives and use
complete-linkage/cannot-link constraints so one weak bridge cannot merge two
clusters.

A bounded 2026-08-28 feasibility run now exercises the pinned OpenCV 4.14.0.94,
YuNet, and SFace artifacts on four preselected source/Reddit frame pairs. All eight
frames had exactly one detection and produced four private raw routing values; the
run took 1.33 seconds and peaked at 172,936 KiB RSS with one OpenCV thread. Exact
face-bearing timestamps and values remain in the ignored owner-private packet. This
is a transcode/crop positive control because prior A/V work selected the corresponding
coordinates. It supplies no negatives, recording-disjoint calibration, tracker,
probability, threshold, identity decision, or biometric approval. Its isolated Python
3.14.7 runner is exploratory evidence only; production promotion still requires the
pinned Python 3.12 environment and the gates below.

A separate 2026-08-28 synthetic-only bridge now emits the anonymous dense-frame
contract expected by the shot-local tracker without invoking SFace or retaining crops
or embeddings. Four procedural frames across two explicit shots produced zero YuNet
detections and flowed into four tracker frame rows with zero tracks. This verifies the
offline runtime and negative-path contract only. It does not license sparse DN0 frames
or unreviewed scene candidates as tracker input, measure detector quality, or advance
Gate C/Gate 3.

[InsightFace](https://github.com/deepinsight/insightface) may be evaluated privately
only after license approval. Its library code is MIT, but its official
[licensing notice](https://github.com/deepinsight/insightface/blob/master/server/LICENSING.md)
says the public model packages are for non-commercial academic research unless a
separate license is obtained. Do not substitute an InsightFace `buffalo` or
`antelope` weight because it scores better without recording that restriction.

For voices, start with the
[SpeechBrain ECAPA-TDNN VoxCeleb model](https://huggingface.co/speechbrain/spkrec-ecapa-voxceleb),
whose model card states Apache-2.0. [WeSpeaker](https://github.com/wenet-e2e/wespeaker)
is the Apache-2.0 comparison toolkit, but its documentation correctly notes that
pretrained-model terms follow their datasets. Pin and review the exact model, not
just the toolkit license.

Extract voice candidates only from at least three seconds of clean, non-overlapped,
non-playback speech after diarization. Exclude TTS, altered-speed audio, AI-generated
material, background media, and phone audio that fails the quality gate. Keep face
and voice clusters separate; agreement can prioritize review but cannot automatically
name or fuse a person. Public names require a reviewer-approved public anchor.

### Shots, text detection, and OCR

[PySceneDetect](https://github.com/Breakthrough/PySceneDetect) 0.7.1 is the CPU shot
boundary default. It is BSD-3-Clause; `ContentDetector` handles fast cuts and the
two-pass `AdaptiveDetector` is intended to be more robust to fast camera motion.
Pin 0.7.1 and select/configure the detector against HIMR cuts, fades, handheld
movement, screen recordings, and reaction inserts.

Run OCR on a small set of frames per shot plus frames triggered by text-region
change. Preserve bounding polygons, frame timestamp, script/language choice, raw
text, model score, and temporal track. Collapse repeated captions/chat messages into
one observation with a visible interval, but retain all contributing frame locators.

[PaddleOCR](https://github.com/PaddlePaddle/PaddleOCR) is Apache-2.0. The July 2026
project describes PP-OCRv6 tiny/small/medium models and a unified 50-language model,
including Chinese, English, and Japanese, with an OpenVINO CPU path. Pilot v6 tiny
and small for common scene text. Use the 2M-parameter PP-OCRv5 multilingual
recognizers as pinned fallbacks for Korean or scripts not covered by the selected v6
model; the v5 documentation covers 109 languages. Do not use a 0.9B OCR-VL model for
the sparse CPU pass.

[Tesseract](https://github.com/tesseract-ocr/tesseract) 5.5.2 is the Apache-2.0 CPU
fallback and evaluation baseline. It supports UTF-8 and more than 100 languages, but
its layout assumptions and scene-text performance differ from PaddleOCR. It is
useful for clean overlays, debugging, and disagreement routing rather than as the
sole OCR engine.

OCR can reveal addresses, account identifiers, private messages, and unrelated
people. Screen detected text for the publication exclusions in
`CORPUS_RIGHTS_AND_TAKEDOWN.md` before it can enter a public release.

### Sound events and visual actions

[YAMNet](https://github.com/tensorflow/models/tree/master/research/audioset/yamnet)
is the first sound-event candidate. TensorFlow Model Garden is Apache-2.0; YAMNet is
a 3.7M-weight MobileNet model producing 521 AudioSet class scores over overlapping
0.96-second frames. Its own README also says the current implementation relies on
Keras 2 and is incompatible with Keras 3, so it belongs in a small isolated
environment, not the core ASR environment.

Do not ingest all 521 labels. Pre-register a small observable vocabulary such as
music, laughter, applause, cat sounds, door/knock, vehicle, and silence/noise, then
evaluate each class. Labels suggesting emotion, diagnosis, intent, intoxication,
consent, relationship, age, ethnicity, or gender are prohibited. Sound predictions
are candidate observations; transcript stage directions require media review.

[MMAction2](https://github.com/open-mmlab/mmaction2) is an Apache-2.0 toolbox with
action recognition, temporal localization, and spatiotemporal detection models. Its
model zoo is broad but largely trained on generic action taxonomies, and model/checkpoint
and dataset terms require individual review. A VideoMAE configuration can require
hundreds of GFLOPs per clip, so generic corpus-wide action inference is not justified.

After ASR/OCR/diarization is stable, test one lightweight temporal model and one
strong remote-GPU model only on priority intervals. Define a narrow, literal action
vocabulary with enough labeled examples. Prefer an unknown result over creative
captioning. General video-language-model summaries, inferred motives, and automatic
event narratives are out of scope.

### Fingerprints and clip-to-parent matching

No single fingerprint answers every deduplication question. Use a cascade:

1. SHA-256 identifies byte-exact media objects.
2. [Chromaprint](https://github.com/acoustid/chromaprint) identifies near-identical
   full audio and renditions cheaply. Its own README says it is optimized for
   near-identical audio, duplicate files, and long-stream monitoring, not general
   audio fingerprinting. Current Chromaprint code is MIT, but audit the exact library
   linked into the local FFmpeg build.
3. [audfprint](https://github.com/dpwe/audfprint) is an MIT-licensed landmark matcher
   designed to find noisy query excerpts and report offsets/time ranges. It is the
   primary audio route for a short Reddit clip into a longer parent recording.
4. Meta ThreatExchange's
   [vPDQ](https://github.com/facebook/ThreatExchange/tree/main/vpdq) hashes sampled
   video frames with timestamps and matches shared similar frames. The repository is
   BSD-licensed with documented file exceptions. Use it to recover visual-only,
   muted, overlaid, or differently encoded clips that audio misses.

Record query interval, parent interval, offset, transformation class, raw match
counts/distances, algorithm revision, and reviewed relation. Test cropping,
letterboxing, overlays, subtitle burn-in, re-encoding, gain/noise, muted video,
leading/trailing edits, and modest speed changes. A high match proposes a rendition,
excerpt, or mirror relation; it does not prove provenance, uploader intent, or which
copy is original.

### Confidence calibration

Never combine raw scores from ASR, VAD, alignment, diarization, ASD, face, voice,
OCR, action, or fingerprints into one “confidence.” Each task needs its own target,
quality strata, calibration set, calibration revision, and abstention threshold.

Use a held-out calibration split disjoint from model selection and final test.
[scikit-learn's calibration guide](https://scikit-learn.org/stable/modules/calibration.html)
recommends fitting calibration on data independent from model training and warns
that isotonic regression overfits small datasets; it notes isotonic generally needs
more than roughly 1,000 samples. Therefore:

- use sigmoid/Platt calibration for binary scores while data is limited;
- use temperature scaling for genuine multiclass logits;
- consider isotonic only after a task/stratum has at least 1,000 representative
  calibration examples and wins held-out reliability tests; and
- if there are fewer than 200 calibration observations or fewer than 50 positives
  and 50 negatives for a decision, publish no probability—retain a raw score and an
  explicitly uncalibrated band.

Report reliability diagrams, adaptive-bin expected calibration error (ECE), maximum
calibration error, Brier score with its limitations, discrimination metrics, and
risk/coverage curves with bootstrap confidence intervals. Calibration cannot repair
a model used outside its domain. A human review state remains categorical provenance,
not `1.0` probability.

## License and access gate

Code, weights, training data, runtime binaries, and hosted inference can have
different terms. Before any model enters a run, archive the exact model card and
license text and complete this table for the pinned artifact:

| Component                           | Code license                           | Weight/model access known at evaluation                                      | Decision                                                             |
| ----------------------------------- | -------------------------------------- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| OpenAI Whisper / faster-whisper     | MIT / MIT                              | Whisper weights MIT                                                          | Approved for benchmark                                               |
| whisper.cpp                         | MIT                                    | Converted Whisper weights retain Whisper terms                               | Approved for benchmark                                               |
| Silero VAD                          | MIT                                    | Project provides weights without gating                                      | Approved for benchmark                                               |
| WhisperX                            | BSD-2-Clause                           | Every language aligner has separate terms                                    | Approve per aligner only                                             |
| pyannote.audio / Community-1        | MIT / CC-BY-4.0                        | Gated acceptance and HF token required                                       | Approved after maintainer accepts terms; attribute and cache offline |
| NeMo / Streaming Sortformer 4spk-v2 | Apache-2.0 / CC-BY-4.0                 | Four-speaker NeMo and GGUF artifacts; no gated token at review               | Approved comparison after exact offline pin and attribution          |
| NeMo / offline Sortformer 4spk-v1   | Apache-2.0 / CC-BY-NC-4.0              | Four-speaker, short-window checkpoint                                        | Rejected for production; no reason to prefer it over v2              |
| Streaming Sortformer 4spk-v2.1      | Apache-2.0 / NVIDIA Open Model License | Custom license; four-speaker checkpoint                                      | Deferred pending written acceptance and measured benefit over v2     |
| LR-ASD                              | MIT                                    | Weights included in repository                                               | GPU benchmark approved after file hash pin                           |
| Light-ASD                           | MIT                                    | Weights included in repository                                               | Model fallback after file hash pin                                   |
| TalkNet                             | MIT code                               | Weights downloaded separately; legacy environment                            | Reference parity only after independent weight admission             |
| YuNet                               | MIT directory license                  | ONNX file included                                                           | Approved for private detection/tracking                              |
| ByteTrack association               | MIT                                    | No detector/model weight selected                                            | Optional tracking comparison only                                    |
| SFace                               | Apache-2.0 directory license           | ONNX file included                                                           | Approved for private benchmark                                       |
| InsightFace                         | MIT code                               | Public pretrained packages restricted to non-commercial research             | License-blocked as default                                           |
| SpeechBrain ECAPA model             | Apache-2.0 model card                  | Public Hugging Face artifact                                                 | Approved for private benchmark                                       |
| WeSpeaker                           | Apache-2.0 code                        | Model terms follow training dataset                                          | Approve exact model only                                             |
| PySceneDetect                       | BSD-3-Clause                           | No model weights                                                             | Approved                                                             |
| PaddleOCR                           | Apache-2.0                             | Pin exact PP-OCR model and card                                              | Approved for benchmark                                               |
| Tesseract                           | Apache-2.0                             | Language data must also be pinned/audited                                    | Approved baseline                                                    |
| YAMNet                              | Apache-2.0 repository                  | Audit exact HDF5 artifact and AudioSet-related notices before redistribution | Private experiment only                                              |
| MMAction2                           | Apache-2.0 toolbox                     | Checkpoint/dataset terms vary                                                | Deferred, approve per checkpoint                                     |
| Chromaprint                         | MIT current code                       | Local FFmpeg linkage/build terms vary                                        | Approved for private processing; audit binary redistribution         |
| audfprint                           | MIT                                    | No gated weights                                                             | Approved                                                             |
| ThreatExchange PDQ/vPDQ             | BSD repository with listed exceptions  | No model account required                                                    | Approved after exact-file audit                                      |

FFmpeg itself is LGPL/GPL depending on build options and linked libraries. Running the
installed binary internally is different from redistributing it in a container or
release; audit the complete build before distributing a runtime image.

## Biometric and privacy gate

The following conditions are mandatory before any cross-video face or voice job:

- document the lawful/ethical purpose, scope, operator, retention period, deletion
  path, and applicable jurisdiction;
- process only material already admitted under the corpus rights policy;
- exclude incidental people and minors unless a specific reviewed necessity exists;
- keep crops, waveforms, embeddings, pair scores, and clusters in private encrypted
  storage with access logs;
- do not send media or embeddings to a hosted API without explicit approval of the
  provider, region, retention, training-use policy, and contractual terms;
- never expose nearest-neighbor or biometric search to the public;
- allow a person to remain `unknown`; optimize against false merges, not for maximum
  naming coverage; and
- propagate corrections/deletions through enrollment items, derived tracks,
  embeddings, clusters, identity assertions, search caches, and backups.

Synthetic footage, face swaps, TTS, dubbed audio, reaction playback, and altered
speed are separate media types and must not enter identity enrollment. An account
name, diarization label, face cluster, voice cluster, and person are distinct objects.

## Run integrity and replaceable model candidates

Never run `main`, `master`, `latest`, or an unqualified Hugging Face model ID in a
published processing run. This rule makes a completed run auditable; it does not
freeze the model catalogue. A run manifest must record:

- repository URL, release/tag and immutable commit SHA;
- package version plus lockfile and downloaded-wheel hashes;
- model repository and immutable revision/commit;
- SHA-256 and byte size of every weight, tokenizer, vocabulary, language data, and
  configuration file;
- archived model card/license text hash and the approval decision;
- Python, OS/container digest, CPU instruction set, thread limits, and for GPU jobs
  the GPU model, driver, CUDA, cuDNN, PyTorch/CTranslate2/ONNX Runtime versions;
- exact FFmpeg build and normalization command;
- every inference parameter, random seed, VAD padding, decoding option, prompt,
  glossary revision, calibration revision, and threshold;
- input artifact hashes, output artifact hashes, code commit, run ID, start/end
  timestamps, peak RSS/VRAM, wall time, and exit state; and
- whether remote egress occurred. Access tokens and signed URLs are explicitly
  excluded.

Mirror approved model artifacts into a private content-addressed cache after license
acceptance, then run offline. CPU and GPU locks are separate because CUDA/CTranslate2
compatibility is strict. The current minimal faster-whisper production candidate uses
CUDA-driver host libraries plus image-contained cuBLAS and does not include cuDNN or
NVRTC. Another runtime may have a different closed dependency set. A model/runtime
upgrade creates a new processing revision, records its own closure, and runs the same
frozen benchmark before backfill; improving technology is expected.

ADR 0007 records the exact reviewed pyannote, Community-1, Sortformer, NeMo,
NeMo-Speech.cpp, LR-ASD, Light-ASD, OpenCV, YuNet, ByteTrack, CPU-base, and GPU-base
revisions/digests. Its Git blob identifiers for the small ASD weights are acquisition
locators, not substitutes for the SHA-256 that model admission must compute. The
pinned pyannote loader resolves classes named by model YAML, so Community-1 config is
allowlisted and hashed as executable supply-chain input before it is loaded. Runtime
containers set `PYANNOTE_METRICS_ENABLED=0`, receive no download credential, and have
networking disabled.

## Pilot dataset and benchmark protocol

### Frozen material

Use the roadmap's 12-recording, six-hour vertical slice. Group exact duplicates,
reposts, excerpts, and mirrors into recording families before splitting so the same
speech excerpt, frame, or face crop cannot leak across selection, calibration, and
test. Recurring people may appear in more than one split; record that subject overlap
rather than mislabeling this recording-family-disjoint benchmark as subject-disjoint.

Create three immutable partitions at the recording-family level:

- **development:** tune preprocessing and debug integrations;
- **calibration:** fit thresholds/probability mappings only; and
- **test:** open once for the go/no-go report, then retain unchanged for future model
  comparisons.

At least one hour must be fully human annotated for the transcript pilot, growing
toward the 24-hour evaluation set described in `CORPUS_REVIEW.md`. Include clean/noisy
monologues, outdoor/car audio, long streams, remote/phone audio, music and playback,
overlap, at least two non-English/code-switched samples, reaction inserts, short
reposts, text-heavy screens, synthetic material, and known off-screen speech.

Gold annotations need orthographic and verbatim transcript policies, speech/VAD
boundaries, word-boundary samples, language spans, speaker/overlap turns, visible
active face versus off-screen speech, same/different face and voice pairs, shots,
legible OCR text and intervals, approved sound/action labels, and clip-parent offsets.
Two reviewers adjudicate identity pairs and any sensitive output. Reviewer names and
decisions are retained; the media itself is not redistributed as an evaluation set
without rights review.

For the speaker/face/ASD sub-benchmark, ADR 0007 freezes eight recording families and
80 minutes as 30-minute development, 20-minute calibration, and 30-minute sealed test
partitions. It adds face/track annotation, one-second track-time ASD scoring, and
explicit no-visible-speaker/visible-overlap conditions. This smaller overlay does not
replace the transcript freeze or permit a recording family to cross its split.

### Resource measurements

Every candidate runs cold and warm on the same media and deterministic audio. Record
real-time factor (processing seconds / media seconds), peak RSS/VRAM, CPU/GPU
utilization, model-load time, output/cache bytes per media hour, failures, and human
review minutes. On CPU, test 8 versus 16 threads and only the batch sizes that remain
under the memory gate. Do not run competing benchmarks concurrently.

CPU Bronze ASR must meet both gates:

- real-time factor no worse than 0.35 on the six-hour mix; and
- peak RSS no greater than 5.5 GiB under the audited host state.

At 0.35, a single uninterrupted lane still needs roughly 34 days for 2,335 media
hours before retries and alignment, so a slower model cannot be the sole backfill
path. The remote high-quality ASR target is real-time factor at or below 0.08 and
peak VRAM below 20 GiB on the selected 24 GiB runner. These are capacity gates, not
accuracy substitutes.

### Initial quality gates

The numbers below are pre-registered pilot acceptance thresholds. If the gold data
shows a threshold is ill-posed, revise it before looking at the final test and record
the change; do not move a gate after seeing a disappointing test result.

| Task                         | Held-out gate                                                                                                                                                                                                                                                                             |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| VAD                          | At least 98% speech-time recall; median onset/offset absolute error at most 200 ms; false-speech time at most 15%. Report short speech and music/playback separately.                                                                                                                     |
| ASR, clean English monologue | Normalized WER at most 12%.                                                                                                                                                                                                                                                               |
| ASR, all English strata      | Micro WER at most 22%, with every stratum reported; no stratum may be hidden by the aggregate.                                                                                                                                                                                            |
| Multilingual/code-switch ASR | Language-appropriate normalized WER or CER at most 30% on each language with enough test material; otherwise label the result exploratory.                                                                                                                                                |
| HIMRverse terms              | Entity-token recall at least 90%. A contextual pass must reach 92% or improve by at least 10 relative percent while increasing non-entity WER by no more than 0.5 percentage points and inserting at most one false glossary term per media hour.                                         |
| Hallucination                | At most one nonempty ASR segment per hour of annotated nonspeech and zero unbounded repetition loops.                                                                                                                                                                                     |
| Alignment                    | At least 98% of supported words monotonic and in bounds; median boundary error at most 200 ms and 95th percentile at most 800 ms. Unsupported tokens remain unaligned.                                                                                                                    |
| Diarization                  | DER at most 20% with no forgiveness collar and overlap scored; no required stratum above 35%; overlap recall at least 70%; exact speaker-count accuracy at least 80%. Report JER and playback/off-screen strata. This qualifies anonymous turns, never names.                             |
| Face tracking                | At least 95% recall for adjudicated faces at least 64 px wide, 90% usable-crop coverage, and fewer than one identity switch per ten track-minutes. Track IDs remain shot/run local.                                                                                                       |
| Active speaker               | At least 95% precision at the chosen operating point; false visible-face assignment at most 1% of evaluated speech time; `no_visible_speaker` recall at least 90%; at least 50% coverage of otherwise eligible track-time.                                                                |
| Face/voice candidate links   | Zero false merges in the pilot hard-negative set and a bootstrap 95% lower precision bound of at least 99% before any automatic cluster merge. If sample size cannot support that bound, remain candidate-only. Positive-pair recall target is 70% without relaxing the false-merge gate. |
| Shot detection               | At least 95% hard-cut recall and 90% precision; report fades and handheld false cuts separately.                                                                                                                                                                                          |
| OCR                          | Legible-text region recall at least 90%; CER at most 10% on clean overlays and 25% on challenging scene text; repeated temporal observations after dedupe at most 10%.                                                                                                                    |
| Audio/action classes         | At least 90% precision per class with at least 50 positive and 50 negative held-out examples. Otherwise keep the class experimental and out of transcripts.                                                                                                                               |
| Clip matching                | Zero false parent links across at least 1,000 hard-negative queries; at least 95% recall across registered benign transformations; median reported offset error below 1 second.                                                                                                           |
| Calibration                  | ECE at most 0.05 and bootstrap upper 95% bound at most 0.08, plus no worse Brier/log loss than the uncalibrated score. Publish risk/coverage. Insufficient sample sizes mean no probability.                                                                                              |

Accuracy comparisons also require uncertainty. Bootstrap by recording family, not by
individual word/frame, and report confidence intervals. Select a more expensive model
only when it passes resource gates and produces a meaningful held-out improvement,
for example at least 10% relative WER reduction or a material entity-recall gain
without higher hallucination risk.

Use the paired candidate-comparison contract in `evaluation/` for champion/challenger
decisions. It resamples the same recording family for both systems, which measures the
uncertainty of their difference directly. It also compares runner wall RTF, sampled
GPU-active RTF, energy per media hour, and peak process VRAM under an identical
measurement protocol. Those dimensions remain separate; a single blended
"accuracy-per-GPU-hour" number could conceal a transcript-quality regression behind
speed. Promotion is accuracy-first, then efficiency, and remains a human decision.
Condition rows for language/code-switch, overlap, playback, and noise must also be
covered; overall WER cannot hide a condition regression. Repetition candidates block
unattended promotion pending human review.

## Staged go/no-go plan

### Gate A — useful CPU corpus

Benchmark normalization, Silero, faster-whisper/whisper.cpp, PySceneDetect,
PP-OCR/Tesseract, SHA-256, Chromaprint, audfprint, and vPDQ. Exit when the selected
CPU ASR meets the quality/resource gates and every output carries enough run-integrity
metadata to explain how it was produced. This is the only work needed for the first
searchable transcript release.

### Gate B — timing and anonymous speakers

Provision the controlled GPU runner. Benchmark `turbo` versus `large-v3`, validate
one aligner per observed language, and compare pyannote Community-1 with Streaming
Sortformer v2 under ADR 0007. Exit when word timing and anonymous diarization pass
their accuracy, run-integrity, license, isolation, and resource gates on the frozen
test.

### Gate C — audiovisual and private identity candidates

Benchmark YuNet/shot-local tracking and LR-ASD under the private audiovisual gate.
Complete the separate biometric/privacy approval before SFace or ECAPA candidate
matching. Exit only if high-precision abstaining thresholds pass. Public labels still
require human identity assertions; failing this gate does not block transcripts.

### Gate D — bounded enrichment

Evaluate YAMNet and a narrowly selected action model on priority intervals. Adopt
only literal classes with adequate positives and high precision. This gate is
optional and must not delay catalogue, ASR, alignment, OCR, or review tooling.

## Explicitly deferred

- Corpus-wide dense OCR, face detection, active-speaker inference, or action
  recognition.
- Automatic open-set naming, face/voice search against the internet, public
  embeddings, and public nearest-neighbor endpoints.
- Automatic fusion of face, voice, account, or diarization clusters into a person.
- Emotion, mental-health, intoxication, sexuality, ethnicity, age, consent, motive,
  or relationship inference from audio/video.
- General-purpose video-language-model captions, event narratives, and LLM-written
  wiki edits from machine observations.
- Fine-tuning ASR, diarization, face, voice, ASD, OCR, or action models before the
  frozen evaluation set and error taxonomy exist.
- Offline Sortformer v1 and any non-commercial checkpoint; Streaming Sortformer v2
  remains only a four-speaker comparison until its HIMR gate passes.
- Streaming Sortformer v2.1 until its custom NVIDIA license is accepted and it
  materially beats the simpler CC-BY v2 artifact.
- InsightFace public pretrained weights as a default without a documented license
  decision.
- Isotonic calibration, automatic identity cluster merges, and displayed numeric
  confidence where the sample-size gates are unmet.

This ordering keeps the ambitious parts possible without allowing them to become the
bottleneck or to contaminate the publication-safe corpus with unreviewed biometrics,
overconfident scores, or inferred claims.
