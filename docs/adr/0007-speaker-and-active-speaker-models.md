# ADR 0007: Pilot anonymous diarization and active-speaker association offline

- **Status:** Accepted for a private benchmark; production backfill remains blocked
- **Date:** 2026-08-26

## Context

The corpus needs anonymous “who spoke when” turns and, where a face is visible, a
candidate link between a voice interval and an on-screen face track. Neither task
identifies a person. The source mix includes long monologues, conversations, remote
audio, playback and reaction inserts, overlapping speech, small faces, cuts, noisy
mobile footage, and multilingual or code-switched material.

The routing foundation in
[`pipeline/SPEAKER_ACTIVITY_ROUTING.md`](../../pipeline/SPEAKER_ACTIVITY_ROUTING.md)
already fails closed when a capability is not pinned. This decision selects artifacts
for the first private benchmark; it does not install them, download weights, approve
biometric identity inference, or create a publication decision.

## Decision

1. Use
   [pyannote `speaker-diarization-community-1`](https://huggingface.co/pyannote/speaker-diarization-community-1)
   as the batch diarization baseline. Retain its ordinary overlap-aware output and
   its separate exclusive output. Run the same artifact on GPU when available and on
   CPU only as a measured short-job fallback.
2. Compare
   [NVIDIA Streaming Sortformer 4-speaker v2](https://huggingface.co/nvidia/diar_streaming_sortformer_4spk-v2)
   on long-form and overlap strata. It is the model fallback only if it passes the
   same held-out gate. The older offline v1 checkpoint is rejected because it is
   CC-BY-NC-4.0, limited to four speakers, and documented as reaching only about 12
   minutes on a 48 GiB RTX A6000. The custom-licensed v2.1 checkpoint is not needed
   for the first comparison.
3. Use [LR-ASD](https://github.com/Junhua-Liao/LR-ASD) with its AVA weight as the
   active-speaker baseline. Use
   [Light-ASD](https://github.com/Junhua-Liao/Light-ASD) only as the model fallback.
   Keep [TalkNet](https://github.com/TaoRuijie/TalkNet-ASD) as a reference parity
   check, not a production candidate: its documented environment begins at Python
   3.7.9 and its pretrained weights are fetched outside the Git repository.
4. Detect faces with the FP32 2023 YuNet ONNX artifact under OpenCV 4.x. Track only
   inside one shot with a deterministic motion/IoU assignment implementation. Do not
   use face embeddings for tracking and never join tracks across a cut. Compare the
   association portion of ByteTrack only if the simple tracker fails its held-out
   fragmentation/identity-switch gate.
5. A runtime failure, an unavailable model, or a failed quality gate falls back to
   `unknown_single`, `unknown_speakers`, or human annotation. It never falls back to
   guessing one visible face, assigning the loudest voice, or deriving a name.
6. Keep `unknown_single` as the anonymous default. Permit the separate public label
   `Daniel` only as a forwarded human presumption under the exact basis
   `confirmed_daniel_source_solo_presumption`. The source attestation must confirm
   Daniel ownership with reviewed provenance evidence, cover the complete recording,
   attest exactly one live speaker, find no title/context contradiction, and bind the
   same human reviewer and review time as the interval hints. A named interval must
   also be high-confidence reviewed, conflict-free, present, single, and `live_voice`.
   Its face visibility and on/off-screen relation do not gate the source-wide
   presumption. Guests, multi-speaker recordings, overlap, playback, reaction inserts,
   TTS, synthetic voices, and unknown audio origin remain unnamed.

## Why these baselines

### Diarization candidates

| Candidate                      | Overlap and output shape                                                                                                                                                        | Runtime and scale                                                                                                                                                                                     | Language evidence                                                                                                                                         | License/public-site disposition                                                                                                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| pyannote Community-1           | Ordinary diarization preserves overlap; `exclusive_speaker_diarization` is a separate convenience view for transcript alignment. Speaker count can be unconstrained or bounded. | CPU by default and movable to CUDA. It consumes 16 kHz mono audio and supports a completely offline clone. Exact HIMR RTF/RSS/VRAM is not published and must be measured.                             | The official no-collar/overlap-scored table includes Chinese meeting sets and diverse diarization sets, but that is not a blanket multilingual guarantee. | `pyannote.audio` code is MIT; Community-1 is gated CC-BY-4.0. Commercial use is not barred by that license, but access conditions must be accepted and attribution/license records retained. No hosted inference. |
| Streaming Sortformer 4spk-v2   | Emits a `T × 4` activity matrix at 80 ms resolution, so simultaneous activity is representable. It does not provide pyannote’s exclusive transcript view.                       | Long-form streaming cache; maximum four output speakers. NVIDIA reports RTF 0.002 for the 30.4 s-latency profile on an RTX 6000 Ada, not on this runner. A Q8 GGUF exists for the new native runtime. | Its card says training was primarily English and warns of degradation on non-English/noisy material, despite including AISHELL-4 and AliMeeting.          | NeMo is Apache-2.0 and the v2 checkpoint is CC-BY-4.0. This is acceptable for a private benchmark with attribution. v1’s non-commercial checkpoint is excluded.                                                   |
| Streaming Sortformer 4spk-v2.1 | Same four-speaker activity shape; better self-reported meeting DER in several rows, mixed changes elsewhere.                                                                    | Same NeMo family; no need to add a second 471 MB checkpoint in pilot one.                                                                                                                             | Same explicit non-English caution.                                                                                                                        | NVIDIA Open Model License, not CC-BY. Defer until a written license acceptance and a measured v2 benefit justify the extra artifact.                                                                              |

Community-1’s published benchmark scores all overlap with no forgiveness collar; its
reported DER varies sharply by domain, from 8.9 on REPERE to 46.8 on Ego4D. Streaming
Sortformer’s own card likewise reports severe degradation above four speakers and on
some meeting data. These upstream results justify a comparison, not a HIMR accuracy
claim.

### Active-speaker candidates

| Candidate            | Published/repository evidence                                                                                                                                                                                    | Operational fit                                                                                                                                                                                                                                                                                                                                       | Disposition                                                                                             |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| LR-ASD AVA weight    | MIT repository includes code and weights; authors report 0.84M parameters, 0.51 GFLOPs, 94.45% AVA validation mAP, and stronger cross-dataset results than Light-ASD.                                            | Single-face-candidate input is compatible with independent per-track scoring. The reference path uses CUDA; no trustworthy end-to-end CPU RTF/RSS or GPU VRAM figure is published, so those are benchmark outputs rather than assumptions. It returns the positive-class **logit**, thresholds it at zero, and applies a five-frame mean in its demo. | Baseline after an adapter reproduces reference output and removes network/downloader/demo side effects. |
| Light-ASD AVA weight | MIT repository includes code and weights; authors report 94.06% AVA validation mAP and lower untuned Columbia F1 than LR-ASD.                                                                                    | Similar small CUDA-oriented integration; useful as a compatibility fallback. CPU throughput, host RSS, and GPU VRAM remain unverified until measured on the routed end-to-end stage.                                                                                                                                                                  | Benchmark only if LR-ASD fails compatibility or held-out quality.                                       |
| TalkNet              | MIT code; authors report 92.3 AVA validation mAP and warn that an AVA-trained model is difficult to apply out of domain.                                                                                         | Legacy Python 3.7.9/CUDA instructions, unbounded requirements, and externally downloaded weights make a clean pin harder.                                                                                                                                                                                                                             | Reference parity only; no production promotion in pilot one.                                            |
| C3ASD                | MIT 2026 research code reports corruption robustness, but its [repository at the reviewed revision](https://github.com/jisoo-o/C3ASD/tree/95fabed8d3481959bfe8a5286b5752d7cd887e3e) contains no released weight. | Re-training would change the task from model selection to model development.                                                                                                                                                                                                                                                                          | Excluded until an official immutable checkpoint and model card exist.                                   |

ASD scores each visible track independently. There may be zero active visible tracks
(off-screen/playback speech) or more than one (visible overlap). Selecting the maximum
score in every speech interval is prohibited.

These ASD architectures do not decode words, but that does not make their scores
language-neutral. Their published AVA/Columbia/WASD evidence does not establish
calibration on this corpus's non-English or code-switched material. Report that
stratum separately and retain abstention unless its held-out precision gate passes.

### Face detection and tracking

[YuNet’s official OpenCV Zoo directory](https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_detection_yunet)
is MIT-licensed, documents its approximate 10×10 to 300×300 trained face-size range,
and publishes WIDER Face results. The 2023 static-shape model is the OpenCV 4.x pilot;
the 2026 dynamic-shape export exists primarily for OpenCV 5’s ONNX Runtime engine.
Start with the official confidence/NMS/top-k values `0.9 / 0.3 / 5000`, then tune only
on development recordings.

The first tracker is deliberately not a recognition model. It performs Hungarian IoU
assignment with a constant-velocity prediction, starts/ends at shot boundaries, and
records every gap and assignment score. Its exact gap, IoU, and confirmation values
belong in the recipe selected on development data.
[ByteTrack](https://github.com/FoundationVision/ByteTrack/tree/d1bf0191adff59bc8fcfeaa0b33d3d1642552a99)
is MIT and associates low-score detections, but its published results are person MOT,
not HIMR face tracking. Its association algorithm is an optional comparison without
its YOLOX detector or person-model weights.

## Exact pilot artifact ledger

No artifact may be addressed by `main`, `latest`, a mutable tag, or model name alone.
The following are the upstream roots reviewed for this decision. Git blob IDs for the
small ASD weights are exact Git content identities, but not SHA-256; model registration
and execution remain blocked until an approved acquisition computes and records their
SHA-256 without altering the files.

| Purpose                     | Exact upstream artifact                                                                                               | Integrity pin                                                                                                                                        |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| CPU base                    | `python:3.12.11-slim-bookworm`, Linux amd64 platform manifest                                                         | `sha256:c00fc7b44d844b6da22861ec24af43968a5200eac4ec607b4725d585165d6b49`                                                                            |
| GPU base                    | `nvidia/cuda:12.6.3-cudnn-runtime-ubuntu24.04`, Linux amd64 platform manifest                                         | `sha256:23debbe74125dc84df96df79cff42079b3b15265c27140714fd27b5aa718faa4`                                                                            |
| pyannote runtime            | `pyannote.audio==4.0.7`, tag commit `b749285c5cdd4636b2edc7f766f1352c8dde9369`, universal wheel, 894,598 bytes        | `sha256:852ea15c4d85bc34773e618267603ffca6a521669a74d33742692cc67fc700d6`                                                                            |
| Community-1 repository      | Hugging Face revision `3533c8cf8e369892e6b79ff1bf80f7b0286a54ee`                                                      | Mirror every file and freeze the model card/config/license snapshot.                                                                                 |
| Community-1 segmentation    | `segmentation/pytorch_model.bin`, 5,906,507 bytes                                                                     | `sha256:7ad24338d844fb95985486eb1a464e32d229f6d7a03c9abe60f978bacf3f816e`                                                                            |
| Community-1 embedding       | `embedding/pytorch_model.bin`, 26,646,242 bytes                                                                       | `sha256:6f10ff60898a1d185fa22e1d11e0bfa8a92efec811f11bca48cb8cafebefd929`                                                                            |
| Community-1 PLDA            | `plda/plda.npz`, 133,852 bytes; `plda/xvec_transform.npz`, 134,376 bytes                                              | `sha256:9b77bcd840692710dd3496f62ecfeed8d8e5f002fd991b785079b244eab7d255`; `sha256:325f1ce8e48f7e55e9c8aa47e05d2766b7c48c4b25b8de8dd751e7a4cc5fbe8f` |
| Sortformer v2 repository    | Hugging Face revision `5240a64075176943f677d30fa2171c780229f341`                                                      | Freeze model card and all selected model/config files.                                                                                               |
| Sortformer v2 NeMo weight   | `diar_streaming_sortformer_4spk-v2.nemo`, 471,367,680 bytes                                                           | `sha256:b371afce2c4958186469df33d939936b9746c89f38b10a69cfd2c61254e83329`                                                                            |
| Sortformer v2 native weight | `diar_streaming_sortformer_4spk-v2.q8_0.gguf`, 147,075,776 bytes                                                      | `sha256:0679cfeb1ce356d0dea9470b31274f4bfc7eb927497d82005483770666da998a`                                                                            |
| NeMo comparison image       | `nvcr.io/nvidia/nemo:26.02`                                                                                           | manifest `sha256:5852a213751955315a5dd54ce50eff69ac87d474f33968135fc88f1cdbb1dd06`                                                                   |
| NeMo source reference       | NeMo `v2.7.3`                                                                                                         | commit `1d4ee423806d461f9146ae982f9da8eb32495ae7`                                                                                                    |
| Native Sortformer reference | NVIDIA NeMo-Speech.cpp                                                                                                | commit `4f9676226f667d14608487df744f375db87127f8`, including its recorded submodule SHAs and third-party notices                                     |
| LR-ASD                      | repository commit `1b6dcd2d8fc2895683de6508ec6294ec47d388ca`; `weight/pretrain_AVA.model`, 3,426,337 bytes            | Git blob `d724be582f6d34f1b099657235dedafa0668fd82`; SHA-256 required at admission                                                                   |
| Light-ASD fallback          | repository commit `ed38c232de5efe0261dbd68627c0ade7cdfe14eb`; `weight/pretrain_AVA_CVPR.model`, 4,175,289 bytes       | Git blob `dbc9703e3a5132a54278c784117076ca612e1e66`; SHA-256 required at admission                                                                   |
| TalkNet reference           | repository commit `6d6821479af485e251c4991487e40573b42181b4`                                                          | No model is approved until its externally fetched bytes are independently hashed and licensed.                                                       |
| OpenCV Python               | `opencv-python-headless==4.14.0.94`, manylinux 2.28 x86-64 wheel, 61,960,165 bytes                                    | `sha256:211e581f5a4670acbbe08fff36a35e9946039d2eea28b80394632d036d1be527`                                                                            |
| YuNet detector              | OpenCV Zoo commit `47534e27c9851bb1128ccc0102f1145e27f23f98`; `face_detection_yunet_2023mar.onnx`, 232,589 bytes      | `sha256:8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4`                                                                            |
| SFace embedder              | OpenCV Zoo commit `47534e27c9851bb1128ccc0102f1145e27f23f98`; `face_recognition_sface_2021dec.onnx`, 38,696,353 bytes | `sha256:0ba9fbfa01b5270c96627c4ef784da859931e02f04419c829e83484087c34e79`                                                                            |
| ByteTrack comparison        | source commit `d1bf0191adff59bc8fcfeaa0b33d3d1642552a99`                                                              | Source and every transitive build artifact must be hashed; no bundled detector weight is selected.                                                   |

The Python images must use Python 3.12, `torch==2.8.0`,
`torchaudio==2.8.0`, and `torchvision==0.23.0`; the GPU lock uses the official
PyTorch `cu126` wheel index. `torchcodec==0.7.0` is the initial compatibility pin,
but the runner should pass the already-normalized waveform in memory so model I/O
does not silently decode/resample the source a second time. Generate separate CPU
and CUDA `uv` lock/requirements files with hashes for every transitive wheel. The
artifact table does not substitute for those locks.

Builds and model acquisition occur in a controlled connected staging job only. The
runtime receives the existing `/input`, `/models`, and `/output` mounts, has networking
disabled, and sets `PYANNOTE_METRICS_ENABLED=0`. Hugging Face or NGC credentials are
download-time secrets and are never copied into the image, model manifest, run
result, log, or cache archive.

Before loading Community-1, an admission tool must hash and parse `config.yaml` and
allow only the reviewed pipeline/preprocessor classes. The pinned pyannote loader
[resolves class names from model configuration](https://github.com/pyannote/pyannote-audio/blob/b749285c5cdd4636b2edc7f766f1352c8dde9369/src/pyannote/audio/core/pipeline.py#L271-L311),
so the configuration is executable supply-chain input, not inert metadata.

No selected container, dependency wheel, checkpoint, face crop, track, raw score, or
calibration artifact is served by the public site. If a selected CC-BY model is used,
its title, creator, source revision, license link, and changes belong in the public
methods/third-party notice even though the weights remain private. Any public derived
observation still passes the corpus's independent rights, privacy, sensitivity, and
human-review gates; model licensing alone does not authorize publication. Commercial
deployment is eligible for the MIT, Apache-2.0, and CC-BY-4.0 candidates reviewed
here, subject to their notices/access conditions and a final artifact-specific license
snapshot. CC-BY-NC v1 remains excluded, and the NVIDIA-licensed v2.1 remains blocked.

## Fixed integration profiles

### Diarization profile

- Input: the preprocessing run’s verified 16 kHz mono s16 FLAC, loaded once and
  passed as a waveform/sample-rate pair. Preserve the original multichannel media
  reference so a future reviewed channel-aware run can be a separate revision.
- Community-1: do not provide `num_speakers`, `min_speakers`, or `max_speakers`
  unless a direct-media human review supplied the bound. Persist ordinary and
  exclusive turns as different named representations.
- Sortformer v2 comparison: batch size 1; `chunk_len=340`,
  `chunk_right_context=40`, `fifo_len=40`, `spkcache_update_period=300`, and
  `spkcache_len=188`. This is the card’s 30.4-second high-latency profile, appropriate
  for offline archives. Do not apply DIHARD/CALLHOME-tuned post-processing to HIMR.
- Split deterministic long jobs only at the routing contract’s boundaries with
  registered left/right context. A chunk boundary may not reset speaker state
  invisibly. If the implementation cannot carry state or reconcile anonymous labels
  without a reviewed overlap, do not stitch the chunks.
- Store recording-local labels, half-open integer-millisecond turns, overlap flags,
  model representation (`ordinary`, `exclusive`, or `activity_matrix`), parameters,
  and quality flags. Never reuse a local label across runs or recordings.

Community-1 does not expose a documented calibrated confidence for a final clustered
turn. Its turn rows therefore have `score_state=unavailable`, not `1.0`. Sortformer’s
sigmoid activity values are preserved as raw model outputs with
`calibration_state=uncalibrated`, despite the model card calling them probabilities.

### Face/ASD profile

- Route only speech-positive intervals with video, plus reviewed negative/off-screen
  samples required by evaluation. Decode the verified 640×360, 25 fps CFR proxy.
- Run YuNet FP32 on each routed frame with OpenCV CPU backend. Preserve detector box,
  five landmarks, raw detector score, frame timestamp, input dimensions, and whether
  the face is below 64 px wide.
- Split tracks at every registered shot boundary. A track ID is scoped to one run
  and shot. Store detections and gaps; never infer cross-shot continuity.
- Convert each track to 112×112 grayscale crops and 13-coefficient MFCCs with 16 kHz
  audio, 25 ms windows, and 10 ms steps, matching LR-ASD’s reference preprocessing.
  The compatibility test must match the authors’ reference logits on a fixed public
  fixture before using the adapter on corpus media.
- Preserve the positive-class logit before any threshold. Register the optional
  centered five-frame mean as a separate post-processing recipe. Score each face
  independently and allow `active`, `inactive`, and `abstain`; retain an explicit
  `no_visible_speaker` outcome.

Face boxes, landmarks, crops, tracks, ASD logits, and calibration artifacts remain
private. A face track is not a face identity. A track-to-turn association is not a
named speaker assertion.

The narrow Daniel solo-source rule is outside model scoring. It does not use a face
embedding, voiceprint, diarization cluster, ASR wording, or ASD score. The routing
result preserves `unknown_single` as its processing label and carries any `Daniel`
value in a separate field together with the human reviewer, review time,
source-confirmation attestation ID, and basis. Title/context contradiction or
ambiguity disables the public label even if the source itself is confirmed. Off-screen
speech and `none`, `unknown`, or `multiple_faces` observations do not disable an
otherwise qualifying interval, while every face observation still fixes
`speaking_face_claimed=false`. The route plan remains private and supplies no
publication authority.

## Recording-disjoint benchmark

Freeze eight recording families and 80 minutes selected from direct media without
looking at any candidate output. Exact duplicates, excerpts, reposts, reaction
inserts, and mirrors stay in one family. Use three development families (30 minutes),
two calibration families (20 minutes), and three sealed test families (30 minutes).
No clip, audio excerpt, duplicate, or parent/child recording may cross a split when
that relationship is known. Keep incidental participants in one split where feasible;
the recurring principal subject may necessarily appear in all three, and that leakage
must be reported. This is recording-family-disjoint evaluation, not a claim of
subject-disjoint generalization.

The frozen intervals must jointly include:

- a clean solo on-camera speaker;
- at least two genuine multi-speaker scenes and at least five minutes of overlap;
- off-screen speech, playback/reaction audio, silence, and visible lip motion from a
  non-speaker;
- multiple, small, partly occluded, and entering/leaving faces plus hard cuts;
- phone/remote/noisy audio and a long-form interval; and
- at least ten minutes of non-English or code-switched speech if the admitted pilot
  material contains it.

Two reviewers independently mark speech and anonymous speaker turns, overlap,
off-screen/playback state, face boxes/tracks, and per-track speaking state; a third
reviewer adjudicates. `uncertain` is a valid reference label and is excluded from
binary calibration rather than forced positive/negative.

Score diarization with DER using no collar and overlap included, JER, speaker-count
accuracy, overlap precision/recall/F1, and miss/false-alarm/confusion components.
Report all metrics by recording family and by solo/multi-speaker, overlap, playback,
noise, speaker-count, and language strata.

Score detection/tracking with face recall/precision at IoU 0.5, IDF1, track
fragmentation, identity switches per ten track-minutes, and usable-crop coverage.
Score ASD on track-time, not isolated correlated frames: aggregate into nonoverlapping
one-second bins, report average precision, precision/recall/F1 at the chosen operating
point, false visible-face assignment as speech time, `no_visible_speaker` recall, and
risk/coverage under abstention. Score overlapping visible speakers independently.
Bootstrap confidence intervals by recording family.

Fit a sigmoid/Platt mapping for LR-ASD logits and Sortformer speaker-activity outputs
only on the calibration families and separately for pre-registered quality strata.
Do not calibrate pyannote cluster turns that have no defined raw score. Fewer than 200
eligible one-second bins, 50 positives, or 50 negatives means no displayed probability.
Adjacent bins are not treated as independent when uncertainty is estimated: confidence
intervals are bootstrapped by recording family. Test data is opened once after image,
model, parameters, thresholds, and metric code are sealed.

## Stop/go gates

### Gate 0 — artifact, license, and isolation

Go only when every image, wheel, source commit, model/config/card/license file, and
calibration file has a registered SHA-256 and byte count; dependency locks install
offline from a private cache; required CC-BY attribution is drafted; the selected
use is compatible with all terms; the model-config class allowlist passes; telemetry
is disabled; a network-denied smoke test makes no connection attempt; and no secret
appears in image history or results. Any failure is a stop.

### Gate 1 — reference and reproducibility

Go only when the family-disjoint freeze and adjudication validate, two cold runs have
identical canonical interval decisions and artifact hashes (allowing separately
recorded raw floating-point tensors only within a predeclared tolerance), all times
are in bounds and monotonic, and the LR-ASD adapter matches the reference fixture.

### Gate 2 — diarization quality and resources

The selected anonymous diarizer must meet all of:

- overall DER at most 20% with no collar and overlap scored;
- no required stratum DER above 35%;
- overlap speech-time recall at least 70% and exact speaker-count accuracy at least
  80%;
- GPU RTF at most 0.15, peak VRAM below 20 GiB, and no failure on a continuous
  30-minute interval; and
- CPU fallback RTF at most 1.0 and peak RSS at most 5.5 GiB on routed short jobs.

If both models fail, keep anonymous speaker assignment human-only. Sortformer is not
eligible on an interval with a reviewed lower speaker bound above four.

### Gate 3 — face tracking and active speaker

Go only when YuNet/tracking reaches at least 95% face recall for reference faces at
least 64 px wide, at least 90% usable-crop coverage, and fewer than one identity
switch per ten track-minutes; and calibrated ASD reaches at least 95% precision, at
most 1% false visible-face assignment by evaluated speech time, at least 90%
`no_visible_speaker` recall, and at least 50% coverage of otherwise eligible
track-time. The routed end-to-end stage must stay at or below 0.5 RTF, 8 GiB VRAM,
and 5.5 GiB host RSS. Failure leaves associations unknown and does not block text
transcripts.

### Gate 4 — promotion and change control

Promotion creates versioned work-order/result contracts and a model-registry entry;
it does not begin corpus-wide backfill. Public release still requires direct-media
human review and the independent publication gates. Any model, weight, runtime,
container, preprocessing, tracker, threshold, smoothing, or calibration change
creates a new recipe and repeats the sealed test.

## Primary-source record

- [pyannote.audio 4.0.7 release](https://github.com/pyannote/pyannote-audio/releases/tag/4.0.7),
  [Community-1 model card](https://huggingface.co/pyannote/speaker-diarization-community-1),
  and [Community-1 pinned file tree](https://huggingface.co/pyannote/speaker-diarization-community-1/tree/3533c8cf8e369892e6b79ff1bf80f7b0286a54ee)
- [Streaming Sortformer v2 model card and metrics](https://huggingface.co/nvidia/diar_streaming_sortformer_4spk-v2),
  [pinned v2 file tree](https://huggingface.co/nvidia/diar_streaming_sortformer_4spk-v2/tree/5240a64075176943f677d30fa2171c780229f341),
  [offline v1 limitations/license](https://huggingface.co/nvidia/diar_sortformer_4spk-v1),
  and [NeMo diarization documentation](https://docs.nvidia.com/nemo-framework/user-guide/latest/nemotoolkit/asr/speaker_diarization/models.html)
- [NeMo-Speech.cpp source/license](https://github.com/NVIDIA/NeMo-Speech.cpp/tree/4f9676226f667d14608487df744f375db87127f8),
  [NVIDIA Open Model License](https://www.nvidia.com/en-us/agreements/enterprise-software/nvidia-open-model-license/),
  and [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/legalcode)
- [official Python container record](https://hub.docker.com/_/python),
  [official NVIDIA CUDA container record](https://hub.docker.com/r/nvidia/cuda),
  [NVIDIA NeMo container record](https://catalog.ngc.nvidia.com/orgs/nvidia/containers/nemo),
  and [pyannote.audio 4.0.7 package record](https://pypi.org/project/pyannote-audio/4.0.7/)
- [LR-ASD pinned source and weights](https://github.com/Junhua-Liao/LR-ASD/tree/1b6dcd2d8fc2895683de6508ec6294ec47d388ca),
  [LR-ASD paper](https://junhua-liao.github.io/Junhua-Liao/publications/papers/IJCV_2025.pdf),
  [Light-ASD pinned source and weights](https://github.com/Junhua-Liao/Light-ASD/tree/ed38c232de5efe0261dbd68627c0ade7cdfe14eb),
  [TalkNet pinned source](https://github.com/TaoRuijie/TalkNet-ASD/tree/6d6821479af485e251c4991487e40573b42181b4),
  and [C3ASD pinned source](https://github.com/jisoo-o/C3ASD/tree/95fabed8d3481959bfe8a5286b5752d7cd887e3e)
- [YuNet pinned model directory/license](https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_detection_yunet),
  [SFace pinned model directory/license](https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_recognition_sface),
  [OpenCV Python 4.14.0.94 package record](https://pypi.org/project/opencv-python-headless/4.14.0.94/),
  [ByteTrack pinned source](https://github.com/FoundationVision/ByteTrack/tree/d1bf0191adff59bc8fcfeaa0b33d3d1642552a99),
  and [official PyTorch 2.8 wheel combinations](https://pytorch.org/get-started/previous-versions/#v280)

## Implementation checklist

- [ ] Accept and snapshot Community-1 access/license conditions under the project
      owner’s account; acquire the exact revision into the private model cache.
- [ ] Generate hash-locked CPU and CUDA dependency locks and build images from the
      exact platform manifests above.
- [ ] Admit model/config/card/license artifacts through the model registry; validate
      Community-1’s config class allowlist before load.
- [ ] Add strict diarization work-order/result schemas preserving ordinary,
      exclusive, overlap, and raw-activity representations without identities.
- [ ] Add strict face-track and ASD schemas with shot-scoped tracks, raw logits,
      abstention, off-screen state, and calibration lineage.
- [x] Add strict anonymous frame-detection, shot-local tracking work-order, and
      tracking-result schemas. They preserve detector geometry, every gap and
      assignment IoU, track lifecycle, and null/false identity, active-speaker, and
      publication authority. ASD logits/calibration remain unimplemented.
- [ ] Implement the Community-1 adapter and a network-denied repeatability fixture.
- [ ] Implement the fixed Sortformer v2 profile; verify NeMo and optional GGUF parity
      before treating the native runtime as a fallback.
- [ ] Implement YuNet plus the shot-local tracker and reproduce detector fixtures.
- [x] Implement the bounded YuNet detector bridge that emits the existing anonymous
      frame-local contract. Revision `0.2.0` pins exact model/license/runtime/frame
      bytes, enforces complete explicit 25 fps shot lineage, uses one OpenCV CPU
      thread with no network, preserves zero/multiple detections, validates tracker
      consumption before sealing, and grants no identity, active-speaker, or
      publication authority. Its real-media profile accepts only one manually reviewed
      public-source shot under the overall 64-frame bound. This does not complete the
      held-out benchmark or Gate 3 work above.
- [x] Implement the deterministic geometry-only tracker foundation with explicit
      shot bounds, last-two-match constant-velocity prediction, dummy-augmented
      Hungarian IoU assignment, no cross-shot state, and adversarial synthetic
      replay. Implementation `0.2.0` hashes and parses one captured input buffer,
      enforces constant dimensions within each shot, evaluates eligibility on raw
      IoU, keeps extrapolated extents positive, records system Python as
      `host_runtime`, and seals staging before an atomic final rename. YuNet
      integration and the held-out tracking gate remain incomplete.
- [x] Implement a bounded explicit-frame YuNet/SFace private positive-control
      adapter with hash-pinned inputs/runtime/models, detection-cardinality
      abstention, null calibration and identity decisions, and owner-private
      biometric outputs. This does not satisfy the tracker, benchmark, or biometric
      approval gates.
- [ ] Implement the LR-ASD adapter and prove reference-logit parity; add Light-ASD
      only if needed.
- [ ] Freeze/adjudicate the eight-family benchmark, pre-register thresholds, and
      produce a held-out report with family-bootstrap confidence intervals.
- [ ] Register calibration artifacts only when sample-size and reliability gates pass.
- [ ] Keep every model output private until human review and the existing publication
      controls explicitly admit a derived observation.
- [x] Enforce the Daniel solo-source presumption as a human-attested, complete-source
      contract with multi-person, origin, visual-independence, contradiction, and
      reviewer-mismatch tests; do not treat it as a model identity result.
- [x] Add the separate ADR 0012 private catalog bridge for exact-interval named solo
      voices. It requires direct listening plus independent privacy review, rejects
      source/channel/transcript/model/confidence identity shortcuts, and grants no
      public export authority.

## Consequences

- Anonymous speaker and active-face candidates can advance without building a
  cross-video biometric identity system.
- Confirmed solo Daniel sources can carry a review-bound downstream public label
  without weakening the anonymous default or naming uncertain intervals.
- Community-1 is compact and operationally simple but gated; Sortformer provides a
  complementary long-form/overlap architecture at substantially higher model/runtime
  cost and with a hard four-speaker ceiling.
- LR-ASD is small, but its raw score and domain-transfer limits require explicit
  calibration and abstention.
- A four-pair 2026-08-28 positive control proved the bounded YuNet/SFace adapter can
  produce timestamped private review candidates across one confirmed main-channel
  source and one public Reddit clip. Its exact face-bearing timestamps and raw values
  remain in the ignored owner-private packet; they are not probabilities, thresholds,
  identity decisions, or independent recording-relationship findings.
- A separate synthetic 2026-08-28 fixture proves implementation `0.2.0` of the
  shot-local association adapter replays deterministically, records gaps and
  assignment IoUs, and forces a new track at every cut. The sealed `0.1.0` result is
  retained only as quarantined/superseded audit evidence after independent review
  found six integrity, geometry, threshold, schema, and visibility defects. The
  fixture recipe values are synthetic parameters only; neither result establishes
  YuNet recall, real-video tracking quality, identity continuity, or Gate 3 passage.
- A second separate synthetic 2026-08-28 packet proves the pinned YuNet runtime can
  emit the strict frame-local artifact and hand it unchanged to the fixed tracker.
  Four procedural frames across two shots produced zero detections and therefore zero
  tracks. This is negative-path/runtime/contract evidence only; a fake-runtime test
  separately exercises zero/multiple detections and positive shot-local geometry.
  Neither fixture measures detector recall or advances the real-media/Gate 3 gate.
- A third ignored owner-private packet passes one complete two-second, 50-frame shot
  from an admitted public main-channel source through YuNet `0.2.0` and the unchanged
  geometry tracker. The detector emitted 20 anonymous detections across 20 frames and
  retained 30 zero-detection rows; development recipe values routed those detections
  into four shot-local tracks. This is real-byte runtime/contract continuity only,
  not detector recall, tracking quality, identity continuity, active-speaker evidence,
  publication authority, or Gate 3 passage.
- A failed vision or speaker gate does not delay transcript search: the safe fallback
  is unknown/human review.
- The project has exact upstream roots to acquire later without silently following
  mutable repositories, while acquisition and installation remain separate approved
  operations.
