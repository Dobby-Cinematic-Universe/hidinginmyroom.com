# Provisional Daniel multi-era voice references

This separate pilot generated TitaNet embeddings from six archive recordings,
dated 2015, 2017, 2019, 2020, 2025 and 2026. Neither a title, a transcript lead,
a visible face nor acoustic similarity is treated as verified identity.
Production transcripts, diarization routing and Gemini inputs are unchanged.

## Results

18 five-second candidate clips were decoded; 15 passed the independent Silero
speech-fraction gate (at least 60% of samples in positive 32ms VAD frames).
Three clips remain retained but have no embedding. Model loading plus inference
took 12.96 seconds on one CPU thread, excluding preparation and environment setup.
No transcription API requests, Pinecone uploads or GPU work were performed.

| Date | Candidate recording | Usable excerpts |
| --- | --- | ---: |
| 2015-12-06 | How to make a jumping platformer game on xcode 7! - part 1 | 1/3 |
| 2017-10-25 | I feel lonely and desperate | 3/3 |
| 2019-07-01 | iPad pro hands on review | 3/3 |
| 2020-11-06 | Unboxing my NEW iPad Pro magic keyboard | 2/3 |
| 2025-05-25 | I'm Financially ruined... Life updates and my BIG plan | 3/3 |
| 2026-09-10 | life updates | 3/3 |

The 2019 and 2020 transcripts contain self-introduction leads naming Daniel;
these are unverified transcript evidence, not an authenticated enrollment.
Other candidates were selected by archive/title context. Dates are filename
metadata, not independently verified recording dates.

Three provisional era profiles cover 2015–2018, 2019–2022 and 2023–2026.
Individual vectors and per-recording profiles are retained; recordings receive
equal weight within each era rather than favoring recordings with more clips.
Every held-out comparison excludes the query's entire recording from its
reference set. No same-video clips leak into that test's reference side.

Median held-out prototype similarity, summarized by era pair:

| Comparison | Median cosine |
| --- | ---: |
| Early ↔ middle | 0.677 |
| Early ↔ recent | 0.654 |
| Middle ↔ recent | 0.767 |

These are **not match probabilities or accuracy measurements**. Identity labels
are not confirmed, and no confirmed negative speakers were included. Age,
microphones, rooms, background noise, speaking style and candidate-selection
errors are confounded. This pilot cannot attribute the differences to ageing.
The single surviving 2015 sample especially needs additional support.

## Frame review, without identity recognition

One frame was inspected per recording, at the first selected audio window:

- 2015: Xcode screen capture; no visible face. Narrator is off-camera.
- 2017: one foreground person outdoors; no second person apparent in this frame.
- 2019: tablet demonstration with hands; no visible face.
- 2020: keyboard-box demonstration with hands; no visible face.
- 2025: one foreground person with a microphone; a monitor is visible behind them.
- 2026: very dark close-up with one partly visible face; limited scene evidence.

These observations do not establish identity, who is speaking, absence of
off-camera speakers, or whether playback occurs elsewhere in the recording.
No facial identity matching or cross-frame face association was performed.

## Artifacts and next gate

Private workspace, relative to repository root:
`research/private-transcriptions/cloud-archive-20260913/daniel-era-reference-v1/`.

- `plan.json`: selected sources, dates, time intervals, transcript/model/frame
  hashes and extraction provenance. SHA-256:
  `f8cf3c046fb02d977412fa45207ed19e62fefdb95ac120f95aac9c928677b3fd`.
- `embeddings.json`: 15 normalized 192D vectors, VAD results and runtime versions.
  SHA-256: `91b4dbf1a59a59450aa62921c5e94679dfd33d8a927857610f1667608c1b3211`.
- `provisional-profile.json`: individual/era profiles and held-out comparisons.
  SHA-256: `d3d66c7e67485a28e420514607e19ba8be73c417028c8299c44bf50c879ac756`.
- `clip-000.wav` through `clip-017.wav`, and six scene frames, remain private
  for listening/scene review. Source media and transcripts are unchanged.

Status is `provisional_not_enrolled`; every identity decision remains unknown.
There is no automatic labeling, calibrated cutoff, verified positive set or
verified negative set. Next gates are confirming reference voices, obtaining
more early clean samples, and evaluating other voices/playback/TTS as negative
or unknown cases before choosing thresholds and ambiguity margins. Keep named
identity distinct from whether the speech is live, quoted, TTS or playback.

The preparer and profile builder are `pipeline/speaker_reference_eras.py`.
The pinned embedding executor is `pipeline/titanet_embedding_dense_pilot.py`.
The isolated CPU environment is `research/corpus/titanet-pilot-runtime-20260913`;
the original screening environment and both paid services remain unchanged.
The model is the official NVIDIA TitaNet-Large artifact at revision
`0dc382f40121a5fbd34db10a2bb04d826c2be6a8`, SHA-256
`e838520693f269e7984f55bc8eb3c2d60ccf246bf4b896d4be9bcabe3e4b0fe3`.
Inference denies new IPv4/IPv6 sockets in the kernel and uses no API keys.

13 focused tests pass, including temporal coverage, disjoint sampling, finite
vectors, whole-recording holdouts and unknown/disabled identity gates.
