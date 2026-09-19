# Speaker, face, and active-speaker routing foundation

[`speaker_activity_router.py`](speaker_activity_router.py) is an offline planning
stage. It does **not** diarize audio, detect faces, infer a person's identity, decide
who is speaking from model output, download a model, or invoke a configured engine.
It turns immutable input evidence into a deterministic, resource-checked set of work
intervals. A narrow `Daniel` field may forward a complete-recording human
source/speaker attestation; that is an attributed presumption, not router inference.

The normative contracts are:

- [`speaker-reviewed-hints.schema.json`](schemas/speaker-reviewed-hints.schema.json)
  for a separate, explicitly human-reviewed hint document;
- [`speaker-activity-routing-work-order.schema.json`](schemas/speaker-activity-routing-work-order.schema.json)
  for exact inputs, local capability pins, resources, and routing policy; and
- [`speaker-activity-routing-result.schema.json`](schemas/speaker-activity-routing-result.schema.json)
  for the route plan and deliberately unknown active-speaker placeholders.

## What is trusted

The router accepts only resolved local files without symlinks. Before planning, it
reads each file with before/file-descriptor/after stability checks and verifies its
SHA-256. It then reconciles all of the following:

1. the completed preprocessing envelope, preprocessing run ID, exact normalized FLAC
   hash, optional proxy-video hash, and recording duration;
2. the completed whisper.cpp envelope, its parent preprocessing run, exact audio hash,
   ASR window, transcript intervals, and explicit glossary hash (including explicit
   `null` when the ASR run used no glossary); and
3. a reviewed-hint document bound to the same recording ID and duration, reviewer,
   and review time; and
4. when supplied, a source-identity attestation bound to that same human reviewer and
   time, explicit source-confirmation evidence, and complete-recording speaker review.

A configured downstream capability is not considered pinned unless its engine file,
model manifest, model weights, and calibration artifact all have absolute local paths
and expected SHA-256 values. Every file is rehashed by the router. The result records
the observed byte counts and hashes and derives a capability recipe digest. An
unconfigured capability is valid, but every task requiring it is marked
`blocked_unconfigured`; this is how the current repository represents the honest fact
that no diarization, face, or active-speaker model has been selected.

The router itself is also hashed into every result. It has no network code and never
opens the corpus database.

## Routing model

ASR segments and reviewed-hint boundaries are split into non-empty integer-millisecond
`[start_ms,end_ms)` intervals. Reviewed hints may characterize speech presence,
multiplicity, audio origin, visible-face count, and the relation between a voice and
the picture. Hints must be sorted and non-overlapping. An ASR segment overlapping a
reviewed `speech_presence: absent` hint becomes an explicit evidence-conflict review;
neither source silently wins.

The planner emits these routes:

| Evidence | Route |
| --- | --- |
| Reviewed single, live voice without the narrow source attestation | `solo_fast_path`, anonymous label `unknown_single`; no public label |
| Fully eligible reviewed Daniel-owned solo source and high-confidence single/live interval | `solo_fast_path`; anonymous label remains `unknown_single`, with separate public label `Daniel` and explicit basis, independent of face visibility or on/off-screen relation |
| Overlap or unresolved count | overlap-aware diarization |
| Offscreen or mixed visual relation | offscreen-speech review |
| Unknown, playback, reaction insert, TTS, or synthetic origin | a distinct origin-review route |
| Visible or policy-routed unknown faces | face tracking |
| A non-trivial speech/face combination | active-speaker association |

Long inference tasks are deterministically partitioned into half-open chunks using the
work-order policy. Pinned tasks become `ready_pinned` only when CPU, memory, GPU, and
GPU-memory requirements fit the declared resource envelope. Otherwise their exact
deficits are recorded. Human review and the solo fast path do not pretend to be model
inference.

`max_parallel_tasks` is retained as an outer-scheduler limit. The router itself is
serial and does not dispatch work; a future scheduler must honor that limit while
preserving the task bytes and capability recipe hashes.

## Identity and overlap invariants

- `unknown_single` remains the default and the anonymous processing label. A single
  hint interval, ASR output, visible face, official-looking title, or source URL can
  never create a named label.
- The only named result admitted by this contract is `Daniel`, under the exact basis
  `confirmed_daniel_source_solo_presumption`. It requires one human-reviewed
  attestation that:

  - confirms the source as Daniel-owned and cites the reviewed provenance evidence;
  - covers the complete recording with contiguous hints;
  - attests exactly one live speaker across that recording;
  - records `no_contradiction_found` after title/context review; and
  - binds the same reviewer ID and zoned review time as the hint document.

  The individual named interval must additionally be high-confidence reviewed,
  conflict-free, present, single, and `live_voice`. Its result repeats the human
  reviewer, source-confirmation attestation ID, and explicit basis. Face visibility
  and on/off-screen relation neither strengthen nor block this source-wide
  presumption, and the router does not convert it into biometric evidence.
- A title/context contradiction, ambiguity, multiple reviewed live speakers,
  incomplete hint coverage, unknown speech presence, or inconsistent reviewer
  binding prevents the source-wide presumption. Contradictory and ambiguous
  title/context assessments may be retained in the attestation, but yield no
  `Daniel` label.
- Guests, overlap, playback, reaction inserts, TTS, synthetic voices, and unknown
  audio origin never receive a named label. Off-screen or mixed live speech may carry
  `Daniel` only through the qualifying source-wide solo presumption; it does not
  create a speaking-face claim or bypass its separate review route.
- ASR alone can never select the solo fast path. A human-reviewed single/live hint is
  required.
- Future diarization labels must be anonymous and scoped to one downstream processing
  run, for example `speaker_run_00`. The same spelling in another run has no
  relationship. Cross-recording voice or face identity is outside this stage and is
  forbidden as an inference from these results.
- Overlapping voices stay representable as one interval with
  `speech_multiplicity: overlap`; the router does not flatten them to one turn.
- Face visibility and speech are separate observations. Every face-visibility object
  has `speaking_face_claimed: false`. `none`, `unknown`, or `multiple_faces` does not
  defeat a qualifying source-wide solo presumption, and `single_face` is not evidence
  for it.
- Active-speaker placeholders begin in the `unknown` state with null face track,
  diarization label, raw confidence, calibrated confidence, and calibration hash.
  A future inference-result contract must retain the raw score and calibrated score
  separately, bind a non-null calibrated score to the exact calibration artifact
  SHA-256, and continue to permit an unknown association. A visible face is never
  sufficient evidence that it is speaking.
- Playback speech, a reaction-video insert, TTS, and broader synthetic media have
  separate enum values and separate review task types. None may be normalized into a
  live speaker observation.

Face/voice embeddings, when introduced, remain private. Identity results other than
the narrow human-attested solo-source presumption require a separate human-reviewed
catalog workflow; they must not be written into anonymous speaker labels or
active-speaker associations.

That catalog workflow now exists only for a narrower private direct-audio case under
[ADR 0012](../docs/adr/0012-private-solo-voice-attestations.md). It rechecks an exact
source/recording/rendition/media interval, requires a second independent privacy
reviewer, excludes playback/TTS/overlap and all source-, transcript-, model-, and
confidence-based identity shortcuts, and has no public export. Router output does not
automatically satisfy or populate it.

## Running a plan

Start from the tracked examples, replace every illustrative path and digest, and keep
the output on a durable private volume:

```sh
pipeline/bin/speaker-activity-router validate \
  --work-order /srv/himr-media/work-orders/speaker-routing-001.json

pipeline/bin/speaker-activity-router run \
  --work-order /srv/himr-media/work-orders/speaker-routing-001.json
```

`run` writes and seals the exact result bytes atomically and also prints them.
Repeating the same order is byte-identical. A writable existing result or an existing
output with different bytes fails closed. The result is a private work plan, not a
catalog import envelope and not public-release data. `public_speaker_label` describes
the label's intended downstream visibility; it does not itself authorize publication.

## Current limitations

- No diarization, active-speaker detection, voice matching, or speaker/ASD calibration
  inference is implemented. Separate private YuNet detection and shot-local face-
  tracking adapters now exist and have bounded runtime pilots, but this router neither
  executes nor consumes their results; a track remains anonymous and cannot establish
  who is speaking.
- The separate [`whispercpp_vad.py`](whispercpp_vad.py) lane now produces private
  uncalibrated speech-candidate intervals. Router contract v1 does not yet consume
  those results, and VAD supplies neither a speaker count nor an identity.
- Reviewed hints remain routing evidence, not biometric ground truth. The optional
  source identity object records a human presumption and provenance attestation.
- ASR speech presence is a routing cue and can conflict with a reviewed absence.
- The router clips bounded whisper.cpp segment overruns to recording duration for
  planning; the original ASR result remains the immutable source of the raw interval.
- The router does not infer cuts, playback, TTS, or synthetic media. Those categories
  exist only when a reviewer explicitly supplies them.
- `max_parallel_tasks` is not an invitation to run multiple memory-heavy models until
  pilot measurements establish safe concurrency.

The offline tests use non-executable fake bytes solely to exercise file hashing and
resource gating. They do not simulate model output and must never be described as
inference-quality evaluation.
