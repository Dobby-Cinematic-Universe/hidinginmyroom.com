# User-reviewed audio references

## Current revision: September 14 follow-up

Use `character-voice-pilot-v1/user-labels-v2.json` and
`reviewed-voice-gallery-v2/` for the current offline pilot. The v1 results below
are historical, superseded diagnostics, not current Mila accuracy.

The user reports M1 correctly contains Mila, M4 contains Daniel and Mila, and
M20 is mostly music. Retain the original probe 1 reference. Conservatively
exclude probes 4 and 20 from enrollment; the context review does not establish
exact speaker boundaries within the original or adjacent clips. Original
annotations are preserved, and v2 records the exclusions and review provenance.
No adjacent excerpts inherit an identity.

The rebuilt gallery has one Mila reference from one video, seven Daniel
references from three videos, and six Sunny references from two videos.
Daniel remains 7/7 and Sunny 6/6 in recording-held-out diagnostics. Mila has
zero eligible cross-video trials, **not** a measured 0% success rate. A clean
user-confirmed Mila excerpt from another video is needed to evaluate matching.
Production labeling remains disabled; paid pipelines are unchanged.

## Historical v1 review

The user's probe labels are preserved separately from the original anonymous
pilot and its review page. Probe IDs are the zero-based IDs displayed on that
page, not list positions or model-assigned speaker labels. Every annotation
binds the exact clip path and SHA-256 and the original embedding-result artifact.

## Enrollment and exclusions

| User label | Probes | Enrollment |
| --- | --- | --- |
| Mila | 1, 4, 20 | 3 single-speaker references, 2 videos |
| Daniel | 2, 28, 30, 33, 39, 46, 47 | 7 single-speaker references, 3 videos |
| Sunny | 24, 25, 26, 29, 42, 44 | 6 single-speaker references, 2 videos |
| Mila and Daniel | 5, 7, 9, 10 | Mixed; excluded |
| Sunny and Daniel | 27, 31, 32, 36, 45 | Mixed; excluded |
| Text to speech | 35 | TTS; excluded |

Probe 32 retains the specific note that Sunny's contribution is laughter only.
It is not treated as a Sunny-only speech example or split into invented turns.
All nine mixed clips and the TTS clip remain challenge examples. The 22
unreviewed/speech-gate-rejected probes remain unreviewed; no labels are inferred
for them. No existing unverified Daniel-era reference is promoted automatically.

Identity authority is the user's exact audio-probe annotations, not an inference
from a face or title. No face identity matching was performed. Embeddings were
reused: no retranscription, new inference, API spending or production changes.

## Cross-video diagnostic

For each single-speaker probe, all samples from the query's entire recording
are excluded. Each remaining speaker is scored by its maximum reference cosine.
The diagnostic reports the nearest label; it is **not** an accepted identity
decision and there is no calibrated acceptance threshold.

| Speaker | Nearest label correct on held-out probes |
| --- | ---: |
| Daniel | 7/7 |
| Sunny | 6/6 |
| Mila | 1/3 |

This small reviewed set is not a production accuracy estimate. Mila is currently
weak across recordings: probe 1 ranks Sunny (0.315) above Mila (0.262); probe 4
ranks Daniel (0.316) above Mila (0.031). Probe 20 ranks Mila first, but only at
0.262 versus Sunny at 0.239. These low/close scores warrant abstention, not
changing the user's labels or asserting that the user misidentified a voice.
Cleaner and more varied Mila references are needed before automatic matching.

Mixed/TTS examples are always withheld regardless of their nearest score. The
diagnostic excludes their entire video too; for example, mixed probe 32 ranks
Daniel highest (0.555), demonstrating why a whole-clip embedding must not be
treated as a complete description of everyone in it. Probe 35's nearest score
is Daniel at 0.256, but the authoritative TTS label keeps it out of enrollment.

## Artifacts

Relative to `research/private-transcriptions/cloud-archive-20260913`:

- `character-voice-pilot-v1/user-labels-v1.json`: all 26 exact user annotations.
- `reviewed-voice-gallery-v1/gallery.json`: 16 reviewed reference vectors,
  three named profiles, and ten excluded challenge examples.
- `reviewed-voice-gallery-v1/diagnostics.json`: whole-recording-held-out rankings
  and the individual reference probe IDs used in each test.

Profiles retain individual samples and give recordings equal weight when forming
their aggregate vector. Raw transcripts, anonymous provider labels, original
review HTML and earlier provisional profiles remain unchanged. Both paid
services were checked and remain active.

Implementation: `pipeline/reviewed_voice_gallery.py`. Four focused tests cover
mixed/TTS/laughter exclusion, complete-video holdouts, exact clip binding,
duplicate rejection and invalid label combinations. Automatic production
labeling remains disabled until there is a larger independent evaluation set
with adequate coverage of noise, playback, TTS, overlap and voice variation.
