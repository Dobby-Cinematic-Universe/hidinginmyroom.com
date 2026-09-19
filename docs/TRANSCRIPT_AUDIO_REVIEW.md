# Separate audio-source review pilot

`pipeline.transcript_audio_review` is an offline review tool, not a new production
transcript source. Cloud/Gemini workers and their immutable releases are unchanged.
It does not retranscribe, purchase API work, scan the archive, or assign identities.

It checks exact AssemblyAI raw/normalized utterance alignment and label mapping,
then nominates low-confidence, very short, rare-label, donation/TTS-context and
nearby repeated-text segments. These are **review leads, not detected errors**.
Confidence is transcription confidence, not speaker confidence. Donation keywords
do not prove that the speaker is synthetic. Rare labels are not automatically Daniel.

Each recording gets at most 12 clips by default, each at most 12 seconds; up to 24
clips may be requested. Label representatives precede prioritized review leads.
Long utterances may only be partly sampled. RMS dBFS describes the clip level,
not noise level, SNR, intelligibility or hallucination probability. No quiet speech
is removed. Source size and pre/post stat witnesses are checked, but the full media
hash is deliberately not reread. Transcript/raw response bytes are hash checked.

Run with a new private output directory:

```sh
python3 -m pipeline.transcript_audio_review \
  --transcript /absolute/path/to/transcript.json \
  --output /absolute/path/to/new-review-directory
```

Open `review.html` locally to listen to the bounded evidence. `report.json` retains
all original text, segment times and labels, raw/source references and clip hashes.
`preview.json` initially includes no text in its summary preview because nothing
has been reviewed. It is never discovered by the existing Gemini submitter.

## Explicit review projection

To test reviewed decisions, supply a JSON object with `report` (exact path/SHA-256
reference) and `decisions` (list). Each decision requires `segment_index`, `reviewer`,
`evidence` (nonempty list of exact file path/SHA-256 references), `audio_source`
(`participant`, `playback`, `tts`, `unknown`), `intelligibility` (`clear`, `uncertain`,
`unintelligible`), and optionally `identity`. Identity is allowed only for a
reviewed participant. Use evidence covering the decision, not merely a short
prefix of a long segment. Mixed-source turns must remain unresolved; this pilot
does not split their text or infer word timing. Evidence hashes prove provenance,
not correctness of the judgment.

```sh
python3 -m pipeline.transcript_audio_review \
  --review-report /absolute/path/to/report.json \
  --decisions /absolute/path/to/decisions.json \
  --output /absolute/path/to/new-preview.json
```

The projection retains every segment and decision. Only explicitly identified,
clear participant speech appears in its timestamp-free summary preview. The same
reviewed identity can consolidate different original labels without destroying
them. Playback/TTS/uncertain spans remain in the evidence, not in that preview.
Even a fully reviewed projection stays `production_eligible: false`.

## Initial bounded pilot

Artifacts: `research/private-transcriptions/cloud-archive-20260913/audio-source-review-v1/`.

| Case | Segments | Provider labels | Flagged segments | Clips |
| --- | ---: | ---: | ---: | ---: |
| Gaming | 68 | 2 | 3 | 5 |
| Donation context | 177 | 2 | 16 | 12 |
| Late-night beer/chat | 550 | 4 | 48 | 12 |
| Girlfriend stream | 149 | 3 | 30 | 12 |

944 existing segments inspected, 41 local clips extracted, no paid requests.
This validates input compatibility and bounded extraction, **not classification
accuracy**. No clips have yet been human-adjudicated. There is no trained playback,
TTS or identity classifier here; automated acoustic merging and frame-based
attribution remain future work requiring labeled examples and a measured pilot.
Both production services remained active after extraction.

Validation: `python3 -m unittest pipeline.tests.test_transcript_audio_review -v`.
