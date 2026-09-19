# Title-led voice candidates: Mila and Sunny pilot

This is a separate local voice-embedding and scene-review workflow. It does not
identify people from faces or name every non-Daniel voice after a video title.
The user-requested ordering is retained: audio comparison with provisional Daniel
references first, then scene-frame extraction from high/low similarity examples.
Every named identity remains unknown; no production transcript is changed.

## Pilot results

| Title lead | Video | Probes | Usable vectors | Strong Daniel-reference hints |
| --- | --- | ---: | ---: | ---: |
| Mila | Live Q&A with Mila | 12 | 7 | 2 |
| Mila | Christmas with Daniel and Mila 2020 | 12 | 1 | 0 |
| Sunny | stream with sunny | 12 | 11 | 3 |
| Sunny | SUNNY IS HERE! | 12 | 7 | 2 |

48 five-second excerpts were sampled across the four recordings; 26 passed the
speech-fraction gate and produced 192D TitaNet embeddings. Inference including
model load took 17.22 seconds on one CPU thread; extraction is additional.
Seven met the provisional reference-similarity hint, not a verified Daniel label.
The remaining nineteen are not automatically other speakers or title characters.

The strongest cross-video pair among the lower-Daniel-similarity Sunny-title
clips has cosine 0.709; the corresponding Mila-title maximum is only 0.262.
These are useful review leads, not calibrated probabilities or established
identities. Christmas supplied only one usable probe and needs better examples.

Complete-link grouping at an exploratory 0.65 threshold produced seventeen
anonymous acoustic groups; these are NOT a speaker count. A clip can contain
more than one voice, and microphones or noise can fragment one person's voice.
Groups retain original clip indices and title leads, never named identities.

## Scene review after audio comparison

Seven frames were inspected (the Christmas recording had only one usable clip):

- Q&A: two visible people in both selected frames, with one partially cropped
  in the high-similarity frame; stream/donation overlays are present.
- Christmas: the selected frame shows food/cooking, with no visible person.
- Sunny stream: one visible person in the high-similarity frame; two in the
  low-similarity frame, near a microphone.
- SUNNY IS HERE: two visible people in both sampled frames, under dim colored
  lighting with stream overlays.

These observations neither identify a person nor establish active-speaker
alignment. No face embeddings, facial identity matching or mouth-sync inference
were used. Playback/TTS and off-camera voices remain possible.

## Safety and reproducibility

Three recordings use existing exact imported third-party subtitle timing. The
Christmas import is ambiguous in the production plan, so this pilot uses only
the separately bound local media and uniform audio probes; it does not admit
the ambiguous transcript, change its production status, or purchase a replacement.
Titles about someone being absent/discussed were not chosen as presence evidence.
No historical local ASR was used.

The extractor prefers nearby subtitle-dense five-second windows, otherwise uses
uniform bounded audio probes. Subtitles are hints, not speech detection. All
clips subsequently pass through independent Silero VAD. Native inference uses
the isolated CPU environment, pinned NVIDIA TitaNet model and kernel-denied
IPv4/IPv6 sockets from the earlier pilot. No API keys or paid requests are needed.
Only bounded portions of media are read; full-file media hashes are not reread.
Transcripts, profiles, model files, clips and derived plans have retained hashes.

Audio comparisons require exactly the same model artifact as the provisional
[Daniel multi-era reference](DANIEL_MULTI_ERA_REFERENCES.md). The illustrative
strong hint requires maximum era similarity >=0.70 and median >=0.55, but even
that outcome stays `unknown_unverified_references`. No false-accept rate has
been measured and no automatic-labeling threshold is approved.

## Files

Private workspace:
`research/private-transcriptions/cloud-archive-20260913/character-voice-pilot-v1/`.

- `plan.json`: explicit title leads, media bindings and 48 selected intervals.
- `embeddings.json`: 26 vectors and 22 speech-gate rejections.
- `voice-review.json`: era comparisons and anonymous groups.
- `scene-frames/frames.json`: post-comparison frame provenance.
- `review.html`: local listening worksheet with scores, clips and selected frames.

Implementation: `pipeline/character_voice_pilot.py`; renderer:
`pipeline/render_character_voice_review.py`. The selection accepts explicit
`--selection JOB_ID:TITLE_NAME_LEAD` entries, at most four recordings per pilot;
it does not automatically discover or process the full archive.

19 focused tests passed across sampling, vectors, whole-video holdouts, anonymous
grouping and identity gates. Both paid production services remained active.
Next steps are listening review and cleaner confirmed references, especially
for Mila, followed by held-out positive/negative testing before named enrollment.
