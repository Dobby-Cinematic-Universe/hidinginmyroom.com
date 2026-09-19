# Text-only conversation review leads

`pipeline.transcript_conversation_leads` is a separate offline helper. It reads
third-party SRT/text transcripts, not historical local ASR, media, model weights,
or API keys. It does not submit transcription requests or alter pipeline routing.

The helper looks for differing explicit speaker labels, nearby invitation/reply
pairs, two-sided call checks, reciprocal greetings, and compact reciprocal
question/answer clusters. It discounts explicit chat reading, reported or
hypothetical speech, quotations, lyrics, and language examples. A subtitle cue
boundary is never treated as proof of a speaker turn.

Each lead retains its exact source path and SHA-256, zero-based cue indexes,
millisecond timestamps, bounded context, and the reason for its priority tier.
Optional canonical inventory matching uses the existing exact-identity matcher;
ambiguous/shared identifiers remain review holds. It does not hash or open media.

Run into a fresh private output directory whose parent already exists:

```sh
python3 -B -m pipeline.transcript_conversation_leads \
  --transcript-root /absolute/path/research/HIMR-Transcripts \
  --inventory /absolute/path/inventory.json \
  --inventory-sha256 EXACT_INVENTORY_SHA256 \
  --output-root /absolute/path/new-private-leads
```

Outputs are immutable `report.json` and a readable `TOP-20.md`. Tests:

```sh
python3 -B -m unittest pipeline.tests.test_transcript_conversation_leads -q
```

These are review leads, not detected speakers, calibrated probabilities, or
permission to retranscribe an existing usable transcript. One person can read
both sides of a conversation. Unmarked chat, performance, or embedded recordings
can remain false positives. No lead also does not prove a single speaker: short
replies or voices omitted by the transcript may be missed. Review promising
timestamped intervals with audio before converting a text lead into a positive
multi-speaker finding.
