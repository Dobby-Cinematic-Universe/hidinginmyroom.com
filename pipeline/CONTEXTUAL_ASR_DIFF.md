# Text-private contextual ASR comparison

`contextual_asr_diff.py` compares one completed raw whisper.cpp result with one
completed glossary-assisted result over deterministic fixed time blocks. It is a
private change detector, not an accuracy evaluator and not a correction tool.

The comparator revalidates both immutable result envelopes and their normalized/raw
artifacts, rehashes the retained audio, engine, model, and glossary, and requires the
two processing recipes to differ only in glossary provenance. It then assigns lexical
tokens to half-open media-time blocks by token midpoint. Tokens without usable timing
use their parent segment midpoint and are counted explicitly.

The output contains no transcript wording. For changed blocks it retains only:

- character and lexical-token counts;
- character edit distance (explicitly not WER);
- raw decoder-score summaries, still uncalibrated;
- empty/nonempty transitions and repeated-word-run lengths;
- first/last timed-token drift; and
- literal glossary-term counts and deltas.

It never selects a preferred revision, merges text, labels either pass accurate or
improved, claims human review, or grants publication authority. A contextual-only
glossary term is a review candidate, not proof that the contextual pass corrected an
error; a baseline-only term is likewise not proof that the raw pass was correct.

Run it after a paired contextual result exists:

```sh
pipeline/bin/contextual-asr-diff \
  --baseline-result /srv/himr/asr/raw/result.json \
  --contextual-result /srv/himr/asr/contextual/result.json \
  --output /srv/himr/review/contextual-diffs/pair-001.json
```

The default block is 30 seconds. `--block-ms` accepts 5,000 through 120,000 ms. An
existing output is reused only when its bytes match exactly.

The comparator is useful before reference transcription because it exposes where the
prompt changed machine output without pretending that change is improvement. WER,
name error, false glossary insertion, or pass acceptance still require the frozen
human reference and adjudication workflow.
