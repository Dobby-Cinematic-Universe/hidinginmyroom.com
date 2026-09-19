# Automatically populated local Entities & events

The operator requested a mostly raw/unreviewed corpus. In the active local preview,
`loadCorpusGraph()` now builds a separate in-memory derived layer from the active
corpus and transcript summaries. Original transcripts, speaker decisions, summary
artifacts, and the reviewed public graph are not edited. No paid API is called.

## Extraction

- Existing non-placeholder speaker labels seed person-name candidates. Daniel,
  common places and platforms also seed literal mention matching.
- Repeated capitalized name-like phrases and contextual name candidates in at least
  two recording summaries extend the dictionary. These remain **name-like phrases**,
  not asserted people or verified identities. This lightweight heuristic is incomplete
  and may still have false positives. Grammar lead-ins and date words are excluded.
- Case-sensitive whole-name matching indexes explicit mentions in raw transcripts
  and summaries. Matching is not coreference resolution: no pronoun, relationship,
  family-role, or similar-name identity is guessed. Aliases are not automatically merged.
- Speaker-label references are recorded separately from mentions, once per recording.
  Neither establishes an on-camera appearance or live participation rather than playback.
- Transcript-summary `events` items provide event descriptions. Broader summaries
  are not re-extracted, avoiding another hierarchy of duplicate event claims.
- Only identical normalized descriptions with the same source classification are
  grouped. Links to all source transcripts and source summaries are retained.
  Grouping does not assert that every recording describes the same real occurrence.
- A separate [local embedding layer](LOCAL_EVENT_GROUPING.md) now groups similar
  descriptions into related-account pages without replacing those original entries.
  Multi-recording groups appear first; unmatched descriptions remain searchable.
- Explicit date strings are displayed as **dates mentioned**, not assigned event dates.
  Recording dates remain clearly labeled source metadata. Relative dates, conflicting
  accounts, and variant spellings are not silently reconciled.

## UI and refresh

The directory, entity pages, and event pages show one shared machine-extracted,
unverified-content notice rather than claiming human review. Human corrections are
optional. Entity pages separate mentions, speaker labels, and associated descriptions;
event pages link to transcripts without requiring timestamps. Directories support text
filtering and incremental display (40 entries at a time).

The graph is generated on its first request and cached for that Astro process. A new
active corpus/summary snapshot is picked up on the normal site restart; no separate
graph-export or manual approval step is needed. A first request reads the active text
shards but never hashes, decodes, screens, or retranscribes archive media.

The automatic layer is currently **local-preview only**. Production continues to use
the existing reviewed graph and release boundaries; this change does not deploy any
content or assert publication rights. Local entity/event IDs are deterministic and
can survive rebuilds when their name/description is unchanged.

Tests: `node --test scripts/tests/derived-corpus-graph.test.mjs` and `npx astro check`.
