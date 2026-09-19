# Full local corpus and summary preview

The operator requested that prepared material be visible in the local site, without
deploying it. Development mode reads the private pointer at
`research/corpus/site-previews/current.json`. Production mode never reads it.
The UI displays a private-preview banner and no-index metadata. Serve on loopback;
do not expose this development server to the public Internet.

## What is prepared

`pipeline/corpus_summary_preview.py` projects the retained cloud/third-party
inventory, preferred completed summaries, and manually reviewed transcript copies
into a private, hash-validated sharded corpus. It makes no paid calls, edits no
canonical transcripts, grants no publication decisions, and does not read media.

- Physical recordings receive stable IDs, not title-based identity guesses.
- Preferred completed summaries link to the corresponding transcript recording.
- Original text and segment timestamps are preserved. Invalid timing is withheld,
  not silently repaired or fabricated.
- Speaker-reviewed copies take precedence; speaker review does not imply human
  correction of the machine transcription.
- Saved speaker names, uncertainty and audio-source distinctions are retained.
- Third-party attribution and model provenance appear on local recording pages.
- Short transcripts can be read while their summaries remain withheld.
- Missing/withheld transcripts have metadata-only recording pages when a retained
  public source listing exists. No synthetic transcript or summary fills the gap.
- Historical local ASR is excluded.

The private `preparation.json` records all exclusions and open publication gates;
`identity-map.json` retains source-artifact bindings. These must not be published.
The prepared summary projection uses `publication: prepared`, which the production
summary validator rejects. No files are installed in `src/data` or `public`.

## Recreate or refresh

Run the preparer from the retained cloud runtime environment used by the campaign,
providing `--base`, `--selection`, `--broader`, and a fresh `--root` below
`research/corpus/site-previews/`. Each snapshot is immutable except the optional
broader-summary overlay and its derived search directory.

Validate and activate a completed candidate from the repository root:

```sh
node scripts/activate-corpus-preview.mjs release-20260916-v4
```

The dev server caches release data, so restart **only Astro** after switching:

```sh
npx astro dev stop
npm run dev
```

Build private full-text search without writing to the production output:

```sh
HIMR_CORPUS_DATA_ROOT=research/corpus/site-previews/release-20260916-v4/corpus \
HIMR_CORPUS_PREVIEW_OUTPUT=research/corpus/site-previews/release-20260916-v4/search \
node scripts/build-corpus-index.mjs
```

Use a fresh search output directory: the private index builder refuses to overwrite
an existing one. Local search indexes whole recordings to avoid millions of tiny
segment search records. It preserves every word, but a result opens the first
segment of the recording; browser Find locates the passage. Speaker filtering is
disabled for this recording-level index. Actual transcript timestamps remain intact.
The existing production segment-index behavior is unchanged.

When Claude completes more monthly/yearly/archive outputs, refresh only summaries:

```sh
node scripts/refresh-preview-summaries.mjs release-20260916-v4 \
  research/private-summaries/sonnet-broader-20260916/reader/index.json
```

Restart Astro to reload new routes. This does not copy/rebuild the corpus or search
index, re-summarize transcripts, or restart Claude. Broader summaries whose cited
transcripts are withheld stay held rather than receiving broken source links.

## Verification and remaining release work

Focused projection, release-contract, and watcher tests pass. Populated corpus,
recording, summary and search-index endpoints return HTTP 200. Chromium checks
confirmed full-text corpus search and filtered/paginated summary search. A separate
production build remained empty and passed search isolation with the local pointer
active, confirming private text is not included in production output.

Public deployment still requires publication review (rights, privacy, sensitivity,
source availability), approved public attribution metadata, resolving source holds,
and the existing repository-wide public-release gate failures. Local preview is
not a substitute for those checks or a claim that generated text is accurate.
