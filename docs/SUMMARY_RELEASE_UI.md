# Summary release UI

The static summary library lives at `/corpus/summaries/`. It is separate from
reviewed wiki search and uses the corpus layout's search-isolation marker.
No credentials, API calls, live pipeline reads, or media assets are needed.

The first implementation provides:

- Archive overview, yearly, monthly, and recording summary pages.
- Search across released titles and preview text, with level/year filters and
  24-result pages. Query filters are shareable in the URL.
- Static browse pages (48 entries each) that work without JavaScript.
- Period drill-down and parent navigation, including a separate undated group.
- Overview, themes, developments, and uncertainty sections.
- Transcript-level sources in expandable lists for broader summaries; a single
  recording-transcript link for transcript summaries. No timestamp citations.
- A shared attribution/accuracy warning, explicit allegation/uncertainty labels,
  and escaped plain-text model output (not model-generated HTML or Markdown).

## Publication boundary

The build reads only `src/data/summaries/release.json`, not private reader exports.
The production release is intentionally empty. An explicitly activated,
development-only [local candidate](LOCAL_CORPUS_SUMMARY_PREVIEW.md) can display
prepared material without changing that release. Private generation completion is not
publication approval. Preparing this UI does not deploy the site or publish drafts.

Each approved entry must use this exact contract:

```json
{
  "id": "year-2020",
  "kind": "yearly",
  "period": "2020",
  "title": "2020 in review",
  "recording_id": null,
  "publication": "approved",
  "sections": {
    "summary": [{
      "text": "A supported paraphrase.",
      "classification": "reported_statement",
      "source_recording_ids": ["rec_REPLACE_WITH_PUBLIC_RECORDING_ID"]
    }],
    "topics": [],
    "events": [],
    "uncertainties": []
  }
}
```

The release wrapper contains `schema_version: 1`, a `summaries_`-prefixed
`release_id`, an explicit UTC `generated_at` timestamp, and `summaries`.
Supported kinds: `transcript`, `monthly`, `yearly`, `archive`. Use `YYYY-MM`
for monthly and recording periods, `YYYY` for yearly, and null for archive or
undated material. A transcript entry has its public `recording_id`; others use
null. Internal evidence remains in private canonical exports, not this projection.

Before admitting real summaries:

1. Release the corresponding corpus transcripts through the existing publication
   workflow; the current public corpus has no released recordings.
2. Map private transcript identities to public corpus recording IDs. Keep this
   map and internal evidence paths private. Do not infer mappings from names alone.
3. Select the preferred completed summary revisions; review the text and explicitly
   approve publication. Copy only the fields above, not private job metadata.
4. Resolve every cited transcript to a released, non-retracted, nonempty public
   transcript. The loader checks all referenced recordings and fails closed.
5. Run `node --test scripts/tests/summary-release.test.mjs` and `npm run build`.
   Inspect populated desktop/mobile reading pages before deployment.

The validator rejects missing/foreign sources, extra fields, duplicate scopes,
unapproved entries, invalid period labels, empty summaries, and invalid evidence
classifications. A retracted source fails the build until affected summaries are
withdrawn or re-reviewed. Source links are constructed from the public catalog;
the release cannot inject a local path or arbitrary source URL.

No automatic exporter or publication action is enabled by this UI preparation.
The remaining release work is the approved public transcript projection and source
mapping, followed by a populated UI review—not another summarization run.

## Preparation checks

Six release-contract/search/render-safety tests pass. Astro's type check and direct
static build pass, as does search isolation. The empty release was inspected in
headless Chromium at a 390-pixel viewport. Populated real-content review remains
pending source mapping and publication approval.

The full `npm run build` remains blocked by existing repository-wide publication
checks (local filesystem paths in unrelated operational files and access-bearing
URLs in existing transport tests). These safeguards were not disabled. A direct
Astro build is a UI diagnostic, not evidence that the repository is deployable.
