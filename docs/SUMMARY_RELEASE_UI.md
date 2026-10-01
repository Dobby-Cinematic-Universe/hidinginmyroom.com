# Summary release UI

## October 1 release contents and delivery

The v19 release scope is 3,760 recording summaries and 139 broader summaries
(3,899 total). Of the broader summaries, 104 use the selected Claude Sonnet 5.5
refresh and 35 retain Claude Sonnet 5 results. Actual provider provenance and final
counts are validated against saved responses and the candidate artifacts. The
release preserves the 4,034 available transcripts and their existing Typesafe
analysis. These contents do not establish deployment status: publication is
recorded by the final candidate, verified bundle, deployed commit and live checks
under `research/site-release-publications/release-20261001-summaries-v19/`.

Expandable recording summaries now load through a release-bound index and
content-hashed JSON shards, with at most 64 summaries and 4 MiB per shard. This
keeps the expanded archive below the Pages file limit while retaining every
standalone summary HTML page and source citation route. The old per-recording
JSON endpoints were internal fetch targets; the client now uses
`/corpus/summaries/data/index.json` and its shard descriptors. Shards retain the
same text, classifications and source recording IDs. Clients validate checksums,
release identities and entry mappings, reuse shared downloads, and allow retry
after a failed load. The candidate restores the existing pinned analysis bundle
before building, and selects event groups from its explicit preview snapshot.

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

The build reads the public projection in `src/data/summaries/release.json`, not
private reader exports. The source checkout deliberately retains an empty
placeholder there; it is not the contents of the deployed corpus. The production
Pages build restores the approved, checksum-pinned `corpus-release.json` bundle
before building. The separate `analysis-release.json` pin restores analysis data.
The v19 baseline pin contains 4,103 recordings, 4,034 transcripts and 2,870 summaries;
the refresh changes summaries, not transcript or analysis coverage. The local
preview pointer is a development selection, not proof of what Pages deployed.

An explicitly activated, development-only
[local candidate](LOCAL_CORPUS_SUMMARY_PREVIEW.md) can display prepared material
without changing either production pin. Private generation completion is not
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

Before admitting new or refreshed summaries:

1. Verify the corresponding transcripts belong to the selected approved corpus
   release. Existing released transcripts do not need to be retranscribed or
   republished merely because their summaries are refreshed.
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

The UI itself performs no export, model request or publication action. Release
tooling separately validates the approved public projection and source mapping,
populated UI, candidate audit and remote restoration before pin activation. Final
real-domain verification and publication receipts establish the deployed release.

## Historical initial preparation checks

The following checks describe the initial empty-placeholder UI rehearsal, not
the current deployed data or the October 1 candidate's validation results.

Six release-contract/search/render-safety tests pass. Astro's type check and direct
static build pass, as does search isolation. The empty release was inspected in
headless Chromium at a 390-pixel viewport. Populated real-content review remains
pending source mapping and publication approval.

The full `npm run build` remains blocked by existing repository-wide publication
checks (local filesystem paths in unrelated operational files and access-bearing
URLs in existing transport tests). These safeguards were not disabled. A direct
Astro build is a UI diagnostic, not evidence that the repository is deployable.
