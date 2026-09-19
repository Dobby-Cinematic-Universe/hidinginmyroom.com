# Private populated release rehearsal

## Latest candidate: 2026-09-17 v4

`candidate-20260917-v4` supersedes v1–v3 below. It is served privately at
`http://127.0.0.1:4322/corpus/`. Nothing has been deployed.

- 4,063 recordings, 3,990 usable transcripts, 2,689 transcript summaries,
  126 monthly summaries and 12 yearly summaries; no held summaries in this build.
- 890 entities, 15,598 event descriptions and 503 related-account groups.
  Descriptions occupy 256 anchored pages rather than one file per description.
- Public attribution for all 3,990 transcripts, including the targeted timing
  correction note. Private review decisions and local paths are not exported.
- 16,295 assets, 3,113,676,082 bytes; largest asset 11,699,357 bytes.
  Below the configured Free limits of 20,000 files and 25 MiB per asset.
- All 8,701 HTML pages passed root-relative link checks; no scoped private-path
  or credential-name findings. This is not a comprehensive privacy audit.
- Production search binding now explicitly declares recording-level indexing;
  browser QA caught and fixed a segment-count mismatch in v3. The candidate audit
  now fails if the rendered search count differs from the index manifest.

Browser QA verified recording search, inline summary expansion, entity filtering,
related-account and description links, the recovered transcript, native video
playback and seeking to 1:07:00.12. Mobile transcript testing found no horizontal
overflow. Private local RAG retrieval returned six linked source passages.
Public chat is deliberately absent from this staged static build; actual-domain
Turnstile and public-answer checks remain launch gates.

At 02:47 EDT an intermediate archive chunk failed validation. Investigation found
one duplicate evidence ID in events item 8. An audited copy removes only that
duplicate; text, classification, distinct evidence and original captures are
unchanged. The original validator passes. The final archive synthesis was accepted
at 03:03 EDT under the existing cap (additional maximum reservation $0.10828).
Monthly and yearly scopes are complete. No evidence checks were relaxed.

`himr-sonnet-evidence-recovery-20260917.service` collects that final request.
`himr-release-finalization-v2-20260917.service` waits for it and the existing RAG
follow-up, then refreshes the delta, reconciles backed-up superseded index objects,
builds/audits `candidate-20260917-v5`, and waits for clean remote indexing. It is
bounded to 24 hours and pauses on errors. It never publishes or activates AI.
State is in `research/cloudflare-rag/public-v1/finalization-status.json`.
The v4 preview stays unchanged until the new candidate has been checked.

Remaining: recover the archive overview, refresh the immutable summary snapshot,
build/audit a final candidate, finish indexing and reconcile stale index items,
and obtain explicit site publication approval. Earlier results below are historical.

## Rollout and rollback

1. Keep a copy of the current deployed site's deployment ID and approved release
   manifests. Do not replace the live site with this approval-simulation rehearsal.
2. After summary completion, prepare a fresh candidate using the updated preview
   as the second argument; run its audit and repeat source/search smoke tests.
3. Obtain publication approval for that exact site snapshot. The RAG text approval
   does not itself approve a website deployment. Promote only public projections;
   never copy `.env`, review data or research folders into a deployment.
4. Pages access was confirmed after the owner updated permissions. The existing
   project is `hidinginmyroom-com`, with `hidinginmyroom.com` and
   `hidinginmyroom-com.pages.dev`. The read-only deployment/rollback target receipt
   is `research/cloudflare-rag/public-v1/pages-release-target.json`. No deployment
   was performed. Use the authorized deployment method for this exact project.
5. Deploy the approved static site with chat disabled first, check the real domain,
   then follow `CLOUDFLARE_RAG_PRODUCTION.md` for gated activation and the final
   chat-enabled build. The rehearsal script intentionally forces chat disabled.
6. Roll back site content using the retained previous Pages deployment. Disable
   public AI immediately with the documented RAG `stage` command if needed.
   Do not delete corpus sources or upload receipts during rollback.

Final domain checks: source links, search, mobile layout, media fallback, real
Turnstile verification, retrieval and cited answers. Keep ordinary search usable
when the free AI allowance is unavailable. Never enable paid fallback.

Prepared 2026-09-17 at `research/site-release-candidates/candidate-20260917-v1`.
Nothing was deployed or installed into the working site's production manifests.
The candidate is private and is **not approved for deployment**. Summary approval
fields are simulated only inside this isolated production-build rehearsal; they
are not publication decisions. Do not deploy its `dist` directory directly.

## Results

- 4,063 recording pages; 3,989 usable transcripts.
- 2,688 transcript summaries plus 125 monthly and 11 yearly summaries.
- 14,639 output files, 3,053,289,287 bytes total; largest file 11,698,893 bytes.
- No files exceed the 25 MiB Pages asset limit; the current candidate is below
  the 20,000-file Free limit. This excludes the still-unpackaged derived graph.
- Search reused the matching recording-level index: all 3,989 recording texts.
- Search isolation passed: 56 main-index pages, with corpus HTML excluded.
- All 7,049 generated HTML pages were checked for root-relative links; no missing
  file targets were found. Fragment targets and browser interactions require
  separate smoke tests. No private paths or credential variable names matched
  the scoped output scan; that scan is not a comprehensive privacy review.

## Unresolved gates

1. The archive overview is still processing. Refresh the snapshot after collection.
2. February 2023 and yearly 2023 cite `dirty floor cooking [mfvEeYhkGKs]`.
   Its source transcript contains one zero-duration segment at 4,020,120 ms, out
   of 1,940 segments. No timestamp was fabricated or source overwritten. The two
   summaries remain visible in the private local preview with a catalog-only
   notice, but are withheld from this stricter production rehearsal.
3. Derived Entities & Events and related-account groups currently run only in
   development mode. The production graph is empty; explicitly package it and
   recheck file-count limits before a complete release. A page per event may
   require a more compact static representation to stay on Free.
4. Project public attribution/provenance fields without copying private review
   artifacts, identity maps, or source paths.
5. Finish public RAG indexing, refresh newly available summaries, reconcile stale
   document receipts, and test Turnstile/search/answers on the approved domain.
6. Run final browser/mobile/media/source-link checks on the populated build and
   obtain deployment approval. The original production placeholders are unchanged.

## Reproduce

To include the configured production chat UI in a private candidate, pass
`--production-chat` after the explicit preview directory. This reads only the
public endpoint and Turnstile site key from the approved public RAG configuration.
It does not activate the Worker or deploy Pages. The report records whether the
backend was enabled at build time. The owner requested that activation wait for
indexing to finish; retain the complete/clean indexing gate in `production.mjs`.

The September 17 v8 candidate includes the removal of repetitive “Uncertain”
labels and the production chat UI, with its backend intentionally still disabled.

Use a fresh name; existing candidates are never overwritten:

```sh
node scripts/prepare-release-candidate.mjs candidate-YYYYMMDD-vN
node scripts/audit-release-candidate.mjs candidate-YYYYMMDD-vN
```

Preparation copies only the site source, public assets, build scripts, dependencies
reference, and selected public-format corpus projection. It does not copy `.env`,
review files, media, or the private preview pointer. It makes no provider calls.
It uses separate Astro/Vite caches so the live development server is unaffected.
The candidate reports retain exact counts, held sources, and hosting-size results.

## Targeted timing recovery (v2 candidate)

At the owner's request, `scripts/repair_release_point_segment.py` prepares the
fresh `release-20260917-v9` snapshot. The original provider result and normalized
transcript remain unchanged, verified against their retained SHA-256 binding.
Only segment 275 receives a 1 ms **display interval**: start 4,020,120 ms is kept,
and end becomes 4,020,121 ms. The provider supplied a point timestamp for all five
words, so this is not a recovered acoustic duration or an accuracy claim. Every
word and all other segment timestamps remain unchanged. The private preparation
report records the before/after binding and explicit uncertainty; its local
annotation carries the correction notice for subsequent public attribution export.

Candidate `candidate-20260917-v2` uses this corrected snapshot, admitting the
recording, its existing transcript summary, February 2023, and yearly 2023. The
old v1 rehearsal remains available. Neither the live preview pointer nor original
production data is changed by this operation, and no paid calls are made.

```sh
python3 -B scripts/repair_release_point_segment.py
HIMR_CORPUS_DATA_ROOT=research/corpus/site-previews/release-20260917-v9/corpus \
HIMR_CORPUS_PREVIEW_OUTPUT=research/corpus/site-previews/release-20260917-v9/search \
node scripts/build-corpus-index.mjs
node scripts/prepare-release-candidate.mjs candidate-20260917-v2 release-20260917-v9
node scripts/audit-release-candidate.mjs candidate-20260917-v2
```

These commands require fresh output names. Do not rerun the one-off preparer over
an existing snapshot or overwrite the original transcript. The first interrupted
preparation attempt is retained as `release-20260917-v9-incomplete` for audit.
