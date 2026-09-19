# Release handoff — 2026-09-19

## Current release — quality v13 (approved for publication)

The owner authorized pushing and deploying this release once ready on September 19.
The publication procedure waits for verified R2 restoration before pushing `main`,
uses the existing automatic Pages deployment, then coordinates the chat allowlist
and superseded index entries. Read the deployment receipt for the final outcome;
this preparation record alone is not proof of a successful deployment.

- Local snapshot: `research/corpus/site-previews/release-20260919-quality-v13`.
- Release candidate: `research/site-release-candidates/candidate-20260919-quality-v13`.
- 4,103 catalog recordings; 4,033 transcript-bearing recordings; 2,731 transcript
  summaries and all 139 broader summaries. No summaries are held in this candidate.
- Targeted recovery refreshed 19 broader summaries: 10 monthly, eight yearly and
  the archive overview. It fixed the original placeholder/repair-marker findings,
  plus the subsequently discovered May 2024 `x` and 2026 `skip` outputs. The archive
  overview now includes 2026 and references 17 sources shared with that yearly
  summary. All 2,731 transcript summaries and 120 unaffected broader summaries
  were preserved. Original provider captures and repair/reuse receipts remain private.
- The build and audit passed, with 8,813 HTML pages, no broken root-relative links,
  no scoped private-path findings, zero summary-quality findings, and 16,516 output
  files below the configured Pages limits. These checks do not constitute a factual
  review of every generated claim. Local HTTP checks confirm recovered monthly,
  2026 yearly and archive pages render the new prose.
- The refreshed graph contains 895 entities, 15,825 event descriptions and 519
  related-account groups. Existing cached grouping embeddings were reused.
- All 24,495 current index documents have accepted upload receipts. This recovery
  uploaded only 19 changed documents and reused 24,476 unchanged ones. No global
  reindex, remote deletion, chat disablement or Worker deployment was performed.
- The 232,424,050-byte bundle is uploaded and remotely verified. Its SHA-256 is
  `4a63188ebc609ae585285601e3584d29d8483fad12f91bdbd53b595931a9ad1c`.
  Remote download, checksum, archive and content-quality validation passed at
  21:22:55 UTC. The local `corpus-release.json` now pins that verified bundle.
  Uploading/pinning this data does not itself deploy Pages.
Progress and receipts are under
`research/private-summaries/summary-quality-tail-20260919/finalization/`.
The finalizer is `himr-summary-quality-release-v13-20260919.service` and never
commits, pushes, deploys, disables chat or deletes superseded index items.
Authorized deployment is a separate operation; its receipt is
`research/site-release-publications/release-20260919-quality-v13/publication.json`.

The rejected v11 and intermediate v12 snapshots are retained for audit and must
not be published. The v11 R2 download/checksum passed, but its placeholder content
failed restoration before the old release pin could change. Release preparation,
index export, bundle packing and bundle restoration now reject filler summaries.

Production chat is enabled for `https://hidinginmyroom.com`. The raw AI Search
endpoint is private. The enabled Worker rejects requests lacking Turnstile
verification; no API keys are exposed to the browser. Production client settings
are captured in the private `research/cloudflare-rag/public-v1/public-client.json`.
The candidate includes the production chat UI and its non-secret configuration.

The repository is now site-focused: frontend/assets, release and search tooling,
the chat Worker, corpus validation code, optional event grouping, tests, and
relevant documentation. Acquisition/ASR/diarization, private consoles, evaluation
tooling, historical recovery scripts and operational diaries were removed from
tracking but preserved locally under ignore rules. The earlier commits remain
in Git history; this cleanup does not rewrite history or delete local pipelines.

The source repository intentionally retains empty public corpus placeholders.
Do not deploy an ordinary `npm run build` expecting it to contain this archive:
the Pages command `npm run build:pages` restores the checksum-pinned approved R2
bundle from `corpus-release.json` first. Research, provider receipts, transcripts,
private reviews, embeddings and credentials are not committed. The Worker source
allowlist contains approved public titles and URLs, not private credentials.

## Browser QA and index reconciliation

On September 19, the real HTTPS site served the previous populated release
(4,064 recordings / 3,991 transcripts). Browser QA verified ordinary search
(`camera`: 1,443 matches), a recording result with filtered transcript, a successful
chat answer to “What cameras does Daniel discuss?”, and its linked February 2016
summary. The normal production verification/admission flow accepted the request;
no test token or bypass was used. This checks the **existing live release**, not
the unpublished v13 candidate. Playback itself was not verified in this pass.

All 66 listed stale keys were checked remotely with receipt IDs and local backups:
35 previous deletions are confirmed absent; the other 31 are still referenced by
the deployed Worker allowlist. Those 31 must remain until a coordinated release
switch. No remote item was deleted and chat was not disabled. The detailed private
receipt is `research/private-transcriptions/archive-coverage-20260919/finalization/stale-preflight.json`.

The v13 delta adds 19 newly superseded keys, for 50 pending keys total. The 35
confirmed historical deletions were excluded from that list. Keep all pending
keys until an authorized coordinated cutover; recheck live references then.
At 21:13 UTC, Cloudflare reported 7,654 completed, 16,882 queued, two running,
zero errors and zero skipped items. Provider totals include superseded documents;
accepted uploads are not the same as completed indexing. Index progress is advisory.

## Remaining release actions

1. Push the reviewed release source and verified R2 pin to `main` to trigger the
   configured automatic Pages deployment; confirm its deployed commit matches.
2. Under the owner's publication approval, coordinate the Pages release pin and Worker
   source allowlist, reconcile the 50 pending superseded keys, then repeat real-domain
   search/chat/source-link checks on the new release. Do not run `stage` during
   prepublication preparation: it disables the current production Worker.

The original 3.1 GB direct deployment remains abandoned. The site uses R2 data
bundles with automatic Pages builds from main. No push, deployment, DNS change or
plan downgrade was performed during the preparation checks. Publication is now
authorized as a separate operation after bundle validation.

The production chat expiry was removed at the owner's request on September 19.
Production chat has no scheduled shutdown; the private pilot retains its separate
review gate. Spam protection and the manual enable/disable switch remain intact.
The temporary paid indexing plan was not changed.

For an emergency chat rollback, run
`RAG_PROFILE=public node scripts/rag/production.mjs stage`.
That disables chat without deleting the corpus or deploying the website.
Future export workflows also stage the Worker disabled; reactivation requires
accepted uploads, matching approved sources, a private raw endpoint and explicit
approval. Indexing completeness is no longer a release blocker: queued or running
items may continue after release, and failed/skipped items are reported as advisory
issues to investigate. Chat coverage can be incomplete during indexing.
