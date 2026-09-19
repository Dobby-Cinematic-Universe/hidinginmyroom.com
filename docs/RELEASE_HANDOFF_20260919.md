# Release handoff — 2026-09-19

## Prepared release

- Local snapshot: `research/corpus/site-previews/release-20260918-v10`.
- Release candidate: `research/site-release-candidates/candidate-20260919-v10`.
- Deployable files, after the remaining launch check: the candidate's `dist/`.
- 4,064 catalog recordings; 3,991 transcript-bearing recordings; 2,691 transcript
  summaries and 139 broader summaries. The September 17 livestream is included
  in September 2026, yearly 2026, and the archive overview.
- All 24,269 Cloudflare documents were reconciled item by item on September 19:
  no missing, stale, duplicate, mismatched or failed items.

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

## Remaining launch actions

The old direct upload stopped without publishing the populated candidate. The
replacement uses the R2 bundle and automatic Pages builds from main. The new
workflow must be pushed and successfully built before the empty live corpus is
replaced. No DNS change or plan downgrade was performed.
After the automatic deployment, test a real
Turnstile challenge, search/answer and source link from the approved HTTPS origin.
Unauthenticated API checks and unit tests do not substitute for that browser test.

The production chat expiry was removed at the owner's request on September 19.
Production chat has no scheduled shutdown; the private pilot retains its separate
review gate. Spam protection and the manual enable/disable switch remain intact.
The temporary paid indexing plan was not changed.

For an emergency chat rollback, run
`RAG_PROFILE=public node scripts/rag/production.mjs stage`.
That disables chat without deleting the corpus or deploying the website.
Future export workflows also stage the Worker disabled; reactivation requires
clean completed indexing and explicit approval.
