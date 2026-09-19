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

The source repository intentionally retains empty public corpus placeholders.
Do not deploy an ordinary repository build expecting it to contain this archive:
use the prepared candidate output. Research, provider receipts, transcripts,
private reviews, embeddings and credentials are not committed. The Worker source
allowlist contains approved public titles and URLs, not private credentials.

## Remaining launch actions

No website deployment, DNS change, Git push or plan downgrade was performed.
After explicit deployment approval, publish the candidate and test a real
Turnstile challenge, search/answer and source link from the approved HTTPS origin.
Unauthenticated API checks and unit tests do not substitute for that browser test.

The existing `FREE_REVIEW_BEFORE` safeguard pauses chat on
**2026-09-24 at 00:00 UTC**. Review the intended Workers plan and billing settings
before extending it. The temporary paid indexing plan was not changed.

For an emergency chat rollback, run
`RAG_PROFILE=public node scripts/rag/production.mjs stage`.
That disables chat without deleting the corpus or deploying the website.
Future export workflows also stage the Worker disabled; reactivation requires
clean completed indexing and explicit approval.
