# Private free-only RAG pilot

Production staging, Turnstile, release isolation and launch gates are documented
in [the production runbook](../../docs/CLOUDFLARE_RAG_PRODUCTION.md). The public
Worker is a separate deployment; do not expose this private pilot endpoint.

Local UI: http://localhost:4321/corpus/ask/ (development only).
The browser never receives the Worker bearer secret or management API token.
The local proxy requires loopback access and an exact same-origin POST.
The Worker requires a bearer token; the AI Search public endpoints are disabled.

Approved scope: existing transcript summaries plus transcripts from 100 recordings.
No media, speaker embeddings, or review artifacts are uploaded. Inputs and upload
receipts live in ignored `research/cloudflare-rag/pilot-v1/`, not the static site.
Cloud/third-party transcripts are used, never historical local ASR.

Run from the project root:

```
node scripts/rag/prepare.mjs
node scripts/rag/manage.mjs configure
node scripts/rag/manage.mjs deploy
node scripts/rag/upload.mjs
node scripts/rag/manage.mjs status
node --test scripts/rag/worker.test.mjs
```

Upload receipts make accepted uploads resumable. An uncertain upload is looked up
by exact content-addressed key; if no receipt is found it stops for reconciliation.
Upload acceptance is not indexing completion. Check remote stats for errors.

Uses hybrid keyword/vector retrieval, Qwen 0.6B embeddings, and optional Workers AI
Llama 3.1 8B answers. No paid-provider fallback or plan upgrades. The user confirmed
Workers Free and no paid AI billing/credits. Cloudflare's free platform quota is
the spending boundary. There are no application-wide daily caps or global request
queue. Identical questions share a five-minute cache and per-question in-flight guard.
Other account activity shares Cloudflare's allowance. Exhaustion returns an error.

The query endpoint pauses September 24, 2026 UTC for review of beta/free pricing.
This does not delete cloud data or pause Cloudflare's managed indexing. Do not
enable paid billing or expand the corpus without reviewing cost and authorization.
For a public release, first implement Turnstile, per-client abuse controls, public
data review and renewed capacity limits. This bearer-protected pilot is NOT a
public chatbot. Existing transcription/summarization jobs are independent.
