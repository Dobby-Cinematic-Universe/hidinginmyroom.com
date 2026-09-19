# Failed built-in item recovery

The item-ID PATCH sync endpoint returned success with a null result, but repeated
GETs showed the original error and unchanged last-seen timestamp. This was not a
confirmed requeue. Historical receipts are retained in
`research/cloudflare-rag/public-v1/failed-retry-paid-20260917.json`.

The documented key-based endpoint works:

`PUT /accounts/{account}/ai-search/namespaces/default/instances/{instance}/items`

with `{ "key": "existing-file.txt", "next_action": "INDEX" }`.

The pilot preserved its item ID, moved from error to running, cleared its error,
and updated last-seen. `scripts/rag/requeue-failed.mjs` applies this to receipt-bound
failed items only. It does not upload content, delete objects, change embedding
models, rebuild completed items, publish the site, or change billing settings.
It checks current state before submitting, journals intents, and does not silently
repeat uncertain requests. Four lanes limit API pressure. The source manifest
and original upload receipts remain unchanged.

Run with `RAG_PROFILE=public node scripts/rag/requeue-failed.mjs`.
Audit: `research/cloudflare-rag/public-v1/failed-key-requeue-20260917.json`.
Requeued means Cloudflare accepted a queued/running/completed state, not that all
embeddings are finished. Confirm final status before downgrading the plan.

API reference: https://developers.cloudflare.com/api/resources/ai_search/subresources/namespaces/subresources/instances/subresources/items/methods/create_or_update/
