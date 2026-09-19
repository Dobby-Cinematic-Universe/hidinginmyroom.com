# Cloudflare RAG production handoff

Approved origin: `https://hidinginmyroom.com`. On 2026-09-17 the owner approved
the pilot snapshot, then expanded approval to all usable corpus text for public
search and quoted answers. The full snapshot contains 3,989 current cloud/third-party
transcripts (17,347 files), 2,688 transcript summaries, 136 broader summaries, and
4,063 catalog metadata entries: 24,234 files, approximately 204 MB. Historical local
ASR, private review artifacts, media, and identity embeddings remain excluded.

## Isolation and publication

The production Worker and `himr-public-corpus-v1` index are separate from the
private pilot. Raw AI Search endpoints stay disabled. Public responses and model
context are restricted to the content-addressed keys in `public-sources.json`.
Its release digest must match the deployed configuration. Unapproved or missing
configuration fails closed, including any attempt to use the private pilot index.

`RAG_PROFILE=public node scripts/rag/production.mjs stage` deploys a **disabled**
Worker with production bindings/secrets; this does not publish the website.
`RAG_PROFILE=public node scripts/rag/production.mjs activate --publish` refuses
activation until every manifest file receipt exists and remote indexing is complete
with no pending, skipped or failed files. It then enables the Worker, not the site.
Do not bypass this gate to launch a partially populated public service.

The full uploader runs as `himr-public-rag-full-upload-20260917.service`. Inspect with:

```
systemctl --user status himr-public-rag-full-upload-20260917.service
journalctl --user -u himr-public-rag-full-upload-20260917.service -n 20
RAG_PROFILE=public node scripts/rag/manage.mjs status
```

Four upload lanes, serialized atomic receipts, stop-on-error, and exact-key
reconciliation avoid blind duplicate POSTs. Restart after inspecting any error;
never remove a live upload lock. Managed indexing may lag uploads.

The full snapshot reused all 3,162 pilot receipts without another upload. Preparation
is explicit, network-free, and blocks while the uploader holds its lock:

```
RAG_PROFILE=public node scripts/rag/prepare.mjs --full --publication-approved
RAG_PROFILE=public node scripts/rag/production.mjs prepare --full --publication-approved
```

The export includes all summary levels available in the selected preview snapshot;
future completed summaries require another export. Catalog-only entries are clearly
marked as metadata, not evidence of speech. Broader summaries retain recording links.
Never overwrite the private pilot to expand the public index. A later snapshot that
supersedes documents requires explicit stale-item reconciliation before activation;
the exact remote-count activation check deliberately fails closed in that situation.

## Website rollout

### Queued release delta (2026-09-17)

`himr-public-rag-release-refresh-20260917.service` waits for the current uploader
to drain without disturbing it. It refreshes summaries in `release-20260917-v9`,
exports the recovered transcript and current summaries, reuses accepted receipts,
uploads only the delta and stages the Worker disabled. It does not activate AI or
deploy the site. Check `followup-status.json` in the public RAG working directory.
If superseded keys are recorded in `stale-index-items.json`, reconcile those exact
items before activation; the follow-up does not silently delete remote content.
Summaries completed after that snapshot need a subsequent explicit delta.

The dependent `himr-release-finalization-v2-20260917.service` handles that final
archive-summary delta and builds/audits private candidate v5. It then invokes
`reconcile-stale.mjs --apply`: only exact superseded keys absent from the current
manifest, matched to upload receipts, are removed from the disabled public index.
Local document copies and the prior receipts are retained in a removal audit, so
these index objects can be rebuilt. Current keys and unknown identities fail
closed. It waits for clean indexing and never invokes activation or site deployment.
Inspect `finalization-status.json`; failures pause rather than resubmit blindly.

After activation, `research/cloudflare-rag/public-v1/public-client.json` contains
the three non-secret build settings: `PUBLIC_RAG_ENABLED`, `PUBLIC_RAG_ENDPOINT`,
`PUBLIC_TURNSTILE_SITE_KEY`. Set them in the **production site build environment**.
Do not put API tokens, Turnstile secrets, HMAC secrets or private bearer tokens in
any `PUBLIC_` variable. Build/deploy the site's approved corpus release separately;
verify every source link exists there. RAG approval does not automatically promote
the entire private site preview or its unrelated pages to a public release.

Before launch, test a real Turnstile challenge from the approved origin, a search,
an answer, and a source link. This real-domain browser test is a release gate, not
replaced by mocked unit tests. No production DNS/site deployment is performed by
the RAG scripts. Cross-origin API requests are supported only from the exact origin.

## Abuse and free-only operation

Server-side Turnstile checks success, hostname, action, and single-use tokens via
Siteverify. Exact-origin checks supplement rather than replace token validation.
Daily rotating HMAC client IDs avoid persisting raw IP addresses. Admission state
expires after 48 hours. Attempt limits: 20/hour and 50/day per client before token
verification. Invalid tokens never reach AI retrieval. At the owner's request,
there are no artificial global daily caps or shared single-request queue.
Different questions execute independently; identical questions share a five-minute
cache and in-flight guard. Question/context/output limits and a per-question
60-second provider-error cooldown remain. Browser and provider waits are bounded.
Timeout does not guarantee provider cancellation.
Distributed attackers can exhaust the free allowance: ordinary search must remain
available. Free-tier protection sacrifices availability; this is not an unlimited
public chat service or an SLA.

The owner temporarily upgraded Workers for indexing; plan changes are manual.
Limits are account-shared. Production chat has no scheduled expiry as of the
owner's September 19 request. `FREE_REVIEW_BEFORE` applies only to the private
pilot. Spam protections and the manual enable/disable switch remain intact.
Operational logs do not include questions, answers, credentials or raw IPs.

## Rollback / takedown

Run the `stage` command to disable public AI immediately, then rebuild the site
with `PUBLIC_RAG_ENABLED=false`. Ordinary search is unaffected. For a takedown,
disable first, remove the affected key from the release allowlist and public
index, generate a new release/version (invalidates cached results), verify source
removal and redeploy before re-enabling. Do not reuse the old corpus version.

## Verification

Release-check findings were resolved on 2026-09-17 without modifying the checker:
portable documentation/fixture paths, repository-derived runtime defaults, and
explicitly constructed synthetic signed-URL fixtures. `npm run build` passes its
public boundary, Astro, production build, corpus indexing and search-isolation
checks. The 109 affected Python tests and 40 static-tool tests also pass.

The production corpus and summary manifests are still empty placeholders; this
successful build does not publish the populated local preview. Remaining launch
steps are cloud indexing completion, promotion of the approved corpus/source pages,
and the real-domain Turnstile/source-link test. RAG publication approval does not
automatically promote unrelated preview content. No public site was deployed.

`npm run test:rag` covers private/public guards, input bounds, removal of global caps,
cooldown, caching, Turnstile failure/hostname/action, client limits and cleanup.
Run `npm audit`, `npm run check` and a Worker dry-run before deployment. Dependency
versions are lockfile-pinned; Wrangler is an exact dev dependency.

References: [Turnstile validation](https://developers.cloudflare.com/turnstile/get-started/server-side-validation/),
[Workers AI pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/),
[Durable Objects free limits](https://developers.cloudflare.com/durable-objects/platform/pricing/).
