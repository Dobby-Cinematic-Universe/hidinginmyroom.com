# Cloudflare RAG production handoff

## Summary v19 contents and publication procedure (2026-10-01)

The owner requested the summary refresh and publication, and subsequently selected
Claude Sonnet 5.5 instead of Gemini for the broader-summary refresh.
The screened public projection `release-20261001-summaries-v19-screened` and
`candidate-20261001-summaries-v19` are the intended release/candidate, not a claim
of deployment. Release coverage is 3,760 transcript summaries plus 139 broader
summaries (3,899 total): 104 broader summaries refreshed with Sonnet 5.5 and 35
retained from Sonnet 5. The 4,034 transcripts, 4,103 catalog recordings and separate
Typesafe analysis of 4,034 recordings remain unchanged. Actual release counts,
provider provenance and hashes must be taken from validated artifacts and receipts.

The original assembled v19 snapshot remains private and unchanged. Four exact
residential-location passages in two transcript summaries are generalized only
in the screened projection, with separate editorial provenance and unchanged
classifications and source citations. Use the screened projection for both the
site bundle and public RAG export.

The receipt-bound helper is `pipeline/publish_summary_refresh_20261001.mjs`; records
are under `research/site-release-publications/release-20261001-summaries-v19/`.
Baseline backups include public and private RAG manifests/receipts when present,
the public Worker source allowlist, both release pins, preview pointer and prior
Pages latest/canonical deployment identities. The private pilot must not be
overwritten. The summary publication overlay must bind the approved candidate to
the exact v19 preview bytes before preparing the public RAG manifest, so the site
bundle and RAG export agree on summary release identity.
The retained corpus baseline pin contains 4,103 recordings, 4,034 transcripts and
2,870 summaries. The development preview pointer is not the deployed-site selector;
use the pinned bundle, matching Worker release and canonical Pages/live receipts
to establish publication state.

Continue using the existing instance and content-addressed keys: prepare the full
local manifest, retain accepted upload receipts and submit only its new/changed
documents. The owner requires removal of duplicate superseded summaries/transcripts
from the remote index. Reconcile only exact superseded keys absent from the current
manifest, with identity matched to prior upload receipts; refuse current-manifest
and unknown keys. Retain private recovery copies, prior manifests and upload/removal
receipts, not duplicate old index items. The new public allowlist excludes all
superseded keys. Do not clear current receipts, recreate the instance or trigger
a global resync. No index configuration change is part of this handoff.

The static site may deploy as soon as its audited bundle is remotely verified,
without waiting for Cloudflare RAG indexing. Backend activation separately requires
an accepted, matching upload receipt for every current document, the approved
allowlist/release and a private raw search endpoint. Global indexing status remains
advisory, not a completion gate; retrieval coverage may lag the static publication.

Cutover requires successful candidate/publication QA, remote bundle restoration,
matching approved allowlist/Worker release, Pages deployment and real-domain
Turnstile/search/answer/citation checks. Preserve `analysis-release.json`
byte-for-byte and verify its live identity, scored coverage and factor-loading hash.
The retained Stakey transcript and its timing must also remain unchanged. Final
publication receipts—not historical receipts or completed model jobs—establish
whether v19 was deployed. Provider output remains machine-generated and unreviewed;
citations do not independently establish the truth of Daniel's claims.

## Historical September production handoff

Approved origin: `https://hidinginmyroom.com`. On 2026-09-17 the owner approved
the pilot snapshot, then expanded approval to all usable corpus text for public
search and quoted answers. The September 19 quality-v13 snapshot contains 4,033
current cloud/third-party transcripts (17,522 files), 2,731 transcript summaries,
139 broader summaries, and 4,103 catalog metadata entries: 24,495 files,
approximately 207 MB. Historical local
ASR, private review artifacts, media, and identity embeddings remain excluded.

The owner authorized publishing quality-v13 after the R2 bundle verification.
The existing live site/chat remain on the previous release until that coordinated
cutover. `RELEASE_HANDOFF_20260919.md` records the release and receipt locations.

## Isolation and publication

The production Worker and `himr-public-corpus-v1` index are separate from the
private pilot. Raw AI Search endpoints stay disabled. Public responses and model
context are restricted to the content-addressed keys in `public-sources.json`.
Its release digest must match the deployed configuration. Unapproved or missing
configuration fails closed, including any attempt to use the private pilot index.

`RAG_PROFILE=public node scripts/rag/production.mjs stage` deploys a **disabled**
Worker with production bindings/secrets; this does not publish the website.
`RAG_PROFILE=public node scripts/rag/production.mjs activate --publish` refuses
activation until every manifest file has an accepted upload receipt. Publication
approval, matching source allowlist/release, the private raw endpoint, and explicit
`--publish` remain required. It then enables the Worker, not the site.

As requested on September 19, indexing completeness is **not a release gate**.
Queued, running, outdated or count-mismatched items are reported as advisory status;
failed/skipped items are flagged for attention without blocking release. Unavailable
stats likewise do not prevent release. Search coverage may be incomplete until
Cloudflare finishes indexing. No repeat upload or requeue is triggered by this
check. The same policy applies to release-candidate preparation and finalization.

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
supersedes documents still needs explicit stale-item reconciliation. Current
source allowlists continue to exclude superseded keys from public answers; an
aggregate remote document count is not used as a release authorization check.

### Incremental updates only

For an incremental release, retain the existing instance, embedding/chunk settings,
content-addressed document keys and `uploads.json`. `prepare --full` means exporting
the full manifest locally, **not** re-uploading or reindexing the whole archive.
`upload.mjs` skips accepted keys and submits only new/changed documents. Do not
clear receipts, recreate the index, run a global resync, or retry healthy items.
Failed-item recovery is separate and targets only receipt-bound failed keys.

Do not run `manage.mjs configure` as a release step. It now performs no remote write
when settings match and refuses configuration changes without the explicit
`--allow-index-rebuild` override. Use that override only after approval for a
possible full reindex, never just to unblock an incremental release. The release
workflow cannot prevent Cloudflare or dashboard actions from requeueing items;
a queued item is not evidence that it needs to be uploaded again.

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
closed. It records indexing status without waiting for completion and never invokes
activation or site deployment.
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

The source checkout's corpus and summary manifests intentionally remain empty
placeholders. `npm run build:pages` restores the approved checksum-pinned R2 bundle
before building; plain `npm run build` alone does not publish the local preview.
The real-domain Turnstile/source-link check must be repeated after each cutover.
See the release handoff for current publication approval and deployment status.

`npm run test:rag` covers private/public guards, input bounds, removal of global caps,
cooldown, caching, Turnstile failure/hostname/action, client limits and cleanup.
Run `npm audit`, `npm run check` and a Worker dry-run before deployment. Dependency
versions are lockfile-pinned; Wrangler is an exact dev dependency.

References: [Turnstile validation](https://developers.cloudflare.com/turnstile/get-started/server-side-validation/),
[Workers AI pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/),
[Durable Objects free limits](https://developers.cloudflare.com/durable-objects/platform/pricing/).
