# Gemini transcript-only archive campaign

`python3 -B -m pipeline.transcript_summary_campaign` runs a finite, explicitly
selected archive through Gemini Batch. It never starts Sonnet or monthly/broader
synthesis, discovers new recordings, uploads raw media, changes ASR/screening, or
publishes summaries. Keys are read lazily from the existing private repository
`.env`; they are not saved in requests, status, or service arguments.

The first shard is a production canary (one recording in the original launch;
one retained group in the recovery campaign). Subsequent shards start
only after its chunk and final transcript outputs pass local validation. That
validation checks structure and internal evidence integrity, not semantic
completeness or truth. The September launch uses 24,000-byte initial requests and
180,000-byte reduction requests, plus explicit coverage and self-description
instructions. This addresses the pilot's overly selective first-stage summary;
review live results before relying on the prose.

Use `--canary-only` for an explicit review pause: only shard zero may be prepared
or submitted. After its final transcript passes local validation, both exports
are written and the command returns `awaiting_canary_review` with exit code 0.
This is a deliberate pause, not archive completion or semantic approval. Review
the prose against the beginning, middle and end of the transcript, then rerun
the same manifest without `--canary-only` to allow archive fan-out. No manifest
edit or repeated canary submission is needed. Canary-only mode refuses a workspace
where later shard plans already exist; it does not cancel running work.

The canonical local schema remains strict. Gemini's wire request uses its native
`responseSchema` format, including required object fields, property ordering and
classification enums. Legacy requests retain their original wire schema.
The corrected recovery contract admits up to 256 valid evidence references per
item instead of 24. New manifests use `gemini_schema_policy: local_array_bounds_v2`:
array maxima and the 1,200-character item-text limit remain locally enforced and
in the prompt, but array maxima are not sent to Gemini. A live A/B diagnostic on
2026-09-13 reproduced `INVALID_ARGUMENT` with the nested `maxItems` schema and
successful generation without those bounds. Older contracts retain their exact
wire bytes for receipt replay; do not rewrite a paid request in transport.
The prompt explicitly requires object arrays and supplied evidence IDs. The
projected wire request is bound before submission; malformed string, null or
flattened arrays are rejected, never repaired by inventing evidence.

New campaigns also seal `classification_policy: conservative_evidence_inheritance_v1`
into each Gemini wave. An item's classification is no more certain than either
the model's tag or any cited evidence: `reported_statement` →
`reported_allegation` → `uncertainty`. Raw transcript excerpts have no inherited
tag. This adjusts metadata only; it never changes words, adds evidence, removes
links, clips text or upgrades a claim. Every adjustment is recorded in the
collection, and the original provider capture is retained unchanged. All other
structural and evidence-integrity checks still apply. This is not a semantic
entailment check, fact check, or permission to remove cautious wording from prose.
Legacy waves retain their original validation for exact receipt replay.

## Budget and interruptions

The campaign has a maximum $120 USD local text-generation allowance. Before every
new wave it sums validated terminal token-usage estimates and full conservative
reservations for all other paid intents. It admits the wave only if that sum plus
the new wave's reservation fits the allowance. The original runner's cumulative
per-plan reservation guard is unchanged.

Terminal Gemini `totalTokenCount` includes input, candidates and thinking. Input
is charged at the configured Batch input rate; the remainder is charged at the
output rate, including any extra tokens. Missing or inconsistent usage, an
unexpected model, or unsupported cache/tool usage does not release the original
reservation. Reported usage exceeding a reservation stops the campaign.

This is a conservative local estimate, **not a Google invoice or an account-wide
billing cap**. It excludes taxes, unrelated API activity and prior pilots. Do not
submit this campaign's shard waves manually in parallel with the controller: the
aggregate guard applies to submissions made by this controller. Budget exhaustion
pauses work; it never raises the allowance automatically.

Offline preparation accepts `--budget-microusd` from 1 through 120,000,000;
the default remains 120,000,000 ($120). The chosen allowance is sealed into the
manifest and cannot be overridden at run time. For an explicitly authorized
fresh recovery campaign, retain proof of the previous campaign's settled usage
and outstanding holds, and subtract both from the aggregate allowance. For
example, 7,085 microdollars already used with zero holds leaves 119,992,915
microdollars for a new campaign under a shared $120 allowance. The option does
not import old results, migrate state, or calculate that deduction automatically.

There are no automatic paid retries or provider fallbacks. An uncertain POST
retains its durable intent and any returned operation receipt, then stops for
reconciliation. A terminal invalid result is retained for review; unrelated
transcripts in the same shard, as well as unrelated shards, may continue.
A failed canary prevents archive fan-out after its remaining schedulable work
finishes. Existing remote
batches are not cancelled or recreated when the local controller stops.
Gemini batch creation has a 180-second response deadline; status requests retain
their configured timeout (60 seconds by default). A timeout is not proof that
Google rejected the batch: inspect and reconcile its exact wave identity before
resuming, without blindly repeating the POST.

SIGTERM now requests a graceful pause: an in-flight request finishes and keeps
its receipt, and the controller checks for the pause before another submission.
Allow at least 240 seconds for the service stop timeout. A forced process kill
still requires normal intent/receipt reconciliation.

## Explicit recovery of already-paid results

`pipeline.transcript_summary_recovery` prepares a **new**, offline campaign from
a stopped producer whose paid batches have all been collected. Preserve a
hash-bound snapshot of the producer's exact implementation before changing code;
use that implementation for GET-only collection of any outstanding batches.
The recovery command takes the producer manifest and code-proof bindings, an
audit directory, a fresh manifest path and a fresh workspace path. It does not
submit anything or bypass implementation checks on existing plans.

Recovery retains exact chunk boundaries, source coverage and every valid internal
evidence link. Saved provider responses are revalidated under the corrected
contract, without clipping text or dropping citations. Successful chunks and
recovered chunks are imported as completed dependencies and cannot enter a new
paid wave. Completed whole-transcript groups remain in the original workspace,
are excluded from new work, and are counted separately in combined progress.

An explicit recovery manifest permits one additional paid attempt only for each
old failed chunk that still cannot pass validation. Never-submitted chunks and
new transcript reductions remain ordinary work. There is no retry loop: a second
invalid result remains review-required. The new budget subtracts all original
reported usage **and unknown-usage holds**; local text recovery never releases
a hold. Before starting, the controller replays the parent selection, admission
proofs and accounting. Keep the original controller stopped throughout recovery.

```sh
python3 -B -m pipeline.transcript_summary_recovery \
  --producer-manifest /absolute/private/original.manifest.json \
  --producer-sha256 ORIGINAL_SHA256 \
  --producer-code /absolute/private/producer-code.json \
  --producer-code-sha256 CODE_PROOF_SHA256 \
  --audit-root /absolute/private/recovery-admissions \
  --output /absolute/private/recovery.manifest.json \
  --state-root /absolute/private/recovery-campaign
```

Start the resulting manifest with the normal campaign command, initially using
`--canary-only` when a prose-review pause is desired. Status distinguishes imported
chunk results, newly recovered chunks, newly finished whole-transcript summaries
and previously completed summaries. Validation failures include a specific local
reason. Progress is refreshed around preparation, polling and submission; the
full shard inventory lives in `status.json`, with compact service-journal updates.

For the failed bounded-schema canary only, recovery also accepts `--schema-repair`
and `--schema-repair-sha256`. This binds a private transition object containing
`producer_manifest`, `producer_code` (the failed recovery's exact code snapshot),
and an optional `diagnostic` report binding. The original producer arguments
still identify the archive campaign with the paid chunks. Recovery permits this
transition only when precisely one reducer canary wave is terminal and every
request failed with code 3 / `Request contains an invalid argument.` It rejects
pending requests, successful reducers, paid chunk attempts and later shards.
All canary reservations and diagnostic allowances are subtracted as additional
prior holds. This does not grant general retry authority or discard successful
work. New provider failures expose a bounded error code in status; remote error
text is not copied into the controller log.

`pipeline.transcript_summary_classification` supports a separate, narrowly bounded
offline continuation for a stopped recovery canary whose reducer outputs were
returned successfully but held for classification metadata. It requires the
producer manifest and exact pre-change code snapshot, one collected first-shard
final-reducer wave, and no later shards. It imports every valid final unchanged
and admits other finals only if conservative tag inheritance makes them pass
all existing checks. Missing outputs, provider errors, foreign references and
overlong prose still stop recovery; no new paid retry is authorized.

The continuation reuses the other chunk admissions unchanged, preserves the
complete source partition and all prior receipts, and adds the canary's measured
usage or unknown-usage holds to the budget exactly once. Its first shard is
already complete after import, so exporting it and proceeding does not submit
another canary. Status distinguishes `imported_transcript_jobs` from imported
chunks. Invoke it with the same seven producer/audit/output path and hash
arguments shown above, replacing the module name with
`pipeline.transcript_summary_classification`. Preserve the stopped producer and
all referenced admission/proof files for future replay.

Each selected source is checked when its immutable shard plan is prepared and
reloaded; this checks transcript JSON, not the raw-media archive. A changed source
or implementation stops rather than silently changing a paid plan. Planning is
sharded to bound memory and replay work. Empty transcripts require no paid jobs.

## Persistent recovery checkpoints

`pipeline.transcript_summary_checkpoint` is an optional launcher for the existing,
unchanged campaign implementation. It avoids repeating expensive reconstruction
of already-validated recovery admissions after a process restart. It does not
regenerate summaries or make a restart a new paid attempt. This optimization is
for imported recovery history; live campaign plans and paid receipts still get
their normal checks, so total restart time can grow with the current campaign.

Preparation performs one full offline recovery validation, then writes a small,
immutable private checkpoint. It contains hash-bound admission references and
filesystem witnesses, not another copy of the transcripts or model results.
It also records the inventory of the **closed producer** workspaces: adding an
old shard, wave or receipt invalidates reuse even if existing files are unchanged.
Normal writes in the current campaign do not invalidate that old-history cache.

```sh
python3 -B -m pipeline.transcript_summary_checkpoint prepare \
  --manifest /absolute/private/campaign.json \
  --expected-sha256 MANIFEST_SHA256 \
  --output /absolute/private/separate-checkpoints/recovery-v1.json

python3 -B -m pipeline.transcript_summary_checkpoint check \
  --checkpoint /absolute/private/separate-checkpoints/recovery-v1.json \
  --expected-sha256 RETURNED_CHECKPOINT_SHA256

python3 -B -m pipeline.transcript_summary_checkpoint run \
  --checkpoint /absolute/private/separate-checkpoints/recovery-v1.json \
  --expected-sha256 RETURNED_CHECKPOINT_SHA256 --allow-paid-api
```

Keep the returned checkpoint SHA-256 in the launch command or service
configuration, outside the checkpoint itself. Never calculate a new hash for a
hand-edited checkpoint and treat it as validated; build a fresh checkpoint with
`prepare` instead. A changed checkpoint, manifest or implementation is rejected.
Changed proof-file witnesses cause the original validator to replay the affected
admissions; a changed old-producer inventory causes a full replay. Actual content
or source-binding mismatches still stop processing before new paid work.

Each restore verifies the exact admission bytes and uses the same bounded caches
already used by the runner. Witnesses include device, inode, size, nanosecond
mtime/ctime, mode, owner and link count—not just a timestamp or filename. Treat
these as a local filesystem cache, not forensic verification after an untrusted
filesystem/snapshot restore; use the ordinary launcher or build a fresh
checkpoint when those witnesses cannot be trusted.

The original campaign still recomputes its source partition, prior usage and
unknown-usage holds; audit totals in the checkpoint never become budget
authority. It also retains its workspace lock, ambiguous-submission handling,
no-duplicate-result checks and paid-submission gates. No model results, sealed
requests, producer implementations, manifests or budget limits are rewritten.
Checkpoint preparation and `check` are offline and may run alongside the active
campaign; `run` must not be started as a second controller for the same workspace.

Installing the checkpoint launcher for a future service start does not require
restarting the running campaign or changing its pinned modules. This does not
enable automatic retries, automatic service restart, boot-time startup, Sonnet,
diarization or publication.

## Running and monitoring

The selection builder is offline and records completion-receipt and original-media
deduplication evidence. Review its selection before sealing a campaign manifest.
The manifest pins source specifications, configuration, budget and implementation
hashes. Keep it outside the new private campaign workspace.

```sh
python3 -B -m pipeline.transcript_summary_campaign \
  --selection /absolute/private/selection.json \
  --expected-sha256 SELECTION_SHA256 \
  --output /absolute/private/campaign.json \
  --state-root /absolute/private/new-campaign \
  --budget-microusd 119992915
```

For a quality-review pause before fan-out:

```sh
python3 -B -m pipeline.transcript_summary_campaign \
  --manifest /absolute/private/campaign.json \
  --expected-sha256 MANIFEST_SHA256 --allow-paid-api --canary-only
```

After reviewing the exported canary, invoke the same command without
`--canary-only`. Omitting this flag retains the original automatic structural
canary gate and does not pause for quality review.

The controller is suitable for an explicitly launched user service with no
automatic restart. Its workspace `status.json` reports the selected count,
completed whole-transcript summaries, pending waves, review failures, estimated
usage and outstanding holds. Error status marks cached counts as potentially
stale. The runtime is finite (at most fourteen days per explicit invocation).

Completed shards receive both canonical evidence-rich exports and private reader
exports. Reader summaries have no visible citations; canonical internal evidence
links are retained for review and later synthesis. Reader exports are completed
idempotently on resume if interrupted after collection. The manifest does not
automatically schedule new discoveries or a Sonnet follow-up campaign.

To resume, use the same manifest and matching implementation. Do not edit a
sealed manifest or replace an ambiguous submission to bypass an error. See
[the base pipeline's reconciliation instructions](TRANSCRIPT_SUMMARIZATION.md#interruptions-retries-and-private-export).
