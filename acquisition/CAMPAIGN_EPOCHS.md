# Offline Archive campaign epochs

`materialize-campaign-epochs` converts one large, sealed Archive.org queue plan into
small immutable acquisition bundles suitable for a multi-day controller. It is a
control-plane operation only: it performs no network request, media read/write,
FFmpeg/GPU work, cold-storage copy, catalogue mutation, deletion, or publication.

The parent must be an already-normalized absolute path to a mode-`0400`, canonical
pretty-JSON queue-plan-v1 file. The caller supplies its exact file SHA-256. The tool
replays the full existing queue-plan validator before accepting any selected row and
rejects a selected source that is not an Internet Archive media file.

## Deterministic partition

Selected ready candidates are read in their sealed parent queue-ordinal order. The
stable greedy `stable_greedy_parent_queue_order_v1` partition fills the current epoch
until either adding the next item would exceed the byte budget or the item limit has
already been reached. It never reorders or splits a recording. A single candidate
larger than the byte budget fails closed; raise the explicit epoch byte budget only
after reviewing that candidate and the corresponding job/cache limits.

Defaults are 32 items and 16 GiB of provider-estimated bytes per epoch. Both limits
are explicit in the campaign manifest. Every compact epoch is itself a fully valid
queue-plan-v1 document with:

- only that epoch's candidates;
- local queue ordinals `1..N`;
- only the explicit selectors necessary to reconstruct the retained priority reason
  codes, rather than the parent's potentially thousands-entry selection list; and
- its own content-derived `acqplan_...` ID.

Each epoch plan is passed to the existing guarded queue materializer. Thus work-order
shape, source URL reconstruction, executable pinning, public-only policy, immutable
bundle admission, and output-root binding are unchanged. All epoch bundles share the
one explicit `--media-output-root`; the epoch tool never creates or writes that media
root.

## Materialize controls

Use an owner-only control location on the main drive. The parent of a new campaign
root must already exist. The `yt-dlp` pin remains required by the common bundle
contract even for an Archive-only campaign; it is inspected but never executed.

```sh
umask 077
mkdir -p /srv/himr-private/archive-campaigns
chmod 0700 /srv/himr-private/archive-campaigns

acquisition/bin/materialize-campaign-epochs \
  --parent-plan /srv/himr-private/archive-all-known/queue-plan.json \
  --expected-parent-plan-sha256 PARENT_PLAN_SHA256 \
  --campaign-root /srv/himr-private/archive-campaigns/all-known-v1 \
  --media-output-root /srv/himr-private/archive-media \
  --yt-dlp-executable /srv/himr-private/tools/yt-dlp \
  --yt-dlp-sha256 YT_DLP_SHA256 \
  --global-cache-cap-bytes 68719476736 \
  --free-space-floor-bytes 68719476736 \
  --max-epoch-items 32 \
  --max-epoch-estimated-bytes 17179869184
```

The command writes a small JSON receipt to stdout. Its
`campaign_manifest_path` identifies the immutable mode-`0400` campaign manifest and
`campaign_manifest_sha256` binds its exact bytes. Admission is idempotent: replaying
identical inputs returns the same IDs and paths, while a modified existing control
file fails closed.

The control tree is:

```text
CAMPAIGN_ROOT/
  campaigns/acqcampaign_ID/manifest.json
  epoch-plans/acqplan_ID/queue-plan.json
  bundles/acqbundle_ID/
    manifest.json
    work-orders/000001.json ...
```

The top-level manifest records exact absolute paths, SHA-256 digests, byte counts, and
IDs for the sealed parent, every epoch plan, and every bundle manifest. Its coverage
proof binds an ordered member digest over parent ordinal, recording/source/native
identity, and estimated bytes. The same digest is independently produced from the
epoch union. It also records zero overlap, missing, and unexpected counts, exact union
and unique counts, and contiguous parent/local ordinal assertions. Materialization
aborts before campaign-manifest admission unless the ordered epoch union is byte-for-
byte identical to the selected parent member projection.

The structural contract is
[`schemas/campaign-epoch-manifest.schema.json`](schemas/campaign-epoch-manifest.schema.json).
Runtime coverage and digest replay remain authoritative for relationships that JSON
Schema cannot express.

This command deliberately does not create background-producer schedules. After both
the normal-processing and cold-acquisition-only campaign manifests have been reviewed,
the separate offline
[`materialize-campaign-schedule-set`](CAMPAIGN_SCHEDULE_SET.md) boundary can replay
their exact SHA-256-pinned epochs and seal the fixed autonomous-controller schedule
set. The epoch command itself still chooses no runtime role or backpressure policy.

## Focused offline tests

```sh
python3 -B -m unittest acquisition.tests.test_materialize_campaign_epochs -v
```

The suite uses tiny fixtures beneath `acquisition/.test-work`, a fake executable that
would leave a marker if invoked, and no network, GPU, or cold-storage operation. It
covers item/byte partition boundaries, compact explicit and non-explicit epoch plans,
coverage proof, exact replay, immutable-tamper refusal, parent digest/mode checks,
Archive-only enforcement, and oversized-candidate refusal.
