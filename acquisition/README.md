# Guarded media acquisition

Completed immutable media may be copied from managed hot storage to a distinct
cold-storage filesystem only through the bounded, checksum-verified contract in
[COLD_STORAGE_TRANSFER.md](COLD_STORAGE_TRANSFER.md). That boundary keeps downloads,
processing, indexes, mutable state, and durable receipts on the main filesystem; it
grants no deletion, catalogue-import, or publication authority.

One exact completed public acquisition can be adapted to that sealed-source contract
without chmodding or deleting its producer CAS object. See
[PUBLIC_ACQUISITION_RETENTION.md](PUBLIC_ACQUISITION_RETENTION.md) for the reviewed
work-order/result/payload digest binding, separate content-addressed hot staging
object, fixed `/mnt/archive/HIMR` destination, and operator-console action.

Public subreddit discovery is a separate, metadata-only input lane. See
[REDDIT_RSS.md](REDDIT_RSS.md) for immutable no-cookie Atom snapshots, minimized
post/media locators, optional public yt-dlp duration metadata, and the strict private
catalog importer. It does not download Reddit media or authorize publication.

Explicit public `v.redd.it` clip acquisition has a separate fail-closed boundary. See
[REDDIT_VIDEO_ACQUISITION.md](REDDIT_VIDEO_ACQUISITION.md) for snapshot-bound human
selection, duration-gated deterministic planning, canonical-URL and executable pins,
immutable provenance-preserving work orders, and the representative no-auth pilot.

Public Archive.org item metadata has its own strict snapshot/delta lane. See
[ARCHIVE_ORG_METADATA.md](ARCHIVE_ORG_METADATA.md) for exact unauthenticated
`/metadata/<identifier>` captures, deterministic validation, and the private catalog
handoff. It sends no cookies or authorization, downloads no media, grants no identity
or publication authority, and keeps every capture under ignored `research/` paths.

Sealed public queue downloads can run independently of CPU/GPU processing through the
finite, backpressured foreground worker in
[BACKGROUND_PRODUCER.md](BACKGROUND_PRODUCER.md). It preserves queue ordinals and the
existing one-writer acquisition boundary, uses no credentials, and treats exact
acquisition/preprocess receipts as its only restart state.

Large Archive.org plans can be partitioned offline through
[CAMPAIGN_EPOCHS.md](CAMPAIGN_EPOCHS.md). Once the two reviewed autonomous campaign
roles are sealed, [CAMPAIGN_SCHEDULE_SET.md](CAMPAIGN_SCHEDULE_SET.md) describes the
exact no-network materializer that creates one cold-primary producer schedule per
epoch and proves the complete role-ordered schedule union.

This directory provides a small, dependency-free admission boundary for the private
HIMR media corpus. It can copy an existing local file, download one explicit public
HTTP object (including an Archive.org object URL), or invoke a caller-supplied
`yt-dlp` executable for one explicit public YouTube video or one snapshot-bound
canonical `v.redd.it` clip. A file becomes canonical only after it has been staged,
bounded, hashed, probed, and admitted atomically.

The program does not search, crawl, publish, transcribe, or alter source media. Its
network adapters do not accept cookies, tokens, usernames, passwords, authorization
headers, browser profiles, or private/member-only sources. The local-file adapter may
standardize an already supplied restricted-source copy, but it performs no network
access and an explicit private handling policy can bind stronger publication limits.
It never derives a media URL from a Reddit post and never crawls Reddit comments. A
canonical `v.redd.it` locator uses only the
snapshot-bound, runtime-pinned `yt_dlp` boundary documented above; neither it nor a
Reddit `/comments/` discussion URL is accepted by the generic direct-HTTP path.

For Reddit, a launcher-file hash alone is not sufficient when the executable is a
Python console script. Reddit work orders additionally seal the exact expected
`yt-dlp --version`; script launchers must also seal the full imported `yt_dlp` module
tree hash. The version and runtime tree are rechecked before and after acquisition.

## Requirements

- Python 3.10 or newer on a POSIX host with advisory `flock`, using only the standard
  library
- `ffprobe` on `PATH`
- `yt-dlp` only for that adapter, supplied as an absolute executable path
- an explicit durable output root outside `/tmp` and `/var/tmp`

No model, package, executable, media file, or network credential is downloaded during
setup. Raw media and cache paths are ignored by Git.

## Work orders

Generate a local-file work order:

```sh
acquisition/bin/acquire create-work-order \
  --job-id pilot-local-001 \
  --adapter local_file \
  --platform local \
  --source-kind video \
  --native-id pilot-local-001 \
  --local-path /srv/himr-media/incoming/video.mp4 \
  --output-root /srv/himr-media/acquired \
  > /srv/himr-media/work-orders/pilot-local-001.json
```

For a contributor-supplied local copy that must remain in the private cache, bind the
handling instruction into the work order rather than relying on a filename or operator
memory:

```sh
acquisition/bin/acquire create-work-order \
  --job-id private-local-001 \
  --adapter local_file \
  --platform youtube \
  --source-kind youtube_video \
  --native-id VIDEO_ID \
  --canonical-url 'https://www.youtube.com/watch?v=VIDEO_ID' \
  --access-state unknown \
  --local-path /srv/himr-media/incoming/video.mp4 \
  --publication-disposition never_publish \
  --handling-basis 'Explicit contributor instruction: do not publish this copy.' \
  --output-root /srv/himr-media/private-acquired
```

Local-file possession is not public-access evidence: `local_file` work orders always
declare `source.access_state=unknown` (and the CLI uses that default). Network adapters
remain public-only. `handling_policy` is optional for compatibility with existing
version-1 orders. When present, it is exact and fail-closed: storage scope is
`private_canonical_cache`, publication authority is `none`, and disposition is either
`no_publication_authority` or the stronger `never_publish`. The completed envelope
copies it both top-level and into `catalog_records.sources[].metadata_json`; the two
copies and the originating work order must agree exactly. `no_publication_authority`
means this acquisition grants no publishing permission; `never_publish` records an
explicit prohibition for the supplied copy. Neither value is publication authority.

For a direct public object, add `--url` and use `--adapter direct_http`. For a public
YouTube page, use `--adapter yt_dlp`, add `--url`, and pass
`--yt-dlp-executable /absolute/path/to/yt-dlp`. New manually created orders should also
pass `--yt-dlp-sha256 <lowercase-sha256>`; queue materialization requires and always
emits that pin. The executable is treated as a trusted runtime dependency: its SHA-256
and file metadata are captured before execution and checked again afterward.
`--ignore-config`, a fresh empty home directory, and explicit no-sidecar flags prevent
ambient yt-dlp configuration from adding cookies, comments, subtitles, thumbnails, or
metadata files.
The format selector is limited to the two documented generic profiles or one exact
numeric video-plus-audio pair such as `396+140`. The exact-pair form exists for
hash-bound comparison and reacquisition: it does not accept named selectors, filters,
fallbacks, or other yt-dlp selector expressions.
For ordinary YouTube inputs, the adapter asks yt-dlp to print only the fixed allowlist
of retained fields after the completed file is moved. This avoids copying
fragment-by-fragment internal metadata into the bounded diagnostic channel on long
livestreams. A YouTube URL must carry `source.platform=youtube`; this prevents a
caller-supplied platform label from bypassing the bounded route. Retained strings and
numbers have fixed implementation bounds, and the adapter requires exactly one valid
metadata object. The returned native ID must still equal the requested source ID before
admission. Reddit keeps its separate full extraction record because its stricter
delivery-URL provenance check needs fields outside that allowlist.

## Completed selective-torrent files

[`torrent-completed-handoff`](bin/torrent-completed-handoff) emits a canonical,
non-admitting work order for one explicitly selected torrent media file only after
replaying the sealed plan and selector, validating the saved aria2 session and
control state, proving every covering piece complete across stable before/after
reads, and pinning the exact path without following links. It hashes and ffprobes
only that selected descriptor; an unselected boundary artifact is rejected before
any payload read. The command never contacts the network, invokes or stops aria2,
mutates the torrent state or catalogue, copies media, or grants publication
authority.

See [Completed selected torrent-file handoff](TORRENT_COMPLETED_HANDOFF.md) for the
contract, current-bundle command shape, later-import boundary, and focused tests.

The generated order includes these conservative current recommendations:

- 10 GiB maximum bytes for one job;
- 50 GiB maximum managed hot-cache size; and
- 80 GiB minimum free space remaining on the target filesystem.

They are policy inputs, not hidden globals. Set smaller per-job bounds when the source
is known, and change the cache/floor values only for a deliberately provisioned volume.
Supplying `--expected-sha256` and `--expected-byte-count` turns upstream inventory
facts into admission checks.

The normative shape is
[`schemas/work-order.schema.json`](schemas/work-order.schema.json), with a local-file
example at [`examples/work-order.local.example.json`](examples/work-order.local.example.json).
Runtime validation is authoritative and rejects unknown fields.

## Backlog planning

`plan-queue` turns the private catalogue into a deterministic, read-only acquisition
snapshot before any work orders are materialized. It considers one supported source
per unacquired recording, reconstructs canonical Archive.org and YouTube URLs from
native identifiers, and includes only sources whose current catalogue access state is
`public`. A members-only, private, unavailable, removed, or unknown source is counted
as withheld but never appears as a candidate URL.

```sh
acquisition/bin/plan-queue \
  --db /srv/himr-private/catalog.sqlite3 \
  --wiki-root /srv/himr-repository/src/content/docs \
  --selection-manifest /srv/himr-private/evaluation-selection.json \
  --planned-at 2026-08-26T20:00:00Z \
  --max-items 100 \
  --plan-budget-bytes 21474836480 \
  > /srv/himr-private/queue-plan.json
```

The timestamp is explicit so identical catalogue, wiki, selection, and policy inputs
produce identical bytes and the same `plan_id`. Wiki citations affect acquisition
priority only; they do not establish authenticity or publication rights. An optional
selection manifest follows
[`schemas/queue-selection.schema.json`](schemas/queue-selection.schema.json). The
result follows [`schemas/queue-plan.schema.json`](schemas/queue-plan.schema.json) and
records the migration ledger, a content digest of the query basis, conservative
YouTube size estimates, exact Archive.org provider byte counts when available, and an
explicit reason for every deferral.

The evaluation package can project its strictly validated unfrozen candidate cohort
directly into this contract without a temporary file:

```sh
python3 -m evaluation emit-acquisition-selection \
  evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  | acquisition/bin/plan-queue \
      --db /srv/himr-private/catalog.sqlite3 \
      --wiki-root /srv/himr-repository/src/content/docs \
      --selection-manifest - \
      --selection-only \
      --planned-at 2026-08-26T20:00:00Z
```

`--selection-only` keeps the full backlog visible but queues only identifiers in the
selection projection; all other ready candidates receive the explicit
`outside_explicit_selection` deferral reason.

Recordings longer than the configured single-job threshold, recordings likely to
exceed the byte cap, and YouTube sources without duration metadata are routed to
`requires_chunking` or `requires_metadata`; the planner does not silently weaken the
guarded acquisition limits. It performs no network access, writes nothing, accepts no
credential field, and grants no publication authority.

### Explicit long-recording boundary

`requires_chunking` remains ineligible for `materialize-queue`. When exact local
windows require one immutable parent object, an operator may instead materialize one
explicit candidate through the separate `materialize-long-recording` boundary:

```sh
acquisition/bin/materialize-long-recording \
  --plan /srv/himr-private/evaluation-queue-plan.json \
  --candidate-id 8AbFGYob9SU \
  --bundle-root /srv/himr-private/long-acquisition-bundles \
  --media-output-root /srv/himr-private/media-cache \
  --yt-dlp-executable /opt/himr-tools/yt-dlp \
  --yt-dlp-sha256 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --full-source-max-bytes 6442450944 \
  --global-cache-cap-bytes 32212254720 \
  --free-space-floor-bytes 107374182400
```

This does not reclassify the candidate, assign it a queue ordinal, or weaken the
normal materializer. It revalidates the complete queue plan, requires exactly one
public `requires_chunking` match, refuses a cap below the plan's conservative estimate,
hash-pins `yt-dlp`, and seals one full-source work order plus a manifest following
[`schemas/long-recording-bundle-manifest.schema.json`](schemas/long-recording-bundle-manifest.schema.json).
It performs no network access itself.

Remote `yt-dlp --download-sections` is not used. Separate remote interval requests
cannot prove that every interval came from the same immutable parent bytes, and the
yt-dlp project documents that cuts are not exact without re-encoding. Full acquisition
therefore precedes the offline, hash-bound local-window stage documented in
[`../pipeline/LOCAL_WINDOWS.md`](../pipeline/LOCAL_WINDOWS.md).

Provider bot checks, rate limits, or transient public-access failures are retryable
external availability outcomes. They must produce no admission or catalog handoff and
must never cause cookies, tokens, browser extraction, alternate authentication, or a
remote-section fallback to be added. Retry the same immutable work order only after
ordinary unauthenticated public access recovers.

## Immutable queue materialization

`materialize-queue` is the fail-closed boundary between a plan and executable work
orders. Keep its bundle root private and outside the public repository. It accepts an
absolute plan path or `-` for stdin, plus explicit storage policy and an absolute
`yt-dlp` executable with a caller-calculated SHA-256 pin:

```sh
acquisition/bin/materialize-queue \
  --plan /srv/himr-private/queue-plan.json \
  --bundle-root /srv/himr-private/acquisition-bundles \
  --media-output-root /srv/himr-private/media-cache \
  --yt-dlp-executable /opt/himr-tools/yt-dlp \
  --yt-dlp-sha256 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --global-cache-cap-bytes 53687091200 \
  --free-space-floor-bytes 85899345920 \
  > /srv/himr-private/latest-bundle-manifest.json
```

The command does not trust a plan merely because it satisfies JSON Schema. It rejects
duplicate JSON keys and unknown fields, recalculates the canonical plan-core digest and
`plan_id`, reconstructs every YouTube and Archive.org URL from the native identifier,
and recomputes candidate priority, reason codes, byte estimates, queue state, queue
ordinals, deferrals, summary counts, item cap, and byte budget. The only materialized
candidates are the contiguous `queue_ordinal` entries that independently recompute to
`ready`. `requires_metadata`, `requires_chunking`, `review_required`, budget-deferred,
item-deferred, and selection-deferred candidates never become work orders.

The executable is hashed between stable file-identity observations while the bundle is
created. Each generated YouTube order carries that digest as
`adapter_config.expected_executable_sha256`; guarded acquisition checks it before the
first invocation and still checks that the executable did not change during the run.
This field is optional in the version-1 work-order contract so previously issued v1
orders retain their canonical bytes. The materializer always emits it.

The immutable layout is:

```text
<bundle-root>/
├── .queue-materializer.lock
└── bundles/acqbundle_<content-derived-id>/
    ├── manifest.json
    └── work-orders/
        ├── 000001.json
        └── ...
```

The manifest follows
[`schemas/queue-bundle-manifest.schema.json`](schemas/queue-bundle-manifest.schema.json),
hashes and sizes every exact work-order file, records the canonical source-plan digest
and all materialization policy, and states `publication_authority: none`. A non-waiting
OS lock serializes admission. The complete staged tree is renamed into place and sealed
owner-readable and read-only (files `0400`, directories `0500`); replay returns the same
manifest only after verifying the exact entry set, bytes, hashes, and private read-only
modes. Any pre-existing partial, writable, over-broad, missing, extra, or changed entry
fails closed. Empty valid plans produce an immutable zero-order bundle.

A narrow queue runner is available for these sealed bundles. See
[`QUEUE_RUNNER.md`](QUEUE_RUNNER.md) for its offline validation, exact ordinal replay,
strict completed-result reuse, mandatory per-run item/byte/time/free-space bounds,
fail-fast resumption, and canonical summary contract. It dispatches only through the
existing one-order `acquire.run_acquisition` boundary with maximum concurrency one; it
has no credential, fallback, arbitrary-subset, catalog, import, identity, event,
publication, export, or deletion interface. Materialization itself still never uses
the network, invokes `yt-dlp`, opens SQLite, imports results, or grants review or
publication approval.

Offline-validate one immutable bundle before allowing acquisition:

```sh
acquisition/bin/queue-runner validate \
  --manifest /srv/himr-private/queue/bundles/acqbundle_ID/manifest.json
```

Validate an order without acquiring anything:

```sh
acquisition/bin/acquire validate \
  --work-order /srv/himr-media/work-orders/pilot-local-001.json
```

## Dry runs

```sh
acquisition/bin/acquire run \
  --work-order /srv/himr-media/work-orders/pilot-local-001.json \
  --dry-run
```

All dry runs are network-free and create no output directory. A local-file dry run can
hash and probe the existing file, so it reports the exact content-addressed destination.
Remote dry runs report the guarded command and reservation but cannot know a hash or
target object before bytes exist.

## Admission and resumption

An actual run follows this order:

1. Validate the complete order and reserve capacity against both policy limits.
2. Acquire a non-waiting OS writer lock for the output root, then stage bytes beneath
   it.
3. Check source observations, job size, optional expected size/hash, staged SHA-256,
   and normalized FFprobe metadata.
4. Hard-link the staged file into its canonical destination without overwriting an
   existing object, verify a pre-existing object before reuse, then remove staging.
5. Atomically write a durable result envelope.

The content layout is:

```text
<output-root>/
├── media/sha256/ab/<full-sha256>/payload
└── jobs/<job-id>/<work-order-sha256>/result.json
```

Staging is on the same managed filesystem so hard-link admission is atomic. Originals
are opened read-only. Local source device, inode, size, and nanosecond modification time
must match before and after copying. The yt-dlp executable receives the equivalent
before/after check. HTTP results retain safe response validators and range metadata;
those headers are evidence, not proof of remote identity.

Only one actual acquisition may write an output root at a time. The durable
`.acquisition-writer.lock` file contains bounded diagnostic metadata, while the OS lock
itself is authoritative. Contention fails immediately and reports the holder metadata.
If a process crashes, the kernel releases its lock; the next writer records that it
recovered stale on-disk state and continues safely. Dry runs take no lock because they
write nothing. External job runners should still serialize work per output root, both
for predictable throughput and as a fallback on filesystems whose advisory-lock
semantics have not been verified.

Direct HTTP can resume only a partial created for the exact same URL. It sends `Range`
and, when available, `If-Range` using the saved ETag or Last-Modified value. A mismatched
or ignored range causes a safe restart instead of concatenation. Bodies are requested
with identity encoding and are stopped at the job byte cap. No proxy credentials,
CookieJar, arbitrary headers, or ambient authentication are inherited.

The same completed work order is idempotent. Reuse opens `result.json` and the exact
hash-derived payload with no-follow descriptors rooted through every managed directory
component, rejects links and path substitutions, bounds and strictly decodes the JSON,
and repeats identity and byte-hash checks immediately before returning. It also matches
the complete result shape, normalized work order, source, limits, canonical admission
path/URI, observations, commands, probe relationships, and a freshly reconstructed
catalog handoff before reapplying the YouTube or Reddit selected-identity check.
Different sources with identical bytes still converge on one verified media object.

This is local integrity and consistency checking, not a signature. A process with the
same filesystem authority could replace both media and every internally consistent
envelope field after the final descriptor check; keep the cache access-controlled and
use the one-writer lock cooperatively. Historical timing and capacity observations also
cannot be independently recovered after acquisition, although their types and internal
arithmetic are checked.

The yt-dlp child runs in a separate process group. While it executes, the orchestrator
checks the complete staging tree (including partials, temporary files, caches, and
captured diagnostics) every 50 ms against the job byte cap, the reserved global-cache
budget, and the live free-space floor. A violation terminates the process group,
removes failed yt-dlp staging, and admits nothing. Diagnostic stdout/stderr are also
bounded to 8 MiB each. YouTube stdout contains only the fixed post-download metadata
projection; Reddit stdout retains the larger identity evidence required by its
snapshot-bound adapter. A successful child that emits malformed, missing, or multiple
metadata records has its completed staging tree removed and admits nothing. The host
supervisor should additionally impose a wall-clock
timeout; this layer intentionally does not guess one duration suitable for both short
clips and multi-hour streams.

## Result and catalog handoff

The stdout and durable `result.json` follow
[`schemas/result.schema.json`](schemas/result.schema.json). Commands are arrays rather
than shell strings and contain no accepted secret-bearing fields. Selected remote
metadata is allowlisted. Version 1 defines every envelope, observation, probe, and
catalog-row object with exact keys and typed values; unknown properties are rejected.

`catalog_records` is imported transactionally into the normalized `sources`,
`media_objects`, `media_locations`, and `media_sources` tables with
`himr-corpus import-acquisition-result`. Acquisition completion supplies
`media_objects.first_cataloged_at` and `media_sources.retrieved_at`; neither field is
presented as the source publication time. The strict catalog boundary remaps the
producer-local source ID and creates no publication decision. The orchestrator never
opens or mutates SQLite directly.
An explicit work-order `handling_policy` is retained in the catalog source observation
and in migration 0030's append-only `acquisition_handling_restrictions` ledger. Policy-
bearing imports require a replayable private-artifact seal receipt and reject a missing,
dropped, changed, or weaker policy. The effective restriction propagates from source
and parent media to derived media, recordings, and transcript revisions. New publish
or gate-clear decisions are blocked, publication eligibility excludes the restricted
objects, and validation/export fail closed if a restricted object has conflicting
current publish state. Migration 0030 is additive and must be reviewed/applied through
the normal catalog migration workflow; acquisition itself never opens SQLite.

The review-only seal helper does not write files or change permissions. `plan` opens
the work order, completed result, and admitted media beneath one explicit root without
following symlinks; it prints a portable-path plan binding their exact bytes, media and
source identity, and handling policy while explicitly declining any source-byte-
identity claim. After an operator separately reviews the plan and establishes mode
`0700` on the listed directories and `0600` on the listed files, `validate` reopens and
rehashes them and prints a receipt:

```sh
umask 077

PYTHONPATH=corpus/src python -m himr_corpus.private_acquisition plan \
  --artifact-root /srv/himr-private/acquisition-job \
  --work-order work-order.json \
  --result admitted/result.json \
  --media admitted/media.mp4 > /srv/himr-private/review/seal-plan.json

PYTHONPATH=corpus/src python -m himr_corpus.private_acquisition validate \
  --artifact-root /srv/himr-private/acquisition-job \
  --plan /srv/himr-private/review/seal-plan.json \
  > /srv/himr-private/review/seal-receipt.json

PYTHONPATH=corpus/src python -m himr_corpus import-acquisition-result \
  --db research/corpus/corpus.sqlite3 \
  --result /srv/himr-private/acquisition-job/admitted/result.json \
  --private-artifact-root /srv/himr-private/acquisition-job \
  --private-seal-receipt /srv/himr-private/review/seal-receipt.json
```

The importer treats the receipt as replay evidence, not bearer authority: it validates
the current descriptors, paths, bytes, modes, work-order/result/media binding, source,
and policy again before admitting the restriction.

## Tests

```sh
python3 -m pip install -r scripts/requirements-json-contracts.txt
python3 scripts/validate-json-contracts.py
acquisition/tests/run.sh
```

The standard-library tests create a short FFmpeg `lavfi` fixture under the explicit
repository-local `acquisition/.test-work/` root. They exercise local admission,
read-only dry-run, idempotent reuse, capacity failure, URL/output policy, an interrupted
and resumed local HTTP transfer, writer contention/stale-state recovery, a fake yt-dlp
executable, and live termination/cleanup of an oversized fake yt-dlp writer. They make
no real network request and delete every generated fixture and cache afterward. The
planned, completed, reused, HTTP, and yt-dlp envelopes are validated against the
result schema. The pinned `jsonschema` package is development/CI-only.
