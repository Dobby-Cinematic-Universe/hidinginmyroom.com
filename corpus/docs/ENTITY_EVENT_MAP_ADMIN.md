# Private entity/event map manifests

Entity and event research enters the working catalog through a strict, source-
anchored administrative manifest. This path is private by construction. It creates
review candidates; it does not decide identity, truth, editorial inclusion, or
publication.

Keep filled manifests and any referenced evidence under an ignored private path such
as `research/corpus/entity-event-manifests/`. A filled manifest may contain private
aliases, handles, descriptions, and local paths and must not be committed. The only
tracked example is deliberately fictional:
[`../examples/entity-event-map-manifest.fictional.example.json`](../examples/entity-event-map-manifest.fictional.example.json).
The versioned contract is
[`../schemas/entity-event-map-manifest.schema.json`](../schemas/entity-event-map-manifest.schema.json).

## Wiki seed inventory

Before writing semantic entity or event rows, generate a deterministic inventory of
the existing character and event pages:

```sh
node scripts/build-wiki-graph-seed-inventory.mjs \
  --output research/corpus/graph-seed-inventories/wiki-seeds.json
```

The inventory records page titles and hashes plus the structural `SourceCitation`
IDs, claim IDs, URLs, review-state labels, and ordinals already present in the wiki.
It deliberately copies no claim prose and grants no semantic-mapping, identity,
event-truth, catalog-import, or publication authority. Its purpose is coverage
planning: a reviewer can see which pages and citations still need an explicit private
manifest without treating a page title or citation card as a graph assertion.

The command reads only `characters/*.mdx` and `events/*.mdx`, writes only beneath the
ignored owner-private `research/corpus/graph-seed-inventories/` root, refuses to
replace different existing bytes, and returns the inventory ID, exact hash, byte
count, and coverage counts. Generate a new output name after any wiki change rather
than overwriting an earlier inventory.

Resolve the structural citations against an exact sealed catalog with a separate
owner-private crosswalk:

```sh
python3 scripts/build_wiki_catalog_anchor_inventory.py \
  --inventory research/corpus/graph-seed-inventories/wiki-seeds.json \
  --db /absolute/private/sealed-corpus.sqlite3 \
  --output research/corpus/wiki-catalog-anchor-inventories/wiki-anchors.json
```

Resolve that database path to the exact sidecar-free catalogue checkpoint selected
for the review. Do not substitute an older convenience, fixture, or legacy database
merely because its filename is similar; the inventory pins and reports the catalogue
SHA-256 and schema version.

This second inventory parses direct YouTube, Archive.org, Reddit-post, and
`v.redd.it` locators without making network requests. It uses accepted exact external
IDs before lower-tier candidate IDs, then enumerates every current
source/recording/rendition lineage instead of silently choosing among multiple
renditions. Unmatched locators, unsupported links, absent renditions, rejected or
merged rows, and ambiguous anchor sets remain explicit coverage states.

The crosswalk is still only a planning aid. A matching URL or catalog row does not
establish what the media says, who appears, whether an event happened, or whether a
claim may be linked, imported, or published. Its output records all those authority
flags as false and stays beneath the ignored owner-private research tree.

## Safe workflow

Validate the exact manifest bytes, every optional local file, and all current catalog
references before importing:

```sh
PYTHONPATH=corpus/src python -m himr_corpus validate-entity-event-map-manifest \
  --db /absolute/private/sealed-corpus.sqlite3 \
  --manifest research/corpus/entity-event-manifests/2026-08-26-map.json
```

Then import the same bytes:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-entity-event-map-manifest \
  --db /absolute/private/operator-copy.sqlite3 \
  --manifest research/corpus/entity-event-manifests/2026-08-26-map.json
```

Validation may use the sealed audit checkpoint. Import must target the separately
backed-up, operator-approved writable catalogue copy—not the sealed evidence file.

The importer rehashes the manifest and all declared local evidence again while it
holds one `BEGIN IMMEDIATE` transaction. A failure rolls back every map row, catalog
anchor, artifact, review task, and import-ledger row. Replaying the same manifest ID
and exact bytes verifies all rows and returns `idempotent_replay: true`; changing even
formatting under an imported manifest ID is rejected as a tampered replay. To add or
correct research, use a new manifest ID and new record IDs rather than rewriting an
old manifest. A completed replay also refuses to recreate a missing map, anchor,
artifact, or review-task row: an import ledger whose rows were deleted is treated as
catalog tampering and requires an explicit repair/audit rather than silent healing.
Likewise, a new manifest cannot claim a new row ID that already exists without its
ledger. When adding evidence to an existing entity or event, reference its ID from the
edge/participant collection and omit it from the manifest's `entities` or `events`
array; those arrays describe rows newly asserted by that manifest.

## Evidence semantics

Every appearance and event-evidence edge must name all three catalog layers:

- a current, non-rejected `source_id`;
- a current, non-merged `recording_id` linked to that source by a non-rejected
  `recording_sources` row; and
- a non-rejected `rendition_id` of that recording.

Timed edges use integer half-open intervals, `[start_ms, end_ms)`. The rendition's
exact media object must have a known duration and `end_ms` may not exceed it.
Appearances are always timed. Event evidence may be whole-item evidence by setting
both values to `null`; setting only one is invalid.

Version 1 times are explicitly `rendition_media_ms`, not assumed source-page,
wall-clock, or platform-player coordinates. The anchor and observation provenance
store that coordinate label. A future edit that changes the relevant rendition or
timeline mapping therefore needs a new edge rather than silently reusing the old
milliseconds.

The importer records a private `claim_catalog_links` anchor for every appearance and
event-evidence edge. A timed edge also receives a private `observations` row bound to
the exact rendition. These auxiliary rows preserve the source and rendition columns
that the older `appearances` and `event_evidence` tables do not both carry. They stay
in candidate/machine review state and are not public evidence by themselves.

`evidence_basis_kind` is separate from `support_kind`:

- `direct_media_observation` means the cited interval visibly or audibly contains
  the described occurrence;
- `subject_unverified_claim` means the source directly records the subject making a
  statement, not that the statement is true;
- `third_party_unverified_claim` applies the same distinction to another speaker;
- `documentary_context` records contextual material that does not directly depict
  the event.

Event evidence independently says whether it is `direct`, `contextual`,
`corroborating`, or `contradicting`. Thus `support_kind: direct` never turns a
subject's claim into verification of the claimed event. Contradicting evidence is
retained and receives its own review task.

When an edge cites a `transcript_revision_id`, the revision must belong to the same
recording/rendition and already be `human_corrected` or `media_checked`. Raw or
contextual machine ASR cannot serve as transcript-derived claim evidence. An edge
without a transcript revision remains anchored to the media and requires direct
media review.

## Privacy and graph admission

Entities declare a private research classification: `public_figure`,
`living_private_person`, `deceased_person`, `non_person`, or `unknown`. This is an
administrative routing label, not a publication clearance. Aliases require a current
source. A handle/username or an alias marked `sensitive` for a living private person
must set `privacy_review_required: true`; unknown privacy is treated the same way.
The alias remains private even when that flag is set, and a dedicated privacy review
task is created without copying the alias text into the task reason.

New events must have at least one participant and at least one source-anchored
evidence edge. Unknown entity/event IDs, orphan events, duplicate IDs or natural
keys, and relations that introduce a directed cycle are rejected. Relations describe
ordering or context only; their `basis` must not imply causality that the evidence
does not establish.

Each entity, alias, appearance, event, date, participant, relation, and evidence edge
gets an open review task. Unverified claims and sensitive aliases receive higher-
priority specialized tasks. Later review changes are separate from the immutable
manifest assertion, so an exact replay does not reset a completed or disputed task.

## Optional local evidence

`local_evidence` is either `null` or an exact private-file declaration:

```json
{
  "artifact_id": "artifact_private_source_capture_001",
  "path": "/absolute/resolved/private/path/capture.mp4",
  "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "byte_count": 12345
}
```

Paths must be absolute, resolved, regular files without symlink or traversal aliases.
The importer uses race-aware stat checks and streams SHA-256 before admission and
again inside the transaction. It registers only a `private`
`entity_event_source_evidence` artifact. Local evidence is an integrity anchor, not a
license or publication decision.

## Hard boundary

This workflow forces `private` visibility for entities, aliases, events,
observations, and artifacts. It creates no rows in identity-assertion tables,
publication decisions, or rights/privacy/sensitivity gate streams, and it verifies
those table counts are unchanged before commit. The `public_entities` and
`public_events` views therefore remain fail-closed. Any later public use requires
independent human review, publication intent, and all three publication gates through
the separate publication-admin workflow.

## Separate public graph release

Migration 0031 does not make an imported private map public. It adds narrow
`public_graph_*` views in which every entity, event, appearance, date, participant,
relation, and evidence edge must independently carry a complete matching human
accept/correct review, a nonempty public label, a current human `publish` decision,
and current rights, privacy, and sensitivity clears, each backed by a matching
complete human review. Appearance and event-evidence publication review must also
record direct audio or video perception. A participant's deterministic review target
is bound to its composite catalog row by the append-only
`event_participant_publication_subjects` mapping.

An appearance or evidence edge additionally resolves through one reviewed catalog
link to a reviewed public source and recording, a reviewed source/recording mapping,
a reviewed rendition, and verified media. Timed edges require known media duration.
The exported anchor includes the rendition ID and fixed `rendition_media_ms` time
basis so its milliseconds cannot be mistaken for source-player coordinates.

Graph schema v1 deliberately rejects transcript-backed evidence. A reviewed private
transcript citation therefore remains private; this prevents transcript publication
or lifecycle state from becoming an unpinned static dependency. The release also
omits aliases, descriptions, private classifications, provenance basis, observations,
paths, artifacts, transcript IDs or text, and all biometric or machine-identity data.

Export and validate the isolated, content-addressed graph without changing the
recording v1/v2 release:

```sh
PYTHONPATH=corpus/src python -m himr_corpus export-graph \
  --db research/corpus/corpus.sqlite3 \
  --out-dir src/data/corpus/graph

PYTHONPATH=corpus/src python -m himr_corpus validate-graph-release \
  --manifest src/data/corpus/graph/manifest.json
```

The checked graph fixture is intentionally empty. No private-map import, migration,
or release command authorizes real-person or real-event population by itself.
