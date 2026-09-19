# Corpus architecture

```text
dated source snapshots and discovery manifests
                  |
                  v
       metadata-only import adapters
                  |
                  v
 private checksummed SQLite catalog -- private media/artifact store
                  |
          review + publication decisions
                  |
                  v
 deterministic static JSON and independent transcript search
```

Migrations are forward-only and checksummed in `schema_migrations`. Changing an
applied migration is an error; add a new migration instead.

Exact input identity, observation time, and current metadata are separate layers.
Candidate assertions are append-only and the current source, recording, relation, and
external-ID projections follow the documented deterministic
[metadata precedence policy](METADATA_PRECEDENCE.md), never importer call order.

The automated public metadata policy has a closed scope: media-file metadata beneath
Archive.org items `69999`, `699992`, and `28766`, plus public videos created by the
reviewed official-channel inventory for `UC_yIF-9jOge6nNA0z-ScrBQ`. A public access
state is required. Arbitrary Archive.org imports, validated third-party/search videos,
legacy catalog rows, URL hints, torrent paths, raw artifacts, and biometric data
remain withheld. Transcript revisions use a separate explicit publication decision
and independent current rights, privacy, and sensitivity gates. They do not require a
wording review: `machine` revisions are eligible and export their generated,
unreviewed, and non-quotation status. Rendition-local and media-local hypotheses still
require a reviewed ordinary-coordinate projection before they can enter that lane.
Migration 0029's review is narrowly an exact media-lineage and coordinate attestation;
it never supplies wording, identity, preference, or publication authority. Its
public-at-review basis is an append-only acquisition observation, so later changes to
the mutable current source-availability projection do not rewrite or invalidate the
sealed historical receipt.

The static release contains no runtime database. Schema v2 publishes a small active
manifest, bounded transcript-free catalog shards, and content-addressed full
recording/transcript shards. Every descriptor carries a byte count and SHA-256; the
manifest release ID commits transitively to the complete tree. Export installs the
immutable release directory and validates it before atomically replacing the active
manifest. V1 remains a strict read-only migration fallback.

Migration 0031 adds a separate deny-by-default graph projection and static hash tree;
it does not alter recording schemas v1/v2. Every graph root and edge requires an
independent complete human review, public label, publish decision, and three current
human-reviewed gate clears. Appearance and event-evidence edges also require direct
media perception and a reviewed public source/recording/rendition anchor. Their
milliseconds are explicitly `rendition_media_ms`, with known-duration bounds for
timed edges. The graph v1 exporter omits aliases, private provenance, observations,
transcripts, and biometric or machine-identity state, rejects calendar-invalid dates
and cyclic relations, and suppresses orphan roots. Transcript-backed graph evidence
is intentionally unsupported in v1. See
[ADR 0011](../../docs/adr/0011-public-entity-event-graph-release.md).

Release time is the latest relevant decision time among objects projected into that
release, including their current publication gates and transcript lifecycle history.
Private-only and currently suppressed objects cannot advance the public timestamp.

The site generates bounded corpus Pagefind bundles beneath `/corpus/pagefind/` and
declares them in `/corpus/search-manifest.json`; corpus pages remain excluded from the
wiki's main search index.

Every non-retracted eligible transcript revision remains searchable. The release does
not rank one revision as preferred or hide competing machine and human histories.
Transcript evidence and review records are append-only. A dispute, retraction, or
reinstatement is a separate human-only lifecycle decision with a public explanation;
a retraction immediately replaces current text with a gated, text-free tombstone. A
retracted revision can return only through reinstatement, and a transcript publication
`remove` requires that current retraction.

## Private identity-analysis boundary

Face, voice, and audiovisual matching is private analytical metadata, not evidence of
a person's identity by itself. Cluster versions and memberships are append-only,
model/run lineage is frozen, and explicit cannot-link decisions fail closed against
co-membership. Raw embeddings and enrollment material must be registered as private
biometric artifacts stored outside the repository and public build paths.

A public identity mapping is a separate object. It requires direct-media human review
of both the accepted current cluster version and the identity assertion, plus the
normal publication decision and all three gates. Even then, only the narrow
`public_identity_assertions` view is eligible; raw clusters, observations, scores,
artifacts, and review notes have no public projection. The static graph consumes
independently reviewed entity roots and named appearance edges, not identity-cluster
or identity-assertion objects, so identity-analysis output remains absent from the
supported release workflows.

Migration `0032` adds a separate non-biometric private bridge for a directly heard
solo voice. It freezes the exact source/recording/rendition/media SHA-256 and
`rendition_media_ms` interval, then requires independent human privacy review and a
second human's complete-interval direct-audio attestation. Playback, overlap, TTS,
synthetic/unknown origin, source or channel identity shortcuts, transcript wording,
machine identity output, and machine confidence all fail closed. Its append-only
current view remains private and is not consumed by either static exporter. See
[ADR 0012](../../docs/adr/0012-private-solo-voice-attestations.md).
