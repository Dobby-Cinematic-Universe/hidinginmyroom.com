# ADR 0011: Publish a separately reviewed entity/event graph

- **Status:** Accepted
- **Date:** 2026-08-28

## Context

Migration 0004 contains a broad private entity/event research model. Migration 0005
projects only gated entity and event roots. Neither contract is sufficient for
publishing appearances, participants, dates, relations, or evidence edges: a public
root does not make its connected private edges safe, and a machine observation or
private alias is not publication authority.

The recording release schemas v1 and v2 are already strict public contracts. Adding a
graph to either format would broaden their consumers and make a graph rollout capable
of changing recording-release hashes. Graph intervals also originate in rendition
media coordinates, which must not be mistaken for source-player or recording time.

## Decision

- Migration 0031 is a forward-only projection layer. It does not change the v1/v2
  recording release or weaken the existing `public_entities` and `public_events`
  views.
- Every entity, event, appearance, event date, event participant, event relation, and
  event-evidence edge needs its own current `publish` decision, nonempty public label,
  complete matching human accept/correct review, and current rights, privacy, and
  sensitivity clears. Each gate clear also needs its own matching complete human
  review. The graph views enforce the complete rule for roots and edges. Database
  triggers enforce it at insertion for newly publishable edge types; pre-existing
  entity/event write streams remain compatible with non-graph public workflows and
  do not enter the graph unless the stricter view admits them.
- Appearance and event-evidence publication reviews must record direct audio or video
  perception. Their catalog anchor must resolve to a reviewed public source and
  recording, reviewed source/recording association, reviewed rendition, and verified
  media object. Timed anchors require a known duration and cannot exceed it.
- The public anchor includes the stable rendition ID and the literal time basis
  `rendition_media_ms`. Published milliseconds are never represented as platform-
  player, wall-clock, or ordinary recording coordinates.
- The historical event-participant table has no edge ID. A new append-only mapping
  binds each imported deterministic participant review target to its composite row;
  mapped rows become immutable.
- A released event must retain at least one independently eligible participant and
  one independently eligible evidence edge. Only entities referenced by a surviving
  participant or appearance are emitted. All references must resolve, dates must be
  real calendar values, and public event relations must be a directed acyclic graph.
- Graph schema v1 excludes transcript-backed evidence. This avoids making transcript
  publication or lifecycle state a hidden graph dependency and prevents transcript
  text, revision IDs, or machine-only wording from entering the format. A later
  schema would need to pin and account for those dependencies explicitly.
- The graph uses an isolated active manifest and one content-addressed shard below
  `src/data/corpus/graph/`. The manifest pins exact bytes and SHA-256, and its release
  ID commits to the complete graph. Export validates the temporary tree before
  atomically replacing the active manifest. Python and site loaders enforce exact
  fields, bounds, canonical hashes, references, calendar values, interval rules, and
  relation acyclicity.
- `generated_at` derives only from the current publication and gate decisions for
  emitted graph objects and their emitted public source/recording anchors. Suppressed
  or unrelated private work cannot advance it.
- The public format has an explicit allowlist. It contains labels, stable public IDs,
  slugs, narrow edge classifications, dates, intervals, and public catalog anchors.
  It never contains aliases, private classifications, descriptions, basis or notes,
  local paths, artifacts, observations, transcript text or IDs, biometric clusters,
  embeddings, crops, scores, or machine identity assertions.
- The checked initial fixture is empty. Tests construct only fictional synthetic
  objects; this decision does not authorize populating or publishing a real graph.

## Consequences

- A reviewer can clear one root or edge without accidentally clearing its neighbors.
  Withholding any one object or any one gate removes that object and dependents from
  the current deterministic projection.
- Graph deployment and rollback do not alter recording-release v1/v2 files or their
  loaders.
- Named appearance edges are intentionally expensive: they require direct human
  perception plus independent publication and gate decisions and public catalog
  anchors.
- Transcript-backed research remains available privately but cannot appear in graph
  v1. This is a deliberate omission, not an inference that media-only evidence is
  true or editorially adopted.
