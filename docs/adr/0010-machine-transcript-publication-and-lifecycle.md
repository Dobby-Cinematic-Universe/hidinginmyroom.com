# ADR 0010: Publish gated machine transcripts without silent revision selection

- **Status:** Accepted
- **Date:** 2026-08-27

## Context

The original static exporter exposed only `human_corrected` and `media_checked`
transcript revisions, then selected one preferred revision for each language. That
made a full historical backfill depend on a wording-review bottleneck and hid
competing revisions from ordinary search. It also had no public, auditable distinction
between correcting a machine transcript and retracting its text.

Machine transcripts are navigation aids. Publishing them must not imply exact wording,
speaker certainty, factual truth, or editorial adoption. At the same time, a silent
delete or an automated retraction would make the evidence history difficult to audit.
Rights, privacy, sensitivity, source provenance, coordinate projection, and identity
remain independent concerns.

## Decision

- An ordinary recording-coordinate revision in state `machine`, `human_corrected`,
  `media_checked`, or `disputed` may enter the public view after an explicit current
  publication decision and current `clear` decisions for rights, privacy, and
  sensitivity. A wording review is not a prerequisite.
- Migration `0028` supplies one closed automated path for the initial publication
  decision of `raw_asr` or `contextual_asr` machine revisions. It requires a reviewed
  deterministic plan digest and current human clear rows for all three independent
  gates. The plan contains coordinates, metadata, segment counts, and gate IDs but no
  transcript wording.
- Policy v1 excludes every revision with a prior publication decision, any lifecycle
  history, or any non-null segment speaker label. It therefore cannot override a human
  publish/withhold/remove stream or infer identity; named-speaker transcripts remain
  in a human-capable lane.
- The policy reviewer can append only its exact initial `publish` rows. It cannot
  decide gates, review or correct wording, make restrictive or later publication
  decisions, or dispute, retract, or reinstate a transcript. Those lifecycle actions
  remain human-only.
- A policy run fixes one application instant after revision creation and all three
  human gates. Its admitted segment set and content-bearing word children are sealed
  against later insertion or replacement; explicit conflict guards preserve all
  transcript and human-lifecycle append-only rows even with recursive triggers off.
- Every released revision declares its revision kind, review state, whether it is
  machine-generated, whether it is unreviewed, `verified_quotation: false`, a
  constrained disclaimer code, current lifecycle state, and public lifecycle history.
- Machine revisions display: “Machine-generated and unreviewed; may be wrong; not a
  verified quotation.” The same warning is carried into each search record and shown
  with every rendered revision.
- Every non-retracted revision remains searchable. No exporter, summary, route, or
  index chooses a preferred revision or silently hides another revision in the same
  language.
- Transcript revisions, segments, words, parent edges, reviews, corrections, and
  lifecycle decisions are append-only.
- A lifecycle change is one of `disputed`, `retracted`, or `reinstated`. It requires a
  matching decision by an active human reviewer, a constrained reason code, a basis,
  a strictly later UTC time, and a nonempty public explanation. Automated reviewers
  cannot create lifecycle decisions.
- The lifecycle state machine begins with `disputed` or `retracted`; reinstatement
  requires a current dispute or retraction, and a retracted revision can move only to
  `reinstated`. This prevents a later dispute from republishing retracted text.
- A current retraction is excluded from transcript text and search and immediately
  becomes a text-free tombstone with its human explanation while the current
  publication decision is `publish`. The tombstone remains after the durable `remove`
  decision. A transcript `remove` is accepted only for a current human retraction;
  policy suppression uses `withhold` instead. A gate withhold suppresses even the
  tombstone.
- Before reinstating a removed revision, publication must first move from `remove` to
  `publish` or `withhold`. While the lifecycle state remains retracted this cannot
  expose text; the later human reinstatement is the action that can return it.
- Rendition-local and media-local hypotheses remain private until a separately
  validated operation projects them into ordinary recording coordinates. This ADR
  grants no coordinate, speaker, identity, rights, or publication inference.

## Consequences

- Backfill can publish unreviewed machine transcripts at scale without presenting them
  as quotations or facts.
- Readers can compare competing revisions and see disputes, retractions, and
  reinstatements instead of receiving a silent winner.
- Search volume increases because multiple revisions may contain overlapping text.
  Revision IDs and status labels let readers distinguish those results.
- Human attention moves from mandatory wording review to identity decisions,
  corrections, lifecycle changes, high-risk privacy/sensitivity review, and wiki
  conclusions.
- Released immutable trees may retain historical bytes in old versions; the active
  release and search withdraw retracted text, while external caches and deletion
  obligations still follow the rights and takedown policy.
