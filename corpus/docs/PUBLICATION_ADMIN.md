# Publication administration manifests

Publication and clearance decisions enter the private catalog only through a strict,
append-only manifest workflow. Discovery, metadata, acquisition, preprocessing, ASR,
OCR, and identity-result imports never create or clear publication gates.

Keep filled manifests under an ignored private path such as
`research/corpus/publication-manifests/`. Do not add them to Git: notes can contain
review context that is inappropriate for the public repository. The versioned JSON
contract is [`../schemas/publication-manifest.schema.json`](../schemas/publication-manifest.schema.json).

## Safe workflow

First validate the complete manifest against both the strict shape and the current
catalog:

```sh
PYTHONPATH=corpus/src python -m himr_corpus validate-publication-manifest \
  --db research/corpus/corpus-v8.sqlite3 \
  --manifest research/corpus/publication-manifests/2026-08-26-review.json
```

`import-publication-manifest --dry-run` performs the same checks and is provided for
automation that uses one command in both stages:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-publication-manifest \
  --db research/corpus/corpus-v8.sqlite3 \
  --manifest research/corpus/publication-manifests/2026-08-26-review.json \
  --dry-run
```

Both validation forms open a closed, checkpointed catalog through an immutable
read-only connection. They never initialize or migrate a database, create SQLite
sidecars, or change catalog bytes or timestamps. A pending migration is an error;
run the explicit migration command separately and close the writer before retrying.

After reviewing the reported manifest ID, canonical input SHA-256, and counts, remove
`--dry-run` to commit. The import is one `BEGIN IMMEDIATE` transaction: either every
decision and its private manifest-ledger row are appended or none is. Reusing a
manifest ID, canonical manifest digest, or decision ID is rejected rather than treated
as an update. The ledger stores only identifiers, digest, version, counts, and import
time; each imported decision references that ledger row. It does not store the private
manifest body or path.

Run catalog validation and generate the public release separately after import:

```sh
PYTHONPATH=corpus/src python -m himr_corpus validate \
  --db research/corpus/corpus-v8.sqlite3

PYTHONPATH=corpus/src python -m himr_corpus export-sharded \
  --db research/corpus/corpus-v8.sqlite3 \
  --out-dir src/data/corpus

PYTHONPATH=corpus/src python -m himr_corpus validate-release \
  --release src/data/corpus/manifest.json
```

## Manifest contract

The top-level object has exactly four keys:

```json
{
  "schema_version": 1,
  "manifest_id": "publication_review_2026-08-26_a",
  "publication_decisions": [
    {
      "publication_decision_id": "publication_source_abc_2026-08-26_a",
      "object_type": "source",
      "object_id": "source_abc",
      "decision": "publish",
      "reviewer_id": "reviewer_alice",
      "decided_at": "2026-08-26T19:30:00Z",
      "basis": "Direct review of the source and its current public availability.",
      "note": "Approval is limited to the source metadata represented in the public view."
    }
  ],
  "gate_decisions": [
    {
      "publication_gate_decision_id": "gate_source_abc_rights_2026-08-26_a",
      "object_type": "source",
      "object_id": "source_abc",
      "gate_kind": "rights",
      "decision": "clear",
      "reviewer_id": "reviewer_alice",
      "decided_at": "2026-08-26T19:30:00Z",
      "basis": "Completed the rights checklist for this source record.",
      "note": "No unresolved rights blocker was found for publishing this metadata."
    }
  ]
}
```

Every decision requires an explicit object type and ID, active reviewer ID, whole-
second UTC `decided_at` ending in `Z`, nonempty policy/evidence `basis`, and nonempty
review `note`. A publication decision is `publish`, `withhold`, or `remove`. A gate
decision names exactly one of `rights`, `privacy`, or `sensitivity` and is `clear` or
`withhold`. Supported public object types are `source`, `recording`,
`transcript_revision`, `entity`, `event`, `appearance`, `event_date`,
`event_participant`, `event_relation`, and `event_evidence`.

Graph objects are deliberately stricter than the general contract. A permissive
decision for any entity, event, appearance, participant, date, relation, or evidence
edge must carry a nonempty `public_label` and reference a complete matching human
accept/correct review. Every graph gate clearance must also reference a complete
matching human review. Appearance and event-evidence publication reviews must record
that audio or video was directly perceived. Migration 0031's narrow public views
enforce these rules for roots and edges; database insertion guards additionally
enforce them for the newly publishable edge types even when SQL bypasses this
administrator. The pre-existing entity/event decision streams are not retroactively
narrowed because they can serve non-graph public workflows.

`decided_at` cannot be in the future. These rows take effect immediately in current
publication views and are audit records, not scheduled actions.

`review_decision_id` is optional. When supplied, it must identify an existing review
of the same object by the same reviewer, and the review must not postdate the
publication record. Publication decisions may also contain an optional
`public_label`.

The named reviewer must already exist, be active now, and have a recorded active
interval covering `decided_at`. The manifest does not create reviewers, catalog
objects, reviews, or assessments. Reviewer identities and state are administered
through the separate reviewer-administration workflow.

Reviewer kind is an authority boundary:

- an active `human` reviewer may make publication and gate decisions;
- `imported_legacy` preserves historical attribution but cannot make a new
  publication or gate decision;
- an ordinary `automated_policy` may only make restrictive decisions
  (`withhold`/`remove` for publication and `withhold` for a gate), never publish or
  clear a gate;
- the built-in `reviewer_public_metadata_policy_v1` has one narrower capability: it
  may publish only the exact metadata-only source and recording rows returned by
  `public_metadata_policy_publish_scope`, with that view's exact basis and public
  label. It cannot decide on transcripts, entities, events, unrelated sources, or
  any publication gate, including restrictive decisions;
- the built-in `reviewer_machine_transcript_default_policy_v1` is not available to
  this generic manifest importer. Its dedicated digest-gated command may append only
  an initial `publish` for an unnamed-speaker, ordinary-coordinate machine transcript
  returned by its closed scope after current human rights, privacy, and sensitivity
  clears. It cannot clear or withhold gates, review or correct wording, make a later
  publication decision, or dispute, retract, or reinstate a transcript. See
  [Machine-transcript default publication](MACHINE_TRANSCRIPT_PUBLICATION_POLICY.md).

The generic parser reserves every manifest ID beginning
`machine-transcript-default-policy-v1:` and every publication or gate decision ID
beginning `machine-transcript-default-v1:`. Database triggers enforce the same
namespace boundary for direct SQL. Only the dedicated digest-gated transaction can
create those receipts and decisions.

The built-in policy is registered inactive and explicitly activated at its actual
first use inside the same transaction as metadata approval. Its active reviewer row
does not confer authority outside the database-enforced capability above.

## Fail-closed ordering

Within each object decision stream, records must appear in strictly increasing
`decided_at` order. Gate streams are independent per object and gate kind. The
importer rejects:

- unknown keys, unsupported values, missing notes, offsets other than literal UTC
  `Z`, and empty manifests;
- unknown or type-mismatched objects, unknown/inactive reviewers, and mismatched
  referenced review decisions;
- duplicate JSON keys, manifest/decision IDs, multiple decisions for the same stream
  and instant, or a timestamp older than the current catalog stream;
- a same-time `publish` that would weaken `withhold`/`remove`, or a same-time `clear`
  that would weaken a gate `withhold`.

The database retains append-only insert/update/delete guards (including every
`INSERT OR REPLACE` conflict route), reviewer-time and reviewer-kind checks, and
same-time takedown precedence as a second line of defense. A later clearance or
republication therefore requires a new ID, an explicitly later UTC time, a new
basis, and a new note.

## Eligibility is conjunctive

A `publish` decision alone exposes nothing. The current rights, privacy, and
sensitivity gate decisions for that exact object must all be `clear`, and the
object-specific public view must also accept the object. For example, a source must
still be currently public and non-rejected; an ordinary recording-coordinate
transcript must be in an eligible `machine`, `human_corrected`, `media_checked`, or
`disputed` state and belong to a public recording. Machine wording does not require a
human review merely to enter this lane, but it retains its generated, unreviewed, and
non-quotation warning. Withholding any one gate removes the object from the release
projection without editing historical decisions.
