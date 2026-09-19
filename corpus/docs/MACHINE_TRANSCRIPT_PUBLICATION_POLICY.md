# Closed machine-transcript default-publication policy

Migration `0028` implements the transcript-default rule as one narrow, auditable
capability. It does not publish anything when the migration is installed. It creates
no reviewer, gate, review, lifecycle, or publication row. A later operator must review
a text-free plan digest and explicitly apply that exact plan.

## Exact v1 eligibility

`machine_transcript_policy_publish_scope` contains a revision only when all of these
conditions hold at plan and apply time:

- it is an ordinary recording-coordinate row in `transcript_revisions`, not a
  rendition-local or media-local timing hypothesis;
- `review_state` is `machine` and `revision_kind` is `raw_asr` or `contextual_asr`;
- it has at least one segment;
- every segment has a null `speaker_label`; the public reader may render this as an
  unknown speaker, but v1 makes no speaker or identity decision;
- the current rights, privacy, and sensitivity decisions are three separate `clear`
  gate rows made by human reviewers during recorded active intervals;
- the revision exists before every clearing gate, and the revision and all three
  gates exist no later than the policy application time;
- it has no dispute, retraction, reinstatement, or other lifecycle history; and
- it has no prior publication-decision row of any kind or reviewer. A human publish,
  withhold, or remove permanently takes that revision out of the default lane.

The plan reads catalog coordinates, revision metadata, segment counts, and gate
decision IDs. It does not read or serialize transcript text, normalized text, words,
or private review notes. Its top-level, revision, gates, and per-gate objects have
exact key sets, and the database requires the run to contain the complete current
scope rather than a hand-selected subset. Multiple segments are counted once each
despite the three gate joins.

## Operator workflow

First install and review pending migrations through the separate schema-authority
command, then close the writer so no WAL, SHM, or journal sidecar remains:

```sh
PYTHONPATH=corpus/src python -m himr_corpus migrate \
  --db research/corpus/corpus-v8.sqlite3
```

Generate the complete current plan through the immutable audit connection:

```sh
PYTHONPATH=corpus/src python -m himr_corpus plan-machine-transcript-publication \
  --db research/corpus/corpus-v8.sqlite3
```

Review the policy ID, eligible count, revision metadata, exact gate IDs, and
`plan_sha256`. Apply only that digest:

```sh
PYTHONPATH=corpus/src python -m himr_corpus apply-machine-transcript-publication \
  --db research/corpus/corpus-v8.sqlite3 \
  --expected-plan-sha256 <64-lowercase-hex-plan-sha256>
```

The apply command refuses pending migrations instead of installing them. It
recomputes the scope inside `BEGIN IMMEDIATE`, requires an exact digest match, and
atomically appends one policy-run receipt, one publication-manifest receipt, and one
initial `publish` decision per planned revision. A changed gate, lifecycle row, or
human publication row changes or removes the candidate and makes the reviewed digest
fail closed. Replaying a successfully applied digest validates its stored canonical
plan and exact decisions, then returns an idempotent no-op.

Every decision re-joins its stored plan item to all current pinned fields, including
the segment count and exact three gate IDs, reviewers, and times. Even a separately
committed run receipt cannot be used after a new segment, named speaker, lifecycle
row, publication row, or replacement gate changes that revision's state.

The application time must equal the publication-manifest import time and every policy
decision time, cannot be in the future, and must fall inside the policy reviewer's
recorded active interval. Once a policy decision exists, database guards seal the
revision's exact segment set and prohibit later word insertion. Migration `0028` also
closes `INSERT OR REPLACE` conflict routes for transcript revisions, segments, words,
parent edges, reviews, corrections, and lifecycle rows even when SQLite recursive
triggers are disabled. Public text and human retraction explanations therefore cannot
be silently substituted after admission.

Run ordinary audit validation and export as separate operations afterward. The policy
command itself does not export or deploy a public release.

## Authority boundary

First successful use registers and activates only the reserved
`reviewer_machine_transcript_default_policy_v1`. Database triggers constrain it to
the deterministic decision ID, exact basis, exact public label, exact warning, exact
plan membership, exact run time, and current initial-publication scope.

Migration refuses any pre-`0028` use of the reserved reviewer, admin-manifest/event
IDs, decision prefix, or policy-manifest prefix. After migration, ordinary reviewer
and publication administrators reject those namespaces. The dedicated path verifies
the built-in reviewer's exact registration/activation receipt and uses
`machine-transcript-default-policy-v1:<plan-sha256>` and
`machine-transcript-default-v1:<revision-id>` only inside its atomic transaction.

The reviewer cannot create or clear any publication gate, create a wording review or
correction, publish a source/recording/entity/event, make a restrictive publication
decision, or create a dispute, retraction, or reinstatement. Generic publication
manifests cannot exercise this capability. Human reviewers retain exclusive control
of identity-bearing transcripts, gate changes, corrections, disputes, retractions,
reinstatements, and every stream with existing publication history.

Every admitted machine revision is exported with
`machine_generated: true`, `unreviewed: true`, `verified_quotation: false`, and the
code `machine_generated_unreviewed_not_verified_quotation_v1`. Existing release and
UI code renders the exact warning:

> Machine-generated and unreviewed; may be wrong; not a verified quotation.

The stored publication decision also carries that exact sentence in `notes`, and
catalog validation reconstructs each run from its canonical plan and referenced
historical human gate decisions.
