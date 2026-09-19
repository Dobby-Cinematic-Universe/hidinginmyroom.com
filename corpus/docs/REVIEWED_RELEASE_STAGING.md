# Owner-private reviewed-release staging

`himr_corpus.reviewed_release_staging` rehearses and builds a static release
candidate without opening the active catalog. It starts from a sealed, hash-pinned
SQLite backup, copies it into an owner-only run directory, applies pending migrations
and completed human manifests only to that copy, and never writes under `src/`,
`public/`, `dist/`, or `.git/`.

The command has no deploy or install operation. A successful build is still marked
`candidate_not_published` and must be inspected before any separate, explicit change
to `src/data/corpus`.

## Human boundary

The packet must contain exactly:

- one registered and later activated human reviewer;
- one human `publish` decision for the nominated source;
- one human `publish` decision for the nominated recording; and
- independent human `clear` decisions for rights, privacy, and sensitivity on the
  source, recording, and transcript revision: nine gate decisions total.

Every template marker containing `REPLACE_WITH` is a hard failure. A missing,
withheld, duplicated, automated, or out-of-scope decision is also a hard failure.
The tool does not infer any clearance from public availability.

Transcript wording is not reviewed in this lane. The closed machine policy reads a
text-free plan containing coordinates, segment counts, and gate receipts. It can
publish only the exact ordinary-coordinate revision whose segments all have unnamed
speakers. The resulting revision remains `machine_generated: true`, `unreviewed:
true`, and `verified_quotation: false`, with the mandatory warning:

> Machine-generated and unreviewed; may be wrong; not a verified quotation.

## Two-pass workflow

First copy the packet templates to new owner-private reviewed files and replace every
template marker with the human's actual decision, basis, note, identity, and
whole-second UTC time. Do not treat the template prompts as findings.

Run the plan pass against a sealed backup and a pinned packet evidence file:

```sh
PYTHONPATH=corpus/src python -m himr_corpus.reviewed_release_staging plan \
  --base-catalog /absolute/path/to/sealed-base.sqlite3 \
  --expected-base-sha256 <64-lowercase-hex> \
  --evidence /absolute/path/to/evidence.json \
  --expected-evidence-sha256 <64-lowercase-hex> \
  --reviewer-manifest /absolute/path/to/reviewer-admin.reviewed.json \
  --publication-manifest /absolute/path/to/publication-manifest.reviewed.json \
  --work-root /absolute/path/to/research/corpus/private-release-staging
```

The plan pass mutates only its disposable catalog copy. It validates the fully
migrated copy, applies the explicit human decisions there, and emits a private
`receipt.json` plus a text-free machine-publication plan. It creates no release.
Confirm that the receipt says:

- `state: plan_only`;
- `live_catalog_opened: false`, `live_catalog_mutated: false`, and
  `public_tree_written: false`;
- exactly one eligible revision with the expected recording, revision, and segment
  count; and
- the exact two publication decisions and nine gate decisions.

After reviewing the plan, repeat from the same base and exact input manifests with
all three digests returned by the plan pass:

```sh
PYTHONPATH=corpus/src python -m himr_corpus.reviewed_release_staging build \
  --base-catalog /absolute/path/to/sealed-base.sqlite3 \
  --expected-base-sha256 <64-lowercase-hex> \
  --evidence /absolute/path/to/evidence.json \
  --expected-evidence-sha256 <64-lowercase-hex> \
  --reviewer-manifest /absolute/path/to/reviewer-admin.reviewed.json \
  --publication-manifest /absolute/path/to/publication-manifest.reviewed.json \
  --work-root /absolute/path/to/research/corpus/private-release-staging \
  --expected-machine-plan-sha256 <reviewed-plan-sha256> \
  --expected-reviewer-manifest-sha256 <reviewed-manifest-sha256> \
  --expected-publication-manifest-sha256 <reviewed-manifest-sha256>
```

The build pass starts over rather than trusting the plan-pass database. It refuses a
changed plan or manifest, applies the digest-authorized closed policy to its new
copy, runs full catalog validation, verifies exact packet counts and unnamed-speaker
status, exports v2 shards, validates the entire hash tree, and seals the candidate
release read-only inside the private run directory.

## Output and promotion

Work roots must be absolute, owner-only directories beneath `research/corpus` or
`/tmp`. The base catalog must be a non-symlink, read-only regular file with no WAL,
SHM, or journal sidecar. The active catalog is never an acceptable base while it has
sidecars, and the staging tool never connects to the base in SQLite at all.

Before a separate promotion decision, inspect the candidate page and all search
records, verify the source links and warning, validate `candidate-release/manifest.json`
again, run the repository's full public-release and Astro checks against a temporary
copy, and review the complete static diff. Promotion to `src/data/corpus`, commit,
and deployment are intentionally outside this tool.
