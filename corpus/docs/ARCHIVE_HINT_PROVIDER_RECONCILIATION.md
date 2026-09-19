# Private Archive URL-hint/provider-source reconciliation

This lane handles one fixed late-arrival gap. The retained contributor-linked
complement associated with Reddit post `1q2sk8g` contained 731 Archive download
URLs. At the original 2026-08-26 hint import, 510 URLs already resolved to
`archive_media_file` sources and 221 became separate
`archive_url_discovery_hint` sources. The later sealed Archive metadata snapshot
cataloged one provider-file source for each of those 221 native IDs.

The result is a private reconciliation candidate, not a merge. Both source rows and
all historical observations remain in place.

## Fixed evidence boundary

The planner accepts only these exact ignored research artifacts:

- original UTF-16LE/CRLF complement, SHA-256
  `80fb016eccfd04dfbbf4df6d27f9ac54c5f80bd8e86ef37b0670c7095e04e587`;
- reproduced UTF-8 complement, SHA-256
  `0006f09a7c4e024c2f155e803f22329ca9d94d25e5c8cc105be1a8e9ea754ab7`;
- discovery record, SHA-256
  `35bc77da77e4e7249217bdb1d44d7fd55122b99b664a8b59c35c60f82ca9155a`;
- seven-item Archive snapshot `iams_4f920fcb565f826b8352b450247d18d3`,
  SHA-256
  `45b4aab1690a2c7610844904e1f56f2279f194b420578343beae6d16966874ba`.

It independently decodes the UTF-16 original and requires byte-for-byte reproduction
of the UTF-8 file. Every URL must use one canonical, credential-free
`https://archive.org/download/<item>/<filename>` form. The exact distribution is
510 URLs for `699992`, 79 for `hidinginmyroom`, 52 for `hidinginmyroom2`, and 90 for
`hidinginmyroom3`.

The sealed Archive request, snapshot, and all response payloads are revalidated.
For each late candidate, the provider row must be an original file with exact URL,
native ID, title, metadata observation, projection snapshot, declared byte count,
duration, MD5, SHA-1, CRC32, and one Archive recording projection. A provider source
with acquired-byte lineage is rejected: this lane reads no media and makes no
content-identity claim.

The later no-cookie Reddit Atom capture preserves the post body but does not contain
the Catbox URL or the 731 file URLs. Accordingly, the list is described only as the
retained contributor-linked complement recorded by `discovery.json`. Exact Catbox
response-envelope or comment capture remains a separate prerequisite for any stronger
claim about how the list was presented. The planner does not pretend that capture or
review occurred.

## Read-only plan

The plan command opens the catalogue read-only and does not auto-apply migrations:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  plan-archive-hint-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --original-hints "$PWD/research/corpus/discovery/reddit/1q2sk8g/archive-urls.txt" \
  --normalized-hints "$PWD/research/corpus/discovery/reddit/1q2sk8g/archive-urls.utf8.txt" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1q2sk8g/discovery.json" \
  --archive-snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_4f920fcb565f826b8352b450247d18d3/snapshot.json"
```

The default output omits the private URLs/native IDs. Add `--full` only when writing
the complete 221-candidate plan to an appropriately protected location. The strict
contract is
[`archive-hint-provider-reconciliation-plan.schema.json`](../schemas/archive-hint-provider-reconciliation-plan.schema.json).

Against the audited catalogue, the dry run produced plan SHA-256
`88b237566d95ec32af00552d7e8dfb66e7c009b129071170362c55496a9999a1`
and catalogue-evidence SHA-256
`1d667fb83790f5e5517e9f6310f5a57a155ce9aa9249bbe27544226dea186ab9`.
Those values are observations, not permanent operator overrides: always rebuild and
review the plan against the exact intended catalogue.

## Candidate-only import

Migration `0019_archive_hint_provider_reconciliation.sql` adds only an immutable
receipt and typed private candidate table. The importer additionally creates one
unscored generic match candidate and one open review task per mapping. It requires
the exact SHA-256 from the separately reviewed dry run:

```sh
# Use a disposable catalogue copy for review/canary work.
PYTHONPATH=corpus/src python -m himr_corpus \
  import-archive-hint-reconciliation \
  --db /tmp/himr-archive-hint-canary.sqlite3 \
  --original-hints "$PWD/research/corpus/discovery/reddit/1q2sk8g/archive-urls.txt" \
  --normalized-hints "$PWD/research/corpus/discovery/reddit/1q2sk8g/archive-urls.utf8.txt" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1q2sk8g/discovery.json" \
  --archive-snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_4f920fcb565f826b8352b450247d18d3/snapshot.json" \
  --expected-plan-sha256 <reviewed-plan-sha256>
```

The import command auto-applies pending migrations, so it must not be pointed at the
live catalogue until migration and combined-canary review are complete.

An exact import adds:

- one append-only reconciliation receipt;
- 221 unscored `candidate` match rows;
- 221 typed, private `candidate_only_unreviewed` evidence rows;
- 221 open review tasks.

It adds or changes zero sources, source metadata observations, source relations,
external IDs, recording links, media lineage, review decisions, claims, identity
assertions, publication decisions, or public rows. In particular, it does not copy
the historical `reddit_archive_url_hint` external ID onto the provider source and
does not create a hint-to-provider or post-to-provider relation.

Transaction-local write-rejection triggers protect every table outside the receipt,
generic candidate, typed candidate, task, and import-ledger tables. Migration guards
also require exact historical relation/external-ID observations, prevent candidate
publication, make evidence append-only, and refuse to seal an incomplete batch.
Exact replay verifies every row and is byte-idempotent.

The migration freezes source identity fields (`platform`, `source_kind`, native ID,
parent, and creating import), but intentionally does not freeze the mutable source
row's current `canonical_url` or title projection. Candidate provenance is anchored
to the exact append-only hint/provider metadata-observation IDs, their captured URLs,
the provider projection snapshot, and the typed evidence row. A later observation may
therefore update the current source projection without rewriting what this candidate
actually matched. The referenced item/provider snapshots, declared-hash rows, and
recording-source projection are separately protected against update or deletion.

Review-task completion or a later review decision does not merge sources. Any future
supersession, relationship, external-ID policy, or publication action needs its own
explicit reviewed workflow.
