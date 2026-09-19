# Archive.org bracketed YouTube-ID reconciliation

This is a private, review-only catalog lane for Archive.org video filenames or
per-file titles ending in a bracketed YouTube ID, for example
`Some title [AbCdEfGhI_1].mp4`. It builds on a snapshot already admitted by the
strict `import-archive-metadata-snapshot` boundary. It performs no network access.

The lane never merges recordings, changes a source, attaches a new recording
source, creates a recording relation, or makes an identity, claim, event, or
publication decision. It emits immutable candidates and review tasks. A later,
separate human adjudication is required before any catalog merge or relation.

## Exact recognition contract

Recognition is intentionally narrow:

- the token must contain exactly 11 characters from YouTube's ID alphabet
  (`A-Z`, `a-z`, `0-9`, `_`, `-`);
- it must be enclosed in `[` and `]`;
- it must be the final text in the filename/title, or immediately precede one
  recognized video extension;
- `.ia` immediately before the final video extension is accepted for Archive.org
  derivative names such as `[AbCdEfGhI_1].ia.mp4`;
- trailing whitespace, counters, prose after the bracket, bare IDs, and dash-only
  suffixes are rejected;
- if a filename token and per-file title token disagree, no match candidate is
  emitted. An explicit conflict issue and high-priority review task are emitted.

The historical Archive.org importer still has its older dash-suffix behavior:
`-AbCdEfGhI_1.mp4` (and its optional derivative counter) can determine that
importer's initial recording key. This new lane does not broaden or reinterpret
that behavior. It independently derives only bracketed evidence from the sealed
provider payload.

## Deterministic reconciliation states

Each consistent Archive file gets a source-to-locator candidate. Exact native IDs
are looked up only through the canonical source identity
`(youtube, youtube_video, <ID>)`:

| Catalog state | Candidate result |
| --- | --- |
| No exact YouTube source | Candidate points to the literal YouTube ID; review required |
| YouTube source exists but maps to zero or multiple recordings | Candidate points to the source and is marked `native_source_without_unique_recording`; no recording candidate |
| YouTube source maps only to canonical `youtube:video:<ID>` | Separate archive-recording-to-YouTube-recording candidate; review required |
| Multiple distinct Archive recordings share the same bracket ID | Deterministic anchor-to-peer intra-Archive candidates; review required |
| Filename and file-title IDs conflict | Conflict issue only; no candidate or merge |

Archive original/derivative files remain distinct source objects. Repeated files
that already share one native Archive recording projection remain represented in
the source-level evidence without manufacturing a self-match.

## Commands

First validate and import the sealed metadata snapshot as documented in
`acquisition/ARCHIVE_ORG_METADATA.md`. Then preview the deterministic counts. This
command opens the existing database in SQLite read-only/query-only mode and does not
apply migrations:

```bash
PYTHONPATH=corpus/src python3 -m himr_corpus \
  plan-archive-bracket-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
```

After reviewing the preview and backing up the private catalog, admit the private
candidates:

```bash
PYTHONPATH=corpus/src python3 -m himr_corpus \
  import-archive-bracket-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
```

Run the exact import command a second time. The result and row counts must be
identical. A completed replay verifies every receipt, generic candidate, immutable
subtype row, conflict issue, and review-task identity; it does not repair missing
or altered evidence.

The planner also fails closed before admission above 250,000 provider file records,
50,000 files carrying bracket evidence, 25,000 distinct candidate YouTube IDs,
100,000 emitted candidates, or 10,000 filename/title conflicts. These limits and
the observed usage are included in the deterministic plan core, so changing a cap
changes the plan hash rather than silently changing an existing plan.

## Storage and safety contract

Migration `0013_archive_bracket_reconciliation.sql` adds three private tables:

- `archive_bracket_reconciliation_imports` seals snapshot and plan hashes plus
  exact candidate/task counts;
- `archive_bracket_youtube_candidates` is an immutable subtype of a generic
  `match_candidates` row and fixes `requires_human_review = 1`,
  `relationship_asserted = 0`, and `merge_performed = 0`;
- `archive_bracket_reconciliation_issues` preserves conflicting suffix evidence
  without creating a match.

Admission triggers verify Archive source projections, canonical YouTube source and
recording dependencies when present, and review-task targets. The generic match
row and raw subtype evidence become append-only. Review progress belongs in the
review task/decision stream, never in an edited evidence row. None of these tables
feed a public view.
