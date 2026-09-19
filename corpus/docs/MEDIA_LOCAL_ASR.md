# Private media-local ASR admission and search

Migration 0021 provides a private transcript lane for exact ASR results whose
producer `catalog_context` is null. Its coordinate system is only `media_ms`: zero is
the start of the normalized input artifact. No recording, rendition, source, or
timeline coordinate is implied by a duration match or by catalog discovery.

This bridge accepts only process-routed members of sealed queue
`asrppqueue_66349d9b85c74f2376830edf2a7d4f0c`, pinned by exact physical and semantic
hashes. It revalidates the exact preprocess result, normalized audio bytes, catalog
admission batch, media derivation, work order, model registry, engine and ASR
artifacts before every plan and import. Review-only ordinals 1 and 13 have no results
and cannot be admitted.

Every accepted result must also be an exact member of applied seal receipt
`asrsealreceipt_c0694c5c36586eb433a766490f3dbc01`. The bridge rechecks the pinned
receipt bytes, closed three-entry result directory (mode `0500`), each result file
(mode `0400`, link count one), and exact file bytes before and after artifact
validation. Writable lookalikes, hardlinks, extra entries, and unreceipted copies fail.

## Why a distinct lane is required

The 17 completed short-clip results have no producer recording/rendition context.
Although their normalized media are registered in the private catalog, recording
source bounds are null and their normalized renditions have no timeline spans.
Duration equality is not a timestamp transform. The bridge writes no ordinary
transcript revision, recording-scoped segment, source-coordinate row, timeline span,
identity assertion, claim, event, or publication decision.

The authoritative private tables are `media_local_transcript_revisions`,
`media_local_transcript_segments`, `media_local_transcript_words`, and
`media_local_asr_imports`. An FTS5 index makes segment text searchable without
promoting it to main corpus search. Authoritative rows and receipts are append-only;
generic publication and publication-gate decisions for the lane are rejected.

## Review and admission

First migrate a disposable catalog copy, then create a text-free plan per result:

```sh
queue="$PWD/research/corpus/private-preprocess-asr-work-orders/queues/asrppqueue_66349d9b85c74f2376830edf2a7d4f0c/manifest.json"
result=/absolute/private/asr/whispercpp/sha256/../results/<result-key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus migrate --db /tmp/corpus-review.sqlite3
PYTHONPATH=corpus/src python -m himr_corpus \
  plan-media-local-asr-admission \
  --db /tmp/corpus-review.sqlite3 \
  --queue-manifest "$queue" \
  --result "$result" > /tmp/media-local-plan.json
```

Review the exact result, queue, work-order and preprocess hashes; media/artifact IDs;
ordinal; counts; duration accounting; and `plan_sha256`. The plan contains no
transcript wording. Its catalog context and all source/recording/rendition IDs are
null; both transform states are `unasserted_catalog_context_null`.

Admission requires the reviewed digest and repeats all checks before and inside one
transaction:

```sh
plan_sha=$(python -c 'import json,sys; print(json.load(sys.stdin)["plan_sha256"])' \
  < /tmp/media-local-plan.json)
PYTHONPATH=corpus/src python -m himr_corpus \
  import-media-local-asr-result \
  --db /tmp/corpus-review.sqlite3 \
  --queue-manifest "$queue" \
  --result "$result" \
  --expected-plan-sha256 "$plan_sha"
PYTHONPATH=corpus/src python -m himr_corpus validate \
  --db /tmp/corpus-review.sqlite3
```

Replay is row-idempotent. A changed queue, work order, preprocess receipt, input,
catalog lineage, result artifact, or plan digest fails closed.

## Exact timing preservation

Segment and word endpoints are copied without clipping. A nullable word timing pair
remains nullable. Five results end after their normalized input:

| Queue ordinal | Input end | Maximum segment end | Preserved overrun |
| ---: | ---: | ---: | ---: |
| 3 | 179,003 ms | 185,880 ms | 6,877 ms |
| 4 | 300,559 ms | 306,140 ms | 5,581 ms |
| 6 | 144,057 ms | 144,860 ms | 803 ms |
| 11 | 200,922 ms | 205,900 ms | 4,978 ms |
| 12 | 201,526 ms | 203,200 ms | 1,674 ms |

These are raw machine outputs and review signals, not evidence that speech continues
beyond the media. Ordinal 17 is a valid completed result with one revision/receipt
and zero segments or words.

## Private search

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  search-media-local-transcripts \
  --db /tmp/corpus-review.sqlite3 \
  --query '<private query>' \
  --media-id media_sha256_<sha256> \
  --limit 25
```

Each hit returns exact media-local endpoints. `source_start_ms`, `source_end_ms`,
`recording_id`, `rendition_id`, `recording_start_ms`, and `recording_end_ms` are
always null. Migration 0029 now supplies a deliberately narrow translation lane for
zero-offset full-file inputs with direct public-file lineage, complete pair selection,
no overrun, exact normalized-parent duration equality, and at most 5 ms of
normalized-recording metadata disagreement. A strict reviewed manifest pins all
catalog and raw/contextual receipt IDs, including an exact append-only rank-700
public acquisition observation rather than relying on current availability forever.
It creates new immutable
recording-scoped rows without rewriting these observations; see
[Full-file media-local transcript projection](MEDIA_LOCAL_TRANSCRIPT_PROJECTION.md).
