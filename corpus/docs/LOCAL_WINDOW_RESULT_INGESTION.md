# Private local-window result ingestion

This boundary turns one completed, sealed `pipeline/local_window.py` v1 result into
catalog-backed private analysis inputs. It does not make the derivative original
evidence, publish it, identify anyone, or claim that FFmpeg's packet/frame boundary
matches the integer source-time contract exactly.

## Why the parent run is an admission run

Local-window result v1 records the exact work-order hash, tools, source bytes,
commands, derivative bytes, and source-time transform, but it does not record a
producer start or completion time or a producer processing-run row. The importer
therefore refuses to invent an extraction run. Instead, `--observed-at` records the
time of a real `local_window_result_admission` verification run, with start and end
set to that same observation. Its parameters explicitly state
`catalog_admission_verification_not_extraction_execution` and that the producer
extraction time is absent from v1.

An ASR work order may use that admission run as
`input.parent_processing_run_id`: the referenced private artifact was actually
verified and admitted by that run. It must not describe the ID as the unknown FFmpeg
execution run.

## Fail-closed checks

Validation and import both:

- require a `0500` result directory containing exactly `result.json`,
  `audio-16khz-mono.flac`, and `proxy-640x360-25fps.mp4`;
- require all three files to be regular, non-symlink `0400` files at the exact
  absolute paths in the envelope;
- reject duplicate JSON keys, non-finite numbers, unknown fields, planned results,
  path traversal, extra files, and unsupported producer/profile/safety semantics;
- reconstruct the exact supported v1 work order and all four FFmpeg/FFprobe commands,
  recompute `work_order_sha256`, and rehash/reinspect the pinned executable builds;
- rehash the exact result bytes and both derivatives while checking path/inode/size/
  mtime stability, then independently FFprobe the current parent and both derivatives
  and require exact agreement with every embedded stream, codec, container, duration,
  and normalized-probe field;
- rehash the acquired full parent and exact acquisition-result bytes, then verify the
  media, location, source link, completed acquisition import batch, acquisition run,
  run input, and eligible parent rendition already present in SQLite; and
- accept ASR recording context only from an approved, non-disputed full-source role,
  either whole-source or with an explicit interval covering the complete window;
  partial, related-context,
  candidate, disputed, rejected, conflicting, and out-of-range mappings cannot be
  promoted into a window context;
- repeat the exact result/artifact/acquisition/parent/tool hashes and every catalog
  lineage/context query under the same write transaction used for insertion, reject
  reuse of an admitted result/artifact path for different bytes, and roll back on any
  preflight-to-transaction change; and
- create only private artifacts, unreviewed renditions, metadata-only timeline maps,
  and an exact import ledger. No publication or identity row is created.

The existing append-only `import_batches` ledger honestly represents this exact
result admission, so no schema migration is required.

`0400`/`0500` sealing is an operator-controlled local evidence boundary, not a digital
signature. The verifier proves that the bytes currently present are internally
consistent with the supported producer contract; it cannot prove how coherently
rewritten, never-before-admitted local files were created without rerunning the full
transcode. After first admission, both the logical bundle/window and every storage URI
are immutable: replacement bytes are rejected even when they are otherwise valid
FLAC/MP4 media.

## Validate, review, and import

Use one UTC timestamp for both commands. It is the catalog observation time, not a
reconstruction of the extraction time, and it may not precede acquisition completion.

```sh
admission_observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

PYTHONPATH=corpus/src python3 -m himr_corpus validate-local-window-result \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --result "/absolute/private/window_000001/result.json" \
  --observed-at "$admission_observed_at"

PYTHONPATH=corpus/src python3 -m himr_corpus import-local-window-result \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --result "/absolute/private/window_000001/result.json" \
  --observed-at "$admission_observed_at"
```

The import command fully repeats validation and hashing inside the admission
workflow. Repeating it with the same result and observation time must return exactly
the same IDs and rows. Reusing the same sealed result with a different observation
time is rejected rather than rewriting provenance.

The validation command opens SQLite in query-only mode. It cannot create a missing
database, apply a migration, or write a validation ledger.

The output conforms to
`corpus/schemas/local-window-catalog-admission.schema.json`. Its
`asr_work_order_inputs` array contains the exact `input`, `catalog_context`, and
`window` objects to copy into a whisper.cpp work order. For a normalized local FLAC,
the local-artifact ASR window is always:

```json
{"offset_ms": 0, "duration_ms": null}
```

ASR timestamps are therefore local-artifact coordinates. To obtain the contracted
full-source coordinate, add
`source_time_mapping.artifact_zero_maps_to_source_ms`. The mapping remains marked
`not_calibrated`; encoder delay, padding, discontinuities, and boundary alignment
still require downstream checks. The derived rendition metadata and its private
timeline span preserve that distinction.

That arithmetic yields **source-media** time only. It does not authorize writing the
same value into recording-scoped transcript rows. The ordinary result importer still
rejects local-window `catalog_context` before any writes. Completed machine text can
instead enter the private, separately searchable `rendition_local_*` tables through
the text-free plan/digest workflow documented in
[Private rendition-local ASR admission and search](RENDITION_LOCAL_ASR.md). That lane
keeps every recording endpoint null and makes no timeline assertion.

## Batch validation after implementation review

The following safely covers only completed result envelopes under one sealed bundle:

```sh
admission_observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
for result in "$PWD"/research/corpus/local-window-artifacts/archive-UUqmpEOc5oc/windows/*/*/*/window_*/result.json; do
  PYTHONPATH=corpus/src python3 -m himr_corpus validate-local-window-result \
    --db "$PWD/research/corpus/corpus-v8.sqlite3" \
    --result "$result" \
    --observed-at "$admission_observed_at"
done
```

Do not change the glob to a broad workspace search. Review every returned source,
window, artifact hash, and ASR context before replacing `validate-` with `import-`.
