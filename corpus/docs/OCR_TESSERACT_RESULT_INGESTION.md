# Private Tesseract OCR admission and search

Migration 0033 adds a narrow catalog boundary for completed
`ocr_tesseract_tsv` v1 results. It does not run OCR. The importer independently
rehashes and replays the completed result, its exact completed sparse-frame result,
every selected PNG, every raw TSV, the pinned executable and language models,
Tesseract command construction, TSV parsing, word IDs, and all time/geometry
coordinates.

This lane requires SQLite 3.44.0 or newer. That is the first SQLite release whose
table-scoped `PRAGMA integrity_check` invokes an FTS5 virtual table's native integrity
check. Migration, verification, import, replay, search, and full validation fail
closed on older runtimes rather than accepting an unchecked shadow index.

The upstream sparse-frame result must already be admitted. Each OCR frame is bound to
one exact source, source rendition/media, proxy rendition/media, existing
`sparse_frame_routing_candidate`, and a half-open `rendition_media_ms` interval. No
recording-time transform is inferred.

`start_ms` and `end_ms` are local decoded-media coordinates on the admitted
`low_resolution_cfr_proxy` identified by `rendition_id`/`media_id`. The
`source_id`, `source_rendition_id`, and `source_media_id` columns are exact provenance
anchors only; they do not make those milliseconds original-source time. Likewise,
`requested_timestamp_ms` is the sparse router's seek request on that proxy, not a
recording-timeline assertion. Search therefore returns explicit proxy-rendition
coordinates while source-time and recording-time mappings remain null/not asserted.

## Commands

Validation opens no catalog and writes nothing:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  validate-ocr-tesseract-result \
  --result /ABSOLUTE/PRIVATE/vision/ocr/tesseract/.../result.json
```

Review the printed `result_raw_sha256`, explicitly migrate a disposable or approved
catalog in a separate administrative step, then admit that exact digest:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  import-ocr-tesseract-result \
  --db /ABSOLUTE/PRIVATE/corpus.sqlite3 \
  --result /ABSOLUTE/PRIVATE/vision/ocr/tesseract/.../result.json \
  --expected-result-sha256 SHA256_FROM_VALIDATION
```

The import command refuses pending migrations; it never installs schema as a side
effect. Exact replay revalidates all current files, reconstructs the complete expected
catalog footprint, and compares every receipt, import batch, processing run, run
input, TSV artifact, frame, word, observation, OCR detail, and score field. It returns
the existing immutable receipt without adding FTS rows only when those row sets match
exactly.

Search uses a closed, checkpointed, immutable read connection. Before executing the
FTS query it rehashes and exactly replays every current receipt, upstream result, and
owned catalog row; a coherent forged ledger/index append therefore cannot become a
search result merely because its row counts agree:

```sh
PYTHONPATH=corpus/src python -m himr_corpus search-private-ocr \
  --db /ABSOLUTE/PRIVATE/corpus.sqlite3 \
  --query 'HIMRverse' \
  --recording-id rec_...
```

Optional `--source-id`, `--recording-id`, and `--rendition-id` filters retain the
same exact anchor. Results are raw machine OCR, not quotations.

## Safety semantics

- TSV `conf` is stored as `raw_score` on the producer's 0–100 scale and explicitly
  labeled `not_a_probability` and `not_calibrated`. `calibrated_probability` and
  `calibration_set_id` must remain null.
- Text, TSV artifacts, observations, receipts, FTS state, and frame bindings remain
  private. Human review is required and redaction stays `pending`.
- Append-only triggers protect receipts, import batches, processing runs and inputs,
  TSV artifacts, frame admissions, word admissions, OCR observations, details, and
  scores. Collision guards cover every rowid/primary/unique replacement path even
  when `recursive_triggers` is disabled. Receipt and per-frame declared counts seal
  new frame, observation, and word insertions once admission is complete; new inputs
  or artifacts cannot be attached after a receipt. Exact replay is a match operation,
  not an update.
- FTS5 does not permit ordinary triggers on the virtual table itself. Its contents
  are a non-authoritative private index populated only by the word-admission insert
  trigger. Direct FTS insert, update, delete, or shadow-index mutation is treated as
  tampering: exact replay and search run SQLite's read-only table-scoped FTS checksum
  and compare visible rows both directions with the authoritative word ledger. Full
  database validation repeats those checks. Missing, changed, extra, duplicate, or
  semantically reindexed rows fail closed.
- Machine OCR cannot become an appearance/identity-cluster member, event or claim
  evidence, publication decision, publication-gate subject, or public-export object.
  The guards cover both reserved OCR object labels and the established generic
  `observation`/`artifact` publication labels. They act as soon as an OCR observation
  exists, before its word-ledger row, and reject generic publication state preseeded
  for a future deterministic OCR observation or TSV-artifact ID.
  A later reviewer must create a separate human-governed object from the underlying
  media if any such use is warranted.
- Database validation strictly parses both the OCR and referenced sparse-frame JSON
  without duplicate keys, replays the sealed TSV text into every catalog row, and
  rejects missing or duplicated FTS rows, hidden-index checksum drift, score misuse,
  mutable provenance, null/missing lineage keys, broken anchors, count drift,
  authority leakage, or any row-level disagreement.

Run the bounded producer/importer regression suites with:

```sh
python3 -m unittest pipeline.tests.test_ocr_tesseract_adapter -v
PYTHONPATH=corpus/src python3 -m unittest \
  corpus.tests.test_ocr_tesseract_result_importer -v
```

This lane is deliberately absent from all static release queries. The public exporter
does not read its tables.
