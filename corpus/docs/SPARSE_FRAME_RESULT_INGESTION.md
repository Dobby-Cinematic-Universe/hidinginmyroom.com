# Private sparse-frame result ingestion

`sparse_frame_router` results cross into SQLite only through the strict
`validate-sparse-frame-result` / `import-sparse-frame-result` boundary. The producer
never opens the catalog, and the importer does not grant publication authority.

```sh
PYTHONPATH=corpus/src python3 -m himr_corpus validate-sparse-frame-result \
  --result /durable/private/vision/sparse-frames/sha256/ab/<proxy-sha>/results/<result-key>/result.json

PYTHONPATH=corpus/src python3 -m himr_corpus import-sparse-frame-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/vision/sparse-frames/sha256/ab/<proxy-sha>/results/<result-key>/result.json
```

Validation is independent of the producer. It rejects planned or failed envelopes,
unknown fields, writable or symlinked files, noncanonical file URIs, moved artifacts,
and unsupported implementation versions. It then:

1. rehashes the sealed result, referenced preprocessing result, CFR proxy, FFmpeg
   executable, and every PNG;
2. validates the referenced preprocessing envelope and reconstructs the exact proxy
   probe from its normalized probe artifact;
3. recomputes the sampling plan, recipe digest/ID, result key, run ID, artifact IDs,
   frame IDs, exact PTS/time-base arithmetic, timestamp drift, PNG structure, and
   recorded FFmpeg commands;
4. requires the already-imported preprocessing run, proxy artifact, media object,
   media location, media derivation, source-media run input, proxy rendition, and its
   source rendition to agree exactly; and
5. repeats all file hashes inside one immediate SQLite transaction before admission.

The importer uses existing tables only. It adds one completed processing run, three
explicit run inputs, private PNG artifacts, a private machine routing observation per
frame and eligible proxy rendition, and a completed job/attempt. Reimporting identical
bytes is idempotent; an ID or semantic collision fails rather than being ignored.

## Deliberately absent semantics

The admitted observation kind is `sparse_frame_routing_candidate`. Its metadata says
that the frame is routing-only, OCR is `not_evaluated`, and text presence is `unknown`.
The importer never writes `ocr_observations`, text, face tracks, identity clusters or
assertions, entities, appearances, confidence scores, or publication decisions. A
later OCR or visual-analysis stage needs its own checksummed result contract,
calibration/review policy, and explicit admission boundary.

Frame observation intervals use the proxy rendition's media coordinate space. The
decoded frame PTS is authoritative; requested timestamps remain metadata. Intervals
are bounded to the exact imported proxy duration, while all rational PTS/duration
values and rounding evidence remain in the private observation payload.

Run the focused tests with:

```sh
PYTHONPATH=corpus/src python3 -m unittest \
  corpus.tests.test_sparse_frame_result_importer -v
```

