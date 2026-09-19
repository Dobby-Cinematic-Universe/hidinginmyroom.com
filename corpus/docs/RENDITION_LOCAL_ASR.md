# Private rendition-local ASR admission and search

Local analysis windows start at zero even when they represent a later portion of a
source recording. Ordinary `transcript_segments` are recording-scoped, so copying a
window transcript into that table would silently place it at the wrong time. Migration
0020 adds a separate private lane whose coordinate system is explicitly
`rendition_media_ms`.

This lane stores searchable machine text without changing the recording timeline. It
does not create a normal transcript revision, `timeline_map_spans` row, identity
assertion, claim, or publication decision. Database triggers prohibit generic
publication decisions for every rendition-local import, revision, segment, word, and
transform candidate.

## Review the text-free plan first

The planner rehashes the ASR input, engine, model, optional glossary, both ASR JSON
artifacts, the ASR result, and the exact local-window result named by the admitted
artifact. It also verifies the model registry, local-window admission batch and run,
acquired source rendition, recording/source/media chain, and the one unresolved
full-artifact timeline span.

```sh
result=/absolute/private/asr/whispercpp/sha256/../results/<result-key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus \
  plan-rendition-local-asr-admission \
  --db research/corpus/corpus-v8.sqlite3 \
  --result "$result"
```

The plan includes hashes, IDs, counts, duration accounting, and proposed transform
hypotheses, but no transcript wording. Review its `plan_sha256`, especially:

- `coordinate_system: rendition_media_ms`;
- `recording_transform_state: unresolved` and
  `recording_coordinates_asserted: false`;
- the artifact/source duration delta;
- the maximum ASR end and both boundary-overrun values; and
- every transform candidate's duration inputs, ratio, `hypothesis_only_unreviewed`
  state, and `timeline_application_allowed: false` field.

Admission requires that exact reviewed digest and redoes all checks inside the same
transaction used for insertion:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  import-rendition-local-asr-result \
  --db research/corpus/corpus-v8.sqlite3 \
  --result "$result" \
  --expected-plan-sha256 <reviewed-plan-sha256>
```

Replaying the same result and plan digest is row-idempotent. A different result at the
same URI, changed local-window lineage, changed catalog duration, altered unresolved
timeline, or different plan digest fails closed.

## Coordinate semantics

Segment and non-null word times are copied exactly into the rendition-local tables.
They are never clipped to the requested ASR duration. A separately labelled source
coordinate is computed only by adding the local-window result's integer
`artifact_zero_maps_to_source_ms` offset. That source mapping remains
`integer_contract_not_boundary_calibrated`; it is not a recording transform.

The retained `gPhrE99xwqI` tail window makes all three boundaries visible:

| Boundary | Duration/end |
| --- | ---: |
| Provisional `post_live` platform metadata used by this admission | 13,300,000 ms |
| Acquired provisional parent media | 13,319,833 ms |
| Normalized tail audio | 719,825 ms, reaching nominal source 13,319,825 ms |
| Last machine ASR segment | local end 721,480 ms |

A later finalized platform observation reported 13,330,000 ms and a separately
retained finalized rendition probed at 13,329,903 ms. Those later values do not
retroactively change this admission's immutable provisional-parent coordinates; a
separate calibration receipt records the shared-prefix and added-tail candidates.

The last segment therefore exceeds its audio request by **1,655 ms**. Adding the
unreviewed source offset produces 13,321,480 ms, **1,647 ms** beyond the local-window
source contract. Both values are preserved; neither is clipped to the audio, source,
or platform duration.

The homogeneous eight-window v1.8.7 audit also retains a 20 ms overrun in window 5
and a 9,620 ms overrun in window 6. These are raw engine boundary outputs, not claims
that speech exists beyond the media. They remain machine-review signals in local time.

## Transform hypotheses are not mappings

Where acquired-media and declared-recording durations disagree, the bridge creates
two immutable review candidates:

1. zero-offset identity using the acquired-media duration; and
2. linear endpoint scaling to the declared platform duration.

These are deliberately competing hypotheses, not evidence that either transform is
correct. Each has its own open human review task. They cannot update a rendition,
recording source, or timeline, and a generic review decision does not apply them. A
future promotion tool must require direct boundary/media review, select or correct a
transform, translate every segment and non-null word endpoint, and create a new
recording-scoped revision without modifying this immutable local transcript.

## Search the private lane

SQLite FTS5 indexes the private segment text while the authoritative rows retain the
coordinates and exact producer IDs:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  search-rendition-local-transcripts \
  --db research/corpus/corpus-v8.sqlite3 \
  --query 'Pia arcade' \
  --recording-id rec_... \
  --limit 25
```

Every search hit returns rendition-local and uncalibrated source coordinates. Its
`recording_start_ms` and `recording_end_ms` are always null. Search output contains
private machine transcript wording and belongs only in the ignored research workflow,
never under `src/`, `public/`, static corpus release data, CI artifacts, or logs.
