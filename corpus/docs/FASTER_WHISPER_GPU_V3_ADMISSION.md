# Private faster-whisper GPU v3 admission

Migration 0034 admits a completed, sealed `faster-whisper` GPU v3 result into the
existing private media-local transcript lane. The importer does not run inference.
It replays the canonical result, work order, normalized and raw transcript
artifacts, exact input media and catalog artifact, model/runtime lineage, and—when
provided—the resident-batch manifest and completion receipt.

The input media and its exact artifact must already exist in the private catalog.
Admission creates no source or recording coordinates, speaker assignment, identity,
biometric match, event evidence, wiki authority, or publication authority. Segment
and word times remain half-open `media_ms` coordinates relative to the admitted
audio file.

## Administrative sequence

Install pending migrations only on an explicitly approved writable catalog copy:

```sh
PYTHONPATH=corpus/src python -m himr_corpus migrate \
  --db /ABSOLUTE/PRIVATE/corpus.sqlite3
```

Then build a read-only, transcript-text-free admission plan:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  plan-faster-whisper-gpu-v3-admission \
  --db /ABSOLUTE/PRIVATE/corpus.sqlite3 \
  --result /ABSOLUTE/PRIVATE/asr/faster-whisper-gpu/sha256/.../result.json \
  --work-order /ABSOLUTE/PRIVATE/work-orders/.../work-order.json \
  --batch-completion /ABSOLUTE/PRIVATE/batches/.../completion/receipt.json \
  > /tmp/gpu-v3-admission-plan.json
```

Supply `--batch-completion` only when that exact receipt exists. A validated
completion, manifest, and member chain records `execution_mode: batch`. Without the
option, the ordinary v3 result cannot distinguish standalone from resident-batch
execution, so the plan records `execution_mode: unasserted`; omission never asserts
standalone provenance.

Review the returned lineage, counts, timing-anomaly totals, authority fields, and
`plan_sha256`. Import repeats all physical and semantic checks inside one transaction
and requires the separately reviewed digest:

```sh
plan_sha=$(python -c \
  'import json,sys; print(json.load(sys.stdin)["plan_sha256"])' \
  < /tmp/gpu-v3-admission-plan.json)

PYTHONPATH=corpus/src python -m himr_corpus \
  import-faster-whisper-gpu-v3-result \
  --db /ABSOLUTE/PRIVATE/corpus.sqlite3 \
  --result /ABSOLUTE/PRIVATE/asr/faster-whisper-gpu/sha256/.../result.json \
  --work-order /ABSOLUTE/PRIVATE/work-orders/.../work-order.json \
  --batch-completion /ABSOLUTE/PRIVATE/batches/.../completion/receipt.json \
  --expected-plan-sha256 "$plan_sha"

PYTHONPATH=corpus/src python -m himr_corpus validate \
  --db /ABSOLUTE/PRIVATE/corpus.sqlite3
```

Use the identical option set for planning and importing. Replay is idempotent only
when every authoritative row still matches.

## Transcript and timing semantics

- Zero-segment and zero-word results are valid and create no FTS rows.
- The current half-open segment schema requires `end_ms > start_ms`. A producer
  segment with zero duration is rejected explicitly; the importer never invents a
  duration or silently drops it.
- Raw word probabilities and segment scores are retained as machine output. They are
  labeled uncalibrated and are not confidence probabilities.
- Timing clipping and anomaly flags remain attached to the private rows for later
  review.
- The plan deliberately omits transcript wording. The admitted text remains private,
  machine-generated, and human-review-required; this lane has no public export path.

Search uses the existing isolated media-local FTS boundary:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  search-media-local-transcripts \
  --db /ABSOLUTE/PRIVATE/corpus.sqlite3 \
  --query '<private query>' \
  --media-id media_sha256_<sha256> \
  --limit 25
```

Search results retain media-local coordinates. They do not imply a recording time,
source time, speaker identity, event, allegation, or publishable quotation.
