# Reviewed full-file media-local transcript projection

Migration 0029 is the narrow bridge from private `media_ms` hypotheses to ordinary
recording-coordinate transcript revisions. It copies complete raw/contextual machine
pairs without changing text or integer timestamps. Review authority in this lane is
limited to the pinned media lineage and the identity coordinate transform. It does
not review wording, identify speakers, claim accuracy or improvement, choose a
preferred hypothesis, clear a publication gate, or publish.

## Exact admission boundary

Policy `media_local_full_file_identity_v1` requires three to five recordings in one
closed manifest. Every recording contributes exactly one admitted `raw_asr` revision,
one admitted `contextual_asr` revision, their no-preference pair, and its private diff
receipt. Both revisions must use the same full normalized input, have a segment, have
zero producer boundary overrun, and preserve null and zero-length word timings.

The coordinate policy is intentionally asymmetric:

- normalized input and acquired parent media durations must be exactly equal;
- normalized input and recording-duration metadata may differ by at most 5 ms;
- the transform itself is exactly `recording_ms=media_ms` on half-open intervals;
- rescaling, interpolation, clipping, padding, and endpoint repair are forbidden;
- every segment and timed word must fit inside the minimum of all three durations.

The manifest pins the recording and canonical-key snapshot, source,
`media_sources` and `recording_sources` rows, normalized and parent media, the parent
video's rendition, all three duration snapshots, both source revisions and import
receipts, and pair/diff IDs. It also pins the exact append-only rank-700
`acquisition_result_v1` source-metadata observation and completed import-observation
receipt that said the source was public at review time. Current
`sources.access_state` is an additional new-plan/apply gate, not the historical
receipt. Automatic "best source" selection is not allowed.

The selected direct `media_sources` row must have a null `source_snapshot_id` in this
v1 lane. A non-null snapshot would add another mutable provenance object that this
policy does not attest; such candidates need a future policy that pins and seals that
snapshot and its import receipt.

The normalized input must be an `audio`/`flac` media object whose selected
normalization derivation declares 16 kHz mono output and whose parent is video. The
manifest also closes over the exact private `audio_16khz_mono_flac` artifact, its
completed `media_preprocess` producer run, and the completed
`media_preprocess_result_v1` import receipt. It digests the producer's full ordered
run-input and artifact sets and requires its one exact `source_media` input to name
the parent video and digest. Each source revision must come from a completed,
error-free `asr_whispercpp` run and a completed variant-specific import receipt.
All source/preprocess runs and receipts must predate coordinate review. These
producer/import receipts are provenance, not merely duration hints.

For each already chosen `source_id`, enumerate eligible immutable public receipts
read-only and pin one exact row; do not copy `sources.current_metadata_observation_id`
without reviewing the receipt:

```sql
SELECT observation.source_metadata_observation_id,
       observation.import_observation_id,
       observation.observed_at,
       observation.import_batch_id,
       batch.input_sha256
FROM source_metadata_observations AS observation
JOIN import_batches AS batch
  ON batch.import_batch_id = observation.import_batch_id
JOIN import_observations AS receipt
  ON receipt.import_observation_id = observation.import_observation_id
WHERE observation.source_id = :source_id
  AND observation.quality_rank = 700
  AND observation.access_state = 'public'
  AND observation.review_state = 'metadata_only'
  AND batch.importer_name = 'acquisition_result_v1'
  AND batch.status = 'completed'
  AND receipt.status = 'completed'
ORDER BY julianday(observation.observed_at) DESC,
         observation.source_metadata_observation_id DESC;
```

The exact ASR input is normalized audio. At admission, the current vertical-slice
inputs have no rendition and the parent video has the sole rendition. Consequently
each generated
`transcript_revisions.rendition_id` is `NULL`. The projection receipt records
`normalized_media_id` and `parent_rendition_id` separately. A parent-video rendition
must never be presented as the media that ASR transcribed. Later cataloging another
rendition does not rewrite that historical receipt or change the target's null
`rendition_id`.

## Manifest and review

Use the strict JSON contract at
[`schemas/media-local-transcript-projection-manifest.schema.json`](../schemas/media-local-transcript-projection-manifest.schema.json).
The loader rejects duplicate keys, unknown fields, non-finite values, unstable files,
unsafe identifiers, future review times, repeated pinned IDs, and selections outside
three to five recordings. The reviewer must already be a governed human whose event
stream says they were active at `reviewed_at`; this workflow never creates or
activates a reviewer.

Start with `review.approved_plan_sha256` set to `null`. Plan against a disposable,
current review database:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  plan-media-local-transcript-projection \
  --db /tmp/corpus-review.sqlite3 \
  --manifest /absolute/path/projection-manifest.json
```

The plan contains no transcript wording. It binds the manifest core, catalog IDs,
durations, counts, exact source receipt digests, input-artifact/preprocess provenance,
complete source transcript/run/input digests, pair/diff digest, deterministic output
IDs, generated run/revision/projection payload digest, and safety flags. Inspect the
lineage and boundaries, then place the printed
`plan_sha256` in
`review.approved_plan_sha256`. The approved hash is excluded from the manifest-core
digest, avoiding digest self-reference.

Apply requires the same hash independently on the command line:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  apply-media-local-transcript-projection \
  --db /tmp/corpus-review.sqlite3 \
  --manifest /absolute/path/projection-manifest.json \
  --expected-plan-sha256 <reviewed-sha256>
```

Application rereads the stable manifest inside `BEGIN IMMEDIATE`. For a new batch,
the manifest approval, command-line hash, and recomputed current-catalog plan hash
must all agree. Deterministic processing runs, run inputs, ordinary revisions, segments,
words, projection receipts, and pair receipts are inserted first. Deferred foreign
keys allow the reviewed batch header to be inserted last; its trigger verifies the
complete 3--5-recording graph before commit.

The batch embeds the exact reviewed UTF-8 manifest text as `manifest_raw_json`, in
addition to its normalized JSON and raw/canonical/core digests. Deep replay hashes
that sealed copy and verifies its byte count, so `manifest_uri` is durable provenance
but not a portability dependency after application. Moving or deleting the original
manifest file cannot make a closed batch unverifiable.

## Append-only replay

`require_exact_projection_batch` is shared by apply idempotency and
`validate_database`. It compares exact child-ID sets rather than aggregate counts,
reconstructs every admitted raw/contextual receipt, immutable public-access
observation, selected lineage, and pair/diff graph, recomputes source digests, and
compares every projection, processing-run, run-input, target
revision, deterministic segment, word, and metadata value. Projection runs must have
one source-revision input and no artifacts; each source ASR run must have exactly one
`normalized_audio` media input matching the normalized media digest. Equal-count
substitutions, extra or missing sealed rows, or a changed source digest fail; replay
never repairs the database. An independent child-identity digest binds every generated
segment and word ID to its source ID, so generator drift cannot remain self-consistent
between insertion and replay.

Historical replay deliberately does not depend on mutable current projections. A
later append-only observation may change a source from public to removed; recording
duration or canonical-key metadata may be corrected; recording-source confidence may
become disputed; rendition labels/review metadata may change; and additional
derivations or renditions may be cataloged. Those changes do not invalidate a batch
that was valid when reviewed. Idempotent re-apply checks for the already-closed plan
before evaluating current eligibility.

The referenced content identity (`sha256`, byte count, kind, container and duration),
source platform/kind/native identity, exact selected `media_sources` and normalization
derivation rows, recording-source mapping geometry/method, and parent-rendition
media/recording identity are sealed after closure. Current availability, recording
metadata, mapping confidence/notes, media integrity/location state, and rendition
labels/review metadata remain mutable. Their reviewed values are snapshots in the
sealed manifest/plan, not assertions about the present.

SQL guards independently prohibit replacement, update, deletion, incomplete batch
closure, mutation of selected immutable lineage/acquisition and preprocess receipts,
adding source or target transcript children after receipt, adding projection run
inputs or artifacts, and publication decisions on projection evidence. Batch closure
also compares every copyable segment/word field and the exact coordinate-projection
metadata in both directions. It requires exact projection-run parameters/environment,
null seed/error state, and exact target/projection JSON; matching counts or a changed
helper cannot disguise substituted content or add an accuracy claim.
Later
publication or gate decisions may target the generated ordinary revisions through
their normal workflows, but none may predate projection.

## Publication remains separate

Projection creates no publication, gate, identity, preference, or lifecycle
decision. Each generated raw and contextual revision remains a competing machine
hypothesis. Source and recording eligibility and current human rights, privacy, and
sensitivity clearances are still required. Migration 0028 may then publish each
eligible revision with the exact machine-generated, unreviewed, non-quotation
warning; it does not convert coordinate review into wording review.

The Breakfast recording is outside this v1 lane: its acquired parent differs from
the normalized input by 1 ms, its recording duration differs by roughly 4,592 ms,
and its raw source overruns the input. It is not permanently denylisted; corrected
evidence or a future separately reviewed non-identity policy would require a new
append-only lane.
