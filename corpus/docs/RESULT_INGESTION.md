# Result-envelope ingestion

Acquisition, preprocessing, offline ASR, sparse-frame, and fingerprint stages write
durable JSON envelopes but never modify the catalog directly. The catalog accepts only
completed, non-dry-run version 1 envelopes through stage-specific transactional
commands:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-acquisition-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/acquired/jobs/<job>/<recipe>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-preprocess-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/processed/media/sha256/<prefix>/<sha>/recipes/<recipe>/executions/<run>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-asr-whispercpp-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/asr/whispercpp/sha256/<prefix>/<sha>/results/<key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-sparse-frame-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/vision/sparse-frames/sha256/<prefix>/<sha>/results/<key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-audio-fingerprint-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/fingerprints/.../executions/<run>/result.json

PYTHONPATH=corpus/src python -m himr_corpus import-audio-fingerprint-compare-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/fingerprint-comparisons/.../executions/<run>/result.json
```

The corresponding read-only check rehashes every input and artifact and verifies all
cross-object invariants without opening a database:

```sh
PYTHONPATH=corpus/src python -m himr_corpus validate-asr-whispercpp-result \
  --result /durable/private/asr/whispercpp/sha256/<prefix>/<sha>/results/<key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus validate-sparse-frame-result \
  --result /durable/private/vision/sparse-frames/sha256/<prefix>/<sha>/results/<key>/result.json

PYTHONPATH=corpus/src python -m himr_corpus validate-audio-fingerprint-result \
  --result /durable/private/fingerprints/.../executions/<run>/result.json

PYTHONPATH=corpus/src python -m himr_corpus validate-audio-fingerprint-compare-result \
  --result /durable/private/fingerprint-comparisons/.../executions/<run>/result.json
```

## Private local-acquisition policy

`local_file` acquisition results must declare `source.access_state=unknown`; possession
of a local copy does not establish public access. A result with a `handling_policy`
must carry the exact same four-field policy at the result top level, in its source
metadata observation, and in the hash-bound originating work order. The catalog
importer rejects omission, weakening, extra metadata, or disagreement.

Before importing such a result, use the non-mutating
`himr_corpus.private_acquisition plan` helper to produce a portable-path review plan.
After the listed directory and file modes have separately been made exactly `0700` and
`0600`, respectively, `validate` reopens all three artifacts through no-follow file
descriptors and emits a receipt. Supply that receipt at admission:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-acquisition-result \
  --db research/corpus/corpus.sqlite3 \
  --result /durable/private/acquisition-job/admitted/result.json \
  --private-artifact-root /durable/private/acquisition-job \
  --private-seal-receipt /durable/private/review/seal-receipt.json
```

The receipt binds the exact work order, raw and canonical result, admitted media bytes,
producer source identity, and handling policy. Paths inside it are relative to the
explicit root, and it records `source_byte_identity_claimed=false`; equality between an
admitted local copy and bytes once served by a named remote source is outside this
contract. Receipt replay rechecks current descriptors, bytes, and modes, so the receipt
is not transferable publication authority.

Migration 0030 records the restriction in an append-only ledger and computes an
effective restriction across linked sources, parent/derived media, recordings, and
transcript revisions. It blocks new publish and gate-clear decisions for either
`never_publish` or `no_publication_authority`, removes restricted objects from
publication eligibility, and makes validation/export fail closed on conflicts. Legacy
catalog rows remain readable; publication/export code fails closed if the restriction
schema is unavailable. Migration installation is a separate reviewed operation and is
not performed by result import.

## Archive.org metadata snapshot handoff

Archive.org metadata snapshots are a separate metadata-only input, not media result
envelopes. Keep the complete sealed tree under ignored `research/` paths, validate it,
and then import it into the private catalog:

```sh
himr-corpus validate-archive-metadata-snapshot \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"

himr-corpus import-archive-metadata-snapshot \
  --db "$PWD/research/corpus/corpus.sqlite3" \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
```

The lane requests only the public unauthenticated
`/metadata/<identifier>` endpoint; it downloads no media and sends no cookies or
authorization. The boundary revalidates the exact request, snapshot, and provider
payload bytes, but every admitted field remains unreviewed and supplies no content,
identity, or publication authority. See
[Public Archive.org item-metadata snapshots](../../acquisition/ARCHIVE_ORG_METADATA.md).

An already admitted snapshot can be inspected for strict terminal bracketed
YouTube IDs with `plan-archive-bracket-reconciliation`, then admitted to private
candidate/review tables with `import-archive-bracket-reconciliation`. This lane
does not merge, attach, or relate recordings. Conflicting filename/title IDs fail
closed into an issue task. See
[`ARCHIVE_BRACKET_RECONCILIATION.md`](ARCHIVE_BRACKET_RECONCILIATION.md).

The fixed `1q2sk8g` contributor-complement gap has a separate read-only planner and
candidate-only importer. It preserves the 221 URL-hint sources, binds them to the
later sealed provider snapshot, and creates no source relation, external-ID copy,
merge, decision, or public row. See
[`ARCHIVE_HINT_PROVIDER_RECONCILIATION.md`](ARCHIVE_HINT_PROVIDER_RECONCILIATION.md).

## Admission rules

The producer JSON Schemas reject unknown properties throughout the versioned result
objects and type every stable envelope and catalog-row field. The Python catalog
boundary independently repeats exact-key/type checks and adds constraints JSON Schema
cannot enforce: terminal status, timestamp ordering, deterministic identifiers,
cross-row identity, mirrored objects, current file bytes, and deny-by-default
publication state. It rejects nonterminal or dry-run output, nonempty errors, naive
timestamps, malformed digests, non-private artifacts, and any disagreement among the
input, admission, probe, artifact, media, location, derivation, or routing identities.

Declared hashes are not accepted on faith. Immediately before the transaction, the
importer opens every acquisition payload, preprocessing input, and preprocessing
artifact as a regular local file; compares its stat identity and byte count; streams
SHA-256 over the current bytes; and stats the descriptor and path again to detect an
in-place change or path replacement. Missing, tampered, or mutating files are rejected,
so a `verified` media row is never created from envelope metadata alone.

Preprocessing results and artifacts must additionally be sealed, non-symlink regular
files in the exact source/recipe/execution tree. The importer recomputes deterministic
recipe identity from parameters and executable/build provenance while separately
recomputing the unique run ID from the source, recipe, and execution nonce. For a
replay it rehashes the referenced immutable prior result and all prior artifacts,
requires an identical artifact set, and verifies source-probe and routing JSON bytes
against their mirrored envelope/catalog objects. A replay is a distinct auditable run,
not an overwrite of the first execution.

The ASR importer performs the same race-aware byte verification for the normalized
audio input, pinned whisper executable, pinned model, optional neutral glossary, raw
whisper JSON, and normalized transcript JSON. Local paths must already be resolved;
artifact URIs must be canonical `file:` URIs without a remote host, query, fragment,
symlink alias, or dot traversal. It then compares the normalized artifact to the
envelope and every preserved raw segment/token object and engine-metadata object back
to the raw whisper artifact. A transcript cannot be admitted from catalog rows alone.

One envelope is one SQLite transaction. A validation or constraint failure leaves no
partial import batch, source, media, run, artifact, job, rendition, or observation.
The import-batch digest is computed from canonical JSON, so whitespace and object-key
ordering do not change its identity. Reimporting the same result is idempotent.

An exact replay of an already completed preprocessing envelope has a stricter,
read-only path. After revalidating and rehashing the sealed result, current input,
current artifacts, and any declared prior-result lineage, the importer locates the
deterministic `(importer, canonical result digest)` batch and verifies its complete
catalog footprint with `SELECT` statements only. This includes the batch and exact
statistics, processing run and inputs, media and locations, derivations, private
artifacts, logical job and deterministic attempt, producer external ID, derived
renditions, and routing observations and scores. Pre-existing acquisition metadata
and earlier media timestamps remain valid according to the original enrichment
rules; a verified reuse execution may retain its declared prior run as the owner of
an already cataloged content derivation.

A missing, extra, or different result-owned row is corruption or drift, not an
invitation to repair with `INSERT OR IGNORE`. A missing batch with the deterministic
run still present, a non-completed batch, or conflicting lineage also fails closed.
On an exact match the importer returns the same public result object without opening
a write transaction, renumbering job attempts, changing `total_changes`, modifying
the SQLite file, or appending a WAL record. Its read transaction pins one coherent
catalog snapshot. First-time preprocessing imports retain the single-transaction
behavior described above.

## Identifier boundary

Acquisition's `catalog_records.sources[].source_id` is producer-local. The importer
validates every internal producer reference and then maps the source to the catalog's
UUIDv5 identity derived from `(platform, source_kind, native_id)`. The producer ID is
retained as an `external_ids` value. Exact media bytes use the shared
`media_sha256_<sha256>` identity. Preprocessing recipe IDs are deterministic;
processing-run IDs are unique per execution. Both processing-run and run-scoped
artifact IDs are preserved only after their respective consistency checks.

The ASR importer additionally recomputes the recipe digest/ID, result key, run-input
ID, artifact IDs, transcript revision ID, segment IDs, and word IDs from their exact
semantic inputs. The input media and normalized-audio artifact must already exist and
match their exact bytes and parent preprocessing run. The model must already be a
registry row with `task: asr` and matching name, revision, weights hash, license label,
and `configuration_json.source`. An optional glossary revision must already match its
exact SHA-256 and artifact URI. Neither registry entry is invented from a filename or
producer assertion.

Use the checksummed, append-only workflow in
[Private model-registry manifests](MODEL_REGISTRY.md); direct ad-hoc model insertion is
not an accepted operator procedure.

The job table represents one logical `(stage, target_type, target_id)` job. Therefore,
the catalog derives the job ID from that tuple and preserves the work-order `job_id` as
an external processing-run identifier. A deterministic job-attempt row connects each
completed processing run.

## Time semantics

- Acquisition completion is the retrieval event. It supplies both the first admitted
  `media_objects.first_cataloged_at` and `media_sources.retrieved_at` for a new object.
- A repeated import can move `first_cataloged_at` earlier, never later. Multiple
  source/media retrieval claims retain the earliest retrieval and its tool provenance.
- Preprocessing never invents an acquisition time. It must report
  `acquisition_timestamp_state: not_claimed_by_preprocessing`; its source media uses
  the upstream first-cataloged time when supplied, otherwise the preprocessing
  observation time.
- Run, retrieval, verification, and catalog times must be timezone-aware RFC 3339 and
  are normalized to UTC.

## Renditions and routing observations

If an acquired source already has a non-rejected `recording_sources` mapping, its exact
bytes become an unreviewed `acquired_source_media` rendition. Preprocessed audio and
proxy media become unreviewed derived renditions for the same recording. If no reviewed
or candidate recording mapping exists, ingestion keeps the media and reports the
number of deferred routing observations; it does not fabricate a recording identity.

Scene changes and silence intervals are workload-routing candidates, not content
findings. When recording/rendition context exists, they enter `observations` as private,
machine-generated intervals. Raw scene scores remain uncalibrated
`observation_scores`; no speaker, action, identity, or event assertion is inferred.

## ASR context and machine revisions

`catalog_context: null` imports only the completed run, normalized-audio run input,
two private JSON artifacts, job provenance, and import ledger. It creates no recording,
rendition, transcript revision, or placeholder identity.

When context is present, the recording must already exist. A supplied rendition must
belong to that recording and exact input media. Only then are the producer's exact
`raw_asr` or `contextual_asr` revision, segments, and token rows inserted. All remain
`review_state: machine`; speaker labels, normalized text, confidence bands, alignment
scores, and calibrated probabilities remain null. Raw token probability is retained
only as its uncalibrated log probability. Zero-length token spans are valid because
whisper.cpp emits them for control or collapsed tokens. A segment may exceed the
requested end by at most the producer contract's 30-second boundary allowance, and
the exact segment/transcript overrun flags must be present.

A contextual result whose input artifact or rendition carries local-window lineage is
rejected before the import ledger is opened. The ASR producer reports artifact-local
milliseconds, while transcript rows are recording-scoped; `source_time_mapping` is
not a recording transform. An unknown, estimated, partial, scaled, or absent catalogue
timeline therefore cannot authorize ordinary transcript insertion. The separate
`rendition_local_*` bridge can admit and FTS-index exact machine text in
`rendition_media_ms` while retaining null recording coordinates, uncalibrated source
offset arithmetic, and separately reviewed transform hypotheses. It never writes a
normal transcript or timeline row. See
[Private rendition-local ASR admission and search](RENDITION_LOCAL_ASR.md). Private
artifact-local ASR can also still be imported with `catalog_context: null` when only
run/artifact provenance is wanted. Legacy non-local/full-media contextual results,
including zero-offset full-media results, remain compatible.

For exact process-routed short clips in the sealed preprocess queue, migration 0021
also provides a separately searchable `media_local_*` lane. It binds every revision
to exact normalized media/artifact and preprocess admission while leaving all
recording, rendition, source, and translated coordinates null. It preserves engine
endpoint overruns and valid empty results without clipping. See
[Private media-local ASR admission and search](MEDIA_LOCAL_ASR.md).

The retained `gPhrE99xwqI` batch demonstrates the hazard. Its eight work orders all
start ASR at artifact time zero, but the source windows begin at 0, 1,800,000,
3,600,000, 5,400,000, 7,200,000, 9,000,000, 10,800,000, and 12,600,000 ms. Their
catalogue timeline rows have null recording endpoints and `mapping_kind: unknown`.
Copying producer timestamps unchanged would place all eight transcripts near
recording time zero, so contextual import is intentionally refused.

## Audio-fingerprint context and candidate relations

The fingerprint importer rehashes the sealed normalized-audio input, pinned FFmpeg
executable, every raw fingerprint artifact, and the sealed result. It recomputes the
recipe, execution, fingerprint, artifact, observation, score, and match-candidate IDs;
checks deterministic half-open window expansion and exact `atrim` commands; and
requires each raw artifact to occupy its declared execution/window/digest path.

With `catalog_context: null`, only processing-run provenance, its input, and private
artifacts are inserted. With context, the exact recording/rendition/media chain must
already exist before private `audio_fingerprint` observations and raw word-count
scores are admitted. Raw scores have no calibration set or calibrated probability.

The comparison importer dispatches the immutable `exact_raw_bytes_v1` contract or the
separate sealed-envelope `exact_raw_bytes_v2` contract by schema version. V1 retains
its historical same recipe-qualified implementation requirement. V2 independently
revalidates both exact extraction-result envelopes and current evidence, permits their
input/recipe IDs to differ, and requires the same engine/build, algorithm, raw format,
sample rate, and channels. Both extraction receipts, fingerprints, private artifact
rows, machine observations, and recording/rendition/media lineages must already agree
before v2 admission.

V2 catalog admission is relational rather than permissive JSON interpretation.
`audio_fingerprint_compare_v2_receipts` binds the sealed comparison result and its
single exact-comparison ledger receipt; two role-keyed
`audio_fingerprint_compare_v2_sides` rows bind the extraction result files, recipes,
runs, canonical contexts, normalized inputs, engines/build evidence, selected raw
artifacts, byte counts, and fingerprint configuration. Trusted comparison JSON must
equal deterministic compact SQL JSON; producer run JSON is exact-bound to typed
columns and any duplicate object key is rejected. The importer inserts the generic
receipt before these bindings and the subtype last, in one transaction.

After subtype admission, pair-scoped triggers reject new or changed run inputs,
artifacts, observations, typed observations, fingerprints, scores, receipts, and
referenced media/rendition lineage. SQL enforces exact graph cardinalities and
`raw_score = 1` exactly when both selected artifact SHA-256 and byte count pairs are
equal. SQLite cannot prove that a local URI currently exists or recompute SHA-256;
the importer and `validate` therefore resolve, stat, and stream-hash the sealed result,
input, raw-artifact, and engine paths. Migration 0017 deliberately aborts if any row
was admitted by the provisional v2 schema because those rows cannot be safely
backfilled into the complete typed evidence contract.

Both versions insert only a `match_candidates` relation with
`decision_state: candidate` and an append-only restrictive subtype. The v2 subtype
also binds both extraction result SHA-256 values and fixes calibration to
`not_calibrated`, visibility to `private`, and publication authority to `none`.
Cross-recording/input/recipe, duration, and short/empty-window quality flags are
preserved. No `recording_relations` row, merge, duplicate claim, parent claim, or
publication decision is created. See
[`pipeline/AUDIO_FINGERPRINT.md`](../../pipeline/AUDIO_FINGERPRINT.md) and
[ADR 0005](../../docs/adr/0005-audio-fingerprint-candidates.md).

## Sparse-frame routing context

The sparse-frame importer admits only completed `sparse_frame_router` v1 output. It
rehashes the sealed result, preprocess handoff, CFR proxy, FFmpeg executable, and every
PNG, then independently recomputes selection, recipe/result/run identities, exact
PTS/time-base arithmetic, artifact/frame identities, and command provenance. The
already-imported preprocess run, proxy artifact/media/location/derivation, and proxy
rendition-to-source-rendition lineage must all agree.

Frames become private `sparse_frame_routing_candidate` machine observations in proxy-
rendition media coordinates. Their route remains `not_evaluated`, text presence stays
`unknown`, and no calibrated score is created. Admission never writes OCR text, face
or identity data, content claims, or publication decisions. See
[Private sparse-frame result ingestion](SPARSE_FRAME_RESULT_INGESTION.md) for the full
trust boundary.

## Private Tesseract OCR context

Migration 0033 admits only completed `ocr_tesseract_tsv` v1 output whose exact
sparse-frame result is already present. The importer replays all sealed JSON, PNG, and
TSV bytes; source/rendition lineage; PTS/time-base coordinates; pinned engine/model
files; command arrays; TSV words, geometry, raw score lexemes, and producer IDs.

Each word becomes a private `ocr_tesseract_word_candidate` in half-open
`rendition_media_ms`. The interval is local to the low-resolution proxy named by
`rendition_id`/`media_id`; source-rendition/media columns are provenance anchors and
do not assert original-source or recording timeline coordinates. Raw 0–100 Tesseract
scores remain `not_calibrated` and
`not_a_probability`; calibrated probability is null. Text stays redaction-pending and
human-review-required in a private FTS table. Exact replay reconstructs and compares
the complete result-owned catalog footprint, including run/input/artifact provenance;
the FTS cache must pass a table-scoped checksum as well as an exact visible-row
projection. SQLite 3.44.0 or newer is mandatory because older runtimes do not invoke
FTS5's native integrity check from table-scoped read-only integrity checks. Every
private search also rehashes and exactly replays all current receipts before returning
raw text. Append-only replacement and capacity guards seal admitted provenance even
when recursive trigger execution is disabled. Database triggers cover
reserved and generic observation/artifact publication labels and forbid identity,
event, claim, publication, gate, and export authority even before the word ledger is
inserted. See
[Private Tesseract OCR admission and search](OCR_TESSERACT_RESULT_INGESTION.md).

## Sparse visual-fingerprint context

The visual-fingerprint importer accepts only a completed, non-dry-run
`visual_fingerprint_extract` v1 envelope. It independently reconstructs the exact
work-order digest, recipe, immutable result layout, FFmpeg commands, result/run IDs,
sample-to-frame mapping, decoded rational PTS and time base, drift, artifact and
fingerprint IDs, and aggregate quality flags. It rehashes the sealed result, video
input, current producer implementation, pinned FFmpeg executable, and each private
1,024-byte gray artifact both before and inside the catalog transaction. Each pHash
is recomputed with the separately committed signed-Q20 DCT matrix.

The input media, local location, parent processing run, and private upstream artifact
must already agree byte-for-byte with the catalog. With `catalog_context: null`, only
the completed run, two exact run inputs, private gray artifacts, and append-only
result receipt enter the catalog. No fingerprint or observation is inferred. With
context, the rendition must belong to the named recording and exact input media.
Only then does the importer create private `visual_fingerprint` observations and
their typed timing/pHash rows.

Every typed row is fixed to `calibration_state: not_calibrated`, null calibrated
probability, and `requires_human_review = 1`. Identity, duplicate, and relationship
assertions are fixed false. The raw Hamming distance is therefore a future review
routing measurement, never a probability or proof. Extraction import creates no
`match_candidates`, `recording_relations`, identity rows, review acceptance,
publication decision, or gate clearance. See
[`pipeline/VISUAL_FINGERPRINT.md`](../../pipeline/VISUAL_FINGERPRINT.md) and
[ADR 0008](../../docs/adr/0008-sparse-visual-fingerprint-candidates.md).

The separate comparison importer accepts only completed
`visual_fingerprint_compare` v1 envelopes whose two extraction envelopes have already
been admitted with the exact non-null recording/rendition contexts named by the
comparison. It revalidates and rehashes the comparison, both full extraction graphs,
the producer implementation, every selected gray artifact, and every pHash. It then
reconstructs the work order, immutable pair layout, recipe/result/run/candidate IDs,
all pairwise Hamming distances, exact-gray equality, deterministic ordering, bounded
top list, threshold state, and summary counts.

Admission writes a private append-only receipt, two typed sides, the selected-frame
set, raw comparison summary, top pairs, and a final completion receipt. A result with
`candidate_emitted: true` also receives one private generic match candidate and one
open direct-media review task. A result with `candidate_emitted: false` receives
neither. `below_configured_threshold` is preserved verbatim and is not translated to
generic `rejected`, an `unrelated` assertion, or a recording relation. Both outcomes
retain null calibrated probability, mandatory human review, all six producer
assertions fixed false, private visibility, and publication authority `none`.

## Publication boundary

These importers create zero publication decisions. Artifacts and routing observations
are forced private, renditions stay unreviewed, and local paths/storage URIs are used
only by private catalog tables. The static release exporter reads allowlisted public
views and has no query path to media locations, artifacts, jobs, or observations.

The acquisition/preprocessing end-to-end test runs a short local source through both
real producers, executes a verified preprocessing replay, imports every durable result
twice, verifies ID translation, unique execution identity, reuse lineage, and time
semantics, and checks that the public projection contains no local path. It validates
both producers' planned and completed synthetic envelopes against their JSON Schemas
before catalog admission:

```sh
PYTHONPATH=corpus/src python -m unittest corpus.tests.test_result_importers -v
```

The ASR importer integration test runs the current `asr_whispercpp.py` contract with
a local fake engine and real normalized FLAC. It covers current 0.3.0 results, exact
0.2.3 timing-anomaly compatibility, the legacy 0.2.2 normalized-token shape,
idempotency, absent context, exact registries,
raw/normalized tampering, unsafe URIs, unknown fields, mirrored-array mismatches,
zero-length token spans, provenance-preserving inverted upstream offsets with null
catalog word timing, bounded-window overrun flags, zero-offset full-media context,
provenance-only local-window null context, and no-write rejection of both the real
unknown-timeline metadata shape and a future exact non-identity span:

```sh
PYTHONPATH=corpus/src python -m unittest corpus.tests.test_asr_result_importer -v
```

The audio-fingerprint integration tests use real local FFmpeg/Chromaprint output and
cover full/window/chunk extraction, half-open boundaries, immutable paths, historical
v1 comparison, cross-recording/different-recipe v2 comparison, mismatched engines,
sealed-envelope/artifact/side tampering, unsafe paths, missing receipts and lineage,
idempotency, null-context provenance, and mandatory private uncalibrated
human-review/no-authority candidates:

```sh
python3 -m unittest pipeline.tests.test_audio_fingerprint -v
PYTHONPATH=corpus/src python3 -m unittest \
  corpus.tests.test_fingerprint_result_importer -v
```

The visual-fingerprint admission tests use real local FFmpeg output and cover exact
pHash recomputation, decoded timing, extraction and comparison idempotency,
provenance-only extraction context, mandatory two-sided comparison context, threshold
pass and below-threshold semantics, pair recomputation, transaction rollback,
artifact/result/URI tampering, append-only database guards, review-task preservation,
and the absence of identity, duplicate, relation, unrelated, and publication
decisions:

```sh
PYTHONPATH=corpus/src python3 -m unittest \
  corpus.tests.test_visual_fingerprint_result_importer -v
```

Sparse-frame admission uses a real acquisition → preprocessing → frame-extraction
fixture and covers exact byte revalidation, private/idempotent catalog writes,
timestamp forgery, artifact tampering, missing rendition lineage, and transaction
rollback:

```sh
PYTHONPATH=corpus/src python3 -m unittest \
  corpus.tests.test_sparse_frame_result_importer -v
```

Schema authors and CI install the validator separately from runtime dependencies:

```sh
python3 -m pip install -r scripts/requirements-json-contracts.txt
python3 scripts/validate-json-contracts.py
python3 scripts/validate-json-contracts.py \
  --validate acquisition/schemas/result.schema.json /private/result.json
```

The validator checks Draft 2020-12 schema validity, unique schema identifiers, exact
object definitions, all tracked work-order/glossary examples, and any generated
instances passed with repeatable `--validate` arguments.

## Contract limits

Standard JSON Schema can constrain each field but cannot state that a media ID is the
concatenation of a prefix and a sibling SHA-256, recompute stable IDs from semantic
inputs, hash the file named by a path, compare timestamps, require two nested objects
to be byte-for-byte mirrors, or reconcile cross-array foreign keys and routing totals.
It also cannot require uniqueness by one property within an array. Those invariants
remain mandatory and are enforced by the independent Python admission boundary; the
schema is not a substitute for catalog validation or storage-integrity verification.
