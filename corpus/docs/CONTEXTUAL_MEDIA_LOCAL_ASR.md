# Private paired contextual media-local ASR

Migrations 0023 through 0025 provide a closed, private lane for the 17-result neutral-
glossary pilot. Each contextual transcript remains a separate machine hypothesis
beside one already admitted raw `media_local_transcript_revision`. The lane does not
choose wording, correct the raw pass, claim improved accuracy, claim human review, or
authorize publication.

The closed batch identity is
`ctxasrbatch_b696c14db13d7ae51a207e528c5e1d0e`. Admission deterministically replays
its manifest and work orders and accepts only the exact glossary revision
`glossary_himrverse_neutral_en_20260827_v1`. A pair must preserve the normalized
input, engine, model, full-input window, inference settings, and null catalog context.
Only the glossary, job, and output identities may differ.

## Two-stage administration

Register the exact private glossary before planning a contextual result. The plan
contains hashes, counts, and the revision label, but no glossary terms:

```sh
glossary="$PWD/research/corpus/private-admin/glossaries/himr-neutral-en-v1.json"
observed_at=2026-08-27T14:10:00Z

PYTHONPATH=corpus/src python -m himr_corpus \
  plan-private-glossary-registration \
  --glossary "$glossary" \
  --observed-at "$observed_at" > /tmp/private-glossary-plan.json

plan_sha=$(python -c \
  'import json,sys; print(json.load(sys.stdin)["plan_sha256"])' \
  < /tmp/private-glossary-plan.json)

PYTHONPATH=corpus/src python -m himr_corpus \
  import-private-glossary-registration \
  --db /tmp/corpus-contextual-review.sqlite3 \
  --glossary "$glossary" \
  --observed-at "$observed_at" \
  --expected-plan-sha256 "$plan_sha"
```

The registration records exact raw and canonical digests, byte count, revision and
prompt digests, language, and term count. Terms remain only in the ignored private
file. Plans and catalog rows use `urn:private:sha256:<digest>` references, never local
paths. Migration 0024 freezes the referenced legacy `glossary_revisions` provenance
row and rejects lookalike glossary or batch receipts. Migration 0025 binds every
glossary, manifest, work-order, result, diff, and contextual artifact reference to
its raw digest.

For one contextual result, supply its exact batch manifest and deterministic private
diff:

```sh
batch="$PWD/research/corpus/private-contextual-asr-work-orders/batches/\
ctxasrbatch_b696c14db13d7ae51a207e528c5e1d0e/manifest.json"
result=/absolute/private/contextual/result.json
diff=/absolute/private/contextual-diff.json

PYTHONPATH=corpus/src python -m himr_corpus \
  plan-contextual-media-local-asr-admission \
  --db /tmp/corpus-contextual-review.sqlite3 \
  --result "$result" \
  --batch-manifest "$batch" \
  --diff "$diff" > /tmp/contextual-admission-plan.json

plan_sha=$(python -c \
  'import json,sys; print(json.load(sys.stdin)["plan_sha256"])' \
  < /tmp/contextual-admission-plan.json)

PYTHONPATH=corpus/src python -m himr_corpus \
  import-contextual-media-local-asr-result \
  --db /tmp/corpus-contextual-review.sqlite3 \
  --result "$result" \
  --batch-manifest "$batch" \
  --diff "$diff" \
  --expected-plan-sha256 "$plan_sha"

PYTHONPATH=corpus/src python -m himr_corpus validate \
  --db /tmp/corpus-contextual-review.sqlite3
```

Use a disposable catalog copy for plan review and canaries. Import commands migrate
their target catalog before dispatch; a read-only preview should use the dedicated
`plan-*` command, which opens an already migrated catalog read-only.

## Retained evidence

The authoritative private rows are:

- `private_glossary_registrations` for the exact term-free registry receipt;
- `contextual_asr_batch_registrations` for the exact closed manifest;
- `contextual_media_local_asr_imports` for result/work-order provenance;
- `contextual_media_local_asr_pairs` for equality assertions and explicit
  no-preference state; and
- `contextual_asr_text_private_diffs` for text-free numeric comparison evidence.

Contextual segment and token wording is stored in the existing private
`media_local_transcript_segments` and `media_local_transcript_words` tables and their
private FTS index. It is never copied into the plan or diff receipt. Diff receipts
retain block counts, character edit distance, token/timing counts, drift maxima, and
the number of glossary-term metrics. They do not retain changed blocks, term labels,
or transcript strings. The complete deterministic diff remains an ignored private
file bound by opaque reference, byte count, raw digest, canonical digest, and internal
identity.

Both revisions use `media_ms`, half-open bounds, and null recording/source transform
state. No duration match is treated as a recording timestamp translation.

## Filesystem and review semantics

The importer revalidates current result, artifact, input, engine, model, glossary,
manifest, baseline, work order, and diff bytes. Opaque references resolve only
through the fixed ignored closed-pilot roots. Resolution fails on missing, drifted,
ambiguous, symlinked, multiply linked, or incorrectly permissioned private files.
Contextual result directories must retain the sealed mode-0500 three-file layout;
the result and both output artifacts must be mode 0400. The result receipt
deliberately says
`stable_hash_bound_no_seal_claim`: this catalog lane does not impersonate or depend
on a separate contextual result-store seal receipt. A future seal can be admitted by
a new append-only receipt without rewriting these observations.

The exact producer result remains the replay source. Its `processing_run`
`environment_json` is not copied verbatim: bridge v2 stores a canonical redacted
projection with CPU/network/version facts and hashes of the source environment,
parameters, logical commands, executed commands, and prompt. Raw argv, the prompt
string, and local paths remain only in ignored result files. Output artifact
`storage_uri` values are digest-addressed opaque references. Migration 0025 requires
that projection and freezes the exact graph at one normalized-audio input and two
private output artifacts once its contextual receipt exists.

Every plan and import repeats validation; imports are transactional and row-
idempotent. Database validation resolves and rebuilds each private diff, then compares
its identity, alignment, fail-closed policy, and every retained numeric projection; a
matching file hash alone is insufficient. Generic publication and publication-gate
decisions for glossary, batch,
import, pair, or diff objects are rejected. Existing media-local publication guards
also cover the contextual revision, segments, and words.

The machine-readable plan contracts are
`corpus/schemas/private-glossary-registration.schema.json` and
`corpus/schemas/contextual-media-local-asr-admission.schema.json`. Dedicated guard and
row-construction tests live in `corpus/tests/test_contextual_media_local_asr.py`.
