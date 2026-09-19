# Frozen transcript evaluation workflow

This module defines the Phase 3 evaluation boundary. Its optional ASR-blind
proposal helper prepares unreviewed review queues; the core workflow freezes what will be
evaluated **before anyone inspects the system output being evaluated**, binds every
interval to exact media bytes, and keeps human reference text private by default.
The freeze/reference validators remain independent of the catalogue database, ASR
runner, preprocessing runner, and public release exporter. The proposal helper has
a narrower read-only integration with sealed preprocessing metadata and the private
catalogue; it has no freeze, reference, write, or publication authority.

The runtime validator uses only the Python standard library. Draft 2020-12 JSON
Schemas provide independently checkable wire contracts; runtime checks add the
cross-file and arithmetic invariants JSON Schema cannot express.

## What is tracked now

[`cohorts/himr-asr-candidate-cohort-v1.json`](./cohorts/himr-asr-candidate-cohort-v1.json)
is a **candidate list, not a frozen reference set and not an availability claim**.
It deliberately preserves the 12 candidates' selection-time `unresolved` state.
Separate private catalog records now bind acquired bytes, duration, and rendition
lineage for all 12, but those records do not rewrite the candidate manifest or assert
current availability, publication rights, suitability, or reference acceptance. It
contains:

- `h3ySLeBAoXs`
- `0frp1tHu7ek`
- `s2OW-jRyFrw`
- `92LgEG6NhUw`
- `SlTpGmCrTxE`
- `8AbFGYob9SU`
- `UUqmpEOc5oc`
- `Z32Y-D5kJTg`
- `94ff_90_Dzs`
- `F_G3PXJL2AM`
- `tiMqrpC6ZZY`
- `fku-kaaUStw`

The members-only video `rKdcv4QvGig` is explicitly excluded. It must not be used as
a replacement candidate, reference source, or evaluation input.

## Order of operations

1. **Resolve candidates without looking at ASR output.** Acquire permitted public
   bytes, compute SHA-256 and byte count, record duration, and resolve the exact
   `recording_id`, `source_id`, `media_id`, `rendition_id`, and rendition kind.
   Duplicate media hashes across two recording candidates must be resolved before
   splitting; the validator rejects them.
2. **Choose intervals from source metadata and direct media only.** Use half-open
   `[start_ms, end_ms)` rendition-media coordinates. Intervals in one recording must
   be sorted and nonoverlapping. Record language tags, code switching, speaker
   overlap, playback speech, and noise for every interval.
3. **Split by recording.** Put each recording wholly in `calibration` or `scoring`.
   No interval from one recording may cross the boundary. Both splits must be
   nonempty. The calibration set is for thresholds, prompts, and model selection;
   the scoring references stay held out until the system and scoring procedure are
   locked.
4. **Recompute accounting and seal the freeze.** The manifest records exact total,
   split, and stratum durations and counts. Runtime validation recomputes all of
   them. Set `manifest_sha256` to the canonical digest printed by the CLI, then run
   `validate-freeze`. Any later byte change invalidates the seal and requires a new
   freeze ID and an explained protocol revision.
5. **Only now inspect/run the ASR systems under evaluation.** System output is never
   an input to interval selection or reference transcription.
6. **Create two independent human passes.** Pass A and pass B use different
   pseudonymous annotator IDs. Each annotator reviews the exact media directly and
   attests that they have not inspected ASR output, the other pass, or adjudication.
   Every frozen interval is transcribed once per pass.
7. **Adjudicate after both passes.** A third reviewer compares both passes, reviews
   the direct media, records the source utterance IDs used, and preserves genuine
   uncertainty instead of guessing. The adjudication binds both annotation
   manifest hashes and the exact freeze hash.
8. **Score separately.** A valid adjudication means a human reference exists; it
   does not itself compute or establish accuracy. Do not report WER, name accuracy,
   timing accuracy, calibration, or comparative quality until a scoring result is
   bound to this frozen reference and its metric implementation is recorded.

The selection attestation is an auditable statement, not a technical way to prove
what a reviewer saw. Enforce blinding operationally with access separation: the
selection reviewer should not receive ASR result paths, and the two transcribers
should receive separate work packages.

## Proposal-only routing helper

[`INTERVAL_PROPOSALS.md`](./INTERVAL_PROPOSALS.md) documents a deterministic
ASR-blind helper for two additive lanes: full-rendition inputs and grouped local
windows from oversized parent recordings. It consumes exact media/preprocess lineage
plus scene/silence routing metadata, never transcript or ASR output. It emits only
`proposal_unreviewed`
intervals with all inclusion, split, stratum, language, overlap, playback, and noise
judgments left `null` for a reviewer. A proposal is not a freeze and makes no
reference-quality claim.

Use `emit-local-window-proposal-request` for the additive schema-v2 path. Repeat
`--local-window-result` and `--preprocess-result` in matching order; the command
verifies existing catalogue admission lineage query-only, groups nonoverlapping
windows with unique IDs/ordinals under their unique acquired parent, and preserves
local plus parent coordinates. Each sealed preprocess envelope has separate,
explicit bindings for its exact raw file bytes and the corpus importer's canonical
parsed-envelope digest plus deterministic import-batch ID. Per-recording interval
count and gap policy apply across every window of that parent. The schema-v1
full-rendition command and legacy one-window request shape remain supported without
migration; see the proposal guide for the exact CLI, versioned schemas, and
compatibility boundary.

Catalogue validation requires a quiescent, checkpointed main file with no SQLite
sidecars and opens it using `mode=ro&immutable=1`. This makes proposal and selection
validation filesystem-nonmutating; it also means operators must checkpoint and close
any writer before invoking the evaluator.

## Private selection-review bridge

`emit-selection-review-template` merges exactly two validated request/proposal
pairs whose disjoint union covers all 12 cohort candidates. Both proposal versions
are accepted. Recordings are emitted in global cohort order even when one proposal
covers disjoint cohort ranges. The command is query-only and writes the incomplete
private template to stdout; it has no output-path option.

The human reviewer works from direct parent media and source metadata only. A
completed review must decide every proposed interval exactly once, keep each
recording wholly in calibration or scoring, populate every condition flag for an
include, give an enumerated reason for an exclude, accept at least one interval per
recording, and attest that neither ASR output nor reference text was inspected.
Accepted bounds may equal the proposal or trim to a nonempty subset; trims require
an enumerated `adjustment_reason`, and shifting or expanding requires exclusion as
`boundary_requires_reproposal`. Accepted duration must be at least 3,600,000 ms;
surplus duration is permitted and shortfalls report their exact millisecond value.

`review_id` is the stable identity of the cohort/proposal/protocol review plan.
`manifest_sha256` identifies one exact incomplete or completed revision. Freeze v2
binds both, plus the exact two request/proposal IDs, hashes, and schema versions.
Its accepted interval IDs bind the completed review hash, proposal interval, and
final bounds. Local-window proposals compile the acquired parent media/rendition
and parent coordinates, never local derivative coordinates. Review completion,
attestation, and explicit freeze time are ordered; freeze `created_at` and
`frozen_at` both identify the compile event.

The stdlib `validate_interval_freeze` entry point is intentionally self-contained
so downstream annotation validation can consume freeze v2 without private review
files. It proves internal structure, deterministic IDs, the one-hour floor, and
hash sealing, but cannot prove that external proposal hashes name the supplied
private documents. The operational `validate-freeze` CLI therefore requires the
completed review, catalogue, and both request/proposal pairs for v2 and
deterministically recompiles the freeze before accepting it. The
`validate-selection-review` CLI accepts completed private reviews only; incomplete
template validation is internal to template emission.

### Completing a selection template

#### Private loopback review workspace

`serve-selection-review` is the recommended interface for the 123-decision review.
It is a tracked local tool, not part of the Astro site or Cloudflare build. It binds
only to IPv4 `127.0.0.1`, prints a one-time random bootstrap URL, requires a private
same-site session plus Origin/CSRF checks, and exposes only the exact 12 parent-media
objects named by the validated proposals. It has no transcript, ASR, reference, wiki,
network, publication, freeze, or arbitrary-path input.

Partial work is saved to a separate ignored `interval_selection_draft`; the immutable
all-null template is never edited. Every interval must have near-complete continuous
playback coverage, a deliberate include/exclude decision, and complete flags or an
enumerated rejection reason. Playback telemetry is a workflow guard only—it cannot
prove what a reviewer perceived and never fills an attestation. Finalization first
persists the explicit reviewer intent, then rehashes all parent bytes, revalidates the
exact inputs against a private logical SQLite snapshot, stages mode-0400 noncanonical
review and receipt files, rechecks the inputs, and publishes the canonical review
last. Staging files are private crash-recovery material and are never completed
reviews. Finalized reloads recheck the exact inputs, review, receipt, and their closing
path identities. It does not compile a freeze.

If a draft replacement reaches its pathname but directory synchronization fails, the
server reconciles the on-disk revision and then refuses further work until restart.
This avoids silently continuing from stale memory after an indeterminate save.

The current acquired parents are mode `0644`: public-source bytes, but still writable
by their owner. The default command refuses them. The explicit
`--guard-writable-media` mode holds read-only descriptors, verifies all 4.91 GB before
serving, checks identity around every range response, and rehashes everything before
finalization. This is detect-and-fail protection, not a write lock, immutable storage,
or an operating-system confidentiality claim. A same-UID process can still modify a
writable file after a completed point-in-time check; the next guarded access fails,
but the tool does not claim to prevent that race. The workspace itself is an ignored
mode-0700 directory; its draft is mode 0600 and its immutable snapshot/manifest are
mode 0400.

Prepare or resume the current private package without starting a listener:

```sh
python3 -m evaluation serve-selection-review \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --catalog research/corpus/private-catalog-backups/corpus-v8.post-0021-pre-media-asr-7fe9865a6ceb.sqlite3 \
  --request research/corpus/evaluation/interval-proposals/interval_proposal_38b6d7225a1753a29d603d4bf12bde5f/request.json \
  --proposal research/corpus/evaluation/interval-proposals/interval_proposal_38b6d7225a1753a29d603d4bf12bde5f/proposal.json \
  --request research/corpus/evaluation/interval-proposals/interval_proposal_e8e12a2c1a1c584cbd7182aa06b2924a/request.json \
  --proposal research/corpus/evaluation/interval-proposals/interval_proposal_e8e12a2c1a1c584cbd7182aa06b2924a/proposal.json \
  --template research/evaluation/selection_review_a8aed2107b9f599696ec228599e6c880/selection-review-template.json \
  --media-root research/corpus/acquired/media/sha256 \
  --workspace research/evaluation/selection_review_a8aed2107b9f599696ec228599e6c880/workspace \
  --guard-writable-media \
  --prepare-only \
  --no-open-browser
```

Remove `--prepare-only` to review. Keep `--no-open-browser` if you prefer to copy the
printed one-time loopback URL manually; otherwise the command opens it locally. Do
not share that URL. Stop with Ctrl-C and rerun the same command to resume. Optimistic
revisions reject stale tabs instead of accepting last-write-wins updates.

Use Firefox for the current package unless you have independently smoke-tested your
browser. A 2026-08-27 local seek test passed all 12 exact parents in Firefox; Chromium
151 loaded and sought 11 but reported a decode error for the first proposed interval
of `h3ySLeBAoXs`. That is a browser compatibility observation, not a media-quality or
content finding. A playback error leaves coverage incomplete and blocks finalization;
never substitute a proxy, remux, transcode, or re-download.

The live package totals 3,690,000 proposed ms (61:30) against a 3,600,000 ms floor.
Only 90,000 ms may be lost to exclusions and trims. The interface reports the maximum
possible accepted duration and remaining removal budget, but it supplies no semantic
defaults or bulk decisions.

#### Manual JSON fallback

Edit a private copy of the emitted template; do not edit IDs, proposal bounds,
proposal order, or input hashes. For each recording, choose one recording-wide
`split` (`calibration` or `scoring`). For every interval, use exactly one branch:

- `include`: set nonempty `accepted_start_ms`/`accepted_end_ms` inside the proposed
  range, leave `rejection_reason` null, and fill all five flags. Language tags must
  be unique and sorted; `code_switch: true` requires at least two. Noise is one of
  `clean`, `light`, `moderate`, `heavy`, or `unknown`.
- `exclude`: leave accepted bounds, adjustment reason, and all flags null. Use one of
  `boundary_requires_reproposal`, `duplicate_or_redundant`, `no_usable_speech`,
  `out_of_scope`, `playback_only`, `privacy_or_sensitivity`, `technical_quality`, or
  `other_reviewed_reason`.

An included interval with unchanged bounds has a null `adjustment_reason`. A genuine
trim uses `remove_non_speech_edge`, `remove_sensitive_edge`,
`speech_boundary_refinement`, or `other_reviewed_trim`. Never shift or expand a
proposal; exclude it as `boundary_requires_reproposal` and generate another proposal
if the missing context is important. Set `review_state` to `completed_private`, fill
the reviewer/tool/timestamp fields, and make the direct-media and blinding
attestations only if they are true. Finally recompute the canonical hash and insert
it as `manifest_sha256` before validation:

```sh
python3 -m evaluation digest \
  research/evaluation/REVIEW/interval-selection-review.json

python3 -m evaluation validate-selection-review \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --catalog research/corpus/corpus-v8.sqlite3 \
  --request research/corpus/evaluation/interval-proposals/V1/request.json \
  --proposal research/corpus/evaluation/interval-proposals/V1/proposal.json \
  --request research/corpus/evaluation/interval-proposals/V2/request.json \
  --proposal research/corpus/evaluation/interval-proposals/V2/proposal.json \
  research/evaluation/REVIEW/interval-selection-review.json
```

The digest command reports a value; it never edits the review file. Validation fails
closed if any decision is missing, the accepted total is below one hour, either split
is empty, or an attestation is incomplete.

## Identifiers and lineage

- YouTube source and recording IDs follow the catalogue's deterministic UUIDv5
  convention and are recomputed by the validator. The source identity uses the
  catalogue source kind `youtube_video` (not the platform-generic word `video`).
- `media_id` must be the literal `media_sha256_<64 lowercase hex>` identity of the
  exact rendition bytes.
- `rendition_id` is recomputed from recording ID, media ID, and rendition kind.
- A freeze repeats exact source/native locator and media byte count/duration.
- Annotation and adjudication intervals repeat and must exactly match the frozen
  recording/source/media/rendition IDs, digest, split, and time range.
- Every manifest has a canonical SHA-256 over sorted compact JSON with only its root
  `manifest_sha256` field omitted. Parent manifests bind that digest.

Do not substitute a re-download, remux, audio normalization, edited mirror, or
different rendition under an existing freeze. Even perceptually identical content
with different bytes gets a different media ID and a new selection freeze.

## Reference annotation conventions

Time is integer milliseconds in rendition-media coordinates. Utterances must stay
within their frozen interval. Utterances for the same recording-local speaker label
must be sorted and nonoverlapping; utterances from different speakers may overlap
when two voices genuinely overlap. Speaker labels such as `speaker_local_01` are
not cross-video identities.

Use `text_state` deliberately:

- `verbatim`: a nonempty transcription the annotator considers intelligible;
- `contains_unintelligible_marker`: a nonempty transcription with an explicitly
  retained uncertain/inaudible portion;
- `unintelligible`: empty text for an utterance intentionally left unresolved; and
- `non_speech`: empty text, only where an utterance record is appropriate.

At interval level, `non_speech` and `unintelligible` contain no utterance records.
Do not normalize names from the wiki, silently repair grammar, expand slang, or use
an ASR hypothesis as a draft. Reference annotations are direct-media verbatim work.

## Private storage and deliberate release

Raw human annotations, adjudicated reference text, reviewer notes, and work packages
belong under ignored private storage, normally:

```text
research/evaluation/<freeze-id>/
  interval-selection-review.json
  interval-freeze.json
  pass-a.json
  pass-b.json
  adjudication.json
  system-output.json
  transcript-score-report.json
```

Annotation and adjudication manifests default to:

```json
{ "status": "withheld", "storage_policy": "private_only" }
```

Do not put such files in Git. A system-output manifest repeats every machine
hypothesis and is private even though it is machine-generated. `audit-tracked`
unconditionally rejects every tracked system output, transcript score report, GPU
evaluation measurement, transcript system comparison, and every tracked
`interval_selection_review`, including malformed copies with missing privacy
metadata. It rejects a tracked reference manifest
unless it has an explicit `released` decision with decision ID, reviewer, UTC time,
and basis. Changing this field does not bypass editorial, rights, privacy, or
sensitivity review; it merely makes deliberate release machine-visible.

The candidate cohort and an interval-only freeze contain no transcript text and may
be tracked after normal review. Media files remain private and ignored.

## Manifest-bound transcript scoring

[`transcript-system-output.schema.json`](./schemas/transcript-system-output.schema.json)
is the private adapter boundary between an ASR run and the evaluator. It binds one
system/run manifest, the exact frozen manifest hash, a preselected HIMR term-set
revision, bootstrap settings, recording-family assignments, measured resources, and
every segment/word hypothesis. It must preserve the freeze's recording and interval
order and cover every frozen interval exactly once with state `completed`. Segment
text must normalize to exactly the supplied word sequence. Unknown fields, partial
runs, missing intervals, changed bounds, duplicate IDs, bad hashes, cross-split
recording families, or text outside its segment bounds fail closed.

Fix the term set, recording-family map, bootstrap seed, and model/run manifest before
opening scoring references. They are immutable for that evaluation run only; the
incumbent model, runtime, and decoder remain replaceable candidates. The contract
makes run choices visible but cannot prove that an operator chose them blind. A
family groups exact duplicates,
reposts, excerpts, and mirrors; assigning families per system invalidates comparisons.
The manifest's `resource_profile` selects only a preregistered resource gate:
`cpu_bronze` uses wall RTF at most 0.35 and peak RSS at most 5.5 GiB,
`local_gpu` and `remote_gpu` use wall RTF at most 0.08 and leave process RSS
non-evaluable because the registered GPU gate is peak VRAM; the paired measurement
below carries actual peak VRAM and energy. `other_measured` reports RTF/RSS without
claiming either gate. These rows compare the complete frozen interval cohort with the
target; they do not establish the separate six-hour capacity benchmark or validate
the host/environment manifest named only by hash.

For contextual ASR, 92% term recall is the directly evaluable absolute branch. A
lower value stays `not_evaluable` until a separately bound baseline report can test
the registered relative-improvement and non-entity-WER branch. The one-false-term-
per-hour gate applies to contextual ASR; raw baselines still report the metric but do
not invent an unregistered pass/fail threshold. Repetition candidates likewise
require human review instead of becoming automatic failures or hallucination claims.

The scorer revalidates the cohort, freeze, both independent passes, completed human
adjudication, and system output before reading reference text. It emits a sealed,
exactly replayable aggregate-only report for run-integrity checks with these semantics:

- micro WER and CER use Unicode NFKC, case folding, lexical tokens, normalized
  apostrophes, and no punctuation/whitespace in CER. Aggregate values do not replace
  future language-specific and condition-stratum gates;
- HIMR-term recall requires an exact, contiguous token alignment to an adjudicated
  occurrence; unmatched hypothesis occurrences are false insertions, including all
  occurrences on adjudicated nonspeech. Its per-hour denominator includes only clear
  transcribed and adjudicated-nonspeech duration, not unresolved reference text;
- WER/CER exclude adjudicated nonspeech, unintelligible, and retained text
  uncertainty, while the report exposes each exclusion count;
- nonspeech hallucination counts every nonempty output segment, including
  punctuation- or symbol-only text; bounded literal 1--8-token cycles repeated at
  least four times and spanning at least 12 tokens become review candidates, never
  automatic hallucination findings;
- timing reports timed-word and monotonic coverage plus errors between exact-matched
  utterance edge tokens and human utterance spans. These are not word-boundary gold
  measurements;
- wall/CPU real-time factors divide summed processing milliseconds by exact frozen
  audio milliseconds; peak RSS is the maximum registered per-recording measurement.
  Registered resource gates use the complete frozen cohort, not one split; and
- deterministic percentile 95% confidence intervals resample recording families,
  never words or intervals. A metric reports `not_available` when there are fewer
  than two families or its denominator disappears.

Calibration readiness is task-specific and uses only the calibration split. The
current tasks are word correctness (matched tokens are positive; substitutions and
insertions are negative) and interval speech presence. The report checks raw-score
coverage plus the preregistered minimum of 200 observations, 50 positives, and 50
negatives. It does not fit a calibrator, compute ECE/Brier/log loss, or describe a raw
score as a probability. A `ready_to_fit_held_out_calibrator` row authorizes only the
next held-out fitting/evaluation step.

Prepare an unsealed private system-output JSON with placeholder ID/hash, then seal it
to stdout and validate the sealed copy. Keep shell redirection inside an ignored,
owner-only private directory:

```sh
umask 077

python3 -m evaluation seal-transcript-system-output \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  research/evaluation/FREEZE/system-output.unsealed.json \
  > research/evaluation/FREEZE/system-output.json

python3 -m evaluation validate-transcript-system-output \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  research/evaluation/FREEZE/system-output.json

python3 -m evaluation score-transcript-system \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  --pass-a research/evaluation/FREEZE/pass-a.json \
  --pass-b research/evaluation/FREEZE/pass-b.json \
  --adjudication research/evaluation/FREEZE/adjudication.json \
  --system-output research/evaluation/FREEZE/system-output.json \
  --created-at 2026-08-27T00:00:00Z \
  > research/evaluation/FREEZE/transcript-score-report.json

python3 -m evaluation validate-transcript-score-report \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  --pass-a research/evaluation/FREEZE/pass-a.json \
  --pass-b research/evaluation/FREEZE/pass-b.json \
  --adjudication research/evaluation/FREEZE/adjudication.json \
  --system-output research/evaluation/FREEZE/system-output.json \
  research/evaluation/FREEZE/transcript-score-report.json
```

The sealing and scoring commands write JSON only to stdout; they do not edit an
input, access media, open a catalogue, run inference, or grant publication authority.
The score report omits all transcript and term text but remains `private_only` by
default because its release still requires deliberate review. Report validation
recomputes every aggregate, gate, confidence interval, ID, and hash from the exact
six bound inputs and rejects any byte-level semantic drift. The report also binds the
scorer source hash, implementation version, Python version, and Unicode-data version
used for normalization and deterministic bootstrap replay.

## Paired accuracy and GPU-efficiency comparison

An individual score report cannot establish that a challenger should replace the
incumbent. In particular, comparing two separately bootstrapped confidence intervals
is not a paired test, and the v1 transcript output does not carry the NVML
measurements already available from the GPU worker.

[`candidate_comparison.py`](./candidate_comparison.py),
[`gpu-asr-evaluation-measurement.schema.json`](./schemas/gpu-asr-evaluation-measurement.schema.json),
and [`transcript-system-comparison.schema.json`](./schemas/transcript-system-comparison.schema.json)
close that decision-support gap without freezing the technology stack. Exact
model/runtime/profile hashes bind
the two measured runs only. The comparator requires the same frozen reference,
recording-family map, scoring profile, GPU hardware, and measurement protocol. It
then resamples each recording family once and applies that same draw to baseline and
challenger. The text-free output reports paired absolute and relative deltas with 95%
intervals for WER, CER, HIMR-term recall and false insertions, nonspeech
hallucinations, and timing.

The same report breaks paired WER out by language, code-switch state, speaker
overlap, playback speech, and noise level. A condition needs at least two recording
families and 100 scored reference words; otherwise it is explicitly `undercovered`
with no inferential WER row. Overall WER cannot override a detected condition
regression. Any challenger repetition candidate also sets
`human_review_required` and blocks unattended promotion until a separate human
decision is bound.

A separately sealed `gpu_asr_evaluation_measurement` binds each system output to:

- runner wall time and sampled GPU-active time;
- peak process VRAM and estimated NVML energy;
- whether model load, audio preflight, and result publication are in the timed scope;
- warmup/measured-run counts, exact hardware, profile identity, and source receipts.

The comparison reports media hours per runner/GPU-active hour and estimated Wh per
media hour. It deliberately emits no blended scalar: throughput must not compensate
for a quality regression. Accuracy-first decision support requires both the paired
absolute WER upper bound to be at most +0.005 and the paired relative upper bound to
be at most +0.03. If the relative interval loses its denominator in a bootstrap
sample, non-inferiority remains `not_evaluable`; it is never silently inferred from
the absolute interval. Automatic promotion is always false.

Prepare unsealed measurement JSON for each system, then seal it to stdout:

```sh
python3 -m evaluation seal-gpu-evaluation-measurement \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  --system-output research/evaluation/FREEZE/baseline-system-output.json \
  research/evaluation/FREEZE/baseline-gpu-measurement.unsealed.json \
  > research/evaluation/FREEZE/baseline-gpu-measurement.json
```

Compare the two candidates on the exact common inputs:

```sh
python3 -m evaluation compare-transcript-systems \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  --pass-a research/evaluation/FREEZE/pass-a.json \
  --pass-b research/evaluation/FREEZE/pass-b.json \
  --adjudication research/evaluation/FREEZE/adjudication.json \
  --baseline-system-output research/evaluation/FREEZE/baseline-system-output.json \
  --challenger-system-output research/evaluation/FREEZE/challenger-system-output.json \
  --baseline-gpu-measurement research/evaluation/FREEZE/baseline-gpu-measurement.json \
  --challenger-gpu-measurement research/evaluation/FREEZE/challenger-gpu-measurement.json \
  --created-at 2026-08-29T20:00:00Z \
  > research/evaluation/FREEZE/candidate-comparison.json
```

The measurement and comparison are `private_only`, perform no inference or media
access, and are rejected by `audit-tracked` if accidentally added to Git.

## CLI

Run from the repository root:

```sh
python3 -m evaluation validate-candidate \
  evaluation/cohorts/himr-asr-candidate-cohort-v1.json

python3 -m evaluation emit-acquisition-selection \
  evaluation/cohorts/himr-asr-candidate-cohort-v1.json

python3 -m evaluation emit-proposal-request --help

python3 -m evaluation propose-intervals --help

python3 -m evaluation emit-selection-review-template --help

python3 -m evaluation validate-selection-review --help

python3 -m evaluation compile-freeze --help

python3 -m evaluation digest research/evaluation/FREEZE/interval-freeze.json

python3 -m evaluation validate-freeze \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  research/evaluation/FREEZE/interval-freeze.json

python3 -m evaluation validate-annotation \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  research/evaluation/FREEZE/pass-a.json

python3 -m evaluation validate-adjudication \
  --cohort evaluation/cohorts/himr-asr-candidate-cohort-v1.json \
  --freeze research/evaluation/FREEZE/interval-freeze.json \
  --pass-a research/evaluation/FREEZE/pass-a.json \
  --pass-b research/evaluation/FREEZE/pass-b.json \
  research/evaluation/FREEZE/adjudication.json

python3 -m evaluation audit-tracked .
evaluation/tests/run.sh
```

`digest` does not edit a file. Insert the printed digest, then run the corresponding
strict validator. `validate-adjudication` returns `reference_ready: true` only after
the cohort, freeze, two independent passes, and adjudication all validate. It also
returns `accuracy_metrics_computed: false` to prevent confusing readiness with a
quality result.

`emit-acquisition-selection` validates the complete unfrozen cohort, rejects any
ineligible or non-public variant, and writes only this exact JSON shape to stdout:
`schema_version`, `purpose`, `youtube_video_ids`, `source_ids`, and `recording_ids`.
All three arrays preserve candidate-manifest order and have matching indexes. This
is the supported input boundary for a decoupled acquisition queue planner.

## Contract files

- [`candidate-cohort.schema.json`](./schemas/candidate-cohort.schema.json)
- [`acquisition-selection.schema.json`](./schemas/acquisition-selection.schema.json)
- [`interval-proposal-request.schema.json`](./schemas/interval-proposal-request.schema.json)
- [`interval-proposal.schema.json`](./schemas/interval-proposal.schema.json)
- [`interval-proposal-request-v2.schema.json`](./schemas/interval-proposal-request-v2.schema.json)
- [`interval-proposal-v2.schema.json`](./schemas/interval-proposal-v2.schema.json)
- [`interval-freeze.schema.json`](./schemas/interval-freeze.schema.json)
- [`interval-selection-review.schema.json`](./schemas/interval-selection-review.schema.json)
- [`interval-selection-draft.schema.json`](./schemas/interval-selection-draft.schema.json)
- [`interval-freeze-v2.schema.json`](./schemas/interval-freeze-v2.schema.json)
- [`reference-annotation.schema.json`](./schemas/reference-annotation.schema.json)
- [`reference-adjudication.schema.json`](./schemas/reference-adjudication.schema.json)
- [`transcript-system-output.schema.json`](./schemas/transcript-system-output.schema.json)
- [`transcript-score-report.schema.json`](./schemas/transcript-score-report.schema.json)

The schemas reject unknown fields. The runtime additionally verifies deterministic
IDs, manifest hashes, parent hashes, exact lineage, timestamp order, interval bounds,
nonoverlap, split isolation, language/condition flags, accounting, independent
annotators, adjudication sources, complete system-output coverage, recording-family
isolation, aggregate recomputation, and scoring-report replay.
