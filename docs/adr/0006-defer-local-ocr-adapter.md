# ADR 0006: Defer the local OCR adapter until word geometry and language assets are pinned

- Status: accepted
- Date: 2026-08-26

## Context

The sparse-frame router already emits private, sealed PNGs with exact decoded PTS,
source-media lineage, and an explicit `not_evaluated` OCR route. The next stage must
preserve that lineage while producing text regions, not merely a string detached from
its position in the source frame. It also needs raw engine scores, explicit language
and model-data provenance, and a task-specific calibration state.

The current Fedora host contains FFmpeg 8.1.2 built with `--enable-libtesseract`.
FFmpeg advertises the `ocr` filter, and the filter accepts `datapath`, `language`,
`whitelist`, and `blacklist`. That makes an engine probe possible without a model
download, but it does not by itself satisfy the corpus result contract.

## Evidence from the 2026-08-26 local probe

The tracked fail-closed preflight records exact executable, library, language-data,
font, help-output, version-output, and license hashes. On this host it found:

| Asset | Installed version or SHA-256 | Finding |
| --- | --- | --- |
| FFmpeg | 8.1.2-3.fc44; `abecc2e819e1754850556d6978d726ea4ece8fa4fb65536780137d6537809f27` | Present; OCR filter enabled |
| `libavfilter` | 11.14.102; `b6f4e98304d9612eef16f657d8ae7d51443fafc62115a48020aaec358d72dbbb` | Present; contains the FFmpeg OCR filter |
| `libtesseract` | 5.5.3-1.fc44; `6897861815f39dc54930eadb48be794c7e4367c5625dcea41ea943556291b5b0` | Present and dynamically linked |
| `libleptonica` | 1.87.0-4.fc44; `27f70f645a88e57cc5be893913933c8bd3a92de934093f5d8d5f82ccf5fb6c63` | Present and dynamically linked |
| English tessdata | 4.1.0 fast model; `7d4322bd2a7749724879683fc3912cb542f19906c83bcc1a52132556427170b2` | Present |
| Japanese tessdata | none | Required pilot language missing |
| Korean tessdata | none | Required pilot language missing |
| Tesseract CLI | none | No local TSV/hOCR word-box producer |
| Tesseract/tessdata license | Apache-2.0 text; `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30` | Present and pinned |

Three repeated FFmpeg-filter passes over each available synthetic fixture produced
byte-identical metadata. The English HIMR-term fixture returned `HIMRverse Daniel
2026` with raw confidence text `86 96 96`. The Latin-diacritic fixture returned
`Café Turkce Frangais Espanol` for the ground-truth `Café Türkçe Français Español`,
with raw confidence text `96 68 11 96`. The latter is an immediate example of why
repeatability is not an accuracy or language-support finding.

The FFmpeg output exposes exactly these OCR metadata keys:

```text
lavfi.ocr.text
lavfi.ocr.confidence
```

It exposes no word or line rectangles, polygons, reading-order objects, or stable
token IDs. Although its confidence string appears word-oriented, the filter does not
emit a typed mapping from each value to a bounded source-frame region. A Japanese
probe using `language=jpn` fails during filter initialization because
`jpn.traineddata` is absent; the same is true for Korean.

## Decision

Do not implement or admit an OCR execution adapter from the currently installed
assets. FFmpeg frame-level OCR metadata is diagnostic evidence only and must not be
converted into corpus observations, entity mentions, claims, transcript corrections,
or public search records.

Keep the separate preflight as an executable gate. It exits nonzero until all of the
following are true:

1. A standalone Tesseract executable or another reviewed offline engine is present,
   byte-pinned, and has byte-pinned version output.
2. English, Japanese, and Korean model data and their license evidence are locally
   present and byte-pinned. More languages must be added when the frozen corpus
   cohort establishes the need.
3. Three repeated synthetic runs for every required language are byte-identical.
4. The engine emits UTF-8 TSV-equivalent word rows containing positive pixel boxes,
   raw confidence values, and text. Missing, zero-area, non-finite, or malformed rows
   fail closed.
5. Network access, automatic publication, and identity inference remain disabled.

The later execution environment must also pin its complete shared-library closure or
an immutable container/image digest. The three critical OCR/filter libraries in this
feasibility snapshot are necessary evidence, not a claim that the current host
package set is a portable runtime closure.

Passing this preflight will mean only `ready_for_adapter_implementation`. It will not
mean the engine is accurate, calibrated, safe to backfill, or approved for release.

## Required later adapter contract

After the gate passes, a separate work/result contract must consume a completed,
sealed sparse-frame-router result by path and expected SHA-256. It must rehash the
envelope and every selected PNG and retain, without rounding away, the upstream:

- source media ID and proxy artifact/processing-run lineage;
- sparse-frame result key and processing-run ID;
- frame ID, frame artifact ID, byte hash, and ordinal;
- requested timestamp, exact PTS/time base/duration, the producer-named
  `rounded_source_timestamp_ms` (a proxy-rendition-local coordinate), and timestamp
  drift; no original-source or recording-time transform is implied; and
- sparse selection and OCR-routing reason codes.

Every OCR region must then retain the raw text, raw engine score, rectangle or polygon
in source-frame pixels, reading order, script/language choice and evidence, engine and
model-data hashes, complete parameters, and an immutable processing-run ID. Scores
start as `not_calibrated`; no field may call them probabilities. Region deduplication
or temporal tracking must retain every contributing frame locator rather than replacing
the raw observations.

All output remains private and requires human review. OCR must never infer who a person
is. Text resembling an address, account identifier, private message, credential, or
unrelated person's information needs the publication-exclusion review before it can
enter any public release.

## Consequences

- Sparse-frame extraction can continue because it makes no OCR claim.
- No unreliable coordinate-free text is admitted merely because the installed filter
  is convenient.
- The missing local executable and language assets are explicit, machine-checkable
  blockers instead of an undocumented environment assumption.
- Tesseract remains a future clean-overlay baseline, not the sole planned scene-text
  engine; PaddleOCR or another detector/recognizer still needs a separate pinned pilot.
- At the time of this decision there was deliberately no database migration, OCR
  importer, or public export change. The 2026-08-28 follow-up below records the later
  private-only implementation; public export remains unchanged.

## 2026-08-28 implementation follow-up

The deferred feasibility condition is now satisfied for a private Fedora 44 pilot;
the accuracy, calibration, review, and publication conditions are not. Tesseract
5.5.3-1.fc44 is byte-pinned, and signed Fedora 44 Japanese and Korean fast-model RPMs
were downloaded with every repository except `fedora` disabled. `rpmkeys` verified
their OpenPGP header, header SHA-256, and payload SHA-256 under Fedora key fingerprint
`36f612dcf27f7d1a48a835e4dbfcf71c6d9f90a6`. The complete package and extracted
model hashes are retained in `pipeline/ocr-asset-provenance-fedora44-v1.json`.

An ignored, owner-private bundle containing the pinned English, Japanese, and Korean
models plus the pinned Apache-2.0 license passed all repeated synthetic probes. The
gate reported `ready_for_adapter_implementation`, no blocking requirements, and the
unchanged policy values `not_calibrated`, no automatic publication, and no identity
inference. The repository-default system-tessdata manifest continues to fail closed
because the Japanese and Korean files are not installed there.

The first isolated-bundle run also caught a command-level hazard: Tesseract accepted
the trailing `tsv` config name, could not find the system config inside the isolated
bundle, returned exit status zero, and emitted plain text. Preflight 0.2 now uses the
explicit `-c tessedit_create_tsv=1` parameter and validates the complete TSV header.
All English, Japanese, and Korean synthetic TSV runs were then byte-identical across
three repetitions and exposed positive word boxes plus finite raw scores. This is a
runtime-contract finding only; no real corpus frame was consumed by the preflight.

The private adapter subsequently passed its schema, seal-replay,
deterministic-output, privacy, and adversarial tests and processed a small frozen
pilot. Migration 0033 now provides a separate strict importer and private FTS search
lane. It replays the upstream sparse-frame and raw TSV evidence, keeps raw scores
uncalibrated, fixes redaction to pending, and grants no identity, event, claim,
publication, gate, or export authority. This supersedes only the earlier statement
that no importer exists; the producer still cannot write the catalogue, and public
export remains unchanged. See
[`corpus/docs/OCR_TESSERACT_RESULT_INGESTION.md`](../../corpus/docs/OCR_TESSERACT_RESULT_INGESTION.md).
