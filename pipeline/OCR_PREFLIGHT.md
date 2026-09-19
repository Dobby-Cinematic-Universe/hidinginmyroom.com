# Offline OCR feasibility preflight

`ocr_preflight.py` is a non-inference gate for the separate sparse-frame OCR stage. It
hashes reviewed local assets, renders bounded synthetic fixtures, repeats local OCR
probes three times, and requires deterministic TSV word boxes plus raw confidence. It
does not accept a media path or sparse-frame result, does not use the network, and
does not emit an OCR observation.

Run the reviewed host snapshot with:

```sh
pipeline/bin/ocr-preflight > /tmp/himr-ocr-preflight.json
```

The repository-default manifest intentionally targets the system tessdata directory.
Its current expected exit status is `1` with `status: "blocked"` because Japanese and
Korean data are not installed into that directory. The JSON report lists every
failing or skipped required check in `blocking_requirements`. The Tesseract executable
itself is now present and pinned.

On 2026-08-28 an owner-private, ignored pilot bundle cleared the gate for all three
languages. Its manifest and model bytes remain under `research/corpus/ocr-assets/`;
they are not public-site inputs. Run that reviewed snapshot with:

```sh
pipeline/bin/ocr-preflight \
  --requirements research/corpus/ocr-assets/fedora-44-tessdata-4.1.0-12/requirements.json
```

The observed result was `ready_for_adapter_implementation` with no blockers. That
means only that a pinned offline engine can emit deterministic TSV geometry on the
synthetic fixtures. It is not an accuracy, calibration, privacy, or publication
finding.

The 2026-08-26 blockers were:

- no Tesseract CLI executable or reviewed executable/version-output pin;
- no reviewed Japanese or Korean traineddata files; and
- therefore no deterministic TSV word-geometry/confidence probes for the three pilot
  languages.

The FFmpeg English probes pass determinism, but their geometry check is
`unsupported`: FFmpeg emits only `lavfi.ocr.text` and `lavfi.ocr.confidence`. That
result is not sufficient for a corpus adapter.

Those first and model-availability blockers have now been resolved in the reviewed
private pilot. The repository-default path still fails closed when its two pinned
model files are absent.

The pin and fixture requirements are in
[`ocr-preflight-requirements-v1.json`](ocr-preflight-requirements-v1.json). Null hashes
remain blockers, not wildcards. The reviewed Fedora 44 executable, model-package,
signature, license, and byte hashes are recorded in
[`ocr-asset-provenance-fedora44-v1.json`](ocr-asset-provenance-fedora44-v1.json).
The Japanese and Korean RPMs were downloaded separately from the Fedora `fedora`
repository and verified with `rpmkeys`; preflight itself remains offline and never
downloads an executable or model.

The isolated bundle deliberately does not depend on Tesseract's unpinned system
`configs/tsv` file. Version 0.2 requests TSV through the explicit
`-c tessedit_create_tsv=1` parameter and still rejects a successful return code unless
the exact required TSV header, positive word boxes, finite raw scores, and text are
present. This closes a discovered case where Tesseract returned plain text with exit
status zero after failing to find the named `tsv` config.

When all checks pass, the output status is `ready_for_adapter_implementation`. It
still reports `calibration_state:
"not_calibrated"`, `automatic_publication: false`, and `identity_inference: false`.
The separate adapter and private admission boundary are now implemented. Accuracy
evaluation, redaction review, calibration, and any distinct public-use workflow remain
unfinished; migration 0033 does not authorize a backfill or public use. See
[`../corpus/docs/OCR_TESSERACT_RESULT_INGESTION.md`](../corpus/docs/OCR_TESSERACT_RESULT_INGESTION.md).

Run the adversarial tests with:

```sh
python3 -m unittest pipeline.tests.test_ocr_preflight -v
```

The tests use offline fake executables for the fully pinned success path. They cover
hash changes, unpinned assets, unknown fields, weakened no-publication policy, symlink
manifests and executables, malformed/zero-area TSV geometry, non-finite confidence,
missing text, environment-secret isolation, and the rule that valid raw scores remain
uncalibrated.
