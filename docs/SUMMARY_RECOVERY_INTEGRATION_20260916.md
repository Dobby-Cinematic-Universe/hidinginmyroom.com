# Recovered transcript-summary integration

The operator approved integrating the successful non-graphic Gemini/Claude
chunks and the five explicitly authorized uncertain-submission replacements.
The continuation covers **18 recordings**, **112 source chunks**, and **22
recovered jobs** (21 chunks and one existing final transcript reduction).
Already successful chunks are retained, not purchased again.

## Evidence and provenance

`pipeline/summary_recovery_integration.py` replays the recovery collections,
binds exact original plans and source transcripts, validates original chunk
coverage, and substitutes only the explicitly recovered failures. The two Claude
outputs retain their Claude job IDs and model identity. No result is mislabeled
as a successful response to the original refused Gemini request.

The `Chicken And Beer` replacement required two conservative classification-tag
corrections in one output: allegations cannot become statements. Model wording
and evidence references were unchanged; strict normalization passed after the
correction. The original failed collection remains intact. No extra API call was
made for this repair.

Only remaining transcript reductions are generated. They depend on all chunks
in source order, retain local citations, and send Gemini text/evidence without
timestamps or source metadata. No new transcription or broader synthesis is
started. A source with any missing chunk cannot receive a complete final summary.

## Runtime and publication

Service: `himr-integrated-summary-recovery-20260916.service`.
Workspace: `research/private-transcriptions/cloud-archive-20260913/summaries-v2/integrated-recovery-20260916/`.
Manifest SHA-256:
`fef46b44e10925a8aa894c0a0b43e26074924b26d0c6f693db5c3328817f10bf`.

The preflight-only manifest is retained separately. The execution manifest pins
the finalized implementation and exact admitted snapshots. Original records,
receipts, failed collections, unknown intents and reservations remain unchanged.
The existing main worker was not restarted or altered.

Each new reduction has an immutable wave, exact wire bytes, durable intent and
provider receipt. An uncertain POST is held and never automatically repeated;
an individual transport error does not prevent other records from advancing.
Collection continues through the finite reduction hierarchy, then publishes the
validated final summary and citation-free reader view automatically.

- `preferred-exports.json` contains only completed recovered recording summaries.
- `status.json` reports this continuation's actual progress.
- `overall-status.json` combines the latest main-worker snapshot with completed
  preferred recoveries, without counting partial chunks as full summaries.
- `records/*/exports/` retains canonical evidence, the reader index and a linked
  timestamp-free transcript copy.

For transcript-only exports, `pipeline/transcript_summary_reader.py` now prefers
an exact matching completed recovery. It checks the continuation, original plan
binding and full reduction chain before using that result. Unrelated plans and
broader synthesis retain the existing reader behavior. The historical worker
status intentionally still records original holds; consult the combined status
for effective completion rather than deleting the historical failure evidence.

Twenty-three focused integration, reader, projection and duplicate-POST tests
passed. Tests cover mixed-provider provenance, missing-chunk rejection, refusing
to overwrite a successful chunk, foreign-scope rejection, retained-final reuse,
metadata-free reduction input and reader preference for validated recovery.
A synthetic provider lifecycle also checks submission, collection, final
publication, combined-count updates and no repeated POST after completion.
