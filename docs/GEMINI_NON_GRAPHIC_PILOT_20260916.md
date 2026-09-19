# Non-graphic Gemini recovery pilot

The operator approved non-graphic input copies with originals and local evidence
links preserved, then explicitly chose Gemini before Claude. No Claude submission
is part of this pilot.

Five previously blocked chunk requests from five recordings are selected by
exact original wave, collection and job bindings. They cover one longer discussion
and four short excerpts. The operator subsequently explicitly requested submission
of the remaining nine blocked chunks; those have a separate reviewed selection
and submission receipt, described below.

The private directory is
`research/private-transcriptions/cloud-archive-20260913/summaries-v2/non-graphic-pilot-20260916/`.
`decisions.json` contains the manually reviewed substitutions; `selection.json`
binds them to the original blocked requests; `run/manifest.json` binds the exact
implementation and new request bodies. The five requests have a combined
conservative allowance of **$0.637815**, not a prediction of Google's invoice.

## Meaning and safeguards

- Explicit descriptions are replaced with marked editorial, non-graphic
  paraphrases. These are not presented as verbatim transcript text. Ages,
  allegations and uncertainty are not disguised to evade provider screening.
- Unedited evidence text is preserved exactly. Every replacement retains the
  complete original citation ranges, timing and transcript hashes locally.
- Gemini receives compact evidence IDs and text, not local paths, hashes or
  structured timestamps. The source material and earlier Gemini receipts are
  unchanged. Output normalization retains allegations and uncertainty.
- No safety settings are weakened, and no provider refusal is automatically
  retried. A durable intent precedes the single batch POST; an uncertain POST
  remains held instead of being repeated.
- Each derived job gets a distinct identity. These outputs are **chunk-level
  recovery candidates**, not full-recording summaries. They are not silently
  injected into the original Gemini dependency graph or counted as completed
  recordings. They require content review and an explicit downstream integration
  step before replacing a recording's summary.
- The existing archive worker, its authority and its source selection are not
  edited or restarted. Pilot accounting is retained separately in its manifest,
  submission receipt and provider captures.

`pipeline/gemini_non_graphic_pilot.py` supports offline preparation, explicit
paid submission and read-only polling/watching. Its watcher exits at a terminal
result or after 24 hours. It never submits, retries, switches providers or
promotes outputs. Fourteen focused pilot/classification tests passed, covering
unchanged originals, citation retention, metadata exclusion, invalid edit ranges,
idempotent submission and ambiguous-intent preservation.

## Remaining nine: explicitly authorized submission

Following the operator's “Please resubmit the remaining,” nine additional
non-graphic chunk copies across eight recordings were submitted as one batch:
`batches/hibst6ntzayq9v5p6bazu45j6j6ebcxt6mzt`.
The original five requests were excluded by exact job ID. All fourteen originally
blocked chunks now have one submitted editorial-copy attempt; this does not mean
fourteen completed outputs or completed recording summaries.

Audit directory:
`research/private-transcriptions/cloud-archive-20260913/summaries-v2/non-graphic-remaining-20260916/`.
The offline `prepare.py` binds the exact blocked originals, applies reviewed
edits, checks original citation retention and excludes prior pilot requests.
`decisions.json`, `selection.json` and `run/manifest.json` retain the selection;
the manifest SHA-256 is
`e3c29ae0915983102e0b677c4f3cdd1e7d0059d8e4c32c043bfd6c1d602362b7`.
The conservative cost allowance is **$1.194201** for these nine requests,
**$1.832016** including the first five; neither figure is actual billed spend.

The separate read-only watcher is
`himr-gemini-non-graphic-remaining-20260916.service`. It collects results but does
not retry refusals, use Claude, or promote partial summaries. The existing pilot
implementation and main archive worker were not modified or restarted. Fourteen
focused tests passed again before submission.
