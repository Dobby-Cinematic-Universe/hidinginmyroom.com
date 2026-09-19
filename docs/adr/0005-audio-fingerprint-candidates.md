# ADR 0005: Preserve raw audio fingerprints; defer approximate identity matching

- Status: accepted
- Date: 2026-08-26

## Context

The corpus needs a scalable way to route reposted clips toward possible parent
recordings. FFmpeg on the processing host already exposes the Chromaprint muxer, but
the repository has no evaluated approximate matcher and no HIMR-specific calibration
set. Treating an arbitrary distance as a probability or automatically merging records
would make provenance errors difficult to unwind.

Short clips, silence, re-encoding, overlays, speed changes, edits, and unequal window
durations also change what a raw fingerprint comparison can support.

## Decision

Version 1 extracts and preserves FFmpeg's raw Chromaprint output under exact input,
tool-build, algorithm, format, and half-open-window provenance. Recipe identity is
deterministic; execution identity is nonce-derived. Every artifact is private,
content-addressed within its execution, immutable, and rehashed at catalog admission.

The only shipped pair helper is `exact_raw_bytes_v1`. It produces a boolean raw score,
explicitly uncalibrated, with duration/short-window/empty quality flags. All pair rows
require human review and state that no relationship was asserted. They enter the
generic private match-candidate table, not `recording_relations`.

Approximate matching, offset search, clip-parent direction, and duplicate/merge
decisions are deferred. A future matcher must have a pinned implementation/model,
versioned work-order/result contract, frozen evaluation pairs, duration/edit/noise
strata, measured false-candidate behavior, and a named calibration set before any
score is exposed as probability.

## Consequences

- Extraction can begin without downloads or new ML dependencies.
- Raw evidence is reproducible and available to a later matcher.
- Exact equality can route identical windows but cannot prove recording identity,
  parentage, or ownership.
- Approximate discovery remains a later private pilot, not a hidden heuristic.
- Null-context imports retain provenance only, preventing placeholder recording links.

## V2 amendment: cross-recording exact comparison

The v1 extractor embeds its input-dependent `recipe_id` in each fingerprint's
`implementation_version`. Consequently, v1's requirement that the complete
implementation strings match rejects fingerprints extracted from different input
bytes even when the engine/build, algorithm, raw format, sample rate, and channel
binding are identical. That behavior remains frozen as part of the v1 contract.

`exact_raw_bytes_v2` is a separate sealed-envelope lane. A work order selects one
fingerprint ID and role from each exact extraction result and pins each result's
SHA-256. The comparator and catalog importer independently reconstruct both producer
envelopes and current artifacts. Compatibility is defined by engine/build, algorithm,
raw format, sample rate, and channels; extraction input and recipe identities may
differ. Only selected raw bytes are compared.

V2 remains private, uncalibrated, candidate-only, human-review-required, unable to
assert a relationship, and has explicitly zero publication authority. Its catalog
subtype is separate from v1 and binds both extraction result digests. This amendment
does not authorize approximate matching, identity, parentage, duplicate merging, or
publication.

The catalog realization uses one typed comparison receipt and two role-keyed side
bindings, admitted before the subtype in the same transaction. Duplicate JSON keys
are rejected wherever SQL reads producer JSON, comparison-owned JSON is required in
deterministic compact form, and exact graph cardinalities are sealed after admission.
The catalog score is derived from the selected artifact digest and byte-count pair;
the runtime remains responsible for comparing and rehashing the actual local bytes,
because SQLite cannot establish filesystem existence or content by itself.
