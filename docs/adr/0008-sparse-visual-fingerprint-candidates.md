# ADR 0008: Use fixed-integer sparse visual hashes only for review candidates

- Status: Accepted
- Date: 2026-08-26

## Context

HIMR media can recur as full uploads, excerpts, recompressed clips, screen recordings,
and edited reposts. Exact media SHA-256 cannot surface a re-encode, while dense neural
video embeddings would add model licensing, hardware, calibration, storage, and
identity risks before the basic archive pipeline is stable. The corpus still needs a
small, reproducible visual signal that can help a researcher prioritize possible
clip-to-source comparisons.

## Decision

Add an offline pipeline-only stage that decodes only explicit timestamps inside
explicit half-open windows from a sealed video/proxy. Preserve each decoded 32×32
grayscale frame privately, hash it exactly with SHA-256, and compute a 64-bit
fixed-integer low-frequency DCT perceptual hash. Pin the source, FFmpeg executable and
capability output, implementation bytes, algorithm, scaling, time evidence, and all
limits in immutable work/result contracts.

A separate bounded pair contract reports raw Hamming scores and whether the minimum
distance meets a caller-configured threshold. It always labels calibration as absent,
requires human review, and fixes identity, duplicate, parent, ownership, relationship,
and unrelated assertions to false. No database migration or importer is added at this
stage.

## Consequences

The signal is cheap, inspectable, and can tolerate some ordinary re-encoding. It can
be joined manually with sparse-frame and audio-fingerprint evidence without granting
either signal authority over catalog relationships.

It will miss some crops, overlays, borders, rotations, time shifts, interpolated
frames, and neighboring-frame selections; repeated or low-detail images can collide.
A threshold pass is only a candidate and a threshold miss does not prove material is
unrelated. Corpus-wide matching and probabilistic language remain blocked on frozen,
recording-disjoint evaluation with edit/codec/resolution/content strata and measured
review workload.
