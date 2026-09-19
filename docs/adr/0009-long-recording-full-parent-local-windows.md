# ADR 0009: Full-parent acquisition and local windows for long recordings

- Status: accepted
- Date: 2026-08-26

## Context

The deterministic acquisition planner routes recordings over the single-job duration
or byte policy to `requires_chunking`. The two remaining public ASR evaluation streams,
YouTube IDs `8AbFGYob9SU` and `UUqmpEOc5oc`, are roughly 2.6 and 2.7 hours. Treating
multiple remote time-range downloads as evidence fragments would make their common
parent an assumption rather than a verified byte identity.

yt-dlp's documented `--download-sections` option needs FFmpeg, while the project's
[official FAQ](https://github.com/yt-dlp/yt-dlp-wiki/blob/master/FAQ.md#downloading-clips-and-cutting-out-sponsor-sections-is-inaccurate)
states that exact cuts require re-encoding. FFmpeg's
[`-ss` contract](https://ffmpeg.org/ffmpeg.html#Main-options) explains that input seeks
normally land on an earlier seek point and accurate transcoding discards material up
to the requested position. Neither behavior proves that independently resolved remote
formats and URLs are one immutable object.

## Decision

Keep `requires_chunking` ineligible for the normal queue materializer. Add a distinct
operator-approved boundary that revalidates the original queue plan and materializes
exactly one full-source acquisition work order with an explicit larger byte cap. It
must remain public-only, credential-free, hash-pin `yt-dlp`, and record that it changes
no normal queue semantics.

After guarded acquisition admits and hashes the full parent, materialize a second,
offline bundle of contiguous half-open `[start_ms, end_ms)` work orders. Bind every
work order to:

- the full parent path, SHA-256, byte count, media ID, and duration;
- the completed acquisition-result path and SHA-256;
- exact FFmpeg and FFprobe executable hashes and version-output hashes;
- one source-time window and its ordinal;
- a per-window output cap, free-space floor, and timeout; and
- a private, no-publication, no-identity safety policy.

Each work order transcodes only local bytes into normalized lossless audio and a CFR
analysis proxy. The contract coordinates are exact and gapless; the representations
are not byte-exact fragments and must say so. Completed windows are immutable and
independently replay-verifiable.

The coordinate transform alone does not calibrate packet/frame alignment. Stream
gaps, discontinuities, encoder delay, and padding remain downstream boundary checks;
until then, the artifacts are private analysis inputs rather than timestamp evidence.

A provider bot challenge, rate limit, or transient public-access failure is a
retryable availability outcome with zero admission. It never authorizes cookies,
tokens, browser extraction, alternate authentication, or remote section fallbacks.

## Consequences

- All windows share one cryptographically identified parent.
- Long downstream work becomes bounded and independently resumable.
- The full network acquisition still has to complete once and needs an explicitly
  provisioned cap. `yt-dlp` may resume its own partials, but no partial is canonical.
- Accurate local transcoding costs CPU and produces non-original derivatives.
- A 30-minute default yields six windows for each current evaluation stream, including
  a marked final tail where necessary.
- The producer itself introduces no database row, ASR execution claim, review
  decision, or publication authority. A later private catalog-admission verifier may
  rehash a sealed result, independently inspect its media, and create an artifact
  reference for bounded ASR. Its processing run is explicitly the admission time,
  never a fabricated extraction time; see
  [`corpus/docs/LOCAL_WINDOW_RESULT_INGESTION.md`](../../corpus/docs/LOCAL_WINDOW_RESULT_INGESTION.md).
