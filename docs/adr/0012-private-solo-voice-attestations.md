# ADR 0012: Admit named solo voices only through private direct-audio attestations

- **Status:** Accepted for private catalog administration
- **Date:** 2026-08-28

## Context

The speaker-activity router safely distinguishes anonymous solo speech from work that
needs diarization or active-speaker analysis. Its optional source-wide Daniel field
is a forwarded planning presumption, not a durable identity decision. The biometric
cluster workflow can associate private voice observations across recordings, but it
is intentionally expensive and cannot turn a model score into identity.

The catalog lacked a narrow middle path for the common case where a human directly
listens to one exact, confirmed media interval, hears one live speaker, and can name
that voice without relying on the uploader, channel, title, transcript, or a machine
match.

## Decision

Add migration `0032`, a strict v1 private manifest, and an explicit corpus
administration command.

1. Freeze an immutable subject to entity, source, recording, rendition, media ID,
   media SHA-256, and half-open `rendition_media_ms` bounds. All catalog relationships
   must be currently reviewed and the exact media must be verified with a known
   duration.
2. Require an independent active human to clear private named-voice use after an
   explicit personal-data/biometric-risk review. This lane permits no biometric
   artifact or machine identity output.
3. Require a different active human to listen to the complete interval and attest
   exactly one live voice, with overlap, playback, TTS, synthetic voice, and unknown
   audio origin absent. Source/channel context, transcript text, machine identity,
   and machine confidence are expressly not identity evidence.
4. Store privacy and speaker actions as independent append-only chronological streams.
   Withdrawals, disputes, and rejections never rewrite history. Conflicting current
   overlapping identities fail closed across every rendition backed by the same
   media object; rendition aliases cannot bypass the media-local time conflict.
5. Make validation/dry-run non-mutating and import opt-in with `--apply`. Exact replay
   is idempotent; all other collisions fail. Apply requires the exact
   `input_sha256` returned for the stable-read manifest bytes, is atomic, and cites
   immutable generic human review rows.
6. Keep the entire lane private. It has no publication schema or exporter, carries
   no public label or confidence, refuses preexisting reserved publication/gate state
   at migration time, and blocks every later publication or gate decision for its
   object types.

## Consequences

Most confirmed one-speaker recordings can receive precise private named-voice
timestamps after brief human listening without running cross-video recognition. A
source owner, familiar channel, transcript phrase, face on screen, or high model score
still cannot create that attribution. Playback, reaction inserts, TTS, synthetic
voices, overlap, guests, and uncertain audio stay unnamed or use other review lanes.

This decision does not publish named speaker segments, alter transcripts, identify a
speaking face, approve biometric processing, or relax ADR 0007's anonymous model
gates. A future public speaker-label proposal requires a new ADR, explicit release
schema, rights/privacy/sensitivity policy, and adversarial export tests.
