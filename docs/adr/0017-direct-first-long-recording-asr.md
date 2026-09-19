# ADR 0017: Direct-first long-recording ASR with logical-span recovery

- Status: accepted for isolated implementation; corpus admission pending
- Date: 2026-08-30

## Context

The ordinary GPU lane deliberately admits only bounded recordings. Longer recordings
are retained as complete parent media and parked instead of being partially
transcribed. The older local-window lane makes persistent audio and video derivatives
for each fixed window. That is useful when a bounded media artifact is independently
required, but it is a poor default for transcription alone: it duplicates storage,
adds FFmpeg work, creates extra identities, and turns boundary reconciliation into a
property of files rather than of one recording.

Faster Whisper already decodes a recording as a sequence of model windows. A moderate
recording can therefore be submitted as one logical job and returned as one
transcript. Extremely long inputs still need bounded restart points: one crash near
the end must not discard hours of inference, and decoding an entire multi-hour audio
array at once eventually becomes an avoidable host-memory risk.

The production Archive campaign was running when this successor was implemented. Its
controller configuration, queues, receipts, results, locks, and operator actions are
outside this change.

## Decision

Add a separate, opt-in long-recording lane. It is recording-first rather than
chunk-first:

```text
one hash-bound 16 kHz parent audio object
                 |
                 v
       deterministic recording plan
          /                    \
 direct whole recording     adaptive logical spans
          \                    /
                 v
     immutable local hypotheses
                 |
                 v
  one recording transcript + coverage ledger
```

### Direct first

A recording at or below a separately admitted duration and memory budget is passed to
the engine once. The engine's native internal windows are implementation detail, not
corpus artifacts. The result still covers the complete parent timeline, including
intervals where no speech was returned.

The initial duration threshold is a policy input, not a claim that a particular GPU
or recording length has been soaked. Production thresholds require short, 30-minute,
2-hour, 4-hour, and 8-hour canaries plus memory, thermal, timestamp, and accuracy
review. This implementation does not run those canaries automatically.

### Adaptive recovery without persistent media chunks

Above the direct threshold, the planner creates logical source views. Coordinates are
half-open exact sample ranges at 16,000 Hz. A direct-attempt failure currently remains
pending; automatically classifying that failure and deriving a replacement adaptive
plan is deferred so infrastructure or source-integrity failures are not mistaken for
memory pressure. Each view has:

- a non-overlapping core, whose union is exactly `[0, parent_total_samples)`;
- an analysis range that may add left and right context;
- a deterministic ordinal and boundary reason; and
- no independently materialized audio-file identity.

Where admitted activity or silence observations exist, the planner may move a core
boundary to a nearby quiet point. Otherwise it uses a deterministic bounded fallback.
Activity chooses boundaries only; it never removes quiet intervals from the coverage
contract. The executor reads a logical range from the one verified parent and emits
only transcript hypotheses and execution evidence. A small rolling working buffer is
allowed, but durable FLAC/WAV fragments are not.

### Ownership, reconciliation, and completeness

Every decoded word or segment is projected from its analysis-local coordinate into
the parent sample coordinate system. Core ownership makes adjacent outputs
deterministic: context can improve a boundary hypothesis, but it cannot make the same
time range canonical twice. The assembler retains duplicate/conflict evidence rather
than silently inventing agreement.

Word rows are intentionally compact. Native rows carry text, paired exact start/end
samples when the engine supplies word timing, and only a present raw word score or
anomaly flag. If any native word in a segment has no timing, the assembler preserves
that native evidence but emits one segment-owned unit with `words: []`; it never
fabricates exact per-word coordinates from the segment interval. Recording identity,
span lineage, model/decoder provenance, policy, language, and coordinate semantics are
recorded once at document, result, or segment scope instead of being repeated for every
word.

A recording is complete only when every core interval has one explicit disposition:

- `decoded`: a valid hypothesis was returned;
- `no_speech`: the engine completed the interval and returned no speech;
- `failed`: execution was attempted but did not complete; or
- `pending`: execution has not completed.

The assembler and bindings contract reserve `failed` for a future scheduler failure
receipt. The current isolated runner commits only completed results; after an
exception, an absent result therefore replays as `pending` and can be retried. It does
not yet claim durable attempted-versus-unattempted failure lineage.

Only a ledger containing `decoded` or `no_speech` for the entire parent may claim full
machine coverage. A list of speech segments alone is never proof of completeness.
Failed and pending plans remain resumable at their exact logical span; completed raw
hypotheses are immutable.

### Isolation and scheduling

The lane has new contract kinds, state roots, output roots, and commands. It is not
registered with the running autonomous controller and cannot consume or mutate that
controller's state. Real CUDA execution must use the same GPU-UUID exclusive lock as
the ordinary ASR worker. The initial scheduler permits one long recording and one
resident model process; it does not run another CUDA model concurrently.

Planning, replay, assembly, and fake-engine tests are CPU-only. A real engine is an
explicit operator action. Publication, catalogue import, speaker identity, wiki
mutation, source deletion, and retraction are outside this lane.

## Consequences

- Moderate recordings avoid persistent chunk files and repeated model loads.
- Long-tail recordings gain bounded restart points without losing the identity of the
  full recording or duplicating normalized audio on disk.
- Exact sample ownership makes gaps, overlap, and boundary disagreements auditable.
- One assembled transcript is easier to search than hundreds of fragments, while raw
  logical-span evidence remains available for later reassembly improvements.
- Silence-boundary quality and the direct-duration threshold still require real-media
  evaluation before this lane is attached to the autonomous campaign.
- The physical local-window lane remains available for visual analysis or other work
  that genuinely needs bounded media artifacts; it is no longer the preferred ASR
  representation.
