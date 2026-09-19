# Private whisper.cpp/Silero voice-activity adapter

`pipeline/bin/whispercpp-vad` is a deterministic, offline producer of private
**speech-candidate intervals**. It is a cheap routing stage for deciding where later
diarization or active-speaker analysis may be useful. It does not identify a voice,
count speakers, associate a face with speech, detect actions, or publish anything.

The adapter accepts only sealed 16 kHz, mono, 16-bit FLAC work units of at most two
hours and 512 MiB. It reads the exact sample count from FLAC `STREAMINFO`, retains and
hashes the input, executable, and model, and runs CPU-only. The executable and model
must be current-user-owned, single-link files at modes `0500` and `0400`; immediately
before execution their verified bytes are copied into anonymous Linux `memfd` objects
with write/grow/shrink seals. The child receives only those sealed engine/model
descriptors and the retained input descriptor through `/proc/self/fd`.

Result staging walks and retains the entire output component chain with directory
descriptors, `mkdirat` semantics, and `O_NOFOLLOW`; a replaced logical ancestor cannot
redirect later creation. Files are created relative to the retained owner-private
parent, and Linux `renameat2(RENAME_NOREPLACE)` publishes the result without replacing
a concurrent winner. Child stdout/stderr are drained concurrently and the process is
killed as soon as either byte cap or the time limit is exceeded. A dry run performs
the same hashes, profile checks, sealed-copy preflight, output-path checks, and
existing-result replay validation, but creates no output.

## Exact admitted profile

The initial lane is deliberately narrow:

| Component | Version / revision | SHA-256 | Bytes |
| --- | --- | --- | ---: |
| `whisper-vad-speech-segments` | whisper.cpp v1.8.7, commit `48f628a84833905ee4a0658ee6d4a5c915ce1997` | `ca7828ddc277c93daf5f356a52e853f0d4964933c2e1f01652924ec9a4e7d39e` | 658,912 |
| Silero VAD GGML weights | Silero v6.2.0 | `2aa269b785eeb53a82983a20501ddf7c1d9c48e33ab63a41391ac6c9f7fb6987` | 885,098 |

The reviewed model is the MIT-licensed
[`ggml-silero-v6.2.0.bin`](https://huggingface.co/ggml-org/whisper-vad/blob/main/ggml-silero-v6.2.0.bin)
published by the whisper.cpp project. The matching upstream Silero release is also
MIT-licensed. The local checkout may name the byte-identical file
`for-tests-silero-v6.2.0-ggml.bin`; filename is not identity.

## Important upstream timing and CLI facts

The v1.8.7 example prints `whisper_vad_segments_get_segment_t0/t1()` values with two
decimal places. Those C API values are **centiseconds**, not seconds. The adapter
preserves the exact printed strings and integer centiseconds, then multiplies by ten
to obtain artifact-local milliseconds. This distinction is covered by regression
tests; treating a printed `4058.00` as seconds would be a hundredfold error.

The same v1.8.7 example has a source-level option-parser defect:

- the documented short minimum-speech flag is not the parsed short flag;
- the minimum-silence branch assigns the minimum-speech field;
- the duplicated short-flag branch makes the intended silence control unreliable.

Contract v1 therefore uses the exact reviewed source defaults and omits both broken
arguments. It records `parameter_binding` as
`reviewed_source_defaults_with_broken_cli_fields_omitted_v1`. The fixed values are a
0.5 threshold, 250 ms minimum speech, 100 ms minimum silence, unlimited maximum
speech duration, 30 ms padding, 0.1-second overlap, and CPU execution. A later
configurable profile requires either an upstream fix or a separately reviewed,
hash-pinned build.

The engine rounds to a 10 ms grid and may describe its padded analysis tail slightly
beyond the exact FLAC sample duration. The adapter retains the raw coordinate, permits
at most 64 ms of explicit tail overrun, clips only the normalized end to the exact
input duration, and labels the affected interval. Larger overruns fail closed.

## Work order and execution

The schema and non-resolving example are:

- `schemas/whispercpp-vad-work-order.schema.json`
- `examples/whispercpp-vad-work-order.example.json`

Validate shape and resolved local paths:

```sh
pipeline/bin/whispercpp-vad validate \
  --work-order /srv/himr-private/work-orders/vad-001.json
```

Hash, inspect FLAC metadata, match the exact engine/model profiles, and plan without
writing:

```sh
pipeline/bin/whispercpp-vad run \
  --work-order /srv/himr-private/work-orders/vad-001.json \
  --dry-run
```

Execute:

```sh
pipeline/bin/whispercpp-vad run \
  --work-order /srv/himr-private/work-orders/vad-001.json
```

The content-addressed layout is:

```text
<output-root>/vad/whispercpp/sha256/ab/<input-sha256>/results/<result-key>/
├── engine.stdout.txt
├── engine.stderr.txt
└── result.json
```

Files are mode `0400`, the completed directory is mode `0500`, and exact replay
returns the original envelope after revalidating modes, hashes, raw stdout, and the
normalized segment projection. Existing partial, mutable, extra, or altered content
is rejected rather than repaired. Artifact descriptors must contain stdout followed
by stderr exactly once. Output roots under `/tmp` or `/var/tmp` are prohibited; a
repository-local root is allowed only under the ignored `research/corpus` tree (plus
the self-cleaning test fixture). Output ancestors are identity-checked before and
after execution, every component is retained during creation, and an appeared
concurrent result is never replaced.

Each normalized interval retains:

- exact raw engine start/end text and integer centiseconds;
- artifact-local integer milliseconds;
- optional caller-supplied `rendition_media_ms` projection;
- an explicit tail-quality state and overrun amount;
- `score: null`, `calibrated_probability: null`, and
  `calibration_state: not_calibrated`.

The 0.5 segmentation threshold is a model decision parameter, **not** a confidence
score. The current example executable exposes no per-frame probabilities through its
stdout contract, so none are invented.

## Real private pilot

On 2026-08-28, the exact profile processed a sealed 1,799,979 ms normalized window in
5.99 seconds in a direct one-thread timing probe (real-time factor about `0.00333`;
peak RSS 122,828 KiB). The frozen v0.3 adapter run, including provenance checks,
sealed execution copies, bounded pipe handling, retained output-component traversal,
and descriptor-relative admission, took 6.006 seconds. It emitted 486 speech-candidate
intervals.
Their normalized union was 923,679 ms, or about `0.51316` of the input. The last raw
endpoint was 1,800,000 ms and was therefore retained with a 21 ms tail overrun while
its normalized endpoint was clipped to the exact FLAC duration.

The current immutable result key is
`f1bcdb8c10e5ae9bd44462c5564d74997e758497d6ea46aa973ca0fd59283bbb`
with result SHA-256
`1600aa1e3b141a79b290d40c34b7a838a5289312f19e94e264d26f78c49c8676`.
An exact replay preserved its inode, timestamps, bytes, and hashes. The earlier v0.1
and v0.2 pilots remain retained as historical evidence, but v0.3 is the current
execution boundary. Its distinct implementation/policy pair prevents the hardened
admission semantics from colliding with either historical recipe identity.

This pilot establishes feasibility and coordinate handling only. It is not an
accuracy evaluation, speaker count, identity decision, calibrated score, or
publication review.

## Tests and trust boundary

```sh
python3 -m unittest pipeline.tests.test_whispercpp_vad -v
python3 -m unittest pipeline.tests.test_whispercpp_vad_schema -v
python3 scripts/validate-json-contracts.py
```

The 27 runtime tests and eight schema tests cover strict JSON and closed result shapes;
sealed, single-link inputs and engine/model assets; hash/size/profile matching; sealed
execution copies and attempted post-seal mutation; FLAC metadata and zero-duration
rejection; bounded numeric parsing and direct oversized-pipe failures; zero-speech
output; malformed/inverted/overlapping intervals; tail clipping and rejection;
nonzero exits and timeouts; retained-path ancestor replacement; descriptor-relative
private output; no-replace contention; dry-run/replay parity; semantic and byte
tampering; exact artifact order and uniqueness; canonical processing times; symlinks;
output denial; and fixed null authority/calibration fields. The private schema canary
targets all three retained pilots, including current v0.3 and historical v0.1/v0.2.

The adapter contains no network client. A caller-supplied native program cannot be
proven network-free by Python alone, so production workers must still disable network
access at the container or service boundary. No result has catalog-import,
speaker-label, identity, review-clearance, or publication authority.
