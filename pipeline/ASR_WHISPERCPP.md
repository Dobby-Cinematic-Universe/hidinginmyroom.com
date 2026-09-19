# Offline whisper.cpp ASR contract

`asr_whispercpp.py` is the first transcription adapter. It invokes one explicitly
supplied, hash-pinned `whisper-cli` executable with one explicitly supplied, hash-pinned
model and a normalized local audio artifact. It never downloads a binary, model,
glossary, or media file, and it never publishes or edits wiki content.

**Status:** contract-, fixture-, and real-pilot-tested. A successful execution is not
an accuracy or calibration validation; publication still requires a frozen human
reference and direct media review.

For deterministic raw-pass work orders derived from sealed local-window results and
read-only catalog lineage, use the separate private batch controller documented in
[`ASR_WHISPERCPP_BATCH.md`](ASR_WHISPERCPP_BATCH.md). The adapter contract on this page
remains the execution authority for every emitted order.

The adapter targets whisper.cpp's documented `--output-json-full` interface. That
format includes segment offsets and token text, IDs, probabilities, optional offsets,
and DTW values. The upstream CLI flags used here are documented in the official
[whisper.cpp CLI README](https://github.com/ggml-org/whisper.cpp/tree/master/examples/cli),
and the emitted JSON fields are defined by the official
[`cli.cpp`](https://github.com/ggml-org/whisper.cpp/blob/master/examples/cli/cli.cpp)
implementation. A caller must still pin a tested executable commit and hash; `master`
is not a runtime version.

## Input boundary

Every work order must identify:

- a 16 kHz, mono, signed-16-bit FLAC produced by preprocessing, with exact SHA-256,
  `media_id`, parent artifact ID, and parent processing-run ID;
- an absolute executable path, exact executable hash, reviewed version label, and
  caller-recorded repository revision, target, and build configuration;
- an absolute model path, exact model hash, model ID, exact revision, source, and
  reviewed license label;
- a language, bounded half-open audio window, thread count, timeout, and complete
  decoding parameters;
- an optional checksummed neutral glossary revision;
- optional caller-supplied recording/rendition catalog IDs; and
- an explicit durable output root outside `/`, `/tmp`, and `/var/tmp`.

Runtime validation rejects unknown keys. Input, executable, model, and glossary paths
must exist, be outside the output root, and remain byte- and stat-identical throughout
an actual run. FFprobe rejects anything other than one 16 kHz mono s16 FLAC stream.
The requested window may be at most 24 hours and must fit inside the probed input.

Execution requires Linux with a mounted `/proc/self/fd`. Before probing, the adapter
opens the input, executable, model, and optional glossary as regular non-symlink files,
hashes their retained descriptors, and binds an internal fingerprint containing device,
inode, size, mtime, ctime, file type, permission mode, and link count. FFprobe reads the
retained input descriptor; whisper.cpp executes from the retained executable descriptor
and receives retained model and input descriptors. The glossary is parsed directly from
its retained descriptor. Descriptors are inherited only by the applicable child through
`pass_fds`. After child execution, the adapter rehashes every descriptor and requires
its original logical path to retain the exact internal fingerprint. A rename-away and
restore, permission change and restore, hard-link change, or byte mutation therefore
fails the run even if size and mtime were restored. Platforms or sandboxes without the
Linux proc descriptor transport fail closed before FFprobe or whisper.cpp runs.

The complete schema is
[`schemas/asr-whispercpp-work-order.schema.json`](schemas/asr-whispercpp-work-order.schema.json).
Start from
[`examples/asr-whispercpp-work-order.example.json`](examples/asr-whispercpp-work-order.example.json)
and replace every placeholder, especially hashes, revisions, build options, model
license, and catalog IDs.

Validate without inference:

```sh
pipeline/bin/asr-whispercpp validate \
  --work-order /srv/himr-work-orders/asr-pilot-001.json
```

## Neutral glossary

The optional glossary is a separate, checksummed JSON document following
[`schemas/neutral-glossary.schema.json`](schemas/neutral-glossary.schema.json). It may
contain only a versioned language and vocabulary terms—no biographies, relationships,
claims, corrections, desired sentences, or editorial instructions. The adapter builds
the bounded prompt `Vocabulary terms, spelling only: …` and records its hash.

[`examples/neutral-glossary.example.json`](examples/neutral-glossary.example.json) is a
minimal example. Glossary-assisted output is labeled `contextual_asr`; an unprompted
run is `raw_asr`. Both remain machine review state.

## Dry run

```sh
pipeline/bin/asr-whispercpp run \
  --work-order /srv/himr-work-orders/asr-pilot-001.json \
  --dry-run
```

A dry run hashes the audio, binary, model, and optional glossary, probes the audio from
its retained descriptor, records the pinned source/build identity, resolves the window,
and prints two child-facing commands. `commands[0]` is the exact descriptor-backed
FFprobe argv that ran; `commands[1]` is the exact descriptor-backed whisper.cpp argv
that was planned but was not executed. Proc descriptor numbers are execution-local,
so canonical `processing_run.environment_json.command_provenance` separately records
the deterministic logical commands with the original input paths and final output path,
plus the `executed`/`planned` state of each child-facing command. It creates no output.
The recipe ID is deterministic; the planned execution-run ID is fresh and therefore
deliberately different on each dry run.

Runtime admission does not depend on whether a particular `whisper-cli` build exposes
a version command. Executable identity is verified from its exact SHA-256 and the work
order records `source_revision_plus_executable_sha256` as the version evidence. The
human-readable version label is an audit label supported by the pinned source revision
and build record.

The exact whisper.cpp v1.8.3 executable profile (SHA-256
`4831024debb4e60e9433d27967ba6dae033d4b0c770c4ef208c16fc5a8fe77d6`, 1,010,544
bytes) is retained for historical validation only. Work-order shape validation and the
sealed-batch replay described in
[`ASR_WHISPERCPP_BATCH.md`](ASR_WHISPERCPP_BATCH.md) remain available, but direct
adapter `run` rejects that exact profile in both dry and non-dry modes before creating
an output root. New execution must use a reviewed current engine, and current batch
work uses the exact shared v1.8.7 profile.

The wrapper itself contains no network code and supplies a minimal environment without
proxy variables or ambient caches. A caller-supplied native executable cannot be
proved network-free from Python, so production workers must also disable network
access at the container or service boundary. The executable is forced to CPU mode with
`--no-gpu` in contract version 1.

## Actual run and immutable layout

```sh
pipeline/bin/asr-whispercpp run \
  --work-order /srv/himr-work-orders/asr-pilot-001.json
```

The deterministic recipe SHA covers the adapter version, exact binary/model identity,
build provenance, decoding parameters, resolved window, output contract, and optional
glossary/prompt hashes. It also binds the Linux retained-descriptor execution policy.
`recipe_id` is deliberately distinct from the random UUID-based `processing_run_id`
of a real execution. A separate deterministic result key also
binds the exact work-order digest and input/catalog identities.

whisper.cpp writes only into a private staging directory. The adapter parses and
validates full JSON, builds normalized integer-millisecond rows, checks every immutable
input again, writes both artifacts, then renames the complete directory atomically:

```text
<output-root>/asr/whispercpp/sha256/ab/<input-sha256>/results/<result-key>/
├── whisper.raw.json
├── transcript.normalized.json
└── result.json
```

For a completed result, top-level `commands` contains the exact child-facing argv that
ran, including `/proc/self/fd/<n>` input bindings and the private staging prefix.
`processing_run.environment_json.command_provenance.logical_commands` retains the
deterministic original paths and final logical output prefix. The importer validates
both representations and their relationship; neither is presented as the other.

Both artifacts have `visibility: private`. The raw upstream JSON is retained byte for
byte. The normalized artifact and envelope retain every segment/token span, token ID,
raw probability `p`, raw DTW value `t_dtw`, and canonical raw segment/token JSON in
`metadata_json`. No calibrated confidence is invented. Catalog `asr_log_probability`
is the mathematical log of the preserved raw token probability when it is greater than
zero; calibration fields remain null.

whisper.cpp may give a control token—or occasionally a lexical token—a zero-length
timestamp span. Those equal offsets are retained exactly. Since adapter 0.2.3, it also handles
one narrowly observed upstream defect: two structurally valid nonnegative token
offsets can be inverted. It leaves the segment timing untouched, sets only that
token's normalized `start_ms` and `end_ms` to null, retains the exact pair in
`original_offsets`, and records `timing_state: unavailable` with
`invalid_upstream_inverted`. The raw token remains intact in both raw JSON and
`metadata_json`. It never swaps, clamps, or substitutes a segment boundary for the
invalid token span. Malformed, negative, oversized, and non-inverted token spans that
escape their segment still fail closed.

A segment that starts inside the requested window may also end slightly beyond its
boundary. Overruns up to one 30-second Whisper context window are retained with an
explicit timing-quality flag and millisecond overrun; a segment starting outside the
window or exceeding that bound is rejected. The adapter never stretches or clips a
timestamp silently. Affected transcript revisions and segments carry
`token_timing_unavailable`; catalog segment metadata lists the token ordinal, exact
original offsets, and quality flag while the catalog word timing remains null.

If a completed result already exists, the adapter verifies both artifact paths, sizes,
hashes, run-scoped artifact IDs, raw JSON readability, and equality between the
normalized artifact and the envelope. Only then does it return the original envelope
unchanged, including its original unique processing-run ID. A mismatch is an integrity
failure: no artifact or completed result is repaired or overwritten automatically.

Failures are emitted as a strict JSON envelope on stderr and return status 2. Adapter
0.3.0 decodes `--output-json-full` bytes with strict UTF-8 before JSON parsing. If the
engine emits an invalid sequence, the adapter never ignores or replacement-decodes it:
the full exact byte stream and a deterministic lineage receipt are atomically retained
under the owner-private content-addressed quarantine tree
`<output-root>/quarantine/whispercpp-output-json-full/sha256/...`. The immutable bundle
is replay-verified on retry; a changed receipt, raw byte, mode, link count, or directory
entry fails closed. No incomplete ASR result directory survives, and neither the raw
bytes nor decoded text are copied into stderr. The receipt shape is
[`schemas/asr-whispercpp-quarantine-receipt.schema.json`](schemas/asr-whispercpp-quarantine-receipt.schema.json).

Other staging from a failed execution is removed, but every input remains untouched.
The result and failure shapes are in
[`schemas/asr-whispercpp-result.schema.json`](schemas/asr-whispercpp-result.schema.json).

## Catalog and review boundary

The result mirrors its processing-run, run-input, and artifact objects exactly under
`catalog_records`. `input.media_id` must be `media_sha256_<input-sha256>`, and artifact
IDs include the unique processing-run ID.

When `catalog_context` is supplied, deterministic transcript revision, segment, and
token rows are generated from the caller's IDs and ordinals. They are never inferred.
When context is null, transcript catalog keys are omitted rather than inventing a
recording identity. Every revision has `review_state: machine`; the producer emits no
publication decision. Import, review, correction, evidence use, and public release are
separate operations.

The catalog must already contain or transactionally register the caller-supplied model
and optional glossary revision before inserting a processing run that references them.
This producer deliberately does not invent registry records from filenames.

## Real CPU pilot

On 2026-08-26, adapter 0.2.2 completed a private five-minute English pilot against a
public commentary recording using whisper.cpp v1.8.3, `small.en`, six CPU threads, and
no glossary. The run took 100.91 seconds (real-time factor 0.3364; peak RSS about
1.16 GiB), produced 102 segments and 1,325 preserved tokens, passed the strict result
schema, and was immutably revalidated/reused in 0.95 seconds. Its final segment began
inside the requested window and carried a recorded 5,120 ms boundary overrun.

The 1,212 non-special token scores are raw decoder values, not calibrated
probabilities. No catalog recording identity or publication decision was emitted. The
wrapper transcription matched the earlier exploratory direct-CLI transcription after
canonicalization; only the absolute model-path string in engine metadata differed.

## Tests

Run all pipeline tests:

```sh
pipeline/tests/run.sh
```

The ASR suite uses a real FFmpeg-generated 16 kHz mono s16 FLAC and a fake local
whisper-cli/model. It exercises version and hash provenance, the exact command,
glossary construction, token scores/timestamps, catalog rows, no-write dry runs,
immutable verified reuse, tamper refusal, input normalization, zero-length token
spans, provenance-preserving inverted-token handling, rejection of negative and
ordinary out-of-segment spans, flagged boundary overruns, bounded windows, strict
unknown-key handling, safe output roots, null catalog context, and exact-byte invalid
UTF-8 quarantine/replay/tamper behavior. It also exercises descriptor-backed fake
execution, transient logical-path swap/restore rejection, and missing-proc fail-closed
behavior. It does not obtain or run a real model during tests and makes no network
request.
