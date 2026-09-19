# Hash-bound local windows for long recordings

`local-window` is the processing half of the fail-closed long-recording path. It does
not download media. It accepts only a completed guarded-acquisition result for a
public source, rehashes the full admitted parent, independently verifies duration and
primary streams with pinned FFprobe, and creates an immutable private work-order
bundle.

## Why the full parent comes first

yt-dlp documents `--download-sections` as an FFmpeg-dependent time-range feature, and
its [official FAQ](https://github.com/yt-dlp/yt-dlp-wiki/blob/master/FAQ.md#downloading-clips-and-cutting-out-sponsor-sections-is-inaccurate)
states that exact cuts require re-encoding. FFmpeg likewise documents that input
seeking ordinarily lands on an earlier seek point and that accurate transcoding
decodes and discards the extra material; see the official
[`-ss` documentation](https://ffmpeg.org/ffmpeg.html#Main-options).

More importantly, separate remote section requests do not bind every returned byte to
one immutable parent download. Formats, manifests, and transient media URLs can change
between calls. The evidence boundary is therefore:

1. acquire the complete public object under one explicit byte cap;
2. hash and admit that object once;
3. derive every processing window locally from that exact SHA-256 parent; and
4. retain source-time mappings while marking every derivative as non-original.

The half-open `[start_ms, end_ms)` coordinates are exact contract coordinates. The
audio and video artifacts are transcoded analysis representations, not byte-exact
source fragments. Their result explicitly records `byte_exact_source_fragment: false`
and `artifact_zero_maps_to_source_ms`.
That field is the declared coordinate transform, not a claim that a source packet
exists exactly on the boundary. Downstream timestamp admission must still check for
stream gaps, discontinuities, encoder delay, and boundary padding before treating a
word or frame alignment as calibrated evidence.

## Materialize a bundle

Calculate the executable hashes independently, then run:

```sh
pipeline/bin/local-window materialize \
  --acquisition-result /srv/himr-private/media-cache/jobs/JOB/ORDER_SHA/result.json \
  --bundle-root /srv/himr-private/window-bundles \
  --window-output-root /srv/himr-private/window-artifacts \
  --ffmpeg /usr/bin/ffmpeg \
  --ffmpeg-sha256 FFMPEG_SHA256 \
  --ffprobe /usr/bin/ffprobe \
  --ffprobe-sha256 FFPROBE_SHA256 \
  --chunk-duration-ms 1800000 \
  --max-windows 64 \
  --max-window-output-bytes 4294967296 \
  --free-space-floor-bytes 107374182400 \
  --timeout-seconds 7200
```

The result follows
[`schemas/local-window-bundle-manifest.schema.json`](schemas/local-window-bundle-manifest.schema.json).
Work orders follow
[`schemas/local-window-work-order.schema.json`](schemas/local-window-work-order.schema.json).
The bundle covers the parent duration exactly: it starts at zero, contains no gap or
overlap, and its last window ends at the independently probed duration. A short final
tail is retained and marked.

Materialization is offline and immutable. It rejects duplicate JSON keys, an altered
acquisition result, a changed parent, mismatched acquisition/local durations, changed
tool bytes or version output, too many windows, unsafe paths, and writable/tampered
replay bundles.

## Execute or inspect one window

```sh
pipeline/bin/local-window run \
  --work-order /srv/himr-private/window-bundles/bundles/windowbundle_ID/work-orders/000001.json \
  --dry-run

pipeline/bin/local-window run \
  --work-order /srv/himr-private/window-bundles/bundles/windowbundle_ID/work-orders/000001.json
```

Each work order is independently resumable at the job level. A completed sealed result
is reused only after its exact result bytes, entry set, modes, artifact sizes, and
SHA-256 values pass replay validation. An incomplete or failed window admits nothing;
its staging tree is removed without touching other windows.

FFmpeg runs in a separate process group. The runner enforces the per-window combined
output cap, free-space floor, command timeout, and an 8 MiB diagnostic limit while the
process is live. It rehashes the parent before and after processing. Output is private,
grants no publication authority, carries no identity claim, and is never a substitute
for the full source object.

Normalized FLAC can feed bounded ASR work. The CFR proxy is suitable for later sparse
vision routing, but downstream adapters must preserve the recorded source offset when
converting window-relative timestamps to recording time. A separate, private-only
catalog admission boundary is documented in
[`corpus/docs/LOCAL_WINDOW_RESULT_INGESTION.md`](../corpus/docs/LOCAL_WINDOW_RESULT_INGESTION.md).
That boundary independently revalidates the current bytes and may create an exact,
catalog-bound ASR artifact reference; neither this producer nor that admission path
grants public, identity, or claim authority.

Provider challenges remain outside this offline stage. If full public acquisition is
temporarily blocked by a bot check or rate limit, keep the acquisition work order and
retry without credentials after public access recovers. Do not create windows from an
unadmitted remote clip.
