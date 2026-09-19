# Private source/recording transform calibration evidence

The `pipeline/bin/transform-calibration` entry point preserves machine evidence about
competing source-time to recording-time hypotheses. It is deliberately separate from
catalog promotion. The producer has no database argument or SQLite import, reads no
transcript text, performs
no network request, and emits no review, relationship, timeline, transcript, or
publication decision.

The current use case is a YouTube livestream acquired while yt-dlp still reported
`post_live`, then reacquired after the same public video became `was_live`. YouTube may
replace or extend finalized delivery formats after a stream. A platform duration
captured during that transition is therefore not sufficient evidence for either
endpoint scaling or identity mapping.

## Exact credential-free reacquisition

Use the guarded acquisition runner with the exact numeric video+audio format pair from
the earlier result. The acquisition boundary accepts only its two generic profiles or
one closed numeric pair such as `396+140`; arbitrary selectors, filters, names, and
fallback expressions remain forbidden.

```sh
yt_dlp=/absolute/private/tools/yt-dlp
yt_dlp_sha256=<sha256-of-executable>
capture=/absolute/private/finalized-source-capture

acquisition/bin/acquire create-work-order \
  --job-id finalized-youtube-VIDEO_ID-exact-formats-v1 \
  --adapter yt_dlp \
  --platform youtube \
  --source-kind youtube_video \
  --native-id VIDEO_ID \
  --canonical-url 'https://www.youtube.com/watch?v=VIDEO_ID' \
  --access-state public \
  --url 'https://www.youtube.com/watch?v=VIDEO_ID' \
  --yt-dlp-executable "$yt_dlp" \
  --yt-dlp-sha256 "$yt_dlp_sha256" \
  --format-selector 'VIDEO_FORMAT+AUDIO_FORMAT' \
  --output-root /absolute/private/acquired \
  --max-job-bytes 1073741824 \
  --global-cache-cap-bytes 8589934592 \
  --free-space-floor-bytes 171798691840 \
  > "$capture/acquisition-work-order.json"

acquisition/bin/acquire run \
  --work-order "$capture/acquisition-work-order.json"
```

The existing acquisition policy supplies `--ignore-config`, a fresh empty home,
no-playlist and no-sidecar flags, no cookie/browser/token option, a caller-pinned
yt-dlp executable, one public YouTube URL, capacity limits, content-addressed
admission, and a normalized FFprobe record. A finalized calibration requires the same
exact `video+audio` format IDs in both completed results, `post_live` for the earlier
capture, and `was_live` for the finalized capture.

The acquisition evidence proves the observed yt-dlp launcher bytes, file stat, and
reported version. It proves a full imported yt-dlp runtime tree only when the source
work order contains the optional runtime-tree pins. The calibration conclusion itself
rests on the hash-bound media and independently pinned FFmpeg/FFprobe audiovisual
comparisons, not on a claim that an unpinned launcher runtime was identical.

Do not use an expiring delivery URL as retained evidence. The durable inputs are the
work-order and acquisition-result bytes, admitted media SHA-256s, minimized platform
metadata, and exact local probes.

## Produce a receipt

Choose at least three sorted source-time checkpoints covering the beginning, middle,
and final tenth of the earlier media. Every visual and audio window must remain inside
the earlier acquired bytes.

```sh
pipeline/bin/transform-calibration produce \
  --old-work-order /absolute/private/earlier-work-order.json \
  --old-result /absolute/private/earlier-result.json \
  --finalized-work-order /absolute/private/finalized-work-order.json \
  --finalized-result /absolute/private/finalized-result.json \
  --ffmpeg /usr/bin/ffmpeg \
  --ffprobe /usr/bin/ffprobe \
  --identity-candidate-id srtc_... \
  --scaled-candidate-id srtc_... \
  --declared-recording-duration-ms 13300000 \
  --checkpoint-ms 0 \
  --checkpoint-ms 6000000 \
  --checkpoint-ms 13280000 \
  --output /absolute/git-ignored/private/transform-calibration/receipt.json
```

The output must be an absolute Git-ignored path whose parent chain contains no
symlinks. The current producer rechecks that chain, opens the final directory with
no-follow semantics, and performs its temporary write, chmod, replace, and fsync
relative to the pinned directory descriptor. The receipt is sealed mode `0400` and
replayed only when its complete bytes and mode match. No capture or receipt belongs
under `src/`, `public/`, `dist/`, or a static corpus release.

The default comparison recipe performs, at every checkpoint:

- a five-second FFmpeg SSIM comparison at the identity timestamp;
- the same SSIM comparison at the endpoint-scaled timestamp;
- a 30-second, 4 kHz mono audio correlation search around the identity timestamp;
- a narrowly bounded correlation search around the scaled timestamp; and
- explicit identity-versus-scaled margins and predicted accumulated drift.

The producer requires high distributed identity similarity, stable audio offset across
all checkpoints, and at least two independently discriminating checkpoints where both
visual and audio evidence contradict endpoint scaling. It records decoded PCM hashes,
numeric SSIM payload hashes, every probe time base/duration counter, the exact FFmpeg,
FFprobe, Python, NumPy and SciPy identities, and full NumPy/SciPy runtime-tree hashes.
Process-specific FFmpeg log prefixes are not retained as evidence.

The four conclusion-bearing thresholds have approved lower bounds: identity SSIM
`0.98`, identity audio correlation `0.95`, visual margin `0.03`, and audio margin
`0.10`. Callers may make a run stricter, but neither the CLI, programmatic producer,
nor schema accepts weaker values.

The earlier media's probed video duration defines the half-open shared-prefix coverage.
If the finalized video is longer, the receipt records the exact uncovered finalized
tail separately. It never pretends the earlier bytes contain that tail. Machine ASR
endpoints outside the calibrated prefix remain outside promotion coverage; no clipping
or extrapolation is authorized.

## Validate exact replay

```sh
pipeline/bin/transform-calibration validate \
  --receipt /absolute/git-ignored/private/transform-calibration/receipt.json
```

Validation rehashes and revalidates both guarded acquisitions and media objects,
re-probes both files, rehashes all tools and numerical runtime trees, reruns every
distributed comparison, recomputes the semantic receipt identity, and requires exact
receipt bytes. Changed media, metadata, tools, checkpoints, comparison output, package
tree, receipt path, or file mode fails closed.

The current producer is implementation v0.2.0. The byte-identical v0.1.0 module is
retained only as a SHA-256-pinned replay engine for receipts it originally produced;
the public CLI does not use it to create new receipts. This keeps historical receipt
bytes and implementation evidence stable while allowing the active producer to fail
closed on the stronger threshold and output-path policy.

The normative output contract is
[`schemas/transform-calibration-receipt.schema.json`](schemas/transform-calibration-receipt.schema.json).
Runtime replay is authoritative.

## Authority boundary

An accepted receipt supports only a private evidentiary statement about its hash-bound
media pair and covered interval. Its safety fields are fixed to:

- `catalog_opened: false` and `catalog_mutated: false`;
- `catalog_transform_decision: null` and `review_decision: null`;
- `publication_decision: null` and `relationship_decision: null`;
- `timeline_application_allowed: false`;
- `transcript_projection_allowed: false`; and
- publication, relationship, and review authority `none`.

A future, separately reviewed catalog migration and promotion tool must consume this
evidence explicitly. This producer must not be widened into that decision boundary.

## Requirements and tests

This isolated lane requires local FFmpeg/FFprobe plus NumPy and SciPy. The base media
preprocessor remains standard-library-only.

```sh
python -m unittest pipeline.tests.test_transform_calibration
python scripts/validate-json-contracts.py
```

The focused suite creates short synthetic prefix/finalized media, obtains both through
the guarded fake-yt-dlp acquisition path, proves exact receipt replay, checks schema
validation and scaled-hypothesis contradiction, rejects unsafe output and tampering,
rejects below-floor thresholds and symlinked output paths, and verifies the producer
has no catalog or publication path. The retained v0.1.0 implementation SHA-256 is also
locked by the suite.
