# Corpus static-release scale benchmark

Measured on 2026-08-26 in the project container with Python 3.14.1, Node 22.21.1,
Astro 7.2.6, and Pagefind 1.5.2. The fixture contains publication-shaped synthetic
records only; it contains no private or discovered HIMR metadata.

## Reproduce

```sh
python3 corpus/benchmarks/sharded_release_benchmark.py \
  --segments 100000 --recordings 100 --catalog-shard-size 100
```

To exercise Astro and Pagefind without replacing the checked-in release, retain the
benchmark in an explicit temporary directory and point only that build at it:

```sh
python3 corpus/benchmarks/sharded_release_benchmark.py \
  --segments 100000 --recordings 100 \
  --output /tmp/himr-v2-scale

HIMR_CORPUS_DATA_ROOT=/tmp/himr-v2-scale npm run build
```

`HIMR_CORPUS_INDEX_BATCH_SIZE` may be set from 1,000 through 100,000; production uses
25,000. This is a build-memory/number-of-indexes tradeoff, not a publication control.

## Results

| Measurement | 100,000-segment result |
| --- | ---: |
| Release generation | 0.329 s |
| Atomic export | 1.525 s |
| Full hash-tree validation | 0.735 s |
| Python peak RSS | 130,188 KiB |
| Recording shard bytes | 24,644,395 |
| Largest recording shard | 246,556 bytes |
| Catalog shard | 60,565 bytes |
| Active manifest | 919 bytes |
| Corpus landing HTML | 48,971 bytes |
| 100-record browse HTML | 72,643 bytes |
| Largest 1,000-segment recording HTML | 703,205 bytes |
| Batched Pagefind indexes | 4 × 25,000 records |
| Pagefind build | 60.03 s |
| Pagefind-builder peak RSS | 919,640 KiB |
| Pagefind output | 48,515,663 bytes / 100,317 files |
| Main wiki Pagefind count | 49 pages (unchanged) |

The end-to-end unbatched comparison completed in 136.63 seconds at 1,079,180 KiB peak
RSS. Batching reduced the index-only run to 60.03 seconds and 919,640 KiB, and the
isolation checker counted exactly 100,000 corpus records across four bundles. A
headless-browser smoke search loaded all four bundles and returned merged results.

## Million-segment projection

The benchmark's explicit dry projection is 246,443,950 recording-shard bytes for one
million segments. With the default index batch there would be 40 Pagefind bundles.
This is a linear byte projection from the empirical release shards, not a timed or
memory measurement. Real speech, Unicode text, speaker labels, and revision counts
will change compression and size.

Pagefind produced roughly one output file per synthetic segment. Extrapolating that
layout to a million records is not accepted as deploy-ready even though the release
hash tree itself remains bounded. Before that scale, run a real Cloudflare artifact
count/size check and either group searchable transcript windows or adopt a compact
sharded static search representation while preserving exact segment deep links.

