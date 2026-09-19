# Public Reddit Atom discovery

This lane discovers public post and media locators without using Reddit's JSON API,
credentials, cookies, comments, or media downloads. It is intentionally a discovery
system, not evidence that a post title accurately describes a linked clip.

## Capture the latest public posts

Run this from the repository root. The absolute output path is required, and
`research/` is ignored by Git:

```sh
acquisition/bin/reddit-rss snapshot \
  --subreddit HIMRFAM2 \
  --limit 100 \
  --out-dir "$PWD/research/corpus/reddit-rss/HIMRFAM2"
```

The command makes one unauthenticated `GET` request to the canonical `new` Atom
feed. It sends a descriptive user agent and Atom accept header, but never sends a
cookie or authorization header. Ambient proxy discovery is disabled as well, so a
proxy credential from the environment cannot enter the request. It then writes three
sibling artifacts:

- `rrs_….atom.xml`: the exact response bytes;
- `rrs_….snapshot.json`: request time, response observation time, final URL,
  status, content type, byte count, and exact SHA-256;
- `rdd_….discovery.json`: the minimized, reproducible post and media-locator
  manifest.

Artifacts are immutable: a different payload is never allowed to overwrite an
existing filename. Both the snapshot and minimized manifest are validated against
their sibling payload before the command succeeds.

The exact public feed can itself contain author labels and post-body markup. Keeping
the exact response is necessary to audit its hash, so raw snapshots belong only in
the ignored research workspace. The derived discovery manifest deliberately omits
authors, bodies, and comments. The tool never follows comment feeds.

## Validate or reproduce a parse

```sh
acquisition/bin/reddit-rss validate-snapshot \
  --snapshot "$PWD/research/corpus/reddit-rss/HIMRFAM2/rrs_….snapshot.json"

acquisition/bin/reddit-rss parse \
  --snapshot "$PWD/research/corpus/reddit-rss/HIMRFAM2/rrs_….snapshot.json"

acquisition/bin/reddit-rss validate-discovery \
  --discovery "$PWD/research/corpus/reddit-rss/HIMRFAM2/rdd_….discovery.json"
```

The parser recognizes stable IDs and canonical locators for:

- `v.redd.it` videos and Reddit galleries;
- YouTube watch, short, live, embed, and `youtu.be` links;
- Archive.org items and download files;
- Reddit-hosted and direct image links;
- direct external video-file links.

Every title and locator carries an `unreviewed` state. Surrounding post text is not
retained. Stable provider IDs, rather than titles, drive deduplication.

## Optional public yt-dlp metadata

The parser can minimize an existing public yt-dlp info JSON without copying uploader
or account fields. It accepts only a uniquely matched `v.redd.it` locator, duration,
public/unknown access state, observation time, and the exact info-file hash:

```sh
acquisition/bin/reddit-rss parse \
  --snapshot "$PWD/research/corpus/reddit-rss/HIMRFAM2/rrs_….snapshot.json" \
  --v-reddit-info "$PWD/research/corpus/reddit-rss/HIMRFAM2/1vvver5.info.json" \
  --metadata-observed-at 2026-08-26T12:05:00Z
```

This is metadata enrichment only; the lane has no media downloader. Public Reddit
or YouTube endpoints may return 403, 429, or no-cookie challenges. Those outcomes are
expected, retryable failures and do not create partial snapshots or infer access.

`--metadata-observed-at` is an exact observation instant, not a user-chosen catalog
date. It must be canonical whole-second UTC, must not be later than the parser's
current clock, and must not precede the newest supplied info file's filesystem
modification time. The comparison is deliberately conservative at nanosecond file
precision: if the newest mtime is `12:05:00.000000001Z`, the earliest accepted value
is `12:05:01Z`; `12:05:00Z` is accepted only when the mtime is exactly on or before
that whole-second boundary. Generate the value only after every info JSON is fully
written. A failed check writes no discovery artifact. Existing sealed discovery
manifests remain valid under the v1 schema; this check governs new enrichment.

After enrichment and human selection, the separate guarded media boundary is
documented in [REDDIT_VIDEO_ACQUISITION.md](REDDIT_VIDEO_ACQUISITION.md). A locator
without a positive public duration observation remains `requires_metadata` there.

The tracked representative contract records post `1vvver5`, locator
`v.redd.it/ftkqj9bfg1lh1`, and a 139-second public yt-dlp metadata observation.

## Import private candidates

Validate first, then explicitly import the minimized discovery manifest:

```sh
PYTHONPATH=corpus/src python3 -m himr_corpus \
  validate-reddit-rss-discovery \
  --manifest "$PWD/research/corpus/reddit-rss/HIMRFAM2/rdd_….discovery.json"

PYTHONPATH=corpus/src python3 -m himr_corpus \
  import-reddit-rss-discovery \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --manifest "$PWD/research/corpus/reddit-rss/HIMRFAM2/rdd_….discovery.json"
```

The strict importer uses only existing catalog tables:

- the feed and each post become public-access, unreviewed metadata sources;
- `v.redd.it` IDs become unreviewed recording candidates and open review tasks;
- YouTube and Archive.org IDs deduplicate against their standard catalog source
  identities and receive explicit contextual source relations;
- image, gallery, and direct-file locators remain unreviewed source leads;
- the exact Atom response hash, observation time, final URL, and local artifact path
  are retained in `source_snapshots`.

The importer creates no media objects, renditions, review decisions, publication
decisions, publication-gate clearances, or public search objects. A reviewer must
still check relevance, completeness, rights, privacy, sensitivity, and the media
itself before any later acquisition or publication step.
