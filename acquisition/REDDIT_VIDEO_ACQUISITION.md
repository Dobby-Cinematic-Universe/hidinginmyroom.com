# Guarded public Reddit clip acquisition

This lane turns explicitly selected `v.redd.it` locators from one sealed public Atom
discovery manifest into immutable guarded-acquisition work orders. It never reads a
Reddit discussion page, comment feed, account, cookie jar, browser profile, token, or
credential. Planning and materialization perform no network request, media download,
catalog mutation, or publication.

The three tracked contracts are:

- [`schemas/reddit-video-selection.schema.json`](schemas/reddit-video-selection.schema.json)
  for a human-chosen list tied to the Atom snapshot;
- [`schemas/reddit-video-plan.schema.json`](schemas/reddit-video-plan.schema.json) for
  deterministic readiness, cap, and budget decisions;
- [`schemas/reddit-video-bundle-manifest.schema.json`](schemas/reddit-video-bundle-manifest.schema.json)
  for the immutable private work-order bundle.

Runtime validation is stricter than JSON Schema. It rejects duplicate JSON keys,
revalidates the exact Atom payload and snapshot, reproduces the minimized discovery
manifest, recomputes every plan decision and content-derived ID, reconstructs every
canonical media URL, hashes the supplied `yt-dlp` executable, and checks exact replay
of an existing bundle.

## Security boundary

The guarded `yt_dlp` adapter accepts Reddit only when all of these are true:

- `source.platform` is exactly `reddit`;
- `source.source_kind` is exactly `reddit_video`;
- `source.native_id` is a bounded stable `v.redd.it` path ID;
- both input URLs equal `https://v.redd.it/<native_id>` byte for byte;
- the provider webpage identity is pinned to the exact Atom post permalink;
- the source is declared public, the executable has a non-null SHA-256 pin, and the
  exact expected `yt-dlp --version` value is sealed;
- a script launcher also pins every regular file in its imported `yt_dlp` module
  tree; a genuinely standalone binary is fully covered by its executable hash;
- the format selector is one of the two fixed bounded selectors in the acquisition
  contract.

Discussion permalinks, lookalike hosts, extra path segments, HTTP, query strings,
credential-like parameters, fragments, playlists, comments, sidecars, subtitles,
thumbnails, unsafe format selectors, and generic sites fail before invocation.
`yt-dlp` runs with `--ignore-config`, a fresh empty home, `--no-playlist`, and explicit
no-comment/no-sidecar flags. Provider extraction may internally request Reddit's
public media-delivery objects, but the retained result identity must still report the
exact requested `v.redd.it` URL and media ID. The webpage field may equal only the
sealed Atom post permalink; a different discussion page or unrelated redirect cannot
be admitted or used as the media input. Every selected media or manifest URL reported
by the extractor must also stay below `https://v.redd.it/<native_id>/`; alternate
schemes, ports, hosts, or media-ID paths fail before admission.

The executable hash, exact version, and (for a Python console-script wrapper) module
tree hash are verified during materialization and again before and after live
execution. This distinction matters for a virtualenv console script: its small
launcher file does not itself contain the imported package code.

## Metadata is a prerequisite

The baseline discovery manifest from the current sealed HIMRFAM2 snapshot has 37
`v.redd.it` locators but no embedded duration observations. Enriched sibling
manifests may add bounded public/no-auth observations without changing the sealed
Atom payload. Missing metadata is intentionally not guessed around: selected clips
appear as `requires_metadata` and cannot become work orders.

Collect public, no-cookie info JSON privately with an independently reviewed
`yt-dlp` executable. The raw info may contain account labels, so keep it below ignored
`research/`; the Reddit Atom parser retains only the matching media ID, duration,
public/unknown access state, observation time, and info-file SHA-256.

For a small representative pilot, these three Atom-derived leads span a recent
general statement, a smell-related statement, and a debt-related statement. Their
post titles are selection hints, not content truth:

| `v.redd.it` ID | Atom post ID |
| --- | --- |
| `ftkqj9bfg1lh1` | `1vvver5` |
| `lffc541rdbih1` | `1vjkxoh` |
| `o1xbwhf9ebih1` | `1vjl05h` |

The exact no-auth metadata commands for that proposed cohort are:

```sh
reddit_root="$PWD/research/corpus/reddit-rss/HIMRFAM2"
metadata_root="$reddit_root/vreddit-info-pilot-v1"
yt_dlp="$PWD/research/corpus/.venv/bin/yt-dlp"
mkdir -p "$metadata_root"

for reddit_video_id in ftkqj9bfg1lh1 lffc541rdbih1 o1xbwhf9ebih1
do
  empty_home="$metadata_root/home-$reddit_video_id"
  mkdir -p "$empty_home"
  env -i HOME="$empty_home" XDG_CONFIG_HOME="$empty_home/.config" XDG_CACHE_HOME="$empty_home/.cache" LC_ALL=C LANG=C TZ=UTC \
    "$yt_dlp" \
      --ignore-config \
      --no-playlist \
      --skip-download \
      --dump-single-json \
      --no-write-comments \
      --no-write-subs \
      --no-write-auto-subs \
      --no-write-thumbnail \
      "https://v.redd.it/$reddit_video_id" \
      > "$metadata_root/$reddit_video_id.info.json"
done

metadata_observed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
acquisition/bin/reddit-rss parse \
  --snapshot "$reddit_root/rrs_4cf0b31e099c8e7465b4f42f6036741d.snapshot.json" \
  --v-reddit-info "$metadata_root/ftkqj9bfg1lh1.info.json" \
  --v-reddit-info "$metadata_root/lffc541rdbih1.info.json" \
  --v-reddit-info "$metadata_root/o1xbwhf9ebih1.info.json" \
  --metadata-observed-at "$metadata_observed_at" \
  > "$metadata_root/enrichment-result.json"
```

A 403, 429, unavailable result, or non-public access observation is a normal external
failure. Do not add cookies, browser extraction, credentials, a Reddit discussion URL,
or an alternate generic downloader. Retry the same public metadata request later.

## Select and plan

Read `discovery_id` from `enrichment-result.json`; its sibling
`<reddit_root>/<discovery_id>.discovery.json` is the enriched manifest. Then seal the
selection and plan with explicit timestamps and caps:

```sh
selection="$metadata_root/pilot-selection-v1.json"
plan="$metadata_root/pilot-plan-v1.json"
enriched_discovery="$reddit_root/rdd_REPLACE_WITH_ENRICHED_ID.discovery.json"

acquisition/bin/reddit-video-acquisition create-selection \
  --purpose "three-clip public Reddit acquisition pilot" \
  --snapshot-id rrs_4cf0b31e099c8e7465b4f42f6036741d \
  --reddit-video-id ftkqj9bfg1lh1 \
  --reddit-video-id lffc541rdbih1 \
  --reddit-video-id o1xbwhf9ebih1 \
  > "$selection"

planned_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
acquisition/bin/reddit-video-acquisition plan \
  --discovery "$enriched_discovery" \
  --selection "$selection" \
  --planned-at "$planned_at" \
  --max-items 3 \
  --max-duration-ms 600000 \
  --max-job-bytes 536870912 \
  --plan-budget-bytes 1610612736 \
  --estimated-bytes-per-second 500000 \
  --fixed-overhead-bytes 67108864 \
  > "$plan"

acquisition/bin/reddit-video-acquisition validate-plan \
  --plan "$plan" \
  --discovery "$enriched_discovery" \
  --selection "$selection"
```

The conservative estimate is `64 MiB + duration × 500,000 bytes/second`. Only a
positive observed duration within both the duration and byte caps can be `ready`.
When yt-dlp reports `availability=public`, the plan records
`yt_dlp_reported_public`. Reddit's extractor commonly omits that field; in that case
the plan records `public_atom_locator_no_auth_runtime_gate`: the locator came from the
sealed public Atom response, but the actual work order still has to succeed through
the credential-free adapter before any bytes can be admitted. Missing metadata,
duration overflow, byte overflow, item limit, and plan-budget deferrals remain
explicit in the plan.

## Materialize, review, then acquire

Calculate the executable hash independently and compare it with the approved tool
inventory before materialization. This example uses the current private cache and
keeps the existing 30 GiB cache cap and 100 GiB free-space floor:

```sh
yt_dlp_sha256=$(sha256sum "$yt_dlp" | awk '{print $1}')
yt_dlp_version=2026.08.19
yt_dlp_module_root="$PWD/research/corpus/.venv/lib/python3.14/site-packages/yt_dlp"
bundle_root="$PWD/research/corpus/reddit-video-acquisition-bundles"
media_root="$PWD/research/corpus/media-cache"
bundle_result="$metadata_root/bundle-result.json"

acquisition/bin/reddit-video-acquisition materialize \
  --plan "$plan" \
  --discovery "$enriched_discovery" \
  --selection "$selection" \
  --bundle-root "$bundle_root" \
  --media-output-root "$media_root" \
  --yt-dlp-executable "$yt_dlp" \
  --yt-dlp-sha256 "$yt_dlp_sha256" \
  --yt-dlp-version "$yt_dlp_version" \
  --yt-dlp-module-root "$yt_dlp_module_root" \
  --global-cache-cap-bytes 32212254720 \
  --free-space-floor-bytes 107374182400 \
  > "$bundle_result"
```

The bundle retains snapshot, discovery, post, permalink, locator-basis, and metadata
hash provenance for each order while marking the Atom title unreviewed and publication
authority false. Materialization can produce zero orders and still succeeds as an
auditable fail-closed result.

After human review, validate and dry-run each exact order before any live acquisition:

```sh
bundle_id=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["bundle_id"])' "$bundle_result")
for work_order in "$bundle_root/bundles/$bundle_id"/work-orders/*.json
do
  acquisition/bin/acquire validate --work-order "$work_order"
  acquisition/bin/acquire run --work-order "$work_order" --dry-run
done
```

Remove `--dry-run` only when the reviewed plan, executable pin, storage reservation,
and public provider availability are all acceptable. Acquisition still grants no
catalog import, relevance finding, content claim, identity inference, rights finding,
privacy clearance, sensitivity clearance, or publication decision.
