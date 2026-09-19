# Public Archive.org item-metadata snapshots

This lane records the exact JSON returned by the public
`https://archive.org/metadata/<identifier>` endpoint. It makes one unauthenticated
`GET` request per explicitly listed identifier and does not search or crawl
Archive.org.

The lane has no media-download path. It never requests an Archive.org `/download/`
URL, sends cookies or authorization, accepts credentials, or uses a browser profile.
Redirects must remain on `archive.org` or `www.archive.org` at the exact metadata
endpoint for the requested identifier.

Archive.org is the provider of these fields, not a reviewer. Titles, creators,
dates, collections, descriptions, and file records remain unreviewed provider
metadata. A snapshot or delta is not proof of content, authorship, identity,
completeness, relevance, availability, or rights, and grants no publication or
identity authority.

## Private storage boundary

Every request manifest, exact provider response, snapshot, delta, command receipt,
and catalog produced from this lane must live only below the repository's ignored
`research/` path. For example:

```text
research/corpus/archive-org-metadata/
  requests/
  snapshots/
  deltas/
```

`research/` is excluded by [the repository ignore policy](../.gitignore). Do not
commit these artifacts or copy them into `src/data/corpus`, `public/`, tracked
examples, or a public release manifest. The exact provider payload can contain fields
that have not passed relevance, privacy, sensitivity, rights, or identity review.

## Create and validate a request

A request is a strict version 1 JSON object. It contains 1 to 100 unique Archive.org
identifiers sorted bytewise, a whole-second UTC request time, an allowlisted basis for
each identifier, and the exact no-auth policy. The accepted bases are
`catalog_archive_item`, `catalog_file_hint`, and `manual_public_lead`.

This example creates canonical request bytes and computes the request ID from the
canonical semantic content:

```sh
mkdir -p "$PWD/research/corpus/archive-org-metadata/requests"

python3 - <<'PY' \
  > "$PWD/research/corpus/archive-org-metadata/requests/request-20260826T210000Z.json"
import sys

from acquisition.archive_org_metadata import pretty_bytes, stable_id

request = {
    "schema_version": 1,
    "request_kind": "archive_org_metadata_targets",
    "requested_at": "2026-08-26T21:00:00Z",
    "items": [
        {
            "identifier": "example_public_identifier",
            "basis": "manual_public_lead",
        }
    ],
    "policy": {
        "public_unauthenticated_metadata_only": True,
        "media_download": False,
        "cookies_sent": False,
        "authorization_sent": False,
        "publication_authority": False,
    },
}
request["request_id"] = stable_id("iamr", request)
sys.stdout.buffer.write(pretty_bytes(request))
PY

python3 acquisition/archive_org_metadata.py validate-request \
  --request "$PWD/research/corpus/archive-org-metadata/requests/request-20260826T210000Z.json"
```

Validation rejects unknown or missing keys, unsafe identifiers, unsorted or duplicate
targets, unsupported bases, malformed timestamps, a weakened policy, and a request ID
that does not reproduce. The ID is the `iamr_` prefix plus the first 32 hexadecimal
characters of SHA-256 over canonical JSON for every field except `request_id`.
Whitespace and object-key order therefore do not change request identity. The exact
request file bytes are nevertheless retained and hashed as snapshot evidence.

## Capture and validate a snapshot

Run capture from the repository root with an absolute output root under `research/`:

```sh
python3 acquisition/archive_org_metadata.py capture \
  --request "$PWD/research/corpus/archive-org-metadata/requests/request-20260826T210000Z.json" \
  --output-root "$PWD/research/corpus/archive-org-metadata/snapshots" \
  --timeout-seconds 60 \
  --max-item-bytes 134217728
```

The command returns the snapshot path and ID as JSON. A completed tree is named for
its deterministic `iams_...` snapshot ID and contains the exact request, one exact
metadata response per identifier, and `snapshot.json`:

```text
snapshots/iams_.../
  request.json
  item-<identifier>.metadata.json
  snapshot.json
```

Capture requires every request to start at or after the sealed request timestamp,
HTTP 200, a JSON content type, a bounded UTF-8 JSON object, a
matching `metadata.identifier`, a `files` array within the configured cap, and unique
valid file names. It records the exact request and final URLs, response times and
selected response headers, byte counts, SHA-256 digests, and reproducible item
summaries. Any failed item removes the staging tree; no partial snapshot is admitted.
Completed files and their directory are sealed read-only before the staging tree is
atomically renamed.

Validate the complete sealed tree without network access:

```sh
python3 acquisition/archive_org_metadata.py validate-snapshot \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
```

Snapshot validation rejects noncanonical JSON, unknown fields, policy drift, invalid
URLs or timestamps, a request/hash/count mismatch, missing evidence, a writable
snapshot directory/manifest/payload, payload byte or digest changes, summaries that
do not reproduce from the payloads, and a snapshot ID or directory name that does not
reproduce. Snapshot identity binds the request ID, observation time, ordered
identifiers, exact payload hashes, and final metadata URLs.

## Build and validate a delta

Capture a new request at a new explicit request time, then compare two independently
validated snapshots:

```sh
python3 acquisition/archive_org_metadata.py delta \
  --previous "$PWD/research/corpus/archive-org-metadata/snapshots/iams_previous/snapshot.json" \
  --current "$PWD/research/corpus/archive-org-metadata/snapshots/iams_current/snapshot.json" \
  --output "$PWD/research/corpus/archive-org-metadata/deltas/previous--current.json"

python3 acquisition/archive_org_metadata.py validate-delta \
  --delta "$PWD/research/corpus/archive-org-metadata/deltas/previous--current.json" \
  --previous "$PWD/research/corpus/archive-org-metadata/snapshots/iams_previous/snapshot.json" \
  --current "$PWD/research/corpus/archive-org-metadata/snapshots/iams_current/snapshot.json"
```

Delta construction first fully revalidates both sealed snapshot trees. Items are
compared by exact response SHA-256 and sorted as the union of both target sets. File
additions and removals are keyed by provider file name; a changed file means the
canonical provider file record changed, not that media bytes were downloaded or
compared. The delta binds both snapshot IDs and snapshot-manifest hashes, recomputes
all item/file totals, derives a deterministic `iamd_...` ID, refuses to overwrite
different output, and seals the result read-only.

`validate-delta` checks the sealed delta's canonical bytes, exact fields, ordering,
digests, summary counts, assertion policy, and deterministic ID, then fully
revalidates both supplied snapshot trees and reproduces the complete delta from
them. Retain both source snapshot trees: the delta is a comparison receipt, not a
replacement for its evidence. Its `content_change_asserted` and
`publication_authority` fields remain false even when provider records differ.

## Strict private catalog handoff

Validate the snapshot again at the catalog boundary, then import it transactionally:

```sh
himr-corpus validate-archive-metadata-snapshot \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"

himr-corpus import-archive-metadata-snapshot \
  --db "$PWD/research/corpus/corpus.sqlite3" \
  --snapshot "$PWD/research/corpus/archive-org-metadata/snapshots/iams_.../snapshot.json"
```

The importer is an admission boundary, not a review or publication step. It rechecks
the sealed request, snapshot, and payload bytes and admits only private, unreviewed
provider observations. It does not import media, create an identity assertion, make
a publication decision, or clear any publication gate. Deltas remain private
research comparison receipts; the catalog commands accept the complete snapshot.

After strict admission, terminal bracketed YouTube IDs can be routed into a
separate private review lane. It rehashes and reparses this same sealed snapshot,
does not trust a normalized filename from the base importer, and never performs an
automatic merge. See
[`corpus/docs/ARCHIVE_BRACKET_RECONCILIATION.md`](../corpus/docs/ARCHIVE_BRACKET_RECONCILIATION.md).
