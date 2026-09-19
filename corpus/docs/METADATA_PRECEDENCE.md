# Metadata observation and precedence policy

Catalog imports separate exact input identity from observation time:

- `import_batches` identifies an importer plus the SHA-256 of its exact input bytes.
- `import_observations` records every distinct time those bytes were observed. An
  exact replay at the same time is idempotent; the same bytes seen later are a new
  observation. Importer version is also part of observation identity, so revised
  parsing logic can retain a new assertion over the same captured bytes and time.
- `source_metadata_observations` and `recording_metadata_observations` retain every
  candidate assertion. `sources` and `recordings` are deterministic current
  projections, not a last-writer-wins audit log.
- Convergent source-relation and external-ID assertions use the same pattern, so
  importer order cannot silently choose which provenance label survives.

## Precedence

The importer policy assigns evidence classes the following integer ranks. These
integers are auditable ordering rules, **not confidence scores or probabilities**.

| Rank | Evidence class |
| ---: | --- |
| 700 | Locally byte- and contract-validated acquisition-result source metadata |
| 600 | Validated yt-dlp platform info and captured first-party channel inventory |
| 550 | Captured Internet Archive item/file metadata |
| 300 | Preserved normalized legacy manifest |
| 250 | Locally parsed discovery torrent manifest |
| 200 | Unclassified metadata importer (conservative default) |
| 100 | Search candidates and URL discovery hints |

For each field, a non-empty assertion from the highest evidence class wins. A newer
observation wins within the same class. Contradictory assertions with the same class
and instant use canonical SHA-256 and stable IDs as the final ordering key. Source
access adds a restrictive tie-break at that final step, so `private`, `members_only`,
or otherwise unavailable access cannot lose to `public` merely because calls arrived
in a different order.

This is field-aware: a newer incomplete capture does not erase an older known URL,
date, or duration from the same or stronger class. Conversely, recording duration is
no longer selected with `MAX()`; a newer same-class correction may legitimately be
shorter. The overall winning observation supplies access/review/metadata state, while
`observed_at` and `updated_at` record the latest observation of any quality.

Manually reviewed, disputed, rejected, or merged states are not downgraded by a
metadata reimport. Metadata precedence does not create publication permission. Public
views still require a current publish decision, public access, and separate current
rights, privacy, and sensitivity clearances.

## Viewer-local display times

A timestamp copied from a platform interface is not assumed to be UTC. Preserve the
displayed wall time verbatim together with the contributor-supplied locale or time-zone
description, and keep it distinct from the catalog's UTC observation and processing
timestamps.

When a contributor supplies a wall time and labels it `EST`, preserve that wall time
and zone label verbatim. Do not silently reinterpret `EST` as UTC,
`America/New_York`, or daylight-adjusted `EDT`, and do not create a UTC instant unless
the evidence also establishes the applicable offset. Catalog observation and
processing timestamps remain separate canonical UTC fields.

Notification timestamps describe when the viewer-facing notification was displayed.
Without an independently measured delivery lag they do not establish a broadcast,
upload, or event start time.

## Rebuild and test invariant

Rebuilding the same source assertions in any order must produce byte-equivalent
source/recording, source-relation, external-ID, snapshot, mapping, review-task, and
observation sets. The adversarial test suite runs all 24 permutations of search,
first-party inventory, older direct platform metadata, and a newer restricted
correction. It also tests exact replay, later observation of identical bytes,
corrected shorter duration, a same-time public/private conflict, and append-only
enforcement.
