# Cold-storage transfer contract

This contract defines a narrow, offline boundary for copying one completed, sealed
media object from managed hot storage to a distinct cold-storage filesystem. The
repository implementation is `acquisition/bin/cold-storage-transfer`. It is
not an acquisition adapter, media processor, catalogue importer, backup scheduler,
or deletion tool. Do not substitute an unverified `cp`, `mv`, synchronization, or
recursive archive command.

Completed public acquisition payloads normally remain in producer-owned mode `0644`
or `0600` and therefore are not direct inputs to this command. Use the separately
documented [one-object public-acquisition retention adapter](PUBLIC_ACQUISITION_RETENTION.md)
to replay the exact public work order/result, create an independent mode-`0400` hot
staging object, and invoke this contract with the fixed `/mnt/archive/HIMR`
destination. The adapter does not weaken this command's sealed-source requirement.

The cold filesystem is a destination for completed immutable objects only. Downloads,
active-admission hashing, probing, decoding, preprocessing, ASR, OCR, comparisons,
indexes, mutable databases, and temporary processing remain on the main filesystem.
The only full cold-object read permitted here is one bounded SHA-256 verification of
the exact temporary or existing object involved in the requested transfer.

## Required interface

The implementation must expose one explicit-object command with this shape:

```sh
acquisition/bin/cold-storage-transfer run \
  --source-root /srv/himr-hot-cache \
  --source-relative-path \
    media/sha256/ab/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789/payload \
  --destination-root /srv/himr-cold-archive \
  --receipt-root /srv/himr-private-control \
  --expected-sha256 \
    abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789 \
  --expected-byte-count 123456789 \
  --free-space-floor-bytes 107374182400
```

`--source-root`, `--destination-root`, and `--receipt-root` are mandatory absolute,
lexically normalized paths to existing directories. The source is a canonical
relative POSIX path strictly below its root. Empty components, `.` or `..`, backslash,
NUL, symlinks, special nodes, and traversal across a nested mount are rejected.
Resolving a symlink and silently substituting its target is not acceptable.
The receipt root and sealed source root must be disjoint directory trees so receipt
creation cannot change metadata on the root or components retained by the source
seal. Lexically distinct bind aliases of the same directory are also rejected.

The expected digest and byte count are mandatory. They bind the reviewed hot-store
object before any archive write and determine the only allowed destination:

```text
<destination-root>/media/sha256/<first-two-sha256-characters>/<sha256>/payload
```

The receipt path is derived from a transfer ID computed from canonical JSON of the
complete normalized request, excluding runtime timestamps:

```text
<receipt-root>/cold-storage-transfers/<transfer-id>/receipt.json
```

The implementation may also provide receipt validation, but neither validation nor
replay may scan the archive or discover objects implicitly. Every operation remains
bound to one explicit content digest and one exact path.

## Source seal and retained paths

The source must be a nonempty, current-user-owned regular file at mode `0400` with
exactly one hard link. The source root, every directory component, and the leaf are
opened and retained through directory-relative descriptors with `O_NOFOLLOW`; opened
descriptor identities must match no-follow path observations. The retained identity
includes device, inode, file type and mode, link count, byte count, mtime, and ctime.
Linux mount IDs are checked in addition to device IDs so same-device bind mounts do
not bypass the no-nested-mount boundary.

Hash the retained source descriptor on the main filesystem and require the observed
SHA-256 and byte count to equal the request. The same descriptor supplies the copy.
Recheck the complete retained source path, metadata, and digest before committing the
receipt. The tool never changes source permissions, renames the source, or deletes it.

Destination and receipt directory chains receive equivalent no-follow, retained
descriptor treatment. Newly managed directories are current-user-owned mode `0700`.
Unnamed staging inodes have link count zero; final payload and receipt files are
single-link regular files at mode `0400`. Receipt directories are owner-private.

## Filesystem and capacity preflight

Preflight occurs before creating archive staging:

1. Retain all three roots and record their device identities.
2. Require the source and receipt roots to be on the same filesystem.
3. Require the destination root to have a different device identity. A bind path or
   second directory on the hot filesystem is not cold storage.
4. Require the source leaf and every managed component to remain on its root's device;
   nested mount escapes fail closed.
5. Read available capacity from the retained destination filesystem descriptor. When
   the object is absent, `available - expected_byte_count` must remain at or above the
   explicit free-space floor.

Do not calculate capacity by walking the archive tree. Such a walk is expensive,
racy, crosses the intended one-object boundary, and is unnecessary when the exact
incoming byte count and filesystem free space are known. Actual transfers take a
nonblocking advisory lock on the retained destination-root directory descriptor
before replay inspection, capacity preflight, staging, and publication. Dry runs are
lock-free. Atomic no-replace publication remains the final collision boundary.

## Transfer and durable publication

For an absent target, the implementation follows this sequence:

1. Create an unnamed `O_TMPFILE` staging inode in the retained final archive
   directory. It has no pathname that another process can swap, and a crash before
   publication discards it automatically.
2. Stream the retained source descriptor into that temporary descriptor while
   calculating a copy-stream SHA-256 and enforcing the exact byte count.
3. Flush and `fsync` the temporary file.
4. Read the exact temporary descriptor once from the archive, bounded by the expected
   byte count, and independently calculate SHA-256. Both digests and sizes must match
   the request.
5. Set mode `0400`, `fsync` again, then publish the retained temporary descriptor
   with Linux `linkat(AT_EMPTY_PATH)` inside the retained final parent. This
   no-replace link is bound to the verified inode and creates its first and only
   pathname. Never replace an existing content-addressed object.
6. `fsync` the final parent and every newly created archive directory before treating
   the object as durable.
7. Retain the verified final payload descriptor and recheck its exact path, inode,
   link count, mode, and mount identity without another content read.
8. Reverify the retained source and atomically commit the receipt on the main
   filesystem using an unnamed `O_TMPFILE`, file `fsync`, the same fd-bound
   no-replace publication, and parent-directory `fsync`.

An object found before staging is retained and validated as an exact single-link
mode-`0400` regular file with one bounded full checksum read. Matching bytes are
idempotent recovery; a symlink, special node, wrong mode, wrong size, wrong digest, or
changed path fails closed. If an object unexpectedly appears at the final no-replace
publication boundary, the current invocation fails without a second archive read;
the next invocation can validate that now-pre-existing object once. A target left by
a crash before receipt commit may be verified and receive the missing receipt. A
receipt without its exact target, or a pre-existing divergent receipt, is an error.

For `destination.admission: copied`, the receipt records the copy-stream digest and
an affirmative temporary-file `fsync`. For `existing_verified` or
`recovered_existing`, no temporary file or copy stream occurred, so those two receipt
fields are respectively `null` and `false`; the independent bounded target read,
final-file `fsync`, and containing-directory `fsync` still occur.

On failure, closing an unpublished `O_TMPFILE` descriptor discards only the unnamed
inode created by the current invocation. The tool never removes a source, an
existing final object, an unknown archive entry, or a receipt it did not create.

## Dry run and replay

`--dry-run` is read-only and lock-free. It validates and hashes the sealed source on
the main filesystem, verifies root/device/capacity policy, derives the transfer ID,
target, and receipt path, and prints a planned result. It creates no root, lock,
directory, temporary file, payload, or receipt and does not read existing archive
payload content.

The same normalized request always derives the same transfer ID. An actual replay
validates the immutable receipt and exact archive target, performs at most one bounded
full target checksum read, and returns the existing receipt without rewriting it.
Replays perform no media processing and do not refresh timestamps in durable state.

## Receipt and catalogue boundary

The durable receipt conforms to
[`cold-storage-transfer-receipt.schema.json`](schemas/cold-storage-transfer-receipt.schema.json).
Runtime validation remains authoritative for canonical request identity, path
containment, no-follow descriptor checks, device relationships, arithmetic, hashes,
cross-field equality, atomic publication, and replay.

`identity_sha256` is the SHA-256 of canonical JSON for every receipt field except
`identity_sha256` and `receipt_id`; `receipt_id` is
`coldreceipt_<first-32-identity-hex-characters>`. `transfer_id` is independently
derived from the normalized request. Receipt timestamps describe the first durable
admission and are unchanged on replay.

The `catalog_location_candidate` is only a typed handoff. It mirrors the existing
`media_locations` shape with `storage_class: local_cold_archive`, `is_primary: 0`, and
the receipt verification time. The transfer does not open SQLite, alter a primary
location, grant catalogue-import authority, or authorize publication. A future
catalogue-side importer must independently validate the receipt and archive object.

## Test boundary

Tests must use only small files in disposable directories on the main or temporary
filesystem. They must never inspect, create, hash, or delete content in a real archive
mount. Distinct-device and capacity observations should be injected or mocked while
the copy and atomic-publication behavior is exercised with disposable files.

Required coverage includes sealed-mode, owner, hard-link, symlink-component, special
file, path-escape, nested-mount, same-device, and insufficient-space refusals;
source mutation and checksum mismatch; unnamed staging; crash boundaries;
no-replace races; target-without-receipt recovery; receipt-without-target refusal;
exact replay; directory `fsync`; dry-run non-mutation; and an assertion that source
deletion is never attempted.
