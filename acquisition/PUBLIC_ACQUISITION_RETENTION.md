# One-object public-acquisition retention

`acquisition/bin/retain-public-acquisition` is the narrow operator boundary for
retaining one exact, completed public acquisition at `/mnt/archive/HIMR`. It adapts
the mutable acquisition CAS to the stricter sealed-source contract in
[COLD_STORAGE_TRANSFER.md](COLD_STORAGE_TRANSFER.md). It is not a recursive backup,
archive discovery, deletion, eviction, restore, catalogue, or publication command.

The executable is an isolated `/usr/bin/python3 -IB` launcher, not a shell
trampoline. Before importing application code it retains, metadata-checks, and hashes
the exact `acquire.py`, `cold_storage_transfer.py`, and
`retain_public_acquisition.py` files against embedded pins. It compiles and executes
those retained bytes directly, so a pathname replacement between verification and
import cannot substitute different code. Any source edit requires a separately
reviewed launcher-pin update; a mismatch exits before work-order or archive access.

The destination root is a code constant. There is no `--destination-root` argument,
and the operator console rejects an extra destination parameter. Every other path
must be one explicit normalized absolute path; wildcards, directory enumeration,
symlinks, path aliases, and cold-source paths are rejected.

## Admission contract

One invocation binds all of the following before writing staging data:

- the exact acquisition work-order path and physical SHA-256;
- the exact deterministic `result.json` path and physical SHA-256;
- a current typed replay of that completed result under the work order;
- a `direct_http` or `yt_dlp` work order whose source access state is `public`;
- the result's exact acquisition-CAS payload path, SHA-256, and byte count; and
- disjoint acquisition, staging, and receipt roots on the main filesystem.

Local-file, private, unknown, credential-bearing, absent, incomplete, legacy-invalid,
or digest-mismatched results fail closed. Control and payload files must be
current-user-owned, single-link regular files with no group/other write permission.
The acquisition payload may remain in its producer mode (`0600`, `0640`, or `0644`);
the adapter never changes that mode.

The staging and receipt roots must already exist as current-user-owned mode-`0700`
directories. The staging root is locked non-blockingly. It receives only this path:

```text
<staging-root>/media/sha256/<digest-prefix>/<digest>/payload
```

The adapter streams the retained acquisition descriptor into an unnamed
`O_TMPFILE`, hashes the copy, reads it back, changes only the unnamed staging inode to
mode `0400`, and publishes it with descriptor-bound `linkat(AT_EMPTY_PATH)` and
no-replace semantics. It then delegates the sealed staging object to the existing
cold-transfer implementation, whose only target is:

```text
/mnt/archive/HIMR/media/sha256/<digest-prefix>/<digest>/payload
```

The acquisition payload is never deleted, renamed, linked, or chmodded. The sealed
hot staging object is intentionally retained after success; no automatic cleanup or
eviction authority is present. Repeating the same request verifies and reuses both
content-addressed objects without rewriting either one.

For a new staging object, the raw acquisition payload has exactly three bounded full
read passes: typed completed-result admission, the staging copy/digest, and one
post-copy source verification. Later source checks are descriptor/path/metadata
identity checks. If the exact sealed staging object already exists, the raw source
has two full passes: admission and one source verification; the staging object is
independently read back. Cold transfer reads the sealed staging object, not the
mutable acquisition object.

## Direct command

Review the three digests and byte count independently, then run:

```sh
acquisition/bin/retain-public-acquisition run \
  --work-order /absolute/main-drive/path/work-order.json \
  --acquisition-root /absolute/main-drive/path/acquired \
  --staging-root /absolute/main-drive/path/cold-retention-staging \
  --receipt-root /absolute/main-drive/path/cold-retention-receipts \
  --expected-work-order-sha256 WORK_ORDER_PHYSICAL_SHA256 \
  --expected-result-sha256 RESULT_PHYSICAL_SHA256 \
  --expected-sha256 PAYLOAD_SHA256 \
  --expected-byte-count PAYLOAD_BYTES \
  --free-space-floor-bytes 107374182400
```

The command emits one JSON result. The nested cold-transfer receipt remains the
durable archive authority. The result explicitly records that source deletion,
source mutation, catalogue mutation, archive scanning, and publication authority are
all absent.

## Operator-console profile

The registered action is `retention.public_acquisition`, uses the dedicated
`cold_storage` resource lock, also claims the network/acquisition lane, and requires
the exact phrase `RETAIN ONE PUBLIC ACQUISITION`. It therefore cannot overlap a
producer or rolling acquisition job, but it may overlap preprocessing or GPU work. A
profile supplies only repository-private hot paths,
the three digests, byte count, and free-space floor. The browser never supplies any
path or argument, and the profile cannot name a different cold destination. Console
prepare/execute revalidation binds the launcher bytes, and the launcher in turn binds
the full three-file implementation closure.

Do not edit the active mode-`0400` profile until all values have been reviewed. The
tracked [`operator_console/profiles.example.json`](../operator_console/profiles.example.json)
contains a non-runnable placeholder shape.

## Failure and recovery

An unpublished unnamed staging inode disappears when its descriptor closes. A crash
after staging publication leaves a valid sealed content-addressed object; rerunning
verifies and reuses it. The cold-transfer layer has the same no-replace and
target-without-receipt recovery behavior described in its own contract. No failure
path removes the hot acquisition payload, sealed staging payload, cold payload, or a
receipt.

Tests use disposable repository-local files and a fake transfer runner. They do not
open, inspect, hash, create, or delete anything beneath `/mnt/archive/HIMR`.
