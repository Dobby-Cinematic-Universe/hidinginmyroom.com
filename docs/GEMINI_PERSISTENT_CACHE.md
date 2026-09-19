# Persistent Gemini validation cache (staged, not running)

The running `himr-cloud-gemini-20260913-hundred-v1.service` was not edited,
stopped, or restarted. No handover was queued. It can finish its existing
memory-cache rebuild without interruption.

The successor runtime is
`research/private-transcriptions/cloud-archive-20260913/summary-persistent-v1/runtime`.
Its release is `summary-persistent-v1/execution-release/release.json`, SHA-256
`ff048633aa9f35a70651543d40d6d5a1e4e01e60556a4ced39ef2256d8bb4e99`, relative to the campaign root.
It loads the unchanged summary manifest, cloud plan, budgets and paid receipts.

## What is persisted

Validated compact record snapshots and completed-export references are saved in
`summaries-v2/validation-cache-v1/cache.sqlite3`. Both parent and collection
processes use the cache in the new runtime. Successful disk hits are promoted
into the existing bounded memory cache.

Entries are bound to the worker manifest, configuration, full implementation
fingerprint, single-label normalization scope, exact source/request references,
and record metadata witnesses. Witnesses include inode, size, nanosecond mtime
and ctime, ownership, permissions and link count throughout the record tree.
Source/request bytes are rehashed before disk reuse; exports still require their
exact artifact hash. Changed code, configuration, sources, receipts, directory
contents or metadata cause a miss and normal validation.

The cache is disposable, private derived state, not a receipt or spending ledger.
Atomic SQLite transactions prevent partial publication. Payload checksums detect
corruption; missing, malformed, locked or unsafe caches fall back to validation.
It trusts the owning user's private workspace, not an adversarial owner who can
rewrite both the database and its checksums. It never uses pickle or executes
cache contents. Corrupt databases are not automatically deleted or repaired.

The cache keeps at most 8,192 snapshots and 64 MiB of payload, with a 1 MiB
per-entry limit and a 128 MiB database ceiling at the default 4 KiB page size.
Older cache rows are evicted transactionally; no original artifacts are deleted.
Unsafe links, foreign files, non-private permissions and oversized cache files
are rejected. Global reservations and spending totals are still checked every
cycle, and pending jobs still pass through normal polling and collection.

## Activation and first-run limitation

This is ready for a future planned restart, **not enabled in the current worker**.
Use the new runtime and exact release above with the existing manifest and
100-slot worker arguments documented in [the scheduler release](GEMINI_HUNDRED_SCHEDULING.md).
Never launch it concurrently against the running worker's state root.

The first run of this successor must validate records once to populate the disk
cache. The current process's memory-only snapshots cannot be recovered by this
separate implementation. Subsequent restarts of the same implementation reuse
unchanged validated records; new/changed records still need work. Initial job
objects remain memory-only and use the existing retained-request reconstruction
when needed. Source selection and identity checks still run, so startup is not
instant and no measured archive-wide startup ETA is claimed.

185 targeted and regression tests passed, including completed-record and export
reuse without paid replay, a real fresh-process cache read, source/receipt
invalidation, reservation-loss rejection, corruption, symlinks, rollback and
storage bounds. The release preserves anonymous-speaker policy, timestamp
stripping, model requests, adaptive admission and the original spending caps.
