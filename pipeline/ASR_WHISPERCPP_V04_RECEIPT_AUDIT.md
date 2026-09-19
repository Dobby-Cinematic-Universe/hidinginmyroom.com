# Historical v0.4 ASR receipt compatibility audit

`asr-whispercpp-v04-receipt-audit` is a read-only compatibility lane for one
narrow historical contract: a schema-v2, queue-only result-store seal receipt
created by the exact retained v0.4 sealer from an exact v0.2 preprocess-ASR
queue. It does not repair or replace a historical plan or receipt.

The old validator compares producer-time Linux device numbers with the current
mount. A clean restart changed the observed main-drive `st_dev` from `53` to
`38`, so an otherwise unchanged real receipt now fails the old validator. Device
and inode numbers, plus producer-time ctime and mtime values, remain useful
diagnostics but are not durable content identity across restarts, remounts,
copies, or restores.

## Command

```sh
pipeline/bin/asr-whispercpp-v04-receipt-audit \
  --receipt /absolute/path/to/asrsealreceipt_….json
```

The command has only `--receipt`. It has no plan, apply, import, write, repair,
relocation, or publication subcommand. Success prints an ephemeral JSON audit
summary to standard output. It does not create an audit receipt or confer
authority on another pipeline stage.

## Exact compatibility boundary

The auditor fails closed unless these repository files have the exact byte
counts and SHA-256 digests below:

| Role | Bytes | SHA-256 |
| --- | ---: | --- |
| retained v0.4 sealer | 126,781 | `0e2d05652e3b8bde279d22e20b97f630dd6669c42c82eed8369f089b74701d90` |
| retained v0.2 queue validator | 86,523 | `e0fffe5af2f403cd46fff82bde452f81fabb1d165f82dffcb052206d00b0fe87` |
| retained v0.2 queue schema | 31,540 | `f8b5f1debca171b5dd58f2d5e6bcc007489e389d2e2617c581bcff0eaabfdffa` |
| catalog-free result validator | 94,217 | `77607428cd10aac794bb3d913075a07f34521513d10b2fe45cb295f7dae30197` |
| restart-portable v0.5 replay helper | 126,605 | `a5c988f02d5f65246639e07ed36c0141fd880814703000f145f275074fbcdaa2` |

It then requires the plan itself to pin that v0.4 sealer, result validator,
v0.2 queue validator, and v0.2 schema. A v0.1 or v0.3 queue, a two-source v1
seal, a batch-only v3 seal, an unknown implementation, or a different byte
identity is outside this lane.

The two historical v0.1 and v0.2 queue schemas have the same old `$id`. This
auditor selects v0.2 by the sealed materializer version and exact path, byte
count, and SHA-256—not by `$id`.

## What remains authoritative

The replay validates the canonical plan and receipt bytes and identities, the
exact plan reference, absolute closed paths, the queue manifest and every work
order, queue selection lineage, source and result identities, the exact
three-file result tree, regular file/directory types, sealed modes, file link
counts, byte counts, content hashes, result-envelope fields, and catalog-free
raw/normalized artifact validity.

For each live read, no-follow descriptors are retained. Descriptor and pathname
device/inode equality, plus before/after metadata stability, still detect
replacement or mutation during that audit operation. Only comparisons between
current values and the historical plan/receipt `device`, `inode`, `ctime_ns_*`,
and `mtime_ns` observations are omitted.

The seal apply-lock file is opened `O_RDONLY`; the auditor cannot acquire the
apply lock. Python bytecode writes are disabled. A traced real canary issued no
write-capable artifact opens or mutating filesystem calls and no network system
calls; the catalog-free validator's `ffprobe` children wrote only their pipe
output.

## Deliberate limitations

- The unversioned v0.4 sealer and its `validate-receipt` behavior remain frozen
  and reboot-fragile. Use this separate command only for the exact contract
  above.
- Absolute paths are still authoritative. This is not a relocation or archive
  restore adapter.
- `media_local_asr_bridge` independently compares the persisted seal member's
  `directory.device`. It therefore remains blocked by the same `53` to `38`
  restart drift even after this audit succeeds. This command does not bypass or
  authorize that bridge.
- The command produces no durable successor receipt. Any future consumer must
  receive its own reviewed, versioned portability contract rather than treating
  console output as authority.
- The retained v0.2 queue validator imports its historical Python dependency
  graph by repository pathname, and the catalog-free importer has its own
  pathname-loaded dependencies. The auditor exact-pins the top-level assets
  listed above but is not a complete pre-execution trust anchor for every
  transitive Python source file. Run it only from a reviewed checkout; a future
  hardened lane would need a closed, hash-pinned dependency manifest and isolated
  loader.

## Regression evidence

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest -v \
  pipeline.tests.test_asr_whispercpp_v04_receipt_audit
```

The focused suite proves that synthetic device/inode/ctime/mtime drift fails the
old v0.4 validator but passes this auditor, while changed artifact content and a
non-v0.2 queue contract still fail closed. A real post-restart schema-v2 receipt
also replays successfully without changing its plan, receipt, result artifacts,
or source queue.
