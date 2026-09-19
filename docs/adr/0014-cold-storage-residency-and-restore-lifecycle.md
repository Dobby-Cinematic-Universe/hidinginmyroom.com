# ADR 0014: Cold-storage residency and restore lifecycle

- Status: proposed
- Date: 2026-08-29

## Context

The mounted cold archive is large, but its current contract intentionally authorizes
only one exact, sealed, checksum-verified copy at a time. Exactly one transfer has
completed. Its single-use authority is consumed, its source remains on the main
drive, and the transfer grants no catalogue-import or deletion authority.

That boundary is safe for a preservation copy, but it is not yet a safe “move”
system. There is no collection manifest, cold-to-hot restore command, append-only
residency record, restore drill, or separately reviewed hot-copy eviction plan.
Blindly moving large directories would also put active runtimes, mutable databases,
or reproducible caches on a filesystem whose policy forbids processing and mutable
state.

The main filesystem currently has roughly 189 GB free, so lifecycle correctness is
more urgent than emergency reclamation. The cold disk is also one local failure
domain, not a substitute for an independent backup of unique media.

## Proposed decision

Keep the existing one-object transfer executor unchanged. Add four separate
contracts around it before authorizing another transfer:

1. **Cold plan.** A main-drive-only, content-deduplicated manifest lists exact source
   paths, SHA-256 values, byte counts, media/artifact identities, provenance,
   privacy class, dependency closure, retention class, and required hot residency.
   Planning must never scan or probe the archive mount.
2. **Batch completion.** Each object still receives its own no-replace transfer and
   receipt. An aggregate receipt is emitted only after every planned object and every
   per-object receipt exact-replays. The immutable plan is mirrored
   content-addressably to both filesystems during an authorized execution.
3. **Restore and residency.** Add an exact cold-to-hot restore command with capacity
   preflight, bounded reads, temporary no-replace publication, SHA-256 verification,
   `fsync`, and a durable restore receipt. Record preservation and operational
   locations as append-only catalogue observations; neither location alone implies
   availability, publication rights, or processing authority.
4. **Eviction.** Hot-copy deletion remains a different digest-bound human decision.
   It is eligible only after the cold location is catalogued, the object has passed a
   restore drill, dependency consumers no longer require the hot path, and another
   independent durable copy exists for irreplaceable material. The transfer tool
   never deletes its source.

A future batch authorization must bind the exact plan digest, object count, total
bytes, destination policy, and expiry. Execution resumes per object without scanning
unlisted archive content and without rolling back already verified copies. This is a
new human authority; the consumed transaction cannot be replayed or broadened.

## Candidate classes

The following main-drive inventory is a planning estimate, not transfer authority:

| Priority | Candidate class | Approximate logical size | Required preparation |
| --- | --- | ---: | --- |
| 1 | Completed July capture, matching, and preprocess evidence | 4.723 GiB | Build one aggregate manifest over the already completed trees; archive canonical raw objects once and keep compact verification metadata hot. |
| 1 | Completed objects in the acquired raw CAS | Up to 5.65 GiB | Select per completed item, make it immutable/single-link, and bind acquisition provenance. |
| 2 | Unreferenced sealed and legacy catalogue snapshots | 1.867 GiB | Add a generalized immutable-artifact lane; integrity-check and seal legacy snapshots, while referenced frozen baselines remain hot. |
| 3 | Completed local-window artifacts | 1.071 GiB | Add one aggregate manifest and hydration support; keep any actively referenced windows hot. |
| 4 | Closed preprocess/evaluation artifacts | About 3.25 GiB | Archive only when expensive or required for reproducibility; otherwise prefer verified regeneration or deletion review. |

The following stay on the main drive:

- the active approximately 7.42 GiB GPU runtime, environment, model, wheelhouse, and
  caches;
- live catalogues, WAL/SHM files, FTS/vector indexes, queue state, and operational
  receipts needed by workers;
- active planning, guest-review, torrent, download-staging, and temporary trees; and
- current normalized audio/proxies needed by the CPU/GPU queues.

Regenerable caches and redundant derived outputs should not be copied merely to make
space. In particular, the roughly 2.8 GiB GPU package cache and the 1.003 GiB
evaluation-preprocess payload tree already present byte-identically under `processed`
are cleanup candidates only after exact dependency replay and a separate deletion
decision. Duplicate private/root inputs totaling about 2.40 GB decimal are hot
eviction candidates once their canonical CAS copies and restore path are proven; they
are not additional archive objects. Including `node_modules`, verified-but-not-yet-
authorized cleanup candidates total about 5.41 GiB before the duplicate evaluation
tree.

## Consequences

- “Archive” means add a verified preservation location; “evict” remains a later,
  explainable, reversible-by-restore decision.
- Active GPU work and mutable/search data remain fast and local.
- Batch planning can reduce operator work while retaining one-object receipts and
  exact resumability.
- No further cold transfer may occur until a new human authorization names a sealed
  plan. This ADR itself grants no transfer, mount-enumeration, catalogue, or deletion
  authority.
