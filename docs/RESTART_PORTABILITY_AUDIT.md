# Restart and archive portability audit

- Audit date: 2026-08-29
- Scope: active acquisition, preprocessing, ASR/GPU, sealing, and private-corpus
  replay boundaries on the main drive
- Method: read-only contract inspection and bounded replay canaries after restart

No archive payload was opened, no cold transfer was executed, no GPU inference ran,
no database was mutated, and no historical evidence or pinned implementation was
changed during this audit.

## Finding

The current evidence is not shown to be corrupt. Several validators instead produce
false negatives because they treat a producer-time Linux `st_dev` value as durable
identity. The historical main-drive value is `53`; the same files are currently on
device `38` after restart. In the examined cases, the path still resolves, the inode,
size, and recorded times still match, and the affected content remains hash-bound.
Linux device numbers are assigned by the running kernel and are not durable evidence
identifiers. An inode is meaningful only inside one filesystem instance and is not
preserved by an archive copy or restore. Modification and change times may be useful
historical observations, but copies, restores, metadata operations, and filesystem
tools can change them without changing content.

Device-and-inode comparison remains a strong race control **inside one operation**:
open a no-follow descriptor, compare the descriptor with the live path, retain the
descriptor through hashing or execution, and compare both again before return. The
defect is carrying those numeric values across operations, restarts, filesystems, or
restores and then treating equality as content authority.

## Contract matrix

| Contract or boundary | Current evidence after restart | Classification | Priority and smallest successor remedy |
| --- | --- | --- | --- |
| Raw ASR result-store seal v0.4 | 14 applied receipts contain 54 result members. All 54 member directories record device `53` and now resolve on `38`; a real receipt replay fails with `device differs from the plan`. | Unsafe persisted device authority; evidence itself is still present. | **P0.** Preserve v0.4 bytes. The narrow exact-hash-allowlisted compatibility auditor now validates a stable projection and prints an ephemeral summary without creating authority. A future successor may emit a separately reviewed append-only audit receipt. Future seal receipts keep device/inode/times as historical diagnostics only. |
| Contextual-ASR result seal | One applied receipt contains 17 members; all 17 directory records have the same `53` to `38` mismatch, and real receipt replay fails. | Unsafe persisted device authority. | **P0.** Use the same compatibility-audit and stable-projection pattern as raw ASR without rewriting the historical plan or receipt. |
| Media-local ASR bridge v1 | Its pinned seal has 25 members, including 17 bridge-eligible members. All 17 eligible directory records mismatch only on device; a real eligible member fails the bridge's exact filesystem-record comparison. | Unsafe downstream dependence on historical seal metadata. | **P0.** A bridge successor must bind the original receipt hash and member identity, then check current content, closed-tree policy, and lineage without requiring historical device/inode/time equality. |
| GPU runtime receipt and v1-v4 work-order core | A real runtime-receipt replay fails because the receipt is no longer on its recorded main-drive device. A real v3 work-order validation fails because `runtime.root` is not on persisted `runtime.expected_device`. V4 inherits this v1 field. | Unsafe persisted placement authority; distinct from the v4 pre-execution trust-anchor blocker. | **P0 before more GPU admission.** Resolve the current hot-tier root once per invocation, enforce same-filesystem relations live, and bind the tier through a stable filesystem UUID/root-registry identity rather than a kernel device number in every receipt and work order. |
| Cold-storage transfer v1 replay | The one completed receipt's local source, source root, and receipt root record `53` and are currently on `38`; its inode, size, mode, link count, and times still match. The replay code requires exact historical device and source-identity equality. The transfer was not rerun and the archive was not inspected. | Unsafe persisted device/inode authority in replay; original transfer evidence remains historical evidence. | **P1.** Add a read-only compatibility audit, separate from transfer authority. Re-evaluate source/receipt/destination filesystem relations live and validate both content-addressed endpoints; do not repeat the transfer to repair a receipt. |
| Local-window corpus admission | 20 of 20 result envelopes have a parent-source stat mismatch solely because device changed; a real importer read fails. | Unsafe persisted source-stat authority. | **P1.** Preserve producer-time stat rows as observations, but admit by retained-descriptor hash/size, current sealed-file policy, exact acquisition lineage, and current-operation race checks. |
| Visual-fingerprint corpus admission | 2 of 2 results mismatch solely on the input device; a real validator fails. | Unsafe persisted source-stat authority. | **P1.** Apply the same observation-versus-authority split and keep the input SHA-256, byte count, media ID, recipe, and artifact tree authoritative. |
| OCR corpus admission | Across 12 results, 75 of 83 persisted file-stat references mismatch solely on device; the eight system-root references still match. A real validator fails on a local model binding. | Unsafe persisted stat authority for models, frames, and result inputs. | **P1.** Revalidate exact model/frame/result bytes and lineage with retained descriptors; do not compare current device/inode/time to producer snapshots. |
| Transform-calibration receipt replay | The one historical receipt is v0.1. Its repository-local media, work-order, result, and implementation bindings record `53` and now resolve on `38`; v0.2 delegates to an exact-rebuild legacy replay. | Unsafe exact rebuild of producer-time filesystem observations. | **P1.** A successor compatibility validator should rerun the numeric calibration while comparing a stable evidence projection; retain the old receipt and implementation hash unchanged. |
| Guarded acquisition reuse, media preprocessing, preprocess-ASR queues, sparse-frame routing/import, CPU ASR import, and GPU-v3 corpus import | These contracts either compare device/inode only while a descriptor is retained, compare producer `before`/`after` observations only to each other, or treat them as typed diagnostics. Existing preprocess queue replay succeeded after restart; the GPU-v3 importer deliberately does not claim its historical input device/inode is current. | Safe with respect to this reboot failure. Absolute local paths still limit relocation. | **P2.** Keep the live TOCTOU checks. Reuse this pattern in successor validators and address root relocation separately. |
| Absolute paths and `file://` URIs across private evidence | Reboot at the same checkout path works where stat values are not authoritative, but moving the repository or restoring the CAS under another root still breaks exact-path predicates. | Relocation/archive portability gap. | **P2.** Persist a root identifier plus normalized relative CAS path. Permit only an explicit, reviewed old-root to new-root mapping for legacy evidence, and bind that mapping in the new audit receipt. |

Counts describe the evidence present during this audit, not a promise that no other
inactive or future contract contains the same pattern.

## Stable replay semantics

A successor contract should separate four concepts:

1. **Content identity:** SHA-256 plus byte count, or an equivalently reviewed digest
   contract. A directory is an ordered tree manifest of relative name, entry type,
   byte count, content digest, and required policy bits.
2. **Semantic identity and lineage:** canonical work-order, recipe, implementation,
   model, source, prior receipt, and tree-manifest digests. These remain independent
   of the current mount and checkout location.
3. **Logical location:** a fixed root ID and a normalized relative CAS path, never an
   arbitrary absolute host path as identity. A legacy relocation requires an exact
   reviewed prefix mapping; validators must still open beneath the selected root with
   no-follow semantics.
4. **Current physical policy:** file type, ownership where required, sealed mode,
   link count, free-space gate, and live same-filesystem/different-filesystem
   relationships. Where the distinction between the hot main-drive tier and cold
   archive tier matters, a root registry may bind the tier to a stable filesystem
   UUID. That UUID is placement evidence, not content identity; migration to a new
   filesystem requires an append-only reviewed root-registry transition.

Producer-time `st_dev`, `st_ino`, `mtime_ns`, and `ctime_ns` may remain in a receipt
as observations. Post-restart or post-restore replay must not require them to equal
current values. During every live read, however, descriptor/path device-and-inode
equality and before/after metadata equality remain mandatory race checks.

## Bounded implementation order

1. Freeze existing implementations and schemas exactly; never repair a pinned
   receipt by editing its producer or changing its bytes.
2. Add one reusable retained-file/tree verifier that returns stable content evidence
   separately from current-operation race evidence and historical diagnostics.
3. Keep the current raw-ASR compatibility command read-only and non-authoritative.
   Add separately reviewed compatibility successors for raw/contextual ASR seals and
   the completed cold-transfer receipt only where a downstream consumer needs durable
   authority. Each must allowlist exact legacy source/schema hashes and may then emit
   a new append-only, text-free audit receipt.
4. Version the media-local ASR, local-window, visual-fingerprint, OCR, and calibration
   validators so old observations remain parseable but no longer assert current
   device/inode/time identity.
5. Remove persisted numeric `runtime.expected_device` from the next GPU contract.
   Resolve and attest the current hot-tier filesystem before work-order execution;
   keep the independent external pre-execution trust-anchor gate.
6. Introduce root IDs and relative CAS paths for new evidence. Add an explicit legacy
   root-map option only to compatibility auditors, not to ordinary producers.

Regression tests should cover a changed reported device number with identical bytes,
an inode-changing exact copy under an approved relocated root, changed content at the
same path, path replacement during an open read, symlink/hardlink substitution,
wrong filesystem UUID/tier, and an unreviewed relocation. Only the first two should
pass, and only in their intended replay mode.
