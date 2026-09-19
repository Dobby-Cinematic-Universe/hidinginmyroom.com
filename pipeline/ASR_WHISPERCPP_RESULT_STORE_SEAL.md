# Completed whisper.cpp result-store sealing

`asr-whispercpp-result-store-seal` is a separate administrative lane for completed
private ASR result directories. It cannot execute ASR, import into a catalog, write a
database, publish data, assign identities, or use the network. It does not change the
ASR adapter, batch/queue runners, engine profiles, or their active schemas.

The lane has four commands:

- `plan` inspects an explicit list of `result.json` paths and writes a mode-`0400`
  prepared plan. It does not chmod any result.
- `validate-plan` repeats all source, byte, catalog-free, path, link, mode, and race
  checks for an unchanged prepared plan.
- `apply` applies one reviewed plan and writes a mode-`0400` receipt. This is the only
  command that changes result permissions.
- `validate-receipt` repeats the checks against the sealed files and receipt.

There is no generic directory scan in the tool. `plan` requires every result path as
an explicit `--result` argument. It has three versioned source-authority modes:

- `two-source-v1` is the backward-compatible default. It requires exactly one sealed
  long-window batch and one sealed short preprocess queue. Every batch entry and only
  queue entries whose sealed `routing_hint` is `process` must be represented.
- `queue-only-v2` requires exactly one sealed short preprocess queue, forbids a batch,
  and requires all and only entries whose sealed `routing_hint` is `process`.
- `batch-only-v3` requires exactly one sealed long-window batch, forbids a queue, and
  requires every batch work order to have exactly one allowlisted completed result.

A v3 plan and receipt carry a batch-only `source_authority` record. Replay invokes
`asr_whispercpp_batch.validate_batch`, which deterministically rebuilds the batch from
its read-only catalog snapshot and sealed local-window inputs. The authority record
pins the batch validator and manifest-schema byte identities, catalog binding, safety
digest, materializer name/version, exact batch identity, and
`all_batch_work_orders` selection policy.

Missing, duplicate, review-only, legacy, or otherwise unmatched result paths fail
closed. A v2 plan and its v2 receipt both carry a `source_authority` record binding the
queue ID, semantic identity, manifest path and byte hash, authority schema version,
and exact `all_and_only_routing_hint_process_entries` selection policy. Its
`queue_contract` also pins the authoritative preprocess-queue validator and JSON
schema byte identities, materializer name/version, manifest schema version, and
safety-policy digest. Replay calls `preprocess_asr_queue.validate_queue`, repeats its
deterministic evidence/input reconstruction, opens the same mode-`0400` manifest and
work orders, and reconstructs the complete eligible set. Receipt provenance is not
inferred from filenames or a directory scan. Queue routing hints retain the upstream
`string|null` contract, including an empty string; only exact `"process"` selects a
result.

## Checks and transition

For every planned result, the tool retains descriptors for the result directory and
all three allowed files for the entire operation:

1. `result.json`
2. `transcript.normalized.json`
3. `whisper.raw.json`

The directory must contain exactly those entries, be a real resolved directory at
mode `0700`, and retain the planned device, inode, mtime, and ctime. Each file must be
a real resolved regular file at mode `0644`, have `nlink == 1`, and retain the planned
device, inode, size, mtime, ctime, and SHA-256. Opens use `O_NOFOLLOW`; descriptor and
logical-path identities must agree before and after every path-based validation.

The tool calls only
`himr_corpus.asr_result_importer.validate_asr_whispercpp_result_file`, which validates
the completed result and artifact bytes without opening a catalog. Source manifest,
work-order, input, engine, adapter-version, result-key, and result-path identities must
all agree.

`plan` precreates and retains the private receipt directory and a single-link
mode-`0600` store-wide apply lock before observing any result. `apply` holds an
exclusive lock from receipt preflight through the terminal receipt commit. An exact
existing valid receipt is an idempotent success; any other preexisting target fails
before a result mode can change.

`apply` then uses `fchmod` on the retained descriptors to change the three files to
`0400` and the exact result directory to `0500`. It fsyncs every file and directory,
reruns catalog-free validation, and completes every source, plan, descriptor, path,
content, mtime, and captured-ctime check before committing the receipt. The receipt is
built from the already retained plan bytes. Publication uses a retained control-dir
descriptor, a retained temporary-file descriptor, directory-relative no-clobber hard
linking, and final inode/body/mode checks. File content, size, and mtime and directory
entries and mtime must remain unchanged.

The private result-store root is already mode `0700`, so this policy changes no shared
descendant containers. Tightening shared hash/result containers would add unrelated
state changes without improving access past the existing private-root barrier.

## Safety boundaries

Modes are evidence and accidental-write protection, not an append-only filesystem:
the owning account can reverse chmod, and a writable parent can remove a sealed
directory. Durable detection comes from the plan/receipt hashes, retained inode
bindings, and repeatable receipt validation.

Application is fail-closed and transaction-like. Under the lock it accepts only an
exact all-before state or an exact all-after/no-receipt state. A mixed or invalid state
is restored to every mode recorded by the plan and reported as an error. Any caught
error before receipt commit likewise restores all recorded pre-modes; rollback
failures are reported explicitly. A fully verified all-after state can complete a
receipt after an interrupted attempt, while a valid existing receipt replays
idempotently. Human review is still required before each `apply`; never apply a plan
whose implementation or source pins no longer validate.

The plan and receipt are private control documents under the selected mode-`0700`
result store's `sealing-control/{plans,receipts}` directory. They are ignored private
state, outside all sealed ASR source directories. Existing v1 and v2 plans and
receipts keep their original shapes and remain replayable; queue-only authority is
emitted only as schema v2, while batch-only plans and receipts use schema v3 without
an unrelated queue authority record. The one already deployed v1 implementation byte
identity is explicitly grandfathered for closed replay only. V2 and v3 never accept
a legacy implementation pin; any prepared plan whose current implementation pin no
longer matches is stale and must be regenerated and reviewed.

The exact historical v0.4 `validate-receipt` command remains frozen and can reject an
otherwise unchanged queue-only v2 receipt after a reboot changes the mount's Linux
device number. The separate
[`asr-whispercpp-v04-receipt-audit`](bin/asr-whispercpp-v04-receipt-audit) command is
read-only compatibility replay for only the exact retained v0.4-sealer/v0.2-queue
contract. It creates no receipt and does not authorize downstream import. See
[`ASR_WHISPERCPP_V04_RECEIPT_AUDIT.md`](ASR_WHISPERCPP_V04_RECEIPT_AUDIT.md).

## Generic command shape

```sh
pipeline/bin/asr-whispercpp-result-store-seal plan \
  --source-mode two-source-v1 \
  --batch-manifest /absolute/path/to/batch/manifest.json \
  --queue-manifest /absolute/path/to/queue/manifest.json \
  --output-directory /absolute/path/to/sealing-control/plans \
  --result /absolute/path/to/first/result.json \
  --result /absolute/path/to/each/additional/result.json

pipeline/bin/asr-whispercpp-result-store-seal plan \
  --source-mode queue-only-v2 \
  --queue-manifest /absolute/path/to/queue/manifest.json \
  --store-root /absolute/path/to/private-result-store \
  --output-directory /absolute/path/to/private-result-store/sealing-control/plans \
  --result /absolute/path/to/first/result.json \
  --result /absolute/path/to/each/additional/result.json

pipeline/bin/asr-whispercpp-result-store-seal plan \
  --source-mode batch-only-v3 \
  --batch-manifest /absolute/path/to/batch/manifest.json \
  --store-root /absolute/path/to/private-result-store \
  --output-directory /absolute/path/to/private-result-store/sealing-control/plans \
  --result /absolute/path/to/first/result.json \
  --result /absolute/path/to/each/additional/result.json

pipeline/bin/asr-whispercpp-result-store-seal validate-plan \
  --plan /absolute/path/to/asrsealplan_ID.json

pipeline/bin/asr-whispercpp-result-store-seal apply \
  --plan /absolute/path/to/asrsealplan_ID.json

pipeline/bin/asr-whispercpp-result-store-seal validate-receipt \
  --receipt /absolute/path/to/asrsealreceipt_ID.json
```

Always run `validate-plan` immediately before the separately reviewed `apply` command.
The current recovery plan is prepared but must not be applied by its materializer.

## Prepared queue-only plan commands for the completed local queues

These commands create reviewable plans only; they do not chmod results. Run them from
the repository root. The explicit allowlists below are the three current Australian
results and five current family/Mila results. `himr_repo_root=$PWD` deliberately keeps
the public documentation independent of one workstation while satisfying the CLI's
absolute-path requirement.

```sh
himr_repo_root=$PWD

pipeline/bin/asr-whispercpp-result-store-seal plan \
  --source-mode queue-only-v2 \
  --queue-manifest "$himr_repo_root/research/corpus/australian-asr-queue/queues/asrppqueue_d9b48cc13b79f710223468bb5f41be75/manifest.json" \
  --store-root "$himr_repo_root/research/corpus/australian-asr-results" \
  --output-directory "$himr_repo_root/research/corpus/australian-asr-results/sealing-control/plans" \
  --result "$himr_repo_root/research/corpus/australian-asr-results/asr/whispercpp/sha256/2b/2b785c318076bad68159f580473b6550944c678e36d8d59904ea0ccc2ab2c898/results/679ff360bb542712869964f04c6b91515f669d01c3158282903daf3605af4b7c/result.json" \
  --result "$himr_repo_root/research/corpus/australian-asr-results/asr/whispercpp/sha256/51/51fd3bf4f1371971ee8b41f102505c2f7183f68fe23a74731d970941e4707a78/results/153074eb346f648ce4d562a284aca6a5dda5785671118e90ef3d775af7060908/result.json" \
  --result "$himr_repo_root/research/corpus/australian-asr-results/asr/whispercpp/sha256/f3/f3e7118f597415da8650886adf2df2c6e6eaf4295cecf031262498c84567752f/results/249fcf5ab55d23d930ce5d94279d21e5b9104d8f7a8a53a004e237c95cb34460/result.json"
```

```sh
himr_repo_root=$PWD

pipeline/bin/asr-whispercpp-result-store-seal plan \
  --source-mode queue-only-v2 \
  --queue-manifest "$himr_repo_root/research/corpus/family-mila-asr-queue/queues/asrppqueue_cd9d18224d89c5b6c433263c4d45273a/manifest.json" \
  --store-root "$himr_repo_root/research/corpus/family-mila-asr-results" \
  --output-directory "$himr_repo_root/research/corpus/family-mila-asr-results/sealing-control/plans" \
  --result "$himr_repo_root/research/corpus/family-mila-asr-results/asr/whispercpp/sha256/18/18079fe6b5822e81043373480a761838f85adb6128a580bf577c377fcb8f898b/results/a5e05b6db7689d3a17f34aed332d2fb0d4d4a6ea89bdd76b86abefff5aa96cbd/result.json" \
  --result "$himr_repo_root/research/corpus/family-mila-asr-results/asr/whispercpp/sha256/97/97e2aa5484f41f2dcf1dcfa14cf78b5ce906e8972cc46cefbba4b0e75646eb77/results/8d71e7b36d9b324ac3dd32e84a8716175d985853ce2ad4f4b7de38e60bf4f794/result.json" \
  --result "$himr_repo_root/research/corpus/family-mila-asr-results/asr/whispercpp/sha256/c2/c2427c831afc1c7e0456769f337d1587af616d16c7cc25f12cb3e6151effe4a3/results/bbb64765a6eee63028c6ba6008381d50542982c229240f123cb86f51917f2de0/result.json" \
  --result "$himr_repo_root/research/corpus/family-mila-asr-results/asr/whispercpp/sha256/e1/e19d224801229feba9804a6652f8ee71c85b579091b931b9dc69e977c1a55c37/results/45aa9529430b09ec7a6c9305f300d851470865725b289acbbfb1ce263066048f/result.json" \
  --result "$himr_repo_root/research/corpus/family-mila-asr-results/asr/whispercpp/sha256/fe/fe0b870619a47af611dac8f6f8b6bc709133a5920b313e9d180b4cf0af1f6495/results/3982da03b0fd801b6cddafd424102cbd5dcb172db6a43d840a7ad835953b2fe9/result.json"
```
