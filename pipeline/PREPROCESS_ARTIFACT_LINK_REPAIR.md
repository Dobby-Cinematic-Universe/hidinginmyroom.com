# Preprocess artifact single-link repair

`preprocess-artifact-link-repair` is a narrow administrative compatibility
tool for historical normalized-audio artifacts created by verified prior-result
hardlink reuse. It does not run preprocessing, edit a completed `result.json`,
or edit any existing receipt.

The tool handles one `audio_16khz_mono_flac` path per plan. The plan binds the
exact completed result, artifact path, SHA-256, byte count, uid/gid, mode,
inode, timestamps, and initial link count. Apply holds the recipe's existing
`.preprocess.lock`, retains the root/result/artifact through `O_NOFOLLOW`
descriptor chains, creates and verifies a distinct sibling inode, and atomically
exchanges it with only the artifact path using Linux
`renameat2(RENAME_EXCHANGE)`. The displaced entry is verified as the exact
planned inode before it is unlinked, so a target-name race is restored rather
than overwritten. Both plan and repair receipt must live directly
in the campaign's existing owner-only mode-`0700` `repair-control/` directory,
outside the preprocess and controller-state roots.
Apply also requires the exact existing controller `controller.lock`; it takes a
nonblocking exclusive flock before touching the artifact and holds it through
replacement, verification, and repair-receipt commit. An operator-console or
service restart therefore cannot race the repair.

Apply is idempotent across the exchange-to-receipt crash window. A deterministic
swap name lets a retry verify and finish an interrupted exchange. If the swap was
already cleaned up, a retry can reconcile only a single-link file with the exact
planned path, bytes, uid/gid, and mode; its receipt records that limited
observation instead of claiming an attributable replacement. An already durable
exact receipt is replayed without rewriting it.

Plans and repair receipts are published with same-directory
`renameat2(RENAME_NOREPLACE)`. Their final path therefore never passes through a
two-link publication state; an error after publication leaves a sealed
single-link document that `validate-plan` or exact receipt replay can recover.

Do not apply a repair while the autonomous controller or any preprocess writer
is running. First create and review a plan:

```sh
install -d -m 0700 /absolute/campaign/repair-control

pipeline/bin/preprocess-artifact-link-repair plan \
  --root /absolute/campaign/preprocess-output \
  --result /absolute/campaign/preprocess-output/.../run_preprocess_ID/result.json \
  --output /absolute/campaign/repair-control/repair-plan.json \
  --controller-lock /absolute/campaign/state/controller.lock

pipeline/bin/preprocess-artifact-link-repair validate-plan \
  --plan /absolute/campaign/repair-control/repair-plan.json
```

After review, apply that exact plan and retain its separate audit receipt:

```sh
pipeline/bin/preprocess-artifact-link-repair apply \
  --plan /absolute/campaign/repair-control/repair-plan.json \
  --receipt /absolute/campaign/repair-control/repair-receipt.json \
  --controller-lock /absolute/campaign/state/controller.lock
```

For an inode with exactly two artifact paths, detaching either one leaves both
paths single-link. Larger hardlink sets require fresh one-path plans until every
remaining referenced path reports `nlink == 1`; a stale plan fails closed.
