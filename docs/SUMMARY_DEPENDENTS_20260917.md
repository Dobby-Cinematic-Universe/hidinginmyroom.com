# Summary-dependent refresh

The September 17 standard-API tail and five targeted recoveries add six transcript
summaries after the original Claude selection (2,684 → 2,690 recordings).

`pipeline.sonnet_summary_delta` retains 135 completed scopes and recomputes only
December 2016, the undated monthly group, 2016, and the archive overview. Five
undated recordings remain undated. The new run is
`research/private-summaries/sonnet-summary-delta-20260917-v3`; v1/v2 are unsubmitted
preparation attempts, not resumable production runs. Existing source responses
remain unchanged. The remaining shared Anthropic allowance is enforced.

The local summary refresh selects the new reader only after all 139 scopes are
complete. The local watcher also watches the completed standard-API recovery.
Until then, the previous complete broader summaries stay visible.

`scripts/finish-summary-dependents.mjs` waits for the incremental synthesis, then:

- Refreshes local v8 and corrected release v9 summary overlays and event groups.
- Uploads only changed public RAG documents and stages the public Worker disabled.
- Updates private-chat summaries, retaining its exact original transcript set.
- Removes superseded index objects only with retained local backups and receipts.
- Builds and audits the fresh private `candidate-20260917-v7` release candidate.

Services: `himr-sonnet-summary-delta-v3-20260917.service`,
`himr-summary-dependents-20260917.service`, and the existing local preview watcher.
The dependent runner records progress in the delta directory's
`dependents-status.json`. A held job or failure requires inspection; it does not
retry uncertain paid submissions. These are finite jobs, not permanent monitors.

Upload acceptance is not remote indexing completion. No Pages deployment or
public AI activation is performed. Do not reuse the candidate name after a
partially completed packaging attempt without inspecting its receipt.

## Targeted recovery after the daily indexing allowance was exhausted

The incremental campaign reached 137/139 scopes. Five intermediate jobs failed
validation (too many items, invalid evidence references, or truncated output).
`pipeline.sonnet_delta_recovery` applies the existing strict recovery policy to
these held jobs, retaining successful results, source captures and the shared
budget. Each held job gets at most one replacement in this recovery overlay.
The service is `himr-sonnet-delta-recovery-20260917.service` and its receipts are
under `research/private-summaries/sonnet-delta-recovery-20260917`.

The replacement dependent service,
`himr-summary-dependents-recovery-20260917.service`, uses `--skip-cloud`: it refreshes
local summaries, event groups and the private release candidate, but makes no
Cloudflare calls. New broader-summary documents still need a separate index
refresh after the free daily allowance resets. No plan upgrade is requested.
