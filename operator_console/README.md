# HIMR local operator console

The operator console is a private, loopback-only interface for running finite,
registered HIMR pipeline commands without editing shell commands in a browser. It is
separate from Astro, Cloudflare Pages, the public corpus export, and the evaluation
review workspace. Its job records and logs are operational aids only; pipeline
validators, immutable results, and receipts remain completion authority.

## Active autonomous profile

The installed private profile set now exposes exactly two actions: **Start** and
**Stop**. Start runs the sealed eight-collection Archive.org campaign. The installed
adjacent long-form registration makes the same Start supervise that companion and
Stop their shared orderly stop request; no new
button or profile argument is introduced. Everything else is a monitor. The profile
binds the mode-`0400` production config at
`research/corpus/autonomous/archive-all-known-2026-08-30/controller-config.json`
with SHA-256
`5d0b549e91a870874e17724510723b33e557288f507ef41a35ab2849656dc913`.

Start first commits durable running intent and only then admits the foreground
controller to its user-systemd unit. The controller never overwrites a later Stop,
including a Stop clicked while the unit is still launching. Closing the browser or
restarting the console does not terminate the admitted unit. The outer unit has a
30-day runtime ceiling, 16-GiB memory limit, zero swap, 256-task cap, and 64-GiB
per-file limit. Ordinary GPU batches run in their exact bounded sibling units. The
long-form runner remains in the supervised outer operation and uses the same GPU-UUID
lock, offline runtime controls, and live GPU admission checks.

When a long-form companion is registered, the paired supervisor is the outer
unit's `MainPID`. Only its direct source-controller child receives the explicit GPU
delegation marker; admission also requires that marker, the live parent PID, the
unit `MainPID`, InvocationID, and cgroup to agree exactly. The long-form companion
does not inherit that delegation.

The sealed inventory contains eight Archive.org identifiers and 4,484 files. The
controller acquires all of them to `/mnt/archive/HIMR/corpus/raw/acquisition-cas`.
It preprocesses and runs ordinary GPU ASR for the 3,680 normal items. The
registered companion covers the 804 long items, plus any ordinary GPU queue result
later assigned `requires_chunking`, without persistent audio chunk files.

Validate and open the installed console from the repository root:

```sh
operator_console/bin/himr-operator validate-profiles
operator_console/bin/himr-operator serve --open-browser
```

Neither validation nor merely opening the console starts processing. Press **Start**
once when ready. Press **Stop** to finish the currently bounded stage and quiesce;
press Start again later to replay receipts and resume. The autonomous controller
overlaps its one acquisition lane, one preprocess lane, and one GPU child; Stop is
observed between acquisition dispatches and singleton preprocess items. Once
registered, the companion starts and stops automatically with the same buttons. It
gives ordinary ASR priority, and its GPU work remains held until the entire ordinary
pipeline is fully drained. It checks Stop between recordings and adaptive spans and
resumes only missing spans. A direct recording of 30 minutes or less is one bounded
span; longer recordings use overlapping logical spans and one forward-only, in-memory
FFmpeg decode.

### Long-form companion monitor and recovery

After companion setup, its registered status is the private mode-`0600` file:

```text
/srv/himr/research/operator-state/longform-asr-archive-all-known-2026-08-30/status.json
```

It reports the bound source and companion configs; lifecycle (`ready`, `running`,
`waiting`, `stopped`, or `faulted`); expected cold backlog; discovered cold, queue,
and total candidates; job counts for `unprepared`, `preprocessed`, `prepared`,
`incomplete`, and `completed`; current `active_job`; `updated_at`; and `last_error`.
`waiting` is normal while the next cold result is unavailable or the source controller
has not reached its all-lanes-drained snapshot. The campaign cycle classifies the
latter as `primary_pipeline_not_fully_drained`; the cached status remains the exact
field set listed above. Shared-lock contention is also a held retry, not a fault.

### Recording-level statistics without interrupting the campaign

The console's separate **Long-form ASR** panel displays completed recordings,
discovered recordings, remaining discovered work, preparation-stage counts,
active work, and the companion's own snapshot age. Completion means the
companion reported a whole-recording completion receipt; it does not count
completed spans, ordinary queue items, or merely successful subprocess exits.
The counters are cached projections, not a new verification of all receipts.
Waiting heartbeats can refresh the timestamp without recounting jobs, and the
current recording can finish after the snapshot was written.

Progress is explicitly a percentage **of discovered recordings**. Additional
expected cold candidates not yet discovered are shown separately. Queue-derived
candidates count recordings, not queue files. Active work is already included
in the discovered snapshot and must not be added to remaining work; its marker
can also remain present during finalization after completion is recorded. Cold
candidates not yet discovered are not necessarily undownloaded. The
ordinary-queue ASR counter remains separate: no combined unique-video total is
claimed without cross-lane deduplication evidence. Hours, throughput, and an ETA
are not inferred from these counts.

Absent registration, unavailable/corrupt statistics, zero completed recordings,
and an old cached snapshot are distinct states. Statistics failures do not
disable otherwise valid controller Start/Stop controls. This read-only path checks
only bounded configuration, registration, and cached-status metadata; it never
enumerates job directories, replays the event journal, hashes media, acquires
execution locks, or changes pipeline state.

For statistics immediately, without starting/restarting the console:

```sh
operator_console/bin/himr-pipeline-statistics \
  --config /srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/controller-config.json \
  --expected-config-sha256 5d0b549e91a870874e17724510723b33e557288f507ef41a35ab2849656dc913
```

Add `--json` for the separate controller and long-form projections, their own
timestamps, and availability markers. Unavailable values remain null, not zero;
partial reports remain visible with exit status 2. A missing optional companion
registration is not an error.

The existing console pins HTML/JavaScript/CSS in memory at startup. Browser
refresh does **not** load an on-disk dashboard update. The new panel appears at
the next reviewed, safe console launch. No console or campaign restart is
performed to install these reporting changes. Console startup can reconcile
operational records and Stop intent, so do not treat it as a read-only refresh.

For a read-only authoritative replay, use the bound config path and digest:

```sh
HIMR_LONGFORM_CONFIG=/srv/himr/research/corpus/autonomous/archive-all-known-2026-08-30/longform-asr-campaign-config.json
HIMR_LONGFORM_SHA=85a60d152e225bf9f09129bd613794d7105c003f398520bbab4c929b7e08a756

pipeline/bin/longform-asr-campaign status \
  --config "$HIMR_LONGFORM_CONFIG" \
  --expected-config-sha256 "$HIMR_LONGFORM_SHA"
```

That digest belongs to the installed config
`himrlongcfg_93be7667e911b9846d2dd711004e9d67`; operation must preserve those
exact bytes.

Do not delete an `incomplete` job. Start again replays its immutable completed spans
and continues the pending suffix. Recording assembly occurs only after full span
coverage. A `faulted` lifecycle always supplies `last_error`; inspect that error and
the outer Start job result, then use Stop if it is not already durable. The supervisor
coordinates Stop when either child fails and bounds shutdown of the other child.
Immutable queue-discovery receipts avoid repeatedly hashing unchanged ordinary queue
manifests or media during status/recovery; media is deeply checked when admitted for
the selected recording.

After a cold job seals its transcript and completion document, the companion removes
only that job's derived `preprocess/output` scratch and writes a cleanup receipt. Raw
media, plans, span results, bindings, transcripts, and receipts remain. Cleanup is
restart-reconciled; never manually remove its tombstone or broaden the deletion scope.
The compact transcript word schema stores text, exact sample times, and only present
raw probability/anomaly fields; shared provenance is not repeated on every word.

### Staging a revised campaign

Do not edit `profiles.json` in place while the console is running. Its SHA-256 is
pinned for the lifetime of the server, and a mid-session change intentionally makes
all later preparations fail closed. Stage a revised, already initialized campaign as
a separate profile set instead:

```sh
operator_console/bin/himr-operator stage-autonomy-successor \
  --expected-profile-set-sha256 CURRENT_PROFILE_SET_SHA256 \
  --config "$PWD/research/corpus/autonomous/FUTURE/controller-config.json" \
  --expected-config-sha256 SUCCESSOR_CONFIG_SHA256 \
  --output "$PWD/research/operator-console/profiles.future.json"
```

The staging command performs no processing and never replaces the active profile
file. It requires the current profile to contain exactly one Start and one Stop,
deep-replays both bound controller configs, requires the predecessor to be stopped,
requires the successor to be stopped or not yet started, verifies zero in-flight
lanes and no held controller lock, and emits a single-link mode-`0400` candidate.
It also requires a distinct successor config ID, digest, and state root.

Stop the console, then restart it with the staged set:

```sh
operator_console/bin/himr-operator validate-profiles \
  --profiles "$PWD/research/operator-console/profiles.future.json"
operator_console/bin/himr-operator serve --open-browser \
  --profiles "$PWD/research/operator-console/profiles.future.json"
```

The staged set contains only the successor Start and Stop bindings, so the old Start
is no longer exposed after that restart. The old controller config and stopped state
remain intact as provenance and recovery evidence.

## Registered implementation scope

The first admitted console version can:

- validate or run one bounded public acquisition-producer cycle;
- replay that producer schedule and preprocess up to eight exact completed,
  unacknowledged Archive results without a result-path glob;
- run one bounded rolling Archive action whose single network worker and single
  FFmpeg worker overlap while retaining producer backpressure;
- retain one exact, completed public acquisition at the fixed cold-storage content
  address after reviewed work-order, result, payload-digest, and byte-count replay;
- validate, dry-run, inspect, or run a bounded preprocessing bundle;
- validate an exact v0.2/v0.3 preprocess-ASR queue without executing it;
- deep-validate the candidate v0.3 runner or perform its explicitly confirmed,
  CPU-intensive whole-queue dry-run without writing ASR results;
- validate a closed private catalogue or create a text-free GPU-v3 admission plan;
- run the read-only historical v0.4 ASR receipt compatibility audit;
- materialize selected, explicitly ordered ready v0.3 queue members into exact v5
  work orders and one finite batch without reading audio payload bytes;
- run one reviewed v2 GPU batch through either the exact root-owned launcher or the
  admitted local-private launcher after every applicable admission gate passes;
- automatically supervise the registered recording-first long-form companion under
  the same Start/Stop intent and GPU UUID lock; and
- retain capped, private stdout/stderr and a restart-visible job history.

The `gpu.batch_v2` action uses the operator-side `systemd --user` supervisor, a
one-hour hard runtime ceiling, and a tracked 12 GiB `MemoryMax`. It invokes only
`/usr/local/libexec/himr-gpu/trusted-launcher-v2`; profile validation rejects a
missing, mutable, symlinked, non-root-owned, or byte-changed installation. The
legacy `gpu.batch` action remains disabled.

The interface deliberately does **not** expose arbitrary commands, arguments, URLs,
paths, environment variables, credentials, arbitrary deletion, arbitrary cold paths or
transfers, migrations, catalogue imports, publication, identity/biometric stages, or
an unlimited backlog button. The sole cold action retains one profile-bound public
acquisition at the fixed `/mnt/archive/HIMR` CAS path and grants none of those other
authorities. The supervised companion's only deletion is its closed, receipt-backed
post-completion `preprocess/output` scratch cleanup; the browser cannot select or
expand its target. Real v0.3 CPU-ASR dispatch is visibly blocked until it has a
bounded-prefix control, a dispatch lock, and a reviewed v0.3-to-v0.5 sealing pilot. Legacy GPU
execution is also visibly blocked because its work orders retain reboot-unstable
numeric filesystem-device bindings. The v2 lane instead uses a restart-portable root
identity, typed queue lineage, an immutable execution image, and an external
launcher; it still refuses production until the installed runtime receipt contains
all required passing gates.

The `archive.preprocess_next` action closes the acquisition-to-preprocessing handoff.
It derives result paths only from the replayed schedule's sealed work orders, applies
the background producer's existing receipt acknowledgements, and admits one
deterministic ASR-ready bundle per selected queue ordinal. A crash before receipt
admission reconstructs the same selection and resumes it; a valid receipt removes
that acquisition result from the next ready prefix. The action is limited to eight
items and uses the `preprocess` resource class, so it can overlap one network
acquisition action without competing with another FFmpeg worker. Its Python launcher
hash-pins the tracked handoff implementation before importing it.

For unattended overlap within one finite console job, use
`archive.rolling_pipeline`. It owns the composite `archive_pipeline` resource. The
console maps that resource to both the `network` and `preprocess` claims, so a rolling
job cannot race either standalone action; ordinary network and preprocess actions
remain able to overlap each other. Internally, the rolling action dispatches at most
one acquisition and one ASR-ready preprocess operation concurrently. It advances the
producer one ordinal per cycle, counts full sealed reservation bytes cumulatively,
honors the schedule's time and free-space limits, and waits on receipt-driven
backpressure instead of polling an unbounded backlog. Out-of-band manual commands
must not be launched against the same roots while this console action is active. The
no-shell Python launcher hash-pins both the rolling supervisor and its handoff module,
so a source edit after console preparation fails before either worker starts.

The `retention.public_acquisition` action copies exactly one completed public
`direct_http` or `yt_dlp` acquisition. Its profile binds physical SHA-256 values for
the work order and completed result plus the payload digest and byte count. The
adapter typed-replays that result, retains the original payload descriptor, seals a
separate content-addressed mode-`0400` staging object, and invokes the existing cold
transfer with a code-fixed `/mnt/archive/HIMR` destination. There is no destination
profile field. Its no-shell `/usr/bin/python3 -IB` entrypoint verifies embedded hashes
for the acquisition, cold-transfer, and retention modules and executes only their
retained bytes; the console separately rechecks the launcher itself. The action uses
the `cold_storage` resource and also claims the
network/acquisition lane, so it cannot race an acquisition or rolling job while it
retains the shared hot-root topology; preprocessing and GPU work may continue. It
requires `RETAIN ONE PUBLIC ACQUISITION` and never deletes, chmods, renames, or links
the acquisition payload. See
[`PUBLIC_ACQUISITION_RETENTION.md`](../acquisition/PUBLIC_ACQUISITION_RETENTION.md).

The receipt-bound ASR-ready-to-GPU handoff is implemented as
`gpu.materialize_v1`. Its profile carries a sorted, unique list of at most 32 queue
ordinals; the browser cannot alter that selection or any path/digest. It requires
`MATERIALIZE GPU BATCH` and emits only private work orders, a batch manifest, and a
materialization receipt. All seven output directories must exist as owner-only
directories before the profile is validated.

The stronger root-owned GPU path must first install and admit its trusted launcher,
immutable execution image, launcher profile, and runtime receipt. The local-private
path below uses the same production lineage and runtime gates with an explicitly
weaker same-UID trust boundary. Example profiles are templates only: replace every
digest and create their private directories after admission. Merely adding a profile
cannot bypass launcher or runtime gates.

The console reports those as blocked capabilities. It never fills the gap with a
recursive filesystem scan or filename guess.

## Initialize and configure

From the repository root:

```sh
operator_console/bin/himr-operator init
```

This creates `research/operator-console/`, an owner-only `service-state/` directory,
and an empty mode-`0400` `profiles.json`. It refuses to replace an existing profile
file. `service-state/` is reserved for the console's lock, job records, and logs;
profile-bound GPU, acquisition, preprocessing, and cold-retention state may remain in
separate trees such as `research/operator-console/state/`. All mutable console state
stays under the Git-ignored `research/` tree on the main drive.

A profile selects one tracked action and binds every path and limit before the server
starts. The browser receives non-parameter profile metadata only; it never receives
or sends a path, environment value, or argv fragment. To configure profiles:

```sh
chmod 0600 research/operator-console/profiles.json
${EDITOR:-vi} research/operator-console/profiles.json
chmod 0400 research/operator-console/profiles.json

operator_console/bin/himr-operator validate-profiles
```

Use [`profiles.example.json`](profiles.example.json) as a shape guide, replacing every
placeholder with a reviewed path. `$REPO/` expands to the current repository root.
Profiles may reference only non-symlink paths beneath this repository; private state
roots must be below `$REPO/research`. Public output trees, repository control files,
temporary system directories, and the cold archive are rejected before launch. The
public-retention action does not relax this rule: its destination is absent from the
profile and fixed inside the tracked adapter.

The configuration is strict JSON with this top-level shape:

```json
{
  "schema_version": 1,
  "profiles": [
    {
      "id": "preprocess-run-one",
      "label": "Run one ASR-ready item",
      "description": "One finite, resumable preprocessing invocation.",
      "action": "preprocess.run",
      "parameters": {
        "bundle": "$REPO/research/operator-inputs/preprocess-bundle",
        "state_root": "$REPO/research/operator-state/preprocess",
        "limit": 1
      }
    }
  ]
}
```

Unknown keys, duplicate JSON keys, non-finite numbers, unregistered actions, unsafe
paths, missing inputs, and out-of-range limits fail closed. Profiles cannot contain
tokens, cookies, passwords, proxy settings, or arbitrary environment entries.

## Start the interface

```sh
operator_console/bin/himr-operator serve --open-browser
```

Or use the repository shortcut:

```sh
npm run operator
```

The server binds only IPv4 `127.0.0.1`, choosing an ephemeral port by default. It
prints a one-use bootstrap URL that exchanges into an opaque route and an HttpOnly,
SameSite-strict session cookie. Mutating requests also require the in-memory CSRF
token and exact same origin. The console has no CORS support, remote-listen option,
external asset, CDN, analytics, or network API of its own.

The systemd-user backend requires root-owned, non-writable executables at
`/usr/bin/systemd-run`, `/usr/bin/systemctl`, and `/usr/bin/env`, plus a running user
manager. A configured systemd-user profile fails during profile validation if those
fixed tools are not installed; it never falls back to a shell, PID signal, or direct
supervisor.

Closing the browser is safe. Keep the console process running for actions using the
original `direct` supervisor: it cannot safely signal an entire descendant tree and
therefore does not offer cancellation. The admission-gated `gpu.batch_v2` action instead
uses the implemented `systemd_user` supervisor. Such a job survives a console restart in
one uniquely derived transient-service cgroup; the restarted console reconciles the
persisted unit name and `InvocationID`. No PID is stored or signalled.

For a supervised GPU job, the manager enforces the action's sealed `RuntimeMaxSec`,
`MemoryMax`, `MemorySwapMax=0`, `TasksMax=64`, `LimitNOFILE=1024`, a 32 MiB
file-output limit, `LimitCORE=0`,
`ExitType=cgroup`, `KillMode=control-group`, and a 15-second stop timeout followed by
SIGKILL. The
browser cannot choose a unit name, property, path, environment value, or limit.
Cancellation requires `CANCEL GPU JOB` and stops only the unit derived from that job
ID. It always ends in `cancelled_reconciliation_required`; run the underlying
validator before resuming.

The v0.3 runner dry-run is intentionally a **whole-queue** action, not a bounded
prefix. Its sealed contract can contain up to 128 items, 256 GiB of audio, and seven
days of duration. It writes no ASR result, but it can still spend substantial CPU and
I/O on hashing and media probing. The console therefore requires the exact confirmation
phrase shown in the prepared preview.

Custom locations remain explicit:

```sh
operator_console/bin/himr-operator serve \
  --workspace "$PWD/research/operator-console" \
  --profiles "$PWD/research/operator-console/profiles.json" \
  --state-root "$PWD/research/operator-console/service-state" \
  --port 0
```

Do not point `--state-root` at a pipeline asset tree. The console state root accepts
only its own `jobs/`, `service.lock`, and `service.json` entries and fails closed on
anything else. Omitting the option selects the separated `service-state/` path shown
above.

## Operating model

Each launch is a two-step operation:

1. **Prepare** reloads and rehashes the mode-`0400` profile file, validates its typed
   paths and bounds, verifies the exact entrypoint bytes, and returns a short-lived,
   one-use preparation token.
2. **Execute** consumes that token, repeats the profile and entrypoint checks, enforces
   the action's confirmation phrase, and queues the fixed argv array with `shell=False`.
   Supervisor kind, timeout, resource class, and host-memory ceiling are part of the
   repeated command identity check.

The child receives an explicit minimal environment, with user-site Python disabled,
model hubs offline, no inherited proxy or credential variables, UTC process time, and
stdin closed. At most three jobs may run concurrently, with one job per resource
class. This allows the network acquisition and preprocessing lanes to overlap while
preventing two workers from competing inside one lane. Underlying pipeline locks and
resource ceilings remain authoritative. A systemd-user worker starts behind
`/usr/bin/env -i`, so it does not inherit console or user-manager credentials.

The dashboard separates:

- process state (`launching`, `running`, detached, or exited);
- each controller lane's live running state from its latest completed outcome;
- stage status parsed from the latest bounded JSON, when available; and
- gates such as blocked, review-required, or admission-required.

The reservation panel is launch admission, not hardware utilization. `Reserved`
means that an active console job conflicts with that action class; a single
autonomous job therefore reserves network, preprocess, GPU, and cold-storage launch
classes even while only some internal lanes are physically active. The controller
lane cards, not those reservations, show which stages are running now. Cold
retention correctly reports `Skipped` in the cold-primary campaign because
acquisition already writes directly to `/mnt/archive/HIMR`.

It never presents one misleading “corpus complete” percentage. GET and log polling
read cached console state only; they do not repeatedly hash the corpus. Run an
explicit validation/status profile when authoritative replay is needed.

Stdout and stderr are captured separately, capped at 8 MiB each, committed after the
process exits, and rendered as text. Parsed summaries are capped at 32 KiB; larger
valid summaries become digest-bearing omission records, while the complete captured
stream remains available. The dashboard shows the newest 32 jobs and retains all job
records privately. An exit code or console log never proves completion. Check the
immutable result, receipt, or status command named by the underlying stage.

## Local-private GPU sequence

The independent local GPU path is a closed sequence of reviewed profiles:

1. `gpu.preprocess_queue_v1` seals one explicit preprocess bundle/state pair;
2. `gpu.prepare_local_private` creates user-owned launcher/profile/runtime controls;
3. `gpu.doctor_local_private` runs the no-media checks and emits a readiness receipt;
4. `gpu.materialize_local_v1` creates a candidate-runtime production-lineage batch;
5. `gpu.batch_local_private` requires that readiness receipt and runs under the
   same systemd-user 12 GiB/no-swap/64-task envelope as root production.

The local launcher pathname is an owner-reviewed profile parameter. The console
accepts it only as a current-user, single-link mode-`0500` file below the private
repository tree, then the launcher authenticates its own digest and bound profile.
The run action is unusable before doctor output exists; none of these actions can
import, publish, mutate the catalogue, access cold storage, or delete source media.

## Tests

The console test suite uses temporary repositories, fake finite executables, and a
fake systemd control plane only:

```sh
npm run test:operator
```

It must not start a real transient unit or access real media, a live catalogue, the
GPU, the network, or cold storage. Tests assert the exact manager argv, restart and
missing-unit reconciliation, `InvocationID` binding, timeout classification, private
logs, and exact-unit cancellation. Public-release checks also keep
`research/operator-console/` state and logs outside Astro, Pagefind, and Cloudflare
output.
