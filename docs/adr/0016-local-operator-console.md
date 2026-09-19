# ADR 0016: Local registered-profile operator console

- Status: accepted
- Date: 2026-08-29

## Context

The corpus now has finite, resumable acquisition and preprocessing workers, strict
validation commands, private admission planners, and a GPU batch prototype. Operating
them directly requires repeatedly assembling long argv arrays and remembering which
contracts are admitted, candidate-only, or blocked. That is error-prone and makes
routine operation depend unnecessarily on an interactive engineering session.

The existing Astro site cannot host pipeline controls: it is a static,
publication-safe Cloudflare artifact. The ASR-blind evaluation workspace already has
a hardened private loopback server, but its inputs, attestations, and threat model are
specific to evaluation review and must not be expanded into a general executor.

The pipeline is also not yet one automatic GPU chain. Acquisition summaries omit the
exact result paths required by preprocessing; no reviewed ASR-ready receipt to GPU-v3
work-order builder exists; and current GPU contracts retain reboot-unstable numeric
filesystem identity. A user interface must show those gaps instead of guessing paths
or relabelling the whisper.cpp v0.3 queue as GPU work.

## Decision

Add a separate, standard-library-only `operator_console` package with these boundaries:

1. It binds only IPv4 `127.0.0.1` and uses a one-use bootstrap URL, opaque route,
   route-scoped HttpOnly SameSite-strict session cookie, separate CSRF token, exact
   Host and Origin checks, strict request framing, restrictive local-only CSP, and no
   CORS or external assets.
2. The browser selects only a profile ID. A mode-`0400`, owner-controlled JSON profile
   maps that ID onto one tracked `ActionSpec` and its exact typed parameters. There is
   no shell box, argv editor, environment field, URL input, path picker, or plugin
   command.
3. Before preparation and again before launch, the server reloads the exact profile
   bytes, validates paths and numeric bounds, and rehashes the registered entrypoint.
   It invokes an argv array with `shell=False`, closed stdin, fixed repository cwd,
   `umask 077`, and a newly constructed environment that inherits no credentials,
   proxies, or user Python configuration.
4. Job records and logs are owner-private, bounded, and operational only. They never
   become completion, catalogue, publication, identity, deletion, or transfer
   authority. The underlying immutable result, receipt, and explicit validator remain
   authoritative.
5. GET requests return cached console state and capped logs only. They never turn UI
   polling into repeated full-corpus hashing. Authoritative reconciliation is an
   explicit finite validation/status job.
6. Resource classes supplement rather than replace stage locks. The initial global
   capacity is three with one job in each resource class, allowing an acquisition and
   preprocessing job to overlap without permitting two workers in either lane.
7. The initial direct-process supervisor does not offer cancellation. It cannot prove
   that signalling a direct parent also stopped every nested child or new session.
   Jobs are finite and bounded; interruption recovery is delegated to the stage's
   normal receipt replay. A future systemd-user control-group supervisor may add safe
   cancellation and console-restart survival.

## Initial runnable registry

The initial allowlist contains read-only acquisition/preprocess/queue/catalogue and
historical-receipt validation, one bounded public acquisition cycle, and one bounded
preprocessing cycle. It also exposes deep v0.3 runner validation and an explicitly
confirmed whole-queue CPU dry-run. Real v0.3 dispatch remains disabled because that
candidate lacks a bounded-prefix control and dispatch lock and has not completed its
reviewed v0.3-to-v0.5 sealing pilot. Catalogue imports, migrations, publication,
export, model/runtime admission, identity/biometric execution, deletion, and cold
transfer are absent.

GPU batch controls remain disabled until the portability successor and soak gates are
admitted. The console may display the blocker but cannot accept a profile flag or HTTP
request that bypasses it. Private GPU-v3 catalogue admission is plan-only while
migration 0034 remains unapplied; the digest-gated import stays a separate
administrative operation.

## State and UI semantics

The dashboard keeps three axes distinct:

- process state, such as launching, running, detached, or exited;
- pipeline-reported stage status parsed from a bounded final JSON document, when
  present; and
- readiness gates, such as blocked, review-required, or admission-required.

It does not invent one project-completion percentage. Stdout and stderr remain
separate; invalid or oversized output is diagnostic failure, not a reason to reinterpret
the underlying stage. Missing console logs do not invalidate an immutable receipt, and
a log claiming completion does not establish one.

Polling state exposes only the newest 32 jobs. Valid parsed summaries are capped at
32 KiB and larger summaries are represented by a digest-bearing omission record; the
bounded diagnostic stream remains available separately. The complete private job
history remains on disk, so response size does not grow without bound over a multiweek
run.

## Consequences

- Routine bounded work can be launched and inspected without reconstructing commands
  or giving a browser general command-execution authority.
- The console can evolve as successor contracts are admitted by adding reviewed typed
  actions; a private profile cannot enable code that is not in the tracked registry.
- Full unattended GPU backfill remains honestly blocked on pipeline work rather than
  hidden behind a decorative “run all” control.
- The server must remain running for the first supervisor version. Long-running
  restart survival and safe cancellation require a separately tested control-group
  worker.
- Operator state stays below the ignored `research/` root and is excluded from Astro,
  Pagefind, Cloudflare output, and public Git history.
