"""Finite supervisor for independent screening, transcription and Gemini stages.

Only an explicit paid run starts children. The launcher reads bound manifests,
not .env contents: each child loads its private file, with only its provider's
explicit environment-key overrides forwarded. A failure or deadline
requests graceful group shutdown, never automatic restart or SIGKILL. Paid
requests that outlive the shutdown grace remain with their original workers and
durable receipts; report them for inspection before attempting another launch.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from pipeline import cloud_transcription as cloud
from pipeline import cloud_transcription_summary as summaries


POLL_SECONDS = 60
# The shared bound must fit the screen stage's one-day invocation limit as well
# as the longer cloud/Gemini limits. Another invocation resumes retained work.
MAX_RUNTIME_SECONDS = 86400
CHILD_ENV = {"PATH": "/usr/bin:/bin", "PYTHONUNBUFFERED": "1", "LANG": "C"}
CHILD_KEY_NAMES = {
    "screen": (),
    "transcription": ("ASSEMBLYAI_API_KEY", "REVAI_ACCESS_TOKEN", "REVAI_API_KEY"),
    "summaries": ("GEMINI_API_KEY",),
}


class ParallelError(RuntimeError):
    pass


def _usd(microusd):
    return str(microusd // 1_000_000) + "." + str(microusd % 1_000_000).zfill(6)


def _child_environment(stage):
    # Keep documented explicit overrides, but never send a provider credential
    # to another provider/model stage or expose one in a command/log/report.
    return {**CHILD_ENV, **{name: os.environ[name] for name in CHILD_KEY_NAMES[stage] if name in os.environ}}


def commands(plan_ref, summary_ref, *, transcription_budget_microusd,
             max_runtime_seconds, env_file=None, transcription_concurrency=4, summary_concurrency=8):
    """Pure command construction; credentials are never read or placed in argv."""
    cloud.io.safe.file_binding(plan_ref)
    cloud.io.safe.file_binding(summary_ref)
    cloud.io.safe.integer(transcription_budget_microusd, 1, 10**12, "transcription budget")
    cloud.io.safe.integer(max_runtime_seconds, 1, MAX_RUNTIME_SECONDS, "parallel runtime")
    cloud.io.safe.integer(transcription_concurrency, 1, 8, "transcription concurrency")
    cloud.io.safe.integer(summary_concurrency, 1, 8, "summary concurrency")
    environment = [] if env_file is None else ["--env-file", str(cloud.io.safe.path_value(env_file))]
    python = str(Path(sys.executable).resolve())
    transcription = [python, "-B", "-m", "pipeline.cloud_transcription"]
    bound = ["--plan", plan_ref["path"], "--expected-sha256", plan_ref["sha256"]]
    duration = ["--max-runtime-seconds", str(max_runtime_seconds)]
    return {
        "screen": [*transcription, "screen", *bound, "--max-jobs", str(cloud.MAX_RECORDINGS), *duration],
        "transcription": [*transcription, "run", *bound, "--allow-paid-api",
                          "--budget-usd", _usd(transcription_budget_microusd),
                          "--max-active", str(transcription_concurrency),
                          "--max-new-jobs", str(transcription_concurrency), *duration,
                          "--poll-seconds", str(POLL_SECONDS), *environment],
        "summaries": [python, "-B", "-m", "pipeline.cloud_transcription_summary", "run",
                      "--manifest", summary_ref["path"], "--expected-sha256", summary_ref["sha256"],
                      "--allow-paid-api", "--max-active", str(summary_concurrency),
                      "--max-new-waves", str(summary_concurrency),
                      "--max-cycles", str(math.ceil(max_runtime_seconds / POLL_SECONDS)),
                      "--poll-seconds", str(POLL_SECONDS), *environment],
    }


def _validated_commands(plan_ref, summary_ref, *, transcription_budget_microusd,
                        max_runtime_seconds, env_file, transcription_concurrency=4, summary_concurrency=8):
    result = commands(plan_ref, summary_ref,
                      transcription_budget_microusd=transcription_budget_microusd,
                      max_runtime_seconds=max_runtime_seconds, env_file=env_file,
                      transcription_concurrency=transcription_concurrency, summary_concurrency=summary_concurrency)
    plan = cloud.load_plan(plan_ref)
    worker = summaries.load_manifest(summary_ref)
    if worker["cloud_plan"] != plan_ref:
        raise ParallelError("summary worker belongs to a different cloud transcription plan")
    if plan["screen_config"] is None:
        raise ParallelError("parallel stages require a hash-bound screening configuration")
    # Detect a lost/corrupt reservation ledger or changed immutable ceiling
    # before another stage can start spending independently.
    status = cloud.status(plan_ref)
    limit = status["spending_limit_microusd"]
    if limit is not None and limit != transcription_budget_microusd:
        raise ParallelError("transcription budget differs from the existing immutable workspace limit")
    return result


@contextmanager
def pause_signals():
    requested = [None]
    def request(signum, _frame):
        if requested[0] is None:
            requested[0] = signum
    previous = {kind: signal.signal(kind, request) for kind in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield lambda: requested[0]
    finally:
        for kind, handler in previous.items():
            signal.signal(kind, handler)


def _alive(children):
    return {name: child for name, child in children.items() if child.poll() is None}


def _terminate(children):
    """Only the three sessions started here; never arbitrary/broad process sets."""
    errors = []
    for name, child in _alive(children).items():
        if type(child.pid) is not int or child.pid <= 1:
            errors.append(name)
            continue
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except OSError:
            errors.append(name)
    return errors


def run(plan_ref, summary_ref, *, allow_paid_api=False,
        transcription_budget_microusd=None, max_runtime_seconds=3600,
        env_file=None, shutdown_grace_seconds=240, stopping=lambda: None,
        on_event=lambda _event: None, transcription_concurrency=4, summary_concurrency=8):
    """Run at most three children once, preserving in-flight paid operations.

    Return codes: 0 workers exited (not proof the archive is complete), 2 review
    required, 124 finite run deadline, 128+signal an operator pause. The default
    240-second grace is bounded; unresponsive children are reported, not killed.
    """
    if allow_paid_api is not True:
        raise ParallelError("parallel paid stages require explicit --allow-paid-api")
    cloud.io.safe.integer(shutdown_grace_seconds, 1, 1800, "graceful shutdown interval")
    stages = _validated_commands(plan_ref, summary_ref,
        transcription_budget_microusd=transcription_budget_microusd,
        max_runtime_seconds=max_runtime_seconds, env_file=env_file,
        transcription_concurrency=transcription_concurrency, summary_concurrency=summary_concurrency)
    children, observed = {}, {}
    reason, exit_code, error = None, None, None
    signal_request = stopping()
    if signal_request:
        return {"state": "paused", "exit_code": 128 + int(signal_request), "workers": {},
                "remaining_worker_pids": {}, "automatic_restart": False, "force_kill_used": False}
    deadline = time.monotonic() + max_runtime_seconds
    try:
        for name, command in stages.items():
            signal_request = stopping()
            if signal_request:
                reason, exit_code = "paused", 128 + int(signal_request)
                break
            try:
                child = subprocess.Popen(command, cwd=str(cloud.io.ROOT), env=_child_environment(name),
                    stdin=subprocess.DEVNULL, stdout=None, stderr=None, close_fds=True,
                    start_new_session=True)
            except (OSError, ValueError, subprocess.SubprocessError):
                reason, exit_code, error = "launch_failed", 2, "child process could not be started"
                break
            children[name] = child
            on_event({"event": "worker_started", "stage": name, "pid": child.pid})
            immediate = child.poll()
            if immediate is not None and immediate != 0:
                observed[name] = immediate
                reason, exit_code = "child_failed", 2
                break
        while reason is None:
            for name, child in children.items():
                code = child.poll()
                if code is not None and name not in observed:
                    observed[name] = code
                    on_event({"event": "worker_exited", "stage": name, "exit_code": code})
            if any(code != 0 for code in observed.values()):
                reason, exit_code = "child_failed", 2
                break
            # Screen normally finishes before cloud and Gemini; its successful
            # exit alone neither cancels nor restarts the other two stages.
            if len(observed) == len(children):
                reason, exit_code = "workers_exited", 0
                break
            signal_request = stopping()
            if signal_request:
                reason, exit_code = "paused", 128 + int(signal_request)
                break
            if time.monotonic() >= deadline:
                reason, exit_code = "runtime_limit", 124
                break
            time.sleep(min(1, max(0, deadline - time.monotonic())))
    except KeyboardInterrupt:
        reason, exit_code = "paused", 128 + signal.SIGINT
    except Exception:
        reason, exit_code, error = "supervision_failed", 2, "local supervisor monitoring failed"
    except BaseException:
        # Also preserve workers on unexpected local interruption. Do not fall
        # through to Popen context-manager waits or unbounded communicate().
        _terminate(children)
        raise
    shutdown_errors = []
    if _alive(children):
        on_event({"event": "graceful_shutdown_requested", "reason": reason,
                  "grace_seconds": shutdown_grace_seconds})
        shutdown_errors = _terminate(children)
        grace_end = time.monotonic() + shutdown_grace_seconds
        while _alive(children) and time.monotonic() < grace_end:
            time.sleep(min(1, max(0, grace_end - time.monotonic())))
    remaining = {name: child.pid for name, child in _alive(children).items()}
    workers = {name: {"pid": child.pid, "exit_code": child.poll()} for name, child in children.items()}
    report = {"state": "shutdown_incomplete" if remaining or shutdown_errors else reason,
              "exit_code": 2 if remaining or shutdown_errors else exit_code,
              "stop_reason": reason, "workers": workers, "remaining_worker_pids": remaining,
              "automatic_restart": False, "force_kill_used": False,
              "transcription_budget_microusd": transcription_budget_microusd,
              "summary_budget_authority": "immutable_summary_manifest",
              "worker_exit_is_full_archive_completion": False}
    if error:
        report["error"] = error
    if remaining or shutdown_errors:
        report["action_required"] = "inspect retained workers and paid receipts before another launch"
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--summary-manifest", required=True)
    parser.add_argument("--summary-expected-sha256", required=True)
    parser.add_argument("--allow-paid-api", action="store_true")
    parser.add_argument("--transcription-budget-usd", type=cloud.usd, required=True)
    parser.add_argument("--max-runtime-seconds", type=int, default=3600)
    parser.add_argument("--shutdown-grace-seconds", type=int, default=240)
    parser.add_argument("--transcription-concurrency", type=int, default=4)
    parser.add_argument("--summary-concurrency", type=int, default=8)
    parser.add_argument("--env-file")
    args = parser.parse_args(argv)
    with pause_signals() as stopping:
        report = run({"path": args.plan, "sha256": args.expected_sha256},
            {"path": args.summary_manifest, "sha256": args.summary_expected_sha256},
            allow_paid_api=args.allow_paid_api,
            transcription_budget_microusd=args.transcription_budget_usd,
            max_runtime_seconds=args.max_runtime_seconds, env_file=args.env_file,
            shutdown_grace_seconds=args.shutdown_grace_seconds, stopping=stopping,
            transcription_concurrency=args.transcription_concurrency, summary_concurrency=args.summary_concurrency,
            on_event=lambda event: print(cloud.io.canonical(event).decode().strip(), flush=True))
    print(cloud.io.canonical(report).decode().strip(), flush=True)
    return report["exit_code"]


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError, KeyError, TypeError):
        # Underlying filesystem/launch errors may contain arbitrary paths;
        # credentials/provider response bodies never belong in supervisor logs.
        print("Parallel cloud pipeline stopped during local preflight or supervision; inspect the bound workspaces.", file=sys.stderr)
        raise SystemExit(2)
