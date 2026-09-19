"""Finite cloud lane with retries restricted to unbillable upload and GET calls.

Run this new, separately hash-pinned module from the already approved runtime
package. It never edits that runtime or its manifests. Paid POSTs are delegated
exactly once; their errors escape unchanged so original intents/reservations and
ambiguous-submission protections remain authoritative.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
import random
import sys
import time

from pipeline import cloud_transcription as cloud
from pipeline import cloud_transcription_release as release

MAX_RUNTIME_SECONDS = 86400
MAX_BUDGET_MICROUSD = 150_000_000
MAX_ATTEMPTS = 4
MAX_BACKOFF_SECONDS = 300
HTTP_MESSAGES = frozenset({"cloud request failed with an HTTP status",
                           "cloud request returned an unexpected HTTP status"})


class ResilientError(RuntimeError):
    pass


class RetryInterrupted(ResilientError):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class SafeOperationExhausted(ResilientError):
    """Only the narrow retry proxy may authorize an outer safe-operation retry."""
    def __init__(self, provider, operation, error, attempts):
        super().__init__("safe operation exhausted its bounded retry window")
        self.provider, self.operation = provider, operation
        self.error, self.attempts = error, attempts


def error_metadata(error):
    status = getattr(error, "status_code", None)
    ambiguous = getattr(error, "ambiguous", None)
    retry_after = getattr(error, "retry_after_seconds", None)
    return {"status_code": status if type(status) is int and 100 <= status <= 599 else None,
            "ambiguous": ambiguous if type(ambiguous) is bool else None,
            "retry_after_seconds": retry_after if type(retry_after) in {int, float}
                and 0 <= retry_after <= 2**53 and math.isfinite(retry_after) else None}


def transient(error):
    """No generic status=None retry: normalization/data errors also use None."""
    if not isinstance(error, cloud.clients.CloudClientError) or error.response is not None:
        return False
    status = error.status_code
    controlled = str(error)
    if controlled in HTTP_MESSAGES:
        return type(status) is int and (status in {408, 429} or 500 <= status <= 599)
    if controlled == "cloud request transport failed":
        return status is None or type(status) is int and (
            200 <= status <= 299 or status in {408, 429} or 500 <= status <= 599)
    return False


def _wait(seconds, *, deadline, stopping, clock, sleep):
    end = min(clock() + seconds, deadline)
    while clock() < end:
        if stopping():
            raise RetryInterrupted("paused")
        sleep(min(1.0, end - clock()))
    if stopping():
        raise RetryInterrupted("paused")
    if clock() >= deadline:
        raise RetryInterrupted("runtime_limit")


class RetryingClient:
    """Retry three explicit safe operations, never either paid submission API."""
    def __init__(self, provider, client, *, deadline, stopping=lambda: False,
                 on_event=lambda _event: None, attempts=MAX_ATTEMPTS,
                 clock=time.monotonic, sleep=time.sleep, jitter=random.random):
        if provider not in {"assemblyai", "revai"}:
            raise ResilientError("unsupported retry provider")
        cloud.io.safe.integer(attempts, 1, MAX_ATTEMPTS, "safe retry attempts")
        self.provider, self.client = provider, client
        self.deadline, self.stopping, self.on_event = deadline, stopping, on_event
        self.attempts, self.clock, self.sleep, self.jitter = attempts, clock, sleep, jitter

    def _safe(self, operation, *args, **kwargs):
        if operation not in {"upload", "poll", "transcript"} or operation == "upload" and self.provider != "assemblyai":
            raise ResilientError("retry operation is not an explicitly unbillable upload or GET")
        for attempt in range(1, self.attempts + 1):
            if self.stopping():
                raise RetryInterrupted("paused")
            if self.clock() >= self.deadline:
                raise RetryInterrupted("runtime_limit")
            try:
                return getattr(self.client, operation)(*args, **kwargs)
            except cloud.clients.CloudClientError as error:
                if not transient(error):
                    raise  # Preserve data/auth failures and any captured response.
                if attempt == self.attempts:
                    raise SafeOperationExhausted(self.provider, operation, error, attempt) from None
                metadata = error_metadata(error)
                backoff = min(MAX_BACKOFF_SECONDS, 5 * 2**(attempt - 1))
                backoff *= 1 + 0.25 * max(0.0, min(1.0, self.jitter()))
                delay = max(backoff, metadata["retry_after_seconds"] or 0)
                if delay > MAX_BACKOFF_SECONDS:
                    # Do not retry sooner than a long Retry-After. The outer
                    # cooldown honors it, bounded by the invocation deadline.
                    raise SafeOperationExhausted(self.provider, operation, error, attempt) from None
                self.on_event({"event": "safe_operation_retry", "provider": self.provider,
                               "operation": operation, "attempt": attempt, "next_attempt": attempt + 1,
                               "delay_seconds": delay, **metadata, "paid_post_retried": False})
                _wait(delay, deadline=self.deadline, stopping=self.stopping,
                      clock=self.clock, sleep=self.sleep)

    def upload(self, *args, **kwargs):
        return self._safe("upload", *args, **kwargs)

    def poll(self, *args, **kwargs):
        return self._safe("poll", *args, **kwargs)

    def transcript(self, *args, **kwargs):
        return self._safe("transcript", *args, **kwargs)

    def submit(self, *args, **kwargs):
        return self.client.submit(*args, **kwargs)

    def submit_file(self, *args, **kwargs):
        return self.client.submit_file(*args, **kwargs)


def run(plan_ref, *, allow_paid_api=False, budget_microusd=MAX_BUDGET_MICROUSD,
        max_runtime_seconds=MAX_RUNTIME_SECONDS, max_active=4, env_file,
        poll_seconds=60, cooldown_seconds=300, stopping=lambda: False,
        on_event=lambda _event: None, client_factory=None, clock=time.monotonic,
        sleep=time.sleep, jitter=random.random):
    """Resume original durable state, with finite cooldown after safe failures."""
    if allow_paid_api is not True:
        raise ResilientError("resilient cloud lane requires explicit paid API approval")
    cloud.io.safe.file_binding(plan_ref)
    cloud.io.safe.path_value(env_file)
    for value, low, high, label in (
            (budget_microusd, 1, MAX_BUDGET_MICROUSD, "cloud budget"),
            (max_runtime_seconds, 1, MAX_RUNTIME_SECONDS, "runtime"),
            (max_active, 1, 4, "active cloud jobs"),
            (poll_seconds, 15, 3600, "poll interval"),
            (cooldown_seconds, 15, 3600, "safe-operation cooldown")):
        cloud.io.safe.integer(value, low, high, label)
    deadline = clock() + max_runtime_seconds
    cycles = exhausted = 0

    def stopped():
        return stopping() or clock() >= deadline

    def factory(provider):
        client = client_factory(provider) if client_factory else cloud.client_for(provider, env_file)
        return RetryingClient(provider, client, deadline=deadline, stopping=stopping, on_event=on_event,
                              clock=clock, sleep=sleep, jitter=jitter)

    try:
        while not stopped():
            try:
                value = cloud.cycle(plan_ref, allow_paid_api=True, budget_microusd=budget_microusd,
                    max_new_jobs=max_active, max_active=max_active, env_file=env_file,
                    client_factory=factory, stopping=stopped)
            except SafeOperationExhausted as error:
                exhausted += 1
                metadata = error_metadata(error.error)
                delay = max(cooldown_seconds, metadata["retry_after_seconds"] or 0)
                on_event({"event": "safe_operation_cooldown", "provider": error.provider,
                          "operation": error.operation, "attempts": error.attempts,
                          "delay_seconds": min(delay, max(0, deadline - clock())),
                          **metadata, "paid_post_retried": False})
                _wait(delay, deadline=deadline, stopping=stopping, clock=clock, sleep=sleep)
                continue  # Only a dedicated, classified safe-operation error.
            cycles += 1
            on_event(value)
            if value["state"] != "running":
                if value["state"] == "paused" and clock() >= deadline and not stopping():
                    value = {**value, "state": "runtime_limit"}
                return {**value, "completed_cycles": cycles, "exhausted_safe_retry_windows": exhausted,
                        "automatic_paid_retries": False}
            _wait(poll_seconds, deadline=deadline, stopping=stopping, clock=clock, sleep=sleep)
    except RetryInterrupted as error:
        reason = error.reason
    else:
        reason = "paused" if stopping() else "runtime_limit"
    return {"state": reason, "completed_cycles": cycles, "exhausted_safe_retry_windows": exhausted,
            "automatic_paid_retries": False, "durable_paid_evidence_preserved": True}


def _emit(value):
    print(cloud.io.canonical(value).decode().strip(), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--execution-release", required=True)
    parser.add_argument("--execution-release-sha256", required=True)
    parser.add_argument("--runtime-path", required=True)
    parser.add_argument("--runner-sha256", required=True)
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--allow-paid-api", action="store_true")
    parser.add_argument("--budget-usd", type=cloud.usd, default=MAX_BUDGET_MICROUSD)
    parser.add_argument("--max-runtime-seconds", type=int, default=MAX_RUNTIME_SECONDS)
    parser.add_argument("--max-active", type=int, default=4)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--cooldown-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    try:
        runtime = cloud.io.safe.path_value(args.runtime_path)
        with cloud.io.paths.retained_directory(runtime):
            if Path.cwd() != runtime or Path(cloud.__file__).parent != runtime / "pipeline":
                raise ResilientError("runner must import the exact approved runtime from its own working directory")
        cloud.io.read_bytes({"path": str(Path(__file__).absolute()), "sha256": args.runner_sha256})
        reference = {"path": args.plan, "sha256": args.expected_sha256}
        execution_release = {"path": args.execution_release, "sha256": args.execution_release_sha256}
        with release.activate(execution_release), cloud.pause_signal() as stopping:
            result = run(reference, allow_paid_api=args.allow_paid_api, budget_microusd=args.budget_usd,
                         max_runtime_seconds=args.max_runtime_seconds, max_active=args.max_active,
                         env_file=args.env_file, poll_seconds=args.poll_seconds,
                         cooldown_seconds=args.cooldown_seconds, stopping=stopping, on_event=_emit)
        _emit(result)
        return 2 if result["state"] in {"needs_review", "reconciliation_required"} else 0
    except KeyboardInterrupt:
        _emit({"state": "paused", "durable_paid_evidence_preserved": True, "automatic_paid_retries": False})
        return 130
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        metadata = error_metadata(error) if isinstance(error, cloud.clients.CloudClientError) else {}
        _emit({"state": "stopped", "error_type": type(error).__name__, **metadata,
               "automatic_paid_retries": False, "durable_paid_evidence_preserved": True})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
