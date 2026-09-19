"""Finite foreground supervisor for an explicitly registered ASR companion."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from .config import load_config
from .gpu_child import CONTROLLER_SUPERVISOR_PID_ENV
from .longform_companion import (
    LongformCompanionError,
    LongformCompanionRegistration,
    canonical_bytes,
    load_registration_if_present,
    read_companion_status,
)
from .state import request_stop, utc_now


# The outer operator console has an 8 MiB total stream cap.  Two independently
# parsed child results must still fit after they are wrapped in one object.
MAX_CHILD_STREAM_BYTES = 3 * 1024 * 1024
FORCED_TERM_GRACE_SECONDS = 30.0


class CompanionSupervisorError(RuntimeError):
    """The registered process pair could not be launched or replayed safely."""


@dataclass
class _Child:
    name: str
    process: subprocess.Popen[bytes]
    stdout: Any
    stderr: Any
    result: dict[str, Any] | None = None
    result_error: str | None = None


def _strict_result(body: bytes, label: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CompanionSupervisorError(f"{label} repeats key {key!r}")
            result[key] = value
        return result

    try:
        value = json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                CompanionSupervisorError(
                    f"{label} contains non-finite number {token}"
                )
            ),
        )
    except CompanionSupervisorError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise CompanionSupervisorError(f"{label} is not strict JSON: {error}") from error
    if not isinstance(value, dict):
        raise CompanionSupervisorError(f"{label} top level is not an object")
    return value


def _capture_body(stream: Any, label: str) -> bytes:
    size = os.fstat(stream.fileno()).st_size
    if size > MAX_CHILD_STREAM_BYTES:
        raise CompanionSupervisorError(f"{label} exceeds the 3 MiB capture limit")
    stream.flush()
    stream.seek(0)
    body = stream.read(MAX_CHILD_STREAM_BYTES + 1)
    if len(body) > MAX_CHILD_STREAM_BYTES:
        raise CompanionSupervisorError(f"{label} exceeds the 3 MiB capture limit")
    return body


def _replay_child(child: _Child) -> None:
    returncode = child.process.poll()
    if returncode is None or child.result is not None or child.result_error is not None:
        return
    try:
        stdout = _capture_body(child.stdout, f"{child.name} stdout")
        stderr = _capture_body(child.stderr, f"{child.name} stderr")
        if child.name == "source_controller":
            selected, unused = (stdout, stderr) if returncode == 0 else (stderr, stdout)
        else:
            # The long-form campaign intentionally returns both success and its
            # strict failure envelope on stdout.
            selected, unused = stdout, stderr
        if unused.strip():
            raise CompanionSupervisorError(
                f"{child.name} emitted unexpected output on its secondary stream"
            )
        if not selected.strip():
            if returncode < 0:
                raise CompanionSupervisorError(
                    f"{child.name} was terminated by signal {-returncode} "
                    "before emitting one result object"
                )
            raise CompanionSupervisorError(
                f"{child.name} exited with status {returncode} "
                "without emitting one result object"
            )
        child.result = _strict_result(selected, f"{child.name} result")
    except Exception as error:
        child.result_error = f"{type(error).__name__}: {error}"


def _child_failure_detail(child: _Child) -> str:
    """Prefer a parsed child's typed failure over a generic envelope label."""

    if child.result_error is not None:
        return child.result_error
    if isinstance(child.result, dict):
        error = child.result.get("error")
        if isinstance(error, dict):
            error_type = error.get("type")
            message = error.get("message")
            if (
                isinstance(error_type, str)
                and error_type
                and isinstance(message, str)
                and message
            ):
                return f"{error_type}: {message}"[:2048]
    return "strict failure result"


def _child_argvs(
    controller_config_path: Path,
    expected_controller_sha256: str,
    registration: LongformCompanionRegistration,
) -> tuple[list[str], list[str]]:
    # Invoke the legacy module directly.  Calling the public wrapper here would
    # rediscover the same registration and recursively create supervisors.
    controller_argv = [
        sys.executable,
        "-B",
        "-m",
        "autonomous_controller.cli",
        "run",
        "--config",
        str(controller_config_path),
        "--expected-config-sha256",
        expected_controller_sha256,
    ]
    companion_argv = [
        str(registration.entrypoint_path),
        "run",
        "--config",
        str(registration.campaign_config_path),
        "--expected-config-sha256",
        registration.campaign_config_sha256,
    ]
    return controller_argv, companion_argv


def _spawn(
    name: str,
    argv: Sequence[str],
    repository_root: Path,
    *,
    controller_supervisor_pid: int | None = None,
) -> _Child:
    stdout = tempfile.TemporaryFile(mode="w+b")
    stderr = tempfile.TemporaryFile(mode="w+b")
    try:
        child_environment = dict(os.environ)
        child_environment.pop(CONTROLLER_SUPERVISOR_PID_ENV, None)
        if controller_supervisor_pid is not None:
            if name != "source_controller":
                raise CompanionSupervisorError(
                    "controller supervisor delegation is restricted to the source controller"
                )
            if (
                isinstance(controller_supervisor_pid, bool)
                or not isinstance(controller_supervisor_pid, int)
                or not 1 < controller_supervisor_pid <= 2**31 - 1
            ):
                raise CompanionSupervisorError(
                    "controller supervisor delegation PID is invalid"
                )
            child_environment[CONTROLLER_SUPERVISOR_PID_ENV] = str(
                controller_supervisor_pid
            )
        process = subprocess.Popen(
            list(argv),
            cwd=repository_root,
            env=child_environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            shell=False,
            close_fds=True,
            start_new_session=True,
        )
    except Exception:
        stdout.close()
        stderr.close()
        raise
    return _Child(name=name, process=process, stdout=stdout, stderr=stderr)


def _signal_group(child: _Child, signum: int) -> None:
    if child.process.poll() is not None:
        return
    try:
        os.killpg(child.process.pid, signum)
    except ProcessLookupError:
        return


def _emergency_quiesce(
    children: Sequence[_Child], controller_config: Any
) -> None:
    """Best-effort bounded cleanup for an exception outside the normal loop.

    This is intentionally process-group based because the long-form child may be
    waiting on FFmpeg or a CUDA runner.  Their durable outputs are atomic and
    resumable; leaving such a descendant detached from the failed outer Start job
    would be the less safe outcome.
    """

    with contextlib.suppress(Exception):
        request_stop(controller_config, requested_at=utc_now())
    for child in children:
        with contextlib.suppress(Exception):
            _signal_group(child, signal.SIGTERM)
    deadline = time.monotonic() + FORCED_TERM_GRACE_SECONDS
    while any(child.process.poll() is None for child in children):
        if time.monotonic() >= deadline:
            break
        time.sleep(0.05)
    for child in children:
        if child.process.poll() is None:
            with contextlib.suppress(Exception):
                _signal_group(child, signal.SIGKILL)
    for child in children:
        with contextlib.suppress(Exception):
            child.process.wait(timeout=5.0)


def supervise_registered_campaign(
    *,
    controller_config_path: Path,
    expected_controller_sha256: str,
    registration: LongformCompanionRegistration,
) -> tuple[int, dict[str, Any]]:
    """Run the source and companion as one strict foreground operation."""

    controller_config = load_config(
        controller_config_path, expected_controller_sha256
    )
    # Validate the status projection before either process is admitted.  This
    # catches a stale or cross-campaign registration during the safe Start window.
    read_companion_status(registration)
    repository_root = Path(__file__).resolve().parents[1]
    controller_argv, companion_argv = _child_argvs(
        controller_config_path, expected_controller_sha256, registration
    )
    children: list[_Child] = []
    stop_requested = False
    stop_reason: str | None = None
    stop_started: float | None = None
    term_sent: float | None = None
    interrupted_by: int | None = None
    supervision_completed = False

    def observe_signal(signum: int, _frame: Any) -> None:
        nonlocal interrupted_by
        interrupted_by = signum

    prior_term = signal.getsignal(signal.SIGTERM)
    prior_int = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGTERM, observe_signal)
    signal.signal(signal.SIGINT, observe_signal)
    try:
        try:
            children.append(
                _spawn(
                    "source_controller",
                    controller_argv,
                    repository_root,
                    controller_supervisor_pid=os.getpid(),
                )
            )
            children.append(_spawn("longform_companion", companion_argv, repository_root))
        except Exception as error:
            stop_reason = f"child launch failed: {type(error).__name__}: {error}"

        if stop_reason is not None and not children:
            # Start intent is durable before the operator launches this process.
            # A failure to admit even the source child must not leave that intent
            # armed for an unrelated later invocation.
            request_stop(controller_config, requested_at=utc_now())
            stop_requested = True
            stop_started = time.monotonic()

        poll_seconds = (
            registration.document["supervision"]["poll_interval_milliseconds"] / 1000.0
        )
        graceful_seconds = registration.document["supervision"][
            "graceful_stop_timeout_seconds"
        ]
        while any(child.process.poll() is None for child in children):
            for child in children:
                if os.fstat(child.stdout.fileno()).st_size > MAX_CHILD_STREAM_BYTES:
                    stop_reason = stop_reason or f"{child.name} stdout exceeded its capture limit"
                if os.fstat(child.stderr.fileno()).st_size > MAX_CHILD_STREAM_BYTES:
                    stop_reason = stop_reason or f"{child.name} stderr exceeded its capture limit"
                _replay_child(child)
                returncode = child.process.poll()
                if returncode is not None and (
                    returncode != 0 or child.result_error is not None
                ):
                    stop_reason = stop_reason or (
                        f"{child.name} failed with status {returncode}: "
                        f"{_child_failure_detail(child)}"
                    )
            if interrupted_by is not None:
                stop_reason = stop_reason or f"supervisor received signal {interrupted_by}"

            # The pair is one finite operation.  Once either side exits, durably
            # stop the source so the surviving side converges at its next safe
            # boundary, even when the first exit was successful.
            if any(child.process.poll() is not None for child in children):
                stop_reason = stop_reason or "one supervised child exited"
            if stop_reason is not None and not stop_requested:
                request_stop(controller_config, requested_at=utc_now())
                stop_requested = True
                stop_started = time.monotonic()
            if (
                stop_started is not None
                and time.monotonic() - stop_started >= graceful_seconds
                and term_sent is None
            ):
                for child in children:
                    _signal_group(child, signal.SIGTERM)
                term_sent = time.monotonic()
            if (
                term_sent is not None
                and time.monotonic() - term_sent >= FORCED_TERM_GRACE_SECONDS
            ):
                for child in children:
                    _signal_group(child, signal.SIGKILL)
            time.sleep(poll_seconds)

        for child in children:
            child.process.wait()
            _replay_child(child)

        # A launch failure can leave zero or one child.  It is still returned as
        # one bounded supervisor failure object after any admitted child quiesces.
        complete_pair = len(children) == 2
        success = complete_pair and all(
            child.process.returncode == 0
            and child.result is not None
            and child.result_error is None
            for child in children
        )
        result = {
            "status": (
                "supervised_campaign_exited" if success else "supervised_campaign_faulted"
            ),
            "source_config_id": controller_config.config_id,
            "registration_identity_sha256": registration.document["identity_sha256"],
            "registration_sha256": registration.physical_sha256,
            "stop_coordinated": stop_requested,
            "stop_reason": stop_reason,
            "children": {
                child.name: {
                    "returncode": child.process.returncode,
                    "result": child.result,
                    "result_error": child.result_error,
                }
                for child in children
            },
        }
        if not success:
            failed_children = [
                child
                for child in children
                if child.process.returncode != 0 or child.result_error is not None
            ]
            failure_details = [
                f"{child.name} failed with status {child.process.returncode}: "
                f"{_child_failure_detail(child)}"
                for child in failed_children
            ]
            failure_message = stop_reason
            if (
                failure_message in {None, "one supervised child exited"}
                and failure_details
            ):
                failure_message = "; ".join(failure_details)
            result["error"] = {
                "type": "CompanionSupervisorError",
                "message": failure_message
                or (
                    "supervised child failure: "
                    + ", ".join(child.name for child in failed_children)
                    if failed_children
                    else "both supervised children were not admitted"
                ),
            }
        supervision_completed = True
        return (0 if success else 2), result
    finally:
        if not supervision_completed and any(
            child.process.poll() is None for child in children
        ):
            _emergency_quiesce(children, controller_config)
        signal.signal(signal.SIGTERM, prior_term)
        signal.signal(signal.SIGINT, prior_int)
        for child in children:
            child.stdout.close()
            child.stderr.close()


def maybe_supervise_run(
    argv: Sequence[str],
) -> tuple[int, dict[str, Any]] | None:
    """Return ``None`` for the exact legacy path, otherwise supervise ``run``."""

    # The public wrapper parses with the canonical CLI parser first, so these
    # fields are structurally known.  Keep this helper small and deterministic.
    if not argv or argv[0] != "run":
        return None
    from .cli import build_parser

    parsed = build_parser().parse_args(list(argv))
    registration = load_registration_if_present(
        parsed.config, parsed.expected_config_sha256
    )
    if registration is None:
        return None
    return supervise_registered_campaign(
        controller_config_path=parsed.config,
        expected_controller_sha256=parsed.expected_config_sha256,
        registration=registration,
    )


def emit_supervisor_result(returncode: int, value: dict[str, Any]) -> None:
    stream = sys.stdout.buffer if returncode == 0 else sys.stderr.buffer
    stream.write(canonical_bytes(value))


def emit_supervisor_error(error: Exception) -> int:
    value = {
        "status": "supervised_campaign_faulted",
        "error": {"type": type(error).__name__, "message": str(error)},
    }
    sys.stderr.buffer.write(canonical_bytes(value))
    return 2


__all__ = [
    "CompanionSupervisorError",
    "LongformCompanionError",
    "emit_supervisor_error",
    "emit_supervisor_result",
    "maybe_supervise_run",
    "supervise_registered_campaign",
]
