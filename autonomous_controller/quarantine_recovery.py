"""Explicit, bounded recovery of sealed acquisition quarantines.

The original work orders, three-attempt ledgers, quarantine receipts, result
identities, schedules and controller checkpoint are never rewritten. A separately
sealed plan admits only its exact quarantined jobs. Recovery holds both legacy
execution locks, invokes the existing guarded downloader, and seals a completion
proof before permitting the original campaign to restart.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
from typing import Any, Iterator

from . import acquisition_retry_proof as proof
from .config import ControllerConfig, load_config
from .gpu_child import ControllerUnitContext
from .longform_companion import load_registration_if_present, read_companion_status
from .sealed_backend import SealedArchiveBackend, _load_modules
from .state import (
    MAX_CONTROL_BYTES, _atomic_mutable_json, _open_lock, _read_control_path,
    read_control_state, utc_now,
)


PLAN_KIND = "himr_acquisition_quarantine_retry_plan"
MAX_ITEMS = 100
PER_ITEM_SECONDS = 4 * 60 * 60
REPOSITORY = Path(__file__).resolve().parents[1]


class RecoveryError(RuntimeError):
    """Recovery authority, exclusion, integrity, or a bound failed closed."""


class RecoveryStopped(RecoveryError):
    """An operator or time bound interrupted a resumable recovery."""


def _hash(value: Any) -> str:
    import hashlib

    return hashlib.sha256(proof.canonical_bytes(value)).hexdigest()


def _physical(path: Path, runner: Any, *, mode: int = 0o400) -> str:
    body, _ = runner._stable_read(
        path, maximum=4 * 1024 * 1024, label="recovery authority", required_mode=mode
    )
    return runner.sha256_bytes(body)


def _bundles(config: ControllerConfig, modules: Any) -> Iterator[tuple[Any, ...]]:
    """Read small sealed configuration/bundle documents, never media."""
    runner = modules.queue_runner
    for reference in config.section("campaign")["schedules"]:
        schedule, _path, body = modules.background.load_schedule(Path(reference["path"]))
        if (runner.sha256_bytes(body) != reference["sha256"]
                or schedule["schedule_id"] != reference["schedule_id"]):
            raise RecoveryError("schedule differs from the sealed campaign")
        bundle = runner._load_bundle(Path(schedule["queue"]["manifest_path"]))
        if (runner.sha256_bytes(bundle["body"]) != schedule["queue"]["manifest_sha256"]
                or bundle["manifest"]["bundle_id"] != schedule["queue"]["bundle_id"]):
            raise RecoveryError("bundle differs from the sealed schedule")
        yield reference, schedule, bundle


def _entry(bundle: dict[str, Any], entry: dict[str, Any], order: dict[str, Any],
           quarantine: dict[str, Any], runner: Any) -> dict[str, Any]:
    if order["adapter"] != "direct_http" or order["source"]["access_state"] != "public":
        raise RecoveryError("recovery is restricted to exact public HTTP work orders")
    return {
        "manifest_path": str(bundle["path"]),
        "manifest_sha256": runner.sha256_bytes(bundle["body"]),
        "bundle_id": bundle["manifest"]["bundle_id"],
        "ordinal": entry["queue_ordinal"],
        "job_id": order["job_id"],
        "work_order_sha256": runner.sha256_bytes(runner.canonical_bytes(order)),
        "quarantine_receipt_sha256": quarantine["receipt_sha256"],
        "result_path": str(runner._result_path(order)),
        "expected_bytes": order["adapter_config"]["expected_byte_count"],
        "url": order["adapter_config"]["url"],
    }


def build_retry_plan(config: ControllerConfig, expected_count: int, *, modules: Any = None) -> dict[str, Any]:
    if type(expected_count) is not int or not 1 <= expected_count <= MAX_ITEMS:
        raise RecoveryError("expected quarantine count must be between 1 and 100")
    modules = modules or _load_modules()
    runner = modules.queue_runner
    entries = []
    for _reference, _schedule, bundle in _bundles(config, modules):
        failures = runner._scan_failure_states(bundle)
        for entry, order, failure in zip(bundle["manifest"]["work_orders"], bundle["orders"], failures, strict=True):
            if failure["quarantine"] is None:
                continue
            result_path = runner._result_path(order)
            if result_path.exists() or result_path.is_symlink():
                raise RecoveryError("quarantined result already exists; resume its original recovery plan")
            entries.append(_entry(bundle, entry, order, failure["quarantine"], runner))
    if len(entries) != expected_count:
        raise RecoveryError(f"expected {expected_count} quarantines, observed {len(entries)}")
    if len({entry["job_id"] for entry in entries}) != len(entries):
        raise RecoveryError("quarantines repeat an original job identity")
    core = {
        "schema_version": 1, "kind": PLAN_KIND,
        "config_path": str(config.path), "config_sha256": config.physical_sha256,
        "config_id": config.config_id, "created_at": utc_now(), "entries": entries,
        "limits": {"max_attempts_per_item": 3, "retry_backoff_seconds": 120,
                   "max_run_seconds": 86400},
    }
    return {**core, "plan_sha256": _hash(core)}


def _contexts(plan: dict[str, Any], config: ControllerConfig, modules: Any) -> list[tuple[Any, ...]]:
    by_manifest = {entry["manifest_path"]: [] for entry in plan["entries"]}
    for item in plan["entries"]:
        by_manifest[item["manifest_path"]].append(item)
    contexts = {}
    runner = modules.queue_runner
    for reference, schedule, bundle in _bundles(config, modules):
        wanted = by_manifest.get(str(bundle["path"]))
        if wanted is None:
            continue
        for item in wanted:
            index = item["ordinal"] - 1
            if not 0 <= index < len(bundle["orders"]):
                raise RecoveryError("retry ordinal is outside its bundle")
            entry, order = bundle["manifest"]["work_orders"][index], bundle["orders"][index]
            failure = runner._inspect_failure_state(bundle, entry, order)
            quarantine = failure["quarantine"]
            if quarantine is None or _entry(bundle, entry, order, quarantine, runner) != item:
                raise RecoveryError("retry target differs from its original quarantine authority")
            contexts[item["job_id"]] = (reference, schedule, bundle, entry, order, quarantine)
    if set(contexts) != {entry["job_id"] for entry in plan["entries"]}:
        raise RecoveryError("retry plan contains work outside the sealed campaign")
    return [contexts[entry["job_id"]] for entry in plan["entries"]]


def _guards(config: ControllerConfig) -> list[dict[str, str]]:
    registration = load_registration_if_present(config.path, config.physical_sha256)
    if registration is None:
        raise RecoveryError("recovery requires the registered long-form companion")
    read_companion_status(registration)
    state_root = Path(config.document["state_root"])
    return [{
        "control_path": str(state_root / "control.json"),
        "status_path": str(state_root / "status.json"),
        "controller_lock": str(state_root / "controller.lock"),
        "companion_status_path": str(registration.status_path),
        "companion_lock": str(registration.status_path.parent / "dispatch.lock"),
    }]


def _receipt(path: Path, core: dict[str, Any], runner: Any) -> dict[str, Any]:
    value = {**core, "receipt_sha256": _hash(core)}
    runner._write_immutable_receipt(path, value, "quarantine recovery attempt")
    return value


def _read_receipt(path: Path, runner: Any) -> dict[str, Any]:
    body, _ = runner._stable_read(path, maximum=64 * 1024, label="recovery attempt", required_mode=0o400)
    value = runner._strict_json(body, "recovery attempt")
    if (not isinstance(value, dict) or body != proof.pretty_bytes(value)
            or value.get("receipt_sha256") != _hash({k: v for k, v in value.items() if k != "receipt_sha256"})):
        raise RecoveryError("recovery attempt receipt is not canonical and hash-bound")
    return value


def _attempts(root: Path, item: dict[str, Any], plan_sha: str, limit: int, runner: Any) -> list[dict[str, Any]]:
    if not root.exists() and not root.is_symlink():
        return []
    runner._safe_directory(root, "recovery attempts", required_mode=0o700)
    names = runner._directory_names(root, "recovery attempts")
    if not names:
        # A crash after mkdir but before the first immutable start admitted no
        # attempt and must not manufacture one on restart.
        return []
    starts = sorted(name for name in names if re.fullmatch(r"[0-9]{6}\.started\.json", name))
    if (not 1 <= len(starts) <= limit
            or starts != [f"{number:06d}.started.json" for number in range(1, len(starts) + 1)]
            or not names <= set(starts) | {name.replace(".started.", ".finished.") for name in starts}):
        raise RecoveryError("recovery attempts are not a bounded contiguous ledger")
    observed = []
    for number, name in enumerate(starts, 1):
        start = _read_receipt(root / name, runner)
        expected = {"schema_version", "kind", "authorization_plan_sha256", "job_id", "result_path",
                    "attempt_number", "started_at", "receipt_sha256"}
        if (set(start) != expected or start["schema_version"] != 1
                or start["kind"] != "himr_quarantine_retry_attempt_started"
                or start["authorization_plan_sha256"] != plan_sha
                or start["job_id"] != item["job_id"] or start["result_path"] != item["result_path"]
                or start["attempt_number"] != number):
            raise RecoveryError("recovery attempt belongs to different authority")
        runner.materialize_queue.validate_utc_timestamp(start["started_at"], "retry attempt start")
        finish_path = root / name.replace(".started.", ".finished.")
        finish = None
        if finish_path.exists() or finish_path.is_symlink():
            finish = _read_receipt(finish_path, runner)
            if (set(finish) != {"schema_version", "kind", "started_receipt_sha256", "finished_at", "status", "error", "receipt_sha256"}
                    or finish["schema_version"] != 1 or finish["kind"] != "himr_quarantine_retry_attempt_finished"
                    or finish["started_receipt_sha256"] != start["receipt_sha256"]
                    or finish["status"] not in {"completed", "transient_failure", "fatal_failure", "interrupted"}
                    or not (finish["error"] is None or isinstance(finish["error"], str))):
                raise RecoveryError("recovery attempt finish differs from its start")
            runner.materialize_queue.validate_utc_timestamp(finish["finished_at"], "retry attempt finish")
        observed.append({"start": start, "finish": finish})
    if any(row["finish"] is None for row in observed[:-1]):
        raise RecoveryError("a non-final recovery attempt has no finish receipt")
    return observed


def _finish(root: Path, start: dict[str, Any], status: str, error: str | None, runner: Any) -> None:
    _receipt(root / f"{start['attempt_number']:06d}.finished.json", {
        "schema_version": 1, "kind": "himr_quarantine_retry_attempt_finished",
        "started_receipt_sha256": start["receipt_sha256"], "finished_at": utc_now(),
        "status": status, "error": None if error is None else error.replace("\x00", "�")[:4096],
    }, runner)


def _transient(error: Exception) -> bool:
    message = str(error)
    if re.fullmatch(r"HTTP request failed with status (408|429|500|502|503|504)", message):
        return True
    if re.fullmatch(r"incomplete HTTP body: expected (None|[0-9]+) bytes, staged [0-9]+", message):
        return True
    return message.startswith(("HTTP request failed:", "HTTP body transfer failed:")) and any(
        token in message.lower() for token in (
            "temporary failure in name resolution", "name or service not known",
            "no address associated with hostname", "timed out", "connection reset",
            "connection refused", "network is unreachable", "remote end closed connection",
        )
    )


def _status(root: Path, plan: dict[str, Any], physical_sha: str, **fields: Any) -> dict[str, Any]:
    value = {"schema_version": 1, "kind": "himr_quarantine_recovery_status",
             "authorization_plan_sha256": physical_sha, "config_id": plan["config_id"],
             "updated_at": utc_now(), "pid": os.getpid(), "total_items": len(plan["entries"]), **fields}
    _atomic_mutable_json(root / "status.json", value, maximum=1024 * 1024)
    return value


@contextmanager
def _signals() -> Iterator[None]:
    def stop(signum: int, _frame: Any) -> None:
        raise RecoveryStopped(f"recovery interrupted by signal {signum}")
    previous = {number: signal.signal(number, stop) for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def _backoff(seconds: int, held: Any, deadline: float) -> None:
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        held.check_unchanged()
        if time.monotonic() >= deadline:
            raise RecoveryStopped("recovery wall-clock limit reached")
        time.sleep(max(0.0, min(1.0, until - time.monotonic())))


def _set_control_if_unchanged(config: ControllerConfig, expected: dict[str, Any], desired: str) -> dict[str, Any]:
    """Compare-and-set intent so even a second Stop cancels automatic resume."""
    if desired not in {"running", "stopped"}:
        raise RecoveryError("invalid recovery handoff intent")
    descriptor = _open_lock(config.state_root / "control.lock", "recovery control handoff")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        current = _read_control_path(config, config.state_root / "control.json")
        if current != expected or current["generation"] >= 2**63 - 1:
            raise RecoveryError("operator intent changed; automatic recovery handoff refused")
        next_control = {**current, "generation": current["generation"] + 1,
                        "desired_state": desired, "requested_at": utc_now()}
        _atomic_mutable_json(config.state_root / "control.json", next_control, maximum=MAX_CONTROL_BYTES)
        return next_control
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _inspect_bounded(order: dict[str, Any], runner: Any, deadline: float) -> dict[str, Any] | None:
    try:
        with runner._hard_deadline(deadline - time.monotonic()):
            return runner._inspect_result(order)
    except runner.QueueDeadlineError as error:
        raise RecoveryStopped(str(error)) from error


def _dispatch_bounded(order: dict[str, Any], runner: Any, seconds: float) -> dict[str, Any]:
    # The legacy _dispatch_one times only the downloader. Extend this explicit
    # recovery boundary over its exact post-download payload verification too.
    try:
        with runner._hard_deadline(seconds):
            returned = runner.acquire.run_acquisition(order, dry_run=False)
            state = runner._inspect_result(order)
            if state is None:
                raise RecoveryError("acquisition returned without a completed result")
            runner._validate_adapter_return(returned, state)
            return state
    except runner.QueueDeadlineError as error:
        raise RecoveryStopped(str(error)) from error


def run_plan(path: Path, expected_sha: str, *, resume_campaign: bool = False) -> dict[str, Any]:
    # A separate per-plan mutex also protects status reporting. A losing second
    # recovery invocation cannot overwrite the active invocation's status.
    proof.validate_plan(path, expected_sha)
    descriptor = _open_lock(path.parent / "recovery.lock", "quarantine recovery run lock")
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RecoveryError("this recovery plan is already running") from error
        return _run_plan_exclusive(path, expected_sha, resume_campaign=resume_campaign)
    finally:
        os.close(descriptor)


def _run_plan_exclusive(path: Path, expected_sha: str, *, resume_campaign: bool = False) -> dict[str, Any]:
    """Execute only this plan, with legacy exclusion and no corpus-wide scan."""
    from pipeline.hybrid_legacy_guard import hold_legacy

    plan = proof.validate_plan(path, expected_sha)
    config = load_config(Path(plan["config_path"]), plan["config_sha256"])
    if resume_campaign:
        # Fail before any acquisition if this invocation cannot safely become
        # the original GPU-admitted campaign supervisor.
        ControllerUnitContext.from_current_systemd_unit()
    modules = _load_modules()
    runner = modules.queue_runner
    root = path.parent
    runner._safe_directory(root, "recovery plan directory", required_mode=0o700)
    contexts = _contexts(plan, config, modules)
    backend = SealedArchiveBackend(config, modules=modules)
    runtimes = [(ref, schedule, bundle, [], {}) for ref, schedule, bundle, *_rest in contexts]
    backend._validate_cold_storage_identity(runtimes)
    software = runner._software_document()
    completed: list[str] = []
    failed: list[str] = []
    current = None
    armed_control = None
    deadline = time.monotonic() + plan["limits"]["max_run_seconds"]
    try:
        with _signals(), hold_legacy(_guards(config)) as held:
            stopped_control = read_control_state(config)
            if stopped_control["desired_state"] != "stopped":
                raise RecoveryError("recovery requires stopped campaign intent")
            for item, context in zip(plan["entries"], contexts, strict=True):
                _ref, _schedule, bundle, entry, order, quarantine = context
                current = item["job_id"]
                attempt_root = root / "attempts" / current
                held.check_unchanged()
                backend._validate_cold_storage_identity(runtimes)
                if proof.validate_plan(path, expected_sha) != plan:
                    raise RecoveryError("recovery plan changed during execution")
                bundle = runner._replay_bundle(bundle, software)
                if runner._inspect_failure_state(bundle, entry, order)["quarantine"] != quarantine:
                    raise RecoveryError("original quarantine changed during recovery")
                attempts = _attempts(attempt_root, item, expected_sha, plan["limits"]["max_attempts_per_item"], runner)
                _status(root, plan, expected_sha, lifecycle="verifying", current_job=current,
                        completed_items=len(completed), failed_items=len(failed))
                state = _inspect_bounded(order, runner, deadline)
                if state is not None:
                    if not attempts:
                        raise RecoveryError("completed quarantined result lacks a recovery attempt start")
                    proof.seal_completion(bundle, entry, order, quarantine, path, expected_sha, state)
                    if attempts[-1]["finish"] is None:
                        _finish(attempt_root, attempts[-1]["start"], "completed", None, runner)
                    completed.append(current)
                    continue
                if proof.completion_path(bundle, entry, order).exists():
                    raise RecoveryError("recovery proof exists without its completed result")
                if attempts and attempts[-1]["finish"] is None:
                    _finish(attempt_root, attempts[-1]["start"], "interrupted", "prior attempt ended without a durable result", runner)
                if any(row["finish"] is not None and row["finish"]["status"] in {"completed", "fatal_failure"} for row in attempts):
                    raise RecoveryError("previous terminal retry requires explicit review")
                for number in range(len(attempts) + 1, plan["limits"]["max_attempts_per_item"] + 1):
                    if number > 1:
                        _status(root, plan, expected_sha, lifecycle="backoff", current_job=current,
                                completed_items=len(completed), failed_items=len(failed), attempt=number)
                        _backoff(plan["limits"]["retry_backoff_seconds"], held, deadline)
                    held.check_unchanged()
                    remaining = min(PER_ITEM_SECONDS, deadline - time.monotonic())
                    if remaining <= 0:
                        raise RecoveryStopped("recovery wall-clock limit reached")
                    if not runner._capacity_allows(order, free_space_floor_bytes=order["limits"]["free_space_floor_bytes"]):
                        raise RecoveryStopped("sealed free-space floor prevents recovery")
                    bundle = runner._replay_bundle(bundle, software)
                    backend._validate_cold_storage_identity(runtimes)
                    if time.monotonic() >= deadline:
                        raise RecoveryStopped("recovery wall-clock limit reached during preflight")
                    start = _receipt(attempt_root / f"{number:06d}.started.json", {
                        "schema_version": 1, "kind": "himr_quarantine_retry_attempt_started",
                        "authorization_plan_sha256": expected_sha, "job_id": current,
                        "result_path": item["result_path"], "attempt_number": number, "started_at": utc_now(),
                    }, runner)
                    _status(root, plan, expected_sha, lifecycle="downloading", current_job=current,
                            completed_items=len(completed), failed_items=len(failed), attempt=number)
                    try:
                        state = _dispatch_bounded(order, runner, min(PER_ITEM_SECONDS, deadline - time.monotonic()))
                    except runner.acquire.AcquisitionError as error:
                        # A valid result published immediately before an exception
                        # must be reconciled before another invocation is allowed.
                        state = _inspect_bounded(order, runner, deadline)
                        if state is None:
                            transient = _transient(error)
                            _finish(attempt_root, start, "transient_failure" if transient else "fatal_failure", str(error), runner)
                            if not transient:
                                raise RecoveryError(f"non-transient acquisition failure: {error}") from error
                            continue
                    held.check_unchanged()
                    backend._validate_cold_storage_identity(runtimes)
                    bundle = runner._replay_bundle(bundle, software)
                    proof.seal_completion(bundle, entry, order, quarantine, path, expected_sha, state)
                    _finish(attempt_root, start, "completed", None, runner)
                    completed.append(current)
                    break
                else:
                    failed.append(current)
                _status(root, plan, expected_sha, lifecycle="recovering", current_job=None,
                        completed_items=len(completed), failed_items=len(failed))
            held.check_unchanged()
            result = _status(root, plan, expected_sha, lifecycle="recovered", current_job=None,
                             completed_items=len(completed), failed_items=len(failed),
                             completed_jobs=completed, failed_jobs=failed, campaign_resumed=False)
        if resume_campaign:
            # Releasing the two execution leases precedes starting the original
            # supervisor. The supervisor must become this service's MainPID.
            armed_control = _set_control_if_unchanged(config, stopped_control, "running")
            _status(root, plan, expected_sha, lifecycle="resuming_campaign", current_job=None,
                    completed_items=len(completed), failed_items=len(failed),
                    completed_jobs=completed, failed_jobs=failed, campaign_resumed=False)
            wrapper = REPOSITORY / "autonomous_controller/bin/himr-autonomous-controller"
            os.execv(str(wrapper), [str(wrapper), "run", "--config", str(config.path),
                                   "--expected-config-sha256", config.physical_sha256])
        return result
    except Exception as error:
        if armed_control is not None:
            try:
                _set_control_if_unchanged(config, armed_control, "stopped")
            except Exception:
                # A newer operator request owns the control document; do not
                # overwrite it while handling an exec or status-write failure.
                pass
        _status(root, plan, expected_sha, lifecycle="stopped" if isinstance(error, RecoveryStopped) else "faulted",
                current_job=current, completed_items=len(completed), failed_items=len(failed),
                error={"type": type(error).__name__, "message": str(error)[:4096]}, campaign_resumed=False)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan_cmd = commands.add_parser("plan", help="seal an exact metadata-only quarantine retry plan")
    plan_cmd.add_argument("--config", required=True, type=Path)
    plan_cmd.add_argument("--expected-config-sha256", required=True)
    plan_cmd.add_argument("--expected-quarantined-count", required=True, type=int)
    plan_cmd.add_argument("--output", required=True, type=Path)
    for name in ("run", "status", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--plan", required=True, type=Path)
        command.add_argument("--expected-plan-sha256", required=True)
        if name == "run":
            command.add_argument("--resume-campaign", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            config = load_config(args.config, args.expected_config_sha256)
            modules = _load_modules()
            plan = build_retry_plan(config, args.expected_quarantined_count, modules=modules)
            if args.output.name != "plan.json" or not args.output.is_absolute():
                raise RecoveryError("plan output must be an absolute plan.json path")
            modules.queue_runner._write_immutable_receipt(args.output, plan, "quarantine retry authorization")
            physical = _physical(args.output, modules.queue_runner)
            proof.validate_plan(args.output, physical)
            result = {"status": "planned", "plan_path": str(args.output), "plan_sha256": physical,
                      "items": len(plan["entries"]), "expected_bytes": sum(entry["expected_bytes"] for entry in plan["entries"]),
                      "acquisition_started": False}
        elif args.command == "run":
            result = run_plan(args.plan, args.expected_plan_sha256, resume_campaign=args.resume_campaign)
        else:
            plan = proof.validate_plan(args.plan, args.expected_plan_sha256)
            if args.command == "validate":
                config = load_config(Path(plan["config_path"]), plan["config_sha256"])
                contexts = _contexts(plan, config, _load_modules())
                result = {"status": "validated", "items": len(contexts), "media_scanned": False}
            else:
                modules = _load_modules()
                body, _ = modules.queue_runner._stable_read(args.plan.parent / "status.json", maximum=1024 * 1024,
                                                             label="quarantine recovery status", required_mode=0o600)
                result = modules.queue_runner._strict_json(body, "quarantine recovery status")
                if result.get("authorization_plan_sha256") != args.expected_plan_sha256:
                    raise RecoveryError("status belongs to a different recovery plan")
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    except Exception as error:
        print(json.dumps({"status": "error", "error": {"type": type(error).__name__, "message": str(error)}}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
