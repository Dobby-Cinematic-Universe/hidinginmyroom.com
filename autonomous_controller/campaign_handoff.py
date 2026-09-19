"""Explicit, one-shot handoff between two independently sealed campaigns.

This is not a scheduler and does not repair, stop, or reconfigure the predecessor.
It must start while the exact predecessor supervisor is alive, in a separate
admitted operator service. Its only write is a compare-and-set Start of the
successor after complete, successful termination of the predecessor pair.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from copy import deepcopy
import fcntl
import json
import math
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
from typing import Any, Iterator

from pipeline import hybrid_legacy_guard as legacy

from .config import ControllerConfig, load_config
from .gpu_child import (
    CONTROLLER_SUPERVISOR_PID_ENV, ControllerUnitContext, INVOCATION_ID_RE,
    OUTER_UNIT_RE, SYSTEMCTL, _systemd_control_environment, _validate_system_tools,
)
from .longform_companion import load_registration_if_present, read_companion_status
from .quarantine_recovery import _set_control_if_unchanged
from .state import read_control_state


REPOSITORY = Path(__file__).resolve().parents[1]
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
UNIT_PROPERTIES = {
    "Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlPID",
    "Result", "ExecMainCode", "ExecMainStatus", "InvocationID", "ControlGroup",
    "StandardOutput",
}


class HandoffError(RuntimeError):
    """Completion, identity, or exclusion could not be established."""


class HandoffCancelled(HandoffError):
    """A changed operator intent or bounded wait cancelled this one-shot job."""


def _integer(value: Any, minimum: int = 0, maximum: int = 2**63 - 1) -> bool:
    return type(value) is int and minimum <= value <= maximum


def _unit_state(unit: str, invocation: str) -> dict[str, str]:
    if (not isinstance(unit, str) or OUTER_UNIT_RE.fullmatch(unit) is None
            or not isinstance(invocation, str) or INVOCATION_ID_RE.fullmatch(invocation) is None):
        raise HandoffError("predecessor unit identity is invalid")
    _validate_system_tools()
    result = subprocess.run(
        [SYSTEMCTL, "--user", "--no-pager", "--no-ask-password", "show",
         "--property=" + ",".join(sorted(UNIT_PROPERTIES)), unit],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=10, check=False, close_fds=True, cwd="/",
        env=_systemd_control_environment(),
    )
    if result.returncode != 0 or len(result.stdout) > 65536 or len(result.stderr) > 65536:
        raise HandoffError("cannot inspect predecessor service")
    state: dict[str, str] = {}
    try:
        for line in result.stdout.decode("utf-8", errors="strict").splitlines():
            key, value = line.split("=", 1)
            if key in state or key not in UNIT_PROPERTIES or any(ord(c) < 32 for c in value):
                raise ValueError("invalid property")
            state[key] = value
    except (UnicodeError, ValueError) as error:
        raise HandoffError("invalid predecessor service properties") from error
    if (set(state) != UNIT_PROPERTIES or state["Id"] != unit
            or state["InvocationID"] != invocation or state["LoadState"] != "loaded"
            or state["StandardOutput"] != "append"):
        raise HandoffError("predecessor service identity or output mode changed")
    for key in ("MainPID", "ControlPID", "ExecMainCode", "ExecMainStatus"):
        value = state[key]
        if not value.isascii() or not value.isdigit() or str(int(value)) != value or int(value) > 2**31 - 1:
            raise HandoffError("invalid predecessor service process metadata")
    return state


def _cgroup_path(value: str) -> Path:
    prefix = f"/user.slice/user-{os.geteuid()}.slice/user@{os.geteuid()}.service/"
    if (not value.startswith(prefix) or os.path.normpath(value) != value
            or "\x00" in value or value.endswith("/")):
        raise HandoffError("predecessor cgroup is outside its user service")
    return Path("/sys/fs/cgroup") / value.lstrip("/")


def _cgroup_empty(path: Path) -> bool:
    try:
        with (path / "cgroup.events").open("rb") as stream:
            body = stream.read(4097)
    except FileNotFoundError:
        # A removed cgroup is empty; the exact unit must already be terminal.
        return True
    if len(body) > 4096:
        raise HandoffError("predecessor cgroup state exceeds its bound")
    values = {}
    try:
        for line in body.decode("ascii").splitlines():
            key, value = line.split()
            if key in values:
                raise ValueError("duplicate cgroup property")
            values[key] = value
    except (ValueError, UnicodeError) as error:
        raise HandoffError("invalid predecessor cgroup state") from error
    if values.get("populated") not in {"0", "1"}:
        raise HandoffError("predecessor cgroup population is unavailable")
    return values["populated"] == "0"


def _terminal_unit(state: dict[str, str], initial: dict[str, str]) -> bool:
    if state["MainPID"] != "0":
        if (state["MainPID"] != initial["MainPID"]
                or state["ControlGroup"] != initial["ControlGroup"]
                or state["ActiveState"] != "active" or state["SubState"] != "running"):
            raise HandoffError("predecessor service changed or is stopping")
        return False
    if (state["ControlPID"] != "0" or state["Result"] != "success"
            or state["ExecMainCode"] != "1" or state["ExecMainStatus"] != "0"
            or (state["ActiveState"], state["SubState"]) not in {("active", "exited"), ("inactive", "dead")}
            or state["ControlGroup"] not in {"", initial["ControlGroup"]}):
        raise HandoffError("predecessor service did not exit successfully")
    if not _cgroup_empty(_cgroup_path(initial["ControlGroup"])):
        raise HandoffError("predecessor service still has processes")
    return True


def _output_file(info: os.stat_result) -> None:
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
            or info.st_size > MAX_OUTPUT_BYTES):
        raise HandoffError("predecessor output must be a private bounded regular file")


def _strict_json(body: bytes) -> dict[str, Any]:
    def pairs(rows):
        value = {}
        for key, item in rows:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    def constant(_value):
        raise ValueError("nonfinite JSON value")

    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite JSON value")
        return result

    try:
        value = json.loads(body, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)
        if not isinstance(value, dict):
            raise ValueError("not a JSON object")
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise HandoffError("predecessor output is not one strict JSON object") from error


@contextmanager
def _bound_output(path: Path, state: dict[str, str]) -> Iterator[Any]:
    """Bind a fresh output inode to the live, exact supervisor's stdout.

    systemd exposes StandardOutput=append but not the path through `show` on
    supported hosts. The live descriptor supplies the missing exact association.
    The retained parent descriptor and inode prevent later path substitution.
    """
    if not path.is_absolute() or os.path.normpath(str(path)) != str(path):
        raise HandoffError("predecessor output path must be absolute and normalized")
    with legacy._parent(path) as parent:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent)
        try:
            info = os.fstat(descriptor)
            _output_file(info)
            if info.st_size != 0:
                raise HandoffError("handoff requires fresh empty predecessor output at initial binding")
            stdout = os.stat(f"/proc/{state['MainPID']}/fd/1")
            if (stdout.st_dev, stdout.st_ino) != (info.st_dev, info.st_ino):
                raise HandoffError("predecessor stdout is not the admitted output file")

            def read_result():
                before = os.fstat(descriptor)
                _output_file(before)
                linked = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
                if ((before.st_dev, before.st_ino) != (info.st_dev, info.st_ino)
                        or legacy._fingerprint(before) != legacy._fingerprint(linked)):
                    raise HandoffError("predecessor output identity changed")
                os.lseek(descriptor, 0, os.SEEK_SET)
                blocks, total = [], 0
                while total <= MAX_OUTPUT_BYTES:
                    block = os.read(descriptor, min(65536, MAX_OUTPUT_BYTES + 1 - total))
                    if not block:
                        break
                    blocks.append(block)
                    total += len(block)
                after = os.fstat(descriptor)
                linked = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
                if (total > MAX_OUTPUT_BYTES or total != before.st_size
                        or legacy._fingerprint(before) != legacy._fingerprint(after)
                        or legacy._fingerprint(after) != legacy._fingerprint(linked)):
                    raise HandoffError("predecessor output changed while being read")
                return _strict_json(b"".join(blocks))

            yield read_result
        finally:
            os.close(descriptor)


def _source_status(config: ControllerConfig) -> dict[str, Any]:
    value = legacy._document(config.state_root / "status.json")
    if value.get("config_id") != config.config_id or value.get("config_sha256") != config.physical_sha256:
        raise HandoffError("predecessor status config binding changed")
    return value


def _completed_source(value: dict[str, Any]) -> bool:
    return (value.get("completion_reason") == "campaign_drained"
            and value.get("lifecycle") == "completed" and value.get("actual_state") == "completed"
            and value.get("desired_state") == "stopped" and value.get("last_error") is None)


def _completed_companion(value: dict[str, Any]) -> bool:
    try:
        return (value["lifecycle"] == "stopped" and value["active_job"] is None
                and value["last_error"] is None
                and value["discovered"]["cold_candidates"] == value["expected_cold_backlog"]
                and value["jobs"]["completed"] == value["discovered"]["total_candidates"])
    except (KeyError, TypeError):
        return False


def _check_intents(predecessor: ControllerConfig, successor: ControllerConfig,
                   initial_predecessor: dict, initial_successor: dict,
                   source_status: dict, *, allow_completion_transition: bool = False) -> dict:
    if read_control_state(successor) != initial_successor:
        raise HandoffCancelled("successor operator intent changed; handoff cancelled")
    current = read_control_state(predecessor)
    if current == initial_predecessor:
        return current
    # Source completion writes Stop before its final checkpoint/status, then the
    # supervisor may write one more. While that exact supervisor remains alive,
    # these generations permit WAITING ONLY. A manual Stop cannot authorize Start:
    # final source summary, full drain, and exact automatic trajectory are required.
    if (current.get("desired_state") == "stopped"
            and (_completed_source(source_status) or allow_completion_transition)
            and _integer(current.get("generation"))
            and current["generation"] in {initial_predecessor["generation"] + 1,
                                          initial_predecessor["generation"] + 2}):
        return current
    raise HandoffCancelled("predecessor operator intent changed or stopped before completion")


def _validate_result(result: dict, config: ControllerConfig, registration: Any,
                     initial: dict, final: dict, status_value: dict) -> None:
    expected_fields = {"status", "source_config_id", "registration_identity_sha256",
                       "registration_sha256", "stop_coordinated", "stop_reason", "children"}
    if (set(result) != expected_fields or result.get("status") != "supervised_campaign_exited"
            or result.get("source_config_id") != config.config_id
            or result.get("registration_sha256") != registration.physical_sha256
            or result.get("registration_identity_sha256") != registration.document["identity_sha256"]
            or type(result.get("stop_coordinated")) is not bool
            or result.get("stop_reason") != ("one supervised child exited" if result["stop_coordinated"] else None)):
        raise HandoffError("predecessor supervisor completion proof is invalid")
    if (final.get("desired_state") != "stopped"
            or final.get("generation") != initial["generation"] + 1 + int(result["stop_coordinated"])):
        raise HandoffCancelled("predecessor Stop generation differs from automatic completion")
    children = result.get("children")
    if not isinstance(children, dict) or set(children) != {"source_controller", "longform_companion"}:
        raise HandoffError("predecessor child result pair is incomplete")
    for child in children.values():
        if (not isinstance(child, dict) or set(child) != {"returncode", "result", "result_error"}
                or type(child.get("returncode")) is not int or child["returncode"] != 0
                or child.get("result_error") is not None or not isinstance(child.get("result"), dict)):
            raise HandoffError("predecessor child failed or lacks a result")
    source = children["source_controller"]["result"]
    if (source.get("status") != "controller_exited" or source.get("config_id") != config.config_id
            or type(source.get("returncode")) is not int or source["returncode"] != 0
            or not _completed_source(source) or not _completed_source(status_value)
            or not _integer(source.get("cycle")) or source["cycle"] != status_value.get("cycle")):
        raise HandoffError("source controller did not prove a drained campaign")
    companion = children["longform_companion"]["result"]
    if (companion.get("status") != "stopped"
            or companion.get("reason") != "source_controller_desired_state_stopped"
            or companion.get("source_controller_mutated") is not False):
        raise HandoffError("long-form companion did not stop normally with the source")


def _completed_snapshot(config: ControllerConfig, registration: Any) -> dict[str, Any]:
    control = read_control_state(config)
    source = _source_status(config)
    companion = read_companion_status(registration)
    if not _completed_source(source) or not _completed_companion(companion):
        raise HandoffError("both predecessor lanes must be fully complete and inactive")
    # The ordinary guard intentionally accepts only stopped campaigns. This
    # isolated projection permits completed ONLY after the stronger full-drain
    # proof, and preserves the entire raw snapshot as the unchanged witness.
    projected = deepcopy(source)
    projected.update(lifecycle="stopped", actual_state="stopped")
    legacy._validate_documents(control, projected, companion)
    return {"control": control, "source": source, "companion": companion}


@contextmanager
def _hold_completed(config: ControllerConfig, registration: Any) -> Iterator[Any]:
    first = _completed_snapshot(config, registration)
    locks = []
    try:
        for path in sorted((config.state_root / "controller.lock", registration.status_path.parent / "dispatch.lock")):
            with legacy._parent(path) as parent:
                descriptor = os.open(path.name, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                try:
                    info = os.fstat(descriptor)
                    legacy._file_safe(info, lock=True)
                    if legacy._fingerprint(info) != legacy._fingerprint(os.stat(path.name, dir_fd=parent, follow_symlinks=False)):
                        raise HandoffError("predecessor execution lock changed")
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BaseException:
                    os.close(descriptor)
                    raise
            locks.append((path, descriptor, legacy._fingerprint(info)))

        def check():
            for path, descriptor, fingerprint in locks:
                if (legacy._fingerprint(os.fstat(descriptor)) != fingerprint
                        or legacy._fingerprint(legacy._stat_lock(path)) != fingerprint):
                    raise HandoffError("predecessor execution lock identity changed")
            if _completed_snapshot(config, registration) != first:
                raise HandoffCancelled("predecessor completion state changed under exclusion")

        check()
        yield check
    finally:
        for _path, descriptor, _fingerprint in reversed(locks):
            os.close(descriptor)


@contextmanager
def _cancellable() -> Iterator[None]:
    def stop(signum, _frame):
        raise HandoffCancelled(f"handoff received signal {signum}")

    previous = {number: signal.signal(number, stop) for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def handoff(predecessor_config: Path, predecessor_sha256: str, *, predecessor_unit: str,
            predecessor_invocation_id: str, predecessor_generation: int,
            predecessor_output: Path, successor_config: Path, successor_sha256: str,
            max_wait_seconds: int = 86400, poll_seconds: int = 10) -> None:
    """Wait read-only, then replace this admitted MainPID with the successor."""
    if (not _integer(predecessor_generation, 0, 2**63 - 4)
            or not _integer(max_wait_seconds, 1, 86400) or not _integer(poll_seconds, 1, 10)):
        raise HandoffError("handoff generation or wait bounds are invalid")
    with _cancellable():
        _handoff(predecessor_config, predecessor_sha256, predecessor_unit=predecessor_unit,
                 predecessor_invocation_id=predecessor_invocation_id,
                 predecessor_generation=predecessor_generation, predecessor_output=predecessor_output,
                 successor_config=successor_config, successor_sha256=successor_sha256,
                 max_wait_seconds=max_wait_seconds, poll_seconds=poll_seconds)


def _handoff(predecessor_config: Path, predecessor_sha256: str, **options: Any) -> None:
    if CONTROLLER_SUPERVISOR_PID_ENV in os.environ:
        raise HandoffError("handoff must itself be its admitted service MainPID")
    own_context = ControllerUnitContext.from_current_systemd_unit()
    if own_context.outer_unit == options["predecessor_unit"]:
        raise HandoffError("handoff and predecessor must have separate admitted services")
    predecessor = load_config(predecessor_config, predecessor_sha256)
    successor = load_config(options["successor_config"], options["successor_sha256"])
    if (predecessor.config_id == successor.config_id or predecessor.path == successor.path
            or predecessor.state_root == successor.state_root):
        raise HandoffError("successor must be an independent sealed campaign")
    registration = load_registration_if_present(predecessor.path, predecessor.physical_sha256)
    successor_registration = load_registration_if_present(successor.path, successor.physical_sha256)
    if registration is None or successor_registration is None:
        raise HandoffError("both campaigns require registered long-form companions")
    if registration.status_path == successor_registration.status_path:
        raise HandoffError("campaigns must have independent companion state")
    read_companion_status(registration)
    read_companion_status(successor_registration)
    initial_predecessor = read_control_state(predecessor)
    initial_successor = read_control_state(successor)
    if (initial_predecessor["desired_state"] != "running"
            or initial_predecessor["generation"] != options["predecessor_generation"]
            or initial_successor["desired_state"] != "stopped"):
        raise HandoffCancelled("campaign intent does not match the admitted handoff")
    unit, invocation = options["predecessor_unit"], options["predecessor_invocation_id"]
    initial_unit = _unit_state(unit, invocation)
    if (initial_unit["ActiveState"] != "active" or initial_unit["SubState"] != "running"
            or initial_unit["MainPID"] == "0" or initial_unit["ControlPID"] != "0"):
        raise HandoffError("handoff must bind the predecessor while its supervisor is running")
    _cgroup_path(initial_unit["ControlGroup"])
    deadline = time.monotonic() + options["max_wait_seconds"]
    armed = None
    try:
        with _bound_output(options["predecessor_output"], initial_unit) as read_result:
            if _unit_state(unit, invocation) != initial_unit:
                raise HandoffError("predecessor service changed during stdout binding")
            # A single acknowledgement lets the operator distinguish an admitted
            # wait from a service that merely exists. No polling log or state write.
            print(json.dumps({
                "status": "waiting_for_predecessor", "campaign_started": False,
                "predecessor_config_id": predecessor.config_id,
                "successor_config_id": successor.config_id,
                "predecessor_unit": unit, "predecessor_invocation_id": invocation,
                "predecessor_generation": initial_predecessor["generation"],
                "successor_generation": initial_successor["generation"],
            }, sort_keys=True), flush=True)
            while True:
                if time.monotonic() >= deadline:
                    raise HandoffCancelled("bounded campaign handoff wait expired")
                source = _source_status(predecessor)
                state = _unit_state(unit, invocation)
                terminal = _terminal_unit(state, initial_unit)
                final = _check_intents(predecessor, successor, initial_predecessor, initial_successor, source,
                                       allow_completion_transition=not terminal)
                if terminal:
                    result = read_result()
                    _validate_result(result, predecessor, registration, initial_predecessor, final, source)
                    break
                time.sleep(min(options["poll_seconds"], max(0.0, deadline - time.monotonic())))
            with _hold_completed(predecessor, registration) as check_held:
                check_held()
                if not _terminal_unit(_unit_state(unit, invocation), initial_unit):
                    raise HandoffError("predecessor service restarted before handoff")
                # Re-read sealed registration to bind against replacement while waiting.
                current_registration = load_registration_if_present(predecessor.path, predecessor.physical_sha256)
                if (current_registration is None
                        or current_registration.physical_sha256 != registration.physical_sha256):
                    raise HandoffError("predecessor companion registration changed")
                current_successor = load_registration_if_present(successor.path, successor.physical_sha256)
                if (current_successor is None
                        or current_successor.physical_sha256 != successor_registration.physical_sha256):
                    raise HandoffError("successor companion registration changed")
                final = _check_intents(predecessor, successor, initial_predecessor, initial_successor,
                                       _source_status(predecessor))
                _validate_result(read_result(), predecessor, registration, initial_predecessor, final,
                                 _source_status(predecessor))
                if ControllerUnitContext.from_current_systemd_unit() != own_context:
                    raise HandoffError("handoff service identity changed")
                check_held()
                armed = _set_control_if_unchanged(successor, initial_successor, "running")
            # No execution leases are inherited by the different campaign.
            # An intervening Stop is checked without overwriting the newer intent.
            if read_control_state(predecessor) != final or read_control_state(successor) != armed:
                raise HandoffCancelled("operator intent changed at campaign handoff")
        wrapper = REPOSITORY / "autonomous_controller/bin/himr-autonomous-controller"
        os.execv(str(wrapper), [str(wrapper), "run", "--config", str(successor.path),
                               "--expected-config-sha256", successor.physical_sha256])
        raise HandoffError("successor exec unexpectedly returned")
    except BaseException:
        if armed is not None:
            try:
                _set_control_if_unchanged(successor, armed, "stopped")
            except Exception:
                # Never overwrite a later operator request while undoing our Start.
                pass
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--predecessor-config", type=Path, required=True)
    parser.add_argument("--predecessor-config-sha256", dest="predecessor_sha256", required=True)
    parser.add_argument("--predecessor-unit", required=True)
    parser.add_argument("--predecessor-invocation-id", required=True)
    parser.add_argument("--predecessor-generation", type=int, required=True)
    parser.add_argument("--predecessor-output", type=Path, required=True)
    parser.add_argument("--successor-config", type=Path, required=True)
    parser.add_argument("--successor-config-sha256", dest="successor_sha256", required=True)
    parser.add_argument("--max-wait-seconds", type=int, default=86400)
    parser.add_argument("--poll-seconds", type=int, default=10)
    args = vars(parser.parse_args(argv))
    try:
        handoff(**args)
    except HandoffCancelled as error:
        print(json.dumps({"status": "cancelled", "campaign_started": False, "reason": str(error)}))
        return 0
    except Exception as error:
        print(json.dumps({"status": "faulted", "campaign_started": False,
                          "error": {"type": type(error).__name__, "message": str(error)}}))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
