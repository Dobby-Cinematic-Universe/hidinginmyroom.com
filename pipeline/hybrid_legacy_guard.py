"""Read-only legacy-state checks and nonblocking execution exclusion.

Inspection never creates files or takes locks. An explicitly invoked hybrid cycle
may hold the two pre-existing legacy execution locks; neither control.lock nor
the operator service lock is opened. No legacy configuration/state is rewritten.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import errno
import fcntl
import grp
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import re
import stat
from typing import Any, Iterator


MAX_GUARDS = 16
MAX_STATUS_BYTES = 1024 * 1024
STAGES = {"acquisition", "preprocess", "gpu_readiness", "cold_retention"}
GUARD_FIELDS = {
    "control_path", "status_path", "companion_status_path",
    "controller_lock", "companion_lock",
}
CONTROL_FIELDS = {
    "kind", "schema_version", "config_id", "generation", "desired_state", "requested_at",
}
STATUS_FIELDS = {
    "kind", "schema_version", "config_id", "config_sha256", "lifecycle", "actual_state",
    "desired_state", "current_stage", "pid", "started_at", "updated_at", "cycle",
    "dispatch_sequence", "scheduler_mode", "completion_reason", "campaign",
    "consecutive_failures", "last_error", "errors", "last_event", "monitor", "stages",
    "lanes", "execution", "progress", "pipeline_telemetry", "throughput", "storage",
    "recent_activity", "current_gpu_child", "safety",
}
COMPANION_FIELDS = {
    "kind", "schema_version", "source_controller", "campaign_config", "lifecycle",
    "expected_cold_backlog", "discovered", "jobs", "active_job", "updated_at", "last_error",
}
LANE_FIELDS = {
    "state", "active", "limit", "dispatch_id", "started_at", "last_status", "wait_reason",
    "last_transition_at",
}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
CONTROLLER_ID = re.compile(r"himrautocfg_[0-9a-f]{32}\Z")
CAMPAIGN_ID = re.compile(r"himrlongcfg_[0-9a-f]{32}\Z")


class LegacyBusy(RuntimeError):
    """A fixed, sanitized reason that legacy exclusion is unavailable."""


def _fail(reason: str) -> None:
    raise LegacyBusy(reason)


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _integer(value: Any, minimum: int = 0, maximum: int = 2**63 - 1) -> bool:
    return type(value) is int and minimum <= value <= maximum


def _timestamp(value: Any, *, nullable: bool = False) -> bool:
    if nullable and value is None:
        return True
    if not isinstance(value, str):
        return False
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%SZ") == value
    except ValueError:
        return False


def _shape(value: Any, fields: set[str]) -> bool:
    return isinstance(value, dict) and set(value) == fields


def _normalize(guards: Any) -> list[dict[str, str]]:
    if not isinstance(guards, list) or not 1 <= len(guards) <= MAX_GUARDS:
        _fail("legacy_guard_configuration_invalid")
    seen: set[str] = set()
    normalized = []
    for guard in guards:
        if not _shape(guard, GUARD_FIELDS):
            _fail("legacy_guard_configuration_invalid")
        for value in guard.values():
            if not isinstance(value, str) or not value or "\x00" in value:
                _fail("legacy_guard_configuration_invalid")
            path = Path(value)
            if not path.is_absolute() or str(path) != value or os.path.normpath(value) != value or value == "/":
                _fail("legacy_guard_configuration_invalid")
            if value in seen:
                _fail("legacy_guard_paths_repeated")
            seen.add(value)
        control, status_path, lock = (Path(guard[key]) for key in ("control_path", "status_path", "controller_lock"))
        companion, companion_lock = (Path(guard[key]) for key in ("companion_status_path", "companion_lock"))
        if (
            (control.name, status_path.name, lock.name) != ("control.json", "status.json", "controller.lock")
            or not control.parent == status_path.parent == lock.parent
            or (companion.name, companion_lock.name) != ("status.json", "dispatch.lock")
            or companion.parent != companion_lock.parent
            or companion.parent == control.parent
        ):
            _fail("legacy_guard_configuration_invalid")
        normalized.append(dict(guard))
    return normalized


def _owner_private_group(info: os.stat_result) -> bool:
    """Resolve a user-private primary group, without trusting its name alone.

    This accommodates the conventional owner:owner 0775 home directory while
    retaining peer-write exclusion. Both supplemental and primary memberships
    must be owner-only according to the operating system's account databases.
    Recheck on every path traversal so membership changes revoke this exception.
    """
    if info.st_uid != os.geteuid():
        return False
    try:
        owner = pwd.getpwuid(info.st_uid)
        group = grp.getgrgid(info.st_gid)
        accounts = pwd.getpwall()
        if (owner.pw_uid != info.st_uid or owner.pw_gid != info.st_gid
                or group.gr_gid != info.st_gid
                or not isinstance(owner.pw_name, str) or not owner.pw_name
                or not isinstance(group.gr_mem, list)
                or any(member != owner.pw_name for member in group.gr_mem)):
            return False
        primary_members = [account for account in accounts if account.pw_gid == info.st_gid]
        return bool(primary_members) and all(
            account.pw_uid == info.st_uid and account.pw_name == owner.pw_name
            for account in primary_members
        )
    except Exception:
        # Missing/NSS lookup errors and malformed records never grant access.
        return False


def _directory_safe(info: os.stat_result, *, private: bool = False) -> None:
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in {0, os.geteuid()}:
        _fail("legacy_directory_unsafe")
    if private:
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            _fail("legacy_directory_unsafe")
    elif info.st_mode & 0o022 and not (info.st_uid == 0 and info.st_mode & stat.S_ISVTX):
        if info.st_mode & 0o002 or not _owner_private_group(info):
            _fail("legacy_directory_unsafe")


@contextmanager
def _parent(path: Path) -> Iterator[int]:
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        _directory_safe(os.fstat(descriptor))
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            _directory_safe(os.fstat(descriptor))
        _directory_safe(os.fstat(descriptor), private=True)
        yield descriptor
    finally:
        os.close(descriptor)


def _file_safe(info: os.stat_result, *, lock: bool = False) -> None:
    if (
        not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
        or (lock and info.st_size != 0)
    ):
        _fail("legacy_lock_unsafe" if lock else "legacy_document_unsafe")


def _fingerprint(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _document(path: Path, maximum: int = MAX_STATUS_BYTES) -> dict[str, Any]:
    with _parent(path) as parent:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent)
        try:
            before = os.fstat(descriptor)
            _file_safe(before)
            if not 0 < before.st_size <= maximum:
                _fail("legacy_document_size_invalid")
            blocks = []
            remaining = before.st_size
            while remaining:
                block = os.read(descriptor, min(remaining, 1024 * 1024))
                if not block:
                    _fail("legacy_document_changed")
                blocks.append(block)
                remaining -= len(block)
            linked = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
            if _fingerprint(before) != _fingerprint(os.fstat(descriptor)) or _fingerprint(before) != _fingerprint(linked):
                _fail("legacy_document_changed")
        finally:
            os.close(descriptor)
    body = b"".join(blocks)

    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                _fail("legacy_document_invalid")
            result[key] = value
        return result

    def constant(_value):
        _fail("legacy_document_invalid")

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            _fail("legacy_document_invalid")
        return number

    try:
        value = json.loads(body, object_pairs_hook=pairs, parse_constant=constant, parse_float=finite_float)
        if not isinstance(value, dict) or _canonical(value) != body:
            _fail("legacy_document_invalid")
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError):
        _fail("legacy_document_invalid")


def _pid_present(pid: int) -> bool:
    try:
        Path(f"/proc/{pid}").lstat()
    except FileNotFoundError:
        return False
    except OSError:
        _fail("legacy_process_state_unknown")
    return True


def _validate_documents(control: dict, status_value: dict, companion: dict) -> None:
    if (
        not _shape(control, CONTROL_FIELDS)
        or control.get("kind") != "himr_autonomous_controller_control"
        or type(control.get("schema_version")) is not int or control["schema_version"] != 1
        or not isinstance(control.get("config_id"), str) or CONTROLLER_ID.fullmatch(control["config_id"]) is None
        or not _integer(control.get("generation"))
        or not _timestamp(control.get("requested_at"), nullable=True)
    ):
        _fail("legacy_control_invalid")
    if control.get("desired_state") != "stopped":
        _fail("legacy_control_not_stopped")
    if (
        not _shape(status_value, STATUS_FIELDS)
        or status_value.get("kind") != "himr_autonomous_controller_status"
        or type(status_value.get("schema_version")) is not int or status_value["schema_version"] != 1
        or status_value.get("config_id") != control["config_id"]
        or not isinstance(status_value.get("config_sha256"), str) or SHA256.fullmatch(status_value["config_sha256"]) is None
        or not _integer(status_value.get("pid"), 1, 2**31 - 1)
        or not _timestamp(status_value.get("updated_at"))
        or not _timestamp(status_value.get("started_at"), nullable=True)
        or any(not _integer(status_value.get(key)) for key in ("cycle", "dispatch_sequence", "consecutive_failures"))
        or not isinstance(status_value.get("errors"), list)
        or not isinstance(status_value.get("recent_activity"), list)
    ):
        _fail("legacy_status_invalid")
    if any(status_value.get(key) != "stopped" for key in ("desired_state", "actual_state", "lifecycle")):
        _fail("legacy_status_not_stopped")
    if status_value.get("current_stage") is not None or status_value.get("current_gpu_child") is not None:
        _fail("legacy_work_still_active")
    execution = status_value["execution"]
    if (
        not _shape(execution, {"accepting_new_work", "draining", "inflight_total"})
        or execution.get("accepting_new_work") is not False
        or execution.get("draining") is not False
        or type(execution.get("inflight_total")) is not int or execution["inflight_total"] != 0
    ):
        _fail("legacy_work_still_active")
    lanes = status_value["lanes"]
    if not _shape(lanes, STAGES):
        _fail("legacy_lanes_invalid")
    for lane in lanes.values():
        if (
            not _shape(lane, LANE_FIELDS) or lane.get("state") not in {"idle", "waiting", "stopped"}
            or type(lane.get("active")) is not int or lane["active"] != 0
            or type(lane.get("limit")) is not int or lane["limit"] != 1
        ):
            _fail("legacy_work_still_active")
    monitor, stages = status_value["monitor"], status_value["stages"]
    if not isinstance(monitor, dict) or not STAGES <= set(monitor) or not _shape(stages, STAGES):
        _fail("legacy_monitor_invalid")
    for stage in STAGES:
        row = stages[stage]
        if row != monitor[stage] or (row is not None and not isinstance(row, dict)):
            _fail("legacy_monitor_invalid")
    gpu = stages["gpu_readiness"]
    if gpu is not None and (
        type(gpu.get("active_children")) is not int or gpu["active_children"] != 0
        or "current_gpu_child" not in gpu or gpu["current_gpu_child"] is not None
    ):
        _fail("legacy_gpu_child_not_quiesced")
    for key in ("campaign", "progress", "pipeline_telemetry", "throughput", "storage", "safety"):
        if not isinstance(status_value[key], dict):
            _fail("legacy_status_invalid")
    if _pid_present(status_value["pid"]):
        # A recycled PID is a conservative hold, never a reason to signal it.
        _fail("legacy_controller_process_present")
    if (
        not _shape(companion, COMPANION_FIELDS)
        or companion.get("kind") != "himr_longform_asr_campaign_status"
        or type(companion.get("schema_version")) is not int or companion["schema_version"] != 1
        or companion.get("source_controller") != {
            "config_id": control["config_id"], "physical_sha256": status_value["config_sha256"]
        }
        or not _shape(companion.get("campaign_config"), {"config_id", "physical_sha256"})
        or not isinstance(companion["campaign_config"].get("config_id"), str)
        or CAMPAIGN_ID.fullmatch(companion["campaign_config"]["config_id"]) is None
        or not isinstance(companion["campaign_config"].get("physical_sha256"), str)
        or SHA256.fullmatch(companion["campaign_config"]["physical_sha256"]) is None
        or not _timestamp(companion.get("updated_at"))
        or not _integer(companion.get("expected_cold_backlog"))
    ):
        _fail("legacy_companion_status_invalid")
    if companion.get("lifecycle") not in {"ready", "stopped"} or companion.get("active_job") is not None:
        _fail("legacy_companion_not_stopped")
    counts, jobs = companion["discovered"], companion["jobs"]
    if (
        not _shape(counts, {"cold_candidates", "queue_candidates", "total_candidates"})
        or not _shape(jobs, {"unprepared", "preprocessed", "prepared", "incomplete", "completed"})
        or any(not _integer(value) for value in [*counts.values(), *jobs.values()])
        or counts["cold_candidates"] + counts["queue_candidates"] != counts["total_candidates"]
        or counts["cold_candidates"] > companion["expected_cold_backlog"]
        or sum(jobs.values()) != counts["total_candidates"]
    ):
        _fail("legacy_companion_counts_invalid")


def _stat_lock(path: Path) -> os.stat_result:
    with _parent(path) as parent:
        info = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
    _file_safe(info, lock=True)
    return info


def _snapshot(guards: list[dict[str, str]]) -> list[dict[str, Any]]:
    snapshots = []
    for guard in guards:
        control = _document(Path(guard["control_path"]), 32 * 1024)
        # An active intent is sufficient to stop; never open execution lock files.
        if control.get("desired_state") != "stopped":
            _fail("legacy_control_not_stopped")
        status_value = _document(Path(guard["status_path"]))
        companion = _document(Path(guard["companion_status_path"]))
        _validate_documents(control, status_value, companion)
        locks = {key: list(_fingerprint(_stat_lock(Path(guard[key])))) for key in ("controller_lock", "companion_lock")}
        snapshots.append({"paths": guard, "control": control, "status": status_value, "companion": companion, "locks": locks})
    return snapshots


def _witness(snapshots: list[dict[str, Any]]) -> str:
    return hashlib.sha256(_canonical(snapshots)).hexdigest()


def inspect_legacy(guards: list[dict[str, str]]) -> dict[str, Any]:
    """Inspect small state and metadata without locking or writing.

    ``safe`` is a metadata snapshot, not a claim that the locks are available.
    Only ``hold_legacy`` establishes execution exclusion.
    """
    try:
        normalized = _normalize(guards)
        witness = _witness(_snapshot(normalized))
    except LegacyBusy as error:
        return {"safe": False, "reasons": [str(error)], "guard_count": len(guards) if isinstance(guards, list) else 0, "witness_sha256": None}
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        return {"safe": False, "reasons": ["legacy_state_unavailable_or_unsafe"], "guard_count": len(guards) if isinstance(guards, list) else 0, "witness_sha256": None}
    return {"safe": True, "reasons": [], "guard_count": len(normalized), "witness_sha256": witness}


@dataclass
class HeldLegacy:
    guards: list[dict[str, str]]
    witness_sha256: str
    _locks: list[tuple[Path, int, tuple[int, ...]]]
    _active: bool = True

    @property
    def inherited_fds(self) -> tuple[int, ...]:
        """Descriptors for an explicit subprocess ``pass_fds`` lease handoff.

        They remain CLOEXEC in this process; do not globally mark them inheritable.
        A child explicitly receiving them shares the same open-file descriptions,
        so its copies keep execution exclusion after the parent exits or is killed.
        The child must retain them until all its heavy work has terminated.
        """
        self.check_unchanged()
        return tuple(descriptor for _path, descriptor, _expected in self._locks)

    def check_unchanged(self) -> None:
        """Call before each new heavy dispatch; generation/status changes hold."""
        if not self._active:
            _fail("legacy_guard_not_held")
        try:
            for path, descriptor, expected in self._locks:
                if _fingerprint(os.fstat(descriptor)) != expected or _fingerprint(_stat_lock(path)) != expected:
                    _fail("legacy_execution_lock_changed")
            if _witness(_snapshot(self.guards)) != self.witness_sha256:
                _fail("legacy_state_changed")
        except LegacyBusy:
            raise
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            _fail("legacy_state_unavailable_or_unsafe")


@contextmanager
def hold_legacy(guards: list[dict[str, str]]) -> Iterator[HeldLegacy]:
    """Hold both existing execution locks for one explicitly authorized cycle."""
    locks: list[tuple[Path, int, tuple[int, ...]]] = []
    held = None
    try:
        normalized = _normalize(guards)
        first = inspect_legacy(normalized)
        if not first["safe"]:
            _fail(first["reasons"][0])
        paths = sorted(Path(guard[key]) for guard in normalized for key in ("controller_lock", "companion_lock"))
        for path in paths:
            with _parent(path) as parent:
                descriptor = os.open(path.name, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                try:
                    info = os.fstat(descriptor)
                    _file_safe(info, lock=True)
                    if _fingerprint(info) != _fingerprint(os.stat(path.name, dir_fd=parent, follow_symlinks=False)):
                        _fail("legacy_execution_lock_changed")
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError as error:
                        if error.errno in {errno.EAGAIN, errno.EACCES}:
                            _fail("legacy_execution_lock_busy")
                        _fail("legacy_execution_lock_unavailable")
                except BaseException:
                    os.close(descriptor)
                    raise
            locks.append((path, descriptor, _fingerprint(info)))
        held = HeldLegacy(normalized, first["witness_sha256"], locks)
        held.check_unchanged()
    except LegacyBusy:
        for _path, descriptor, _expected in reversed(locks):
            os.close(descriptor)
        raise
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        for _path, descriptor, _expected in reversed(locks):
            os.close(descriptor)
        _fail("legacy_state_unavailable_or_unsafe")
    except BaseException:
        for _path, descriptor, _expected in reversed(locks):
            os.close(descriptor)
        raise
    try:
        yield held
    finally:
        held._active = False
        for _path, descriptor, _expected in reversed(locks):
            # Closing releases only these descriptors' locks, including on errors.
            os.close(descriptor)
