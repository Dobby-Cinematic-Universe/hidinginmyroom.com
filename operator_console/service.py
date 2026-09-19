"""Private operational state and finite subprocess supervision for the console.

This module is deliberately an operational aid.  A successful console job is not a
pipeline receipt and does not grant catalogue, publication, identity, or completion
authority.  The browser selects only an opaque profile ID; :mod:`registry` remains
the closed command and path boundary.

The original direct supervisor remains available and deliberately does not expose
cancellation.  Actions admitted with ``supervisor="systemd_user"`` instead run in one
derived transient service cgroup with a manager-enforced deadline and exact-unit
cancellation.  In both cases, underlying finite pipeline limits and immutable
receipts remain authoritative.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from autonomous_controller.config import ControllerConfig, load_config
from autonomous_controller.state import request_start, request_stop

from . import registry


SERVICE_SCHEMA_VERSION = 2
SERVICE_IMPLEMENTATION_VERSION = "0.2.3"
DEFAULT_SERVICE_STATE_DIRECTORY = "service-state"
MAX_STREAM_BYTES = 8 * 1024 * 1024
# RLIMIT_FSIZE applies to every file written by the supervised process, not only
# its captured stdout/stderr.  GPU v5 permits a 16 MiB result artifact, so keep a
# separate ceiling with headroom for a contract-valid atomic artifact.  Log reads
# remain independently capped by MAX_STREAM_BYTES.
SYSTEMD_CHILD_MAX_FILE_BYTES = 32 * 1024 * 1024
SYSTEMD_DEFAULT_TASKS_MAX = 64
SYSTEMD_DEFAULT_STOP_TIMEOUT_SECONDS = 15
SYSTEMD_DEFAULT_MEMORY_SWAP_MAX_BYTES = 0
MAX_SUMMARY_BYTES = 32 * 1024
MAX_RECORD_BYTES = 2 * 1024 * 1024
MAX_JOBS = 10_000
# Keep a complete private history, but keep the polling response below the UI's
# two-million-character fail-closed bound even when every visible summary is full.
PUBLIC_JOB_LIMIT = 32
GLOBAL_JOB_LIMIT = 3
PREPARATION_LIFETIME_SECONDS = 300
READ_CHUNK_BYTES = 64 * 1024
LOG_RESPONSE_BYTES = 64 * 1024
JOB_ID_RE = re.compile(r"job_[0-9a-f]{32}\Z")
SYSTEMD_UNIT_RE = re.compile(r"himr-operator-job-[0-9a-f]{32}\.service\Z")
INVOCATION_ID_RE = re.compile(r"[0-9a-f]{32}\Z")
TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{32,192}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
HISTORICAL_ACTION_ID_RE = re.compile(r"[a-z][a-z0-9._-]{2,127}\Z")
RESOURCE_ID_RE = re.compile(r"[a-z][a-z0-9._-]{1,127}\Z")
# The rolling Archive action owns both stage claims while it internally runs one
# worker per stage. Cold retention owns its distinct destination writer plus the
# network/acquisition lane because both retain the same mutable acquisition-root
# topology. It may still overlap preprocessing or GPU work. Unknown/historical
# resources retain their original one-name claim.
RESOURCE_CLAIMS: dict[str, frozenset[str]] = {
    "network": frozenset({"network"}),
    "preprocess": frozenset({"preprocess"}),
    "archive_pipeline": frozenset({"network", "preprocess"}),
    "cold_storage": frozenset({"cold_storage", "network"}),
    "autonomous_pipeline": frozenset(
        {"network", "preprocess", "gpu", "cold_storage"}
    ),
    # The stop command only writes the exact controller's durable intent.  It must
    # remain runnable while the composite campaign resource is occupied.
    "autonomous_control": frozenset({"autonomous_control"}),
}
UTC_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")
ACTIVE_JOB_STATES = {
    "launching",
    "running",
    "detached_running",
    "reconciling",
    "cancelling",
}
TERMINAL_JOB_STATES = {
    "succeeded",
    "failed",
    "indeterminate_after_restart",
    "cancelled_reconciliation_required",
}
SYSTEMD_RUN = "/usr/bin/systemd-run"
SYSTEMCTL = "/usr/bin/systemctl"
ENV = "/usr/bin/env"
SYSTEMD_CONTROL_TIMEOUT_SECONDS = 15
SYSTEMD_POLL_SECONDS = 0.5
SYSTEMD_OUTPUT_LIMIT = 64 * 1024
AUTONOMY_START_ACTION_ID = "autonomy.run"
AUTONOMY_START_ENTRYPOINT = "autonomous_controller/bin/himr-autonomous-controller"
SYSTEMD_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "Result",
    "ExecMainCode",
    "ExecMainStatus",
    "InvocationID",
)
SYSTEMD_LIVE_ACTIVE_STATES = {"activating", "active", "reloading", "deactivating"}
CANCEL_CONFIRMATION = "CANCEL GPU JOB"
CANCELLATION_REASON = (
    "Cancellation is disabled in the direct-subprocess supervisor because it cannot "
    "prove whole-process-tree termination. Active systemd-user jobs expose exact-unit "
    "control-group cancellation."
)


class ServiceError(RuntimeError):
    """A bounded, user-presentable service failure."""

    def __init__(self, code: str, message: str, *, status: int = 422):
        super().__init__(message)
        self.code = code
        self.status = status


def _resource_claims(resource: str) -> frozenset[str]:
    return RESOURCE_CLAIMS.get(resource, frozenset({resource}))


class StaleRevision(ServiceError):
    def __init__(self, current_revision: int):
        super().__init__(
            "stale_revision",
            f"state changed; current revision is {current_revision}",
            status=409,
        )
        self.current_revision = current_revision


@dataclass(frozen=True)
class Preparation:
    token: str
    profile_id: str
    command: registry.PreparedCommand
    expires_at: str
    expires_monotonic: float


@dataclass(frozen=True)
class SystemdUnitState:
    load_state: str
    active_state: str
    sub_state: str
    result: str
    exec_main_code: int | None
    exec_main_status: int | None
    invocation_id: str | None

    @property
    def absent(self) -> bool:
        return self.load_state == "not-found"

    @property
    def process_live(self) -> bool:
        if self.absent or self.active_state not in SYSTEMD_LIVE_ACTIVE_STATES:
            return False
        return not (self.active_state == "active" and self.sub_state == "exited")

    @property
    def returncode(self) -> int | None:
        if self.exec_main_code == 1 and self.exec_main_status is not None:
            return self.exec_main_status
        if self.exec_main_code in {2, 3} and self.exec_main_status is not None:
            return -self.exec_main_status
        return None

    @property
    def deadline_exceeded(self) -> bool:
        return self.result in {"timeout", "watchdog"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_after(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _strict_json_bytes(body: bytes, label: str) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ServiceError("invalid_state", f"{label} contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ServiceError("invalid_state", f"{label} contains non-finite number {value!r}")

    try:
        return json.loads(
            body.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=reject_constant,
        )
    except ServiceError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise ServiceError("invalid_state", f"{label} is not strict JSON: {error}") from error


def _is_utc_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not UTC_TIMESTAMP_RE.fullmatch(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def _is_bounded_clean_text(value: Any, maximum: int, *, nonempty: bool = True) -> bool:
    if not isinstance(value, str) or len(value) > maximum:
        return False
    if nonempty and not value:
        return False
    return "\x00" not in value and not any(ord(character) < 32 for character in value)


def _is_beneath(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _assert_no_symlink_components(path: Path, label: str, *, allow_missing_leaf: bool) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    parts = absolute.parts[1:]
    for index, component in enumerate(parts):
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            if allow_missing_leaf and index == len(parts) - 1:
                return
            raise ServiceError("unsafe_path", f"{label} has a missing parent component")
        except OSError as error:
            raise ServiceError("unsafe_path", f"cannot inspect {label}: {error}") from error
        if stat.S_ISLNK(info.st_mode):
            raise ServiceError("unsafe_path", f"{label} contains a symlink component")


def _ensure_private_directory(path: Path, label: str, *, create: bool) -> Path:
    absolute = Path(os.path.abspath(path))
    if create and not absolute.exists():
        parent = absolute.parent
        _assert_no_symlink_components(parent, f"{label} parent", allow_missing_leaf=False)
        old_umask = os.umask(0o077)
        try:
            absolute.mkdir(mode=0o700)
        except FileExistsError:
            pass
        except OSError as error:
            raise ServiceError("unsafe_state", f"cannot create {label}: {error}") from error
        finally:
            os.umask(old_umask)
    _assert_no_symlink_components(absolute, label, allow_missing_leaf=False)
    try:
        info = absolute.lstat()
    except OSError as error:
        raise ServiceError("unsafe_state", f"cannot inspect {label}: {error}") from error
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise ServiceError("unsafe_state", f"{label} must be a non-symlink directory")
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ServiceError(
            "unsafe_state", f"{label} must be current-user-owned with exact mode 0700"
        )
    return absolute


def _validate_private_workspace_path(path: Path, repo_root: Path, label: str) -> Path:
    absolute = Path(os.path.abspath(path))
    research_root = repo_root / "research"
    if not _is_beneath(absolute, research_root):
        raise ServiceError("unsafe_path", f"{label} must stay beneath the repository research root")
    # Reject cold storage lexically, before any filesystem access to that mount.
    if _is_beneath(absolute, Path("/mnt/archive/HIMR")):
        raise ServiceError("unsafe_path", f"{label} cannot reference cold storage")
    return absolute


def _open_private_lock(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    old_umask = os.umask(0o077)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        raise ServiceError("unsafe_state", f"cannot open service lock: {error}") from error
    finally:
        os.umask(old_umask)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ServiceError(
                "unsafe_state", "service lock must be a current-user-owned single-link mode-0600 file"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ServiceError(
                "service_busy", "another operator console holds this state root", status=409
            ) from error
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _atomic_write(path: Path, body: bytes, *, mode: int = 0o600, replace: bool = True) -> None:
    parent = _ensure_private_directory(path.parent, "operational record parent", create=False)
    if replace and (path.exists() or path.is_symlink()):
        try:
            existing = path.lstat()
        except OSError as error:
            raise ServiceError("unsafe_state", f"cannot inspect existing record: {error}") from error
        if (
            stat.S_ISLNK(existing.st_mode)
            or not stat.S_ISREG(existing.st_mode)
            or existing.st_nlink != 1
            or existing.st_uid != os.geteuid()
            or stat.S_IMODE(existing.st_mode) != mode
        ):
            raise ServiceError("unsafe_state", "existing operational record has unsafe metadata")
    temporary = parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    old_umask = os.umask(0o077)
    descriptor = -1
    try:
        descriptor = os.open(temporary, flags, mode)
        os.fchmod(descriptor, mode)
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise OSError("short operational-record write")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path, follow_symlinks=False)
            temporary.unlink()
        directory_descriptor = os.open(
            parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except FileExistsError as error:
        raise ServiceError("state_conflict", f"operational record already exists: {path.name}") from error
    except OSError as error:
        raise ServiceError("state_write_failed", f"cannot persist operational record: {error}") from error
    finally:
        os.umask(old_umask)
        if descriptor >= 0:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        with contextlib.suppress(OSError):
            temporary.unlink()


def _stable_private_read(path: Path, *, maximum: int, mode: int = 0o600) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ServiceError("unsafe_state", f"cannot open private record: {error}") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size > maximum
            or before.st_nlink != 1
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != mode
        ):
            raise ServiceError("unsafe_state", "private record has unsafe metadata")
        body = os.pread(descriptor, before.st_size + 1, 0)
        after = os.fstat(descriptor)
        try:
            named = path.lstat()
        except OSError as error:
            raise ServiceError("unsafe_state", f"cannot reinspect private file: {error}") from error
    finally:
        os.close(descriptor)
    fingerprint = lambda value: (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
    if (
        len(body) != before.st_size
        or fingerprint(before) != fingerprint(after)
        or (named.st_dev, named.st_ino) != (before.st_dev, before.st_ino)
    ):
        raise ServiceError("unsafe_state", "private record changed while being read")
    return body


def _stable_private_chunk(
    path: Path,
    *,
    offset: int,
    maximum: int,
    expected_size: int,
    mode: int = 0o600,
) -> bytes:
    """Read one bounded operational-log chunk with live path/descriptor checks."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ServiceError("unsafe_state", f"cannot open private log: {error}") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size != expected_size
            or before.st_nlink != 1
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != mode
        ):
            raise ServiceError("unsafe_state", "private log has unsafe metadata")
        body = os.pread(descriptor, maximum, offset)
        after = os.fstat(descriptor)
        try:
            named = path.lstat()
        except OSError as error:
            raise ServiceError("unsafe_state", f"cannot reinspect private log: {error}") from error
    finally:
        os.close(descriptor)
    fingerprint = lambda value: (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
    if (
        fingerprint(before) != fingerprint(after)
        or (named.st_dev, named.st_ino) != (before.st_dev, before.st_ino)
    ):
        raise ServiceError("unsafe_state", "private log changed while being read")
    return body


def _inspect_private_file(
    path: Path,
    *,
    maximum: int,
    expected_size: int | None = None,
    mode: int = 0o600,
) -> int:
    """Inspect bounded private-file metadata without scanning historical log bytes."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ServiceError("unsafe_state", f"cannot open private file: {error}") from error
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_size < 0
            or info.st_size > maximum
            or (expected_size is not None and info.st_size != expected_size)
            or info.st_nlink != 1
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != mode
        ):
            raise ServiceError("unsafe_state", "private file has unsafe metadata")
        try:
            named = path.lstat()
        except OSError as error:
            raise ServiceError("unsafe_state", f"cannot reinspect private file: {error}") from error
    finally:
        os.close(descriptor)
    if (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino):
        raise ServiceError("unsafe_state", "private file pathname changed while inspected")
    return info.st_size


def initialize_workspace(*, repo_root: Path, workspace: Path) -> dict[str, Any]:
    """Create an empty private console workspace without replacing anything."""

    repo = repo_root.resolve(strict=True)
    root = _validate_private_workspace_path(workspace, repo, "operator workspace")
    if root.exists() or root.is_symlink():
        root = _ensure_private_directory(root, "operator workspace", create=False)
    else:
        parent = root.parent
        if parent != repo / "research":
            _ensure_private_directory(parent, "operator workspace parent", create=False)
        old_umask = os.umask(0o077)
        try:
            root.mkdir(mode=0o700)
        finally:
            os.umask(old_umask)
        root = _ensure_private_directory(root, "operator workspace", create=False)
    profiles = root / "profiles.json"
    if profiles.exists() or profiles.is_symlink():
        raise ServiceError(
            "state_conflict", "profiles.json already exists; init never overwrites it", status=409
        )
    # Console process state has a deliberately distinct namespace from profile-bound
    # pipeline state. Active GPU/cold-retention assets may live in a workspace-level
    # ``state/`` tree and must never be mistaken for console jobs.
    state_root = root / DEFAULT_SERVICE_STATE_DIRECTORY
    _ensure_private_directory(state_root, "operator state root", create=True)
    _ensure_private_directory(state_root / "jobs", "operator jobs root", create=True)
    body = _canonical_bytes({"schema_version": registry.PROFILE_SCHEMA_VERSION, "profiles": []})
    _atomic_write(profiles, body, mode=0o400, replace=False)
    return {
        "schema_version": SERVICE_SCHEMA_VERSION,
        "status": "initialized",
        "workspace": str(root),
        "profiles": str(profiles),
        "state_root": str(state_root),
        "profiles_sha256": hashlib.sha256(body).hexdigest(),
    }


def validate_profiles(*, repo_root: Path, profile_path: Path) -> dict[str, Any]:
    repo = repo_root.resolve(strict=True)
    profile = _validate_private_workspace_path(profile_path, repo, "profile configuration")
    _assert_no_symlink_components(profile, "profile configuration", allow_missing_leaf=False)
    profile_set = registry.load_profiles(profile)
    prepared: list[dict[str, Any]] = []
    for row in profile_set.profiles:
        command = registry.prepare_profile(profile_set, row.profile_id, repo)
        prepared.append(
            {
                "profile_id": row.profile_id,
                "action_id": command.action.action_id,
                "effect": command.action.effect,
                "resource": command.action.resource,
                "supervisor": command.action.supervisor,
                "timeout_seconds": command.action.timeout_seconds,
                "memory_max_bytes": command.action.memory_max_bytes,
                "tasks_max": command.action.tasks_max,
                "file_size_max_bytes": command.action.file_size_max_bytes,
                "stop_timeout_seconds": command.action.stop_timeout_seconds,
                "memory_swap_max_bytes": command.action.memory_swap_max_bytes,
                "entrypoint_sha256": command.entrypoint_sha256,
                "entrypoint_byte_count": command.entrypoint_byte_count,
            }
        )
    return {
        "schema_version": SERVICE_SCHEMA_VERSION,
        "status": "validated",
        "profile_set_sha256": profile_set.raw_sha256,
        "profile_count": len(prepared),
        "profiles": prepared,
        "subprocess_executed": False,
        "files_written": False,
    }


def _prepared_identity(command: registry.PreparedCommand) -> tuple[Any, ...]:
    return (
        command.profile.profile_id,
        command.profile.action_id,
        tuple(command.argv),
        str(command.cwd),
        tuple(sorted(command.environment.items())),
        str(command.entrypoint_path),
        command.entrypoint_sha256,
        command.entrypoint_byte_count,
        command.profile_set_sha256,
        command.action.supervisor,
        command.action.memory_max_bytes,
        command.action.tasks_max,
        command.action.file_size_max_bytes,
        command.action.stop_timeout_seconds,
        command.action.memory_swap_max_bytes,
        command.action.timeout_seconds,
        command.action.resource,
    )


def _process_start_ticks(pid: int) -> int | None:
    """Read Linux process start ticks without treating a PID as durable identity."""

    try:
        body = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        close = body.rfind(")")
        if close < 0:
            return None
        fields = body[close + 2 :].split()
        # Field 22 overall; fields begins at field 3 after the parenthesized comm.
        return int(fields[19])
    except (OSError, UnicodeError, ValueError, IndexError):
        return None


def _same_live_process(pid: Any, start_ticks: Any) -> bool:
    return (
        isinstance(pid, int)
        and not isinstance(pid, bool)
        and pid > 1
        and isinstance(start_ticks, int)
        and not isinstance(start_ticks, bool)
        and start_ticks >= 0
        and _process_start_ticks(pid) == start_ticks
    )


def _systemd_unit_name(job_id: str) -> str:
    if not JOB_ID_RE.fullmatch(job_id):
        raise ServiceError("invalid_state", "cannot derive a unit from an invalid job ID")
    return f"himr-operator-job-{job_id[4:]}.service"


def _systemd_control_environment() -> dict[str, str]:
    """Return the closed environment used only to contact this user's manager."""

    runtime = f"/run/user/{os.geteuid()}"
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TZ": "UTC",
        "XDG_RUNTIME_DIR": runtime,
        "DBUS_SESSION_BUS_ADDRESS": f"unix:path={runtime}/bus",
    }


def _bounded_systemd_text(body: bytes, label: str) -> str:
    if len(body) > SYSTEMD_OUTPUT_LIMIT:
        raise ServiceError("supervisor_failed", f"{label} exceeded its output cap")
    try:
        value = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ServiceError("supervisor_failed", f"{label} was not UTF-8") from error
    if "\x00" in value:
        raise ServiceError("supervisor_failed", f"{label} contained a NUL byte")
    return value


class OperatorService:
    """Thread-safe operational service over one exact closed profile set."""

    def __init__(self, *, repo_root: Path, profile_path: Path, state_root: Path):
        self.repo_root = repo_root.resolve(strict=True)
        self.profile_path = _validate_private_workspace_path(
            profile_path, self.repo_root, "profile configuration"
        )
        _assert_no_symlink_components(
            self.profile_path, "profile configuration", allow_missing_leaf=False
        )
        self.state_root = _validate_private_workspace_path(
            state_root, self.repo_root, "operator state root"
        )
        self.state_root = _ensure_private_directory(
            self.state_root, "operator state root", create=True
        )
        self.jobs_root = _ensure_private_directory(
            self.state_root / "jobs", "operator jobs root", create=True
        )
        self._lock_fd = _open_private_lock(self.state_root / "service.lock")
        self._mutex = threading.RLock()
        self._closed = False
        self._preparations: dict[str, Preparation] = {}
        self._commands: dict[str, registry.PreparedCommand] = {}
        self._armed_autonomy: dict[str, ControllerConfig] = {}
        self._threads: dict[str, threading.Thread] = {}
        self.jobs: dict[str, dict[str, Any]] = {}
        try:
            root_names = {entry.name for entry in self.state_root.iterdir()}
            if not root_names <= {"jobs", "service.lock", "service.json"}:
                raise ServiceError(
                    "invalid_state", "operator state root contains an unexpected entry"
                )
            self.profile_set = registry.load_profiles(self.profile_path)
            # Validate every configured profile at startup so the UI never advertises a
            # profile whose path, parameters, or entrypoint already fail closed.
            self._startup_commands = {
                profile.profile_id: registry.prepare_profile(
                    self.profile_set, profile.profile_id, self.repo_root
                )
                for profile in self.profile_set.profiles
            }
            self.revision = self._load_service_revision()
            self._load_jobs()
            self._persist_service_locked()
        except Exception:
            self.close()
            raise

    @property
    def cancellation_supported(self) -> bool:
        return any(
            action.enabled and action.supervisor == "systemd_user"
            for action in registry.ACTIONS.values()
        )

    def _service_path(self) -> Path:
        return self.state_root / "service.json"

    def _job_dir(self, job_id: str) -> Path:
        if not JOB_ID_RE.fullmatch(job_id):
            raise ServiceError("unknown_job", "job ID is invalid", status=404)
        return self.jobs_root / job_id

    def _job_record_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / "record.json"

    def _load_service_revision(self) -> int:
        path = self._service_path()
        if not path.exists() and not path.is_symlink():
            return 0
        value = _strict_json_bytes(
            _stable_private_read(path, maximum=MAX_RECORD_BYTES), "service state"
        )
        if (
            not isinstance(value, dict)
            or set(value) != {
                "schema_version",
                "revision",
                "profile_set_sha256",
                "updated_at",
            }
            or type(value.get("schema_version")) is not int
            or value["schema_version"] not in {1, SERVICE_SCHEMA_VERSION}
            or isinstance(value.get("revision"), bool)
            or not isinstance(value.get("revision"), int)
            or not 0 <= value["revision"] <= 2**63 - 1
            or not isinstance(value.get("profile_set_sha256"), str)
            or not SHA256_RE.fullmatch(value["profile_set_sha256"])
            or not _is_utc_timestamp(value.get("updated_at"))
        ):
            raise ServiceError("invalid_state", "service state has an invalid shape")
        return value["revision"]

    def _persist_service_locked(self) -> None:
        document = {
            "schema_version": SERVICE_SCHEMA_VERSION,
            "revision": self.revision,
            "profile_set_sha256": self.profile_set.raw_sha256,
            "updated_at": _utc_now(),
        }
        _atomic_write(self._service_path(), _canonical_bytes(document))

    def _load_jobs(self) -> None:
        entries = list(self.jobs_root.iterdir())
        if len(entries) > MAX_JOBS:
            raise ServiceError("invalid_state", f"job count exceeds {MAX_JOBS}")
        highest_revision = self.revision
        detached: list[str] = []
        supervised: list[str] = []
        restart_changed: list[str] = []
        for path in entries:
            if path.name.startswith("."):
                raise ServiceError("invalid_state", "jobs root contains an unexpected temporary")
            if not JOB_ID_RE.fullmatch(path.name):
                raise ServiceError("invalid_state", "jobs root contains an unexpected entry")
            _ensure_private_directory(path, f"job directory {path.name}", create=False)
            allowed = {"record.json", "stdout.log", "stderr.log"}
            names = {entry.name for entry in path.iterdir()}
            if "record.json" not in names or not names <= allowed:
                raise ServiceError("invalid_state", f"job {path.name} has unexpected files")
            value = _strict_json_bytes(
                _stable_private_read(path / "record.json", maximum=MAX_RECORD_BYTES),
                f"job {path.name}",
            )
            self._validate_loaded_job(value, path.name)
            record = self._normalize_loaded_job(value)
            self._validate_loaded_log_files(record, path, names)
            highest_revision = max(highest_revision, record["updated_revision"])
            if record["state"] in ACTIVE_JOB_STATES:
                if record["supervisor"] == "systemd_user":
                    desired = (
                        "cancelling"
                        if record["cancellation_requested_at"] is not None
                        else "reconciling"
                    )
                    if record["state"] != desired:
                        record["state"] = desired
                        record["error"] = (
                            "Console restarted; reconciling the persisted exact transient "
                            "unit and its cancellation state."
                        )
                        restart_changed.append(path.name)
                    supervised.append(path.name)
                elif _same_live_process(record["pid"], record["process_start_ticks"]):
                    record["state"] = "detached_running"
                    record["error"] = (
                        "Console restarted while this exact process leader remained live; "
                        "output capture and cancellation are unavailable."
                    )
                    detached.append(path.name)
                else:
                    record["state"] = "indeterminate_after_restart"
                    record["completed_at"] = _utc_now()
                    record["error"] = (
                        "Console restarted without an observable exact process leader; "
                        "run the underlying pipeline validator to establish receipt state."
                    )
                    restart_changed.append(path.name)
                self.jobs[path.name] = record
            else:
                self.jobs[path.name] = record
        self.revision = highest_revision
        # Persist restart classifications under new revisions.
        for job_id in restart_changed:
            self._commit_job_locked(self.jobs[job_id])
        for job_id in detached:
            thread = threading.Thread(
                target=self._monitor_detached_job,
                args=(job_id,),
                name=f"himr-operator-detached-{job_id[-8:]}",
                daemon=True,
            )
            self._threads[job_id] = thread
            thread.start()
        for job_id in supervised:
            thread = threading.Thread(
                target=self._monitor_systemd_job,
                args=(job_id,),
                name=f"himr-operator-systemd-{job_id[-8:]}",
                daemon=True,
            )
            self._threads[job_id] = thread
            thread.start()

    @staticmethod
    def _normalize_loaded_job(value: Mapping[str, Any]) -> dict[str, Any]:
        record = dict(value)
        if record["schema_version"] == 1:
            record.update(
                schema_version=SERVICE_SCHEMA_VERSION,
                supervisor="direct",
                unit=None,
                cancellation_requested_at=None,
            )
        return record

    @staticmethod
    def _validate_loaded_job(value: Any, expected_id: str) -> None:
        required_v1 = {
            "schema_version",
            "job_id",
            "profile_id",
            "action_id",
            "label",
            "effect",
            "resource",
            "state",
            "created_at",
            "started_at",
            "completed_at",
            "timeout_seconds",
            "deadline_exceeded",
            "cancellation_supported",
            "pid",
            "process_start_ticks",
            "returncode",
            "error",
            "summary",
            "logs",
            "command",
            "updated_revision",
        }
        if not isinstance(value, dict) or type(value.get("schema_version")) is not int:
            raise ServiceError("invalid_state", f"job {expected_id} record has invalid shape")
        schema_version = value["schema_version"]
        required = required_v1 | (
            {"supervisor", "unit", "cancellation_requested_at"}
            if schema_version == SERVICE_SCHEMA_VERSION
            else set()
        )
        if schema_version not in {1, SERVICE_SCHEMA_VERSION} or set(value) != required:
            raise ServiceError("invalid_state", f"job {expected_id} record has invalid shape")
        optional_time = lambda item: item is None or _is_utc_timestamp(item)
        optional_int = lambda item: item is None or (
            isinstance(item, int) and not isinstance(item, bool)
        )
        if (
            value.get("job_id") != expected_id
            or not isinstance(value.get("profile_id"), str)
            or not registry.PROFILE_ID_RE.fullmatch(value["profile_id"])
            or not isinstance(value.get("action_id"), str)
            or not HISTORICAL_ACTION_ID_RE.fullmatch(value["action_id"])
            or not _is_bounded_clean_text(value.get("label"), 300)
            or value.get("effect") not in {"inspect", "execute"}
            or not isinstance(value.get("resource"), str)
            or not RESOURCE_ID_RE.fullmatch(value["resource"])
            or value.get("state") not in ACTIVE_JOB_STATES | TERMINAL_JOB_STATES
            or not optional_time(value.get("created_at"))
            or value.get("created_at") is None
            or not optional_time(value.get("started_at"))
            or not optional_time(value.get("completed_at"))
            or isinstance(value.get("timeout_seconds"), bool)
            or not isinstance(value.get("timeout_seconds"), int)
            or not 1
            <= value["timeout_seconds"]
            <= registry.MAX_ACTION_TIMEOUT_SECONDS
            or not isinstance(value.get("deadline_exceeded"), bool)
            or not isinstance(value.get("cancellation_supported"), bool)
            or not optional_int(value.get("pid"))
            or (value.get("pid") is not None and not 1 < value["pid"] <= 2**31 - 1)
            or not optional_int(value.get("process_start_ticks"))
            or (
                value.get("process_start_ticks") is not None
                and not 0 <= value["process_start_ticks"] <= 2**63 - 1
            )
            or not optional_int(value.get("returncode"))
            or (
                value.get("returncode") is not None
                and not -(2**31) <= value["returncode"] <= 2**31 - 1
            )
            or not (
                value.get("error") is None
                or _is_bounded_clean_text(value.get("error"), 1000, nonempty=False)
            )
            or not (value.get("summary") is None or isinstance(value.get("summary"), dict))
            or isinstance(value.get("updated_revision"), bool)
            or not isinstance(value.get("updated_revision"), int)
            or not 0 <= value["updated_revision"] <= 2**63 - 1
        ):
            raise ServiceError("invalid_state", f"job {expected_id} record is invalid")
        if schema_version == 1:
            if value["cancellation_supported"] is not False:
                raise ServiceError("invalid_state", f"job {expected_id} legacy record is invalid")
        else:
            supervisor = value.get("supervisor")
            unit = value.get("unit")
            cancellation_requested_at = value.get("cancellation_requested_at")
            valid_unit = (
                isinstance(unit, dict)
                and set(unit) == {"name", "invocation_id"}
                and isinstance(unit.get("name"), str)
                and SYSTEMD_UNIT_RE.fullmatch(unit["name"])
                and unit["name"] == _systemd_unit_name(expected_id)
                and (
                    unit.get("invocation_id") is None
                    or isinstance(unit.get("invocation_id"), str)
                    and INVOCATION_ID_RE.fullmatch(unit["invocation_id"])
                )
            )
            if (
                supervisor not in {"direct", "systemd_user"}
                or not (
                    cancellation_requested_at is None
                    or _is_utc_timestamp(cancellation_requested_at)
                )
                or (
                    supervisor == "direct"
                    and (
                        unit is not None
                        or cancellation_requested_at is not None
                        or value["cancellation_supported"] is not False
                    )
                )
                or (
                    supervisor == "systemd_user"
                    and (
                        not valid_unit
                        or value["cancellation_supported"] is not True
                        or value["resource"] not in {"gpu", "autonomous_pipeline"}
                        or value["pid"] is not None
                        or value["process_start_ticks"] is not None
                    )
                )
                or (
                    cancellation_requested_at is not None
                    and value["state"]
                    not in {"cancelling", "cancelled_reconciliation_required"}
                )
            ):
                raise ServiceError(
                    "invalid_state", f"job {expected_id} supervisor metadata is invalid"
                )
        try:
            summary_size = (
                0 if value["summary"] is None else len(_canonical_bytes(value["summary"]))
            )
        except (TypeError, ValueError, RecursionError) as error:
            raise ServiceError(
                "invalid_state", f"job {expected_id} summary cannot be serialized"
            ) from error
        if summary_size > MAX_SUMMARY_BYTES:
            raise ServiceError("invalid_state", f"job {expected_id} summary exceeds its cap")
        if value["state"] in ACTIVE_JOB_STATES and (
            value["completed_at"] is not None or value["returncode"] is not None
        ):
            raise ServiceError("invalid_state", f"job {expected_id} active state is inconsistent")
        if value["state"] in TERMINAL_JOB_STATES and value["completed_at"] is None:
            raise ServiceError("invalid_state", f"job {expected_id} terminal state is incomplete")
        if value["state"] == "succeeded" and (
            value["started_at"] is None
            or value["returncode"] != 0
            or not isinstance(value["summary"], dict)
        ):
            raise ServiceError("invalid_state", f"job {expected_id} success record is inconsistent")
        logs = value.get("logs")
        if not isinstance(logs, dict) or set(logs) != {"stdout", "stderr"}:
            raise ServiceError("invalid_state", f"job {expected_id} logs are invalid")
        for stream, row in logs.items():
            if (
                not isinstance(row, dict)
                or set(row)
                != {
                    "available",
                    "byte_count",
                    "captured_byte_count",
                    "sha256",
                    "truncated",
                }
                or not isinstance(row.get("available"), bool)
                or not isinstance(row.get("truncated"), bool)
                or isinstance(row.get("byte_count"), bool)
                or not isinstance(row.get("byte_count"), int)
                or not 0 <= row["byte_count"] <= 2**63 - 1
                or isinstance(row.get("captured_byte_count"), bool)
                or not isinstance(row.get("captured_byte_count"), int)
                or not 0 <= row["captured_byte_count"] <= MAX_STREAM_BYTES
                or row["captured_byte_count"] > row["byte_count"]
                or not (
                    row.get("sha256") is None
                    or isinstance(row.get("sha256"), str)
                    and SHA256_RE.fullmatch(row["sha256"])
                )
                or (row["available"] and row["sha256"] is None)
                or (
                    not row["available"]
                    and (
                        row["byte_count"] != 0
                        or row["captured_byte_count"] != 0
                        or row["sha256"] is not None
                        or row["truncated"]
                    )
                )
                or (
                    row["available"]
                    and not row["truncated"]
                    and row["byte_count"] != row["captured_byte_count"]
                )
                or (
                    row["available"]
                    and row["truncated"]
                    and (
                        row["captured_byte_count"] != MAX_STREAM_BYTES
                        or row["byte_count"] <= row["captured_byte_count"]
                    )
                )
            ):
                raise ServiceError(
                    "invalid_state", f"job {expected_id} {stream} metadata is invalid"
                )
        if value["state"] in {"succeeded", "failed"} and not all(
            logs[stream]["available"] for stream in ("stdout", "stderr")
        ):
            raise ServiceError("invalid_state", f"job {expected_id} terminal logs are incomplete")
        command = value.get("command")
        if (
            not isinstance(command, dict)
            or set(command)
            != {
                "argv",
                "cwd",
                "environment",
                "profile_set_sha256",
                "entrypoint_path",
                "entrypoint_sha256",
                "entrypoint_byte_count",
                "shell",
            }
            or not isinstance(command.get("argv"), list)
            or not 1 <= len(command["argv"]) <= 128
            or any(
                not _is_bounded_clean_text(item, 16_384)
                or len(item.encode("utf-8")) > 16_384
                for item in command["argv"]
            )
            or sum(len(item.encode("utf-8")) for item in command["argv"]) > 256 * 1024
            or not _is_bounded_clean_text(command.get("cwd"), 4096)
            or not os.path.isabs(command["cwd"])
            or not _is_bounded_clean_text(command.get("entrypoint_path"), 4096)
            or not os.path.isabs(command["entrypoint_path"])
            or command["argv"][0] != command["entrypoint_path"]
            or not isinstance(command.get("environment"), dict)
            or len(command["environment"]) > 32
            or any(
                not isinstance(key, str)
                or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", key)
                or not _is_bounded_clean_text(item, 4096, nonempty=False)
                or len(item.encode("utf-8")) > 4096
                for key, item in command["environment"].items()
            )
            or not isinstance(command.get("profile_set_sha256"), str)
            or not SHA256_RE.fullmatch(command["profile_set_sha256"])
            or not isinstance(command.get("entrypoint_sha256"), str)
            or not SHA256_RE.fullmatch(command["entrypoint_sha256"])
            or isinstance(command.get("entrypoint_byte_count"), bool)
            or not isinstance(command.get("entrypoint_byte_count"), int)
            or not 1 <= command["entrypoint_byte_count"] <= 4 * 1024 * 1024
            or command.get("shell") is not False
        ):
            raise ServiceError("invalid_state", f"job {expected_id} command record is invalid")

    @staticmethod
    def _validate_loaded_log_files(
        record: Mapping[str, Any], job_dir: Path, names: set[str]
    ) -> None:
        for stream in ("stdout", "stderr"):
            name = f"{stream}.log"
            metadata = record["logs"][stream]
            present = name in names
            if metadata["available"] and not present:
                raise ServiceError(
                    "invalid_state", f"job {record['job_id']} {stream} log is missing"
                )
            if present:
                _inspect_private_file(
                    job_dir / name,
                    maximum=MAX_STREAM_BYTES,
                    expected_size=(
                        metadata["captured_byte_count"] if metadata["available"] else None
                    ),
                )

    def _commit_job_locked(self, record: dict[str, Any]) -> None:
        self.revision += 1
        record["updated_revision"] = self.revision
        _atomic_write(self._job_record_path(record["job_id"]), _canonical_bytes(record))
        self.jobs[record["job_id"]] = record
        self._persist_service_locked()

    def _new_job_record(
        self, command: registry.PreparedCommand, job_id: str
    ) -> dict[str, Any]:
        systemd_user = command.action.supervisor == "systemd_user"
        return {
            "schema_version": SERVICE_SCHEMA_VERSION,
            "job_id": job_id,
            "profile_id": command.profile.profile_id,
            "action_id": command.action.action_id,
            "label": command.action.label,
            "effect": command.action.effect,
            "resource": command.action.resource,
            "state": "launching",
            "created_at": _utc_now(),
            "started_at": None,
            "completed_at": None,
            "timeout_seconds": command.action.timeout_seconds,
            "deadline_exceeded": False,
            "cancellation_supported": systemd_user,
            "pid": None,
            "process_start_ticks": None,
            "returncode": None,
            "error": None,
            "summary": None,
            "supervisor": command.action.supervisor,
            "unit": (
                {"name": _systemd_unit_name(job_id), "invocation_id": None}
                if systemd_user
                else None
            ),
            "cancellation_requested_at": None,
            "logs": {
                "stdout": {
                    "available": False,
                    "byte_count": 0,
                    "captured_byte_count": 0,
                    "sha256": None,
                    "truncated": False,
                },
                "stderr": {
                    "available": False,
                    "byte_count": 0,
                    "captured_byte_count": 0,
                    "sha256": None,
                    "truncated": False,
                },
            },
            "command": {
                "argv": list(command.argv),
                "cwd": str(command.cwd),
                "environment": dict(command.environment),
                "profile_set_sha256": command.profile_set_sha256,
                "entrypoint_path": str(command.entrypoint_path),
                "entrypoint_sha256": command.entrypoint_sha256,
                "entrypoint_byte_count": command.entrypoint_byte_count,
                "shell": False,
            },
            "updated_revision": self.revision,
        }

    def _check_revision_locked(self, expected_revision: Any) -> None:
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 0
        ):
            raise ServiceError("invalid_request", "expected_revision must be a nonnegative integer")
        if expected_revision != self.revision:
            raise StaleRevision(self.revision)

    def _reload_exact_command(
        self, expected: registry.PreparedCommand
    ) -> registry.PreparedCommand:
        try:
            profile_set = registry.load_profiles(self.profile_path)
            if profile_set.raw_sha256 != self.profile_set.raw_sha256:
                raise ServiceError(
                    "profile_changed",
                    "profile configuration changed after console startup; restart after review",
                    status=409,
                )
            current = registry.prepare_profile(
                profile_set, expected.profile.profile_id, self.repo_root
            )
        except registry.RegistryError as error:
            raise ServiceError("profile_revalidation_failed", str(error), status=409) from error
        if _prepared_identity(current) != _prepared_identity(expected):
            raise ServiceError(
                "command_changed",
                "profile, entrypoint, or command binding changed after preparation",
                status=409,
            )
        return current

    def _expire_preparations_locked(self) -> None:
        now = time.monotonic()
        expired = [
            token
            for token, preparation in self._preparations.items()
            if preparation.expires_monotonic <= now
        ]
        for token in expired:
            self._preparations.pop(token, None)

    def prepare(self, *, profile_id: Any, expected_revision: Any) -> dict[str, Any]:
        if not isinstance(profile_id, str) or not registry.PROFILE_ID_RE.fullmatch(profile_id):
            raise ServiceError("unknown_profile", "profile ID is invalid", status=404)
        with self._mutex:
            self._check_revision_locked(expected_revision)
            self._expire_preparations_locked()
            matches = [
                profile
                for profile in self.profile_set.profiles
                if profile.profile_id == profile_id
            ]
            if len(matches) != 1:
                raise ServiceError("unknown_profile", "profile is not registered", status=404)
            expected = self._startup_commands[profile_id]
            command = self._reload_exact_command(expected)
            token = secrets.token_urlsafe(32)
            preparation = Preparation(
                token=token,
                profile_id=profile_id,
                command=command,
                expires_at=_utc_after(PREPARATION_LIFETIME_SECONDS),
                expires_monotonic=time.monotonic() + PREPARATION_LIFETIME_SECONDS,
            )
            self._preparations[token] = preparation
            self.revision += 1
            self._persist_service_locked()
            return self._public_preparation(preparation)

    @staticmethod
    def _public_preparation(preparation: Preparation) -> dict[str, Any]:
        command = preparation.command
        return {
            "preparation_token": preparation.token,
            "expires_at": preparation.expires_at,
            "profile_id": command.profile.profile_id,
            "action_id": command.action.action_id,
            "label": command.action.label,
            "effect": command.action.effect,
            "resource": command.action.resource,
            "supervisor": command.action.supervisor,
            "confirmation": command.action.confirmation,
            "timeout_seconds": command.action.timeout_seconds,
            "memory_max_bytes": command.action.memory_max_bytes,
            "tasks_max": command.action.tasks_max,
            "file_size_max_bytes": command.action.file_size_max_bytes,
            "stop_timeout_seconds": command.action.stop_timeout_seconds,
            "memory_swap_max_bytes": command.action.memory_swap_max_bytes,
            "profile_set_sha256": command.profile_set_sha256,
            "entrypoint_sha256": command.entrypoint_sha256,
        }

    def _active_records_locked(self) -> list[dict[str, Any]]:
        return [record for record in self.jobs.values() if record["state"] in ACTIVE_JOB_STATES]

    def _assert_capacity_locked(self, resource: str) -> None:
        active = self._active_records_locked()
        if len(active) >= GLOBAL_JOB_LIMIT:
            raise ServiceError(
                "capacity_busy", f"global job capacity {GLOBAL_JOB_LIMIT} is occupied", status=409
            )
        requested_claims = _resource_claims(resource)
        for record in active:
            if requested_claims & _resource_claims(record["resource"]):
                raise ServiceError(
                    "resource_busy",
                    f"resource group {resource!r} already has an active job conflict "
                    f"with {record['resource']!r}",
                    status=409,
                )

    def _autonomy_start_config(
        self, command: registry.PreparedCommand
    ) -> ControllerConfig | None:
        """Resolve only the one closed autonomous Start action to its sealed config."""

        if command.action.action_id != AUTONOMY_START_ACTION_ID:
            return None
        action = command.action
        expected_fields = (
            ("config", "--config", "input_file", True, None, None),
            (
                "expected_config_sha256",
                "--expected-config-sha256",
                "sha256",
                True,
                None,
                None,
            ),
        )
        observed_fields = tuple(
            (
                field.name,
                field.flag,
                field.kind,
                field.required,
                field.minimum,
                field.maximum,
            )
            for field in action.fields
        )
        expected_entrypoint = self.repo_root / AUTONOMY_START_ENTRYPOINT
        if (
            registry.ACTIONS.get(AUTONOMY_START_ACTION_ID) != action
            or action.effect != "execute"
            or action.resource != "autonomous_pipeline"
            or action.launcher != "repo"
            or action.entrypoint != AUTONOMY_START_ENTRYPOINT
            or action.prefix != ("run",)
            or observed_fields != expected_fields
            or action.confirmation is not None
            or action.timeout_seconds != registry.MAX_ACTION_TIMEOUT_SECONDS
            or not action.enabled
            or action.blocked_reason is not None
            or action.supervisor != "systemd_user"
            or action.memory_max_bytes != 16 * 1024**3
            or action.tasks_max != 256
            or action.file_size_max_bytes != 64 * 1024**3
            or action.stop_timeout_seconds != 30
            or action.memory_swap_max_bytes != 0
            or action.entrypoint_parameter is not None
            or command.cwd != self.repo_root
            or command.entrypoint_path != expected_entrypoint
            or len(command.argv) != 6
            or command.argv[0] != str(expected_entrypoint)
            or command.argv[1] != "run"
            or command.argv[2] != "--config"
            or command.argv[4] != "--expected-config-sha256"
            or SHA256_RE.fullmatch(command.argv[5]) is None
        ):
            raise ServiceError(
                "invalid_autonomy_start",
                "autonomous Start command differs from its closed action binding",
            )
        config_path = Path(command.argv[3])
        if not config_path.is_absolute():
            raise ServiceError(
                "invalid_autonomy_start",
                "autonomous Start config path is not absolute",
            )
        try:
            config_path.relative_to(self.repo_root)
        except ValueError as error:
            raise ServiceError(
                "invalid_autonomy_start",
                "autonomous Start config escaped the repository boundary",
            ) from error
        try:
            return load_config(config_path, command.argv[5])
        except Exception as error:
            raise ServiceError(
                "autonomy_start_arm_failed",
                f"autonomous Start config replay failed: {type(error).__name__}: {error}",
            ) from error

    @staticmethod
    def _arm_autonomy(config: ControllerConfig | None) -> None:
        if config is None:
            return
        try:
            request_start(config)
        except Exception as error:
            recovery_error: Exception | None = None
            try:
                request_stop(config)
            except Exception as recovery:
                recovery_error = recovery
            detail = f"{type(error).__name__}: {error}"
            if recovery_error is not None:
                detail += (
                    "; fail-closed stop restoration also failed: "
                    f"{type(recovery_error).__name__}: {recovery_error}"
                )
            raise ServiceError(
                "autonomy_start_arm_failed",
                f"autonomous Start intent was not safely committed: {detail}",
            ) from error

    @staticmethod
    def _disarm_autonomy(config: ControllerConfig | None) -> None:
        if config is not None:
            request_stop(config)

    def _fail_unlaunched_job_locked(
        self,
        record: dict[str, Any],
        error: Exception,
        autonomy_config: ControllerConfig | None,
    ) -> ServiceError:
        """Stop an armed controller before releasing an unlaunched job claim."""

        message = f"{type(error).__name__}: {error}"[:900]
        try:
            self._disarm_autonomy(autonomy_config)
        except Exception as disarm_error:
            record["state"] = "reconciling"
            record["error"] = (
                "No transient unit was launched, but controller stop intent could not "
                "be restored; autonomous capacity remains held: "
                f"{type(disarm_error).__name__}: {disarm_error}"
            )[:1000]
            self._commit_job_locked(record)
            return ServiceError(
                "autonomy_start_disarm_failed",
                record["error"],
            )
        record["state"] = "failed"
        record["completed_at"] = _utc_now()
        record["error"] = f"Worker launch was refused before process admission: {message}"
        self._commit_job_locked(record)
        self._commands.pop(record["job_id"], None)
        self._armed_autonomy.pop(record["job_id"], None)
        return ServiceError(
            "worker_launch_failed",
            record["error"],
        )

    def execute(
        self,
        *,
        preparation_token: Any,
        expected_revision: Any,
        confirmation: Any,
    ) -> dict[str, Any]:
        if not isinstance(preparation_token, str) or not TOKEN_RE.fullmatch(preparation_token):
            raise ServiceError("invalid_preparation", "preparation token is invalid", status=404)
        with self._mutex:
            self._check_revision_locked(expected_revision)
            self._expire_preparations_locked()
            preparation = self._preparations.pop(preparation_token, None)
            if preparation is None:
                raise ServiceError(
                    "invalid_preparation", "preparation is absent, expired, or already consumed", status=404
                )
            expected_confirmation = preparation.command.action.confirmation
            if expected_confirmation is None:
                if confirmation is not None:
                    raise ServiceError(
                        "invalid_confirmation", "inspect action confirmation must be null"
                    )
            elif not isinstance(confirmation, str) or not hmac.compare_digest(
                confirmation, expected_confirmation
            ):
                raise ServiceError(
                    "invalid_confirmation", "confirmation phrase does not match the prepared action"
                )
            command = self._reload_exact_command(preparation.command)
            self._assert_capacity_locked(command.action.resource)
            if len(self.jobs) >= MAX_JOBS:
                raise ServiceError("capacity_busy", f"job history reached {MAX_JOBS}", status=409)
            job_id = f"job_{secrets.token_hex(16)}"
            job_dir = self.jobs_root / job_id
            old_umask = os.umask(0o077)
            try:
                job_dir.mkdir(mode=0o700)
            except OSError as error:
                raise ServiceError("state_write_failed", f"cannot create job state: {error}") from error
            finally:
                os.umask(old_umask)
            _ensure_private_directory(job_dir, "job directory", create=False)
            if command.action.supervisor == "systemd_user":
                try:
                    for stream in ("stdout", "stderr"):
                        _atomic_write(
                            job_dir / f"{stream}.log",
                            b"",
                            mode=0o600,
                            replace=False,
                        )
                except Exception:
                    with contextlib.suppress(OSError):
                        for stream in ("stdout", "stderr"):
                            (job_dir / f"{stream}.log").unlink(missing_ok=True)
                        job_dir.rmdir()
                    raise
            record = self._new_job_record(command, job_id)
            self._commands[job_id] = command
            autonomy_config: ControllerConfig | None = None
            try:
                # The launching record and empty systemd logs become durable before
                # start intent. A persistence failure therefore cannot strand a
                # running intent with no worker or restart reconciliation record.
                self._commit_job_locked(record)
                autonomy_config = self._autonomy_start_config(command)
                self._arm_autonomy(autonomy_config)
            except Exception as error:
                if job_id not in self.jobs:
                    self._commands.pop(job_id, None)
                    with contextlib.suppress(OSError):
                        for stream in ("stdout", "stderr"):
                            (job_dir / f"{stream}.log").unlink(missing_ok=True)
                        job_dir.rmdir()
                    raise
                raise self._fail_unlaunched_job_locked(
                    record,
                    error,
                    autonomy_config,
                ) from error
            if autonomy_config is not None:
                self._armed_autonomy[job_id] = autonomy_config
            thread = threading.Thread(
                target=self._run_job,
                args=(job_id,),
                name=f"himr-operator-job-{job_id[-8:]}",
                daemon=True,
            )
            self._threads[job_id] = thread
            try:
                thread.start()
            except Exception as error:
                self._threads.pop(job_id, None)
                raise self._fail_unlaunched_job_locked(
                    record, error, autonomy_config
                ) from error
            return self._public_job_locked(record, prefix="")

    def cancel(
        self, *, job_id: Any, expected_revision: Any, confirmation: Any = None
    ) -> dict[str, Any]:
        with self._mutex:
            self._check_revision_locked(expected_revision)
            if not isinstance(job_id, str) or job_id not in self.jobs:
                raise ServiceError("unknown_job", "job is not registered", status=404)
            record = self.jobs[job_id]
            if record["supervisor"] != "systemd_user":
                raise ServiceError("cancellation_disabled", CANCELLATION_REASON, status=409)
            if record["state"] not in ACTIVE_JOB_STATES:
                raise ServiceError("job_not_active", "job is no longer active", status=409)
            if record["state"] == "launching":
                raise ServiceError(
                    "cancellation_not_ready",
                    "the transient unit has not yet been accepted; refresh and retry",
                    status=409,
                )
            if not isinstance(confirmation, str) or not hmac.compare_digest(
                confirmation, CANCEL_CONFIRMATION
            ):
                raise ServiceError(
                    "invalid_confirmation",
                    "cancellation confirmation phrase does not match",
                )
            unit = record["unit"]["name"]
            record["state"] = "cancelling"
            record["cancellation_requested_at"] = _utc_now()
            record["error"] = "Exact-unit control-group cancellation requested."
            self._commit_job_locked(record)

        try:
            preflight = self._query_systemd_unit(unit)
        except ServiceError:
            with self._mutex:
                current = self.jobs[job_id]
                if current["state"] == "cancelling":
                    current["state"] = "reconciling"
                    current["cancellation_requested_at"] = None
                    current["error"] = (
                        "Cancellation preflight could not verify the exact unit; no stop "
                        "command was sent."
                    )
                    self._commit_job_locked(current)
            raise
        if not self._bind_systemd_invocation(job_id, preflight):
            raise ServiceError(
                "unit_identity_changed",
                "transient-unit identity changed; cancellation was refused",
                status=409,
            )
        if preflight.invocation_id is None:
            with self._mutex:
                current = self.jobs[job_id]
                if current["state"] == "cancelling":
                    current["state"] = "reconciling"
                    current["cancellation_requested_at"] = None
                    current["error"] = (
                        "Cancellation is waiting for a manager-issued InvocationID; no "
                        "stop command was sent."
                    )
                    self._commit_job_locked(current)
            raise ServiceError(
                "cancellation_not_ready",
                "the exact transient-unit InvocationID is not available yet",
                status=409,
            )
        if preflight.absent or not preflight.process_live:
            with self._mutex:
                current = self.jobs[job_id]
                if current["state"] == "cancelling":
                    current["state"] = "reconciling"
                    current["cancellation_requested_at"] = None
                    current["error"] = "Unit exited before cancellation; reconciling its result."
                    self._commit_job_locked(current)
            self._complete_systemd_job(job_id, snapshot=preflight)
            with self._mutex:
                return self._public_job_locked(self.jobs[job_id], prefix="")

        try:
            self._stop_systemd_unit(unit)
        except ServiceError:
            with self._mutex:
                current = self.jobs[job_id]
                if current["state"] == "cancelling":
                    current["state"] = "reconciling"
                    current["cancellation_requested_at"] = None
                    current["error"] = (
                        "Exact-unit stop was not accepted; unit reconciliation continues."
                    )
                    self._commit_job_locked(current)
            raise

        try:
            snapshot = self._query_systemd_unit(unit)
        except ServiceError:
            # A successful, synchronous `systemctl stop` is the process-tree boundary.
            # Receipt/output reconciliation is still explicitly required.
            self._complete_systemd_job(
                job_id,
                snapshot=None,
                forced_state="cancelled_reconciliation_required",
                forced_error=(
                    "The exact unit stop succeeded, but final unit metadata was unavailable; "
                    "run the pipeline validator before resuming."
                ),
            )
        else:
            if self._bind_systemd_invocation(job_id, snapshot):
                if snapshot.process_live:
                    self._update_live_systemd_job(job_id, snapshot)
                else:
                    self._complete_systemd_job(
                        job_id,
                        snapshot=snapshot,
                        forced_state="cancelled_reconciliation_required",
                        forced_error=(
                            "The systemd cgroup was stopped by operator request; run the "
                            "pipeline validator before resuming."
                        ),
                    )
        with self._mutex:
            return self._public_job_locked(self.jobs[job_id], prefix="")

    @staticmethod
    def _systemd_control(argv: list[str], *, timeout: int) -> subprocess.CompletedProcess[bytes]:
        if not argv or argv[0] not in {SYSTEMD_RUN, SYSTEMCTL}:
            raise ServiceError("supervisor_failed", "systemd control argv is not fixed")
        try:
            completed = subprocess.run(
                argv,
                cwd="/",
                env=_systemd_control_environment(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                close_fds=True,
                start_new_session=True,
                umask=0o077,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise ServiceError("supervisor_timeout", "systemd control command timed out") from error
        except OSError as error:
            raise ServiceError(
                "supervisor_unavailable", f"cannot execute the fixed systemd control tool: {error}"
            ) from error
        _bounded_systemd_text(completed.stdout, "systemd stdout")
        _bounded_systemd_text(completed.stderr, "systemd stderr")
        return completed

    def _query_systemd_unit(self, unit: str) -> SystemdUnitState:
        if not SYSTEMD_UNIT_RE.fullmatch(unit):
            raise ServiceError("invalid_state", "persisted transient unit name is invalid")
        argv = [
            SYSTEMCTL,
            "--user",
            "--no-pager",
            "--no-ask-password",
            "show",
            f"--property={','.join(SYSTEMD_PROPERTIES)}",
            unit,
        ]
        completed = self._systemd_control(
            argv, timeout=SYSTEMD_CONTROL_TIMEOUT_SECONDS
        )
        text = _bounded_systemd_text(completed.stdout, "systemctl show output")
        values: dict[str, str] = {}
        for line in text.splitlines():
            if not line or "=" not in line:
                raise ServiceError("supervisor_failed", "systemctl show output is malformed")
            key, value = line.split("=", 1)
            if key not in SYSTEMD_PROPERTIES or key in values:
                raise ServiceError("supervisor_failed", "systemctl show fields are not closed")
            if len(value) > 128 or any(ord(character) < 32 for character in value):
                raise ServiceError("supervisor_failed", "systemctl show value is invalid")
            values[key] = value
        if set(values) != set(SYSTEMD_PROPERTIES):
            raise ServiceError("supervisor_failed", "systemctl show omitted required fields")
        if completed.returncode != 0 and values["LoadState"] != "not-found":
            detail = _bounded_systemd_text(completed.stderr, "systemctl show error").strip()
            raise ServiceError(
                "supervisor_unavailable",
                f"systemctl show failed for the exact unit{': ' + detail[:300] if detail else ''}",
            )

        def optional_integer(name: str) -> int | None:
            raw = values[name]
            if raw == "":
                return None
            if not raw.isascii() or not raw.isdigit():
                raise ServiceError("supervisor_failed", f"systemctl {name} is not an integer")
            result = int(raw)
            if not 0 <= result <= 2**31 - 1:
                raise ServiceError("supervisor_failed", f"systemctl {name} is out of range")
            return result

        invocation_id = values["InvocationID"] or None
        if invocation_id is not None and not INVOCATION_ID_RE.fullmatch(invocation_id):
            raise ServiceError("supervisor_failed", "systemctl InvocationID is invalid")
        for name in ("LoadState", "ActiveState", "SubState", "Result"):
            if not re.fullmatch(r"[a-z0-9_-]{0,64}", values[name]):
                raise ServiceError("supervisor_failed", f"systemctl {name} is invalid")
        return SystemdUnitState(
            load_state=values["LoadState"],
            active_state=values["ActiveState"],
            sub_state=values["SubState"],
            result=values["Result"],
            exec_main_code=optional_integer("ExecMainCode"),
            exec_main_status=optional_integer("ExecMainStatus"),
            invocation_id=invocation_id,
        )

    def _stop_systemd_unit(self, unit: str) -> None:
        if not SYSTEMD_UNIT_RE.fullmatch(unit):
            raise ServiceError("invalid_state", "persisted transient unit name is invalid")
        completed = self._systemd_control(
            [
                SYSTEMCTL,
                "--user",
                "--no-ask-password",
                "stop",
                unit,
            ],
            timeout=SYSTEMD_CONTROL_TIMEOUT_SECONDS,
        )
        if completed.returncode != 0:
            detail = _bounded_systemd_text(completed.stderr, "systemctl stop error").strip()
            raise ServiceError(
                "cancellation_failed",
                f"systemctl did not stop the exact unit{': ' + detail[:300] if detail else ''}",
                status=409,
            )

    @staticmethod
    def _systemd_run_argv(
        command: registry.PreparedCommand, job_id: str, job_dir: Path
    ) -> list[str]:
        if command.action.supervisor != "systemd_user":
            raise ServiceError("invalid_state", "direct action reached systemd argv builder")
        memory_max = command.action.memory_max_bytes
        if isinstance(memory_max, bool) or not isinstance(memory_max, int):
            raise ServiceError("invalid_state", "systemd action has no sealed MemoryMax")
        tasks_max = command.action.tasks_max or SYSTEMD_DEFAULT_TASKS_MAX
        file_size_max = (
            command.action.file_size_max_bytes or SYSTEMD_CHILD_MAX_FILE_BYTES
        )
        stop_timeout = (
            command.action.stop_timeout_seconds
            or SYSTEMD_DEFAULT_STOP_TIMEOUT_SECONDS
        )
        memory_swap_max = (
            SYSTEMD_DEFAULT_MEMORY_SWAP_MAX_BYTES
            if command.action.memory_swap_max_bytes is None
            else command.action.memory_swap_max_bytes
        )
        unit = _systemd_unit_name(job_id)
        stdout_path = job_dir / "stdout.log"
        stderr_path = job_dir / "stderr.log"
        for label, path in (
            ("working directory", command.cwd),
            ("stdout path", stdout_path),
            ("stderr path", stderr_path),
        ):
            rendered = str(path)
            if not _is_bounded_clean_text(rendered, 4096) or "%" in rendered:
                raise ServiceError(
                    "unsafe_path",
                    f"systemd {label} contains an unsafe control or specifier character",
                )
        child_environment = dict(command.environment)
        if command.action.resource == "autonomous_pipeline":
            # GPU child units bind PartOf/BindsTo to this exact outer unit.  The
            # console supplies the unique name after allocating the job ID; the
            # scrubbed controller resolves that live unit's manager InvocationID,
            # MainPID, and cgroup before it can launch a GPU child.
            child_environment["HIMR_AUTONOMY_OUTER_UNIT"] = unit
        environment = [
            f"{key}={value}" for key, value in sorted(child_environment.items())
        ]
        return [
            SYSTEMD_RUN,
            "--user",
            "--no-block",
            "--quiet",
            "--no-ask-password",
            f"--unit={unit}",
            "--property=Type=exec",
            "--property=ExitType=cgroup",
            "--property=Restart=no",
            "--property=KillMode=control-group",
            f"--property=RuntimeMaxSec={command.action.timeout_seconds}s",
            f"--property=TimeoutStopSec={stop_timeout}s",
            "--property=SendSIGKILL=yes",
            "--property=OOMPolicy=stop",
            f"--property=TasksMax={tasks_max}",
            "--property=LimitNOFILE=1024",
            f"--property=LimitFSIZE={file_size_max}",
            "--property=LimitCORE=0",
            f"--property=MemoryMax={memory_max}",
            f"--property=MemorySwapMax={memory_swap_max}",
            "--property=UMask=0077",
            "--property=StandardInput=null",
            f"--property=StandardOutput=append:{stdout_path}",
            f"--property=StandardError=append:{stderr_path}",
            "--property=RemainAfterExit=yes",
            f"--working-directory={command.cwd}",
            "--",
            ENV,
            "-i",
            *environment,
            *command.argv,
        ]

    def _bind_systemd_invocation(
        self, job_id: str, snapshot: SystemdUnitState
    ) -> bool:
        if snapshot.absent or snapshot.invocation_id is None:
            return True
        with self._mutex:
            record = self.jobs[job_id]
            expected = record["unit"]["invocation_id"]
            if expected is not None and not hmac.compare_digest(
                expected, snapshot.invocation_id
            ):
                if record["state"] in ACTIVE_JOB_STATES:
                    record["state"] = "reconciling"
                    record["cancellation_requested_at"] = None
                    record["error"] = (
                        "Transient-unit InvocationID changed; automatic completion and "
                        "cancellation are refused."
                    )
                    self._commit_job_locked(record)
                return False
            if expected is None:
                record["unit"]["invocation_id"] = snapshot.invocation_id
                self._commit_job_locked(record)
        return True

    def _update_live_systemd_job(
        self, job_id: str, snapshot: SystemdUnitState
    ) -> None:
        with self._mutex:
            record = self.jobs[job_id]
            if record["state"] not in ACTIVE_JOB_STATES:
                return
            desired = (
                "cancelling"
                if record["cancellation_requested_at"] is not None
                else "running"
            )
            changed = record["state"] != desired or record["started_at"] is None
            record["state"] = desired
            if record["started_at"] is None:
                record["started_at"] = _utc_now()
            if record["error"] is not None and desired == "running":
                record["error"] = None
                changed = True
            if changed:
                self._commit_job_locked(record)

    def _systemd_log_results(self, job_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for stream in ("stdout", "stderr"):
            body = _stable_private_read(
                self._job_dir(job_id) / f"{stream}.log", maximum=MAX_STREAM_BYTES
            )
            results.append(
                {"body": body, "byte_count": len(body), "truncated": False}
            )
        return results[0], results[1]

    def _complete_systemd_job(
        self,
        job_id: str,
        *,
        snapshot: SystemdUnitState | None,
        forced_state: str | None = None,
        forced_error: str | None = None,
    ) -> None:
        with self._mutex:
            record = self.jobs[job_id]
            if record["state"] not in ACTIVE_JOB_STATES:
                return
            if forced_state is None and record["cancellation_requested_at"] is not None:
                forced_state = "cancelled_reconciliation_required"
                forced_error = (
                    "The systemd cgroup was stopped by operator request; run the pipeline "
                    "validator before resuming."
                )
        retired_before_logs = False
        if (
            snapshot is not None
            and not snapshot.absent
            and snapshot.active_state in SYSTEMD_LIVE_ACTIVE_STATES
        ):
            # RemainAfterExit preserves main-process status across a console crash,
            # but a child could still linger in the cgroup after the main process
            # exits. Stop the exact unit before sealing logs or releasing capacity.
            self._stop_systemd_unit(_systemd_unit_name(job_id))
            retired_before_logs = True
        stdout_result, stderr_result = self._systemd_log_results(job_id)
        returncode = snapshot.returncode if snapshot is not None else None
        if forced_state is None and (snapshot is None or snapshot.absent or returncode is None):
            forced_state = "indeterminate_after_restart"
            forced_error = (
                "The exact transient unit is absent or lacks a durable exit status; run the "
                "pipeline validator to establish receipt state."
            )
        self._finish_job(
            job_id,
            returncode=returncode,
            stdout_result=stdout_result,
            stderr_result=stderr_result,
            forced_state=forced_state,
            forced_error=forced_error,
            deadline_exceeded=bool(snapshot and snapshot.deadline_exceeded),
        )
        if snapshot is not None and not snapshot.absent and not retired_before_logs:
            # Retire RemainAfterExit units only after terminal metadata and logs are
            # durable. Failure is harmless; the unique name is never reused.
            with contextlib.suppress(ServiceError):
                self._stop_systemd_unit(_systemd_unit_name(job_id))

    def _mark_systemd_reconciling(self, job_id: str, message: str) -> None:
        with self._mutex:
            record = self.jobs.get(job_id)
            if record is None or record["state"] not in ACTIVE_JOB_STATES:
                return
            desired = "cancelling" if record["cancellation_requested_at"] else "reconciling"
            bounded = message[:1000]
            if record["state"] != desired or record["error"] != bounded:
                record["state"] = desired
                record["error"] = bounded
                self._commit_job_locked(record)

    def _persisted_autonomy_config(self, job_id: str) -> ControllerConfig | None:
        """Replay an active persisted Start binding for absent-unit reconciliation."""

        with self._mutex:
            record = self.jobs.get(job_id)
            if record is None or record["action_id"] != AUTONOMY_START_ACTION_ID:
                return None
            expected = self._startup_commands.get(record["profile_id"])
            if expected is None:
                raise ServiceError(
                    "autonomy_reconciliation_failed",
                    "persisted autonomous Start profile is no longer registered",
                )
            matches = self._autonomy_job_matches_command(record, expected)
        if not matches:
            raise ServiceError(
                "autonomy_reconciliation_failed",
                "persisted autonomous Start command differs from its exact profile",
            )
        current = self._reload_exact_command(expected)
        return self._autonomy_start_config(current)

    @staticmethod
    def _autonomy_job_matches_command(
        record: Mapping[str, Any], expected: registry.PreparedCommand
    ) -> bool:
        """Bind a persisted managed job to the currently sealed Start profile."""

        persisted = record.get("command")
        return bool(
            isinstance(persisted, dict)
            and record.get("profile_id") == expected.profile.profile_id
            and record.get("action_id") == expected.action.action_id
            and record.get("resource") == expected.action.resource
            and record.get("supervisor") == expected.action.supervisor
            and record.get("timeout_seconds") == expected.action.timeout_seconds
            and persisted.get("argv") == list(expected.argv)
            and persisted.get("cwd") == str(expected.cwd)
            and persisted.get("environment") == dict(expected.environment)
            and persisted.get("profile_set_sha256") == expected.profile_set_sha256
            and persisted.get("entrypoint_path") == str(expected.entrypoint_path)
            and persisted.get("entrypoint_sha256") == expected.entrypoint_sha256
            and persisted.get("entrypoint_byte_count")
            == expected.entrypoint_byte_count
        )

    def _monitor_systemd_job(self, job_id: str, *, initial_launch: bool = False) -> None:
        absent_grace = 10 if initial_launch else 0
        while True:
            with self._mutex:
                if self._closed:
                    return
                record = self.jobs.get(job_id)
                if record is None or record["state"] not in ACTIVE_JOB_STATES:
                    return
                unit = record["unit"]["name"]
            try:
                snapshot = self._query_systemd_unit(unit)
            except ServiceError as error:
                self._mark_systemd_reconciling(
                    job_id, f"Transient-unit reconciliation failed: {error}"
                )
                time.sleep(SYSTEMD_POLL_SECONDS)
                continue
            if not self._bind_systemd_invocation(job_id, snapshot):
                time.sleep(SYSTEMD_POLL_SECONDS)
                continue
            if snapshot.absent and absent_grace > 0:
                absent_grace -= 1
                time.sleep(SYSTEMD_POLL_SECONDS)
                continue
            if snapshot.absent:
                try:
                    self._disarm_autonomy(
                        self._persisted_autonomy_config(job_id)
                    )
                except Exception as error:
                    self._mark_systemd_reconciling(
                        job_id,
                        "Absent autonomous unit could not restore durable stop intent: "
                        f"{type(error).__name__}: {error}",
                    )
                    time.sleep(SYSTEMD_POLL_SECONDS)
                    continue
            if snapshot.process_live:
                self._update_live_systemd_job(job_id, snapshot)
                time.sleep(SYSTEMD_POLL_SECONDS)
                continue
            try:
                self._complete_systemd_job(job_id, snapshot=snapshot)
            except ServiceError as error:
                self._mark_systemd_reconciling(
                    job_id, f"Terminal unit metadata could not be sealed: {error}"
                )
                time.sleep(SYSTEMD_POLL_SECONDS)
                continue
            return

    def _run_systemd_job(self, job_id: str) -> None:
        unit_accepted = False
        retain_failed_arm = False
        try:
            job_dir = self._job_dir(job_id)
            with self._mutex:
                command = self._commands[job_id]
            command = self._reload_exact_command(command)
            argv = self._systemd_run_argv(command, job_id, job_dir)
            completed = self._systemd_control(
                argv, timeout=SYSTEMD_CONTROL_TIMEOUT_SECONDS
            )
            if completed.returncode != 0:
                detail = _bounded_systemd_text(
                    completed.stderr, "systemd-run error"
                ).strip()
                raise ServiceError(
                    "supervisor_launch_failed",
                    f"systemd-run rejected the sealed unit{': ' + detail[:300] if detail else ''}",
                )
            unit_accepted = True
            with self._mutex:
                record = self.jobs[job_id]
                record["state"] = "reconciling"
                record["started_at"] = _utc_now()
                record["error"] = "Transient unit accepted; awaiting exact unit metadata."
                self._commit_job_locked(record)
            self._monitor_systemd_job(job_id, initial_launch=True)
        except Exception as error:
            message = (
                f"Systemd-user launch or supervision failed: "
                f"{type(error).__name__}: {error}"
            )[:1000]
            if unit_accepted:
                # Never release resource capacity merely because the console-side
                # monitor failed while the manager may still own a live cgroup.
                self._mark_systemd_reconciling(job_id, message)
            else:
                with self._mutex:
                    autonomy_config = self._armed_autonomy.get(job_id)
                try:
                    self._disarm_autonomy(autonomy_config)
                except Exception as disarm_error:
                    retain_failed_arm = True
                    self._mark_systemd_reconciling(
                        job_id,
                        message
                        + "; controller stop intent could not be restored before "
                        + "releasing autonomous capacity: "
                        + f"{type(disarm_error).__name__}: {disarm_error}",
                    )
                    return
                with contextlib.suppress(Exception):
                    stdout_result, stderr_result = self._systemd_log_results(job_id)
                    self._finish_job(
                        job_id,
                        returncode=None,
                        stdout_result=stdout_result,
                        stderr_result=stderr_result,
                        forced_state="failed",
                        forced_error=message,
                    )
                with self._mutex:
                    record = self.jobs.get(job_id)
                    if record is not None and record["state"] in ACTIVE_JOB_STATES:
                        record["state"] = "failed"
                        record["completed_at"] = _utc_now()
                        record["error"] = message
                        self._commit_job_locked(record)
        finally:
            with self._mutex:
                if not retain_failed_arm:
                    self._commands.pop(job_id, None)
                    self._armed_autonomy.pop(job_id, None)

    @staticmethod
    def _drain_stream(stream: Any, result: dict[str, Any]) -> None:
        captured = bytearray()
        total = 0
        try:
            while True:
                block = stream.read(READ_CHUNK_BYTES)
                if not block:
                    break
                total += len(block)
                if len(captured) < MAX_STREAM_BYTES:
                    remaining = MAX_STREAM_BYTES - len(captured)
                    captured.extend(block[:remaining])
        finally:
            with contextlib.suppress(Exception):
                stream.close()
        result["body"] = bytes(captured)
        result["byte_count"] = total
        result["truncated"] = total > len(captured)

    def _run_job(self, job_id: str) -> None:
        with self._mutex:
            command = self._commands[job_id]
        if command.action.supervisor == "systemd_user":
            self._run_systemd_job(job_id)
        else:
            self._run_direct_job(job_id)

    def _run_direct_job(self, job_id: str) -> None:
        command: registry.PreparedCommand | None = None
        process: subprocess.Popen[bytes] | None = None
        detached_monitor: threading.Thread | None = None
        stdout_result: dict[str, Any] = {}
        stderr_result: dict[str, Any] = {}
        try:
            with self._mutex:
                command = self._commands[job_id]
            # The execution thread repeats the complete profile and entrypoint replay
            # immediately before Popen, closing request/thread scheduling drift.
            command = self._reload_exact_command(command)
            process = subprocess.Popen(
                list(command.argv),
                cwd=command.cwd,
                env=dict(command.environment),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                close_fds=True,
                start_new_session=True,
                umask=0o077,
            )
            assert process.stdout is not None and process.stderr is not None
            with self._mutex:
                record = self.jobs[job_id]
                record["state"] = "running"
                record["started_at"] = _utc_now()
                record["pid"] = process.pid
                record["process_start_ticks"] = _process_start_ticks(process.pid)
                self._commit_job_locked(record)
            stdout_thread = threading.Thread(
                target=self._drain_stream,
                args=(process.stdout, stdout_result),
                daemon=True,
            )
            stderr_thread = threading.Thread(
                target=self._drain_stream,
                args=(process.stderr, stderr_result),
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()
            deadline = time.monotonic() + command.action.timeout_seconds
            deadline_recorded = False
            while process.poll() is None:
                if not deadline_recorded and time.monotonic() >= deadline:
                    deadline_recorded = True
                    with self._mutex:
                        record = self.jobs[job_id]
                        record["deadline_exceeded"] = True
                        record["error"] = (
                            "Configured deadline exceeded; direct supervisor did not send an "
                            "unsafe partial-tree signal."
                        )
                        self._commit_job_locked(record)
                time.sleep(0.1)
            returncode = process.wait()
            stdout_thread.join()
            stderr_thread.join()
            self._finish_job(
                job_id,
                returncode=returncode,
                stdout_result=stdout_result,
                stderr_result=stderr_result,
            )
        except Exception as error:
            # Never expose an exception as success.  Popen failures have no child to
            # cancel; if a child existed, this path records uncertainty without a signal.
            if process is not None and process.poll() is None:
                state = "detached_running"
                completed_at = None
                message = (
                    f"Supervisor failed while the exact process leader remained live: "
                    f"{type(error).__name__}"
                )
            else:
                state = "failed"
                completed_at = _utc_now()
                message = f"Job launch or supervision failed: {type(error).__name__}: {error}"
            with contextlib.suppress(Exception):
                self._write_log_result(job_id, "stdout", stdout_result)
                self._write_log_result(job_id, "stderr", stderr_result)
            with self._mutex:
                record = self.jobs[job_id]
                record["state"] = state
                record["completed_at"] = completed_at
                record["returncode"] = process.poll() if process is not None else None
                record["error"] = message[:1000]
                self._refresh_log_metadata(
                    record,
                    job_id,
                    {"stdout": stdout_result, "stderr": stderr_result},
                )
                self._commit_job_locked(record)
                if state == "detached_running":
                    detached_monitor = threading.Thread(
                        target=self._monitor_detached_job,
                        args=(job_id,),
                        name=f"himr-operator-detached-{job_id[-8:]}",
                        daemon=True,
                    )
                    self._threads[job_id] = detached_monitor
            if detached_monitor is not None:
                detached_monitor.start()
        finally:
            with self._mutex:
                self._commands.pop(job_id, None)

    def _write_log_result(self, job_id: str, stream: str, result: Mapping[str, Any]) -> None:
        body = result.get("body", b"")
        if not isinstance(body, bytes):
            body = b""
        _atomic_write(self._job_dir(job_id) / f"{stream}.log", body)

    def _refresh_log_metadata(
        self,
        record: dict[str, Any],
        job_id: str,
        results: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        for stream in ("stdout", "stderr"):
            path = self._job_dir(job_id) / f"{stream}.log"
            if not path.exists() or path.is_symlink():
                continue
            body = _stable_private_read(path, maximum=MAX_STREAM_BYTES)
            prior = record["logs"][stream]
            result = (results or {}).get(stream, {})
            reported_total = result.get("byte_count", len(body))
            if (
                isinstance(reported_total, bool)
                or not isinstance(reported_total, int)
                or reported_total < len(body)
                or reported_total > 2**63 - 1
            ):
                reported_total = len(body)
            truncated = (
                len(body) == MAX_STREAM_BYTES
                and reported_total > len(body)
                and bool(result.get("truncated"))
            )
            if not truncated:
                # A supervision exception can race a still-draining reader. Preserve
                # only the exact atomically captured prefix, not an unstable total.
                reported_total = len(body)
            prior.update(
                available=True,
                byte_count=reported_total,
                captured_byte_count=len(body),
                sha256=hashlib.sha256(body).hexdigest(),
                truncated=truncated,
            )

    @staticmethod
    def _parse_result_summary(body: bytes) -> tuple[dict[str, Any] | None, str | None]:
        try:
            value = _strict_json_bytes(body, "job output")
        except ServiceError as error:
            return None, str(error)
        if not isinstance(value, dict):
            return None, "job output top level is not a JSON object"
        canonical = _canonical_bytes(value)
        if len(canonical) > MAX_SUMMARY_BYTES:
            return (
                {
                    "omitted": True,
                    "reason": "parsed JSON summary exceeds public state cap",
                    "canonical_byte_count": len(canonical),
                    "canonical_sha256": hashlib.sha256(canonical).hexdigest(),
                },
                None,
            )
        return value, None

    def _finish_job(
        self,
        job_id: str,
        *,
        returncode: int | None,
        stdout_result: Mapping[str, Any],
        stderr_result: Mapping[str, Any],
        forced_state: str | None = None,
        forced_error: str | None = None,
        deadline_exceeded: bool = False,
    ) -> None:
        self._write_log_result(job_id, "stdout", stdout_result)
        self._write_log_result(job_id, "stderr", stderr_result)
        stdout_body = stdout_result.get("body", b"")
        stderr_body = stderr_result.get("body", b"")
        stdout_truncated = bool(stdout_result.get("truncated"))
        stderr_truncated = bool(stderr_result.get("truncated"))
        chosen = stdout_body if returncode in {0, None} else stderr_body
        summary, summary_error = self._parse_result_summary(
            chosen if isinstance(chosen, bytes) else b""
        )
        if forced_state is not None:
            if forced_state not in TERMINAL_JOB_STATES:
                raise ServiceError("invalid_state", "forced terminal job state is invalid")
            outcome = forced_state
            error = forced_error
            if summary_error is not None:
                summary = None
        elif stdout_truncated or stderr_truncated:
            outcome = "failed"
            error = "stdout or stderr exceeded the 8 MiB capture cap"
        elif returncode != 0:
            outcome = "failed"
            error = f"subprocess exited with status {returncode}"
            if summary_error:
                error += "; failure output was not a strict JSON object"
        elif summary_error is not None:
            outcome = "failed"
            error = f"exit-zero subprocess did not emit one strict JSON object: {summary_error}"
        else:
            outcome = "succeeded"
            error = None
        with self._mutex:
            record = self.jobs[job_id]
            record["state"] = outcome
            record["completed_at"] = _utc_now()
            record["returncode"] = returncode
            record["error"] = error
            record["summary"] = summary
            record["deadline_exceeded"] = (
                record["deadline_exceeded"] or deadline_exceeded
            )
            for stream, result in (("stdout", stdout_result), ("stderr", stderr_result)):
                body = result.get("body", b"")
                record["logs"][stream].update(
                    available=True,
                    byte_count=int(result.get("byte_count", 0)),
                    captured_byte_count=len(body) if isinstance(body, bytes) else 0,
                    sha256=(
                        hashlib.sha256(body).hexdigest()
                        if isinstance(body, bytes)
                        else hashlib.sha256(b"").hexdigest()
                    ),
                    truncated=bool(result.get("truncated")),
                )
            self._commit_job_locked(record)

    def _monitor_detached_job(self, job_id: str) -> None:
        while True:
            with self._mutex:
                record = self.jobs.get(job_id)
                if record is None or record["state"] != "detached_running":
                    return
                pid = record["pid"]
                ticks = record["process_start_ticks"]
            if not _same_live_process(pid, ticks):
                with self._mutex:
                    record = self.jobs[job_id]
                    if record["state"] == "detached_running":
                        record["state"] = "indeterminate_after_restart"
                        record["completed_at"] = _utc_now()
                        record["error"] = (
                            "Detached process leader exited without an observed return code; "
                            "run the underlying pipeline validator to establish receipt state."
                        )
                        self._commit_job_locked(record)
                return
            time.sleep(1.0)

    def _capacity_locked(self) -> dict[str, Any]:
        active = self._active_records_locked()
        resources = sorted(
            {action.resource for action in registry.ACTIONS.values() if action.enabled}
            | {record["resource"] for record in active}
        )
        active_count = len(active)
        reservation_rows: dict[str, dict[str, Any]] = {}
        for resource in resources:
            claims = _resource_claims(resource)
            conflicting_job_count = sum(
                bool(_resource_claims(row["resource"]) & claims) for row in active
            )
            reservation_rows[resource] = {
                # ``limit`` and ``active_count`` are retained for older console
                # clients.  Despite the historical name, active_count is the
                # number of active *console jobs that conflict with admission*;
                # it is not runtime device or lane utilization.
                "limit": 1,
                "active_count": conflicting_job_count,
                "conflicting_job_count": conflicting_job_count,
                "blocked": conflicting_job_count >= 1,
                "claim_ids": sorted(claims),
            }
        return {
            "kind": "operator_console_admission_reservations",
            "semantics": "launch_admission_not_runtime_utilization",
            # Preserve the original top-level fields while publishing their
            # explicit console-job meaning for newer clients.
            "global_limit": GLOBAL_JOB_LIMIT,
            "active_count": active_count,
            "global": {
                "limit": GLOBAL_JOB_LIMIT,
                "active_job_count": active_count,
                "available_slot_count": max(0, GLOBAL_JOB_LIMIT - active_count),
            },
            "resources": reservation_rows,
        }

    def _public_job_locked(self, record: Mapping[str, Any], *, prefix: str) -> dict[str, Any]:
        logs: dict[str, Any] = {}
        for stream in ("stdout", "stderr"):
            item = dict(record["logs"][stream])
            item["base_url"] = (
                f"{prefix}api/jobs/{record['job_id']}/logs/{stream}/"
                if item["available"]
                else None
            )
            logs[stream] = item
        return {
            "job_id": record["job_id"],
            "profile_id": record["profile_id"],
            "action_id": record["action_id"],
            "label": record["label"],
            "effect": record["effect"],
            "resource": record["resource"],
            "supervisor": record.get("supervisor", "direct"),
            "state": record["state"],
            "created_at": record["created_at"],
            "started_at": record["started_at"],
            "completed_at": record["completed_at"],
            "timeout_seconds": record["timeout_seconds"],
            "deadline_exceeded": record["deadline_exceeded"],
            "cancellation_supported": (
                record.get("supervisor") == "systemd_user"
                and record["state"] in ACTIVE_JOB_STATES
                and record["state"] != "launching"
                and record.get("unit", {}).get("invocation_id") is not None
            ),
            "cancellation_confirmation": (
                CANCEL_CONFIRMATION
                if record.get("supervisor") == "systemd_user"
                and record["state"] in ACTIVE_JOB_STATES
                and record["state"] != "launching"
                and record.get("unit", {}).get("invocation_id") is not None
                else None
            ),
            "returncode": record["returncode"],
            "error": record["error"],
            "summary": record["summary"],
            "logs": logs,
        }

    def _reconcile_terminal_autonomy_status_locked(
        self,
        status: dict[str, Any],
        command: registry.PreparedCommand,
    ) -> dict[str, Any]:
        """Project an exact terminal managed job over a stale active cache.

        A controller can fail while rebuilding after it has observed a durable Stop
        request but before it gets a chance to publish its final ``status.json``.
        The cached status then remains ``running`` indefinitely even though the
        console's exact systemd cgroup has a durable terminal result.  Permit a new
        Start only when one currently sealed Start command, one terminal managed
        job, and the cached status timestamps all bind to the same execution.
        """

        if (
            status.get("desired_state") != "stopped"
            or status.get("actual_state")
            not in {"starting", "running", "retrying", "stopping"}
        ):
            return status
        start_jobs = [
            record
            for record in self.jobs.values()
            if record.get("action_id") == AUTONOMY_START_ACTION_ID
        ]
        active_jobs = [
            record
            for record in self.jobs.values()
            if record.get("state") in ACTIVE_JOB_STATES
        ]
        autonomous_claims = _resource_claims(command.action.resource)
        if (
            not start_jobs
            or any(
                record.get("action_id")
                in {AUTONOMY_START_ACTION_ID, "autonomy.request_stop"}
                or bool(
                    _resource_claims(str(record.get("resource", "")))
                    & autonomous_claims
                )
                for record in active_jobs
            )
        ):
            return status
        latest = max(
            start_jobs,
            key=lambda record: (
                record["created_at"],
                record["updated_revision"],
                record["job_id"],
            ),
        )
        unit = latest.get("unit")
        job_created_at = latest.get("created_at")
        status_updated_at = status.get("updated_at")
        job_started_at = latest.get("started_at")
        job_completed_at = latest.get("completed_at")
        if (
            latest.get("state") not in {"succeeded", "failed"}
            or latest.get("supervisor") != "systemd_user"
            or isinstance(latest.get("returncode"), bool)
            or not isinstance(latest.get("returncode"), int)
            or not isinstance(unit, dict)
            or not isinstance(unit.get("invocation_id"), str)
            or not INVOCATION_ID_RE.fullmatch(unit["invocation_id"])
            or not self._autonomy_job_matches_command(latest, command)
            or not _is_utc_timestamp(job_created_at)
            or not _is_utc_timestamp(status_updated_at)
            or not _is_utc_timestamp(job_started_at)
            or not _is_utc_timestamp(job_completed_at)
            or not job_created_at
            <= job_started_at
            <= status_updated_at
            <= job_completed_at
        ):
            return status

        terminal_projection = (
            "stopped" if latest["state"] == "succeeded" else "faulted"
        )
        reconciled = dict(status)
        reconciled.update(
            lifecycle=terminal_projection,
            actual_state=terminal_projection,
            running=False,
            can_start=True,
            can_stop=False,
        )
        controls = dict(status.get("controls", {}))
        controls.update(can_start=True, can_stop=False)
        reconciled["controls"] = controls
        reconciled["terminal_job_reconciliation"] = {
            "kind": "managed_systemd_terminal_over_stale_controller_cache",
            "job_id": latest["job_id"],
            "job_state": latest["state"],
            "returncode": latest["returncode"],
            "completed_at": job_completed_at,
            "cached_actual_state": status["actual_state"],
            "cached_lifecycle": status.get("lifecycle"),
            "cached_updated_at": status_updated_at,
        }
        return reconciled

    def _public_autonomy_locked(self) -> dict[str, Any] | None:
        commands = [
            command
            for command in self._startup_commands.values()
            if command.action.action_id == "autonomy.run"
        ]
        if not commands:
            return None
        if len(commands) != 1:
            return {
                "lifecycle": "status_unavailable",
                "actual_state": "status_unavailable",
                "last_error": {
                    "type": "ConsoleConfigurationError",
                    "message": "exactly one autonomous start profile is required",
                },
                "errors": [
                    {
                        "type": "ConsoleConfigurationError",
                        "message": "exactly one autonomous start profile is required",
                    }
                ],
            }
        argv = commands[0].argv
        try:
            config_index = argv.index("--config") + 1
            digest_index = argv.index("--expected-config-sha256") + 1
            config_path = Path(argv[config_index])
            expected_sha256 = argv[digest_index]
            from autonomous_controller.public_status import read_public_status

            status = read_public_status(config_path, expected_sha256)
            status = dict(self._reconcile_terminal_autonomy_status_locked(
                status, commands[0]
            ))
            # Companion statistics are advisory and independently available.
            # Failure here must not hide the source controller or its Stop gate.
            try:
                from .longform_statistics import read_longform_statistics

                status["longform"] = read_longform_statistics(config_path, expected_sha256)
            except Exception:
                # Keep the fallback independent even of importing the reader.
                status["longform"] = {
                    "schema_version": 1, "state": "unavailable", "updated_at": None,
                    "lifecycle": None, "counts": None, "completion_percent": None,
                    "basis": "cached_companion_status_completed_recording_jobs",
                    "last_error": None, "diagnostic": "statistics_reader_failure",
                }
            return status
        except Exception as error:
            message = f"{type(error).__name__}: {error}"[:1000]
            return {
                "lifecycle": "status_unavailable",
                "actual_state": "status_unavailable",
                "last_error": {"type": type(error).__name__, "message": str(error)[:900]},
                "errors": [{"type": type(error).__name__, "message": str(error)[:900]}],
                "current_stage": None,
                "updated_at": None,
                "monitor": {},
                "stages": {},
                "progress": {},
                "throughput": {},
                "storage": {},
                "recent_activity": [],
                "current_gpu_child": None,
                "diagnostic": message,
            }

    def public_state(self, *, csrf_token: str, prefix: str) -> dict[str, Any]:
        with self._mutex:
            jobs = sorted(
                self.jobs.values(), key=lambda row: (row["created_at"], row["job_id"]), reverse=True
            )
            visible_jobs = jobs[:PUBLIC_JOB_LIMIT]
            return {
                "schema_version": SERVICE_SCHEMA_VERSION,
                "revision": self.revision,
                "csrf_token": csrf_token,
                "service": {
                    "implementation_version": SERVICE_IMPLEMENTATION_VERSION,
                    "advisory_only": True,
                    "completion_authority": "pipeline_receipts_and_validators",
                    "profile_set_sha256": self.profile_set.raw_sha256,
                    "profile_count": len(self.profile_set.profiles),
                    "cancellation": {
                        "supported": self.cancellation_supported,
                        "reason": CANCELLATION_REASON,
                        "confirmation": CANCEL_CONFIRMATION,
                    },
                },
                "profiles": registry.public_profiles(self.profile_set),
                "actions": registry.public_actions(),
                "blocked_capabilities": list(registry.BLOCKED_CAPABILITIES),
                "autonomy": self._public_autonomy_locked(),
                "capacity": self._capacity_locked(),
                "job_history_count": len(jobs),
                "job_history_truncated": len(jobs) > len(visible_jobs),
                "jobs": [
                    self._public_job_locked(row, prefix=prefix) for row in visible_jobs
                ],
            }

    def public_job(self, job_id: str, *, prefix: str) -> dict[str, Any]:
        with self._mutex:
            record = self.jobs.get(job_id)
            if record is None:
                raise ServiceError("unknown_job", "job is not registered", status=404)
            return self._public_job_locked(record, prefix=prefix)

    def read_log_chunk(self, job_id: str, stream: str, offset: Any) -> dict[str, Any]:
        if stream not in {"stdout", "stderr"}:
            raise ServiceError("unknown_log", "log stream is invalid", status=404)
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ServiceError("invalid_log_offset", "log offset is invalid", status=416)
        with self._mutex:
            record = self.jobs.get(job_id)
            if record is None or not record["logs"][stream]["available"]:
                raise ServiceError("unknown_log", "log is not available", status=404)
            expected = dict(record["logs"][stream])
        captured = expected["captured_byte_count"]
        if offset > captured:
            raise ServiceError(
                "invalid_log_offset", "log offset is beyond captured bytes", status=416
            )
        body = _stable_private_chunk(
            self._job_dir(job_id) / f"{stream}.log",
            offset=offset,
            maximum=LOG_RESPONSE_BYTES,
            expected_size=captured,
        )
        next_offset = offset + len(body)
        return {
            "stream": stream,
            "offset": offset,
            "next_offset": next_offset,
            "eof": next_offset == captured,
            "truncated": bool(expected["truncated"]),
            # Logs are diagnostics. Invalid or boundary-split UTF-8 is represented
            # explicitly rather than making the JSON endpoint undecodable.
            "text": body.decode("utf-8", errors="replace"),
        }

    def wait_for_jobs(self, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            with self._mutex:
                threads = [thread for thread in self._threads.values() if thread.is_alive()]
            if not threads:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            for thread in threads:
                thread.join(timeout=min(0.1, remaining))

    def close(self) -> None:
        with getattr(self, "_mutex", contextlib.nullcontext()):
            if getattr(self, "_closed", True):
                return
            self._closed = True
            descriptor = getattr(self, "_lock_fd", -1)
            self._lock_fd = -1
        if descriptor >= 0:
            with contextlib.suppress(OSError):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    def __enter__(self) -> "OperatorService":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        self.close()
