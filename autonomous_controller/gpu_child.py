"""Durable systemd supervision for one exact local-private GPU batch.

The autonomous controller is intentionally not the GPU worker's resource boundary.
Each batch is admitted through a separate transient user service carrying the same
limits as the reviewed operator-console GPU action.  This module owns only that
manager boundary: it validates a closed launch specification, journals the exact
unit and InvocationID, reconciles result receipts, and stops only a previously
journalled exact unit.

No function in this module discovers batches, downloads media, performs inference,
or grants import/publication/deletion authority.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence


SYSTEMD_RUN = "/usr/bin/systemd-run"
SYSTEMCTL = "/usr/bin/systemctl"
ENV = "/usr/bin/env"

RUNTIME_MAX_SECONDS = 3_600
MEMORY_MAX_BYTES = 12 * 1024**3
MEMORY_SWAP_MAX_BYTES = 0
TASKS_MAX = 64
NOFILE_MAX = 1_024
FILE_SIZE_MAX_BYTES = 32 * 1024**2
STOP_TIMEOUT_SECONDS = 30
CONTROL_TIMEOUT_SECONDS = 15
MAX_SYSTEMD_OUTPUT_BYTES = 64 * 1024
MAX_RECORD_BYTES = 256 * 1024
MAX_CONTROL_FILE_BYTES = 16 * 1024**2
MAX_CHILDREN = 1_000_000
# Startup cancellation needs a latency-bounded certificate, not an unbounded
# historical-journal replay. Larger histories fall back to normal GPU recovery.
STARTUP_TERMINAL_PREFLIGHT_MAX_RECORDS = 4_096

OUTER_UNIT_ENV = "HIMR_AUTONOMY_OUTER_UNIT"
SYSTEMD_INVOCATION_ENV = "INVOCATION_ID"
CONTROLLER_SUPERVISOR_PID_ENV = "HIMR_AUTONOMY_CONTROLLER_SUPERVISOR_PID"

SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
INVOCATION_ID_RE = re.compile(r"[0-9a-f]{32}\Z")
OUTER_UNIT_RE = re.compile(r"himr-operator-job-[0-9a-f]{32}\.service\Z")
CHILD_UNIT_RE = re.compile(
    r"himr-autonomy-gpu-[0-9a-f]{32}-[0-9a-f]{64}-[0-9]{6}\.service\Z"
)
BATCH_ID_RE = re.compile(r"gpuasrbatch2_[0-9a-f]{32}\Z")
UTC_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")

SYSTEMD_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "Result",
    "ExecMainCode",
    "ExecMainStatus",
    "InvocationID",
)
CONTROLLER_CONTEXT_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "InvocationID",
    "MainPID",
    "ControlGroup",
)
SYSTEMD_LIVE_ACTIVE_STATES = {"activating", "active", "reloading", "deactivating"}

CHILD_ENVIRONMENT = {
    "PATH": "/usr/bin:/bin",
    "HOME": "/nonexistent",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "TZ": "UTC",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONNOUSERSITE": "1",
    "HF_HUB_OFFLINE": "1",
    "HF_DATASETS_OFFLINE": "1",
    "HF_HUB_DISABLE_TELEMETRY": "1",
    "DO_NOT_TRACK": "1",
}

RECORD_STATES = {
    "launching",
    "running",
    "retiring",
    "stopping",
    "succeeded",
    "failed",
    "stopped",
    "reconciliation_required",
}
TERMINAL_RECORD_STATES = {
    "succeeded",
    "failed",
    "stopped",
    "reconciliation_required",
}


class GpuChildError(RuntimeError):
    """The exact GPU child could not be safely launched or reconciled."""


class GpuChildReconciliationRequired(GpuChildError):
    """Manager identity or termination could not be proven automatically."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _canonical_bytes(value: Any) -> bytes:
    try:
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
    except (TypeError, ValueError) as error:
        raise GpuChildError(f"value is not canonical JSON: {error}") from error


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _bounded_text(value: Any, label: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or "\x00" in value
        or any(ord(character) < 32 for character in value)
    ):
        raise GpuChildError(f"{label} must be bounded non-control text")
    return value


def _safe_error(value: Any) -> str:
    """Collapse untrusted exception/manager text into one bounded journal line."""

    rendered = " ".join(str(value).split())[:2048]
    return rendered or "unspecified GPU child error"


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise GpuChildError(f"{label} must be a lowercase SHA-256")
    return value


def _clean_path(path: Path, label: str, *, require_exists: bool = True) -> Path:
    if not isinstance(path, Path):
        raise GpuChildError(f"{label} must be a Path")
    rendered = str(path)
    if (
        not path.is_absolute()
        or os.path.normpath(rendered) != rendered
        or rendered == "/"
        or "//" in rendered
        or "\\" in rendered
        or "%" in rendered
        or any(ord(character) < 32 for character in rendered)
        or len(rendered.encode("utf-8")) > 4096
    ):
        raise GpuChildError(f"{label} must be one normalized absolute systemd-safe path")
    if require_exists:
        try:
            if path.resolve(strict=True) != path:
                raise GpuChildError(f"{label} may not traverse a symlink")
        except OSError as error:
            raise GpuChildError(f"cannot resolve {label}: {error}") from error
    return path


def _fingerprint(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
    )


def _stable_sha256(path: Path, expected: str, label: str) -> None:
    expected = _digest(expected, f"expected {label} SHA-256")
    _clean_path(path, label)
    try:
        inspected = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise GpuChildError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o400
            or not 1 <= opened.st_size <= MAX_CONTROL_FILE_BYTES
            or (inspected.st_dev, inspected.st_ino, inspected.st_size, inspected.st_mode)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mode)
        ):
            raise GpuChildError(f"{label} must be current-user, single-link mode 0400")
        digest = hashlib.sha256()
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, min(64 * 1024, opened.st_size - offset), offset)
            if not chunk:
                raise GpuChildError(f"{label} ended during hashing")
            digest.update(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        linked = path.lstat()
        if _fingerprint(opened) != _fingerprint(after) or _fingerprint(after) != _fingerprint(linked):
            raise GpuChildError(f"{label} changed during hashing")
        if digest.hexdigest() != expected:
            raise GpuChildError(f"{label} differs from its exact SHA-256 binding")
    finally:
        os.close(descriptor)


def _private_directory(path: Path, label: str) -> Path:
    _clean_path(path, label)
    try:
        observed = path.lstat()
    except OSError as error:
        raise GpuChildError(f"cannot inspect {label}: {error}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise GpuChildError(f"{label} must be current-user-owned mode 0700")
    return path


def _trees_intersect(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _validate_system_tools() -> None:
    for path in (Path(SYSTEMD_RUN), Path(SYSTEMCTL), Path(ENV)):
        try:
            observed = path.lstat()
        except OSError as error:
            raise GpuChildError(f"required system tool is unavailable: {path}") from error
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != 0
            or observed.st_mode & 0o022
            or not os.access(path, os.X_OK)
        ):
            raise GpuChildError(
                f"system tool must be root-owned, executable, and not writable: {path}"
            )


def _systemd_control_environment() -> dict[str, str]:
    uid = os.geteuid()
    runtime = f"/run/user/{uid}"
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TZ": "UTC",
        "XDG_RUNTIME_DIR": runtime,
        "DBUS_SESSION_BUS_ADDRESS": f"unix:path={runtime}/bus",
    }


@dataclass(frozen=True)
class ControllerUnitContext:
    """Exact outer service identity bound to the controller process."""

    outer_unit: str
    outer_invocation_id: str

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "ControllerUnitContext":
        source = os.environ if environment is None else environment
        return cls(
            outer_unit=source.get(OUTER_UNIT_ENV, ""),
            outer_invocation_id=source.get(SYSTEMD_INVOCATION_ENV, ""),
        ).validated()

    @classmethod
    def from_current_systemd_unit(
        cls,
        environment: Mapping[str, str] | None = None,
        *,
        runner: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
        process_id: int | None = None,
        parent_process_id: int | None = None,
        process_cgroup_path: Path = Path("/proc/self/cgroup"),
    ) -> "ControllerUnitContext":
        """Resolve the scrubbed controller's exact manager identity fail-closed.

        The operator console deliberately launches through ``env -i``, which also
        removes systemd's per-exec ``INVOCATION_ID``.  The unique outer unit name is
        retained, then bound here to this exact process, InvocationID, and cgroup
        using a fixed closed-environment manager query.  A companion supervisor
        may explicitly delegate its exact MainPID to its direct source-controller
        child; an ordinary controller must remain the MainPID itself.
        """

        source = os.environ if environment is None else environment
        outer_unit = source.get(OUTER_UNIT_ENV, "")
        if OUTER_UNIT_RE.fullmatch(outer_unit) is None:
            raise GpuChildError(
                f"{OUTER_UNIT_ENV} is not an exact operator controller service"
            )
        inherited_invocation = source.get(SYSTEMD_INVOCATION_ENV, "")
        if inherited_invocation and INVOCATION_ID_RE.fullmatch(inherited_invocation) is None:
            raise GpuChildError("inherited controller INVOCATION_ID is invalid")
        delegated_supervisor_present = CONTROLLER_SUPERVISOR_PID_ENV in source
        raw_delegated_supervisor_pid = source.get(CONTROLLER_SUPERVISOR_PID_ENV, "")
        delegated_supervisor_pid: int | None = None
        if delegated_supervisor_present:
            if (
                not isinstance(raw_delegated_supervisor_pid, str)
                or not raw_delegated_supervisor_pid.isascii()
                or not raw_delegated_supervisor_pid.isdigit()
                or raw_delegated_supervisor_pid.startswith("0")
            ):
                raise GpuChildError("delegated controller supervisor PID is invalid")
            delegated_supervisor_pid = int(raw_delegated_supervisor_pid)
            if not 1 < delegated_supervisor_pid <= 2**31 - 1:
                raise GpuChildError("delegated controller supervisor PID is invalid")
        expected_pid = os.getpid() if process_id is None else process_id
        if (
            isinstance(expected_pid, bool)
            or not isinstance(expected_pid, int)
            or not 1 < expected_pid <= 2**31 - 1
        ):
            raise GpuChildError("controller process ID is invalid")

        _validate_system_tools()
        argv = [
            SYSTEMCTL,
            "--user",
            "--no-pager",
            "--no-ask-password",
            "show",
            f"--property={','.join(CONTROLLER_CONTEXT_PROPERTIES)}",
            outer_unit,
        ]
        try:
            completed = runner(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                close_fds=True,
                cwd="/",
                env=_systemd_control_environment(),
                timeout=CONTROL_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise GpuChildError("controller systemd identity query timed out") from error
        except OSError as error:
            raise GpuChildError(
                f"cannot query controller systemd identity: {error}"
            ) from error
        for label, body in (("stdout", completed.stdout), ("stderr", completed.stderr)):
            if not isinstance(body, bytes) or len(body) > MAX_SYSTEMD_OUTPUT_BYTES:
                raise GpuChildError(
                    f"controller systemd identity {label} is invalid or exceeds its cap"
                )
            try:
                body.decode("utf-8")
            except UnicodeDecodeError as error:
                raise GpuChildError(
                    f"controller systemd identity {label} is not UTF-8"
                ) from error
            if b"\x00" in body:
                raise GpuChildError(
                    f"controller systemd identity {label} contains a NUL byte"
                )
        if completed.returncode != 0:
            raise GpuChildError("controller systemd identity query failed")

        values: dict[str, str] = {}
        for line in completed.stdout.decode("utf-8").splitlines():
            if not line or "=" not in line:
                raise GpuChildError("controller systemd identity output is malformed")
            key, value = line.split("=", 1)
            if key not in CONTROLLER_CONTEXT_PROPERTIES or key in values:
                raise GpuChildError("controller systemd identity fields are not closed")
            if len(value.encode("utf-8")) > 4096 or any(
                ord(character) < 32 for character in value
            ):
                raise GpuChildError("controller systemd identity value is invalid")
            values[key] = value
        if set(values) != set(CONTROLLER_CONTEXT_PROPERTIES):
            raise GpuChildError("controller systemd identity omitted required fields")
        if (
            values["LoadState"] != "loaded"
            or values["ActiveState"] not in SYSTEMD_LIVE_ACTIVE_STATES
            or re.fullmatch(r"[a-z0-9_-]{1,64}", values["SubState"]) is None
        ):
            raise GpuChildError("controller outer unit is not loaded and process-live")
        invocation = values["InvocationID"]
        if INVOCATION_ID_RE.fullmatch(invocation) is None:
            raise GpuChildError("controller systemd InvocationID is invalid or unavailable")
        if inherited_invocation and inherited_invocation != invocation:
            raise GpuChildError(
                "inherited controller InvocationID differs from the systemd unit"
            )
        raw_main_pid = values["MainPID"]
        if not raw_main_pid.isascii() or not raw_main_pid.isdigit():
            raise GpuChildError("controller systemd MainPID is invalid")
        manager_main_pid = int(raw_main_pid)
        if manager_main_pid == expected_pid:
            if delegated_supervisor_present:
                raise GpuChildError(
                    "controller supervisor delegation is invalid for the outer unit MainPID"
                )
        else:
            if delegated_supervisor_pid is None:
                raise GpuChildError("controller process is not the outer unit MainPID")
            if delegated_supervisor_pid != manager_main_pid:
                raise GpuChildError(
                    "delegated controller supervisor PID differs from the outer unit MainPID"
                )
            actual_parent_pid = (
                os.getppid() if parent_process_id is None else parent_process_id
            )
            if (
                isinstance(actual_parent_pid, bool)
                or not isinstance(actual_parent_pid, int)
                or not 1 < actual_parent_pid <= 2**31 - 1
            ):
                raise GpuChildError("controller direct parent PID is invalid")
            if actual_parent_pid != manager_main_pid:
                raise GpuChildError(
                    "controller direct parent is not the delegated outer unit MainPID"
                )

        manager_cgroup = values["ControlGroup"]
        if (
            not manager_cgroup.startswith("/")
            or os.path.normpath(manager_cgroup) != manager_cgroup
            or "//" in manager_cgroup
        ):
            raise GpuChildError("controller systemd ControlGroup is invalid")
        try:
            cgroup_body = process_cgroup_path.read_bytes()
        except OSError as error:
            raise GpuChildError(f"cannot read controller process cgroup: {error}") from error
        if not 1 <= len(cgroup_body) <= 4096 or b"\x00" in cgroup_body:
            raise GpuChildError("controller process cgroup is invalid or exceeds its cap")
        try:
            cgroup_text = cgroup_body.decode("utf-8")
        except UnicodeDecodeError as error:
            raise GpuChildError("controller process cgroup is not UTF-8") from error
        unified = [line[3:] for line in cgroup_text.splitlines() if line.startswith("0::")]
        if len(unified) != 1 or unified[0] != manager_cgroup:
            raise GpuChildError("controller process cgroup differs from the outer unit")
        return cls(outer_unit, invocation).validated()

    def validated(self) -> "ControllerUnitContext":
        if OUTER_UNIT_RE.fullmatch(self.outer_unit) is None:
            raise GpuChildError(
                f"{OUTER_UNIT_ENV} is not an exact operator controller service"
            )
        if INVOCATION_ID_RE.fullmatch(self.outer_invocation_id) is None:
            raise GpuChildError("controller INVOCATION_ID is invalid or unavailable")
        return self


@dataclass(frozen=True)
class LocalPrivateGpuLaunchSpec:
    """All dynamic and fixed bindings for one local-private GPU attempt."""

    batch_id: str
    batch_manifest: Path
    expected_batch_sha256: str
    runtime_admission: Path
    expected_runtime_admission_sha256: str
    production_profile: Path
    expected_production_profile_sha256: str
    root_registration: Path
    expected_root_registration_sha256: str
    launcher_profile: Path
    expected_launcher_profile_sha256: str
    local_readiness: Path
    expected_local_readiness_sha256: str
    local_launcher: Path
    writable_result_root: Path
    writable_event_root: Path
    writable_lock_root: Path
    working_directory: Path
    attempt_ordinal: int = 1

    def validated(self) -> "LocalPrivateGpuLaunchSpec":
        if not isinstance(self.batch_id, str) or BATCH_ID_RE.fullmatch(self.batch_id) is None:
            raise GpuChildError("batch_id is invalid")
        if (
            isinstance(self.attempt_ordinal, bool)
            or not isinstance(self.attempt_ordinal, int)
            or not 1 <= self.attempt_ordinal <= 999_999
        ):
            raise GpuChildError("attempt_ordinal must be in 1..999999")

        for path, digest, label in (
            (self.batch_manifest, self.expected_batch_sha256, "batch manifest"),
            (self.runtime_admission, self.expected_runtime_admission_sha256, "runtime admission"),
            (self.production_profile, self.expected_production_profile_sha256, "production profile"),
            (self.root_registration, self.expected_root_registration_sha256, "root registration"),
            (self.launcher_profile, self.expected_launcher_profile_sha256, "launcher profile"),
            (self.local_readiness, self.expected_local_readiness_sha256, "local readiness"),
        ):
            _stable_sha256(path, digest, label)

        _clean_path(self.local_launcher, "local-private trusted launcher")
        try:
            launcher = self.local_launcher.lstat()
        except OSError as error:
            raise GpuChildError(f"cannot inspect local-private trusted launcher: {error}") from error
        if (
            not stat.S_ISREG(launcher.st_mode)
            or launcher.st_uid != os.geteuid()
            or launcher.st_nlink != 1
            or stat.S_IMODE(launcher.st_mode) != 0o500
        ):
            raise GpuChildError(
                "local-private trusted launcher must be current-user, single-link mode 0500"
            )

        roots = [
            _private_directory(self.writable_result_root, "GPU result root"),
            _private_directory(self.writable_event_root, "GPU event root"),
            _private_directory(self.writable_lock_root, "GPU lock root"),
        ]
        if any(
            _trees_intersect(left, right)
            for index, left in enumerate(roots)
            for right in roots[index + 1 :]
        ):
            raise GpuChildError("GPU writable roots must be distinct and non-nested")
        working = _clean_path(self.working_directory, "GPU working directory")
        if not working.is_dir():
            raise GpuChildError("GPU working directory is not a directory")
        return self

    def identity_document(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "batch_manifest": str(self.batch_manifest),
            "expected_batch_sha256": _digest(
                self.expected_batch_sha256, "expected batch SHA-256"
            ),
            "runtime_admission": str(self.runtime_admission),
            "expected_runtime_admission_sha256": _digest(
                self.expected_runtime_admission_sha256,
                "expected runtime-admission SHA-256",
            ),
            "production_profile": str(self.production_profile),
            "expected_production_profile_sha256": _digest(
                self.expected_production_profile_sha256,
                "expected production-profile SHA-256",
            ),
            "root_registration": str(self.root_registration),
            "expected_root_registration_sha256": _digest(
                self.expected_root_registration_sha256,
                "expected root-registration SHA-256",
            ),
            "launcher_profile": str(self.launcher_profile),
            "expected_launcher_profile_sha256": _digest(
                self.expected_launcher_profile_sha256,
                "expected launcher-profile SHA-256",
            ),
            "local_readiness": str(self.local_readiness),
            "expected_local_readiness_sha256": _digest(
                self.expected_local_readiness_sha256,
                "expected local-readiness SHA-256",
            ),
            "local_launcher": str(self.local_launcher),
            "writable_result_root": str(self.writable_result_root),
            "writable_event_root": str(self.writable_event_root),
            "writable_lock_root": str(self.writable_lock_root),
            "working_directory": str(self.working_directory),
            "attempt_ordinal": self.attempt_ordinal,
        }

    @property
    def identity_sha256(self) -> str:
        return _sha256_bytes(_canonical_bytes(self.identity_document()))

    def launcher_argv(self) -> tuple[str, ...]:
        return (
            str(self.local_launcher),
            "run",
            "--mode",
            "local-private-production",
            "--batch-manifest",
            str(self.batch_manifest),
            "--expected-batch-sha256",
            self.expected_batch_sha256,
            "--runtime-admission",
            str(self.runtime_admission),
            "--expected-runtime-admission-sha256",
            self.expected_runtime_admission_sha256,
            "--production-profile",
            str(self.production_profile),
            "--expected-production-profile-sha256",
            self.expected_production_profile_sha256,
            "--root-registration",
            str(self.root_registration),
            "--expected-root-registration-sha256",
            self.expected_root_registration_sha256,
            "--launcher-profile",
            str(self.launcher_profile),
            "--expected-launcher-profile-sha256",
            self.expected_launcher_profile_sha256,
            "--local-readiness",
            str(self.local_readiness),
            "--expected-local-readiness-sha256",
            self.expected_local_readiness_sha256,
            "--writable-result-root",
            str(self.writable_result_root),
            "--writable-event-root",
            str(self.writable_event_root),
            "--writable-lock-root",
            str(self.writable_lock_root),
        )


@dataclass(frozen=True)
class BatchResultStatus:
    status: str
    batch_id: str
    completed_ordinals: tuple[int, ...]
    absent_ordinals: tuple[int, ...]
    invalid: tuple[dict[str, Any], ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "BatchResultStatus":
        if not isinstance(value, Mapping):
            raise GpuChildError("batch result probe did not return an object")
        expected = {
            "status",
            "batch_id",
            "completed_ordinals",
            "absent_ordinals",
            "invalid",
        }
        if set(value) != expected:
            raise GpuChildError("batch result probe fields differ from the closed contract")
        status = value["status"]
        batch_id = value["batch_id"]
        if status not in {"pending", "completed", "invalid"}:
            raise GpuChildError("batch result probe status is invalid")
        if not isinstance(batch_id, str) or BATCH_ID_RE.fullmatch(batch_id) is None:
            raise GpuChildError("batch result probe batch ID is invalid")

        def ordinals(name: str) -> tuple[int, ...]:
            rows = value[name]
            if not isinstance(rows, (list, tuple)) or any(
                isinstance(row, bool) or not isinstance(row, int) or row < 1 for row in rows
            ):
                raise GpuChildError(f"batch result probe {name} are invalid")
            result = tuple(rows)
            if tuple(sorted(set(result))) != result:
                raise GpuChildError(f"batch result probe {name} are not unique and sorted")
            return result

        invalid_value = value["invalid"]
        if not isinstance(invalid_value, (list, tuple)) or any(
            not isinstance(row, dict) for row in invalid_value
        ):
            raise GpuChildError("batch result probe invalid rows are malformed")
        invalid: list[dict[str, Any]] = []
        for row in invalid_value:
            if set(row) != {"ordinal", "error_type", "message"}:
                raise GpuChildError("batch result probe invalid-row fields differ")
            ordinal = row["ordinal"]
            if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
                raise GpuChildError("batch result probe invalid ordinal is malformed")
            invalid.append(
                {
                    "ordinal": ordinal,
                    "error_type": _bounded_text(row["error_type"], "result error type", 256),
                    "message": _bounded_text(row["message"], "result error message", 2048),
                }
            )
        completed = ordinals("completed_ordinals")
        absent = ordinals("absent_ordinals")
        if set(completed) & set(absent):
            raise GpuChildError("batch result ordinals overlap")
        if status == "completed" and (absent or invalid):
            raise GpuChildError("completed batch result has absent or invalid members")
        if status == "invalid" and not invalid:
            raise GpuChildError("invalid batch result lacks invalid members")
        return cls(status, batch_id, completed, absent, tuple(invalid))

    def document(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "batch_id": self.batch_id,
            "completed_ordinals": list(self.completed_ordinals),
            "absent_ordinals": list(self.absent_ordinals),
            "invalid": [dict(row) for row in self.invalid],
        }


class BatchResultProbe(Protocol):
    def inspect(self, spec: LocalPrivateGpuLaunchSpec) -> BatchResultStatus: ...


class ProductionBatchResultProbe:
    """Replay exact v2 batch results without launching media or CUDA work."""

    def inspect(self, spec: LocalPrivateGpuLaunchSpec) -> BatchResultStatus:
        # Import lazily so supervisor-only tests and status startup do not import the
        # GPU execution modules unless an exact batch is being reconciled.
        from pipeline.gpu import production_asr_batch_v2 as batch_v2
        from pipeline.gpu import production_asr_v5 as asr_v5

        profile = asr_v5.load_profile_document(
            spec.production_profile,
            spec.expected_production_profile_sha256,
        )
        manifest, _body = batch_v2.load_manifest(
            spec.batch_manifest,
            spec.expected_batch_sha256,
            profile=profile,
        )
        if manifest["batch_id"] != spec.batch_id:
            raise GpuChildError("batch manifest ID differs from the launch binding")
        if manifest.get("execution_class") != batch_v2.EXECUTION_CLASS_LOCAL_PRIVATE:
            raise GpuChildError("executor accepts only local-private production batches")
        raw = batch_v2.batch_status(manifest, profile)
        return BatchResultStatus.from_mapping(
            {
                key: raw[key]
                for key in (
                    "status",
                    "batch_id",
                    "completed_ordinals",
                    "absent_ordinals",
                    "invalid",
                )
            }
        )


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

    def document(self) -> dict[str, Any]:
        return {
            "load_state": self.load_state,
            "active_state": self.active_state,
            "sub_state": self.sub_state,
            "result": self.result,
            "exec_main_code": self.exec_main_code,
            "exec_main_status": self.exec_main_status,
            "invocation_id": self.invocation_id,
        }


@dataclass(frozen=True)
class GpuChildRecord:
    unit_name: str
    outer_unit: str
    outer_invocation_id: str
    child_invocation_id: str | None
    batch_id: str
    batch_sha256: str
    spec_identity_sha256: str
    attempt_ordinal: int
    state: str
    created_at: str
    updated_at: str
    launch_accepted_at: str | None
    started_at: str | None
    completed_at: str | None
    stop_requested_at: str | None
    returncode: int | None
    systemd_result: str | None
    result: dict[str, Any] | None
    error: str | None

    def document(self) -> dict[str, Any]:
        return {
            "kind": "himr_autonomous_gpu_child",
            "schema_version": 1,
            "unit_name": self.unit_name,
            "outer_unit": self.outer_unit,
            "outer_invocation_id": self.outer_invocation_id,
            "child_invocation_id": self.child_invocation_id,
            "batch_id": self.batch_id,
            "batch_sha256": self.batch_sha256,
            "spec_identity_sha256": self.spec_identity_sha256,
            "attempt_ordinal": self.attempt_ordinal,
            "state": self.state,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "launch_accepted_at": self.launch_accepted_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "stop_requested_at": self.stop_requested_at,
            "returncode": self.returncode,
            "systemd_result": self.systemd_result,
            "result": self.result,
            "error": self.error,
        }

    @classmethod
    def from_document(cls, value: Any) -> "GpuChildRecord":
        expected = {
            "kind",
            "schema_version",
            "unit_name",
            "outer_unit",
            "outer_invocation_id",
            "child_invocation_id",
            "batch_id",
            "batch_sha256",
            "spec_identity_sha256",
            "attempt_ordinal",
            "state",
            "created_at",
            "updated_at",
            "launch_accepted_at",
            "started_at",
            "completed_at",
            "stop_requested_at",
            "returncode",
            "systemd_result",
            "result",
            "error",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise GpuChildError("GPU child record fields differ from the closed contract")
        if value["kind"] != "himr_autonomous_gpu_child" or value["schema_version"] != 1:
            raise GpuChildError("GPU child record header is unsupported")
        record = cls(**{key: value[key] for key in expected - {"kind", "schema_version"}})
        record.validated()
        return record

    def validated(self) -> "GpuChildRecord":
        if CHILD_UNIT_RE.fullmatch(self.unit_name) is None:
            raise GpuChildError("GPU child unit name is invalid")
        ControllerUnitContext(self.outer_unit, self.outer_invocation_id).validated()
        if self.child_invocation_id is not None and INVOCATION_ID_RE.fullmatch(
            self.child_invocation_id
        ) is None:
            raise GpuChildError("GPU child InvocationID is invalid")
        if BATCH_ID_RE.fullmatch(self.batch_id) is None:
            raise GpuChildError("GPU child batch ID is invalid")
        _digest(self.batch_sha256, "GPU child batch SHA-256")
        _digest(self.spec_identity_sha256, "GPU child specification identity")
        if (
            isinstance(self.attempt_ordinal, bool)
            or not isinstance(self.attempt_ordinal, int)
            or not 1 <= self.attempt_ordinal <= 999_999
        ):
            raise GpuChildError("GPU child attempt ordinal is invalid")
        if self.state not in RECORD_STATES:
            raise GpuChildError("GPU child record state is invalid")
        for label, value, optional in (
            ("created_at", self.created_at, False),
            ("updated_at", self.updated_at, False),
            ("launch_accepted_at", self.launch_accepted_at, True),
            ("started_at", self.started_at, True),
            ("completed_at", self.completed_at, True),
            ("stop_requested_at", self.stop_requested_at, True),
        ):
            if value is None and optional:
                continue
            if not isinstance(value, str) or UTC_RE.fullmatch(value) is None:
                raise GpuChildError(f"GPU child {label} is invalid")
        if self.returncode is not None and (
            isinstance(self.returncode, bool)
            or not isinstance(self.returncode, int)
            or not -(2**31) <= self.returncode <= 2**31 - 1
        ):
            raise GpuChildError("GPU child return code is invalid")
        if self.systemd_result is not None:
            _bounded_text(self.systemd_result, "GPU child systemd result", 64)
        if self.error is not None:
            _bounded_text(self.error, "GPU child error", 2048)
        if self.result is not None:
            BatchResultStatus.from_mapping(self.result)
        if self.state in {"running", "retiring", "stopping"} and self.child_invocation_id is None:
            raise GpuChildError("active GPU child state lacks a persisted InvocationID")
        if self.child_invocation_id is not None and self.launch_accepted_at is None:
            raise GpuChildError(
                "GPU child manager identity lacks durable launch acceptance"
            )
        if (
            self.launch_accepted_at is not None
            and self.child_invocation_id is None
            and self.state not in {"launching", "reconciliation_required"}
        ):
            raise GpuChildError(
                "accepted GPU child outcome lacks durable manager identity"
            )
        if self.state == "succeeded" and (
            self.result is None or self.result.get("status") != "completed"
        ):
            raise GpuChildError("succeeded GPU child lacks completed result replay")
        return self


class GpuChildJournal(Protocol):
    def authority_lock(self) -> AbstractContextManager[None]: ...

    def authority_token(self) -> tuple[int, int, int, int]: ...

    def load(self, unit_name: str) -> GpuChildRecord | None: ...

    def save(self, record: GpuChildRecord) -> None: ...

    def prepare_logs(self, unit_name: str) -> tuple[Path, Path]: ...

    def list_records(self) -> tuple[GpuChildRecord, ...]: ...


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _private_child_directory(root: Path, unit_name: str, *, create: bool) -> Path:
    if CHILD_UNIT_RE.fullmatch(unit_name) is None:
        raise GpuChildError("cannot derive journal path from invalid child unit")
    child = root / unit_name.removesuffix(".service")
    if create and not child.exists() and not child.is_symlink():
        try:
            child.mkdir(mode=0o700)
            _fsync_directory(root)
        except FileExistsError:
            pass
        except OSError as error:
            raise GpuChildError(f"cannot create GPU child journal directory: {error}") from error
    return _private_directory(child, "GPU child journal directory")


def _atomic_record(path: Path, document: dict[str, Any]) -> None:
    body = _canonical_bytes(document)
    if not 1 <= len(body) <= MAX_RECORD_BYTES:
        raise GpuChildError("GPU child record exceeds its byte cap")
    if path.exists() or path.is_symlink():
        observed = path.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o600
        ):
            raise GpuChildError("existing GPU child record has unsafe metadata")
    descriptor, name = tempfile.mkstemp(prefix=".record.tmp-", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise GpuChildError("GPU child record write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _read_record(path: Path) -> GpuChildRecord:
    try:
        inspected = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise GpuChildError(f"cannot open GPU child record: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or not 1 <= opened.st_size <= MAX_RECORD_BYTES
            or (inspected.st_dev, inspected.st_ino, inspected.st_size, inspected.st_mode)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mode)
        ):
            raise GpuChildError("GPU child record has unsafe metadata")
        body = bytearray()
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, opened.st_size - offset, offset)
            if not chunk:
                raise GpuChildError("GPU child record ended during read")
            body.extend(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        linked = path.lstat()
        if _fingerprint(opened) != _fingerprint(after) or _fingerprint(after) != _fingerprint(linked):
            raise GpuChildError("GPU child record changed during read")
    finally:
        os.close(descriptor)

    def unique(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise GpuChildError(f"GPU child record repeats field {key!r}")
            result[key] = value
        return result

    try:
        value = json.loads(bytes(body), object_pairs_hook=unique)
    except GpuChildError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise GpuChildError(f"GPU child record is not strict JSON: {error}") from error
    record = GpuChildRecord.from_document(value)
    if bytes(body) != _canonical_bytes(record.document()):
        raise GpuChildError("GPU child record is not canonical JSON")
    return record


class PrivateGpuChildJournal:
    """One owner-private, durable record and two append logs per child unit."""

    def __init__(self, root: Path):
        self.root = _private_directory(root, "GPU child journal root")
        root_identity = self.root.stat()
        self._root_identity = (root_identity.st_dev, root_identity.st_ino)
        entries = list(self.root.iterdir())
        if len(entries) > MAX_CHILDREN:
            raise GpuChildError("GPU child journal exceeds its entry cap")
        for entry in entries:
            unit = f"{entry.name}.service"
            if CHILD_UNIT_RE.fullmatch(unit) is None:
                raise GpuChildError("GPU child journal contains an unexpected entry")
            child = _private_directory(entry, "GPU child journal directory")
            names = {path.name for path in child.iterdir()}
            if "record.json" not in names or not names <= {
                "record.json",
                "stdout.log",
                "stderr.log",
            }:
                raise GpuChildError("GPU child journal directory has unexpected contents")
            record = _read_record(child / "record.json")
            if record.unit_name != unit:
                raise GpuChildError("GPU child record is in the wrong journal directory")

    @classmethod
    def _classify_startup_authority(cls, root: Path) -> str:
        """Boundedly classify durable child authority as empty/terminal/unsafe.

        The same advisory authority used by ``launch`` closes the cooperative
        create race. Every record in a bounded history is canonically replayed;
        only states whose durable transition already proved the manager resource
        absent/inactive can be reported as terminal. Only an empty classification
        authorizes cancellation before exact GPU checkpoint recovery: nonempty
        records, including terminal history, still need batch/spec binding first.
        """

        root = _private_directory(root, "GPU child journal root")
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(root, flags)
        except OSError as error:
            raise GpuChildReconciliationRequired(
                f"cannot open GPU child startup authority: {error}"
            ) from error
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise GpuChildReconciliationRequired(
                    "GPU child startup authority is already held"
                ) from error
            except OSError as error:
                raise GpuChildReconciliationRequired(
                    f"cannot acquire GPU child startup authority: {error}"
                ) from error
            try:
                opened = os.fstat(descriptor)
                linked = root.lstat()
                with os.scandir(descriptor) as entries:
                    names: list[str] = []
                    oversized = False
                    for entry in entries:
                        if len(names) >= STARTUP_TERMINAL_PREFLIGHT_MAX_RECORDS:
                            oversized = True
                            break
                        names.append(entry.name)
            except OSError as error:
                raise GpuChildReconciliationRequired(
                    f"cannot inspect GPU child startup authority: {error}"
                ) from error
            stable = (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
                opened.st_uid,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            )
            if (
                not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) != 0o700
                or stat.S_ISLNK(linked.st_mode)
                or (opened.st_dev, opened.st_ino)
                != (linked.st_dev, linked.st_ino)
            ):
                raise GpuChildReconciliationRequired(
                    "GPU child startup authority changed during terminal preflight"
                )
            if oversized:
                return "requires_recovery"
            safe_terminal_states = {"succeeded", "failed", "stopped"}
            for name in names:
                unit = f"{name}.service"
                if CHILD_UNIT_RE.fullmatch(unit) is None:
                    raise GpuChildReconciliationRequired(
                        "GPU child startup authority contains an unexpected entry"
                    )
                child = _private_child_directory(root, unit, create=False)
                child_names = {path.name for path in child.iterdir()}
                if "record.json" not in child_names or not child_names <= {
                    "record.json",
                    "stdout.log",
                    "stderr.log",
                }:
                    raise GpuChildReconciliationRequired(
                        "GPU child startup authority has unexpected contents"
                    )
                record = _read_record(child / "record.json")
                if record.unit_name != unit:
                    raise GpuChildReconciliationRequired(
                        "GPU child startup record is in the wrong directory"
                    )
                if record.state not in safe_terminal_states:
                    return "requires_recovery"
            # Record validation reads child directories after the root snapshot;
            # verify the launch-authority generation again before certifying it.
            final = os.fstat(descriptor)
            final_linked = root.lstat()
            if (
                stable
                != (
                    final.st_dev,
                    final.st_ino,
                    final.st_mode,
                    final.st_uid,
                    final.st_mtime_ns,
                    final.st_ctime_ns,
                )
                or (opened.st_dev, opened.st_ino)
                != (final_linked.st_dev, final_linked.st_ino)
                or stat.S_ISLNK(final_linked.st_mode)
            ):
                raise GpuChildReconciliationRequired(
                    "GPU child startup authority changed during record preflight"
                )
            return "empty" if not names else "terminal"
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(descriptor)

    @classmethod
    def prove_empty_startup_authority(cls, root: Path) -> bool:
        """Prove there is no persisted child requiring batch binding."""

        return cls._classify_startup_authority(root) == "empty"

    @classmethod
    def prove_quiescent_startup_authority(cls, root: Path) -> bool:
        """Prove bounded records are empty or durably terminal.

        This is diagnostic support only; clean controller cancellation still
        binds nonempty terminal records to exact recovered GPU batch authority.
        """

        return cls._classify_startup_authority(root) in {"empty", "terminal"}

    def authority_token(self) -> tuple[int, int, int, int]:
        """Return an O(1) token that changes when a child unit is added."""

        try:
            observed = self.root.lstat()
        except OSError as error:
            raise GpuChildError(
                f"cannot inspect GPU child journal authority: {error}"
            ) from error
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o700
            or (observed.st_dev, observed.st_ino) != self._root_identity
        ):
            raise GpuChildError("GPU child journal authority root changed")
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )

    @contextmanager
    def authority_lock(self) -> Iterator[None]:
        """Serialize the exact prelaunch authority check through durable intent."""

        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(self.root, flags)
        except OSError as error:
            raise GpuChildReconciliationRequired(
                f"cannot open GPU child launch authority: {error}"
            ) from error
        try:
            try:
                opened = os.fstat(descriptor)
                linked = self.root.lstat()
            except OSError as error:
                raise GpuChildReconciliationRequired(
                    f"cannot validate GPU child launch authority: {error}"
                ) from error
            if (
                not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) != 0o700
                or (opened.st_dev, opened.st_ino) != self._root_identity
                or (linked.st_dev, linked.st_ino) != self._root_identity
                or stat.S_ISLNK(linked.st_mode)
            ):
                raise GpuChildReconciliationRequired(
                    "GPU child launch authority root changed"
                )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise GpuChildReconciliationRequired(
                    "GPU child launch authority is already held"
                ) from error
            except OSError as error:
                raise GpuChildReconciliationRequired(
                    f"cannot acquire GPU child launch authority: {error}"
                ) from error
            yield
        finally:
            # Closing the descriptor releases the advisory lock without a
            # second operation that could mask the primary launch outcome.
            try:
                os.close(descriptor)
            except OSError:
                pass

    def load(self, unit_name: str) -> GpuChildRecord | None:
        if CHILD_UNIT_RE.fullmatch(unit_name) is None:
            raise GpuChildError("GPU child unit name is invalid")
        child = self.root / unit_name.removesuffix(".service")
        if not child.exists() and not child.is_symlink():
            return None
        child = _private_child_directory(self.root, unit_name, create=False)
        names = {path.name for path in child.iterdir()}
        if "record.json" not in names or not names <= {
            "record.json",
            "stdout.log",
            "stderr.log",
        }:
            raise GpuChildError("GPU child journal directory has unexpected contents")
        record = _read_record(child / "record.json")
        if record.unit_name != unit_name:
            raise GpuChildError("GPU child journal record unit differs from its path")
        return record

    def save(self, record: GpuChildRecord) -> None:
        record.validated()
        child = _private_child_directory(self.root, record.unit_name, create=True)
        record_path = child / "record.json"
        existing = _read_record(record_path) if record_path.exists() or record_path.is_symlink() else None
        if existing is not None and (
            existing.outer_unit != record.outer_unit
            or existing.outer_invocation_id != record.outer_invocation_id
            or existing.batch_sha256 != record.batch_sha256
            or existing.spec_identity_sha256 != record.spec_identity_sha256
            or existing.attempt_ordinal != record.attempt_ordinal
        ):
            raise GpuChildError("refusing to overwrite a differently bound GPU child record")
        _atomic_record(record_path, record.document())

    def prepare_logs(self, unit_name: str) -> tuple[Path, Path]:
        child = _private_child_directory(self.root, unit_name, create=False)
        if self.load(unit_name) is None:
            raise GpuChildError("cannot create logs before the GPU child record")
        paths = (child / "stdout.log", child / "stderr.log")
        for path in paths:
            if not path.exists() and not path.is_symlink():
                try:
                    descriptor = os.open(
                        path,
                        os.O_WRONLY
                        | os.O_CREAT
                        | os.O_EXCL
                        | getattr(os, "O_CLOEXEC", 0)
                        | getattr(os, "O_NOFOLLOW", 0),
                        0o600,
                    )
                except FileExistsError:
                    pass
                except OSError as error:
                    raise GpuChildError(f"cannot create GPU child log: {error}") from error
                else:
                    os.fsync(descriptor)
                    os.close(descriptor)
                    _fsync_directory(child)
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise GpuChildError("GPU child log has unsafe metadata")
        return paths

    def list_records(self) -> tuple[GpuChildRecord, ...]:
        entries = sorted(self.root.iterdir(), key=lambda path: path.name)
        if len(entries) > MAX_CHILDREN:
            raise GpuChildError("GPU child journal exceeds its entry cap")
        records = []
        for entry in entries:
            unit = f"{entry.name}.service"
            if CHILD_UNIT_RE.fullmatch(unit) is None:
                raise GpuChildError("GPU child journal contains an unexpected entry")
            record = self.load(unit)
            if record is None:
                raise GpuChildError("GPU child journal entry lacks a record")
            records.append(record)
        return tuple(records)


Runner = Callable[..., subprocess.CompletedProcess[bytes]]


class SystemdGpuChildExecutor:
    """Launch and reconcile deterministic sibling services for exact GPU batches."""

    def __init__(
        self,
        context: ControllerUnitContext,
        journal: GpuChildJournal,
        *,
        result_probe: BatchResultProbe | None = None,
        runner: Runner = subprocess.run,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], str] = _utc_now,
        bind_attempts: int = 20,
        stop_attempts: int = 20,
    ):
        self.context = context.validated()
        self.journal = journal
        self.result_probe = result_probe or ProductionBatchResultProbe()
        self.runner = runner
        self.sleep = sleep
        self.now = now
        if (
            isinstance(bind_attempts, bool)
            or not isinstance(bind_attempts, int)
            or not 1 <= bind_attempts <= 1_000
            or isinstance(stop_attempts, bool)
            or not isinstance(stop_attempts, int)
            or not 1 <= stop_attempts <= 1_000
        ):
            raise GpuChildError("systemd reconciliation attempt counts are invalid")
        self.bind_attempts = bind_attempts
        self.stop_attempts = stop_attempts
        self._foreign_attempt_authority: frozenset[
            tuple[str, str, int]
        ] | None = None
        self._foreign_attempt_authority_token: tuple[int, int, int, int] | None = None
        _validate_system_tools()

    def unit_name(self, spec: LocalPrivateGpuLaunchSpec) -> str:
        _digest(spec.expected_batch_sha256, "expected batch SHA-256")
        if (
            isinstance(spec.attempt_ordinal, bool)
            or not isinstance(spec.attempt_ordinal, int)
            or not 1 <= spec.attempt_ordinal <= 999_999
        ):
            raise GpuChildError("attempt ordinal is invalid")
        unit = (
            f"himr-autonomy-gpu-{self.context.outer_invocation_id}-"
            f"{spec.expected_batch_sha256}-{spec.attempt_ordinal:06d}.service"
        )
        if CHILD_UNIT_RE.fullmatch(unit) is None:
            raise GpuChildError("derived GPU child unit name is invalid")
        return unit

    @staticmethod
    def _control_environment() -> dict[str, str]:
        return _systemd_control_environment()

    def _control(
        self, argv: list[str], *, timeout: int = CONTROL_TIMEOUT_SECONDS
    ) -> subprocess.CompletedProcess[bytes]:
        if not argv or argv[0] not in {SYSTEMD_RUN, SYSTEMCTL}:
            raise GpuChildError("systemd control argv is not fixed")
        try:
            completed = self.runner(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                close_fds=True,
                cwd="/",
                env=self._control_environment(),
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise GpuChildError("fixed systemd control command timed out") from error
        except OSError as error:
            raise GpuChildError(f"cannot execute fixed systemd control command: {error}") from error
        for label, body in (("stdout", completed.stdout), ("stderr", completed.stderr)):
            if not isinstance(body, bytes) or len(body) > MAX_SYSTEMD_OUTPUT_BYTES:
                raise GpuChildError(f"systemd {label} is invalid or exceeds its cap")
            try:
                body.decode("utf-8")
            except UnicodeDecodeError as error:
                raise GpuChildError(f"systemd {label} is not UTF-8") from error
            if b"\x00" in body:
                raise GpuChildError(f"systemd {label} contains a NUL byte")
        return completed

    def _query(self, unit_name: str) -> SystemdUnitState:
        if not (
            OUTER_UNIT_RE.fullmatch(unit_name) is not None
            or CHILD_UNIT_RE.fullmatch(unit_name) is not None
        ):
            raise GpuChildError("refusing to query an unrecognised systemd unit")
        completed = self._control(
            [
                SYSTEMCTL,
                "--user",
                "--no-pager",
                "--no-ask-password",
                "show",
                f"--property={','.join(SYSTEMD_PROPERTIES)}",
                unit_name,
            ]
        )
        try:
            text = completed.stdout.decode("utf-8")
        except UnicodeDecodeError as error:  # already bounded; retain local context
            raise GpuChildError("systemctl show output is not UTF-8") from error
        values: dict[str, str] = {}
        for line in text.splitlines():
            if not line or "=" not in line:
                raise GpuChildError("systemctl show output is malformed")
            key, value = line.split("=", 1)
            if key not in SYSTEMD_PROPERTIES or key in values:
                raise GpuChildError("systemctl show fields are not closed")
            if len(value) > 128 or any(ord(character) < 32 for character in value):
                raise GpuChildError("systemctl show contains an invalid value")
            values[key] = value
        if set(values) != set(SYSTEMD_PROPERTIES):
            raise GpuChildError("systemctl show omitted required fields")
        if completed.returncode != 0 and values["LoadState"] != "not-found":
            raise GpuChildError("systemctl show failed for the exact unit")

        def optional_integer(name: str) -> int | None:
            raw = values[name]
            if raw == "":
                return None
            if not raw.isascii() or not raw.isdigit():
                raise GpuChildError(f"systemctl {name} is not an integer")
            result = int(raw)
            if not 0 <= result <= 2**31 - 1:
                raise GpuChildError(f"systemctl {name} is out of range")
            return result

        invocation = values["InvocationID"] or None
        if invocation is not None and INVOCATION_ID_RE.fullmatch(invocation) is None:
            raise GpuChildError("systemctl InvocationID is invalid")
        for name in ("LoadState", "ActiveState", "SubState", "Result"):
            if re.fullmatch(r"[a-z0-9_-]{0,64}", values[name]) is None:
                raise GpuChildError(f"systemctl {name} is invalid")
        return SystemdUnitState(
            load_state=values["LoadState"],
            active_state=values["ActiveState"],
            sub_state=values["SubState"],
            result=values["Result"],
            exec_main_code=optional_integer("ExecMainCode"),
            exec_main_status=optional_integer("ExecMainStatus"),
            invocation_id=invocation,
        )

    def _assert_outer_live(self) -> None:
        outer = self._query(self.context.outer_unit)
        if (
            outer.absent
            or not outer.process_live
            or outer.invocation_id != self.context.outer_invocation_id
        ):
            raise GpuChildError(
                "controller outer unit is absent, inactive, or has a different InvocationID"
            )

    def _new_record(
        self,
        spec: LocalPrivateGpuLaunchSpec,
        *,
        unit_name: str,
        state: str,
        result: BatchResultStatus | None,
    ) -> GpuChildRecord:
        now = self.now()
        return GpuChildRecord(
            unit_name=unit_name,
            outer_unit=self.context.outer_unit,
            outer_invocation_id=self.context.outer_invocation_id,
            child_invocation_id=None,
            batch_id=spec.batch_id,
            batch_sha256=spec.expected_batch_sha256,
            spec_identity_sha256=spec.identity_sha256,
            attempt_ordinal=spec.attempt_ordinal,
            state=state,
            created_at=now,
            updated_at=now,
            launch_accepted_at=None,
            started_at=None,
            completed_at=now if state == "succeeded" else None,
            stop_requested_at=None,
            returncode=0 if state == "succeeded" else None,
            systemd_result=None,
            result=None if result is None else result.document(),
            error=None,
        ).validated()

    def _validate_binding(
        self, record: GpuChildRecord, spec: LocalPrivateGpuLaunchSpec
    ) -> None:
        expected_unit = self.unit_name(spec)
        if (
            record.unit_name != expected_unit
            or record.outer_unit != self.context.outer_unit
            or record.outer_invocation_id != self.context.outer_invocation_id
            or record.batch_id != spec.batch_id
            or record.batch_sha256 != spec.expected_batch_sha256
            or record.spec_identity_sha256 != spec.identity_sha256
            or record.attempt_ordinal != spec.attempt_ordinal
        ):
            raise GpuChildError("persisted GPU child is bound to a different exact launch")

    def _save(self, record: GpuChildRecord, **changes: Any) -> GpuChildRecord:
        if changes.get("error") is not None:
            changes["error"] = _safe_error(changes["error"])
        updated = replace(record, updated_at=self.now(), **changes).validated()
        self.journal.save(updated)
        return updated

    def _probe(self, spec: LocalPrivateGpuLaunchSpec) -> BatchResultStatus:
        result = self.result_probe.inspect(spec)
        if not isinstance(result, BatchResultStatus):
            raise GpuChildError("batch result probe returned an unsupported value")
        normalized = BatchResultStatus.from_mapping(result.document())
        if normalized.batch_id != spec.batch_id:
            raise GpuChildError("batch result probe returned a different batch ID")
        return normalized

    def _systemd_run_argv(
        self,
        spec: LocalPrivateGpuLaunchSpec,
        unit_name: str,
        stdout_path: Path,
        stderr_path: Path,
    ) -> list[str]:
        for path, label in (
            (stdout_path, "GPU stdout log"),
            (stderr_path, "GPU stderr log"),
            (spec.working_directory, "GPU working directory"),
        ):
            _clean_path(path, label)
        environment = [f"{key}={value}" for key, value in sorted(CHILD_ENVIRONMENT.items())]
        outer = self.context.outer_unit
        return [
            SYSTEMD_RUN,
            "--user",
            "--no-block",
            "--quiet",
            "--no-ask-password",
            f"--unit={unit_name}",
            "--property=Type=exec",
            "--property=ExitType=cgroup",
            "--property=Restart=no",
            "--property=KillMode=control-group",
            f"--property=RuntimeMaxSec={RUNTIME_MAX_SECONDS}s",
            "--property=TimeoutStopSec=15s",
            "--property=SendSIGKILL=yes",
            "--property=OOMPolicy=stop",
            f"--property=TasksMax={TASKS_MAX}",
            f"--property=LimitNOFILE={NOFILE_MAX}",
            f"--property=LimitFSIZE={FILE_SIZE_MAX_BYTES}",
            "--property=LimitCORE=0",
            f"--property=MemoryMax={MEMORY_MAX_BYTES}",
            f"--property=MemorySwapMax={MEMORY_SWAP_MAX_BYTES}",
            "--property=UMask=0077",
            "--property=StandardInput=null",
            f"--property=StandardOutput=append:{stdout_path}",
            f"--property=StandardError=append:{stderr_path}",
            "--property=RemainAfterExit=yes",
            f"--property=PartOf={outer}",
            f"--property=BindsTo={outer}",
            f"--property=After={outer}",
            f"--working-directory={spec.working_directory}",
            "--",
            ENV,
            "-i",
            *environment,
            *spec.launcher_argv(),
        ]

    def _assert_no_prior_invocation_authority(
        self, spec: LocalPrivateGpuLaunchSpec
    ) -> None:
        """Refuse a duplicate attempt while an older controller owns it."""

        token_method = getattr(self.journal, "authority_token", None)
        try:
            token = token_method() if callable(token_method) else None
        except Exception as error:
            raise GpuChildReconciliationRequired(
                "GPU child journal authority token is unavailable"
            ) from error
        if token is not None and (
            not isinstance(token, tuple)
            or len(token) != 4
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in token
            )
        ):
            raise GpuChildReconciliationRequired(
                "GPU child journal authority token is malformed"
            )
        if (
            token is None
            or self._foreign_attempt_authority is None
            or token != self._foreign_attempt_authority_token
        ):
            try:
                records = self.journal.list_records()
            except Exception as error:
                raise GpuChildReconciliationRequired(
                    "GPU child journal could not be scanned before launch"
                ) from error
            seen: set[str] = set()
            authority: set[tuple[str, str, int]] = set()
            try:
                for record in records:
                    record.validated()
                    expected_unit = (
                        f"himr-autonomy-gpu-{record.outer_invocation_id}-"
                        f"{record.batch_sha256}-{record.attempt_ordinal:06d}.service"
                    )
                    if record.unit_name != expected_unit or record.unit_name in seen:
                        raise GpuChildError(
                            "GPU child journal contains inconsistent attempt authority"
                        )
                    seen.add(record.unit_name)
                    foreign = (
                        record.outer_unit != self.context.outer_unit
                        or record.outer_invocation_id
                        != self.context.outer_invocation_id
                    )
                    if foreign and record.state in {
                        "launching",
                        "running",
                        "retiring",
                        "stopping",
                        "reconciliation_required",
                    }:
                        authority.add(
                            (
                                record.batch_id,
                                record.batch_sha256,
                                record.attempt_ordinal,
                            )
                        )
            except Exception as error:
                raise GpuChildReconciliationRequired(
                    "GPU child journal failed exact prelaunch validation"
                ) from error
            try:
                after = token_method() if callable(token_method) else None
            except Exception as error:
                raise GpuChildReconciliationRequired(
                    "GPU child journal authority changed during prelaunch scan"
                ) from error
            if token is not None and after != token:
                # A new unit appeared while the bounded snapshot was read.  Do
                # not cache or trust a partial generation; the next launch can
                # retry from a stable journal view.
                self._foreign_attempt_authority = None
                self._foreign_attempt_authority_token = None
                raise GpuChildReconciliationRequired(
                    "GPU child journal changed during prelaunch validation"
                )
            self._foreign_attempt_authority = frozenset(authority)
            self._foreign_attempt_authority_token = after

        key = (
            spec.batch_id,
            spec.expected_batch_sha256,
            spec.attempt_ordinal,
        )
        if key in self._foreign_attempt_authority:
            raise GpuChildReconciliationRequired(
                "prior controller invocation retains exact GPU attempt authority"
            )

    def _refresh_authority_token_after_intent(self) -> None:
        token_method = getattr(self.journal, "authority_token", None)
        if not callable(token_method):
            return
        try:
            token = token_method()
        except Exception as error:
            raise GpuChildReconciliationRequired(
                "GPU child journal authority token could not follow launch intent"
            ) from error
        if (
            not isinstance(token, tuple)
            or len(token) != 4
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in token
            )
        ):
            raise GpuChildReconciliationRequired(
                "GPU child journal authority token is malformed"
            )
        self._foreign_attempt_authority_token = token

    def launch(self, spec: LocalPrivateGpuLaunchSpec) -> GpuChildRecord:
        spec.validated()
        self._assert_outer_live()
        authority_lock = getattr(self.journal, "authority_lock", None)
        if not callable(authority_lock):
            raise GpuChildReconciliationRequired(
                "GPU child journal lacks serialized launch authority"
            )
        try:
            authority_context = authority_lock()
        except Exception as error:
            raise GpuChildReconciliationRequired(
                "GPU child launch authority could not be established"
            ) from error
        try:
            with authority_context:
                return self._launch_under_authority(spec)
        except GpuChildError:
            raise
        except Exception as error:
            raise GpuChildReconciliationRequired(
                "GPU child launch authority failed closed"
            ) from error

    def _launch_under_authority(
        self, spec: LocalPrivateGpuLaunchSpec
    ) -> GpuChildRecord:
        unit = self.unit_name(spec)
        self._assert_no_prior_invocation_authority(spec)
        existing = self.journal.load(unit)
        if existing is not None:
            self._validate_binding(existing, spec)
            reconciled = self.reconcile(spec)
            if reconciled is None:  # pragma: no cover - guarded by existing record
                raise GpuChildError("persisted GPU child disappeared during reconciliation")
            return reconciled

        collision = self._query(unit)
        if not collision.absent:
            raise GpuChildReconciliationRequired(
                "deterministic GPU child unit exists without a persisted launch record"
            )

        result = self._probe(spec)
        if result.status == "invalid":
            raise GpuChildError("exact GPU batch has invalid existing results")
        if result.status == "completed":
            record = self._new_record(
                spec, unit_name=unit, state="succeeded", result=result
            )
            self.journal.save(record)
            self._refresh_authority_token_after_intent()
            return record

        record = self._new_record(spec, unit_name=unit, state="launching", result=result)
        # Persist deterministic intent before asking the manager to create anything.
        self.journal.save(record)
        self._refresh_authority_token_after_intent()
        stdout_path, stderr_path = self.journal.prepare_logs(unit)
        try:
            completed = self._control(
                self._systemd_run_argv(spec, unit, stdout_path, stderr_path)
            )
        except Exception as error:
            record = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                error=(
                    f"systemd-run outcome is ambiguous: {type(error).__name__}: {error}"
                )[:2048],
            )
            raise GpuChildReconciliationRequired(
                record.error or "systemd-run outcome is ambiguous"
            ) from error
        if completed.returncode != 0:
            detail = completed.stderr.decode("utf-8").strip()
            try:
                rejected_snapshot = self._query(unit)
            except Exception as error:
                record = self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    error=(
                        "systemd-run returned an error and exact-unit state is unavailable: "
                        f"{type(error).__name__}: {error}"
                    )[:2048],
                )
                raise GpuChildReconciliationRequired(record.error or "launch is ambiguous") from error
            if not rejected_snapshot.absent:
                if rejected_snapshot.invocation_id is None:
                    record = self._save(
                        record,
                        state="reconciliation_required",
                        completed_at=self.now(),
                        error="systemd-run returned an error but an unbound exact unit exists",
                    )
                    raise GpuChildReconciliationRequired(record.error or "launch is ambiguous")
                record = self._save(
                    record,
                    child_invocation_id=rejected_snapshot.invocation_id,
                    launch_accepted_at=self.now(),
                    systemd_result=rejected_snapshot.result or None,
                )
                if rejected_snapshot.process_live:
                    return self._save(
                        record,
                        state="running",
                        started_at=record.started_at or self.now(),
                    )
                return self._reconcile_terminal(spec, record, rejected_snapshot)
            record = self._save(
                record,
                state="failed",
                completed_at=self.now(),
                error=(
                    "systemd-run rejected the exact GPU child"
                    + (f": {detail[:512]}" if detail else "")
                ),
            )
            raise GpuChildError(record.error or "systemd-run rejected the GPU child")
        record = self._save(record, launch_accepted_at=self.now())

        for _attempt in range(self.bind_attempts):
            snapshot = self._query(unit)
            if snapshot.invocation_id is not None:
                # The InvocationID is committed before `running` can leave this method.
                record = self._save(
                    record,
                    child_invocation_id=snapshot.invocation_id,
                    systemd_result=snapshot.result or None,
                )
                if snapshot.process_live:
                    return self._save(
                        record,
                        state="running",
                        started_at=record.started_at or self.now(),
                    )
                return self._reconcile_terminal(spec, record, snapshot)
            self.sleep(0.1)
        record = self._save(
            record,
            state="reconciliation_required",
            completed_at=self.now(),
            error="systemd accepted the child but no InvocationID could be durably bound",
        )
        raise GpuChildReconciliationRequired(record.error or "InvocationID unavailable")

    def _identity_checked_snapshot(
        self, record: GpuChildRecord, snapshot: SystemdUnitState
    ) -> GpuChildRecord:
        if snapshot.invocation_id is None:
            return record
        if (
            record.child_invocation_id is not None
            and record.child_invocation_id != snapshot.invocation_id
        ):
            failed = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                error="GPU child InvocationID changed; automatic control was refused",
            )
            raise GpuChildReconciliationRequired(failed.error or "InvocationID changed")
        changes: dict[str, Any] = {}
        if record.child_invocation_id is None:
            changes["child_invocation_id"] = snapshot.invocation_id
        if record.launch_accepted_at is None:
            if record.state not in {"launching", "reconciliation_required"}:
                failed = self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    error=(
                        "GPU child exists despite a record with no durable launch authority"
                    ),
                )
                raise GpuChildReconciliationRequired(
                    failed.error or "launch authority is unavailable"
                )
            changes["launch_accepted_at"] = self.now()
        if changes:
            # Persist manager acceptance and InvocationID in one journal
            # replacement before a live state can be returned or controlled.
            record = self._save(record, **changes)
        return record

    def reconcile(self, spec: LocalPrivateGpuLaunchSpec) -> GpuChildRecord | None:
        spec.validated()
        unit = self.unit_name(spec)
        record = self.journal.load(unit)
        if record is None:
            snapshot = self._query(unit)
            if snapshot.absent:
                return None
            raise GpuChildReconciliationRequired(
                "GPU child exists without a persisted exact launch record"
            )
        self._validate_binding(record, spec)

        # A batch found complete before launch has no manager identity to reconcile.
        if record.launch_accepted_at is None and record.state == "succeeded":
            try:
                result = self._probe(spec)
            except Exception as error:
                return self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    error=(
                        "previously completed prelaunch GPU result replay failed: "
                        f"{type(error).__name__}: {error}"
                    )[:2048],
                )
            if result.status != "completed":
                return self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    result=result.document(),
                    error="previously completed batch no longer replays as complete",
                )
            return self._save(record, result=result.document(), error=None)

        snapshot = self._query(unit)
        record = self._identity_checked_snapshot(record, snapshot)
        if snapshot.absent and record.state in TERMINAL_RECORD_STATES:
            try:
                result = self._probe(spec)
            except Exception as error:
                return self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    error=(
                        f"terminal GPU result replay failed: {type(error).__name__}: {error}"
                    )[:2048],
                )
            if result.status == "invalid" or (
                record.state == "succeeded" and result.status != "completed"
            ):
                return self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    result=result.document(),
                    error="retired GPU child no longer has a result state consistent with its record",
                )
            return self._save(record, result=result.document())
        if snapshot.process_live:
            if snapshot.invocation_id is None:
                return self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    error="live GPU child lacks a manager InvocationID",
                )
            stop_pending = record.stop_requested_at is not None
            desired = "stopping" if stop_pending else "running"
            return self._save(
                record,
                state=desired,
                started_at=record.started_at or self.now(),
                completed_at=None,
                systemd_result=snapshot.result or None,
                # Seeing the same child alive does not resolve a failed or
                # unproven stop.  Retain its durable ambiguity for the retry.
                error=record.error if stop_pending else None,
            )
        return self._reconcile_terminal(spec, record, snapshot)

    def _stop_verified(self, record: GpuChildRecord) -> SystemdUnitState:
        preflight = self._query(record.unit_name)
        record = self._identity_checked_snapshot(record, preflight)
        if preflight.absent:
            return preflight
        if record.child_invocation_id is None or preflight.invocation_id is None:
            raise GpuChildReconciliationRequired(
                "exact GPU child InvocationID is unavailable; stop was refused"
            )
        if record.child_invocation_id != preflight.invocation_id:
            raise GpuChildReconciliationRequired(
                "exact GPU child InvocationID changed; stop was refused"
            )
        if preflight.active_state == "inactive":
            return preflight
        completed = self._control(
            [SYSTEMCTL, "--user", "--no-ask-password", "stop", record.unit_name],
            timeout=STOP_TIMEOUT_SECONDS,
        )
        if completed.returncode != 0:
            raise GpuChildReconciliationRequired(
                "systemctl did not accept the exact GPU child stop"
            )
        latest = preflight
        for _attempt in range(self.stop_attempts):
            latest = self._query(record.unit_name)
            if latest.absent or latest.active_state == "inactive":
                return latest
            self.sleep(0.1)
        raise GpuChildReconciliationRequired(
            "exact GPU child did not become inactive after stop"
        )

    def _reconcile_terminal(
        self,
        spec: LocalPrivateGpuLaunchSpec,
        record: GpuChildRecord,
        snapshot: SystemdUnitState,
    ) -> GpuChildRecord:
        if snapshot.absent or snapshot.invocation_id is None:
            return self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                systemd_result=snapshot.result or None,
                error="GPU child is absent or lacks durable terminal manager identity",
            )
        record = self._identity_checked_snapshot(record, snapshot)
        try:
            result = self._probe(spec)
        except Exception as error:
            record = self._save(
                record,
                state="retiring",
                stop_requested_at=record.stop_requested_at or self.now(),
                returncode=snapshot.returncode,
                systemd_result=snapshot.result or None,
                error=f"exact GPU result replay failed: {type(error).__name__}: {error}"[:2048],
            )
            try:
                self._stop_verified(record)
            except Exception as stop_error:
                return self._save(
                    record,
                    state="reconciliation_required",
                    completed_at=self.now(),
                    error=(
                        f"result replay failed and terminal child could not be retired: {stop_error}"
                    )[:2048],
                )
            return self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
            )

        returncode = snapshot.returncode
        if returncode == 0 and result.status == "completed":
            terminal_state = "succeeded"
            terminal_error = None
        elif returncode is not None and returncode != 0 and result.status != "invalid":
            terminal_state = "failed"
            terminal_error = f"local-private GPU child exited with status {returncode}"
        else:
            terminal_state = "reconciliation_required"
            terminal_error = (
                "GPU child terminal status and exact result replay do not establish completion"
            )

        record = self._save(
            record,
            state="retiring",
            stop_requested_at=record.stop_requested_at or self.now(),
            returncode=returncode,
            systemd_result=snapshot.result or None,
            result=result.document(),
            error=terminal_error,
        )
        try:
            self._stop_verified(record)
        except Exception as error:
            return self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                error=f"terminal child could not be retired: {error}"[:2048],
            )
        return self._save(
            record,
            state=terminal_state,
            completed_at=self.now(),
            error=terminal_error,
        )

    def _preserve_inactive_terminal(
        self,
        spec: LocalPrivateGpuLaunchSpec,
        record: GpuChildRecord,
    ) -> GpuChildRecord:
        """Preserve terminal execution authority after proving no live resource."""

        if record.state != "succeeded":
            return record
        try:
            result = self._probe(spec)
        except Exception as error:
            return self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                error=(
                    "terminal succeeded GPU result replay failed: "
                    f"{type(error).__name__}: {error}"
                )[:2048],
            )
        if result.status != "completed":
            return self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                result=result.document(),
                error=(
                    "terminal succeeded GPU result no longer establishes completion"
                ),
            )
        return self._save(record, result=result.document(), error=None)

    def stop(self, spec: LocalPrivateGpuLaunchSpec) -> GpuChildRecord:
        spec.validated()
        unit = self.unit_name(spec)
        record = self.journal.load(unit)
        if record is None:
            raise GpuChildError("refusing to stop a GPU child without a persisted record")
        self._validate_binding(record, spec)
        terminal_outcome = (
            {
                "state": record.state,
                "completed_at": record.completed_at,
                "returncode": record.returncode,
                "systemd_result": record.systemd_result,
                "result": record.result,
                "error": record.error,
            }
            if record.state in TERMINAL_RECORD_STATES
            else None
        )
        stop_requested_at = record.stop_requested_at or self.now()
        try:
            snapshot = self._query(unit)
        except Exception as error:
            failed = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                stop_requested_at=stop_requested_at,
                error=(
                    "exact GPU child stop preflight failed: "
                    f"{type(error).__name__}: {error}"
                )[:2048],
            )
            raise GpuChildReconciliationRequired(
                failed.error or "stop preflight could not be proven"
            ) from error

        # A missing acceptance timestamp is never evidence that no service was
        # created.  Only an exact manager query proving absence can close a
        # never-launched intent without attempting manager control.
        if snapshot.absent and terminal_outcome is not None:
            return self._preserve_inactive_terminal(spec, record)
        if (
            snapshot.absent
            and record.launch_accepted_at is None
            and record.child_invocation_id is None
        ):
            return self._save(
                record,
                state="stopped",
                completed_at=self.now(),
                stop_requested_at=stop_requested_at,
                error=None,
            )
        try:
            record = self._identity_checked_snapshot(record, snapshot)
        except Exception as error:
            latest = self.journal.load(unit) or record
            failed = self._save(
                latest,
                state="reconciliation_required",
                completed_at=self.now(),
                stop_requested_at=stop_requested_at,
                error=latest.error
                or f"exact GPU child stop identity failed: {type(error).__name__}: {error}",
            )
            raise GpuChildReconciliationRequired(
                failed.error or "stop identity could not be proven"
            ) from error
        if snapshot.active_state == "inactive" and terminal_outcome is not None:
            return self._preserve_inactive_terminal(spec, record)
        if snapshot.absent and record.child_invocation_id is None:
            failed = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                stop_requested_at=stop_requested_at,
                error="accepted GPU child is absent without a persisted InvocationID",
            )
            raise GpuChildReconciliationRequired(failed.error or "child identity unavailable")
        if not snapshot.absent and record.child_invocation_id is None:
            failed = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                stop_requested_at=stop_requested_at,
                error="GPU child stop could not bind an InvocationID",
            )
            raise GpuChildReconciliationRequired(failed.error or "InvocationID unavailable")
        record = self._save(
            record,
            state="stopping",
            stop_requested_at=stop_requested_at,
            systemd_result=snapshot.result or None,
        )
        try:
            self._stop_verified(record)
        except Exception as error:
            failed = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                error=f"exact GPU child stop could not be proven: {error}"[:2048],
            )
            raise GpuChildReconciliationRequired(failed.error or "stop could not be proven") from error
        if terminal_outcome is not None:
            # Stopping an already-terminal manager resource must not rewrite
            # the durable execution outcome into the generic `stopped` state.
            preserved = self._save(record, **terminal_outcome)
            return self._preserve_inactive_terminal(spec, preserved)
        try:
            result = self._probe(spec)
        except Exception as error:
            failed = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                error=f"post-stop GPU result replay failed: {type(error).__name__}: {error}"[:2048],
            )
            raise GpuChildReconciliationRequired(failed.error or "result replay failed") from error
        if result.status == "invalid":
            failed = self._save(
                record,
                state="reconciliation_required",
                completed_at=self.now(),
                result=result.document(),
                error="post-stop GPU result replay found invalid results",
            )
            raise GpuChildReconciliationRequired(failed.error or "invalid GPU results")
        return self._save(
            record,
            state="stopped",
            completed_at=self.now(),
            result=result.document(),
            error=None,
        )


__all__ = [
    "BatchResultProbe",
    "BatchResultStatus",
    "CONTROLLER_SUPERVISOR_PID_ENV",
    "ControllerUnitContext",
    "GpuChildError",
    "GpuChildJournal",
    "GpuChildRecord",
    "GpuChildReconciliationRequired",
    "LocalPrivateGpuLaunchSpec",
    "PrivateGpuChildJournal",
    "ProductionBatchResultProbe",
    "SystemdGpuChildExecutor",
]
