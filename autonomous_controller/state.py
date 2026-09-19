"""Owner-private durable control, status, checkpoints, and event journal."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import (
    CONFIG_ID_RE,
    SHA256_RE,
    ControllerConfig,
    canonical_bytes,
    sha256_bytes,
)


STATE_SCHEMA_VERSION = 1
MAX_CONTROL_BYTES = 32 * 1024
MAX_STATUS_BYTES = 256 * 1024
# The queue restart document is independently bounded at 256 MiB.  Leave room
# for preprocess/GPU witnesses and the outer digest envelope while retaining a
# finite owner-private state bound.
MAX_CHECKPOINT_BYTES = 384 * 1024 * 1024
MAX_EVENT_BYTES = 256 * 1024
MAX_EVENTS = 1_000_000
EVENT_NAME_RE = re.compile(r"^([0-9]{12})-([0-9a-f]{16})\.json$")
CHECKPOINT_KIND = "himr_autonomous_controller_checkpoint"
CHECKPOINT_KEYS = {
    "kind",
    "schema_version",
    "config_id",
    "config_sha256",
    "created_at",
    "anchor",
    "backend",
    "checkpoint_sha256",
}
CHECKPOINT_ANCHOR_KEYS = {"sequence", "event_sha256", "event_type"}
MUTABLE_TEMP_DIRECTORY = ".mutable-tmp"
STATE_ROOT_ENTRIES = frozenset(
    {
        "control.json",
        "status.json",
        "checkpoint.json",
        "controller.lock",
        "control.lock",
        "events",
        "gpu-children",
        MUTABLE_TEMP_DIRECTORY,
    }
)


class StateError(RuntimeError):
    """Operational controller state is unsafe or internally inconsistent."""


@dataclass(frozen=True)
class RecoveryView:
    """One validated backend checkpoint and the journal events after its anchor."""

    checkpoint: dict[str, Any] | None
    tail_events: tuple[dict[str, Any], ...]
    anchor_sequence: int
    legacy_full_replay: bool


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_utc_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value


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


def _require_private_directory(path: Path, label: str) -> Path:
    try:
        if path.resolve(strict=True) != path:
            raise StateError(f"{label} may not traverse a symlink")
        observed = path.lstat()
    except StateError:
        raise
    except OSError as error:
        raise StateError(f"cannot inspect {label}: {error}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise StateError(f"{label} must be current-user-owned mode 0700")
    return path


def _open_lock(path: Path, label: str) -> int:
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    try:
        descriptor = os.open(path, flags, 0o600)
        opened = os.fstat(descriptor)
        linked = path.lstat()
    except OSError as error:
        if descriptor >= 0:
            os.close(descriptor)
        raise StateError(f"cannot open {label}: {error}") from error
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.geteuid()
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o600
        or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
    ):
        os.close(descriptor)
        raise StateError(f"{label} must be an owner-private single-link lock")
    return descriptor


def _stable_json(path: Path, *, maximum: int, label: str, mode: int) -> dict[str, Any]:
    try:
        inspected = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise StateError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != mode
            or not 1 <= opened.st_size <= maximum
            or (inspected.st_dev, inspected.st_ino, inspected.st_size, inspected.st_mode)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mode)
        ):
            raise StateError(f"{label} has unsafe metadata")
        body = bytearray()
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, opened.st_size - offset, offset)
            if not chunk:
                raise StateError(f"{label} ended during read")
            body.extend(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        linked = path.lstat()
        if _fingerprint(opened) != _fingerprint(after) or _fingerprint(after) != _fingerprint(linked):
            raise StateError(f"{label} changed during read")
    finally:
        os.close(descriptor)
    try:
        value = json.loads(bytes(body))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StateError(f"{label} is not strict JSON: {error}") from error
    if not isinstance(value, dict) or bytes(body) != canonical_bytes(value):
        raise StateError(f"{label} is not canonical JSON")
    return value


def _stable_mutable_json(
    path: Path, *, maximum: int, label: str, mode: int
) -> dict[str, Any]:
    """Read an atomically replaced mutable document across a bounded race.

    A reader may open the complete old inode immediately before ``os.replace``
    installs the complete new inode.  That is not torn data, but the strict path
    identity fence correctly reports a change.  Retry only that exact condition;
    every metadata, schema, and content failure still fails closed.
    """

    retryable = {
        f"{label} changed during read",
        # lstat may describe the old inode while open obtains the newly replaced
        # inode (or vice versa).  A persistent metadata fault still fails on the
        # eighth exact replay.
        f"{label} has unsafe metadata",
    }
    for attempt in range(8):
        try:
            return _stable_json(path, maximum=maximum, label=label, mode=mode)
        except StateError as error:
            if str(error) not in retryable or attempt == 7:
                raise
    raise StateError(f"{label} changed during read")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _mutable_temp_directory(root: Path, *, create: bool) -> Path | None:
    """Return the private scratch directory used by mutable state writers.

    Mutable documents are atomically renamed into the state root.  Keeping their
    temporary inodes in a dedicated same-filesystem directory prevents an exact
    top-level layout reader from mistaking an in-progress write for foreign state.
    """

    path = root / MUTABLE_TEMP_DIRECTORY
    try:
        path.lstat()
    except FileNotFoundError:
        if not create:
            return None
        created = False
        try:
            path.mkdir(mode=0o700)
            created = True
        except FileExistsError:
            # Another mutable writer may have installed the shared scratch
            # directory after our lstat.  Its metadata is verified below.
            pass
        if created:
            _fsync_directory(root)
    except OSError as error:
        raise StateError(
            f"cannot inspect controller mutable temp root: {error}"
        ) from error
    return _require_private_directory(path, "controller mutable temp root")


def _validate_state_root_layout(root: Path) -> None:
    unexpected = sorted(
        path.name for path in root.iterdir() if path.name not in STATE_ROOT_ENTRIES
    )
    if unexpected:
        raise StateError(f"controller state root has unexpected entries: {unexpected}")
    if (root / MUTABLE_TEMP_DIRECTORY).exists() or (
        root / MUTABLE_TEMP_DIRECTORY
    ).is_symlink():
        _mutable_temp_directory(root, create=False)


def _atomic_mutable_json(path: Path, value: dict[str, Any], *, maximum: int) -> None:
    body = canonical_bytes(value)
    if not 1 <= len(body) <= maximum:
        raise StateError(f"{path.name} exceeds its byte cap")
    if path.exists() or path.is_symlink():
        observed = path.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o600
        ):
            raise StateError(f"existing {path.name} has unsafe metadata")
    temporary_root = _mutable_temp_directory(path.parent, create=True)
    if temporary_root is None:  # pragma: no cover - create=True is exhaustive
        raise StateError("controller mutable temp root was not created")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp-", dir=temporary_root
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise StateError(f"{path.name} write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(temporary_root)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        else:
            _fsync_directory(temporary_root)


def _default_control(config: ControllerConfig) -> dict[str, Any]:
    return {
        "kind": "himr_autonomous_controller_control",
        "schema_version": STATE_SCHEMA_VERSION,
        "config_id": config.config_id,
        "generation": 0,
        "desired_state": "stopped",
        "requested_at": None,
    }


def _read_control_path(config: ControllerConfig, path: Path) -> dict[str, Any]:
    if not path.exists() and not path.is_symlink():
        return _default_control(config)
    value = _stable_mutable_json(
        path,
        maximum=MAX_CONTROL_BYTES,
        label="controller control",
        mode=0o600,
    )
    expected = {
        "kind",
        "schema_version",
        "config_id",
        "generation",
        "desired_state",
        "requested_at",
    }
    if (
        set(value) != expected
        or value["kind"] != "himr_autonomous_controller_control"
        or value["schema_version"] != STATE_SCHEMA_VERSION
        or value["config_id"] != config.config_id
        or isinstance(value["generation"], bool)
        or not isinstance(value["generation"], int)
        or not 0 <= value["generation"] <= 2**63 - 1
        or value["desired_state"] not in {"running", "stopped"}
        or (
            value["requested_at"] is not None
            and not isinstance(value["requested_at"], str)
        )
    ):
        raise StateError("controller control document is invalid")
    return value


def _lightweight_state_root(config: ControllerConfig) -> Path:
    """Validate only the fixed top-level state layout, never the event journal."""

    root = _require_private_directory(config.state_root, "controller state root")
    _validate_state_root_layout(root)
    return root


def read_control_state(config: ControllerConfig) -> dict[str, Any]:
    """Read only the small exact-config-bound control document."""

    root = _lightweight_state_root(config)
    return _read_control_path(config, root / "control.json")


def set_desired_state_lightweight(
    config: ControllerConfig,
    desired_state: str,
    *,
    requested_at: str | None = None,
) -> dict[str, Any]:
    """Atomically update control state without enumerating recovery events."""

    if desired_state not in {"running", "stopped"}:
        raise StateError("desired state is unsupported")
    root = _lightweight_state_root(config)
    control_path = root / "control.json"
    descriptor = _open_lock(root / "control.lock", "controller control lock")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        previous = _read_control_path(config, control_path)
        if previous["generation"] >= 2**63 - 1:
            raise StateError("controller control generation is exhausted")
        document = {
            "kind": "himr_autonomous_controller_control",
            "schema_version": STATE_SCHEMA_VERSION,
            "config_id": config.config_id,
            "generation": previous["generation"] + 1,
            "desired_state": desired_state,
            "requested_at": requested_at or utc_now(),
        }
        _atomic_mutable_json(control_path, document, maximum=MAX_CONTROL_BYTES)
        return document
    finally:
        with suppress_os_error():
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def request_stop(config: ControllerConfig, *, requested_at: str | None = None) -> dict[str, Any]:
    """Persist a graceful stop request in O(1) operational state."""

    return set_desired_state_lightweight(
        config, "stopped", requested_at=requested_at
    )


def request_start(config: ControllerConfig, *, requested_at: str | None = None) -> dict[str, Any]:
    """Persist start intent before an outer systemd worker is admitted."""

    return set_desired_state_lightweight(
        config, "running", requested_at=requested_at
    )


class ControlStore:
    """Durable state rooted in one pre-created owner-only directory."""

    def __init__(self, config: ControllerConfig):
        self._thread_lock = threading.RLock()
        self.config = config
        self.root = _lightweight_state_root(config)
        self.control_path = self.root / "control.json"
        self.status_path = self.root / "status.json"
        self.checkpoint_path = self.root / "checkpoint.json"
        self.run_lock_path = self.root / "controller.lock"
        self.control_lock_path = self.root / "control.lock"
        self.events_root = self.root / "events"
        self.gpu_children_root = self.root / "gpu-children"
        if not self.events_root.exists() and not self.events_root.is_symlink():
            self.events_root.mkdir(mode=0o700)
            _fsync_directory(self.root)
        if not self.gpu_children_root.exists() and not self.gpu_children_root.is_symlink():
            self.gpu_children_root.mkdir(mode=0o700)
            _fsync_directory(self.root)
        _require_private_directory(self.events_root, "controller event root")
        _require_private_directory(
            self.gpu_children_root, "controller GPU child journal root"
        )
        _validate_state_root_layout(self.root)
        self._events = self.load_events()

    @contextmanager
    def run_lock(self) -> Iterator[None]:
        descriptor = _open_lock(self.run_lock_path, "controller run lock")
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno in {errno.EACCES, errno.EAGAIN}:
                    raise StateError("another controller run is active") from error
                raise StateError(f"controller run lock failed: {error}") from error
            yield
        finally:
            with suppress_os_error():
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @contextmanager
    def _control_lock(self) -> Iterator[None]:
        descriptor = _open_lock(self.control_lock_path, "controller control lock")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            with suppress_os_error():
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def read_control(self) -> dict[str, Any]:
        return _read_control_path(self.config, self.control_path)

    def set_desired_state(self, desired_state: str, *, requested_at: str | None = None) -> dict[str, Any]:
        if desired_state not in {"running", "stopped"}:
            raise StateError("desired state is unsupported")
        with self._control_lock():
            previous = self.read_control()
            if previous["generation"] >= 2**63 - 1:
                raise StateError("controller control generation is exhausted")
            document = {
                "kind": "himr_autonomous_controller_control",
                "schema_version": STATE_SCHEMA_VERSION,
                "config_id": self.config.config_id,
                "generation": previous["generation"] + 1,
                "desired_state": desired_state,
                "requested_at": requested_at or utc_now(),
            }
            _atomic_mutable_json(self.control_path, document, maximum=MAX_CONTROL_BYTES)
            return document

    def read_status(self) -> dict[str, Any] | None:
        with self._thread_lock:
            if not self.status_path.exists() and not self.status_path.is_symlink():
                return None
            value = _stable_mutable_json(
                self.status_path,
                maximum=MAX_STATUS_BYTES,
                label="controller status",
                mode=0o600,
            )
            if (
                value.get("kind") != "himr_autonomous_controller_status"
                or value.get("schema_version") != STATE_SCHEMA_VERSION
                or value.get("config_id") != self.config.config_id
            ):
                raise StateError("controller status is for a different configuration")
            return value

    def write_status(self, value: dict[str, Any]) -> None:
        if (
            not isinstance(value, dict)
            or value.get("kind") != "himr_autonomous_controller_status"
            or value.get("schema_version") != STATE_SCHEMA_VERSION
            or value.get("config_id") != self.config.config_id
        ):
            raise StateError("refusing an invalid controller status document")
        with self._thread_lock:
            _atomic_mutable_json(self.status_path, value, maximum=MAX_STATUS_BYTES)

    def _read_checkpoint_locked(self) -> dict[str, Any] | None:
        if not self.checkpoint_path.exists() and not self.checkpoint_path.is_symlink():
            return None
        value = _stable_mutable_json(
            self.checkpoint_path,
            maximum=MAX_CHECKPOINT_BYTES,
            label="controller checkpoint",
            mode=0o600,
        )
        if set(value) != CHECKPOINT_KEYS:
            raise StateError("controller checkpoint has unexpected fields")
        anchor = value["anchor"]
        if not isinstance(anchor, dict) or set(anchor) != CHECKPOINT_ANCHOR_KEYS:
            raise StateError("controller checkpoint anchor is invalid")
        sequence = anchor["sequence"]
        checkpoint_sha256 = value["checkpoint_sha256"]
        if (
            value["kind"] != CHECKPOINT_KIND
            or isinstance(value["schema_version"], bool)
            or not isinstance(value["schema_version"], int)
            or value["schema_version"] != STATE_SCHEMA_VERSION
            or not isinstance(value["config_id"], str)
            or CONFIG_ID_RE.fullmatch(value["config_id"]) is None
            or value["config_id"] != self.config.config_id
            or not isinstance(value["config_sha256"], str)
            or SHA256_RE.fullmatch(value["config_sha256"]) is None
            or value["config_sha256"] != self.config.physical_sha256
            or not _is_utc_timestamp(value["created_at"])
            or not isinstance(value["backend"], dict)
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or not 1 <= sequence <= MAX_EVENTS
            or not isinstance(anchor["event_sha256"], str)
            or SHA256_RE.fullmatch(anchor["event_sha256"]) is None
            or not isinstance(anchor["event_type"], str)
            or not anchor["event_type"]
            or len(anchor["event_type"]) > 128
            or not isinstance(checkpoint_sha256, str)
            or SHA256_RE.fullmatch(checkpoint_sha256) is None
        ):
            raise StateError("controller checkpoint is invalid")
        core = {
            key: value[key]
            for key in CHECKPOINT_KEYS - {"checkpoint_sha256"}
        }
        if checkpoint_sha256 != sha256_bytes(canonical_bytes(core)):
            raise StateError("controller checkpoint digest is invalid")
        if sequence > len(self._events):
            raise StateError("controller checkpoint anchor is beyond the event journal")
        event = self._events[sequence - 1]
        expected_anchor = {
            "sequence": event["sequence"],
            "event_sha256": event["event_sha256"],
            "event_type": event["event_type"],
        }
        if anchor != expected_anchor:
            raise StateError("controller checkpoint anchor does not match the event journal")
        return value

    def read_checkpoint(self) -> dict[str, Any] | None:
        """Read and fully validate the optional config-bound checkpoint."""

        with self._thread_lock:
            return self._read_checkpoint_locked()

    def write_checkpoint(
        self,
        backend: dict[str, Any],
        *,
        created_at: str | None = None,
        expected_anchor_sequence: int | None = None,
        expected_anchor_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Atomically checkpoint backend state at an optionally expected journal head."""

        if not isinstance(backend, dict):
            raise StateError("checkpoint backend state must be an object")
        timestamp = utc_now() if created_at is None else created_at
        if not _is_utc_timestamp(timestamp):
            raise StateError("checkpoint creation time must be canonical UTC")
        if (expected_anchor_sequence is None) != (expected_anchor_sha256 is None):
            raise StateError("checkpoint expected anchor must include sequence and hash")
        if expected_anchor_sequence is not None and (
            isinstance(expected_anchor_sequence, bool)
            or not isinstance(expected_anchor_sequence, int)
            or not 1 <= expected_anchor_sequence <= MAX_EVENTS
            or not isinstance(expected_anchor_sha256, str)
            or SHA256_RE.fullmatch(expected_anchor_sha256) is None
        ):
            raise StateError("checkpoint expected anchor is invalid")
        with self._thread_lock:
            if not self._events:
                raise StateError("checkpoint requires an event-journal anchor")
            event = self._events[-1]
            if expected_anchor_sequence is not None and (
                event["sequence"] != expected_anchor_sequence
                or event["event_sha256"] != expected_anchor_sha256
            ):
                raise StateError(
                    "checkpoint journal head changed since backend snapshot"
                )
            core = {
                "kind": CHECKPOINT_KIND,
                "schema_version": STATE_SCHEMA_VERSION,
                "config_id": self.config.config_id,
                "config_sha256": self.config.physical_sha256,
                "created_at": timestamp,
                "anchor": {
                    "sequence": event["sequence"],
                    "event_sha256": event["event_sha256"],
                    "event_type": event["event_type"],
                },
                "backend": dict(backend),
            }
            document = {
                **core,
                "checkpoint_sha256": sha256_bytes(canonical_bytes(core)),
            }
            _atomic_mutable_json(
                self.checkpoint_path,
                document,
                maximum=MAX_CHECKPOINT_BYTES,
            )
            return document

    def recovery_view(self) -> RecoveryView:
        """Return a checkpoint and only its tail, or the full legacy journal."""

        with self._thread_lock:
            checkpoint = self._read_checkpoint_locked()
            if checkpoint is None:
                return RecoveryView(
                    checkpoint=None,
                    tail_events=tuple(self._events),
                    anchor_sequence=0,
                    legacy_full_replay=True,
                )
            sequence = checkpoint["anchor"]["sequence"]
            return RecoveryView(
                checkpoint=checkpoint,
                tail_events=tuple(self._events[sequence:]),
                anchor_sequence=sequence,
                legacy_full_replay=False,
            )

    def load_events(self) -> list[dict[str, Any]]:
        entries = sorted(self.events_root.iterdir(), key=lambda path: path.name)
        if len(entries) > MAX_EVENTS:
            raise StateError("controller event journal exceeds its entry cap")
        events: list[dict[str, Any]] = []
        previous_hash: str | None = None
        for sequence, path in enumerate(entries, 1):
            match = EVENT_NAME_RE.fullmatch(path.name)
            if match is None or int(match.group(1)) != sequence:
                raise StateError("controller event journal is not contiguous")
            value = _stable_json(
                path,
                maximum=MAX_EVENT_BYTES,
                label=f"controller event {sequence}",
                mode=0o400,
            )
            expected_keys = {
                "kind",
                "schema_version",
                "config_id",
                "sequence",
                "previous_event_sha256",
                "event_type",
                "occurred_at",
                "payload",
                "event_sha256",
            }
            core = {key: value[key] for key in expected_keys - {"event_sha256"}} if set(value) == expected_keys else None
            digest = sha256_bytes(canonical_bytes(core)) if core is not None else None
            if (
                core is None
                or value["kind"] != "himr_autonomous_controller_event"
                or value["schema_version"] != STATE_SCHEMA_VERSION
                or value["config_id"] != self.config.config_id
                or value["sequence"] != sequence
                or value["previous_event_sha256"] != previous_hash
                or not isinstance(value["event_type"], str)
                or not value["event_type"]
                or not isinstance(value["occurred_at"], str)
                or not isinstance(value["payload"], dict)
                or value["event_sha256"] != digest
                or match.group(2) != digest[:16]
            ):
                raise StateError(f"controller event {sequence} is invalid")
            events.append(value)
            previous_hash = digest
        return events

    @property
    def events(self) -> tuple[dict[str, Any], ...]:
        with self._thread_lock:
            return tuple(self._events)

    def append_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        with self._thread_lock:
            if not isinstance(event_type, str) or not event_type or len(event_type) > 128:
                raise StateError("event type must be bounded text")
            if not isinstance(payload, dict):
                raise StateError("event payload must be an object")
            sequence = len(self._events) + 1
            if sequence > MAX_EVENTS:
                raise StateError("controller event journal is full")
            previous = None if not self._events else self._events[-1]["event_sha256"]
            core = {
                "kind": "himr_autonomous_controller_event",
                "schema_version": STATE_SCHEMA_VERSION,
                "config_id": self.config.config_id,
                "sequence": sequence,
                "previous_event_sha256": previous,
                "event_type": event_type,
                "occurred_at": occurred_at or utc_now(),
                "payload": payload,
            }
            digest = sha256_bytes(canonical_bytes(core))
            document = {**core, "event_sha256": digest}
            body = canonical_bytes(document)
            if len(body) > MAX_EVENT_BYTES:
                raise StateError("controller event exceeds its byte cap")
            path = self.events_root / f"{sequence:012d}-{digest[:16]}.json"
            descriptor, temporary_name = tempfile.mkstemp(prefix=f".{sequence:012d}.tmp-", dir=self.events_root)
            temporary = Path(temporary_name)
            try:
                offset = 0
                while offset < len(body):
                    written = os.write(descriptor, body[offset:])
                    if written <= 0:
                        raise StateError("event write made no progress")
                    offset += written
                os.fchmod(descriptor, 0o400)
                os.fsync(descriptor)
                os.close(descriptor)
                descriptor = -1
                try:
                    os.link(temporary, path)
                except FileExistsError as error:
                    raise StateError("controller event admission raced") from error
                _fsync_directory(self.events_root)
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
                temporary.unlink(missing_ok=True)
                _fsync_directory(self.events_root)
            self._events.append(document)
            return document


@contextmanager
def suppress_os_error() -> Iterator[None]:
    try:
        yield
    except OSError:
        pass
