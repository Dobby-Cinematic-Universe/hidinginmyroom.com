"""Operational replay for immutable acquisition queue results.

The sealed acquisition modules deliberately perform a complete payload replay on
each invocation.  This adapter leaves their source bytes untouched and installs one
persistent, process-local dispatch layer around ``queue_runner._scan_results`` and
``queue_runner._inspect_result``.  A successful two-pass deep replay seeds a cache;
later calls reuse a completed result only while exact path/descriptor metadata is
unchanged.  New or changed paths always return to the original exact inspector.

The original v1 checkpoint remains digest/lineage-only and is not restart
authority.  The separately named v2 restart checkpoint persists the complete
admitted result state and its filesystem witnesses.  A fresh process can hydrate
that checkpoint by rereading only the bounded result envelope and stable no-follow
metadata for unchanged files; any new or changed ordinal returns to the original
exact payload inspector.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import stat
import threading
import time
import weakref
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Sequence


ADAPTER_NAME = "himr-in-process-queue-operational-replay"
ADAPTER_KIND = "himr_in_process_queue_operational_replay_v1"
IMPLEMENTATION_VERSION = "0.1.0"
CHECKPOINT_KIND = "himr_queue_operational_replay_checkpoint"
SCHEMA_VERSION = 1
RESTART_CHECKPOINT_KIND = "himr_queue_operational_replay_restart_checkpoint"
RESTART_CHECKPOINT_SCHEMA_VERSION = 2
RESTART_CHECKPOINT_IMPLEMENTATION_VERSION = "0.2.0"
MAX_ORDERS = 10_000
MAX_SNAPSHOTS = 10_000
MAX_CHECKPOINT_ORDERS = 50_000
MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024
MAX_RESTART_CHECKPOINT_BYTES = 256 * 1024 * 1024
MAX_WITNESS_ANCESTORS = 128
MAX_REMEMBERED_DELTAS = 65_536
MAX_RESULT_ENVELOPE_BYTES = 16 * 1024 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
# ``acquire.atomic_write`` creates the immutable result directory before its
# fsynced temporary envelope is renamed to ``result.json``.  A peer reader can
# therefore observe one safe, short-lived incomplete shape.  Retry that one
# exact source error locally so a 4,000-item runtime replay is not repeated from
# ordinal one.  Every attempt repeats the complete metadata bracket below; no
# partial observation is admitted to the session snapshot.
ACQUISITION_RESULT_ADMISSION_ATTEMPTS = 64
ACQUISITION_RESULT_ADMISSION_RETRY_SECONDS = 0.025
_PENDING_RESULT_ADMITTED_DURING_BRACKET = (
    "exact pending result changed before its metadata bracket closed"
)


class OperationalReplayError(RuntimeError):
    """The replay adapter could not preserve its fail-closed contract."""


class DeepAuditRequired(OperationalReplayError):
    """No same-process witness may be reused until a fresh deep replay succeeds."""


class CheckpointDeferred(OperationalReplayError):
    """Logical shared queue authority is ahead of the journal-observed root."""


def canonical_bytes(value: Any) -> bytes:
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
        raise OperationalReplayError(
            f"operational replay value is not canonical JSON: {error}"
        ) from error


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise OperationalReplayError(f"{label} must be a lowercase SHA-256")
    return value


def _text(value: Any, label: str, maximum: int = 4_096) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise OperationalReplayError(f"{label} must be bounded non-empty text")
    return value


def _integer(value: Any, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise OperationalReplayError(f"{label} must be an integer >= {minimum}")
    return value


def _lexical_absolute(path: Path | str, label: str) -> Path:
    try:
        raw = os.fspath(path)
    except TypeError as error:
        raise OperationalReplayError(f"{label} must be path-like") from error
    if not isinstance(raw, str) or not raw or "\x00" in raw or "://" in raw:
        raise OperationalReplayError(f"{label} must be a bounded local path")
    return Path(os.path.abspath(raw))


@dataclass(frozen=True)
class QueueBinding:
    """Exact controller binding for one queue schedule."""

    schedule_id: str
    role: str
    schedule_sha256: str
    manifest_path: Path
    manifest_sha256: str
    bundle_id: str

    def __post_init__(self) -> None:
        _text(self.schedule_id, "schedule ID", 128)
        _text(self.role, "schedule role", 128)
        _digest(self.schedule_sha256, "schedule SHA-256")
        object.__setattr__(
            self,
            "manifest_path",
            _lexical_absolute(self.manifest_path, "queue manifest path"),
        )
        _digest(self.manifest_sha256, "queue manifest SHA-256")
        _text(self.bundle_id, "queue bundle ID", 128)


@dataclass(frozen=True)
class _FileFingerprint:
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    mode: int
    link_count: int
    uid: int


@dataclass(frozen=True)
class _DirectoryIdentity:
    path: str
    fingerprint: _FileFingerprint


@dataclass(frozen=True)
class _CompletedWitness:
    result_file: _FileFingerprint
    result_parent: _FileFingerprint
    result_ancestors: tuple[_DirectoryIdentity, ...]
    payload_file: _FileFingerprint
    payload_ancestors: tuple[_DirectoryIdentity, ...]


@dataclass(frozen=True)
class _OrderBinding:
    ordinal: int
    job_id: str
    work_order_sha256: str
    work_order_file_sha256: str
    result_path: str


@dataclass
class _QueueSnapshot:
    binding: QueueBinding
    generation: int
    orders: tuple[_OrderBinding, ...]
    states: list[dict[str, Any] | None]
    witnesses: list[_CompletedWitness | None]
    state_digest: str


class PreparedRestartCheckpoint:
    """Process-local parsed v2 document token for many schedule hydrations.

    Construction is owned by :meth:`OperationalReplayStore.prepare_restart_checkpoint`.
    The token prevents an all-schedules document from being canonicalized, parsed,
    and deep-copied once per schedule during startup.
    """

    __slots__ = ("_authority", "_document", "_pid", "_schedules")

    def __init__(
        self,
        *,
        authority: object,
        document: dict[str, Any],
        schedules: dict[str, dict[str, Any]],
    ):
        self._authority = authority
        self._pid = os.getpid()
        self._document = document
        self._schedules = schedules

    @property
    def identity_sha256(self) -> str:
        return self._document["identity_sha256"]

    @property
    def schedule_count(self) -> int:
        return len(self._schedules)


_RESTART_POLICY = {
    "rebuildable": True,
    "cross_process_witness_reuse": True,
    "filesystem_witnesses_persisted": True,
    "filesystem_identity_authority": "externally_validated_filesystem_uuid",
    "unchanged_completed_validation": (
        "bounded_result_envelope_plus_stable_nofollow_metadata"
    ),
    "changed_ordinal_validation": "original_exact_payload_inspector",
    "completed_removal_policy": "fail_closed",
    "completed_conflict_policy": "fail_closed",
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "deletion_authority": "none",
}


def _full_fingerprint(value: os.stat_result) -> _FileFingerprint:
    return _FileFingerprint(
        device=value.st_dev,
        inode=value.st_ino,
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
        mode=value.st_mode,
        link_count=value.st_nlink,
        uid=value.st_uid,
    )


def _directory_identity(path: Path, observed: os.stat_result) -> _DirectoryIdentity:
    return _DirectoryIdentity(
        path=str(path),
        fingerprint=_full_fingerprint(observed),
    )


def _open_stable_regular(
    path: Path,
    *,
    label: str,
    require_owner: bool,
    reject_peer_writable: bool,
) -> _FileFingerprint:
    path = _lexical_absolute(path, label)
    try:
        before_path = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise OperationalReplayError(f"cannot inspect {label}") from error
    try:
        opened = os.fstat(descriptor)
        after = os.fstat(descriptor)
        after_path = path.lstat()
    except OSError as error:
        raise OperationalReplayError(f"{label} changed while inspecting metadata") from error
    finally:
        os.close(descriptor)
    values = tuple(
        _full_fingerprint(value)
        for value in (before_path, opened, after, after_path)
    )
    if len(set(values)) != 1:
        raise OperationalReplayError(f"{label} changed while inspecting metadata")
    observed = values[0]
    if (
        stat.S_ISLNK(observed.mode)
        or not stat.S_ISREG(observed.mode)
        or observed.link_count != 1
        or (require_owner and observed.uid != os.getuid())
        or (reject_peer_writable and stat.S_IMODE(observed.mode) & 0o022)
    ):
        raise OperationalReplayError(f"{label} has unsafe metadata")
    return observed


def _read_stable_regular(
    path: Path,
    *,
    label: str,
    maximum: int,
) -> tuple[bytes, _FileFingerprint]:
    """Read one bounded regular file through a no-follow stable descriptor."""

    path = _lexical_absolute(path, label)
    descriptor = -1
    try:
        before_path = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(opened.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_size < 1
            or opened.st_size > maximum
        ):
            raise OperationalReplayError(f"{label} has unsafe metadata or size")
        chunks = []
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(
                descriptor,
                min(1024 * 1024, opened.st_size - offset),
                offset,
            )
            if not chunk:
                raise OperationalReplayError(f"{label} ended during stable read")
            chunks.append(chunk)
            offset += len(chunk)
        if os.pread(descriptor, 1, opened.st_size):
            raise OperationalReplayError(f"{label} grew during stable read")
        after = os.fstat(descriptor)
        after_path = path.lstat()
    except OperationalReplayError:
        raise
    except OSError as error:
        raise OperationalReplayError(f"cannot read {label}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    values = tuple(
        _full_fingerprint(value)
        for value in (before_path, opened, after, after_path)
    )
    if len(set(values)) != 1:
        raise OperationalReplayError(f"{label} changed during stable read")
    return b"".join(chunks), values[0]


def _open_stable_directory(path: Path, *, label: str) -> os.stat_result:
    path = _lexical_absolute(path, label)
    descriptor = -1
    try:
        before_path = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_DIRECTORY", 0),
        )
        opened = os.fstat(descriptor)
        after = os.fstat(descriptor)
        after_path = path.lstat()
    except OSError as error:
        raise OperationalReplayError(f"cannot inspect {label}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    values = tuple(
        _full_fingerprint(value)
        for value in (before_path, opened, after, after_path)
    )
    if len(set(values)) != 1 or not stat.S_ISDIR(values[0].mode):
        raise OperationalReplayError(f"{label} changed or is not a real directory")
    return after


def _directory_names_and_fingerprint(
    path: Path, *, label: str
) -> tuple[set[str], _FileFingerprint]:
    path = _lexical_absolute(path, label)
    descriptor = -1
    try:
        before_path = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_DIRECTORY", 0),
        )
        opened = os.fstat(descriptor)
        names = set(os.listdir(descriptor))
        after = os.fstat(descriptor)
        after_path = path.lstat()
    except OSError as error:
        raise OperationalReplayError(f"cannot enumerate {label}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    values = tuple(
        _full_fingerprint(value)
        for value in (before_path, opened, after, after_path)
    )
    if len(set(values)) != 1 or not stat.S_ISDIR(values[0].mode):
        raise OperationalReplayError(f"{label} changed while enumerating")
    return names, values[0]


def _ancestor_identities(
    root: Path, leaf: Path, *, label: str
) -> tuple[_DirectoryIdentity, ...]:
    root = _lexical_absolute(root, f"{label} root")
    leaf = _lexical_absolute(leaf, label)
    if leaf == root or root not in leaf.parents:
        raise OperationalReplayError(f"{label} must be strictly beneath its root")
    relative = leaf.relative_to(root)
    paths = [root]
    current = root
    for component in relative.parts[:-1]:
        current = current / component
        paths.append(current)
    identities = []
    for ordinal, path in enumerate(paths):
        observed = _open_stable_directory(
            path, label=f"{label} ancestor {ordinal}"
        )
        identities.append(_directory_identity(path, observed))
    return tuple(identities)


def _state_row(
    order: _OrderBinding, state: dict[str, Any] | None
) -> dict[str, Any]:
    if state is None:
        return {
            "ordinal": order.ordinal,
            "job_id": order.job_id,
            "work_order_file_sha256": order.work_order_file_sha256,
            "work_order_identity_sha256": order.work_order_sha256,
            "status": "pending",
            "result_path": order.result_path,
            "result_sha256": None,
            "media_sha256": None,
            "media_byte_count": None,
        }
    return {
        "ordinal": order.ordinal,
        "job_id": order.job_id,
        "work_order_file_sha256": order.work_order_file_sha256,
        "work_order_identity_sha256": order.work_order_sha256,
        "status": "completed",
        "result_path": order.result_path,
        "result_sha256": state["result_sha256"],
        "media_sha256": state["media_sha256"],
        "media_byte_count": state["byte_count"],
    }


def _state_digest(
    orders: Sequence[_OrderBinding], states: Sequence[dict[str, Any] | None]
) -> str:
    return _sha256(
        canonical_bytes(
            [_state_row(order, state) for order, state in zip(orders, states, strict=True)]
        )
    )


def _fingerprint_document(value: _FileFingerprint) -> dict[str, int]:
    return {
        "device": value.device,
        "inode": value.inode,
        "size": value.size,
        "mtime_ns": value.mtime_ns,
        "ctime_ns": value.ctime_ns,
        "mode": value.mode,
        "link_count": value.link_count,
        "uid": value.uid,
    }


def _fingerprint_from_document(
    value: Any, *, label: str, expected_kind: str
) -> _FileFingerprint:
    keys = {
        "device",
        "inode",
        "size",
        "mtime_ns",
        "ctime_ns",
        "mode",
        "link_count",
        "uid",
    }
    if not isinstance(value, dict) or set(value) != keys:
        raise OperationalReplayError(f"{label} has unexpected fields")
    parsed = _FileFingerprint(
        device=_integer(value["device"], f"{label} device"),
        inode=_integer(value["inode"], f"{label} inode"),
        size=_integer(value["size"], f"{label} size"),
        mtime_ns=_integer(value["mtime_ns"], f"{label} mtime"),
        ctime_ns=_integer(value["ctime_ns"], f"{label} ctime"),
        mode=_integer(value["mode"], f"{label} mode"),
        link_count=_integer(value["link_count"], f"{label} link count", 1),
        uid=_integer(value["uid"], f"{label} uid"),
    )
    expected = stat.S_ISREG if expected_kind == "regular" else stat.S_ISDIR
    if not expected(parsed.mode):
        raise OperationalReplayError(f"{label} is not a persisted {expected_kind}")
    return parsed


def _directory_document(value: _DirectoryIdentity) -> dict[str, Any]:
    return {
        "path": value.path,
        "fingerprint": _fingerprint_document(value.fingerprint),
    }


def _directory_from_document(value: Any, *, label: str) -> _DirectoryIdentity:
    if not isinstance(value, dict) or set(value) != {"path", "fingerprint"}:
        raise OperationalReplayError(f"{label} has unexpected fields")
    path = _lexical_absolute(value["path"], f"{label} path")
    if value["path"] != str(path):
        raise OperationalReplayError(f"{label} path is not lexical absolute")
    return _DirectoryIdentity(
        path=str(path),
        fingerprint=_fingerprint_from_document(
            value["fingerprint"], label=f"{label} fingerprint", expected_kind="directory"
        ),
    )


def _witness_document(value: _CompletedWitness) -> dict[str, Any]:
    return {
        "result_file": _fingerprint_document(value.result_file),
        "result_parent": _fingerprint_document(value.result_parent),
        "result_ancestors": [
            _directory_document(item) for item in value.result_ancestors
        ],
        "payload_file": _fingerprint_document(value.payload_file),
        "payload_ancestors": [
            _directory_document(item) for item in value.payload_ancestors
        ],
    }


def _witness_from_document(value: Any, *, label: str) -> _CompletedWitness:
    keys = {
        "result_file",
        "result_parent",
        "result_ancestors",
        "payload_file",
        "payload_ancestors",
    }
    if not isinstance(value, dict) or set(value) != keys:
        raise OperationalReplayError(f"{label} has unexpected fields")
    result_ancestors = value["result_ancestors"]
    payload_ancestors = value["payload_ancestors"]
    if (
        not isinstance(result_ancestors, list)
        or not isinstance(payload_ancestors, list)
        or not 1 <= len(result_ancestors) <= MAX_WITNESS_ANCESTORS
        or not 1 <= len(payload_ancestors) <= MAX_WITNESS_ANCESTORS
    ):
        raise OperationalReplayError(f"{label} ancestor vector is invalid")
    return _CompletedWitness(
        result_file=_fingerprint_from_document(
            value["result_file"], label=f"{label} result file", expected_kind="regular"
        ),
        result_parent=_fingerprint_from_document(
            value["result_parent"],
            label=f"{label} result parent",
            expected_kind="directory",
        ),
        result_ancestors=tuple(
            _directory_from_document(item, label=f"{label} result ancestor {index}")
            for index, item in enumerate(result_ancestors)
        ),
        payload_file=_fingerprint_from_document(
            value["payload_file"], label=f"{label} payload file", expected_kind="regular"
        ),
        payload_ancestors=tuple(
            _directory_from_document(item, label=f"{label} payload ancestor {index}")
            for index, item in enumerate(payload_ancestors)
        ),
    )


def _expected_ancestor_paths(root: Path, leaf: Path, *, label: str) -> tuple[str, ...]:
    root = _lexical_absolute(root, f"{label} root")
    leaf = _lexical_absolute(leaf, label)
    if leaf == root or root not in leaf.parents:
        raise OperationalReplayError(f"{label} must be strictly beneath its root")
    relative = leaf.relative_to(root)
    paths = [root]
    current = root
    for component in relative.parts[:-1]:
        current = current / component
        paths.append(current)
    return tuple(str(path) for path in paths)


def _canonical_utc(value: Any, label: str) -> str:
    value = _text(value, label, 64)
    try:
        if datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ) != value:
            raise ValueError(value)
    except ValueError as error:
        raise OperationalReplayError(f"{label} must be canonical UTC") from error
    return value


def _restart_state_from_document(
    value: Any, *, label: str
) -> dict[str, Any] | None:
    if value is None:
        return None
    keys = {"result", "result_sha256", "media_sha256", "byte_count"}
    if not isinstance(value, dict) or set(value) != keys:
        raise OperationalReplayError(f"{label} has unexpected fields")
    result = value["result"]
    if not isinstance(result, dict) or not isinstance(result.get("admission"), dict):
        raise OperationalReplayError(f"{label} result is malformed")
    admission = result["admission"]
    result_sha256 = _digest(value["result_sha256"], f"{label} result SHA-256")
    media_sha256 = _digest(value["media_sha256"], f"{label} media SHA-256")
    byte_count = _integer(value["byte_count"], f"{label} byte count", 1)
    try:
        admitted_sha256 = _digest(
            admission.get("sha256"), f"{label} admitted media SHA-256"
        )
        admitted_byte_count = _integer(
            admission.get("byte_count"), f"{label} admitted byte count", 1
        )
        _text(admission.get("path"), f"{label} admitted path", 16_384)
    except OperationalReplayError as error:
        raise OperationalReplayError(f"{label} admission is malformed") from error
    if admitted_sha256 != media_sha256 or admitted_byte_count != byte_count:
        raise OperationalReplayError(f"{label} admission is malformed")
    # Force the entire nested result through the same bounded canonical-JSON
    # value domain used by the checkpoint identity before retaining it.
    canonical_bytes(result)
    return {
        "result": copy.deepcopy(result),
        "result_sha256": result_sha256,
        "media_sha256": media_sha256,
        "byte_count": byte_count,
    }


class QueueReplayRouter:
    """One persistent dispatcher installed on an exact queue-runner module."""

    def __init__(self, queue_runner: ModuleType):
        self.queue_runner = queue_runner
        self.original_scan_results = self._required_callable("_scan_results")
        self.original_inspect_result = self._required_callable("_inspect_result")
        for name in (
            "_result_path",
            "_safe_existing_result_parents",
            "canonical_bytes",
            "sha256_bytes",
        ):
            self._required_callable(name)
        acquire = getattr(queue_runner, "acquire", None)
        if not callable(getattr(acquire, "pretty_json", None)):
            raise OperationalReplayError(
                "queue runner lacks callable acquire.pretty_json"
            )
        self._local = threading.local()

        def scan_wrapper(bundle: dict[str, Any]) -> list[dict[str, Any] | None]:
            session = self.current_session()
            if session is None:
                return self.original_scan_results(bundle)
            return session.scan_results(bundle)

        def inspect_wrapper(order: dict[str, Any]) -> dict[str, Any] | None:
            session = self.current_session()
            if session is None:
                return self.original_inspect_result(order)
            if getattr(self._local, "inside_original_scan", False):
                return session.observe_deep_scan_inspection(order)
            return session.inspect_direct(order)

        scan_wrapper.__name__ = "himr_operational_scan_results_v1"
        inspect_wrapper.__name__ = "himr_operational_inspect_result_v1"
        setattr(scan_wrapper, "__himr_operational_replay_router__", self)
        setattr(inspect_wrapper, "__himr_operational_replay_router__", self)
        self.scan_wrapper = scan_wrapper
        self.inspect_wrapper = inspect_wrapper

    def _required_callable(self, name: str) -> Callable[..., Any]:
        value = getattr(self.queue_runner, name, None)
        if not callable(value):
            raise OperationalReplayError(f"queue runner lacks callable {name}")
        return value

    def install(self) -> None:
        if (
            getattr(self.queue_runner, "_scan_results", None)
            is not self.original_scan_results
            or getattr(self.queue_runner, "_inspect_result", None)
            is not self.original_inspect_result
        ):
            raise OperationalReplayError(
                "queue replay functions changed before router installation"
            )
        setattr(self.queue_runner, "_scan_results", self.scan_wrapper)
        setattr(self.queue_runner, "_inspect_result", self.inspect_wrapper)
        self.assert_installed()

    def assert_installed(self) -> None:
        if (
            getattr(self.queue_runner, "_scan_results", None) is not self.scan_wrapper
            or getattr(self.queue_runner, "_inspect_result", None)
            is not self.inspect_wrapper
        ):
            raise OperationalReplayError(
                "queue operational replay router was replaced or tampered with"
            )

    def current_session(self) -> "ReplaySession | None":
        return getattr(self._local, "session", None)

    def enter(self, session: "ReplaySession") -> None:
        self.assert_installed()
        if self.current_session() is not None:
            raise OperationalReplayError("nested operational replay sessions are forbidden")
        self._local.session = session

    def leave(self, session: "ReplaySession") -> None:
        if self.current_session() is not session:
            raise OperationalReplayError("operational replay thread context changed")
        del self._local.session

    def call_original_scan(
        self, bundle: dict[str, Any]
    ) -> list[dict[str, Any] | None]:
        previous = getattr(self._local, "inside_original_scan", False)
        self._local.inside_original_scan = True
        try:
            return self.original_scan_results(bundle)
        finally:
            self._local.inside_original_scan = previous


_INSTALL_LOCK = threading.Lock()
_ROUTERS: "weakref.WeakKeyDictionary[ModuleType, QueueReplayRouter]" = (
    weakref.WeakKeyDictionary()
)


def install_queue_replay_router(queue_runner: ModuleType) -> QueueReplayRouter:
    """Install or return the sole router for ``queue_runner`` in this process."""

    if not isinstance(queue_runner, ModuleType):
        raise OperationalReplayError("queue runner must be an imported module")
    with _INSTALL_LOCK:
        existing = _ROUTERS.get(queue_runner)
        if existing is not None:
            existing.assert_installed()
            return existing
        for name in ("_scan_results", "_inspect_result"):
            installed = getattr(
                getattr(queue_runner, name, None),
                "__himr_operational_replay_router__",
                None,
            )
            if installed is not None:
                raise OperationalReplayError(
                    "queue runner has an unregistered operational replay wrapper"
                )
        router = QueueReplayRouter(queue_runner)
        router.install()
        _ROUTERS[queue_runner] = router
        return router


class ReplaySession:
    """One thread-confined deep or operational source invocation."""

    def __init__(
        self,
        store: "OperationalReplayStore",
        binding: QueueBinding,
        mode: str,
        snapshot: _QueueSnapshot | None,
    ):
        self.store = store
        self.router = store.router
        self.binding = binding
        self.mode = mode
        self.base_snapshot = copy.deepcopy(snapshot)
        self.working_snapshot = copy.deepcopy(snapshot)
        self.deep_passes: list[_QueueSnapshot] = []
        self.fast_reused_items = 0
        self.avoided_logical_payload_bytes = 0
        self.targeted_revalidated_ordinals: set[int] = set()
        self.new_exact_ordinals: set[int] = set()
        self.delta: dict[str, Any] | None = None
        self._entered = False
        self._active_deep_captures: list[
            tuple[
                tuple[str, str],
                dict[str, Any] | None,
                _CompletedWitness | None,
            ]
        ] | None = None

    def __enter__(self) -> "ReplaySession":
        self.store._assert_process()
        if self._entered:
            raise OperationalReplayError("replay session cannot be entered twice")
        self.router.enter(self)
        self._entered = True
        return self

    def __exit__(self, error_type, error, traceback) -> bool:
        leave_error: Exception | None = None
        try:
            self.router.leave(self)
        except Exception as observed:
            leave_error = observed
        if error_type is None and leave_error is None:
            self.delta = self.store._commit_session(self)
        if leave_error is not None:
            raise leave_error
        return False

    def _bundle_shape(
        self, bundle: dict[str, Any], *, deep: bool
    ) -> tuple[tuple[_OrderBinding, ...], list[dict[str, Any]]]:
        return self.store._bundle_shape(bundle, self.binding, deep=deep)

    def scan_results(
        self, bundle: dict[str, Any]
    ) -> list[dict[str, Any] | None]:
        self.store._assert_process()
        if self.mode == "deep":
            self._bundle_shape(bundle, deep=True)
            captures: list[
                tuple[
                    tuple[str, str],
                    dict[str, Any] | None,
                    _CompletedWitness | None,
                ]
            ] = []
            if self._active_deep_captures is not None:
                raise OperationalReplayError("nested deep exact capture is forbidden")
            self._active_deep_captures = captures
            try:
                states = self.router.call_original_scan(bundle)
            finally:
                self._active_deep_captures = None
            snapshot = self.store._snapshot_from_exact_scan(
                self.binding,
                bundle,
                states,
                exact_captures=captures,
                generation=0,
            )
            self.deep_passes.append(snapshot)
            return copy.deepcopy(states)
        if self.mode != "operational" or self.working_snapshot is None:
            raise OperationalReplayError("operational session has no admitted snapshot")
        orders, raw_orders = self._bundle_shape(bundle, deep=False)
        if orders != self.working_snapshot.orders:
            raise DeepAuditRequired("queue order binding changed since deep replay")
        returned: list[dict[str, Any] | None] = []
        for index, (order_binding, raw_order) in enumerate(
            zip(orders, raw_orders, strict=True)
        ):
            state = self.working_snapshot.states[index]
            witness = self.working_snapshot.witnesses[index]
            if state is None:
                if self.store._pending_unchanged(raw_order):
                    returned.append(None)
                    continue
                observed, exact_witness = (
                    self.store._coherent_exact_inspection(raw_order)
                )
                self._admit_exact(
                    index,
                    raw_order,
                    observed,
                    exact_witness=exact_witness,
                    targeted=True,
                )
                returned.append(copy.deepcopy(observed))
                continue
            current: _CompletedWitness | None = None
            try:
                current = self.store._completed_witness(raw_order, state)
                unchanged = (
                    witness is not None
                    and self.store._steady_witness_compatible(witness, current)
                )
            except Exception:
                unchanged = False
            if unchanged:
                # Shared CAS ancestors legitimately change as unrelated ordinals
                # arrive. Leaf authority and stable path identities are unchanged,
                # so refresh only the volatile directory metadata without hashing.
                self.working_snapshot.witnesses[index] = current
                self.fast_reused_items += 1
                self.avoided_logical_payload_bytes += 2 * state["byte_count"]
                returned.append(copy.deepcopy(state))
                continue
            observed, exact_witness = self.store._coherent_exact_inspection(
                raw_order,
                before_state=state if current is not None else None,
                before_witness=current,
            )
            self._admit_exact(
                index,
                raw_order,
                observed,
                exact_witness=exact_witness,
                targeted=True,
            )
            returned.append(copy.deepcopy(observed))
        return returned

    def _admit_exact(
        self,
        index: int,
        order: dict[str, Any],
        observed: dict[str, Any] | None,
        *,
        exact_witness: _CompletedWitness | None,
        targeted: bool,
    ) -> None:
        if self.working_snapshot is None:
            raise OperationalReplayError("cannot admit exact state without a snapshot")
        before = self.working_snapshot.states[index]
        ordinal = self.working_snapshot.orders[index].ordinal
        if before is not None and observed is None:
            raise OperationalReplayError(
                f"completed acquisition result {ordinal} disappeared"
            )
        if before is not None and observed != before:
            raise OperationalReplayError(
                f"completed acquisition result {ordinal} changed"
            )
        if observed is not None:
            if exact_witness is None:
                raise OperationalReplayError(
                    "exact completed state lacks a coherent metadata bracket"
                )
            self.store._validate_completed_state(order, observed)
        else:
            if exact_witness is not None:
                raise OperationalReplayError(
                    "exact pending state has a completed metadata witness"
                )
        if before is None and observed is not None:
            self.new_exact_ordinals.add(ordinal)
        if targeted:
            self.targeted_revalidated_ordinals.add(ordinal)
        self.working_snapshot.states[index] = copy.deepcopy(observed)
        self.working_snapshot.witnesses[index] = exact_witness
        self.working_snapshot.state_digest = _state_digest(
            self.working_snapshot.orders, self.working_snapshot.states
        )

    def _bound_order_index(self, order: dict[str, Any]) -> int:
        if self.working_snapshot is None:
            raise OperationalReplayError("active replay has no queue snapshot")
        identity = self.store._order_identity(order)
        matches = [
            index
            for index, binding in enumerate(self.working_snapshot.orders)
            if binding.work_order_sha256 == identity[0]
            and binding.result_path == identity[1]
        ]
        if len(matches) != 1:
            raise OperationalReplayError(
                "direct result inspection is outside the active queue binding"
            )
        return matches[0]

    def inspect_direct(
        self, order: dict[str, Any]
    ) -> dict[str, Any] | None:
        if self.mode != "operational" or self.working_snapshot is None:
            raise OperationalReplayError(
                "direct exact inspection requires an operational session"
            )
        index = self._bound_order_index(order)
        before = self.working_snapshot.states[index]
        before_witness: _CompletedWitness | None = None
        if before is not None:
            try:
                before_witness = self.store._completed_witness(order, before)
            except Exception:
                before_witness = None
        observed, exact_witness = self.store._coherent_exact_inspection(
            order,
            before_state=before if before_witness is not None else None,
            before_witness=before_witness,
        )
        self._admit_exact(
            index,
            order,
            observed,
            exact_witness=exact_witness,
            targeted=False,
        )
        return observed

    def observe_deep_scan_inspection(
        self, order: dict[str, Any]
    ) -> dict[str, Any] | None:
        if self.mode != "deep" or self._active_deep_captures is None:
            raise OperationalReplayError(
                "source scan escaped its active deep replay boundary"
            )
        identity = self.store._order_identity(order)
        observed, exact_witness = self.store._coherent_exact_inspection(order)
        self._active_deep_captures.append(
            (identity, copy.deepcopy(observed), exact_witness)
        )
        return observed


class OperationalReplayStore:
    """Thread-safe same-process snapshots beneath independently forked lanes."""

    def __init__(self, router: QueueReplayRouter):
        if not isinstance(router, QueueReplayRouter):
            raise OperationalReplayError("operational replay store requires a router")
        router.assert_installed()
        self.router = router
        self._pid = os.getpid()
        self._lock = threading.RLock()
        self._snapshots: dict[str, _QueueSnapshot] = {}
        self._accepted_delta_digests: dict[str, None] = {}
        self._restart_checkpoint_authority = object()

    @classmethod
    def for_queue_runner(cls, queue_runner: ModuleType) -> "OperationalReplayStore":
        return cls(install_queue_replay_router(queue_runner))

    def _assert_process(self) -> None:
        if os.getpid() != self._pid:
            raise DeepAuditRequired(
                "same-process replay witnesses cannot survive a process fork"
            )
        self.router.assert_installed()

    def deep_capture(self, binding: QueueBinding) -> ReplaySession:
        self._assert_process()
        with self._lock:
            snapshot = self._snapshots.get(binding.schedule_id)
            if snapshot is not None and snapshot.binding != binding:
                raise DeepAuditRequired(
                    f"schedule {binding.schedule_id} binding changed since deep replay"
                )
            return ReplaySession(self, binding, "deep", snapshot)

    def operational_session(self, binding: QueueBinding) -> ReplaySession:
        self._assert_process()
        with self._lock:
            snapshot = self._snapshots.get(binding.schedule_id)
            if snapshot is None:
                raise DeepAuditRequired(
                    f"schedule {binding.schedule_id} has no same-process deep snapshot"
                )
            if snapshot.binding != binding:
                raise DeepAuditRequired(
                    f"schedule {binding.schedule_id} binding changed since deep replay"
                )
            return ReplaySession(self, binding, "operational", snapshot)

    def has_snapshot(self, binding: QueueBinding) -> bool:
        self._assert_process()
        with self._lock:
            snapshot = self._snapshots.get(binding.schedule_id)
            return snapshot is not None and snapshot.binding == binding

    def _bundle_shape(
        self,
        bundle: dict[str, Any],
        binding: QueueBinding,
        *,
        deep: bool,
    ) -> tuple[tuple[_OrderBinding, ...], list[dict[str, Any]]]:
        error_type = OperationalReplayError if deep else DeepAuditRequired
        try:
            path = _lexical_absolute(bundle["path"], "replayed queue manifest")
            body = bundle["body"]
            manifest = bundle["manifest"]
            entries = manifest["work_orders"]
            orders = bundle["orders"]
            if not isinstance(body, bytes):
                raise TypeError("body")
            if (
                path != binding.manifest_path
                or _sha256(body) != binding.manifest_sha256
                or manifest["bundle_id"] != binding.bundle_id
                or not isinstance(entries, list)
                or not isinstance(orders, list)
                or len(entries) != len(orders)
                or len(orders) > MAX_ORDERS
            ):
                raise ValueError("binding")
            shaped = []
            for index, (entry, order) in enumerate(
                zip(entries, orders, strict=True), 1
            ):
                ordinal = entry["queue_ordinal"]
                if ordinal != index or entry["job_id"] != order["job_id"]:
                    raise ValueError("ordinal")
                identity, result_path = self._order_identity(order)
                shaped.append(
                    _OrderBinding(
                        ordinal=ordinal,
                        job_id=entry["job_id"],
                        work_order_sha256=identity,
                        work_order_file_sha256=_digest(
                            entry["sha256"], "work-order file SHA-256"
                        ),
                        result_path=result_path,
                    )
                )
            return tuple(shaped), orders
        except Exception as error:
            raise error_type(
                "queue bundle differs from its operational replay binding"
            ) from error

    def _order_identity(self, order: dict[str, Any]) -> tuple[str, str]:
        try:
            digest = self.router.queue_runner.sha256_bytes(
                self.router.queue_runner.canonical_bytes(order)
            )
            result_path = _lexical_absolute(
                self.router.queue_runner._result_path(order), "acquisition result path"
            )
        except Exception as error:
            raise OperationalReplayError(
                "cannot derive exact work-order result identity"
            ) from error
        return _digest(digest, "work-order identity SHA-256"), str(result_path)

    def _validate_completed_state(
        self, order: dict[str, Any], state: dict[str, Any]
    ) -> None:
        if not isinstance(state, dict) or set(state) != {
            "result",
            "result_sha256",
            "media_sha256",
            "byte_count",
        }:
            raise OperationalReplayError("exact completed state is malformed")
        result = state["result"]
        if not isinstance(result, dict) or not isinstance(result.get("admission"), dict):
            raise OperationalReplayError("exact completed result is malformed")
        identity, result_path = self._order_identity(order)
        admission = result["admission"]
        if (
            result.get("work_order_sha256") != identity
            or result.get("result_path") != result_path
            or _digest(state["result_sha256"], "result SHA-256")
            != self.router.queue_runner.sha256_bytes(
                self.router.queue_runner.acquire.pretty_json(result).encode("utf-8")
            )
            or _digest(state["media_sha256"], "media SHA-256")
            != admission.get("sha256")
            or _integer(state["byte_count"], "media byte count", 1)
            != admission.get("byte_count")
        ):
            raise OperationalReplayError(
                "exact completed state differs from its result envelope"
            )

    def _completed_witness(
        self, order: dict[str, Any], state: dict[str, Any]
    ) -> _CompletedWitness:
        self._validate_completed_state(order, state)
        output_root = _lexical_absolute(order["output"]["root"], "output root")
        result_path = _lexical_absolute(
            self.router.queue_runner._result_path(order), "acquisition result path"
        )
        self.router.queue_runner._safe_existing_result_parents(
            result_path, output_root
        )
        result_ancestors = _ancestor_identities(
            output_root, result_path, label="acquisition result"
        )
        names, result_parent = _directory_names_and_fingerprint(
            result_path.parent, label="completed result directory"
        )
        if names != {"result.json"}:
            raise OperationalReplayError(
                "completed result directory has missing or extra entries"
            )
        result_file = _open_stable_regular(
            result_path,
            label="completed acquisition result",
            require_owner=True,
            reject_peer_writable=True,
        )
        payload_path = _lexical_absolute(
            state["result"]["admission"]["path"], "content-addressed payload"
        )
        payload_ancestors = _ancestor_identities(
            output_root, payload_path, label="content-addressed payload"
        )
        payload_file = _open_stable_regular(
            payload_path,
            label="content-addressed payload",
            require_owner=False,
            reject_peer_writable=False,
        )
        if payload_file.size != state["byte_count"]:
            raise OperationalReplayError(
                "content-addressed payload size differs from completed state"
            )
        return _CompletedWitness(
            result_file=result_file,
            result_parent=result_parent,
            result_ancestors=result_ancestors,
            payload_file=payload_file,
            payload_ancestors=payload_ancestors,
        )

    @staticmethod
    def _steady_file_identity(value: _FileFingerprint) -> tuple[int, int, int, int]:
        return value.device, value.inode, value.mode, value.uid

    @classmethod
    def _steady_directory_identity(
        cls, value: _DirectoryIdentity
    ) -> tuple[str, int, int, int, int]:
        return (value.path, *cls._steady_file_identity(value.fingerprint))

    @classmethod
    def _steady_witness_compatible(
        cls, trusted: _CompletedWitness, current: _CompletedWitness
    ) -> bool:
        """Allow only volatile ancestor metadata to refresh without payload I/O."""

        return (
            trusted.result_file == current.result_file
            and trusted.payload_file == current.payload_file
            and cls._steady_file_identity(trusted.result_parent)
            == cls._steady_file_identity(current.result_parent)
            and tuple(
                cls._steady_directory_identity(value)
                for value in trusted.result_ancestors
            )
            == tuple(
                cls._steady_directory_identity(value)
                for value in current.result_ancestors
            )
            and tuple(
                cls._steady_directory_identity(value)
                for value in trusted.payload_ancestors
            )
            == tuple(
                cls._steady_directory_identity(value)
                for value in current.payload_ancestors
            )
        )

    @staticmethod
    def _restart_leaf_identity(
        value: _FileFingerprint,
    ) -> tuple[int, int, int, int, int, int, int]:
        """Stable leaf identity after mount UUID validation by the backend.

        Linux ``st_dev`` is a runtime major/minor number and can legitimately
        change across reboot or remount.  The sealed backend separately binds the
        retained XFS filesystem UUID, so restart comparison excludes only device
        while retaining inode, size, both change clocks, mode, link count, and UID.
        """

        return (
            value.inode,
            value.size,
            value.mtime_ns,
            value.ctime_ns,
            value.mode,
            value.link_count,
            value.uid,
        )

    @staticmethod
    def _restart_directory_identity(
        value: _DirectoryIdentity,
    ) -> tuple[str, int, int, int]:
        return (
            value.path,
            value.fingerprint.inode,
            value.fingerprint.mode,
            value.fingerprint.uid,
        )

    @classmethod
    def _restart_witness_compatible(
        cls, trusted: _CompletedWitness, current: _CompletedWitness
    ) -> bool:
        """Compare persisted witnesses after external filesystem-UUID binding."""

        return (
            cls._restart_leaf_identity(trusted.result_file)
            == cls._restart_leaf_identity(current.result_file)
            and cls._restart_leaf_identity(trusted.payload_file)
            == cls._restart_leaf_identity(current.payload_file)
            and (
                trusted.result_parent.inode,
                trusted.result_parent.mode,
                trusted.result_parent.uid,
            )
            == (
                current.result_parent.inode,
                current.result_parent.mode,
                current.result_parent.uid,
            )
            and tuple(
                cls._restart_directory_identity(value)
                for value in trusted.result_ancestors
            )
            == tuple(
                cls._restart_directory_identity(value)
                for value in current.result_ancestors
            )
            and tuple(
                cls._restart_directory_identity(value)
                for value in trusted.payload_ancestors
            )
            == tuple(
                cls._restart_directory_identity(value)
                for value in current.payload_ancestors
            )
        )

    def _provisional_completed_state(
        self, order: dict[str, Any]
    ) -> tuple[dict[str, Any], _CompletedWitness]:
        """Capture a cheap pre-hash witness from the bounded result envelope."""

        result_path = _lexical_absolute(
            self.router.queue_runner._result_path(order),
            "provisional acquisition result",
        )
        body, body_fingerprint = _read_stable_regular(
            result_path,
            label="provisional acquisition result",
            maximum=MAX_RESULT_ENVELOPE_BYTES,
        )

        def unique_object(values: list[tuple[str, Any]]) -> dict[str, Any]:
            returned: dict[str, Any] = {}
            for key, value in values:
                if key in returned:
                    raise OperationalReplayError(
                        "provisional acquisition result repeats a JSON key"
                    )
                returned[key] = value
            return returned

        try:
            result = json.loads(
                body.decode("utf-8"),
                object_pairs_hook=unique_object,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(value)
                ),
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise OperationalReplayError(
                "provisional acquisition result is not strict JSON"
            ) from error
        if (
            not isinstance(result, dict)
            or body
            != self.router.queue_runner.acquire.pretty_json(result).encode("utf-8")
            or not isinstance(result.get("admission"), dict)
        ):
            raise OperationalReplayError(
                "provisional acquisition result is not a canonical envelope"
            )
        admission = result["admission"]
        state = {
            "result": result,
            "result_sha256": self.router.queue_runner.sha256_bytes(body),
            "media_sha256": admission.get("sha256"),
            "byte_count": admission.get("byte_count"),
        }
        self._validate_completed_state(order, state)
        witness = self._completed_witness(order, state)
        if witness.result_file != body_fingerprint:
            raise OperationalReplayError(
                "provisional result changed before its metadata witness"
            )
        return state, witness

    @staticmethod
    def _trusted_copy_witness_compatible(
        trusted: _CompletedWitness, current: _CompletedWitness
    ) -> bool:
        """Preserve content-envelope/permission topology across an approved copy.

        This is not content verification. Only explicit stopped migration uses it;
        ordinary recovery retains its inode/time checks and exact rehash fallback.
        """

        def leaf(value: _FileFingerprint) -> tuple[int, int, int, int]:
            return value.size, value.mode, value.link_count, value.uid

        def ancestors(values: tuple[_DirectoryIdentity, ...]) -> tuple[Any, ...]:
            return tuple(
                (value.path, value.fingerprint.mode, value.fingerprint.uid)
                for value in values
            )

        return (
            leaf(trusted.result_file) == leaf(current.result_file)
            and leaf(trusted.payload_file) == leaf(current.payload_file)
            and current.payload_file.uid == os.getuid()
            and current.payload_file.link_count == 1
            and not stat.S_IMODE(current.payload_file.mode) & 0o022
            and ancestors(trusted.result_ancestors) == ancestors(current.result_ancestors)
            and ancestors(trusted.payload_ancestors) == ancestors(current.payload_ancestors)
        )

    def _pre_exact_observation(
        self, order: dict[str, Any]
    ) -> tuple[
        dict[str, Any] | None,
        _CompletedWitness | None,
        bool,
    ]:
        if self._pending_unchanged(order):
            return None, None, True
        try:
            state, witness = self._provisional_completed_state(order)
            return state, witness, False
        except Exception:
            # The unchanged source inspector owns the precise malformed/racing
            # result error. If it succeeds, a second stable exact pass below is
            # required before that state can become replay authority.
            return None, None, False

    def _post_exact_observation(
        self,
        order: dict[str, Any],
        observed: dict[str, Any] | None,
    ) -> tuple[_CompletedWitness | None, bool]:
        if observed is None:
            if not self._pending_unchanged(order):
                raise OperationalReplayError(
                    "exact pending result changed before its metadata bracket closed"
                )
            return None, True
        self._validate_completed_state(order, observed)
        return self._completed_witness(order, observed), False

    def _coherent_exact_inspection(
        self,
        order: dict[str, Any],
        *,
        before_state: dict[str, Any] | None = None,
        before_witness: _CompletedWitness | None = None,
    ) -> tuple[dict[str, Any] | None, _CompletedWitness | None]:
        """Bracket exact payload hashing so later stat reuse cannot bless a race."""

        for attempt in range(ACQUISITION_RESULT_ADMISSION_ATTEMPTS):
            try:
                return self._coherent_exact_inspection_once(
                    order,
                    before_state=before_state,
                    before_witness=before_witness,
                )
            except Exception as error:
                queue_error = getattr(
                    self.router.queue_runner, "QueueRunnerError", None
                )
                job_id = order.get("job_id")
                source_publication_race = (
                    isinstance(queue_error, type)
                    and isinstance(error, queue_error)
                    and isinstance(job_id, str)
                    and str(error)
                    == f"result directory exists without result.json for {job_id}"
                )
                bracket_publication_race = (
                    type(error) is OperationalReplayError
                    and str(error) == _PENDING_RESULT_ADMITTED_DURING_BRACKET
                )
                if (
                    not (source_publication_race or bracket_publication_race)
                    or attempt + 1 == ACQUISITION_RESULT_ADMISSION_ATTEMPTS
                ):
                    raise
                time.sleep(ACQUISITION_RESULT_ADMISSION_RETRY_SECONDS)
        raise OperationalReplayError("unreachable result-admission retry state")

    def _coherent_exact_inspection_once(
        self,
        order: dict[str, Any],
        *,
        before_state: dict[str, Any] | None = None,
        before_witness: _CompletedWitness | None = None,
    ) -> tuple[dict[str, Any] | None, _CompletedWitness | None]:
        """Perform one complete, non-committing exact-observation bracket."""

        if (before_state is None) != (before_witness is None):
            raise OperationalReplayError(
                "exact inspection received a partial completed pre-witness"
            )
        if before_witness is None:
            pre_state, pre_witness, pre_pending = self._pre_exact_observation(order)
        else:
            self._validate_completed_state(order, before_state)
            pre_state = before_state
            pre_witness = before_witness
            pre_pending = False

        first = self.router.original_inspect_result(order)
        first_witness, first_pending = self._post_exact_observation(order, first)
        if first is None:
            coherent = pre_pending and first_pending
        else:
            coherent = pre_state == first and pre_witness == first_witness
        if coherent:
            return first, first_witness

        # The result may have been atomically admitted after the cheap pre-read.
        # One additional exact pass is permitted only with the first pass's closed
        # post-witness serving as the second pass's pre-witness.
        second = self.router.original_inspect_result(order)
        second_witness, second_pending = self._post_exact_observation(order, second)
        if first is None:
            stable = first_pending and second is None and second_pending
        else:
            stable = (
                second == first
                and second_witness == first_witness
                and not second_pending
            )
        if not stable:
            raise OperationalReplayError(
                "exact acquisition state changed across metadata-bracketed replay"
            )
        return second, second_witness

    def _pending_unchanged(self, order: dict[str, Any]) -> bool:
        output_root = _lexical_absolute(order["output"]["root"], "output root")
        result_path = _lexical_absolute(
            self.router.queue_runner._result_path(order), "acquisition result path"
        )
        try:
            self.router.queue_runner._safe_existing_result_parents(
                result_path, output_root
            )
            result_path.lstat()
            return False
        except FileNotFoundError:
            if result_path.parent.exists() or result_path.parent.is_symlink():
                return False
            return True
        except Exception:
            # The unchanged original inspector owns the exact error and transient
            # admission-race semantics for every non-clean pending shape.
            return False

    def _snapshot_from_exact_scan(
        self,
        binding: QueueBinding,
        bundle: dict[str, Any],
        states: Sequence[dict[str, Any] | None],
        *,
        exact_captures: Sequence[
            tuple[
                tuple[str, str],
                dict[str, Any] | None,
                _CompletedWitness | None,
            ]
        ],
        generation: int,
    ) -> _QueueSnapshot:
        orders, raw_orders = self._bundle_shape(bundle, binding, deep=True)
        if (
            not isinstance(states, (list, tuple))
            or not isinstance(exact_captures, (list, tuple))
            or len(states) != len(orders)
            or len(exact_captures) != len(orders)
        ):
            raise OperationalReplayError(
                "deep result vector differs from queue cardinality"
            )
        copied: list[dict[str, Any] | None] = []
        witnesses: list[_CompletedWitness | None] = []
        for order, state, capture in zip(
            raw_orders, states, exact_captures, strict=True
        ):
            captured_identity, captured_state, captured_witness = capture
            if (
                captured_identity != self._order_identity(order)
                or captured_state != state
            ):
                raise OperationalReplayError(
                    "deep exact capture differs from its returned result vector"
                )
            if state is None:
                if not self._pending_unchanged(order):
                    raise OperationalReplayError(
                        "deep scan returned pending for a non-clean result path"
                    )
                if captured_witness is not None:
                    raise OperationalReplayError(
                        "deep pending result has a completed metadata witness"
                    )
                copied.append(None)
                witnesses.append(None)
                continue
            self._validate_completed_state(order, state)
            if captured_witness is None:
                raise OperationalReplayError(
                    "deep completed result lacks a coherent metadata bracket"
                )
            copied.append(copy.deepcopy(state))
            witnesses.append(captured_witness)
        return _QueueSnapshot(
            binding=binding,
            generation=generation,
            orders=orders,
            states=copied,
            witnesses=witnesses,
            state_digest=_state_digest(orders, copied),
        )

    @staticmethod
    def _same_completed(
        left: dict[str, Any], right: dict[str, Any]
    ) -> bool:
        return left == right

    def _merge_snapshots(
        self,
        current: _QueueSnapshot,
        candidate: _QueueSnapshot,
        *,
        prefer_candidate_witness: bool = False,
    ) -> _QueueSnapshot:
        if current.binding != candidate.binding or current.orders != candidate.orders:
            raise DeepAuditRequired("concurrent replay snapshot binding changed")
        states: list[dict[str, Any] | None] = []
        witnesses: list[_CompletedWitness | None] = []
        for ordinal, (current_state, candidate_state) in enumerate(
            zip(current.states, candidate.states, strict=True), 1
        ):
            if current_state is None and candidate_state is None:
                states.append(None)
                witnesses.append(None)
            elif current_state is None:
                states.append(copy.deepcopy(candidate_state))
                witnesses.append(candidate.witnesses[ordinal - 1])
            elif candidate_state is None:
                # A peer may have completed this ordinal after the candidate's
                # final coherent scan.  Never regress the shared store.
                states.append(copy.deepcopy(current_state))
                witnesses.append(current.witnesses[ordinal - 1])
            elif self._same_completed(current_state, candidate_state):
                states.append(copy.deepcopy(current_state))
                witnesses.append(
                    candidate.witnesses[ordinal - 1]
                    if prefer_candidate_witness
                    else current.witnesses[ordinal - 1]
                )
            else:
                raise OperationalReplayError(
                    f"concurrent completed result {ordinal} conflicts"
                )
        return _QueueSnapshot(
            binding=current.binding,
            generation=current.generation,
            orders=current.orders,
            states=states,
            witnesses=witnesses,
            state_digest=_state_digest(current.orders, states),
        )

    @staticmethod
    def _snapshot_content_equal(
        left: _QueueSnapshot, right: _QueueSnapshot
    ) -> bool:
        return (
            left.binding == right.binding
            and left.orders == right.orders
            and left.states == right.states
            and left.witnesses == right.witnesses
            and left.state_digest == right.state_digest
        )

    def _remember_delta(self, digest: str) -> None:
        self._accepted_delta_digests[digest] = None
        while len(self._accepted_delta_digests) > MAX_REMEMBERED_DELTAS:
            del self._accepted_delta_digests[next(iter(self._accepted_delta_digests))]

    def _commit_session(self, session: ReplaySession) -> dict[str, Any]:
        self._assert_process()
        if session.mode == "deep":
            if len(session.deep_passes) < 2:
                raise OperationalReplayError(
                    "deep capture requires at least two exact scan passes"
                )
            first = session.deep_passes[0]
            if any(
                row.binding != first.binding
                or row.orders != first.orders
                or row.states != first.states
                or row.witnesses != first.witnesses
                for row in session.deep_passes[1:]
            ):
                raise OperationalReplayError(
                    "deep capture result state or witness changed between exact passes"
                )
            candidate = session.deep_passes[-1]
        elif session.mode == "operational" and session.working_snapshot is not None:
            candidate = session.working_snapshot
        else:
            raise OperationalReplayError("cannot commit an invalid replay session")
        with self._lock:
            current = self._snapshots.get(session.binding.schedule_id)
            base_generation = (
                0
                if session.base_snapshot is None
                else session.base_snapshot.generation
            )
            if current is None:
                committed = copy.deepcopy(candidate)
                committed.generation = 1
            elif current.generation == base_generation:
                if current.binding != candidate.binding or current.orders != candidate.orders:
                    raise DeepAuditRequired(
                        "operational replay binding changed before commit"
                    )
                # No peer advanced the store. Preserve targeted/deep witness
                # refreshes instead of retaining the stale base metadata.
                if self._snapshot_content_equal(current, candidate):
                    committed = copy.deepcopy(current)
                else:
                    committed = copy.deepcopy(candidate)
                    committed.generation = current.generation + 1
            else:
                committed = self._merge_snapshots(
                    current,
                    candidate,
                    prefer_candidate_witness=session.mode == "deep",
                )
                if not self._snapshot_content_equal(current, committed):
                    committed.generation = current.generation + 1
            self._snapshots[session.binding.schedule_id] = committed
            delta = self._delta_document(
                session,
                base_generation=base_generation,
                returned=candidate,
                committed=committed,
            )
            self._remember_delta(delta["delta_sha256"])
            return delta

    def _delta_document(
        self,
        session: ReplaySession,
        *,
        base_generation: int,
        returned: _QueueSnapshot,
        committed: _QueueSnapshot,
    ) -> dict[str, Any]:
        returned_completed = sum(state is not None for state in returned.states)
        completed = sum(state is not None for state in committed.states)
        core = {
            "adapter": ADAPTER_KIND,
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "mode": session.mode,
            "schedule_id": committed.binding.schedule_id,
            "bundle_id": committed.binding.bundle_id,
            "bundle_manifest_sha256": committed.binding.manifest_sha256,
            "base_generation": base_generation,
            "generation": committed.generation,
            # A concurrent session can advance the shared monotonic snapshot
            # after this session has produced its source return value.  Preserve
            # both projections: queue summaries and runtime objects are bound to
            # ``session_*`` while ``generation``/``state_digest`` describe the
            # merged store authority after commit.
            "session_state_digest": returned.state_digest,
            "session_completed_count": returned_completed,
            "session_pending_count": len(returned.states) - returned_completed,
            "state_digest": committed.state_digest,
            "completed_count": completed,
            "pending_count": len(committed.states) - completed,
            "new_exact_ordinals": sorted(session.new_exact_ordinals),
            "targeted_revalidated_ordinals": sorted(
                session.targeted_revalidated_ordinals
            ),
            "fast_reused_items": session.fast_reused_items,
            "avoided_logical_payload_bytes": session.avoided_logical_payload_bytes,
        }
        return {**core, "delta_sha256": _sha256(canonical_bytes(core))}

    def verify_peer_delta(self, value: Any) -> dict[str, Any]:
        self._assert_process()
        keys = {
            "adapter",
            "schema_version",
            "implementation_version",
            "mode",
            "schedule_id",
            "bundle_id",
            "bundle_manifest_sha256",
            "base_generation",
            "generation",
            "session_state_digest",
            "session_completed_count",
            "session_pending_count",
            "state_digest",
            "completed_count",
            "pending_count",
            "new_exact_ordinals",
            "targeted_revalidated_ordinals",
            "fast_reused_items",
            "avoided_logical_payload_bytes",
            "delta_sha256",
        }
        if not isinstance(value, dict) or set(value) != keys:
            raise OperationalReplayError("peer replay delta has unexpected fields")
        core = {key: value[key] for key in keys - {"delta_sha256"}}
        digest = _digest(value["delta_sha256"], "peer replay delta SHA-256")
        if digest != _sha256(canonical_bytes(core)):
            raise OperationalReplayError("peer replay delta digest is invalid")
        for label in ("schedule_id", "bundle_id", "adapter", "mode"):
            _text(value[label], f"peer replay {label}", 128)
        for label in (
            "bundle_manifest_sha256",
            "session_state_digest",
            "state_digest",
        ):
            _digest(value[label], f"peer replay {label}")
        for label in (
            "schema_version",
            "base_generation",
            "generation",
            "session_completed_count",
            "session_pending_count",
            "completed_count",
            "pending_count",
            "fast_reused_items",
            "avoided_logical_payload_bytes",
        ):
            _integer(value[label], f"peer replay {label}")
        if (
            value["schema_version"] != SCHEMA_VERSION
            or value["implementation_version"] != IMPLEMENTATION_VERSION
            or value["mode"] not in {"deep", "operational"}
            or value["generation"] < 1
            or value["base_generation"] > value["generation"]
            or value["session_completed_count"]
            + value["session_pending_count"]
            > MAX_ORDERS
            or value["session_completed_count"]
            + value["session_pending_count"]
            != value["completed_count"] + value["pending_count"]
        ):
            raise OperationalReplayError("peer replay delta header is invalid")
        session_total = (
            value["session_completed_count"] + value["session_pending_count"]
        )
        for label in ("new_exact_ordinals", "targeted_revalidated_ordinals"):
            ordinals = value[label]
            if (
                not isinstance(ordinals, list)
                or any(
                    isinstance(ordinal, bool)
                    or not isinstance(ordinal, int)
                    or not 1 <= ordinal <= session_total
                    for ordinal in ordinals
                )
                or ordinals != sorted(set(ordinals))
            ):
                raise OperationalReplayError(
                    f"peer replay {label} is not a sorted unique ordinal list"
                )
        if (
            len(value["new_exact_ordinals"])
            > value["session_completed_count"]
            or (
                value["mode"] == "deep"
                and (
                    value["new_exact_ordinals"]
                    or value["targeted_revalidated_ordinals"]
                    or value["fast_reused_items"] != 0
                    or value["avoided_logical_payload_bytes"] != 0
                )
            )
            or (value["mode"] == "operational" and value["base_generation"] < 1)
            or (
                value["fast_reused_items"] == 0
                and value["avoided_logical_payload_bytes"] != 0
            )
        ):
            raise OperationalReplayError("peer replay session telemetry is invalid")
        with self._lock:
            if digest in self._accepted_delta_digests:
                return copy.deepcopy(value)
            snapshot = self._snapshots.get(value["schedule_id"])
            if snapshot is None:
                raise DeepAuditRequired(
                    "peer delta cannot hydrate an empty same-process replay store"
                )
            completed = sum(state is not None for state in snapshot.states)
            if (
                value["adapter"] != ADAPTER_KIND
                or value["schema_version"] != SCHEMA_VERSION
                or value["implementation_version"] != IMPLEMENTATION_VERSION
                or value["bundle_id"] != snapshot.binding.bundle_id
                or value["bundle_manifest_sha256"] != snapshot.binding.manifest_sha256
                or value["generation"] != snapshot.generation
                or value["state_digest"] != snapshot.state_digest
                or value["completed_count"] != completed
                or value["completed_count"] + value["pending_count"]
                != len(snapshot.states)
                # The unkeyed canonical digest proves only integrity, not that
                # this process emitted the claimed session projection. Once a
                # delta falls out of the bounded same-process remembered set,
                # snapshot fallback is therefore safe only for a session return
                # exactly equal to current committed authority. Returned-stale
                # projections remain admissible solely while their original
                # digest is remembered.
                or value["session_state_digest"] != snapshot.state_digest
                or value["session_completed_count"] != completed
                or value["session_pending_count"] != len(snapshot.states) - completed
                or any(
                    snapshot.states[ordinal - 1] is None
                    for ordinal in value["new_exact_ordinals"]
                )
            ):
                raise OperationalReplayError(
                    "peer replay delta conflicts with the shared snapshot"
                )
            self._remember_delta(digest)
            return copy.deepcopy(value)

    def export_checkpoint(
        self,
        *,
        config_id: str,
        config_sha256: str,
        campaign_id: str,
        schedule_set_id: str,
        created_at: str,
        previous_checkpoint_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Export digest/lineage state; never serialize live stat witnesses."""

        self._assert_process()
        _text(config_id, "config ID", 128)
        _digest(config_sha256, "config SHA-256")
        _text(campaign_id, "campaign ID", 128)
        _text(schedule_set_id, "schedule-set ID", 128)
        _text(created_at, "checkpoint creation time", 64)
        try:
            if datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ) != created_at:
                raise ValueError(created_at)
        except ValueError as error:
            raise OperationalReplayError(
                "checkpoint creation time must be canonical UTC"
            ) from error
        if previous_checkpoint_sha256 is not None:
            _digest(previous_checkpoint_sha256, "previous checkpoint SHA-256")
        with self._lock:
            snapshots = [
                copy.deepcopy(self._snapshots[key])
                for key in sorted(self._snapshots)
            ]
        if len(snapshots) > MAX_SNAPSHOTS:
            raise OperationalReplayError("operational replay checkpoint is too large")
        schedules = []
        completed_total = 0
        media_bytes = 0
        work_orders = 0
        for snapshot in snapshots:
            rows = [
                _state_row(order, state)
                for order, state in zip(
                    snapshot.orders, snapshot.states, strict=True
                )
            ]
            completed_total += sum(row["status"] == "completed" for row in rows)
            media_bytes += sum(
                row["media_byte_count"] or 0 for row in rows
            )
            work_orders += len(rows)
            if work_orders > MAX_CHECKPOINT_ORDERS:
                raise OperationalReplayError(
                    "operational replay checkpoint has too many work orders"
                )
            schedules.append(
                {
                    "schedule_id": snapshot.binding.schedule_id,
                    "role": snapshot.binding.role,
                    "schedule_sha256": snapshot.binding.schedule_sha256,
                    "bundle_id": snapshot.binding.bundle_id,
                    "manifest_path": str(snapshot.binding.manifest_path),
                    "manifest_sha256": snapshot.binding.manifest_sha256,
                    "work_order_count": len(rows),
                    "generation": snapshot.generation,
                    "state_digest": snapshot.state_digest,
                    "states": rows,
                }
            )
        core = {
            "kind": CHECKPOINT_KIND,
            "schema_version": SCHEMA_VERSION,
            "adapter": {
                "name": ADAPTER_NAME,
                "version": IMPLEMENTATION_VERSION,
            },
            "config_id": config_id,
            "config_sha256": config_sha256,
            "campaign_id": campaign_id,
            "schedule_set_id": schedule_set_id,
            "created_at": created_at,
            "previous_checkpoint_sha256": previous_checkpoint_sha256,
            "schedules": schedules,
            "totals": {
                "schedule_count": len(schedules),
                "work_order_count": work_orders,
                "completed_count": completed_total,
                "pending_count": work_orders - completed_total,
                "completed_media_byte_count": media_bytes,
            },
            "policy": {
                "rebuildable": True,
                "same_process_witness_authority_only": True,
                "cross_process_witness_reuse": False,
                "filesystem_witnesses_persisted": False,
                "publication_authority": "none",
                "catalogue_mutation_authority": "none",
                "deletion_authority": "none",
            },
        }
        document = {**core, "identity_sha256": _sha256(canonical_bytes(core))}
        if len(canonical_bytes(document)) > MAX_CHECKPOINT_BYTES:
            raise OperationalReplayError(
                "operational replay checkpoint exceeds its byte cap"
            )
        return document

    def export_restart_checkpoint(
        self,
        *,
        config_id: str,
        config_sha256: str,
        campaign_id: str,
        schedule_set_id: str,
        created_at: str,
        previous_checkpoint_sha256: str | None = None,
        expected_snapshots: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Export v2 restart authority with full states and stat witnesses.

        This API is intentionally separate from :meth:`export_checkpoint`; the
        latter's v1 digest-only schema and no-cross-process-witness policy remain
        unchanged for existing consumers.
        """

        self._assert_process()
        _text(config_id, "restart checkpoint config ID", 128)
        _digest(config_sha256, "restart checkpoint config SHA-256")
        _text(campaign_id, "restart checkpoint campaign ID", 128)
        _text(schedule_set_id, "restart checkpoint schedule-set ID", 128)
        _canonical_utc(created_at, "restart checkpoint creation time")
        if previous_checkpoint_sha256 is not None:
            _digest(
                previous_checkpoint_sha256,
                "previous restart checkpoint SHA-256",
            )
        if expected_snapshots is not None:
            if not isinstance(expected_snapshots, dict):
                raise OperationalReplayError(
                    "restart checkpoint expected snapshots are malformed"
                )
            normalized_expected: dict[str, tuple[int, str]] = {}
            for schedule_id, row in expected_snapshots.items():
                if (
                    not isinstance(schedule_id, str)
                    or not schedule_id
                    or not isinstance(row, dict)
                    or set(row) != {"generation", "state_digest"}
                ):
                    raise OperationalReplayError(
                        "restart checkpoint expected snapshot row is malformed"
                    )
                normalized_expected[schedule_id] = (
                    _integer(
                        row["generation"],
                        "restart checkpoint expected generation",
                        1,
                    ),
                    _digest(
                        row["state_digest"],
                        "restart checkpoint expected state digest",
                    ),
                )
        else:
            normalized_expected = None
        with self._lock:
            if normalized_expected is not None:
                observed = {
                    schedule_id: (snapshot.generation, snapshot.state_digest)
                    for schedule_id, snapshot in self._snapshots.items()
                }
                # A replay generation also advances when exact validation
                # refreshes only filesystem witnesses.  That metadata-only
                # transition leaves ``state_digest`` unchanged and is safe to
                # persist at the current journal anchor: it introduces no queue
                # completion, failure, quarantine, or pending-state authority.
                # Requiring generation equality here made an unrelated ancestor
                # metadata change permanently defer every later checkpoint even
                # after all stage mutators had drained.
                #
                # Never make the inverse concession.  The shared store must
                # cover every generation already observed by the root, and any
                # digest difference still means logical queue authority is not
                # aligned with the journal/root boundary.
                witness_only_advance = (
                    set(observed) == set(normalized_expected)
                    and all(
                        observed[schedule_id][0] >= expected_generation
                        and observed[schedule_id][1] == expected_digest
                        for schedule_id, (
                            expected_generation,
                            expected_digest,
                        ) in normalized_expected.items()
                    )
                )
                if observed != normalized_expected and not witness_only_advance:
                    raise CheckpointDeferred(
                        "shared queue authority is ahead of the journal-observed root"
                    )
            snapshots = [
                copy.deepcopy(self._snapshots[key])
                for key in sorted(self._snapshots)
            ]
        if len(snapshots) > MAX_SNAPSHOTS:
            raise OperationalReplayError("restart checkpoint has too many schedules")

        schedules: list[dict[str, Any]] = []
        completed_total = 0
        media_bytes = 0
        work_orders = 0
        for snapshot in snapshots:
            rows: list[dict[str, Any]] = []
            for order, state, witness in zip(
                snapshot.orders,
                snapshot.states,
                snapshot.witnesses,
                strict=True,
            ):
                if (state is None) != (witness is None):
                    raise OperationalReplayError(
                        "restart checkpoint snapshot has partial state/witness authority"
                    )
                if state is not None:
                    completed_total += 1
                    media_bytes += state["byte_count"]
                rows.append(
                    {
                        "ordinal": order.ordinal,
                        "job_id": order.job_id,
                        "work_order_identity_sha256": order.work_order_sha256,
                        "work_order_file_sha256": order.work_order_file_sha256,
                        "result_path": order.result_path,
                        "state": copy.deepcopy(state),
                        "witness": (
                            None if witness is None else _witness_document(witness)
                        ),
                    }
                )
            work_orders += len(rows)
            if work_orders > MAX_CHECKPOINT_ORDERS:
                raise OperationalReplayError(
                    "restart checkpoint has too many work orders"
                )
            schedules.append(
                {
                    "schedule_id": snapshot.binding.schedule_id,
                    "role": snapshot.binding.role,
                    "schedule_sha256": snapshot.binding.schedule_sha256,
                    "bundle_id": snapshot.binding.bundle_id,
                    "manifest_path": str(snapshot.binding.manifest_path),
                    "manifest_sha256": snapshot.binding.manifest_sha256,
                    "work_order_count": len(rows),
                    "generation": snapshot.generation,
                    "state_digest": snapshot.state_digest,
                    "orders": rows,
                }
            )
        core = {
            "kind": RESTART_CHECKPOINT_KIND,
            "schema_version": RESTART_CHECKPOINT_SCHEMA_VERSION,
            "adapter": {
                "name": ADAPTER_NAME,
                "kind": ADAPTER_KIND,
                "implementation_version": IMPLEMENTATION_VERSION,
                "restart_checkpoint_implementation_version": (
                    RESTART_CHECKPOINT_IMPLEMENTATION_VERSION
                ),
            },
            "config_id": config_id,
            "config_sha256": config_sha256,
            "campaign_id": campaign_id,
            "schedule_set_id": schedule_set_id,
            "created_at": created_at,
            "previous_checkpoint_sha256": previous_checkpoint_sha256,
            "schedules": schedules,
            "totals": {
                "schedule_count": len(schedules),
                "work_order_count": work_orders,
                "completed_count": completed_total,
                "pending_count": work_orders - completed_total,
                "completed_media_byte_count": media_bytes,
            },
            "policy": copy.deepcopy(_RESTART_POLICY),
        }
        document = {**core, "identity_sha256": _sha256(canonical_bytes(core))}
        if len(canonical_bytes(document)) > MAX_RESTART_CHECKPOINT_BYTES:
            raise OperationalReplayError("restart checkpoint exceeds its byte cap")
        return document

    def _validated_restart_checkpoint(
        self,
        value: Any,
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        """Validate and parse a complete v2 restart document without filesystem I/O."""

        if not isinstance(value, dict):
            raise OperationalReplayError("restart checkpoint must be an object")
        encoded = canonical_bytes(value)
        if cancellation_boundary is not None:
            cancellation_boundary()
        if len(encoded) > MAX_RESTART_CHECKPOINT_BYTES:
            raise OperationalReplayError("restart checkpoint exceeds its byte cap")
        keys = {
            "kind",
            "schema_version",
            "adapter",
            "config_id",
            "config_sha256",
            "campaign_id",
            "schedule_set_id",
            "created_at",
            "previous_checkpoint_sha256",
            "schedules",
            "totals",
            "policy",
            "identity_sha256",
        }
        if set(value) != keys:
            raise OperationalReplayError("restart checkpoint has unexpected fields")
        core = {key: value[key] for key in keys - {"identity_sha256"}}
        identity = _digest(
            value["identity_sha256"], "restart checkpoint identity SHA-256"
        )
        if identity != _sha256(canonical_bytes(core)):
            raise OperationalReplayError("restart checkpoint identity is invalid")
        if cancellation_boundary is not None:
            cancellation_boundary()
        expected_adapter = {
            "name": ADAPTER_NAME,
            "kind": ADAPTER_KIND,
            "implementation_version": IMPLEMENTATION_VERSION,
            "restart_checkpoint_implementation_version": (
                RESTART_CHECKPOINT_IMPLEMENTATION_VERSION
            ),
        }
        if (
            value["kind"] != RESTART_CHECKPOINT_KIND
            or value["schema_version"] != RESTART_CHECKPOINT_SCHEMA_VERSION
            or value["adapter"] != expected_adapter
            or value["policy"] != _RESTART_POLICY
        ):
            raise OperationalReplayError("restart checkpoint header is incompatible")
        _text(value["config_id"], "restart checkpoint config ID", 128)
        _digest(value["config_sha256"], "restart checkpoint config SHA-256")
        _text(value["campaign_id"], "restart checkpoint campaign ID", 128)
        _text(
            value["schedule_set_id"],
            "restart checkpoint schedule-set ID",
            128,
        )
        _canonical_utc(value["created_at"], "restart checkpoint creation time")
        if value["previous_checkpoint_sha256"] is not None:
            _digest(
                value["previous_checkpoint_sha256"],
                "previous restart checkpoint SHA-256",
            )
        raw_schedules = value["schedules"]
        if (
            not isinstance(raw_schedules, list)
            or len(raw_schedules) > MAX_SNAPSHOTS
        ):
            raise OperationalReplayError("restart checkpoint schedule vector is invalid")

        parsed: dict[str, dict[str, Any]] = {}
        completed_total = 0
        media_bytes = 0
        work_order_total = 0
        schedule_keys = {
            "schedule_id",
            "role",
            "schedule_sha256",
            "bundle_id",
            "manifest_path",
            "manifest_sha256",
            "work_order_count",
            "generation",
            "state_digest",
            "orders",
        }
        order_keys = {
            "ordinal",
            "job_id",
            "work_order_identity_sha256",
            "work_order_file_sha256",
            "result_path",
            "state",
            "witness",
        }
        schedule_order: list[str] = []
        for schedule_index, schedule in enumerate(raw_schedules):
            label = f"restart checkpoint schedule {schedule_index}"
            if not isinstance(schedule, dict) or set(schedule) != schedule_keys:
                raise OperationalReplayError(f"{label} has unexpected fields")
            schedule_id = _text(schedule["schedule_id"], f"{label} ID", 128)
            if schedule_id in parsed:
                raise OperationalReplayError("restart checkpoint repeats a schedule ID")
            schedule_order.append(schedule_id)
            role = _text(schedule["role"], f"{label} role", 128)
            schedule_sha256 = _digest(
                schedule["schedule_sha256"], f"{label} schedule SHA-256"
            )
            bundle_id = _text(schedule["bundle_id"], f"{label} bundle ID", 128)
            manifest_path = _lexical_absolute(
                schedule["manifest_path"], f"{label} manifest path"
            )
            if schedule["manifest_path"] != str(manifest_path):
                raise OperationalReplayError(f"{label} manifest path is not lexical")
            manifest_sha256 = _digest(
                schedule["manifest_sha256"], f"{label} manifest SHA-256"
            )
            work_order_count = _integer(
                schedule["work_order_count"], f"{label} work-order count"
            )
            generation = _integer(schedule["generation"], f"{label} generation", 1)
            state_digest = _digest(
                schedule["state_digest"], f"{label} state digest"
            )
            raw_orders = schedule["orders"]
            if (
                not isinstance(raw_orders, list)
                or len(raw_orders) != work_order_count
                or len(raw_orders) > MAX_ORDERS
            ):
                raise OperationalReplayError(f"{label} order vector is invalid")
            orders: list[_OrderBinding] = []
            states: list[dict[str, Any] | None] = []
            witnesses: list[_CompletedWitness | None] = []
            for expected_ordinal, row in enumerate(raw_orders, 1):
                row_label = f"{label} order {expected_ordinal}"
                if not isinstance(row, dict) or set(row) != order_keys:
                    raise OperationalReplayError(f"{row_label} has unexpected fields")
                ordinal = _integer(row["ordinal"], f"{row_label} ordinal", 1)
                if ordinal != expected_ordinal:
                    raise OperationalReplayError(f"{row_label} ordinal is not contiguous")
                result_path = _lexical_absolute(
                    row["result_path"], f"{row_label} result path"
                )
                if row["result_path"] != str(result_path):
                    raise OperationalReplayError(f"{row_label} result path is not lexical")
                order_binding = _OrderBinding(
                    ordinal=ordinal,
                    job_id=_text(row["job_id"], f"{row_label} job ID", 256),
                    work_order_sha256=_digest(
                        row["work_order_identity_sha256"],
                        f"{row_label} work-order identity SHA-256",
                    ),
                    work_order_file_sha256=_digest(
                        row["work_order_file_sha256"],
                        f"{row_label} work-order file SHA-256",
                    ),
                    result_path=str(result_path),
                )
                state = _restart_state_from_document(
                    row["state"], label=f"{row_label} state"
                )
                if state is None:
                    if row["witness"] is not None:
                        raise OperationalReplayError(
                            f"{row_label} pending state has a witness"
                        )
                    witness = None
                else:
                    if row["witness"] is None:
                        raise OperationalReplayError(
                            f"{row_label} completed state lacks a witness"
                        )
                    witness = _witness_from_document(
                        row["witness"], label=f"{row_label} witness"
                    )
                    completed_total += 1
                    media_bytes += state["byte_count"]
                orders.append(order_binding)
                states.append(state)
                witnesses.append(witness)
                # Parsing one canonical order row is the smallest complete
                # restart-document unit.  Cancellation here abandons only local
                # decoded vectors; no replay snapshot has been installed.
                if cancellation_boundary is not None:
                    cancellation_boundary()
            order_tuple = tuple(orders)
            if state_digest != _state_digest(order_tuple, states):
                raise OperationalReplayError(f"{label} state digest is invalid")
            parsed[schedule_id] = {
                "binding": QueueBinding(
                    schedule_id=schedule_id,
                    role=role,
                    schedule_sha256=schedule_sha256,
                    manifest_path=manifest_path,
                    manifest_sha256=manifest_sha256,
                    bundle_id=bundle_id,
                ),
                "generation": generation,
                "state_digest": state_digest,
                "orders": order_tuple,
                "states": states,
                "witnesses": witnesses,
            }
            work_order_total += work_order_count
            if work_order_total > MAX_CHECKPOINT_ORDERS:
                raise OperationalReplayError(
                    "restart checkpoint has too many work orders"
                )
        if schedule_order != sorted(schedule_order):
            raise OperationalReplayError("restart checkpoint schedules are not sorted")
        totals = value["totals"]
        expected_totals = {
            "schedule_count": len(raw_schedules),
            "work_order_count": work_order_total,
            "completed_count": completed_total,
            "pending_count": work_order_total - completed_total,
            "completed_media_byte_count": media_bytes,
        }
        if (
            not isinstance(totals, dict)
            or set(totals) != set(expected_totals)
            or any(
                _integer(totals.get(key), f"restart checkpoint total {key}")
                != expected
                for key, expected in expected_totals.items()
            )
        ):
            raise OperationalReplayError("restart checkpoint totals are invalid")
        if cancellation_boundary is not None:
            cancellation_boundary()
        validated = copy.deepcopy(value)
        if cancellation_boundary is not None:
            cancellation_boundary()
        return validated, parsed

    def prepare_restart_checkpoint(
        self,
        document: Any,
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> PreparedRestartCheckpoint:
        """Parse one v2 document once for bounded O(document + schedules) restore."""

        self._assert_process()
        if isinstance(document, PreparedRestartCheckpoint):
            if (
                document._authority is not self._restart_checkpoint_authority
                or document._pid != os.getpid()
            ):
                raise OperationalReplayError(
                    "prepared restart checkpoint belongs to another store or process"
                )
            if cancellation_boundary is not None:
                cancellation_boundary()
            return document
        validated, schedules = self._validated_restart_checkpoint(
            document,
            cancellation_boundary=cancellation_boundary,
        )
        if cancellation_boundary is not None:
            cancellation_boundary()
        return PreparedRestartCheckpoint(
            authority=self._restart_checkpoint_authority,
            document=validated,
            schedules=schedules,
        )

    def _validate_persisted_witness(
        self,
        order: dict[str, Any],
        state: dict[str, Any],
        witness: _CompletedWitness,
    ) -> None:
        """Bind serialized metadata to the exact order/state topology."""

        self._validate_completed_state(order, state)
        output_root = _lexical_absolute(order["output"]["root"], "output root")
        result_path = _lexical_absolute(
            self.router.queue_runner._result_path(order), "acquisition result path"
        )
        payload_path = _lexical_absolute(
            state["result"]["admission"]["path"], "content-addressed payload"
        )
        if tuple(item.path for item in witness.result_ancestors) != (
            _expected_ancestor_paths(
                output_root, result_path, label="persisted acquisition result"
            )
        ):
            raise OperationalReplayError(
                "persisted result witness has an invalid ancestor topology"
            )
        if tuple(item.path for item in witness.payload_ancestors) != (
            _expected_ancestor_paths(
                output_root, payload_path, label="persisted content-addressed payload"
            )
        ):
            raise OperationalReplayError(
                "persisted payload witness has an invalid ancestor topology"
            )
        expected_result_size = len(
            self.router.queue_runner.acquire.pretty_json(state["result"]).encode("utf-8")
        )
        if (
            witness.result_file.size != expected_result_size
            or not 1 <= witness.result_file.size <= MAX_RESULT_ENVELOPE_BYTES
            or witness.payload_file.size != state["byte_count"]
            or witness.result_file.link_count != 1
            or witness.payload_file.link_count != 1
            or witness.result_file.uid != os.getuid()
            or stat.S_IMODE(witness.result_file.mode) & 0o022
            or witness.result_parent != witness.result_ancestors[-1].fingerprint
        ):
            raise OperationalReplayError(
                "persisted completed witness conflicts with its admitted state"
            )

    def _install_restart_snapshot(self, snapshot: _QueueSnapshot) -> None:
        """Atomically install one fully validated restart candidate."""

        with self._lock:
            existing = self._snapshots.get(snapshot.binding.schedule_id)
            if existing is not None:
                raise OperationalReplayError(
                    f"schedule {snapshot.binding.schedule_id} already has replay authority"
                )
            self._snapshots[snapshot.binding.schedule_id] = copy.deepcopy(snapshot)

    def hydrate_restart_checkpoint(
        self,
        binding: QueueBinding,
        bundle: dict[str, Any],
        document: Any | PreparedRestartCheckpoint,
        *,
        cancellation_boundary: Callable[[], None] | None = None,
        trust_completed_copy: bool = False,
    ) -> dict[str, Any]:
        """Hydrate v2 authority, hashing only new or metadata-changed ordinals."""

        self._assert_process()
        if type(trust_completed_copy) is not bool:
            raise OperationalReplayError("trusted-copy recovery flag must be boolean")
        prepared = self.prepare_restart_checkpoint(
            document,
            cancellation_boundary=cancellation_boundary,
        )
        checkpoint = prepared._document
        schedules = prepared._schedules
        persisted = schedules.get(binding.schedule_id)
        if persisted is None:
            raise DeepAuditRequired(
                f"restart checkpoint lacks schedule {binding.schedule_id}"
            )
        if persisted["binding"] != binding:
            raise DeepAuditRequired(
                f"restart checkpoint binding differs for schedule {binding.schedule_id}"
            )
        orders, raw_orders = self._bundle_shape(bundle, binding, deep=True)
        if orders != persisted["orders"]:
            raise DeepAuditRequired("restart checkpoint queue order binding changed")

        persisted_states = persisted["states"]
        persisted_witnesses = persisted["witnesses"]
        for raw_order, state, witness in zip(
            raw_orders, persisted_states, persisted_witnesses, strict=True
        ):
            if state is not None and witness is not None:
                self._validate_persisted_witness(raw_order, state, witness)
            if cancellation_boundary is not None:
                cancellation_boundary()

        states: list[dict[str, Any] | None] = []
        witnesses: list[_CompletedWitness | None] = []
        fast_reused_ordinals: list[int] = []
        targeted_ordinals: list[int] = []
        new_exact_ordinals: list[int] = []
        trusted_copy_ordinals: list[int] = []
        result_envelope_bytes_read = 0
        targeted_media_bytes = 0

        for order_binding, raw_order, old_state, old_witness in zip(
            orders,
            raw_orders,
            persisted_states,
            persisted_witnesses,
            strict=True,
        ):
            ordinal = order_binding.ordinal
            if old_state is None:
                if self._pending_unchanged(raw_order):
                    states.append(None)
                    witnesses.append(None)
                    if cancellation_boundary is not None:
                        cancellation_boundary()
                    continue
                observed, exact_witness = self._coherent_exact_inspection(raw_order)
                targeted_ordinals.append(ordinal)
                if observed is not None:
                    new_exact_ordinals.append(ordinal)
                    targeted_media_bytes += observed["byte_count"]
                states.append(copy.deepcopy(observed))
                witnesses.append(exact_witness)
                if cancellation_boundary is not None:
                    cancellation_boundary()
                continue

            if old_witness is None:
                raise OperationalReplayError(
                    f"completed restart state {ordinal} lacks a witness"
                )
            if self._pending_unchanged(raw_order):
                raise OperationalReplayError(
                    f"completed acquisition result {ordinal} disappeared"
                )

            provisional_state: dict[str, Any] | None = None
            provisional_witness: _CompletedWitness | None = None
            metadata_coherent = False
            try:
                provisional_state, provisional_witness = (
                    self._provisional_completed_state(raw_order)
                )
                result_envelope_bytes_read += provisional_witness.result_file.size
                closing_witness = self._completed_witness(
                    raw_order, provisional_state
                )
                metadata_coherent = closing_witness == provisional_witness
            except Exception:
                metadata_coherent = False

            if (
                metadata_coherent
                and provisional_state == old_state
                and provisional_witness is not None
                and self._restart_witness_compatible(
                    old_witness, provisional_witness
                )
            ):
                states.append(copy.deepcopy(old_state))
                witnesses.append(provisional_witness)
                fast_reused_ordinals.append(ordinal)
                if cancellation_boundary is not None:
                    cancellation_boundary()
                continue

            before_state = provisional_state if metadata_coherent else None
            before_witness = provisional_witness if metadata_coherent else None
            if trust_completed_copy:
                if (
                    not metadata_coherent
                    or provisional_state != old_state
                    or provisional_witness is None
                    or not self._trusted_copy_witness_compatible(old_witness, provisional_witness)
                ):
                    raise OperationalReplayError(
                        f"trusted copied acquisition {ordinal} conflicts with saved envelope or metadata"
                    )
                states.append(copy.deepcopy(old_state))
                witnesses.append(provisional_witness)
                trusted_copy_ordinals.append(ordinal)
                if cancellation_boundary is not None:
                    cancellation_boundary()
                continue
            observed, exact_witness = self._coherent_exact_inspection(
                raw_order,
                before_state=before_state,
                before_witness=before_witness,
            )
            targeted_ordinals.append(ordinal)
            if observed is None:
                raise OperationalReplayError(
                    f"completed acquisition result {ordinal} disappeared"
                )
            targeted_media_bytes += observed["byte_count"]
            if observed != old_state:
                raise OperationalReplayError(
                    f"completed acquisition result {ordinal} conflicts with restart authority"
                )
            states.append(copy.deepcopy(observed))
            witnesses.append(exact_witness)
            if cancellation_boundary is not None:
                cancellation_boundary()

        changed = bool(targeted_ordinals or trusted_copy_ordinals)
        candidate = _QueueSnapshot(
            binding=binding,
            generation=persisted["generation"] + (1 if changed else 0),
            orders=orders,
            states=states,
            witnesses=witnesses,
            state_digest=_state_digest(orders, states),
        )
        if cancellation_boundary is not None:
            cancellation_boundary()
        self._install_restart_snapshot(candidate)
        completed = sum(state is not None for state in states)
        legacy_deep_logical_payload_bytes = 4 * sum(
            state["byte_count"] for state in states if state is not None
        )
        targeted_revalidated_logical_payload_bytes = 2 * targeted_media_bytes
        avoided_logical_payload_bytes = max(
            0,
            legacy_deep_logical_payload_bytes
            - targeted_revalidated_logical_payload_bytes,
        )
        telemetry = {
            "mode": "restart_checkpoint_hydration",
            "checkpoint_identity_sha256": checkpoint["identity_sha256"],
            "schedule_id": binding.schedule_id,
            "checkpoint_generation": persisted["generation"],
            "generation": candidate.generation,
            "state_digest": candidate.state_digest,
            "work_order_count": len(states),
            "completed_count": completed,
            "pending_count": len(states) - completed,
            "fast_reused_items": len(fast_reused_ordinals),
            "fast_reused_ordinals": fast_reused_ordinals,
            "targeted_revalidated_items": len(targeted_ordinals),
            "targeted_revalidated_ordinals": targeted_ordinals,
            "new_exact_items": len(new_exact_ordinals),
            "new_exact_ordinals": new_exact_ordinals,
            "trusted_copy_items": len(trusted_copy_ordinals),
            "trusted_copy_ordinals": trusted_copy_ordinals,
            "result_envelope_bytes_read": result_envelope_bytes_read,
            "targeted_revalidated_media_bytes": targeted_media_bytes,
            "targeted_revalidated_logical_payload_bytes": (
                targeted_revalidated_logical_payload_bytes
            ),
            "legacy_deep_logical_payload_bytes": (
                legacy_deep_logical_payload_bytes
            ),
            "avoided_logical_payload_bytes": avoided_logical_payload_bytes,
        }
        return {"states": copy.deepcopy(states), "telemetry": telemetry}

    def bootstrap_restart_snapshot(
        self,
        binding: QueueBinding,
        bundle: dict[str, Any],
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """TOFU-migrate legacy results using two stable metadata-only passes.

        This deliberately authorized migration reads canonical result envelopes and
        no-follow metadata, but never reads or hashes payload bytes.  Its exported
        v2 checkpoint becomes the authority used by subsequent restarts.
        """

        self._assert_process()
        orders, raw_orders = self._bundle_shape(bundle, binding, deep=True)
        states: list[dict[str, Any] | None] = []
        witnesses: list[_CompletedWitness | None] = []
        completed_ordinals: list[int] = []
        envelope_bytes = 0
        avoided_logical_payload_bytes = 0
        for order_binding, raw_order in zip(orders, raw_orders, strict=True):
            if self._pending_unchanged(raw_order):
                states.append(None)
                witnesses.append(None)
                if cancellation_boundary is not None:
                    cancellation_boundary()
                continue
            first_state, first_witness = self._provisional_completed_state(raw_order)
            second_state, second_witness = self._provisional_completed_state(raw_order)
            envelope_bytes += (
                first_witness.result_file.size + second_witness.result_file.size
            )
            if first_state != second_state or first_witness != second_witness:
                raise OperationalReplayError(
                    f"metadata bootstrap state changed for ordinal {order_binding.ordinal}"
                )
            states.append(copy.deepcopy(second_state))
            witnesses.append(second_witness)
            completed_ordinals.append(order_binding.ordinal)
            avoided_logical_payload_bytes += 4 * second_state["byte_count"]
            if cancellation_boundary is not None:
                cancellation_boundary()
        candidate = _QueueSnapshot(
            binding=binding,
            generation=1,
            orders=orders,
            states=states,
            witnesses=witnesses,
            state_digest=_state_digest(orders, states),
        )
        if cancellation_boundary is not None:
            cancellation_boundary()
        self._install_restart_snapshot(candidate)
        completed = len(completed_ordinals)
        telemetry = {
            "mode": "metadata_bootstrap",
            "schedule_id": binding.schedule_id,
            "generation": candidate.generation,
            "state_digest": candidate.state_digest,
            "work_order_count": len(states),
            "completed_count": completed,
            "pending_count": len(states) - completed,
            "metadata_bootstrap_items": completed,
            "metadata_bootstrap_ordinals": completed_ordinals,
            "result_envelope_bytes_read": envelope_bytes,
            "payload_bytes_read": 0,
            "avoided_logical_payload_bytes": avoided_logical_payload_bytes,
        }
        return {"states": copy.deepcopy(states), "telemetry": telemetry}

    def snapshot_summary(self, binding: QueueBinding) -> dict[str, Any]:
        """Return bounded digest-only telemetry for backend integration/tests."""

        self._assert_process()
        with self._lock:
            snapshot = self._snapshots.get(binding.schedule_id)
            if snapshot is None or snapshot.binding != binding:
                raise DeepAuditRequired("no matching operational replay snapshot")
            completed = sum(state is not None for state in snapshot.states)
            return {
                "schedule_id": binding.schedule_id,
                "bundle_id": binding.bundle_id,
                "generation": snapshot.generation,
                "state_digest": snapshot.state_digest,
                "completed_count": completed,
                "pending_count": len(snapshot.states) - completed,
            }

    def admit_if_snapshot_current(
        self,
        binding: QueueBinding,
        *,
        generation: int,
        state_digest: str,
        completed_count: int,
        pending_count: int,
        admit: Callable[[], None],
    ) -> bool:
        """Linearize one cache admission against exact snapshot commits.

        ``False`` means the caller's already-verified authority became historical
        before admission.  Equal generations must still match every bounded state
        projection; ahead or conflicting authority fails closed.  The callback is
        intentionally executed while the replay-store lock is held so a concurrent
        session cannot commit between the comparison and the cache mutation.
        """

        self._assert_process()
        generation = _integer(generation, "admitted replay generation", 1)
        state_digest = _digest(
            state_digest, "admitted replay state digest"
        )
        completed_count = _integer(
            completed_count, "admitted replay completed count"
        )
        pending_count = _integer(
            pending_count, "admitted replay pending count"
        )
        if not callable(admit):
            raise OperationalReplayError("snapshot admission callback is not callable")
        with self._lock:
            snapshot = self._snapshots.get(binding.schedule_id)
            if snapshot is None or snapshot.binding != binding:
                raise DeepAuditRequired("no matching operational replay snapshot")
            if generation > snapshot.generation:
                raise OperationalReplayError(
                    "admitted replay generation is ahead of shared exact authority"
                )
            if generation < snapshot.generation:
                return False
            completed = sum(state is not None for state in snapshot.states)
            if (
                state_digest != snapshot.state_digest
                or completed_count != completed
                or pending_count != len(snapshot.states) - completed
            ):
                raise OperationalReplayError(
                    "admitted replay generation conflicts with shared exact authority"
                )
            admit()
            return True


__all__ = [
    "ADAPTER_KIND",
    "ADAPTER_NAME",
    "CHECKPOINT_KIND",
    "CheckpointDeferred",
    "DeepAuditRequired",
    "IMPLEMENTATION_VERSION",
    "OperationalReplayError",
    "OperationalReplayStore",
    "PreparedRestartCheckpoint",
    "QueueBinding",
    "QueueReplayRouter",
    "RESTART_CHECKPOINT_IMPLEMENTATION_VERSION",
    "RESTART_CHECKPOINT_KIND",
    "RESTART_CHECKPOINT_SCHEMA_VERSION",
    "canonical_bytes",
    "install_queue_replay_router",
]
