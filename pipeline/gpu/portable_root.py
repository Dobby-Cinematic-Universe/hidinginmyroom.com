#!/usr/bin/env python3
"""Restart-portable root registration and retained Linux filesystem checks.

This module is a dependency-light foundation for successor GPU contracts.  A root
registration binds a logical root ID, an exact path, a physical tier, an owner
policy, and a stable Btrfs filesystem UUID.  Linux device numbers, inode numbers,
and timestamps may be retained as producer-time diagnostics, but they are never
compared with a later invocation.

During one invocation the rules are deliberately stricter.  Every component from
``/`` to the registered root is opened without following symlinks and retained.
Descriptor/path identity, the current mount ID, the current Btrfs UUID, ownership,
mode, and same-filesystem relationships are rechecked before return.  Those live
checks prevent pathname replacement without turning kernel object numbers into
durable evidence identity.

The module performs no GPU work, subprocess execution, network access, catalogue
mutation, publication, or filesystem discovery outside explicitly supplied paths.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


REGISTRATION_KIND = "himr_gpu_root_registration"
REGISTRATION_SCHEMA_VERSION = 1
REGISTRATION_ID_PREFIX = "gpurootreg_"
FILESYSTEM_TYPE = "btrfs"

ALLOWED_TIERS = frozenset({"hot_main_drive", "trusted_execution_snapshot"})
ROOT_ID_RE = re.compile(r"^[a-z][a-z0-9._-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REGISTRATION_ID_RE = re.compile(r"^gpurootreg_[0-9a-f]{32}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

MAX_REGISTRATION_BYTES = 64 * 1024
MAX_PATH_BYTES = 4096
MAX_FDINFO_BYTES = 16 * 1024
READ_CHUNK_BYTES = 1024 * 1024

# ``struct btrfs_ioctl_fs_info_args`` is 1024 bytes and begins with two u64
# values followed by the 16-byte filesystem ID.  Linux defines this request as
# ``_IOR(BTRFS_IOCTL_MAGIC, 31, struct btrfs_ioctl_fs_info_args)``.
BTRFS_IOC_FS_INFO = 0x8400941F
BTRFS_FS_INFO_SIZE = 1024
BTRFS_FSID_OFFSET = 16
BTRFS_FSID_BYTES = 16

REGISTRATION_POLICY = {
    "append_only_successor_documents": True,
    "filesystem_uuid_is_placement_authority": True,
    "historical_stat_fields_are_authoritative": False,
    "live_retained_descriptor_checks_required": True,
}


class PortableRootError(RuntimeError):
    """A registration, retained path, or live filesystem check failed."""


def canonical_bytes(value: Any) -> bytes:
    """Return the sole canonical JSON encoding used for stable identities."""

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
        raise PortableRootError(f"value is not canonical JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PortableRootError(f"JSON object contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise PortableRootError(f"JSON contains non-finite numeric constant {value}")


def parse_json_bytes(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise PortableRootError(f"{label} is not strict UTF-8") from error
    try:
        return json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except PortableRootError:
        raise
    except (json.JSONDecodeError, ValueError) as error:
        raise PortableRootError(f"{label} is not strict JSON: {error}") from error


def _exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise PortableRootError(f"{label} must contain exactly {sorted(keys)}")
    return value


def _strict_int(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PortableRootError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise PortableRootError(
            f"{label} must be between {minimum} and {maximum}"
        )
    return value


def _strict_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise PortableRootError(f"{label} must be a lowercase SHA-256")
    return value


def _canonical_uuid(value: Any, label: str) -> str:
    if not isinstance(value, str) or not UUID_RE.fullmatch(value):
        raise PortableRootError(f"{label} must be a canonical lowercase UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise PortableRootError(f"{label} is not a valid UUID") from error
    if str(parsed) != value or parsed.int == 0:
        raise PortableRootError(f"{label} must be a nonzero canonical UUID")
    return value


def normalized_absolute_path(value: str | Path, label: str) -> Path:
    raw = os.fspath(value)
    if (
        not isinstance(raw, str)
        or not raw
        or len(raw.encode("utf-8")) > MAX_PATH_BYTES
        or "\x00" in raw
        or "\\" in raw
        or "//" in raw
        or not os.path.isabs(raw)
        or os.path.normpath(raw) != raw
        or raw == "/"
    ):
        raise PortableRootError(
            f"{label} must be one normalized absolute non-filesystem-root path"
        )
    return Path(raw)


def normalized_relative_path(value: str | Path, label: str) -> PurePosixPath:
    raw = os.fspath(value)
    if (
        not isinstance(raw, str)
        or not raw
        or len(raw.encode("utf-8")) > MAX_PATH_BYTES
        or "\x00" in raw
        or "\\" in raw
        or "//" in raw
        or raw.startswith("/")
        or raw.endswith("/")
    ):
        raise PortableRootError(f"{label} must be one normalized relative path")
    pure = PurePosixPath(raw)
    if (
        pure.as_posix() != raw
        or not pure.parts
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise PortableRootError(f"{label} contains an unsafe path component")
    return pure


def historical_stat_observation(value: os.stat_result) -> dict[str, int]:
    """Return diagnostics which a later replay must never compare as authority."""

    return {
        "st_dev": int(value.st_dev),
        "st_ino": int(value.st_ino),
        "st_mtime_ns": int(value.st_mtime_ns),
        "st_ctime_ns": int(value.st_ctime_ns),
    }


def _validate_historical_observation(value: Any) -> dict[str, int] | None:
    if value is None:
        return None
    item = _exact_object(
        value,
        "historical_observation",
        {"st_dev", "st_ino", "st_mtime_ns", "st_ctime_ns"},
    )
    return {
        key: _strict_int(item[key], f"historical_observation.{key}", 0, 2**63 - 1)
        for key in ("st_dev", "st_ino", "st_mtime_ns", "st_ctime_ns")
    }


def _validate_predecessor(value: Any) -> dict[str, str] | None:
    if value is None:
        return None
    item = _exact_object(
        value,
        "predecessor",
        {"registration_id", "identity_sha256"},
    )
    registration_id = item["registration_id"]
    if not isinstance(registration_id, str) or not REGISTRATION_ID_RE.fullmatch(
        registration_id
    ):
        raise PortableRootError("predecessor.registration_id is invalid")
    identity_sha256 = _strict_sha256(
        item["identity_sha256"], "predecessor.identity_sha256"
    )
    if registration_id != f"{REGISTRATION_ID_PREFIX}{identity_sha256[:32]}":
        raise PortableRootError(
            "predecessor.registration_id does not match predecessor identity"
        )
    return {
        "registration_id": registration_id,
        "identity_sha256": identity_sha256,
    }


def make_registration(
    *,
    root_id: str,
    tier: str,
    path: str | Path,
    filesystem_uuid: str,
    owner_uid: int,
    predecessor: dict[str, str] | None = None,
    historical_observation: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Build one immutable registration or append-only successor document."""

    if not isinstance(root_id, str) or not ROOT_ID_RE.fullmatch(root_id):
        raise PortableRootError("root_id is invalid")
    if not isinstance(tier, str) or tier not in ALLOWED_TIERS:
        raise PortableRootError(f"tier must be one of {sorted(ALLOWED_TIERS)}")
    normalized_path = normalized_absolute_path(path, "registration path")
    owner_uid = _strict_int(owner_uid, "owner.uid", 0, 2**31 - 1)
    predecessor = _validate_predecessor(predecessor)
    historical = _validate_historical_observation(historical_observation)
    core = {
        "kind": REGISTRATION_KIND,
        "schema_version": REGISTRATION_SCHEMA_VERSION,
        "root_id": root_id,
        "tier": tier,
        "path": str(normalized_path),
        "filesystem": {
            "type": FILESYSTEM_TYPE,
            "uuid": _canonical_uuid(filesystem_uuid, "filesystem.uuid"),
        },
        "owner": {"policy": "exact_uid", "uid": owner_uid},
        "predecessor": predecessor,
        "historical_observation": historical,
        "policy": dict(REGISTRATION_POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    registration = {
        **core,
        "identity_sha256": identity,
        "registration_id": f"{REGISTRATION_ID_PREFIX}{identity[:32]}",
    }
    if predecessor is not None and predecessor["registration_id"] == registration[
        "registration_id"
    ]:
        raise PortableRootError("registration cannot name itself as predecessor")
    return registration


def validate_registration(value: Any) -> dict[str, Any]:
    item = _exact_object(
        value,
        "root registration",
        {
            "kind",
            "schema_version",
            "root_id",
            "tier",
            "path",
            "filesystem",
            "owner",
            "predecessor",
            "historical_observation",
            "policy",
            "identity_sha256",
            "registration_id",
        },
    )
    if (
        item["kind"] != REGISTRATION_KIND
        or isinstance(item["schema_version"], bool)
        or not isinstance(item["schema_version"], int)
        or item["schema_version"] != REGISTRATION_SCHEMA_VERSION
    ):
        raise PortableRootError("root registration kind or schema version is invalid")
    filesystem = _exact_object(
        item["filesystem"], "filesystem", {"type", "uuid"}
    )
    if filesystem["type"] != FILESYSTEM_TYPE:
        raise PortableRootError(f"filesystem.type must be {FILESYSTEM_TYPE}")
    owner = _exact_object(item["owner"], "owner", {"policy", "uid"})
    if owner["policy"] != "exact_uid":
        raise PortableRootError("owner.policy must be exact_uid")
    policy = _exact_object(
        item["policy"], "policy", set(REGISTRATION_POLICY)
    )
    if any(
        not isinstance(policy[key], bool)
        or policy[key] is not expected_value
        for key, expected_value in REGISTRATION_POLICY.items()
    ):
        raise PortableRootError(
            "registration policy is not the exact fail-closed policy"
        )
    rebuilt = make_registration(
        root_id=item["root_id"],
        tier=item["tier"],
        path=item["path"],
        filesystem_uuid=filesystem["uuid"],
        owner_uid=owner["uid"],
        predecessor=item["predecessor"],
        historical_observation=item["historical_observation"],
    )
    if item != rebuilt:
        raise PortableRootError(
            "root registration is noncanonical or its stable identity is invalid"
        )
    return rebuilt


def _live_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    """Current-operation identity only; never serialize this tuple as authority."""

    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _live_directory_identity(value: os.stat_result) -> tuple[int, ...]:
    """Return stable identity and policy fields for a retained directory.

    Directory link counts and ctime describe namespace activity, not object
    identity. Creating or removing an unrelated child can change both while
    every retained descriptor and path component still names the exact same
    directory. Device/inode, type/mode, owner/group, mount identity, and the
    registered filesystem UUID remain the replacement and policy guards.
    """

    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
    )


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )


def _file_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _read_bounded_fd(descriptor: int, maximum: int, label: str) -> bytes:
    chunks: list[bytes] = []
    remaining = maximum + 1
    while remaining:
        chunk = os.read(descriptor, min(READ_CHUNK_BYTES, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    body = b"".join(chunks)
    if len(body) > maximum:
        raise PortableRootError(f"{label} exceeds {maximum} bytes")
    return body


def current_mount_id(descriptor: int) -> int:
    """Read Linux's current mount ID for a retained descriptor."""

    fdinfo_path = f"/proc/self/fdinfo/{descriptor}"
    try:
        fdinfo_fd = os.open(fdinfo_path, _file_flags())
    except OSError as error:
        raise PortableRootError(
            f"cannot inspect retained descriptor mount identity: {error}"
        ) from error
    try:
        body = _read_bounded_fd(
            fdinfo_fd, MAX_FDINFO_BYTES, "descriptor mount metadata"
        )
    finally:
        os.close(fdinfo_fd)
    try:
        text = body.decode("ascii", errors="strict")
    except UnicodeDecodeError as error:
        raise PortableRootError("descriptor mount metadata is not ASCII") from error
    values = [
        line.split(":", 1)[1].strip()
        for line in text.splitlines()
        if line.startswith("mnt_id:")
    ]
    if len(values) != 1 or not values[0].isdigit():
        raise PortableRootError("descriptor mount metadata lacks one valid mount ID")
    return int(values[0])


def btrfs_filesystem_uuid(descriptor: int) -> str:
    """Measure the Btrfs FSID through a retained directory descriptor."""

    buffer = bytearray(BTRFS_FS_INFO_SIZE)
    try:
        fcntl.ioctl(descriptor, BTRFS_IOC_FS_INFO, buffer, True)
    except OSError as error:
        raise PortableRootError(
            f"Btrfs filesystem UUID ioctl failed for retained root: {error}"
        ) from error
    raw = bytes(
        buffer[BTRFS_FSID_OFFSET : BTRFS_FSID_OFFSET + BTRFS_FSID_BYTES]
    )
    if len(raw) != BTRFS_FSID_BYTES or not any(raw):
        raise PortableRootError("Btrfs filesystem UUID ioctl returned a zero FSID")
    return str(uuid.UUID(bytes=raw))


def _stable_registration_file(
    path: Path,
    label: str,
    *,
    expected_document_uid: int | None = None,
    expected_document_mode: int | None = None,
) -> bytes:
    try:
        lexical = path.lstat()
    except OSError as error:
        raise PortableRootError(f"cannot safely inspect {label}: {error}") from error
    if stat.S_ISLNK(lexical.st_mode) or not stat.S_ISREG(lexical.st_mode):
        raise PortableRootError(f"{label} must be a direct regular file")
    try:
        descriptor = os.open(path, _file_flags())
    except OSError as error:
        raise PortableRootError(f"cannot safely open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if expected_document_uid is None:
            allowed_owner_modes = {(os.geteuid(), 0o400), (0, 0o400)}
        else:
            if (
                isinstance(expected_document_uid, bool)
                or not isinstance(expected_document_uid, int)
                or expected_document_uid < 0
                or expected_document_mode not in {0o400, 0o444}
            ):
                raise PortableRootError(
                    "registration document owner/mode expectation is invalid"
                )
            allowed_owner_modes = {
                (expected_document_uid, int(expected_document_mode))
            }
        if (
            not stat.S_ISREG(opened.st_mode)
            or _live_stat_identity(opened) != _live_stat_identity(lexical)
            or opened.st_nlink != 1
            or (opened.st_uid, stat.S_IMODE(opened.st_mode))
            not in allowed_owner_modes
        ):
            raise PortableRootError(
                f"{label} owner/mode is outside its exact single-link policy"
            )
        body = _read_bounded_fd(descriptor, MAX_REGISTRATION_BYTES, label)
        after_fd = os.fstat(descriptor)
        try:
            after_path = path.lstat()
        except OSError as error:
            raise PortableRootError(
                f"{label} pathname disappeared while read"
            ) from error
        if (
            len(body) != opened.st_size
            or _live_stat_identity(after_fd) != _live_stat_identity(opened)
            or _live_stat_identity(after_path) != _live_stat_identity(opened)
        ):
            raise PortableRootError(f"{label} changed while it was read")
        return body
    finally:
        os.close(descriptor)


def load_registration(
    path_value: str | Path,
    expected_file_sha256: str,
    *,
    expected_root_id: str | None = None,
    expected_tier: str | None = None,
    expected_path: str | Path | None = None,
    expected_document_uid: int | None = None,
    expected_document_mode: int | None = None,
) -> dict[str, Any]:
    """Load one immutable registration with exact physical and semantic binding."""

    path = normalized_absolute_path(path_value, "registration document path")
    expected_digest = _strict_sha256(
        expected_file_sha256, "expected registration document SHA-256"
    )
    body = _stable_registration_file(
        path,
        "root registration document",
        expected_document_uid=expected_document_uid,
        expected_document_mode=expected_document_mode,
    )
    observed_digest = sha256_bytes(body)
    if observed_digest != expected_digest:
        raise PortableRootError(
            "root registration document SHA-256 differs from its expected digest"
        )
    registration = validate_registration(
        parse_json_bytes(body, "root registration document")
    )
    if body != canonical_bytes(registration):
        raise PortableRootError("root registration document is not canonical JSON")
    _require_expected_registration(
        registration,
        expected_root_id=expected_root_id,
        expected_tier=expected_tier,
        expected_path=expected_path,
    )
    return registration


def _require_expected_registration(
    registration: dict[str, Any],
    *,
    expected_root_id: str | None,
    expected_tier: str | None,
    expected_path: str | Path | None,
) -> None:
    if expected_root_id is not None and registration["root_id"] != expected_root_id:
        raise PortableRootError("root registration ID differs from the expected root")
    if expected_tier is not None:
        if not isinstance(expected_tier, str) or expected_tier not in ALLOWED_TIERS:
            raise PortableRootError("expected tier is not a supported physical tier")
        if registration["tier"] != expected_tier:
            raise PortableRootError(
                "root registration tier differs from the expected tier"
            )
    if expected_path is not None:
        normalized = normalized_absolute_path(expected_path, "expected root path")
        if registration["path"] != str(normalized):
            raise PortableRootError(
                "root registration path differs from the expected path"
            )


@dataclass(frozen=True)
class LiveFilesystemObservation:
    """Ephemeral current-operation evidence; never durable identity authority."""

    filesystem_uuid: str
    mount_id: int
    device: int


@dataclass
class _RetainedDirectoryComponent:
    parent_fd: int
    name: str
    descriptor: int
    initial_stat: os.stat_result
    mount_id: int


class RetainedRoot:
    """A registered root whose complete no-follow path chain remains retained."""

    def __init__(
        self,
        *,
        registration: dict[str, Any],
        descriptors: list[int],
        components: list[_RetainedDirectoryComponent],
        filesystem_uuid: str,
    ) -> None:
        self.registration = registration
        self.path = Path(registration["path"])
        self._descriptors = descriptors
        self._components = components
        self.descriptor = descriptors[-1]
        self._initial_stat = os.fstat(self.descriptor)
        self.mount_id = current_mount_id(self.descriptor)
        self.filesystem_uuid = filesystem_uuid
        self._closed = False

    @classmethod
    def open(
        cls,
        registration_value: Any,
        *,
        expected_root_id: str | None = None,
        expected_tier: str | None = None,
        expected_path: str | Path | None = None,
    ) -> "RetainedRoot":
        registration = validate_registration(registration_value)
        _require_expected_registration(
            registration,
            expected_root_id=expected_root_id,
            expected_tier=expected_tier,
            expected_path=expected_path,
        )
        path = Path(registration["path"])
        flags = _directory_flags()
        descriptors: list[int] = []
        components: list[_RetainedDirectoryComponent] = []
        owner_uid = registration["owner"]["uid"]
        allowed_owners = {0, owner_uid}
        try:
            parent_fd = os.open("/", flags)
            descriptors.append(parent_fd)
            for component in path.parts[1:]:
                try:
                    inspected = os.stat(
                        component, dir_fd=parent_fd, follow_symlinks=False
                    )
                except OSError as error:
                    raise PortableRootError(
                        "cannot inspect registered root component "
                        f"{component!r}: {error}"
                    ) from error
                if (
                    stat.S_ISLNK(inspected.st_mode)
                    or not stat.S_ISDIR(inspected.st_mode)
                    or inspected.st_uid not in allowed_owners
                    or (
                        inspected.st_uid == 0
                        and stat.S_IMODE(inspected.st_mode) & 0o022
                    )
                    or (
                        inspected.st_uid == owner_uid
                        and stat.S_IMODE(inspected.st_mode) & 0o002
                    )
                ):
                    raise PortableRootError(
                        "registered root contains an unsafe, writable, or "
                        "unexpected-owner directory component"
                    )
                try:
                    child_fd = os.open(component, flags, dir_fd=parent_fd)
                except OSError as error:
                    raise PortableRootError(
                        "cannot retain registered root component "
                        f"{component!r}: {error}"
                    ) from error
                descriptors.append(child_fd)
                opened = os.fstat(child_fd)
                if _live_directory_identity(opened) != _live_directory_identity(
                    inspected
                ):
                    raise PortableRootError(
                        "registered root changed while a component was opened"
                    )
                components.append(
                    _RetainedDirectoryComponent(
                        parent_fd=parent_fd,
                        name=component,
                        descriptor=child_fd,
                        initial_stat=opened,
                        mount_id=current_mount_id(child_fd),
                    )
                )
                parent_fd = child_fd
            final = os.fstat(descriptors[-1])
            if (
                final.st_uid != owner_uid
                or stat.S_IMODE(final.st_mode) & 0o022
                or not stat.S_ISDIR(final.st_mode)
            ):
                raise PortableRootError(
                    "registered root owner or mode differs from its exact policy"
                )
            observed_uuid = btrfs_filesystem_uuid(descriptors[-1])
            expected_uuid = registration["filesystem"]["uuid"]
            if observed_uuid != expected_uuid:
                raise PortableRootError(
                    "registered root Btrfs filesystem UUID differs from its "
                    "placement binding"
                )
            retained = cls(
                registration=registration,
                descriptors=descriptors,
                components=components,
                filesystem_uuid=observed_uuid,
            )
            retained.verify()
            return retained
        except Exception:
            for descriptor in reversed(descriptors):
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise PortableRootError("retained root is already closed")

    def verify(self) -> None:
        self._ensure_open()
        for component in self._components:
            linked_fd: int | None = None
            try:
                try:
                    linked = os.stat(
                        component.name,
                        dir_fd=component.parent_fd,
                        follow_symlinks=False,
                    )
                    opened = os.fstat(component.descriptor)
                    linked_fd = os.open(
                        component.name,
                        _directory_flags(),
                        dir_fd=component.parent_fd,
                    )
                except OSError as error:
                    raise PortableRootError(
                        f"retained root changed or disappeared: {self.path}"
                    ) from error
                expected = _live_directory_identity(component.initial_stat)
                if (
                    stat.S_ISLNK(linked.st_mode)
                    or not stat.S_ISDIR(linked.st_mode)
                    or _live_directory_identity(linked) != expected
                    or _live_directory_identity(opened) != expected
                    or current_mount_id(component.descriptor) != component.mount_id
                    or current_mount_id(linked_fd) != component.mount_id
                ):
                    raise PortableRootError(
                        f"retained root identity changed: {self.path}"
                    )
            finally:
                if linked_fd is not None:
                    os.close(linked_fd)
        current = os.fstat(self.descriptor)
        if (
            _live_directory_identity(current)
            != _live_directory_identity(self._initial_stat)
            or current_mount_id(self.descriptor) != self.mount_id
            or btrfs_filesystem_uuid(self.descriptor) != self.filesystem_uuid
        ):
            raise PortableRootError(
                f"retained root metadata or filesystem placement changed: {self.path}"
            )

    def open_file(
        self,
        relative_path: str | Path,
        *,
        label: str = "registered-root file",
        allowed_modes: set[int] | frozenset[int] | None = None,
        owner_uid: int | None = None,
        single_link: bool = True,
    ) -> "RetainedFile":
        return RetainedFile.open(
            self,
            relative_path,
            label=label,
            allowed_modes=allowed_modes,
            owner_uid=owner_uid,
            single_link=single_link,
        )

    def close(self) -> None:
        if self._closed:
            return
        for descriptor in reversed(self._descriptors):
            os.close(descriptor)
        self._descriptors.clear()
        self._closed = True

    def __enter__(self) -> "RetainedRoot":
        self._ensure_open()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def require_live_same_filesystem(
    root: RetainedRoot,
    descriptor: int,
    label: str,
) -> LiveFilesystemObservation:
    """Require one retained descriptor to remain on the root's current mount."""

    root._ensure_open()
    try:
        root_current = os.fstat(root.descriptor)
        candidate = os.fstat(descriptor)
        candidate_mount = current_mount_id(descriptor)
    except OSError as error:
        raise PortableRootError(
            f"cannot inspect {label} retained filesystem identity: {error}"
        ) from error
    if (
        candidate.st_dev != root_current.st_dev
        or candidate_mount != root.mount_id
    ):
        raise PortableRootError(
            f"{label} is not on the registered root's current filesystem and mount"
        )
    return LiveFilesystemObservation(
        filesystem_uuid=root.filesystem_uuid,
        mount_id=candidate_mount,
        device=int(candidate.st_dev),
    )


class RetainedFile:
    """A regular file opened beneath and live-bound to one retained root."""

    def __init__(
        self,
        *,
        root: RetainedRoot,
        relative_path: PurePosixPath,
        descriptor: int,
        parent_descriptors: list[int],
        parent_components: list[_RetainedDirectoryComponent],
        leaf_name: str,
        initial_stat: os.stat_result,
        label: str,
        allowed_modes: frozenset[int] | None,
        owner_uid: int,
        single_link: bool,
    ) -> None:
        self.root = root
        self.relative_path = relative_path
        self.descriptor = descriptor
        self._parent_descriptors = parent_descriptors
        self._parent_components = parent_components
        self._leaf_name = leaf_name
        self._initial_stat = initial_stat
        self.label = label
        self._allowed_modes = allowed_modes
        self._owner_uid = owner_uid
        self._single_link = single_link
        self._closed = False

    @classmethod
    def open(
        cls,
        root: RetainedRoot,
        relative_path_value: str | Path,
        *,
        label: str,
        allowed_modes: set[int] | frozenset[int] | None,
        owner_uid: int | None,
        single_link: bool,
    ) -> "RetainedFile":
        root.verify()
        relative = normalized_relative_path(relative_path_value, label)
        expected_owner = (
            root.registration["owner"]["uid"] if owner_uid is None else owner_uid
        )
        expected_owner = _strict_int(expected_owner, f"{label} owner UID", 0, 2**31 - 1)
        normalized_modes: frozenset[int] | None = None
        if allowed_modes is not None:
            if not isinstance(allowed_modes, (set, frozenset)) or not allowed_modes:
                raise PortableRootError(f"{label} allowed modes must be a nonempty set")
            checked_modes = set()
            for value in allowed_modes:
                checked_modes.add(
                    _strict_int(value, f"{label} allowed mode", 0, 0o7777)
                )
            normalized_modes = frozenset(checked_modes)
        parent_fd = root.descriptor
        parent_descriptors: list[int] = []
        parent_components: list[_RetainedDirectoryComponent] = []
        descriptor: int | None = None
        try:
            for component_name in relative.parts[:-1]:
                try:
                    inspected = os.stat(
                        component_name, dir_fd=parent_fd, follow_symlinks=False
                    )
                except OSError as error:
                    raise PortableRootError(
                        f"cannot inspect {label} parent {component_name!r}: {error}"
                    ) from error
                if stat.S_ISLNK(inspected.st_mode) or not stat.S_ISDIR(
                    inspected.st_mode
                ):
                    raise PortableRootError(
                        f"{label} parent path contains a symlink or non-directory"
                    )
                try:
                    child_fd = os.open(
                        component_name, _directory_flags(), dir_fd=parent_fd
                    )
                except OSError as error:
                    raise PortableRootError(
                        f"cannot retain {label} parent {component_name!r}: {error}"
                    ) from error
                parent_descriptors.append(child_fd)
                opened = os.fstat(child_fd)
                if _live_directory_identity(opened) != _live_directory_identity(
                    inspected
                ):
                    raise PortableRootError(
                        f"{label} parent changed while it was opened"
                    )
                require_live_same_filesystem(root, child_fd, f"{label} parent")
                parent_components.append(
                    _RetainedDirectoryComponent(
                        parent_fd=parent_fd,
                        name=component_name,
                        descriptor=child_fd,
                        initial_stat=opened,
                        mount_id=current_mount_id(child_fd),
                    )
                )
                parent_fd = child_fd
            leaf_name = relative.parts[-1]
            try:
                inspected_leaf = os.stat(
                    leaf_name, dir_fd=parent_fd, follow_symlinks=False
                )
            except OSError as error:
                raise PortableRootError(
                    f"cannot inspect {label} leaf {leaf_name!r}: {error}"
                ) from error
            if stat.S_ISLNK(inspected_leaf.st_mode) or not stat.S_ISREG(
                inspected_leaf.st_mode
            ):
                raise PortableRootError(
                    f"{label} must be a retained non-symlink regular file"
                )
            try:
                descriptor = os.open(leaf_name, _file_flags(), dir_fd=parent_fd)
            except OSError as error:
                raise PortableRootError(
                    f"cannot retain {label} leaf {leaf_name!r}: {error}"
                ) from error
            opened_leaf = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened_leaf.st_mode)
                or _live_stat_identity(opened_leaf)
                != _live_stat_identity(inspected_leaf)
            ):
                raise PortableRootError(
                    f"{label} must be a retained non-symlink regular file"
                )
            mode = stat.S_IMODE(opened_leaf.st_mode)
            if (
                opened_leaf.st_uid != expected_owner
                or mode & 0o022
                or (normalized_modes is not None and mode not in normalized_modes)
                or (single_link and opened_leaf.st_nlink != 1)
            ):
                raise PortableRootError(
                    f"{label} owner, mode, or link policy failed"
                )
            require_live_same_filesystem(root, descriptor, label)
            retained = cls(
                root=root,
                relative_path=relative,
                descriptor=descriptor,
                parent_descriptors=parent_descriptors,
                parent_components=parent_components,
                leaf_name=leaf_name,
                initial_stat=opened_leaf,
                label=label,
                allowed_modes=normalized_modes,
                owner_uid=expected_owner,
                single_link=single_link,
            )
            retained.verify()
            return retained
        except Exception:
            if descriptor is not None:
                os.close(descriptor)
            for parent_descriptor in reversed(parent_descriptors):
                os.close(parent_descriptor)
            raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise PortableRootError(f"{self.label} retained descriptor is closed")

    @property
    def _parent_fd(self) -> int:
        return (
            self._parent_descriptors[-1]
            if self._parent_descriptors
            else self.root.descriptor
        )

    def verify(self) -> None:
        self._ensure_open()
        self.root.verify()
        for component in self._parent_components:
            linked_fd: int | None = None
            try:
                try:
                    linked = os.stat(
                        component.name,
                        dir_fd=component.parent_fd,
                        follow_symlinks=False,
                    )
                    linked_fd = os.open(
                        component.name,
                        _directory_flags(),
                        dir_fd=component.parent_fd,
                    )
                    opened = os.fstat(component.descriptor)
                except OSError as error:
                    raise PortableRootError(
                        f"{self.label} parent changed or disappeared"
                    ) from error
                expected = _live_directory_identity(component.initial_stat)
                if (
                    stat.S_ISLNK(linked.st_mode)
                    or not stat.S_ISDIR(linked.st_mode)
                    or _live_directory_identity(linked) != expected
                    or _live_directory_identity(opened) != expected
                    or current_mount_id(component.descriptor) != component.mount_id
                    or current_mount_id(linked_fd) != component.mount_id
                ):
                    raise PortableRootError(f"{self.label} parent identity changed")
            finally:
                if linked_fd is not None:
                    os.close(linked_fd)
        try:
            linked_leaf = os.stat(
                self._leaf_name,
                dir_fd=self._parent_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            raise PortableRootError(f"{self.label} pathname disappeared") from error
        opened_leaf = os.fstat(self.descriptor)
        expected_leaf = _live_stat_identity(self._initial_stat)
        if (
            stat.S_ISLNK(linked_leaf.st_mode)
            or not stat.S_ISREG(linked_leaf.st_mode)
            or _live_stat_identity(linked_leaf) != expected_leaf
            or _live_stat_identity(opened_leaf) != expected_leaf
        ):
            raise PortableRootError(f"{self.label} identity changed during operation")
        require_live_same_filesystem(self.root, self.descriptor, self.label)

    def read_bytes(self, maximum_bytes: int) -> bytes:
        self._ensure_open()
        maximum = _strict_int(
            maximum_bytes, f"{self.label} maximum bytes", 1, 2**63 - 1
        )
        self.verify()
        initial = os.fstat(self.descriptor)
        if initial.st_size > maximum:
            raise PortableRootError(f"{self.label} exceeds {maximum} bytes")
        chunks: list[bytes] = []
        offset = 0
        while offset < initial.st_size:
            chunk = os.pread(
                self.descriptor,
                min(READ_CHUNK_BYTES, initial.st_size - offset),
                offset,
            )
            if not chunk:
                raise PortableRootError(f"{self.label} ended before its retained size")
            chunks.append(chunk)
            offset += len(chunk)
        if os.pread(self.descriptor, 1, offset):
            raise PortableRootError(f"{self.label} grew while it was read")
        body = b"".join(chunks)
        self.verify()
        if len(body) != initial.st_size:
            raise PortableRootError(f"{self.label} changed while it was read")
        return body

    def stable_evidence(self, maximum_bytes: int) -> dict[str, Any]:
        """Return a restart-portable content/policy projection with no object IDs."""

        body = self.read_bytes(maximum_bytes)
        observed = os.fstat(self.descriptor)
        return {
            "relative_path": self.relative_path.as_posix(),
            "kind": "regular_file",
            "sha256": sha256_bytes(body),
            "byte_count": len(body),
            "mode": stat.S_IMODE(observed.st_mode),
            "owner_policy": {"kind": "exact_uid", "uid": self._owner_uid},
            "link_policy": "single_link" if self._single_link else "not_constrained",
        }

    def close(self) -> None:
        if self._closed:
            return
        os.close(self.descriptor)
        for descriptor in reversed(self._parent_descriptors):
            os.close(descriptor)
        self._parent_descriptors.clear()
        self._closed = True

    def __enter__(self) -> "RetainedFile":
        self._ensure_open()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


__all__ = [
    "ALLOWED_TIERS",
    "BTRFS_IOC_FS_INFO",
    "LiveFilesystemObservation",
    "PortableRootError",
    "REGISTRATION_KIND",
    "REGISTRATION_POLICY",
    "REGISTRATION_SCHEMA_VERSION",
    "RetainedFile",
    "RetainedRoot",
    "btrfs_filesystem_uuid",
    "canonical_bytes",
    "current_mount_id",
    "historical_stat_observation",
    "load_registration",
    "make_registration",
    "normalized_absolute_path",
    "normalized_relative_path",
    "parse_json_bytes",
    "require_live_same_filesystem",
    "sha256_bytes",
    "validate_registration",
]
