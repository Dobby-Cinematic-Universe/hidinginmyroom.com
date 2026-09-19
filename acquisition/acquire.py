#!/usr/bin/env python3
"""Guarded, offline-configured acquisition into a content-addressed media cache.

The direct HTTP and yt-dlp adapters can use the network during an actual run.  Dry
runs never contact a remote source.  No adapter accepts credentials, cookies,
tokens, custom headers, comment crawling, or a Reddit discussion URL.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import http.client
import json
import math
import mimetypes
import os
import re
import signal
import shutil
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO


CONTRACT_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.2"
DEFAULT_JOB_CAP = 10 * 1024**3
RECOMMENDED_CACHE_CAP = 50 * 1024**3
RECOMMENDED_FREE_FLOOR = 80 * 1024**3
CHUNK_SIZE = 1024 * 1024
MONITOR_INTERVAL_SECONDS = 0.05
MAX_SUBPROCESS_LOG_BYTES = 8 * 1024 * 1024
MAX_DURABLE_RESULT_BYTES = 16 * 1024 * 1024
MAX_YTDLP_METADATA_STRING_CHARS = 16_384
MAX_YTDLP_METADATA_NUMBER = 2**53 - 1
MAX_RUNTIME_TREE_FILES = 20_000
MAX_RUNTIME_TREE_BYTES = 1024 * 1024 * 1024
LOCK_FILENAME = ".acquisition-writer.lock"
ADAPTERS = ("local_file", "direct_http", "yt_dlp")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
VREDDIT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{5,64}$")
SENSITIVE_QUERY_RE = re.compile(
    r"(?:auth|authorization|cookie|credential|key|password|secret|signature|token)",
    re.IGNORECASE,
)
SAFE_YTDLP_METADATA_FIELDS = (
    "id",
    "title",
    "channel_id",
    "channel",
    "uploader_id",
    "upload_date",
    "timestamp",
    "duration",
    "webpage_url",
    "original_url",
    "extractor",
    "extractor_key",
    "ext",
    "format_id",
    "width",
    "height",
    "fps",
    "vcodec",
    "acodec",
    "filesize",
    "filesize_approx",
    "availability",
    "live_status",
)
YOUTUBE_YTDLP_PRINT_TEMPLATE = (
    "after_move:%(.{" + ",".join(SAFE_YTDLP_METADATA_FIELDS) + "})j"
)
SAFE_YTDLP_FORMAT_SELECTORS = frozenset(
    {
        "b[height<=720]/b",
        "bv*[height<=720]+ba/b[height<=720]/b",
    }
)
EXACT_YTDLP_FORMAT_PAIR_RE = re.compile(
    r"^[1-9][0-9]{0,4}\+[1-9][0-9]{0,4}$"
)


def safe_ytdlp_format_selector(value: Any) -> bool:
    """Accept the closed generic profiles or one exact numeric video+audio pair.

    The exact-pair form supports reproducible comparison against an earlier yt-dlp
    acquisition without opening the work-order boundary to filters, fallbacks,
    selectors with names, or arbitrary extractor expressions.
    """

    return isinstance(value, str) and (
        value in SAFE_YTDLP_FORMAT_SELECTORS
        or EXACT_YTDLP_FORMAT_PAIR_RE.fullmatch(value) is not None
    )


class AcquisitionError(RuntimeError):
    """A policy, capacity, integrity, or adapter failure."""


class LiveCapacityError(AcquisitionError):
    """A child process crossed its live disk or log reservation."""


class CommandError(AcquisitionError):
    def __init__(self, command: list[str], returncode: int, stderr: str):
        tail = "\n".join(stderr.splitlines()[-40:])
        super().__init__(
            f"Command exited with status {returncode}: {command[0]}\n{tail}".rstrip()
        )
        self.command = command
        self.returncode = returncode


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _stat_fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
        value.st_nlink,
    )


def _hash_descriptor(
    descriptor: int, size: int, *, capture: bool, label: str
) -> tuple[str, bytes | None]:
    digest = hashlib.sha256()
    chunks: list[bytes] | None = [] if capture else None
    offset = 0
    while offset < size:
        try:
            chunk = os.pread(descriptor, min(CHUNK_SIZE, size - offset), offset)
        except OSError as error:
            raise AcquisitionError(f"cannot read pinned {label}: {error}") from error
        if not chunk:
            raise AcquisitionError(f"{label} ended during its pinned read")
        digest.update(chunk)
        if chunks is not None:
            chunks.append(chunk)
        offset += len(chunk)
    try:
        if os.pread(descriptor, 1, size):
            raise AcquisitionError(f"{label} grew during its pinned read")
    except OSError as error:
        raise AcquisitionError(f"cannot finish reading pinned {label}: {error}") from error
    return digest.hexdigest(), None if chunks is None else b"".join(chunks)


class PinnedRegularFile:
    """A no-symlink file descriptor pinned through every managed path component."""

    def __init__(
        self,
        *,
        root: Path,
        path: Path,
        directory_fds: list[int],
        root_stat: os.stat_result,
        components: list[tuple[int, str, int, os.stat_result]],
        leaf_name: str,
        descriptor: int,
        initial_stat: os.stat_result,
        digest: str,
        body: bytes | None,
        label: str,
    ) -> None:
        self.root = root
        self.path = path
        self.directory_fds = directory_fds
        self.root_stat = root_stat
        self.components = components
        self.leaf_name = leaf_name
        self.descriptor = descriptor
        self.initial_stat = initial_stat
        self.digest = digest
        self.body = body
        self.label = label

    @classmethod
    def open(
        cls,
        path: Path,
        *,
        root: Path,
        maximum: int,
        capture: bool,
        label: str,
    ) -> "PinnedRegularFile":
        root = Path(os.path.abspath(os.fspath(root)))
        path = Path(os.path.abspath(os.fspath(path)))
        if path == root or root not in path.parents:
            raise AcquisitionError(f"{label} must be strictly beneath the managed output root")
        relative = path.relative_to(root)
        if not relative.parts:
            raise AcquisitionError(f"{label} has no managed leaf name")

        directory_fds: list[int] = []
        descriptor: int | None = None
        try:
            root_lstat = root.lstat()
            if stat.S_ISLNK(root_lstat.st_mode) or not stat.S_ISDIR(root_lstat.st_mode):
                raise AcquisitionError(
                    f"managed output root for {label} must be a non-symlink directory"
                )
            directory_flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_DIRECTORY", 0)
            )
            root_fd = os.open(root, directory_flags)
            directory_fds.append(root_fd)
            root_opened = os.fstat(root_fd)
            if _stat_fingerprint(root_opened) != _stat_fingerprint(root_lstat):
                raise AcquisitionError(
                    f"managed output root for {label} changed while opening"
                )

            components: list[tuple[int, str, int, os.stat_result]] = []
            parent_fd = root_fd
            for component in relative.parts[:-1]:
                inspected = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISLNK(inspected.st_mode) or not stat.S_ISDIR(inspected.st_mode):
                    raise AcquisitionError(
                        f"{label} path component is not a non-symlink directory: {component}"
                    )
                child_fd = os.open(component, directory_flags, dir_fd=parent_fd)
                directory_fds.append(child_fd)
                opened = os.fstat(child_fd)
                if _stat_fingerprint(opened) != _stat_fingerprint(inspected):
                    raise AcquisitionError(
                        f"{label} path component changed while opening: {component}"
                    )
                components.append((parent_fd, component, child_fd, opened))
                parent_fd = child_fd

            leaf_name = relative.parts[-1]
            inspected = os.stat(leaf_name, dir_fd=parent_fd, follow_symlinks=False)
            if (
                stat.S_ISLNK(inspected.st_mode)
                or not stat.S_ISREG(inspected.st_mode)
                or inspected.st_nlink != 1
            ):
                raise AcquisitionError(
                    f"{label} must be a single-link regular file, not a symlink"
                )
            file_flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            descriptor = os.open(leaf_name, file_flags, dir_fd=parent_fd)
            opened = os.fstat(descriptor)
            if _stat_fingerprint(opened) != _stat_fingerprint(inspected):
                raise AcquisitionError(f"{label} changed while opening its descriptor")
            if opened.st_size < 1 or opened.st_size > maximum:
                raise AcquisitionError(
                    f"{label} must contain 1..{maximum} bytes; observed {opened.st_size}"
                )
            digest, body = _hash_descriptor(
                descriptor, opened.st_size, capture=capture, label=label
            )
            pinned = cls(
                root=root,
                path=path,
                directory_fds=directory_fds,
                root_stat=root_opened,
                components=components,
                leaf_name=leaf_name,
                descriptor=descriptor,
                initial_stat=opened,
                digest=digest,
                body=body,
                label=label,
            )
            # The initial descriptor hash is already complete; recheck the path and
            # descriptor identity here, then reserve the second full hash for the
            # operation's closing verification.
            pinned._verify_identity()
            return pinned
        except Exception:
            if descriptor is not None:
                os.close(descriptor)
            for directory_fd in reversed(directory_fds):
                os.close(directory_fd)
            raise

    def _verify_identity(self) -> os.stat_result:
        try:
            root_lstat = self.root.lstat()
            root_opened = os.fstat(self.directory_fds[0])
            expected_root = _stat_fingerprint(self.root_stat)
            if (
                stat.S_ISLNK(root_lstat.st_mode)
                or _stat_fingerprint(root_lstat) != expected_root
                or _stat_fingerprint(root_opened) != expected_root
            ):
                raise AcquisitionError(
                    f"managed output root identity changed while verifying {self.label}"
                )
            for parent_fd, name, child_fd, initial in self.components:
                linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                opened = os.fstat(child_fd)
                expected = _stat_fingerprint(initial)
                if (
                    stat.S_ISLNK(linked.st_mode)
                    or _stat_fingerprint(linked) != expected
                    or _stat_fingerprint(opened) != expected
                ):
                    raise AcquisitionError(
                        f"{self.label} path identity changed at component {name}"
                    )
            parent_fd = self.directory_fds[-1]
            linked = os.stat(
                self.leaf_name, dir_fd=parent_fd, follow_symlinks=False
            )
            opened = os.fstat(self.descriptor)
        except OSError as error:
            raise AcquisitionError(
                f"{self.label} changed or disappeared during verification"
            ) from error
        expected = _stat_fingerprint(self.initial_stat)
        if (
            stat.S_ISLNK(linked.st_mode)
            or _stat_fingerprint(linked) != expected
            or _stat_fingerprint(opened) != expected
        ):
            raise AcquisitionError(
                f"{self.label} identity or metadata changed during verification"
            )
        return opened

    def verify(self) -> None:
        opened = self._verify_identity()
        observed, _ = _hash_descriptor(
            self.descriptor, opened.st_size, capture=False, label=self.label
        )
        if observed != self.digest:
            raise AcquisitionError(f"{self.label} bytes changed during verification")
        self._verify_identity()

    def close(self) -> None:
        os.close(self.descriptor)
        for directory_fd in reversed(self.directory_fds):
            os.close(directory_fd)


class PinnedFiles:
    def __init__(self) -> None:
        self.files: list[PinnedRegularFile] = []

    def open(
        self,
        path: Path,
        *,
        root: Path,
        maximum: int,
        capture: bool,
        label: str,
    ) -> PinnedRegularFile:
        pinned = PinnedRegularFile.open(
            path, root=root, maximum=maximum, capture=capture, label=label
        )
        self.files.append(pinned)
        return pinned

    def verify(self) -> None:
        for pinned in self.files:
            pinned.verify()

    def close(self) -> None:
        for pinned in reversed(self.files):
            pinned.close()
        self.files.clear()


def strict_json_object(body: bytes, label: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise AcquisitionError(f"{label} contains duplicate JSON key {key!r}")
            result[key] = value
        return result

    def integer(value: str) -> int:
        if len(value.lstrip("-")) > 19:
            raise AcquisitionError(f"{label} contains an oversized integer")
        return int(value)

    def finite_float(value: str) -> float:
        if len(value) > 64:
            raise AcquisitionError(f"{label} contains an oversized number")
        parsed = float(value)
        if not math.isfinite(parsed) or abs(parsed) > MAX_YTDLP_METADATA_NUMBER:
            raise AcquisitionError(f"{label} contains a non-finite or oversized number")
        return parsed

    def invalid_constant(value: str) -> None:
        raise AcquisitionError(f"{label} contains invalid number {value}")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_int=integer,
            parse_float=finite_float,
            parse_constant=invalid_constant,
        )
    except AcquisitionError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise AcquisitionError(f"{label} is not strict UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise AcquisitionError(f"{label} must contain one JSON object")
    return value


def validated_ytdlp_version(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 200
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise AcquisitionError(f"{label} must be a non-empty bounded single-line value")
    return value


def executable_is_script(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            return handle.read(2) == b"#!"
    except OSError as error:
        raise AcquisitionError(f"cannot inspect executable type for {path}: {error}") from error


def runtime_tree_fingerprint(root: Path) -> dict[str, Any]:
    """Hash every regular file below an immutable runtime tree.

    Paths and file sizes are framed into the digest as well as content. Symlinks and
    special files fail closed so a Python launcher cannot escape the reviewed module
    tree through indirection. Per-file identity is checked before and after reading.
    """

    if not root.is_absolute():
        raise AcquisitionError("yt-dlp runtime tree root must be absolute")
    if root.is_symlink():
        raise AcquisitionError("yt-dlp runtime tree root may not be a symbolic link")
    try:
        resolved = root.resolve(strict=True)
        root_before = resolved.stat()
    except OSError as error:
        raise AcquisitionError(f"cannot inspect yt-dlp runtime tree: {error}") from error
    if not stat.S_ISDIR(root_before.st_mode):
        raise AcquisitionError("yt-dlp runtime tree root must be a directory")

    files: list[Path] = []
    try:
        for candidate in resolved.rglob("*"):
            metadata = candidate.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise AcquisitionError(
                    f"yt-dlp runtime tree contains a symbolic link: {candidate}"
                )
            if stat.S_ISDIR(metadata.st_mode):
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise AcquisitionError(
                    f"yt-dlp runtime tree contains a special file: {candidate}"
                )
            files.append(candidate)
    except OSError as error:
        raise AcquisitionError(f"cannot enumerate yt-dlp runtime tree: {error}") from error
    files.sort(key=lambda path: path.relative_to(resolved).as_posix())
    if not files or len(files) > MAX_RUNTIME_TREE_FILES:
        raise AcquisitionError(
            "yt-dlp runtime tree must contain 1.."
            f"{MAX_RUNTIME_TREE_FILES} regular files"
        )

    digest = hashlib.sha256()
    digest.update(b"HIMR-YTDLP-RUNTIME-TREE-V1\0")
    total_bytes = 0
    for path in files:
        relative = path.relative_to(resolved).as_posix().encode("utf-8")
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise AcquisitionError(f"yt-dlp runtime file changed type: {path}")
        total_bytes += before.st_size
        if total_bytes > MAX_RUNTIME_TREE_BYTES:
            raise AcquisitionError(
                "yt-dlp runtime tree exceeds the "
                f"{MAX_RUNTIME_TREE_BYTES}-byte integrity limit"
            )
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(before.st_size.to_bytes(8, "big"))
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
            with os.fdopen(descriptor, "rb") as handle:
                opened = os.fstat(handle.fileno())
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or (opened.st_dev, opened.st_ino, opened.st_size)
                    != (before.st_dev, before.st_ino, before.st_size)
                ):
                    raise AcquisitionError(
                        f"yt-dlp runtime file changed while opening: {path}"
                    )
                while chunk := handle.read(CHUNK_SIZE):
                    digest.update(chunk)
                after = os.fstat(handle.fileno())
        except OSError as error:
            raise AcquisitionError(f"cannot hash yt-dlp runtime file {path}: {error}") from error
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise AcquisitionError(f"yt-dlp runtime file changed while hashing: {path}")

    root_after = resolved.stat()
    if (
        root_after.st_dev,
        root_after.st_ino,
        root_after.st_mtime_ns,
    ) != (
        root_before.st_dev,
        root_before.st_ino,
        root_before.st_mtime_ns,
    ):
        raise AcquisitionError("yt-dlp runtime tree root changed while hashing")
    try:
        final_files = []
        for candidate in resolved.rglob("*"):
            metadata = candidate.lstat()
            if stat.S_ISLNK(metadata.st_mode) or (
                not stat.S_ISDIR(metadata.st_mode)
                and not stat.S_ISREG(metadata.st_mode)
            ):
                raise AcquisitionError(
                    f"yt-dlp runtime tree changed type during hashing: {candidate}"
                )
            if stat.S_ISREG(metadata.st_mode):
                final_files.append(candidate.relative_to(resolved).as_posix())
    except OSError as error:
        raise AcquisitionError(
            f"cannot re-enumerate yt-dlp runtime tree: {error}"
        ) from error
    final_files.sort()
    if final_files != [path.relative_to(resolved).as_posix() for path in files]:
        raise AcquisitionError("yt-dlp runtime tree file set changed while hashing")
    return {
        "root": str(resolved),
        "sha256": digest.hexdigest(),
        "file_count": len(files),
        "byte_count": total_bytes,
    }


def atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write(path, pretty_json(value).encode("utf-8"))


def bounded_file_text(path: Path, maximum: int = MAX_SUBPROCESS_LOG_BYTES) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as handle:
        body = handle.read(maximum + 1)
    if len(body) > maximum:
        body = body[:maximum]
        suffix = b"\n[output truncated by acquisition monitor]\n"
        body = body[: max(0, maximum - len(suffix))] + suffix
    return body.decode("utf-8", errors="replace")


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise AcquisitionError(f"Invalid JSON in {path}: {error}") from error


def absolute_path(value: Any, label: str, must_exist: bool) -> Path:
    if not isinstance(value, str) or not value:
        raise AcquisitionError(f"{label} must be a non-empty path string")
    if "://" in value:
        raise AcquisitionError(f"{label} must be a local path, not a URL")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise AcquisitionError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=must_exist)
    except FileNotFoundError as error:
        raise AcquisitionError(f"{label} does not exist: {path}") from error
    if must_exist and not resolved.is_file():
        raise AcquisitionError(f"{label} must identify a regular file: {resolved}")
    return resolved


def validate_output_root(path: Path) -> None:
    if path == Path("/"):
        raise AcquisitionError("output.root may not be the filesystem root")
    if path.exists() and not path.is_dir():
        raise AcquisitionError("output.root must identify a directory or a new path")
    for forbidden in (Path("/tmp"), Path("/var/tmp")):
        if path == forbidden or forbidden in path.parents:
            raise AcquisitionError(f"output.root may not be under {forbidden}")


def require_exact_keys(value: dict[str, Any], label: str, allowed: set[str]) -> None:
    unexpected = sorted(set(value) - allowed)
    if unexpected:
        raise AcquisitionError(f"{label} has unsupported keys: {', '.join(unexpected)}")


def validate_sha256(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise AcquisitionError(f"{label} must be null or a lowercase SHA-256")
    return value


def nonnegative_integer(value: Any, label: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AcquisitionError(f"{label} must be an integer")
    minimum = 1 if positive else 0
    if value < minimum:
        raise AcquisitionError(f"{label} must be at least {minimum}")
    return value


def sanitize_url(value: Any, label: str, *, youtube_only: bool = False) -> str:
    if not isinstance(value, str) or not value:
        raise AcquisitionError(f"{label} must be a non-empty URL")
    split = urllib.parse.urlsplit(value)
    if split.scheme not in ("http", "https") or not split.hostname:
        raise AcquisitionError(f"{label} must use public HTTP or HTTPS")
    if split.username or split.password:
        raise AcquisitionError(f"{label} may not contain credentials")
    if split.fragment:
        raise AcquisitionError(f"{label} may not contain a fragment")
    for key, _ in urllib.parse.parse_qsl(split.query, keep_blank_values=True):
        if SENSITIVE_QUERY_RE.search(key):
            raise AcquisitionError(f"{label} contains a credential-like query parameter")
    host = split.hostname.lower().rstrip(".")
    if youtube_only and host not in {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "youtu.be",
    }:
        raise AcquisitionError(f"{label} must identify a public YouTube page")
    if (
        (host == "reddit.com" or host.endswith(".reddit.com") or host == "redd.it")
        and "/comments/" in split.path
    ):
        raise AcquisitionError(
            "Reddit discussion URLs are not media inputs; supply an explicit public media URL"
        )
    return urllib.parse.urlunsplit(split)


def canonical_vreddit_url(native_id: Any, label: str) -> str:
    if not isinstance(native_id, str) or not VREDDIT_ID_RE.fullmatch(native_id):
        raise AcquisitionError(f"{label} must be a stable v.redd.it media identifier")
    return f"https://v.redd.it/{native_id}"


def validate_reddit_permalink(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise AcquisitionError(f"{label} must be a non-empty Reddit permalink")
    split = urllib.parse.urlsplit(value)
    host = (split.hostname or "").lower().rstrip(".")
    if (
        split.scheme != "https"
        or host not in {"reddit.com", "www.reddit.com", "old.reddit.com"}
        or split.username
        or split.password
        or split.query
        or split.fragment
        or "/comments/" not in split.path.lower()
    ):
        raise AcquisitionError(
            f"{label} must be an exact public Reddit discussion permalink without query or fragment"
        )
    return urllib.parse.urlunsplit(split)


def validate_ytdlp_source_url(
    source: dict[str, Any], url: Any, label: str
) -> str:
    """Accept only an explicit YouTube page or an exact canonical v.redd.it ID.

    Reddit discussion pages are deliberately never upgraded into media requests.
    A Reddit media request must carry the complete reddit/reddit_video source tuple,
    and the URL is reconstructed from its stable native identifier before comparison.
    """

    sanitized = sanitize_url(url, label)
    split = urllib.parse.urlsplit(sanitized)
    host = (split.hostname or "").lower().rstrip(".")
    reddit_claim = (
        source.get("platform") == "reddit"
        or source.get("source_kind") == "reddit_video"
        or host == "v.redd.it"
    )
    if reddit_claim:
        if source.get("platform") != "reddit" or source.get("source_kind") != "reddit_video":
            raise AcquisitionError(
                "v.redd.it acquisition requires source.platform=reddit and "
                "source.source_kind=reddit_video"
            )
        expected = canonical_vreddit_url(source.get("native_id"), "source.native_id")
        if sanitized != expected:
            raise AcquisitionError(
                f"{label} must equal the exact canonical Reddit media URL {expected}"
            )
        return sanitized
    if source.get("platform") != "youtube":
        raise AcquisitionError(
            "public YouTube acquisition requires source.platform=youtube"
        )
    return sanitize_url(sanitized, label, youtube_only=True)


def optional_text(value: Any, label: str, maximum: int = 2_000) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > maximum or "\x00" in value:
        raise AcquisitionError(f"{label} must be null or a bounded text string")
    return value


def optional_timestamp(value: Any, label: str) -> str | None:
    text = optional_text(value, label, 100)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise AcquisitionError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise AcquisitionError(f"{label} must include a UTC offset")
    normalized = parsed.astimezone(timezone.utc)
    return normalized.isoformat(
        timespec="microseconds" if normalized.microsecond else "seconds"
    ).replace("+00:00", "Z")


def validate_source(raw: Any, adapter: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise AcquisitionError("source must be a JSON object")
    allowed = {
        "platform",
        "source_kind",
        "native_id",
        "canonical_url",
        "title",
        "published_at",
        "access_state",
    }
    require_exact_keys(raw, "source", allowed)
    for key in ("platform", "source_kind", "native_id"):
        if not isinstance(raw.get(key), str) or not raw[key] or len(raw[key]) > 500:
            raise AcquisitionError(f"source.{key} must be a non-empty bounded string")
    access_state = raw.get("access_state")
    if access_state not in {
        "public",
        "members_only",
        "private",
        "removed",
        "unavailable",
        "unknown",
    }:
        raise AcquisitionError("source.access_state is invalid")
    if adapter in ("direct_http", "yt_dlp") and access_state != "public":
        raise AcquisitionError(f"{adapter} accepts only sources declared public")
    if adapter == "local_file" and access_state != "unknown":
        raise AcquisitionError(
            "local_file source.access_state must be unknown; local possession does "
            "not independently establish public access"
        )
    canonical_url = raw.get("canonical_url")
    if canonical_url is not None:
        canonical_url = (
            validate_ytdlp_source_url(raw, canonical_url, "source.canonical_url")
            if adapter == "yt_dlp"
            else sanitize_url(canonical_url, "source.canonical_url")
        )
    return {
        "platform": raw["platform"],
        "source_kind": raw["source_kind"],
        "native_id": raw["native_id"],
        "canonical_url": canonical_url,
        "title": optional_text(raw.get("title"), "source.title"),
        "published_at": optional_timestamp(raw.get("published_at"), "source.published_at"),
        "access_state": access_state,
    }


def validate_handling_policy(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise AcquisitionError("handling_policy must be a JSON object")
    require_exact_keys(
        raw,
        "handling_policy",
        {
            "storage_scope",
            "publication_disposition",
            "publication_authority",
            "basis",
        },
    )
    if raw.get("storage_scope") != "private_canonical_cache":
        raise AcquisitionError(
            "handling_policy.storage_scope must be private_canonical_cache"
        )
    if raw.get("publication_disposition") not in {
        "no_publication_authority",
        "never_publish",
    }:
        raise AcquisitionError("handling_policy.publication_disposition is invalid")
    if raw.get("publication_authority") != "none":
        raise AcquisitionError("handling_policy.publication_authority must be none")
    basis = raw.get("basis")
    if (
        not isinstance(basis, str)
        or not basis.strip()
        or len(basis) > 1_000
        or "\x00" in basis
    ):
        raise AcquisitionError(
            "handling_policy.basis must be non-empty bounded text"
        )
    return {
        "storage_scope": "private_canonical_cache",
        "publication_disposition": raw["publication_disposition"],
        "publication_authority": "none",
        "basis": basis,
    }


def validate_adapter_config(adapter: str, raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise AcquisitionError("adapter_config must be a JSON object")
    common = {"expected_sha256", "expected_byte_count"}
    if adapter == "local_file":
        require_exact_keys(raw, "adapter_config", common | {"path"})
        result = {"path": str(absolute_path(raw.get("path"), "adapter_config.path", True))}
    elif adapter == "direct_http":
        require_exact_keys(
            raw,
            "adapter_config",
            common | {"url", "resume", "timeout_seconds"},
        )
        if not isinstance(raw.get("resume"), bool):
            raise AcquisitionError("adapter_config.resume must be boolean")
        timeout = nonnegative_integer(
            raw.get("timeout_seconds"), "adapter_config.timeout_seconds", positive=True
        )
        if timeout > 3_600:
            raise AcquisitionError("adapter_config.timeout_seconds may not exceed 3600")
        result = {
            "url": sanitize_url(raw.get("url"), "adapter_config.url"),
            "resume": raw["resume"],
            "timeout_seconds": timeout,
        }
    else:
        require_exact_keys(
            raw,
            "adapter_config",
            common
            | {
                "url",
                "executable",
                "format_selector",
                "expected_executable_sha256",
                "expected_ytdlp_version",
                "expected_runtime_tree_root",
                "expected_runtime_tree_sha256",
                "expected_webpage_url",
            },
        )
        executable = absolute_path(
            raw.get("executable"), "adapter_config.executable", must_exist=True
        )
        if not os.access(executable, os.X_OK):
            raise AcquisitionError("adapter_config.executable is not executable")
        format_selector = raw.get("format_selector")
        if not safe_ytdlp_format_selector(format_selector):
            raise AcquisitionError(
                "adapter_config.format_selector is outside the fixed safe selector policy"
            )
        result = {
            "url": sanitize_url(raw.get("url"), "adapter_config.url"),
            "executable": str(executable),
            "format_selector": format_selector,
        }
        # Optional for backward compatibility with already-issued version-1 orders.
        # Queue materialization always supplies it; an older order that omitted it
        # retains its original canonical work-order digest.
        if "expected_executable_sha256" in raw:
            result["expected_executable_sha256"] = validate_sha256(
                raw.get("expected_executable_sha256"),
                "adapter_config.expected_executable_sha256",
            )
        if "expected_ytdlp_version" in raw:
            result["expected_ytdlp_version"] = validated_ytdlp_version(
                raw.get("expected_ytdlp_version"),
                "adapter_config.expected_ytdlp_version",
            )
        runtime_root = raw.get("expected_runtime_tree_root")
        runtime_sha256 = raw.get("expected_runtime_tree_sha256")
        if (runtime_root is None) != (runtime_sha256 is None):
            raise AcquisitionError(
                "adapter_config expected runtime-tree root and SHA-256 must be supplied together"
            )
        if runtime_root is not None:
            if not isinstance(runtime_root, str) or not runtime_root:
                raise AcquisitionError(
                    "adapter_config.expected_runtime_tree_root must be an absolute directory"
                )
            root_path = Path(runtime_root)
            if not root_path.is_absolute() or root_path.is_symlink():
                raise AcquisitionError(
                    "adapter_config.expected_runtime_tree_root must be a non-symlink absolute directory"
                )
            try:
                root_path = root_path.resolve(strict=True)
            except OSError as error:
                raise AcquisitionError(
                    "adapter_config.expected_runtime_tree_root does not exist"
                ) from error
            if not root_path.is_dir():
                raise AcquisitionError(
                    "adapter_config.expected_runtime_tree_root must be a directory"
                )
            result["expected_runtime_tree_root"] = str(root_path)
            result["expected_runtime_tree_sha256"] = validate_sha256(
                runtime_sha256,
                "adapter_config.expected_runtime_tree_sha256",
            )
        if "expected_webpage_url" in raw:
            result["expected_webpage_url"] = validate_reddit_permalink(
                raw.get("expected_webpage_url"),
                "adapter_config.expected_webpage_url",
            )
    result["expected_sha256"] = validate_sha256(
        raw.get("expected_sha256"), "adapter_config.expected_sha256"
    )
    expected_bytes = raw.get("expected_byte_count")
    result["expected_byte_count"] = (
        None
        if expected_bytes is None
        else nonnegative_integer(
            expected_bytes, "adapter_config.expected_byte_count", positive=True
        )
    )
    return result


def validate_limits(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise AcquisitionError("limits must be a JSON object")
    keys = {"max_job_bytes", "global_cache_cap_bytes", "free_space_floor_bytes"}
    require_exact_keys(raw, "limits", keys)
    result = {
        key: nonnegative_integer(raw.get(key), f"limits.{key}", positive=key != "free_space_floor_bytes")
        for key in keys
    }
    if result["max_job_bytes"] > result["global_cache_cap_bytes"]:
        raise AcquisitionError("limits.max_job_bytes may not exceed the global cache cap")
    return result


def validate_work_order(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise AcquisitionError("work order must be a JSON object")
    allowed = {
        "schema_version",
        "job_id",
        "adapter",
        "source",
        "adapter_config",
        "output",
        "limits",
        "handling_policy",
    }
    require_exact_keys(raw, "work order", allowed)
    if raw.get("schema_version") != CONTRACT_VERSION:
        raise AcquisitionError(f"schema_version must be {CONTRACT_VERSION}")
    job_id = raw.get("job_id")
    if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
        raise AcquisitionError("job_id contains unsupported characters or is too long")
    adapter = raw.get("adapter")
    if adapter not in ADAPTERS:
        raise AcquisitionError(f"adapter must be one of: {', '.join(ADAPTERS)}")
    output = raw.get("output")
    if not isinstance(output, dict):
        raise AcquisitionError("output must be a JSON object")
    require_exact_keys(output, "output", {"root"})
    output_root = absolute_path(output.get("root"), "output.root", must_exist=False)
    validate_output_root(output_root)
    source = validate_source(raw.get("source"), adapter)
    adapter_config = validate_adapter_config(adapter, raw.get("adapter_config"))
    if adapter == "local_file":
        source_path = Path(adapter_config["path"])
        if source_path == output_root or output_root in source_path.parents:
            raise AcquisitionError("local source may not be inside the managed output root")
    else:
        if adapter == "direct_http":
            direct_host = (
                urllib.parse.urlsplit(adapter_config["url"]).hostname or ""
            ).lower().rstrip(".")
            if (
                direct_host == "v.redd.it"
                or source["platform"] == "reddit"
                or source["source_kind"] == "reddit_video"
            ):
                raise AcquisitionError(
                    "canonical v.redd.it media requires the hash-pinned yt_dlp adapter"
                )
        if adapter == "yt_dlp":
            adapter_config["url"] = validate_ytdlp_source_url(
                source, adapter_config["url"], "adapter_config.url"
            )
            is_reddit_video = (
                source["platform"] == "reddit"
                and source["source_kind"] == "reddit_video"
            )
            if is_reddit_video:
                if source["canonical_url"] is None:
                    raise AcquisitionError(
                        "Reddit video work orders require an explicit canonical source URL"
                    )
                if adapter_config.get("expected_executable_sha256") is None:
                    raise AcquisitionError(
                        "Reddit video acquisition requires a hash-pinned yt-dlp executable"
                    )
                if adapter_config.get("expected_ytdlp_version") is None:
                    raise AcquisitionError(
                        "Reddit video acquisition requires an exact expected yt-dlp version"
                    )
                if executable_is_script(Path(adapter_config["executable"])) and (
                    adapter_config.get("expected_runtime_tree_root") is None
                    or adapter_config.get("expected_runtime_tree_sha256") is None
                ):
                    raise AcquisitionError(
                        "Reddit video acquisition through a script launcher requires a "
                        "hash-pinned runtime module tree"
                    )
                if adapter_config.get("expected_webpage_url") is None:
                    raise AcquisitionError(
                        "Reddit video acquisition requires the sealed expected Reddit permalink"
                    )
            elif "expected_webpage_url" in adapter_config:
                raise AcquisitionError(
                    "adapter_config.expected_webpage_url is only valid for Reddit video"
                )
        if source["canonical_url"] and source["canonical_url"] != adapter_config["url"]:
            raise AcquisitionError("source.canonical_url must match adapter_config.url")
        source["canonical_url"] = adapter_config["url"]
    normalized = {
        "schema_version": CONTRACT_VERSION,
        "job_id": job_id,
        "adapter": adapter,
        "source": source,
        "adapter_config": adapter_config,
        "output": {"root": str(output_root)},
        "limits": validate_limits(raw.get("limits")),
    }
    if "handling_policy" in raw:
        if adapter != "local_file":
            raise AcquisitionError(
                "handling_policy is supported only for local_file acquisition"
            )
        normalized["handling_policy"] = validate_handling_policy(
            raw["handling_policy"]
        )
    return normalized


def file_stat(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "byte_count": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def nearest_existing_directory(path: Path) -> Path:
    current = path
    while not current.exists():
        if current.parent == current:
            raise AcquisitionError(f"no existing parent for output root: {path}")
        current = current.parent
    if current.is_file():
        current = current.parent
    return current


def tree_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, directories, filenames in os.walk(path, followlinks=False):
        directories[:] = [
            name for name in directories if not Path(root, name).is_symlink()
        ]
        for name in filenames:
            candidate = Path(root, name)
            try:
                if not candidate.is_symlink():
                    total += candidate.stat().st_size
            except FileNotFoundError:
                continue
    return total


def acquire_writer_lock(
    output_root: Path, *, job_id: str, work_order_sha256: str
) -> tuple[Any, dict[str, Any]]:
    """Acquire the one-writer lock for a managed output root without waiting."""

    output_root.mkdir(parents=True, exist_ok=True)
    lock_path = output_root / LOCK_FILENAME
    flags = os.O_RDWR | os.O_CREAT
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        if error.errno in (errno.ELOOP, errno.EINVAL):
            raise AcquisitionError(
                f"writer lock path is unsafe or unsupported: {lock_path}"
            ) from error
        raise
    handle = os.fdopen(descriptor, "r+", encoding="utf-8", errors="replace")
    try:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise AcquisitionError(f"writer lock is not a regular file: {lock_path}")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.seek(0)
            holder = handle.read(4_096).strip()
            detail = f"; holder metadata: {holder}" if holder else ""
            raise AcquisitionError(
                f"output root is locked by another acquisition writer: {output_root}{detail}"
            ) from error

        handle.seek(0)
        previous_text = handle.read(4_096)
        previous: dict[str, Any] = {}
        if previous_text.strip():
            try:
                parsed = json.loads(previous_text)
                if isinstance(parsed, dict):
                    previous = parsed
            except json.JSONDecodeError:
                previous = {"state": "unparseable"}
        metadata = {
            "schema_version": CONTRACT_VERSION,
            "state": "locked",
            "pid": os.getpid(),
            "job_id": job_id,
            "work_order_sha256": work_order_sha256,
            "acquired_at": utc_now(),
            "recovered_stale_state": previous.get("state") == "locked",
        }
        handle.seek(0)
        handle.truncate()
        handle.write(pretty_json(metadata))
        handle.flush()
        os.fsync(handle.fileno())
        return handle, metadata
    except Exception:
        handle.close()
        raise


def release_writer_lock(handle: Any, metadata: dict[str, Any]) -> None:
    """Mark and release a lock; a crashed process is released automatically by the OS."""

    try:
        released = {
            **metadata,
            "state": "released",
            "released_at": utc_now(),
        }
        handle.seek(0)
        handle.truncate()
        handle.write(pretty_json(released))
        handle.flush()
        os.fsync(handle.fileno())
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def capacity_snapshot(
    output_root: Path,
    limits: dict[str, int],
    *,
    reserve_bytes: int,
    enforce: bool,
) -> dict[str, Any]:
    parent = nearest_existing_directory(output_root)
    disk = shutil.disk_usage(parent)
    managed = tree_size(output_root)
    projected_managed = managed + reserve_bytes
    projected_free = disk.free - reserve_bytes
    snapshot = {
        "filesystem_path": str(parent),
        "managed_bytes": managed,
        "free_bytes": disk.free,
        "reserve_bytes": reserve_bytes,
        "projected_managed_bytes": projected_managed,
        "projected_free_bytes": projected_free,
        "global_cache_cap_bytes": limits["global_cache_cap_bytes"],
        "free_space_floor_bytes": limits["free_space_floor_bytes"],
    }
    if enforce and projected_managed > limits["global_cache_cap_bytes"]:
        raise AcquisitionError(
            "the job reservation would exceed limits.global_cache_cap_bytes"
        )
    if enforce and projected_free < limits["free_space_floor_bytes"]:
        raise AcquisitionError(
            "the job reservation would cross limits.free_space_floor_bytes"
        )
    return snapshot


def ffprobe_version(ffprobe: str) -> str:
    completed = run_process([ffprobe, "-version"])
    return completed.stdout.splitlines()[0].strip()


def run_process(
    command: list[str], *, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=environment,
        check=False,
    )
    if completed.returncode != 0:
        raise CommandError(command, completed.returncode, completed.stderr)
    return completed


def require_ffprobe() -> str:
    found = shutil.which("ffprobe")
    if not found:
        raise AcquisitionError("ffprobe is required on PATH")
    return str(Path(found).resolve())


def optional_number(value: Any) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None


def milliseconds(value: Any) -> int | None:
    parsed = optional_number(value)
    return None if parsed is None else round(parsed * 1_000)


def normalize_probe(raw: dict[str, Any], version: str) -> dict[str, Any]:
    streams = []
    for stream in sorted(raw.get("streams") or [], key=lambda item: item.get("index", 0)):
        if not isinstance(stream, dict):
            continue
        item: dict[str, Any] = {
            "index": stream.get("index"),
            "codec_type": stream.get("codec_type"),
            "codec_name": stream.get("codec_name"),
            "duration_ms": milliseconds(stream.get("duration")),
            "bit_rate_bps": int(float(stream["bit_rate"]))
            if optional_number(stream.get("bit_rate")) is not None
            else None,
            "language": (stream.get("tags") or {}).get("language"),
        }
        if stream.get("codec_type") == "video":
            item.update(
                {
                    "width": stream.get("width"),
                    "height": stream.get("height"),
                    "pixel_format": stream.get("pix_fmt"),
                    "average_frame_rate": stream.get("avg_frame_rate"),
                }
            )
        elif stream.get("codec_type") == "audio":
            item.update(
                {
                    "sample_rate_hz": int(stream["sample_rate"])
                    if str(stream.get("sample_rate", "")).isdigit()
                    else None,
                    "channels": stream.get("channels"),
                    "channel_layout": stream.get("channel_layout"),
                }
            )
        streams.append(item)
    raw_format = raw.get("format") if isinstance(raw.get("format"), dict) else {}
    duration_ms = milliseconds(raw_format.get("duration"))
    if duration_ms is None:
        duration_ms = max(
            (item["duration_ms"] for item in streams if item["duration_ms"] is not None),
            default=None,
        )
    return {
        "schema_version": CONTRACT_VERSION,
        "tool": {"name": "ffprobe", "version": version},
        "format": {
            "format_name": raw_format.get("format_name"),
            "format_long_name": raw_format.get("format_long_name"),
            "duration_ms": duration_ms,
            "bit_rate_bps": int(float(raw_format["bit_rate"]))
            if optional_number(raw_format.get("bit_rate")) is not None
            else None,
        },
        "streams": streams,
    }


def probe_file(path: Path, ffprobe: str, version: str) -> tuple[dict[str, Any], list[str]]:
    command = [
        ffprobe,
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-of",
        "json",
        str(path),
    ]
    completed = run_process(command)
    try:
        raw = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise AcquisitionError(f"ffprobe returned invalid JSON for {path}") from error
    return normalize_probe(raw, version), command


def media_kind(probe: dict[str, Any]) -> str:
    types = {stream.get("codec_type") for stream in probe["streams"]}
    if "video" in types:
        return "video"
    if "audio" in types:
        return "audio"
    return "other"


def copy_local_source(
    source: Path, staged: Path, max_bytes: int
) -> tuple[str, dict[str, Any], list[str]]:
    before = file_stat(source)
    if before["byte_count"] > max_bytes:
        raise AcquisitionError("local source exceeds limits.max_job_bytes")
    staged.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    written = 0
    with source.open("rb") as input_handle, staged.open("wb") as output_handle:
        while chunk := input_handle.read(CHUNK_SIZE):
            written += len(chunk)
            if written > max_bytes:
                raise AcquisitionError("local source exceeded limits.max_job_bytes while copying")
            digest.update(chunk)
            output_handle.write(chunk)
        output_handle.flush()
        os.fsync(output_handle.fileno())
    after = file_stat(source)
    if after != before:
        raise AcquisitionError("local source metadata changed during admission staging")
    return (
        digest.hexdigest(),
        {"local_source": {"stat_before": before, "stat_after": after, "unchanged": True}},
        ["local-copy", str(source), str(staged)],
    )


def safe_response_metadata(response: Any) -> dict[str, Any]:
    headers = response.headers
    return {
        "status": response.getcode(),
        "final_url": sanitize_url(response.geturl(), "HTTP final URL"),
        "content_type": headers.get("Content-Type"),
        "content_length": int(headers["Content-Length"])
        if str(headers.get("Content-Length", "")).isdigit()
        else None,
        "content_range": headers.get("Content-Range"),
        "accept_ranges": headers.get("Accept-Ranges"),
        "etag": headers.get("ETag"),
        "last_modified": headers.get("Last-Modified"),
    }


def parse_content_range(value: str | None) -> tuple[int, int, int | None] | None:
    if not value:
        return None
    match = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", value.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2)), (
        None if match.group(3) == "*" else int(match.group(3))
    )


def http_opener() -> urllib.request.OpenerDirector:
    # Deliberately do not inherit authenticated proxy settings or a CookieJar.
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPRedirectHandler()
    )


def download_http(
    *,
    url: str,
    staged: Path,
    sidecar: Path,
    max_bytes: int,
    timeout: int,
    resume: bool,
    retry_without_range: bool = True,
) -> tuple[dict[str, Any], list[str]]:
    staged.parent.mkdir(parents=True, exist_ok=True)
    partial_size = staged.stat().st_size if staged.exists() and resume else 0
    prior: dict[str, Any] = {}
    if sidecar.exists() and partial_size:
        try:
            loaded = load_json(sidecar)
            if isinstance(loaded, dict) and loaded.get("url") == url:
                prior = loaded
            else:
                partial_size = 0
        except AcquisitionError:
            partial_size = 0
    if partial_size > max_bytes:
        raise AcquisitionError("resumable partial exceeds limits.max_job_bytes")
    headers = {
        "User-Agent": f"HIMR-Corpus-Acquisition/{IMPLEMENTATION_VERSION}",
        "Accept-Encoding": "identity",
    }
    if partial_size:
        headers["Range"] = f"bytes={partial_size}-"
        validator = prior.get("etag") or prior.get("last_modified")
        if validator:
            headers["If-Range"] = validator
    request = urllib.request.Request(url, headers=headers, method="GET")
    pseudo_command = [
        "http-get",
        url,
        "--resume" if resume else "--no-resume",
        "--max-bytes",
        str(max_bytes),
        "--output",
        str(staged),
    ]
    try:
        response_context = http_opener().open(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        if error.code == 416 and partial_size and retry_without_range:
            expected = prior.get("expected_total_bytes")
            if expected == partial_size:
                return prior.get("response_metadata", {}), pseudo_command
            staged.unlink(missing_ok=True)
            sidecar.unlink(missing_ok=True)
            return download_http(
                url=url,
                staged=staged,
                sidecar=sidecar,
                max_bytes=max_bytes,
                timeout=timeout,
                resume=False,
                retry_without_range=False,
            )
        raise AcquisitionError(f"HTTP request failed with status {error.code}") from error
    except urllib.error.URLError as error:
        raise AcquisitionError(f"HTTP request failed: {error.reason}") from error

    with response_context as response:
        metadata = safe_response_metadata(response)
        status = metadata["status"]
        range_info = parse_content_range(metadata["content_range"])
        append = False
        expected_total: int | None = None
        if partial_size and status == 206:
            if not range_info or range_info[0] != partial_size:
                raise AcquisitionError("server returned an unsafe or mismatched resume range")
            append = True
            expected_total = range_info[2]
        elif partial_size and status == 200:
            partial_size = 0
            expected_total = metadata["content_length"]
        elif status == 206:
            if not range_info or range_info[0] != 0:
                raise AcquisitionError("server returned an unexpected partial response")
            expected_total = range_info[2]
        elif status == 200:
            expected_total = metadata["content_length"]
        else:
            raise AcquisitionError(f"unexpected HTTP status: {status}")
        if expected_total is not None and expected_total > max_bytes:
            raise AcquisitionError("HTTP Content-Length exceeds limits.max_job_bytes")
        resume_record = {
            "schema_version": CONTRACT_VERSION,
            "url": url,
            "etag": metadata.get("etag"),
            "last_modified": metadata.get("last_modified"),
            "expected_total_bytes": expected_total,
            "response_metadata": metadata,
        }
        atomic_write_json(sidecar, resume_record)
        mode = "ab" if append else "wb"
        written = partial_size
        with staged.open(mode) as handle:
            try:
                while chunk := response.read(CHUNK_SIZE):
                    written += len(chunk)
                    if written > max_bytes:
                        raise AcquisitionError("HTTP body exceeded limits.max_job_bytes")
                    handle.write(chunk)
            except http.client.IncompleteRead as error:
                if error.partial:
                    written += len(error.partial)
                    if written > max_bytes:
                        raise AcquisitionError(
                            "HTTP body exceeded limits.max_job_bytes"
                        ) from error
                    handle.write(error.partial)
                handle.flush()
                os.fsync(handle.fileno())
                raise AcquisitionError(
                    f"incomplete HTTP body: expected {expected_total} bytes, "
                    f"staged {written}"
                ) from error
            except (http.client.HTTPException, OSError) as error:
                handle.flush()
                os.fsync(handle.fileno())
                raise AcquisitionError(f"HTTP body transfer failed: {error}") from error
            handle.flush()
            os.fsync(handle.fileno())
        if expected_total is not None and written != expected_total:
            raise AcquisitionError(
                f"incomplete HTTP body: expected {expected_total} bytes, staged {written}"
            )
        metadata["resumed_from_bytes"] = partial_size if append else 0
        metadata["staged_byte_count"] = written
        return metadata, pseudo_command


def minimal_subprocess_environment(home: Path) -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_CACHE_HOME": str(home / ".cache"),
        "LC_ALL": "C",
        "LANG": "C",
        "TZ": "UTC",
    }


def live_capacity_violation(
    *,
    output_root: Path,
    stage_dir: Path,
    baseline_managed_bytes: int,
    limits: dict[str, int],
) -> str | None:
    stage_bytes = tree_size(stage_dir)
    if stage_bytes > limits["max_job_bytes"]:
        return (
            "yt-dlp staging exceeded limits.max_job_bytes: "
            f"{stage_bytes} > {limits['max_job_bytes']}"
        )
    projected_managed = baseline_managed_bytes + stage_bytes
    if projected_managed > limits["global_cache_cap_bytes"]:
        return (
            "yt-dlp staging exceeded limits.global_cache_cap_bytes: "
            f"{projected_managed} > {limits['global_cache_cap_bytes']}"
        )
    free_bytes = shutil.disk_usage(nearest_existing_directory(output_root)).free
    if free_bytes < limits["free_space_floor_bytes"]:
        return (
            "yt-dlp staging crossed limits.free_space_floor_bytes: "
            f"{free_bytes} < {limits['free_space_floor_bytes']}"
        )
    for name in (".yt-dlp.stdout", ".yt-dlp.stderr"):
        candidate = stage_dir / name
        if candidate.exists() and candidate.stat().st_size > MAX_SUBPROCESS_LOG_BYTES:
            return (
                f"yt-dlp {name[1:]} exceeded the {MAX_SUBPROCESS_LOG_BYTES}-byte "
                "diagnostic-output limit"
            )
    return None


def terminate_process_group(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired as error:
        raise AcquisitionError("could not terminate the yt-dlp process group") from error


def run_ytdlp_monitored(
    command: list[str],
    *,
    environment: dict[str, str],
    output_root: Path,
    stage_dir: Path,
    limits: dict[str, int],
) -> subprocess.CompletedProcess[str]:
    """Run yt-dlp while bounding its whole staging tree and diagnostic output."""

    stdout_path = stage_dir / ".yt-dlp.stdout"
    stderr_path = stage_dir / ".yt-dlp.stderr"
    stage_bytes_before = tree_size(stage_dir)
    baseline_managed = max(0, tree_size(output_root) - stage_bytes_before)
    violation: str | None = None
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout_handle,
            stderr=stderr_handle,
            env=environment,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                violation = live_capacity_violation(
                    output_root=output_root,
                    stage_dir=stage_dir,
                    baseline_managed_bytes=baseline_managed,
                    limits=limits,
                )
                if violation:
                    terminate_process_group(process)
                    break
                time.sleep(MONITOR_INTERVAL_SECONDS)
            if violation is None:
                violation = live_capacity_violation(
                    output_root=output_root,
                    stage_dir=stage_dir,
                    baseline_managed_bytes=baseline_managed,
                    limits=limits,
                )
                if violation:
                    terminate_process_group(process)
        except BaseException:
            terminate_process_group(process)
            raise
        returncode = process.wait()

    stdout = bounded_file_text(stdout_path)
    stderr = bounded_file_text(stderr_path)
    stdout_path.unlink(missing_ok=True)
    stderr_path.unlink(missing_ok=True)
    if violation:
        raise LiveCapacityError(violation)
    if returncode != 0:
        raise CommandError(command, returncode, stderr)
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


def safe_ytdlp_metadata(
    raw: dict[str, Any], *, expected_reddit_webpage_url: str | None = None
) -> dict[str, Any]:
    def finite_number(value: Any) -> bool:
        return (
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and (isinstance(value, int) or math.isfinite(value))
            and abs(value) <= MAX_YTDLP_METADATA_NUMBER
        )

    def normalized_nonnegative_integer(value: Any) -> int | None:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or value < 0
            or value > MAX_YTDLP_METADATA_NUMBER
        ):
            return None
        if isinstance(value, int):
            return value
        return int(value) if math.isfinite(value) and value.is_integer() else None

    string_fields = {
        "id",
        "title",
        "channel_id",
        "channel",
        "uploader_id",
        "upload_date",
        "webpage_url",
        "original_url",
        "extractor",
        "extractor_key",
        "ext",
        "format_id",
        "vcodec",
        "acodec",
        "availability",
        "live_status",
    }
    nonnegative_number_fields = {"duration", "fps"}
    nonnegative_integer_fields = {"width", "height", "filesize", "filesize_approx"}
    selected: dict[str, Any] = {}
    for key in SAFE_YTDLP_METADATA_FIELDS:
        if key not in raw:
            continue
        value = raw[key]
        if value is None:
            selected[key] = None
        elif key in string_fields:
            selected[key] = (
                value
                if isinstance(value, str)
                and len(value) <= MAX_YTDLP_METADATA_STRING_CHARS
                else None
            )
        elif key == "timestamp":
            selected[key] = value if finite_number(value) else None
        elif key in nonnegative_number_fields:
            selected[key] = (
                value if finite_number(value) and value >= 0 else None
            )
        elif key in nonnegative_integer_fields:
            selected[key] = normalized_nonnegative_integer(value)
    for key in ("webpage_url", "original_url"):
        if selected.get(key):
            try:
                if key == "webpage_url" and expected_reddit_webpage_url is not None:
                    candidate = validate_reddit_permalink(
                        selected[key], "yt-dlp metadata webpage_url"
                    )
                    if candidate != expected_reddit_webpage_url:
                        raise AcquisitionError(
                            "yt-dlp webpage identity differs from the sealed Reddit permalink"
                        )
                    selected[key] = candidate
                else:
                    selected[key] = sanitize_url(
                        selected[key], f"yt-dlp metadata {key}"
                    )
            except AcquisitionError:
                selected[key] = None
    return selected


def validate_ytdlp_selected_identity(
    source: dict[str, Any],
    selected: dict[str, Any],
    expected_webpage_url: str | None,
) -> None:
    """Fail closed when extraction metadata no longer identifies the request.

    YouTube must retain the exact requested native ID. yt-dlp may contact Reddit
    media-delivery hosts internally, but a Reddit extraction's original URL must
    remain the exact canonical v.redd.it request and its webpage URL must remain the
    exact sealed Reddit post permalink. An unrelated redirect therefore cannot be
    admitted merely because it happened to yield one playable file.
    """

    if source["platform"] == "youtube":
        if selected.get("id") != source["native_id"]:
            raise AcquisitionError(
                "yt-dlp metadata ID does not match the requested YouTube source"
            )
        return
    if not (
        source["platform"] == "reddit"
        and source["source_kind"] == "reddit_video"
    ):
        return
    canonical = canonical_vreddit_url(source["native_id"], "source.native_id")
    if selected.get("original_url") != canonical:
        raise AcquisitionError(
            "yt-dlp selected metadata does not retain the exact requested v.redd.it URL"
        )
    if expected_webpage_url is None:
        raise AcquisitionError("Reddit work order has no expected webpage permalink")
    if selected.get("webpage_url") != expected_webpage_url:
        raise AcquisitionError(
            "yt-dlp selected webpage identity differs from the sealed Reddit permalink"
        )
    selected_id = selected.get("id")
    if selected_id is not None and selected_id != source["native_id"]:
        raise AcquisitionError(
            "yt-dlp metadata ID does not match the requested v.redd.it media ID"
        )


def validate_reddit_delivery_url(value: Any, native_id: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise AcquisitionError(f"{label} has no public media-delivery URL")
    split = urllib.parse.urlsplit(value)
    try:
        port = split.port
    except ValueError as error:
        raise AcquisitionError(f"{label} has an invalid port") from error
    if (
        split.scheme != "https"
        or (split.hostname or "").lower().rstrip(".") != "v.redd.it"
        or split.username
        or split.password
        or port is not None
        or split.fragment
        or not split.path.startswith(f"/{native_id}/")
    ):
        raise AcquisitionError(
            f"{label} must stay under https://v.redd.it/{native_id}/"
        )
    return urllib.parse.urlunsplit(split)


def validate_reddit_selected_delivery(raw: dict[str, Any], native_id: str) -> None:
    """Bind selected media and manifest URLs to the requested v.redd.it path."""

    formats: list[dict[str, Any]] = []
    if isinstance(raw.get("requested_formats"), list):
        formats.extend(
            value for value in raw["requested_formats"] if isinstance(value, dict)
        )
    if isinstance(raw.get("requested_downloads"), list):
        for download in raw["requested_downloads"]:
            if not isinstance(download, dict):
                continue
            if isinstance(download.get("url"), str):
                formats.append(download)
            if isinstance(download.get("requested_formats"), list):
                formats.extend(
                    value
                    for value in download["requested_formats"]
                    if isinstance(value, dict)
                )
    if isinstance(raw.get("url"), str):
        formats.append({"url": raw["url"], "manifest_url": raw.get("manifest_url")})

    observed = 0
    for index, selected_format in enumerate(formats):
        for field in ("url", "manifest_url"):
            value = selected_format.get(field)
            if value is None:
                continue
            validate_reddit_delivery_url(
                value, native_id, f"yt-dlp selected format {index} {field}"
            )
            observed += 1
    if observed == 0:
        raise AcquisitionError(
            "yt-dlp metadata has no selected v.redd.it media-delivery identity"
        )


def validate_ytdlp_raw_identity(
    source: dict[str, Any], raw: dict[str, Any], expected_webpage_url: str | None
) -> None:
    """Validate provider identity before unsafe metadata URLs can be minimized away."""

    if not (
        source["platform"] == "reddit"
        and source["source_kind"] == "reddit_video"
    ):
        return
    canonical = canonical_vreddit_url(source["native_id"], "source.native_id")
    original_url = raw.get("original_url")
    if not isinstance(original_url, str) or original_url != canonical:
        raise AcquisitionError(
            "yt-dlp metadata does not identify the exact requested v.redd.it URL"
        )
    if expected_webpage_url is None:
        raise AcquisitionError("Reddit work order has no expected webpage permalink")
    webpage_url = raw.get("webpage_url")
    if not isinstance(webpage_url, str):
        raise AcquisitionError("yt-dlp metadata has no Reddit webpage identity")
    validated_webpage = validate_reddit_permalink(
        webpage_url, "yt-dlp metadata webpage_url"
    )
    if validated_webpage != expected_webpage_url:
        raise AcquisitionError(
            "yt-dlp reported a Reddit discussion or unrelated redirect outside the sealed post provenance"
        )
    if raw.get("id") != source["native_id"]:
        raise AcquisitionError(
            "yt-dlp metadata ID does not match the requested v.redd.it media ID"
        )
    validate_reddit_selected_delivery(raw, source["native_id"])


def ytdlp_version(executable: Path, environment: dict[str, str]) -> str:
    completed = run_process([str(executable), "--version"], environment=environment)
    return completed.stdout.splitlines()[0].strip() if completed.stdout else "unknown"


def verify_ytdlp_runtime_tree(config: dict[str, Any], phase: str) -> dict[str, Any] | None:
    root = config.get("expected_runtime_tree_root")
    expected = config.get("expected_runtime_tree_sha256")
    if root is None and expected is None:
        return None
    if root is None or expected is None:
        raise AcquisitionError("yt-dlp runtime-tree integrity configuration is incomplete")
    observed = runtime_tree_fingerprint(Path(root))
    if observed["sha256"] != expected:
        raise AcquisitionError(
            f"yt-dlp runtime module tree SHA-256 mismatch {phase}: "
            f"expected {expected}, observed {observed['sha256']}"
        )
    return observed


def ytdlp_download_command(
    *,
    executable: Path,
    config: dict[str, Any],
    source: dict[str, Any],
    output_template: Path,
    max_bytes: int,
) -> list[str]:
    command = [
        str(executable),
        "--ignore-config",
        "--no-playlist",
        "--continue",
        "--no-progress",
        "--no-write-comments",
        "--no-write-subs",
        "--no-write-auto-subs",
        "--no-write-thumbnail",
        "--no-write-info-json",
    ]
    if source["platform"] == "youtube":
        # Full yt-dlp info JSON can contain one entry per media fragment and exceed
        # the bounded diagnostic channel for long livestreams. Ask yt-dlp to emit
        # only allowlisted fields after the final file is moved. `--no-simulate` is
        # explicit because --print otherwise implies simulation.
        command.extend(
            ["--no-simulate", "--print", YOUTUBE_YTDLP_PRINT_TEMPLATE]
        )
    else:
        # Reddit provenance validation additionally needs selected delivery URLs,
        # which are deliberately absent from the public-YouTube metadata template.
        command.append("--print-json")
    command.extend(
        [
            "--max-filesize",
            str(max_bytes),
            "--format",
            config["format_selector"],
            "--output",
            str(output_template),
            config["url"],
        ]
    )
    return command


def download_ytdlp(
    *,
    config: dict[str, Any],
    source: dict[str, Any],
    stage_dir: Path,
    output_root: Path,
    limits: dict[str, int],
    max_bytes: int,
) -> tuple[Path, dict[str, Any], list[str], dict[str, Any]]:
    executable = Path(config["executable"])
    executable_before = file_stat(executable)
    executable_sha256 = sha256_file(executable)
    expected_executable_sha256 = config.get("expected_executable_sha256")
    if (
        expected_executable_sha256 is not None
        and executable_sha256 != expected_executable_sha256
    ):
        raise AcquisitionError(
            "yt-dlp executable SHA-256 does not match "
            "adapter_config.expected_executable_sha256"
        )
    environment = minimal_subprocess_environment(stage_dir / "home")
    runtime_tree_before = verify_ytdlp_runtime_tree(config, "before version check")
    version = ytdlp_version(executable, environment)
    expected_version = config.get("expected_ytdlp_version")
    if expected_version is not None and version != expected_version:
        raise AcquisitionError(
            "yt-dlp version mismatch before acquisition: "
            f"expected {expected_version}, observed {version}"
        )
    runtime_tree_after_version = verify_ytdlp_runtime_tree(
        config, "after initial version check"
    )
    output_template = stage_dir / "download.%(ext)s"
    command = ytdlp_download_command(
        executable=executable,
        config=config,
        source=source,
        output_template=output_template,
        max_bytes=max_bytes,
    )
    stage_dir.mkdir(parents=True, exist_ok=True)
    try:
        completed = run_ytdlp_monitored(
            command,
            environment=environment,
            output_root=output_root,
            stage_dir=stage_dir,
            limits=limits,
        )
    except LiveCapacityError:
        shutil.rmtree(stage_dir, ignore_errors=True)
        staging_parent = stage_dir.parent
        if staging_parent.exists() and not any(staging_parent.iterdir()):
            staging_parent.rmdir()
        raise
    try:
        metadata_candidates = []
        for line in completed.stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if isinstance(value, dict):
                metadata_candidates.append(value)
        if len(metadata_candidates) != 1:
            raise AcquisitionError(
                "yt-dlp must emit exactly one valid metadata object; observed "
                f"{len(metadata_candidates)}"
            )
        raw_metadata = metadata_candidates[0]
        validate_ytdlp_raw_identity(
            source, raw_metadata, config.get("expected_webpage_url")
        )
        selected_metadata = safe_ytdlp_metadata(
            raw_metadata,
            expected_reddit_webpage_url=config.get("expected_webpage_url"),
        )
        candidates = [
            path
            for path in stage_dir.glob("download.*")
            if path.is_file()
            and not path.name.endswith((".part", ".ytdl", ".json", ".vtt", ".srt"))
        ]
        if len(candidates) != 1:
            raise AcquisitionError(
                f"yt-dlp must leave exactly one final media file; observed {len(candidates)}"
            )
        staged = candidates[0]
        if staged.stat().st_size > max_bytes:
            raise AcquisitionError("yt-dlp output exceeded limits.max_job_bytes")
        executable_after = file_stat(executable)
        if (
            executable_after != executable_before
            or sha256_file(executable) != executable_sha256
        ):
            raise AcquisitionError("yt-dlp executable changed during acquisition")
        runtime_tree_after_download = verify_ytdlp_runtime_tree(
            config, "after acquisition"
        )
        version_after = ytdlp_version(executable, environment)
        if expected_version is not None and version_after != expected_version:
            raise AcquisitionError(
                "yt-dlp version mismatch after acquisition: "
                f"expected {expected_version}, observed {version_after}"
            )
        runtime_tree_after = verify_ytdlp_runtime_tree(
            config, "after final version check"
        )
        if version_after != version:
            raise AcquisitionError("yt-dlp version changed during acquisition")
    except (AcquisitionError, OSError):
        clean_stage(stage_dir, output_root)
        raise
    observation = {
        "yt_dlp": {
            "executable": str(executable),
            "executable_sha256": executable_sha256,
            "stat_before": executable_before,
            "stat_after": executable_after,
            "unchanged": True,
            "version": version,
            "version_before": version,
            "version_after": version_after,
            "runtime_tree_before": runtime_tree_before,
            "runtime_tree_after_initial_version": runtime_tree_after_version,
            "runtime_tree_after_download": runtime_tree_after_download,
            "runtime_tree_after": runtime_tree_after,
        }
    }
    return staged, selected_metadata, command, observation


def verify_expected(config: dict[str, Any], digest: str, byte_count: int) -> None:
    if config.get("expected_sha256") and config["expected_sha256"] != digest:
        raise AcquisitionError(
            f"media SHA-256 mismatch: expected {config['expected_sha256']}, observed {digest}"
        )
    if (
        config.get("expected_byte_count") is not None
        and config["expected_byte_count"] != byte_count
    ):
        raise AcquisitionError(
            "media byte count does not match adapter_config.expected_byte_count"
        )


def admit_staged(staged: Path, target: Path, digest: str) -> bool:
    """Atomically admit without overwriting; return True when an object was reused."""

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if not target.is_file() or sha256_file(target) != digest:
            raise AcquisitionError("content-addressed target exists with invalid content")
        staged.unlink(missing_ok=True)
        return True
    try:
        os.link(staged, target)
    except FileExistsError:
        if not target.is_file() or sha256_file(target) != digest:
            raise AcquisitionError("content-addressed admission race produced invalid content")
    except OSError as error:
        raise AcquisitionError(
            "atomic admission requires staging and cache to support same-filesystem hard links"
        ) from error
    staged.unlink()
    return False


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_" + sha256_bytes(canonical_bytes(list(parts)))[:32]


def catalog_records(
    *,
    work_order: dict[str, Any],
    digest: str,
    byte_count: int,
    probe: dict[str, Any],
    target: Path,
    observed_at: str,
    retrieval_tool: str,
    retrieval_tool_version: str,
    selected_metadata: dict[str, Any],
) -> dict[str, Any]:
    source = work_order["source"]
    source_id = stable_id(
        "source", source["platform"], source["source_kind"], source["native_id"]
    )
    media_id = f"media_sha256_{digest}"
    source_metadata = {
        "acquisition_adapter": work_order["adapter"],
        "selected_remote_metadata": selected_metadata,
    }
    if "handling_policy" in work_order:
        source_metadata["handling_policy"] = work_order["handling_policy"]
    source_row = {
        "source_id": source_id,
        "platform": source["platform"],
        "source_kind": source["source_kind"],
        "native_id": source["native_id"],
        "parent_source_id": None,
        "canonical_url": source["canonical_url"],
        "historical_url": None,
        "title": source["title"],
        "published_at": source["published_at"],
        "observed_at": observed_at,
        "access_state": source["access_state"],
        "review_state": "metadata_only",
        "metadata_json": source_metadata,
        "created_at": observed_at,
        "updated_at": observed_at,
    }
    media_row = {
        "media_id": media_id,
        "sha256": digest,
        "byte_count": byte_count,
        "media_kind": media_kind(probe),
        "mime_type": mimetypes.guess_type(source["canonical_url"] or "")[0],
        "container": probe["format"].get("format_name"),
        "duration_ms": probe["format"].get("duration_ms"),
        "ffprobe_json": probe,
        "first_cataloged_at": observed_at,
        "integrity_state": "verified",
    }
    location_row = {
        "media_location_id": stable_id("media_location", media_id, target.as_uri()),
        "media_id": media_id,
        "storage_uri": target.as_uri(),
        "storage_class": "local_hot_cache",
        "verified_at": observed_at,
        "is_primary": 1,
    }
    media_source_row = {
        "media_source_id": stable_id("media_source", media_id, source_id),
        "media_id": media_id,
        "source_id": source_id,
        "retrieved_at": observed_at,
        "retrieval_tool": retrieval_tool,
        "retrieval_tool_version": retrieval_tool_version,
        "source_snapshot_id": None,
    }
    return {
        "sources": [source_row],
        "media_objects": [media_row],
        "media_locations": [location_row],
        "media_sources": [media_source_row],
    }


COMPLETED_RESULT_KEYS = {
    "schema_version",
    "job_id",
    "adapter",
    "status",
    "dry_run",
    "reused",
    "work_order_sha256",
    "started_at",
    "completed_at",
    "duration_ms",
    "source",
    "limits",
    "capacity_before",
    "capacity_after",
    "commands",
    "source_observation",
    "selected_remote_metadata",
    "admission",
    "catalog_records",
    "result_path",
    "errors",
}
CAPACITY_KEYS = {
    "filesystem_path",
    "managed_bytes",
    "free_bytes",
    "reserve_bytes",
    "projected_managed_bytes",
    "projected_free_bytes",
    "global_cache_cap_bytes",
    "free_space_floor_bytes",
}
FILE_STAT_KEYS = {"device", "inode", "byte_count", "mtime_ns"}
RUNTIME_TREE_KEYS = {"root", "sha256", "file_count", "byte_count"}
PROBE_BASE_STREAM_KEYS = {
    "index",
    "codec_type",
    "codec_name",
    "duration_ms",
    "bit_rate_bps",
    "language",
}


def _result_integer(value: Any, *, minimum: int = 0) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= minimum


def _result_text(value: Any, maximum: int = 16_384) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= maximum
        and "\x00" not in value
    )


def _nullable_result_text(value: Any, maximum: int = 16_384) -> bool:
    return value is None or _result_text(value, maximum)


def _result_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _valid_file_stat(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == FILE_STAT_KEYS
        and all(_result_integer(value[key]) for key in FILE_STAT_KEYS)
    )


def _valid_capacity(
    value: Any, *, output_root: Path, limits: dict[str, int], reserve_bytes: int | None
) -> bool:
    if not isinstance(value, dict) or set(value) != CAPACITY_KEYS:
        return False
    if value["filesystem_path"] != str(output_root):
        return False
    integer_fields = CAPACITY_KEYS - {"filesystem_path", "projected_free_bytes"}
    if not all(_result_integer(value[key]) for key in integer_fields):
        return False
    if isinstance(value["projected_free_bytes"], bool) or not isinstance(
        value["projected_free_bytes"], int
    ):
        return False
    if (
        value["global_cache_cap_bytes"] != limits["global_cache_cap_bytes"]
        or value["free_space_floor_bytes"] != limits["free_space_floor_bytes"]
        or value["projected_managed_bytes"]
        != value["managed_bytes"] + value["reserve_bytes"]
        or value["projected_free_bytes"]
        != value["free_bytes"] - value["reserve_bytes"]
    ):
        return False
    return reserve_bytes is None or value["reserve_bytes"] == reserve_bytes


def _valid_optional_nonnegative_integer(value: Any) -> bool:
    return value is None or _result_integer(value)


def _valid_probe(probe: Any) -> bool:
    if not isinstance(probe, dict) or set(probe) != {
        "schema_version",
        "tool",
        "format",
        "streams",
    }:
        return False
    tool = probe["tool"]
    if (
        probe["schema_version"] != CONTRACT_VERSION
        or not isinstance(tool, dict)
        or set(tool) != {"name", "version"}
        or tool["name"] != "ffprobe"
        or not _result_text(tool["version"], 1_000)
    ):
        return False
    format_value = probe["format"]
    if not isinstance(format_value, dict) or set(format_value) != {
        "format_name",
        "format_long_name",
        "duration_ms",
        "bit_rate_bps",
    }:
        return False
    if not (
        _nullable_result_text(format_value["format_name"], 4_096)
        and _nullable_result_text(format_value["format_long_name"], 4_096)
        and _valid_optional_nonnegative_integer(format_value["duration_ms"])
        and _valid_optional_nonnegative_integer(format_value["bit_rate_bps"])
    ):
        return False
    streams = probe["streams"]
    if not isinstance(streams, list) or len(streams) > 10_000:
        return False
    for stream in streams:
        if not isinstance(stream, dict):
            return False
        codec_type = stream.get("codec_type")
        expected = set(PROBE_BASE_STREAM_KEYS)
        if codec_type == "video":
            expected |= {"width", "height", "pixel_format", "average_frame_rate"}
        elif codec_type == "audio":
            expected |= {"sample_rate_hz", "channels", "channel_layout"}
        if set(stream) != expected:
            return False
        if not (
            _valid_optional_nonnegative_integer(stream["index"])
            and _nullable_result_text(codec_type, 1_000)
            and _nullable_result_text(stream["codec_name"], 1_000)
            and _valid_optional_nonnegative_integer(stream["duration_ms"])
            and _valid_optional_nonnegative_integer(stream["bit_rate_bps"])
            and _nullable_result_text(stream["language"], 1_000)
        ):
            return False
        if codec_type == "video" and not (
            _valid_optional_nonnegative_integer(stream["width"])
            and _valid_optional_nonnegative_integer(stream["height"])
            and _nullable_result_text(stream["pixel_format"], 1_000)
            and _nullable_result_text(stream["average_frame_rate"], 1_000)
        ):
            return False
        if codec_type == "audio" and not (
            _valid_optional_nonnegative_integer(stream["sample_rate_hz"])
            and _valid_optional_nonnegative_integer(stream["channels"])
            and _nullable_result_text(stream["channel_layout"], 1_000)
        ):
            return False
    return True


def _valid_runtime_tree(value: Any) -> bool:
    if value is None:
        return True
    return (
        isinstance(value, dict)
        and set(value) == RUNTIME_TREE_KEYS
        and isinstance(value["root"], str)
        and Path(value["root"]).is_absolute()
        and isinstance(value["sha256"], str)
        and bool(SHA256_RE.fullmatch(value["sha256"]))
        and _result_integer(value["file_count"], minimum=1)
        and value["file_count"] <= MAX_RUNTIME_TREE_FILES
        and _result_integer(value["byte_count"], minimum=1)
        and value["byte_count"] <= MAX_RUNTIME_TREE_BYTES
    )


def _valid_selected_metadata(
    selected: Any, work_order: dict[str, Any], byte_count: int
) -> bool:
    if not isinstance(selected, dict):
        return False
    adapter = work_order["adapter"]
    if adapter == "local_file":
        return selected == {}
    if adapter == "yt_dlp":
        try:
            if selected != safe_ytdlp_metadata(
                selected,
                expected_reddit_webpage_url=work_order["adapter_config"].get(
                    "expected_webpage_url"
                ),
            ):
                return False
            validate_ytdlp_selected_identity(
                work_order["source"],
                selected,
                work_order["adapter_config"].get("expected_webpage_url"),
            )
        except AcquisitionError:
            return False
        return True

    keys = {
        "status",
        "final_url",
        "content_type",
        "content_length",
        "content_range",
        "accept_ranges",
        "etag",
        "last_modified",
        "resumed_from_bytes",
        "staged_byte_count",
    }
    if set(selected) != keys:
        return False
    status = selected["status"]
    if isinstance(status, bool) or not isinstance(status, int) or status not in (200, 206):
        return False
    if not all(
        _nullable_result_text(selected[key])
        for key in (
            "final_url",
            "content_type",
            "content_range",
            "accept_ranges",
            "etag",
            "last_modified",
        )
    ):
        return False
    if not (
        _valid_optional_nonnegative_integer(selected["content_length"])
        and _result_integer(selected["resumed_from_bytes"])
        and selected["resumed_from_bytes"] <= byte_count
        and selected["staged_byte_count"] == byte_count
    ):
        return False
    try:
        if selected["final_url"] is not None:
            sanitize_url(selected["final_url"], "reusable HTTP final URL")
    except AcquisitionError:
        return False
    return True


def _valid_source_observation(
    observation: Any,
    *,
    work_order: dict[str, Any],
    selected: dict[str, Any],
    byte_count: int,
) -> tuple[bool, str, str | None]:
    adapter = work_order["adapter"]
    if not isinstance(observation, dict):
        return False, "", None
    if adapter == "local_file":
        if set(observation) != {"local_source"}:
            return False, "", None
        local = observation["local_source"]
        if (
            not isinstance(local, dict)
            or set(local) != {"stat_before", "stat_after", "unchanged"}
            or local["unchanged"] is not True
            or not _valid_file_stat(local["stat_before"])
            or local["stat_before"] != local["stat_after"]
            or local["stat_before"]["byte_count"] != byte_count
        ):
            return False, "", None
        return True, "himr-acquisition-local-file", IMPLEMENTATION_VERSION
    if adapter == "direct_http":
        if set(observation) != {"http_source"}:
            return False, "", None
        http = observation["http_source"]
        expected = {
            "requested_url": work_order["adapter_config"]["url"],
            "final_url": selected["final_url"],
            "etag": selected["etag"],
            "last_modified": selected["last_modified"],
            "resumed_from_bytes": selected["resumed_from_bytes"],
        }
        if http != expected:
            return False, "", None
        return True, "python-urllib", None

    if set(observation) != {"yt_dlp"}:
        return False, "", None
    yt = observation["yt_dlp"]
    keys = {
        "executable",
        "executable_sha256",
        "stat_before",
        "stat_after",
        "unchanged",
        "version",
        "version_before",
        "version_after",
        "runtime_tree_before",
        "runtime_tree_after_initial_version",
        "runtime_tree_after_download",
        "runtime_tree_after",
    }
    if not isinstance(yt, dict) or set(yt) != keys:
        return False, "", None
    version = yt["version"]
    if (
        yt["executable"] != work_order["adapter_config"]["executable"]
        or not isinstance(yt["executable_sha256"], str)
        or not SHA256_RE.fullmatch(yt["executable_sha256"])
        or yt["unchanged"] is not True
        or not _valid_file_stat(yt["stat_before"])
        or yt["stat_before"] != yt["stat_after"]
        or not _result_text(version, 200)
        or yt["version_before"] != version
        or yt["version_after"] != version
    ):
        return False, "", None
    expected_executable = work_order["adapter_config"].get(
        "expected_executable_sha256"
    )
    if expected_executable is not None and yt["executable_sha256"] != expected_executable:
        return False, "", None
    expected_version = work_order["adapter_config"].get("expected_ytdlp_version")
    if expected_version is not None and version != expected_version:
        return False, "", None
    runtime_fields = (
        "runtime_tree_before",
        "runtime_tree_after_initial_version",
        "runtime_tree_after_download",
        "runtime_tree_after",
    )
    runtime_values = [yt[key] for key in runtime_fields]
    if not all(_valid_runtime_tree(value) for value in runtime_values):
        return False, "", None
    if any(value != runtime_values[0] for value in runtime_values[1:]):
        return False, "", None
    expected_root = work_order["adapter_config"].get("expected_runtime_tree_root")
    expected_runtime_sha = work_order["adapter_config"].get(
        "expected_runtime_tree_sha256"
    )
    if expected_root is None:
        if runtime_values[0] is not None:
            return False, "", None
    elif (
        runtime_values[0] is None
        or runtime_values[0]["root"] != expected_root
        or runtime_values[0]["sha256"] != expected_runtime_sha
    ):
        return False, "", None
    return True, "yt-dlp", version


def _valid_commands(
    commands: Any, *, work_order: dict[str, Any], stage_dir: Path
) -> bool:
    if (
        not isinstance(commands, list)
        or len(commands) != 2
        or any(
            not isinstance(command, list)
            or not command
            or any(not _result_text(argument, 32_768) for argument in command)
            for command in commands
        )
    ):
        return False
    adapter = work_order["adapter"]
    config = work_order["adapter_config"]
    if adapter == "local_file":
        expected_first = [
            "local-copy",
            config["path"],
            str(stage_dir / "payload.part"),
        ]
    elif adapter == "direct_http":
        expected_first = [
            "http-get",
            config["url"],
            "--resume" if config["resume"] else "--no-resume",
            "--max-bytes",
            str(work_order["limits"]["max_job_bytes"]),
            "--output",
            str(stage_dir / "payload.part"),
        ]
    else:
        expected_first = ytdlp_download_command(
            executable=Path(config["executable"]),
            config=config,
            source=work_order["source"],
            output_template=stage_dir / "download.%(ext)s",
            max_bytes=work_order["limits"]["max_job_bytes"],
        )
    if commands[0] != expected_first:
        return False
    probe = commands[1]
    if (
        len(probe) != 8
        or not Path(probe[0]).is_absolute()
        or Path(probe[0]).name != "ffprobe"
        or probe[1:7] != ["-v", "error", "-show_format", "-show_streams", "-of", "json"]
    ):
        return False
    probed_path = Path(probe[7])
    if adapter in {"local_file", "direct_http"}:
        return probed_path == stage_dir / "payload.part"
    return (
        probed_path.parent == stage_dir
        and probed_path.name.startswith("download.")
        and probed_path.name not in {"download.part", "download.json"}
    )


def validate_reusable_result(
    result: dict[str, Any],
    output_root: Path,
    work_order: dict[str, Any],
    *,
    result_path: Path | None = None,
    pins: PinnedFiles | None = None,
) -> bool:
    own_pins = pins is None
    pins = pins or PinnedFiles()
    work_order_sha256 = sha256_bytes(canonical_bytes(work_order))
    expected_result_path = result_path or (
        output_root
        / "jobs"
        / work_order["job_id"]
        / work_order_sha256
        / "result.json"
    )
    try:
        expected_result_keys = COMPLETED_RESULT_KEYS | (
            {"handling_policy"} if "handling_policy" in work_order else set()
        )
        if set(result) != expected_result_keys:
            return False
        if (
            result["schema_version"] != CONTRACT_VERSION
            or result["status"] != "completed"
            or result["dry_run"] is not False
            or not isinstance(result["reused"], bool)
            or result["job_id"] != work_order["job_id"]
            or result["adapter"] != work_order["adapter"]
            or result["work_order_sha256"] != work_order_sha256
            or result["source"] != work_order["source"]
            or result["limits"] != work_order["limits"]
            or result["result_path"] != str(expected_result_path)
            or result["errors"] != []
            or not _result_integer(result["duration_ms"])
        ):
            return False
        if result.get("handling_policy") != work_order.get("handling_policy"):
            return False
        started = _result_timestamp(result["started_at"])
        completed = _result_timestamp(result["completed_at"])
        if started is None or completed is None or completed < started:
            return False
        wall_duration_ms = round((completed - started).total_seconds() * 1_000)
        if abs(result["duration_ms"] - wall_duration_ms) > 2_000:
            return False

        reserve_bytes = (
            Path(work_order["adapter_config"]["path"]).stat().st_size
            if work_order["adapter"] == "local_file"
            else work_order["limits"]["max_job_bytes"]
        )
        if not _valid_capacity(
            result["capacity_before"],
            output_root=output_root,
            limits=work_order["limits"],
            reserve_bytes=reserve_bytes,
        ) or not _valid_capacity(
            result["capacity_after"],
            output_root=output_root,
            limits=work_order["limits"],
            reserve_bytes=0,
        ):
            return False

        admission = result["admission"]
        if not isinstance(admission, dict) or set(admission) != {
            "media_id",
            "sha256",
            "byte_count",
            "path",
            "storage_uri",
            "normalized_probe",
        }:
            return False
        digest = admission["sha256"]
        byte_count = admission["byte_count"]
        if (
            not isinstance(digest, str)
            or not SHA256_RE.fullmatch(digest)
            or not _result_integer(byte_count, minimum=1)
            or byte_count > work_order["limits"]["max_job_bytes"]
            or admission["media_id"] != f"media_sha256_{digest}"
            or not _valid_probe(admission["normalized_probe"])
        ):
            return False
        config = work_order["adapter_config"]
        if (
            config.get("expected_sha256") is not None
            and config["expected_sha256"] != digest
        ) or (
            config.get("expected_byte_count") is not None
            and config["expected_byte_count"] != byte_count
        ):
            return False
        target = output_root / "media" / "sha256" / digest[:2] / digest / "payload"
        if admission["path"] != str(target) or admission["storage_uri"] != target.as_uri():
            return False

        selected = result["selected_remote_metadata"]
        if not _valid_selected_metadata(selected, work_order, byte_count):
            return False
        observation_valid, retrieval_tool, retrieval_version = _valid_source_observation(
            result["source_observation"],
            work_order=work_order,
            selected=selected,
            byte_count=byte_count,
        )
        if not observation_valid:
            return False
        stage_dir = output_root / ".staging" / (
            f"{work_order['job_id']}-{work_order_sha256[:16]}"
        )
        if not _valid_commands(
            result["commands"], work_order=work_order, stage_dir=stage_dir
        ):
            return False

        records = result["catalog_records"]
        record_keys = {"sources", "media_objects", "media_locations", "media_sources"}
        if (
            not isinstance(records, dict)
            or set(records) != record_keys
            or any(
                not isinstance(records[key], list) or len(records[key]) != 1
                for key in record_keys
            )
        ):
            return False
        media_source = records["media_sources"][0]
        if not isinstance(media_source, dict):
            return False
        if retrieval_version is None:
            retrieval_version = media_source.get("retrieval_tool_version")
        if (
            media_source.get("retrieval_tool") != retrieval_tool
            or not _result_text(retrieval_version, 1_000)
        ):
            return False
        expected_records = catalog_records(
            work_order=work_order,
            digest=digest,
            byte_count=byte_count,
            probe=admission["normalized_probe"],
            target=target,
            observed_at=result["completed_at"],
            retrieval_tool=retrieval_tool,
            retrieval_tool_version=retrieval_version,
            selected_metadata=selected,
        )
        if records != expected_records:
            return False

        try:
            payload = pins.open(
                target,
                root=output_root,
                maximum=work_order["limits"]["max_job_bytes"],
                capture=False,
                label="reusable content-addressed payload",
            )
        except FileNotFoundError:
            return False
        return payload.initial_stat.st_size == byte_count and payload.digest == digest
    except (KeyError, OSError, TypeError, ValueError):
        return False
    finally:
        if own_pins:
            try:
                pins.verify()
            finally:
                pins.close()


def load_reusable_result(
    result_path: Path, output_root: Path, work_order: dict[str, Any]
) -> dict[str, Any] | None:
    pins = PinnedFiles()
    try:
        try:
            result_file = pins.open(
                result_path,
                root=output_root,
                maximum=MAX_DURABLE_RESULT_BYTES,
                capture=True,
                label="durable acquisition result",
            )
        except FileNotFoundError:
            return None
        if result_file.body is None:
            raise AcquisitionError("durable acquisition result was not captured")
        result = strict_json_object(result_file.body, "durable acquisition result")
        if not validate_reusable_result(
            result,
            output_root,
            work_order,
            result_path=result_path,
            pins=pins,
        ):
            pins.verify()
            return None
        pins.verify()
        return result
    finally:
        pins.close()


def clean_stage(stage_dir: Path, output_root: Path) -> None:
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    parent = stage_dir.parent
    if parent != output_root and parent.exists() and not any(parent.iterdir()):
        parent.rmdir()


def _run_acquisition_body(work_order: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    started_at = utc_now()
    start_clock = time.monotonic()
    output_root = Path(work_order["output"]["root"])
    work_order_sha256 = sha256_bytes(canonical_bytes(work_order))
    result_dir = output_root / "jobs" / work_order["job_id"] / work_order_sha256
    result_path = result_dir / "result.json"
    stage_dir = output_root / ".staging" / f"{work_order['job_id']}-{work_order_sha256[:16]}"
    if not dry_run:
        existing = load_reusable_result(result_path, output_root, work_order)
        if existing is not None:
            reused = dict(existing)
            reused["reused"] = True
            reused["reuse_verified_at"] = utc_now()
            return reused

    adapter = work_order["adapter"]
    config = work_order["adapter_config"]
    if adapter == "local_file":
        reserve_bytes = Path(config["path"]).stat().st_size
        if reserve_bytes > work_order["limits"]["max_job_bytes"]:
            raise AcquisitionError("local source exceeds limits.max_job_bytes")
    else:
        reserve_bytes = work_order["limits"]["max_job_bytes"]
    capacity_before = capacity_snapshot(
        output_root,
        work_order["limits"],
        reserve_bytes=reserve_bytes,
        enforce=True,
    )
    ffprobe = require_ffprobe()
    probe_version = ffprobe_version(ffprobe)
    planned_commands: list[list[str]] = []
    source_observation: dict[str, Any] = {}
    selected_metadata: dict[str, Any] = {}
    local_dry_probe: dict[str, Any] | None = None
    local_dry_digest: str | None = None

    if adapter == "local_file":
        source_path = Path(config["path"])
        planned_commands.append(["local-copy", str(source_path), str(stage_dir / "payload.part")])
        if dry_run:
            before = file_stat(source_path)
            local_dry_digest = sha256_file(source_path)
            after = file_stat(source_path)
            if after != before:
                raise AcquisitionError("local source metadata changed during dry-run inspection")
            verify_expected(config, local_dry_digest, before["byte_count"])
            local_dry_probe, probe_command = probe_file(source_path, ffprobe, probe_version)
            planned_commands.append(probe_command)
            source_observation = {
                "local_source": {
                    "stat_before": before,
                    "stat_after": after,
                    "unchanged": True,
                }
            }
    elif adapter == "direct_http":
        planned_commands.append(
            [
                "http-get",
                config["url"],
                "--resume" if config["resume"] else "--no-resume",
                "--max-bytes",
                str(work_order["limits"]["max_job_bytes"]),
                "--output",
                str(stage_dir / "payload.part"),
            ]
        )
    else:
        executable = Path(config["executable"])
        planned_commands.append(
            ytdlp_download_command(
                executable=executable,
                config=config,
                source=work_order["source"],
                output_template=stage_dir / "download.%(ext)s",
                max_bytes=work_order["limits"]["max_job_bytes"],
            )
        )

    if dry_run:
        media_path = None
        if local_dry_digest:
            media_path = str(
                output_root
                / "media"
                / "sha256"
                / local_dry_digest[:2]
                / local_dry_digest
                / "payload"
            )
        result = {
            "schema_version": CONTRACT_VERSION,
            "job_id": work_order["job_id"],
            "adapter": adapter,
            "status": "planned",
            "dry_run": True,
            "reused": False,
            "work_order_sha256": work_order_sha256,
            "started_at": started_at,
            "completed_at": utc_now(),
            "duration_ms": round((time.monotonic() - start_clock) * 1_000),
            "source": work_order["source"],
            "limits": work_order["limits"],
            "capacity_before": capacity_before,
            "commands": planned_commands,
            "source_observation": source_observation,
            "selected_remote_metadata": selected_metadata,
            "planned_admission": {
                "sha256": local_dry_digest,
                "path": media_path,
                "normalized_probe": local_dry_probe,
            },
            "result_path": str(result_path),
            "errors": [],
        }
        if "handling_policy" in work_order:
            result["handling_policy"] = work_order["handling_policy"]
        return result

    staged: Path
    retrieval_tool: str
    retrieval_tool_version: str
    if adapter == "local_file":
        staged = stage_dir / "payload.part"
        copy_digest, source_observation, command = copy_local_source(
            Path(config["path"]), staged, work_order["limits"]["max_job_bytes"]
        )
        planned_commands = [command]
        retrieval_tool = "himr-acquisition-local-file"
        retrieval_tool_version = IMPLEMENTATION_VERSION
    elif adapter == "direct_http":
        staged = stage_dir / "payload.part"
        selected_metadata, command = download_http(
            url=config["url"],
            staged=staged,
            sidecar=stage_dir / "http-resume.json",
            max_bytes=work_order["limits"]["max_job_bytes"],
            timeout=config["timeout_seconds"],
            resume=config["resume"],
        )
        planned_commands = [command]
        copy_digest = None
        retrieval_tool = "python-urllib"
        retrieval_tool_version = sys.version.split()[0]
        source_observation = {
            "http_source": {
                "requested_url": config["url"],
                "final_url": selected_metadata.get("final_url"),
                "etag": selected_metadata.get("etag"),
                "last_modified": selected_metadata.get("last_modified"),
                "resumed_from_bytes": selected_metadata.get("resumed_from_bytes", 0),
            }
        }
    else:
        staged, selected_metadata, command, source_observation = download_ytdlp(
            config=config,
            source=work_order["source"],
            stage_dir=stage_dir,
            output_root=output_root,
            limits=work_order["limits"],
            max_bytes=work_order["limits"]["max_job_bytes"],
        )
        planned_commands = [command]
        copy_digest = None
        retrieval_tool = "yt-dlp"
        retrieval_tool_version = source_observation["yt_dlp"]["version"]
        try:
            validate_ytdlp_selected_identity(
                work_order["source"],
                selected_metadata,
                config.get("expected_webpage_url"),
            )
        except AcquisitionError:
            clean_stage(stage_dir, output_root)
            raise

    byte_count = staged.stat().st_size
    if byte_count > work_order["limits"]["max_job_bytes"]:
        raise AcquisitionError("staged media exceeds limits.max_job_bytes")
    digest = sha256_file(staged)
    if copy_digest is not None and digest != copy_digest:
        raise AcquisitionError("staged local copy differs from bytes read from the source")
    verify_expected(config, digest, byte_count)
    probe, probe_command = probe_file(staged, ffprobe, probe_version)
    planned_commands.append(probe_command)
    target = output_root / "media" / "sha256" / digest[:2] / digest / "payload"
    admission_reused = admit_staged(staged, target, digest)
    completed_at = utc_now()
    records = catalog_records(
        work_order=work_order,
        digest=digest,
        byte_count=byte_count,
        probe=probe,
        target=target,
        observed_at=completed_at,
        retrieval_tool=retrieval_tool,
        retrieval_tool_version=retrieval_tool_version,
        selected_metadata=selected_metadata,
    )
    clean_stage(stage_dir, output_root)
    capacity_after = capacity_snapshot(
        output_root,
        work_order["limits"],
        reserve_bytes=0,
        enforce=True,
    )
    result = {
        "schema_version": CONTRACT_VERSION,
        "job_id": work_order["job_id"],
        "adapter": adapter,
        "status": "completed",
        "dry_run": False,
        "reused": admission_reused,
        "work_order_sha256": work_order_sha256,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_ms": round((time.monotonic() - start_clock) * 1_000),
        "source": work_order["source"],
        "limits": work_order["limits"],
        "capacity_before": capacity_before,
        "capacity_after": capacity_after,
        "commands": planned_commands,
        "source_observation": source_observation,
        "selected_remote_metadata": selected_metadata,
        "admission": {
            "media_id": f"media_sha256_{digest}",
            "sha256": digest,
            "byte_count": byte_count,
            "path": str(target),
            "storage_uri": target.as_uri(),
            "normalized_probe": probe,
        },
        "catalog_records": records,
        "result_path": str(result_path),
        "errors": [],
    }
    if "handling_policy" in work_order:
        result["handling_policy"] = work_order["handling_policy"]
    atomic_write_json(result_path, result)
    return result


def run_acquisition(work_order: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    if dry_run:
        return _run_acquisition_body(work_order, dry_run=True)

    config = work_order["adapter_config"]
    if work_order["adapter"] == "local_file":
        source_bytes = Path(config["path"]).stat().st_size
        if source_bytes > work_order["limits"]["max_job_bytes"]:
            raise AcquisitionError("local source exceeds limits.max_job_bytes")

    output_root = Path(work_order["output"]["root"])
    work_order_sha256 = sha256_bytes(canonical_bytes(work_order))
    lock_handle, lock_metadata = acquire_writer_lock(
        output_root,
        job_id=work_order["job_id"],
        work_order_sha256=work_order_sha256,
    )
    try:
        return _run_acquisition_body(work_order, dry_run=False)
    finally:
        release_writer_lock(lock_handle, lock_metadata)


def create_work_order(args: argparse.Namespace) -> dict[str, Any]:
    source = {
        "platform": args.platform,
        "source_kind": args.source_kind,
        "native_id": args.native_id,
        "canonical_url": args.canonical_url,
        "title": args.title,
        "published_at": args.published_at,
        "access_state": (
            args.access_state
            if args.access_state is not None
            else ("unknown" if args.adapter == "local_file" else "public")
        ),
    }
    common = {
        "expected_sha256": args.expected_sha256,
        "expected_byte_count": args.expected_byte_count,
    }
    if args.adapter == "local_file":
        adapter_config = {**common, "path": args.local_path}
    elif args.adapter == "direct_http":
        adapter_config = {
            **common,
            "url": args.url,
            "resume": not args.no_resume,
            "timeout_seconds": args.timeout_seconds,
        }
    else:
        adapter_config = {
            **common,
            "url": args.url,
            "executable": args.yt_dlp_executable,
            "format_selector": args.format_selector,
        }
        if args.yt_dlp_sha256 is not None:
            adapter_config["expected_executable_sha256"] = args.yt_dlp_sha256
    raw = {
        "schema_version": CONTRACT_VERSION,
        "job_id": args.job_id,
        "adapter": args.adapter,
        "source": source,
        "adapter_config": adapter_config,
        "output": {"root": args.output_root},
        "limits": {
            "max_job_bytes": args.max_job_bytes,
            "global_cache_cap_bytes": args.global_cache_cap_bytes,
            "free_space_floor_bytes": args.free_space_floor_bytes,
        },
    }
    requested_handling = (
        args.publication_disposition is not None or args.handling_basis is not None
    )
    if requested_handling:
        if args.publication_disposition is None or args.handling_basis is None:
            raise AcquisitionError(
                "--publication-disposition and --handling-basis must be supplied together"
            )
        raw["handling_policy"] = {
            "storage_scope": "private_canonical_cache",
            "publication_disposition": args.publication_disposition,
            "publication_authority": "none",
            "basis": args.handling_basis,
        }
    return validate_work_order(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Guarded local, direct HTTP, and yt-dlp media acquisition"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-work-order")
    create.add_argument("--job-id", required=True)
    create.add_argument("--adapter", choices=ADAPTERS, required=True)
    create.add_argument("--platform", required=True)
    create.add_argument("--source-kind", required=True)
    create.add_argument("--native-id", required=True)
    create.add_argument("--canonical-url")
    create.add_argument("--title")
    create.add_argument("--published-at")
    create.add_argument(
        "--access-state",
        choices=(
            "public",
            "members_only",
            "private",
            "removed",
            "unavailable",
            "unknown",
        ),
        help=(
            "source access state; defaults to unknown for local_file and public "
            "for network adapters"
        ),
    )
    create.add_argument(
        "--publication-disposition",
        choices=("no_publication_authority", "never_publish"),
        help=(
            "optional private-cache publication constraint; requires --handling-basis"
        ),
    )
    create.add_argument(
        "--handling-basis",
        help="bounded provenance note for an explicit publication constraint",
    )
    create.add_argument("--local-path")
    create.add_argument("--url")
    create.add_argument("--yt-dlp-executable")
    create.add_argument(
        "--yt-dlp-sha256",
        help="optional lowercase SHA-256 pin enforced before yt-dlp is invoked",
    )
    create.add_argument(
        "--format-selector", default="bv*[height<=720]+ba/b[height<=720]/b"
    )
    create.add_argument("--no-resume", action="store_true")
    create.add_argument("--timeout-seconds", type=int, default=60)
    create.add_argument("--expected-sha256")
    create.add_argument("--expected-byte-count", type=int)
    create.add_argument("--output-root", required=True)
    create.add_argument("--max-job-bytes", type=int, default=DEFAULT_JOB_CAP)
    create.add_argument(
        "--global-cache-cap-bytes", type=int, default=RECOMMENDED_CACHE_CAP
    )
    create.add_argument(
        "--free-space-floor-bytes", type=int, default=RECOMMENDED_FREE_FLOOR
    )

    validate = commands.add_parser("validate")
    validate.add_argument("--work-order", required=True)
    run = commands.add_parser("run")
    run.add_argument("--work-order", required=True)
    run.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "create-work-order":
            result = create_work_order(args)
        else:
            work_order_path = absolute_path(
                args.work_order, "--work-order", must_exist=True
            )
            result = validate_work_order(load_json(work_order_path))
            if args.command == "run":
                result = run_acquisition(result, args.dry_run)
        sys.stdout.write(pretty_json(result))
        return 0
    except (AcquisitionError, OSError) as error:
        sys.stderr.write(
            pretty_json(
                {
                    "schema_version": CONTRACT_VERSION,
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
