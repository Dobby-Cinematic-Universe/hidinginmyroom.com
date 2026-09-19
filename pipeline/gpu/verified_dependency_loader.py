#!/usr/bin/env python3
"""Load a local Python dependency only from bytes that match an exact digest.

This module is intentionally small and standard-library-only.  It is a building
block for successor GPU adapters whose preserved implementations are immutable
evidence: the dependency pathname is opened without following the leaf symlink,
the retained descriptor is read and checked for drift, and only the verified byte
string is compiled and executed.  A digest mismatch therefore cannot execute the
candidate file even transiently.

The caller remains the root of trust and must pin this loader's own bytes (or embed
an equivalent bootstrap) before importing it.  This module does not make a mutable
pathname self-authenticating.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MODULE_NAME_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)
READ_CHUNK_BYTES = 1024 * 1024
MAX_SOURCE_BYTES = 16 * 1024 * 1024


class VerifiedDependencyError(RuntimeError):
    """A candidate dependency failed its path, file, or digest binding."""


@dataclass(frozen=True)
class VerifiedSource:
    """Exact source bytes and the retained file identity that produced them."""

    path: Path
    body: bytes
    sha256: str
    byte_count: int
    device: int
    inode: int
    mode: int
    link_count: int
    uid: int
    mtime_ns: int
    ctime_ns: int

    def evidence(self) -> dict[str, Any]:
        """Return a JSON-compatible, text-free binding for admission receipts."""

        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "byte_count": self.byte_count,
            "device": self.device,
            "inode": self.inode,
            "mode": self.mode,
            "link_count": self.link_count,
            "uid": self.uid,
            "mtime_ns": self.mtime_ns,
            "ctime_ns": self.ctime_ns,
        }


def _normalized_absolute_path(value: str | Path, label: str) -> Path:
    text = os.fspath(value)
    if not isinstance(text, str) or not text or "\x00" in text:
        raise VerifiedDependencyError(f"{label} must be a non-empty path string")
    path = Path(text)
    if not path.is_absolute() or Path(os.path.normpath(text)) != path:
        raise VerifiedDependencyError(f"{label} must be absolute and normalized")
    return path


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
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


def read_verified_source(
    path_value: str | Path,
    expected_sha256: str,
    *,
    label: str = "Python dependency",
    maximum_bytes: int = MAX_SOURCE_BYTES,
    exact_mode: int | None = None,
    single_link: bool = True,
    current_user_owned: bool = True,
) -> VerifiedSource:
    """Stable-read one dependency and return only exact digest-matching bytes.

    The pathname is checked again after the retained descriptor is read.  The byte
    string is returned only if descriptor metadata stayed stable and the pathname
    still names that same inode.  No candidate source is decoded, compiled, imported,
    or executed in this function.
    """

    if not isinstance(expected_sha256, str) or not SHA256_RE.fullmatch(
        expected_sha256
    ):
        raise VerifiedDependencyError(f"{label} expected SHA-256 is invalid")
    if (
        isinstance(maximum_bytes, bool)
        or not isinstance(maximum_bytes, int)
        or not 1 <= maximum_bytes <= MAX_SOURCE_BYTES
    ):
        raise VerifiedDependencyError(
            f"{label} maximum byte count must be between 1 and {MAX_SOURCE_BYTES}"
        )
    if exact_mode is not None and (
        isinstance(exact_mode, bool)
        or not isinstance(exact_mode, int)
        or not 0 <= exact_mode <= 0o7777
    ):
        raise VerifiedDependencyError(f"{label} exact mode is invalid")

    path = _normalized_absolute_path(path_value, label)
    try:
        lexical = path.lstat()
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or path.resolve(strict=True) != path
        ):
            raise VerifiedDependencyError(
                f"{label} must be an existing non-symlinked regular file"
            )
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
    except VerifiedDependencyError:
        raise
    except OSError as error:
        raise VerifiedDependencyError(f"{label} cannot be opened safely: {error}") from error

    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise VerifiedDependencyError(f"{label} descriptor is not a regular file")
        if single_link and before.st_nlink != 1:
            raise VerifiedDependencyError(f"{label} must have exactly one hard link")
        if current_user_owned and before.st_uid != os.getuid():
            raise VerifiedDependencyError(f"{label} must be owned by the current user")
        observed_mode = stat.S_IMODE(before.st_mode)
        if exact_mode is not None and observed_mode != exact_mode:
            raise VerifiedDependencyError(f"{label} must have mode {exact_mode:04o}")
        if not 1 <= before.st_size <= maximum_bytes:
            raise VerifiedDependencyError(
                f"{label} byte count is outside the admitted bound"
            )
        if (before.st_dev, before.st_ino) != (lexical.st_dev, lexical.st_ino):
            raise VerifiedDependencyError(f"{label} changed while it was opened")

        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise VerifiedDependencyError(f"{label} ended before its sealed size")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise VerifiedDependencyError(f"{label} grew while it was read")
        body = b"".join(chunks)
        after = os.fstat(descriptor)
        try:
            current = path.lstat()
        except OSError as error:
            raise VerifiedDependencyError(
                f"{label} pathname disappeared during verification: {error}"
            ) from error
        if _stat_identity(after) != _stat_identity(before):
            raise VerifiedDependencyError(f"{label} metadata changed while it was read")
        if _stat_identity(current) != _stat_identity(before):
            raise VerifiedDependencyError(f"{label} pathname changed while it was read")

        observed_sha256 = hashlib.sha256(body).hexdigest()
        if not hmac.compare_digest(observed_sha256, expected_sha256):
            raise VerifiedDependencyError(f"{label} SHA-256 does not match its pin")
        return VerifiedSource(
            path=path,
            body=body,
            sha256=observed_sha256,
            byte_count=len(body),
            device=before.st_dev,
            inode=before.st_ino,
            mode=observed_mode,
            link_count=before.st_nlink,
            uid=before.st_uid,
            mtime_ns=before.st_mtime_ns,
            ctime_ns=before.st_ctime_ns,
        )
    finally:
        os.close(descriptor)


def load_verified_module(
    module_name: str,
    path_value: str | Path,
    expected_sha256: str,
    *,
    label: str = "Python dependency",
    maximum_bytes: int = MAX_SOURCE_BYTES,
    exact_mode: int | None = None,
    single_link: bool = True,
    current_user_owned: bool = True,
) -> tuple[ModuleType, VerifiedSource]:
    """Compile and execute a module from verified bytes, never from its pathname."""

    if not isinstance(module_name, str) or not MODULE_NAME_RE.fullmatch(module_name):
        raise VerifiedDependencyError("verified dependency module name is invalid")
    source = read_verified_source(
        path_value,
        expected_sha256,
        label=label,
        maximum_bytes=maximum_bytes,
        exact_mode=exact_mode,
        single_link=single_link,
        current_user_owned=current_user_owned,
    )
    try:
        code = compile(
            source.body,
            str(source.path),
            "exec",
            flags=0,
            dont_inherit=True,
            optimize=0,
        )
    except (SyntaxError, UnicodeError, ValueError) as error:
        raise VerifiedDependencyError(
            f"{label} cannot be compiled after verification: {error}"
        ) from error

    module = ModuleType(module_name)
    module.__file__ = str(source.path)
    module.__loader__ = None
    module.__package__ = module_name.rpartition(".")[0]
    module.__spec__ = None
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        exec(code, module.__dict__)
    except Exception:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous
        raise
    # A verified dependency is not allowed to replace its own import identity
    # while it initializes.  Keeping this exact module registered also preserves
    # ordinary Python semantics for dataclasses, forward references, and pickling.
    sys.modules[module_name] = module
    return module, source
