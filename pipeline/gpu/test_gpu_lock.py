#!/usr/bin/env python3
"""Prove UUID-keyed flock exclusion and crash release without running inference."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import signal
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GPU_UUID_RE = re.compile(r"^GPU-[0-9a-fA-F-]{16,}$")


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def open_lock(path: Path) -> int:
    flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
        or metadata.st_size != 0
        or metadata.st_uid != os.getuid()
    ):
        os.close(descriptor)
        raise RuntimeError("GPU lock must be an owned empty mode-0600 single-link file")
    return descriptor


def wait_exit(pid: int) -> int:
    waited, status = os.waitpid(pid, 0)
    if waited != pid:
        raise RuntimeError("waitpid returned an unexpected process")
    return status


def atomic_write(path: Path, body: bytes) -> None:
    if not path.is_absolute() or path.resolve(strict=False) != path:
        raise ValueError("output path must be absolute and normalized")
    parent = path.parent
    metadata = parent.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != os.getuid()
    ):
        raise ValueError("output parent must be owned mode 0700")
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace output: {path}")
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    temporary = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o400,
            dir_fd=parent_fd,
        )
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.fchmod(descriptor, 0o400)
        os.link(
            temporary,
            path.name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
            follow_symlinks=False,
        )
        os.unlink(temporary, dir_fd=parent_fd)
        os.fsync(parent_fd)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        os.close(parent_fd)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock-path", required=True, type=Path)
    parser.add_argument("--gpu-uuid", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    lock_path = args.lock_path
    if (
        not lock_path.is_absolute()
        or lock_path.resolve(strict=True) != lock_path
        or not GPU_UUID_RE.fullmatch(args.gpu_uuid)
        or lock_path.name != f"{args.gpu_uuid}.lock"
    ):
        raise ValueError("lock path or GPU UUID is not canonical")

    initial = open_lock(lock_path)
    fcntl.flock(initial, fcntl.LOCK_EX | fcntl.LOCK_NB)
    contender = os.fork()
    if contender == 0:
        os.close(initial)
        descriptor = open_lock(lock_path)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os._exit(73)
        os._exit(74)
    contender_status = wait_exit(contender)
    if not os.WIFEXITED(contender_status) or os.WEXITSTATUS(contender_status) != 73:
        raise RuntimeError("concurrent lock contender was not rejected")
    fcntl.flock(initial, fcntl.LOCK_UN)
    os.close(initial)

    ready_read, ready_write = os.pipe2(os.O_CLOEXEC)
    crash_holder = os.fork()
    if crash_holder == 0:
        os.close(ready_read)
        descriptor = open_lock(lock_path)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(ready_write, b"1")
        signal.pause()
        os._exit(75)
    os.close(ready_write)
    if os.read(ready_read, 1) != b"1":
        raise RuntimeError("crash-test holder did not acquire the lock")
    os.close(ready_read)
    os.kill(crash_holder, signal.SIGKILL)
    crash_status = wait_exit(crash_holder)
    if not os.WIFSIGNALED(crash_status) or os.WTERMSIG(crash_status) != signal.SIGKILL:
        raise RuntimeError("crash-test holder did not terminate by SIGKILL")

    recovery = open_lock(lock_path)
    fcntl.flock(recovery, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(recovery, fcntl.LOCK_UN)
    os.close(recovery)

    core = {
        "kind": "himr_gpu_scheduler_lock_test_result",
        "schema_version": 1,
        "gpu_uuid": args.gpu_uuid,
        "lock_path": str(lock_path),
        "attempted_workers": 3,
        "successful_holders": 2,
        "rejected_contenders": 1,
        "maximum_simultaneous_holders": 1,
        "crash_release_verified": True,
        "completed_at": utc_now(),
    }
    result = {
        **core,
        "identity_sha256": hashlib.sha256(canonical_bytes(core)).hexdigest(),
    }
    body = canonical_bytes(result)
    atomic_write(args.output, body)
    print(
        json.dumps(
            {
                "status": "passed",
                "output": str(args.output),
                "sha256": hashlib.sha256(body).hexdigest(),
                "identity_sha256": result["identity_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
