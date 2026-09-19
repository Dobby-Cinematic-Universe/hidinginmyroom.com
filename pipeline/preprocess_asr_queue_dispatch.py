#!/usr/bin/env python3
"""Version-selecting entry point for preprocess-ASR queue materializers.

New materialization always uses the current implementation. Validation first
performs a bounded, stable read of the sealed manifest and then dispatches only
to the implementation version named by that manifest. Unknown and quarantined
historical versions fail closed; no implementation is used as a fallback.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path
from typing import Any, Callable

try:
    from . import preprocess_asr_queue as queue_v02
    from . import preprocess_asr_queue_v03 as queue_v03
except ImportError:  # pragma: no cover - direct script execution
    import preprocess_asr_queue as queue_v02  # type: ignore[no-redef]
    import preprocess_asr_queue_v03 as queue_v03  # type: ignore[no-redef]


CURRENT_IMPLEMENTATION_VERSION = "0.3.0"
MAX_MANIFEST_BYTES = max(queue_v02.MAX_MANIFEST_BYTES, queue_v03.MAX_MANIFEST_BYTES)
VALIDATORS: dict[str, Callable[[list[str] | None], int]] = {
    "0.2.0": queue_v02.main,
    "0.3.0": queue_v03.main,
}


class DispatchError(ValueError):
    """A manifest cannot be routed to one exact supported implementation."""


def _manifest_argument(argv: list[str]) -> Path:
    if len(argv) != 3 or argv[0] != "validate" or argv[1] != "--manifest":
        raise DispatchError("validate requires exactly: validate --manifest PATH")
    path = Path(argv[2])
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _stable_manifest_version(path: Path) -> str:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise DispatchError(f"cannot open queue manifest safely: {error}") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size < 1
            or before.st_size > MAX_MANIFEST_BYTES
        ):
            raise DispatchError("queue manifest is not a bounded regular file")
        chunks: list[bytes] = []
        remaining = MAX_MANIFEST_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if len(body) > MAX_MANIFEST_BYTES or len(body) != before.st_size:
        raise DispatchError("queue manifest exceeds its byte cap or changed during read")
    stable_fields = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise DispatchError("queue manifest changed during version selection")
    try:
        raw: Any = queue_v03.parse_json(body, "queue manifest dispatch header")
    except queue_v03.QueueError as error:
        raise DispatchError(str(error)) from error
    if not isinstance(raw, dict) or "implementation_version" not in raw:
        raise DispatchError("queue manifest has no implementation_version")
    version = raw.get("implementation_version")
    if not isinstance(version, str):
        raise DispatchError("queue manifest implementation_version is not a string")
    return version


def select_validator(argv: list[str]) -> Callable[[list[str] | None], int]:
    version = _stable_manifest_version(_manifest_argument(argv))
    if version == "0.1.0":
        raise DispatchError(
            "queue implementation 0.1.0 requires the documented original-path "
            "sandbox overlay; direct versioned import would violate its __file__ pin"
        )
    selected = VALIDATORS.get(version)
    if selected is None:
        raise DispatchError(f"unsupported queue implementation_version: {version!r}")
    return selected


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments[0] in {"-h", "--help"}:
        return queue_v03.main(arguments)
    if arguments[0] == "materialize":
        return queue_v03.main(arguments)
    try:
        selected = select_validator(arguments)
    except (DispatchError, OSError) as error:
        failure = {
            "schema_version": queue_v03.SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.buffer.write(queue_v03.pretty_bytes(failure))
        return 2
    return selected(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
