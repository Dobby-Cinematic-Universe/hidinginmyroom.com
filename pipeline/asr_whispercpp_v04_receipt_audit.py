#!/usr/bin/env python3
"""Read-only, restart-portable audit of one historical v0.4 queue receipt.

This compatibility lane is intentionally narrow.  It accepts only schema-v2
receipts produced by the exact retained v0.4 result sealer from an exact v0.2
preprocess-ASR queue.  It offers no plan, apply, chmod, import, ASR, catalog-write,
publication, network, or identity operation.

The original v0.4 validator treated persisted filesystem object numbers as durable
authority.  ``st_dev`` can change after a clean reboot/remount even when the exact
path, inode, bytes, tree, modes, and hashes remain unchanged.  This auditor reuses a
hash-pinned copy of the v0.5 read-only replay logic, which treats persisted device,
inode, ctime, and mtime values as diagnostics while retaining live descriptor/path
object identity and metadata checks throughout each audit operation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import types
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator


# The compatibility command is read-only, including for dynamically imported local
# validators.  Do not let Python create or refresh bytecode caches during an audit.
sys.dont_write_bytecode = True

AUDITOR_NAME = "himr-asr-whispercpp-v04-receipt-audit"
IMPLEMENTATION_VERSION = "0.1.0"
PIPELINE_ROOT = Path(__file__).resolve().parent
REPOSITORY_ROOT = PIPELINE_ROOT.parent
CORPUS_SOURCE_ROOT = REPOSITORY_ROOT / "corpus" / "src"

V05_HELPER_SOURCE = PIPELINE_ROOT / "asr_whispercpp_result_store_seal_v05.py"
V04_SEAL_SOURCE = PIPELINE_ROOT / "asr_whispercpp_result_store_seal.py"
V02_QUEUE_SOURCE = PIPELINE_ROOT / "preprocess_asr_queue.py"
V02_QUEUE_SCHEMA = (
    PIPELINE_ROOT / "schemas" / "preprocess-asr-queue-manifest.schema.json"
)
IMPORTER_SOURCE = CORPUS_SOURCE_ROOT / "himr_corpus" / "asr_result_importer.py"

EXPECTED_FILES: dict[Path, tuple[int, str]] = {
    V05_HELPER_SOURCE: (
        126_605,
        "a5c988f02d5f65246639e07ed36c0141fd880814703000f145f275074fbcdaa2",
    ),
    V04_SEAL_SOURCE: (
        126_781,
        "0e2d05652e3b8bde279d22e20b97f630dd6669c42c82eed8369f089b74701d90",
    ),
    V02_QUEUE_SOURCE: (
        86_523,
        "e0fffe5af2f403cd46fff82bde452f81fabb1d165f82dffcb052206d00b0fe87",
    ),
    V02_QUEUE_SCHEMA: (
        31_540,
        "f8b5f1debca171b5dd58f2d5e6bcc007489e389d2e2617c581bcff0eaabfdffa",
    ),
    IMPORTER_SOURCE: (
        94_217,
        "77607428cd10aac794bb3d913075a07f34521513d10b2fe45cb295f7dae30197",
    ),
}

MAX_IMPLEMENTATION_BYTES = 2 * 1024 * 1024
DIAGNOSTIC_FIELDS = (
    "device",
    "inode",
    "ctime_ns_before",
    "ctime_ns_after",
    "mtime_ns",
)


class CompatibilityAuditError(RuntimeError):
    """The receipt is outside the exact historical compatibility contract."""


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _stable_verified_body(path: Path, expected_size: int, expected_sha256: str) -> bytes:
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise CompatibilityAuditError(f"implementation path is not resolved: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise CompatibilityAuditError(
            f"cannot open compatibility implementation {path}: {error}"
        ) from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size != expected_size
            or before.st_size > MAX_IMPLEMENTATION_BYTES
        ):
            raise CompatibilityAuditError(
                f"compatibility implementation size/type differs: {path}"
            )
        chunks: list[bytes] = []
        remaining = expected_size
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
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise CompatibilityAuditError(
            f"compatibility implementation changed while retained: {path}"
        )
    if len(body) != expected_size or _sha256(body) != expected_sha256:
        raise CompatibilityAuditError(
            f"compatibility implementation digest differs: {path}"
        )
    return body


def _file_identity(path: Path) -> dict[str, Any]:
    expected_size, expected_sha256 = EXPECTED_FILES[path]
    body = _stable_verified_body(path, expected_size, expected_sha256)
    return {
        "byte_count": len(body),
        "path": str(path),
        "sha256": _sha256(body),
    }


@contextmanager
def _load_verified_module(
    name: str, path: Path, expected_size: int, expected_sha256: str
) -> Iterator[types.ModuleType]:
    body = _stable_verified_body(path, expected_size, expected_sha256)
    code = compile(
        body,
        str(path),
        "exec",
        flags=0,
        dont_inherit=True,
        optimize=0,
    )
    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__package__ = ""
    module.__loader__ = None
    module.__spec__ = None
    missing = object()
    previous = sys.modules.get(name, missing)
    sys.modules[name] = module
    try:
        exec(code, module.__dict__)
        # Do not permit even an exact-byte dependency to replace its temporary
        # import identity while initializing.
        sys.modules[name] = module
        yield module
    finally:
        if previous is missing:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def _assert_allowlisted_plan(plan: dict[str, Any]) -> None:
    if plan.get("schema_version") != 2:
        raise CompatibilityAuditError(
            "compatibility audit accepts queue-only seal schema v2 receipts only"
        )
    if plan.get("implementation") != _file_identity(V04_SEAL_SOURCE):
        raise CompatibilityAuditError(
            "seal plan is not bound to the exact retained v0.4 implementation"
        )
    if plan.get("importer_validator") != _file_identity(IMPORTER_SOURCE):
        raise CompatibilityAuditError(
            "seal plan importer validator is outside the exact allowlist"
        )
    authority = plan.get("source_authority")
    if not isinstance(authority, dict) or authority.get("mode") != "queue_only":
        raise CompatibilityAuditError("seal plan is not queue-only authority")
    contract = authority.get("queue_contract")
    if not isinstance(contract, dict):
        raise CompatibilityAuditError("seal plan queue contract is missing")
    expected = {
        "manifest_schema_version": 1,
        "materializer": "himr-preprocess-asr-queue",
        "materializer_implementation_version": "0.2.0",
        "queue_manifest_schema": _file_identity(V02_QUEUE_SCHEMA),
        "queue_validator": _file_identity(V02_QUEUE_SOURCE),
    }
    for key, value in expected.items():
        if contract.get(key) != value:
            raise CompatibilityAuditError(
                f"seal plan queue contract {key} is outside the exact v0.2 allowlist"
            )


def _catalog_validator(result_path: Path) -> dict[str, Any]:
    _file_identity(IMPORTER_SOURCE)
    source_text = str(CORPUS_SOURCE_ROOT)
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
    from himr_corpus import asr_result_importer

    if Path(asr_result_importer.__file__).resolve() != IMPORTER_SOURCE:
        raise CompatibilityAuditError("unexpected catalog-free importer module path")
    _file_identity(IMPORTER_SOURCE)
    return asr_result_importer.validate_asr_whispercpp_result_file(result_path)


def _readonly_lock_retainer(helper: types.ModuleType) -> Callable[..., int]:
    def retain(stack: Any, control_root: Path, *, acquire: bool) -> int:
        if acquire:
            raise CompatibilityAuditError(
                "compatibility auditor cannot acquire an apply lock"
            )
        path = helper._require_absolute_resolved(
            control_root / helper.APPLY_LOCK_FILENAME,
            "seal apply lock",
        )
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
            raise CompatibilityAuditError(
                f"cannot retain seal apply lock read-only: {error}"
            ) from error
        stack.callback(os.close, descriptor)
        opened = os.fstat(descriptor)
        logical = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(logical.st_mode)
            or opened.st_dev != logical.st_dev
            or opened.st_ino != logical.st_ino
            or opened.st_nlink != 1
            or logical.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != helper.APPLY_LOCK_FILE_MODE
            or stat.S_IMODE(logical.st_mode) != helper.APPLY_LOCK_FILE_MODE
        ):
            raise CompatibilityAuditError(
                "seal apply lock must remain one retained mode-0600 file"
            )
        return descriptor

    return retain


def validate_receipt(
    receipt_path: Path,
    *,
    validator: Callable[[Path], dict[str, Any]] | None = None,
    queue_validator: Callable[
        [Path], tuple[dict[str, Any], list[dict[str, Any]]]
    ]
    | None = None,
) -> dict[str, Any]:
    """Audit one exact historical receipt without changing any filesystem state."""

    for path in EXPECTED_FILES:
        _file_identity(path)

    helper_size, helper_sha256 = EXPECTED_FILES[V05_HELPER_SOURCE]
    queue_size, queue_sha256 = EXPECTED_FILES[V02_QUEUE_SOURCE]
    with _load_verified_module(
        "_himr_v04_receipt_audit_v05_helper",
        V05_HELPER_SOURCE,
        helper_size,
        helper_sha256,
    ) as helper, _load_verified_module(
        "_himr_v04_receipt_audit_v02_queue",
        V02_QUEUE_SOURCE,
        queue_size,
        queue_sha256,
    ) as queue:
        # Patch only private, exact-byte module instances created above. No shared
        # production module or historical source is modified.
        helper.PREPROCESS_QUEUE_SOURCE = V02_QUEUE_SOURCE
        helper.PREPROCESS_QUEUE_SCHEMA_SOURCE = V02_QUEUE_SCHEMA
        helper._verify_implementation = _assert_allowlisted_plan
        helper._retain_apply_lock = _readonly_lock_retainer(helper)

        selected_validator = validator if validator is not None else _catalog_validator
        selected_queue_validator = (
            queue_validator if queue_validator is not None else queue.validate_queue
        )
        try:
            result = helper.validate_receipt(
                receipt_path,
                validator=selected_validator,
                queue_validator=selected_queue_validator,
            )
        except CompatibilityAuditError:
            raise
        except Exception as error:
            raise CompatibilityAuditError(str(error)) from error

    return {
        "schema_version": 1,
        "status": "validated",
        "state": "valid_applied_receipt_restart_portable_audit",
        "auditor": AUDITOR_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "receipt_path": str(receipt_path),
        "receipt_id": result["receipt_id"],
        "result_count": result["result_count"],
        "historical_contract": {
            "result_sealer": _file_identity(V04_SEAL_SOURCE),
            "queue_validator": _file_identity(V02_QUEUE_SOURCE),
            "queue_schema": _file_identity(V02_QUEUE_SCHEMA),
        },
        "cross_restart_diagnostic_fields": list(DIAGNOSTIC_FIELDS),
        "authority": {
            "asr_execution": False,
            "catalog_import": False,
            "database_writes": False,
            "filesystem_writes": False,
            "identity_authority": "none",
            "network_access": "none",
            "publication_authority": "none",
        },
    }


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or "\x00" in value:
        raise argparse.ArgumentTypeError("receipt path must be absolute without NUL")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=_absolute_path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = validate_receipt(args.receipt)
    except (CompatibilityAuditError, OSError, ValueError) as error:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.write(json.dumps(failure, ensure_ascii=False, indent=2) + "\n")
        return 2
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
