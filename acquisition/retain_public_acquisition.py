#!/usr/bin/env python3
"""Retain one exact completed public acquisition in fixed cold storage.

The acquisition CAS deliberately remains operationally writable by its owner, while
``cold_storage_transfer`` accepts only immutable mode-0400 sources.  This adapter
replays one public acquisition work order and completed result, retains the exact CAS
payload descriptor, publishes a separate content-addressed mode-0400 staging inode on
the hot filesystem, and delegates the only cold write to the existing transfer
contract.  It never chmods, renames, links, or deletes the acquisition payload.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

try:
    import acquire
    import cold_storage_transfer as cold
except ModuleNotFoundError:  # Imported as acquisition.retain_public_acquisition.
    from acquisition import acquire  # type: ignore[no-redef]
    from acquisition import cold_storage_transfer as cold  # type: ignore[no-redef]


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MAX_WORK_ORDER_BYTES = 1024 * 1024
FIXED_DESTINATION_ROOT = Path("/mnt/archive/HIMR")


class PublicRetentionError(RuntimeError):
    """The requested object is not an exact completed public acquisition."""


@dataclass(frozen=True)
class RetentionRequest:
    work_order: Path
    acquisition_root: Path
    staging_root: Path
    receipt_root: Path
    expected_work_order_sha256: str
    expected_result_sha256: str
    expected_sha256: str
    expected_byte_count: int
    free_space_floor_bytes: int


@dataclass
class RetainedPublicPayload:
    work_order: dict[str, Any]
    result: dict[str, Any]
    work_order_path: Path
    result_path: Path
    acquisition_root: Path
    payload: acquire.PinnedRegularFile
    acquisition_pins: acquire.PinnedFiles
    work_order_root: cold.RetainedRoot
    work_order_file: acquire.PinnedRegularFile
    result_file: acquire.PinnedRegularFile

    def verify(self) -> None:
        """Replay small controls and recheck the raw payload identity, not its bytes."""

        self.work_order_root.verify()
        self.work_order_file.verify()
        self.result_file.verify()
        self.payload._verify_identity()

    def close(self) -> None:
        self.acquisition_pins.close()
        self.work_order_file.close()
        self.work_order_root.close()


def _absolute_root(path: Path, label: str) -> Path:
    try:
        return cold._validate_absolute_root(path, label)
    except cold.ColdStorageError as error:
        raise PublicRetentionError(str(error)) from error


def _absolute_file_path(path: Path, label: str) -> Path:
    raw = os.fspath(path)
    if (
        not raw
        or len(raw) > 4096
        or "\x00" in raw
        or "\\" in raw
        or "//" in raw
        or not os.path.isabs(raw)
        or raw == "/"
        or os.path.normpath(raw) != raw
    ):
        raise PublicRetentionError(
            f"{label} must be one explicit normalized absolute file path"
        )
    return Path(raw)


def _trees_intersect(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _validate_request(request: RetentionRequest) -> RetentionRequest:
    acquisition_root = _absolute_root(request.acquisition_root, "acquisition root")
    staging_root = _absolute_root(request.staging_root, "staging root")
    receipt_root = _absolute_root(request.receipt_root, "receipt root")
    work_order = _absolute_file_path(request.work_order, "work order")
    if any(
        _trees_intersect(left, right)
        for index, left in enumerate((acquisition_root, staging_root, receipt_root))
        for right in (acquisition_root, staging_root, receipt_root)[index + 1 :]
    ):
        raise PublicRetentionError(
            "acquisition, staging, and receipt roots must be disjoint trees"
        )
    if staging_root in work_order.parents or receipt_root in work_order.parents:
        raise PublicRetentionError(
            "work order must be outside writable staging and receipt trees"
        )
    for path, label in (
        (acquisition_root, "acquisition root"),
        (staging_root, "staging root"),
        (receipt_root, "receipt root"),
        (work_order, "work order"),
    ):
        if path == FIXED_DESTINATION_ROOT or FIXED_DESTINATION_ROOT in path.parents:
            raise PublicRetentionError(f"{label} cannot be inside fixed cold storage")
        if path != work_order and path in FIXED_DESTINATION_ROOT.parents:
            raise PublicRetentionError(f"{label} cannot contain fixed cold storage")
    for digest, label in (
        (request.expected_work_order_sha256, "expected work-order SHA-256"),
        (request.expected_result_sha256, "expected result SHA-256"),
        (request.expected_sha256, "expected payload SHA-256"),
    ):
        if not cold.SHA256_RE.fullmatch(digest):
            raise PublicRetentionError(f"{label} is invalid")
    if (
        isinstance(request.expected_byte_count, bool)
        or not isinstance(request.expected_byte_count, int)
        or not 1 <= request.expected_byte_count <= cold.MAX_BYTE_COUNT
    ):
        raise PublicRetentionError("expected byte count is outside its bound")
    if (
        isinstance(request.free_space_floor_bytes, bool)
        or not isinstance(request.free_space_floor_bytes, int)
        or not 0 <= request.free_space_floor_bytes <= cold.MAX_BYTE_COUNT
        or request.expected_byte_count
        > cold.MAX_BYTE_COUNT - request.free_space_floor_bytes
    ):
        raise PublicRetentionError("free-space floor is outside its bound")
    return RetentionRequest(
        work_order=work_order,
        acquisition_root=acquisition_root,
        staging_root=staging_root,
        receipt_root=receipt_root,
        expected_work_order_sha256=request.expected_work_order_sha256,
        expected_result_sha256=request.expected_result_sha256,
        expected_sha256=request.expected_sha256,
        expected_byte_count=request.expected_byte_count,
        free_space_floor_bytes=request.free_space_floor_bytes,
    )


def _require_private_root(root: cold.RetainedRoot, label: str) -> None:
    info = os.fstat(root.descriptor)
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise PublicRetentionError(
            f"{label} must be current-user-owned with exact mode 0700"
        )


def load_completed_public_payload(
    request: RetentionRequest,
) -> RetainedPublicPayload:
    """Typed-replay and retain the exact acquisition CAS payload."""

    work_order_root: cold.RetainedRoot | None = None
    work_order_file: acquire.PinnedRegularFile | None = None
    result_file: acquire.PinnedRegularFile | None = None
    payload: acquire.PinnedRegularFile | None = None
    acquisition_pins = acquire.PinnedFiles()
    try:
        work_order_root = cold.RetainedRoot.open(
            request.work_order.parent, "work-order parent"
        )
        work_order_file = acquire.PinnedRegularFile.open(
            request.work_order,
            root=request.work_order.parent,
            maximum=MAX_WORK_ORDER_BYTES,
            capture=True,
            label="public acquisition work order",
        )
        work_order_info = work_order_file.initial_stat
        if (
            work_order_file.body is None
            or work_order_file.digest != request.expected_work_order_sha256
            or work_order_info.st_uid != os.geteuid()
            or work_order_info.st_nlink != 1
            or stat.S_IMODE(work_order_info.st_mode)
            not in {0o400, 0o440, 0o444, 0o600, 0o640, 0o644}
        ):
            raise PublicRetentionError(
                "work order must match its reviewed digest and be an owner-owned, "
                "single-link, non-group/world-writable regular file"
            )
        work_order = acquire.validate_work_order(
            acquire.strict_json_object(
                work_order_file.body, "public acquisition work order"
            )
        )
        if (
            work_order["adapter"] not in {"direct_http", "yt_dlp"}
            or work_order["source"]["access_state"] != "public"
        ):
            raise PublicRetentionError(
                "cold retention accepts only completed credential-free public acquisitions"
            )
        if Path(work_order["output"]["root"]) != request.acquisition_root:
            raise PublicRetentionError(
                "work-order output root differs from the reviewed acquisition root"
            )
        work_order_sha256 = acquire.sha256_bytes(acquire.canonical_bytes(work_order))
        result_path = (
            request.acquisition_root
            / "jobs"
            / work_order["job_id"]
            / work_order_sha256
            / "result.json"
        )
        result_file = acquisition_pins.open(
            result_path,
            root=request.acquisition_root,
            maximum=acquire.MAX_DURABLE_RESULT_BYTES,
            capture=True,
            label="completed public acquisition result",
        )
        result_info = result_file.initial_stat
        if (
            result_file.body is None
            or result_file.digest != request.expected_result_sha256
            or result_info.st_uid != os.geteuid()
            or result_info.st_nlink != 1
            or stat.S_IMODE(result_info.st_mode)
            not in {0o400, 0o440, 0o444, 0o600, 0o640, 0o644}
        ):
            raise PublicRetentionError(
                "completed acquisition result does not match its reviewed digest "
                "or has unsafe metadata"
            )
        result = acquire.strict_json_object(
            result_file.body, "completed public acquisition result"
        )
        retained_count_before_replay = len(acquisition_pins.files)
        if not acquire.validate_reusable_result(
            result,
            request.acquisition_root,
            work_order,
            result_path=result_path,
            pins=acquisition_pins,
        ):
            raise PublicRetentionError(
                "acquisition result is absent or fails exact completed-result replay"
            )
        if len(acquisition_pins.files) != retained_count_before_replay + 1:
            raise PublicRetentionError(
                "typed completed-result replay did not retain exactly one payload"
            )
        result_file.verify()
        admission = result["admission"]
        if (
            admission["sha256"] != request.expected_sha256
            or admission["byte_count"] != request.expected_byte_count
        ):
            raise PublicRetentionError(
                "reviewed digest/byte count differ from the completed acquisition"
            )
        payload_path = (
            request.acquisition_root
            / "media"
            / "sha256"
            / request.expected_sha256[:2]
            / request.expected_sha256
            / "payload"
        )
        if admission["path"] != str(payload_path):
            raise PublicRetentionError(
                "completed acquisition does not name the exact CAS payload"
            )
        payload = acquisition_pins.files[-1]
        if payload.path != payload_path:
            raise PublicRetentionError(
                "typed completed-result replay retained an unexpected payload path"
            )
        payload_info = payload.initial_stat
        payload_mode = stat.S_IMODE(payload_info.st_mode)
        if (
            payload.digest != request.expected_sha256
            or payload_info.st_size != request.expected_byte_count
            or payload_info.st_uid != os.geteuid()
            or payload_info.st_nlink != 1
            or payload_mode not in {0o600, 0o640, 0o644}
        ):
            raise PublicRetentionError(
                "acquisition payload metadata or content differs from its completed result"
            )
        retained = RetainedPublicPayload(
            work_order=work_order,
            result=result,
            work_order_path=request.work_order,
            result_path=result_path,
            acquisition_root=request.acquisition_root,
            payload=payload,
            acquisition_pins=acquisition_pins,
            work_order_root=work_order_root,
            work_order_file=work_order_file,
            result_file=result_file,
        )
        retained.verify()
        return retained
    except Exception:
        if payload is not None:
            # Both the result and payload are owned by this retained set.
            acquisition_pins.close()
            payload = None
            result_file = None
        elif result_file is not None:
            acquisition_pins.close()
            result_file = None
        if work_order_file is not None:
            work_order_file.close()
        if work_order_root is not None:
            work_order_root.close()
        raise


class _SingleHashSourceVerifier:
    """Let the staging primitive perform one post-copy payload rehash.

    ``cold._copy_and_publish`` rechecks an already-sealed cold-transfer source twice
    on its new-object path.  The acquisition object is instead protected by its
    initial typed-admission hash, the staging copy digest, and one post-copy rehash.
    Later calls retain descriptor/path identity without rereading the large source.
    """

    def __init__(self, source: acquire.PinnedRegularFile) -> None:
        self.source = source
        self.descriptor = source.descriptor
        self.full_verifications = 0

    def verify(self) -> None:
        if self.full_verifications == 0:
            self.source.verify()
            self.full_verifications = 1
        else:
            self.source._verify_identity()


def seal_staging_payload(
    source: acquire.PinnedRegularFile,
    *,
    staging_root: Path,
    expected_sha256: str,
    expected_byte_count: int,
    publish_fd_noreplace: Callable[[int, str, int, str], None] = cold._link_fd_noreplace,
) -> tuple[Path, str]:
    """Atomically publish or replay one immutable hot staging object."""

    retained_root = cold.RetainedRoot.open(staging_root, "staging root")
    chain = cold.RetainedDirectoryChain(retained_root)
    lock_held = False
    try:
        _require_private_root(retained_root, "staging root")
        try:
            fcntl.flock(retained_root.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_held = True
        except BlockingIOError as error:
            raise PublicRetentionError("another staging operation holds the lock") from error
        parts = ("media", "sha256", expected_sha256[:2], expected_sha256)
        directory_fd = chain.ensure(parts)
        present = cold._entry_exists(directory_fd, "payload")
        verified_source = _SingleHashSourceVerifier(source)
        admission, _verification, _opened = cold._copy_and_publish(
            source=verified_source,
            destination_directory_fd=directory_fd,
            expected_sha256=expected_sha256,
            expected_byte_count=expected_byte_count,
            expected_target_present=present,
            publish_fd_noreplace=publish_fd_noreplace,
            fault_hook=None,
        )
        if verified_source.full_verifications != 1:
            raise PublicRetentionError(
                "staging did not complete its one required source rehash"
            )
        chain.sync()
        source._verify_identity()
        return staging_root.joinpath(*parts, "payload"), admission
    except cold.ColdStorageError as error:
        raise PublicRetentionError(str(error)) from error
    finally:
        if lock_held:
            fcntl.flock(retained_root.descriptor, fcntl.LOCK_UN)
        chain.close()
        retained_root.close()


def run_retention(
    request: RetentionRequest,
    *,
    payload_loader: Callable[[RetentionRequest], RetainedPublicPayload] = load_completed_public_payload,
    transfer_runner: Callable[..., dict[str, Any]] = cold.run_transfer,
) -> dict[str, Any]:
    request = _validate_request(request)
    retained: RetainedPublicPayload | None = None
    receipt_guard: cold.RetainedRoot | None = None
    try:
        receipt_guard = cold.RetainedRoot.open(request.receipt_root, "receipt root")
        _require_private_root(receipt_guard, "receipt root")
        retained = payload_loader(request)
        staging_path, staging_admission = seal_staging_payload(
            retained.payload,
            staging_root=request.staging_root,
            expected_sha256=request.expected_sha256,
            expected_byte_count=request.expected_byte_count,
        )
        retained.verify()
        receipt_guard.verify()
        staging_relative = staging_path.relative_to(request.staging_root)
        transfer_receipt = transfer_runner(
            cold.TransferRequest(
                source_root=request.staging_root,
                source_relative_path=staging_relative,
                destination_root=FIXED_DESTINATION_ROOT,
                receipt_root=request.receipt_root,
                expected_sha256=request.expected_sha256,
                expected_byte_count=request.expected_byte_count,
                free_space_floor_bytes=request.free_space_floor_bytes,
            )
        )
        retained.verify()
        receipt_guard.verify()
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "public_acquisition_cold_retention_result",
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "completed",
            "source": {
                "work_order": str(request.work_order),
                "work_order_sha256": request.expected_work_order_sha256,
                "result": str(retained.result_path),
                "result_sha256": request.expected_result_sha256,
                "payload": str(retained.payload.path),
                "sha256": request.expected_sha256,
                "byte_count": request.expected_byte_count,
                "access_state": "public",
                "unchanged": True,
            },
            "staging": {
                "path": str(staging_path),
                "admission": staging_admission,
                "mode": "0400",
                "single_link": True,
            },
            "cold_transfer_receipt": transfer_receipt,
            "policy": {
                "source_deleted": False,
                "source_mutated": False,
                "catalogue_mutated": False,
                "publication_authority": "none",
                "deletion_authority": "none",
                "archive_scan_performed": False,
            },
        }
    finally:
        if retained is not None:
            retained.close()
        if receipt_guard is not None:
            receipt_guard.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--work-order", type=Path, required=True)
    run.add_argument("--acquisition-root", type=Path, required=True)
    run.add_argument("--staging-root", type=Path, required=True)
    run.add_argument("--receipt-root", type=Path, required=True)
    run.add_argument("--expected-work-order-sha256", required=True)
    run.add_argument("--expected-result-sha256", required=True)
    run.add_argument("--expected-sha256", required=True)
    run.add_argument("--expected-byte-count", type=int, required=True)
    run.add_argument("--free-space-floor-bytes", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = run_retention(
            RetentionRequest(
                work_order=arguments.work_order,
                acquisition_root=arguments.acquisition_root,
                staging_root=arguments.staging_root,
                receipt_root=arguments.receipt_root,
                expected_work_order_sha256=arguments.expected_work_order_sha256,
                expected_result_sha256=arguments.expected_result_sha256,
                expected_sha256=arguments.expected_sha256,
                expected_byte_count=arguments.expected_byte_count,
                free_space_floor_bytes=arguments.free_space_floor_bytes,
            )
        )
        sys.stdout.write(acquire.pretty_json(result))
        return 0
    except (PublicRetentionError, cold.ColdStorageError, acquire.AcquisitionError, OSError) as error:
        sys.stderr.write(
            acquire.pretty_json(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
