"""Adapters from the scheduler to the existing sealed finite-stage mechanisms.

This module contains no shell, catalogue, publication, identity, or deletion
interface.  Each method performs at most the bound fixed in the sealed controller
config.  GPU inference is admitted only through the exact local-private trusted
launcher in a separately bounded systemd user service.
"""

from __future__ import annotations

import fcntl
import importlib
import json
import os
import re
import stat
import sys
import threading
import time
import weakref
from bisect import bisect_left
from contextlib import contextmanager
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Sequence

from .config import FIXED_COLD_ROOT, ControllerConfig, canonical_bytes, sha256_bytes
from .acquisition_retry_proof import (
    install_adapter as install_acquisition_retry_proof_adapter,
    verify_completion as verify_acquisition_retry_completion,
)
from .cold_mount_migrations import reviewed_cold_mount_transition
from . import gpu_runtime_successor
from .controller import (
    StageOutcome,
    StartupRestoreStopRequested,
    TRANSIENT_PEER_RUNTIME_ARTIFACT,
)
from .gpu_child import (
    ControllerUnitContext,
    GpuChildRecord,
    LocalPrivateGpuLaunchSpec,
    PrivateGpuChildJournal,
    SystemdGpuChildExecutor,
)
from .operational_replay import (
    CheckpointDeferred,
    DeepAuditRequired,
    OperationalReplayError,
    OperationalReplayStore,
    QueueBinding,
)
from .preprocess_stop import (
    PreprocessStopBoundary,
    PreprocessStopError,
    PreprocessStopObservation,
    preprocess_stop_gate,
)
from .state import read_control_state


BACKEND_KIND = "himr_sealed_archive_backend_v1"
BACKEND_CHECKPOINT_KIND = "himr_sealed_archive_backend_restart_checkpoint"
BACKEND_CHECKPOINT_SCHEMA_VERSION = 2
BACKEND_CHECKPOINT_MAX_FILES_PER_CANDIDATE = 2_048
BACKEND_CHECKPOINT_MAX_FILES_PER_GPU_RECORD = 256
BACKEND_CHECKPOINT_SMALL_FILE_BYTES = 16 * 1024 * 1024
BACKEND_CHECKPOINT_MAX_ROWS = 1_000_000
CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON = "owner_single_link_json"
CHECKPOINT_READ_PREPROCESS_JSON_HARDLINKS = (
    "preprocess_readonly_json_allow_hardlinks"
)
CHECKPOINT_READ_FINGERPRINT_ONLY = "fingerprint_only"
CHECKPOINT_READ_POLICIES = frozenset(
    {
        CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON,
        CHECKPOINT_READ_PREPROCESS_JSON_HARDLINKS,
        CHECKPOINT_READ_FINGERPRINT_ONLY,
    }
)
BACKEND_RESTART_POLICY = {
    "recovery_authority": "checkpoint_plus_immutable_journal_tail",
    "first_migration": "metadata_only_tofu_from_canonical_envelopes",
    "unchanged_media_validation": "filesystem_uuid_and_metadata_witness",
    "changed_media_validation": "targeted_exact_replay",
    "deep_audit": "explicit_operator_operation",
    "publication_authority": "none",
    "deletion_authority": "none",
}
INVENTORY_SCOPES = frozenset(
    {
        "all_known_public_archive_org_items_in_sealed_2026_08_29_inventory",
        "all_known_public_archive_org_items_in_sealed_2026_08_30_inventory",
        "public_archive_org_collection_addendum_in_sealed_2026_08_30_inventory",
        "exact_public_archive_item_incremental_inventory_v1",
    }
)
GPU_PACK_RECORD_FORMAT = "cross_queue_pack_v1"
GPU_UUID_RE = re.compile(r"GPU-[A-Za-z0-9-]{8,92}\Z")
# A full model load is expensive compared with sealing a few more immutable work
# orders. Hold a short tail until it reaches this size, reaches the profile's
# preferred audio duration, the upstream is drained, or its oldest buffered epoch
# has waited one minute. This amortizes model startup without starving a slow
# producer whose candidate set changes every few seconds.
MIN_GPU_PACK_ITEMS = 16
GPU_PARTIAL_HOLD_SECONDS = 60.0
COLD_MOUNT = Path("/mnt/archive")
EXPECTED_COLD_MOUNT_FSTYPE = "xfs"
EXPECTED_COLD_MOUNT_UUID = "5b5813ad-b1a4-4f52-9960-e762ceac5636"
_TEMPORAL_PREPROCESS_RECEIPT_CONFLICT = (
    "preprocess receipt acknowledges a non-completed queue result"
)
_TEMPORAL_PREPROCESS_RECEIPT_ENTRY = (
    "preprocess receipts directory has an unsupported entry"
)
# Receipt publication deliberately exposes one reserved temporary directory
# entry until its immutable final hard link has been fsynced.  Independent
# acquisition can observe that entry during an otherwise valid read-only
# snapshot. Retry only the producer's exact error, with less than two seconds of
# retry delay; a persistent or differently malformed entry must still fail closed.
PREPROCESS_RECEIPT_SNAPSHOT_ATTEMPTS = 64
PREPROCESS_RECEIPT_SNAPSHOT_RETRY_SECONDS = 0.025
# Queue validation deliberately compares two complete state projections.  A peer
# acquisition lane may atomically publish a completed result, failure, or
# quarantine between those projections.  That exact transition is safe to replay
# because schedule runtime loading is read-only; keep the retry local and bounded
# so every other queue error remains fail-closed.
QUEUE_RUNTIME_SNAPSHOT_ATTEMPTS = 64
QUEUE_RUNTIME_SNAPSHOT_RETRY_SECONDS = 0.025
_QUEUE_RUNTIME_STATE_ADMISSION_RACE = (
    "completed/pending/quarantine state changed during offline validation"
)
# Version-1 acquisition results use whole-second wall timestamps.  Their strict
# validator therefore permits two seconds of quantization error.  A legacy writer
# sampled ``completed_at`` before final cleanup/capacity accounting but sampled
# ``duration_ms`` afterwards.  The pinned result file's creation mtime is the
# independent end-of-write witness used by the compatibility boundary below.
ACQUISITION_RESULT_TIME_WITNESS_TOLERANCE_MS = 2_000
ACQUISITION_LEGACY_FINALIZATION_WINDOW_MS = 10_000
ACQUISITION_LEGACY_VERSION = "0.3.2"
ACQUISITION_LEGACY_SHA256 = (
    "de7eaa811232aaeecc1bf53f94904aa21753dd8d7e2e35cc0b52033927732d47"
)
QUEUE_RUNNER_LEGACY_VERSION = "0.2.0"
QUEUE_RUNNER_LEGACY_SHA256 = (
    "99dc1311c7cbb960ede2db1fe5c12ee19944915d9cfc90f12f57ddbc1c2d832c"
)

_PREPROCESS_RECEIPT_ADAPTER_LOCK = threading.Lock()
_PREPROCESS_RECEIPT_ADAPTERS: "weakref.WeakKeyDictionary[ModuleType, tuple[Any, Any]]" = (
    weakref.WeakKeyDictionary()
)
_ACQUISITION_RESULT_ADAPTER_LOCK = threading.Lock()
_ACQUISITION_RESULT_ADAPTERS: (
    "weakref.WeakKeyDictionary[ModuleType, tuple[Any, Any, Any, Any]]"
) = weakref.WeakKeyDictionary()
_ACQUISITION_DIRECTORY_IDENTITY_ADAPTER_LOCK = threading.Lock()
_ACQUISITION_DIRECTORY_IDENTITY_ADAPTERS: (
    "weakref.WeakKeyDictionary[ModuleType, tuple[Any, Any]]"
) = weakref.WeakKeyDictionary()


class BackendError(RuntimeError):
    """A sealed stage artifact or exact replay failed closed."""


def _checkpoint_fingerprint(path: Path, *, label: str) -> dict[str, Any]:
    """Return a cheap restart witness for one already-validated regular file.

    The checkpoint is bound separately to the reviewed filesystem identity.  The
    leaf witness therefore omits ``st_dev`` (which is not reboot-stable) while
    retaining every unprivileged mutation signal, most importantly ``ctime_ns``.
    A mismatch never blesses new bytes: incremental restore sends only that item
    through its unchanged deep validator.
    """

    path = Path(os.path.abspath(os.fspath(path)))
    descriptor = -1
    try:
        linked = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
    except OSError as error:
        if descriptor >= 0:
            os.close(descriptor)
        raise BackendError(f"cannot inspect {label}: {error}") from error
    try:
        if (
            stat.S_ISLNK(linked.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or (linked.st_dev, linked.st_ino, linked.st_size, linked.st_mode)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mode)
        ):
            raise BackendError(f"{label} is not one stable no-follow regular file")
        projection = {
            "path": str(path),
            "inode": opened.st_ino,
            "byte_count": opened.st_size,
            "mtime_ns": opened.st_mtime_ns,
            "ctime_ns": opened.st_ctime_ns,
            "mode": opened.st_mode,
            "link_count": opened.st_nlink,
            "uid": opened.st_uid,
        }
    finally:
        os.close(descriptor)
    return projection


def _validate_checkpoint_fingerprint(value: Any, *, label: str) -> dict[str, Any]:
    keys = {
        "path",
        "inode",
        "byte_count",
        "mtime_ns",
        "ctime_ns",
        "mode",
        "link_count",
        "uid",
    }
    if not isinstance(value, dict) or set(value) != keys:
        raise BackendError(f"{label} restart witness is malformed")
    path = value["path"]
    if (
        not isinstance(path, str)
        or not path
        or "\x00" in path
        or not Path(path).is_absolute()
        or any(
            isinstance(value[key], bool)
            or not isinstance(value[key], int)
            or value[key] < 0
            for key in keys - {"path"}
        )
        or value["byte_count"] < 1
        or value["link_count"] < 1
    ):
        raise BackendError(f"{label} restart witness is invalid")
    return dict(value)


def _checkpoint_fingerprint_matches(value: Any, *, label: str) -> bool:
    expected = _validate_checkpoint_fingerprint(value, label=label)
    try:
        return _checkpoint_fingerprint(Path(expected["path"]), label=label) == expected
    except BackendError:
        return False


@dataclass(frozen=True)
class _GpuOpportunityLease:
    """One retained controller-side claim on an ordinary GPU opportunity."""

    descriptor: int
    gpu_uuid: str
    path: Path
    root_identity: tuple[int, int]
    file_identity: tuple[int, int]


@dataclass(frozen=True)
class _Modules:
    background: ModuleType
    queue_runner: ModuleType
    handoff: ModuleType
    preprocess_batch: ModuleType
    gpu_queue: ModuleType
    gpu_bridge: ModuleType
    retention: ModuleType
    schedule_set: ModuleType
    composite_schedule_set: ModuleType


def _install_acquisition_result_finalization_adapter(
    queue_runner: ModuleType,
) -> None:
    """Correct and validate one legacy post-completion timing defect.

    The active schedules pin the acquisition and queue source bytes, so changing
    either producer would invalidate every schedule.  For new results this
    controller-owned adapter moves the completion stamp to the atomic-write
    boundary.  For an existing legacy result it preserves the durable bytes and
    relaxes no payload or envelope field: when positive finalization drift is
    witnessed by the already-pinned result-file mtime, only an in-memory copy of
    ``duration_ms`` is normalized before the original validator runs.
    """

    if not isinstance(queue_runner, ModuleType):
        raise BackendError("acquisition result adapter requires an imported queue module")
    acquire = getattr(queue_runner, "acquire", None)
    if not isinstance(acquire, ModuleType):
        raise BackendError("queue module lacks its imported acquisition module")
    try:
        acquire_path = Path(getattr(acquire, "__file__", "")).resolve(strict=True)
        runner_path = Path(getattr(queue_runner, "__file__", "")).resolve(strict=True)
        acquire_sha256 = sha256_bytes(acquire_path.read_bytes())
        runner_sha256 = sha256_bytes(runner_path.read_bytes())
    except (OSError, TypeError) as error:
        raise BackendError("cannot bind acquisition result adapter to source files") from error
    if (
        getattr(acquire, "IMPLEMENTATION_VERSION", None) != ACQUISITION_LEGACY_VERSION
        or acquire_sha256 != ACQUISITION_LEGACY_SHA256
        or getattr(queue_runner, "IMPLEMENTATION_VERSION", None)
        != QUEUE_RUNNER_LEGACY_VERSION
        or runner_sha256 != QUEUE_RUNNER_LEGACY_SHA256
    ):
        # A later sealed producer must define its own result contract.  Never
        # carry this compatibility behavior onto unreviewed source bytes.
        return
    with _ACQUISITION_RESULT_ADAPTER_LOCK:
        current_validator = getattr(acquire, "validate_reusable_result", None)
        current_writer = getattr(acquire, "atomic_write_json", None)
        installed = _ACQUISITION_RESULT_ADAPTERS.get(acquire)
        if installed is not None:
            (
                _original_validator,
                validator_wrapper,
                _original_writer,
                writer_wrapper,
            ) = installed
            if (
                current_validator is not validator_wrapper
                or current_writer is not writer_wrapper
            ):
                raise BackendError("acquisition result finalization adapter was replaced")
            return
        if getattr(
            current_validator,
            "__himr_acquisition_result_finalization_adapter__",
            None,
        ) or getattr(
            current_writer,
            "__himr_acquisition_result_finalization_adapter__",
            None,
        ):
            raise BackendError("acquisition result finalization adapter is unregistered")
        parse_timestamp = getattr(acquire, "_result_timestamp", None)
        utc_timestamp = getattr(acquire, "utc_now", None)
        if (
            not callable(current_validator)
            or not callable(current_writer)
            or not callable(parse_timestamp)
            or not callable(utc_timestamp)
        ):
            raise BackendError("acquisition module lacks result validation primitives")
        original_validator = current_validator
        original_writer = current_writer

        def write_with_final_completion(path: Path, value: Any) -> None:
            if (
                not isinstance(value, dict)
                or value.get("schema_version") != 1
                or value.get("status") != "completed"
                or value.get("dry_run") is not False
            ):
                original_writer(path, value)
                return
            try:
                if Path(value["result_path"]) != Path(path):
                    raise BackendError("completed acquisition result path is inconsistent")
                started = parse_timestamp(value["started_at"])
                prior_completed = parse_timestamp(value["completed_at"])
                duration_ms = value["duration_ms"]
                records = value["catalog_records"]
                timestamp_fields = (
                    (records["sources"][0], "observed_at"),
                    (records["sources"][0], "created_at"),
                    (records["sources"][0], "updated_at"),
                    (records["media_objects"][0], "first_cataloged_at"),
                    (records["media_locations"][0], "verified_at"),
                    (records["media_sources"][0], "retrieved_at"),
                )
            except (KeyError, IndexError, TypeError) as error:
                raise BackendError(
                    "completed acquisition result lacks finalization primitives"
                ) from error
            if (
                not isinstance(started, datetime)
                or not isinstance(prior_completed, datetime)
                or isinstance(duration_ms, bool)
                or not isinstance(duration_ms, int)
                or duration_ms < 0
                or any(
                    not isinstance(record, dict)
                    or record.get(field) != value["completed_at"]
                    for record, field in timestamp_fields
                )
            ):
                raise BackendError("completed acquisition finalization state is malformed")
            completed_at = utc_timestamp()
            completed = parse_timestamp(completed_at)
            if not isinstance(completed, datetime) or completed < prior_completed:
                raise BackendError("completed acquisition wall clock moved backwards")
            wall_duration_ms = round((completed - started).total_seconds() * 1_000)
            if (
                abs(duration_ms - wall_duration_ms)
                > ACQUISITION_RESULT_TIME_WITNESS_TOLERANCE_MS
            ):
                raise BackendError(
                    "completed acquisition duration disagrees at the write boundary"
                )
            value["completed_at"] = completed_at
            for record, field in timestamp_fields:
                record[field] = completed_at
            original_writer(path, value)

        def validate_with_finalization_witness(
            result: dict[str, Any],
            output_root: Path,
            work_order: dict[str, Any],
            *,
            result_path: Path | None = None,
            pins: Any | None = None,
        ) -> bool:
            # Normal results, malformed inputs, direct/non-pinned validation, and
            # negative clock discrepancies retain the exact source validator.
            try:
                duration_ms = result["duration_ms"]
                started = parse_timestamp(result["started_at"])
                completed = parse_timestamp(result["completed_at"])
            except (KeyError, TypeError):
                return original_validator(
                    result,
                    output_root,
                    work_order,
                    result_path=result_path,
                    pins=pins,
                )
            if (
                isinstance(duration_ms, bool)
                or not isinstance(duration_ms, int)
                or duration_ms < 0
                or not isinstance(started, datetime)
                or not isinstance(completed, datetime)
                or completed < started
            ):
                return original_validator(
                    result,
                    output_root,
                    work_order,
                    result_path=result_path,
                    pins=pins,
                )
            wall_duration_ms = round((completed - started).total_seconds() * 1_000)
            finalization_drift_ms = duration_ms - wall_duration_ms
            if finalization_drift_ms <= ACQUISITION_RESULT_TIME_WITNESS_TOLERANCE_MS:
                return original_validator(
                    result,
                    output_root,
                    work_order,
                    result_path=result_path,
                    pins=pins,
                )
            if result_path is None or pins is None:
                return original_validator(
                    result,
                    output_root,
                    work_order,
                    result_path=result_path,
                    pins=pins,
                )

            expected_path = Path(os.path.abspath(os.fspath(result_path)))
            pinned_results = [
                pinned
                for pinned in getattr(pins, "files", ())
                if getattr(pinned, "path", None) == expected_path
                and getattr(pinned, "label", None) == "durable acquisition result"
            ]
            if len(pinned_results) != 1:
                return False
            initial_stat = getattr(pinned_results[0], "initial_stat", None)
            mtime_ns = getattr(initial_stat, "st_mtime_ns", None)
            if (
                isinstance(mtime_ns, bool)
                or not isinstance(mtime_ns, int)
                or not stat.S_ISREG(getattr(initial_stat, "st_mode", 0))
                or getattr(initial_stat, "st_nlink", 0) != 1
                or getattr(initial_stat, "st_uid", -1) != os.getuid()
                or stat.S_IMODE(getattr(initial_stat, "st_mode", 0)) & 0o022
            ):
                return False
            result_written_at = datetime.fromtimestamp(
                mtime_ns / 1_000_000_000, tz=timezone.utc
            )
            witnessed_duration_ms = round(
                (result_written_at - started).total_seconds() * 1_000
            )
            finalization_window_ms = round(
                (result_written_at - completed).total_seconds() * 1_000
            )
            if (
                witnessed_duration_ms < wall_duration_ms
                or finalization_window_ms < 0
                or finalization_window_ms > ACQUISITION_LEGACY_FINALIZATION_WINDOW_MS
                or abs(duration_ms - witnessed_duration_ms)
                > ACQUISITION_RESULT_TIME_WITNESS_TOLERANCE_MS
            ):
                return False

            normalized = dict(result)
            normalized["duration_ms"] = wall_duration_ms
            return original_validator(
                normalized,
                output_root,
                work_order,
                result_path=result_path,
                pins=pins,
            )

        validate_with_finalization_witness.__name__ = (
            "himr_validate_reusable_result_with_finalization_witness_v1"
        )
        validate_with_finalization_witness.__himr_acquisition_result_finalization_adapter__ = True  # type: ignore[attr-defined]
        validate_with_finalization_witness.__wrapped__ = original_validator  # type: ignore[attr-defined]
        write_with_final_completion.__name__ = (
            "himr_atomic_write_json_with_final_completion_v1"
        )
        write_with_final_completion.__himr_acquisition_result_finalization_adapter__ = True  # type: ignore[attr-defined]
        write_with_final_completion.__wrapped__ = original_writer  # type: ignore[attr-defined]
        setattr(acquire, "validate_reusable_result", validate_with_finalization_witness)
        setattr(acquire, "atomic_write_json", write_with_final_completion)
        _ACQUISITION_RESULT_ADAPTERS[acquire] = (
            original_validator,
            validate_with_finalization_witness,
            original_writer,
            write_with_final_completion,
        )


def _install_acquisition_directory_identity_adapter(
    queue_runner: ModuleType,
    *,
    required: bool = False,
) -> bool:
    """Allow sibling publication without weakening pinned descendant identity.

    The sealed legacy acquisition source uses one strict stat fingerprint for
    both regular files and every ancestor directory.  Directory size, mtime,
    ctime, and link count legitimately change when the parallel acquisition lane
    publishes a sibling job, so a concurrent preprocess replay can otherwise
    reject an unchanged result at the shared ``jobs`` component.  Keep the
    SHA-pinned producer bytes unchanged and adapt only directory comparisons to
    stable path/access-control identity.  Regular-file fingerprints remain the
    exact legacy function.
    """

    if not isinstance(required, bool):
        raise BackendError("acquisition directory adapter required flag must be boolean")
    if not isinstance(queue_runner, ModuleType):
        raise BackendError("acquisition directory adapter requires an imported queue module")
    acquire = getattr(queue_runner, "acquire", None)
    if not isinstance(acquire, ModuleType):
        raise BackendError("queue module lacks its imported acquisition module")
    try:
        acquire_path = Path(getattr(acquire, "__file__", "")).resolve(strict=True)
        runner_path = Path(getattr(queue_runner, "__file__", "")).resolve(strict=True)
        acquire_sha256 = sha256_bytes(acquire_path.read_bytes())
        runner_sha256 = sha256_bytes(runner_path.read_bytes())
    except (OSError, TypeError) as error:
        raise BackendError("cannot bind acquisition directory adapter to source files") from error
    if (
        getattr(acquire, "IMPLEMENTATION_VERSION", None) != ACQUISITION_LEGACY_VERSION
        or acquire_sha256 != ACQUISITION_LEGACY_SHA256
        or getattr(queue_runner, "IMPLEMENTATION_VERSION", None)
        != QUEUE_RUNNER_LEGACY_VERSION
        or runner_sha256 != QUEUE_RUNNER_LEGACY_SHA256
    ):
        if required:
            raise BackendError(
                "required acquisition directory adapter source binding differs "
                "from the reviewed legacy implementation"
            )
        return False
    with _ACQUISITION_DIRECTORY_IDENTITY_ADAPTER_LOCK:
        current = getattr(acquire, "_stat_fingerprint", None)
        installed = _ACQUISITION_DIRECTORY_IDENTITY_ADAPTERS.get(acquire)
        if installed is not None:
            _original, wrapper = installed
            if current is not wrapper:
                raise BackendError("acquisition directory identity adapter was replaced")
            return True
        if getattr(current, "__himr_acquisition_directory_identity_adapter__", None):
            raise BackendError("acquisition directory identity adapter is unregistered")
        if not callable(current):
            raise BackendError("acquisition module lacks its stat fingerprint primitive")
        code = getattr(current, "__code__", None)
        try:
            current_path = Path(code.co_filename).resolve(strict=True)
        except (AttributeError, OSError, TypeError) as error:
            raise BackendError(
                "acquisition stat fingerprint primitive is not the reviewed source binding"
            ) from error
        if (
            getattr(current, "__name__", None) != "_stat_fingerprint"
            or current_path != acquire_path
        ):
            raise BackendError(
                "acquisition stat fingerprint primitive is not the reviewed source binding"
            )
        original = current

        def stable_path_identity(value: os.stat_result) -> tuple[Any, ...]:
            if stat.S_ISDIR(value.st_mode):
                return (
                    "directory_path_identity_v1",
                    value.st_dev,
                    value.st_ino,
                    value.st_mode,
                    value.st_uid,
                    value.st_gid,
                )
            return original(value)

        stable_path_identity.__name__ = (
            "himr_acquisition_stable_directory_path_identity_v1"
        )
        stable_path_identity.__himr_acquisition_directory_identity_adapter__ = True  # type: ignore[attr-defined]
        stable_path_identity.__wrapped__ = original  # type: ignore[attr-defined]
        setattr(acquire, "_stat_fingerprint", stable_path_identity)
        _ACQUISITION_DIRECTORY_IDENTITY_ADAPTERS[acquire] = (
            original,
            stable_path_identity,
        )
        return True


def _install_preprocess_receipt_snapshot_adapter(background: ModuleType) -> None:
    """Retry only a read-only receipt scan which meets the exact transient error.

    The SHA-pinned background producer remains byte-for-byte unchanged.  This
    controller-owned adapter is installed around its private receipt enumerator so
    calls made inside both ``_load_runtime`` and the mutating ``run_producer`` use
    the same stable-snapshot boundary.  It never retries ``run_producer`` itself.
    """

    if not isinstance(background, ModuleType):
        raise BackendError("background producer adapter requires an imported module")
    with _PREPROCESS_RECEIPT_ADAPTER_LOCK:
        current = getattr(background, "_preprocess_receipt_paths", None)
        installed = _PREPROCESS_RECEIPT_ADAPTERS.get(background)
        if installed is not None:
            _original, wrapper = installed
            if current is not wrapper:
                raise BackendError(
                    "background preprocess receipt snapshot adapter was replaced"
                )
            return
        if getattr(current, "__himr_preprocess_receipt_snapshot_adapter__", None):
            raise BackendError(
                "background preprocess receipt snapshot adapter is unregistered"
            )
        error_type = getattr(background, "BackgroundProducerError", None)
        if (
            not callable(current)
            or not isinstance(error_type, type)
            or not issubclass(error_type, Exception)
        ):
            raise BackendError(
                "background producer lacks receipt snapshot adapter primitives"
            )
        original = current

        def stable_receipt_paths(root: Path) -> list[Path]:
            for attempt in range(PREPROCESS_RECEIPT_SNAPSHOT_ATTEMPTS):
                try:
                    return original(root)
                except Exception as error:
                    if (
                        type(error) is not error_type
                        or str(error) != _TEMPORAL_PREPROCESS_RECEIPT_ENTRY
                        or attempt + 1 == PREPROCESS_RECEIPT_SNAPSHOT_ATTEMPTS
                    ):
                        raise
                    time.sleep(PREPROCESS_RECEIPT_SNAPSHOT_RETRY_SECONDS)
            raise BackendError("unreachable preprocess receipt snapshot retry state")

        stable_receipt_paths.__name__ = "himr_stable_preprocess_receipt_paths_v1"
        stable_receipt_paths.__himr_preprocess_receipt_snapshot_adapter__ = True  # type: ignore[attr-defined]
        stable_receipt_paths.__wrapped__ = original  # type: ignore[attr-defined]
        setattr(background, "_preprocess_receipt_paths", stable_receipt_paths)
        _PREPROCESS_RECEIPT_ADAPTERS[background] = (original, stable_receipt_paths)


def _load_modules() -> _Modules:
    repository = Path(__file__).resolve().parent.parent
    for path in (repository / "acquisition", repository / "pipeline"):
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)
    background = importlib.import_module("background_producer")
    queue_runner = importlib.import_module("queue_runner")
    _install_acquisition_result_finalization_adapter(queue_runner)
    _install_acquisition_directory_identity_adapter(queue_runner)
    install_acquisition_retry_proof_adapter(queue_runner)
    _install_preprocess_receipt_snapshot_adapter(background)
    return _Modules(
        background=background,
        queue_runner=queue_runner,
        handoff=importlib.import_module("archive_preprocess_handoff"),
        preprocess_batch=importlib.import_module("preprocess_batch"),
        gpu_queue=importlib.import_module("preprocess_gpu_asr_queue_v1"),
        gpu_bridge=importlib.import_module("materialize_gpu_asr_batch_v1"),
        retention=importlib.import_module("retain_public_acquisition"),
        schedule_set=importlib.import_module("materialize_campaign_schedule_set"),
        composite_schedule_set=importlib.import_module(
            "materialize_composite_campaign_schedule_set"
        ),
    )


class SealedArchiveBackend:
    """Run and reconcile an exact Archive campaign through bounded adapters."""

    _acquisition_gate_lock = threading.Lock()

    def __init__(
        self,
        config: ControllerConfig,
        *,
        modules: _Modules | None = None,
        gpu_executor: Any | None = None,
        _replay_store: OperationalReplayStore | None = None,
    ):
        self.config = config
        self.modules = modules or _load_modules()
        if isinstance(self.modules, _Modules):
            # Historical receipt replay is distinct from fresh launch authority.
            # The copied trusted launcher and every sealed input stay unchanged.
            gpu_runtime_successor.install_historical_replay(
                self.modules.gpu_bridge.ASR_V5, config
            )
        self._operational_replay_store = self._configure_replay_store(
            _replay_store
        )
        self._retained_items: dict[str, dict[str, Any]] = {}
        self._retention_replay_pending: list[str] = []
        self._gpu_records: dict[str, dict[str, Any]] = {}
        self._gpu_status: dict[str, str] = {}
        self._gpu_queue_dispositions: dict[str, dict[str, int]] = {}
        self._gpu_parked: dict[str, dict[str, Any]] = {}
        self._gpu_batch_item_counts: dict[str, int] = {}
        self._gpu_member_claims: dict[tuple[str, int], str] = {}
        self._gpu_queue_cache: dict[
            tuple[str, str], tuple[dict[str, Any], Path]
        ] = {}
        self._gpu_queue_ids: dict[str, tuple[str, str]] = {}
        self._gpu_partial_fingerprint: str | None = None
        self._gpu_partial_hold_started_at: float | None = None
        self._gpu_buffered_ready_items = 0
        self._gpu_executor: Any | None = gpu_executor
        self._gpu_child_records: dict[str, GpuChildRecord] | None = None
        self._gpu_opportunity_lease: _GpuOpportunityLease | None = None
        self._preprocess_failure_attempts: dict[str, list[dict[str, Any]]] = {}
        self._preprocess_parked: dict[str, dict[str, Any]] = {}
        self._preprocess_resolved_after_retry: set[str] = set()
        self._preprocess_candidates_cache: list[tuple[int, Path, str, Path]] | None = None
        self._preprocess_candidate_keys: set[tuple[str, str]] = set()
        self._preprocess_bundle_item_counts: dict[tuple[str, str], int] = {}
        self._preprocessed_item_total = 0
        self._preprocess_restart_witnesses: dict[
            tuple[str, str], list[dict[str, Any]]
        ] = {}
        self._gpu_restart_witnesses: dict[str, list[dict[str, Any]]] = {}
        self._restart_restore_telemetry: dict[str, Any] = {}
        self._trust_completed_copy = False
        self._checkpoint_queue_snapshots: dict[str, dict[str, Any]] = {}
        self._campaign_coverage_checkpoint: dict[str, Any] | None = None
        self._schedule_set_coverage_checkpoint: dict[str, Any] | None = None
        self._cold_storage_identity_checkpoint: dict[str, Any] | None = None
        self._profile: dict[str, Any] | None = None
        self._runtime_cache: dict[
            str,
            tuple[
                dict[str, Any],
                dict[str, Any],
                list[dict[str, Any] | None],
                dict[str, Any],
            ],
        ] = {}
        self._lane_stage: str | None = None

    def _configure_replay_store(
        self, shared: OperationalReplayStore | None
    ) -> OperationalReplayStore | None:
        """Bind one queue router, while leaving deliberately small fakes usable."""

        runner = getattr(self.modules, "queue_runner", None)
        production_modules = isinstance(self.modules, _Modules)
        if production_modules:
            background = self.modules.background
            handoff = self.modules.handoff
            if (
                getattr(background, "queue_runner", None) is not runner
                or getattr(handoff, "queue_runner", None) is not runner
                or getattr(handoff, "background_producer", None) is not background
            ):
                raise BackendError(
                    "sealed producer and handoff do not share one queue module"
                )
        if shared is not None:
            if shared.router.queue_runner is not runner:
                raise BackendError(
                    "shared operational replay store belongs to another queue module"
                )
            try:
                shared.router.assert_installed()
            except OperationalReplayError as error:
                raise BackendError(
                    f"shared operational replay router is unavailable: {error}"
                ) from error
            return shared
        if not isinstance(runner, ModuleType):
            if production_modules:
                raise BackendError("sealed queue runner is not an imported module")
            return None
        required = (
            "_scan_results",
            "_inspect_result",
            "_result_path",
            "_safe_existing_result_parents",
            "canonical_bytes",
            "sha256_bytes",
        )
        acquire = getattr(runner, "acquire", None)
        supported = all(callable(getattr(runner, name, None)) for name in required)
        supported = supported and callable(getattr(acquire, "pretty_json", None))
        if not supported:
            if production_modules:
                raise BackendError(
                    "sealed queue runner lacks operational replay primitives"
                )
            return None
        try:
            return OperationalReplayStore.for_queue_runner(runner)
        except OperationalReplayError as error:
            raise BackendError(
                f"cannot install operational replay router: {error}"
            ) from error

    def fork_lane(self, stage: str) -> "SealedArchiveBackend":
        """Fork restored mutable indexes for one independently scheduled stage.

        Immutable module/config bindings are shared.  Every mutable cache and
        recovery ledger is copied so peer outcomes can be applied on the fork's
        own thread without racing another lane.  The exact GPU executor is owned
        only by the GPU fork.
        """

        if stage not in {
            "acquisition",
            "preprocess",
            "gpu_readiness",
            "cold_retention",
        }:
            raise BackendError(f"cannot fork unsupported lane {stage!r}")
        if self._gpu_opportunity_lease is not None:
            raise BackendError(
                "cannot fork controller authority while a GPU opportunity lease is retained"
            )
        fork = SealedArchiveBackend(
            self.config,
            modules=self.modules,
            gpu_executor=(
                self._gpu_executor if stage == "gpu_readiness" else None
            ),
            _replay_store=self._operational_replay_store,
        )
        for name in (
            "_retained_items",
            "_retention_replay_pending",
            "_gpu_records",
            "_gpu_status",
            "_gpu_queue_dispositions",
            "_gpu_parked",
            "_gpu_batch_item_counts",
            "_gpu_member_claims",
            "_gpu_partial_fingerprint",
            "_gpu_partial_hold_started_at",
            "_gpu_buffered_ready_items",
            "_gpu_child_records",
            "_preprocess_failure_attempts",
            "_preprocess_parked",
            "_preprocess_resolved_after_retry",
            "_preprocess_candidates_cache",
            "_preprocess_candidate_keys",
            "_preprocess_bundle_item_counts",
            "_preprocessed_item_total",
            "_preprocess_restart_witnesses",
            "_gpu_restart_witnesses",
            "_restart_restore_telemetry",
            "_checkpoint_queue_snapshots",
            "_campaign_coverage_checkpoint",
            "_schedule_set_coverage_checkpoint",
            "_cold_storage_identity_checkpoint",
            "_profile",
            "_runtime_cache",
        ):
            setattr(fork, name, deepcopy(getattr(self, name)))
        # Queue manifests are immutable, SHA-bound values.  Copy only the two
        # derived indexes; sharing their manifest objects avoids multiplying a
        # large restored corpus once per independent lane.
        fork._gpu_queue_cache = dict(self._gpu_queue_cache)
        fork._gpu_queue_ids = dict(self._gpu_queue_ids)
        fork._lane_stage = stage
        return fork

    def rebuild_from_events(
        self, events: Sequence[dict[str, Any]]
    ) -> tuple["SealedArchiveBackend", dict[str, Any]]:
        """Construct and exactly restore a fresh root after a lane retry fault."""

        if self._lane_stage is not None:
            raise BackendError("a stage-confined fork cannot rebuild controller authority")
        rebuilt = SealedArchiveBackend(
            self.config,
            modules=self.modules,
            gpu_executor=self._gpu_executor,
        )
        recovery = rebuilt.restore(events)
        return rebuilt, recovery

    def _observe_preprocess_failure(self, record: dict[str, Any]) -> None:
        if not isinstance(record, dict):
            raise BackendError("peer preprocess failure record is malformed")
        item_key = record.get("item_key")
        attempt = record.get("attempt_ordinal")
        disposition = record.get("disposition")
        if (
            not isinstance(item_key, str)
            or not item_key
            or isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or attempt < 1
            or disposition not in {"retryable", "parked"}
        ):
            raise BackendError("peer preprocess failure record is malformed")
        rows = self._preprocess_failure_attempts.setdefault(item_key, [])
        if attempt <= len(rows):
            if rows[attempt - 1] != record:
                raise BackendError("peer preprocess failure conflicts with recovery")
            return
        if attempt != len(rows) + 1:
            raise BackendError("peer preprocess attempts are not contiguous")
        rows.append(deepcopy(record))
        if disposition == "parked":
            self._preprocess_parked[item_key] = deepcopy(record)

    def _invalidate_preprocess_candidates(self) -> None:
        """Drop every derived preprocess index so the next read replays receipts."""

        self._preprocess_candidates_cache = None
        self._preprocess_candidate_keys.clear()
        self._preprocess_bundle_item_counts.clear()
        self._preprocessed_item_total = 0

    def _register_preprocess_candidate(
        self,
        *,
        campaign_ordinal: int,
        bundle_path: Path,
        bundle_id: str,
        state_root: Path,
        item_count: int,
    ) -> None:
        """Register one validated completed bundle and its exact item cardinality."""

        if (
            isinstance(campaign_ordinal, bool)
            or not isinstance(campaign_ordinal, int)
            or campaign_ordinal < 1
            or not isinstance(bundle_path, Path)
            or not isinstance(bundle_id, str)
            or not bundle_id
            or not isinstance(state_root, Path)
            or not state_root.is_absolute()
            or isinstance(item_count, bool)
            or not isinstance(item_count, int)
            or item_count < 1
            or isinstance(self._preprocessed_item_total, bool)
            or not isinstance(self._preprocessed_item_total, int)
            or self._preprocessed_item_total < 0
        ):
            raise BackendError("completed preprocess candidate is malformed")
        if self._preprocess_candidates_cache is not None and not (
            len(self._preprocess_candidates_cache)
            == len(self._preprocess_candidate_keys)
            == len(self._preprocess_bundle_item_counts)
        ):
            raise BackendError("preprocess candidate indexes differ before registration")
        key = (str(state_root), bundle_id)
        previous_count = self._preprocess_bundle_item_counts.get(key)
        if previous_count is not None:
            if previous_count != item_count:
                raise BackendError(
                    "completed preprocess bundle item count changed"
                )
            if (
                self._preprocess_candidates_cache is not None
                and key not in self._preprocess_candidate_keys
            ):
                raise BackendError(
                    "preprocess completed-bundle indexes differ"
                )
            return
        if key in self._preprocess_candidate_keys:
            raise BackendError("preprocess completed-bundle identity is repeated")
        self._preprocess_bundle_item_counts[key] = item_count
        self._preprocess_candidate_keys.add(key)
        self._preprocess_restart_witnesses.pop(key, None)
        self._preprocessed_item_total += item_count
        if self._preprocess_candidates_cache is None:
            return
        candidate = (campaign_ordinal, bundle_path, bundle_id, state_root)
        # Normal preprocessing resolves campaign ordinals monotonically, making
        # the production hot path an O(1) append.  A recovered earlier retry is
        # inserted at its exact sorted position without sorting every historical
        # candidate again.
        if (
            not self._preprocess_candidates_cache
            or self._preprocess_candidates_cache[-1] < candidate
        ):
            self._preprocess_candidates_cache.append(candidate)
        else:
            position = bisect_left(self._preprocess_candidates_cache, candidate)
            if (
                position < len(self._preprocess_candidates_cache)
                and self._preprocess_candidates_cache[position] == candidate
            ):
                raise BackendError("preprocess candidate tuple is repeated")
            self._preprocess_candidates_cache.insert(position, candidate)

    @staticmethod
    def _preprocess_runtime_position(
        runtimes: Sequence[tuple[Any, Any, Any, Any, Any]],
        schedule_id: str,
    ) -> tuple[tuple[Any, Any, Any, Any, Any], int]:
        """Return one sealed active schedule runtime and its campaign offset."""

        position = 0
        matches: list[tuple[tuple[Any, Any, Any, Any, Any], int]] = []
        for runtime in runtimes:
            reference = runtime[0]
            if reference.get("schedule_id") == schedule_id:
                matches.append((runtime, position))
            states = runtime[3]
            if not isinstance(states, list):
                raise BackendError("preprocess campaign runtime states are malformed")
            position += len(states)
        if len(matches) != 1 or matches[0][0][0].get("role") != "normal_processing":
            raise BackendError(
                "peer preprocess bundle lacks one sealed active normal schedule"
            )
        return matches[0]

    def _observe_preprocess_bundle(
        self,
        artifact: dict[str, Any],
        *,
        runtime: tuple[Any, Any, Any, Any, Any],
        campaign_offset: int,
    ) -> None:
        """Replay and register one peer bundle without trusting peer cardinality."""

        expected = {
            "queue_ordinal",
            "bundle_path",
            "bundle_id",
            "bundle_manifest_sha256",
            "campaign_ordinal",
            "preprocess_state_root",
            "item_count",
        }
        if not isinstance(artifact, dict) or set(artifact) != expected:
            raise BackendError("peer preprocess bundle artifact is malformed")
        queue_ordinal = artifact["queue_ordinal"]
        campaign_ordinal = artifact["campaign_ordinal"]
        bundle_id = artifact["bundle_id"]
        raw_bundle_path = artifact["bundle_path"]
        raw_state_root = artifact["preprocess_state_root"]
        bundle_path = Path(raw_bundle_path) if isinstance(raw_bundle_path, str) else Path("")
        state_root = Path(raw_state_root) if isinstance(raw_state_root, str) else Path("")
        item_count = artifact["item_count"]
        bundle_root = Path(self.config.section("preprocess")["bundle_root"])
        if (
            isinstance(queue_ordinal, bool)
            or not isinstance(queue_ordinal, int)
            or queue_ordinal < 1
            or isinstance(campaign_ordinal, bool)
            or not isinstance(campaign_ordinal, int)
            or campaign_ordinal < 1
            or not isinstance(bundle_id, str)
            or not bundle_id
            or not isinstance(raw_bundle_path, str)
            or bundle_path != bundle_root / "bundles" / bundle_id
            or not isinstance(raw_state_root, str)
            or not state_root.is_absolute()
            or not isinstance(artifact["bundle_manifest_sha256"], str)
            or len(artifact["bundle_manifest_sha256"]) != 64
            or isinstance(item_count, bool)
            or not isinstance(item_count, int)
            or item_count < 1
        ):
            raise BackendError("peer preprocess bundle artifact is malformed")
        reference, schedule, acquisition_bundle, states, _ready = runtime
        try:
            exact_state_root = Path(
                schedule["consumer"]["preprocess_state_root"]
            )
            manifest, selection, orders = (
                self.modules.preprocess_batch.validate_bundle(bundle_path)
            )
            receipts = self.modules.preprocess_batch.existing_receipts(
                exact_state_root,
                manifest=manifest,
                selection=selection,
                orders=orders,
            )
            selection_entries = selection["entries"]
            manifest_count = manifest["work_order_count"]
            acquisition_path = selection_entries[0]["acquisition_result"]["path"]
            # The sealed queue manifest guarantees contiguous one-based
            # ordinals. Validate the peer's claimed ordinal directly against its
            # exact entry/order/state triple instead of rescanning the complete
            # schedule once for every singleton receipt.
            acquisition_entries = acquisition_bundle["manifest"]["work_orders"]
            acquisition_orders = acquisition_bundle["orders"]
            acquisition_index = queue_ordinal - 1
            if not (
                len(acquisition_entries)
                == len(acquisition_orders)
                == len(states)
                and 0 <= acquisition_index < len(states)
            ):
                raise BackendError(
                    "peer preprocess ordinal is outside its exact acquisition schedule"
                )
            acquisition_entry = acquisition_entries[acquisition_index]
            acquisition_order = acquisition_orders[acquisition_index]
            acquisition_state = states[acquisition_index]
            indexed_acquisition_matches = (
                acquisition_entry["queue_ordinal"] == queue_ordinal
                and self.modules.background._completed_state(acquisition_state)
                and str(self.modules.queue_runner._result_path(acquisition_order))
                == acquisition_path
            )
        except Exception as error:
            raise BackendError(
                f"peer preprocess bundle exact replay failed for {bundle_id}: {error}"
            ) from error
        if (
            reference.get("role") != "normal_processing"
            or not exact_state_root.is_absolute()
            or state_root != exact_state_root
            or manifest.get("bundle_id") != bundle_id
            or manifest.get("manifest_sha256")
            != artifact["bundle_manifest_sha256"]
            or manifest_count != 1
            or len(selection_entries) != 1
            or len(orders) != 1
            or not isinstance(receipts, dict)
            or set(receipts) != {1}
            or not indexed_acquisition_matches
        ):
            raise BackendError(
                "peer preprocess bundle differs from exact singleton receipt replay"
            )
        derived_queue_ordinal = queue_ordinal
        derived = {
            "queue_ordinal": derived_queue_ordinal,
            "bundle_path": str(bundle_path),
            "bundle_id": manifest["bundle_id"],
            "bundle_manifest_sha256": manifest["manifest_sha256"],
            "campaign_ordinal": campaign_offset + derived_queue_ordinal,
            "preprocess_state_root": str(exact_state_root),
            "item_count": manifest_count,
        }
        if artifact != derived:
            raise BackendError(
                "peer preprocess bundle artifact differs from derived receipt authority"
            )
        self._register_preprocess_candidate(
            campaign_ordinal=derived["campaign_ordinal"],
            bundle_path=bundle_path,
            bundle_id=derived["bundle_id"],
            state_root=exact_state_root,
            item_count=manifest_count,
        )

    def _observe_gpu_record(self, record: dict[str, Any], status: str) -> None:
        """Apply one peer record atomically across every derived GPU ledger."""

        self._apply_gpu_record(record, status, derive_missing_status=False)

    def _restore_gpu_record(
        self, record: dict[str, Any], status: str | None
    ) -> None:
        """Replay one journal record, deriving only an absent legacy status."""

        self._apply_gpu_record(
            record, status, derive_missing_status=status is None
        )

    def _apply_gpu_record(
        self,
        record: dict[str, Any],
        status: str | None,
        *,
        derive_missing_status: bool,
    ) -> None:
        """Apply one record transactionally across every derived GPU ledger."""

        self._apply_gpu_records(
            [(record, status)],
            derive_missing_status=derive_missing_status,
        )

    def _apply_gpu_records(
        self,
        evidence: Sequence[tuple[dict[str, Any], str | None]],
        *,
        derive_missing_status: bool,
    ) -> None:
        """Apply one closed GPU artifact transaction across all six ledgers."""

        ledger_names = (
            "_gpu_records",
            "_gpu_status",
            "_gpu_queue_dispositions",
            "_gpu_parked",
            "_gpu_batch_item_counts",
            "_gpu_member_claims",
            "_gpu_queue_cache",
            "_gpu_queue_ids",
        )
        # Validation only inserts/replaces top-level ledger entries; it never
        # mutates an existing nested value.  Shallow snapshots therefore give
        # exact rollback without repeatedly copying all prior batch records.
        before = {name: dict(getattr(self, name)) for name in ledger_names}
        try:
            for record, status in evidence:
                self._observe_gpu_record_transaction(
                    record,
                    status,
                    derive_missing_status=derive_missing_status,
                )
        except BaseException:
            # Exact validation derives claims/counts before peer status can be
            # compared.  A rejected status or late disposition conflict must not
            # leave those side ledgers ahead of record/status authority.
            for name, value in before.items():
                setattr(self, name, value)
            raise

    def _observe_gpu_record_transaction(
        self,
        record: dict[str, Any],
        status: str | None,
        *,
        derive_missing_status: bool,
    ) -> None:
        key = record.get("batch_key") if isinstance(record, dict) else None
        if not isinstance(key, str) or not key:
            raise BackendError("peer GPU record has an invalid key")
        if status is None and not derive_missing_status:
            raise BackendError("peer GPU record status is invalid")
        if status is not None and status not in {
            "pending",
            "completed",
            "parked",
            "not_applicable",
        }:
            raise BackendError("peer GPU record status is invalid")
        previous = self._gpu_records.get(key)
        if previous is not None:
            if previous != record:
                raise BackendError("peer GPU record conflicts with recovery")
        copied = deepcopy(record)
        exact_status = self._validate_gpu_record(copied)
        if status is None:
            status = exact_status
        elif status == "pending" and exact_status in {"completed", "parked"}:
            # External immutable result evidence may have advanced after the
            # producing lane admitted its record.  Accept only that monotonic,
            # exactly replayed advancement.
            status = exact_status
        elif status != exact_status:
            raise BackendError("peer GPU status differs from exact batch replay")
        if previous is not None:
            prior_status = self._gpu_status.get(key)
            allowed = {
                "pending": {"pending", "completed", "parked"},
                "completed": {"completed"},
                "parked": {"parked"},
                "not_applicable": {"not_applicable"},
            }
            if prior_status not in allowed or status not in allowed[prior_status]:
                raise BackendError("peer GPU record status regresses recovery")
            self._gpu_status[key] = status
            self._gpu_restart_witnesses.pop(key, None)
            return
        self._register_gpu_disposition(copied)
        self._gpu_records[key] = copied
        self._gpu_status[key] = status
        self._gpu_restart_witnesses.pop(key, None)

    @staticmethod
    def _gpu_event_evidence(
        event_type: str,
        outcome: dict[str, Any],
        artifacts: dict[str, Any],
    ) -> list[tuple[dict[str, Any], str | None]]:
        """Validate one durable GPU record/status map as a closed pair."""

        records = artifacts.get("gpu_records", [])
        statuses_present = "gpu_record_statuses" in artifacts
        statuses = artifacts.get("gpu_record_statuses")
        if not isinstance(records, list) or (
            statuses_present and not isinstance(statuses, dict)
        ):
            raise BackendError("journal GPU artifacts are malformed")
        record_keys = [
            record.get("batch_key") if isinstance(record, dict) else None
            for record in records
        ]
        if (
            any(not isinstance(key, str) or not key for key in record_keys)
            or len(set(record_keys)) != len(record_keys)
            or (
                statuses_present
                and (
                    set(statuses) != set(record_keys)
                    or any(
                        not isinstance(status, str)
                        or status
                        not in {
                            "pending",
                            "completed",
                            "parked",
                            "not_applicable",
                        }
                        for status in statuses.values()
                    )
                )
            )
        ):
            raise BackendError("journal GPU record/status ledgers differ")
        if event_type == "gpu_child_quiesced":
            if outcome.get("stage") != "gpu_readiness":
                raise BackendError("journal GPU quiesce outcome has the wrong stage")
            if records:
                raise BackendError("journal GPU quiesce outcome invents batch records")
        elif records and outcome.get("stage") != "gpu_readiness":
            raise BackendError("journal GPU records appear outside GPU readiness")
        return [
            (
                deepcopy(record),
                statuses[record["batch_key"]] if statuses_present else None,
            )
            for record in records
        ]

    def _observe_acquisition_runtime(self, value: Any) -> str:
        """Apply a producer's exact signed queue summary without a deep replay."""

        expected = {"kind", "schedule_id", "queue_summary"}
        if self._operational_replay_store is not None:
            expected.add("operational_replay")
        if not isinstance(value, dict) or set(value) != expected:
            raise BackendError("peer acquisition runtime delta is malformed")
        schedule_id = value["schedule_id"]
        queue_summary = value["queue_summary"]
        if (
            value["kind"] != "acquisition_queue_summary_v1"
            or not isinstance(schedule_id, str)
            or not schedule_id
            or not isinstance(queue_summary, dict)
        ):
            raise BackendError("peer acquisition runtime delta is malformed")
        references = [
            reference
            for reference in self.config.section("campaign")["schedules"]
            if reference["schedule_id"] == schedule_id
        ]
        if len(references) != 1:
            raise BackendError("peer acquisition runtime names an unsealed schedule")
        reference = references[0]
        replay_delta = self._verify_peer_replay_delta(
            value.get("operational_replay"), schedule_id=schedule_id
        )
        if replay_delta is not None:
            self._record_checkpoint_queue_delta(replay_delta)
        cached = self._runtime_cache.get(schedule_id)
        try:
            if cached is None:
                # A concurrent preprocess acknowledgement may have invalidated this
                # one epoch.  Reload only the small sealed schedule document; the
                # exact queue summary below avoids validate_queue's media replay.
                schedule, schedule_path, schedule_body = (
                    self.modules.background.load_schedule(Path(reference["path"]))
                )
                if (
                    schedule_path != Path(reference["path"])
                    or sha256_bytes(schedule_body) != reference["sha256"]
                    or schedule["schedule_id"] != schedule_id
                ):
                    raise BackendError(
                        "peer acquisition schedule differs from controller config"
                    )
            else:
                schedule = cached[0]
            if self._peer_acquisition_replay_is_superseded(
                replay_delta,
                reference=reference,
                schedule=schedule,
            ):
                # Peer outcomes are delivered in completion order, but a slow
                # consumer can accumulate more than one acquisition generation.
                # A later preprocess pass may already have acknowledged a result
                # which this historical queue summary still calls pending.  The
                # signed delta remains valid dependency evidence; it is not valid
                # current runtime authority.  Drop only this lane's old cache so
                # its imminent stage call reloads the latest shared exact snapshot.
                self._runtime_cache.pop(schedule_id, None)
                return schedule_id
            try:
                bundle, states, ready = (
                    self.modules.background._runtime_from_queue_summary(
                        schedule,
                        queue_summary,
                        expected_mode="run",
                        expected_statuses={"bounded", "completed", "parked"},
                    )
                )
            except Exception as error:
                # A preprocess receipt can become visible after the generation
                # precheck but before its replay store session commits the newer
                # completed result. Suppress only that positively identified
                # source conflict. A committed generation advance proves the
                # peer historical immediately. If the producing preprocess
                # session is still open, however, physical receipt authority can
                # lead the replay store briefly; perform one bounded read-only
                # exact reload which can admit that physical transition without
                # ever repeating the producer mutation.
                if self._is_temporal_preprocess_receipt_conflict(error):
                    if self._peer_acquisition_replay_is_superseded(
                        replay_delta,
                        reference=reference,
                        schedule=schedule,
                    ):
                        self._runtime_cache.pop(schedule_id, None)
                        return schedule_id
                    (
                        refreshed_schedule,
                        refreshed_bundle,
                        refreshed_states,
                        refreshed_ready,
                    ) = self._schedule_runtime(reference)
                    if refreshed_schedule != schedule:
                        raise BackendError(
                            "temporal acquisition replay reload changed its sealed schedule"
                        )
                    self._update_cached_runtime(
                        reference,
                        refreshed_schedule,
                        refreshed_bundle,
                        refreshed_states,
                        refreshed_ready,
                    )
                    return schedule_id
                if isinstance(error, BackendError):
                    raise
                raise BackendError(
                    f"peer acquisition runtime delta failed exact replay: {error}"
                ) from error
            # Compare and install under the replay store's commit lock. A newer
            # exact generation therefore either precedes admission (and discards
            # these provisional objects) or follows a fully admitted cache write.
            self._admit_peer_acquisition_runtime(
                replay_delta,
                reference=reference,
                schedule=schedule,
                bundle=bundle,
                states=states,
                ready=ready,
            )
        except BackendError:
            self._runtime_cache.pop(schedule_id, None)
            raise
        except Exception as error:
            self._runtime_cache.pop(schedule_id, None)
            raise BackendError(
                f"peer acquisition runtime delta failed exact replay: {error}"
            ) from error
        return schedule_id

    def _observe_preprocess_runtime(self, value: Any, monitor: dict[str, Any]) -> None:
        expected = {"kind", "schedule_id", "operational_replay"}
        if not isinstance(value, dict) or set(value) != expected:
            raise BackendError("peer preprocess replay delta is malformed")
        schedule_id = value["schedule_id"]
        if (
            value["kind"] != "preprocess_operational_replay_v1"
            or not isinstance(schedule_id, str)
            or schedule_id != monitor.get("active_schedule_id")
            or sum(
                reference["schedule_id"] == schedule_id
                for reference in self.config.section("campaign")["schedules"]
            )
            != 1
        ):
            raise BackendError("peer preprocess replay delta is malformed")
        replay_delta = self._verify_peer_replay_delta(
            value["operational_replay"], schedule_id=schedule_id
        )
        if replay_delta is None:  # pragma: no cover - verification is fail-closed
            raise BackendError("peer preprocess replay emitted no exact authority")
        self._record_checkpoint_queue_delta(replay_delta)
        # A verified preprocess delta proves the shared replay-store generation,
        # not that this peer's cached runtime tuple was materialized from that
        # generation. Preprocess supplies no replacement states to peers, so
        # invalidate the one touched schedule before validating its bundle rows.
        self._runtime_cache.pop(schedule_id, None)

    def observe_peer_outcome(self, outcome: StageOutcome) -> None:
        """Apply one admitted peer delta without a campaign-wide replay."""

        normalized = outcome.normalized()
        artifacts = normalized["artifacts"]
        if artifacts.get("backend_kind") != BACKEND_KIND:
            raise BackendError("peer outcome is not from the sealed Archive backend")
        if outcome.stage != "gpu_readiness":
            # Durable restore rejects GPU authority attached to another stage;
            # live peer admission must enforce the same closed contract.
            self._gpu_event_evidence(
                "stage_finished",
                {"stage": outcome.stage},
                artifacts,
            )
        applied_runtime_schedule: str | None = None
        if outcome.stage == "acquisition" and outcome.progressed:
            delta = artifacts.get(TRANSIENT_PEER_RUNTIME_ARTIFACT)
            applied_runtime_schedule = self._observe_acquisition_runtime(delta)
            if applied_runtime_schedule != outcome.monitor.get("active_schedule_id"):
                self._runtime_cache.pop(applied_runtime_schedule, None)
                raise BackendError(
                    "peer acquisition runtime differs from the active schedule"
                )
        if outcome.stage == "preprocess":
            replay_delta = artifacts.get(TRANSIENT_PEER_RUNTIME_ARTIFACT)
            if replay_delta is not None:
                self._observe_preprocess_runtime(replay_delta, outcome.monitor)
            failures = artifacts.get("preprocess_failure_attempts", [])
            bundles = artifacts.get("preprocess_bundles", [])
            rescan = artifacts.get("preprocess_candidates_rescan", False)
            resolved_predecessors = artifacts.get(
                "resolved_predecessor_ordinals", []
            )
            if (
                not isinstance(failures, list)
                or not isinstance(bundles, list)
                or not isinstance(rescan, bool)
                or not isinstance(resolved_predecessors, list)
                or any(
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 1
                    for value in resolved_predecessors
                )
                or resolved_predecessors != sorted(set(resolved_predecessors))
            ):
                raise BackendError("peer preprocess artifacts are malformed")
            schedule_id = outcome.monitor.get("active_schedule_id")
            failed_ordinal = outcome.monitor.get("failed_queue_ordinal")
            processed_count = outcome.monitor.get("processed_items")
            bundle_ordinals = [
                artifact.get("queue_ordinal")
                if isinstance(artifact, dict)
                else None
                for artifact in bundles
            ]
            if (
                any(
                    isinstance(ordinal, bool)
                    or not isinstance(ordinal, int)
                    or ordinal < 1
                    for ordinal in bundle_ordinals
                )
                or len(set(bundle_ordinals)) != len(bundle_ordinals)
                or (bundles and not isinstance(schedule_id, str))
                or (rescan and bool(bundles))
                or isinstance(processed_count, bool)
                or not isinstance(processed_count, int)
                or processed_count < 0
                or (
                    not resolved_predecessors
                    and processed_count != len(bundles)
                )
                or (
                    (processed_count > 0 or bool(bundles))
                    and not isinstance(schedule_id, str)
                )
            ):
                raise BackendError("peer preprocess bundle artifacts are inconsistent")
            if resolved_predecessors and (
                not isinstance(schedule_id, str)
                or isinstance(failed_ordinal, bool)
                or not isinstance(failed_ordinal, int)
                or any(value >= failed_ordinal for value in resolved_predecessors)
                or isinstance(processed_count, bool)
                or not isinstance(processed_count, int)
                or len(resolved_predecessors) != processed_count
                or (
                    not rescan
                    and bundle_ordinals != resolved_predecessors
                )
            ):
                raise BackendError(
                    "peer preprocess predecessor resolution is inconsistent"
                )
            if isinstance(schedule_id, str):
                if (
                    sum(
                        reference["schedule_id"] == schedule_id
                        for reference in self.config.section("campaign")["schedules"]
                    )
                    != 1
                ):
                    raise BackendError("peer preprocess active schedule is unknown")
                # Stop and operational-failure outcomes can carry completed
                # predecessor bundles without a replay delta. Invalidate before
                # any physical bundle replay so the receiver cannot validate a
                # new ordinal against its pre-handoff cache generation.
                self._runtime_cache.pop(schedule_id, None)
            for record in failures:
                self._observe_preprocess_failure(record)
            if bundles:
                runtime, campaign_offset = self._preprocess_runtime_position(
                    self._campaign_runtimes(), schedule_id
                )
                for artifact in bundles:
                    self._observe_preprocess_bundle(
                        artifact,
                        runtime=runtime,
                        campaign_offset=campaign_offset,
                    )
            if rescan:
                # A list-comprehension handoff can durably finish predecessors
                # before a later item raises.  The exception has no per-item return
                # values, so force the exact receipt-bound candidate scan rather
                # than silently omitting those successful bundles.
                self._invalidate_preprocess_candidates()
            if isinstance(schedule_id, str):
                for artifact in bundles:
                    ordinal = artifact.get("queue_ordinal")
                    if isinstance(ordinal, int) and not isinstance(ordinal, bool):
                        self._preprocess_resolved_after_retry.add(
                            f"{schedule_id}:{ordinal}"
                        )
                for ordinal in resolved_predecessors:
                    self._preprocess_resolved_after_retry.add(
                        f"{schedule_id}:{ordinal}"
                    )
            elif resolved_predecessors:
                raise BackendError(
                    "peer preprocess predecessor resolution lacks a schedule"
                )
        if outcome.stage == "gpu_readiness":
            records = artifacts.get("gpu_records", [])
            statuses = artifacts.get("gpu_record_statuses", {})
            if not isinstance(records, list) or not isinstance(statuses, dict):
                raise BackendError("peer GPU artifacts are malformed")
            record_keys = [
                record.get("batch_key") if isinstance(record, dict) else None
                for record in records
            ]
            if (
                any(not isinstance(key, str) or not key for key in record_keys)
                or len(set(record_keys)) != len(record_keys)
                or set(statuses) != set(record_keys)
            ):
                raise BackendError("peer GPU record/status ledgers differ")
            self._apply_gpu_records(
                [
                    (record, statuses[record["batch_key"]])
                    for record in records
                ],
                derive_missing_status=False,
            )
        if outcome.stage == "cold_retention":
            retained = artifacts.get("retained", [])
            if not isinstance(retained, list):
                raise BackendError("peer retention artifacts are malformed")
            for record in retained:
                key = record.get("item_key") if isinstance(record, dict) else None
                if not isinstance(key, str) or not key:
                    raise BackendError("peer retention record is malformed")
                previous = self._retained_items.get(key)
                if previous is not None and previous != record:
                    raise BackendError("peer retention record conflicts with recovery")
                self._retained_items[key] = deepcopy(record)

        # Acquisition supplies an exact signed runtime delta.  Other progressed
        # outcomes invalidate only their touched schedule epoch; the remaining
        # campaign cache is preserved.
        schedule_id = outcome.monitor.get("active_schedule_id")
        if (
            outcome.progressed
            and isinstance(schedule_id, str)
            and schedule_id != applied_runtime_schedule
        ):
            self._runtime_cache.pop(schedule_id, None)

    def _get_gpu_executor(self) -> Any:
        if self._gpu_executor is None:
            gpu = self.config.section("gpu_readiness")
            self._gpu_executor = SystemdGpuChildExecutor(
                ControllerUnitContext.from_current_systemd_unit(),
                PrivateGpuChildJournal(Path(gpu["child_journal_root"])),
            )
        return self._gpu_executor

    def _gpu_opportunity_uuid(self) -> str:
        profile = self._load_profile()
        hardware = profile.get("hardware") if isinstance(profile, dict) else None
        gpu_uuid = hardware.get("gpu_uuid") if isinstance(hardware, dict) else None
        if not isinstance(gpu_uuid, str) or GPU_UUID_RE.fullmatch(gpu_uuid) is None:
            raise BackendError("production profile GPU UUID is invalid")
        return gpu_uuid

    def _gpu_opportunity_path(self, gpu_uuid: str | None = None) -> Path:
        selected = self._gpu_opportunity_uuid() if gpu_uuid is None else gpu_uuid
        if not isinstance(selected, str) or GPU_UUID_RE.fullmatch(selected) is None:
            raise BackendError("GPU opportunity UUID is invalid")
        root = Path(self.config.section("gpu_readiness")["lock_root"])
        return root / f"gpu-{selected}.opportunity.lock"

    @staticmethod
    def _opportunity_identity(value: os.stat_result) -> tuple[int, int]:
        return value.st_dev, value.st_ino

    def _validate_gpu_opportunity_lease(
        self, lease: _GpuOpportunityLease
    ) -> None:
        if (
            not isinstance(lease, _GpuOpportunityLease)
            or lease.descriptor < 0
            or GPU_UUID_RE.fullmatch(lease.gpu_uuid) is None
            or lease.path != self._gpu_opportunity_path(lease.gpu_uuid)
        ):
            raise BackendError("retained GPU opportunity lease binding is invalid")
        try:
            opened = os.fstat(lease.descriptor)
            linked = lease.path.lstat()
            root = lease.path.parent.lstat()
        except OSError as error:
            raise BackendError(
                f"cannot validate retained GPU opportunity lease: {error}"
            ) from error
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or opened.st_size != 0
            or stat.S_IMODE(opened.st_mode) != 0o600
            or stat.S_ISLNK(linked.st_mode)
            or self._opportunity_identity(opened) != lease.file_identity
            or self._opportunity_identity(linked) != lease.file_identity
            or not stat.S_ISDIR(root.st_mode)
            or root.st_uid != os.geteuid()
            or stat.S_IMODE(root.st_mode) != 0o700
            or self._opportunity_identity(root) != lease.root_identity
        ):
            raise BackendError("retained GPU opportunity lease authority changed")
        try:
            # Reasserting LOCK_EX on the retained open-file description is a cheap
            # proof that the descriptor remains usable and exclusively held.
            fcntl.flock(lease.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise BackendError(
                f"retained GPU opportunity lease is no longer held: {error}"
            ) from error

    def _acquire_gpu_opportunity(self) -> bool:
        """Acquire the UUID-bound ordinary-opportunity lease without waiting.

        ``False`` is the one benign contention result. Every path/inode/metadata
        error remains fail-closed. The retained descriptor is deliberately owned
        by the GPU backend fork rather than by one finite scheduler call.
        """

        if self._lane_stage not in {None, "gpu_readiness"}:
            raise BackendError("only the GPU backend may own a GPU opportunity lease")
        existing = self._gpu_opportunity_lease
        if existing is not None:
            if existing.gpu_uuid != self._gpu_opportunity_uuid():
                raise BackendError(
                    "retained GPU opportunity lease differs from the production profile"
                )
            self._validate_gpu_opportunity_lease(existing)
            return True

        gpu_uuid = self._gpu_opportunity_uuid()
        path = self._gpu_opportunity_path(gpu_uuid)
        root_path = path.parent
        root_flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            root_descriptor = os.open(root_path, root_flags)
        except OSError as error:
            raise BackendError(
                f"cannot open GPU opportunity lock root: {error}"
            ) from error
        descriptor = -1
        try:
            try:
                root_opened = os.fstat(root_descriptor)
                root_linked = root_path.lstat()
            except OSError as error:
                raise BackendError(
                    f"cannot inspect GPU opportunity lock root: {error}"
                ) from error
            root_identity = self._opportunity_identity(root_opened)
            if (
                not stat.S_ISDIR(root_opened.st_mode)
                or root_opened.st_uid != os.geteuid()
                or stat.S_IMODE(root_opened.st_mode) != 0o700
                or stat.S_ISLNK(root_linked.st_mode)
                or self._opportunity_identity(root_linked) != root_identity
            ):
                raise BackendError(
                    "GPU opportunity lock root must be a current-user mode-0700 directory"
                )

            flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(
                os, "O_NOFOLLOW", 0
            )
            created = False
            try:
                descriptor = os.open(
                    path.name,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=root_descriptor,
                )
                created = True
            except FileExistsError:
                try:
                    descriptor = os.open(
                        path.name, flags, dir_fd=root_descriptor
                    )
                except OSError as error:
                    raise BackendError(
                        f"cannot open GPU opportunity lock: {error}"
                    ) from error
            except OSError as error:
                raise BackendError(
                    f"cannot create GPU opportunity lock: {error}"
                ) from error
            if created:
                os.fchmod(descriptor, 0o600)
                os.fsync(descriptor)
                os.fsync(root_descriptor)

            try:
                opened = os.fstat(descriptor)
                linked = os.stat(
                    path.name, dir_fd=root_descriptor, follow_symlinks=False
                )
            except OSError as error:
                raise BackendError(
                    f"cannot inspect GPU opportunity lock: {error}"
                ) from error
            file_identity = self._opportunity_identity(opened)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.geteuid()
                or opened.st_nlink != 1
                or opened.st_size != 0
                or stat.S_IMODE(opened.st_mode) != 0o600
                or self._opportunity_identity(linked) != file_identity
                or opened.st_dev != root_opened.st_dev
            ):
                raise BackendError(
                    "GPU opportunity lock must be an owned empty mode-0600 singleton file"
                )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            except OSError as error:
                raise BackendError(
                    f"cannot acquire GPU opportunity lock: {error}"
                ) from error

            try:
                root_after = root_path.lstat()
                linked_after = os.stat(
                    path.name, dir_fd=root_descriptor, follow_symlinks=False
                )
            except OSError as error:
                raise BackendError(
                    f"cannot validate acquired GPU opportunity lock: {error}"
                ) from error
            if (
                self._opportunity_identity(root_after) != root_identity
                or self._opportunity_identity(linked_after) != file_identity
            ):
                raise BackendError(
                    "GPU opportunity lock authority changed while acquiring"
                )
            self._gpu_opportunity_lease = _GpuOpportunityLease(
                descriptor=descriptor,
                gpu_uuid=gpu_uuid,
                path=path,
                root_identity=root_identity,
                file_identity=file_identity,
            )
            descriptor = -1
            return True
        finally:
            if descriptor >= 0:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except OSError:
                    pass
                os.close(descriptor)
            os.close(root_descriptor)

    def _release_gpu_opportunity(self) -> None:
        lease = self._gpu_opportunity_lease
        if lease is None:
            return
        error: BaseException | None = None
        try:
            self._validate_gpu_opportunity_lease(lease)
        except BaseException as observed:
            error = observed
        finally:
            self._gpu_opportunity_lease = None
            try:
                fcntl.flock(lease.descriptor, fcntl.LOCK_UN)
            except OSError as observed:
                if error is None:
                    error = BackendError(
                        f"cannot release GPU opportunity lock: {observed}"
                    )
            try:
                os.close(lease.descriptor)
            except OSError as observed:
                if error is None:
                    error = BackendError(
                        f"cannot close GPU opportunity lock: {observed}"
                    )
        if error is not None:
            raise error

    def _load_gpu_child_records(
        self, executor: Any | None, *, refresh: bool = False
    ) -> dict[str, GpuChildRecord]:
        if refresh:
            # Clear first so a failed reload cannot make later exact quiesce use
            # a known-stale pre-launch view.
            self._gpu_child_records = None
        if self._gpu_child_records is None:
            if executor is None:
                # Terminal recovery needs immutable child history, not authority
                # to launch systemd services. Keep offline maintenance usable
                # without inventing an outer-controller invocation identity.
                journal = PrivateGpuChildJournal(
                    Path(self.config.section("gpu_readiness")["child_journal_root"])
                )
                with journal.authority_lock():
                    records = journal.list_records()
            else:
                records = executor.journal.list_records()
            loaded = {record.unit_name: record for record in records}
            if len(loaded) != len(records):
                raise BackendError("GPU child journal repeats a deterministic unit")
            self._gpu_child_records = loaded
        return self._gpu_child_records

    def _schedule_runtime(
        self,
        expected: dict[str, Any],
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        list[dict[str, Any] | None],
        dict[str, Any],
    ]:
        schedule, schedule_path, schedule_body = self.modules.background.load_schedule(
            Path(expected["path"])
        )
        if (
            schedule_path != Path(expected["path"])
            or sha256_bytes(schedule_body) != expected["sha256"]
            or schedule["schedule_id"] != expected["schedule_id"]
        ):
            raise BackendError("sealed producer schedule differs from controller config")
        binding = self._queue_replay_binding(expected, schedule)
        replay_store = self._operational_replay_store
        if replay_store is None:
            bundle, states, ready, _summary = self.modules.background._load_runtime(
                schedule
            )
            return schedule, bundle, states, ready

        def load_with(session) -> tuple[tuple[Any, Any, Any, Any], dict[str, Any]]:
            with session as active:
                runtime = self.modules.background._load_runtime(schedule)
            delta = self._replay_delta(active)
            if delta is None:
                raise BackendError("sealed runtime replay emitted no authority delta")
            return runtime, delta

        def queue_state_admission_race(error: Exception) -> bool:
            queue_error = getattr(
                getattr(self.modules, "queue_runner", None),
                "QueueRunnerError",
                None,
            )
            return (
                isinstance(queue_error, type)
                and type(error) is queue_error
                and str(error) == _QUEUE_RUNTIME_STATE_ADMISSION_RACE
            )

        try:
            # A read-only runtime replay can safely repeat if another lane commits
            # a monotonic result while it is reading.  The mutating caller is never
            # repeated: only this bounded projection reload is retried until the
            # session-returned state equals the merged shared authority.
            projection_attempt = 0
            admission_attempt = 0
            while projection_attempt < 8:
                try:
                    if replay_store.has_snapshot(binding):
                        try:
                            runtime, delta = load_with(
                                replay_store.operational_session(binding)
                            )
                        except DeepAuditRequired:
                            # Runtime validation is read-only. A witness/bundle
                            # mismatch can therefore fall back to the unchanged
                            # source's full two-pass replay without repeating an
                            # external side effect.
                            runtime, delta = load_with(
                                replay_store.deep_capture(binding)
                            )
                    else:
                        # A fresh backend always earns its first in-memory witnesses
                        # from the source validator's complete two-pass payload replay.
                        runtime, delta = load_with(
                            replay_store.deep_capture(binding)
                        )
                except Exception as error:
                    admission_attempt += 1
                    if (
                        not queue_state_admission_race(error)
                        or admission_attempt == QUEUE_RUNTIME_SNAPSHOT_ATTEMPTS
                    ):
                        raise
                    # An exceptional replay session cannot commit. Repeat the
                    # complete read-only source validation against the last shared
                    # snapshot after the atomic publisher has closed its brief
                    # admission window.
                    time.sleep(QUEUE_RUNTIME_SNAPSHOT_RETRY_SECONDS)
                    continue
                projection_attempt += 1
                if self._replay_delta_returned_projection_is_current(delta):
                    break
            else:
                raise BackendError(
                    "read-only schedule runtime did not reach one current replay projection"
                )
        except OperationalReplayError as error:
            raise BackendError(
                f"queue operational replay failed for {expected['schedule_id']}: {error}"
            ) from error
        bundle, states, ready, _summary = runtime
        return schedule, bundle, states, ready

    def _queue_replay_binding(
        self, reference: dict[str, Any], schedule: dict[str, Any]
    ) -> QueueBinding:
        try:
            queue = schedule["queue"]
            return QueueBinding(
                schedule_id=reference["schedule_id"],
                role=reference["role"],
                schedule_sha256=reference["sha256"],
                manifest_path=Path(queue["manifest_path"]),
                manifest_sha256=queue["manifest_sha256"],
                bundle_id=queue["bundle_id"],
            )
        except (KeyError, TypeError, OperationalReplayError) as error:
            raise BackendError("sealed schedule lacks an exact replay binding") from error

    @contextmanager
    def _operational_queue_replay(
        self, reference: dict[str, Any], schedule: dict[str, Any]
    ):
        """Route one mutating source invocation through a seeded TLS session."""

        replay_store = self._operational_replay_store
        if replay_store is None:
            yield None
            return
        binding = self._queue_replay_binding(reference, schedule)
        try:
            try:
                session = replay_store.operational_session(binding)
            except DeepAuditRequired:
                # Only the read-only boundary is repeated here. The producer or
                # handoff has not started, so its finite mutation budget is not
                # amplified by this fallback.
                self._runtime_cache.pop(reference["schedule_id"], None)
                self._runtime_cache[reference["schedule_id"]] = (
                    self._schedule_runtime(reference)
                )
                session = replay_store.operational_session(binding)
            with session as active:
                yield active
        except OperationalReplayError as error:
            self._runtime_cache.pop(reference["schedule_id"], None)
            raise BackendError(
                f"queue operational replay failed for {reference['schedule_id']}: {error}"
            ) from error

    @staticmethod
    def _replay_delta(session: Any) -> dict[str, Any] | None:
        if session is None:
            return None
        delta = session.delta
        if not isinstance(delta, dict):
            raise BackendError("queue operational replay emitted no committed delta")
        return deepcopy(delta)

    @staticmethod
    def _replay_delta_returned_projection_is_current(
        delta: dict[str, Any] | None,
    ) -> bool:
        """Whether runtime objects returned by a session equal commit authority."""

        if delta is None:
            return True
        try:
            digests = (delta["session_state_digest"], delta["state_digest"])
            counts = (
                delta["session_completed_count"],
                delta["session_pending_count"],
                delta["completed_count"],
                delta["pending_count"],
            )
        except (KeyError, TypeError) as error:
            raise BackendError("queue replay delta lacks its returned projection") from error
        if (
            any(
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
                for value in digests
            )
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                for value in counts
            )
            or counts[0] + counts[1] != counts[2] + counts[3]
        ):
            raise BackendError("queue replay returned projection is malformed")
        return (
            digests[0] == digests[1]
            and counts[0] == counts[2]
            and counts[1] == counts[3]
        )

    def _verify_peer_replay_delta(
        self, value: Any, *, schedule_id: str
    ) -> dict[str, Any] | None:
        replay_store = self._operational_replay_store
        if replay_store is None:
            if value is not None:
                raise BackendError(
                    "peer supplied queue replay authority to an unbound backend"
                )
            return None
        if not isinstance(value, dict) or value.get("schedule_id") != schedule_id:
            raise BackendError("peer queue replay delta has the wrong schedule binding")
        try:
            return replay_store.verify_peer_delta(value)
        except OperationalReplayError as error:
            self._runtime_cache.pop(schedule_id, None)
            raise BackendError(f"peer queue replay delta failed: {error}") from error

    def _record_checkpoint_queue_delta(self, value: dict[str, Any]) -> None:
        """Advance only journal-observed queue authority, never a lane preview."""

        schedule_id = value.get("schedule_id") if isinstance(value, dict) else None
        generation = value.get("generation") if isinstance(value, dict) else None
        state_digest = value.get("state_digest") if isinstance(value, dict) else None
        if (
            not isinstance(schedule_id, str)
            or not schedule_id
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or not isinstance(state_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", state_digest) is None
        ):
            raise BackendError("checkpoint queue delta is malformed")
        previous = self._checkpoint_queue_snapshots.get(schedule_id)
        if previous is not None:
            if generation < previous["generation"]:
                # Completion-order delivery can expose a historical peer after a
                # newer schedule generation was already journaled.
                return
            if (
                generation == previous["generation"]
                and state_digest != previous["state_digest"]
            ):
                raise BackendError("checkpoint queue generation conflicts")
        self._checkpoint_queue_snapshots[schedule_id] = {
            "generation": generation,
            "state_digest": state_digest,
        }

    def _record_own_outcome_queue_authority(self, outcome: StageOutcome) -> None:
        """Record a root-executed delta which the caller will journal next."""

        if self._lane_stage is not None or outcome.stage not in {
            "acquisition",
            "preprocess",
        }:
            return
        transient = outcome.artifacts.get(TRANSIENT_PEER_RUNTIME_ARTIFACT)
        if transient is None:
            return
        schedule_id = (
            transient.get("schedule_id") if isinstance(transient, dict) else None
        )
        raw_delta = (
            transient.get("operational_replay")
            if isinstance(transient, dict)
            else None
        )
        if not isinstance(schedule_id, str):
            raise BackendError("root outcome queue authority is malformed")
        verified = self._verify_peer_replay_delta(
            raw_delta, schedule_id=schedule_id
        )
        if verified is None:
            raise BackendError("root outcome lacks checkpoint queue authority")
        self._record_checkpoint_queue_delta(verified)

    def _peer_acquisition_replay_is_superseded(
        self,
        replay_delta: dict[str, Any] | None,
        *,
        reference: dict[str, Any],
        schedule: dict[str, Any],
    ) -> bool:
        """Return whether newer exact queue authority supersedes a peer summary."""

        replay_store = self._operational_replay_store
        if replay_store is None:
            if replay_delta is not None:
                raise BackendError(
                    "peer supplied queue replay authority to an unbound backend"
                )
            return False
        if replay_delta is None:
            raise BackendError("peer acquisition runtime lacks queue replay authority")
        schedule_id = reference["schedule_id"]
        try:
            current = replay_store.snapshot_summary(
                self._queue_replay_binding(reference, schedule)
            )
        except OperationalReplayError as error:
            self._runtime_cache.pop(schedule_id, None)
            raise BackendError(
                f"peer queue replay generation lookup failed: {error}"
            ) from error
        peer_generation = replay_delta["generation"]
        current_generation = current["generation"]
        if peer_generation > current_generation:
            self._runtime_cache.pop(schedule_id, None)
            raise BackendError(
                "peer queue replay generation is ahead of shared exact authority"
            )
        if peer_generation < current_generation:
            return True
        if (
            replay_delta["state_digest"] != current["state_digest"]
            or replay_delta["completed_count"] != current["completed_count"]
            or replay_delta["pending_count"] != current["pending_count"]
        ):
            self._runtime_cache.pop(schedule_id, None)
            raise BackendError(
                "peer queue replay generation conflicts with shared exact authority"
            )
        return not self._replay_delta_returned_projection_is_current(
            replay_delta
        )

    def _is_temporal_preprocess_receipt_conflict(self, error: Exception) -> bool:
        """Recognize only the exact source race that newer authority resolves."""

        error_type = getattr(
            self.modules.background, "BackgroundProducerError", None
        )
        return (
            not isinstance(error, BackendError)
            and isinstance(error_type, type)
            and type(error) is error_type
            and str(error) == _TEMPORAL_PREPROCESS_RECEIPT_CONFLICT
        )

    def _admit_peer_acquisition_runtime(
        self,
        replay_delta: dict[str, Any] | None,
        *,
        reference: dict[str, Any],
        schedule: dict[str, Any],
        bundle: dict[str, Any],
        states: list[dict[str, Any] | None],
        ready: dict[str, Any],
    ) -> bool:
        """Atomically admit replayed runtime objects if authority is still current."""

        replay_store = self._operational_replay_store
        schedule_id = reference["schedule_id"]
        if replay_store is None:
            if replay_delta is not None:
                raise BackendError(
                    "peer supplied queue replay authority to an unbound backend"
                )
            self._update_cached_runtime(reference, schedule, bundle, states, ready)
            return True
        if replay_delta is None:
            raise BackendError("peer acquisition runtime lacks queue replay authority")
        try:
            admitted = replay_store.admit_if_snapshot_current(
                self._queue_replay_binding(reference, schedule),
                generation=replay_delta["generation"],
                state_digest=replay_delta["state_digest"],
                completed_count=replay_delta["completed_count"],
                pending_count=replay_delta["pending_count"],
                admit=lambda: self._update_cached_runtime(
                    reference, schedule, bundle, states, ready
                ),
            )
        except OperationalReplayError as error:
            self._runtime_cache.pop(schedule_id, None)
            raise BackendError(
                f"peer queue replay cache admission failed: {error}"
            ) from error
        if not admitted:
            self._runtime_cache.pop(schedule_id, None)
        return admitted

    def _campaign_runtimes(
        self,
    ) -> list[
        tuple[
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            list[dict[str, Any] | None],
            dict[str, Any],
        ]
    ]:
        rows = []
        for reference in self.config.section("campaign")["schedules"]:
            cached = self._runtime_cache.get(reference["schedule_id"])
            if cached is None:
                cached = self._schedule_runtime(reference)
                self._runtime_cache[reference["schedule_id"]] = cached
            schedule, bundle, states, ready = cached
            rows.append((reference, schedule, bundle, states, ready))
        return rows

    def _update_cached_runtime(
        self,
        reference: dict[str, Any],
        schedule: dict[str, Any],
        bundle: dict[str, Any],
        states: list[dict[str, Any] | None],
        ready: dict[str, Any],
    ) -> None:
        self._runtime_cache[reference["schedule_id"]] = (
            schedule,
            bundle,
            states,
            ready,
        )

    @staticmethod
    def _mountinfo_path(value: str) -> str:
        for encoded, decoded in (
            ("\\040", " "),
            ("\\011", "\t"),
            ("\\012", "\n"),
            ("\\134", "\\"),
        ):
            value = value.replace(encoded, decoded)
        return value

    def _validate_cold_storage_identity(
        self,
        runtimes: Sequence[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                list[dict[str, Any] | None],
                dict[str, Any],
            ]
        ],
    ) -> dict[str, Any]:
        """Bind cold-primary writes to the reviewed XFS device, not a pathname."""

        try:
            if COLD_MOUNT.resolve(strict=True) != COLD_MOUNT:
                raise BackendError("cold mount point may not traverse a symlink")
            mount_stat = COLD_MOUNT.stat()
            if not stat.S_ISDIR(mount_stat.st_mode):
                raise BackendError("cold mount point is not a directory")
            body = Path("/proc/self/mountinfo").read_bytes()
        except BackendError:
            raise
        except OSError as error:
            raise BackendError(f"cannot inspect cold mount identity: {error}") from error
        if not 1 <= len(body) <= 4 * 1024 * 1024:
            raise BackendError("mountinfo is outside its bounded size")
        rows: list[tuple[str, str, str]] = []
        for raw_line in body.decode("utf-8", "strict").splitlines():
            fields = raw_line.split(" ")
            try:
                separator = fields.index("-")
            except ValueError:
                raise BackendError("mountinfo row lacks a filesystem separator")
            if len(fields) < 6 or separator + 3 > len(fields):
                raise BackendError("mountinfo row is malformed")
            mount_point = self._mountinfo_path(fields[4])
            if mount_point == str(COLD_MOUNT):
                rows.append((fields[2], fields[separator + 1], fields[separator + 2]))
        if len(rows) != 1:
            raise BackendError("/mnt/archive is not one exact mount point")
        device_number, filesystem, source = rows[0]
        try:
            major_text, minor_text = device_number.split(":", 1)
            major, minor = int(major_text), int(minor_text)
        except (TypeError, ValueError) as error:
            raise BackendError("cold mount device number is malformed") from error
        if (
            filesystem != EXPECTED_COLD_MOUNT_FSTYPE
            or (os.major(mount_stat.st_dev), os.minor(mount_stat.st_dev))
            != (major, minor)
        ):
            raise BackendError("cold mount is not the reviewed XFS device")
        transition = reviewed_cold_mount_transition(self.config)
        # normalize_config admits only the historical UUID or the reviewed
        # replacement. Fresh campaigns seal that choice in their own identity.
        expected_uuid = self.config.section("safety")["cold_mount_uuid"]
        if transition is not None:
            if (
                transition["from_uuid"] != expected_uuid
                or self.config.section("safety")["cold_mount_uuid"] != expected_uuid
                or transition["mount_point"] != str(COLD_MOUNT)
                or transition["filesystem"] != filesystem
            ):
                raise BackendError("reviewed cold mount transition binding differs")
            expected_uuid = transition["to_uuid"]
        uuid_path = Path("/dev/disk/by-uuid") / expected_uuid
        try:
            device_path = uuid_path.resolve(strict=True)
            device_stat = device_path.stat()
            source_path = Path(source).resolve(strict=True)
            source_stat = source_path.stat()
        except OSError as error:
            raise BackendError(f"cannot resolve reviewed cold device UUID: {error}") from error
        if (
            not stat.S_ISBLK(device_stat.st_mode)
            or not stat.S_ISBLK(source_stat.st_mode)
            or (os.major(device_stat.st_rdev), os.minor(device_stat.st_rdev))
            != (major, minor)
            or (source_stat.st_rdev != device_stat.st_rdev)
        ):
            raise BackendError("cold mount source differs from the reviewed UUID")

        cold = self.config.section("cold_retention")
        acquisition_roots = [
            Path(bundle["manifest"]["policy"]["media_output_root"])
            for _reference, _schedule, bundle, _states, _ready in runtimes
        ]
        if not cold["enabled"] and any(
            root == FIXED_COLD_ROOT or FIXED_COLD_ROOT not in root.parents
            for root in acquisition_roots
        ):
            raise BackendError(
                "disabled cold retention requires dedicated cold-primary acquisition roots"
            )
        cold_outputs = [
            root
            for root in acquisition_roots
            if root == FIXED_COLD_ROOT or FIXED_COLD_ROOT in root.parents
        ]
        cold_outputs.append(Path(cold["destination_root"]))
        try:
            cold_root_resolved = FIXED_COLD_ROOT.resolve(strict=True)
            if cold_root_resolved != FIXED_COLD_ROOT:
                raise BackendError("cold HIMR root may not traverse a symlink")
            for output in cold_outputs:
                resolved = output.resolve(strict=True)
                if (
                    resolved != output
                    or not (resolved == FIXED_COLD_ROOT or FIXED_COLD_ROOT in resolved.parents)
                    or resolved.stat().st_dev != mount_stat.st_dev
                ):
                    raise BackendError(
                        "cold output is not on the reviewed cold mount device"
                    )
        except BackendError:
            raise
        except OSError as error:
            raise BackendError(f"cannot inspect cold output device: {error}") from error
        projection = {
            "mount_point": str(COLD_MOUNT),
            "filesystem": filesystem,
            "uuid": expected_uuid,
            "device_major_minor": device_number,
            "validated_cold_output_count": len(cold_outputs),
        }
        if transition is not None:
            projection["reviewed_migration"] = transition
        return projection

    def _completed_acquisitions(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen_results: set[str] = set()
        for _reference, schedule, bundle, states, _ready in self._campaign_runtimes():
            acquisition_root = Path(bundle["manifest"]["policy"]["media_output_root"])
            for entry, order, state in zip(
                bundle["manifest"]["work_orders"], bundle["orders"], states, strict=True
            ):
                if not self.modules.background._completed_state(state):
                    continue
                result_path = self.modules.queue_runner._result_path(order)
                if str(result_path) in seen_results:
                    raise BackendError("campaign schedules repeat an acquisition result path")
                seen_results.add(str(result_path))
                item_key = f"{schedule['schedule_id']}:{entry['queue_ordinal']}"
                rows.append(
                    {
                        "item_key": item_key,
                        "schedule_id": schedule["schedule_id"],
                        "ordinal": entry["queue_ordinal"],
                        "job_id": entry["job_id"],
                        "work_order": bundle["path"].parent / entry["path"],
                        "work_order_sha256": entry["sha256"],
                        "result": result_path,
                        "result_sha256": state["result_sha256"],
                        "media_sha256": state["media_sha256"],
                        "media_byte_count": state["byte_count"],
                        "acquisition_root": acquisition_root,
                    }
                )
        return rows

    def _runtime_counts(
        self,
        runtimes: Sequence[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                list[dict[str, Any] | None],
                dict[str, Any],
            ]
        ],
    ) -> tuple[int, int, int]:
        completed = sum(
            self.modules.background._completed_state(state)
            for row in runtimes
            for state in row[3]
        )
        quarantined = sum(
            self.modules.background._quarantined_state(state)
            for row in runtimes
            for state in row[3]
        )
        runnable = sum(state is None for row in runtimes for state in row[3])
        return completed, quarantined, runnable

    def _retention_request(self, row: dict[str, Any]) -> Any:
        cold = self.config.section("cold_retention")
        return self.modules.retention.RetentionRequest(
            work_order=row["work_order"],
            acquisition_root=row["acquisition_root"],
            staging_root=Path(cold["staging_root"]),
            receipt_root=Path(cold["receipt_root"]),
            expected_work_order_sha256=row["work_order_sha256"],
            expected_result_sha256=row["result_sha256"],
            expected_sha256=row["media_sha256"],
            expected_byte_count=row["media_byte_count"],
            free_space_floor_bytes=cold["free_space_floor_bytes"],
        )

    @staticmethod
    def _retention_record(row: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        receipt = result.get("cold_transfer_receipt")
        policy = result.get("policy")
        if (
            result.get("status") != "completed"
            or not isinstance(receipt, dict)
            or receipt.get("status") != "completed"
            or not isinstance(policy, dict)
            or policy.get("source_deleted") is not False
            or policy.get("source_mutated") is not False
            or policy.get("catalogue_mutated") is not False
            or policy.get("publication_authority") != "none"
            or policy.get("deletion_authority") != "none"
        ):
            raise BackendError("public retention returned an unsafe or incomplete result")
        return {
            "item_key": row["item_key"],
            "schedule_id": row["schedule_id"],
            "ordinal": row["ordinal"],
            "job_id": row["job_id"],
            "work_order_sha256": row["work_order_sha256"],
            "result_sha256": row["result_sha256"],
            "media_sha256": row["media_sha256"],
            "media_byte_count": row["media_byte_count"],
            "transfer_id": receipt["transfer_id"],
            "receipt_id": receipt["receipt_id"],
            "receipt_identity_sha256": receipt["identity_sha256"],
            "cold_relative_path": receipt["destination"]["relative_path"],
        }

    def _load_profile(self) -> dict[str, Any]:
        if self._profile is None:
            gpu = self.config.section("gpu_readiness")
            try:
                self._profile = self.modules.gpu_bridge.ASR_V5.load_profile_document(
                    gpu["production_profile"], gpu["production_profile_sha256"]
                )
            except Exception as error:
                raise BackendError(f"production profile replay failed: {error}") from error
        return self._profile

    @staticmethod
    def _inventory_coverage(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict) or set(value) != {
            "kind",
            "schema_version",
            "scope",
            "inventory_basis",
            "collections",
            "totals",
            "policy",
        }:
            raise BackendError("campaign inventory has unexpected fields")
        policy = value["policy"]
        basis = value["inventory_basis"]
        expected_policy = {
            "access": "public_unauthenticated_only",
            "credentials_allowed": False,
            "youtube_in_campaign": False,
            "catalogue_mutation_authority": "none",
            "publication_authority": "none",
            "parked_items_are_not_silently_dropped": True,
        }
        if (
            value["kind"] != "himr_known_archive_collection_inventory"
            or value["schema_version"] != 1
            or value["scope"] not in INVENTORY_SCOPES
            or not isinstance(basis, dict)
            or set(basis)
            != {
                "archive_metadata_request",
                "broad_plan",
                "archive_only_queue_plan",
            }
            or not isinstance(value["collections"], list)
            or policy != expected_policy
        ):
            raise BackendError("campaign inventory header or safety policy is unsupported")
        for label, reference in basis.items():
            if (
                not isinstance(reference, dict)
                or set(reference) != {"path", "sha256"}
                or not isinstance(reference["path"], str)
                or not Path(reference["path"]).is_absolute()
                or not isinstance(reference["sha256"], str)
                or len(reference["sha256"]) != 64
                or any(character not in "0123456789abcdef" for character in reference["sha256"])
            ):
                raise BackendError(f"campaign inventory {label} binding is malformed")
        fields = {
            "candidate_count",
            "ready_selected_count",
            "parked_requires_chunking_count",
            "estimated_selected_bytes",
            "total_duration_ms",
        }
        sums = {field: 0 for field in fields}
        identifiers: set[str] = set()
        for row in value["collections"]:
            if not isinstance(row, dict) or set(row) != {"identifier", *fields}:
                raise BackendError("campaign inventory collection row is malformed")
            identifier = row["identifier"]
            if not isinstance(identifier, str) or not identifier or identifier in identifiers:
                raise BackendError("campaign inventory repeats or omits a collection ID")
            identifiers.add(identifier)
            for field in fields:
                observed = row[field]
                if isinstance(observed, bool) or not isinstance(observed, int) or observed < 0:
                    raise BackendError(f"campaign inventory {field} is invalid")
                sums[field] += observed
            if row["candidate_count"] != (
                row["ready_selected_count"] + row["parked_requires_chunking_count"]
            ):
                raise BackendError("campaign collection candidate disposition is incomplete")
        totals = value["totals"]
        total_keys = fields | {"collection_count", "ready_selected_duration_ms"}
        if not isinstance(totals, dict) or set(totals) != total_keys:
            raise BackendError("campaign inventory totals are malformed")
        if any(
            isinstance(totals[key], bool)
            or not isinstance(totals[key], int)
            or totals[key] < 0
            for key in total_keys
        ):
            raise BackendError("campaign inventory totals are outside their bounds")
        if totals["collection_count"] != len(identifiers) or any(
            totals[field] != sums[field] for field in fields
        ):
            raise BackendError("campaign inventory totals differ from collection rows")
        if totals["candidate_count"] != (
            totals["ready_selected_count"] + totals["parked_requires_chunking_count"]
        ):
            raise BackendError("campaign total candidate disposition is incomplete")
        return {
            "collection_count": totals["collection_count"],
            "candidate_count": totals["candidate_count"],
            "ready_selected_count": totals["ready_selected_count"],
            "parked_requires_chunking_count": totals[
                "parked_requires_chunking_count"
            ],
            "estimated_selected_bytes": totals["estimated_selected_bytes"],
            "total_duration_ms": totals["total_duration_ms"],
            "ready_selected_duration_ms": totals["ready_selected_duration_ms"],
            "collections": [
                {
                    "identifier": row["identifier"],
                    "candidate_count": row["candidate_count"],
                    "ready_selected_count": row["ready_selected_count"],
                    "parked_requires_chunking_count": row[
                        "parked_requires_chunking_count"
                    ],
                    "estimated_selected_bytes": row[
                        "estimated_selected_bytes"
                    ],
                }
                for row in value["collections"]
            ],
        }

    @staticmethod
    def _expected_archive_role_identities(
        plan: dict[str, Any], inventory_coverage: dict[str, Any]
    ) -> dict[str, set[tuple[str, str]]]:
        """Project the exact Archive identities covered by one sealed campaign.

        A selection-only addendum plan intentionally retains the wider catalogue
        as deferred context.  Those outside-selection rows are not campaign work.
        Require exact Archive source-ID selection so a recording/native selector
        cannot accidentally pull an older collection into the addendum.
        """

        candidates = plan.get("candidates") if isinstance(plan, dict) else None
        limits = plan.get("limits") if isinstance(plan, dict) else None
        selection = plan.get("selection_basis") if isinstance(plan, dict) else None
        collections = (
            inventory_coverage.get("collections")
            if isinstance(inventory_coverage, dict)
            else None
        )
        if (
            not isinstance(candidates, list)
            or not isinstance(limits, dict)
            or not isinstance(limits.get("selection_only"), bool)
            or not isinstance(selection, dict)
            or not isinstance(collections, list)
        ):
            raise BackendError("Archive campaign plan selection projection is malformed")

        collection_ids = {
            row.get("identifier")
            for row in collections
            if isinstance(row, dict) and isinstance(row.get("identifier"), str)
        }
        if len(collection_ids) != len(collections) or not collection_ids:
            raise BackendError("Archive campaign inventory collection scope is malformed")

        archive_by_source: dict[str, dict[str, Any]] = {}
        for row in candidates:
            if not isinstance(row, dict):
                raise BackendError("Archive campaign plan candidate is malformed")
            if row.get("platform") != "internet_archive":
                continue
            source_id = row.get("source_id")
            recording_id = row.get("recording_id")
            native_id = row.get("native_id")
            if (
                not isinstance(source_id, str)
                or not source_id
                or not isinstance(recording_id, str)
                or not recording_id
                or row.get("source_kind") != "archive_media_file"
                or not isinstance(native_id, str)
                or "/" not in native_id
                or source_id in archive_by_source
            ):
                raise BackendError("Archive campaign plan candidate identity is malformed")
            archive_by_source[source_id] = row

        selected_rows: list[dict[str, Any]]
        if limits["selection_only"]:
            source_ids = selection.get("source_ids")
            youtube_ids = selection.get("youtube_video_ids")
            recording_ids = selection.get("recording_ids")
            if (
                not isinstance(source_ids, list)
                or not source_ids
                or any(not isinstance(value, str) or not value for value in source_ids)
                or source_ids != sorted(source_ids)
                or len(source_ids) != len(set(source_ids))
                or youtube_ids != []
                or recording_ids != []
            ):
                raise BackendError(
                    "selection-only Archive campaign requires exact source IDs only"
                )
            selected_rows = []
            for source_id in source_ids:
                row = archive_by_source.get(source_id)
                if row is None:
                    raise BackendError(
                        "selection-only Archive source is absent or outside Archive scope"
                    )
                item_identifier = row["native_id"].split("/", 1)[0]
                if item_identifier not in collection_ids:
                    raise BackendError(
                        "selection-only Archive source is outside inventory collections"
                    )
                if row.get("priority_tier") != "explicit_selection":
                    raise BackendError(
                        "selection-only Archive source lacks explicit-selection priority"
                    )
                selected_rows.append(row)
        else:
            selected_rows = list(archive_by_source.values())

        by_role: dict[str, set[tuple[str, str]]] = {
            "normal_processing": set(),
            "cold_acquisition_only_requires_chunking": set(),
        }
        for row in selected_rows:
            identity = (row["source_id"], row["recording_id"])
            if row.get("queue_state") == "ready":
                if limits["selection_only"] and (
                    not isinstance(row.get("queue_ordinal"), int)
                    or isinstance(row.get("queue_ordinal"), bool)
                    or row.get("defer_reason") is not None
                ):
                    raise BackendError(
                        "selection-only ready Archive source was not admitted to the queue"
                    )
                by_role["normal_processing"].add(identity)
            elif row.get("queue_state") == "requires_chunking":
                by_role["cold_acquisition_only_requires_chunking"].add(identity)
            elif limits["selection_only"]:
                raise BackendError(
                    "selection-only Archive source has an unsupported queue disposition"
                )
        return by_role

    def _validate_schedule_set_manifest(
        self,
        runtimes: Sequence[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                list[dict[str, Any] | None],
                dict[str, Any],
            ]
        ],
        inventory_coverage: dict[str, Any],
    ) -> dict[str, Any]:
        reference = self.config.section("campaign")["schedule_set"]
        if reference["kind"] == "sealed_archive_campaign_composite_schedule_set":
            return self._validate_composite_schedule_set_manifest(
                runtimes, inventory_coverage
            )
        module = self.modules.schedule_set
        try:
            body, _ = self.modules.queue_runner._stable_read(
                Path(reference["path"]),
                maximum=module.MAX_SCHEDULE_SET_BYTES,
                label="sealed Archive campaign schedule set",
                required_mode=0o400,
            )
            value = json.loads(body)
        except Exception as error:
            raise BackendError(f"campaign schedule-set replay failed: {error}") from error
        top_keys = {
            "schedule_set_id",
            "identity_sha256",
            "schema_version",
            "schedule_set_kind",
            "materializer",
            "storage_policy",
            "role_order",
            "role_policies",
            "source_campaigns",
            "schedules",
            "role_totals",
            "coverage_proof",
            "safety",
        }
        if not isinstance(value, dict) or set(value) != top_keys:
            raise BackendError("campaign schedule-set fields differ from the closed contract")
        core = {
            key: value[key]
            for key in top_keys - {"schedule_set_id", "identity_sha256"}
        }
        identity = sha256_bytes(module.canonical_bytes(core))
        if (
            sha256_bytes(body) != reference["sha256"]
            or value["schedule_set_id"] != reference["schedule_set_id"]
            or value["schedule_set_kind"] != reference["kind"]
            or value["schema_version"] != module.SCHEMA_VERSION
            or value["materializer"]
            != {"name": module.MATERIALIZER_NAME, "version": module.IMPLEMENTATION_VERSION}
            or value["identity_sha256"] != identity
            or value["schedule_set_id"] != f"bgacqscheduleset_{identity[:32]}"
            or body != module.pretty_bytes(value)
            or value["safety"] != module.SCHEDULE_SET_SAFETY
            or value["role_order"] != list(module.ROLE_ORDER)
        ):
            raise BackendError("campaign schedule-set header, identity, or safety differs")
        storage = value["storage_policy"]
        if not isinstance(storage, dict) or set(storage) != {
            "media_output_root",
            "control_root",
            "schedules_root",
            "preprocess_state_root",
            "schedule_sets_root",
            "storage_mode",
        }:
            raise BackendError("campaign schedule-set storage policy is malformed")
        control_root = Path(storage["control_root"])
        if (
            storage["media_output_root"] != str(module.COLD_MEDIA_ROOT)
            or storage["storage_mode"] != "cold_primary_shared_cas_hot_controls"
            or Path(storage["schedules_root"]) != control_root / "schedules"
            or Path(storage["preprocess_state_root"])
            != control_root / "preprocess-state"
            or Path(storage["schedule_sets_root"]) != control_root / "schedule-sets"
            or Path(reference["path"])
            != Path(storage["schedule_sets_root"])
            / value["schedule_set_id"]
            / "manifest.json"
        ):
            raise BackendError("campaign schedule-set cold-primary storage policy differs")
        expected_role_policies = [
            {
                "role": role,
                **module.ROLE_EPOCH_CAPS[role],
                **module._expected_schedule_policy(role),
            }
            for role in module.ROLE_ORDER
        ]
        if value["role_policies"] != expected_role_policies:
            raise BackendError("campaign schedule-set role policies differ")

        schedule_rows = value["schedules"]
        configured = self.config.section("campaign")["schedules"]
        if (
            not isinstance(schedule_rows, list)
            or len(schedule_rows) != len(configured)
            or len(schedule_rows) != len(runtimes)
        ):
            raise BackendError("campaign schedule-set cardinality differs from config")
        selected_by_role = {role: 0 for role in module.ROLE_ORDER}
        bytes_by_role = {role: 0 for role in module.ROLE_ORDER}
        schedule_ids: set[str] = set()
        for ordinal, (entry, configured_ref, runtime) in enumerate(
            zip(schedule_rows, configured, runtimes, strict=True), 1
        ):
            if not isinstance(entry, dict) or set(entry) != {
                "schedule_ordinal",
                "role",
                "schedule_path",
                "schedule_sha256",
                "schedule_byte_count",
                "schedule_id",
                "schedule_identity_sha256",
                "preprocess_state_root",
                "source_campaign",
                "source_epoch",
            }:
                raise BackendError("campaign schedule-set entry is malformed")
            _runtime_ref, schedule, bundle, _states, _ready = runtime
            source_epoch = entry["source_epoch"]
            if not isinstance(source_epoch, dict) or set(source_epoch) != {
                "epoch_ordinal",
                "epoch_plan_id",
                "epoch_plan_path",
                "epoch_plan_sha256",
                "bundle_id",
                "bundle_manifest_path",
                "bundle_manifest_sha256",
                "selected_count",
                "selected_estimated_bytes",
                "member_sha256",
            }:
                raise BackendError("campaign schedule-set source epoch is malformed")
            projected = {
                "path": entry["schedule_path"],
                "sha256": entry["schedule_sha256"],
                "schedule_id": entry["schedule_id"],
                "role": entry["role"],
            }
            if (
                entry["schedule_ordinal"] != ordinal
                or projected != configured_ref
                or entry["schedule_id"] in schedule_ids
                or entry["schedule_identity_sha256"] != schedule["identity_sha256"]
                or entry["preprocess_state_root"]
                != schedule["consumer"]["preprocess_state_root"]
                or source_epoch["bundle_id"] != bundle["manifest"]["bundle_id"]
                or source_epoch["bundle_manifest_path"] != str(bundle["path"])
                or source_epoch["bundle_manifest_sha256"]
                != sha256_bytes(bundle["body"])
                or source_epoch["selected_count"] != len(bundle["orders"])
                or entry["schedule_byte_count"] <= 0
                or source_epoch["selected_estimated_bytes"] <= 0
            ):
                raise BackendError("campaign schedule-set entry differs from exact runtime")
            schedule_ids.add(entry["schedule_id"])
            selected_by_role[entry["role"]] += source_epoch["selected_count"]
            bytes_by_role[entry["role"]] += source_epoch[
                "selected_estimated_bytes"
            ]

        source_campaigns = value["source_campaigns"]
        role_totals = value["role_totals"]
        if (
            not isinstance(source_campaigns, list)
            or not isinstance(role_totals, list)
            or len(source_campaigns) != 2
            or len(role_totals) != 2
        ):
            raise BackendError("campaign schedule-set role summaries are malformed")
        for role, source, total in zip(
            module.ROLE_ORDER, source_campaigns, role_totals, strict=True
        ):
            role_schedule_rows = [row for row in schedule_rows if row["role"] == role]
            if (
                not isinstance(source, dict)
                or source.get("role") != role
                or not isinstance(total, dict)
                or total
                != {
                    "role": role,
                    "campaign_count": 1,
                    "epoch_count": len(role_schedule_rows),
                    "schedule_count": len(role_schedule_rows),
                    "selected_count": selected_by_role[role],
                    "selected_estimated_bytes": bytes_by_role[role],
                }
                or source.get("epoch_count") != len(role_schedule_rows)
                or source.get("selected_count") != selected_by_role[role]
                or source.get("selected_estimated_bytes") != bytes_by_role[role]
            ):
                raise BackendError("campaign schedule-set role totals differ")
        if (
            selected_by_role[module.NORMAL_ROLE]
            != inventory_coverage["ready_selected_count"]
            or selected_by_role[module.COLD_ONLY_ROLE]
            != inventory_coverage["parked_requires_chunking_count"]
        ):
            raise BackendError("campaign schedule-set roles differ from inventory totals")
        proof = value["coverage_proof"]
        selected_total = sum(selected_by_role.values())
        selected_bytes = sum(bytes_by_role.values())
        if (
            not isinstance(proof, dict)
            or proof.get("source_campaign_count") != 2
            or proof.get("source_epoch_count") != len(schedule_rows)
            or proof.get("schedule_count") != len(schedule_rows)
            or proof.get("source_selected_count") != selected_total
            or proof.get("schedule_selected_count") != selected_total
            or proof.get("source_selected_estimated_bytes") != selected_bytes
            or proof.get("schedule_selected_estimated_bytes") != selected_bytes
            or proof.get("unique_source_count") != selected_total
            or any(proof.get(key) != 0 for key in ("overlap_count", "missing_count", "unexpected_count"))
            or proof.get("source_epoch_union_sha256")
            != proof.get("schedule_epoch_union_sha256")
            or proof.get("source_member_union_sha256")
            != proof.get("schedule_member_union_sha256")
            or any(
                proof.get(key) is not True
                for key in (
                    "ordered_epoch_union_identical",
                    "ordered_member_union_identical",
                    "roles_exact_and_ordered",
                    "source_epochs_contiguous",
                    "schedule_ordinals_contiguous",
                )
            )
        ):
            raise BackendError("campaign schedule-set exact coverage proof differs")
        return {
            "schedule_set_id": value["schedule_set_id"],
            "schedule_count": len(schedule_rows),
            "normal_selected_count": selected_by_role[module.NORMAL_ROLE],
            "cold_only_selected_count": selected_by_role[module.COLD_ONLY_ROLE],
            "selected_count": selected_total,
            "selected_estimated_bytes": selected_bytes,
            "coverage_identity_sha256": proof["schedule_member_union_sha256"],
        }

    def _validate_composite_schedule_set_manifest(
        self,
        runtimes: Sequence[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                list[dict[str, Any] | None],
                dict[str, Any],
            ]
        ],
        inventory_coverage: dict[str, Any],
    ) -> dict[str, Any]:
        """Deeply replay and independently prove one predecessor/addendum union."""

        reference = self.config.section("campaign")["schedule_set"]
        module = self.modules.composite_schedule_set
        try:
            body, _ = self.modules.queue_runner._stable_read(
                Path(reference["path"]),
                maximum=module.MAX_COMPOSITE_BYTES,
                label="sealed Archive composite campaign schedule set",
                required_mode=0o400,
            )
            value = json.loads(body)
            top_keys = {
                "composite_schedule_set_id",
                "identity_sha256",
                "schema_version",
                "composite_kind",
                "materializer",
                "storage_policy",
                "component_order",
                "role_order",
                "flatten_order",
                "components",
                "schedules",
                "component_role_totals",
                "role_totals",
                "coverage_proof",
                "safety",
            }
            if not isinstance(value, dict) or set(value) != top_keys:
                raise BackendError(
                    "composite campaign schedule-set fields differ from the closed contract"
                )
            core = {
                key: value[key]
                for key in top_keys
                - {"composite_schedule_set_id", "identity_sha256"}
            }
            identity = sha256_bytes(module.canonical_bytes(core))
            composite_id = value["composite_schedule_set_id"]
            if (
                sha256_bytes(body) != reference["sha256"]
                or composite_id != reference["schedule_set_id"]
                or value["composite_kind"] != reference["kind"]
                or value["schema_version"] != module.SCHEMA_VERSION
                or value["materializer"]
                != {
                    "name": module.MATERIALIZER_NAME,
                    "version": module.IMPLEMENTATION_VERSION,
                }
                or value["identity_sha256"] != identity
                or composite_id != f"bgacqcompositeset_{identity[:32]}"
                or module.COMPOSITE_ID_RE.fullmatch(composite_id) is None
                or body != module.pretty_bytes(value)
                or value["safety"] != module.SAFETY
                or value["component_order"] != list(module.COMPONENT_ORDER)
                or value["role_order"] != list(module.ROLE_ORDER)
                or value["flatten_order"]
                != [
                    {"component": component, "role": role}
                    for component, role in module.FLATTEN_ORDER
                ]
            ):
                raise BackendError(
                    "composite campaign schedule-set header, identity, or safety differs"
                )

            storage = value["storage_policy"]
            if not isinstance(storage, dict) or set(storage) != {
                "media_output_root",
                "control_root",
                "composite_schedule_sets_root",
                "source_schedule_storage",
            }:
                raise BackendError(
                    "composite campaign schedule-set storage policy is malformed"
                )
            control_root = module._absolute_path(
                storage["control_root"], "composite schedule-set control root"
            )
            objects_root = control_root / "composite-schedule-sets"
            if (
                storage
                != {
                    "media_output_root": str(module.source_set.COLD_MEDIA_ROOT),
                    "control_root": str(control_root),
                    "composite_schedule_sets_root": str(objects_root),
                    "source_schedule_storage": "referenced_in_place_byte_for_byte",
                }
                or Path(reference["path"])
                != objects_root / composite_id / "manifest.json"
            ):
                raise BackendError(
                    "composite campaign schedule-set storage policy differs"
                )

            raw_components = value["components"]
            if not isinstance(raw_components, list) or len(raw_components) != 2:
                raise BackendError(
                    "composite campaign schedule-set must bind exactly two components"
                )
            components: dict[str, dict[str, Any]] = {}
            component_keys = {
                "component_ordinal",
                "component",
                "schedule_set_id",
                "manifest_path",
                "manifest_sha256",
                "manifest_byte_count",
                "campaign_count",
                "epoch_count",
                "schedule_count",
                "selected_count",
                "selected_estimated_bytes",
                "ordered_schedule_reference_sha256",
                "ordered_member_sha256",
            }
            for ordinal, (component_name, component_ref) in enumerate(
                zip(module.COMPONENT_ORDER, raw_components, strict=True), 1
            ):
                if (
                    not isinstance(component_ref, dict)
                    or set(component_ref) != component_keys
                    or component_ref["component_ordinal"] != ordinal
                    or component_ref["component"] != component_name
                ):
                    raise BackendError(
                        "composite campaign schedule-set component reference is malformed"
                    )
                component = module._validate_component(
                    component_name,
                    Path(component_ref["manifest_path"]),
                    component_ref["manifest_sha256"],
                )
                if component_ref != module._component_reference(component):
                    raise BackendError(
                        "composite campaign schedule-set component differs from deep replay"
                    )
                component_root = component["control_root"]
                if module.source_set._is_within(
                    control_root, component_root
                ) or module.source_set._is_within(component_root, control_root):
                    raise BackendError(
                        "composite and component control roots are not disjoint"
                    )
                components[component_name] = component

            expected_schedules: list[dict[str, Any]] = []
            source_schedule_projection: list[dict[str, Any]] = []
            source_member_rows: list[dict[str, Any]] = []
            expected_component_role_totals: list[dict[str, Any]] = []
            for component_name, role in module.FLATTEN_ORDER:
                component = components[component_name]
                role_schedules = [
                    row
                    for row in component["schedules"]
                    if row["entry"]["role"] == role
                ]
                if not role_schedules:
                    raise BackendError(
                        "composite campaign schedule-set omits one component role"
                    )
                role_members: list[dict[str, Any]] = []
                for source in role_schedules:
                    entry = source["entry"]
                    source_schedule_projection.append(
                        module._schedule_projection(component_name, entry)
                    )
                    members = module._member_rows(
                        component_name,
                        role,
                        entry["source_campaign"]["campaign_id"],
                        entry["source_epoch"]["epoch_ordinal"],
                        source["members"],
                    )
                    source_member_rows.extend(members)
                    role_members.extend(members)
                    expected_schedules.append(
                        {
                            "schedule_ordinal": len(expected_schedules) + 1,
                            "component": component_name,
                            "component_schedule_ordinal": entry[
                                "schedule_ordinal"
                            ],
                            "role": role,
                            "schedule_path": entry["schedule_path"],
                            "schedule_sha256": entry["schedule_sha256"],
                            "schedule_byte_count": entry["schedule_byte_count"],
                            "schedule_id": entry["schedule_id"],
                            "schedule_identity_sha256": entry[
                                "schedule_identity_sha256"
                            ],
                            "preprocess_state_root": entry[
                                "preprocess_state_root"
                            ],
                            "source_schedule_set": {
                                "schedule_set_id": component["manifest"][
                                    "schedule_set_id"
                                ],
                                "manifest_path": str(component["path"]),
                                "manifest_sha256": sha256_bytes(
                                    component["body"]
                                ),
                            },
                            "source_campaign": entry["source_campaign"],
                            "source_epoch": entry["source_epoch"],
                        }
                    )
                expected_component_role_totals.append(
                    {
                        "component": component_name,
                        "role": role,
                        "campaign_count": 1,
                        "epoch_count": len(role_schedules),
                        "schedule_count": len(role_schedules),
                        "selected_count": len(role_members),
                        "selected_estimated_bytes": sum(
                            row["estimated_bytes"] for row in role_members
                        ),
                        "ordered_schedule_reference_sha256": sha256_bytes(
                            module.canonical_bytes(
                                [
                                    module._schedule_projection(
                                        component_name, row["entry"]
                                    )
                                    for row in role_schedules
                                ]
                            )
                        ),
                        "ordered_member_sha256": sha256_bytes(
                            module.canonical_bytes(role_members)
                        ),
                    }
                )

            schedule_rows = value["schedules"]
            configured = self.config.section("campaign")["schedules"]
            if (
                schedule_rows != expected_schedules
                or len(schedule_rows) != len(configured)
                or len(schedule_rows) != len(runtimes)
            ):
                raise BackendError(
                    "composite campaign schedule-set flattening differs from exact component replay"
                )
            selected_by_role = {role: 0 for role in module.ROLE_ORDER}
            bytes_by_role = {role: 0 for role in module.ROLE_ORDER}
            for entry, configured_ref, runtime in zip(
                schedule_rows, configured, runtimes, strict=True
            ):
                runtime_ref, schedule, bundle, _states, _ready = runtime
                source_epoch = entry["source_epoch"]
                projected = {
                    "path": entry["schedule_path"],
                    "sha256": entry["schedule_sha256"],
                    "schedule_id": entry["schedule_id"],
                    "role": entry["role"],
                }
                if (
                    projected != configured_ref
                    or runtime_ref != configured_ref
                    or entry["schedule_identity_sha256"]
                    != schedule["identity_sha256"]
                    or entry["preprocess_state_root"]
                    != schedule["consumer"]["preprocess_state_root"]
                    or source_epoch["bundle_id"]
                    != bundle["manifest"]["bundle_id"]
                    or source_epoch["bundle_manifest_path"] != str(bundle["path"])
                    or source_epoch["bundle_manifest_sha256"]
                    != sha256_bytes(bundle["body"])
                    or source_epoch["selected_count"] != len(bundle["orders"])
                ):
                    raise BackendError(
                        "composite campaign schedule-set entry differs from exact runtime"
                    )
                selected_by_role[entry["role"]] += source_epoch[
                    "selected_count"
                ]
                bytes_by_role[entry["role"]] += source_epoch[
                    "selected_estimated_bytes"
                ]

            if value["component_role_totals"] != expected_component_role_totals:
                raise BackendError(
                    "composite campaign schedule-set component-role totals differ"
                )
            expected_role_totals = []
            for role in module.ROLE_ORDER:
                role_schedules = [
                    row for row in schedule_rows if row["role"] == role
                ]
                expected_role_totals.append(
                    {
                        "role": role,
                        "component_count": 2,
                        "campaign_count": 2,
                        "epoch_count": len(role_schedules),
                        "schedule_count": len(role_schedules),
                        "selected_count": selected_by_role[role],
                        "selected_estimated_bytes": bytes_by_role[role],
                    }
                )
            if value["role_totals"] != expected_role_totals:
                raise BackendError(
                    "composite campaign schedule-set role totals differ"
                )

            schedule_lookup = {
                (component_name, row["entry"]["schedule_ordinal"]): row
                for component_name, component in components.items()
                for row in component["schedules"]
            }
            composite_schedule_projection = [
                {
                    "component": row["component"],
                    "component_schedule_ordinal": row[
                        "component_schedule_ordinal"
                    ],
                    "role": row["role"],
                    "schedule_id": row["schedule_id"],
                    "schedule_path": row["schedule_path"],
                    "schedule_sha256": row["schedule_sha256"],
                    "schedule_byte_count": row["schedule_byte_count"],
                }
                for row in schedule_rows
            ]
            composite_member_rows: list[dict[str, Any]] = []
            for entry in schedule_rows:
                source = schedule_lookup[
                    (entry["component"], entry["component_schedule_ordinal"])
                ]
                composite_member_rows.extend(
                    module._member_rows(
                        entry["component"],
                        entry["role"],
                        entry["source_campaign"]["campaign_id"],
                        entry["source_epoch"]["epoch_ordinal"],
                        source["members"],
                    )
                )
            source_ids = [row["source_id"] for row in source_member_rows]
            recording_ids = [row["recording_id"] for row in source_member_rows]
            native_keys = [
                (row["platform"], row["native_id"])
                for row in source_member_rows
            ]
            source_overlap = len(source_ids) - len(set(source_ids))
            recording_overlap = len(recording_ids) - len(set(recording_ids))
            native_overlap = len(native_keys) - len(set(native_keys))
            expected_proof = {
                "component_count": 2,
                "source_schedule_set_count": 2,
                "source_campaign_count": 4,
                "source_epoch_count": len(schedule_rows),
                "schedule_count": len(schedule_rows),
                "source_selected_count": len(source_member_rows),
                "source_selected_estimated_bytes": sum(
                    row["estimated_bytes"] for row in source_member_rows
                ),
                "schedule_selected_count": len(composite_member_rows),
                "schedule_selected_estimated_bytes": sum(
                    row["estimated_bytes"] for row in composite_member_rows
                ),
                "unique_source_count": len(set(source_ids)),
                "unique_recording_count": len(set(recording_ids)),
                "unique_native_count": len(set(native_keys)),
                "source_overlap_count": source_overlap,
                "recording_overlap_count": recording_overlap,
                "native_overlap_count": native_overlap,
                "missing_count": len(
                    {row["source_id"] for row in source_member_rows}
                    - {row["source_id"] for row in composite_member_rows}
                ),
                "unexpected_count": len(
                    {row["source_id"] for row in composite_member_rows}
                    - {row["source_id"] for row in source_member_rows}
                ),
                "source_schedule_union_sha256": sha256_bytes(
                    module.canonical_bytes(source_schedule_projection)
                ),
                "composite_schedule_union_sha256": sha256_bytes(
                    module.canonical_bytes(composite_schedule_projection)
                ),
                "source_member_union_sha256": sha256_bytes(
                    module.canonical_bytes(source_member_rows)
                ),
                "composite_member_union_sha256": sha256_bytes(
                    module.canonical_bytes(composite_member_rows)
                ),
                "ordered_schedule_union_identical": (
                    source_schedule_projection == composite_schedule_projection
                ),
                "ordered_member_union_identical": (
                    source_member_rows == composite_member_rows
                ),
                "component_order_exact": list(components)
                == list(module.COMPONENT_ORDER),
                "flatten_order_exact": [
                    (row["component"], row["role"])
                    for row in expected_component_role_totals
                ]
                == list(module.FLATTEN_ORDER),
                "schedule_ordinals_contiguous": [
                    row["schedule_ordinal"] for row in schedule_rows
                ]
                == list(range(1, len(schedule_rows) + 1)),
                "source_schedule_files_referenced_not_copied": True,
            }
            proof = value["coverage_proof"]
            if (
                proof != expected_proof
                or source_overlap
                or recording_overlap
                or native_overlap
                or expected_proof["missing_count"]
                or expected_proof["unexpected_count"]
                or not expected_proof["ordered_schedule_union_identical"]
                or not expected_proof["ordered_member_union_identical"]
                or selected_by_role[module.source_set.NORMAL_ROLE]
                != inventory_coverage["ready_selected_count"]
                or selected_by_role[module.source_set.COLD_ONLY_ROLE]
                != inventory_coverage["parked_requires_chunking_count"]
            ):
                raise BackendError(
                    "composite campaign schedule-set exact coverage proof differs"
                )
            return {
                "schedule_set_id": composite_id,
                "schedule_count": len(schedule_rows),
                "normal_selected_count": selected_by_role[
                    module.source_set.NORMAL_ROLE
                ],
                "cold_only_selected_count": selected_by_role[
                    module.source_set.COLD_ONLY_ROLE
                ],
                "selected_count": len(source_member_rows),
                "selected_estimated_bytes": expected_proof[
                    "source_selected_estimated_bytes"
                ],
                "coverage_identity_sha256": expected_proof[
                    "composite_member_union_sha256"
                ],
            }
        except BackendError:
            raise
        except Exception as error:
            raise BackendError(
                f"composite campaign schedule-set replay failed: {error}"
            ) from error

    def _claim_gpu_members(
        self, batch_key: str, claims: Sequence[tuple[str, int]]
    ) -> None:
        """Atomically claim exact queue members for one durable batch record."""

        if not isinstance(batch_key, str) or not batch_key:
            raise BackendError("GPU batch member claim has an invalid batch key")
        normalized = list(claims)
        if (
            not normalized
            or any(
                not isinstance(queue_id, str)
                or not queue_id
                or isinstance(ordinal, bool)
                or not isinstance(ordinal, int)
                or ordinal < 1
                for queue_id, ordinal in normalized
            )
        ):
            raise BackendError("GPU batch member claims are malformed or repeated")
        if len(set(normalized)) != len(normalized):
            raise BackendError("GPU batch member claims are malformed or repeated")
        conflicts = [
            (claim, self._gpu_member_claims[claim])
            for claim in normalized
            if claim in self._gpu_member_claims
            and self._gpu_member_claims[claim] != batch_key
        ]
        if conflicts:
            raise BackendError("GPU queue member is claimed by multiple batches")
        for claim in normalized:
            self._gpu_member_claims[claim] = batch_key

    def _record_gpu_batch_status(
        self,
        record: dict[str, Any],
        batch_manifest: dict[str, Any],
        status: dict[str, Any],
    ) -> str:
        item_count = batch_manifest["totals"]["item_count"]
        self._gpu_batch_item_counts[record["batch_key"]] = item_count
        if status["status"] == "invalid":
            batch = record["batch"]
            self._gpu_parked[record["batch_key"]] = {
                "batch_key": record["batch_key"],
                "batch_id": batch["batch_id"],
                "batch_sha256": batch["sha256"],
                "item_count": item_count,
                "attempt_count": 0,
                "reason": "invalid_exact_batch_result",
                "invalid_result_count": len(status["invalid"]),
            }
            return "parked"
        return status["status"]

    def _gpu_record_restart_witness(
        self, record: dict[str, Any], status: str
    ) -> list[dict[str, Any]]:
        """Bind immutable control/result leaves without reopening audio media."""

        key = record.get("batch_key") if isinstance(record, dict) else None
        if not isinstance(key, str) or not key:
            raise BackendError("GPU restart witness record key is invalid")
        references: dict[str, tuple[str | None, bool]] = {}

        def add(path_value: Any, digest: Any = None) -> None:
            if not isinstance(path_value, str) or not path_value:
                raise BackendError(f"GPU record {key} contains an invalid file path")
            path = str(Path(os.path.abspath(path_value)))
            expected = digest if isinstance(digest, str) else None
            previous = references.get(path)
            candidate = (expected, True)
            if previous is not None and previous != candidate:
                raise BackendError(f"GPU record {key} conflicts on a file identity")
            references[path] = candidate

        if record.get("record_format") == GPU_PACK_RECORD_FORMAT:
            sources = record.get("sources")
            if not isinstance(sources, list):
                raise BackendError(f"GPU record {key} sources are malformed")
            for source in sources:
                if not isinstance(source, dict):
                    raise BackendError(f"GPU record {key} source is malformed")
                queue = source.get("queue")
                receipt = source.get("materialization_receipt")
                if not isinstance(queue, dict) or not isinstance(receipt, dict):
                    raise BackendError(f"GPU record {key} source references are malformed")
                add(queue.get("path"), queue.get("sha256"))
                add(receipt.get("path"), receipt.get("sha256"))
        else:
            queue = record.get("queue")
            if not isinstance(queue, dict):
                raise BackendError(f"GPU record {key} queue is malformed")
            add(queue.get("path"), queue.get("sha256"))
            receipt = record.get("materialization_receipt")
            if receipt is not None:
                if not isinstance(receipt, dict):
                    raise BackendError(
                        f"GPU record {key} materialization reference is malformed"
                    )
                add(receipt.get("path"), receipt.get("sha256"))
        batch = record.get("batch")
        batch_manifest: dict[str, Any] | None = None
        if batch is not None:
            if not isinstance(batch, dict):
                raise BackendError(f"GPU record {key} batch is malformed")
            add(batch.get("path"), batch.get("sha256"))
            batch_manifest, _ = self._checkpoint_small_json(
                Path(batch.get("path", "")), label=f"GPU record {key} batch"
            )
        if status == "completed":
            items = (
                batch_manifest.get("items")
                if isinstance(batch_manifest, dict)
                else None
            )
            if not isinstance(items, list) or not items:
                raise BackendError(f"completed GPU record {key} lacks batch items")
            for ordinal, item in enumerate(items, 1):
                result = item.get("result") if isinstance(item, dict) else None
                if not isinstance(result, dict):
                    raise BackendError(
                        f"completed GPU record {key} item {ordinal} is malformed"
                    )
                for field in (
                    "result_path",
                    "raw_transcript_path",
                    "normalized_transcript_path",
                ):
                    add(result.get(field))
        if len(references) > BACKEND_CHECKPOINT_MAX_FILES_PER_GPU_RECORD:
            raise BackendError(f"GPU record {key} exceeds restart witness file cap")
        return [
            self._checkpoint_file_entry(
                Path(path),
                label=f"GPU record {key} file {index}",
                expected_sha256=digest,
                read_policy=CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON,
            )
            for index, (path, (digest, _verify)) in enumerate(
                sorted(references.items()), 1
            )
        ]

    def _gpu_restart_witness_matches(
        self, rows: Any, *, batch_key: str
    ) -> bool:
        if (
            not isinstance(rows, list)
            or not rows
            or len(rows) > BACKEND_CHECKPOINT_MAX_FILES_PER_GPU_RECORD
        ):
            raise BackendError(f"GPU record {batch_key} restart witnesses are malformed")
        seen: set[str] = set()
        for index, row in enumerate(rows, 1):
            if not isinstance(row, dict) or set(row) != {
                "fingerprint",
                "sha256",
                "read_policy",
            }:
                raise BackendError(
                    f"GPU record {batch_key} restart witness {index} is malformed"
                )
            fingerprint = _validate_checkpoint_fingerprint(
                row["fingerprint"], label=f"GPU record {batch_key} file {index}"
            )
            path = fingerprint["path"]
            if (
                path in seen
                or not isinstance(row["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", row["sha256"]) is None
                or row["read_policy"] != CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON
            ):
                raise BackendError(
                    f"GPU record {batch_key} restart witness {index} is invalid"
                )
            seen.add(path)
            if not _checkpoint_fingerprint_matches(
                fingerprint, label=f"GPU record {batch_key} file {index}"
            ):
                return False
            try:
                body, _ = self.modules.queue_runner._stable_read(
                    Path(path),
                    maximum=BACKEND_CHECKPOINT_SMALL_FILE_BYTES,
                    label=f"GPU record {batch_key} file {index}",
                )
            except Exception:
                return False
            if sha256_bytes(body) != row["sha256"]:
                return False
        return True

    def _gpu_checkpoint_derivation(
        self, record: dict[str, Any]
    ) -> tuple[int, list[tuple[str, int]], list[tuple[str, dict[str, int]]]]:
        """Derive bounded ledgers from an already-validated journal record."""

        if record.get("record_format") == GPU_PACK_RECORD_FORMAT:
            members = record.get("batch_members")
            sources = record.get("sources")
            if not isinstance(members, list) or not isinstance(sources, list):
                raise BackendError("packed GPU checkpoint record is malformed")
            claims = []
            for member in members:
                if not isinstance(member, dict):
                    raise BackendError("packed GPU checkpoint member is malformed")
                claims.append((member.get("queue_id"), member.get("queue_ordinal")))
            dispositions = []
            for source in sources:
                queue = source.get("queue") if isinstance(source, dict) else None
                disposition = (
                    source.get("queue_disposition")
                    if isinstance(source, dict)
                    else None
                )
                if not isinstance(queue, dict) or not isinstance(disposition, dict):
                    raise BackendError("packed GPU checkpoint source is malformed")
                dispositions.append((queue.get("queue_id"), disposition))
            item_count = len(members)
        else:
            queue = record.get("queue") if isinstance(record, dict) else None
            disposition = record.get("queue_disposition") if isinstance(record, dict) else None
            if not isinstance(queue, dict) or not isinstance(disposition, dict):
                raise BackendError("legacy GPU checkpoint record is malformed")
            queue_id = queue.get("queue_id")
            dispositions = [(queue_id, disposition)]
            if record.get("record_kind") == "no_ready_members":
                item_count = 0
                claims = []
            else:
                receipt_ref = record.get("materialization_receipt")
                batch_ref = record.get("batch")
                if not isinstance(receipt_ref, dict) or not isinstance(batch_ref, dict):
                    raise BackendError("legacy GPU checkpoint references are malformed")
                receipt, _body = self._checkpoint_small_json(
                    Path(receipt_ref.get("path", "")),
                    label="legacy GPU materialization receipt",
                )
                selection = receipt.get("selection")
                ordinals = (
                    selection.get("queue_ordinals")
                    if isinstance(selection, dict)
                    else None
                )
                batch, _batch_body = self._checkpoint_small_json(
                    Path(batch_ref.get("path", "")),
                    label="legacy GPU batch manifest",
                )
                item_count = batch.get("totals", {}).get("item_count")
                if not isinstance(ordinals, list) or item_count != len(ordinals):
                    raise BackendError("legacy GPU checkpoint selection is malformed")
                claims = [(queue_id, ordinal) for ordinal in ordinals]
        if (
            isinstance(item_count, bool)
            or not isinstance(item_count, int)
            or item_count < 0
            or any(
                not isinstance(queue_id, str)
                or not queue_id
                or isinstance(ordinal, bool)
                or not isinstance(ordinal, int)
                or ordinal < 1
                for queue_id, ordinal in claims
            )
            or len(claims) != len(set(claims))
            or any(
                not isinstance(queue_id, str)
                or not queue_id
                or not isinstance(disposition, dict)
                for queue_id, disposition in dispositions
            )
        ):
            raise BackendError("GPU checkpoint derived ledgers are malformed")
        return item_count, claims, dispositions

    def _install_gpu_checkpoint_row(
        self,
        *,
        record: dict[str, Any],
        status: str,
        item_count: int,
        claims: Sequence[tuple[str, int]],
        dispositions: Sequence[tuple[str, dict[str, int]]],
        parked: dict[str, Any] | None,
    ) -> None:
        key = record.get("batch_key")
        if (
            not isinstance(key, str)
            or not key
            or status not in {"pending", "completed", "parked", "not_applicable"}
            or (status == "not_applicable") != (item_count == 0)
            or (status == "parked") != (parked is not None)
        ):
            raise BackendError("GPU checkpoint row is inconsistent")
        if key in self._gpu_records:
            raise BackendError("GPU checkpoint repeats a batch record")
        for queue_id, disposition in dispositions:
            expected_keys = {
                "member_count",
                "ready_count",
                "requires_chunking_count",
                "explicit_skip_count",
                "ready_audio_duration_ms",
                "requires_chunking_audio_duration_ms",
            }
            if (
                set(disposition) != expected_keys
                or any(
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 0
                    for value in disposition.values()
                )
            ):
                raise BackendError("GPU checkpoint disposition is malformed")
            previous = self._gpu_queue_dispositions.get(queue_id)
            if previous is not None and previous != disposition:
                raise BackendError("GPU checkpoint dispositions conflict")
            self._gpu_queue_dispositions[queue_id] = deepcopy(disposition)
        if claims:
            self._claim_gpu_members(key, claims)
        elif item_count != 0:
            raise BackendError("GPU checkpoint omits non-empty member claims")
        self._gpu_records[key] = deepcopy(record)
        self._gpu_status[key] = status
        self._gpu_batch_item_counts[key] = item_count
        if parked is not None:
            if not isinstance(parked, dict) or parked.get("batch_key") != key:
                raise BackendError("GPU checkpoint parking evidence is malformed")
            self._gpu_parked[key] = deepcopy(parked)

    def _restore_gpu_checkpoint(
        self,
        rows: Any,
        tail_evidence: Sequence[tuple[dict[str, Any], str | None]],
        *,
        metadata_bootstrap: bool,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> dict[str, int]:
        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise BackendError("GPU checkpoint record vector is malformed")
        fast = 0
        targeted = 0
        bootstrapped = 0
        row_keys = {
            "record",
            "status",
            "item_count",
            "claims",
            "dispositions",
            "parked",
            "files",
        }
        for row in rows:
            if not isinstance(row, dict) or set(row) != row_keys:
                raise BackendError("GPU checkpoint row is malformed")
            record = row["record"]
            key = record.get("batch_key") if isinstance(record, dict) else None
            status = row["status"]
            raw_claims = row["claims"]
            raw_dispositions = row["dispositions"]
            if (
                not isinstance(key, str)
                or not key
                or not isinstance(raw_claims, list)
                or not isinstance(raw_dispositions, list)
            ):
                raise BackendError("GPU checkpoint row identity is malformed")
            claims = []
            for claim in raw_claims:
                if not isinstance(claim, list) or len(claim) != 2:
                    raise BackendError("GPU checkpoint member claim is malformed")
                claims.append((claim[0], claim[1]))
            dispositions = []
            for disposition in raw_dispositions:
                if not isinstance(disposition, dict) or set(disposition) != {
                    "queue_id",
                    "value",
                }:
                    raise BackendError("GPU checkpoint disposition row is malformed")
                dispositions.append(
                    (disposition["queue_id"], disposition["value"])
                )
            derived = self._gpu_checkpoint_derivation(record)
            if (
                row["item_count"] != derived[0]
                or claims != derived[1]
                or dispositions != derived[2]
            ):
                raise BackendError("GPU checkpoint derived ledgers changed")
            unchanged = self._gpu_restart_witness_matches(
                row["files"], batch_key=key
            )
            if unchanged:
                self._install_gpu_checkpoint_row(
                    record=record,
                    status=status,
                    item_count=derived[0],
                    claims=claims,
                    dispositions=dispositions,
                    parked=row["parked"],
                )
                self._gpu_restart_witnesses[key] = deepcopy(row["files"])
                fast += 1
            else:
                replay_status = status
                parked = row["parked"]
                if (
                    status == "parked"
                    and isinstance(parked, dict)
                    and parked.get("reason")
                    == "sealed_child_attempt_limit_exhausted"
                ):
                    # Exact batch files remain pending in this case; the child
                    # journal below is the authority that re-derives parking.
                    replay_status = "pending"
                self._apply_gpu_record(
                    record,
                    replay_status,
                    derive_missing_status=False,
                )
                self._gpu_restart_witnesses[key] = self._gpu_record_restart_witness(
                    record, self._gpu_status[key]
                )
                targeted += 1
            if cancellation_boundary is not None:
                cancellation_boundary()

        for record, status in tail_evidence:
            key = record.get("batch_key") if isinstance(record, dict) else None
            if metadata_bootstrap and key not in self._gpu_records and status is not None:
                item_count, claims, dispositions = self._gpu_checkpoint_derivation(
                    record
                )
                if status == "parked":
                    # Parking reason is not fully represented by old stage events;
                    # retain exact replay for that uncommon terminal state.
                    self._apply_gpu_record(
                        record, status, derive_missing_status=False
                    )
                    targeted += 1
                else:
                    self._install_gpu_checkpoint_row(
                        record=record,
                        status=status,
                        item_count=item_count,
                        claims=claims,
                        dispositions=dispositions,
                        parked=None,
                    )
                    bootstrapped += 1
            else:
                self._apply_gpu_record(
                    record,
                    status,
                    derive_missing_status=status is None,
                )
                targeted += 1
            if isinstance(key, str) and key in self._gpu_records:
                self._gpu_restart_witnesses[key] = self._gpu_record_restart_witness(
                    self._gpu_records[key], self._gpu_status[key]
                )
            if cancellation_boundary is not None:
                cancellation_boundary()
        # A pending checkpoint row intentionally has no result-file witnesses:
        # result publication may advance after the checkpoint while immutable
        # queue/batch lineage remains unchanged.  Reconcile only that mutable
        # disposition here.  This validates the aggregate batch and any published
        # result/transcript JSON, but does not replay source lineage or hash audio.
        # Completion must win before attempt-limit parking is reconstructed.
        self._refresh_gpu_status()
        if cancellation_boundary is not None:
            cancellation_boundary()
        self._restore_gpu_attempt_exhaustion()
        if cancellation_boundary is not None:
            cancellation_boundary()
        return {
            "fast_reused_records": fast,
            "targeted_revalidated_records": targeted,
            "metadata_bootstrap_records": bootstrapped,
        }

    def _validate_gpu_record(self, record: dict[str, Any]) -> str:
        if (
            isinstance(record, dict)
            and record.get("record_format") == GPU_PACK_RECORD_FORMAT
        ):
            return self._validate_packed_gpu_record(record)
        return self._validate_legacy_gpu_record(record)

    def _validate_legacy_gpu_record(self, record: dict[str, Any]) -> str:
        """Replay the original single-queue record without changing its shape."""

        gpu = self.config.section("gpu_readiness")
        expected_common = {
            "record_kind",
            "batch_key",
            "preprocess_bundle_id",
            "queue",
            "batch",
            "materialization_receipt",
            "queue_disposition",
        }
        if not isinstance(record, dict) or set(record) != expected_common:
            raise BackendError("journal GPU record has unexpected fields")
        queue = record["queue"]
        if not isinstance(queue, dict) or set(queue) != {"path", "sha256", "queue_id"}:
            raise BackendError("journal GPU queue reference is malformed")
        try:
            manifest = self.modules.gpu_queue.validate_queue(
                manifest_path=Path(queue["path"]),
                root_registration_path=Path(gpu["root_registration"]),
                root_registration_sha256=gpu["root_registration_sha256"],
            )
        except Exception as error:
            raise BackendError(f"journal GPU queue replay failed: {error}") from error
        queue_body = self.modules.gpu_queue.canonical_bytes(manifest)
        if (
            sha256_bytes(queue_body) != queue["sha256"]
            or manifest["queue_id"] != queue["queue_id"]
        ):
            raise BackendError("journal GPU queue reference differs from replay")
        expected_disposition = self._gpu_queue_disposition(manifest)
        if record["queue_disposition"] != expected_disposition:
            raise BackendError("journal GPU disposition differs from queue replay")
        if isinstance(self.modules, _Modules):
            self._cache_gpu_queue_manifest(
                manifest=manifest,
                manifest_path=Path(queue["path"]),
                expected_bundle_id=record["preprocess_bundle_id"],
                expected_reference=queue,
                expected_disposition=record["queue_disposition"],
            )
        if record["record_kind"] == "no_ready_members":
            if record["batch"] is not None or record["materialization_receipt"] is not None:
                raise BackendError("no-ready GPU record invents a batch")
            if manifest["totals"]["ready_count"] != 0:
                raise BackendError("no-ready GPU record now has ready members")
            self._gpu_batch_item_counts[record["batch_key"]] = 0
            return "not_applicable"
        if record["record_kind"] != "ready_batch":
            raise BackendError("journal GPU record kind is unsupported")
        batch = record["batch"]
        materialization = record["materialization_receipt"]
        if (
            not isinstance(batch, dict)
            or set(batch) != {"path", "sha256", "batch_id"}
            or not isinstance(materialization, dict)
            or set(materialization) != {"path", "sha256", "receipt_id"}
        ):
            raise BackendError("journal ready-batch references are malformed")
        try:
            receipt = self.modules.gpu_bridge.load_receipt(
                materialization["path"], materialization["sha256"], replay=True
            )
        except Exception as error:
            raise BackendError(f"GPU materialization receipt replay failed: {error}") from error
        if (
            receipt["receipt_id"] != materialization["receipt_id"]
            or receipt["source_queue"]["path"] != queue["path"]
            or receipt["source_queue"]["sha256"] != queue["sha256"]
            or receipt["batch"]["path"] != batch["path"]
            or receipt["batch"]["sha256"] != batch["sha256"]
            or receipt["batch"]["batch_id"] != batch["batch_id"]
        ):
            raise BackendError("GPU materialization journal reference differs from receipt")
        try:
            batch_manifest, _body = self.modules.gpu_bridge.BATCH_V2.load_manifest(
                batch["path"], batch["sha256"], profile=self._load_profile()
            )
            status = self.modules.gpu_bridge.BATCH_V2.batch_status(
                batch_manifest, self._load_profile()
            )
        except Exception as error:
            raise BackendError(f"GPU batch status replay failed: {error}") from error
        self._claim_gpu_members(
            record["batch_key"],
            [
                (queue["queue_id"], ordinal)
                for ordinal in receipt["selection"]["queue_ordinals"]
            ],
        )
        return self._record_gpu_batch_status(record, batch_manifest, status)

    def _validate_packed_gpu_record(self, record: dict[str, Any]) -> str:
        expected = {
            "record_kind",
            "record_format",
            "batch_key",
            "sources",
            "batch_members",
            "batch",
        }
        if (
            not isinstance(record, dict)
            or set(record) != expected
            or record["record_kind"] != "ready_batch"
            or record["record_format"] != GPU_PACK_RECORD_FORMAT
        ):
            raise BackendError("journal packed GPU record has unexpected fields")
        sources = record["sources"]
        members = record["batch_members"]
        if (
            not isinstance(sources, list)
            or not 1 <= len(sources) <= 32
            or not isinstance(members, list)
            or not 1 <= len(members) <= 32
        ):
            raise BackendError("journal packed GPU sources or members are malformed")
        gpu = self.config.section("gpu_readiness")
        source_members: dict[tuple[str, int], dict[str, Any]] = {}
        source_dispositions: list[tuple[str, dict[str, int]]] = []
        execution_classes: set[str] = set()
        seen_queues: set[str] = set()
        for source_ordinal, source in enumerate(sources, 1):
            if not isinstance(source, dict) or set(source) != {
                "preprocess_bundle_id",
                "queue",
                "queue_disposition",
                "queue_ordinals",
                "materialization_receipt",
            }:
                raise BackendError(
                    f"journal packed GPU source {source_ordinal} is malformed"
                )
            if (
                not isinstance(source["preprocess_bundle_id"], str)
                or not source["preprocess_bundle_id"]
            ):
                raise BackendError("journal packed GPU bundle ID is invalid")
            queue = source["queue"]
            if not isinstance(queue, dict) or set(queue) != {
                "path",
                "sha256",
                "queue_id",
            }:
                raise BackendError("journal packed GPU queue reference is malformed")
            queue_id = queue["queue_id"]
            if not isinstance(queue_id, str) or not queue_id or queue_id in seen_queues:
                raise BackendError("journal packed GPU queues are invalid or repeated")
            seen_queues.add(queue_id)
            try:
                queue_manifest = self.modules.gpu_queue.validate_queue(
                    manifest_path=Path(queue["path"]),
                    root_registration_path=Path(gpu["root_registration"]),
                    root_registration_sha256=gpu["root_registration_sha256"],
                )
            except Exception as error:
                raise BackendError(
                    f"journal packed GPU queue replay failed: {error}"
                ) from error
            if (
                sha256_bytes(self.modules.gpu_queue.canonical_bytes(queue_manifest))
                != queue["sha256"]
                or queue_manifest["queue_id"] != queue_id
            ):
                raise BackendError(
                    "journal packed GPU queue reference differs from replay"
                )
            disposition = self._gpu_queue_disposition(queue_manifest)
            if source["queue_disposition"] != disposition:
                raise BackendError(
                    "journal packed GPU disposition differs from queue replay"
                )
            if isinstance(self.modules, _Modules):
                self._cache_gpu_queue_manifest(
                    manifest=queue_manifest,
                    manifest_path=Path(queue["path"]),
                    expected_bundle_id=source["preprocess_bundle_id"],
                    expected_reference=queue,
                    expected_disposition=source["queue_disposition"],
                )
            source_dispositions.append((queue_id, disposition))
            queue_ordinals = source["queue_ordinals"]
            if (
                not isinstance(queue_ordinals, list)
                or not queue_ordinals
                or any(
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or not 1 <= value <= self.modules.gpu_queue.MAX_ITEMS
                    for value in queue_ordinals
                )
            ):
                raise BackendError("journal packed GPU queue selection is malformed")
            if len(set(queue_ordinals)) != len(queue_ordinals):
                raise BackendError("journal packed GPU queue selection is malformed")
            materialization = source["materialization_receipt"]
            if not isinstance(materialization, dict) or set(materialization) != {
                "path",
                "sha256",
                "receipt_id",
            }:
                raise BackendError(
                    "journal packed GPU materialization reference is malformed"
                )
            try:
                receipt = self.modules.gpu_bridge.load_receipt(
                    materialization["path"], materialization["sha256"], replay=True
                )
            except Exception as error:
                raise BackendError(
                    f"packed GPU materialization receipt replay failed: {error}"
                ) from error
            if (
                receipt["receipt_id"] != materialization["receipt_id"]
                or receipt["source_queue"]["path"] != queue["path"]
                or receipt["source_queue"]["sha256"] != queue["sha256"]
                or receipt["source_queue"]["queue_id"] != queue_id
                or receipt["selection"]["queue_ordinals"] != queue_ordinals
            ):
                raise BackendError(
                    "packed GPU materialization differs from its source selection"
                )
            execution_classes.add(receipt["batch"]["execution_class"])
            for row in receipt["work_orders"]:
                claim = (queue_id, row["queue_ordinal"])
                if claim in source_members:
                    raise BackendError("packed GPU source members are repeated")
                source_members[claim] = {
                    "preprocess_ordinal": row["preprocess_ordinal"],
                    "member_id": row["member_id"],
                    "path": row["path"],
                    "sha256": row["sha256"],
                    "work_order_id": row["work_order_id"],
                    "identity_sha256": row["identity_sha256"],
                }
        if len(source_members) != len(members) or len(execution_classes) != 1:
            raise BackendError(
                "packed GPU source selections differ from the aggregate members"
            )

        batch = record["batch"]
        if not isinstance(batch, dict) or set(batch) != {
            "path",
            "sha256",
            "batch_id",
        }:
            raise BackendError("journal packed GPU batch reference is malformed")
        if record["batch_key"] != f"packed:{batch['batch_id']}":
            raise BackendError("journal packed GPU batch key differs from its batch")
        try:
            profile = self._load_profile()
            batch_manifest, _body = self.modules.gpu_bridge.BATCH_V2.load_manifest(
                batch["path"], batch["sha256"], profile=profile
            )
            status = self.modules.gpu_bridge.BATCH_V2.batch_status(
                batch_manifest, profile
            )
        except Exception as error:
            raise BackendError(f"packed GPU batch status replay failed: {error}") from error
        if (
            batch_manifest["batch_id"] != batch["batch_id"]
            or batch_manifest["execution_class"] not in execution_classes
            or batch_manifest["totals"]["item_count"] != len(members)
        ):
            raise BackendError("packed GPU aggregate batch differs from its sources")

        claims: list[tuple[str, int]] = []
        external_orders: list[dict[str, Any]] = []
        for ordinal, member in enumerate(members, 1):
            if not isinstance(member, dict) or set(member) != {
                "batch_ordinal",
                "queue_id",
                "queue_ordinal",
                "preprocess_ordinal",
                "member_id",
                "work_order_id",
                "work_order_sha256",
                "work_order_identity_sha256",
            }:
                raise BackendError(f"journal packed GPU member {ordinal} is malformed")
            if member["batch_ordinal"] != ordinal:
                raise BackendError("journal packed GPU batch ordinals are not contiguous")
            if (
                not isinstance(member["queue_id"], str)
                or not member["queue_id"]
                or isinstance(member["queue_ordinal"], bool)
                or not isinstance(member["queue_ordinal"], int)
                or member["queue_ordinal"] < 1
            ):
                raise BackendError("journal packed GPU member claim is malformed")
            claim = (member["queue_id"], member["queue_ordinal"])
            reference = source_members.get(claim)
            if reference is None or any(
                member[key] != reference[reference_key]
                for key, reference_key in (
                    ("preprocess_ordinal", "preprocess_ordinal"),
                    ("member_id", "member_id"),
                    ("work_order_id", "work_order_id"),
                    ("work_order_sha256", "sha256"),
                    ("work_order_identity_sha256", "identity_sha256"),
                )
            ):
                raise BackendError("journal packed GPU member differs from its receipt")
            try:
                order = self.modules.gpu_bridge.ASR_V5.load_work_order(
                    reference["path"],
                    profile_document=profile,
                    expected_sha256=reference["sha256"],
                    replay_bindings=False,
                )
            except Exception as error:
                raise BackendError(
                    f"packed GPU work-order replay failed: {error}"
                ) from error
            external_orders.append(order)
            claims.append(claim)
        if [row["work_order"] for row in batch_manifest["items"]] != external_orders:
            raise BackendError("packed GPU batch order differs from its receipt lineage")

        # Commit mutable recovery indexes only after every external artifact and
        # aggregate ordering constraint has replayed successfully.
        for queue_id, disposition in source_dispositions:
            previous = self._gpu_queue_dispositions.get(queue_id)
            if previous is not None and previous != disposition:
                raise BackendError("GPU journal has conflicting queue dispositions")
        self._claim_gpu_members(record["batch_key"], claims)
        return self._record_gpu_batch_status(record, batch_manifest, status)

    @staticmethod
    def _gpu_queue_disposition(queue: dict[str, Any]) -> dict[str, int]:
        totals = queue["totals"]
        return {
            "member_count": totals["member_count"],
            "ready_count": totals["ready_count"],
            "requires_chunking_count": totals["requires_chunking_count"],
            "explicit_skip_count": totals["explicit_skip_count"],
            "ready_audio_duration_ms": totals["ready_audio_duration_ms"],
            "requires_chunking_audio_duration_ms": totals[
                "requires_chunking_audio_duration_ms"
            ],
        }

    def _register_gpu_disposition(self, record: dict[str, Any]) -> None:
        rows = (
            [
                (source["queue"]["queue_id"], source["queue_disposition"])
                for source in record["sources"]
            ]
            if record.get("record_format") == GPU_PACK_RECORD_FORMAT
            else [(record["queue"]["queue_id"], record["queue_disposition"])]
        )
        for queue_id, disposition in rows:
            previous = self._gpu_queue_dispositions.get(queue_id)
            if previous is not None and previous != disposition:
                raise BackendError("GPU journal has conflicting queue dispositions")
            self._gpu_queue_dispositions[queue_id] = disposition

    def _gpu_disposition_totals(self) -> dict[str, int]:
        return {
            key: sum(row[key] for row in self._gpu_queue_dispositions.values())
            for key in (
                "member_count",
                "ready_count",
                "requires_chunking_count",
                "explicit_skip_count",
                "ready_audio_duration_ms",
                "requires_chunking_audio_duration_ms",
            )
        }

    def _gpu_parked_totals(self) -> dict[str, int]:
        return {
            "parked_batch_count": len(self._gpu_parked),
            "parked_item_count": sum(
                row["item_count"] for row in self._gpu_parked.values()
            ),
        }

    def _gpu_item_status_totals(self) -> dict[str, int]:
        """Return exact cumulative item totals from validated batch ledgers."""

        record_keys = set(self._gpu_records)
        if (
            set(self._gpu_batch_item_counts) != record_keys
            or set(self._gpu_status) != record_keys
        ):
            raise BackendError("GPU record, item-count, and status ledgers differ")
        by_status = {
            "pending": 0,
            "completed": 0,
            "parked": 0,
            "not_applicable": 0,
        }
        for key, status in self._gpu_status.items():
            item_count = self._gpu_batch_item_counts[key]
            record = self._gpu_records[key]
            record_kind = record.get("record_kind") if isinstance(record, dict) else None
            if (
                status not in by_status
                or isinstance(item_count, bool)
                or not isinstance(item_count, int)
                or item_count < 0
                or (
                    record_kind == "ready_batch"
                    and (item_count < 1 or status == "not_applicable")
                )
                or (
                    record_kind == "no_ready_members"
                    and (item_count != 0 or status != "not_applicable")
                )
                or record_kind not in {"ready_batch", "no_ready_members"}
            ):
                raise BackendError("GPU item status ledger is malformed")
            by_status[status] += item_count
        return {
            "pending_items": by_status["pending"],
            "completed_items": by_status["completed"],
            "parked_items": by_status["parked"],
        }

    @staticmethod
    def _validated_gpu_child_history(
        history: Sequence[GpuChildRecord], *, maximum: int
    ) -> list[GpuChildRecord]:
        ordered = sorted(history, key=lambda value: value.attempt_ordinal)
        ordinals = [record.attempt_ordinal for record in ordered]
        if ordinals != list(range(1, len(ordinals) + 1)):
            raise BackendError(
                "GPU child attempts are not unique and contiguous for a batch"
            )
        if len(ordered) > maximum:
            raise BackendError("GPU child journal exceeds the sealed attempt limit")
        if any(record.state == "reconciliation_required" for record in ordered):
            raise BackendError(
                "pending GPU batch has an ambiguous prior child attempt"
            )
        active_states = {"launching", "running", "retiring", "stopping"}
        active_positions = [
            index
            for index, record in enumerate(ordered)
            if record.state in active_states
        ]
        if len(active_positions) > 1 or (
            active_positions and active_positions[0] != len(ordered) - 1
        ):
            raise BackendError(
                "pending GPU batch has non-final active child authority"
            )
        return ordered

    def _restore_gpu_attempt_exhaustion(self) -> None:
        """Rebuild terminal attempt-limit parking from the exact child journal.

        Attempt-limit parking is controller state derived from durable child
        attempts, not a batch-result artifact.  Reconstruct it during restore so
        recovery totals do not temporarily relabel truly exhausted items as
        pending until the first GPU scheduler tick.  A live or ambiguous final
        attempt remains pending and is left for normal child reconciliation.
        """

        ready_by_binding: dict[tuple[str, str], dict[str, Any]] = {}
        for record in self._gpu_records.values():
            if record.get("record_kind") != "ready_batch":
                continue
            binding = (record["batch"]["batch_id"], record["batch"]["sha256"])
            if binding in ready_by_binding:
                raise BackendError("GPU ready records repeat an exact batch binding")
            ready_by_binding[binding] = record
        pending_keys = {
            record["batch_key"]
            for record in ready_by_binding.values()
            if self._gpu_status.get(record["batch_key"]) == "pending"
        }
        if not self._gpu_records:
            gpu = self.config.section("gpu_readiness")
            try:
                journal = PrivateGpuChildJournal(
                    Path(gpu["child_journal_root"])
                )
                with journal.authority_lock():
                    records = journal.list_records()
                    if records:
                        raise BackendError(
                            "GPU child journal has records without GPU recovery authority"
                        )
                    self._gpu_child_records = {}
            except BackendError:
                raise
            except Exception as error:
                raise BackendError(
                    f"empty GPU child journal replay failed: {error}"
                ) from error
            return
        executor = self._gpu_executor
        child_records = self._load_gpu_child_records(executor)
        histories: dict[str, list[GpuChildRecord]] = {
            key: [] for key in pending_keys
        }
        for child in child_records.values():
            try:
                child.validated()
            except Exception as error:
                raise BackendError(
                    f"GPU child journal record validation failed: {error}"
                ) from error
            record = ready_by_binding.get((child.batch_id, child.batch_sha256))
            if record is None:
                raise BackendError(
                    "GPU child journal references a batch outside controller recovery"
                )
            spec = self._gpu_launch_spec(record, child.attempt_ordinal)
            expected_unit = (
                f"himr-autonomy-gpu-{child.outer_invocation_id}-"
                f"{child.batch_sha256}-{child.attempt_ordinal:06d}.service"
            )
            if (
                child.unit_name != expected_unit
                or child.spec_identity_sha256 != spec.identity_sha256
            ):
                raise BackendError(
                    "GPU child journal attempt differs from its exact launch binding"
                )
            if child.state in {
                "launching",
                "running",
                "retiring",
                "stopping",
                "reconciliation_required",
            }:
                executor = executor or self._get_gpu_executor()
                foreign = (
                    child.outer_unit != executor.context.outer_unit
                    or child.outer_invocation_id != executor.context.outer_invocation_id
                )
                if foreign:
                    raise BackendError(
                        "GPU child from a prior controller invocation requires manual reconciliation"
                    )
            if record["batch_key"] in pending_keys:
                histories[record["batch_key"]].append(child)

        maximum = self.config.section("gpu_readiness")["max_attempts_per_batch"]
        for key, history in histories.items():
            history = self._validated_gpu_child_history(
                history, maximum=maximum
            )
            if len(history) == maximum and history[-1].state not in {
                "launching",
                "running",
                "retiring",
                "stopping",
            }:
                self._park_gpu_attempt_exhaustion(
                    self._gpu_records[key], attempt_count=len(history)
                )

    def _park_gpu_attempt_exhaustion(
        self,
        record: dict[str, Any],
        *,
        attempt_count: int,
    ) -> bool:
        key = record["batch_key"]
        item_count = self._gpu_batch_item_counts.get(key)
        if isinstance(item_count, bool) or not isinstance(item_count, int):
            raise BackendError("GPU batch item count is unavailable for parking")
        parked = {
            "batch_key": key,
            "batch_id": record["batch"]["batch_id"],
            "batch_sha256": record["batch"]["sha256"],
            "item_count": item_count,
            "attempt_count": attempt_count,
            "reason": "sealed_child_attempt_limit_exhausted",
            "invalid_result_count": 0,
        }
        previous = self._gpu_parked.get(key)
        if previous is not None and previous != parked:
            raise BackendError("GPU batch parking evidence changed across replay")
        self._gpu_parked[key] = parked
        self._gpu_status[key] = "parked"
        return previous is None

    @staticmethod
    def _safe_failure(error: Exception) -> dict[str, str]:
        message = " ".join(str(error).split())[:2048]
        return {
            "type": type(error).__name__[:256],
            "message": message or "unspecified preprocessing failure",
        }

    def _operational_preprocess_failure(self, error: Exception) -> bool:
        batch_error = getattr(self.modules.preprocess_batch, "BatchError", None)
        return (
            isinstance(batch_error, type)
            and isinstance(error, batch_error)
            and str(error).startswith("media preprocessing failed for item 1:")
        )

    def _preprocess_failure_record(
        self,
        *,
        reference: dict[str, Any],
        bundle: dict[str, Any],
        states: list[dict[str, Any] | None],
        queue_ordinal: int,
        error: Exception,
    ) -> dict[str, Any]:
        index = queue_ordinal - 1
        if not 0 <= index < len(states):
            raise BackendError("preprocess failure ordinal is outside its exact schedule")
        entry = bundle["manifest"]["work_orders"][index]
        order = bundle["orders"][index]
        state = states[index]
        if (
            entry["queue_ordinal"] != queue_ordinal
            or not self.modules.background._completed_state(state)
        ):
            raise BackendError("preprocess failure is not bound to a completed acquisition")
        item_key = f"{reference['schedule_id']}:{queue_ordinal}"
        prior = self._preprocess_failure_attempts.get(item_key, [])
        attempt = len(prior) + 1
        maximum = self.config.section("preprocess")["max_attempts_per_item"]
        if attempt > maximum:
            raise BackendError("preprocess item exceeded its sealed failure-attempt ledger")
        result_path = self.modules.queue_runner._result_path(order)
        return {
            "record_kind": "preprocess_failure_attempt",
            "item_key": item_key,
            "schedule_id": reference["schedule_id"],
            "queue_ordinal": queue_ordinal,
            "job_id": entry["job_id"],
            "work_order": {
                "path": str(bundle["path"].parent / entry["path"]),
                "sha256": entry["sha256"],
            },
            "acquisition_result": {
                "path": str(result_path),
                "sha256": state["result_sha256"],
            },
            "media_sha256": state["media_sha256"],
            "media_byte_count": state["byte_count"],
            "attempt_ordinal": attempt,
            "disposition": "parked" if attempt == maximum else "retryable",
            "error": self._safe_failure(error),
        }

    def _validate_preprocess_failure_record(
        self,
        record: Any,
        completed: dict[str, dict[str, Any]],
    ) -> None:
        expected = {
            "record_kind",
            "item_key",
            "schedule_id",
            "queue_ordinal",
            "job_id",
            "work_order",
            "acquisition_result",
            "media_sha256",
            "media_byte_count",
            "attempt_ordinal",
            "disposition",
            "error",
        }
        if not isinstance(record, dict) or set(record) != expected:
            raise BackendError("journal preprocess failure record has unexpected fields")
        row = completed.get(record["item_key"])
        if row is None:
            raise BackendError("journal parks preprocessing outside completed acquisition")
        if (
            record["record_kind"] != "preprocess_failure_attempt"
            or record["schedule_id"] != row["schedule_id"]
            or record["queue_ordinal"] != row["ordinal"]
            or record["job_id"] != row["job_id"]
            or record["work_order"]
            != {
                "path": str(row["work_order"]),
                "sha256": row["work_order_sha256"],
            }
            or record["acquisition_result"]
            != {"path": str(row["result"]), "sha256": row["result_sha256"]}
            or record["media_sha256"] != row["media_sha256"]
            or record["media_byte_count"] != row["media_byte_count"]
        ):
            raise BackendError("journal preprocess failure pins differ from exact acquisition")
        attempts = self._preprocess_failure_attempts.setdefault(record["item_key"], [])
        expected_attempt = len(attempts) + 1
        maximum = self.config.section("preprocess")["max_attempts_per_item"]
        expected_disposition = "parked" if expected_attempt == maximum else "retryable"
        error = record["error"]
        if (
            record["attempt_ordinal"] != expected_attempt
            or expected_attempt > maximum
            or record["disposition"] != expected_disposition
            or not isinstance(error, dict)
            or set(error) != {"type", "message"}
            or error["type"] != "BatchError"
            or not isinstance(error["message"], str)
            or not error["message"].startswith(
                "media preprocessing failed for item 1:"
            )
            or len(error["message"]) > 2048
        ):
            raise BackendError("journal preprocess failure evidence is invalid")
        attempts.append(record)
        if record["disposition"] == "parked":
            self._preprocess_parked[record["item_key"]] = record

    def _preprocess_failure_totals(self) -> dict[str, int]:
        return {
            "failed_attempt_count": sum(
                len(rows) for rows in self._preprocess_failure_attempts.values()
            ),
            "retryable_failed_item_count": sum(
                bool(rows)
                and rows[-1]["disposition"] == "retryable"
                and item_key not in self._preprocess_resolved_after_retry
                for item_key, rows in self._preprocess_failure_attempts.items()
            ),
            "parked_item_count": len(self._preprocess_parked),
        }

    def _recovery_monitor_projection(self) -> dict[str, Any]:
        """Project current restored authority without re-reading media bytes."""

        if (
            not isinstance(self._campaign_coverage_checkpoint, dict)
            or not isinstance(self._schedule_set_coverage_checkpoint, dict)
            or not isinstance(self._cold_storage_identity_checkpoint, dict)
        ):
            raise BackendError("backend recovery projection lacks campaign authority")
        runtimes = self._campaign_runtimes()
        completed = self._completed_acquisitions()
        _completed_count, scheduled_quarantined, scheduled_pending = (
            self._runtime_counts(runtimes)
        )
        coverage = deepcopy(self._campaign_coverage_checkpoint)
        coverage["scheduled_quarantined_count"] = scheduled_quarantined
        coverage["scheduled_runnable_pending_count"] = scheduled_pending
        gpu_dispositions = self._gpu_disposition_totals()
        gpu_parked = self._gpu_parked_totals()
        gpu_items = self._gpu_item_status_totals()
        preprocess_failures = self._preprocess_failure_totals()
        projection = {
            "authoritative_acquisitions_replayed": len(completed),
            "campaign_schedule_count": len(
                self.config.section("campaign")["schedules"]
            ),
            "campaign_coverage": coverage,
            "schedule_set_coverage": deepcopy(
                self._schedule_set_coverage_checkpoint
            ),
            "cold_storage_identity": deepcopy(
                self._cold_storage_identity_checkpoint
            ),
            "scheduled_quarantined_acquisitions": scheduled_quarantined,
            "scheduled_runnable_pending_acquisitions": scheduled_pending,
            "cold_retention_records_restored": len(self._retained_items),
            "cold_retentions_pending_exact_replay": len(
                self._retention_replay_pending
            ),
            "gpu_records_replayed": len(self._gpu_records),
            "gpu_ready_batches": sum(
                value == "pending" for value in self._gpu_status.values()
            ),
            "gpu_pending_items_restored": gpu_items["pending_items"],
            "gpu_completed_items_restored": gpu_items["completed_items"],
            "gpu_requires_chunking_items_restored": gpu_dispositions[
                "requires_chunking_count"
            ],
            "gpu_explicit_skips_restored": gpu_dispositions[
                "explicit_skip_count"
            ],
            "gpu_parked_batches_restored": gpu_parked["parked_batch_count"],
            "gpu_parked_items_restored": gpu_parked["parked_item_count"],
            "preprocessed_items_cumulative": self._preprocessed_items_cumulative(),
            "preprocess_failed_attempts_restored": preprocess_failures[
                "failed_attempt_count"
            ],
            "preprocess_retryable_failed_items_restored": preprocess_failures[
                "retryable_failed_item_count"
            ],
            "preprocess_parked_items_restored": preprocess_failures[
                "parked_item_count"
            ],
        }
        if self._restart_restore_telemetry:
            projection["restart"] = deepcopy(self._restart_restore_telemetry)
        return projection

    def _incremental_schedule_runtime(
        self,
        reference: dict[str, Any],
        queue_checkpoint: Any | None,
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        list[dict[str, Any] | None],
        dict[str, Any],
        dict[str, Any],
    ]:
        """Restore one queue from envelopes/witnesses, never a corpus-wide scan."""

        try:
            schedule, schedule_path, schedule_body = (
                self.modules.background.load_schedule(Path(reference["path"]))
            )
            if (
                schedule_path != Path(reference["path"])
                or sha256_bytes(schedule_body) != reference["sha256"]
                or schedule["schedule_id"] != reference["schedule_id"]
            ):
                raise BackendError(
                    "incremental schedule differs from controller configuration"
                )
            bundle = self.modules.queue_runner._load_bundle(
                Path(schedule["queue"]["manifest_path"])
            )
            if (
                sha256_bytes(bundle["body"])
                != schedule["queue"]["manifest_sha256"]
                or bundle["manifest"]["bundle_id"]
                != schedule["queue"]["bundle_id"]
            ):
                raise BackendError(
                    "incremental queue bundle differs from its sealed schedule"
                )
            binding = self._queue_replay_binding(reference, schedule)
            store = self._operational_replay_store
            if store is None:
                raise BackendError(
                    "incremental restore requires queue replay authority"
                )
            if queue_checkpoint is None:
                if cancellation_boundary is None:
                    restored = store.bootstrap_restart_snapshot(binding, bundle)
                else:
                    restored = store.bootstrap_restart_snapshot(
                        binding,
                        bundle,
                        cancellation_boundary=cancellation_boundary,
                    )
            else:
                # A bound checkpoint and the controller config describe one exact
                # immutable schedule set. Missing/reordered queue authority is
                # corruption or operator-visible schema drift, never permission to
                # silently downgrade that schedule to first-use trust.
                if self._trust_completed_copy:
                    restored = store.hydrate_restart_checkpoint(
                        binding,
                        bundle,
                        queue_checkpoint,
                        cancellation_boundary=cancellation_boundary,
                        trust_completed_copy=True,
                    )
                elif cancellation_boundary is None:
                    restored = store.hydrate_restart_checkpoint(
                        binding, bundle, queue_checkpoint
                    )
                else:
                    restored = store.hydrate_restart_checkpoint(
                        binding,
                        bundle,
                        queue_checkpoint,
                        cancellation_boundary=cancellation_boundary,
                    )
            exact_states = restored["states"]
            failure_states = self.modules.queue_runner._scan_failure_states(bundle)
            if len(exact_states) != len(failure_states):
                raise BackendError(
                    "incremental queue result and failure vectors differ"
                )
            states: list[dict[str, Any] | None] = []
            for entry, order, exact, failure in zip(
                bundle["manifest"]["work_orders"], bundle["orders"],
                exact_states, failure_states, strict=True
            ):
                if exact is not None:
                    if failure.get("quarantine") is not None:
                        verify_acquisition_retry_completion(
                            bundle, entry, order, failure["quarantine"], exact
                        )
                    states.append(
                        {
                            "queue_state": "completed",
                            "result_sha256": exact["result_sha256"],
                            "media_sha256": exact["media_sha256"],
                            "byte_count": exact["byte_count"],
                        }
                    )
                elif failure.get("quarantine") is not None:
                    states.append(
                        {
                            "queue_state": "quarantined",
                            "failure_attempt_count": len(failure["attempts"]),
                            "quarantine_receipt_sha256": failure["quarantine"][
                                "receipt_sha256"
                            ],
                        }
                    )
                else:
                    states.append(None)
            acknowledged = self.modules.background._acknowledged_results(
                Path(schedule["consumer"]["preprocess_state_root"]),
                bundle=bundle,
                states=states,
            )
            ready = self.modules.background._ready_snapshot(
                schedule, bundle, states, acknowledged
            )
        except (BackendError, StartupRestoreStopRequested):
            raise
        except Exception as error:
            raise BackendError(
                f"incremental schedule restore failed for "
                f"{reference.get('schedule_id')}: {error}"
            ) from error
        return schedule, bundle, states, ready, restored["telemetry"]

    def _validate_incremental_campaign_authority(
        self,
        runtimes: Sequence[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                list[dict[str, Any] | None],
                dict[str, Any],
            ]
        ],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Replay the small sealed campaign documents around restored queues."""

        inventory = self.config.section("campaign")["inventory"]
        try:
            inventory_body, _ = self.modules.queue_runner._stable_read(
                Path(inventory["path"]),
                maximum=256 * 1024 * 1024,
                label="sealed Archive campaign inventory",
            )
            inventory_value = json.loads(inventory_body)
        except Exception as error:
            raise BackendError(f"campaign inventory replay failed: {error}") from error
        if sha256_bytes(inventory_body) != inventory["sha256"]:
            raise BackendError("campaign inventory differs from its external SHA-256")
        coverage = self._inventory_coverage(inventory_value)
        plan_reference = inventory_value["inventory_basis"][
            "archive_only_queue_plan"
        ]
        try:
            plan_body, _ = self.modules.queue_runner._stable_read(
                Path(plan_reference["path"]),
                maximum=256 * 1024 * 1024,
                label="sealed Archive-only queue plan",
                required_mode=0o400,
            )
            if sha256_bytes(plan_body) != plan_reference["sha256"]:
                raise BackendError(
                    "Archive-only queue plan differs from inventory binding"
                )
            plan = self.modules.background.materialize_queue.validate_plan(
                json.loads(plan_body)
            )
            if plan_body != self.modules.background.materialize_queue.pretty_bytes(
                plan
            ):
                raise BackendError("Archive-only queue plan is not canonical")
        except BackendError:
            raise
        except Exception as error:
            raise BackendError(
                f"Archive-only queue plan replay failed: {error}"
            ) from error
        cold_storage_identity = self._validate_cold_storage_identity(runtimes)
        expected_by_role = self._expected_archive_role_identities(plan, coverage)
        observed_by_role = {role: set() for role in expected_by_role}
        for reference, _schedule, bundle, _states, _ready in runtimes:
            role = reference["role"]
            for entry in bundle["manifest"]["work_orders"]:
                identity = (entry["source_id"], entry["recording_id"])
                if identity in observed_by_role[role]:
                    raise BackendError(
                        "campaign schedule roles repeat an exact source/recording identity"
                    )
                observed_by_role[role].add(identity)
        scheduled_ready_count = len(observed_by_role["normal_processing"])
        scheduled_cold_only_count = len(
            observed_by_role["cold_acquisition_only_requires_chunking"]
        )
        if (
            observed_by_role != expected_by_role
            or scheduled_ready_count != coverage["ready_selected_count"]
            or scheduled_cold_only_count
            != coverage["parked_requires_chunking_count"]
            or scheduled_ready_count + scheduled_cold_only_count
            != coverage["candidate_count"]
        ):
            raise BackendError(
                "campaign schedule roles do not exactly cover the sealed Archive inventory"
            )
        coverage["scheduled_ready_count"] = scheduled_ready_count
        coverage["scheduled_cold_only_count"] = scheduled_cold_only_count
        coverage["scheduled_candidate_count"] = (
            scheduled_ready_count + scheduled_cold_only_count
        )
        schedule_set_coverage = self._validate_schedule_set_manifest(
            runtimes, coverage
        )
        coverage["schedule_set"] = schedule_set_coverage
        coverage["configured_normal_schedule_count"] = sum(
            row[0]["role"] == "normal_processing" for row in runtimes
        )
        coverage["configured_cold_only_schedule_count"] = sum(
            row[0]["role"] == "cold_acquisition_only_requires_chunking"
            for row in runtimes
        )
        _completed, quarantined, pending = self._runtime_counts(runtimes)
        coverage["scheduled_quarantined_count"] = quarantined
        coverage["scheduled_runnable_pending_count"] = pending
        return coverage, schedule_set_coverage, cold_storage_identity

    @staticmethod
    def _checkpoint_timestamp() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _validated_backend_checkpoint_envelope(
        self,
        value: Any,
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Validate the complete backend identity without replaying queue paths."""

        keys = {
            "kind",
            "schema_version",
            "backend_kind",
            "config_id",
            "config_sha256",
            "campaign_id",
            "schedule_set_id",
            "created_at",
            "queue_replay",
            "preprocess",
            "gpu",
            "cold_retention",
            "policy",
            "identity_sha256",
        }
        if not isinstance(value, dict) or set(value) != keys:
            raise BackendError("backend restart checkpoint has unexpected fields")
        core = {key: value[key] for key in keys - {"identity_sha256"}}
        identity = value["identity_sha256"]
        try:
            encoded = canonical_bytes(core)
            parsed_time = datetime.strptime(
                value["created_at"], "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
        except (TypeError, ValueError) as error:
            raise BackendError("backend restart checkpoint is not canonical") from error
        if cancellation_boundary is not None:
            cancellation_boundary()
        campaign = self.config.section("campaign")
        if (
            value["kind"] != BACKEND_CHECKPOINT_KIND
            or isinstance(value["schema_version"], bool)
            or value["schema_version"] != BACKEND_CHECKPOINT_SCHEMA_VERSION
            or value["backend_kind"] != BACKEND_KIND
            or value["config_id"] != self.config.config_id
            or value["config_sha256"] != self.config.physical_sha256
            or value["campaign_id"] != campaign["campaign_id"]
            or value["schedule_set_id"]
            != campaign["schedule_set"]["schedule_set_id"]
            or parsed_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            != value["created_at"]
            or not isinstance(identity, str)
            or re.fullmatch(r"[0-9a-f]{64}", identity) is None
            or identity != sha256_bytes(encoded)
            or value["policy"] != BACKEND_RESTART_POLICY
        ):
            raise BackendError("backend restart checkpoint identity is invalid")
        preprocess = value["preprocess"]
        gpu = value["gpu"]
        retention = value["cold_retention"]
        if (
            not isinstance(preprocess, dict)
            or set(preprocess) != {"failure_attempts", "candidates"}
            or not isinstance(preprocess["failure_attempts"], list)
            or not isinstance(preprocess["candidates"], list)
            or not isinstance(gpu, dict)
            or set(gpu) != {"records"}
            or not isinstance(gpu["records"], list)
            or not isinstance(retention, dict)
            or set(retention) != {"records"}
            or not isinstance(retention["records"], list)
            or any(
                len(rows) > BACKEND_CHECKPOINT_MAX_ROWS
                for rows in (
                    preprocess["failure_attempts"],
                    preprocess["candidates"],
                    gpu["records"],
                    retention["records"],
                )
            )
        ):
            raise BackendError("backend restart checkpoint ledgers are malformed")
        if cancellation_boundary is not None:
            cancellation_boundary()
        queue = value["queue_replay"]
        if not isinstance(queue, dict) or any(
            queue.get(key) != expected
            for key, expected in (
                ("config_id", self.config.config_id),
                ("config_sha256", self.config.physical_sha256),
                ("campaign_id", campaign["campaign_id"]),
                (
                    "schedule_set_id",
                    campaign["schedule_set"]["schedule_set_id"],
                ),
            )
        ):
            raise BackendError("queue restart checkpoint binding is invalid")
        return value, queue

    def _prepare_queue_restart_checkpoint(
        self,
        queue: dict[str, Any],
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> Any:
        """Parse exact queue restart authority after GPU containment is safe."""

        store = self._operational_replay_store
        if store is None:
            raise BackendError("backend restart checkpoint lacks queue authority")
        try:
            if cancellation_boundary is None:
                prepared = store.prepare_restart_checkpoint(queue)
            else:
                prepared = store.prepare_restart_checkpoint(
                    queue,
                    cancellation_boundary=cancellation_boundary,
                )
        except OperationalReplayError as error:
            raise BackendError(
                f"queue restart checkpoint is invalid: {error}"
            ) from error
        return prepared

    def _validated_backend_checkpoint(
        self,
        value: Any,
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> tuple[dict[str, Any], Any]:
        """Validate the backend envelope and prepare its queue restart authority."""

        checkpoint, queue = self._validated_backend_checkpoint_envelope(
            value,
            cancellation_boundary=cancellation_boundary,
        )
        prepared = self._prepare_queue_restart_checkpoint(
            queue,
            cancellation_boundary=cancellation_boundary,
        )
        # The state layer returned a freshly decoded, digest-validated document;
        # restore treats it as immutable. Avoid another full queue-document copy.
        return checkpoint, prepared

    def _journal_recovery_evidence(
        self,
        events: Sequence[dict[str, Any]],
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> tuple[
        dict[str, dict[str, Any]],
        list[tuple[dict[str, Any], str | None]],
        list[dict[str, Any]],
    ]:
        """Extract only durable backend evidence from an immutable journal tail."""

        retention_records: dict[str, dict[str, Any]] = {}
        gpu_records: dict[str, dict[str, Any]] = {}
        gpu_final_evidence: dict[
            str, tuple[dict[str, Any], str | None]
        ] = {}
        preprocess_failures: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, dict):
                raise BackendError("journal tail contains a malformed event")
            event_type = event.get("event_type")
            if event_type not in {"stage_finished", "gpu_child_quiesced"}:
                if cancellation_boundary is not None:
                    cancellation_boundary()
                continue
            payload = event.get("payload")
            outcome = payload.get("outcome") if isinstance(payload, dict) else None
            artifacts = outcome.get("artifacts") if isinstance(outcome, dict) else None
            if not isinstance(artifacts, dict) or artifacts.get(
                "backend_kind"
            ) != BACKEND_KIND:
                if cancellation_boundary is not None:
                    cancellation_boundary()
                continue
            for record, status in self._gpu_event_evidence(
                event_type, outcome, artifacts
            ):
                key = record["batch_key"]
                if key in gpu_records and gpu_records[key] != record:
                    raise BackendError("journal tail has conflicting GPU records")
                gpu_records[key] = record
                gpu_final_evidence[key] = (record, status)
            if event_type != "stage_finished":
                if cancellation_boundary is not None:
                    cancellation_boundary()
                continue
            retained = artifacts.get("retained", [])
            failures = artifacts.get("preprocess_failure_attempts", [])
            if not isinstance(retained, list) or not isinstance(failures, list):
                raise BackendError("journal tail backend artifacts are malformed")
            for record in retained:
                item_key = record.get("item_key") if isinstance(record, dict) else None
                if not isinstance(item_key, str) or not item_key:
                    raise BackendError(
                        "journal tail retention record has an invalid item key"
                    )
                if (
                    item_key in retention_records
                    and retention_records[item_key] != record
                ):
                    raise BackendError(
                        "journal tail has conflicting retention records"
                    )
                retention_records[item_key] = deepcopy(record)
            preprocess_failures.extend(deepcopy(failures))
            if cancellation_boundary is not None:
                cancellation_boundary()
        return (
            retention_records,
            list(gpu_final_evidence.values()),
            preprocess_failures,
        )

    def _restore_checkpoint_exact(
        self,
        backend_document: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
        *,
        cancellation_boundary: Callable[[], None] | None = None,
        gpu_authority_recovered_boundary: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        if self._lane_stage is not None:
            raise BackendError("a stage-confined backend cannot restore authority")
        if any(
            (
                self._runtime_cache,
                self._retained_items,
                self._gpu_records,
                self._preprocess_failure_attempts,
                self._preprocess_candidate_keys,
            )
        ):
            raise BackendError("backend restart restore requires a fresh authority root")
        if not isinstance(tail_events, (list, tuple)):
            raise BackendError("backend restart journal tail must be a sequence")
        if cancellation_boundary is not None:
            cancellation_boundary()

        metadata_bootstrap = backend_document is None
        if metadata_bootstrap:
            checkpoint = None
            queue_document = None
            queue_checkpoint = None
            preprocess_rows: Any = []
            gpu_rows: Any = []
            base_failures: list[dict[str, Any]] = []
            base_retention: list[dict[str, Any]] = []
        else:
            if cancellation_boundary is None:
                checkpoint, queue_document = (
                    self._validated_backend_checkpoint_envelope(
                        backend_document
                    )
                )
            else:
                checkpoint, queue_document = (
                    self._validated_backend_checkpoint_envelope(
                        backend_document,
                        cancellation_boundary=cancellation_boundary,
                    )
                )
            queue_checkpoint = None
            preprocess_rows = checkpoint["preprocess"]["candidates"]
            gpu_rows = checkpoint["gpu"]["records"]
            base_failures = checkpoint["preprocess"]["failure_attempts"]
            base_retention = checkpoint["cold_retention"]["records"]

        # Fold the already validated hash-chain tail before consulting campaign
        # queue paths. GPU event evidence is self-contained and can therefore bind
        # every durable child to exact batch authority before expensive queue
        # hydration begins.
        if cancellation_boundary is None:
            tail_retention, tail_gpu, tail_failures = (
                self._journal_recovery_evidence(tail_events)
            )
        else:
            tail_retention, tail_gpu, tail_failures = (
                self._journal_recovery_evidence(
                    tail_events,
                    cancellation_boundary=cancellation_boundary,
                )
            )

        if self.config.section("gpu_readiness")["enabled"]:
            if cancellation_boundary is None:
                gpu_telemetry = self._restore_gpu_checkpoint(
                    gpu_rows,
                    tail_gpu,
                    metadata_bootstrap=metadata_bootstrap,
                )
            else:
                gpu_telemetry = self._restore_gpu_checkpoint(
                    gpu_rows,
                    tail_gpu,
                    metadata_bootstrap=metadata_bootstrap,
                    cancellation_boundary=cancellation_boundary,
                )
            if gpu_authority_recovered_boundary is not None:
                gpu_authority_recovered_boundary()
        elif gpu_rows or tail_gpu:
            raise BackendError(
                "GPU restart evidence exists while GPU readiness is disabled"
            )
        else:
            gpu_telemetry = {
                "fast_reused_records": 0,
                "targeted_revalidated_records": 0,
                "metadata_bootstrap_records": 0,
            }

        if not metadata_bootstrap:
            if not isinstance(queue_document, dict):  # pragma: no cover - envelope invariant
                raise BackendError("backend checkpoint omitted queue restart authority")
            if cancellation_boundary is None:
                queue_checkpoint = self._prepare_queue_restart_checkpoint(
                    queue_document
                )
            else:
                queue_checkpoint = self._prepare_queue_restart_checkpoint(
                    queue_document,
                    cancellation_boundary=cancellation_boundary,
                )

        telemetry_rows: list[dict[str, Any]] = []
        runtimes = []
        for reference in self.config.section("campaign")["schedules"]:
            if cancellation_boundary is None:
                restored = self._incremental_schedule_runtime(
                    reference, queue_checkpoint
                )
            else:
                restored = self._incremental_schedule_runtime(
                    reference,
                    queue_checkpoint,
                    cancellation_boundary=cancellation_boundary,
                )
            schedule, bundle, states, ready, telemetry = restored
            self._runtime_cache[reference["schedule_id"]] = (
                schedule,
                bundle,
                states,
                ready,
            )
            runtimes.append((reference, schedule, bundle, states, ready))
            telemetry_rows.append(telemetry)
            generation = telemetry.get("generation")
            state_digest = telemetry.get("state_digest")
            self._record_checkpoint_queue_delta(
                {
                    "schedule_id": reference["schedule_id"],
                    "generation": generation,
                    "state_digest": state_digest,
                }
            )
            if cancellation_boundary is not None:
                cancellation_boundary()

        coverage, schedule_set_coverage, cold_identity = (
            self._validate_incremental_campaign_authority(runtimes)
        )
        self._campaign_coverage_checkpoint = deepcopy(coverage)
        self._schedule_set_coverage_checkpoint = deepcopy(schedule_set_coverage)
        self._cold_storage_identity_checkpoint = deepcopy(cold_identity)
        if cancellation_boundary is not None:
            cancellation_boundary()

        completed = {
            row["item_key"]: row for row in self._completed_acquisitions()
        }
        for record in [*base_failures, *tail_failures]:
            self._validate_preprocess_failure_record(record, completed)
            if cancellation_boundary is not None:
                cancellation_boundary()

        if cancellation_boundary is None:
            preprocess_telemetry = self._restore_preprocess_checkpoint(
                preprocess_rows,
                metadata_bootstrap=metadata_bootstrap,
            )
        else:
            preprocess_telemetry = self._restore_preprocess_checkpoint(
                preprocess_rows,
                metadata_bootstrap=metadata_bootstrap,
                cancellation_boundary=cancellation_boundary,
            )
        if self.config.section("gpu_readiness")["enabled"]:
            if cancellation_boundary is None:
                admitted_queue_manifests_loaded = (
                    self._hydrate_admitted_gpu_queues()
                )
            else:
                admitted_queue_manifests_loaded = (
                    self._hydrate_admitted_gpu_queues(
                        cancellation_boundary=cancellation_boundary
                    )
                )
            gpu_telemetry["admitted_queue_manifests_loaded"] = (
                admitted_queue_manifests_loaded
            )
            gpu_telemetry["admitted_queue_count"] = len(self._gpu_queue_ids)
        else:
            gpu_telemetry["admitted_queue_manifests_loaded"] = 0
            gpu_telemetry["admitted_queue_count"] = 0
        normal_ready_keys = {
            f"{reference['schedule_id']}:{row['ordinal']}"
            for reference, _schedule, _bundle, _states, ready in runtimes
            if reference["role"] == "normal_processing"
            for row in ready.get("items", [])
            if isinstance(row, dict)
            and isinstance(row.get("ordinal"), int)
            and not isinstance(row.get("ordinal"), bool)
        }
        self._preprocess_resolved_after_retry = {
            item_key
            for item_key, attempts in self._preprocess_failure_attempts.items()
            if attempts[-1]["disposition"] == "retryable"
            and item_key not in normal_ready_keys
        }

        retention_records: dict[str, dict[str, Any]] = {}
        for record in base_retention:
            item_key = record.get("item_key") if isinstance(record, dict) else None
            if not isinstance(item_key, str) or not item_key:
                raise BackendError("checkpoint retention record is malformed")
            if item_key in retention_records:
                raise BackendError("checkpoint repeats a retention record")
            retention_records[item_key] = deepcopy(record)
            if cancellation_boundary is not None:
                cancellation_boundary()
        for item_key, record in tail_retention.items():
            previous = retention_records.get(item_key)
            if previous is not None and previous != record:
                raise BackendError("retention checkpoint and tail conflict")
            retention_records[item_key] = deepcopy(record)
            if cancellation_boundary is not None:
                cancellation_boundary()
        if self.config.section("cold_retention")["enabled"]:
            for item_key, recorded in sorted(retention_records.items()):
                row = completed.get(item_key)
                if row is None:
                    raise BackendError(
                        "retention checkpoint is outside completed acquisition"
                    )
                expected_pins = {
                    "item_key": row["item_key"],
                    "schedule_id": row["schedule_id"],
                    "ordinal": row["ordinal"],
                    "job_id": row["job_id"],
                    "work_order_sha256": row["work_order_sha256"],
                    "result_sha256": row["result_sha256"],
                    "media_sha256": row["media_sha256"],
                    "media_byte_count": row["media_byte_count"],
                }
                if any(
                    recorded.get(key) != expected
                    for key, expected in expected_pins.items()
                ):
                    raise BackendError(
                        "retention checkpoint pins differ from acquisition"
                    )
                self._retained_items[item_key] = recorded
                if cancellation_boundary is not None:
                    cancellation_boundary()
            # Retention is uncommon and externally copied; retain its pre-existing
            # exact replay behavior rather than applying TOFU to those copies.
            self._retention_replay_pending = sorted(self._retained_items)
        elif retention_records:
            raise BackendError(
                "retention evidence exists while cold retention is disabled"
            )

        def total(key: str) -> int:
            values = [row.get(key, 0) for row in telemetry_rows]
            if any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                for value in values
            ):
                raise BackendError(
                    f"queue restart telemetry {key} is malformed"
                )
            return sum(values)

        self._restart_restore_telemetry = {
            "mode": (
                "metadata_bootstrap"
                if metadata_bootstrap
                else "checkpoint_plus_tail"
            ),
            "tail_event_count": len(tail_events),
            "schedule_count": len(telemetry_rows),
            "fast_reused_acquisition_items": total("fast_reused_items"),
            "targeted_revalidated_acquisition_items": total(
                "targeted_revalidated_items"
            ),
            "metadata_bootstrap_acquisition_items": total(
                "metadata_bootstrap_items"
            ),
            "trusted_copy_acquisition_items": total("trusted_copy_items"),
            "result_envelope_bytes_read": total(
                "result_envelope_bytes_read"
            ),
            "targeted_revalidated_media_bytes": total(
                "targeted_revalidated_media_bytes"
            ),
            "avoided_legacy_logical_payload_bytes": total(
                "avoided_logical_payload_bytes"
            ),
            "preprocess": preprocess_telemetry,
            "gpu": gpu_telemetry,
        }
        if cancellation_boundary is not None:
            cancellation_boundary()
        return self._recovery_monitor_projection()

    def _startup_gpu_child_authority_is_empty(self) -> bool:
        """Prove no persisted child needs exact recovered batch authority."""

        gpu = self.config.section("gpu_readiness")
        try:
            return PrivateGpuChildJournal.prove_empty_startup_authority(
                Path(gpu["child_journal_root"])
            )
        except Exception as error:
            raise BackendError(
                f"GPU child startup authority preflight failed: {error}"
            ) from error

    def _quiesce_startup_restored_gpu_authority(self) -> None:
        """Contain exact restored child authority before a clean startup Stop."""

        outcome = self.quiesce()
        normalized = outcome.normalized()
        monitor = normalized["monitor"]
        if (
            normalized["stage"] != "gpu_readiness"
            or monitor.get("active_children") != 0
            or monitor.get("current_gpu_child") is not None
            or monitor.get("stop_reconciled") is not True
        ):
            raise BackendError(
                "startup GPU quiesce did not certify terminal child authority"
            )

    def restore_checkpoint(
        self,
        backend_document: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        """Restore via a durable snapshot and immutable tail, without corpus rehash."""

        return self._restore_checkpoint_with_boundary(
            backend_document,
            tail_events,
            cancellation_boundary=None,
            gpu_authority_recovered_boundary=None,
        )

    def restore_rsync_copy_checkpoint(
        self,
        backend_document: dict[str, Any],
        tail_events: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        """Rebind saved media witnesses after an explicitly trusted rsync copy.

        Only the reviewed config/UUID transition is eligible. Existing completed
        envelopes must remain exact; new crash-window results are still hashed.
        The CLI additionally requires stopped intent and the singleton run lock.
        """

        if backend_document is None or reviewed_cold_mount_transition(self.config) is None:
            raise BackendError("trusted-copy recovery requires a checkpoint and reviewed mount migration")
        self._validate_cold_storage_identity([])
        if self._trust_completed_copy:
            raise BackendError("trusted-copy recovery is already active")
        self._trust_completed_copy = True
        try:
            recovery = self.restore_checkpoint(backend_document, tail_events)
            recovery["trusted_copy_migration"] = {
                "operator_trust": "rsync_completed_media_copy",
                "completed_media_rehashed": False,
                "new_results_exactly_validated": True,
            }
            return recovery
        finally:
            self._trust_completed_copy = False

    def restore_checkpoint_interruptibly(
        self,
        backend_document: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
        *,
        cancellation_boundary: Callable[[], None],
    ) -> dict[str, Any]:
        """Restore while observing Stop only after complete immutable units."""

        if not callable(cancellation_boundary):
            raise BackendError("startup restore cancellation boundary is invalid")
        early_empty = self._startup_gpu_child_authority_is_empty()
        if (
            not self.config.section("gpu_readiness")["enabled"]
            and not early_empty
        ):
            raise BackendError(
                "GPU child startup authority exists while GPU readiness is disabled"
            )
        state = {"stop_latched": False, "gpu_authority_recovered": False}

        def gated_cancellation_boundary() -> None:
            nonlocal early_empty
            if state["stop_latched"]:
                if state["gpu_authority_recovered"]:
                    self._quiesce_startup_restored_gpu_authority()
                    raise StartupRestoreStopRequested
                return
            try:
                cancellation_boundary()
            except StartupRestoreStopRequested:
                if early_empty:
                    # Re-prove exact empty authority under the launch
                    # lock at cancellation time; the initial certificate cannot
                    # authorize a later clean exit if journal authority changed.
                    if self._startup_gpu_child_authority_is_empty():
                        raise
                    early_empty = False
                state["stop_latched"] = True
                if state["gpu_authority_recovered"]:
                    self._quiesce_startup_restored_gpu_authority()
                    raise

        def gpu_authority_recovered_boundary() -> None:
            state["gpu_authority_recovered"] = True
            gated_cancellation_boundary()

        return self._restore_checkpoint_with_boundary(
            backend_document,
            tail_events,
            cancellation_boundary=gated_cancellation_boundary,
            gpu_authority_recovered_boundary=(
                gpu_authority_recovered_boundary
            ),
        )

    def _restore_checkpoint_with_boundary(
        self,
        backend_document: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
        *,
        cancellation_boundary: Callable[[], None] | None,
        gpu_authority_recovered_boundary: Callable[[], None] | None,
    ) -> dict[str, Any]:
        """Apply the shared provenance scope around one restart attempt."""

        preprocess_batch = getattr(self.modules, "preprocess_batch", None)
        scope = getattr(
            preprocess_batch, "restore_scoped_tool_provenance_witness", None
        )
        witness_error = getattr(
            preprocess_batch, "RestoreToolProvenanceError", None
        )
        batch_error = getattr(preprocess_batch, "BatchError", None)
        if not callable(scope):
            if isinstance(self.modules, _Modules):
                raise BackendError(
                    "sealed preprocess module lacks the restore provenance witness"
                )
            return self._restore_checkpoint_exact(
                backend_document,
                tail_events,
                cancellation_boundary=cancellation_boundary,
                gpu_authority_recovered_boundary=(
                    gpu_authority_recovered_boundary
                ),
            )
        if isinstance(self.modules, _Modules) and not (
            isinstance(witness_error, type)
            and isinstance(batch_error, type)
            and issubclass(witness_error, batch_error)
        ):
            raise BackendError(
                "sealed preprocess module lacks the restore provenance error boundary"
            )
        try:
            with scope():
                return self._restore_checkpoint_exact(
                    backend_document,
                    tail_events,
                    cancellation_boundary=cancellation_boundary,
                    gpu_authority_recovered_boundary=(
                        gpu_authority_recovered_boundary
                    ),
                )
        except Exception as error:
            if isinstance(witness_error, type) and isinstance(error, witness_error):
                raise BackendError(
                    f"restore tool provenance witness failed: {error}"
                ) from error
            raise

    def _export_preprocess_checkpoint(self) -> dict[str, Any]:
        if self._preprocess_candidates_cache is None:
            # This state arises only after an acknowledged partial-prefix race.
            # Rebuild the index from canonical receipts and metadata witnesses;
            # the receipts were deeply admitted by the active producer.
            self._restore_preprocess_checkpoint([], metadata_bootstrap=True)
        candidates = []
        for campaign_ordinal, bundle_path, bundle_id, state_root in (
            self._preprocess_bundle_candidates()
        ):
            key = (str(state_root), bundle_id)
            item_count = self._preprocess_bundle_item_counts.get(key)
            if (
                isinstance(item_count, bool)
                or not isinstance(item_count, int)
                or item_count < 1
            ):
                raise BackendError(
                    f"preprocess checkpoint lacks item count for {bundle_id}"
                )
            files = self._preprocess_restart_witnesses.get(key)
            if files is None:
                # New bundles have just crossed the producer's exact validation
                # boundary. Capture that admitted state once. Re-exporting an old
                # candidate must retain its prior witness: refreshing unverified
                # media metadata here could bless out-of-band drift.
                files = self._preprocess_candidate_restart_witness(
                    bundle_path=bundle_path,
                    bundle_id=bundle_id,
                    state_root=state_root,
                    item_count=item_count,
                )
                self._preprocess_restart_witnesses[key] = deepcopy(files)
            else:
                files = deepcopy(files)
            candidates.append(
                {
                    "campaign_ordinal": campaign_ordinal,
                    "bundle_path": str(bundle_path),
                    "bundle_id": bundle_id,
                    "state_root": str(state_root),
                    "item_count": item_count,
                    "files": files,
                }
            )
        failures = [
            deepcopy(record)
            for item_key in sorted(self._preprocess_failure_attempts)
            for record in self._preprocess_failure_attempts[item_key]
        ]
        return {"failure_attempts": failures, "candidates": candidates}

    def _export_gpu_checkpoint(self) -> dict[str, Any]:
        rows = []
        for key in sorted(self._gpu_records):
            record = self._gpu_records[key]
            status = self._gpu_status.get(key)
            item_count, claims, dispositions = self._gpu_checkpoint_derivation(
                record
            )
            if (
                status not in {"pending", "completed", "parked", "not_applicable"}
                or self._gpu_batch_item_counts.get(key) != item_count
                or any(
                    self._gpu_member_claims.get(claim) != key for claim in claims
                )
                or any(
                    self._gpu_queue_dispositions.get(queue_id) != disposition
                    for queue_id, disposition in dispositions
                )
            ):
                raise BackendError("GPU checkpoint ledgers differ")
            parked = self._gpu_parked.get(key)
            if (status == "parked") != (parked is not None):
                raise BackendError("GPU checkpoint parking ledger differs")
            files = self._gpu_restart_witnesses.get(key)
            if files is None:
                # The record/status transition was exactly replayed immediately
                # before its cache entry was invalidated. Bind it once; later
                # exports preserve that witness instead of trusting refreshed
                # transcript metadata without a new exact replay.
                files = self._gpu_record_restart_witness(record, status)
                self._gpu_restart_witnesses[key] = deepcopy(files)
            else:
                files = deepcopy(files)
            rows.append(
                {
                    "record": deepcopy(record),
                    "status": status,
                    "item_count": item_count,
                    "claims": [[queue_id, ordinal] for queue_id, ordinal in claims],
                    "dispositions": [
                        {"queue_id": queue_id, "value": deepcopy(disposition)}
                        for queue_id, disposition in dispositions
                    ],
                    "parked": deepcopy(parked),
                    "files": files,
                }
            )
        return {"records": rows}

    def export_checkpoint(self) -> dict[str, Any] | None:
        """Export complete restart authority; no media payload is read here."""

        if self._lane_stage is not None:
            raise BackendError("a stage-confined backend cannot export authority")
        if self._gpu_opportunity_lease is not None:
            raise BackendError("cannot checkpoint while a GPU opportunity is held")
        if any(
            value is None
            for value in (
                self._campaign_coverage_checkpoint,
                self._schedule_set_coverage_checkpoint,
                self._cold_storage_identity_checkpoint,
            )
        ):
            raise BackendError("backend must be restored before checkpoint export")
        store = self._operational_replay_store
        if store is None:
            raise BackendError("checkpoint export lacks queue replay authority")
        campaign = self.config.section("campaign")
        expected_schedule_ids = {
            row["schedule_id"] for row in campaign["schedules"]
        }
        if set(self._checkpoint_queue_snapshots) != expected_schedule_ids:
            raise BackendError(
                "checkpoint queue authority does not cover the sealed schedule set"
            )
        created_at = self._checkpoint_timestamp()
        try:
            queue = store.export_restart_checkpoint(
                config_id=self.config.config_id,
                config_sha256=self.config.physical_sha256,
                campaign_id=campaign["campaign_id"],
                schedule_set_id=campaign["schedule_set"]["schedule_set_id"],
                created_at=created_at,
                expected_snapshots=deepcopy(
                    self._checkpoint_queue_snapshots
                ),
            )
        except CheckpointDeferred:
            # An independent producer committed logical shared state before the
            # coordinator journaled and applied that lane outcome. Witness-only
            # generations are accepted by the replay store. Keep the older
            # checkpoint and pending journal anchor for a true digest difference;
            # a later idle poll will retry after root authority catches up.
            return None
        except OperationalReplayError as error:
            raise BackendError(
                f"queue restart checkpoint export failed: {error}"
            ) from error
        core = {
            "kind": BACKEND_CHECKPOINT_KIND,
            "schema_version": BACKEND_CHECKPOINT_SCHEMA_VERSION,
            "backend_kind": BACKEND_KIND,
            "config_id": self.config.config_id,
            "config_sha256": self.config.physical_sha256,
            "campaign_id": campaign["campaign_id"],
            "schedule_set_id": campaign["schedule_set"]["schedule_set_id"],
            "created_at": created_at,
            "queue_replay": queue,
            "preprocess": self._export_preprocess_checkpoint(),
            "gpu": self._export_gpu_checkpoint(),
            "cold_retention": {
                "records": [
                    deepcopy(self._retained_items[key])
                    for key in sorted(self._retained_items)
                ]
            },
            "policy": deepcopy(BACKEND_RESTART_POLICY),
        }
        return {
            **core,
            "identity_sha256": sha256_bytes(canonical_bytes(core)),
        }

    def rebuild_from_checkpoint(
        self,
        backend_document: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
    ) -> tuple["SealedArchiveBackend", dict[str, Any]]:
        """Construct a fresh root from the same checkpoint boundary after a fault."""

        if self._lane_stage is not None:
            raise BackendError("a stage-confined fork cannot rebuild authority")
        rebuilt = SealedArchiveBackend(
            self.config,
            modules=self.modules,
            gpu_executor=self._gpu_executor,
        )
        recovery = rebuilt.restore_checkpoint(backend_document, tail_events)
        return rebuilt, recovery

    def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
        """Restore exact controller authority under one tool-version witness."""

        preprocess_batch = getattr(self.modules, "preprocess_batch", None)
        scope = getattr(
            preprocess_batch,
            "restore_scoped_tool_provenance_witness",
            None,
        )
        witness_error = getattr(
            preprocess_batch,
            "RestoreToolProvenanceError",
            None,
        )
        batch_error = getattr(preprocess_batch, "BatchError", None)
        if not callable(scope):
            if isinstance(self.modules, _Modules):
                raise BackendError(
                    "sealed preprocess module lacks the restore provenance witness"
                )
            # Deliberately small test doubles do not execute the production media
            # replay boundary and need not emulate its process-local witness.
            return self._restore_exact(events)
        if isinstance(self.modules, _Modules) and not (
            isinstance(witness_error, type)
            and isinstance(batch_error, type)
            and issubclass(witness_error, batch_error)
        ):
            raise BackendError(
                "sealed preprocess module lacks the restore provenance error boundary"
            )
        try:
            with scope():
                return self._restore_exact(events)
        except Exception as error:
            if isinstance(witness_error, type) and isinstance(error, witness_error):
                raise BackendError(
                    f"restore tool provenance witness failed: {error}"
                ) from error
            raise

    def _restore_exact(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
        retention_records: dict[str, dict[str, Any]] = {}
        gpu_records: dict[str, dict[str, Any]] = {}
        gpu_evidence: list[tuple[dict[str, Any], str | None]] = []
        preprocess_failure_records: list[dict[str, Any]] = []
        for event in events:
            event_type = event.get("event_type")
            if event_type not in {"stage_finished", "gpu_child_quiesced"}:
                continue
            outcome = event.get("payload", {}).get("outcome", {})
            artifacts = outcome.get("artifacts", {}) if isinstance(outcome, dict) else {}
            if artifacts.get("backend_kind") != BACKEND_KIND:
                continue
            evidence = self._gpu_event_evidence(
                event_type, outcome, artifacts
            )
            for record, status in evidence:
                key = record["batch_key"]
                if key in gpu_records and gpu_records[key] != record:
                    raise BackendError("journal has conflicting GPU records")
                gpu_records[key] = record
                gpu_evidence.append((record, status))
            if event_type == "stage_finished":
                for record in artifacts.get("retained", []):
                    item_key = record.get("item_key") if isinstance(record, dict) else None
                    if not isinstance(item_key, str) or not item_key:
                        raise BackendError("journal retention record has an invalid item key")
                    if item_key in retention_records and retention_records[item_key] != record:
                        raise BackendError("journal has conflicting retention records")
                    retention_records[item_key] = record
                raw_preprocess_failures = artifacts.get(
                    "preprocess_failure_attempts", []
                )
                if not isinstance(raw_preprocess_failures, list):
                    raise BackendError("journal preprocess failure artifacts are malformed")
                preprocess_failure_records.extend(raw_preprocess_failures)

        inventory = self.config.section("campaign")["inventory"]
        try:
            inventory_body, _ = self.modules.queue_runner._stable_read(
                Path(inventory["path"]),
                maximum=256 * 1024 * 1024,
                label="sealed Archive campaign inventory",
            )
            inventory_value = json.loads(inventory_body)
        except Exception as error:
            raise BackendError(f"campaign inventory replay failed: {error}") from error
        if sha256_bytes(inventory_body) != inventory["sha256"]:
            raise BackendError("campaign inventory differs from its external SHA-256")
        coverage = self._inventory_coverage(inventory_value)

        plan_reference = inventory_value["inventory_basis"][
            "archive_only_queue_plan"
        ]
        try:
            plan_body, _ = self.modules.queue_runner._stable_read(
                Path(plan_reference["path"]),
                maximum=256 * 1024 * 1024,
                label="sealed Archive-only queue plan",
                required_mode=0o400,
            )
            if sha256_bytes(plan_body) != plan_reference["sha256"]:
                raise BackendError(
                    "Archive-only queue plan differs from inventory binding"
                )
            plan = self.modules.background.materialize_queue.validate_plan(
                json.loads(plan_body)
            )
            if plan_body != self.modules.background.materialize_queue.pretty_bytes(plan):
                raise BackendError("Archive-only queue plan is not canonical")
        except BackendError:
            raise
        except Exception as error:
            raise BackendError(f"Archive-only queue plan replay failed: {error}") from error

        runtimes = self._campaign_runtimes()
        if self._operational_replay_store is not None:
            for reference, schedule, _bundle, _states, _ready in runtimes:
                try:
                    summary = self._operational_replay_store.snapshot_summary(
                        self._queue_replay_binding(reference, schedule)
                    )
                except OperationalReplayError as error:
                    raise BackendError(
                        f"deep restore checkpoint authority failed: {error}"
                    ) from error
                self._record_checkpoint_queue_delta(summary)
        cold_storage_identity = self._validate_cold_storage_identity(runtimes)
        expected_by_role = self._expected_archive_role_identities(plan, coverage)
        observed_by_role = {role: set() for role in expected_by_role}
        for reference, _schedule, bundle, _states, _ready in runtimes:
            role = reference["role"]
            for entry in bundle["manifest"]["work_orders"]:
                identity = (entry["source_id"], entry["recording_id"])
                if identity in observed_by_role[role]:
                    raise BackendError(
                        "campaign schedule roles repeat an exact source/recording identity"
                    )
                observed_by_role[role].add(identity)
        scheduled_ready_count = len(observed_by_role["normal_processing"])
        scheduled_cold_only_count = len(
            observed_by_role["cold_acquisition_only_requires_chunking"]
        )
        if (
            observed_by_role != expected_by_role
            or scheduled_ready_count != coverage["ready_selected_count"]
            or scheduled_cold_only_count
            != coverage["parked_requires_chunking_count"]
            or scheduled_ready_count + scheduled_cold_only_count
            != coverage["candidate_count"]
        ):
            raise BackendError(
                "campaign schedule roles do not exactly cover the sealed Archive inventory"
            )
        coverage["scheduled_ready_count"] = scheduled_ready_count
        coverage["scheduled_cold_only_count"] = scheduled_cold_only_count
        coverage["scheduled_candidate_count"] = (
            scheduled_ready_count + scheduled_cold_only_count
        )
        schedule_set_coverage = self._validate_schedule_set_manifest(
            runtimes, coverage
        )
        coverage["schedule_set"] = schedule_set_coverage
        coverage["configured_normal_schedule_count"] = sum(
            row[0]["role"] == "normal_processing" for row in runtimes
        )
        coverage["configured_cold_only_schedule_count"] = sum(
            row[0]["role"] == "cold_acquisition_only_requires_chunking"
            for row in runtimes
        )
        _scheduled_completed, scheduled_quarantined, scheduled_pending = (
            self._runtime_counts(runtimes)
        )
        coverage["scheduled_quarantined_count"] = scheduled_quarantined
        coverage["scheduled_runnable_pending_count"] = scheduled_pending

        completed = {row["item_key"]: row for row in self._completed_acquisitions()}
        for record in preprocess_failure_records:
            self._validate_preprocess_failure_record(record, completed)
        normal_ready_keys = {
            f"{reference['schedule_id']}:{row['ordinal']}"
            for reference, _schedule, _bundle, _states, ready in runtimes
            if reference["role"] == "normal_processing"
            for row in ready.get("items", [])
            if isinstance(row, dict) and isinstance(row.get("ordinal"), int)
        }
        self._preprocess_resolved_after_retry = {
            item_key
            for item_key, attempts in self._preprocess_failure_attempts.items()
            if attempts[-1]["disposition"] == "retryable"
            and item_key not in normal_ready_keys
        }
        if self.config.section("cold_retention")["enabled"]:
            for item_key, recorded in sorted(retention_records.items()):
                row = completed.get(item_key)
                if row is None:
                    raise BackendError("journal retains an acquisition that is not completed")
                expected_pins = {
                    "item_key": row["item_key"],
                    "schedule_id": row["schedule_id"],
                    "ordinal": row["ordinal"],
                    "job_id": row["job_id"],
                    "work_order_sha256": row["work_order_sha256"],
                    "result_sha256": row["result_sha256"],
                    "media_sha256": row["media_sha256"],
                    "media_byte_count": row["media_byte_count"],
                }
                if any(recorded.get(key) != value for key, value in expected_pins.items()):
                    raise BackendError("journal retention pins differ from acquisition replay")
                self._retained_items[item_key] = recorded
            self._retention_replay_pending = sorted(self._retained_items)

        if self.config.section("gpu_readiness")["enabled"]:
            self._apply_gpu_records(
                gpu_evidence,
                derive_missing_status=True,
            )
            self._restore_gpu_attempt_exhaustion()
        elif gpu_evidence:
            raise BackendError("journal has GPU records while GPU readiness is disabled")
        self._campaign_coverage_checkpoint = deepcopy(coverage)
        self._schedule_set_coverage_checkpoint = deepcopy(schedule_set_coverage)
        self._cold_storage_identity_checkpoint = deepcopy(cold_storage_identity)
        self._restart_restore_telemetry = {
            "mode": "deep_audit",
            "tail_event_count": len(events),
            "fast_reused_acquisition_items": 0,
            "targeted_revalidated_acquisition_items": len(completed),
            "metadata_bootstrap_acquisition_items": 0,
            "preprocess_fast_reused_bundles": 0,
            "preprocess_targeted_revalidated_bundles": (
                self._preprocessed_item_total
            ),
        }
        return self._recovery_monitor_projection()

    @contextmanager
    def _acquisition_stop_gate(self):
        """Observe durable Stop between queue items without changing pinned code."""

        runner = self.modules.queue_runner
        original_capacity = getattr(runner, "_capacity_allows", None)
        original_dispatch = getattr(runner, "_dispatch_one", None)
        deadline_error = getattr(runner, "QueueDeadlineError", None)
        reservation_bytes = getattr(runner, "_reservation_bytes", None)
        runner_canonical = getattr(runner, "canonical_bytes", None)
        runner_sha256 = getattr(runner, "sha256_bytes", None)
        if (
            not callable(original_capacity)
            or not callable(original_dispatch)
            or not isinstance(deadline_error, type)
            or not issubclass(deadline_error, Exception)
            or not callable(reservation_bytes)
            or not callable(runner_canonical)
            or not callable(runner_sha256)
        ):
            raise BackendError("sealed queue runner lacks the exact dispatch gates")
        if not self._acquisition_gate_lock.acquire(blocking=False):
            raise BackendError("a concurrent acquisition stop gate is forbidden")
        observed: dict[str, Any] = {
            "stop_requested_between_items": False,
            "stop_requested_before_dispatch": None,
        }

        def gated_capacity(*args: Any, **kwargs: Any) -> bool:
            control = read_control_state(self.config)
            if control["desired_state"] == "stopped":
                observed["stop_requested_between_items"] = True
                return False
            returned = original_capacity(*args, **kwargs)
            if not isinstance(returned, bool):
                raise BackendError("sealed queue capacity gate returned non-boolean")
            return returned

        def gated_dispatch(order: dict[str, Any], remaining_seconds: float):
            control = read_control_state(self.config)
            if control["desired_state"] == "stopped":
                observed["stop_requested_between_items"] = True
                observed["stop_requested_before_dispatch"] = {
                    "work_order_sha256": runner_sha256(runner_canonical(order)),
                    "reservation_bytes": reservation_bytes(order),
                }
                # queue_runner already treats this as a clean finite boundary.
                # Its provisional deadline accounting is normalized by this
                # backend before the summary crosses the sealed adapter boundary.
                raise deadline_error(
                    "durable stop requested immediately before dispatch"
                )
            return original_dispatch(order, remaining_seconds)

        try:
            setattr(runner, "_capacity_allows", gated_capacity)
            setattr(runner, "_dispatch_one", gated_dispatch)
        except Exception:
            setattr(runner, "_capacity_allows", original_capacity)
            setattr(runner, "_dispatch_one", original_dispatch)
            self._acquisition_gate_lock.release()
            raise
        changed = False
        try:
            yield observed
        finally:
            changed = (
                getattr(runner, "_capacity_allows", None) is not gated_capacity
                or getattr(runner, "_dispatch_one", None) is not gated_dispatch
            )
            setattr(runner, "_capacity_allows", original_capacity)
            setattr(runner, "_dispatch_one", original_dispatch)
            self._acquisition_gate_lock.release()
            if changed:
                raise BackendError(
                    "sealed queue dispatch gates changed during acquisition"
                )

    def _normalize_predispatch_stop(
        self, result: dict[str, Any], observed: dict[str, Any]
    ) -> dict[str, Any]:
        """Remove queue_runner's provisional deadline accounting for a stop gate."""

        stopped = observed.get("stop_requested_before_dispatch")
        queue = result.get("queue_summary")
        if not isinstance(stopped, dict) or not isinstance(queue, dict):
            raise BackendError("predispatch stop did not return a bounded queue summary")
        normalized = deepcopy(result)
        normalized_queue = deepcopy(queue)
        matches = [
            row
            for row in normalized_queue.get("results", [])
            if isinstance(row, dict)
            and row.get("work_order_sha256") == stopped.get("work_order_sha256")
            and row.get("action") == "deadline_interrupted"
            and row.get("adapter_invoked") is True
        ]
        reservation = stopped.get("reservation_bytes")
        invocation_count = normalized_queue.get("adapter_invocation_count")
        reserved = normalized_queue.get("dispatch_reservation_bytes")
        if (
            len(matches) != 1
            or isinstance(reservation, bool)
            or not isinstance(reservation, int)
            or reservation < 0
            or isinstance(invocation_count, bool)
            or not isinstance(invocation_count, int)
            or invocation_count < 1
            or isinstance(reserved, bool)
            or not isinstance(reserved, int)
            or reserved < reservation
        ):
            raise BackendError("predispatch stop queue accounting is malformed")
        matches[0]["action"] = "stop_requested_before_dispatch"
        matches[0]["adapter_invoked"] = False
        normalized_queue["adapter_invocation_count"] = invocation_count - 1
        normalized_queue["dispatch_reservation_bytes"] = reserved - reservation
        normalized_queue["stop_reason"] = "durable_stop_requested_between_items"
        queue_core = {
            key: value
            for key, value in normalized_queue.items()
            if key != "summary_sha256"
        }
        normalized_queue["summary_sha256"] = self.modules.queue_runner.sha256_bytes(
            self.modules.queue_runner.canonical_bytes(queue_core)
        )
        normalized["queue_summary"] = normalized_queue
        normalized["stop_reason"] = "durable_stop_requested_between_items"
        result_core = {
            key: value for key, value in normalized.items() if key != "summary_sha256"
        }
        if "summary_sha256" in normalized:
            normalized["summary_sha256"] = self.modules.background.sha256_bytes(
                self.modules.background.canonical_bytes(result_core)
            )
        return normalized

    def _run_acquisition(self, *, item_limit: int | None = None) -> StageOutcome:
        if item_limit is not None:
            raise BackendError(
                "acquisition item overrides would amplify completed-media replay"
            )
        section = self.config.section("acquisition")
        runtimes = self._campaign_runtimes()
        self._validate_cold_storage_identity(runtimes)
        completed_total, quarantined_total, pending_total = self._runtime_counts(
            runtimes
        )
        normal_runtimes = [
            row for row in runtimes if row[0]["role"] == "normal_processing"
        ]
        cold_only_runtimes = [
            row
            for row in runtimes
            if row[0]["role"] == "cold_acquisition_only_requires_chunking"
        ]
        global_ready_items, global_ready_bytes, raw_normal_ready_items = (
            self._preprocess_ready_counts(normal_runtimes)
        )
        cold_only_ready_items = sum(
            row[4]["ready_item_count"] for row in cold_only_runtimes
        )
        cold_only_ready_bytes = sum(
            row[4]["ready_byte_count"] for row in cold_only_runtimes
        )
        campaign = self.config.section("campaign")
        normal_backpressured = (
            global_ready_items >= campaign["global_ready_high_items"]
            or global_ready_bytes >= campaign["global_ready_high_bytes"]
        )
        selected = next(
            (
                row
                for row in runtimes
                if any(state is None for state in row[3])
                and row[4]["zone"] == "at_or_below_low_water"
                and not (
                    normal_backpressured
                    and row[0]["role"] == "normal_processing"
                )
            ),
            None,
        )
        if selected is None:
            return StageOutcome(
                "acquisition",
                "complete" if pending_total == 0 else "held",
                False,
                {
                    "completed": completed_total,
                    "pending": pending_total,
                    "quarantined_items": quarantined_total,
                    "ready_items": global_ready_items,
                    "ready_bytes": global_ready_bytes,
                    "raw_normal_ready_items": raw_normal_ready_items,
                    "preprocess_parked_ready_items": (
                        raw_normal_ready_items - global_ready_items
                    ),
                    "cold_only_ready_items": cold_only_ready_items,
                    "cold_only_ready_bytes": cold_only_ready_bytes,
                    "new_items": 0,
                    "new_bytes": 0,
                    "stop_reason": (
                        "all_runnable_work_exhausted_with_quarantine"
                        if pending_total == 0 and quarantined_total
                        else "all_acquired"
                        if pending_total == 0
                        else "campaign_global_ready_high_water"
                        if normal_backpressured
                        else "per_schedule_hysteresis_hold"
                    ),
                },
                {"backend_kind": BACKEND_KIND},
            )
        reference = selected[0]
        limits = section[reference["role"]]
        replay_session = None
        try:
            with self._operational_queue_replay(
                reference, selected[1]
            ) as replay_session:
                with self._acquisition_stop_gate() as stop_gate:
                    result = self.modules.background.run_producer(
                        Path(reference["path"]),
                        max_new_items=limits["max_new_items"],
                        max_new_bytes=limits["max_new_bytes"],
                        max_run_seconds=limits["max_run_seconds"],
                        free_space_floor_bytes=limits["free_space_floor_bytes"],
                    )
                    if stop_gate["stop_requested_before_dispatch"] is not None:
                        result = self._normalize_predispatch_stop(result, stop_gate)
        except Exception as error:
            self._runtime_cache.pop(reference["schedule_id"], None)
            raise BackendError(f"bounded acquisition cycle failed: {error}") from error
        replay_delta = self._replay_delta(replay_session)
        queue = result.get("queue_summary")
        schedule, bundle, states = selected[1], selected[2], selected[3]
        if isinstance(queue, dict):
            if not self._replay_delta_returned_projection_is_current(
                replay_delta
            ):
                # The producer call already reached its one finite mutation
                # boundary.  A concurrent lane then advanced the shared exact
                # snapshot before this session committed, so its returned queue
                # summary is historical even though its commit generation is
                # current. Reload only the read-only merged runtime; never invoke
                # the producer a second time.
                (
                    replay_schedule,
                    replay_bundle,
                    replay_states,
                    replay_ready,
                ) = self._schedule_runtime(reference)
                if replay_schedule != schedule:
                    raise BackendError(
                        "merged acquisition runtime changed its sealed schedule"
                    )
            else:
                try:
                    replay_bundle, replay_states, replay_ready = (
                        self.modules.background._runtime_from_queue_summary(
                            schedule,
                            queue,
                            expected_mode="run",
                            expected_statuses={"bounded", "completed", "parked"},
                        )
                    )
                except Exception as error:
                    self._runtime_cache.pop(reference["schedule_id"], None)
                    raise BackendError(
                        f"bounded acquisition summary replay failed: {error}"
                    ) from error
            self._update_cached_runtime(
                reference, schedule, replay_bundle, replay_states, replay_ready
            )
            selected_ready_after = replay_ready
        else:
            self._update_cached_runtime(
                reference, schedule, bundle, states, result["ready_after"]
            )
            selected_ready_after = result["ready_after"]
        new_items = queue.get("new_item_count", 0) if isinstance(queue, dict) else 0
        new_failures = (
            queue.get("new_failed_attempt_count", 0) if isinstance(queue, dict) else 0
        )
        new_quarantined = (
            queue.get("new_quarantined_count", 0) if isinstance(queue, dict) else 0
        )
        updated_runtimes = [
            (
                row[0],
                row[1],
                row[2],
                replay_states if row[0]["schedule_id"] == reference["schedule_id"] else row[3],
                replay_ready if row[0]["schedule_id"] == reference["schedule_id"] else row[4],
            )
            for row in runtimes
        ] if isinstance(queue, dict) else runtimes
        completed_after, quarantined_after, pending_after = self._runtime_counts(
            updated_runtimes
        )
        normal_selected = reference["role"] == "normal_processing"
        if normal_selected:
            selected_before_items, selected_before_bytes, _selected_raw_before = (
                self._preprocess_ready_counts([selected])
            )
            selected_after_runtime = (
                selected[0],
                selected[1],
                selected[2],
                replay_states if isinstance(queue, dict) else selected[3],
                selected_ready_after,
            )
            selected_after_items, selected_after_bytes, selected_raw_after = (
                self._preprocess_ready_counts([selected_after_runtime])
            )
            campaign_ready_items = (
                global_ready_items - selected_before_items + selected_after_items
            )
            campaign_ready_bytes = (
                global_ready_bytes - selected_before_bytes + selected_after_bytes
            )
            raw_normal_ready_after = (
                raw_normal_ready_items
                - selected[4]["ready_item_count"]
                + selected_raw_after
            )
        else:
            campaign_ready_items = global_ready_items
            campaign_ready_bytes = global_ready_bytes
            raw_normal_ready_after = raw_normal_ready_items
        cold_ready_items_after = (
            cold_only_ready_items
            - (selected[4]["ready_item_count"] if not normal_selected else 0)
            + (selected_ready_after["ready_item_count"] if not normal_selected else 0)
        )
        cold_ready_bytes_after = (
            cold_only_ready_bytes
            - (selected[4]["ready_byte_count"] if not normal_selected else 0)
            + (selected_ready_after["ready_byte_count"] if not normal_selected else 0)
        )
        return StageOutcome(
            "acquisition",
            "progressed"
            if new_items or new_failures or new_quarantined
            else (
                "complete"
                if result.get("status") in {"completed", "parked"}
                else "held"
            ),
            bool(new_items or new_failures or new_quarantined),
            {
                "completed": completed_after,
                "pending": pending_after,
                "quarantined_items": quarantined_after,
                "ready_items": campaign_ready_items,
                "ready_bytes": campaign_ready_bytes,
                "raw_normal_ready_items": raw_normal_ready_after,
                "preprocess_parked_ready_items": (
                    raw_normal_ready_after - campaign_ready_items
                ),
                "cold_only_ready_items": cold_ready_items_after,
                "cold_only_ready_bytes": cold_ready_bytes_after,
                "new_items": new_items,
                "new_bytes": queue.get("new_byte_count", 0) if isinstance(queue, dict) else 0,
                "new_failed_attempts": new_failures,
                "new_quarantined_items": new_quarantined,
                "retryable_failed_items": (
                    queue.get("retryable_failed_count", 0)
                    if isinstance(queue, dict)
                    else 0
                ),
                "failed_attempt_count": (
                    queue.get("failed_attempt_count", 0)
                    if isinstance(queue, dict)
                    else 0
                ),
                "stop_reason": (
                    "durable_stop_requested_between_items"
                    if stop_gate["stop_requested_between_items"]
                    else result.get("stop_reason")
                ),
                "durable_stop_observed_between_items": stop_gate[
                    "stop_requested_between_items"
                ],
                "active_schedule_id": reference["schedule_id"],
                "active_schedule_role": reference["role"],
            },
            {
                "backend_kind": BACKEND_KIND,
                TRANSIENT_PEER_RUNTIME_ARTIFACT: {
                    "kind": "acquisition_queue_summary_v1",
                    "schedule_id": reference["schedule_id"],
                    "queue_summary": queue,
                    **(
                        {"operational_replay": replay_delta}
                        if replay_delta is not None
                        else {}
                    ),
                },
            },
        )

    def _parked_preprocess_ordinals(self, schedule_id: str) -> list[int]:
        return sorted(
            record["queue_ordinal"]
            for record in self._preprocess_parked.values()
            if record["schedule_id"] == schedule_id
        )

    def _runnable_preprocess_ready_rows(
        self,
        runtime: tuple[
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            list[dict[str, Any] | None],
            dict[str, Any],
        ],
    ) -> list[dict[str, Any]]:
        reference, _schedule, _bundle, _states, ready = runtime
        raw = ready.get("items", [])
        if not isinstance(raw, list):
            raise BackendError("preprocess ready snapshot items are malformed")
        parked = set(self._parked_preprocess_ordinals(reference["schedule_id"]))
        rows = [
            row
            for row in raw
            if isinstance(row, dict) and row.get("ordinal") not in parked
        ]
        if len(rows) != sum(
            isinstance(row, dict) and row.get("ordinal") not in parked for row in raw
        ):
            raise BackendError("preprocess ready snapshot contains a non-object row")
        return rows

    @contextmanager
    def _preprocess_item_stop_gate(self):
        """Install the durable Stop check on a production per-item handoff."""

        boundary = getattr(
            self.modules.handoff, "_materialize_and_run_one", None
        )
        if not callable(boundary):
            # Small backend unit fakes predate the private source boundary. The
            # imported production module must always expose it.
            if isinstance(self.modules, _Modules):
                raise BackendError(
                    "sealed preprocess handoff lacks its per-item Stop boundary"
                )
            yield None
            return
        try:
            with preprocess_stop_gate(
                self.modules.handoff,
                desired_state=lambda: read_control_state(self.config)[
                    "desired_state"
                ],
            ) as observed:
                yield observed
        except PreprocessStopError as error:
            raise BackendError(
                f"preprocess Stop boundary failed closed: {error}"
            ) from error

    def _preprocess_ready_counts(
        self,
        runtimes: Sequence[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                list[dict[str, Any] | None],
                dict[str, Any],
            ]
        ],
    ) -> tuple[int, int, int]:
        runnable_items = 0
        runnable_bytes = 0
        raw_items = 0
        for runtime in runtimes:
            ready = runtime[4]
            raw_count = ready.get("ready_item_count")
            if isinstance(raw_count, bool) or not isinstance(raw_count, int):
                raise BackendError("preprocess ready item count is malformed")
            raw_items += raw_count
            raw_rows = ready.get("items", [])
            if not isinstance(raw_rows, list) or len(raw_rows) != raw_count:
                raise BackendError("preprocess ready rows differ from their count")
            rows = self._runnable_preprocess_ready_rows(runtime)
            runnable_items += len(rows)
            for row in rows:
                size = row.get("media_byte_count")
                if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                    raise BackendError("preprocess ready byte count is malformed")
                runnable_bytes += size
        return runnable_items, runnable_bytes, raw_items

    def _register_completed_preprocess_rows(
        self,
        rows: Sequence[dict[str, Any]],
        *,
        reference: dict[str, Any],
        schedule: dict[str, Any],
        all_runtimes: Sequence[tuple[Any, Any, Any, Any, Any]],
    ) -> list[dict[str, Any]]:
        """Derive singleton peer artifacts from exact completed handoff rows."""

        runtime, campaign_offset = self._preprocess_runtime_position(
            all_runtimes, reference["schedule_id"]
        )
        if runtime[1] != schedule:
            raise BackendError(
                "completed preprocess rows differ from their active schedule"
            )
        state_root = Path(schedule["consumer"]["preprocess_state_root"])
        bundle_root = Path(self.config.section("preprocess")["bundle_root"])
        available_ordinals = {
            entry["queue_ordinal"]
            for entry, state in zip(
                runtime[2]["manifest"]["work_orders"],
                runtime[3],
                strict=True,
            )
            if self.modules.background._completed_state(state)
        }
        artifacts: list[dict[str, Any]] = []
        for row in rows:
            queue_ordinal = row.get("queue_ordinal") if isinstance(row, dict) else None
            receipt_count = (
                row.get("preprocess_receipt_count")
                if isinstance(row, dict)
                else None
            )
            bundle = row.get("bundle") if isinstance(row, dict) else None
            if (
                isinstance(queue_ordinal, bool)
                or not isinstance(queue_ordinal, int)
                or queue_ordinal not in available_ordinals
                or isinstance(receipt_count, bool)
                or not isinstance(receipt_count, int)
                or receipt_count != 1
                or not isinstance(bundle, dict)
            ):
                raise BackendError(
                    "completed preprocess row violates the singleton handoff contract"
                )
            bundle_id = bundle.get("bundle_id")
            raw_bundle_path = bundle.get("path")
            bundle_path = (
                Path(raw_bundle_path)
                if isinstance(raw_bundle_path, str)
                else Path("")
            )
            manifest_sha256 = bundle.get("manifest_sha256")
            if (
                not isinstance(bundle_id, str)
                or not bundle_id.startswith("ppbatch_")
                or len(bundle_id) != 40
                or not isinstance(raw_bundle_path, str)
                or bundle_path != bundle_root / "bundles" / bundle_id
                or not isinstance(manifest_sha256, str)
                or len(manifest_sha256) != 64
                or any(character not in "0123456789abcdef" for character in manifest_sha256)
                or not state_root.is_absolute()
            ):
                raise BackendError("completed preprocess bundle reference is malformed")
            artifacts.append(
                {
                    "queue_ordinal": queue_ordinal,
                    "bundle_path": str(bundle_path),
                    "bundle_id": bundle_id,
                    "bundle_manifest_sha256": manifest_sha256,
                    "campaign_ordinal": campaign_offset + queue_ordinal,
                    "preprocess_state_root": str(state_root),
                    # The sealed Archive handoff validates and runs exactly one
                    # work order/receipt per returned row. Never forward its scalar
                    # as independent authority.
                    "item_count": 1,
                }
            )
        ordinals = [row["queue_ordinal"] for row in artifacts]
        if ordinals != sorted(set(ordinals)):
            raise BackendError(
                "completed preprocess rows are not an ordered unique prefix"
            )
        for artifact in artifacts:
            self._register_preprocess_candidate(
                campaign_ordinal=artifact["campaign_ordinal"],
                bundle_path=Path(artifact["bundle_path"]),
                bundle_id=artifact["bundle_id"],
                state_root=state_root,
                item_count=1,
            )
        return artifacts

    def _run_preprocess(self, *, item_limit: int | None = None) -> StageOutcome:
        section = self.config.section("preprocess")
        if item_limit is None:
            effective_limit = section["max_items"]
        elif (
            isinstance(item_limit, bool)
            or not isinstance(item_limit, int)
            or not 1 <= item_limit <= section["max_items"]
        ):
            raise BackendError("preprocess lane item limit is invalid")
        else:
            effective_limit = item_limit
        all_runtimes = self._campaign_runtimes()
        runtimes = [
            row
            for row in all_runtimes
            if row[0]["role"] == "normal_processing"
        ]
        selected = next(
            (row for row in runtimes if self._runnable_preprocess_ready_rows(row)),
            None,
        )
        campaign_ready_before, _ready_bytes_before, raw_ready_before = (
            self._preprocess_ready_counts(runtimes)
        )
        failure_totals = self._preprocess_failure_totals()
        preprocessed_items_cumulative = self._preprocessed_items_cumulative()
        if selected is None:
            completed, quarantined, pending = self._runtime_counts(runtimes)
            return StageOutcome(
                "preprocess",
                "complete" if pending == 0 else "held",
                False,
                {
                    "processed_items": 0,
                    "preprocessed_items_cumulative": preprocessed_items_cumulative,
                    "ready_items_before": campaign_ready_before,
                    "ready_items_after": campaign_ready_before,
                    "raw_ready_items": raw_ready_before,
                    "completed_acquisitions": completed,
                    "quarantined_acquisitions": quarantined,
                    "failed_attempt_count": failure_totals[
                        "failed_attempt_count"
                    ],
                    "retryable_failed_items": failure_totals[
                        "retryable_failed_item_count"
                    ],
                    "parked_items": failure_totals["parked_item_count"],
                    "stop_reason": (
                        "all_runnable_preprocess_work_exhausted_with_parking"
                        if failure_totals["parked_item_count"]
                        else "no_completed_unacknowledged_ready_results"
                    ),
                },
                {"backend_kind": BACKEND_KIND},
            )
        reference = selected[0]
        parked_ordinals = self._parked_preprocess_ordinals(
            reference["schedule_id"]
        )
        runnable_before = self._runnable_preprocess_ready_rows(selected)
        selected_before = [
            row["ordinal"] for row in runnable_before[:effective_limit]
        ]
        replay_session = None
        stop_observation: PreprocessStopObservation | None = None
        preprocess_stop_observed = False
        try:
            with self._operational_queue_replay(
                reference, selected[1]
            ) as replay_session:
                with self._preprocess_item_stop_gate() as stop_observation:
                    result = self.modules.handoff.run_handoff(
                        Path(reference["path"]),
                        bundle_root=Path(section["bundle_root"]),
                        processing_output_root=Path(
                            section["processing_output_root"]
                        ),
                        # Freeze the bounded cardinality. The source's later
                        # exact replay remains identity authority because an
                        # acquisition retry may make a lower ordinal ready.
                        limit=len(selected_before),
                        parked_queue_ordinals=parked_ordinals,
                    )
        except PreprocessStopBoundary as error:
            # The source writer lock and replay TLS session have unwound. Re-read
            # the exact receipt state once; never retry a file that already
            # completed before the durable Stop boundary.
            replay_session = None
            preprocess_stop_observed = True
            self._runtime_cache.pop(reference["schedule_id"], None)
            if stop_observation is None:
                raise BackendError(
                    "preprocess Stop escaped without boundary evidence"
                ) from error
            completed = stop_observation.completed
            completed_ordinals = [row["queue_ordinal"] for row in completed]
            completed_count = len(completed_ordinals)
            attempted_ordinals = stop_observation.attempted_queue_ordinals
            boundary_ordinal = stop_observation.boundary_queue_ordinal
            actual_observed_prefix = [*completed_ordinals, boundary_ordinal]
            if (
                not stop_observation.stop_requested_between_items
                or completed_count >= len(selected_before)
                or attempted_ordinals != completed_ordinals
                or boundary_ordinal is None
                or any(
                    isinstance(ordinal, bool)
                    or not isinstance(ordinal, int)
                    or ordinal < 1
                    for ordinal in actual_observed_prefix
                )
                or actual_observed_prefix != sorted(set(actual_observed_prefix))
            ):
                raise BackendError(
                    "preprocess Stop boundary is not an exact source prefix"
                ) from error
            try:
                replayed = self._schedule_runtime(reference)
            except Exception as replay_error:
                raise BackendError(
                    f"preprocess Stop reconciliation replay failed: {replay_error}"
                ) from replay_error
            replay_schedule, replay_bundle, replay_states, replay_ready = replayed
            remaining_rows = replay_ready.get("items", [])
            if not isinstance(remaining_rows, list) or any(
                not isinstance(row, dict) for row in remaining_rows
            ):
                raise BackendError(
                    "preprocess Stop reconciliation returned malformed ready rows"
                )
            remaining = {row.get("ordinal") for row in remaining_rows}
            stale_unprocessed = [
                ordinal
                for ordinal in selected_before
                if ordinal not in completed_ordinals
            ]
            if (
                any(ordinal in remaining for ordinal in completed_ordinals)
                or boundary_ordinal not in remaining
                or any(ordinal not in remaining for ordinal in stale_unprocessed)
            ):
                raise BackendError(
                    "preprocess Stop reconciliation did not preserve an exact prefix"
                )
            selected = (
                reference,
                replay_schedule,
                replay_bundle,
                replay_states,
                replay_ready,
            )
            self._runtime_cache[reference["schedule_id"]] = replayed
            result = {
                "ready_after": replay_ready,
                "processed_items": completed,
                "stop_reason": "durable_stop_requested_between_preprocess_items",
            }
        except Exception as error:
            self._runtime_cache.pop(reference["schedule_id"], None)
            if not self._operational_preprocess_failure(error):
                self._invalidate_preprocess_candidates()
                raise BackendError(f"bounded preprocess handoff failed: {error}") from error
            try:
                replayed = self._schedule_runtime(reference)
            except Exception as replay_error:
                self._invalidate_preprocess_candidates()
                raise BackendError(
                    f"preprocess failure recovery replay failed: {replay_error}"
                ) from replay_error
            self._runtime_cache[reference["schedule_id"]] = replayed
            replay_schedule, replay_bundle, replay_states, replay_ready = replayed
            remaining = {
                row["ordinal"]
                for row in replay_ready.get("items", [])
                if isinstance(row, dict)
            }
            if stop_observation is not None:
                completed_rows = stop_observation.completed
                completed_ordinals = [
                    row["queue_ordinal"] for row in completed_rows
                ]
                attempted_ordinals = stop_observation.attempted_queue_ordinals
                if (
                    attempted_ordinals[:-1] != completed_ordinals
                    or len(attempted_ordinals) != len(completed_ordinals) + 1
                    or attempted_ordinals != sorted(set(attempted_ordinals))
                ):
                    raise BackendError(
                        "operational preprocess failure has no exact attempted item"
                    ) from error
                failed_ordinal = attempted_ordinals[-1]
                stale_unprocessed = [
                    ordinal
                    for ordinal in selected_before
                    if ordinal not in completed_ordinals
                ]
                if (
                    failed_ordinal not in remaining
                    or any(
                        ordinal in remaining for ordinal in completed_ordinals
                    )
                    or any(
                        ordinal not in remaining for ordinal in stale_unprocessed
                    )
                ):
                    raise BackendError(
                        "operational preprocess failure reconciliation changed its prefix"
                    ) from error
                resolved_predecessors = completed_ordinals
                exact_completed_rows: list[dict[str, Any]] | None = completed_rows
            else:
                candidates = [
                    ordinal for ordinal in selected_before if ordinal in remaining
                ]
                if not candidates:
                    raise BackendError(
                        "operational preprocess failure did not leave an exact failed item"
                    ) from error
                failed_ordinal = candidates[0]
                resolved_predecessors = selected_before[
                    : selected_before.index(failed_ordinal)
                ]
                exact_completed_rows = None
            record = self._preprocess_failure_record(
                reference=reference,
                bundle=replay_bundle,
                states=replay_states,
                queue_ordinal=failed_ordinal,
                error=error,
            )
            completed_by_key = {
                row["item_key"]: row for row in self._completed_acquisitions()
            }
            self._validate_preprocess_failure_record(record, completed_by_key)
            for ordinal in resolved_predecessors:
                self._preprocess_resolved_after_retry.add(
                    f"{reference['schedule_id']}:{ordinal}"
                )
            updated_runtimes = [
                (
                    row[0],
                    replay_schedule if row[0]["schedule_id"] == reference["schedule_id"] else row[1],
                    replay_bundle if row[0]["schedule_id"] == reference["schedule_id"] else row[2],
                    replay_states if row[0]["schedule_id"] == reference["schedule_id"] else row[3],
                    replay_ready if row[0]["schedule_id"] == reference["schedule_id"] else row[4],
                )
                for row in runtimes
            ]
            reconciled_all_runtimes = [
                (
                    row[0],
                    replay_schedule,
                    replay_bundle,
                    replay_states,
                    replay_ready,
                )
                if row[0]["schedule_id"] == reference["schedule_id"]
                else row
                for row in all_runtimes
            ]
            campaign_ready_after, _ready_bytes_after, raw_ready_after = (
                self._preprocess_ready_counts(updated_runtimes)
            )
            processed_count = len(resolved_predecessors)
            if exact_completed_rows is None:
                preprocess_artifacts: list[dict[str, Any]] = []
                rescan_candidates = bool(processed_count)
                if rescan_candidates:
                    self._invalidate_preprocess_candidates()
            else:
                preprocess_artifacts = self._register_completed_preprocess_rows(
                    exact_completed_rows,
                    reference=reference,
                    schedule=replay_schedule,
                    all_runtimes=reconciled_all_runtimes,
                )
                if [row["queue_ordinal"] for row in preprocess_artifacts] != (
                    resolved_predecessors
                ):
                    raise BackendError(
                        "operational preprocess completed artifacts differ from their prefix"
                    )
                rescan_candidates = False
            failure_totals = self._preprocess_failure_totals()
            preprocessed_items_cumulative = self._preprocessed_items_cumulative()
            return StageOutcome(
                "preprocess",
                "progressed",
                True,
                {
                    "processed_items": processed_count,
                    "preprocessed_items_cumulative": preprocessed_items_cumulative,
                    "ready_items_before": campaign_ready_before,
                    "ready_items_after": campaign_ready_after,
                    "raw_ready_items": raw_ready_after,
                    "new_failed_attempts": 1,
                    "new_parked_items": int(record["disposition"] == "parked"),
                    "failed_attempt_count": failure_totals[
                        "failed_attempt_count"
                    ],
                    "retryable_failed_items": failure_totals[
                        "retryable_failed_item_count"
                    ],
                    "parked_items": failure_totals["parked_item_count"],
                    "stop_reason": (
                        "item_parked_after_bounded_operational_failures"
                        if record["disposition"] == "parked"
                        else "item_operational_failure_will_retry"
                    ),
                    "active_schedule_id": reference["schedule_id"],
                    "failed_queue_ordinal": failed_ordinal,
                },
                {
                    "backend_kind": BACKEND_KIND,
                    "preprocess_failure_attempts": [record],
                    "preprocess_bundles": preprocess_artifacts,
                    "preprocess_candidates_rescan": rescan_candidates,
                    "resolved_predecessor_ordinals": resolved_predecessors,
                },
            )
        replay_delta = self._replay_delta(replay_session)
        # Validate the returned/committed projection relationship even though the
        # source result is not reused as cache authority below.
        self._replay_delta_returned_projection_is_current(replay_delta)
        processed = result.get("processed_items", [])
        # Handoff performs a final source replay after writing receipts. A peer
        # acquisition can become visible either during that replay or immediately
        # around its replay-store commit. The handoff result carries ``ready_after``
        # but not the matching complete state vector, so combining it with the
        # pre-call states would create a mixed-generation cache. Reload exactly the
        # touched schedule; the operational replay store makes this a witness-backed
        # metadata pass rather than a campaign-wide payload rehash.
        if preprocess_stop_observed:
            # The Stop reconciliation path already replayed the active schedule
            # after the source writer lock unwound.
            result_ready_after = selected[4]
        else:
            try:
                (
                    refreshed_schedule,
                    refreshed_bundle,
                    refreshed_states,
                    refreshed_ready,
                ) = self._schedule_runtime(reference)
            except Exception as error:
                self._runtime_cache.pop(reference["schedule_id"], None)
                raise BackendError(
                    f"post-handoff preprocess runtime replay failed: {error}"
                ) from error
            if refreshed_schedule != selected[1]:
                self._runtime_cache.pop(reference["schedule_id"], None)
                raise BackendError(
                    "post-handoff preprocess runtime changed its sealed schedule"
                )
            selected = (
                reference,
                refreshed_schedule,
                refreshed_bundle,
                refreshed_states,
                refreshed_ready,
            )
            result_ready_after = refreshed_ready
        active_runtime = (
            reference,
            selected[1],
            selected[2],
            selected[3],
            result_ready_after,
        )
        all_runtimes = [
            active_runtime
            if row[0]["schedule_id"] == reference["schedule_id"]
            else row
            for row in all_runtimes
        ]
        runtimes = [
            active_runtime
            if row[0]["schedule_id"] == reference["schedule_id"]
            else row
            for row in runtimes
        ]
        self._update_cached_runtime(
            reference,
            selected[1],
            selected[2],
            selected[3],
            result_ready_after,
        )
        for row in processed:
            self._preprocess_resolved_after_retry.add(
                f"{reference['schedule_id']}:{row['queue_ordinal']}"
            )
        failure_totals = self._preprocess_failure_totals()
        updated_runtimes = [
            (
                row[0],
                row[1],
                row[2],
                row[3],
                result_ready_after
                if row[0]["schedule_id"] == reference["schedule_id"]
                else row[4],
            )
            for row in runtimes
        ]
        campaign_ready_after, _ready_bytes_after, raw_ready_after = (
            self._preprocess_ready_counts(updated_runtimes)
        )
        artifacts = self._register_completed_preprocess_rows(
            processed,
            reference=reference,
            schedule=selected[1],
            all_runtimes=all_runtimes,
        )
        preprocessed_items_cumulative = self._preprocessed_items_cumulative()
        return StageOutcome(
            "preprocess",
            "progressed" if processed else "held",
            bool(processed),
            {
                "processed_items": len(processed),
                "preprocessed_items_cumulative": preprocessed_items_cumulative,
                "ready_items_before": campaign_ready_before,
                "ready_items_after": campaign_ready_after,
                "raw_ready_items": raw_ready_after,
                "failed_attempt_count": failure_totals["failed_attempt_count"],
                "retryable_failed_items": failure_totals[
                    "retryable_failed_item_count"
                ],
                "parked_items": failure_totals["parked_item_count"],
                "stop_reason": result.get("stop_reason"),
                "active_schedule_id": reference["schedule_id"],
                **(
                    {"durable_stop_observed_between_items": True}
                    if preprocess_stop_observed
                    else {}
                ),
            },
            {
                "backend_kind": BACKEND_KIND,
                "preprocess_bundles": artifacts,
                **(
                    {
                        TRANSIENT_PEER_RUNTIME_ARTIFACT: {
                            "kind": "preprocess_operational_replay_v1",
                            "schedule_id": reference["schedule_id"],
                            "operational_replay": replay_delta,
                        }
                    }
                    if replay_delta is not None
                    else {}
                ),
            },
        )

    def run_parallel_stages(self) -> tuple[StageOutcome, StageOutcome]:
        """Overlap one bounded acquisition call with one preprocess call.

        Queue acquisition must remain on the POSIX main thread because its sealed
        deadline guard uses process signals.  Preprocessing is therefore the sole
        worker lane.  Both finite calls are joined, then every campaign runtime is
        reloaded before a later GPU/cold stage can observe it.  Completion order
        never affects the returned acquisition/preprocess order.
        """

        # Populate immutable schedule/bundle snapshots before sharing reads across
        # the two lanes.  Each adapter still performs its own strict boundary replay.
        self._campaign_runtimes()
        cache_before = dict(self._runtime_cache)
        acquisition: StageOutcome | None = None
        preprocess: StageOutcome | None = None
        acquisition_error: Exception | None = None
        preprocess_error: Exception | None = None
        with ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="himr-preprocess-lane"
        ) as pool:
            future = pool.submit(self._run_preprocess)
            try:
                acquisition = self._run_acquisition()
            except Exception as error:
                acquisition_error = error
            try:
                preprocess = future.result()
            except Exception as error:
                preprocess_error = error

        # Both lanes may have updated the same ready snapshot.  Replay only the
        # exact 0..2 schedules that report a material active binding; untouched
        # epoch caches retain object identity and are never rehashed here.
        touched = {
            outcome.monitor.get("active_schedule_id")
            for outcome in (acquisition, preprocess)
            if outcome is not None
            and isinstance(outcome.monitor.get("active_schedule_id"), str)
        }
        touched.update(
            schedule_id
            for schedule_id, prior in cache_before.items()
            if self._runtime_cache.get(schedule_id) is not prior
        )
        references = {
            row["schedule_id"]: row
            for row in self.config.section("campaign")["schedules"]
        }
        if not touched <= set(references) or len(touched) > 2:
            raise BackendError(
                "overlapped stages changed an unsupported schedule set"
            )
        try:
            for schedule_id in sorted(touched):
                self._runtime_cache.pop(schedule_id, None)
                self._runtime_cache[schedule_id] = self._schedule_runtime(
                    references[schedule_id]
                )
        except Exception as error:
            for schedule_id in touched:
                self._runtime_cache.pop(schedule_id, None)
            raise BackendError(
                f"post-overlap touched-schedule replay failed: {error}"
            ) from error
        if acquisition_error is not None:
            suffix = (
                f"; preprocess lane also failed: {preprocess_error}"
                if preprocess_error is not None
                else ""
            )
            raise BackendError(
                f"overlapped acquisition lane failed: {acquisition_error}{suffix}"
            ) from acquisition_error
        if preprocess_error is not None:
            raise BackendError(
                f"overlapped preprocess lane failed: {preprocess_error}"
            ) from preprocess_error
        if acquisition is None or preprocess is None:
            raise BackendError("overlapped finite stages returned no outcome")
        self._record_own_outcome_queue_authority(acquisition)
        self._record_own_outcome_queue_authority(preprocess)
        return acquisition, preprocess

    def _checkpoint_small_json(
        self, path: Path, *, label: str
    ) -> tuple[dict[str, Any], bytes]:
        try:
            body, _ = self.modules.queue_runner._stable_read(
                path,
                maximum=BACKEND_CHECKPOINT_SMALL_FILE_BYTES,
                label=label,
            )
            value = json.loads(body)
        except Exception as error:
            raise BackendError(f"cannot read {label}: {error}") from error
        if not isinstance(value, dict):
            raise BackendError(f"{label} must contain one JSON object")
        return value, body

    def _checkpoint_file_entry(
        self,
        path: Path,
        *,
        label: str,
        expected_sha256: str | None = None,
        read_policy: str,
    ) -> dict[str, Any]:
        if read_policy not in CHECKPOINT_READ_POLICIES:
            raise BackendError(f"{label} restart witness read policy is invalid")
        fingerprint = _checkpoint_fingerprint(path, label=label)
        if read_policy == CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON:
            try:
                body, _ = self.modules.queue_runner._stable_read(
                    path,
                    maximum=BACKEND_CHECKPOINT_SMALL_FILE_BYTES,
                    label=label,
                )
            except Exception as error:
                raise BackendError(f"cannot bind {label}: {error}") from error
        elif read_policy == CHECKPOINT_READ_PREPROCESS_JSON_HARDLINKS:
            body = self._checkpoint_preprocess_artifact_read(path, label=label)
        else:
            body = None
        if body is not None:
            observed_sha256 = sha256_bytes(body)
            if expected_sha256 is None:
                expected_sha256 = observed_sha256
            elif observed_sha256 != expected_sha256:
                raise BackendError(f"{label} differs from its expected SHA-256")
        elif expected_sha256 is None:
            raise BackendError(
                f"{label} restart witness lacks a content identity"
            )
        if (
            not isinstance(expected_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
        ):
            raise BackendError(f"{label} restart witness SHA-256 is invalid")
        return {
            "fingerprint": fingerprint,
            "sha256": expected_sha256,
            "read_policy": read_policy,
        }

    def _checkpoint_preprocess_artifact_read(
        self, path: Path, *, label: str
    ) -> bytes:
        """Read one producer-sealed JSON artifact under its exact link policy.

        Verified preprocess reuse deliberately hard-links immutable artifacts
        into later run directories.  Acquisition control files have a different
        single-link contract, so this exception is both path-confined to the
        configured private processing root and restricted to receipt-declared
        JSON artifacts by its caller.
        """

        batch = self.modules.preprocess_batch
        try:
            configured_root = Path(
                self.config.section("preprocess")["processing_output_root"]
            )
            # Configuration admission already applies the producer's placement
            # policy.  Re-check the live root's exact owner-private identity here
            # without reinterpreting its parent-directory policy (test and
            # operator roots may legitimately live on different mount layouts).
            root = batch.require_private_directory(
                configured_root,
                "checkpoint preprocess processing output root",
            )
            requested = Path(os.path.abspath(os.fspath(path)))
            resolved = batch.absolute_path(
                requested,
                f"{label} path",
                must_exist=True,
            )
            if requested != resolved:
                raise ValueError("path may not traverse a symlink")
            batch.require_descendant(resolved, root, label)
            batch.validate_private_ancestor_chain(
                resolved.parent,
                root,
                f"{label} directory chain",
            )
            _resolved, body, observed = batch.readonly_file(
                resolved,
                BACKEND_CHECKPOINT_SMALL_FILE_BYTES,
                label,
                allow_hardlinks=True,
            )
            if observed.st_uid != os.getuid():
                raise ValueError("artifact is not owned by the pipeline user")
        except Exception as error:
            raise BackendError(f"cannot bind {label}: {error}") from error
        return body

    def _preprocess_candidate_restart_witness(
        self,
        *,
        bundle_path: Path,
        bundle_id: str,
        state_root: Path,
        item_count: int,
    ) -> list[dict[str, Any]]:
        """Capture cheap immutable leaves for one already-admitted bundle."""

        manifest_path = bundle_path / "manifest.json"
        manifest, manifest_body = self._checkpoint_small_json(
            manifest_path, label=f"preprocess bundle {bundle_id} manifest"
        )
        if (
            manifest.get("bundle_id") != bundle_id
            or manifest.get("work_order_count") != item_count
            or not isinstance(manifest.get("selection"), dict)
            or not isinstance(manifest.get("work_orders"), list)
            or len(manifest["work_orders"]) != item_count
        ):
            raise BackendError(
                f"preprocess bundle {bundle_id} differs from checkpoint candidate"
            )
        entries: list[dict[str, Any]] = [
            self._checkpoint_file_entry(
                manifest_path,
                label=f"preprocess bundle {bundle_id} manifest",
                expected_sha256=sha256_bytes(manifest_body),
                read_policy=CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON,
            )
        ]
        selection = manifest["selection"]
        selection_path = Path(selection.get("path", ""))
        entries.append(
            self._checkpoint_file_entry(
                selection_path,
                label=f"preprocess bundle {bundle_id} selection",
                expected_sha256=selection.get("sha256"),
                read_policy=CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON,
            )
        )
        for ordinal, descriptor in enumerate(manifest["work_orders"], 1):
            if not isinstance(descriptor, dict):
                raise BackendError(
                    f"preprocess bundle {bundle_id} work-order descriptor is malformed"
                )
            relative = descriptor.get("path")
            if not isinstance(relative, str) or not relative:
                raise BackendError(
                    f"preprocess bundle {bundle_id} work-order path is malformed"
                )
            entries.append(
                self._checkpoint_file_entry(
                    bundle_path / relative,
                    label=f"preprocess bundle {bundle_id} work order {ordinal}",
                    expected_sha256=descriptor.get("sha256"),
                    read_policy=CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON,
                )
            )
        receipts_root = state_root / "runs" / bundle_id / "receipts"
        for ordinal in range(1, item_count + 1):
            receipt_path = receipts_root / f"{ordinal:06d}.json"
            receipt, receipt_body = self._checkpoint_small_json(
                receipt_path,
                label=f"preprocess bundle {bundle_id} receipt {ordinal}",
            )
            if (
                receipt.get("bundle_id") != bundle_id
                or receipt.get("ordinal") != ordinal
                or not isinstance(receipt.get("preprocess_result"), dict)
                or not isinstance(receipt.get("artifacts"), list)
            ):
                raise BackendError(
                    f"preprocess bundle {bundle_id} receipt {ordinal} is malformed"
                )
            try:
                self.modules.background._validate_receipt_digest(receipt)
                if receipt_body != self.modules.background.pretty_bytes(receipt):
                    raise ValueError("non-canonical receipt")
            except Exception as error:
                raise BackendError(
                    f"preprocess bundle {bundle_id} receipt {ordinal} "
                    f"failed envelope replay: {error}"
                ) from error
            entries.append(
                self._checkpoint_file_entry(
                    receipt_path,
                    label=f"preprocess bundle {bundle_id} receipt {ordinal}",
                    expected_sha256=sha256_bytes(receipt_body),
                    read_policy=CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON,
                )
            )
            result = receipt["preprocess_result"]
            entries.append(
                self._checkpoint_file_entry(
                    Path(result.get("path", "")),
                    label=f"preprocess bundle {bundle_id} result {ordinal}",
                    expected_sha256=result.get("sha256"),
                    read_policy=CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON,
                )
            )
            for artifact_index, artifact in enumerate(receipt["artifacts"], 1):
                if not isinstance(artifact, dict):
                    raise BackendError(
                        f"preprocess bundle {bundle_id} artifact descriptor is malformed"
                    )
                mime_type = artifact.get("mime_type")
                entries.append(
                    self._checkpoint_file_entry(
                        Path(artifact.get("path", "")),
                        label=(
                            f"preprocess bundle {bundle_id} artifact "
                            f"{ordinal}.{artifact_index}"
                        ),
                        expected_sha256=artifact.get("sha256"),
                        read_policy=(
                            CHECKPOINT_READ_PREPROCESS_JSON_HARDLINKS
                            if mime_type == "application/json"
                            else CHECKPOINT_READ_FINGERPRINT_ONLY
                        ),
                    )
                )
        if len(entries) > BACKEND_CHECKPOINT_MAX_FILES_PER_CANDIDATE:
            raise BackendError(
                f"preprocess bundle {bundle_id} exceeds restart witness file cap"
            )
        paths = [row["fingerprint"]["path"] for row in entries]
        if len(paths) != len(set(paths)):
            raise BackendError(
                f"preprocess bundle {bundle_id} repeats a restart witness path"
            )
        return entries

    def _preprocess_restart_witness_matches(
        self,
        rows: Any,
        *,
        bundle_id: str,
    ) -> bool:
        if (
            not isinstance(rows, list)
            or not rows
            or len(rows) > BACKEND_CHECKPOINT_MAX_FILES_PER_CANDIDATE
        ):
            raise BackendError(
                f"preprocess bundle {bundle_id} restart witnesses are malformed"
            )
        seen: set[str] = set()
        for index, row in enumerate(rows, 1):
            if not isinstance(row, dict) or set(row) != {
                "fingerprint",
                "sha256",
                "read_policy",
            }:
                raise BackendError(
                    f"preprocess bundle {bundle_id} restart witness {index} is malformed"
                )
            fingerprint = _validate_checkpoint_fingerprint(
                row["fingerprint"],
                label=f"preprocess bundle {bundle_id} file {index}",
            )
            path = fingerprint["path"]
            if (
                path in seen
                or not isinstance(row["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", row["sha256"]) is None
                or row["read_policy"] not in CHECKPOINT_READ_POLICIES
            ):
                raise BackendError(
                    f"preprocess bundle {bundle_id} restart witness {index} is invalid"
                )
            seen.add(path)
            if not _checkpoint_fingerprint_matches(
                fingerprint,
                label=f"preprocess bundle {bundle_id} file {index}",
            ):
                return False
            if row["read_policy"] == CHECKPOINT_READ_OWNER_SINGLE_LINK_JSON:
                try:
                    body, _ = self.modules.queue_runner._stable_read(
                        Path(path),
                        maximum=BACKEND_CHECKPOINT_SMALL_FILE_BYTES,
                        label=f"preprocess bundle {bundle_id} file {index}",
                    )
                except Exception:
                    return False
                if sha256_bytes(body) != row["sha256"]:
                    return False
            elif row["read_policy"] == CHECKPOINT_READ_PREPROCESS_JSON_HARDLINKS:
                try:
                    body = self._checkpoint_preprocess_artifact_read(
                        Path(path),
                        label=f"preprocess bundle {bundle_id} file {index}",
                    )
                except BackendError:
                    return False
                if sha256_bytes(body) != row["sha256"]:
                    return False
        return True

    def _preprocess_campaign_positions(
        self,
    ) -> tuple[
        dict[str, int],
        list[tuple[Path, set[str]]],
    ]:
        completed_ordinals: dict[str, int] = {}
        schedule_states: list[tuple[Path, set[str]]] = []
        campaign_position = 0
        for reference, schedule, bundle, states, _ready in self._campaign_runtimes():
            if reference["role"] != "normal_processing":
                campaign_position += len(states)
                continue
            state_root = Path(schedule["consumer"]["preprocess_state_root"])
            schedule_paths: set[str] = set()
            for entry, order, state in zip(
                bundle["manifest"]["work_orders"],
                bundle["orders"],
                states,
                strict=True,
            ):
                if self.modules.background._completed_state(state):
                    path = str(self.modules.queue_runner._result_path(order))
                    completed_ordinals[path] = (
                        campaign_position + entry["queue_ordinal"]
                    )
                    schedule_paths.add(path)
            campaign_position += len(states)
            schedule_states.append((state_root, schedule_paths))
        return completed_ordinals, schedule_states

    def _validate_incremental_preprocess_candidate(
        self,
        *,
        bundle_path: Path,
        bundle_id: str,
        state_root: Path,
        schedule_paths: set[str],
        completed_ordinals: dict[str, int],
        metadata_bootstrap: bool,
    ) -> tuple[int, int, list[dict[str, Any]]]:
        """Validate one new/changed bundle, deeply unless this is migration."""

        if metadata_bootstrap:
            manifest, _body = self._checkpoint_small_json(
                bundle_path / "manifest.json",
                label=f"preprocess bootstrap bundle {bundle_id}",
            )
            if (
                manifest.get("bundle_id") != bundle_id
                or not isinstance(manifest.get("selection"), dict)
                or not isinstance(manifest.get("work_order_count"), int)
                or isinstance(manifest.get("work_order_count"), bool)
                or manifest["work_order_count"] < 1
            ):
                raise BackendError(
                    f"preprocess bootstrap bundle {bundle_id} is malformed"
                )
            selection, _selection_body = self._checkpoint_small_json(
                Path(manifest["selection"].get("path", "")),
                label=f"preprocess bootstrap selection {bundle_id}",
            )
            entries = selection.get("entries")
            item_count = manifest["work_order_count"]
            if not isinstance(entries, list) or len(entries) != item_count:
                raise BackendError(
                    f"preprocess bootstrap selection {bundle_id} is malformed"
                )
            acquisition_paths = [
                row.get("acquisition_result", {}).get("path")
                if isinstance(row, dict)
                and isinstance(row.get("acquisition_result"), dict)
                else None
                for row in entries
            ]
        else:
            try:
                manifest, selection, orders = (
                    self.modules.preprocess_batch.validate_bundle(bundle_path)
                )
                receipts = self.modules.preprocess_batch.existing_receipts(
                    state_root,
                    manifest=manifest,
                    selection=selection,
                    orders=orders,
                )
            except Exception as error:
                raise BackendError(
                    f"preprocess targeted replay failed for {bundle_id}: {error}"
                ) from error
            item_count = manifest["work_order_count"]
            if len(orders) != item_count or len(receipts) != item_count:
                raise BackendError(
                    f"preprocess targeted bundle {bundle_id} is incomplete"
                )
            acquisition_paths = [
                row["acquisition_result"]["path"]
                for row in selection["entries"]
            ]
        if (
            not acquisition_paths
            or any(
                not isinstance(path, str)
                or path not in schedule_paths
                or path not in completed_ordinals
                for path in acquisition_paths
            )
        ):
            raise BackendError(
                f"preprocess bundle {bundle_id} is outside completed acquisition authority"
            )
        witness = self._preprocess_candidate_restart_witness(
            bundle_path=bundle_path,
            bundle_id=bundle_id,
            state_root=state_root,
            item_count=item_count,
        )
        return (
            min(completed_ordinals[path] for path in acquisition_paths),
            item_count,
            witness,
        )

    def _restore_preprocess_checkpoint(
        self,
        rows: Any,
        *,
        metadata_bootstrap: bool,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> dict[str, int]:
        """Hydrate known bundles and deeply inspect only new/changed receipts."""

        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise BackendError("preprocess checkpoint candidate vector is malformed")
        if self._preprocess_candidates_cache not in (None, []):
            raise BackendError("preprocess checkpoint restore requires an empty index")
        prior_witnesses = deepcopy(self._preprocess_restart_witnesses)
        self._preprocess_candidates_cache = []
        raw_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        expected_keys = {
            "campaign_ordinal",
            "bundle_path",
            "bundle_id",
            "state_root",
            "item_count",
            "files",
        }
        for row in rows:
            if not isinstance(row, dict) or set(row) != expected_keys:
                raise BackendError("preprocess checkpoint candidate is malformed")
            raw_state_root = row["state_root"]
            bundle_id = row["bundle_id"]
            if (
                not isinstance(raw_state_root, str)
                or not Path(raw_state_root).is_absolute()
                or not isinstance(bundle_id, str)
                or not bundle_id
            ):
                raise BackendError("preprocess checkpoint candidate identity is invalid")
            key = (raw_state_root, bundle_id)
            if key in raw_by_key:
                raise BackendError("preprocess checkpoint repeats a candidate")
            raw_by_key[key] = row
            if cancellation_boundary is not None:
                cancellation_boundary()

        completed_ordinals, schedule_states = self._preprocess_campaign_positions()
        bundle_root = Path(self.config.section("preprocess")["bundle_root"])
        observed: list[tuple[Path, str, set[str]]] = []
        seen: set[tuple[str, str]] = set()
        for state_root, schedule_paths in schedule_states:
            receipt_paths = self.modules.background._preprocess_receipt_paths(
                state_root
            )
            receipts_by_bundle: dict[str, list[Path]] = {}
            for receipt_path in receipt_paths:
                receipts_by_bundle.setdefault(
                    receipt_path.parents[1].name, []
                ).append(receipt_path)
                if cancellation_boundary is not None:
                    cancellation_boundary()
            for bundle_id in sorted(receipts_by_bundle):
                key = (str(state_root), bundle_id)
                if key in seen:
                    continue
                manifest, _manifest_body = self._checkpoint_small_json(
                    bundle_root / "bundles" / bundle_id / "manifest.json",
                    label=f"preprocess bundle {bundle_id} manifest",
                )
                item_count = manifest.get("work_order_count")
                if (
                    isinstance(item_count, bool)
                    or not isinstance(item_count, int)
                    or item_count < 1
                ):
                    raise BackendError(
                        f"preprocess bundle {bundle_id} item count is malformed"
                    )
                if len(receipts_by_bundle[bundle_id]) < item_count:
                    # A producer may stop after an exact item boundary inside a
                    # multi-item bundle. Acknowledgements remain authoritative,
                    # but GPU candidacy waits for the complete bundle just as the
                    # legacy deep discovery path does.
                    continue
                if len(receipts_by_bundle[bundle_id]) != item_count:
                    raise BackendError(
                        f"preprocess bundle {bundle_id} has excess receipts"
                    )
                seen.add(key)
                observed.append((state_root, bundle_id, schedule_paths))
                if cancellation_boundary is not None:
                    cancellation_boundary()
        if set(raw_by_key) - seen:
            raise BackendError(
                "preprocess checkpoint names a bundle without a durable receipt"
            )

        fast = 0
        targeted = 0
        bootstrapped = 0
        for state_root, bundle_id, schedule_paths in observed:
            key = (str(state_root), bundle_id)
            bundle_path = bundle_root / "bundles" / bundle_id
            checkpoint = raw_by_key.get(key)
            if checkpoint is not None:
                campaign_ordinal = checkpoint["campaign_ordinal"]
                item_count = checkpoint["item_count"]
                if (
                    isinstance(campaign_ordinal, bool)
                    or not isinstance(campaign_ordinal, int)
                    or campaign_ordinal < 1
                    or isinstance(item_count, bool)
                    or not isinstance(item_count, int)
                    or item_count < 1
                    or checkpoint["bundle_path"] != str(bundle_path)
                ):
                    raise BackendError(
                        f"preprocess checkpoint candidate {bundle_id} is invalid"
                    )
                if self._preprocess_restart_witness_matches(
                    checkpoint["files"], bundle_id=bundle_id
                ):
                    witness = deepcopy(checkpoint["files"])
                    fast += 1
                else:
                    campaign_ordinal, item_count, witness = (
                        self._validate_incremental_preprocess_candidate(
                            bundle_path=bundle_path,
                            bundle_id=bundle_id,
                            state_root=state_root,
                            schedule_paths=schedule_paths,
                            completed_ordinals=completed_ordinals,
                            metadata_bootstrap=False,
                        )
                    )
                    targeted += 1
            else:
                prior = prior_witnesses.get(key)
                if (
                    prior is not None
                    and self._preprocess_restart_witness_matches(
                        prior, bundle_id=bundle_id
                    )
                ):
                    campaign_ordinal, item_count, _observed_witness = (
                        self._validate_incremental_preprocess_candidate(
                            bundle_path=bundle_path,
                            bundle_id=bundle_id,
                            state_root=state_root,
                            schedule_paths=schedule_paths,
                            completed_ordinals=completed_ordinals,
                            metadata_bootstrap=True,
                        )
                    )
                    witness = prior
                    fast += 1
                else:
                    campaign_ordinal, item_count, witness = (
                        self._validate_incremental_preprocess_candidate(
                            bundle_path=bundle_path,
                            bundle_id=bundle_id,
                            state_root=state_root,
                            schedule_paths=schedule_paths,
                            completed_ordinals=completed_ordinals,
                            metadata_bootstrap=(
                                metadata_bootstrap and prior is None
                            ),
                        )
                    )
                    if metadata_bootstrap and prior is None:
                        bootstrapped += 1
                    else:
                        targeted += 1
            self._register_preprocess_candidate(
                campaign_ordinal=campaign_ordinal,
                bundle_path=bundle_path,
                bundle_id=bundle_id,
                state_root=state_root,
                item_count=item_count,
            )
            self._preprocess_restart_witnesses[key] = witness
            if cancellation_boundary is not None:
                cancellation_boundary()
        self._preprocess_candidates_cache.sort()
        return {
            "fast_reused_bundles": fast,
            "targeted_revalidated_bundles": targeted,
            "metadata_bootstrap_bundles": bootstrapped,
        }

    def _preprocessed_items_cumulative(self) -> int:
        """Count durable completed preprocess receipts across validated bundles."""

        production = isinstance(self.modules, _Modules)
        if production and self._preprocess_candidates_cache is None:
            self._preprocess_bundle_candidates()
        if (
            isinstance(self._preprocessed_item_total, bool)
            or not isinstance(self._preprocessed_item_total, int)
            or self._preprocessed_item_total < 0
        ):
            raise BackendError("preprocess cumulative item total is malformed")
        if production:
            if self._preprocess_candidates_cache is None or not (
                len(self._preprocess_candidates_cache)
                == len(self._preprocess_candidate_keys)
                == len(self._preprocess_bundle_item_counts)
            ):
                raise BackendError(
                    "preprocess completed-bundle indexes differ"
                )
        else:
            # Small test doubles do not implement receipt discovery. Retain a
            # fail-closed ledger assertion there; production cached reads above
            # remain O(1) regardless of completed-bundle count.
            counts = self._preprocess_bundle_item_counts.values()
            if any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 1
                for value in counts
            ) or sum(counts) != self._preprocessed_item_total:
                raise BackendError("preprocess bundle item count ledger is malformed")
        return self._preprocessed_item_total

    def _preprocess_bundle_candidates(self) -> list[tuple[int, Path, str, Path]]:
        if self._preprocess_candidates_cache is not None:
            if not (
                len(self._preprocess_candidates_cache)
                == len(self._preprocess_candidate_keys)
                == len(self._preprocess_bundle_item_counts)
            ):
                raise BackendError("preprocess completed-bundle indexes differ")
            return list(self._preprocess_candidates_cache)
        completed_ordinals: dict[str, int] = {}
        schedule_states: list[tuple[Path, set[str]]] = []
        campaign_position = 0
        for _reference, schedule, acquisition_bundle, states, _ready in self._campaign_runtimes():
            if _reference["role"] != "normal_processing":
                campaign_position += len(states)
                continue
            state_root = Path(schedule["consumer"]["preprocess_state_root"])
            schedule_paths: set[str] = set()
            for entry, order, state in zip(
                acquisition_bundle["manifest"]["work_orders"],
                acquisition_bundle["orders"],
                states,
                strict=True,
            ):
                if self.modules.background._completed_state(state):
                    path = str(self.modules.queue_runner._result_path(order))
                    completed_ordinals[path] = campaign_position + entry["queue_ordinal"]
                    schedule_paths.add(path)
            campaign_position += len(states)
            schedule_states.append((state_root, schedule_paths))
        candidates: list[tuple[int, Path, str, Path]] = []
        bundle_root = Path(self.config.section("preprocess")["bundle_root"])
        seen: set[tuple[str, str]] = set()
        item_counts: dict[tuple[str, str], int] = {}
        for state_root, schedule_paths in schedule_states:
            bundle_ids = {
                path.parents[1].name
                for path in self.modules.background._preprocess_receipt_paths(state_root)
            }
            for bundle_id in sorted(bundle_ids):
                identity = (str(state_root), bundle_id)
                if identity in seen:
                    continue
                seen.add(identity)
                bundle_path = bundle_root / "bundles" / bundle_id
                try:
                    manifest, selection, orders = self.modules.preprocess_batch.validate_bundle(bundle_path)
                    receipts = self.modules.preprocess_batch.existing_receipts(
                        state_root,
                        manifest=manifest,
                        selection=selection,
                        orders=orders,
                    )
                except Exception as error:
                    raise BackendError(f"preprocess bundle replay failed for {bundle_id}: {error}") from error
                item_count = manifest["work_order_count"]
                if (
                    isinstance(item_count, bool)
                    or not isinstance(item_count, int)
                    or item_count < 1
                    or len(orders) != item_count
                ):
                    raise BackendError(
                        "preprocess bundle work-order count is malformed"
                    )
                if len(receipts) != item_count:
                    continue
                acquisition_paths = [
                    entry["acquisition_result"]["path"] for entry in selection["entries"]
                ]
                if any(path not in schedule_paths for path in acquisition_paths):
                    continue
                first = min(completed_ordinals[path] for path in acquisition_paths)
                candidates.append((first, bundle_path, bundle_id, state_root))
                item_counts[identity] = item_count
        self._preprocess_candidates_cache = sorted(candidates)
        self._preprocess_candidate_keys = set(item_counts)
        self._preprocess_bundle_item_counts = item_counts
        self._preprocessed_item_total = sum(item_counts.values())
        if not (
            len(self._preprocess_candidates_cache)
            == len(self._preprocess_candidate_keys)
            == len(self._preprocess_bundle_item_counts)
        ):
            raise BackendError("preprocess completed-bundle replay is inconsistent")
        return list(self._preprocess_candidates_cache)

    def _cache_gpu_queue_manifest(
        self,
        *,
        manifest: dict[str, Any],
        manifest_path: Path,
        expected_bundle_id: str,
        expected_reference: dict[str, Any],
        expected_disposition: dict[str, int],
        expected_candidate: tuple[Path, Path] | None = None,
    ) -> tuple[dict[str, Any], Path]:
        """Bind one admitted immutable queue to its exact preprocess source."""

        if (
            not isinstance(manifest, dict)
            or not isinstance(manifest_path, Path)
            or not isinstance(expected_bundle_id, str)
            or not expected_bundle_id
            or not isinstance(expected_reference, dict)
            or set(expected_reference) != {"path", "sha256", "queue_id"}
            or not isinstance(expected_disposition, dict)
        ):
            raise BackendError("admitted GPU queue binding is malformed")
        queue_id = manifest.get("queue_id")
        if (
            not isinstance(queue_id, str)
            or not queue_id
            or expected_reference["queue_id"] != queue_id
            or expected_reference["path"] != str(manifest_path)
            or expected_reference["sha256"]
            != sha256_bytes(self.modules.gpu_queue.canonical_bytes(manifest))
        ):
            raise BackendError("admitted GPU queue reference differs from manifest")
        try:
            origin = manifest["origin"]
            bundle = origin["preprocess_bundle"]
            bundle_path = bundle["path"]
            bundle_id = bundle["bundle_id"]
            state_root = origin["state_root"]
            queue_root = manifest["output"]["queue_root"]
            profile_reference = manifest["production_profile"]["reference"]
            disposition = self._gpu_queue_disposition(manifest)
        except (KeyError, TypeError) as error:
            raise BackendError("admitted GPU queue lineage is malformed") from error
        gpu = self.config.section("gpu_readiness")
        if (
            not all(
                isinstance(value, str) and value and Path(value).is_absolute()
                for value in (bundle_path, state_root, queue_root)
            )
            or bundle_id != expected_bundle_id
            or disposition != expected_disposition
            or queue_root != gpu["queue_root"]
            or not isinstance(profile_reference, dict)
            or profile_reference.get("path") != gpu["production_profile"]
            or profile_reference.get("physical_sha256")
            != gpu["production_profile_sha256"]
        ):
            raise BackendError("admitted GPU queue authority differs from configuration")
        key = (bundle_path, state_root)
        if expected_candidate is not None:
            expected_bundle_path, expected_state_root = expected_candidate
            if key != (str(expected_bundle_path), str(expected_state_root)):
                raise BackendError("admitted GPU queue differs from its candidate")
        previous_key = self._gpu_queue_ids.get(queue_id)
        if previous_key is not None and previous_key != key:
            raise BackendError("GPU queue ID is bound to multiple preprocess candidates")
        previous = self._gpu_queue_cache.get(key)
        candidate = (manifest, manifest_path)
        if previous is not None:
            previous_manifest, previous_path = previous
            if (
                previous_path != manifest_path
                or previous_manifest != manifest
                or previous_manifest.get("queue_id") != queue_id
            ):
                raise BackendError("preprocess candidate is bound to conflicting GPU queues")
            self._gpu_queue_ids[queue_id] = key
            return previous
        self._gpu_queue_cache[key] = candidate
        self._gpu_queue_ids[queue_id] = key
        return candidate

    @staticmethod
    def _gpu_record_queue_references(
        record: dict[str, Any],
    ) -> list[tuple[str, dict[str, Any], dict[str, int]]]:
        if not isinstance(record, dict):
            raise BackendError("GPU queue record is malformed")
        if record.get("record_format") == GPU_PACK_RECORD_FORMAT:
            sources = record.get("sources")
            if not isinstance(sources, list):
                raise BackendError("packed GPU record sources are malformed")
            rows = sources
        else:
            rows = [record]
        references: list[tuple[str, dict[str, Any], dict[str, int]]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise BackendError("GPU queue record source is malformed")
            bundle_id = row.get("preprocess_bundle_id")
            queue = row.get("queue")
            disposition = row.get("queue_disposition")
            if (
                not isinstance(bundle_id, str)
                or not bundle_id
                or not isinstance(queue, dict)
                or set(queue) != {"path", "sha256", "queue_id"}
                or not isinstance(disposition, dict)
            ):
                raise BackendError("GPU queue record authority is malformed")
            references.append((bundle_id, queue, disposition))
        return references

    def _hydrate_admitted_gpu_queues(
        self,
        *,
        cancellation_boundary: Callable[[], None] | None = None,
    ) -> int:
        """Restore queue cache from SHA-witnessed records without media replay."""

        gpu = self.config.section("gpu_readiness")
        loaded = 0
        for record in self._gpu_records.values():
            for bundle_id, reference, disposition in self._gpu_record_queue_references(
                record
            ):
                queue_id = reference.get("queue_id")
                if isinstance(queue_id, str) and queue_id in self._gpu_queue_ids:
                    key = self._gpu_queue_ids[queue_id]
                    manifest, manifest_path = self._gpu_queue_cache[key]
                else:
                    try:
                        manifest = self.modules.gpu_queue.load_admitted_queue(
                            manifest_path=Path(reference.get("path", "")),
                            expected_manifest_sha256=reference.get("sha256", ""),
                            root_registration_path=Path(gpu["root_registration"]),
                            root_registration_sha256=gpu[
                                "root_registration_sha256"
                            ],
                        )
                    except Exception as error:
                        raise BackendError(
                            f"admitted GPU queue restore failed: {error}"
                        ) from error
                    manifest_path = Path(reference["path"])
                    loaded += 1
                self._cache_gpu_queue_manifest(
                    manifest=manifest,
                    manifest_path=manifest_path,
                    expected_bundle_id=bundle_id,
                    expected_reference=reference,
                    expected_disposition=disposition,
                )
                if cancellation_boundary is not None:
                    cancellation_boundary()

        candidate_paths = {
            (str(state_root), bundle_id): str(bundle_path)
            for _ordinal, bundle_path, bundle_id, state_root in (
                self._preprocess_bundle_candidates()
            )
        }
        for queue_id, key in self._gpu_queue_ids.items():
            manifest, _manifest_path = self._gpu_queue_cache[key]
            try:
                bundle_id = manifest["origin"]["preprocess_bundle"]["bundle_id"]
            except (KeyError, TypeError) as error:
                raise BackendError(
                    f"admitted GPU queue {queue_id} lineage is malformed"
                ) from error
            candidate_path = candidate_paths.get((key[1], bundle_id))
            if candidate_path != key[0]:
                raise BackendError(
                    "admitted GPU queue is outside restored preprocess candidates"
                )
            if cancellation_boundary is not None:
                cancellation_boundary()
        return loaded

    def _gpu_queue_for_bundle(
        self, bundle_path: Path, state_root: Path
    ) -> tuple[dict[str, Any], Path]:
        key = (str(bundle_path), str(state_root))
        cached = self._gpu_queue_cache.get(key)
        if cached is not None:
            return cached
        gpu = self.config.section("gpu_readiness")
        try:
            queue, queue_path = self.modules.gpu_queue.materialize_queue(
                preprocess_bundle=bundle_path,
                preprocess_state_root=state_root,
                queue_root=Path(gpu["queue_root"]),
                production_profile_path=Path(gpu["production_profile"]),
                root_registration_path=Path(gpu["root_registration"]),
                root_registration_sha256=gpu["root_registration_sha256"],
            )
        except Exception as error:
            raise BackendError(
                f"GPU handoff queue materialization failed: {error}"
            ) from error
        queue_path = Path(queue_path)
        reference = {
            "path": str(queue_path),
            "sha256": sha256_bytes(
                self.modules.gpu_queue.canonical_bytes(queue)
            ),
            "queue_id": queue.get("queue_id"),
        }
        return self._cache_gpu_queue_manifest(
            manifest=queue,
            manifest_path=queue_path,
            expected_bundle_id=bundle_path.name,
            expected_reference=reference,
            expected_disposition=self._gpu_queue_disposition(queue),
            expected_candidate=(bundle_path, state_root),
        )

    def _gpu_inputs_drained(self) -> bool:
        normal = [
            row
            for row in self._campaign_runtimes()
            if row[0]["role"] == "normal_processing"
        ]
        _completed, _quarantined, pending = self._runtime_counts(normal)
        runnable, _ready_bytes, _raw_ready = self._preprocess_ready_counts(normal)
        failures = self._preprocess_failure_totals()
        return (
            pending == 0
            and runnable == 0
            and failures["retryable_failed_item_count"] == 0
        )

    def _bounded_gpu_partial_flush(
        self, candidates: Sequence[dict[str, Any]], *, force: bool
    ) -> bool:
        if force or not candidates:
            self._gpu_partial_fingerprint = None
            self._gpu_partial_hold_started_at = None
            return force
        identity = [
            [row["queue"]["queue_id"], row["member"]["ordinal"]]
            for row in candidates
        ]
        fingerprint = sha256_bytes(
            json.dumps(identity, separators=(",", ":"), ensure_ascii=True).encode(
                "ascii"
            )
        )
        if fingerprint != self._gpu_partial_fingerprint:
            self._gpu_partial_fingerprint = fingerprint
            # Additions to a partial tail must not restart its starvation clock.
            # The timer is reset only after a pack is sealed or the tail drains.
            if self._gpu_partial_hold_started_at is None:
                self._gpu_partial_hold_started_at = time.monotonic()
        if self._gpu_partial_hold_started_at is None:
            raise BackendError("GPU partial hold has no monotonic start")
        return (
            time.monotonic() - self._gpu_partial_hold_started_at
            >= GPU_PARTIAL_HOLD_SECONDS
        )

    @staticmethod
    def _gpu_partial_reaches_preferred_duration(
        candidates: Sequence[dict[str, Any]],
        *,
        maximum_items: int,
        maximum_total_audio_ms: int,
        preferred_total_audio_ms: int,
    ) -> bool:
        """Return true when one legal partial pack already amortizes startup."""

        selected = 0
        duration = 0
        input_hashes: set[str] = set()
        for row in candidates:
            audio = row["member"]["audio"]
            audio_sha256 = audio["sha256"]
            audio_duration = audio["duration_ms"]
            if audio_sha256 in input_hashes:
                continue
            if duration + audio_duration > maximum_total_audio_ms:
                continue
            selected += 1
            duration += audio_duration
            input_hashes.add(audio_sha256)
            if (
                selected >= maximum_items
                or duration >= preferred_total_audio_ms
            ):
                return True
        return False

    @staticmethod
    def _select_gpu_packs(
        candidates: Sequence[dict[str, Any]],
        *,
        maximum_items: int,
        maximum_total_audio_ms: int,
        preferred_total_audio_ms: int,
        maximum_batches: int,
        allow_partial: bool,
    ) -> tuple[list[list[dict[str, Any]]], list[dict[str, Any]]]:
        """Select fair packs, then order fixed worker pairs by similar duration."""

        remaining = list(candidates)
        packs: list[list[dict[str, Any]]] = []
        minimum = min(MIN_GPU_PACK_ITEMS, maximum_items)
        for _ in range(maximum_batches):
            if not remaining:
                break
            selected: list[dict[str, Any]] = []
            input_hashes: set[str] = set()
            duration = 0
            for row in remaining:
                audio = row["member"]["audio"]
                audio_sha256 = audio["sha256"]
                audio_duration = audio["duration_ms"]
                if audio_sha256 in input_hashes:
                    continue
                if duration + audio_duration > maximum_total_audio_ms:
                    continue
                selected.append(row)
                input_hashes.add(audio_sha256)
                duration += audio_duration
                if len(selected) >= maximum_items or (
                    len(selected) >= minimum
                    and duration >= preferred_total_audio_ms
                ):
                    break
            if not selected:
                raise BackendError(
                    "a ready GPU member cannot fit the production batch limits"
                )
            # If duplicate inputs or a duration limit prevent a nominally full
            # pack, pressure from at least one full pack of buffered members still
            # permits progress.  Equal audio hashes remain in distinct batches.
            enough_pressure = len(remaining) >= minimum
            if len(selected) < minimum and not enough_pressure and not allow_partial:
                break
            selected.sort(
                key=lambda row: (
                    -row["member"]["audio"]["duration_ms"],
                    row["campaign_ordinal"],
                    row["member"]["ordinal"],
                )
            )
            packs.append(selected)
            selected_claims = {
                (row["queue"]["queue_id"], row["member"]["ordinal"])
                for row in selected
            }
            remaining = [
                row
                for row in remaining
                if (row["queue"]["queue_id"], row["member"]["ordinal"])
                not in selected_claims
            ]
        return packs, remaining

    def _refresh_gpu_status(self) -> int:
        """Poll mutable results only after full record admission has succeeded.

        Queue manifests, materialization receipts, and external work orders are
        immutable lineage authority.  They are replayed at every boundary which
        can admit a record into ``_gpu_records``: journal restore, peer outcome,
        and local creation.  Re-entering that complete source replay on every
        two-second pending-batch poll is neither a new authority boundary nor
        necessary to interpret result files.  It also makes a long-lived GPU
        lane repeatedly traverse process-global replay machinery while other
        lanes are active.

        The status poll therefore reloads the exact SHA-pinned aggregate batch,
        checks it against the already-admitted batch ID and item count, and asks
        the batch contract to replay only its mutable result disposition.
        Restarts still perform the full source-lineage replay before reaching
        this method.
        """

        ready = 0
        for key, record in sorted(self._gpu_records.items()):
            status = self._gpu_status.get(key)
            if status is None:
                raise BackendError("admitted GPU record lacks exact status authority")
            if status == "pending":
                refreshed = self._refresh_admitted_gpu_record_status(key, record)
                self._gpu_status[key] = refreshed
                if refreshed != status:
                    # Pending witnesses deliberately omit mutable result leaves.
                    # A later checkpoint must recapture the terminal witness set
                    # instead of reusing the now-incomplete pending one.
                    self._gpu_restart_witnesses.pop(key, None)
                status = refreshed
            elif status not in {"completed", "parked", "not_applicable"}:
                raise BackendError("admitted GPU record status is invalid")
            ready += status == "pending"
        return ready

    def _refresh_admitted_gpu_record_status(
        self, ledger_key: str, record: dict[str, Any]
    ) -> str:
        """Replay one admitted batch and its result files, never its sources."""

        if (
            not isinstance(record, dict)
            or record.get("record_kind") != "ready_batch"
            or not isinstance(record.get("batch_key"), str)
            or not record["batch_key"]
            or ledger_key != record["batch_key"]
        ):
            raise BackendError("pending GPU record is not an admitted ready batch")
        batch = record.get("batch")
        if not isinstance(batch, dict) or set(batch) != {
            "path",
            "sha256",
            "batch_id",
        }:
            raise BackendError("pending GPU record batch reference is malformed")
        expected_count = self._gpu_batch_item_counts.get(ledger_key)
        if (
            isinstance(expected_count, bool)
            or not isinstance(expected_count, int)
            or expected_count < 1
        ):
            raise BackendError("pending GPU record lacks its admitted item count")
        try:
            profile = self._load_profile()
            batch_manifest, _body = self.modules.gpu_bridge.BATCH_V2.load_manifest(
                batch["path"], batch["sha256"], profile=profile
            )
            status = self.modules.gpu_bridge.BATCH_V2.batch_status(
                batch_manifest, profile
            )
        except Exception as error:
            raise BackendError(
                f"admitted GPU batch status replay failed: {error}"
            ) from error
        try:
            observed_batch_id = batch_manifest["batch_id"]
            observed_count = batch_manifest["totals"]["item_count"]
        except (KeyError, TypeError) as error:
            raise BackendError(
                "admitted GPU batch status replay returned a malformed manifest"
            ) from error
        if (
            observed_batch_id != batch["batch_id"]
            or observed_count != expected_count
        ):
            raise BackendError(
                "admitted GPU batch status replay differs from admission authority"
            )
        status_fields = {
            "status",
            "batch_id",
            "completed_ordinals",
            "absent_ordinals",
            "invalid",
            "inference_performed",
            "files_written",
        }
        if not isinstance(status, dict) or set(status) != status_fields:
            raise BackendError(
                "admitted GPU batch status replay returned a malformed disposition"
            )
        completed = status["completed_ordinals"]
        absent = status["absent_ordinals"]
        invalid = status["invalid"]
        if (
            status["status"] not in {"pending", "completed", "invalid"}
            or status["batch_id"] != batch["batch_id"]
            or status["inference_performed"] is not False
            or status["files_written"] is not False
            or not isinstance(completed, list)
            or not isinstance(absent, list)
            or not isinstance(invalid, list)
        ):
            raise BackendError(
                "admitted GPU batch status replay returned a malformed disposition"
            )
        invalid_ordinals: list[int] = []
        for row in invalid:
            if (
                not isinstance(row, dict)
                or set(row) != {"ordinal", "error_type", "message"}
                or not isinstance(row["error_type"], str)
                or not row["error_type"]
                or len(row["error_type"]) > 256
                or not isinstance(row["message"], str)
                or len(row["message"]) > 4096
            ):
                raise BackendError(
                    "admitted GPU batch status replay returned a malformed disposition"
                )
            invalid_ordinals.append(row["ordinal"])
        ordinal_sets = (completed, absent, invalid_ordinals)
        if any(
            any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= expected_count
                for value in values
            )
            or values != sorted(set(values))
            for values in ordinal_sets
        ):
            raise BackendError(
                "admitted GPU batch status replay returned a malformed disposition"
            )
        observed_ordinals = [*completed, *absent, *invalid_ordinals]
        if (
            sorted(observed_ordinals) != list(range(1, expected_count + 1))
            or len(observed_ordinals) != len(set(observed_ordinals))
        ):
            raise BackendError(
                "admitted GPU batch status replay returned a malformed disposition"
            )
        expected_status = (
            "invalid"
            if invalid
            else "completed"
            if len(completed) == expected_count
            else "pending"
        )
        if status["status"] != expected_status:
            raise BackendError(
                "admitted GPU batch status replay returned a contradictory disposition"
            )
        return self._record_gpu_batch_status(record, batch_manifest, status)

    def _gpu_launch_spec(
        self, record: dict[str, Any], attempt_ordinal: int
    ) -> LocalPrivateGpuLaunchSpec:
        if record.get("record_kind") != "ready_batch":
            raise BackendError("cannot launch a GPU child without an exact ready batch")
        batch = record["batch"]
        gpu = self.config.section("gpu_readiness")
        try:
            if gpu_runtime_successor.load_successor(self.config) is not None:
                manifest, body = self._checkpoint_small_json(
                    Path(batch["path"]), label="GPU runtime selection batch"
                )
                runtime = manifest.get("runtime_admission")
                if (
                    sha256_bytes(body) != batch["sha256"]
                    or manifest.get("batch_id") != batch["batch_id"]
                    or not isinstance(runtime, dict)
                ):
                    raise BackendError("GPU runtime selection lacks its exact batch binding")
                # Reconstruct old child-journal specs byte-for-byte; only new
                # batches may name the newly admitted executable environment.
                gpu = gpu_runtime_successor.effective_gpu(
                    self.config, batch_runtime=runtime
                )
        except BackendError:
            raise
        except Exception as error:
            raise BackendError(f"GPU runtime selection failed: {error}") from error
        return LocalPrivateGpuLaunchSpec(
            batch_id=batch["batch_id"],
            batch_manifest=Path(batch["path"]),
            expected_batch_sha256=batch["sha256"],
            runtime_admission=Path(gpu["runtime_admission"]),
            expected_runtime_admission_sha256=gpu["runtime_admission_sha256"],
            production_profile=Path(gpu["production_profile"]),
            expected_production_profile_sha256=gpu[
                "production_profile_sha256"
            ],
            root_registration=Path(gpu["root_registration"]),
            expected_root_registration_sha256=gpu[
                "root_registration_sha256"
            ],
            launcher_profile=Path(gpu["launcher_profile"]),
            expected_launcher_profile_sha256=gpu["launcher_profile_sha256"],
            local_readiness=Path(gpu["local_readiness"]),
            expected_local_readiness_sha256=gpu["local_readiness_sha256"],
            local_launcher=Path(gpu["local_launcher"]),
            writable_result_root=Path(gpu["result_root"]),
            writable_event_root=Path(gpu["event_root"]),
            writable_lock_root=Path(gpu["lock_root"]),
            working_directory=Path(gpu["working_directory"]),
            attempt_ordinal=attempt_ordinal,
        )

    def _gpu_materialization_controls(self) -> dict[str, Any]:
        try:
            return gpu_runtime_successor.effective_gpu(self.config)
        except Exception as error:
            raise BackendError(f"GPU runtime successor replay failed: {error}") from error

    def _assert_gpu_launch_runtime_current(self, spec: LocalPrivateGpuLaunchSpec) -> None:
        gpu = self._gpu_materialization_controls()
        if (
            str(spec.runtime_admission) != gpu["runtime_admission"]
            or spec.expected_runtime_admission_sha256 != gpu["runtime_admission_sha256"]
        ):
            raise BackendError(
                "pending GPU batch binds a retired runtime; explicit rematerialization is required"
            )

    @staticmethod
    def _gpu_child_summary(record: GpuChildRecord | None) -> dict[str, Any] | None:
        if record is None:
            return None
        return {
            "unit_name": record.unit_name,
            "batch_id": record.batch_id,
            "batch_sha256": record.batch_sha256,
            "attempt_ordinal": record.attempt_ordinal,
            "state": record.state,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "started_at": record.started_at,
            "completed_at": record.completed_at,
            "returncode": record.returncode,
            "result_status": (
                record.result.get("status")
                if isinstance(record.result, dict)
                else None
            ),
            "error": record.error,
        }

    @staticmethod
    def _gpu_child_material_state(record: GpuChildRecord) -> tuple[Any, ...]:
        return (
            record.state,
            record.child_invocation_id,
            record.started_at,
            record.completed_at,
            record.stop_requested_at,
            record.returncode,
            record.systemd_result,
            record.result,
            record.error,
        )

    def _supervise_gpu_child(
        self, *, stop: bool = False
    ) -> tuple[bool, dict[str, Any] | None, int]:
        """Reconcile or launch at most one exact GPU child.

        Child records, not the controller event log, are the durable manager
        authority.  This method caches their initial bounded replay and updates the
        cache only with exact records returned by the executor.
        """

        ready_records = [
            record
            for record in self._gpu_records.values()
            if record.get("record_kind") == "ready_batch"
        ]
        executor = self._get_gpu_executor()
        child_records = self._load_gpu_child_records(
            executor,
            # Quiesce must observe intents persisted by a launch call which
            # raised before returning its record to this in-memory cache.
            refresh=stop,
        )
        by_binding = {
            (record["batch"]["batch_id"], record["batch"]["sha256"]): record
            for record in ready_records
        }
        for child in child_records.values():
            if (child.batch_id, child.batch_sha256) not in by_binding:
                raise BackendError(
                    "GPU child journal references a batch outside controller recovery"
                )
        if self._gpu_opportunity_lease is not None:
            self._validate_gpu_opportunity_lease(self._gpu_opportunity_lease)
        if not ready_records:
            self._release_gpu_opportunity()
            return False, None, 0

        changed = False
        active: list[GpuChildRecord] = []
        reconciliation_states = {"launching", "running", "retiring", "stopping"}
        opportunity_authority_states = reconciliation_states | {
            "reconciliation_required"
        }
        for child in child_records.values():
            foreign = (
                child.outer_unit != executor.context.outer_unit
                or child.outer_invocation_id
                != executor.context.outer_invocation_id
            )
            if foreign and child.state in opportunity_authority_states:
                raise BackendError(
                    "GPU child from a prior controller invocation requires manual reconciliation"
                )
        current_authority = [
            child
            for child in child_records.values()
            if child.outer_unit == executor.context.outer_unit
            and child.outer_invocation_id == executor.context.outer_invocation_id
            and child.state in opportunity_authority_states
        ]
        if current_authority and not self._acquire_gpu_opportunity():
            raise BackendError(
                "active GPU child could not reacquire its retained opportunity lease"
            )
        for child in sorted(
            child_records.values(), key=lambda value: value.unit_name
        ):
            foreign = (
                child.outer_unit != executor.context.outer_unit
                or child.outer_invocation_id
                != executor.context.outer_invocation_id
            )
            if foreign:
                continue
            if child.state == "reconciliation_required" and not stop:
                raise BackendError(
                    "GPU child manager identity requires manual reconciliation"
                )
            stop_ambiguous = stop and child.state == "reconciliation_required"
            if child.state not in reconciliation_states and not stop_ambiguous:
                continue
            gpu_record = by_binding[(child.batch_id, child.batch_sha256)]
            spec = self._gpu_launch_spec(gpu_record, child.attempt_ordinal)
            before = self._gpu_child_material_state(child)
            try:
                updated = executor.stop(spec) if stop else executor.reconcile(spec)
            except Exception as error:
                raise BackendError(f"GPU child reconciliation failed: {error}") from error
            if updated is None:
                raise BackendError("persisted GPU child disappeared during reconciliation")
            child_records[updated.unit_name] = updated
            changed = changed or self._gpu_child_material_state(updated) != before
            if updated.state == "reconciliation_required" and not stop_ambiguous:
                raise BackendError(
                    "GPU child reconciliation did not establish an exact terminal state"
                )
            if updated.state in reconciliation_states:
                active.append(updated)
        if len(active) > self.config.section("gpu_readiness")["max_active_children"]:
            raise BackendError("GPU child journal exceeds the sealed active-child cap")
        if stop:
            if not active:
                self._release_gpu_opportunity()
            return changed, (self._gpu_child_summary(active[0]) if active else None), len(active)
        if active:
            if self._gpu_opportunity_lease is None:
                raise BackendError("active GPU child lacks its opportunity lease")
            return changed, self._gpu_child_summary(active[0]), len(active)

        # Exact batch results are completion authority even if a retired process had
        # a non-zero manager result.  Only still-pending batches consume attempts.
        self._refresh_gpu_status()
        pending = [
            record
            for record in ready_records
            if self._gpu_status.get(record["batch_key"]) == "pending"
        ]
        if not pending:
            self._release_gpu_opportunity()
            return changed, None, 0
        maximum = self.config.section("gpu_readiness")["max_attempts_per_batch"]
        for selected in pending:
            batch = selected["batch"]
            history = self._validated_gpu_child_history(
                [
                    record
                    for record in child_records.values()
                    if record.batch_id == batch["batch_id"]
                    and record.batch_sha256 == batch["sha256"]
                ],
                maximum=maximum,
            )
            attempt = len(history) + 1
            if attempt > maximum:
                changed = (
                    self._park_gpu_attempt_exhaustion(
                        selected, attempt_count=len(history)
                    )
                    or changed
                )
                continue
            if not self._acquire_gpu_opportunity():
                # Opportunity contention is a normal wait state. No child intent
                # has been persisted, so this does not consume an attempt.
                return changed, None, 0
            spec = self._gpu_launch_spec(selected, attempt)
            try:
                self._assert_gpu_launch_runtime_current(spec)
                launched = executor.launch(spec)
            except Exception as error:
                try:
                    self._load_gpu_child_records(executor, refresh=True)
                except Exception as refresh_error:
                    raise BackendError(
                        "exact GPU child launch failed and its durable journal "
                        f"could not be reloaded: {refresh_error}"
                    ) from error
                raise BackendError(f"exact GPU child launch failed: {error}") from error
            child_records[launched.unit_name] = launched
            if launched.state == "reconciliation_required":
                raise BackendError("GPU child launch outcome requires reconciliation")
            current = launched if launched.state in reconciliation_states else None
            if current is None:
                self._release_gpu_opportunity()
            return (
                True,
                self._gpu_child_summary(current or launched),
                int(current is not None),
            )
        self._release_gpu_opportunity()
        return changed, None, 0

    def _gpu_record(
        self,
        *,
        bundle_id: str,
        queue: dict[str, Any],
        queue_path: Path,
        ordinals: list[int],
    ) -> dict[str, Any]:
        queue_sha256 = sha256_bytes(self.modules.gpu_queue.canonical_bytes(queue))
        queue_ref = {
            "path": str(queue_path),
            "sha256": queue_sha256,
            "queue_id": queue["queue_id"],
        }
        disposition = self._gpu_queue_disposition(queue)
        if not ordinals:
            key = f"{queue['queue_id']}:no-ready-members"
            return {
                "record_kind": "no_ready_members",
                "batch_key": key,
                "preprocess_bundle_id": bundle_id,
                "queue": queue_ref,
                "batch": None,
                "materialization_receipt": None,
                "queue_disposition": disposition,
            }
        gpu = self._gpu_materialization_controls()
        try:
            receipt, receipt_path, _disposition = self.modules.gpu_bridge.materialize(
                queue_manifest_path=queue_path,
                expected_queue_sha256=queue_sha256,
                root_registration_path=Path(gpu["root_registration"]),
                expected_root_registration_sha256=gpu["root_registration_sha256"],
                runtime_admission_path=Path(gpu["runtime_admission"]),
                expected_runtime_admission_sha256=gpu["runtime_admission_sha256"],
                production_profile_path=Path(gpu["production_profile"]),
                expected_production_profile_sha256=gpu["production_profile_sha256"],
                queue_ordinals=ordinals,
                work_order_root=Path(gpu["work_order_root"]),
                receipt_root=Path(gpu["receipt_root"]),
                result_root=Path(gpu["result_root"]),
                batch_root=Path(gpu["batch_root"]),
                event_root=Path(gpu["event_root"]),
                lock_root=Path(gpu["lock_root"]),
                execution_mode=gpu["execution_mode"],
            )
        except Exception as error:
            raise BackendError(f"GPU batch materialization failed: {error}") from error
        receipt_sha256 = sha256_bytes(self.modules.gpu_bridge.canonical_bytes(receipt))
        key = f"{queue['queue_id']}:{','.join(str(value) for value in ordinals)}"
        return {
            "record_kind": "ready_batch",
            "batch_key": key,
            "preprocess_bundle_id": bundle_id,
            "queue": queue_ref,
            "batch": {
                "path": receipt["batch"]["path"],
                "sha256": receipt["batch"]["sha256"],
                "batch_id": receipt["batch"]["batch_id"],
            },
            "materialization_receipt": {
                "path": str(receipt_path),
                "sha256": receipt_sha256,
                "receipt_id": receipt["receipt_id"],
            },
            "queue_disposition": disposition,
        }

    def _gpu_packed_record(
        self, selected: Sequence[dict[str, Any]]
    ) -> dict[str, Any]:
        if not 1 <= len(selected) <= 32:
            raise BackendError("cross-queue GPU pack must contain 1..32 members")
        gpu = self._gpu_materialization_controls()
        grouped: dict[str, dict[str, Any]] = {}
        for row in selected:
            queue_id = row["queue"]["queue_id"]
            group = grouped.setdefault(
                queue_id,
                {
                    "first_campaign_ordinal": row["campaign_ordinal"],
                    "prototype": row,
                    "members": [],
                },
            )
            group["first_campaign_ordinal"] = min(
                group["first_campaign_ordinal"], row["campaign_ordinal"]
            )
            group["members"].append(row)

        sources: list[dict[str, Any]] = []
        work_orders: dict[tuple[str, int], dict[str, Any]] = {}
        for group in sorted(
            grouped.values(), key=lambda value: value["first_campaign_ordinal"]
        ):
            prototype = group["prototype"]
            ordinals = [row["member"]["ordinal"] for row in group["members"]]
            try:
                receipt, receipt_path, _disposition = (
                    self.modules.gpu_bridge.materialize(
                        queue_manifest_path=prototype["queue_path"],
                        expected_queue_sha256=prototype["queue"]["sha256"],
                        root_registration_path=Path(gpu["root_registration"]),
                        expected_root_registration_sha256=gpu[
                            "root_registration_sha256"
                        ],
                        runtime_admission_path=Path(gpu["runtime_admission"]),
                        expected_runtime_admission_sha256=gpu[
                            "runtime_admission_sha256"
                        ],
                        production_profile_path=Path(gpu["production_profile"]),
                        expected_production_profile_sha256=gpu[
                            "production_profile_sha256"
                        ],
                        queue_ordinals=ordinals,
                        work_order_root=Path(gpu["work_order_root"]),
                        receipt_root=Path(gpu["receipt_root"]),
                        result_root=Path(gpu["result_root"]),
                        batch_root=Path(gpu["batch_root"]),
                        event_root=Path(gpu["event_root"]),
                        lock_root=Path(gpu["lock_root"]),
                        execution_mode=gpu["execution_mode"],
                    )
                )
            except Exception as error:
                raise BackendError(
                    f"GPU source materialization for packed batch failed: {error}"
                ) from error
            if receipt["selection"]["queue_ordinals"] != ordinals:
                raise BackendError(
                    "GPU source materializer changed the packed queue selection"
                )
            receipt_sha256 = sha256_bytes(
                self.modules.gpu_bridge.canonical_bytes(receipt)
            )
            sources.append(
                {
                    "preprocess_bundle_id": prototype["bundle_id"],
                    "queue": dict(prototype["queue"]),
                    "queue_disposition": dict(prototype["queue_disposition"]),
                    "queue_ordinals": ordinals,
                    "materialization_receipt": {
                        "path": str(receipt_path),
                        "sha256": receipt_sha256,
                        "receipt_id": receipt["receipt_id"],
                    },
                }
            )
            for work_order in receipt["work_orders"]:
                claim = (
                    prototype["queue"]["queue_id"],
                    work_order["queue_ordinal"],
                )
                if claim in work_orders:
                    raise BackendError("GPU packed source materialization repeated a member")
                work_orders[claim] = work_order

        ordered_paths: list[Path] = []
        batch_members: list[dict[str, Any]] = []
        for batch_ordinal, row in enumerate(selected, 1):
            claim = (row["queue"]["queue_id"], row["member"]["ordinal"])
            work_order = work_orders.get(claim)
            if work_order is None:
                raise BackendError("GPU packed source materialization omitted a member")
            ordered_paths.append(Path(work_order["path"]))
            batch_members.append(
                {
                    "batch_ordinal": batch_ordinal,
                    "queue_id": claim[0],
                    "queue_ordinal": claim[1],
                    "preprocess_ordinal": work_order["preprocess_ordinal"],
                    "member_id": work_order["member_id"],
                    "work_order_id": work_order["work_order_id"],
                    "work_order_sha256": work_order["sha256"],
                    "work_order_identity_sha256": work_order["identity_sha256"],
                }
            )
        try:
            batch, batch_path = self.modules.gpu_bridge.BATCH_V2.materialize_batch(
                work_order_paths=ordered_paths,
                profile_path=Path(gpu["production_profile"]),
                expected_profile_sha256=gpu["production_profile_sha256"],
                batch_root=Path(gpu["batch_root"]),
                event_root=Path(gpu["event_root"]),
                lock_root=Path(gpu["lock_root"]),
            )
        except Exception as error:
            raise BackendError(f"cross-queue GPU batch materialization failed: {error}") from error
        batch_sha256 = sha256_bytes(
            self.modules.gpu_bridge.BATCH_V2.canonical_bytes(batch)
        )
        return {
            "record_kind": "ready_batch",
            "record_format": GPU_PACK_RECORD_FORMAT,
            "batch_key": f"packed:{batch['batch_id']}",
            "sources": sources,
            "batch_members": batch_members,
            "batch": {
                "path": str(batch_path),
                "sha256": batch_sha256,
                "batch_id": batch["batch_id"],
            },
        }

    def _run_gpu_readiness(self) -> StageOutcome:
        gpu = self.config.section("gpu_readiness")
        if not gpu["enabled"]:
            self._release_gpu_opportunity()
            return StageOutcome(
                "gpu_readiness",
                "skipped",
                False,
                {
                    "reason": "disabled_by_sealed_config",
                    "ready_batches": 0,
                    "pending_batches": 0,
                    "pending_items": 0,
                    "completed_items": 0,
                    "active_children": 0,
                    "current_gpu_child": None,
                    "buffered_ready_items": 0,
                    "parked_batches": 0,
                    "parked_items": 0,
                    "requires_chunking_items_cumulative": 0,
                    "explicit_skips_cumulative": 0,
                },
                {"backend_kind": BACKEND_KIND},
            )
        active_ready = self._refresh_gpu_status()
        profile = self._load_profile()
        high_water = profile["batch_limits"]["ready_batch_high_water"]
        child_progressed, current_child, active_children = (
            self._supervise_gpu_child()
        )
        active_ready = self._refresh_gpu_status()
        cumulative = self._gpu_disposition_totals()
        parked = self._gpu_parked_totals()
        item_totals = self._gpu_item_status_totals()
        if active_ready >= high_water:
            return StageOutcome(
                "gpu_readiness",
                "progressed" if child_progressed else "held",
                child_progressed,
                {
                    "ready_batches": active_ready,
                    "pending_batches": active_ready,
                    "pending_items": item_totals["pending_items"],
                    "completed_items": item_totals["completed_items"],
                    "completed_batches": sum(
                        status == "completed"
                        for status in self._gpu_status.values()
                    ),
                    "ready_batch_high_water": high_water,
                    "active_children": active_children,
                    "current_gpu_child": current_child,
                    "buffered_ready_items": self._gpu_buffered_ready_items,
                    "gpu_execution_supervised": True,
                    "parked_batches": parked["parked_batch_count"],
                    "parked_items": item_totals["parked_items"],
                    "requires_chunking_items_cumulative": cumulative[
                        "requires_chunking_count"
                    ],
                    "explicit_skips_cumulative": cumulative[
                        "explicit_skip_count"
                    ],
                    "dispositioned_queue_count": len(
                        self._gpu_queue_dispositions
                    ),
                },
                {"backend_kind": BACKEND_KIND},
            )

        created: list[dict[str, Any]] = []
        requires_chunking = 0
        explicit_skips = 0
        unclaimed: list[dict[str, Any]] = []
        no_ready: list[tuple[str, dict[str, Any], Path, str]] = []
        claimed_by_queue: dict[str, int] = {}
        for queue_id, _ordinal in self._gpu_member_claims:
            claimed_by_queue[queue_id] = claimed_by_queue.get(queue_id, 0) + 1
        for campaign_ordinal, bundle_path, bundle_id, state_root in (
            self._preprocess_bundle_candidates()
        ):
            queue, queue_path = self._gpu_queue_for_bundle(bundle_path, state_root)
            queue_sha256 = sha256_bytes(
                self.modules.gpu_queue.canonical_bytes(queue)
            )
            queue_reference = {
                "path": str(queue_path),
                "sha256": queue_sha256,
                "queue_id": queue["queue_id"],
            }
            disposition = self._gpu_queue_disposition(queue)
            requires_chunking += queue["totals"]["requires_chunking_count"]
            explicit_skips += queue["totals"]["explicit_skip_count"]
            claimed_count = claimed_by_queue.get(queue["queue_id"], 0)
            if claimed_count > disposition["ready_count"]:
                raise BackendError("GPU queue has more claims than ready members")
            if disposition["ready_count"] == 0:
                key = f"{queue['queue_id']}:no-ready-members"
                if key not in self._gpu_records:
                    no_ready.append((bundle_id, queue, queue_path, key))
                continue
            if claimed_count == disposition["ready_count"]:
                continue
            ready = [
                row
                for row in queue["members"]
                if row["resource_disposition"]["state"] == "ready"
            ]
            for member in ready:
                claim = (queue["queue_id"], member["ordinal"])
                if claim in self._gpu_member_claims:
                    continue
                unclaimed.append(
                    {
                        "campaign_ordinal": campaign_ordinal,
                        "bundle_id": bundle_id,
                        "queue": queue_reference,
                        "queue_path": queue_path,
                        "queue_disposition": disposition,
                        "member": member,
                    }
                )

        maximum = min(
            gpu["max_items_per_batch"], profile["batch_limits"]["maximum_items"]
        )
        batch_slots = min(
            gpu["max_batches_per_cycle"], max(0, high_water - active_ready)
        )
        threshold = min(MIN_GPU_PACK_ITEMS, maximum)
        force_tail = (
            bool(unclaimed)
            and len(unclaimed) < threshold
            and self._gpu_inputs_drained()
        )
        duration_ready = (
            bool(unclaimed)
            and len(unclaimed) < threshold
            and self._gpu_partial_reaches_preferred_duration(
                unclaimed,
                maximum_items=maximum,
                maximum_total_audio_ms=profile["batch_limits"][
                    "maximum_total_audio_ms"
                ],
                preferred_total_audio_ms=profile["batch_limits"][
                    "preferred_total_audio_ms"
                ],
            )
        )
        allow_partial = (
            len(unclaimed) >= threshold
            or duration_ready
            or self._bounded_gpu_partial_flush(unclaimed, force=force_tail)
        )
        packs, buffered = self._select_gpu_packs(
            unclaimed,
            maximum_items=maximum,
            maximum_total_audio_ms=profile["batch_limits"][
                "maximum_total_audio_ms"
            ],
            preferred_total_audio_ms=profile["batch_limits"][
                "preferred_total_audio_ms"
            ],
            maximum_batches=batch_slots,
            allow_partial=allow_partial,
        )
        for selected in packs:
            record = self._gpu_packed_record(selected)
            key = record["batch_key"]
            if key in self._gpu_records:
                raise BackendError("new packed GPU batch repeats a journal key")
            status = self._validate_gpu_record(record)
            self._gpu_records[key] = record
            self._gpu_status[key] = status
            self._register_gpu_disposition(record)
            created.append(record)
            active_ready += status == "pending"
        self._gpu_buffered_ready_items = len(buffered)
        if packs:
            self._gpu_partial_fingerprint = None
            self._gpu_partial_hold_started_at = None

        # Queue-only dispositions are durable records but do not consume a batch
        # slot.  Keep their per-cycle event payload bounded independently.
        for bundle_id, queue, queue_path, key in no_ready[
            : gpu["max_batches_per_cycle"]
        ]:
            record = self._gpu_record(
                bundle_id=bundle_id,
                queue=queue,
                queue_path=queue_path,
                ordinals=[],
            )
            status = self._validate_gpu_record(record)
            self._gpu_records[key] = record
            self._gpu_status[key] = status
            self._register_gpu_disposition(record)
            created.append(record)

        # A newly sealed aggregate must first become controller-event authority.
        # Supervision at the beginning of the next readiness tick may launch it;
        # launching here would leave an orphan child if the stage event were lost.
        cumulative = self._gpu_disposition_totals()
        parked = self._gpu_parked_totals()
        item_totals = self._gpu_item_status_totals()
        progressed = bool(created) or child_progressed
        terminal = (
            not created
            and active_ready == 0
            and active_children == 0
            and self._gpu_buffered_ready_items == 0
        )
        return StageOutcome(
            "gpu_readiness",
            "progressed" if progressed else ("complete" if terminal else "held"),
            progressed,
            {
                "ready_batches": active_ready,
                "pending_batches": active_ready,
                "pending_items": item_totals["pending_items"],
                "completed_items": item_totals["completed_items"],
                "completed_batches": sum(
                    status == "completed" for status in self._gpu_status.values()
                ),
                "parked_batches": parked["parked_batch_count"],
                "parked_items": item_totals["parked_items"],
                "ready_batch_high_water": high_water,
                "new_records": len(created),
                "new_batches": len(packs),
                "buffered_ready_items": self._gpu_buffered_ready_items,
                "requires_chunking_items_observed": requires_chunking,
                "explicit_skips_observed": explicit_skips,
                "requires_chunking_items_cumulative": cumulative[
                    "requires_chunking_count"
                ],
                "requires_chunking_audio_duration_ms_cumulative": cumulative[
                    "requires_chunking_audio_duration_ms"
                ],
                "explicit_skips_cumulative": cumulative["explicit_skip_count"],
                "dispositioned_queue_count": len(self._gpu_queue_dispositions),
                "active_children": active_children,
                "current_gpu_child": current_child,
                "gpu_execution_supervised": True,
                "gpu_execution_performed": child_progressed,
            },
            {
                "backend_kind": BACKEND_KIND,
                "gpu_records": created,
                "gpu_record_statuses": {
                    record["batch_key"]: self._gpu_status[record["batch_key"]]
                    for record in created
                },
            },
        )

    def quiesce(self) -> StageOutcome:
        """Stop only an exact, currently bound child after a durable stop request."""

        if not self.config.section("gpu_readiness")["enabled"]:
            self._release_gpu_opportunity()
            return StageOutcome(
                "gpu_readiness",
                "skipped",
                False,
                {
                    "reason": "disabled_by_sealed_config",
                    "pending_batches": 0,
                    "pending_items": 0,
                    "completed_items": 0,
                    "active_children": 0,
                    "current_gpu_child": None,
                    "buffered_ready_items": 0,
                    "parked_batches": 0,
                    "parked_items": 0,
                },
                {"backend_kind": BACKEND_KIND},
            )
        changed, current, active = self._supervise_gpu_child(stop=True)
        pending = self._refresh_gpu_status()
        cumulative = self._gpu_disposition_totals()
        parked = self._gpu_parked_totals()
        item_totals = self._gpu_item_status_totals()
        return StageOutcome(
            "gpu_readiness",
            "progressed" if changed else "held",
            changed,
            {
                "ready_batches": pending,
                "pending_batches": pending,
                "pending_items": item_totals["pending_items"],
                "completed_items": item_totals["completed_items"],
                "active_children": active,
                "current_gpu_child": current,
                "buffered_ready_items": self._gpu_buffered_ready_items,
                "parked_batches": parked["parked_batch_count"],
                "parked_items": item_totals["parked_items"],
                "stop_reconciled": True,
                "requires_chunking_items_cumulative": cumulative[
                    "requires_chunking_count"
                ],
                "explicit_skips_cumulative": cumulative["explicit_skip_count"],
            },
            {"backend_kind": BACKEND_KIND},
        )

    def _run_cold_retention(self, *, item_limit: int | None = None) -> StageOutcome:
        cold = self.config.section("cold_retention")
        if not cold["enabled"]:
            return StageOutcome(
                "cold_retention",
                "skipped",
                False,
                {
                    "reason": "disabled_by_sealed_config",
                    "retained_items": 0,
                    "replay_pending_items": 0,
                    "pending_items": 0,
                },
                {"backend_kind": BACKEND_KIND},
            )
        new_records: list[dict[str, Any]] = []
        completed = self._completed_acquisitions()
        completed_by_key = {row["item_key"]: row for row in completed}
        replayed_count = 0
        if item_limit is None:
            remaining_capacity = cold["max_items_per_cycle"]
        elif (
            isinstance(item_limit, bool)
            or not isinstance(item_limit, int)
            or not 1 <= item_limit <= cold["max_items_per_cycle"]
        ):
            raise BackendError("cold-retention lane item limit is invalid")
        else:
            remaining_capacity = item_limit
        while self._retention_replay_pending and remaining_capacity > 0:
            item_key = self._retention_replay_pending[0]
            row = completed_by_key.get(item_key)
            if row is None:
                raise BackendError("recorded cold retention lost its completed acquisition")
            try:
                result = self.modules.retention.run_retention(self._retention_request(row))
            except Exception as error:
                raise BackendError(
                    f"exact cold replay failed for {item_key}: {error}"
                ) from error
            replayed = self._retention_record(row, result)
            if replayed != self._retained_items[item_key]:
                raise BackendError("recorded cold retention differs from exact replay")
            self._retention_replay_pending.pop(0)
            replayed_count += 1
            remaining_capacity -= 1
        for row in completed:
            if remaining_capacity <= 0:
                break
            if row["item_key"] in self._retained_items:
                continue
            try:
                result = self.modules.retention.run_retention(self._retention_request(row))
            except Exception as error:
                raise BackendError(
                    f"exact cold retention failed for ordinal {row['ordinal']}: {error}"
                ) from error
            record = self._retention_record(row, result)
            self._retained_items[row["item_key"]] = record
            new_records.append(record)
            remaining_capacity -= 1
        pending = len(completed) - len(self._retained_items)
        return StageOutcome(
            "cold_retention",
            "progressed"
            if new_records or replayed_count
            else (
                "complete"
                if pending == 0 and not self._retention_replay_pending
                else "held"
            ),
            bool(new_records or replayed_count),
            {
                "completed_acquisitions": len(completed),
                "retained_items": len(self._retained_items),
                "exactly_replayed_this_run": (
                    len(self._retained_items) - len(self._retention_replay_pending)
                ),
                "replay_pending_items": len(self._retention_replay_pending),
                "pending_items": pending,
                "new_items": len(new_records),
                "replayed_items": replayed_count,
                "source_deletions": 0,
            },
            {"backend_kind": BACKEND_KIND, "retained": new_records},
        )

    def run_stage(
        self, stage: str, *, item_limit: int | None = None
    ) -> StageOutcome:
        if self._lane_stage is not None and stage != self._lane_stage:
            raise BackendError(
                f"{self._lane_stage} fork cannot execute {stage}"
            )
        if stage == "acquisition":
            outcome = self._run_acquisition(item_limit=item_limit)
        elif stage == "preprocess":
            outcome = self._run_preprocess(item_limit=item_limit)
        elif stage == "gpu_readiness":
            if item_limit is not None:
                raise BackendError("GPU readiness does not accept an item limit")
            outcome = self._run_gpu_readiness()
        elif stage == "cold_retention":
            outcome = self._run_cold_retention(item_limit=item_limit)
        else:
            raise BackendError(f"unsupported scheduler stage {stage!r}")
        self._record_own_outcome_queue_authority(outcome)
        return outcome
