#!/usr/bin/env python3
"""Advance a sealed Archive acquisition schedule into ASR-ready preprocessing.

The handoff deliberately owns no discovery, catalogue, cold-storage, deletion, or
publication capability.  It replays one background-producer schedule and selects
only completed acquisition results that the producer's receipt contract still
classifies as ready.  Each queue ordinal gets its own deterministic one-item
preprocess bundle.  That keeps a crash before receipt admission exactly resumable:
the next invocation reconstructs the same selection and bundle instead of scanning
for, or guessing, prior work.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Sequence


ACQUISITION_ROOT = Path(__file__).resolve().parent
if str(ACQUISITION_ROOT) not in sys.path:
    sys.path.insert(0, str(ACQUISITION_ROOT))

try:
    from . import background_producer, queue_runner
except ImportError:  # pragma: no cover - direct script execution
    import background_producer  # type: ignore[no-redef]
    import queue_runner  # type: ignore[no-redef]


PIPELINE_ROOT = ACQUISITION_ROOT.parent / "pipeline"
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))
import preprocess_batch  # noqa: E402


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.0"
HANDOFF_NAME = "himr-archive-acquisition-preprocess-handoff"
MAX_HANDOFF_ITEMS = 8
RESULT_ADMISSION_RETRY_LIMIT = 20
RESULT_ADMISSION_RETRY_SECONDS = 0.1
RESULT_ADMISSION_RACE_MESSAGES = (
    "result directory exists without result.json",
    "completed result directory has missing or extra entries",
    "completed result changed across strict immutable replay",
    "managed output root identity changed while verifying",
    "managed output root for durable acquisition result changed while opening",
    "managed output root for reusable content-addressed payload changed while opening",
    "path component changed while opening",
    "path identity changed at component",
    "completed/pending/quarantine state changed during offline validation",
)
COLD_ARCHIVE_ROOT = Path("/mnt/archive/HIMR")
SCHEDULE_ID_RE = re.compile(r"^bgacqsched_[0-9a-f]{32}$")

SAFETY = {
    "access_policy": "sealed_background_schedule_ready_results_only",
    "allowed_source_platform": "internet_archive",
    "catalog_access": "forbidden",
    "catalog_writes": False,
    "bounded_result_admission_retries": RESULT_ADMISSION_RETRY_LIMIT,
    "cold_storage_access": "forbidden",
    "credentials_allowed": False,
    "deletion_authority": "none",
    "discovery_allowed": False,
    "network_access_performed": False,
    "publication_authority": "none",
    "publication_performed": False,
    "selection_order": (
        "sealed_queue_ordinal_order_excluding_exact_controller_parked_ordinals"
    ),
}

COLD_PRIMARY_SAFETY = {
    **SAFETY,
    "cold_storage_access": "read_exact_sealed_acquisition_payloads_only",
}


class ArchivePreprocessHandoffError(RuntimeError):
    """The exact schedule-to-preprocess handoff failed closed."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ArchivePreprocessHandoffError(
            f"handoff value cannot be encoded canonically: {error}"
        ) from error


def pretty_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ArchivePreprocessHandoffError(
            f"handoff value cannot be serialized: {error}"
        ) from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ArchivePreprocessHandoffError(
            f"{label} must be an integer from {minimum} through {maximum}"
        )
    return value


def _lexical_absolute(path: Path, label: str) -> Path:
    raw = os.fspath(path)
    if not isinstance(raw, str) or not raw or "\x00" in raw or "://" in raw:
        raise ArchivePreprocessHandoffError(f"{label} must be a local path")
    requested = Path(raw)
    if not requested.is_absolute() or str(requested) != raw:
        raise ArchivePreprocessHandoffError(
            f"{label} must be a lexical absolute path"
        )
    return requested


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _reject_cold(path: Path, label: str) -> None:
    if _is_within(path, COLD_ARCHIVE_ROOT):
        raise ArchivePreprocessHandoffError(
            f"{label} may not reference the cold archive"
        )


def _acquisition_storage_safety(path: Path) -> dict[str, Any]:
    """Admit either a hot CAS or one dedicated cold-primary CAS.

    Controls and derived outputs remain hot and private.  A cold source is allowed
    only below (never at or above) the fixed archive root, and downstream selection
    still replays and hashes the immutable acquisition result and payload before
    preprocessing.
    """

    if path == COLD_ARCHIVE_ROOT:
        raise ArchivePreprocessHandoffError(
            "acquisition output root must be a dedicated descendant of the cold archive"
        )
    if COLD_ARCHIVE_ROOT in path.parents:
        return COLD_PRIMARY_SAFETY
    if path in COLD_ARCHIVE_ROOT.parents:
        raise ArchivePreprocessHandoffError(
            "acquisition output root may not contain the cold archive"
        )
    return SAFETY


def _require_disjoint(paths: list[tuple[str, Path]]) -> None:
    for index, (left_label, left) in enumerate(paths):
        for right_label, right in paths[index + 1 :]:
            if _is_within(left, right) or _is_within(right, left):
                raise ArchivePreprocessHandoffError(
                    f"{left_label} and {right_label} must be disjoint"
                )


def _ready_rows(
    bundle: dict[str, Any],
    states: list[dict[str, Any] | None],
    ready: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_items = ready.get("items")
    if not isinstance(raw_items, list):
        raise ArchivePreprocessHandoffError("producer ready snapshot is malformed")
    by_ordinal: dict[int, dict[str, Any]] = {}
    for raw in raw_items:
        if not isinstance(raw, dict) or set(raw) != {
            "ordinal",
            "job_id",
            "media_sha256",
            "media_byte_count",
        }:
            raise ArchivePreprocessHandoffError("producer ready row is malformed")
        ordinal = _integer(raw["ordinal"], "ready queue ordinal", 1, 10_000)
        if ordinal in by_ordinal:
            raise ArchivePreprocessHandoffError(
                "producer ready snapshot repeats a queue ordinal"
            )
        by_ordinal[ordinal] = raw

    rows: list[dict[str, Any]] = []
    seen_ready: set[int] = set()
    manifest_rows = bundle["manifest"]["work_orders"]
    orders = bundle["orders"]
    if len(manifest_rows) != len(orders) or len(orders) != len(states):
        raise ArchivePreprocessHandoffError(
            "validated producer bundle/state cardinality differs"
        )
    if any(
        not isinstance(order.get("source"), dict)
        or order["source"].get("platform") != "internet_archive"
        for order in orders
    ):
        raise ArchivePreprocessHandoffError(
            "Archive handoff requires an internet_archive-only sealed queue"
        )
    for entry, order, state in zip(manifest_rows, orders, states, strict=True):
        ordinal = entry["queue_ordinal"]
        raw = by_ordinal.get(ordinal)
        if raw is None:
            continue
        seen_ready.add(ordinal)
        if state is None:
            raise ArchivePreprocessHandoffError(
                "producer classified a pending acquisition result as ready"
            )
        result_path = queue_runner._result_path(order)
        if (
            raw["job_id"] != entry["job_id"]
            or raw["media_sha256"] != state["media_sha256"]
            or raw["media_byte_count"] != state["byte_count"]
        ):
            raise ArchivePreprocessHandoffError(
                "producer ready row differs from its sealed queue result"
            )
        rows.append(
            {
                "queue_ordinal": ordinal,
                "job_id": entry["job_id"],
                "result_path": result_path,
                "result_sha256": state["result_sha256"],
                "media_sha256": state["media_sha256"],
                "media_byte_count": state["byte_count"],
            }
        )
    if seen_ready != set(by_ordinal):
        raise ArchivePreprocessHandoffError(
            "producer ready snapshot names an ordinal outside the sealed queue"
        )
    if [row["queue_ordinal"] for row in rows] != sorted(
        row["queue_ordinal"] for row in rows
    ):
        raise ArchivePreprocessHandoffError(
            "producer ready snapshot does not follow sealed queue order"
        )
    return rows


def _load_runtime_with_admission_retry(
    schedule: dict[str, Any],
) -> tuple[
    dict[str, Any],
    list[dict[str, Any] | None],
    dict[str, Any],
    dict[str, Any],
]:
    """Replay through the acquisition result's bounded atomic-admission window."""

    for attempt in range(1, RESULT_ADMISSION_RETRY_LIMIT + 1):
        try:
            return background_producer._load_runtime(schedule)
        except queue_runner.QueueRunnerError as error:
            transient_shape = any(
                message in str(error) for message in RESULT_ADMISSION_RACE_MESSAGES
            )
            if not transient_shape or attempt == RESULT_ADMISSION_RETRY_LIMIT:
                raise
            # result.json is atomically replaced, but its exact parent and temporary
            # file exist briefly first. Every retry repeats the complete strict replay;
            # no malformed shape is ever accepted as a completed result.
            time.sleep(RESULT_ADMISSION_RETRY_SECONDS)
    raise ArchivePreprocessHandoffError("unreachable result-admission retry state")


def _selection_path(
    selection_root: Path,
    *,
    queue_ordinal: int,
    selection_id: str,
) -> Path:
    return selection_root / f"ordinal-{queue_ordinal:06d}-{selection_id}.json"


def _materialize_and_run_one(
    row: dict[str, Any],
    *,
    selection_root: Path,
    bundle_root: Path,
    processing_output_root: Path,
    state_root: Path,
) -> dict[str, Any]:
    result_path = Path(row["result_path"])
    intended = preprocess_batch.build_selection([result_path])
    selection_path = _selection_path(
        selection_root,
        queue_ordinal=row["queue_ordinal"],
        selection_id=intended["selection_id"],
    )
    admitted = preprocess_batch.write_selection([result_path], selection_path)
    if admitted != intended:
        raise ArchivePreprocessHandoffError(
            "preprocess selection changed between construction and admission"
        )
    bundle_path = preprocess_batch.materialize_bundle(
        selection_path,
        bundle_root,
        processing_output_root,
        operation_profile="asr-ready",
    )
    manifest, selection, orders = preprocess_batch.validate_bundle(bundle_path)
    if (
        selection != admitted
        or manifest["work_order_count"] != 1
        or len(orders) != 1
        or orders[0]["operations"] != preprocess_batch.ASR_READY_OPERATIONS
        or manifest["processing_output_root"] != str(processing_output_root)
    ):
        raise ArchivePreprocessHandoffError(
            "materialized bundle differs from the exact ASR-ready handoff"
        )
    run = preprocess_batch.run_batch(bundle_path, state_root, limit=1)
    if (
        run.get("bundle_id") != manifest["bundle_id"]
        or run.get("status") != "complete"
        or run.get("pending_count") != 0
        or run.get("completed_ordinals") != [1]
    ):
        raise ArchivePreprocessHandoffError(
            "preprocess run did not produce a complete receipt-bound one-item batch"
        )
    return {
        "queue_ordinal": row["queue_ordinal"],
        "job_id": row["job_id"],
        "acquisition_result": {
            "path": str(result_path),
            "sha256": row["result_sha256"],
        },
        "selection": {
            "path": str(selection_path),
            "selection_id": admitted["selection_id"],
            "selection_sha256": admitted["selection_sha256"],
        },
        "bundle": {
            "path": str(bundle_path),
            "bundle_id": manifest["bundle_id"],
            "manifest_sha256": manifest["manifest_sha256"],
        },
        "preprocess_receipt_count": run["completed_count"],
        "preprocess_state_sha256": run["state_sha256"],
    }


def _summary(
    *,
    status: str,
    schedule: dict[str, Any],
    schedule_path: Path,
    schedule_body: bytes,
    bundle_root: Path,
    processing_output_root: Path,
    state_root: Path,
    acquisition_output_root: Path,
    limit: int,
    ready_before: dict[str, Any],
    ready_after: dict[str, Any],
    selected_ordinals: list[int],
    parked_ordinals: list[int],
    processed: list[dict[str, Any]],
    stop_reason: str,
    safety: dict[str, Any],
) -> dict[str, Any]:
    core = {
        "schema_version": SCHEMA_VERSION,
        "handoff_name": HANDOFF_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": status,
        "schedule": {
            "path": str(schedule_path),
            "schedule_id": schedule["schedule_id"],
            "physical_sha256": sha256_bytes(schedule_body),
            "identity_sha256": schedule["identity_sha256"],
        },
        "queue_bundle_id": schedule["queue"]["bundle_id"],
        "limit": limit,
        "roots": {
            "acquisition_output_root": str(acquisition_output_root),
            "bundle_root": str(bundle_root),
            "processing_output_root": str(processing_output_root),
            "preprocess_state_root": str(state_root),
        },
        "storage_mode": (
            "cold_primary" if safety == COLD_PRIMARY_SAFETY else "hot_primary"
        ),
        "ready_before": ready_before,
        "ready_after": ready_after,
        "selected_queue_ordinals": selected_ordinals,
        "parked_queue_ordinals": parked_ordinals,
        "processed_items": processed,
        "stop_reason": stop_reason,
        "safety": safety,
    }
    return {**core, "summary_sha256": sha256_bytes(canonical_bytes(core))}


def run_handoff(
    schedule_path: Path,
    *,
    bundle_root: Path,
    processing_output_root: Path,
    limit: int,
    parked_queue_ordinals: Sequence[int] = (),
) -> dict[str, Any]:
    limit = _integer(limit, "--limit", 1, MAX_HANDOFF_ITEMS)
    if not isinstance(parked_queue_ordinals, (list, tuple)):
        raise ArchivePreprocessHandoffError(
            "parked queue ordinals must be an ordered finite sequence"
        )
    parked_ordinals = [
        _integer(value, "parked queue ordinal", 1, 10_000)
        for value in parked_queue_ordinals
    ]
    if parked_ordinals != sorted(set(parked_ordinals)):
        raise ArchivePreprocessHandoffError(
            "parked queue ordinals must be unique and sorted"
        )
    schedule, resolved_schedule, schedule_body = background_producer.load_schedule(
        schedule_path
    )
    schedule_id = schedule["schedule_id"]
    if not isinstance(schedule_id, str) or not SCHEDULE_ID_RE.fullmatch(schedule_id):
        raise ArchivePreprocessHandoffError("schedule ID is invalid")

    bundle_root = _lexical_absolute(bundle_root, "--bundle-root")
    processing_output_root = _lexical_absolute(
        processing_output_root, "--processing-output-root"
    )
    state_root = _lexical_absolute(
        Path(schedule["consumer"]["preprocess_state_root"]),
        "schedule preprocess state root",
    )
    acquisition_output_root = _lexical_absolute(
        Path(
            queue_runner._load_bundle(
                Path(schedule["queue"]["manifest_path"])
            )["manifest"]["policy"]["media_output_root"]
        ),
        "schedule acquisition output root",
    )
    for label, path in (
        ("bundle root", bundle_root),
        ("processing output root", processing_output_root),
        ("preprocess state root", state_root),
    ):
        _reject_cold(path, label)
    safety = _acquisition_storage_safety(acquisition_output_root)
    _require_disjoint(
        [
            ("bundle root", bundle_root),
            ("processing output root", processing_output_root),
            ("preprocess state root", state_root),
            ("acquisition output root", acquisition_output_root),
        ]
    )
    preprocess_batch.ensure_private_directory(bundle_root, "handoff bundle root")
    preprocess_batch.ensure_private_directory(
        processing_output_root, "handoff processing output root"
    )
    selection_root = preprocess_batch.ensure_private_directory(
        bundle_root / "selections" / schedule_id,
        "handoff selection root",
    )

    lock_name = f".archive-preprocess-{schedule_id}.lock"
    with preprocess_batch.writer_lock(bundle_root, lock_name):
        # Repeat the complete schedule replay after taking the handoff lock.  The
        # result snapshot, not a directory scan, is selection authority.
        replayed, replayed_path, replayed_body = background_producer.load_schedule(
            resolved_schedule
        )
        if replayed != schedule or replayed_path != resolved_schedule or replayed_body != schedule_body:
            raise ArchivePreprocessHandoffError(
                "sealed schedule changed before handoff execution"
            )
        producer_bundle, states, ready_before, _queue_summary = (
            _load_runtime_with_admission_retry(schedule)
        )
        ready_rows = _ready_rows(producer_bundle, states, ready_before)
        parked_set = set(parked_ordinals)
        ready_rows = [
            row for row in ready_rows if row["queue_ordinal"] not in parked_set
        ]
        selected = ready_rows[:limit]
        if not selected:
            return _summary(
                status="held",
                schedule=schedule,
                schedule_path=resolved_schedule,
                schedule_body=schedule_body,
                bundle_root=bundle_root,
                processing_output_root=processing_output_root,
                state_root=state_root,
                acquisition_output_root=acquisition_output_root,
                limit=limit,
                ready_before=ready_before,
                ready_after=ready_before,
                selected_ordinals=[],
                parked_ordinals=parked_ordinals,
                processed=[],
                stop_reason=(
                    "no_runnable_completed_unacknowledged_ready_results"
                    if parked_ordinals
                    else "no_completed_unacknowledged_ready_results"
                ),
                safety=safety,
            )

        selected_ordinals = [row["queue_ordinal"] for row in selected]
        processed = [
            _materialize_and_run_one(
                row,
                selection_root=selection_root,
                bundle_root=bundle_root,
                processing_output_root=processing_output_root,
                state_root=state_root,
            )
            for row in selected
        ]

        _final_bundle, _final_states, ready_after, _final_summary = (
            _load_runtime_with_admission_retry(schedule)
        )
        remaining = {
            row["ordinal"] for row in ready_after.get("items", []) if isinstance(row, dict)
        }
        if any(ordinal in remaining for ordinal in selected_ordinals):
            raise ArchivePreprocessHandoffError(
                "completed preprocess receipt did not release its acquisition result"
            )
        return _summary(
            status="bounded",
            schedule=schedule,
            schedule_path=resolved_schedule,
            schedule_body=schedule_body,
            bundle_root=bundle_root,
            processing_output_root=processing_output_root,
            state_root=state_root,
            acquisition_output_root=acquisition_output_root,
            limit=limit,
            ready_before=ready_before,
            ready_after=ready_after,
            selected_ordinals=selected_ordinals,
            parked_ordinals=parked_ordinals,
            processed=processed,
            stop_reason=(
                "limit_reached"
                if len(selected) == limit and ready_after["ready_item_count"] > 0
                else "ready_prefix_processed"
            ),
            safety=safety,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one bounded, receipt-aware Archive acquisition-to-ASR-ready "
            "preprocessing handoff"
        )
    )
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--processing-output-root", required=True)
    parser.add_argument(
        "--limit",
        required=True,
        type=int,
        help=f"maximum ready queue ordinals to preprocess (1-{MAX_HANDOFF_ITEMS})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_handoff(
            Path(args.schedule),
            bundle_root=Path(args.bundle_root),
            processing_output_root=Path(args.processing_output_root),
            limit=args.limit,
        )
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except (
        ArchivePreprocessHandoffError,
        background_producer.BackgroundProducerError,
        queue_runner.QueueRunnerError,
        preprocess_batch.BatchError,
        OSError,
    ) as error:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "handoff_name": HANDOFF_NAME,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
            "safety": SAFETY,
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
