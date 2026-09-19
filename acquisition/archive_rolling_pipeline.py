#!/usr/bin/env python3
"""Bounded rolling Archive acquisition and ASR-ready preprocessing.

One network worker advances a sealed background-producer schedule one ordinal at a
time while one preprocessing worker consumes exact receipt-unacknowledged results.
The workers share no discovery or catalogue authority.  Their concurrency is fixed
at one producer and one preprocessor; immutable acquisition results and preprocess
receipts remain the only restart state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ACQUISITION_ROOT = Path(__file__).resolve().parent
if str(ACQUISITION_ROOT) not in sys.path:
    sys.path.insert(0, str(ACQUISITION_ROOT))

try:
    from . import (
        archive_preprocess_handoff,
        background_producer,
        queue_runner,
    )
except ImportError:  # pragma: no cover - direct script execution
    import archive_preprocess_handoff  # type: ignore[no-redef]
    import background_producer  # type: ignore[no-redef]
    import queue_runner  # type: ignore[no-redef]


PIPELINE_ROOT = ACQUISITION_ROOT.parent / "pipeline"
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))
import preprocess_batch  # noqa: E402


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
PIPELINE_NAME = "himr-rolling-archive-acquisition-preprocess"
MAX_ITEMS = 8
WAIT_SECONDS = 2.0

SAFETY = {
    "access_policy": "one_sealed_archive_background_schedule",
    "allowed_source_platform": "internet_archive",
    "catalog_access": "forbidden",
    "catalog_writes": False,
    "cold_storage_access": "forbidden",
    "credentials_allowed": False,
    "deletion_authority": "none",
    "discovery_allowed": False,
    "maximum_network_concurrency": 1,
    "maximum_preprocess_concurrency": 1,
    "bounded_result_admission_retries": (
        archive_preprocess_handoff.RESULT_ADMISSION_RETRY_LIMIT
    ),
    "publication_authority": "none",
    "publication_performed": False,
    "shell_execution_allowed": False,
}

COLD_PRIMARY_SAFETY = {
    **SAFETY,
    "cold_storage_access": (
        "sealed_queue_media_output_write_and_exact_payload_read_only"
    ),
}


class RollingPipelineError(RuntimeError):
    """The bounded rolling supervisor failed before it could start."""


class RollingPipelineFailure(RollingPipelineError):
    """One worker failed after zero or more durable partial completions."""

    def __init__(self, summary: dict[str, Any]):
        super().__init__("the rolling Archive pipeline failed")
        self.summary = summary


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
        raise RollingPipelineError(
            f"rolling summary cannot be encoded canonically: {error}"
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
        raise RollingPipelineError(
            f"rolling summary cannot be serialized: {error}"
        ) from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise RollingPipelineError(
            f"{label} must be an integer from {minimum} through {maximum}"
        )
    return value


def _safe_error(stage: str, error: BaseException) -> dict[str, str]:
    return {
        "stage": stage,
        "type": type(error).__name__,
        "message": str(error)[:2_000],
    }


def _acquisition_cycle(
    summary: dict[str, Any], *, started_ns: int, ended_ns: int
) -> dict[str, Any]:
    queue = summary.get("queue_summary")
    if not isinstance(queue, dict):
        queue = {}
    return {
        "status": summary.get("status"),
        "stop_reason": summary.get("stop_reason"),
        "planned_ordinals": summary.get("planned_ordinals", []),
        "planned_reservation_bytes": summary.get("planned_reservation_bytes", 0),
        "new_item_count": queue.get("new_item_count", 0),
        "new_byte_count": queue.get("new_byte_count", 0),
        "ready_item_count_before": (summary.get("ready_before") or {}).get(
            "ready_item_count"
        ),
        "ready_item_count_after": (summary.get("ready_after") or {}).get(
            "ready_item_count"
        ),
        "started_monotonic_ns": started_ns,
        "ended_monotonic_ns": ended_ns,
        "duration_ms": max(0, round((ended_ns - started_ns) / 1_000_000)),
    }


def _preprocess_cycle(
    summary: dict[str, Any], *, started_ns: int, ended_ns: int
) -> dict[str, Any]:
    return {
        "status": summary.get("status"),
        "stop_reason": summary.get("stop_reason"),
        "selected_queue_ordinals": summary.get("selected_queue_ordinals", []),
        "processed_items": summary.get("processed_items", []),
        "ready_item_count_before": (summary.get("ready_before") or {}).get(
            "ready_item_count"
        ),
        "ready_item_count_after": (summary.get("ready_after") or {}).get(
            "ready_item_count"
        ),
        "started_monotonic_ns": started_ns,
        "ended_monotonic_ns": ended_ns,
        "duration_ms": max(0, round((ended_ns - started_ns) / 1_000_000)),
    }


def _overlap(cycles: list[dict[str, Any]], other: list[dict[str, Any]]) -> int:
    nanoseconds = 0
    for left in cycles:
        for right in other:
            start = max(left["started_monotonic_ns"], right["started_monotonic_ns"])
            end = min(left["ended_monotonic_ns"], right["ended_monotonic_ns"])
            if end > start:
                nanoseconds += end - start
    return nanoseconds


def _public_cycles(cycles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            key: value
            for key, value in row.items()
            if key not in {"started_monotonic_ns", "ended_monotonic_ns"}
        }
        for row in cycles
    ]


def _summary(
    *,
    status: str,
    started_at: str,
    schedule: dict[str, Any],
    schedule_path: Path,
    schedule_body: bytes,
    bundle_root: Path,
    processing_output_root: Path,
    acquisition_output_root: Path,
    limits: dict[str, int],
    shared: dict[str, Any],
    final_validation: dict[str, Any] | None,
    final_validation_error: dict[str, str] | None,
) -> dict[str, Any]:
    acquisition_cycles = shared["acquisition_cycles"]
    preprocess_cycles = shared["preprocess_cycles"]
    overlap_ns = _overlap(acquisition_cycles, preprocess_cycles)
    errors = list(shared["errors"])
    if final_validation_error is not None:
        errors.append(final_validation_error)
    core = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_name": PIPELINE_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": status,
        "started_at": started_at,
        "completed_at": utc_now(),
        "schedule": {
            "path": str(schedule_path),
            "schedule_id": schedule["schedule_id"],
            "physical_sha256": sha256_bytes(schedule_body),
            "identity_sha256": schedule["identity_sha256"],
            "queue_bundle_id": schedule["queue"]["bundle_id"],
        },
        "roots": {
            "acquisition_output_root": str(acquisition_output_root),
            "bundle_root": str(bundle_root),
            "processing_output_root": str(processing_output_root),
            "preprocess_state_root": schedule["consumer"]["preprocess_state_root"],
        },
        "storage_mode": (
            "cold_primary"
            if archive_preprocess_handoff._acquisition_storage_safety(
                acquisition_output_root
            )
            == archive_preprocess_handoff.COLD_PRIMARY_SAFETY
            else "hot_primary"
        ),
        "limits": limits,
        "accounting": {
            "new_acquisition_items": shared["new_items"],
            "new_acquisition_bytes": shared["new_bytes"],
            "acquisition_reservation_bytes": shared["reservation_bytes"],
            "preprocessed_items": shared["preprocessed_items"],
            "acquisition_cycle_count": len(acquisition_cycles),
            "preprocess_cycle_count": len(preprocess_cycles),
        },
        "acquisition_cycles": _public_cycles(acquisition_cycles),
        "preprocess_cycles": _public_cycles(preprocess_cycles),
        "overlap": {
            "observed": overlap_ns > 0,
            "duration_ms": round(overlap_ns / 1_000_000),
            "measurement": "producer_and_preprocess_call_intervals",
        },
        "worker_stop_reasons": {
            "acquisition": shared["acquisition_stop_reason"],
            "preprocess": shared["preprocess_stop_reason"],
        },
        "errors": errors,
        "final_schedule_validation": final_validation,
        "completion_authority": {
            "acquisition": "immutable_queue_result_json",
            "preprocess": "immutable_completed_private_media_preprocess_batch_item_receipt",
            "summary_is_authority": False,
        },
        "safety": (
            COLD_PRIMARY_SAFETY
            if archive_preprocess_handoff._acquisition_storage_safety(
                acquisition_output_root
            )
            == archive_preprocess_handoff.COLD_PRIMARY_SAFETY
            else SAFETY
        ),
    }
    return {**core, "summary_sha256": sha256_bytes(canonical_bytes(core))}


def _validate_limits(schedule: dict[str, Any], raw: dict[str, int]) -> dict[str, int]:
    policy = schedule["policy"]
    values = {
        "max_new_items": _integer(raw["max_new_items"], "--max-new-items", 1, MAX_ITEMS),
        "max_new_bytes": _integer(raw["max_new_bytes"], "--max-new-bytes", 1, 2**63 - 1),
        "max_run_seconds": _integer(
            raw["max_run_seconds"], "--max-run-seconds", 1, 14_400
        ),
        "free_space_floor_bytes": _integer(
            raw["free_space_floor_bytes"],
            "--free-space-floor-bytes",
            1,
            2**63 - 1,
        ),
        "max_preprocess_items": _integer(
            raw["max_preprocess_items"], "--max-preprocess-items", 1, MAX_ITEMS
        ),
    }
    if values["max_new_items"] > policy["maximum_dispatch_items_per_run"]:
        raise RollingPipelineError(
            "--max-new-items exceeds the sealed producer schedule bound"
        )
    if values["max_new_bytes"] > policy["maximum_dispatch_bytes_per_run"]:
        raise RollingPipelineError(
            "--max-new-bytes exceeds the sealed producer schedule bound"
        )
    if values["max_run_seconds"] > policy["maximum_run_seconds"]:
        raise RollingPipelineError(
            "--max-run-seconds exceeds the sealed producer schedule bound"
        )
    if values["free_space_floor_bytes"] < policy["free_space_floor_bytes"]:
        raise RollingPipelineError(
            "--free-space-floor-bytes weakens the sealed producer schedule floor"
        )
    return values


def run_rolling_pipeline(
    schedule_path: Path,
    *,
    bundle_root: Path,
    processing_output_root: Path,
    max_new_items: int,
    max_new_bytes: int,
    max_run_seconds: int,
    free_space_floor_bytes: int,
    max_preprocess_items: int,
) -> dict[str, Any]:
    started_at = utc_now()
    schedule, resolved_schedule, schedule_body = background_producer.load_schedule(
        schedule_path
    )
    limits = _validate_limits(
        schedule,
        {
            "max_new_items": max_new_items,
            "max_new_bytes": max_new_bytes,
            "max_run_seconds": max_run_seconds,
            "free_space_floor_bytes": free_space_floor_bytes,
            "max_preprocess_items": max_preprocess_items,
        },
    )

    bundle_root = archive_preprocess_handoff._lexical_absolute(
        bundle_root, "--bundle-root"
    )
    processing_output_root = archive_preprocess_handoff._lexical_absolute(
        processing_output_root, "--processing-output-root"
    )
    state_root = archive_preprocess_handoff._lexical_absolute(
        Path(schedule["consumer"]["preprocess_state_root"]),
        "schedule preprocess state root",
    )
    producer_bundle = queue_runner._load_bundle(
        Path(schedule["queue"]["manifest_path"])
    )
    acquisition_output_root = archive_preprocess_handoff._lexical_absolute(
        Path(producer_bundle["manifest"]["policy"]["media_output_root"]),
        "schedule acquisition output root",
    )
    if any(
        not isinstance(order.get("source"), dict)
        or order["source"].get("platform") != "internet_archive"
        for order in producer_bundle["orders"]
    ):
        raise RollingPipelineError(
            "rolling Archive pipeline requires an internet_archive-only sealed queue"
        )
    roots = [
        ("bundle root", bundle_root),
        ("processing output root", processing_output_root),
        ("preprocess state root", state_root),
        ("acquisition output root", acquisition_output_root),
    ]
    for label, path in roots[:-1]:
        archive_preprocess_handoff._reject_cold(path, label)
    archive_preprocess_handoff._acquisition_storage_safety(acquisition_output_root)
    archive_preprocess_handoff._require_disjoint(roots)
    preprocess_batch.ensure_private_directory(bundle_root, "rolling bundle root")
    preprocess_batch.ensure_private_directory(
        processing_output_root, "rolling processing output root"
    )

    condition = threading.Condition()
    deadline = time.monotonic() + limits["max_run_seconds"]
    shared: dict[str, Any] = {
        "stop": False,
        "producer_done": False,
        "preprocess_done": False,
        "new_items": 0,
        "new_bytes": 0,
        "reservation_bytes": 0,
        "preprocessed_items": 0,
        "acquisition_cycles": [],
        "preprocess_cycles": [],
        "errors": [],
        "acquisition_stop_reason": None,
        "preprocess_stop_reason": None,
        "generation": 0,
    }

    def fail(stage: str, error: BaseException) -> None:
        with condition:
            shared["errors"].append(_safe_error(stage, error))
            shared["stop"] = True
            shared["generation"] += 1
            condition.notify_all()

    def producer_worker() -> None:
        try:
            while True:
                with condition:
                    if shared["stop"]:
                        shared["acquisition_stop_reason"] = "peer_or_worker_failure"
                        break
                    if shared["new_items"] >= limits["max_new_items"]:
                        shared["acquisition_stop_reason"] = "max_new_items"
                        break
                    if shared["reservation_bytes"] >= limits["max_new_bytes"]:
                        shared["acquisition_stop_reason"] = "max_new_bytes"
                        break
                    if time.monotonic() >= deadline:
                        shared["acquisition_stop_reason"] = "max_run_seconds"
                        break
                    if shared["preprocess_done"]:
                        shared["acquisition_stop_reason"] = (
                            "preprocess_worker_finished_before_producer_capacity"
                        )
                        break
                    remaining_bytes = (
                        limits["max_new_bytes"] - shared["reservation_bytes"]
                    )
                    remaining_seconds = max(1, int(deadline - time.monotonic()))

                started_ns = time.monotonic_ns()
                try:
                    result = background_producer.run_producer(
                        resolved_schedule,
                        max_new_items=1,
                        max_new_bytes=remaining_bytes,
                        max_run_seconds=remaining_seconds,
                        free_space_floor_bytes=limits["free_space_floor_bytes"],
                    )
                except background_producer.BackgroundProducerFailure as error:
                    ended_ns = time.monotonic_ns()
                    cycle = _acquisition_cycle(
                        error.summary, started_ns=started_ns, ended_ns=ended_ns
                    )
                    with condition:
                        shared["acquisition_cycles"].append(cycle)
                        shared["new_items"] += cycle["new_item_count"]
                        shared["new_bytes"] += cycle["new_byte_count"]
                        shared["reservation_bytes"] += cycle[
                            "planned_reservation_bytes"
                        ]
                    raise
                ended_ns = time.monotonic_ns()
                cycle = _acquisition_cycle(
                    result, started_ns=started_ns, ended_ns=ended_ns
                )
                with condition:
                    shared["acquisition_cycles"].append(cycle)
                    shared["new_items"] += cycle["new_item_count"]
                    shared["new_bytes"] += cycle["new_byte_count"]
                    shared["reservation_bytes"] += cycle[
                        "planned_reservation_bytes"
                    ]
                    shared["generation"] += 1
                    condition.notify_all()

                if result["status"] in {"completed", "parked"}:
                    with condition:
                        shared["acquisition_stop_reason"] = result["stop_reason"]
                    break
                if result["status"] == "held":
                    reason = result["stop_reason"]
                    if reason not in {"at_or_above_high_water", "hysteresis_hold"}:
                        with condition:
                            shared["acquisition_stop_reason"] = reason
                        break
                    with condition:
                        generation = shared["generation"]
                        while (
                            not shared["stop"]
                            and not shared["preprocess_done"]
                            and shared["generation"] == generation
                            and time.monotonic() < deadline
                        ):
                            condition.wait(
                                timeout=min(
                                    WAIT_SECONDS,
                                    max(0.05, deadline - time.monotonic()),
                                )
                            )
        except BaseException as error:  # worker boundary; re-emitted as typed JSON
            fail("acquisition", error)
        finally:
            with condition:
                shared["producer_done"] = True
                if shared["acquisition_stop_reason"] is None:
                    shared["acquisition_stop_reason"] = "worker_exit"
                shared["generation"] += 1
                condition.notify_all()

    def preprocess_worker() -> None:
        try:
            while True:
                with condition:
                    if shared["stop"]:
                        shared["preprocess_stop_reason"] = "peer_or_worker_failure"
                        break
                    if shared["preprocessed_items"] >= limits["max_preprocess_items"]:
                        shared["preprocess_stop_reason"] = "max_preprocess_items"
                        break
                    if time.monotonic() >= deadline:
                        shared["preprocess_stop_reason"] = "max_run_seconds"
                        break

                started_ns = time.monotonic_ns()
                result = archive_preprocess_handoff.run_handoff(
                    resolved_schedule,
                    bundle_root=bundle_root,
                    processing_output_root=processing_output_root,
                    limit=1,
                )
                ended_ns = time.monotonic_ns()
                cycle = _preprocess_cycle(
                    result, started_ns=started_ns, ended_ns=ended_ns
                )
                processed_count = len(cycle["processed_items"])
                if processed_count not in {0, 1}:
                    raise RollingPipelineError(
                        "one-item handoff returned invalid completion accounting"
                    )
                with condition:
                    shared["preprocess_cycles"].append(cycle)
                    shared["preprocessed_items"] += processed_count
                    shared["generation"] += 1
                    condition.notify_all()

                if result["status"] == "held":
                    with condition:
                        if shared["producer_done"]:
                            shared["preprocess_stop_reason"] = "no_ready_results"
                            break
                        generation = shared["generation"]
                        while (
                            not shared["stop"]
                            and not shared["producer_done"]
                            and shared["generation"] == generation
                            and time.monotonic() < deadline
                        ):
                            condition.wait(
                                timeout=min(
                                    WAIT_SECONDS,
                                    max(0.05, deadline - time.monotonic()),
                                )
                            )
        except BaseException as error:  # worker boundary; re-emitted as typed JSON
            fail("preprocess", error)
        finally:
            with condition:
                shared["preprocess_done"] = True
                if shared["preprocess_stop_reason"] is None:
                    shared["preprocess_stop_reason"] = "worker_exit"
                shared["generation"] += 1
                condition.notify_all()

    # The full-run lock prevents two out-of-band rolling supervisors from sharing
    # one control root.  The console's archive_pipeline resource additionally owns
    # both network and preprocess claims, preventing standalone console actions.
    lock_name = f".archive-rolling-{schedule['schedule_id']}.lock"
    with preprocess_batch.writer_lock(bundle_root, lock_name):
        producer_thread = threading.Thread(
            target=producer_worker,
            name="himr-archive-network-1",
            daemon=False,
        )
        preprocess_thread = threading.Thread(
            target=preprocess_worker,
            name="himr-archive-preprocess-1",
            daemon=False,
        )
        preprocess_thread.start()
        producer_thread.start()
        producer_thread.join()
        preprocess_thread.join()

    final_validation: dict[str, Any] | None = None
    final_error: dict[str, str] | None = None
    try:
        validation = background_producer.validate_producer(resolved_schedule)
        final_validation = {
            "status": validation["status"],
            "completed_acquisition_count": validation["ready_before"][
                "completed_acquisition_count"
            ],
            "quarantined_acquisition_count": validation["ready_before"][
                "quarantined_acquisition_count"
            ],
            "acknowledged_preprocess_count": validation["ready_before"][
                "acknowledged_preprocess_count"
            ],
            "ready_item_count": validation["ready_before"]["ready_item_count"],
            "pending_acquisition_count": validation["queue_summary"]["pending_count"],
            "parked_acquisition_count": validation["queue_summary"]["parked_count"],
            "summary_sha256": validation["summary_sha256"],
        }
    except BaseException as error:
        final_error = _safe_error("final_validation", error)

    failed = bool(shared["errors"] or final_error)
    progressed = bool(shared["new_items"] or shared["preprocessed_items"])
    parked = bool(
        final_validation
        and final_validation["pending_acquisition_count"] == 0
        and final_validation["parked_acquisition_count"] > 0
    )
    summary = _summary(
        status=(
            "failed"
            if failed
            else "parked"
            if parked
            else "bounded"
            if progressed
            else "held"
        ),
        started_at=started_at,
        schedule=schedule,
        schedule_path=resolved_schedule,
        schedule_body=schedule_body,
        bundle_root=bundle_root,
        processing_output_root=processing_output_root,
        acquisition_output_root=acquisition_output_root,
        limits=limits,
        shared=shared,
        final_validation=final_validation,
        final_validation_error=final_error,
    )
    if failed:
        raise RollingPipelineFailure(summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Overlap one sealed Archive producer with one receipt-bound ASR-ready "
            "preprocessor under finite limits"
        )
    )
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--processing-output-root", required=True)
    parser.add_argument("--max-new-items", required=True, type=int)
    parser.add_argument("--max-new-bytes", required=True, type=int)
    parser.add_argument("--max-run-seconds", required=True, type=int)
    parser.add_argument("--free-space-floor-bytes", required=True, type=int)
    parser.add_argument("--max-preprocess-items", required=True, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_rolling_pipeline(
            Path(args.schedule),
            bundle_root=Path(args.bundle_root),
            processing_output_root=Path(args.processing_output_root),
            max_new_items=args.max_new_items,
            max_new_bytes=args.max_new_bytes,
            max_run_seconds=args.max_run_seconds,
            free_space_floor_bytes=args.free_space_floor_bytes,
            max_preprocess_items=args.max_preprocess_items,
        )
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except RollingPipelineFailure as error:
        sys.stderr.buffer.write(pretty_bytes(error.summary))
        return 2
    except (
        RollingPipelineError,
        archive_preprocess_handoff.ArchivePreprocessHandoffError,
        background_producer.BackgroundProducerError,
        queue_runner.QueueRunnerError,
        preprocess_batch.BatchError,
        OSError,
    ) as error:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "pipeline_name": PIPELINE_NAME,
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "failed",
            "errors": [_safe_error("preflight", error)],
            "completion_authority": {
                "acquisition": "immutable_queue_result_json",
                "preprocess": "immutable_completed_private_media_preprocess_batch_item_receipt",
                "summary_is_authority": False,
            },
            "safety": SAFETY,
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
