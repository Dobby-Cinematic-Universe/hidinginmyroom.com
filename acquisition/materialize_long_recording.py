#!/usr/bin/env python3
"""Materialize one explicit long-recording candidate for full-source acquisition.

The normal queue materializer intentionally refuses ``requires_chunking`` entries.
This separate boundary preserves that invariant while allowing an operator to approve
one bounded full-source acquisition.  Remote time-section downloads are deliberately
not supported: exact processing windows are produced later from the admitted, hashed
parent object by ``pipeline/bin/local-window``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import acquire
import materialize_queue as queue


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
MAX_FULL_SOURCE_BYTES = 100 * 1024**3
DEFAULT_FORMAT_SELECTOR = queue.DEFAULT_FORMAT_SELECTOR
COLD_ARCHIVE_ROOT = Path("/mnt/archive/HIMR")


class LongRecordingError(RuntimeError):
    """A candidate, policy, or immutable-admission failure."""


def storage_access_policy(output_root: Path) -> str:
    """Describe, and bound, the output capability of a long-source bundle."""

    if output_root == COLD_ARCHIVE_ROOT:
        raise LongRecordingError(
            "media output root must be a dedicated descendant of the cold archive"
        )
    if COLD_ARCHIVE_ROOT in output_root.parents:
        return "sealed_work_order_media_output_root_write_only"
    if output_root in COLD_ARCHIVE_ROOT.parents:
        raise LongRecordingError(
            "media output root may not contain the cold archive"
        )
    return "forbidden"


def positive_integer(value: Any, label: str, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LongRecordingError(f"{label} must be a positive integer")
    if maximum is not None and value > maximum:
        raise LongRecordingError(f"{label} may not exceed {maximum}")
    return value


def select_candidate(plan: dict[str, Any], identifier: str) -> dict[str, Any]:
    if not identifier or "\x00" in identifier or len(identifier) > 500:
        raise LongRecordingError("--candidate-id must be a non-empty bounded identifier")
    matches = [
        candidate
        for candidate in plan["candidates"]
        if identifier
        in {
            candidate["native_id"],
            candidate["source_id"],
            candidate["recording_id"],
        }
    ]
    if len(matches) != 1:
        raise LongRecordingError(
            f"--candidate-id must match exactly one plan candidate; observed {len(matches)}"
        )
    candidate = matches[0]
    if candidate["queue_state"] != "requires_chunking":
        raise LongRecordingError("candidate is not routed requires_chunking")
    if candidate["queue_ordinal"] is not None or candidate["defer_reason"] != "requires_chunking":
        raise LongRecordingError("requires_chunking candidate has inconsistent queue disposition")
    if candidate["duration_ms"] is None:
        raise LongRecordingError("long-recording materialization requires known duration metadata")
    return candidate


def executable_policy(
    candidate: dict[str, Any], executable_text: str | None, sha256: str | None
) -> dict[str, Any] | None:
    if candidate["adapter"] != "yt_dlp":
        if executable_text is not None or sha256 is not None:
            raise LongRecordingError("yt-dlp pin options are only valid for a yt_dlp candidate")
        return None
    if executable_text is None or sha256 is None:
        raise LongRecordingError(
            "yt_dlp long recordings require --yt-dlp-executable and --yt-dlp-sha256"
        )
    path = queue.absolute_path(executable_text, "--yt-dlp-executable", must_exist=True)
    try:
        return queue.inspect_pinned_executable(path, sha256)
    except queue.MaterializationError as error:
        raise LongRecordingError(str(error)) from error


def build_work_order(
    candidate: dict[str, Any],
    *,
    plan_id: str,
    output_root: Path,
    executable: dict[str, Any] | None,
    full_source_max_bytes: int,
    global_cache_cap_bytes: int,
    free_space_floor_bytes: int,
    format_selector: str,
    http_timeout_seconds: int,
) -> dict[str, Any]:
    common = {
        "expected_sha256": candidate["expected_sha256"],
        "expected_byte_count": candidate["expected_byte_count"],
    }
    if candidate["adapter"] == "yt_dlp":
        if executable is None:
            raise AssertionError("validated yt_dlp executable pin is missing")
        adapter_config = {
            **common,
            "url": candidate["canonical_url"],
            "executable": executable["executable"],
            "expected_executable_sha256": executable["sha256"],
            "format_selector": format_selector,
        }
    elif candidate["adapter"] == "direct_http":
        adapter_config = {
            **common,
            "url": candidate["canonical_url"],
            "resume": True,
            "timeout_seconds": http_timeout_seconds,
        }
    else:
        raise LongRecordingError("candidate adapter is unsupported")
    job_id = queue.stable_id(
        "long-acq",
        plan_id,
        candidate["recording_id"],
        candidate["source_id"],
        str(full_source_max_bytes),
    )
    raw = {
        "schema_version": 1,
        "job_id": job_id,
        "adapter": candidate["adapter"],
        "source": {
            "platform": candidate["platform"],
            "source_kind": candidate["source_kind"],
            "native_id": candidate["native_id"],
            "canonical_url": candidate["canonical_url"],
            "title": candidate["title"],
            "published_at": None,
            "access_state": "public",
        },
        "adapter_config": adapter_config,
        "output": {"root": str(output_root)},
        "limits": {
            "max_job_bytes": full_source_max_bytes,
            "global_cache_cap_bytes": global_cache_cap_bytes,
            "free_space_floor_bytes": free_space_floor_bytes,
        },
    }
    try:
        return acquire.validate_work_order(raw)
    except acquire.AcquisitionError as error:
        raise LongRecordingError(f"generated acquisition work order is invalid: {error}") from error


def build_manifest(
    plan: dict[str, Any],
    candidate: dict[str, Any],
    *,
    output_root: Path,
    executable: dict[str, Any] | None,
    full_source_max_bytes: int,
    global_cache_cap_bytes: int,
    free_space_floor_bytes: int,
    format_selector: str,
    http_timeout_seconds: int,
) -> tuple[dict[str, Any], list[tuple[str, bytes]]]:
    if full_source_max_bytes < candidate["estimated_bytes"]:
        raise LongRecordingError(
            "--full-source-max-bytes is below the plan's conservative candidate estimate"
        )
    if global_cache_cap_bytes < full_source_max_bytes:
        raise LongRecordingError(
            "--global-cache-cap-bytes must be at least --full-source-max-bytes"
        )
    order = build_work_order(
        candidate,
        plan_id=plan["plan_id"],
        output_root=output_root,
        executable=executable,
        full_source_max_bytes=full_source_max_bytes,
        global_cache_cap_bytes=global_cache_cap_bytes,
        free_space_floor_bytes=free_space_floor_bytes,
        format_selector=format_selector,
        http_timeout_seconds=http_timeout_seconds,
    )
    order_body = queue.pretty_bytes(order)
    order_path = "work-orders/000001.json"
    plan_core = {key: value for key, value in plan.items() if key != "plan_id"}
    core = {
        "schema_version": SCHEMA_VERSION,
        "materializer": {
            "name": "himr-long-recording-materializer",
            "version": IMPLEMENTATION_VERSION,
        },
        "plan": {
            "plan_id": plan["plan_id"],
            "canonical_sha256": queue.sha256_bytes(queue.canonical_bytes(plan)),
            "core_sha256": queue.sha256_bytes(queue.canonical_bytes(plan_core)),
            "planned_at": plan["planned_at"],
        },
        "candidate": {
            key: candidate[key]
            for key in (
                "recording_id",
                "source_id",
                "platform",
                "source_kind",
                "native_id",
                "duration_ms",
                "estimated_bytes",
                "estimate_basis",
                "queue_state",
                "defer_reason",
            )
        },
        "policy": {
            "media_output_root": str(output_root),
            "full_source_max_bytes": full_source_max_bytes,
            "plan_single_job_max_bytes": plan["limits"]["max_job_bytes"],
            "global_cache_cap_bytes": global_cache_cap_bytes,
            "free_space_floor_bytes": free_space_floor_bytes,
            "direct_http_resume": True,
            "direct_http_timeout_seconds": http_timeout_seconds,
            "yt_dlp": None
            if executable is None
            else {**executable, "format_selector": format_selector},
        },
        "safety": {
            "bundle_class": "private_explicit_long_recording_acquisition",
            "access_policy": "public_only",
            "publication_authority": "none",
            "credentials_allowed": False,
            "network_access_performed": False,
            "catalog_mutated": False,
            "normal_queue_semantics_changed": False,
            "remote_time_sections_allowed": False,
            "full_source_required_for_local_windows": True,
            "access_challenge_policy": "retryable_without_credentials_or_admission",
            "cold_storage_access": storage_access_policy(output_root),
        },
        "work_order": {
            "job_id": order["job_id"],
            "adapter": order["adapter"],
            "path": order_path,
            "sha256": queue.sha256_bytes(order_body),
            "byte_count": len(order_body),
        },
    }
    bundle_id = queue.stable_id(
        "longacqbundle", queue.sha256_bytes(queue.canonical_bytes(core))
    )
    manifest = {
        "bundle_id": bundle_id,
        "bundle_relative_path": f"bundles/{bundle_id}",
        **core,
    }
    return manifest, [(order_path, order_body)]


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Materialize one explicit requires_chunking candidate for full acquisition"
    )
    result.add_argument("--plan", required=True, help="absolute queue plan path or -")
    result.add_argument("--candidate-id", required=True)
    result.add_argument("--bundle-root", required=True)
    result.add_argument("--media-output-root", required=True)
    result.add_argument("--yt-dlp-executable")
    result.add_argument("--yt-dlp-sha256")
    result.add_argument("--format-selector", default=DEFAULT_FORMAT_SELECTOR)
    result.add_argument("--full-source-max-bytes", type=int, required=True)
    result.add_argument("--global-cache-cap-bytes", type=int, required=True)
    result.add_argument("--free-space-floor-bytes", type=int, required=True)
    result.add_argument("--direct-http-timeout-seconds", type=int, default=60)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        try:
            plan = queue.validate_plan(queue.read_plan(args.plan))
        except queue.MaterializationError as error:
            raise LongRecordingError(str(error)) from error
        candidate = select_candidate(plan, args.candidate_id)
        output_root = queue.absolute_path(
            args.media_output_root, "--media-output-root", must_exist=False
        )
        bundle_root = queue.absolute_path(args.bundle_root, "--bundle-root", must_exist=False)
        queue.require_safe_root(output_root, "--media-output-root")
        executable = executable_policy(
            candidate, args.yt_dlp_executable, args.yt_dlp_sha256
        )
        full_source_max_bytes = positive_integer(
            args.full_source_max_bytes,
            "--full-source-max-bytes",
            MAX_FULL_SOURCE_BYTES,
        )
        global_cache_cap_bytes = positive_integer(
            args.global_cache_cap_bytes, "--global-cache-cap-bytes"
        )
        free_space_floor_bytes = queue.integer(
            args.free_space_floor_bytes, "--free-space-floor-bytes"
        )
        http_timeout = positive_integer(
            args.direct_http_timeout_seconds, "--direct-http-timeout-seconds", 3600
        )
        if not args.format_selector or len(args.format_selector) > 256:
            raise LongRecordingError("--format-selector must contain 1..256 characters")
        manifest, orders = build_manifest(
            plan,
            candidate,
            output_root=output_root,
            executable=executable,
            full_source_max_bytes=full_source_max_bytes,
            global_cache_cap_bytes=global_cache_cap_bytes,
            free_space_floor_bytes=free_space_floor_bytes,
            format_selector=args.format_selector,
            http_timeout_seconds=http_timeout,
        )
        try:
            queue.admit_bundle(bundle_root, manifest, orders)
        except queue.MaterializationError as error:
            raise LongRecordingError(str(error)) from error
        sys.stdout.write(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        return 0
    except (LongRecordingError, OSError) as error:
        sys.stderr.write(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
