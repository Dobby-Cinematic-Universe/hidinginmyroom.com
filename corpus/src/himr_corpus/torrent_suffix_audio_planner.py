"""Read-only plans for deferred torrent filename locator grammars.

This module intentionally does not extend the sealed terminal-bracket importer.
It reuses that lane's exact torrent/discovery/catalogue replay boundary, but emits
only a private plan for two disjoint filename shapes:

* ``[11-char YouTube ID] 480p.<recognized video extension>``; and
* ``[11-char YouTube ID].m4a``.

The plan has no catalogue admission path.  A filename is locator evidence only;
it is never a content match, relationship, recording merge, identity assertion,
claim, event, or publication decision.
"""

from __future__ import annotations

import base64
import re
import sqlite3
from pathlib import Path
from typing import Any

from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .torrent_bracket_reconciler import (
    MAX_CANDIDATES,
    MAX_DISCOVERY_BYTES,
    MAX_PATH_COMPONENT_BYTES,
    MAX_SCOPED_FILES,
    MAX_TORRENT_BYTES,
    MAX_TORRENT_FILES,
    SCOPED_DIRECTORY_BYTES,
    SCOPED_DIRECTORY_LABELS,
    VIDEO_EXTENSIONS,
    YOUTUBE_ID_RE,
    TorrentBracketReconciliationError,
    _catalog_binding,
    _combined_import_digest,
    _parse_torrent,
    _raw_path_sha256,
    _stable_file,
    _validate_discovery,
    _youtube_resolution,
    terminal_bracketed_youtube_id_bytes,
)


PLAN_KIND = "torrent_suffix_audio_youtube_reconciliation_review_plan"
PLANNER_VERSION = "torrent_suffix_audio_plan_v1"

FORMAT_LABEL_VIDEO_LANE = "format_label_video"
AUDIO_ONLY_LANE = "audio_only"

FORMAT_LABEL_VIDEO_EVIDENCE = (
    "terminal_filename_bracket_before_480p_label_and_video_extension"
)
AUDIO_ONLY_EVIDENCE = "terminal_filename_bracket_before_m4a_audio_extension"

FORMAT_LABEL_VIDEO_CONFIDENCE = "uncalibrated_format_label_filename_locator"
AUDIO_ONLY_CONFIDENCE = "uncalibrated_audio_only_filename_locator"

FORMAT_LABEL_VIDEO_ROUTE = "torrent_format_label_video_locator_review"
AUDIO_ONLY_ROUTE = "torrent_audio_only_locator_review"

FORMAT_LABEL_VIDEO_PRIORITY = 55
AUDIO_ONLY_PRIORITY = 50

FORMAT_LABEL_VIDEO_BYTES_RE = re.compile(
    rb"\[([A-Za-z0-9_-]{11})\] 480p\.((?i:mp4|webm|ogv|mkv|mov|m4v))$"
)
AUDIO_ONLY_BYTES_RE = re.compile(rb"\[([A-Za-z0-9_-]{11})\]\.m4a$", re.IGNORECASE)

# These two broader recognizers are used only for aggregate rejection counts.  A
# token captured by either expression is never emitted as a candidate unless it
# independently satisfies one of the exact expressions above.
FORMAT_LABEL_TOKEN_BYTES_RE = re.compile(
    rb"\[([^\[\]\r\n]{1,64})\] 480p\.((?i:mp4|webm|ogv|mkv|mov|m4v))$"
)
AUDIO_ONLY_TOKEN_BYTES_RE = re.compile(
    rb"\[([^\[\]\r\n]{1,64})\]\.m4a$", re.IGNORECASE
)
BRACKET_TOKEN_BYTES_RE = re.compile(rb"\[[^\[\]\r\n]{1,64}\]")
BRACKET_FORMAT_LABEL_HINT_BYTES_RE = re.compile(
    rb"\[[^\[\]\r\n]{1,64}\][ \t]+[0-9]{3,4}[pP]"
)


def suffix_audio_locator_bytes(filename: object) -> dict[str, str] | None:
    """Parse only one of the two exact, mutually exclusive deferred suffixes."""

    if (
        not isinstance(filename, bytes)
        or not filename
        or len(filename) > MAX_PATH_COMPONENT_BYTES
    ):
        return None
    video = FORMAT_LABEL_VIDEO_BYTES_RE.search(filename)
    if video is not None:
        return {
            "lane": FORMAT_LABEL_VIDEO_LANE,
            "youtube_video_id": video.group(1).decode("ascii"),
            "file_extension": video.group(2).decode("ascii").lower(),
            "format_label": "480p",
        }
    audio = AUDIO_ONLY_BYTES_RE.search(filename)
    if audio is not None:
        return {
            "lane": AUDIO_ONLY_LANE,
            "youtube_video_id": audio.group(1).decode("ascii"),
            "file_extension": "m4a",
            "format_label": "none",
        }
    return None


def _rejection_class(filename: bytes) -> str | None:
    """Return a bounded aggregate rejection class, never path/token content."""

    if suffix_audio_locator_bytes(filename) is not None:
        return None
    # The sealed terminal-video grammar belongs to migration 0018, not this plan.
    if terminal_bracketed_youtube_id_bytes(filename) is not None:
        return None
    format_match = FORMAT_LABEL_TOKEN_BYTES_RE.search(filename)
    if format_match is not None and not re.fullmatch(
        rb"[A-Za-z0-9_-]{11}", format_match.group(1)
    ):
        return "format_label_invalid_id_token"
    audio_match = AUDIO_ONLY_TOKEN_BYTES_RE.search(filename)
    if audio_match is not None and not re.fullmatch(
        rb"[A-Za-z0-9_-]{11}", audio_match.group(1)
    ):
        return "audio_only_invalid_id_token"
    lower = filename.lower()
    if BRACKET_TOKEN_BYTES_RE.search(filename) is not None and (
        b"480p" in lower
        or b".m4a" in lower
        or BRACKET_FORMAT_LABEL_HINT_BYTES_RE.search(filename) is not None
    ):
        return "ambiguous_or_noncanonical_suffix"
    return None


def _lane_contract(lane: str) -> dict[str, Any]:
    if lane == FORMAT_LABEL_VIDEO_LANE:
        return {
            "candidate_kind": "torrent_format_label_video_to_youtube_locator",
            "evidence_basis": FORMAT_LABEL_VIDEO_EVIDENCE,
            "confidence_profile": FORMAT_LABEL_VIDEO_CONFIDENCE,
            "review_route": FORMAT_LABEL_VIDEO_ROUTE,
            "review_priority": FORMAT_LABEL_VIDEO_PRIORITY,
            "review_method": "filename_provenance_then_video_content_identity",
        }
    if lane == AUDIO_ONLY_LANE:
        return {
            "candidate_kind": "torrent_audio_only_to_youtube_locator",
            "evidence_basis": AUDIO_ONLY_EVIDENCE,
            "confidence_profile": AUDIO_ONLY_CONFIDENCE,
            "review_route": AUDIO_ONLY_ROUTE,
            "review_priority": AUDIO_ONLY_PRIORITY,
            "review_method": "filename_provenance_then_audio_content_identity",
        }
    raise TorrentBracketReconciliationError("suffix/audio planner escaped its fixed lanes")


def build_torrent_suffix_audio_plan(
    connection: sqlite3.Connection,
    torrent_path: Path,
    discovery_metadata_path: Path,
) -> dict[str, Any]:
    """Build a deterministic, read-only plan in one catalogue snapshot."""

    owns_read_transaction = not connection.in_transaction
    if owns_read_transaction:
        connection.execute("BEGIN")
    try:
        return _build_torrent_suffix_audio_plan_in_snapshot(
            connection, torrent_path, discovery_metadata_path
        )
    finally:
        if owns_read_transaction and connection.in_transaction:
            connection.rollback()


def _build_torrent_suffix_audio_plan_in_snapshot(
    connection: sqlite3.Connection,
    torrent_path: Path,
    discovery_metadata_path: Path,
) -> dict[str, Any]:
    resolved_torrent, torrent_body = _stable_file(
        Path(torrent_path), MAX_TORRENT_BYTES, "torrent manifest"
    )
    resolved_discovery, discovery_body = _stable_file(
        Path(discovery_metadata_path),
        MAX_DISCOVERY_BYTES,
        "torrent discovery metadata",
    )
    if resolved_torrent == resolved_discovery:
        raise TorrentBracketReconciliationError("torrent and discovery inputs must differ")
    torrent = _parse_torrent(torrent_body)
    discovery = _validate_discovery(
        discovery_body,
        discovery_path=resolved_discovery,
        torrent_path=resolved_torrent,
        torrent=torrent,
    )
    combined_input_sha256 = _combined_import_digest(
        resolved_torrent, torrent_body, resolved_discovery, discovery_body
    )
    binding = _catalog_binding(
        connection,
        torrent=torrent,
        discovery=discovery,
        combined_input_sha256=combined_input_sha256,
        torrent_filename=resolved_torrent.name,
    )

    scoped_files = 0
    candidates: list[dict[str, Any]] = []
    lane_counts = {FORMAT_LABEL_VIDEO_LANE: 0, AUDIO_ONLY_LANE: 0}
    directory_counts = {
        label: {FORMAT_LABEL_VIDEO_LANE: 0, AUDIO_ONLY_LANE: 0, "total": 0}
        for label in SCOPED_DIRECTORY_LABELS
    }
    resolution_counts = {
        "missing_native_source": 0,
        "native_source_without_unique_recording": 0,
        "unique_native_recording": 0,
    }
    rejection_counts = {
        "format_label_invalid_id_tokens": 0,
        "audio_only_invalid_id_tokens": 0,
        "ambiguous_or_noncanonical_suffixes": 0,
    }
    distinct_ids: set[str] = set()
    lane_distinct_ids = {
        FORMAT_LABEL_VIDEO_LANE: set(),
        AUDIO_ONLY_LANE: set(),
    }
    resolution_cache: dict[str, dict[str, Any]] = {}

    for file_record in torrent["files"]:
        raw_components = file_record["raw_components"]
        directory_label = SCOPED_DIRECTORY_BYTES.get(raw_components[0])
        if directory_label is None:
            continue
        scoped_files += 1
        if scoped_files > MAX_SCOPED_FILES:
            raise TorrentBracketReconciliationError("torrent exceeds scoped-file cap")
        basename = raw_components[-1]
        parsed = suffix_audio_locator_bytes(basename)
        if parsed is None:
            rejection = _rejection_class(basename)
            if rejection == "format_label_invalid_id_token":
                rejection_counts["format_label_invalid_id_tokens"] += 1
            elif rejection == "audio_only_invalid_id_token":
                rejection_counts["audio_only_invalid_id_tokens"] += 1
            elif rejection == "ambiguous_or_noncanonical_suffix":
                rejection_counts["ambiguous_or_noncanonical_suffixes"] += 1
            continue

        lane = parsed["lane"]
        video_id = parsed["youtube_video_id"]
        if not YOUTUBE_ID_RE.fullmatch(video_id):
            raise TorrentBracketReconciliationError(
                "suffix/audio YouTube ID parser escaped grammar"
            )
        if lane == FORMAT_LABEL_VIDEO_LANE and parsed["file_extension"].encode(
            "ascii"
        ) not in VIDEO_EXTENSIONS:
            raise TorrentBracketReconciliationError(
                "suffix/audio video-extension parser escaped grammar"
            )

        lane_contract = _lane_contract(lane)
        lane_counts[lane] += 1
        directory_counts[directory_label][lane] += 1
        directory_counts[directory_label]["total"] += 1
        distinct_ids.add(video_id)
        lane_distinct_ids[lane].add(video_id)

        resolution = resolution_cache.get(video_id)
        if resolution is None:
            resolution = _youtube_resolution(connection, video_id)
            resolution_cache[video_id] = resolution
        resolution_counts[resolution["resolution_state"]] += 1

        torrent_file_source_id = binding["torrent_file_source_ids"][
            file_record["file_index"]
        ]
        youtube_source_id = resolution["youtube_source_id"]
        target_type = "source" if youtube_source_id else "youtube_video_id"
        target_id = youtube_source_id or video_id
        plan_candidate_id = stable_id(
            "tsc",
            PLANNER_VERSION,
            torrent["torrent_sha256"],
            discovery["discovery_sha256"],
            torrent_file_source_id,
            lane,
            video_id,
        )
        candidates.append(
            {
                "plan_candidate_id": plan_candidate_id,
                "lane_contract": {
                    "candidate_kind": lane_contract["candidate_kind"],
                    "lane": lane,
                    "file_extension": parsed["file_extension"],
                    "format_label": parsed["format_label"],
                    "evidence_basis": lane_contract["evidence_basis"],
                    "confidence": {
                        "profile": lane_contract["confidence_profile"],
                        "calibration_state": "not_calibrated",
                        "raw_score": None,
                        "calibrated_probability": None,
                    },
                    "routing": {
                        "review_route": lane_contract["review_route"],
                        "review_priority": lane_contract["review_priority"],
                        "review_method": lane_contract["review_method"],
                        "automatic_catalog_admission": False,
                    },
                },
                "torrent_manifest_source_id": binding["torrent_manifest_source_id"],
                "torrent_file_source_id": torrent_file_source_id,
                "torrent_file_index": file_record["file_index"],
                "directory_label": directory_label,
                "manifest_path": file_record["manifest_path"],
                "manifest_path_components_base64": [
                    base64.b64encode(component).decode("ascii")
                    for component in raw_components
                ],
                "manifest_path_sha256": _raw_path_sha256(raw_components),
                "byte_count": file_record["byte_count"],
                "youtube_video_id": video_id,
                "locator_target_type": target_type,
                "locator_target_id": target_id,
                "youtube_source_id": youtube_source_id,
                "youtube_recording_id": resolution["youtube_recording_id"],
                "mapped_recording_ids": resolution["mapped_recording_ids"],
                "resolution_state": resolution["resolution_state"],
                "requires_human_review": True,
                "relationship_asserted": False,
                "merge_performed": False,
                "publication_authority": False,
                "payload_downloaded_or_read": False,
            }
        )
        if len(candidates) > MAX_CANDIDATES:
            raise TorrentBracketReconciliationError(
                "torrent exceeds suffix/audio candidate cap"
            )

    candidates.sort(key=lambda value: value["plan_candidate_id"])
    candidate_ids = [value["plan_candidate_id"] for value in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise TorrentBracketReconciliationError(
            "torrent suffix/audio plan has duplicate candidate identities"
        )
    cross_lane_ids = lane_distinct_ids[FORMAT_LABEL_VIDEO_LANE] & lane_distinct_ids[
        AUDIO_ONLY_LANE
    ]
    statistics = {
        "provider_file_records_scanned": torrent["file_count"],
        "scoped_file_records": scoped_files,
        "candidates_total": len(candidates),
        "candidates_by_lane": lane_counts,
        "distinct_youtube_video_ids": len(distinct_ids),
        "distinct_ids_by_lane": {
            lane: len(values) for lane, values in lane_distinct_ids.items()
        },
        "cross_lane_distinct_ids": len(cross_lane_ids),
        "candidates_by_directory": directory_counts,
        "resolution_state_counts": resolution_counts,
        "rejected_suffix_counts": rejection_counts,
        "payload_files_read": 0,
        "payload_bytes_read": 0,
        "catalog_rows_written": 0,
        "review_tasks_created": 0,
        "source_or_recording_mutations": 0,
        "source_relations": 0,
        "recording_relations": 0,
        "recording_merges": 0,
        "publication_decisions": 0,
        "identity_assertions": 0,
        "claims": 0,
    }
    core = {
        "schema_version": 1,
        "plan_kind": PLAN_KIND,
        "planner_version": PLANNER_VERSION,
        "inputs": {
            "torrent_sha256": torrent["torrent_sha256"],
            "torrent_byte_count": len(torrent_body),
            "torrent_filename": resolved_torrent.name,
            "info_hash_sha1": torrent["info_hash_sha1"],
            "discovery_sha256": discovery["discovery_sha256"],
            "discovery_byte_count": discovery["discovery_byte_count"],
            "discovery_filename": resolved_discovery.name,
            "combined_import_input_sha256": combined_input_sha256,
        },
        "catalog_binding": {
            "torrent_manifest_source_id": binding["torrent_manifest_source_id"],
            "torrent_import_batch_id": binding["torrent_import_batch_id"],
            "torrent_importer_name": "torrent_manifest_metadata",
            "torrent_importer_version": binding["torrent_importer_version"],
            "observed_at": discovery["observed_at"],
        },
        "scope": {
            "directory_labels": list(SCOPED_DIRECTORY_LABELS),
            "accepted_lanes": [FORMAT_LABEL_VIDEO_LANE, AUDIO_ONLY_LANE],
        },
        "candidates": candidates,
        "statistics": statistics,
        "policy": {
            "plan_only": True,
            "catalog_admission_implemented": False,
            "sealed_terminal_bracket_lane_modified": False,
            "exact_suffix_grammars_only": True,
            "invalid_id_tokens_accepted": False,
            "ambiguous_suffix_variants_accepted": False,
            "torrent_paths_are_content_truth": False,
            "payload_download_or_read": False,
            "requires_human_review": True,
            "relationship_asserted": False,
            "merge_performed": False,
            "publication_authority": False,
            "capacity_limits": {
                "torrent_bytes": MAX_TORRENT_BYTES,
                "discovery_bytes": MAX_DISCOVERY_BYTES,
                "torrent_files": MAX_TORRENT_FILES,
                "scoped_files": MAX_SCOPED_FILES,
                "candidates": MAX_CANDIDATES,
            },
        },
    }
    plan_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {
        **core,
        "plan_id": f"tsap_{plan_sha256[:32]}",
        "plan_sha256": plan_sha256,
    }


def summarize_torrent_suffix_audio_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Return the path-free default CLI summary."""

    return {
        "valid": True,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "planner_version": plan["planner_version"],
        "torrent_sha256": plan["inputs"]["torrent_sha256"],
        "discovery_sha256": plan["inputs"]["discovery_sha256"],
        "info_hash_sha1": plan["inputs"]["info_hash_sha1"],
        "torrent_manifest_source_id": plan["catalog_binding"][
            "torrent_manifest_source_id"
        ],
        "torrent_import_batch_id": plan["catalog_binding"]["torrent_import_batch_id"],
        "observed_at": plan["catalog_binding"]["observed_at"],
        "scope": plan["scope"],
        "statistics": plan["statistics"],
        "plan_only": True,
        "catalog_admission_implemented": False,
        "requires_human_review": True,
        "relationship_asserted": False,
        "merge_performed": False,
        "publication_authority": False,
    }
