"""Deterministic, read-only planning for selective torrent acquisition.

The planner never opens a BitTorrent session and never reads torrent payload
bytes.  It reconciles exact filename locators against two independently pinned
forms of prior evidence:

* exact YouTube-ID locator rows already present in the catalogue; and
* terminal bracketed YouTube IDs in sealed Archive.org metadata snapshots.

For each remaining usable YouTube ID it chooses the smallest torrent file (and
then the lowest file index as a deterministic tie-break).  That choice is only a
probe candidate.  File indices enter ``selected_files`` after a separate,
credential-free, no-download availability result reports the corresponding
YouTube page unavailable.  Indeterminate probe outcomes remain unselected.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .archive_bracket_reconciler import (
    _payload_document,
    terminal_bracketed_youtube_id,
)
from .archive_metadata_snapshot_importer import validate_archive_metadata_snapshot
from .ids import VIDEO_EXTENSION_RE
from .importers import canonical_json, sha256_bytes
from .torrent_bracket_reconciler import (
    MAX_DISCOVERY_BYTES,
    MAX_SCOPED_FILES,
    MAX_TORRENT_BYTES,
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
    _strict_json_bytes,
    _timestamp,
    _validate_discovery,
    terminal_bracketed_youtube_id_bytes,
)
from .torrent_suffix_audio_planner import (
    FORMAT_LABEL_VIDEO_LANE,
    suffix_audio_locator_bytes,
)


PLAN_KIND = "torrent_selective_acquisition_plan"
PLANNER_VERSION = "torrent_selective_acquisition_plan_v1"
PROBE_KIND = "youtube_no_download_availability_probe_v1"
PROBE_REQUEST_KIND = "youtube_no_download_availability_request_v1"

MAX_ARCHIVE_SNAPSHOTS = 64
MAX_ARCHIVE_EVIDENCE_ROWS = 250_000
MAX_CATALOG_EVIDENCE_ROWS = 250_000
MAX_PROBE_BYTES = 16 * 1024 * 1024
MAX_PROBE_TARGETS = 50_000
MAX_MANUAL_REVIEW_FILES = 10_000

CATALOG_NAMESPACES = (
    "youtube_video_id",
    "youtube_video_id_candidate",
)
CATALOG_EXACT_BASES = {
    "youtube_video_id": frozenset(
        {
            "Validated yt-dlp info JSON",
            "Captured YouTube watch-page inventory",
        }
    ),
    "youtube_video_id_candidate": frozenset(
        {"Eleven-character filename suffix"}
    ),
}

PROBE_FLAGS = (
    "--ignore-config",
    "--skip-download",
    "--no-playlist",
    "--no-warnings",
    "--dump-single-json",
)
AVAILABLE_CODES = frozenset({"metadata_resolved"})
UNAVAILABLE_CODES = frozenset({"private", "removed", "video_unavailable"})
INDETERMINATE_CODES = frozenset(
    {"sign_in_required", "network_error", "rate_limited", "extractor_error"}
)
PROBE_CODES_BY_STATE = {
    "available": AVAILABLE_CODES,
    "unavailable": UNAVAILABLE_CODES,
    "indeterminate": INDETERMINATE_CODES,
}

VIDEO_SUFFIX_RE = re.compile(rb"\.([A-Za-z0-9]{1,8})$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class TorrentSelectivePlannerError(TorrentBracketReconciliationError):
    """A selective plan input failed closed."""


def _exact_keys(value: object, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        actual = set(value) if isinstance(value, dict) else set()
        raise TorrentSelectivePlannerError(
            f"{label} has unknown shape "
            f"(missing={sorted(keys - actual)}, extra={sorted(actual - keys)})"
        )
    return value


def _torrent_video_locator(filename: bytes) -> tuple[str, str] | None:
    """Return ``(video_id, lane)`` for the two reviewed video grammars."""

    terminal = terminal_bracketed_youtube_id_bytes(filename)
    if terminal is not None:
        return terminal, "terminal_bracket_video"
    suffix = suffix_audio_locator_bytes(filename)
    if suffix is not None and suffix["lane"] == FORMAT_LABEL_VIDEO_LANE:
        return suffix["youtube_video_id"], "format_label_480p_video"
    return None


def _is_scoped_video(filename: bytes) -> bool:
    match = VIDEO_SUFFIX_RE.search(filename)
    return bool(match and match.group(1).lower() in VIDEO_EXTENSIONS)


def _catalog_exact_locator_evidence(
    connection: sqlite3.Connection,
) -> tuple[set[str], dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT external_id_id, object_type, object_id, namespace, external_value,
               confidence_state, basis, source_id,
               current_external_id_observation_id
        FROM external_ids
        WHERE namespace IN ('youtube_video_id', 'youtube_video_id_candidate')
        ORDER BY namespace, external_value, external_id_id
        LIMIT ?
        """,
        (MAX_CATALOG_EVIDENCE_ROWS + 1,),
    ).fetchall()
    if len(rows) > MAX_CATALOG_EVIDENCE_ROWS:
        raise TorrentSelectivePlannerError(
            "catalogue YouTube locator evidence exceeds the bounded row cap"
        )
    accepted: list[dict[str, Any]] = []
    ignored_basis = Counter()
    for row in rows:
        namespace = row["namespace"]
        video_id = row["external_value"]
        if (
            row["object_type"] != "recording"
            or not isinstance(row["object_id"], str)
            or not isinstance(row["source_id"], str)
            or row["confidence_state"] not in {"metadata_only", "reviewed"}
            or not isinstance(video_id, str)
            or YOUTUBE_ID_RE.fullmatch(video_id) is None
        ):
            raise TorrentSelectivePlannerError(
                "catalogue YouTube locator row escaped its exact identity contract"
            )
        if row["basis"] not in CATALOG_EXACT_BASES[namespace]:
            ignored_basis[(namespace, str(row["basis"]))] += 1
            continue
        accepted.append(
            {
                "external_id_id": row["external_id_id"],
                "object_type": row["object_type"],
                "object_id": row["object_id"],
                "namespace": namespace,
                "external_value": video_id,
                "confidence_state": row["confidence_state"],
                "basis": row["basis"],
                "source_id": row["source_id"],
                "current_external_id_observation_id": row[
                    "current_external_id_observation_id"
                ],
            }
        )
    identities = {row["external_value"] for row in accepted}
    evidence_sha256 = sha256_bytes(canonical_json(accepted).encode("utf-8"))
    return identities, {
        "accepted_row_count": len(accepted),
        "distinct_youtube_video_ids": len(identities),
        "accepted_rows_by_namespace": dict(
            sorted(Counter(row["namespace"] for row in accepted).items())
        ),
        "ignored_unapproved_basis_rows": sum(ignored_basis.values()),
        "exact_locator_evidence_sha256": evidence_sha256,
    }


def _archive_exact_locator_evidence(
    snapshot_paths: list[Path],
) -> tuple[set[str], list[dict[str, Any]], dict[str, Any]]:
    if not snapshot_paths or len(snapshot_paths) > MAX_ARCHIVE_SNAPSHOTS:
        raise TorrentSelectivePlannerError(
            "one to 64 sealed Archive.org snapshots are required"
        )
    snapshots = [validate_archive_metadata_snapshot(Path(path)) for path in snapshot_paths]
    identities = [(value["snapshot_id"], value["_sha256"]) for value in snapshots]
    if len({item[0] for item in identities}) != len(identities):
        raise TorrentSelectivePlannerError("Archive.org snapshot IDs must be unique")
    snapshots.sort(key=lambda value: (value["snapshot_id"], value["_sha256"]))

    evidence: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for snapshot in snapshots:
        for item in snapshot["items"]:
            document = _payload_document(item)
            for file_record in document["files"]:
                if not isinstance(file_record, dict):
                    raise TorrentSelectivePlannerError(
                        "validated Archive.org file record changed shape"
                    )
                filename = file_record.get("name")
                title = file_record.get("title")
                if (
                    not isinstance(filename, str)
                    or VIDEO_EXTENSION_RE.search(filename) is None
                ):
                    continue
                filename_id = terminal_bracketed_youtube_id(filename)
                title_id = terminal_bracketed_youtube_id(title)
                if filename_id is None and title_id is None:
                    continue
                common = {
                    "snapshot_id": snapshot["snapshot_id"],
                    "snapshot_sha256": snapshot["_sha256"],
                    "archive_item": item["identifier"],
                    "archive_filename": filename,
                    "archive_title": title if isinstance(title, str) else None,
                }
                if (
                    filename_id is not None
                    and title_id is not None
                    and filename_id != title_id
                ):
                    conflicts.append(
                        {
                            **common,
                            "filename_youtube_video_id": filename_id,
                            "title_youtube_video_id": title_id,
                        }
                    )
                    if len(evidence) + len(conflicts) > MAX_ARCHIVE_EVIDENCE_ROWS:
                        raise TorrentSelectivePlannerError(
                            "Archive.org evidence exceeds the bounded row cap"
                        )
                    continue
                evidence.append(
                    {
                        **common,
                        "youtube_video_id": filename_id or title_id,
                        "evidence_basis": (
                            "filename_and_title_terminal_bracket"
                            if filename_id is not None and title_id is not None
                            else (
                                "filename_terminal_bracket"
                                if filename_id is not None
                                else "title_terminal_bracket"
                            )
                        ),
                    }
                )
                if len(evidence) + len(conflicts) > MAX_ARCHIVE_EVIDENCE_ROWS:
                    raise TorrentSelectivePlannerError(
                        "Archive.org evidence exceeds the bounded row cap"
                    )
    evidence.sort(
        key=lambda row: (
            row["snapshot_id"],
            row["archive_item"],
            row["archive_filename"],
            row["youtube_video_id"],
        )
    )
    conflicts.sort(
        key=lambda row: (
            row["snapshot_id"], row["archive_item"], row["archive_filename"]
        )
    )
    video_ids = {row["youtube_video_id"] for row in evidence}
    snapshot_bindings = [
        {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_sha256": snapshot["_sha256"],
            "observed_at": snapshot["observed_at"],
            "item_count": len(snapshot["items"]),
        }
        for snapshot in snapshots
    ]
    return video_ids, snapshot_bindings, {
        "evidence_row_count": len(evidence),
        "distinct_youtube_video_ids": len(video_ids),
        "conflicting_rows_excluded": len(conflicts),
        "exact_locator_evidence_sha256": sha256_bytes(
            canonical_json(evidence).encode("utf-8")
        ),
        "conflict_evidence_sha256": sha256_bytes(
            canonical_json(conflicts).encode("utf-8")
        ),
    }


def _probe_request(
    candidate_ids: list[str], *, evidence_binding_sha256: str
) -> dict[str, Any]:
    targets = [
        {
            "youtube_video_id": video_id,
            "canonical_url": f"https://www.youtube.com/watch?v={video_id}",
        }
        for video_id in candidate_ids
    ]
    core = {
        "schema_version": 1,
        "request_kind": PROBE_REQUEST_KIND,
        "evidence_binding_sha256": evidence_binding_sha256,
        "producer_contract": {
            "tool": "yt-dlp",
            "invocation_flags": list(PROBE_FLAGS),
            "one_target_per_process": True,
        },
        "network_policy": {
            "cookies_sent": False,
            "authorization_sent": False,
            "media_payload_downloaded": False,
            "playlist_expansion": False,
        },
        "targets": targets,
    }
    digest = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {**core, "request_sha256": digest}


def _validate_availability_probe(
    path: Path, request: dict[str, Any]
) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    resolved, body = _stable_file(
        Path(path), MAX_PROBE_BYTES, "YouTube no-download availability probe"
    )
    value = _strict_json_bytes(body, "YouTube no-download availability probe")
    _exact_keys(
        value,
        {
            "schema_version",
            "probe_kind",
            "request_sha256",
            "observed_at",
            "producer",
            "network_policy",
            "outcomes",
        },
        "availability probe",
    )
    if value["schema_version"] != 1 or value["probe_kind"] != PROBE_KIND:
        raise TorrentSelectivePlannerError("availability probe identity is unsupported")
    if value["request_sha256"] != request["request_sha256"]:
        raise TorrentSelectivePlannerError(
            "availability probe does not match the exact probe request"
        )
    observed_at = _timestamp(value["observed_at"], "availability probe observed_at")

    producer = _exact_keys(
        value["producer"],
        {"tool", "version", "executable_sha256", "invocation_flags"},
        "availability probe producer",
    )
    if (
        producer["tool"] != "yt-dlp"
        or not isinstance(producer["version"], str)
        or not producer["version"]
        or len(producer["version"]) > 128
        or not isinstance(producer["executable_sha256"], str)
        or SHA256_RE.fullmatch(producer["executable_sha256"]) is None
        or producer["invocation_flags"] != list(PROBE_FLAGS)
    ):
        raise TorrentSelectivePlannerError(
            "availability probe producer escaped the pinned no-download contract"
        )
    network = _exact_keys(
        value["network_policy"],
        {
            "cookies_sent",
            "authorization_sent",
            "media_payload_downloaded",
            "playlist_expansion",
        },
        "availability probe network policy",
    )
    if network != request["network_policy"]:
        raise TorrentSelectivePlannerError(
            "availability probe used credentials, payload download, or playlist expansion"
        )

    outcomes = value["outcomes"]
    targets = request["targets"]
    if (
        not isinstance(outcomes, list)
        or len(outcomes) != len(targets)
        or len(outcomes) > MAX_PROBE_TARGETS
    ):
        raise TorrentSelectivePlannerError(
            "availability probe must contain exactly one outcome per requested ID"
        )
    by_id: dict[str, dict[str, str]] = {}
    normalized: list[dict[str, str]] = []
    for index, (raw, target) in enumerate(zip(outcomes, targets, strict=True)):
        row = _exact_keys(
            raw,
            {
                "youtube_video_id",
                "canonical_url",
                "availability_state",
                "evidence_code",
            },
            f"availability probe outcome {index}",
        )
        video_id = row["youtube_video_id"]
        state = row["availability_state"]
        code = row["evidence_code"]
        if (
            video_id != target["youtube_video_id"]
            or row["canonical_url"] != target["canonical_url"]
            or state not in PROBE_CODES_BY_STATE
            or code not in PROBE_CODES_BY_STATE[state]
            or video_id in by_id
        ):
            raise TorrentSelectivePlannerError(
                "availability probe outcomes are incomplete, reordered, or inconsistent"
            )
        normalized_row = {
            "youtube_video_id": video_id,
            "canonical_url": row["canonical_url"],
            "availability_state": state,
            "evidence_code": code,
        }
        normalized.append(normalized_row)
        by_id[video_id] = normalized_row
    return by_id, {
        "probe_filename": resolved.name,
        "probe_sha256": sha256_bytes(body),
        "probe_byte_count": len(body),
        "request_sha256": request["request_sha256"],
        "observed_at": observed_at,
        "producer": producer,
        "network_policy": network,
        "outcome_counts": dict(
            sorted(Counter(row["availability_state"] for row in normalized).items())
        ),
    }


def build_torrent_selective_acquisition_plan(
    connection: sqlite3.Connection,
    torrent_path: Path,
    discovery_metadata_path: Path,
    archive_snapshot_paths: list[Path],
    *,
    availability_probe_path: Path | None = None,
) -> dict[str, Any]:
    """Build a reproducible plan in one catalogue snapshot without any writes."""

    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN")
    try:
        return _build_torrent_selective_acquisition_plan_in_snapshot(
            connection,
            torrent_path,
            discovery_metadata_path,
            archive_snapshot_paths,
            availability_probe_path=availability_probe_path,
        )
    finally:
        if owns_transaction and connection.in_transaction:
            connection.rollback()


def _build_torrent_selective_acquisition_plan_in_snapshot(
    connection: sqlite3.Connection,
    torrent_path: Path,
    discovery_metadata_path: Path,
    archive_snapshot_paths: list[Path],
    *,
    availability_probe_path: Path | None,
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
        raise TorrentSelectivePlannerError("torrent and discovery inputs must differ")
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

    catalog_ids, catalog_binding = _catalog_exact_locator_evidence(connection)
    archive_ids, archive_bindings, archive_binding = _archive_exact_locator_evidence(
        list(archive_snapshot_paths)
    )
    evidence_binding = {
        "torrent_sha256": torrent["torrent_sha256"],
        "discovery_sha256": discovery["discovery_sha256"],
        "catalog_exact_locator_evidence_sha256": catalog_binding[
            "exact_locator_evidence_sha256"
        ],
        "archive_exact_locator_evidence_sha256": archive_binding[
            "exact_locator_evidence_sha256"
        ],
        "archive_conflict_evidence_sha256": archive_binding[
            "conflict_evidence_sha256"
        ],
        "archive_snapshots": archive_bindings,
    }
    evidence_binding_sha256 = sha256_bytes(
        canonical_json(evidence_binding).encode("utf-8")
    )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    malformed: list[dict[str, Any]] = []
    scoped_files = 0
    scoped_video_files = 0
    inventory_digest_rows: list[dict[str, Any]] = []
    for file_record in torrent["files"]:
        components = file_record["raw_components"]
        directory_label = SCOPED_DIRECTORY_BYTES.get(components[0])
        if directory_label is None:
            continue
        scoped_files += 1
        if scoped_files > MAX_SCOPED_FILES:
            raise TorrentSelectivePlannerError("torrent exceeds scoped-file cap")
        basename = components[-1]
        if not _is_scoped_video(basename):
            continue
        scoped_video_files += 1
        raw_components = [
            base64.b64encode(component).decode("ascii") for component in components
        ]
        common = {
            "torrent_file_index": file_record["file_index"],
            "directory_label": directory_label,
            "manifest_path": file_record["manifest_path"],
            "manifest_path_components_base64": raw_components,
            "manifest_path_sha256": _raw_path_sha256(components),
            "byte_count": file_record["byte_count"],
        }
        locator = _torrent_video_locator(basename)
        if locator is None:
            malformed.append(
                {
                    **common,
                    "review_reason": "scoped_video_filename_has_no_accepted_youtube_id_grammar",
                }
            )
            inventory_digest_rows.append({**common, "youtube_video_id": None})
            if len(malformed) > MAX_MANUAL_REVIEW_FILES:
                raise TorrentSelectivePlannerError(
                    "torrent exceeds malformed-video manual-review cap"
                )
            continue
        video_id, lane = locator
        if YOUTUBE_ID_RE.fullmatch(video_id) is None:
            raise TorrentSelectivePlannerError("torrent locator parser escaped ID grammar")
        row = {**common, "youtube_video_id": video_id, "locator_lane": lane}
        grouped[video_id].append(row)
        inventory_digest_rows.append(row)

    malformed.sort(key=lambda row: row["torrent_file_index"])
    inventory_digest_rows.sort(key=lambda row: row["torrent_file_index"])
    candidates: list[dict[str, Any]] = []
    covered_counts = Counter()
    covered_ids: list[dict[str, str]] = []
    for video_id in sorted(grouped):
        files = sorted(
            grouped[video_id],
            key=lambda row: (row["byte_count"], row["torrent_file_index"]),
        )
        in_catalog = video_id in catalog_ids
        in_archive = video_id in archive_ids
        if in_catalog or in_archive:
            basis = (
                "catalog_and_archive_exact_locator"
                if in_catalog and in_archive
                else (
                    "catalog_exact_locator"
                    if in_catalog
                    else "archive_exact_locator"
                )
            )
            covered_counts[basis] += 1
            covered_ids.append({"youtube_video_id": video_id, "coverage_basis": basis})
            continue
        smallest = files[0]
        candidates.append(
            {
                "youtube_video_id": video_id,
                "smallest_rendition_file_index": smallest["torrent_file_index"],
                "smallest_rendition_byte_count": smallest["byte_count"],
                "directory_label": smallest["directory_label"],
                "manifest_path": smallest["manifest_path"],
                "manifest_path_components_base64": smallest[
                    "manifest_path_components_base64"
                ],
                "manifest_path_sha256": smallest["manifest_path_sha256"],
                "locator_lane": smallest["locator_lane"],
                "rendition_file_count": len(files),
                "rendition_file_indices": sorted(
                    row["torrent_file_index"] for row in files
                ),
                "availability_state": "awaiting_probe",
            }
        )
    covered_ids.sort(key=lambda row: row["youtube_video_id"])
    candidate_ids = [row["youtube_video_id"] for row in candidates]
    request = _probe_request(
        candidate_ids, evidence_binding_sha256=evidence_binding_sha256
    )

    probe_by_id: dict[str, dict[str, str]] | None = None
    probe_binding: dict[str, Any] | None = None
    if availability_probe_path is not None:
        probe_by_id, probe_binding = _validate_availability_probe(
            Path(availability_probe_path), request
        )

    selected: list[dict[str, Any]] = []
    final_candidates: list[dict[str, Any]] = []
    for row in candidates:
        outcome = probe_by_id.get(row["youtube_video_id"]) if probe_by_id else None
        state = outcome["availability_state"] if outcome else "awaiting_probe"
        finalized = {
            **row,
            "availability_state": state,
            "availability_evidence_code": (
                outcome["evidence_code"] if outcome else None
            ),
        }
        final_candidates.append(finalized)
        if state == "unavailable":
            selected.append(
                {
                    "youtube_video_id": row["youtube_video_id"],
                    "torrent_file_index": row["smallest_rendition_file_index"],
                    "byte_count": row["smallest_rendition_byte_count"],
                    "manifest_path": row["manifest_path"],
                    "manifest_path_sha256": row["manifest_path_sha256"],
                    "availability_evidence_code": outcome["evidence_code"],
                    "selection_reason": (
                        "smallest_rendition_after_exact_archive_catalog_exclusion_"
                        "and_no_download_youtube_unavailability_probe"
                    ),
                }
            )

    selected.sort(key=lambda row: row["torrent_file_index"])
    upper_candidate_bytes = sum(
        row["smallest_rendition_byte_count"] for row in final_candidates
    )
    malformed_bytes = sum(row["byte_count"] for row in malformed)
    selected_bytes = sum(row["byte_count"] for row in selected)
    statistics = {
        "provider_file_records_scanned": torrent["file_count"],
        "scoped_file_records": scoped_files,
        "scoped_video_file_records": scoped_video_files,
        "usable_video_file_records": sum(len(rows) for rows in grouped.values()),
        "distinct_usable_youtube_video_ids": len(grouped),
        "covered_distinct_ids": sum(covered_counts.values()),
        "covered_ids_by_basis": dict(sorted(covered_counts.items())),
        "probe_candidate_distinct_ids": len(final_candidates),
        "probe_candidate_file_records": sum(
            row["rendition_file_count"] for row in final_candidates
        ),
        "probe_candidate_smallest_renditions_bytes": upper_candidate_bytes,
        "malformed_manual_review_files": len(malformed),
        "malformed_manual_review_bytes": malformed_bytes,
        "preprobe_review_upper_bound_bytes": upper_candidate_bytes + malformed_bytes,
        "selected_file_count": len(selected),
        "selected_payload_bytes": selected_bytes,
        "payload_files_read": 0,
        "payload_bytes_read": 0,
        "catalog_rows_written": 0,
        "torrent_sessions_started": 0,
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
            "torrent_video_inventory_sha256": sha256_bytes(
                canonical_json(inventory_digest_rows).encode("utf-8")
            ),
        },
        "catalog_binding": {
            "torrent_manifest_source_id": binding["torrent_manifest_source_id"],
            "torrent_import_batch_id": binding["torrent_import_batch_id"],
            "torrent_importer_name": "torrent_manifest_metadata",
            "torrent_importer_version": binding["torrent_importer_version"],
            "observed_at": discovery["observed_at"],
            "exact_locator_evidence": catalog_binding,
        },
        "archive_binding": {
            "snapshots": archive_bindings,
            "exact_locator_evidence": archive_binding,
        },
        "evidence_binding_sha256": evidence_binding_sha256,
        "coverage": {
            "covered_ids_sha256": sha256_bytes(
                canonical_json(covered_ids).encode("utf-8")
            ),
            "covered_ids_by_basis": dict(sorted(covered_counts.items())),
        },
        "availability_probe_request": request,
        "availability_probe": probe_binding,
        "probe_candidates": final_candidates,
        "malformed_manual_review": malformed,
        "selected_files": selected,
        "selected_torrent_file_indices": [
            row["torrent_file_index"] for row in selected
        ],
        "statistics": statistics,
        "policy": {
            "read_only_plan": True,
            "torrent_client_invoked": False,
            "torrent_swarm_joined": False,
            "payload_downloaded_or_read": False,
            "smallest_rendition_per_id": True,
            "exact_catalog_locator_is_content_identity": False,
            "exact_archive_locator_is_content_identity": False,
            "availability_probe_downloads_media": False,
            "availability_probe_uses_credentials": False,
            "indeterminate_probe_outcomes_selected": False,
            "malformed_paths_selected": False,
            "operator_review_required_before_client_use": True,
            "selective_file_index_client_required": True,
            "publication_authority": False,
        },
    }
    plan_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {
        **core,
        "plan_id": f"tslp_{plan_sha256[:32]}",
        "plan_sha256": plan_sha256,
    }


def summarize_torrent_selective_acquisition_plan(
    plan: dict[str, Any]
) -> dict[str, Any]:
    """Return a path-free summary suitable for the default CLI output."""

    return {
        "valid": True,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "planner_version": plan["planner_version"],
        "torrent_sha256": plan["inputs"]["torrent_sha256"],
        "info_hash_sha1": plan["inputs"]["info_hash_sha1"],
        "evidence_binding_sha256": plan["evidence_binding_sha256"],
        "availability_probe_request_sha256": plan["availability_probe_request"][
            "request_sha256"
        ],
        "availability_probe": plan["availability_probe"],
        "statistics": plan["statistics"],
        "read_only_plan": True,
        "torrent_swarm_joined": False,
        "payload_downloaded_or_read": False,
        "operator_review_required_before_client_use": True,
        "selective_file_index_client_required": True,
        "publication_authority": False,
    }


def publish_private_torrent_selective_acquisition_plan(
    plan: dict[str, Any], output_path: Path
) -> dict[str, Any]:
    """Atomically publish one owner-read-only full plan without overwriting.

    Full plans contain raw manifest paths and probe targets.  This writer keeps
    those details out of stdout, refuses ambiguous/symlinked destinations, and
    uses a hard-link publication boundary so an existing file is never replaced.
    """

    requested = Path(output_path)
    if not requested.is_absolute() or requested.name in {"", ".", ".."}:
        raise TorrentSelectivePlannerError(
            "private full-plan output must be an absolute file path"
        )
    try:
        resolved_parent = requested.parent.resolve(strict=True)
    except OSError as error:
        raise TorrentSelectivePlannerError(
            "private full-plan output parent does not exist"
        ) from error
    if resolved_parent != requested.parent or not resolved_parent.is_dir():
        raise TorrentSelectivePlannerError(
            "private full-plan output parent must be a real directory"
        )
    destination = resolved_parent / requested.name
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    except OSError as error:
        raise TorrentSelectivePlannerError(
            "private full-plan output cannot be inspected"
        ) from error
    else:
        raise TorrentSelectivePlannerError(
            "private full-plan output already exists"
        )

    body = (
        json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=resolved_parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o400)
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written < 1:
                raise TorrentSelectivePlannerError(
                    "private full-plan output write made no progress"
                )
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as error:
            raise TorrentSelectivePlannerError(
                "private full-plan output already exists"
            ) from error
        directory_descriptor = os.open(
            resolved_parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)

    return {
        "valid": True,
        "full_plan_written": True,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "output_byte_count": len(body),
        "output_sha256": hashlib.sha256(body).hexdigest(),
        "output_mode": "0400",
        "path_disclosed": False,
        "torrent_client_invoked": False,
        "torrent_swarm_joined": False,
        "payload_downloaded_or_read": False,
    }
