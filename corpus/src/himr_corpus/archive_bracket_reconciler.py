"""Fail-closed review candidates for bracketed YouTube IDs in Archive.org metadata.

This lane independently re-derives terminal ``[YouTubeID]`` suffixes from a sealed
Archive.org snapshot that has already crossed the strict metadata importer.  It
does not rewrite sources, attach recordings, create recording relations, or merge
anything.  Every possible reconciliation remains private immutable evidence with
an explicit human review task.
"""

from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .archive_metadata_snapshot_importer import (
    MAX_PAYLOAD_BYTES,
    _stable_read,
    _strict_json_loads,
    validate_archive_metadata_snapshot,
)
from .db import transaction
from .ids import VIDEO_EXTENSION_RE, recording_id, source_id, stable_id
from .importers import (
    _begin_batch,
    _complete_batch,
    canonical_json,
    sha256_bytes,
)


IMPORTER_NAME = "archive_bracket_reconciliation_v1"
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
TERMINAL_BRACKET_RE = re.compile(r"\[([A-Za-z0-9_-]{11})\]$")
MAX_TOTAL_PROVIDER_FILE_RECORDS = 250_000
MAX_BRACKET_EVIDENCE_FILES = 50_000
MAX_DISTINCT_YOUTUBE_IDS = 25_000
MAX_RECONCILIATION_CANDIDATES = 100_000
MAX_CONFLICT_ISSUES = 10_000
ALLOWED_WRITE_TABLES = frozenset(
    {
        "import_batches",
        "import_observations",
        "match_candidates",
        "review_tasks",
        "archive_bracket_reconciliation_imports",
        "archive_bracket_youtube_candidates",
        "archive_bracket_reconciliation_issues",
    }
)


class ArchiveBracketReconciliationError(ValueError):
    pass


def terminal_bracketed_youtube_id(value: object) -> str | None:
    """Return only an exact terminal bracketed ID, optionally before a video suffix.

    The function deliberately does not trim whitespace, accept bare/dash IDs, or
    search in the middle of a string.  Archive derivatives ending in ``.ia.mp4``
    are treated as one known compound video suffix.
    """

    if not isinstance(value, str) or not value or len(value) > 4096 or "\x00" in value:
        return None
    base = unicodedata.normalize("NFC", value)
    extension = VIDEO_EXTENSION_RE.search(base)
    if extension:
        base = base[: extension.start()]
        if base.lower().endswith(".ia"):
            base = base[:-3]
    match = TERMINAL_BRACKET_RE.search(base)
    return match.group(1) if match and YOUTUBE_ID_RE.fullmatch(match.group(1)) else None


def _payload_document(item: dict[str, Any]) -> dict[str, Any]:
    payload = _stable_read(
        item["_payload_path"],
        min(int(item["byte_count"]), MAX_PAYLOAD_BYTES),
        f"Archive.org reconciliation payload for {item['identifier']}",
        sealed=True,
    )
    document = _strict_json_loads(
        payload, f"Archive.org reconciliation payload for {item['identifier']}"
    )
    if not isinstance(document, dict) or not isinstance(document.get("files"), list):
        raise ArchiveBracketReconciliationError("validated Archive.org payload changed shape")
    return document


def _require_strict_snapshot_import(
    connection: sqlite3.Connection, snapshot: dict[str, Any]
) -> None:
    for item in snapshot["items"]:
        item_source = source_id("internet_archive", "archive_item", item["identifier"])
        source = connection.execute(
            """
            SELECT platform, source_kind, native_id
            FROM sources WHERE source_id = ?
            """,
            (item_source,),
        ).fetchone()
        if source is None or tuple(source) != (
            "internet_archive",
            "archive_item",
            item["identifier"],
        ):
            raise ArchiveBracketReconciliationError(
                "snapshot must be admitted by import-archive-metadata-snapshot first"
            )
        evidence = connection.execute(
            """
            SELECT metadata_json
            FROM source_snapshots
            WHERE source_id = ? AND observed_at = ? AND http_status = 200
              AND payload_sha256 = ? AND request_url = ? AND final_url = ?
            """,
            (
                item_source,
                item["observed_at"],
                item["payload_sha256"],
                item["request_url"],
                item["final_url"],
            ),
        ).fetchone()
        if evidence is None:
            raise ArchiveBracketReconciliationError(
                "strict Archive.org capture evidence is missing for this snapshot"
            )
        try:
            metadata = json.loads(evidence["metadata_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise ArchiveBracketReconciliationError(
                "strict Archive.org capture evidence metadata is invalid"
            ) from error
        if (
            metadata.get("archive_metadata_snapshot_id") != snapshot["snapshot_id"]
            or metadata.get("archive_metadata_snapshot_sha256") != snapshot["_sha256"]
            or metadata.get("publication_authority") is not False
        ):
            raise ArchiveBracketReconciliationError(
                "strict Archive.org capture evidence differs from the sealed snapshot"
            )


def _archive_projection(
    connection: sqlite3.Connection, item: str, filename: str
) -> tuple[str, str]:
    native_id = f"{item}/{filename}"
    archive_source = source_id(
        "internet_archive", "archive_media_file", native_id
    )
    source = connection.execute(
        """
        SELECT platform, source_kind, native_id
        FROM sources WHERE source_id = ?
        """,
        (archive_source,),
    ).fetchone()
    if source is None or tuple(source) != (
        "internet_archive",
        "archive_media_file",
        native_id,
    ):
        raise ArchiveBracketReconciliationError(
            f"Archive file projection is missing for {native_id!r}"
        )
    recordings = connection.execute(
        """
        SELECT DISTINCT link.recording_id
        FROM recording_sources AS link
        JOIN recordings AS recording ON recording.recording_id = link.recording_id
        WHERE link.source_id = ?
          AND link.mapping_role IN ('archive_original_file', 'archive_derivative_file')
        ORDER BY link.recording_id
        """,
        (archive_source,),
    ).fetchall()
    if len(recordings) != 1:
        raise ArchiveBracketReconciliationError(
            f"Archive file {native_id!r} lacks one deterministic native recording projection"
        )
    return archive_source, recordings[0]["recording_id"]


def _youtube_resolution(connection: sqlite3.Connection, video_id: str) -> dict[str, Any]:
    expected_source = source_id("youtube", "youtube_video", video_id)
    source = connection.execute(
        """
        SELECT source_id, platform, source_kind, native_id
        FROM sources WHERE source_id = ?
        """,
        (expected_source,),
    ).fetchone()
    if source is None:
        return {
            "resolution_state": "missing_native_source",
            "youtube_source_id": None,
            "youtube_recording_id": None,
            "mapped_recording_ids": [],
        }
    if tuple(source) != (
        expected_source,
        "youtube",
        "youtube_video",
        video_id,
    ):
        raise ArchiveBracketReconciliationError(
            f"deterministic YouTube source identity collision for {video_id}"
        )
    mapped = [
        row["recording_id"]
        for row in connection.execute(
            """
            SELECT DISTINCT recording_id
            FROM recording_sources WHERE source_id = ?
            ORDER BY recording_id
            """,
            (expected_source,),
        ).fetchall()
    ]
    expected_recording = recording_id(f"youtube:video:{video_id}")
    canonical = connection.execute(
        "SELECT canonical_key FROM recordings WHERE recording_id = ?",
        (expected_recording,),
    ).fetchone()
    if (
        canonical is not None
        and canonical["canonical_key"] == f"youtube:video:{video_id}"
        and mapped == [expected_recording]
    ):
        return {
            "resolution_state": "unique_native_recording",
            "youtube_source_id": expected_source,
            "youtube_recording_id": expected_recording,
            "mapped_recording_ids": mapped,
        }
    return {
        "resolution_state": "native_source_without_unique_recording",
        "youtube_source_id": expected_source,
        "youtube_recording_id": None,
        "mapped_recording_ids": mapped,
    }


def _review_task(
    *, task_kind: str, target_type: str, target_id: str, reason: str, priority: int,
    observed_at: str,
) -> dict[str, Any]:
    return {
        "review_task_id": stable_id("rtk", task_kind, target_type, target_id),
        "task_kind": task_kind,
        "target_type": target_type,
        "target_id": target_id,
        "reason": reason,
        "priority": priority,
        "created_at": observed_at,
    }


def _candidate(
    *, snapshot: dict[str, Any], candidate_kind: str, youtube_video_id: str,
    archive_source_id: str | None, archive_recording_id: str,
    youtube_source_id: str | None, youtube_recording_id: str | None,
    comparison_archive_recording_id: str | None, evidence_basis: str,
    resolution_state: str, evidence: dict[str, Any], left_type: str, left_id: str,
    right_type: str, right_id: str, match_method: str,
) -> dict[str, Any]:
    match_id = stable_id(
        "mat",
        "archive_bracket_reconciliation_v1",
        snapshot["snapshot_id"],
        candidate_kind,
        left_type,
        left_id,
        right_type,
        right_id,
        youtube_video_id,
    )
    generic_metadata = {
        "schema_version": 1,
        "archive_metadata_snapshot_id": snapshot["snapshot_id"],
        "archive_metadata_snapshot_sha256": snapshot["_sha256"],
        "candidate_kind": candidate_kind,
        "youtube_video_id": youtube_video_id,
        "evidence_basis": evidence_basis,
        "resolution_state": resolution_state,
        "calibration_state": "not_calibrated",
        "requires_human_review": True,
        "relationship_asserted": False,
        "merge_performed": False,
        "provider_fields_are_content_truth": False,
        "publication_authority": False,
    }
    task = _review_task(
        task_kind="archive_bracket_reconciliation_candidate",
        target_type="match_candidate",
        target_id=match_id,
        reason=(
            f"Review terminal bracketed YouTube ID {youtube_video_id} candidate; "
            "provider filename/title metadata is a locator hint, not proof of identical content"
        ),
        priority=65 if resolution_state == "unique_native_recording" else 55,
        observed_at=snapshot["observed_at"],
    )
    return {
        "generic": {
            "match_candidate_id": match_id,
            "left_object_type": left_type,
            "left_object_id": left_id,
            "right_object_type": right_type,
            "right_object_id": right_id,
            "match_method": match_method,
            "raw_score": None,
            "calibrated_probability": None,
            "decision_state": "candidate",
            "metadata_json": generic_metadata,
        },
        "evidence": {
            "match_candidate_id": match_id,
            "review_task_id": task["review_task_id"],
            "candidate_kind": candidate_kind,
            "youtube_video_id": youtube_video_id,
            "archive_source_id": archive_source_id,
            "archive_recording_id": archive_recording_id,
            "youtube_source_id": youtube_source_id,
            "youtube_recording_id": youtube_recording_id,
            "comparison_archive_recording_id": comparison_archive_recording_id,
            "evidence_basis": evidence_basis,
            "resolution_state": resolution_state,
            "requires_human_review": 1,
            "relationship_asserted": 0,
            "merge_performed": 0,
            "evidence_json": evidence,
        },
        "review_task": task,
    }


def build_archive_bracket_reconciliation_plan(
    connection: sqlite3.Connection, snapshot_path: Path
) -> dict[str, Any]:
    """Build a deterministic private review plan without changing the catalog."""

    snapshot = validate_archive_metadata_snapshot(Path(snapshot_path).resolve())
    _require_strict_snapshot_import(connection, snapshot)
    total_provider_files = sum(int(item["file_count"]) for item in snapshot["items"])
    if total_provider_files > MAX_TOTAL_PROVIDER_FILE_RECORDS:
        raise ArchiveBracketReconciliationError(
            "snapshot exceeds the total provider-file reconciliation cap"
        )
    rows: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    resolution_cache: dict[str, dict[str, Any]] = {}

    for item in snapshot["items"]:
        document = _payload_document(item)
        for file_record in sorted(
            document["files"], key=lambda row: str(row.get("name") or "")
        ):
            if not isinstance(file_record, dict):
                raise ArchiveBracketReconciliationError("validated file record changed shape")
            filename = file_record.get("name")
            if not isinstance(filename, str) or not VIDEO_EXTENSION_RE.search(filename):
                continue
            file_title = file_record.get("title")
            filename_token = terminal_bracketed_youtube_id(filename)
            title_token = terminal_bracketed_youtube_id(file_title)
            if filename_token is None and title_token is None:
                continue
            archive_source, archive_recording = _archive_projection(
                connection, item["identifier"], filename
            )
            common = {
                "archive_item": item["identifier"],
                "archive_filename": filename,
                "archive_file_title": file_title if isinstance(file_title, str) else None,
                "archive_source_id": archive_source,
                "archive_recording_id": archive_recording,
                "filename_youtube_video_id": filename_token,
                "title_youtube_video_id": title_token,
            }
            if (
                filename_token is not None
                and title_token is not None
                and filename_token != title_token
            ):
                issue_id = stable_id(
                    "abi",
                    snapshot["snapshot_id"],
                    archive_source,
                    filename_token,
                    title_token,
                )
                task = _review_task(
                    task_kind="archive_bracket_reconciliation_conflict",
                    target_type="archive_bracket_reconciliation_issue",
                    target_id=issue_id,
                    reason=(
                        f"Filename ID {filename_token} conflicts with title ID {title_token}; "
                        "do not reconcile or merge without direct human verification"
                    ),
                    priority=35,
                    observed_at=snapshot["observed_at"],
                )
                issues.append(
                    {
                        "issue_id": issue_id,
                        "review_task_id": task["review_task_id"],
                        "issue_kind": "conflicting_terminal_bracket_ids",
                        "archive_source_id": archive_source,
                        "archive_recording_id": archive_recording,
                        "filename_youtube_video_id": filename_token,
                        "title_youtube_video_id": title_token,
                        "requires_human_review": 1,
                        "relationship_asserted": 0,
                        "merge_performed": 0,
                        "evidence_json": {
                            **common,
                            "requires_human_review": True,
                            "relationship_asserted": False,
                            "merge_performed": False,
                        },
                        "review_task": task,
                    }
                )
                if len(issues) > MAX_CONFLICT_ISSUES:
                    raise ArchiveBracketReconciliationError(
                        "snapshot exceeds the bracket conflict-issue cap"
                    )
                if len(rows) + len(issues) > MAX_BRACKET_EVIDENCE_FILES:
                    raise ArchiveBracketReconciliationError(
                        "snapshot exceeds the bracket evidence-file cap"
                    )
                continue

            video_id = filename_token or title_token
            assert video_id is not None
            basis = (
                "filename_and_title_terminal_bracket"
                if filename_token is not None and title_token is not None
                else (
                    "filename_terminal_bracket"
                    if filename_token is not None
                    else "title_terminal_bracket"
                )
            )
            resolution = resolution_cache.setdefault(
                video_id, _youtube_resolution(connection, video_id)
            )
            rows.append(
                {
                    **common,
                    "youtube_video_id": video_id,
                    "evidence_basis": basis,
                    **resolution,
                }
            )
            if len(rows) + len(issues) > MAX_BRACKET_EVIDENCE_FILES:
                raise ArchiveBracketReconciliationError(
                    "snapshot exceeds the bracket evidence-file cap"
                )

    candidates: list[dict[str, Any]] = []
    by_video: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_video[row["youtube_video_id"]].append(row)
        target_source = row["youtube_source_id"]
        candidates.append(
            _candidate(
                snapshot=snapshot,
                candidate_kind="archive_source_to_youtube_locator",
                youtube_video_id=row["youtube_video_id"],
                archive_source_id=row["archive_source_id"],
                archive_recording_id=row["archive_recording_id"],
                youtube_source_id=target_source,
                youtube_recording_id=row["youtube_recording_id"],
                comparison_archive_recording_id=None,
                evidence_basis=row["evidence_basis"],
                resolution_state=row["resolution_state"],
                evidence={
                    **row,
                    "requires_human_review": True,
                    "relationship_asserted": False,
                    "merge_performed": False,
                },
                left_type="source",
                left_id=row["archive_source_id"],
                right_type="source" if target_source else "youtube_video_id",
                right_id=target_source or row["youtube_video_id"],
                match_method="archive_bracketed_youtube_locator_v1",
            )
        )
        if len(candidates) > MAX_RECONCILIATION_CANDIDATES:
            raise ArchiveBracketReconciliationError(
                "snapshot exceeds the reconciliation-candidate cap"
            )

    if len(by_video) > MAX_DISTINCT_YOUTUBE_IDS:
        raise ArchiveBracketReconciliationError(
            "snapshot exceeds the distinct bracketed YouTube-ID cap"
        )

    for video_id in sorted(by_video):
        group = by_video[video_id]
        archive_recordings = sorted({row["archive_recording_id"] for row in group})
        resolution = resolution_cache[video_id]
        target_recording = resolution["youtube_recording_id"]
        if target_recording is not None:
            for archive_recording in archive_recordings:
                if archive_recording == target_recording:
                    continue
                candidates.append(
                    _candidate(
                        snapshot=snapshot,
                        candidate_kind="archive_recording_to_youtube_recording",
                        youtube_video_id=video_id,
                        archive_source_id=None,
                        archive_recording_id=archive_recording,
                        youtube_source_id=resolution["youtube_source_id"],
                        youtube_recording_id=target_recording,
                        comparison_archive_recording_id=None,
                        evidence_basis="shared_terminal_bracket_id",
                        resolution_state="unique_native_recording",
                        evidence={
                            "archive_source_ids": sorted(
                                {
                                    row["archive_source_id"]
                                    for row in group
                                    if row["archive_recording_id"] == archive_recording
                                }
                            ),
                            "youtube_video_id": video_id,
                            "youtube_source_id": resolution["youtube_source_id"],
                            "youtube_recording_id": target_recording,
                            "requires_human_review": True,
                            "relationship_asserted": False,
                            "merge_performed": False,
                        },
                        left_type="recording",
                        left_id=archive_recording,
                        right_type="recording",
                        right_id=target_recording,
                        match_method="archive_bracketed_native_recording_v1",
                    )
                )
                if len(candidates) > MAX_RECONCILIATION_CANDIDATES:
                    raise ArchiveBracketReconciliationError(
                        "snapshot exceeds the reconciliation-candidate cap"
                    )
        if len(archive_recordings) > 1:
            anchor = archive_recordings[0]
            for comparison in archive_recordings[1:]:
                candidates.append(
                    _candidate(
                        snapshot=snapshot,
                        candidate_kind="intra_archive_repeat",
                        youtube_video_id=video_id,
                        archive_source_id=None,
                        archive_recording_id=anchor,
                        youtube_source_id=None,
                        youtube_recording_id=None,
                        comparison_archive_recording_id=comparison,
                        evidence_basis="shared_terminal_bracket_id",
                        resolution_state="intra_archive_repeat",
                        evidence={
                            "youtube_video_id": video_id,
                            "archive_recording_ids": archive_recordings,
                            "archive_source_ids": sorted(
                                {row["archive_source_id"] for row in group}
                            ),
                            "pairing_policy": "lowest_recording_id_anchor_v1",
                            "requires_human_review": True,
                            "relationship_asserted": False,
                            "merge_performed": False,
                        },
                        left_type="recording",
                        left_id=anchor,
                        right_type="recording",
                        right_id=comparison,
                        match_method="archive_bracketed_intra_archive_repeat_v1",
                    )
                )
                if len(candidates) > MAX_RECONCILIATION_CANDIDATES:
                    raise ArchiveBracketReconciliationError(
                        "snapshot exceeds the reconciliation-candidate cap"
                    )

    candidates.sort(key=lambda value: value["generic"]["match_candidate_id"])
    issues.sort(key=lambda value: value["issue_id"])
    candidate_ids = [value["generic"]["match_candidate_id"] for value in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ArchiveBracketReconciliationError("reconciliation plan has duplicate candidates")
    issue_ids = [value["issue_id"] for value in issues]
    if len(issue_ids) != len(set(issue_ids)):
        raise ArchiveBracketReconciliationError("reconciliation plan has duplicate issues")

    counts = Counter(
        value["evidence"]["candidate_kind"] for value in candidates
    )
    counts.update({"conflicting_terminal_bracket_ids": len(issues)})
    statistics = {
        "provider_file_records_scanned": total_provider_files,
        "archive_files_with_consistent_terminal_bracket_id": len(rows),
        "archive_files_with_any_terminal_bracket_evidence": len(rows) + len(issues),
        "distinct_youtube_video_ids": len(by_video),
        "candidates_total": len(candidates),
        "candidate_archive_source_to_youtube_locator": counts[
            "archive_source_to_youtube_locator"
        ],
        "candidate_archive_recording_to_youtube_recording": counts[
            "archive_recording_to_youtube_recording"
        ],
        "candidate_intra_archive_repeat": counts["intra_archive_repeat"],
        "issues_conflicting_terminal_bracket_ids": len(issues),
        "review_tasks_total": len(candidates) + len(issues),
        "recording_merges": 0,
        "recording_relations": 0,
        "source_or_recording_mutations": 0,
        "publication_decisions": 0,
        "identity_assertions": 0,
        "claims": 0,
    }
    core = {
        "schema_version": 1,
        "plan_kind": "archive_bracket_youtube_reconciliation_review_plan",
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_sha256": snapshot["_sha256"],
        "observed_at": snapshot["observed_at"],
        "candidates": candidates,
        "issues": issues,
        "statistics": statistics,
        "policy": {
            "terminal_brackets_only": True,
            "bare_or_dash_suffixes_accepted": False,
            "provider_metadata_is_content_truth": False,
            "requires_human_review": True,
            "relationship_asserted": False,
            "merge_performed": False,
            "publication_authority": False,
            "capacity_limits": {
                "provider_file_records": MAX_TOTAL_PROVIDER_FILE_RECORDS,
                "bracket_evidence_files": MAX_BRACKET_EVIDENCE_FILES,
                "distinct_youtube_video_ids": MAX_DISTINCT_YOUTUBE_IDS,
                "reconciliation_candidates": MAX_RECONCILIATION_CANDIDATES,
                "conflict_issues": MAX_CONFLICT_ISSUES,
            },
        },
    }
    plan_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {
        **core,
        "plan_id": f"abrp_{plan_sha256[:32]}",
        "plan_sha256": plan_sha256,
    }


def summarize_archive_bracket_reconciliation_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "valid": True,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "snapshot_id": plan["snapshot_id"],
        "snapshot_sha256": plan["snapshot_sha256"],
        "observed_at": plan["observed_at"],
        "statistics": plan["statistics"],
        "requires_human_review": True,
        "merge_performed": False,
        "publication_authority": False,
    }


def _protected_table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = [
        row["name"]
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        if row["name"] not in ALLOWED_WRITE_TABLES
    ]
    return {
        table: int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
        for table in tables
    }


def _insert_review_task(
    connection: sqlite3.Connection, task: dict[str, Any], *, allow_insert: bool
) -> None:
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO review_tasks(
                review_task_id, task_kind, target_type, target_id, reason, priority,
                status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (
                task["review_task_id"], task["task_kind"], task["target_type"],
                task["target_id"], task["reason"], task["priority"],
                task["created_at"], task["created_at"],
            ),
        )
    row = connection.execute(
        """
        SELECT task_kind, target_type, target_id, reason, priority, created_at
        FROM review_tasks WHERE review_task_id = ?
        """,
        (task["review_task_id"],),
    ).fetchone()
    expected = (
        task["task_kind"], task["target_type"], task["target_id"], task["reason"],
        task["priority"], task["created_at"],
    )
    if row is None or tuple(row) != expected:
        raise ArchiveBracketReconciliationError("existing review task conflicts with plan")


def _insert_candidate(
    connection: sqlite3.Connection, candidate: dict[str, Any], batch_id: str,
    *, allow_insert: bool,
) -> None:
    generic = candidate["generic"]
    generic_values = (
        generic["left_object_type"], generic["left_object_id"],
        generic["right_object_type"], generic["right_object_id"],
        generic["match_method"], None, None, "candidate",
        canonical_json(generic["metadata_json"]),
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (generic["match_candidate_id"], *generic_values),
        )
    row = connection.execute(
        """
        SELECT left_object_type, left_object_id, right_object_type, right_object_id,
               match_method, raw_score, calibrated_probability, decision_state,
               metadata_json
        FROM match_candidates WHERE match_candidate_id = ?
        """,
        (generic["match_candidate_id"],),
    ).fetchone()
    if row is None or tuple(row) != generic_values:
        raise ArchiveBracketReconciliationError("existing generic match candidate conflicts")

    evidence = candidate["evidence"]
    evidence_values = (
        batch_id, evidence["review_task_id"], evidence["candidate_kind"],
        evidence["youtube_video_id"], evidence["archive_source_id"],
        evidence["archive_recording_id"], evidence["youtube_source_id"],
        evidence["youtube_recording_id"],
        evidence["comparison_archive_recording_id"], evidence["evidence_basis"],
        evidence["resolution_state"], 1, 0, 0,
        canonical_json(evidence["evidence_json"]),
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO archive_bracket_youtube_candidates(
                match_candidate_id, import_batch_id, review_task_id, candidate_kind,
                youtube_video_id, archive_source_id, archive_recording_id,
                youtube_source_id, youtube_recording_id,
                comparison_archive_recording_id, evidence_basis, resolution_state,
                requires_human_review, relationship_asserted, merge_performed,
                evidence_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (generic["match_candidate_id"], *evidence_values),
        )
    row = connection.execute(
        """
        SELECT import_batch_id, review_task_id, candidate_kind, youtube_video_id,
               archive_source_id, archive_recording_id, youtube_source_id,
               youtube_recording_id, comparison_archive_recording_id,
               evidence_basis, resolution_state, requires_human_review,
               relationship_asserted, merge_performed, evidence_json
        FROM archive_bracket_youtube_candidates WHERE match_candidate_id = ?
        """,
        (generic["match_candidate_id"],),
    ).fetchone()
    if row is None or tuple(row) != evidence_values:
        raise ArchiveBracketReconciliationError("existing bracket candidate conflicts")


def _insert_issue(
    connection: sqlite3.Connection, issue: dict[str, Any], batch_id: str,
    *, allow_insert: bool,
) -> None:
    values = (
        batch_id, issue["review_task_id"], issue["issue_kind"],
        issue["archive_source_id"], issue["archive_recording_id"],
        issue["filename_youtube_video_id"], issue["title_youtube_video_id"],
        1, 0, 0, canonical_json(issue["evidence_json"]),
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO archive_bracket_reconciliation_issues(
                issue_id, import_batch_id, review_task_id, issue_kind,
                archive_source_id, archive_recording_id, filename_youtube_video_id,
                title_youtube_video_id, requires_human_review,
                relationship_asserted, merge_performed, evidence_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (issue["issue_id"], *values),
        )
    row = connection.execute(
        """
        SELECT import_batch_id, review_task_id, issue_kind, archive_source_id,
               archive_recording_id, filename_youtube_video_id,
               title_youtube_video_id, requires_human_review,
               relationship_asserted, merge_performed, evidence_json
        FROM archive_bracket_reconciliation_issues WHERE issue_id = ?
        """,
        (issue["issue_id"],),
    ).fetchone()
    if row is None or tuple(row) != values:
        raise ArchiveBracketReconciliationError("existing bracket conflict issue differs")


def _verify_receipt(
    connection: sqlite3.Connection, plan: dict[str, Any], batch_id: str,
) -> None:
    statistics_text = canonical_json(plan["statistics"])
    expected = (
        plan["snapshot_id"], plan["snapshot_sha256"], plan["plan_sha256"],
        plan["observed_at"], len(plan["candidates"]), len(plan["issues"]),
        plan["statistics"]["review_tasks_total"], statistics_text,
        plan["observed_at"],
    )
    row = connection.execute(
        """
        SELECT snapshot_id, snapshot_sha256, plan_sha256, observed_at,
               candidate_count, issue_count, review_task_count, statistics_json,
               imported_at
        FROM archive_bracket_reconciliation_imports WHERE import_batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    if row is None or tuple(row) != expected:
        raise ArchiveBracketReconciliationError("archive bracket import receipt differs")


def _verify_plan_rows(
    connection: sqlite3.Connection, plan: dict[str, Any], batch_id: str,
    *, allow_insert: bool,
) -> None:
    if allow_insert:
        receipt_values = (
            plan["snapshot_id"], plan["snapshot_sha256"], plan["plan_sha256"],
            plan["observed_at"], len(plan["candidates"]), len(plan["issues"]),
            plan["statistics"]["review_tasks_total"],
            canonical_json(plan["statistics"]), plan["observed_at"],
        )
        connection.execute(
            """
            INSERT INTO archive_bracket_reconciliation_imports(
                import_batch_id, snapshot_id, snapshot_sha256, plan_sha256,
                observed_at, candidate_count, issue_count, review_task_count,
                statistics_json, imported_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (batch_id, *receipt_values),
        )
    _verify_receipt(connection, plan, batch_id)
    for candidate in plan["candidates"]:
        _insert_review_task(
            connection, candidate["review_task"], allow_insert=allow_insert
        )
        _insert_candidate(
            connection, candidate, batch_id, allow_insert=allow_insert
        )
    for issue in plan["issues"]:
        _insert_review_task(
            connection, issue["review_task"], allow_insert=allow_insert
        )
        _insert_issue(connection, issue, batch_id, allow_insert=allow_insert)


def import_archive_bracket_reconciliation(
    connection: sqlite3.Connection, snapshot_path: Path
) -> dict[str, Any]:
    """Admit immutable private candidates; never alter recording/source identity."""

    snapshot_path = Path(snapshot_path).resolve()
    initial = build_archive_bracket_reconciliation_plan(connection, snapshot_path)
    with transaction(connection):
        protected_before = _protected_table_counts(connection)
        locked = build_archive_bracket_reconciliation_plan(connection, snapshot_path)
        if locked["plan_sha256"] != initial["plan_sha256"]:
            raise ArchiveBracketReconciliationError(
                "reconciliation inputs changed before the write lock"
            )
        batch_id, existing = _begin_batch(
            connection,
            IMPORTER_NAME,
            locked["plan_sha256"],
            locked["observed_at"][:10],
            locked["observed_at"],
        )
        _verify_plan_rows(
            connection, locked, batch_id, allow_insert=existing is None
        )
        if existing is None:
            _complete_batch(
                connection, batch_id, locked["observed_at"], locked["statistics"]
            )
        elif existing != locked["statistics"]:
            raise ArchiveBracketReconciliationError(
                "completed reconciliation statistics differ from the rebuilt plan"
            )
        final = build_archive_bracket_reconciliation_plan(connection, snapshot_path)
        if final["plan_sha256"] != locked["plan_sha256"]:
            raise ArchiveBracketReconciliationError(
                "reconciliation inputs changed during import"
            )
        protected_after = _protected_table_counts(connection)
        if protected_after != protected_before:
            changed = {
                table: [protected_before[table], protected_after[table]]
                for table in protected_before
                if protected_before[table] != protected_after[table]
            }
            raise ArchiveBracketReconciliationError(
                f"reconciliation touched protected catalog tables: {changed}"
            )
        return {
            "import_batch_id": batch_id,
            **summarize_archive_bracket_reconciliation_plan(locked),
        }
