"""Deterministic, text-free coverage snapshots for a sealed private catalogue.

This module reports counts and stage coverage only.  It deliberately exposes no
source locator, title, transcript text, person label, claim, or review decision
payload, and it never writes to the catalogue.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from pathlib import Path
from typing import Any

from .db import _migration_ledger, _migration_manifest


SNAPSHOT_SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
SHA256_HEX = frozenset("0123456789abcdef")
MAX_CATALOG_BYTES = 64 * 1024**3
IMPLEMENTATION_PATH = Path(__file__).resolve()

# These are reporting vocabularies, not permissive validators for the underlying
# free-TEXT catalogue columns.  An unfamiliar value must be deliberately reviewed
# and added here before it can appear in an aggregate snapshot.  This prevents a
# database-controlled snake_case label from becoming an accidental text-export lane.
SOURCE_PLATFORMS = frozenset(
    {"bittorrent", "internet_archive", "legacy_himr_archive", "reddit", "web", "youtube"}
)
SOURCE_KINDS = frozenset(
    {
        "archive_item",
        "archive_media_file",
        "archive_url_discovery_hint",
        "channel",
        "image",
        "legacy_catalog_entry",
        "post",
        "reddit_comment",
        "reddit_declared_media",
        "reddit_gallery",
        "reddit_post",
        "reddit_video",
        "subreddit_atom_feed",
        "torrent_file_candidate",
        "torrent_manifest",
        "youtube_video",
    }
)
PROCESSING_STAGES = frozenset(
    {
        "asr_whispercpp",
        "audio_fingerprint_chromaprint",
        "audio_fingerprint_exact_compare",
        "audio_fingerprint_exact_compare_v2",
        "local_window_result_admission",
        "media_acquisition",
        "media_local_transcript_identity_projection",
        "media_preprocess",
        "ocr_tesseract_tsv",
        "sparse_frame_router",
        "visual_fingerprint_compare",
        "visual_fingerprint_extract",
    }
)
PROCESSING_STATUSES = frozenset(
    {"queued", "running", "completed", "failed", "cancelled"}
)
JOB_STATES = frozenset(
    {"pending", "running", "completed", "failed", "cancelled", "blocked"}
)
REVIEW_TASK_KINDS = frozenset(
    {
        "appearance_media_review",
        "archive_bracket_reconciliation_candidate",
        "archive_bracket_reconciliation_conflict",
        "archive_hint_provider_source_reconciliation_candidate",
        "entity_alias_privacy_review",
        "entity_alias_review",
        "entity_map_review",
        "event_date_review",
        "event_evidence_review",
        "event_map_review",
        "event_participant_review",
        "event_relation_review",
        "reddit_citation_media_context_review",
        "reddit_citation_media_ocr_candidate",
        "reddit_citation_media_visual_candidate",
        "reddit_external_locator_discovery",
        "reddit_media_locator_discovery",
        "reddit_video_discovery_candidate",
        "shared_legacy_mapping",
        "source_recording_transform_review",
        "source_recovery",
        "torrent_bracket_reconciliation_candidate",
        "torrent_manifest_assessment",
        "visual_fingerprint_comparison_review",
        "youtube_discovery_candidate",
    }
)
REVIEW_STATUSES = frozenset(
    {"open", "in_progress", "completed", "deferred", "cancelled"}
)
MATCH_METHODS = frozenset(
    {
        "archive_bracketed_intra_archive_repeat_v1",
        "archive_bracketed_native_recording_v1",
        "archive_bracketed_youtube_locator_v1",
        "archive_hint_exact_native_id_candidate_v1",
        "audio_fingerprint_exact_compare_v1",
        "audio_fingerprint_exact_compare_v2",
        "torrent_terminal_bracket_youtube_locator_v1",
        "visual_phash_minimum_hamming_v1",
    }
)
MATCH_DECISION_STATES = frozenset({"candidate", "accepted", "rejected", "disputed"})


class CoverageSnapshotError(RuntimeError):
    """A catalogue or reporting boundary failed closed."""


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
        raise CoverageSnapshotError(
            f"coverage snapshot cannot be canonically encoded: {error}"
        ) from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _hash_descriptor(descriptor: int, size: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset < size:
        try:
            chunk = os.pread(descriptor, min(8 * 1024 * 1024, size - offset), offset)
        except OSError as error:
            raise CoverageSnapshotError(f"cannot read pinned catalogue: {error}") from error
        if not chunk:
            raise CoverageSnapshotError("catalogue ended during its pinned read")
        digest.update(chunk)
        offset += len(chunk)
    try:
        if os.pread(descriptor, 1, size):
            raise CoverageSnapshotError("catalogue grew during its pinned read")
    except OSError as error:
        raise CoverageSnapshotError(f"cannot finish reading pinned catalogue: {error}") from error
    return digest.hexdigest()


def _fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
        value.st_nlink,
    )


def _sqlite_sidecars(path: Path) -> list[str]:
    return [
        Path(f"{path}{suffix}").name
        for suffix in ("-wal", "-shm", "-journal")
        if Path(f"{path}{suffix}").exists()
    ]


def _required_sha256(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in SHA256_HEX for character in value)
    ):
        raise CoverageSnapshotError("expected catalogue SHA-256 is invalid")
    return value


def _implementation_binding() -> dict[str, Any]:
    before = IMPLEMENTATION_PATH.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise CoverageSnapshotError("coverage implementation must be a regular file")
    body = IMPLEMENTATION_PATH.read_bytes()
    after = IMPLEMENTATION_PATH.stat()
    if _fingerprint(before) != _fingerprint(after) or len(body) != before.st_size:
        raise CoverageSnapshotError("coverage implementation changed while reading")
    return {
        "name": "himr-catalog-coverage-snapshot",
        "version": IMPLEMENTATION_VERSION,
        "source_sha256": sha256_bytes(body),
        "source_byte_count": len(body),
    }


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
    }


def _count(connection: sqlite3.Connection, table: str, tables: set[str]) -> int | None:
    if table not in tables:
        return None
    # All names reaching this helper are fixed constants declared below.
    return int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])


def _grouped_counts(
    connection: sqlite3.Connection,
    sql: str,
    fields: tuple[str, ...],
    vocabularies: dict[str, frozenset[str]],
) -> list[dict[str, Any]]:
    if set(vocabularies) != set(fields):
        raise CoverageSnapshotError("coverage reporting vocabulary is misconfigured")
    result: list[dict[str, Any]] = []
    for row in connection.execute(sql):
        categories: dict[str, str] = {}
        for field in fields:
            value = row[field]
            if not isinstance(value, str) or value not in vocabularies[field]:
                raise CoverageSnapshotError(
                    f"coverage category {field} is outside its finite reporting vocabulary"
                )
            categories[field] = value
        count = row["count"]
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise CoverageSnapshotError("coverage grouped count is invalid")
        result.append({**categories, "count": count})
    return result


def _open_descriptor_fingerprints() -> dict[int, tuple[int, int, int, int, int, int, int]]:
    """Snapshot the process file descriptors that can be stably inspected."""

    root = Path("/proc/self/fd")
    try:
        entries = list(root.iterdir())
    except OSError as error:
        raise CoverageSnapshotError("cannot enumerate /proc/self/fd") from error
    result: dict[int, tuple[int, int, int, int, int, int, int]] = {}
    for entry in entries:
        try:
            descriptor = int(entry.name)
            result[descriptor] = _fingerprint(os.stat(entry))
        except (ValueError, FileNotFoundError):
            # The directory descriptor used by iterdir can disappear before stat.
            continue
        except OSError as error:
            raise CoverageSnapshotError("cannot inspect an open file descriptor") from error
    return result


def _verify_sqlite_descriptors(
    descriptors: tuple[int, ...], expected: os.stat_result
) -> None:
    for descriptor in descriptors:
        try:
            observed = os.fstat(descriptor)
        except OSError as error:
            raise CoverageSnapshotError(
                "SQLite released its pinned catalogue descriptor during the snapshot"
            ) from error
        if _fingerprint(observed) != _fingerprint(expected):
            raise CoverageSnapshotError(
                "SQLite catalogue descriptor identity changed during the snapshot"
            )


def _connect_pinned_readonly(
    descriptor: int, expected: os.stat_result
) -> tuple[sqlite3.Connection, tuple[int, ...]]:
    """Open SQLite through the pin and prove the handle retained that exact inode."""

    descriptor_path = Path("/proc/self/fd") / str(descriptor)
    try:
        if _fingerprint(descriptor_path.stat()) != _fingerprint(expected):
            raise CoverageSnapshotError("pinned catalogue descriptor identity changed")
        descriptors_before = _open_descriptor_fingerprints()
        connection = sqlite3.connect(
            f"file:/proc/self/fd/{descriptor}?mode=ro&immutable=1",
            uri=True,
            isolation_level=None,
        )
    except CoverageSnapshotError:
        raise
    except (OSError, sqlite3.Error) as error:
        raise CoverageSnapshotError(
            "cannot open the pinned catalogue through /proc/self/fd"
        ) from error
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA recursive_triggers = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA query_only = ON")
        if _fingerprint(descriptor_path.stat()) != _fingerprint(expected):
            raise CoverageSnapshotError("pinned catalogue descriptor changed during SQLite open")
        descriptors_after = _open_descriptor_fingerprints()
        sqlite_descriptors = tuple(
            sorted(
                opened
                for opened, identity in descriptors_after.items()
                if opened != descriptor
                and identity == _fingerprint(expected)
                and descriptors_before.get(opened) != identity
            )
        )
        if len(sqlite_descriptors) != 1:
            raise CoverageSnapshotError(
                "SQLite did not retain exactly one descriptor for the pinned catalogue"
            )
        _verify_sqlite_descriptors(sqlite_descriptors, expected)
        return connection, sqlite_descriptors
    except Exception:
        connection.close()
        raise


def _validated_migration_state(connection: sqlite3.Connection) -> dict[str, Any]:
    manifest = _migration_manifest()
    ledger = _migration_ledger(connection)
    applied_versions = sorted(ledger)
    if applied_versions != list(range(1, len(applied_versions) + 1)):
        raise CoverageSnapshotError("catalogue migration ledger is not a contiguous prefix")
    if len(applied_versions) > len(manifest):
        raise CoverageSnapshotError("catalogue has migrations missing from this source tree")
    for migration in manifest[: len(applied_versions)]:
        row = ledger[migration.version]
        if row["name"] != migration.name or row["sha256"] != migration.sha256:
            raise CoverageSnapshotError(
                f"catalogue migration {migration.version:04d} differs from source"
            )
    return {
        "applied_count": len(applied_versions),
        "latest_applied": (
            manifest[len(applied_versions) - 1].name if applied_versions else None
        ),
        "pending_count": len(manifest) - len(applied_versions),
        "pending": [migration.name for migration in manifest[len(applied_versions) :]],
    }


def _distinct_recording_transcript_coverage(
    connection: sqlite3.Connection, tables: set[str]
) -> int:
    selects: list[str] = []
    if "transcript_revisions" in tables:
        selects.append("SELECT recording_id FROM transcript_revisions")
    if "rendition_local_transcript_revisions" in tables:
        selects.append("SELECT recording_id FROM rendition_local_transcript_revisions")
    if not selects:
        return 0
    return int(
        connection.execute(
            "SELECT count(*) FROM (" + " UNION ".join(selects) + ")"
        ).fetchone()[0]
    )


def build_coverage_snapshot(
    connection: sqlite3.Connection,
    *,
    catalog_sha256: str,
    catalog_byte_count: int,
    catalog_mode: str,
) -> dict[str, Any]:
    """Build a deterministic aggregate-only snapshot from one read-only connection."""

    if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
        raise CoverageSnapshotError("coverage connection must have query_only enabled")
    integrity = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
    if integrity != ["ok"]:
        raise CoverageSnapshotError("catalogue quick_check failed")

    tables = _table_names(connection)
    required = {
        "sources",
        "recordings",
        "recording_sources",
        "media_objects",
        "media_sources",
        "renditions",
        "transcript_revisions",
        "processing_runs",
        "review_tasks",
        "match_candidates",
        "recording_relations",
        "entities",
        "events",
    }
    missing = sorted(required - tables)
    if missing:
        raise CoverageSnapshotError(f"catalogue lacks required tables: {missing}")

    migration_state = _validated_migration_state(connection)
    count_tables = (
        "sources",
        "recordings",
        "recording_sources",
        "media_objects",
        "media_sources",
        "renditions",
        "transcript_revisions",
        "transcript_segments",
        "transcript_words",
        "rendition_local_transcript_revisions",
        "rendition_local_transcript_segments",
        "rendition_local_transcript_words",
        "media_local_transcript_revisions",
        "media_local_transcript_segments",
        "media_local_transcript_words",
        "private_gpu_v3_asr_imports",
        "processing_runs",
        "jobs",
        "job_attempts",
        "observations",
        "review_tasks",
        "review_decisions",
        "match_candidates",
        "recording_relations",
        "entities",
        "events",
        "public_sources",
        "public_recordings",
        "public_entities",
        "public_events",
        "public_identity_assertions",
        "publication_decisions",
        "publication_gate_decisions",
    )
    counts = {table: _count(connection, table, tables) for table in count_tables}

    enrichment_tables = (
        "speaker_turn_observations",
        "active_speaker_observations",
        "face_track_observations",
        "ocr_observations",
        "ocr_tesseract_result_imports",
        "ocr_tesseract_frames",
        "ocr_tesseract_words",
        "action_observations",
        "sound_observations",
    )
    enrichment = {
        table: {
            "table_present": table in tables,
            "row_count": _count(connection, table, tables),
        }
        for table in enrichment_tables
    }

    coverage = {
        "recordings_total": counts["recordings"],
        "recordings_with_any_source": int(
            connection.execute(
                "SELECT count(DISTINCT recording_id) FROM recording_sources"
            ).fetchone()[0]
        ),
        "sources_with_any_recording": int(
            connection.execute(
                "SELECT count(DISTINCT source_id) FROM recording_sources"
            ).fetchone()[0]
        ),
        "recordings_with_any_rendition": int(
            connection.execute(
                "SELECT count(DISTINCT recording_id) FROM renditions"
            ).fetchone()[0]
        ),
        "recordings_with_recording_scoped_transcript": int(
            connection.execute(
                "SELECT count(DISTINCT recording_id) FROM transcript_revisions"
            ).fetchone()[0]
        ),
        "recordings_with_rendition_local_transcript": (
            int(
                connection.execute(
                    "SELECT count(DISTINCT recording_id) "
                    "FROM rendition_local_transcript_revisions"
                ).fetchone()[0]
            )
            if "rendition_local_transcript_revisions" in tables
            else None
        ),
        "recordings_with_any_catalog_transcript": _distinct_recording_transcript_coverage(
            connection, tables
        ),
        "media_with_media_local_transcript": (
            int(
                connection.execute(
                    "SELECT count(DISTINCT media_id) FROM media_local_transcript_revisions"
                ).fetchone()[0]
            )
            if "media_local_transcript_revisions" in tables
            else None
        ),
        "catalogued_media_bytes": int(
            connection.execute("SELECT coalesce(sum(byte_count), 0) FROM media_objects").fetchone()[0]
        ),
        "catalogued_media_duration_ms_known_sum": int(
            connection.execute(
                "SELECT coalesce(sum(duration_ms), 0) FROM media_objects WHERE duration_ms IS NOT NULL"
            ).fetchone()[0]
        ),
        "catalogued_media_with_known_duration": int(
            connection.execute(
                "SELECT count(*) FROM media_objects WHERE duration_ms IS NOT NULL"
            ).fetchone()[0]
        ),
    }

    core = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "snapshot_kind": "private_catalog_aggregate_coverage",
        "implementation": _implementation_binding(),
        "catalog": {
            "sha256": catalog_sha256,
            "byte_count": catalog_byte_count,
            "mode": catalog_mode,
            "sqlite_version": str(connection.execute("SELECT sqlite_version()").fetchone()[0]),
            "quick_check": "ok",
            "migration_state": migration_state,
        },
        "counts": counts,
        "coverage": coverage,
        "source_families": _grouped_counts(
            connection,
            "SELECT platform, source_kind, count(*) AS count "
            "FROM sources GROUP BY platform, source_kind "
            "ORDER BY platform, source_kind",
            ("platform", "source_kind"),
            {"platform": SOURCE_PLATFORMS, "source_kind": SOURCE_KINDS},
        ),
        "processing_stages": _grouped_counts(
            connection,
            "SELECT stage, status, count(*) AS count FROM processing_runs "
            "GROUP BY stage, status ORDER BY stage, status",
            ("stage", "status"),
            {"stage": PROCESSING_STAGES, "status": PROCESSING_STATUSES},
        ),
        "job_states": (
            _grouped_counts(
                connection,
                "SELECT stage, state, count(*) AS count FROM jobs "
                "GROUP BY stage, state ORDER BY stage, state",
                ("stage", "state"),
                {"stage": PROCESSING_STAGES, "state": JOB_STATES},
            )
            if "jobs" in tables
            else []
        ),
        "review_queues": _grouped_counts(
            connection,
            "SELECT task_kind, status, count(*) AS count FROM review_tasks "
            "GROUP BY task_kind, status ORDER BY task_kind, status",
            ("task_kind", "status"),
            {"task_kind": REVIEW_TASK_KINDS, "status": REVIEW_STATUSES},
        ),
        "match_candidate_states": _grouped_counts(
            connection,
            "SELECT match_method, decision_state, count(*) AS count "
            "FROM match_candidates GROUP BY match_method, decision_state "
            "ORDER BY match_method, decision_state",
            ("match_method", "decision_state"),
            {
                "match_method": MATCH_METHODS,
                "decision_state": MATCH_DECISION_STATES,
            },
        ),
        "machine_enrichment": enrichment,
        "assertion_policy": {
            "aggregate_counts_only": True,
            "catalog_mutated": False,
            "transcript_text_included": False,
            "source_locators_included": False,
            "titles_included": False,
            "person_or_entity_labels_included": False,
            "claims_included": False,
            "review_or_identity_decisions_created": False,
            "publication_authority": "none",
            "visibility": "private",
        },
    }
    digest = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "snapshot_id": f"covsnap_{digest[:32]}",
        "snapshot_sha256": digest,
    }


def snapshot_catalog(database: str | Path, *, expected_sha256: str) -> dict[str, Any]:
    """Pin, hash, query, and rehash one closed mode-0400 catalogue."""

    expected_sha256 = _required_sha256(expected_sha256)
    path = Path(database)
    try:
        before_path = path.lstat()
    except OSError as error:
        raise CoverageSnapshotError(f"cannot stat catalogue: {error}") from error
    if stat.S_ISLNK(before_path.st_mode) or not stat.S_ISREG(before_path.st_mode):
        raise CoverageSnapshotError("catalogue must be a regular non-symlink file")
    if stat.S_IMODE(before_path.st_mode) != 0o400:
        raise CoverageSnapshotError("catalogue must have exact private mode 0400")
    if before_path.st_nlink != 1:
        raise CoverageSnapshotError("catalogue must have exactly one hard link")
    if before_path.st_size <= 0 or before_path.st_size > MAX_CATALOG_BYTES:
        raise CoverageSnapshotError("catalogue byte count is outside the bounded range")

    absolute = Path(os.path.abspath(path))
    try:
        resolved = absolute.resolve(strict=True)
    except OSError as error:
        raise CoverageSnapshotError(f"cannot resolve catalogue path: {error}") from error
    if resolved != absolute:
        raise CoverageSnapshotError("catalogue path components may not be symlinks")
    sidecars = _sqlite_sidecars(absolute)
    if sidecars:
        raise CoverageSnapshotError(
            "coverage snapshot requires no SQLite sidecars; "
            f"found: {sidecars}"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(absolute, flags)
    except OSError as error:
        raise CoverageSnapshotError(f"cannot open pinned catalogue: {error}") from error
    try:
        before_fd = os.fstat(descriptor)
        if _fingerprint(before_fd) != _fingerprint(before_path):
            raise CoverageSnapshotError("catalogue path changed before its pinned read")
        initial_sha256 = _hash_descriptor(descriptor, before_fd.st_size)
        if initial_sha256 != expected_sha256:
            raise CoverageSnapshotError("catalogue SHA-256 differs from the explicit pin")

        connection, sqlite_descriptors = _connect_pinned_readonly(descriptor, before_fd)
        try:
            snapshot = build_coverage_snapshot(
                connection,
                catalog_sha256=initial_sha256,
                catalog_byte_count=before_fd.st_size,
                catalog_mode="0400",
            )
            _verify_sqlite_descriptors(sqlite_descriptors, before_fd)
        finally:
            connection.close()

        after_fd = os.fstat(descriptor)
        try:
            after_path = absolute.lstat()
        except OSError as error:
            raise CoverageSnapshotError(f"cannot restat catalogue: {error}") from error
        if (
            _fingerprint(after_fd) != _fingerprint(before_fd)
            or _fingerprint(after_path) != _fingerprint(before_fd)
        ):
            raise CoverageSnapshotError("catalogue changed during the coverage snapshot")
        if _hash_descriptor(descriptor, after_fd.st_size) != initial_sha256:
            raise CoverageSnapshotError("catalogue bytes changed during the coverage snapshot")
        sidecars = _sqlite_sidecars(absolute)
        if sidecars:
            raise CoverageSnapshotError(
                f"coverage snapshot observed unexpected SQLite sidecars: {sidecars}"
            )
        return snapshot
    finally:
        os.close(descriptor)


def compact_snapshot_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return a stable concise view suitable for an operator progress note."""

    coverage = snapshot["coverage"]
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_sha256": snapshot["snapshot_sha256"],
        "catalog_sha256": snapshot["catalog"]["sha256"],
        "migration_state": snapshot["catalog"]["migration_state"],
        "recordings_total": coverage["recordings_total"],
        "recordings_with_any_source": coverage["recordings_with_any_source"],
        "recordings_with_any_rendition": coverage["recordings_with_any_rendition"],
        "recordings_with_any_catalog_transcript": coverage[
            "recordings_with_any_catalog_transcript"
        ],
        "media_objects": snapshot["counts"]["media_objects"],
        "open_review_tasks": sum(
            row["count"]
            for row in snapshot["review_queues"]
            if row["status"] == "open"
        ),
        "candidate_matches": sum(
            row["count"]
            for row in snapshot["match_candidate_states"]
            if row["decision_state"] == "candidate"
        ),
        "publication_authority": "none",
    }
