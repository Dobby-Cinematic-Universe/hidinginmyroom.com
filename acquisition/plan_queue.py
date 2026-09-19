#!/usr/bin/env python3
"""Build a deterministic, read-only acquisition backlog from the private catalog.

The planner does not acquire media, mutate SQLite, resolve URLs, or grant publication
approval.  It chooses at most one currently public source per unacquired recording and
records why each candidate was selected, deferred, or routed to another queue.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, unquote


SCHEMA_VERSION = 1
YOUTUBE_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
UTC_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$"
)
YOUTUBE_URL = re.compile(
    r"https?://(?:www\.|m\.)?(?:youtube\.com/(?:watch\?[^\s)>'\"]*?v=|shorts/|live/)|youtu\.be/)"
    r"([A-Za-z0-9_-]{11})",
    re.IGNORECASE,
)
ARCHIVE_URL = re.compile(
    r"https?://(?:www\.)?archive\.org/download/([^/\s)>'\"]+)/([^\s)>'\"?#]+)",
    re.IGNORECASE,
)
SUPPORTED_TEXT_SUFFIXES = frozenset({".md", ".mdx"})
QUEUE_STATES = frozenset(
    {"ready", "requires_metadata", "requires_chunking", "review_required"}
)
DEFAULT_MAX_JOB_BYTES = 10 * 1024**3
DEFAULT_PLAN_BUDGET_BYTES = 20 * 1024**3
DEFAULT_LONG_RECORDING_MS = 2 * 60 * 60 * 1000
DEFAULT_YOUTUBE_BYTES_PER_SECOND = 500_000
DEFAULT_YOUTUBE_FIXED_OVERHEAD_BYTES = 64 * 1024**2


class PlanningError(RuntimeError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:32]}"


def require_absolute_existing(path_text: str, label: str, *, directory: bool = False) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        raise PlanningError(f"{label} must be an absolute path")
    if not path.exists():
        raise PlanningError(f"{label} does not exist: {path}")
    if directory and not path.is_dir():
        raise PlanningError(f"{label} is not a directory: {path}")
    if not directory and not path.is_file():
        raise PlanningError(f"{label} is not a file: {path}")
    return path.resolve()


def validate_selection_manifest(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise PlanningError("selection manifest must be a JSON object")
    allowed = {
        "schema_version",
        "purpose",
        "youtube_video_ids",
        "source_ids",
        "recording_ids",
    }
    unknown = sorted(set(raw) - allowed)
    missing = sorted(allowed - set(raw))
    if unknown or missing:
        raise PlanningError(
            "selection manifest keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    if raw["schema_version"] != 1:
        raise PlanningError("selection manifest.schema_version must equal 1")
    purpose = raw["purpose"]
    if not isinstance(purpose, str) or not purpose or len(purpose) > 500:
        raise PlanningError("selection manifest.purpose must be 1..500 characters")

    def string_list(name: str, pattern: re.Pattern[str] | None = None) -> list[str]:
        values = raw[name]
        if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
            raise PlanningError(f"selection manifest.{name} must be a string array")
        if len(values) != len(set(values)):
            raise PlanningError(f"selection manifest.{name} contains duplicates")
        normalized = sorted(values)
        if pattern and any(not pattern.fullmatch(value) for value in normalized):
            raise PlanningError(f"selection manifest.{name} contains an invalid identifier")
        if any(not value or len(value) > 500 for value in normalized):
            raise PlanningError(f"selection manifest.{name} contains an invalid string")
        return normalized

    return {
        "schema_version": 1,
        "purpose": purpose,
        "youtube_video_ids": string_list("youtube_video_ids", YOUTUBE_ID),
        "source_ids": string_list("source_ids"),
        "recording_ids": string_list("recording_ids"),
    }


def empty_selection() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "purpose": "catalog_priority_policy",
        "youtube_video_ids": [],
        "source_ids": [],
        "recording_ids": [],
    }


def wiki_citations(roots: Iterable[Path]) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    youtube: dict[str, set[str]] = defaultdict(set)
    archive: dict[str, set[str]] = defaultdict(set)
    for root in sorted(set(roots), key=str):
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in SUPPORTED_TEXT_SUFFIXES:
                continue
            try:
                relative = path.relative_to(root).as_posix()
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as error:
                raise PlanningError(f"Cannot scan wiki source {path}: {error}") from error
            for match in YOUTUBE_URL.finditer(text):
                youtube[match.group(1)].add(relative)
            for match in ARCHIVE_URL.finditer(text):
                native_id = f"{unquote(match.group(1))}/{unquote(match.group(2))}"
                archive[native_id].add(relative)
    return (
        {key: sorted(value) for key, value in sorted(youtube.items())},
        {key: sorted(value) for key, value in sorted(archive.items())},
    )


def connect_read_only(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
    except sqlite3.Error as error:
        raise PlanningError(f"Cannot open catalog read-only: {error}") from error
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def require_catalog_tables(connection: sqlite3.Connection) -> None:
    required = {
        "schema_migrations",
        "sources",
        "recordings",
        "recording_sources",
        "media_sources",
        "renditions",
        "source_hashes",
    }
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    missing = sorted(required - present)
    if missing:
        raise PlanningError(f"catalog is missing required tables: {missing}")
    integrity = connection.execute("PRAGMA quick_check").fetchone()[0]
    if integrity != "ok":
        raise PlanningError(f"catalog quick_check failed: {integrity}")


def migration_ledger(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT version, name, sha256 FROM schema_migrations ORDER BY version"
    ).fetchall()
    if not rows:
        raise PlanningError("catalog has no applied migration ledger")
    result = []
    for row in rows:
        digest = row["sha256"]
        if not isinstance(digest, str) or not SHA256.fullmatch(digest):
            raise PlanningError(f"migration {row['version']} has an invalid checksum")
        result.append(
            {"version": row["version"], "name": row["name"], "sha256": digest}
        )
    return result


SOURCE_QUERY = """
SELECT
    r.recording_id,
    r.title AS recording_title,
    r.duration_ms AS recording_duration_ms,
    r.recording_type,
    r.review_state AS recording_review_state,
    s.source_id,
    s.platform,
    s.source_kind,
    s.native_id,
    s.title AS source_title,
    s.canonical_url,
    s.published_at,
    s.access_state,
    s.review_state AS source_review_state,
    s.metadata_json,
    (
        SELECT group_concat(role.mapping_role, char(31))
        FROM (
            SELECT DISTINCT rs2.mapping_role
            FROM recording_sources rs2
            WHERE rs2.recording_id = r.recording_id
              AND rs2.source_id = s.source_id
            ORDER BY rs2.mapping_role
        ) AS role
    ) AS mapping_roles,
    EXISTS(
        SELECT 1 FROM media_sources ms WHERE ms.source_id = s.source_id
    ) AS source_acquired,
    EXISTS(
        SELECT 1 FROM renditions rr WHERE rr.recording_id = r.recording_id
    ) AS recording_acquired,
    (
        SELECT sh.digest FROM source_hashes sh
        WHERE sh.source_id = s.source_id AND lower(sh.algorithm) = 'sha256'
        ORDER BY sh.observed_at DESC, sh.digest
        LIMIT 1
    ) AS expected_sha256
FROM recordings r
JOIN recording_sources rs ON rs.recording_id = r.recording_id
JOIN sources s ON s.source_id = rs.source_id
WHERE r.merged_into_recording_id IS NULL
GROUP BY r.recording_id, s.source_id
ORDER BY r.recording_id, s.source_id
"""


def safe_source_url(row: sqlite3.Row) -> tuple[str | None, str | None]:
    platform = row["platform"]
    native_id = row["native_id"]
    if platform == "youtube" and row["source_kind"] == "youtube_video":
        if not YOUTUBE_ID.fullmatch(native_id):
            return None, None
        return f"https://www.youtube.com/watch?v={native_id}", "yt_dlp"
    if platform == "internet_archive" and row["source_kind"] == "archive_media_file":
        if "/" not in native_id:
            return None, None
        item, filename = native_id.split("/", 1)
        if not item or not filename or any(part in {".", ".."} for part in native_id.split("/")):
            return None, None
        return (
            f"https://archive.org/download/{quote(item, safe='')}/{quote(filename, safe='/()[],-_.')}",
            "direct_http",
        )
    return None, None


def positive_integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def candidate_from_row(
    row: sqlite3.Row,
    *,
    youtube_citations: dict[str, list[str]],
    archive_citations: dict[str, list[str]],
    selected_youtube: set[str],
    selected_sources: set[str],
    selected_recordings: set[str],
    max_job_bytes: int,
    long_recording_ms: int,
    youtube_bytes_per_second: int,
    youtube_fixed_overhead_bytes: int,
) -> dict[str, Any] | None:
    canonical_url, adapter = safe_source_url(row)
    if canonical_url is None or adapter is None:
        return None
    if row["access_state"] != "public":
        return None
    try:
        metadata = json.loads(row["metadata_json"])
    except (TypeError, json.JSONDecodeError) as error:
        raise PlanningError(f"source {row['source_id']} has invalid metadata_json") from error
    if not isinstance(metadata, dict):
        raise PlanningError(f"source {row['source_id']} metadata_json is not an object")

    mapping_roles = sorted(set(filter(None, (row["mapping_roles"] or "").split("\x1f"))))
    native_selected = (
        row["platform"] == "youtube" and row["native_id"] in selected_youtube
    )
    source_selected = row["source_id"] in selected_sources
    recording_selected = row["recording_id"] in selected_recordings
    explicit = native_selected or source_selected or recording_selected
    citation_paths = (
        youtube_citations.get(row["native_id"], [])
        if row["platform"] == "youtube"
        else archive_citations.get(row["native_id"], [])
    )
    duration_ms = positive_integer(row["recording_duration_ms"]) or positive_integer(
        metadata.get("duration_ms")
    )
    provider_bytes = positive_integer(metadata.get("byte_count"))
    source_class = metadata.get("source_class")
    if not isinstance(source_class, str):
        source_class = None

    if provider_bytes is not None:
        estimated_bytes = provider_bytes
        estimate_basis = "provider_declared_byte_count"
        expected_byte_count = provider_bytes
    elif duration_ms is not None:
        estimated_bytes = (
            (duration_ms * youtube_bytes_per_second + 999) // 1000
            + youtube_fixed_overhead_bytes
        )
        estimate_basis = "duration_conservative_rate"
        expected_byte_count = None
    else:
        estimated_bytes = max_job_bytes
        estimate_basis = "job_cap_unknown_duration"
        expected_byte_count = None

    reason_codes: list[str] = []
    if explicit:
        priority = 10
        tier = "explicit_selection"
        reason_codes.append("explicit_selection")
        if native_selected:
            reason_codes.append("explicit_native_id")
        if source_selected:
            reason_codes.append("explicit_source_id")
        if recording_selected:
            reason_codes.append("explicit_recording_id")
    elif citation_paths:
        priority = 20
        tier = "wiki_cited"
        reason_codes.append("wiki_cited")
    elif "current_platform_listing" in mapping_roles:
        priority = 30
        tier = "current_public_upload"
        reason_codes.append("current_platform_listing")
    elif row["platform"] == "youtube":
        priority = 35
        tier = "public_guest_or_clip"
        reason_codes.append("public_youtube_guest_or_clip")
    elif duration_ms is not None and duration_ms <= 15 * 60 * 1000:
        priority = 40
        tier = "short_archive"
        reason_codes.append("short_archive_recording")
    elif duration_ms is not None and duration_ms <= long_recording_ms:
        priority = 50
        tier = "medium_archive"
        reason_codes.append("medium_archive_recording")
    else:
        priority = 60
        tier = "long_recording"
        reason_codes.append("long_or_unknown_archive_recording")

    if source_class == "original":
        reason_codes.append("provider_original")
    elif source_class == "derivative":
        reason_codes.append("provider_derivative")
    if row["recording_type"] == "guest_appearance":
        reason_codes.append("guest_appearance")

    if row["source_review_state"] in {"rejected", "disputed"} or row[
        "recording_review_state"
    ] in {"rejected", "disputed"}:
        queue_state = "review_required"
        reason_codes.append("catalog_review_state_requires_attention")
    elif duration_ms is None:
        queue_state = "requires_metadata"
        reason_codes.append("duration_unknown")
    elif duration_ms > long_recording_ms or estimated_bytes > max_job_bytes:
        queue_state = "requires_chunking"
        reason_codes.append("exceeds_single_job_policy")
    else:
        queue_state = "ready"
        reason_codes.append("bounded_single_job")
    if queue_state not in QUEUE_STATES:
        raise AssertionError(queue_state)

    expected_sha256 = row["expected_sha256"]
    if not isinstance(expected_sha256, str) or not SHA256.fullmatch(expected_sha256):
        expected_sha256 = None

    return {
        "recording_id": row["recording_id"],
        "source_id": row["source_id"],
        "platform": row["platform"],
        "source_kind": row["source_kind"],
        "native_id": row["native_id"],
        "title": row["source_title"] or row["recording_title"],
        "canonical_url": canonical_url,
        "adapter": adapter,
        "recording_type": row["recording_type"],
        "duration_ms": duration_ms,
        "estimated_bytes": estimated_bytes,
        "estimate_basis": estimate_basis,
        "expected_byte_count": expected_byte_count,
        "expected_sha256": expected_sha256,
        "source_class": source_class,
        "mapping_roles": mapping_roles,
        "priority": priority,
        "priority_tier": tier,
        "reason_codes": sorted(set(reason_codes)),
        "wiki_reference_count": len(citation_paths),
        "wiki_reference_paths": citation_paths,
        "queue_state": queue_state,
        "queue_ordinal": None,
        "defer_reason": None,
    }


def source_preference(candidate: dict[str, Any]) -> tuple[Any, ...]:
    if "explicit_native_id" in candidate["reason_codes"] or "explicit_source_id" in candidate[
        "reason_codes"
    ]:
        explicit_rank = 0
    elif "explicit_recording_id" in candidate["reason_codes"]:
        explicit_rank = 1
    else:
        explicit_rank = 2
    source_class_rank = {"original": 0, None: 1, "derivative": 2}.get(
        candidate["source_class"], 1
    )
    adapter_rank = {"yt_dlp": 0, "direct_http": 1}[candidate["adapter"]]
    return (
        candidate["priority"],
        explicit_rank,
        0 if candidate["queue_state"] == "ready" else 1,
        source_class_rank,
        adapter_rank,
        candidate["estimated_bytes"],
        candidate["source_id"],
    )


def count_entries(values: Iterable[str]) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count}
        for key, count in sorted(Counter(values).items())
    ]


def build_plan(
    *,
    database: Path,
    wiki_roots: list[Path],
    selection: dict[str, Any],
    selection_sha256: str | None,
    planned_at: str,
    max_items: int,
    plan_budget_bytes: int,
    max_job_bytes: int,
    long_recording_ms: int,
    youtube_bytes_per_second: int,
    youtube_fixed_overhead_bytes: int,
    selection_only: bool,
) -> dict[str, Any]:
    if not UTC_TIMESTAMP.fullmatch(planned_at):
        raise PlanningError("--planned-at must be an RFC 3339 UTC timestamp ending in Z")
    for name, value in {
        "max_items": max_items,
        "plan_budget_bytes": plan_budget_bytes,
        "max_job_bytes": max_job_bytes,
        "long_recording_ms": long_recording_ms,
        "youtube_bytes_per_second": youtube_bytes_per_second,
        "youtube_fixed_overhead_bytes": youtube_fixed_overhead_bytes,
    }.items():
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise PlanningError(f"{name} must be a positive integer")
    if selection_only and not (
        selection["youtube_video_ids"]
        or selection["source_ids"]
        or selection["recording_ids"]
    ):
        raise PlanningError("selection_only requires at least one explicit identifier")

    youtube_citations, archive_citations = wiki_citations(wiki_roots)
    connection = connect_read_only(database)
    try:
        connection.execute("BEGIN")
        require_catalog_tables(connection)
        migrations = migration_ledger(connection)
        rows = connection.execute(SOURCE_QUERY).fetchall()
        already_acquired_recordings = len(
            {row["recording_id"] for row in rows if row["recording_acquired"]}
        )
        withheld_access_sources = len(
            {
                row["source_id"]
                for row in rows
                if row["access_state"] != "public"
                and safe_source_url(row)[0] is not None
            }
        )

        selected_youtube = set(selection["youtube_video_ids"])
        selected_sources = set(selection["source_ids"])
        selected_recordings = set(selection["recording_ids"])
        by_recording: dict[str, list[dict[str, Any]]] = defaultdict(list)
        supported_unacquired_sources = 0
        requested_seen_eligible: set[str] = set()
        requested_seen_acquired: set[str] = set()
        for row in rows:
            requested_matches = {
                value
                for value in (
                    row["native_id"]
                    if row["platform"] == "youtube" and row["native_id"] in selected_youtube
                    else None,
                    row["source_id"] if row["source_id"] in selected_sources else None,
                    row["recording_id"]
                    if row["recording_id"] in selected_recordings
                    else None,
                )
                if value is not None
            }
            if row["source_acquired"] or row["recording_acquired"]:
                if safe_source_url(row)[0] is not None and row["access_state"] == "public":
                    requested_seen_acquired.update(requested_matches)
                continue
            candidate = candidate_from_row(
                row,
                youtube_citations=youtube_citations,
                archive_citations=archive_citations,
                selected_youtube=selected_youtube,
                selected_sources=selected_sources,
                selected_recordings=selected_recordings,
                max_job_bytes=max_job_bytes,
                long_recording_ms=long_recording_ms,
                youtube_bytes_per_second=youtube_bytes_per_second,
                youtube_fixed_overhead_bytes=youtube_fixed_overhead_bytes,
            )
            if candidate is None:
                continue
            supported_unacquired_sources += 1
            requested_seen_eligible.update(requested_matches)
            by_recording[candidate["recording_id"]].append(candidate)

        candidates = [
            min(options, key=source_preference)
            for _, options in sorted(by_recording.items())
        ]
        candidates.sort(
            key=lambda item: (
                item["priority"],
                item["duration_ms"] if item["duration_ms"] is not None else 2**63,
                item["recording_id"],
                item["source_id"],
            )
        )

        selected_count = 0
        selected_bytes = 0
        for candidate in candidates:
            if candidate["queue_state"] != "ready":
                candidate["defer_reason"] = candidate["queue_state"]
                continue
            if selection_only and candidate["priority_tier"] != "explicit_selection":
                candidate["defer_reason"] = "outside_explicit_selection"
                continue
            if selected_count >= max_items:
                candidate["defer_reason"] = "plan_item_limit"
                continue
            if selected_bytes + candidate["estimated_bytes"] > plan_budget_bytes:
                candidate["defer_reason"] = "plan_byte_budget"
                continue
            selected_count += 1
            selected_bytes += candidate["estimated_bytes"]
            candidate["queue_ordinal"] = selected_count

        requested = selected_youtube | selected_sources | selected_recordings
        already_acquired_requested = sorted(
            requested_seen_acquired - requested_seen_eligible
        )
        missing_requested = sorted(
            requested - requested_seen_eligible - requested_seen_acquired
        )
        catalog_basis = {
            "migrations": migrations,
            "source_rows": [dict(row) for row in rows],
        }
        catalog_basis_sha256 = hashlib.sha256(canonical_bytes(catalog_basis)).hexdigest()
        selection_basis = {
            "purpose": selection["purpose"],
            "manifest_sha256": selection_sha256,
            "youtube_video_ids": selection["youtube_video_ids"],
            "source_ids": selection["source_ids"],
            "recording_ids": selection["recording_ids"],
            "requested_identifiers_already_acquired": already_acquired_requested,
            "requested_identifiers_not_eligible": missing_requested,
        }
        limits = {
            "max_items": max_items,
            "plan_budget_bytes": plan_budget_bytes,
            "max_job_bytes": max_job_bytes,
            "long_recording_ms": long_recording_ms,
            "youtube_bytes_per_second": youtube_bytes_per_second,
            "youtube_fixed_overhead_bytes": youtube_fixed_overhead_bytes,
            "selection_only": selection_only,
        }
        summary = {
            "supported_unacquired_sources": supported_unacquired_sources,
            "recording_candidates": len(candidates),
            "selected_count": selected_count,
            "selected_estimated_bytes": selected_bytes,
            "deferred_count": len(candidates) - selected_count,
            "already_acquired_recordings": already_acquired_recordings,
            "withheld_access_sources": withheld_access_sources,
            "by_queue_state": count_entries(item["queue_state"] for item in candidates),
            "by_platform": count_entries(item["platform"] for item in candidates),
            "by_priority_tier": count_entries(item["priority_tier"] for item in candidates),
            "by_defer_reason": count_entries(
                item["defer_reason"] for item in candidates if item["defer_reason"] is not None
            ),
        }
        core = {
            "schema_version": SCHEMA_VERSION,
            "planned_at": planned_at,
            "catalog_basis_sha256": catalog_basis_sha256,
            "catalog_migrations": migrations,
            "selection_basis": selection_basis,
            "wiki_scan": {
                "root_count": len(wiki_roots),
                "youtube_ids_cited": len(youtube_citations),
                "archive_objects_cited": len(archive_citations),
            },
            "limits": limits,
            "safety": {
                "access_policy": "public_only",
                "publication_authority": "none",
                "credentials_allowed": False,
                "network_access_performed": False,
                "catalog_mutated": False,
            },
            "summary": summary,
            "candidates": candidates,
        }
        plan_id = stable_id("acqplan", hashlib.sha256(canonical_bytes(core)).hexdigest())
        return {"plan_id": plan_id, **core}
    except sqlite3.Error as error:
        raise PlanningError(f"Catalog query failed: {error}") from error
    finally:
        if connection.in_transaction:
            connection.rollback()
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a deterministic, read-only public-media acquisition queue"
    )
    parser.add_argument("--db", required=True)
    parser.add_argument(
        "--wiki-root",
        action="append",
        default=[],
        help="repeatable absolute Markdown/MDX root used only for citation priority",
    )
    parser.add_argument("--selection-manifest")
    parser.add_argument("--planned-at", required=True)
    parser.add_argument("--max-items", type=int, default=100)
    parser.add_argument("--plan-budget-bytes", type=int, default=DEFAULT_PLAN_BUDGET_BYTES)
    parser.add_argument("--max-job-bytes", type=int, default=DEFAULT_MAX_JOB_BYTES)
    parser.add_argument("--long-recording-ms", type=int, default=DEFAULT_LONG_RECORDING_MS)
    parser.add_argument(
        "--youtube-bytes-per-second",
        type=int,
        default=DEFAULT_YOUTUBE_BYTES_PER_SECOND,
    )
    parser.add_argument(
        "--youtube-fixed-overhead-bytes",
        type=int,
        default=DEFAULT_YOUTUBE_FIXED_OVERHEAD_BYTES,
    )
    parser.add_argument(
        "--selection-only",
        action="store_true",
        help="queue only explicitly selected identifiers; retain other candidates as deferred",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        database = require_absolute_existing(args.db, "--db")
        wiki_roots = [
            require_absolute_existing(value, "--wiki-root", directory=True)
            for value in args.wiki_root
        ]
        selection_sha256 = None
        if args.selection_manifest:
            if args.selection_manifest == "-":
                selection_bytes = sys.stdin.buffer.read(1024 * 1024 + 1)
                if len(selection_bytes) > 1024 * 1024:
                    raise PlanningError("stdin selection manifest exceeds 1 MiB")
            else:
                selection_path = require_absolute_existing(
                    args.selection_manifest, "--selection-manifest"
                )
                selection_bytes = selection_path.read_bytes()
            selection_sha256 = hashlib.sha256(selection_bytes).hexdigest()
            selection = validate_selection_manifest(json.loads(selection_bytes))
        else:
            selection = empty_selection()
        plan = build_plan(
            database=database,
            wiki_roots=wiki_roots,
            selection=selection,
            selection_sha256=selection_sha256,
            planned_at=args.planned_at,
            max_items=args.max_items,
            plan_budget_bytes=args.plan_budget_bytes,
            max_job_bytes=args.max_job_bytes,
            long_recording_ms=args.long_recording_ms,
            youtube_bytes_per_second=args.youtube_bytes_per_second,
            youtube_fixed_overhead_bytes=args.youtube_fixed_overhead_bytes,
            selection_only=args.selection_only,
        )
        sys.stdout.write(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        return 0
    except (PlanningError, OSError, json.JSONDecodeError) as error:
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
