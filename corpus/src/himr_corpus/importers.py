"""Metadata-only importers for preserved HIMR source inventories."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import nullcontext
from datetime import datetime, timezone
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

from . import __version__
from .db import transaction, utc_now
from .ids import (
    VIDEO_EXTENSION_RE,
    date_label_from_filename,
    platform_video_id,
    recording_id,
    recording_key_for_archive,
    recording_slug,
    source_id,
    stable_id,
    title_from_media_filename,
)
from .reviewer_admin import ensure_public_metadata_policy_reviewer


PUBLIC_METADATA_REVIEWER_ID = "reviewer_public_metadata_policy_v1"
PUBLIC_METADATA_POLICY = "public-source-metadata-policy-v1"

# Metadata precedence is deliberately coarse and auditable.  It is not a model
# confidence and must never be presented as one.  Higher-quality evidence wins;
# chronology then resolves revisions from the same evidence class.  A final digest
# tie-break makes contradictory captures at the same instant order-independent.
IMPORTER_METADATA_QUALITY = {
    "acquisition_result_v1": (700, "locally verified acquisition result"),
    "ytdlp_info_metadata": (600, "validated direct platform extraction"),
    "current_youtube_channel_inventory": (600, "captured first-party channel inventory"),
    "internet_archive_metadata": (550, "captured provider item metadata"),
    "legacy_source_manifest_metadata": (300, "preserved normalized legacy manifest"),
    "torrent_manifest_metadata": (250, "locally parsed discovery manifest"),
    "archive_url_discovery_hints": (100, "unreviewed discovery hint"),
    "youtube_discovery_candidates": (100, "unreviewed search candidate"),
    "reddit_atom_discovery": (100, "unreviewed public Atom discovery assertion"),
}
DEFAULT_METADATA_QUALITY = (200, "unclassified metadata importer")

# At otherwise identical precedence, the restrictive access assertion wins.  This
# is only a deterministic, fail-closed tie-break; normal revisions are decided by
# quality and observation time first.
ACCESS_RESTRICTION_RANK = {
    "public": 0,
    "unknown": 1,
    "unavailable": 2,
    "removed": 3,
    "members_only": 4,
    "private": 5,
}


def _import_observation_id(
    batch_id: str, observed_at: str, importer_version: str = __version__
) -> str:
    return stable_id("iob", batch_id, importer_version, observed_at)


def _existing_import_observation_id(
    connection: sqlite3.Connection, batch_id: str, observed_at: str
) -> str | None:
    row = connection.execute(
        """
        SELECT import_observation_id
        FROM import_observations
        WHERE import_batch_id = ? AND observed_at = ?
        ORDER BY importer_version DESC, import_observation_id DESC
        LIMIT 1
        """,
        (batch_id, observed_at),
    ).fetchone()
    return row["import_observation_id"] if row else None


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def combined_digest(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.name):
        body = path.read_bytes()
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(len(body).to_bytes(8, "big"))
        digest.update(body)
    return digest.hexdigest()


def load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number}: expected an object")
        records.append(value)
    return records


def snapshot_timestamp(snapshot_date: str | None, preferred: str | None = None) -> str:
    if preferred:
        return preferred
    if snapshot_date and re.fullmatch(r"\d{4}-\d{2}-\d{2}", snapshot_date):
        return f"{snapshot_date}T00:00:00Z"
    return utc_now()


def numeric(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def duration_ms(value: object) -> int | None:
    parsed = numeric(value)
    return None if parsed is None or parsed < 0 else round(parsed * 1000)


def byte_count(value: object) -> int | None:
    parsed = numeric(value)
    return None if parsed is None or parsed < 0 else int(parsed)


def infer_recording_type(title: str, explicit: str | None = None) -> str:
    if explicit == "livestream":
        return "livestream"
    if explicit == "short":
        return "short"
    if re.search(r"\b(?:live\s*stream|livestream|live q\s*&?\s*a|streaming)\b", title, re.I):
        return "livestream"
    return "video"


def _begin_batch(
    connection: sqlite3.Connection,
    importer_name: str,
    digest: str,
    snapshot_date: str | None,
    started_at: str,
) -> tuple[str, dict | None]:
    batch_id = stable_id("imp", importer_name, digest)
    observation_id = _import_observation_id(batch_id, started_at)
    existing_batch = connection.execute(
        "SELECT importer_name, input_sha256 FROM import_batches WHERE import_batch_id = ?",
        (batch_id,),
    ).fetchone()
    if existing_batch and (
        existing_batch["importer_name"] != importer_name
        or existing_batch["input_sha256"] != digest
    ):
        raise ValueError("Deterministic import-batch identity collision")
    connection.execute(
        """
        INSERT INTO import_batches(
            import_batch_id, importer_name, importer_version, input_sha256,
            source_snapshot_date, started_at, status, statistics_json
        ) VALUES(?, ?, ?, ?, ?, ?, 'running', '{}')
        ON CONFLICT(import_batch_id) DO UPDATE SET
            importer_version = CASE
                WHEN excluded.importer_version > import_batches.importer_version
                    THEN excluded.importer_version ELSE import_batches.importer_version END,
            source_snapshot_date = CASE
                WHEN import_batches.source_snapshot_date IS NULL THEN excluded.source_snapshot_date
                WHEN excluded.source_snapshot_date IS NULL THEN import_batches.source_snapshot_date
                WHEN excluded.source_snapshot_date < import_batches.source_snapshot_date
                    THEN excluded.source_snapshot_date ELSE import_batches.source_snapshot_date END,
            started_at = CASE WHEN julianday(excluded.started_at) < julianday(import_batches.started_at)
                THEN excluded.started_at ELSE import_batches.started_at END
        """,
        (batch_id, importer_name, __version__, digest, snapshot_date, started_at),
    )
    existing_observation = connection.execute(
        """
        SELECT status, statistics_json, source_snapshot_date
        FROM import_observations
        WHERE import_observation_id = ?
        """,
        (observation_id,),
    ).fetchone()
    if existing_observation:
        if existing_observation["source_snapshot_date"] != snapshot_date:
            raise ValueError(
                "The same input and observation time has a different snapshot date"
            )
        if existing_observation["status"] == "completed":
            return batch_id, json.loads(existing_observation["statistics_json"])
    connection.execute(
        """
        INSERT INTO import_observations(
            import_observation_id, import_batch_id, importer_version,
            source_snapshot_date, observed_at, status, statistics_json
        ) VALUES(?, ?, ?, ?, ?, 'running', '{}')
        ON CONFLICT(import_observation_id) DO UPDATE SET
            importer_version = excluded.importer_version,
            status = 'running',
            completed_at = NULL,
            statistics_json = '{}'
        """,
        (observation_id, batch_id, __version__, snapshot_date, started_at),
    )
    return batch_id, None


def _complete_batch(
    connection: sqlite3.Connection, batch_id: str, completed_at: str, statistics: dict
) -> None:
    observation_id = _import_observation_id(batch_id, completed_at)
    statistics_text = canonical_json(statistics)
    connection.execute(
        """
        UPDATE import_observations
        SET completed_at = ?, status = 'completed', statistics_json = ?
        WHERE import_observation_id = ?
        """,
        (completed_at, statistics_text, observation_id),
    )
    if connection.execute("SELECT changes()").fetchone()[0] != 1:
        raise RuntimeError("Import observation was not started before completion")
    connection.execute(
        """
        UPDATE import_batches
        SET completed_at = CASE
                WHEN completed_at IS NULL OR julianday(?) > julianday(completed_at)
                    THEN ? ELSE completed_at END,
            status = 'completed',
            statistics_json = CASE
                WHEN completed_at IS NULL OR julianday(?) > julianday(completed_at)
                     OR (julianday(?) = julianday(completed_at)
                         AND ? >= importer_version)
                    THEN ? ELSE statistics_json END
        WHERE import_batch_id = ?
        """,
        (
            completed_at,
            completed_at,
            completed_at,
            completed_at,
            __version__,
            statistics_text,
            batch_id,
        ),
    )


def _metadata_quality(
    connection: sqlite3.Connection, batch_id: str
) -> tuple[int, str]:
    row = connection.execute(
        "SELECT importer_name FROM import_batches WHERE import_batch_id = ?", (batch_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Unknown import batch {batch_id}")
    rank, label = IMPORTER_METADATA_QUALITY.get(
        row["importer_name"], DEFAULT_METADATA_QUALITY
    )
    return rank, f"{row['importer_name']}: {label}"


def _source_observation_sort_key(row: sqlite3.Row) -> tuple:
    return (
        row["quality_rank"],
        row["observed_julian"],
        ACCESS_RESTRICTION_RANK[row["access_state"]],
        row["candidate_sha256"],
        row["import_batch_id"],
        row["source_metadata_observation_id"],
    )


def _recording_observation_sort_key(row: sqlite3.Row) -> tuple:
    return (
        row["quality_rank"],
        row["observed_julian"],
        row["candidate_sha256"],
        row["import_batch_id"],
        row["recording_metadata_observation_id"],
    )


def _first_present(rows: list[sqlite3.Row], field: str, *, reject: set | None = None):
    rejected = reject or set()
    for row in rows:
        value = row[field]
        if value is not None and value != "" and value not in rejected:
            return value, row
    return None, None


def _upsert_source(
    connection: sqlite3.Connection,
    *,
    source: str,
    platform: str,
    source_kind: str,
    native_id: str,
    observed_at: str,
    batch_id: str,
    parent_source: str | None = None,
    canonical_url: str | None = None,
    historical_url: str | None = None,
    title: str | None = None,
    published_at: str | None = None,
    access_state: str = "unknown",
    review_state: str = "metadata_only",
    metadata: dict | None = None,
) -> None:
    metadata_text = canonical_json(metadata or {})
    candidate_payload = {
        "parent_source_id": parent_source,
        "canonical_url": canonical_url,
        "historical_url": historical_url,
        "title": title,
        "published_at": published_at,
        "access_state": access_state,
        "review_state": review_state,
        "metadata": metadata or {},
    }
    candidate_digest = sha256_bytes(
        canonical_json(candidate_payload).encode("utf-8")
    )
    existing = connection.execute(
        "SELECT * FROM sources WHERE source_id = ?", (source,)
    ).fetchone()
    if existing and (
        existing["platform"] != platform
        or existing["source_kind"] != source_kind
        or existing["native_id"] != native_id
    ):
        raise ValueError(f"Source identity collision for {source}")

    connection.execute(
        """
        INSERT INTO sources(
            source_id, platform, source_kind, native_id, parent_source_id,
            canonical_url, historical_url, title, published_at, observed_at,
            access_state, review_state, metadata_json, created_by_import_batch_id,
            created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO NOTHING
        """,
        (
            source,
            platform,
            source_kind,
            native_id,
            parent_source,
            canonical_url,
            historical_url,
            title,
            published_at,
            observed_at,
            access_state,
            review_state,
            metadata_text,
            batch_id,
            observed_at,
            observed_at,
        ),
    )

    # Rows created by an older catalog build or another strict result importer may
    # predate the observation projection.  Seed their current assertion once before
    # adding the new assertion so no provenance is erased during the transition.
    if existing and not connection.execute(
        "SELECT 1 FROM source_metadata_observations WHERE source_id = ? LIMIT 1",
        (source,),
    ).fetchone():
        origin_batch = existing["created_by_import_batch_id"]
        if origin_batch and connection.execute(
            "SELECT 1 FROM import_batches WHERE import_batch_id = ?", (origin_batch,)
        ).fetchone():
            prior_rank, prior_basis = _metadata_quality(connection, origin_batch)
            prior_payload = {
                "parent_source_id": existing["parent_source_id"],
                "canonical_url": existing["canonical_url"],
                "historical_url": existing["historical_url"],
                "title": existing["title"],
                "published_at": existing["published_at"],
                "access_state": existing["access_state"],
                "review_state": existing["review_state"],
                "metadata": json.loads(existing["metadata_json"]),
            }
            prior_digest = sha256_bytes(canonical_json(prior_payload).encode("utf-8"))
            # A result imported by an older build may be replayed after observation
            # support is added.  When its surviving projection is exactly the
            # assertion being admitted from the same batch and instant, let the
            # canonical observation below represent it.  Seeding that identical
            # projection against the newly created import-observation receipt would
            # consume the uniqueness key and suppress the canonical assertion.
            is_same_batch_assertion = (
                origin_batch == batch_id
                and existing["observed_at"] == observed_at
                and prior_digest == candidate_digest
            )
            if not is_same_batch_assertion:
                prior_observation_id = stable_id(
                    "smo", source, origin_batch, existing["observed_at"], prior_digest
                )
                possible_import_observation = _existing_import_observation_id(
                    connection, origin_batch, existing["observed_at"]
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO source_metadata_observations(
                        source_metadata_observation_id, source_id, import_batch_id,
                        import_observation_id, observed_at, quality_rank, quality_basis,
                        candidate_sha256, parent_source_id, canonical_url, historical_url,
                        title, published_at, access_state, review_state, metadata_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        prior_observation_id,
                        source,
                        origin_batch,
                        possible_import_observation,
                        existing["observed_at"],
                        prior_rank,
                        f"pre-observation projection; {prior_basis}",
                        prior_digest,
                        existing["parent_source_id"],
                        existing["canonical_url"],
                        existing["historical_url"],
                        existing["title"],
                        existing["published_at"],
                        existing["access_state"],
                        existing["review_state"],
                        existing["metadata_json"],
                    ),
                )

    quality_rank, quality_basis = _metadata_quality(connection, batch_id)
    import_observation_id = _import_observation_id(batch_id, observed_at)
    metadata_observation_id = stable_id(
        "smo", source, import_observation_id, candidate_digest
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO source_metadata_observations(
            source_metadata_observation_id, source_id, import_batch_id,
            import_observation_id, observed_at, quality_rank, quality_basis,
            candidate_sha256, parent_source_id, canonical_url, historical_url,
            title, published_at, access_state, review_state, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metadata_observation_id,
            source,
            batch_id,
            import_observation_id,
            observed_at,
            quality_rank,
            quality_basis,
            candidate_digest,
            parent_source,
            canonical_url,
            historical_url,
            title,
            published_at,
            access_state,
            review_state,
            metadata_text,
        ),
    )

    observations = connection.execute(
        """
        SELECT *, julianday(observed_at) AS observed_julian
        FROM source_metadata_observations
        WHERE source_id = ?
        """,
        (source,),
    ).fetchall()
    ordered = sorted(observations, key=_source_observation_sort_key, reverse=True)
    winner = ordered[0]
    earliest = min(
        observations,
        key=lambda row: (
            row["observed_julian"], row["observed_at"],
            row["source_metadata_observation_id"],
        ),
    )
    latest = max(
        observations,
        key=lambda row: (
            row["observed_julian"], row["observed_at"],
            row["source_metadata_observation_id"],
        ),
    )
    parent_value, _ = _first_present(ordered, "parent_source_id")
    canonical_value, _ = _first_present(ordered, "canonical_url")
    historical_value, _ = _first_present(ordered, "historical_url")
    title_value, _ = _first_present(ordered, "title")
    published_value, _ = _first_present(ordered, "published_at")
    current_review = connection.execute(
        "SELECT review_state FROM sources WHERE source_id = ?", (source,)
    ).fetchone()["review_state"]
    projected_review = (
        current_review
        if current_review in {"reviewed", "disputed", "rejected"}
        else winner["review_state"]
    )
    connection.execute(
        """
        UPDATE sources
        SET parent_source_id = ?, canonical_url = ?, historical_url = ?, title = ?,
            published_at = ?, observed_at = ?, access_state = ?, review_state = ?,
            metadata_json = ?, created_by_import_batch_id = ?, created_at = ?,
            updated_at = ?, current_metadata_observation_id = ?
        WHERE source_id = ?
        """,
        (
            parent_value,
            canonical_value,
            historical_value,
            title_value,
            published_value,
            latest["observed_at"],
            winner["access_state"],
            projected_review,
            winner["metadata_json"],
            earliest["import_batch_id"],
            earliest["observed_at"],
            latest["observed_at"],
            winner["source_metadata_observation_id"],
            source,
        ),
    )


def _add_source_snapshot(
    connection: sqlite3.Connection,
    *,
    source: str,
    observed_at: str,
    payload: object,
    batch_id: str,
    request_url: str | None,
    artifact_path: str,
    metadata: dict | None = None,
) -> None:
    payload_text = canonical_json(payload).encode("utf-8")
    digest = sha256_bytes(payload_text)
    snapshot_id = stable_id("ssn", source, observed_at, digest)
    connection.execute(
        """
        INSERT OR IGNORE INTO source_snapshots(
            source_snapshot_id, source_id, observed_at, request_url, final_url,
            http_status, payload_sha256, artifact_path, metadata_json, import_batch_id
        ) VALUES(?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            source,
            observed_at,
            request_url,
            request_url,
            digest,
            artifact_path,
            canonical_json(metadata or {}),
            batch_id,
        ),
    )


def _add_external_id(
    connection: sqlite3.Connection,
    *,
    object_type: str,
    object_id: str,
    namespace: str,
    value: str,
    basis: str,
    batch_id: str,
    observed_at: str,
    source: str | None = None,
) -> None:
    external_id_id = stable_id("ext", object_type, object_id, namespace, value)
    existing = connection.execute(
        "SELECT * FROM external_ids WHERE external_id_id = ?", (external_id_id,)
    ).fetchone()
    if existing and (
        existing["object_type"] != object_type
        or existing["object_id"] != object_id
        or existing["namespace"] != namespace
        or existing["external_value"] != value
    ):
        raise ValueError(f"External-ID identity collision for {external_id_id}")
    connection.execute(
        """
        INSERT OR IGNORE INTO external_ids(
            external_id_id, object_type, object_id, namespace, external_value,
            confidence_state, basis, source_id
        ) VALUES(?, ?, ?, ?, ?, 'metadata_only', ?, ?)
        """,
        (external_id_id, object_type, object_id, namespace, value, basis, source),
    )
    if existing and not connection.execute(
        "SELECT 1 FROM external_id_observations WHERE external_id_id = ? LIMIT 1",
        (external_id_id,),
    ).fetchone():
        prior_source = existing["source_id"]
        origin = (
            connection.execute(
                """
                SELECT created_by_import_batch_id AS batch_id, observed_at
                FROM sources WHERE source_id = ?
                """,
                (prior_source,),
            ).fetchone()
            if prior_source
            else None
        )
        if origin and origin["batch_id"]:
            prior_rank, prior_basis = _metadata_quality(connection, origin["batch_id"])
            prior_payload = {
                "basis": existing["basis"],
                "confidence_state": existing["confidence_state"],
                "source_id": prior_source,
            }
            prior_digest = sha256_bytes(canonical_json(prior_payload).encode("utf-8"))
            prior_id = stable_id(
                "eio", external_id_id, origin["batch_id"], origin["observed_at"], prior_digest
            )
            possible_import_observation = _existing_import_observation_id(
                connection, origin["batch_id"], origin["observed_at"]
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO external_id_observations(
                    external_id_observation_id, external_id_id, import_batch_id,
                    import_observation_id, observed_at, quality_rank, quality_basis,
                    candidate_sha256, basis, confidence_state, source_id
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prior_id,
                    external_id_id,
                    origin["batch_id"],
                    possible_import_observation,
                    origin["observed_at"],
                    prior_rank,
                    f"pre-observation projection; {prior_basis}",
                    prior_digest,
                    existing["basis"],
                    existing["confidence_state"],
                    prior_source,
                ),
            )
    quality_rank, quality_basis = _metadata_quality(connection, batch_id)
    payload = {"basis": basis, "confidence_state": "metadata_only", "source_id": source}
    candidate_digest = sha256_bytes(canonical_json(payload).encode("utf-8"))
    import_observation_id = _import_observation_id(batch_id, observed_at)
    observation_id = stable_id(
        "eio", external_id_id, import_observation_id, candidate_digest
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO external_id_observations(
            external_id_observation_id, external_id_id, import_batch_id,
            import_observation_id, observed_at, quality_rank, quality_basis,
            candidate_sha256, basis, confidence_state, source_id
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'metadata_only', ?)
        """,
        (
            observation_id,
            external_id_id,
            batch_id,
            import_observation_id,
            observed_at,
            quality_rank,
            quality_basis,
            candidate_digest,
            basis,
            source,
        ),
    )
    winner = connection.execute(
        """
        SELECT * FROM external_id_observations
        WHERE external_id_id = ?
        ORDER BY quality_rank DESC, julianday(observed_at) DESC,
                 candidate_sha256 DESC, import_batch_id DESC,
                 external_id_observation_id DESC
        LIMIT 1
        """,
        (external_id_id,),
    ).fetchone()
    connection.execute(
        """
        UPDATE external_ids
        SET confidence_state = ?, basis = ?, source_id = ?,
            current_external_id_observation_id = ?
        WHERE external_id_id = ?
        """,
        (
            winner["confidence_state"],
            winner["basis"],
            winner["source_id"],
            winner["external_id_observation_id"],
            external_id_id,
        ),
    )


def _upsert_recording(
    connection: sqlite3.Connection,
    *,
    canonical_key: str,
    title: str,
    date_label: str | None,
    date_basis: str,
    duration: int | None,
    recording_type: str,
    observed_at: str,
    batch_id: str,
    metadata: dict | None = None,
    review_state: str = "metadata_only",
) -> str:
    recording = recording_id(canonical_key)
    slug = recording_slug(recording, title, date_label)
    year = int(date_label[:4]) if date_label and re.match(r"^\d{4}", date_label) else None
    existing = connection.execute(
        "SELECT * FROM recordings WHERE recording_id = ?", (recording,)
    ).fetchone()
    if existing and existing["canonical_key"] != canonical_key:
        raise ValueError(f"Recording identity collision for {recording}")
    connection.execute(
        """
        INSERT INTO recordings(
            recording_id, canonical_key, slug, title, date_label, date_year,
            date_basis, duration_ms, recording_type, review_state, metadata_json,
            created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO NOTHING
        """,
        (
            recording,
            canonical_key,
            slug,
            title,
            date_label,
            year,
            date_basis,
            duration,
            recording_type,
            review_state,
            canonical_json(metadata or {}),
            observed_at,
            observed_at,
        ),
    )

    if existing and not connection.execute(
        "SELECT 1 FROM recording_metadata_observations WHERE recording_id = ? LIMIT 1",
        (recording,),
    ).fetchone():
        origin = connection.execute(
            """
            SELECT source.created_by_import_batch_id AS import_batch_id
            FROM recording_sources AS link
            JOIN sources AS source ON source.source_id = link.source_id
            WHERE link.recording_id = ?
              AND source.created_by_import_batch_id IS NOT NULL
            ORDER BY julianday(source.observed_at), source.source_id
            LIMIT 1
            """,
            (recording,),
        ).fetchone()
        if origin:
            origin_batch = origin["import_batch_id"]
            prior_rank, prior_basis = _metadata_quality(connection, origin_batch)
            prior_payload = {
                "title": existing["title"],
                "date_label": existing["date_label"],
                "date_basis": existing["date_basis"],
                "duration_ms": existing["duration_ms"],
                "recording_type": existing["recording_type"],
                "review_state": existing["review_state"],
                "metadata": json.loads(existing["metadata_json"]),
            }
            prior_digest = sha256_bytes(canonical_json(prior_payload).encode("utf-8"))
            prior_id = stable_id(
                "rmo", recording, origin_batch, existing["updated_at"], prior_digest
            )
            possible_import_observation = _existing_import_observation_id(
                connection, origin_batch, existing["updated_at"]
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO recording_metadata_observations(
                    recording_metadata_observation_id, recording_id, import_batch_id,
                    import_observation_id, observed_at, quality_rank, quality_basis,
                    candidate_sha256, title, date_label, date_basis, duration_ms,
                    recording_type, review_state, metadata_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prior_id,
                    recording,
                    origin_batch,
                    possible_import_observation,
                    existing["updated_at"],
                    prior_rank,
                    f"pre-observation projection; {prior_basis}",
                    prior_digest,
                    existing["title"],
                    existing["date_label"],
                    existing["date_basis"],
                    existing["duration_ms"],
                    existing["recording_type"],
                    existing["review_state"],
                    existing["metadata_json"],
                ),
            )

    quality_rank, quality_basis = _metadata_quality(connection, batch_id)
    candidate_payload = {
        "title": title,
        "date_label": date_label,
        "date_basis": date_basis,
        "duration_ms": duration,
        "recording_type": recording_type,
        "review_state": review_state,
        "metadata": metadata or {},
    }
    candidate_digest = sha256_bytes(canonical_json(candidate_payload).encode("utf-8"))
    import_observation_id = _import_observation_id(batch_id, observed_at)
    metadata_observation_id = stable_id(
        "rmo", recording, import_observation_id, candidate_digest
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO recording_metadata_observations(
            recording_metadata_observation_id, recording_id, import_batch_id,
            import_observation_id, observed_at, quality_rank, quality_basis,
            candidate_sha256, title, date_label, date_basis, duration_ms,
            recording_type, review_state, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metadata_observation_id,
            recording,
            batch_id,
            import_observation_id,
            observed_at,
            quality_rank,
            quality_basis,
            candidate_digest,
            title,
            date_label,
            date_basis,
            duration,
            recording_type,
            review_state,
            canonical_json(metadata or {}),
        ),
    )
    observations = connection.execute(
        """
        SELECT *, julianday(observed_at) AS observed_julian
        FROM recording_metadata_observations
        WHERE recording_id = ?
        """,
        (recording,),
    ).fetchall()
    ordered = sorted(observations, key=_recording_observation_sort_key, reverse=True)
    winner = ordered[0]
    earliest = min(
        observations,
        key=lambda row: (
            row["observed_julian"], row["observed_at"],
            row["recording_metadata_observation_id"],
        ),
    )
    latest = max(
        observations,
        key=lambda row: (
            row["observed_julian"], row["observed_at"],
            row["recording_metadata_observation_id"],
        ),
    )
    selected_title, _ = _first_present(ordered, "title")
    selected_date, date_observation = _first_present(ordered, "date_label")
    selected_duration, _ = _first_present(ordered, "duration_ms")
    selected_type, _ = _first_present(ordered, "recording_type", reject={"unknown"})
    if selected_type is None:
        selected_type = "unknown"
    selected_date_basis = (
        date_observation["date_basis"] if date_observation else winner["date_basis"]
    )
    selected_year = (
        int(selected_date[:4])
        if selected_date and re.match(r"^\d{4}", selected_date)
        else None
    )
    selected_slug = recording_slug(recording, selected_title, selected_date)
    current_review = connection.execute(
        "SELECT review_state FROM recordings WHERE recording_id = ?", (recording,)
    ).fetchone()["review_state"]
    projected_review = (
        current_review
        if current_review in {"reviewed", "disputed", "rejected", "merged"}
        else winner["review_state"]
    )
    connection.execute(
        """
        UPDATE recordings
        SET slug = ?, title = ?, date_label = ?, date_year = ?, date_basis = ?,
            duration_ms = ?, recording_type = ?, review_state = ?, metadata_json = ?,
            created_at = ?, updated_at = ?, current_metadata_observation_id = ?
        WHERE recording_id = ?
        """,
        (
            selected_slug,
            selected_title,
            selected_date,
            selected_year,
            selected_date_basis,
            selected_duration,
            selected_type,
            projected_review,
            winner["metadata_json"],
            earliest["observed_at"],
            latest["observed_at"],
            winner["recording_metadata_observation_id"],
            recording,
        ),
    )
    return recording


def _attach_recording_source(
    connection: sqlite3.Connection,
    *,
    recording: str,
    source: str,
    role: str,
    method: str,
    metadata: dict | None = None,
    confidence_state: str = "metadata_only",
) -> None:
    mapping_id = stable_id("rso", recording, source, role)
    connection.execute(
        """
        INSERT OR IGNORE INTO recording_sources(
            recording_source_id, recording_id, source_id, mapping_role,
            mapping_method, confidence_state, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mapping_id,
            recording,
            source,
            role,
            method,
            confidence_state,
            canonical_json(metadata or {}),
        ),
    )


def _relate_sources(
    connection: sqlite3.Connection,
    *,
    from_source: str,
    relation_kind: str,
    to_source: str,
    basis: str,
    batch_id: str,
    observed_at: str,
    metadata: dict | None = None,
) -> None:
    relation_id = stable_id("sre", from_source, relation_kind, to_source)
    existing = connection.execute(
        "SELECT * FROM source_relations WHERE source_relation_id = ?", (relation_id,)
    ).fetchone()
    if existing and (
        existing["from_source_id"] != from_source
        or existing["relation_kind"] != relation_kind
        or existing["to_source_id"] != to_source
    ):
        raise ValueError(f"Source-relation identity collision for {relation_id}")
    connection.execute(
        """
        INSERT OR IGNORE INTO source_relations(
            source_relation_id, from_source_id, relation_kind, to_source_id,
            basis, confidence_state, metadata_json, import_batch_id
        ) VALUES(?, ?, ?, ?, ?, 'metadata_only', ?, ?)
        """,
        (
            relation_id,
            from_source,
            relation_kind,
            to_source,
            basis,
            canonical_json(metadata or {}),
            batch_id,
        ),
    )
    if existing and not connection.execute(
        "SELECT 1 FROM source_relation_observations WHERE source_relation_id = ? LIMIT 1",
        (relation_id,),
    ).fetchone():
        origin_batch = existing["import_batch_id"]
        if origin_batch:
            prior_time_row = connection.execute(
                "SELECT observed_at FROM sources WHERE source_id = ?", (from_source,)
            ).fetchone()
            prior_time = prior_time_row["observed_at"]
            prior_rank, prior_basis = _metadata_quality(connection, origin_batch)
            prior_payload = {
                "basis": existing["basis"],
                "confidence_state": existing["confidence_state"],
                "metadata": json.loads(existing["metadata_json"]),
            }
            prior_digest = sha256_bytes(canonical_json(prior_payload).encode("utf-8"))
            prior_id = stable_id(
                "sro", relation_id, origin_batch, prior_time, prior_digest
            )
            possible_import_observation = _existing_import_observation_id(
                connection, origin_batch, prior_time
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO source_relation_observations(
                    source_relation_observation_id, source_relation_id, import_batch_id,
                    import_observation_id, observed_at, quality_rank, quality_basis,
                    candidate_sha256, basis, confidence_state, metadata_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prior_id,
                    relation_id,
                    origin_batch,
                    possible_import_observation,
                    prior_time,
                    prior_rank,
                    f"pre-observation projection; {prior_basis}",
                    prior_digest,
                    existing["basis"],
                    existing["confidence_state"],
                    existing["metadata_json"],
                ),
            )
    metadata_text = canonical_json(metadata or {})
    quality_rank, quality_basis = _metadata_quality(connection, batch_id)
    payload = {
        "basis": basis,
        "confidence_state": "metadata_only",
        "metadata": metadata or {},
    }
    candidate_digest = sha256_bytes(canonical_json(payload).encode("utf-8"))
    import_observation_id = _import_observation_id(batch_id, observed_at)
    observation_id = stable_id(
        "sro", relation_id, import_observation_id, candidate_digest
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO source_relation_observations(
            source_relation_observation_id, source_relation_id, import_batch_id,
            import_observation_id, observed_at, quality_rank, quality_basis,
            candidate_sha256, basis, confidence_state, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'metadata_only', ?)
        """,
        (
            observation_id,
            relation_id,
            batch_id,
            import_observation_id,
            observed_at,
            quality_rank,
            quality_basis,
            candidate_digest,
            basis,
            metadata_text,
        ),
    )
    winner = connection.execute(
        """
        SELECT * FROM source_relation_observations
        WHERE source_relation_id = ?
        ORDER BY quality_rank DESC, julianday(observed_at) DESC,
                 candidate_sha256 DESC, import_batch_id DESC,
                 source_relation_observation_id DESC
        LIMIT 1
        """,
        (relation_id,),
    ).fetchone()
    connection.execute(
        """
        UPDATE source_relations
        SET basis = ?, confidence_state = ?, metadata_json = ?, import_batch_id = ?,
            current_relation_observation_id = ?
        WHERE source_relation_id = ?
        """,
        (
            winner["basis"],
            winner["confidence_state"],
            winner["metadata_json"],
            winner["import_batch_id"],
            winner["source_relation_observation_id"],
            relation_id,
        ),
    )


def import_internet_archive(
    connection: sqlite3.Connection,
    metadata_paths: list[Path],
    *,
    snapshot_date: str | None,
    observed_at: str,
    manage_transaction: bool = True,
) -> dict:
    paths = [Path(path) for path in metadata_paths]
    digest = combined_digest(paths)
    transaction_scope = transaction(connection) if manage_transaction else nullcontext()
    with transaction_scope:
        batch_id, existing = _begin_batch(
            connection, "internet_archive_metadata", digest, snapshot_date, observed_at
        )
        if existing is not None:
            return existing

        statistics = Counter()
        derivative_links: list[tuple[str, str, str]] = []
        for metadata_path in sorted(paths, key=lambda item: item.name):
            document = load_json(metadata_path)
            if not isinstance(document, dict):
                raise ValueError(f"{metadata_path}: expected an object")
            item = str((document.get("metadata") or {}).get("identifier") or "")
            if not item:
                match = re.search(r"(\d+)", metadata_path.stem)
                if not match:
                    raise ValueError(f"Cannot determine Archive.org item for {metadata_path}")
                item = match.group(1)

            item_source = source_id("internet_archive", "archive_item", item)
            item_url = f"https://archive.org/details/{quote(item, safe='')}"
            item_title = str((document.get("metadata") or {}).get("title") or item)
            _upsert_source(
                connection,
                source=item_source,
                platform="internet_archive",
                source_kind="archive_item",
                native_id=item,
                canonical_url=item_url,
                title=item_title,
                observed_at=observed_at,
                access_state="public",
                batch_id=batch_id,
                metadata={"snapshot_file": metadata_path.name},
            )
            _add_source_snapshot(
                connection,
                source=item_source,
                observed_at=observed_at,
                payload={"identifier": item, "title": item_title, "file_count": len(document.get("files") or [])},
                batch_id=batch_id,
                request_url=f"https://archive.org/metadata/{quote(item, safe='')}",
                artifact_path=str(metadata_path),
            )
            statistics["archive_items"] += 1

            files = document.get("files") or []
            for file_record in sorted(files, key=lambda value: str(value.get("name") or "")):
                filename = file_record.get("name")
                if not isinstance(filename, str) or not VIDEO_EXTENSION_RE.search(filename):
                    continue
                native_id = f"{item}/{filename}"
                file_source = source_id("internet_archive", "archive_media_file", native_id)
                media_url = (
                    f"https://archive.org/download/{quote(item, safe='')}/"
                    f"{quote(filename, safe='')}"
                )
                title = title_from_media_filename(filename)
                date_label = date_label_from_filename(filename)
                source_class = str(file_record.get("source") or "unknown")
                derivative_of = file_record.get("original")
                selected_metadata = {
                    "internet_archive_item": item,
                    "filename": filename,
                    "format": file_record.get("format"),
                    "source_class": source_class,
                    "derivative_of": derivative_of,
                    "byte_count": byte_count(file_record.get("size")),
                    "duration_ms": duration_ms(file_record.get("length")),
                }
                _upsert_source(
                    connection,
                    source=file_source,
                    platform="internet_archive",
                    source_kind="archive_media_file",
                    native_id=native_id,
                    parent_source=item_source,
                    canonical_url=media_url,
                    title=title,
                    observed_at=observed_at,
                    access_state="public",
                    batch_id=batch_id,
                    metadata=selected_metadata,
                )
                _add_source_snapshot(
                    connection,
                    source=file_source,
                    observed_at=observed_at,
                    payload=selected_metadata,
                    batch_id=batch_id,
                    request_url=f"https://archive.org/metadata/{quote(item, safe='')}",
                    artifact_path=str(metadata_path),
                )
                _add_external_id(
                    connection,
                    object_type="source",
                    object_id=file_source,
                    namespace="internet_archive_item_filename",
                    value=native_id,
                    basis="Archive.org item metadata",
                    batch_id=batch_id,
                    observed_at=observed_at,
                    source=file_source,
                )
                for algorithm in ("md5", "sha1", "crc32"):
                    hash_value = file_record.get(algorithm)
                    if isinstance(hash_value, str) and hash_value:
                        connection.execute(
                            """
                            INSERT OR IGNORE INTO source_hashes(
                                source_id, algorithm, digest, declared_by, observed_at
                            ) VALUES(?, ?, ?, 'internet_archive_metadata', ?)
                            """,
                            (file_source, algorithm, hash_value.lower(), observed_at),
                        )

                canonical_key = recording_key_for_archive(item, filename)
                recording = _upsert_recording(
                    connection,
                    canonical_key=canonical_key,
                    title=title,
                    date_label=date_label,
                    date_basis="internet_archive_filename",
                    duration=duration_ms(file_record.get("length")),
                    recording_type=infer_recording_type(title),
                    observed_at=observed_at,
                    batch_id=batch_id,
                    metadata={"identity_basis": "archive_filename_or_platform_id"},
                )
                _attach_recording_source(
                    connection,
                    recording=recording,
                    source=file_source,
                    role=("archive_original_file" if source_class == "original" else "archive_derivative_file"),
                    method="archive_filename_platform_id_grouping",
                    metadata={"source_class": source_class},
                )
                video_id = platform_video_id(filename)
                if video_id:
                    _add_external_id(
                        connection,
                        object_type="recording",
                        object_id=recording,
                        namespace="youtube_video_id_candidate",
                        value=video_id,
                        basis="Eleven-character filename suffix",
                        batch_id=batch_id,
                        observed_at=observed_at,
                        source=file_source,
                    )
                if isinstance(derivative_of, str) and derivative_of:
                    derivative_links.append((file_source, item, derivative_of))
                statistics["video_files"] += 1
                statistics[f"source_class_{source_class}"] += 1

        for derivative_source, item, original_filename in derivative_links:
            original_source = source_id(
                "internet_archive", "archive_media_file", f"{item}/{original_filename}"
            )
            if connection.execute(
                "SELECT 1 FROM sources WHERE source_id = ?", (original_source,)
            ).fetchone():
                _relate_sources(
                    connection,
                    from_source=derivative_source,
                    relation_kind="derivative_of",
                    to_source=original_source,
                    basis="Archive.org file metadata original field",
                    batch_id=batch_id,
                    observed_at=observed_at,
                )
                statistics["derivative_links"] += 1

        statistics["recordings_after_import"] = connection.execute(
            "SELECT count(*) FROM recordings"
        ).fetchone()[0]
        result = dict(sorted(statistics.items()))
        _complete_batch(connection, batch_id, observed_at, result)
        return result


def import_legacy_manifest(
    connection: sqlite3.Connection,
    manifest_path: Path,
    *,
    snapshot_date: str | None,
    observed_at: str,
) -> dict:
    manifest_path = Path(manifest_path)
    digest = sha256_bytes(manifest_path.read_bytes())
    records = load_jsonl(manifest_path)
    with transaction(connection):
        batch_id, existing = _begin_batch(
            connection, "legacy_source_manifest_metadata", digest, snapshot_date, observed_at
        )
        if existing is not None:
            return existing

        statistics = Counter()
        mapped_groups: dict[str, list[str]] = defaultdict(list)
        for record in records:
            legacy_id = record.get("source_record_id")
            if not isinstance(legacy_id, str) or not legacy_id:
                raise ValueError("Legacy manifest entry is missing source_record_id")
            transcript = record.get("transcript") or {}
            video = record.get("video") or {}
            checks = record.get("checks") or {}
            legacy_source = source_id(
                "legacy_himr_archive", "legacy_catalog_entry", legacy_id
            )
            historical_url = transcript.get("url") if isinstance(transcript.get("url"), str) else None
            title = str(transcript.get("title") or legacy_id)
            # Deliberately excludes machine_summary and speakers. Transcript-derived
            # counts remain quarantined metadata and are never exported or converted
            # into transcript revisions.
            legacy_metadata = {
                "archive_path": transcript.get("archive_path"),
                "source_filename": transcript.get("source_filename"),
                "date_label": transcript.get("date_label"),
                "date_basis": transcript.get("date_basis"),
                "year": transcript.get("year"),
                "legacy_audit": {
                    "word_count": transcript.get("word_count"),
                    "displayed_duration": transcript.get("displayed_duration"),
                    "displayed_duration_seconds": transcript.get("displayed_duration_seconds"),
                    "checks": checks,
                },
            }
            _upsert_source(
                connection,
                source=legacy_source,
                platform="legacy_himr_archive",
                source_kind="legacy_catalog_entry",
                native_id=legacy_id,
                historical_url=historical_url,
                title=title,
                observed_at=observed_at,
                access_state="unavailable",
                batch_id=batch_id,
                metadata=legacy_metadata,
            )
            _add_source_snapshot(
                connection,
                source=legacy_source,
                observed_at=observed_at,
                payload=legacy_metadata,
                batch_id=batch_id,
                request_url=historical_url,
                artifact_path=str(manifest_path),
                metadata={"legacy_record_id": legacy_id},
            )
            _add_external_id(
                connection,
                object_type="source",
                object_id=legacy_source,
                namespace="legacy_source_record_id",
                value=legacy_id,
                basis="Preserved normalized legacy manifest",
                batch_id=batch_id,
                observed_at=observed_at,
                source=legacy_source,
            )
            statistics["legacy_entries"] += 1

            item = video.get("internet_archive_item")
            filename = video.get("internet_archive_filename")
            if isinstance(item, str) and item and isinstance(filename, str) and filename:
                archive_source = source_id(
                    "internet_archive", "archive_media_file", f"{item}/{filename}"
                )
                if not connection.execute(
                    "SELECT 1 FROM sources WHERE source_id = ?", (archive_source,)
                ).fetchone():
                    raise ValueError(
                        f"Legacy mapping {legacy_id} points to an unimported Archive.org file"
                    )
                relation_metadata = {
                    "url_basis": video.get("url_basis"),
                    "upstream_url": video.get("upstream_url"),
                    "reconciliation_basis": video.get("reconciliation_basis"),
                    "manifest_video_url": video.get("url"),
                }
                _relate_sources(
                    connection,
                    from_source=legacy_source,
                    relation_kind="mapped_to",
                    to_source=archive_source,
                    basis=(
                        str(video.get("reconciliation_basis"))
                        if video.get("url_basis") == "archive_metadata_reconciliation"
                        else "Preserved legacy manifest Archive.org filename mapping"
                    ),
                    batch_id=batch_id,
                    observed_at=observed_at,
                    metadata=relation_metadata,
                )
                recording_row = connection.execute(
                    "SELECT recording_id FROM recording_sources WHERE source_id = ? ORDER BY recording_id LIMIT 1",
                    (archive_source,),
                ).fetchone()
                if recording_row:
                    _attach_recording_source(
                        connection,
                        recording=recording_row["recording_id"],
                        source=legacy_source,
                        role="legacy_catalog_mapping",
                        method="preserved_manifest_mapping",
                    )
                mapped_groups[str(video.get("url") or f"{item}/{filename}")].append(legacy_source)
                statistics["mapped_entries"] += 1
                if video.get("url_basis") == "archive_metadata_reconciliation":
                    statistics["explicit_reconciliations"] += 1
            else:
                task_id = stable_id("rtk", "source_recovery", legacy_source)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_tasks(
                        review_task_id, task_kind, target_type, target_id, reason,
                        priority, status, created_at, updated_at
                    ) VALUES(?, 'source_recovery', 'source', ?, ?, 60, 'open', ?, ?)
                    """,
                    (
                        task_id,
                        legacy_source,
                        "Legacy catalog entry has no mapped raw-video source",
                        observed_at,
                        observed_at,
                    ),
                )
                statistics["unresolved_entries"] += 1

        for mapping_key, legacy_sources in sorted(mapped_groups.items()):
            if len(legacy_sources) <= 1:
                continue
            first_relation = connection.execute(
                """
                SELECT to_source_id FROM source_relations
                WHERE from_source_id = ? AND relation_kind = 'mapped_to'
                """,
                (legacy_sources[0],),
            ).fetchone()
            if first_relation:
                target = first_relation["to_source_id"]
                task_id = stable_id("rtk", "shared_legacy_mapping", target)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_tasks(
                        review_task_id, task_kind, target_type, target_id, reason,
                        priority, status, created_at, updated_at
                    ) VALUES(?, 'shared_legacy_mapping', 'source', ?, ?, 50, 'open', ?, ?)
                    """,
                    (
                        task_id,
                        target,
                        f"{len(legacy_sources)} legacy records share one media mapping; review for duplicate, chunk, or bad mapping",
                        observed_at,
                        observed_at,
                    ),
                )
                statistics["shared_mapping_groups"] += 1
                statistics["entries_in_shared_mapping_groups"] += len(legacy_sources)

        statistics["old_transcript_revisions_imported"] = connection.execute(
            "SELECT count(*) FROM transcript_revisions WHERE origin LIKE 'legacy%'"
        ).fetchone()[0]
        result = dict(sorted(statistics.items()))
        _complete_batch(connection, batch_id, observed_at, result)
        return result


def import_current_channel(
    connection: sqlite3.Connection,
    inventory_path: Path,
    *,
    snapshot_date: str | None,
    observed_at: str,
) -> dict:
    inventory_path = Path(inventory_path)
    digest = sha256_bytes(inventory_path.read_bytes())
    inventory = load_json(inventory_path)
    if not isinstance(inventory, dict):
        raise ValueError(f"{inventory_path}: expected an object")
    with transaction(connection):
        batch_id, existing = _begin_batch(
            connection, "current_youtube_channel_inventory", digest, snapshot_date, observed_at
        )
        if existing is not None:
            return existing

        statistics = Counter()
        channel_data = inventory.get("expected_channel") or inventory.get("observed_channel") or {}
        channel_id = str(
            channel_data.get("stable_channel_id")
            or channel_data.get("channel_id")
            or "UC_yIF-9jOge6nNA0z-ScrBQ"
        )
        channel_source = source_id("youtube", "channel", channel_id)
        _upsert_source(
            connection,
            source=channel_source,
            platform="youtube",
            source_kind="channel",
            native_id=channel_id,
            canonical_url=f"https://www.youtube.com/channel/{quote(channel_id, safe='')}",
            title=str(channel_data.get("display_name") or "Hiding in my room"),
            observed_at=observed_at,
            access_state="public",
            batch_id=batch_id,
            metadata={"snapshot_file": inventory_path.name},
        )
        _add_source_snapshot(
            connection,
            source=channel_source,
            observed_at=observed_at,
            payload=channel_data,
            batch_id=batch_id,
            request_url=f"https://www.youtube.com/channel/{quote(channel_id, safe='')}",
            artifact_path=str(inventory_path),
        )
        _add_external_id(
            connection,
            object_type="source",
            object_id=channel_source,
            namespace="youtube_channel_id",
            value=channel_id,
            basis="Captured channel inventory",
            batch_id=batch_id,
            observed_at=observed_at,
            source=channel_source,
        )
        statistics["channels"] += 1

        items = list(inventory.get("items") or []) + list(
            inventory.get("additional_observed_items") or []
        )
        seen: set[str] = set()
        for entry in sorted(items, key=lambda value: str(value.get("video_id") or "")):
            video_id = entry.get("video_id")
            if not isinstance(video_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
                continue
            if video_id in seen:
                continue
            seen.add(video_id)
            observed = entry.get("observed") or {}
            title = str(observed.get("title") or entry.get("title_label") or video_id)
            published_at = observed.get("publish_date_utc") or entry.get("published_utc")
            date_label = str(published_at)[:10] if published_at else None
            item_duration = observed.get("duration_seconds")
            if item_duration is None:
                item_duration = entry.get("duration_seconds")
            access = str(observed.get("inferred_access") or entry.get("access") or "unknown")
            if access not in {"public", "members_only", "private", "removed", "unavailable", "unknown"}:
                access = "unknown"
            video_source = source_id("youtube", "youtube_video", video_id)
            watch_url = str(entry.get("watch_url") or f"https://www.youtube.com/watch?v={video_id}")
            selected_metadata = {
                "video_id": video_id,
                "inventory_type": entry.get("type"),
                "playability_status": observed.get("playability_status"),
                "is_live_content": observed.get("is_live_content"),
                "snapshot_matches_expected": entry.get("matches_expected"),
            }
            _upsert_source(
                connection,
                source=video_source,
                platform="youtube",
                source_kind="youtube_video",
                native_id=video_id,
                parent_source=channel_source,
                canonical_url=watch_url,
                title=title,
                published_at=str(published_at) if published_at else None,
                observed_at=observed_at,
                access_state=access,
                batch_id=batch_id,
                metadata=selected_metadata,
            )
            _add_source_snapshot(
                connection,
                source=video_source,
                observed_at=observed_at,
                payload=selected_metadata,
                batch_id=batch_id,
                request_url=watch_url,
                artifact_path=str(inventory_path),
            )
            _relate_sources(
                connection,
                from_source=video_source,
                relation_kind="published_by",
                to_source=channel_source,
                basis="Captured current-channel inventory",
                batch_id=batch_id,
                observed_at=observed_at,
            )
            recording = _upsert_recording(
                connection,
                canonical_key=f"youtube:video:{video_id}",
                title=title,
                date_label=date_label,
                date_basis="youtube_publish_metadata",
                duration=duration_ms(item_duration),
                recording_type=infer_recording_type(title, entry.get("type")),
                observed_at=observed_at,
                batch_id=batch_id,
                metadata={"identity_basis": "stable_youtube_video_id"},
            )
            _attach_recording_source(
                connection,
                recording=recording,
                source=video_source,
                role="current_platform_listing",
                method="stable_youtube_video_id",
            )
            _add_external_id(
                connection,
                object_type="recording",
                object_id=recording,
                namespace="youtube_video_id",
                value=video_id,
                basis="Captured YouTube watch-page inventory",
                batch_id=batch_id,
                observed_at=observed_at,
                source=video_source,
            )
            statistics["videos"] += 1
            statistics[f"access_{access}"] += 1

        result = dict(sorted(statistics.items()))
        _complete_batch(connection, batch_id, observed_at, result)
        return result


def import_youtube_discovery_candidates(
    connection: sqlite3.Connection,
    candidates_path: Path,
    *,
    observed_at: str,
    query_label: str,
) -> dict:
    """Import yt-dlp search rows as unreviewed discovery leads only."""

    candidates_path = Path(candidates_path)
    digest = sha256_bytes(candidates_path.read_bytes())
    records = load_jsonl(candidates_path)
    with transaction(connection):
        batch_id, existing = _begin_batch(
            connection, "youtube_discovery_candidates", digest, observed_at[:10], observed_at
        )
        if existing is not None:
            return existing
        statistics = Counter()
        seen: set[str] = set()
        for record in records:
            video_id = record.get("id")
            if not isinstance(video_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
                statistics["invalid_rows"] += 1
                continue
            if video_id in seen:
                statistics["duplicate_rows"] += 1
                continue
            seen.add(video_id)
            video_source = source_id("youtube", "youtube_video", video_id)
            existing_source = connection.execute(
                "SELECT review_state FROM sources WHERE source_id = ?", (video_source,)
            ).fetchone()
            selected_metadata = {
                "discovery_state": "search_candidate",
                "query_label": query_label,
                "channel_label": record.get("channel"),
                "candidate_title": record.get("title"),
            }
            if existing_source:
                statistics["deduplicated_existing_sources"] += 1
            else:
                statistics["new_candidate_sources"] += 1
            # Always retain the discovery assertion.  Skipping it for a source that
            # happened to be imported first made provenance and winner inputs depend
            # on importer order.
            title = str(record.get("title") or video_id)
            _upsert_source(
                connection,
                source=video_source,
                platform="youtube",
                source_kind="youtube_video",
                native_id=video_id,
                canonical_url=str(
                    record.get("url") or f"https://www.youtube.com/watch?v={video_id}"
                ),
                title=title,
                observed_at=observed_at,
                access_state="unknown",
                review_state="unreviewed",
                batch_id=batch_id,
                metadata=selected_metadata,
            )
            recording = _upsert_recording(
                connection,
                canonical_key=f"youtube:video:{video_id}",
                title=title,
                date_label=None,
                date_basis="unknown",
                duration=None,
                recording_type="unknown",
                observed_at=observed_at,
                batch_id=batch_id,
                review_state="unreviewed",
                metadata={"identity_basis": "yt_dlp_search_candidate"},
            )
            _attach_recording_source(
                connection,
                recording=recording,
                source=video_source,
                role="discovery_candidate",
                method="yt_dlp_search_result",
                confidence_state="candidate",
                metadata={"query_label": query_label},
            )
            task_id = stable_id("rtk", "youtube_discovery_candidate", video_source)
            connection.execute(
                """
                INSERT OR IGNORE INTO review_tasks(
                    review_task_id, task_kind, target_type, target_id, reason,
                    priority, status, created_at, updated_at
                ) VALUES(?, 'youtube_discovery_candidate', 'source', ?, ?, 80, 'open', ?, ?)
                """,
                (
                    task_id,
                    video_source,
                    f"Search candidate from {query_label}; review relevance before acquisition or publication",
                    observed_at,
                    observed_at,
                ),
            )
            _add_source_snapshot(
                connection,
                source=video_source,
                observed_at=observed_at,
                payload=selected_metadata,
                batch_id=batch_id,
                request_url=str(record.get("url") or f"https://www.youtube.com/watch?v={video_id}"),
                artifact_path=str(candidates_path),
            )
            statistics["candidate_rows"] += 1
        result = dict(sorted(statistics.items()))
        _complete_batch(connection, batch_id, observed_at, result)
        return result


def import_ytdlp_info(
    connection: sqlite3.Connection,
    info_path: Path,
    *,
    observed_at: str,
) -> dict:
    """Import selected public metadata from one validated yt-dlp info JSON."""

    info_path = Path(info_path)
    digest = sha256_bytes(info_path.read_bytes())
    info = load_json(info_path)
    if not isinstance(info, dict):
        raise ValueError(f"{info_path}: expected an object")
    video_id = info.get("id")
    if not isinstance(video_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
        raise ValueError(f"{info_path}: missing valid YouTube video ID")
    with transaction(connection):
        batch_id, existing = _begin_batch(
            connection, "ytdlp_info_metadata", digest, observed_at[:10], observed_at
        )
        if existing is not None:
            return existing
        channel_id = info.get("channel_id")
        channel_source = None
        if isinstance(channel_id, str) and channel_id:
            channel_source = source_id("youtube", "channel", channel_id)
            _upsert_source(
                connection,
                source=channel_source,
                platform="youtube",
                source_kind="channel",
                native_id=channel_id,
                canonical_url=str(info.get("channel_url") or f"https://www.youtube.com/channel/{channel_id}"),
                title=str(info.get("channel") or info.get("uploader") or channel_id),
                observed_at=observed_at,
                access_state="public",
                batch_id=batch_id,
                metadata={"metadata_source": "yt_dlp_info"},
            )
        published_at = None
        timestamp_value = numeric(info.get("timestamp"))
        if timestamp_value is not None:
            published_at = datetime.fromtimestamp(timestamp_value, timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )
        elif isinstance(info.get("upload_date"), str) and re.fullmatch(
            r"\d{8}", info["upload_date"]
        ):
            published_at = (
                f"{info['upload_date'][:4]}-{info['upload_date'][4:6]}-"
                f"{info['upload_date'][6:8]}T00:00:00Z"
            )
        availability = str(info.get("availability") or "unknown")
        access = availability if availability in {
            "public",
            "members_only",
            "private",
            "removed",
            "unavailable",
            "unknown",
        } else "unknown"
        title = str(info.get("title") or video_id)
        video_source = source_id("youtube", "youtube_video", video_id)
        selected_metadata = {
            "metadata_source": "yt_dlp_info",
            "extractor": info.get("extractor"),
            "extractor_key": info.get("extractor_key"),
            "channel_label": info.get("channel"),
            "channel_id": channel_id,
            "uploader_label": info.get("uploader"),
            "duration_ms": duration_ms(info.get("duration")),
            "live_status": info.get("live_status"),
            "was_live": info.get("was_live"),
            "availability": availability,
        }
        _upsert_source(
            connection,
            source=video_source,
            platform="youtube",
            source_kind="youtube_video",
            native_id=video_id,
            parent_source=channel_source,
            canonical_url=str(info.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}"),
            title=title,
            published_at=published_at,
            observed_at=observed_at,
            access_state=access,
            review_state="metadata_only",
            batch_id=batch_id,
            metadata=selected_metadata,
        )
        _add_source_snapshot(
            connection,
            source=video_source,
            observed_at=observed_at,
            payload=selected_metadata,
            batch_id=batch_id,
            request_url=str(info.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}"),
            artifact_path=str(info_path),
        )
        if channel_source:
            _relate_sources(
                connection,
                from_source=video_source,
                relation_kind="published_by",
                to_source=channel_source,
                basis="Validated yt-dlp info JSON channel_id",
                batch_id=batch_id,
                observed_at=observed_at,
            )
        date_label = published_at[:10] if published_at else None
        recording = _upsert_recording(
            connection,
            canonical_key=f"youtube:video:{video_id}",
            title=title,
            date_label=date_label,
            date_basis="youtube_publish_metadata",
            duration=duration_ms(info.get("duration")),
            recording_type=("livestream" if info.get("was_live") else "video"),
            observed_at=observed_at,
            batch_id=batch_id,
            metadata={"identity_basis": "validated_ytdlp_info"},
        )
        _attach_recording_source(
            connection,
            recording=recording,
            source=video_source,
            role="validated_platform_listing",
            method="stable_youtube_video_id",
        )
        _add_external_id(
            connection,
            object_type="recording",
            object_id=recording,
            namespace="youtube_video_id",
            value=video_id,
            basis="Validated yt-dlp info JSON",
            batch_id=batch_id,
            observed_at=observed_at,
            source=video_source,
        )
        result = {"videos": 1, "channels": int(channel_source is not None)}
        _complete_batch(connection, batch_id, observed_at, result)
        return result


def import_ytdlp_infos(
    connection: sqlite3.Connection,
    info_inputs: list[Path],
    *,
    observed_at: str,
) -> dict:
    """Import validated info JSON files from repeatable file or directory inputs.

    Directories are intentionally non-recursive and include only ``*.info.json``.
    Native video IDs must be unique within one invocation so two conflicting
    captures cannot silently win based on filesystem traversal order.
    """

    expanded: dict[str, Path] = {}
    for value in info_inputs:
        path = Path(value)
        candidates = sorted(path.glob("*.info.json")) if path.is_dir() else [path]
        if not candidates:
            raise ValueError(f"{path}: no *.info.json files found")
        for candidate in candidates:
            if not candidate.is_file():
                raise ValueError(f"{candidate}: expected an info JSON file")
            expanded[str(candidate.resolve())] = candidate

    selected: list[tuple[str, Path]] = []
    by_video_id: dict[str, Path] = {}
    for path in sorted(expanded.values(), key=lambda item: item.as_posix()):
        info = load_json(path)
        video_id = info.get("id") if isinstance(info, dict) else None
        if not isinstance(video_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            raise ValueError(f"{path}: missing valid YouTube video ID")
        if video_id in by_video_id:
            raise ValueError(
                f"duplicate YouTube video ID {video_id!r}: {by_video_id[video_id]} and {path}"
            )
        by_video_id[video_id] = path
        selected.append((video_id, path))

    totals = Counter()
    channel_ids: set[str] = set()
    for _, path in sorted(selected, key=lambda item: (item[0], item[1].as_posix())):
        info = load_json(path)
        if isinstance(info.get("channel_id"), str) and info["channel_id"]:
            channel_ids.add(info["channel_id"])
        result = import_ytdlp_info(connection, path, observed_at=observed_at)
        totals.update(result)
    totals["info_files"] = len(selected)
    totals["unique_channels"] = len(channel_ids)
    return dict(sorted(totals.items()))


def import_archive_url_hints(
    connection: sqlite3.Connection,
    hints_path: Path,
    *,
    observed_at: str,
    reddit_post_id: str,
) -> dict:
    """Import a public URL list as coverage hints, never as verified media."""

    hints_path = Path(hints_path)
    digest = sha256_bytes(hints_path.read_bytes())
    urls = [line.strip() for line in hints_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    with transaction(connection):
        batch_id, existing = _begin_batch(
            connection, "archive_url_discovery_hints", digest, observed_at[:10], observed_at
        )
        if existing is not None:
            return existing
        post_source = source_id("reddit", "post", reddit_post_id)
        post_url = f"https://www.reddit.com/comments/{reddit_post_id}/"
        _upsert_source(
            connection,
            source=post_source,
            platform="reddit",
            source_kind="post",
            native_id=reddit_post_id,
            canonical_url=post_url,
            title="Archive URL discovery source",
            observed_at=observed_at,
            access_state="public",
            review_state="unreviewed",
            batch_id=batch_id,
            metadata={"discovery_state": "coverage_hints_only"},
        )
        statistics = Counter()
        seen: set[str] = set()
        for raw_url in urls:
            try:
                parsed = urlparse(raw_url)
            except ValueError:
                statistics["invalid_urls"] += 1
                continue
            segments = parsed.path.split("/")
            if parsed.scheme != "https" or parsed.hostname != "archive.org" or len(segments) < 4 or segments[1] != "download":
                statistics["invalid_urls"] += 1
                continue
            item = unquote(segments[2])
            filename = unquote("/".join(segments[3:]))
            native_id = f"{item}/{filename}"
            if native_id in seen:
                statistics["duplicate_urls"] += 1
                continue
            seen.add(native_id)
            target = source_id("internet_archive", "archive_media_file", native_id)
            target_exists = connection.execute(
                "SELECT 1 FROM sources WHERE source_id = ?", (target,)
            ).fetchone()
            if target_exists:
                statistics["resolved_existing_sources"] += 1
            else:
                target = source_id("internet_archive", "archive_url_discovery_hint", native_id)
                _upsert_source(
                    connection,
                    source=target,
                    platform="internet_archive",
                    source_kind="archive_url_discovery_hint",
                    native_id=native_id,
                    canonical_url=raw_url,
                    title=title_from_media_filename(filename),
                    observed_at=observed_at,
                    access_state="unknown",
                    review_state="unreviewed",
                    batch_id=batch_id,
                    metadata={"discovery_state": "url_hint_only", "reddit_post_id": reddit_post_id},
                )
                statistics["new_hint_sources"] += 1
            _relate_sources(
                connection,
                from_source=post_source,
                relation_kind="references",
                to_source=target,
                basis="URL present in contributor-linked archive complement list",
                batch_id=batch_id,
                observed_at=observed_at,
                metadata={"coverage_hint_only": True},
            )
            _add_external_id(
                connection,
                object_type="source",
                object_id=target,
                namespace="reddit_archive_url_hint",
                value=raw_url,
                basis=f"Reddit post {reddit_post_id} URL list",
                batch_id=batch_id,
                observed_at=observed_at,
                source=post_source,
            )
            statistics["valid_hints"] += 1
        result = dict(sorted(statistics.items()))
        _complete_batch(connection, batch_id, observed_at, result)
        return result


class _BencodeParser:
    def __init__(self, data: bytes):
        self.data = data

    def parse(self, index: int = 0):
        if index >= len(self.data):
            raise ValueError("Unexpected end of bencode data")
        marker = self.data[index : index + 1]
        if marker == b"i":
            end = self.data.index(b"e", index + 1)
            return int(self.data[index + 1 : end]), end + 1
        if marker == b"l":
            values = []
            index += 1
            while self.data[index : index + 1] != b"e":
                value, index = self.parse(index)
                values.append(value)
            return values, index + 1
        if marker == b"d":
            values = {}
            index += 1
            while self.data[index : index + 1] != b"e":
                key, index = self.parse(index)
                if not isinstance(key, bytes):
                    raise ValueError("Bencode dictionary key is not bytes")
                value, index = self.parse(index)
                values[key] = value
            return values, index + 1
        if marker.isdigit():
            colon = self.data.index(b":", index)
            length = int(self.data[index:colon])
            start = colon + 1
            end = start + length
            if end > len(self.data):
                raise ValueError("Bencode byte string exceeds input")
            return self.data[start:end], end
        raise ValueError(f"Unsupported bencode marker at byte {index}")

    def root_with_info_span(self):
        if self.data[:1] != b"d":
            raise ValueError("Torrent root must be a bencoded dictionary")
        result = {}
        info_span = None
        index = 1
        while self.data[index : index + 1] != b"e":
            key, index = self.parse(index)
            if not isinstance(key, bytes):
                raise ValueError("Torrent root key is not bytes")
            value_start = index
            value, index = self.parse(index)
            result[key] = value
            if key == b"info":
                info_span = (value_start, index)
        if index + 1 != len(self.data):
            raise ValueError("Trailing bytes after torrent dictionary")
        if info_span is None:
            raise ValueError("Torrent has no info dictionary")
        return result, info_span


def _decode_torrent_text(value: object) -> str:
    if not isinstance(value, bytes):
        return str(value)
    return value.decode("utf-8", errors="replace")


def import_torrent_manifest(
    connection: sqlite3.Connection,
    torrent_path: Path,
    *,
    observed_at: str,
    discovery_metadata_path: Path | None = None,
) -> dict:
    """Import only a torrent's manifest tree; never acquire its payload."""

    torrent_path = Path(torrent_path)
    paths = [torrent_path]
    if discovery_metadata_path:
        paths.append(Path(discovery_metadata_path))
    digest = combined_digest(paths)
    raw = torrent_path.read_bytes()
    root, info_span = _BencodeParser(raw).root_with_info_span()
    info = root.get(b"info")
    if not isinstance(info, dict):
        raise ValueError("Torrent info value is not a dictionary")
    info_hash = hashlib.sha1(raw[info_span[0] : info_span[1]]).hexdigest()
    root_name = _decode_torrent_text(info.get(b"name.utf-8") or info.get(b"name") or b"")
    files_value = info.get(b"files")
    files: list[tuple[str, int]] = []
    if isinstance(files_value, list):
        for file_record in files_value:
            if not isinstance(file_record, dict):
                raise ValueError("Torrent file entry is not a dictionary")
            path_parts = file_record.get(b"path.utf-8") or file_record.get(b"path")
            if not isinstance(path_parts, list):
                raise ValueError("Torrent file entry has no path list")
            relative_path = "/".join(_decode_torrent_text(part) for part in path_parts)
            length = file_record.get(b"length")
            if not isinstance(length, int) or length < 0:
                raise ValueError("Torrent file length is invalid")
            files.append((relative_path, length))
    else:
        length = info.get(b"length")
        if not isinstance(length, int) or length < 0:
            raise ValueError("Single-file torrent length is invalid")
        files.append((root_name, length))

    discovery = {}
    if discovery_metadata_path:
        discovery = load_json(Path(discovery_metadata_path))
        if not isinstance(discovery, dict):
            discovery = {}
    expected = discovery.get("torrent") or {}
    if expected.get("info_hash_sha1") and str(expected["info_hash_sha1"]).lower() != info_hash:
        raise ValueError("Torrent info hash differs from reviewed discovery metadata")
    if expected.get("file_count") is not None and int(expected["file_count"]) != len(files):
        raise ValueError("Torrent file count differs from reviewed discovery metadata")
    if expected.get("total_bytes") is not None and int(expected["total_bytes"]) != sum(
        length for _, length in files
    ):
        raise ValueError("Torrent byte total differs from reviewed discovery metadata")

    with transaction(connection):
        batch_id, existing = _begin_batch(
            connection, "torrent_manifest_metadata", digest, observed_at[:10], observed_at
        )
        if existing is not None:
            return existing
        manifest_source = source_id("bittorrent", "torrent_manifest", info_hash)
        torrent_url = expected.get("url") if isinstance(expected.get("url"), str) else None
        manifest_metadata = {
            "info_hash_sha1": info_hash,
            "torrent_sha256": sha256_bytes(raw),
            "root_name": root_name,
            "piece_length_bytes": info.get(b"piece length"),
            "file_count": len(files),
            "total_bytes": sum(length for _, length in files),
            "payload_downloaded": False,
        }
        _upsert_source(
            connection,
            source=manifest_source,
            platform="bittorrent",
            source_kind="torrent_manifest",
            native_id=info_hash,
            canonical_url=torrent_url,
            title=root_name or "Torrent manifest",
            observed_at=observed_at,
            access_state="public" if torrent_url else "unknown",
            review_state="unreviewed",
            batch_id=batch_id,
            metadata=manifest_metadata,
        )
        _add_source_snapshot(
            connection,
            source=manifest_source,
            observed_at=observed_at,
            payload=manifest_metadata,
            batch_id=batch_id,
            request_url=torrent_url,
            artifact_path=str(torrent_path),
        )
        _add_external_id(
            connection,
            object_type="source",
            object_id=manifest_source,
            namespace="bittorrent_info_hash_sha1",
            value=info_hash,
            basis="SHA-1 of exact bencoded info dictionary",
            batch_id=batch_id,
            observed_at=observed_at,
            source=manifest_source,
        )
        for relative_path, length in files:
            native_id = f"{info_hash}/{relative_path}"
            candidate_source = source_id("bittorrent", "torrent_file_candidate", native_id)
            _upsert_source(
                connection,
                source=candidate_source,
                platform="bittorrent",
                source_kind="torrent_file_candidate",
                native_id=native_id,
                parent_source=manifest_source,
                title=relative_path.rsplit("/", 1)[-1] or relative_path,
                observed_at=observed_at,
                access_state="unknown",
                review_state="unreviewed",
                batch_id=batch_id,
                metadata={
                    "manifest_path": relative_path,
                    "byte_count": length,
                    "discovery_state": "private_manifest_candidate",
                    "payload_downloaded": False,
                },
            )
        task_id = stable_id("rtk", "torrent_manifest_assessment", manifest_source)
        connection.execute(
            """
            INSERT OR IGNORE INTO review_tasks(
                review_task_id, task_kind, target_type, target_id, reason,
                priority, status, created_at, updated_at
            ) VALUES(?, 'torrent_manifest_assessment', 'source', ?, ?, 70, 'open', ?, ?)
            """,
            (
                task_id,
                manifest_source,
                "Review manifest candidates and rights before any payload acquisition or public exposure",
                observed_at,
                observed_at,
            ),
        )
        result = {
            "info_hash_sha1": info_hash,
            "torrent_sha256": sha256_bytes(raw),
            "file_count": len(files),
            "total_bytes": sum(length for _, length in files),
            "payload_files_downloaded": 0,
        }
        _complete_batch(connection, batch_id, observed_at, result)
        return result


def approve_public_source_metadata(connection: sqlite3.Connection) -> dict:
    """Allowlist metadata from the reviewed IA items and official-channel snapshot.

    Existence/availability validation alone is deliberately insufficient: arbitrary
    IA items and third-party YouTube search candidates require a human relevance and
    publication decision.
    """

    reviewer_id = PUBLIC_METADATA_REVIEWER_ID
    statistics = Counter()
    with transaction(connection):
        ensure_public_metadata_policy_reviewer(connection)
        decided_at = utc_now()

        def ensure_policy_decision(
            *,
            decision_id: str,
            object_type: str,
            object_id: str,
            basis: str,
            public_label: str,
        ) -> bool:
            existing = connection.execute(
                """
                SELECT object_type, object_id, decision, reviewer_id, basis,
                       public_label
                FROM publication_decisions
                WHERE publication_decision_id = ?
                """,
                (decision_id,),
            ).fetchone()
            expected = (
                object_type,
                object_id,
                "publish",
                reviewer_id,
                basis,
                public_label,
            )
            if existing is not None:
                if tuple(existing) != expected:
                    raise ValueError(
                        f"public metadata policy decision {decision_id!r} conflicts "
                        "with an existing append-only row"
                    )
                return False
            connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis, public_label
                ) VALUES(?, ?, ?, 'publish', ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    object_type,
                    object_id,
                    reviewer_id,
                    decided_at,
                    basis,
                    public_label,
                ),
            )
            return True

        safe_objects = connection.execute(
            """
            SELECT object_type, object_id, basis, public_label
            FROM public_metadata_policy_publish_scope
            ORDER BY object_type, object_id
            """
        ).fetchall()
        for row in safe_objects:
            decision_id = stable_id(
                "pub", row["object_type"], row["object_id"], PUBLIC_METADATA_POLICY
            )
            if ensure_policy_decision(
                decision_id=decision_id,
                object_type=row["object_type"],
                object_id=row["object_id"],
                basis=row["basis"],
                public_label=row["public_label"],
            ):
                statistics[f"{row['object_type']}_decisions_added"] += 1
    statistics["public_sources"] = connection.execute(
        "SELECT count(*) FROM public_sources"
    ).fetchone()[0]
    statistics["public_recordings"] = connection.execute(
        "SELECT count(*) FROM public_recordings"
    ).fetchone()[0]
    return dict(sorted(statistics.items()))


def import_snapshot_bundle(
    connection: sqlite3.Connection,
    archive_dir: Path,
    channel_dir: Path,
    *,
    approve_public_metadata: bool = False,
) -> dict:
    archive_dir = Path(archive_dir)
    channel_dir = Path(channel_dir)
    archive_provenance = load_json(archive_dir / "provenance.json")
    archive_date = str((archive_provenance or {}).get("snapshot_date") or archive_dir.name)
    archive_observed = snapshot_timestamp(
        archive_date, (archive_provenance or {}).get("fetched_at")
    )
    channel_provenance_path = channel_dir / "provenance.json"
    channel_provenance = (
        load_json(channel_provenance_path) if channel_provenance_path.exists() else {}
    )
    channel_date = str((channel_provenance or {}).get("snapshot_date") or channel_dir.name)
    channel_observed = snapshot_timestamp(
        channel_date, (channel_provenance or {}).get("created_at")
    )
    raw_dir = archive_dir / "raw"
    results = {
        "internet_archive": import_internet_archive(
            connection,
            [
                raw_dir / "internet-archive-69999.json",
                raw_dir / "internet-archive-699992.json",
            ],
            snapshot_date=archive_date,
            observed_at=archive_observed,
        ),
        "legacy_manifest": import_legacy_manifest(
            connection,
            archive_dir / "source-manifest.jsonl",
            snapshot_date=archive_date,
            observed_at=archive_observed,
        ),
        "current_channel": import_current_channel(
            connection,
            channel_dir / "inventory.json",
            snapshot_date=channel_date,
            observed_at=channel_observed,
        ),
    }
    if approve_public_metadata:
        results["publication"] = approve_public_source_metadata(connection)
    return results
