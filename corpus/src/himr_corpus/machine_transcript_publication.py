"""Closed default-publication policy for ordinary-coordinate machine transcripts.

The policy consumes only catalog metadata and explicit human gate decisions. It never
reads transcript wording, clears a gate, creates a review/correction, or changes the
human-only dispute/retraction/reinstatement lifecycle.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from typing import Any

from .db import transaction, utc_now
from .reviewer_admin import (
    MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
    MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
    _machine_transcript_policy_provenance_is_exact,
    ensure_machine_transcript_policy_reviewer,
)


PLAN_SCHEMA_VERSION = 1
POLICY_ID = "machine_transcript_default_publication_v1"
POLICY_BASIS = (
    "Closed automated policy v1: the ordinary recording-coordinate machine "
    "transcript has current human clear decisions for each independent rights, "
    "privacy, and sensitivity gate; no wording review was performed."
)
PUBLIC_LABEL = "machine transcript (unreviewed)"
DISCLAIMER_CODE = "machine_generated_unreviewed_not_verified_quotation_v1"
DISCLAIMER_TEXT = (
    "Machine-generated and unreviewed; may be wrong; not a verified quotation."
)
DECISION_ID_PREFIX = "machine-transcript-default-v1:"
MANIFEST_ID_PREFIX = "machine-transcript-default-policy-v1:"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class MachineTranscriptPublicationPolicyError(ValueError):
    """The requested policy run is stale, conflicting, or outside its capability."""


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _plan_payload(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT revision_id, recording_id, revision_kind, language, created_at,
               segment_count,
               rights_gate_decision_id, rights_reviewer_id, rights_decided_at,
               privacy_gate_decision_id, privacy_reviewer_id, privacy_decided_at,
               sensitivity_gate_decision_id, sensitivity_reviewer_id,
               sensitivity_decided_at,
               public_label, basis, disclaimer_code, disclaimer_text
        FROM machine_transcript_policy_publish_scope
        ORDER BY revision_id
        """
    ).fetchall()
    revisions: list[dict[str, Any]] = []
    for row in rows:
        if (
            row["public_label"] != PUBLIC_LABEL
            or row["basis"] != POLICY_BASIS
            or row["disclaimer_code"] != DISCLAIMER_CODE
            or row["disclaimer_text"] != DISCLAIMER_TEXT
        ):
            raise MachineTranscriptPublicationPolicyError(
                "database policy scope differs from the code-pinned policy literals"
            )
        revisions.append(
            {
                "revision_id": row["revision_id"],
                "recording_id": row["recording_id"],
                "revision_kind": row["revision_kind"],
                "review_state": "machine",
                "language": row["language"],
                "created_at": row["created_at"],
                "segment_count": row["segment_count"],
                "gates": {
                    gate_kind: {
                        "publication_gate_decision_id": row[
                            f"{gate_kind}_gate_decision_id"
                        ],
                        "reviewer_id": row[f"{gate_kind}_reviewer_id"],
                        "decided_at": row[f"{gate_kind}_decided_at"],
                    }
                    for gate_kind in ("rights", "privacy", "sensitivity")
                },
            }
        )
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "policy_id": POLICY_ID,
        "reviewer_id": MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
        "public_label": PUBLIC_LABEL,
        "basis": POLICY_BASIS,
        "disclaimer_code": DISCLAIMER_CODE,
        "disclaimer": DISCLAIMER_TEXT,
        "eligible_revision_count": len(revisions),
        "revisions": revisions,
    }


def build_machine_transcript_publication_plan(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Return the complete text-free policy scope and its deterministic digest."""

    payload = _plan_payload(connection)
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return {**payload, "plan_sha256": digest}


def _decision_id(revision_id: str) -> str:
    return f"{DECISION_ID_PREFIX}{revision_id}"


def _manifest_id(plan_sha256: str) -> str:
    return f"{MANIFEST_ID_PREFIX}{plan_sha256}"


def _stored_plan(
    run: sqlite3.Row,
    *,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    try:
        payload = json.loads(run["plan_json"])
    except (TypeError, json.JSONDecodeError) as error:
        raise MachineTranscriptPublicationPolicyError(
            "existing machine transcript publication plan JSON is invalid"
        ) from error
    canonical = _canonical_json(payload)
    if (
        canonical != run["plan_json"]
        or hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        != expected_plan_sha256
        or not isinstance(payload, dict)
        or payload.get("eligible_revision_count")
        != run["eligible_revision_count"]
    ):
        raise MachineTranscriptPublicationPolicyError(
            "existing machine transcript publication run conflicts with its digest"
        )
    return {**payload, "plan_sha256": expected_plan_sha256}


def _matching_policy_decision(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> bool:
    return (
        row["publication_decision_id"] == _decision_id(row["object_id"])
        and row["object_type"] == "transcript_revision"
        and row["decision"] == "publish"
        and row["reviewer_id"] == MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID
        and row["review_decision_id"] is None
        and row["basis"] == POLICY_BASIS
        and row["notes"] == DISCLAIMER_TEXT
        and row["public_label"] == PUBLIC_LABEL
        and row["manifest_id"] is not None
        and connection.execute(
            """
            SELECT 1
            FROM machine_transcript_publication_policy_runs
            WHERE manifest_id = ?
            """,
            (row["manifest_id"],),
        ).fetchone()
        is not None
    )


def _partition_revisions(
    connection: sqlite3.Connection,
    revisions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    new: list[dict[str, Any]] = []
    existing_policy = 0
    protected = 0
    for revision in revisions:
        rows = connection.execute(
            """
            SELECT publication_decision_id, object_type, object_id, decision,
                   reviewer_id, review_decision_id, basis, notes, public_label,
                   manifest_id
            FROM publication_decisions
            WHERE object_type = 'transcript_revision' AND object_id = ?
            ORDER BY decision_sequence
            """,
            (revision["revision_id"],),
        ).fetchall()
        deterministic_id = _decision_id(revision["revision_id"])
        conflicting_reserved = [
            row
            for row in rows
            if row["publication_decision_id"] == deterministic_id
            and not _matching_policy_decision(connection, row)
        ]
        if conflicting_reserved:
            raise MachineTranscriptPublicationPolicyError(
                f"reserved policy decision ID conflicts for {revision['revision_id']!r}"
            )
        if not rows:
            new.append(revision)
        elif len(rows) == 1 and _matching_policy_decision(connection, rows[0]):
            existing_policy += 1
        else:
            # Any human or restrictive history is authoritative. The default policy
            # never appends a later row that could override it.
            protected += 1
    return new, existing_policy, protected


def _result(
    *,
    plan: dict[str, Any],
    reviewer_created: bool,
    decisions_inserted: int,
    existing_policy: int,
    protected: int,
    manifest_id: str | None,
    already_applied: bool,
) -> dict[str, Any]:
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "policy_id": POLICY_ID,
        "plan_sha256": plan["plan_sha256"],
        "eligible_revision_count": plan["eligible_revision_count"],
        "decisions_inserted": decisions_inserted,
        "existing_policy_decisions": existing_policy,
        "protected_existing_streams": protected,
        "reviewer_created": reviewer_created,
        "manifest_id": manifest_id,
        "gate_decisions_inserted": 0,
        "wording_reviews_inserted": 0,
        "lifecycle_decisions_inserted": 0,
        "already_applied": already_applied,
        "disclaimer_code": DISCLAIMER_CODE,
        "disclaimer": DISCLAIMER_TEXT,
    }


def apply_machine_transcript_publication_plan(
    connection: sqlite3.Connection,
    *,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    """Digest-authorize and atomically apply the closed initial-publish policy."""

    if not SHA256_RE.fullmatch(expected_plan_sha256):
        raise MachineTranscriptPublicationPolicyError(
            "expected plan SHA-256 must be 64 lowercase hexadecimal characters"
        )
    try:
        with transaction(connection):
            existing_run = connection.execute(
                """
                SELECT manifest_id, plan_json, eligible_revision_count,
                       new_decision_count, existing_policy_count,
                       protected_existing_count
                FROM machine_transcript_publication_policy_runs
                WHERE plan_sha256 = ?
                """,
                (expected_plan_sha256,),
            ).fetchone()
            if existing_run is not None:
                plan = _stored_plan(
                    existing_run,
                    expected_plan_sha256=expected_plan_sha256,
                )
                validate_machine_transcript_publication_policy(connection)
                return _result(
                    plan=plan,
                    reviewer_created=False,
                    decisions_inserted=0,
                    existing_policy=existing_run["existing_policy_count"]
                    + existing_run["new_decision_count"],
                    protected=existing_run["protected_existing_count"],
                    manifest_id=existing_run["manifest_id"],
                    already_applied=True,
                )

            plan = build_machine_transcript_publication_plan(connection)
            if plan["plan_sha256"] != expected_plan_sha256:
                raise MachineTranscriptPublicationPolicyError(
                    "machine transcript publication plan changed; review the new digest"
                )
            plan_json = _canonical_json(
                {key: value for key, value in plan.items() if key != "plan_sha256"}
            )

            revisions = plan["revisions"]
            new, existing_policy, protected = _partition_revisions(
                connection, revisions
            )
            if existing_policy or protected:
                raise MachineTranscriptPublicationPolicyError(
                    "initial-publication scope acquired an existing decision stream"
                )
            if not new:
                return _result(
                    plan=plan,
                    reviewer_created=False,
                    decisions_inserted=0,
                    existing_policy=existing_policy,
                    protected=protected,
                    manifest_id=None,
                    already_applied=False,
                )

            reviewer_created = ensure_machine_transcript_policy_reviewer(connection)
            decided_at = utc_now()
            manifest_id = _manifest_id(expected_plan_sha256)
            connection.execute(
                """
                INSERT INTO machine_transcript_publication_policy_runs(
                    plan_sha256, policy_id, manifest_id, reviewer_id, plan_json,
                    eligible_revision_count, new_decision_count,
                    existing_policy_count, protected_existing_count, applied_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    expected_plan_sha256,
                    POLICY_ID,
                    manifest_id,
                    MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                    plan_json,
                    plan["eligible_revision_count"],
                    len(new),
                    existing_policy,
                    protected,
                    decided_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO publication_manifest_imports(
                    manifest_id, input_sha256, schema_version,
                    publication_decision_count, gate_decision_count, imported_at
                ) VALUES(?, ?, 1, ?, 0, ?)
                """,
                (manifest_id, expected_plan_sha256, len(new), decided_at),
            )
            connection.executemany(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, review_decision_id, decided_at, basis, notes,
                    public_label, manifest_id
                ) VALUES(?, 'transcript_revision', ?, 'publish', ?, NULL, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        _decision_id(revision["revision_id"]),
                        revision["revision_id"],
                        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                        decided_at,
                        POLICY_BASIS,
                        DISCLAIMER_TEXT,
                        PUBLIC_LABEL,
                        manifest_id,
                    )
                    for revision in new
                ],
            )
            return _result(
                plan=plan,
                reviewer_created=reviewer_created,
                decisions_inserted=len(new),
                existing_policy=existing_policy,
                protected=protected,
                manifest_id=manifest_id,
                already_applied=False,
            )
    except sqlite3.DatabaseError as error:
        raise MachineTranscriptPublicationPolicyError(
            f"database rejected machine transcript publication policy atomically: {error}"
        ) from error


def validate_machine_transcript_publication_policy(
    connection: sqlite3.Connection,
) -> None:
    """Validate policy receipts, exact capability, referenced gates, and warning."""

    # Historical-schema migration tests deliberately validate a catalog whose
    # on-disk migration set ends at 0027. With the real current migration set,
    # verify_migrations has already made absence of this table impossible.
    if connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table'
          AND name = 'machine_transcript_publication_policy_runs'
        """
    ).fetchone() is None:
        return

    forbidden = connection.execute(
        """
        SELECT
            (SELECT count(*) FROM publication_gate_decisions
             WHERE reviewer_id = ?)
          + (SELECT count(*) FROM review_decisions WHERE reviewer_id = ?)
          + (SELECT count(*) FROM corrections WHERE reviewer_id = ?)
          + (SELECT count(*) FROM transcript_lifecycle_decisions
             WHERE reviewer_id = ?)
        """,
        (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,) * 4,
    ).fetchone()[0]
    if forbidden:
        raise RuntimeError(
            "Machine transcript publication policy escaped into gate, review, "
            "correction, or lifecycle authority"
        )

    reviewer = connection.execute(
        """
        SELECT display_label, reviewer_kind
        FROM reviewers WHERE reviewer_id = ?
        """,
        (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
    ).fetchone()
    if reviewer is not None and (
        tuple(reviewer)
        != (
            MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
            "automated_policy",
        )
        or not _machine_transcript_policy_provenance_is_exact(connection)
    ):
        raise RuntimeError(
            "Machine transcript policy reviewer identity or provenance is invalid"
        )

    reserved_namespace_squatting = connection.execute(
        """
        SELECT
            (SELECT count(*)
             FROM publication_manifest_imports AS manifest
             LEFT JOIN machine_transcript_publication_policy_runs AS run
               ON run.manifest_id = manifest.manifest_id
             WHERE manifest.manifest_id GLOB 'machine-transcript-default-policy-v1:*'
               AND run.manifest_id IS NULL)
          + (SELECT count(*) FROM publication_decisions
             WHERE publication_decision_id GLOB 'machine-transcript-default-v1:*'
               AND reviewer_id <> ?)
          + (SELECT count(*) FROM publication_gate_decisions
             WHERE publication_gate_decision_id GLOB 'machine-transcript-default-v1:*')
        """,
        (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
    ).fetchone()[0]
    if reserved_namespace_squatting:
        raise RuntimeError("Machine transcript policy namespace was used outside policy")

    runs: dict[str, tuple[sqlite3.Row, dict[str, Any]]] = {}
    for run in connection.execute(
        """
        SELECT run.*, manifest.input_sha256, manifest.schema_version,
               manifest.publication_decision_count, manifest.gate_decision_count,
               manifest.imported_at AS manifest_imported_at
        FROM machine_transcript_publication_policy_runs AS run
        LEFT JOIN publication_manifest_imports AS manifest
          ON manifest.manifest_id = run.manifest_id
        ORDER BY run.plan_sha256
        """
    ):
        try:
            payload = json.loads(run["plan_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise RuntimeError("Machine transcript policy plan JSON is invalid") from error
        if _canonical_json(payload) != run["plan_json"]:
            raise RuntimeError("Machine transcript policy plan JSON is not canonical")
        digest = hashlib.sha256(run["plan_json"].encode("utf-8")).hexdigest()
        expected_payload_keys = {
            "schema_version",
            "policy_id",
            "reviewer_id",
            "public_label",
            "basis",
            "disclaimer_code",
            "disclaimer",
            "eligible_revision_count",
            "revisions",
        }
        revisions = payload.get("revisions") if isinstance(payload, dict) else None
        if (
            digest != run["plan_sha256"]
            or run["policy_id"] != POLICY_ID
            or run["reviewer_id"] != MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID
            or run["input_sha256"] != run["plan_sha256"]
            or run["schema_version"] != 1
            or run["publication_decision_count"] != run["new_decision_count"]
            or run["gate_decision_count"] != 0
            or run["manifest_imported_at"] != run["applied_at"]
            or connection.execute(
                "SELECT julianday(?) > julianday('now')", (run["applied_at"],)
            ).fetchone()[0]
            or not isinstance(revisions, list)
            or set(payload) != expected_payload_keys
            or len(revisions) != run["eligible_revision_count"]
            or run["eligible_revision_count"] != run["new_decision_count"]
            or run["existing_policy_count"] != 0
            or run["protected_existing_count"] != 0
            or payload.get("schema_version") != PLAN_SCHEMA_VERSION
            or payload.get("policy_id") != POLICY_ID
            or payload.get("reviewer_id")
            != MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID
            or payload.get("public_label") != PUBLIC_LABEL
            or payload.get("basis") != POLICY_BASIS
            or payload.get("disclaimer_code") != DISCLAIMER_CODE
            or payload.get("disclaimer") != DISCLAIMER_TEXT
            or payload.get("eligible_revision_count") != len(revisions)
        ):
            raise RuntimeError("Machine transcript policy run or manifest is inconsistent")
        revision_ids = [item.get("revision_id") for item in revisions if isinstance(item, dict)]
        if len(revision_ids) != len(revisions) or len(set(revision_ids)) != len(revisions):
            raise RuntimeError("Machine transcript policy plan has invalid revision IDs")
        runs[run["manifest_id"]] = (run, payload)

    decisions = connection.execute(
        """
        SELECT * FROM publication_decisions
        WHERE reviewer_id = ?
        ORDER BY decision_sequence
        """,
        (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
    ).fetchall()
    per_manifest: dict[str, int] = {}
    for decision in decisions:
        entry = runs.get(decision["manifest_id"])
        if entry is None or not _matching_policy_decision(connection, decision):
            raise RuntimeError("Machine transcript policy decision escaped its exact run")
        run, payload = entry
        item = next(
            (
                candidate
                for candidate in payload["revisions"]
                if candidate["revision_id"] == decision["object_id"]
            ),
            None,
        )
        if item is None:
            raise RuntimeError("Machine transcript policy decision is absent from its plan")
        revision = connection.execute(
            """
            SELECT recording_id, revision_kind, review_state, language, created_at,
                   (SELECT count(*) FROM transcript_segments AS segment
                    WHERE segment.revision_id = transcript_revisions.revision_id)
                       AS segment_count,
                   (SELECT count(*) FROM transcript_segments AS segment
                    WHERE segment.revision_id = transcript_revisions.revision_id
                      AND segment.speaker_label IS NOT NULL)
                       AS named_segment_count
            FROM transcript_revisions WHERE revision_id = ?
            """,
            (decision["object_id"],),
        ).fetchone()
        if (
            revision is None
            or revision["revision_kind"] not in {"raw_asr", "contextual_asr"}
            or revision["review_state"] != "machine"
            or revision["segment_count"] <= 0
            or revision["named_segment_count"] != 0
            or set(item) != {
                "revision_id",
                "recording_id",
                "revision_kind",
                "review_state",
                "language",
                "created_at",
                "segment_count",
                "gates",
            }
            or item["recording_id"] != revision["recording_id"]
            or item["revision_kind"] != revision["revision_kind"]
            or item["review_state"] != revision["review_state"]
            or item["language"] != revision["language"]
            or item["created_at"] != revision["created_at"]
            or item["segment_count"] != revision["segment_count"]
            or not isinstance(item["gates"], dict)
            or set(item["gates"]) != {"rights", "privacy", "sensitivity"}
            or decision["decided_at"] != run["applied_at"]
            or connection.execute(
                "SELECT julianday(?) > julianday(?)",
                (revision["created_at"], decision["decided_at"]),
            ).fetchone()[0]
            or connection.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM publication_decisions AS earlier
                    WHERE earlier.object_type = 'transcript_revision'
                      AND earlier.object_id = ?
                      AND earlier.decision_sequence < ?
                )
                """,
                (decision["object_id"], decision["decision_sequence"]),
            ).fetchone()[0]
            or connection.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM transcript_lifecycle_decisions AS lifecycle
                    WHERE lifecycle.revision_id = ?
                      AND julianday(lifecycle.decided_at) <= julianday(?)
                )
                """,
                (decision["object_id"], decision["decided_at"]),
            ).fetchone()[0]
            or not connection.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM reviewer_admin_events AS event
                    WHERE event.reviewer_id = ?
                      AND julianday(event.effective_at) <= julianday(?)
                      AND event.new_active = 1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM reviewer_admin_events AS later
                          WHERE later.reviewer_id = event.reviewer_id
                            AND julianday(later.effective_at) <= julianday(?)
                            AND later.event_sequence > event.event_sequence
                      )
                )
                """,
                (
                    MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                    decision["decided_at"],
                    decision["decided_at"],
                ),
            ).fetchone()[0]
        ):
            raise RuntimeError("Machine transcript policy decision targets an invalid revision")
        for gate_kind in ("rights", "privacy", "sensitivity"):
            gate_plan = item.get("gates", {}).get(gate_kind, {})
            gate = connection.execute(
                """
                SELECT gate.object_type, gate.object_id, gate.gate_kind,
                       gate.decision, gate.reviewer_id, gate.decided_at,
                       reviewer.reviewer_kind
                FROM publication_gate_decisions AS gate
                LEFT JOIN reviewers AS reviewer
                  ON reviewer.reviewer_id = gate.reviewer_id
                WHERE gate.publication_gate_decision_id = ?
                """,
                (gate_plan.get("publication_gate_decision_id"),),
            ).fetchone()
            if (
                gate is None
                or gate["object_type"] != "transcript_revision"
                or gate["object_id"] != decision["object_id"]
                or gate["gate_kind"] != gate_kind
                or gate["decision"] != "clear"
                or gate["reviewer_kind"] != "human"
                or not isinstance(gate_plan, dict)
                or set(gate_plan) != {
                    "publication_gate_decision_id",
                    "reviewer_id",
                    "decided_at",
                }
                or gate["reviewer_id"] != gate_plan.get("reviewer_id")
                or gate["decided_at"] != gate_plan.get("decided_at")
                or connection.execute(
                    "SELECT julianday(?) > julianday(?)",
                    (gate["decided_at"], decision["decided_at"]),
                ).fetchone()[0]
                or connection.execute(
                    "SELECT julianday(?) < julianday(?)",
                    (gate["decided_at"], revision["created_at"]),
                ).fetchone()[0]
                or not connection.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM reviewer_admin_events AS event
                        WHERE event.reviewer_id = ?
                          AND julianday(event.effective_at) <= julianday(?)
                          AND event.new_active = 1
                          AND NOT EXISTS (
                              SELECT 1
                              FROM reviewer_admin_events AS later
                              WHERE later.reviewer_id = event.reviewer_id
                                AND julianday(later.effective_at) <= julianday(?)
                                AND later.event_sequence > event.event_sequence
                          )
                    )
                    """,
                    (gate["reviewer_id"], gate["decided_at"], gate["decided_at"]),
                ).fetchone()[0]
                or connection.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM publication_gate_decisions AS later
                        WHERE later.object_type = 'transcript_revision'
                          AND later.object_id = ?
                          AND later.gate_kind = ?
                          AND later.gate_decision_sequence > (
                              SELECT gate_decision_sequence
                              FROM publication_gate_decisions
                              WHERE publication_gate_decision_id = ?
                          )
                          AND julianday(later.decided_at) <= julianday(?)
                    )
                    """,
                    (
                        decision["object_id"],
                        gate_kind,
                        gate_plan.get("publication_gate_decision_id"),
                        decision["decided_at"],
                    ),
                ).fetchone()[0]
            ):
                raise RuntimeError(
                    "Machine transcript policy decision lacks its planned human gate"
                )
        per_manifest[decision["manifest_id"]] = (
            per_manifest.get(decision["manifest_id"], 0) + 1
        )

    for manifest_id, (run, _) in runs.items():
        if per_manifest.get(manifest_id, 0) != run["new_decision_count"]:
            raise RuntimeError("Machine transcript policy decision count is inconsistent")

    invalid_public_warning = connection.execute(
        """
        SELECT count(*)
        FROM public_transcript_revisions AS revision
        JOIN current_publication_decisions AS publication
          ON publication.object_type = 'transcript_revision'
         AND publication.object_id = revision.revision_id
        WHERE publication.reviewer_id = ?
          AND (revision.machine_generated <> 1
               OR revision.verified_quotation <> 0
               OR revision.disclaimer_code <> ?)
        """,
        (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID, DISCLAIMER_CODE),
    ).fetchone()[0]
    if invalid_public_warning:
        raise RuntimeError("Machine transcript policy output lacks the exact warning")
