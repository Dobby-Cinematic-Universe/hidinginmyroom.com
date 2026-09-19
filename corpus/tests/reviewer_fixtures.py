"""Governed reviewer setup helpers for disposable corpus test databases."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from himr_corpus.ids import stable_id
from himr_corpus.reviewer_admin import apply_reviewer_admin_manifest


def _apply(connection, manifest: dict[str, object]) -> None:
    with tempfile.TemporaryDirectory(prefix="reviewer-fixture-") as directory:
        path = Path(directory) / "manifest.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        apply_reviewer_admin_manifest(connection, path, dry_run=False)


def register_reviewer_fixture(
    connection,
    reviewer_id: str,
    display_label: str,
    reviewer_kind: str = "human",
    *,
    active: bool = True,
) -> None:
    """Register a disposable reviewer through the production audit boundary."""

    registration_id = stable_id("rfxreg", reviewer_id, display_label, reviewer_kind)
    state_changes: list[dict[str, object]] = []
    if active:
        state_changes.append(
            {
                "reviewer_state_change_id": stable_id("rfxstate", reviewer_id, "active"),
                "reviewer_id": reviewer_id,
                "active": True,
                "changed_at": "1971-01-01T00:00:01Z",
                "basis": "Activate the governed disposable test reviewer.",
            }
        )
    _apply(
        connection,
        {
            "schema_version": 1,
            "manifest_id": stable_id("rfxmanifest", reviewer_id, display_label, reviewer_kind),
            "created_at": "1971-01-01T00:00:02Z",
            "authorized_by": "test_fixture_harness",
            "basis": "Create a governed reviewer in a disposable test catalog.",
            "registrations": [
                {
                    "reviewer_registration_id": registration_id,
                    "reviewer_id": reviewer_id,
                    "display_label": display_label,
                    "reviewer_kind": reviewer_kind,
                    "registered_at": "1971-01-01T00:00:00Z",
                    "basis": "Register the governed disposable test reviewer inactive.",
                }
            ],
            "state_changes": state_changes,
        },
    )


def set_reviewer_active_fixture(
    connection,
    reviewer_id: str,
    active: bool,
    *,
    changed_at: str,
    sequence_label: str,
) -> None:
    """Append one explicit state event in a disposable test catalog."""

    _apply(
        connection,
        {
            "schema_version": 1,
            "manifest_id": stable_id(
                "rfxmanifest", reviewer_id, active, changed_at, sequence_label
            ),
            "created_at": changed_at,
            "authorized_by": "test_fixture_harness",
            "basis": "Change governed reviewer state in a disposable test catalog.",
            "registrations": [],
            "state_changes": [
                {
                    "reviewer_state_change_id": stable_id(
                        "rfxstate", reviewer_id, active, changed_at, sequence_label
                    ),
                    "reviewer_id": reviewer_id,
                    "active": active,
                    "changed_at": changed_at,
                    "basis": "Explicit disposable test reviewer state transition.",
                }
            ],
        },
    )
