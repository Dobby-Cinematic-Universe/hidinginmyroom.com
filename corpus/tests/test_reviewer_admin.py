from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser, main  # noqa: E402
import himr_corpus.db as db_module  # noqa: E402
from himr_corpus.db import connect, migrate, transaction, utc_now  # noqa: E402
from himr_corpus.publication_admin import (  # noqa: E402
    PublicationManifestError,
    apply_publication_manifest,
    validate_publication_manifest,
)
from himr_corpus.reviewer_admin import (  # noqa: E402
    ReviewerAdminManifestError,
    apply_reviewer_admin_manifest,
    ensure_public_metadata_policy_reviewer,
    load_reviewer_admin_manifest,
    validate_reviewer_admin_manifest,
)
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


class ReviewerAdminTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "corpus.sqlite3"
        self.connection = connect(self.database)
        self.addCleanup(self.connection.close)
        migrate(self.connection)
        self.manifest = {
            "schema_version": 1,
            "manifest_id": "reviewer-admin-fixture-001",
            "created_at": "2026-08-27T20:05:00Z",
            "authorized_by": "fixture_maintainer",
            "basis": "Reviewed fixture role assignments.",
            "registrations": [
                {
                    "reviewer_registration_id": "register_alice_001",
                    "reviewer_id": "reviewer_alice",
                    "display_label": "Alice Maintainer",
                    "reviewer_kind": "human",
                    "registered_at": "2026-08-27T20:00:00Z",
                    "basis": "Approved human reviewer fixture.",
                },
                {
                    "reviewer_registration_id": "register_policy_001",
                    "reviewer_id": "reviewer_policy",
                    "display_label": "Policy Bot v1",
                    "reviewer_kind": "automated_policy",
                    "registered_at": "2026-08-27T20:00:01Z",
                    "basis": "Constrained policy reviewer fixture.",
                },
                {
                    "reviewer_registration_id": "register_legacy_001",
                    "reviewer_id": "reviewer_legacy",
                    "display_label": "Legacy Review Import",
                    "reviewer_kind": "imported_legacy",
                    "registered_at": "2026-08-27T20:00:02Z",
                    "basis": "Historical attribution fixture.",
                },
            ],
            "state_changes": [
                {
                    "reviewer_state_change_id": "activate_alice_001",
                    "reviewer_id": "reviewer_alice",
                    "active": True,
                    "changed_at": "2026-08-27T20:01:00Z",
                    "basis": "Explicit human-review activation.",
                }
            ],
        }

    def _write(self, value: dict[str, object], name: str = "manifest.json") -> Path:
        path = self.root / name
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return path

    def test_dry_run_writes_nothing_and_apply_registers_all_kinds(self) -> None:
        path = self._write(self.manifest)
        before = self.connection.total_changes
        validated = validate_reviewer_admin_manifest(self.connection, path)
        self.assertTrue(validated["dry_run"])
        self.assertEqual(validated["reviewers_inserted"], 0)
        self.assertEqual(validated["events_inserted"], 0)
        self.assertEqual(self.connection.total_changes, before)
        for table in (
            "reviewers",
            "reviewer_admin_manifest_imports",
            "reviewer_admin_events",
        ):
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                0,
            )

        imported = apply_reviewer_admin_manifest(
            self.connection, path, dry_run=False
        )
        self.assertFalse(imported["dry_run"])
        self.assertEqual(imported["reviewers_inserted"], 3)
        self.assertEqual(imported["events_inserted"], 4)
        self.assertEqual(imported["publication_decisions_created"], 0)
        self.assertEqual(imported["publication_gate_decisions_created"], 0)
        rows = self.connection.execute(
            """
            SELECT reviewer_id, reviewer_kind, active
            FROM reviewers ORDER BY reviewer_id
            """
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in rows],
            [
                ("reviewer_alice", "human", 1),
                ("reviewer_legacy", "imported_legacy", 0),
                ("reviewer_policy", "automated_policy", 0),
            ],
        )
        event = self.connection.execute(
            """
            SELECT event_kind, previous_active, new_active
            FROM current_reviewer_admin_events
            WHERE reviewer_id = 'reviewer_alice'
            """
        ).fetchone()
        self.assertEqual(tuple(event), ("set_active", 0, 1))
        with self.assertRaisesRegex(ReviewerAdminManifestError, "already imported"):
            apply_reviewer_admin_manifest(self.connection, path, dry_run=False)

    def test_registration_and_activation_are_distinct_and_chronological(self) -> None:
        same_time = copy.deepcopy(self.manifest)
        same_time["state_changes"][0]["changed_at"] = "2026-08-27T20:00:00Z"
        with self.assertRaisesRegex(ReviewerAdminManifestError, "must be later"):
            validate_reviewer_admin_manifest(
                self.connection, self._write(same_time, "same-time.json")
            )

        future = copy.deepcopy(self.manifest)
        future["registrations"][0]["registered_at"] = "2026-08-27T20:06:00Z"
        with self.assertRaisesRegex(ReviewerAdminManifestError, "manifest.created_at"):
            load_reviewer_admin_manifest(self._write(future, "future.json"))

        no_activation = copy.deepcopy(self.manifest)
        no_activation["state_changes"] = []
        apply_reviewer_admin_manifest(
            self.connection, self._write(no_activation, "inactive.json"), dry_run=False
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT sum(active) FROM reviewers"
            ).fetchone()[0],
            0,
        )

    def test_unknown_keys_duplicate_ids_unsafe_labels_and_types_fail_closed(self) -> None:
        unknown = copy.deepcopy(self.manifest)
        unknown["registrations"][0]["extra"] = True
        with self.assertRaisesRegex(ReviewerAdminManifestError, "unknown"):
            load_reviewer_admin_manifest(self._write(unknown, "unknown.json"))

        duplicate = copy.deepcopy(self.manifest)
        duplicate["state_changes"][0]["reviewer_state_change_id"] = (
            "register_alice_001"
        )
        with self.assertRaisesRegex(ReviewerAdminManifestError, "duplicate operation IDs"):
            load_reviewer_admin_manifest(self._write(duplicate, "duplicate.json"))

        duplicate_reviewer = copy.deepcopy(self.manifest)
        duplicate_reviewer["registrations"][1]["reviewer_id"] = "reviewer_alice"
        with self.assertRaisesRegex(
            ReviewerAdminManifestError, "duplicate reviewer registrations"
        ):
            load_reviewer_admin_manifest(
                self._write(duplicate_reviewer, "duplicate-reviewer.json")
            )

        unsafe = copy.deepcopy(self.manifest)
        unsafe["registrations"][0]["display_label"] = "<script>alert(1)</script>"
        with self.assertRaisesRegex(ReviewerAdminManifestError, "without markup"):
            load_reviewer_admin_manifest(self._write(unsafe, "unsafe.json"))

        integer_boolean = copy.deepcopy(self.manifest)
        integer_boolean["state_changes"][0]["active"] = 1
        with self.assertRaisesRegex(ReviewerAdminManifestError, "must be a boolean"):
            load_reviewer_admin_manifest(
                self._write(integer_boolean, "integer-boolean.json")
            )

        duplicate_key = self.root / "duplicate-key.json"
        duplicate_key.write_text(
            '{"schema_version":1,"schema_version":1}', encoding="utf-8"
        )
        with self.assertRaisesRegex(ReviewerAdminManifestError, "duplicate key"):
            load_reviewer_admin_manifest(duplicate_key)

        future = copy.deepcopy(self.manifest)
        future["created_at"] = "2099-01-01T00:00:00Z"
        future["registrations"][0]["registered_at"] = "2098-12-31T23:59:58Z"
        future["state_changes"][0]["changed_at"] = "2098-12-31T23:59:59Z"
        with self.assertRaisesRegex(ReviewerAdminManifestError, "current UTC time"):
            load_reviewer_admin_manifest(self._write(future, "future-manifest.json"))

        too_many_registrations = copy.deepcopy(self.manifest)
        too_many_registrations["registrations"] = [
            copy.deepcopy(self.manifest["registrations"][0]) for _ in range(1_001)
        ]
        too_many_registrations["state_changes"] = []
        with self.assertRaisesRegex(
            ReviewerAdminManifestError, "registrations exceeds.*1000"
        ):
            load_reviewer_admin_manifest(
                self._write(too_many_registrations, "too-many-registrations.json")
            )

        too_many_state_changes = copy.deepcopy(self.manifest)
        too_many_state_changes["registrations"] = []
        too_many_state_changes["state_changes"] = [
            copy.deepcopy(self.manifest["state_changes"][0]) for _ in range(1_001)
        ]
        with self.assertRaisesRegex(
            ReviewerAdminManifestError, "state_changes exceeds.*1000"
        ):
            load_reviewer_admin_manifest(
                self._write(too_many_state_changes, "too-many-state-changes.json")
            )

    def test_existing_unknown_noop_and_operation_conflicts_fail_closed(self) -> None:
        register_reviewer_fixture(
            self.connection, "reviewer_existing", "Existing Reviewer"
        )
        conflict = copy.deepcopy(self.manifest)
        conflict["registrations"][0]["reviewer_id"] = "reviewer_existing"
        with self.assertRaisesRegex(ReviewerAdminManifestError, "conflicts with existing"):
            validate_reviewer_admin_manifest(
                self.connection, self._write(conflict, "existing.json")
            )

        unknown = copy.deepcopy(self.manifest)
        unknown["state_changes"][0]["reviewer_id"] = "reviewer_missing"
        with self.assertRaisesRegex(ReviewerAdminManifestError, "unknown reviewer"):
            validate_reviewer_admin_manifest(
                self.connection, self._write(unknown, "missing.json")
            )

        noop = copy.deepcopy(self.manifest)
        noop["state_changes"] = [
            {
                "reviewer_state_change_id": "keep_existing_active_001",
                "reviewer_id": "reviewer_existing",
                "active": True,
                "changed_at": "2026-08-27T20:01:00Z",
                "basis": "This no-op must be rejected.",
            }
        ]
        with self.assertRaisesRegex(ReviewerAdminManifestError, "no-op"):
            validate_reviewer_admin_manifest(
                self.connection, self._write(noop, "noop.json")
            )

    def test_manifest_is_atomic_and_database_guards_managed_reviewers(self) -> None:
        self.connection.execute(
            """
            CREATE TRIGGER inject_reviewer_admin_failure
            BEFORE INSERT ON reviewer_admin_events
            WHEN NEW.reviewer_admin_event_id = 'register_policy_001'
            BEGIN
                SELECT RAISE(ABORT, 'injected reviewer admin failure');
            END
            """
        )
        with self.assertRaisesRegex(
            ReviewerAdminManifestError, "injected reviewer admin failure"
        ):
            apply_reviewer_admin_manifest(
                self.connection, self._write(self.manifest), dry_run=False
            )
        for table in (
            "reviewers",
            "reviewer_admin_manifest_imports",
            "reviewer_admin_events",
        ):
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                0,
            )
        self.connection.execute("DROP TRIGGER inject_reviewer_admin_failure")

        apply_reviewer_admin_manifest(
            self.connection, self._write(self.manifest), dry_run=False
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "requires a new admin event"):
            self.connection.execute(
                "UPDATE reviewers SET active = 0 WHERE reviewer_id = 'reviewer_alice'"
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "identity is immutable"):
            self.connection.execute(
                """
                UPDATE reviewers SET display_label = 'Alice Changed'
                WHERE reviewer_id = 'reviewer_alice'
                """
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "cannot be deleted"):
            self.connection.execute(
                "DELETE FROM reviewers WHERE reviewer_id = 'reviewer_alice'"
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "DELETE FROM reviewer_admin_events WHERE reviewer_id = 'reviewer_alice'"
            )

    def test_state_change_is_audited_and_deactivation_revokes_publication_use(self) -> None:
        register_reviewer_fixture(
            self.connection, "reviewer_existing", "Existing Reviewer"
        )
        transition = {
            "schema_version": 1,
            "manifest_id": "reviewer-admin-deactivate-001",
            "created_at": "2026-08-27T21:01:00Z",
            "authorized_by": "fixture_maintainer",
            "basis": "Revoke current review authority.",
            "registrations": [],
            "state_changes": [
                {
                    "reviewer_state_change_id": "deactivate_existing_001",
                    "reviewer_id": "reviewer_existing",
                    "active": False,
                    "changed_at": "2026-08-27T21:00:00Z",
                    "basis": "Reviewer access was withdrawn.",
                }
            ],
        }
        apply_reviewer_admin_manifest(
            self.connection, self._write(transition, "deactivate.json"), dry_run=False
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT active FROM reviewers WHERE reviewer_id = 'reviewer_existing'"
            ).fetchone()[0],
            0,
        )
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, created_at, updated_at
            ) VALUES('source_one', 'youtube', 'youtube_video', 'one',
                     'https://www.youtube.com/watch?v=one',
                     '2026-08-27T19:00:00Z', 'public', 'reviewed',
                     '2026-08-27T19:00:00Z', '2026-08-27T19:00:00Z')
            """
        )
        publication = {
            "schema_version": 1,
            "manifest_id": "publication-inactive-reviewer-fixture",
            "publication_decisions": [
                {
                    "publication_decision_id": "publication-source-one-001",
                    "object_type": "source",
                    "object_id": "source_one",
                    "decision": "publish",
                    "reviewer_id": "reviewer_existing",
                    "decided_at": "2026-08-27T21:02:00Z",
                    "basis": "Fixture.",
                    "note": "Must fail because the reviewer is inactive.",
                }
            ],
            "gate_decisions": [],
        }
        with self.assertRaisesRegex(PublicationManifestError, "inactive reviewer"):
            validate_publication_manifest(
                self.connection, self._write(publication, "publication.json")
            )

    def test_cli_defaults_to_immutable_dry_run_and_requires_apply_flag(self) -> None:
        path = self._write(self.manifest)
        parser = build_parser()
        dry_args = parser.parse_args(
            [
                "import-reviewer-admin-manifest",
                "--db",
                str(self.database),
                "--manifest",
                str(path),
            ]
        )
        self.assertFalse(dry_args.apply)
        apply_args = parser.parse_args(
            [
                "import-reviewer-admin-manifest",
                "--db",
                str(self.database),
                "--manifest",
                str(path),
                "--apply",
            ]
        )
        self.assertTrue(apply_args.apply)

        self.connection.close()
        self.addCleanup(lambda: None)
        before_hash = hashlib.sha256(self.database.read_bytes()).hexdigest()
        before_mtime = self.database.stat().st_mtime_ns
        with contextlib.redirect_stdout(io.StringIO()) as output:
            main(
                [
                    "import-reviewer-admin-manifest",
                    "--db",
                    str(self.database),
                    "--manifest",
                    str(path),
                ]
            )
        result = json.loads(output.getvalue())
        self.assertTrue(result["dry_run"])
        self.assertEqual(hashlib.sha256(self.database.read_bytes()).hexdigest(), before_hash)
        self.assertEqual(self.database.stat().st_mtime_ns, before_mtime)
        self.assertFalse(Path(f"{self.database}-wal").exists())
        self.assertFalse(Path(f"{self.database}-shm").exists())

    def test_post_migration_direct_insert_and_every_replace_route_are_blocked(self) -> None:
        apply_reviewer_admin_manifest(
            self.connection, self._write(self.manifest), dry_run=False
        )
        for active in (0, 1):
            with self.assertRaisesRegex(sqlite3.IntegrityError, "prior registration"):
                self.connection.execute(
                    """
                    INSERT INTO reviewers(
                        reviewer_id, display_label, reviewer_kind, active
                    ) VALUES(?, 'Bypass Reviewer', 'human', ?)
                    """,
                    (f"reviewer_bypass_{active}", active),
                )

        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, created_at, updated_at
            ) VALUES('source_replace', 'youtube', 'youtube_video', 'replace',
                     'https://www.youtube.com/watch?v=replace',
                     '2026-08-27T20:00:00Z', 'public', 'reviewed',
                     '2026-08-27T20:00:00Z', '2026-08-27T20:00:00Z')
            """
        )
        publication_path = self._write(
            {
                "schema_version": 1,
                "manifest_id": "publication-replace-fixture",
                "publication_decisions": [
                    {
                        "publication_decision_id": "publication_replace_001",
                        "object_type": "source",
                        "object_id": "source_replace",
                        "decision": "publish",
                        "reviewer_id": "reviewer_alice",
                        "decided_at": "2026-08-27T20:03:00Z",
                        "basis": "Reviewed fixture.",
                        "note": "Original append-only publication row.",
                    }
                ],
                "gate_decisions": [
                    {
                        "publication_gate_decision_id": "gate_replace_001",
                        "object_type": "source",
                        "object_id": "source_replace",
                        "gate_kind": "privacy",
                        "decision": "clear",
                        "reviewer_id": "reviewer_alice",
                        "decided_at": "2026-08-27T20:03:00Z",
                        "basis": "Reviewed privacy fixture.",
                        "note": "Original append-only gate row.",
                    }
                ],
            },
            "publication-replace.json",
        )
        apply_publication_manifest(self.connection, publication_path)
        self.connection.execute("PRAGMA recursive_triggers = OFF")
        self.connection.execute("PRAGMA foreign_keys = OFF")
        try:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO reviewer_admin_manifest_imports(
                        manifest_id, input_sha256, schema_version,
                        manifest_created_at, imported_at, authorized_by, basis,
                        registration_count, state_change_count, adoption_count
                    ) VALUES('reviewer-admin-fixture-001', ?, 1,
                             '2026-08-27T20:05:00Z', '2026-08-27T20:05:00Z',
                             'forged', 'forged', 1, 0, 0)
                    """,
                    ("e" * 64,),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO reviewer_admin_manifest_imports(
                        rowid, manifest_id, input_sha256, schema_version,
                        manifest_created_at, imported_at, authorized_by, basis,
                        registration_count, state_change_count, adoption_count
                    ) VALUES(
                        (SELECT rowid FROM reviewer_admin_manifest_imports
                         WHERE manifest_id = 'reviewer-admin-fixture-001'),
                        'reviewer-admin-rowid-forgery', ?, 1,
                        '2026-08-27T20:05:00Z', '2026-08-27T20:05:00Z',
                        'forged', 'forged', 1, 0, 0
                    )
                    """,
                    ("b" * 64,),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO reviewer_admin_events(
                        reviewer_admin_event_id, manifest_id, ordinal, event_kind,
                        reviewer_id, display_label, reviewer_kind, previous_active,
                        new_active, effective_at, basis
                    ) VALUES('activate_alice_001', 'reviewer-admin-fixture-001', 3,
                             'set_active', 'reviewer_alice', 'Alice Maintainer',
                             'human', 1, 0, '2026-08-27T20:04:00Z', 'forged')
                    """
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO reviewer_admin_events(
                        event_sequence, reviewer_admin_event_id, manifest_id,
                        ordinal, event_kind, reviewer_id, display_label,
                        reviewer_kind, previous_active, new_active, effective_at,
                        basis
                    ) VALUES(
                        (SELECT event_sequence FROM reviewer_admin_events
                         WHERE reviewer_admin_event_id = 'activate_alice_001'),
                        'forged-event-sequence', 'reviewer-admin-fixture-001', 3,
                        'set_active', 'reviewer_alice', 'Alice Maintainer', 'human',
                        1, 0, '2026-08-27T20:04:00Z', 'forged sequence'
                    )
                    """
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_manifest_imports(
                        manifest_id, input_sha256, schema_version,
                        publication_decision_count, gate_decision_count, imported_at
                    ) VALUES('publication-replace-fixture', ?, 1, 1, 0,
                             '2026-08-27T20:04:00Z')
                    """,
                    ("d" * 64,),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_manifest_imports(
                        rowid, manifest_id, input_sha256, schema_version,
                        publication_decision_count, gate_decision_count, imported_at
                    ) VALUES(
                        (SELECT rowid FROM publication_manifest_imports
                         WHERE manifest_id = 'publication-replace-fixture'),
                        'publication-rowid-forgery', ?, 1, 1, 0,
                        '2026-08-27T20:04:00Z'
                    )
                    """,
                    ("a" * 64,),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_decisions(
                        publication_decision_id, object_type, object_id, decision,
                        reviewer_id, decided_at, basis
                    ) VALUES('publication_replace_001', 'source', 'source_replace',
                             'remove', 'reviewer_alice', '2026-08-27T20:04:00Z',
                             'forged')
                    """
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_decisions(
                        decision_sequence, publication_decision_id, object_type,
                        object_id, decision, reviewer_id, decided_at, basis
                    ) VALUES(
                        (SELECT decision_sequence FROM publication_decisions
                         WHERE publication_decision_id = 'publication_replace_001'),
                        'publication-sequence-forgery', 'source', 'source_replace',
                        'remove', 'reviewer_alice', '2026-08-27T20:04:00Z',
                        'forged sequence'
                    )
                    """
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_decisions(
                        publication_decision_id, object_type, object_id, decision,
                        reviewer_id, decided_at, basis
                    ) VALUES('publication_replace_natural', 'source', 'source_replace',
                             'publish', 'reviewer_alice', '2026-08-27T20:03:00Z',
                             'forged natural-key replacement')
                    """
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_gate_decisions(
                        publication_gate_decision_id, object_type, object_id,
                        gate_kind, decision, reviewer_id, decided_at, basis
                    ) VALUES('gate_replace_001', 'source', 'source_replace', 'privacy',
                             'withhold', 'reviewer_alice', '2026-08-27T20:04:00Z',
                             'forged')
                    """
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_gate_decisions(
                        publication_gate_decision_id, object_type, object_id,
                        gate_kind, decision, reviewer_id, decided_at, basis
                    ) VALUES('gate_replace_natural', 'source', 'source_replace',
                             'privacy', 'clear', 'reviewer_alice',
                             '2026-08-27T20:03:00Z', 'forged natural-key replacement')
                    """
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "replacement is forbidden"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO publication_gate_decisions(
                        gate_decision_sequence, publication_gate_decision_id,
                        object_type, object_id, gate_kind, decision, reviewer_id,
                        decided_at, basis
                    ) VALUES(
                        (SELECT gate_decision_sequence
                         FROM publication_gate_decisions
                         WHERE publication_gate_decision_id = 'gate_replace_001'),
                        'gate-sequence-forgery', 'source', 'source_replace',
                        'privacy', 'withhold', 'reviewer_alice',
                        '2026-08-27T20:04:00Z', 'forged sequence'
                    )
                    """
                )

            attack_time = utc_now()
            self.connection.execute("BEGIN")
            try:
                self.connection.execute(
                    """
                    INSERT INTO reviewer_admin_manifest_imports(
                        manifest_id, input_sha256, schema_version,
                        manifest_created_at, imported_at, authorized_by, basis,
                        registration_count, state_change_count, adoption_count
                    ) VALUES('reviewer-rowid-attack', ?, 1, ?, ?, 'forged',
                             'forged rowid replacement attempt', 1, 0, 0)
                    """,
                    ("9" * 64, attack_time, attack_time),
                )
                self.connection.execute(
                    """
                    INSERT INTO reviewer_admin_events(
                        reviewer_admin_event_id, manifest_id, ordinal, event_kind,
                        reviewer_id, display_label, reviewer_kind, previous_active,
                        new_active, effective_at, basis
                    ) VALUES('reviewer-rowid-register', 'reviewer-rowid-attack', 0,
                             'register', 'reviewer_rowid_attack', 'Rowid Attack',
                             'human', NULL, 0, ?, 'forged exact registration')
                    """,
                    (attack_time,),
                )
                with self.assertRaisesRegex(sqlite3.IntegrityError, "prior registration"):
                    self.connection.execute(
                        """
                        INSERT OR REPLACE INTO reviewers(
                            rowid, reviewer_id, display_label, reviewer_kind, active
                        ) VALUES(
                            (SELECT rowid FROM reviewers
                             WHERE reviewer_id = 'reviewer_alice'),
                            'reviewer_rowid_attack', 'Rowid Attack', 'human', 0
                        )
                        """
                    )
            finally:
                self.connection.rollback()
        finally:
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA recursive_triggers = ON")
        self.assertEqual(
            tuple(
                self.connection.execute(
                    """
                    SELECT object_id, decision, decided_at
                    FROM publication_decisions
                    WHERE publication_decision_id = 'publication_replace_001'
                    """
                ).fetchone()
            ),
            ("source_replace", "publish", "2026-08-27T20:03:00Z"),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT active FROM reviewers WHERE reviewer_id = 'reviewer_alice'"
            ).fetchone()[0],
            1,
        )

    def test_reviewer_kind_scope_and_future_database_events_fail_closed(self) -> None:
        register_reviewer_fixture(
            self.connection,
            "reviewer_policy_other",
            "Other Policy",
            "automated_policy",
        )
        register_reviewer_fixture(
            self.connection,
            "reviewer_imported",
            "Imported Attribution",
            "imported_legacy",
        )
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, created_at, updated_at
            ) VALUES('source_scope', 'youtube', 'youtube_video', 'scope',
                     'https://www.youtube.com/watch?v=scope',
                     '2026-08-27T20:00:00Z', 'public', 'reviewed',
                     '2026-08-27T20:00:00Z', '2026-08-27T20:00:00Z')
            """
        )
        for reviewer_id in ("reviewer_policy_other", "reviewer_imported"):
            manifest = {
                "schema_version": 1,
                "manifest_id": f"publication-scope-{reviewer_id}",
                "publication_decisions": [
                    {
                        "publication_decision_id": f"publication-scope-{reviewer_id}",
                        "object_type": "source",
                        "object_id": "source_scope",
                        "decision": "publish",
                        "reviewer_id": reviewer_id,
                        "decided_at": "2026-08-27T20:03:00Z",
                        "basis": "Must be rejected.",
                        "note": "Reviewer kind has no publication grant.",
                    }
                ],
                "gate_decisions": [],
            }
            with self.assertRaisesRegex(PublicationManifestError, "not authorized"):
                validate_publication_manifest(
                    self.connection,
                    self._write(manifest, f"scope-{reviewer_id}.json"),
                )
            with self.assertRaisesRegex(PublicationManifestError, "not authorized"):
                apply_publication_manifest(
                    self.connection,
                    self._write(manifest, f"scope-apply-{reviewer_id}.json"),
                )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "not authorized"):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id, gate_kind,
                    decision, reviewer_id, decided_at, basis
                ) VALUES('gate-policy-clear', 'source', 'source_scope', 'privacy',
                         'clear', 'reviewer_policy_other',
                         '2026-08-27T20:03:00Z', 'must fail')
                """
            )
        with transaction(self.connection):
            ensure_public_metadata_policy_reviewer(self.connection)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "not authorized"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis, public_label
                ) VALUES('builtin-arbitrary', 'source', 'source_scope', 'publish',
                         'reviewer_public_metadata_policy_v1', ?, ?, 'source metadata')
                """,
                (
                    utc_now(),
                    "Public URL, creator/platform title label, access label, and "
                    "stable native ID only; no legacy transcript content",
                ),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "not authorized"):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id, gate_kind,
                    decision, reviewer_id, decided_at, basis
                ) VALUES('builtin-arbitrary-gate', 'source', 'source_scope', 'privacy',
                         'withhold', 'reviewer_public_metadata_policy_v1', ?,
                         'must fail outside the metadata publication capability')
                """,
                (utc_now(),),
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "time must not be in the future"):
            self.connection.execute(
                """
                INSERT INTO reviewer_admin_manifest_imports(
                    manifest_id, input_sha256, schema_version, manifest_created_at,
                    imported_at, authorized_by, basis, registration_count,
                    state_change_count, adoption_count
                ) VALUES('future-direct', ?, 1, '2099-01-01T00:00:00Z',
                         '2099-01-01T00:00:00Z', 'fixture', 'must fail', 1, 0, 0)
                """,
                ("f" * 64,),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "time must not be in the future"):
            self.connection.execute(
                """
                INSERT INTO reviewer_admin_events(
                    reviewer_admin_event_id, manifest_id, ordinal, event_kind,
                    reviewer_id, display_label, reviewer_kind, previous_active,
                    new_active, effective_at, basis
                ) VALUES('future-event-direct', ?, 2, 'set_active',
                         'reviewer_policy_other', 'Other Policy',
                         'automated_policy', 1, 0, '2099-01-01T00:00:00Z',
                         'must fail')
                """,
                (
                    self.connection.execute(
                        """
                        SELECT manifest_id FROM reviewer_admin_events
                        WHERE reviewer_id = 'reviewer_policy_other'
                        ORDER BY event_sequence DESC LIMIT 1
                        """
                    ).fetchone()[0],
                ),
            )

    def test_database_validation_detects_a_forged_transition_chain(self) -> None:
        apply_reviewer_admin_manifest(
            self.connection, self._write(self.manifest), dry_run=False
        )
        imported_at = utc_now()
        self.connection.execute(
            """
            INSERT INTO reviewer_admin_manifest_imports(
                manifest_id, input_sha256, schema_version, manifest_created_at,
                imported_at, authorized_by, basis, registration_count,
                state_change_count, adoption_count
            ) VALUES('forged-chain-manifest', ?, 1, ?, ?, 'test_fixture_harness',
                     'Disposable validation corruption fixture.', 0, 1, 0)
            """,
            ("c" * 64, imported_at, imported_at),
        )
        self.connection.execute("DROP TRIGGER reviewer_admin_state_snapshot_matches")
        self.connection.execute(
            """
            INSERT INTO reviewer_admin_events(
                reviewer_admin_event_id, manifest_id, ordinal, event_kind,
                reviewer_id, display_label, reviewer_kind, previous_active,
                new_active, effective_at, basis
            ) VALUES('forged-chain-event', 'forged-chain-manifest', 0,
                     'set_active', 'reviewer_alice', 'Alice Maintainer', 'human',
                     0, 1, ?, 'Previous state deliberately disagrees with the stream.')
            """,
            (imported_at,),
        )
        with self.assertRaisesRegex(RuntimeError, "invalid enrollment or chronology"):
            validate_database(self.connection)

    def test_migration_adopts_only_preexisting_rows_then_closes_legacy_path(self) -> None:
        migration_root = self.root / "legacy-migrations"
        migration_root.mkdir()
        source_root = db_module.MIGRATIONS_DIR
        for source in sorted(source_root.glob("*.sql")):
            if int(source.name[:4]) <= 26:
                shutil.copy2(source, migration_root / source.name)
        legacy_database = self.root / "legacy.sqlite3"
        with mock.patch.object(db_module, "MIGRATIONS_DIR", migration_root):
            legacy = connect(legacy_database)
            self.addCleanup(legacy.close)
            self.assertEqual(len(migrate(legacy)), 26)
            legacy.execute(
                """
                INSERT INTO reviewers(reviewer_id, display_label, reviewer_kind, active)
                VALUES('legacy_before_0027', '<Legacy label>', 'imported_legacy', 1)
                """
            )
            shutil.copy2(
                source_root / "0027_reviewer_admin.sql",
                migration_root / "0027_reviewer_admin.sql",
            )
            self.assertEqual(migrate(legacy), ["0027_reviewer_admin.sql"])
            adoption = legacy.execute(
                """
                SELECT event_kind, display_label, reviewer_kind, new_active,
                       effective_at
                FROM reviewer_admin_events
                WHERE reviewer_id = 'legacy_before_0027'
                """
            ).fetchone()
            self.assertEqual(
                tuple(adoption)[:4],
                ("legacy_adopt", "<Legacy label>", "imported_legacy", 1),
            )
            self.assertNotEqual(adoption["effective_at"], "1970-01-01T00:00:00Z")
            with self.assertRaisesRegex(sqlite3.IntegrityError, "prior registration"):
                legacy.execute(
                    """
                    INSERT INTO reviewers(
                        reviewer_id, display_label, reviewer_kind, active
                    ) VALUES('legacy_after_0027', 'Late bypass', 'human', 1)
                    """
                )
            validate_database(legacy)

    def test_migration_refuses_preexisting_future_publication_authority(self) -> None:
        migration_root = self.root / "future-decision-migrations"
        migration_root.mkdir()
        source_root = db_module.MIGRATIONS_DIR
        for source in sorted(source_root.glob("*.sql")):
            if int(source.name[:4]) <= 26:
                shutil.copy2(source, migration_root / source.name)
        database = self.root / "future-decision.sqlite3"
        with mock.patch.object(db_module, "MIGRATIONS_DIR", migration_root):
            connection = connect(database)
            self.addCleanup(connection.close)
            self.assertEqual(len(migrate(connection)), 26)
            connection.execute(
                """
                INSERT INTO reviewers(
                    reviewer_id, display_label, reviewer_kind, active
                ) VALUES('future_legacy_reviewer', 'Future Legacy Reviewer',
                         'human', 1)
                """
            )
            connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('future_before_0027', 'source', 'future_source', 'publish',
                         'future_legacy_reviewer', '2099-01-01T00:00:00Z',
                         'A future row that must block migration.')
                """
            )
            shutil.copy2(
                source_root / "0027_reviewer_admin.sql",
                migration_root / "0027_reviewer_admin.sql",
            )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "CHECK constraint"):
                migrate(connection)
            self.assertEqual(
                connection.execute(
                    "SELECT max(version) FROM schema_migrations"
                ).fetchone()[0],
                26,
            )
            self.assertIsNone(
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'reviewer_admin_events'
                    """
                ).fetchone()
            )
            self.assertIsNone(
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table'
                      AND name = 'migration_0027_no_future_decision_guard'
                    """
                ).fetchone()
            )
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM publication_decisions "
                    "WHERE publication_decision_id = 'future_before_0027'"
                ).fetchone()[0],
                1,
            )


if __name__ == "__main__":
    unittest.main()
