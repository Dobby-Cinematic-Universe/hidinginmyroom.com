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
from himr_corpus.importers import import_current_channel  # noqa: E402
from himr_corpus.publication_admin import (  # noqa: E402
    PublicationManifestError,
    apply_publication_manifest,
    load_publication_manifest,
    validate_publication_manifest,
)
from himr_corpus.reviewer_admin import (  # noqa: E402
    ensure_public_metadata_policy_reviewer,
)
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


class PublicationAdminTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.connection = connect(self.root / "corpus.sqlite3")
        self.addCleanup(self.connection.close)
        migrate(self.connection)
        register_reviewer_fixture(
            self.connection, "reviewer_alice", "Alice Maintainer"
        )
        register_reviewer_fixture(
            self.connection,
            "reviewer_inactive",
            "Inactive Maintainer",
            active=False,
        )
        self._insert_source("source_one", "one")
        self._insert_source("source_two", "two")

    def _insert_source(self, source_id: str, native_id: str) -> None:
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, created_at, updated_at
            ) VALUES(?, 'youtube', 'youtube_video', ?, ?, '2026-08-26T10:00:00Z',
                     'public', 'reviewed', '2026-08-26T10:00:00Z',
                     '2026-08-26T10:00:00Z')
            """,
            (source_id, native_id, f"https://www.youtube.com/watch?v={native_id}"),
        )

    def _manifest(self, *, object_id: str = "source_one") -> dict[str, object]:
        timestamp = "2026-08-26T12:00:00Z"
        return {
            "schema_version": 1,
            "manifest_id": "publication_batch_001",
            "publication_decisions": [
                {
                    "publication_decision_id": "publication_source_one_001",
                    "object_type": "source",
                    "object_id": object_id,
                    "decision": "publish",
                    "reviewer_id": "reviewer_alice",
                    "decided_at": timestamp,
                    "basis": "Reviewed source metadata and public availability.",
                    "note": "Approved only for the catalog's public source projection.",
                }
            ],
            "gate_decisions": [
                {
                    "publication_gate_decision_id": f"gate_source_one_{gate_kind}_001",
                    "object_type": "source",
                    "object_id": object_id,
                    "gate_kind": gate_kind,
                    "decision": "clear",
                    "reviewer_id": "reviewer_alice",
                    "decided_at": timestamp,
                    "basis": f"Completed {gate_kind} checklist.",
                    "note": f"No unresolved {gate_kind} blocker found in this review.",
                }
                for gate_kind in ("rights", "privacy", "sensitivity")
            ],
        }

    def _write(self, value: dict[str, object], name: str = "manifest.json") -> Path:
        path = self.root / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_validate_dry_run_and_atomic_import(self):
        path = self._write(self._manifest())
        validation = validate_publication_manifest(self.connection, path)
        self.assertTrue(validation["validated"])
        self.assertTrue(validation["dry_run"])
        self.assertEqual(validation["publication_decisions"], 1)
        self.assertEqual(validation["gate_decisions"], 3)
        self.assertEqual(validation["decisions_inserted"], 0)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_decisions"
            ).fetchone()[0],
            0,
        )

        dry_run = apply_publication_manifest(self.connection, path, dry_run=True)
        self.assertTrue(dry_run["dry_run"])
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_gate_decisions"
            ).fetchone()[0],
            0,
        )

        imported = apply_publication_manifest(self.connection, path)
        self.assertFalse(imported["dry_run"])
        self.assertEqual(imported["decisions_inserted"], 4)
        self.assertEqual(
            tuple(self.connection.execute(
                "SELECT notes, manifest_id FROM publication_decisions"
            ).fetchone()),
            (
                "Approved only for the catalog's public source projection.",
                "publication_batch_001",
            ),
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM public_sources").fetchone()[0],
            1,
        )

        with self.assertRaisesRegex(PublicationManifestError, "already imported"):
            apply_publication_manifest(self.connection, path)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_decisions"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_gate_decisions"
            ).fetchone()[0],
            3,
        )
        ledger = self.connection.execute(
            """
            SELECT manifest_id, input_sha256, publication_decision_count,
                   gate_decision_count
            FROM publication_manifest_imports
            """
        ).fetchone()
        self.assertEqual(ledger["manifest_id"], "publication_batch_001")
        self.assertEqual(len(ledger["input_sha256"]), 64)
        self.assertEqual(
            (ledger["publication_decision_count"], ledger["gate_decision_count"]),
            (1, 3),
        )

    def test_strict_shape_utc_identity_and_reference_validation(self):
        unknown = self._manifest()
        unknown["unexpected"] = True
        with self.assertRaisesRegex(PublicationManifestError, "unknown"):
            load_publication_manifest(self._write(unknown, "unknown.json"))

        no_note = self._manifest()
        del no_note["publication_decisions"][0]["note"]  # type: ignore[index]
        with self.assertRaisesRegex(PublicationManifestError, "missing.*note"):
            load_publication_manifest(self._write(no_note, "no-note.json"))

        offset_time = self._manifest()
        offset_time["publication_decisions"][0]["decided_at"] = (  # type: ignore[index]
            "2026-08-26T08:00:00-04:00"
        )
        with self.assertRaisesRegex(PublicationManifestError, "ending in Z"):
            load_publication_manifest(self._write(offset_time, "offset.json"))

        inactive = self._manifest()
        inactive["publication_decisions"][0]["reviewer_id"] = (  # type: ignore[index]
            "reviewer_inactive"
        )
        with self.assertRaisesRegex(PublicationManifestError, "inactive reviewer"):
            validate_publication_manifest(self.connection, self._write(inactive, "inactive.json"))

        wrong_type = self._manifest()
        wrong_type["publication_decisions"][0]["object_type"] = "recording"  # type: ignore[index]
        with self.assertRaisesRegex(PublicationManifestError, "unknown recording"):
            validate_publication_manifest(
                self.connection, self._write(wrong_type, "wrong-type.json")
            )

        missing_object = self._manifest(object_id="source_missing")
        with self.assertRaisesRegex(PublicationManifestError, "unknown source"):
            validate_publication_manifest(
                self.connection, self._write(missing_object, "missing-object.json")
            )

        before_activation = self._manifest()
        before_activation["publication_decisions"] = [
            before_activation["publication_decisions"][0]  # type: ignore[index]
        ]
        before_activation["gate_decisions"] = []
        before_activation["publication_decisions"][0]["decided_at"] = (  # type: ignore[index]
            "1970-12-31T23:59:59Z"
        )
        with self.assertRaisesRegex(PublicationManifestError, "not active at decided_at"):
            validate_publication_manifest(
                self.connection,
                self._write(before_activation, "before-activation.json"),
            )

        future_publication = self._manifest()
        future_publication["publication_decisions"][0]["decided_at"] = (  # type: ignore[index]
            "2099-01-01T00:00:00Z"
        )
        with self.assertRaisesRegex(PublicationManifestError, "current UTC time"):
            load_publication_manifest(
                self._write(future_publication, "future-publication.json")
            )

        future_gate = self._manifest()
        future_gate["publication_decisions"] = []
        future_gate["gate_decisions"] = [future_gate["gate_decisions"][0]]  # type: ignore[index]
        future_gate["gate_decisions"][0]["decided_at"] = (  # type: ignore[index]
            "2099-01-01T00:00:00Z"
        )
        with self.assertRaisesRegex(PublicationManifestError, "current UTC time"):
            load_publication_manifest(self._write(future_gate, "future-gate.json"))

        with self.assertRaisesRegex(sqlite3.IntegrityError, "time must not be in the future"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('future-publication-direct', 'source', 'source_one',
                         'withhold', 'reviewer_alice', '2099-01-01T00:00:00Z',
                         'Future decisions take effect immediately and must fail.')
                """
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "time must not be in the future"):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id, gate_kind,
                    decision, reviewer_id, decided_at, basis
                ) VALUES('future-gate-direct', 'source', 'source_one', 'privacy',
                         'withhold', 'reviewer_alice', '2099-01-01T00:00:00Z',
                         'Future gate decisions take effect immediately and must fail.')
                """
            )

    def test_built_in_metadata_policy_has_one_shared_exact_scope(self):
        inventory = self.root / "policy-inventory.json"
        observed = "2026-08-26T12:00:00Z"
        inventory.write_text(
            json.dumps(
                {
                    "expected_channel": {
                        "stable_channel_id": "UC_yIF-9jOge6nNA0z-ScrBQ",
                        "display_name": "Hiding in my room",
                    },
                    "items": [
                        {
                            "video_id": "policy00001",
                            "type": "video",
                            "observed": {
                                "title": "Policy scope fixture",
                                "publish_date_utc": observed,
                                "duration_seconds": 5,
                                "inferred_access": "public",
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        import_current_channel(
            self.connection,
            inventory,
            snapshot_date="2026-08-26",
            observed_at=observed,
        )
        with transaction(self.connection):
            ensure_public_metadata_policy_reviewer(self.connection)
        scope = self.connection.execute(
            """
            SELECT object_type, object_id, basis, public_label
            FROM public_metadata_policy_publish_scope
            ORDER BY object_type, object_id
            """
        ).fetchall()
        self.assertEqual([row["object_type"] for row in scope], ["recording", "source"])
        decided_at = utc_now()

        def policy_manifest(manifest_id: str) -> dict[str, object]:
            return {
                "schema_version": 1,
                "manifest_id": manifest_id,
                "publication_decisions": [
                    {
                        "publication_decision_id": f"{manifest_id}_{row['object_type']}",
                        "object_type": row["object_type"],
                        "object_id": row["object_id"],
                        "decision": "publish",
                        "reviewer_id": "reviewer_public_metadata_policy_v1",
                        "decided_at": decided_at,
                        "basis": row["basis"],
                        "note": "Exact built-in policy capability fixture.",
                        "public_label": row["public_label"],
                    }
                    for row in scope
                ],
                "gate_decisions": [],
            }

        wrong_label = policy_manifest("policy-scope-wrong-label")
        wrong_label["publication_decisions"][0]["public_label"] = (  # type: ignore[index]
            "full content"
        )
        wrong_path = self._write(wrong_label, "policy-wrong-label.json")
        for operation in (validate_publication_manifest, apply_publication_manifest):
            with self.assertRaisesRegex(PublicationManifestError, "not authorized"):
                operation(self.connection, wrong_path)

        restrictive = policy_manifest("policy-scope-restrictive")
        restrictive["publication_decisions"] = [  # type: ignore[index]
            restrictive["publication_decisions"][0]  # type: ignore[index]
        ]
        restrictive["publication_decisions"][0]["decision"] = "remove"  # type: ignore[index]
        restrictive_path = self._write(restrictive, "policy-restrictive.json")
        for operation in (validate_publication_manifest, apply_publication_manifest):
            with self.assertRaisesRegex(PublicationManifestError, "not authorized"):
                operation(self.connection, restrictive_path)

        exact_path = self._write(policy_manifest("policy-scope-exact"), "policy-exact.json")
        self.assertTrue(validate_publication_manifest(self.connection, exact_path)["dry_run"])
        applied = apply_publication_manifest(self.connection, exact_path)
        self.assertEqual(applied["decisions_inserted"], 2)

    def test_duplicate_ambiguous_and_nonchronological_manifest_is_rejected(self):
        duplicate = self._manifest()
        duplicate["gate_decisions"][0]["publication_gate_decision_id"] = (  # type: ignore[index]
            "publication_source_one_001"
        )
        with self.assertRaisesRegex(PublicationManifestError, "duplicate decision IDs"):
            load_publication_manifest(self._write(duplicate, "duplicate.json"))

        duplicate_key_path = self.root / "duplicate-key.json"
        duplicate_key_path.write_text(
            '{"schema_version":1,"schema_version":1,"manifest_id":"x",'
            '"publication_decisions":[],"gate_decisions":[]}',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PublicationManifestError, "duplicate key"):
            load_publication_manifest(duplicate_key_path)

        boolean_version = self._manifest()
        boolean_version["schema_version"] = True
        with self.assertRaisesRegex(PublicationManifestError, "schema_version"):
            load_publication_manifest(self._write(boolean_version, "boolean-version.json"))

        ambiguous = self._manifest()
        ambiguous["publication_decisions"].append(  # type: ignore[union-attr]
            {
                **copy.deepcopy(ambiguous["publication_decisions"][0]),  # type: ignore[index]
                "publication_decision_id": "publication_source_one_002",
                "decision": "withhold",
            }
        )
        with self.assertRaisesRegex(PublicationManifestError, "ambiguous same-time"):
            load_publication_manifest(self._write(ambiguous, "ambiguous.json"))

        unsafe = self._manifest()
        unsafe["publication_decisions"][0]["decision"] = "remove"  # type: ignore[index]
        unsafe["publication_decisions"].append(  # type: ignore[union-attr]
            {
                **copy.deepcopy(unsafe["publication_decisions"][0]),  # type: ignore[index]
                "publication_decision_id": "publication_source_one_002",
                "decision": "publish",
            }
        )
        with self.assertRaisesRegex(PublicationManifestError, "unsafe same-time weakening"):
            load_publication_manifest(self._write(unsafe, "unsafe-inside.json"))

        backwards = self._manifest()
        backwards["publication_decisions"].append(  # type: ignore[union-attr]
            {
                **copy.deepcopy(backwards["publication_decisions"][0]),  # type: ignore[index]
                "publication_decision_id": "publication_source_one_002",
                "decided_at": "2026-08-26T11:59:59Z",
            }
        )
        with self.assertRaisesRegex(PublicationManifestError, "not chronological"):
            load_publication_manifest(self._write(backwards, "backwards.json"))

    def test_existing_same_time_weakening_rejects_entire_manifest(self):
        timestamp = "2026-08-26T12:00:00Z"
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis, notes
            ) VALUES('existing_remove', 'source', 'source_one', 'remove',
                     'reviewer_alice', ?, 'Emergency takedown.', 'Pending review.')
            """,
            (timestamp,),
        )
        self.connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id, gate_kind,
                decision, reviewer_id, decided_at, basis, notes
            ) VALUES('existing_privacy_withhold', 'source', 'source_one', 'privacy',
                     'withhold', 'reviewer_alice', ?, 'Privacy hold.', 'Pending review.')
            """,
            (timestamp,),
        )
        manifest = self._manifest(object_id="source_two")
        manifest["publication_decisions"].append(  # type: ignore[union-attr]
            {
                "publication_decision_id": "unsafe_source_one_publish",
                "object_type": "source",
                "object_id": "source_one",
                "decision": "publish",
                "reviewer_id": "reviewer_alice",
                "decided_at": timestamp,
                "basis": "Attempted republication.",
                "note": "This must not override the same-time removal.",
            }
        )
        with self.assertRaisesRegex(PublicationManifestError, "unsafe same-time weakening"):
            apply_publication_manifest(
                self.connection, self._write(manifest, "unsafe-existing.json")
            )
        self.assertIsNone(
            self.connection.execute(
                "SELECT 1 FROM publication_decisions WHERE object_id = 'source_two'"
            ).fetchone()
        )

        gate_manifest = self._manifest(object_id="source_two")
        gate_manifest["publication_decisions"] = []
        gate_manifest["gate_decisions"] = [
            {
                "publication_gate_decision_id": "unsafe_source_one_privacy_clear",
                "object_type": "source",
                "object_id": "source_one",
                "gate_kind": "privacy",
                "decision": "clear",
                "reviewer_id": "reviewer_alice",
                "decided_at": timestamp,
                "basis": "Attempted clearance.",
                "note": "This must not override the same-time privacy hold.",
            }
        ]
        with self.assertRaisesRegex(PublicationManifestError, "unsafe same-time weakening"):
            apply_publication_manifest(
                self.connection, self._write(gate_manifest, "unsafe-gate.json")
            )

    def test_database_insert_failure_rolls_back_decisions_and_manifest_ledger(self):
        self.connection.execute(
            """
            CREATE TRIGGER inject_publication_admin_failure
            BEFORE INSERT ON publication_gate_decisions
            WHEN NEW.gate_kind = 'sensitivity'
            BEGIN
                SELECT RAISE(ABORT, 'injected publication admin failure');
            END
            """
        )
        with self.assertRaisesRegex(
            PublicationManifestError, "injected publication admin failure"
        ):
            apply_publication_manifest(
                self.connection, self._write(self._manifest(), "injected-failure.json")
            )
        for table in (
            "publication_decisions",
            "publication_gate_decisions",
            "publication_manifest_imports",
        ):
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                0,
            )

    def test_cli_exposes_validation_import_and_dry_run(self):
        parser = build_parser()
        validate_args = parser.parse_args(
            [
                "validate-publication-manifest",
                "--db",
                "private.sqlite3",
                "--manifest",
                "private.json",
            ]
        )
        self.assertEqual(validate_args.command, "validate-publication-manifest")
        import_args = parser.parse_args(
            [
                "import-publication-manifest",
                "--db",
                "private.sqlite3",
                "--manifest",
                "private.json",
                "--dry-run",
            ]
        )
        self.assertTrue(import_args.dry_run)

    def test_publication_cli_dry_modes_are_filesystem_immutable(self):
        manifest = self._write(self._manifest(), "immutable-publication.json")
        self.connection.close()
        before_hash = hashlib.sha256(
            self.root.joinpath("corpus.sqlite3").read_bytes()
        ).hexdigest()
        before_mtime = self.root.joinpath("corpus.sqlite3").stat().st_mtime_ns
        for arguments in (
            ["validate-publication-manifest"],
            ["import-publication-manifest", "--dry-run"],
        ):
            with self.subTest(command=arguments[0]):
                with contextlib.redirect_stdout(io.StringIO()) as output:
                    main(
                        [
                            arguments[0],
                            "--db",
                            str(self.root / "corpus.sqlite3"),
                            "--manifest",
                            str(manifest),
                            *arguments[1:],
                        ]
                    )
                self.assertTrue(json.loads(output.getvalue())["dry_run"])
                self.assertEqual(
                    hashlib.sha256(
                        self.root.joinpath("corpus.sqlite3").read_bytes()
                    ).hexdigest(),
                    before_hash,
                )
                self.assertEqual(
                    self.root.joinpath("corpus.sqlite3").stat().st_mtime_ns,
                    before_mtime,
                )
                for suffix in ("-wal", "-shm", "-journal"):
                    self.assertFalse(
                        Path(f"{self.root / 'corpus.sqlite3'}{suffix}").exists()
                    )

    def test_publication_cli_dry_modes_refuse_pending_migrations_without_writes(self):
        migration_root = self.root / "migrations-v26"
        migration_root.mkdir()
        for source in sorted(db_module.MIGRATIONS_DIR.glob("*.sql")):
            if int(source.name[:4]) <= 26:
                shutil.copy2(source, migration_root / source.name)
        database = self.root / "pending.sqlite3"
        with mock.patch.object(db_module, "MIGRATIONS_DIR", migration_root):
            connection = connect(database)
            try:
                migrate(connection)
            finally:
                connection.close()
        manifest = self._write(self._manifest(), "pending-publication.json")
        before_hash = hashlib.sha256(database.read_bytes()).hexdigest()
        before_mtime = database.stat().st_mtime_ns
        for arguments in (
            ["validate-publication-manifest"],
            ["import-publication-manifest", "--dry-run"],
        ):
            with self.subTest(command=arguments[0]):
                with self.assertRaisesRegex(RuntimeError, "Pending migrations"):
                    main(
                        [
                            arguments[0],
                            "--db",
                            str(database),
                            "--manifest",
                            str(manifest),
                            *arguments[1:],
                        ]
                    )
                self.assertEqual(
                    hashlib.sha256(database.read_bytes()).hexdigest(), before_hash
                )
                self.assertEqual(database.stat().st_mtime_ns, before_mtime)
                for suffix in ("-wal", "-shm", "-journal"):
                    self.assertFalse(Path(f"{database}{suffix}").exists())


if __name__ == "__main__":
    unittest.main()
