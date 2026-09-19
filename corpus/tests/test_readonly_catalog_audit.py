from __future__ import annotations

import hashlib
import io
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import (  # noqa: E402
    connect,
    connect_audit_readonly,
    migrate,
    verify_migrations,
)
from himr_corpus.cli import main as cli_main  # noqa: E402
from himr_corpus.validation import database_status, validate_database  # noqa: E402


class ReadOnlyCatalogAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="catalog-audit-")
        self.root = Path(self.temporary.name)
        self.migrations = self.root / "migrations"
        self.migrations.mkdir()
        self.database = self.root / "catalog.sqlite3"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _migration(self, version: int, name: str, sql: str) -> Path:
        path = self.migrations / f"{version:04d}_{name}.sql"
        path.write_text(sql, encoding="utf-8")
        return path

    def _migrate_and_close(self) -> None:
        connection = connect(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                migrate(connection)
        finally:
            connection.close()

    @staticmethod
    def _filesystem_snapshot(path: Path) -> tuple[bytes, int, int, int, tuple[str, ...]]:
        file_stat = path.stat()
        directory_stat = path.parent.stat()
        return (
            path.read_bytes(),
            file_stat.st_mtime_ns,
            file_stat.st_ctime_ns,
            directory_stat.st_mtime_ns,
            tuple(sorted(child.name for child in path.parent.iterdir())),
        )

    @staticmethod
    def _run_cli(command: str, database: Path) -> dict:
        output = io.StringIO()
        with (
            mock.patch(
                "himr_corpus.cli._connection",
                side_effect=AssertionError("writable CLI path must not be used"),
            ),
            redirect_stdout(output),
        ):
            cli_main([command, "--db", str(database)])
        return json.loads(output.getvalue())

    def test_immutable_audit_open_verifies_without_filesystem_mutation(self) -> None:
        self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        self._migrate_and_close()
        before = self.database.read_bytes()
        before_stat = self.database.stat()
        before_directory_stat = self.root.stat()
        before_entries = sorted(path.name for path in self.root.iterdir())
        self.assertFalse(Path(f"{self.database}-wal").exists())
        self.assertFalse(Path(f"{self.database}-shm").exists())

        with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
            connection = connect_audit_readonly(self.database)
            try:
                self.assertEqual(verify_migrations(connection), ("0001_one.sql",))
            finally:
                connection.close()

        after_stat = self.database.stat()
        after_directory_stat = self.root.stat()
        self.assertEqual(self.database.read_bytes(), before)
        self.assertEqual(after_stat.st_mtime_ns, before_stat.st_mtime_ns)
        self.assertEqual(after_stat.st_ctime_ns, before_stat.st_ctime_ns)
        self.assertEqual(
            after_directory_stat.st_mtime_ns, before_directory_stat.st_mtime_ns
        )
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), before_entries)
        self.assertFalse(Path(f"{self.database}-wal").exists())
        self.assertFalse(Path(f"{self.database}-shm").exists())
        self.assertFalse(Path(f"{self.database}-journal").exists())

    def test_audit_open_rejects_any_sqlite_sidecar(self) -> None:
        self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        self._migrate_and_close()
        sidecar = Path(f"{self.database}-wal")
        sidecar.write_bytes(b"")
        with self.assertRaisesRegex(RuntimeError, "closed, checkpointed"):
            connect_audit_readonly(self.database)

    def test_verifier_rejects_pending_and_missing_migrations(self) -> None:
        first = self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        self._migrate_and_close()
        second = self._migration(2, "two", "CREATE TABLE two(value TEXT);\n")
        connection = connect_audit_readonly(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                with self.assertRaisesRegex(RuntimeError, "Pending migrations"):
                    verify_migrations(connection)
        finally:
            connection.close()

        second.unlink()
        connection = connect(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                self.assertEqual(migrate(connection), [])
        finally:
            connection.close()
        first.unlink()
        self._migration(1, "replacement", "CREATE TABLE replacement(value TEXT);\n")
        connection = connect_audit_readonly(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                with self.assertRaisesRegex(RuntimeError, "name differs"):
                    verify_migrations(connection)
        finally:
            connection.close()

        # Build a separate two-version ledger, then remove its terminal file.
        other_db = self.root / "two.sqlite3"
        self.migrations.joinpath("0001_replacement.sql").unlink()
        self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        terminal = self._migration(2, "two", "CREATE TABLE two(value TEXT);\n")
        writable = connect(other_db)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                migrate(writable)
        finally:
            writable.close()
        terminal.unlink()
        readonly = connect_audit_readonly(other_db)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                with self.assertRaisesRegex(RuntimeError, "missing from disk"):
                    verify_migrations(readonly)
        finally:
            readonly.close()

    def test_verifier_rejects_renamed_and_hash_changed_migrations(self) -> None:
        migration = self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        self._migrate_and_close()
        original = migration.read_bytes()

        migration.rename(self.migrations / "0001_renamed.sql")
        connection = connect_audit_readonly(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                with self.assertRaisesRegex(RuntimeError, "name differs"):
                    verify_migrations(connection)
        finally:
            connection.close()

        renamed = self.migrations / "0001_renamed.sql"
        renamed.rename(migration)
        migration.write_bytes(original + b"-- changed\n")
        connection = connect_audit_readonly(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                with self.assertRaisesRegex(RuntimeError, "hash differs"):
                    verify_migrations(connection)
        finally:
            connection.close()

    def test_manifest_rejects_gap_duplicate_zero_and_bad_names(self) -> None:
        cases = (
            (("0002_two.sql",), "contiguous"),
            (("0001_one.sql", "0001_two.sql"), "Duplicate"),
            (("0000_zero.sql",), "start at 0001"),
            (("0001.bad.sql",), "Invalid migration filename"),
        )
        for index, (names, message) in enumerate(cases):
            with self.subTest(names=names):
                case_root = self.root / f"case-{index}"
                case_root.mkdir()
                for name in names:
                    (case_root / name).write_text("SELECT 1;\n", encoding="utf-8")
                connection = connect(":memory:")
                try:
                    with mock.patch("himr_corpus.db.MIGRATIONS_DIR", case_root):
                        with self.assertRaisesRegex(RuntimeError, message):
                            migrate(connection)
                finally:
                    connection.close()

    def test_migration_hash_in_ledger_matches_exact_bytes(self) -> None:
        migration = self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        self._migrate_and_close()
        connection = connect_audit_readonly(self.database)
        try:
            row = connection.execute(
                "SELECT name, sha256 FROM schema_migrations WHERE version = 1"
            ).fetchone()
            self.assertEqual(row["name"], migration.name)
            self.assertEqual(row["sha256"], hashlib.sha256(migration.read_bytes()).hexdigest())
        finally:
            connection.close()

    def test_verifier_rejects_duplicate_rows_in_a_tampered_ledger(self) -> None:
        self._migration(1, "one", "SELECT 1;\n")
        digest = hashlib.sha256(
            (self.migrations / "0001_one.sql").read_bytes()
        ).hexdigest()
        connection = sqlite3.connect(self.database)
        try:
            connection.execute(
                "CREATE TABLE schema_migrations("
                "version INTEGER, name TEXT, sha256 TEXT, applied_at TEXT)"
            )
            connection.executemany(
                "INSERT INTO schema_migrations VALUES(1, ?, ?, '2026-08-27T00:00:00Z')",
                (("0001_one.sql", digest), ("0001_one.sql", digest)),
            )
            connection.commit()
        finally:
            connection.close()
        readonly = connect_audit_readonly(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                with self.assertRaisesRegex(RuntimeError, "duplicate version"):
                    verify_migrations(readonly)
        finally:
            readonly.close()

    def test_audit_open_does_not_create_missing_or_initialize_empty_database(self) -> None:
        missing = self.root / "missing" / "catalog.sqlite3"
        with self.assertRaises(FileNotFoundError):
            connect_audit_readonly(missing)
        self.assertFalse(missing.parent.exists())

        empty = self.root / "empty.sqlite3"
        empty.touch()
        before = empty.stat()
        readonly = connect_audit_readonly(empty)
        try:
            with self.assertRaisesRegex(RuntimeError, "no schema_migrations"):
                verify_migrations(readonly)
        finally:
            readonly.close()
        after = empty.stat()
        self.assertEqual(empty.read_bytes(), b"")
        self.assertEqual(after.st_mtime_ns, before.st_mtime_ns)
        self.assertFalse(Path(f"{empty}-wal").exists())
        self.assertFalse(Path(f"{empty}-shm").exists())

    def test_status_and_validate_use_immutable_audit_path_without_mutation(self) -> None:
        baseline = self.root / "full-baseline.sqlite3"
        writable = connect(baseline)
        try:
            migrate(writable)
        finally:
            writable.close()
        self.assertFalse(Path(f"{baseline}-wal").exists())
        self.assertFalse(Path(f"{baseline}-shm").exists())

        for command in ("status", "validate"):
            with self.subTest(command=command):
                command_root = self.root / command
                command_root.mkdir()
                catalog = command_root / "catalog.sqlite3"
                shutil.copy2(baseline, catalog)
                before = self._filesystem_snapshot(catalog)
                payload = self._run_cli(command, catalog)
                self.assertIn("sources" if command == "status" else "database", payload)
                self.assertEqual(self._filesystem_snapshot(catalog), before)
                self.assertFalse(Path(f"{catalog}-wal").exists())
                self.assertFalse(Path(f"{catalog}-shm").exists())
                self.assertFalse(Path(f"{catalog}-journal").exists())

    def test_status_and_validate_fail_pending_without_applying_it(self) -> None:
        self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        self._migrate_and_close()
        self._migration(2, "canary", "CREATE TABLE migration_canary(value TEXT);\n")
        before = self._filesystem_snapshot(self.database)
        with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
            for command in ("status", "validate"):
                with self.subTest(command=command):
                    with self.assertRaisesRegex(RuntimeError, "Pending migrations"):
                        self._run_cli(command, self.database)
        self.assertEqual(self._filesystem_snapshot(self.database), before)
        readonly = connect_audit_readonly(self.database)
        try:
            self.assertEqual(
                readonly.execute("SELECT max(version) FROM schema_migrations").fetchone()[0],
                1,
            )
            self.assertIsNone(
                readonly.execute(
                    "SELECT 1 FROM sqlite_master WHERE name = 'migration_canary'"
                ).fetchone()
            )
        finally:
            readonly.close()

    def test_cli_migration_mismatch_failures_preserve_catalog(self) -> None:
        cases = (("missing", "status", "missing from disk"), ("hash", "validate", "hash differs"))
        for state, command, message in cases:
            with self.subTest(state=state, command=command):
                case_root = self.root / f"cli-{state}"
                case_root.mkdir()
                migrations = case_root / "migrations"
                migrations.mkdir()
                first = migrations / "0001_one.sql"
                first.write_text("CREATE TABLE one(value TEXT);\n", encoding="utf-8")
                if state == "missing":
                    terminal = migrations / "0002_two.sql"
                    terminal.write_text("CREATE TABLE two(value TEXT);\n", encoding="utf-8")
                catalog = case_root / "catalog.sqlite3"
                writable = connect(catalog)
                try:
                    with mock.patch("himr_corpus.db.MIGRATIONS_DIR", migrations):
                        migrate(writable)
                finally:
                    writable.close()
                if state == "missing":
                    terminal.unlink()
                else:
                    first.write_text(
                        "CREATE TABLE one(value TEXT);\n-- changed\n", encoding="utf-8"
                    )
                before = self._filesystem_snapshot(catalog)
                with mock.patch("himr_corpus.db.MIGRATIONS_DIR", migrations):
                    with self.assertRaisesRegex(RuntimeError, message):
                        self._run_cli(command, catalog)
                self.assertEqual(self._filesystem_snapshot(catalog), before)

    def test_public_status_and_validation_functions_never_migrate(self) -> None:
        self._migration(1, "one", "CREATE TABLE one(value TEXT);\n")
        connection = connect(self.database)
        try:
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                migrate(connection)
            self._migration(
                2, "canary", "CREATE TABLE public_function_canary(value TEXT);\n"
            )
            before_changes = connection.total_changes
            with mock.patch("himr_corpus.db.MIGRATIONS_DIR", self.migrations):
                for function in (database_status, validate_database):
                    with self.subTest(function=function.__name__):
                        with self.assertRaisesRegex(RuntimeError, "Pending migrations"):
                            function(connection)
            self.assertEqual(connection.total_changes, before_changes)
            self.assertEqual(
                connection.execute(
                    "SELECT max(version) FROM schema_migrations"
                ).fetchone()[0],
                1,
            )
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name = 'public_function_canary'"
                ).fetchone()
            )
        finally:
            connection.close()

    def test_cli_does_not_create_missing_or_initialize_empty_database(self) -> None:
        missing = self.root / "missing-cli" / "catalog.sqlite3"
        with self.assertRaises(FileNotFoundError):
            self._run_cli("status", missing)
        self.assertFalse(missing.parent.exists())

        empty = self.root / "empty-cli.sqlite3"
        empty.touch()
        before = self._filesystem_snapshot(empty)
        with self.assertRaisesRegex(RuntimeError, "no schema_migrations"):
            self._run_cli("validate", empty)
        self.assertEqual(self._filesystem_snapshot(empty), before)


if __name__ == "__main__":
    unittest.main()
