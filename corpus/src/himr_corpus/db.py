"""SQLite connection, checksummed migrations, and transaction helpers."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]{4})_[A-Za-z0-9][A-Za-z0-9_-]*\.sql$")
SQLITE_VERSION = re.compile(r"^(?P<major>[0-9]+)\.(?P<minor>[0-9]+)\.(?P<patch>[0-9]+)(?:\D.*)?$")

# SQLite only began routing table-scoped PRAGMA integrity_check through virtual-table
# xIntegrity in 3.44.0. Migration 0033's private OCR FTS cache depends on that
# read-only check to detect shadow-table tampering, including from immutable readers.
MIGRATION_SQLITE_MINIMUMS: dict[int, tuple[int, int, int]] = {
    33: (3, 44, 0),
}


@dataclass(frozen=True)
class _Migration:
    version: int
    name: str
    sha256: str
    sql_bytes: bytes


def require_sqlite_version(
    connection: sqlite3.Connection,
    minimum: tuple[int, int, int],
    *,
    feature: str,
) -> tuple[int, int, int]:
    """Fail closed when a connection cannot provide a required SQLite feature."""

    compiled = tuple(int(value) for value in sqlite3.sqlite_version_info[:3])
    if compiled < minimum:
        required = ".".join(str(value) for value in minimum)
        found = ".".join(str(value) for value in compiled)
        raise RuntimeError(
            f"{feature} requires SQLite {required} or newer; Python is linked to {found}"
        )
    raw = connection.execute("SELECT sqlite_version()").fetchone()[0]
    if not isinstance(raw, str):
        raise RuntimeError(f"Cannot determine SQLite version required for {feature}")
    match = SQLITE_VERSION.fullmatch(raw)
    if match is None:
        raise RuntimeError(
            f"Cannot parse SQLite version {raw!r} required for {feature}"
        )
    current = tuple(int(match.group(name)) for name in ("major", "minor", "patch"))
    if current < minimum:
        required = ".".join(str(value) for value in minimum)
        raise RuntimeError(
            f"{feature} requires SQLite {required} or newer; found {raw}"
        )
    return current


def _require_migration_runtime(
    connection: sqlite3.Connection, versions: Iterator[int]
) -> None:
    for version in versions:
        minimum = MIGRATION_SQLITE_MINIMUMS.get(version)
        if minimum is not None:
            require_sqlite_version(
                connection,
                minimum,
                feature=f"migration {version:04d}",
            )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def connect(database: str | Path) -> sqlite3.Connection:
    path = Path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA recursive_triggers = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    return connection


def connect_readonly(database: str | Path) -> sqlite3.Connection:
    """Open an existing catalog without granting database-write authority.

    SQLite may still create or attach WAL bookkeeping files for a WAL-mode database.
    Commands requiring filesystem-level non-mutation must use
    :func:`connect_audit_readonly` instead.
    """

    path = Path(database).resolve(strict=True)
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA recursive_triggers = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA query_only = ON")
    return connection


def connect_audit_readonly(database: str | Path) -> sqlite3.Connection:
    """Open a quiescent catalog without creating SQLite sidecar files.

    ``immutable=1`` is safe only after excluding WAL/journal state: immutable readers
    intentionally ignore a WAL and could otherwise inspect a stale main database.
    Requiring a closed, checkpointed catalog also makes status/validation a strictly
    non-mutating filesystem operation.
    """

    path = Path(database).resolve(strict=True)
    if not path.is_file():
        raise RuntimeError("Audit catalog must be a regular file")
    sidecars = [
        Path(f"{path}{suffix}")
        for suffix in ("-wal", "-shm", "-journal")
        if Path(f"{path}{suffix}").exists()
    ]
    if sidecars:
        names = ", ".join(sidecar.name for sidecar in sidecars)
        raise RuntimeError(
            "Audit requires a closed, checkpointed catalog with no SQLite "
            f"sidecars; found: {names}"
        )
    connection = sqlite3.connect(
        f"{path.as_uri()}?mode=ro&immutable=1", uri=True, isolation_level=None
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA recursive_triggers = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA query_only = ON")
    return connection


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _migration_manifest() -> tuple[_Migration, ...]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        raise RuntimeError(f"No migrations found under {MIGRATIONS_DIR}")

    migrations: list[_Migration] = []
    seen_versions: dict[int, str] = {}
    for path in files:
        match = MIGRATION_NAME.fullmatch(path.name)
        if match is None:
            raise RuntimeError(f"Invalid migration filename: {path.name}")
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"Migration must be a regular non-symlink file: {path.name}")
        version = int(match.group("version"))
        if version == 0:
            raise RuntimeError("Migration versions must start at 0001")
        if version in seen_versions:
            raise RuntimeError(
                f"Duplicate migration version {version}: "
                f"{seen_versions[version]} and {path.name}"
            )
        body = path.read_bytes()
        migrations.append(
            _Migration(
                version=version,
                name=path.name,
                sha256=hashlib.sha256(body).hexdigest(),
                sql_bytes=body,
            )
        )
        seen_versions[version] = path.name

    migrations.sort(key=lambda migration: migration.version)
    versions = [migration.version for migration in migrations]
    expected = list(range(1, len(migrations) + 1))
    if versions != expected:
        raise RuntimeError(
            "Migration versions must be one contiguous sequence starting at 0001; "
            f"found {versions}"
        )
    return tuple(migrations)


def _migration_ledger(connection: sqlite3.Connection) -> dict[int, sqlite3.Row]:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master "
        "WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if table is None:
        raise RuntimeError("Catalog has no schema_migrations ledger")
    ledger: dict[int, sqlite3.Row] = {}
    for row in connection.execute(
        "SELECT version, name, sha256 FROM schema_migrations ORDER BY version"
    ):
        version = row["version"]
        if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
            raise RuntimeError("Migration ledger contains an invalid version")
        if version in ledger:
            raise RuntimeError(f"Migration ledger contains duplicate version {version:04d}")
        if not isinstance(row["name"], str) or not isinstance(row["sha256"], str):
            raise RuntimeError(f"Migration ledger row {version:04d} has invalid types")
        ledger[version] = row
    return ledger


def verify_migrations(connection: sqlite3.Connection) -> tuple[str, ...]:
    """Verify the exact on-disk migration ledger without executing SQL or writing."""

    manifest = _migration_manifest()
    expected = {migration.version: migration for migration in manifest}
    applied = _migration_ledger(connection)

    missing_files = sorted(set(applied) - set(expected))
    if missing_files:
        raise RuntimeError(
            "Applied migrations are missing from disk: "
            + ", ".join(f"{version:04d}" for version in missing_files)
        )
    pending = sorted(set(expected) - set(applied))
    if pending:
        raise RuntimeError(
            "Pending migrations require the explicit migrate command: "
            + ", ".join(expected[version].name for version in pending)
        )
    for version, migration in expected.items():
        row = applied[version]
        if row["name"] != migration.name:
            raise RuntimeError(
                f"Applied migration {version:04d} name differs: "
                f"ledger={row['name']!r}, disk={migration.name!r}"
            )
        if row["sha256"] != migration.sha256:
            raise RuntimeError(
                f"Applied migration {version:04d} hash differs from "
                f"{migration.name}; migrations are append-only"
            )
    _require_migration_runtime(connection, iter(sorted(applied)))
    return tuple(migration.name for migration in manifest)


def migrate(connection: sqlite3.Connection) -> list[str]:
    manifest = _migration_manifest()
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
            applied_at TEXT NOT NULL
        )
        """
    )
    applied = {
        row["version"]: row
        for row in connection.execute(
            "SELECT version, name, sha256 FROM schema_migrations ORDER BY version"
        )
    }
    applied_now: list[str] = []
    manifest_by_version = {migration.version: migration for migration in manifest}
    missing_files = sorted(set(applied) - set(manifest_by_version))
    if missing_files:
        raise RuntimeError(
            "Applied migrations are missing from disk: "
            + ", ".join(f"{version:04d}" for version in missing_files)
        )
    if applied and sorted(applied) != list(range(1, max(applied) + 1)):
        raise RuntimeError("Applied migration ledger is not a contiguous prefix")
    _require_migration_runtime(connection, iter(sorted(applied)))

    for migration in manifest:
        version = migration.version
        existing = applied.get(version)
        if existing:
            if (
                existing["name"] != migration.name
                or existing["sha256"] != migration.sha256
            ):
                raise RuntimeError(
                    f"Applied migration {version} differs from {migration.name}; "
                    "migrations are append-only"
                )
            continue

        _require_migration_runtime(connection, iter((version,)))

        sql = migration.sql_bytes.decode("utf-8")
        applied_at = utc_now()
        # sqlite3.executescript() commits any already-open transaction before it
        # starts. Include the migration and its ledger row in one explicit script so
        # a failure at either point rolls back both the schema and the checksum record.
        # SQLite's quote() produces safe literals without attempting to parse trusted
        # migration SQL into individual statements.
        literals = connection.execute(
            "SELECT quote(?), quote(?), quote(?), quote(?)",
            (version, migration.name, migration.sha256, applied_at),
        ).fetchone()
        ledger = (
            "INSERT INTO schema_migrations(version, name, sha256, applied_at) "
            f"VALUES({literals[0]}, {literals[1]}, {literals[2]}, {literals[3]});"
        )
        try:
            connection.executescript(f"BEGIN IMMEDIATE;\n{sql}\n{ledger}\nCOMMIT;")
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        applied_now.append(migration.name)
    return applied_now


def scalar(connection: sqlite3.Connection, query: str, parameters: tuple = ()):
    row = connection.execute(query, parameters).fetchone()
    return row[0] if row else None
