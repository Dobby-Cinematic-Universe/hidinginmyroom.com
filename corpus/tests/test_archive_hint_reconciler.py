from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

import himr_corpus.archive_hint_reconciler as reconciler  # noqa: E402
from himr_corpus.archive_hint_reconciler import (  # noqa: E402
    ArchiveHintReconciliationError,
    build_archive_hint_reconciliation_plan,
    import_archive_hint_reconciliation,
    summarize_archive_hint_reconciliation_plan,
)
from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import (  # noqa: E402
    connect,
    connect_audit_readonly,
    migrate,
    transaction,
)
from himr_corpus.ids import source_id, stable_id  # noqa: E402
from himr_corpus.importers import (  # noqa: E402
    _add_source_snapshot,
    _attach_recording_source,
    _begin_batch,
    _complete_batch,
    _upsert_recording,
    _upsert_source,
    canonical_json,
    import_archive_url_hints,
    sha256_bytes,
    title_from_media_filename,
)
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


HINT_OBSERVED_AT = "2026-08-26T18:29:44Z"
ARCHIVE_OBSERVED_AT = "2026-08-26T22:57:32Z"
POST_ID = "1q2sk8g"
EXISTING_URL = "https://archive.org/download/699992/existing.mp4"
LATE_URL = "https://archive.org/download/hidinginmyroom/late.mp4"
LATE_NATIVE_ID = "hidinginmyroom/late.mp4"
LATE_FILENAME = "late.mp4"
FAKE_SNAPSHOT_ID = "iams_" + "a" * 32
FAKE_SNAPSHOT_SHA = "b" * 64
FAKE_REQUEST_ID = "iamr_" + "c" * 32
PREIMPORT_CATALOG_NAME = "corpus-v8.pre-0019-70a6f287e86d.sqlite3"
PREIMPORT_CATALOG_SHA256 = (
    "70a6f287e86d77892771a83f89eec27b6f8081915737ffc901debaae2f9864a0"
)


class ArchiveHintReconcilerTests(unittest.TestCase):
    def setUp(self) -> None:
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="archive-hint-reconciler-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        self.original_path = self.root / "archive-urls.txt"
        self.normalized_path = self.root / "archive-urls.utf8.txt"
        self.discovery_path = self.root / "discovery.json"
        self.snapshot_path = self.root / "snapshot.json"
        self.provider_payload_path = self.root / "item-hidinginmyroom.metadata.json"

        normalized = f"{EXISTING_URL}\n{LATE_URL}\n".encode("utf-8")
        original = f"{EXISTING_URL}\r\n{LATE_URL}\r\n".encode("utf-16")
        self.original_path.write_bytes(original)
        self.normalized_path.write_bytes(normalized)
        self.snapshot_path.write_text("{}\n", encoding="utf-8")
        self.provider_payload_path.write_text("{}\n", encoding="utf-8")
        self.original_sha = sha256_bytes(original)
        self.normalized_sha = sha256_bytes(normalized)

        discovery = {
            "schema_version": 1,
            "discovered_at": HINT_OBSERVED_AT,
            "discovery_source": {
                "platform": "reddit",
                "subreddit": "HIMRFAM",
                "post_id": POST_ID,
                "url": f"https://www.reddit.com/r/HIMRFAM/comments/{POST_ID}/fixture/",
                "title": "Fixture",
                "published_at": "2026-01-03T11:38:40.713Z",
            },
            "archive_item": {
                "identifier": "699992",
                "url": "https://archive.org/details/699992",
                "title": "Fixture",
                "metadata_path": "fixture.json",
                "metadata_sha256": "d" * 64,
                "file_count": 1,
                "video_extension_file_count": 1,
                "video_original_count": 1,
                "video_declared_bytes": 1,
                "video_declared_seconds": 1.0,
            },
            "complement_list": {
                "retrieval_url": reconciler.CATBOX_RETRIEVAL_URL,
                "retained_original_path": "archive-urls.txt",
                "retained_original_sha256": self.original_sha,
                "encoding": "UTF-16LE with BOM",
                "normalized_utf8_path": "archive-urls.utf8.txt",
                "normalized_utf8_sha256": self.normalized_sha,
                "nonempty_url_count": 2,
                "distinct_url_count": 2,
            },
            "interpretation_warning": "Fixture contributor complement only.",
        }
        discovery_body = json.dumps(discovery, indent=2).encode("utf-8")
        self.discovery_path.write_bytes(discovery_body)
        self.discovery_sha = sha256_bytes(discovery_body)

        self._seed_catalog()
        self.archive_evidence = self._fake_archive_evidence()
        self.patches = ExitStack()
        self.patches.enter_context(
            patch.object(reconciler, "ORIGINAL_HINTS_SHA256", self.original_sha)
        )
        self.patches.enter_context(
            patch.object(reconciler, "NORMALIZED_HINTS_SHA256", self.normalized_sha)
        )
        self.patches.enter_context(
            patch.object(reconciler, "DISCOVERY_SHA256", self.discovery_sha)
        )
        self.patches.enter_context(
            patch.object(reconciler, "ARCHIVE_SNAPSHOT_ID", FAKE_SNAPSHOT_ID)
        )
        self.patches.enter_context(
            patch.object(reconciler, "ARCHIVE_SNAPSHOT_SHA256", FAKE_SNAPSHOT_SHA)
        )
        self.patches.enter_context(
            patch.object(reconciler, "ARCHIVE_REQUEST_ID", FAKE_REQUEST_ID)
        )
        self.patches.enter_context(
            patch.object(reconciler, "EXPECTED_RETAINED_URLS", 2)
        )
        self.patches.enter_context(
            patch.object(reconciler, "EXPECTED_ALREADY_PROVIDER", 1)
        )
        self.patches.enter_context(
            patch.object(reconciler, "EXPECTED_LATE_HINTS", 1)
        )
        self.patches.enter_context(
            patch.object(reconciler, "LATE_ARCHIVE_ITEMS", ("hidinginmyroom",))
        )
        self.patches.enter_context(
            patch.object(
                reconciler,
                "EXPECTED_URLS_BY_ITEM",
                {"699992": 1, "hidinginmyroom": 1},
            )
        )
        self.patches.enter_context(
            patch.object(
                reconciler,
                "_archive_snapshot_evidence",
                return_value=self.archive_evidence,
            )
        )
        self.patches.enter_context(
            patch.object(
                reconciler,
                "_archive_catalog_binding",
                return_value={
                    "archive_import_batch_id": self.archive_batch_id,
                    "archive_importer_version": "0.2.0",
                    "archive_import_input_sha256": "2" * 64,
                    "capture_rows": [],
                },
            )
        )

    def tearDown(self) -> None:
        self.patches.close()
        self.connection.close()
        self.temporary.cleanup()

    @property
    def plan_args(self) -> tuple[Path, Path, Path, Path]:
        return (
            self.original_path,
            self.normalized_path,
            self.discovery_path,
            self.snapshot_path,
        )

    def _seed_provider(
        self,
        *,
        batch_id: str,
        observed_at: str,
        item: str,
        filename: str,
        with_full_evidence: bool,
    ) -> str:
        item_source = source_id("internet_archive", "archive_item", item)
        _upsert_source(
            self.connection,
            source=item_source,
            platform="internet_archive",
            source_kind="archive_item",
            native_id=item,
            canonical_url=f"https://archive.org/details/{item}",
            observed_at=observed_at,
            batch_id=batch_id,
            access_state="public",
        )
        native_id = f"{item}/{filename}"
        url = f"https://archive.org/download/{item}/{filename}"
        provider = source_id("internet_archive", "archive_media_file", native_id)
        metadata = {
            "internet_archive_item": item,
            "filename": filename,
            "format": "MPEG4",
            "source_class": "original",
            "derivative_of": None,
            "byte_count": 100,
            "duration_ms": 2000,
        }
        _upsert_source(
            self.connection,
            source=provider,
            platform="internet_archive",
            source_kind="archive_media_file",
            native_id=native_id,
            parent_source=item_source,
            canonical_url=url,
            title=title_from_media_filename(filename),
            observed_at=observed_at,
            batch_id=batch_id,
            access_state="public",
            review_state="metadata_only",
            metadata=metadata,
        )
        if not with_full_evidence:
            return provider
        _add_source_snapshot(
            self.connection,
            source=provider,
            observed_at=observed_at,
            payload=metadata,
            batch_id=batch_id,
            request_url=f"https://archive.org/metadata/{item}",
            artifact_path=str(self.provider_payload_path),
        )
        for algorithm, digest in (
            ("crc32", "1" * 8),
            ("md5", "2" * 32),
            ("sha1", "3" * 40),
        ):
            self.connection.execute(
                """
                INSERT INTO source_hashes(
                    source_id, algorithm, digest, declared_by, observed_at
                ) VALUES(?, ?, ?, 'internet_archive_metadata', ?)
                """,
                (provider, algorithm, digest, observed_at),
            )
        recording = _upsert_recording(
            self.connection,
            canonical_key=f"archive:{item}:{filename}",
            title=title_from_media_filename(filename),
            date_label=None,
            date_basis="internet_archive_filename",
            duration=2000,
            recording_type="video",
            observed_at=observed_at,
            batch_id=batch_id,
            metadata={"identity_basis": "fixture"},
        )
        _attach_recording_source(
            self.connection,
            recording=recording,
            source=provider,
            role="archive_original_file",
            method="archive_filename_platform_id_grouping",
            confidence_state="metadata_only",
            metadata={"source_class": "original"},
        )
        return provider

    def _seed_catalog(self) -> None:
        with transaction(self.connection):
            early_batch, _ = _begin_batch(
                self.connection,
                "internet_archive_metadata",
                "1" * 64,
                "2026-08-26",
                "2026-08-26T18:00:00Z",
            )
            self._seed_provider(
                batch_id=early_batch,
                observed_at="2026-08-26T18:00:00Z",
                item="699992",
                filename="existing.mp4",
                with_full_evidence=False,
            )
            _complete_batch(
                self.connection, early_batch, "2026-08-26T18:00:00Z", {"video_files": 1}
            )
        result = import_archive_url_hints(
            self.connection,
            self.normalized_path,
            observed_at=HINT_OBSERVED_AT,
            reddit_post_id=POST_ID,
        )
        self.assertEqual(
            result,
            {"new_hint_sources": 1, "resolved_existing_sources": 1, "valid_hints": 2},
        )
        with transaction(self.connection):
            self.archive_batch_id, _ = _begin_batch(
                self.connection,
                "internet_archive_metadata",
                "2" * 64,
                "2026-08-26",
                ARCHIVE_OBSERVED_AT,
            )
            for index, item in enumerate(reconciler.LATE_ARCHIVE_ITEMS):
                item_source = source_id("internet_archive", "archive_item", item)
                _upsert_source(
                    self.connection,
                    source=item_source,
                    platform="internet_archive",
                    source_kind="archive_item",
                    native_id=item,
                    canonical_url=f"https://archive.org/details/{item}",
                    observed_at=ARCHIVE_OBSERVED_AT,
                    batch_id=self.archive_batch_id,
                    access_state="public",
                )
                payload_sha = hashlib.sha256(f"capture-{item}".encode()).hexdigest()
                capture_id = stable_id(
                    "ssn", item_source, ARCHIVE_OBSERVED_AT, payload_sha
                )
                self.connection.execute(
                    """
                    INSERT INTO source_snapshots(
                        source_snapshot_id, source_id, observed_at, request_url,
                        final_url, http_status, payload_sha256, artifact_path,
                        metadata_json, import_batch_id
                    ) VALUES(?, ?, ?, ?, ?, 200, ?, ?, ?, ?)
                    """,
                    (
                        capture_id,
                        item_source,
                        ARCHIVE_OBSERVED_AT,
                        f"https://archive.org/metadata/{item}",
                        f"https://archive.org/metadata/{item}",
                        payload_sha,
                        str(self.provider_payload_path),
                        canonical_json(
                            {
                                "archive_metadata_snapshot_id": FAKE_SNAPSHOT_ID,
                                "archive_metadata_snapshot_sha256": FAKE_SNAPSHOT_SHA,
                                "request_id": FAKE_REQUEST_ID,
                                "provider_fields_are_content_truth": False,
                                "publication_authority": False,
                            }
                        ),
                        self.archive_batch_id,
                    ),
                )
            self.late_provider = self._seed_provider(
                batch_id=self.archive_batch_id,
                observed_at=ARCHIVE_OBSERVED_AT,
                item="hidinginmyroom",
                filename=LATE_FILENAME,
                with_full_evidence=True,
            )
            _complete_batch(
                self.connection,
                self.archive_batch_id,
                ARCHIVE_OBSERVED_AT,
                {"video_files": 1},
            )

    def _fake_archive_evidence(self) -> dict:
        items = {}
        for item in reconciler.LATE_ARCHIVE_ITEMS:
            item_source = source_id("internet_archive", "archive_item", item)
            payload_sha = hashlib.sha256(f"capture-{item}".encode()).hexdigest()
            items[item] = {
                "archive_item_source_id": item_source,
                "capture_snapshot_id": stable_id(
                    "ssn", item_source, ARCHIVE_OBSERVED_AT, payload_sha
                ),
                "payload_sha256": payload_sha,
                "payload_filename": f"item-{item}.metadata.json",
                "observed_at": ARCHIVE_OBSERVED_AT,
            }
        return {
            "snapshot": {
                "snapshot_id": FAKE_SNAPSHOT_ID,
                "_sha256": FAKE_SNAPSHOT_SHA,
                "request": {"request_id": FAKE_REQUEST_ID},
                "observed_at": ARCHIVE_OBSERVED_AT,
                "_path": self.snapshot_path,
            },
            "records": {
                LATE_NATIVE_ID: {
                    "name": LATE_FILENAME,
                    "source": "original",
                    "format": "MPEG4",
                    "size": "100",
                    "length": "2",
                    "crc32": "1" * 8,
                    "md5": "2" * 32,
                    "sha1": "3" * 40,
                }
            },
            "items": items,
        }

    def _counts(self) -> dict[str, int]:
        tables = (
            "sources",
            "source_metadata_observations",
            "source_relations",
            "source_relation_observations",
            "external_ids",
            "external_id_observations",
            "recording_sources",
            "review_decisions",
            "publication_decisions",
            "public_sources",
        )
        return {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in tables
        }

    def test_plan_is_deterministic_read_only_and_candidate_only(self) -> None:
        before = self._counts()
        changes = self.connection.total_changes
        first = build_archive_hint_reconciliation_plan(self.connection, *self.plan_args)
        second = build_archive_hint_reconciliation_plan(self.connection, *self.plan_args)
        self.assertEqual(first, second)
        self.assertEqual(self.connection.total_changes, changes)
        self.assertEqual(self._counts(), before)
        self.assertEqual(first["statistics"]["retained_urls_total"], 2)
        self.assertEqual(first["statistics"]["late_exact_provider_candidates"], 1)
        candidate = first["candidates"][0]
        self.assertEqual(candidate["typed_evidence"]["hint_source_id"], source_id(
            "internet_archive", "archive_url_discovery_hint", LATE_NATIVE_ID
        ))
        self.assertEqual(candidate["typed_evidence"]["provider_source_id"], self.late_provider)
        self.assertFalse(candidate["typed_evidence"]["relationship_asserted"])
        self.assertFalse(candidate["typed_evidence"]["external_id_copied"])
        summary = summarize_archive_hint_reconciliation_plan(first)
        self.assertNotIn("candidates", summary)
        self.assertNotIn(LATE_URL, json.dumps(summary))

    def test_adversarial_input_and_catalog_drift_fail_without_writes(self) -> None:
        before = self._counts()
        self.normalized_path.write_bytes(self.normalized_path.read_bytes() + b"\n")
        with self.assertRaisesRegex(ArchiveHintReconciliationError, "fixed 1q2sk8g"):
            build_archive_hint_reconciliation_plan(self.connection, *self.plan_args)
        self.normalized_path.write_text(f"{EXISTING_URL}\n{LATE_URL}\n", encoding="utf-8")
        self.assertEqual(self._counts(), before)

        self.connection.execute(
            "UPDATE source_hashes SET digest = ? WHERE source_id = ? AND algorithm = 'md5'",
            ("f" * 32, self.late_provider),
        )
        with self.assertRaisesRegex(ArchiveHintReconciliationError, "provider hashes differ"):
            build_archive_hint_reconciliation_plan(self.connection, *self.plan_args)
        self.connection.execute(
            "UPDATE source_hashes SET digest = ? WHERE source_id = ? AND algorithm = 'md5'",
            ("2" * 32, self.late_provider),
        )

        hint_source = source_id(
            "internet_archive", "archive_url_discovery_hint", LATE_NATIVE_ID
        )
        relation_id = stable_id("sre", hint_source, "unsafe_alias", self.late_provider)
        self.connection.execute(
            """
            INSERT INTO source_relations(
                source_relation_id, from_source_id, relation_kind, to_source_id,
                basis, confidence_state, metadata_json
            ) VALUES(?, ?, 'unsafe_alias', ?, 'fixture tamper', 'candidate', '{}')
            """,
            (relation_id, hint_source, self.late_provider),
        )
        with self.assertRaisesRegex(ArchiveHintReconciliationError, "already have relation"):
            build_archive_hint_reconciliation_plan(self.connection, *self.plan_args)

    def test_non_audited_fixture_cannot_cross_sealed_import_boundary(self) -> None:
        plan = build_archive_hint_reconciliation_plan(self.connection, *self.plan_args)
        before = self._counts()
        with self.assertRaisesRegex(ArchiveHintReconciliationError, "expected plan"):
            import_archive_hint_reconciliation(
                self.connection,
                *self.plan_args,
                expected_plan_sha256="0" * 64,
            )
        self.assertEqual(self._counts(), before)
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "CHECK constraint failed|lacks exact Archive captures",
        ):
            import_archive_hint_reconciliation(
                self.connection,
                *self.plan_args,
                expected_plan_sha256=plan["plan_sha256"],
            )
        self.assertEqual(self._counts(), before)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM archive_hint_provider_candidates"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM review_tasks WHERE task_kind = ?",
                (reconciler.TASK_KIND,),
            ).fetchone()[0],
            0,
        )

    def test_cli_exposes_plan_and_digest_gated_import(self) -> None:
        parser = build_parser()
        choices = parser._subparsers._group_actions[0].choices
        self.assertIn("plan-archive-hint-reconciliation", choices)
        self.assertIn("import-archive-hint-reconciliation", choices)
        with self.assertRaises(SystemExit):
            parser.parse_args(
                [
                    "import-archive-hint-reconciliation",
                    "--db",
                    "fixture.sqlite3",
                    "--original-hints",
                    "original.txt",
                    "--normalized-hints",
                    "normalized.txt",
                    "--discovery-metadata",
                    "discovery.json",
                    "--archive-snapshot",
                    "snapshot.json",
                ]
            )


class ArchiveHintPrivateAuditIntegrationTests(unittest.TestCase):
    def _private_evidence_paths(
        self,
    ) -> tuple[Path, tuple[Path, Path, Path, Path]]:
        """Select the sealed, sidecar-free catalog that predates the import.

        The live private catalog is deliberately not an input to these canaries.  It
        can legitimately be open in WAL mode while another private pipeline is
        running, and reading its main file with ``immutable=1`` would silently omit
        WAL state.  The reviewed pre-0019 backup is the reproducible audit boundary:
        it has a fixed digest, contains all provider evidence needed by the plan, and
        predates the reconciliation under test.

        A checkout with none of the private evidence is the expected public-CI case
        and is classified as unavailable.  A partial private evidence set is an
        error, not a skip, so missing or damaged audit material cannot hide a failed
        integration canary.
        """

        database = (
            REPOSITORY_ROOT
            / "research/corpus/private-catalog-backups"
            / PREIMPORT_CATALOG_NAME
        )
        live_database = REPOSITORY_ROOT / "research/corpus/corpus-v8.sqlite3"
        discovery = REPOSITORY_ROOT / "research/corpus/discovery/reddit/1q2sk8g"
        snapshot = (
            REPOSITORY_ROOT
            / "research/corpus/archive-org-metadata/snapshots"
            / "iams_4f920fcb565f826b8352b450247d18d3/snapshot.json"
        )
        evidence = (
            discovery / "archive-urls.txt",
            discovery / "archive-urls.utf8.txt",
            discovery / "discovery.json",
            snapshot,
        )
        required = (database, *evidence)
        present = tuple(path.exists() or path.is_symlink() for path in required)
        private_catalog_present = live_database.exists() or live_database.is_symlink()
        if not any(present) and not private_catalog_present:
            self.skipTest("private Archive hint audit evidence is unavailable")
        missing = [str(path) for path, exists in zip(required, present) if not exists]
        if missing:
            self.fail(
                "private Archive hint audit evidence is incomplete; missing: "
                + ", ".join(missing)
            )
        invalid = [
            str(path)
            for path in required
            if path.is_symlink() or not path.is_file()
        ]
        if invalid:
            self.fail(
                "private Archive hint audit evidence must be regular non-symlink files: "
                + ", ".join(invalid)
            )
        with database.open("rb") as stream:
            observed_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
        if observed_sha256 != PREIMPORT_CATALOG_SHA256:
            self.fail(
                "private pre-0019 Archive-hint catalog backup hash differs: "
                f"expected {PREIMPORT_CATALOG_SHA256}, observed {observed_sha256}"
            )
        return database, evidence

    def _disposable_catalog(
        self,
    ) -> tuple[sqlite3.Connection, tuple[Path, Path, Path, Path]]:
        source_database, paths = self._private_evidence_paths()
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(
            prefix="archive-hint-private-canary-", dir=work
        )
        self.addCleanup(temporary.cleanup)
        source_before = source_database.stat()
        source = connect_audit_readonly(source_database)
        destination = connect(Path(temporary.name) / "catalog.sqlite3")
        try:
            source.backup(destination)
        finally:
            source.close()
        source_after = source_database.stat()
        self.assertEqual(
            (source_before.st_size, source_before.st_mtime_ns),
            (source_after.st_size, source_after.st_mtime_ns),
        )
        migrate(destination)
        self.addCleanup(destination.close)
        return destination, paths

    @staticmethod
    def _protected_counts(connection: sqlite3.Connection) -> dict[str, int]:
        return reconciler._protected_counts(connection)

    def test_private_audit_plan_reproduces_221_without_catalog_writes(self) -> None:
        database, paths = self._private_evidence_paths()
        before = database.stat()
        connection = connect_audit_readonly(database)
        try:
            changes = connection.total_changes
            plan = build_archive_hint_reconciliation_plan(
                connection,
                *paths,
            )
            self.assertEqual(connection.total_changes, changes)
        finally:
            connection.close()
        after = database.stat()
        self.assertEqual(
            (before.st_size, before.st_mtime_ns), (after.st_size, after.st_mtime_ns)
        )
        self.assertEqual(plan["statistics"]["late_exact_provider_candidates"], 221)
        self.assertEqual(
            plan["statistics"]["candidates_by_archive_item"],
            {"hidinginmyroom": 79, "hidinginmyroom2": 52, "hidinginmyroom3": 90},
        )
        self.assertFalse(plan["policy"]["catbox_response_envelope_captured"])
        self.assertFalse(plan["policy"]["direct_post_body_catbox_link_asserted"])

    def test_disposable_import_is_sealed_adversarial_and_exact_replay(self) -> None:
        connection, paths = self._disposable_catalog()
        plan = build_archive_hint_reconciliation_plan(connection, *paths)
        before = self._protected_counts(connection)

        receipt_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'archive_hint_reconciliation_imports'"
        ).fetchone()[0]
        for literal in (
            reconciler.ORIGINAL_HINTS_SHA256,
            reconciler.NORMALIZED_HINTS_SHA256,
            reconciler.DISCOVERY_SHA256,
            reconciler.ARCHIVE_SNAPSHOT_ID,
            reconciler.ARCHIVE_SNAPSHOT_SHA256,
            reconciler.ARCHIVE_REQUEST_ID,
            "retained_url_count = 731",
            "already_provider_count = 510",
            "candidate_count = 221",
            "review_task_count = 221",
        ):
            self.assertIn(literal, receipt_sql)
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'archive_hint_provider_candidates_admission'"
        ).fetchone()[0]
        self.assertIn("json_each(NEW.evidence_json)) <> 33", trigger_sql)
        self.assertIn("'$.provider_format') <> NEW.provider_format", trigger_sql)
        self.assertIn("'$.archive_snapshot_id'", trigger_sql)
        self.assertIn("'$.original_hints_sha256'", trigger_sql)

        def serializer_with(**changes: object):
            def serialize(value: object) -> str:
                if (
                    isinstance(value, dict)
                    and value.get("schema_version") == 1
                    and value.get("candidate_state") == "candidate_only_unreviewed"
                    and "archive_snapshot_id" in value
                ):
                    value = {**value, **changes}
                return canonical_json(value)

            return serialize

        for changes in (
            {"unexpected_key": True},
            {"provider_format": "tampered-format"},
            {"archive_snapshot_id": "iams_" + "0" * 32},
            {"original_hints_sha256": "0" * 64},
        ):
            with patch.object(
                reconciler, "canonical_json", side_effect=serializer_with(**changes)
            ):
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "candidate evidence differs"
                ):
                    import_archive_hint_reconciliation(
                        connection,
                        *paths,
                        expected_plan_sha256=plan["plan_sha256"],
                    )
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM archive_hint_reconciliation_imports"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(before, self._protected_counts(connection))

        connection.execute(
            """
            CREATE TRIGGER malicious_archive_hint_fixture
            AFTER INSERT ON match_candidates
            BEGIN
                UPDATE sources SET title = title WHERE source_id = NEW.left_object_id;
            END
            """
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "protected-table write"):
            import_archive_hint_reconciliation(
                connection,
                *paths,
                expected_plan_sha256=plan["plan_sha256"],
            )
        connection.execute("DROP TRIGGER malicious_archive_hint_fixture")
        self.assertEqual(before, self._protected_counts(connection))

        first = import_archive_hint_reconciliation(
            connection,
            *paths,
            expected_plan_sha256=plan["plan_sha256"],
        )
        after = self._protected_counts(connection)
        second = import_archive_hint_reconciliation(
            connection,
            *paths,
            expected_plan_sha256=plan["plan_sha256"],
        )
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(before, after)
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM archive_hint_provider_candidates"
            ).fetchone()[0],
            221,
        )

        expected_evidence_keys = {
            "schema_version",
            "archive_snapshot_id",
            "archive_snapshot_sha256",
            "original_hints_sha256",
            "normalized_hints_sha256",
            "archive_item",
            "archive_filename",
            "archive_native_id",
            "archive_url",
            "hint_metadata_observation_id",
            "provider_metadata_observation_id",
            "provider_projection_snapshot_id",
            "provider_projection_sha256",
            "archive_item_capture_snapshot_id",
            "reference_relation_observation_id",
            "external_id_observation_id",
            "provider_recording_source_id",
            "provider_recording_id",
            "provider_format",
            "provider_declared_byte_count",
            "provider_declared_duration_ms",
            "provider_declared_crc32",
            "provider_declared_md5",
            "provider_declared_sha1",
            "match_basis",
            "candidate_state",
            "requires_human_review",
            "relationship_asserted",
            "source_merge_performed",
            "external_id_copied",
            "provider_fields_are_content_truth",
            "payload_downloaded_or_read",
            "publication_authority",
        }
        typed_rows = connection.execute(
            """
            SELECT evidence_json, provider_format, provider_source_id,
                   provider_metadata_observation_id, provider_projection_snapshot_id,
                   archive_item_capture_snapshot_id, provider_recording_source_id,
                   archive_url
            FROM archive_hint_provider_candidates
            ORDER BY match_candidate_id
            """
        ).fetchall()
        self.assertEqual(len(typed_rows), 221)
        for row in typed_rows:
            evidence = json.loads(row["evidence_json"])
            self.assertEqual(set(evidence), expected_evidence_keys)
            self.assertEqual(evidence["provider_format"], row["provider_format"])
            self.assertEqual(
                evidence["archive_snapshot_id"], reconciler.ARCHIVE_SNAPSHOT_ID
            )
            self.assertEqual(
                evidence["archive_snapshot_sha256"],
                reconciler.ARCHIVE_SNAPSHOT_SHA256,
            )
            self.assertEqual(
                evidence["original_hints_sha256"], reconciler.ORIGINAL_HINTS_SHA256
            )
            self.assertEqual(
                evidence["normalized_hints_sha256"],
                reconciler.NORMALIZED_HINTS_SHA256,
            )

        candidate_id = plan["candidates"][0]["generic"]["match_candidate_id"]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "UPDATE archive_hint_provider_candidates SET candidate_state = candidate_state "
                "WHERE match_candidate_id = ?",
                (candidate_id,),
            )
        reviewer = stable_id("rev", "archive-hint-private-canary")
        register_reviewer_fixture(
            connection, reviewer, "Private canary", "human"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "no publication authority"):
            connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES(?, 'match_candidate', ?, 'publish', ?, ?, 'unsafe canary')
                """,
                (
                    stable_id("pub", candidate_id),
                    candidate_id,
                    reviewer,
                    "2026-08-27T00:00:00Z",
                ),
            )

        first_typed = typed_rows[0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "snapshots are append-only"):
            connection.execute(
                "UPDATE source_snapshots SET payload_sha256 = payload_sha256 "
                "WHERE source_snapshot_id = ?",
                (first_typed["archive_item_capture_snapshot_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "hashes are append-only"):
            connection.execute(
                "UPDATE source_hashes SET digest = digest WHERE source_id = ?",
                (first_typed["provider_source_id"],),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "recording projection is append-only"
        ):
            connection.execute(
                "UPDATE recording_sources SET mapping_role = mapping_role "
                "WHERE recording_source_id = ?",
                (first_typed["provider_recording_source_id"],),
            )

        # Source rows are mutable current projections. The exact historical
        # observation and typed evidence remain the immutable candidate anchor.
        row = typed_rows[0]
        changed_url = row["archive_url"] + "?later-projection=1"
        connection.execute(
            "UPDATE sources SET canonical_url = ? WHERE source_id = ?",
            (changed_url, row["provider_source_id"]),
        )
        self.assertEqual(
            connection.execute(
                "SELECT canonical_url FROM sources WHERE source_id = ?",
                (row["provider_source_id"],),
            ).fetchone()[0],
            changed_url,
        )
        self.assertEqual(
            connection.execute(
                "SELECT canonical_url FROM source_metadata_observations "
                "WHERE source_metadata_observation_id = ?",
                (row["provider_metadata_observation_id"],),
            ).fetchone()[0],
            row["archive_url"],
        )
        self.assertEqual(
            connection.execute(
                "SELECT archive_url FROM archive_hint_provider_candidates "
                "WHERE match_candidate_id = ?",
                (candidate_id,),
            ).fetchone()[0],
            plan["candidates"][0]["typed_evidence"]["archive_url"],
        )


if __name__ == "__main__":
    unittest.main()
