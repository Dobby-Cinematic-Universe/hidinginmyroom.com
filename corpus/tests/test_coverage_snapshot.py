from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from himr_corpus.cli import main as corpus_main
from himr_corpus.coverage_snapshot import (
    CoverageSnapshotError,
    canonical_bytes,
    compact_snapshot_summary,
    snapshot_catalog,
)
from himr_corpus.db import connect, migrate


NOW = "2026-08-28T12:00:00Z"


class CoverageSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "sealed.sqlite3"
        connection = connect(self.database)
        migrate(connection)
        connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, title, observed_at,
                access_state, review_state, metadata_json, created_at, updated_at
            ) VALUES(
                'src_fixture', 'youtube', 'youtube_video', 'AAAAAAAAAAA',
                'SECRET SOURCE TITLE', ?, 'public', 'metadata_only', '{}', ?, ?
            )
            """,
            (NOW, NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(
                'rec_fixture', 'fixture:secret-native-key', 'secret-recording-slug',
                'SECRET RECORDING TITLE', 'unknown', 'video', 'metadata_only', '{}', ?, ?
            )
            """,
            (NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(
                'rs_fixture', 'rec_fixture', 'src_fixture', 'primary',
                'fixture', 'metadata_only', '{}'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(
                'media_fixture', ?, 1234, 'video', 'video/mp4', 'mp4', 5000, ?, 'verified'
            )
            """,
            ("a" * 64, NOW),
        )
        connection.execute(
            """
            INSERT INTO media_sources(
                media_source_id, media_id, source_id, retrieved_at, retrieval_tool
            ) VALUES('ms_fixture', 'media_fixture', 'src_fixture', ?, 'fixture')
            """,
            (NOW,),
        )
        connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(
                'rend_fixture', 'rec_fixture', 'media_fixture', 'source',
                'SECRET RENDITION LABEL', 'unreviewed', '{}'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, rendition_id, revision_kind,
                origin, language, review_state, created_at, metadata_json
            ) VALUES(
                'rev_fixture', 'rec_fixture', 'rend_fixture', 'raw_asr',
                'fixture', 'en', 'machine', ?, '{}'
            )
            """,
            (NOW,),
        )
        connection.execute(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version, parameters_json,
                environment_json, started_at, completed_at, status
            ) VALUES(
                'run_fixture', 'media_acquisition', '1.0.0', '{}', '{}', ?, ?, 'completed'
            )
            """,
            (NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO jobs(
                job_id, stage, target_type, target_id, priority, state,
                max_attempts, created_at, updated_at
            ) VALUES(
                'job_fixture', 'media_acquisition', 'recording', 'rec_fixture',
                100, 'completed', 1, ?, ?
            )
            """,
            (NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO review_tasks(
                review_task_id, task_kind, target_type, target_id, reason,
                priority, status, created_at, updated_at
            ) VALUES(
                'task_fixture', 'source_recovery', 'recording', 'rec_fixture',
                'SECRET REVIEW REASON', 100, 'open', ?, ?
            )
            """,
            (NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method,
                decision_state, metadata_json
            ) VALUES(
                'match_fixture', 'recording', 'rec_fixture', 'source',
                'src_fixture', 'archive_bracketed_youtube_locator_v1', 'candidate', '{}'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO entities(
                entity_id, entity_type, canonical_label, slug, visibility,
                review_state, metadata_json, created_at
            ) VALUES(
                'ent_fixture', 'person', 'SECRET PERSON LABEL', 'secret-person',
                'private', 'unreviewed', '{}', ?
            )
            """,
            (NOW,),
        )
        connection.execute(
            """
            INSERT INTO events(
                event_id, canonical_label, slug, event_kind, description,
                visibility, review_state, metadata_json, created_at
            ) VALUES(
                'evt_fixture', 'SECRET EVENT LABEL', 'secret-event', 'fixture',
                'SECRET EVENT DESCRIPTION', 'private', 'unreviewed', '{}', ?
            )
            """,
            (NOW,),
        )
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.close()
        os.chmod(self.database, 0o400)
        self.digest = hashlib.sha256(self.database.read_bytes()).hexdigest()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_snapshot_is_deterministic_aggregate_only_and_text_free(self) -> None:
        before = self.database.stat()
        before_sidecars = {
            path.name for path in self.root.iterdir() if path.name != self.database.name
        }
        first = snapshot_catalog(self.database, expected_sha256=self.digest)
        second = snapshot_catalog(self.database, expected_sha256=self.digest)
        after = self.database.stat()
        after_sidecars = {
            path.name for path in self.root.iterdir() if path.name != self.database.name
        }
        self.assertEqual(
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_mode),
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_mode),
        )
        self.assertEqual(before_sidecars, after_sidecars)
        self.assertEqual(first, second)
        self.assertEqual(first["snapshot_sha256"], hashlib.sha256(canonical_bytes({
            key: value
            for key, value in first.items()
            if key not in {"snapshot_id", "snapshot_sha256"}
        })).hexdigest())
        self.assertEqual(first["counts"]["sources"], 1)
        self.assertEqual(first["counts"]["recordings"], 1)
        self.assertEqual(first["counts"]["media_objects"], 1)
        self.assertEqual(first["coverage"]["recordings_with_any_source"], 1)
        self.assertEqual(first["coverage"]["recordings_with_any_rendition"], 1)
        self.assertEqual(first["coverage"]["recordings_with_any_catalog_transcript"], 1)
        self.assertEqual(first["coverage"]["catalogued_media_bytes"], 1234)
        self.assertEqual(first["coverage"]["catalogued_media_duration_ms_known_sum"], 5000)
        serialized = canonical_bytes(first).decode("utf-8")
        for forbidden in (
            "SECRET SOURCE TITLE",
            "SECRET RECORDING TITLE",
            "SECRET RENDITION LABEL",
            "SECRET REVIEW REASON",
            "SECRET PERSON LABEL",
            "SECRET EVENT LABEL",
            "SECRET EVENT DESCRIPTION",
            "fixture:secret-native-key",
            "secret-recording-slug",
            str(self.database),
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertEqual(first["assertion_policy"]["publication_authority"], "none")
        self.assertFalse(first["assertion_policy"]["catalog_mutated"])

    def test_compact_summary_retains_only_progress_counts_and_hashes(self) -> None:
        snapshot = snapshot_catalog(self.database, expected_sha256=self.digest)
        compact = compact_snapshot_summary(snapshot)
        self.assertEqual(compact["recordings_total"], 1)
        self.assertEqual(compact["recordings_with_any_rendition"], 1)
        self.assertEqual(compact["recordings_with_any_catalog_transcript"], 1)
        self.assertEqual(compact["open_review_tasks"], 1)
        self.assertEqual(compact["candidate_matches"], 1)
        self.assertEqual(compact["publication_authority"], "none")

    def test_cli_prints_valid_compact_json(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            corpus_main(
                [
                    "coverage-snapshot",
                    "--db",
                    str(self.database),
                    "--expected-sha256",
                    self.digest,
                    "--compact",
                ]
            )
        value = json.loads(output.getvalue())
        self.assertEqual(value["recordings_total"], 1)
        self.assertEqual(value["catalog_sha256"], self.digest)

    def test_wrong_hash_fails_closed(self) -> None:
        with self.assertRaisesRegex(CoverageSnapshotError, "differs from the explicit pin"):
            snapshot_catalog(self.database, expected_sha256="0" * 64)

    def test_writable_catalog_fails_closed(self) -> None:
        os.chmod(self.database, 0o600)
        with self.assertRaisesRegex(CoverageSnapshotError, "exact private mode 0400"):
            snapshot_catalog(self.database, expected_sha256=self.digest)

    def test_sidecar_fails_closed(self) -> None:
        sidecar = Path(f"{self.database}-wal")
        sidecar.write_bytes(b"")
        with self.assertRaisesRegex(CoverageSnapshotError, "no SQLite sidecars"):
            snapshot_catalog(self.database, expected_sha256=self.digest)

    def test_sqlite_queries_the_exact_pinned_descriptor(self) -> None:
        real_connect = sqlite3.connect
        observed_uris: list[str] = []

        def inspect_connect(database: str, *args: object, **kwargs: object) -> sqlite3.Connection:
            self.assertRegex(database, r"^file:/proc/self/fd/[0-9]+\?mode=ro&immutable=1$")
            descriptor = int(database.split("/fd/", 1)[1].split("?", 1)[0])
            self.assertEqual(os.fstat(descriptor).st_ino, self.database.stat().st_ino)
            observed_uris.append(database)
            return real_connect(database, *args, **kwargs)

        with mock.patch(
            "himr_corpus.coverage_snapshot.sqlite3.connect",
            side_effect=inspect_connect,
        ):
            snapshot_catalog(self.database, expected_sha256=self.digest)
        self.assertEqual(len(observed_uris), 1)

    def test_sqlite_path_substitution_cannot_supply_different_inode(self) -> None:
        decoy = self.root / "decoy.sqlite3"
        decoy.write_bytes(self.database.read_bytes())
        os.chmod(decoy, 0o400)
        real_connect = sqlite3.connect

        def substitute_connect(
            _database: str, *args: object, **kwargs: object
        ) -> sqlite3.Connection:
            return real_connect(
                f"file:{decoy}?mode=ro&immutable=1",
                uri=True,
                isolation_level=None,
            )

        with mock.patch(
            "himr_corpus.coverage_snapshot.sqlite3.connect",
            side_effect=substitute_connect,
        ):
            with self.assertRaisesRegex(
                CoverageSnapshotError, "exactly one descriptor"
            ):
                snapshot_catalog(self.database, expected_sha256=self.digest)

    def test_symlinked_parent_path_fails_before_sqlite_open(self) -> None:
        real_parent = self.root / "real-parent"
        real_parent.mkdir()
        moved = real_parent / self.database.name
        self.database.rename(moved)
        alias_parent = self.root / "alias-parent"
        alias_parent.symlink_to(real_parent, target_is_directory=True)
        with mock.patch("himr_corpus.coverage_snapshot.sqlite3.connect") as connector:
            with self.assertRaisesRegex(CoverageSnapshotError, "components may not be symlinks"):
                snapshot_catalog(alias_parent / moved.name, expected_sha256=self.digest)
        connector.assert_not_called()

    def test_symlink_fails_closed(self) -> None:
        symlink = self.root / "alias.sqlite3"
        symlink.symlink_to(self.database)
        with self.assertRaisesRegex(CoverageSnapshotError, "regular non-symlink"):
            snapshot_catalog(symlink, expected_sha256=self.digest)

    def test_snake_case_group_label_cannot_leak_through_aggregate_output(self) -> None:
        os.chmod(self.database, 0o600)
        connection = sqlite3.connect(self.database)
        connection.execute(
            "UPDATE processing_runs SET stage = 'secret_person_label' "
            "WHERE processing_run_id = 'run_fixture'"
        )
        connection.commit()
        connection.close()
        os.chmod(self.database, 0o400)
        digest = hashlib.sha256(self.database.read_bytes()).hexdigest()
        with self.assertRaises(CoverageSnapshotError) as raised:
            snapshot_catalog(self.database, expected_sha256=digest)
        self.assertIn("finite reporting vocabulary", str(raised.exception))
        self.assertNotIn("secret_person_label", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
