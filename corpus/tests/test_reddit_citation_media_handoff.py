from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate, transaction  # noqa: E402
import himr_corpus.reddit_citation_media_handoff as handoff  # noqa: E402


SNAPSHOT = REPOSITORY_ROOT / "research" / "reddit-snapshots" / "2026-08-26"


def _approved_ffprobe_available() -> bool:
    try:
        return (
            hashlib.sha256(handoff.FFPROBE_PATH.read_bytes()).hexdigest()
            == handoff.FFPROBE_SHA256
        )
    except OSError:
        return False


class RedditCitationMediaHandoffGuardTests(unittest.TestCase):
    def test_default_refuses_current_user_writable_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError,
                "writable.*explicit writable-input guard",
            ):
                handoff.materialize_reddit_citation_media_handoff(
                    SNAPSHOT, Path(temporary) / "bundle"
                )

    def test_single_gzip_member_rejects_trailing_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "body.gz"
            body = b"bounded fixture body"
            path.write_bytes(gzip.compress(body, mtime=0) + b"trailing")
            path.chmod(0o400)
            pinned = handoff._PinnedFile.open(
                path,
                maximum=1024,
                label="fixture gzip",
                guard_writable=False,
            )
            try:
                with self.assertRaisesRegex(
                    handoff.RedditCitationMediaHandoffError,
                    "second gzip member or trailing bytes",
                ):
                    handoff._strict_gzip(
                        pinned, expected_size=len(body), maximum=1024
                    )
            finally:
                pinned.close()

    def test_single_gzip_member_rejects_concatenated_member(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "body.gz"
            body = b"bounded fixture body"
            path.write_bytes(
                gzip.compress(body, mtime=0)
                + gzip.compress(b"forbidden second member", mtime=0)
            )
            path.chmod(0o400)
            pinned = handoff._PinnedFile.open(
                path,
                maximum=1024,
                label="fixture gzip",
                guard_writable=False,
            )
            try:
                with self.assertRaisesRegex(
                    handoff.RedditCitationMediaHandoffError,
                    "second gzip member or trailing bytes",
                ):
                    handoff._strict_gzip(
                        pinned, expected_size=len(body), maximum=1024
                    )
            finally:
                pinned.close()

    def test_strict_json_rejects_duplicates_floats_and_oversized_integers(self) -> None:
        fixtures = (
            (b'{"x":1,"x":2}', "duplicate JSON key"),
            (b'{"x":1.25}', "invalid number"),
            (b'{"x":123456789012345678901}', "oversized integer"),
        )
        for body, error in fixtures:
            with self.subTest(body=body), self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError, error
            ):
                handoff._strict_json(body, "adversarial JSON")

    def test_pinned_file_detects_requested_path_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.bin"
            displaced = Path(temporary) / "displaced.bin"
            path.write_bytes(b"exact evidence")
            path.chmod(0o400)
            pinned = handoff._PinnedFile.open(
                path,
                maximum=1024,
                label="adversarial evidence",
                guard_writable=False,
            )
            try:
                path.rename(displaced)
                path.write_bytes(b"exact evidence")
                path.chmod(0o400)
                with self.assertRaisesRegex(
                    handoff.RedditCitationMediaHandoffError,
                    "identity or metadata changed",
                ):
                    pinned.verify()
            finally:
                pinned.close()

    def test_atomic_publication_never_replaces_empty_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            destination.mkdir()
            (source / "source-marker").write_text("source", encoding="utf-8")
            with self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError,
                "output appeared before atomic publication",
            ):
                handoff._rename_directory_noreplace(source, destination)
            self.assertTrue((source / "source-marker").is_file())
            self.assertEqual(list(destination.iterdir()), [])

    def test_early_bundle_failure_closes_snapshot_session(self) -> None:
        snapshot = mock.Mock()
        snapshot.media = []
        with mock.patch.object(handoff, "_open_snapshot", return_value=snapshot), mock.patch.object(
            handoff,
            "_open_bundle",
            side_effect=handoff.RedditCitationMediaHandoffError("invalid bundle"),
        ):
            with self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError, "invalid bundle"
            ):
                handoff.validate_reddit_citation_media_handoff(
                    Path("snapshot"), Path("manifest")
                )
        snapshot.close.assert_called_once_with()

    def test_early_import_bundle_failure_closes_snapshot_session(self) -> None:
        snapshot = mock.Mock()
        snapshot.media = []
        with mock.patch.object(handoff, "_open_snapshot", return_value=snapshot), mock.patch.object(
            handoff,
            "_open_bundle",
            side_effect=handoff.RedditCitationMediaHandoffError("invalid bundle"),
        ):
            with self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError, "invalid bundle"
            ):
                handoff.import_reddit_citation_media_handoff(
                    mock.Mock(), Path("snapshot"), Path("manifest")
                )
        snapshot.close.assert_called_once_with()

    def test_ffprobe_open_failure_closes_snapshot_session(self) -> None:
        snapshot = mock.Mock()
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            handoff, "_open_snapshot", return_value=snapshot
        ), mock.patch.object(
            handoff,
            "_ffprobe_pin",
            side_effect=handoff.RedditCitationMediaHandoffError("bad ffprobe"),
        ):
            with self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError, "bad ffprobe"
            ):
                handoff.materialize_reddit_citation_media_handoff(
                    Path("snapshot"), Path(temporary) / "bundle"
                )
        snapshot.close.assert_called_once_with()

    def test_protected_guard_blocks_same_count_trigger_update(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "catalog.sqlite3")
            self.addCleanup(connection.close)
            migrate(connection)
            connection.execute(
                "INSERT INTO catalog_snapshots(catalog_snapshot_id, created_at, "
                "schema_version, generator_version, notes) VALUES(?, ?, 1, ?, ?)",
                ("protected", "2026-08-26T00:00:00Z", "fixture", "before"),
            )
            connection.execute(
                "CREATE TRIGGER adversarial_handoff_update AFTER INSERT ON media_objects "
                "BEGIN UPDATE catalog_snapshots SET notes = 'after' "
                "WHERE catalog_snapshot_id = 'protected'; END"
            )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "protected-table write"
            ):
                with transaction(connection), handoff._protected_write_guard(connection):
                    connection.execute(
                        "INSERT INTO media_objects(media_id, sha256, byte_count, "
                        "media_kind, first_cataloged_at) VALUES(?, ?, 0, 'other', ?)",
                        ("media_fixture", "0" * 64, "2026-08-26T00:00:00Z"),
                    )
            self.assertEqual(
                connection.execute(
                    "SELECT notes FROM catalog_snapshots "
                    "WHERE catalog_snapshot_id = 'protected'"
                ).fetchone()[0],
                "before",
            )
            self.assertEqual(
                connection.execute("SELECT count(*) FROM media_objects").fetchone()[0],
                0,
            )

    def test_comment_media_requires_exact_comment_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "snapshot"
            root.mkdir()
            shutil.copy2(SNAPSHOT / "provenance.json", root / "provenance.json")
            inventory = json.loads(
                (SNAPSHOT / "citation-inventory.json").read_text(encoding="utf-8")
            )
            target = next(
                row
                for row in inventory["citations"]
                if row["comment_id_from_citation_url"] == "ozttjrg"
            )
            target["observations"]["embed_page"]["comment_id"] = "different1"
            inventory["citations"].remove(target)
            inventory["citations"].insert(0, target)
            inventory_body = json.dumps(
                inventory,
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8") + b"\n"
            (root / "citation-inventory.json").write_bytes(inventory_body)
            os.symlink(SNAPSHOT / "raw", root / "raw", target_is_directory=True)
            with mock.patch.object(
                handoff,
                "FROZEN_INVENTORY_SHA256",
                hashlib.sha256(inventory_body).hexdigest(),
            ):
                with self.assertRaisesRegex(
                    handoff.RedditCitationMediaHandoffError,
                    "official embed comment ID does not equal the cited comment ID",
                ):
                    handoff._open_snapshot(root, guard_writable_inputs=True)

    def test_cli_exposes_materialize_validate_and_import(self) -> None:
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("materialize-reddit-citation-media-handoff", choices)
        self.assertIn("validate-reddit-citation-media-handoff", choices)
        self.assertIn("import-reddit-citation-media-handoff", choices)


@unittest.skipUnless(
    SNAPSHOT.is_dir() and _approved_ffprobe_available(),
    "frozen Reddit snapshot and approved ffprobe are required",
)
class RedditCitationMediaHandoffIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name).resolve()
        cls.bundle = cls.root / "bundle"
        cls.materialized = handoff.materialize_reddit_citation_media_handoff(
            SNAPSHOT,
            cls.bundle,
            guard_writable_inputs=True,
        )
        cls.manifest = cls.bundle / "handoff-manifest.json"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def test_exact_frozen_bundle_validates_and_is_read_only(self) -> None:
        result = handoff.validate_reddit_citation_media_handoff(
            SNAPSHOT,
            self.manifest,
            guard_writable_inputs=True,
        )
        self.assertTrue(result["valid"])
        self.assertEqual(result["handoff_id"], "rcmh_17fe1350e49581c5463fd1ad49506216")
        self.assertEqual(
            self.materialized["manifest_sha256"],
            "62ca804003b747cbb8be528ddb40839df34832e6c5459beb35de67eb41de48fc",
        )
        self.assertEqual(result["statistics"]["contexts"], 14)
        self.assertEqual(result["statistics"]["media"], 17)
        self.assertEqual(result["statistics"]["videos"], 10)
        self.assertEqual(result["statistics"]["images"], 7)
        self.assertEqual(result["statistics"]["video_duration_ms"], 1_482_600)
        self.assertEqual(self.manifest.stat().st_mode & 0o777, 0o400)
        for path in (self.bundle / "media").rglob("*"):
            if path.is_file():
                self.assertEqual(path.stat().st_mode & 0o777, 0o400)

    def test_wrong_media_boundary_is_rejected_before_ffprobe(self) -> None:
        manifest = json.loads(self.manifest.read_text(encoding="utf-8"))
        manifest["media"][0]["sha256"] = "0" * 64
        identity = {
            "snapshot_date": handoff.FROZEN_SNAPSHOT_DATE,
            "provenance_sha256": handoff.FROZEN_PROVENANCE_SHA256,
            "citation_inventory_sha256": handoff.FROZEN_INVENTORY_SHA256,
            "media": [row["sha256"] for row in manifest["media"]],
        }
        manifest["handoff_id"] = handoff._derived_id("rcmh", identity)
        path = self.root / "wrong-media-manifest.json"
        path.write_bytes(handoff._canonical_bytes(manifest) + b"\n")
        path.chmod(0o400)
        expected = [
            row["sha256"]
            for row in json.loads(self.manifest.read_text(encoding="utf-8"))["media"]
        ]
        with mock.patch.object(
            handoff, "_ffprobe_pin", side_effect=AssertionError("must not probe")
        ), self.assertRaisesRegex(
            handoff.RedditCitationMediaHandoffError,
            "media hashes/order differ from the frozen snapshot",
        ):
            handoff._open_bundle(path, expected_media_sha256s=expected)

    def test_import_is_candidate_only_and_byte_idempotent(self) -> None:
        database = self.root / "catalog.sqlite3"
        connection = connect(database)
        self.addCleanup(connection.close)
        migrate(connection)
        protected = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "recordings",
                "renditions",
                "transcript_revisions",
                "review_decisions",
                "publication_decisions",
                "publication_gate_decisions",
                "public_sources",
                "public_recordings",
                "public_transcript_revisions",
                "public_transcript_segments",
            )
        }
        first = handoff.import_reddit_citation_media_handoff(
            connection,
            SNAPSHOT,
            self.manifest,
            guard_writable_inputs=True,
        )
        counts = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "sources",
                "source_snapshots",
                "source_relations",
                "media_objects",
                "media_locations",
                "media_sources",
                "review_tasks",
            )
        }
        second = handoff.import_reddit_citation_media_handoff(
            connection,
            SNAPSHOT,
            self.manifest,
            guard_writable_inputs=True,
        )
        replay_counts = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in counts
        }
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["statistics"], second["statistics"])
        self.assertEqual(counts, replay_counts)
        self.assertEqual(
            counts,
            {
                "sources": 31,
                "source_snapshots": 31,
                "source_relations": 17,
                "media_objects": 17,
                "media_locations": 17,
                "media_sources": 17,
                "review_tasks": 51,
            },
        )
        self.assertEqual(
            {
                table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in protected
            },
            protected,
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM source_relations WHERE confidence_state = 'candidate'"
            ).fetchone()[0],
            17,
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM media_objects WHERE media_kind = 'image' "
                "AND container = 'jpeg'"
            ).fetchone()[0],
            7,
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM media_objects WHERE media_kind = 'video' "
                "AND json_extract(ffprobe_json, '$.audio_stream_count') = 0"
            ).fetchone()[0],
            10,
        )
        self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertEqual(list(connection.execute("PRAGMA foreign_key_check")), [])
        snapshot_session = handoff._open_snapshot(
            SNAPSHOT, guard_writable_inputs=True
        )
        bundle_session = handoff._open_bundle(
            self.manifest,
            expected_media_sha256s=[
                row["sha256"] for row in snapshot_session.media
            ],
        )
        try:
            batch_id = second["import_batch_id"]
            target_snapshot = connection.execute(
                "SELECT source_snapshot_id, request_url FROM source_snapshots "
                "ORDER BY source_snapshot_id LIMIT 1"
            ).fetchone()
            connection.execute(
                "UPDATE source_snapshots SET request_url = ? "
                "WHERE source_snapshot_id = ?",
                ("https://example.invalid/tampered", target_snapshot[0]),
            )
            with self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError,
                "completed handoff .* snapshot conflicts",
            ):
                handoff._verify_completed_import_rows(
                    connection,
                    snapshot=snapshot_session,
                    bundle=bundle_session,
                    batch_id=batch_id,
                    observed_at=bundle_session.manifest["observed_at"],
                    statistics=second["statistics"],
                )
            connection.execute(
                "UPDATE source_snapshots SET request_url = ? "
                "WHERE source_snapshot_id = ?",
                (target_snapshot[1], target_snapshot[0]),
            )
            missing_task = connection.execute(
                "SELECT review_task_id FROM review_tasks "
                "ORDER BY review_task_id LIMIT 1"
            ).fetchone()[0]
            connection.execute(
                "DELETE FROM review_tasks WHERE review_task_id = ?", (missing_task,)
            )
            with self.assertRaisesRegex(
                handoff.RedditCitationMediaHandoffError,
                "completed handoff review-task identity conflicts",
            ):
                handoff._verify_completed_import_rows(
                    connection,
                    snapshot=snapshot_session,
                    bundle=bundle_session,
                    batch_id=batch_id,
                    observed_at=bundle_session.manifest["observed_at"],
                    statistics=second["statistics"],
                )
        finally:
            snapshot_session.close()
            bundle_session.close()


if __name__ == "__main__":
    unittest.main()
