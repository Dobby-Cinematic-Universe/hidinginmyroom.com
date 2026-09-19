from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from acquisition.archive_org_metadata import (  # noqa: E402
    capture_snapshot,
    make_writable,
    stable_id,
)
from himr_corpus.archive_metadata_snapshot_importer import (  # noqa: E402
    ArchiveMetadataSnapshotImportError,
    import_archive_metadata_snapshot,
    validate_archive_metadata_snapshot,
)
from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.ids import source_id  # noqa: E402


PAYLOAD = json.dumps(
    {
        "metadata": {
            "identifier": "fixture-himr-item",
            "title": "Unreviewed provider item title",
            "collection": ["fixture", "opensource_movies"],
        },
        "files": [
            {
                "name": "20260801-guest fixture-AbCdEfGhI_1.mp4",
                "source": "original",
                "format": "MPEG4",
                "size": "1000",
                "length": "60.5",
                "md5": "11111111111111111111111111111111",
            },
            {
                "name": "20260801-guest fixture-AbCdEfGhI_1.ia.mp4",
                "source": "derivative",
                "original": "20260801-guest fixture-AbCdEfGhI_1.mp4",
                "format": "h.264",
                "size": "500",
                "length": "60.5",
            },
            {"name": "fixture-himr-item_meta.sqlite", "source": "metadata"},
        ],
    },
    ensure_ascii=False,
    sort_keys=True,
).encode("utf-8")


class FakeResponse(io.BytesIO):
    status = 200

    def __init__(self, url: str):
        super().__init__(PAYLOAD)
        self._url = url
        self.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(PAYLOAD)),
            "ETag": '"fixture"',
            "Date": "Wed, 26 Aug 2026 12:00:02 GMT",
        }

    def getcode(self):
        return 200

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeOpener:
    def open(self, request, timeout):
        return FakeResponse(request.full_url)


class FixedClock:
    def __init__(self):
        self.values = iter(
            ["2026-08-26T12:00:01Z", "2026-08-26T12:00:02Z"]
        )

    def __call__(self):
        return next(self.values)


class ArchiveMetadataSnapshotImporterTests(unittest.TestCase):
    def setUp(self):
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="archive-import-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        request = {
            "schema_version": 1,
            "request_kind": "archive_org_metadata_targets",
            "requested_at": "2026-08-26T12:00:00Z",
            "items": [
                {
                    "identifier": "fixture-himr-item",
                    "basis": "manual_public_lead",
                }
            ],
            "policy": {
                "public_unauthenticated_metadata_only": True,
                "media_download": False,
                "cookies_sent": False,
                "authorization_sent": False,
                "publication_authority": False,
            },
        }
        request["request_id"] = stable_id("iamr", request)
        request_path = self.root / "request.json"
        request_path.write_text(
            json.dumps(request, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        self.snapshot = capture_snapshot(
            request_path,
            output_root=self.root / "snapshots",
            opener_factory=lambda _identifier: FakeOpener(),
            clock=FixedClock(),
        )

    def tearDown(self):
        self.connection.close()
        make_writable(self.root)
        self.temporary.cleanup()

    def counts(self) -> dict[str, int]:
        return {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "sources",
                "source_snapshots",
                "recordings",
                "recording_sources",
                "source_relations",
                "media_objects",
                "artifacts",
                "publication_decisions",
                "publication_gate_decisions",
                "identity_clusters",
                "identity_assertions",
                "identity_cluster_versions",
                "identity_cluster_memberships",
                "identity_cannot_link_decisions",
            )
        }

    def test_import_is_transactional_idempotent_and_private(self):
        validated = validate_archive_metadata_snapshot(self.snapshot)
        first = import_archive_metadata_snapshot(self.connection, self.snapshot)
        counts_after_first = self.counts()
        second = import_archive_metadata_snapshot(self.connection, self.snapshot)
        counts_after_second = self.counts()

        self.assertEqual(first, second)
        self.assertEqual(counts_after_first, counts_after_second)
        self.assertEqual(first["items"], 1)
        self.assertEqual(first["catalog_statistics"]["archive_items"], 1)
        self.assertEqual(first["catalog_statistics"]["video_files"], 2)
        self.assertEqual(first["exact_capture_evidence_rows"], 1)
        self.assertEqual(first["media_downloads"], 0)
        self.assertEqual(first["publication_decisions_created"], 0)
        self.assertEqual(first["identity_assertions_created"], 0)
        for table in (
            "media_objects",
            "artifacts",
            "publication_decisions",
            "publication_gate_decisions",
            "identity_clusters",
            "identity_assertions",
            "identity_cluster_versions",
            "identity_cluster_memberships",
            "identity_cannot_link_decisions",
        ):
            self.assertEqual(counts_after_second[table], 0, table)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM public_sources").fetchone()[0],
            0,
        )

        item = validated["items"][0]
        evidence = self.connection.execute(
            """
            SELECT * FROM source_snapshots
            WHERE source_id = ? AND payload_sha256 = ? AND http_status = 200
            """,
            (
                source_id("internet_archive", "archive_item", "fixture-himr-item"),
                item["payload_sha256"],
            ),
        ).fetchone()
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence["final_url"], item["final_url"])
        metadata = json.loads(evidence["metadata_json"])
        self.assertEqual(metadata["archive_metadata_snapshot_id"], validated["snapshot_id"])
        self.assertFalse(metadata["provider_fields_are_content_truth"])
        self.assertFalse(metadata["publication_authority"])

    def test_tampered_payload_fails_before_any_catalog_write(self):
        before = self.counts()
        payload = self.snapshot.parent / "item-fixture-himr-item.metadata.json"
        payload.chmod(0o600)
        altered = PAYLOAD.replace(b"Unreviewed", b"UnrevieweD")
        self.assertEqual(len(altered), len(PAYLOAD))
        payload.write_bytes(altered)
        payload.chmod(0o400)
        with self.assertRaisesRegex(ArchiveMetadataSnapshotImportError, "payload bytes differ"):
            import_archive_metadata_snapshot(self.connection, self.snapshot)
        self.assertEqual(before, self.counts())

    def test_completed_replay_refuses_to_repair_deleted_exact_evidence(self):
        validated = validate_archive_metadata_snapshot(self.snapshot)
        import_archive_metadata_snapshot(self.connection, self.snapshot)
        item = validated["items"][0]
        self.connection.execute(
            "DELETE FROM source_snapshots WHERE source_id = ? AND payload_sha256 = ?",
            (
                source_id("internet_archive", "archive_item", "fixture-himr-item"),
                item["payload_sha256"],
            ),
        )
        after_delete = self.counts()
        with self.assertRaisesRegex(
            ArchiveMetadataSnapshotImportError, "missing exact capture evidence"
        ):
            import_archive_metadata_snapshot(self.connection, self.snapshot)
        self.assertEqual(after_delete, self.counts())

    def test_cli_exposes_strict_snapshot_commands(self):
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("validate-archive-metadata-snapshot", choices)
        self.assertIn("import-archive-metadata-snapshot", choices)


if __name__ == "__main__":
    unittest.main()
