from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from acquisition.reddit_rss import capture_snapshot, write_discovery_manifest  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.ids import recording_id, source_id  # noqa: E402
from himr_corpus.reddit_discovery_importer import (  # noqa: E402
    RedditDiscoveryImportError,
    import_reddit_discovery_manifest,
    validate_reddit_discovery_manifest,
)


ATOM_PAYLOAD = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>fixture feed</title>
  <entry>
    <id>t3_1vvver5</id>
    <title>Unreviewed discovery title</title>
    <published>2026-08-25T12:00:00Z</published>
    <updated>2026-08-25T12:01:00Z</updated>
    <link href="https://www.reddit.com/r/HIMRFAM2/comments/1vvver5/example/" />
    <author><name>private_derivative_handle</name></author>
    <content type="html">
      &lt;a href="https://v.redd.it/ftkqj9bfg1lh1"&gt;video&lt;/a&gt;
      &lt;a href="https://www.youtube.com/watch?v=Z32Y-D5kJTg"&gt;youtube&lt;/a&gt;
      &lt;a href="https://archive.org/details/fixture-item"&gt;archive item&lt;/a&gt;
      &lt;a href="https://archive.org/download/fixture-item/clip.mp4"&gt;archive&lt;/a&gt;
      &lt;a href="https://www.reddit.com/gallery/1vvver5"&gt;gallery&lt;/a&gt;
      &lt;img src="https://i.redd.it/example123.jpg" /&gt;
    </content>
  </entry>
</feed>
"""


class FakeResponse(io.BytesIO):
    status = 200

    def __init__(self, url: str):
        super().__init__(ATOM_PAYLOAD)
        self._url = url
        self.headers = {
            "Content-Type": "application/atom+xml",
            "Content-Length": str(len(ATOM_PAYLOAD)),
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


class RedditDiscoveryImporterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        self.addCleanup(self.connection.close)
        migrate(self.connection)

        snapshot_path = capture_snapshot(
            subreddit="HIMRFAM2",
            limit=100,
            out_dir=self.root,
            opener=FakeOpener(),
        )
        info = self.root / "fixture.info.json"
        info.write_text(
            json.dumps(
                {
                    "id": "1vvver5",
                    "webpage_url": "https://www.reddit.com/r/HIMRFAM2/comments/1vvver5/example/",
                    "duration": 139,
                    "availability": "public",
                    "uploader": "must_not_enter_catalog",
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        fixture_mtime_seconds = int(
            datetime(2026, 8, 26, 12, 4, tzinfo=timezone.utc).timestamp()
        )
        os.utime(
            info,
            ns=(fixture_mtime_seconds * 1_000_000_000,) * 2,
        )
        self.manifest = write_discovery_manifest(
            snapshot_path,
            v_reddit_info_paths=[info],
            metadata_observed_at="2026-08-26T12:05:00Z",
        )

    def test_import_is_idempotent_private_and_contextual(self):
        first = import_reddit_discovery_manifest(self.connection, self.manifest)
        counts_before = {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "sources",
                "source_snapshots",
                "source_relations",
                "recordings",
                "recording_sources",
                "review_tasks",
                "publication_decisions",
                "publication_gate_decisions",
                "media_objects",
            )
        }
        second = import_reddit_discovery_manifest(self.connection, self.manifest)
        counts_after = {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in counts_before
        }
        self.assertEqual(first, second)
        self.assertEqual(counts_before, counts_after)
        self.assertEqual(first["posts"], 1)
        self.assertEqual(first["recording_candidates"], 1)
        self.assertEqual(first["publication_decisions_created"], 0)
        self.assertEqual(counts_after["publication_decisions"], 0)
        self.assertEqual(counts_after["publication_gate_decisions"], 0)
        self.assertEqual(counts_after["media_objects"], 0)

        reddit_video_source = source_id("reddit", "reddit_video", "ftkqj9bfg1lh1")
        source = self.connection.execute(
            "SELECT * FROM sources WHERE source_id = ?", (reddit_video_source,)
        ).fetchone()
        self.assertEqual(source["access_state"], "public")
        self.assertEqual(source["review_state"], "unreviewed")
        self.assertNotIn("must_not_enter_catalog", source["metadata_json"])
        self.assertNotIn("private_derivative_handle", source["metadata_json"])

        recording = self.connection.execute(
            "SELECT * FROM recordings WHERE recording_id = ?",
            (recording_id("reddit:video:ftkqj9bfg1lh1"),),
        ).fetchone()
        self.assertEqual(recording["duration_ms"], 139000)
        self.assertEqual(recording["review_state"], "unreviewed")
        task = self.connection.execute(
            "SELECT * FROM review_tasks WHERE target_type = 'recording' AND target_id = ?",
            (recording["recording_id"],),
        ).fetchone()
        self.assertEqual(task["status"], "open")

        youtube = self.connection.execute(
            "SELECT * FROM sources WHERE source_id = ?",
            (source_id("youtube", "youtube_video", "Z32Y-D5kJTg"),),
        ).fetchone()
        archive = self.connection.execute(
            "SELECT * FROM sources WHERE source_id = ?",
            (source_id("internet_archive", "archive_media_file", "fixture-item/clip.mp4"),),
        ).fetchone()
        self.assertEqual(youtube["access_state"], "unknown")
        self.assertEqual(archive["access_state"], "unknown")
        self.assertGreaterEqual(counts_after["source_relations"], 5)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM public_sources").fetchone()[0],
            0,
        )

    def test_exact_atom_payload_hash_is_cataloged(self):
        manifest = validate_reddit_discovery_manifest(self.manifest)
        import_reddit_discovery_manifest(self.connection, self.manifest)
        snapshot = self.connection.execute(
            "SELECT * FROM source_snapshots WHERE payload_sha256 = ?",
            (manifest["snapshot"]["payload_sha256"],),
        ).fetchone()
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["http_status"], 200)
        self.assertEqual(snapshot["final_url"], manifest["snapshot"]["final_url"])

    def test_unknown_personal_data_field_is_rejected(self):
        raw = json.loads(self.manifest.read_text(encoding="utf-8"))
        raw["posts"][0]["author"] = "should not be retained"
        bad = self.root / "bad.discovery.json"
        bad.write_text(json.dumps(raw, sort_keys=True), encoding="utf-8")
        with self.assertRaisesRegex(RedditDiscoveryImportError, "unknown=.*author"):
            validate_reddit_discovery_manifest(bad)

    def test_cli_exposes_validation_and_import_commands(self):
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("validate-reddit-rss-discovery", choices)
        self.assertIn("import-reddit-rss-discovery", choices)


if __name__ == "__main__":
    unittest.main()
