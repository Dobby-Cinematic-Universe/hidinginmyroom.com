from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.build_wiki_catalog_anchor_inventory import (
    AnchorInventoryError,
    _reject_symlink_components,
    build_anchor_inventory,
    extract_locator,
    wiki_seed_inventory_id,
    write_owner_private_exact,
)


class WikiCatalogAnchorInventoryTests(unittest.TestCase):
    def test_extracts_exact_provider_locators_without_network(self) -> None:
        self.assertEqual(
            extract_locator("https://www.youtube.com/watch?v=gPhrE99xwqI&t=4s"),
            {
                "kind": "youtube_video_id",
                "value": "gPhrE99xwqI",
                "lookups": [
                    {"namespace": "youtube_video_id", "tier": 0},
                    {"namespace": "youtube_video_id_candidate", "tier": 1},
                ],
            },
        )
        self.assertEqual(
            extract_locator(
                "https://archive.org/download/69999/A%20title%20%5Babc123DEF_0%5D.mp4"
            )["value"],
            "69999/A title [abc123DEF_0].mp4",
        )
        self.assertEqual(
            extract_locator("https://www.reddit.com/r/HIMRFAM/comments/1ABCdef/x/")[
                "value"
            ],
            "1abcdef",
        )
        self.assertEqual(
            extract_locator("/wiki/archive/methodology/")["kind"],
            "unsupported_href",
        )

    def _database(self, path: Path) -> None:
        connection = sqlite3.connect(path)
        connection.executescript(
            """
            CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
            INSERT INTO schema_migrations VALUES (31);
            CREATE TABLE sources(
              source_id TEXT PRIMARY KEY,
              access_state TEXT NOT NULL,
              review_state TEXT NOT NULL
            );
            CREATE TABLE recordings(
              recording_id TEXT PRIMARY KEY,
              review_state TEXT NOT NULL
            );
            CREATE TABLE recording_sources(
              recording_source_id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL,
              recording_id TEXT NOT NULL,
              mapping_role TEXT NOT NULL,
              source_start_ms INTEGER,
              source_end_ms INTEGER,
              recording_start_ms INTEGER,
              recording_end_ms INTEGER,
              mapping_method TEXT NOT NULL,
              confidence_state TEXT NOT NULL
            );
            CREATE TABLE media_objects(
              media_id TEXT PRIMARY KEY,
              media_kind TEXT NOT NULL,
              container TEXT,
              integrity_state TEXT NOT NULL,
              duration_ms INTEGER
            );
            CREATE TABLE renditions(
              rendition_id TEXT PRIMARY KEY,
              recording_id TEXT NOT NULL,
              media_id TEXT NOT NULL,
              rendition_kind TEXT NOT NULL,
              review_state TEXT NOT NULL
            );
            CREATE TABLE external_ids(
              external_id_id TEXT PRIMARY KEY,
              object_type TEXT NOT NULL,
              object_id TEXT NOT NULL,
              namespace TEXT NOT NULL,
              external_value TEXT NOT NULL,
              confidence_state TEXT NOT NULL,
              source_id TEXT
            );

            INSERT INTO sources VALUES ('src_yt', 'public', 'reviewed');
            INSERT INTO recordings VALUES ('rec_yt', 'reviewed');
            INSERT INTO recording_sources VALUES
              ('rs_yt', 'src_yt', 'rec_yt', 'primary', NULL, NULL, NULL, NULL,
               'provider_id', 'reviewed');
            INSERT INTO media_objects VALUES
              ('media_yt_1', 'video', 'mp4', 'verified', 1000);
            INSERT INTO media_objects VALUES
              ('media_yt_2', 'audio', 'flac', 'verified', 900);
            INSERT INTO renditions VALUES
              ('rnd_yt_1', 'rec_yt', 'media_yt_1', 'full', 'reviewed');
            INSERT INTO renditions VALUES
              ('rnd_yt_2', 'rec_yt', 'media_yt_2', 'normalized_audio', 'unreviewed');
            INSERT INTO external_ids VALUES
              ('eid_yt', 'recording', 'rec_yt', 'youtube_video_id',
               'gPhrE99xwqI', 'reviewed', 'src_yt');
            INSERT INTO external_ids VALUES
              ('eid_yt_hint', 'recording', 'rec_yt', 'youtube_video_id_candidate',
               'gPhrE99xwqI', 'candidate', 'src_yt');

            INSERT INTO sources VALUES ('src_ia', 'public', 'metadata_only');
            INSERT INTO recordings VALUES ('rec_ia', 'metadata_only');
            INSERT INTO recording_sources VALUES
              ('rs_ia', 'src_ia', 'rec_ia', 'primary', NULL, NULL, NULL, NULL,
               'archive_metadata', 'metadata_only');
            INSERT INTO media_objects VALUES
              ('media_ia', 'video', 'mp4', 'verified', 2000);
            INSERT INTO renditions VALUES
              ('rnd_ia', 'rec_ia', 'media_ia', 'archive_original', 'unreviewed');
            INSERT INTO external_ids VALUES
              ('eid_ia', 'source', 'src_ia', 'internet_archive_item_filename',
               '69999/A title [abc123DEF_0].mp4', 'metadata_only', 'src_ia');
            """
        )
        connection.commit()
        connection.close()

    def test_builds_deterministic_tiered_anchor_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "catalog.sqlite3"
            inventory_path = root / "wiki.json"
            self._database(database)
            inventory_body = {
                "schema_version": 1,
                "kind": "wiki_graph_seed_inventory",
                "source_tree_sha256": "a" * 64,
                "pages": [
                    {
                        "domain": "events",
                        "slug": "example",
                        "source_path": "src/content/docs/wiki/events/example.mdx",
                        "source_citations": [
                            {
                                "ordinal": 1,
                                "source_id": "yt",
                                "claim_id": "CLAIM-1",
                                "href": "https://youtube.com/watch?v=gPhrE99xwqI",
                                "review_state": "primary-media-checked",
                                "checked_attribute_present": True,
                            },
                            {
                                "ordinal": 2,
                                "source_id": "ia",
                                "claim_id": "CLAIM-2",
                                "href": "https://archive.org/download/69999/A%20title%20%5Babc123DEF_0%5D.mp4",
                                "review_state": "source-matched",
                                "checked_attribute_present": True,
                            },
                            {
                                "ordinal": 3,
                                "source_id": "missing",
                                "claim_id": None,
                                "href": None,
                                "review_state": None,
                                "checked_attribute_present": False,
                            },
                            {
                                "ordinal": 4,
                                "source_id": "internal",
                                "claim_id": None,
                                "href": "/corpus/",
                                "review_state": None,
                                "checked_attribute_present": False,
                            },
                        ],
                    }
                ],
            }
            inventory = {
                **inventory_body,
                "inventory_id": wiki_seed_inventory_id(inventory_body),
            }
            inventory_path.write_text(
                json.dumps(inventory)
                + "\n",
                encoding="utf-8",
            )

            first = build_anchor_inventory(inventory_path, database)
            second = build_anchor_inventory(inventory_path, database)
            self.assertEqual(first, second)
            self.assertRegex(first["inventory_id"], r"^wcai_[0-9a-f]{32}$")
            self.assertEqual(first["counts"]["citations"], 4)
            self.assertEqual(first["counts"]["anchor_candidates"], 3)
            self.assertEqual(
                first["counts"]["resolution_states"],
                {
                    "missing_href": 1,
                    "multiple_anchor_candidates": 1,
                    "single_anchor_candidate": 1,
                    "unsupported_href": 1,
                },
            )
            youtube = first["citations"][0]
            self.assertEqual(youtube["selected_external_id_tier"], 0)
            self.assertEqual(youtube["lower_tier_match_count"], 1)
            self.assertEqual(len(youtube["anchor_candidates"]), 2)
            self.assertEqual(
                {item["media_kind"] for item in youtube["anchor_candidates"]},
                {"audio", "video"},
            )
            self.assertFalse(first["authority"]["claim_catalog_link"])
            self.assertFalse(first["authority"]["publication"])

    def test_owner_private_write_reuses_exact_bytes_and_refuses_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "private"
            output = root / "nested" / "inventory.json"
            self.assertFalse(write_owner_private_exact(output, root, b"one\n"))
            self.assertEqual(stat_mode(output), 0o600)
            self.assertEqual(stat_mode(root), 0o700)
            self.assertTrue(write_owner_private_exact(output, root, b"one\n"))
            with self.assertRaisesRegex(AnchorInventoryError, "Refusing to overwrite"):
                write_owner_private_exact(output, root, b"two\n")

    def test_symlinked_path_component_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            real = root / "real"
            real.mkdir()
            link = root / "link"
            link.symlink_to(real, target_is_directory=True)
            with self.assertRaisesRegex(AnchorInventoryError, "symlink component"):
                _reject_symlink_components(link / "inventory.json", "Output")


def stat_mode(path: Path) -> int:
    return os.stat(path).st_mode & 0o777


if __name__ == "__main__":
    unittest.main()
