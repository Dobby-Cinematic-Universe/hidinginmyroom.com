from __future__ import annotations

import itertools
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.ids import recording_id, source_id  # noqa: E402
from himr_corpus.importers import (  # noqa: E402
    approve_public_source_metadata,
    import_current_channel,
    import_youtube_discovery_candidates,
    import_ytdlp_info,
)
from himr_corpus.validation import validate_database  # noqa: E402


VIDEO_ID = "orderproof1"
CHANNEL_ID = "UC_order_invariance_fixture"


class CatalogPrecedenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.inventory = self.root / "inventory.json"
        self.inventory.write_text(
            json.dumps(
                {
                    "expected_channel": {
                        "stable_channel_id": CHANNEL_ID,
                        "display_name": "Official fixture channel",
                    },
                    "items": [
                        {
                            "video_id": VIDEO_ID,
                            "type": "video",
                            "observed": {
                                "title": "Newer first-party title",
                                "publish_date_utc": "2026-08-20T00:00:00Z",
                                "duration_seconds": 90,
                                "inferred_access": "public",
                            },
                        }
                    ],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.candidate = self.root / "candidate.jsonl"
        self.candidate.write_text(
            json.dumps(
                {
                    "id": VIDEO_ID,
                    "title": "Newest but low-quality search title",
                    "channel": "search label",
                    "url": f"https://www.youtube.com/watch?v={VIDEO_ID}",
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        self.old_info = self._write_info(
            "old.info.json",
            title="Older direct title",
            availability="public",
            duration=120,
            upload_date="20260819",
            was_live=False,
        )
        self.corrected_info = self._write_info(
            "corrected.info.json",
            title="Corrected direct title",
            availability="members_only",
            duration=80,
            upload_date="20260821",
            was_live=True,
        )

    def _write_info(
        self,
        filename: str,
        *,
        title: str,
        availability: str,
        duration: int,
        upload_date: str,
        was_live: bool,
    ) -> Path:
        path = self.root / filename
        path.write_text(
            json.dumps(
                {
                    "id": VIDEO_ID,
                    "title": title,
                    "channel": "Official fixture channel",
                    "channel_id": CHANNEL_ID,
                    "channel_url": f"https://www.youtube.com/channel/{CHANNEL_ID}",
                    "webpage_url": f"https://www.youtube.com/watch?v={VIDEO_ID}",
                    "availability": availability,
                    "duration": duration,
                    "upload_date": upload_date,
                    "was_live": was_live,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return path

    def _database(self, label: str):
        connection = connect(self.root / f"{label}.sqlite3")
        migrate(connection)
        self.addCleanup(connection.close)
        return connection

    @staticmethod
    def _rows(connection, query: str) -> list[dict]:
        return [dict(row) for row in connection.execute(query)]

    def _projection(self, connection) -> dict:
        return {
            "sources": self._rows(
                connection,
                """
                SELECT source_id, platform, source_kind, native_id, parent_source_id,
                       canonical_url, historical_url, title, published_at, observed_at,
                       access_state, review_state, metadata_json,
                       created_by_import_batch_id, created_at, updated_at,
                       current_metadata_observation_id
                FROM sources ORDER BY source_id
                """,
            ),
            "source_observations": self._rows(
                connection,
                """
                SELECT source_metadata_observation_id, source_id, import_batch_id,
                       import_observation_id, observed_at, quality_rank, quality_basis,
                       candidate_sha256, parent_source_id, canonical_url, historical_url,
                       title, published_at, access_state, review_state, metadata_json
                FROM source_metadata_observations
                ORDER BY source_metadata_observation_id
                """,
            ),
            "recordings": self._rows(
                connection,
                """
                SELECT recording_id, canonical_key, slug, title, date_label, date_year,
                       date_basis, duration_ms, recording_type, review_state,
                       metadata_json, created_at, updated_at,
                       current_metadata_observation_id
                FROM recordings ORDER BY recording_id
                """,
            ),
            "recording_observations": self._rows(
                connection,
                """
                SELECT recording_metadata_observation_id, recording_id, import_batch_id,
                       import_observation_id, observed_at, quality_rank, quality_basis,
                       candidate_sha256, title, date_label, date_basis, duration_ms,
                       recording_type, review_state, metadata_json
                FROM recording_metadata_observations
                ORDER BY recording_metadata_observation_id
                """,
            ),
            "source_snapshots": self._rows(
                connection,
                """
                SELECT source_snapshot_id, source_id, observed_at, request_url, final_url,
                       http_status, payload_sha256, artifact_path, metadata_json,
                       import_batch_id
                FROM source_snapshots ORDER BY source_snapshot_id
                """,
            ),
            "source_relations": self._rows(
                connection,
                """
                SELECT source_relation_id, from_source_id, relation_kind, to_source_id,
                       basis, confidence_state, metadata_json, import_batch_id,
                       current_relation_observation_id
                FROM source_relations ORDER BY source_relation_id
                """,
            ),
            "source_relation_observations": self._rows(
                connection,
                "SELECT * FROM source_relation_observations "
                "ORDER BY source_relation_observation_id",
            ),
            "external_ids": self._rows(
                connection,
                "SELECT * FROM external_ids ORDER BY external_id_id",
            ),
            "external_id_observations": self._rows(
                connection,
                "SELECT * FROM external_id_observations ORDER BY external_id_observation_id",
            ),
            "recording_sources": self._rows(
                connection,
                "SELECT * FROM recording_sources ORDER BY recording_source_id",
            ),
            "review_tasks": self._rows(
                connection,
                "SELECT * FROM review_tasks ORDER BY review_task_id",
            ),
        }

    def test_importer_permutations_have_identical_projection_and_all_observations(self):
        operations = {
            "candidate": lambda connection: import_youtube_discovery_candidates(
                connection,
                self.candidate,
                observed_at="2026-08-25T00:00:00Z",
                query_label="order-invariance fixture",
            ),
            "channel": lambda connection: import_current_channel(
                connection,
                self.inventory,
                snapshot_date="2026-08-20",
                observed_at="2026-08-20T12:00:00Z",
            ),
            "old_info": lambda connection: import_ytdlp_info(
                connection, self.old_info, observed_at="2026-08-19T12:00:00Z"
            ),
            "corrected_info": lambda connection: import_ytdlp_info(
                connection, self.corrected_info, observed_at="2026-08-21T12:00:00Z"
            ),
        }
        baseline = None
        for index, order in enumerate(itertools.permutations(operations)):
            connection = self._database(f"permutation-{index}")
            for operation in order:
                operations[operation](connection)
            projection = self._projection(connection)
            if baseline is None:
                baseline = projection
            else:
                self.assertEqual(projection, baseline, order)

            video_source = source_id("youtube", "youtube_video", VIDEO_ID)
            source_row = connection.execute(
                "SELECT * FROM sources WHERE source_id = ?", (video_source,)
            ).fetchone()
            self.assertEqual(source_row["title"], "Corrected direct title")
            self.assertEqual(source_row["access_state"], "members_only")
            # The low-quality search capture is the latest observation but cannot
            # replace a direct platform assertion.
            self.assertEqual(source_row["observed_at"], "2026-08-25T00:00:00Z")
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM source_metadata_observations WHERE source_id = ?",
                    (video_source,),
                ).fetchone()[0],
                4,
            )
            recording = recording_id(f"youtube:video:{VIDEO_ID}")
            recording_row = connection.execute(
                "SELECT * FROM recordings WHERE recording_id = ?", (recording,)
            ).fetchone()
            self.assertEqual(recording_row["title"], "Corrected direct title")
            self.assertEqual(recording_row["duration_ms"], 80_000)
            self.assertEqual(recording_row["recording_type"], "livestream")
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM recording_metadata_observations WHERE recording_id = ?",
                    (recording,),
                ).fetchone()[0],
                4,
            )
            # Restricted current access prevents even the metadata policy from
            # creating a publish decision; missing gates also remain fail closed.
            self.assertEqual(
                approve_public_source_metadata(connection)["public_recordings"], 0
            )
            self.assertEqual(
                connection.execute("SELECT count(*) FROM public_sources").fetchone()[0], 0
            )

    def test_same_input_at_new_time_is_a_new_observation_but_exact_replay_is_not(self):
        connection = self._database("repeat-observations")
        first = import_current_channel(
            connection,
            self.inventory,
            snapshot_date="2026-08-20",
            observed_at="2026-08-20T12:00:00Z",
        )
        second = import_current_channel(
            connection,
            self.inventory,
            snapshot_date="2026-08-21",
            observed_at="2026-08-21T12:00:00Z",
        )
        before_replay = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "import_batches",
                "import_observations",
                "source_snapshots",
                "source_metadata_observations",
                "recording_metadata_observations",
            )
        }
        self.assertEqual(
            import_current_channel(
                connection,
                self.inventory,
                snapshot_date="2026-08-21",
                observed_at="2026-08-21T12:00:00Z",
            ),
            second,
        )
        self.assertEqual(first, second)
        self.assertEqual(
            {
                table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in before_replay
            },
            before_replay,
        )
        self.assertEqual(before_replay["import_batches"], 1)
        self.assertEqual(before_replay["import_observations"], 2)
        self.assertEqual(before_replay["source_snapshots"], 4)
        self.assertEqual(before_replay["source_metadata_observations"], 4)
        self.assertEqual(before_replay["recording_metadata_observations"], 2)
        batch = connection.execute("SELECT * FROM import_batches").fetchone()
        self.assertEqual(batch["started_at"], "2026-08-20T12:00:00Z")
        self.assertEqual(batch["completed_at"], "2026-08-21T12:00:00Z")
        source_observation = connection.execute(
            "SELECT source_metadata_observation_id FROM source_metadata_observations LIMIT 1"
        ).fetchone()[0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "UPDATE source_metadata_observations SET quality_rank = quality_rank + 1 "
                "WHERE source_metadata_observation_id = ?",
                (source_observation,),
            )
        recording_observation = connection.execute(
            "SELECT recording_metadata_observation_id "
            "FROM recording_metadata_observations LIMIT 1"
        ).fetchone()[0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "DELETE FROM recording_metadata_observations "
                "WHERE recording_metadata_observation_id = ?",
                (recording_observation,),
            )

    def test_same_time_conflict_uses_restrictive_access_tie_break(self):
        public_info = self._write_info(
            "tie-public.info.json",
            title="Public tie",
            availability="public",
            duration=10,
            upload_date="20260822",
            was_live=False,
        )
        private_info = self._write_info(
            "tie-private.info.json",
            title="Private tie",
            availability="private",
            duration=11,
            upload_date="20260822",
            was_live=False,
        )
        for index, paths in enumerate(((public_info, private_info), (private_info, public_info))):
            connection = self._database(f"tie-{index}")
            for path in paths:
                import_ytdlp_info(
                    connection, path, observed_at="2026-08-22T12:00:00Z"
                )
            row = connection.execute(
                "SELECT access_state FROM sources WHERE source_id = ?",
                (source_id("youtube", "youtube_video", VIDEO_ID),),
            ).fetchone()
            self.assertEqual(row["access_state"], "private")
            self.assertEqual(approve_public_source_metadata(connection)["public_sources"], 0)

    def test_database_validation_recomputes_the_current_winner(self):
        connection = self._database("winner-validation")
        import_youtube_discovery_candidates(
            connection,
            self.candidate,
            observed_at="2026-08-25T00:00:00Z",
            query_label="winner validation fixture",
        )
        import_current_channel(
            connection,
            self.inventory,
            snapshot_date="2026-08-20",
            observed_at="2026-08-20T12:00:00Z",
        )
        validate_database(connection)
        video_source = source_id("youtube", "youtube_video", VIDEO_ID)
        losing_observation = connection.execute(
            """
            SELECT source_metadata_observation_id
            FROM source_metadata_observations
            WHERE source_id = ?
            ORDER BY quality_rank, source_metadata_observation_id
            LIMIT 1
            """,
            (video_source,),
        ).fetchone()[0]
        connection.execute(
            "UPDATE sources SET current_metadata_observation_id = ? WHERE source_id = ?",
            (losing_observation, video_source),
        )
        with self.assertRaisesRegex(RuntimeError, "Current metadata winner"):
            validate_database(connection)


if __name__ == "__main__":
    unittest.main()
