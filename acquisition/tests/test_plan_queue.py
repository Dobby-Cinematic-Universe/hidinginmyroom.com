from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROGRAM = REPOSITORY_ROOT / "acquisition" / "plan_queue.py"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
PLAN_SCHEMA = REPOSITORY_ROOT / "acquisition" / "schemas" / "queue-plan.schema.json"


def run(
    arguments: list[str], *, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", str(PROGRAM), *arguments],
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


class QueuePlannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-queue-test-")
        self.root = Path(self.temporary.name)
        self.database = self.root / "catalog.sqlite3"
        self.wiki = self.root / "wiki"
        self.wiki.mkdir()
        self.selection = self.root / "selection.json"
        self._create_catalog()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _create_catalog(self) -> None:
        connection = sqlite3.connect(self.database)
        connection.executescript(
            """
            CREATE TABLE schema_migrations(
                version INTEGER PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL
            );
            CREATE TABLE sources(
                source_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                native_id TEXT NOT NULL,
                title TEXT,
                canonical_url TEXT,
                published_at TEXT,
                access_state TEXT NOT NULL,
                review_state TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );
            CREATE TABLE recordings(
                recording_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                duration_ms INTEGER,
                recording_type TEXT NOT NULL,
                review_state TEXT NOT NULL,
                merged_into_recording_id TEXT
            );
            CREATE TABLE recording_sources(
                recording_id TEXT NOT NULL,
                source_id TEXT NOT NULL,
                mapping_role TEXT NOT NULL
            );
            CREATE TABLE media_sources(
                media_id TEXT NOT NULL,
                source_id TEXT NOT NULL
            );
            CREATE TABLE renditions(
                recording_id TEXT NOT NULL,
                media_id TEXT NOT NULL
            );
            CREATE TABLE source_hashes(
                source_id TEXT NOT NULL,
                algorithm TEXT NOT NULL,
                digest TEXT NOT NULL,
                observed_at TEXT NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT INTO schema_migrations VALUES(1, '0001_fixture.sql', ?)",
            ("a" * 64,),
        )

        def recording(
            recording_id: str,
            duration_ms: int | None,
            *,
            review_state: str = "unreviewed",
        ) -> None:
            connection.execute(
                "INSERT INTO recordings VALUES(?, ?, ?, 'video', ?, NULL)",
                (recording_id, f"Title {recording_id}", duration_ms, review_state),
            )

        def source(
            source_id: str,
            recording_id: str,
            platform: str,
            source_kind: str,
            native_id: str,
            metadata: dict,
            *,
            role: str,
            access_state: str = "public",
            review_state: str = "unreviewed",
        ) -> None:
            connection.execute(
                """
                INSERT INTO sources VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
                """,
                (
                    source_id,
                    platform,
                    source_kind,
                    native_id,
                    f"Source {source_id}",
                    "https://example.invalid/signed?secret=must-not-survive",
                    access_state,
                    review_state,
                    json.dumps(metadata, sort_keys=True),
                ),
            )
            connection.execute(
                "INSERT INTO recording_sources VALUES(?, ?, ?)",
                (recording_id, source_id, role),
            )

        recording("rec_youtube", 60_000)
        source(
            "src_youtube",
            "rec_youtube",
            "youtube",
            "youtube_video",
            "abcDEF12345",
            {"duration_ms": 60_000},
            role="current_platform_listing",
        )
        # A second role must remain deterministically sorted and must not duplicate
        # the candidate.
        connection.execute(
            "INSERT INTO recording_sources VALUES('rec_youtube', 'src_youtube', 'validated_platform_listing')"
        )

        recording("rec_archive", 90_000)
        source(
            "src_archive_derivative",
            "rec_archive",
            "internet_archive",
            "archive_media_file",
            "item-one/movie.mp4.ia.mp4",
            {"byte_count": 500_000, "source_class": "derivative", "duration_ms": 90_000},
            role="archive_derivative",
        )
        source(
            "src_archive_original",
            "rec_archive",
            "internet_archive",
            "archive_media_file",
            "item-one/movie.mp4",
            {"byte_count": 1_000_000, "source_class": "original", "duration_ms": 90_000},
            role="primary_archive_file",
        )
        connection.execute(
            "INSERT INTO source_hashes VALUES('src_archive_original', 'sha256', ?, '2026-08-26T00:00:00Z')",
            ("b" * 64,),
        )

        recording("rec_long", 8_000_000)
        source(
            "src_long",
            "rec_long",
            "internet_archive",
            "archive_media_file",
            "item-long/long.mp4",
            {"byte_count": 2_000_000, "source_class": "original", "duration_ms": 8_000_000},
            role="primary_archive_file",
        )

        recording("rec_unknown", None)
        source(
            "src_unknown",
            "rec_unknown",
            "youtube",
            "youtube_video",
            "unknOWN1234",
            {},
            role="current_platform_listing",
        )

        recording("rec_disputed", 20_000, review_state="disputed")
        source(
            "src_disputed",
            "rec_disputed",
            "youtube",
            "youtube_video",
            "disPUTE1234",
            {"duration_ms": 20_000},
            role="discovery_candidate",
        )

        recording("rec_members", 30_000)
        source(
            "src_members",
            "rec_members",
            "youtube",
            "youtube_video",
            "memberS1234",
            {"duration_ms": 30_000},
            role="current_platform_listing",
            access_state="members_only",
        )

        recording("rec_acquired", 10_000)
        source(
            "src_acquired",
            "rec_acquired",
            "youtube",
            "youtube_video",
            "acquireD123",
            {"duration_ms": 10_000},
            role="current_platform_listing",
        )
        connection.execute("INSERT INTO renditions VALUES('rec_acquired', 'media_one')")
        connection.commit()
        connection.close()

    def arguments(self, *extra: str) -> list[str]:
        return [
            "--db",
            str(self.database.resolve()),
            "--wiki-root",
            str(self.wiki.resolve()),
            "--planned-at",
            "2026-08-26T20:00:00Z",
            "--max-items",
            "10",
            "--plan-budget-bytes",
            str(10 * 1024**3),
            *extra,
        ]

    def test_deterministic_public_only_priority_and_routing(self) -> None:
        (self.wiki / "event.mdx").write_text(
            "[evidence](https://www.youtube.com/watch?v=abcDEF12345)\n",
            encoding="utf-8",
        )
        self.selection.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "purpose": "fixture cohort",
                    "youtube_video_ids": ["memberS1234"],
                    "source_ids": [],
                    "recording_ids": [],
                }
            ),
            encoding="utf-8",
        )
        arguments = self.arguments(
            "--selection-manifest",
            str(self.selection.resolve()),
        )
        first = run(arguments)
        second = run(arguments)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(first.stdout, second.stdout)
        plan = json.loads(first.stdout)

        by_recording = {item["recording_id"]: item for item in plan["candidates"]}
        self.assertNotIn("rec_members", by_recording)
        self.assertNotIn("rec_acquired", by_recording)
        self.assertEqual(
            plan["selection_basis"]["requested_identifiers_not_eligible"],
            ["memberS1234"],
        )
        self.assertEqual(plan["summary"]["withheld_access_sources"], 1)
        self.assertEqual(plan["summary"]["already_acquired_recordings"], 1)

        youtube = by_recording["rec_youtube"]
        self.assertEqual(youtube["priority_tier"], "wiki_cited")
        self.assertEqual(youtube["canonical_url"], "https://www.youtube.com/watch?v=abcDEF12345")
        self.assertNotIn("secret", first.stdout)
        self.assertEqual(
            youtube["mapping_roles"],
            ["current_platform_listing", "validated_platform_listing"],
        )

        archive = by_recording["rec_archive"]
        self.assertEqual(archive["source_id"], "src_archive_original")
        self.assertEqual(archive["expected_byte_count"], 1_000_000)
        self.assertEqual(archive["expected_sha256"], "b" * 64)
        self.assertEqual(archive["estimate_basis"], "provider_declared_byte_count")
        self.assertEqual(by_recording["rec_long"]["queue_state"], "requires_chunking")
        self.assertEqual(by_recording["rec_unknown"]["queue_state"], "requires_metadata")
        self.assertEqual(by_recording["rec_disputed"]["queue_state"], "review_required")
        self.assertFalse(plan["safety"]["network_access_performed"])
        self.assertFalse(plan["safety"]["catalog_mutated"])

        output = self.root / "plan.json"
        output.write_text(first.stdout, encoding="utf-8")
        contract = subprocess.run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(PLAN_SCHEMA),
                str(output),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(contract.returncode, 0, contract.stdout + contract.stderr)

    def test_budget_and_item_limits_are_explicit(self) -> None:
        completed = run(
            self.arguments(
                "--max-items",
                "1",
                "--plan-budget-bytes",
                str(2 * 1024**3),
            )
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        plan = json.loads(completed.stdout)
        self.assertEqual(plan["summary"]["selected_count"], 1)
        selected = [item for item in plan["candidates"] if item["queue_ordinal"]]
        self.assertEqual(len(selected), 1)
        ready_deferred = [
            item
            for item in plan["candidates"]
            if item["queue_state"] == "ready" and item["queue_ordinal"] is None
        ]
        self.assertTrue(ready_deferred)
        self.assertTrue(
            all(item["defer_reason"] == "plan_item_limit" for item in ready_deferred)
        )

    def test_selection_manifest_rejects_unknown_fields(self) -> None:
        self.selection.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "purpose": "bad fixture",
                    "youtube_video_ids": [],
                    "source_ids": [],
                    "recording_ids": [],
                    "cookie_file": "/tmp/forbidden",
                }
            ),
            encoding="utf-8",
        )
        completed = run(
            self.arguments("--selection-manifest", str(self.selection.resolve()))
        )
        self.assertEqual(completed.returncode, 2)
        self.assertEqual(completed.stdout, "")
        error = json.loads(completed.stderr)
        self.assertIn("unknown=['cookie_file']", error["error"]["message"])

    def test_selection_manifest_can_be_read_from_stdin(self) -> None:
        selection = json.dumps(
            {
                "schema_version": 1,
                "purpose": "stdin fixture",
                "youtube_video_ids": ["abcDEF12345"],
                "source_ids": [],
                "recording_ids": [],
            }
        )
        completed = run(
            self.arguments("--selection-manifest", "-"), input_text=selection
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        plan = json.loads(completed.stdout)
        candidate = next(
            item
            for item in plan["candidates"]
            if item["recording_id"] == "rec_youtube"
        )
        self.assertEqual(candidate["priority_tier"], "explicit_selection")
        self.assertEqual(
            plan["selection_basis"]["manifest_sha256"],
            hashlib.sha256(selection.encode("utf-8")).hexdigest(),
        )

    def test_exact_source_selection_outranks_recording_level_fallback(self) -> None:
        selection = json.dumps(
            {
                "schema_version": 1,
                "purpose": "source specificity fixture",
                "youtube_video_ids": [],
                "source_ids": ["src_archive_derivative"],
                "recording_ids": ["rec_archive"],
            }
        )
        completed = run(
            self.arguments("--selection-manifest", "-"), input_text=selection
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        plan = json.loads(completed.stdout)
        candidate = next(
            item
            for item in plan["candidates"]
            if item["recording_id"] == "rec_archive"
        )
        self.assertEqual(candidate["source_id"], "src_archive_derivative")
        self.assertIn("explicit_source_id", candidate["reason_codes"])
        self.assertIn("explicit_recording_id", candidate["reason_codes"])

    def test_selection_only_does_not_fill_with_unselected_backlog(self) -> None:
        selection = json.dumps(
            {
                "schema_version": 1,
                "purpose": "bounded cohort fixture",
                "youtube_video_ids": ["abcDEF12345"],
                "source_ids": [],
                "recording_ids": [],
            }
        )
        completed = run(
            self.arguments("--selection-manifest", "-", "--selection-only"),
            input_text=selection,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        plan = json.loads(completed.stdout)
        self.assertTrue(plan["limits"]["selection_only"])
        self.assertEqual(plan["summary"]["selected_count"], 1)
        self.assertEqual(
            [item["native_id"] for item in plan["candidates"] if item["queue_ordinal"]],
            ["abcDEF12345"],
        )
        unselected_ready = [
            item
            for item in plan["candidates"]
            if item["queue_state"] == "ready"
            and item["priority_tier"] != "explicit_selection"
        ]
        self.assertTrue(unselected_ready)
        self.assertTrue(
            all(
                item["defer_reason"] == "outside_explicit_selection"
                for item in unselected_ready
            )
        )

    def test_read_only_database_is_unchanged(self) -> None:
        before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        completed = run(self.arguments())
        after = hashlib.sha256(self.database.read_bytes()).hexdigest()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
