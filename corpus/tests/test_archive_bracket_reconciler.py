from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from acquisition.archive_org_metadata import (  # noqa: E402
    capture_snapshot,
    make_writable,
    stable_id as producer_stable_id,
)
from himr_corpus.archive_bracket_reconciler import (  # noqa: E402
    ArchiveBracketReconciliationError,
    build_archive_bracket_reconciliation_plan,
    import_archive_bracket_reconciliation,
    terminal_bracketed_youtube_id,
)
from himr_corpus.archive_metadata_snapshot_importer import (  # noqa: E402
    ArchiveMetadataSnapshotImportError,
    import_archive_metadata_snapshot,
)
from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate, transaction  # noqa: E402
from himr_corpus.ids import recording_id, source_id  # noqa: E402
from himr_corpus.importers import (  # noqa: E402
    _attach_recording_source,
    _begin_batch,
    _complete_batch,
    _upsert_recording,
    _upsert_source,
)


UNIQUE_ID = "AbCdEfGhI_1"
MISSING_ID = "ZyXwVuTsRq0"
AMBIGUOUS_ID = "AmbigID1234"
CONFLICT_FILENAME_ID = "abcdefghijk"
CONFLICT_TITLE_ID = "lmnopqrstuv"

PAYLOAD_OBJECT = {
    "metadata": {
        "identifier": "fixture-bracket-item",
        "title": "Fixture bracketed-ID item",
        "collection": ["fixture"],
    },
    "files": [
        {
            "name": f"first [{UNIQUE_ID}].mp4",
            "source": "original",
            "format": "MPEG4",
            "size": "1000",
            "length": "60",
        },
        {
            "name": f"first [{UNIQUE_ID}].ia.mp4",
            "source": "derivative",
            "original": f"first [{UNIQUE_ID}].mp4",
            "format": "h.264",
            "size": "500",
            "length": "60",
        },
        {
            "name": f"second [{UNIQUE_ID}].webm",
            "source": "original",
            "format": "WebM",
            "size": "900",
            "length": "59",
        },
        {
            "name": "title-only.mp4",
            "title": f"Provider title [{MISSING_ID}]",
            "source": "original",
            "format": "MPEG4",
            "size": "800",
            "length": "30",
        },
        {
            "name": f"ambiguous [{AMBIGUOUS_ID}].mp4",
            "source": "original",
            "format": "MPEG4",
            "size": "700",
            "length": "20",
        },
        {
            "name": f"conflict [{CONFLICT_FILENAME_ID}].mp4",
            "title": f"Different provider title [{CONFLICT_TITLE_ID}]",
            "source": "original",
            "format": "MPEG4",
            "size": "600",
            "length": "10",
        },
        {
            "name": f"middle [{UNIQUE_ID}] trailing.mp4",
            "source": "original",
            "format": "MPEG4",
            "size": "500",
            "length": "10",
        },
        {"name": "fixture-bracket-item_meta.sqlite", "source": "metadata"},
    ],
}
PAYLOAD = json.dumps(
    PAYLOAD_OBJECT, ensure_ascii=False, sort_keys=True
).encode("utf-8")


class FakeResponse(io.BytesIO):
    status = 200

    def __init__(self, url: str):
        super().__init__(PAYLOAD)
        self._url = url
        self.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(PAYLOAD)),
            "ETag": '"fixture-brackets"',
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
        self.values = iter(["2026-08-26T12:00:01Z", "2026-08-26T12:00:02Z"])

    def __call__(self):
        return next(self.values)


class ArchiveBracketReconcilerTests(unittest.TestCase):
    def setUp(self):
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="archive-bracket-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        request = {
            "schema_version": 1,
            "request_kind": "archive_org_metadata_targets",
            "requested_at": "2026-08-26T12:00:00Z",
            "items": [
                {"identifier": "fixture-bracket-item", "basis": "manual_public_lead"}
            ],
            "policy": {
                "public_unauthenticated_metadata_only": True,
                "media_download": False,
                "cookies_sent": False,
                "authorization_sent": False,
                "publication_authority": False,
            },
        }
        request["request_id"] = producer_stable_id("iamr", request)
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

    def _seed_youtube(self, video_id: str, *, ambiguous: bool = False) -> None:
        observed_at = "2026-08-26T12:00:03Z"
        with transaction(self.connection):
            batch_id, existing = _begin_batch(
                self.connection,
                "youtube_discovery_candidates",
                (video_id.encode("ascii").hex() + "0" * 64)[:64],
                "2026-08-26",
                observed_at,
            )
            self.assertIsNone(existing)
            youtube_source = source_id("youtube", "youtube_video", video_id)
            _upsert_source(
                self.connection,
                source=youtube_source,
                platform="youtube",
                source_kind="youtube_video",
                native_id=video_id,
                canonical_url=f"https://www.youtube.com/watch?v={video_id}",
                title=f"Fixture {video_id}",
                observed_at=observed_at,
                access_state="public",
                review_state="unreviewed",
                batch_id=batch_id,
                metadata={"fixture": True},
            )
            target = _upsert_recording(
                self.connection,
                canonical_key=f"youtube:video:{video_id}",
                title=f"Fixture {video_id}",
                date_label=None,
                date_basis="youtube_native_id",
                duration=None,
                recording_type="video",
                observed_at=observed_at,
                batch_id=batch_id,
                review_state="unreviewed",
                metadata={"identity_basis": "stable_youtube_video_id"},
            )
            _attach_recording_source(
                self.connection,
                recording=target,
                source=youtube_source,
                role="platform_video",
                method="stable_youtube_video_id",
            )
            if ambiguous:
                other = _upsert_recording(
                    self.connection,
                    canonical_key=f"fixture:ambiguous:{video_id}",
                    title="Conflicting extra recording mapping",
                    date_label=None,
                    date_basis="fixture",
                    duration=None,
                    recording_type="video",
                    observed_at=observed_at,
                    batch_id=batch_id,
                    review_state="unreviewed",
                    metadata={"fixture": True},
                )
                _attach_recording_source(
                    self.connection,
                    recording=other,
                    source=youtube_source,
                    role="fixture_conflicting_mapping",
                    method="fixture",
                    confidence_state="candidate",
                )
            _complete_batch(self.connection, batch_id, observed_at, {"fixture": 1})

    def _prepare(self) -> None:
        import_archive_metadata_snapshot(self.connection, self.snapshot)
        self._seed_youtube(UNIQUE_ID)
        self._seed_youtube(AMBIGUOUS_ID, ambiguous=True)

    def _counts(self) -> dict[str, int]:
        return {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "sources",
                "recordings",
                "recording_sources",
                "source_relations",
                "recording_relations",
                "match_candidates",
                "review_tasks",
                "archive_bracket_reconciliation_imports",
                "archive_bracket_youtube_candidates",
                "archive_bracket_reconciliation_issues",
                "publication_decisions",
                "publication_gate_decisions",
                "identity_assertions",
                "claim_catalog_links",
            )
        }

    def test_terminal_parser_is_strict_and_adversarial(self):
        self.assertEqual(
            terminal_bracketed_youtube_id(f"title [{UNIQUE_ID}]"), UNIQUE_ID
        )
        self.assertEqual(
            terminal_bracketed_youtube_id(f"title [{UNIQUE_ID}].mp4"), UNIQUE_ID
        )
        self.assertEqual(
            terminal_bracketed_youtube_id(f"title [{UNIQUE_ID}].ia.MP4"), UNIQUE_ID
        )
        for rejected in (
            UNIQUE_ID,
            f"title-{UNIQUE_ID}.mp4",
            f"title [{UNIQUE_ID}] trailing.mp4",
            f"title [{UNIQUE_ID}] .mp4",
            f"title [{UNIQUE_ID}].txt",
            f"title [{UNIQUE_ID}] ",
            f"title [{UNIQUE_ID}]-1.mp4",
            "title [short].mp4",
            f"title [{UNIQUE_ID}x].mp4",
        ):
            self.assertIsNone(terminal_bracketed_youtube_id(rejected), rejected)

    def test_plan_requires_strict_snapshot_import_and_is_read_only(self):
        before = self._counts()
        with self.assertRaisesRegex(
            ArchiveBracketReconciliationError,
            "import-archive-metadata-snapshot first",
        ):
            build_archive_bracket_reconciliation_plan(self.connection, self.snapshot)
        self.assertEqual(before, self._counts())

        self._prepare()
        before = self._counts()
        first = build_archive_bracket_reconciliation_plan(self.connection, self.snapshot)
        second = build_archive_bracket_reconciliation_plan(self.connection, self.snapshot)
        self.assertEqual(first, second)
        self.assertEqual(before, self._counts())
        self.assertEqual(
            first["statistics"],
            {
                "archive_files_with_consistent_terminal_bracket_id": 5,
                "archive_files_with_any_terminal_bracket_evidence": 6,
                "provider_file_records_scanned": 8,
                "distinct_youtube_video_ids": 3,
                "candidates_total": 8,
                "candidate_archive_source_to_youtube_locator": 5,
                "candidate_archive_recording_to_youtube_recording": 2,
                "candidate_intra_archive_repeat": 1,
                "issues_conflicting_terminal_bracket_ids": 1,
                "review_tasks_total": 9,
                "recording_merges": 0,
                "recording_relations": 0,
                "source_or_recording_mutations": 0,
                "publication_decisions": 0,
                "identity_assertions": 0,
                "claims": 0,
            },
        )
        ambiguous = [
            value
            for value in first["candidates"]
            if value["evidence"]["youtube_video_id"] == AMBIGUOUS_ID
        ]
        self.assertEqual(len(ambiguous), 1)
        self.assertEqual(
            ambiguous[0]["evidence"]["resolution_state"],
            "native_source_without_unique_recording",
        )
        self.assertIsNone(ambiguous[0]["evidence"]["youtube_recording_id"])

    def test_explicit_caps_fail_closed_without_catalog_writes(self):
        self._prepare()
        before = self._counts()
        cases = (
            ("MAX_TOTAL_PROVIDER_FILE_RECORDS", "provider-file reconciliation cap"),
            ("MAX_BRACKET_EVIDENCE_FILES", "bracket evidence-file cap"),
            ("MAX_RECONCILIATION_CANDIDATES", "reconciliation-candidate cap"),
        )
        for constant, message in cases:
            with self.subTest(constant=constant), patch(
                f"himr_corpus.archive_bracket_reconciler.{constant}", 1
            ):
                with self.assertRaisesRegex(
                    ArchiveBracketReconciliationError, message
                ):
                    build_archive_bracket_reconciliation_plan(
                        self.connection, self.snapshot
                    )
                self.assertEqual(before, self._counts())

    def test_import_is_exactly_replayable_private_and_never_merges(self):
        self._prepare()
        protected_before = self._counts()
        first = import_archive_bracket_reconciliation(self.connection, self.snapshot)
        after_first = self._counts()
        second = import_archive_bracket_reconciliation(self.connection, self.snapshot)
        self.assertEqual(first, second)
        self.assertEqual(after_first, self._counts())
        self.assertEqual(after_first["match_candidates"] - protected_before["match_candidates"], 8)
        self.assertEqual(after_first["review_tasks"] - protected_before["review_tasks"], 9)
        self.assertEqual(after_first["archive_bracket_youtube_candidates"], 8)
        self.assertEqual(after_first["archive_bracket_reconciliation_issues"], 1)
        for table in (
            "sources",
            "recordings",
            "recording_sources",
            "source_relations",
            "recording_relations",
            "publication_decisions",
            "publication_gate_decisions",
            "identity_assertions",
            "claim_catalog_links",
        ):
            self.assertEqual(protected_before[table], after_first[table], table)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM public_sources").fetchone()[0],
            0,
        )
        self.assertTrue(
            all(
                row[0] == 1 and row[1] == 0 and row[2] == 0
                for row in self.connection.execute(
                    """
                    SELECT requires_human_review, relationship_asserted, merge_performed
                    FROM archive_bracket_youtube_candidates
                    """
                )
            )
        )

    def test_snapshot_and_catalog_tampering_fail_without_repair(self):
        self._prepare()
        import_archive_bracket_reconciliation(self.connection, self.snapshot)
        task = self.connection.execute(
            """
            SELECT review_task_id FROM archive_bracket_youtube_candidates
            ORDER BY match_candidate_id LIMIT 1
            """
        ).fetchone()[0]
        self.connection.execute(
            "UPDATE review_tasks SET reason = 'tampered reason' WHERE review_task_id = ?",
            (task,),
        )
        before = self._counts()
        with self.assertRaisesRegex(
            ArchiveBracketReconciliationError, "review task conflicts"
        ):
            import_archive_bracket_reconciliation(self.connection, self.snapshot)
        self.assertEqual(before, self._counts())
        self.assertEqual(
            self.connection.execute(
                "SELECT reason FROM review_tasks WHERE review_task_id = ?", (task,)
            ).fetchone()[0],
            "tampered reason",
        )

        payload = self.snapshot.parent / "item-fixture-bracket-item.metadata.json"
        payload.chmod(0o600)
        altered = PAYLOAD.replace(b"Fixture bracketed-ID item", b"Fixture bracketed-Id item")
        self.assertEqual(len(altered), len(PAYLOAD))
        payload.write_bytes(altered)
        payload.chmod(0o400)
        with self.assertRaisesRegex(
            ArchiveMetadataSnapshotImportError, "payload bytes differ"
        ):
            build_archive_bracket_reconciliation_plan(self.connection, self.snapshot)

    def test_raw_candidate_evidence_is_append_only(self):
        self._prepare()
        import_archive_bracket_reconciliation(self.connection, self.snapshot)
        match_id = self.connection.execute(
            "SELECT match_candidate_id FROM archive_bracket_youtube_candidates LIMIT 1"
        ).fetchone()[0]
        with self.assertRaisesRegex(
            Exception, "archive bracket match candidates are append-only"
        ):
            self.connection.execute(
                "UPDATE match_candidates SET metadata_json = '{}' WHERE match_candidate_id = ?",
                (match_id,),
            )
        with self.assertRaisesRegex(
            Exception, "archive bracket candidates are append-only"
        ):
            self.connection.execute(
                "DELETE FROM archive_bracket_youtube_candidates WHERE match_candidate_id = ?",
                (match_id,),
            )

    def test_cli_exposes_plan_and_import_commands(self):
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("plan-archive-bracket-reconciliation", choices)
        self.assertIn("import-archive-bracket-reconciliation", choices)


if __name__ == "__main__":
    unittest.main()
