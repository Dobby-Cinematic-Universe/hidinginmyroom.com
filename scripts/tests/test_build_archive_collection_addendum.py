from __future__ import annotations

import io
import json
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(REPOSITORY_ROOT / "corpus" / "src"))

from acquisition.archive_org_metadata import (  # noqa: E402
    capture_snapshot,
    make_writable,
    stable_id as archive_stable_id,
)
from acquisition.tests.test_materialize_queue import fixture_plan  # noqa: E402
from himr_corpus.ids import source_id  # noqa: E402
from scripts.build_archive_collection_addendum import (  # noqa: E402
    ArchiveCollectionAddendumError,
    build_addendum,
    pretty_bytes,
    sha256_bytes,
    terminal_youtube_id,
)


class _Response(io.BytesIO):
    status = 200

    def __init__(self, body: bytes, identifier: str):
        super().__init__(body)
        self._url = f"https://archive.org/metadata/{identifier}"
        self.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "ETag": '"fixture"',
            "Last-Modified": "Sun, 30 Aug 2026 12:00:00 GMT",
            "Date": "Sun, 30 Aug 2026 12:00:01 GMT",
        }

    def getcode(self) -> int:
        return 200

    def geturl(self) -> str:
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class _Opener:
    def __init__(self, body: bytes, identifier: str):
        self.body = body
        self.identifier = identifier

    def open(self, _request, timeout):
        self.timeout = timeout
        return _Response(self.body, self.identifier)


class _Clock:
    def __init__(self, *values: str):
        self.values = iter(values)

    def __call__(self) -> str:
        return next(self.values)


def _archive_file(
    name: str,
    *,
    size: int,
    sha1: str,
    md5: str,
) -> dict[str, str]:
    return {
        "name": name,
        "source": "original",
        "size": str(size),
        "sha1": sha1,
        "md5": md5,
        "format": "MPEG4" if name.endswith(".mp4") else "WebM",
        "length": "10.0",
    }


class ArchiveCollectionAddendumTests(unittest.TestCase):
    collection = "fixture-new"

    def setUp(self) -> None:
        work = REPOSITORY_ROOT / "corpus" / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="archive-addendum-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.new_files = [
            _archive_file(
                "20240101-New-abcDEF12345.mp4",
                size=100,
                sha1="1" * 40,
                md5="a" * 32,
            ),
            _archive_file(
                "00000000-Intro.mp4",
                size=20,
                sha1="2" * 40,
                md5="b" * 32,
            ),
            _archive_file(
                "same-recording.mp4",
                size=30,
                sha1="6" * 40,
                md5="f" * 32,
            ),
            _archive_file(
                "unique-new.mp4",
                size=40,
                sha1="5" * 40,
                md5="e" * 32,
            ),
            _archive_file(
                "same-title.mp4",
                size=50,
                sha1="4" * 40,
                md5="d" * 32,
            ),
            _archive_file(
                "duplicate-small-DUPid_12345.mp4",
                size=60,
                sha1="7" * 40,
                md5="1" * 32,
            ),
            _archive_file(
                "duplicate-large-DUPid_12345.webm",
                size=70,
                sha1="8" * 40,
                md5="2" * 32,
            ),
        ]
        self.old_files = [
            _archive_file(
                "Old title [abcDEF12345].mp4",
                size=101,
                sha1="9" * 40,
                md5="3" * 32,
            ),
            _archive_file(
                "00000000-Intro.mp4",
                size=20,
                sha1="2" * 40,
                md5="b" * 32,
            ),
            _archive_file(
                "same-title.mp4",
                size=51,
                sha1="3" * 40,
                md5="c" * 32,
            ),
        ]
        self.new_snapshot = self._snapshot(
            self.collection,
            self.new_files,
            "2026-08-30T12:00:00Z",
            "2026-08-30T12:00:01Z",
            "2026-08-30T12:00:02Z",
            "new-snapshots",
        )
        self.old_snapshot = self._snapshot(
            "fixture-old",
            self.old_files,
            "2026-08-30T11:00:00Z",
            "2026-08-30T11:00:01Z",
            "2026-08-30T11:00:02Z",
            "old-snapshots",
        )
        self.plan_path = self.root / "predecessor-plan.json"
        self.plan_path.write_bytes(pretty_bytes(fixture_plan()))
        self.plan_path.chmod(0o400)
        self.database = self.root / "successor.sqlite3"
        self._database(self.database)
        self.selection_path = self.root / "output" / "selection.json"
        self.report_path = self.root / "output" / "overlap-report.json"
        self.selection_path.parent.mkdir()

    def tearDown(self) -> None:
        make_writable(self.root)
        self.temporary.cleanup()

    def _snapshot(
        self,
        identifier: str,
        files: list[dict[str, str]],
        requested_at: str,
        started_at: str,
        observed_at: str,
        output_name: str,
    ) -> Path:
        request_body = {
            "schema_version": 1,
            "request_kind": "archive_org_metadata_targets",
            "requested_at": requested_at,
            "items": [{"identifier": identifier, "basis": "manual_public_lead"}],
            "policy": {
                "public_unauthenticated_metadata_only": True,
                "media_download": False,
                "cookies_sent": False,
                "authorization_sent": False,
                "publication_authority": False,
            },
        }
        request_body["request_id"] = archive_stable_id("iamr", request_body)
        request = self.root / f"request-{identifier}.json"
        request.write_bytes(pretty_bytes(request_body))
        payload = json.dumps(
            {
                "metadata": {
                    "identifier": identifier,
                    "title": f"Provider title {identifier}",
                    "collection": ["fixture"],
                },
                "files": files,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        return capture_snapshot(
            request,
            output_root=self.root / output_name,
            opener_factory=lambda requested_identifier: _Opener(
                payload, requested_identifier
            ),
            clock=_Clock(started_at, observed_at),
        )

    def _database(self, path: Path) -> None:
        connection = sqlite3.connect(path)
        connection.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
            INSERT INTO schema_migrations VALUES(1);
            CREATE TABLE sources(
              source_id TEXT PRIMARY KEY,
              platform TEXT NOT NULL,
              source_kind TEXT NOT NULL,
              native_id TEXT NOT NULL,
              access_state TEXT NOT NULL,
              metadata_json TEXT NOT NULL
            );
            CREATE TABLE recordings(
              recording_id TEXT PRIMARY KEY,
              merged_into_recording_id TEXT REFERENCES recordings(recording_id)
            );
            CREATE TABLE recording_sources(
              recording_id TEXT NOT NULL REFERENCES recordings(recording_id),
              source_id TEXT NOT NULL REFERENCES sources(source_id)
            );
            CREATE TABLE source_hashes(
              source_id TEXT NOT NULL REFERENCES sources(source_id),
              algorithm TEXT NOT NULL,
              digest TEXT NOT NULL,
              declared_by TEXT NOT NULL
            );
            """
        )
        recordings_by_filename = {
            "20240101-New-abcDEF12345.mp4": "rec_new_id",
            "00000000-Intro.mp4": "rec_intro",
            "same-recording.mp4": "rec_archive",
            "unique-new.mp4": "rec_unique",
            "same-title.mp4": "rec_same_title_but_not_same_evidence",
            "duplicate-small-DUPid_12345.mp4": "rec_duplicate",
            "duplicate-large-DUPid_12345.webm": "rec_duplicate",
        }
        for recording_id in sorted(set(recordings_by_filename.values())):
            connection.execute(
                "INSERT INTO recordings VALUES(?, NULL)", (recording_id,)
            )
        for file_record in self.new_files:
            filename = file_record["name"]
            native_id = f"{self.collection}/{filename}"
            source = source_id(
                "internet_archive", "archive_media_file", native_id
            )
            metadata = {
                "internet_archive_item": self.collection,
                "filename": filename,
                "format": file_record["format"],
                "source_class": "original",
                "derivative_of": None,
                "byte_count": int(file_record["size"]),
                "duration_ms": 10_000,
            }
            connection.execute(
                "INSERT INTO sources VALUES(?, 'internet_archive', "
                "'archive_media_file', ?, 'public', ?)",
                (
                    source,
                    native_id,
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            connection.execute(
                "INSERT INTO recording_sources VALUES(?, ?)",
                (recordings_by_filename[filename], source),
            )
            for algorithm in ("sha1", "md5"):
                connection.execute(
                    "INSERT INTO source_hashes VALUES(?, ?, ?, "
                    "'internet_archive_metadata')",
                    (source, algorithm, file_record[algorithm]),
                )
        connection.commit()
        connection.close()

    def _digest(self, path: Path) -> str:
        return sha256_bytes(path.read_bytes())

    def _arguments(self) -> dict:
        return {
            "database": self.database,
            "expected_database_sha256": self._digest(self.database),
            "new_snapshot_path": self.new_snapshot,
            "expected_new_snapshot_sha256": self._digest(self.new_snapshot),
            "predecessor_plan_path": self.plan_path,
            "expected_predecessor_plan_sha256": self._digest(self.plan_path),
            "predecessor_snapshot_path": self.old_snapshot,
            "expected_predecessor_snapshot_sha256": self._digest(
                self.old_snapshot
            ),
            "collection_identifier": self.collection,
            "purpose": "fixture exact-evidence addendum",
            "selection_output": self.selection_path,
            "report_output": self.report_path,
        }

    def test_exact_evidence_excludes_overlap_without_title_matching(self) -> None:
        first = build_addendum(**self._arguments())
        selection = json.loads(self.selection_path.read_text(encoding="utf-8"))
        report = json.loads(self.report_path.read_text(encoding="utf-8"))

        self.assertEqual(first["selection"]["source_id_count"], 3)
        self.assertEqual(report["counts"]["new_snapshot_original_video_sources"], 7)
        self.assertEqual(report["counts"]["new_logical_recordings"], 6)
        self.assertEqual(report["counts"]["covered_logical_recordings"], 3)
        self.assertEqual(report["counts"]["selected_logical_recordings"], 3)
        self.assertEqual(
            report["counts"]["overlap_logical_recordings_by_method"],
            {
                "exact_provider_md5": 1,
                "exact_provider_sha1": 1,
                "exact_recording_id": 1,
                "exact_terminal_youtube_id": 1,
            },
        )
        intro = next(
            overlap
            for overlap in report["overlaps"]
            if any(
                row["filename"] == "00000000-Intro.mp4"
                for row in overlap["new_sources"]
            )
        )
        self.assertEqual(
            intro["match_methods"], ["exact_provider_md5", "exact_provider_sha1"]
        )
        selected_native_ids = {
            row["selected_source"]["native_id"]
            for row in report["selected_groups"]
        }
        self.assertIn(f"{self.collection}/same-title.mp4", selected_native_ids)
        self.assertIn(
            f"{self.collection}/duplicate-small-DUPid_12345.mp4",
            selected_native_ids,
        )
        self.assertNotIn(
            f"{self.collection}/duplicate-large-DUPid_12345.webm",
            selected_native_ids,
        )
        self.assertTrue(report["equivalence_policy"]["titles_examined"] is False)
        self.assertEqual(stat.S_IMODE(self.selection_path.stat().st_mode), 0o400)
        self.assertEqual(stat.S_IMODE(self.report_path.stat().st_mode), 0o400)
        self.assertEqual(self.selection_path.read_bytes(), pretty_bytes(selection))
        self.assertEqual(self.report_path.read_bytes(), pretty_bytes(report))

        second = build_addendum(**self._arguments())
        self.assertTrue(second["selection"]["reused"])
        self.assertTrue(second["overlap_report"]["reused"])
        self.assertEqual(second["overlap_report"]["report_id"], report["report_id"])

    def test_terminal_id_parser_accepts_only_exact_terminal_forms(self) -> None:
        self.assertEqual(
            terminal_youtube_id("Title-abcDEF12345.mp4"), "abcDEF12345"
        )
        self.assertEqual(
            terminal_youtube_id("Title [abcDEF12345].ia.mp4"), "abcDEF12345"
        )
        self.assertEqual(
            terminal_youtube_id("Title--tqBDUcWYzk.mp4"), "-tqBDUcWYzk"
        )
        self.assertIsNone(terminal_youtube_id("abcDEF12345.mp4"))
        self.assertIsNone(terminal_youtube_id("abcDEF12345 appears in title.mp4"))

    def test_catalog_hash_mismatch_and_sidecars_fail_closed(self) -> None:
        arguments = self._arguments()
        arguments["expected_database_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            ArchiveCollectionAddendumError, "successor catalog SHA-256 differs"
        ):
            build_addendum(**arguments)

        arguments = self._arguments()
        sidecar = Path(f"{self.database}-shm")
        sidecar.write_bytes(b"")
        with self.assertRaisesRegex(
            ArchiveCollectionAddendumError, "no SQLite sidecars"
        ):
            build_addendum(**arguments)


if __name__ == "__main__":
    unittest.main()
