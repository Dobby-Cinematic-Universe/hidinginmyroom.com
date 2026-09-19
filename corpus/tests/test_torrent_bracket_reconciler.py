from __future__ import annotations

import base64
import copy
import hashlib
import json
import math
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate, transaction  # noqa: E402
from himr_corpus.ids import source_id, stable_id  # noqa: E402
from himr_corpus.importers import (  # noqa: E402
    _attach_recording_source,
    _begin_batch,
    _complete_batch,
    _upsert_recording,
    _upsert_source,
    import_torrent_manifest,
)
from himr_corpus.torrent_bracket_reconciler import (  # noqa: E402
    SCOPED_DIRECTORY_LABELS,
    TorrentBracketReconciliationError,
    _parse_torrent,
    _stable_file,
    _verify_plan_rows,
    build_torrent_bracket_reconciliation_plan,
    import_torrent_bracket_reconciliation,
    terminal_bracketed_youtube_id_bytes,
)
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


UNIQUE_ID = "AbCdEfGhI_1"
MISSING_ID = "ZyXwVuTsRq0"
AMBIGUOUS_ID = "AmbigID1234"
RAW_ID = "RawByte1234"
IGNORED_ID = "Ignored1234"
OBSERVED_AT = "2026-08-26T18:20:00Z"


def bencode(value) -> bytes:
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    if isinstance(value, str):
        return bencode(value.encode("utf-8"))
    if isinstance(value, bool):
        raise TypeError("booleans are not bencode integers")
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii") + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        return b"d" + b"".join(
            bencode(key) + bencode(value[key]) for key in sorted(value)
        ) + b"e"
    raise TypeError(type(value))


class TorrentBracketReconcilerTests(unittest.TestCase):
    def setUp(self):
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="torrent-bracket-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        self.files = [
            ([b"YouTube Videos", f"Known [{UNIQUE_ID}].webm".encode()], 11),
            ([b"YouTube Videos", b"No terminal ID.webm"], 7),
            ([b"Old YouTube Livestreams", f"Missing [{MISSING_ID}].mkv".encode()], 13),
            ([b"New YouTube Livestreams", f"Ambiguous [{AMBIGUOUS_ID}].mp4".encode()], 17),
            (
                [
                    b"New New YouTube Livestreams",
                    b"bad-utf8-\xed\xa0\xbd " + f"[{RAW_ID}].mp4".encode(),
                ],
                19,
            ),
            ([b"Clips", f"Out of scope [{IGNORED_ID}].mp4".encode()], 23),
        ]
        self._write_inputs(self.files)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _write_inputs(self, files, *, discovery_extra=None, info_extra=None):
        total_bytes = sum(length for _path, length in files)
        piece_length = 16
        info = {
            b"files": [
                {b"length": length, b"path": path} for path, length in files
            ],
            b"name": b"fixture torrent",
            b"piece length": piece_length,
            b"pieces": b"p" * (math.ceil(total_bytes / piece_length) * 20),
        }
        if info_extra:
            info.update(info_extra)
        info_bytes = bencode(info)
        torrent_body = bencode({b"info": info})
        self.torrent_path = self.root / "fixture.torrent"
        self.torrent_path.write_bytes(torrent_body)
        top_level = {}
        for path, length in files:
            label = path[0].decode("utf-8", errors="replace")
            row = top_level.setdefault(label, {"files": 0, "bytes": 0})
            row["files"] += 1
            row["bytes"] += length
        for label in SCOPED_DIRECTORY_LABELS:
            top_level.setdefault(label, {"files": 0, "bytes": 0})
        discovery = {
            "access": {
                "download_attempted": False,
                "publication_state": "research_lead_only",
                "reason": "Metadata-only fixture; payload was not acquired.",
                "rights_state": "unknown",
            },
            "canonical_url": "https://www.reddit.com/r/HIMRFAM/comments/fixture1/example/",
            "discovery_method": "public fixture review",
            "interpretation_warning": "Paths are unverified locator hints.",
            "observed_at": OBSERVED_AT,
            "platform": "reddit",
            "post_claims_unverified": {
                "described_contents": ["video paths"],
                "nominal_size_label": "fixture",
            },
            "post_id": "fixture1",
            "schema_version": 1,
            "source_id": "reddit-post-fixture1",
            "status": "metadata_only_not_downloaded",
            "subreddit": "HIMRFAM",
            "title_label": "Fixture manifest",
            "torrent": {
                "file_count": len(files),
                "info_hash_sha1": hashlib.sha1(info_bytes).hexdigest().upper(),
                "local_review_filename": self.torrent_path.name,
                "magnet_info_hash_matched": True,
                "piece_length_bytes": piece_length,
                "root_name": "fixture torrent",
                "top_level": top_level,
                "torrent_sha256": hashlib.sha256(torrent_body).hexdigest(),
                "total_bytes": total_bytes,
                "url": "https://example.invalid/fixture.torrent",
            },
        }
        if discovery_extra:
            discovery.update(discovery_extra)
        self.discovery_path = self.root / "discovery.json"
        self.discovery_path.write_text(
            json.dumps(discovery, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def _seed_youtube(self, video_id: str, *, ambiguous: bool = False) -> None:
        with transaction(self.connection):
            digest = hashlib.sha256(video_id.encode("ascii")).hexdigest()
            batch_id, existing = _begin_batch(
                self.connection,
                "youtube_discovery_candidates",
                digest,
                OBSERVED_AT[:10],
                OBSERVED_AT,
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
                observed_at=OBSERVED_AT,
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
                observed_at=OBSERVED_AT,
                batch_id=batch_id,
                review_state="unreviewed",
                metadata={"fixture": True},
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
                    title="Other mapping",
                    date_label=None,
                    date_basis="fixture",
                    duration=None,
                    recording_type="video",
                    observed_at=OBSERVED_AT,
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
            _complete_batch(self.connection, batch_id, OBSERVED_AT, {"fixture": 1})

    def _prepare(self):
        import_torrent_manifest(
            self.connection,
            self.torrent_path,
            observed_at=OBSERVED_AT,
            discovery_metadata_path=self.discovery_path,
        )
        self._seed_youtube(UNIQUE_ID)
        self._seed_youtube(AMBIGUOUS_ID, ambiguous=True)

    def _counts(self):
        return {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "sources",
                "source_metadata_observations",
                "recordings",
                "recording_sources",
                "source_relations",
                "recording_relations",
                "match_candidates",
                "review_tasks",
                "torrent_bracket_reconciliation_imports",
                "torrent_bracket_youtube_candidates",
                "publication_decisions",
                "identity_assertions",
                "claim_catalog_links",
            )
        }

    def test_terminal_parser_is_byte_exact_and_adversarial(self):
        self.assertEqual(
            terminal_bracketed_youtube_id_bytes(f"Title [{UNIQUE_ID}].MP4".encode()),
            UNIQUE_ID,
        )
        self.assertEqual(
            terminal_bracketed_youtube_id_bytes(
                b"bad-utf8-\xed\xa0\xbd " + f"[{RAW_ID}].webm".encode()
            ),
            RAW_ID,
        )
        for rejected in (
            UNIQUE_ID.encode(),
            f"Title-{UNIQUE_ID}.mp4".encode(),
            f"Title [{UNIQUE_ID}] trailing.mp4".encode(),
            f"Title [{UNIQUE_ID}] .mp4".encode(),
            f"Title [{UNIQUE_ID}].txt".encode(),
            f"Title [{UNIQUE_ID}].ia.mp4".encode(),
            f"Title [{UNIQUE_ID}] .webm".encode(),
            f"Title [{UNIQUE_ID}x].mp4".encode(),
        ):
            self.assertIsNone(terminal_bracketed_youtube_id_bytes(rejected), rejected)

    def test_plan_requires_exact_prior_import_and_is_deterministic_read_only(self):
        before = self._counts()
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError, "exact torrent manifest source is missing"
        ):
            build_torrent_bracket_reconciliation_plan(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.assertEqual(before, self._counts())

        self._prepare()
        before = self._counts()
        statements = []
        self.connection.set_trace_callback(statements.append)
        try:
            first = build_torrent_bracket_reconciliation_plan(
                self.connection, self.torrent_path, self.discovery_path
            )
        finally:
            self.connection.set_trace_callback(None)
        self.assertIn("BEGIN", statements)
        self.assertIn("ROLLBACK", statements)
        self.assertFalse(self.connection.in_transaction)
        second = build_torrent_bracket_reconciliation_plan(
            self.connection, self.torrent_path, self.discovery_path
        )
        self.assertEqual(first, second)
        self.assertEqual(before, self._counts())
        self.assertEqual(
            first["statistics"],
            {
                "provider_file_records_scanned": 6,
                "scoped_file_records": 5,
                "scoped_video_file_records": 5,
                "terminal_bracket_candidates": 4,
                "distinct_youtube_video_ids": 4,
                "candidates_by_directory": {
                    "YouTube Videos": 1,
                    "Old YouTube Livestreams": 1,
                    "New YouTube Livestreams": 1,
                    "New New YouTube Livestreams": 1,
                },
                "resolution_state_counts": {
                    "missing_native_source": 2,
                    "native_source_without_unique_recording": 1,
                    "unique_native_recording": 1,
                },
                "review_tasks_total": 4,
                "payload_files_read": 0,
                "payload_bytes_read": 0,
                "source_or_recording_mutations": 0,
                "source_relations": 0,
                "recording_relations": 0,
                "recording_merges": 0,
                "publication_decisions": 0,
                "identity_assertions": 0,
                "claims": 0,
            },
        )
        raw = next(
            item for item in first["candidates"]
            if item["evidence"]["youtube_video_id"] == RAW_ID
        )
        self.assertEqual(
            base64.b64decode(raw["evidence"]["manifest_path_components_base64"][-1]),
            self.files[4][0][-1],
        )
        self.assertIn("\ufffd", raw["evidence"]["manifest_path"])
        self.assertTrue(all(
            item["evidence"]["directory_label"] in SCOPED_DIRECTORY_LABELS
            for item in first["candidates"]
        ))
        self.assertNotIn(
            IGNORED_ID,
            {item["evidence"]["youtube_video_id"] for item in first["candidates"]},
        )

    def test_unknown_discovery_shapes_duplicate_json_and_input_rename_fail(self):
        self._prepare()
        original = self.discovery_path.read_text(encoding="utf-8")
        value = json.loads(original)
        value["unexpected"] = True
        self.discovery_path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "unknown shape"):
            build_torrent_bracket_reconciliation_plan(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.discovery_path.write_text(original, encoding="utf-8")

        duplicate = original.replace(
            '"schema_version": 1,', '"schema_version": 1, "schema_version": 1,', 1
        )
        self.discovery_path.write_text(duplicate, encoding="utf-8")
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "duplicate key"):
            build_torrent_bracket_reconciliation_plan(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.discovery_path.write_text(original, encoding="utf-8")

        invalid_url = json.loads(original)
        invalid_url["canonical_url"] = "https://[invalid"
        self.discovery_path.write_text(json.dumps(invalid_url), encoding="utf-8")
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "valid HTTPS URL"):
            build_torrent_bracket_reconciliation_plan(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.discovery_path.write_text(original, encoding="utf-8")

        for forbidden_url_character in ("\x01", "\u0085", "\u00a0"):
            control_url = json.loads(original)
            control_url["canonical_url"] = control_url["canonical_url"].replace(
                "/comments/", f"/{forbidden_url_character}comments/"
            )
            self.discovery_path.write_text(json.dumps(control_url), encoding="utf-8")
            with self.subTest(character=repr(forbidden_url_character)), self.assertRaisesRegex(
                TorrentBracketReconciliationError, "control text"
            ):
                build_torrent_bracket_reconciliation_plan(
                    self.connection, self.torrent_path, self.discovery_path
                )
        self.discovery_path.write_text(original, encoding="utf-8")

        renamed = self.root / "renamed.torrent"
        renamed.write_bytes(self.torrent_path.read_bytes())
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "filename differs"):
            build_torrent_bracket_reconciliation_plan(
                self.connection, renamed, self.discovery_path
            )

    def test_torrent_parser_rejects_noncanonical_unknown_and_decoding_collisions(self):
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "noncanonical"):
            _parse_torrent(b"d4:infod4:name1:x4:name1:yee")

        files = list(self.files)
        total = sum(length for _path, length in files)
        info = {
            b"files": [
                {b"length": length, b"path": path, b"unknown": b"x"}
                for path, length in files
            ],
            b"name": b"fixture torrent",
            b"piece length": 16,
            b"pieces": b"p" * (math.ceil(total / 16) * 20),
        }
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "unknown shape"):
            _parse_torrent(bencode({b"info": info}))

        collision_files = [
            {b"length": 1, b"path": [b"YouTube Videos", b"bad-\xff.mp4"]},
            {b"length": 1, b"path": [b"YouTube Videos", b"bad-\xfe.mp4"]},
        ]
        collision_info = {
            b"files": collision_files,
            b"name": b"fixture torrent",
            b"piece length": 16,
            b"pieces": b"p" * 20,
        }
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "decoding-collides"):
            _parse_torrent(bencode({b"info": collision_info}))

        overlong_path = [b"YouTube Videos", *(b"a" * 4096 for _ in range(4))]
        overlong_path.append(f"Candidate [{UNIQUE_ID}].mp4".encode())
        overlong_info = {
            b"files": [{b"length": 1, b"path": overlong_path}],
            b"name": b"fixture torrent",
            b"piece length": 16,
            b"pieces": b"p" * 20,
        }
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError, "decoded file path exceeds"
        ):
            _parse_torrent(bencode({b"info": overlong_info}))

    def test_stable_read_rejects_regular_file_swapped_to_symlink_after_lstat(self):
        if not hasattr(os, "symlink"):
            self.skipTest("symlinks are unavailable")
        requested = self.root / "swap-input.bin"
        displaced = self.root / "swap-input.original.bin"
        target = self.root / "swap-target.bin"
        requested.write_bytes(b"original")
        target.write_bytes(b"replacement")
        real_lstat = Path.lstat
        swapped = False

        def lstat_then_swap(path, *args, **kwargs):
            nonlocal swapped
            result = real_lstat(path, *args, **kwargs)
            if Path(path) == requested and not swapped:
                swapped = True
                requested.rename(displaced)
                requested.symlink_to(target)
            return result

        with patch.object(Path, "lstat", lstat_then_swap), self.assertRaisesRegex(
            TorrentBracketReconciliationError, "cannot be opened safely"
        ):
            _stable_file(requested, 1024, "swap fixture")

    def test_stable_read_rejects_fifo_and_regular_file_swapped_to_fifo(self):
        if not hasattr(os, "mkfifo") or not hasattr(os, "O_NONBLOCK"):
            self.skipTest("nonblocking FIFOs are unavailable")
        direct_fifo = self.root / "direct-input.fifo"
        os.mkfifo(direct_fifo)
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "regular file"):
            _stable_file(direct_fifo, 1024, "FIFO fixture")

        requested = self.root / "fifo-swap-input.bin"
        displaced = self.root / "fifo-swap-input.original.bin"
        requested.write_bytes(b"original")
        real_lstat = Path.lstat
        swapped = False

        def lstat_then_swap(path, *args, **kwargs):
            nonlocal swapped
            result = real_lstat(path, *args, **kwargs)
            if Path(path) == requested and not swapped:
                swapped = True
                requested.rename(displaced)
                os.mkfifo(requested)
            return result

        with patch.object(Path, "lstat", lstat_then_swap), self.assertRaisesRegex(
            TorrentBracketReconciliationError, "regular file"
        ):
            _stable_file(requested, 1024, "FIFO swap fixture")

    def test_caps_and_symlinks_fail_without_writes(self):
        self._prepare()
        before = self._counts()
        for constant, message in (
            ("MAX_SCOPED_FILES", "scoped-file cap"),
            ("MAX_CANDIDATES", "reconciliation-candidate cap"),
        ):
            with self.subTest(constant=constant), patch(
                f"himr_corpus.torrent_bracket_reconciler.{constant}", 1
            ):
                with self.assertRaisesRegex(TorrentBracketReconciliationError, message):
                    build_torrent_bracket_reconciliation_plan(
                        self.connection, self.torrent_path, self.discovery_path
                    )
                self.assertEqual(before, self._counts())
        if hasattr(os, "symlink"):
            link = self.root / "linked.torrent"
            link.symlink_to(self.torrent_path)
            with self.assertRaisesRegex(TorrentBracketReconciliationError, "symlink"):
                build_torrent_bracket_reconciliation_plan(
                    self.connection, link, self.discovery_path
                )

    def test_noncanonical_native_source_identity_fails_during_planning(self):
        self._prepare()
        with transaction(self.connection):
            digest = hashlib.sha256(b"noncanonical-source-fixture").hexdigest()
            batch_id, existing = _begin_batch(
                self.connection,
                "youtube_discovery_candidates",
                digest,
                OBSERVED_AT[:10],
                OBSERVED_AT,
            )
            self.assertIsNone(existing)
            _upsert_source(
                self.connection,
                source="src_ffffffffffffffffffffffffffffffff",
                platform="youtube",
                source_kind="youtube_video",
                native_id=MISSING_ID,
                canonical_url=f"https://www.youtube.com/watch?v={MISSING_ID}",
                title="Noncanonical source fixture",
                observed_at=OBSERVED_AT,
                access_state="public",
                review_state="unreviewed",
                batch_id=batch_id,
                metadata={"fixture": True},
            )
            _complete_batch(self.connection, batch_id, OBSERVED_AT, {"fixture": 1})
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError,
            f"deterministic YouTube source identity collision for {MISSING_ID}",
        ):
            build_torrent_bracket_reconciliation_plan(
                self.connection, self.torrent_path, self.discovery_path
            )

    def test_file_source_must_belong_to_the_exact_original_torrent_import(self):
        self._prepare()
        torrent_batch = self.connection.execute(
            "SELECT import_batch_id FROM import_batches WHERE importer_name = 'torrent_manifest_metadata'"
        ).fetchone()[0]
        other_batch = self.connection.execute(
            "SELECT import_batch_id FROM import_batches WHERE importer_name = 'youtube_discovery_candidates' LIMIT 1"
        ).fetchone()[0]
        manifest_source = self.connection.execute(
            "SELECT source_id FROM sources WHERE created_by_import_batch_id = ? AND source_kind = 'torrent_manifest'",
            (torrent_batch,),
        ).fetchone()[0]
        file_source = self.connection.execute(
            "SELECT source_id FROM sources WHERE parent_source_id = ? LIMIT 1",
            (manifest_source,),
        ).fetchone()[0]
        self.connection.execute(
            "UPDATE sources SET created_by_import_batch_id = ? WHERE source_id = ?",
            (other_batch, file_source),
        )
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError, "torrent file projection differs"
        ):
            build_torrent_bracket_reconciliation_plan(
                self.connection, self.torrent_path, self.discovery_path
            )

    def test_import_is_private_idempotent_and_never_mutates_identity(self):
        self._prepare()
        before = self._counts()
        first = import_torrent_bracket_reconciliation(
            self.connection, self.torrent_path, self.discovery_path
        )
        after = self._counts()
        second = import_torrent_bracket_reconciliation(
            self.connection, self.torrent_path, self.discovery_path
        )
        self.assertEqual(first, second)
        self.assertEqual(after, self._counts())
        observations_before_upgrade = self.connection.execute(
            "SELECT count(*) FROM import_observations WHERE import_batch_id = ?",
            (first["import_batch_id"],),
        ).fetchone()[0]

        def upgraded_observation_id(batch_id, observed_at, importer_version="0.3.0"):
            return stable_id("iob", batch_id, importer_version, observed_at)

        with patch("himr_corpus.importers.__version__", "0.3.0"), patch(
            "himr_corpus.importers._import_observation_id",
            side_effect=upgraded_observation_id,
        ):
            upgraded = import_torrent_bracket_reconciliation(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.assertEqual(first, upgraded)
        self.assertEqual(after, self._counts())
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM import_observations WHERE import_batch_id = ?",
                (first["import_batch_id"],),
            ).fetchone()[0],
            observations_before_upgrade + 1,
        )
        self.assertEqual(after["match_candidates"] - before["match_candidates"], 4)
        self.assertEqual(after["review_tasks"] - before["review_tasks"], 4)
        self.assertEqual(after["torrent_bracket_reconciliation_imports"], 1)
        self.assertEqual(after["torrent_bracket_youtube_candidates"], 4)
        for table in (
            "sources",
            "source_metadata_observations",
            "recordings",
            "recording_sources",
            "source_relations",
            "recording_relations",
            "publication_decisions",
            "identity_assertions",
            "claim_catalog_links",
        ):
            self.assertEqual(before[table], after[table], table)
        flags = self.connection.execute(
            """
            SELECT count(*) FROM torrent_bracket_youtube_candidates
            WHERE requires_human_review = 1 AND relationship_asserted = 0
              AND merge_performed = 0 AND visibility = 'private'
              AND publication_authority = 'none'
            """
        ).fetchone()[0]
        self.assertEqual(flags, 4)
        status = validate_database(self.connection)
        self.assertEqual(status["torrent_bracket_youtube_candidates"], 4)
        protected_source = self.connection.execute(
            "SELECT torrent_file_source_id FROM torrent_bracket_youtube_candidates LIMIT 1"
        ).fetchone()[0]
        with self.assertRaisesRegex(Exception, "torrent bracket source identity is immutable"):
            self.connection.execute(
                "UPDATE sources SET native_id = native_id || '-tampered' WHERE source_id = ?",
                (protected_source,),
            )
        with self.assertRaisesRegex(
            Exception, "completed torrent bracket reconciliation import cannot reopen"
        ):
            self.connection.execute(
                "UPDATE import_batches SET status = 'running' WHERE import_batch_id = ?",
                (first["import_batch_id"],),
            )

    def test_replay_detects_task_tampering_and_evidence_is_append_only(self):
        self._prepare()
        import_torrent_bracket_reconciliation(
            self.connection, self.torrent_path, self.discovery_path
        )
        row = self.connection.execute(
            """
            SELECT candidate.match_candidate_id, candidate.review_task_id, task.reason
            FROM torrent_bracket_youtube_candidates AS candidate
            JOIN review_tasks AS task ON task.review_task_id = candidate.review_task_id
            LIMIT 1
            """
        ).fetchone()
        self.connection.execute(
            "UPDATE review_tasks SET reason = 'tampered' WHERE review_task_id = ?",
            (row["review_task_id"],),
        )
        with self.assertRaisesRegex(TorrentBracketReconciliationError, "review task conflicts"):
            import_torrent_bracket_reconciliation(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.connection.execute(
            "UPDATE review_tasks SET reason = ? WHERE review_task_id = ?",
            (row["reason"], row["review_task_id"]),
        )
        batch = self.connection.execute(
            """
            SELECT import_batch_id, statistics_json
            FROM import_batches
            WHERE importer_name = 'torrent_bracket_reconciliation_v1'
            """
        ).fetchone()
        self.connection.execute(
            "UPDATE import_batches SET statistics_json = '{}' WHERE import_batch_id = ?",
            (batch["import_batch_id"],),
        )
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError, "candidate import batch differs"
        ):
            import_torrent_bracket_reconciliation(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.connection.execute(
            "UPDATE import_batches SET statistics_json = ?, importer_version = '9.9.9' WHERE import_batch_id = ?",
            (batch["statistics_json"], batch["import_batch_id"]),
        )
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError,
            "candidate import observation differs",
        ):
            import_torrent_bracket_reconciliation(
                self.connection, self.torrent_path, self.discovery_path
            )
        with self.assertRaisesRegex(Exception, "append-only"):
            self.connection.execute(
                "UPDATE torrent_bracket_youtube_candidates SET byte_count = byte_count + 1 WHERE match_candidate_id = ?",
                (row["match_candidate_id"],),
            )
        with self.assertRaisesRegex(Exception, "append-only"):
            self.connection.execute(
                "UPDATE match_candidates SET metadata_json = '{}' WHERE match_candidate_id = ?",
                (row["match_candidate_id"],),
            )

    def test_candidate_cannot_receive_publication_decision(self):
        self._prepare()
        import_torrent_bracket_reconciliation(
            self.connection, self.torrent_path, self.discovery_path
        )
        match_id = self.connection.execute(
            "SELECT match_candidate_id FROM torrent_bracket_youtube_candidates LIMIT 1"
        ).fetchone()[0]
        register_reviewer_fixture(
            self.connection, "human_fixture", "Fixture reviewer", "human"
        )
        with self.assertRaisesRegex(Exception, "no publication authority"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('pub_fixture', 'match_candidate', ?, 'publish',
                         'human_fixture', ?, 'invalid direct publication')
                """,
                (match_id, OBSERVED_AT),
            )

    def test_database_admission_rejects_unknown_and_fabricated_raw_evidence(self):
        self._prepare()
        plan = build_torrent_bracket_reconciliation_plan(
            self.connection, self.torrent_path, self.discovery_path
        )
        cases = []
        bad_receipt = copy.deepcopy(plan)
        bad_receipt["inputs"]["combined_import_input_sha256"] = "0" * 64
        cases.append((bad_receipt, "invalid torrent bracket reconciliation receipt"))
        unknown_generic = copy.deepcopy(plan)
        unknown_generic["candidates"][0]["generic"]["metadata_json"]["unknown"] = True
        cases.append((unknown_generic, "invalid torrent bracket reconciliation candidate"))
        unknown_evidence = copy.deepcopy(plan)
        unknown_evidence["candidates"][0]["evidence"]["evidence_json"]["unknown"] = True
        cases.append((unknown_evidence, "invalid torrent bracket reconciliation candidate"))
        invalid_raw = copy.deepcopy(plan)
        invalid_raw["candidates"][0]["evidence"]["manifest_path_components_base64"] = ["!!!="]
        invalid_raw["candidates"][0]["evidence"]["evidence_json"][
            "manifest_path_components_base64"
        ] = ["!!!="]
        cases.append((invalid_raw, "raw path evidence is invalid"))
        false_scope = copy.deepcopy(plan)
        original_scope = false_scope["candidates"][0]["evidence"]["directory_label"]
        replacement_scope = next(
            label for label in SCOPED_DIRECTORY_LABELS if label != original_scope
        )
        false_scope["candidates"][0]["evidence"][
            "directory_label"
        ] = replacement_scope
        false_scope["candidates"][0]["evidence"]["evidence_json"][
            "directory_label"
        ] = replacement_scope
        cases.append((false_scope, "invalid torrent bracket reconciliation candidate"))
        for candidate_plan, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(Exception, message):
                with transaction(self.connection):
                    batch_id, existing = _begin_batch(
                        self.connection,
                        "torrent_bracket_reconciliation_v1",
                        candidate_plan["plan_sha256"],
                        OBSERVED_AT[:10],
                        OBSERVED_AT,
                    )
                    self.assertIsNone(existing)
                    _verify_plan_rows(
                        self.connection,
                        candidate_plan,
                        batch_id,
                        allow_insert=True,
                    )

    def test_database_admission_rejects_stale_missing_source_resolution(self):
        self._prepare()
        plan = build_torrent_bracket_reconciliation_plan(
            self.connection, self.torrent_path, self.discovery_path
        )
        self._seed_youtube(MISSING_ID)
        with self.assertRaisesRegex(Exception, "missing-source resolution differs"):
            with transaction(self.connection):
                batch_id, existing = _begin_batch(
                    self.connection,
                    "torrent_bracket_reconciliation_v1",
                    plan["plan_sha256"],
                    OBSERVED_AT[:10],
                    OBSERVED_AT,
                )
                self.assertIsNone(existing)
                _verify_plan_rows(
                    self.connection,
                    plan,
                    batch_id,
                    allow_insert=True,
                )

    def test_import_batch_cannot_complete_with_a_partial_candidate_set(self):
        self._prepare()
        plan = build_torrent_bracket_reconciliation_plan(
            self.connection, self.torrent_path, self.discovery_path
        )
        partial = copy.deepcopy(plan)
        partial["candidates"] = partial["candidates"][:-1]
        before = self._counts()
        with self.assertRaisesRegex(
            Exception, "incomplete torrent bracket reconciliation import"
        ):
            with transaction(self.connection):
                batch_id, existing = _begin_batch(
                    self.connection,
                    "torrent_bracket_reconciliation_v1",
                    partial["plan_sha256"],
                    OBSERVED_AT[:10],
                    OBSERVED_AT,
                )
                self.assertIsNone(existing)
                _verify_plan_rows(
                    self.connection,
                    partial,
                    batch_id,
                    allow_insert=True,
                )
                _complete_batch(
                    self.connection, batch_id, OBSERVED_AT, partial["statistics"]
                )
        self.assertEqual(before, self._counts())

    def test_cli_exposes_readonly_plan_and_separate_import(self):
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("plan-torrent-bracket-reconciliation", choices)
        self.assertIn("import-torrent-bracket-reconciliation", choices)


if __name__ == "__main__":
    unittest.main()
