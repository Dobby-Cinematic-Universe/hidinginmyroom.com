from __future__ import annotations

import base64
import hashlib
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate, transaction  # noqa: E402
from himr_corpus.ids import source_id  # noqa: E402
from himr_corpus.importers import (  # noqa: E402
    _attach_recording_source,
    _begin_batch,
    _complete_batch,
    _upsert_recording,
    _upsert_source,
    canonical_json,
    import_torrent_manifest,
    sha256_bytes,
)
from himr_corpus.torrent_bracket_reconciler import (  # noqa: E402
    SCOPED_DIRECTORY_LABELS,
    TorrentBracketReconciliationError,
    terminal_bracketed_youtube_id_bytes,
)
from himr_corpus.torrent_suffix_audio_planner import (  # noqa: E402
    AUDIO_ONLY_CONFIDENCE,
    AUDIO_ONLY_EVIDENCE,
    AUDIO_ONLY_LANE,
    AUDIO_ONLY_ROUTE,
    FORMAT_LABEL_VIDEO_CONFIDENCE,
    FORMAT_LABEL_VIDEO_EVIDENCE,
    FORMAT_LABEL_VIDEO_LANE,
    FORMAT_LABEL_VIDEO_ROUTE,
    build_torrent_suffix_audio_plan,
    suffix_audio_locator_bytes,
    summarize_torrent_suffix_audio_plan,
)


VIDEO_ID = "AbCdEfGhI_1"
AUDIO_ID = "Q4oJl8Y6w9I"
SEALED_ID = "SealedID123"
OBSERVED_AT = "2026-08-27T06:00:00Z"


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


class TorrentSuffixAudioPlannerTests(unittest.TestCase):
    def setUp(self):
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="torrent-suffix-audio-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        self.files = [
            (
                [
                    b"YouTube Videos",
                    b"bad-utf8-\xed\xa0\xbd "
                    + f"Known [{VIDEO_ID}] 480p.MKV".encode("ascii"),
                ],
                11,
            ),
            (
                [
                    b"Old YouTube Livestreams",
                    f"Duplicate [{VIDEO_ID}] 480p.webm".encode("ascii"),
                ],
                13,
            ),
            (
                [
                    b"New YouTube Livestreams",
                    f"Audio [{AUDIO_ID}].m4a".encode("ascii"),
                ],
                17,
            ),
            (
                [b"New New YouTube Livestreams", b"sad [e1ec23d6-] 480p.mkv"],
                19,
            ),
            (
                [
                    b"New New YouTube Livestreams",
                    b"back [27bd79b8-back] 480p.mkv",
                ],
                23,
            ),
            (
                [
                    b"New New YouTube Livestreams",
                    b"hey [e228fcd0-hey] 480p.mkv",
                ],
                29,
            ),
            (
                [
                    b"YouTube Videos",
                    f"Double space [{VIDEO_ID}]  480p.mkv".encode("ascii"),
                ],
                31,
            ),
            (
                [
                    b"YouTube Videos",
                    f"Wrong label [{VIDEO_ID}] 720p.mkv".encode("ascii"),
                ],
                37,
            ),
            (
                [
                    b"YouTube Videos",
                    f"Counter [{VIDEO_ID}] 480p (1).mkv".encode("ascii"),
                ],
                41,
            ),
            (
                [
                    b"YouTube Videos",
                    f"Part [{VIDEO_ID}] 480p.mkv.part".encode("ascii"),
                ],
                43,
            ),
            (
                [
                    b"YouTube Videos",
                    f"Audio part [{AUDIO_ID}].m4a.part".encode("ascii"),
                ],
                47,
            ),
            (
                [
                    b"YouTube Videos",
                    f"Sealed [{SEALED_ID}].mp4".encode("ascii"),
                ],
                53,
            ),
            ([b"YouTube Videos", b"No locator.txt"], 59),
            (
                [b"Clips", f"Out [{VIDEO_ID}] 480p.mkv".encode("ascii")],
                61,
            ),
            ([b"Clips", f"Out [{AUDIO_ID}].m4a".encode("ascii")], 67),
        ]
        self._write_inputs()

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _write_inputs(self):
        total_bytes = sum(length for _path, length in self.files)
        piece_length = 16
        info = {
            b"files": [
                {b"length": length, b"path": path} for path, length in self.files
            ],
            b"name": b"suffix audio fixture",
            b"piece length": piece_length,
            b"pieces": b"p" * (math.ceil(total_bytes / piece_length) * 20),
        }
        info_bytes = bencode(info)
        torrent_body = bencode({b"info": info})
        self.torrent_path = self.root / "fixture.torrent"
        self.torrent_path.write_bytes(torrent_body)
        top_level = {}
        for path, length in self.files:
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
            "canonical_url": "https://www.reddit.com/r/HIMRFAM/comments/suffix1/example/",
            "discovery_method": "public fixture review",
            "interpretation_warning": "Paths are unverified locator hints.",
            "observed_at": OBSERVED_AT,
            "platform": "reddit",
            "post_claims_unverified": {
                "described_contents": ["video and audio paths"],
                "nominal_size_label": "fixture",
            },
            "post_id": "suffix1",
            "schema_version": 1,
            "source_id": "reddit-post-suffix1",
            "status": "metadata_only_not_downloaded",
            "subreddit": "HIMRFAM",
            "title_label": "Suffix/audio fixture manifest",
            "torrent": {
                "file_count": len(self.files),
                "info_hash_sha1": hashlib.sha1(info_bytes).hexdigest().upper(),
                "local_review_filename": self.torrent_path.name,
                "magnet_info_hash_matched": True,
                "piece_length_bytes": piece_length,
                "root_name": "suffix audio fixture",
                "top_level": top_level,
                "torrent_sha256": hashlib.sha256(torrent_body).hexdigest(),
                "total_bytes": total_bytes,
                "url": "https://example.invalid/fixture.torrent",
            },
        }
        self.discovery_path = self.root / "discovery.json"
        self.discovery_path.write_text(
            json.dumps(discovery, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def _seed_youtube(self, video_id: str) -> None:
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
            recording = _upsert_recording(
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
                recording=recording,
                source=youtube_source,
                role="platform_video",
                method="stable_youtube_video_id",
            )
            _complete_batch(self.connection, batch_id, OBSERVED_AT, {"fixture": 1})

    def _prepare(self):
        import_torrent_manifest(
            self.connection,
            self.torrent_path,
            observed_at=OBSERVED_AT,
            discovery_metadata_path=self.discovery_path,
        )
        self._seed_youtube(VIDEO_ID)

    def _protected_counts(self):
        return {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "sources",
                "source_metadata_observations",
                "recordings",
                "recording_sources",
                "match_candidates",
                "review_tasks",
                "torrent_bracket_reconciliation_imports",
                "torrent_bracket_youtube_candidates",
                "publication_decisions",
                "identity_assertions",
                "claim_catalog_links",
            )
        }

    def test_parsers_are_exact_disjoint_and_reject_ambiguous_variants(self):
        self.assertEqual(
            suffix_audio_locator_bytes(f"Title [{VIDEO_ID}] 480p.MP4".encode()),
            {
                "lane": FORMAT_LABEL_VIDEO_LANE,
                "youtube_video_id": VIDEO_ID,
                "file_extension": "mp4",
                "format_label": "480p",
            },
        )
        self.assertEqual(
            suffix_audio_locator_bytes(f"Title [{AUDIO_ID}].M4A".encode()),
            {
                "lane": AUDIO_ONLY_LANE,
                "youtube_video_id": AUDIO_ID,
                "file_extension": "m4a",
                "format_label": "none",
            },
        )
        for rejected in (
            b"sad [e1ec23d6-] 480p.mkv",
            b"back [27bd79b8-back] 480p.mkv",
            b"hey [e228fcd0-hey] 480p.mkv",
            f"Title [{VIDEO_ID}]  480p.mkv".encode(),
            f"Title [{VIDEO_ID}] 720p.mkv".encode(),
            f"Title [{VIDEO_ID}] 480P.mkv".encode(),
            f"Title [{VIDEO_ID}] 480p .mkv".encode(),
            f"Title [{VIDEO_ID}] 480p.mkv.part".encode(),
            f"Title [{VIDEO_ID}] 480p (1).mkv".encode(),
            f"Title [{AUDIO_ID}] .m4a".encode(),
            f"Title [{AUDIO_ID}].m4a.part".encode(),
            f"Title [{AUDIO_ID}] 480p.m4a".encode(),
            f"Title [{VIDEO_ID}].mp4".encode(),
        ):
            self.assertIsNone(suffix_audio_locator_bytes(rejected), rejected)

        sealed = f"Title [{SEALED_ID}].mp4".encode()
        format_label = f"Title [{VIDEO_ID}] 480p.mp4".encode()
        audio = f"Title [{AUDIO_ID}].m4a".encode()
        self.assertEqual(terminal_bracketed_youtube_id_bytes(sealed), SEALED_ID)
        self.assertIsNone(suffix_audio_locator_bytes(sealed))
        self.assertIsNone(terminal_bracketed_youtube_id_bytes(format_label))
        self.assertIsNone(terminal_bracketed_youtube_id_bytes(audio))

    def test_plan_is_deterministic_read_only_and_keeps_lane_contracts_distinct(self):
        before = self._protected_counts()
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError, "exact torrent manifest source is missing"
        ):
            build_torrent_suffix_audio_plan(
                self.connection, self.torrent_path, self.discovery_path
            )
        self.assertEqual(before, self._protected_counts())

        self._prepare()
        before = self._protected_counts()
        statements = []
        self.connection.set_trace_callback(statements.append)
        try:
            first = build_torrent_suffix_audio_plan(
                self.connection, self.torrent_path, self.discovery_path
            )
        finally:
            self.connection.set_trace_callback(None)
        second = build_torrent_suffix_audio_plan(
            self.connection, self.torrent_path, self.discovery_path
        )
        self.assertEqual(first, second)
        self.assertEqual(before, self._protected_counts())
        self.assertIn("BEGIN", statements)
        self.assertIn("ROLLBACK", statements)
        self.assertFalse(self.connection.in_transaction)

        core = {key: value for key, value in first.items() if key not in {"plan_id", "plan_sha256"}}
        expected_sha = sha256_bytes(canonical_json(core).encode("utf-8"))
        self.assertEqual(first["plan_sha256"], expected_sha)
        self.assertEqual(first["plan_id"], f"tsap_{expected_sha[:32]}")
        self.assertEqual(
            first["statistics"],
            {
                "provider_file_records_scanned": 15,
                "scoped_file_records": 13,
                "candidates_total": 3,
                "candidates_by_lane": {
                    "format_label_video": 2,
                    "audio_only": 1,
                },
                "distinct_youtube_video_ids": 2,
                "distinct_ids_by_lane": {
                    "format_label_video": 1,
                    "audio_only": 1,
                },
                "cross_lane_distinct_ids": 0,
                "candidates_by_directory": {
                    "YouTube Videos": {
                        "format_label_video": 1,
                        "audio_only": 0,
                        "total": 1,
                    },
                    "Old YouTube Livestreams": {
                        "format_label_video": 1,
                        "audio_only": 0,
                        "total": 1,
                    },
                    "New YouTube Livestreams": {
                        "format_label_video": 0,
                        "audio_only": 1,
                        "total": 1,
                    },
                    "New New YouTube Livestreams": {
                        "format_label_video": 0,
                        "audio_only": 0,
                        "total": 0,
                    },
                },
                "resolution_state_counts": {
                    "missing_native_source": 1,
                    "native_source_without_unique_recording": 0,
                    "unique_native_recording": 2,
                },
                "rejected_suffix_counts": {
                    "format_label_invalid_id_tokens": 3,
                    "audio_only_invalid_id_tokens": 0,
                    "ambiguous_or_noncanonical_suffixes": 5,
                },
                "payload_files_read": 0,
                "payload_bytes_read": 0,
                "catalog_rows_written": 0,
                "review_tasks_created": 0,
                "source_or_recording_mutations": 0,
                "source_relations": 0,
                "recording_relations": 0,
                "recording_merges": 0,
                "publication_decisions": 0,
                "identity_assertions": 0,
                "claims": 0,
            },
        )

        contracts = {
            candidate["lane_contract"]["lane"]: candidate["lane_contract"]
            for candidate in first["candidates"]
        }
        self.assertEqual(
            contracts[FORMAT_LABEL_VIDEO_LANE]["evidence_basis"],
            FORMAT_LABEL_VIDEO_EVIDENCE,
        )
        self.assertEqual(
            contracts[FORMAT_LABEL_VIDEO_LANE]["confidence"]["profile"],
            FORMAT_LABEL_VIDEO_CONFIDENCE,
        )
        self.assertEqual(
            contracts[FORMAT_LABEL_VIDEO_LANE]["routing"]["review_route"],
            FORMAT_LABEL_VIDEO_ROUTE,
        )
        self.assertEqual(
            contracts[AUDIO_ONLY_LANE]["evidence_basis"], AUDIO_ONLY_EVIDENCE
        )
        self.assertEqual(
            contracts[AUDIO_ONLY_LANE]["confidence"]["profile"],
            AUDIO_ONLY_CONFIDENCE,
        )
        self.assertEqual(
            contracts[AUDIO_ONLY_LANE]["routing"]["review_route"], AUDIO_ONLY_ROUTE
        )
        self.assertNotEqual(
            contracts[FORMAT_LABEL_VIDEO_LANE]["evidence_basis"],
            contracts[AUDIO_ONLY_LANE]["evidence_basis"],
        )

        raw = next(
            item
            for item in first["candidates"]
            if item["directory_label"] == "YouTube Videos"
        )
        self.assertEqual(
            base64.b64decode(raw["manifest_path_components_base64"][-1]),
            self.files[0][0][-1],
        )
        self.assertIn("\ufffd", raw["manifest_path"])
        self.assertFalse(first["policy"]["catalog_admission_implemented"])
        self.assertFalse(first["policy"]["sealed_terminal_bracket_lane_modified"])

        summary = summarize_torrent_suffix_audio_plan(first)
        self.assertNotIn("candidates", summary)
        self.assertNotIn("manifest_path", json.dumps(summary))
        self.assertTrue(summary["plan_only"])

    def test_candidate_cap_and_catalog_tampering_fail_without_writes(self):
        self._prepare()
        before = self._protected_counts()
        with patch("himr_corpus.torrent_suffix_audio_planner.MAX_CANDIDATES", 1):
            with self.assertRaisesRegex(
                TorrentBracketReconciliationError, "suffix/audio candidate cap"
            ):
                build_torrent_suffix_audio_plan(
                    self.connection, self.torrent_path, self.discovery_path
                )
        self.assertEqual(before, self._protected_counts())

        file_source = self.connection.execute(
            "SELECT source_id FROM sources WHERE source_kind = 'torrent_file_candidate' LIMIT 1"
        ).fetchone()[0]
        other_batch = self.connection.execute(
            "SELECT import_batch_id FROM import_batches WHERE importer_name = 'youtube_discovery_candidates'"
        ).fetchone()[0]
        self.connection.execute(
            "UPDATE sources SET created_by_import_batch_id = ? WHERE source_id = ?",
            (other_batch, file_source),
        )
        with self.assertRaisesRegex(
            TorrentBracketReconciliationError, "torrent file projection differs"
        ):
            build_torrent_suffix_audio_plan(
                self.connection, self.torrent_path, self.discovery_path
            )

    def test_cli_exposes_only_a_readonly_plan_command(self):
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("plan-torrent-suffix-audio-reconciliation", choices)
        self.assertNotIn("import-torrent-suffix-audio-reconciliation", choices)


if __name__ == "__main__":
    unittest.main()
