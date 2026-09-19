from __future__ import annotations

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

from jsonschema.validators import Draft202012Validator


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate, transaction  # noqa: E402
from himr_corpus.ids import source_id  # noqa: E402
from himr_corpus.importers import (  # noqa: E402
    _add_external_id,
    _attach_recording_source,
    _begin_batch,
    _complete_batch,
    _upsert_recording,
    _upsert_source,
    import_torrent_manifest,
)
from himr_corpus.torrent_bracket_reconciler import (  # noqa: E402
    SCOPED_DIRECTORY_LABELS,
)
from himr_corpus.torrent_selective_planner import (  # noqa: E402
    PROBE_FLAGS,
    PROBE_KIND,
    TorrentSelectivePlannerError,
    build_torrent_selective_acquisition_plan,
    publish_private_torrent_selective_acquisition_plan,
    summarize_torrent_selective_acquisition_plan,
)


CATALOG_ID = "CatalogID01"
ARCHIVE_ID = "ArchiveID01"
UNAVAILABLE_ID = "MissingID01"
AVAILABLE_ID = "PublicVid01"
MALFORMED_TOKEN = "short-id"
OBSERVED_AT = "2026-08-27T12:00:00Z"


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


class TorrentSelectivePlannerTests(unittest.TestCase):
    def setUp(self):
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="torrent-selective-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "catalog.sqlite3")
        migrate(self.connection)
        self.files = [
            ([b"YouTube Videos", f"catalog [{CATALOG_ID}].mp4".encode()], 90),
            ([b"YouTube Videos", f"archive [{ARCHIVE_ID}].webm".encode()], 80),
            (
                [
                    b"Old YouTube Livestreams",
                    f"large [{UNAVAILABLE_ID}].mkv".encode(),
                ],
                100,
            ),
            (
                [
                    b"New YouTube Livestreams",
                    f"small [{UNAVAILABLE_ID}] 480p.mp4".encode(),
                ],
                50,
            ),
            (
                [b"New New YouTube Livestreams", f"public [{AVAILABLE_ID}].mov".encode()],
                70,
            ),
            (
                [
                    b"New New YouTube Livestreams",
                    f"malformed [{MALFORMED_TOKEN}] 480p.mkv".encode(),
                ],
                20,
            ),
            ([b"Clips", b"out of scope [Ignored00001].mp4"], 10),
        ]
        self._write_inputs()
        import_torrent_manifest(
            self.connection,
            self.torrent_path,
            observed_at=OBSERVED_AT,
            discovery_metadata_path=self.discovery_path,
        )
        self._seed_catalog_youtube(CATALOG_ID)
        self.snapshot_path = self.root / "snapshot.json"
        self.snapshot_path.write_text("fixture", encoding="utf-8")

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
            b"name": b"fixture torrent",
            b"piece length": piece_length,
            b"pieces": b"p" * (math.ceil(total_bytes / piece_length) * 20),
        }
        info_bytes = bencode(info)
        torrent_body = bencode({b"info": info})
        self.torrent_path = self.root / "fixture.torrent"
        self.torrent_path.write_bytes(torrent_body)
        top_level = {}
        for path, length in self.files:
            label = path[0].decode("utf-8")
            row = top_level.setdefault(label, {"files": 0, "bytes": 0})
            row["files"] += 1
            row["bytes"] += length
        for label in SCOPED_DIRECTORY_LABELS:
            top_level.setdefault(label, {"files": 0, "bytes": 0})
        discovery = {
            "access": {
                "download_attempted": False,
                "publication_state": "research_lead_only",
                "reason": "Metadata-only fixture.",
                "rights_state": "unknown",
            },
            "canonical_url": "https://www.reddit.com/r/HIMRFAM/comments/fixture1/example/",
            "discovery_method": "public fixture review",
            "interpretation_warning": "Paths are unverified locators.",
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
                "file_count": len(self.files),
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
        self.discovery_path = self.root / "discovery.json"
        self.discovery_path.write_text(
            json.dumps(discovery, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def _seed_catalog_youtube(self, video_id: str):
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
                title="Fixture",
                observed_at=OBSERVED_AT,
                access_state="public",
                review_state="unreviewed",
                batch_id=batch_id,
                metadata={"fixture": True},
            )
            recording = _upsert_recording(
                self.connection,
                canonical_key=f"youtube:video:{video_id}",
                title="Fixture",
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
            _add_external_id(
                self.connection,
                object_type="recording",
                object_id=recording,
                namespace="youtube_video_id",
                value=video_id,
                basis="Validated yt-dlp info JSON",
                batch_id=batch_id,
                observed_at=OBSERVED_AT,
                source=youtube_source,
            )
            _complete_batch(self.connection, batch_id, OBSERVED_AT, {"fixture": 1})

    @staticmethod
    def _snapshot():
        return {
            "snapshot_id": "iams_fixture000000000000000000000000",
            "_sha256": "a" * 64,
            "observed_at": OBSERVED_AT,
            "items": [{"identifier": "fixture-archive"}],
        }

    @staticmethod
    def _archive_document(_item):
        return {
            "files": [
                {
                    "name": f"archived video [{ARCHIVE_ID}].mp4",
                    "title": None,
                }
            ]
        }

    def _plan(self, probe_path=None):
        with (
            patch(
                "himr_corpus.torrent_selective_planner.validate_archive_metadata_snapshot",
                return_value=self._snapshot(),
            ),
            patch(
                "himr_corpus.torrent_selective_planner._payload_document",
                side_effect=self._archive_document,
            ),
        ):
            return build_torrent_selective_acquisition_plan(
                self.connection,
                self.torrent_path,
                self.discovery_path,
                [self.snapshot_path],
                availability_probe_path=probe_path,
            )

    def _write_probe(self, request, outcomes, *, network_override=None):
        value = {
            "schema_version": 1,
            "probe_kind": PROBE_KIND,
            "request_sha256": request["request_sha256"],
            "observed_at": "2026-08-27T13:00:00Z",
            "producer": {
                "tool": "yt-dlp",
                "version": "fixture-1",
                "executable_sha256": "b" * 64,
                "invocation_flags": list(PROBE_FLAGS),
            },
            "network_policy": {
                "cookies_sent": False,
                "authorization_sent": False,
                "media_payload_downloaded": False,
                "playlist_expansion": False,
            },
            "outcomes": outcomes,
        }
        if network_override:
            value["network_policy"].update(network_override)
        path = self.root / "probe.json"
        path.write_text(
            json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _probe_outcome(target, state, code):
        return {
            **target,
            "availability_state": state,
            "evidence_code": code,
        }

    def test_preprobe_is_deterministic_read_only_and_chooses_smallest(self):
        before = self.connection.total_changes
        statements = []
        self.connection.set_trace_callback(statements.append)
        try:
            first = self._plan()
        finally:
            self.connection.set_trace_callback(None)
        second = self._plan()
        self.assertEqual(first, second)
        self.assertEqual(before, self.connection.total_changes)
        self.assertIn("BEGIN", statements)
        self.assertIn("ROLLBACK", statements)
        self.assertFalse(any("INSERT" in statement.upper() for statement in statements))

        stats = first["statistics"]
        self.assertEqual(stats["scoped_video_file_records"], 6)
        self.assertEqual(stats["usable_video_file_records"], 5)
        self.assertEqual(stats["distinct_usable_youtube_video_ids"], 4)
        self.assertEqual(stats["covered_distinct_ids"], 2)
        self.assertEqual(stats["probe_candidate_distinct_ids"], 2)
        self.assertEqual(stats["probe_candidate_file_records"], 3)
        self.assertEqual(stats["probe_candidate_smallest_renditions_bytes"], 120)
        self.assertEqual(stats["malformed_manual_review_files"], 1)
        self.assertEqual(stats["preprobe_review_upper_bound_bytes"], 140)
        self.assertEqual(stats["selected_file_count"], 0)
        self.assertEqual(first["selected_torrent_file_indices"], [])
        chosen = next(
            row for row in first["probe_candidates"]
            if row["youtube_video_id"] == UNAVAILABLE_ID
        )
        self.assertEqual(chosen["smallest_rendition_file_index"], 3)
        self.assertEqual(chosen["smallest_rendition_byte_count"], 50)
        self.assertEqual(chosen["rendition_file_indices"], [2, 3])
        self.assertEqual(
            first["malformed_manual_review"][0]["torrent_file_index"], 5
        )
        self.assertEqual(
            [
                target["youtube_video_id"]
                for target in first["availability_probe_request"]["targets"]
            ],
            sorted([UNAVAILABLE_ID, AVAILABLE_ID]),
        )
        summary = summarize_torrent_selective_acquisition_plan(first)
        self.assertNotIn("probe_candidates", summary)
        self.assertNotIn("malformed_manual_review", summary)

    def test_only_probe_unavailable_outcome_selects_one_smallest_file(self):
        preprobe = self._plan()
        targets = preprobe["availability_probe_request"]["targets"]
        outcomes = []
        for target in targets:
            if target["youtube_video_id"] == UNAVAILABLE_ID:
                outcomes.append(self._probe_outcome(target, "unavailable", "private"))
            else:
                outcomes.append(self._probe_outcome(target, "available", "metadata_resolved"))
        probe = self._write_probe(preprobe["availability_probe_request"], outcomes)
        plan = self._plan(probe)
        plan_schema = json.loads(
            (CORPUS_ROOT / "schemas" / "torrent-selective-acquisition-plan.schema.json")
            .read_text(encoding="utf-8")
        )
        probe_schema = json.loads(
            (CORPUS_ROOT / "schemas" / "torrent-youtube-availability-probe.schema.json")
            .read_text(encoding="utf-8")
        )
        Draft202012Validator.check_schema(plan_schema)
        Draft202012Validator(plan_schema).validate(plan)
        Draft202012Validator.check_schema(probe_schema)
        Draft202012Validator(probe_schema).validate(
            json.loads(probe.read_text(encoding="utf-8"))
        )
        self.assertEqual(plan["selected_torrent_file_indices"], [3])
        self.assertEqual(plan["statistics"]["selected_file_count"], 1)
        self.assertEqual(plan["statistics"]["selected_payload_bytes"], 50)
        self.assertEqual(plan["selected_files"][0]["youtube_video_id"], UNAVAILABLE_ID)
        self.assertEqual(plan["selected_files"][0]["byte_count"], 50)

    def test_indeterminate_and_tampered_probe_fail_closed(self):
        preprobe = self._plan()
        targets = preprobe["availability_probe_request"]["targets"]
        indeterminate = [
            self._probe_outcome(target, "indeterminate", "network_error")
            for target in targets
        ]
        probe = self._write_probe(preprobe["availability_probe_request"], indeterminate)
        plan = self._plan(probe)
        self.assertEqual(plan["selected_files"], [])

        reversed_probe = self._write_probe(
            preprobe["availability_probe_request"], list(reversed(indeterminate))
        )
        with self.assertRaisesRegex(
            TorrentSelectivePlannerError, "reordered"
        ):
            self._plan(reversed_probe)

        credential_probe = self._write_probe(
            preprobe["availability_probe_request"],
            indeterminate,
            network_override={"cookies_sent": True},
        )
        with self.assertRaisesRegex(TorrentSelectivePlannerError, "credentials"):
            self._plan(credential_probe)

        wrong = copy.deepcopy(preprobe["availability_probe_request"])
        wrong["request_sha256"] = "0" * 64
        wrong_probe = self._write_probe(wrong, indeterminate)
        with self.assertRaisesRegex(TorrentSelectivePlannerError, "exact probe request"):
            self._plan(wrong_probe)

    def test_sign_in_gate_is_indeterminate_and_never_selects_torrent_payload(self):
        preprobe = self._plan()
        outcomes = [
            self._probe_outcome(target, "indeterminate", "sign_in_required")
            for target in preprobe["availability_probe_request"]["targets"]
        ]
        probe = self._write_probe(preprobe["availability_probe_request"], outcomes)
        plan = self._plan(probe)
        self.assertEqual(plan["selected_files"], [])
        self.assertEqual(plan["selected_torrent_file_indices"], [])
        self.assertTrue(
            all(
                row["availability_state"] == "indeterminate"
                for row in plan["probe_candidates"]
            )
        )

    def test_cli_exposes_read_only_planner_only(self):
        choices = build_parser()._subparsers._group_actions[0].choices
        self.assertIn("plan-torrent-selective-acquisition", choices)
        self.assertNotIn("download-torrent-selective-acquisition", choices)
        self.assertNotIn("import-torrent-selective-acquisition", choices)

    def test_private_full_plan_writer_is_atomic_read_only_and_no_overwrite(self):
        plan = self._plan()
        output = self.root / "private-plan.json"
        receipt = publish_private_torrent_selective_acquisition_plan(plan, output)
        self.assertTrue(receipt["full_plan_written"])
        self.assertFalse(receipt["path_disclosed"])
        self.assertEqual(receipt["plan_sha256"], plan["plan_sha256"])
        self.assertEqual(os.stat(output).st_mode & 0o777, 0o400)
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), plan)
        with self.assertRaisesRegex(
            TorrentSelectivePlannerError, "already exists"
        ):
            publish_private_torrent_selective_acquisition_plan(plan, output)
        with self.assertRaisesRegex(
            TorrentSelectivePlannerError, "absolute file path"
        ):
            publish_private_torrent_selective_acquisition_plan(
                plan, Path("relative-plan.json")
            )


if __name__ == "__main__":
    unittest.main()
