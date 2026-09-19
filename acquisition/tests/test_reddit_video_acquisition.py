from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
sys.path.insert(0, str(ACQUISITION_ROOT))

import acquire  # noqa: E402
import reddit_rss  # noqa: E402
import reddit_video_acquisition as lane  # noqa: E402


ATOM = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>fixture</title>
  <entry>
    <id>t3_1abcde1</id>
    <title>Unreviewed first clip title</title>
    <published>2026-08-25T12:00:00+00:00</published>
    <updated>2026-08-25T12:00:00+00:00</updated>
    <link href="https://www.reddit.com/r/HIMRFAM2/comments/1abcde1/example/" />
    <content type="html">&lt;a href="https://v.redd.it/clipalpha1"&gt;clip&lt;/a&gt;</content>
  </entry>
  <entry>
    <id>t3_1abcde2</id>
    <title>Unreviewed second clip title</title>
    <published>2026-08-25T13:00:00+00:00</published>
    <updated>2026-08-25T13:00:00+00:00</updated>
    <link href="https://www.reddit.com/r/HIMRFAM2/comments/1abcde2/example/" />
    <content type="html">&lt;a href="https://v.redd.it/clipbravo2"&gt;clip&lt;/a&gt;</content>
  </entry>
</feed>
"""


class FakeResponse(io.BytesIO):
    status = 200

    def __init__(self, payload: bytes, url: str):
        super().__init__(payload)
        self._url = url
        self.headers = {
            "Content-Type": "application/atom+xml",
            "Content-Length": str(len(payload)),
        }

    def getcode(self):
        return self.status

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeOpener:
    def open(self, request, timeout):
        headers = {key.lower() for key, _value in request.header_items()}
        if "cookie" in headers or "authorization" in headers:
            raise AssertionError("credential-bearing Atom request")
        return FakeResponse(ATOM, request.full_url)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class RedditVideoAcquisitionTests(unittest.TestCase):
    def setUp(self):
        sandbox = ACQUISITION_ROOT / ".test-reddit-video"
        sandbox.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=sandbox)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.snapshot = reddit_rss.capture_snapshot(
            subreddit="HIMRFAM2",
            limit=100,
            out_dir=self.root,
            opener=FakeOpener(),
        )
        info = self.root / "clipalpha1.info.json"
        info.write_text(
            json.dumps(
                {
                    "id": "clipalpha1",
                    "original_url": "https://v.redd.it/clipalpha1",
                    "webpage_url": "https://v.redd.it/clipalpha1",
                    "duration": 30,
                    "uploader": "must_not_cross_minimization_boundary",
                }
            ),
            encoding="utf-8",
        )
        info_mtime = int(
            datetime(2026, 8, 26, 12, 4, 59, tzinfo=timezone.utc).timestamp()
        ) * 1_000_000_000 + 999_999_999
        os.utime(info, ns=(info_mtime, info_mtime))
        with patch.object(
            reddit_rss,
            "_now_utc_datetime",
            return_value=datetime(2026, 8, 26, 12, 5, 1, tzinfo=timezone.utc),
        ):
            self.discovery = reddit_rss.write_discovery_manifest(
                self.snapshot,
                v_reddit_info_paths=[info],
                metadata_observed_at="2026-08-26T12:05:00Z",
            )
        snapshot_id = reddit_rss.validate_snapshot_manifest(self.snapshot)["snapshot_id"]
        self.selection = self.root / "selection.json"
        self.selection.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "manifest_kind": "reddit_video_selection",
                    "purpose": "strict two-clip fixture",
                    "snapshot_id": snapshot_id,
                    "reddit_video_ids": ["clipalpha1", "clipbravo2"],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.executable = self.root / "yt-dlp"
        self.executable.write_text(
            "#!/bin/sh\n"
            "if [ \"${1:-}\" = \"--version\" ]; then\n"
            "  printf '%s\\n' '2026.fixture'\n"
            "  exit 0\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        self.executable.chmod(0o755)
        self.runtime_root = self.root / "yt_dlp"
        self.runtime_root.mkdir()
        (self.runtime_root / "__init__.py").write_text(
            "__version__ = '2026.fixture'\n", encoding="utf-8"
        )
        self.runtime_tree = acquire.runtime_tree_fingerprint(self.runtime_root)

    def policy(self, **updates):
        value = {
            "max_items": 5,
            "max_duration_ms": 15 * 60 * 1000,
            "max_job_bytes": 512 * 1024**2,
            "plan_budget_bytes": 2 * 1024**3,
            "estimated_bytes_per_second": 500_000,
            "fixed_overhead_bytes": 64 * 1024**2,
        }
        value.update(updates)
        return value

    def build_plan(self, **policy_updates):
        discovery, discovery_bytes = lane.load_discovery(self.discovery)
        selection, selection_bytes = lane.load_selection(self.selection)
        return lane.build_plan(
            discovery=discovery,
            discovery_bytes=discovery_bytes,
            selection=selection,
            selection_bytes=selection_bytes,
            planned_at="2026-08-26T16:00:00Z",
            policy=self.policy(**policy_updates),
        )

    def write_plan(self, plan=None):
        path = self.root / "plan.json"
        path.write_bytes(lane.pretty_bytes(plan or self.build_plan()))
        return path

    def reddit_order(self, url="https://v.redd.it/clipalpha1", **updates):
        source = {
            "platform": "reddit",
            "source_kind": "reddit_video",
            "native_id": "clipalpha1",
            "canonical_url": url,
            "title": "fixture",
            "published_at": None,
            "access_state": "public",
        }
        source.update(updates.pop("source", {}))
        adapter_config = {
            "url": url,
            "executable": str(self.executable),
            "expected_executable_sha256": sha(self.executable),
            "expected_ytdlp_version": "2026.fixture",
            "expected_runtime_tree_root": str(self.runtime_root),
            "expected_runtime_tree_sha256": self.runtime_tree["sha256"],
            "expected_webpage_url": "https://www.reddit.com/r/HIMRFAM2/comments/1abcde1/example/",
            "format_selector": lane.FORMAT_SELECTOR,
            "expected_sha256": None,
            "expected_byte_count": None,
        }
        adapter_config.update(updates.pop("adapter_config", {}))
        value = {
            "schema_version": 1,
            "job_id": "reddit-fixture-001",
            "adapter": "yt_dlp",
            "source": source,
            "adapter_config": adapter_config,
            "output": {"root": str(self.root / "media")},
            "limits": {
                "max_job_bytes": 512 * 1024**2,
                "global_cache_cap_bytes": 2 * 1024**3,
                "free_space_floor_bytes": 0,
            },
        }
        value.update(updates)
        return value

    def test_missing_metadata_is_visible_but_never_queued(self):
        plan = self.build_plan()
        by_id = {row["native_id"]: row for row in plan["candidates"]}
        self.assertEqual(by_id["clipalpha1"]["queue_state"], "ready")
        self.assertEqual(
            by_id["clipalpha1"]["access_basis"],
            "public_atom_locator_no_auth_runtime_gate",
        )
        self.assertEqual(by_id["clipalpha1"]["queue_ordinal"], 1)
        self.assertEqual(by_id["clipbravo2"]["queue_state"], "requires_metadata")
        self.assertIsNone(by_id["clipbravo2"]["queue_ordinal"])
        self.assertEqual(plan["summary"]["queued_count"], 1)
        self.assertFalse(plan["assertion_policy"]["publication_authority"])
        self.assertFalse(plan["assertion_policy"]["comments_collected"])

    def test_materialization_is_exact_replay_and_preserves_post_provenance(self):
        plan_path = self.write_plan()
        kwargs = {
            "plan_path": plan_path,
            "discovery_path": self.discovery,
            "selection_path": self.selection,
            "bundle_root": self.root / "bundles-private",
            "media_output_root": self.root / "media-private",
            "yt_dlp_executable": self.executable,
            "yt_dlp_sha256": sha(self.executable),
            "yt_dlp_version": "2026.fixture",
            "yt_dlp_module_root": self.runtime_root,
            "global_cache_cap_bytes": 2 * 1024**3,
            "free_space_floor_bytes": 0,
        }
        first = lane.materialize_bundle(**kwargs)
        first_bytes = first.read_bytes()
        second = lane.materialize_bundle(**kwargs)
        self.assertEqual(first, second)
        self.assertEqual(first.read_bytes(), first_bytes)
        manifest = json.loads(first_bytes)
        self.assertEqual(len(manifest["work_orders"]), 1)
        entry = manifest["work_orders"][0]
        self.assertEqual(entry["provenance"]["post_id"], "1abcde1")
        self.assertIn("/comments/1abcde1/", entry["provenance"]["post_permalink"])
        self.assertEqual(entry["provenance"]["title_assertion_state"], "unreviewed")
        self.assertFalse(entry["publication_authority"])
        self.assertFalse(manifest["assertion_policy"]["publication_authority"])
        self.assertFalse(manifest["yt_dlp"]["comments_allowed"])
        self.assertEqual(manifest["yt_dlp"]["expected_version"], "2026.fixture")
        self.assertEqual(
            manifest["yt_dlp"]["runtime_tree"]["sha256"],
            self.runtime_tree["sha256"],
        )
        order_path = first.parent / entry["work_order_file"]
        order = json.loads(order_path.read_text(encoding="utf-8"))
        self.assertEqual(acquire.validate_work_order(order), order)
        self.assertEqual(order["source"]["platform"], "reddit")
        self.assertEqual(order["source"]["source_kind"], "reddit_video")
        self.assertEqual(order["source"]["canonical_url"], "https://v.redd.it/clipalpha1")
        order_path.write_bytes(order_path.read_bytes() + b" ")
        with self.assertRaisesRegex(
            lane.RedditVideoAcquisitionError, "immutable Reddit video bundle differs"
        ):
            lane.materialize_bundle(**kwargs)

    def test_plan_and_discovery_tamper_fail_closed(self):
        plan = self.build_plan()
        plan["candidates"][0]["canonical_url"] = "https://v.redd.it/otherclip1"
        plan_path = self.write_plan(plan)
        with self.assertRaisesRegex(lane.RedditVideoAcquisitionError, "plan_id"):
            lane.reproduce_plan(
                plan_path=plan_path,
                discovery_path=self.discovery,
                selection_path=self.selection,
            )

        discovery_copy = self.root / "tampered.discovery.json"
        discovery_copy.write_bytes(self.discovery.read_bytes().replace(b"Unreviewed", b"Tampered!!", 1))
        with self.assertRaises(lane.RedditVideoAcquisitionError):
            lane.load_discovery(discovery_copy)

    def test_selection_rejects_unknown_fields_duplicates_and_missing_ids(self):
        selection, _body = lane.load_selection(self.selection)
        selection["comments"] = True
        with self.assertRaisesRegex(lane.RedditVideoAcquisitionError, "unknown"):
            lane.validate_selection(selection)
        selection.pop("comments")
        selection["reddit_video_ids"] = ["clipalpha1", "clipalpha1"]
        with self.assertRaisesRegex(lane.RedditVideoAcquisitionError, "sorted and unique"):
            lane.validate_selection(selection)
        selection["reddit_video_ids"] = ["absentclip9"]
        self.selection.write_text(json.dumps(selection), encoding="utf-8")
        with self.assertRaisesRegex(lane.RedditVideoAcquisitionError, "absent"):
            self.build_plan()

    def test_guarded_adapter_accepts_only_exact_canonical_reddit_media(self):
        normalized = acquire.validate_work_order(self.reddit_order())
        self.assertEqual(normalized["source"]["canonical_url"], "https://v.redd.it/clipalpha1")
        bad_urls = [
            "https://www.reddit.com/r/HIMRFAM2/comments/1abcde1/example/",
            "https://v.redd.it.evil.example/clipalpha1",
            "https://v.redd.it/clipalpha1/extra",
            "https://v.redd.it/clipalpha1?" + "token=secret",
            "http://v.redd.it/clipalpha1",
            "https://example.com/video.mp4",
        ]
        for url in bad_urls:
            with self.subTest(url=url), self.assertRaises(acquire.AcquisitionError):
                acquire.validate_work_order(self.reddit_order(url=url))

        confused = self.reddit_order(source={"platform": "web"})
        with self.assertRaisesRegex(acquire.AcquisitionError, "requires source.platform"):
            acquire.validate_work_order(confused)

        direct = self.reddit_order()
        direct["adapter"] = "direct_http"
        direct["adapter_config"] = {
            "url": "https://v.redd.it/clipalpha1",
            "resume": True,
            "timeout_seconds": 30,
            "expected_sha256": None,
            "expected_byte_count": None,
        }
        with self.assertRaisesRegex(acquire.AcquisitionError, "hash-pinned yt_dlp"):
            acquire.validate_work_order(direct)

    def test_guarded_adapter_rejects_missing_pin_unsafe_selector_and_tokens(self):
        without_pin = self.reddit_order(
            adapter_config={"expected_executable_sha256": None}
        )
        with self.assertRaisesRegex(acquire.AcquisitionError, "hash-pinned"):
            acquire.validate_work_order(without_pin)
        without_version = self.reddit_order()
        without_version["adapter_config"].pop("expected_ytdlp_version")
        with self.assertRaisesRegex(acquire.AcquisitionError, "expected yt-dlp version"):
            acquire.validate_work_order(without_version)
        without_runtime_tree = self.reddit_order()
        without_runtime_tree["adapter_config"].pop("expected_runtime_tree_root")
        without_runtime_tree["adapter_config"].pop("expected_runtime_tree_sha256")
        with self.assertRaisesRegex(acquire.AcquisitionError, "runtime module tree"):
            acquire.validate_work_order(without_runtime_tree)
        unsafe_selector = self.reddit_order(
            adapter_config={"format_selector": "all,-storyboard"}
        )
        with self.assertRaisesRegex(acquire.AcquisitionError, "safe selector"):
            acquire.validate_work_order(unsafe_selector)
        forbidden_fields = {
            "authorization_token": "forbidden",
            "cookies": "/tmp/cookies.txt",
            "cookies_from_browser": "firefox",
            "username": "account",
            "password": "secret",
            "playlist_items": "1-5",
            "write_comments": True,
        }
        for key, value in forbidden_fields.items():
            token_field = self.reddit_order()
            token_field["adapter_config"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(
                acquire.AcquisitionError, "unsupported keys"
            ):
                acquire.validate_work_order(token_field)

    def test_ytdlp_version_and_runtime_tree_are_exact_and_tamper_evident(self):
        version, runtime = lane.stable_ytdlp_runtime(
            self.executable, "2026.fixture", self.runtime_root
        )
        self.assertEqual(version, "2026.fixture")
        self.assertEqual(runtime, self.runtime_tree)
        with self.assertRaisesRegex(
            lane.RedditVideoAcquisitionError, "version does not match"
        ):
            lane.stable_ytdlp_runtime(
                self.executable, "2026.wrong", self.runtime_root
            )

        normalized = acquire.validate_work_order(self.reddit_order())
        acquire.verify_ytdlp_runtime_tree(
            normalized["adapter_config"], "positive fixture"
        )
        (self.runtime_root / "__init__.py").write_text(
            "__version__ = 'tampered'\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(acquire.AcquisitionError, "SHA-256 mismatch"):
            acquire.verify_ytdlp_runtime_tree(
                normalized["adapter_config"], "after tamper"
            )

    def test_live_adapter_rechecks_exact_version_after_execution(self):
        marker = self.root / "version-changed.marker"
        executable = self.root / "changing-yt-dlp"
        executable.write_text(
            "#!/usr/bin/env python3\n"
            "import json, pathlib, sys\n"
            f"marker = pathlib.Path({str(marker)!r})\n"
            "if sys.argv[1:] == ['--version']:\n"
            "    print('changed-after-run' if marker.exists() else '2026.fixture')\n"
            "    raise SystemExit(0)\n"
            "template = sys.argv[sys.argv.index('--output') + 1]\n"
            "pathlib.Path(template.replace('%(ext)s', 'mp4')).write_bytes(b'fixture')\n"
            "marker.write_text('changed', encoding='utf-8')\n"
            "print(json.dumps({\n"
            "  'id': 'clipalpha1',\n"
            "  'original_url': 'https://v.redd.it/clipalpha1',\n"
            "  'webpage_url': 'https://www.reddit.com/r/HIMRFAM2/comments/1abcde1/example/',\n"
            "  'url': 'https://v.redd.it/clipalpha1/DASH_720.mp4'\n"
            "}))\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)
        order = self.reddit_order()
        order["adapter_config"]["executable"] = str(executable)
        order["adapter_config"]["expected_executable_sha256"] = sha(executable)
        normalized = acquire.validate_work_order(order)
        output_root = self.root / "live-version-check"
        output_root.mkdir()
        stage_dir = output_root / ".staging" / "fixture"
        with self.assertRaisesRegex(acquire.AcquisitionError, "version mismatch after"):
            acquire.download_ytdlp(
                config=normalized["adapter_config"],
                source=normalized["source"],
                stage_dir=stage_dir,
                output_root=output_root,
                limits=normalized["limits"],
                max_bytes=normalized["limits"]["max_job_bytes"],
            )
        self.assertFalse(stage_dir.exists())

    def test_reddit_result_identity_rejects_discussion_or_unrelated_redirect(self):
        source = self.reddit_order()["source"]
        expected_permalink = (
            "https://www.reddit.com/r/HIMRFAM2/comments/1abcde1/example/"
        )
        selected = acquire.safe_ytdlp_metadata(
            {
                "id": "clipalpha1",
                "original_url": "https://v.redd.it/clipalpha1",
                "webpage_url": expected_permalink,
            },
            expected_reddit_webpage_url=expected_permalink,
        )
        self.assertEqual(selected["original_url"], "https://v.redd.it/clipalpha1")
        self.assertEqual(selected["webpage_url"], expected_permalink)
        acquire.validate_ytdlp_selected_identity(
            source,
            selected,
            expected_permalink,
        )
        with self.assertRaisesRegex(acquire.AcquisitionError, "sealed Reddit permalink"):
            acquire.validate_ytdlp_selected_identity(
                source,
                {
                    "id": "clipalpha1",
                    "original_url": "https://v.redd.it/clipalpha1",
                    "webpage_url": "https://www.reddit.com/r/x/comments/1abcde1/post/",
                },
                expected_permalink,
            )
        with self.assertRaisesRegex(acquire.AcquisitionError, "exact requested"):
            acquire.validate_ytdlp_selected_identity(
                source,
                {
                    "id": "clipalpha1",
                    "original_url": "https://v.redd.it/wrongclip9",
                    "webpage_url": expected_permalink,
                },
                expected_permalink,
            )
        with self.assertRaisesRegex(acquire.AcquisitionError, "exact requested"):
            acquire.validate_ytdlp_selected_identity(
                source, {"id": "clipalpha1"}, expected_permalink
            )
        acquire.validate_ytdlp_raw_identity(
            source,
            {
                "id": "clipalpha1",
                "original_url": "https://v.redd.it/clipalpha1",
                "webpage_url": "https://www.reddit.com/r/HIMRFAM2/comments/1abcde1/example/",
                "requested_formats": [
                    {"url": "https://v.redd.it/clipalpha1/DASH_720.mp4"},
                    {
                        "url": "https://v.redd.it/clipalpha1/audio.mp4",
                        "manifest_url": "https://v.redd.it/clipalpha1/DASHPlaylist.mpd?a=public",
                    },
                ],
            },
            expected_permalink,
        )
        with self.assertRaisesRegex(acquire.AcquisitionError, "must stay under"):
            acquire.validate_ytdlp_raw_identity(
                source,
                {
                    "id": "clipalpha1",
                    "original_url": "https://v.redd.it/clipalpha1",
                    "webpage_url": expected_permalink,
                    "url": "https://v.redd.it.evil.example/clipalpha1/video.mp4",
                },
                expected_permalink,
            )
        with self.assertRaises(acquire.AcquisitionError):
            acquire.validate_ytdlp_raw_identity(
                source,
                {
                    "id": "clipalpha1",
                    "original_url": "https://v.redd.it/clipalpha1",
                    "webpage_url": "https://www.reddit.com/r/x/comments/1abcde1/post/",
                },
                expected_permalink,
            )

    def test_caps_are_recomputed_not_trusted(self):
        plan = self.build_plan(max_duration_ms=10_000)
        candidate = plan["candidates"][0]
        self.assertEqual(candidate["queue_state"], "exceeds_duration_cap")
        self.assertIsNone(candidate["queue_ordinal"])
        with self.assertRaisesRegex(lane.RedditVideoAcquisitionError, "may not exceed"):
            lane.validate_policy(self.policy(max_job_bytes=10, plan_budget_bytes=9))


def tearDownModule():
    shutil.rmtree(ACQUISITION_ROOT / ".test-reddit-video", ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
