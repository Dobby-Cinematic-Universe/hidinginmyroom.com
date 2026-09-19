from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from contextlib import redirect_stderr
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from acquisition.reddit_rss import (  # noqa: E402
    RedditRssError,
    capture_snapshot,
    main,
    validate_discovery_manifest,
    validate_snapshot_manifest,
    write_discovery_manifest,
)


ATOM_PAYLOAD = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>fixture feed</title>
  <entry>
    <id>t3_1vvver5</id>
    <title>Unreviewed &amp; contextual title</title>
    <published>2026-08-25T12:00:00+00:00</published>
    <updated>2026-08-25T12:01:00+00:00</updated>
    <link href="https://www.reddit.com/r/HIMRFAM2/comments/1vvver5/example/" />
    <author><name>sensitive_handle_must_not_be_derived</name></author>
    <content type="html">&lt;p&gt;body_marker_must_not_be_derived&lt;/p&gt;
      &lt;a href="https://v.redd.it/ftkqj9bfg1lh1"&gt;video&lt;/a&gt;
      &lt;a href="https://www.youtube.com/watch?v=Z32Y-D5kJTg"&gt;youtube&lt;/a&gt;
      &lt;a href="https://archive.org/details/fixture-item"&gt;archive item&lt;/a&gt;
      &lt;a href="https://archive.org/download/fixture-item/clip.mp4"&gt;archive&lt;/a&gt;
      &lt;a href="https://www.reddit.com/gallery/1vvver5"&gt;gallery&lt;/a&gt;
      &lt;img src="https://i.redd.it/example123.jpg?width=640&amp;amp;format=pjpg" /&gt;
    </content>
  </entry>
</feed>
"""


class FakeResponse(io.BytesIO):
    status = 200

    def __init__(self, payload: bytes, url: str):
        super().__init__(payload)
        self._url = url
        self.headers = {
            "Content-Type": "application/atom+xml; charset=UTF-8",
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
    def __init__(self, payload: bytes = ATOM_PAYLOAD):
        self.payload = payload

    def open(self, request, timeout):
        self.request = request
        self.timeout = timeout
        headers = {key.lower(): value for key, value in request.header_items()}
        if "cookie" in headers or "authorization" in headers:
            raise AssertionError("capture sent credentials")
        return FakeResponse(self.payload, request.full_url)


class RedditRssTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def _capture(self) -> Path:
        return capture_snapshot(
            subreddit="HIMRFAM2",
            limit=100,
            out_dir=self.root,
            opener=FakeOpener(),
        )

    def _write_info(self, name: str = "public.info.json") -> Path:
        path = self.root / name
        path.write_text(
            json.dumps(
                {
                    "id": "1vvver5",
                    "webpage_url": "https://www.reddit.com/r/HIMRFAM2/comments/1vvver5/example/",
                    "duration": 139,
                    "availability": "public",
                    "uploader": "must_not_be_copied",
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _set_mtime(path: Path, value: str, *, extra_nanoseconds: int = 0) -> None:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        seconds = (parsed - epoch).days * 86_400 + (parsed - epoch).seconds
        timestamp_ns = seconds * 1_000_000_000 + extra_nanoseconds
        os.utime(path, ns=(timestamp_ns, timestamp_ns))

    def test_capture_parse_and_minimize_public_atom(self):
        snapshot_path = self._capture()
        snapshot = validate_snapshot_manifest(snapshot_path, verify_payload=True)
        self.assertFalse(snapshot["request"]["cookies_sent"])
        self.assertFalse(snapshot["request"]["authorization_sent"])
        self.assertEqual(snapshot["response"]["byte_count"], len(ATOM_PAYLOAD))

        info_path = self._write_info()
        self._set_mtime(
            info_path,
            "2026-08-26T12:04:59Z",
            extra_nanoseconds=999_999_999,
        )
        with patch(
            "acquisition.reddit_rss._now_utc_datetime",
            return_value=datetime(2026, 8, 26, 12, 5, 1, tzinfo=timezone.utc),
        ):
            discovery_path = write_discovery_manifest(
                snapshot_path,
                v_reddit_info_paths=[info_path],
                metadata_observed_at="2026-08-26T12:05:00Z",
            )
        discovery = validate_discovery_manifest(discovery_path, verify_artifacts=True)
        serialized = discovery_path.read_text(encoding="utf-8")
        self.assertNotIn("sensitive_handle_must_not_be_derived", serialized)
        self.assertNotIn("body_marker_must_not_be_derived", serialized)
        self.assertNotIn("must_not_be_copied", serialized)
        self.assertFalse(discovery["assertion_policy"]["titles_are_content_truth"])
        self.assertFalse(discovery["assertion_policy"]["authors_retained"])

        post = discovery["posts"][0]
        self.assertEqual(post["post_id"], "1vvver5")
        by_kind = {item["locator_kind"]: item for item in post["media_locators"]}
        self.assertEqual(by_kind["reddit_video"]["native_id"], "ftkqj9bfg1lh1")
        self.assertEqual(
            by_kind["reddit_video"]["metadata_observation"]["duration_ms"],
            139000,
        )
        self.assertEqual(by_kind["youtube_video"]["native_id"], "Z32Y-D5kJTg")
        self.assertEqual(by_kind["internet_archive_item"]["native_id"], "fixture-item")
        self.assertEqual(by_kind["internet_archive_file"]["native_id"], "fixture-item/clip.mp4")
        self.assertEqual(by_kind["reddit_gallery"]["native_id"], "1vvver5")
        self.assertEqual(by_kind["image"]["canonical_url"], "https://i.redd.it/example123.jpg")

    def test_parse_cli_rejects_future_metadata_time_without_writing(self):
        snapshot_path = self._capture()
        info_path = self._write_info()
        self._set_mtime(info_path, "2026-08-26T12:04:00Z")

        stderr = io.StringIO()
        with patch(
            "acquisition.reddit_rss._now_utc_datetime",
            return_value=datetime(2026, 8, 26, 12, 5, 0, tzinfo=timezone.utc),
        ):
            with redirect_stderr(stderr):
                status = main(
                    [
                        "parse",
                        "--snapshot",
                        str(snapshot_path),
                        "--v-reddit-info",
                        str(info_path),
                        "--metadata-observed-at",
                        "2026-08-26T12:05:01Z",
                    ]
                )

        self.assertEqual(status, 2)
        self.assertIn("metadata-observed-at must not be in the future", stderr.getvalue())
        self.assertEqual(list(self.root.glob("rdd_*.discovery.json")), [])

    def test_metadata_observation_rejects_time_before_newest_info_mtime(self):
        snapshot_path = self._capture()
        older = self._write_info("older.info.json")
        newest = self._write_info("newest.info.json")
        self._set_mtime(older, "2026-08-26T12:04:59Z")
        self._set_mtime(
            newest,
            "2026-08-26T12:05:00Z",
            extra_nanoseconds=1,
        )

        with patch(
            "acquisition.reddit_rss._now_utc_datetime",
            return_value=datetime(2026, 8, 26, 12, 6, 0, tzinfo=timezone.utc),
        ):
            with self.assertRaisesRegex(
                RedditRssError,
                "earliest whole-second value is 2026-08-26T12:05:01Z",
            ):
                write_discovery_manifest(
                    snapshot_path,
                    v_reddit_info_paths=[older, newest],
                    metadata_observed_at="2026-08-26T12:05:00Z",
                )

        self.assertEqual(list(self.root.glob("rdd_*.discovery.json")), [])

    def test_metadata_observation_accepts_exact_whole_second_mtime(self):
        snapshot_path = self._capture()
        info_path = self._write_info()
        self._set_mtime(info_path, "2026-08-26T12:05:00Z")

        with patch(
            "acquisition.reddit_rss._now_utc_datetime",
            return_value=datetime(2026, 8, 26, 12, 5, 0, tzinfo=timezone.utc),
        ):
            path = write_discovery_manifest(
                snapshot_path,
                v_reddit_info_paths=[info_path],
                metadata_observed_at="2026-08-26T12:05:00Z",
            )

        self.assertTrue(path.is_file())

    def test_metadata_observation_requires_whole_seconds(self):
        snapshot_path = self._capture()
        info_path = self._write_info()
        self._set_mtime(info_path, "2026-08-26T12:04:00Z")

        with patch(
            "acquisition.reddit_rss._now_utc_datetime",
            return_value=datetime(2026, 8, 26, 12, 6, 0, tzinfo=timezone.utc),
        ):
            with self.assertRaisesRegex(RedditRssError, "whole-second UTC"):
                write_discovery_manifest(
                    snapshot_path,
                    v_reddit_info_paths=[info_path],
                    metadata_observed_at="2026-08-26T12:05:00.5Z",
                )

        self.assertEqual(list(self.root.glob("rdd_*.discovery.json")), [])

    def test_payload_tamper_fails_closed(self):
        snapshot_path = self._capture()
        snapshot = validate_snapshot_manifest(snapshot_path, verify_payload=True)
        payload_path = self.root / snapshot["response"]["payload_file"]
        payload_path.write_bytes(payload_path.read_bytes() + b" ")
        with self.assertRaisesRegex(RedditRssError, "do not match"):
            validate_snapshot_manifest(snapshot_path, verify_payload=True)

    def test_http_429_is_retryable_and_writes_no_snapshot(self):
        class RateLimited:
            def open(self, request, timeout):
                raise urllib.error.HTTPError(request.full_url, 429, "rate limited", {}, None)

        with self.assertRaisesRegex(RedditRssError, "HTTP 429; retryable=true"):
            capture_snapshot(
                subreddit="HIMRFAM2",
                limit=100,
                out_dir=self.root,
                opener=RateLimited(),
            )
        self.assertEqual(list(self.root.iterdir()), [])

    def test_relative_output_directory_is_rejected(self):
        with self.assertRaisesRegex(RedditRssError, "absolute"):
            capture_snapshot(
                subreddit="HIMRFAM2",
                limit=100,
                out_dir=Path("relative"),
                opener=FakeOpener(),
            )


if __name__ == "__main__":
    unittest.main()
