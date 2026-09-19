from __future__ import annotations

import io
import json
import stat
import sys
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from acquisition.archive_org_metadata import (  # noqa: E402
    ACCEPT,
    USER_AGENT,
    ArchiveMetadataError,
    _write_delta,
    capture_snapshot,
    make_writable,
    stable_id,
    validate_delta,
    validate_request_manifest,
    validate_snapshot,
)


def metadata(identifier: str, files: list[dict] | None = None) -> bytes:
    return json.dumps(
        {
            "metadata": {
                "identifier": identifier,
                "title": f"Provider title for {identifier}",
                "collection": ["opensource_movies", "fixture"],
            },
            "files": files or [],
        },
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")


class FakeResponse(io.BytesIO):
    status = 200

    def __init__(self, body: bytes, url: str):
        super().__init__(body)
        self._url = url
        self.headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(body)),
            "ETag": '"fixture-etag"',
            "Last-Modified": "Wed, 26 Aug 2026 12:00:00 GMT",
            "Date": "Wed, 26 Aug 2026 12:00:01 GMT",
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
    def __init__(self, body: bytes, *, final_url: str | None = None):
        self.body = body
        self.final_url = final_url
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return FakeResponse(self.body, self.final_url or request.full_url)


class FixedClock:
    def __init__(self, *values: str):
        self.values = iter(values)

    def __call__(self) -> str:
        return next(self.values)


class ArchiveOrgMetadataTests(unittest.TestCase):
    def setUp(self):
        work = REPOSITORY_ROOT / "corpus" / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="archive-metadata-test-", dir=work)
        self.root = Path(self.temporary.name).resolve()

    def tearDown(self):
        make_writable(self.root)
        self.temporary.cleanup()

    def request(self, name: str, items: list[tuple[str, str]], requested_at: str) -> Path:
        body = {
            "schema_version": 1,
            "request_kind": "archive_org_metadata_targets",
            "requested_at": requested_at,
            "items": [
                {"identifier": identifier, "basis": basis}
                for identifier, basis in sorted(items)
            ],
            "policy": {
                "public_unauthenticated_metadata_only": True,
                "media_download": False,
                "cookies_sent": False,
                "authorization_sent": False,
                "publication_authority": False,
            },
        }
        body["request_id"] = stable_id("iamr", body)
        path = self.root / name
        path.write_text(json.dumps(body, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        return path

    def capture(
        self,
        request: Path,
        payloads: dict[str, bytes],
        times: list[str],
        output_name: str,
    ) -> tuple[Path, dict[str, FakeOpener]]:
        openers = {identifier: FakeOpener(body) for identifier, body in payloads.items()}
        snapshot = capture_snapshot(
            request,
            output_root=self.root / output_name,
            opener_factory=openers.__getitem__,
            clock=FixedClock(*times),
        )
        return snapshot, openers

    def test_capture_is_sealed_exact_and_never_sends_credentials(self):
        request = self.request(
            "targets.json",
            [("fixture-a", "catalog_archive_item")],
            "2026-08-26T12:00:00Z",
        )
        payload = metadata("fixture-a", [{"name": "clip.mp4", "size": "123"}])
        snapshot_path, openers = self.capture(
            request,
            {"fixture-a": payload},
            ["2026-08-26T12:00:01Z", "2026-08-26T12:00:02Z"],
            "snapshots",
        )
        result = validate_snapshot(snapshot_path)

        self.assertEqual(result["items"][0]["file_count"], 1)
        self.assertEqual((snapshot_path.parent / "item-fixture-a.metadata.json").read_bytes(), payload)
        self.assertFalse(snapshot_path.parent.stat().st_mode & stat.S_IWUSR)
        request_object, timeout = openers["fixture-a"].requests[0]
        headers = {key.lower(): value for key, value in request_object.header_items()}
        self.assertEqual(request_object.full_url, "https://archive.org/metadata/fixture-a")
        self.assertEqual(request_object.method, "GET")
        self.assertEqual(timeout, 60)
        self.assertEqual(headers["accept"], ACCEPT)
        self.assertEqual(headers["user-agent"], USER_AGENT)
        self.assertNotIn("authorization", headers)
        self.assertNotIn("cookie", headers)

        # A byte change remains detectable even if an attacker reseals the file.
        evidence = snapshot_path.parent / "item-fixture-a.metadata.json"
        evidence.chmod(0o600)
        evidence.write_bytes(payload + b" ")
        evidence.chmod(0o400)
        with self.assertRaisesRegex(ArchiveMetadataError, "payload (?:bytes differ|exceeds)"):
            validate_snapshot(snapshot_path)

    def test_request_contract_rejects_unsorted_targets(self):
        request = self.request(
            "targets.json",
            [("fixture-a", "catalog_archive_item"), ("fixture-b", "manual_public_lead")],
            "2026-08-26T12:00:00Z",
        )
        body = json.loads(request.read_text())
        body["items"].reverse()
        identity = {key: value for key, value in body.items() if key != "request_id"}
        body["request_id"] = stable_id("iamr", identity)
        request.write_text(json.dumps(body))
        with self.assertRaisesRegex(ArchiveMetadataError, "unique and sorted"):
            validate_request_manifest(request)

        duplicate = self.root / "duplicate-key.json"
        duplicate.write_text('{"schema_version":1,"schema_version":1}\n')
        with self.assertRaisesRegex(ArchiveMetadataError, "duplicate JSON key"):
            validate_request_manifest(duplicate)

    def test_capture_rejects_wrong_item_and_cross_host_final_url(self):
        request = self.request(
            "targets.json",
            [("fixture-a", "catalog_archive_item")],
            "2026-08-26T12:00:00Z",
        )
        with self.assertRaisesRegex(ArchiveMetadataError, "identifier differs"):
            self.capture(
                request,
                {"fixture-a": metadata("different-item")},
                ["2026-08-26T12:00:01Z", "2026-08-26T12:00:02Z"],
                "wrong-item",
            )

        opener = FakeOpener(
            metadata("fixture-a"),
            final_url="https://example.com/metadata/fixture-a",
        )
        with self.assertRaisesRegex(ArchiveMetadataError, "exact public metadata endpoint"):
            capture_snapshot(
                request,
                output_root=self.root / "wrong-host",
                opener_factory=lambda _identifier: opener,
                clock=FixedClock("2026-08-26T12:00:01Z"),
            )

    def test_capture_rejects_a_future_sealed_request_time(self):
        request = self.request(
            "future-targets.json",
            [("fixture-a", "catalog_archive_item")],
            "2026-08-26T12:01:00Z",
        )
        with self.assertRaisesRegex(ArchiveMetadataError, "before the sealed request"):
            self.capture(
                request,
                {"fixture-a": metadata("fixture-a")},
                ["2026-08-26T12:00:01Z", "2026-08-26T12:00:02Z"],
                "future-request",
            )

    def test_delta_reproduces_full_file_record_changes(self):
        previous_request = self.request(
            "previous-request.json",
            [("fixture-a", "catalog_archive_item"), ("fixture-b", "catalog_file_hint")],
            "2026-08-26T12:00:00Z",
        )
        previous, _ = self.capture(
            previous_request,
            {
                "fixture-a": metadata(
                    "fixture-a",
                    [
                        {"name": "same.mp4", "md5": "a"},
                        {"name": "changed.mp4", "md5": "old"},
                        {"name": "removed.mp4", "md5": "gone"},
                    ],
                ),
                "fixture-b": metadata("fixture-b", [{"name": "only-b.mp4"}]),
            },
            [
                "2026-08-26T12:00:01Z",
                "2026-08-26T12:00:02Z",
                "2026-08-26T12:00:03Z",
                "2026-08-26T12:00:04Z",
            ],
            "previous",
        )
        current_request = self.request(
            "current-request.json",
            [("fixture-a", "catalog_archive_item"), ("fixture-c", "manual_public_lead")],
            "2026-08-27T12:00:00Z",
        )
        current, _ = self.capture(
            current_request,
            {
                "fixture-a": metadata(
                    "fixture-a",
                    [
                        {"name": "same.mp4", "md5": "a"},
                        {"name": "changed.mp4", "md5": "new"},
                        {"name": "added.mp4", "md5": "new"},
                    ],
                ),
                "fixture-c": metadata("fixture-c", [{"name": "only-c.mp4"}]),
            },
            [
                "2026-08-27T12:00:01Z",
                "2026-08-27T12:00:02Z",
                "2026-08-27T12:00:03Z",
                "2026-08-27T12:00:04Z",
            ],
            "current",
        )

        delta_path = _write_delta(previous, current, self.root / "deltas" / "delta.json")
        self.assertEqual(
            _write_delta(previous, current, self.root / "deltas" / "delta.json"),
            delta_path,
        )
        delta = validate_delta(delta_path, previous, current)
        self.assertEqual(
            delta["summary"],
            {
                "added_items": 1,
                "removed_items": 1,
                "changed_items": 1,
                "unchanged_items": 0,
                "added_files": 2,
                "removed_files": 2,
                "changed_files": 1,
            },
        )
        changed = next(item for item in delta["items"] if item["identifier"] == "fixture-a")
        self.assertEqual(changed["added_files"], ["added.mp4"])
        self.assertEqual(changed["removed_files"], ["removed.mp4"])
        self.assertEqual(changed["changed_files"], ["changed.mp4"])
        self.assertFalse(delta["assertion_policy"]["content_change_asserted"])


if __name__ == "__main__":
    unittest.main()
