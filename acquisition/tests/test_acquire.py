from __future__ import annotations

import hashlib
import fcntl
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
PROGRAM = ACQUISITION_ROOT / "acquire.py"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
RESULT_SCHEMA = ACQUISITION_ROOT / "schemas" / "result.schema.json"
WORK_ORDER_SCHEMA = ACQUISITION_ROOT / "schemas" / "work-order.schema.json"
TEST_ROOT = ACQUISITION_ROOT / ".test-work"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=check,
    )


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def load_acquire_module():
    spec = importlib.util.spec_from_file_location("himr_acquire_test_module", PROGRAM)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load acquisition implementation for focused tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def generate_fixture(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=s=160x120:r=15:d=1.25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=16000:duration=1.25",
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            str(path),
        ]
    )


def limits(max_job_bytes: int = 10 * 1024 * 1024) -> dict[str, int]:
    return {
        "max_job_bytes": max_job_bytes,
        "global_cache_cap_bytes": 100 * 1024 * 1024,
        "free_space_floor_bytes": 0,
    }


def source(platform: str, native_id: str, canonical_url: str | None = None) -> dict:
    return {
        "platform": platform,
        "source_kind": "video",
        "native_id": native_id,
        "canonical_url": canonical_url,
        "title": "Generated acquisition fixture",
        "published_at": None,
        "access_state": "public",
    }


def local_order(
    source_path: Path,
    output_root: Path,
    *,
    job_id: str = "local-fixture-001",
    job_limits: dict[str, int] | None = None,
) -> dict:
    local_source = source("local", source_path.name)
    local_source["access_state"] = "unknown"
    return {
        "schema_version": 1,
        "job_id": job_id,
        "adapter": "local_file",
        "source": local_source,
        "adapter_config": {
            "path": str(source_path.resolve()),
            "expected_sha256": digest(source_path),
            "expected_byte_count": source_path.stat().st_size,
        },
        "output": {"root": str(output_root.resolve())},
        "limits": job_limits or limits(),
    }


def http_order(url: str, output_root: Path, *, job_id: str = "http-fixture-001") -> dict:
    return {
        "schema_version": 1,
        "job_id": job_id,
        "adapter": "direct_http",
        "source": source("archive_org", job_id, url),
        "adapter_config": {
            "url": url,
            "resume": True,
            "timeout_seconds": 10,
            "expected_sha256": None,
            "expected_byte_count": None,
        },
        "output": {"root": str(output_root.resolve())},
        "limits": limits(),
    }


class FixtureHTTPHandler(BaseHTTPRequestHandler):
    payload = b""
    truncate_first_response = False
    truncated = False
    ranges: list[str | None] = []
    etag = '"fixture-v1"'

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path != "/media.mp4":
            self.send_error(404)
            return
        requested_range = self.headers.get("Range")
        type(self).ranges.append(requested_range)
        start = 0
        if requested_range:
            prefix = "bytes="
            if not requested_range.startswith(prefix) or not requested_range.endswith("-"):
                self.send_error(416)
                return
            start = int(requested_range[len(prefix) : -1])
            if start >= len(self.payload):
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(self.payload)}")
                self.end_headers()
                return

        body = self.payload[start:]
        self.send_response(206 if start else 200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", self.etag)
        if start:
            self.send_header(
                "Content-Range", f"bytes {start}-{len(self.payload) - 1}/{len(self.payload)}"
            )
        self.end_headers()

        if self.truncate_first_response and not type(self).truncated and not start:
            type(self).truncated = True
            halfway = max(1, len(body) // 2)
            self.wfile.write(body[:halfway])
            self.wfile.flush()
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


class AcquisitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            raise unittest.SkipTest("ffmpeg and ffprobe are required")
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        cls.fixture = TEST_ROOT / "fixture.mp4"
        generate_fixture(cls.fixture)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, exist_ok=True)

    def write_order(self, value: dict) -> Path:
        path = self.case / "work-order.json"
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return path

    def execute(self, value: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        return run(
            [
                "python3",
                str(PROGRAM),
                "run",
                "--work-order",
                str(self.write_order(value)),
                *arguments,
            ],
            check=False,
        )

    def assert_result_contract(self, result: dict, filename: str) -> None:
        result_path = self.case / filename
        result_path.write_text(json.dumps(result), encoding="utf-8")
        completed = run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(RESULT_SCHEMA),
                str(result_path),
            ],
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            f"Generated acquisition result failed its JSON contract:\n"
            f"{completed.stdout}{completed.stderr}",
        )

    def assert_work_order_contract(self, order: dict, filename: str) -> None:
        order_path = self.case / filename
        order_path.write_text(json.dumps(order), encoding="utf-8")
        completed = run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(WORK_ORDER_SCHEMA),
                str(order_path),
            ],
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            f"Generated acquisition work order failed its JSON contract:\n"
            f"{completed.stdout}{completed.stderr}",
        )

    def serve_fixture(self, *, truncate_once: bool = False):
        FixtureHTTPHandler.payload = self.fixture.read_bytes()
        FixtureHTTPHandler.truncate_first_response = truncate_once
        FixtureHTTPHandler.truncated = False
        FixtureHTTPHandler.ranges = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHTTPHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, thread

    def test_local_admission_preserves_source_and_reuses_result(self) -> None:
        output_root = self.case / "cache"
        before = self.fixture.stat()
        first = self.execute(local_order(self.fixture, output_root))
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        self.assert_result_contract(result, "local-result.contract.json")
        self.assertEqual(result["status"], "completed")
        self.assertFalse(result["dry_run"])
        admitted = Path(result["admission"]["path"])
        self.assertEqual(admitted.read_bytes(), self.fixture.read_bytes())
        self.assertEqual(result["admission"]["sha256"], digest(self.fixture))
        self.assertTrue(result["source_observation"]["local_source"]["unchanged"])
        self.assertEqual(self.fixture.stat().st_size, before.st_size)
        self.assertEqual(self.fixture.stat().st_mtime_ns, before.st_mtime_ns)
        self.assertEqual(
            result["catalog_records"]["media_objects"][0]["first_cataloged_at"],
            result["completed_at"],
        )
        self.assertEqual(
            result["catalog_records"]["media_locations"][0]["storage_uri"],
            admitted.as_uri(),
        )

        second = self.execute(local_order(self.fixture, output_root))
        self.assertEqual(second.returncode, 0, second.stderr)
        reused = json.loads(second.stdout)
        self.assert_result_contract(reused, "local-reused-result.contract.json")
        self.assertTrue(reused["reused"])
        self.assertIn("reuse_verified_at", reused)
        self.assertFalse((output_root / ".staging").exists())

    def test_private_handling_policy_is_sealed_and_preserved(self) -> None:
        output_root = self.case / "private-cache"
        order = local_order(
            self.fixture,
            output_root,
            job_id="private-handling-policy-001",
        )
        policy = {
            "storage_scope": "private_canonical_cache",
            "publication_disposition": "never_publish",
            "publication_authority": "none",
            "basis": "Explicit contributor instruction: do not publish this copy.",
        }
        order["handling_policy"] = policy
        self.assert_work_order_contract(order, "private-handling-work-order.contract.json")

        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "private-handling-result.contract.json")
        self.assertEqual(
            result["catalog_records"]["sources"][0]["metadata_json"][
                "handling_policy"
            ],
            policy,
        )

        reused = self.execute(order)
        self.assertEqual(reused.returncode, 0, reused.stderr)
        reused_result = json.loads(reused.stdout)
        self.assertTrue(reused_result["reused"])
        self.assertEqual(
            reused_result["catalog_records"]["sources"][0]["metadata_json"][
                "handling_policy"
            ],
            policy,
        )

        for label, mutate in (
            (
                "unknown disposition",
                lambda value: value["handling_policy"].__setitem__(
                    "publication_disposition", "publish"
                ),
            ),
            (
                "publication authority",
                lambda value: value["handling_policy"].__setitem__(
                    "publication_authority", "operator"
                ),
            ),
            (
                "unknown key",
                lambda value: value["handling_policy"].__setitem__(
                    "exception", True
                ),
            ),
        ):
            with self.subTest(label=label):
                invalid = json.loads(json.dumps(order))
                mutate(invalid)
                rejected = self.execute(invalid, "--dry-run")
                self.assertEqual(rejected.returncode, 2)
                self.assertFalse((self.case / f"rejected-{label}").exists())

    def test_create_work_order_requires_complete_private_handling_policy(self) -> None:
        command = [
            "python3",
            str(PROGRAM),
            "create-work-order",
            "--job-id",
            "private-cli-policy-001",
            "--adapter",
            "local_file",
            "--platform",
            "youtube",
            "--source-kind",
            "youtube_video",
            "--native-id",
            "privateFixture01",
            "--canonical-url",
            "https://www.youtube.com/watch?v=privateFixture01",
            "--access-state",
            "unknown",
            "--local-path",
            str(self.fixture.resolve()),
            "--publication-disposition",
            "never_publish",
            "--output-root",
            str((self.case / "private-cache").resolve()),
        ]
        incomplete = run(command, check=False)
        self.assertEqual(incomplete.returncode, 2)
        self.assertIn(
            "must be supplied together",
            json.loads(incomplete.stderr)["error"]["message"],
        )

        completed = run(
            [
                *command,
                "--handling-basis",
                "Explicit contributor instruction.",
            ],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        order = json.loads(completed.stdout)
        self.assertEqual(
            order["handling_policy"],
            {
                "storage_scope": "private_canonical_cache",
                "publication_disposition": "never_publish",
                "publication_authority": "none",
                "basis": "Explicit contributor instruction.",
            },
        )

    def test_reuse_rejects_symlinked_result_and_payload(self) -> None:
        result_root = self.case / "result-symlink-cache"
        result_order = local_order(
            self.fixture, result_root, job_id="result-symlink-001"
        )
        first = self.execute(result_order)
        self.assertEqual(first.returncode, 0, first.stderr)
        result_path = Path(json.loads(first.stdout)["result_path"])
        saved_result = self.case / "saved-result.json"
        result_path.rename(saved_result)
        result_path.symlink_to(saved_result)

        rejected_result = self.execute(result_order)
        self.assertEqual(rejected_result.returncode, 2)
        self.assertIn(
            "not a symlink",
            json.loads(rejected_result.stderr)["error"]["message"],
        )
        self.assertTrue(result_path.is_symlink())

        directory_root = self.case / "result-directory-symlink-cache"
        directory_order = local_order(
            self.fixture, directory_root, job_id="result-directory-symlink-001"
        )
        first = self.execute(directory_order)
        self.assertEqual(first.returncode, 0, first.stderr)
        directory_result_path = Path(json.loads(first.stdout)["result_path"])
        result_directory = directory_result_path.parent
        saved_directory = self.case / "saved-result-directory"
        result_directory.rename(saved_directory)
        result_directory.symlink_to(saved_directory, target_is_directory=True)

        rejected_directory = self.execute(directory_order)
        self.assertEqual(rejected_directory.returncode, 2)
        self.assertIn(
            "path component",
            json.loads(rejected_directory.stderr)["error"]["message"],
        )
        self.assertTrue(result_directory.is_symlink())

        payload_root = self.case / "payload-symlink-cache"
        payload_order = local_order(
            self.fixture, payload_root, job_id="payload-symlink-001"
        )
        first = self.execute(payload_order)
        self.assertEqual(first.returncode, 0, first.stderr)
        payload_path = Path(json.loads(first.stdout)["admission"]["path"])
        saved_payload = self.case / "saved-payload.mp4"
        payload_path.rename(saved_payload)
        payload_path.symlink_to(saved_payload)

        rejected_payload = self.execute(payload_order)
        self.assertEqual(rejected_payload.returncode, 2)
        self.assertIn(
            "not a symlink",
            json.loads(rejected_payload.stderr)["error"]["message"],
        )
        self.assertTrue(payload_path.is_symlink())

    def test_reuse_rejects_incomplete_or_inconsistent_envelopes(self) -> None:
        output_root = self.case / "cache"
        order = local_order(self.fixture, output_root, job_id="tampered-envelope-001")
        first = self.execute(order)
        self.assertEqual(first.returncode, 0, first.stderr)
        result_path = Path(json.loads(first.stdout)["result_path"])

        mutations = (
            lambda value: value.pop("capacity_after"),
            lambda value: value.__setitem__("result_path", str(self.case / "other.json")),
            lambda value: value["admission"].__setitem__(
                "byte_count", value["admission"]["byte_count"] + 1
            ),
            lambda value: value["catalog_records"]["media_locations"][0].__setitem__(
                "storage_uri", "file:///unrelated/payload"
            ),
            lambda value: value["source_observation"]["local_source"][
                "stat_after"
            ].__setitem__("mtime_ns", 0),
            lambda value: value["commands"][0].__setitem__(
                2, str(self.case / "unrelated-stage")
            ),
            lambda value: value.__setitem__("unexpected", True),
        )
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                tampered = json.loads(result_path.read_text(encoding="utf-8"))
                mutate(tampered)
                result_path.write_text(json.dumps(tampered), encoding="utf-8")
                repaired = self.execute(order)
                self.assertEqual(repaired.returncode, 0, repaired.stderr)
                repaired_result = json.loads(repaired.stdout)
                self.assertNotIn("reuse_verified_at", repaired_result)
                self.assertEqual(
                    json.loads(result_path.read_text(encoding="utf-8")), repaired_result
                )

    def test_reuse_result_json_is_strict_and_bounded(self) -> None:
        output_root = self.case / "cache"
        order = local_order(self.fixture, output_root, job_id="duplicate-result-json-001")
        first = self.execute(order)
        self.assertEqual(first.returncode, 0, first.stderr)
        result_path = Path(json.loads(first.stdout)["result_path"])
        result_path.write_text(
            '{"schema_version":1,"schema_version":1}\n', encoding="utf-8"
        )

        rejected = self.execute(order)
        self.assertEqual(rejected.returncode, 2)
        self.assertIn(
            "duplicate JSON key",
            json.loads(rejected.stderr)["error"]["message"],
        )

        result_path.unlink()
        result_path.write_bytes(b"{" + b" " * (16 * 1024 * 1024) + b"}")
        oversized = self.execute(order)
        self.assertEqual(oversized.returncode, 2)
        self.assertIn(
            "1..16777216 bytes",
            json.loads(oversized.stderr)["error"]["message"],
        )

    def test_pinned_result_detects_replacement_and_in_place_mutation(self) -> None:
        implementation = load_acquire_module()
        output_root = self.case / "cache"
        order = local_order(self.fixture, output_root, job_id="pinned-result-001")
        first = self.execute(order)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        result_path = Path(result["result_path"])

        pinned_result = implementation.PinnedRegularFile.open(
            result_path,
            root=output_root,
            maximum=implementation.MAX_DURABLE_RESULT_BYTES,
            capture=True,
            label="test durable result",
        )
        try:
            replacement = self.case / "replacement-result.json"
            replacement.write_bytes(result_path.read_bytes())
            os.replace(replacement, result_path)
            with self.assertRaisesRegex(
                implementation.AcquisitionError,
                "path identity changed|identity or metadata changed",
            ):
                pinned_result.verify()
        finally:
            pinned_result.close()

        payload_path = Path(result["admission"]["path"])
        pinned_payload = implementation.PinnedRegularFile.open(
            payload_path,
            root=output_root,
            maximum=order["limits"]["max_job_bytes"],
            capture=False,
            label="test payload",
        )
        try:
            with payload_path.open("r+b") as handle:
                original = handle.read(1)
                handle.seek(0)
                handle.write(bytes([original[0] ^ 0xFF]))
                handle.flush()
                os.fsync(handle.fileno())
            with self.assertRaisesRegex(
                implementation.AcquisitionError, "identity or metadata changed|bytes changed"
            ):
                pinned_payload.verify()
        finally:
            pinned_payload.close()

    def test_local_dry_run_is_read_only_and_resolves_exact_target(self) -> None:
        output_root = self.case / "cache"
        completed = self.execute(local_order(self.fixture, output_root), "--dry-run")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "local-plan.contract.json")
        self.assertEqual(result["status"], "planned")
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["planned_admission"]["sha256"], digest(self.fixture))
        self.assertTrue(result["planned_admission"]["path"].endswith("/payload"))
        self.assertFalse(output_root.exists())

    def test_capacity_reservation_fails_before_staging(self) -> None:
        output_root = self.case / "cache"
        too_small = limits(max_job_bytes=max(1, self.fixture.stat().st_size - 1))
        completed = self.execute(local_order(self.fixture, output_root, job_limits=too_small))
        self.assertEqual(completed.returncode, 2)
        failure = json.loads(completed.stderr)
        self.assertIn("exceeds limits.max_job_bytes", failure["error"]["message"])
        self.assertFalse(output_root.exists())

    def test_rejects_temporary_output_and_reddit_discussion_urls(self) -> None:
        temporary = local_order(self.fixture, Path("/tmp/himr-acquisition-test"))
        completed = self.execute(temporary)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("may not be under /tmp", json.loads(completed.stderr)["error"]["message"])

        url = "https://www.reddit.com/r/HIMRFAM/comments/example/post/"
        rejected = http_order(url, self.case / "cache")
        completed = self.execute(rejected)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("not media inputs", json.loads(completed.stderr)["error"]["message"])

    def test_rejects_credentials_and_credential_like_query_parameters(self) -> None:
        for url in (
            "https://user:password@example.test/video.mp4",
            "https://example.test/video.mp4?token=secret",
        ):
            with self.subTest(url=url):
                completed = self.execute(http_order(url, self.case / "cache"))
                self.assertEqual(completed.returncode, 2)
                message = json.loads(completed.stderr)["error"]["message"]
                self.assertIn("credential", message)

    def test_invalid_source_publication_timestamp_is_rejected(self) -> None:
        order = local_order(self.fixture, self.case / "cache")
        order["source"]["published_at"] = "yesterday"
        completed = self.execute(order, "--dry-run")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("RFC 3339", json.loads(completed.stderr)["error"]["message"])
        self.assertFalse((self.case / "cache").exists())

    def test_direct_http_resumes_a_guarded_partial_download(self) -> None:
        server, thread = self.serve_fixture(truncate_once=True)
        try:
            url = f"http://127.0.0.1:{server.server_port}/media.mp4"
            order = http_order(url, self.case / "cache")
            first = self.execute(order)
            self.assertEqual(first.returncode, 2)
            failure = json.loads(first.stderr)
            self.assertIn("incomplete HTTP body", failure["error"]["message"])
            partials = list((self.case / "cache" / ".staging").rglob("payload.part"))
            self.assertEqual(len(partials), 1)
            self.assertGreater(partials[0].stat().st_size, 0)
            self.assertLess(partials[0].stat().st_size, self.fixture.stat().st_size)

            second = self.execute(order)
            self.assertEqual(second.returncode, 0, second.stderr)
            result = json.loads(second.stdout)
            self.assert_result_contract(result, "http-result.contract.json")
            self.assertGreater(result["source_observation"]["http_source"]["resumed_from_bytes"], 0)
            self.assertEqual(Path(result["admission"]["path"]).read_bytes(), self.fixture.read_bytes())
            self.assertIsNone(FixtureHTTPHandler.ranges[0])
            self.assertTrue(FixtureHTTPHandler.ranges[-1].startswith("bytes="))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_yt_dlp_adapter_uses_caller_executable_without_auth(self) -> None:
        fake = self.case / "fake-yt-dlp"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import json, shutil, sys\n"
            "from pathlib import Path\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-2026.08.26')\n"
            "    raise SystemExit(0)\n"
            "target = sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4')\n"
            f"shutil.copyfile({str(self.fixture)!r}, target)\n"
            "print(json.dumps({'id': 'fixture-id', 'title': 'Fixture', "
            "'webpage_url': sys.argv[-1], 'extractor': 'fake', 'ext': 'mp4', "
            "'width': 'not-an-integer', 'duration': -1, 'timestamp': True, "
            "'fps': 15.0, 'filesize': 42.0, 'channel': 'C' * 16385, "
            "'filesize_approx': 10**30, 'live_status': {'bad': 'type'}}))\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=fixture-id"
        order = {
            "schema_version": 1,
            "job_id": "yt-fixture-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "fixture-id", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": digest(self.fixture),
                "expected_byte_count": self.fixture.stat().st_size,
            },
            "output": {"root": str((self.case / "cache").resolve())},
            "limits": limits(),
        }
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "yt-dlp-result.contract.json")
        self.assertEqual(result["selected_remote_metadata"]["id"], "fixture-id")
        self.assertIsNone(result["selected_remote_metadata"]["width"])
        self.assertIsNone(result["selected_remote_metadata"]["duration"])
        self.assertIsNone(result["selected_remote_metadata"]["timestamp"])
        self.assertIsNone(result["selected_remote_metadata"]["live_status"])
        self.assertEqual(result["selected_remote_metadata"]["fps"], 15.0)
        self.assertEqual(result["selected_remote_metadata"]["filesize"], 42)
        self.assertIsNone(result["selected_remote_metadata"]["channel"])
        self.assertIsNone(result["selected_remote_metadata"]["filesize_approx"])
        self.assertTrue(result["source_observation"]["yt_dlp"]["unchanged"])
        command = result["commands"][0]
        self.assertIn("--ignore-config", command)
        self.assertIn("--no-playlist", command)
        self.assertIn("--no-progress", command)
        self.assertIn("--no-simulate", command)
        self.assertIn("--print", command)
        self.assertNotIn("--print-json", command)
        self.assertNotIn("--cookies", command)
        self.assertNotIn("--username", command)
        self.assertNotIn("--password", command)
        self.assertEqual(Path(result["admission"]["path"]).read_bytes(), self.fixture.read_bytes())

    def test_exact_numeric_ytdlp_format_pair_is_closed_and_safe(self) -> None:
        module = load_acquire_module()
        fake = self.case / "fake-yt-dlp"
        fake.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=exactPair01"
        order = {
            "schema_version": 1,
            "job_id": "yt-exact-pair-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "exactPair01", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "396+140",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str((self.case / "cache").resolve())},
            "limits": limits(),
        }
        validated = module.validate_work_order(order)
        self.assertEqual(validated["adapter_config"]["format_selector"], "396+140")

        for unsafe in (
            "396+140/b",
            "396+bestaudio",
            "bestvideo+140",
            "0396+140",
            "396+0140",
            "396",
            "396+140,18",
        ):
            changed = json.loads(json.dumps(order))
            changed["adapter_config"]["format_selector"] = unsafe
            with self.subTest(selector=unsafe), self.assertRaisesRegex(
                module.AcquisitionError, "fixed safe selector policy"
            ):
                module.validate_work_order(changed)

    def test_youtube_uses_bounded_metadata_template_for_fragmented_media(self) -> None:
        fake = self.case / "fragmented-yt-dlp"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import json, shutil, sys\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-fragmented-2026.08.27')\n"
            "    raise SystemExit(0)\n"
            "target = sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4')\n"
            f"shutil.copyfile({str(self.fixture)!r}, target)\n"
            "if '--print-json' in sys.argv:\n"
            "    sys.stdout.write('X' * (9 * 1024 * 1024))\n"
            "    raise SystemExit(0)\n"
            "print(json.dumps({'id': 'longmeta001', 'title': 'Long fixture', "
            "'webpage_url': sys.argv[-1], 'original_url': sys.argv[-1], "
            "'ext': 'mp4', 'duration': 13300}))\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=longmeta001"
        order = {
            "schema_version": 1,
            "job_id": "yt-fragmented-metadata-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "longmeta001", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": digest(self.fixture),
                "expected_byte_count": self.fixture.stat().st_size,
            },
            "output": {"root": str((self.case / "cache").resolve())},
            "limits": limits(),
        }
        planned = self.execute(order, "--dry-run")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        planned_result = json.loads(planned.stdout)
        self.assert_result_contract(
            planned_result, "yt-fragmented-plan.contract.json"
        )
        planned_command = planned_result["commands"][0]
        self.assertFalse((self.case / "cache").exists())
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(
            result, "yt-fragmented-result.contract.json"
        )
        command = result["commands"][0]
        self.assertEqual(command, planned_command)
        self.assertNotIn("--print-json", command)
        self.assertIn("--no-progress", command)
        self.assertIn("--no-simulate", command)
        self.assertEqual(command[command.index("--print") + 1].split(":", 1)[0], "after_move")
        self.assertEqual(result["selected_remote_metadata"]["id"], "longmeta001")

    def test_youtube_url_requires_youtube_source_routing(self) -> None:
        fake = self.case / "mislabeled-yt-dlp"
        marker = self.case / "mislabeled-was-invoked"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            f"Path({str(marker)!r}).write_text('invoked')\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=routeCheck1"
        order = {
            "schema_version": 1,
            "job_id": "yt-route-check-001",
            "adapter": "yt_dlp",
            "source": source("other", "routeCheck1", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str((self.case / "cache").resolve())},
            "limits": limits(),
        }
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 2)
        self.assertIn(
            "source.platform=youtube",
            json.loads(completed.stderr)["error"]["message"],
        )
        self.assertFalse(marker.exists())
        self.assertFalse((self.case / "cache").exists())

    def test_youtube_metadata_id_mismatch_fails_before_admission(self) -> None:
        fake = self.case / "redirected-yt-dlp"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import json, shutil, sys\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-redirected-2026.08.27')\n"
            "    raise SystemExit(0)\n"
            "target = sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4')\n"
            f"shutil.copyfile({str(self.fixture)!r}, target)\n"
            "print(json.dumps({'id': 'different01', 'ext': 'mp4'}))\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=requested01"
        output_root = self.case / "cache"
        order = {
            "schema_version": 1,
            "job_id": "yt-identity-mismatch-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "requested01", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(output_root.resolve())},
            "limits": limits(),
        }
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 2)
        self.assertIn(
            "metadata ID does not match",
            json.loads(completed.stderr)["error"]["message"],
        )
        media_root = output_root / "media"
        self.assertFalse(media_root.exists() and any(media_root.rglob("payload")))
        staging_root = output_root / ".staging"
        self.assertFalse(staging_root.exists() and any(staging_root.rglob("*")))

    def test_youtube_reuse_reapplies_selected_id_validation(self) -> None:
        fake = self.case / "reusable-yt-dlp"
        marker = self.case / "invocations"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import json, shutil, sys\n"
            "from pathlib import Path\n"
            f"marker = Path({str(marker)!r})\n"
            "with marker.open('a', encoding='utf-8') as handle:\n"
            "    handle.write('invoke\\n')\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-reuse-2026.08.27')\n"
            "    raise SystemExit(0)\n"
            "target = sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4')\n"
            f"shutil.copyfile({str(self.fixture)!r}, target)\n"
            "print(json.dumps({'id': 'reuseCheck1', 'ext': 'mp4'}))\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=reuseCheck1"
        order = {
            "schema_version": 1,
            "job_id": "yt-reuse-identity-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "reuseCheck1", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": digest(self.fixture),
                "expected_byte_count": self.fixture.stat().st_size,
            },
            "output": {"root": str((self.case / "cache").resolve())},
            "limits": limits(),
        }
        first = self.execute(order)
        self.assertEqual(first.returncode, 0, first.stderr)
        first_result = json.loads(first.stdout)
        durable_result = Path(first_result["result_path"])

        verified_reuse = self.execute(order)
        self.assertEqual(verified_reuse.returncode, 0, verified_reuse.stderr)
        verified_reuse_result = json.loads(verified_reuse.stdout)
        self.assertIn("reuse_verified_at", verified_reuse_result)
        self.assertEqual(
            marker.read_text(encoding="utf-8").splitlines(), ["invoke"] * 3
        )

        tampered = json.loads(durable_result.read_text(encoding="utf-8"))
        tampered["selected_remote_metadata"]["id"] = "different01"
        durable_result.write_text(json.dumps(tampered), encoding="utf-8")

        second = self.execute(order)
        self.assertEqual(second.returncode, 0, second.stderr)
        second_result = json.loads(second.stdout)
        self.assert_result_contract(
            second_result, "yt-reacquired-result.contract.json"
        )
        self.assertEqual(
            second_result["selected_remote_metadata"]["id"], "reuseCheck1"
        )
        self.assertEqual(marker.read_text(encoding="utf-8").splitlines(), ["invoke"] * 6)

    def test_unparseable_youtube_metadata_is_bounded_and_cleaned(self) -> None:
        fake = self.case / "huge-integer-yt-dlp"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import shutil, sys\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-huge-integer-2026.08.27')\n"
            "    raise SystemExit(0)\n"
            "target = sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4')\n"
            f"shutil.copyfile({str(self.fixture)!r}, target)\n"
            "print('{\"id\":' + ('9' * 5000) + '}')\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=hugeInteger"
        output_root = self.case / "cache"
        order = {
            "schema_version": 1,
            "job_id": "yt-huge-integer-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "hugeInteger", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(output_root.resolve())},
            "limits": limits(),
        }
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 2)
        self.assertNotIn("Traceback", completed.stderr)
        self.assertIn(
            "exactly one valid metadata object",
            json.loads(completed.stderr)["error"]["message"],
        )
        staging_root = output_root / ".staging"
        self.assertFalse(staging_root.exists() and any(staging_root.rglob("*")))
        media_root = output_root / "media"
        self.assertFalse(media_root.exists() and any(media_root.rglob("payload")))

    def test_yt_dlp_optional_executable_pin_fails_before_invocation(self) -> None:
        fake = self.case / "wrong-pin-yt-dlp"
        marker = self.case / "wrong-pin-was-invoked"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            f"Path({str(marker)!r}).write_text('invoked')\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=wrongpin001"
        order = {
            "schema_version": 1,
            "job_id": "yt-wrong-pin-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "wrongpin001", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": "0" * 64,
                "format_selector": "b[height<=720]/b",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str((self.case / "wrong-pin-cache").resolve())},
            "limits": limits(),
        }
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 2)
        self.assertIn(
            "executable SHA-256 does not match",
            json.loads(completed.stderr)["error"]["message"],
        )
        self.assertFalse(marker.exists())

    def test_existing_v1_yt_dlp_order_without_pin_keeps_its_canonical_shape(self) -> None:
        fake = self.case / "legacy-v1-yt-dlp"
        fake.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=legacyV1001"
        order = {
            "schema_version": 1,
            "job_id": "yt-legacy-v1-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "legacyV1001", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str((self.case / "legacy-cache").resolve())},
            "limits": limits(),
        }
        order_path = self.case / "legacy-order.json"
        order_path.write_text(json.dumps(order), encoding="utf-8")
        completed = run(
            [
                "python3",
                str(PROGRAM),
                "validate",
                "--work-order",
                str(order_path.resolve()),
            ],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        normalized = json.loads(completed.stdout)
        self.assertNotIn(
            "expected_executable_sha256", normalized["adapter_config"]
        )
        self.assertEqual(normalized, order)

    def test_yt_dlp_executable_mutation_during_run_fails_before_admission(self) -> None:
        fake = self.case / "mutating-yt-dlp"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import json, shutil, sys\n"
            "from pathlib import Path\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-mutating-2026.08.26')\n"
            "    raise SystemExit(0)\n"
            "target = sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4')\n"
            f"shutil.copyfile({str(self.fixture)!r}, target)\n"
            "print(json.dumps({'id': 'mutateX1234', 'ext': 'mp4'}))\n"
            "with Path(__file__).open('ab') as handle:\n"
            "    handle.write(b'\\n# changed during acquisition\\n')\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        executable_pin = digest(fake)
        url = "https://www.youtube.com/watch?v=mutateX1234"
        output_root = self.case / "mutating-tool-cache"
        order = {
            "schema_version": 1,
            "job_id": "yt-mutating-tool-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "mutateX1234", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": executable_pin,
                "format_selector": "b[height<=720]/b",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(output_root.resolve())},
            "limits": limits(),
        }
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 2)
        self.assertIn(
            "executable changed during acquisition",
            json.loads(completed.stderr)["error"]["message"],
        )
        self.assertNotEqual(digest(fake), executable_pin)
        media_root = output_root / "media"
        self.assertFalse(media_root.exists() and any(media_root.rglob("payload")))
        staging_root = output_root / ".staging"
        self.assertFalse(staging_root.exists() and any(staging_root.rglob("*")))

    def test_writer_lock_rejects_a_contending_process(self) -> None:
        output_root = self.case / "cache"
        output_root.mkdir()
        lock_path = output_root / ".acquisition-writer.lock"
        with lock_path.open("w+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_handle.write(
                json.dumps(
                    {
                        "schema_version": 1,
                        "state": "locked",
                        "pid": os.getpid(),
                        "job_id": "test-lock-holder",
                    }
                )
            )
            lock_handle.flush()
            completed = self.execute(local_order(self.fixture, output_root))
            self.assertEqual(completed.returncode, 2)
            message = json.loads(completed.stderr)["error"]["message"]
            self.assertIn("locked by another acquisition writer", message)
            self.assertIn("test-lock-holder", message)
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

        recovered = self.execute(local_order(self.fixture, output_root))
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        released_metadata = json.loads(lock_path.read_text(encoding="utf-8"))
        self.assertEqual(released_metadata["state"], "released")
        self.assertTrue(released_metadata["recovered_stale_state"])

    def test_yt_dlp_is_terminated_and_cleaned_when_staging_exceeds_cap(self) -> None:
        fake = self.case / "oversize-yt-dlp"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import os, sys, time\n"
            "if '--version' in sys.argv:\n"
            "    print('fake-oversize-2026.08.26')\n"
            "    raise SystemExit(0)\n"
            "target = sys.argv[sys.argv.index('--output') + 1].replace('%(ext)s', 'mp4')\n"
            "with open(target, 'wb') as handle:\n"
            "    for _ in range(256):\n"
            "        handle.write(b'X' * 65536)\n"
            "        handle.flush()\n"
            "        time.sleep(0.002)\n"
            "time.sleep(10)\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        url = "https://www.youtube.com/watch?v=oversize-fixture"
        output_root = self.case / "cache"
        order = {
            "schema_version": 1,
            "job_id": "yt-oversize-001",
            "adapter": "yt_dlp",
            "source": source("youtube", "oversize-fixture", url),
            "adapter_config": {
                "url": url,
                "executable": str(fake.resolve()),
                "expected_executable_sha256": digest(fake),
                "format_selector": "b[height<=720]/b",
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(output_root.resolve())},
            "limits": {
                "max_job_bytes": 128 * 1024,
                "global_cache_cap_bytes": 100 * 1024 * 1024,
                "free_space_floor_bytes": 0,
            },
        }
        started = time.monotonic()
        completed = self.execute(order)
        elapsed = time.monotonic() - started
        self.assertEqual(completed.returncode, 2)
        self.assertLess(elapsed, 5, "oversize yt-dlp was not terminated promptly")
        failure = json.loads(completed.stderr)
        self.assertIn(
            "staging exceeded limits.max_job_bytes", failure["error"]["message"]
        )
        staging_root = output_root / ".staging"
        self.assertFalse(staging_root.exists() and any(staging_root.rglob("*")))
        media_root = output_root / "media"
        self.assertFalse(media_root.exists() and any(media_root.rglob("payload")))


if __name__ == "__main__":
    unittest.main()
