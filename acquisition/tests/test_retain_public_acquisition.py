from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from acquisition import acquire
from acquisition import retain_public_acquisition as retention


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TEST_ROOT = REPOSITORY_ROOT / "acquisition" / ".test-work"


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def fingerprint(path: Path) -> tuple[int, ...]:
    info = path.stat()
    return (
        info.st_dev,
        info.st_ino,
        stat.S_IMODE(info.st_mode),
        info.st_nlink,
        info.st_uid,
        info.st_gid,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


class PublicAcquisitionRetentionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            TEST_ROOT.rmdir()
        except OSError:
            pass

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="public-acquisition-retention-", dir=TEST_ROOT
        )
        self.root = Path(self.temporary.name).resolve()
        self.acquisition_root = self.root / "acquired"
        self.staging_root = self.root / "staging"
        self.receipt_root = self.root / "receipts"
        self.control_root = self.root / "controls"
        for directory in (
            self.acquisition_root,
            self.staging_root,
            self.receipt_root,
            self.control_root,
        ):
            directory.mkdir(mode=0o700)
        self.payload_body = b"one exact public acquisition payload\0" + bytes(range(64))
        self.payload_sha256 = digest(self.payload_body)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def request(
        self,
        *,
        work_order: Path | None = None,
        work_order_sha256: str = "a" * 64,
        result_sha256: str = "b" * 64,
    ) -> retention.RetentionRequest:
        return retention.RetentionRequest(
            work_order=work_order or self.control_root / "work-order.json",
            acquisition_root=self.acquisition_root,
            staging_root=self.staging_root,
            receipt_root=self.receipt_root,
            expected_work_order_sha256=work_order_sha256,
            expected_result_sha256=result_sha256,
            expected_sha256=self.payload_sha256,
            expected_byte_count=len(self.payload_body),
            free_space_floor_bytes=4096,
        )

    def open_payload(self, path: Path) -> acquire.PinnedRegularFile:
        return acquire.PinnedRegularFile.open(
            path,
            root=self.acquisition_root,
            maximum=len(self.payload_body),
            capture=False,
            label="retention test payload",
        )

    def test_staging_is_content_addressed_sealed_and_replay_safe(self) -> None:
        source_path = self.acquisition_root / "source" / "payload"
        source_path.parent.mkdir(mode=0o700)
        source_path.write_bytes(self.payload_body)
        source_path.chmod(0o644)
        before = fingerprint(source_path)
        pinned = self.open_payload(source_path)
        try:
            staged, admission = retention.seal_staging_payload(
                pinned,
                staging_root=self.staging_root,
                expected_sha256=self.payload_sha256,
                expected_byte_count=len(self.payload_body),
            )
            first_inode = staged.stat().st_ino
            replayed, replay_admission = retention.seal_staging_payload(
                pinned,
                staging_root=self.staging_root,
                expected_sha256=self.payload_sha256,
                expected_byte_count=len(self.payload_body),
            )
        finally:
            pinned.close()

        self.assertEqual("copied", admission)
        self.assertEqual("recovered_existing", replay_admission)
        self.assertEqual(staged, replayed)
        self.assertEqual(
            self.staging_root
            / "media"
            / "sha256"
            / self.payload_sha256[:2]
            / self.payload_sha256
            / "payload",
            staged,
        )
        self.assertEqual(first_inode, staged.stat().st_ino)
        self.assertEqual(0o400, stat.S_IMODE(staged.stat().st_mode))
        self.assertEqual(1, staged.stat().st_nlink)
        self.assertEqual(self.payload_body, staged.read_bytes())
        self.assertEqual(before, fingerprint(source_path))
        self.assertEqual(self.payload_body, source_path.read_bytes())

    def test_run_uses_only_fixed_cold_root_and_preserves_hot_source(self) -> None:
        source_path = self.acquisition_root / "source" / "payload"
        source_path.parent.mkdir(mode=0o700)
        source_path.write_bytes(self.payload_body)
        source_path.chmod(0o644)
        before = fingerprint(source_path)
        pinned = self.open_payload(source_path)
        fake_result_path = self.acquisition_root / "jobs" / "result.json"

        class FakeRetained:
            def __init__(self) -> None:
                self.payload = pinned
                self.result_path = fake_result_path
                self.verifications = 0
                self.closed = False

            def verify(self) -> None:
                self.payload.verify()
                self.verifications += 1

            def close(self) -> None:
                self.payload.close()
                self.closed = True

        fake = FakeRetained()
        observed_requests: list[object] = []

        def load_payload(request: retention.RetentionRequest) -> FakeRetained:
            self.assertEqual(self.payload_sha256, request.expected_sha256)
            return fake

        def transfer(request: object) -> dict[str, object]:
            observed_requests.append(request)
            self.assertEqual(retention.FIXED_DESTINATION_ROOT, request.destination_root)
            staged = request.source_root / request.source_relative_path
            self.assertEqual(0o400, stat.S_IMODE(staged.stat().st_mode))
            self.assertEqual(self.payload_body, staged.read_bytes())
            self.assertEqual(before, fingerprint(source_path))
            return {"kind": "cold_storage_transfer_receipt", "status": "completed"}

        result = retention.run_retention(
            self.request(), payload_loader=load_payload, transfer_runner=transfer
        )

        self.assertEqual(1, len(observed_requests))
        self.assertGreaterEqual(fake.verifications, 2)
        self.assertTrue(fake.closed)
        self.assertEqual("completed", result["status"])
        self.assertEqual(
            {"kind": "cold_storage_transfer_receipt", "status": "completed"},
            result["cold_transfer_receipt"],
        )
        self.assertTrue(result["source"]["unchanged"])
        self.assertFalse(result["policy"]["source_deleted"])
        self.assertFalse(result["policy"]["source_mutated"])
        self.assertEqual(before, fingerprint(source_path))

    def _public_work_order(self) -> dict[str, object]:
        url = "https://archive.org/download/example/public-fixture.mp4"
        return {
            "schema_version": 1,
            "job_id": "public-retention-fixture-001",
            "adapter": "direct_http",
            "source": {
                "platform": "internet_archive",
                "source_kind": "archive_media_file",
                "native_id": "example/public-fixture.mp4",
                "canonical_url": url,
                "title": "Public retention fixture",
                "published_at": None,
                "access_state": "public",
            },
            "adapter_config": {
                "url": url,
                "resume": True,
                "timeout_seconds": 60,
                "expected_sha256": self.payload_sha256,
                "expected_byte_count": len(self.payload_body),
            },
            "output": {"root": str(self.acquisition_root)},
            "limits": {
                "max_job_bytes": 1024,
                "global_cache_cap_bytes": 4096,
                "free_space_floor_bytes": 0,
            },
        }

    def test_loader_requires_reviewed_controls_and_public_typed_replay(self) -> None:
        work_order_path = self.control_root / "work-order.json"
        work_order_body = (
            json.dumps(self._public_work_order(), sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode()
        work_order_path.write_bytes(work_order_body)
        work_order_path.chmod(0o644)
        normalized = acquire.validate_work_order(
            acquire.strict_json_object(work_order_body, "test work order")
        )
        work_order_identity = acquire.sha256_bytes(acquire.canonical_bytes(normalized))
        result_path = (
            self.acquisition_root
            / "jobs"
            / normalized["job_id"]
            / work_order_identity
            / "result.json"
        )
        result_path.parent.mkdir(parents=True, mode=0o700)
        payload_path = (
            self.acquisition_root
            / "media"
            / "sha256"
            / self.payload_sha256[:2]
            / self.payload_sha256
            / "payload"
        )
        payload_path.parent.mkdir(parents=True, mode=0o700)
        payload_path.write_bytes(self.payload_body)
        payload_path.chmod(0o644)
        result = {
            "admission": {
                "path": str(payload_path),
                "sha256": self.payload_sha256,
                "byte_count": len(self.payload_body),
            }
        }
        result_body = (json.dumps(result, sort_keys=True) + "\n").encode()
        result_path.write_bytes(result_body)
        result_path.chmod(0o644)
        expected_result_path = result_path
        request = retention._validate_request(
            self.request(
                work_order=work_order_path,
                work_order_sha256=digest(work_order_body),
                result_sha256=digest(result_body),
            )
        )

        def replay_result(
            replayed_result: dict[str, object],
            output_root: Path,
            _work_order: dict[str, object],
            *,
            result_path: Path,
            pins: acquire.PinnedFiles,
        ) -> bool:
            self.assertEqual(result, replayed_result)
            self.assertEqual(self.acquisition_root, output_root)
            self.assertEqual(expected_result_path, result_path)
            pins.open(
                payload_path,
                root=self.acquisition_root,
                maximum=1024,
                capture=False,
                label="replayed test payload",
            )
            return True

        with mock.patch.object(
            retention.acquire,
            "validate_reusable_result",
            side_effect=replay_result,
        ) as replay:
            retained = retention.load_completed_public_payload(request)
            try:
                retained.verify()
                self.assertEqual(self.payload_sha256, retained.payload.digest)
                self.assertEqual(digest(result_body), retained.result_file.digest)
                self.assertEqual(1, replay.call_count)
            finally:
                retained.close()

        rejected = retention.RetentionRequest(
            **{
                **request.__dict__,
                "expected_work_order_sha256": "0" * 64,
            }
        )
        with self.assertRaisesRegex(retention.PublicRetentionError, "reviewed digest"):
            retention.load_completed_public_payload(rejected)

    def test_loader_rejects_local_or_nonpublic_acquisition_before_result_access(self) -> None:
        local_source = self.root / "incoming.bin"
        local_source.write_bytes(self.payload_body)
        work_order = {
            "schema_version": 1,
            "job_id": "local-retention-rejected-001",
            "adapter": "local_file",
            "source": {
                "platform": "local",
                "source_kind": "video",
                "native_id": "local-retention-rejected-001",
                "canonical_url": None,
                "title": "Local fixture",
                "published_at": None,
                "access_state": "unknown",
            },
            "adapter_config": {
                "path": str(local_source),
                "expected_sha256": self.payload_sha256,
                "expected_byte_count": len(self.payload_body),
            },
            "output": {"root": str(self.acquisition_root)},
            "limits": {
                "max_job_bytes": 1024,
                "global_cache_cap_bytes": 4096,
                "free_space_floor_bytes": 0,
            },
        }
        body = (json.dumps(work_order, sort_keys=True) + "\n").encode()
        path = self.control_root / "local-work-order.json"
        path.write_bytes(body)
        path.chmod(0o400)
        request = retention._validate_request(
            self.request(work_order=path, work_order_sha256=digest(body))
        )
        with self.assertRaisesRegex(
            retention.PublicRetentionError, "only completed credential-free public"
        ):
            retention.load_completed_public_payload(request)

    def test_request_rejects_implicit_or_cold_or_overlapping_paths(self) -> None:
        cases = (
            self.request(work_order=Path("relative.json")),
            retention.RetentionRequest(
                **{
                    **self.request().__dict__,
                    "staging_root": self.acquisition_root / "stage",
                }
            ),
            retention.RetentionRequest(
                **{
                    **self.request().__dict__,
                    "receipt_root": Path("/mnt/archive/HIMR/receipts"),
                }
            ),
        )
        for index, request in enumerate(cases):
            with self.subTest(index=index):
                with self.assertRaises(retention.PublicRetentionError):
                    retention._validate_request(request)

    def test_isolated_launcher_pins_and_executes_the_full_source_closure(self) -> None:
        launcher = REPOSITORY_ROOT / "acquisition" / "bin" / "retain-public-acquisition"
        source_names = (
            "acquire.py",
            "cold_storage_transfer.py",
            "retain_public_acquisition.py",
        )
        launcher_text = launcher.read_text(encoding="utf-8")
        self.assertTrue(launcher_text.startswith("#!/usr/bin/python3 -IB\n"))
        self.assertNotIn("/bin/sh", launcher_text)
        self.assertIn("compile(body", launcher_text)
        self.assertIn("exec(code", launcher_text)
        for name in source_names:
            source = REPOSITORY_ROOT / "acquisition" / name
            self.assertIn(digest(source.read_bytes()), launcher_text)

        def copy_closure(label: str) -> tuple[Path, Path]:
            fixture_root = self.root / label / "acquisition"
            fixture_bin = fixture_root / "bin"
            fixture_bin.mkdir(parents=True)
            copied_launcher = fixture_bin / launcher.name
            shutil.copyfile(launcher, copied_launcher)
            copied_launcher.chmod(0o500)
            for source_name in source_names:
                copied = fixture_root / source_name
                shutil.copyfile(REPOSITORY_ROOT / "acquisition" / source_name, copied)
                copied.chmod(0o600)
            return copied_launcher, fixture_root

        copied_launcher, _fixture_root = copy_closure("valid-launcher")
        hostile = self.root / "hostile-pythonpath"
        hostile.mkdir()
        sentinel = self.root / "sitecustomize-ran"
        (hostile / "sitecustomize.py").write_text(
            f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('bad')\n",
            encoding="utf-8",
        )
        completed = subprocess.run(
            [str(copied_launcher), "--help"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            cwd=self.root,
            env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(hostile)},
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertFalse(sentinel.exists())

        for index, source_name in enumerate(source_names):
            with self.subTest(source=source_name):
                rejected_launcher, fixture_root = copy_closure(f"mutated-{index}")
                with (fixture_root / source_name).open("ab") as handle:
                    handle.write(b"\n# unpinned mutation\n")
                rejected = subprocess.run(
                    [str(rejected_launcher), "--help"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                    cwd=self.root,
                    env={"PATH": "/usr/bin:/bin"},
                )
                self.assertNotEqual(0, rejected.returncode)
                self.assertIn("differs from launcher pin", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
