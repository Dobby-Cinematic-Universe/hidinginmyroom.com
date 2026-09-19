from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import stat
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from acquisition import cold_storage_transfer as transfer


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
TEST_ROOT = ACQUISITION_ROOT / ".test-work"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
RECEIPT_SCHEMA = (
    ACQUISITION_ROOT / "schemas" / "cold-storage-transfer-receipt.schema.json"
)


def digest_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def stable_file_identity(path: Path) -> tuple[int, ...]:
    observed = path.stat()
    return (
        observed.st_dev,
        observed.st_ino,
        stat.S_IMODE(observed.st_mode),
        observed.st_nlink,
        observed.st_uid,
        observed.st_gid,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


class InjectedCrash(BaseException):
    """Model process death without invoking ordinary exception cleanup."""


class ColdStorageTransferTests(unittest.TestCase):
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
            prefix="cold-storage-transfer-", dir=TEST_ROOT
        )
        self.root = Path(self.temporary.name).resolve()
        self.source_root = self.root / "main"
        self.destination_root = self.root / "cold"
        self.receipt_root = self.root / "receipts"
        for directory in (
            self.source_root,
            self.destination_root,
            self.receipt_root,
        ):
            directory.mkdir(mode=0o700)

        source_directory = self.source_root / "sealed"
        source_directory.mkdir(mode=0o700)
        self.source_path = source_directory / "fixture.bin"
        self.payload = (
            b"HIMR cold-storage transfer fixture\0"
            + bytes(range(64))
            + b"\nsource must remain unchanged\n"
        )
        self.source_path.write_bytes(self.payload)
        self.source_path.chmod(0o400)
        self.sha256 = digest_bytes(self.payload)
        self.free_space_floor_bytes = 4096
        self.available_bytes = (
            len(self.payload) + self.free_space_floor_bytes + 8192
        )
        self.request = transfer.TransferRequest(
            source_root=self.source_root,
            source_relative_path=Path("sealed/fixture.bin"),
            destination_root=self.destination_root,
            receipt_root=self.receipt_root,
            expected_sha256=self.sha256,
            expected_byte_count=len(self.payload),
            free_space_floor_bytes=self.free_space_floor_bytes,
        )
        self.device_calls: list[Path] = []
        self.statvfs_calls: list[int] = []
        self.rename_calls: list[tuple[str, str]] = []

    def tearDown(self) -> None:
        for child in sorted(
            self.root.rglob("*"), key=lambda value: len(value.parts), reverse=True
        ):
            if child.is_symlink():
                continue
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        self.temporary.cleanup()

    def device_id_provider(self, path: Path, descriptor: int) -> int:
        path = Path(path)
        self.device_calls.append(path)
        self.assertTrue(stat.S_ISDIR(os.fstat(descriptor).st_mode))
        if path == self.destination_root:
            return 202
        if path in {self.source_root, self.receipt_root}:
            return 101
        raise AssertionError(f"unexpected retained root: {path}")

    def statvfs_provider(self, descriptor: int) -> SimpleNamespace:
        self.assertTrue(stat.S_ISDIR(os.fstat(descriptor).st_mode))
        self.statvfs_calls.append(descriptor)
        return SimpleNamespace(f_bavail=self.available_bytes, f_frsize=1)

    def publish_fd_noreplace(
        self,
        source_descriptor: int,
        source_name: str,
        destination_directory_fd: int,
        destination_name: str,
    ) -> None:
        self.rename_calls.append((source_name, destination_name))
        transfer._link_fd_noreplace(
            source_descriptor,
            source_name,
            destination_directory_fd,
            destination_name,
        )

    def run_transfer(
        self,
        request: transfer.TransferRequest | None = None,
        **overrides: object,
    ) -> dict:
        options: dict[str, object] = {
            "device_id_provider": self.device_id_provider,
            "statvfs_provider": self.statvfs_provider,
            "publish_fd_noreplace": self.publish_fd_noreplace,
        }
        options.update(overrides)
        return transfer.run_transfer(request or self.request, **options)

    def destination_path(self) -> Path:
        return (
            self.destination_root
            / "media"
            / "sha256"
            / self.sha256[:2]
            / self.sha256
            / "payload"
        )

    def receipt_path(self, result: dict) -> Path:
        return (
            self.receipt_root
            / "cold-storage-transfers"
            / result["transfer_id"]
            / "receipt.json"
        )

    def assert_no_transfer_writes(self) -> None:
        self.assertEqual([], list(self.destination_root.iterdir()))
        self.assertEqual([], list(self.receipt_root.iterdir()))
        self.assertEqual([], self.rename_calls)

    def assert_no_temporaries(self) -> None:
        leftovers = [
            path
            for root in (self.destination_root, self.receipt_root)
            for path in root.rglob("*")
            if path.name.startswith(".payload.tmp-")
            or path.name.startswith(".receipt.json.tmp-")
        ]
        self.assertEqual([], leftovers)

    def assert_receipt_schema(self, result: dict) -> None:
        result_path = self.root / "receipt.contract.json"
        result_path.write_text(json.dumps(result), encoding="utf-8")
        completed = self.validate_receipt_schema(result_path)
        self.assertEqual(
            0,
            completed.returncode,
            "cold-storage receipt failed its schema:\n"
            f"{completed.stdout}{completed.stderr}",
        )

    def validate_receipt_schema(
        self, receipt_path: Path
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(RECEIPT_SCHEMA),
                str(receipt_path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            cwd=REPOSITORY_ROOT,
        )

    def test_dry_run_is_read_only(self) -> None:
        source_before = stable_file_identity(self.source_path)

        result = self.run_transfer(dry_run=True)

        self.assertEqual("cold_storage_transfer_dry_run", result["kind"])
        self.assertEqual("validated", result["status"])
        self.assertFalse(result["writes_performed"])
        self.assertFalse(result["destination"]["target_present_unverified"])
        self.assertEqual(len(self.payload), result["preflight"]["required_new_bytes"])
        self.assertEqual(101, result["preflight"]["source_root_device"])
        self.assertEqual(101, result["preflight"]["receipt_root_device"])
        self.assertEqual(202, result["preflight"]["destination_root_device"])
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual(self.payload, self.source_path.read_bytes())
        self.assert_no_transfer_writes()
        self.assertEqual(1, len(self.statvfs_calls))

    def test_success_is_schema_valid_and_preserves_source(self) -> None:
        source_before = stable_file_identity(self.source_path)

        result = self.run_transfer()

        destination = self.destination_path()
        receipt = self.receipt_path(result)
        self.assert_receipt_schema(result)
        self.assertEqual("completed", result["status"])
        self.assertEqual("copied", result["destination"]["admission"])
        self.assertEqual(self.sha256, result["verification"]["copy_stream_sha256"])
        self.assertEqual(
            self.sha256, result["verification"]["archive_readback_sha256"]
        )
        self.assertTrue(result["verification"]["destination_directories_fsynced"])
        self.assertFalse(result["policy"]["source_deleted"])
        self.assertFalse(result["policy"]["source_mutated"])
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertEqual(0o400, stat.S_IMODE(destination.stat().st_mode))
        self.assertEqual(1, destination.stat().st_nlink)
        self.assertEqual(0o400, stat.S_IMODE(receipt.stat().st_mode))
        self.assertEqual(1, receipt.stat().st_nlink)
        self.assertEqual(result, json.loads(receipt.read_text(encoding="utf-8")))
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual(self.payload, self.source_path.read_bytes())
        self.assertEqual(
            ["payload", "receipt.json"],
            [destination_name for _, destination_name in self.rename_calls],
        )
        self.assert_no_temporaries()

    def test_exact_replay_is_byte_inode_and_receipt_stable(self) -> None:
        source_before = stable_file_identity(self.source_path)
        first = self.run_transfer()
        destination = self.destination_path()
        receipt = self.receipt_path(first)
        destination_before = stable_file_identity(destination)
        receipt_before = stable_file_identity(receipt)
        destination_body = destination.read_bytes()
        receipt_body = receipt.read_bytes()

        def unexpected_statvfs(_descriptor: int) -> object:
            raise AssertionError("exact replay must not perform a capacity check")

        def unexpected_rename(
            _source_directory_fd: int,
            _source_name: str,
            _destination_directory_fd: int,
            _destination_name: str,
        ) -> None:
            raise AssertionError("exact replay must not publish any file")

        replay = self.run_transfer(
            statvfs_provider=unexpected_statvfs,
            publish_fd_noreplace=unexpected_rename,
        )

        self.assertEqual(first, replay)
        self.assertEqual(destination_before, stable_file_identity(destination))
        self.assertEqual(receipt_before, stable_file_identity(receipt))
        self.assertEqual(destination_body, destination.read_bytes())
        self.assertEqual(receipt_body, receipt.read_bytes())
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_temporaries()

    def test_completed_transfer_dry_run_does_not_read_or_rewrite_cold_payload(
        self,
    ) -> None:
        completed = self.run_transfer()
        destination = self.destination_path()
        receipt = self.receipt_path(completed)
        destination_before = stable_file_identity(destination)
        receipt_before = stable_file_identity(receipt)
        destination_body = destination.read_bytes()
        receipt_body = receipt.read_bytes()
        archive_identity = (destination.stat().st_dev, destination.stat().st_ino)
        rename_count = len(self.rename_calls)
        real_pread = os.pread

        def reject_archive_payload_read(
            descriptor: int, byte_count: int, offset: int
        ) -> bytes:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) == archive_identity:
                raise AssertionError("dry-run must not read the cold payload")
            return real_pread(descriptor, byte_count, offset)

        def unexpected_rename(
            _source_directory_fd: int,
            _source_name: str,
            _destination_directory_fd: int,
            _destination_name: str,
        ) -> None:
            raise AssertionError("dry-run must not rewrite payload or receipt")

        with (
            mock.patch.object(
                transfer,
                "_validate_sealed_file",
                side_effect=AssertionError(
                    "dry-run must not validate the completed cold payload"
                ),
            ),
            mock.patch.object(
                transfer,
                "_read_receipt",
                side_effect=AssertionError(
                    "dry-run must return a plan rather than replay a receipt"
                ),
            ),
            mock.patch.object(
                transfer.os, "pread", side_effect=reject_archive_payload_read
            ),
        ):
            planned = self.run_transfer(
                dry_run=True,
                publish_fd_noreplace=unexpected_rename,
            )

        self.assertEqual("cold_storage_transfer_dry_run", planned["kind"])
        self.assertEqual("validated", planned["status"])
        self.assertFalse(planned["writes_performed"])
        self.assertTrue(planned["destination"]["target_present_unverified"])
        self.assertEqual(0, planned["preflight"]["required_new_bytes"])
        self.assertEqual(rename_count, len(self.rename_calls))
        self.assertEqual(destination_before, stable_file_identity(destination))
        self.assertEqual(receipt_before, stable_file_identity(receipt))
        self.assertEqual(destination_body, destination.read_bytes())
        self.assertEqual(receipt_body, receipt.read_bytes())
        self.assert_no_temporaries()

    def test_rejects_wrong_source_checksum_without_writes(self) -> None:
        source_before = stable_file_identity(self.source_path)
        request = replace(self.request, expected_sha256="0" * 64)

        with self.assertRaisesRegex(transfer.ColdStorageError, "SHA-256"):
            self.run_transfer(request)

        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_transfer_writes()

    def test_rejects_unsealed_source_mode_without_writes(self) -> None:
        self.source_path.chmod(0o600)
        source_before = stable_file_identity(self.source_path)

        with self.assertRaisesRegex(transfer.ColdStorageError, "mode-0400"):
            self.run_transfer()

        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_transfer_writes()

    def test_rejects_hardlinked_source_without_writes(self) -> None:
        os.link(self.source_path, self.source_path.with_name("second-link.bin"))
        source_before = stable_file_identity(self.source_path)

        with self.assertRaisesRegex(transfer.ColdStorageError, "single-link"):
            self.run_transfer()

        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_transfer_writes()

    def test_rejects_symlinked_source_without_writes(self) -> None:
        real_source = self.source_path.with_name("real-fixture.bin")
        self.source_path.rename(real_source)
        self.source_path.symlink_to(real_source.name)

        with self.assertRaisesRegex(transfer.ColdStorageError, "symlink"):
            self.run_transfer()

        self.assertEqual(self.payload, real_source.read_bytes())
        self.assertTrue(self.source_path.is_symlink())
        self.assert_no_transfer_writes()

    def test_managed_directory_chain_detects_canonical_ancestor_replacement(
        self,
    ) -> None:
        destination_parent = self.destination_path().parent
        destination_parent.mkdir(parents=True, mode=0o700)
        current = self.destination_root
        for component in destination_parent.relative_to(
            self.destination_root
        ).parts:
            current /= component
            current.chmod(0o700)

        retained_root = transfer.RetainedRoot.open(
            self.destination_root, "test destination root"
        )
        chain = transfer.RetainedDirectoryChain(retained_root)
        try:
            opened = chain.open_existing(
                ("media", "sha256", self.sha256[:2], self.sha256)
            )
            self.assertIsNotNone(opened)
            canonical = self.destination_root / "media"
            canonical_before = canonical.stat().st_ino
            displaced = self.destination_root / "media-displaced"
            canonical.rename(displaced)
            canonical.mkdir(mode=0o700)
            self.assertNotEqual(canonical_before, canonical.stat().st_ino)

            with self.assertRaisesRegex(
                transfer.ColdStorageError, "managed destination identity changed"
            ):
                chain.verify()

            self.assertTrue(canonical.is_dir())
            self.assertTrue(displaced.is_dir())
        finally:
            chain.close()
            retained_root.close()

    def test_rejects_same_device_before_capacity_or_writes(self) -> None:
        source_before = stable_file_identity(self.source_path)

        def same_device(_path: Path, _descriptor: int) -> int:
            return 101

        def unexpected_statvfs(_descriptor: int) -> object:
            raise AssertionError("same-device refusal must precede capacity")

        with self.assertRaisesRegex(transfer.ColdStorageError, "distinct"):
            self.run_transfer(
                device_id_provider=same_device,
                statvfs_provider=unexpected_statvfs,
            )

        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_transfer_writes()

    def test_destination_root_flock_refuses_before_capacity_or_writes(self) -> None:
        source_before = stable_file_identity(self.source_path)
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
        )
        lock_descriptor = os.open(self.destination_root, flags)

        def unexpected_statvfs(_descriptor: int) -> object:
            raise AssertionError("destination lock refusal must precede capacity")

        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(
                transfer.ColdStorageError, "destination-root lock"
            ):
                self.run_transfer(statvfs_provider=unexpected_statvfs)
        finally:
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
            os.close(lock_descriptor)

        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_transfer_writes()

    def test_rejects_insufficient_capacity_without_writes(self) -> None:
        source_before = stable_file_identity(self.source_path)
        available = len(self.payload) + self.free_space_floor_bytes - 1

        def insufficient(_descriptor: int) -> SimpleNamespace:
            return SimpleNamespace(f_bavail=available, f_frsize=1)

        with self.assertRaisesRegex(transfer.ColdStorageError, "free-space floor"):
            self.run_transfer(statvfs_provider=insufficient)

        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_transfer_writes()

    def test_rejects_wrong_existing_target_without_overwrite(self) -> None:
        destination = self.destination_path()
        destination.parent.mkdir(parents=True, mode=0o700)
        current = self.destination_root
        for component in destination.parent.relative_to(self.destination_root).parts:
            current /= component
            current.chmod(0o700)
        wrong_body = bytes([self.payload[0] ^ 0xFF]) + self.payload[1:]
        destination.write_bytes(wrong_body)
        destination.chmod(0o400)
        destination_before = stable_file_identity(destination)
        source_before = stable_file_identity(self.source_path)

        with self.assertRaisesRegex(transfer.ColdStorageError, "content address"):
            self.run_transfer()

        self.assertEqual(wrong_body, destination.read_bytes())
        self.assertEqual(destination_before, stable_file_identity(destination))
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual([], list(self.receipt_root.iterdir()))
        self.assertEqual([], self.rename_calls)
        self.assert_no_temporaries()

    def test_replay_rejects_schema_invalid_receipt_with_recomputed_identity(
        self,
    ) -> None:
        completed = self.run_transfer()
        destination = self.destination_path()
        receipt = self.receipt_path(completed)
        destination_before = stable_file_identity(destination)

        receipt.chmod(0o600)
        tampered = json.loads(receipt.read_text(encoding="utf-8"))
        tampered["policy"]["publication_authority"] = "forged_authority"
        tampered["identity_sha256"] = transfer._receipt_identity(tampered)
        tampered["receipt_id"] = (
            f"coldreceipt_{tampered['identity_sha256'][:32]}"
        )
        receipt.write_text(transfer.pretty_json(tampered), encoding="utf-8")
        receipt.chmod(0o400)
        receipt_before = stable_file_identity(receipt)
        receipt_body = receipt.read_bytes()
        self.assertEqual(
            tampered["identity_sha256"], transfer._receipt_identity(tampered)
        )
        self.assertNotEqual(0, self.validate_receipt_schema(receipt).returncode)

        def unexpected_statvfs(_descriptor: int) -> object:
            raise AssertionError("invalid receipt must fail before capacity")

        def unexpected_rename(
            _source_directory_fd: int,
            _source_name: str,
            _destination_directory_fd: int,
            _destination_name: str,
        ) -> None:
            raise AssertionError("invalid receipt must never be rewritten")

        with self.assertRaisesRegex(
            transfer.ColdStorageError, "invalid policy boundary"
        ):
            self.run_transfer(
                statvfs_provider=unexpected_statvfs,
                publish_fd_noreplace=unexpected_rename,
            )

        self.assertEqual(destination_before, stable_file_identity(destination))
        self.assertEqual(receipt_before, stable_file_identity(receipt))
        self.assertEqual(receipt_body, receipt.read_bytes())
        self.assert_no_temporaries()

    def test_replay_rejects_boolean_in_integer_receipt_fields(self) -> None:
        completed = self.run_transfer()
        destination = self.destination_path()
        receipt = self.receipt_path(completed)
        valid_receipt = json.loads(receipt.read_text(encoding="utf-8"))
        destination_before = stable_file_identity(destination)
        source_before = stable_file_identity(self.source_path)
        rename_count = len(self.rename_calls)

        def unexpected_statvfs(_descriptor: int) -> object:
            raise AssertionError("invalid receipt must fail before capacity")

        def unexpected_rename(
            _source_directory_fd: int,
            _source_name: str,
            _destination_directory_fd: int,
            _destination_name: str,
        ) -> None:
            raise AssertionError("invalid receipt must never be rewritten")

        cases = (
            (
                "schema_version",
                lambda value: value.__setitem__("schema_version", True),
                "invalid identity fields",
            ),
            (
                "destination_nlink",
                lambda value: value["destination"].__setitem__("nlink", True),
                "invalid destination nlink",
            ),
        )
        for label, mutate, expected_error in cases:
            with self.subTest(label=label):
                tampered = json.loads(json.dumps(valid_receipt))
                mutate(tampered)
                tampered["identity_sha256"] = transfer._receipt_identity(tampered)
                tampered["receipt_id"] = (
                    f"coldreceipt_{tampered['identity_sha256'][:32]}"
                )
                receipt.chmod(0o600)
                receipt.write_text(
                    transfer.pretty_json(tampered), encoding="utf-8"
                )
                receipt.chmod(0o400)
                receipt_before = stable_file_identity(receipt)
                receipt_body = receipt.read_bytes()
                self.assertEqual(
                    tampered["identity_sha256"],
                    transfer._receipt_identity(tampered),
                )
                self.assertNotEqual(
                    0, self.validate_receipt_schema(receipt).returncode
                )

                with self.assertRaisesRegex(
                    transfer.ColdStorageError, expected_error
                ):
                    self.run_transfer(
                        statvfs_provider=unexpected_statvfs,
                        publish_fd_noreplace=unexpected_rename,
                    )

                self.assertEqual(
                    destination_before, stable_file_identity(destination)
                )
                self.assertEqual(receipt_before, stable_file_identity(receipt))
                self.assertEqual(receipt_body, receipt.read_bytes())
                self.assertEqual(source_before, stable_file_identity(self.source_path))
                self.assertEqual(rename_count, len(self.rename_calls))
                self.assert_no_temporaries()

    def test_payload_publication_eexist_fails_closed_then_recovers_once(self) -> None:
        source_before = stable_file_identity(self.source_path)
        collision_calls: list[str] = []

        def publish_competing_payload_then_eexist(
            _source_directory_fd: int,
            _source_name: str,
            destination_directory_fd: int,
            destination_name: str,
        ) -> None:
            collision_calls.append(destination_name)
            self.assertEqual("payload", destination_name)
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            descriptor = os.open(
                destination_name,
                flags,
                0o600,
                dir_fd=destination_directory_fd,
            )
            try:
                offset = 0
                while offset < len(self.payload):
                    count = os.write(descriptor, self.payload[offset:])
                    self.assertGreater(count, 0)
                    offset += count
                os.fsync(descriptor)
                os.fchmod(descriptor, 0o400)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.fsync(destination_directory_fd)
            raise FileExistsError(
                errno.EEXIST, os.strerror(errno.EEXIST), destination_name
            )

        with self.assertRaisesRegex(
            transfer.ColdStorageError, "appeared during no-replace publication"
        ):
            self.run_transfer(
                publish_fd_noreplace=publish_competing_payload_then_eexist
            )

        destination = self.destination_path()
        destination_after_collision = stable_file_identity(destination)
        self.assertEqual(["payload"], collision_calls)
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertEqual(0o400, stat.S_IMODE(destination.stat().st_mode))
        self.assertEqual(1, destination.stat().st_nlink)
        self.assertEqual([], list(self.receipt_root.iterdir()))
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_temporaries()

        with mock.patch.object(
            transfer,
            "_validate_sealed_file",
            wraps=transfer._validate_sealed_file,
        ) as validate_sealed:
            recovered = self.run_transfer()

        self.assertEqual(1, validate_sealed.call_count)
        self.assertEqual("recovered_existing", recovered["destination"]["admission"])
        self.assertEqual(1, recovered["verification"]["archive_full_read_count"])
        self.assertEqual(0, recovered["preflight"]["required_new_bytes"])
        self.assert_receipt_schema(recovered)
        self.assertEqual(
            destination_after_collision, stable_file_identity(destination)
        )
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertTrue(self.receipt_path(recovered).is_file())
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual(
            ["receipt.json"],
            [name for _, name in self.rename_calls],
        )
        self.assert_no_temporaries()

    def test_before_receipt_hook_payload_replacement_refuses_without_receipt(
        self,
    ) -> None:
        source_before = stable_file_identity(self.source_path)
        events: list[str] = []
        published_identity: list[tuple[int, ...]] = []
        replacement_identity: list[tuple[int, ...]] = []

        def replace_payload_before_receipt(event: str) -> None:
            events.append(event)
            if event != "before_receipt_write":
                return
            destination = self.destination_path()
            published_identity.append(stable_file_identity(destination))
            destination.unlink()
            destination.write_bytes(self.payload)
            destination.chmod(0o400)
            replacement_identity.append(stable_file_identity(destination))

        with self.assertRaisesRegex(
            transfer.ColdStorageError,
            "verified cold-storage target changed before receipt publication",
        ):
            self.run_transfer(fault_hook=replace_payload_before_receipt)

        destination = self.destination_path()
        self.assertEqual(
            [
                "after_temp_fsync",
                "after_payload_publish",
                "before_receipt_write",
            ],
            events,
        )
        self.assertEqual(1, len(published_identity))
        self.assertEqual(1, len(replacement_identity))
        self.assertNotEqual(
            published_identity[0][1], replacement_identity[0][1]
        )
        self.assertEqual(replacement_identity[0], stable_file_identity(destination))
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertEqual(0o400, stat.S_IMODE(destination.stat().st_mode))
        self.assertEqual([], list(self.receipt_root.rglob("receipt.json")))
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual(self.payload, self.source_path.read_bytes())
        self.assert_no_temporaries()

    def test_unnamed_payload_staging_has_no_raceable_path_on_crash(self) -> None:
        source_before = stable_file_identity(self.source_path)
        events: list[str] = []

        def crash_while_unnamed(event: str) -> None:
            if event != "after_temp_fsync":
                return
            events.append(event)
            self.assertEqual(
                [], list(self.destination_path().parent.glob(".payload.tmp-*"))
            )
            self.assertFalse(self.destination_path().exists())
            raise InjectedCrash("simulated process death during unnamed staging")

        with self.assertRaises(InjectedCrash):
            self.run_transfer(fault_hook=crash_while_unnamed)

        self.assertEqual(["after_temp_fsync"], events)
        self.assertFalse(self.destination_path().exists())
        self.assertEqual([], list(self.receipt_root.rglob("receipt.json")))
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual([], self.rename_calls)
        self.assert_no_temporaries()

    def test_payload_and_receipt_publish_from_unnamed_single_inodes(self) -> None:
        source_before = stable_file_identity(self.source_path)
        observations: list[tuple[str, int, tuple[str, ...]]] = []

        def inspect_then_publish(
            source_descriptor: int,
            source_label: str,
            destination_directory_fd: int,
            destination_name: str,
        ) -> None:
            observations.append(
                (
                    source_label,
                    os.fstat(source_descriptor).st_nlink,
                    tuple(sorted(os.listdir(destination_directory_fd))),
                )
            )
            self.publish_fd_noreplace(
                source_descriptor,
                source_label,
                destination_directory_fd,
                destination_name,
            )

        completed = self.run_transfer(publish_fd_noreplace=inspect_then_publish)

        self.assertEqual(
            ["<unnamed-payload>", "<unnamed-receipt>"],
            [label for label, _, _ in observations],
        )
        self.assertEqual([0, 0], [nlink for _, nlink, _ in observations])
        self.assertTrue(
            all(
                not any(name.startswith(".payload.tmp-") for name in entries)
                and not any(
                    name.startswith(".receipt.json.tmp-") for name in entries
                )
                for _, _, entries in observations
            )
        )
        self.assertEqual(self.payload, self.destination_path().read_bytes())
        self.assertEqual(1, self.destination_path().stat().st_nlink)
        self.assertEqual(1, self.receipt_path(completed).stat().st_nlink)
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assert_no_temporaries()

    def test_receipt_publication_eexist_with_different_valid_receipt_fails(
        self,
    ) -> None:
        source_before = stable_file_identity(self.source_path)
        proposed_receipts: list[dict] = []
        competing_receipts: list[dict] = []
        collision_calls: list[str] = []

        def publish_different_receipt_then_eexist(
            source_descriptor: int,
            source_name: str,
            destination_directory_fd: int,
            destination_name: str,
        ) -> None:
            if destination_name == "payload":
                self.publish_fd_noreplace(
                    source_descriptor,
                    source_name,
                    destination_directory_fd,
                    destination_name,
                )
                return

            self.assertEqual("receipt.json", destination_name)
            collision_calls.append(destination_name)
            size = os.fstat(source_descriptor).st_size
            proposed_body = os.pread(source_descriptor, size, 0)
            proposed = json.loads(proposed_body)
            proposed_receipts.append(proposed)
            competing = json.loads(json.dumps(proposed))
            competing_timestamp = "2001-02-03T04:05:06Z"
            competing["completed_at"] = competing_timestamp
            competing["destination"]["verified_at"] = competing_timestamp
            competing["catalog_location_candidate"][
                "verified_at"
            ] = competing_timestamp
            competing["identity_sha256"] = transfer._receipt_identity(competing)
            competing["receipt_id"] = (
                f"coldreceipt_{competing['identity_sha256'][:32]}"
            )
            competing_receipts.append(competing)
            body = transfer.pretty_json(competing).encode("utf-8")

            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            competing_fd = os.open(
                destination_name,
                flags,
                0o600,
                dir_fd=destination_directory_fd,
            )
            try:
                offset = 0
                while offset < len(body):
                    count = os.write(competing_fd, body[offset:])
                    self.assertGreater(count, 0)
                    offset += count
                os.fsync(competing_fd)
                os.fchmod(competing_fd, 0o400)
                os.fsync(competing_fd)
            finally:
                os.close(competing_fd)
            os.fsync(destination_directory_fd)
            raise FileExistsError(
                errno.EEXIST, os.strerror(errno.EEXIST), destination_name
            )

        with self.assertRaisesRegex(
            transfer.ColdStorageError,
            "pre-existing transfer receipt differs from the sealed receipt",
        ):
            self.run_transfer(
                publish_fd_noreplace=publish_different_receipt_then_eexist
            )

        self.assertEqual(1, len(proposed_receipts))
        self.assertEqual(1, len(competing_receipts))
        self.assertNotEqual(proposed_receipts[0], competing_receipts[0])
        self.assertEqual(["receipt.json"], collision_calls)
        destination = self.destination_path()
        receipt = (
            self.receipt_root
            / "cold-storage-transfers"
            / proposed_receipts[0]["transfer_id"]
            / "receipt.json"
        )
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertEqual(0o400, stat.S_IMODE(destination.stat().st_mode))
        self.assertEqual(0o400, stat.S_IMODE(receipt.stat().st_mode))
        self.assertEqual(1, receipt.stat().st_nlink)
        self.assertEqual(
            competing_receipts[0], json.loads(receipt.read_text(encoding="utf-8"))
        )
        validation = self.validate_receipt_schema(receipt)
        self.assertEqual(0, validation.returncode, validation.stderr)
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual(self.payload, self.source_path.read_bytes())
        self.assert_no_temporaries()

    def test_crash_after_payload_publish_recovers_without_recopy(self) -> None:
        source_before = stable_file_identity(self.source_path)
        events: list[str] = []

        def crash_after_payload(event: str) -> None:
            events.append(event)
            if event == "after_payload_publish":
                raise InjectedCrash("simulated process death after payload publish")

        with self.assertRaises(InjectedCrash):
            self.run_transfer(fault_hook=crash_after_payload)

        destination = self.destination_path()
        destination_after_crash = stable_file_identity(destination)
        self.assertEqual(
            ["after_temp_fsync", "after_payload_publish"], events
        )
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertEqual(0o400, stat.S_IMODE(destination.stat().st_mode))
        self.assertEqual([], list(self.receipt_root.iterdir()))
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual(["payload"], [name for _, name in self.rename_calls])

        recovered = self.run_transfer()
        receipt = self.receipt_path(recovered)
        self.assert_receipt_schema(recovered)
        self.assertEqual("recovered_existing", recovered["destination"]["admission"])
        self.assertIsNone(recovered["verification"]["copy_stream_sha256"])
        self.assertFalse(recovered["verification"]["temporary_file_fsynced"])
        self.assertEqual(destination_after_crash, stable_file_identity(destination))
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertTrue(receipt.is_file())
        self.assertEqual(source_before, stable_file_identity(self.source_path))
        self.assertEqual(
            ["payload", "receipt.json"],
            [name for _, name in self.rename_calls],
        )
        self.assert_no_temporaries()

        receipt_before = stable_file_identity(receipt)
        receipt_body = receipt.read_bytes()

        def unexpected_statvfs(_descriptor: int) -> object:
            raise AssertionError("recovered receipt replay must bypass capacity")

        def unexpected_rename(
            _source_directory_fd: int,
            _source_name: str,
            _destination_directory_fd: int,
            _destination_name: str,
        ) -> None:
            raise AssertionError("recovered receipt replay must not publish")

        replay = self.run_transfer(
            statvfs_provider=unexpected_statvfs,
            publish_fd_noreplace=unexpected_rename,
        )
        self.assertEqual(recovered, replay)
        self.assertEqual(destination_after_crash, stable_file_identity(destination))
        self.assertEqual(receipt_before, stable_file_identity(receipt))
        self.assertEqual(receipt_body, receipt.read_bytes())
        self.assertEqual(source_before, stable_file_identity(self.source_path))


if __name__ == "__main__":
    unittest.main()
