from __future__ import annotations

import importlib.util
import os
import stat
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from types import ModuleType
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPOSITORY_ROOT / "pipeline/gpu/portable_root.py"
TEST_WORK_ROOT = REPOSITORY_ROOT / "pipeline/.test-work"
TEST_UUID = "2f0df088-4057-48f3-9be6-3f20c26fc6df"


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


PORTABLE = load_module("himr_gpu_portable_root_test_module", MODULE_PATH)


def recursive_keys(value: object) -> set[str]:
    result: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            result.add(str(key))
            result.update(recursive_keys(child))
    elif isinstance(value, list):
        for child in value:
            result.update(recursive_keys(child))
    return result


class PortableRootTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="gpu-portable-root-test-", dir=TEST_WORK_ROOT
        )
        self.base = Path(self.temporary.name)
        self.root = self.base / "managed-root"
        self.root.mkdir(mode=0o700)
        self.payload = self.root / "payload.bin"
        self.payload.write_bytes(b"portable-root-fixture\n")
        self.payload.chmod(0o400)
        self.registration = PORTABLE.make_registration(
            root_id="gpu-hot-test-v1",
            tier="hot_main_drive",
            path=self.root,
            filesystem_uuid=TEST_UUID,
            owner_uid=os.geteuid(),
            historical_observation={
                "st_dev": 2**62,
                "st_ino": 123456,
                "st_mtime_ns": 100,
                "st_ctime_ns": 101,
            },
        )
        self.registration_path = self.base / "registration.json"
        self.registration_path.write_bytes(PORTABLE.canonical_bytes(self.registration))
        self.registration_path.chmod(0o400)
        self.registration_sha256 = PORTABLE.sha256_bytes(
            self.registration_path.read_bytes()
        )
        self.uuid_patch = mock.patch.object(
            PORTABLE, "btrfs_filesystem_uuid", return_value=TEST_UUID
        )
        self.uuid_patch.start()

    def tearDown(self) -> None:
        self.uuid_patch.stop()
        self.temporary.cleanup()

    def open_root(self, **kwargs: object):
        return PORTABLE.RetainedRoot.open(self.registration, **kwargs)

    def test_registration_identity_is_canonical_append_only_and_strict(self) -> None:
        observed = PORTABLE.validate_registration(self.registration)
        self.assertEqual(observed, self.registration)
        self.assertEqual(
            observed["registration_id"],
            f"gpurootreg_{observed['identity_sha256'][:32]}",
        )
        self.assertEqual(
            PORTABLE.canonical_bytes(observed), self.registration_path.read_bytes()
        )
        successor = PORTABLE.make_registration(
            root_id="gpu-hot-test-v1",
            tier="hot_main_drive",
            path=self.root,
            filesystem_uuid=TEST_UUID,
            owner_uid=os.geteuid(),
            predecessor={
                "registration_id": observed["registration_id"],
                "identity_sha256": observed["identity_sha256"],
            },
        )
        self.assertEqual(
            successor["predecessor"]["registration_id"],
            observed["registration_id"],
        )
        changed = dict(observed)
        changed["tier"] = "trusted_execution_snapshot"
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "identity"):
            PORTABLE.validate_registration(changed)
        unknown = dict(observed)
        unknown["mutable_pointer"] = True
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "exactly"):
            PORTABLE.validate_registration(unknown)
        wrong_typed_policy = dict(observed)
        wrong_typed_policy["policy"] = dict(observed["policy"])
        wrong_typed_policy["policy"]["append_only_successor_documents"] = 1
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "policy"):
            PORTABLE.validate_registration(wrong_typed_policy)

    def test_registration_file_load_binds_digest_root_tier_and_path(self) -> None:
        loaded = PORTABLE.load_registration(
            self.registration_path,
            self.registration_sha256,
            expected_root_id="gpu-hot-test-v1",
            expected_tier="hot_main_drive",
            expected_path=self.root,
        )
        self.assertEqual(loaded, self.registration)
        self.assertEqual(
            PORTABLE.load_registration(
                self.registration_path,
                self.registration_sha256,
                expected_document_uid=os.geteuid(),
                expected_document_mode=0o400,
            ),
            self.registration,
        )
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "owner/mode"):
            PORTABLE.load_registration(
                self.registration_path,
                self.registration_sha256,
                expected_document_uid=0,
                expected_document_mode=0o444,
            )
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "SHA-256 differs"):
            PORTABLE.load_registration(self.registration_path, "0" * 64)
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "tier differs"):
            PORTABLE.load_registration(
                self.registration_path,
                self.registration_sha256,
                expected_tier="trusted_execution_snapshot",
            )
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "path differs"):
            PORTABLE.load_registration(
                self.registration_path,
                self.registration_sha256,
                expected_path=self.base / "different-root",
            )

    def test_historical_reboot_stat_observation_is_not_replay_authority(self) -> None:
        current = self.root.stat()
        self.assertNotEqual(
            self.registration["historical_observation"]["st_dev"], current.st_dev
        )
        self.assertNotEqual(
            self.registration["historical_observation"]["st_ino"], current.st_ino
        )
        with self.open_root(expected_tier="hot_main_drive") as retained:
            retained.verify()
            self.assertEqual(retained.filesystem_uuid, TEST_UUID)
        self.assertFalse(
            self.registration["policy"]["historical_stat_fields_are_authoritative"]
        )

    def test_wrong_current_btrfs_uuid_is_rejected(self) -> None:
        with mock.patch.object(
            PORTABLE,
            "btrfs_filesystem_uuid",
            return_value="695c88d6-52b8-4546-ad01-ae1734237ad8",
        ):
            with self.assertRaisesRegex(PORTABLE.PortableRootError, "UUID differs"):
                self.open_root()

    def test_retained_root_binds_expected_owner_tier_and_path(self) -> None:
        with self.open_root(
            expected_root_id="gpu-hot-test-v1",
            expected_tier="hot_main_drive",
            expected_path=self.root,
        ) as retained:
            self.assertEqual(retained.path, self.root)
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "tier differs"):
            self.open_root(expected_tier="trusted_execution_snapshot")
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "path differs"):
            self.open_root(expected_path=self.base / "different-root")
        wrong_owner = PORTABLE.make_registration(
            root_id="gpu-hot-test-v1",
            tier="hot_main_drive",
            path=self.root,
            filesystem_uuid=TEST_UUID,
            owner_uid=os.geteuid() + 1,
        )
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "owner"):
            PORTABLE.RetainedRoot.open(wrong_owner)

    def test_btrfs_uuid_is_read_from_retained_fd_ioctl(self) -> None:
        self.uuid_patch.stop()
        try:
            observed_request: list[int] = []

            def fake_ioctl(
                descriptor: int, request: int, buffer: bytearray, mutate: bool
            ) -> int:
                self.assertGreaterEqual(descriptor, 0)
                self.assertTrue(mutate)
                observed_request.append(request)
                raw = uuid.UUID(TEST_UUID).bytes
                start = PORTABLE.BTRFS_FSID_OFFSET
                buffer[start : start + len(raw)] = raw
                return 0

            descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with mock.patch.object(PORTABLE.fcntl, "ioctl", side_effect=fake_ioctl):
                    self.assertEqual(
                        PORTABLE.btrfs_filesystem_uuid(descriptor), TEST_UUID
                    )
            finally:
                os.close(descriptor)
            self.assertEqual(observed_request, [PORTABLE.BTRFS_IOC_FS_INFO])
        finally:
            self.uuid_patch.start()

    def test_registration_symlink_hardlink_and_fifo_are_rejected(self) -> None:
        symlink = self.base / "registration-link.json"
        symlink.symlink_to(self.registration_path.name)
        with self.assertRaisesRegex(
            PORTABLE.PortableRootError, "direct regular file|safely open|mode-0400"
        ):
            PORTABLE.load_registration(symlink, self.registration_sha256)

        hardlink = self.base / "registration-hardlink.json"
        os.link(self.registration_path, hardlink)
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "single-link"):
            PORTABLE.load_registration(self.registration_path, self.registration_sha256)
        hardlink.unlink()

        fifo = self.base / "registration.fifo"
        os.mkfifo(fifo, 0o400)
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "regular file"):
            PORTABLE.load_registration(fifo, "0" * 64)

    def test_registration_file_replacement_during_read_is_rejected(self) -> None:
        original_reader = PORTABLE._read_bounded_fd
        replaced = False

        def replacing_reader(descriptor: int, maximum: int, label: str) -> bytes:
            nonlocal replaced
            body = original_reader(descriptor, maximum, label)
            if label == "root registration document" and not replaced:
                replaced = True
                stale = self.base / "registration-stale.json"
                self.registration_path.rename(stale)
                self.registration_path.write_bytes(body)
                self.registration_path.chmod(0o400)
            return body

        with mock.patch.object(
            PORTABLE, "_read_bounded_fd", side_effect=replacing_reader
        ):
            with self.assertRaisesRegex(PORTABLE.PortableRootError, "changed"):
                PORTABLE.load_registration(
                    self.registration_path, self.registration_sha256
                )

    def test_registered_root_symlink_component_is_rejected(self) -> None:
        real = self.base / "real-root"
        real.mkdir(mode=0o700)
        linked = self.base / "linked-root"
        linked.symlink_to(real.name, target_is_directory=True)
        registration = PORTABLE.make_registration(
            root_id="gpu-linked-test",
            tier="hot_main_drive",
            path=linked,
            filesystem_uuid=TEST_UUID,
            owner_uid=os.geteuid(),
        )
        with self.assertRaisesRegex(PORTABLE.PortableRootError, "unsafe"):
            PORTABLE.RetainedRoot.open(registration)

    def test_relative_symlink_hardlink_and_fifo_are_rejected(self) -> None:
        symlink = self.root / "payload-link.bin"
        symlink.symlink_to(self.payload.name)
        hardlink = self.root / "payload-hardlink.bin"
        os.link(self.payload, hardlink)
        fifo = self.root / "payload.fifo"
        os.mkfifo(fifo, 0o400)
        with self.open_root() as retained:
            with self.assertRaises(PORTABLE.PortableRootError):
                retained.open_file("payload-link.bin", allowed_modes={0o400})

            with self.assertRaisesRegex(PORTABLE.PortableRootError, "link policy"):
                retained.open_file("payload.bin", allowed_modes={0o400})

            with self.assertRaisesRegex(PORTABLE.PortableRootError, "regular file"):
                retained.open_file("payload.fifo", allowed_modes={0o400})

    def test_stable_file_evidence_has_no_filesystem_object_authority(self) -> None:
        with self.open_root() as retained:
            with retained.open_file(
                "payload.bin", allowed_modes={0o400}
            ) as payload:
                evidence = payload.stable_evidence(1024)
        self.assertEqual(
            set(evidence),
            {
                "relative_path",
                "kind",
                "sha256",
                "byte_count",
                "mode",
                "owner_policy",
                "link_policy",
            },
        )
        self.assertTrue(
            recursive_keys(evidence).isdisjoint(
                {
                    "st_dev",
                    "st_ino",
                    "device",
                    "inode",
                    "mount_id",
                    "st_mtime_ns",
                    "st_ctime_ns",
                }
            )
        )

    def test_live_same_filesystem_rejects_current_mount_mismatch(self) -> None:
        with self.open_root() as retained:
            descriptor = os.open(self.payload, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                observation = PORTABLE.require_live_same_filesystem(
                    retained, descriptor, "payload"
                )
                self.assertEqual(observation.filesystem_uuid, TEST_UUID)
                with mock.patch.object(
                    PORTABLE,
                    "current_mount_id",
                    return_value=retained.mount_id + 1,
                ):
                    with self.assertRaisesRegex(
                        PORTABLE.PortableRootError, "current filesystem and mount"
                    ):
                        PORTABLE.require_live_same_filesystem(
                            retained, descriptor, "payload"
                        )
            finally:
                os.close(descriptor)

    def test_retained_file_replacement_during_operation_is_rejected(self) -> None:
        with self.open_root() as retained:
            payload = retained.open_file("payload.bin", allowed_modes={0o400})
            try:
                stale = self.root / "payload-stale.bin"
                self.payload.rename(stale)
                self.payload.write_bytes(b"portable-root-fixture\n")
                self.payload.chmod(0o400)
                with self.assertRaisesRegex(
                    PORTABLE.PortableRootError, "identity changed"
                ):
                    payload.verify()
            finally:
                payload.close()

    def test_leaf_replacement_between_inspection_and_open_is_rejected(self) -> None:
        inputs = self.root / "inputs"
        inputs.mkdir(mode=0o700)
        original = inputs / "source.bin"
        original.write_bytes(b"original\n")
        original.chmod(0o400)
        retained = self.open_root()
        original_open = PORTABLE.os.open
        replaced = False

        def racing_open(
            path: object,
            flags: int,
            mode: int = 0o777,
            *,
            dir_fd: int | None = None,
        ) -> int:
            nonlocal replaced
            if path == "source.bin" and dir_fd is not None and not replaced:
                replaced = True
                original.rename(inputs / "stale.bin")
                original.write_bytes(b"replacement\n")
                original.chmod(0o400)
            if dir_fd is None:
                return original_open(path, flags, mode)
            return original_open(path, flags, mode, dir_fd=dir_fd)

        try:
            with mock.patch.object(PORTABLE.os, "open", side_effect=racing_open):
                with self.assertRaisesRegex(
                    PORTABLE.PortableRootError, "retained non-symlink regular"
                ):
                    retained.open_file("inputs/source.bin", allowed_modes={0o400})
            self.assertTrue(replaced)
        finally:
            retained.close()

    def test_retained_root_replacement_during_operation_is_rejected(self) -> None:
        retained = self.open_root()
        try:
            stale = self.base / "managed-root-stale"
            self.root.rename(stale)
            self.root.mkdir(mode=0o700)
            with self.assertRaisesRegex(PORTABLE.PortableRootError, "identity changed"):
                retained.verify()
        finally:
            retained.close()

    def test_retained_root_allows_unrelated_child_entry_churn(self) -> None:
        with self.open_root() as retained:
            unrelated = self.root / "unrelated-directory"
            unrelated.mkdir(mode=0o700)
            retained.verify()
            unrelated.rmdir()
            retained.verify()

    def test_retained_root_allows_ancestor_sibling_churn(self) -> None:
        with self.open_root() as retained:
            sibling = self.base / "unrelated-ancestor-directory"
            sibling.mkdir(mode=0o700)
            retained.verify()
            sibling.rmdir()
            retained.verify()

    def test_retained_file_allows_sibling_entry_churn(self) -> None:
        with self.open_root() as retained:
            with retained.open_file(
                "payload.bin", allowed_modes={0o400}
            ) as payload:
                sibling = self.root / "unrelated-file"
                sibling.write_bytes(b"unrelated\n")
                sibling.chmod(0o400)
                payload.verify()
                sibling.unlink()
                payload.verify()

    def test_retained_root_mode_change_remains_rejected(self) -> None:
        retained = self.open_root()
        try:
            self.root.chmod(0o750)
            with self.assertRaisesRegex(
                PORTABLE.PortableRootError, "identity changed|metadata"
            ):
                retained.verify()
        finally:
            self.root.chmod(0o700)
            retained.close()


if __name__ == "__main__":
    unittest.main()
