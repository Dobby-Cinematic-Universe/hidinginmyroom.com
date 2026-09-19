from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
TEST_WORK_ROOT = ROOT / "pipeline/.test-work"
FS_UUID = "27bd4222-a8f3-4d91-b48f-7ce38d70e507"


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


REGISTER = load_module(
    "himr_gpu_register_portable_root_test_module",
    ROOT / "pipeline/gpu/register_portable_root.py",
)


class RegisterPortableRootTests(unittest.TestCase):
    def test_create_is_private_exact_and_restart_portable(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        with tempfile.TemporaryDirectory(dir=TEST_WORK_ROOT) as temporary:
            parent = Path(temporary)
            parent.chmod(0o700)
            managed = parent / "managed"
            managed.mkdir(mode=0o700)
            output = parent / "registration.json"
            with mock.patch.object(
                REGISTER.PORTABLE,
                "btrfs_filesystem_uuid",
                return_value=FS_UUID,
            ):
                value = REGISTER.create_registration(
                    root=managed,
                    root_id="himr-hot-test-v1",
                    filesystem_uuid=FS_UUID,
                    output=output,
                )
            self.assertEqual(output.stat().st_mode & 0o777, 0o400)
            self.assertEqual(value["filesystem"]["uuid"], FS_UUID)
            self.assertFalse(
                value["policy"]["historical_stat_fields_are_authoritative"]
            )
            self.assertEqual(
                REGISTER.PORTABLE.load_registration(
                    output,
                    REGISTER.PORTABLE.sha256_bytes(output.read_bytes()),
                    expected_document_uid=os.geteuid(),
                    expected_document_mode=0o400,
                ),
                value,
            )

    def test_no_replace_and_cold_root_are_rejected(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        with tempfile.TemporaryDirectory(dir=TEST_WORK_ROOT) as temporary:
            parent = Path(temporary)
            parent.chmod(0o700)
            managed = parent / "managed"
            managed.mkdir(mode=0o700)
            output = parent / "registration.json"
            with mock.patch.object(
                REGISTER.PORTABLE,
                "btrfs_filesystem_uuid",
                return_value=FS_UUID,
            ):
                REGISTER.create_registration(
                    root=managed,
                    root_id="himr-hot-test-v1",
                    filesystem_uuid=FS_UUID,
                    output=output,
                )
                with self.assertRaisesRegex(
                    REGISTER.RegistrationCommandError, "new absolute"
                ):
                    REGISTER.create_registration(
                        root=managed,
                        root_id="himr-hot-test-v1",
                        filesystem_uuid=FS_UUID,
                        output=output,
                    )
            with self.assertRaisesRegex(
                REGISTER.RegistrationCommandError, "cold storage"
            ):
                REGISTER.create_registration(
                    root=Path("/mnt/archive/HIMR"),
                    root_id="himr-cold-forbidden-v1",
                    filesystem_uuid=FS_UUID,
                    output=parent / "forbidden.json",
                )


if __name__ == "__main__":
    unittest.main()
