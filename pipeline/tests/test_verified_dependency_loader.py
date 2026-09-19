from __future__ import annotations

import hashlib
import importlib.util
import os
import pickle
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


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


LOADER = load_module(
    "himr_verified_dependency_loader_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/verified_dependency_loader.py",
)


class VerifiedDependencyLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()

    def tearDown(self) -> None:
        for name in tuple(sys.modules):
            if name.startswith("fixture.") or name.startswith("fixture_"):
                sys.modules.pop(name, None)
        self.temporary.cleanup()

    def source(self, name: str, body: bytes) -> Path:
        path = self.root / name
        path.write_bytes(body)
        path.chmod(0o644)
        return path

    def test_executes_only_exact_verified_bytes(self) -> None:
        body = b"VALUE = 41\n\ndef answer():\n    return VALUE + 1\n"
        path = self.source("valid_dependency.py", body)
        digest = hashlib.sha256(body).hexdigest()

        module, source = LOADER.load_verified_module(
            "fixture.valid_dependency", path, digest
        )

        self.assertEqual(module.answer(), 42)
        self.assertEqual(Path(module.__file__), path)
        self.assertEqual(source.body, body)
        self.assertEqual(source.sha256, digest)
        self.assertEqual(source.evidence()["inode"], path.stat().st_ino)

    def test_module_is_registered_during_and_after_verified_execution(self) -> None:
        body = (
            b"import sys\n"
            b"REGISTERED_DURING_EXEC = sys.modules.get(__name__) is not None\n"
            b"class Item:\n"
            b"    pass\n"
        )
        path = self.source("registered_dependency.py", body)
        module, _ = LOADER.load_verified_module(
            "fixture_registered_dependency", path, hashlib.sha256(body).hexdigest()
        )

        self.assertTrue(module.REGISTERED_DURING_EXEC)
        self.assertIs(sys.modules["fixture_registered_dependency"], module)
        self.assertIsInstance(pickle.loads(pickle.dumps(module.Item())), module.Item)

    def test_failed_execution_restores_preexisting_module_registration(self) -> None:
        name = "fixture.failing_dependency"
        previous = ModuleType(name)
        sys.modules[name] = previous
        body = b"raise RuntimeError('fixture failure')\n"
        path = self.source("failing_dependency.py", body)

        with self.assertRaisesRegex(RuntimeError, "fixture failure"):
            LOADER.load_verified_module(name, path, hashlib.sha256(body).hexdigest())

        self.assertIs(sys.modules[name], previous)

    def test_digest_mismatch_never_executes_candidate(self) -> None:
        marker = self.root / "must-not-exist"
        body = f"from pathlib import Path\nPath({str(marker)!r}).touch()\n".encode()
        path = self.source("untrusted_dependency.py", body)

        with self.assertRaisesRegex(
            LOADER.VerifiedDependencyError, "SHA-256 does not match"
        ):
            LOADER.load_verified_module(
                "fixture.untrusted_dependency", path, "0" * 64
            )

        self.assertFalse(marker.exists())

    def test_symlink_and_multiple_link_candidates_are_rejected(self) -> None:
        body = b"VALUE = 1\n"
        path = self.source("dependency.py", body)
        digest = hashlib.sha256(body).hexdigest()
        alias = self.root / "alias.py"
        alias.symlink_to(path)
        with self.assertRaisesRegex(
            LOADER.VerifiedDependencyError, "non-symlinked regular file"
        ):
            LOADER.load_verified_module("fixture.alias", alias, digest)

        alias.unlink()
        os.link(path, alias)
        with self.assertRaisesRegex(
            LOADER.VerifiedDependencyError, "exactly one hard link"
        ):
            LOADER.load_verified_module("fixture.hardlink", path, digest)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires POSIX named pipes")
    def test_regular_file_to_fifo_open_race_is_nonblocking_and_rejected(self) -> None:
        body = b"VALUE = 1\n"
        path = self.source("raced_dependency.py", body)
        digest = hashlib.sha256(body).hexdigest()
        original_open = LOADER.os.open
        observed_flags: list[int] = []

        def racing_open(path_value: object, flags: int, *args: object) -> int:
            observed_flags.append(flags)
            self.assertTrue(flags & getattr(os, "O_NONBLOCK", 0))
            path.unlink()
            os.mkfifo(path, mode=0o600)
            return original_open(path_value, flags, *args)

        with (
            mock.patch.object(LOADER.os, "open", side_effect=racing_open),
            self.assertRaisesRegex(
                LOADER.VerifiedDependencyError, "descriptor is not a regular file"
            ),
        ):
            LOADER.read_verified_source(path, digest)

        self.assertEqual(len(observed_flags), 1)

    def test_metadata_drift_during_retained_read_fails_before_execution(self) -> None:
        marker = self.root / "must-not-run"
        prefix = f"from pathlib import Path\nPath({str(marker)!r}).touch()\n#".encode()
        body = prefix + b"x" * (LOADER.READ_CHUNK_BYTES + 128) + b"\n"
        path = self.source("drifting_dependency.py", body)
        digest = hashlib.sha256(body).hexdigest()
        original_read = LOADER.os.read
        changed = False

        def drifting_read(descriptor: int, byte_count: int) -> bytes:
            nonlocal changed
            chunk = original_read(descriptor, byte_count)
            if chunk and not changed:
                changed = True
                observed = path.stat()
                os.utime(
                    path,
                    ns=(observed.st_atime_ns, observed.st_mtime_ns + 1_000_000),
                )
            return chunk

        with (
            mock.patch.object(LOADER.os, "read", side_effect=drifting_read),
            self.assertRaisesRegex(
                LOADER.VerifiedDependencyError, "metadata changed while it was read"
            ),
        ):
            LOADER.load_verified_module("fixture.drifting_dependency", path, digest)

        self.assertTrue(changed)
        self.assertFalse(marker.exists())

    def test_rejects_relative_path_bad_hash_and_bad_module_name(self) -> None:
        with self.assertRaises(LOADER.VerifiedDependencyError):
            LOADER.read_verified_source("relative.py", "0" * 64)
        with self.assertRaises(LOADER.VerifiedDependencyError):
            LOADER.read_verified_source(self.root / "missing.py", "not-a-hash")
        with self.assertRaises(LOADER.VerifiedDependencyError):
            LOADER.load_verified_module(
                "not/a/module", self.root / "missing.py", "0" * 64
            )


if __name__ == "__main__":
    unittest.main()
