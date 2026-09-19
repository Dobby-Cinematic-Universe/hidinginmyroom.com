from __future__ import annotations

import errno
import hashlib
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipeline import media_preprocess_v034 as successor


class MediaPreprocessSuccessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="media-preprocess-v034-"
        )
        self.root = Path(self.temporary.name)
        self.source = self.root / "source.bin"
        self.payload = (b"verified-single-link-reuse\n" * 1024) + b"tail"
        self.source.write_bytes(self.payload)
        self.source.chmod(0o444)

    def tearDown(self) -> None:
        for path in sorted(self.root.rglob("*"), reverse=True):
            try:
                path.chmod(0o700 if path.is_dir() else 0o600)
            except OSError:
                pass
        self.temporary.cleanup()

    def assert_single_link_copy(self, destination: Path) -> None:
        self.assertEqual(self.payload, destination.read_bytes())
        self.assertNotEqual(self.source.stat().st_ino, destination.stat().st_ino)
        self.assertEqual(1, self.source.stat().st_nlink)
        self.assertEqual(1, destination.stat().st_nlink)
        self.assertEqual(0o444, stat.S_IMODE(destination.stat().st_mode))
        self.assertEqual(
            hashlib.sha256(self.payload).hexdigest(),
            hashlib.sha256(destination.read_bytes()).hexdigest(),
        )

    def test_legacy_source_pin_is_exact_and_unchanged(self) -> None:
        observed = successor.verify_legacy_source()
        self.assertEqual(successor.LEGACY_SOURCE_SHA256, observed["sha256"])
        self.assertEqual(
            successor.LEGACY_SOURCE_SHA256,
            hashlib.sha256(successor.LEGACY_SOURCE_PATH.read_bytes()).hexdigest(),
        )
        self.assertEqual(0o755, stat.S_IMODE(successor.LEGACY_SOURCE_PATH.stat().st_mode))

    def test_reuse_publishes_a_distinct_single_link_inode(self) -> None:
        destination = self.root / "destination" / "artifact.bin"
        successor.single_link_immutable_reuse(
            self.source, destination, "test artifact"
        )
        self.assert_single_link_copy(destination)
        self.assertEqual([], list(destination.parent.glob(".*.reuse-*")))

    def test_unsupported_reflink_falls_back_to_verified_byte_copy(self) -> None:
        destination = self.root / "fallback" / "artifact.bin"
        with mock.patch.object(
            successor.fcntl,
            "ioctl",
            side_effect=OSError(errno.EOPNOTSUPP, "unsupported"),
        ):
            successor.single_link_immutable_reuse(
                self.source, destination, "test artifact"
            )
        self.assert_single_link_copy(destination)

    def test_existing_destination_is_never_overwritten(self) -> None:
        destination = self.root / "existing" / "artifact.bin"
        destination.parent.mkdir()
        destination.write_bytes(b"existing")
        destination.chmod(0o444)
        with self.assertRaisesRegex(
            successor.PipelineError, "destination already exists"
        ):
            successor.single_link_immutable_reuse(
                self.source, destination, "test artifact"
            )
        self.assertEqual(b"existing", destination.read_bytes())
        self.assertEqual([], list(destination.parent.glob(".*.reuse-*")))

    def test_unexpected_reflink_failure_cleans_temporary(self) -> None:
        destination = self.root / "failed" / "artifact.bin"
        with (
            mock.patch.object(
                successor.fcntl,
                "ioctl",
                side_effect=OSError(errno.EIO, "injected failure"),
            ),
            self.assertRaisesRegex(successor.PipelineError, "reflink failed"),
        ):
            successor.single_link_immutable_reuse(
                self.source, destination, "test artifact"
            )
        self.assertFalse(destination.exists())
        self.assertEqual([], list(destination.parent.glob(".*.reuse-*")))

    def test_destination_parent_swap_fails_closed_and_cleans_retained_parent(self) -> None:
        parent = self.root / "publish"
        parent.mkdir()
        held = self.root / "publish-held"
        destination = parent / "artifact.bin"
        real_rename = successor._rename_noreplace

        def swap_then_rename(
            source_directory: int,
            source_name: str,
            destination_directory: int,
            destination_name: str,
        ) -> None:
            parent.rename(held)
            parent.mkdir()
            real_rename(
                source_directory,
                source_name,
                destination_directory,
                destination_name,
            )

        with (
            mock.patch.object(
                successor, "_rename_noreplace", side_effect=swap_then_rename
            ),
            self.assertRaisesRegex(successor.PipelineError, "not single-link"),
        ):
            successor.single_link_immutable_reuse(
                self.source, destination, "test artifact"
            )
        self.assertFalse(destination.exists())
        self.assertFalse((held / destination.name).exists())
        self.assertEqual([], list(held.glob(".*.reuse-*")))


if __name__ == "__main__":
    unittest.main()

