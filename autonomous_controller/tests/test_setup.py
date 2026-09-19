from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from autonomous_controller.config import build_config, canonical_bytes, sha256_bytes
from autonomous_controller.setup_archive_all_known import (
    INVENTORY,
    SCHEDULE_SET,
    SetupError,
    _write_sealed_no_replace,
    archive_all_known_config_core,
)


class ArchiveAllKnownSetupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-autonomy-setup-")
        self.root = Path(self.temporary.name).resolve()
        os.chmod(self.root, 0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_fixed_core_targets_exact_inventory_and_full_schedule_set(self) -> None:
        names = (
            "state",
            "preprocess-control",
            "preprocess-output",
            "gpu-queues",
            "gpu-work-orders",
            "gpu-materializations",
            "gpu-results",
            "gpu-batches",
            "gpu-events",
            "gpu-locks",
            "cold-staging",
            "cold-receipts",
        )
        roots = {name: self.root / name for name in names}
        document = build_config(archive_all_known_config_core(roots))
        self.assertEqual(INVENTORY, document["campaign"]["inventory"])
        self.assertEqual(SCHEDULE_SET, document["campaign"]["schedule_set"])
        self.assertEqual(193, len(document["campaign"]["schedules"]))
        self.assertEqual(
            102,
            sum(
                row["role"] == "normal_processing"
                for row in document["campaign"]["schedules"]
            ),
        )
        self.assertEqual(
            91,
            sum(
                row["role"] == "cold_acquisition_only_requires_chunking"
                for row in document["campaign"]["schedules"]
            ),
        )
        self.assertEqual(
            roots["state"] / "gpu-children",
            Path(document["gpu_readiness"]["child_journal_root"]),
        )
        self.assertEqual(1, document["gpu_readiness"]["max_active_children"])
        self.assertFalse(document["cold_retention"]["enabled"])
        self.assertGreaterEqual(
            document["acquisition"]["cold_acquisition_only_requires_chunking"][
                "max_new_bytes"
            ],
            64 * 1024**3,
        )
        self.assertEqual("none", document["safety"]["publication_authority"])
        self.assertEqual("none", document["safety"]["deletion_authority"])

    def test_config_write_is_canonical_mode_0400_and_never_overwrites(self) -> None:
        output = self.root / "controller.json"
        body = canonical_bytes({"fixture": True})
        _write_sealed_no_replace(output, body)
        self.assertEqual(body, output.read_bytes())
        self.assertEqual(0o400, output.stat().st_mode & 0o777)
        self.assertEqual(1, output.stat().st_nlink)
        self.assertEqual(sha256_bytes(body), sha256_bytes(output.read_bytes()))
        with self.assertRaisesRegex(SetupError, "overwrite"):
            _write_sealed_no_replace(output, body)


if __name__ == "__main__":
    unittest.main()
