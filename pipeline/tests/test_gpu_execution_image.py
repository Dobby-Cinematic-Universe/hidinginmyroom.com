from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPOSITORY_ROOT / "pipeline/gpu/build_execution_image.py"


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


IMAGE = load_module("himr_gpu_execution_image_test_module", SOURCE_PATH)
LEGACY_RECEIPT_PATH = (
    REPOSITORY_ROOT
    / "research/corpus/gpu-runtime/portable-v2-local-private-20260829T211303Z/"
    "execution-v2-receipt.json"
)
LEGACY_RECEIPT_SHA256 = (
    "9da089609a6bfcc7bbc0028ace9592b542a08c600de4a319159f7855d1a40a81"
)


def physical_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class GPUExecutionImageLegacyCompatibilityTests(unittest.TestCase):
    def legacy_receipt(self) -> dict[str, object]:
        if not LEGACY_RECEIPT_PATH.is_file():
            self.skipTest("sealed local-private v2 receipt is unavailable")
        return json.loads(LEGACY_RECEIPT_PATH.read_text(encoding="utf-8"))

    @staticmethod
    def resign(receipt: dict[str, object]) -> None:
        core = {
            key: value
            for key, value in receipt.items()
            if key not in {"identity_sha256", "receipt_id"}
        }
        identity = IMAGE.sha256_bytes(IMAGE.canonical_bytes(core))
        receipt["identity_sha256"] = identity
        receipt["receipt_id"] = f"gpuexecimg_{identity[:32]}"

    def test_exact_sealed_legacy_builder_receipt_replays(self) -> None:
        receipt = self.legacy_receipt()
        self.assertEqual(
            IMAGE.load_receipt(
                LEGACY_RECEIPT_PATH,
                expected_sha256=LEGACY_RECEIPT_SHA256,
                verify_image=False,
            ),
            receipt,
        )

    def test_legacy_builder_requires_exact_physical_receipt(self) -> None:
        receipt = self.legacy_receipt()
        with self.assertRaisesRegex(
            IMAGE.ExecutionImageError, "current builder source differs"
        ):
            IMAGE._validate_receipt_document(receipt)

    def test_legacy_builder_near_miss_is_rejected_even_when_resigned(self) -> None:
        receipt = self.legacy_receipt()
        receipt["builder"]["portable_root_sha256"] = "0" * 64
        self.resign(receipt)
        with self.assertRaisesRegex(
            IMAGE.ExecutionImageError, "current builder source differs"
        ):
            IMAGE._validate_receipt_document(
                receipt, physical_receipt_sha256=LEGACY_RECEIPT_SHA256
            )

    def test_legacy_builder_cannot_authorize_a_different_receipt(self) -> None:
        receipt = self.legacy_receipt()
        receipt["intended_mount_path"] = "/run/user/1000/himr-gpu-v2/other"
        self.resign(receipt)
        with self.assertRaisesRegex(
            IMAGE.ExecutionImageError, "current builder source differs"
        ):
            IMAGE._validate_receipt_document(
                receipt, physical_receipt_sha256=LEGACY_RECEIPT_SHA256
            )


@unittest.skipUnless(Path("/usr/bin/mksquashfs").is_file(), "mksquashfs unavailable")
class GPUExecutionImageTests(unittest.TestCase):
    def setUp(self) -> None:
        research = REPOSITORY_ROOT / "research"
        research.mkdir(mode=0o700, exist_ok=True)
        try:
            filesystem = IMAGE._filesystem_for(research)
        except IMAGE.ExecutionImageError as error:
            self.skipTest(f"Btrfs test root unavailable: {error}")
        self.filesystem_uuid = filesystem["uuid"]
        self.temporary = tempfile.TemporaryDirectory(
            prefix="gpu-execution-image-test-", dir=research
        )
        self.root = Path(self.temporary.name).resolve()
        os.chmod(self.root, 0o700)
        self.sources = self.root / "sources"
        self.sources.mkdir(mode=0o700)
        self.runtime = self.sources / "runtime-source"
        self.runtime.mkdir(mode=0o700)
        self.python = self.sources / "python.bin"
        self.python.write_bytes(b"fixture-python\n")
        self.python.chmod(0o700)
        self.model = self.sources / "model-source"
        self.model.mkdir(mode=0o700)
        self.weights = self.sources / "model.bin"
        self.weights.write_bytes(b"fixture-model\x00bytes\n")
        self.weights.chmod(0o600)
        self.adapter = self.sources / "adapter.py"
        self.adapter.write_text("print('fixture')\n", encoding="utf-8")
        self.adapter.chmod(0o600)
        self.outputs = self.root / "outputs"
        self.outputs.mkdir(mode=0o700)
        self.receipts = self.root / "receipts"
        self.receipts.mkdir(mode=0o700)

    def tearDown(self) -> None:
        if hasattr(self, "temporary"):
            self.temporary.cleanup()

    def spec_value(self, *, python_source: Path | None = None) -> dict[str, object]:
        python_source = python_source or self.python
        entries = [
            {
                "kind": "directory",
                "source_path": str(self.runtime),
                "image_relative_path": "runtime",
                "image_mode": "0555",
            },
            {
                "kind": "regular_file",
                "source_path": str(python_source),
                "image_relative_path": "runtime/bin-python",
                "image_mode": "0555",
            },
            {
                "kind": "directory",
                "source_path": str(self.model),
                "image_relative_path": "model",
                "image_mode": "0555",
            },
            {
                "kind": "regular_file",
                "source_path": str(self.weights),
                "image_relative_path": "model/model.bin",
                "image_mode": "0444",
            },
            {
                "kind": "directory",
                "source_path": str(self.sources),
                "image_relative_path": "sources",
                "image_mode": "0555",
            },
            {
                "kind": "regular_file",
                "source_path": str(self.adapter),
                "image_relative_path": "sources/adapter.py",
                "image_mode": "0444",
            },
        ]
        entries.sort(key=lambda item: item["image_relative_path"])
        mappings = [
            {
                "name": "adapter_source",
                "image_relative_path": "sources/adapter.py",
                "sandbox_path": "/opt/himr-gpu/sources/adapter.py",
                "role": "source",
            },
            {
                "name": "model_root",
                "image_relative_path": "model",
                "sandbox_path": "/opt/himr-gpu/model",
                "role": "model_root",
            },
            {
                "name": "python_executable",
                "image_relative_path": "runtime/bin-python",
                "sandbox_path": "/opt/himr-gpu/runtime/bin-python",
                "role": "executable",
            },
            {
                "name": "runtime_root",
                "image_relative_path": "runtime",
                "sandbox_path": "/opt/himr-gpu/runtime",
                "role": "runtime_root",
            },
        ]
        mappings.sort(key=lambda item: item["name"])
        return {
            "kind": IMAGE.SPEC_KIND,
            "schema_version": IMAGE.SCHEMA_VERSION,
            "source_epoch": 1_700_000_000,
            "intended_mount_path": "/run/user/1000/himr-gpu-image",
            "entries": entries,
            "logical_mappings": mappings,
            "policy": dict(IMAGE.SPEC_POLICY),
        }

    def write_spec(
        self, value: dict[str, object] | None = None, *, name: str = "spec.json"
    ) -> tuple[Path, str]:
        path = self.root / name
        body = IMAGE.canonical_bytes(value or self.spec_value())
        path.write_bytes(body)
        path.chmod(0o400)
        return path, hashlib.sha256(body).hexdigest()

    def build(
        self,
        *,
        value: dict[str, object] | None = None,
        suffix: str = "one",
        image_mode: int = IMAGE.IMAGE_MODE,
    ) -> tuple[dict[str, object], Path, Path]:
        spec_path, spec_sha = self.write_spec(value, name=f"spec-{suffix}.json")
        image_path = self.outputs / f"execution-{suffix}.squashfs"
        receipt_path = self.receipts / f"execution-{suffix}.json"
        receipt = IMAGE.build_execution_image(
            spec_path=spec_path,
            expected_spec_sha256=spec_sha,
            image_path=image_path,
            receipt_path=receipt_path,
            filesystem_uuid=self.filesystem_uuid,
            maximum_wall_seconds=120,
            image_mode=image_mode,
        )
        return receipt, image_path, receipt_path

    def test_build_is_deterministic_and_replays_without_sources(self) -> None:
        first, first_image, first_receipt = self.build(suffix="one")
        second, second_image, _ = self.build(suffix="two")
        self.assertEqual(physical_sha256(first_image), physical_sha256(second_image))
        self.assertEqual(first["image"]["sha256"], second["image"]["sha256"])
        self.assertEqual(first_image.stat().st_mode & 0o777, 0o444)
        self.assertEqual(first_receipt.stat().st_mode & 0o777, 0o400)
        self.assertEqual(first["image"]["filesystem"]["type"], "btrfs")
        self.assertEqual(first["image"]["filesystem"]["uuid"], self.filesystem_uuid)
        self.assertFalse(first["policy"]["wheelhouse_execution_dependency"])
        self.assertEqual(first["build"]["tool"]["path"], "/usr/bin/mksquashfs")
        self.assertEqual(first["build"]["tool"]["uid"], 0)
        self.assertEqual(first["build"]["settings"]["compression"], "zstd")
        self.assertEqual(first["build"]["settings"]["command_options"][-1], "$PRIVATE_SORT_FILE")
        receipt_sha = physical_sha256(first_receipt)
        replayed = IMAGE.load_receipt(first_receipt, receipt_sha, verify_image=True)
        self.assertEqual(replayed, first)
        # Ordinary receipt-only replay does not touch any source path.
        with mock.patch.object(
            IMAGE, "_directory_observation", side_effect=AssertionError("source traversed")
        ):
            self.assertEqual(
                IMAGE.load_receipt(first_receipt, receipt_sha, verify_image=False),
                first,
            )

    def test_receipt_binds_full_source_projection_and_mappings(self) -> None:
        receipt, _, _ = self.build()
        source_tree = receipt["source_tree"]
        self.assertEqual(source_tree["entry_count"], 6)
        self.assertEqual(source_tree["regular_file_count"], 3)
        self.assertEqual(source_tree["directory_count"], 3)
        file_rows = [
            row for row in source_tree["entries"] if row["kind"] == "regular_file"
        ]
        self.assertEqual(
            {row["sha256"] for row in file_rows},
            {physical_sha256(path) for path in (self.python, self.weights, self.adapter)},
        )
        self.assertEqual(
            [row["name"] for row in receipt["logical_mappings"]],
            ["adapter_source", "model_root", "python_executable", "runtime_root"],
        )

    def test_local_private_image_mode_is_authenticated_and_replayed(self) -> None:
        receipt, image_path, receipt_path = self.build(
            suffix="private", image_mode=IMAGE.PRIVATE_IMAGE_MODE
        )
        self.assertEqual(receipt["image"]["mode"], "0400")
        self.assertEqual(stat.S_IMODE(image_path.stat().st_mode), 0o400)
        self.assertEqual(
            IMAGE.load_receipt(
                receipt_path,
                physical_sha256(receipt_path),
                verify_image=True,
                expected_image_uid=os.geteuid(),
            ),
            receipt,
        )
        self.assertEqual(receipt["intended_mount_path"], "/run/user/1000/himr-gpu-image")

    def test_sort_identity_is_independent_of_private_staging_path(self) -> None:
        first, _, _ = self.build(suffix="one")
        second, _, _ = self.build(suffix="two")
        first_settings = first["build"]["settings"]
        second_settings = second["build"]["settings"]
        self.assertEqual(first_settings["sort_sha256"], second_settings["sort_sha256"])
        self.assertEqual(
            first_settings["command_options"], second_settings["command_options"]
        )

    def test_image_hash_path_can_force_durable_bytes_before_publication(self) -> None:
        with mock.patch.object(IMAGE.os, "fsync", wraps=os.fsync) as sync:
            digest, count = IMAGE._hash_path(
                self.python,
                "durability fixture",
                1024,
                sync_before_return=True,
            )
        self.assertEqual(digest, physical_sha256(self.python))
        self.assertEqual(count, self.python.stat().st_size)
        sync.assert_called_once()

    def test_validator_rejects_mutable_or_drifted_image(self) -> None:
        _, image_path, receipt_path = self.build()
        receipt_sha = physical_sha256(receipt_path)
        image_path.chmod(0o600)
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "exact mode"):
            IMAGE.load_receipt(receipt_path, receipt_sha)
        image_path.write_bytes(image_path.read_bytes() + b"drift")
        image_path.chmod(0o444)
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "drifted"):
            IMAGE.load_receipt(receipt_path, receipt_sha)

    def test_validator_has_explicit_candidate_and_production_owner_policy(self) -> None:
        receipt, _, receipt_path = self.build()
        receipt_sha = physical_sha256(receipt_path)
        self.assertEqual(
            IMAGE.load_receipt(
                receipt_path,
                receipt_sha,
                verify_image=True,
                expected_image_uid=os.getuid(),
            ),
            receipt,
        )
        if os.getuid() != 0:
            with self.assertRaisesRegex(
                IMAGE.ExecutionImageError, "uid 0 mode 0444"
            ):
                IMAGE.load_receipt(
                    receipt_path,
                    receipt_sha,
                    verify_image=True,
                    expected_image_uid=0,
                )

    def test_validator_rejects_receipt_drift_and_expected_hash_mismatch(self) -> None:
        _, _, receipt_path = self.build()
        original_sha = physical_sha256(receipt_path)
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "expected value"):
            IMAGE.load_receipt(receipt_path, "0" * 64)
        receipt_path.chmod(0o600)
        value = json.loads(receipt_path.read_text(encoding="utf-8"))
        value["intended_mount_path"] = "/run/tampered"
        receipt_path.write_bytes(IMAGE.canonical_bytes(value))
        receipt_path.chmod(0o400)
        with self.assertRaises(IMAGE.ExecutionImageError):
            IMAGE.load_receipt(receipt_path, verify_image=False)
        self.assertNotEqual(physical_sha256(receipt_path), original_sha)

    def test_rejects_symlink_special_and_hardlinked_sources(self) -> None:
        cases: list[tuple[str, Path]] = []
        symlink = self.sources / "escape"
        symlink.symlink_to(self.python)
        cases.append(("regular file", symlink))
        fifo = self.sources / "fifo"
        os.mkfifo(fifo, 0o600)
        cases.append(("regular file", fifo))
        hardlink = self.sources / "hardlink"
        os.link(self.python, hardlink)
        cases.append(("hard link", self.python))
        for ordinal, (message, source) in enumerate(cases, start=1):
            with self.subTest(source=source.name):
                value = self.spec_value(python_source=source)
                spec_path, spec_sha = self.write_spec(
                    value, name=f"unsafe-{ordinal}.json"
                )
                with self.assertRaisesRegex(IMAGE.ExecutionImageError, message):
                    IMAGE.build_execution_image(
                        spec_path=spec_path,
                        expected_spec_sha256=spec_sha,
                        image_path=self.outputs / f"unsafe-{ordinal}.squashfs",
                        receipt_path=self.receipts / f"unsafe-{ordinal}.json",
                        filesystem_uuid=self.filesystem_uuid,
                        maximum_wall_seconds=120,
                    )

    def test_spec_rejects_archive_traversal_and_duplicate_mappings_without_io(self) -> None:
        value = self.spec_value()
        value["entries"][0]["source_path"] = "/mnt/archive/HIMR/forbidden"
        with mock.patch.object(
            IMAGE, "_open_parent", side_effect=AssertionError("archive path touched")
        ):
            with self.assertRaisesRegex(IMAGE.ExecutionImageError, "archive tier"):
                IMAGE.normalize_spec(value)
        value = self.spec_value()
        value["logical_mappings"].append(dict(value["logical_mappings"][0]))
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "names must be unique"):
            IMAGE.normalize_spec(value)

    def test_spec_requires_explicit_parent_directories_and_safe_image_paths(self) -> None:
        value = self.spec_value()
        value["entries"] = [
            entry for entry in value["entries"] if entry["image_relative_path"] != "model"
        ]
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "parent directory"):
            IMAGE.normalize_spec(value)
        value = self.spec_value()
        value["entries"][0]["image_relative_path"] = "bad path"
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "relative path"):
            IMAGE.normalize_spec(value)

    def test_spec_and_receipt_reject_wheelhouse_execution_paths(self) -> None:
        value = self.spec_value()
        value["entries"][0]["image_relative_path"] = "wheelhouse"
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "wheelhouse"):
            IMAGE.normalize_spec(value)

        receipt, _, receipt_path = self.build()
        forged = json.loads(json.dumps(receipt))
        forged["source_tree"]["entries"][0]["image_relative_path"] = "wheelhouse"
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "wheelhouse"):
            IMAGE._validate_receipt_document(forged)

    def test_source_special_permission_bits_are_rejected(self) -> None:
        self.python.chmod(0o4700)
        value = self.spec_value()
        spec_path, spec_sha = self.write_spec(value, name="set-id-source.json")
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "set-id or sticky"):
            IMAGE.build_execution_image(
                spec_path=spec_path,
                expected_spec_sha256=spec_sha,
                image_path=self.outputs / "set-id.squashfs",
                receipt_path=self.receipts / "set-id.json",
                filesystem_uuid=self.filesystem_uuid,
                maximum_wall_seconds=120,
            )

    def test_output_and_receipt_are_no_replace(self) -> None:
        value = self.spec_value()
        spec_path, spec_sha = self.write_spec(value)
        image_path = self.outputs / "existing.squashfs"
        image_path.write_bytes(b"existing")
        image_path.chmod(0o400)
        with self.assertRaisesRegex(IMAGE.ExecutionImageError, "already exists"):
            IMAGE.build_execution_image(
                spec_path=spec_path,
                expected_spec_sha256=spec_sha,
                image_path=image_path,
                receipt_path=self.receipts / "new.json",
                filesystem_uuid=self.filesystem_uuid,
                maximum_wall_seconds=120,
            )

    def test_contract_and_public_loader_fields_are_stable(self) -> None:
        contract = IMAGE.contract_document()
        self.assertEqual(contract["descriptor"]["maximum_entries"], 50_000)
        self.assertEqual(contract["descriptor"]["processors"], 4)
        self.assertIn("image", IMAGE.RECEIPT_FIELDS)
        self.assertEqual(
            contract["identity_sha256"],
            IMAGE.sha256_bytes(IMAGE.canonical_bytes(contract["descriptor"])),
        )


if __name__ == "__main__":
    unittest.main()
