from __future__ import annotations

import ast
import copy
import hashlib
import json
import os
import shutil
import stat
import sys
import unittest
from pathlib import Path

from jsonschema.validators import Draft202012Validator


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / f"asr-whispercpp-batch-archive-{os.getpid()}"
sys.path.insert(0, str(PIPELINE_ROOT))

import asr_whispercpp_batch_archive as archive  # noqa: E402


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def remove_tree() -> None:
    if not TEST_ROOT.exists():
        return
    for current, directories, files in os.walk(TEST_ROOT):
        current_path = Path(current)
        if not current_path.is_symlink():
            current_path.chmod(0o700)
        for name in directories:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o700)
        for name in files:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o600)
    shutil.rmtree(TEST_ROOT)


def immutable_stat(path: Path) -> tuple[int, int, int, int, int, int, str | None]:
    value = path.lstat()
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IMODE(value.st_mode),
        digest(path) if path.is_file() else None,
    )


class ArchiveDispositionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.receipt = REPOSITORY_ROOT / archive.RECEIPT_RELATIVE_PATH
        cls.schema = json.loads(
            (PIPELINE_ROOT / "schemas/asr-whispercpp-batch-disposition.schema.json").read_text(
                encoding="utf-8"
            )
        )

    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)

    def tearDown(self) -> None:
        remove_tree()

    def test_exact_receipt_satisfies_the_strict_schema(self) -> None:
        Draft202012Validator.check_schema(self.schema)
        Draft202012Validator(self.schema).validate(archive.EXPECTED_RECEIPT)

        unknown = copy.deepcopy(archive.EXPECTED_RECEIPT)
        unknown["unexpected"] = True
        self.assertTrue(list(Draft202012Validator(self.schema).iter_errors(unknown)))

    def test_schema_rejects_cross_paired_legacy_adapter_identity(self) -> None:
        crossed = copy.deepcopy(archive.EXPECTED_RECEIPT)
        crossed["subject"]["asr_adapter"]["implementation_version"] = "0.2.3"
        crossed["subject"]["asr_adapter"]["sha256"] = archive.ARCHIVES["successor"][
            "asr_adapter"
        ]["sha256"]
        crossed["subject"]["asr_adapter"]["byte_count"] = 57_311
        errors = list(Draft202012Validator(self.schema).iter_errors(crossed))
        self.assertGreaterEqual(len(errors), 1)

        mutated = copy.deepcopy(archive.EXPECTED_RECEIPT)
        mutated["successor"]["manifest_sha256"] = "0" * 64
        self.assertTrue(list(Draft202012Validator(self.schema).iter_errors(mutated)))

    def test_runtime_receipt_is_canonical_mode_0400_and_exactly_allowlisted(self) -> None:
        if not self.receipt.is_file():
            self.skipTest("private archival disposition receipt is unavailable")
        body = self.receipt.read_bytes()
        self.assertEqual(self.receipt.stat().st_mode & 0o777, 0o400)
        self.assertEqual(len(body), archive.RECEIPT_BYTE_COUNT)
        self.assertEqual(hashlib.sha256(body).hexdigest(), archive.RECEIPT_SHA256)
        self.assertEqual(body, archive.canonical_bytes(archive.EXPECTED_RECEIPT) + b"\n")

    def test_real_closed_archive_validates_without_mutating_evidence(self) -> None:
        if not self.receipt.is_file():
            self.skipTest("private archival disposition receipt is unavailable")
        watched = [self.receipt]
        for record in archive.ARCHIVES.values():
            manifest = REPOSITORY_ROOT / record["manifest_relative_path"]
            if not manifest.is_file():
                self.skipTest("private sealed archive fixtures are unavailable")
            watched.extend([manifest.parent, manifest.parent / "work-orders", manifest])
            watched.extend(sorted((manifest.parent / "work-orders").iterdir()))
        evidence = archive.EXPECTED_RECEIPT["execution_evidence"]
        for key in ("subject_job_1", "successor_job_1"):
            watched.append(REPOSITORY_ROOT / evidence[key]["result_relative_path"])
        for artifact in evidence["job_1_artifacts"].values():
            watched.append(REPOSITORY_ROOT / artifact["subject_relative_path"])
            watched.append(REPOSITORY_ROOT / artifact["successor_relative_path"])
        job_2 = evidence["successor_job_2_inverted_offsets"]
        watched.extend(
            [
                REPOSITORY_ROOT / job_2["result_relative_path"],
                REPOSITORY_ROOT / job_2["normalized_relative_path"],
            ]
        )
        before = {path: immutable_stat(path) for path in watched}

        result = archive.validate_disposition(self.receipt)

        self.assertEqual(result["status"], "valid")
        self.assertEqual(result["disposition"], "superseded_validation_only_non_executable")
        self.assertEqual(result["execution_authority"], "none")
        self.assertEqual(result["import_authority"], "none")
        self.assertEqual(result["publication_authority"], "none")
        self.assertEqual({path: immutable_stat(path) for path in watched}, before)

    def test_real_manifests_are_closed_and_only_declared_fields_differ(self) -> None:
        paths = [
            REPOSITORY_ROOT / archive.ARCHIVES[label]["manifest_relative_path"]
            for label in ("subject", "successor")
        ]
        if not all(path.is_file() for path in paths):
            self.skipTest("private sealed archive fixtures are unavailable")
        manifests = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
        for manifest, label in zip(manifests, ("subject", "successor"), strict=True):
            archive.validate_manifest(manifest, archive.ARCHIVES[label])
        self.assertEqual(
            archive._equivalence_projection(manifests[0]),
            archive._equivalence_projection(manifests[1]),
        )

        crossed = copy.deepcopy(manifests[0])
        crossed["software"]["asr_adapter"] = copy.deepcopy(
            manifests[1]["software"]["asr_adapter"]
        )
        with self.assertRaisesRegex(archive.ArchiveValidationError, "software tuple"):
            archive.validate_manifest(crossed, archive.ARCHIVES["subject"])

        reordered = copy.deepcopy(manifests[0])
        reordered["work_orders"].reverse()
        with self.assertRaisesRegex(archive.ArchiveValidationError, "identity|ordered"):
            archive.validate_manifest(reordered, archive.ARCHIVES["subject"])

    def test_retained_read_rejects_modes_hardlinks_and_symlinks(self) -> None:
        value = TEST_ROOT / "value.json"
        value.write_text("{}\n", encoding="utf-8")
        value.chmod(0o600)
        with archive.ReadOnlyAudit() as audit:
            with self.assertRaisesRegex(archive.ArchiveValidationError, "mode 0400"):
                audit.file(value, "fixture", maximum_bytes=100, exact_mode=0o400)

        value.chmod(0o400)
        hardlink = TEST_ROOT / "hardlink.json"
        os.link(value, hardlink)
        with archive.ReadOnlyAudit() as audit:
            with self.assertRaisesRegex(archive.ArchiveValidationError, "one hard link"):
                audit.file(value, "fixture", maximum_bytes=100, exact_mode=0o400)
        hardlink.unlink()

        symlink = TEST_ROOT / "symlink.json"
        symlink.symlink_to(value)
        with archive.ReadOnlyAudit() as audit:
            with self.assertRaisesRegex(archive.ArchiveValidationError, "non-symlink"):
                audit.file(symlink, "fixture", maximum_bytes=100, exact_mode=0o400)

    def test_retained_final_check_rejects_swap_and_restore(self) -> None:
        value = TEST_ROOT / "value.json"
        backup = TEST_ROOT / "backup.json"
        value.write_text("{}\n", encoding="utf-8")
        value.chmod(0o400)
        with self.assertRaisesRegex(archive.ArchiveValidationError, "changed during validation"):
            with archive.ReadOnlyAudit() as audit:
                audit.file(value, "fixture", maximum_bytes=100, exact_mode=0o400)
                value.rename(backup)
                value.write_text('{"replacement":true}\n', encoding="utf-8")
                value.unlink()
                backup.rename(value)
        self.assertEqual(value.read_text(encoding="utf-8"), "{}\n")

    def test_strict_json_rejects_duplicate_keys_invalid_utf8_and_nonfinite_numbers(self) -> None:
        with self.assertRaisesRegex(archive.ArchiveValidationError, "duplicate key"):
            archive.parse_json(b'{"a":1,"a":2}\n', "fixture")
        with self.assertRaisesRegex(archive.ArchiveValidationError, "strict UTF-8"):
            archive.parse_json(b'{"a":"\xff"}\n', "fixture")
        with self.assertRaisesRegex(archive.ArchiveValidationError, "non-finite"):
            archive.parse_json(b'{"a":NaN}\n', "fixture")

    def test_parser_and_imports_expose_no_execution_import_publication_or_io_lane(self) -> None:
        parser = archive.build_parser()
        subparsers = next(
            action
            for action in parser._actions
            if isinstance(action, __import__("argparse")._SubParsersAction)
        )
        self.assertEqual(set(subparsers.choices), {"validate"})
        source = (PIPELINE_ROOT / "asr_whispercpp_batch_archive.py").read_text(encoding="utf-8")
        imported: set[str] = set()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertTrue(
            {"sqlite3", "subprocess", "socket", "urllib", "http", "requests"}.isdisjoint(imported)
        )


if __name__ == "__main__":
    unittest.main()
