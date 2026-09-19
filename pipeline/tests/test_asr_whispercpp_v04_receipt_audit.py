from __future__ import annotations

import argparse
import copy
import json
import os
import stat
import subprocess
import sys
import types
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"

sys.path.insert(0, str(PIPELINE_ROOT))

import asr_whispercpp_result_store_seal as legacy_seal  # noqa: E402
import asr_whispercpp_v04_receipt_audit as audit  # noqa: E402
from pipeline.tests import test_asr_whispercpp_result_store_seal as legacy_tests  # noqa: E402


TEST_ROOT = (
    PIPELINE_ROOT
    / ".test-work"
    / f"asr-v04-receipt-compatibility-{os.getpid()}"
)


class V04ReceiptCompatibilityAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_legacy_root = legacy_tests.TEST_ROOT
        legacy_tests.TEST_ROOT = TEST_ROOT
        legacy_tests.remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)

    def tearDown(self) -> None:
        legacy_tests.remove_tree()
        legacy_tests.TEST_ROOT = self.original_legacy_root

    def _allowed_fixture(self) -> legacy_tests.SyntheticStore:
        fixture = legacy_tests.SyntheticStore()
        manifest = json.loads(fixture.queue_manifest.read_text(encoding="utf-8"))
        manifest["implementation_version"] = "0.2.0"
        manifest["materializer"] = "himr-preprocess-asr-queue"
        fixture.queue_manifest.chmod(0o600)
        legacy_tests.write_json(fixture.queue_manifest, manifest, 0o400)
        fixture._queue_manifest_body = fixture.queue_manifest.read_bytes()
        return fixture

    @staticmethod
    def _drift_plan(plan: dict[str, object]) -> None:
        for result in plan["results"]:
            result["directory"]["device"] += 10_000
            result["directory"]["inode"] += 10_000
            result["directory"]["ctime_ns_before"] = 0
            result["directory"]["mtime_ns"] = 0
            for item in result["files"]:
                item["device"] += 10_000
                item["inode"] += 10_000
                item["ctime_ns_before"] = 0
                item["mtime_ns"] = 0

    @staticmethod
    def _drift_receipt(receipt: dict[str, object]) -> None:
        for result in receipt["results"]:
            result["directory"]["device"] += 20_000
            result["directory"]["inode"] += 20_000
            result["directory"]["ctime_ns_after"] = 0
            result["directory"]["mtime_ns"] = 0
            for item in result["files"]:
                item["device"] += 20_000
                item["inode"] += 20_000
                item["ctime_ns_after"] = 0
                item["mtime_ns"] = 0

    def _drifted_receipt(
        self, fixture: legacy_tests.SyntheticStore
    ) -> Path:
        original_plan_path = fixture.queue_only_plan()
        original_receipt_path = legacy_seal.apply_plan(
            original_plan_path,
            validator=fixture.validator,
            queue_validator=fixture.queue_validator,
        )

        plan = json.loads(original_plan_path.read_text(encoding="utf-8"))
        self._drift_plan(plan)
        drifted_plan_path = legacy_tests.repin_plan(fixture, plan)
        drifted_plan = json.loads(drifted_plan_path.read_text(encoding="utf-8"))
        drifted_plan_body = drifted_plan_path.read_bytes()

        receipt = json.loads(original_receipt_path.read_text(encoding="utf-8"))
        receipt["plan"] = {
            "byte_count": len(drifted_plan_body),
            "identity_sha256": drifted_plan["identity_sha256"],
            "path": str(drifted_plan_path),
            "plan_id": drifted_plan["plan_id"],
            "sha256": legacy_seal.sha256_bytes(drifted_plan_body),
        }
        receipt["receipt_id"] = (
            f"asrsealreceipt_{drifted_plan['identity_sha256'][:32]}"
        )
        self._drift_receipt(receipt)
        semantic = {
            key: value
            for key, value in receipt.items()
            if key not in {"identity_sha256", "receipt_id"}
        }
        receipt["identity_sha256"] = legacy_seal._document_identity(semantic, set())
        drifted_receipt_path = Path(drifted_plan["receipt_path"])
        legacy_tests.write_json(drifted_receipt_path, receipt, 0o400)
        return drifted_receipt_path

    def test_exact_dependencies_and_cli_are_read_only_and_narrow(self) -> None:
        for path, (expected_size, expected_sha256) in audit.EXPECTED_FILES.items():
            identity = audit._file_identity(path)
            self.assertEqual(expected_size, identity["byte_count"])
            self.assertEqual(expected_sha256, identity["sha256"])
        parser = audit.build_parser()
        self.assertFalse(
            any(
                isinstance(action, argparse._SubParsersAction)
                for action in parser._actions
            )
        )
        parsed = parser.parse_args(["--receipt", str(TEST_ROOT / "receipt.json")])
        self.assertEqual(TEST_ROOT / "receipt.json", parsed.receipt)

    def test_verified_module_loader_restores_existing_private_import_name(self) -> None:
        name = "_himr_v04_receipt_audit_collision_test"
        previous = types.ModuleType(name)
        original = sys.modules.get(name)
        was_present = name in sys.modules
        sys.modules[name] = previous
        try:
            size, sha256 = audit.EXPECTED_FILES[audit.V02_QUEUE_SOURCE]
            with audit._load_verified_module(
                name, audit.V02_QUEUE_SOURCE, size, sha256
            ) as loaded:
                self.assertIs(sys.modules[name], loaded)
                self.assertIsNot(previous, loaded)
            self.assertIs(sys.modules[name], previous)
        finally:
            if was_present:
                sys.modules[name] = original
            else:
                sys.modules.pop(name, None)

    def test_verified_module_loader_restores_import_name_after_exec_failure(self) -> None:
        name = "_himr_v04_receipt_audit_failure_test"
        previous = types.ModuleType(name)
        source = TEST_ROOT / "failing-verified-module.py"
        body = b'raise RuntimeError("expected verified module failure")\n'
        source.write_bytes(body)
        sys.modules[name] = previous
        try:
            with self.assertRaisesRegex(RuntimeError, "expected verified module failure"):
                with audit._load_verified_module(
                    name, source, len(body), audit._sha256(body)
                ):
                    self.fail("failing verified module unexpectedly yielded")
            self.assertIs(sys.modules[name], previous)
        finally:
            sys.modules.pop(name, None)

    def test_fifo_apply_lock_is_rejected_without_blocking(self) -> None:
        fixture = self._allowed_fixture()
        receipt_path = self._drifted_receipt(fixture)
        lock_path = fixture.control / legacy_seal.APPLY_LOCK_FILENAME
        lock_path.unlink()
        os.mkfifo(lock_path, 0o600)
        command = [
            str(PIPELINE_ROOT / "bin/asr-whispercpp-v04-receipt-audit"),
            "--receipt",
            str(receipt_path),
        ]
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=5,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertEqual(2, completed.returncode)
        self.assertIn(b"must remain one retained mode-0600 file", completed.stderr)

    def test_drifted_v04_receipt_passes_portable_audit_but_not_legacy_validator(
        self,
    ) -> None:
        fixture = self._allowed_fixture()
        receipt_path = self._drifted_receipt(fixture)
        with self.assertRaisesRegex(
            legacy_seal.SealError, "device differs|inode differs|mtime differs"
        ):
            legacy_seal.validate_receipt(
                receipt_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )
        result = audit.validate_receipt(
            receipt_path,
            validator=fixture.validator,
            queue_validator=fixture.queue_validator,
        )
        self.assertEqual("validated", result["status"])
        self.assertEqual(1, result["result_count"])
        self.assertFalse(result["authority"]["filesystem_writes"])
        self.assertEqual(
            list(audit.DIAGNOSTIC_FIELDS), result["cross_restart_diagnostic_fields"]
        )

    def test_stable_content_tamper_is_still_rejected(self) -> None:
        fixture = self._allowed_fixture()
        receipt_path = self._drifted_receipt(fixture)
        artifact = fixture.queue_results[0].parent / "transcript.normalized.json"
        original = artifact.read_bytes()
        artifact.chmod(0o600)
        artifact.write_bytes(b'{"tampered":true}\n')
        artifact.chmod(0o400)
        try:
            with self.assertRaisesRegex(
                audit.CompatibilityAuditError,
                "digest differs|content changed|catalog-free validation",
            ):
                audit.validate_receipt(
                    receipt_path,
                    validator=fixture.validator,
                    queue_validator=fixture.queue_validator,
                )
        finally:
            artifact.chmod(0o600)
            artifact.write_bytes(original)
            artifact.chmod(0o400)

    def test_non_v02_queue_contract_is_rejected_before_compatibility_replay(self) -> None:
        fixture = self._allowed_fixture()
        plan_path = fixture.queue_only_plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        forged = copy.deepcopy(plan)
        forged["source_authority"]["queue_contract"][
            "materializer_implementation_version"
        ] = "0.3.0"
        with self.assertRaisesRegex(
            audit.CompatibilityAuditError, "outside the exact v0.2 allowlist"
        ):
            audit._assert_allowlisted_plan(forged)


if __name__ == "__main__":
    unittest.main()
