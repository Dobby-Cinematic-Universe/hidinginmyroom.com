from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from jsonschema import FormatChecker
from jsonschema.validators import Draft202012Validator

from pipeline import preprocess_batch as batch


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-work"
SCHEMAS = {
    "work_order": PIPELINE_ROOT / "schemas" / "work-order.schema.json",
    "selection": PIPELINE_ROOT / "schemas" / "preprocess-batch-selection.schema.json",
    "manifest": PIPELINE_ROOT / "schemas" / "preprocess-batch-manifest.schema.json",
    "receipt": PIPELINE_ROOT / "schemas" / "preprocess-batch-receipt.schema.json",
    "summary": PIPELINE_ROOT / "schemas" / "preprocess-batch-run-summary.schema.json",
}


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def assert_schema(test: unittest.TestCase, name: str, value: dict) -> None:
    schema = json.loads(SCHEMAS[name].read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value),
        key=lambda error: list(error.absolute_path),
    )
    test.assertEqual([], [error.message for error in errors])


class PreprocessBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        TEST_ROOT.chmod(0o700)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="preprocess-batch-", dir=TEST_ROOT
        )
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)

    def tearDown(self) -> None:
        for child in sorted(self.root.rglob("*"), reverse=True):
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        self.temporary.cleanup()

    def acquisition_result(self, ordinal: int, payload: bytes) -> Path:
        media = self.root / "acquired-media" / f"object-{ordinal}.bin"
        media.parent.mkdir(mode=0o700, exist_ok=True)
        media.write_bytes(payload)
        media_sha256 = digest(media)
        media_id = f"media_sha256_{media_sha256}"
        work_order_sha256 = f"{ordinal:064x}"
        job_id = f"acquisition-fixture-{ordinal:03d}"
        result_path = (
            self.root
            / "acquisition-results"
            / job_id
            / work_order_sha256
            / "result.json"
        )
        result_path.parent.mkdir(parents=True, mode=0o700)
        observed_at = f"2026-08-27T00:00:{ordinal:02d}Z"
        probe = {"format": {"duration_ms": 1_000 + ordinal}}
        result = {
            "schema_version": 1,
            "job_id": job_id,
            "adapter": "local_file",
            "status": "completed",
            "dry_run": False,
            "reused": False,
            "work_order_sha256": work_order_sha256,
            "started_at": "2026-08-27T00:00:00Z",
            "completed_at": observed_at,
            "duration_ms": ordinal,
            "source": {},
            "limits": {},
            "capacity_before": {},
            "capacity_after": {},
            "commands": [],
            "source_observation": {},
            "selected_remote_metadata": {},
            "admission": {
                "media_id": media_id,
                "sha256": media_sha256,
                "byte_count": media.stat().st_size,
                "path": str(media.resolve()),
                "storage_uri": media.resolve().as_uri(),
                "normalized_probe": probe,
            },
            "catalog_records": {
                "sources": [{}],
                "media_objects": [
                    {
                        "media_id": media_id,
                        "sha256": media_sha256,
                        "byte_count": media.stat().st_size,
                        "media_kind": "video",
                        "mime_type": "video/mp4",
                        "container": "mov,mp4",
                        "duration_ms": 1_000 + ordinal,
                        "ffprobe_json": probe,
                        "first_cataloged_at": observed_at,
                        "integrity_state": "verified",
                    }
                ],
                "media_locations": [
                    {
                        "media_id": media_id,
                        "storage_uri": media.resolve().as_uri(),
                        "is_primary": 1,
                    }
                ],
                "media_sources": [{}],
            },
            "result_path": str(result_path.resolve()),
            "errors": [],
        }
        result_path.write_bytes(batch.pretty_bytes(result))
        return result_path

    def sealed_private_acquisition(self, ordinal: int = 91):
        artifact_root = self.root / f"private-acquisition-{ordinal}"
        artifact_root.mkdir(mode=0o700)
        media_sha256 = digest_bytes(f"private-media-{ordinal}".encode())
        media_id = f"media_sha256_{media_sha256}"
        media_path = (
            artifact_root
            / "cache"
            / "media"
            / "sha256"
            / media_sha256[:2]
            / media_sha256
            / "payload"
        )
        media_path.parent.mkdir(parents=True, mode=0o700)
        for parent in [
            artifact_root / "cache",
            artifact_root / "cache" / "media",
            artifact_root / "cache" / "media" / "sha256",
            artifact_root / "cache" / "media" / "sha256" / media_sha256[:2],
            media_path.parent,
        ]:
            parent.chmod(0o700)
        payload = f"private-media-{ordinal}".encode()
        media_path.write_bytes(payload)
        media_path.chmod(0o600)
        policy = {
            "storage_scope": "private_canonical_cache",
            "publication_disposition": "no_publication_authority",
            "publication_authority": "none",
            "basis": "Fixture possession grants no publication authority.",
        }
        source = {
            "platform": "local",
            "source_kind": "livestream_capture",
            "native_id": f"private-fixture-{ordinal}",
            "canonical_url": None,
            "title": "Private fixture",
            "published_at": None,
            "access_state": "unknown",
        }
        job_id = f"private-acquisition-fixture-{ordinal}"
        work_order = {
            "schema_version": 1,
            "job_id": job_id,
            "adapter": "local_file",
            "source": source,
            "handling_policy": policy,
        }
        work_order_sha256 = digest_bytes(
            batch.private_acquisition.canonical_json(work_order).encode("utf-8")
        )
        work_path = artifact_root / "work-orders" / f"{ordinal}.json"
        work_path.parent.mkdir(mode=0o700)
        work_path.write_bytes(batch.pretty_bytes(work_order))
        work_path.chmod(0o600)
        result_path = (
            artifact_root
            / "cache"
            / "jobs"
            / job_id
            / work_order_sha256
            / "result.json"
        )
        result_path.parent.mkdir(parents=True, mode=0o700)
        for parent in [
            artifact_root / "cache" / "jobs",
            artifact_root / "cache" / "jobs" / job_id,
            result_path.parent,
        ]:
            parent.chmod(0o700)
        observed_at = "2026-08-27T12:00:00Z"
        probe = {"format": {"duration_ms": 1_000}}
        source_id = f"source_{ordinal:032x}"
        result = {
            "schema_version": 1,
            "job_id": job_id,
            "adapter": "local_file",
            "status": "completed",
            "dry_run": False,
            "reused": False,
            "work_order_sha256": work_order_sha256,
            "started_at": observed_at,
            "completed_at": observed_at,
            "duration_ms": 1,
            "source": source,
            "limits": {},
            "capacity_before": {},
            "capacity_after": {},
            "commands": [],
            "source_observation": {},
            "selected_remote_metadata": {},
            "handling_policy": policy,
            "admission": {
                "media_id": media_id,
                "sha256": media_sha256,
                "byte_count": len(payload),
                "path": str(media_path.resolve()),
                "storage_uri": media_path.resolve().as_uri(),
                "normalized_probe": probe,
            },
            "catalog_records": {
                "sources": [
                    {
                        "source_id": source_id,
                        **source,
                        "metadata_json": {
                            "acquisition_adapter": "local_file",
                            "selected_remote_metadata": {},
                            "handling_policy": policy,
                        },
                    }
                ],
                "media_objects": [
                    {
                        "media_id": media_id,
                        "sha256": media_sha256,
                        "byte_count": len(payload),
                        "media_kind": "video",
                        "mime_type": "video/mp4",
                        "container": "mov,mp4",
                        "duration_ms": 1_000,
                        "ffprobe_json": probe,
                        "first_cataloged_at": observed_at,
                        "integrity_state": "verified",
                    }
                ],
                "media_locations": [
                    {
                        "media_id": media_id,
                        "storage_uri": media_path.resolve().as_uri(),
                        "is_primary": 1,
                    }
                ],
                "media_sources": [
                    {"media_id": media_id, "source_id": source_id}
                ],
            },
            "result_path": str(result_path.resolve()),
            "errors": [],
        }
        result_path.write_bytes(batch.pretty_bytes(result))
        result_path.chmod(0o600)
        plan = batch.private_acquisition.build_private_acquisition_seal_plan(
            artifact_root,
            work_order_path=work_path.relative_to(artifact_root).as_posix(),
            result_path=result_path.relative_to(artifact_root).as_posix(),
            media_path=media_path.relative_to(artifact_root).as_posix(),
        )
        receipt = batch.private_acquisition.validate_private_acquisition_seal_plan(
            artifact_root, plan
        )
        receipt_path = artifact_root / "seals" / "receipts" / f"{ordinal}.json"
        receipt_path.parent.mkdir(parents=True, mode=0o700)
        (artifact_root / "seals").chmod(0o700)
        receipt_path.parent.chmod(0o700)
        receipt_path.write_bytes(batch.pretty_bytes(receipt))
        receipt_path.chmod(0o600)
        return artifact_root, result_path, receipt_path, policy

    def selection_and_bundle(self, count: int = 2):
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        results = [
            self.acquisition_result(index, f"media-{index}".encode())
            for index in range(1, count + 1)
        ]
        selection_path = self.root / "private-selection" / "selection.json"
        selection = batch.write_selection(results, selection_path)
        bundle = batch.materialize_bundle(
            selection_path,
            self.root / "private-bundles",
            self.root / "private-processing",
        )
        manifest, validated_selection, orders = batch.validate_bundle(bundle)
        return results, selection_path, selection, bundle, manifest, validated_selection, orders

    def rewrite_bundle_order_operations(
        self,
        bundle: Path,
        manifest: dict,
        *,
        ordinal: int,
        operations: dict[str, bool],
        materializer_version: str | None = None,
    ) -> None:
        descriptor = manifest["work_orders"][ordinal - 1]
        order_path = bundle / descriptor["path"]
        order = json.loads(order_path.read_text(encoding="utf-8"))
        order["operations"] = dict(operations)
        order_body = batch.pretty_bytes(order)
        order_path.chmod(0o600)
        order_path.write_bytes(order_body)
        order_path.chmod(0o400)
        descriptor["sha256"] = digest_bytes(order_body)
        descriptor["byte_count"] = len(order_body)
        if materializer_version is not None:
            manifest["materializer"]["version"] = materializer_version
        manifest_path = bundle / "manifest.json"
        manifest_path.chmod(0o600)
        manifest_path.write_bytes(batch.pretty_bytes(manifest))
        manifest_path.chmod(0o400)

    def test_selection_is_deterministic_sealed_and_revalidates_exact_inputs(self) -> None:
        first = self.acquisition_result(1, b"first-media")
        second = self.acquisition_result(2, b"second-media")
        path_a = self.root / "selection-a" / "selection.json"
        path_b = self.root / "selection-b" / "selection.json"
        value_a = batch.write_selection([first, second], path_a)
        value_b = batch.write_selection([second, first], path_b)
        self.assertEqual(value_a, value_b)
        self.assertEqual(path_a.read_bytes(), path_b.read_bytes())
        self.assertEqual(stat.S_IMODE(path_a.stat().st_mode), 0o400)
        self.assertEqual([1, 2], [row["ordinal"] for row in value_a["entries"]])
        assert_schema(self, "selection", value_a)
        batch.revalidate_selection_entries(batch.read_selection(path_a)[0])

        acquisition_path = Path(value_a["entries"][0]["acquisition_result"]["path"])
        acquisition_body = acquisition_path.read_bytes()
        acquisition_path.write_bytes(acquisition_body + b"\n")
        with self.assertRaisesRegex(batch.BatchError, "SHA-256"):
            batch.revalidate_selection_entries(batch.read_selection(path_a)[0])
        acquisition_path.write_bytes(acquisition_body)

        media_path = Path(value_a["entries"][0]["source_media"]["path"])
        media_path.write_bytes(media_path.read_bytes() + b"tamper")
        with self.assertRaisesRegex(batch.BatchError, "byte count|SHA-256"):
            batch.revalidate_selection_entries(batch.read_selection(path_a)[0])

    def test_selection_accepts_durable_cas_admission_reuse(self) -> None:
        result_path = self.acquisition_result(3, b"content-already-in-cas")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["reused"] = True
        result_path.write_bytes(batch.pretty_bytes(result))

        selection = batch.build_selection([result_path])

        self.assertTrue(result["reused"])
        self.assertNotIn("reuse_verified_at", result)
        self.assertEqual(
            result["admission"]["sha256"],
            selection["entries"][0]["source_media"]["sha256"],
        )
        assert_schema(self, "selection", selection)

        result["reuse_verified_at"] = "2026-08-27T01:00:00Z"
        result_path.write_bytes(batch.pretty_bytes(result))
        with self.assertRaisesRegex(batch.BatchError, "keys differ from the exact contract"):
            batch.build_selection([result_path])

    def test_selection_rejects_duplicate_media_count_cap_and_public_output(self) -> None:
        first = self.acquisition_result(1, b"same-media")
        with self.assertRaisesRegex(batch.BatchError, "repeats an acquisition|repeats exact"):
            batch.build_selection([first, first])
        with self.assertRaisesRegex(batch.BatchError, "1 to 128"):
            batch.build_selection([first] * 129)
        public_output = REPOSITORY_ROOT / "public" / "forbidden-selection.json"
        with self.assertRaisesRegex(batch.BatchError, "public"):
            batch.write_selection([first], public_output)
        self.assertFalse(public_output.exists())

    def test_v30_private_seal_is_required_replayed_and_propagated(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        artifact_root, result_path, receipt_path, policy = (
            self.sealed_private_acquisition()
        )
        with self.assertRaisesRegex(batch.BatchError, "requires an exact replayed"):
            batch.build_selection([result_path])

        selection_path = self.root / "private-control" / "selection.json"
        selection = batch.write_selection(
            [result_path],
            selection_path,
            private_acquisition_root=artifact_root,
            private_seal_receipt_paths=[receipt_path],
        )
        boundary = selection["entries"][0]["handling_boundary"]
        self.assertEqual(policy, boundary["handling_policy"])
        self.assertFalse(
            boundary["private_acquisition_seal"]["source_byte_identity_claimed"]
        )
        assert_schema(self, "selection", selection)

        bundle = batch.materialize_bundle(
            selection_path,
            self.root / "private-bundles-v30",
            self.root / "private-processing-v30",
        )
        manifest, _selection, _orders = batch.validate_bundle(bundle)
        self.assertEqual(policy, manifest["handling_control"]["entries"][0]["handling_policy"])
        self.assertTrue(manifest["safety"]["handling_policy_propagation_required"])
        assert_schema(self, "manifest", manifest)

        summary = batch.run_batch(
            bundle,
            self.root / "private-state-v30",
            dry_run=True,
        )
        self.assertEqual(manifest["handling_control"], summary["handling_control"])
        self.assertTrue(summary["safety"]["private_acquisition_seal_required"])
        self.assertFalse((self.root / "private-state-v30").exists())
        assert_schema(self, "summary", summary)

        changed = json.loads(json.dumps(boundary))
        changed["handling_policy"]["basis"] = "Different policy basis."
        with self.assertRaisesRegex(batch.BatchError, "differs from exact seal replay"):
            batch.replay_handling_boundary(changed)

    def test_bundle_is_exact_immutable_and_contract_valid(self) -> None:
        (_results, selection_path, _selection, bundle, manifest, _validated, orders) = (
            self.selection_and_bundle()
        )
        replay = batch.materialize_bundle(
            selection_path,
            self.root / "private-bundles",
            self.root / "private-processing",
        )
        self.assertEqual(bundle, replay)
        self.assertEqual(stat.S_IMODE(bundle.stat().st_mode), 0o500)
        self.assertEqual(stat.S_IMODE((bundle / "manifest.json").stat().st_mode), 0o400)
        self.assertEqual(2, len(orders))
        self.assertTrue(all(order["operations"] == batch.OPERATIONS for order in orders))
        self.assertTrue(all(order["profile"]["profile_id"] == "cpu-balanced-v1" for order in orders))
        assert_schema(self, "manifest", manifest)
        for descriptor in manifest["work_orders"]:
            work_order = json.loads((bundle / descriptor["path"]).read_text())
            assert_schema(self, "work_order", work_order)
            self.assertLessEqual(len(batch.pretty_bytes(work_order)), batch.MAX_WORK_ORDER_BYTES)

        bundle.chmod(0o700)
        (bundle / "unexpected.txt").write_text("unexpected", encoding="utf-8")
        bundle.chmod(0o500)
        with self.assertRaisesRegex(batch.BatchError, "missing or extra"):
            batch.validate_bundle(bundle)

    def test_asr_ready_lane_skips_proxy_and_routing_but_remains_replayable(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        result = self.acquisition_result(1, b"asr-ready-media")
        selection_path = self.root / "asr-ready-selection" / "selection.json"
        batch.write_selection([result], selection_path)
        bundle = batch.materialize_bundle(
            selection_path,
            self.root / "asr-ready-bundles",
            self.root / "asr-ready-processing",
            operation_profile="asr-ready",
        )
        manifest, _selection, orders = batch.validate_bundle(bundle)
        self.assertEqual("0.3.0", manifest["materializer"]["version"])
        self.assertEqual([batch.ASR_READY_OPERATIONS], [row["operations"] for row in orders])
        self.assertTrue(orders[0]["operations"]["audio_flac"])
        self.assertFalse(orders[0]["operations"]["proxy"])
        self.assertFalse(orders[0]["operations"]["routing"])
        assert_schema(self, "manifest", manifest)

        replay = batch.materialize_bundle(
            selection_path,
            self.root / "asr-ready-bundles",
            self.root / "asr-ready-processing",
            operation_profile="asr-ready",
        )
        self.assertEqual(bundle, replay)

    def test_enrichment_only_lane_disables_audio_without_changing_default(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        result = self.acquisition_result(1, b"enrichment-only-media")
        selection_path = self.root / "enrichment-selection" / "selection.json"
        batch.write_selection([result], selection_path)
        bundle = batch.materialize_bundle(
            selection_path,
            self.root / "enrichment-bundles",
            self.root / "enrichment-processing",
            operation_profile="enrichment-only",
        )
        manifest, _selection, orders = batch.validate_bundle(bundle)
        self.assertEqual(
            [batch.ENRICHMENT_ONLY_OPERATIONS],
            [row["operations"] for row in orders],
        )
        self.assertFalse(orders[0]["operations"]["audio_flac"])
        self.assertTrue(orders[0]["operations"]["proxy"])
        self.assertTrue(orders[0]["operations"]["routing"])
        self.assertEqual("0.3.0", manifest["materializer"]["version"])

    def test_bundle_rejects_mixed_lanes_and_non_full_legacy_lane(self) -> None:
        (_results, _selection_path, _selection, bundle, manifest, _validated, _orders) = (
            self.selection_and_bundle(count=2)
        )
        self.rewrite_bundle_order_operations(
            bundle,
            manifest,
            ordinal=2,
            operations=batch.ASR_READY_OPERATIONS,
        )
        with self.assertRaisesRegex(batch.BatchError, "mixes preprocessing"):
            batch.validate_bundle(bundle)

        self.rewrite_bundle_order_operations(
            bundle,
            manifest,
            ordinal=1,
            operations=batch.ENRICHMENT_ONLY_OPERATIONS,
            materializer_version="0.2.0",
        )
        with self.assertRaisesRegex(batch.BatchError, "legacy.*full"):
            batch.validate_bundle(bundle)

    def test_preprocess_batch_rejects_unknown_operation_profile(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        result = self.acquisition_result(1, b"operation-profile-media")
        selection_path = self.root / "operation-profile-selection" / "selection.json"
        selection = batch.write_selection([result], selection_path)
        with self.assertRaisesRegex(batch.BatchError, "operation_profile"):
            batch.build_bundle(
                selection,
                selection_path.resolve(),
                selection_path.read_bytes(),
                self.root / "operation-profile-processing",
                operation_profile="unknown",
            )

    def test_bundle_symlink_cannot_validate_or_satisfy_exact_admission(self) -> None:
        (_results, selection_path, _selection, bundle, manifest, _validated, _orders) = (
            self.selection_and_bundle(count=1)
        )
        target_parent = self.root / "alternate-private-bundles"
        target_parent.mkdir(mode=0o700)
        target = target_parent / manifest["bundle_id"]
        bundle.chmod(0o700)
        bundle.rename(target)
        target.chmod(0o500)
        bundle.symlink_to(target, target_is_directory=True)
        target_manifest_sha256 = digest(target / "manifest.json")
        exact_order_files = [
            (row["path"], (target / row["path"]).read_bytes())
            for row in manifest["work_orders"]
        ]

        with self.assertRaisesRegex(batch.BatchError, "non-symlink|symlink"):
            batch.validate_bundle(bundle)
        with self.assertRaisesRegex(batch.BatchError, "outside"):
            batch.verify_existing_bundle(
                target,
                manifest,
                exact_order_files,
                expected_admission_parent=bundle.parent,
            )
        with self.assertRaisesRegex(batch.BatchError, "symlink"):
            batch.materialize_bundle(
                selection_path,
                self.root / "private-bundles",
                self.root / "private-processing",
            )
        self.assertEqual(target_manifest_sha256, digest(target / "manifest.json"))
        self.assertEqual(0o500, stat.S_IMODE(target.stat().st_mode))

    def test_existing_processing_root_requires_owner_only_mode_0700(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        result = self.acquisition_result(1, b"processing-root-mode")
        selection_path = self.root / "private-selection" / "selection.json"
        batch.write_selection([result], selection_path)
        processing_root = self.root / "private-processing"
        processing_root.mkdir(mode=0o755)
        processing_root.chmod(0o755)
        bundle_root = self.root / "private-bundles"

        with self.assertRaisesRegex(batch.BatchError, "0700"):
            batch.materialize_bundle(selection_path, bundle_root, processing_root)
        self.assertFalse(bundle_root.exists())

        processing_root.chmod(0o700)
        bundle = batch.materialize_bundle(
            selection_path, bundle_root, processing_root
        )
        batch.validate_bundle(bundle)
        processing_root.chmod(0o755)
        with self.assertRaisesRegex(batch.BatchError, "0700"):
            batch.validate_bundle(bundle)
        processing_root.chmod(0o700)

    def fake_runner_functions(self):
        calls: list[str] = []

        def fake_context(order, entry, manifest):
            return {"recipe_dir": Path(manifest["processing_output_root"]) / "recipe"}

        def fake_run(order, dry_run):
            self.assertFalse(dry_run)
            calls.append(order["job_id"])
            run_dir = Path(order["output"]["root"]) / "fake-runs" / order["job_id"]
            run_dir.mkdir(parents=True, mode=0o700)
            for parent in [run_dir.parent, run_dir]:
                parent.chmod(0o700)
            artifact = run_dir / "artifact.bin"
            artifact.write_bytes(f"artifact:{order['job_id']}".encode())
            artifact.chmod(0o444)
            result_path = run_dir / "result.json"
            returned = {"job_id": order["job_id"], "result_path": str(result_path)}
            result_path.write_bytes(batch.pretty_bytes(returned))
            result_path.chmod(0o444)
            return returned

        def fake_validate(result_path, order, entry, manifest):
            artifact = result_path.parent / "artifact.bin"
            artifact_body = artifact.read_bytes()
            result_body = result_path.read_bytes()
            suffix = hashlib.sha256(order["job_id"].encode()).hexdigest()[:32]
            return {
                "result": {
                    "path": str(result_path),
                    "sha256": digest_bytes(result_body),
                    "byte_count": len(result_body),
                    "processing_run_id": f"run_{suffix}",
                    "recipe_sha256": "d" * 64,
                    "reuse_mode": "none",
                },
                "artifacts": [
                    {
                        "artifact_id": f"artifact_{suffix}",
                        "artifact_kind": "synthetic_test_artifact",
                        "path": str(artifact),
                        "storage_uri": artifact.as_uri(),
                        "sha256": digest_bytes(artifact_body),
                        "byte_count": len(artifact_body),
                        "media_kind": "document",
                        "mime_type": "application/octet-stream",
                    }
                ],
            }

        return calls, fake_context, fake_run, fake_validate

    def test_runner_is_private_sequential_resumable_and_exact_on_replay(self) -> None:
        (_results, _selection_path, _selection, bundle, manifest, _validated, _orders) = (
            self.selection_and_bundle()
        )
        state_root = self.root / "private-state"
        self.assertFalse(Path(manifest["processing_output_root"]).exists())
        calls, fake_context, fake_run, fake_validate = self.fake_runner_functions()
        with (
            mock.patch.object(batch, "result_validation_context", fake_context),
            mock.patch.object(batch.media_preprocess, "run_work_order", fake_run),
            mock.patch.object(batch, "validate_completed_result", fake_validate),
        ):
            dry = batch.run_batch(bundle, state_root, dry_run=True)
            self.assertEqual([], calls)
            self.assertFalse(state_root.exists())
            self.assertEqual(2, dry["pending_count"])
            assert_schema(self, "summary", dry)

            first = batch.run_batch(bundle, state_root)
            self.assertEqual(1, len(calls))
            self.assertEqual([1], first["newly_completed_ordinals"])
            self.assertEqual(1, first["completed_count"])
            second = batch.run_batch(bundle, state_root)
            self.assertEqual(2, len(calls))
            self.assertEqual([2], second["newly_completed_ordinals"])
            self.assertEqual("complete", second["status"])
            state_sha256 = second["state_sha256"]
            replay = batch.run_batch(bundle, state_root)
            self.assertEqual(2, len(calls))
            self.assertEqual([], replay["newly_completed_ordinals"])
            self.assertEqual(state_sha256, replay["state_sha256"])
            self.assertEqual(stat.S_IMODE(state_root.stat().st_mode), 0o700)
            assert_schema(self, "summary", replay)

            receipts_dir = state_root / "runs" / manifest["bundle_id"] / "receipts"
            receipts = sorted(receipts_dir.glob("*.json"))
            self.assertEqual(2, len(receipts))
            for receipt_path in receipts:
                self.assertEqual(stat.S_IMODE(receipt_path.stat().st_mode), 0o400)
                assert_schema(self, "receipt", json.loads(receipt_path.read_text()))

            artifact = Path(json.loads(receipts[0].read_text())["artifacts"][0]["path"])
            artifact.chmod(0o600)
            artifact.write_bytes(artifact.read_bytes() + b"tamper")
            artifact.chmod(0o444)
            with self.assertRaisesRegex(batch.BatchError, "receipt differs"):
                batch.batch_status(bundle, state_root)

    def test_historical_completed_bundle_replays_but_incomplete_fails_closed(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        calls, fake_context, fake_run, fake_validate = self.fake_runner_functions()
        with (
            mock.patch.object(batch, "PRODUCER_PATH", batch.LEGACY_PRODUCER_PATH),
            mock.patch.object(
                batch,
                "active_producer_module_path",
                return_value=batch.LEGACY_PRODUCER_PATH.resolve(),
            ),
        ):
            completed_result = self.acquisition_result(31, b"historical-complete")
            completed_selection = self.root / "historical-complete-selection" / "selection.json"
            batch.write_selection([completed_result], completed_selection)
            completed_bundle = batch.materialize_bundle(
                completed_selection,
                self.root / "historical-complete-bundles",
                self.root / "historical-complete-output",
            )
            completed_manifest = batch.validate_bundle(completed_bundle)[0]
            self.assertEqual(
                batch.LEGACY_PRODUCER_PATH.resolve(),
                Path(completed_manifest["producer"]["path"]),
            )
            completed_state = self.root / "historical-complete-state"
            with (
                mock.patch.object(batch, "result_validation_context", fake_context),
                mock.patch.object(batch.media_preprocess, "run_work_order", fake_run),
                mock.patch.object(batch, "validate_completed_result", fake_validate),
            ):
                self.assertEqual(
                    "complete",
                    batch.run_batch(completed_bundle, completed_state)["status"],
                )

            pending_result = self.acquisition_result(32, b"historical-pending")
            pending_selection = self.root / "historical-pending-selection" / "selection.json"
            batch.write_selection([pending_result], pending_selection)
            pending_bundle = batch.materialize_bundle(
                pending_selection,
                self.root / "historical-pending-bundles",
                self.root / "historical-pending-output",
            )

        with mock.patch.object(batch, "validate_completed_result", fake_validate):
            replay = batch.batch_status(completed_bundle, completed_state)
            self.assertEqual("complete", replay["status"])
            self.assertEqual(1, replay["completed_count"])
            self.assertEqual([], replay["pending_ordinals"])
            self.assertEqual(
                [],
                batch.run_batch(completed_bundle, completed_state)[
                    "newly_completed_ordinals"
                ],
            )
        self.assertEqual(
            batch.LEGACY_PRODUCER_PATH.resolve(),
            Path(batch.validate_bundle(completed_bundle)[0]["producer"]["path"]),
        )
        with (
            mock.patch.object(batch, "result_validation_context", fake_context),
            mock.patch.object(batch.media_preprocess, "run_work_order", fake_run),
            mock.patch.object(batch, "validate_completed_result", fake_validate),
            self.assertRaisesRegex(batch.BatchError, "historical inactive producer"),
        ):
            batch.run_batch(pending_bundle, self.root / "historical-pending-state")

    def test_active_producer_pin_must_match_imported_module(self) -> None:
        with mock.patch.object(
            batch.media_preprocess,
            "__file__",
            str(self.root / "different-producer.py"),
        ):
            with self.assertRaisesRegex(
                batch.BatchError, "module path differs from the producer pin"
            ):
                batch.producer_observation()

    def test_state_and_receipt_admission_crashes_resume_without_false_completion(self) -> None:
        (_results, _selection_path, _selection, bundle, manifest, _validated, _orders) = (
            self.selection_and_bundle()
        )
        state_root = self.root / "crash-state"
        runs = state_root / "runs"
        run_dir = runs / manifest["bundle_id"]
        run_dir.mkdir(parents=True, mode=0o700)
        state_root.chmod(0o700)
        runs.chmod(0o700)
        run_dir.chmod(0o700)
        calls, fake_context, fake_run, fake_validate = self.fake_runner_functions()
        with (
            mock.patch.object(batch, "result_validation_context", fake_context),
            mock.patch.object(batch.media_preprocess, "run_work_order", fake_run),
            mock.patch.object(batch, "validate_completed_result", fake_validate),
        ):
            incomplete = batch.batch_status(bundle, state_root)
            self.assertEqual(0, incomplete["completed_count"])
            self.assertEqual(2, incomplete["pending_count"])
            self.assertFalse((run_dir / "receipts").exists())

            first = batch.run_batch(bundle, state_root)
            self.assertEqual([1], first["newly_completed_ordinals"])
            receipts_dir = run_dir / "receipts"
            first_receipt = receipts_dir / "000001.json"

            linked_temporary = receipts_dir / (
                ".000001.json.tmp-99991-" + "a" * 32
            )
            os.link(first_receipt, linked_temporary)
            orphan_temporary = receipts_dir / (
                ".000002.json.tmp-99992-" + "b" * 32
            )
            orphan_temporary.write_bytes(b"partial receipt")
            orphan_temporary.chmod(0o600)
            legacy_orphan = receipts_dir / ".000002.json.tmp-99993"
            legacy_orphan.write_bytes(b"")
            legacy_orphan.chmod(0o600)

            crash_status = batch.batch_status(bundle, state_root)
            self.assertEqual([1], crash_status["completed_ordinals"])
            self.assertEqual([2], crash_status["pending_ordinals"])
            self.assertTrue(linked_temporary.exists())
            self.assertTrue(orphan_temporary.exists())

            completed = batch.run_batch(bundle, state_root)
            self.assertEqual([2], completed["newly_completed_ordinals"])
            self.assertEqual(2, len(calls))
            self.assertFalse(linked_temporary.exists())
            self.assertFalse(orphan_temporary.exists())
            self.assertFalse(legacy_orphan.exists())
            self.assertEqual(1, first_receipt.stat().st_nlink)

            unrelated = receipts_dir / "operator-notes.txt"
            unrelated.write_text("not a receipt", encoding="utf-8")
            unrelated.chmod(0o600)
            with self.assertRaisesRegex(batch.BatchError, "extra entry"):
                batch.batch_status(bundle, state_root)
            unrelated.unlink()

            unsafe_temporary = receipts_dir / (
                ".000001.json.tmp-99994-" + "c" * 32
            )
            unsafe_temporary.symlink_to(first_receipt)
            with self.assertRaisesRegex(batch.BatchError, "unsafe receipt temporary"):
                batch.batch_status(bundle, state_root)

            bad_state = self.root / "bad-incomplete-state"
            bad_run = bad_state / "runs" / manifest["bundle_id"]
            bad_run.mkdir(parents=True, mode=0o700)
            bad_state.chmod(0o700)
            bad_run.parent.chmod(0o700)
            bad_run.chmod(0o700)
            (bad_run / "unexpected.txt").write_text("unexpected", encoding="utf-8")
            with self.assertRaisesRegex(batch.BatchError, "unexpected entries"):
                batch.batch_status(bundle, bad_state)

    def test_private_root_and_child_command_guards_fail_closed(self) -> None:
        relative = Path("relative-private-root")
        with self.assertRaisesRegex(batch.BatchError, "absolute"):
            batch.ensure_private_directory(relative, "test root")

        non_private = self.root / "non-private"
        non_private.mkdir(mode=0o755)
        non_private.chmod(0o755)
        with self.assertRaisesRegex(batch.BatchError, "0700"):
            batch.ensure_private_directory(non_private, "test root")

        created = self.root / "new" / "nested" / "private"
        batch.ensure_private_directory(created, "test root")
        self.assertEqual(0o700, stat.S_IMODE(created.stat().st_mode))
        self.assertEqual(0o700, stat.S_IMODE(created.parent.stat().st_mode))

        reused_artifact = created / "reused.json"
        reused_artifact.write_text("{}\n", encoding="utf-8")
        reused_artifact.chmod(0o444)
        reused_link = created / "reused-link.json"
        os.link(reused_artifact, reused_link)
        with self.assertRaisesRegex(batch.BatchError, "single-link"):
            batch.readonly_file(reused_artifact, 64, "sealed JSON")
        self.assertEqual(
            b"{}\n",
            batch.readonly_file(
                reused_artifact, 64, "reused producer JSON", allow_hardlinks=True
            )[1],
        )

        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("FFmpeg and FFprobe are required")
        tools = batch.tool_observations()
        manifest = {"tools": tools}
        original_environment = batch.media_preprocess.subprocess_environment
        with batch.guarded_preprocessor_environment(manifest):
            self.assertEqual(
                {"LC_ALL": "C", "LANG": "C", "TZ": "UTC"},
                batch.media_preprocess.subprocess_environment(),
            )
            with self.assertRaisesRegex(
                batch.media_preprocess.PipelineError, "non-local"
            ):
                batch.media_preprocess.run_command(
                    [tools["ffprobe"]["path"], "https://example.invalid/media"]
                )
        self.assertIs(
            original_environment, batch.media_preprocess.subprocess_environment
        )

    def test_guarded_policies_overlap_without_cross_thread_state_or_wrappers(
        self,
    ) -> None:
        """Each concurrent lane sees only its own command policy."""

        def manifest(tool: str) -> dict:
            return {
                "tools": {
                    "ffmpeg": {"path": tool},
                    "ffprobe": {"path": tool},
                }
            }

        overlap = threading.Barrier(2)
        finish = threading.Barrier(2)
        failures: list[BaseException] = []
        results: list[tuple[str, tuple[str, ...]]] = []
        original_command = batch.media_preprocess.run_command
        original_environment = batch.media_preprocess.subprocess_environment
        dispatcher = batch._PREPROCESSOR_ENVIRONMENT_DISPATCHER
        self.assertIs(dispatcher, batch._install_preprocessor_environment_dispatcher())
        self.assertIs(original_command, batch.media_preprocess.run_command)
        self.assertIs(
            original_environment, batch.media_preprocess.subprocess_environment
        )

        def lane(name: str, own_tool: str, other_tool: str) -> None:
            try:
                with batch.guarded_preprocessor_environment(manifest(own_tool)):
                    overlap.wait(timeout=2)
                    observed = batch.media_preprocess.subprocess_environment()
                    if observed != {"LC_ALL": "C", "LANG": "C", "TZ": "UTC"}:
                        raise AssertionError("lane did not receive restricted environment")
                    returned = batch.media_preprocess.run_command([own_tool, "local"])
                    results.append((name, returned))
                    try:
                        batch.media_preprocess.run_command([other_tool, "local"])
                    except batch.media_preprocess.PipelineError:
                        pass
                    else:
                        raise AssertionError("lane admitted the peer command policy")
                    finish.wait(timeout=2)
                if batch.media_preprocess.subprocess_environment() != {"BASE": "1"}:
                    raise AssertionError("lane retained environment policy after exit")
            except BaseException as error:  # pragma: no cover - thread relay
                failures.append(error)

        def fake_command(command: list[str]) -> tuple[str, ...]:
            return tuple(command)

        with (
            mock.patch.object(dispatcher, "_base_command", side_effect=fake_command),
            mock.patch.object(
                dispatcher, "_base_environment", return_value={"BASE": "1"}
            ),
        ):
            first = threading.Thread(
                target=lane, args=("first", "/fixture/tool-a", "/fixture/tool-b")
            )
            second = threading.Thread(
                target=lane, args=("second", "/fixture/tool-b", "/fixture/tool-a")
            )
            first.start()
            second.start()
            first.join(timeout=3)
            second.join(timeout=3)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(
            {
                ("first", ("/fixture/tool-a", "local")),
                ("second", ("/fixture/tool-b", "local")),
            },
            set(results),
        )
        self.assertIs(original_command, batch.media_preprocess.run_command)
        self.assertIs(
            original_environment, batch.media_preprocess.subprocess_environment
        )

    def test_tool_provenance_interruption_is_not_reported_as_build_drift(
        self,
    ) -> None:
        tools = {
            name: {"name": name, "path": f"/fixture/{name}"}
            for name in ("ffmpeg", "ffprobe")
        }

        def interrupted(row: dict[str, object]) -> None:
            if row["name"] == "ffprobe":
                raise batch.media_preprocess.CommandError(
                    ["/fixture/ffprobe", "-version"], -15, ""
                )

        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: f"/fixture/{name}",
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
                side_effect=interrupted,
            ),
            self.assertRaisesRegex(
                batch.BatchError,
                r"current ffprobe provenance check was interrupted by signal 15",
            ) as raised,
        ):
            batch.verify_tools(tools)
        self.assertNotIn("build differs", str(raised.exception))

    def test_tool_provenance_mismatch_keeps_build_drift_classification(
        self,
    ) -> None:
        tools = {
            name: {"name": name, "path": f"/fixture/{name}"}
            for name in ("ffmpeg", "ffprobe")
        }
        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: f"/fixture/{name}",
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
                side_effect=batch.media_preprocess.PipelineError("fixture mismatch"),
            ),
            self.assertRaisesRegex(
                batch.BatchError,
                r"current ffmpeg build differs from the bundle pin",
            ),
        ):
            batch.verify_tools(tools)

    def fixture_tool_observations(self) -> dict[str, dict[str, object]]:
        tools: dict[str, dict[str, object]] = {}
        for name in ("ffmpeg", "ffprobe"):
            path = self.root / name
            body = f"fixture executable bytes for {name}".encode("utf-8")
            path.write_bytes(body)
            version_output = f"{name} fixture version\nconfiguration: fixture"
            tools[name] = {
                "name": name,
                "path": str(path.resolve()),
                "executable_sha256": digest_bytes(body),
                "executable_byte_count": len(body),
                "version": f"{name} fixture version",
                "version_output": version_output,
                "version_output_sha256": digest_bytes(version_output.encode("utf-8")),
                "build_configuration": "fixture",
            }
        return tools

    def test_restore_tool_witness_caches_only_version_output_work(self) -> None:
        tools = self.fixture_tool_observations()
        observed_full_checks: list[str] = []

        def full_check(row: dict[str, object]) -> None:
            observed_full_checks.append(str(row["name"]))

        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: tools[name]["path"],
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
                side_effect=full_check,
            ),
            mock.patch.object(
                batch,
                "_verify_tool_executable_bytes",
                wraps=batch._verify_tool_executable_bytes,
            ) as byte_checks,
        ):
            with batch.restore_scoped_tool_provenance_witness():
                batch.verify_tools(tools)
                batch.verify_tools(tools)

        self.assertEqual(
            ["ffmpeg", "ffprobe", "ffmpeg", "ffprobe"],
            observed_full_checks,
        )
        self.assertEqual(2, byte_checks.call_count)
        self.assertIsNone(batch._RESTORE_TOOL_PROVENANCE_WITNESS.get())

    def test_restore_tool_witness_hashes_executable_on_every_hit(self) -> None:
        tools = self.fixture_tool_observations()
        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: tools[name]["path"],
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
            ),
        ):
            with batch.restore_scoped_tool_provenance_witness():
                batch.verify_tools(tools)
                path = Path(str(tools["ffmpeg"]["path"]))
                path.write_bytes(path.read_bytes()[::-1])
                with self.assertRaisesRegex(
                    batch.BatchError,
                    r"ffmpeg executable SHA-256 differs from the bundle pin",
                ):
                    batch.verify_tools(tools)
        self.assertIsNone(batch._RESTORE_TOOL_PROVENANCE_WITNESS.get())

    def test_restore_tool_witness_rejects_divergent_bundle_pin(self) -> None:
        tools = self.fixture_tool_observations()
        divergent = json.loads(json.dumps(tools))
        divergent["ffprobe"]["build_configuration"] = "different"
        full_checks: list[str] = []
        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: tools[name]["path"],
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
                side_effect=lambda row: full_checks.append(row["name"]),
            ),
        ):
            with batch.restore_scoped_tool_provenance_witness():
                batch.verify_tools(tools)
                with self.assertRaisesRegex(
                    batch.BatchError,
                    r"ffprobe provenance differs from the active restore witness",
                ):
                    batch.verify_tools(divergent)
        self.assertEqual(4, len(full_checks))

    def test_restore_tool_witness_exit_rechecks_full_provenance(self) -> None:
        tools = self.fixture_tool_observations()

        def full_check(row: dict[str, object]) -> None:
            if digest(Path(str(row["path"]))) != row["executable_sha256"]:
                raise batch.media_preprocess.PipelineError("fixture drift")

        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: tools[name]["path"],
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
                side_effect=full_check,
            ),
            self.assertRaisesRegex(
                batch.BatchError,
                r"current ffmpeg build differs from the bundle pin",
            ),
        ):
            with batch.restore_scoped_tool_provenance_witness():
                batch.verify_tools(tools)
                path = Path(str(tools["ffmpeg"]["path"]))
                path.write_bytes(path.read_bytes()[::-1])
        self.assertIsNone(batch._RESTORE_TOOL_PROVENANCE_WITNESS.get())

    def test_restore_tool_witness_is_context_local_across_threads(self) -> None:
        tools = self.fixture_tool_observations()
        overlap = threading.Barrier(2)
        failures: list[BaseException] = []

        def lane() -> None:
            try:
                with batch.restore_scoped_tool_provenance_witness():
                    overlap.wait(timeout=2)
                    batch.verify_tools(tools)
                    batch.verify_tools(tools)
            except BaseException as error:  # pragma: no cover - thread relay
                failures.append(error)

        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: tools[name]["path"],
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
            ) as full_checks,
        ):
            first = threading.Thread(target=lane)
            second = threading.Thread(target=lane)
            first.start()
            second.start()
            first.join(timeout=4)
            second.join(timeout=4)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(8, full_checks.call_count)
        self.assertIsNone(batch._RESTORE_TOOL_PROVENANCE_WITNESS.get())

    def test_restore_tool_witness_preserves_signal_classification_and_cleans_up(
        self,
    ) -> None:
        tools = self.fixture_tool_observations()

        def interrupted(row: dict[str, object]) -> None:
            if row["name"] == "ffprobe":
                raise batch.media_preprocess.CommandError(
                    [str(row["path"]), "-version"], -15, ""
                )

        with (
            mock.patch.object(
                batch.media_preprocess,
                "require_tool",
                side_effect=lambda name: tools[name]["path"],
            ),
            mock.patch.object(
                batch.media_preprocess,
                "verify_executable_provenance",
                side_effect=interrupted,
            ),
            self.assertRaisesRegex(
                batch.BatchError,
                r"current ffprobe provenance check was interrupted by signal 15",
            ),
        ):
            with batch.restore_scoped_tool_provenance_witness():
                batch.verify_tools(tools)
        self.assertIsNone(batch._RESTORE_TOOL_PROVENANCE_WITNESS.get())

    def test_credential_and_execution_guards_overlap_with_thread_local_policy(
        self,
    ) -> None:
        """A peer execution policy cannot restrict a credential-only context."""

        manifest = {
            "tools": {
                "ffmpeg": {"path": "/fixture/guarded-tool"},
                "ffprobe": {"path": "/fixture/guarded-tool"},
            }
        }
        overlap = threading.Barrier(2)
        finish = threading.Barrier(2)
        failures: list[BaseException] = []
        results: list[tuple[str, ...]] = []
        original_command = batch.media_preprocess.run_command
        original_environment = batch.media_preprocess.subprocess_environment
        dispatcher = batch._PREPROCESSOR_ENVIRONMENT_DISPATCHER

        def credential_lane() -> None:
            try:
                with batch.credential_free_tool_environment():
                    overlap.wait(timeout=2)
                    if batch.media_preprocess.subprocess_environment() != {
                        "LC_ALL": "C",
                        "LANG": "C",
                        "TZ": "UTC",
                    }:
                        raise AssertionError("credential lane environment is unrestricted")
                    results.append(
                        batch.media_preprocess.run_command(
                            ["/fixture/credential-only-tool", "local"]
                        )
                    )
                    finish.wait(timeout=2)
                if batch.media_preprocess.subprocess_environment() != {"BASE": "1"}:
                    raise AssertionError("credential lane retained policy after exit")
            except BaseException as error:  # pragma: no cover - thread relay
                failures.append(error)

        def execution_lane() -> None:
            try:
                with batch.guarded_preprocessor_environment(manifest):
                    overlap.wait(timeout=2)
                    if batch.media_preprocess.subprocess_environment() != {
                        "LC_ALL": "C",
                        "LANG": "C",
                        "TZ": "UTC",
                    }:
                        raise AssertionError("execution lane environment is unrestricted")
                    results.append(
                        batch.media_preprocess.run_command(
                            ["/fixture/guarded-tool", "local"]
                        )
                    )
                    try:
                        batch.media_preprocess.run_command(
                            ["/fixture/credential-only-tool", "local"]
                        )
                    except batch.media_preprocess.PipelineError:
                        pass
                    else:
                        raise AssertionError("execution lane admitted an unpinned tool")
                    finish.wait(timeout=2)
                if batch.media_preprocess.subprocess_environment() != {"BASE": "1"}:
                    raise AssertionError("execution lane retained policy after exit")
            except BaseException as error:  # pragma: no cover - thread relay
                failures.append(error)

        def fake_command(command: list[str]) -> tuple[str, ...]:
            return tuple(command)

        with (
            mock.patch.object(dispatcher, "_base_command", side_effect=fake_command),
            mock.patch.object(
                dispatcher, "_base_environment", return_value={"BASE": "1"}
            ),
        ):
            credential = threading.Thread(target=credential_lane)
            execution = threading.Thread(target=execution_lane)
            credential.start()
            execution.start()
            credential.join(timeout=3)
            execution.join(timeout=3)

        self.assertFalse(credential.is_alive())
        self.assertFalse(execution.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(
            {
                ("/fixture/credential-only-tool", "local"),
                ("/fixture/guarded-tool", "local"),
            },
            set(results),
        )
        self.assertIs(original_command, batch.media_preprocess.run_command)
        self.assertIs(
            original_environment, batch.media_preprocess.subprocess_environment
        )

    def test_parser_defaults_to_one_and_exposes_read_only_dry_run(self) -> None:
        args = batch.build_parser().parse_args(
            [
                "run",
                "--bundle",
                str(self.root / "bundle"),
                "--state-root",
                str(self.root / "state"),
                "--dry-run",
            ]
        )
        self.assertEqual(1, args.limit)
        self.assertTrue(args.dry_run)

        default_lane = batch.build_parser().parse_args(
            [
                "materialize",
                "--selection",
                str(self.root / "selection.json"),
                "--bundle-root",
                str(self.root / "bundles"),
                "--processing-output-root",
                str(self.root / "processing"),
            ]
        )
        self.assertEqual("full", default_lane.lane)
        for lane in ("asr-ready", "enrichment-only"):
            selected = batch.build_parser().parse_args(
                [
                    "materialize",
                    "--selection",
                    str(self.root / "selection.json"),
                    "--bundle-root",
                    str(self.root / "bundles"),
                    "--processing-output-root",
                    str(self.root / "processing"),
                    "--lane",
                    lane,
                ]
            )
            self.assertEqual(lane, selected.lane)


if __name__ == "__main__":
    unittest.main()
