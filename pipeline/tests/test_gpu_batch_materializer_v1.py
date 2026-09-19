from __future__ import annotations

import copy
import hashlib
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipeline import materialize_gpu_asr_batch_v1 as BRIDGE


ROOT = Path(__file__).resolve().parents[2]
TEST_WORK_ROOT = ROOT / "pipeline/.test-work"


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class GPUBatchMaterializerV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="gpu-batch-materializer-v1-test-", dir=TEST_WORK_ROOT
        )
        self.base = Path(self.temporary.name)
        self.hot = self.base / "hot"
        self.hot.mkdir(mode=0o700)
        self.roots: dict[str, Path] = {}
        for name in ("work-order", "receipt", "result", "batch", "event", "lock"):
            path = self.hot / name
            path.mkdir(mode=0o700)
            self.roots[name] = path
        self.profile = BRIDGE.ASR_V5._load_local_module(
            "himr_gpu_profile_for_materializer_v1_test", "production_profile_v2.py"
        ).default_profile()
        self.profile_path = self.hot / "profile.json"
        self.profile_body = BRIDGE.ASR_V5.canonical_bytes(self.profile)
        self.profile_path.write_bytes(self.profile_body)
        self.profile_path.chmod(0o400)
        self.profile_sha256 = digest(self.profile_body)
        self.registration_path = self.hot / "root-registration.json"
        self.registration_sha256 = "1" * 64
        self.root_identity = "2" * 64
        self.registration_id = "gpurootreg_" + self.root_identity[:32]
        self.runtime_path = self.hot / "runtime.json"
        self.runtime_sha256 = "3" * 64
        self.runtime_identity = "4" * 64
        self.runtime_reference = {
            "receipt_path": str(self.runtime_path),
            "receipt_sha256": self.runtime_sha256,
            "receipt_id": "gpurtv2_" + self.runtime_identity[:32],
            "identity_sha256": self.runtime_identity,
            "status": "admitted",
        }
        self.root_reference = {
            "registration_path": str(self.registration_path),
            "registration_sha256": self.registration_sha256,
            "registration_id": self.registration_id,
            "identity_sha256": self.root_identity,
            "root_id": "himr-hot-v1",
            "path": str(self.hot),
            "filesystem_uuid": "11111111-1111-1111-1111-111111111111",
            "tier": "hot_main_drive",
        }
        self.root_registration_document = {"fixture_registration": True}
        self.profile_reference = {
            "path": str(self.profile_path),
            "sha256": self.profile_sha256,
            "profile_id": self.profile["profile_id"],
            "identity_sha256": self.profile["identity_sha256"],
        }
        self.runtime_receipt = {
            "status": "admitted",
            "identity_sha256": self.runtime_identity,
            "production_profile": self.profile,
            "production_profile_file": {
                **self.profile_reference,
                "byte_count": len(self.profile_body),
            },
            "root": {
                "root_id": self.root_reference["root_id"],
                "tier": "hot_main_drive",
                "path": str(self.hot),
                "filesystem": {
                    "type": "btrfs",
                    "uuid": self.root_reference["filesystem_uuid"],
                },
            },
            "root_registration": {
                "path": str(self.registration_path),
                "sha256": self.registration_sha256,
                "identity_sha256": self.root_identity,
                "registration_id": self.registration_id,
            },
        }
        self.manifest = self.queue_manifest(2)
        self.queue_path = (
            self.hot
            / "gpu-queues"
            / self.manifest["queue_relative_path"]
            / "manifest.json"
        )
        self.queue_sha256 = digest(BRIDGE.GPU_QUEUE.canonical_bytes(self.manifest))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def member(self, ordinal: int) -> dict[str, object]:
        audio_sha = digest(f"audio-{ordinal}".encode())
        receipt = {
            "path": str(self.hot / "receipts" / f"{ordinal:06d}.json"),
            "uri": (self.hot / "receipts" / f"{ordinal:06d}.json").as_uri(),
            "physical_sha256": digest(f"receipt-{ordinal}".encode()),
            "receipt_id": f"ppreceipt_{ordinal:032x}",
            "receipt_sha256": digest(f"receipt-core-{ordinal}".encode()),
            "ordinal": ordinal,
        }
        result = {
            "path": str(self.hot / "preprocess" / f"result-{ordinal}.json"),
            "uri": (self.hot / "preprocess" / f"result-{ordinal}.json").as_uri(),
            "sha256": digest(f"result-{ordinal}".encode()),
            "byte_count": 2048,
            "job_id": f"preprocess-job-{ordinal}",
            "processing_run_id": f"processing_run_{ordinal}",
            "recipe_sha256": digest(f"recipe-{ordinal}".encode()),
        }
        audio = {
            "artifact_id": f"artifact_{ordinal:032x}",
            "artifact_kind": "audio_16khz_mono_flac",
            "processing_run_id": f"processing_run_{ordinal}",
            "media_id": f"media_sha256_{audio_sha}",
            # Deliberately absent: a successful bridge proves it did not open
            # or hash the corpus payload.
            "path": str(self.hot / "audio" / f"absent-{ordinal}.flac"),
            "uri": (self.hot / "audio" / f"absent-{ordinal}.flac").as_uri(),
            "sha256": audio_sha,
            "byte_count": 1024 + ordinal,
            "duration_ms": 1000 + ordinal,
            "sealed_mode": "0400",
            "format": {
                "format_name": "flac",
                "codec_name": "flac",
                "sample_rate_hz": 16_000,
                "sample_format": "s16",
                "channels": 1,
                "channel_layout": "mono",
                "start_ms": 0,
            },
            "normalized_probe": {"fixture": True},
        }
        core = {
            "ordinal": ordinal,
            "preprocess_ordinal": ordinal,
            "audio": audio,
            "resource_disposition": {
                "state": "ready",
                "reasons": [],
                "evaluated_against": {
                    "profile_id": self.profile["profile_id"],
                    "profile_identity_sha256": self.profile["identity_sha256"],
                    "maximum_audio_bytes": self.profile["item_limits"]["maximum_audio_bytes"],
                    "maximum_audio_seconds": self.profile["item_limits"]["maximum_audio_seconds"],
                },
                "chunk_plan": None,
            },
            "lineage": {
                "preprocess_bundle": {
                    "path": str(self.hot / "bundle"),
                    "manifest_path": str(self.hot / "bundle" / "manifest.json"),
                    "manifest_physical_sha256": digest(b"bundle-physical"),
                    "bundle_id": "ppbatch_" + "9" * 32,
                    "identity_sha256": "9" * 64,
                    "manifest_sha256": "c" * 64,
                },
                "preprocess_receipt_state": {
                    "state_root": str(self.hot / "state"),
                    "receipt_count": 2,
                    "receipt_state_sha256": "5" * 64,
                    "receipt_refs_sha256": "6" * 64,
                },
                "receipt": receipt,
                "preprocess_result": result,
                "source_media": {"not_opened": True},
            },
            "routing_hint": None,
            "private_handling": None,
        }
        identity = digest(BRIDGE.ASR_V5.canonical_bytes(core))
        return {
            **core,
            "member_id": f"gpuasrmember_{identity[:32]}",
            "identity_sha256": identity,
        }

    def queue_manifest(self, count: int) -> dict[str, object]:
        members = [self.member(ordinal) for ordinal in range(1, count + 1)]
        queue_root = self.hot / "gpu-queues"
        core = {
            "kind": BRIDGE.GPU_QUEUE.KIND,
            "schema_version": BRIDGE.GPU_QUEUE.SCHEMA_VERSION,
            "implementation_version": BRIDGE.GPU_QUEUE.IMPLEMENTATION_VERSION,
            "materializer": BRIDGE.GPU_QUEUE.MATERIALIZER,
            "portable_root_registration": {
                "document_path": str(self.registration_path),
                "document_sha256": self.registration_sha256,
                "document_uid": os.geteuid(),
                "document_mode": "0400",
                "registration_id": self.registration_id,
                "identity_sha256": self.root_identity,
                "root_id": self.root_reference["root_id"],
                "tier": "hot_main_drive",
                "path": str(self.hot),
                "filesystem": {
                    "type": "btrfs",
                    "uuid": self.root_reference["filesystem_uuid"],
                },
                "owner": {"policy": "exact_uid", "uid": os.geteuid()},
            },
            "production_profile": {
                "reference": {
                    "path": str(self.profile_path),
                    "relative_path": self.profile_path.relative_to(self.hot).as_posix(),
                    "physical_sha256": self.profile_sha256,
                    "byte_count": len(self.profile_body),
                    "document_uid": os.geteuid(),
                    "document_mode": "0400",
                    "profile_id": self.profile["profile_id"],
                    "identity_sha256": self.profile["identity_sha256"],
                },
                "document": self.profile,
            },
            "origin": {"fixture": True},
            "output": {"queue_root": str(queue_root)},
            "members": members,
            "explicit_skips": [],
            "totals": {"ready_count": count},
            "handling_control": None,
            "safety": dict(BRIDGE.GPU_QUEUE.SAFETY),
        }
        identity = digest(BRIDGE.ASR_V5.canonical_bytes(core))
        queue_id = f"gpuasrqueue_{identity[:32]}"
        return {
            **core,
            "queue_id": queue_id,
            "identity_sha256": identity,
            "queue_relative_path": f"queues/{queue_id}",
        }

    def kwargs(self) -> dict[str, object]:
        return {
            "queue_manifest_path": self.queue_path,
            "expected_queue_sha256": self.queue_sha256,
            "root_registration_path": self.registration_path,
            "expected_root_registration_sha256": self.registration_sha256,
            "runtime_admission_path": self.runtime_path,
            "expected_runtime_admission_sha256": self.runtime_sha256,
            "production_profile_path": self.profile_path,
            "expected_production_profile_sha256": self.profile_sha256,
            "queue_ordinals": [2, 1],
            "work_order_root": self.roots["work-order"],
            "receipt_root": self.roots["receipt"],
            "result_root": self.roots["result"],
            "batch_root": self.roots["batch"],
            "event_root": self.roots["event"],
            "lock_root": self.roots["lock"],
        }

    def patched_materialize(self):
        return (
            mock.patch.object(
                BRIDGE.GPU_QUEUE, "validate_queue", return_value=self.manifest
            ),
            mock.patch.object(
                BRIDGE.ASR_V5,
                "runtime_admission_reference",
                return_value=(self.runtime_reference, self.runtime_receipt),
            ),
            mock.patch.object(
                BRIDGE.ASR_V5,
                "hot_root_reference",
                return_value=(
                    self.root_reference,
                    self.root_registration_document,
                ),
            ),
            mock.patch.object(
                BRIDGE.ASR_V5,
                "_verify_output_directory",
                return_value=None,
            ),
            mock.patch.object(
                BRIDGE.BATCH_V2.V5,
                "replay_external_bindings",
                return_value={"status": "replayed", "input_media_read": False},
            ),
        )

    def test_finite_bridge_is_idempotent_and_never_opens_audio(self) -> None:
        queue_patch, runtime_patch, root_patch, output_patch, replay_patch = (
            self.patched_materialize()
        )
        with (
            queue_patch as queue_validator,
            runtime_patch as runtime_validator,
            root_patch,
            output_patch as output_validator,
            replay_patch,
        ):
            first, first_path, first_disposition = BRIDGE.materialize(**self.kwargs())
            second, second_path, second_disposition = BRIDGE.materialize(**self.kwargs())
        self.assertEqual(first, second)
        self.assertEqual(first_path, second_path)
        self.assertEqual(first_disposition["created_work_orders"], 2)
        self.assertEqual(second_disposition["reused_work_orders"], 2)
        self.assertEqual(first_disposition["created_receipt"], 1)
        self.assertEqual(second_disposition["reused_receipt"], 1)
        self.assertEqual(first["selection"]["queue_ordinals"], [2, 1])
        self.assertEqual(first["batch"]["execution_class"], "production_private_asr")
        self.assertFalse(first["policy"]["input_media_payload_read"])
        self.assertFalse(first["policy"]["cold_storage_access"])
        self.assertEqual(first["policy"]["publication_authority"], "none")
        self.assertEqual(first["policy"]["result_import_authority"], "none")
        self.assertEqual(first["policy"]["deletion_authority"], "none")
        self.assertEqual(stat.S_IMODE(first_path.stat().st_mode), 0o400)
        self.assertEqual(
            [row["queue_ordinal"] for row in first["work_orders"]], [2, 1]
        )
        self.assertTrue(
            all(not Path(member["audio"]["path"]).exists() for member in self.manifest["members"])
        )
        self.assertEqual(queue_validator.call_count, 2)
        self.assertEqual(output_validator.call_count, 12)
        self.assertTrue(
            all(
                call.kwargs["require_admitted"]
                for call in runtime_validator.call_args_list
            )
        )

    def test_receipt_replay_checks_queue_orders_and_batch_without_media(self) -> None:
        queue_patch, runtime_patch, root_patch, output_patch, replay_patch = (
            self.patched_materialize()
        )
        with queue_patch, runtime_patch, root_patch, output_patch, replay_patch:
            receipt, receipt_path, _ = BRIDGE.materialize(**self.kwargs())
        receipt_sha = digest(BRIDGE.canonical_bytes(receipt))
        with mock.patch.object(
            BRIDGE.GPU_QUEUE, "validate_queue", return_value=self.manifest
        ), mock.patch.object(
            BRIDGE.ASR_V5,
            "hot_root_reference",
            return_value=(self.root_reference, self.root_registration_document),
        ), mock.patch.object(
            BRIDGE.ASR_V5,
            "runtime_admission_reference",
            return_value=(self.runtime_reference, self.runtime_receipt),
        ):
            replayed = BRIDGE.load_receipt(receipt_path, receipt_sha, replay=True)
        self.assertEqual(replayed, receipt)
        self.assertTrue(
            all(not Path(member["audio"]["path"]).exists() for member in self.manifest["members"])
        )

    def test_candidate_runtime_and_binding_mismatches_fail_before_writes(self) -> None:
        candidate_reference = {**self.runtime_reference, "status": "candidate"}
        candidate_receipt = {**self.runtime_receipt, "status": "candidate"}
        with mock.patch.object(
            BRIDGE.GPU_QUEUE, "validate_queue", return_value=self.manifest
        ), mock.patch.object(
            BRIDGE.ASR_V5,
            "hot_root_reference",
            return_value=(self.root_reference, self.root_registration_document),
        ), mock.patch.object(
            BRIDGE.ASR_V5,
            "runtime_admission_reference",
            return_value=(candidate_reference, candidate_receipt),
        ), self.assertRaisesRegex(BRIDGE.MaterializerError, "admitted runtime"):
            BRIDGE.materialize(**self.kwargs())
        self.assertFalse((self.roots["work-order"] / "work-orders").exists())
        changed = self.kwargs()
        changed["expected_production_profile_sha256"] = "f" * 64
        with mock.patch.object(
            BRIDGE.GPU_QUEUE, "validate_queue", return_value=self.manifest
        ), self.assertRaisesRegex(BRIDGE.MaterializerError, "profile differs"):
            BRIDGE.materialize(**changed)

    def test_local_private_mode_materializes_candidate_runtime_production_lineage(self) -> None:
        self.runtime_reference = {**self.runtime_reference, "status": "candidate"}
        self.runtime_receipt = {**self.runtime_receipt, "status": "candidate"}
        queue_patch, runtime_patch, root_patch, output_patch, replay_patch = (
            self.patched_materialize()
        )
        arguments = self.kwargs()
        arguments["execution_mode"] = "local-private-production"
        with queue_patch, runtime_patch as runtime_validator, root_patch, output_patch, replay_patch:
            receipt, _path, _disposition = BRIDGE.materialize(**arguments)
        self.assertEqual(
            receipt["batch"]["execution_class"],
            BRIDGE.BATCH_V2.EXECUTION_CLASS_LOCAL_PRIVATE,
        )
        self.assertEqual(receipt["runtime_admission"]["status"], "candidate")
        self.assertTrue(
            all(
                call.kwargs["require_admitted"] is False
                for call in runtime_validator.call_args_list
            )
        )

    def test_selection_is_explicit_unique_and_ready_only(self) -> None:
        with self.assertRaisesRegex(BRIDGE.MaterializerError, "unique"):
            BRIDGE._selected_members(self.manifest, [1, 1])
        with self.assertRaisesRegex(BRIDGE.MaterializerError, "absent"):
            BRIDGE._selected_members(self.manifest, [3])
        changed = copy.deepcopy(self.manifest)
        changed["members"][0]["resource_disposition"]["state"] = "requires_chunking"
        with self.assertRaisesRegex(BRIDGE.MaterializerError, "not ready"):
            BRIDGE._selected_members(changed, [1])
        with self.assertRaisesRegex(BRIDGE.MaterializerError, "finite"):
            BRIDGE._selected_members(self.manifest, [])

    def test_expected_queue_digest_and_cold_storage_fail_closed(self) -> None:
        changed = self.kwargs()
        changed["expected_queue_sha256"] = "f" * 64
        with mock.patch.object(
            BRIDGE.GPU_QUEUE, "validate_queue", return_value=self.manifest
        ), self.assertRaisesRegex(BRIDGE.MaterializerError, "physical SHA"):
            BRIDGE.materialize(**changed)
        changed = self.kwargs()
        changed["work_order_root"] = Path("/mnt/archive/HIMR/work-orders")
        with mock.patch.object(
            BRIDGE.GPU_QUEUE, "validate_queue", return_value=self.manifest
        ), mock.patch.object(
            BRIDGE.ASR_V5,
            "hot_root_reference",
            return_value=(self.root_reference, self.root_registration_document),
        ), mock.patch.object(
            BRIDGE.ASR_V5,
            "runtime_admission_reference",
            return_value=(self.runtime_reference, self.runtime_receipt),
        ), self.assertRaisesRegex(BRIDGE.MaterializerError, "cold storage"):
            BRIDGE.materialize(**changed)

    def test_receipt_schema_is_closed_and_content_addressed(self) -> None:
        queue_patch, runtime_patch, root_patch, output_patch, replay_patch = (
            self.patched_materialize()
        )
        with queue_patch, runtime_patch, root_patch, output_patch, replay_patch:
            receipt, _, _ = BRIDGE.materialize(**self.kwargs())
        self.assertEqual(BRIDGE.validate_receipt(receipt), receipt)
        self.assertEqual(
            receipt["receipt_id"], f"gpuasrmat1_{receipt['identity_sha256'][:32]}"
        )
        forged = copy.deepcopy(receipt)
        forged["selection"]["extra"] = True
        with self.assertRaisesRegex(BRIDGE.MaterializerError, "unexpected"):
            BRIDGE.validate_receipt(forged)
        forged = copy.deepcopy(receipt)
        forged["identity_sha256"] = "0" * 64
        with self.assertRaisesRegex(BRIDGE.MaterializerError, "identity"):
            BRIDGE.validate_receipt(forged)

    def test_contract_and_cli_are_finite_and_non_authoritative(self) -> None:
        contract = BRIDGE.contract_document()
        self.assertEqual(contract["descriptor"]["selection"], "explicit_unique_queue_ordinals_1_to_32")
        self.assertEqual(contract["descriptor"]["policy"]["inference_authority"], "none")
        args = BRIDGE.build_parser().parse_args(
            [
                "materialize",
                "--queue-manifest", str(self.queue_path),
                "--expected-queue-sha256", self.queue_sha256,
                "--root-registration", str(self.registration_path),
                "--expected-root-registration-sha256", self.registration_sha256,
                "--runtime-admission", str(self.runtime_path),
                "--expected-runtime-admission-sha256", self.runtime_sha256,
                "--production-profile", str(self.profile_path),
                "--expected-production-profile-sha256", self.profile_sha256,
                "--queue-ordinal", "2",
                "--queue-ordinal", "1",
                "--work-order-root", str(self.roots["work-order"]),
                "--receipt-root", str(self.roots["receipt"]),
                "--result-root", str(self.roots["result"]),
                "--batch-root", str(self.roots["batch"]),
                "--event-root", str(self.roots["event"]),
                "--lock-root", str(self.roots["lock"]),
            ]
        )
        self.assertEqual(args.queue_ordinal, [2, 1])


if __name__ == "__main__":
    unittest.main()
