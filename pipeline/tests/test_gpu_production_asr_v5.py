from __future__ import annotations

import copy
import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


V5 = load_module(
    "himr_gpu_production_asr_v5_test_module",
    ROOT / "pipeline/gpu/production_asr_v5.py",
)
PROFILE = V5._load_local_module(
    "himr_gpu_profile_for_asr_v5", "production_profile_v2.py"
)


class GPUProductionASRV5Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.profile = PROFILE.default_profile()

    def hot_root(self, path: str = "/hot") -> dict[str, object]:
        return {
            "registration_path": f"{path}/root-registration.json",
            "registration_sha256": "1" * 64,
            "registration_id": "gpurootreg_" + "2" * 32,
            "identity_sha256": "2" * 64,
            "root_id": "himr-hot-v1",
            "path": path,
            "filesystem_uuid": "11111111-1111-1111-1111-111111111111",
            "tier": "hot_main_drive",
        }

    def runtime(self, path: str = "/hot", status: str = "candidate") -> dict[str, object]:
        return {
            "receipt_path": f"{path}/runtime.json",
            "receipt_sha256": "3" * 64,
            "receipt_id": "gpurtv2_" + "4" * 32,
            "identity_sha256": "4" * 64,
            "status": status,
        }

    def profile_reference(self, path: str = "/hot") -> dict[str, object]:
        return {
            "path": f"{path}/profile.json",
            "sha256": "5" * 64,
            "profile_id": self.profile["profile_id"],
            "identity_sha256": self.profile["identity_sha256"],
        }

    def input(self, path: str = "/hot") -> dict[str, object]:
        digest = "a" * 64
        return {
            "path": f"{path}/audio.flac",
            "expected_sha256": digest,
            "expected_byte_count": 1024,
            "expected_duration_ms": 1000,
            "media_id": f"media_sha256_{digest}",
            "artifact_id": "artifact_" + "b" * 32,
            "parent_processing_run_id": "processing_run_1",
            "media_format": dict(V5.MEDIA_FORMAT),
            "sealed_mode": "0400",
            "timeline_offset_ms": 0,
        }

    def synthetic_lineage(self, path: str = "/hot") -> dict[str, object]:
        return {
            "kind": V5.SOURCE_LINEAGE_SYNTHETIC,
            "fixture_manifest": {
                "path": f"{path}/fixture.json",
                "sha256": "6" * 64,
                "identity_sha256": "7" * 64,
                "fixture_id": "fixture-1",
            },
            "fixture_case_id": "case-1",
            "contains_corpus_media": False,
            "scope": "purpose_built_synthetic_only",
            "corpus_authority": "none",
        }

    def production_lineage(self, path: str = "/hot") -> dict[str, object]:
        return {
            "kind": V5.SOURCE_LINEAGE_PRODUCTION,
            "implementation_version": "0.3.0",
            "gpu_handoff": {
                "manifest_path": f"{path}/gpu-queue/manifest.json",
                "manifest_sha256": "7" * 64,
                "queue_id": "gpuasrqueue_" + "8" * 32,
                "identity_sha256": "8" * 64,
                "member_id": "gpuasrmember_" + "9" * 32,
                "member_identity_sha256": "9" * 64,
                "queue_ordinal": 1,
                "preprocess_ordinal": 1,
            },
            "bundle_manifest": {
                "path": f"{path}/bundle/manifest.json",
                "sha256": "8" * 64,
                "bundle_id": "ppbatch_" + "9" * 32,
                "identity_sha256": "9" * 64,
                "manifest_sha256": "c" * 64,
            },
            "receipt": {
                "path": f"{path}/receipts/000001.json",
                "sha256": "d" * 64,
                "receipt_id": "ppreceipt_" + "e" * 32,
                "receipt_sha256": "e" * 64,
                "ordinal": 1,
            },
            "preprocess_result": {
                "path": f"{path}/preprocess/result.json",
                "sha256": "f" * 64,
                "byte_count": 2048,
                "processing_run_id": "processing_run_1",
                "recipe_sha256": "0" * 64,
            },
            "handling": {
                "mode": "none",
                "descriptor_sha256": None,
                "control_identity_sha256": None,
                "handling_boundary_sha256": None,
                "seal_receipt_sha256": None,
                "seal_plan_sha256": None,
                "source_byte_identity_claimed": False,
                "publication_authority": "none",
            },
            "corpus_authority": "preprocess_receipt_lineage_only",
        }

    def core(
        self,
        *,
        path: str = "/hot",
        production: bool = False,
    ) -> dict[str, object]:
        return {
            "kind": V5.WORK_ORDER_KIND,
            "schema_version": V5.CONTRACT_VERSION,
            "implementation_version": V5.IMPLEMENTATION_VERSION,
            "job_id": "gpu-asr-test",
            "input": self.input(path),
            "source_lineage": (
                self.production_lineage(path)
                if production
                else self.synthetic_lineage(path)
            ),
            "runtime_admission": self.runtime(
                path, "admitted" if production else "candidate"
            ),
            "production_profile": self.profile_reference(path),
            "hot_root": self.hot_root(path),
            "execution_contract": V5.execution_contract_from_profile(self.profile),
            "transcript_semantics": copy.deepcopy(V5.TRANSCRIPT_SEMANTICS),
            "catalog_context": None,
            "output": {
                "root": f"{path}/results",
                "layout": V5.OUTPUT_LAYOUT,
                "atomic_no_replace": True,
            },
            "policy": dict(V5.POLICY),
        }

    def order(self, **kwargs: object) -> dict[str, object]:
        return V5.make_work_order(self.core(**kwargs), profile_document=self.profile)

    @staticmethod
    def histogram(
        minimum: int, mean: float, p50: int, p95: int, maximum: int, width: int = 1
    ) -> dict[str, object]:
        return {
            "sample_count": 4,
            "minimum": minimum,
            "mean": mean,
            "p50": p50,
            "p95": p95,
            "maximum": maximum,
            "bin_width": width,
        }

    def result_core(self, order: dict[str, object]) -> dict[str, object]:
        plan = V5.result_plan(order, profile_document=self.profile)
        telemetry_limits = self.profile["telemetry"]
        telemetry = {
            "implementation_version": "0.1.0",
            "fast_interval_seconds": telemetry_limits["fast_interval_ms"] / 1000,
            "slow_interval_seconds": telemetry_limits["slow_interval_ms"] / 1000,
            "fast_sample_count": 20,
            "slow_sample_count": 4,
            "sample_span_seconds": 1.0,
            "last_fast_sample_age_seconds": 0.01,
            "last_slow_sample_age_seconds": 0.02,
            "process_vram_measurement_seen": True,
            "process_peak_used_bytes": 900 * 1024**2,
            "global_peak_used_bytes": 1000 * 1024**2,
            "minimum_free_bytes": telemetry_limits["minimum_free_vram_bytes"] + 1,
            "utilization_percent": self.histogram(10, 25.0, 20, 40, 50),
            "memory_controller_utilization_percent": self.histogram(5, 15.0, 10, 25, 30),
            "active_sample_fraction": 1.0,
            "temperature_c": self.histogram(40, 45.0, 45, 49, 50),
            "power_mw": self.histogram(10_000, 20_000.0, 20_000, 30_000, 30_000, 1000),
            "estimated_energy_millijoules": 20_000.0,
            "sm_clock_mhz": self.histogram(200, 500.0, 500, 800, 900, 10),
            "throttle_reasons_bitmask_or": 0,
            "sampler_error": None,
        }
        input_item = order["input"]
        return {
            "kind": V5.RESULT_KIND,
            "schema_version": V5.CONTRACT_VERSION,
            "implementation_version": V5.IMPLEMENTATION_VERSION,
            "status": "completed",
            "work_order": {
                "work_order_id": order["work_order_id"],
                "identity_sha256": order["identity_sha256"],
            },
            "runtime_admission": {
                "receipt_id": order["runtime_admission"]["receipt_id"],
                "identity_sha256": order["runtime_admission"]["identity_sha256"],
            },
            "production_profile": {
                "profile_id": order["production_profile"]["profile_id"],
                "identity_sha256": order["production_profile"]["identity_sha256"],
            },
            "input": {
                "sha256": input_item["expected_sha256"],
                "byte_count": input_item["expected_byte_count"],
                "duration_ms": input_item["expected_duration_ms"],
                "media_id": input_item["media_id"],
                "artifact_id": input_item["artifact_id"],
                "timeline_offset_ms": 0,
            },
            "execution": {
                "attempt_id": "gpuasrattempt_" + "a" * 32,
                "started_at": "2026-08-29T12:00:00Z",
                "completed_at": "2026-08-29T12:00:02Z",
                "wall_seconds": 2.0,
                "model_load_seconds": 0.5,
                "inference_seconds": 1.0,
                "phase_seconds": {
                    "preflight_input_hash": 0.05,
                    "preflight_ffprobe": 0.05,
                    "transcribe_setup": 0.2,
                    "model_iterator": 0.8,
                    "transcript_normalization": 0.05,
                    "artifact_serialization": 0.05,
                },
                "model_load_count": 1,
                "live_gpu_uuid": self.profile["hardware"]["gpu_uuid"],
                "telemetry": telemetry,
            },
            "transcript": {
                "language": {
                    "value": "en",
                    "selection_basis": "forced_by_inference_profile",
                    "detection_performed": False,
                    "raw_probability": None,
                    "calibrated_probability": None,
                },
                "timeline": {
                    "coordinate_system": "media_ms",
                    "source_duration_ms": input_item["expected_duration_ms"],
                    "source_offset_ms": 0,
                    "end_ms": input_item["expected_duration_ms"],
                },
                "segment_count": 1,
                "word_count": 2,
                "text_character_count": 11,
                "word_timing_anomalies": {
                    "anomalous_word_count": 1,
                    "total_flag_count": 1,
                    "flag_counts": {
                        "precedes_segment_start": 0,
                        "extends_beyond_segment_end": 0,
                        "start_regresses_from_previous": 0,
                        "overlaps_previous": 1,
                    },
                    "human_review_required": True,
                },
                "review_status": "unreviewed_machine_output",
                "semantics": copy.deepcopy(V5.TRANSCRIPT_SEMANTICS),
            },
            "artifacts": [
                {
                    "artifact_kind": "faster_whisper_raw_transcript_json",
                    "path": plan["raw_transcript_path"],
                    "sha256": "b" * 64,
                    "byte_count": 100,
                    "document_id": "gpuasrraw_" + "b" * 32,
                    "identity_sha256": "b" * 64,
                },
                {
                    "artifact_kind": "transcript_normalized_json",
                    "path": plan["normalized_transcript_path"],
                    "sha256": "c" * 64,
                    "byte_count": 100,
                    "document_id": "gpuasrnorm_" + "c" * 32,
                    "identity_sha256": "c" * 64,
                },
            ],
            "policy": dict(V5.POLICY),
        }

    def queue_fixture(self, *, private: bool = False) -> tuple[dict[str, object], dict[str, object]]:
        profile_body = V5.canonical_bytes(self.profile)
        receipt = {
            "path": "/hot/receipts/000001.json",
            "uri": "file:///hot/receipts/000001.json",
            "physical_sha256": "d" * 64,
            "receipt_id": "ppreceipt_" + "e" * 32,
            "receipt_sha256": "e" * 64,
            "ordinal": 1,
        }
        result = {
            "path": "/hot/preprocess/result.json",
            "uri": "file:///hot/preprocess/result.json",
            "sha256": "f" * 64,
            "byte_count": 2048,
            "job_id": "preprocess-job-1",
            "processing_run_id": "processing_run_1",
            "recipe_sha256": "0" * 64,
        }
        handling_control = None
        private_handling = None
        if private:
            boundary = {"fixture": "legacy-v03-no-final-newline"}
            control_entry = {
                "ordinal": 1,
                "entry_id": "entry-1",
                "handling_policy": {
                    "collection": "private",
                    "retention": "private",
                },
                "handling_boundary_sha256": V5.sha256_bytes(
                    V5._legacy_v03_canonical_bytes(boundary)
                ),
                "seal_receipt_sha256": "1" * 64,
                "seal_plan_sha256": "2" * 64,
                "source_byte_identity_claimed": False,
            }
            control_core = {
                "kind": "v30_private_acquisition_handling_control",
                "private_entry_count": 1,
                "entries": [control_entry],
                "seal_receipt_replay_required": True,
                "handling_policy_propagation_required": True,
                "publication_authority": "none",
            }
            handling_control = {
                **control_core,
                "identity_sha256": V5.sha256_bytes(
                    V5._legacy_v03_canonical_bytes(control_core)
                ),
            }
            private_handling = {
                "preprocess_control_entry": control_entry,
                "handling_boundary": boundary,
            }
        audio = {
            "artifact_id": "artifact_" + "b" * 32,
            "artifact_kind": "audio_16khz_mono_flac",
            "processing_run_id": "processing_run_1",
            "media_id": "media_sha256_" + "a" * 64,
            "path": "/hot/audio.flac",
            "uri": "file:///hot/audio.flac",
            "sha256": "a" * 64,
            "byte_count": 1024,
            "duration_ms": 1000,
            "sealed_mode": "0444",
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
        member_core = {
            "ordinal": 1,
            "preprocess_ordinal": 1,
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
                    "path": "/hot/bundle",
                    "manifest_path": "/hot/bundle/manifest.json",
                    "manifest_physical_sha256": "8" * 64,
                    "bundle_id": "ppbatch_" + "9" * 32,
                    "identity_sha256": "9" * 64,
                    "manifest_sha256": "c" * 64,
                },
                "preprocess_receipt_state": {
                    "state_root": "/hot/state",
                    "receipt_count": 1,
                    "receipt_state_sha256": "3" * 64,
                    "receipt_refs_sha256": "4" * 64,
                },
                "receipt": receipt,
                "preprocess_result": result,
                "source_media": {"fixture": True},
            },
            "routing_hint": None,
            "private_handling": private_handling,
        }
        member_identity = V5.sha256_bytes(V5.canonical_bytes(member_core))
        member = {
            **member_core,
            "member_id": "gpuasrmember_" + member_identity[:32],
            "identity_sha256": member_identity,
        }
        safety = {
            "visibility": "private",
            "network_access": False,
            "execution_authority": "none",
            "gpu_execution_authority": "none",
            "chunking_authority": "none",
            "result_import_authority": "none",
            "publication_authority": "none",
            "catalogue_mutation_authority": "none",
            "identity_authority": "none",
            "biometric_authority": "none",
            "wiki_authority": "none",
            "archive_authority": "none",
            "deletion_authority": "none",
            "input_selection": "exact_completed_preprocess_receipts_only",
        }
        manifest_core = {
            "kind": "himr_preprocess_gpu_asr_queue",
            "schema_version": 1,
            "implementation_version": "0.1.0",
            "materializer": "himr-preprocess-gpu-asr-queue",
            "portable_root_registration": {
                "document_path": "/hot/root-registration.json",
                "document_sha256": "1" * 64,
                "document_uid": os.geteuid(),
                "document_mode": "0400",
                "registration_id": "gpurootreg_" + "2" * 32,
                "identity_sha256": "2" * 64,
                "root_id": "himr-hot-v1",
                "tier": "hot_main_drive",
                "path": "/hot",
                "filesystem": {
                    "type": "btrfs",
                    "uuid": "11111111-1111-1111-1111-111111111111",
                },
                "owner": {"policy": "exact_uid", "uid": os.geteuid()},
            },
            "production_profile": {
                "reference": {
                    "path": "/hot/profile.json",
                    "relative_path": "profile.json",
                    "physical_sha256": V5.sha256_bytes(profile_body),
                    "byte_count": len(profile_body),
                    "document_uid": os.geteuid(),
                    "document_mode": "0400",
                    "profile_id": self.profile["profile_id"],
                    "identity_sha256": self.profile["identity_sha256"],
                },
                "document": self.profile,
            },
            "origin": {"fixture": True},
            "output": {"queue_root": "/hot/gpu-queues"},
            "members": [member],
            "explicit_skips": [],
            "totals": {"ready_count": 1},
            "handling_control": handling_control,
            "safety": safety,
        }
        manifest_identity = V5.sha256_bytes(V5.canonical_bytes(manifest_core))
        queue_id = "gpuasrqueue_" + manifest_identity[:32]
        manifest = {
            **manifest_core,
            "queue_id": queue_id,
            "identity_sha256": manifest_identity,
            "queue_relative_path": f"queues/{queue_id}",
        }
        return member, manifest

    def test_synthetic_work_order_is_content_addressed_and_has_no_numeric_device(self) -> None:
        order = self.order()
        self.assertEqual(V5.validate_work_order(order, profile_document=self.profile), order)
        self.assertEqual(order["work_order_id"], "gpuasrwo5_" + order["identity_sha256"][:32])
        encoded = V5.canonical_bytes(order)
        self.assertNotIn(b"expected_device", encoded)
        self.assertNotIn(b"st_dev", encoded)
        self.assertEqual(order["source_lineage"]["kind"], V5.SOURCE_LINEAGE_SYNTHETIC)

    def test_content_change_changes_work_order_identity(self) -> None:
        first = self.order()
        changed = self.core()
        changed["job_id"] = "gpu-asr-test-2"
        second = V5.make_work_order(changed, profile_document=self.profile)
        self.assertNotEqual(first["identity_sha256"], second["identity_sha256"])

    def test_production_lineage_candidate_runtime_remains_contract_only(self) -> None:
        order = self.order(production=True)
        self.assertEqual(order["source_lineage"]["kind"], V5.SOURCE_LINEAGE_PRODUCTION)
        core = self.core(production=True)
        core["runtime_admission"]["status"] = "candidate"
        candidate = V5.make_work_order(core, profile_document=self.profile)
        self.assertEqual(candidate["runtime_admission"]["status"], "candidate")
        self.assertEqual(
            candidate["source_lineage"]["kind"], V5.SOURCE_LINEAGE_PRODUCTION
        )

    def test_synthetic_canary_cannot_claim_corpus_authority(self) -> None:
        core = self.core()
        core["source_lineage"]["contains_corpus_media"] = True
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "corpus authority"):
            V5.make_work_order(core, profile_document=self.profile)
        core = self.core()
        core["source_lineage"]["type"] = core["source_lineage"].pop("kind")
        with self.assertRaises(V5.ProductionASRV5Error):
            V5.make_work_order(core, profile_document=self.profile)

    def test_unknown_fields_and_non_normalized_paths_fail(self) -> None:
        core = self.core()
        core["input"]["extra"] = True
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "unexpected fields"):
            V5.make_work_order(core, profile_document=self.profile)
        core = self.core()
        core["output"]["root"] = "/hot/results/../escape"
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "normalized absolute"):
            V5.make_work_order(core, profile_document=self.profile)

    def test_output_and_all_source_paths_are_confined_to_hot_root(self) -> None:
        core = self.core()
        core["output"]["root"] = "/other/results"
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "outside"):
            V5.make_work_order(core, profile_document=self.profile)
        core = self.core(production=True)
        core["source_lineage"]["receipt"]["path"] = "/other/receipt.json"
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "outside"):
            V5.make_work_order(core, profile_document=self.profile)

    def test_all_execution_limits_must_derive_from_profile(self) -> None:
        core = self.core()
        core["execution_contract"]["item_limits"]["maximum_words"] -= 1
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "derive exactly"):
            V5.make_work_order(core, profile_document=self.profile)
        self.assertEqual(
            self.core()["execution_contract"]["batch_limits"],
            self.profile["batch_limits"],
        )

    def test_profile_audio_limits_are_enforced(self) -> None:
        core = self.core()
        core["input"]["expected_byte_count"] = (
            self.profile["item_limits"]["maximum_audio_bytes"] + 1
        )
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "maximum_audio_bytes"):
            V5.make_work_order(core, profile_document=self.profile)

    def test_ready_queue_member_projects_to_production_order_without_live_stat(self) -> None:
        member, manifest = self.queue_fixture()
        lineage = V5.source_lineage_from_preprocess_descriptor(member, manifest)
        self.assertEqual(lineage["gpu_handoff"]["preprocess_ordinal"], 1)
        self.assertEqual(lineage["handling"]["mode"], "none")
        order, profile = V5.work_order_from_preprocess_descriptor(
            entry=member,
            manifest=manifest,
            runtime_admission=self.runtime(status="admitted"),
            output_root="/hot/results",
        )
        self.assertEqual(profile, self.profile)
        self.assertEqual(order["input"]["sealed_mode"], "0444")
        self.assertEqual(order["source_lineage"]["gpu_handoff"]["member_id"], member["member_id"])

    def test_private_v03_handling_uses_legacy_no_newline_hashes(self) -> None:
        member, manifest = self.queue_fixture(private=True)
        lineage = V5.source_lineage_from_preprocess_descriptor(member, manifest)
        self.assertEqual(lineage["handling"]["mode"], "private_v30")
        self.assertEqual(
            lineage["handling"]["handling_boundary_sha256"],
            member["private_handling"]["preprocess_control_entry"]["handling_boundary_sha256"],
        )
        changed = copy.deepcopy(member)
        changed["resource_disposition"]["state"] = "requires_chunking"
        with self.assertRaises(V5.ProductionASRV5Error):
            V5.source_lineage_from_preprocess_descriptor(changed, manifest)

    def test_result_binds_live_gpu_uuid_and_telemetry(self) -> None:
        order = self.order()
        result = V5.make_result(
            self.result_core(order), work_order=order, profile_document=self.profile
        )
        self.assertEqual(
            V5.validate_result(result, work_order=order, profile_document=self.profile),
            result,
        )
        encoded = V5.canonical_bytes(result)
        self.assertIn(self.profile["hardware"]["gpu_uuid"].encode(), encoded)
        self.assertNotIn(b"expected_device", encoded)
        self.assertNotIn(b"st_dev", encoded)

    def test_result_rejects_other_gpu_and_resource_violation(self) -> None:
        order = self.order()
        core = self.result_core(order)
        core["execution"]["live_gpu_uuid"] = "GPU-11111111-2222-3333-4444-555555555555"
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "live GPU UUID"):
            V5.make_result(core, work_order=order, profile_document=self.profile)
        core = self.result_core(order)
        core["execution"]["telemetry"]["process_peak_used_bytes"] = (
            self.profile["telemetry"]["maximum_process_vram_bytes"] + 1
        )
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "telemetry violates"):
            V5.make_result(core, work_order=order, profile_document=self.profile)
        core = self.result_core(order)
        core["execution"]["phase_seconds"]["model_iterator"] = 0.7
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "setup plus iterator"):
            V5.make_result(core, work_order=order, profile_document=self.profile)

    def test_result_language_timeline_and_anomaly_contracts_are_exact(self) -> None:
        order = self.order()
        core = self.result_core(order)
        core["transcript"]["language"]["detection_performed"] = True
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "language provenance"):
            V5.make_result(core, work_order=order, profile_document=self.profile)
        core = self.result_core(order)
        core["transcript"]["word_timing_anomalies"]["total_flag_count"] = 2
        with self.assertRaisesRegex(V5.ProductionASRV5Error, "anomaly summary"):
            V5.make_result(core, work_order=order, profile_document=self.profile)

    def test_result_plan_is_deterministic_and_confined(self) -> None:
        order = self.order()
        first = V5.result_plan(order, profile_document=self.profile)
        second = V5.result_plan(order, profile_document=self.profile)
        self.assertEqual(first, second)
        self.assertTrue(first["result_path"].startswith("/hot/results/asr/"))

    def test_status_absent_performs_no_execution_or_write(self) -> None:
        order = self.order()
        observed = V5.status(order, profile_document=self.profile)
        self.assertEqual(observed["status"], "absent")
        self.assertFalse(observed["inference_performed"])
        self.assertFalse(observed["files_written"])

    def test_status_validates_a_canonical_completed_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "results"
            output.mkdir(mode=0o700)
            order = self.order(path=str(root))
            core = self.result_core(order)
            result = V5.make_result(
                core, work_order=order, profile_document=self.profile
            )
            result_path = Path(
                V5.result_plan(order, profile_document=self.profile)["result_path"]
            )
            result_path.parent.mkdir(parents=True, mode=0o700)
            result_path.write_bytes(V5.canonical_bytes(result))
            result_path.chmod(0o400)
            observed = V5.status(order, profile_document=self.profile)
            self.assertEqual(observed["status"], "completed")
            self.assertEqual(observed["result_id"], result["result_id"])

    def test_contract_document_exposes_contract_only_commands(self) -> None:
        contract = V5.contract_document()
        self.assertEqual(contract["commands"], ["contract", "create", "validate", "status"])
        self.assertIsNone(contract["inference_entry_point"])
        self.assertEqual(
            contract["work_order"]["descriptor"]["source_lineage_types"],
            [V5.SOURCE_LINEAGE_PRODUCTION, V5.SOURCE_LINEAGE_SYNTHETIC],
        )

    def test_control_documents_have_exact_candidate_or_production_modes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "control.json"
            path.write_bytes(b"{}\n")
            path.chmod(0o400)
            self.assertEqual(
                V5._stable_file_bytes(
                    path, label="fixture control", maximum_bytes=1024
                ),
                b"{}\n",
            )
            path.chmod(0o440)
            with self.assertRaisesRegex(V5.ProductionASRV5Error, "metadata"):
                V5._stable_file_bytes(
                    path, label="fixture control", maximum_bytes=1024
                )

    def test_only_preprocess_result_replay_accepts_historical_readonly_modes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            core = self.core(path=str(root), production=True)
            lineage = core["source_lineage"]
            sources = {
                "gpu_handoff_manifest": (
                    Path(lineage["gpu_handoff"]["manifest_path"]),
                    lineage["gpu_handoff"],
                    "manifest_sha256",
                ),
                "bundle_manifest": (
                    Path(lineage["bundle_manifest"]["path"]),
                    lineage["bundle_manifest"],
                    "sha256",
                ),
                "preprocess_receipt": (
                    Path(lineage["receipt"]["path"]),
                    lineage["receipt"],
                    "sha256",
                ),
                "preprocess_result": (
                    Path(lineage["preprocess_result"]["path"]),
                    lineage["preprocess_result"],
                    "sha256",
                ),
            }
            for role, (path, reference, digest_field) in sources.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                body = V5.canonical_bytes({"role": role})
                path.write_bytes(body)
                path.chmod(0o444 if role == "preprocess_result" else 0o400)
                reference[digest_field] = V5.sha256_bytes(body)
                if role == "preprocess_result":
                    reference["byte_count"] = len(body)

            order = V5.make_work_order(core, profile_document=self.profile)
            hot = order["hot_root"]
            registration = {
                "registration_id": hot["registration_id"],
                "root_id": hot["root_id"],
                "tier": hot["tier"],
                "path": hot["path"],
                "filesystem": {
                    "type": "btrfs",
                    "uuid": hot["filesystem_uuid"],
                },
            }
            runtime_receipt = {
                "receipt_id": order["runtime_admission"]["receipt_id"],
                "production_profile": {
                    "identity_sha256": self.profile["identity_sha256"]
                },
                "root": {
                    "root_id": registration["root_id"],
                    "tier": registration["tier"],
                    "path": registration["path"],
                    "filesystem": registration["filesystem"],
                },
                "root_registration": {
                    "registration_id": registration["registration_id"]
                },
            }
            patches = (
                mock.patch.object(
                    V5, "hot_root_reference", return_value=(hot, registration)
                ),
                mock.patch.object(V5, "_verify_output_directory"),
                mock.patch.object(
                    V5,
                    "runtime_admission_reference",
                    return_value=(order["runtime_admission"], runtime_receipt),
                ),
                mock.patch.object(
                    V5, "load_profile_document", return_value=self.profile
                ),
            )
            with patches[0], patches[1], patches[2], patches[3]:
                replay = V5.replay_external_bindings(
                    order, profile_document=self.profile
                )
            self.assertEqual(replay["status"], "replayed")

            bundle_path = sources["bundle_manifest"][0]
            bundle_path.chmod(0o444)
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                self.assertRaisesRegex(
                    V5.ProductionASRV5Error,
                    r"source lineage JSON \(bundle_manifest\) metadata is unsafe",
                ),
            ):
                V5.replay_external_bindings(order, profile_document=self.profile)

            bundle_path.chmod(0o400)
            result_path = sources["preprocess_result"][0]
            result_path.chmod(0o644)
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                self.assertRaisesRegex(
                    V5.ProductionASRV5Error,
                    r"source lineage JSON \(preprocess_result\) metadata is unsafe",
                ),
            ):
                V5.replay_external_bindings(order, profile_document=self.profile)


if __name__ == "__main__":
    unittest.main()
