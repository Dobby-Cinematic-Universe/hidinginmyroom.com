from __future__ import annotations

import copy
import errno
import fcntl
import importlib.util
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


BATCH = load_module(
    "himr_gpu_production_batch_v2_test_module",
    ROOT / "pipeline/gpu/production_asr_batch_v2.py",
)
V5 = BATCH.V5
PROFILE = BATCH.PROFILE


class GPUProductionBatchV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "hot"
        self.root.mkdir(mode=0o700)
        self.result_root = self.directory("results")
        self.batch_root = self.directory("batches-root")
        self.event_root = self.directory("events")
        self.lock_root = self.directory("locks")
        self.profile = PROFILE.default_profile()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def directory(self, name: str) -> Path:
        path = self.root / name
        path.mkdir(mode=0o700)
        return path

    def hot_root(self) -> dict[str, object]:
        return {
            "registration_path": str(self.root / "root-registration.json"),
            "registration_sha256": "1" * 64,
            "registration_id": "gpurootreg_" + "2" * 32,
            "identity_sha256": "2" * 64,
            "root_id": "himr-hot-v1",
            "path": str(self.root),
            "filesystem_uuid": "11111111-1111-1111-1111-111111111111",
            "tier": "hot_main_drive",
        }

    def runtime(self, status: str = "candidate") -> dict[str, object]:
        return {
            "receipt_path": str(self.root / "runtime.json"),
            "receipt_sha256": "3" * 64,
            "receipt_id": "gpurtv2_" + "4" * 32,
            "identity_sha256": "4" * 64,
            "status": status,
        }

    def profile_reference(self) -> dict[str, object]:
        return {
            "path": str(self.root / "profile.json"),
            "sha256": "5" * 64,
            "profile_id": self.profile["profile_id"],
            "identity_sha256": self.profile["identity_sha256"],
        }

    def synthetic_lineage(self) -> dict[str, object]:
        return {
            "kind": V5.SOURCE_LINEAGE_SYNTHETIC,
            "fixture_manifest": {
                "path": str(self.root / "fixture.json"),
                "sha256": "6" * 64,
                "identity_sha256": "7" * 64,
                "fixture_id": "fixture-1",
            },
            "fixture_case_id": "case-1",
            "contains_corpus_media": False,
            "scope": "purpose_built_synthetic_only",
            "corpus_authority": "none",
        }

    def production_lineage(self) -> dict[str, object]:
        return {
            "kind": V5.SOURCE_LINEAGE_PRODUCTION,
            "implementation_version": "0.3.0",
            "gpu_handoff": {
                "manifest_path": str(self.root / "handoff.json"),
                "manifest_sha256": "7" * 64,
                "queue_id": "gpuasrqueue_" + "8" * 32,
                "identity_sha256": "8" * 64,
                "member_id": "gpuasrmember_" + "9" * 32,
                "member_identity_sha256": "9" * 64,
                "queue_ordinal": 1,
                "preprocess_ordinal": 1,
            },
            "bundle_manifest": {
                "path": str(self.root / "bundle.json"),
                "sha256": "8" * 64,
                "bundle_id": "ppbatch_" + "9" * 32,
                "identity_sha256": "9" * 64,
                "manifest_sha256": "c" * 64,
            },
            "receipt": {
                "path": str(self.root / "receipt.json"),
                "sha256": "d" * 64,
                "receipt_id": "ppreceipt_" + "e" * 32,
                "receipt_sha256": "e" * 64,
                "ordinal": 1,
            },
            "preprocess_result": {
                "path": str(self.root / "preprocess-result.json"),
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

    def order(self, ordinal: int = 1, *, production: bool = False) -> dict[str, object]:
        digest = f"{ordinal:x}" * 64
        digest = digest[:64]
        core = {
            "kind": V5.WORK_ORDER_KIND,
            "schema_version": V5.CONTRACT_VERSION,
            "implementation_version": V5.IMPLEMENTATION_VERSION,
            "job_id": f"gpu-asr-test-{ordinal}",
            "input": {
                "path": str(self.root / f"audio-{ordinal}.flac"),
                "expected_sha256": digest,
                "expected_byte_count": 1024 + ordinal,
                "expected_duration_ms": 1000 + ordinal,
                "media_id": f"media_sha256_{digest}",
                "artifact_id": "artifact_" + digest[:32],
                "parent_processing_run_id": "processing_run_1",
                "media_format": dict(V5.MEDIA_FORMAT),
                "sealed_mode": "0400",
                "timeline_offset_ms": 0,
            },
            "source_lineage": self.production_lineage() if production else self.synthetic_lineage(),
            "runtime_admission": self.runtime("admitted" if production else "candidate"),
            "production_profile": self.profile_reference(),
            "hot_root": self.hot_root(),
            "execution_contract": V5.execution_contract_from_profile(self.profile),
            "transcript_semantics": copy.deepcopy(V5.TRANSCRIPT_SEMANTICS),
            "catalog_context": None,
            "output": {"root": str(self.result_root), "layout": V5.OUTPUT_LAYOUT, "atomic_no_replace": True},
            "policy": dict(V5.POLICY),
        }
        return V5.make_work_order(core, profile_document=self.profile)

    def manifest(self, count: int = 2) -> dict[str, object]:
        records = []
        for ordinal in range(1, count + 1):
            order = self.order(ordinal)
            body = BATCH.canonical_bytes(order)
            records.append((self.root / f"work-{ordinal}.json", order, body))
        return BATCH.make_manifest(
            work_order_records=records,
            profile=self.profile,
            batch_root=self.batch_root,
            event_root=self.event_root,
            lock_root=self.lock_root,
        )

    def strict_fixture_order(self) -> tuple[dict[str, object], dict[str, object]]:
        order = self.order()
        fixture = {
            "kind": "himr_synthetic_audio_fixture",
            "schema_version": 1,
            "created_at": "2026-08-29T12:00:00Z",
            "synthetic": True,
            "corpus_evidence": False,
            "generator": {
                "name": "cpu-test-tone",
                "version": "1.0.0",
                "executable": "/usr/bin/espeak-ng",
                "executable_sha256": "a" * 64,
                "text": "Synthetic CPU test with no corpus evidence.",
                "voice": "en-us",
                "speed_words_per_minute": 145,
                "command": ["/usr/bin/espeak-ng", "synthetic CPU test"],
                "wav_sha256": "b" * 64,
                "wav_byte_count": 4096,
            },
            "normalization": {
                "name": "ffmpeg",
                "version": "8.1.2",
                "executable": "/usr/bin/ffmpeg",
                "executable_sha256": "c" * 64,
                "command": ["/usr/bin/ffmpeg", "-i", "synthetic.wav"],
                "codec": "flac",
                "sample_rate_hz": 16000,
                "channels": 1,
                "sample_format": "s16",
                "duration_seconds": order["input"]["expected_duration_ms"] / 1000,
            },
            "artifact": {
                "path": order["input"]["path"],
                "sha256": order["input"]["expected_sha256"],
                "byte_count": order["input"]["expected_byte_count"],
            },
            "policy": {
                "catalogue_authority": "none",
                "identity_authority": "none",
                "publication_authority": "none",
                "wiki_authority": "none",
                "existing_corpus_asr_rerun": False,
            },
        }
        body = BATCH.canonical_bytes(fixture)
        digest = BATCH.sha256_bytes(body)
        core = {key: order[key] for key in V5.WORK_ORDER_CORE_KEYS}
        core["source_lineage"] = {
            **order["source_lineage"],
            "fixture_manifest": {
                "path": str(self.root / "fixture.json"),
                "sha256": digest,
                "identity_sha256": digest,
                "fixture_id": "fixture-1",
            },
        }
        return V5.make_work_order(core, profile_document=self.profile), fixture

    def launch_attestation(
        self,
        manifest: dict[str, object],
        lineage_preflight: dict[str, object],
        lineage_sha256: str,
    ) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
        runtime = self.runtime("candidate")
        root = {
            "registration_id": manifest["hot_root"]["registration_id"],
            "identity_sha256": manifest["hot_root"]["identity_sha256"],
            "root_id": manifest["hot_root"]["root_id"],
            "filesystem": {"uuid": manifest["hot_root"]["filesystem_uuid"]},
        }
        top_runtime = {
            "path": runtime["receipt_path"],
            "sha256": runtime["receipt_sha256"],
            "identity_sha256": runtime["identity_sha256"],
            "status": runtime["status"],
        }
        top_profile = {
            "path": manifest["production_profile"]["path"],
            "sha256": manifest["production_profile"]["sha256"],
            "identity_sha256": self.profile["identity_sha256"],
            "gpu_uuid": self.profile["hardware"]["gpu_uuid"],
        }
        top_root = {
            "path": manifest["hot_root"]["registration_path"],
            "sha256": manifest["hot_root"]["registration_sha256"],
            "identity_sha256": root["identity_sha256"],
            "registration_id": root["registration_id"],
            "root_id": root["root_id"],
            "filesystem_uuid": root["filesystem"]["uuid"],
        }
        batch_sha = BATCH.sha256_bytes(BATCH.canonical_bytes(manifest))
        top_batch = {
            "path": str(self.root / "batch.json"),
            "sha256": batch_sha,
            "execution_class": manifest["execution_class"],
            "identity_sha256": manifest["identity_sha256"],
        }
        top_lineage = {
            "path": str(self.root / "lineage-preflight.json"),
            "sha256": lineage_sha256,
            "identity_sha256": lineage_preflight["identity_sha256"],
            "attestation_id": lineage_preflight["attestation_id"],
        }
        host_abi_identity = "9" * 64
        top_host_abi = {
            "identity_sha256": host_abi_identity,
            "manifest_id": f"gpuhostabi_{host_abi_identity[:32]}",
            "platform_replayed": True,
            "libraries_replayed": True,
            "library_count": 1,
            "binding_count": 1,
        }
        mappings = [
            {
                "name": name,
                "image_relative_path": f"mapping/{name}",
                "sandbox_path": layout[0],
                "role": layout[1],
            }
            for name, layout in sorted(BATCH.REQUIRED_MAPPING_LAYOUT.items())
        ]
        item_bindings = []
        for ordinal, member in enumerate(manifest["items"], 1):
            bindings = []
            for row in BATCH.expected_item_read_bindings(member["work_order"]):
                bindings.append(
                    {
                        **row,
                        "target": row["path"],
                        "byte_count": row["byte_count"] or 123,
                        "mode": row["mode"] or "0400",
                    }
                )
            item_bindings.append(
                {
                    "ordinal": ordinal,
                    "work_order_identity_sha256": member["work_order"]["identity_sha256"],
                    "bindings": bindings,
                }
            )
        plan = {
            "mappings": mappings,
            "hot_root": {"path": manifest["hot_root"]["path"], "bound": False},
            "batch_manifest": {"path": top_batch["path"], "sha256": top_batch["sha256"], "identity_sha256": top_batch["identity_sha256"], "target": "/run/himr-gpu/input/batch.json"},
            "controls": {
                "runtime": {"path": top_runtime["path"], "sha256": top_runtime["sha256"], "identity_sha256": top_runtime["identity_sha256"], "target": "/run/himr-gpu/control/runtime.json", "work_order_target": manifest["runtime_admission"]["receipt_path"]},
                "profile": {"path": top_profile["path"], "sha256": top_profile["sha256"], "identity_sha256": top_profile["identity_sha256"], "target": "/run/himr-gpu/control/profile.json", "work_order_target": manifest["production_profile"]["path"]},
                "root": {"path": top_root["path"], "sha256": top_root["sha256"], "identity_sha256": top_root["identity_sha256"], "target": "/run/himr-gpu/control/root.json", "work_order_target": manifest["hot_root"]["registration_path"]},
                "lineage_preflight": {"path": top_lineage["path"], "sha256": top_lineage["sha256"], "identity_sha256": top_lineage["identity_sha256"], "target": "/run/himr-gpu/control/lineage-preflight.json", "work_order_target": None},
            },
            "item_read_bindings": item_bindings,
            "writable_roots": {
                name: {"source": path, "sandbox": path}
                for name, path in manifest["writable_roots"].items()
                if name in {"result", "event", "lock"}
            },
            "gpu": {
                "uuid": self.profile["hardware"]["gpu_uuid"],
                "host_index": 0,
                "device_minor": 0,
                "visible_index": 0,
                "devices": ["/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools", "/dev/nvidia0"],
                "driver_version": self.profile["hardware"]["minimum_driver_version"],
                "compute_capability": self.profile["hardware"]["minimum_compute_capability"],
                "minimum_driver_version": self.profile["hardware"]["minimum_driver_version"],
                "minimum_compute_capability": self.profile["hardware"]["minimum_compute_capability"],
            },
            "host_abi_bindings": [
                {
                    "role": "host_abi_library",
                    "source_path": "/usr/lib64/libc.so.6",
                    "target": "/usr/lib64/libc.so.6",
                    "sha256": "8" * 64,
                    "byte_count": 1024,
                    "uid": 0,
                    "gid": 0,
                    "mode": "0755",
                }
            ],
            "system_library_directories": [],
            "system_readonly_files": [],
            "namespaces": ["cgroup_try", "ipc", "network", "pid", "user", "uts"],
            "network_access": False,
        }
        maximum_result = self.profile["item_limits"]["maximum_result_bytes"]
        host_envelope = {
            "cgroup_version": 2,
            "cgroup_path": "/test.slice",
            "effective": {"memory_max_bytes": 1024**3, "memory_swap_max_bytes": 0, "pids_max": 64},
            "rlimits": {
                "core": {"soft": 0, "hard": 0},
                "fsize": {"soft": maximum_result, "hard": maximum_result},
                "nofile": {"soft": 1024, "hard": 1024},
            },
            "requirements": {
                "memory_max_bytes_at_most": BATCH.MAX_HOST_MEMORY_BYTES,
                "memory_swap_max_bytes": 0,
                "pids_max_at_most": BATCH.MAX_HOST_PIDS,
                "nofile_at_most": BATCH.MAX_HOST_NOFILE,
                "core_bytes": 0,
                "fsize_bytes_at_least": maximum_result,
                "fsize_bytes_at_most": BATCH.MAX_HOST_FSIZE_BYTES,
            },
            "status": "passed",
            "violations": [],
        }
        core = {
            "kind": BATCH.ATTESTATION_KIND,
            "schema_version": 2,
            "implementation_version": BATCH.ATTESTATION_VERSION,
            "mode": "candidate-synthetic-canary",
            "nonce": "a" * 64,
            "launcher": {"path": str(self.root / "launcher.py"), "sha256": "b" * 64, "profile": {"path": str(self.root / "launcher-profile.json"), "sha256": "c" * 64, "identity_sha256": "d" * 64}},
            "runtime_admission": top_runtime,
            "root_registration": top_root,
            "execution_image": {"path": str(self.root / "image.squashfs"), "sha256": "e" * 64, "byte_count": 1024, "identity_sha256": "f" * 64, "receipt_path": str(self.root / "image-receipt.json"), "receipt_sha256": "0" * 64},
            "production_profile": top_profile,
            "batch": top_batch,
            "lineage_preflight": top_lineage,
            "host_abi": top_host_abi,
            "sandbox_plan": plan,
            "sandbox_plan_identity_sha256": BATCH.sha256_bytes(BATCH.canonical_bytes(plan)),
            "host_envelope": host_envelope,
            "parent_network_namespace": "net:[123]",
            "child_network_namespace_must_differ": True,
            "local_readiness": None,
            "trust_boundary": {
                "kind": "synthetic_candidate",
                "control_owner": "candidate_binding",
                "same_uid_mutation_resistance": False,
            },
            "host_trust_checks": {"runtime_receipt_owner_checked": True, "root_registration_replayed": True, "execution_image_full_sha256_checked": True, "launcher_and_tools_sha256_checked": True, "host_abi_manifest_replayed": True, "gpu_uuid_resolved_at_launch": True},
            "policy": dict(BATCH.LAUNCH_POLICY),
        }
        identity = BATCH.sha256_bytes(BATCH.canonical_bytes(core))
        return {**core, "identity_sha256": identity, "attestation_id": f"gpulaunch_{identity[:32]}"}, runtime, root

    @staticmethod
    def histogram(minimum: int, maximum: int, *, width: int = 1) -> dict[str, object]:
        return {"sample_count": 4, "minimum": minimum, "mean": (minimum + maximum) / 2, "p50": minimum, "p95": maximum, "maximum": maximum, "bin_width": width}

    @staticmethod
    def fake_av(duration_ms: int = 1001, *, on_open: object | None = None) -> SimpleNamespace:
        codec = SimpleNamespace(
            name="flac",
            sample_rate=16000,
            channels=1,
            format=SimpleNamespace(name="s16"),
            layout=SimpleNamespace(name="mono"),
        )
        stream = SimpleNamespace(
            type="audio",
            duration=duration_ms,
            time_base=0.001,
            codec_context=codec,
        )

        def open_container(path: str, *, mode: str) -> SimpleNamespace:
            if on_open is not None:
                on_open(path, mode)
            return SimpleNamespace(
                streams=[stream],
                duration=duration_ms * 1000,
                format=SimpleNamespace(name="flac"),
                close=lambda: None,
            )

        return SimpleNamespace(open=open_container, time_base=1_000_000, __version__="18.1.0")

    def telemetry(self) -> dict[str, object]:
        limits = self.profile["telemetry"]
        return {
            "implementation_version": "0.1.0",
            "fast_interval_seconds": 0.05,
            "slow_interval_seconds": 0.25,
            "fast_sample_count": 20,
            "slow_sample_count": 4,
            "sample_span_seconds": 1.0,
            "last_fast_sample_age_seconds": 0.01,
            "last_slow_sample_age_seconds": 0.02,
            "process_vram_measurement_seen": True,
            "process_peak_used_bytes": 1024,
            "global_peak_used_bytes": 2048,
            "minimum_free_bytes": limits["minimum_free_vram_bytes"] + 1,
            "utilization_percent": self.histogram(10, 50),
            "memory_controller_utilization_percent": self.histogram(5, 30),
            "active_sample_fraction": 1.0,
            "temperature_c": self.histogram(40, 50),
            "power_mw": self.histogram(10_000, 30_000, width=1000),
            "estimated_energy_millijoules": 20_000.0,
            "sm_clock_mhz": self.histogram(200, 900, width=10),
            "throttle_reasons_bitmask_or": 0,
            "sampler_error": None,
        }

    def test_manifest_is_deterministic_and_embeds_full_orders(self) -> None:
        first = self.manifest()
        second = self.manifest()
        self.assertEqual(first, second)
        self.assertEqual(first["execution_class"], "synthetic_canary")
        self.assertEqual(first["items"][0]["work_order"]["kind"], V5.WORK_ORDER_KIND)
        self.assertNotIn("source_work_order", first["items"][0])
        self.assertEqual(BATCH.validate_manifest(first, profile=self.profile), first)

    def test_manifest_rejects_mixed_execution_classes(self) -> None:
        records = []
        for ordinal, production in ((1, False), (2, True)):
            order = self.order(ordinal, production=production)
            records.append((self.root / f"work-{ordinal}.json", order, BATCH.canonical_bytes(order)))
        with self.assertRaisesRegex(BATCH.BatchV2Error, "may not be mixed"):
            BATCH.make_manifest(
                work_order_records=records,
                profile=self.profile,
                batch_root=self.batch_root,
                event_root=self.event_root,
                lock_root=self.lock_root,
            )

    def test_candidate_runtime_production_lineage_gets_local_private_class(self) -> None:
        order = self.order(1, production=True)
        core = {key: order[key] for key in V5.WORK_ORDER_CORE_KEYS}
        core["runtime_admission"] = self.runtime("candidate")
        order = V5.make_work_order(core, profile_document=self.profile)
        body = BATCH.canonical_bytes(order)
        manifest = BATCH.make_manifest(
            work_order_records=[(self.root / "local-work.json", order, body)],
            profile=self.profile,
            batch_root=self.batch_root,
            event_root=self.event_root,
            lock_root=self.lock_root,
        )
        self.assertEqual(
            manifest["execution_class"], BATCH.EXECUTION_CLASS_LOCAL_PRIVATE
        )
        self.assertEqual(BATCH.validate_manifest(manifest, profile=self.profile), manifest)

    def test_manifest_identity_tamper_is_rejected(self) -> None:
        manifest = self.manifest()
        manifest["totals"]["audio_duration_ms"] += 1
        with self.assertRaisesRegex(BATCH.BatchV2Error, "totals"):
            BATCH.validate_manifest(manifest, profile=self.profile)

    def test_manifest_rejects_nested_or_role_confused_writable_roots(self) -> None:
        manifest = self.manifest()
        manifest["writable_roots"]["result"] = manifest["writable_roots"]["event"]
        with self.assertRaisesRegex(BATCH.BatchV2Error, "distinct and non-nested"):
            BATCH.validate_manifest(manifest, profile=self.profile)

    def test_manifest_rejects_more_than_profile_maximum(self) -> None:
        records = []
        for ordinal in range(1, BATCH.MAX_ITEMS + 2):
            order = self.order(ordinal)
            records.append((self.root / f"work-{ordinal}.json", order, BATCH.canonical_bytes(order)))
        with self.assertRaisesRegex(BATCH.BatchV2Error, "item count"):
            BATCH.make_manifest(
                work_order_records=records,
                profile=self.profile,
                batch_root=self.batch_root,
                event_root=self.event_root,
                lock_root=self.lock_root,
            )

    def test_item_read_bindings_are_exact_and_sorted(self) -> None:
        synthetic = BATCH.expected_item_read_bindings(self.order())
        self.assertEqual([row["role"] for row in synthetic], ["fixture_manifest", "input_audio"])
        production = BATCH.expected_item_read_bindings(self.order(production=True))
        self.assertEqual(
            [row["role"] for row in production],
            ["bundle_manifest", "gpu_handoff_manifest", "input_audio", "preprocess_receipt", "preprocess_result"],
        )

    def test_strict_synthetic_fixture_and_lineage_attestation_replay(self) -> None:
        order, fixture = self.strict_fixture_order()
        fixture_path = self.root / "fixture.json"
        fixture_path.write_bytes(BATCH.canonical_bytes(fixture))
        fixture_path.chmod(0o400)
        manifest = BATCH.make_manifest(
            work_order_records=[(self.root / "work.json", order, BATCH.canonical_bytes(order))],
            profile=self.profile,
            batch_root=self.batch_root,
            event_root=self.event_root,
            lock_root=self.lock_root,
        )
        root_registration = {
            "registration_id": order["hot_root"]["registration_id"],
            "identity_sha256": order["hot_root"]["identity_sha256"],
            "filesystem": {"uuid": order["hot_root"]["filesystem_uuid"]},
        }
        manifest_sha = BATCH.sha256_bytes(BATCH.canonical_bytes(manifest))
        value = BATCH.make_lineage_preflight(
            manifest=manifest,
            manifest_physical_sha256=manifest_sha,
            profile=self.profile,
            root_registration=root_registration,
            root_registration_path=Path(order["hot_root"]["registration_path"]),
            root_registration_sha256=order["hot_root"]["registration_sha256"],
        )
        self.assertEqual(
            BATCH.validate_lineage_preflight(
                value,
                manifest=manifest,
                manifest_physical_sha256=manifest_sha,
                profile=self.profile,
                root_registration=root_registration,
            ),
            value,
        )
        tampered = copy.deepcopy(value)
        tampered["items"][0]["lineage"]["case_id"] = "case-2"
        with self.assertRaisesRegex(BATCH.BatchV2Error, "differs from v5"):
            BATCH.validate_lineage_preflight(
                tampered,
                manifest=manifest,
                manifest_physical_sha256=manifest_sha,
                profile=self.profile,
                root_registration=root_registration,
            )

    def test_production_lineage_uses_sealed_queue_projection_without_upstream_runtime(self) -> None:
        order = self.order(production=True)
        handoff = order["source_lineage"]["gpu_handoff"]
        entry = {
            "member_id": handoff["member_id"],
            "identity_sha256": handoff["member_identity_sha256"],
            "ordinal": handoff["queue_ordinal"],
            "preprocess_ordinal": handoff["preprocess_ordinal"],
        }
        queue = {
            "queue_id": handoff["queue_id"],
            "identity_sha256": handoff["identity_sha256"],
            "members": [entry],
            "fixture": "sealed-only",
        }
        body = BATCH.canonical_bytes(queue)
        queue_path = self.root / "handoff.json"
        queue_path.write_bytes(body)
        queue_path.chmod(0o400)
        handoff["manifest_path"] = str(queue_path)
        handoff["manifest_sha256"] = BATCH.sha256_bytes(body)
        with (
            mock.patch.object(
                V5,
                "source_lineage_from_preprocess_descriptor",
                return_value=order["source_lineage"],
            ) as source_projection,
            mock.patch.object(
                V5,
                "input_from_preprocess_descriptor",
                return_value=order["input"],
            ),
            mock.patch.object(
                V5,
                "hot_root_from_gpu_queue_manifest",
                return_value=order["hot_root"],
            ),
            mock.patch.object(
                V5,
                "profile_from_gpu_queue_manifest",
                return_value=(order["production_profile"], self.profile),
            ),
        ):
            summary = BATCH._deep_validate_production_order(
                order,
                profile=self.profile,
                queue_cache={},
            )
        self.assertEqual(summary["source_id"], handoff["queue_id"])
        source_projection.assert_called_once_with(entry, queue)

        queue_path.chmod(0o444)
        with self.assertRaisesRegex(BATCH.BatchV2Error, "metadata is unsafe"):
            BATCH._deep_validate_production_order(
                order,
                profile=self.profile,
                queue_cache={},
            )

    def test_existing_smoke_fixture_schema_replays_strictly(self) -> None:
        fixture_path = ROOT / "research/corpus/gpu-runtime/asr-smoke-20260828T2319Z/fixture-manifest.json"
        if not fixture_path.is_file():
            self.skipTest("existing synthetic smoke fixture is absent")
        body = fixture_path.read_bytes()
        fixture = BATCH.parse_json(body, "existing fixture")
        digest = BATCH.sha256_bytes(body)
        order = self.order()
        order["input"]["path"] = fixture["artifact"]["path"]
        order["input"]["expected_sha256"] = fixture["artifact"]["sha256"]
        order["input"]["expected_byte_count"] = fixture["artifact"]["byte_count"]
        order["input"]["expected_duration_ms"] = round(fixture["normalization"]["duration_seconds"] * 1000)
        order["source_lineage"]["fixture_manifest"] = {
            "path": str(fixture_path),
            "sha256": digest,
            "identity_sha256": digest,
            "fixture_id": "legacy-smoke-fixture",
        }
        order["source_lineage"]["fixture_case_id"] = "legacy-smoke-case"
        summary = BATCH._strict_fixture(order, body=body, value=fixture)
        self.assertEqual(summary["identity_sha256"], digest)
        self.assertEqual(summary["case_id"], "legacy-smoke-case")

    def test_launch_attestation_exactly_cross_checks_phase_one_and_plan(self) -> None:
        manifest = self.manifest()
        lineage_identity = "a" * 64
        lineage = {
            "identity_sha256": lineage_identity,
            "attestation_id": f"gpuasrlineage2_{lineage_identity[:32]}",
        }
        lineage_sha = "b" * 64
        attestation, runtime, root = self.launch_attestation(
            manifest, lineage, lineage_sha
        )
        manifest_sha = BATCH.sha256_bytes(BATCH.canonical_bytes(manifest))
        self.assertEqual(
            BATCH.validate_launch_attestation(
                attestation,
                manifest=manifest,
                manifest_sha256=manifest_sha,
                runtime=runtime,
                runtime_sha256=runtime["receipt_sha256"],
                profile=self.profile,
                profile_sha256=manifest["production_profile"]["sha256"],
                root=root,
                root_sha256=manifest["hot_root"]["registration_sha256"],
                lineage_preflight=lineage,
                lineage_preflight_sha256=lineage_sha,
            ),
            attestation,
        )
        tampered = copy.deepcopy(attestation)
        tampered["lineage_preflight"]["sha256"] = "c" * 64
        core = {
            key: value
            for key, value in tampered.items()
            if key not in {"identity_sha256", "attestation_id"}
        }
        identity = BATCH.sha256_bytes(BATCH.canonical_bytes(core))
        tampered["identity_sha256"] = identity
        tampered["attestation_id"] = f"gpulaunch_{identity[:32]}"
        with self.assertRaisesRegex(BATCH.BatchV2Error, "lineage preflight differs"):
            BATCH.validate_launch_attestation(
                tampered,
                manifest=manifest,
                manifest_sha256=manifest_sha,
                runtime=runtime,
                runtime_sha256=runtime["receipt_sha256"],
                profile=self.profile,
                profile_sha256=manifest["production_profile"]["sha256"],
                root=root,
                root_sha256=manifest["hot_root"]["registration_sha256"],
                lineage_preflight=lineage,
                lineage_preflight_sha256=lineage_sha,
            )

    def test_candidate_host_envelope_accepts_null_only_as_reported_violation(self) -> None:
        maximum_result = self.profile["item_limits"]["maximum_result_bytes"]
        envelope = {
            "cgroup_version": 2,
            "cgroup_path": "/test.slice",
            "effective": {"memory_max_bytes": None, "memory_swap_max_bytes": None, "pids_max": None},
            "rlimits": {
                "core": {"soft": None, "hard": None},
                "fsize": {"soft": None, "hard": None},
                "nofile": {"soft": None, "hard": None},
            },
            "requirements": {
                "memory_max_bytes_at_most": BATCH.MAX_HOST_MEMORY_BYTES,
                "memory_swap_max_bytes": 0,
                "pids_max_at_most": BATCH.MAX_HOST_PIDS,
                "nofile_at_most": BATCH.MAX_HOST_NOFILE,
                "core_bytes": 0,
                "fsize_bytes_at_least": maximum_result,
                "fsize_bytes_at_most": BATCH.MAX_HOST_FSIZE_BYTES,
            },
            "status": "report_only_failed",
            "violations": ["RLIMIT_CORE", "RLIMIT_FSIZE", "RLIMIT_NOFILE", "memory.max", "memory.swap.max", "pids.max"],
        }
        self.assertEqual(
            BATCH.validate_host_envelope(envelope, self.profile, enforce=False),
            envelope,
        )
        with self.assertRaisesRegex(BATCH.BatchV2Error, "host envelope failed"):
            BATCH.validate_host_envelope(envelope, self.profile, enforce=True)

    def test_raw_normalization_preserves_timing_anomalies(self) -> None:
        order = self.order()
        segment = SimpleNamespace(
            id=0,
            seek=0,
            start=1.0,
            end=2.0,
            text=" hello",
            tokens=[1],
            temperature=0.0,
            avg_logprob=-0.2,
            compression_ratio=1.0,
            no_speech_prob=0.1,
            words=[
                SimpleNamespace(start=0.9, end=1.3, word=" hello", probability=0.8),
                SimpleNamespace(start=1.2, end=2.1, word=" world", probability=0.7),
            ],
        )
        rows = [BATCH.raw_segment(segment, 0, self.profile)]
        runtime = {"runtime": {"packages": {"faster-whisper": "1.2.3"}}}
        raw = BATCH.build_raw_transcript(SimpleNamespace(duration=1.001, duration_after_vad=None), rows, order, self.profile, runtime)
        normalized = BATCH.normalize_raw_transcript(raw, order, self.profile)
        flags = normalized["word_timing_anomalies"]["flag_counts"]
        self.assertEqual(flags["precedes_segment_start"], 1)
        self.assertEqual(flags["overlaps_previous"], 1)
        self.assertEqual(flags["extends_beyond_segment_end"], 1)
        self.assertEqual(normalized["word_count"], 2)

    def test_make_item_result_is_exact_v5(self) -> None:
        order = self.order()
        segment = SimpleNamespace(id=0, seek=0, start=0.0, end=1.0, text=" hello", tokens=[], temperature=0.0, avg_logprob=-0.1, compression_ratio=1.0, no_speech_prob=0.0, words=[])
        runtime = {"runtime": {"packages": {"faster-whisper": "1.2.3"}}}
        raw = BATCH.build_raw_transcript(SimpleNamespace(duration=1.0, duration_after_vad=None), [BATCH.raw_segment(segment, 0, self.profile)], order, self.profile, runtime)
        normalized = BATCH.normalize_raw_transcript(raw, order, self.profile)
        retained = SimpleNamespace(work_order=order, hash_seconds=0.01, probe_seconds=0.02)
        computed = BATCH.ComputedItem(retained, "2026-08-29T12:00:00Z", "2026-08-29T12:00:01Z", 1.0, 1.5, 0.1, 0.2, 0.05, 0.01, raw, normalized, BATCH.canonical_bytes(raw), BATCH.canonical_bytes(normalized))
        result, body = BATCH.make_item_result(computed, profile=self.profile, telemetry=self.telemetry(), runtime=runtime, attempt_id="gpuasrattempt_" + "a" * 32, model_load_seconds=0.1)
        self.assertEqual(V5.validate_result(result, work_order=order, profile_document=self.profile), result)
        self.assertEqual(body, BATCH.canonical_bytes(result))
        self.assertEqual(result["execution"]["model_load_count"], 1)

    def test_execute_batch_loads_one_model_runs_two_calls_and_replays_results(self) -> None:
        records = []
        for ordinal in (1, 2):
            content = (f"sealed-audio-{ordinal}".encode() * 16)
            path = self.root / f"audio-{ordinal}.flac"
            path.write_bytes(content)
            path.chmod(0o400)
            base = self.order(ordinal)
            digest = BATCH.sha256_bytes(content)
            core = {key: base[key] for key in V5.WORK_ORDER_CORE_KEYS}
            core["input"] = {
                **base["input"],
                "expected_sha256": digest,
                "expected_byte_count": len(content),
                "expected_duration_ms": 1001,
                "media_id": f"media_sha256_{digest}",
                "artifact_id": f"artifact_{digest[:32]}",
            }
            order = V5.make_work_order(core, profile_document=self.profile)
            records.append((self.root / f"work-{ordinal}.json", order, BATCH.canonical_bytes(order)))
        manifest = BATCH.make_manifest(
            work_order_records=records,
            profile=self.profile,
            batch_root=self.batch_root,
            event_root=self.event_root,
            lock_root=self.lock_root,
        )
        runtime = {
            "status": "candidate",
            "identity_sha256": records[0][1]["runtime_admission"]["identity_sha256"],
            "runtime": {"packages": {"faster-whisper": "1.2.3", "av": "18.1.0"}},
        }
        attestation: dict[str, object] = {}
        attestation_body = BATCH.canonical_bytes(attestation)
        attestation_sha = BATCH.sha256_bytes(attestation_body)
        args = SimpleNamespace(
            launch_attestation=str(self.root / "launch.json"),
            expected_launch_attestation_sha256=attestation_sha,
        )
        model_observation = {"loads": 0, "active": 0, "peak": 0, "calls": []}
        active_lock = __import__("threading").Lock()

        class FakeModel:
            def __init__(inner_self, *model_args: object, **model_kwargs: object) -> None:
                del inner_self
                model_observation["loads"] += 1
                self.assertEqual(model_kwargs["num_workers"], 2)
                self.assertEqual(model_kwargs["device_index"], 0)
                self.assertEqual(model_args, ("/opt/himr-gpu/model/snapshot",))

            def transcribe(inner_self, fd_path: str, **kwargs: object) -> tuple[list[SimpleNamespace], SimpleNamespace]:
                del inner_self
                with active_lock:
                    model_observation["active"] += 1
                    model_observation["peak"] = max(model_observation["peak"], model_observation["active"])
                try:
                    time.sleep(0.01)
                    self.assertEqual(kwargs["vad_filter"], False)
                    model_observation["calls"].append(fd_path)
                    segment = SimpleNamespace(id=0, seek=0, start=0.0, end=1.001, text=" synthetic", tokens=[], temperature=0.0, avg_logprob=-0.1, compression_ratio=1.0, no_speech_prob=0.0, words=[])
                    return [segment], SimpleNamespace(duration=1.001, duration_after_vad=None)
                finally:
                    with active_lock:
                        model_observation["active"] -= 1

        class FakeSampler:
            def __init__(inner_self, *_args: object, **_kwargs: object) -> None:
                inner_self.started = False

            def start(inner_self) -> None:
                inner_self.started = True

            def snapshot(inner_self, **_kwargs: object) -> dict[str, object]:
                self.assertTrue(inner_self.started)
                return self.telemetry()

            def stop(inner_self) -> None:
                inner_self.started = False

        pynvml = SimpleNamespace(
            nvmlInit=lambda: None,
            nvmlShutdown=lambda: None,
            nvmlDeviceGetHandleByUUID=lambda _uuid: "handle",
            nvmlDeviceGetUUID=lambda _handle: self.profile["hardware"]["gpu_uuid"],
            nvmlDeviceGetComputeRunningProcesses=lambda _handle: [],
            nvmlDeviceGetGraphicsRunningProcesses=lambda _handle: [],
            nvmlDeviceGetMemoryInfo=lambda _handle: SimpleNamespace(total=6 * 1024**3, used=1024**3, free=5 * 1024**3),
        )
        control_return = (manifest, self.profile, runtime, attestation, {}, BATCH.canonical_bytes(manifest))
        with mock.patch.object(BATCH, "_load_controls", return_value=control_return), mock.patch.object(BATCH, "network_isolation", return_value={"verified": True}), mock.patch.object(BATCH, "load_canonical_document", return_value=(attestation, attestation_body)), mock.patch.object(BATCH.TELEMETRY, "NVMLTelemetrySampler", FakeSampler), mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": self.profile["hardware"]["gpu_uuid"], "HF_HUB_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1"}, clear=False):
            completion = BATCH.execute_batch(
                args,
                model_class=FakeModel,
                pynvml=pynvml,
                av_module=self.fake_av(),
                fatal=lambda code: self.fail(f"unexpected watchdog exit {code}"),
            )
        self.assertEqual(completion["model_load_count"], 1)
        self.assertEqual(model_observation["loads"], 1)
        self.assertEqual(model_observation["peak"], 2)
        self.assertEqual(len(model_observation["calls"]), 2)
        self.assertEqual([row["ordinal"] for row in completion["items"]], [1, 2])
        for _path, order, _body in records:
            replay = BATCH.replay_completed_result(order, profile=self.profile)
            self.assertEqual(replay["result"]["status"], "completed")

    def test_retain_input_hashes_and_probes_retained_fd(self) -> None:
        content = b"fLaC" + b"x" * 100
        path = self.root / "audio-1.flac"
        path.write_bytes(content)
        path.chmod(0o400)
        order = self.order()
        order["input"]["expected_sha256"] = BATCH.sha256_bytes(content)
        order["input"]["expected_byte_count"] = len(content)
        order["input"]["expected_duration_ms"] = 1001

        def on_open(path: str, mode: str) -> None:
            self.assertIn("/proc/self/fd/", path)
            self.assertEqual(mode, "r")

        retained = BATCH.retain_and_preflight_input(
            1, order, av_module=self.fake_av(on_open=on_open)
        )
        try:
            retained.verify()
            self.assertEqual(retained.info.st_size, len(content))
        finally:
            retained.close()

    def test_original_mutation_after_preflight_cannot_change_inference_memfd(self) -> None:
        original = b"fLaC" + b"a" * 100
        replacement = b"fLaC" + b"b" * 100
        path = self.root / "audio-1.flac"
        path.write_bytes(original)
        path.chmod(0o400)
        order = self.order()
        order["input"]["expected_sha256"] = BATCH.sha256_bytes(original)
        order["input"]["expected_byte_count"] = len(original)

        retained = BATCH.retain_and_preflight_input(
            1, order, av_module=self.fake_av()
        )
        try:
            path.chmod(0o600)
            path.write_bytes(replacement)
            self.assertEqual(os.pread(retained.descriptor, len(original), 0), original)
            retained.verify()
        finally:
            retained.close()

    def test_pyav_probe_rejects_duration_drift_and_extra_streams(self) -> None:
        descriptor = os.memfd_create("probe-test", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        try:
            os.write(descriptor, b"x")
            fcntl.fcntl(descriptor, BATCH.F_ADD_SEALS, BATCH.MEMFD_REQUIRED_SEALS)
            order = self.order()
            with self.assertRaisesRegex(BATCH.BatchV2Error, "duration differs"):
                BATCH._probe_fd((descriptor, order), av_module=self.fake_av(999))
            with self.assertRaisesRegex(BATCH.BatchV2Error, "version differs"):
                BATCH._probe_fd(
                    (descriptor, order),
                    av_module=self.fake_av(),
                    expected_av_version="17.0.0",
                )
            bad = self.fake_av()
            original_open = bad.open

            def open_with_video(path: str, *, mode: str) -> SimpleNamespace:
                container = original_open(path, mode=mode)
                container.streams.append(SimpleNamespace(type="video"))
                return container

            bad.open = open_with_video
            with self.assertRaisesRegex(BATCH.BatchV2Error, "no other streams"):
                BATCH._probe_fd((descriptor, order), av_module=bad)
        finally:
            os.close(descriptor)

    def test_unsealed_memfd_is_rejected(self) -> None:
        descriptor = os.memfd_create("unsealed-test", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        try:
            os.write(descriptor, b"x")
            os.fchmod(descriptor, 0o400)
            retained = BATCH.RetainedInput(
                1,
                self.order(),
                descriptor,
                os.fstat(descriptor),
                0.0,
                0.0,
                {},
                0,
            )
            with self.assertRaisesRegex(BATCH.BatchV2Error, "irrevocably sealed"):
                retained.verify()
        finally:
            os.close(descriptor)

    def test_bundled_python_memfd_uapi_round_trip(self) -> None:
        python = ROOT / "research/corpus/gpu-runtime/env/bin/python3.12"
        if not python.is_file():
            self.skipTest("admitted bundled CPython is not present in this checkout")
        worker = ROOT / "pipeline/gpu/production_asr_batch_v2.py"
        script = f"""
import fcntl, hashlib, importlib.util, os, pathlib, sys
path = pathlib.Path({str(worker)!r})
spec = importlib.util.spec_from_file_location('batch_bundled_cpu_test', path)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
assert not hasattr(module, '_queue_module')
source = os.memfd_create('source', os.MFD_CLOEXEC)
os.write(source, b'bundled-python-seal-test')
output, _, _ = module.sealed_memfd_from_source(
    source, os.fstat(source),
    expected_sha256=hashlib.sha256(b'bundled-python-seal-test').hexdigest(),
    label='bundled test',
)
assert module.F_ADD_SEALS == 1033 and module.F_GET_SEALS == 1034
assert fcntl.fcntl(output, module.F_GET_SEALS) == 15
try:
    os.write(output, b'x')
except OSError:
    pass
else:
    raise AssertionError('sealed memfd remained writable')
os.close(output)
os.close(source)
audio_path = pathlib.Path({str(ROOT / 'research/corpus/gpu-runtime/asr-smoke-20260828T2319Z/synthetic.flac')!r})
audio_body = audio_path.read_bytes()
source = os.open(audio_path, os.O_RDONLY | os.O_CLOEXEC)
sealed, _, _ = module.sealed_memfd_from_source(
    source, os.fstat(source),
    expected_sha256=hashlib.sha256(audio_body).hexdigest(),
    label='bundled PyAV test',
)
probe = module._probe_fd(
    (sealed, {{'input': {{'media_format': dict(module.V5.MEDIA_FORMAT), 'expected_duration_ms': 5945}}}})
)
assert probe['engine'] == 'pyav_authenticated_execution_image'
assert probe['engine_version'] == '18.1.0'
os.close(sealed)
os.close(source)
"""
        completed = subprocess.run(
            [str(python), "-B", "-I", "-c", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_failed_memfd_copy_closes_the_unsealed_candidate(self) -> None:
        source = self.root / "source.bin"
        source.write_bytes(b"abc")
        source.chmod(0o400)
        source_fd = os.open(source, os.O_RDONLY | os.O_CLOEXEC)
        created: list[int] = []
        real_create = BATCH.os.memfd_create

        def create(*args: object, **kwargs: object) -> int:
            descriptor = real_create(*args, **kwargs)
            created.append(descriptor)
            return descriptor

        try:
            with mock.patch.object(BATCH.os, "memfd_create", side_effect=create), mock.patch.object(BATCH.os, "write", side_effect=OSError(errno.EIO, "injected")):
                with self.assertRaises(OSError):
                    BATCH.sealed_memfd_from_source(
                        source_fd,
                        os.fstat(source_fd),
                        expected_sha256=BATCH.sha256_bytes(b"abc"),
                        label="test input",
                    )
            self.assertEqual(len(created), 1)
            with self.assertRaises(OSError):
                os.fstat(created[0])
        finally:
            os.close(source_fd)

    def test_retain_input_rejects_symlink(self) -> None:
        target = self.root / "target.flac"
        target.write_bytes(b"x")
        target.chmod(0o400)
        link = self.root / "audio-1.flac"
        link.symlink_to(target)
        with self.assertRaises(OSError):
            BATCH.retain_and_preflight_input(
                1, self.order(), av_module=self.fake_av()
            )

    def test_nonblocking_lock_reports_busy(self) -> None:
        path = self.lock_root / "gpu.lock"
        with BATCH.nonblocking_lock(path, "GPU"):
            with self.assertRaises(BATCH.GPUResourceBusy):
                with BATCH.nonblocking_lock(path, "GPU"):
                    pass

    def test_retained_chain_detects_parent_rename_before_commit(self) -> None:
        with BATCH.RetainedDirectory.open(self.result_root, "result root") as root:
            with self.assertRaisesRegex(BATCH.BatchV2Error, "link changed|escaped"):
                with BATCH.retained_private_chain(root, Path("a/b"), "result parent") as parent:
                    (self.result_root / "a").rename(self.result_root / "moved-a")
                    parent.verify()
                    BATCH._write_new_at(parent.descriptor, "must-not-exist.json", b"{}\n")
        self.assertFalse((self.result_root / "moved-a/b/must-not-exist.json").exists())

    def test_event_journal_detects_parent_rename_before_write(self) -> None:
        journal = BATCH.EventJournal(
            self.event_root,
            "gpuasrbatch2_" + "a" * 32,
            "gpuasrattempt_" + "b" * 32,
        )
        try:
            batch_dir = self.event_root / ("gpuasrbatch2_" + "a" * 32)
            moved = self.event_root / "moved-batch"
            batch_dir.rename(moved)
            with self.assertRaisesRegex(BATCH.BatchV2Error, "link changed|escaped"):
                journal.emit("must-not-write", {})
            self.assertFalse(any(moved.rglob("*must-not-write*")))
        finally:
            journal.close()

    def test_fixed_pair_groups_survive_partial_resume(self) -> None:
        values = [SimpleNamespace(ordinal=value) for value in (1, 3, 4, 6)]
        self.assertEqual(
            [[row.ordinal for row in pair] for pair in BATCH._pairs(values)],
            [[1], [3, 4], [6]],
        )

    def test_gpu_baseline_allows_compute_rows_that_are_graphics(self) -> None:
        current = SimpleNamespace(pid=os.getpid(), usedGpuMemory=100)
        desktop = SimpleNamespace(pid=44, usedGpuMemory=200)
        nvml = SimpleNamespace(
            nvmlDeviceGetComputeRunningProcesses=lambda _handle: [current, desktop],
            nvmlDeviceGetGraphicsRunningProcesses=lambda _handle: [desktop],
        )
        value = BATCH.gpu_process_baseline(nvml, object())
        self.assertEqual(value["graphics_baseline_bytes"], 200)
        self.assertEqual(value["foreign_cuda_pids"], [])

    def test_gpu_baseline_rejects_foreign_cuda(self) -> None:
        nvml = SimpleNamespace(
            nvmlDeviceGetComputeRunningProcesses=lambda _handle: [SimpleNamespace(pid=99, usedGpuMemory=200)],
            nvmlDeviceGetGraphicsRunningProcesses=lambda _handle: [],
        )
        with self.assertRaises(BATCH.GPUResourceBusy):
            BATCH.gpu_process_baseline(nvml, object())

    def test_gpu_memory_admission_enforces_reserve_and_consistency(self) -> None:
        nvml = SimpleNamespace(
            nvmlDeviceGetMemoryInfo=lambda _handle: SimpleNamespace(total=1000, used=400, free=600)
        )
        self.assertEqual(
            BATCH.gpu_memory_admission(nvml, object(), minimum_free_bytes=500)["free_bytes"],
            600,
        )
        with self.assertRaises(BATCH.GPUResourceBusy):
            BATCH.gpu_memory_admission(nvml, object(), minimum_free_bytes=700)
        broken = SimpleNamespace(
            nvmlDeviceGetMemoryInfo=lambda _handle: SimpleNamespace(total=100, used=200, free=0)
        )
        with self.assertRaisesRegex(BATCH.BatchV2Error, "inconsistent"):
            BATCH.gpu_memory_admission(broken, object(), minimum_free_bytes=1)

    def test_event_journal_is_immutable_and_bounded(self) -> None:
        journal = BATCH.EventJournal(self.event_root, "gpuasrbatch2_" + "a" * 32, "gpuasrattempt_" + "b" * 32)
        first = journal.emit("attempt-start", {"x": 1})
        self.assertEqual(stat.S_IMODE(first.stat().st_mode), 0o400)
        journal.ordinal = BATCH.MAX_EVENTS_PER_ATTEMPT
        with self.assertRaisesRegex(BATCH.BatchV2Error, "finite bound"):
            journal.emit("extra", {})
        journal.close()

    def test_hard_deadline_can_be_cancelled(self) -> None:
        calls = []
        with BATCH.HardDeadline(0.1, fatal=calls.append):
            time.sleep(0.01)
        self.assertEqual(calls, [])

    def test_contract_binds_two_worker_non_neural_execution(self) -> None:
        contract = BATCH.contract_document()
        self.assertEqual(contract["descriptor"]["execution"]["concurrency"], 2)
        self.assertEqual(contract["descriptor"]["policy"]["neural_batching"], False)


if __name__ == "__main__":
    unittest.main()
