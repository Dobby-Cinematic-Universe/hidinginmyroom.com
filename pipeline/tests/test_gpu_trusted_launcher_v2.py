from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import stat
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace


SOURCE = Path(__file__).resolve().parents[1] / "gpu/trusted_launcher_v2.py"
SPEC = importlib.util.spec_from_file_location("himr_test_trusted_launcher_v2", SOURCE)
assert SPEC is not None and SPEC.loader is not None
LAUNCHER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LAUNCHER
SPEC.loader.exec_module(LAUNCHER)

ZERO = "0" * 64
ONE = "1" * 64
TWO = "2" * 64
GPU_UUID = "GPU-0b9d7029-b3c6-1a0b-d483-6a3febc3b557"


def mapping_rows() -> list[dict[str, str]]:
    return [
        {
            "name": name,
            "image_relative_path": sandbox_path.removeprefix("/opt/himr-gpu/"),
            "sandbox_path": sandbox_path,
            "role": role,
        }
        for name, (sandbox_path, role) in sorted(LAUNCHER.REQUIRED_MAPPING_LAYOUT.items())
    ]


def host_abi_manifest(execution_image_identity: str = TWO) -> dict:
    driver = "610.57.04"
    runtime_loaded = sorted(
        {
            "ld-linux-x86-64.so.2",
            "libcuda.so.1",
            f"libnvidia-gpucomp.so.{driver}",
            "libnvidia-ml.so.1",
            "libnvidia-nvvm.so.4",
            "libnvidia-nvvm70.so.4",
            "libnvidia-ptxjitcompiler.so.1",
        }
    )
    cublas_paths = sorted(LAUNCHER.HOST_ABI_EXPLICIT_IMAGE_DLOPEN_ROOTS)
    consumers = [
        {
            "image_relative_path": "runtime/bin/python3.12",
            "sha256": hashlib.sha256(b"consumer").hexdigest(),
            "byte_count": 64,
            "elf": {
                "elf_class": 64,
                "endianness": "little",
                "machine": 62,
                "elf_type": 3,
                "soname": None,
                "needed": [],
                "interpreter": "/lib64/ld-linux-x86-64.so.2",
                "rpath": [],
                "runpath": [],
            },
        },
        *[
            {
                "image_relative_path": path,
                "sha256": hashlib.sha256(path.encode()).hexdigest(),
                "byte_count": 64,
                "elf": {
                    "elf_class": 64,
                    "endianness": "little",
                    "machine": 62,
                    "elf_type": 3,
                    "soname": Path(path).name,
                    "needed": [],
                    "interpreter": None,
                    "rpath": [],
                    "runpath": [],
                },
            }
            for path in cublas_paths
        ],
    ]
    consumer_core = {
        "execution_image_identity_sha256": execution_image_identity,
        "source_tree_identity_sha256": hashlib.sha256(b"source tree").hexdigest(),
        "source_tree_regular_file_count": len(consumers),
        "elf_file_count": len(consumers),
        "consumers": consumers,
        "load_roots": sorted(
            {"runtime/bin/python3.12", *cublas_paths}
        ),
        "runtime_loaded_sonames": runtime_loaded,
        "external_sonames": runtime_loaded,
    }
    consumer = {
        **consumer_core,
        "identity_sha256": LAUNCHER.sha256_bytes(
            LAUNCHER.canonical_bytes(consumer_core)
        ),
    }
    root_libraries = sorted(f"/usr/lib64/{name}" for name in runtime_loaded)
    libraries = [
        {
            "sandbox_path": path,
            "source_path": path,
            "sha256": hashlib.sha256(path.encode()).hexdigest(),
            "byte_count": 4096 + ordinal,
            "uid": 0,
            "gid": 0,
            "mode": "0755",
            "aliases": [],
            "elf": {
                "elf_class": 64,
                "endianness": "little",
                "machine": 62,
                "elf_type": 3,
                "soname": Path(path).name,
                "needed": [],
                "interpreter": None,
                "rpath": [],
                "runpath": [],
            },
        }
        for ordinal, path in enumerate(root_libraries, 1)
    ]
    core = {
        "kind": LAUNCHER.HOST_ABI_KIND,
        "schema_version": 1,
        "implementation_version": "0.1.0",
        "platform": {
            "sysname": "Linux",
            "release": "6.17.9-test",
            "version": "#1 SMP PREEMPT_DYNAMIC",
            "machine": "x86_64",
            "nvidia_driver_version": driver,
            "nvidia_kernel_module_version": driver,
            "nvidia_kernel_module_report_sha256": hashlib.sha256(b"nvrm").hexdigest(),
            "nvidia_kernel_module_report_byte_count": 4,
        },
        "consumer_scan": consumer,
        "library_roots": ["/usr/lib64"],
        "root_libraries": root_libraries,
        "required_owner": {"uid": 0, "gid": 0},
        "libraries": libraries,
        "dependency_edges": [],
        "policy": dict(LAUNCHER.HOST_ABI_POLICY),
    }
    identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
    return {
        **core,
        "identity_sha256": identity,
        "manifest_id": f"gpuhostabi_{identity[:32]}",
    }


def host_abi_bindings(manifest: dict | None = None) -> list[dict]:
    value = manifest or host_abi_manifest()
    return [
        {
            "role": "host_abi_library",
            "source_path": row["source_path"],
            "target": row["sandbox_path"],
            "sha256": row["sha256"],
            "byte_count": row["byte_count"],
            "uid": row["uid"],
            "gid": row["gid"],
            "mode": row["mode"],
        }
        for row in value["libraries"]
    ]


def host_abi_summary(manifest: dict | None = None) -> dict:
    value = manifest or host_abi_manifest()
    count = len(value["libraries"])
    return {
        "identity_sha256": value["identity_sha256"],
        "manifest_id": value["manifest_id"],
        "platform_replayed": True,
        "libraries_replayed": True,
        "library_count": count,
        "binding_count": count,
    }


def launcher_profile_core() -> dict:
    tools = {
        name: {
            "path": path,
            "sha256": hashlib.sha256(name.encode()).hexdigest(),
            "byte_count": 100 + ordinal,
            "uid": 0,
            "mode": "4755" if name == "fusermount" else "0755",
        }
        for ordinal, (name, path) in enumerate(sorted(LAUNCHER.SYSTEM_TOOL_PATHS.items()))
    }
    return {
        "kind": LAUNCHER.PROFILE_KIND,
        "schema_version": 2,
        "implementation_version": LAUNCHER.IMPLEMENTATION_VERSION,
        "launcher": {"path": "/usr/local/libexec/himr-gpu/trusted-launcher-v2", "sha256": ZERO},
        "runtime_admission_install_path": "/etc/himr-gpu/runtime-admission-v2.json",
        "execution_image": {
            "path": "/var/lib/himr-gpu/execution-v2.squashfs",
            "sha256": ONE,
            "byte_count": 123456,
            "identity_sha256": TWO,
            "receipt_path": "/var/lib/himr-gpu/execution-v2-receipt.json",
            "receipt_sha256": hashlib.sha256(b"image receipt").hexdigest(),
        },
        "production_profile": {
            "path": "/srv/himr/research/profile-v2.json",
            "sha256": hashlib.sha256(b"profile").hexdigest(),
            "identity_sha256": hashlib.sha256(b"profile identity").hexdigest(),
        },
        "root_registration": {
            "path": "/srv/himr/research/root-v1.json",
            "sha256": hashlib.sha256(b"root").hexdigest(),
            "identity_sha256": hashlib.sha256(b"root identity").hexdigest(),
            "registration_id": "gpurootreg_" + "a" * 32,
            "root_id": "himr-hot-main-v1",
        },
        "system_tools": tools,
        "host_abi": host_abi_manifest(),
        "sandbox": {
            "image_mapping_prefix": "/opt/himr-gpu",
            "control_root": "/run/himr-gpu/control",
            "input_root": "/run/himr-gpu/input",
            "output_root": "/run/himr-gpu/output",
            "state_root": "/run/himr-gpu/state",
            "system_library_directories": [],
            "system_readonly_files": [],
            "gpu_control_devices": ["/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"],
        },
        "policy": dict(LAUNCHER.POLICY),
    }


def launcher_profile() -> dict:
    core = launcher_profile_core()
    identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
    return {**core, "identity_sha256": identity, "profile_id": f"gpulaunchprofile_{identity[:32]}"}


def production_profile() -> dict:
    core = {
        "kind": LAUNCHER.PRODUCTION_PROFILE_KIND,
        "schema_version": 2,
        "implementation_version": "0.2.1",
        "name": "test",
        "model": {},
        "hardware": {
            "gpu_uuid": GPU_UUID,
            "device_index": 0,
            "compute_type": "float16",
            "minimum_driver_version": "610.57.04",
            "minimum_compute_capability": [8, 6],
        },
        "decoding": {"num_workers": 2},
        "item_limits": {"maximum_result_bytes": 16 * 1024 * 1024},
        "batch_limits": {"maximum_wall_seconds": 1800, "inference_concurrency": 2},
        "telemetry": {},
        "scheduler": {},
        "safety": {"visibility": "private", "network_access": False},
    }
    identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
    return {**core, "identity_sha256": identity, "profile_id": f"gpuprofile_{identity[:32]}"}


def host_envelope() -> dict:
    return {
        "cgroup_version": 2,
        "cgroup_path": "/test.slice",
        "effective": {
            "memory_max_bytes": 12 * 1024**3,
            "memory_swap_max_bytes": 0,
            "pids_max": 64,
        },
        "rlimits": {
            "core": {"soft": 0, "hard": 0},
            "fsize": {"soft": 32 * 1024**2, "hard": 32 * 1024**2},
            "nofile": {"soft": 1024, "hard": 1024},
        },
        "requirements": {
            "memory_max_bytes_at_most": 12 * 1024**3,
            "memory_swap_max_bytes": 0,
            "pids_max_at_most": 64,
            "nofile_at_most": 1024,
            "core_bytes": 0,
            "fsize_bytes_at_least": 16 * 1024 * 1024,
            "fsize_bytes_at_most": 32 * 1024**2,
        },
        "status": "passed",
        "violations": [],
    }


def runtime_receipt(profile: dict, *, admitted: bool) -> dict:
    candidate_identity = hashlib.sha256(b"runtime candidate").hexdigest()
    gates = {
        name: (
            {
                "status": "passed",
                "path": f"/gates/{name}.json",
                "sha256": ZERO,
                "byte_count": 100,
                "identity_sha256": hashlib.sha256(name.encode()).hexdigest(),
                "gate_id": "gpugate_" + hashlib.sha256(name.encode()).hexdigest()[:32],
                "runtime_candidate_identity_sha256": candidate_identity,
            }
            if admitted
            else None
        )
        for name in LAUNCHER.GATE_NAMES
    }
    runtime_tools = []
    for name in sorted(LAUNCHER.RUNTIME_TOOL_NAMES):
        runtime_tools.append({"name": name, **profile["system_tools"][name]})
    core = {
        "kind": LAUNCHER.RUNTIME_KIND,
        "schema_version": 2,
        "implementation_version": LAUNCHER.IMPLEMENTATION_VERSION,
        "status": "admitted" if admitted else "candidate",
        "root_registration": {**profile["root_registration"], "byte_count": 100},
        "root": {"root_id": profile["root_registration"]["root_id"], "tier": "hot_main_drive", "path": "/srv/himr", "filesystem": {"type": "btrfs", "uuid": "27bd4222-a8f3-4d91-b48f-7ce38d70e507"}},
        "execution_image": {
            "receipt_path": profile["execution_image"]["receipt_path"],
            "receipt_sha256": profile["execution_image"]["receipt_sha256"],
            "receipt_byte_count": 100,
            "identity_sha256": profile["execution_image"]["identity_sha256"],
            "image": {"path": profile["execution_image"]["path"], "sha256": profile["execution_image"]["sha256"], "byte_count": profile["execution_image"]["byte_count"], "mode": "0444", "filesystem": {"type": "btrfs", "uuid": "27bd4222-a8f3-4d91-b48f-7ce38d70e507"}},
            "logical_mappings": mapping_rows(),
        },
        "production_profile": {"identity_sha256": profile["production_profile"]["identity_sha256"]},
        "production_profile_file": {**profile["production_profile"], "byte_count": 100},
        "trusted_install": {
            "owner_uid": 0,
            "launcher": {**profile["launcher"], "byte_count": 100, "uid": 0, "mode": "0555"},
            "launcher_profile": {"path": "/etc/himr-gpu/launcher-profile-v2.json", "sha256": hashlib.sha256(b"launcher profile").hexdigest(), "byte_count": 100, "uid": 0, "mode": "0444"},
            "system_tools": runtime_tools,
            "root_ownership_enforced": admitted,
        },
        "runtime_candidate_identity_sha256": candidate_identity,
        "runtime": {},
        "gates": gates,
        "policy": dict(LAUNCHER.RUNTIME_POLICY),
    }
    identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
    return {**core, "identity_sha256": identity, "receipt_id": f"gpurtv2_{identity[:32]}"}


def minimal_work_order(*, production: bool, admitted: bool = True) -> dict:
    digest = hashlib.sha256(b"input").hexdigest()
    hot = {
        "registration_path": "/srv/himr/research/root-v1.json",
        "registration_sha256": hashlib.sha256(b"root").hexdigest(),
        "registration_id": "gpurootreg_" + "a" * 32,
        "identity_sha256": hashlib.sha256(b"root identity").hexdigest(),
        "root_id": "himr-hot-main-v1",
        "path": "/srv/himr",
        "filesystem_uuid": "27bd4222-a8f3-4d91-b48f-7ce38d70e507",
        "tier": "hot_main_drive",
    }
    profile = {
        "path": "/srv/himr/research/profile-v2.json",
        "sha256": hashlib.sha256(b"profile").hexdigest(),
        "profile_id": "gpuprofile_" + "b" * 32,
        "identity_sha256": hashlib.sha256(b"profile identity").hexdigest(),
    }
    runtime = {
        "receipt_path": "/srv/himr/research/runtime-v2.json",
        "receipt_sha256": hashlib.sha256(b"runtime").hexdigest(),
        "receipt_id": "gpurtv2_" + "c" * 32,
        "identity_sha256": hashlib.sha256(b"runtime identity").hexdigest(),
        "status": "admitted" if admitted else "candidate",
    }
    if production:
        lineage = {"kind": "production_preprocess_v03"}
    else:
        lineage = {
            "kind": "synthetic_canary",
            "contains_corpus_media": False,
            "scope": "purpose_built_synthetic_only",
            "corpus_authority": "none",
        }
    core = {
        "kind": LAUNCHER.WORK_ORDER_KIND,
        "schema_version": LAUNCHER.WORK_ORDER_SCHEMA_VERSION,
        "implementation_version": LAUNCHER.WORK_ORDER_IMPLEMENTATION_VERSION,
        "job_id": "test",
        "input": {
            "path": "/srv/himr/research/input.flac",
            "expected_sha256": digest,
            "expected_byte_count": 123,
            "expected_duration_ms": 1000,
        },
        "source_lineage": lineage,
        "runtime_admission": runtime,
        "production_profile": profile,
        "hot_root": hot,
        "execution_contract": {},
        "transcript_semantics": {},
        "catalog_context": None,
        "output": {},
        "policy": {},
    }
    identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
    return {**core, "identity_sha256": identity, "work_order_id": f"gpuasrwo5_{identity[:32]}"}


def minimal_batch(*, production: bool, admitted: bool = True) -> dict:
    order = minimal_work_order(production=production, admitted=admitted)
    return {
        "execution_class": "production_private_asr" if production else "synthetic_canary",
        "hot_root": order["hot_root"],
        "production_profile": order["production_profile"],
        "runtime_admission": order["runtime_admission"],
        "items": [
            {
                "ordinal": 1,
                "work_order": order,
                "input": {
                    "sha256": order["input"]["expected_sha256"],
                    "byte_count": order["input"]["expected_byte_count"],
                    "duration_ms": order["input"]["expected_duration_ms"],
                },
                "result": {},
            }
        ],
    }


class TrustedLauncherContractsTest(unittest.TestCase):
    def test_tool_closure_includes_uuid_resolver_and_isolated_host_python(self) -> None:
        self.assertEqual(
            LAUNCHER.SYSTEM_TOOL_PATHS["nvidia_smi"], "/usr/bin/nvidia-smi"
        )
        self.assertEqual(
            LAUNCHER.SYSTEM_TOOL_PATHS["host_python"], "/usr/bin/python3.14"
        )
        self.assertEqual(set(LAUNCHER.SYSTEM_TOOL_PATHS), LAUNCHER.RUNTIME_TOOL_NAMES)
        self.assertTrue(SOURCE.read_text().startswith("#!/usr/bin/python3.14 -IB\n"))

    def test_host_abi_runtime_dlopen_roots_and_exclusions_are_exact(self) -> None:
        roots = LAUNCHER._production_runtime_loaded_sonames("610.57.04")
        self.assertIn("libnvidia-nvvm70.so.4", roots)
        self.assertNotIn("libnvidia-tileiras.so.610.57.04", roots)
        self.assertNotIn("libnvidia-pkcs11-openssl3.so.610.57.04", roots)
        self.assertNotIn("libnvidia-pkcs11.so.610.57.04", roots)
        self.assertNotIn("libcudadebugger.so.1", roots)
        self.assertEqual(
            LAUNCHER.HOST_ABI_POLICY["nvidia_tileir_runtime_loading"],
            "prohibited",
        )
        self.assertEqual(
            LAUNCHER.HOST_ABI_POLICY["nvidia_pkcs11_runtime_loading"],
            "prohibited",
        )

    def test_profile_exact_identity_and_system_allowlist(self) -> None:
        value = launcher_profile()
        self.assertEqual(LAUNCHER.validate_launcher_profile(value), value)
        changed = {**value, "sandbox": {**value["sandbox"], "system_library_directories": ["/usr"]}}
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "allowlist"):
            LAUNCHER.validate_launcher_profile(changed)

    def test_host_abi_manifest_rejects_disconnected_extra_library(self) -> None:
        value = json.loads(json.dumps(host_abi_manifest()))
        extra = json.loads(json.dumps(value["libraries"][0]))
        extra["sandbox_path"] = "/usr/lib64/libdisconnected.so.1"
        extra["source_path"] = extra["sandbox_path"]
        extra["sha256"] = hashlib.sha256(b"disconnected").hexdigest()
        extra["elf"]["soname"] = "libdisconnected.so.1"
        value["libraries"].append(extra)
        value["libraries"].sort(key=lambda row: row["sandbox_path"])
        core = {
            key: item
            for key, item in value.items()
            if key not in {"identity_sha256", "manifest_id"}
        }
        identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
        value["identity_sha256"] = identity
        value["manifest_id"] = f"gpuhostabi_{identity[:32]}"
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError, "disconnected extra"
        ):
            LAUNCHER.validate_host_abi_manifest(value)

    def test_host_abi_manifest_rejects_forged_alias_cycle(self) -> None:
        value = json.loads(json.dumps(host_abi_manifest()))
        row = value["libraries"][0]
        alias_a = row["sandbox_path"]
        alias_b = "/usr/lib64/libforged-cycle.so.1"
        metadata = {"uid": 0, "gid": 0, "mode": "0777"}
        row["aliases"] = [
            {"path": alias_a, "target": Path(alias_b).name, **metadata},
            {"path": alias_b, "target": Path(alias_a).name, **metadata},
            {
                "path": alias_a,
                "target": Path(row["source_path"]).name,
                **metadata,
            },
        ]
        core = {
            key: item
            for key, item in value.items()
            if key not in {"identity_sha256", "manifest_id"}
        }
        identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
        value["identity_sha256"] = identity
        value["manifest_id"] = f"gpuhostabi_{identity[:32]}"
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "cycles"):
            LAUNCHER.validate_host_abi_manifest(value)

    def test_host_abi_rpath_inherits_but_runpath_is_direct_only(self) -> None:
        extension = "runtime/lib/python3.12/site-packages/ctranslate2/_ext.so"
        private = "runtime/lib/python3.12/site-packages/ctranslate2.libs"
        child = f"{private}/libctranslate2.so.4"
        leaf = f"{private}/libgomp-private.so.1"

        def consumer(path: str, needed: list[str], **paths: list[str]) -> dict:
            return {
                "image_relative_path": path,
                "elf": {
                    "needed": needed,
                    "rpath": paths.get("rpath", []),
                    "runpath": paths.get("runpath", []),
                },
            }

        inherited = [
            consumer(
                extension,
                ["libctranslate2.so.4"],
                rpath=["$ORIGIN/../ctranslate2.libs"],
            ),
            consumer(child, ["libgomp-private.so.1"]),
            consumer(leaf, []),
        ]
        self.assertEqual(
            LAUNCHER._host_abi_external_sonames(inherited, [], [extension]),
            [],
        )
        direct_only = json.loads(json.dumps(inherited))
        direct_only[0]["elf"]["runpath"] = direct_only[0]["elf"].pop("rpath")
        direct_only[0]["elf"]["rpath"] = []
        self.assertEqual(
            LAUNCHER._host_abi_external_sonames(direct_only, [], [extension]),
            ["libgomp-private.so.1"],
        )

        invalid_elf = json.loads(
            json.dumps(host_abi_manifest()["consumer_scan"]["consumers"][0]["elf"])
        )
        invalid_elf["rpath"] = ["$ORIGIN"]
        invalid_elf["runpath"] = ["$ORIGIN"]
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError, "both DT_RPATH and DT_RUNPATH"
        ):
            LAUNCHER._normalize_host_abi_elf(invalid_elf, "test ELF")
        for loader_path in (
            "",
            "/usr/lib64",
            "$LIB",
            "${ORIGIN}/private",
            "$ORIGIN/./private",
            "$ORIGIN/private//nested",
        ):
            invalid_elf["rpath"] = [loader_path]
            invalid_elf["runpath"] = []
            with self.subTest(loader_path=loader_path), self.assertRaises(
                LAUNCHER.TrustedLauncherError
            ):
                LAUNCHER._normalize_host_abi_elf(invalid_elf, "test ELF")

    def test_host_abi_consumer_load_roots_are_exact(self) -> None:
        scan = json.loads(json.dumps(host_abi_manifest()["consumer_scan"]))
        scan["load_roots"].pop()
        core = {key: value for key, value in scan.items() if key != "identity_sha256"}
        scan["identity_sha256"] = LAUNCHER.sha256_bytes(
            LAUNCHER.canonical_bytes(core)
        )
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError, "load roots differ"
        ):
            LAUNCHER._normalize_host_abi_consumer_scan(scan)

    def test_host_abi_parser_rejects_multiple_interpreters(self) -> None:
        body = bytearray(256)
        ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + b"\x00" * 8
        struct.pack_into(
            "<16sHHIQQQIHHHHHH",
            body,
            0,
            ident,
            3,
            LAUNCHER.EM_X86_64,
            1,
            0,
            64,
            0,
            0,
            64,
            56,
            2,
            0,
            0,
            0,
        )
        interpreter = b"/lib64/ld-linux-x86-64.so.2\x00"
        for ordinal, offset in enumerate((176, 216)):
            body[offset : offset + len(interpreter)] = interpreter
            struct.pack_into(
                "<IIQQQQQQ",
                body,
                64 + ordinal * 56,
                LAUNCHER.PT_INTERP,
                4,
                offset,
                0,
                0,
                len(interpreter),
                len(interpreter),
                1,
            )
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError, "multiple PT_INTERP"
        ):
            LAUNCHER._parse_host_abi_elf(bytes(body), "duplicate interpreter")

    def test_host_abi_parser_rejects_load_file_size_above_memory_size(self) -> None:
        body = bytearray(128)
        ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + b"\x00" * 8
        struct.pack_into(
            "<16sHHIQQQIHHHHHH",
            body,
            0,
            ident,
            3,
            LAUNCHER.EM_X86_64,
            1,
            0,
            64,
            0,
            0,
            64,
            56,
            1,
            0,
            0,
            0,
        )
        struct.pack_into(
            "<IIQQQQQQ",
            body,
            64,
            LAUNCHER.PT_LOAD,
            4,
            120,
            0x400000,
            0,
            2,
            1,
            4096,
        )
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError,
            "file size larger than memory size",
        ):
            LAUNCHER._parse_host_abi_elf(bytes(body), "invalid load segment")

    def test_runtime_requires_admitted_and_passed_gates(self) -> None:
        profile = launcher_profile()
        receipt = runtime_receipt(profile, admitted=True)
        observed = LAUNCHER.validate_runtime_receipt(
            receipt,
            mode="production",
            launcher_profile=profile,
            launcher_profile_path="/etc/himr-gpu/launcher-profile-v2.json",
            profile_sha256=receipt["trusted_install"]["launcher_profile"]["sha256"],
        )
        self.assertEqual(observed["status"], "admitted")
        failed = runtime_receipt(profile, admitted=True)
        failed["gates"]["thermal_8h"]["status"] = "failed"
        core = {key: value for key, value in failed.items() if key not in {"identity_sha256", "receipt_id"}}
        identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
        failed["identity_sha256"] = identity
        failed["receipt_id"] = f"gpurtv2_{identity[:32]}"
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "gate thermal_8h"):
            LAUNCHER.validate_runtime_receipt(
                failed,
                mode="production",
                launcher_profile=profile,
                launcher_profile_path="/etc/himr-gpu/launcher-profile-v2.json",
                profile_sha256=failed["trusted_install"]["launcher_profile"]["sha256"],
            )

        duplicate = json.loads(json.dumps(receipt))
        duplicate["trusted_install"]["system_tools"][-1] = dict(
            duplicate["trusted_install"]["system_tools"][0]
        )
        duplicate["trusted_install"]["system_tools"].sort(
            key=lambda row: row["name"]
        )
        core = {
            key: value
            for key, value in duplicate.items()
            if key not in {"identity_sha256", "receipt_id"}
        }
        identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
        duplicate["identity_sha256"] = identity
        duplicate["receipt_id"] = f"gpurtv2_{identity[:32]}"
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError, "tool set is not exact"
        ):
            LAUNCHER.validate_runtime_receipt(
                duplicate,
                mode="production",
                launcher_profile=profile,
                launcher_profile_path="/etc/himr-gpu/launcher-profile-v2.json",
                profile_sha256=duplicate["trusted_install"]["launcher_profile"][
                    "sha256"
                ],
            )

    def test_candidate_cannot_accept_production_or_corpus_lineage(self) -> None:
        synthetic = minimal_batch(production=False, admitted=False)
        self.assertEqual(LAUNCHER.validate_batch_execution_class(synthetic, "candidate-synthetic-canary"), "synthetic_canary")
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "cannot execute"):
            LAUNCHER.validate_batch_execution_class({**synthetic, "execution_class": "production_private_asr"}, "candidate-synthetic-canary")
        synthetic = minimal_batch(production=False, admitted=False)
        synthetic["items"][0]["work_order"]["source_lineage"]["contains_corpus_media"] = True
        order = synthetic["items"][0]["work_order"]
        core = {key: value for key, value in order.items() if key not in {"identity_sha256", "work_order_id"}}
        identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
        order["identity_sha256"] = identity
        order["work_order_id"] = f"gpuasrwo5_{identity[:32]}"
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "synthetic scope"):
            LAUNCHER.validate_batch_execution_class(synthetic, "candidate-synthetic-canary")

    def test_local_private_accepts_only_candidate_production_lineage(self) -> None:
        manifest = minimal_batch(production=True, admitted=False)
        manifest["execution_class"] = "local_private_production_asr"
        self.assertEqual(
            LAUNCHER.validate_batch_execution_class(
                manifest, "local-private-production"
            ),
            "local_private_production_asr",
        )
        admitted = minimal_batch(production=True, admitted=True)
        admitted["execution_class"] = "local_private_production_asr"
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "candidate"):
            LAUNCHER.validate_batch_execution_class(
                admitted, "local-private-production"
            )

    def test_local_private_launch_requires_readiness_before_controls(self) -> None:
        args = SimpleNamespace(
            mode="local-private-production",
            local_readiness=None,
            expected_local_readiness_sha256=None,
        )
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError, "requires a passed readiness"
        ):
            LAUNCHER.launch(args)

    def test_production_items_require_preprocess_lineage_and_admitted_runtime(self) -> None:
        manifest = minimal_batch(production=True)
        self.assertEqual(LAUNCHER.validate_batch_execution_class(manifest, "production"), "production_private_asr")
        manifest = minimal_batch(production=True, admitted=False)
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "not admitted"):
            LAUNCHER.validate_batch_execution_class(manifest, "production")

    def test_gpu_uuid_resolution_is_index_portable(self) -> None:
        def runner(command, **_kwargs):
            if "-x" in command:
                return SimpleNamespace(
                    returncode=0,
                    stdout=(
                        f"<nvidia_smi_log><driver_version>610.57.04</driver_version><gpu><uuid>{GPU_UUID}</uuid><minor_number>3</minor_number></gpu>"
                        "<gpu><uuid>GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</uuid><minor_number>7</minor_number></gpu></nvidia_smi_log>"
                    ).encode(),
                    stderr=b"",
                )
            return SimpleNamespace(returncode=0, stdout=f"3, {GPU_UUID}, 610.57.04, 8.6\n7, GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee, 610.57.04, 8.6\n".encode(), stderr=b"")

        observation = LAUNCHER.resolve_gpu_observation(
            "/usr/bin/nvidia-smi", GPU_UUID, "610.57.04", [8, 6], runner=runner
        )
        self.assertEqual(observation["host_index"], 3)
        self.assertEqual(observation["device_minor"], 3)
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "below"):
            LAUNCHER.resolve_gpu_observation(
                "/usr/bin/nvidia-smi", GPU_UUID, "611.0.0", [8, 6], runner=runner
            )
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "exact admitted"):
            LAUNCHER.resolve_gpu_observation(
                "/usr/bin/nvidia-smi", GPU_UUID, "609.0.0", [8, 6], runner=runner
            )

        def changed_driver_runner(command, **kwargs):
            result = runner(command, **kwargs)
            if "-x" in command:
                result.stdout = result.stdout.replace(b"610.57.04", b"610.57.05")
            return result

        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "changed"):
            LAUNCHER.resolve_gpu_observation(
                "/usr/bin/nvidia-smi",
                GPU_UUID,
                "610.57.04",
                [8, 6],
                runner=changed_driver_runner,
            )
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "absent"):
            LAUNCHER.resolve_gpu_observation(
                "/usr/bin/nvidia-smi",
                "GPU-ffffffff-1111-2222-3333-444444444444",
                "610.57.04",
                [8, 6],
                runner=runner,
            )

    def test_bwrap_argv_has_closed_mount_env_and_batch_contract(self) -> None:
        profile = launcher_profile()
        mappings = mapping_rows()
        argv = LAUNCHER.build_bwrap_argv(
            launcher_profile=profile,
            mappings=mappings,
            mapping_sources={row["name"]: 30 + ordinal for ordinal, row in enumerate(mappings)},
            host_abi_sources={
                row["target"]: 200 + ordinal
                for ordinal, row in enumerate(host_abi_bindings())
            },
            manifest_source=11,
            control_sources={
                "runtime": {"control": 12, "work_order": 22},
                "profile": {"control": 13, "work_order": 23},
                "root": {"control": 14, "work_order": 24},
            },
            control_original_targets={
                "runtime": "/srv/himr/research/runtime.json",
                "profile": "/srv/himr/research/profile.json",
                "root": "/srv/himr/research/root.json",
            },
            lineage_preflight_source=19,
            lineage_preflight_sha256=hashlib.sha256(b"lineage").hexdigest(),
            read_binding_sources={
                "/srv/himr/research/input.flac": 20
            },
            writable_sources={"result": 15, "event": 16, "lock": 17},
            writable_targets={"result": "/srv/himr/research/results", "event": "/srv/himr/research/events", "lock": "/run/user/1000/himr-gpu-locks"},
            attestation_source=18,
            attestation_sha256=ZERO,
            gpu_devices=["/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools", "/dev/nvidia3"],
            gpu_uuid=GPU_UUID,
            gpu_index=3,
            batch_manifest_sha256=ONE,
            runtime_sha256=TWO,
            profile_sha256=hashlib.sha256(b"p").hexdigest(),
            root_sha256=hashlib.sha256(b"r").hexdigest(),
        )
        self.assertIn("--unshare-all", argv)
        self.assertIn("--unshare-user", argv)
        self.assertEqual(argv.count("--remount-ro"), 1)
        self.assertGreater(
            argv.index("--remount-ro"),
            max(
                index
                for index, value in enumerate(argv)
                if value in {"--ro-bind-fd", "--bind-fd", "--dev-bind"}
            ),
        )
        self.assertIn("--clearenv", argv)
        self.assertNotIn(["--ro-bind", "/", "/"], [argv[index:index + 3] for index in range(len(argv) - 2)])
        self.assertIn("CUDA_VISIBLE_DEVICES", argv)
        self.assertEqual(argv[argv.index("CUDA_VISIBLE_DEVICES") + 1], GPU_UUID)
        self.assertIn("--expected-launch-attestation-sha256", argv)
        command_separator = len(argv) - 1 - argv[::-1].index("--")
        self.assertEqual(argv[command_separator + 2 : command_separator + 4], ["-B", "-I"])
        self.assertEqual(
            argv[command_separator + 4],
            LAUNCHER.REQUIRED_MAPPING_LAYOUT["worker_source"][0],
        )
        self.assertNotIn("/srv/himr", [argv[index + 1] for index, value in enumerate(argv[:-1]) if value == "--ro-bind"], "phase 2 must never bind the hot root")
        self.assertIn("--ro-bind-fd", argv)
        self.assertIn("31", argv)
        self.assertNotIn("/proc/self/fd/31", argv)
        self.assertFalse(
            any(value.startswith("/proc/self/fd/") for value in argv)
        )
        self.assertNotIn("/etc/ld.so.cache", argv)
        self.assertNotIn(
            ["--symlink", "usr/lib", "/lib"],
            [argv[index : index + 3] for index in range(len(argv) - 2)],
        )
        self.assertEqual(
            argv[argv.index("LD_LIBRARY_PATH") + 1],
            f"{LAUNCHER.REQUIRED_MAPPING_LAYOUT['cublas_library_directory'][0]}:/usr/lib64",
        )
        self.assertFalse(
            any(
                argv[index] == "--ro-bind"
                and argv[index + 1] in {"/usr/lib", "/usr/lib64"}
                for index in range(len(argv) - 2)
            )
        )
        self.assertTrue(
            all(
                ["--ro-bind-fd", str(200 + ordinal), row["target"]]
                in [argv[index : index + 3] for index in range(len(argv) - 2)]
                for ordinal, row in enumerate(host_abi_bindings())
            )
        )
        self.assertNotIn("/mnt/archive/HIMR", argv)

    def test_bwrap_fd_bind_consumes_source_before_adversarial_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source"
            source.mkdir(mode=0o700)
            sentinel = source / "sentinel"
            sentinel.write_text("safe")
            descriptor = os.open(
                source, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
            )
            writable = Path(temporary) / "writable"
            writable.mkdir(mode=0o700)
            writable_descriptor = os.open(
                writable, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
            )
            payload = """
import errno
import os
import sys

source_fd = int(sys.argv[1])
try:
    os.open("sentinel", os.O_WRONLY | os.O_TRUNC, dir_fd=source_fd)
except OSError as error:
    assert error.errno == errno.EBADF, error
else:
    raise SystemExit("Bubblewrap leaked its writable source dirfd")

try:
    os.open("/sealed/sentinel", os.O_WRONLY | os.O_TRUNC)
except OSError as error:
    assert error.errno in {errno.EROFS, errno.EACCES, errno.EPERM}, error
else:
    raise SystemExit("Bubblewrap readonly bind was writable")

writable_fd = int(sys.argv[2])
try:
    os.open("leaked", os.O_WRONLY | os.O_CREAT, 0o600, dir_fd=writable_fd)
except OSError as error:
    assert error.errno == errno.EBADF, error
else:
    raise SystemExit("Bubblewrap leaked its writable source dirfd")

with open("/writable/result", "w", encoding="utf-8") as stream:
    stream.write("written-through-bind")
"""
            argv = [
                "/usr/bin/bwrap",
                "--die-with-parent",
                "--new-session",
                "--unshare-all",
                "--unshare-user",
                "--uid",
                str(os.geteuid()),
                "--gid",
                str(os.getegid()),
                "--disable-userns",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--ro-bind",
                "/usr",
                "/usr",
                "--symlink",
                "usr/lib",
                "/lib",
                "--symlink",
                "usr/lib64",
                "/lib64",
                "--ro-bind",
                "/etc/ld.so.cache",
                "/etc/ld.so.cache",
                "--ro-bind-fd",
                str(descriptor),
                "/sealed",
                "--bind-fd",
                str(writable_descriptor),
                "/writable",
                "--clearenv",
                "--setenv",
                "PATH",
                "/usr/bin",
                "--",
                "/usr/bin/python3.14",
                "-I",
                "-B",
                "-c",
                payload,
                str(descriptor),
                str(writable_descriptor),
            ]
            try:
                completed = subprocess.run(
                    argv,
                    check=False,
                    capture_output=True,
                    text=True,
                    close_fds=True,
                    pass_fds=(descriptor, writable_descriptor),
                    env=LAUNCHER._clean_helper_environment(),
                    timeout=10,
                )
            finally:
                os.close(descriptor)
                os.close(writable_descriptor)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(sentinel.read_text(), "safe")
            self.assertEqual(
                (writable / "result").read_text(), "written-through-bind"
            )

    def test_bwrap_root_remount_blocks_unmanifested_host_abi_files(self) -> None:
        """Exercise the pinned kernel/bwrap mount ordering, not just argv text."""

        with tempfile.TemporaryDirectory() as temporary:
            writable = Path(temporary) / "writable"
            writable.mkdir(mode=0o700)
            sources = {
                "/usr/bin/bash": "/usr/bin/bash",
                "/usr/lib64/ld-linux-x86-64.so.2": "/usr/lib64/ld-linux-x86-64.so.2",
                "/usr/lib64/libc.so.6": "/usr/lib64/libc.so.6",
                "/usr/lib64/libtinfo.so.6": "/usr/lib64/libtinfo.so.6",
            }
            descriptors = {
                target: os.open(source, os.O_RDONLY | os.O_CLOEXEC)
                for target, source in sources.items()
            }
            writable_descriptor = os.open(
                writable, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
            )
            argv = [
                "/usr/bin/bwrap",
                "--die-with-parent",
                "--new-session",
                "--unshare-all",
                "--unshare-user",
                "--uid",
                str(os.geteuid()),
                "--gid",
                str(os.getegid()),
                "--disable-userns",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--dir",
                "/usr",
                "--dir",
                "/usr/bin",
                "--dir",
                "/usr/lib64",
                "--dir",
                "/writable",
                "--symlink",
                "usr/lib64",
                "/lib64",
            ]
            for target, descriptor in sorted(descriptors.items()):
                argv.extend(["--ro-bind-fd", str(descriptor), target])
            argv.extend(
                [
                    "--bind-fd",
                    str(writable_descriptor),
                    "/writable",
                    "--remount-ro",
                    "/",
                    "--clearenv",
                    "--setenv",
                    "PATH",
                    "/usr/bin",
                    "--setenv",
                    "LANG",
                    "C",
                    "--setenv",
                    "LC_ALL",
                    "C",
                    "--",
                    "/usr/bin/bash",
                    "--noprofile",
                    "--norc",
                    "-c",
                    "if (: > /usr/lib64/evil.so) 2>/dev/null; then exit 90; fi; "
                    "if (: > /usr/lib64/libc.so.6) 2>/dev/null; then exit 91; fi; "
                    "if test -e /usr/lib64/libnvidia-tileiras.so.610.57.04; then exit 92; fi; "
                    "if test -e /usr/lib64/libnvidia-pkcs11-openssl3.so.610.57.04; then exit 93; fi; "
                    "if (: > /usr/lib64/libnvidia-tileiras.so.610.57.04) 2>/dev/null; then exit 94; fi; "
                    "printf authorized > /writable/result",
                ]
            )
            try:
                completed = subprocess.run(
                    argv,
                    check=False,
                    capture_output=True,
                    text=True,
                    close_fds=True,
                    pass_fds=tuple([*descriptors.values(), writable_descriptor]),
                    env=LAUNCHER._clean_helper_environment(),
                    timeout=10,
                )
            finally:
                for descriptor in descriptors.values():
                    os.close(descriptor)
                os.close(writable_descriptor)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertFalse(Path("/usr/lib64/evil.so").exists())
            self.assertEqual((writable / "result").read_text(), "authorized")

    def test_lineage_preflight_is_two_phase_and_candidate_never_binds_hot_root(self) -> None:
        profile = launcher_profile()
        mappings = mapping_rows()
        sources = {
            row["name"]: 40 + ordinal
            for ordinal, row in enumerate(mappings)
        }
        common = dict(
            launcher_profile=profile,
            mappings=mappings,
            mapping_sources=sources,
            host_abi_sources={
                row["target"]: 200 + ordinal
                for ordinal, row in enumerate(host_abi_bindings())
            },
            root_source=10,
            root_target="/srv/himr",
            manifest_source=11,
            profile_source=12,
            root_control_source=13,
            root_control_target=(
                "/srv/himr/research/corpus/gpu-runtime/"
                "root-registration.json"
            ),
            output_directory_source=14,
            batch_manifest_sha256=ZERO,
            profile_sha256=ONE,
            root_sha256=TWO,
        )
        candidate = LAUNCHER.build_lineage_preflight_bwrap_argv(
            mode="candidate-synthetic-canary",
            candidate_read_sources={
                "/srv/himr/research/fixture.json": 15,
                "/srv/himr/research/audio.flac": 16,
            },
            **common,
        )
        triples = [candidate[index : index + 3] for index in range(len(candidate) - 2)]
        self.assertNotIn(
            ["--ro-bind-fd", "10", "/srv/himr"],
            triples,
        )
        self.assertEqual(
            candidate[candidate.index("--root-registration") + 1],
            "/run/himr-gpu/control/root.json",
        )
        self.assertFalse(any("/dev/nvidia" in value for value in candidate))
        self.assertIn("preflight-lineage", candidate)
        self.assertEqual(candidate.count("--remount-ro"), 1)
        self.assertGreater(
            candidate.index("--remount-ro"),
            max(
                index
                for index, value in enumerate(candidate)
                if value in {"--ro-bind-fd", "--bind-fd"}
            ),
        )
        self.assertIn("/opt/himr-gpu/corpus/src", candidate)
        production = LAUNCHER.build_lineage_preflight_bwrap_argv(
            mode="production",
            candidate_read_sources={},
            **common,
        )
        triples = [production[index : index + 3] for index in range(len(production) - 2)]
        self.assertIn(
            ["--ro-bind-fd", "10", "/srv/himr"],
            triples,
        )
        self.assertIn(
            [
                "--ro-bind-fd",
                "13",
                "/srv/himr/research/corpus/gpu-runtime/"
                "root-registration.json",
            ],
            triples,
        )
        self.assertEqual(
            production[production.index("--root-registration") + 1],
            "/srv/himr/research/corpus/gpu-runtime/"
            "root-registration.json",
        )
        self.assertLess(
            production.index("10"),
            production.index("13"),
            "the exact registration file must overlay its containing hot-root bind",
        )
        self.assertFalse(any("/dev/nvidia" in value for value in production))
        self.assertEqual(production.count("--remount-ro"), 1)
        local = LAUNCHER.build_lineage_preflight_bwrap_argv(
            mode="local-private-production",
            candidate_read_sources={},
            **common,
        )
        triples = [local[index : index + 3] for index in range(len(local) - 2)]
        self.assertIn(
            ["--ro-bind-fd", "10", "/srv/himr"], triples
        )
        self.assertIn(
            [
                "--ro-bind-fd",
                "13",
                "/srv/himr/research/corpus/gpu-runtime/"
                "root-registration.json",
            ],
            triples,
        )
        self.assertEqual(
            local[local.index("--root-registration") + 1],
            "/srv/himr/research/corpus/gpu-runtime/"
            "root-registration.json",
        )
        self.assertLess(
            local.index("10"),
            local.index("13"),
            "the exact registration file must overlay its containing hot-root bind",
        )
        self.assertFalse(any("/dev/nvidia" in value for value in local))

        outside = dict(common)
        outside["root_control_target"] = "/other/root-registration.json"
        with self.assertRaisesRegex(
            LAUNCHER.TrustedLauncherError, "strict descendant"
        ):
            LAUNCHER.build_lineage_preflight_bwrap_argv(
                mode="local-private-production",
                candidate_read_sources={},
                **outside,
            )

    def test_host_envelope_is_fail_closed_for_production(self) -> None:
        value = host_envelope()
        self.assertEqual(
            LAUNCHER.validate_host_envelope_observation(
                value,
                minimum_result_bytes=16 * 1024 * 1024,
                enforce=True,
            ),
            value,
        )
        failed = {**value, "effective": {**value["effective"], "memory_swap_max_bytes": 1}}
        failed["status"] = "report_only_failed"
        failed["violations"] = ["memory.swap.max"]
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "memory.swap.max"):
            LAUNCHER.validate_host_envelope_observation(
                failed,
                minimum_result_bytes=16 * 1024 * 1024,
                enforce=True,
            )

        self.assertEqual(
            LAUNCHER.validate_host_envelope_observation(
                failed,
                minimum_result_bytes=16 * 1024 * 1024,
                enforce=False,
            )["status"],
            "report_only_failed",
        )
        unbounded = json.loads(json.dumps(value))
        unbounded["effective"]["memory_max_bytes"] = None
        unbounded["rlimits"]["fsize"] = {"soft": None, "hard": None}
        unbounded["status"] = "report_only_failed"
        unbounded["violations"] = ["RLIMIT_FSIZE", "memory.max"]
        self.assertEqual(
            LAUNCHER.validate_host_envelope_observation(
                unbounded,
                minimum_result_bytes=16 * 1024 * 1024,
                enforce=False,
            )["violations"],
            ["RLIMIT_FSIZE", "memory.max"],
        )
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "RLIMIT_FSIZE"):
            LAUNCHER.validate_host_envelope_observation(
                unbounded,
                minimum_result_bytes=16 * 1024 * 1024,
                enforce=True,
            )

    def test_writable_root_authority_survives_expected_child_creation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "writable"
            root.mkdir(mode=0o700)
            before = root.lstat()
            (root / "authorized-child").mkdir(mode=0o700)
            after = root.lstat()
            self.assertNotEqual(
                LAUNCHER._directory_identity(before),
                LAUNCHER._directory_identity(after),
                "the fixture must exercise mutable directory metadata",
            )
            self.assertEqual(
                LAUNCHER._writable_directory_authority_identity(before),
                LAUNCHER._writable_directory_authority_identity(after),
            )
            root.chmod(0o750)
            self.assertNotEqual(
                LAUNCHER._writable_directory_authority_identity(before),
                LAUNCHER._writable_directory_authority_identity(root.lstat()),
            )

    def test_squashfuse_is_parent_death_armed_and_environment_cleared(self) -> None:
        fake = mock.Mock()
        fake.poll.return_value = None
        retained = SimpleNamespace(descriptor=23, fd_path="/proc/self/fd/23")
        with mock.patch.object(LAUNCHER.subprocess, "Popen", return_value=fake) as popen:
            with mock.patch.object(LAUNCHER, "_mount_present", return_value=True):
                observed = LAUNCHER.start_squashfuse(
                    "/usr/bin/squashfuse_ll", retained, Path("/tmp/private-image")
                )
        self.assertIs(observed, fake)
        kwargs = popen.call_args.kwargs
        self.assertEqual(kwargs["pass_fds"], (23,))
        self.assertEqual(kwargs["env"], LAUNCHER._clean_helper_environment())
        self.assertTrue(kwargs["start_new_session"])
        self.assertTrue(callable(kwargs["preexec_fn"]))

    def test_parent_death_hook_arms_sigkill_and_checks_parent_race(self) -> None:
        libc = mock.Mock()
        libc.prctl.return_value = 0
        with mock.patch.object(LAUNCHER.ctypes, "CDLL", return_value=libc):
            with mock.patch.object(LAUNCHER.os, "getppid", return_value=123):
                LAUNCHER._parent_death_setup(123)()
        libc.prctl.assert_called_once_with(
            LAUNCHER.PR_SET_PDEATHSIG,
            LAUNCHER.signal.SIGKILL,
            0,
            0,
            0,
        )
        with mock.patch.object(LAUNCHER.ctypes, "CDLL", return_value=libc):
            with mock.patch.object(LAUNCHER.os, "getppid", return_value=124):
                with mock.patch.object(LAUNCHER.os, "_exit") as exit_process:
                    LAUNCHER._parent_death_setup(123)()
        exit_process.assert_called_once_with(LAUNCHER.PDEATHSIG_FAILURE_STATUS)

    def test_stale_transient_recovery_is_scoped_and_reusable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            parent.chmod(0o700)
            runtime = parent / LAUNCHER.PRIVATE_RUNTIME_DIRECTORY
            runtime.mkdir(mode=0o700)
            stale = runtime / LAUNCHER.TRANSIENT_DIRECTORY
            stale.mkdir(mode=0o700)
            (stale / LAUNCHER.MOUNT_DIRECTORY).mkdir(mode=0o700)
            (stale / LAUNCHER.PREFLIGHT_DIRECTORY).mkdir(mode=0o700)
            transient, descriptor = LAUNCHER.prepare_transient_root(
                "/usr/bin/fusermount3", runtime_parent=parent
            )
            try:
                self.assertEqual(transient, stale)
                self.assertTrue((transient / LAUNCHER.MOUNT_DIRECTORY).is_dir())
                self.assertTrue((transient / LAUNCHER.PREFLIGHT_DIRECTORY).is_dir())
                LAUNCHER.cleanup_transient_root(
                    transient, "/usr/bin/fusermount3"
                )
            finally:
                os.close(descriptor)
            self.assertFalse(transient.exists())

    def test_stale_mount_recovery_uses_only_clean_unmount(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            transient = Path(temporary) / LAUNCHER.TRANSIENT_DIRECTORY
            transient.mkdir(mode=0o700)
            mountpoint = transient / LAUNCHER.MOUNT_DIRECTORY
            mountpoint.mkdir(mode=0o700)
            (transient / LAUNCHER.PREFLIGHT_DIRECTORY).mkdir(mode=0o700)
            with mock.patch.object(
                LAUNCHER, "_mount_present", side_effect=[True, False]
            ) as mounted:
                with mock.patch.object(
                    LAUNCHER, "_unmount", return_value=True
                ) as unmount:
                    LAUNCHER.cleanup_transient_root(
                        transient, "/usr/bin/fusermount3"
                    )
            unmount.assert_called_once_with("/usr/bin/fusermount3", mountpoint)
            self.assertEqual(mounted.call_count, 2)
            self.assertFalse(transient.exists())

    def test_isolated_shebang_ignores_hostile_pythonpath(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "executed"
            (root / "sitecustomize.py").write_text(
                f"from pathlib import Path\nPath({str(marker)!r}).write_text('bad')\n"
            )
            executable = root / "trusted-launcher-v2"
            executable.write_bytes(SOURCE.read_bytes())
            executable.chmod(0o500)
            completed = subprocess.run(
                [str(executable), "contracts"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env={"PATH": "/usr/bin", "PYTHONPATH": str(root), "LANG": "C"},
                timeout=10,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr.decode())
            self.assertFalse(marker.exists())

    def test_candidate_launch_wires_lineage_phase_before_descriptor_only_gpu_phase(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            hot = Path(temporary) / "hot"
            hot.mkdir(mode=0o700)

            def sealed(name: str, body: bytes = b"{}\n", mode: int = 0o400) -> tuple[Path, str]:
                path = hot / name
                path.write_bytes(body)
                path.chmod(mode)
                return path, hashlib.sha256(body).hexdigest()

            profile_path, profile_sha = sealed("profile.json")
            runtime_path, runtime_sha = sealed("runtime.json")
            root_path, root_sha = sealed("root.json")
            launcher_profile_path, launcher_profile_sha = sealed("launcher-profile.json")
            input_body = b"synthetic-audio"
            input_path, input_sha = sealed("input.flac", input_body)
            fixture_path, fixture_sha = sealed("fixture.json")
            image_path, image_sha = sealed("execution.squashfs", b"image")
            manifest_path = hot / "batch.json"
            for name in ("results", "events", "locks"):
                (hot / name).mkdir(mode=0o700)

            prod = production_profile()
            prod["hardware"]["device_index"] = 0
            registration = {
                "kind": LAUNCHER.ROOT_KIND,
                "schema_version": 1,
                "root_id": "himr-hot-main-v1",
                "tier": "hot_main_drive",
                "path": str(hot),
                "filesystem": {
                    "type": "btrfs",
                    "uuid": "27bd4222-a8f3-4d91-b48f-7ce38d70e507",
                },
                "owner": {"policy": "exact_uid", "uid": os.geteuid()},
                "predecessor": None,
                "historical_observation": None,
                "policy": dict(LAUNCHER.ROOT_POLICY),
                "identity_sha256": hashlib.sha256(b"registered root").hexdigest(),
                "registration_id": "gpurootreg_" + "d" * 32,
            }
            work_profile = {
                "path": str(profile_path),
                "sha256": profile_sha,
                "profile_id": prod["profile_id"],
                "identity_sha256": prod["identity_sha256"],
            }
            runtime_identity = hashlib.sha256(b"runtime identity").hexdigest()
            work_runtime = {
                "receipt_path": str(runtime_path),
                "receipt_sha256": runtime_sha,
                "receipt_id": "gpurtv2_" + "e" * 32,
                "identity_sha256": runtime_identity,
                "status": "candidate",
            }
            work_root = {
                "registration_path": str(root_path),
                "registration_sha256": root_sha,
                "registration_id": registration["registration_id"],
                "identity_sha256": registration["identity_sha256"],
                "root_id": registration["root_id"],
                "path": str(hot),
                "filesystem_uuid": registration["filesystem"]["uuid"],
                "tier": "hot_main_drive",
            }
            fixture_identity = hashlib.sha256(b"fixture identity").hexdigest()
            order_core = {
                "kind": LAUNCHER.WORK_ORDER_KIND,
                "schema_version": 5,
                "implementation_version": "0.5.0",
                "job_id": "synthetic-test",
                "input": {
                    "path": str(input_path),
                    "expected_sha256": input_sha,
                    "expected_byte_count": len(input_body),
                    "expected_duration_ms": 1000,
                    "media_id": f"media_sha256_{input_sha}",
                    "artifact_id": "artifact_" + "f" * 32,
                    "parent_processing_run_id": "synthetic-run",
                    "media_format": {
                        "container": "flac",
                        "codec": "flac",
                        "sample_rate_hz": 16000,
                        "channels": 1,
                        "sample_format": "s16",
                    },
                    "sealed_mode": "0400",
                    "timeline_offset_ms": 0,
                },
                "source_lineage": {
                    "kind": "synthetic_canary",
                    "fixture_manifest": {
                        "path": str(fixture_path),
                        "sha256": fixture_sha,
                        "identity_sha256": fixture_identity,
                        "fixture_id": "synthetic-fixture",
                    },
                    "fixture_case_id": "case-1",
                    "contains_corpus_media": False,
                    "scope": "purpose_built_synthetic_only",
                    "corpus_authority": "none",
                },
                "runtime_admission": work_runtime,
                "production_profile": work_profile,
                "hot_root": work_root,
                "execution_contract": {},
                "transcript_semantics": {},
                "catalog_context": None,
                "output": {"root": str(hot / "results")},
                "policy": {},
            }
            order_identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(order_core))
            order = {
                **order_core,
                "identity_sha256": order_identity,
                "work_order_id": f"gpuasrwo5_{order_identity[:32]}",
            }
            manifest = {
                "execution_class": "synthetic_canary",
                "hot_root": work_root,
                "production_profile": work_profile,
                "runtime_admission": work_runtime,
                "items": [
                    {
                        "ordinal": 1,
                        "work_order": order,
                        "input": {
                            "sha256": input_sha,
                            "byte_count": len(input_body),
                            "duration_ms": 1000,
                        },
                        "result": {},
                    }
                ],
            }
            manifest_identity = hashlib.sha256(
                LAUNCHER.canonical_bytes(manifest)
            ).hexdigest()
            manifest.update(
                {
                    "identity_sha256": manifest_identity,
                    "batch_id": f"gpuasrbatch2_{manifest_identity[:32]}",
                }
            )
            manifest_body = LAUNCHER.canonical_bytes(manifest)
            manifest_path.write_bytes(manifest_body)
            manifest_path.chmod(0o400)
            manifest_sha = hashlib.sha256(manifest_body).hexdigest()

            lp = launcher_profile()
            launcher_body = SOURCE.read_bytes()
            lp["launcher"] = {
                "path": str(SOURCE),
                "sha256": hashlib.sha256(launcher_body).hexdigest(),
            }
            lp["execution_image"] = {
                **lp["execution_image"],
                "path": str(image_path),
                "sha256": image_sha,
                "byte_count": len(b"image"),
            }
            lp["production_profile"] = {
                "path": str(profile_path),
                "sha256": profile_sha,
                "identity_sha256": prod["identity_sha256"],
            }
            lp["root_registration"] = {
                "path": str(root_path),
                "sha256": root_sha,
                "identity_sha256": registration["identity_sha256"],
                "registration_id": registration["registration_id"],
                "root_id": registration["root_id"],
            }
            lp["runtime_admission_install_path"] = str(runtime_path)

            runtime = {
                "identity_sha256": runtime_identity,
                "status": "candidate",
                "execution_image": {"logical_mappings": mapping_rows()},
            }

            controls = []
            for path, digest, label in (
                (launcher_profile_path, launcher_profile_sha, "launcher profile"),
                (runtime_path, runtime_sha, "runtime admission"),
                (profile_path, profile_sha, "production profile"),
                (root_path, root_sha, "root registration"),
                (manifest_path, manifest_sha, "batch manifest"),
            ):
                retained = LAUNCHER.retain_file(
                    path,
                    label,
                    expected_sha256=digest,
                    maximum=LAUNCHER.MAX_JSON_BYTES,
                    allowed_owner_modes={(os.geteuid(), 0o400)},
                    keep_body=True,
                )
                controls.append(retained)

            class FakeRoot:
                def __init__(self) -> None:
                    self.descriptor = os.open(hot, os.O_RDONLY | os.O_DIRECTORY)

                def verify(self) -> None:
                    os.fstat(self.descriptor)

                def close(self) -> None:
                    if self.descriptor >= 0:
                        os.close(self.descriptor)
                        self.descriptor = -1

            class FakeMapping:
                def __init__(self) -> None:
                    self.descriptor = os.open("/dev/null", os.O_RDONLY)

                def close(self) -> None:
                    if self.descriptor >= 0:
                        os.close(self.descriptor)
                        self.descriptor = -1

                def verify(self) -> None:
                    os.fstat(self.descriptor)

            fake_root = FakeRoot()
            fake_root_descriptor = fake_root.descriptor
            fake_mappings = {
                name: FakeMapping() for name in LAUNCHER.REQUIRED_MAPPING_NAMES
            }
            fake_host_abi_files = [
                FakeMapping() for _row in lp["host_abi"]["libraries"]
            ]
            fake_host_abi_bindings = host_abi_bindings(lp["host_abi"])
            for retained, binding in zip(
                fake_host_abi_files, fake_host_abi_bindings, strict=True
            ):
                retained.path = Path(binding["source_path"])
            fake_host_abi_summary = host_abi_summary(lp["host_abi"])
            fake_host_abi_root = mock.Mock()
            mapping_descriptors = {
                name: retained.descriptor
                for name, retained in fake_mappings.items()
            }
            transient = Path(temporary) / "transient"
            transient.mkdir(mode=0o700)
            (transient / LAUNCHER.MOUNT_DIRECTORY).mkdir(mode=0o700)
            (transient / LAUNCHER.PREFLIGHT_DIRECTORY).mkdir(mode=0o700)
            lock_path = Path(temporary) / "launch.lock"
            lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)

            observed_preflight_fds: list[int] = []

            def preflight(*_args, **_kwargs):
                observed_preflight_fds.extend(_args[1])
                core = {
                    "kind": LAUNCHER.LINEAGE_ATTESTATION_KIND,
                    "schema_version": 2,
                    "implementation_version": LAUNCHER.IMPLEMENTATION_VERSION,
                    "batch": {
                        "batch_id": manifest["batch_id"],
                        "identity_sha256": manifest["identity_sha256"],
                        "physical_sha256": manifest_sha,
                    },
                    "production_profile": {
                        "profile_id": prod["profile_id"],
                        "identity_sha256": prod["identity_sha256"],
                    },
                    "root_registration": {
                        "registration_id": registration["registration_id"],
                        "identity_sha256": registration["identity_sha256"],
                        "filesystem_uuid": registration["filesystem"]["uuid"],
                    },
                    "items": [
                        {
                            "ordinal": 1,
                            "work_order_identity_sha256": order_identity,
                            "lineage": {
                                "kind": "synthetic_canary",
                                "identity_sha256": fixture_identity,
                                "source_id": "synthetic-fixture",
                                "member_id": None,
                                "case_id": "case-1",
                            },
                            "input": {
                                "sha256": input_sha,
                                "byte_count": len(input_body),
                            },
                            "status": "passed",
                        }
                    ],
                    "policy": dict(LAUNCHER.LINEAGE_POLICY),
                }
                identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(core))
                value = {
                    **core,
                    "identity_sha256": identity,
                    "attestation_id": f"gpuasrlineage2_{identity[:32]}",
                }
                body = LAUNCHER.canonical_bytes(value)
                output = transient / LAUNCHER.PREFLIGHT_DIRECTORY / "lineage-preflight.json"
                LAUNCHER._write_new(output, body, 0o400)
                summary = {
                    "status": "passed",
                    "attestation_id": value["attestation_id"],
                    "identity_sha256": identity,
                    "physical_sha256": hashlib.sha256(body).hexdigest(),
                }
                return 0, json.dumps(summary, sort_keys=True, separators=(",", ":")).encode() + b"\n", b""

            args = SimpleNamespace(
                mode="candidate-synthetic-canary",
                launcher_profile=str(launcher_profile_path),
                expected_launcher_profile_sha256=launcher_profile_sha,
                runtime_admission=str(runtime_path),
                expected_runtime_admission_sha256=runtime_sha,
                production_profile=str(profile_path),
                expected_production_profile_sha256=profile_sha,
                root_registration=str(root_path),
                expected_root_registration_sha256=root_sha,
                batch_manifest=str(manifest_path),
                expected_batch_sha256=manifest_sha,
                writable_result_root=str(hot / "results"),
                writable_event_root=str(hot / "events"),
                writable_lock_root=str(hot / "locks"),
            )
            main_argv: list[str] = []
            observed_main_fds: list[int] = []

            def run_main(argv, pass_fds, *_args, **_kwargs):
                main_argv.extend(argv)
                observed_main_fds.extend(pass_fds)
                return 0

            with mock.patch.object(
                LAUNCHER,
                "_load_control",
                side_effect=[
                    (controls[0], {}),
                    (controls[1], {}),
                    (controls[2], {}),
                    (controls[3], {}),
                    (controls[4], manifest),
                ],
            ), mock.patch.object(LAUNCHER, "validate_launcher_profile", return_value=lp), mock.patch.object(
                LAUNCHER, "validate_runtime_receipt", return_value=runtime
            ), mock.patch.object(
                LAUNCHER, "validate_production_profile", return_value=prod
            ), mock.patch.object(
                LAUNCHER, "validate_root_registration", return_value=registration
            ), mock.patch.object(
                LAUNCHER, "retain_root", return_value=fake_root
            ), mock.patch.object(
                LAUNCHER, "_observed_tool", side_effect=lambda path, _label: next(row for row in lp["system_tools"].values() if row["path"] == path)
            ), mock.patch.object(
                LAUNCHER, "observe_host_envelope", return_value=host_envelope()
            ), mock.patch.object(
                LAUNCHER,
                "resolve_gpu_observation",
                return_value={
                    "uuid": GPU_UUID,
                    "host_index": 0,
                    "device_minor": 0,
                    "visible_index": 0,
                    "driver_version": "610.57.04",
                    "compute_capability": [8, 6],
                    "minimum_driver_version": "610.57.04",
                    "minimum_compute_capability": [8, 6],
                },
            ), mock.patch.object(
                LAUNCHER,
                "validate_gpu_devices",
                return_value=[
                    "/dev/nvidiactl",
                    "/dev/nvidia-uvm",
                    "/dev/nvidia-uvm-tools",
                    "/dev/nvidia0",
                ],
            ), mock.patch.object(
                LAUNCHER,
                "replay_host_abi_manifest",
                return_value=(
                    fake_host_abi_files,
                    fake_host_abi_bindings,
                    fake_host_abi_summary,
                    fake_host_abi_root,
                ),
            ), mock.patch.object(
                LAUNCHER,
                "observe_host_abi_platform",
                return_value=lp["host_abi"]["platform"],
            ), mock.patch.object(
                LAUNCHER, "prepare_transient_root", return_value=(transient, lock_fd)
            ), mock.patch.object(
                LAUNCHER, "start_squashfuse", return_value=None
            ), mock.patch.object(
                LAUNCHER,
                "verify_mounted_mappings",
                return_value=fake_mappings,
            ), mock.patch.object(
                LAUNCHER, "_run_bounded_child", side_effect=preflight
            ), mock.patch.object(LAUNCHER, "_run_child", side_effect=run_main):
                self.assertEqual(LAUNCHER.launch(args), 0)
            self.assertIn("--lineage-preflight-attestation", main_argv)
            self.assertIn("CUDA_VISIBLE_DEVICES", main_argv)
            self.assertEqual(main_argv[main_argv.index("CUDA_VISIBLE_DEVICES") + 1], GPU_UUID)
            self.assertNotIn(
                fake_root_descriptor,
                observed_preflight_fds,
                "candidate preflight must not inherit the retained hot-root FD",
            )
            self.assertNotIn(
                fake_root_descriptor,
                observed_main_fds,
                "main inference must not inherit the retained hot-root FD",
            )
            preflight_mapping_names = {
                "application_root",
                "application_support_root",
                "runtime_root",
            }
            self.assertEqual(
                set(observed_preflight_fds) & set(mapping_descriptors.values()),
                {
                    mapping_descriptors[name]
                    for name in preflight_mapping_names
                },
            )
            main_mapping_names = {
                row["name"]
                for row in LAUNCHER._minimal_execution_bindings(mapping_rows())
            }
            self.assertEqual(
                set(observed_main_fds) & set(mapping_descriptors.values()),
                {mapping_descriptors[name] for name in main_mapping_names},
            )
            self.assertFalse(
                any(
                    main_argv[index] == "--ro-bind"
                    and main_argv[index + 2] == str(hot)
                    for index in range(len(main_argv) - 2)
                )
            )

    def test_retained_large_file_hash_path_does_not_keep_body(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "image"
            body = b"x" * (LAUNCHER.HASH_CHUNK_BYTES + 17)
            path.write_bytes(body)
            path.chmod(0o400)
            with LAUNCHER.retain_file(
                path,
                "test image",
                expected_sha256=hashlib.sha256(body).hexdigest(),
                maximum=len(body),
                allowed_owner_modes={(os.geteuid(), 0o400)},
                keep_body=False,
            ) as retained:
                self.assertIsNone(retained.body)
                self.assertEqual(retained.sha256, hashlib.sha256(body).hexdigest())

    def test_verify_mounted_mappings_rejects_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            mount = Path(temporary)
            mappings = mapping_rows()
            directory_paths = []
            for row in mappings:
                path = mount / row["image_relative_path"]
                path.parent.mkdir(parents=True, exist_ok=True)
                if row["role"].endswith("directory") or row["role"] in {"application_root", "application_support_root", "runtime_root", "model_bundle", "model_root", "shared_library_directory"}:
                    path.mkdir(exist_ok=True)
                    directory_paths.append(path)
                else:
                    path.write_bytes(b"x")
                    path.chmod(0o555 if row["role"] == "executable" else 0o444)
            for path in sorted(directory_paths, key=lambda item: len(item.parts), reverse=True):
                path.chmod(0o555)
            sources = LAUNCHER.verify_mounted_mappings(mount, mappings)
            self.assertEqual(set(sources), LAUNCHER.REQUIRED_MAPPING_NAMES)
            self.assertTrue(all(source.descriptor >= 3 for source in sources.values()))
            bad_row = next(row for row in mappings if row["name"] == "worker_source")
            bad = mount / bad_row["image_relative_path"]
            bad.parent.chmod(0o755)
            bad.unlink()
            bad.symlink_to("missing")
            with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "unavailable|wrong file kind"):
                LAUNCHER.verify_mounted_mappings(mount, [bad_row])
            for source in sources.values():
                source.close()

    def test_cold_paths_rejected_lexically_without_access(self) -> None:
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "archive tier"):
            LAUNCHER.normalized_absolute_path("/mnt/archive/HIMR/video.mp4", "test")

    def test_retained_control_rejects_symlinked_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            actual = root / "actual"
            actual.mkdir()
            body = b"{}\n"
            (actual / "control.json").write_bytes(body)
            (actual / "control.json").chmod(0o400)
            (root / "alias").symlink_to(actual, target_is_directory=True)
            with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "symlink component"):
                LAUNCHER.retain_file(
                    root / "alias/control.json",
                    "control",
                    expected_sha256=hashlib.sha256(body).hexdigest(),
                    maximum=100,
                    allowed_owner_modes={(os.geteuid(), 0o400)},
                    keep_body=True,
                )

    def test_stage_install_is_no_replace_and_emits_only_reviewable_argv(self) -> None:
        profile = launcher_profile_core()
        spec = {
            "kind": LAUNCHER.INSTALL_SPEC_KIND,
            "schema_version": 2,
            "launcher_install_path": profile["launcher"]["path"],
            "launcher_profile_install_path": "/etc/himr-gpu/launcher-profile-v2.json",
            "runtime_admission_install_path": profile["runtime_admission_install_path"],
            "execution_image": profile["execution_image"],
            "production_profile": profile["production_profile"],
            "root_registration": profile["root_registration"],
            "system_tool_paths": dict(LAUNCHER.SYSTEM_TOOL_PATHS),
            "host_abi": profile["host_abi"],
            "sandbox": profile["sandbox"],
            "policy": dict(LAUNCHER.POLICY),
        }
        body = LAUNCHER.canonical_bytes(spec)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "spec.json"
            source.write_bytes(body)
            source.chmod(0o400)
            staging = root / "stage"
            staging.mkdir(mode=0o700)

            def observe(path: str, _label: str) -> dict:
                name = next(name for name, expected in LAUNCHER.SYSTEM_TOOL_PATHS.items() if expected == path)
                return profile["system_tools"][name]

            args = SimpleNamespace(
                spec=str(source),
                expected_spec_sha256=hashlib.sha256(body).hexdigest(),
                staging_dir=str(staging),
            )
            with mock.patch.object(LAUNCHER, "_observed_tool", side_effect=observe):
                manifest = LAUNCHER.stage_install(args)
            self.assertEqual(manifest["kind"], LAUNCHER.INSTALL_MANIFEST_KIND)
            self.assertTrue(all(row[0] == "/usr/bin/install" for row in manifest["install_argv"]))
            self.assertEqual(stat.S_IMODE((staging / "trusted-launcher-v2").stat().st_mode), 0o500)
            self.assertEqual(stat.S_IMODE((staging / "launcher-profile-v2.json").stat().st_mode), 0o400)
            with mock.patch.object(LAUNCHER, "_observed_tool", side_effect=observe):
                with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "empty"):
                    LAUNCHER.stage_install(args)

    def test_launch_attestation_is_exact_and_semantically_sealed(self) -> None:
        profile = launcher_profile()
        runtime = runtime_receipt(profile, admitted=True)
        root_core = {
            "kind": LAUNCHER.ROOT_KIND,
            "schema_version": 1,
            "root_id": profile["root_registration"]["root_id"],
            "tier": "hot_main_drive",
            "path": "/srv/himr",
            "filesystem": {"type": "btrfs", "uuid": "27bd4222-a8f3-4d91-b48f-7ce38d70e507"},
            "owner": {"policy": "exact_uid", "uid": os.geteuid()},
            "predecessor": None,
            "historical_observation": None,
            "policy": dict(LAUNCHER.ROOT_POLICY),
        }
        root_identity = LAUNCHER.sha256_bytes(LAUNCHER.canonical_bytes(root_core))
        registration = {**root_core, "identity_sha256": root_identity, "registration_id": f"gpurootreg_{root_identity[:32]}"}
        prod = production_profile()
        manifest = {"execution_class": "production_private_asr", "identity_sha256": hashlib.sha256(b"batch identity").hexdigest()}
        plan = {
            "gpu": {
                "uuid": GPU_UUID,
                "host_index": 3,
                "device_minor": 3,
                "visible_index": 0,
                "devices": [
                    "/dev/nvidiactl",
                    "/dev/nvidia-uvm",
                    "/dev/nvidia-uvm-tools",
                    "/dev/nvidia3",
                ],
                "driver_version": "610.57.04",
                "compute_capability": [8, 6],
                "minimum_driver_version": "610.57.04",
                "minimum_compute_capability": [8, 6],
            },
            "host_abi_bindings": host_abi_bindings(profile["host_abi"]),
            "system_library_directories": [],
            "system_readonly_files": [],
            "network_access": False,
            "namespaces": ["cgroup_try", "ipc", "network", "pid", "user", "uts"],
        }
        attestation = LAUNCHER.make_launch_attestation(
            mode="production",
            launcher_profile=profile,
            launcher_profile_file={"path": "/etc/himr-gpu/launcher-profile-v2.json", "sha256": hashlib.sha256(b"lp").hexdigest()},
            runtime_receipt=runtime,
            runtime_file={"path": "/etc/himr-gpu/runtime-admission-v2.json", "sha256": hashlib.sha256(b"rt").hexdigest()},
            root_registration=registration,
            root_file={"path": profile["root_registration"]["path"], "sha256": profile["root_registration"]["sha256"]},
            production_profile=prod,
            production_profile_file={"path": profile["production_profile"]["path"], "sha256": profile["production_profile"]["sha256"]},
            manifest=manifest,
            manifest_file={"path": "/srv/himr/research/batch.json", "sha256": hashlib.sha256(b"batch").hexdigest()},
            lineage_preflight_file={
                "path": "/run/user/1000/himr-gpu-launcher-v2/transient/preflight-output/lineage-preflight.json",
                "sha256": hashlib.sha256(b"lineage file").hexdigest(),
                "identity_sha256": hashlib.sha256(b"lineage identity").hexdigest(),
                "attestation_id": "gpuasrlineage2_" + hashlib.sha256(b"lineage identity").hexdigest()[:32],
            },
            host_abi=host_abi_summary(profile["host_abi"]),
            plan=plan,
            host_envelope=host_envelope(),
            parent_network_namespace="net:[1234]",
        )
        self.assertEqual(LAUNCHER.validate_launch_attestation(attestation), attestation)
        forged = {**attestation, "parent_network_namespace": "net:[9999]"}
        with self.assertRaisesRegex(LAUNCHER.TrustedLauncherError, "semantic identity"):
            LAUNCHER.validate_launch_attestation(forged)


if __name__ == "__main__":
    unittest.main()
