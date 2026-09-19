from __future__ import annotations

import copy
import importlib.util
import os
import struct
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


SOURCE = Path(__file__).resolve().parents[1] / "gpu/host_abi_manifest_v1.py"
TEST_WORK_ROOT = SOURCE.parents[1] / ".test-work"
SPEC = importlib.util.spec_from_file_location("himr_test_host_abi_manifest_v1", SOURCE)
assert SPEC is not None and SPEC.loader is not None
HOST_ABI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HOST_ABI)

LAUNCHER_SOURCE = SOURCE.with_name("trusted_launcher_v2.py")
LAUNCHER_SPEC = importlib.util.spec_from_file_location(
    "himr_test_host_abi_launcher_v2", LAUNCHER_SOURCE
)
assert LAUNCHER_SPEC is not None and LAUNCHER_SPEC.loader is not None
LAUNCHER = importlib.util.module_from_spec(LAUNCHER_SPEC)
sys.modules[LAUNCHER_SPEC.name] = LAUNCHER
LAUNCHER_SPEC.loader.exec_module(LAUNCHER)


def platform() -> dict:
    return {
        "sysname": "Linux",
        "release": "7.1.10-test.x86_64",
        "version": "#1 SMP PREEMPT_DYNAMIC test",
        "machine": "x86_64",
        "nvidia_driver_version": "610.57.04",
        "nvidia_kernel_module_version": "610.57.04",
        "nvidia_kernel_module_report_sha256": "1" * 64,
        "nvidia_kernel_module_report_byte_count": 123,
    }


def consumer_scan(*, external: tuple[str, ...] = ("libroot.so.1",)) -> dict:
    consumer_body = elf64(
        soname="python3.12",
        needed=external,
        interpreter=HOST_ABI.SANDBOX_INTERPRETER,
    )
    cublas = elf64(soname="libcublas.so.12")
    cublas_lt = elf64(soname="libcublasLt.so.12")
    consumers = [
        {
            "image_relative_path": "runtime/bin/python3.12",
            "sha256": HOST_ABI.sha256_bytes(consumer_body),
            "byte_count": len(consumer_body),
            "elf": HOST_ABI.parse_elf64_x86_64(
                consumer_body, allow_origin_needed=True
            ),
        },
        {
            "image_relative_path": "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublas.so.12",
            "sha256": HOST_ABI.sha256_bytes(cublas),
            "byte_count": len(cublas),
            "elf": HOST_ABI.parse_elf64_x86_64(cublas),
        },
        {
            "image_relative_path": "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublasLt.so.12",
            "sha256": HOST_ABI.sha256_bytes(cublas_lt),
            "byte_count": len(cublas_lt),
            "elf": HOST_ABI.parse_elf64_x86_64(cublas_lt),
        },
    ]
    return seal_scan(consumers, list(external))


def seal_scan(
    consumers: list[dict],
    runtime_loaded_sonames: list[str],
    *,
    regular_file_count: int | None = None,
) -> dict:
    consumers = sorted(
        copy.deepcopy(consumers), key=lambda row: row["image_relative_path"]
    )
    load_roots = HOST_ABI._canonical_image_load_roots(consumers)
    external = HOST_ABI._consumer_external_sonames(
        consumers, sorted(runtime_loaded_sonames), load_roots
    )
    core = {
        "execution_image_identity_sha256": "2" * 64,
        "source_tree_identity_sha256": "3" * 64,
        "source_tree_regular_file_count": regular_file_count or len(consumers),
        "elf_file_count": len(consumers),
        "consumers": consumers,
        "load_roots": load_roots,
        "runtime_loaded_sonames": sorted(runtime_loaded_sonames),
        "external_sonames": external,
    }
    return {
        **core,
        "identity_sha256": HOST_ABI.sha256_bytes(HOST_ABI.canonical_bytes(core)),
    }


def reseal_manifest(value: dict) -> dict:
    core = {
        key: copy.deepcopy(item)
        for key, item in value.items()
        if key not in {"identity_sha256", "manifest_id"}
    }
    identity = HOST_ABI.sha256_bytes(HOST_ABI.canonical_bytes(core))
    return {
        **core,
        "identity_sha256": identity,
        "manifest_id": f"gpuhostabi_{identity[:32]}",
    }


def consumer_row(path: str, body: bytes) -> dict:
    return {
        "image_relative_path": path,
        "sha256": HOST_ABI.sha256_bytes(body),
        "byte_count": len(body),
        "elf": HOST_ABI.parse_elf64_x86_64(
            body, allow_origin_needed=True
        ),
    }


def elf64(
    *,
    soname: str,
    needed: tuple[str, ...] = (),
    rpath: str | None = None,
    runpath: str | None = None,
    interpreter: str | None = None,
) -> bytes:
    """Create the smallest useful ELF64 dynamic object for parser tests."""

    strings = bytearray(b"\x00")

    def string(value: str) -> int:
        offset = len(strings)
        strings.extend(value.encode("ascii") + b"\x00")
        return offset

    soname_offset = string(soname)
    needed_offsets = [string(value) for value in needed]
    rpath_offset = string(rpath) if rpath is not None else None
    runpath_offset = string(runpath) if runpath is not None else None
    dynamic_rows = [(HOST_ABI.DT_STRTAB, 0x400300), (HOST_ABI.DT_STRSZ, len(strings))]
    dynamic_rows.extend((HOST_ABI.DT_NEEDED, value) for value in needed_offsets)
    dynamic_rows.append((HOST_ABI.DT_SONAME, soname_offset))
    if rpath_offset is not None:
        dynamic_rows.append((HOST_ABI.DT_RPATH, rpath_offset))
    if runpath_offset is not None:
        dynamic_rows.append((HOST_ABI.DT_RUNPATH, runpath_offset))
    dynamic_rows.append((HOST_ABI.DT_NULL, 0))
    dynamic = b"".join(struct.pack("<qQ", *row) for row in dynamic_rows)
    total = 0x300 + len(strings)
    body = bytearray(total)
    ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + b"\x00" * 8
    struct.pack_into(
        "<16sHHIQQQIHHHHHH",
        body,
        0,
        ident,
        3,
        HOST_ABI.EM_X86_64,
        1,
        0,
        64,
        0,
        0,
        64,
        56,
        3 if interpreter is not None else 2,
        0,
        0,
        0,
    )
    struct.pack_into("<IIQQQQQQ", body, 64, HOST_ABI.PT_LOAD, 5, 0, 0x400000, 0, total, total, 0x1000)
    struct.pack_into(
        "<IIQQQQQQ",
        body,
        120,
        HOST_ABI.PT_DYNAMIC,
        4,
        0x180,
        0x400180,
        0,
        len(dynamic),
        len(dynamic),
        8,
    )
    if interpreter is not None:
        encoded_interpreter = interpreter.encode("ascii") + b"\x00"
        if len(encoded_interpreter) > 0x80:
            raise ValueError("test interpreter is oversized")
        struct.pack_into(
            "<IIQQQQQQ",
            body,
            176,
            HOST_ABI.PT_INTERP,
            4,
            0x240,
            0x400240,
            0,
            len(encoded_interpreter),
            len(encoded_interpreter),
            1,
        )
        body[0x240 : 0x240 + len(encoded_interpreter)] = encoded_interpreter
    body[0x180 : 0x180 + len(dynamic)] = dynamic
    body[0x300 : 0x300 + len(strings)] = strings
    return bytes(body)


class HostABIManifestV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.uid = os.geteuid()
        self.gid = os.getegid()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_library(self, name: str, body: bytes) -> Path:
        path = self.root / name
        path.write_bytes(body)
        path.chmod(0o555)
        return path

    def manifest(self) -> dict:
        self.write_library("libdep.so.1", elf64(soname="libdep.so.1"))
        versioned = self.write_library(
            "libroot.so.1.2",
            elf64(soname="libroot.so.1", needed=("libdep.so.1",)),
        )
        alias = self.root / "libroot.so.1"
        alias.symlink_to(versioned.name)
        return HOST_ABI.build_manifest(
            [str(alias)],
            consumer_scan=consumer_scan(),
            library_roots=[str(self.root)],
            platform=platform(),
            required_owner_uid=self.uid,
            required_owner_gid=self.gid,
        )

    def test_recursive_closure_is_canonical_and_replayable(self) -> None:
        manifest = self.manifest()
        self.assertEqual(
            [Path(row["sandbox_path"]).name for row in manifest["libraries"]],
            ["libdep.so.1", "libroot.so.1"],
        )
        self.assertEqual(
            manifest["dependency_edges"],
            [
                {
                    "consumer": str(self.root / "libroot.so.1"),
                    "needed": "libdep.so.1",
                    "provider": str(self.root / "libdep.so.1"),
                }
            ],
        )
        self.assertEqual(HOST_ABI.validate_manifest(manifest), manifest)
        self.assertEqual(
            HOST_ABI.replay_manifest(
                manifest,
                observed_platform=platform(),
                require_production_owner=False,
            ),
            manifest,
        )

    def test_alias_retarget_is_detected_even_when_both_targets_are_valid(self) -> None:
        manifest = self.manifest()
        alternate = self.write_library(
            "libroot.so.1.3",
            elf64(soname="libroot.so.1", needed=("libdep.so.1",)),
        )
        alias = self.root / "libroot.so.1"
        alias.unlink()
        alias.symlink_to(alternate.name)
        with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "alias chain changed"):
            HOST_ABI.replay_manifest(
                manifest,
                observed_platform=platform(),
                require_production_owner=False,
            )

    def test_changed_library_bytes_are_detected(self) -> None:
        manifest = self.manifest()
        dependency = self.root / "libdep.so.1"
        dependency.chmod(0o755)
        dependency.write_bytes(elf64(soname="libdep.so.1") + b"changed")
        dependency.chmod(0o555)
        with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "changed"):
            HOST_ABI.replay_manifest(
                manifest,
                observed_platform=platform(),
                require_production_owner=False,
            )

    def test_missing_dependency_fails_closed(self) -> None:
        root = self.write_library(
            "libroot.so.1",
            elf64(soname="libroot.so.1", needed=("libabsent.so.1",)),
        )
        with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "resolves to 0"):
            HOST_ABI.build_manifest(
                [str(root)],
                consumer_scan=consumer_scan(external=("libroot.so.1",)),
                library_roots=[str(self.root)],
                platform=platform(),
                required_owner_uid=self.uid,
                required_owner_gid=self.gid,
            )

    def test_forged_aggregate_dependency_edge_is_rejected(self) -> None:
        manifest = self.manifest()
        forged = copy.deepcopy(manifest)
        forged["dependency_edges"] = []
        core = {key: value for key, value in forged.items() if key not in {"identity_sha256", "manifest_id"}}
        identity = HOST_ABI.sha256_bytes(HOST_ABI.canonical_bytes(core))
        forged["identity_sha256"] = identity
        forged["manifest_id"] = f"gpuhostabi_{identity[:32]}"
        with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "dependency graph"):
            HOST_ABI.validate_manifest(forged)

    def test_disconnected_extra_library_is_rejected(self) -> None:
        manifest = self.manifest()
        body = elf64(soname="libextra.so.1")
        extra = {
            "sandbox_path": str(self.root / "libextra.so.1"),
            "source_path": str(self.root / "libextra.so.1"),
            "sha256": HOST_ABI.sha256_bytes(body),
            "byte_count": len(body),
            "uid": self.uid,
            "gid": self.gid,
            "mode": "0555",
            "aliases": [],
            "elf": HOST_ABI.parse_elf64_x86_64(body),
        }
        forged = copy.deepcopy(manifest)
        forged["libraries"].append(extra)
        forged["libraries"].sort(key=lambda row: row["sandbox_path"])
        forged = reseal_manifest(forged)
        with self.assertRaisesRegex(
            HOST_ABI.HostABIManifestError, "disconnected or unreachable"
        ):
            HOST_ABI.validate_manifest(forged)

    def test_forged_alias_cycle_is_rejected_by_schema(self) -> None:
        manifest = self.manifest()
        forged = copy.deepcopy(manifest)
        row = next(
            row
            for row in forged["libraries"]
            if Path(row["sandbox_path"]).name == "libroot.so.1"
        )
        alias_a = row["sandbox_path"]
        alias_b = str(self.root / "libroot-cycle.so.1")
        metadata = {
            "uid": self.uid,
            "gid": self.gid,
            "mode": "0777",
        }
        row["aliases"] = [
            {"path": alias_a, "target": Path(alias_b).name, **metadata},
            {"path": alias_b, "target": Path(alias_a).name, **metadata},
            {
                "path": alias_a,
                "target": Path(row["source_path"]).name,
                **metadata,
            },
        ]
        forged = reseal_manifest(forged)
        with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "cycles"):
            HOST_ABI.validate_manifest(forged)

    def test_kernel_or_driver_change_is_detected_before_library_replay(self) -> None:
        manifest = self.manifest()
        changed = {**platform(), "release": "7.1.11-test.x86_64"}
        with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "platform differs"):
            HOST_ABI.replay_manifest(
                manifest,
                observed_platform=changed,
                require_production_owner=False,
            )

    def test_parser_rejects_non_basename_needed_entry(self) -> None:
        body = elf64(soname="libroot.so.1", needed=("../libevil.so",))
        with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "non-basename"):
            HOST_ABI.parse_elf64_x86_64(body)

    def test_parser_rejects_duplicate_pt_interp(self) -> None:
        body = bytearray(
            elf64(
                soname="python3.12",
                interpreter=HOST_ABI.SANDBOX_INTERPRETER,
            )
        )
        struct.pack_into("<H", body, 56, 4)
        second = b"/second/ld.so\x00"
        struct.pack_into(
            "<IIQQQQQQ",
            body,
            232,
            HOST_ABI.PT_INTERP,
            4,
            0x2A0,
            0x4002A0,
            0,
            len(second),
            len(second),
            1,
        )
        body[0x2A0 : 0x2A0 + len(second)] = second
        with self.assertRaisesRegex(
            HOST_ABI.HostABIManifestError, "multiple PT_INTERP"
        ):
            HOST_ABI.parse_elf64_x86_64(bytes(body))

    def test_parser_rejects_load_file_size_above_memory_size(self) -> None:
        body = bytearray(elf64(soname="libroot.so.1"))
        struct.pack_into("<Q", body, 64 + 40, 1)
        with self.assertRaisesRegex(
            HOST_ABI.HostABIManifestError,
            "PT_LOAD file size above its memory size",
        ):
            HOST_ABI.parse_elf64_x86_64(bytes(body))

    def test_consumer_interpreter_is_exact(self) -> None:
        metadata = HOST_ABI.parse_elf64_x86_64(
            elf64(
                soname="python3.12",
                interpreter=HOST_ABI.SANDBOX_INTERPRETER,
            )
        )
        metadata["interpreter"] = "/tmp/attacker-loader"
        with self.assertRaisesRegex(
            HOST_ABI.HostABIManifestError, "exact sandbox ABI"
        ):
            HOST_ABI._consumer_elf(metadata, "test consumer")

    def test_unsupported_loader_search_paths_are_rejected(self) -> None:
        for search in (
            "/opt/himr-gpu/runtime/lib/private",
            "$LIB/private",
            "${ORIGIN}/private",
            "$ORIGIN//private",
            "",
        ):
            with self.subTest(search=search):
                metadata = HOST_ABI.parse_elf64_x86_64(
                    elf64(soname="consumer.so", rpath=search)
                )
                with self.assertRaisesRegex(
                    HOST_ABI.HostABIManifestError,
                    "unsupported|noncanonical",
                ):
                    HOST_ABI._consumer_elf(metadata, "test consumer")

    def test_driver_dlopen_roots_require_nvvm70_and_exclude_unapproved_features(
        self,
    ) -> None:
        roots = HOST_ABI.production_runtime_loaded_sonames("610.57.04")
        self.assertIn("libnvidia-nvvm.so.4", roots)
        self.assertIn("libnvidia-nvvm70.so.4", roots)
        excluded = HOST_ABI.reviewed_excluded_driver_dlopen_sonames(
            "610.57.04"
        )
        self.assertTrue(set(roots).isdisjoint(excluded))
        self.assertEqual(
            excluded,
            [
                "libnvidia-pkcs11-openssl3.so.610.57.04",
                "libnvidia-pkcs11.so.610.57.04",
                "libnvidia-tileiras.so.610.57.04",
            ],
        )

    def test_consumer_schema_matches_launcher_and_rejects_unsafe_paths(self) -> None:
        scan = consumer_scan(external=("libcuda.so.1",))
        self.assertEqual(
            HOST_ABI.normalize_consumer_scan(scan),
            LAUNCHER._normalize_host_abi_consumer_scan(scan),
        )
        for path in (
            "runtime/lib/naïve.so",
            "runtime/lib/back\\slash.so",
            "runtime/lib/new\nline.so",
            "runtime/" + "x" * 256,
        ):
            with self.subTest(path=path):
                with self.assertRaises(HOST_ABI.HostABIManifestError):
                    HOST_ABI._relative_image_path(path, "test image path")
                with self.assertRaises(LAUNCHER.TrustedLauncherError):
                    LAUNCHER.normalized_relative_path(path, "test image path")

    def test_projected_provider_requires_literal_reachable_filename(self) -> None:
        scan = consumer_scan(external=("libcuda.so.1",))
        consumers = copy.deepcopy(scan["consumers"])
        python = next(
            row
            for row in consumers
            if row["image_relative_path"] == "runtime/bin/python3.12"
        )
        python["elf"]["needed"] = ["libfoo.so.1"]
        python["elf"]["runpath"] = ["$ORIGIN/../lib"]
        provider_body = elf64(soname="libfoo.so.1")
        consumers.append(
            consumer_row("runtime/lib/libfoo.so.1.2", provider_body)
        )
        value = seal_scan(consumers, ["libcuda.so.1"])
        self.assertIn("libfoo.so.1", value["external_sonames"])
        self.assertEqual(HOST_ABI.normalize_consumer_scan(value), value)

    def test_unrelated_private_directory_is_not_global_loader_authority(self) -> None:
        scan = consumer_scan(external=("libcuda.so.1",))
        consumers = copy.deepcopy(scan["consumers"])
        python = next(
            row
            for row in consumers
            if row["image_relative_path"] == "runtime/bin/python3.12"
        )
        python["elf"]["needed"] = [
            "libcublasLt.so.12",
            "libghost.so.1",
        ]
        ghost = elf64(soname="libghost.so.1")
        consumers.append(
            consumer_row(
                "runtime/lib/python3.12/site-packages/numpy.libs/libghost.so.1",
                ghost,
            )
        )
        value = seal_scan(consumers, ["libcuda.so.1"])
        self.assertNotIn("libcublasLt.so.12", value["external_sonames"])
        self.assertIn("libghost.so.1", value["external_sonames"])

    def test_rpath_inherits_but_runpath_is_direct_only(self) -> None:
        base = consumer_scan(external=("libcuda.so.1",))["consumers"]
        extension_path = (
            "runtime/lib/python3.12/site-packages/ctranslate2/"
            "_ext.cpython-312-x86_64-linux-gnu.so"
        )
        private = "runtime/lib/python3.12/site-packages/ctranslate2.libs"

        def make(path_field: str) -> dict:
            extension = elf64(
                soname="_ext.so",
                needed=("libchild.so.1",),
                **{path_field: "$ORIGIN/../ctranslate2.libs"},
            )
            child = elf64(
                soname="libchild.so.1", needed=("libgrand.so.1",)
            )
            grand = elf64(soname="libgrand.so.1")
            consumers = [
                *copy.deepcopy(base),
                consumer_row(extension_path, extension),
                consumer_row(f"{private}/libchild.so.1", child),
                consumer_row(f"{private}/libgrand.so.1", grand),
            ]
            return seal_scan(consumers, ["libcuda.so.1"])

        inherited = make("rpath")
        direct_only = make("runpath")
        self.assertNotIn("libgrand.so.1", inherited["external_sonames"])
        self.assertIn("libgrand.so.1", direct_only["external_sonames"])

    def test_large_bundled_consumer_does_not_raise_host_library_limit(self) -> None:
        scan = consumer_scan()
        core = {
            key: copy.deepcopy(value)
            for key, value in scan.items()
            if key != "identity_sha256"
        }
        core["consumers"][0]["byte_count"] = 749_210_000
        value = {
            **core,
            "identity_sha256": HOST_ABI.sha256_bytes(
                HOST_ABI.canonical_bytes(core)
            ),
        }
        self.assertEqual(
            HOST_ABI.normalize_consumer_scan(value)["consumers"][0][
                "byte_count"
            ],
            749_210_000,
        )

    def test_consumer_scan_audits_every_regular_entry_and_prefers_image_provider(self) -> None:
        bundled = self.write_library(
            "libbundled.so.1",
            elf64(soname="libbundled.so.1", needed=("libhost.so.1",)),
        )
        consumer = self.write_library(
            "consumer",
            elf64(
                soname="python3.12",
                needed=("libbundled.so.1", "libhost.so.1"),
                runpath="$ORIGIN/../lib",
                interpreter=HOST_ABI.SANDBOX_INTERPRETER,
            ),
        )
        cublas = self.write_library(
            "libcublas.so.12", elf64(soname="libcublas.so.12")
        )
        cublas_lt = self.write_library(
            "libcublasLt.so.12", elf64(soname="libcublasLt.so.12")
        )
        text = self.root / "context.json"
        text.write_bytes(b"{}\n")
        text.chmod(0o444)

        entries = []
        for source, image in (
            (bundled, "runtime/lib/libbundled.so.1"),
            (consumer, "runtime/bin/python3.12"),
            (
                cublas,
                "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublas.so.12",
            ),
            (
                cublas_lt,
                "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublasLt.so.12",
            ),
            (text, "runtime/context.json"),
        ):
            body = source.read_bytes()
            entries.append(
                {
                    "kind": "regular_file",
                    "source_path": str(source),
                    "image_relative_path": image,
                    "sha256": HOST_ABI.sha256_bytes(body),
                    "byte_count": len(body),
                }
            )
        receipt = {
            "identity_sha256": "4" * 64,
            "source_tree": {
                "identity_sha256": "5" * 64,
                "regular_file_count": 5,
                "entries": entries,
            },
        }
        scan = HOST_ABI.derive_consumer_scan(
            receipt, runtime_loaded_sonames=["libcuda.so.1"]
        )
        self.assertEqual(scan["elf_file_count"], 4)
        self.assertEqual(
            scan["external_sonames"], ["libcuda.so.1", "libhost.so.1"]
        )
        self.assertNotIn("libbundled.so.1", scan["external_sonames"])

        text.chmod(0o644)
        text.write_bytes(b"\x7fELFmutated")
        with self.assertRaisesRegex(
            HOST_ABI.HostABIManifestError,
            "metadata is unsafe|differs from the execution image receipt",
        ):
            HOST_ABI.derive_consumer_scan(
                receipt, runtime_loaded_sonames=["libcuda.so.1"]
            )

    def test_production_validation_requires_root_owner(self) -> None:
        manifest = self.manifest()
        if self.uid != 0 or self.gid != 0:
            with self.assertRaisesRegex(HOST_ABI.HostABIManifestError, "root-owned"):
                HOST_ABI.validate_manifest(manifest, require_production_owner=True)

    def test_writable_library_root_ancestor_is_rejected(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        with tempfile.TemporaryDirectory(dir=TEST_WORK_ROOT) as temporary:
            root = Path(temporary) / "library-root"
            root.mkdir(mode=0o700)
            root.chmod(0o777)
            with self.assertRaisesRegex(
                HOST_ABI.HostABIManifestError, "mutable or unsafe"
            ):
                HOST_ABI.verify_trusted_directory_chain(
                    root, expected_uid=self.uid, expected_gid=self.gid
                )

    def test_exclusive_write_rejects_unreplayable_size(self) -> None:
        output = self.root / "oversized.json"
        with self.assertRaisesRegex(
            HOST_ABI.HostABIManifestError, "replay size bound"
        ):
            HOST_ABI._write_exclusive(
                output, {"payload": "x" * HOST_ABI.MAX_JSON_BYTES}
            )
        self.assertFalse(output.exists())

    def test_exclusive_write_reports_post_publish_failure(self) -> None:
        output = self.root / "published.json"
        with mock.patch.object(
            HOST_ABI.os,
            "fsync",
            side_effect=[None, OSError("directory fsync failed")],
        ):
            with self.assertRaisesRegex(
                HOST_ABI.HostABIManifestPublishedError,
                "is visible.*durability finalization failed",
            ):
                HOST_ABI._write_exclusive(output, {"small": True})
        self.assertTrue(output.is_file())


if __name__ == "__main__":
    unittest.main()
