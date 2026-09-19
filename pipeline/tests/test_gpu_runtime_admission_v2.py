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


ADMISSION = load_module(
    "himr_gpu_runtime_admission_v2_test_module",
    ROOT / "pipeline/gpu/admit_runtime_v2.py",
)


class GPURuntimeAdmissionV2Tests(unittest.TestCase):
    def spec(self, state: str = "candidate") -> dict[str, object]:
        return {
            "kind": ADMISSION.SPEC_KIND,
            "schema_version": ADMISSION.SCHEMA_VERSION,
            "requested_state": state,
            "root_registration": {
                "path": "/private/root.json",
                "sha256": "1" * 64,
                "registration_id": "gpurootreg_" + "2" * 32,
                "root_id": "himr-hot-v1",
            },
            "execution_image": {
                "receipt_path": "/private/image-receipt.json",
                "receipt_sha256": "3" * 64,
                "identity_sha256": "4" * 64,
            },
            "production_profile": {
                "path": "/private/profile.json",
                "sha256": "5" * 64,
                "identity_sha256": "6" * 64,
            },
            "trusted_install": {
                "owner_uid": 0,
                "launcher": {"path": "/usr/local/libexec/himr-gpu/trusted-launcher-v2", "sha256": "7" * 64},
                "launcher_profile": {"path": "/etc/himr-gpu/launcher-profile-v2.json", "sha256": "8" * 64},
                "system_tools": [
                    {"name": "bubblewrap", "path": "/usr/bin/bwrap", "sha256": "9" * 64},
                    {"name": "fusermount", "path": "/usr/bin/fusermount3", "sha256": "a" * 64},
                    {"name": "nvidia_smi", "path": "/usr/bin/nvidia-smi", "sha256": "e" * 64},
                    {"name": "host_python", "path": "/usr/bin/python3.14", "sha256": "f" * 64},
                    {"name": "squashfuse", "path": "/usr/bin/squashfuse_ll", "sha256": "b" * 64},
                    {"name": "systemctl", "path": "/usr/bin/systemctl", "sha256": "c" * 64},
                    {"name": "systemd_run", "path": "/usr/bin/systemd-run", "sha256": "d" * 64},
                ],
            },
            "runtime": {
                "python_version": "3.12.14",
                "packages": {
                    "av": "18.1.0",
                    "ctranslate2": "4.8.1",
                    "faster-whisper": "1.2.1",
                    "huggingface-hub": "1.29.0",
                    "nvidia-cublas-cu12": "12.9.2.10",
                    "nvidia-ml-py": "13.610.43",
                    "numpy": "2.5.2",
                    "pyyaml": "6.0.3",
                    "tokenizers": "0.23.1",
                    "tqdm": "4.70.0",
                },
                "required_mapping_names": sorted(ADMISSION.REQUIRED_MAPPING_NAMES),
            },
            "gates": {name: None for name in ADMISSION.GATES.GATES},
            "policy": ADMISSION.POLICY,
        }

    def test_spec_has_no_persisted_device_and_binds_complete_profile(self) -> None:
        normalized = ADMISSION.normalize_spec(self.spec())
        encoded = ADMISSION.canonical_bytes(normalized)
        self.assertNotIn(b"expected_device", encoded)
        self.assertNotIn(b"st_dev", encoded)
        self.assertEqual(
            set(normalized["runtime"]["required_mapping_names"]),
            ADMISSION.REQUIRED_MAPPING_NAMES,
        )

    def test_candidate_accepts_current_user_local_install_but_admitted_does_not(self) -> None:
        value = self.spec("candidate")
        value["trusted_install"]["owner_uid"] = os.geteuid()
        value["trusted_install"]["launcher"]["path"] = "/private/hot/launcher"
        value["trusted_install"]["launcher_profile"]["path"] = (
            "/private/hot/launcher-profile.json"
        )
        normalized = ADMISSION.normalize_spec(value)
        self.assertEqual(normalized["trusted_install"]["owner_uid"], os.geteuid())
        value["requested_state"] = "admitted"
        with self.assertRaisesRegex(
            ADMISSION.RuntimeAdmissionV2Error, "must be root-owned"
        ):
            ADMISSION.normalize_spec(value)

    def test_exact_system_tool_and_package_sets_are_required(self) -> None:
        value = self.spec()
        value["trusted_install"]["system_tools"].pop()
        with self.assertRaisesRegex(ADMISSION.RuntimeAdmissionV2Error, "tool set"):
            ADMISSION.normalize_spec(value)
        value = self.spec()
        del value["runtime"]["packages"]["av"]
        with self.assertRaisesRegex(ADMISSION.RuntimeAdmissionV2Error, "packages"):
            ADMISSION.normalize_spec(value)
        value = self.spec()
        value["runtime"]["packages"]["ctranslate2"] = "4.8.2"
        with self.assertRaisesRegex(ADMISSION.RuntimeAdmissionV2Error, "versions"):
            ADMISSION.normalize_spec(value)
        value = self.spec()
        value["trusted_install"]["system_tools"][0]["path"] = "/opt/fake-bwrap"
        with self.assertRaisesRegex(ADMISSION.RuntimeAdmissionV2Error, "paths"):
            ADMISSION.normalize_spec(value)

    def test_admitted_semantic_requires_every_gate(self) -> None:
        normalized = ADMISSION.normalize_spec(self.spec("admitted"))
        profile = ADMISSION.PROFILE.default_profile()
        image = {
            "identity_sha256": normalized["execution_image"]["identity_sha256"],
            "logical_mappings": [
                {"name": name, "image_relative_path": name, "sandbox_path": f"/x/{name}", "role": "fixture"}
                for name in sorted(ADMISSION.REQUIRED_MAPPING_NAMES)
            ],
            "image": {"path": "/image", "sha256": "0" * 64, "byte_count": 1, "mode": "0444", "filesystem": {"type": "btrfs", "uuid": "1" * 36}},
        }
        with (
            mock.patch.object(
                ADMISSION,
                "_load_root",
                return_value=(
                    {
                        "root_id": "himr-hot-v1",
                        "tier": "hot_main_drive",
                        "path": "/hot",
                        "filesystem": {"type": "btrfs", "uuid": "1" * 36},
                    },
                    {},
                ),
            ),
            mock.patch.object(ADMISSION, "_load_profile", return_value=(profile, {})),
            mock.patch.object(ADMISSION, "_load_image", return_value=(image, {})),
            mock.patch.object(ADMISSION, "_load_install", return_value={}),
            self.assertRaisesRegex(ADMISSION.RuntimeAdmissionV2Error, "missing gate"),
        ):
            ADMISSION.build_semantic(normalized)

    def test_candidate_semantic_is_not_admitted_authority(self) -> None:
        normalized = ADMISSION.normalize_spec(self.spec("candidate"))
        profile = ADMISSION.PROFILE.default_profile()
        image = {
            "identity_sha256": "4" * 64,
            "logical_mappings": [],
            "image": {},
        }
        with (
            mock.patch.object(ADMISSION, "_load_root", return_value=({"root_id": "himr-hot-v1", "tier": "hot_main_drive", "path": "/hot", "filesystem": {}}, {})),
            mock.patch.object(ADMISSION, "_load_profile", return_value=(profile, {})),
            mock.patch.object(ADMISSION, "_load_image", return_value=(image, {})),
            mock.patch.object(ADMISSION, "_load_gates", return_value={name: None for name in ADMISSION.GATES.GATES}),
            mock.patch.object(ADMISSION, "_load_install", return_value={}),
        ):
            receipt = ADMISSION.make_receipt(normalized)
            deeply_audited = ADMISSION.make_receipt(normalized, deep_image=True)
        self.assertEqual(receipt["status"], "candidate")
        self.assertEqual(
            deeply_audited,
            receipt,
            "performing a deep image audit must not create a different runtime identity",
        )
        with self.assertRaisesRegex(ADMISSION.RuntimeAdmissionV2Error, "not admitted"):
            # Header check must fail before an expensive semantic replay.
            ADMISSION.validate_receipt(receipt, require_admitted=True)

    def test_legacy_numeric_observation_cannot_be_added(self) -> None:
        value = self.spec()
        value["root_registration"]["expected_device"] = 38
        with self.assertRaisesRegex(ADMISSION.RuntimeAdmissionV2Error, "unexpected"):
            ADMISSION.normalize_spec(value)

    def test_mapping_layout_binds_roles_and_sandbox_paths(self) -> None:
        rows = [
            {
                "name": name,
                "sandbox_path": sandbox_path,
                "role": role,
            }
            for name, (sandbox_path, role) in sorted(
                ADMISSION.REQUIRED_MAPPING_LAYOUT.items()
            )
        ]
        ADMISSION._require_mapping_layout(rows)
        changed = copy.deepcopy(rows)
        changed[0]["role"] = "executable"
        with self.assertRaisesRegex(
            ADMISSION.RuntimeAdmissionV2Error, "roles or sandbox"
        ):
            ADMISSION._require_mapping_layout(changed)

    def test_candidate_identity_binds_embedded_host_abi_via_launcher_profile(self) -> None:
        arguments = {
            "root_registration": {"identity_sha256": "1" * 64},
            "root": {"root_id": "himr-hot-main-v1"},
            "execution_image": {"identity_sha256": "2" * 64},
            "production_profile": {"identity_sha256": "3" * 64},
            "production_profile_file": {"sha256": "4" * 64},
            "trusted_install": {
                "launcher": {"sha256": "5" * 64},
                # host_abi is embedded in this canonical profile. Its complete
                # file digest is therefore the admission-evidence authority.
                "launcher_profile": {"sha256": "6" * 64},
                "system_tools": [],
            },
            "runtime": {"python_version": "3.12.14"},
        }
        original = ADMISSION._runtime_candidate_closure(**arguments)
        changed_arguments = copy.deepcopy(arguments)
        changed_arguments["trusted_install"]["launcher_profile"]["sha256"] = (
            "7" * 64
        )
        changed = ADMISSION._runtime_candidate_closure(**changed_arguments)
        self.assertNotEqual(
            original["identity_sha256"], changed["identity_sha256"]
        )

    def gate_fixture(
        self, directory: Path, *, forged_item_count: int | None = None
    ) -> tuple[dict[str, object], str, str, str]:
        hot = directory / "hot"
        hot.mkdir()
        profile_identity = "1" * 64
        image_identity = "2" * 64
        candidate_identity = "4" * 64
        metrics = ADMISSION.GATES.empty_metrics()
        metrics["item_count"] = 32
        raw = ADMISSION.GATES.make_raw_evidence(
            {
                "kind": ADMISSION.GATES.RAW_EVIDENCE_KIND,
                "schema_version": ADMISSION.GATES.RAW_EVIDENCE_SCHEMA_VERSION,
                "implementation_version": ADMISSION.GATES.RAW_EVIDENCE_IMPLEMENTATION_VERSION,
                "gate": "batch_32",
                "production_profile_identity_sha256": profile_identity,
                "execution_image_identity_sha256": image_identity,
                "runtime_candidate_identity_sha256": candidate_identity,
                "reducer": ADMISSION.GATES.REDUCER_REFERENCE,
                "observations": [ADMISSION.GATES.observation_from_metrics(metrics)],
                "policy": ADMISSION.GATES.RAW_POLICY,
            }
        )
        raw_path = hot / "raw.json"
        raw_path.write_bytes(ADMISSION.GATES.canonical_bytes(raw))
        raw_path.chmod(0o400)
        reduced = ADMISSION.GATES.reduce_raw_evidence(raw)
        if forged_item_count is not None:
            reduced["item_count"] = forged_item_count
        gate = ADMISSION.GATES.make_gate(
            {
                "kind": ADMISSION.GATES.KIND,
                "schema_version": ADMISSION.GATES.SCHEMA_VERSION,
                "implementation_version": ADMISSION.GATES.IMPLEMENTATION_VERSION,
                "gate": "batch_32",
                "status": "passed",
                "production_profile_identity_sha256": profile_identity,
                "execution_image_identity_sha256": image_identity,
                "runtime_candidate_identity_sha256": candidate_identity,
                "raw_evidence": ADMISSION.GATES.make_raw_reference(str(raw_path), raw),
                "metrics": reduced,
                "policy": ADMISSION.GATES.POLICY,
            }
        )
        gate_path = hot / "gate.json"
        gate_body = ADMISSION.GATES.canonical_bytes(gate)
        gate_path.write_bytes(gate_body)
        gate_path.chmod(0o400)
        spec = {"gates": {name: None for name in ADMISSION.GATES.GATES}}
        spec["gates"]["batch_32"] = {
            "path": str(gate_path),
            "sha256": ADMISSION.sha256_bytes(gate_body),
        }
        return spec, profile_identity, image_identity, candidate_identity

    def test_gate_loading_deep_replays_typed_raw_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec, profile, image, candidate = self.gate_fixture(root)
            loaded = ADMISSION._load_gates(
                spec,
                require_all=False,
                profile_identity=profile,
                image_identity=image,
                runtime_candidate_identity=candidate,
                hot_root_path=str(root / "hot"),
            )
            self.assertEqual(loaded["batch_32"]["status"], "passed")

    def test_hash_bound_but_forged_gate_aggregate_fails_deep_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec, profile, image, candidate = self.gate_fixture(
                root, forged_item_count=33
            )
            with self.assertRaisesRegex(
                ADMISSION.RuntimeAdmissionV2Error, "failed deep replay.*do not replay"
            ):
                ADMISSION._load_gates(
                    spec,
                    require_all=False,
                    profile_identity=profile,
                    image_identity=image,
                    runtime_candidate_identity=candidate,
                    hot_root_path=str(root / "hot"),
                )


if __name__ == "__main__":
    unittest.main()
