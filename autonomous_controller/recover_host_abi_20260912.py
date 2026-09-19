#!/usr/bin/env python3
"""Re-admit the unchanged August GPU image against the reviewed September boot.

The normal creator rescans historical build source paths. Those mutable sources
have since changed, while the sealed image has not. Reuse only the authenticated
ELF inventory of that exact image; never reinterpret current sources as old ones.
This dated, bounded utility does not install controls or start pipeline work.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
DEPLOYMENT = ROOT / "research/corpus/gpu-runtime/portable-v2-local-private-20260829T211303Z"
OLD_MANIFEST_SHA256 = "88a5ed01e773c8e058d37a2e3941c31446d907d24daba51671f5c91a4b2adef7"
OLD_PROFILE_SHA256 = "d669112164cbb7a0e9f34ce950ea9e9231879058140dd11e088687d6601d6d0d"
IMAGE_RECEIPT_SHA256 = "9da089609a6bfcc7bbc0028ace9592b542a08c600de4a319159f7855d1a40a81"
IMAGE_SHA256 = "890e08825a1ddce5e62ebacabe491f76dae853da98c926eae7ffebdd44108de0"
IMAGE_IDENTITY = "cd12833be1183e5813fb0f32cd74c034e2e07abc9e24e8f3d88397755a96bbf0"
DRIVER_VERSION = "610.57.04"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    source = ROOT / "pipeline/gpu/host_abi_manifest_v1.py"
    spec = importlib.util.spec_from_file_location("himr_host_abi_20260912_recovery", source)
    require(spec is not None and spec.loader is not None, "host ABI helper unavailable")
    api = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = api
    spec.loader.exec_module(api)

    output = DEPLOYMENT / "host-abi-20260912-recovery.json"
    audit_path = DEPLOYMENT / "host-abi-20260912-recovery-audit.json"
    require(not output.exists() and not output.is_symlink(), "new manifest already exists")
    require(not audit_path.exists() and not audit_path.is_symlink(), "new audit already exists")
    old_path = DEPLOYMENT / "host-abi-v1.json"
    old = api.load_manifest(old_path, OLD_MANIFEST_SHA256)
    profile_path = DEPLOYMENT / "controls-20260829T211733Z/launcher-profile-v2.json"
    profile_body, _ = api._stable_regular(profile_path, "historical launcher profile")
    require(hashlib.sha256(profile_body).hexdigest() == OLD_PROFILE_SHA256, "historical profile pin differs")
    profile = api._strict_json(profile_body, "historical launcher profile")
    require(profile["host_abi"] == old, "historical profile binds another host ABI")

    receipt_path = DEPLOYMENT / "execution-v2-receipt.json"
    image = api._load_execution_image_receipt(receipt_path, IMAGE_RECEIPT_SHA256)
    require(image["identity_sha256"] == IMAGE_IDENTITY, "execution image identity differs")
    require(image["image"]["sha256"] == IMAGE_SHA256, "execution image SHA differs")
    scan = old["consumer_scan"]
    require(scan["execution_image_identity_sha256"] == IMAGE_IDENTITY, "consumer inventory image differs")
    require(scan["source_tree_identity_sha256"] == image["source_tree"]["identity_sha256"], "consumer inventory source-tree identity differs")
    require(scan["source_tree_regular_file_count"] == image["source_tree"]["regular_file_count"], "consumer inventory source count differs")
    require(scan["runtime_loaded_sonames"] == sorted(api.production_runtime_loaded_sonames(DRIVER_VERSION)), "runtime-loaded library set differs")
    new = api.build_manifest(old["root_libraries"], consumer_scan=scan,
                             library_roots=old["library_roots"],
                             nvidia_driver_version=DRIVER_VERSION)
    allowed = {"platform", "identity_sha256", "manifest_id"}
    require({key for key in old if old[key] != new[key]} <= allowed,
            "host libraries or image inventory changed; separate review required")
    expected_changes = {"release", "version"}
    changed = {key: {"old": old["platform"][key], "new": new["platform"][key]}
               for key in old["platform"] if old["platform"][key] != new["platform"][key]}
    require(set(changed) == expected_changes, "platform drift exceeds reviewed kernel-only change")
    require(new["platform"]["release"] == "7.1.13-200.fc44.x86_64", "booted kernel is not the reviewed successor")
    api.replay_manifest(new, observed_driver_version=DRIVER_VERSION)
    api._write_exclusive(output, new)
    digest = hashlib.sha256(api.canonical_bytes(new)).hexdigest()
    api.load_manifest(output, digest, replay=True, observed_driver_version=DRIVER_VERSION)
    audit = {
        "schema_version": 1,
        "kind": "himr_gpu_host_abi_recovery_audit",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "reason": "reuse_authenticated_consumer_inventory_of_byte_identical_image_after_build_source_changes",
        "original_manifest": {"path": str(old_path), "sha256": OLD_MANIFEST_SHA256},
        "original_launcher_profile": {"path": str(profile_path), "sha256": OLD_PROFILE_SHA256},
        "execution_image_receipt": {"path": str(receipt_path), "sha256": IMAGE_RECEIPT_SHA256},
        "execution_image_sha256": IMAGE_SHA256,
        "execution_image_identity_sha256": IMAGE_IDENTITY,
        "image_fully_rehashed": True,
        "consumer_scan_unchanged": True,
        "host_libraries_rehashed_and_unchanged": len(new["libraries"]),
        "changed_platform_fields": changed,
        "manifest": {"path": str(output), "sha256": digest, "identity_sha256": new["identity_sha256"]},
        "new_manifest_replay_passed": True,
        "old_artifacts_modified": False,
        "pipeline_started": False,
    }
    api._write_exclusive(audit_path, audit)
    print(json.dumps(audit, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
