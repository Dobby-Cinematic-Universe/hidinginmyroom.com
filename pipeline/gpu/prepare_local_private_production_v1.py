#!/usr/bin/env python3
"""Prepare same-UID local-private GPU controls without media or inference."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any


try:
    from . import admit_runtime_v2 as ADMISSION
    from . import build_execution_image as IMAGE
    from . import host_abi_manifest_v1 as HOST_ABI
    from . import trusted_launcher_v2 as LAUNCHER
except ImportError:  # pragma: no cover - isolated direct execution.
    gpu_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(gpu_dir))
    import admit_runtime_v2 as ADMISSION  # type: ignore[no-redef]
    import build_execution_image as IMAGE  # type: ignore[no-redef]
    import host_abi_manifest_v1 as HOST_ABI  # type: ignore[no-redef]
    import trusted_launcher_v2 as LAUNCHER  # type: ignore[no-redef]


KIND = "himr_gpu_local_private_production_preparation"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"


class PreparationError(RuntimeError):
    """An explicit local control binding failed closed."""


def _output(path_value: str, hot_root: Path, label: str) -> Path:
    path = LAUNCHER.normalized_absolute_path(path_value, label)
    if hot_root not in path.parents:
        raise PreparationError(f"{label} must be a strict descendant of the hot root")
    parent = path.parent
    info = parent.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise PreparationError(
            f"{label} parent must be current-user-owned mode 0700"
        )
    return path


def _write_or_reuse(path: Path, body: bytes, mode: int, label: str) -> str:
    if not path.exists() and not path.is_symlink():
        LAUNCHER._write_new(path, body, mode)
        return "created"
    with LAUNCHER.retain_file(
        path,
        label,
        expected_sha256=LAUNCHER.sha256_bytes(body),
        maximum=max(len(body), 1),
        allowed_owner_modes={(os.geteuid(), mode)},
        keep_body=False,
    ) as retained:
        if retained.info.st_size != len(body):
            raise PreparationError(f"existing {label} byte count differs")
    return "reused"


def _local_control(path: str, digest: str, label: str) -> tuple[LAUNCHER.RetainedFile, Any]:
    return LAUNCHER._load_control(
        path,
        digest,
        label,
        production_root_owned=False,
        local_user_owned=True,
    )


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    retained: list[LAUNCHER.RetainedFile] = []
    root_handle: LAUNCHER.RetainedRoot | None = None
    try:
        root_file, root_value = _local_control(
            args.root_registration,
            args.expected_root_registration_sha256,
            "root registration",
        )
        retained.append(root_file)
        registration = LAUNCHER.validate_root_registration(root_value)
        root_handle = LAUNCHER.retain_root(registration)
        hot_root = Path(registration["path"])

        production_file, production_value = _local_control(
            args.production_profile,
            args.expected_production_profile_sha256,
            "production profile",
        )
        retained.append(production_file)
        production = LAUNCHER.validate_production_profile(production_value)

        image_receipt_file, _ = _local_control(
            args.execution_image_receipt,
            args.expected_execution_image_receipt_sha256,
            "execution image receipt",
        )
        retained.append(image_receipt_file)
        try:
            image_receipt = IMAGE.load_receipt(
                image_receipt_file.path,
                image_receipt_file.sha256,
                verify_image=True,
                expected_image_uid=os.geteuid(),
            )
        except IMAGE.ExecutionImageError as error:
            raise PreparationError(f"execution image failed deep replay: {error}") from error
        if image_receipt["image"]["mode"] != "0400":
            raise PreparationError(
                "local-private preparation requires an image built with --image-mode 0400"
            )

        host_file, host_value = _local_control(
            args.host_abi_manifest,
            args.expected_host_abi_manifest_sha256,
            "host ABI manifest",
        )
        retained.append(host_file)
        try:
            host_abi = HOST_ABI.validate_manifest(host_value)
        except HOST_ABI.HostABIManifestError as error:
            raise PreparationError(f"host ABI manifest failed replay: {error}") from error
        if (
            host_abi["consumer_scan"]["execution_image_identity_sha256"]
            != image_receipt["identity_sha256"]
        ):
            raise PreparationError("host ABI manifest binds a different execution image")

        for label, path in (
            ("root registration", root_file.path),
            ("production profile", production_file.path),
            ("execution image receipt", image_receipt_file.path),
            ("execution image", Path(image_receipt["image"]["path"])),
            ("host ABI manifest", host_file.path),
        ):
            if hot_root not in path.parents:
                raise PreparationError(f"{label} must be a strict descendant of the hot root")

        launcher_output = _output(args.launcher_output, hot_root, "launcher output")
        profile_output = _output(
            args.launcher_profile_output, hot_root, "launcher profile output"
        )
        runtime_output = _output(
            args.runtime_admission_output, hot_root, "runtime admission output"
        )
        outputs = {launcher_output, profile_output, runtime_output}
        if len(outputs) != 3:
            raise PreparationError("local control output paths must be distinct")

        launcher_source = Path(LAUNCHER.__file__).resolve()
        launcher_body = launcher_source.read_bytes()
        launcher_sha256 = LAUNCHER.sha256_bytes(launcher_body)
        launcher_disposition = _write_or_reuse(
            launcher_output, launcher_body, 0o500, "local trusted launcher"
        )

        execution_image = {
            "path": image_receipt["image"]["path"],
            "sha256": image_receipt["image"]["sha256"],
            "byte_count": image_receipt["image"]["byte_count"],
            "identity_sha256": image_receipt["identity_sha256"],
            "receipt_path": str(image_receipt_file.path),
            "receipt_sha256": image_receipt_file.sha256,
        }
        profile_reference = {
            "path": str(production_file.path),
            "sha256": production_file.sha256,
            "identity_sha256": production["identity_sha256"],
        }
        root_reference = {
            "path": str(root_file.path),
            "sha256": root_file.sha256,
            "identity_sha256": registration["identity_sha256"],
            "registration_id": registration["registration_id"],
            "root_id": registration["root_id"],
        }
        install_spec = {
            "kind": LAUNCHER.INSTALL_SPEC_KIND,
            "schema_version": LAUNCHER.SCHEMA_VERSION,
            "launcher_install_path": str(launcher_output),
            "launcher_profile_install_path": str(profile_output),
            "runtime_admission_install_path": str(runtime_output),
            "execution_image": execution_image,
            "production_profile": profile_reference,
            "root_registration": root_reference,
            "system_tool_paths": dict(LAUNCHER.SYSTEM_TOOL_PATHS),
            "host_abi": host_abi,
            "sandbox": {
                "image_mapping_prefix": str(LAUNCHER.IMAGE_MAPPING_PREFIX),
                "control_root": str(LAUNCHER.CONTROL_ROOT),
                "input_root": str(LAUNCHER.INPUT_ROOT),
                "output_root": str(LAUNCHER.OUTPUT_ROOT),
                "state_root": str(LAUNCHER.STATE_ROOT),
                "system_library_directories": [],
                "system_readonly_files": [],
                "gpu_control_devices": [
                    "/dev/nvidiactl",
                    "/dev/nvidia-uvm",
                    "/dev/nvidia-uvm-tools",
                ],
            },
            "policy": dict(LAUNCHER.POLICY),
        }
        normalized_install = LAUNCHER._normalize_install_spec(install_spec)
        launcher_profile = LAUNCHER._make_profile_from_spec(
            normalized_install, launcher_sha256
        )
        launcher_profile_body = LAUNCHER.canonical_bytes(launcher_profile)
        profile_disposition = _write_or_reuse(
            profile_output,
            launcher_profile_body,
            0o400,
            "local launcher profile",
        )

        runtime_spec = ADMISSION.normalize_spec(
            {
                "kind": ADMISSION.SPEC_KIND,
                "schema_version": ADMISSION.SCHEMA_VERSION,
                "requested_state": "candidate",
                "root_registration": {
                    "path": str(root_file.path),
                    "sha256": root_file.sha256,
                    "registration_id": registration["registration_id"],
                    "root_id": registration["root_id"],
                },
                "execution_image": {
                    "receipt_path": str(image_receipt_file.path),
                    "receipt_sha256": image_receipt_file.sha256,
                    "identity_sha256": image_receipt["identity_sha256"],
                },
                "production_profile": profile_reference,
                "trusted_install": {
                    "owner_uid": os.geteuid(),
                    "launcher": {
                        "path": str(launcher_output),
                        "sha256": launcher_sha256,
                    },
                    "launcher_profile": {
                        "path": str(profile_output),
                        "sha256": LAUNCHER.sha256_bytes(launcher_profile_body),
                    },
                    "system_tools": [
                        {
                            "name": name,
                            "path": row["path"],
                            "sha256": row["sha256"],
                        }
                        for name, row in sorted(
                            launcher_profile["system_tools"].items()
                        )
                    ],
                },
                "runtime": {
                    "python_version": "3.12.14",
                    "packages": dict(ADMISSION.REQUIRED_PACKAGE_VERSIONS),
                    "required_mapping_names": sorted(
                        ADMISSION.REQUIRED_MAPPING_NAMES
                    ),
                },
                "gates": {name: None for name in ADMISSION.GATES.GATES},
                "policy": dict(ADMISSION.POLICY),
            }
        )
        runtime_receipt = ADMISSION.make_receipt(runtime_spec, deep_image=True)
        runtime_body = ADMISSION.canonical_bytes(runtime_receipt)
        runtime_disposition = _write_or_reuse(
            runtime_output, runtime_body, 0o400, "candidate runtime admission"
        )
        replayed = ADMISSION.load_receipt(
            runtime_output,
            ADMISSION.sha256_bytes(runtime_body),
            require_admitted=False,
            deep_image=False,
        )
        if replayed != runtime_receipt:
            raise PreparationError("prepared runtime differs from exact replay")
        LAUNCHER.validate_runtime_receipt(
            runtime_receipt,
            mode=LAUNCHER.MODE_LOCAL_PRIVATE,
            launcher_profile=launcher_profile,
            launcher_profile_path=str(profile_output),
            profile_sha256=LAUNCHER.sha256_bytes(launcher_profile_body),
        )
        return {
            "kind": KIND,
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "prepared",
            "mode": LAUNCHER.MODE_LOCAL_PRIVATE,
            "execution_class": LAUNCHER.EXECUTION_CLASS_LOCAL_PRIVATE,
            "trust_boundary": {
                "kind": "current_user_same_uid",
                "same_uid_mutation_resistance": False,
            },
            "launcher": {
                "path": str(launcher_output),
                "sha256": launcher_sha256,
                "disposition": launcher_disposition,
            },
            "launcher_profile": {
                "path": str(profile_output),
                "sha256": LAUNCHER.sha256_bytes(launcher_profile_body),
                "identity_sha256": launcher_profile["identity_sha256"],
                "disposition": profile_disposition,
            },
            "runtime_admission": {
                "path": str(runtime_output),
                "sha256": ADMISSION.sha256_bytes(runtime_body),
                "identity_sha256": runtime_receipt["identity_sha256"],
                "status": runtime_receipt["status"],
                "disposition": runtime_disposition,
            },
            "execution_image": execution_image,
            "input_media_read": False,
            "inference_performed": False,
            "publication_performed": False,
            "import_performed": False,
            "catalogue_mutated": False,
            "deletion_performed": False,
        }
    finally:
        if root_handle is not None:
            root_handle.close()
        for value in retained:
            value.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contract")
    create = commands.add_parser("prepare")
    create.add_argument("--execution-image-receipt", required=True)
    create.add_argument("--expected-execution-image-receipt-sha256", required=True)
    create.add_argument("--production-profile", required=True)
    create.add_argument("--expected-production-profile-sha256", required=True)
    create.add_argument("--root-registration", required=True)
    create.add_argument("--expected-root-registration-sha256", required=True)
    create.add_argument("--host-abi-manifest", required=True)
    create.add_argument("--expected-host-abi-manifest-sha256", required=True)
    create.add_argument("--launcher-output", required=True)
    create.add_argument("--launcher-profile-output", required=True)
    create.add_argument("--runtime-admission-output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contract":
            result = {
                "kind": f"{KIND}_contract",
                "schema_version": SCHEMA_VERSION,
                "implementation_version": IMPLEMENTATION_VERSION,
                "outputs": [
                    "current_user_mode_0500_launcher",
                    "current_user_mode_0400_launcher_profile",
                    "current_user_mode_0400_candidate_runtime",
                ],
                "media_access": False,
                "inference_authority": "none",
                "publication_authority": "none",
                "deletion_authority": "none",
            }
        else:
            result = prepare(args)
        sys.stdout.buffer.write(LAUNCHER.canonical_bytes(result))
        return 0
    except (
        PreparationError,
        LAUNCHER.TrustedLauncherError,
        ADMISSION.RuntimeAdmissionV2Error,
        FileExistsError,
        OSError,
        ValueError,
    ) as error:
        failure = {
            "kind": f"{KIND}_failure",
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
            "input_media_read": False,
            "inference_performed": False,
            "publication_performed": False,
            "deletion_performed": False,
        }
        sys.stderr.buffer.write(LAUNCHER.canonical_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
