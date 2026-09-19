#!/usr/bin/env python3
"""Create or replay one restart-portable hot-root registration.

This command records a Btrfs filesystem UUID and content identity.  Linux device
and inode observations are diagnostics only and are never replay authority.  It has
no media, inference, archive, catalogue, or publication authority.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import secrets
import stat
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


HERE = Path(__file__).resolve().parent
COLD_ROOT = Path("/mnt/archive/HIMR")


class RegistrationCommandError(RuntimeError):
    """Registration creation or replay failed closed."""


def _load_module() -> ModuleType:
    path = HERE / "portable_root.py"
    spec = importlib.util.spec_from_file_location("himr_gpu_root_registration_api", path)
    if spec is None or spec.loader is None:
        raise RegistrationCommandError("portable-root API cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PORTABLE = _load_module()


def _forbid_cold(path: Path) -> None:
    try:
        path.relative_to(COLD_ROOT)
    except ValueError:
        return
    raise RegistrationCommandError("cold storage cannot be a GPU hot-root registration")


def _write_exclusive(path: Path, value: Any) -> None:
    if not path.is_absolute() or path.exists() or path.is_symlink():
        raise RegistrationCommandError("output must be a new absolute path")
    parent = path.parent
    info = parent.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise RegistrationCommandError(
            "output parent must be current-user-owned mode 0700"
        )
    body = PORTABLE.canonical_bytes(value)
    temporary = parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o400,
    )
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def create_registration(
    *, root: Path, root_id: str, filesystem_uuid: str, output: Path
) -> dict[str, Any]:
    root = PORTABLE.normalized_absolute_path(root, "hot root")
    _forbid_cold(root)
    observed = root.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise RegistrationCommandError(
            "hot root must be a current-user-owned non-group/other-writable directory"
        )
    registration = PORTABLE.make_registration(
        root_id=root_id,
        tier="hot_main_drive",
        path=root,
        filesystem_uuid=filesystem_uuid,
        owner_uid=os.geteuid(),
        historical_observation=PORTABLE.historical_stat_observation(observed),
    )
    with PORTABLE.RetainedRoot.open(
        registration,
        expected_root_id=root_id,
        expected_tier="hot_main_drive",
        expected_path=root,
    ) as retained:
        retained.verify()
    _write_exclusive(output, registration)
    replayed = PORTABLE.load_registration(
        output,
        PORTABLE.sha256_bytes(PORTABLE.canonical_bytes(registration)),
        expected_root_id=root_id,
        expected_tier="hot_main_drive",
        expected_path=root,
        expected_document_uid=os.geteuid(),
        expected_document_mode=0o400,
    )
    if replayed != registration:
        raise RegistrationCommandError("published registration failed exact replay")
    return registration


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts")
    create = commands.add_parser("create")
    create.add_argument("--root", required=True)
    create.add_argument("--root-id", required=True)
    create.add_argument("--filesystem-uuid", required=True)
    create.add_argument("--output", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--registration", required=True)
    validate.add_argument("--expected-sha256", required=True)
    validate.add_argument("--root-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contracts":
            response = {
                "kind": "himr_gpu_hot_root_registration_contract",
                "schema_version": PORTABLE.REGISTRATION_SCHEMA_VERSION,
                "tier": "hot_main_drive",
                "document_modes": {
                    "candidate": "current-user:0400",
                    "production": "root:0444",
                },
                "cold_storage_allowed": False,
            }
        elif args.command == "create":
            output = PORTABLE.normalized_absolute_path(args.output, "output")
            registration = create_registration(
                root=Path(args.root),
                root_id=args.root_id,
                filesystem_uuid=args.filesystem_uuid,
                output=output,
            )
            body = PORTABLE.canonical_bytes(registration)
            response = {
                "status": "created",
                "path": str(output),
                "sha256": PORTABLE.sha256_bytes(body),
                "registration_id": registration["registration_id"],
                "identity_sha256": registration["identity_sha256"],
            }
        else:
            registration_path = PORTABLE.normalized_absolute_path(
                args.registration, "registration"
            )
            document_info = registration_path.lstat()
            document_pair = (
                document_info.st_uid,
                stat.S_IMODE(document_info.st_mode),
            )
            if document_pair not in {(os.geteuid(), 0o400), (0, 0o444)}:
                raise RegistrationCommandError(
                    "registration must be current-user mode 0400 or root-owned mode 0444"
                )
            registration = PORTABLE.load_registration(
                registration_path,
                args.expected_sha256,
                expected_root_id=args.root_id,
                expected_tier="hot_main_drive",
                expected_document_uid=document_pair[0],
                expected_document_mode=document_pair[1],
            )
            with PORTABLE.RetainedRoot.open(
                registration,
                expected_root_id=args.root_id,
                expected_tier="hot_main_drive",
            ) as retained:
                retained.verify()
            response = {
                "status": "validated",
                "registration_id": registration["registration_id"],
                "identity_sha256": registration["identity_sha256"],
            }
        sys.stdout.write(json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except (OSError, ValueError, RegistrationCommandError, PORTABLE.PortableRootError) as error:
        sys.stderr.write(
            json.dumps(
                {
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
