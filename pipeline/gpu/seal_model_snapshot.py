#!/usr/bin/env python3
"""Seal one explicitly acquired public model snapshot and emit a private manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import stat
from pathlib import Path
from typing import Any


REVISION_RE = re.compile(r"^[0-9a-f]{40}$")


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_files(root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        for name in sorted(directory_names):
            child = directory_path / name
            if stat.S_ISLNK(child.lstat().st_mode):
                raise ValueError(f"model snapshot contains a symlinked directory: {child}")
        for name in sorted(file_names):
            path = directory_path / name
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError(f"model snapshot contains a non-regular file: {path}")
            if metadata.st_nlink != 1:
                raise ValueError(f"model snapshot file must have one link: {path}")
            if metadata.st_uid != os.getuid():
                raise ValueError(f"model snapshot file must be owned by the current user: {path}")
            records.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "byte_count": metadata.st_size,
                    "sha256": sha256_file(path),
                }
            )
    if not records:
        raise ValueError("model snapshot is empty")
    return sorted(records, key=lambda item: item["path"])


def seal_tree(root: Path) -> None:
    directories: list[Path] = []
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        directories.append(directory_path)
        for name in directory_names:
            child = directory_path / name
            if stat.S_ISLNK(child.lstat().st_mode):
                raise ValueError(f"refusing symlinked directory during sealing: {child}")
        for name in file_names:
            path = directory_path / name
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ValueError(f"refusing unsafe model file during sealing: {path}")
            os.chmod(path, 0o400, follow_symlinks=False)
    for path in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        os.chmod(path, 0o500, follow_symlinks=False)


def write_exclusive(path: Path, payload: bytes) -> None:
    if not path.is_absolute():
        raise ValueError("manifest output must be absolute")
    parent = path.parent
    if not parent.is_dir() or parent.resolve() != parent:
        raise ValueError("manifest parent must be an existing, non-symlinked directory")
    parent_descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o400,
            dir_fd=parent_descriptor,
        )
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.fchmod(descriptor, 0o400)
        os.link(
            temporary_name,
            path.name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        os.unlink(temporary_name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except FileNotFoundError:
            pass
        os.close(parent_descriptor)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-root", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--license", required=True, dest="license_label")
    parser.add_argument("--sealed-at", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    root = args.model_root
    if not root.is_absolute() or not root.is_dir() or root.resolve() != root:
        raise ValueError("model root must be an existing absolute non-symlinked directory")
    if not REVISION_RE.fullmatch(args.revision):
        raise ValueError("revision must be a full lowercase 40-hex commit")
    if args.output == root or root in args.output.parents:
        raise ValueError("manifest output must be outside the model snapshot")

    before = snapshot_files(root)
    seal_tree(root)
    after = snapshot_files(root)
    if before != after:
        raise RuntimeError("model bytes changed while sealing")

    identity_basis = {
        "kind": "himr_private_model_snapshot",
        "schema_version": 1,
        "repository": args.repository,
        "revision": args.revision,
        "license": args.license_label,
        "files": after,
    }
    manifest = {
        **identity_basis,
        "identity_sha256": hashlib.sha256(canonical_bytes(identity_basis)).hexdigest(),
        "sealed_at": args.sealed_at,
        "snapshot_root": str(root),
        "acquisition": {
            "access": "public_unauthenticated",
            "credential_used": False,
            "method": "huggingface_snapshot_download_exact_revision",
        },
        "policy": {
            "catalogue_authority": "none",
            "identity_authority": "none",
            "publication_authority": "none",
            "training_performed": False,
        },
    }
    write_exclusive(args.output, canonical_bytes(manifest))
    print(json.dumps(manifest, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
