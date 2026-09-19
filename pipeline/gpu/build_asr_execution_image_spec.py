#!/usr/bin/env python3
"""Materialize the exact deterministic source projection for GPU ASR image v2.

The projection deliberately excludes the build wheelhouse, caches, symlinks,
bytecode, and unrelated repository files. It enumerates the standalone CPython
runtime, an exact allowlisted installed-package closure, the admitted model bundle,
and the closed worker/support source set. The execution-image builder performs the
authoritative byte audit and SquashFS build afterward.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import secrets
import stat
import sys
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, Iterable


HERE = Path(__file__).resolve().parent
REPOSITORY_ROOT = HERE.parents[1]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
CORPUS_PACKAGE_ROOT = REPOSITORY_ROOT / "corpus/src/himr_corpus"
COLD_ROOT = Path("/mnt/archive/HIMR")
DEFAULT_PYTHON_ROOT = (
    REPOSITORY_ROOT
    / "research/corpus/gpu-runtime/python/cpython-3.12.14-linux-x86_64-gnu"
)
DEFAULT_SITE_PACKAGES = (
    REPOSITORY_ROOT
    / "research/corpus/gpu-runtime/env/lib/python3.12/site-packages"
)
DEFAULT_MODEL_BUNDLE = (
    REPOSITORY_ROOT
    / "research/corpus/gpu-runtime/production-model-admissions/"
    "faster-whisper-small.en-d1d751a5f8271d482d14ca55d9e2deeebbae577f"
)
APP_SOURCE_NAMES = (
    "admit_hf_model.py",
    "admit_runtime_v2.py",
    "build_execution_image.py",
    "gpu_admission_evidence.py",
    "gpu_telemetry.py",
    "portable_root.py",
    "production_asr_batch_v2.py",
    "production_asr_v5.py",
    "production_profile_v2.py",
    "verified_dependency_loader.py",
)
PIPELINE_SUPPORT_SOURCE_NAMES = (
    "asr_whispercpp.py",
    "media_preprocess.py",
    "preprocess_asr_queue_v03.py",
    "preprocess_batch.py",
    "preprocess_gpu_asr_queue_v1.py",
    "whispercpp_engine_profiles.py",
)
GPU_PACKAGE_SUPPORT_SOURCE_NAMES = (
    "portable_root.py",
    "production_profile_v2.py",
)
CORPUS_SUPPORT_SOURCE_NAMES = (
    "__init__.py",
    "db.py",
    "ids.py",
    "importers.py",
    "private_acquisition.py",
    "result_importers.py",
    "reviewer_admin.py",
)
SITE_PACKAGES_ALLOW_TOP_LEVEL = frozenset(
    {
        "_yaml",
        "av",
        "av-18.1.0.dist-info",
        "av.libs",
        "ctranslate2",
        "ctranslate2-4.8.1.dist-info",
        "ctranslate2.libs",
        "faster_whisper",
        "faster_whisper-1.2.1.dist-info",
        "huggingface_hub",
        "huggingface_hub-1.29.0.dist-info",
        "numpy",
        "numpy-2.5.2.dist-info",
        "numpy.libs",
        "nvidia",
        "nvidia_cublas_cu12-12.9.2.10.dist-info",
        "nvidia_ml_py-13.610.43.dist-info",
        "pynvml.py",
        "pyyaml-6.0.3.dist-info",
        "tokenizers",
        "tokenizers-0.23.1.dist-info",
        "tqdm",
        "tqdm-4.70.0.dist-info",
        "yaml",
    }
)
SITE_PACKAGES_EXCLUDE_TOP_LEVEL = frozenset({
    "__pycache__",
    "_distutils_hack",
    "_virtualenv.pth",
    "_virtualenv.py",
    "anyio",
    "anyio-4.14.2.dist-info",
    "certifi",
    "certifi-2026.7.22.dist-info",
    "click",
    "click-8.5.0.dist-info",
    "distutils-precedence.pth",
    "example.py",
    "filelock",
    "filelock-3.32.4.dist-info",
    "flatbuffers",
    "flatbuffers-25.12.19.dist-info",
    "fsspec",
    "fsspec-2026.7.0.dist-info",
    "google",
    "h11",
    "h11-0.16.0.dist-info",
    "hf_xet",
    "hf_xet-1.6.0.dist-info",
    "httpcore",
    "httpcore-1.0.9.dist-info",
    "httpx",
    "httpx-0.28.1.dist-info",
    "idna",
    "idna-3.19.dist-info",
    "nvidia_cuda_nvrtc_cu12-12.9.86.dist-info",
    "nvidia_cudnn_cu12-9.24.0.43.dist-info",
    "onnxruntime",
    "onnxruntime-1.29.0.dist-info",
    "packaging",
    "packaging-26.3.dist-info",
    "protobuf-7.36.0.dist-info",
    "setuptools",
    "setuptools-84.0.0.dist-info",
    "typing_extensions-4.16.0.dist-info",
    "typing_extensions.py",
})
SITE_PACKAGES_EXCLUDE_RELATIVE_PREFIXES = frozenset({
    PurePosixPath("nvidia/cublas/include"),
    PurePosixPath("nvidia/cublas/lib/libnvblas.so.12"),
    PurePosixPath("nvidia/cuda_nvrtc"),
    PurePosixPath("nvidia/cudnn"),
})
NVIDIA_ALLOW_DIRECT_CHILDREN = frozenset({"cublas"})
NVIDIA_EXCLUDE_DIRECT_CHILDREN = frozenset({"cuda_nvrtc", "cudnn"})
NVIDIA_CUBLAS_ALLOW_LIBRARY_FILES = frozenset(
    {"libcublas.so.12", "libcublasLt.so.12"}
)
NVIDIA_CUBLAS_EXCLUDE_LIBRARY_FILES = frozenset({"libnvblas.so.12"})
PYTHON_STDLIB_EXCLUDE_RELATIVE_PREFIXES = frozenset(
    {PurePosixPath("lib-dynload")}
)


class ASRImageSpecError(RuntimeError):
    """The ASR execution source closure is incomplete or unsafe."""


def _load_builder() -> ModuleType:
    path = HERE / "build_execution_image.py"
    spec = importlib.util.spec_from_file_location("himr_asr_image_spec_builder_api", path)
    if spec is None or spec.loader is None:
        raise ASRImageSpecError("execution-image builder API cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


BUILDER = _load_builder()


MAPPING_LAYOUT = {
    "runtime_root": ("runtime", "/opt/himr-gpu/runtime", "runtime_root"),
    "python_executable": (
        "runtime/bin/python3.12",
        "/opt/himr-gpu/runtime/bin/python3.12",
        "executable",
    ),
    "model_bundle": ("model", "/opt/himr-gpu/model", "model_bundle"),
    "model_root": (
        "model/snapshot",
        "/opt/himr-gpu/model/snapshot",
        "model_root",
    ),
    "application_root": ("app", "/opt/himr-gpu/app", "application_root"),
    "application_support_root": (
        "corpus/src",
        "/opt/himr-gpu/corpus/src",
        "application_support_root",
    ),
    "adapter_source": (
        "app/production_asr_v5.py",
        "/opt/himr-gpu/app/production_asr_v5.py",
        "python_source",
    ),
    "worker_source": (
        "app/production_asr_batch_v2.py",
        "/opt/himr-gpu/app/production_asr_batch_v2.py",
        "python_source",
    ),
    "verified_loader": (
        "app/verified_dependency_loader.py",
        "/opt/himr-gpu/app/verified_dependency_loader.py",
        "python_source",
    ),
    "model_admission_helper": (
        "app/admit_hf_model.py",
        "/opt/himr-gpu/app/admit_hf_model.py",
        "python_source",
    ),
    "runtime_admission_helper": (
        "app/admit_runtime_v2.py",
        "/opt/himr-gpu/app/admit_runtime_v2.py",
        "python_source",
    ),
    "cublas_library_directory": (
        "runtime/lib/python3.12/site-packages/nvidia/cublas/lib",
        "/opt/himr-gpu/runtime/lib/python3.12/site-packages/nvidia/cublas/lib",
        "shared_library_directory",
    ),
}


def _absolute(path: Path, label: str) -> Path:
    try:
        normalized = BUILDER._absolute_path(str(path), label)
    except BUILDER.ExecutionImageError as error:
        raise ASRImageSpecError(str(error)) from error
    try:
        normalized.relative_to(COLD_ROOT)
    except ValueError:
        return normalized
    raise ASRImageSpecError(f"{label} may not reference cold storage")


def _entry(kind: str, source: Path, image: PurePosixPath, mode: int) -> dict[str, Any]:
    return {
        "kind": kind,
        "source_path": str(source),
        "image_relative_path": image.as_posix(),
        "image_mode": f"{mode:04o}",
    }


def _walk_projection(
    source_root: Path,
    image_root: PurePosixPath,
    *,
    exclude_top_level: set[str] | None = None,
    exclude_relative_prefixes: set[PurePosixPath] | None = None,
) -> list[dict[str, Any]]:
    source_root = _absolute(source_root, "projection source root")
    root_info = source_root.lstat()
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise ASRImageSpecError("projection source root must be a direct directory")
    excluded = exclude_top_level or set()
    excluded_prefixes = exclude_relative_prefixes or set()
    rows = [_entry("directory", source_root, image_root, 0o555)]
    stack: list[tuple[Path, PurePosixPath, int]] = [(source_root, image_root, 0)]
    while stack:
        source_parent, image_parent, depth = stack.pop()
        with os.scandir(source_parent) as iterator:
            children = sorted(iterator, key=lambda item: item.name)
        directories: list[tuple[Path, PurePosixPath, int]] = []
        for child in children:
            if depth == 0 and child.name in excluded:
                continue
            if child.name == "__pycache__" or child.name.endswith(".pyc"):
                continue
            source = source_parent / child.name
            relative = PurePosixPath(*source.relative_to(source_root).parts)
            if any(
                relative == prefix or prefix in relative.parents
                for prefix in excluded_prefixes
            ):
                continue
            info = child.stat(follow_symlinks=False)
            image = image_parent / child.name
            if stat.S_ISLNK(info.st_mode):
                raise ASRImageSpecError(f"source symlink is not allowed: {source}")
            if stat.S_ISDIR(info.st_mode):
                rows.append(_entry("directory", source, image, 0o555))
                directories.append((source, image, depth + 1))
            elif stat.S_ISREG(info.st_mode):
                if info.st_nlink != 1 or stat.S_IMODE(info.st_mode) & 0o7022:
                    raise ASRImageSpecError(f"unsafe source file metadata: {source}")
                rows.append(_entry("regular_file", source, image, 0o444))
            else:
                raise ASRImageSpecError(f"special source file is not allowed: {source}")
        stack.extend(reversed(directories))
    return rows


def _validate_site_packages_layout(site_packages: Path) -> None:
    """Reject every unreviewed top-level import or NVIDIA namespace entry."""

    expected = SITE_PACKAGES_ALLOW_TOP_LEVEL | SITE_PACKAGES_EXCLUDE_TOP_LEVEL
    with os.scandir(site_packages) as iterator:
        children = list(iterator)
    observed_names = {child.name for child in children}
    missing = sorted(SITE_PACKAGES_ALLOW_TOP_LEVEL - observed_names)
    if missing:
        raise ASRImageSpecError(
            f"required site-packages top-level entry is absent: {missing[0]}"
        )
    for child in children:
        info = child.stat(follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise ASRImageSpecError(
                f"site-packages symlink is not allowed: {site_packages / child.name}"
            )
        if child.name not in expected:
            raise ASRImageSpecError(
                f"unreviewed site-packages top-level entry: {child.name}"
            )

    nvidia = site_packages / "nvidia"
    if not nvidia.exists():
        return
    allowed_nvidia = NVIDIA_ALLOW_DIRECT_CHILDREN | NVIDIA_EXCLUDE_DIRECT_CHILDREN
    with os.scandir(nvidia) as iterator:
        nvidia_children = list(iterator)
    for child in nvidia_children:
        info = child.stat(follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise ASRImageSpecError(
                f"NVIDIA package symlink is not allowed: {nvidia / child.name}"
            )
        if child.name not in allowed_nvidia:
            raise ASRImageSpecError(
                f"unreviewed NVIDIA package entry: {child.name}"
            )

    cublas = nvidia / "cublas"
    if not cublas.exists():
        raise ASRImageSpecError("required NVIDIA cublas package is absent")
    with os.scandir(cublas) as iterator:
        cublas_children = list(iterator)
    if {child.name for child in cublas_children} != {"include", "lib"}:
        raise ASRImageSpecError("NVIDIA cublas package layout is not exact")
    for child in cublas_children:
        info = child.stat(follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ASRImageSpecError("NVIDIA cublas package layout is unsafe")

    cublas_lib = cublas / "lib"
    with os.scandir(cublas_lib) as iterator:
        cublas_libraries = list(iterator)
    expected_libraries = (
        NVIDIA_CUBLAS_ALLOW_LIBRARY_FILES
        | NVIDIA_CUBLAS_EXCLUDE_LIBRARY_FILES
    )
    if {child.name for child in cublas_libraries} != expected_libraries:
        raise ASRImageSpecError("NVIDIA cublas library closure is not exact")
    for child in cublas_libraries:
        info = child.stat(follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise ASRImageSpecError("NVIDIA cublas library closure is unsafe")


def _exact_source_set(
    sources: Iterable[Path],
    names: tuple[str, ...],
    label: str,
) -> tuple[list[Path], Path]:
    normalized = sorted(
        (_absolute(path, f"{label} source") for path in sources),
        key=lambda path: path.name,
    )
    if [path.name for path in normalized] != sorted(names):
        raise ASRImageSpecError(f"{label} source set is not the exact required closure")
    parents = {path.parent for path in normalized}
    if len(parents) != 1:
        raise ASRImageSpecError(f"{label} sources must share one direct parent")
    for source in normalized:
        info = source.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) & 0o7022
        ):
            raise ASRImageSpecError(f"unsafe {label} source metadata: {source}")
    return normalized, next(iter(parents))


def build_spec(
    *,
    python_root: Path,
    site_packages: Path,
    model_bundle: Path,
    app_sources: Iterable[Path],
    pipeline_support_sources: Iterable[Path],
    gpu_package_support_sources: Iterable[Path],
    corpus_support_sources: Iterable[Path],
    intended_mount_path: str,
    source_epoch: int,
) -> dict[str, Any]:
    python_root = _absolute(python_root, "Python root")
    site_packages = _absolute(site_packages, "site-packages")
    model_bundle = _absolute(model_bundle, "model bundle")
    _validate_site_packages_layout(site_packages)

    entries: list[dict[str, Any]] = []
    entries.extend(
        _walk_projection(
            python_root / "lib/python3.12",
            PurePosixPath("runtime/lib/python3.12"),
            exclude_top_level={"site-packages"},
            exclude_relative_prefixes=PYTHON_STDLIB_EXCLUDE_RELATIVE_PREFIXES,
        )
    )
    # Explicit parents not supplied by the stdlib projection.
    entries.extend(
        [
            _entry("directory", python_root, PurePosixPath("runtime"), 0o555),
            _entry("directory", python_root / "bin", PurePosixPath("runtime/bin"), 0o555),
            _entry("directory", python_root / "lib", PurePosixPath("runtime/lib"), 0o555),
            # The standalone interpreter has every extension used by this
            # runtime built in, so projecting the host-specific extension
            # modules would only widen the ABI closure.  Python still probes
            # for this conventional directory while calculating exec_prefix;
            # retaining the empty directory avoids a warning on every worker
            # start without admitting any extension object.
            _entry(
                "directory",
                python_root / "lib/python3.12/lib-dynload",
                PurePosixPath("runtime/lib/python3.12/lib-dynload"),
                0o555,
            ),
            _entry(
                "regular_file",
                python_root / "bin/python3.12",
                PurePosixPath("runtime/bin/python3.12"),
                0o555,
            ),
        ]
    )
    entries.extend(
        _walk_projection(
            site_packages,
            PurePosixPath("runtime/lib/python3.12/site-packages"),
            exclude_top_level=SITE_PACKAGES_EXCLUDE_TOP_LEVEL,
            exclude_relative_prefixes=SITE_PACKAGES_EXCLUDE_RELATIVE_PREFIXES,
        )
    )
    entries.extend(_walk_projection(model_bundle, PurePosixPath("model")))

    normalized_sources, app_source_root = _exact_source_set(
        app_sources,
        APP_SOURCE_NAMES,
        "application",
    )
    normalized_pipeline_support, pipeline_support_root = _exact_source_set(
        pipeline_support_sources,
        PIPELINE_SUPPORT_SOURCE_NAMES,
        "pipeline support",
    )
    normalized_gpu_support, gpu_support_root = _exact_source_set(
        gpu_package_support_sources,
        GPU_PACKAGE_SUPPORT_SOURCE_NAMES,
        "GPU package support",
    )
    normalized_corpus_support, corpus_package_root = _exact_source_set(
        corpus_support_sources,
        CORPUS_SUPPORT_SOURCE_NAMES,
        "corpus support",
    )
    entries.extend(
        [
            _entry("directory", app_source_root, PurePosixPath("app"), 0o555),
            *[
                _entry(
                    "regular_file", source, PurePosixPath("app") / source.name, 0o444
                )
                for source in normalized_sources
            ],
            *[
                _entry(
                    "regular_file", source, PurePosixPath("app") / source.name, 0o444
                )
                for source in normalized_pipeline_support
            ],
            _entry(
                "directory", gpu_support_root, PurePosixPath("app/gpu"), 0o555
            ),
            *[
                _entry(
                    "regular_file",
                    source,
                    PurePosixPath("app/gpu") / source.name,
                    0o444,
                )
                for source in normalized_gpu_support
            ],
            _entry(
                "directory",
                corpus_package_root.parent.parent,
                PurePosixPath("corpus"),
                0o555,
            ),
            _entry(
                "directory",
                corpus_package_root.parent,
                PurePosixPath("corpus/src"),
                0o555,
            ),
            _entry(
                "directory",
                corpus_package_root,
                PurePosixPath("corpus/src/himr_corpus"),
                0o555,
            ),
            *[
                _entry(
                    "regular_file",
                    source,
                    PurePosixPath("corpus/src/himr_corpus") / source.name,
                    0o444,
                )
                for source in normalized_corpus_support
            ],
        ]
    )
    entries.sort(key=lambda item: item["image_relative_path"])
    paths = [item["image_relative_path"] for item in entries]
    if len(paths) != len(set(paths)):
        raise ASRImageSpecError("source projections overlap at an image path")

    mappings = [
        {
            "name": name,
            "image_relative_path": image_path,
            "sandbox_path": sandbox_path,
            "role": role,
        }
        for name, (image_path, sandbox_path, role) in sorted(MAPPING_LAYOUT.items())
    ]
    raw = {
        "kind": BUILDER.SPEC_KIND,
        "schema_version": BUILDER.SCHEMA_VERSION,
        "source_epoch": source_epoch,
        "intended_mount_path": intended_mount_path,
        "entries": entries,
        "logical_mappings": mappings,
        "policy": dict(BUILDER.SPEC_POLICY),
    }
    try:
        return BUILDER.normalize_spec(raw)
    except BUILDER.ExecutionImageError as error:
        raise ASRImageSpecError(f"generated projection is invalid: {error}") from error


def _write_exclusive(path: Path, value: Any) -> None:
    path = _absolute(path, "spec output")
    if path.exists() or path.is_symlink():
        raise ASRImageSpecError("spec output already exists")
    info = path.parent.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ASRImageSpecError("spec output parent must be current-user mode 0700")
    body = BUILDER.canonical_bytes(value)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
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
    finally:
        if temporary.exists():
            temporary.unlink()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts")
    create = commands.add_parser("create")
    create.add_argument("--output", required=True)
    create.add_argument("--source-epoch", required=True, type=int)
    create.add_argument("--intended-mount-path", required=True)
    create.add_argument("--python-root", default=str(DEFAULT_PYTHON_ROOT))
    create.add_argument("--site-packages", default=str(DEFAULT_SITE_PACKAGES))
    create.add_argument("--model-bundle", default=str(DEFAULT_MODEL_BUNDLE))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contracts":
            response = {
                "kind": "himr_gpu_asr_execution_image_spec_contract",
                "schema_version": 1,
                "application_sources": list(APP_SOURCE_NAMES),
                "pipeline_support_sources": list(PIPELINE_SUPPORT_SOURCE_NAMES),
                "gpu_package_support_sources": list(
                    GPU_PACKAGE_SUPPORT_SOURCE_NAMES
                ),
                "corpus_support_sources": list(CORPUS_SUPPORT_SOURCE_NAMES),
                "site_packages_allowed_top_level": sorted(
                    SITE_PACKAGES_ALLOW_TOP_LEVEL
                ),
                "site_packages_excluded_top_level": sorted(
                    SITE_PACKAGES_EXCLUDE_TOP_LEVEL
                ),
                "site_packages_excluded_relative_prefixes": sorted(
                    path.as_posix()
                    for path in SITE_PACKAGES_EXCLUDE_RELATIVE_PREFIXES
                ),
                "nvidia_allowed_direct_children": sorted(
                    NVIDIA_ALLOW_DIRECT_CHILDREN
                ),
                "nvidia_excluded_direct_children": sorted(
                    NVIDIA_EXCLUDE_DIRECT_CHILDREN
                ),
                "nvidia_cublas_allowed_library_files": sorted(
                    NVIDIA_CUBLAS_ALLOW_LIBRARY_FILES
                ),
                "nvidia_cublas_excluded_library_files": sorted(
                    NVIDIA_CUBLAS_EXCLUDE_LIBRARY_FILES
                ),
                "python_stdlib_excluded_relative_prefixes": sorted(
                    path.as_posix()
                    for path in PYTHON_STDLIB_EXCLUDE_RELATIVE_PREFIXES
                ),
                "mapping_layout": {
                    name: {
                        "image_relative_path": row[0],
                        "sandbox_path": row[1],
                        "role": row[2],
                    }
                    for name, row in sorted(MAPPING_LAYOUT.items())
                },
                "wheelhouse_included": False,
                "shared_libpython_included": False,
                "cold_storage_allowed": False,
            }
        else:
            spec = build_spec(
                python_root=Path(args.python_root),
                site_packages=Path(args.site_packages),
                model_bundle=Path(args.model_bundle),
                app_sources=[HERE / name for name in APP_SOURCE_NAMES],
                pipeline_support_sources=[
                    PIPELINE_ROOT / name for name in PIPELINE_SUPPORT_SOURCE_NAMES
                ],
                gpu_package_support_sources=[
                    HERE / name for name in GPU_PACKAGE_SUPPORT_SOURCE_NAMES
                ],
                corpus_support_sources=[
                    CORPUS_PACKAGE_ROOT / name for name in CORPUS_SUPPORT_SOURCE_NAMES
                ],
                intended_mount_path=args.intended_mount_path,
                source_epoch=args.source_epoch,
            )
            output = Path(args.output)
            _write_exclusive(output, spec)
            body = BUILDER.canonical_bytes(spec)
            response = {
                "status": "created",
                "path": str(output),
                "sha256": BUILDER.sha256_bytes(body),
                "entry_count": len(spec["entries"]),
            }
        sys.stdout.buffer.write(BUILDER.canonical_bytes(response))
        return 0
    except (ASRImageSpecError, BUILDER.ExecutionImageError, OSError, ValueError) as error:
        sys.stderr.buffer.write(
            BUILDER.canonical_bytes(
                {
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
