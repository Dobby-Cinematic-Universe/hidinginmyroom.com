from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]
TEST_WORK_ROOT = ROOT / "pipeline/.test-work"


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SPEC = load_module(
    "himr_gpu_asr_execution_image_spec_test_module",
    ROOT / "pipeline/gpu/build_asr_execution_image_spec.py",
)
ADMISSION = load_module(
    "himr_gpu_admission_for_image_spec_test_module",
    ROOT / "pipeline/gpu/admit_runtime_v2.py",
)


class GPUASRExecutionImageSpecTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="gpu-image-spec-", dir=TEST_WORK_ROOT
        )
        self.base = Path(self.temporary.name)
        self.base.chmod(0o700)
        self.python = self.base / "python"
        (self.python / "bin").mkdir(parents=True)
        (self.python / "lib/python3.12/site-packages").mkdir(parents=True)
        self.file(self.python / "bin/python3.12", b"python\n", 0o500)
        self.file(self.python / "lib/libpython3.12.so.1.0", b"libpython\n")
        self.file(self.python / "lib/python3.12/os.py", b"# os\n")
        self.file(
            self.python / "lib/python3.12/lib-dynload/_crypt.fake.so",
            b"unused extension\n",
        )
        self.file(
            self.python / "lib/python3.12/site-packages/ignored.py",
            b"# base package must be excluded\n",
        )
        self.site = self.base / "site"
        for relative in (
            "nvidia/cublas/lib",
            "nvidia/cuda_nvrtc/lib",
            "nvidia/cudnn/lib",
            "faster_whisper",
        ):
            (self.site / relative).mkdir(parents=True, exist_ok=True)
        self.file(self.site / "nvidia/cublas/lib/libcublas.so.12", b"cublas\n")
        self.file(self.site / "nvidia/cublas/lib/libcublasLt.so.12", b"cublas lt\n")
        self.file(self.site / "nvidia/cublas/lib/libnvblas.so.12", b"nvblas\n")
        self.file(self.site / "nvidia/cublas/include/cublas.h", b"header\n")
        self.file(self.site / "nvidia/cudnn/lib/libcudnn.so.9", b"cudnn\n")
        self.file(self.site / "nvidia/cuda_nvrtc/lib/libnvrtc.so.12", b"nvrtc\n")
        self.file(self.site / "faster_whisper/__init__.py", b"# package\n")
        for name in SPEC.SITE_PACKAGES_ALLOW_TOP_LEVEL:
            path = self.site / name
            if path.exists():
                continue
            if name.endswith(".py") or name == "_yaml":
                self.file(path, b"# admitted package fixture\n")
            else:
                path.mkdir(parents=True)
        self.file(self.site / "__pycache__/discard.pyc", b"bytecode\n")
        self.file(self.site / "_virtualenv.pth", b"import _virtualenv\n")
        self.file(
            self.site / "setuptools/launcher manifest.xml",
            b"must not enter the execution image\n",
        )
        self.file(
            self.site / "onnxruntime/__init__.py",
            b"VAD is disabled by the exact profile\n",
        )
        self.model = self.base / "model"
        (self.model / "snapshot").mkdir(parents=True)
        self.file(self.model / "manifest.json", b"{}\n")
        self.file(self.model / "snapshot/model.bin", b"model\n")
        self.app = self.base / "app"
        self.app.mkdir()
        self.sources = []
        for name in SPEC.APP_SOURCE_NAMES:
            path = self.app / name
            self.file(path, f"# {name}\n".encode())
            self.sources.append(path)
        self.pipeline_support = self.base / "pipeline-support"
        self.pipeline_support_sources = []
        for name in SPEC.PIPELINE_SUPPORT_SOURCE_NAMES:
            path = self.pipeline_support / name
            self.file(path, f"# {name}\n".encode())
            self.pipeline_support_sources.append(path)
        self.gpu_support = self.base / "gpu-support"
        self.gpu_support_sources = []
        for name in SPEC.GPU_PACKAGE_SUPPORT_SOURCE_NAMES:
            path = self.gpu_support / name
            self.file(path, f"# {name}\n".encode())
            self.gpu_support_sources.append(path)
        self.corpus_support = self.base / "corpus/src/himr_corpus"
        self.corpus_support_sources = []
        for name in SPEC.CORPUS_SUPPORT_SOURCE_NAMES:
            path = self.corpus_support / name
            self.file(path, f"# {name}\n".encode())
            self.corpus_support_sources.append(path)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def file(path: Path, body: bytes, mode: int = 0o400) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        path.chmod(mode)

    def build(self) -> dict[str, object]:
        return SPEC.build_spec(
            python_root=self.python,
            site_packages=self.site,
            model_bundle=self.model,
            app_sources=self.sources,
            pipeline_support_sources=self.pipeline_support_sources,
            gpu_package_support_sources=self.gpu_support_sources,
            corpus_support_sources=self.corpus_support_sources,
            intended_mount_path="/run/user/1000/himr-gpu-v2/image",
            source_epoch=1_700_000_000,
        )

    def test_projection_is_closed_deterministic_and_matches_admission_layout(self) -> None:
        first = self.build()
        second = self.build()
        self.assertEqual(first, second)
        paths = {row["image_relative_path"] for row in first["entries"]}
        self.assertIn("runtime/bin/python3.12", paths)
        self.assertNotIn("runtime/lib/libpython3.12.so.1.0", paths)
        self.assertIn("runtime/lib/python3.12/os.py", paths)
        self.assertIn("runtime/lib/python3.12/lib-dynload", paths)
        self.assertFalse(
            any(path.startswith("runtime/lib/python3.12/lib-dynload/") for path in paths)
        )
        self.assertNotIn("runtime/lib/python3.12/site-packages/ignored.py", paths)
        self.assertNotIn("runtime/lib/python3.12/site-packages/discard.pyc", paths)
        self.assertNotIn("runtime/lib/python3.12/site-packages/_virtualenv.pth", paths)
        self.assertFalse(any("/setuptools" in path for path in paths))
        self.assertFalse(any("/onnxruntime" in path for path in paths))
        self.assertFalse(any("/nvidia/cudnn" in path for path in paths))
        self.assertFalse(any("/nvidia/cuda_nvrtc" in path for path in paths))
        self.assertFalse(any("/nvidia/cublas/include" in path for path in paths))
        self.assertFalse(any("libnvblas" in path for path in paths))
        self.assertIn(
            "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublasLt.so.12",
            paths,
        )
        self.assertFalse(any("wheelhouse" in path for path in paths))
        self.assertIn("app/preprocess_gpu_asr_queue_v1.py", paths)
        self.assertIn("app/gpu/portable_root.py", paths)
        self.assertIn("corpus/src/himr_corpus/result_importers.py", paths)
        layout = {
            row["name"]: (row["sandbox_path"], row["role"])
            for row in first["logical_mappings"]
        }
        self.assertEqual(layout, ADMISSION.REQUIRED_MAPPING_LAYOUT)

    def test_missing_app_source_symlink_and_cold_path_fail(self) -> None:
        with self.assertRaisesRegex(SPEC.ASRImageSpecError, "exact required closure"):
            SPEC.build_spec(
                python_root=self.python,
                site_packages=self.site,
                model_bundle=self.model,
                app_sources=self.sources[:-1],
                pipeline_support_sources=self.pipeline_support_sources,
                gpu_package_support_sources=self.gpu_support_sources,
                corpus_support_sources=self.corpus_support_sources,
                intended_mount_path="/run/user/1000/himr-gpu-v2/image",
                source_epoch=1_700_000_000,
            )
        link = self.site / "faster_whisper/link.py"
        link.symlink_to("__init__.py")
        with self.assertRaisesRegex(SPEC.ASRImageSpecError, "symlink"):
            self.build()
        link.unlink()
        self.file(self.site / "evil.pth", b"import evil\n")
        with self.assertRaisesRegex(
            SPEC.ASRImageSpecError, "unreviewed site-packages"
        ):
            self.build()
        (self.site / "evil.pth").unlink()
        (self.site / "unknown_package").mkdir()
        with self.assertRaisesRegex(
            SPEC.ASRImageSpecError, "unreviewed site-packages"
        ):
            self.build()
        (self.site / "unknown_package").rmdir()
        (self.site / "av").rmdir()
        with self.assertRaisesRegex(
            SPEC.ASRImageSpecError, "required site-packages"
        ):
            self.build()
        (self.site / "av").mkdir()
        with self.assertRaisesRegex(
            SPEC.ASRImageSpecError, "cold storage|archive tier"
        ):
            SPEC.build_spec(
                python_root=Path("/mnt/archive/HIMR/python"),
                site_packages=self.site,
                model_bundle=self.model,
                app_sources=self.sources,
                pipeline_support_sources=self.pipeline_support_sources,
                gpu_package_support_sources=self.gpu_support_sources,
                corpus_support_sources=self.corpus_support_sources,
                intended_mount_path="/run/user/1000/himr-gpu-v2/image",
                source_epoch=1_700_000_000,
            )

    @unittest.skipUnless(
        SPEC.DEFAULT_PYTHON_ROOT.is_dir()
        and SPEC.DEFAULT_SITE_PACKAGES.is_dir()
        and SPEC.DEFAULT_MODEL_BUNDLE.is_dir(),
        "local admitted GPU candidate is unavailable",
    )
    def test_local_candidate_projection_has_the_reviewed_minimal_closure(self) -> None:
        value = SPEC.build_spec(
            python_root=SPEC.DEFAULT_PYTHON_ROOT,
            site_packages=SPEC.DEFAULT_SITE_PACKAGES,
            model_bundle=SPEC.DEFAULT_MODEL_BUNDLE,
            app_sources=[SPEC.HERE / name for name in SPEC.APP_SOURCE_NAMES],
            pipeline_support_sources=[
                SPEC.PIPELINE_ROOT / name
                for name in SPEC.PIPELINE_SUPPORT_SOURCE_NAMES
            ],
            gpu_package_support_sources=[
                SPEC.HERE / name for name in SPEC.GPU_PACKAGE_SUPPORT_SOURCE_NAMES
            ],
            corpus_support_sources=[
                SPEC.CORPUS_PACKAGE_ROOT / name
                for name in SPEC.CORPUS_SUPPORT_SOURCE_NAMES
            ],
            intended_mount_path="/run/user/1000/himr-gpu-v2/image",
            source_epoch=1_788_019_906,
        )
        paths = {row["image_relative_path"] for row in value["entries"]}
        self.assertEqual(len(value["logical_mappings"]), 12)
        self.assertFalse(any(path.endswith(".pth") for path in paths))
        self.assertFalse(any("/nvidia/cudnn" in path for path in paths))
        self.assertFalse(any("/nvidia/cuda_nvrtc" in path for path in paths))
        self.assertFalse(any("/nvidia/cublas/include" in path for path in paths))
        self.assertFalse(any("libnvblas" in path for path in paths))
        self.assertNotIn("runtime/lib/libpython3.12.so.1.0", paths)
        self.assertIn("runtime/lib/python3.12/lib-dynload", paths)
        self.assertFalse(
            any(path.startswith("runtime/lib/python3.12/lib-dynload/") for path in paths)
        )


if __name__ == "__main__":
    unittest.main()
