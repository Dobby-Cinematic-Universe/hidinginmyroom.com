from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PLANNER = load_module(
    "himr_longform_planner_runner_test",
    ROOT / "corpus/src/himr_corpus/longform_asr_planner.py",
)
RUNNER = load_module(
    "himr_longform_runner_test",
    ROOT / "pipeline/gpu/longform_asr_runner_v1.py",
)
PROFILE = load_module(
    "himr_gpu_profile_longform_runner_test",
    ROOT / "pipeline/gpu/production_profile_v2.py",
)


class FakeEngine:
    def __init__(
        self,
        *,
        model_revision: str = "fixture",
        model_identity_sha256: str = "9" * 64,
    ) -> None:
        self.calls: list[object] = []
        self.model_revision = model_revision
        self.model_identity_sha256 = model_identity_sha256

    def transcribe(self, request: object) -> dict[str, object]:
        self.calls.append(request)
        return {
            "engine": {
                "library": "fake-cpu-engine",
                "library_version": "1.0",
                "model_revision": self.model_revision,
                "model_identity_sha256": self.model_identity_sha256,
                "input_decoder": {
                    "kind": "fake_retained_input",
                    "executable_path": None,
                    "executable_sha256": None,
                    "execution_mode": "retained_parent_descriptor_fixture",
                    "persistent_audio_chunks": False,
                },
            },
            "language": {
                "value": "en",
                "selection_basis": "forced_by_execution_work_order",
                "detection_performed": False,
                "probability_raw": None,
            },
            "duration_samples": request.source.analysis_sample_count,
            "segments": [],
        }


class FakeSequentialDecoder:
    instances: list["FakeSequentialDecoder"] = []

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.args = args
        self.kwargs = kwargs
        self.finished = False
        self.closed = False
        self.__class__.instances.append(self)

    def finish(self, *, required_end_sample: int | None = None) -> None:
        self.required_end_sample = required_end_sample
        self.finished = True

    def close(self) -> None:
        self.closed = True


class LongFormRunnerV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.audio = self.root / "audio.flac"
        self.audio.write_bytes(b"runner-fixture")
        body = self.audio.read_bytes()
        manifest = {
            "kind": PLANNER.MANIFEST_KIND,
            "schema_version": 1,
            "recording": {
                "recording_id": "rec_runner",
                "media_id": "media_runner",
                "input": {
                    "artifact_id": "artifact_runner",
                    "path": str(self.audio),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "byte_count": len(body),
                    "sample_rate_hz": 16_000,
                    "channels": 1,
                    "total_samples": 16_000,
                    "duration_ms": 1_000,
                },
            },
            "boundary_candidates": [],
        }
        policy = {
            "kind": PLANNER.POLICY_KIND,
            "schema_version": 1,
            "direct_max_samples": 40_000,
            "adaptive": {
                "min_core_samples": 10_000,
                "target_core_samples": 20_000,
                "max_core_samples": 40_000,
                "boundary_search_samples": 1_000,
                "padding_samples": 1_000,
                "max_span_count": 10,
            },
        }
        self.plan = PLANNER.build_longform_asr_plan(manifest, policy)
        self.plan_path = self.root / "plan.json"
        self.plan_path.write_text(
            PLANNER.canonical_json(self.plan), encoding="utf-8"
        )
        self.output = self.root / "output"
        self.profile = PROFILE.default_profile()
        self.profile_path = (
            self.root
            / "gpu-runtime"
            / "portable-v2-control"
            / "production-profile-v2.json"
        )
        self.profile_path.parent.mkdir(parents=True)
        profile_body = RUNNER.E.canonical_bytes(self.profile)
        self.profile_path.write_bytes(profile_body)
        runtime_core = {
            "kind": "himr_gpu_runtime_admission_receipt",
            "schema_version": 2,
            "status": "candidate",
            "production_profile": {
                "identity_sha256": self.profile["identity_sha256"]
            },
            "runtime": {
                "packages": {"fake-runtime-package": "1.0"},
                "python_version": "fixture",
            },
        }
        runtime_identity = hashlib.sha256(
            RUNNER.E.canonical_bytes(runtime_core)
        ).hexdigest()
        runtime = {
            **runtime_core,
            "identity_sha256": runtime_identity,
            "receipt_id": f"gpurtv2_{runtime_identity[:32]}",
        }
        self.runtime_path = self.profile_path.parent / "runtime-candidate-v2.json"
        runtime_body = RUNNER.E.canonical_bytes(runtime)
        self.runtime_path.write_bytes(runtime_body)
        self.runtime_sha256 = hashlib.sha256(runtime_body).hexdigest()
        self.profile_for_control = dict(self.profile)
        self.profile_for_control.update(
            {
                "_physical_sha256": hashlib.sha256(profile_body).hexdigest(),
                "_runtime_receipt_id": runtime["receipt_id"],
                "_runtime_identity_sha256": runtime_identity,
                "_runtime_physical_sha256": self.runtime_sha256,
                "_runtime_status": "candidate",
            }
        )
        self.lock_root = self.root / "locks"
        self.lock_root.mkdir(mode=0o700)
        self.lock_root.chmod(0o700)
        self.gpu_uuid = self.profile["hardware"]["gpu_uuid"]
        self.gpu_lock = self.lock_root / f"gpu-{self.gpu_uuid}.lock"
        self.gpu_opportunity_lock = (
            self.lock_root / f"gpu-{self.gpu_uuid}.opportunity.lock"
        )
        for lock_path in (self.gpu_lock, self.gpu_opportunity_lock):
            descriptor = os.open(
                lock_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            os.close(descriptor)
        controller = {
            "kind": "himr_autonomous_archive_controller_config",
            "gpu_readiness": {
                "enabled": True,
                "production_profile": str(self.profile_path),
                "production_profile_sha256": hashlib.sha256(profile_body).hexdigest(),
                "runtime_admission": str(self.runtime_path),
                "runtime_admission_sha256": self.runtime_sha256,
                "lock_root": str(self.lock_root),
            },
        }
        controller_body = RUNNER.E.canonical_bytes(controller)
        self.controller_path = self.root / "controller-config.json"
        self.controller_path.write_bytes(controller_body)
        self.controller_sha256 = hashlib.sha256(controller_body).hexdigest()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def arguments(self) -> argparse.Namespace:
        return argparse.Namespace(
            plan=self.plan_path,
            output_root=self.output,
            initial_prompt=None,
            hotwords_json=None,
            controller_config=self.controller_path,
            controller_config_sha256=self.controller_sha256,
            honor_controller_stop=False,
            yield_to_ordinary_gpu=False,
        )

    def test_status_is_read_only_and_reports_pending(self) -> None:
        result = RUNNER.command_status(self.arguments())
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["span_counts"], {"completed": 0, "pending": 1, "failed": 0})
        self.assertFalse(result["gpu_invoked"])
        self.assertFalse(self.output.exists())

    def test_status_cli_emits_one_strict_json_object(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(ROOT / "pipeline/gpu/longform_asr_runner_v1.py"),
                "status",
                "--plan",
                str(self.plan_path),
                "--output-root",
                str(self.output),
                "--controller-config",
                str(self.controller_path),
                "--controller-config-sha256",
                self.controller_sha256,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        value = json.loads(completed.stdout)
        self.assertEqual(value["status"], "incomplete")
        self.assertFalse(value["gpu_invoked"])

    def test_gpu_uuid_lock_excludes_a_second_holder(self) -> None:
        gpu_uuid = "GPU-01234567-89ab-cdef-0123-456789abcdef"
        lock = self.root / f"gpu-{gpu_uuid}.lock"
        descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        with RUNNER._gpu_lock(lock, gpu_uuid):
            with self.assertRaisesRegex(RUNNER.RunnerError, "occupied"):
                with RUNNER._gpu_lock(lock, gpu_uuid):
                    self.fail("second lock holder entered")

    def test_gpu_opportunity_lock_excludes_a_second_scheduler(self) -> None:
        with RUNNER._gpu_opportunity_lock(
            self.gpu_opportunity_lock, self.gpu_uuid
        ):
            with self.assertRaisesRegex(RUNNER.RunnerError, "opportunity lock is occupied"):
                with RUNNER._gpu_opportunity_lock(
                    self.gpu_opportunity_lock, self.gpu_uuid
                ):
                    self.fail("second opportunity holder entered")

    def test_run_plan_opportunity_contention_does_not_load_the_model(self) -> None:
        arguments = self.arguments()
        arguments.bindings_output = self.root / "contended-bindings.json"
        arguments.ffmpeg = Path("/unused/for/direct/strategy")
        arguments.ffmpeg_sha256 = "0" * 64
        control = {
            "config_sha256": self.controller_sha256,
            "profile": self.profile_for_control,
            "profile_path": self.profile_path,
            "profile_sha256": hashlib.sha256(self.profile_path.read_bytes()).hexdigest(),
            "runtime_sha256": self.runtime_sha256,
            "gpu_uuid": self.gpu_uuid,
            "lock_path": self.gpu_lock,
            "opportunity_lock_path": self.gpu_opportunity_lock,
            "model_root": self.root / "fake-model",
        }
        with (
            RUNNER._gpu_opportunity_lock(
                self.gpu_opportunity_lock, self.gpu_uuid
            ),
            mock.patch.object(RUNNER, "_admitted_control", return_value=control),
            mock.patch.object(RUNNER, "_ensure_cuda_wheel_libraries"),
            mock.patch.object(RUNNER, "_verify_gpu_uuid") as verify_gpu,
            mock.patch.object(RUNNER, "_model") as model_factory,
            self.assertRaisesRegex(RUNNER.RunnerError, "opportunity lock is occupied"),
        ):
            RUNNER.command_run(arguments)

        verify_gpu.assert_not_called()
        model_factory.assert_not_called()
        self.assertFalse(arguments.bindings_output.exists())

    def test_run_plan_fake_engine_materializes_then_resumes_without_model_load(self) -> None:
        arguments = self.arguments()
        arguments.bindings_output = self.root / "bindings.json"
        arguments.ffmpeg = Path("/unused/for/direct/strategy")
        arguments.ffmpeg_sha256 = "0" * 64
        control = {
            "config_sha256": self.controller_sha256,
            "profile": self.profile_for_control,
            "profile_path": self.profile_path,
            "profile_sha256": hashlib.sha256(self.profile_path.read_bytes()).hexdigest(),
            "runtime_sha256": self.runtime_sha256,
            "gpu_uuid": self.gpu_uuid,
            "lock_path": self.gpu_lock,
            "opportunity_lock_path": self.gpu_opportunity_lock,
            "model_root": self.root / "fake-model",
        }
        engine = FakeEngine(
            model_revision=self.profile["model"]["revision"],
            model_identity_sha256=self.profile["model"]["identity_sha256"],
        )
        with (
            mock.patch.object(RUNNER, "_admitted_control", return_value=control),
            mock.patch.object(
                RUNNER, "_ensure_cuda_wheel_libraries"
            ) as cuda_libraries,
            mock.patch.object(RUNNER, "_verify_gpu_uuid"),
            mock.patch.object(RUNNER, "_model", return_value=engine) as model_factory,
        ):
            first = RUNNER.command_run(arguments)
        self.assertEqual(first["status"], "completed")
        self.assertEqual(first["model_load_count"], 1)
        self.assertEqual(len(engine.calls), 1)
        model_factory.assert_called_once()
        cuda_libraries.assert_called_once_with(control)
        bindings_body = arguments.bindings_output.read_bytes()
        bindings = json.loads(bindings_body)
        self.assertEqual(bindings["spans"][0]["status"], "completed")
        self.assertTrue(Path(bindings["spans"][0]["result"]["path"]).is_file())
        self.assertTrue(Path(bindings["spans"][0]["transcript"]["path"]).is_file())

        with (
            mock.patch.object(RUNNER, "_admitted_control", return_value=control),
            mock.patch.object(
                RUNNER, "_ensure_cuda_wheel_libraries"
            ) as cuda_libraries,
            mock.patch.object(RUNNER, "_verify_gpu_uuid") as verify_gpu,
            mock.patch.object(RUNNER, "_model") as model_factory,
        ):
            resumed = RUNNER.command_run(arguments)
        self.assertEqual(resumed["model_load_count"], 0)
        self.assertEqual(arguments.bindings_output.read_bytes(), bindings_body)
        verify_gpu.assert_not_called()
        model_factory.assert_not_called()
        cuda_libraries.assert_not_called()

    def test_model_rejects_inconsistent_visible_device_before_import(self) -> None:
        control = {
            "gpu_uuid": self.gpu_uuid,
            "model_root": self.root / "fake-model",
            "profile": self.profile_for_control,
        }
        with mock.patch.dict(
            os.environ,
            {"CUDA_VISIBLE_DEVICES": "GPU-ffffffff-ffff-ffff-ffff-ffffffffffff"},
        ):
            with self.assertRaisesRegex(RUNNER.RunnerError, "differs"):
                RUNNER._model(control, None)

    def test_cuda_library_reexec_is_exact_and_probes_after_restart(self) -> None:
        purelib = self.root / "runtime-purelib"
        library_directory = purelib / "nvidia" / "cublas" / "lib"
        library_directory.mkdir(parents=True)
        for name in ("libcublasLt.so.12", "libcublas.so.12"):
            (library_directory / name).write_bytes(b"fixture")
        control = {
            "runtime": {
                "runtime": {
                    "packages": {"nvidia-cublas-cu12": "12.9.2.10"}
                }
            }
        }
        with (
            mock.patch.object(
                RUNNER.sysconfig,
                "get_paths",
                return_value={"purelib": str(purelib)},
            ),
            mock.patch.object(
                RUNNER.sys,
                "argv",
                [str(RUNNER.SOURCE_PATH), "run-plan", "--fixture"],
            ),
            mock.patch.dict(
                RUNNER.os.environ,
                {
                    "LD_LIBRARY_PATH": "/unadmitted/host/library",
                    "PYTHONHOME": "/unadmitted/python-home",
                    "PYTHONPATH": "/unadmitted/python-path",
                },
                clear=False,
            ),
            mock.patch.object(
                RUNNER.os, "execve", side_effect=RuntimeError("fixture exec")
            ) as execute,
            self.assertRaisesRegex(RuntimeError, "fixture exec"),
        ):
            RUNNER._ensure_cuda_wheel_libraries(control)
        executable, argv, environment = execute.call_args.args
        self.assertEqual(executable, RUNNER.sys.executable)
        self.assertEqual(
            argv[:4],
            [
                RUNNER.sys.executable,
                "-B",
                "-I",
                str(RUNNER.SOURCE_PATH),
            ],
        )
        self.assertEqual(argv[4:], ["run-plan", "--fixture"])
        self.assertEqual(environment["LD_LIBRARY_PATH"], str(library_directory))
        self.assertNotIn("PYTHONHOME", environment)
        self.assertNotIn("PYTHONPATH", environment)

        with (
            mock.patch.object(
                RUNNER.sysconfig,
                "get_paths",
                return_value={"purelib": str(purelib)},
            ),
            mock.patch.dict(
                RUNNER.os.environ,
                {"LD_LIBRARY_PATH": str(library_directory)},
                clear=False,
            ),
            mock.patch.object(RUNNER.ctypes, "CDLL") as load_library,
        ):
            RUNNER._ensure_cuda_wheel_libraries(control)
        self.assertEqual(
            load_library.call_args_list,
            [
                mock.call("libcublasLt.so.12", mode=RUNNER.ctypes.RTLD_GLOBAL),
                mock.call("libcublas.so.12", mode=RUNNER.ctypes.RTLD_GLOBAL),
            ],
        )

    def test_cuda_library_bootstrap_rejects_missing_admitted_files(self) -> None:
        purelib = self.root / "missing-runtime-purelib"
        (purelib / "nvidia" / "cublas" / "lib").mkdir(parents=True)
        control = {
            "runtime": {
                "runtime": {
                    "packages": {"nvidia-cublas-cu12": "12.9.2.10"}
                }
            }
        }
        with mock.patch.object(
            RUNNER.sysconfig,
            "get_paths",
            return_value={"purelib": str(purelib)},
        ), self.assertRaisesRegex(RUNNER.RunnerError, "libraries are missing"):
            RUNNER._ensure_cuda_wheel_libraries(control)

        library_directory = purelib / "nvidia" / "cublas" / "lib"
        (library_directory / "libcublasLt.so.12").write_bytes(b"fixture")
        (library_directory / "libcublas.so.12").symlink_to("libcublasLt.so.12")
        with mock.patch.object(
            RUNNER.sysconfig,
            "get_paths",
            return_value={"purelib": str(purelib)},
        ), self.assertRaisesRegex(RUNNER.RunnerError, "unsafe metadata"):
            RUNNER._ensure_cuda_wheel_libraries(control)

    def test_live_gpu_resource_admission_and_shutdown(self) -> None:
        class NVML:
            NVML_TEMPERATURE_GPU = 0

            def __init__(
                self,
                *,
                compute_pid: int | None = None,
                graphics_pid: int | None = None,
            ) -> None:
                self.compute_pid = compute_pid
                self.graphics_pid = graphics_pid
                self.initialized = 0
                self.shutdown = 0

            def nvmlInit(self) -> None:
                self.initialized += 1

            def nvmlShutdown(self) -> None:
                self.shutdown += 1

            def nvmlDeviceGetHandleByUUID(self, value: object) -> object:
                return object()

            def nvmlDeviceGetUUID(self, handle: object) -> str:
                return self_uuid

            def nvmlDeviceGetComputeRunningProcesses(self, handle: object) -> list[object]:
                return (
                    []
                    if self.compute_pid is None
                    else [SimpleNamespace(pid=self.compute_pid)]
                )

            def nvmlDeviceGetGraphicsRunningProcesses(self, handle: object) -> list[object]:
                return (
                    []
                    if self.graphics_pid is None
                    else [SimpleNamespace(pid=self.graphics_pid)]
                )

            def nvmlDeviceGetMemoryInfo(self, handle: object) -> object:
                return SimpleNamespace(
                    total=8 * 1024**3,
                    used=2 * 1024**3,
                    free=6 * 1024**3,
                )

            def nvmlDeviceGetTemperature(self, handle: object, sensor: int) -> int:
                return 55

        self_uuid = self.gpu_uuid
        nvml = NVML()
        evidence = RUNNER._verify_gpu_uuid(
            self.gpu_uuid,
            self.profile["telemetry"],
            pynvml_module=nvml,
        )
        self.assertEqual(evidence["gpu_uuid"], self.gpu_uuid)
        self.assertEqual(evidence["temperature_c"], 55)
        self.assertEqual(nvml.initialized, 1)
        self.assertEqual(nvml.shutdown, 1)

        desktop_pid = os.getpid() + 10_000
        mixed = NVML(compute_pid=desktop_pid, graphics_pid=desktop_pid)
        evidence = RUNNER._verify_gpu_uuid(
            self.gpu_uuid,
            self.profile["telemetry"],
            pynvml_module=mixed,
        )
        self.assertEqual(evidence["foreign_cuda_pids"], [])
        self.assertEqual(evidence["compute_process_count"], 1)
        self.assertEqual(evidence["graphics_process_count"], 1)
        self.assertEqual(mixed.shutdown, 1)

        compute_only = NVML(compute_pid=os.getpid() + 20_000)
        with self.assertRaisesRegex(RUNNER.RunnerError, "other CUDA"):
            RUNNER._verify_gpu_uuid(
                self.gpu_uuid,
                self.profile["telemetry"],
                pynvml_module=compute_only,
            )
        self.assertEqual(compute_only.shutdown, 1)

        own_process = NVML(compute_pid=os.getpid())
        evidence = RUNNER._verify_gpu_uuid(
            self.gpu_uuid,
            self.profile["telemetry"],
            pynvml_module=own_process,
        )
        self.assertEqual(evidence["foreign_cuda_pids"], [])
        self.assertEqual(own_process.shutdown, 1)

    def test_adaptive_partial_resume_executes_only_pending_spans(self) -> None:
        source_body = self.audio.read_bytes()
        total_samples = 100_000
        manifest = {
            "kind": PLANNER.MANIFEST_KIND,
            "schema_version": 1,
            "recording": {
                "recording_id": "rec_runner_adaptive",
                "media_id": "media_runner_adaptive",
                "input": {
                    "artifact_id": "artifact_runner_adaptive",
                    "path": str(self.audio),
                    "sha256": hashlib.sha256(source_body).hexdigest(),
                    "byte_count": len(source_body),
                    "sample_rate_hz": 16_000,
                    "channels": 1,
                    "total_samples": total_samples,
                    "duration_ms": PLANNER.duration_ms_for_samples(total_samples),
                },
            },
            "boundary_candidates": [],
        }
        policy = {
            "kind": PLANNER.POLICY_KIND,
            "schema_version": 1,
            "direct_max_samples": 40_000,
            "adaptive": {
                "min_core_samples": 16_000,
                "target_core_samples": 32_000,
                "max_core_samples": 40_000,
                "boundary_search_samples": 4_000,
                "padding_samples": 8_000,
                "max_span_count": 100,
            },
        }
        plan = PLANNER.build_longform_asr_plan(manifest, policy)
        self.assertEqual(plan["strategy"], "adaptive_spans")
        self.assertGreater(len(plan["spans"]), 1)
        self.plan_path.write_text(PLANNER.canonical_json(plan), encoding="utf-8")
        orders = RUNNER._work_orders(
            plan,
            self.output,
            profile=self.profile_for_control,
            initial_prompt=None,
            hotwords=[],
        )
        with RUNNER.E.RetainedSource(plan["recording"]["input"]) as source:
            first_bundle = RUNNER.E.execute_work_order(
                orders[0],
                FakeEngine(
                    model_revision=self.profile["model"]["revision"],
                    model_identity_sha256=self.profile["model"]["identity_sha256"],
                ),
                retained_source=source,
            )
        RUNNER.E.materialize_result_bundle(first_bundle)
        first_result = Path(RUNNER.E.result_plan(orders[0])["result_path"])
        first_result_body = first_result.read_bytes()

        arguments = self.arguments()
        arguments.bindings_output = self.root / "adaptive-bindings.json"
        arguments.ffmpeg = Path("/fixture/ffmpeg")
        arguments.ffmpeg_sha256 = "0" * 64
        control = {
            "config_sha256": self.controller_sha256,
            "profile": self.profile_for_control,
            "profile_path": self.profile_path,
            "profile_sha256": hashlib.sha256(self.profile_path.read_bytes()).hexdigest(),
            "runtime_sha256": self.runtime_sha256,
            "gpu_uuid": self.gpu_uuid,
            "lock_path": self.gpu_lock,
            "opportunity_lock_path": self.gpu_opportunity_lock,
            "model_root": self.root / "fake-model",
        }
        engine = FakeEngine(
            model_revision=self.profile["model"]["revision"],
            model_identity_sha256=self.profile["model"]["identity_sha256"],
        )
        FakeSequentialDecoder.instances.clear()
        with (
            mock.patch.object(RUNNER, "_admitted_control", return_value=control),
            mock.patch.object(RUNNER, "_ensure_cuda_wheel_libraries"),
            mock.patch.object(RUNNER, "_verify_gpu_uuid"),
            mock.patch.object(RUNNER, "_model", return_value=engine) as model_factory,
            mock.patch.object(
                RUNNER.E,
                "SequentialFFmpegSpanDecoder",
                FakeSequentialDecoder,
            ),
        ):
            result = RUNNER.command_run(arguments)
        self.assertEqual(result["model_load_count"], 1)
        self.assertEqual(len(engine.calls), len(orders) - 1)
        self.assertEqual(
            [call.span_id for call in engine.calls],
            [order["span"]["span_id"] for order in orders[1:]],
        )
        model_factory.assert_called_once()
        self.assertEqual(len(FakeSequentialDecoder.instances), 1)
        self.assertTrue(FakeSequentialDecoder.instances[0].finished)
        self.assertTrue(FakeSequentialDecoder.instances[0].closed)
        self.assertEqual(first_result.read_bytes(), first_result_body)
        bindings = json.loads(arguments.bindings_output.read_bytes())
        self.assertTrue(all(row["status"] == "completed" for row in bindings["spans"]))

    def test_durable_stop_after_one_adaptive_span_returns_resumable_incomplete(self) -> None:
        source_body = self.audio.read_bytes()
        total_samples = 100_000
        manifest = {
            "kind": PLANNER.MANIFEST_KIND,
            "schema_version": 1,
            "recording": {
                "recording_id": "rec_stop_between_spans",
                "media_id": "media_stop_between_spans",
                "input": {
                    "artifact_id": "artifact_stop_between_spans",
                    "path": str(self.audio),
                    "sha256": hashlib.sha256(source_body).hexdigest(),
                    "byte_count": len(source_body),
                    "sample_rate_hz": 16_000,
                    "channels": 1,
                    "total_samples": total_samples,
                    "duration_ms": PLANNER.duration_ms_for_samples(total_samples),
                },
            },
            "boundary_candidates": [],
        }
        policy = {
            "kind": PLANNER.POLICY_KIND,
            "schema_version": 1,
            "direct_max_samples": 40_000,
            "adaptive": {
                "min_core_samples": 16_000,
                "target_core_samples": 32_000,
                "max_core_samples": 40_000,
                "boundary_search_samples": 4_000,
                "padding_samples": 8_000,
                "max_span_count": 100,
            },
        }
        plan = PLANNER.build_longform_asr_plan(manifest, policy)
        self.plan_path.write_text(PLANNER.canonical_json(plan), encoding="utf-8")
        arguments = self.arguments()
        arguments.bindings_output = self.root / "stopped-bindings.json"
        arguments.ffmpeg = Path("/fixture/ffmpeg")
        arguments.ffmpeg_sha256 = "0" * 64
        arguments.honor_controller_stop = True
        control = {
            "config_sha256": self.controller_sha256,
            "profile": self.profile_for_control,
            "profile_path": self.profile_path,
            "profile_sha256": hashlib.sha256(self.profile_path.read_bytes()).hexdigest(),
            "runtime_sha256": self.runtime_sha256,
            "gpu_uuid": self.gpu_uuid,
            "lock_path": self.gpu_lock,
            "opportunity_lock_path": self.gpu_opportunity_lock,
            "model_root": self.root / "fake-model",
            "controller_config_id": "himrautocfg_" + "1" * 32,
            "controller_state_root": self.root / "controller-state",
        }
        engine = FakeEngine(
            model_revision=self.profile["model"]["revision"],
            model_identity_sha256=self.profile["model"]["identity_sha256"],
        )
        FakeSequentialDecoder.instances.clear()
        with (
            mock.patch.object(RUNNER, "_admitted_control", return_value=control),
            mock.patch.object(RUNNER, "_ensure_cuda_wheel_libraries"),
            mock.patch.object(RUNNER, "_verify_gpu_uuid"),
            mock.patch.object(RUNNER, "_model", return_value=engine),
            mock.patch.object(
                RUNNER, "_controller_stop_requested", side_effect=[False, False, True]
            ),
            mock.patch.object(
                RUNNER.E,
                "SequentialFFmpegSpanDecoder",
                FakeSequentialDecoder,
            ),
        ):
            result = RUNNER.command_run(arguments)
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(
            result["reason"], "durable_controller_stop_observed_between_spans"
        )
        self.assertEqual(result["newly_completed_spans"], 1)
        self.assertEqual(result["completed_spans"], 1)
        self.assertEqual(result["remaining_spans"], len(plan["spans"]) - 1)
        self.assertFalse(arguments.bindings_output.exists())
        orders = RUNNER._work_orders(
            plan,
            self.output,
            profile=self.profile_for_control,
            initial_prompt=None,
            hotwords=[],
        )
        replayed = RUNNER._bindings(
            hashlib.sha256(self.plan_path.read_bytes()).hexdigest(), orders
        )
        self.assertEqual(replayed["spans"][0]["status"], "completed")
        self.assertTrue(
            all(row["status"] == "pending" for row in replayed["spans"][1:])
        )
        self.assertTrue(FakeSequentialDecoder.instances[0].finished)
        self.assertTrue(FakeSequentialDecoder.instances[0].closed)

    def test_ordinary_gpu_demand_after_one_span_returns_resumable_incomplete(
        self,
    ) -> None:
        source_body = self.audio.read_bytes()
        total_samples = 100_000
        manifest = {
            "kind": PLANNER.MANIFEST_KIND,
            "schema_version": 1,
            "recording": {
                "recording_id": "rec_yield_between_spans",
                "media_id": "media_yield_between_spans",
                "input": {
                    "artifact_id": "artifact_yield_between_spans",
                    "path": str(self.audio),
                    "sha256": hashlib.sha256(source_body).hexdigest(),
                    "byte_count": len(source_body),
                    "sample_rate_hz": 16_000,
                    "channels": 1,
                    "total_samples": total_samples,
                    "duration_ms": PLANNER.duration_ms_for_samples(total_samples),
                },
            },
            "boundary_candidates": [],
        }
        policy = {
            "kind": PLANNER.POLICY_KIND,
            "schema_version": 1,
            "direct_max_samples": 40_000,
            "adaptive": {
                "min_core_samples": 16_000,
                "target_core_samples": 32_000,
                "max_core_samples": 40_000,
                "boundary_search_samples": 4_000,
                "padding_samples": 8_000,
                "max_span_count": 100,
            },
        }
        plan = PLANNER.build_longform_asr_plan(manifest, policy)
        self.plan_path.write_text(PLANNER.canonical_json(plan), encoding="utf-8")
        arguments = self.arguments()
        arguments.bindings_output = self.root / "yielded-bindings.json"
        arguments.ffmpeg = Path("/fixture/ffmpeg")
        arguments.ffmpeg_sha256 = "0" * 64
        arguments.yield_to_ordinary_gpu = True
        control = {
            "config_sha256": self.controller_sha256,
            "profile": self.profile_for_control,
            "profile_path": self.profile_path,
            "profile_sha256": hashlib.sha256(self.profile_path.read_bytes()).hexdigest(),
            "runtime_sha256": self.runtime_sha256,
            "gpu_uuid": self.gpu_uuid,
            "lock_path": self.gpu_lock,
            "opportunity_lock_path": self.gpu_opportunity_lock,
            "model_root": self.root / "fake-model",
            "controller_config_id": "himrautocfg_" + "1" * 32,
            "controller_state_root": self.root / "controller-state",
        }
        engine = FakeEngine(
            model_revision=self.profile["model"]["revision"],
            model_identity_sha256=self.profile["model"]["identity_sha256"],
        )
        FakeSequentialDecoder.instances.clear()
        with (
            mock.patch.object(RUNNER, "_admitted_control", return_value=control),
            mock.patch.object(RUNNER, "_ensure_cuda_wheel_libraries"),
            mock.patch.object(RUNNER, "_verify_gpu_uuid"),
            mock.patch.object(RUNNER, "_model", return_value=engine),
            mock.patch.object(
                RUNNER, "_ordinary_gpu_demand", side_effect=[False, False, True]
            ) as ordinary_demand,
            mock.patch.object(
                RUNNER.E,
                "SequentialFFmpegSpanDecoder",
                FakeSequentialDecoder,
            ),
        ):
            result = RUNNER.command_run(arguments)

        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(
            result["reason"], "ordinary_gpu_work_observed_between_spans"
        )
        self.assertEqual(result["newly_completed_spans"], 1)
        self.assertEqual(result["completed_spans"], 1)
        self.assertEqual(result["remaining_spans"], len(plan["spans"]) - 1)
        self.assertEqual(result["model_load_count"], 1)
        self.assertEqual(len(engine.calls), 1)
        self.assertEqual(ordinary_demand.call_count, 3)
        self.assertFalse(arguments.bindings_output.exists())
        orders = RUNNER._work_orders(
            plan,
            self.output,
            profile=self.profile_for_control,
            initial_prompt=None,
            hotwords=[],
        )
        replayed = RUNNER._bindings(
            hashlib.sha256(self.plan_path.read_bytes()).hexdigest(), orders
        )
        self.assertEqual(replayed["spans"][0]["status"], "completed")
        self.assertTrue(
            all(row["status"] == "pending" for row in replayed["spans"][1:])
        )
        self.assertTrue(FakeSequentialDecoder.instances[0].finished)
        self.assertTrue(FakeSequentialDecoder.instances[0].closed)


class RuntimeToolMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.helper_path = Path(self.temporary.name) / "helper.py"
        self.helper_path.write_bytes(b"reviewed helper fixture\n")
        self.runtime = {
            "status": "candidate",
            "trusted_install": {"system_tools": [dict(RUNNER._HISTORICAL_BWRAP)]},
        }
        self.digest = hashlib.sha256(RUNNER.E.canonical_bytes(self.runtime)).hexdigest()
        self.helper = ModuleType("fixture_runtime_helper")
        self.helper.__file__ = str(self.helper_path)
        self.helper.RuntimeAdmissionV2Error = type("RuntimeErrorFixture", (RuntimeError,), {})
        self.current = dict(RUNNER._REVIEWED_BWRAP_SUCCESSOR)
        self.calls = []

        def tool_reference(value, label):
            self.calls.append(dict(value))
            if value["sha256"] != self.current["sha256"]:
                raise self.helper.RuntimeAdmissionV2Error(
                    f"{label} SHA-256 differs from its reference"
                )
            return dict(self.current)

        self.original = tool_reference
        self.helper._tool_reference = tool_reference

        def validate(value, **_kwargs):
            old = RUNNER._HISTORICAL_BWRAP
            result = self.helper._tool_reference(
                {key: old[key] for key in ("name", "path", "sha256")}, "system tool bubblewrap"
            )
            if result != value["trusted_install"]["system_tools"][0]:
                raise self.helper.RuntimeAdmissionV2Error("historical receipt differs")
            return value

        self.helper.validate_receipt = validate
        self.patches = [
            mock.patch.object(RUNNER, "REVIEWED_LONGFORM_RUNTIME_SHA256", self.digest),
            mock.patch.object(
                RUNNER, "REVIEWED_RUNTIME_HELPER_SHA256",
                hashlib.sha256(self.helper_path.read_bytes()).hexdigest(),
            ),
        ]
        for patch in self.patches:
            patch.start()

    def tearDown(self) -> None:
        for patch in reversed(self.patches):
            patch.stop()
        self.temporary.cleanup()

    def test_exact_reviewed_unused_tool_upgrade_preserves_receipt(self) -> None:
        before = RUNNER.E.canonical_bytes(self.runtime)
        observed, migrations = RUNNER._replay_longform_runtime(
            self.helper, self.runtime, self.digest
        )
        self.assertEqual(observed, self.runtime)
        self.assertEqual(before, RUNNER.E.canonical_bytes(self.runtime))
        self.assertEqual(len(migrations), 1)
        self.assertEqual(len(self.calls), 2)
        self.assertIs(self.helper._tool_reference, self.original)

    def test_unchanged_tool_needs_no_transition(self) -> None:
        self.current = dict(RUNNER._HISTORICAL_BWRAP)
        _, migrations = RUNNER._replay_longform_runtime(self.helper, self.runtime, self.digest)
        self.assertEqual(migrations, [])
        self.assertEqual(len(self.calls), 1)

    def test_unreviewed_tool_bytes_fail_and_restore_helper(self) -> None:
        self.current["sha256"] = "a" * 64
        with self.assertRaises(self.helper.RuntimeAdmissionV2Error):
            RUNNER._replay_longform_runtime(self.helper, self.runtime, self.digest)
        self.assertIs(self.helper._tool_reference, self.original)

    def test_wrong_successor_owner_fails_and_restores_helper(self) -> None:
        self.current["uid"] = 1000
        with self.assertRaisesRegex(RUNNER.RunnerError, "successor metadata differs"):
            RUNNER._replay_longform_runtime(self.helper, self.runtime, self.digest)
        self.assertIs(self.helper._tool_reference, self.original)

    def test_other_runtime_cannot_use_transition(self) -> None:
        with self.assertRaises(self.helper.RuntimeAdmissionV2Error):
            RUNNER._replay_longform_runtime(self.helper, self.runtime, "f" * 64)
        self.assertEqual(len(self.calls), 1)
        self.assertIs(self.helper._tool_reference, self.original)

    def test_changed_receipt_or_helper_cannot_use_transition(self) -> None:
        self.runtime["status"] = "admitted"
        with self.assertRaisesRegex(RUNNER.RunnerError, "binding differs"):
            RUNNER._replay_longform_runtime(self.helper, self.runtime, self.digest)
        self.runtime["status"] = "candidate"
        self.helper_path.write_bytes(b"unreviewed helper\n")
        with self.assertRaisesRegex(RUNNER.RunnerError, "binding differs"):
            RUNNER._replay_longform_runtime(self.helper, self.runtime, self.digest)
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
