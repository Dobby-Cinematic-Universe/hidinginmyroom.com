from __future__ import annotations

import contextlib
import copy
import hashlib
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BATCH = load_module(
    "himr_gpu_production_batch_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/production_asr_batch.py",
)


class GPUProductionBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name).resolve()
        self.paths = {}
        for name in (
            "runtime",
            "model",
            "output",
            "batch",
            "receipt",
            "inputs",
            "orders",
        ):
            path = root / name
            path.mkdir(mode=0o700)
            self.paths[name] = path
        self.device = root.stat().st_dev

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def work_order(
        self,
        ordinal: int,
        *,
        duration_ms: int = 60_000,
    ) -> dict[str, object]:
        input_body = f"input-{ordinal}".encode()
        input_path = self.paths["inputs"] / f"{ordinal}.flac"
        if input_path.exists():
            input_path.chmod(0o600)
        input_path.write_bytes(input_body)
        input_path.chmod(0o444)
        digest = hashlib.sha256(input_body).hexdigest()
        identity = hashlib.sha256(f"work-order-{ordinal}".encode()).hexdigest()
        runtime_receipt = self.paths["runtime"] / "receipt.json"
        runtime_receipt.touch(exist_ok=True)
        runtime_receipt.chmod(0o400)
        return {
            "kind": "himr_faster_whisper_gpu_work_order",
            "schema_version": 3,
            "implementation_version": "0.3.0",
            "job_id": f"batch-test-{ordinal}",
            "input": {
                "path": str(input_path),
                "expected_sha256": digest,
                "expected_byte_count": len(input_body),
                "expected_duration_ms": duration_ms,
                "sealed_mode": "0444",
                "media_id": f"media_{ordinal}",
                "artifact_id": f"artifact_{ordinal}",
                "parent_processing_run_id": f"run_{ordinal}",
                "timeline_offset_ms": 0,
                "media_format": {
                    "container": "flac",
                    "codec": "flac",
                    "sample_rate_hz": 16_000,
                    "channels": 1,
                    "sample_format": "s16",
                },
            },
            "model": {
                "manifest_path": str(self.paths["model"] / "manifest.json"),
                "expected_manifest_sha256": "1" * 64,
                "receipt_path": str(self.paths["model"] / "receipt.json"),
                "expected_receipt_sha256": "2" * 64,
                "snapshot_root": str(self.paths["model"]),
                "identity_sha256": "3" * 64,
                "repository": "Systran/faster-whisper-small.en",
                "revision": "d1d751a5f8271d482d14ca55d9e2deeebbae577f",
                "license": "mit",
            },
            "runtime": {
                "root": str(self.paths["runtime"]),
                "expected_device": self.device,
                "python": {"path": "/python", "expected_sha256": "4" * 64},
                "pyproject": {"path": "/pyproject", "expected_sha256": "5" * 64},
                "lock": {"path": "/lock", "expected_sha256": "6" * 64},
                "runtime_manifest": {
                    "path": str(runtime_receipt),
                    "expected_sha256": hashlib.sha256(runtime_receipt.read_bytes()).hexdigest(),
                },
                "ffprobe": {"path": "/ffprobe", "expected_sha256": "7" * 64},
                "adapter": {
                    "path": str(BATCH.V3_PATH),
                    "expected_sha256": BATCH._file_reference(
                        BATCH.V3_PATH, "test v3 source"
                    )["sha256"],
                },
                "packages": {
                    "av": "18.1.0",
                    "ctranslate2": "4.8.1",
                    "faster-whisper": "1.2.1",
                    "nvidia-cublas-cu12": "12.9.2.10",
                    "nvidia-cudnn-cu12": "9.24.0.43",
                    "nvidia-ml-py": "13.610.43",
                },
                "offline_policy": BATCH.BASE.OFFLINE_POLICY,
            },
            "gpu": {
                "expected_uuid": "GPU-0b9d7029-b3c6-1a0b-d483-6a3febc3b557",
                "device_index": 0,
                "compute_type": "float16",
                "lock_path": str(
                    self.paths["runtime"]
                    / "locks/GPU-0b9d7029-b3c6-1a0b-d483-6a3febc3b557.lock"
                ),
                "lock_policy": BATCH.BASE.GPU_LOCK_POLICY,
                "minimum_free_vram_bytes": 2 * 1024**3,
                "maximum_process_vram_bytes": 4 * 1024**3,
            },
            "inference": {
                "language": "en",
                "beam_size": 5,
                "best_of": 5,
                "temperature": 0.0,
                "condition_on_previous_text": False,
                "word_timestamps": True,
                "vad_filter": False,
                "cpu_threads": 4,
                "num_workers": 1,
                "max_audio_bytes": 16 * 1024 * 1024,
                "max_audio_seconds": 3600.0,
                "max_wall_seconds": 900.0,
                "max_result_bytes": 16 * 1024 * 1024,
                "max_segments": 4096,
                "max_words": 32768,
            },
            "catalog_context": None,
            "output": {"root": str(self.paths["output"])},
            "policy": dict(BATCH.BASE.POLICY),
            "identity_sha256": identity,
            "work_order_id": f"gpuasrwo_{identity[:32]}",
        }

    def records(self, count: int, *, duration_ms: int = 60_000) -> list[dict[str, object]]:
        records = []
        for ordinal in range(1, count + 1):
            order = self.work_order(ordinal, duration_ms=duration_ms)
            body = BATCH.canonical_bytes(order)
            records.append(
                {
                    "work_order": order,
                    "body": body,
                    "source_reference": {
                        "path": str(self.paths["orders"] / f"{ordinal}.json"),
                        "sha256": BATCH.sha256_bytes(body),
                        "byte_count": len(body),
                    },
                }
            )
        return records

    def manifest(self, records: list[dict[str, object]]) -> dict[str, object]:
        return BATCH._manifest_from_records(
            records=records,
            batch_root=self.paths["batch"],
            receipt_root=self.paths["receipt"],
            maximum_batch_wall_seconds=3600,
            software=BATCH.software_document(),
        )

    def run_harness(
        self,
        *,
        states: list[dict[str, object]],
        transcribe_side_effect: object,
    ) -> tuple[object, dict[str, object]]:
        records = self.records(len(states))
        manifest = self.manifest(records)
        members = []
        for entry, record in zip(manifest["items"], records, strict=True):
            order = record["work_order"]
            members.append(
                {
                    "entry": entry,
                    "work_order": order,
                    "work_order_file": {
                        "path": f"/work-order-{entry['ordinal']}",
                        "sha256": record["source_reference"]["sha256"],
                        "byte_count": len(record["body"]),
                        "identity_sha256": order["identity_sha256"],
                        "work_order_id": order["work_order_id"],
                    },
                }
            )
        common = {
            "runtime": {"binding": "runtime"},
            "model": {"binding": "model"},
            "batch_source_admission": {"all_batch_sources_bound": True},
        }
        events: list[str] = []
        lock_entries: list[int] = []
        model_initializations: list[tuple[tuple[object, ...], dict[str, object]]] = []
        sampler_instances: list[object] = []

        @contextlib.contextmanager
        def gpu_lock(_order: dict[str, object]):
            lock_entries.append(1)
            yield {"held": True}

        class FakeSampler:
            def __init__(self, *_args: object) -> None:
                self.started = 0
                self.waited = 0
                self.stopped = 0
                sampler_instances.append(self)

            def start(self) -> None:
                self.started += 1

            def wait_for_process_sample(self, _timeout: float) -> None:
                self.waited += 1

            def snapshot(self, **_kwargs: object) -> dict[str, object]:
                return {
                    "global_peak_used_bytes": 1024,
                    "process_peak_used_bytes": 512,
                    "process_vram_measurement_seen": True,
                    "process_sample_age_seconds": 0.01,
                    "sampler_error": None,
                }

            def stop(self) -> None:
                self.stopped += 1

        class FakeWhisperModel:
            def __init__(self, *args: object, **kwargs: object) -> None:
                model_initializations.append((args, kwargs))

        ctranslate2 = ModuleType("ctranslate2")
        ctranslate2.get_cuda_device_count = lambda: 1
        ctranslate2.get_supported_compute_types = lambda _device, _index: {"float16"}
        pynvml = ModuleType("pynvml")
        pynvml.nvmlInit = mock.Mock()
        pynvml.nvmlShutdown = mock.Mock()
        pynvml.nvmlDeviceGetHandleByIndex = lambda _index: object()
        pynvml.nvmlDeviceGetUUID = lambda _handle: manifest["common"]["gpu"]["expected_uuid"]
        pynvml.nvmlDeviceGetName = lambda _handle: "fixture GPU"
        pynvml.nvmlSystemGetDriverVersion = lambda: "fixture"
        pynvml.nvmlSystemGetCudaDriverVersion_v2 = lambda: 1
        pynvml.nvmlDeviceGetCudaComputeCapability = lambda _handle: (8, 6)
        pynvml.nvmlDeviceGetMemoryInfo = lambda _handle: SimpleNamespace(
            total=6 * 1024**3, free=5 * 1024**3, used=1024**3
        )
        faster_whisper = ModuleType("faster_whisper")
        faster_whisper.WhisperModel = FakeWhisperModel

        def completed_or_pending(order: dict[str, object], _plan: dict[str, object]):
            ordinal = int(str(order["job_id"]).rsplit("-", 1)[1])
            if states[ordinal - 1]["state"] == "completed":
                return "completed", {"result": "existing"}
            return "pending", None

        def replay_common(*_args: object, **_kwargs: object) -> dict[str, object]:
            events.append("common")
            return common

        publish = mock.Mock(return_value={"completed": True})
        with (
            mock.patch.object(BATCH.sys, "flags", SimpleNamespace(isolated=1)),
            mock.patch.dict(BATCH.os.environ, {"CUDA_VISIBLE_DEVICES": ""}, clear=False),
            mock.patch.dict(
                BATCH.sys.modules,
                {
                    "ctranslate2": ctranslate2,
                    "pynvml": pynvml,
                    "faster_whisper": faster_whisper,
                },
            ),
            mock.patch.object(BATCH.BASE, "ensure_offline_environment"),
            mock.patch.multiple(
                BATCH,
                _process_elapsed_seconds=mock.Mock(return_value=0.1),
                load_manifest=mock.Mock(return_value=(manifest, members)),
                validate_completion=mock.Mock(return_value=None),
                _member_states=mock.Mock(return_value=states),
                ensure_cuda_wheel_libraries=mock.Mock(),
            ),
            mock.patch.object(BATCH.BASE, "gpu_advisory_lock", gpu_lock),
            mock.patch.object(BATCH.BASE, "HardDeadline", return_value=contextlib.nullcontext()),
            mock.patch.object(
                BATCH.BASE,
                "network_isolation_evidence",
                side_effect=lambda _value: events.append("network") or {"verified": True},
            ),
            mock.patch.object(BATCH, "replay_common_bindings", side_effect=replay_common),
            mock.patch.object(BATCH, "BatchNVMLSampler", FakeSampler),
            mock.patch.object(
                BATCH.BASE, "completed_or_pending", side_effect=completed_or_pending
            ),
            mock.patch.object(BATCH, "_transcribe_one", side_effect=transcribe_side_effect) as transcribe,
            mock.patch.object(
                BATCH,
                "_result_reference",
                side_effect=lambda member, _result, disposition: {
                    "ordinal": member["entry"]["ordinal"],
                    "disposition": disposition,
                },
            ),
            mock.patch.object(BATCH, "_completed_results", return_value=[]),
            mock.patch.object(BATCH, "_batch_manifest_reference", return_value={}),
            mock.patch.object(BATCH, "_publish_completion", publish),
        ):
            try:
                result: object = BATCH.run_batch(Path("/manifest"), "net:[1]")
            except BATCH.BatchRunFailure as error:
                result = error
        return result, {
            "events": events,
            "lock_entries": lock_entries,
            "model_initializations": model_initializations,
            "samplers": sampler_instances,
            "transcribe": transcribe,
            "publish": publish,
        }

    def test_contract_is_finite_sequential_v3_and_private(self) -> None:
        contract = BATCH.contract_document()
        descriptor = contract["descriptor"]
        self.assertEqual(descriptor["maximum_items"], 32)
        self.assertEqual(descriptor["maximum_total_audio_ms"], 43_200_000)
        self.assertEqual(descriptor["execution"]["model_loads"], 1)
        self.assertEqual(
            descriptor["execution"]["dispatch"],
            "sequential_unchanged_whispermodel_transcribe",
        )
        self.assertFalse(descriptor["policy"]["network_access"])
        self.assertFalse(descriptor["policy"]["neural_batching"])
        self.assertEqual(
            contract["identity_sha256"],
            BATCH.sha256_bytes(BATCH.canonical_bytes(descriptor)),
        )

    def test_cuda_library_reexec_targets_batch_source(self) -> None:
        purelib = Path(self.temporary.name) / "purelib"
        for relative in ("nvidia/cublas/lib", "nvidia/cudnn/lib"):
            (purelib / relative).mkdir(parents=True)
        with (
            mock.patch.object(
                BATCH.sysconfig, "get_paths", return_value={"purelib": str(purelib)}
            ),
            mock.patch.object(BATCH.sys, "argv", [str(BATCH.SOURCE_PATH), "run", "--x"]),
            mock.patch.dict(BATCH.os.environ, {"LD_LIBRARY_PATH": ""}, clear=False),
            mock.patch.object(BATCH.os, "execve", side_effect=RuntimeError("exec")) as execute,
        ):
            with self.assertRaisesRegex(RuntimeError, "exec"):
                BATCH.ensure_cuda_wheel_libraries()
        executable, argv, environment = execute.call_args.args
        self.assertEqual(executable, BATCH.sys.executable)
        self.assertEqual(argv[:4], [BATCH.sys.executable, "-B", "-I", str(BATCH.SOURCE_PATH)])
        self.assertEqual(argv[4:], ["run", "--x"])
        self.assertIn(str(purelib / "nvidia/cublas/lib"), environment["LD_LIBRARY_PATH"])
        self.assertEqual(environment["HF_HUB_OFFLINE"], "1")

    def test_frozen_runtime_path_never_calls_live_hardware_replay(self) -> None:
        order = self.work_order(1)
        software = {
            "batch_worker": {"path": "/worker", "sha256": "1" * 64, "byte_count": 1}
        }
        receipt = {
            "evidence": {
                "bindings": {
                    "sources": [{"requested_path": "/worker", "sha256": "1" * 64}],
                    "executables": [],
                }
            }
        }
        receipt_path = Path(order["runtime"]["runtime_manifest"]["path"])
        receipt_path.chmod(0o600)
        body = json.dumps(receipt, sort_keys=True).encode()
        receipt_path.write_bytes(body)
        receipt_path.chmod(0o400)
        order["runtime"]["runtime_manifest"]["expected_sha256"] = BATCH.sha256_bytes(body)
        frozen = {"admission": {"receipt_id": "receipt", "identity_sha256": "2" * 64}}
        with (
            mock.patch.object(
                BATCH, "_replay_frozen_runtime_binding", return_value=frozen
            ) as replay_frozen,
            mock.patch.object(
                BATCH.BASE,
                "replay_runtime_binding",
                side_effect=AssertionError("live hardware replay was called"),
            ),
        ):
            observed, admission = BATCH._runtime_source_binding(
                order,
                software,
                require_current=False,
                live_hardware=False,
            )
        self.assertEqual(observed, frozen)
        replay_frozen.assert_called_once_with(order, require_current=False)
        self.assertTrue(admission["all_batch_sources_bound"])

    def test_sampler_rejects_error_stale_and_over_limit_snapshots(self) -> None:
        fake_nvml = SimpleNamespace(NVMLError=RuntimeError)
        sampler = BATCH.BatchNVMLSampler(fake_nvml, object(), 100)
        sampler.thread = mock.Mock()
        sampler.thread.is_alive.return_value = True
        sampler.process_measurement_seen = True
        sampler.last_process_sample_monotonic = BATCH.time.monotonic() - 2
        sampler.process_peak_bytes = 50
        with self.assertRaisesRegex(BATCH.BatchError, "stale"):
            sampler.snapshot(maximum_age_seconds=0.5)
        sampler.last_process_sample_monotonic = BATCH.time.monotonic()
        sampler.sample_error = "NVML unavailable"
        with self.assertRaisesRegex(BATCH.BatchError, "failed closed"):
            sampler.snapshot()
        sampler.sample_error = None
        sampler.process_peak_bytes = 101
        with self.assertRaisesRegex(BATCH.BatchError, "VRAM"):
            sampler.snapshot()

    def test_run_holds_one_lock_loads_one_model_and_resumes_in_order(self) -> None:
        states = [
            {"ordinal": 1, "state": "completed"},
            {"ordinal": 2, "state": "pending"},
            {"ordinal": 3, "state": "pending"},
        ]
        result, evidence = self.run_harness(
            states=states,
            transcribe_side_effect=lambda **kwargs: (
                {
                    "result": kwargs["member"]["entry"]["ordinal"],
                },
                1.0,
            ),
        )
        self.assertEqual(result, {"completed": True})
        self.assertEqual(len(evidence["lock_entries"]), 1)
        self.assertEqual(len(evidence["model_initializations"]), 1)
        self.assertEqual(
            [call.kwargs["member"]["entry"]["ordinal"] for call in evidence["transcribe"].call_args_list],
            [2, 3],
        )
        self.assertEqual(evidence["events"][:2], ["network", "common"])
        sampler = evidence["samplers"][0]
        self.assertEqual((sampler.started, sampler.waited, sampler.stopped), (1, 1, 1))
        core = evidence["publish"].call_args.args[2]
        self.assertEqual(core["execution"]["model_load_count"], 1)
        self.assertEqual(core["execution"]["inference_item_count"], 2)
        self.assertEqual(core["execution"]["reused_item_count"], 1)

    def test_run_fail_stops_without_attempting_later_member(self) -> None:
        attempted: list[int] = []

        def transcribe(**kwargs: object):
            ordinal = kwargs["member"]["entry"]["ordinal"]
            attempted.append(ordinal)
            if ordinal == 2:
                raise BATCH.BatchError("fixture item failure")
            return {"result": ordinal}, 1.0

        result, evidence = self.run_harness(
            states=[
                {"ordinal": 1, "state": "pending"},
                {"ordinal": 2, "state": "pending"},
                {"ordinal": 3, "state": "pending"},
            ],
            transcribe_side_effect=transcribe,
        )
        self.assertIsInstance(result, BATCH.BatchRunFailure)
        self.assertEqual(attempted, [1, 2])
        self.assertEqual(result.document["failed_item"]["ordinal"], 2)
        self.assertEqual(len(result.document["completed_in_this_attempt"]), 1)
        self.assertTrue(result.document["files_published"])
        self.assertFalse(evidence["publish"].called)

    def test_publish_then_replay_failure_is_reported_as_published(self) -> None:
        result, evidence = self.run_harness(
            states=[
                {"ordinal": 1, "state": "pending"},
                {"ordinal": 2, "state": "pending"},
            ],
            transcribe_side_effect=BATCH.MemberResultFailure(
                "fixture replay failure", files_published=True
            ),
        )
        self.assertIsInstance(result, BATCH.BatchRunFailure)
        self.assertEqual(result.document["failed_item"]["ordinal"], 1)
        self.assertTrue(result.document["files_published"])
        self.assertEqual(evidence["transcribe"].call_count, 1)
        self.assertFalse(evidence["publish"].called)

    def test_all_completed_recovery_uses_frozen_runtime_without_cuda(self) -> None:
        records = self.records(2)
        manifest = self.manifest(records)
        members = [
            {
                "entry": entry,
                "work_order": record["work_order"],
                "work_order_file": {},
            }
            for entry, record in zip(manifest["items"], records, strict=True)
        ]
        states = [
            {"ordinal": ordinal, "state": "completed"}
            for ordinal in (1, 2)
        ]
        common = {"runtime": {}, "model": {}, "batch_source_admission": {}}
        with (
            mock.patch.object(BATCH.sys, "flags", SimpleNamespace(isolated=1)),
            mock.patch.dict(BATCH.os.environ, {"CUDA_VISIBLE_DEVICES": ""}, clear=False),
            mock.patch.object(BATCH.BASE, "ensure_offline_environment"),
            mock.patch.object(BATCH, "_process_elapsed_seconds", return_value=0.1),
            mock.patch.object(BATCH, "load_manifest", return_value=(manifest, members)),
            mock.patch.object(BATCH, "validate_completion", return_value=None),
            mock.patch.object(BATCH, "_member_states", return_value=states),
            mock.patch.object(
                BATCH.BASE, "HardDeadline", return_value=contextlib.nullcontext()
            ),
            mock.patch.object(BATCH.BASE, "network_isolation_evidence"),
            mock.patch.object(BATCH, "replay_common_bindings", return_value=common) as replay,
            mock.patch.object(
                BATCH,
                "ensure_cuda_wheel_libraries",
                side_effect=AssertionError("CUDA setup must not run"),
            ),
            mock.patch.object(
                BATCH, "_completion_without_cuda", return_value={"completed": True}
            ) as complete,
        ):
            observed = BATCH.run_batch(Path("/manifest"), "net:[1]")
        self.assertEqual(observed, {"completed": True})
        self.assertFalse(replay.call_args.kwargs["live_hardware"])
        self.assertTrue(replay.call_args.kwargs["require_current"])
        complete.assert_called_once_with(Path("/manifest"), manifest, members, common)

    def test_batch_wall_budget_includes_process_startup(self) -> None:
        records = self.records(1)
        manifest = self.manifest(records)
        members = [{"entry": manifest["items"][0], "work_order": records[0]["work_order"]}]
        with (
            mock.patch.object(BATCH.sys, "flags", SimpleNamespace(isolated=1)),
            mock.patch.dict(BATCH.os.environ, {"CUDA_VISIBLE_DEVICES": ""}, clear=False),
            mock.patch.object(BATCH.BASE, "ensure_offline_environment"),
            mock.patch.object(BATCH, "load_manifest", return_value=(manifest, members)),
            mock.patch.object(
                BATCH,
                "_process_elapsed_seconds",
                return_value=manifest["limits"]["maximum_batch_wall_seconds"] + 1,
            ),
            mock.patch.object(BATCH, "validate_completion") as completion,
            mock.patch.object(
                BATCH,
                "ensure_cuda_wheel_libraries",
                side_effect=AssertionError("CUDA setup must not run"),
            ),
        ):
            with self.assertRaisesRegex(BATCH.BatchError, "expired before execution"):
                BATCH.run_batch(Path("/manifest"), "net:[1]")
        completion.assert_not_called()

    def test_completion_execution_rejects_bad_types_times_and_member_vram(self) -> None:
        records = self.records(1)
        manifest = self.manifest(records)
        manifest["common"]["gpu"]["maximum_process_vram_bytes"] = 100
        members = [{"work_order": records[0]["work_order"]}]
        execution = {
            "batch_run_id": "run_gpu_asr_batch_fixture",
            "started_at": "2026-08-29T00:00:00Z",
            "completed_at": "2026-08-29T00:00:01Z",
            "duration_ms": 1000,
            "model_loaded": True,
            "model_load_count": 1,
            "model_load_seconds": 0.1,
            "inference_item_count": 1,
            "reused_item_count": 0,
            "inference_seconds": 0.5,
            "resume_completion_only": False,
        }
        hardware = {
            "device_index": 0,
            "name": "fixture GPU",
            "uuid": manifest["common"]["gpu"]["expected_uuid"],
            "driver_version": "fixture",
            "cuda_driver_version": 1,
            "compute_capability": [8, 6],
            "ctranslate2_cuda_device_count": 1,
            "ctranslate2_supported_compute_types": ["float16"],
            "memory_before": {
                "total_bytes": 1000,
                "free_bytes": 600,
                "used_bytes": 400,
            },
            "memory_after": {
                "total_bytes": 1000,
                "free_bytes": 600,
                "used_bytes": 400,
            },
            "global_peak_used_bytes": 500,
            "process_peak_used_bytes": 100,
            "process_vram_measurement_seen": True,
            "process_sample_age_seconds": 0.1,
            "sampler_error": None,
        }
        observed, inferred, reused = BATCH._validate_completion_execution(
            execution, hardware, manifest, members
        )
        self.assertEqual((observed, inferred, reused), (execution, 1, 0))
        mutations = (
            ("execution", "duration_ms", True),
            ("execution", "model_load_seconds", float("nan")),
            ("execution", "started_at", "2026-08-29T00:00:00+00:00"),
            ("execution", "completed_at", "2026-08-28T23:59:59Z"),
            ("hardware", "process_peak_used_bytes", 101),
            ("hardware", "process_sample_age_seconds", 0.6),
        )
        for target, key, value in mutations:
            with self.subTest(target=target, key=key):
                changed_execution = copy.deepcopy(execution)
                changed_hardware = copy.deepcopy(hardware)
                (changed_execution if target == "execution" else changed_hardware)[
                    key
                ] = value
                with self.assertRaises(BATCH.BatchError):
                    BATCH._validate_completion_execution(
                        changed_execution, changed_hardware, manifest, members
                    )

    def test_manifest_binds_ordered_members_and_replays_identity(self) -> None:
        manifest = self.manifest(self.records(2))
        self.assertEqual(manifest["totals"]["item_count"], 2)
        self.assertEqual(manifest["totals"]["audio_duration_ms"], 120_000)
        self.assertEqual(
            [item["ordinal"] for item in manifest["items"]], [1, 2]
        )
        self.assertEqual(manifest["limits"]["maximum_items"], 32)
        identity_core = {
            key: value
            for key, value in manifest.items()
            if key not in {"identity_sha256", "batch_id", "batch_relative_path"}
        }
        expected = BATCH.sha256_bytes(BATCH.canonical_bytes(identity_core))
        self.assertEqual(manifest["identity_sha256"], expected)
        self.assertEqual(manifest["batch_id"], f"gpuasrbatch_{expected[:32]}")

    def test_materializer_seals_exact_copies_and_replays(self) -> None:
        source_paths = []
        for ordinal, record in enumerate(self.records(2), start=1):
            path = self.paths["orders"] / f"{ordinal}.json"
            path.write_bytes(record["body"])
            path.chmod(0o400)
            source_paths.append(path)

        def fake_load(path_value: str) -> tuple[dict[str, object], dict[str, object]]:
            path = Path(path_value)
            body = path.read_bytes()
            order = json.loads(body)
            return order, {
                "path": str(path),
                "sha256": BATCH.sha256_bytes(body),
                "byte_count": len(body),
                "identity_sha256": order["identity_sha256"],
                "work_order_id": order["work_order_id"],
            }

        with mock.patch.object(BATCH.BASE, "load_work_order", side_effect=fake_load):
            manifest, path = BATCH.materialize_batch(
                work_order_paths=source_paths,
                batch_root=self.paths["batch"],
                receipt_root=self.paths["receipt"],
                maximum_batch_wall_seconds=3600,
            )
            replayed, members = BATCH.load_manifest(path, replay_runtime=False)
            self.assertEqual(replayed, manifest)
            self.assertEqual(len(members), 2)
            self.assertEqual(path.stat().st_mode & 0o777, 0o400)
            self.assertEqual(path.parent.stat().st_mode & 0o777, 0o500)
            copied = path.parent / "work-orders/000001.json"
            self.assertEqual(copied.read_bytes(), source_paths[0].read_bytes())
            copied.chmod(0o600)
            with self.assertRaises(BATCH.BatchError):
                BATCH.load_manifest(path, replay_runtime=False)

    def test_completion_replay_rejects_schema_and_ancestry_drift(self) -> None:
        records = self.records(1)
        manifest = self.manifest(records)
        order = records[0]["work_order"]
        member = {
            "entry": manifest["items"][0],
            "work_order": order,
            "work_order_file": {},
        }
        manifest_dir = (
            self.paths["batch"] / "batches" / manifest["batch_id"]
        )
        manifest_dir.mkdir(parents=True)
        manifest_path = manifest_dir / "manifest.json"
        manifest_body = BATCH.canonical_bytes(manifest)
        manifest_path.write_bytes(manifest_body)
        manifest_path.chmod(0o400)
        manifest_dir.chmod(0o500)
        result_reference = {
            "ordinal": 1,
            "job_id": order["job_id"],
            "disposition": "reused",
            "result_key": manifest["items"][0]["result_key"],
            "result_path": manifest["items"][0]["result_path"],
            "result_sha256": "1" * 64,
            "result_byte_count": 1,
            "result_identity_sha256": "2" * 64,
            "result_id": "gpuasrresult_fixture",
            "processing_run_id": "run_fixture",
        }
        common = {
            "runtime": {},
            "model": {},
            "batch_source_admission": {},
        }
        core = {
            "kind": BATCH.COMPLETION_KIND,
            "schema_version": BATCH.SCHEMA_VERSION,
            "implementation_version": BATCH.IMPLEMENTATION_VERSION,
            "status": "completed",
            "batch_id": manifest["batch_id"],
            "manifest": {
                "path": str(manifest_path),
                "sha256": BATCH.sha256_bytes(manifest_body),
                "byte_count": len(manifest_body),
                "identity_sha256": manifest["identity_sha256"],
            },
            "execution": {
                "batch_run_id": "run_gpu_asr_batch_fixture",
                "started_at": "2026-08-29T00:00:00Z",
                "completed_at": "2026-08-29T00:00:01Z",
                "duration_ms": 0,
                "model_loaded": False,
                "model_load_count": 0,
                "model_load_seconds": 0.0,
                "inference_item_count": 0,
                "reused_item_count": 1,
                "inference_seconds": 0.0,
                "resume_completion_only": True,
            },
            "common_bindings": common,
            "hardware": None,
            "results": [result_reference],
            "safety": BATCH.SAFETY_POLICY,
        }
        identity = BATCH.sha256_bytes(BATCH.canonical_bytes(core))
        receipt = {
            **core,
            "identity_sha256": identity,
            "completion_id": f"gpuasrbatchdone_{identity[:32]}",
        }
        completion_dir = (
            self.paths["receipt"]
            / "batches"
            / manifest["batch_id"]
            / "completion"
        )
        completion_dir.mkdir(parents=True)
        (self.paths["receipt"] / "batches").chmod(0o700)
        completion_dir.parent.chmod(0o700)
        completion_path = completion_dir / "receipt.json"
        completion_path.write_bytes(BATCH.canonical_bytes(receipt))
        completion_path.chmod(0o400)
        completion_dir.chmod(0o500)
        with (
            mock.patch.object(
                BATCH, "replay_common_bindings", return_value=common
            ),
            mock.patch.object(BATCH.V3, "validate_completed_result", return_value={}),
            mock.patch.object(BATCH, "_result_reference", return_value=result_reference),
        ):
            self.assertEqual(BATCH.validate_completion(manifest, [member]), receipt)
            completion_dir.parent.chmod(0o755)
            with self.assertRaisesRegex(BATCH.BatchError, "ownership/mode/device"):
                BATCH.validate_completion(manifest, [member])
            completion_dir.parent.chmod(0o700)
            receipt["schema_version"] = 2
            bad_core = {
                key: value
                for key, value in receipt.items()
                if key not in {"identity_sha256", "completion_id"}
            }
            bad_identity = BATCH.sha256_bytes(BATCH.canonical_bytes(bad_core))
            receipt["identity_sha256"] = bad_identity
            receipt["completion_id"] = f"gpuasrbatchdone_{bad_identity[:32]}"
            completion_path.chmod(0o600)
            completion_path.write_bytes(BATCH.canonical_bytes(receipt))
            completion_path.chmod(0o400)
            with self.assertRaisesRegex(BATCH.BatchError, "identity/header/policy"):
                BATCH.validate_completion(manifest, [member])

    def test_manifest_rejects_any_common_profile_drift(self) -> None:
        mutations = (
            ("model", "identity_sha256", "9" * 64),
            ("runtime", "expected_device", self.device + 1),
            ("gpu", "compute_type", "int8"),
            ("inference", "beam_size", 4),
            ("output", "root", str(self.paths["orders"])),
        )
        for section, key, value in mutations:
            with self.subTest(section=section, key=key):
                records = self.records(2)
                records[1]["work_order"][section][key] = value
                records[1]["body"] = BATCH.canonical_bytes(records[1]["work_order"])
                records[1]["source_reference"]["sha256"] = BATCH.sha256_bytes(
                    records[1]["body"]
                )
                records[1]["source_reference"]["byte_count"] = len(records[1]["body"])
                with self.assertRaises(BATCH.BatchError):
                    self.manifest(records)

    def test_manifest_enforces_item_duration_worker_vram_and_dedupe_caps(self) -> None:
        with self.assertRaises(BATCH.BatchError):
            self.manifest(self.records(33))
        with self.assertRaises(BATCH.BatchError):
            self.manifest(self.records(2, duration_ms=21_600_001))

        workers = self.records(1)
        workers[0]["work_order"]["inference"]["num_workers"] = 2
        workers[0]["body"] = BATCH.canonical_bytes(workers[0]["work_order"])
        workers[0]["source_reference"]["sha256"] = BATCH.sha256_bytes(workers[0]["body"])
        workers[0]["source_reference"]["byte_count"] = len(workers[0]["body"])
        with self.assertRaises(BATCH.BatchError):
            self.manifest(workers)

        vram = self.records(1)
        vram[0]["work_order"]["gpu"]["maximum_process_vram_bytes"] += 1
        vram[0]["body"] = BATCH.canonical_bytes(vram[0]["work_order"])
        vram[0]["source_reference"]["sha256"] = BATCH.sha256_bytes(vram[0]["body"])
        vram[0]["source_reference"]["byte_count"] = len(vram[0]["body"])
        with self.assertRaises(BATCH.BatchError):
            self.manifest(vram)

        duplicate = self.records(2)
        duplicate[1]["work_order"]["input"]["expected_sha256"] = duplicate[0][
            "work_order"
        ]["input"]["expected_sha256"]
        duplicate[1]["body"] = BATCH.canonical_bytes(duplicate[1]["work_order"])
        duplicate[1]["source_reference"]["sha256"] = BATCH.sha256_bytes(
            duplicate[1]["body"]
        )
        duplicate[1]["source_reference"]["byte_count"] = len(duplicate[1]["body"])
        with self.assertRaises(BATCH.BatchError):
            self.manifest(duplicate)

    def test_runtime_receipt_must_bind_every_batch_software_source(self) -> None:
        order = self.work_order(1)
        software = {
            "batch_worker": {"path": "/worker", "sha256": "1" * 64, "byte_count": 1},
            "batch_wrapper": {"path": "/wrapper", "sha256": "2" * 64, "byte_count": 1},
            "v3_adapter": {"path": "/v3", "sha256": "3" * 64, "byte_count": 1},
            "v2_preserved": {"path": "/v2", "sha256": "4" * 64, "byte_count": 1},
            "v1_preserved": {"path": "/v1", "sha256": "5" * 64, "byte_count": 1},
        }
        receipt = {
            "evidence": {
                "bindings": {
                    "sources": [
                        {"requested_path": item["path"], "sha256": item["sha256"]}
                        for name, item in software.items()
                        if name != "batch_wrapper"
                    ],
                    "executables": [
                        {
                            "requested_path": software["batch_wrapper"]["path"],
                            "sha256": software["batch_wrapper"]["sha256"],
                        }
                    ],
                }
            }
        }
        receipt_path = Path(order["runtime"]["runtime_manifest"]["path"])
        receipt_path.chmod(0o600)
        body = json.dumps(receipt, sort_keys=True).encode()
        receipt_path.write_bytes(body)
        receipt_path.chmod(0o400)
        order["runtime"]["runtime_manifest"]["expected_sha256"] = BATCH.sha256_bytes(body)
        runtime_replay = {
            "admission": {"receipt_id": "receipt", "identity_sha256": "6" * 64}
        }
        with mock.patch.object(
            BATCH.BASE, "replay_runtime_binding", return_value=runtime_replay
        ):
            observed, admission = BATCH._runtime_source_binding(
                order,
                software,
                require_current=False,
                live_hardware=True,
            )
            self.assertEqual(observed, runtime_replay)
            self.assertTrue(admission["all_batch_sources_bound"])
            receipt["evidence"]["bindings"]["sources"].pop()
            receipt_path.chmod(0o600)
            body = json.dumps(receipt, sort_keys=True).encode()
            receipt_path.write_bytes(body)
            receipt_path.chmod(0o400)
            order["runtime"]["runtime_manifest"]["expected_sha256"] = BATCH.sha256_bytes(body)
            with self.assertRaises(BATCH.BatchError):
                BATCH._runtime_source_binding(
                    order,
                    software,
                    require_current=False,
                    live_hardware=True,
                )

    def test_one_item_uses_unchanged_transcribe_call_and_replays_before_return(self) -> None:
        order = self.work_order(1)
        member = {
            "entry": {"ordinal": 1},
            "work_order": order,
            "work_order_file": {
                "path": "/order",
                "sha256": "1" * 64,
                "byte_count": 1,
                "identity_sha256": order["identity_sha256"],
                "work_order_id": order["work_order_id"],
            },
        }
        segment = SimpleNamespace(
            id=0,
            seek=0,
            start=0.0,
            end=0.5,
            text="fixture",
            tokens=[1],
            temperature=0.0,
            avg_logprob=-0.1,
            compression_ratio=1.0,
            no_speech_prob=0.1,
            words=[SimpleNamespace(start=0.0, end=0.5, word="fixture", probability=0.9)],
        )
        info = SimpleNamespace(
            language="en",
            language_probability=1.0,
            all_language_probs=None,
            duration=60.0,
            duration_after_vad=60.0,
        )
        model = mock.Mock()
        model.transcribe.return_value = (iter([segment]), info)

        @contextlib.contextmanager
        def retained(_input: dict[str, object]):
            yield 9, "/proc/self/fd/9", {
                "path": order["input"]["path"],
                "sha256": order["input"]["expected_sha256"],
                "byte_count": order["input"]["expected_byte_count"],
                "device": self.device,
                "inode": 1,
                "mode": 0o444,
                "link_count": 1,
            }
        completed = {
            "result_path": BATCH.V3.result_plan(order)["result_path"],
            "result_key": BATCH.V3.result_plan(order)["result_key"],
            "identity_sha256": "8" * 64,
            "result_id": "gpuasrresult_test",
            "processing_run": {"processing_run_id": "run_test"},
        }
        with (
            mock.patch.object(BATCH.BASE, "completed_or_pending", return_value=("pending", None)),
            mock.patch.object(BATCH.BASE, "HardDeadline", return_value=contextlib.nullcontext()),
            mock.patch.object(BATCH.BASE, "retained_verified_input", retained),
            mock.patch.object(
                BATCH.BASE,
                "probe_audio",
                return_value=(
                    {
                        "container": "flac",
                        "codec": "flac",
                        "sample_rate_hz": 16_000,
                        "channels": 1,
                        "sample_format": "s16",
                        "duration_ms": 60_000,
                    },
                    ["ffprobe"],
                ),
            ),
            mock.patch.object(BATCH.BASE, "build_result", return_value=completed),
            mock.patch.object(BATCH.BASE, "publish_result", return_value=completed) as publish,
            mock.patch.object(BATCH.V3, "validate_completed_result", return_value=completed) as replay,
        ):
            result, _ = BATCH._transcribe_one(
                model=model,
                member=member,
                common={"model": {}, "runtime": {}},
                hardware_provider=lambda: {
                    "process_vram_measurement_seen": True,
                    "process_peak_used_bytes": 1,
                },
                lock_evidence={"held": True},
                network={"verified": True},
                model_load_seconds=0.5,
                batch_provenance={"batch_id": "batch"},
            )
        self.assertEqual(result, completed)
        model.transcribe.assert_called_once_with(
            "/proc/self/fd/9",
            task="transcribe",
            language="en",
            beam_size=5,
            best_of=5,
            temperature=0.0,
            word_timestamps=True,
            vad_filter=False,
            condition_on_previous_text=False,
            without_timestamps=False,
        )
        publish.assert_called_once()
        replay.assert_called_once()


if __name__ == "__main__":
    unittest.main()
