"""Synthetic isolation/RPC tests. No models, media, downloads or inference."""
import base64
import errno
import fcntl
import hashlib
import multiprocessing
import os
from pathlib import Path
import resource
import signal
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

from pipeline import speaker_screen_accelerated_worker as worker


class SyntheticEngine:
    def __init__(self, models, **execution):
        self.mode = models["ecapa_embedding"]["sha256"][-1]
        self.calls = 0

    def initialize(self):
        if self.mode == "1":
            time.sleep(10)
        if "SCREEN_TEST_SECRET" in os.environ or os.environ.get("CUDA_VISIBLE_DEVICES") != "":
            raise AssertionError("environment not scrubbed")
        if resource.getrlimit(resource.RLIMIT_AS) != (worker.HOST_MEMORY_MAX, worker.HOST_MEMORY_MAX):
            raise AssertionError("CPU address-space bound missing")
        if os.getpriority(os.PRIO_PROCESS, 0) < 10:
            raise AssertionError("CPU niceness missing")
        for family in (socket.AF_INET, socket.AF_INET6):
            try:
                connection = socket.socket(family, socket.SOCK_STREAM)
            except OSError as error:
                if error.errno != errno.EPERM:
                    raise
            else:
                connection.close()
                raise AssertionError("network socket was not denied")
        return self.provenance()

    def provenance(self):
        return {"test_engine": "synthetic", "generation": self.calls if self.mode == "3" else 0}

    def analyze_batch(self, items):
        self.calls += 1
        if self.mode == "2":
            time.sleep(10)
        if self.mode == "4":
            return []
        if self.mode == "5":
            raise RuntimeError("PRIVATE_VECTOR [1.0, 2.0]")
        return [{**item["window"], "speech_ms": 0, "embedding": None} for item in items]


def synthetic_child(*args):
    # These eight tiny files are source-binding fixtures, never actual code.
    worker.PIPELINE_DIRECTORY = Path(args[1]["silero_vad"]["path"]).parent / "implementation"
    with mock.patch("pipeline.speaker_screen_accelerated_engine.ResidentScreenEngine", SyntheticEngine):
        worker._model_child(*args)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="resident-worker-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.impl_root = self.root / "implementation"
        self.impl_root.mkdir(mode=0o700)
        self.implementation = {}
        for name in worker.IMPLEMENTATION_FILES:
            body = f"# synthetic source binding: {name}\n".encode()
            (self.impl_root / name).write_bytes(body)
            (self.impl_root / name).chmod(0o600)
            self.implementation[name] = hashlib.sha256(body).hexdigest()
        self.models = {"kind": "himr_speaker_screen_models", "schema_version": 1,
                       "silero_vad": {"path": str(self.root / "vad.onnx"), "sha256": "1" * 64},
                       "ecapa_embedding": {"path": str(self.root / "ecapa.ckpt"), "sha256": "0" * 64}}
        self.execution = {"device": "cpu", "threads": 1, "batch_size": 8, "decode_prefetch": 1,
                          "max_run_seconds": 30, "cuda_memory_fraction": 0.5,
                          "gpu_uuid": None, "host_memory_max_bytes": worker.HOST_MEMORY_MAX}
        self.patch = mock.patch.object(worker, "PIPELINE_DIRECTORY", self.impl_root)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.context = multiprocessing.get_context("spawn")

    def item(self, index=0, duration=10000):
        return {"pcm": bytes(duration * 32), "window": {"index": index, "start_ms": 0, "end_ms": duration}}

    def resident(self, mode="0", lock_fd=None):
        self.models["ecapa_embedding"]["sha256"] = "0" * 63 + mode
        def process(**kwargs):
            kwargs["target"] = synthetic_child
            return self.context.Process(**kwargs)
        with mock.patch.object(worker.multiprocessing, "get_context", return_value=mock.Mock(Process=process)):
            result = worker.ResidentModelProcess(self.models, self.execution, self.implementation,
                                                  max_run_seconds=30, lock_fd=lock_fd)
        self.addCleanup(result.close)
        return result

    def test_fixed_source_set_hashes_and_safe_files(self):
        worker.verify_implementation(self.implementation)
        for bad in ({}, {**self.implementation, "../escape.py": "0" * 64},
                    {**self.implementation, "speaker_screen.py": "F" * 64}):
            with self.subTest(bad=bad), self.assertRaises(worker.ScreenError):
                worker.verify_implementation(bad)
        path = self.impl_root / "speaker_screen.py"
        path.write_bytes(b"changed")
        with self.assertRaisesRegex(worker.ScreenError, "changed"):
            worker.verify_implementation(self.implementation)
        path.unlink()
        path.symlink_to(self.impl_root / "speaker_screen_core.py")
        with self.assertRaises(OSError):
            worker.verify_implementation(self.implementation)

    def test_execution_exact_bounds_and_cpu_gpu_separation(self):
        self.assertEqual(worker.validate_execution(self.execution), self.execution)
        invalid = [("threads", True), ("threads", 3), ("batch_size", 17), ("decode_prefetch", 0),
                   ("max_run_seconds", 9), ("host_memory_max_bytes", 1),
                   ("cuda_memory_fraction", float("nan")), ("cuda_memory_fraction", True),
                   ("cuda_memory_fraction", 0.76), ("device", "auto"), ("gpu_uuid", "GPU-bad")]
        for key, value in invalid:
            with self.subTest(key=key, value=value), self.assertRaises(worker.ScreenError):
                worker.validate_execution({**self.execution, key: value})
        with self.assertRaises(worker.ScreenError):
            worker.validate_execution({**self.execution, "extra": 1})
        cuda = {**self.execution, "device": "cuda", "gpu_uuid": "GPU-12345678-1234-1234-abcd-123456789abc"}
        self.assertEqual(worker.validate_execution(cuda), cuda)
        with self.assertRaises(worker.ScreenError):
            worker.validate_execution({**cuda, "gpu_uuid": None})

    def test_cuda_uses_aggregate_memory_not_address_space_limit(self):
        with mock.patch.object(worker.resource, "setrlimit") as limits, \
                mock.patch.object(worker.os, "nice") as nice, \
                mock.patch.object(worker.screen, "deny_internet") as offline, \
                mock.patch.object(worker, "verify_cuda_memory_limit") as cgroup:
            worker._child_resource_limits({**self.execution, "device": "cuda", "threads": 2}, 20)
        limits.assert_any_call(resource.RLIMIT_CPU, (50, 50))
        limits.assert_any_call(resource.RLIMIT_CORE, (0, 0))
        self.assertNotIn(resource.RLIMIT_AS, [call.args[0] for call in limits.call_args_list])
        nice.assert_called_once_with(10)
        offline.assert_called_once()
        cgroup.assert_called_once()

    def test_async_start_reuses_one_initialized_worker_and_scrubs_env(self):
        with mock.patch.dict(os.environ, {"SCREEN_TEST_SECRET": "never inherited by ML"}):
            resident = self.resident()
        pid = resident.process.pid
        first = resident.analyze_batch([self.item(0), self.item(0)], 10)
        self.assertEqual(first["statistics"], {"model_initializations": 1, "batches": 1, "windows": 2})
        second = resident.analyze_batch([self.item(1)], 10)
        self.assertEqual(resident.process.pid, pid)
        self.assertEqual(first["runtime"], second["runtime"])
        self.assertEqual(second["runtime"]["requested_gpu_uuid"], None)
        self.assertEqual(second["statistics"], {"model_initializations": 1, "batches": 2, "windows": 3})
        self.assertFalse((self.root / "vad.onnx").exists())
        resident.close()
        resident.close()
        self.assertFalse(resident.process.is_alive())
        self.assertIsNotNone(resident.process.exitcode)
        with self.assertRaisesRegex(worker.ScreenError, "closed"):
            resident.analyze_batch([self.item()], 1)

    def test_initialization_is_async_and_first_batch_deadline_kills_reaps(self):
        before = time.monotonic()
        resident = self.resident("1")
        self.assertLess(time.monotonic() - before, 5)
        with self.assertRaises(worker.ScreenError):
            resident.analyze_batch([self.item()], 0.15)
        self.assertTrue(resident.closed)
        self.assertFalse(resident.process.is_alive())

    def test_inference_timeout_closes_unacknowledged_batch(self):
        resident = self.resident("2")
        with self.assertRaises(worker.ScreenError):
            resident.analyze_batch([self.item()], 0.5)
        self.assertEqual(resident.requests, 0)
        self.assertFalse(resident.process.is_alive())

    def test_runtime_drift_invalid_output_and_error_never_emit_partial_batch(self):
        for mode in ("3", "4", "5"):
            with self.subTest(mode=mode):
                resident = self.resident(mode)
                with self.assertRaises(worker.ScreenError) as failure:
                    resident.analyze_batch([self.item()], 10)
                self.assertNotIn("PRIVATE_VECTOR", str(failure.exception))
                self.assertEqual(resident.requests, 0)
                self.assertFalse(resident.process.is_alive())

    def test_changed_source_binding_poisoned_before_request(self):
        resident = self.resident()
        resident.analyze_batch([self.item()], 10)
        (self.impl_root / "speaker_screen.py").write_bytes(b"altered")
        with self.assertRaisesRegex(worker.ScreenError, "changed"):
            resident.analyze_batch([self.item()], 10)
        self.assertEqual(resident.requests, 1)
        self.assertFalse(resident.process.is_alive())

    def test_changed_implementation_after_response_discards_batch(self):
        resident = self.resident()
        with mock.patch.object(worker, "verify_implementation", side_effect=[None, worker.ScreenError("implementation changed")]):
            with self.assertRaisesRegex(worker.ScreenError, "changed"):
                resident.analyze_batch([self.item()], 10)
        self.assertEqual(resident.requests, 0)
        self.assertFalse(resident.process.is_alive())

    def test_observation_validation_preserves_order_and_exact_excerpt_bounds(self):
        items = [self.item(0), self.item(0)]  # Separate recordings may repeat indices.
        vector = [1.0] + [0.0] * 191
        row = {"index": 0, "start_ms": 1000, "end_ms": 3000, "speech_ms": 2000, "embedding": vector}
        worker._observations([row, row], items)
        for altered in ({**row, "index": True}, {**row, "start_ms": -1}, {**row, "speech_ms": 1999},
                        {**row, "embedding": vector[:-1]}, {**row, "embedding": [2.0] + vector[1:]},
                        {**row, "embedding": None}):
            with self.subTest(altered=list(altered)), self.assertRaises(worker.ScreenError):
                worker._observations([altered, row], items)

    def test_invalid_batch_rejected_and_worker_closed(self):
        for invalid in ([], [self.item()] * 9, [{**self.item(), "pcm": b"bad"}],
                        [self.item(True)], [self.item(duration=10001)]):
            with self.subTest(count=len(invalid)):
                resident = self.resident()
                with self.assertRaises(worker.ScreenError):
                    resident.analyze_batch(invalid, 1)
                self.assertFalse(resident.process.is_alive())

    def test_child_retains_inherited_lock_until_reaped(self):
        lock_path = self.root / "lease.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        resident = self.resident(lock_fd=fd)
        try:
            resident.analyze_batch([self.item()], 10)
        finally:
            os.close(fd)  # Do not LOCK_UN: duplicate shares the open description.
        contender = os.open(lock_path, os.O_RDWR)
        try:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            resident.close()
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(contender)

    def test_start_failure_closes_both_sockets_and_registered_child(self):
        process = mock.Mock(pid=123456789)
        process.start.side_effect = RuntimeError("start failed after child registration")
        process.is_alive.side_effect = [True, False, False]
        parent, child = socket.socketpair()
        with mock.patch.object(worker.multiprocessing, "get_context", return_value=mock.Mock(Process=mock.Mock(return_value=process))), \
                mock.patch.object(worker.socket, "socketpair", return_value=(parent, child)), \
                mock.patch.object(worker.os, "killpg", side_effect=ProcessLookupError), \
                self.assertRaisesRegex(RuntimeError, "start failed"):
            worker.ResidentModelProcess(self.models, self.execution, self.implementation, max_run_seconds=30)
        self.assertEqual(parent.fileno(), -1)
        self.assertEqual(child.fileno(), -1)
        process.kill.assert_called_once()
        process.join.assert_called_once_with(timeout=2)

    def test_interrupt_restoring_spawn_mask_reaps_owned_child(self):
        process = mock.Mock(pid=123456789)
        process.is_alive.return_value = False
        parent, child = socket.socketpair()
        real_mask = signal.pthread_sigmask
        restores = 0
        def mask(how, values):
            nonlocal restores
            result = real_mask(how, values)
            if how == signal.SIG_SETMASK:
                restores += 1
                if restores == 1:
                    raise KeyboardInterrupt
            return result
        with mock.patch.object(worker.multiprocessing, "get_context", return_value=mock.Mock(Process=mock.Mock(return_value=process))), \
                mock.patch.object(worker.socket, "socketpair", return_value=(parent, child)), \
                mock.patch.object(worker.signal, "pthread_sigmask", side_effect=mask), \
                self.assertRaises(KeyboardInterrupt):
            worker.ResidentModelProcess(self.models, self.execution, self.implementation, max_run_seconds=30)
        process.start.assert_called_once()
        process.join.assert_called_once_with(timeout=2)
        self.assertEqual(parent.fileno(), -1)
        self.assertEqual(child.fileno(), -1)


class PacketTests(unittest.TestCase):
    def setUp(self):
        self.left, self.right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(self.left.close)
        self.addCleanup(self.right.close)

    def test_round_trip_large_bounded_base64_batch(self):
        body = {"items": [{"pcm_base64": base64.b64encode(bytes(320000)).decode()}] * 16}
        errors = []
        def send():
            try:
                worker.send_packet(self.left, body, time.monotonic() + 5)
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=send)
        thread.start()
        self.assertEqual(worker.receive_packet(self.right, time.monotonic() + 5), body)
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertFalse(errors)

    def test_send_rejects_nonfinite_python_values_and_size(self):
        for value in ({"x": float("nan")}, {"x": float("inf")}, {1: "key"}, {"x": (1, 2)},
                      [], {"x": "a" * worker.MAX_PACKET}):
            with self.subTest(kind=type(value)), self.assertRaises(worker.ScreenError):
                worker.send_packet(self.left, value, time.monotonic() + 1)

    def test_strict_received_json_and_oversized_header(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}', b'[]', b'{"x":"\xff"}', b'{bad'):
            with self.subTest(raw=raw):
                self.left.sendall(len(raw).to_bytes(4, "big") + raw)
                with self.assertRaises(worker.ScreenError):
                    worker.receive_packet(self.right, time.monotonic() + 1)
        self.left.sendall((worker.MAX_PACKET + 1).to_bytes(4, "big"))
        with self.assertRaisesRegex(worker.ScreenError, "size"):
            worker.receive_packet(self.right, time.monotonic() + 1)

    def test_deadline_and_eof_are_bounded(self):
        with self.assertRaises(TimeoutError):
            worker.receive_packet(self.right, time.monotonic() + 0.01)
        self.left.close()
        with self.assertRaises(EOFError):
            worker.receive_packet(self.right, time.monotonic() + 1)


class CgroupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="resident-cgroup-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.parent = self.root / "user.slice"
        self.leaf = self.parent / "worker.scope"
        self.leaf.mkdir(parents=True)
        self.limits(self.parent, "max", "max")
        self.limits(self.leaf, "max", "max")

    def limits(self, path, memory, swap):
        (path / "memory.max").write_text(str(memory) + "\n")
        (path / "memory.swap.max").write_text(str(swap) + "\n")

    def test_aggregate_inherited_limit_with_unconstrained_leaf(self):
        self.limits(self.parent, worker.HOST_MEMORY_MAX, 0)
        self.assertEqual(worker._memory_hierarchy(self.leaf, self.root),
                         {"memory_max_bytes": worker.HOST_MEMORY_MAX, "memory_swap_max_bytes": 0})
        self.limits(self.leaf, worker.HOST_MEMORY_MAX // 2, "max")
        self.assertEqual(worker._memory_hierarchy(self.leaf, self.root)["memory_max_bytes"], worker.HOST_MEMORY_MAX // 2)

    def test_unbounded_excessive_swap_missing_and_malformed_fail_closed(self):
        for memory, swap in (("max", "max"), (worker.HOST_MEMORY_MAX + 1, 0),
                             (worker.HOST_MEMORY_MAX, 1), (0, 0), ("+1", 0), ("-1", 0)):
            with self.subTest(memory=memory, swap=swap):
                self.limits(self.leaf, memory, swap)
                with self.assertRaises(worker.ScreenError):
                    worker._memory_hierarchy(self.leaf, self.root)
        (self.leaf / "memory.max").unlink()
        with self.assertRaises(worker.ScreenError):
            worker._memory_hierarchy(self.leaf, self.root)

    def test_proc_mount_binding_and_cgroup_movement(self):
        self.limits(self.parent, worker.HOST_MEMORY_MAX, 0)
        real_read = worker._small_text
        def read(path):
            if path == "/proc/self/cgroup":
                return "0::/user.slice/worker.scope"
            if path == "/proc/self/mountinfo":
                return f"1 2 0:1 / {self.root} rw - cgroup2 cgroup rw"
            return real_read(path)
        with mock.patch.object(worker, "_small_text", side_effect=read):
            result = worker.verify_cuda_memory_limit()
        self.assertEqual(result["cgroup"], "/user.slice/worker.scope")
        with mock.patch.object(worker, "_small_text", side_effect=["0::/../../escape"]), self.assertRaises(worker.ScreenError):
            worker.verify_cuda_memory_limit()
        with mock.patch.object(worker, "_small_text", side_effect=["0::/", "1 2 0:1 /hidden /sys/fs/cgroup rw - cgroup2 cgroup rw"]), \
                self.assertRaisesRegex(worker.ScreenError, "ancestors"):
            worker.verify_cuda_memory_limit()

    def test_gpu_provenance_contains_uuid_driver_not_cgroup_path(self):
        execution = {"device": "cuda", "gpu_uuid": "GPU-12345678-1234-1234-abcd-123456789abc"}
        engine = mock.Mock(provenance=mock.Mock(return_value={"runtime": "synthetic"}))
        with mock.patch.object(worker, "_small_text", return_value="NVRM version: NVIDIA UNIX Open Kernel Module 610.57.04 Release Build"):
            result = worker._runtime_provenance(engine, execution)
        self.assertEqual(result["nvidia_driver_version"], "610.57.04")
        self.assertEqual(result["requested_gpu_uuid"], execution["gpu_uuid"])
        self.assertNotIn("cgroup", result)
        with mock.patch.object(worker, "_small_text", return_value="unknown"), self.assertRaises(worker.ScreenError):
            worker._runtime_provenance(engine, execution)


if __name__ == "__main__":
    unittest.main()
