from __future__ import annotations

import importlib.util
import sys
import time
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


TELEMETRY = load_module(
    "himr_gpu_telemetry_test_module", ROOT / "pipeline/gpu/gpu_telemetry.py"
)


class FakeNVML:
    NVML_TEMPERATURE_GPU = 0
    NVML_CLOCK_SM = 1

    def __init__(self) -> None:
        self.process_bytes = 800_000_000
        self.free_bytes = 5_000_000_000
        self.temperature = 55
        self.fail = False

    def _check(self) -> None:
        if self.fail:
            raise RuntimeError("fixture NVML failure")

    def nvmlDeviceGetMemoryInfo(self, _handle: object) -> SimpleNamespace:
        self._check()
        return SimpleNamespace(
            total=6_000_000_000,
            used=6_000_000_000 - self.free_bytes,
            free=self.free_bytes,
        )

    def nvmlDeviceGetComputeRunningProcesses(
        self, _handle: object
    ) -> list[SimpleNamespace]:
        self._check()
        return [SimpleNamespace(pid=123, usedGpuMemory=self.process_bytes)]

    def nvmlDeviceGetUtilizationRates(self, _handle: object) -> SimpleNamespace:
        self._check()
        return SimpleNamespace(gpu=80, memory=30)

    def nvmlDeviceGetTemperature(self, _handle: object, _sensor: int) -> int:
        self._check()
        return self.temperature

    def nvmlDeviceGetPowerUsage(self, _handle: object) -> int:
        self._check()
        return 40_000

    def nvmlDeviceGetClockInfo(self, _handle: object, _clock: int) -> int:
        self._check()
        return 1_500

    def nvmlDeviceGetCurrentClocksThrottleReasons(self, _handle: object) -> int:
        self._check()
        return 4


class GPUTelemetryTests(unittest.TestCase):
    def test_histogram_is_bounded_and_reports_quantiles(self) -> None:
        histogram = TELEMETRY.IntegerHistogram(minimum=0, maximum=100, width=10)
        for value in (0, 1, 9, 10, 80, 100, 200):
            histogram.add(value)
        summary = histogram.summary()
        self.assertEqual(summary["sample_count"], 7)
        self.assertEqual(summary["minimum"], 0)
        self.assertEqual(summary["maximum"], 100)
        self.assertEqual(summary["p50"], 10)
        self.assertEqual(summary["p95"], 100)
        self.assertEqual(len(histogram.counts), 11)

    def test_histogram_quantiles_cannot_fall_below_exact_minimum(self) -> None:
        histogram = TELEMETRY.IntegerHistogram(
            minimum=0, maximum=10_000, width=10
        )
        for value in (1_505, 1_506, 1_509):
            histogram.add(value)
        summary = histogram.summary()
        self.assertEqual(summary["minimum"], 1_505)
        self.assertEqual(summary["p50"], 1_505)
        self.assertEqual(summary["p95"], 1_505)

    def test_sampler_emits_resource_and_thermal_evidence(self) -> None:
        nvml = FakeNVML()
        sampler = TELEMETRY.NVMLTelemetrySampler(
            nvml,
            object(),
            TELEMETRY.TelemetryLimits(
                maximum_process_vram_bytes=4 * 1024**3,
                maximum_temperature_c=80,
                minimum_free_vram_bytes=2 * 1024**3,
            ),
            process_id=123,
        )
        sampler.start()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                result = sampler.snapshot()
                break
            except TELEMETRY.TelemetryError:
                time.sleep(0.02)
        else:
            self.fail("sampler did not become ready")
        sampler.stop()
        self.assertTrue(result["process_vram_measurement_seen"])
        self.assertEqual(result["process_peak_used_bytes"], 800_000_000)
        self.assertEqual(result["utilization_percent"]["p50"], 80)
        self.assertEqual(result["temperature_c"]["maximum"], 55)
        self.assertEqual(result["throttle_reasons_bitmask_or"], 4)

    def test_sampler_fails_closed_after_nvml_error(self) -> None:
        nvml = FakeNVML()
        sampler = TELEMETRY.NVMLTelemetrySampler(
            nvml,
            object(),
            TELEMETRY.TelemetryLimits(
                maximum_process_vram_bytes=4 * 1024**3,
                maximum_temperature_c=80,
                minimum_free_vram_bytes=2 * 1024**3,
            ),
            process_id=123,
        )
        sampler.start()
        time.sleep(0.06)
        nvml.fail = True
        deadline = time.monotonic() + 1
        while sampler.thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        with self.assertRaisesRegex(TELEMETRY.TelemetryError, "failed closed"):
            sampler.snapshot()
        with self.assertRaisesRegex(TELEMETRY.TelemetryError, "failed closed"):
            sampler.stop()

    def test_resource_ceiling_calls_fatal_hook(self) -> None:
        nvml = FakeNVML()
        nvml.process_bytes = 5 * 1024**3
        exits: list[int] = []
        sampler = TELEMETRY.NVMLTelemetrySampler(
            nvml,
            object(),
            TELEMETRY.TelemetryLimits(
                maximum_process_vram_bytes=4 * 1024**3,
                maximum_temperature_c=80,
                minimum_free_vram_bytes=1,
            ),
            process_id=123,
            fatal=exits.append,
        )
        sampler._fast(1.0)
        self.assertEqual(exits, [125])

    def test_temperature_ceiling_calls_distinct_fatal_hook(self) -> None:
        nvml = FakeNVML()
        nvml.temperature = 90
        exits: list[int] = []
        sampler = TELEMETRY.NVMLTelemetrySampler(
            nvml,
            object(),
            TELEMETRY.TelemetryLimits(
                maximum_process_vram_bytes=4 * 1024**3,
                maximum_temperature_c=80,
                minimum_free_vram_bytes=1,
            ),
            process_id=123,
            fatal=exits.append,
        )
        sampler._slow(1.0)
        self.assertEqual(exits, [126])


if __name__ == "__main__":
    unittest.main()
