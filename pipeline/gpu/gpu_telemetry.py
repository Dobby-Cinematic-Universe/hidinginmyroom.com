#!/usr/bin/env python3
"""Bounded NVML telemetry for the finite HIMR GPU worker.

This module has no scheduling or inference authority.  It samples one admitted
physical GPU, enforces a process-VRAM ceiling at 20 Hz, and retains bounded
histograms rather than an unbounded sample list.  A slower 4 Hz lane records
utilization, temperature, power, clocks, and throttle observations for production
receipts.  Any NVML failure makes the sampler unhealthy; callers must check a fresh
snapshot before publishing an item or batch receipt.
"""

from __future__ import annotations

import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


IMPLEMENTATION_VERSION = "0.1.0"
FAST_INTERVAL_SECONDS = 0.05
SLOW_INTERVAL_SECONDS = 0.25
MAX_SAMPLE_AGE_SECONDS = 0.75
MAX_PROCESS_VRAM_BYTES = 6 * 1024**3


class TelemetryError(RuntimeError):
    """GPU telemetry is absent, stale, malformed, or over its bound."""


class IntegerHistogram:
    """Fixed-width integer histogram with exact bounded memory use."""

    def __init__(self, *, minimum: int, maximum: int, width: int) -> None:
        if minimum < 0 or maximum < minimum or width < 1:
            raise ValueError("invalid histogram bounds")
        self.minimum = minimum
        self.maximum = maximum
        self.width = width
        self.counts = [0] * (((maximum - minimum) // width) + 1)
        self.count = 0
        self.total = 0
        self.minimum_observed: int | None = None
        self.maximum_observed: int | None = None

    def add(self, value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TelemetryError("NVML metric is not an integer")
        bounded = min(max(value, self.minimum), self.maximum)
        index = (bounded - self.minimum) // self.width
        self.counts[index] += 1
        self.count += 1
        self.total += bounded
        self.minimum_observed = (
            bounded
            if self.minimum_observed is None
            else min(self.minimum_observed, bounded)
        )
        self.maximum_observed = (
            bounded
            if self.maximum_observed is None
            else max(self.maximum_observed, bounded)
        )

    def percentile(self, fraction: float) -> int | None:
        if not self.count:
            return None
        if not math.isfinite(fraction) or not 0 <= fraction <= 1:
            raise ValueError("percentile fraction must be within [0, 1]")
        rank = max(1, math.ceil(fraction * self.count))
        cumulative = 0
        for index, count in enumerate(self.counts):
            cumulative += count
            if cumulative >= rank:
                return self.minimum + index * self.width
        raise AssertionError("histogram rank escaped its bins")

    def summary(self) -> dict[str, int | float | None]:
        p50 = self.percentile(0.50)
        p95 = self.percentile(0.95)
        # Percentiles are represented by a bin's lower edge.  Clamp that
        # approximation to the exact observed extrema so a non-unit bin can
        # never emit the impossible ordering p50 < minimum (or p95 > maximum).
        if self.count:
            assert self.minimum_observed is not None
            assert self.maximum_observed is not None
            assert p50 is not None and p95 is not None
            p50 = min(max(p50, self.minimum_observed), self.maximum_observed)
            p95 = min(max(p95, self.minimum_observed), self.maximum_observed)
        return {
            "sample_count": self.count,
            "minimum": self.minimum_observed,
            "mean": None if not self.count else self.total / self.count,
            "p50": p50,
            "p95": p95,
            "maximum": self.maximum_observed,
            "bin_width": self.width,
        }


@dataclass(frozen=True)
class TelemetryLimits:
    maximum_process_vram_bytes: int
    maximum_temperature_c: int
    minimum_free_vram_bytes: int

    def __post_init__(self) -> None:
        for name, value in (
            ("maximum_process_vram_bytes", self.maximum_process_vram_bytes),
            ("maximum_temperature_c", self.maximum_temperature_c),
            ("minimum_free_vram_bytes", self.minimum_free_vram_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.maximum_process_vram_bytes > MAX_PROCESS_VRAM_BYTES:
            raise ValueError("process VRAM limit exceeds the module ceiling")
        if self.maximum_temperature_c > 120:
            raise ValueError("temperature limit exceeds the module ceiling")


def _nvml_integer(call: Callable[..., Any], *args: Any) -> int:
    value = call(*args)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TelemetryError("NVML returned an invalid non-negative integer")
    return value


class NVMLTelemetrySampler:
    """Fail-closed, bounded telemetry sampler for one process and one GPU."""

    def __init__(
        self,
        pynvml: Any,
        handle: Any,
        limits: TelemetryLimits,
        *,
        process_id: int | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        fatal: Callable[[int], None] = os._exit,
    ) -> None:
        self.pynvml = pynvml
        self.handle = handle
        self.limits = limits
        self.process_id = os.getpid() if process_id is None else process_id
        self.monotonic = monotonic
        self.fatal = fatal
        self.stop_event = threading.Event()
        self.guard = threading.Lock()
        self.thread = threading.Thread(
            target=self._sample,
            name="himr-gpu-telemetry-v2",
            daemon=True,
        )
        self.error: str | None = None
        self.last_fast_sample: float | None = None
        self.last_slow_sample: float | None = None
        self.started_monotonic: float | None = None
        self.process_measurement_seen = False
        self.process_peak_bytes = 0
        self.global_peak_bytes = 0
        self.minimum_free_bytes: int | None = None
        self.fast_sample_count = 0
        self.slow_sample_count = 0
        self.active_sample_count = 0
        self.energy_millijoules = 0.0
        self.last_power_sample: tuple[float, int] | None = None
        self.throttle_reason_or = 0
        self.utilization = IntegerHistogram(minimum=0, maximum=100, width=1)
        self.memory_utilization = IntegerHistogram(
            minimum=0, maximum=100, width=1
        )
        self.temperature = IntegerHistogram(minimum=0, maximum=120, width=1)
        self.power_mw = IntegerHistogram(
            minimum=0, maximum=1_000_000, width=1_000
        )
        self.sm_clock_mhz = IntegerHistogram(
            minimum=0, maximum=10_000, width=10
        )

    def _process_memory(self) -> int | None:
        rows = self.pynvml.nvmlDeviceGetComputeRunningProcesses(self.handle)
        values: list[int] = []
        for row in rows:
            used = getattr(row, "usedGpuMemory", None)
            if (
                getattr(row, "pid", None) == self.process_id
                and isinstance(used, int)
                and not isinstance(used, bool)
                and 0 <= used < 2**63
            ):
                values.append(used)
        return None if not values else max(values)

    def _fast(self, now: float) -> None:
        memory = self.pynvml.nvmlDeviceGetMemoryInfo(self.handle)
        total = int(memory.total)
        used = int(memory.used)
        free = int(memory.free)
        if min(total, used, free) < 0 or used + free > total + 64 * 1024**2:
            raise TelemetryError("NVML memory observation is inconsistent")
        process_used = self._process_memory()
        with self.guard:
            self.fast_sample_count += 1
            self.last_fast_sample = now
            self.global_peak_bytes = max(self.global_peak_bytes, used)
            self.minimum_free_bytes = (
                free
                if self.minimum_free_bytes is None
                else min(self.minimum_free_bytes, free)
            )
            if process_used is not None:
                self.process_measurement_seen = True
                self.process_peak_bytes = max(self.process_peak_bytes, process_used)
                if process_used > self.limits.maximum_process_vram_bytes:
                    self.fatal(125)

    def _slow(self, now: float) -> None:
        rates = self.pynvml.nvmlDeviceGetUtilizationRates(self.handle)
        gpu = int(rates.gpu)
        memory = int(rates.memory)
        temperature = _nvml_integer(
            self.pynvml.nvmlDeviceGetTemperature,
            self.handle,
            self.pynvml.NVML_TEMPERATURE_GPU,
        )
        power = _nvml_integer(
            self.pynvml.nvmlDeviceGetPowerUsage, self.handle
        )
        sm_clock = _nvml_integer(
            self.pynvml.nvmlDeviceGetClockInfo,
            self.handle,
            self.pynvml.NVML_CLOCK_SM,
        )
        throttle = _nvml_integer(
            self.pynvml.nvmlDeviceGetCurrentClocksThrottleReasons,
            self.handle,
        )
        with self.guard:
            self.slow_sample_count += 1
            self.last_slow_sample = now
            self.utilization.add(gpu)
            self.memory_utilization.add(memory)
            self.temperature.add(temperature)
            self.power_mw.add(power)
            self.sm_clock_mhz.add(sm_clock)
            self.throttle_reason_or |= throttle
            if gpu > 5:
                self.active_sample_count += 1
            if self.last_power_sample is not None:
                previous_time, previous_power = self.last_power_sample
                elapsed = now - previous_time
                if 0 <= elapsed <= 2 * SLOW_INTERVAL_SECONDS:
                    self.energy_millijoules += (
                        (previous_power + power) / 2 * elapsed
                    )
            self.last_power_sample = (now, power)
            if temperature > self.limits.maximum_temperature_c:
                self.fatal(126)

    def _sample(self) -> None:
        next_slow = 0.0
        with self.guard:
            self.started_monotonic = self.monotonic()
        while not self.stop_event.is_set():
            try:
                now = self.monotonic()
                self._fast(now)
                if now >= next_slow:
                    self._slow(now)
                    next_slow = now + SLOW_INTERVAL_SECONDS
            except Exception as error:  # NVML exposes version-specific exception types.
                with self.guard:
                    self.error = f"{type(error).__name__}: {error}"
                self.stop_event.set()
                return
            self.stop_event.wait(FAST_INTERVAL_SECONDS)

    def start(self) -> None:
        if self.thread.is_alive() or self.started_monotonic is not None:
            raise TelemetryError("telemetry sampler may start only once")
        self.thread.start()

    def snapshot(
        self, *, maximum_age_seconds: float = MAX_SAMPLE_AGE_SECONDS
    ) -> dict[str, Any]:
        now = self.monotonic()
        with self.guard:
            error = self.error
            last_fast = self.last_fast_sample
            last_slow = self.last_slow_sample
            started = self.started_monotonic
            process_seen = self.process_measurement_seen
            process_peak = self.process_peak_bytes
            global_peak = self.global_peak_bytes
            minimum_free = self.minimum_free_bytes
            fast_count = self.fast_sample_count
            slow_count = self.slow_sample_count
            active_count = self.active_sample_count
            energy = self.energy_millijoules
            throttle = self.throttle_reason_or
            utilization = self.utilization.summary()
            memory_utilization = self.memory_utilization.summary()
            temperature = self.temperature.summary()
            power = self.power_mw.summary()
            clock = self.sm_clock_mhz.summary()
        if error is not None:
            raise TelemetryError(f"NVML sampler failed closed: {error}")
        if started is None or last_fast is None or last_slow is None:
            raise TelemetryError("NVML sampler has not produced complete telemetry")
        if now - last_fast > maximum_age_seconds or now - last_slow > maximum_age_seconds:
            raise TelemetryError("NVML telemetry heartbeat is stale")
        if not process_seen:
            raise TelemetryError("NVML has not observed the worker process")
        if process_peak > self.limits.maximum_process_vram_bytes:
            raise TelemetryError("process VRAM exceeded the admitted ceiling")
        if minimum_free is None or minimum_free < self.limits.minimum_free_vram_bytes:
            raise TelemetryError("free VRAM fell below the admitted reserve")
        return {
            "implementation_version": IMPLEMENTATION_VERSION,
            "fast_interval_seconds": FAST_INTERVAL_SECONDS,
            "slow_interval_seconds": SLOW_INTERVAL_SECONDS,
            "fast_sample_count": fast_count,
            "slow_sample_count": slow_count,
            "sample_span_seconds": now - started,
            "last_fast_sample_age_seconds": now - last_fast,
            "last_slow_sample_age_seconds": now - last_slow,
            "process_vram_measurement_seen": True,
            "process_peak_used_bytes": process_peak,
            "global_peak_used_bytes": global_peak,
            "minimum_free_bytes": minimum_free,
            "utilization_percent": utilization,
            "memory_controller_utilization_percent": memory_utilization,
            "active_sample_fraction": active_count / slow_count,
            "temperature_c": temperature,
            "power_mw": power,
            "estimated_energy_millijoules": energy,
            "sm_clock_mhz": clock,
            "throttle_reasons_bitmask_or": throttle,
            "sampler_error": None,
        }

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise TelemetryError("NVML sampler thread did not stop")
        with self.guard:
            error = self.error
        if error is not None:
            raise TelemetryError(f"NVML sampler failed closed: {error}")
