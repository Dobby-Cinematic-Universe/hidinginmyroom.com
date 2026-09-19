from __future__ import annotations

import copy
import builtins
import io
import json
from pathlib import Path
import unittest
from unittest import mock

from operator_console import pipeline_statistics as statistics
from operator_console.longform_statistics import unavailable_statistics
from operator_console.tests import test_service as service_fixture


def available_longform():
    return {"schema_version": 1, "state": "available", "updated_at": "2026-09-06T20:00:00Z",
            "lifecycle": "running", "completion_percent": 60.0,
            "counts": {"completed_recordings": 3, "discovered_recordings": 5,
                       "remaining_discovered_recordings": 2, "unprepared_recordings": 1,
                       "preprocessed_recordings": 0, "prepared_recordings": 0, "incomplete_recordings": 1,
                       "cold_candidates": 4, "queue_candidates": 1, "expected_cold_backlog": 8,
                       "cold_candidates_not_discovered": 4, "active_recordings": 1},
            "basis": "cached_companion_status_completed_recording_jobs", "last_error": None, "diagnostic": None}


class PipelineStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.config = Path("/fixture/controller-config.json")
        self.sha = "a" * 64
        self.controller = {"actual_state": "running", "desired_state": "running",
                           "updated_at": "2026-09-06T21:00:00Z",
                           "pipeline_telemetry": {"queued_items": 7, "preprocessed_items": 12, "asr_completed_items": 10}}

    def test_lanes_keep_distinct_counts_and_timestamps_without_combined_total(self):
        with mock.patch.object(statistics, "read_public_status", return_value=self.controller) as normal, \
                mock.patch.object(statistics, "read_longform_statistics", return_value=available_longform()) as companion:
            report = statistics.collect_statistics(self.config, self.sha)
        normal.assert_called_once_with(self.config, self.sha)
        companion.assert_called_once_with(self.config, self.sha)
        self.assertEqual(report["controller"]["asr_completed_items"], 10)
        self.assertEqual(report["longform"]["counts"]["completed_recordings"], 3)
        self.assertIsNone(report["combined_unique_asr_complete"])
        self.assertNotEqual(report["controller"]["updated_at"], report["longform"]["updated_at"])
        self.assertTrue(report["read_only"])
        self.assertFalse(report["media_scanned"])

    def test_companion_failure_does_not_hide_normal_counts(self):
        with mock.patch.object(statistics, "read_public_status", return_value=self.controller), \
                mock.patch.object(statistics, "read_longform_statistics", side_effect=OSError("private path")):
            report = statistics.collect_statistics(self.config, self.sha)
        self.assertEqual(report["controller"]["asr_completed_items"], 10)
        self.assertEqual(report["longform"]["state"], "unavailable")
        self.assertIsNone(report["longform"]["counts"])
        self.assertNotIn("private path", json.dumps(report))

    def test_normal_failure_does_not_hide_companion_counts(self):
        with mock.patch.object(statistics, "read_public_status", side_effect=OSError("private path")), \
                mock.patch.object(statistics, "read_longform_statistics", return_value=available_longform()):
            report = statistics.collect_statistics(self.config, self.sha)
        self.assertEqual(report["longform"]["counts"]["completed_recordings"], 3)
        self.assertIsNone(report["controller"]["asr_completed_items"])
        self.assertNotIn("private path", json.dumps(report))

    def test_companion_import_failure_preserves_ordinary_cli_report(self):
        original_import = builtins.__import__
        def broken_optional(name, *args, **kwargs):
            if name == "longform_statistics":
                raise ImportError("fixture missing optional reader")
            return original_import(name, *args, **kwargs)
        with mock.patch.object(statistics, "read_public_status", return_value=self.controller), \
                mock.patch("builtins.__import__", side_effect=broken_optional):
            report = statistics.collect_statistics(self.config, self.sha)
        self.assertEqual(report["controller"]["asr_completed_items"], 10)
        self.assertEqual(report["longform"]["state"], "unavailable")

    def test_human_output_distinguishes_unavailable_from_zero_and_discovered_scope(self):
        report = {"controller": {**self.controller["pipeline_telemetry"], "actual_state": "running", "updated_at": None},
                  "longform": available_longform()}
        report["controller"]["asr_completed_items"] = None
        text = statistics.format_statistics(report)
        self.assertIn("Standard-queue ASR complete: Not reported", text)
        self.assertIn("Long-form ASR complete: 3 / 5 discovered recordings", text)
        self.assertIn("60.0% of discovered recordings only", text)
        self.assertIn("Lane totals are separate, not summed", text)
        report["controller"]["asr_completed_items"] = 0
        report["longform"] = unavailable_statistics("fixture")
        text = statistics.format_statistics(report)
        self.assertIn("Standard-queue ASR complete: 0", text)
        self.assertIn("Long-form ASR: Unavailable", text)

    def test_json_cli_does_not_construct_console_or_write_state(self):
        report = {"controller": {"state": "available"}, "longform": {"state": "not_registered"}, "read_only": True}
        with mock.patch.object(statistics, "collect_statistics", return_value=report) as collect, \
                mock.patch("operator_console.service.OperatorService", side_effect=AssertionError("no service startup")), \
                mock.patch("sys.stdout", new_callable=io.StringIO) as output:
            code = statistics.main(["--config", str(self.config), "--expected-config-sha256", self.sha, "--json"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), report)
        collect.assert_called_once_with(self.config, self.sha)

    def test_unavailable_cli_uses_failure_exit_without_losing_partial_report(self):
        report = {"controller": {"state": "available"}, "longform": {"state": "unavailable"}}
        with mock.patch.object(statistics, "collect_statistics", return_value=report), \
                mock.patch("sys.stdout", new_callable=io.StringIO) as output:
            code = statistics.main(["--config", str(self.config), "--expected-config-sha256", self.sha, "--json"])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output.getvalue()), report)


class ConsoleStatisticsIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = service_fixture.OperatorServiceTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        profile, self.config = self.fixture.autonomy_fixture()
        self.service = self.fixture.open_service([profile])
        self.cached = {"actual_state": "running", "desired_state": "running", "can_stop": True,
                       "controls": {"can_start": False, "can_stop": True}, "pipeline_telemetry": {"asr_completed_items": 10}}

    def test_companion_projection_is_advisory_and_does_not_mutate_controller_cache(self):
        before = copy.deepcopy(self.cached)
        with mock.patch("autonomous_controller.public_status.read_public_status", return_value=self.cached), \
                mock.patch("operator_console.longform_statistics.read_longform_statistics", return_value=available_longform()) as reader:
            result = self.service.public_state(csrf_token="fixture", prefix="/o/fixture/")["autonomy"]
        self.assertEqual(result["longform"]["counts"]["completed_recordings"], 3)
        self.assertEqual(result["controls"], before["controls"])
        self.assertEqual(self.cached, before)
        reader.assert_called_once_with(self.config.path, self.config.physical_sha256)

    def test_unexpected_companion_failure_preserves_controller_stop_gate(self):
        with mock.patch("autonomous_controller.public_status.read_public_status", return_value=self.cached), \
                mock.patch("operator_console.longform_statistics.read_longform_statistics", side_effect=RuntimeError("fixture")):
            result = self.service.public_state(csrf_token="fixture", prefix="/o/fixture/")["autonomy"]
        self.assertEqual(result["actual_state"], "running")
        self.assertTrue(result["controls"]["can_stop"])
        self.assertEqual(result["longform"]["state"], "unavailable")

    def test_companion_import_failure_preserves_controller_stop_gate(self):
        original_import = builtins.__import__
        def broken_optional(name, *args, **kwargs):
            if name == "longform_statistics":
                raise ImportError("fixture missing optional reader")
            return original_import(name, *args, **kwargs)
        with mock.patch("autonomous_controller.public_status.read_public_status", return_value=self.cached), \
                mock.patch("builtins.__import__", side_effect=broken_optional):
            result = self.service.public_state(csrf_token="fixture", prefix="/o/fixture/")["autonomy"]
        self.assertEqual(result["actual_state"], "running")
        self.assertTrue(result["controls"]["can_stop"])
        self.assertEqual(result["longform"]["state"], "unavailable")


if __name__ == "__main__":
    unittest.main()
