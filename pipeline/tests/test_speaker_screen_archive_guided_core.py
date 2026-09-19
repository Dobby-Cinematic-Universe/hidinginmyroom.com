from __future__ import annotations

import copy
import unittest

from pipeline import speaker_screen_core as core
from pipeline import speaker_screen_guided_core as previous
from pipeline import speaker_screen_archive_guided_core as archive


POLICY = {**core.DEFAULT_POLICY, "probe_ms": 10000, "stride_ms": 300000, "max_windows": 64}


class ArchiveFastSamplingTests(unittest.TestCase):
    def test_fixed_start_preserves_absolute_timestamps_and_full_duration(self):
        value = archive.build_sampling(900000, POLICY, [])
        self.assertEqual(value["duration_ms"], 900000)
        self.assertEqual(value["windows"][0]["start_ms"], 100)
        self.assertEqual(value["windows"][-1]["end_ms"], 900000)
        self.assertEqual(value["admitted_interval"], {
            "start_ms": 100, "end_ms": 900000, "leading_interval_not_screened_ms": 100})
        self.assertFalse(value["semantics"]["source_timestamps_rebased"])
        self.assertFalse(value["semantics"]["missing_audio_padded"])
        self.assertFalse(value["semantics"]["baseline_preserved"])

    def test_exact_uniform_baseline_over_the_admitted_interval(self):
        for duration in (101, 5000, 10000, 10100, 300000, 600001, 43200000, 86400000):
            with self.subTest(duration=duration):
                value = archive.build_sampling(duration, POLICY, [])
                expected = [{**row, "start_ms": row["start_ms"] + 100, "end_ms": row["end_ms"] + 100}
                            for row in core.plan_windows(duration - 100, POLICY)]
                self.assertEqual(value["windows"], expected)
                self.assertLessEqual(len(expected), 64)
                for left, right in zip(expected, expected[1:]):
                    self.assertLessEqual(left["end_ms"], right["start_ms"])

    def test_very_short_inputs_without_admitted_interval_rejected(self):
        for duration in (0, 1, 99, 100, True):
            with self.subTest(duration=duration), self.assertRaises(core.ScreenError):
                archive.build_sampling(duration, POLICY, [])

    def test_fast_only_and_no_timed_hints(self):
        for change in ({"probe_ms": 9999}, {"stride_ms": 60000}, {"max_windows": 63}):
            with self.subTest(change=change), self.assertRaisesRegex(core.ScreenError, "fast-triage"):
                archive.build_sampling(900000, {**POLICY, **change}, [])
        for targets in (None, {}, [{"hint_id": "x", "start_ms": 20000, "end_ms": 40000}]):
            with self.subTest(targets=targets), self.assertRaisesRegex(core.ScreenError, "temporal hints"):
                archive.build_sampling(900000, POLICY, targets)

    def test_tampered_sampling_and_old_recipe_rejected(self):
        good = archive.build_sampling(900000, POLICY, [])
        for key, value in (("recipe", "old"), ("admitted_interval", {}), ("baseline_indices", [])):
            bad = {**copy.deepcopy(good), key: value}
            with self.subTest(key=key), self.assertRaises(core.ScreenError):
                archive.validate_sampling(900000, POLICY, bad)
        old = previous.build_sampling(900000, POLICY, [])
        with self.assertRaises(core.ScreenError):
            archive.validate_sampling(900000, POLICY, old)

    def test_summary_discloses_unsampled_opening_and_counts_full_duration(self):
        sampling = archive.build_sampling(5100, POLICY, [])
        row = {"index": 0, "start_ms": 100, "end_ms": 5100, "speech_ms": 0, "embedding": None}
        result = archive.summarize(5100, sampling["windows"], [row], POLICY, sampling=sampling)
        self.assertEqual(result["status"], "uncertain")
        self.assertIn("opening_100ms_not_screened", result["reason_flags"])
        self.assertEqual(result["coverage"]["inspected_window_ms"], 5000)
        self.assertEqual(result["coverage"]["inspected_fraction"], 5000 / 5100)
        self.assertFalse(result["coverage"]["whole_recording_inspected"])
        self.assertEqual(result["semantics"]["leading_interval_not_screened_ms"], 100)

    def test_supported_distinct_groups_still_require_acoustic_evidence(self):
        sampling = archive.build_sampling(1200100, POLICY, [])
        observations = [{"index": row["index"], "start_ms": row["start_ms"],
                         "end_ms": row["start_ms"] + 3000, "speech_ms": 3000,
                         "embedding": [1.0, 0.0] if row["index"] < 2 else [0.0, 1.0]}
                        for row in sampling["windows"]]
        result = archive.summarize(1200100, sampling["windows"], observations, POLICY, sampling=sampling)
        self.assertEqual(result["status"], "multiple_speaker_candidate")
        self.assertEqual(result["coverage"]["baseline_inspected_windows"], 4)
        self.assertFalse(result["semantics"]["whole_recording_solo_claimed"])

    def test_missing_windows_not_complete_or_negative(self):
        sampling = archive.build_sampling(1200100, POLICY, [])
        result = archive.summarize(1200100, sampling["windows"], [], POLICY, sampling=sampling)
        self.assertEqual(result["status"], "uncertain")
        self.assertEqual(result["coverage"]["baseline_uninspected_windows"], 4)
        self.assertIn("planned_probes_uninspected", result["reason_flags"])


if __name__ == "__main__":
    unittest.main()
