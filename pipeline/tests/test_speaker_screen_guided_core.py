from __future__ import annotations

import copy
import random
import unittest

from pipeline import speaker_screen_core as original
from pipeline import speaker_screen_guided_core as core


class GuidedSamplingTests(unittest.TestCase):
    def policy(self, **changes):
        return {**original.DEFAULT_POLICY, **changes}

    def target(self, start=90_000, end=150_000, hint="guest-1"):
        return {"hint_id": hint, "start_ms": start, "end_ms": end}

    def assert_plan(self, duration, policy, value):
        windows = value["windows"]
        self.assertEqual([row["index"] for row in windows], list(range(len(windows))))
        self.assertTrue(all(0 <= row["start_ms"] < row["end_ms"] <= duration for row in windows))
        self.assertTrue(all(a["end_ms"] <= b["start_ms"] for a, b in zip(windows, windows[1:])))
        self.assertTrue(all(row["end_ms"] - row["start_ms"] <= policy["probe_ms"] for row in windows))
        baseline = original.plan_windows(duration, policy)
        selected = [windows[index] for index in value["baseline_indices"]]
        self.assertEqual([(row["start_ms"], row["end_ms"]) for row in baseline],
                         [(row["start_ms"], row["end_ms"]) for row in selected])
        self.assertEqual(sorted(value["baseline_indices"] + value["target_indices"]), list(range(len(windows))))
        self.assertLessEqual(len(value["target_indices"]), value["max_target_windows"])
        self.assertLessEqual(len(windows), 576)
        self.assertEqual(core.validate_sampling(duration, policy, value), value)

    def test_empty_targets_are_exact_old_baseline(self):
        policy = self.policy()
        value = core.build_sampling(600_000, policy, [])
        self.assertEqual(value["windows"], original.plan_windows(600_000, policy))
        self.assertEqual(value["target_indices"], [])
        self.assertEqual(value["target_coverage"], [])
        self.assert_plan(600_000, policy, value)

    def test_baseline_retained_and_additions_do_not_overlap(self):
        policy = self.policy()
        value = core.build_sampling(600_000, policy, [self.target()])
        self.assertGreater(len(value["target_indices"]), 1)
        self.assert_plan(600_000, policy, value)
        self.assertFalse(value["semantics"]["metadata_is_speaker_evidence"])

    def test_narrow_hint_gets_neighboring_confirmation_opportunities(self):
        policy = self.policy(max_windows=2)
        value = core.build_sampling(240_000, policy, [self.target(90_000, 91_000)])
        self.assertGreaterEqual(len(value["target_indices"]), 2)
        report = value["target_coverage"][0]
        self.assertEqual(report["unprobed_interval_ms"], 0)
        self.assertGreaterEqual(len(report["assigned_target_indices"]), 2)
        self.assert_plan(240_000, policy, value)

    def test_target_cap_zero_is_explicit_and_preserves_baseline(self):
        policy = self.policy()
        value = core.build_sampling(600_000, policy, [self.target()], max_target_windows=0)
        self.assertEqual(value["windows"], original.plan_windows(600_000, policy))
        self.assertIn("target_window_budget", value["target_coverage"][0]["reason_flags"])

    def test_hint_priority_is_round_robin_not_first_hint_consumes_budget(self):
        policy = self.policy(max_windows=2)
        hints = [self.target(50_000, 100_000, "first"), self.target(300_000, 350_000, "second")]
        value = core.build_sampling(600_000, policy, hints, max_target_windows=2)
        self.assertEqual({row["hint_id"] for row in value["target_reasons"]}, {"first", "second"})
        self.assertTrue(all("target_window_budget" in row["reason_flags"] for row in value["target_coverage"]))

    def test_repeated_or_overlapping_intervals_do_not_duplicate_audio(self):
        policy = self.policy()
        hints = [self.target(hint="a"), self.target(hint="b"), self.target(80_000, 160_000, "c")]
        value = core.build_sampling(600_000, policy, hints)
        self.assert_plan(600_000, policy, value)
        for row in value["target_coverage"]:
            self.assertEqual(row["planned_overlap_ms"] + row["unprobed_interval_ms"], row["interval_ms"])

    def test_dense_baseline_leaves_no_target_space(self):
        policy = self.policy(stride_ms=10_000)
        value = core.build_sampling(60_000, policy, [self.target(20_000, 30_000)])
        self.assertEqual(value["target_indices"], [])
        row = value["target_coverage"][0]
        self.assertEqual(row["baseline_overlap_ms"], 10_000)
        self.assertEqual(row["unprobed_interval_ms"], 0)
        self.assertIn("no_available_nonoverlapping_probe", row["reason_flags"])
        self.assertIn("hint_already_covered_by_baseline", row["reason_flags"])

    def test_short_inputs_remain_bounded_and_keep_baseline(self):
        policy = self.policy()
        for duration in (1, 99, 1_999, 5_001, 9_999, 10_000, 10_001):
            with self.subTest(duration=duration):
                value = core.build_sampling(duration, policy, [self.target(0, duration)])
                self.assert_plan(duration, policy, value)
                self.assertEqual(value["target_indices"], [])

    def test_edge_hints_never_seek_before_zero_or_past_eof(self):
        policy = self.policy()
        value = core.build_sampling(300_001, policy,
                                    [self.target(0, 1, "begin"), self.target(300_000, 300_001, "end")])
        self.assert_plan(300_001, policy, value)

    def test_all_supported_target_slots_can_be_added_to_capped_baseline(self):
        duration = original.MAX_DURATION_MS
        policy = self.policy()
        hints = [self.target(10_000_000 * index + 100_000, 10_000_000 * index + 200_000, f"hint-{index}")
                 for index in range(32)]
        value = core.build_sampling(duration, policy, hints, max_target_windows=64)
        self.assertEqual(len(value["baseline_indices"]), 512)
        self.assertEqual(len(value["target_indices"]), 64)
        self.assertEqual(len(value["windows"]), 576)
        self.assert_plan(duration, policy, value)

    def test_randomized_baseline_nonoverlap_determinism_and_bounds(self):
        rng = random.Random(829175)
        for _ in range(60):
            duration = rng.randint(1, 60_000_000)
            width = rng.randint(1, 10_000)
            policy = self.policy(probe_ms=width, stride_ms=rng.randint(width, max(width, duration)),
                                 max_windows=rng.randint(1, 512))
            targets = []
            for index in range(rng.randint(0, 12)):
                start = rng.randint(0, duration - 1)
                targets.append(self.target(start, rng.randint(start + 1, duration), f"hint-{index}"))
            cap = rng.randint(0, 64)
            value = core.build_sampling(duration, policy, targets, max_target_windows=cap)
            self.assert_plan(duration, policy, value)
            self.assertEqual(value, core.build_sampling(duration, policy, targets, max_target_windows=cap))

    def test_input_objects_are_not_mutated(self):
        policy, hints = self.policy(), [self.target()]
        before = copy.deepcopy((policy, hints))
        value = core.build_sampling(600_000, policy, hints)
        self.assertEqual((policy, hints), before)
        value["targets"][0]["hint_id"] = "changed"
        value["policy"]["max_windows"] = 1
        self.assertEqual((policy, hints), before)

    def test_invalid_hints_rejected(self):
        invalid = [None, {}, (), [self.target(hint="")], [self.target(hint=" line\nbreak")],
                   [self.target(hint="a" * 129)], [self.target(hint=" leading")],
                   [self.target(-1, 5)], [self.target(1, 1)], [self.target(2, 1)],
                   [self.target(0, 600_001)], [self.target(True, 10)],
                   [self.target(1.0, 10)], [self.target(0, float("inf"))],
                   [{**self.target(), "title": "two speakers"}],
                   [self.target(), self.target()],
                   [self.target(hint=f"hint-{i}") for i in range(33)]]
        for targets in invalid:
            with self.subTest(targets=targets), self.assertRaises(core.ScreenError):
                core.build_sampling(600_000, self.policy(), targets)

    def test_invalid_duration_policy_and_caps_rejected(self):
        for duration in (0, -1, True, 100.0, original.MAX_DURATION_MS + 1):
            with self.subTest(duration=duration), self.assertRaises(core.ScreenError):
                core.build_sampling(duration, self.policy(), [])
        for cap in (-1, 65, True, 1.0, None):
            with self.subTest(cap=cap), self.assertRaises(core.ScreenError):
                core.build_sampling(600_000, self.policy(), [], max_target_windows=cap)
        with self.assertRaises(core.ScreenError):
            core.build_sampling(600_000, {**self.policy(), "title_weight": 1}, [])

    def test_sealed_sampling_tampering_rejected(self):
        value = core.build_sampling(600_000, self.policy(), [self.target()])
        changes = [lambda s: s["windows"].reverse(),
                   lambda s: s["windows"].append(dict(s["windows"][0])),
                   lambda s: s["windows"][0].update(index=False),
                   lambda s: s["windows"][0].update(start_ms=0.0),
                   lambda s: s["windows"][1].update(start_ms=s["windows"][0]["start_ms"]),
                   lambda s: s["target_reasons"][0].update(hint_id="other"),
                   lambda s: s["target_coverage"][0].update(unprobed_interval_ms=-1),
                   lambda s: s["baseline_indices"].pop(),
                   lambda s: s.update(schema_version=True),
                   lambda s: s.update(extra="not bound"),
                   lambda s: s["semantics"].update(metadata_is_speaker_evidence=True)]
        for change in changes:
            altered = copy.deepcopy(value)
            change(altered)
            with self.subTest(change=change), self.assertRaises(core.ScreenError):
                core.validate_sampling(600_000, self.policy(), altered)


class GuidedSummaryTests(unittest.TestCase):
    def sampling(self, targets=None, *, duration=600_000, policy=None):
        return core.build_sampling(duration, policy, targets or [])

    def observations(self, sampling, vectors):
        windows = sampling["windows"]
        self.assertEqual(len(vectors), len(windows))
        return [{"index": row["index"], "start_ms": row["start_ms"],
                 "end_ms": row["start_ms"] + 3_000 if vector is not None else row["end_ms"],
                 "speech_ms": 3_000 if vector is not None else 0, "embedding": vector}
                for row, vector in zip(windows, vectors)]

    def summarize(self, sampling, observations, **kwargs):
        return core.summarize(sampling["duration_ms"], sampling["windows"], observations,
                              sampling["policy"], sampling=sampling, **kwargs)

    def old_projection(self, summary):
        value = copy.deepcopy(summary)
        value["kind"] = "himr_speaker_diversity_screen"
        value.pop("metadata_is_speaker_evidence")
        value["semantics"].pop("metadata_is_speaker_evidence")
        value["semantics"].pop("baseline_preserved")
        value["coverage"] = {key: item for key, item in value["coverage"].items()
                             if not key.startswith(("baseline_", "targeted_"))}
        return value

    def test_empty_hint_acoustic_parity_randomized(self):
        rng = random.Random(829176)
        vectors = ([1, 0, 0], [0, 1, 0], [0, 0, 1], [0.7, 0.7, 0], [0.8, 0.6, 0], None)
        for _ in range(60):
            count = rng.randint(1, 30)
            sampling = self.sampling(duration=count * 60_000)
            observations = self.observations(sampling, [rng.choice(vectors) for _ in range(count)])
            observations = observations[:rng.randint(0, count)]
            guided = self.summarize(sampling, observations)
            expected = original.summarize(sampling["duration_ms"], sampling["windows"], observations,
                                          sampling["policy"])
            self.assertEqual(self.old_projection(guided), expected)

    def test_hints_without_audio_are_uncertain_not_speaker_evidence(self):
        sampling = self.sampling([{"hint_id": "two speakers", "start_ms": 90_000, "end_ms": 150_000}])
        value = self.summarize(sampling, [])
        self.assertEqual(value["status"], "uncertain")
        self.assertEqual(value["groups"], [])
        self.assertFalse(value["metadata_is_speaker_evidence"])
        self.assertFalse(value["semantics"]["identity_inferred"])

    def test_baseline_only_completion_is_not_negative_when_targets_remain(self):
        sampling = self.sampling([{"hint_id": "guest", "start_ms": 90_000, "end_ms": 150_000}])
        rows = self.observations(sampling, [[1, 0]] * len(sampling["windows"]))
        baseline = set(sampling["baseline_indices"])
        value = self.summarize(sampling, [row for row in rows if row["index"] in baseline])
        self.assertEqual(value["status"], "uncertain")
        self.assertEqual(value["coverage"]["baseline_uninspected_windows"], 0)
        self.assertGreater(value["coverage"]["targeted_uninspected_windows"], 0)

    def test_completed_one_group_is_only_sampled_negative(self):
        sampling = self.sampling([{"hint_id": "guest", "start_ms": 90_000, "end_ms": 150_000}])
        rows = self.observations(sampling, [[1, 0]] * len(sampling["windows"]))
        value = self.summarize(sampling, rows)
        self.assertEqual(value["status"], "no_second_voice_detected_in_sampled_audio")
        self.assertFalse(value["semantics"]["whole_recording_solo_claimed"])
        self.assertEqual(value["coverage"]["uninspected_windows"], 0)
        self.assertEqual(value["coverage"]["baseline_inspected_windows"], len(sampling["baseline_indices"]))
        self.assertEqual(value["coverage"]["targeted_inspected_windows"], len(sampling["target_indices"]))

    def test_supported_positive_partial_result_keeps_uninspected_warning(self):
        sampling = self.sampling([{"hint_id": "guest", "start_ms": 90_000, "end_ms": 150_000}])
        vectors = [[1, 0], [1, 0], [0, 1], [0, 1]] + [[1, 0]] * (len(sampling["windows"]) - 4)
        rows = self.observations(sampling, vectors)
        value = self.summarize(sampling, rows[:4])
        self.assertEqual(value["status"], "multiple_speaker_candidate")
        self.assertIn("planned_probes_uninspected", value["reason_flags"])
        self.assertFalse(value["semantics"]["exact_speaker_count_claimed"])

    def test_isolated_outlier_cannot_establish_second_voice(self):
        sampling = self.sampling()
        rows = self.observations(sampling, [[0, 1]] + [[1, 0]] * 9)
        self.assertEqual(self.summarize(sampling, rows)["status"], "uncertain")

    def test_metadata_hint_label_does_not_change_acoustic_decision(self):
        first = self.sampling([{"hint_id": "guest", "start_ms": 90_000, "end_ms": 150_000}])
        second = self.sampling([{"hint_id": "solo", "start_ms": 90_000, "end_ms": 150_000}])
        rows = self.observations(first, [[1, 0]] * len(first["windows"]))
        self.assertEqual(self.summarize(first, rows), self.summarize(second, rows))

    def test_existing_recording_local_cache_matches_uncached_summary(self):
        sampling = self.sampling([{"hint_id": "guest", "start_ms": 90_000, "end_ms": 150_000}])
        rows = self.observations(sampling, [[1, 0]] * len(sampling["windows"]))
        cache = original.SummaryCache()
        for count in range(len(rows) + 1):
            self.assertEqual(self.summarize(sampling, rows[:count], cache=cache),
                             self.summarize(sampling, rows[:count]))

    def test_576_window_summary_remains_bounded_and_valid(self):
        duration = original.MAX_DURATION_MS
        targets = [{"hint_id": f"hint-{index}", "start_ms": 10_000_000 * index + 100_000,
                    "end_ms": 10_000_000 * index + 200_000} for index in range(32)]
        sampling = core.build_sampling(duration, None, targets, max_target_windows=64)
        rows = self.observations(sampling, [[1, 0]] * 576)
        value = self.summarize(sampling, rows, cache=original.SummaryCache())
        self.assertEqual(value["coverage"]["inspected_windows"], 576)
        self.assertEqual(value["status"], "no_second_voice_detected_in_sampled_audio")

    def test_short_no_speech_is_uncertain(self):
        sampling = self.sampling(duration=99)
        rows = [{**sampling["windows"][0], "speech_ms": 0, "embedding": None}]
        self.assertEqual(self.summarize(sampling, rows)["status"], "uncertain")

    def test_invalid_observations_and_cache_rejected(self):
        sampling = self.sampling()
        good = self.observations(sampling, [[1, 0]] * 10)
        bad = [good + [good[0]], [good[0], good[0]], [{**good[0], "index": True}],
               [{**good[0], "start_ms": -1}], [{**good[0], "end_ms": 999_999}],
               [{**good[0], "speech_ms": 3_001}], [{**good[0], "embedding": [0, 0]}],
               [{**good[0], "embedding": [float("nan"), 1]}],
               [good[0], {**good[1], "embedding": [1, 0, 0]}],
               [{**good[0], "person": "unknown"}]]
        for rows in bad:
            with self.subTest(rows=rows), self.assertRaises(core.ScreenError):
                self.summarize(sampling, rows)
        with self.assertRaises(core.ScreenError):
            self.summarize(sampling, good, cache={})

    def test_mismatched_supplied_windows_rejected(self):
        sampling = self.sampling()
        with self.assertRaises(core.ScreenError):
            core.summarize(600_000, list(reversed(sampling["windows"])), [], sampling=sampling)


if __name__ == "__main__":
    unittest.main()
