from __future__ import annotations

import copy
import math
import random
import unittest
from unittest import mock

from pipeline import speaker_screen_core as core


def legacy_cosine(left, right):
    """Frozen pre-optimization arithmetic; do not substitute sumprod/BLAS."""
    return max(-1.0, min(1.0, math.fsum(a * b for a, b in zip(left, right))))


def reference_classify(usable, selected, _unused_similarity):
    """Frozen pre-optimization grouping and full cross-group maximum scans."""
    similarities = {}
    def similarity(left, right):
        key = (min(left, right), max(left, right))
        if key not in similarities:
            similarities[key] = legacy_cosine(usable[left]["embedding"], usable[right]["embedding"])
        return similarities[key]
    groups = []
    ambiguous = []
    for index in range(len(usable)):
        compatible = [group for group in groups if all(similarity(index, member) >= selected["match_cosine_min"] for member in group)]
        if len(compatible) == 1:
            compatible[0].append(index)
        elif len(compatible) > 1:
            ambiguous.append(index)
        else:
            groups.append([index])
    supported = [index for index, group in enumerate(groups) if len(group) >= selected["min_support"]]
    distinct_pairs = []
    for offset, left in enumerate(supported):
        for right in supported[offset + 1:]:
            maximum = max(similarity(a, b) for a in groups[left] for b in groups[right])
            if maximum <= selected["distinct_cosine_max"]:
                distinct_pairs.append({"group_ids": [f"screen_group_{left:04d}", f"screen_group_{right:04d}"],
                                       "maximum_cross_cosine": round(maximum, 8)})
    return groups, supported, distinct_pairs, ambiguous


class SpeakerScreenCoreTests(unittest.TestCase):
    def policy(self, **updates):
        return {**core.DEFAULT_POLICY, **updates}

    def observations(self, vectors, *, duration=None, policy=None):
        duration = duration or len(vectors) * 60_000
        windows = core.plan_windows(duration, policy)
        self.assertEqual(len(windows), len(vectors))
        rows = [{"index": window["index"], "start_ms": window["start_ms"],
                 "end_ms": window["start_ms"] + (3_000 if vector is not None else window["end_ms"] - window["start_ms"]),
                 "speech_ms": 3_000 if vector is not None else 0, "embedding": vector}
                for window, vector in zip(windows, vectors)]
        return duration, windows, rows

    def screen(self, vectors, **kwargs):
        return core.summarize(*self.observations(vectors, **kwargs), policy=kwargs.get("policy"))

    def test_supported_diverse_voices_are_candidates_not_person_counts(self):
        value = self.screen([[1, 0], [0.99, 0.01], [0, 1], [0.01, 0.99]])
        self.assertEqual(value["status"], "multiple_speaker_candidate")
        self.assertEqual(len(value["distinct_group_evidence"]), 1)
        self.assertTrue(all(group["supported"] for group in value["groups"]))
        self.assertFalse(value["semantics"]["groups_are_person_counts"])
        self.assertFalse(value["semantics"]["whole_recording_solo_claimed"])
        self.assertEqual(value["coverage"]["embedded_speech_ms"], 12_000)
        self.assertEqual(value["coverage"]["inspected_window_ms"], 40_000)

    def test_identical_voice_never_asserts_whole_recording_solo(self):
        value = self.screen([[1, 0], [10, 0], [1e300, 0]])
        self.assertEqual(value["status"], "no_second_voice_detected_in_sampled_audio")
        self.assertEqual(len(value["groups"]), 1)
        self.assertFalse(value["coverage"]["whole_recording_inspected"])

    def test_grouping_does_not_force_exactly_two_groups(self):
        vectors = [[1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [0, 0, 1], [0, 0, 1]]
        value = self.screen(vectors)
        self.assertEqual(value["status"], "multiple_speaker_candidate")
        self.assertEqual(len(value["groups"]), 3)
        self.assertEqual(len(value["distinct_group_evidence"]), 3)
        self.assertFalse(value["semantics"]["exact_speaker_count_claimed"])

    def test_isolated_rare_second_voice_requires_review(self):
        value = self.screen([[1, 0], [1, 0], [0, 1]])
        self.assertEqual(value["status"], "uncertain")
        self.assertIn("unsupported_or_isolated_voice_group", value["reason_flags"])
        self.assertEqual(value["distinct_group_evidence"], [])

    def test_supported_but_threshold_ambiguous_groups_are_uncertain(self):
        other = [0.75, math.sqrt(1 - 0.75**2)]
        value = self.screen([[1, 0], [1, 0], other, other])
        self.assertEqual(value["status"], "uncertain")
        self.assertIn("supported_groups_not_clearly_distinct", value["reason_flags"])

    def test_similarity_bridge_does_not_join_distinct_groups(self):
        vectors = [[1, 0], [math.cos(math.radians(50)), math.sin(math.radians(50))],
                   [math.cos(math.radians(25)), math.sin(math.radians(25))]]
        value = self.screen(vectors)
        self.assertEqual(value["status"], "uncertain")
        self.assertIn("ambiguous_group_membership", value["reason_flags"])
        self.assertEqual(len(value["ambiguous_evidence"]), 1)

    def test_no_speech_is_uncertain_not_solo(self):
        value = self.screen([None, None])
        self.assertEqual(value["status"], "uncertain")
        self.assertEqual(value["coverage"]["embedded_excerpts"], 0)
        self.assertEqual(value["groups"], [])

    def test_missing_observations_are_explicit_and_not_a_negative(self):
        duration, windows, rows = self.observations([[1, 0]] * 3)
        value = core.summarize(duration, windows, rows[:2])
        self.assertEqual(value["status"], "uncertain")
        self.assertEqual(value["coverage"]["uninspected_windows"], 1)
        empty = core.summarize(duration, windows, [])
        self.assertEqual(empty["coverage"]["inspected_window_ms"], 0)

    def test_positive_partial_screen_retains_coverage_warning(self):
        duration, windows, rows = self.observations([[1, 0], [1, 0], [0, 1], [0, 1], [1, 0]])
        value = core.summarize(duration, windows, rows[:4])
        self.assertEqual(value["status"], "multiple_speaker_candidate")
        self.assertIn("planned_probes_uninspected", value["reason_flags"])

    def test_null_embedding_with_sufficient_reported_speech_is_uncertain(self):
        duration, windows, rows = self.observations([[1, 0], [1, 0], None])
        rows[-1]["speech_ms"] = 4_000
        value = core.summarize(duration, windows, rows)
        self.assertEqual(value["status"], "uncertain")
        self.assertIn("speech_without_usable_embedding", value["reason_flags"])

    def test_reported_speech_is_distinct_from_selected_excerpt_coverage(self):
        duration, windows, rows = self.observations([[1, 0], None])
        rows[1]["speech_ms"] = 1_500
        value = core.summarize(duration, windows, rows)
        self.assertEqual(value["coverage"]["reported_speech_ms"], 4_500)
        self.assertEqual(value["coverage"]["embedded_speech_ms"], 3_000)

    def test_capped_plan_covers_whole_timeline_not_prefix(self):
        duration = 24 * 60 * 60 * 1000
        policy = self.policy(max_windows=7)
        rows = core.plan_windows(duration, policy)
        self.assertEqual(len(rows), 7)
        self.assertEqual(rows[0]["start_ms"], 0)
        self.assertEqual(rows[-1]["end_ms"], duration)
        gaps = [right["start_ms"] - left["start_ms"] for left, right in zip(rows, rows[1:])]
        self.assertLessEqual(max(gaps) - min(gaps), 1)
        self.assertTrue(all(left["end_ms"] <= right["start_ms"] for left, right in zip(rows, rows[1:])))

    def test_single_capped_probe_is_centered(self):
        self.assertEqual(core.plan_windows(60_000, self.policy(max_windows=1)),
                         [{"index": 0, "start_ms": 25_000, "end_ms": 35_000}])

    def test_short_files_can_report_insufficient_speech(self):
        for duration in (1, 19, 1_999):
            windows = core.plan_windows(duration)
            self.assertEqual(windows, [{"index": 0, "start_ms": 0, "end_ms": duration}])
            rows = [{**windows[0], "speech_ms": duration, "embedding": None}]
            self.assertEqual(core.summarize(duration, windows, rows)["status"], "uncertain")

    def test_dense_policy_never_overlaps_or_extends_past_end(self):
        policy = self.policy(stride_ms=10_000)
        for duration in (10_001, 15_000, 19_999, 25_000, 60_001):
            rows = core.plan_windows(duration, policy)
            self.assertTrue(all(0 <= row["start_ms"] < row["end_ms"] <= duration for row in rows))
            self.assertTrue(all(a["end_ms"] <= b["start_ms"] for a, b in zip(rows, rows[1:])))

    def test_altered_overlapping_or_reordered_windows_are_rejected(self):
        duration, windows, rows = self.observations([[1, 0], [1, 0]])
        variants = [list(reversed(windows)), [windows[0], windows[0]], windows[:1]]
        changed = copy.deepcopy(windows)
        changed[1]["start_ms"] = windows[0]["start_ms"]
        variants.append(changed)
        for variant in variants:
            with self.subTest(variant=variant), self.assertRaises(core.ScreenError):
                core.summarize(duration, variant, rows)

    def test_observation_order_does_not_change_output(self):
        duration, windows, rows = self.observations([[1, 0], [0, 1], [1, 0], [0, 1]])
        self.assertEqual(core.summarize(duration, windows, rows), core.summarize(duration, windows, list(reversed(rows))))

    def test_malformed_embeddings_fail_closed(self):
        for vector in ([], [1], [0, 0], [1, float("nan")], [float("inf"), 1], [True, 0], ["1", 0], [10**1000, 0], [1] * 2049):
            duration, windows, rows = self.observations([[1, 0], [1, 0]])
            rows[0]["embedding"] = vector
            with self.subTest(vector=str(vector)[:40]), self.assertRaises(core.ScreenError):
                core.summarize(duration, windows, rows)
        duration, windows, rows = self.observations([[1, 0], [1, 0, 0]])
        with self.assertRaises(core.ScreenError):
            core.summarize(duration, windows, rows)

    def test_invalid_observation_bounds_and_duplicates_are_rejected(self):
        duration, windows, original = self.observations([[1, 0], [1, 0]])
        changes = [("start_ms", -1), ("end_ms", 20_000), ("speech_ms", 10_001),
                   ("speech_ms", 1_999), ("speech_ms", 9_000), ("end_ms", 1_999), ("end_ms", 5_001), ("index", True)]
        for key, value in changes:
            rows = copy.deepcopy(original)
            rows[0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(core.ScreenError):
                core.summarize(duration, windows, rows)
        with self.assertRaises(core.ScreenError):
            core.summarize(duration, windows, [original[0], original[0]])

    def test_invalid_policy_and_duration_bounds(self):
        invalid = [{}, self.policy(extra=True), self.policy(probe_ms=10_001), self.policy(stride_ms=9_999),
                   self.policy(max_windows=513), self.policy(max_windows=True), self.policy(min_support=1),
                   self.policy(min_speech_ms=1_999), self.policy(min_speech_ms=5_001),
                   self.policy(match_cosine_min=float("nan")), self.policy(distinct_cosine_max=0.9),
                   self.policy(match_cosine_min=True), self.policy(match_cosine_min=10**1000)]
        for policy in invalid:
            with self.subTest(policy=policy), self.assertRaises(core.ScreenError):
                core.validate_policy(policy)
        for duration in (0, -1, True, 1.5, core.MAX_DURATION_MS + 1):
            with self.assertRaises(core.ScreenError):
                core.plan_windows(duration)

    def test_inputs_and_default_policy_are_not_mutated(self):
        duration, windows, rows = self.observations([[2, 0], [2, 0]])
        before = copy.deepcopy((windows, rows, core.DEFAULT_POLICY))
        core.summarize(duration, windows, rows)
        self.assertEqual(before, (windows, rows, core.DEFAULT_POLICY))

    def test_randomized_outputs_equal_frozen_reference_with_and_without_cache(self):
        rng = random.Random(20260912)
        cache = core.SummaryCache()
        for case in range(160):
            count = rng.randrange(2, 49)
            dimensions = rng.choice((2, 3, 8, 192))
            centers = [[rng.uniform(-1, 1) for _ in range(dimensions)] for _ in range(rng.randrange(1, 6))]
            vectors = []
            for _ in range(count):
                center = rng.choice(centers)
                scale = rng.choice((1e-250, 1, 1e250))
                vectors.append(None if rng.random() < .12 else
                               [(value + rng.uniform(-.2, .2)) * scale for value in center])
            cutoffs = sorted(rng.sample((-1.0, -.7, -.1, 0.0, .4, .65, .85, .95, 1.0), 2))
            policy = self.policy(distinct_cosine_max=cutoffs[0], match_cosine_min=cutoffs[1],
                                 min_support=rng.randrange(2, 5))
            duration, windows, rows = self.observations(vectors, policy=policy)
            for row in rows:
                if row["embedding"] is None:
                    row["speech_ms"] = rng.choice((0, 1_500, 4_000))
            if case % 3 == 0:
                rows = rows[:rng.randrange(count + 1)]
            rng.shuffle(rows)
            with mock.patch.object(core, "_classify", side_effect=reference_classify):
                expected = core.summarize(duration, windows, rows, policy)
            with self.subTest(case=case):
                self.assertEqual(expected, core.summarize(duration, windows, rows, policy))
                self.assertEqual(expected, core.summarize(duration, windows, rows, policy, cache=cache))

    def test_cosine_products_and_boundary_rounding_match_legacy_exactly(self):
        rng = random.Random(912)
        for dimensions in (2, 3, 192, 2048):
            for _ in range(12):
                left = core._embedding([rng.uniform(-1, 1) for _ in range(dimensions)])
                right = core._embedding([rng.uniform(-1, 1) for _ in range(dimensions)])
                self.assertEqual(legacy_cosine(left, right).hex(), core._cosine(left, right).hex())
        for cosine in (.65, .85, 0.0, -1.0, 1.0):
            other = [cosine, math.sqrt(max(0, 1 - cosine**2))]
            exact = legacy_cosine(core._embedding([1, 0]), core._embedding(other))
            for threshold in (math.nextafter(exact, -math.inf), exact, math.nextafter(exact, math.inf)):
                if not -1 < threshold <= 1:
                    continue
                policy = self.policy(match_cosine_min=threshold, distinct_cosine_max=-1.0)
                duration, windows, rows = self.observations([[1, 0], [1, 0], other, other], policy=policy)
                with mock.patch.object(core, "_classify", side_effect=reference_classify):
                    expected = core.summarize(duration, windows, rows, policy)
                self.assertEqual(expected, core.summarize(duration, windows, rows, policy))
            if exact < .95:
                for threshold in (math.nextafter(exact, -math.inf), exact, math.nextafter(exact, math.inf)):
                    if threshold < -1:
                        continue
                    policy = self.policy(match_cosine_min=.95, distinct_cosine_max=threshold)
                    duration, windows, rows = self.observations([[1, 0], [1, 0], other, other], policy=policy)
                    with mock.patch.object(core, "_classify", side_effect=reference_classify):
                        expected = core.summarize(duration, windows, rows, policy)
                    self.assertEqual(expected, core.summarize(duration, windows, rows, policy))

    def test_cache_reuses_each_prefix_pair_once_without_changing_reports(self):
        duration, windows, rows = self.observations([[1, .01]] * 16)
        cache = core.SummaryCache()
        with mock.patch.object(core, "_cosine", wraps=core._cosine) as cosine:
            for count in range(1, len(rows) + 1):
                actual = core.summarize(duration, windows, rows[:count], cache=cache)
                with mock.patch.object(core, "_classify", side_effect=reference_classify):
                    expected = core.summarize(duration, windows, rows[:count])
                self.assertEqual(expected, actual)
            self.assertEqual(16 * 15 // 2, cosine.call_count)
            core.summarize(duration, windows, rows, cache=cache)
            self.assertEqual(16 * 15 // 2, cosine.call_count)

    def test_cache_invalidates_mutated_removed_inserted_and_new_dimension_vectors(self):
        duration, windows, original = self.observations([[1, 0], [1, 0], [0, 1], [0, 1], [-1, 0], [-1, 0]])
        cache = core.SummaryCache()
        changed = copy.deepcopy(original)
        changed[0]["embedding"] = [0, 1]
        different_dimensions = copy.deepcopy(original)
        for row in different_dimensions:
            row["embedding"] = [1, 0, 1]
        variants = [original, changed, original[:2], original[2:], original,
                    list(reversed(original)), different_dimensions, [], original]
        for rows in variants:
            self.assertEqual(core.summarize(duration, windows, rows),
                             core.summarize(duration, windows, rows, cache=cache))
        for policy in (self.policy(match_cosine_min=.9), self.policy(distinct_cosine_max=.5), self.policy()):
            self.assertEqual(core.summarize(duration, windows, original, policy),
                             core.summarize(duration, windows, original, policy, cache=cache))

    def test_cached_reports_still_reject_invalid_inputs(self):
        duration, windows, rows = self.observations([[1, 0], [1, 0]])
        cache = core.SummaryCache()
        core.summarize(duration, windows, rows, cache=cache)
        invalid = copy.deepcopy(rows)
        invalid[0]["embedding"][1] = False
        with self.assertRaises(core.ScreenError):
            core.summarize(duration, windows, invalid, cache=cache)
        altered = copy.deepcopy(windows)
        altered[0]["start_ms"] = False
        with self.assertRaises(core.ScreenError):
            core.summarize(duration, altered, rows, cache=cache)
        with self.assertRaises(core.ScreenError):
            core.summarize(duration, windows, rows, cache={})
        self.assertEqual(core.summarize(duration, windows, rows), core.summarize(duration, windows, rows, cache=cache))

    def test_cache_pair_storage_is_bounded_by_512_probes(self):
        duration, windows, rows = self.observations([[1, 0]] * core.MAX_WINDOWS)
        cache = core.SummaryCache()
        result = core.summarize(duration, windows, rows, cache=cache)
        self.assertEqual("no_second_voice_detected_in_sampled_audio", result["status"])
        self.assertEqual(512, len(cache._rows))
        self.assertEqual(512 * 511 // 2, sum(len(row) for row in cache._rows))
        core.summarize(duration, windows, rows[:2], cache=cache)
        self.assertEqual(2, len(cache._rows))

    def test_planning_validation_is_not_duplicated_and_default_coverage_unchanged(self):
        with mock.patch.object(core, "validate_policy", wraps=core.validate_policy) as validate:
            windows = core.plan_windows(24 * 60 * 60 * 1000)
            self.assertEqual(1, validate.call_count)
        self.assertEqual(512, len(windows))
        self.assertEqual(0, windows[0]["start_ms"])
        self.assertEqual(24 * 60 * 60 * 1000, windows[-1]["end_ms"])
        with mock.patch.object(core, "validate_policy", wraps=core.validate_policy) as validate:
            core.summarize(24 * 60 * 60 * 1000, windows, [])
            self.assertEqual(1, validate.call_count)


if __name__ == "__main__":
    unittest.main()
