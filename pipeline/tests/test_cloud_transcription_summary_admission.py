"""No-API, no-ledger tests for bounded adaptive Gemini admission."""
from copy import deepcopy
import unittest

from pipeline import cloud_transcription_summary_admission as a


class AdmissionTests(unittest.TestCase):
    def assess(self, **overrides):
        args = {"active_waves": [], "candidate_input_tokens": 100,
                "candidate_cost_microusd": 10, "settled_microusd": 100,
                "held_microusd": 0, "budget_limit_microusd": 1000}
        args.update(overrides)
        return a.assess_admission(**args)

    def wave(self, index, state="pending", input_tokens=100):
        return {"wave_id": f"wave-{index}", "state": state, "input_tokens": input_tokens}

    def test_sum_every_job_input_allowance(self):
        self.assertEqual(a.wave_input_tokens([{"budget": {"input_token_allowance": 3}},
                                             {"budget": {"input_token_allowance": 19}}]), 22)

    def test_wave_jobs_require_strict_positive_integer_allowances(self):
        for jobs in ([], None, {}, [None], [{}], [{"budget": {}}]):
            with self.subTest(jobs=jobs), self.assertRaises(a.AdmissionError):
                a.wave_input_tokens(jobs)
        for value in (True, False, None, 0, -1, 2.5, "3"):
            with self.subTest(value=value), self.assertRaises(a.AdmissionError):
                a.wave_input_tokens([{"budget": {"input_token_allowance": value}}])

    def test_admit_up_to_sixteen_slots(self):
        self.assertTrue(self.assess(active_waves=[self.wave(i) for i in range(15)], target=16)["allowed"])
        result = self.assess(active_waves=[self.wave(i) for i in range(16)], target=16)
        self.assertEqual(result["reasons"], ["slot_limit"])
        self.assertTrue(result["wait_for_paid_results"])

    def test_ambiguous_and_orphan_keep_slots_and_tokens(self):
        result = self.assess(active_waves=[self.wave(1, "pending", 20),
            self.wave(2, "ambiguous", 30), self.wave(3, "orphan", 50)], target=3)
        self.assertEqual(result["active_slots"], 3)
        self.assertEqual(result["pending_waves"], 1)
        self.assertEqual(result["unknown_held_waves"], 2)
        self.assertEqual(result["enqueued_input_tokens"], 100)
        self.assertFalse(result["allowed"])

    def test_unknown_only_pressure_requires_reconciliation(self):
        result = self.assess(active_waves=[self.wave(1, "orphan")], target=1)
        self.assertTrue(result["reconciliation_required"])
        self.assertFalse(result["wait_for_paid_results"])

    def test_duplicate_active_wave_fails_instead_of_double_counting(self):
        with self.assertRaises(a.AdmissionError):
            self.assess(active_waves=[self.wave(1), self.wave(1, "orphan")])

    def test_exact_token_ceiling_admits_and_excess_blocks(self):
        args = {"active_waves": [self.wave(1, input_tokens=2_999_900)]}
        self.assertTrue(self.assess(**args)["allowed"])
        result = self.assess(**args, candidate_input_tokens=101)
        self.assertEqual(result["reasons"], ["token_limit"])
        self.assertTrue(result["wait_for_paid_results"])
        self.assertFalse(result["account_quota_verified"])

    def test_oversized_candidate_cannot_be_freed_by_pending_work(self):
        result = self.assess(active_waves=[self.wave(1)], candidate_input_tokens=3_000_001)
        self.assertTrue(result["candidate_exceeds_token_limit"])
        self.assertEqual(result["candidate_state"], "too_large")
        self.assertIn("candidate_too_large", result["reasons"])
        self.assertFalse(result["wait_for_paid_results"])

    def test_candidate_larger_than_immutable_cap_does_not_block_smaller_work(self):
        result = self.assess(candidate_cost_microusd=1001)
        self.assertTrue(result["candidate_exceeds_budget_limit"])
        self.assertEqual(result["candidate_state"], "too_large")
        self.assertTrue(self.assess(candidate_cost_microusd=900)["allowed"])

    def test_exact_budget_admits(self):
        self.assertTrue(self.assess(held_microusd=890)["allowed"])

    def test_reservation_pressure_is_not_completed_exhaustion(self):
        result = self.assess(active_waves=[self.wave(1)], held_microusd=891)
        self.assertEqual(result["reasons"], ["budget_limit"])
        self.assertEqual(result["budget_state"], "temporary_reservation_pressure")
        self.assertTrue(result["wait_for_paid_results"])

    def test_completed_exhaustion_is_candidate_specific(self):
        result = self.assess(active_waves=[self.wave(1)], settled_microusd=995)
        self.assertEqual(result["budget_state"], "completed_exhaustion")
        self.assertFalse(result["wait_for_paid_results"])
        self.assertTrue(self.assess(settled_microusd=995, candidate_cost_microusd=5)["allowed"])

    def test_all_pressure_reasons_returned(self):
        result = self.assess(active_waves=[self.wave(1, input_tokens=3_000_000)], target=1,
                             held_microusd=891, now_seconds=10, cooldown_until_seconds=30)
        self.assertEqual(result["reasons"], ["rate_limit_cooldown", "slot_limit", "token_limit", "budget_limit"])
        self.assertEqual(result["cooldown_remaining_seconds"], 20)

    def test_backoff_never_evicts_existing_work(self):
        waves = [self.wave(i) for i in range(16)]
        before = deepcopy(waves)
        result = self.assess(active_waves=waves, target=4)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["active_slots"], 16)
        self.assertEqual(waves, before)

    def test_input_validation_does_not_allow_bool_or_unbounded_configuration(self):
        for kwargs in ({"target": a.MAX_ACTIVE + 1}, {"target": True}, {"target": 0},
                       {"token_limit": 3_000_001}, {"token_limit": False},
                       {"settled_microusd": True}, {"held_microusd": -1},
                       {"held_microusd": 901}, {"budget_limit_microusd": 0},
                       {"candidate_input_tokens": 0}, {"candidate_cost_microusd": -1},
                       {"now_seconds": 2.1}, {"cooldown_until_seconds": True},
                       {"active_waves": {}}, {"active_waves": [self.wave(1, "finished")]},
                       {"active_waves": [{"wave_id": "", "input_tokens": 2}]},
                       {"active_waves": [{"wave_id": "a", "input_tokens": True}]}):
            with self.subTest(kwargs=kwargs), self.assertRaises(a.AdmissionError):
                self.assess(**kwargs)


class AdaptiveTests(unittest.TestCase):
    def test_initial_eight_and_successful_growth_to_sixteen(self):
        state = a.initial_state(max_active=16)
        self.assertEqual(state["target"], 8)
        targets = []
        for now in range(10):
            state = a.advance(state, now_seconds=now, successful_cycle=True)
            targets.append(state["target"])
        self.assertEqual(targets, [8, 10, 10, 12, 12, 14, 14, 16, 16, 16])

    def test_idle_does_not_increase_target(self):
        state = a.initial_state()
        for now in range(20):
            state = a.advance(state, now_seconds=now)
        self.assertEqual(state["target"], 8)

    def test_429_halves_and_honors_retry_after(self):
        state = a.initial_state()
        before = deepcopy(state)
        backoff = a.advance(state, now_seconds=10, successful_cycle=True,
                            rate_limited=True, retry_after_seconds=120)
        self.assertEqual(state, before)
        self.assertEqual(backoff["target"], 4)
        self.assertEqual(backoff["cooldown_until_seconds"], 130)
        self.assertEqual(backoff["rate_limit_events"], 1)
        self.assertEqual(backoff["success_streak"], 0)

    def test_minimum_cooldown_and_floor_one(self):
        state = a.initial_state(start=1)
        state = a.advance(state, now_seconds=10, rate_limited=True, retry_after_seconds=2)
        self.assertEqual(state["target"], 1)
        self.assertEqual(state["cooldown_until_seconds"], 70)

    def test_cooldown_blocks_growth_and_admission_but_not_collection(self):
        state = a.advance(a.initial_state(), now_seconds=10, rate_limited=True)
        state = a.advance(state, now_seconds=69, successful_cycle=True)
        self.assertEqual(state["success_streak"], 0)
        state = a.advance(state, now_seconds=70, successful_cycle=True)
        state = a.advance(state, now_seconds=71, successful_cycle=True)
        self.assertEqual(state["target"], 6)

    def test_repeated_429_never_shortens_existing_deadline(self):
        state = a.advance(a.initial_state(), now_seconds=1, rate_limited=True, retry_after_seconds=300)
        state = a.advance(state, now_seconds=2, rate_limited=True)
        self.assertEqual(state["cooldown_until_seconds"], 301)

    def test_configured_lower_maximum_is_honored(self):
        state = a.initial_state(start=4, max_active=5)
        for now in range(8):
            state = a.advance(state, now_seconds=now, successful_cycle=True)
        self.assertEqual(state["target"], 5)

    def test_adaptive_values_are_strict(self):
        for args in ({"start": True}, {"start": 9, "max_active": 8}, {"max_active": a.MAX_ACTIVE + 1}):
            with self.subTest(args=args), self.assertRaises(a.AdmissionError):
                a.initial_state(**args)
        for args in ({"now_seconds": -1}, {"now_seconds": 1, "rate_limited": 1},
                     {"now_seconds": 1, "successful_cycle": 1},
                     {"now_seconds": 1, "retry_after_seconds": True}):
            with self.subTest(args=args), self.assertRaises(a.AdmissionError):
                a.advance(a.initial_state(), **args)
        for changes in ({"target": a.MAX_ACTIVE + 1}, {"success_streak": 2}, {"extra": 1}):
            with self.subTest(changes=changes), self.assertRaises(a.AdmissionError):
                a.advance({**a.initial_state(), **changes}, now_seconds=0)


if __name__ == "__main__":
    unittest.main()
