"""Initial-plan memoization is bounded and never returns shared mutable jobs."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import unittest
from unittest.mock import patch

from pipeline import transcript_summary_core as core
from pipeline.tests.test_transcript_summary_core import source
from pipeline.tests.test_transcript_summary_synthesis import config, topic


class InitialSummaryCacheTests(unittest.TestCase):
    def setUp(self):
        core._clear_initial_job_cache()

    def tearDown(self):
        core._clear_initial_job_cache()

    def test_same_source_and_full_config_hit_without_replanning(self):
        selected = [source(["An early discussion.", "A different ending."])]
        settings = {**core.DEFAULT_CONFIG, "max_chunk_input_bytes": 24000}
        with patch.object(core, "_fits", wraps=core._fits) as fits:
            first = core.initial_jobs(selected, settings)
            calls = fits.call_count
            self.assertGreater(calls, 0)
            second = core.initial_jobs(deepcopy(selected), deepcopy(settings))
            self.assertEqual(fits.call_count, calls)
        self.assertEqual(first, second)
        self.assertIsNot(first[0], second[0])
        self.assertEqual(core.source_coverage(selected, second)[0]["state"], "fully_planned")
        self.assertEqual(core.validate_job(second[0]), second[0])

    def test_mutating_initial_or_cached_return_cannot_change_future_hits(self):
        selected = [source()]
        first = core.initial_jobs(selected)
        expected = deepcopy(first)
        first[0]["evidence"][0]["text"] = "An invented replacement."
        first[0]["request"]["body"]["contents"] = []
        second = core.initial_jobs(selected)
        self.assertEqual(second, expected)
        second[0]["config"]["max_input_bytes"] = 12000
        second[0]["scope"]["source_ids"].clear()
        self.assertEqual(core.initial_jobs(selected), expected)

    def test_changed_source_config_or_prompt_contract_replans(self):
        selected = [source()]
        original = core.initial_jobs(selected)
        with patch.object(core, "_fits", wraps=core._fits) as fits:
            changed_source = core.initial_jobs([source(["Changed source text."])])
            source_calls = fits.call_count
            self.assertGreater(source_calls, 0)
            changed_config = core.initial_jobs(selected, {**core.DEFAULT_CONFIG, "max_output_tokens": 4096})
            config_calls = fits.call_count
            self.assertGreater(config_calls, source_calls)
            with patch.object(core, "INSTRUCTIONS", core.INSTRUCTIONS + "\nAdditional instruction.\n"):
                changed_prompt = core.initial_jobs(selected)
                self.assertGreater(fits.call_count, config_calls)
            prompt_calls = fits.call_count
            with patch.object(core, "GEMINI_OUTPUT_CONTRACT", core.GEMINI_OUTPUT_CONTRACT + "\nJSON only.\n"):
                changed_contract = core.initial_jobs(selected)
                self.assertGreater(fits.call_count, prompt_calls)
        self.assertEqual(len({jobs[0]["job_id"] for jobs in
                              (original, changed_source, changed_config, changed_prompt, changed_contract)}), 5)

    def test_source_and_topic_selection_validation_still_precedes_cache(self):
        one, two = source(recording="one"), source(recording="two")
        settings = config(topics=[topic(["one", "two"])])
        core.initial_jobs([one, two], settings)
        with self.assertRaisesRegex(core.SummaryError, "missing recording"):
            core.initial_jobs([one], settings)
        changed = deepcopy(one)
        changed["segments"][0]["text"] = "Tampered without updating source identity."
        with self.assertRaises(core.SummaryError):
            core.initial_jobs([changed, two], settings)
        with self.assertRaisesRegex(core.SummaryError, "duplicate summary source"):
            core.initial_jobs([one, one], settings)

    def test_lru_entry_limit_and_source_order(self):
        one, two, three = [source(recording=name) for name in ("one", "two", "three")]
        with patch.object(core, "_INITIAL_CACHE_MAX_ENTRIES", 2):
            first = core.initial_jobs([one, two])
            reversed_jobs = core.initial_jobs([two, one])
            self.assertEqual([job["job_id"] for job in reversed_jobs],
                             [job["job_id"] for job in reversed(first)])
            core.initial_jobs([three])  # Source two is now the least recent entry.
            self.assertEqual(len(core._INITIAL_JOB_CACHE), 2)
            with patch.object(core, "_fits", wraps=core._fits) as fits:
                core.initial_jobs([one])
                self.assertEqual(fits.call_count, 0)
                core.initial_jobs([two])
                self.assertGreater(fits.call_count, 0)
            self.assertEqual(len(core._INITIAL_JOB_CACHE), 2)

    def test_serialized_byte_limit_evicts_and_oversized_entries_are_not_cached(self):
        one, two = source(recording="one"), source(recording="two")
        expected = core.initial_jobs([one])
        one_cost = core._INITIAL_CACHE_BYTES
        core._clear_initial_job_cache()
        with patch.object(core, "_INITIAL_CACHE_MAX_BYTES", one_cost + 128):
            core.initial_jobs([one])
            core.initial_jobs([two])
            self.assertEqual(len(core._INITIAL_JOB_CACHE), 1)
            self.assertLessEqual(core._INITIAL_CACHE_BYTES, one_cost + 128)
            self.assertEqual(core._INITIAL_CACHE_BYTES,
                             sum(len(key) + len(value) for key, value in core._INITIAL_JOB_CACHE.items()))
        core._clear_initial_job_cache()
        with patch.object(core, "_INITIAL_CACHE_MAX_BYTES", 1):
            self.assertEqual(core.initial_jobs([one]), expected)
            self.assertEqual(len(core._INITIAL_JOB_CACHE), 0)
            self.assertEqual(core._INITIAL_CACHE_BYTES, 0)

    def test_concurrent_calls_keep_consistent_accounting_and_independent_results(self):
        selected = [source(["A first passage.", "A later passage."])]
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda _: core.initial_jobs(selected), range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(len(core._INITIAL_JOB_CACHE), 1)
        self.assertEqual(core._INITIAL_CACHE_BYTES,
                         sum(len(key) + len(value) for key, value in core._INITIAL_JOB_CACHE.items()))
        results[0][0]["evidence"][0]["text"] = "Caller mutation."
        self.assertNotEqual(results[0], results[1])
        self.assertEqual(core.initial_jobs(selected), results[1])


if __name__ == "__main__":
    unittest.main()
