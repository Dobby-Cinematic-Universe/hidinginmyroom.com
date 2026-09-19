from copy import deepcopy
import random
import unittest
from unittest.mock import patch

from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_fast_initial as fast
from pipeline import transcript_summary_sources as sources
from pipeline.tests.test_transcript_summary_core import source


class FastInitialJobsTests(unittest.TestCase):
    def setUp(self):
        core._clear_initial_job_cache()

    def assertParity(self, selected, config=None):
        core._clear_initial_job_cache()
        expected = core.initial_jobs(selected, config)
        actual = fast.initial_jobs(selected, config)
        self.assertEqual(core.canonical(actual), core.canonical(expected))
        return actual

    def test_pinned_core_is_current(self):
        self.assertTrue(fast._CORE_SUPPORTED)

    def test_default_and_text_only_projection(self):
        selected = [source(["A beginning.", "A middle.", "An ending."], date="2020-03-12")]
        for extra in ({}, {"transcript_input_policy": "text_and_speaker_evidence_v1"}):
            with self.subTest(extra=extra):
                self.assertParity(selected, {**core.DEFAULT_CONFIG, **extra})

    def test_all_profiles_and_schema_output_bounds(self):
        selected = [source(["Important topic. " * 70] * 18)]
        for profile in core.PROFILES:
            for refs in (None, 24, 256):
                for local_bounds in (False, True):
                    config = {**core.DEFAULT_CONFIG, "transcript_profile": profile,
                              "max_input_bytes": 14000, "max_chunk_input_bytes": 12000,
                              "max_output_tokens": 1024, "max_item_chars": 1600,
                              "max_items_per_section": 32}
                    if refs is not None:
                        config["max_evidence_refs_per_item"] = refs
                    if local_bounds:
                        config["gemini_schema_policy"] = "local_array_bounds_v2"
                    with self.subTest(profile=profile, refs=refs, local_bounds=local_bounds):
                        self.assertParity(selected, config)

    def test_unicode_escaping_splits_and_coarse_timestamps(self):
        selected = [source(["漢字🙂\\\"\n\t\r/é\u2028" * 1700, "Final portion."])]
        for policy in (None, "text_and_speaker_evidence_v1"):
            config = {**core.DEFAULT_CONFIG, "max_input_bytes": 12000}
            if policy:
                config["transcript_input_policy"] = policy
            jobs = self.assertParity(selected, config)
            self.assertGreater(len(jobs), 3)
            self.assertEqual(core.source_coverage(selected, jobs)[0]["state"], "fully_planned")

    def test_alias_digits_and_source_scoped_speakers(self):
        selected = source(["Spoken text " * 20] * 130, speaker="SPEAKER_0001")
        for index, row in enumerate(selected["segments"]):
            row["speaker"] = None if index % 4 == 0 else "SPEAKER_" + str(index % 13).zfill(4)
            row["source_ref"]["speaker_scope"] = "scope-" + str(index % 3)
            row["evidence_id"] = sources._evidence_id(selected["transcript"], row)
        selected["source_id"] = "summarysrc_" + sources._hash({
            key: value for key, value in selected.items() if key != "source_id"})[:32]
        for policy in (None, "text_and_speaker_evidence_v1"):
            config = {**core.DEFAULT_CONFIG, "max_input_bytes": 18000}
            if policy:
                config["transcript_input_policy"] = policy
            self.assertParity([selected], config)

    def test_empty_whitespace_and_multiple_sources_order(self):
        selected = [source([], "empty"), source(["", " \n\t"], "blank"),
                    source(["", " \n", "Actual text", ""], "first", timed=False),
                    source(["Other source"], "second", date="2024-01-09")]
        self.assertParity(selected)

    def test_many_tiny_segments_do_not_rebuild_pending_jobs(self):
        selected = [source(["Short." for _ in range(350)])]
        expected = core.initial_jobs(selected)
        with patch.object(core, "make_job", wraps=core.make_job) as build:
            actual = fast.initial_jobs(selected)
        self.assertEqual(actual, expected)
        self.assertEqual(build.call_count, len(actual) + 1)
        self.assertEqual(len(actual), 1)

    def test_exact_byte_boundary_and_one_extra_character(self):
        config = {**core.DEFAULT_CONFIG, "max_input_bytes": 12000,
                  "transcript_input_policy": "text_and_speaker_evidence_v1"}
        probe = core.initial_jobs([source(["x"])], config)[0]
        length = 12000 - probe["budget"]["input_utf8_bytes"] + 1
        exact = self.assertParity([source(["x" * length])], config)
        extra = self.assertParity([source(["x" * (length + 1)])], config)
        self.assertEqual(len(exact), 1)
        self.assertEqual(exact[0]["budget"]["input_utf8_bytes"], 12000)
        self.assertEqual(len(extra), 2)

    def test_deterministic_random_boundaries(self):
        generator = random.Random(93241)
        alphabet = "abc漢🙂\\\"\n\t\ré"
        for case in range(12):
            texts = ["".join(generator.choices(alphabet, k=generator.randrange(1, 7000)))
                     for _ in range(generator.randrange(1, 8))]
            config = {**core.DEFAULT_CONFIG, "max_input_bytes": generator.randrange(12000, 19000)}
            if case % 2:
                config["transcript_input_policy"] = "text_and_speaker_evidence_v1"
            with self.subTest(case=case):
                self.assertParity([source(texts, recording="random-" + str(case))], config)

    def test_config_and_source_validation_are_not_skipped(self):
        original = source()
        invalid = deepcopy(original)
        invalid["segments"][0]["text"] = "tampered"
        for selected, config in (([invalid], None), ([original, original], None),
                                 ([original], {**core.DEFAULT_CONFIG, "max_input_bytes": 1})):
            with self.subTest(config=config), self.assertRaises(core.SummaryError):
                fast.initial_jobs(selected, config)

    def test_unknown_core_fallback_avoids_scoped_wrapper_recursion(self):
        selected = [source()]
        expected = core.initial_jobs(selected)
        with patch.object(fast, "_CORE_SUPPORTED", False), patch.object(
                core, "initial_jobs", side_effect=AssertionError("wrapper must not be reentered")):
            self.assertEqual(fast.initial_jobs(selected), expected)

    def test_projection_mismatch_fails_before_returning_jobs(self):
        with patch.object(fast._ChunkSizer, "append_size", return_value=1):
            with self.assertRaisesRegex(core.SummaryError, "byte accounting differs"):
                fast.initial_jobs([source()])


if __name__ == "__main__":
    unittest.main()
