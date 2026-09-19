"""Smaller raw chunks preserve coverage without constraining later reductions."""
from copy import deepcopy
import json
import unittest

from pipeline import transcript_summary_core as core
from pipeline.tests.test_transcript_summary_core import complete, output, source


class SummaryCoverageTests(unittest.TestCase):
    def test_optional_chunk_bound_is_exact_and_does_not_change_default_config(self):
        self.assertNotIn("max_chunk_input_bytes", core.normalize_config())
        settings = {**core.DEFAULT_CONFIG, "max_chunk_input_bytes": 24000}
        self.assertEqual(core.normalize_config(settings), settings)
        for bound in (None, True, 11999, 180001, "24000"):
            with self.subTest(bound=bound), self.assertRaises(core.SummaryError):
                core.normalize_config({**core.DEFAULT_CONFIG, "max_chunk_input_bytes": bound})
        self.assertEqual(core.normalize_config({**core.DEFAULT_CONFIG, "max_input_bytes": 12000,
                                                "max_chunk_input_bytes": 12000})["max_chunk_input_bytes"], 12000)

    def test_small_chunks_cover_early_middle_and_ending_text_exactly(self):
        texts = ["EARLY: work plans."] + ["Ordinary material. " * 5 for _ in range(348)] + [
            "ENDING: reconsidering housing after a stressful evening."]
        selected = [source(texts)]
        settings = {**core.DEFAULT_CONFIG, "max_chunk_input_bytes": 24000}
        original = deepcopy(selected)
        self.assertEqual(len(core.initial_jobs(selected)), 1)
        jobs = core.initial_jobs(selected, settings)
        self.assertGreater(len(jobs), 1)
        self.assertEqual("".join(item["text"] for job in jobs for item in job["evidence"]), "".join(texts))
        self.assertEqual(jobs[0]["evidence"][0]["text"], texts[0])
        self.assertEqual(jobs[-1]["evidence"][-1]["text"], texts[-1])
        self.assertEqual(core.source_coverage(selected, jobs)[0]["state"], "fully_planned")
        self.assertEqual(selected, original)
        for job in jobs:
            self.assertLessEqual(len(core.canonical(job["request"]["body"])), 24000)
            self.assertEqual(core.validate_job(job), job)
        self.assertEqual(core.initial_jobs(selected, settings), jobs)

    def test_small_chunk_bound_preserves_utf8_and_original_citation_ranges(self):
        selected = [source(["漢字🙂\\\"\n" * 5000])]
        settings = {**core.DEFAULT_CONFIG, "max_chunk_input_bytes": 12000}
        jobs = core.initial_jobs(selected, settings)
        evidence = [item for job in jobs for item in job["evidence"]]
        self.assertGreater(len(jobs), 2)
        self.assertEqual("".join(item["text"] for item in evidence), selected[0]["segments"][0]["text"])
        self.assertEqual(core.source_coverage(selected, jobs)[0]["state"], "fully_planned")
        self.assertTrue(all(item["citations"][0]["start_ms"] == 0 for item in evidence))
        self.assertTrue(all(job["budget"]["input_utf8_bytes"] <= 12000 for job in jobs))

    def test_reducers_keep_the_larger_bound_and_receive_every_chunk_topic(self):
        selected = [source(["x" * 50000])]
        settings = {**core.DEFAULT_CONFIG, "max_chunk_input_bytes": 24000}
        jobs = core.initial_jobs(selected, settings)
        self.assertGreaterEqual(len(jobs), 3)
        results = {}
        for ordinal, job in enumerate(jobs):
            payload = output(job, text=(f"Distinct topic from chunk {ordinal}. " + "a" * 1200)[:1200])
            payload["summary"] *= 12
            results[job["job_id"]] = core.normalize_result(job, payload)
        reductions = core.next_jobs(selected, jobs, results, settings, stages={"transcript"})
        self.assertEqual(len(reductions), 1)
        reduction = reductions[0]
        self.assertTrue(reduction["scope"]["final"])
        self.assertEqual(set(reduction["dependencies"]), set(results))
        self.assertGreater(reduction["budget"]["input_utf8_bytes"], 24000)
        self.assertLessEqual(reduction["budget"]["input_utf8_bytes"], 180000)
        self.assertEqual(len(reduction["evidence"]), len(jobs) * 12)
        self.assertEqual(reduction["evidence"][-1]["text"],
                         results[jobs[-1]["job_id"]]["sections"]["summary"][-1]["text"])
        self.assertEqual(core.validate_job(reduction), reduction)
        # The same long summaries cannot combine with one shared 24 KB cap.
        tight = {**core.DEFAULT_CONFIG, "max_input_bytes": 24000}
        tight_jobs = core.initial_jobs(selected, tight)
        tight_results = {}
        for job in tight_jobs:
            payload = output(job, text="a" * 1200)
            payload["summary"] *= 12
            tight_results[job["job_id"]] = core.normalize_result(job, payload)
        with self.assertRaisesRegex(core.SummaryError, "hierarchy would not shrink"):
            core.next_jobs(selected, tight_jobs, tight_results, tight, stages={"transcript"})

    def test_transcript_reduction_waits_for_last_small_chunk(self):
        selected = [source(["x" * 50000])]
        settings = {**core.DEFAULT_CONFIG, "max_chunk_input_bytes": 24000}
        jobs = core.initial_jobs(selected, settings)
        results = {job["job_id"]: complete(job) for job in jobs[:-1]}
        self.assertEqual(core.next_jobs(selected, jobs, results, settings, stages={"transcript"}), [])
        results[jobs[-1]["job_id"]] = complete(jobs[-1], text="The ending introduces a new plan.")
        reduction = core.next_jobs(selected, jobs, results, settings, stages={"transcript"})[0]
        self.assertEqual(reduction["evidence"][-1]["text"], "The ending introduces a new plan.")

    def test_coverage_and_self_description_guidance_preserve_direct_daniel_default(self):
        selected = [source(["I rarely spoke at school. I now plan to rent a house."])]
        jobs = core.initial_jobs(selected)
        instructions = jobs[0]["prompt"]["instructions"]
        self.assertIn("later discussions, changes of mind and the ending", instructions)
        self.assertIn("separate summary items", instructions)
        self.assertIn("transcription loops or name-list artifacts", instructions)
        self.assertIn("Qualify clinical-sounding self-descriptions; do not diagnose", instructions)
        self.assertIn("Daniel is the user-designated default main speaker", instructions)
        self.assertIn('Use direct wording like "Daniel returned two books"', instructions)
        results = {jobs[0]["job_id"]: complete(jobs[0])}
        reduction = core.next_jobs(selected, jobs, results)[0]
        self.assertIn("each substantive input topic survives", reduction["prompt"]["instructions"])
        self.assertIn("ending developments", reduction["prompt"]["instructions"])
        self.assertIn("changes of plan and explicitly stated reasons", reduction["prompt"]["instructions"])
        self.assertEqual(jobs[0]["evidence"][0]["text"], selected[0]["segments"][0]["text"])

    def test_gemini_schema_is_projected_before_hashing_and_local_limits_remain_strict(self):
        settings = {**core.DEFAULT_CONFIG, "max_items_per_section": 2, "max_item_chars": 200}
        job = core.initial_jobs([source()], settings)[0]
        wire = job["request"]["body"]["generationConfig"]["responseSchema"]
        original = job["prompt"]["response_schema"]
        self.assertNotIn("responseJsonSchema", job["request"]["body"]["generationConfig"])
        for keyword in ("minLength", "maxLength", "maxItems", "additionalProperties"):
            self.assertNotIn(keyword, core.canonical(wire).decode())
            self.assertIn(keyword, core.canonical(original).decode())
        self.assertEqual(wire["properties"]["summary"]["minItems"], "1")
        self.assertFalse(original["additionalProperties"])
        self.assertIn("at most 2 items per section", job["prompt"]["instructions"])
        self.assertIn("1..200 nonblank text characters", job["prompt"]["instructions"])
        self.assertEqual(core.validate_job(job), job)
        for payload in (output(job, text="a" * 201),
                        {**output(job), "summary": output(job)["summary"] * 3}):
            with self.assertRaises(core.SummaryError):
                core.normalize_result(job, payload)
        altered = deepcopy(job)
        altered["request"]["body"]["generationConfig"]["responseSchema"] = original
        with self.assertRaisesRegex(core.SummaryError, "job replay"):
            core.validate_job(altered)
        openai = core.initial_jobs([source()], {**settings, "transcript_profile": "openai_mini_batch"})[0]
        self.assertEqual(openai["request"]["body"]["text"]["format"]["schema"], original)

    def test_native_gemini_schema_has_recursive_types_ordering_and_int64_minima(self):
        original = core.response_schema()
        before = deepcopy(original)
        wire = core._gemini_schema(original)
        self.assertEqual(set(wire), {"type", "properties", "required", "propertyOrdering"})
        self.assertEqual(wire["type"], "OBJECT")
        self.assertEqual(wire["required"], list(core.SECTIONS))
        self.assertEqual(wire["propertyOrdering"], list(core.SECTIONS))
        for section in core.SECTIONS:
            array = wire["properties"][section]
            self.assertEqual(set(array), {"type", "items", "minItems"})
            self.assertEqual(array["type"], "ARRAY")
            self.assertEqual(array["minItems"], "1" if section == "summary" else "0")
            item = array["items"]
            self.assertEqual(set(item), {"type", "properties", "required", "propertyOrdering"})
            self.assertEqual(item["type"], "OBJECT")
            self.assertEqual(item["required"], ["text", "classification", "evidence_ids"])
            self.assertEqual(item["propertyOrdering"], item["required"])
            self.assertEqual(item["properties"]["text"], {"type": "STRING"})
            self.assertEqual(item["properties"]["classification"], {
                "type": "STRING", "format": "enum", "enum": list(core.CLASSIFICATIONS)})
            self.assertEqual(item["properties"]["evidence_ids"], {
                "type": "ARRAY", "minItems": "1", "items": {"type": "STRING"}})
        # JSON serialization sorts property maps, but required/propertyOrdering
        # retain the intended section and item-field sequence.
        self.assertEqual(core._gemini_schema(json.loads(core.canonical(original))), wire)
        wire["properties"]["summary"]["items"]["properties"]["classification"]["enum"].clear()
        wire["required"].clear()
        self.assertEqual(original, before)


if __name__ == "__main__":
    unittest.main()
