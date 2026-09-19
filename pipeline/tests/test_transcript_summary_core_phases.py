"""Creation filters must never weaken replay or eagerly build another phase."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from pipeline import transcript_summary_core as core
from pipeline.tests.test_transcript_summary_core import complete, output, source
from pipeline.tests.test_transcript_summary_synthesis import config, through_transcripts, topic


class SummaryCorePhaseTests(unittest.TestCase):
    def test_default_and_explicit_all_keep_identical_jobs_and_empty_set_only_replays(self):
        selected = [source()]
        jobs = core.initial_jobs(selected)
        results = {job["job_id"]: complete(job) for job in jobs}
        default = core.next_jobs(selected, jobs, results)
        self.assertEqual(default, core.next_jobs(selected, jobs, results, stages=set(core.STAGES)))
        self.assertEqual(default, core.next_jobs(selected, jobs, results, stages=frozenset({"transcript"})))
        self.assertEqual(core.next_jobs(selected, jobs, results, stages=set()), [])
        self.assertEqual(core.next_jobs(selected, jobs, results, stages={"chunk"}), [])
        self.assertEqual(core.initial_jobs(selected)[0]["job_id"],
                         "summaryjob_9d4fb093fc918be6b002888d1782a353")

    def test_stage_filter_rejects_nonsets_and_unknown_names(self):
        for stages in ("transcript", ["transcript"], ("transcript",), {}, True, 1,
                       {"transcripts"}, {"timeline", "bogus"}, {None}):
            with self.subTest(stages=stages), self.assertRaisesRegex(core.SummaryError, "known stage names"):
                core.next_jobs([], [], {}, stages=stages)

    def test_excluded_completed_finals_remain_available_to_included_dependents(self):
        selected = [source(date="2020-01-01")]
        settings = config(topics=[topic(["recording-1"])])
        jobs = core.initial_jobs(selected, settings)
        results = {job["job_id"]: complete(job) for job in jobs}
        synthesis = {"timeline", "yearly", "archive", "topic"}
        self.assertEqual(core.next_jobs(selected, jobs, results, settings, stages=synthesis), [])
        transcripts = core.next_jobs(selected, jobs, results, settings, stages={"transcript"})
        self.assertEqual([job["stage"] for job in transcripts], ["transcript"])
        jobs += transcripts
        results.update({job["job_id"]: complete(job) for job in transcripts})
        self.assertEqual(core.next_jobs(selected, jobs, results, settings, stages={"yearly"}), [])
        topics = core.next_jobs(selected, jobs, results, settings, stages={"topic"})
        self.assertEqual([job["stage"] for job in topics], ["topic"])
        self.assertEqual(topics[0]["dependencies"], [transcripts[0]["job_id"]])
        monthly = core.next_jobs(selected, jobs, results, settings, stages={"timeline"})
        jobs += monthly
        results.update({job["job_id"]: complete(job) for job in monthly})
        yearly = core.next_jobs(selected, jobs, results, settings, stages={"yearly"})
        self.assertEqual([job["stage"] for job in yearly], ["yearly"])
        self.assertEqual(yearly[0]["dependencies"], [monthly[0]["job_id"]])
        jobs += yearly
        results.update({job["job_id"]: complete(job) for job in yearly})
        archive = core.next_jobs(selected, jobs, results, settings, stages={"archive"})
        self.assertEqual([job["stage"] for job in archive], ["archive"])
        self.assertEqual(archive[0]["dependencies"], [yearly[0]["job_id"]])

    def test_transcript_phase_does_not_build_absent_oversized_timeline(self):
        settings = {**core.DEFAULT_CONFIG, "max_input_bytes": 12000,
                    "timeline_profile": "anthropic_sonnet_batch"}
        selected = [source(["x" * 7000])]
        jobs, results = through_transcripts(selected, settings)
        transcript = next(job for job in jobs if job["stage"] == "transcript")
        payload = output(transcript, text="a" * 1200)
        payload["summary"] *= 2
        results[transcript["job_id"]] = core.normalize_result(transcript, payload)
        with patch.object(core, "_reduce_jobs", wraps=core._reduce_jobs) as reductions:
            self.assertEqual(core.next_jobs(selected, jobs, results, settings, stages={"transcript"}), [])
        self.assertTrue(reductions.call_args_list)
        self.assertEqual({call.args[0] for call in reductions.call_args_list}, {"transcript"})
        with self.assertRaisesRegex(core.SummaryError, "full cited excerpts"):
            core.next_jobs(selected, jobs, results, settings)
        with self.assertRaisesRegex(core.SummaryError, "full cited excerpts"):
            core.next_jobs(selected, jobs, results, settings, stages={"timeline"})

    def test_transcript_phase_does_not_build_absent_oversized_broader_summary(self):
        settings = config(max_input_bytes=12000, topics=[topic(["recording-1"])])
        selected = [source(["x" * 7000], date="2020-01-01")]
        jobs, results = through_transcripts(selected, settings)
        transcript = next(job for job in jobs if job["stage"] == "transcript")
        payload = output(transcript, text="a" * 1200)
        payload["summary"] *= 2
        results[transcript["job_id"]] = core.normalize_result(transcript, payload)
        monthly = core.next_jobs(selected, jobs, results, settings, stages={"timeline"})
        jobs += monthly
        payload = output(monthly[0], text="a" * 1200)
        payload["summary"] *= 2
        results[monthly[0]["job_id"]] = core.normalize_result(monthly[0], payload)
        with patch.object(core, "_reduce_jobs", wraps=core._reduce_jobs) as reductions:
            self.assertEqual(core.next_jobs(selected, jobs, results, settings, stages={"transcript"}), [])
        self.assertEqual({call.args[0] for call in reductions.call_args_list}, {"transcript", "timeline"})
        for stages in ({"yearly"}, {"topic"}):
            with self.subTest(stages=stages), self.assertRaisesRegex(core.SummaryError, "full cited excerpts"):
                core.next_jobs(selected, jobs, results, settings, stages=stages)

    def test_excluded_broader_job_and_result_still_receive_full_replay(self):
        selected = [source(date="2020-01-01")]
        settings = config()
        jobs, results = through_transcripts(selected, settings)
        monthly = core.next_jobs(selected, jobs, results, settings, stages={"timeline"})
        jobs += monthly
        results.update({job["job_id"]: complete(job) for job in monthly})
        yearly = core.next_jobs(selected, jobs, results, settings, stages={"yearly"})[0]
        evidence = deepcopy(yearly["evidence"])
        evidence[0]["text"] = "An invented replacement summary."
        forged = core.make_job("yearly", yearly["scope"], evidence, yearly["dependencies"], settings)
        with self.assertRaisesRegex(core.SummaryError, "dependency replay"):
            core.next_jobs(selected, jobs + [forged], results, settings, stages={"transcript"})
        altered_result = complete(yearly)
        altered_result["semantics"]["fact_checked"] = True
        with self.assertRaisesRegex(core.SummaryError, "result replay"):
            core.next_jobs(selected, jobs + [yearly], {**results, yearly["job_id"]: altered_result},
                           settings, stages={"transcript"})

    def test_excluded_partial_level_is_replayed_without_returning_missing_groups(self):
        selected = [source(recording=f"recording-{i}", date="2020-03-01") for i in range(14)]
        settings = {**core.DEFAULT_CONFIG, "max_input_bytes": 12000}
        jobs, results = through_transcripts(selected, settings)
        for job in jobs:
            if job["stage"] == "transcript":
                results[job["job_id"]] = complete(job, text="a" * 1200)
        groups = core.next_jobs(selected, jobs, results, settings, stages={"timeline"})
        self.assertGreater(len(groups), 1)
        partial_jobs = jobs + groups[:1]
        partial_results = {**results, groups[0]["job_id"]: complete(groups[0])}
        self.assertEqual(core.next_jobs(selected, partial_jobs, partial_results, settings,
                                        stages={"transcript"}), [])
        self.assertEqual(core.next_jobs(selected, partial_jobs, partial_results, settings,
                                        stages={"timeline"}), groups[1:])
        forged = core.make_job("timeline", {**groups[0]["scope"], "level": 2, "final": True},
                               core._result_evidence(partial_results[groups[0]["job_id"]]),
                               [groups[0]["job_id"]], settings)
        with self.assertRaisesRegex(core.SummaryError, "complete parent level"):
            core.next_jobs(selected, partial_jobs + [forged], partial_results, settings, stages=set())

    def test_excluded_jobs_cannot_skip_a_dependency_level_or_source_selection(self):
        selected = [source(date="2020-01-01")]
        jobs, results = through_transcripts(selected, core.DEFAULT_CONFIG)
        monthly = core.next_jobs(selected, jobs, results)[0]
        forged = core.make_job("timeline", {**monthly["scope"], "level": 2},
                               monthly["evidence"], monthly["dependencies"])
        with self.assertRaisesRegex(core.SummaryError, "missing dependency level"):
            core.next_jobs(selected, jobs + [forged], results, stages=set())
        foreign = core.make_job("timeline", {**monthly["scope"], "period": "2020-02"},
                                monthly["evidence"], monthly["dependencies"])
        with self.assertRaisesRegex(core.SummaryError, "outside selected sources or stages"):
            core.next_jobs(selected, jobs + [foreign], results, stages={"transcript"})


if __name__ == "__main__":
    unittest.main()
