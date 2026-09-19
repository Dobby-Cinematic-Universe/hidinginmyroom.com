"""Explicit transcript/synthesis phases with synthetic, network-free providers."""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from pipeline import transcript_summary as runner
from pipeline.tests import test_transcript_summary as existing
from pipeline.tests import test_transcript_summary_sonnet as sonnet


class SummaryPhaseTests(unittest.TestCase):
    setUp = sonnet.SonnetRunnerTests.setUp
    file = sonnet.SonnetRunnerTests.file
    source = sonnet.SonnetRunnerTests.source
    plan = sonnet.SonnetRunnerTests.plan
    submit = sonnet.SonnetRunnerTests.submit
    finish = sonnet.SonnetRunnerTests.finish

    def wave(self, prepared):
        self.assertIn(prepared["state"], ("prepared", "already_prepared"))
        folder = Path(self.request["state_root"]) / "waves" / prepared["wave_id"]
        wave = runner.read(runner.binding(folder / "wave.json"))
        service = (sonnet.AnthropicService(wave) if wave["provider"] == "anthropic"
                   else existing.Service(wave, self.creation["plan_id"]))
        return wave, service, folder

    def snapshot(self):
        root = Path(self.request["state_root"])
        return {str(path.relative_to(root)): path.read_bytes()
                for path in root.rglob("*") if path.is_file()}

    def ready(self):
        plan, selected = runner.load_plan(*self.args)
        state = runner.load_state(plan, selected)
        return plan, state, runner.ready_jobs(selected, state["jobs"], state["results"],
                                               self.request["config"])

    def partial_transcripts(self, *, empty=False, same_provider=False):
        selected = [self.source("rec-1", date="2025-12-20"),
                    self.source("rec-2", date="2026-08-20")]
        if empty:
            selected.append(self.source("rec-empty", text=None, date=None))
        self.plan(selected, broader=not same_provider,
                  config={"timeline_profile": "gemini_flash_batch"} if same_provider else None)
        wave, service, _ = self.wave(runner.prepare_plan(*self.args, phase="transcripts"))
        self.assertEqual({job["stage"] for job in wave["jobs"]}, {"chunk"})
        self.finish(wave, service)
        plan, state, ready = self.ready()
        first = next(job for job in ready if job["stage"] == "transcript")
        prepared = runner.create_wave(plan, state, [first])
        wave, service, _ = self.wave(prepared)
        self.finish(wave, service)
        _, _, ready = self.ready()
        self.assertIn("transcript", {job["stage"] for job in ready})
        self.assertIn("timeline", {job["stage"] for job in ready})

    def test_transcript_phase_pauses_then_synthesis_finishes_without_duplicate_work(self):
        self.plan()
        transcript_jobs = []
        for _ in range(8):
            prepared = runner.prepare_plan(*self.args, phase="transcripts")
            if prepared["state"] == "phase_complete":
                break
            wave, service, _ = self.wave(prepared)
            self.assertEqual(wave["provider"], "gemini")
            self.assertTrue(all(job["stage"] in {"chunk", "transcript"} for job in wave["jobs"]))
            transcript_jobs.extend(job["job_id"] for job in wave["jobs"])
            self.finish(wave, service)
        self.assertEqual(prepared["state"], "phase_complete")
        self.assertEqual(len(transcript_jobs), len(set(transcript_jobs)))
        before = self.snapshot()
        self.assertEqual(runner.prepare_plan(*self.args, phase="transcripts")["state"], "phase_complete")
        self.assertEqual(self.snapshot(), before)
        status = runner.status_plan(*self.args, phase="transcripts")
        self.assertEqual(status["phase"], "transcripts")
        self.assertEqual(status["state"], "completed")
        self.assertTrue(status["transcript_phase_complete"])
        self.assertEqual(status["transcript_summaries_remaining"], 0)
        self.assertEqual(status["ready_jobs"], 0)
        self.assertFalse(status["synthesis_phase_complete"])
        review = runner.export_plan(*self.args, phase="transcripts")
        document = runner.read(review["artifact"])
        self.assertEqual(document["phase"], "transcripts")
        self.assertTrue(document["phase_complete"])
        self.assertFalse(document["complete"])
        self.assertEqual({result["stage"] for result in document["results"]}, {"transcript"})
        stages = set()
        for _ in range(12):
            prepared = runner.prepare_plan(*self.args, phase="synthesis")
            if prepared["state"] == "phase_complete":
                break
            wave, service, _ = self.wave(prepared)
            self.assertEqual(wave["provider"], "anthropic")
            stages.update(job["stage"] for job in wave["jobs"])
            self.finish(wave, service)
        self.assertEqual(prepared["state"], "phase_complete")
        self.assertEqual(stages, {"timeline", "yearly", "archive", "topic"})
        self.assertTrue(runner.status_plan(*self.args, phase="synthesis")["synthesis_phase_complete"])
        self.assertEqual(runner.prepare_plan(*self.args)["state"], "no_ready_jobs")
        self.assertTrue(runner.read(runner.export_plan(*self.args)["artifact"])["complete"])

    def test_early_month_and_topic_never_enter_transcript_phase(self):
        self.partial_transcripts()
        _, _, ready = self.ready()
        self.assertIn("topic", {job["stage"] for job in ready})
        status = runner.status_plan(*self.args, phase="transcripts")
        self.assertEqual(status["transcript_summaries_remaining"], 1)
        self.assertEqual(status["ready_jobs"], 1)
        wave, service, _ = self.wave(runner.prepare_plan(*self.args, phase="transcripts"))
        self.assertEqual({job["stage"] for job in wave["jobs"]}, {"transcript"})
        self.submit(wave, service)
        before = self.snapshot()
        self.assertEqual(runner.prepare_plan(*self.args, phase="transcripts")["state"], "no_ready_jobs")
        self.assertEqual(self.snapshot(), before)
        plan, selected = runner.load_plan(*self.args)
        self.assertTrue(all(item["provider"] == "gemini"
                            for item in runner.load_state(plan, selected)["waves"]))

    def test_synthesis_waits_for_partial_transcripts_and_ignores_empty_source(self):
        self.partial_transcripts(empty=True)
        before = self.snapshot()
        status = runner.status_plan(*self.args, phase="synthesis")
        self.assertEqual(status["phase"], "synthesis")
        self.assertFalse(status["transcript_phase_complete"])
        self.assertEqual(status["transcript_summaries_remaining"], 1)
        self.assertEqual(status["empty_transcripts"], 1)
        self.assertEqual(status["ready_jobs"], 0)
        self.assertEqual(runner.prepare_plan(*self.args, phase="synthesis")["state"], "waiting_for_transcripts")
        self.assertEqual(self.snapshot(), before)
        wave, service, _ = self.wave(runner.prepare_plan(*self.args, phase="transcripts"))
        self.finish(wave, service)
        self.assertTrue(runner.status_plan(*self.args, phase="synthesis")["transcript_phase_complete"])
        wave, _, _ = self.wave(runner.prepare_plan(*self.args, phase="synthesis"))
        self.assertEqual(wave["provider"], "anthropic")

    def test_all_empty_transcripts_complete_both_phases_without_requests(self):
        self.plan([self.source("rec-1", text=None), self.source("rec-empty", text=" \n\t")],
                  broader=False, config={"broader_synthesis": {"profile": sonnet.PROFILE,
                                         "yearly": True, "archive": True, "topics": []}})
        for phase in ("transcripts", "synthesis"):
            with self.subTest(phase=phase):
                before = self.snapshot()
                self.assertEqual(runner.prepare_plan(*self.args, phase=phase)["state"], "phase_complete")
                status = runner.status_plan(*self.args, phase=phase)
                self.assertEqual(status["state"], "completed")
                self.assertEqual(status["transcript_summaries_remaining"], 0)
                self.assertEqual(status["ready_jobs"], 0)
                self.assertEqual(self.snapshot(), before)

    def test_unrelated_prepared_synthesis_wave_is_skipped_then_reused(self):
        self.partial_transcripts()
        plan, state, ready = self.ready()
        prepared = runner.create_wave(plan, state, [job for job in ready if job["stage"] != "transcript"])
        synthesis, _, _ = self.wave(prepared)
        transcript, service, _ = self.wave(runner.prepare_plan(*self.args, phase="transcripts"))
        self.assertNotEqual(transcript["wave_id"], synthesis["wave_id"])
        self.assertEqual({job["stage"] for job in transcript["jobs"]}, {"transcript"})
        status = runner.status_plan(*self.args, phase="transcripts")
        self.assertEqual(status["prepared_waves"], [transcript["wave_id"]])
        self.finish(transcript, service)
        self.assertEqual(runner.prepare_plan(*self.args, phase="transcripts")["state"], "phase_complete")
        reused = runner.prepare_plan(*self.args, phase="synthesis")
        self.assertEqual(reused["state"], "already_prepared")
        self.assertEqual(reused["wave_id"], synthesis["wave_id"])

    def test_mixed_prepared_wave_is_not_admitted_to_transcript_phase(self):
        self.partial_transcripts(same_provider=True)
        plan, state, ready = self.ready()
        prepared = runner.create_wave(plan, state, ready)
        wave, _, _ = self.wave(prepared)
        self.assertEqual({job["stage"] for job in wave["jobs"]}, {"transcript", "timeline"})
        before = self.snapshot()
        with self.assertRaises(runner.Error):
            runner.prepare_plan(*self.args, phase="transcripts")
        self.assertEqual(self.snapshot(), before)

    def test_submit_phase_mismatch_and_incomplete_transcripts_precede_paid_intent(self):
        self.partial_transcripts()
        plan, state, ready = self.ready()
        prepared = runner.create_wave(plan, state, [job for job in ready if job["stage"] != "transcript"])
        wave, service, folder = self.wave(prepared)
        with self.assertRaises(runner.Error):
            runner.submit_wave(*self.args, wave["wave_id"], phase="transcripts",
                               allow_paid_api=True, client=service.api)
        result = runner.submit_wave(*self.args, wave["wave_id"], phase="synthesis",
                                    allow_paid_api=True, client=service.api)
        self.assertEqual(result["state"], "waiting_for_transcripts")
        self.assertEqual(service.calls, [])
        self.assertFalse((folder / "submit-intent.json").exists())
        transcript, transcript_service, _ = self.wave(runner.prepare_plan(*self.args, phase="transcripts"))
        self.finish(transcript, transcript_service)
        self.assertEqual(runner.submit_wave(*self.args, wave["wave_id"], phase="synthesis",
                         allow_paid_api=True, client=service.api)["state"], "submitted")

    def test_retry_filters_failed_mixed_wave_to_requested_phase(self):
        self.partial_transcripts(same_provider=True)
        plan, state, ready = self.ready()
        mixed, service, _ = self.wave(runner.create_wave(plan, state, ready))
        for row in service.rows:
            row.update(response=None, error={"code": 13})
        self.assertEqual(self.finish(mixed, service)["needs_review"], 2)
        before = self.snapshot()
        self.assertEqual(runner.prepare_plan(*self.args, retry_wave=mixed["wave_id"],
                         phase="synthesis")["state"], "waiting_for_transcripts")
        self.assertEqual(self.snapshot(), before)
        retry, retry_service, _ = self.wave(runner.prepare_plan(*self.args, retry_wave=mixed["wave_id"],
                                                               phase="transcripts"))
        self.assertEqual({job["stage"] for job in retry["jobs"]}, {"transcript"})
        self.finish(retry, retry_service)
        synthesis, _, _ = self.wave(runner.prepare_plan(*self.args, retry_wave=mixed["wave_id"],
                                                       phase="synthesis"))
        self.assertEqual({job["stage"] for job in synthesis["jobs"]}, {"timeline"})

    def test_cli_phase_flags_dispatch_and_default_remains_all(self):
        self.plan(approved=False)
        common = ["--manifest", self.args[0], "--expected-sha256", self.args[1]]
        with patch.dict(os.environ, {}, clear=True), patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(runner.main(["status", *common, "--phase", "transcripts"]), 0)
            self.assertEqual(json.loads(output.getvalue())["phase"], "transcripts")
        with patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(runner.main(["status", *common]), 0)
            self.assertEqual(json.loads(output.getvalue())["phase"], "all")
        fake_wave = "summarywave_" + "f" * 32
        for command, function in (("prepare", "prepare_plan"), ("retry", "prepare_plan"),
                                  ("submit", "submit_wave"), ("export", "export_plan")):
            with self.subTest(command=command), patch.object(runner, function,
                    return_value={"state": "waiting_for_transcripts"}) as invoked, patch(
                    "sys.stdout", new_callable=io.StringIO):
                args = [command, *common, "--phase", "synthesis"]
                if command in {"retry", "submit"}:
                    args += ["--wave", fake_wave]
                self.assertEqual(runner.main(args), 0)
                self.assertEqual(invoked.call_args.kwargs["phase"], "synthesis")


if __name__ == "__main__":
    unittest.main()
