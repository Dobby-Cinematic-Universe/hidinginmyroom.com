"""Finite campaign bookkeeping and owned subprocess supervision contracts."""
import copy
import errno
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest import mock

from pipeline import speaker_screen_campaign as campaign
from pipeline.tests import test_speaker_screen_archive_guided as fixtures


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = []
        self.references = []
        for index in range(2):
            fixture = fixtures.ArchiveRunnerTests("runTest")
            fixture.setUp()
            self.addCleanup(fixture.doCleanups)
            fixture.orders = fixture.orders[:1]
            fixture.request["work_orders"] = fixture.request["work_orders"][:1]
            fixture.orders[0]["recording"]["media_id"] = f"campaign:{index}"
            fixture.seal()
            self.fixtures.append(fixture)
            self.references.append({"path": str(fixture.path), "sha256": fixture.sha})
        self.root = self.fixtures[0].root
        self.request = {"kind": "himr_guided_speaker_screen_campaign_request", "schema_version": 1,
            "campaign_root": str(self.root / "campaign"), "python": campaign.old_batch._python_binding(),
            "batches": self.references, "max_passes_per_batch": 8, "max_run_seconds": 3600}

    def seal(self):
        request_path = self.root / "campaign-request.json"
        campaign.screen.write_immutable(request_path, self.request)
        self.path = Path(self.request["campaign_root"]) / "manifest.json"
        self.manifest = campaign.create_campaign(str(request_path), campaign.screen.digest(self.request), str(self.path))
        self.sha = campaign.screen.digest(self.manifest)

    def report(self, batch, *, complete=True, errors=None):
        counts = dict(batch["planned_counts"])
        if complete:
            counts.update(screening_decisions_complete=counts["recordings"], sampling_plans_completed=counts["recordings"],
                completed_windows=counts["planned_windows"], baseline_inspected_windows=counts["baseline_planned_windows"],
                baseline_remaining_windows=0, targeted_inspected_windows=counts["targeted_planned_windows"],
                targeted_remaining_windows=0, uncertain=counts["recordings"])
        return {"kind": "himr_archive_guided_speaker_screen_status", "schema_version": 1, "batch_id": batch["batch_id"],
            "state": "screening_complete" if complete else "paused", "invocation_state": "finished", "counts": counts,
            "recordings": [{"index": index, "media_id": f"synthetic:{index}"} for index in range(counts["recordings"])],
            "errors": errors or []}

    def outcome(self, batch, *, complete=True, state=None, errors=None):
        report = campaign._project_child(batch, self.report(batch, complete=complete, errors=errors))
        return {"state": state or ("completed" if complete else "paused"), "exit_code": 0,
                "screening": report, "errors": errors or []}

    def run_fake(self, execute=None):
        if execute is None:
            execute = lambda manifest, batch, lock, deadline, progress: self.outcome(batch)
        with mock.patch.object(campaign, "_execute_batch", side_effect=execute):
            return campaign.run_campaign(str(self.path), self.sha)

    def test_plan_is_finite_metadata_only_and_preserves_existing_roots(self):
        before = [{str(path): path.read_bytes() for path in fixture.path.parent.rglob("*.json")} for fixture in self.fixtures]
        self.seal()
        status = campaign.status_campaign(str(self.path), self.sha)
        self.assertEqual(status["counts"]["recordings"], 2)
        self.assertEqual(status["batches"]["remaining"], 2)
        self.assertEqual(before, [{str(path): path.read_bytes() for path in fixture.path.parent.rglob("*.json")}
                                  for fixture in self.fixtures])
        self.assertFalse((self.path.parent / "campaign.lock").exists())

    def test_explicit_order_normal_pause_repetition_and_completed_skip(self):
        self.seal()
        calls = []

        def execute(manifest, batch, lock, deadline, progress):
            calls.append(batch["index"])
            return self.outcome(batch, complete=len(calls) > 1)

        completed = self.run_fake(execute)
        self.assertEqual(calls, [0, 0, 1])
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["counts"]["completed_windows"], 8)
        self.assertEqual(campaign.status_campaign(str(self.path), self.sha)["batches"]["completed"], 2)
        with mock.patch.object(campaign, "_execute_batch", side_effect=AssertionError("should skip")):
            self.assertEqual(campaign.run_campaign(str(self.path), self.sha)["state"], "completed")

    def test_failed_batch_does_not_hide_unprocessed_records_and_rest_continue(self):
        self.seal()
        calls = []

        def execute(manifest, batch, lock, deadline, progress):
            calls.append(batch["index"])
            if batch["index"] == 0:
                return {"state": "failed", "exit_code": 2, "screening": None, "errors": [{"reason": "synthetic failure"}]}
            return self.outcome(batch)

        result = self.run_fake(execute)
        self.assertEqual(calls, [0, 1])
        self.assertEqual(result["state"], "finished_with_failures")
        self.assertEqual(result["batches"]["failed"], 1)
        self.assertEqual(result["counts"]["screening_decisions_complete"], 1)
        self.assertGreater(result["counts"]["baseline_remaining_windows"], 0)

    def test_storage_error_stops_whole_campaign_without_automatic_retries(self):
        self.seal()
        calls = []

        def execute(manifest, batch, lock, deadline, progress):
            calls.append(batch["index"])
            raise OSError(errno.EIO, "Input/output error")

        result = self.run_fake(execute)
        self.assertEqual(result["state"], "storage_error")
        self.assertEqual(calls, [0])
        self.assertEqual(result["batch_statuses"][1]["state"], "not_started")
        with mock.patch.object(campaign, "_execute_batch", side_effect=AssertionError("must not retry storage")):
            self.assertEqual(campaign.run_campaign(str(self.path), self.sha)["state"], "storage_error")

    def test_uncertain_child_cleanup_stops_before_launching_another_batch(self):
        self.seal()
        result = self.run_fake(lambda *args: (_ for _ in ()).throw(campaign.ChildCleanupError("unreaped child")))
        self.assertEqual(result["state"], "supervisor_error")
        self.assertEqual(result["batch_statuses"][1]["passes_started"], 0)

    def test_pass_budget_is_finite_and_persists_across_restarts(self):
        self.request["max_passes_per_batch"] = 2
        self.seal()
        calls = []

        def execute(manifest, batch, lock, deadline, progress):
            calls.append(batch["index"])
            return self.outcome(batch, complete=False)

        result = self.run_fake(execute)
        self.assertEqual(calls, [0, 0, 1, 1])
        self.assertEqual(result["state"], "finished_with_failures")
        self.assertEqual(result["batches"]["failed"], 2)
        self.assertEqual(result["counts"]["screening_decisions_complete"], 0)
        self.run_fake(execute)
        self.assertEqual(calls, [0, 0, 1, 1])

    def test_cancellation_is_resumable_and_does_not_consume_later_batches(self):
        self.seal()
        result = self.run_fake(lambda *args: (_ for _ in ()).throw(KeyboardInterrupt()))
        self.assertEqual(result["state"], "cancelled")
        self.assertEqual(result["batch_statuses"][1]["passes_started"], 0)
        complete = self.run_fake()
        self.assertEqual(complete["state"], "completed")
        self.assertEqual(complete["batch_statuses"][0]["passes_started"], 2)

    def test_campaign_deadline_does_not_reset_when_resumed(self):
        self.seal()
        origin = {"kind": "himr_guided_speaker_screen_campaign_start", "schema_version": 1,
                  "campaign_id": self.manifest["campaign_id"], "started_unix": time.time() - 4000}
        campaign.screen.write_immutable(self.path.parent / "campaign-start.json", origin)
        with mock.patch.object(campaign, "_execute_batch", side_effect=AssertionError("expired")):
            result = campaign.run_campaign(str(self.path), self.sha)
        self.assertEqual(result["state"], "time_limit")
        self.assertEqual(result["batches"]["remaining"], 2)
        self.assertEqual(campaign.status_campaign(str(self.path), self.sha)["state"], "time_limit")

    def test_interrupted_start_spends_a_pass_and_resume_keeps_prefix(self):
        self.seal()
        batch = self.manifest["batches"][0]
        start = {"kind": "himr_guided_speaker_screen_campaign_pass", "schema_version": 1,
            "campaign_id": self.manifest["campaign_id"], "batch_id": batch["batch_id"], "index": 0,
            "pass": 1, "started_unix": time.time()}
        path = campaign._receipt_path(self.path.parent, 0, 1, "start")
        campaign.screen.write_immutable(path, start)
        original = path.read_bytes()
        result = self.run_fake()
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["batch_statuses"][0]["passes_started"], 2)
        self.assertEqual(path.read_bytes(), original)

    def test_active_status_uses_atomic_snapshot_without_racy_journal_replay(self):
        self.seal()
        states = [campaign._empty_state(batch) for batch in self.manifest["batches"]]
        snapshot = campaign._status(self.manifest, states, state="running", active={"batch_index": 0})
        campaign._atomic_snapshot(self.path.parent, snapshot)
        with campaign._locked(self.path.parent), mock.patch.object(campaign, "_read_states", side_effect=AssertionError("racy")):
            value = campaign.status_campaign(str(self.path), self.sha)
        self.assertTrue(value["supervisor_lease_active"])
        self.assertTrue(value["snapshot_is_atomic"])
        self.assertEqual(value["active"], {"batch_index": 0})

    def test_receipt_gap_and_changed_batch_fail_closed(self):
        self.seal()
        self.run_fake()
        campaign._receipt_path(self.path.parent, 0, 1, "start").unlink()
        with self.assertRaises(campaign.ScreenError):
            campaign.status_campaign(str(self.path), self.sha)
        self.fixtures[1].path.write_bytes(b"{}")
        with self.assertRaises(campaign.ScreenError):
            self.run_fake()

    def test_request_rejects_duplicate_batches_wrong_python_and_overlap(self):
        for field, value in (("batches", [self.references[0], self.references[0]]),
                             ("max_passes_per_batch", 9), ("max_run_seconds", 604801),
                             ("campaign_root", str(self.fixtures[0].path.parent)),
                             ("python", {"path": sys.executable})):
            request = {**self.request, field: value}
            with self.subTest(field=field), self.assertRaises(campaign.ScreenError):
                campaign.build_manifest(request)

    def test_child_projection_strips_embedding_payloads(self):
        self.seal()
        value = self.report(self.manifest["batches"][0])
        value["recordings"][0]["embedding"] = [1, 2, 3]
        report = campaign._project_child(self.manifest["batches"][0], value)
        self.assertNotIn("embedding", json.dumps(report))

    def test_live_telemetry_reads_only_exact_completed_result_receipts(self):
        self.seal()
        fixture = self.fixtures[0]
        before = campaign._completed_receipts(fixture.manifest)
        self.assertEqual(before["active_batch_completed_receipts"], 0)
        fixture.run_fake()
        value = campaign._completed_receipts(fixture.manifest)
        self.assertEqual(value["active_batch_completed_receipts"], 1)
        self.assertEqual(value["active_batch_completed_windows_lower_bound"], 4)
        self.assertTrue(value["active_batch_receipts_are_unverified_telemetry"])
        self.assertNotIn("embedding", json.dumps(value))
        # Telemetry does not read/replay/checkpoint-lock the running batch.
        with mock.patch.object(campaign.guided, "_read_job", side_effect=AssertionError("checkpoint replay forbidden")):
            self.assertEqual(campaign._completed_receipts(fixture.manifest), value)
        result_path = fixture.path.parent / fixture.manifest["plans"][0]["plan_id"] / "result.json"
        result_path.write_bytes(b"{}")
        invalid = campaign._completed_receipts(fixture.manifest)
        self.assertEqual(invalid["active_batch_completed_receipts"], 0)
        self.assertEqual(invalid["active_batch_receipt_read_errors"], 1)

    def test_real_owned_child_inherits_lease_and_reports_completion(self):
        self.seal()
        batch = self.manifest["batches"][0]
        report = self.report(batch)
        proof = self.root / "lease-proof.txt"
        script = self.root / "synthetic-child.py"
        script.write_text("import fcntl, json, os\n"
            + f"descriptor = os.open({str(self.path.parent / 'campaign.lock')!r}, os.O_RDWR)\n"
            + "try:\n    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
            + "except BlockingIOError:\n"
            + f"    open({str(proof)!r}, 'w').write('lease held')\n"
            + f"print({json.dumps(report)!r})\n")
        with campaign._locked(self.path.parent) as lock_fd, \
                mock.patch.object(campaign, "_command", return_value=[sys.executable, "-B", str(script)]):
            outcome = campaign._execute_batch(self.manifest, batch, lock_fd, time.monotonic() + 10, lambda pid: None)
        self.assertEqual(outcome["state"], "completed")
        self.assertEqual(proof.read_text(), "lease held")

    def test_real_child_eio_is_classified_and_excess_output_is_bounded(self):
        self.seal()
        script = self.root / "synthetic-child.py"
        script.write_text("import sys\nprint('[Errno 5] Input/output error', file=sys.stderr)\nsys.exit(2)\n")
        with campaign._locked(self.path.parent) as lock_fd, \
                mock.patch.object(campaign, "_command", return_value=[sys.executable, "-B", str(script)]):
            outcome = campaign._execute_batch(self.manifest, self.manifest["batches"][0], lock_fd,
                                              time.monotonic() + 10, lambda pid: None)
        self.assertEqual(outcome["state"], "storage_error")
        script.write_text("print('x' * 4096)\n")
        with campaign._locked(self.path.parent) as lock_fd, \
                mock.patch.object(campaign, "_command", return_value=[sys.executable, "-B", str(script)]), \
                mock.patch.object(campaign, "MAX_STDOUT", 32):
            with self.assertRaisesRegex(campaign.ScreenError, "bounded capture"):
                campaign._execute_batch(self.manifest, self.manifest["batches"][0], lock_fd,
                                        time.monotonic() + 10, lambda pid: None)

    def test_real_child_is_terminated_and_reaped_on_cancellation(self):
        self.seal()
        script = self.root / "synthetic-child.py"
        script.write_text("import time\ntime.sleep(60)\n")
        pids = []

        def interrupt(pid):
            pids.append(pid)
            raise KeyboardInterrupt

        with campaign._locked(self.path.parent) as lock_fd, \
                mock.patch.object(campaign, "_command", return_value=[sys.executable, "-B", str(script)]):
            with self.assertRaises(KeyboardInterrupt):
                campaign._execute_batch(self.manifest, self.manifest["batches"][0], lock_fd,
                                        time.monotonic() + 10, interrupt)
        self.assertEqual(len(pids), 1)
        with self.assertRaises(ProcessLookupError):
            os.kill(pids[0], 0)


if __name__ == "__main__":
    unittest.main()
