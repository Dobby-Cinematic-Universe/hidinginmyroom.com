"""Persistent validation survives a fresh process, without caching paid state."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

from pipeline import transcript_summary as r
from pipeline import transcript_summary_campaign as campaign
from pipeline import transcript_summary_checkpoint as checkpoint
from pipeline import transcript_summary_classification as classification
from pipeline import transcript_summary_recovery as recovery
from pipeline.tests import test_transcript_summary as fixtures
from pipeline.tests import test_transcript_summary_recovery as recovery_fixtures
from pipeline.tests import test_transcript_summary_classification as classification_fixtures


class CheckpointTests(unittest.TestCase):
    file = fixtures.SummaryRunnerTests.file
    source = fixtures.SummaryRunnerTests.source
    producer = recovery_fixtures.RecoveryTests.producer
    finish_ready = recovery_fixtures.RecoveryTests.finish_ready

    def setUp(self):
        fixtures.SummaryRunnerTests.setUp(self)
        self.enterContext(mock.patch("builtins.print"))
        self.addCleanup(checkpoint.clear_caches)

    def prepare(self, *, unknown_usage=False):
        self.producer(unknown_usage=unknown_usage)
        with mock.patch("builtins.print"):
            recovered = recovery.prepare_campaign(self.parent, self.code, self.root / "admissions",
                self.root / "recovered.json", self.root / "recovered")
        self.manifest_ref = recovered["manifest"]
        self.manifest = r.read(self.manifest_ref)
        self.admission_ref = self.manifest["recovery"]["imports"]["0"]
        self.admission = r.read(self.admission_ref)
        self.original_implementation = campaign.implementation()
        with mock.patch.object(r, "api_client", side_effect=AssertionError("offline")), \
                mock.patch.object(r, "submit_wave", side_effect=AssertionError("no paid requests")):
            prepared = checkpoint.prepare(self.manifest_ref, self.root / "checkpoint" / "validated.json")
        self.ref = prepared["checkpoint"]
        self.assertEqual(prepared["new_paid_requests"], 0)
        self.assertEqual(campaign.implementation(), self.original_implementation)
        return prepared

    def replace(self, ref, value=None):
        raw = r.read_bytes(ref) if value is None else r.canonical(value)
        self.counter += 1
        path = self.root / ("replacement-" + str(self.counter))
        r.put_bytes(path, raw)
        os.replace(path, ref["path"])

    def test_clean_restart_uses_verified_results_without_build_or_paid_calls(self):
        prepared = self.prepare()
        self.assertEqual(prepared["cached_admissions"], 1)
        checkpoint.clear_caches()
        with mock.patch.object(recovery, "_build", side_effect=AssertionError("must not rebuild")), \
                mock.patch.object(r, "api_client", side_effect=AssertionError("offline")):
            result = checkpoint.check(self.ref)
        self.assertEqual(result["restored_admissions"], 1)
        self.assertEqual(result["cold_admissions"], 0)
        self.assertEqual(result["validation"], r.read(self.ref)["validation"])
        self.assertEqual(result["new_paid_requests"], 0)
        self.assertEqual(campaign.implementation(), self.original_implementation)
        self.assertFalse(Path(self.manifest["state_root"]).exists())

    def test_independent_process_restores_the_same_checkpoint(self):
        self.prepare()
        code = """import json,sys
from unittest import mock
from pipeline import transcript_summary_checkpoint as c, transcript_summary_recovery as r
from pipeline import transcript_summary as runner
with mock.patch.object(r,'_build',side_effect=AssertionError('cold rebuild')), mock.patch.object(runner,'api_client',side_effect=AssertionError('network')):
    print(json.dumps(c.check({'path':sys.argv[1],'sha256':sys.argv[2]})))
"""
        completed = subprocess.run([sys.executable, "-B", "-c", code, self.ref["path"], self.ref["sha256"]],
            cwd=r.ROOT, capture_output=True, text=True, timeout=60, check=True)
        self.assertEqual(json.loads(completed.stdout)["restored_admissions"], 1)

    def test_unchanged_bytes_with_changed_inode_and_restored_mtime_replay(self):
        self.prepare()
        proof = self.admission["producer_plan"]
        before = os.stat(proof["path"])
        self.replace(proof)
        os.utime(proof["path"], ns=(before.st_atime_ns, before.st_mtime_ns))
        _, restored = checkpoint.restore(self.ref)
        self.assertTrue(restored["producer_inventory_unchanged"])
        self.assertEqual(restored["cold_admissions"], 1)
        with mock.patch.object(recovery, "_build", wraps=recovery._build) as build:
            result = checkpoint.check(self.ref)
        self.assertGreater(build.call_count, 0)
        self.assertEqual(result["validation"], r.read(self.ref)["validation"])

    def test_same_inode_and_mtime_cannot_hide_in_place_write(self):
        self.prepare()
        proof = self.admission["producer_plan"]
        path = Path(proof["path"])
        before, raw = path.stat(), r.read_bytes(proof)
        path.chmod(0o600)
        path.write_bytes(raw)
        path.chmod(0o400)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertEqual(path.stat().st_ino, before.st_ino)
        self.assertEqual(path.stat().st_mtime_ns, before.st_mtime_ns)
        _, result = checkpoint.restore(self.ref)
        self.assertEqual(result["cold_admissions"], 1)
        self.assertEqual(checkpoint.check(self.ref)["validation"], r.read(self.ref)["validation"])

    def test_changed_proof_is_not_admitted_or_resubmitted(self):
        self.prepare()
        proof = self.admission["producer_plan"]
        value = r.read(proof)
        value["plan_id"] = "summaryplan_" + "a" * 32
        self.replace(proof, value)
        with mock.patch.object(r, "submit_wave") as submit, self.assertRaises(r.Error):
            checkpoint.run(self.ref, allow_paid_api=True)
        submit.assert_not_called()

    def test_new_producer_wave_invalidates_even_unchanged_existing_proofs(self):
        self.prepare()
        root = Path(self.admission["producer_plan"]["path"]).parent / "waves"
        r.mkdir(root / ("summarywave_" + "a" * 32))
        _, result = checkpoint.restore(self.ref)
        self.assertFalse(result["producer_inventory_unchanged"])
        self.assertEqual(result["restored_admissions"], 0)
        self.assertEqual(result["cold_admissions"], 1)
        # An inert directory is not a paid attempt; the full validator decides.
        with mock.patch.object(recovery, "_build", wraps=recovery._build) as build:
            checkpoint.check(self.ref)
        self.assertGreater(build.call_count, 0)

    def test_new_receipt_in_existing_wave_invalidates_inventory(self):
        self.prepare()
        root = Path(self.admission["producer_plan"]["path"]).parent / "waves"
        folder = next(root.iterdir())
        r.put(folder / "reconciliation-response.json", {"new": "unvalidated receipt"})
        _, result = checkpoint.restore(self.ref)
        self.assertEqual(result["restored_admissions"], 0)
        self.assertFalse(result["producer_inventory_unchanged"])
        with self.assertRaisesRegex(r.Error, "does not replay"):
            checkpoint.check(self.ref)

    def test_live_campaign_changes_do_not_invalidate_closed_producer_checkpoint(self):
        self.prepare()
        root = Path(self.manifest["state_root"])
        r.mkdir(root)
        r.put(root / "status.json", {"untrusted_status": "not a cache authority"})
        _, result = checkpoint.restore(self.ref)
        self.assertEqual(result["restored_admissions"], 1)

    def test_checkpoint_digest_and_implementation_are_required(self):
        self.prepare()
        value = r.read(self.ref)
        changed = deepcopy(value)
        changed["implementation"] = {}
        self.replace(self.ref, changed)
        with self.assertRaisesRegex(r.Error, "digest differs"):
            checkpoint.restore(self.ref)
        with self.assertRaisesRegex(r.Error, "implementation"):
            checkpoint.restore(r.binding(Path(self.ref["path"])))

    def test_malformed_or_duplicate_entries_never_partially_restore(self):
        self.prepare()
        value = r.read(self.ref)
        value["entries"].append(deepcopy(value["entries"][0]))
        bad = self.file(value)
        with self.assertRaisesRegex(r.Error, "duplicate"):
            checkpoint.restore(bad)
        self.assertFalse(recovery._CACHE)
        self.assertFalse(classification._CACHE)

    def test_audit_totals_are_not_used_as_budget_authority(self):
        self.prepare(unknown_usage=True)
        value = r.read(self.ref)
        value["validation"]["prior_unsettled_hold_microusd"] = 0
        result = checkpoint.check(self.file(value))
        self.assertEqual(result["validation"]["prior_unsettled_hold_microusd"],
                         self.manifest["recovery"]["prior_unsettled_hold_microusd"])

    def test_symlinked_proof_is_rejected_even_with_matching_bytes(self):
        self.prepare()
        proof = self.admission["producer_plan"]
        target = self.file(r.read(proof))
        path = Path(proof["path"])
        path.unlink()
        path.symlink_to(target["path"])
        with self.assertRaises((RuntimeError, OSError)):
            checkpoint.check(self.ref)

    def test_changed_manifest_budget_cannot_use_checkpoint(self):
        self.prepare()
        changed = deepcopy(self.manifest)
        changed["budget_microusd"] += 1
        self.replace(self.manifest_ref, changed)
        with mock.patch.object(r, "submit_wave") as submit, self.assertRaisesRegex(r.Error, "digest differs"):
            checkpoint.run(self.ref, allow_paid_api=True)
        submit.assert_not_called()

    def test_changed_source_is_checked_even_on_warm_admission(self):
        self.prepare()
        source_ref = self.manifest["shards"][0][0]["transcript"]
        changed = r.read(source_ref)
        changed["segments"][0]["text"] = "Changed transcript."
        self.replace(source_ref, changed)
        with self.assertRaises(RuntimeError):
            checkpoint.check(self.ref)

    def test_unknown_usage_holds_survive_warm_restart(self):
        self.prepare(unknown_usage=True)
        result = checkpoint.check(self.ref)
        self.assertEqual(result["validation"]["prior_usage_estimate_microusd"], 0)
        self.assertEqual(result["validation"]["prior_unsettled_hold_microusd"],
                         self.manifest["recovery"]["prior_unsettled_hold_microusd"])
        self.assertGreater(result["validation"]["prior_unsettled_hold_microusd"], 0)

    def test_original_workspace_lock_still_prevents_duplicate_runner(self):
        self.prepare()
        root = Path(self.manifest["state_root"])
        r.mkdir(root)
        with r.locked(root), mock.patch.object(r, "submit_wave") as submit:
            with self.assertRaisesRegex(r.Error, "another summary command"):
                checkpoint.run(self.ref, allow_paid_api=True)
        submit.assert_not_called()

    def test_paid_flag_required_and_checkpoint_cannot_overlap_campaign(self):
        self.prepare()
        with mock.patch.object(campaign, "run") as run, self.assertRaisesRegex(r.Error, "allow-paid-api"):
            checkpoint.run(self.ref)
        run.assert_not_called()
        with self.assertRaisesRegex(r.Error, "overlaps"):
            checkpoint.prepare(self.manifest_ref, Path(self.manifest["state_root"]) / "cache.json")
        with self.assertRaisesRegex(r.Error, "fresh"):
            checkpoint.prepare(self.manifest_ref, self.ref["path"])

    def test_continuation_canary_restores_both_cache_types_without_rebuilding(self):
        case = classification_fixtures.ContinuationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.setup_canary()
        manifest_ref = case.prepare()["manifest"]
        ref = checkpoint.prepare(manifest_ref, case.root / "checkpoint" / "validated.json")["checkpoint"]
        checkpoint.clear_caches()
        with mock.patch.object(recovery, "_build", side_effect=AssertionError("chunk rebuild")), \
                mock.patch.object(classification, "build", side_effect=AssertionError("canary rebuild")):
            result = checkpoint.check(ref)
        self.assertEqual(result["restored_admissions"], 2)
        self.assertEqual(result["cold_admissions"], 0)


if __name__ == "__main__":
    unittest.main()
