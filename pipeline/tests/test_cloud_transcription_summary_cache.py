"""Retained initial-job reuse and bounded scoped memoization; network forbidden."""
from contextvars import Context
from copy import deepcopy
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_summary as worker
from pipeline import cloud_transcription_summary_cache as cache
from pipeline import transcript_summary as r
from pipeline import transcript_summary_fast_initial as fast
from pipeline.tests import test_cloud_transcription_summary as fixtures


class SummaryCacheTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.prepare()
        self.manifest = worker.load_manifest(self.case.ref)
        self.config = self.manifest["config"]
        self.original = r.core.initial_jobs

    def source(self, available=None):
        selected = available or self.case.available[0]
        spec = worker._request(self.manifest, selected)["sources"][0]
        return r.sources_module.normalize_source(spec)

    def record(self, *, with_wave=True, selected=None, partial=False):
        entry = worker._ensure_record(self.manifest, selected or self.case.available[0])
        plan, sources = r.load_plan(entry["plan"]["path"], entry["plan"]["sha256"])
        expected = fast.initial_jobs(sources, self.config)
        if with_wave:
            state = r.load_state(plan, sources)
            r.create_wave(plan, state, expected[:1] if partial else expected)
        return entry, plan, sources, expected

    @staticmethod
    def rewrite(path, value):
        path.chmod(0o600)
        path.write_bytes(r.canonical(value))
        path.chmod(0o400)

    def test_exact_retained_jobs_seed_without_any_boundary_reconstruction(self):
        entry, old_plan, sources, expected = self.record()
        with patch.object(cache, "_select_builder", return_value=lambda *_: self.fail("retained boundary reconstruction")):
            with cache.scope(self.case.ref) as state:
                plan, current_sources = r.load_plan(entry["plan"]["path"], entry["plan"]["sha256"])
                actual = r.core.initial_jobs(current_sources, self.config)
                self.assertEqual(r.canonical(actual), r.canonical(expected))
                retained = r.load_state(plan, current_sources)
                r.status_from_state(plan, current_sources, retained, phase="transcripts")
                self.assertEqual(state.statistics()["retained_seed_hits"], 1)
                self.assertGreater(state.statistics()["hits"], 0)
                self.assertEqual(state.statistics()["fresh_builds"], 0)
        self.assertEqual(self.case.client.created, [])
        self.assertIs(r.core.initial_jobs, self.original)

    def test_more_than_sixteen_sources_do_not_thrash_default_cache(self):
        sources = [self.source(self.case.source(recording="recording-" + str(index))) for index in range(18)]
        with cache.scope(self.case.ref) as state:
            first = [r.core.initial_jobs([source], self.config) for source in sources]
            second = [r.core.initial_jobs([source], self.config) for source in sources]
            self.assertEqual(first, second)
            self.assertEqual(state.statistics()["fresh_builds"], 18)
            self.assertEqual(state.statistics()["hits"], 18)
            self.assertEqual(state.statistics()["cache_entries"], 18)
            self.assertEqual(state.statistics()["evictions"], 0)

    def test_missing_frontier_falls_back_once_then_returns_serialized_copies(self):
        entry, plan, sources, expected = self.record(with_wave=False)
        with cache.scope(self.case.ref) as state:
            first = r.core.initial_jobs(sources, self.config)
            first[0]["evidence"][0]["text"] = "Caller mutation"
            second = r.core.initial_jobs(sources, self.config)
            self.assertEqual(second, expected)
            self.assertEqual(state.statistics()["fresh_builds"], 1)
            self.assertEqual(state.statistics()["hits"], 1)
            self.assertEqual(state.statistics()["retained_seed_hits"], 0)

    def test_partial_valid_frontier_falls_back_to_exact_full_initial_ids(self):
        selected = self.case.source(recording="long-recording", text="Daniel walked outside. " * 3000)
        entry, plan, sources, expected = self.record(selected=selected, partial=True)
        self.assertGreater(len(expected), 1)
        with cache.scope(self.case.ref) as state:
            actual = r.core.initial_jobs(sources, self.config)
            self.assertEqual(actual, expected)
            self.assertEqual([job["job_id"] for job in actual], plan["initial_job_ids"])
            self.assertEqual(state.statistics()["fresh_builds"], 1)

    def test_changed_retained_requests_reject_instead_of_falling_back(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        wire = next((root / "waves").glob("*/requests.bin"))
        wire.chmod(0o600)
        wire.write_bytes(b"changed retained paid request")
        wire.chmod(0o400)
        with patch.object(cache, "_select_builder", return_value=lambda *_: self.fail("corruption fallback")):
            with cache.scope(self.case.ref):
                with self.assertRaises(RuntimeError):
                    r.core.initial_jobs(sources, self.config)

    def test_changed_retained_wave_identity_rejects(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        path = next((root / "waves").glob("*/wave.json"))
        wave = r.read(r.binding(path))
        wave["jobs"][0]["evidence"][0]["text"] = "Injected text"
        self.rewrite(path, wave)
        with cache.scope(self.case.ref):
            with self.assertRaises(RuntimeError):
                r.core.initial_jobs(sources, self.config)

    def test_changed_normalized_source_snapshot_rejects(self):
        entry, plan, sources, expected = self.record()
        path = Path(plan["request_value"]["state_root"]) / "sources" / (sources[0]["source_id"] + ".json")
        stored = r.read(r.binding(path))
        stored["segments"][0]["text"] = "Injected normalized text"
        self.rewrite(path, stored)
        with cache.scope(self.case.ref):
            with self.assertRaises(RuntimeError):
                r.core.initial_jobs(sources, self.config)

    def test_changed_source_and_configuration_do_not_hit_prior_cache_entry(self):
        first_source = self.source(self.case.source(recording="not-yet-prepared", text="First source text."))
        changed_source = self.source(self.case.source(recording="not-yet-prepared", text="Different source text."))
        changed_config = r.core.normalize_config({**self.config, "max_output_tokens": self.config["max_output_tokens"] - 1})
        with cache.scope(self.case.ref) as state:
            first = r.core.initial_jobs([first_source], self.config)
            second = r.core.initial_jobs([changed_source], self.config)
            third = r.core.initial_jobs([changed_source], changed_config)
            self.assertNotEqual(first, second)
            self.assertNotEqual(second, third)
            self.assertEqual(state.statistics()["fresh_builds"], 3)
            self.assertEqual(state.statistics()["hits"], 0)

    def test_changed_core_is_rejected_and_scope_restores_original(self):
        with self.assertRaises(cache.CacheError):
            with cache.scope(self.case.ref):
                with patch.object(cache, "_core_sha256", return_value="0" * 64):
                    r.core.initial_jobs([self.source()], self.config)
        self.assertIs(r.core.initial_jobs, self.original)
        self.assertIsNone(cache.statistics())

    def test_scope_isolation_nesting_and_exception_restoration(self):
        with self.assertRaisesRegex(ValueError, "synthetic"):
            with cache.scope(self.case.ref):
                with self.assertRaises(cache.CacheError):
                    with cache.scope(self.case.ref):
                        self.fail("nested scope accepted")
                with patch.object(cache, "_ORIGINAL_INITIAL_JOBS", return_value=["original path"]) as original:
                    self.assertEqual(Context().run(r.core.initial_jobs, [], self.config), ["original path"])
                    original.assert_called_once()
                raise ValueError("synthetic")
        self.assertIs(r.core.initial_jobs, self.original)
        with cache.scope(self.case.ref):
            pass  # Lock is reusable after failures, never left globally installed.

    def test_eviction_and_oversize_bounds_are_enforced(self):
        first = self.source(self.case.source(recording="new-a"))
        second = self.source(self.case.source(recording="new-b"))
        with cache.scope(self.case.ref, max_entries=1) as state:
            r.core.initial_jobs([first], self.config)
            r.core.initial_jobs([second], self.config)
            self.assertEqual(state.statistics()["cache_entries"], 1)
            self.assertEqual(state.statistics()["evictions"], 1)
        with cache.scope(self.case.ref, max_cache_bytes=1) as state:
            r.core.initial_jobs([first], self.config)
            self.assertEqual(state.statistics()["cache_entries"], 0)
            self.assertEqual(state.statistics()["cache_bytes"], 0)
            self.assertEqual(state.statistics()["uncached_oversize"], 1)

    def test_full_original_receipt_and_reservation_guards_still_run_after_seed(self):
        entry, plan, sources, expected = self.record()
        with cache.scope(self.case.ref):
            r.core.initial_jobs(sources, self.config)
            folder = next((Path(plan["request_value"]["state_root"]) / "waves").iterdir())
            r.put(folder / "submit-intent.json", {"wave_id": folder.name, "maximum_cost_microusd": 0,
                                                   "input_sha256": "0" * 64})
            with self.assertRaises(RuntimeError):
                r.load_state(plan, sources)

    def test_compact_record_snapshot_reuses_only_stable_metadata_and_returns_fresh_copies(self):
        entry, plan, sources, expected = self.record()
        calls = []
        def validator():
            calls.append(True)
            return {"status": {"state": "waiting_remote"}, "accounted": {"held": 12}, "waves": []}
        with cache.scope(self.case.ref) as state:
            first = cache.cached_record(entry, validator)
            first["status"]["state"] = "caller tampering"
            second = cache.cached_record(entry, validator)
            self.assertEqual(second["status"]["state"], "waiting_remote")
            self.assertEqual(len(calls), 1)
            self.assertEqual(state.statistics()["record_hits"], 1)
            self.assertEqual(state.statistics()["record_validations"], 1)
            self.assertEqual(state.statistics()["record_cache_entries"], 1)
        cache.cached_record(entry, validator)
        self.assertEqual(len(calls), 2)  # Outside scope, nothing is cached.

    def test_record_added_changed_and_removed_receipts_force_full_validator(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        receipt = root / "new-terminal-receipt.json"
        calls = []
        def validator():
            calls.append(True)
            return {"status": {"state": "pending"}, "accounted": {"hold": len(calls)}, "waves": []}
        with cache.scope(self.case.ref) as state:
            cache.cached_record(entry, validator)
            r.put(receipt, {"state": "pending"})
            cache.cached_record(entry, validator)
            before = receipt.stat()
            self.rewrite(receipt, {"state": "changed"})
            os.utime(receipt, ns=(before.st_atime_ns, before.st_mtime_ns))
            self.assertEqual(receipt.stat().st_size, before.st_size)
            cache.cached_record(entry, validator)  # ctime still catches same-size/mtime edits.
            receipt.rename(self.case.root / "retained-terminal-receipt.json")
            cache.cached_record(entry, validator)
            self.assertEqual(len(calls), 4)
            self.assertEqual(state.statistics()["record_hits"], 0)

    def test_record_cache_keys_include_source_entry_config_and_worker_binding(self):
        entry, plan, sources, expected = self.record()
        calls = []
        def validator():
            calls.append(True)
            return {"status": {}, "accounted": {}, "waves": []}
        changed = deepcopy(entry)
        changed["source"]["transcript"]["sha256"] = "0" * 64
        with cache.scope(self.case.ref) as state:
            cache.cached_record(entry, validator)
            cache.cached_record(changed, validator)
            state.manifest["config"] = {**state.manifest["config"], "max_output_tokens": 8191}
            cache.cached_record(changed, validator)
            state.worker_reference = {**state.worker_reference, "sha256": "1" * 64}
            cache.cached_record(changed, validator)
            self.assertEqual(len(calls), 4)

    def test_record_cache_rejects_symlinks_before_using_a_prior_snapshot(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        calls = []
        def validator():
            calls.append(True)
            return {"status": {}, "accounted": {}, "waves": []}
        with cache.scope(self.case.ref):
            cache.cached_record(entry, validator)
            (root / "unsafe-link").symlink_to(self.case.root, target_is_directory=True)
            with self.assertRaises(cache.CacheError):
                cache.cached_record(entry, validator)
            self.assertEqual(len(calls), 1)

    def test_record_changed_during_validation_does_not_publish_a_cache_entry(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        def validator():
            r.put(root / "concurrent-terminal.json", {"changed": True})
            return {"status": {}, "accounted": {}, "waves": []}
        with cache.scope(self.case.ref) as state:
            with self.assertRaises(cache.CacheError):
                cache.cached_record(entry, validator)
            self.assertEqual(state.statistics()["record_cache_entries"], 0)

    def test_record_cache_enforces_traversal_and_serialized_size_bounds(self):
        entry, plan, sources, expected = self.record()
        calls = []
        def validator():
            calls.append(True)
            return {"status": {}, "accounted": {}, "waves": []}
        with cache.scope(self.case.ref) as state:
            with patch.object(cache, "MAX_RECORD_FILES", 0):
                with self.assertRaises(cache.CacheError):
                    cache.cached_record(entry, validator)
            self.assertEqual(calls, [])
            with patch.object(cache, "MAX_RECORD_CACHE_BYTES", 1):
                cache.cached_record(entry, validator)
                self.assertEqual(state.statistics()["record_cache_entries"], 0)
                self.assertEqual(state.statistics()["record_uncached_oversize"], 1)

    def test_original_export_repeats_only_after_record_proof_change(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        calls = []
        def validator():
            calls.append(True)
            return r.export_plan(entry["plan"]["path"], entry["plan"]["sha256"], phase="transcripts")
        with cache.scope(self.case.ref) as state:
            first = cache.cached_export(entry, validator)
            second = cache.cached_export(entry, validator)
            self.assertEqual(first, second)
            self.assertEqual(len(calls), 1)
            self.assertEqual(state.statistics()["export_hits"], 1)
            r.put(root / "new-result-proof.json", {"changed": True})
            third = cache.cached_export(entry, validator)
            self.assertEqual(third, first)
            self.assertEqual(len(calls), 2)
            self.assertEqual(state.statistics()["export_validations"], 2)
        cache.cached_export(entry, validator)
        self.assertEqual(len(calls), 3)

    def test_export_cache_checks_artifact_hash_even_if_metadata_is_frozen(self):
        entry, plan, sources, expected = self.record()
        with cache.scope(self.case.ref) as state:
            artifact = cache.cached_export(entry, lambda: r.export_plan(
                entry["plan"]["path"], entry["plan"]["sha256"], phase="transcripts"))
            key, (signature, encoded) = next(iter(state.export_cache.items()))
            damaged = r.parse(encoded)
            damaged["artifact"]["sha256"] = "0" * 64
            state.export_cache[key] = (signature, r.canonical(damaged))
            with self.assertRaises(RuntimeError):
                cache.cached_export(entry, lambda: self.fail("unchanged witness should hit"))

    def test_export_keys_bind_entry_source_and_configuration(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        calls = []
        def validator():
            calls.append(True)
            return {"state": "exported_private", "phase": "transcripts",
                    "artifact": r.put(root / "exports" / "test-summary.json", {"same": "bytes"})}
        changed = deepcopy(entry)
        changed["source"]["transcript"]["sha256"] = "0" * 64
        with cache.scope(self.case.ref) as state:
            cache.cached_export(entry, validator)
            cache.cached_export(changed, validator)
            state.manifest["config"] = {**state.manifest["config"], "max_output_tokens": 8191}
            cache.cached_export(changed, validator)
            self.assertEqual(len(calls), 3)

    def test_export_callback_cannot_change_any_nonexport_proof(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        def validator():
            r.put(root / "changed-paid-receipt.json", {"not_allowed": True})
            return {"state": "exported_private", "phase": "transcripts",
                    "artifact": r.put(root / "exports" / "test-summary.json", {"same": "bytes"})}
        with cache.scope(self.case.ref) as state:
            with self.assertRaises(cache.CacheError):
                cache.cached_export(entry, validator)
            self.assertEqual(state.statistics()["export_cache_entries"], 0)

    def test_export_cache_rejects_escaped_artifact_and_unsafe_symlink(self):
        entry, plan, sources, expected = self.record()
        root = Path(plan["request_value"]["state_root"])
        outside = r.put(self.case.root / "not-an-export.json", {"outside": True})
        with cache.scope(self.case.ref):
            with self.assertRaises(cache.CacheError):
                cache.cached_export(entry, lambda: {"state": "exported_private", "phase": "transcripts",
                                                     "artifact": outside})
            (root / "exports" / "unsafe").symlink_to(self.case.root, target_is_directory=True)
            with self.assertRaises(cache.CacheError):
                cache.cached_export(entry, lambda: self.fail("unsafe witness must reject first"))

    def test_export_cache_serialized_size_limit_is_independent(self):
        entry, plan, sources, expected = self.record()
        with cache.scope(self.case.ref) as state:
            with patch.object(cache, "MAX_EXPORT_CACHE_BYTES", 1):
                cache.cached_export(entry, lambda: r.export_plan(
                    entry["plan"]["path"], entry["plan"]["sha256"], phase="transcripts"))
            self.assertEqual(state.statistics()["export_cache_entries"], 0)
            self.assertEqual(state.statistics()["export_uncached_oversize"], 1)


if __name__ == "__main__":
    unittest.main()
