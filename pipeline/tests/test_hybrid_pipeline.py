from __future__ import annotations

from contextlib import contextmanager
import copy
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import unittest
from unittest import mock

from pipeline import hybrid_pipeline as hybrid
from pipeline import hybrid_audio_prepare, hybrid_cpu_worker
from pipeline.tests import test_hybrid_legacy_guard as legacy_fixture


class HybridPipelineTests(unittest.TestCase):
    """Synthetic standalone integration fixtures: no corpus, ASR, or network."""

    def setUp(self) -> None:
        self.legacy = legacy_fixture.LegacyGuardTests()
        self.legacy.setUp()
        self.addCleanup(self.legacy.doCleanups)
        self.root = self.legacy.root
        self.assets = self.root / "fake-assets"
        self.assets.mkdir(mode=0o700)
        for name in ("ffmpeg", "ffprobe", "whisper-cli", "model.bin"):
            target = self.assets / name
            target.write_bytes(("synthetic asset " + name).encode())
            target.chmod(0o700 if name != "model.bin" else 0o600)
        self.sources = self.root / "synthetic-sources"
        self.sources.mkdir(mode=0o700)
        self.config = self.make_config(self.root / "new" / "hybrid" / "standby")
        self.store = hybrid.Store(self.config)
        self.store.initialize()
        self.prepare_calls = []
        self.cpu_calls = []

    def make_config(self, root: Path, *, budget="10.00") -> dict:
        profile = next(row for row in hybrid.ENGINE_PROFILES if row["admission"] == "current_new_batch")
        core = {
            "kind": "himr_hybrid_pipeline_config", "schema_version": 1,
            "state_root": str(root), "inboxes": [str(root / "inbox")],
            "legacy_guards": [copy.deepcopy(self.legacy.config)],
            "tools": {name: {"path": str(self.assets / name), "sha256": hybrid.salad.file_sha256(self.assets / name)} for name in ("ffmpeg", "ffprobe")},
            "cpu": {
                "engine": {"executable": str(self.assets / "whisper-cli"), "expected_sha256": profile["expected_sha256"],
                           "version_label": profile["version_label"], "version_evidence": profile["version_evidence"], "build": copy.deepcopy(profile["build"])},
                "model": {"path": str(self.assets / "model.bin"), "expected_sha256": hybrid.EXPECTED_MODEL_SHA256, **hybrid.EXPECTED_MODEL},
                "threads": 4, "window_seconds": 1800, "timeout_seconds": 7200,
            },
            "cloud": {"organization": "example-org", "engine": "transcribe", "rate_usd_per_hour": "0.20",
                      "max_estimated_total_cost_usd": budget, "diarization": "both", "summary_words": 200, "max_inflight_jobs": 2},
            "limits": {"max_admissions_per_cycle": 8, "max_source_bytes": hybrid.MAX_SOURCE_BYTES, "max_attempts": 3, "poll_seconds": 30},
            "software": hybrid.software_bindings(),
            "policy": {"activation": "explicit_standby_first", "legacy_state_writes": False,
                       "source_deletion": False, "catalogue_writes": False, "publication": False,
                       "finished_handoffs_only": True, "same_source_single_route": True},
        }
        return hybrid.validate_config(hybrid.seal(core, "config_id", "hybridcfg_"))

    def publish(self, origin="new_video", *, source_key="video-1", body=b"synthetic video one", filename=None) -> dict:
        sha = hashlib.sha256(body).hexdigest()
        source = self.sources / (sha + ".media")
        if not source.exists():
            source.write_bytes(body)
            source.chmod(0o600)
        handoff = hybrid.make_handoff({"path": str(source), "sha256": sha, "byte_count": len(body), "media_id": "media_sha256_" + sha}, origin, source_key)
        target = Path(self.config["inboxes"][0]) / (filename or (handoff["handoff_id"] + ".json"))
        hybrid.salad._write_json(target, handoff)
        return handoff

    def job(self, handoff: dict) -> dict:
        row = self.store.connection.execute("SELECT * FROM jobs WHERE media_sha256=?", (handoff["source"]["sha256"],)).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def fake_prepared(self, source: dict, output_root: Path, *, seconds=10) -> dict:
        root = hybrid.salad._private_directory(output_root / "synthetic-preparation")
        audio_path = root / "audio.flac"
        body = ("normalized fixture " + source["sha256"]).encode()
        if not audio_path.exists():
            audio_path.write_bytes(body)
            audio_path.chmod(0o600)
        sha = hashlib.sha256(body).hexdigest()
        manifest = {
            "kind": "himr_longform_recording_input_manifest", "schema_version": 1,
            "boundary_candidates": [],
            "recording": {"recording_id": "hybridrec_" + source["sha256"][:32], "media_id": source["media_id"],
                          "input": {"artifact_id": "hybrid_audio_" + sha[:32], "path": str(audio_path),
                                    "sha256": sha, "byte_count": len(body), "sample_rate_hz": 16000,
                                    "channels": 1, "duration_ms": seconds * 1000, "total_samples": seconds * 16000}},
        }
        path = root / "recording-input.json"
        hybrid.salad._write_json(path, manifest)
        return {"recording_input": str(path), "sha256": hybrid.digest(manifest), "recording_id": manifest["recording"]["recording_id"], "reused": False}

    def fake_prepare_worker(self, source, **kwargs):
        self.prepare_calls.append(kwargs)
        return self.fake_prepared(source, kwargs["output_root"])

    def reserve_cloud(self, handoff, *, seconds=3600):
        job = self.job(handoff)
        prepared = self.fake_prepared(handoff["source"], self.store.root / "jobs" / job["job_id"] / "audio", seconds=seconds)
        self.store.update_job(job["job_id"], prepared_json=hybrid.canonical_bytes(prepared).decode(), status="ready")
        plan = self.store._cloud_plan(self.job(handoff), prepared)
        return self.job(handoff), plan

    def initialize_cloud(self, job, plan):
        binding = json.loads(job["cloud_plan_json"])
        hybrid.salad._write_json(self.store.root / "jobs" / job["job_id"] / "cloud-started.json",
                                 {"config_id": self.config["config_id"], "job_id": job["job_id"], "plan": binding})
        with hybrid.salad.Workspace(plan).locked():
            pass

    def test_initialization_is_standby_and_does_not_touch_legacy(self) -> None:
        before = {path: path.read_bytes() for parent in (self.legacy.controller, self.legacy.companion) for path in parent.iterdir()}
        with self.store.database():
            result = self.store.summary()
        self.assertEqual(result["control"]["desired"], "stopped")
        self.assertEqual(result["jobs"], [])
        after = {path: path.read_bytes() for parent in (self.legacy.controller, self.legacy.companion) for path in parent.iterdir()}
        self.assertEqual(before, after)

    def test_standby_cycle_never_scans_prepares_or_constructs_cloud_client(self) -> None:
        self.publish()
        with self.store.database(writable=True), mock.patch.object(self.store, "scan", side_effect=AssertionError("standby scan")), mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=AssertionError("audio work")), mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("cloud client")):
            result = self.store.cycle(allow_local=True, allow_cloud=True)
        self.assertEqual(result["status"], "standby")

    def test_running_legacy_holds_before_any_worker_or_admission(self) -> None:
        self.publish()
        self.store.set_control("running")
        self.legacy.control["desired_state"] = "running"
        self.legacy.save()
        with self.store.database(writable=True), mock.patch.object(self.store, "scan", side_effect=AssertionError("must hold before scan")), mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=AssertionError("audio work")), mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("cloud client")):
            result = self.store.cycle(allow_local=True, allow_cloud=True)
        self.assertEqual(result["status"], "held_legacy_active_or_uncertain")

    def test_route_mapping_and_duplicate_replay_are_durable(self) -> None:
        archive = self.publish("archive_batch", source_key="archive:one", body=b"archive fixture")
        video = self.publish("new_video", source_key="youtube:one", body=b"video fixture")
        live = self.publish("finished_livestream", source_key="youtube:live", body=b"finished stream fixture")
        with self.store.database(writable=True):
            self.assertEqual(self.store.scan()["admitted"], 3)
            self.assertEqual(self.job(archive)["route"], "cloud")
            self.assertEqual(self.job(video)["route"], "cpu")
            self.assertEqual(self.job(live)["route"], "cpu")
            self.assertEqual(self.store.scan()["admitted"], 0)
        with self.store.database():
            self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 3)

    def test_same_media_conflicting_origin_never_creates_second_route(self) -> None:
        original = self.publish("new_video", source_key="original")
        with self.store.database(writable=True):
            self.store.scan()
            self.publish("archive_batch", source_key="bulk-alias")
            report = self.store.scan()
            self.assertEqual(report["admitted"], 0)
            self.assertEqual(self.job(original)["route"], "cpu")
            self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)
            self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM seen WHERE disposition='conflicting_source_or_route'").fetchone()[0], 1)

    def test_source_identity_with_changed_bytes_is_conflict_not_silent_replacement(self) -> None:
        original = self.publish(source_key="same-id", body=b"first bytes")
        with self.store.database(writable=True):
            self.store.scan()
            self.publish(source_key="same-id", body=b"later different bytes")
            self.store.scan()
            self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)
            self.assertEqual(self.job(original)["media_sha256"], original["source"]["sha256"])

    def test_scan_is_metadata_only_and_does_not_call_audio_worker(self) -> None:
        self.publish()
        with self.store.database(writable=True), mock.patch.object(hybrid.salad, "_hash_fd", side_effect=AssertionError("must not hash media")), mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=AssertionError("must not prepare")):
            report = self.store.scan()
        self.assertEqual(report["admitted"], 1)
        self.assertFalse(report["media_hashed"])

    def test_stop_changes_only_hybrid_control_and_blocks_following_cycle(self) -> None:
        self.store.set_control("running")
        before = Path(self.legacy.config["control_path"]).read_bytes()
        stopped = self.store.set_control("stopped")
        self.assertEqual(stopped["generation"], 2)
        self.assertEqual(Path(self.legacy.config["control_path"]).read_bytes(), before)
        with self.store.database(writable=True):
            self.assertEqual(self.store.cycle(allow_local=True)["status"], "standby")

    def test_cpu_resumes_next_window_without_preparing_again_and_inherits_leases(self) -> None:
        handoff = self.publish()
        self.store.set_control("running")
        outcomes = [{"status": "incomplete", "completed_windows": 1}, {"status": "completed", "completed_windows": 2}]

        def cpu(*args, **kwargs):
            self.cpu_calls.append(kwargs)
            leases = kwargs.get("lease_fds", ())
            self.assertIsInstance(leases, tuple)
            self.assertGreaterEqual(len(leases), 2)
            self.assertTrue(all(os.fstat(fd).st_size == 0 for fd in leases))
            self.legacy.assert_held("controller_lock")
            self.legacy.assert_held("companion_lock")
            return outcomes.pop(0)

        with mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=self.fake_prepare_worker), mock.patch.object(hybrid_cpu_worker, "run_cpu", side_effect=cpu), mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("CPU route must not construct cloud client")):
            with self.store.database(writable=True):
                self.store.cycle(allow_local=True)
                self.assertEqual(self.job(handoff)["status"], "ready")
            with self.store.database(writable=True):
                self.store.cycle(allow_local=True)
                self.assertEqual(self.job(handoff)["status"], "completed")
        self.assertEqual(len(self.prepare_calls), 1)
        self.assertEqual(len(self.cpu_calls), 2)
        self.assertGreaterEqual(len(self.prepare_calls[0].get("lease_fds", ())), 2)
        self.assertTrue(all(call["max_windows"] == 1 for call in self.cpu_calls))

    def test_stop_during_preparation_does_not_start_cpu_inference(self) -> None:
        self.publish()
        self.store.set_control("running")

        def prepare(source, **kwargs):
            result = self.fake_prepare_worker(source, **kwargs)
            self.store.set_control("stopped")
            return result

        with self.store.database(writable=True), mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=prepare), mock.patch.object(hybrid_cpu_worker, "run_cpu", side_effect=AssertionError("must observe Stop before ASR")) as cpu:
            self.store.cycle(allow_local=True)
            cpu.assert_not_called()

    def test_cpu_windows_rotate_between_recordings_and_failures_are_bounded(self) -> None:
        first = self.publish(source_key="first", body=b"first recording")
        second = self.publish(source_key="second", body=b"second recording")
        self.store.set_control("running")
        processed = []

        def cpu(recording_input, *_args, **_kwargs):
            processed.append(str(recording_input))
            raise OSError("synthetic worker failure")

        with mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=self.fake_prepare_worker), mock.patch.object(hybrid_cpu_worker, "run_cpu", side_effect=cpu):
            for _ in range(2 * self.config["limits"]["max_attempts"] + 1):
                with self.store.database(writable=True):
                    self.store.cycle(allow_local=True)
            with self.store.database():
                for handoff in (first, second):
                    job = self.job(handoff)
                    self.assertEqual(job["status"], "held")
                    self.assertEqual(job["attempts"], self.config["limits"]["max_attempts"])
        self.assertEqual(len(processed), 2 * self.config["limits"]["max_attempts"])
        self.assertNotEqual(processed[0], processed[1])
        self.assertEqual(len(self.prepare_calls), 2)

    def test_cached_preparation_cannot_cross_source_or_job_boundaries(self) -> None:
        first = self.publish(source_key="first", body=b"first recording identity")
        second = self.publish(source_key="second", body=b"different recording identity")
        with self.store.database(writable=True):
            self.store.scan()
            first_job, second_job = self.job(first), self.job(second)
            other_prepared = self.fake_prepared(second["source"], self.store.root / "jobs" / second_job["job_id"] / "audio")
            self.store.update_job(first_job["job_id"], prepared_json=hybrid.canonical_bytes(other_prepared).decode(), status="ready")
            with mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=AssertionError("cached input must be checked without new preparation")), mock.patch.object(hybrid_cpu_worker, "run_cpu", side_effect=AssertionError("cross-job input must not reach inference")):
                with self.assertRaises(hybrid.HybridError):
                    self.store._cpu(self.job(first), lambda: None)

    def test_cloud_route_needs_explicit_allow_cloud(self) -> None:
        self.publish("archive_batch")
        self.store.set_control("running")
        with self.store.database(writable=True), mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=AssertionError("cloud disabled")), mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("cloud disabled")):
            report = self.store.cycle(allow_local=True, allow_cloud=False)
        self.assertEqual(report["dispatched"], [])

    def test_cumulative_budget_reserves_before_network_and_never_releases_automatically(self) -> None:
        self.config = self.make_config(self.root / "budget" / "hybrid" / "standby", budget="0.30")
        self.store = hybrid.Store(self.config)
        self.store.initialize()
        first = self.publish("archive_batch", source_key="a", body=b"archive a")
        second = self.publish("archive_batch", source_key="b", body=b"archive b")
        with self.store.database(writable=True), mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("reservation must precede network")):
            self.store.scan()
            job, plan = self.reserve_cloud(first)
            self.assertEqual(Decimal(job["reserved_cost"]), Decimal(plan["estimate"]["estimated_cost_usd"]))
            self.store.update_job(job["job_id"], status="held")
            with self.assertRaises(hybrid.HybridError):
                self.reserve_cloud(second)
            self.assertEqual(Decimal(self.store.summary()["reserved_estimated_cloud_cost_usd"]), Decimal("0.20"))

    def test_reservation_drift_is_detected_before_reopening_for_dispatch(self) -> None:
        handoff = self.publish("archive_batch")
        with self.store.database(writable=True):
            self.store.scan()
            job, _ = self.reserve_cloud(handoff)
            self.store.connection.execute("UPDATE jobs SET reserved_cost=NULL WHERE job_id=?", (job["job_id"],))
            self.store.connection.commit()
        with mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("corrupt reservation must hold before cloud")):
            with self.assertRaises(hybrid.HybridError):
                with self.store.database(writable=True):
                    pass

    def test_orphan_admission_after_ledger_rollback_fails_closed(self) -> None:
        handoff = self.publish("archive_batch")
        with self.store.database(writable=True):
            self.store.scan()
            job = self.job(handoff)
            self.assertTrue((self.store.root / "jobs" / job["job_id"] / "admission.json").is_file())
            self.store.connection.execute("DELETE FROM seen")
            self.store.connection.execute("DELETE FROM jobs")
            self.store.connection.commit()
        with self.assertRaises(hybrid.HybridError):
            with self.store.database(writable=True):
                pass

    def test_cloud_plan_ahead_of_rolled_back_ledger_fails_closed(self) -> None:
        handoff = self.publish("archive_batch")
        with self.store.database(writable=True):
            self.store.scan()
            job, _ = self.reserve_cloud(handoff)
            self.store.connection.execute("UPDATE jobs SET cloud_plan_json=NULL,reserved_cost=NULL WHERE job_id=?", (job["job_id"],))
            self.store.connection.commit()
        with self.assertRaises(hybrid.HybridError):
            with self.store.database(writable=True):
                pass

    def test_previously_started_cloud_workspace_loss_never_recreates_paid_work(self) -> None:
        handoff = self.publish("archive_batch")
        with self.store.database(writable=True):
            self.store.scan()
            job, plan = self.reserve_cloud(handoff)
            self.initialize_cloud(job, plan)
            self.store.update_job(job["job_id"], status="cloud_running", result_json='{"states":{"running":1}}')
        runtime = Path(plan["output_root"])
        runtime.rename(self.root / "simulated-lost-cloud-runtime")
        with mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("lost cloud runtime must never authorize another POST")):
            with self.assertRaises((hybrid.HybridError, hybrid.salad.CloudPipelineError, OSError)):
                with self.store.database(writable=True):
                    pass
        self.assertFalse(runtime.exists())

    def test_held_cloud_poll_failure_does_not_restore_submission_eligibility(self) -> None:
        handoff = self.publish("archive_batch")
        self.store.set_control("running")
        with self.store.database(writable=True):
            self.store.scan()
            job, _ = self.reserve_cloud(handoff)
            self.store.update_job(job["job_id"], status="held")
            selected = self.store.connection.execute("SELECT rowid AS sequence,* FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone()
            # Selection is covered separately below; isolate preservation of
            # an already-held row when its authorized poll raises an error.
            with mock.patch.object(self.store, "_next_job", side_effect=lambda route: selected if route == "cloud" else None), mock.patch.object(self.store, "_cloud", side_effect=OSError("synthetic polling failure")) as cloud:
                self.store.cycle(allow_local=True, allow_cloud=True)
            cloud.assert_called_once()
            self.assertEqual(self.job(handoff)["status"], "held")

    def test_known_cloud_polls_take_priority_over_fresh_backlog_even_when_held(self) -> None:
        fresh = [self.publish("archive_batch", source_key=f"fresh-{index}", body=f"fresh recording {index}".encode(), filename=f"{index:03d}-fresh.json") for index in range(5)]
        known = self.publish("archive_batch", source_key="known", body=b"known running recording", filename="999-known.json")
        with self.store.database(writable=True):
            self.assertEqual(self.store.scan()["admitted"], 6)
            job, plan = self.reserve_cloud(known, seconds=10)
            self.initialize_cloud(job, plan)
            sequence = self.store.connection.execute("SELECT rowid FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone()[0]
            for status in ("cloud_running", "held"):
                with self.subTest(status=status):
                    self.store.update_job(job["job_id"], status=status)
                    with self.store.connection:
                        self.store.connection.execute("INSERT OR REPLACE INTO cursors VALUES('cloud',?)", (str(sequence),))
                    with mock.patch.object(hybrid.salad.Workspace, "summary", return_value={"states": {"running": 1}}), mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("selection must not create cloud client")):
                        self.assertEqual(self.store._next_job("cloud")["job_id"], job["job_id"])
            with mock.patch.object(hybrid.salad.Workspace, "summary", return_value={"states": {"submission_unknown": 1}}):
                selected = self.store._next_job("cloud")
            self.assertNotEqual(selected["job_id"], job["job_id"])
            self.assertIn(selected["media_sha256"], {handoff["source"]["sha256"] for handoff in fresh})

    def test_missing_ledger_is_not_recreated_after_initialization(self) -> None:
        database = self.store.root / "ledger.sqlite3"
        database.unlink()
        with self.assertRaises(hybrid.HybridError):
            self.store.initialize()
        self.assertFalse(database.exists())

    def test_reinitialization_rejects_unrelated_nonempty_directory_before_writes(self) -> None:
        root = self.root / "existing" / "other" / "state"
        hybrid.salad._private_directory(root)
        marker = root / "user-file.txt"
        marker.write_bytes(b"unrelated existing data")
        marker.chmod(0o600)
        store = hybrid.Store(self.make_config(root))
        with self.assertRaises(hybrid.HybridError):
            store.initialize()
        self.assertFalse((root / "config.json").exists())
        self.assertEqual(marker.read_bytes(), b"unrelated existing data")

    def test_configuration_identity_tamper_is_rejected(self) -> None:
        changed = copy.deepcopy(self.config)
        changed["cloud"]["max_estimated_total_cost_usd"] = "9999"
        with self.assertRaisesRegex(hybrid.HybridError, "identity"):
            hybrid.validate_config(changed)

    def test_readiness_with_running_legacy_does_not_lock_or_hash_media(self) -> None:
        self.legacy.control["desired_state"] = "running"
        self.legacy.save()
        before = Path(self.legacy.config["control_path"]).read_bytes()
        with mock.patch.object(hybrid.fcntl, "flock", side_effect=AssertionError("readiness must not acquire locks")), mock.patch.object(hybrid.salad, "_hash_fd", side_effect=AssertionError("readiness must not hash media or model")), mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("readiness must be offline")):
            result = hybrid.readiness(self.config)
        self.assertFalse(result["legacy"]["safe"])
        self.assertTrue(result["no_media_read"])
        self.assertTrue(result["no_network"])
        self.assertEqual(Path(self.legacy.config["control_path"]).read_bytes(), before)

    def test_peer_readable_ledger_and_symlink_are_rejected(self) -> None:
        path = self.store.root / "ledger.sqlite3"
        path.chmod(0o644)
        with self.assertRaises(hybrid.HybridError):
            with self.store.database():
                pass
        path.chmod(0o600)
        backup = path.with_name("saved-ledger.sqlite3")
        path.rename(backup)
        path.symlink_to(backup)
        with self.assertRaises(hybrid.HybridError):
            with self.store.database():
                pass

    def test_sqlite_uri_handles_question_mark_and_hash_as_path_characters(self) -> None:
        config = self.make_config(self.root / "uri" / "hybrid" / "state?literal#name")
        store = hybrid.Store(config)
        store.initialize()
        with store.database():
            self.assertEqual(store.summary()["config_id"], config["config_id"])
        self.assertTrue((store.root / "ledger.sqlite3").is_file())
        self.assertFalse((store.root.parent / "state").exists())

    def test_oversized_handoff_is_rejected_before_generic_json_reader(self) -> None:
        path = self.store.root / "inbox" / "oversized.json"
        path.write_bytes(b" " * (65536 + 1))
        path.chmod(0o600)
        original = hybrid.salad.read_json

        def checked_read(target, *args, **kwargs):
            self.assertNotEqual(Path(target), path, "oversized descriptor reached 64-MiB JSON reader")
            return original(target, *args, **kwargs)

        with self.store.database(writable=True), mock.patch.object(hybrid.salad, "read_json", side_effect=checked_read):
            self.assertEqual(self.store.scan()["rejected"], 1)

    def test_inspection_cursor_bounds_repeated_or_invalid_handoffs_and_makes_progress(self) -> None:
        for ordinal in range(40):
            path = self.store.root / "inbox" / f"{ordinal:03d}-invalid.json"
            path.write_bytes(b"not json")
            path.chmod(0o600)
        self.publish(filename="999-valid.json")
        with self.store.database(writable=True):
            first = self.store.scan()
            second = self.store.scan()
            self.assertLessEqual(first["metadata_inspected"], 32)
            self.assertLessEqual(second["metadata_inspected"], 32)
            self.assertEqual(first["admitted"] + second["admitted"], 1)

    def test_ambiguous_job_blocks_new_posts_but_known_jobs_can_still_be_polled(self) -> None:
        unknown = self.publish("archive_batch", source_key="unknown", body=b"unknown cloud recording")
        known = self.publish("archive_batch", source_key="known", body=b"known cloud recording")
        self.store.set_control("running")
        run_calls = []
        states_by_plan = {}

        class WorkspaceFixture:
            def __init__(self, plan, *, lease_fds=()):
                self.plan = plan
                self.lease_fds = lease_fds

            @contextmanager
            def locked(self, **_kwargs):
                yield self

            def summary(self):
                return {"states": states_by_plan[self.plan["plan_id"]]}

            def run_cycle(self, _client, **kwargs):
                run_calls.append(kwargs)
                return {"states": states_by_plan[self.plan["plan_id"]], "chunks": len(self.plan["recordings"][0]["chunks"])}

        with self.store.database(writable=True):
            self.store.scan()
            first_job, first_plan = self.reserve_cloud(unknown, seconds=9001)
            second_job, second_plan = self.reserve_cloud(known, seconds=10)
            self.initialize_cloud(first_job, first_plan)
            self.initialize_cloud(second_job, second_plan)
            states_by_plan[first_plan["plan_id"]] = {"submission_unknown": 1, "running": 1}
            states_by_plan[second_plan["plan_id"]] = {"pending": 1}
            self.store.update_job(first_job["job_id"], status="held")
            self.store.update_job(second_job["job_id"], status="cloud_running")
            with mock.patch.object(hybrid.salad, "Workspace", WorkspaceFixture), mock.patch.object(hybrid.salad, "SaladClient", return_value=mock.Mock()):
                active, ambiguous = self.store._cloud_active()
                self.assertTrue(ambiguous)
                self.assertGreaterEqual(active, 2)
                self.store._cloud(self.job(known), lambda: None)
            self.assertTrue(run_calls, "known remote jobs must be polled even while another submission is ambiguous")
            self.assertTrue(all(call["max_new_jobs"] == 0 for call in run_calls))

    def test_changed_software_holds_long_lived_supervisor_before_dispatch(self) -> None:
        self.store.set_control("running")
        with self.store.database(writable=True), mock.patch.object(hybrid, "software_bindings", return_value=[]), \
                mock.patch.object(self.store, "scan", side_effect=AssertionError("changed software must hold before admission")):
            with self.assertRaises(hybrid.HybridError):
                self.store.cycle(allow_local=True, allow_cloud=True)

    def test_failed_runtime_prevents_new_posts_even_if_mutable_job_is_ready(self) -> None:
        handoff = self.publish("archive_batch")
        run_calls = []

        class WorkspaceFixture:
            def __init__(self, plan, *, lease_fds=()):
                self.plan = plan

            @contextmanager
            def locked(self, **_kwargs):
                yield self

            def summary(self):
                return {"states": {"failed": 1, "planned": 1}}

            def run_cycle(self, _client, **kwargs):
                run_calls.append(kwargs)
                return {**self.summary(), "chunks": 2}

        with self.store.database(writable=True):
            self.store.scan()
            job, plan = self.reserve_cloud(handoff, seconds=9001)
            self.initialize_cloud(job, plan)
            with mock.patch.object(hybrid.salad, "Workspace", WorkspaceFixture), mock.patch.object(hybrid.salad, "SaladClient", return_value=mock.Mock()):
                self.store._cloud(self.job(handoff), lambda: None)
            self.assertEqual(self.job(handoff)["status"], "held")
        self.assertEqual(len(run_calls), 1)
        self.assertEqual(run_calls[0]["max_new_jobs"], 0)


if __name__ == "__main__":
    unittest.main()
