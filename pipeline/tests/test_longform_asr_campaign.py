from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from autonomous_controller.config import ControllerConfig
from pipeline import longform_asr_campaign as campaign


class LongformCampaignTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.deployment = self.root / "deployment"
        self.deployment.mkdir(mode=0o700)
        (self.deployment / "jobs").mkdir(mode=0o700)
        (self.deployment / "discovery").mkdir(mode=0o700)
        (self.deployment / "discovery" / "candidate-receipts").mkdir(mode=0o700)
        (self.deployment / "discovery" / "queue-receipts").mkdir(mode=0o700)
        lock = self.deployment / "dispatch.lock"
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        self.controller_path = self.root / "controller.json"
        self.controller_path.write_text("{}\n")
        self.controller_path.chmod(0o400)
        self.controller = ControllerConfig(
            document={
                "config_id": "himrautocfg_" + "1" * 32,
                "identity_sha256": "2" * 64,
                "campaign": {"schedules": []},
                "state_root": str(self.root / "source-state"),
                "preprocess": {
                    "bundle_root": str(self.root / "source-preprocess-bundles"),
                    "processing_output_root": str(self.root / "source-preprocess-output"),
                },
                "gpu_readiness": {
                    "queue_root": str(self.root / "source-queues"),
                    "lock_root": str(self.root / "source-locks"),
                    "work_order_root": str(self.root / "source-work-orders"),
                    "receipt_root": str(self.root / "source-receipts"),
                    "result_root": str(self.root / "source-results"),
                    "batch_root": str(self.root / "source-batches"),
                    "event_root": str(self.root / "source-events"),
                    "root_registration": str(self.root / "root-registration.json"),
                    "root_registration_sha256": "3" * 64,
                    "production_profile": str(self.root / "profile.json"),
                    "production_profile_sha256": "4" * 64,
                },
                "cold_retention": {
                    "staging_root": str(self.root / "source-cold-staging"),
                    "receipt_root": str(self.root / "source-cold-receipts"),
                },
            },
            path=self.controller_path,
            physical_sha256="5" * 64,
        )
        locator = campaign.build_cold_locator_manifest(self.controller)
        locator_body = campaign.canonical_bytes(locator)
        self.locator_path = self.deployment / "discovery" / "cold-locators.json"
        self.locator_path.write_bytes(locator_body)
        self.locator_path.chmod(0o400)
        self.config_document = campaign.build_config(
            controller=self.controller,
            planning_policy=self.root / "policy.json",
            planning_policy_sha256="6" * 64,
            ffmpeg=Path("/usr/bin/ffmpeg"),
            ffmpeg_sha256="7" * 64,
            ffprobe=Path("/usr/bin/ffprobe"),
            ffprobe_sha256="8" * 64,
            deployment_root=self.deployment,
            max_run_seconds=3600,
            cold_locator_manifest_sha256=hashlib.sha256(locator_body).hexdigest(),
        )
        self.config = campaign.CampaignConfig(
            self.config_document,
            self.root / "longform-config.json",
            "9" * 64,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def queue_candidate(self, duration_ms: int, identity: str) -> dict:
        return {
            "candidate_kind": "gpu_queue_requires_chunking",
            "identity_sha256": identity * 64,
            "audio": {"duration_ms": duration_ms},
        }

    def test_config_binds_source_gpu_lock_and_isolated_roots(self) -> None:
        source = self.config_document["source_controller"]
        self.assertEqual(source["gpu_lock_root"], str(self.root / "source-locks"))
        self.assertEqual(
            self.config_document["deployment"]["cold_locator_manifest"],
            str(self.locator_path),
        )
        self.assertNotEqual(
            Path(source["gpu_lock_root"]),
            Path(self.config_document["deployment"]["root"]),
        )
        self.assertEqual(
            campaign._normalize_config(self.config_document), self.config_document
        )

    def test_shortest_candidate_wins_with_identity_tie_break(self) -> None:
        rows = [
            self.queue_candidate(20_000, "c"),
            self.queue_candidate(10_000, "b"),
            self.queue_candidate(10_000, "a"),
        ]
        with mock.patch.object(campaign, "_job_status", return_value="unprepared"):
            selected = campaign._select_next_candidate(self.config, rows)
        self.assertEqual(selected["identity_sha256"], "a" * 64)

    def test_duration_admission_accepts_only_exact_half_millisecond_tie(self) -> None:
        half_millisecond_tie = {
            "duration_ms": 491_149,
            "sample_rate_hz": 16_000,
            "total_samples": 7_858_376,
        }
        self.assertTrue(
            campaign._duration_ms_matches_exact_samples(
                491_148, half_millisecond_tie
            )
        )
        self.assertTrue(
            campaign._duration_ms_matches_exact_samples(
                491_149, half_millisecond_tie
            )
        )
        self.assertFalse(
            campaign._duration_ms_matches_exact_samples(
                491_147, half_millisecond_tie
            )
        )
        self.assertFalse(
            campaign._duration_ms_matches_exact_samples(
                491_150, half_millisecond_tie
            )
        )

        non_tie = {**half_millisecond_tie, "total_samples": 7_858_377}
        self.assertFalse(
            campaign._duration_ms_matches_exact_samples(491_148, non_tie)
        )
        self.assertTrue(
            campaign._duration_ms_matches_exact_samples(491_149, non_tie)
        )

    def test_incremental_cold_discovery_deep_replays_once_then_uses_receipt(self) -> None:
        result_path = self.root / "result.json"
        envelope = {
            "status": "completed",
            "dry_run": False,
            "job_id": "acq-fixture",
            "result_path": str(result_path),
            "admission": {
                "normalized_probe": {"format": {"duration_ms": 12_345}}
            },
        }
        result_path.write_bytes(campaign.canonical_bytes(envelope))
        order = {"job_id": "acq-fixture", "output": {"root": str(self.root)}}
        locator_core = {
            "schedule": {
                "path": str(self.root / "schedule.json"),
                "sha256": "a" * 64,
                "schedule_id": "bgacqsched_" + "b" * 32,
            },
            "queue": {
                "bundle_id": "acqbundle_" + "c" * 32,
                "manifest_path": str(self.root / "manifest.json"),
                "manifest_sha256": "d" * 64,
                "ordinal": 1,
                "job_id": "acq-fixture",
            },
            "work_order": order,
            "work_order_sha256": hashlib.sha256(
                campaign.archive_preprocess_handoff.queue_runner.canonical_bytes(order)
            ).hexdigest(),
            "result_path": str(result_path),
        }
        identity = hashlib.sha256(campaign.canonical_bytes(locator_core)).hexdigest()
        locator = {
            **locator_core,
            "locator_id": f"himrcoldloc_{identity[:32]}",
            "identity_sha256": identity,
        }
        locator_manifest = {"locators": [locator]}
        state = {
            "result_sha256": "e" * 64,
            "media_sha256": "f" * 64,
            "byte_count": 123,
            "result": envelope,
        }
        with (
            mock.patch.object(campaign, "_load_cold_locator_manifest", return_value=locator_manifest),
            mock.patch.object(
                campaign.archive_preprocess_handoff.queue_runner,
                "_inspect_result",
                return_value=state,
            ) as inspect_result,
        ):
            first = campaign.discover_cold_schedule_candidates(
                self.config, admit_new=True
            )
            second = campaign.discover_cold_schedule_candidates(
                self.config, admit_new=True
            )
        self.assertEqual(first, second)
        self.assertEqual(len(first), 1)
        inspect_result.assert_called_once_with(order)
        receipt = (
            self.deployment
            / "discovery"
            / "candidate-receipts"
            / f"{locator['locator_id']}.json"
        )
        self.assertEqual(receipt.stat().st_mode & 0o777, 0o400)

    def test_cold_result_replay_retries_only_recognized_publication_races(self) -> None:
        result = {"status": "completed"}
        transient = campaign.archive_preprocess_handoff.queue_runner.QueueRunnerError(
            "completed result failed immutable reuse: durable acquisition result "
            "path identity changed at component jobs"
        )
        with (
            mock.patch.object(
                campaign,
                "_install_acquisition_directory_identity_adapter",
                return_value=True,
            ) as install_adapter,
            mock.patch.object(
                campaign.archive_preprocess_handoff.queue_runner,
                "_inspect_result",
                side_effect=[transient, result],
            ) as inspect_result,
            mock.patch.object(campaign.time, "sleep") as sleep,
        ):
            observed = campaign._inspect_cold_result_with_admission_retry(
                {"job_id": "fixture"}
            )
        self.assertIs(observed, result)
        install_adapter.assert_called_once_with(
            campaign.archive_preprocess_handoff.queue_runner,
            required=True,
        )
        self.assertEqual(inspect_result.call_count, 2)
        sleep.assert_called_once_with(
            campaign.archive_preprocess_handoff.RESULT_ADMISSION_RETRY_SECONDS
        )

        integrity_failure = (
            campaign.archive_preprocess_handoff.queue_runner.QueueRunnerError(
                "completed result payload digest differs"
            )
        )
        with (
            mock.patch.object(
                campaign,
                "_install_acquisition_directory_identity_adapter",
                return_value=True,
            ) as install_adapter,
            mock.patch.object(
                campaign.archive_preprocess_handoff.queue_runner,
                "_inspect_result",
                side_effect=integrity_failure,
            ) as inspect_result,
            mock.patch.object(campaign.time, "sleep") as sleep,
            self.assertRaisesRegex(
                campaign.archive_preprocess_handoff.queue_runner.QueueRunnerError,
                "payload digest differs",
            ),
        ):
            campaign._inspect_cold_result_with_admission_retry(
                {"job_id": "fixture"}
            )
        install_adapter.assert_called_once_with(
            campaign.archive_preprocess_handoff.queue_runner,
            required=True,
        )
        inspect_result.assert_called_once()
        sleep.assert_not_called()

    def test_cold_result_replay_fails_closed_if_directory_adapter_fails(self) -> None:
        with (
            mock.patch.object(
                campaign,
                "_install_acquisition_directory_identity_adapter",
                side_effect=campaign.SealedBackendError("fixture binding failure"),
            ),
            mock.patch.object(
                campaign.archive_preprocess_handoff.queue_runner,
                "_inspect_result",
            ) as inspect_result,
            self.assertRaisesRegex(
                campaign.CampaignError,
                "cold result directory identity adapter failed: fixture binding failure",
            ),
        ):
            campaign._inspect_cold_result_with_admission_retry(
                {"job_id": "fixture"}
            )
        inspect_result.assert_not_called()

    def test_cold_result_replay_fails_closed_if_required_adapter_is_not_installed(
        self,
    ) -> None:
        with (
            mock.patch.object(
                campaign,
                "_install_acquisition_directory_identity_adapter",
                return_value=False,
            ),
            mock.patch.object(
                campaign.archive_preprocess_handoff.queue_runner,
                "_inspect_result",
            ) as inspect_result,
            self.assertRaisesRegex(
                campaign.CampaignError,
                "required acquisition directory identity adapter was not installed",
            ),
        ):
            campaign._inspect_cold_result_with_admission_retry(
                {"job_id": "fixture"}
            )
        inspect_result.assert_not_called()

    def test_cold_result_replay_installs_real_sha_gated_directory_adapter(self) -> None:
        runner = campaign.archive_preprocess_handoff.queue_runner
        acquire = runner.acquire
        with mock.patch.object(runner, "_inspect_result", return_value=None):
            self.assertIsNone(
                campaign._inspect_cold_result_with_admission_retry(
                    {"job_id": "fixture"}
                )
            )

        fingerprint = acquire._stat_fingerprint
        self.assertTrue(
            getattr(
                fingerprint,
                "__himr_acquisition_directory_identity_adapter__",
                False,
            )
        )
        directory_before = fingerprint(self.root.stat())
        (self.root / "concurrent-sibling").mkdir()
        directory_after = fingerprint(self.root.stat())
        self.assertEqual(directory_before, directory_after)

        leaf = self.root / "strict-leaf"
        leaf.write_bytes(b"before")
        leaf_before = fingerprint(leaf.stat())
        leaf.write_bytes(b"after-change")
        leaf_after = fingerprint(leaf.stat())
        self.assertNotEqual(leaf_before, leaf_after)

    def test_completed_cold_cleanup_removes_only_derived_output(self) -> None:
        candidate = {
            "candidate_kind": "cold_schedule_completed_acquisition",
            "identity_sha256": "a" * 64,
            "schedule": {},
            "queue": {},
            "acquisition_result": {},
            "source_media": {"duration_ms": 1},
        }
        job = campaign._job(self.config, candidate)
        job_root = Path(job["paths"]["root"])
        job_root.mkdir(mode=0o700)
        (job_root / "preprocess").mkdir(mode=0o700)
        output = Path(job["paths"]["preprocess_output_root"])
        output.mkdir(mode=0o700)
        derived = output / "audio.flac"
        derived.write_bytes(b"derived")
        source = self.root / "raw-source"
        source.write_bytes(b"raw")
        plan = Path(job["paths"]["plan"])
        plan.write_bytes(b"plan")
        transcript = Path(job["paths"]["transcript"])
        transcript.write_bytes(b"transcript")
        completion = {"status": "completed"}
        completion_path = Path(job["paths"]["completion"])
        completion_path.write_bytes(campaign.canonical_bytes(completion))
        completion_path.chmod(0o400)
        receipt = campaign._cleanup_completed_cold_scratch(
            self.config, job, completion
        )
        self.assertEqual(receipt["status"], "completed")
        self.assertFalse(output.exists())
        self.assertEqual(source.read_bytes(), b"raw")
        self.assertEqual(plan.read_bytes(), b"plan")
        self.assertEqual(transcript.read_bytes(), b"transcript")

    def test_cold_preprocess_resume_accepts_full_acquisition_result_shape(self) -> None:
        acquisition_path = self.root / "acquisition-result.json"
        candidate = {
            "candidate_kind": "cold_schedule_completed_acquisition",
            "identity_sha256": "a" * 64,
            "acquisition_result": {
                "path": str(acquisition_path),
                "sha256": "b" * 64,
            },
            "source_media": {
                "sha256": "c" * 64,
                "byte_count": 123,
                "duration_ms": 4_564_586,
            },
        }
        job = campaign._job(self.config, candidate)
        job_root = Path(job["paths"]["root"])
        job_root.mkdir(mode=0o700)
        preprocess_root = job_root / "preprocess"
        preprocess_root.mkdir(mode=0o700)
        selection_path = Path(job["paths"]["preprocess_selection"])
        selection_path.write_bytes(b"replayed selection\n")
        selection_path.chmod(0o400)

        acquisition_result = {
            "byte_count": 8991,
            "completed_at": "2026-08-30T01:51:16Z",
            "job_id": "acq-fixture-000002",
            "path": str(acquisition_path),
            "sha256": "b" * 64,
            "work_order_sha256": "d" * 64,
        }
        selection = {"entries": [{"acquisition_result": acquisition_result}]}
        manifest = {"work_order_count": 1}
        orders = [{"operations": campaign.preprocess_batch.ASR_READY_OPERATIONS}]
        receipt = {
            "acquisition_result": {
                "path": str(acquisition_path),
                "sha256": "b" * 64,
            },
            "source_media": {
                "media_id": "media-fixture",
                "sha256": "c" * 64,
                "byte_count": 123,
            },
            "preprocess_result": {
                "path": str(self.root / "preprocess-result.json"),
                "sha256": "e" * 64,
            },
            "artifacts": [
                {
                    "artifact_kind": "audio_16khz_mono_flac",
                    "artifact_id": "audio-fixture",
                    "path": str(self.root / "audio.flac"),
                    "sha256": "f" * 64,
                    "byte_count": 456,
                }
            ],
        }
        bundle_path = self.root / "bundle" / "manifest.json"

        with (
            mock.patch.object(
                campaign.shutil,
                "disk_usage",
                return_value=mock.Mock(
                    free=campaign.HOT_SCRATCH_FREE_FLOOR_BYTES + 1
                ),
            ),
            mock.patch.object(
                campaign.preprocess_batch,
                "build_selection",
                return_value=selection,
            ),
            mock.patch.object(
                campaign.preprocess_batch,
                "read_selection",
                return_value=(selection, b"selection", selection_path),
            ),
            mock.patch.object(
                campaign.preprocess_batch,
                "materialize_bundle",
                return_value=bundle_path,
            ),
            mock.patch.object(
                campaign.preprocess_batch,
                "validate_bundle",
                return_value=(manifest, selection, orders),
            ),
            mock.patch.object(
                campaign.preprocess_batch,
                "existing_receipts",
                return_value={
                    1: {
                        "receipt": receipt,
                        "physical_sha256": "1" * 64,
                    }
                },
            ),
            mock.patch.object(campaign, "_strict_subprocess") as subprocess,
        ):
            result = campaign._cold_preprocess_source(self.config, candidate, job)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["media_id"], "media-fixture")
        self.assertEqual(result["audio"]["artifact_id"], "audio-fixture")
        subprocess.assert_not_called()

    def test_continuous_run_exits_cleanly_on_source_stop(self) -> None:
        with mock.patch.object(campaign, "_source_stop_requested", return_value=True):
            with mock.patch.object(campaign, "discover_candidates", return_value=[]):
                with mock.patch.object(campaign, "_persist_status") as persist:
                    result = campaign.run_continuous(self.config)
        self.assertEqual(result["status"], "stopped")
        self.assertEqual(result["cycles"], 0)
        persist.assert_called_once()

    def test_waiting_heartbeat_refreshes_only_small_validated_status_projection(
        self,
    ) -> None:
        status_path = Path(self.config.document["deployment"]["status_path"])
        original = {
            "kind": "himr_longform_asr_campaign_status",
            "schema_version": 1,
            "source_controller": {
                "config_id": self.config.document["source_controller"]["config_id"],
                "physical_sha256": self.config.document["source_controller"][
                    "physical_sha256"
                ],
            },
            "campaign_config": {
                "config_id": self.config.config_id,
                "physical_sha256": self.config.physical_sha256,
            },
            "lifecycle": "waiting",
            "expected_cold_backlog": 4,
            "discovered": {
                "cold_candidates": 1,
                "queue_candidates": 2,
                "total_candidates": 3,
            },
            "jobs": {
                "unprepared": 1,
                "preprocessed": 0,
                "prepared": 1,
                "incomplete": 0,
                "completed": 1,
            },
            "active_job": None,
            "updated_at": "2000-01-01T00:00:00Z",
            "last_error": None,
        }
        status_path.write_bytes(campaign.canonical_bytes(original))
        status_path.chmod(0o600)
        with (
            mock.patch.object(
                campaign,
                "discover_candidates",
                side_effect=AssertionError("heartbeat must not rediscover candidates"),
            ),
            mock.patch.object(
                campaign,
                "_load_cold_locator_manifest",
                side_effect=AssertionError("heartbeat must not replay cold locators"),
            ),
        ):
            refreshed = campaign._refresh_status_heartbeat(self.config)

        self.assertEqual(refreshed["lifecycle"], "waiting")
        self.assertNotEqual(refreshed["updated_at"], original["updated_at"])
        self.assertEqual(
            {key: value for key, value in refreshed.items() if key != "updated_at"},
            {key: value for key, value in original.items() if key != "updated_at"},
        )
        self.assertEqual(status_path.stat().st_mode & 0o777, 0o600)

    def test_continuous_primary_wait_polls_status_without_rediscovery(self) -> None:
        for reason in (
            "ordinary_gpu_lane_not_idle",
            "ordinary_gpu_work_observed_between_spans",
        ):
            with self.subTest(reason=reason):
                held = {
                    "status": "held",
                    "reason": reason,
                    "gpu_invoked": False,
                }
                with (
                    mock.patch.object(
                        campaign,
                        "_source_stop_requested",
                        side_effect=[False, False, False, True],
                    ),
                    mock.patch.object(
                        campaign, "run_once", return_value=held
                    ) as run_once,
                    mock.patch.object(
                        campaign, "_ordinary_gpu_busy", side_effect=[True, False]
                    ) as primary_busy,
                    mock.patch.object(
                        campaign, "discover_candidates", return_value=[]
                    ) as discover,
                    mock.patch.object(campaign, "_persist_status"),
                    mock.patch.object(
                        campaign, "_refresh_status_heartbeat"
                    ) as heartbeat,
                ):
                    pauses = []
                    result = campaign.run_continuous(
                        self.config,
                        sleep=pauses.append,
                        idle_seconds=7.5,
                    )

                self.assertEqual("stopped", result["status"])
                self.assertEqual(1, result["cycles"])
                run_once.assert_called_once_with(self.config)
                self.assertEqual(2, primary_busy.call_count)
                self.assertEqual([7.5], pauses)
                discover.assert_called_once_with(self.config)
                heartbeat.assert_called_once_with(self.config)

    def test_continuous_primary_poll_failure_persists_faulted_status(self) -> None:
        held = {
            "status": "held",
            "reason": "ordinary_gpu_lane_not_idle",
            "gpu_invoked": False,
        }
        failure = campaign.CampaignError("invalid source monitor fixture")
        with (
            mock.patch.object(campaign, "_source_stop_requested", return_value=False),
            mock.patch.object(campaign, "run_once", return_value=held),
            mock.patch.object(campaign, "_ordinary_gpu_busy", side_effect=failure),
            mock.patch.object(campaign, "discover_candidates", return_value=[]) as discover,
            mock.patch.object(campaign, "_persist_status") as persist,
        ):
            with self.assertRaisesRegex(
                campaign.CampaignError, "invalid source monitor fixture"
            ) as raised:
                campaign.run_continuous(self.config)

        self.assertIs(failure, raised.exception)
        discover.assert_called_once_with(self.config)
        persist.assert_called_once_with(
            self.config,
            [],
            lifecycle="faulted",
            last_error={
                "type": "CampaignError",
                "message": "invalid source monitor fixture",
            },
        )

    def test_runner_argv_honors_controller_stop_and_shared_lock_config(self) -> None:
        candidate = {
            "candidate_kind": "cold_schedule_completed_acquisition",
            "identity_sha256": "a" * 64,
            "schedule": {},
            "queue": {},
            "acquisition_result": {},
            "source_media": {"duration_ms": 1},
        }
        argv = campaign.runner_argv(self.config, campaign._job(self.config, candidate))
        self.assertIn("--honor-controller-stop", argv)
        self.assertIn("--yield-to-ordinary-gpu", argv)
        index = argv.index("--controller-config-sha256")
        self.assertEqual(argv[index + 1], self.controller.physical_sha256)

    def test_runner_lock_contention_is_a_normal_ordinary_gpu_wait(self) -> None:
        for message in (
            "GPU opportunity lock is occupied",
            "GPU UUID lock is occupied",
        ):
            with self.subTest(message=message):
                failure = campaign.canonical_bytes(
                    {
                        "status": "failed",
                        "error": {"type": "RunnerError", "message": message},
                    }
                )
                completed = campaign.subprocess.CompletedProcess(
                    args=["runner"], returncode=2, stdout=failure, stderr=b""
                )
                with mock.patch.object(
                    campaign.subprocess, "run", return_value=completed
                ):
                    result = campaign._strict_subprocess(
                        ["runner"], timeout=60, label="long-form runner"
                    )

                self.assertEqual(
                    result,
                    {"status": "held", "reason": "ordinary_gpu_lane_not_idle"},
                )

    def test_run_once_holds_prepared_job_while_ordinary_gpu_lane_is_not_idle(
        self,
    ) -> None:
        selected = {
            "candidate_kind": "gpu_queue_requires_chunking",
            "identity_sha256": "a" * 64,
        }
        job = campaign._job(self.config, selected)
        with (
            mock.patch.object(campaign, "discover_candidates", return_value=[selected]),
            mock.patch.object(campaign, "_reconcile_completed_cleanup"),
            mock.patch.object(campaign, "_select_next_candidate", return_value=selected),
            mock.patch.object(
                campaign,
                "prepare_candidate",
                return_value={"status": "prepared", "job": job},
            ),
            mock.patch.object(campaign, "_source_stop_requested", return_value=False),
            mock.patch.object(campaign, "_ordinary_gpu_busy", return_value=True),
            mock.patch.object(campaign, "_strict_subprocess") as subprocess,
            mock.patch.object(campaign, "_persist_status"),
        ):
            result = campaign.run_once(self.config)

        self.assertEqual(result["status"], "held")
        self.assertEqual(result["reason"], "ordinary_gpu_lane_not_idle")
        self.assertFalse(result["gpu_invoked"])
        subprocess.assert_not_called()

    def test_run_once_treats_between_span_priority_yield_as_waiting_not_stopped(
        self,
    ) -> None:
        selected = {
            "candidate_kind": "gpu_queue_requires_chunking",
            "identity_sha256": "a" * 64,
        }
        job = campaign._job(self.config, selected)
        yielded = {
            "status": "incomplete",
            "reason": "ordinary_gpu_work_observed_between_spans",
            "model_load_count": 1,
        }
        with (
            mock.patch.object(campaign, "discover_candidates", return_value=[selected]),
            mock.patch.object(campaign, "_reconcile_completed_cleanup"),
            mock.patch.object(campaign, "_select_next_candidate", return_value=selected),
            mock.patch.object(
                campaign,
                "prepare_candidate",
                return_value={"status": "prepared", "job": job},
            ),
            mock.patch.object(campaign, "_source_stop_requested", return_value=False),
            mock.patch.object(campaign, "_ordinary_gpu_busy", return_value=False),
            mock.patch.object(campaign, "_strict_subprocess", return_value=yielded),
            mock.patch.object(campaign, "_persist_status") as persist,
        ):
            result = campaign.run_once(self.config)

        self.assertEqual(result["status"], "held")
        self.assertEqual(
            result["reason"], "ordinary_gpu_work_observed_between_spans"
        )
        self.assertTrue(result["gpu_invoked"])
        self.assertEqual(persist.call_args.kwargs["lifecycle"], "waiting")

    def test_run_once_durable_stop_stays_stopped_and_never_checks_gpu_gate(self) -> None:
        selected = {
            "candidate_kind": "gpu_queue_requires_chunking",
            "identity_sha256": "a" * 64,
        }
        job = campaign._job(self.config, selected)
        with (
            mock.patch.object(campaign, "discover_candidates", return_value=[selected]),
            mock.patch.object(campaign, "_reconcile_completed_cleanup"),
            mock.patch.object(campaign, "_select_next_candidate", return_value=selected),
            mock.patch.object(
                campaign,
                "prepare_candidate",
                return_value={"status": "prepared", "job": job},
            ),
            mock.patch.object(campaign, "_source_stop_requested", return_value=True),
            mock.patch.object(campaign, "_ordinary_gpu_busy") as primary_busy,
            mock.patch.object(campaign, "_strict_subprocess") as subprocess,
            mock.patch.object(campaign, "_persist_status") as persist,
        ):
            result = campaign.run_once(self.config)

        self.assertEqual(result["status"], "held")
        self.assertEqual(result["reason"], "durable_stop_requested")
        self.assertEqual(persist.call_args.kwargs["lifecycle"], "stopped")
        primary_busy.assert_not_called()
        subprocess.assert_not_called()

    @staticmethod
    def primary_status(
        *,
        desired: str = "running",
        actual: str = "running",
        lifecycle: str = "running",
        ready_gpu_batches: int = 0,
        pending_gpu: int = 0,
        pending_gpu_items: int = 0,
        active_gpu_children: int = 0,
        buffered_gpu_items: int = 0,
        current_gpu_child: dict | None = None,
    ) -> dict:
        return {
            "desired_state": desired,
            "actual_state": actual,
            "lifecycle": lifecycle,
            "monitor": {
                "acquisition": {
                    "status": "progressed",
                    "pending": 2_257,
                },
                "preprocess": {
                    "status": "progressed",
                    "ready_items_after": 7,
                },
                "gpu_readiness": {
                    "status": "held",
                    "ready_batches": ready_gpu_batches,
                    "pending_batches": pending_gpu,
                    "pending_items": pending_gpu_items,
                    "active_children": active_gpu_children,
                    "buffered_ready_items": buffered_gpu_items,
                    "current_gpu_child": current_gpu_child,
                },
                "cold_retention": {
                    "status": "progressed",
                    "progressed": False,
                    "retained_items": 11,
                    "pending_items": 23,
                    "replay_pending_items": 5,
                },
            },
        }

    def test_primary_gate_holds_stale_faulted_status(self) -> None:
        value = self.primary_status(actual="faulted", lifecycle="faulted")
        with mock.patch.object(campaign, "read_public_status", return_value=value):
            self.assertTrue(campaign._ordinary_gpu_busy(self.config))

    def test_primary_gate_holds_starting_status_with_null_monitors(self) -> None:
        value = self.primary_status(actual="starting", lifecycle="starting")
        value["monitor"] = None
        with mock.patch.object(campaign, "read_public_status", return_value=value):
            self.assertTrue(campaign._ordinary_gpu_busy(self.config))

    def test_primary_gate_admits_idle_gpu_while_upstream_lanes_are_active(self) -> None:
        value = self.primary_status()
        with mock.patch.object(campaign, "read_public_status", return_value=value):
            self.assertFalse(campaign._ordinary_gpu_busy(self.config))

    def test_primary_gate_holds_for_every_ordinary_gpu_demand_signal(self) -> None:
        demand = (
            {"ready_gpu_batches": 1},
            {"pending_gpu": 1},
            {"pending_gpu_items": 1},
            {"active_gpu_children": 1},
            {"buffered_gpu_items": 1},
            {
                "current_gpu_child": {
                    "unit_name": "himr-gpu-asr-fixture.service",
                    "state": "running",
                }
            },
        )
        for changes in demand:
            with self.subTest(changes=changes):
                value = self.primary_status(**changes)
                with mock.patch.object(
                    campaign, "read_public_status", return_value=value
                ):
                    self.assertTrue(campaign._ordinary_gpu_busy(self.config))

    def test_primary_gate_keeps_gpu_demand_counters_strict(self) -> None:
        for field in (
            "ready_batches",
            "pending_batches",
            "pending_items",
            "active_children",
            "buffered_ready_items",
        ):
            for invalid in (None, False, -1):
                with self.subTest(field=field, invalid=invalid):
                    value = self.primary_status()
                    value["monitor"]["gpu_readiness"][field] = invalid
                    with mock.patch.object(
                        campaign, "read_public_status", return_value=value
                    ):
                        with self.assertRaisesRegex(
                            campaign.CampaignError,
                            rf"source public gpu_readiness\.{field} counter is invalid",
                        ):
                            campaign._ordinary_gpu_busy(self.config)

    def test_queue_discovery_replays_immutable_receipt_without_source_payload(self) -> None:
        queue_id = "gpuasrqueue_" + "a" * 32
        queues = self.root / "source-queues" / "queues"
        queues.mkdir(parents=True, mode=0o700)
        queue_root = queues / queue_id
        queue_root.mkdir(mode=0o700)
        manifest_path = queue_root / "manifest.json"
        queue_body = b"{}\n"
        manifest_path.write_bytes(queue_body)
        manifest_path.chmod(0o400)
        queue_root.chmod(0o500)
        candidate_core = {
            "candidate_kind": "gpu_queue_requires_chunking",
            "queue": {
                "path": str(manifest_path),
                "sha256": hashlib.sha256(queue_body).hexdigest(),
                "queue_id": queue_id,
            },
            "member": {
                "ordinal": 1,
                "member_id": "gpuasrmember_" + "b" * 32,
                "identity_sha256": "c" * 64,
            },
            "audio": {
                "artifact_id": "artifact",
                "media_id": "media",
                "path": str(self.root / "audio.flac"),
                "sha256": "d" * 64,
                "byte_count": 1,
                "duration_ms": 421_000,
            },
            "preprocess_result": {
                "path": str(self.root / "result.json"),
                "sha256": "e" * 64,
            },
            "disposition": {
                "state": "requires_chunking",
                "reasons": ["maximum_audio_duration_exceeded"],
            },
        }
        candidate = {
            **candidate_core,
            "identity_sha256": hashlib.sha256(
                campaign.canonical_bytes(candidate_core)
            ).hexdigest(),
        }
        queue = {"queue_id": queue_id, "identity_sha256": "f" * 64}
        with (
            mock.patch.object(campaign, "_queue_anchor_documents", return_value=({}, {}, {})),
            mock.patch.object(
                campaign,
                "_shallow_queue_manifest",
                return_value=(queue, queue_body, [candidate]),
            ) as shallow,
        ):
            first = campaign.discover_gpu_queue_candidates(
                self.config, admit_new=True
            )
            second = campaign.discover_gpu_queue_candidates(self.config)
        self.assertEqual(first, second)
        self.assertEqual(first, [candidate])
        shallow.assert_called_once()
        receipt = self.deployment / "discovery" / "queue-receipts" / f"{queue_id}.json"
        self.assertEqual(receipt.stat().st_mode & 0o777, 0o400)

    def _queue_receipt_fixture(self):
        queue_id = "gpuasrqueue_" + "a" * 32
        parent = self.root / queue_id
        parent.mkdir(mode=0o700)
        path = parent / "manifest.json"
        body = b"{}\n"
        path.write_bytes(body)
        path.chmod(0o400)
        receipt = campaign._queue_discovery_receipt(
            path, {"queue_id": queue_id, "identity_sha256": "b" * 64}, body, []
        )
        return path, body, receipt

    def test_queue_receipt_unchanged_witness_avoids_envelope_reread(self) -> None:
        path, _body, receipt = self._queue_receipt_fixture()
        with mock.patch.object(campaign, "_stable_file") as read:
            self.assertEqual(campaign._validate_queue_discovery_receipt(receipt, path), receipt)
        read.assert_not_called()

    def test_queue_receipt_changed_device_rehashes_only_manifest(self) -> None:
        path, body, receipt = self._queue_receipt_fixture()
        metadata = dict(receipt["queue"]["file_metadata"])
        metadata["device"] += 1
        original = campaign.canonical_bytes(receipt)
        with (
            mock.patch.object(campaign, "_queue_file_metadata", return_value=metadata),
            mock.patch.object(campaign, "_stable_file", wraps=campaign._stable_file) as read,
        ):
            self.assertEqual(campaign._validate_queue_discovery_receipt(receipt, path), receipt)
        read.assert_called_once_with(
            path, "source GPU queue manifest", maximum=32 * 1024 * 1024,
            allowed_modes=frozenset({0o400}),
        )
        self.assertEqual(campaign.canonical_bytes(receipt), original)
        self.assertEqual(path.read_bytes(), body)

    def test_queue_receipt_accepts_exact_inode_changing_copy(self) -> None:
        path, body, receipt = self._queue_receipt_fixture()
        copied = path.with_name("copied.json")
        copied.write_bytes(body)
        copied.chmod(0o400)
        os.replace(copied, path)
        self.assertNotEqual(path.stat().st_ino, receipt["queue"]["file_metadata"]["inode"])
        self.assertEqual(campaign._validate_queue_discovery_receipt(receipt, path), receipt)

    def test_queue_receipt_rejects_changed_bytes_after_copy(self) -> None:
        path, _body, receipt = self._queue_receipt_fixture()
        changed = path.with_name("changed.json")
        changed.write_bytes(b"[]\n")
        changed.chmod(0o400)
        os.replace(changed, path)
        with self.assertRaisesRegex(campaign.CampaignError, "SHA-256 differs"):
            campaign._validate_queue_discovery_receipt(receipt, path)

    def test_queue_receipt_portability_never_accepts_writable_or_linked_files(self) -> None:
        path, _body, receipt = self._queue_receipt_fixture()
        path.chmod(0o600)
        with self.assertRaisesRegex(campaign.CampaignError, "unsafe metadata"):
            campaign._validate_queue_discovery_receipt(receipt, path)
        path.chmod(0o400)
        os.link(path, path.with_name("extra-link.json"))
        with self.assertRaisesRegex(campaign.CampaignError, "unsafe metadata"):
            campaign._validate_queue_discovery_receipt(receipt, path)


if __name__ == "__main__":
    unittest.main()
