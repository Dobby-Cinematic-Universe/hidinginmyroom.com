from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipeline import longform_asr_campaign as campaign
from pipeline import salad_transcription_inventory as inventory


class CloudInventoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = campaign.CampaignConfig(
            {
                "config_id": "himrlongcfg_" + "a" * 32,
                "identity_sha256": "b" * 64,
                "deployment": {"jobs_root": str(self.root / "jobs")},
            },
            self.root / "campaign.json",
            "c" * 64,
        )
        (self.root / "jobs").mkdir(mode=0o700)
        self.candidates: list[dict] = []
        self.load = self.enterContext(mock.patch.object(inventory, "_load_config", return_value=self.config))
        self.discover = self.enterContext(mock.patch.object(campaign, "discover_candidates", return_value=self.candidates))

    def write_json(self, path: Path, value: dict) -> bytes:
        body = campaign.canonical_bytes(value)
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(body)
        path.chmod(0o400)
        return body

    def candidate(self, ordinal: int = 1, *, prepared: bool = True) -> dict:
        source = {
            "candidate_kind": "gpu_queue_requires_chunking",
            "identity_sha256": f"{ordinal:064x}",
            "audio": {
                "artifact_id": f"audio-{ordinal}",
                "path": str(self.root / f"absent-audio-{ordinal}.flac"),
                "sha256": "d" * 64,
                "byte_count": 123,
                "duration_ms": 1_000,
                "media_id": f"media-{ordinal}",
            },
        }
        self.candidates.append(source)
        job = campaign._job(self.config, source)
        if prepared:
            Path(job["paths"]["root"]).mkdir(mode=0o700)
            self.write_json(Path(job["paths"]["root"]) / "job.json", job)
            self.write_json(Path(job["paths"]["recording_input"]), {
                "kind": "himr_longform_recording_input_manifest",
                "schema_version": 1,
                "boundary_candidates": [],
                "recording": {
                    "recording_id": job["job_id"],
                    "media_id": source["audio"]["media_id"],
                    "input": {
                        **{key: source["audio"][key] for key in (
                            "artifact_id", "path", "sha256", "byte_count", "duration_ms"
                        )},
                        "channels": 1,
                        "sample_rate_hz": 16_000,
                        "total_samples": 16_000,
                    },
                },
            })
        return job

    def complete(self, job: dict) -> dict:
        transcript = Path(job["paths"]["transcript"])
        # This intentionally is not JSON; inventory must never open or parse it.
        transcript.write_bytes(b"private transcript body")
        transcript.chmod(0o400)
        binding = {
            "path": str(transcript),
            "sha256": "e" * 64,
            "byte_count": transcript.stat().st_size,
        }
        value = {
            "kind": "himr_longform_asr_campaign_completion",
            "schema_version": 1,
            "job_id": job["job_id"],
            "campaign_config_id": self.config.config_id,
            "candidate_identity_sha256": job["source"]["identity_sha256"],
            "transcript": binding,
            "runner": {
                "status": "completed",
                "bindings_path": job["paths"]["bindings"],
                "bindings_sha256": "f" * 64,
            },
            "assembler": {"status": "completed", "coverage_complete": True, "output": copy.deepcopy(binding)},
            "policy": {
                "machine_generated": True,
                "human_review_required": True,
                "publication_authority": "none",
                "catalogue_mutation_authority": "none",
            },
        }
        self.write_json(Path(job["paths"]["completion"]), value)
        return value

    def run_inventory(self, **kwargs) -> dict:
        return inventory.inventory(self.config.path, self.config.physical_sha256, **kwargs)

    def test_prepared_selection_reads_only_small_metadata(self) -> None:
        job = self.candidate()
        original = campaign._stable_file
        read_paths: list[Path] = []

        def tracking(path: Path, *args, **kwargs):
            read_paths.append(path)
            return original(path, *args, **kwargs)

        with mock.patch.object(campaign, "_stable_file", side_effect=tracking):
            value = self.run_inventory()
        self.assertEqual(value["kind"], "himr_salad_input_selection")
        self.assertEqual(value["counts"]["selected"], 1)
        path = Path(job["paths"]["recording_input"])
        self.assertEqual(value["recordings"], [{
            "recording_input": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()
        }])
        self.assertEqual(set(read_paths), {path, path.parent / "job.json"})
        self.assertFalse(value["policy"]["media_content_verified"])
        self.discover.assert_called_once_with(self.config, admit_new_cold=False, admit_new_queues=False)

    def test_completed_excluded_without_opening_transcript(self) -> None:
        job = self.candidate()
        self.complete(job)
        with mock.patch.object(campaign, "_stable_file", wraps=campaign._stable_file) as read:
            value = self.run_inventory()
        self.assertEqual(value["recordings"], [])
        self.assertEqual(value["counts"]["completed"], 1)
        self.assertEqual([call.args[0] for call in read.call_args_list], [Path(job["paths"]["completion"])])
        self.assertFalse(value["policy"]["transcript_content_verified"])

    def test_missing_recording_input_excluded_without_preparation(self) -> None:
        self.candidate(prepared=False)
        with mock.patch.object(campaign, "prepare_candidate", side_effect=AssertionError("must not prepare")):
            value = self.run_inventory()
        self.assertEqual(value["recordings"], [])
        self.assertEqual(value["counts"]["needs_audio_preparation"], 1)

    def test_partial_span_directory_excluded_by_default(self) -> None:
        job = self.candidate()
        results = Path(job["paths"]["results"])
        results.mkdir(mode=0o700)
        (results / "interrupted-span").mkdir(mode=0o700)
        value = self.run_inventory()
        self.assertEqual(value["recordings"], [])
        self.assertEqual(value["counts"]["partial_local_work"], 1)

    def test_partial_optin_warns_whole_recording_retranscription(self) -> None:
        job = self.candidate()
        self.write_json(Path(job["paths"]["bindings"]), {})
        value = self.run_inventory(include_partial=True)
        self.assertEqual(value["counts"]["selected"], 1)
        self.assertEqual(value["counts"]["included_partial_local_work"], 1)
        self.assertEqual(value["partial_local_job_ids"], [job["job_id"]])
        self.assertTrue(value["policy"]["whole_recording_paid_retranscription_warning"])
        self.assertFalse(value["policy"]["cloud_resumes_local_spans"])

    def test_empty_results_directory_is_not_partial_work(self) -> None:
        job = self.candidate()
        Path(job["paths"]["results"]).mkdir(mode=0o700)
        self.assertEqual(self.run_inventory()["counts"]["selected"], 1)

    def test_completion_job_binding_corruption_fails_closed(self) -> None:
        job = self.candidate()
        value = self.complete(job)
        value["candidate_identity_sha256"] = "1" * 64
        self.write_json(Path(job["paths"]["completion"]), value)
        with self.assertRaisesRegex(inventory.CloudInventoryError, "different job binding"):
            self.run_inventory()

    def test_completion_assembler_hash_mismatch_fails_closed(self) -> None:
        job = self.candidate()
        value = self.complete(job)
        value["assembler"]["output"]["sha256"] = "1" * 64
        self.write_json(Path(job["paths"]["completion"]), value)
        with self.assertRaisesRegex(inventory.CloudInventoryError, "execution receipts"):
            self.run_inventory()

    def test_completion_missing_transcript_fails_closed(self) -> None:
        job = self.candidate()
        self.complete(job)
        Path(job["paths"]["transcript"]).unlink()
        with self.assertRaisesRegex(inventory.CloudInventoryError, "absent or has a different size"):
            self.run_inventory()

    def test_completion_duplicate_keys_fail_closed(self) -> None:
        job = self.candidate()
        self.complete(job)
        path = Path(job["paths"]["completion"])
        path.chmod(0o600)
        path.write_bytes(b'{"kind":"x","kind":"y"}\n')
        path.chmod(0o400)
        with self.assertRaisesRegex(inventory.CloudInventoryError, "repeats key"):
            self.run_inventory()

    def test_foreign_recording_id_fails_closed(self) -> None:
        job = self.candidate()
        path = Path(job["paths"]["recording_input"])
        manifest = json.loads(path.read_bytes())
        manifest["recording"]["recording_id"] = "other-recording"
        self.write_json(path, manifest)
        with self.assertRaisesRegex(inventory.CloudInventoryError, "different local job"):
            self.run_inventory()

    def test_queue_audio_hash_mismatch_fails_closed(self) -> None:
        job = self.candidate()
        path = Path(job["paths"]["recording_input"])
        manifest = json.loads(path.read_bytes())
        manifest["recording"]["input"]["sha256"] = "3" * 64
        self.write_json(path, manifest)
        with self.assertRaisesRegex(inventory.CloudInventoryError, "admitted queue audio"):
            self.run_inventory()

    def test_missing_committed_job_receipt_fails_closed(self) -> None:
        job = self.candidate()
        (Path(job["paths"]["root"]) / "job.json").unlink()
        with self.assertRaisesRegex(inventory.CloudInventoryError, "no committed local job"):
            self.run_inventory()

    def test_symlink_manifest_fails_closed(self) -> None:
        job = self.candidate()
        path = Path(job["paths"]["recording_input"])
        other = self.root / "other-manifest.json"
        path.rename(other)
        path.symlink_to(other)
        with self.assertRaisesRegex(inventory.CloudInventoryError, "unsafe metadata"):
            self.run_inventory()

    def test_partial_work_appearing_during_snapshot_is_excluded(self) -> None:
        self.candidate()
        with mock.patch.object(inventory, "_partial", side_effect=[False, True]):
            self.assertEqual(self.run_inventory()["counts"]["partial_local_work"], 1)

    def test_duplicate_jobs_fail_closed(self) -> None:
        self.candidate()
        self.candidates.append(copy.deepcopy(self.candidates[0]))
        with self.assertRaisesRegex(inventory.CloudInventoryError, "repeated a local job"):
            self.run_inventory()

    def test_wrong_partial_parameter_type_fails_closed(self) -> None:
        with self.assertRaisesRegex(inventory.CloudInventoryError, "boolean"):
            self.run_inventory(include_partial="false")


class MetadataConfigTests(unittest.TestCase):
    def test_config_hash_pin_without_execution_input_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            document = {"example": "pinned config body"}
            body = campaign.canonical_bytes(document)
            path.write_bytes(body)
            path.chmod(0o400)
            with (
                mock.patch.object(campaign, "_normalize_config", return_value=document) as validate,
                mock.patch.object(campaign, "_verify_external_inputs", side_effect=AssertionError("no executable replay")),
                mock.patch.object(campaign, "load_campaign_config", side_effect=AssertionError("no execution loader")),
            ):
                loaded = inventory._load_config(path, hashlib.sha256(body).hexdigest())
            self.assertEqual(loaded.document, document)
            validate.assert_called_once_with(document)
            with self.assertRaisesRegex(inventory.CloudInventoryError, "supplied SHA-256"):
                inventory._load_config(path, "0" * 64)


if __name__ == "__main__":
    unittest.main()
