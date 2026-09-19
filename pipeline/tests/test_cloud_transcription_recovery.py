"""No-network recovery tests using real immutable source/screen/audio proofs."""
from copy import deepcopy
import hashlib
from pathlib import Path
import unittest
from unittest import mock
import wave

from pipeline import cloud_transcription_recovery as recovery
from pipeline import cloud_transcription_client as clients
from pipeline import cloud_transcription_media as media
from pipeline import cloud_transcription_screen as screen
from pipeline import transcript_summary as io
from pipeline.tests import test_cloud_transcription_screen as fixtures


class PaidRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ScreenAdapterTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        f = self.fixture
        f.complete(no_audio=True)
        self.base = f.base
        self.recording = {**deepcopy(f.recording), "state": "ready", "reasons": [],
                          "date": {"value": "2026-09-13", "basis": "test"},
                          "source_ids": {"youtube": ["xKuOtWjOCaA"], "archive_native": []}}
        self.root = self.base / "producer"
        self.root.mkdir(mode=0o700)
        for name in ("jobs", "reservations", "imports", "screen-worker"):
            (self.root / name).mkdir(mode=0o700)
        self.out = self.base / "admission-output"
        code_root = self.base / "code"
        code_root.mkdir(mode=0o700)
        files = {name: io.put_bytes(code_root / name, ("# Bound fixture snapshot: " + name + "\n").encode())
                 for name in sorted(recovery.PRODUCER_FILES)}
        self.implementation = {name: ref["sha256"] for name, ref in files.items()}
        self.inventory = io.put(self.base / "cloud-inventory.json", {
            "kind": "himr_cloud_transcription_archive_inventory", "schema_version": 1,
            "recordings": [self.recording]})
        third = io.put(self.base / "third.json", {"kind": "himr_cloud_third_party_inventory", "entries": []})
        matching = io.put(self.base / "matching.json", {"recording_ids": [self.recording["recording_id"]],
                                                       "matches": [{"status": "missing"}]})
        self.identity = recovery._job_id(self.recording)
        amount = recovery._cost("assemblyai", self.recording["duration_ms"], True)
        self.row = {"job_id": self.identity, "recording": self.recording, "provider": "assemblyai",
                    "disposition": "cloud", "reason": "within_assemblyai_whole_recording_limit", "import": None,
                    "maximum_cost_microusd": amount, "match_status": "missing"}
        self.plan = {"kind": recovery.PRODUCER_KIND, "schema_version": 1, "state_root": str(self.root),
            "inventory": self.inventory, "third_party_inventory": third, "matching": matching,
            "implementation": self.implementation, "ffmpeg": io.binding("/usr/bin/ffmpeg"),
            "screen_config": f.config_ref, "rates_microusd_hour": recovery.RATES,
            "policy": recovery.POLICY, "recordings": [self.row]}
        self.plan_ref = io.put(self.root / "plan.json", self.plan)
        self.code = {"kind": "himr_paid_cloud_v3_code_snapshot", "schema_version": 1,
            "old_plan": self.plan_ref, "implementation": self.implementation, "files": files,
            "snapshot_before_english_locale_compatibility_fix": True, "source_mutation": False, "new_paid_requests": 0}
        self.code_ref = io.put(self.base / "code-proof.json", self.code)
        io.put(self.root / "workspace.json", {"kind": recovery.PRODUCER_KIND + "_workspace", "schema_version": 1,
            "inventory": self.inventory, "screen_config": f.config_ref, "implementation": self.implementation})
        io.put(self.root / "spending-limit.json", {"kind": "himr_cloud_spending_limit", "plan": self.plan_ref,
            "maximum_microusd": 150000000, "scope": "this_workspace_not_provider_account", "automatic_hold_release": False})
        self.folder = self.root / "jobs" / self.identity
        self.folder.mkdir(mode=0o700)
        io.put(self.folder / "job.json", {"plan": self.plan_ref, "recording": self.row})
        decision = screen.screen_one(self.recording, f.folder, f.config_ref)
        self.screen_ref = io.put(self.folder / "screen.json", decision)
        self.wav = self.folder / "audio.wav"
        with wave.open(str(self.wav), "wb") as audio:
            audio.setnchannels(1)
            audio.setsampwidth(2)
            audio.setframerate(16000)
            audio.writeframes(b"\0\0" * 960000)
        self.audio = media.inspect_wav(self.wav, self.recording["duration_ms"])
        io.put(self.folder / "audio.json", {"kind": "himr_cloud_prepared_audio", "schema_version": 1,
            "source": self.recording["media"], "ffmpeg": self.plan["ffmpeg"], "audio": self.audio,
            "whole_recording": True, "speech_filter": False, "cuts": False})
        intent = {"kind": "himr_cloud_paid_intent", "schema_version": 1, "plan": self.plan_ref,
            "job_id": self.identity, "recording_id": self.recording["recording_id"], "provider": "assemblyai",
            "audio": self.audio, "screen_decision": self.screen_ref, "diarization": decision["diarization"],
            "request_metadata": self.identity + "_" + io.digest({"audio": self.audio, "screen": self.screen_ref})[:24],
            "maximum_cost_microusd": amount}
        io.put(self.folder / "intent.json", intent)
        self.reservation = self.root / "reservations" / (self.identity + ".json")
        io.put(self.reservation, intent)
        url = "https://cdn.assemblyai.com/upload/project-fixture/upload-fixture"
        io.put(self.folder / "upload.json", {"upload_url": url})
        options = clients.assemblyai_options(url, diarization=True)
        self.submission = {**options, "id": "fixture-paid-job", "status": "processing",
                           "language_code": "en_us", "language_detection": False}
        io.put(self.folder / "submission.json", self.submission)
        word = {"start": 100, "end": 700, "speaker": "A", "text": "Hello.", "confidence": .99}
        self.terminal = {**self.submission, "status": "completed", "speech_model_used": clients.ASSEMBLYAI_MODEL,
            "audio_duration": 60, "text": "Hello.", "words": [word],
            "utterances": [{"speaker": "A", "start": 100, "end": 700, "text": "Hello.", "words": [word]}]}
        io.put(self.folder / "terminal-job.json", self.terminal)

    def replace(self, path, value):
        path.chmod(0o600)
        path.write_bytes(io.canonical(value))
        path.chmod(0o400)
        return io.binding(path)

    def recover(self):
        return recovery.recover_completed(self.plan_ref, self.code_ref, self.out)

    def test_successful_locale_recovery_preserves_old_paid_proofs(self):
        old = {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
               for path in self.root.rglob("*") if path.is_file()}
        with mock.patch.object(clients.AssemblyAIClient, "submit", side_effect=AssertionError("no paid request")), \
             mock.patch.object(clients.AssemblyAIClient, "poll", side_effect=AssertionError("no remote GET")):
            catalog_ref = self.recover()
            catalog = recovery.load_catalog(catalog_ref)
            result = recovery.load_admission(catalog["admissions"][0]["admission"], self.recording)
        self.assertEqual(old, {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
                               for path in self.root.rglob("*") if path.is_file()})
        self.assertEqual(result["prior_reserved_microusd"], self.row["maximum_cost_microusd"])
        transcript = io.read(result["transcript"])
        self.assertEqual(transcript["text"], "Hello.")
        self.assertEqual(transcript["raw_result"]["path"], str(self.folder / "terminal-job.json"))
        self.assertEqual(transcript["screen_decision"], self.screen_ref)
        self.assertNotIn("words", transcript["segments"][0])
        self.assertFalse(transcript["speaker_identity_inferred"])
        self.assertEqual(io.read(io.binding(self.folder / "terminal-job.json"))["language_code"], "en_us")
        self.assertFalse(list(self.out.rglob("intent.json")))
        self.assertFalse(list(self.out.rglob("submission.json")))

    def test_changed_code_snapshot_is_rejected(self):
        path = Path(self.code["files"]["cloud_transcription_client.py"]["path"])
        path.chmod(0o600)
        path.write_text("# changed\n")
        with self.assertRaises(RuntimeError):
            self.recover()
        self.assertFalse(self.out.exists())

    def test_incomplete_code_proof_is_rejected(self):
        changed = deepcopy(self.code)
        del changed["files"]["cloud_transcription_client.py"]
        self.code_ref = self.replace(Path(self.code_ref["path"]), changed)
        with self.assertRaises(recovery.RecoveryError):
            self.recover()

    def test_wrong_plan_hash_rejected(self):
        self.plan_ref = {**self.plan_ref, "sha256": "f"*64}
        with self.assertRaises(RuntimeError):
            self.recover()

    def test_pending_paid_job_blocks_entire_catalog(self):
        (self.folder / "terminal-job.json").unlink()
        with self.assertRaisesRegex(recovery.RecoveryError, "pending or unreconciled"):
            self.recover()
        self.assertFalse(self.out.exists())

    def test_failed_paid_job_blocks_entire_catalog(self):
        self.replace(self.folder / "terminal-job.json", {**self.terminal, "status": "error"})
        with self.assertRaisesRegex(recovery.RecoveryError, "did not succeed"):
            self.recover()

    def test_unknown_reservation_cannot_be_hidden(self):
        io.put(self.root / "reservations" / "unknown.json", {})
        with self.assertRaisesRegex(recovery.RecoveryError, "unaccounted producer reservation"):
            self.recover()

    def test_unknown_job_directory_cannot_be_hidden(self):
        (self.root / "jobs" / "unknown").mkdir(mode=0o700)
        with self.assertRaisesRegex(recovery.RecoveryError, "unaccounted producer job"):
            self.recover()

    def test_untrusted_submission_response_blocks_recovery(self):
        io.put(self.folder / "submission-untrusted-response.json", {})
        with self.assertRaisesRegex(recovery.RecoveryError, "unaccounted producer job or artifact"):
            self.recover()

    def test_paid_receipt_without_original_intent_rejected(self):
        (self.folder / "intent.json").unlink()
        self.reservation.unlink()
        with self.assertRaisesRegex(recovery.RecoveryError, "lacks original intent"):
            self.recover()

    def test_reservation_without_intent_rejected(self):
        (self.folder / "intent.json").unlink()
        with self.assertRaisesRegex(recovery.RecoveryError, "incomplete paid"):
            self.recover()

    def test_reduced_original_reservation_rejected(self):
        value = io.read(io.binding(self.reservation))
        value["maximum_cost_microusd"] -= 1
        self.replace(self.reservation, value)
        with self.assertRaisesRegex(recovery.RecoveryError, "intent or reservation differs"):
            self.recover()

    def test_wrong_remote_job_identity_rejected(self):
        self.replace(self.folder / "terminal-job.json", {**self.terminal, "id": "different-job"})
        with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
            self.recover()

    def test_wrong_uploaded_audio_echo_rejected(self):
        self.replace(self.folder / "terminal-job.json", {**self.terminal,
            "audio_url": "https://cdn.assemblyai.com/upload/other"})
        with self.assertRaisesRegex(recovery.RecoveryError, "option echo differs"):
            self.recover()

    def test_wrong_request_options_rejected(self):
        for field, value in (("speaker_labels", False), ("punctuate", False), ("filter_profanity", True),
                             ("language_detection", True), ("language_code", "fr")):
            with self.subTest(field=field):
                self.replace(self.folder / "terminal-job.json", {**self.terminal, field: value})
                with self.assertRaises(RuntimeError):
                    recovery.inspect_producer(self.plan_ref, self.code_ref)

    def test_changed_media_bytes_rejected(self):
        Path(self.recording["media"]["path"]).write_bytes(b"changedxxx")
        with self.assertRaises(RuntimeError):
            self.recover()

    def test_changed_prepared_audio_rejected(self):
        self.wav.write_bytes(b"not an audio file")
        with self.assertRaises((RuntimeError, wave.Error)):
            self.recover()

    def test_existing_output_root_rejected(self):
        self.out.mkdir(mode=0o700)
        with self.assertRaisesRegex(recovery.RecoveryError, "fresh"):
            self.recover()

    def test_new_paid_marker_after_admission_invalidates_replay(self):
        catalog = self.recover()
        io.put(self.root / "reservations" / "unknown.json", {})
        with self.assertRaises(recovery.RecoveryError):
            recovery.load_catalog(catalog)

    def test_source_output_or_admission_tampering_rejected(self):
        reference = self.recover()
        catalog = io.read(reference)
        admission_ref = catalog["admissions"][0]["admission"]
        admitted = io.read(admission_ref)
        body = io.read(admitted["transcript"])
        body["text"] = "Fabricated."
        self.replace(Path(admitted["transcript"]["path"]), body)
        with self.assertRaises(RuntimeError):
            recovery.load_admission(admission_ref, self.recording)

    def test_catalog_cannot_omit_the_paid_job_or_reserve(self):
        reference = self.recover()
        value = io.read(reference)
        value["admissions"] = []
        changed = self.replace(Path(reference["path"]), value)
        with self.assertRaisesRegex(recovery.RecoveryError, "omits"):
            recovery.load_catalog(changed)

    def test_admission_for_other_recording_rejected(self):
        catalog = recovery.load_catalog(self.recover())
        changed = deepcopy(self.recording)
        changed["duration_ms"] += 1
        with self.assertRaisesRegex(recovery.RecoveryError, "recording"):
            recovery.load_admission(catalog["admissions"][0]["admission"], changed)

    def test_catalog_cannot_release_the_prior_hold(self):
        reference = self.recover()
        value = io.read(reference)
        value["prior_reserved_microusd"] = 0
        changed = self.replace(Path(reference["path"]), value)
        with self.assertRaisesRegex(recovery.RecoveryError, "loses reservations"):
            recovery.load_catalog(changed)

    def test_duplicate_catalog_admission_is_rejected(self):
        reference = self.recover()
        value = io.read(reference)
        value["admissions"].append(deepcopy(value["admissions"][0]))
        changed = self.replace(Path(reference["path"]), value)
        with self.assertRaisesRegex(recovery.RecoveryError, "omits producer jobs"):
            recovery.load_catalog(changed)

    def test_admission_cannot_escape_its_provenance_root(self):
        catalog = recovery.load_catalog(self.recover())
        value = io.read(catalog["admissions"][0]["admission"])
        elsewhere = io.put(self.base / "elsewhere.json", value)
        with self.assertRaisesRegex(recovery.RecoveryError, "provenance"):
            recovery.load_admission(elsewhere, self.recording)

    def test_changed_normalizer_contract_invalidates_admission(self):
        reference = self.recover()
        with mock.patch.object(recovery, "implementation", return_value={}):
            with self.assertRaisesRegex(recovery.RecoveryError, "implementation"):
                recovery.load_catalog(reference)

    def test_missing_option_echo_is_not_assumed(self):
        incomplete = deepcopy(self.terminal)
        del incomplete["format_text"]
        self.replace(self.folder / "terminal-job.json", incomplete)
        with self.assertRaisesRegex(recovery.RecoveryError, "option echo differs"):
            self.recover()


if __name__ == "__main__":
    unittest.main()
