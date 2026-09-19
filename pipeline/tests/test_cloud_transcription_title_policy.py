"""Model-free title routing and immutable sampled-proof overlay tests."""
from copy import deepcopy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import cloud_transcription_archive as archive
from pipeline import cloud_transcription_title_policy as titles
from pipeline import cloud_transcription_screen as screen


class MatchingTests(unittest.TestCase):
    def test_explicit_interaction_titles(self):
        for title in (
            "Interview with Nobita", "Nobita interview", "Interviewing Mila",
            "Q&A with my wife", "Q + A WITH MY SISTER", "Questions and answers with Chihiro",
            "Answering questions with my girlfriend", "A conversation with my dad",
            "Talking to my sister", "Speaking with Pia", "Chatting to an old friend",
            "Debate with a friend", "Phone call with my wife", "Calling my mother",
            "Skype call", "On a video call", "Discord call with viewers",
            "Livestream with my wife", "Live stream with Mila", "Joined by my sister",
            "2026-09-12_Q＆A_with_my_wife_[aBc123_-DEF].mp4",
            "[Interview with Mila] [aBc123_-DEF]", "Conversation with José",
            "A conversation with someone", "Calling my wife on Skype",
        ):
            with self.subTest(title=title):
                result = titles.match_titles({"title": title})
                self.assertTrue(result["matched"], result)
                self.assertTrue(result["reasons"])

    def test_mentions_monologues_tutorials_and_non_conversation_calls(self):
        for title in (
            "Thoughts about my girlfriend", "Talking about my wife", "My phone didn't arrive",
            "Q&A", "Q&A with me", "Q&A about my wife", "Talking to myself", "Talking to the camera",
            "Speaking to a camera", "Talking to God", "Conversation with ChatGPT", "Talking with chat",
            "Calling out my haters", "Calling myself a genius", "Call of Duty",
            "How to call my mother", "How to interview with confidence", "Job interview tips",
            "Preparing for an interview with an employer", "My thoughts about the Nobita interview",
            "Reaction to an interview with my wife", "Watching a conversation with Mila",
            "Story about a phone call", "Missed phone call from my wife", "Waiting for a Skype call",
            "No phone call", "Phone call never happened", "Fake interview with Pia",
            "Not an interview with Mila", "Living with my girlfriend", "My job interview",
            "Sunny day", "Thoughts about a conversation with my girlfriend",
            "Phone call with myself", "Phone call settings", "Phone call tutorial",
            "Skype call with ChatGPT", "My Skype call isn't working", "Testing video calls",
            "Calling my girlfriend a liar", "Calling my wife names",
        ):
            with self.subTest(title=title):
                self.assertFalse(titles.match_titles({"title": title})["matched"])

    def test_alias_matches_without_promoting_title_to_audio_evidence(self):
        recording = {"title": "Day 123", "aliases": [{"title": None}, {"title": "Q&A with my sister"}]}
        result = titles.match_titles(recording)
        self.assertEqual(result["matched_titles"][0]["source"], "aliases[1].title")
        self.assertEqual(result["reasons"], ["joint_question_and_answer"])
        self.assertEqual(titles.match_titles(recording), result)

    def test_missing_titles_do_not_manufacture_evidence(self):
        self.assertEqual(titles.match_titles({}), {"matched": False, "reasons": [], "matched_titles": []})
        self.assertFalse(titles.match_titles({"title": "", "aliases": [{}]})["matched"])

    def test_input_bounds_and_types(self):
        invalid = [None, {"title": 2}, {"title": "x" * 8193}, {"aliases": {}},
                   {"aliases": [None]}, {"aliases": [{}] * 1025},
                   {"aliases": [{"title": "x" * 8192}] * 33}]
        for recording in invalid:
            with self.subTest(recording_type=type(recording)):
                with self.assertRaises(titles.TitlePolicyError):
                    titles.match_titles(recording)

    def test_matching_is_pure_and_does_not_read_media_or_policy(self):
        with mock.patch.object(Path, "read_bytes", side_effect=AssertionError("no file IO")), \
             mock.patch.object(archive, "read_bound", side_effect=AssertionError("no file IO")):
            self.assertTrue(titles.match_titles({"title": "Interview with my wife"})["matched"])


class OverlayTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.policy_ref = archive.write_inventory(self.root / "policy.json", titles.prepare_policy())
        self.config_ref = {"path": str(self.root / "config.json"), "sha256": "a" * 64}
        self.recording = {"recording_id": "media_sha256_" + "b" * 64,
                          "media": {"path": str(self.root / "media.mp4"), "sha256": "b" * 64, "byte_count": 1},
                          "duration_ms": 60000, "title": "Q&A with my wife", "aliases": []}
        self.base = {"kind": screen.KIND + "_decision", "schema_version": 1,
                     "recording_id": self.recording["recording_id"], "media": deepcopy(self.recording["media"]),
                     "configuration": self.config_ref, "state": "screen_negative", "diarization": False,
                     "reasons": ["complete_samples_without_supported_multispeaker_evidence"],
                     "evidence_summary": {"audio_state": "no_supported_diversity_in_samples"},
                     "method": {"decision_policy": deepcopy(screen.POLICY)}, "semantics": deepcopy(screen.SEMANTICS)}
        self.base_ref = archive.write_inventory(self.root / "base.json", self.base)

    def effective(self):
        return titles.effective_decision(self.base, self.base_ref, self.recording, self.policy_ref)

    def test_negative_title_override_keeps_raw_state_and_evidence(self):
        before = deepcopy(self.base)
        with mock.patch.object(archive, "read_bound", side_effect=AssertionError("builder is pure")):
            result = self.effective()
        self.assertTrue(result["diarization"])
        self.assertEqual(result["state"], "screen_negative")
        for key in ("reasons", "semantics", "evidence_summary"):
            self.assertEqual(result[key], self.base[key])
        self.assertEqual(self.base, before)
        override = result["method"]["title_override"]
        self.assertTrue(override["applied"])
        self.assertEqual(override["base_decision"], self.base_ref)
        self.assertEqual(override["recording_sha256"], hashlib.sha256(archive.canonical(self.recording)).hexdigest())
        self.assertTrue(override["semantics"]["usable_transcript_requires_positive_screen_for_retranscription"])

    def test_low_risk_negative_stays_off(self):
        self.recording["title"] = "Thoughts about my wife"
        result = self.effective()
        self.assertFalse(result["diarization"])
        self.assertFalse(result["method"]["title_override"]["applied"])

    def test_positive_and_uncertain_remain_on_with_any_title(self):
        for state in ("screen_positive", "screen_uncertain"):
            for title in ("Q&A with my wife", "Talking about my wife", None):
                self.base.update(state=state, diarization=True)
                self.recording["title"] = title
                result = self.effective()
                self.assertEqual(result["state"], state)
                self.assertTrue(result["diarization"])
                self.assertFalse(result["method"]["title_override"]["applied"])

    def test_exact_replay_calls_original_acoustic_validator(self):
        result = self.effective()
        with mock.patch.object(screen, "validate_decision", return_value=self.base) as verify:
            self.assertEqual(titles.validate_effective(result, self.recording, self.config_ref, self.policy_ref), result)
        verify.assert_called_once_with(self.base, self.recording, self.config_ref)

    def test_corrupt_acoustic_proof_still_blocks_title_override(self):
        with mock.patch.object(screen, "validate_decision", side_effect=screen.ScreenError("proof changed")):
            with self.assertRaisesRegex(screen.ScreenError, "proof changed"):
                titles.validate_effective(self.effective(), self.recording, self.config_ref, self.policy_ref)

    def test_overlay_and_recording_tampering_rejected(self):
        original = self.effective()
        variants = []
        changed = deepcopy(original)
        changed["state"] = "screen_positive"
        variants.append(changed)
        changed = deepcopy(original)
        changed["diarization"] = False
        variants.append(changed)
        changed = deepcopy(original)
        changed["method"]["title_override"]["matching"]["reasons"] = []
        variants.append(changed)
        changed = deepcopy(original)
        changed["reasons"].append("title_is_positive")
        variants.append(changed)
        with mock.patch.object(screen, "validate_decision", return_value=self.base):
            for changed in variants:
                with self.assertRaisesRegex(titles.TitlePolicyError, "does not replay"):
                    titles.validate_effective(changed, self.recording, self.config_ref, self.policy_ref)
            self.recording["aliases"] = [{"title": "a new alias"}]
            with self.assertRaisesRegex(titles.TitlePolicyError, "does not replay"):
                titles.validate_effective(original, self.recording, self.config_ref, self.policy_ref)

    def test_policy_and_base_hashes_are_external_authority(self):
        other = deepcopy(titles.prepare_policy())
        other["policy"]["title_alone_authorizes_retranscription"] = True
        other_ref = archive.write_inventory(self.root / "other-policy.json", other)
        with self.assertRaisesRegex(titles.TitlePolicyError, "configuration differs"):
            titles.load_policy(other_ref)
        changed_ref = {**self.policy_ref, "sha256": "c" * 64}
        with self.assertRaises(archive.ArchiveInventoryError):
            titles.load_policy(changed_ref)
        result = self.effective()
        result["method"]["title_override"]["base_decision"]["sha256"] = "c" * 64
        with mock.patch.object(screen, "validate_decision", side_effect=AssertionError("hash must be checked first")):
            with self.assertRaises(archive.ArchiveInventoryError):
                titles.validate_effective(result, self.recording, self.config_ref, self.policy_ref)

    def test_overlay_cannot_be_reused_as_a_raw_screen(self):
        with self.assertRaisesRegex(titles.TitlePolicyError, "unmodified base"):
            titles.effective_decision(self.effective(), self.base_ref, self.recording, self.policy_ref)

    def test_invalid_envelope_types_rejected_cleanly(self):
        for decision in (None, {}, {"method": "invalid"}):
            with self.assertRaises(titles.TitlePolicyError):
                titles.validate_effective(decision, self.recording, self.config_ref, self.policy_ref)
        with self.assertRaises(titles.TitlePolicyError):
            titles.effective_decision(self.base, self.base_ref, None, self.policy_ref)


@unittest.skipUnless(Path("/usr/bin/ffmpeg").exists(), "native tool binding unavailable")
class FullProofReplayTests(unittest.TestCase):
    def test_title_overlay_replays_completed_negative_without_model_or_writes(self):
        from pipeline.tests.test_cloud_transcription_screen import ScreenAdapterTests

        helper = ScreenAdapterTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.complete()
        helper.recording["title"] = "Q&A with my wife"
        base = screen.screen_one(helper.recording, helper.folder, helper.config_ref)
        self.assertEqual(base["state"], "screen_negative")
        base_ref = archive.write_inventory(helper.base / "raw-screen.json", base)
        policy_ref = archive.write_inventory(helper.base / "title-policy.json", titles.prepare_policy())
        effective = titles.effective_decision(base, base_ref, helper.recording, policy_ref)
        before = {str(p): p.stat().st_mtime_ns for p in helper.base.rglob("*")}
        with mock.patch.object(screen.mm, "run", side_effect=AssertionError("no model execution")):
            result = titles.validate_effective(effective, helper.recording, helper.config_ref, policy_ref)
        self.assertTrue(result["diarization"])
        self.assertEqual(result["state"], "screen_negative")
        self.assertEqual(before, {str(p): p.stat().st_mtime_ns for p in helper.base.rglob("*")})


if __name__ == "__main__":
    unittest.main()
