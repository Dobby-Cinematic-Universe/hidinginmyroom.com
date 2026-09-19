"""Text leads must remain offline, bounded and distinct from positive screens."""
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import transcript_conversation_leads as leads
from pipeline import transcript_summary as io


def parsed(*texts, speakers=None):
    return {"segments": [{"start_ms": i * 2000, "end_ms": i * 2000 + 1500,
                           "text": text, "speaker": speakers[i] if speakers else None}
                          for i, text in enumerate(texts)]}


class ConversationLeadTests(unittest.TestCase):
    def test_explicit_two_labels_are_strong_text_evidence_not_identity(self):
        result = leads.analyze(parsed("Hello.", "Hi there.", speakers=["SPEAKER_0000", "SPEAKER_0001"]))
        self.assertEqual(result["tier"], "strong_text_lead")
        self.assertIn("explicit_distinct_speaker_labels", result["reasons"])
        self.assertFalse(leads.SEMANTICS["positive_speaker_screen"])
        self.assertFalse(leads.SEMANTICS["speaker_identity_inferred"])

    def test_one_label_and_mentions_are_not_leads(self):
        self.assertIsNone(leads.analyze(parsed("My wife is in Japan.", "She has a job.",
            speakers=["SPEAKER_0000", "SPEAKER_0000"]))["tier"])

    def test_greeting_invitation_with_separate_reply(self):
        result = leads.analyze(parsed("Would you say hello to everyone?", "Hello, my name is Pia."))
        self.assertEqual(result["tier"], "strong_text_lead")
        evidence = result["examples"][0]["evidence"]
        self.assertEqual([row["cue_index"] for row in evidence], [0, 1])
        self.assertEqual(evidence[1]["start_ms"], 2000)

    def test_live_call_check_needs_a_reply(self):
        self.assertIsNone(leads.analyze(parsed("Can you hear me?", "The stream is starting now."))["tier"])
        result = leads.analyze(parsed("Can you hear me?", "Yes, I can hear you fine."))
        self.assertIn("call_check_and_nearby_response", result["reasons"])

    def test_reply_after_large_gap_is_not_paired(self):
        fixture = parsed("Can you hear me?", "I can hear you fine.")
        fixture["segments"][1]["start_ms"] = 900000
        fixture["segments"][1]["end_ms"] = 901000
        self.assertIsNone(leads.analyze(fixture)["tier"])

    def test_recount_chat_and_quoted_dialogue_are_discounted(self):
        for context in ("She said it yesterday.", "Reading the chat now.",
                        "Someone asked about my microphone.", 'He wrote "please call me".',
                        "Imagine that we are strangers.", "Here are the song lyrics."):
            with self.subTest(context=context):
                result = leads.analyze(parsed(context, "Can you hear me?", "I can hear you fine."))
                self.assertIsNone(result["tier"])
                self.assertTrue(result["discounted_candidate_reasons"])

    def test_audience_mic_check_is_discounted(self):
        result = leads.analyze(parsed("Can you guys hear me?", "I can hear you fine."))
        self.assertIsNone(result["tier"])

    def test_reciprocal_greeting_is_only_moderate(self):
        result = leads.analyze(parsed("Nice to meet you.", "You too."))
        self.assertEqual(result["tier"], "moderate_text_lead")

    def test_generic_self_answered_question_is_not_lead(self):
        result = leads.analyze(parsed("Do you want to know why?", "Yes, I am going to explain it."))
        self.assertIsNone(result["tier"])

    def test_observed_archive_false_positives_are_not_recommended(self):
        for texts in (
            ("If I go down there and they say hello to me, then", "I have to say hello back.", "Hello again."),
            ("How do you say hello in Zulu?", "Hello. Sawubona."),
            ("I don't even know how to say hello in any black language.", "Hello. Do you know the way?"),
            ("You never say hello you all.", "Hello you all? What do you mean hello you all?"),
            ("He would say hello, I'd say hello, then we'd say goodnight.", "Hello, goodnight."),
            ("Look at my Tinder conversation with this Japanese woman.", "Hi, nice to meet you.", "Nice to meet you too."),
            ("I'm gonna match her back. And say hello.", "Hello, waving emoji. Guys, help me chat her up."),
            ("Maybe I'll just say hi and we would speak in Japanese.", "Hi to anyone."),
            ("Can you hear me?", "Yes. The stream where he talked about her was great."),
        ):
            with self.subTest(texts=texts):
                self.assertIsNone(leads.analyze(parsed(*texts))["tier"])

    def test_named_invitation_embedded_in_sentence_still_survives(self):
        result = leads.analyze(parsed("I'm with Connor now. Hi Connor, say hi.",
                                      "Hello Daniel, I'm Daniel's best friend."))
        self.assertEqual(result["tier"], "strong_text_lead")

    def test_subtitle_fragment_cannot_turn_recount_into_imperative(self):
        for texts in (("I don't know if I should", "say hello if they don't say", "Hello, for example."),
                      ("But the awkward thing is when they don't", "say hi and I don't say hi.", "Hi, hello.")):
            self.assertIsNone(leads.analyze(parsed(*texts))["tier"])

    def test_viewer_raid_and_performance_are_not_leads(self):
        for texts in (("Daniel, we just raided you.", "Say hi.", "Hello!"),
                      ("My pet hamster is called Jeeves.", "Say hello, Jeeves.", "Hello, everyone."),
                      ("Can you say hello, my name is Daniel?", "I don't want to do the accent.", "My name is Daniel.")):
            self.assertIsNone(leads.analyze(parsed(*texts))["tier"])

    def test_bare_greeting_invitation_remains_weak_without_guest_context(self):
        self.assertEqual(leads.analyze(parsed("Please say hi, Julian.", "Hello."))["tier"], "weak_text_lead")

    def test_girlfriend_invitation_question_survives(self):
        self.assertEqual(leads.analyze(parsed("I'm with my girlfriend.", "Do you want to say hello?",
                                              "Hello everyone."))["tier"], "strong_text_lead")

    def test_reciprocal_question_answer_cluster_remains_weak(self):
        fixture = parsed("Do you like it?", "Yes, very much.", "Are you from here?", "No, I am from London.",
                         "What about you?", "Have you been here before?", "Yes, last year.")
        self.assertEqual(leads.analyze(fixture)["tier"], "weak_text_lead")

    def test_examples_are_bounded(self):
        fixture = parsed(*(["Can you hear me?", "I can hear you fine.", "Okay.", "Thanks.", "Next.", "Wait."] * 50))
        result = leads.analyze(fixture)
        self.assertLessEqual(len(result["examples"]), leads.MAX_EXAMPLES)
        self.assertTrue(all(len(row["context"]) <= leads.MAX_CONTEXT_CHARS for row in result["examples"]))

    def test_plain_text_does_not_invent_timestamps_or_turns(self):
        fixture = {"segments": [{"start_ms": None, "end_ms": None,
                                "text": "Can you hear me? I can hear you fine.", "speaker": None}]}
        self.assertIsNone(leads.analyze(fixture)["tier"])

    def test_scan_cli_preserves_sources_and_maps_exact_recording_without_network(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "third-party"
            source.mkdir()
            transcript = source / "2026-09-13 - A call [abcdefghijk].txt"
            body = b"1\n00:00:00,000 --> 00:00:01,500\nCan you hear me?\n\n2\n00:00:02,000 --> 00:00:03,500\nI can hear you fine.\n"
            transcript.write_bytes(body)
            original = (transcript.stat().st_mtime_ns, transcript.read_bytes())
            inventory = io.put(root / "inventory.json", {
                "kind": "himr_cloud_transcription_archive_inventory", "schema_version": 1,
                "recordings": [{"recording_id": "recording-test", "title": "A call", "duration_ms": 4000,
                                "youtube_id": "abcdefghijk", "aliases": []}]})
            output = root / "output"
            with mock.patch("socket.socket", side_effect=AssertionError("network forbidden")), mock.patch("builtins.print"):
                self.assertEqual(leads.main(["--transcript-root", str(source), "--inventory", inventory["path"],
                    "--inventory-sha256", inventory["sha256"], "--output-root", str(output)]), 0)
            report = io.read(io.binding(output / "report.json"))
            self.assertEqual(report["counts"]["files_read"], 1)
            self.assertEqual(report["leads"][0]["exact_selected_recording_ids"], ["recording-test"])
            self.assertEqual(report["leads"][0]["source"]["sha256"], io.binding(transcript)["sha256"])
            self.assertEqual((transcript.stat().st_mtime_ns, transcript.read_bytes()), original)
            self.assertTrue((output / "TOP-20.md").exists())

    def test_symlinked_transcript_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            original = root / "outside.txt"
            original.write_text("hello")
            (source / "link.txt").symlink_to(original)
            with self.assertRaises(leads.imports.ImportError):
                leads.scan(source)

    def test_malformed_competing_source_remains_an_identity_conflict(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            (source / "2026-09-13 - A call [abcdefghijk].txt").write_text(
                "1\n00:00:00,000 --> 00:00:01,500\nCan you hear me?\n\n"
                "2\n00:00:02,000 --> 00:00:03,500\nI can hear you fine.\n")
            (source / "2026-09-13 - Conflicting [abcdefghijk].txt").write_text("broken --> cue")
            inventory = io.put(root / "inventory.json", {
                "kind": "himr_cloud_transcription_archive_inventory", "schema_version": 1,
                "recordings": [{"recording_id": "recording-test", "title": "A call", "duration_ms": 4000,
                                "youtube_id": "abcdefghijk", "aliases": []}]})
            report = leads.scan(source, inventory_ref=inventory)
            self.assertEqual(report["counts"]["files_read"], 2)
            self.assertEqual(report["counts"]["malformed_files"], 1)
            self.assertEqual(report["recording_match_counts"], {"ambiguous": 1})
            self.assertEqual(report["leads"][0]["exact_selected_recording_ids"], [])


if __name__ == "__main__":
    unittest.main()
