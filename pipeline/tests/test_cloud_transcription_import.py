"""Offline fixtures: no archive media, network, credentials, or source edits."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_import as imp
from pipeline import transcript_summary_sources as summary


SRT = b"1\n00:00:01,123 --> 00:00:02,789\nOriginal text.\n\n2\n00:00:03,000 --> 00:00:04,000\nAnother line.\n"
VIDEO = "xKuOtWjOCaA"


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sources = self.root / "sources"
        self.sources.mkdir()

    def write(self, name=None, body=SRT):
        path = self.sources / (name or f"2015-10-20 - A title [{VIDEO}].txt")
        path.write_bytes(body)
        return path

    def entry(self, **kwargs):
        self.write(**kwargs)
        return imp.inventory(self.sources)[0]

    def recording(self, **changes):
        return {"recording_id": "media_sha256_" + "a" * 64,
                "duration_ms": 4500, "source_ids": {"youtube": [VIDEO], "archive_native": []},
                "aliases": [], **changes}

    def test_inventory_read_only_stable_hash_and_no_network(self):
        path = self.write()
        path.chmod(0o777)
        before = path.stat()
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            entries = imp.inventory(self.sources)
        entry = entries[0]
        self.assertEqual(entry["sha256"], hashlib.sha256(SRT).hexdigest())
        self.assertEqual(entry["byte_count"], len(SRT))
        self.assertEqual(entry["status"], "eligible")
        self.assertEqual(entry["segment_count"], 2)
        self.assertEqual(entry["last_end_ms"], 4000)
        self.assertEqual(path.stat().st_mtime_ns, before.st_mtime_ns)
        self.assertEqual(path.stat().st_mode, before.st_mode)
        self.assertEqual(path.read_bytes(), SRT)

    def test_srt_crlf_bom_timestamps_and_exact_text(self):
        parsed = imp.parse_transcript(b"\xef\xbb\xbf" + SRT.replace(b"\n", b"\r\n"))
        self.assertEqual(parsed["issues"], [])
        self.assertEqual(parsed["segments"][0], {"start_ms": 1123, "end_ms": 2789,
                                                "text": "Original text.", "speaker": None})

    def test_multiline_cues_are_preserved(self):
        parsed = imp.parse_transcript(SRT.replace(b"Original text.", b"Original\ntext."))
        self.assertEqual(parsed["segments"][0]["text"], "Original\ntext.")

    def test_speaker_labels_preserved_anonymously_in_sidecar(self):
        entry = self.entry(body=SRT.replace(b"Original text.", b"[Speaker 7] Original text.")
                           .replace(b"Another line.", b"Speaker 2: Another line."))
        bundle = imp.normalize(entry, "recording-1", duration_ms=4500)
        self.assertEqual(bundle["transcript"]["segments"][0]["speaker"], "SPEAKER_0000")
        self.assertEqual(bundle["provenance"]["speaker_label_mapping"],
                         {"Speaker 7": "SPEAKER_0000", "Speaker 2": "SPEAKER_0001"})
        self.assertFalse(bundle["provenance"]["speaker_identity_inferred"])

    def test_does_not_infer_person_from_name_or_first_person(self):
        entry = self.entry(body=SRT.replace(b"Original text.", b"Daniel: I think so."))
        segment = imp.normalize(entry, "recording-1")["transcript"]["segments"][0]
        self.assertIsNone(segment["speaker"])
        self.assertEqual(segment["text"], "Daniel: I think so.")

    def test_empty_and_plain_text_are_review_not_completed(self):
        for name, body, expected in (("empty.txt", b" \n", "empty"),
                                     ("plain.txt", b"Plain words.", "plain_text")):
            self.write(name, body)
        entries = imp.inventory(self.sources)
        self.assertEqual({entry["format"] for entry in entries}, {"empty", "plain_text"})
        for entry in entries:
            self.assertEqual(entry["status"], "review_required")
            with self.assertRaises(imp.ImportError):
                imp.normalize(entry, "recording-1")

    def test_malformed_or_truncated_srt_never_silently_drops_text(self):
        bodies = [SRT + b"\n3\n", SRT + b"\n3\n00:00:05,000 --> 00:00:06,000\n",
                  SRT.replace(b"00:00:03,000", b"00:61:03,000"),
                  SRT.replace(b"00:00:04,000", b"00:00:02,000"),
                  SRT.replace(b"\n\n2", b"\n2"), b"1\ninvalid --> timing\nText.\n"]
        for body in bodies:
            with self.subTest(body=body), self.assertRaises(imp.ImportError):
                imp.parse_transcript(body)

    def test_invalid_encoding_control_bytes_and_size(self):
        for body in (b"\xff", b"Hello\x00", b"Hello\x01"):
            with self.subTest(body=body), self.assertRaises(imp.ImportError):
                imp.parse_transcript(body)
        with patch.object(imp, "MAX_FILE_BYTES", 3), self.assertRaises(imp.ImportError):
            imp.parse_transcript(SRT)
        with patch.object(imp, "MAX_TEXT_CHARACTERS", 3), self.assertRaises(imp.ImportError):
            imp.parse_transcript(b"Four")

    def test_absurd_cue_number_is_bounded_malformed_input(self):
        with self.assertRaises(imp.ImportError):
            imp.parse_transcript(b"1" * 5000 + SRT[1:])

    def test_number_gaps_and_time_reordering_require_review(self):
        parsed = imp.parse_transcript(SRT.replace(b"\n2\n", b"\n4\n")
                                    .replace(b"00:00:03,000", b"00:00:00,000"))
        self.assertEqual(parsed["issues"], ["nonconsecutive_cue_numbers", "nonmonotonic_cue_start"])

    def test_legitimate_overlapping_cues_are_not_destroyed(self):
        parsed = imp.parse_transcript(SRT.replace(b"00:00:03,000", b"00:00:02,000"))
        self.assertEqual(parsed["issues"], [])
        self.assertEqual(len(parsed["segments"]), 2)

    def test_exact_youtube_match_not_title_date_or_case(self):
        entry = self.entry()
        selected = imp.match(self.recording(title="Different display title"), [entry])
        self.assertEqual(selected["status"], "selected")
        self.assertEqual(selected["matched_keys"], ["youtube:" + VIDEO])
        for recording in ({"title": "A title", "date": "2015-10-20"},
                          {"youtube_id": VIDEO.lower()}):
            self.assertEqual(imp.match(recording, [entry])["status"], "missing")

    def test_literal_bracketed_id_basename_resolves_specific_archive_copy(self):
        entry = self.entry()
        records = [self.recording(source_ids={"youtube": [VIDEO], "archive_native":
                      [f"hidinginmyroom/A title [{VIDEO}].mp4"]}),
                   self.recording(recording_id="alternate", source_ids={"youtube": [VIDEO], "archive_native":
                      [f"69999/20151020-A title-{VIDEO}.mp4"]})]
        first, second = imp.match_recordings(records, [entry])
        self.assertEqual(first["status"], "selected")
        self.assertEqual(first["matching_basis"], ["literal_export_basename"])
        self.assertEqual(len(first["related_physical_recordings"]), 2)
        self.assertEqual(second["status"], "ambiguous")

    def test_literal_basename_does_not_normalize_case_or_punctuation(self):
        entry = self.entry()
        result = imp.match({"source_native_id": f"item/A Title! [{VIDEO}].mp4"}, [entry])
        self.assertEqual(result["matching_basis"], ["exact_youtube_id"])

    def test_literal_basename_resolution_keeps_duration_hold(self):
        entry = self.entry()
        first = self.recording(duration_ms=600000, source_ids={"youtube": [VIDEO], "archive_native":
                      [f"hidinginmyroom/A title [{VIDEO}].mp4"]})
        result = imp.match_recordings([first, self.recording(recording_id="alternate")], [entry])[0]
        self.assertEqual(result["status"], "review_required")
        self.assertIn("possible_missing_tail", result["issues"])

    def test_archive_filename_suffix_youtube_match(self):
        entry = self.entry()
        record = {"aliases": [{"source_native_id": f"69999/Different title-{VIDEO}.mp4"}]}
        self.assertEqual(imp.match(record, [entry])["status"], "selected")
        record["aliases"][0]["source_native_id"] = f"69999/20151020-Different title-{VIDEO}.ia.mp4"
        self.assertEqual(imp.match(record, [entry])["status"], "selected")

    def test_archive_item_and_literal_original_stem_without_hash_guess(self):
        entry = self.entry(name="undated - 99999999-Original recording [IA-69999-deadbeef1234].txt")
        right = {"source_native_id": "69999/99999999-Original recording.mp4"}
        wrong_item = {"source_native_id": "699994/99999999-Original recording.mp4"}
        self.assertEqual(imp.match(right, [entry])["status"], "selected")
        self.assertEqual(imp.match(wrong_item, [entry])["status"], "missing")
        self.assertFalse(any("deadbeef" in key for key in entry["identity_keys"]))

    def test_opaque20_export_matches_literal_filename_not_display_title(self):
        entry = self.entry(name="undated - 99999999-Original recording [7581acbd2c8b78a3e8b1].txt")
        right = {"source_native_id": "69999/99999999-Original recording.mp4"}
        self.assertEqual(imp.match(right, [entry])["status"], "selected")
        self.assertEqual(imp.match({"title": "99999999-Original recording"}, [entry])["status"], "missing")

    def test_youtube_and_archive_url_parsing(self):
        expected = "youtube:" + VIDEO
        for url in (f"https://www.youtube.com/watch?v={VIDEO}&t=12", f"https://youtu.be/{VIDEO}"):
            self.assertIn(expected, imp.recording_keys({"canonical_url": url}))
        self.assertNotIn(expected, imp.recording_keys({"canonical_url": f"https://youtube.com.evil.test/?v={VIDEO}"}))
        self.assertIn("archive_stem:69999/a b", imp.recording_keys(
            {"canonical_url": "https://archive.org/download/69999/a%20b.mp4"}))

    def test_generic_title_without_identity_is_missing(self):
        entry = self.entry(name="2026-09-11 life updates.txt")
        self.assertEqual(entry["identity_keys"], [])
        self.assertEqual(imp.match({"title": "life updates", "date": "2026-09-11"}, [entry])["status"], "missing")

    def test_distinct_transcript_copies_are_ambiguous_not_arbitrarily_ranked(self):
        self.write()
        self.write(f"2015-10-20 - Other [{VIDEO}].txt", SRT.replace(b"Original", b"Contradictory"))
        result = imp.match(self.recording(), imp.inventory(self.sources))
        self.assertEqual(result["status"], "ambiguous")
        self.assertIsNone(result["selected"])

    def test_equivalent_crlf_copy_collapses_deterministically(self):
        self.write()
        self.write(f"2015-10-20 - Z [{VIDEO}].txt", SRT.replace(b"\n", b"\r\n"))
        entries = imp.inventory(self.sources)
        result = imp.match(self.recording(), list(reversed(entries)))
        self.assertEqual(result["status"], "selected")
        self.assertEqual(result["selected"]["path"], min(entry["path"] for entry in entries))
        self.assertEqual(len(result["candidates"]), 2)

    def test_invalid_matching_copy_prevents_clean_copy_admission(self):
        self.write()
        self.write(f"2015-10-20 - Broken [{VIDEO}].txt", SRT + b"\n3\n")
        self.assertEqual(imp.match(self.recording(), imp.inventory(self.sources))["status"], "ambiguous")

    def test_possible_short_tail_is_review_not_a_claim_of_truncation(self):
        entry = self.entry()
        result = imp.match(self.recording(duration_ms=600000), [entry])
        self.assertEqual(result["status"], "review_required")
        self.assertEqual(result["issues"], ["possible_missing_tail"])
        self.assertFalse(result["full_media_coverage_verified"])
        with self.assertRaises(imp.ImportError):
            imp.normalize(entry, "recording-1", duration_ms=600000)

    def test_quiet_tail_within_allowance_is_not_rejected(self):
        self.assertEqual(imp.match(self.recording(duration_ms=30000), [self.entry()])["status"], "selected")

    def test_timestamps_past_duration_require_review(self):
        body = SRT.replace(b"00:00:04,000", b"00:05:04,000")
        result = imp.match(self.recording(), [self.entry(body=body)])
        self.assertEqual(result["status"], "review_required")
        self.assertIn("timestamps_exceed_media_duration", result["issues"])

    def test_identity_key_cannot_auto_match_two_physical_recordings(self):
        entry = self.entry()
        records = [self.recording(), self.recording(recording_id="media_sha256_" + "b" * 64)]
        for result in imp.match_recordings(records, [entry]):
            self.assertEqual(result["status"], "ambiguous")
            self.assertIsNone(result["selected"])
            self.assertIn("identity_key_matches_multiple_recordings", result["issues"])

    def test_exact_archive_source_outweighs_shared_youtube_identity(self):
        self.write(name=f"2015-10-20 - Original-{VIDEO} [IA-69999-deadbeef1234].txt")
        self.write(name=f"2015-10-20 - A title [{VIDEO}].txt",
                   body=SRT.replace(b"Original text.", b"Different edition's words."))
        first = self.recording(source_ids={"youtube": [VIDEO],
            "archive_native": [f"69999/Original-{VIDEO}.mp4"]})
        second = self.recording(recording_id="other-edit")
        one, two = imp.match_recordings([first, second], imp.inventory(self.sources))
        self.assertEqual(one["status"], "selected")
        self.assertEqual(one["matched_keys"], [f"archive_stem:69999/Original-{VIDEO}"])
        self.assertEqual(len(one["lower_specificity_candidates"]), 1)
        self.assertEqual(two["status"], "ambiguous")

    def test_unique_literal_filename_outweighs_shared_video_id(self):
        entry = self.entry(name=f"undated - Original-{VIDEO} [7581acbd2c8b78a3e8b1].txt")
        first = self.recording(source_ids={"youtube": [VIDEO],
            "archive_native": [f"69999/Original-{VIDEO}.mp4"]})
        one, two = imp.match_recordings([first, self.recording(recording_id="other")], [entry])
        self.assertEqual(one["status"], "selected")
        self.assertEqual(one["matched_keys"], [f"media_stem:Original-{VIDEO}"])
        self.assertEqual(two["status"], "ambiguous")

    def test_even_strong_archive_stem_collision_remains_held(self):
        entry = self.entry(name="undated - Original [IA-69999-deadbeef1234].txt")
        records = [{"source_native_id": "69999/Original.mp4"},
                   {"source_native_id": "69999/Original.webm"}]
        for result in imp.match_recordings(records, [entry]):
            self.assertEqual(result["status"], "ambiguous")

    def test_explicit_export_date_restores_exact_compact_archive_prefix(self):
        entry = self.entry(name=f"2015-10-20 - Original-{VIDEO} [IA-69999-deadbeef1234].txt")
        first = {"source_native_id": f"69999/20151020-Original-{VIDEO}.ia.mp4"}
        one, two = imp.match_recordings([first, self.recording(recording_id="other")], [entry])
        self.assertEqual(one["status"], "selected")
        self.assertEqual(one["matched_keys"], [f"archive_stem:69999/20151020-Original-{VIDEO}.ia"])
        self.assertEqual(two["status"], "ambiguous")

    def test_export_date_cannot_match_wrong_calendar_source(self):
        entry = self.entry(name="2015-10-20 - Original [IA-69999-deadbeef1234].txt")
        result = imp.match({"source_native_id": "69999/20151021-Original.mp4"}, [entry])
        self.assertEqual(result["status"], "missing")

    def test_original_and_ia_derivative_cannot_claim_same_export_twice(self):
        entry = self.entry(name=f"2015-10-20 - Original-{VIDEO} [IA-69999-deadbeef1234].txt")
        results = imp.match_recordings([
            {"source_native_id": f"69999/20151020-Original-{VIDEO}.mp4"},
            {"source_native_id": f"69999/20151020-Original-{VIDEO}.ia.mp4"}], [entry])
        self.assertTrue(all(row["status"] == "ambiguous" for row in results))
        self.assertTrue(all("transcript_source_matches_multiple_recordings" in row["issues"] for row in results))

    def test_malformed_strong_candidate_does_not_fall_back_to_broad_identity(self):
        self.write(name=f"2015-10-20 - Original-{VIDEO} [IA-69999-deadbeef1234].txt", body=SRT + b"\n3\n")
        self.write()
        result = imp.match({"source_native_id": f"69999/Original-{VIDEO}.mp4"}, imp.inventory(self.sources))
        self.assertEqual(result["status"], "review_required")
        self.assertIsNone(result["selected"])

    def test_index_reused_without_reparsing(self):
        entries = [self.entry()]
        index = imp.index_entries(entries)
        with patch.object(imp, "parse_transcript", side_effect=AssertionError("no reparse")), \
                patch.object(imp, "index_entries", side_effect=AssertionError("index reused")):
            self.assertEqual(imp.match(self.recording(), index)["status"], "selected")

    def test_changed_original_rejected_even_same_byte_count(self):
        entry = self.entry()
        path = Path(entry["path"])
        path.write_bytes(SRT.replace(b"Original", b"Modified"))
        with self.assertRaises(imp.ImportError):
            imp.read_raw(entry)
        with self.assertRaises(imp.ImportError):
            imp.normalize(entry, "recording-1")

    def test_tampered_inventory_duration_cannot_bypass_quality_gate(self):
        entry = self.entry()
        entry["last_end_ms"] = 600000
        with self.assertRaises(imp.ImportError):
            imp.normalize(entry, "recording-1", duration_ms=600000)

    def test_parent_traversal_rejected(self):
        self.write()
        with self.assertRaises(imp.ImportError):
            imp.inventory(self.sources / ".." / "sources")

    def test_symlink_file_and_ancestor_rejected(self):
        path = self.write()
        other = self.sources / "linked.txt"
        other.symlink_to(path)
        with self.assertRaises(imp.ImportError):
            imp.inventory(self.sources)
        other.unlink()
        link = self.root / "linked-directory"
        link.symlink_to(self.sources, target_is_directory=True)
        with self.assertRaises(imp.ImportError):
            imp.inventory(link)

    def test_symlinked_subdirectory_rejected(self):
        self.write()
        (self.sources / "nested").symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(imp.ImportError):
            imp.inventory(self.sources)

    def test_bounded_files_and_total_bytes(self):
        self.write()
        with patch.object(imp, "MAX_FILES", 0), self.assertRaises(imp.ImportError):
            imp.inventory(self.sources)
        with patch.object(imp, "MAX_TOTAL_BYTES", 2), self.assertRaises(imp.ImportError):
            imp.inventory(self.sources)

    def test_normalized_document_accepted_by_existing_summary_adapter(self):
        bundle = imp.normalize(self.entry(), "recording-1", duration_ms=4500)
        body = summary.canonical_bytes(bundle["transcript"])
        path = self.root / "import.json"
        path.write_bytes(body)
        path.chmod(0o600)
        source = summary.normalize_source({"transcript": {"path": str(path), "sha256": hashlib.sha256(body).hexdigest()},
            "format": "third_party", "recording_id": "recording-1", "title": None, "date": None, "completion": None})
        self.assertEqual(source["segments"][0]["text"], "Original text.")
        self.assertEqual(source["segments"][0]["start_ms"], 1123)
        self.assertEqual(source["segments"][0]["timing_basis"], "third_party_supplied_ms")
        self.assertTrue(bundle["provenance"]["machine_generated"])
        self.assertEqual(bundle["provenance"]["model_provenance"], "user_attested_author_confirmation")
        self.assertFalse(bundle["provenance"]["model_independently_verified"])
        self.assertFalse(bundle["provenance"]["rights_granted"])
        self.assertEqual(bundle["provenance"]["attribution"], "u/MelatoninHighs")

    def test_normalization_preserves_raw_bytes_and_does_not_call_summary_model(self):
        entry = self.entry()
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            imp.normalize(entry, "recording-1")
            self.assertEqual(imp.read_raw(entry), SRT)
        self.assertEqual(len(list(self.sources.iterdir())), 1)


if __name__ == "__main__":
    unittest.main()
