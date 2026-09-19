from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import salad_transcription_contract as contract


def recording(recording_id: str = "recording-one", *, seconds: int = 4000) -> dict:
    return {
        "manifest": {"path": f"/private/{recording_id}/recording-input.json", "sha256": "a" * 64},
        "recording_id": recording_id,
        "media_id": "media-" + recording_id,
        "audio": {
            "path": f"/private/{recording_id}/audio.flac",
            "sha256": hashlib.sha256(recording_id.encode()).hexdigest(),
            "byte_count": 1_000_000,
            "sample_rate_hz": 16_000,
            "total_samples": seconds * 16_000,
            "duration_ms": seconds * 1000,
        },
    }


def plan(rows: list[dict] | None = None, **overrides) -> dict:
    kwargs = {
        "organization": "example-organization",
        "output_root": "/private/cloud-output",
        "ffmpeg": {"path": "/usr/bin/ffmpeg", "sha256": "f" * 64},
        "rate_usd_per_hour": "0.10",
        "max_estimated_cost_usd": "10.00",
        "chunk_seconds": 1800,
    }
    kwargs.update(overrides)
    return contract.build_plan([recording()] if rows is None else rows, **kwargs)


def provider_output(text: str = "Hello world.", *, start: float = 1.0, end: float = 2.0) -> dict:
    return {
        "text": text,
        "duration": 0.5014,
        "processing_time": 20.5,
        "sentence_level_timestamps": [{"text": text, "start": start, "end": end, "timestamp": [start, end]}],
        "word_segments": [{"word": text, "start": start, "end": end, "score": 0.8}],
    }


class ManifestTests(unittest.TestCase):
    def manifest(self) -> dict:
        row = recording(seconds=3)
        return {
            "kind": "himr_longform_recording_input_manifest",
            "schema_version": 1,
            "boundary_candidates": [],
            "recording": {
                "recording_id": row["recording_id"],
                "media_id": row["media_id"],
                "input": {**row["audio"], "artifact_id": "audio-artifact", "channels": 1},
            },
        }

    def write(self, root: Path, value: dict) -> tuple[Path, str]:
        path = root / "recording-input.json"
        body = contract.canonical_bytes(value)
        path.write_bytes(body)
        return path, hashlib.sha256(body).hexdigest()

    def test_existing_manifest_loaded_without_media_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            path, digest = self.write(Path(temporary), self.manifest())
            original_open = contract.os.open
            with mock.patch.object(contract.os, "open", wraps=original_open) as opened:
                row = contract.load_recording_input(path, digest)
            self.assertEqual(opened.call_count, 1)
            self.assertEqual(opened.call_args.args[0], path)
            self.assertEqual(row["audio"]["total_samples"], 48_000)
            self.assertEqual(row["manifest"], {"path": str(path), "sha256": digest})

    def test_hash_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path, _digest = self.write(Path(temporary), self.manifest())
            with self.assertRaisesRegex(contract.CloudContractError, "SHA-256 differs"):
                contract.load_recording_input(path, "0" * 64)

    def test_duration_mismatch_rejected(self):
        value = self.manifest()
        value["recording"]["input"]["duration_ms"] += 2
        with tempfile.TemporaryDirectory() as temporary:
            path, digest = self.write(Path(temporary), value)
            with self.assertRaisesRegex(contract.CloudContractError, "long-form manifest"):
                contract.load_recording_input(path, digest)

    def test_unknown_manifest_fields_rejected(self):
        value = self.manifest()
        value["recording"]["unexpected"] = "value"
        with tempfile.TemporaryDirectory() as temporary:
            path, digest = self.write(Path(temporary), value)
            with self.assertRaises(contract.CloudContractError):
                contract.load_recording_input(path, digest)

    def test_symlink_manifest_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path, digest = self.write(Path(temporary), self.manifest())
            link = Path(temporary) / "link.json"
            link.symlink_to(path)
            with self.assertRaises(contract.CloudContractError):
                contract.load_recording_input(link, digest)

    def test_duplicate_json_keys_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            body = b'{"kind":"one","kind":"two"}'
            path.write_bytes(body)
            with self.assertRaisesRegex(contract.CloudContractError, "duplicate"):
                contract.load_recording_input(path, hashlib.sha256(body).hexdigest())

    def test_fifo_manifest_rejected_without_waiting_for_a_writer(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "fifo"
            os.mkfifo(path)
            with self.assertRaisesRegex(contract.CloudContractError, "bounded regular"):
                contract.load_recording_input(path, "0" * 64)


class PlanningTests(unittest.TestCase):
    def test_plan_is_deterministic_and_nonmutating(self):
        rows = [recording("second"), recording("first")]
        before = copy.deepcopy(rows)
        value = plan(rows)
        self.assertEqual(value, plan(list(reversed(rows))))
        self.assertEqual(rows, before)
        self.assertEqual(value, contract.validate_plan(value))

    def test_core_tiling_and_overlap_exact(self):
        value = plan()
        chunks = contract.plan_chunks(value)
        self.assertEqual(len(chunks), 3)
        self.assertEqual([row["ordinal"] for row in chunks], [0, 1, 2])
        self.assertEqual(chunks[0]["core"], {"start_sample": 0, "end_sample": 28_800_000})
        self.assertEqual(chunks[1]["analysis"], {"start_sample": 28_720_000, "end_sample": 57_680_000})
        self.assertEqual(chunks[-1]["analysis"]["end_sample"], 64_000_000)
        for left, right in zip(chunks, chunks[1:]):
            self.assertEqual(left["core"]["end_sample"], right["core"]["start_sample"])
        self.assertTrue(all(row["wav_max_bytes"] <= 100_000_000 for row in chunks))

    def test_conservative_minimum_billable_rounding(self):
        value = plan([recording(seconds=1)])
        self.assertEqual(value["estimate"]["billable_hours"], "0.01")
        self.assertEqual(value["estimate"]["estimated_cost_usd"], "0.001")
        self.assertFalse(value["estimate"]["hard_actual_price_cap"])
        value = plan([recording(seconds=37)])
        self.assertEqual(value["estimate"]["billable_hours"], "0.02")

    def test_cost_includes_overlap_for_every_job(self):
        value = plan()
        # 1805 seconds => .51 h, 1810 => .51 h, 405 => .12 h.
        self.assertEqual(value["estimate"]["billable_hours"], "1.14")
        self.assertEqual(value["estimate"]["estimated_cost_usd"], "0.114")

    def test_budget_rejected_not_silently_exceeded(self):
        with self.assertRaisesRegex(contract.CloudContractError, "budget"):
            plan(max_estimated_cost_usd="0.01")

    def test_duplicate_recordings_and_audio_rejected(self):
        for rows in ([recording(), recording()], [recording("one"), {**recording("two"), "audio": recording("one")["audio"]}]):
            with self.subTest(rows=len(rows)), self.assertRaisesRegex(contract.CloudContractError, "repeat"):
                plan(rows)

    def test_changed_source_changes_chunk_identity(self):
        first = recording()
        second = recording()
        second["audio"]["sha256"] = "b" * 64
        self.assertNotEqual(contract.plan_chunks(plan([first]))[0]["chunk_id"], contract.plan_chunks(plan([second]))[0]["chunk_id"])

    def test_analysis_above_s4_limit_routes_explicitly_to_temp_sh(self):
        value = plan(chunk_seconds=3500)
        chunks = contract.plan_chunks(value)
        self.assertGreater(chunks[0]["wav_max_bytes"], 100_000_000)
        self.assertEqual(chunks[0]["upload_provider"], "temp_sh")
        self.assertEqual(chunks[1]["upload_provider"], "s4")

    def test_tiny_recording_allows_long_requested_core(self):
        self.assertEqual(len(contract.plan_chunks(plan([recording(seconds=1)], chunk_seconds=9000))), 1)

    def test_duration_requires_exact_sample_projection(self):
        row = recording()
        row["audio"]["duration_ms"] -= 1
        with self.assertRaisesRegex(contract.CloudContractError, "exact sample"):
            plan([row])

    def test_malformed_parameters(self):
        for overrides in (
            {"organization": "../escape"}, {"organization": "org?secret=yes"},
            {"organization": "Uppercase"}, {"organization": "underscored_name"},
            {"organization": "x"}, {"organization": "trailing-"},
            {"output_root": "/private/../escape"}, {"output_root": "/"},
            {"chunk_seconds": True}, {"overlap_seconds": -1},
            {"chunk_seconds": 5, "overlap_seconds": 5},
            {"rate_usd_per_hour": "NaN"}, {"rate_usd_per_hour": "0"},
            {"rate_usd_per_hour": 0.1}, {"max_estimated_cost_usd": "1e20"},
            {"engine": "arbitrary"},
            {"engine": "whisper-large-v3"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(contract.CloudContractError):
                plan(**overrides)

    def test_transcription_lite_is_an_explicit_supported_engine(self):
        value = plan(engine="transcription-lite")
        self.assertEqual(contract.validate_plan(value)["provider"]["engine"], "transcription-lite")

    def test_transcription_options_defaults_preserve_timestamps_without_extra_features(self):
        value = plan()
        self.assertEqual(value["transcription_options"], {
            "language_code": "en", "sentence_level_timestamps": True,
            "word_level_timestamps": True, "diarization": False,
            "sentence_diarization": False, "summarize": 0,
        })

    def test_requested_diarization_and_summary_are_sealed_and_replayed(self):
        value = plan(diarization=True, sentence_diarization=True, summary_words=200)
        self.assertEqual(value["transcription_options"]["summarize"], 200)
        self.assertTrue(value["transcription_options"]["diarization"])
        self.assertTrue(value["transcription_options"]["sentence_diarization"])
        self.assertEqual(contract.validate_plan(value), value)
        self.assertNotEqual(value["plan_id"], plan()["plan_id"])

    def test_diarization_requires_real_booleans_and_summary_bounded_integer(self):
        for options in (
            {"diarization": 1}, {"diarization": "true"},
            {"sentence_diarization": None}, {"sentence_diarization": 0},
            {"summary_words": True}, {"summary_words": -1},
            {"summary_words": 2001}, {"summary_words": "200"},
            {"summary_words": 1.0},
        ):
            with self.subTest(options=options), self.assertRaises(contract.CloudContractError):
                plan(**options)
        self.assertEqual(plan(summary_words=2000)["transcription_options"]["summarize"], 2000)

    def test_lite_diarization_allowed_but_summarization_rejected(self):
        value = plan(engine="transcription-lite", diarization=True, sentence_diarization=True)
        self.assertEqual(contract.validate_plan(value), value)
        with self.assertRaisesRegex(contract.CloudContractError, "requires the transcribe engine"):
            plan(engine="transcription-lite", summary_words=1)

    def test_transcription_option_tampering_rejected(self):
        for key, replacement in (
            ("language_code", "fr"), ("sentence_level_timestamps", False),
            ("word_level_timestamps", False), ("diarization", True),
            ("sentence_diarization", True), ("summarize", 200),
        ):
            value = plan()
            value["transcription_options"][key] = replacement
            with self.subTest(key=key), self.assertRaises(contract.CloudContractError):
                contract.validate_plan(value)

    def test_empty_recordings_rejected(self):
        with self.assertRaises(contract.CloudContractError):
            plan([])

    def test_tampering_all_major_sections_rejected(self):
        original = plan()
        modifications = [
            lambda p: p["recordings"][0]["chunks"][0]["core"].update(end_sample=1),
            lambda p: p["recordings"][0]["chunks"].pop(),
            lambda p: p["estimate"].update(estimated_cost_usd="0"),
            lambda p: p["policy"].update(publication_authority="all"),
            lambda p: p.update(plan_id="saladplan_" + "0" * 32),
            lambda p: p.update(schema_version=True),
            lambda p: p["provider"].update(name="other"),
            lambda p: p["chunking"].update(sample_rate_hz=8000),
        ]
        for modify in modifications:
            value = copy.deepcopy(original)
            modify(value)
            with self.subTest(value=value["plan_id"]), self.assertRaises(contract.CloudContractError):
                contract.validate_plan(value)

    def test_no_file_reads_during_plan_or_replay(self):
        with mock.patch.object(contract.os, "open", side_effect=AssertionError("no I/O")):
            value = plan()
            contract.validate_plan(value)


class NormalizationTests(unittest.TestCase):
    def setUp(self):
        self.plan = plan()
        self.chunks = contract.plan_chunks(self.plan)

    def normalize(self, value=None, chunk=None):
        return contract.normalize_output(self.plan, self.chunks[0] if chunk is None else chunk, "provider-job-1", provider_output() if value is None else value)

    def test_documented_output_and_honest_provenance(self):
        value = self.normalize()
        self.assertEqual(value["segments"][0]["start_ms"], 1000)
        self.assertEqual(value["words"][0]["text"], "Hello world.")
        self.assertEqual(value["kind"], "himr_salad_chunk_transcript")
        self.assertFalse(value["policy"]["verified_quotation"])
        self.assertFalse(value["policy"]["provider_output_verbatim_guaranteed"])
        self.assertEqual(value["raw_output_sha256"], hashlib.sha256(contract.canonical_bytes(provider_output())).hexdigest())

    def test_analysis_offset_and_half_up_projection(self):
        value = self.normalize(provider_output(start=0.0005, end=1.2345), self.chunks[1])
        self.assertEqual(value["segments"][0]["start_ms"], 1_795_001)
        self.assertEqual(value["words"][0]["end_ms"], 1_796_235)

    def test_input_not_mutated(self):
        source = provider_output()
        before = copy.deepcopy(source)
        self.normalize(source)
        self.assertEqual(source, before)

    def test_untimed_text_preserved(self):
        value = self.normalize({"text": "Text without timings."})
        self.assertEqual(value["text"], "Text without timings.")
        self.assertEqual(value["segments"], [])
        self.assertEqual(value["words"], [])

    def test_untimed_units_remain_null(self):
        value = self.normalize({"text": "Text", "word_segments": [{"word": "Text"}]})
        self.assertIsNone(value["words"][0]["start_ms"])
        self.assertIsNone(value["words"][0]["end_ms"])

    def test_missing_one_endpoint_rejected(self):
        for row in ({"word": "one", "start": 1}, {"word": "one", "end": 2}):
            with self.subTest(row=row), self.assertRaisesRegex(contract.CloudContractError, "one endpoint"):
                self.normalize({"text": "one", "word_segments": [row]})

    def test_invalid_timestamps_rejected(self):
        for start, end in [(-1, 1), (2, 1), (0, 1805.001), (True, 1), ("1", 2), (float("nan"), 2), (1, float("inf"))]:
            with self.subTest(start=start, end=end), self.assertRaises(contract.CloudContractError):
                self.normalize(provider_output(start=start, end=end))

    def test_repeated_timestamp_pair_must_agree(self):
        source = provider_output()
        source["sentence_level_timestamps"][0]["timestamp"] = [0, 2]
        with self.assertRaisesRegex(contract.CloudContractError, "disagrees"):
            self.normalize(source)

    def test_timestamp_only_does_not_fabricate_documented_endpoints(self):
        source = {"text": "one", "sentence_level_timestamps": [{"text": "one", "timestamp": [0, 1]}]}
        with self.assertRaisesRegex(contract.CloudContractError, "documented start/end"):
            self.normalize(source)

    def test_chunk_must_match_plan(self):
        chunk = copy.deepcopy(self.chunks[0])
        chunk["analysis"]["end_sample"] -= 1
        with self.assertRaisesRegex(contract.CloudContractError, "chunk differs"):
            self.normalize(chunk=chunk)

    def test_provider_job_identifier_bounded(self):
        with self.assertRaises(contract.CloudContractError):
            contract.normalize_output(self.plan, self.chunks[0], "../../escape", provider_output())

    def test_speaker_labels_preserved_and_scoped_to_chunk(self):
        source = provider_output()
        source["sentence_level_timestamps"][0]["speaker"] = "SPEAKER_00"
        source["word_segments"][0]["speaker"] = "SPEAKER_00"
        value = self.normalize(source)
        for key in ("segments", "words"):
            self.assertEqual(value[key][0]["speaker"], "SPEAKER_00")
            self.assertEqual(value[key][0]["speaker_id"], self.chunks[0]["chunk_id"] + ":SPEAKER_00")
        self.assertFalse(value["speaker_semantics"]["cross_chunk_speaker_linking"])
        self.assertFalse(value["speaker_semantics"]["person_identity_claimed"])

    def test_missing_speaker_labels_are_not_invented(self):
        value = self.normalize()
        for key in ("segments", "words"):
            self.assertIsNone(value[key][0]["speaker"])
            self.assertIsNone(value[key][0]["speaker_id"])

    def test_untimed_word_can_preserve_provider_speaker_without_made_up_timing(self):
        value = self.normalize({"text": "Hello", "word_segments": [{"word": "Hello", "speaker": "Speaker α"}]})
        word = value["words"][0]
        self.assertEqual(word["speaker"], "Speaker α")
        self.assertTrue(word["speaker_id"].endswith(":Speaker α"))
        self.assertIsNone(word["start_ms"])
        self.assertIsNone(word["end_ms"])

    def test_invalid_or_unbounded_speaker_labels_rejected(self):
        for label in (1, False, "", " ", "x" * 129, "speaker\n0", "speaker\x00"):
            source = provider_output()
            source["word_segments"][0]["speaker"] = label
            with self.subTest(label=label), self.assertRaises(contract.CloudContractError):
                self.normalize(source)

    def test_requested_summary_preserved_separately_from_transcript(self):
        self.plan = plan(diarization=True, sentence_diarization=True, summary_words=200)
        self.chunks = contract.plan_chunks(self.plan)
        source = {**provider_output(), "summary": "A short provider-generated summary."}
        value = self.normalize(source)
        self.assertEqual(value["summary"], source["summary"])
        self.assertEqual(value["summary_status"], "returned")
        self.assertEqual(value["text"], source["text"])
        self.assertTrue(value["policy"]["human_review_required"])

    def test_missing_requested_summary_is_explicit_and_never_generated_locally(self):
        self.plan = plan(summary_words=100)
        self.chunks = contract.plan_chunks(self.plan)
        for source in (provider_output(), {**provider_output(), "summary": None}):
            value = self.normalize(source)
            self.assertIsNone(value["summary"])
            self.assertEqual(value["summary_status"], "missing")

    def test_unrequested_summary_absence_and_unsolicited_summary_are_distinguished(self):
        value = self.normalize()
        self.assertIsNone(value["summary"])
        self.assertEqual(value["summary_status"], "not_requested")
        value = self.normalize({**provider_output(), "summary": "Unsolicited provider summary."})
        self.assertEqual(value["summary_status"], "returned")
        self.assertEqual(value["summary"], "Unsolicited provider summary.")

    def test_nontext_summaries_rejected_without_coercion(self):
        for summary in (42, [], {}, True):
            with self.subTest(summary=summary), self.assertRaises(contract.CloudContractError):
                self.normalize({**provider_output(), "summary": summary})


class WholeRecordingPlanningTests(unittest.TestCase):
    def whole_plan(self, row, **options):
        kwargs = {
            "organization": "example-organization", "output_root": "/private/cloud-output",
            "ffmpeg": {"path": "/usr/bin/ffmpeg", "sha256": "f" * 64},
            "rate_usd_per_hour": "0.10", "max_estimated_cost_usd": "10.00",
        }
        kwargs.update(options)
        # Deliberately omit chunk_seconds to exercise the production default.
        return contract.build_plan([row], **kwargs)

    def test_default_two_hour_video_is_one_temp_sh_audio_job(self):
        value = self.whole_plan(recording(seconds=7200))
        chunks = contract.plan_chunks(value)
        self.assertEqual(value["chunking"]["chunk_seconds"], 9000)
        self.assertEqual(value["chunking"]["strategy"], "whole_recording_when_possible")
        self.assertEqual(value["chunking"]["upload_policy"], "s4_then_temp_sh")
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["upload_provider"], "temp_sh")
        self.assertEqual(chunks[0]["analysis"], chunks[0]["core"])
        self.assertEqual(chunks[0]["analysis"], {"start_sample": 0, "end_sample": 7200 * 16_000})
        self.assertEqual(contract.validate_plan(value), value)

    def test_default_forty_minute_video_remains_one_s4_job(self):
        value = self.whole_plan(recording(seconds=2400))
        chunks = contract.plan_chunks(value)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["upload_provider"], "s4")
        self.assertLessEqual(chunks[0]["wav_max_bytes"], 100_000_000)

    def test_exact_two_and_half_hour_recording_is_not_split(self):
        value = self.whole_plan(recording(seconds=9000))
        chunks = contract.plan_chunks(value)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["analysis"], {"start_sample": 0, "end_sample": 144_000_000})
        self.assertEqual(chunks[0]["wav_max_bytes"], 288_001_024)
        self.assertEqual(chunks[0]["upload_provider"], "temp_sh")
        self.assertEqual(value["estimate"]["billable_hours"], "2.5")

    def test_four_point_six_five_hours_split_only_at_provider_limit(self):
        value = self.whole_plan(recording(seconds=16_740))
        chunks = contract.plan_chunks(value)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["core"]["end_sample"], 8990 * 16_000)
        self.assertEqual(chunks[0]["analysis"]["end_sample"], 8995 * 16_000)
        self.assertEqual(chunks[1]["analysis"]["start_sample"], 8985 * 16_000)
        self.assertEqual(chunks[-1]["core"]["end_sample"], 16_740 * 16_000)
        for chunk in chunks:
            self.assertLessEqual(chunk["analysis"]["end_sample"] - chunk["analysis"]["start_sample"], 9000 * 16_000)
        # Two jobs include ten seconds of repeated overlap and conservative
        # hundredth-hour rounding: 2.50 + 2.16 = 4.66 billed-hour estimate.
        self.assertEqual(value["estimate"]["billable_hours"], "4.66")
        self.assertEqual(value["estimate"]["estimated_cost_usd"], "0.466")

    def test_interior_analysis_never_exceeds_provider_duration_limit(self):
        value = self.whole_plan(recording(seconds=27_000))
        chunks = contract.plan_chunks(value)
        self.assertGreater(len(chunks), 2)
        self.assertEqual(chunks[1]["analysis"]["end_sample"] - chunks[1]["analysis"]["start_sample"], 9000 * 16_000)
        self.assertTrue(all(chunk["analysis"]["end_sample"] - chunk["analysis"]["start_sample"] <= 9000 * 16_000 for chunk in chunks))

    def test_noninteger_second_tail_preserves_every_exact_sample(self):
        row = recording(seconds=9000)
        row["audio"]["total_samples"] += 1
        samples = row["audio"]["total_samples"]
        row["audio"]["duration_ms"] = (samples * 1000 + 8000) // 16_000
        value = self.whole_plan(row)
        chunks = contract.plan_chunks(value)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["core"]["start_sample"], 0)
        self.assertEqual(chunks[-1]["core"]["end_sample"], samples)
        self.assertEqual(sum(chunk["core"]["end_sample"] - chunk["core"]["start_sample"] for chunk in chunks), samples)
        for left, right in zip(chunks, chunks[1:]):
            self.assertEqual(left["core"]["end_sample"], right["core"]["start_sample"])
        self.assertEqual(chunks[-1]["upload_provider"], "s4")

    def test_explicit_shorter_custom_chunks_keep_their_core_size(self):
        value = self.whole_plan(recording(seconds=4000), chunk_seconds=1800)
        chunks = contract.plan_chunks(value)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0]["core"]["end_sample"], 1800 * 16_000)
        self.assertEqual(chunks[1]["core"]["end_sample"] - chunks[1]["core"]["start_sample"], 1800 * 16_000)

    def test_s4_routing_uses_conservative_wav_size_boundary(self):
        maximum_samples = (100_000_000 - 1024) // 2
        for samples, provider in ((maximum_samples, "s4"), (maximum_samples + 1, "temp_sh")):
            row = recording(seconds=1)
            row["audio"]["total_samples"] = samples
            row["audio"]["duration_ms"] = (samples * 1000 + 8000) // 16_000
            with self.subTest(samples=samples):
                value = self.whole_plan(row)
                self.assertEqual(contract.plan_chunks(value)[0]["upload_provider"], provider)

    def test_upload_provider_and_provider_limits_are_sealed(self):
        original = self.whole_plan(recording(seconds=7200))
        edits = [
            lambda value: value["recordings"][0]["chunks"][0].update(upload_provider="s4"),
            lambda value: value["chunking"].update(max_job_seconds=10_000),
            lambda value: value["chunking"].update(s4_max_upload_bytes=300_000_000),
            lambda value: value["chunking"].update(upload_policy="arbitrary_host_fallback"),
            lambda value: value["chunking"].update(strategy="arbitrary"),
        ]
        for edit in edits:
            value = copy.deepcopy(original)
            edit(value)
            with self.assertRaises(contract.CloudContractError):
                contract.validate_plan(value)

    def test_overlap_that_leaves_no_provider_legal_core_is_rejected(self):
        with self.assertRaisesRegex(contract.CloudContractError, "leaves no core"):
            self.whole_plan(recording(seconds=9001), overlap_seconds=4500)

    def test_no_overlap_respects_two_and_half_hour_core(self):
        value = self.whole_plan(recording(seconds=16_740), overlap_seconds=0)
        chunks = contract.plan_chunks(value)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["core"]["end_sample"], 9000 * 16_000)
        self.assertTrue(all(chunk["analysis"] == chunk["core"] for chunk in chunks))


class AssemblyTests(unittest.TestCase):
    def setUp(self):
        self.plan = plan([recording(seconds=40)], chunk_seconds=20, overlap_seconds=5)
        self.chunks = contract.plan_chunks(self.plan)

    def normalize(self, index: int, output: dict) -> dict:
        return contract.normalize_output(self.plan, self.chunks[index], f"job-{index}", output)

    def test_overlap_midpoint_core_ownership(self):
        # Analysis chunks are [0,25] and [15,40]; repeated 19..21 second
        # hypothesis belongs to second core because its midpoint is 20 seconds.
        first = self.normalize(0, provider_output("boundary", start=19, end=21))
        second = self.normalize(1, provider_output("boundary", start=4, end=6))
        assembled = contract.assemble_recording(self.plan, "recording-one", [second, first])
        self.assertEqual(assembled["text"], "boundary")
        self.assertEqual(len(assembled["words"]), 1)
        self.assertEqual(assembled["words"][0]["chunk_id"], self.chunks[1]["chunk_id"])
        self.assertEqual(assembled["overlap"]["excluded_context_units"], {"segments": 1, "words": 1})
        self.assertFalse(assembled["coverage"]["exact_sample_coverage_claimed"])

    def test_untimed_chunk_text_preserved_without_alignment(self):
        transcripts = [self.normalize(0, {"text": "first text"}), self.normalize(1, {"text": "second text"})]
        assembled = contract.assemble_recording(self.plan, "recording-one", transcripts)
        self.assertEqual(assembled["text"], "first text second text")
        self.assertEqual(len(assembled["unplaced"]), 2)
        self.assertEqual(assembled["words"], [])
        self.assertTrue(assembled["text_semantics"]["may_include_unplaced_or_overlapping_text"])

    def test_missing_or_duplicate_chunks_rejected(self):
        transcript = self.normalize(0, {"text": "first"})
        for rows in ([], [transcript], [transcript, transcript]):
            with self.subTest(count=len(rows)), self.assertRaises(contract.CloudContractError):
                contract.assemble_recording(self.plan, "recording-one", rows)

    def test_corrupt_chunk_artifact_rejected(self):
        rows = [self.normalize(0, provider_output()), self.normalize(1, provider_output())]
        rows[0]["words"][0]["text"] = "modified"
        with self.assertRaisesRegex(contract.CloudContractError, "identity differs"):
            contract.assemble_recording(self.plan, "recording-one", rows)

    def test_same_provider_job_cannot_cover_distinct_chunks(self):
        rows = [contract.normalize_output(self.plan, chunk, "same-provider-job", provider_output()) for chunk in self.chunks]
        with self.assertRaisesRegex(contract.CloudContractError, "repeats a provider job"):
            contract.assemble_recording(self.plan, "recording-one", rows)

    def test_unrelated_plan_or_recording_rejected(self):
        rows = [self.normalize(0, provider_output()), self.normalize(1, provider_output())]
        with self.assertRaises(contract.CloudContractError):
            contract.assemble_recording(self.plan, "missing", rows)
        changed = copy.deepcopy(self.plan)
        changed["provider"]["organization"] = "different-org"
        with self.assertRaises(contract.CloudContractError):
            contract.assemble_recording(changed, "recording-one", rows)

    def test_same_speaker_label_in_two_jobs_does_not_link_people(self):
        outputs = [provider_output("first", start=1, end=2), provider_output("second", start=6, end=7)]
        for source in outputs:
            source["word_segments"][0]["speaker"] = "SPEAKER_00"
        rows = [self.normalize(index, source) for index, source in enumerate(outputs)]
        result = contract.assemble_recording(self.plan, "recording-one", rows)
        self.assertEqual([word["speaker"] for word in result["words"]], ["SPEAKER_00", "SPEAKER_00"])
        self.assertNotEqual(result["words"][0]["speaker_id"], result["words"][1]["speaker_id"])
        self.assertFalse(result["speaker_semantics"]["person_identity_claimed"])

    def test_summary_collection_is_per_chunk_not_a_whole_recording_summary(self):
        self.plan = plan([recording(seconds=40)], chunk_seconds=20, overlap_seconds=5, summary_words=150)
        self.chunks = contract.plan_chunks(self.plan)
        rows = [self.normalize(index, {**provider_output(), "summary": f"Summary {index}."}) for index in range(2)]
        result = contract.assemble_recording(self.plan, "recording-one", rows)
        self.assertEqual([item["text"] for item in result["chunk_summaries"]], ["Summary 0.", "Summary 1."])
        for index, item in enumerate(result["chunk_summaries"]):
            self.assertEqual(item["status"], "returned")
            self.assertEqual(item["chunk_id"], self.chunks[index]["chunk_id"])
            self.assertEqual(item["provider_job_id"], f"job-{index}")
            self.assertEqual(item["analysis"], self.chunks[index]["analysis"])
            self.assertEqual(item["core"], self.chunks[index]["core"])
        self.assertEqual(result["summary_semantics"], {"scope": "per_chunk_provider_summary", "whole_recording_summary_claimed": False})
        self.assertNotIn("summary", result)

    def test_missing_chunk_summary_status_preserved_in_assembly(self):
        self.plan = plan([recording(seconds=40)], chunk_seconds=20, overlap_seconds=5, summary_words=100)
        self.chunks = contract.plan_chunks(self.plan)
        rows = [self.normalize(0, {**provider_output(), "summary": "First."}), self.normalize(1, provider_output())]
        result = contract.assemble_recording(self.plan, "recording-one", rows)
        self.assertEqual(result["chunk_summaries"][1]["status"], "missing")
        self.assertIsNone(result["chunk_summaries"][1]["text"])

    def test_single_whole_job_returned_summary_covers_the_recording(self):
        self.plan = plan([recording(seconds=40)], chunk_seconds=9000, summary_words=150)
        self.chunks = contract.plan_chunks(self.plan)
        row = self.normalize(0, {**provider_output(), "summary": "Whole recording provider summary."})
        result = contract.assemble_recording(self.plan, "recording-one", [row])
        self.assertEqual(len(result["chunk_summaries"]), 1)
        self.assertEqual(result["chunk_summaries"][0]["text"], "Whole recording provider summary.")
        self.assertEqual(result["summary_semantics"], {
            "scope": "whole_recording_provider_summary", "whole_recording_summary_claimed": True,
        })
        self.assertTrue(result["policy"]["human_review_required"])
        self.assertFalse(result["policy"]["verified_quotation"])

    def test_missing_single_job_summary_does_not_claim_whole_recording_summary(self):
        self.plan = plan([recording(seconds=40)], chunk_seconds=9000, summary_words=150)
        self.chunks = contract.plan_chunks(self.plan)
        result = contract.assemble_recording(self.plan, "recording-one", [self.normalize(0, provider_output())])
        self.assertEqual(result["chunk_summaries"][0]["status"], "missing")
        self.assertFalse(result["summary_semantics"]["whole_recording_summary_claimed"])
        self.assertEqual(result["summary_semantics"]["scope"], "per_chunk_provider_summary")

    def test_speaker_namespace_tampering_rejected_even_with_recomputed_identity(self):
        source = provider_output()
        source["word_segments"][0]["speaker"] = "SPEAKER_00"
        rows = [self.normalize(0, source), self.normalize(1, provider_output())]
        rows[0]["words"][0]["speaker_id"] = self.chunks[1]["chunk_id"] + ":SPEAKER_00"
        core = {key: value for key, value in rows[0].items() if key not in {"identity_sha256", "transcript_id"}}
        rows[0] = contract._seal(core, "transcript_id", "saladchunktranscript_")
        with self.assertRaisesRegex(contract.CloudContractError, "source chunk"):
            contract.assemble_recording(self.plan, "recording-one", rows)

    def test_fabricated_summary_status_rejected_even_with_recomputed_identity(self):
        rows = [self.normalize(0, provider_output()), self.normalize(1, provider_output())]
        rows[0]["summary_status"] = "returned"
        core = {key: value for key, value in rows[0].items() if key not in {"identity_sha256", "transcript_id"}}
        rows[0] = contract._seal(core, "transcript_id", "saladchunktranscript_")
        with self.assertRaisesRegex(contract.CloudContractError, "summary status differs"):
            contract.assemble_recording(self.plan, "recording-one", rows)


if __name__ == "__main__":
    unittest.main()
