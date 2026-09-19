"""Pure diarization contracts: no models, real recordings, GPU or filesystem work."""
import copy
import json
import random
import unittest

from pipeline import screened_diarization_core as core


MEDIA = "a" * 64
RUN = "diarjob_" + "b" * 32


def turn(start, end, speaker="native-label"):
    return {"start": start, "end": end, "speaker": speaker}


def reviewed(parameters=None):
    return {"parameters": parameters or {"min_speakers": 1, "max_speakers": 2},
            "review": {"basis": "direct_media_human_review", "media_sha256": MEDIA,
                       "reviewer": "reviewer-1", "reviewed_at": "2026-09-12T12:00:00Z",
                       "evidence": {"path": "/private/review.json", "sha256": "c" * 64}}}


class ReviewedBoundsTests(unittest.TestCase):
    def validate(self, value):
        return core.validate_speaker_bounds(value, media_sha256=MEDIA)

    def test_default_automatic_has_no_speaker_kwargs_or_review(self):
        self.assertEqual(self.validate(None), {"parameters": {}, "review": None})
        self.assertEqual(self.validate({"parameters": {}, "review": None}), self.validate(None))

    def test_explicit_min_max_and_exact_count_require_and_preserve_review(self):
        for parameters in ({"min_speakers": 1, "max_speakers": 2}, {"num_speakers": 2},
                           {"max_speakers": 2}, {"min_speakers": 1}):
            with self.subTest(parameters=parameters):
                value = self.validate(reviewed(parameters))
                self.assertEqual(value["parameters"], parameters)
                self.assertEqual(value["review"]["media_sha256"], MEDIA)
                self.assertEqual(value["review"]["reviewer"], "reviewer-1")
                self.assertEqual(value["review"]["evidence"]["sha256"], "c" * 64)

    def test_timezone_is_required_and_normalized_to_utc(self):
        value = reviewed()
        value["review"]["reviewed_at"] = "2026-09-12T08:00:00-04:00"
        self.assertEqual(self.validate(value)["review"]["reviewed_at"], "2026-09-12T12:00:00Z")
        for timestamp in ("2026-09-12T12:00:00", "2026-02-30T12:00:00Z", "2026-09-12", "today"):
            value["review"]["reviewed_at"] = timestamp
            with self.subTest(timestamp=timestamp), self.assertRaises(core.DiarizationError):
                self.validate(value)

    def test_guesses_titles_and_wrong_source_cannot_authorize_bounds(self):
        for change in (lambda v: v.update(review=None), lambda v: v["review"].update(basis="title_guess"),
                       lambda v: v["review"].update(basis="speaker_screen"),
                       lambda v: v["review"].update(media_sha256="d" * 64),
                       lambda v: v["review"].update(reviewer="")):
            value = reviewed()
            change(value)
            with self.subTest(value=value), self.assertRaises(core.DiarizationError):
                self.validate(value)

    def test_count_parameters_are_strict_bounded_and_mutually_exclusive(self):
        invalid = ({"num_speakers": 2, "max_speakers": 2}, {"min_speakers": 3, "max_speakers": 2},
                   {"max_speakers": 257}, {"num_speakers": True}, {"min_speakers": 0},
                   {"num_speakers": 2.0}, {"speaker_count": 2})
        for parameters in invalid:
            with self.subTest(parameters=parameters), self.assertRaises(core.DiarizationError):
                self.validate(reviewed(parameters))

    def test_evidence_binding_paths_hashes_and_unknown_fields_rejected(self):
        for path in ("relative.json", "/", "/a/../b", "/a//b", "//a/b", "/a\\b", "/a\nb"):
            value = reviewed()
            value["review"]["evidence"]["path"] = path
            with self.subTest(path=path), self.assertRaises(core.DiarizationError):
                self.validate(value)
        for change in (lambda v: v["review"]["evidence"].update(sha256="bad"),
                       lambda v: v.update(confidence=1), lambda v: v["review"].update(inferred=True)):
            value = reviewed()
            change(value)
            with self.assertRaises(core.DiarizationError):
                self.validate(value)

    def test_input_review_is_not_mutated_or_returned_by_reference(self):
        value = reviewed()
        before = copy.deepcopy(value)
        result = self.validate(value)
        result["parameters"]["max_speakers"] = 9
        result["review"]["evidence"]["path"] = "/different"
        self.assertEqual(value, before)


class DiarizationNormalizationTests(unittest.TestCase):
    def normalize(self, ordinary=None, exclusive=None, *, duration=10_000, bounds=None):
        ordinary = [] if ordinary is None else ordinary
        exclusive = ordinary if exclusive is None else exclusive
        return core.normalize_output(duration, ordinary, exclusive, media_sha256=MEDIA, run_id=RUN, bounds=bounds)

    def example(self):
        return self.normalize([turn(0, 3, "private-name-a"), turn(2, 5, "private-name-b")],
                              [turn(0, 2.5, "private-name-a"), turn(2.5, 5, "private-name-b")])

    def test_labels_are_jointly_anonymized_and_recording_run_scoped(self):
        result = self.example()
        self.assertEqual(result["speakers"], ["SPEAKER_0000", "SPEAKER_0001"])
        self.assertEqual(result["run_id"], RUN)
        self.assertEqual(result["recording"], {"media_sha256": MEDIA, "duration_ms": 10_000})
        self.assertNotIn("private-name", json.dumps(result))
        for name in ("ordinary", "exclusive"):
            self.assertEqual([row["speaker"] for row in result["representations"][name]], result["speakers"])
        self.assertTrue(result["semantics"]["recording_local_labels"])
        self.assertFalse(result["semantics"]["identity_inferred"])
        self.assertFalse(result["semantics"]["groups_are_person_counts"])
        self.assertFalse(result["semantics"]["publication_authority"])

    def test_ordinary_overlap_and_exclusive_alignment_remain_separate(self):
        result = self.example()
        self.assertTrue(all(row["overlap"] for row in result["representations"]["ordinary"]))
        self.assertFalse(any(row["overlap"] for row in result["representations"]["exclusive"]))
        self.assertEqual(result["overlap_intervals"], [{"start_ms": 2_000, "end_ms": 3_000}])
        self.assertEqual(result["summary"]["speech_ms"], 5_000)
        self.assertEqual(result["summary"]["overlap_ms"], 1_000)
        self.assertEqual(result["summary"]["speaker_count"], 2)

    def test_no_confidence_is_fabricated(self):
        result = self.example()
        for rows in result["representations"].values():
            self.assertTrue(all(row["score_state"] == "unavailable" for row in rows))
            self.assertFalse(any("confidence" in row or "score" in row for row in rows))
        self.assertEqual(result["summary"]["score_state"], "unavailable")

    def test_empty_output_preserves_no_speech_even_with_reviewed_minimum(self):
        result = self.normalize(bounds=reviewed())
        self.assertEqual(result["summary"]["status"], "no_speech_turns")
        self.assertEqual(result["summary"]["speaker_count"], 0)
        self.assertEqual(result["representations"], {"ordinary": [], "exclusive": []})
        self.assertEqual(core.validate_result(result), result)

    def test_nonempty_output_must_respect_explicit_reviewed_count_bounds(self):
        rows = [turn(0, 1, "a"), turn(1, 2, "b")]
        self.assertEqual(self.normalize(rows, bounds=reviewed())["summary"]["speaker_count"], 2)
        for bounds in (reviewed({"num_speakers": 1}), reviewed({"max_speakers": 1}), reviewed({"min_speakers": 3})):
            with self.subTest(bounds=bounds), self.assertRaisesRegex(core.DiarizationError, "violates"):
                self.normalize(rows, bounds=bounds)

    def test_outward_integer_millisecond_rounding(self):
        result = self.normalize([turn(0.00025, 0.00175)], duration=10)
        row = result["representations"]["ordinary"][0]
        self.assertEqual((row["start_ms"], row["end_ms"]), (0, 2))
        self.assertEqual(core.validate_result(result), result)

    def test_exclusive_shared_boundary_collision_is_explicitly_reconciled(self):
        rows = [turn(0, 1.0005, "a"), turn(1.0005, 2, "b")]
        result = self.normalize(rows)
        left, right = result["representations"]["exclusive"]
        self.assertEqual(left["end_ms"], right["start_ms"])
        self.assertEqual(left["end_ms"], 1_001)
        self.assertFalse(any(row["overlap"] for row in result["representations"]["ordinary"]))
        self.assertEqual(result["summary"]["overlap_ms"], 0)
        self.assertTrue(any(row["kind"] == "shared_boundary_quantization" for row in result["timing_provenance"]["rounding_adjustments"]))
        self.assertEqual(core.validate_result(result), result)

    def test_binary_float_roundoff_snaps_only_with_explicit_nanosecond_tolerance(self):
        rows = [turn(0, 1.0000000000000002, "a"), turn(1.0, 2, "b")]
        result = self.normalize(rows)
        self.assertEqual(result["representations"]["exclusive"][0]["end_ms"], 1000)
        self.assertEqual(result["summary"]["overlap_ms"], 0)
        self.assertTrue(any(row["kind"] == "floating_roundoff_to_integer_ms" for row in result["timing_provenance"]["rounding_adjustments"]))

    def test_only_small_recording_edge_overshoot_is_reconciled_and_reported(self):
        result = self.normalize([turn(-0.0005, 1.0005)], duration=1000)
        row = result["representations"]["ordinary"][0]
        self.assertEqual((row["start_ms"], row["end_ms"]), (0, 1000))
        self.assertEqual(result["timing_provenance"]["edge_tolerance_ms"], 1)
        self.assertTrue(any(row["kind"] == "recording_edge_quantization" for row in result["timing_provenance"]["rounding_adjustments"]))
        for rows in ([turn(-0.0011, 1)], [turn(0, 1.0011)], [turn(-0.001, 0)], [turn(1, 1.001)]):
            with self.subTest(rows=rows), self.assertRaises(core.DiarizationError):
                self.normalize(rows, duration=1000)

    def test_genuine_exclusive_overlap_is_rejected_not_silently_trimmed(self):
        ordinary = [turn(0, 2, "a"), turn(1, 3, "b")]
        with self.assertRaisesRegex(core.DiarizationError, "genuine overlapping"):
            self.normalize(ordinary, ordinary)

    def test_quantization_that_would_erase_tiny_turn_is_rejected(self):
        with self.assertRaisesRegex(core.DiarizationError, "erase a turn"):
            self.normalize([turn(0, .0002, "a"), turn(.0002, .0003, "b")], duration=1)

    def test_duplicate_turn_unknown_exclusive_label_and_missing_view_rejected(self):
        for ordinary, exclusive in (([turn(0, 1)] * 2, [turn(0, 1)]),
                                    ([turn(0, 1)], [turn(0, 1, "unknown")]),
                                    ([turn(0, 1)], []), ([], [turn(0, 1)])):
            with self.subTest(ordinary=ordinary, exclusive=exclusive), self.assertRaises(core.DiarizationError):
                self.normalize(ordinary, exclusive)

    def test_malformed_nonfinite_and_chunk_annotated_rows_rejected(self):
        invalid = ([turn(float("nan"), 1)], [turn(0, float("inf"))], [turn(True, 2)],
                   [turn("0", 1)], [turn(1, 1)], [turn(2, 1)], [turn(0, 1, "")],
                   [{**turn(0, 1), "chunk_id": "part-1"}], [{**turn(0, 1), "confidence": 1.0}],
                   [turn(0, 1, "label\nname")])
        for rows in invalid:
            with self.subTest(rows=rows), self.assertRaises(core.DiarizationError):
                self.normalize(rows)

    def test_turn_speaker_and_document_bounds_are_enforced(self):
        with self.assertRaisesRegex(core.DiarizationError, "100000"):
            self.normalize([turn(0, 1)] * (core.MAX_TURNS + 1))
        rows = [turn(index, index + .5, f"speaker-{index}") for index in range(257)]
        with self.assertRaisesRegex(core.DiarizationError, "256"):
            self.normalize(rows, duration=300_000)
        from unittest import mock
        with mock.patch.object(core, "MAX_DOCUMENT_BYTES", 100):
            with self.assertRaisesRegex(core.DiarizationError, "silent chunk stitching"):
                self.normalize([turn(0, 1)])

    def test_duration_source_and_run_ids_are_strict(self):
        for duration in (0, True, 1000.0, 86_400_001):
            with self.subTest(duration=duration), self.assertRaises(core.DiarizationError):
                self.normalize(duration=duration)
        for media, run_id in (("bad", RUN), (MEDIA, "other_" + "b" * 32), (MEDIA, "diarjob_" + "b" * 31)):
            with self.assertRaises(core.DiarizationError):
                core.normalize_output(1000, [], [], media_sha256=media, run_id=run_id)

    def test_inputs_are_not_mutated(self):
        ordinary = [turn(0, 1, "b"), turn(1, 2, "a")]
        exclusive = copy.deepcopy(ordinary)
        before = copy.deepcopy((ordinary, exclusive))
        self.normalize(ordinary, exclusive)
        self.assertEqual((ordinary, exclusive), before)

    def test_result_replay_rejects_tampered_derived_fields_or_semantics(self):
        original = self.example()
        changes = [lambda r: r.update(schema_version=True), lambda r: r.update(extra=True),
                   lambda r: r["representations"]["ordinary"][0].update(start_ms=1),
                   lambda r: r["representations"]["ordinary"][0].update(overlap=False),
                   lambda r: r["representations"]["exclusive"][0].update(score_state="calibrated"),
                   lambda r: r["representations"]["exclusive"][0].update(speaker="SPEAKER_0999"),
                   lambda r: r["summary"].update(speaker_count=1),
                   lambda r: r["semantics"].update(identity_inferred=True),
                   lambda r: r["semantics"].update(chunk_stitching=True),
                   lambda r: r["timing_provenance"].update(edge_tolerance_ms=1000),
                   lambda r: r["timing_provenance"]["ordinary"][0].update(end="2.0")]
        for change in changes:
            value = copy.deepcopy(original)
            change(value)
            with self.subTest(change=change), self.assertRaises(core.DiarizationError):
                core.validate_result(value)

    def test_randomized_whole_recording_outputs_replay_deterministically(self):
        rng = random.Random(940472)
        for _ in range(80):
            boundary = 0
            rows = []
            for _ in range(rng.randint(1, 30)):
                start = boundary + rng.randint(0, 10) / 10000
                end = start + rng.randint(1, 50) / 10
                rows.append(turn(start, end, f"native-{rng.randrange(4)}"))
                boundary = end
            duration = int(boundary * 1000) + 100
            result = self.normalize(rows, duration=duration)
            self.assertEqual(core.validate_result(result), result)
            self.assertEqual(result["summary"]["overlap_ms"], 0)
            exclusive = result["representations"]["exclusive"]
            self.assertTrue(all(a["end_ms"] <= b["start_ms"] for a, b in zip(exclusive, exclusive[1:])))


if __name__ == "__main__":
    unittest.main()
