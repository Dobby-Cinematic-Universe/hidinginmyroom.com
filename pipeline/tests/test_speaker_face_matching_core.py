"""Pure anonymous A/V contracts; no faces, models, GPU, files or network."""
import copy
import unittest

from pipeline import screened_diarization_core as diarization
from pipeline import speaker_face_matching_core as core


MEDIA = "a" * 64
RUN = "diarjob_" + "b" * 32
SHOT = "shot_" + "c" * 32
TRACK = "face_track_" + "d" * 32
TRACK2 = "face_track_" + "e" * 32


def turn(start, end, speaker="native-a"):
    return {"start": start, "end": end, "speaker": speaker}


def diarized(ordinary=None, exclusive=None, duration=120_000):
    ordinary = [turn(0, duration / 1000)] if ordinary is None else ordinary
    exclusive = ordinary if exclusive is None else exclusive
    return diarization.normalize_output(duration, ordinary, exclusive, media_sha256=MEDIA, run_id=RUN)


def clip():
    return core.plan_clips(diarized())["clips"][0]


def face(track=TRACK, score=2.0, **kw):
    return {"track_id": track, "raw_logit": score, "face_width_px": 128,
            "face_height_px": 128, "visible": True, "occluded": False, **kw}


def observations(sample=None, faces=None):
    sample = clip() if sample is None else sample
    faces = [face()] if faces is None else faces
    return {"av_sync": {"state": "verified", "offset_ms": 0},
            "frames": [{"time_ms": when, "shot_id": SHOT, "faces": copy.deepcopy(faces)}
                       for when in range(sample["start_ms"], sample["end_ms"], 40)]}


class PolicyTests(unittest.TestCase):
    def test_defaults_are_fresh_and_bounded(self):
        result = core.validate_policy()
        self.assertEqual(result["min_face_px"], 64)
        self.assertEqual(result["max_clips"], 96)
        result["max_clips"] = 1
        self.assertEqual(core.validate_policy()["max_clips"], 96)

    def test_invalid_policy_values_are_rejected(self):
        for key, value in (("min_clip_ms", True), ("max_clip_ms", 5001), ("max_clip_ms", 2001),
                           ("clips_per_speaker", 17), ("max_clips", 257), ("min_face_px", 0),
                           ("min_track_coverage", float("nan")), ("min_mean_raw_logit", float("inf")),
                           ("min_winner_margin", -1), ("min_winner_margin", 10**1000)):
            policy = core.validate_policy()
            policy[key] = value
            with self.subTest(key=key, value=str(value)[:40]), self.assertRaises(core.MatchingError):
                core.validate_policy(policy)

    def test_missing_extra_and_reversed_policy_fields_are_rejected(self):
        for change in (lambda p: p.pop("min_clip_ms"), lambda p: p.update(person_name="invented"),
                       lambda p: p.update(min_clip_ms=5000, max_clip_ms=2000)):
            policy = core.validate_policy()
            change(policy)
            with self.assertRaises(core.MatchingError):
                core.validate_policy(policy)


class PlannerTests(unittest.TestCase):
    def test_long_recording_samples_early_middle_late_not_entire_recording(self):
        result = core.plan_clips(diarized())
        clips = result["clips"]
        self.assertEqual(len(clips), 3)
        self.assertLess(clips[0]["start_ms"], 1000)
        self.assertTrue(clips[1]["start_ms"] < 60_000 < clips[1]["end_ms"])
        self.assertGreater(clips[-1]["end_ms"], 119_000)
        self.assertEqual(result["coverage"]["sampled_ms"], 15_000)
        self.assertFalse(result["coverage"]["complete_recording_screened"])
        self.assertEqual(result["coverage"]["speakers"][0]["unsampled_speech_ms"], 105_000)
        for sample in clips:
            self.assertEqual(core.validate_clip(sample), sample)

    def test_silence_has_no_invented_speaker_or_samples(self):
        result = core.plan_clips(diarized([], []))
        self.assertEqual(result["clips"], [])
        self.assertEqual(result["coverage"]["speakers"], [])
        self.assertEqual(result["coverage"]["total_speech_ms"], 0)

    def test_overlap_is_excluded_using_ordinary_not_exclusive(self):
        source = diarized([turn(0, 30), turn(10, 20, "native-b")], [turn(0, 30)])
        result = core.plan_clips(source)
        for sample in result["clips"]:
            self.assertTrue(sample["end_ms"] <= 9800 or sample["start_ms"] >= 20_200)
        other = result["coverage"]["speakers"][1]
        self.assertEqual(other["clean_speech_ms"], 0)
        self.assertEqual(other["skipped_reason"], "no_clean_turn_long_enough")

    def test_all_overlapping_speakers_are_skipped(self):
        source = diarized([turn(0, 30), turn(0, 30, "native-b")], [turn(0, 30)])
        self.assertEqual(core.plan_clips(source)["clips"], [])

    def test_same_speaker_adjacent_and_overlapping_turns_are_merged(self):
        source = diarized([turn(0, 5), turn(4, 10), turn(10, 20)], [turn(0, 20)])
        result = core.plan_clips(source)
        self.assertEqual(result["coverage"]["speakers"][0]["ordinary_speech_ms"], 20_000)
        self.assertEqual(result["coverage"]["speakers"][0]["clean_speech_ms"], 20_000)
        self.assertEqual(result["clips"][0]["source_turn_indices"], [0, 1])

    def test_grid_quantization_and_guard_stay_inside_speech(self):
        source = diarized([turn(0.013, 10.017)])
        result = core.plan_clips(source)
        for sample in result["clips"]:
            self.assertEqual(sample["start_ms"] % 40, 0)
            self.assertEqual(sample["end_ms"] % 40, 0)
            self.assertGreaterEqual(sample["start_ms"], 213)
            self.assertLessEqual(sample["end_ms"], 9817)

    def test_short_turn_skips_and_exact_minimum_admits(self):
        short = core.plan_clips(diarized([turn(0, 2.399)]))
        self.assertEqual(short["clips"], [])
        exact = core.plan_clips(diarized([turn(0, 2.4)]))
        self.assertEqual(len(exact["clips"]), 1)
        self.assertEqual(exact["coverage"]["sampled_ms"], 2000)

    def test_sampling_never_spans_silence(self):
        source = diarized([turn(0, 8), turn(90, 100), turn(110, 120)])
        for sample in core.plan_clips(source)["clips"]:
            self.assertTrue(any(sample["start_ms"] >= int(row["start"] * 1000) and
                                sample["end_ms"] <= int(row["end"] * 1000)
                                for row in [turn(0, 8), turn(90, 100), turn(110, 120)]))

    def test_global_cap_is_round_robin_and_explicit(self):
        rows = [turn(i * 20, (i + 1) * 20, "native-" + str(i)) for i in range(4)]
        policy = core.validate_policy()
        policy["max_clips"] = 2
        result = core.plan_clips(diarized(rows), policy)
        self.assertEqual(len(result["clips"]), 2)
        self.assertEqual(len({sample["speaker"] for sample in result["clips"]}), 2)
        self.assertEqual([row["skipped_reason"] for row in result["coverage"]["speakers"]][2:],
                         ["global_clip_limit", "global_clip_limit"])

    def test_recording_scope_changes_clip_id(self):
        first = clip()
        source = diarization.normalize_output(120_000, [turn(0, 120)], [turn(0, 120)],
                                             media_sha256="f" * 64, run_id=RUN)
        self.assertNotEqual(first["clip_id"], core.plan_clips(source)["clips"][0]["clip_id"])

    def test_plan_replays_deterministically_without_mutating_input(self):
        source = diarized()
        before = copy.deepcopy(source)
        self.assertEqual(core.plan_clips(source), core.plan_clips(source))
        self.assertEqual(source, before)

    def test_altered_diarization_is_rejected(self):
        source = diarized()
        source["representations"]["ordinary"][0]["speaker"] = "invented-name"
        with self.assertRaises(core.MatchingError):
            core.plan_clips(source)

    def test_clip_edit_unknown_field_and_bad_grid_are_rejected(self):
        for change in (lambda c: c.update(speaker="Person Name"), lambda c: c.update(start_ms=201),
                       lambda c: c.update(end_ms=c["end_ms"] - 40), lambda c: c.update(name="Somebody"),
                       lambda c: c.update(source_turn_indices=[0, 0])):
            sample = clip()
            change(sample)
            with self.assertRaises(core.MatchingError):
                core.validate_clip(sample)


class AssociationTests(unittest.TestCase):
    def match(self, evidence=None, **kwargs):
        sample = clip()
        return core.associate(sample, observations(sample) if evidence is None else evidence, **kwargs)

    def test_one_positive_face_is_private_anonymous_candidate(self):
        result = self.match()
        self.assertEqual(result["status"], "candidate_match")
        self.assertEqual(result["candidate_track_id"], TRACK)
        self.assertEqual(result["scope"]["speaker"], "SPEAKER_0000")
        self.assertEqual(result["track_metrics"][0]["mean_raw_logit"], 2.0)
        self.assertFalse(result["semantics"]["identity_inferred"])
        self.assertFalse(result["semantics"]["scores_calibrated"])
        self.assertFalse(result["semantics"]["unsampled_intervals_attributed"])
        self.assertEqual(core.validate_result(result), result)

    def test_independent_inactive_competitor_does_not_force_argmax(self):
        result = self.match(observations(faces=[face(), face(TRACK2, -1.0)]))
        self.assertEqual(result["status"], "candidate_match")
        self.assertEqual(len(result["track_metrics"]), 2)

    def test_multiple_positive_faces_abstain_even_with_large_score_difference(self):
        result = self.match(observations(faces=[face(score=10.0), face(TRACK2, 1.0)]))
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["candidate_track_id"])
        self.assertIn("multiple_active_faces", result["reasons"])
        self.assertIn("ambiguous_active_tracks", result["reasons"])

    def test_even_brief_concurrent_active_face_abstains(self):
        evidence = observations(faces=[face(), face(TRACK2, -1.0)])
        evidence["frames"][25]["faces"][1]["raw_logit"] = 1.0
        result = self.match(evidence)
        self.assertEqual(result["status"], "unknown")
        self.assertIn("multiple_active_faces", result["reasons"])

    def test_all_inactive_and_zero_scores_do_not_select_a_face(self):
        for score in (-2.0, 0.0):
            result = self.match(observations(faces=[face(score=score)]))
            self.assertEqual(result["status"], "unknown")
            self.assertIn("no_consistent_active_track", result["reasons"])

    def test_small_occluded_invisible_or_unscored_face_abstains(self):
        for change in ({"face_width_px": 63}, {"face_height_px": 63}, {"occluded": True},
                       {"visible": False}, {"raw_logit": None}):
            evidence = observations()
            evidence["frames"][20]["faces"][0].update(change)
            result = self.match(evidence)
            self.assertEqual(result["status"], "unknown")
            self.assertIn("face_quality_or_missing_score", result["reasons"])

    def test_unscored_competitor_cannot_be_ignored(self):
        result = self.match(observations(faces=[face(), face(TRACK2, None)]))
        self.assertEqual(result["status"], "unknown")

    def test_shot_cut_abstains_without_cross_shot_join(self):
        evidence = observations()
        for frame in evidence["frames"][60:]:
            frame["shot_id"] = "shot_" + "f" * 32
        result = self.match(evidence)
        self.assertEqual(result["status"], "unknown")
        self.assertIn("shot_cut_inside_clip", result["reasons"])
        self.assertEqual(len(result["track_metrics"]), 2)

    def test_missing_middle_or_edge_frames_abstain(self):
        for index in (0, 20, -1):
            evidence = observations()
            del evidence["frames"][index]
            result = self.match(evidence)
            self.assertEqual(result["status"], "unknown")
            self.assertIn("missing_frames_or_timestamp_gap", result["reasons"])

    def test_empty_frames_and_faces_abstain(self):
        for evidence in ({"av_sync": {"state": "verified", "offset_ms": 0}, "frames": []},
                         observations(faces=[])):
            result = self.match(evidence)
            self.assertEqual(result["status"], "unknown")
            self.assertIn("no_visible_faces", result["reasons"])

    def test_unknown_failed_or_bad_timestamp_alignment_abstains(self):
        for sync in ({"state": "unknown", "offset_ms": None}, {"state": "failed", "offset_ms": 0},
                     {"state": "verified", "offset_ms": 41}):
            evidence = observations()
            evidence["av_sync"] = sync
            self.assertEqual(self.match(evidence)["status"], "unknown")

    def test_insufficient_margin_abstains(self):
        result = self.match(observations(faces=[face(score=0.1), face(TRACK2, -0.1)]))
        self.assertEqual(result["status"], "unknown")
        self.assertIn("insufficient_raw_score_margin", result["reasons"])

    def test_calibration_cannot_promote_heuristic_to_probability(self):
        with self.assertRaises(core.MatchingError):
            self.match(calibration={"confidence": 0.99})

    def test_illegal_observation_contracts_are_rejected(self):
        changes = [lambda e: e.update(person_name="Someone"),
                   lambda e: e["av_sync"].update(offset_ms=None),
                   lambda e: e["av_sync"].update(state="human_verified"),
                   lambda e: e["frames"][0].update(time_ms=201),
                   lambda e: e["frames"][0].update(time_ms=True),
                   lambda e: e["frames"][0].update(shot_id="living-room"),
                   lambda e: e["frames"][0]["faces"].append(face()),
                   lambda e: e["frames"][0]["faces"][0].update(raw_logit=float("nan")),
                   lambda e: e["frames"][0]["faces"][0].update(raw_logit=float("inf")),
                   lambda e: e["frames"][0]["faces"][0].update(raw_logit=10**1000),
                   lambda e: e["frames"][0]["faces"][0].update(raw_logit=True),
                   lambda e: e["frames"][0]["faces"][0].update(track_id="Person Name"),
                   lambda e: e["frames"][0]["faces"][0].update(visible=1),
                   lambda e: e["frames"][0]["faces"][0].update(probability=0.9)]
        for change in changes:
            evidence = observations()
            change(evidence)
            with self.subTest(change=change), self.assertRaises(core.MatchingError):
                self.match(evidence)

    def test_reordered_or_duplicate_frames_are_rejected(self):
        for duplicate in (False, True):
            evidence = observations()
            if duplicate:
                evidence["frames"][1] = copy.deepcopy(evidence["frames"][0])
            else:
                evidence["frames"].reverse()
            with self.assertRaises(core.MatchingError):
                self.match(evidence)

    def test_replay_detects_derived_decision_and_scope_edits(self):
        for change in (lambda r: r.update(status="verified_match"),
                       lambda r: r.update(candidate_track_id=TRACK2),
                       lambda r: r["scope"].update(media_sha256="f" * 64),
                       lambda r: r["semantics"].update(publication_authority=True),
                       lambda r: r["track_metrics"][0].update(mean_raw_logit=99)):
            result = self.match()
            change(result)
            with self.assertRaises(core.MatchingError):
                core.validate_result(result)

    def test_association_is_deterministic_and_does_not_mutate_inputs(self):
        sample, evidence = clip(), observations()
        before = copy.deepcopy(evidence)
        self.assertEqual(core.associate(sample, evidence), core.associate(sample, evidence))
        self.assertEqual(evidence, before)


if __name__ == "__main__":
    unittest.main()
