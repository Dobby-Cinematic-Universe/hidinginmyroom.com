from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from pipeline import speaker_face_matching_visual as visual

try:
    import numpy as np
except ImportError:
    np = None


def video_log(start_ms=0, count=25, *, gap_at=None):
    lines = []
    for name in ("match_source", "match_output"):
        lines.append(f"[showinfo@{name} @ 0x1] config in time_base: 1/25, frame_rate: 25/1")
        for index in range(count):
            pts = start_ms // 40 + index
            if name == "match_source" and gap_at is not None and index >= gap_at:
                pts += 1
            lines.append(f"[showinfo@{name} @ 0x1] n: {index} pts: {pts} pts_time:0 duration:1 duration_time:0.04 fmt:bgr24")
    return "\n".join(lines)


def audio_log(start_ms=0, count=25, *, gap_at=None):
    lines = []
    for index in range(count):
        pts = start_ms * 16 + index * 640 + (1 if gap_at is not None and index >= gap_at else 0)
        lines.append(f"[ashowinfo@match_audio @ 0x2] n:{index} pts:{pts} pts_time:0 fmt:s16 channels:1 chlayout:mono rate:16000 nb_samples:640 checksum:0")
    return "\n".join(lines)


class DecodeContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.video = bytes(25 * visual.FRAME_BYTES)
        cls.audio = bytes(25 * 640 * 2)

    def receipt(self, **overrides):
        kwargs = dict(start_ms=0, end_ms=1000, video_stderr=video_log(), audio_stderr=audio_log())
        kwargs.update(overrides)
        return visual.validate_decoded(self.video, self.audio, **kwargs)

    def test_absolute_grid_and_bounds(self):
        for start, end in ((1, 1001), (0, 960), (0, 5040), (True, 1000), (1000, 0)):
            with self.subTest(start=start, end=end), self.assertRaises(visual.VisualError):
                visual.validate_clip(start, end)
        self.assertEqual(visual.validate_clip(440, 5440), 125)

    def test_commands_are_bounded_cpu_timestamp_preserving_and_no_overwrite(self):
        commands = visual.build_decode_commands("/usr/bin/ffmpeg", "/source.mkv", 400, 1400, "/private/v.bgr", "/private/a.pcm")
        self.assertEqual([x["kind"] for x in commands], ["video", "audio"])
        self.assertEqual(commands[0]["max_file_bytes"], 25 * visual.FRAME_BYTES)
        self.assertEqual(commands[1]["max_file_bytes"], 32000)
        for command in commands:
            self.assertIn("-copyts", command["argv"])
            self.assertEqual(command["argv"][command["argv"].index("-hwaccel") + 1], "none")
            self.assertEqual(command["argv"][command["argv"].index("-protocol_whitelist") + 1], "file,pipe")
            self.assertEqual(command["argv"][command["argv"].index("-format_whitelist") + 1], visual.FORMATS)
            self.assertNotIn("hls", visual.FORMATS.split(","))
            self.assertNotIn("concat", visual.FORMATS.split(","))
            self.assertIn("-n", command["argv"])
            self.assertNotIn("-y", command["argv"])
            self.assertNotIn("STARTPTS", " ".join(command["argv"]))
            self.assertNotIn("apad", " ".join(command["argv"]))
        self.assertIn("async=0", " ".join(commands[1]["argv"]))

    def test_decoder_rejects_alias_or_nonlocal_outputs(self):
        for source, video in (("/a", "/a"), ("https://host/a", "/v"), ("/a", "relative")):
            with self.subTest(source=source, video=video), self.assertRaises(visual.VisualError):
                visual.build_decode_commands("/ffmpeg", source, 0, 1000, video, "/audio")

    def test_valid_receipt_replays_without_claiming_source_lipsync(self):
        result = self.receipt()
        self.assertTrue(result["av_sync_verified"])
        self.assertEqual(result["av_sync_scope"], "container_timestamp_alignment_only")
        self.assertFalse(result["source_lip_sync_verified"])
        self.assertEqual(visual.validate_receipt(json.loads(json.dumps(result)), start_ms=0, end_ms=1000), result)

    def test_receipt_absolute_nonzero_pts(self):
        result = self.receipt(start_ms=400, end_ms=1400, video_stderr=video_log(400), audio_stderr=audio_log(400))
        self.assertEqual(result["audio_timestamps"][0], [6400, 640])

    def test_rejects_source_frame_gap(self):
        with self.assertRaisesRegex(visual.VisualError, "gap"):
            self.receipt(video_stderr=video_log(gap_at=10))

    def test_rejects_audio_gap_even_one_sample(self):
        with self.assertRaisesRegex(visual.VisualError, "gap"):
            self.receipt(audio_stderr=audio_log(gap_at=10))

    def test_rejects_initial_audio_offset(self):
        with self.assertRaisesRegex(visual.VisualError, "offset"):
            self.receipt(audio_stderr=audio_log(40))

    def test_rejects_missing_and_duplicate_timebase(self):
        for log in ("", video_log() + "\n[showinfo@match_source @ 0x1] config in time_base: 1/25, frame_rate: 25/1"):
            with self.subTest(log=log[:15]), self.assertRaises(visual.VisualError):
                self.receipt(video_stderr=log)

    def test_rejects_truncated_raw_arrays(self):
        for video, audio in ((self.video[:-1], self.audio), (self.video, self.audio[:-2])):
            with self.assertRaisesRegex(visual.VisualError, "byte count"):
                visual.validate_decoded(video, audio, start_ms=0, end_ms=1000,
                                        video_stderr=video_log(), audio_stderr=audio_log())

    def test_rejects_audio_stereo_and_wrong_rate(self):
        for log in (audio_log().replace("channels:1", "channels:2"), audio_log().replace("rate:16000", "rate:48000")):
            with self.assertRaises(visual.VisualError):
                self.receipt(audio_stderr=log)

    def test_rejects_missing_duration_or_unordered_frames(self):
        for log in (video_log().replace("duration:1", "duration:0", 1), video_log().replace("n: 2 ", "n: 3 ", 1)):
            with self.assertRaises(visual.VisualError):
                self.receipt(video_stderr=log)

    def test_receipt_tampering_is_rejected(self):
        original = self.receipt()
        for field, value in (("av_sync_verified", False), ("video_bytes", 1), ("extra", True),
                             ("audio_sha256", "bad"), ("schema_version", True), ("start_ms", 40)):
            changed = copy.deepcopy(original)
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(visual.VisualError):
                visual.validate_receipt(changed, start_ms=0, end_ms=1000)

    def test_receipt_timeline_tampering_rejected(self):
        original = self.receipt()
        for field, row in (("source_timestamps", [0, 2, 2, 50]), ("audio_timestamps", [1, 640]),
                           ("output_timestamps", [1, 25, 1, 25])):
            changed = copy.deepcopy(original)
            changed[field][0] = row
            with self.subTest(field=field), self.assertRaises(visual.VisualError):
                visual.validate_receipt(changed, start_ms=0, end_ms=1000)

    def test_stderr_limit_and_invalid_utf8(self):
        for log in (b"\xff", b"x" * (visual.MAX_STDERR_BYTES + 1)):
            with self.assertRaises(visual.VisualError):
                self.receipt(audio_stderr=log)


class FakeCV2:
    COLOR_BGR2GRAY = 6
    INTER_LINEAR = 1

    def __init__(self):
        self.threads = None

    def setNumThreads(self, value):
        self.threads = value

    @staticmethod
    def cvtColor(array, mode):
        return array.mean(axis=2).astype(np.uint8)

    @staticmethod
    def resize(array, size, interpolation):
        ys = np.linspace(0, array.shape[0] - 1, size[1]).astype(int)
        xs = np.linspace(0, array.shape[1] - 1, size[0]).astype(int)
        return array[ys[:, None], xs[None, :]]


def face(x=100, y=100, width=60, height=60, score=0.99):
    return [x, y, width, height] + [x + width / 2, y + height / 2] * 5 + [score]


class FakeDetector:
    def __init__(self, detections):
        self.detections = iter(detections)
        self.input_size = None

    def setInputSize(self, size):
        self.input_size = size

    def detect(self, frame):
        return 0, next(self.detections)


@unittest.skipUnless(np is not None, "NumPy is unavailable; no installation is performed")
class VisualGeometryTests(unittest.TestCase):
    def setUp(self):
        self.frames = np.zeros((25, 360, 640, 3), dtype=np.uint8)
        self.cv2 = FakeCV2()

    def run_visual(self, detections, **kwargs):
        return visual.detect_and_track(kwargs.pop("frames", self.frames), clip_id=kwargs.pop("clip_id", "clip_a"),
                                       start_ms=0, detector=FakeDetector(detections), np=np, cv2=self.cv2, **kwargs)

    def test_prepare_arrays_exact_shapes_and_signed_audio(self):
        frames, audio = visual.prepare_arrays(self.frames.tobytes(), b"\xff\x7f" * 16000,
                                              start_ms=0, end_ms=1000, np=np)
        self.assertEqual(frames.shape, self.frames.shape)
        self.assertEqual(audio.shape, (16000,))
        self.assertEqual(int(audio[0]), 32767)

    def test_no_face_is_valid_unknown(self):
        result = self.run_visual([None] * 25)
        self.assertEqual(result["tracks"], [])
        self.assertFalse(result["metadata"]["embeddings_present"])
        self.assertFalse(result["metadata"]["cross_clip_linking"])

    def test_two_faces_remain_separate_contiguous_anonymous_tracks(self):
        result = self.run_visual([[face(100), face(400)]] * 25)
        self.assertEqual(len(result["tracks"]), 2)
        for track in result["tracks"]:
            self.assertRegex(track["track_id"], r"^face_track_[0-9a-f]{32}$")
            self.assertRegex(track["shot_id"], r"^shot_[0-9a-f]{32}$")
            self.assertEqual(track["frame_indices"], list(range(25)))
            self.assertEqual(track["crops"].shape, (25, 112, 112))
        self.assertEqual(self.cv2.threads, 1)

    def test_missing_detection_splits_track_without_interpolation(self):
        result = self.run_visual([[face()]] * 10 + [None] + [[face()]] * 14)
        self.assertEqual([t["frame_indices"] for t in result["tracks"]], [list(range(10)), list(range(11, 25))])

    def test_cut_resets_tracks_and_marks_clip_unscoreable(self):
        self.frames[12:] = 255
        result = self.run_visual([[face()]] * 25)
        self.assertEqual(result["cuts"], [12])
        self.assertEqual(len(result["tracks"]), 2)
        self.assertNotEqual(result["tracks"][0]["shot_id"], result["tracks"][1]["shot_id"])
        self.assertFalse(result["metadata"]["cut_clip_scoreable"])

    def test_border_faces_remain_observed_but_crops_are_not_padded(self):
        result = self.run_visual([[face(0, 0)]] * 25)
        self.assertEqual(len(result["tracks"]), 1)
        self.assertEqual(result["tracks"][0]["frame_indices"], list(range(25)))
        self.assertIsNone(result["tracks"][0]["crops"])
        self.assertEqual(result["metadata"]["rejected_border_detections"], 25)

    def test_border_face_is_preserved_as_competitor_to_usable_face(self):
        result = self.run_visual([[face(0, 0), face(300, 100)]] * 25)
        self.assertEqual(len(result["tracks"]), 2)
        border, usable = result["tracks"]
        self.assertIsNone(border["crops"])
        self.assertEqual(border["frame_indices"], list(range(25)))
        self.assertEqual(usable["crops"].shape, (25, 112, 112))

    def test_one_uncroppable_frame_disables_track_crops_without_hiding_faces(self):
        result = self.run_visual([[face(10, 100)]] * 10 + [[face(5, 100)]] + [[face(10, 100)]] * 14)
        self.assertEqual(len(result["tracks"]), 1)
        self.assertIsNone(result["tracks"][0]["crops"])
        self.assertEqual(result["tracks"][0]["frame_indices"], list(range(25)))

    def test_clip_scope_prevents_cross_recording_face_linking(self):
        first = self.run_visual([[face()]] * 25, clip_id="clip_a")
        second = self.run_visual([[face()]] * 25, clip_id="clip_b")
        self.assertNotEqual(first["tracks"][0]["track_id"], second["tracks"][0]["track_id"])

    def test_ambiguous_geometry_splits_existing_tracks(self):
        result = self.run_visual([[face(100), face(120)]] * 25)
        self.assertEqual(result["metadata"]["ambiguous_geometry_frames"], list(range(1, 25)))
        self.assertTrue(all(len(t["frame_indices"]) == 1 for t in result["tracks"]))

    def test_rejects_malformed_and_excessive_detector_outputs(self):
        for faces in ([[float("nan")] * 15], [face(width=-1)], [face()] * 9, [[1, 2]]):
            with self.subTest(faces=str(faces)[:20]), self.assertRaises(visual.VisualError):
                self.run_visual([faces] * 25)

    def test_low_score_faces_excluded_but_tiny_faces_remain_unknown_competitors(self):
        result = self.run_visual([[face(score=0.5), face(width=10, height=10)]] * 25)
        self.assertEqual(len(result["tracks"]), 1)
        self.assertIsNone(result["tracks"][0]["crops"])
        self.assertEqual(result["tracks"][0]["frame_indices"], list(range(25)))
        self.assertEqual(result["metadata"]["rejected_small_detections"], 25)

    def test_rejects_non_uint8_or_wrong_dimensions(self):
        for frames in (self.frames.astype(np.float32), self.frames[:, :, :300]):
            with self.assertRaises(visual.VisualError):
                self.run_visual([None] * 25, frames=frames)


@unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg is unavailable; no installation is performed")
class NativeDecodeTests(unittest.TestCase):
    def native_decode(self, fps=25, video_filter=None, audio_filter=None, retained_fds=False):
        with tempfile.TemporaryDirectory(prefix="himr-av-matching-test-") as directory:
            root = Path(directory)
            source = root / "synthetic.mkv"
            command = [shutil.which("ffmpeg"), "-hide_banner", "-nostdin", "-loglevel", "error", "-n",
                       "-f", "lavfi", "-i", f"testsrc2=size=320x180:rate={fps}:duration=2",
                       "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=2",
                       "-c:v", "ffv1", "-threads:v", "1", "-c:a", "pcm_s16le"]
            if video_filter:
                command += ["-vf", video_filter, "-fps_mode", "passthrough"]
            if audio_filter:
                command += ["-af", audio_filter]
            command.append(str(source))
            subprocess.run(command, check=True, capture_output=True, timeout=30)
            source_before = source.read_bytes()
            logs = {}
            descriptors = []
            try:
                executable, input_path = shutil.which("ffmpeg"), str(source)
                if retained_fds:
                    descriptors = [os.open(executable, os.O_RDONLY), os.open(source, os.O_RDONLY)]
                    executable, input_path = (f"/proc/self/fd/{fd}" for fd in descriptors)
                    os.lseek(descriptors[1], 0, os.SEEK_END)
                outputs = visual.build_decode_commands(executable, input_path, 400, 1400,
                                                         str(root / "video.bgr"), str(root / "audio.pcm"))
                for output in outputs:
                    completed = subprocess.run(output["argv"], check=True, capture_output=True,
                                               pass_fds=descriptors, timeout=30)
                    self.assertLessEqual(len(completed.stderr), output["stderr_max"])
                    self.assertLessEqual(Path(output["output_path"]).stat().st_size, output["max_file_bytes"])
                    logs[output["kind"]] = completed.stderr
            finally:
                for descriptor in descriptors:
                    os.close(descriptor)
            receipt = visual.validate_decoded((root / "video.bgr").read_bytes(), (root / "audio.pcm").read_bytes(),
                                               start_ms=400, end_ms=1400, video_stderr=logs["video"], audio_stderr=logs["audio"])
            self.assertEqual(receipt["frame_count"], 25)
            self.assertEqual(source.read_bytes(), source_before)
            return receipt

    def test_bounded_native_decode_preserves_nonzero_absolute_timestamps(self):
        self.native_decode()

    def test_native_retained_executable_and_media_fds_are_seekable(self):
        self.native_decode(retained_fds=True)

    def test_native_24fps_and_30fps_normalization(self):
        for fps in (24, 30):
            with self.subTest(fps=fps):
                receipt = self.native_decode(fps=fps)
                self.assertLess(receipt["source_first_frame_offset_ms"], 40)

    def test_native_video_gap_is_not_hidden_by_cfr_conversion(self):
        with self.assertRaisesRegex(visual.VisualError, "gap"):
            self.native_decode(video_filter="select='not(eq(n,20))'")

    def test_native_audio_gap_is_not_hidden_by_pcm_flattening(self):
        with self.assertRaises(visual.VisualError):
            self.native_decode(audio_filter="asetpts=PTS+if(gte(T\\,0.7)\\,0.02/TB\\,0)")

    def test_native_external_reference_playlist_format_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="himr-av-matching-test-") as directory:
            root = Path(directory)
            source = root / "playlist.m3u8"
            source.write_text("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nfile:///nonexistent/never-open.ts\n#EXT-X-ENDLIST\n")
            command = visual.build_decode_commands(shutil.which("ffmpeg"), str(source), 0, 1000,
                                                     str(root / "video.bgr"), str(root / "audio.pcm"))[0]
            completed = subprocess.run(command["argv"], capture_output=True, timeout=30)
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn(b"not on whitelist", completed.stderr)
            self.assertFalse((root / "video.bgr").exists())


if __name__ == "__main__":
    unittest.main()
