from __future__ import annotations

import copy
import errno
import hashlib
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from pipeline import speaker_screen as screen
from pipeline import speaker_screen_source_probe as probe


def document(**updates):
    stream = {"index": 0, "codec_type": "audio", "codec_name": "pcm_s16le",
              "sample_rate": "44100", "channels": 1, "duration_ts": 441001,
              "time_base": "1/44100"}
    stream.update(updates)
    return {"streams": [stream], "format": {"format_name": "wav", "duration": "11.000000"}}


class SourceProbeMetadataTests(unittest.TestCase):
    def test_audio_ticks_use_exact_integer_floor_not_container_rounding(self):
        value = probe._timeline(document(duration_ts=587845632))
        self.assertEqual(value["declared_duration_ms"], 13_329_832)
        self.assertEqual(value["method"], "audio_duration_ts_time_base_floor")
        self.assertFalse(value["container_duration_is_authoritative"])
        self.assertEqual(value["screenable_start_ms"], 100)
        self.assertEqual(value["leading_interval_not_admitted_ms"], 100)

    def test_duration_tag_fallback_is_explicit_and_floored(self):
        source = document(start_time="0.000000", start_pts=0, time_base="1/1000")
        source["streams"][0].pop("duration_ts")
        source["streams"][0]["tags"] = {"DURATION": "00:12:34.567890000"}
        source["format"]["format_name"] = "matroska,webm"
        value = probe._timeline(source)
        self.assertEqual(value["declared_duration_ms"], 754_567)
        self.assertEqual(value["method"], "audio_duration_tag_floor")

    def test_rounded_container_duration_alone_cannot_admit_timeline(self):
        source = document()
        source["streams"][0].pop("duration_ts")
        with self.assertRaisesRegex(probe.NeedsReview, "no_exact_audio_duration"):
            probe._timeline(source)

    def test_nonzero_or_unknown_container_audio_start_needs_review(self):
        for start in ("0.023", "-0.007", "10"):
            with self.subTest(start=start), self.assertRaisesRegex(probe.NeedsReview, "nonzero_audio_start"):
                probe._timeline(document(start_time=start))
        source = document()
        source["format"]["format_name"] = "matroska,webm"
        with self.assertRaisesRegex(probe.NeedsReview, "unknown_audio_start"):
            probe._timeline(source)

    def test_explicit_zero_pts_or_time_are_accepted(self):
        for updates in ({"start_time": "0.000000"}, {"start_pts": 0}):
            source = document(**updates)
            source["format"]["format_name"] = "mov,mp4,m4a,3gp,3g2,mj2"
            self.assertEqual(probe._timeline(source)["audio_origin"], "explicit_zero")

    def test_missing_or_multiple_selected_audio_streams_fail(self):
        for streams in ([], None, [document()["streams"][0]] * 2, [None]):
            with self.subTest(streams=streams), self.assertRaises(probe.NeedsReview):
                probe._timeline({"streams": streams})

    def test_invalid_tick_duration_timebase_or_tag_fail(self):
        for updates in ({"duration_ts": True}, {"duration_ts": 0}, {"duration_ts": 0.5},
                        {"time_base": "1/0"}, {"time_base": "0/1"}, {"time_base": "NaN"},
                        {"time_base": "1/44100", "duration_ts": 44100 * 86401}):
            with self.subTest(updates=updates), self.assertRaises(probe.NeedsReview):
                probe._timeline(document(**updates))
        for value in ("00:60:00.000", "nan", "-01:00:00", "00:00:00.0000000001", None):
            with self.subTest(value=value), self.assertRaises(probe.NeedsReview):
                probe._duration_tag(value)

    def test_probe_json_rejects_duplicates_nonfinite_and_malformed(self):
        for value in (b'{"a":1,"a":2}', b'{"a":NaN}', b'[]', b'not-json', b'\xff'):
            with self.subTest(value=value), self.assertRaises(probe.NeedsReview):
                probe._parse_json(value)

    def test_tail_full_output_is_not_confirmed_eof(self):
        with mock.patch.object(probe, "_bounded_command", return_value=bytes(probe.MAX_TAIL_PCM)):
            with self.assertRaisesRegex(probe.NeedsReview, "hit_bound"):
                probe._tail(1, 2, 1000, 100)

    def test_tail_measures_samples_without_arbitrary_padding(self):
        with mock.patch.object(probe, "_bounded_command", return_value=bytes(123_457 * 2)):
            value = probe._tail(1, 2, 5_000, 100)
        self.assertEqual(value["measured_eof_samples"], 203_457)
        self.assertEqual(value["duration_ms"], 12_716)
        self.assertEqual(value["fractional_millisecond_samples_discarded"], 1)

    def test_exact_probe_propagates_storage_io_failure(self):
        with mock.patch.object(probe, "_bounded_command", side_effect=OSError(errno.EIO, "Input/output error")):
            with self.assertRaises(OSError) as caught:
                probe._exact_probe(1, 2, 0, 10_000, 100)
        self.assertEqual(caught.exception.errno, errno.EIO)

    def test_exact_probe_rejects_short_pcm_without_padding(self):
        with mock.patch.object(probe, "_bounded_command", return_value=bytes(319_998)):
            with self.assertRaisesRegex(probe.NeedsReview, "short_or_malformed"):
                probe._exact_probe(1, 2, 0, 10_000, 100)

    def test_child_io_diagnostic_is_storage_failure_not_media_review(self):
        class FailedChild:
            returncode = 1
            def wait(self, timeout=None):
                return self.returncode
            def poll(self):
                return self.returncode
        def launch(_command, **kwargs):
            kwargs["stderr"].write(b"media: Input/output error\n")
            return FailedChild()
        with mock.patch.object(probe.subprocess, "Popen", side_effect=launch):
            with self.assertRaises(OSError) as caught:
                probe._bounded_command(["unused"], 1, 2, maximum=320_000, deadline=time.monotonic() + 30)
        self.assertEqual(caught.exception.errno, errno.EIO)


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "local FFmpeg tools required")
class SourceProbeNativeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="himr-source-probe-test-")
        cls.root = Path(cls.temp.name)
        cls.ffmpeg = cls.binding(Path(shutil.which("ffmpeg")))
        cls.ffprobe = cls.binding(Path(shutil.which("ffprobe")))
        cls.wav = cls.root / "exact.wav"
        cls.mkv = cls.root / "exact.mkv"
        cls.mp4 = cls.root / "audio.mp4"
        cls.opus_mp4 = cls.root / "opus.mp4"
        cls.opus_webm = cls.root / "opus.webm"
        cls.aac = cls.root / "raw.aac"
        commands = [
            (cls.wav, ["-ar", "44100", "-c:a", "pcm_s16le"]),
            (cls.mkv, ["-ar", "16000", "-c:a", "pcm_s16le"]),
            (cls.mp4, ["-ar", "44100", "-c:a", "aac"]),
            (cls.opus_mp4, ["-ar", "48000", "-c:a", "libopus"]),
            (cls.opus_webm, ["-ar", "48000", "-c:a", "libopus", "-avoid_negative_ts", "make_zero"]),
            (cls.aac, ["-ar", "44100", "-c:a", "aac", "-f", "adts"]),
        ]
        for output, options in commands:
            subprocess.run([cls.ffmpeg["path"], "-v", "error", "-nostdin", "-f", "lavfi", "-i",
                            "sine=frequency=440:sample_rate=44100", "-t", "22.123", "-ac", "1",
                            "-threads", "1", *options, str(output)], check=True, timeout=30,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    @staticmethod
    def binding(path):
        return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def recording(self, path=None):
        path = path or self.wav
        return {"media_id": "test-source", **self.binding(path), "byte_count": path.stat().st_size}

    def check_admitted(self, path):
        before = path.stat()
        value = probe.probe_source(self.recording(path), self.ffmpeg, self.ffprobe)
        self.assertEqual(value["status"], "admitted", value)
        self.assertGreater(value["duration_ms"], 22_000)
        self.assertLess(value["duration_ms"], 22_300)
        self.assertTrue(value["checks"]["first_probe"]["exact_pcm_length"])
        self.assertTrue(value["checks"]["last_probe"]["exact_pcm_length"])
        self.assertEqual(value["checks"]["first_probe"]["start_ms"], 100)
        self.assertEqual(value["checks"]["first_probe"]["end_ms"], min(10_100, value["duration_ms"]))
        self.assertGreaterEqual(value["checks"]["last_probe"]["start_ms"], 100)
        self.assertEqual(value["checks"]["last_probe"]["end_ms"], value["duration_ms"])
        self.assertEqual(value["timeline"]["measured_eof_ms"], value["duration_ms"])
        self.assertEqual(value["timeline"]["screenable_start_ms"], 100)
        self.assertEqual(value["timeline"]["leading_interval_not_admitted_ms"], 100)
        self.assertTrue(value["semantics"]["intro_excluded_from_screening"])
        self.assertTrue(value["semantics"]["absolute_source_timestamps_preserved"])
        self.assertFalse(value["semantics"]["missing_audio_padded"])
        self.assertTrue(value["tail_attempts"][-1]["eof_observed_before_decode_bound"])
        self.assertEqual(before.st_size, path.stat().st_size)
        self.assertEqual(before.st_mtime_ns, path.stat().st_mtime_ns)
        self.assertFalse(value["semantics"]["source_sha256_reverified"])
        return value

    def test_native_wav_exact_seek_timeline_admitted(self):
        value = self.check_admitted(self.wav)
        self.assertEqual(value["timeline"]["method"], "audio_duration_ts_time_base_floor")

    def test_native_matroska_duration_tag_timeline_admitted(self):
        value = self.check_admitted(self.mkv)
        self.assertEqual(value["timeline"]["method"], "audio_duration_tag_floor")

    def test_native_mp4_aac_tail_padding_measured_not_guessed(self):
        self.check_admitted(self.mp4)

    def test_native_opus_mp4_admits_explicit_screening_start(self):
        self.check_admitted(self.opus_mp4)

    def test_native_opus_webm_admits_explicit_screening_start(self):
        self.check_admitted(self.opus_webm)

    def test_native_opus_admission_pcm_matches_unchanged_decoder(self):
        for path in (self.opus_mp4, self.opus_webm):
            value = self.check_admitted(path)
            with self.subTest(path=path), screen.opened(path) as source, screen.opened(self.ffmpeg["path"], executable=True) as decoder:
                for label in ("first_probe", "last_probe"):
                    checked = value["checks"][label]
                    window = {key: checked[key] for key in ("index", "start_ms", "end_ms")}
                    expected = screen.decode_window(source, decoder, window, 30)
                    self.assertEqual(len(expected), checked["pcm_bytes"])
                    self.assertEqual(hashlib.sha256(expected).hexdigest(), checked["pcm_sha256"])

    def test_exact_decode_matches_unchanged_screen_decoder(self):
        for path in (self.wav, self.mkv, self.mp4):
            with self.subTest(path=path), screen.opened(path) as source, screen.opened(self.ffmpeg["path"], executable=True) as decoder:
                window = {"index": 575, "start_ms": 5_000, "end_ms": 15_000}
                try:
                    expected = screen.decode_window(source, decoder, window, 30)
                except screen.ScreenError:
                    # Millisecond container timestamp rounding can make an
                    # interior seek short. Both paths must reject it, not pad.
                    with self.assertRaises(probe.NeedsReview):
                        probe._exact_probe(source, decoder, window["start_ms"], window["end_ms"], time.monotonic() + 30)
                else:
                    actual = probe._exact_probe(source, decoder, window["start_ms"], window["end_ms"], time.monotonic() + 30)
                    self.assertEqual(actual["pcm_sha256"], hashlib.sha256(expected).hexdigest())
                    self.assertEqual(actual["pcm_bytes"], len(expected))

    def test_native_short_file_overstated_duration_repaired_before_first_probe(self):
        path = self.root / "short.wav"
        subprocess.run([self.ffmpeg["path"], "-v", "error", "-nostdin", "-i", str(self.wav), "-t", "2.005",
                        "-c:a", "pcm_s16le", "-threads", "1", str(path)], check=True, timeout=30,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        original = probe._ffprobe
        def overestimate(*args):
            value = original(*args)
            value["streams"][0]["duration_ts"] = 44100 * 3
            return value
        with mock.patch.object(probe, "_ffprobe", side_effect=overestimate):
            value = probe.probe_source(self.recording(path), self.ffmpeg, self.ffprobe)
        self.assertEqual(value["status"], "admitted", value)
        self.assertEqual(value["duration_ms"], 2_005)
        self.assertEqual(value["checks"]["first_probe"]["start_ms"], 100)
        self.assertEqual(value["checks"]["first_probe"]["end_ms"], 2_005)

    def test_audio_not_extending_past_excluded_intro_is_not_admitted(self):
        for duration in (1, 99, 100):
            with self.subTest(duration=duration), mock.patch.object(probe, "_tail", return_value={"duration_ms": duration}), \
                    mock.patch.object(probe, "_exact_probe") as exact:
                value = probe.probe_source(self.recording(), self.ffmpeg, self.ffprobe)
            self.assertEqual(value["status"], "needs_review")
            self.assertIsNone(value["duration_ms"])
            self.assertEqual(value["tail_attempts"][0]["error"], "audio_ends_before_screenable_start")
            exact.assert_not_called()

    def test_native_raw_aac_seek_timeline_admitted_or_explicitly_reviewed(self):
        value = probe.probe_source(self.recording(self.aac), self.ffmpeg, self.ffprobe)
        self.assertIn(value["status"], ("admitted", "needs_review"))
        if value["status"] == "admitted":
            self.assertEqual(value["checks"]["last_probe"]["end_ms"], value["duration_ms"])
        else:
            self.assertIsNone(value["duration_ms"])
            self.assertIsInstance(value["error"], dict)

    def test_native_corrupt_source_reported_without_admission(self):
        source = self.root / "corrupt.bin"
        source.write_bytes(b"not a media file\n")
        value = probe.probe_source(self.recording(source), self.ffmpeg, self.ffprobe)
        self.assertEqual(value["status"], "needs_review")
        self.assertIsNone(value["duration_ms"])
        self.assertIsNotNone(value["error"])

    def test_native_missing_audio_reported_without_admission(self):
        path = self.root / "video-only.mp4"
        subprocess.run([self.ffmpeg["path"], "-v", "error", "-nostdin", "-f", "lavfi", "-i",
                        "color=size=16x16:duration=1", "-c:v", "mpeg4", "-threads", "1", str(path)],
                       check=True, timeout=30, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        value = probe.probe_source(self.recording(path), self.ffmpeg, self.ffprobe)
        self.assertEqual(value["status"], "needs_review")
        self.assertEqual(value["error"]["code"], "no_audio_stream")

    def test_source_is_not_fully_hashed(self):
        original_hash = screen.hash_fd
        sizes = []
        def record_hash(descriptor, maximum, deadline):
            import os
            sizes.append(os.fstat(descriptor).st_size)
            return original_hash(descriptor, maximum, deadline)
        with mock.patch.object(screen, "hash_fd", side_effect=record_hash):
            self.check_admitted(self.wav)
        self.assertEqual(len(sizes), 2)
        self.assertEqual(sizes, [Path(self.ffmpeg["path"]).stat().st_size, Path(self.ffprobe["path"]).stat().st_size])

    def test_mismatched_source_size_or_tool_hash_rejected(self):
        with self.assertRaisesRegex(screen.ScreenError, "byte count"):
            probe.probe_source({**self.recording(), "byte_count": 1}, self.ffmpeg, self.ffprobe)
        with self.assertRaisesRegex(screen.ScreenError, "SHA-256"):
            probe.probe_source(self.recording(), {**self.ffmpeg, "sha256": "0" * 64}, self.ffprobe)

    def test_source_drift_rejected_even_after_apparently_successful_checks(self):
        original = probe._ffprobe
        source = self.root / "drift.wav"
        shutil.copyfile(self.wav, source)
        def mutate(*args):
            value = original(*args)
            import os
            os.utime(source, ns=(source.stat().st_atime_ns, source.stat().st_mtime_ns + 1))
            return value
        with mock.patch.object(probe, "_ffprobe", side_effect=mutate), self.assertRaisesRegex(screen.ScreenError, "changed"):
            probe.probe_source(self.recording(source), self.ffmpeg, self.ffprobe)

    def test_metadata_cannot_truncate_an_underestimated_stream(self):
        original = probe._ffprobe
        def underestimate(*args):
            value = original(*args)
            value["streams"][0]["duration_ts"] = 44100 * 20
            return value
        with mock.patch.object(probe, "_ffprobe", side_effect=underestimate):
            value = self.check_admitted(self.wav)
        self.assertEqual(value["timeline"]["declared_duration_ms"], 20_000)
        self.assertGreater(value["duration_ms"], 22_000)

    def test_duration_hint_only_guides_second_tail_attempt_not_admission(self):
        original = probe._ffprobe
        def underestimate(*args):
            value = original(*args)
            value["streams"][0]["duration_ts"] = 44100 * 10
            return value
        recording = {**self.recording(), "duration_hint_ms": 22_123}
        with mock.patch.object(probe, "_ffprobe", side_effect=underestimate):
            value = probe.probe_source(recording, self.ffmpeg, self.ffprobe)
        self.assertEqual(value["status"], "admitted", value)
        self.assertEqual(len(value["tail_attempts"]), 2)
        self.assertEqual(value["tail_attempts"][0]["status"], "needs_review")
        self.assertGreater(value["duration_ms"], 22_000)

    def test_invalid_call_bounds_rejected(self):
        for timeout in (0, 121, True, float("nan")):
            with self.subTest(timeout=timeout), self.assertRaises(screen.ScreenError):
                probe.probe_source(self.recording(), self.ffmpeg, self.ffprobe, timeout=timeout)
        with self.assertRaises(screen.ScreenError):
            probe.probe_source({**self.recording(), "duration_hint_ms": -1}, self.ffmpeg, self.ffprobe)


if __name__ == "__main__":
    unittest.main()
