from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import wave
from types import SimpleNamespace
from unittest import mock

from pipeline import cloud_transcription_media as media
from pipeline import transcript_summary as io


class CloudTranscriptionMediaTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.folder = self.root / 'lane'
        self.folder.mkdir(mode=0o700)
        self.source = self.root / 'source.wav'
        self.write_wav(self.source, rate=8000, channels=2)
        self.recording = {'duration_ms': 1000, 'media': self.binding(self.source)}
        self.binary_path = Path(shutil.which('ffmpeg') or '/usr/bin/ffmpeg').resolve()
        self.ffmpeg = {'path': str(self.binary_path),
                       'sha256': hashlib.sha256(self.binary_path.read_bytes()).hexdigest()} if self.binary_path.exists() else None
        self.original = self.source.read_bytes()

    @staticmethod
    def write_wav(path, *, rate=16000, channels=1, width=2, seconds=1):
        with wave.open(str(path), 'wb') as output:
            output.setnchannels(channels)
            output.setsampwidth(width)
            output.setframerate(rate)
            output.writeframes(b'\x00' * int(rate * seconds) * channels * width)
        path.chmod(0o600)

    @staticmethod
    def binding(path):
        raw = path.read_bytes()
        return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest(), 'byte_count': len(raw)}

    def fake_decoder(self, command, **kwargs):
        target = kwargs['pass_fds'][-1]
        with os.fdopen(os.dup(target), 'wb') as stream, wave.open(stream, 'wb') as audio:
            audio.setnchannels(1)
            audio.setsampwidth(2)
            audio.setframerate(16000)
            audio.writeframes(b'\x00' * 32000)
        return SimpleNamespace(returncode=0)

    def prepare_fake(self):
        if self.ffmpeg is None:
            self.skipTest('FFmpeg not installed')
        with mock.patch.object(media.subprocess, 'run', side_effect=self.fake_decoder):
            return media.prepare(self.recording, self.folder, self.ffmpeg)

    def completion(self, audio):
        transcript = self.folder / 'transcript.json'
        raw = self.folder / 'raw-result.json'
        io.put(transcript, {'kind': 'test_cloud_transcript', 'text': 'hello'})
        io.put(raw, {'id': 'job', 'text': 'hello'})
        return {'audio': audio, 'transcript': io.binding(transcript), 'raw_result': io.binding(raw)}

    def test_inspect_valid_wav_records_exact_frames_digest_and_format(self):
        output = self.folder / 'audio.wav'
        self.write_wav(output)
        result = media.inspect_wav(output, 1000)
        self.assertEqual(result['duration_ms'], 1000)
        self.assertEqual(result['frames'], 16000)
        self.assertEqual(result['byte_count'], output.stat().st_size)
        self.assertEqual(result['sha256'], hashlib.sha256(output.read_bytes()).hexdigest())
        self.assertEqual((result['sample_rate_hz'], result['channels'], result['sample_width_bytes']), (16000, 1, 2))

    def test_real_ffmpeg_entire_synthetic_stereo_wav_converted_without_source_change(self):
        if self.ffmpeg is None:
            self.skipTest('FFmpeg not installed')
        audio = media.prepare(self.recording, self.folder, self.ffmpeg)
        self.assertEqual(audio['duration_ms'], 1000)
        self.assertEqual(audio['frames'], 16000)
        self.assertEqual(self.source.read_bytes(), self.original)
        receipt = io.read(io.binding(self.folder / 'audio.json'))
        self.assertEqual(receipt['audio'], audio)
        self.assertTrue(receipt['whole_recording'])
        self.assertFalse(receipt['cuts'])
        self.assertFalse(receipt['speech_filter'])
        with mock.patch.object(media.subprocess, 'run') as run:
            self.assertEqual(media.prepare(self.recording, self.folder, self.ffmpeg), audio)
        run.assert_not_called()

    def test_decoder_command_is_local_bounded_and_has_no_cuts_or_vad(self):
        if self.ffmpeg is None:
            self.skipTest('FFmpeg not installed')
        with mock.patch.object(media.subprocess, 'run', side_effect=self.fake_decoder) as run:
            media.prepare(self.recording, self.folder, self.ffmpeg)
        command = run.call_args.args[0]
        options = run.call_args.kwargs
        self.assertTrue(command[0].startswith('/proc/self/fd/'))
        self.assertEqual(command[command.index('-protocol_whitelist') + 1], 'file,pipe')
        self.assertEqual(command[command.index('-map') + 1], '0:a:0')
        self.assertEqual(command[command.index('-map_metadata') + 1], '-1')
        for forbidden in ['-ss', '-to', '-t', '-af', '-filter_complex']:
            self.assertNotIn(forbidden, command)
        self.assertEqual(options['timeout'], 1800)
        self.assertEqual(options['stdin'], subprocess.DEVNULL)
        self.assertEqual(options['stderr'], subprocess.DEVNULL)
        self.assertEqual(options['stdout'], subprocess.DEVNULL)
        self.assertEqual(options['env'], {'PATH': '/usr/bin:/bin', 'LANG': 'C'})
        self.assertEqual(len(options['pass_fds']), 3)
        with mock.patch.object(media.resource, 'setrlimit') as limits, mock.patch.object(media.os, 'nice') as nice:
            options['preexec_fn']()
        self.assertEqual(limits.call_args_list[0].args, (media.resource.RLIMIT_FSIZE, (161536, 161536)))
        self.assertEqual(limits.call_args_list[1].args, (media.resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3)))
        self.assertEqual(limits.call_args_list[2].args, (media.resource.RLIMIT_CPU, (1800, 1800)))
        nice.assert_called_once_with(10)

    def test_changed_source_hash_and_changed_decoder_never_run(self):
        if self.ffmpeg is None:
            self.skipTest('FFmpeg not installed')
        for recording, ffmpeg in [({**self.recording, 'media': {**self.recording['media'], 'sha256': '0' * 64}}, self.ffmpeg),
                                  (self.recording, {**self.ffmpeg, 'sha256': '0' * 64})]:
            with mock.patch.object(media.subprocess, 'run') as run, self.assertRaises(media.MediaError):
                media.prepare(recording, self.folder, ffmpeg)
            run.assert_not_called()
            self.assertFalse((self.folder / 'audio.wav').exists())
        self.assertEqual(self.source.read_bytes(), self.original)

    def test_insufficient_disk_never_starts_or_creates_audio(self):
        with mock.patch.object(media.shutil, 'disk_usage', return_value=SimpleNamespace(free=1)), mock.patch.object(media.subprocess, 'run') as run, self.assertRaises(media.MediaError):
            media.prepare(self.recording, self.folder, self.ffmpeg)
        run.assert_not_called()
        self.assertFalse((self.folder / 'audio.wav').exists())

    def test_interrupted_unreceipted_output_is_preserved_not_overwritten(self):
        output = self.folder / 'audio.wav'
        output.write_bytes(b'interrupted audio')
        with mock.patch.object(media.subprocess, 'run') as run, self.assertRaises(media.MediaError):
            media.prepare(self.recording, self.folder, self.ffmpeg)
        self.assertEqual(output.read_bytes(), b'interrupted audio')
        run.assert_not_called()

    def test_timeout_failure_and_oserror_are_sanitized_and_preserve_source(self):
        if self.ffmpeg is None:
            self.skipTest('FFmpeg not installed')
        for index, error in enumerate([subprocess.TimeoutExpired('private-file', 1800), OSError('private-file')]):
            folder = self.root / ('error-' + str(index))
            folder.mkdir(mode=0o700)
            with mock.patch.object(media.subprocess, 'run', side_effect=error), self.assertRaises(media.MediaError) as raised:
                media.prepare(self.recording, folder, self.ffmpeg)
            self.assertNotIn('private-file', str(raised.exception))
            self.assertTrue((folder / 'audio.wav').exists())
            self.assertFalse((folder / 'audio.json').exists())
        self.assertEqual(self.source.read_bytes(), self.original)

    def test_nonzero_decoder_does_not_publish_receipt(self):
        if self.ffmpeg is None:
            self.skipTest('FFmpeg not installed')
        with mock.patch.object(media.subprocess, 'run', return_value=SimpleNamespace(returncode=1)), self.assertRaises(media.MediaError):
            media.prepare(self.recording, self.folder, self.ffmpeg)
        self.assertTrue((self.folder / 'audio.wav').exists())
        self.assertFalse((self.folder / 'audio.json').exists())
        self.assertEqual(self.source.read_bytes(), self.original)

    def test_prepare_detects_source_mutation_during_decode(self):
        if self.ffmpeg is None:
            self.skipTest('FFmpeg not installed')
        def mutate(command, **kwargs):
            result = self.fake_decoder(command, **kwargs)
            self.source.write_bytes(b'changed source during decode')
            return result
        with mock.patch.object(media.subprocess, 'run', side_effect=mutate), self.assertRaises(media.MediaError):
            media.prepare(self.recording, self.folder, self.ffmpeg)
        self.assertFalse((self.folder / 'audio.json').exists())

    def test_replay_rejects_different_source_different_ffmpeg_or_changed_audio(self):
        audio = self.prepare_fake()
        for recording, ffmpeg in [({**self.recording, 'media': {**self.recording['media'], 'sha256': '0' * 64}}, self.ffmpeg),
                                  (self.recording, {**self.ffmpeg, 'sha256': '0' * 64})]:
            with self.assertRaises(media.MediaError):
                media.prepare(recording, self.folder, ffmpeg)
        output = Path(audio['path'])
        raw = bytearray(output.read_bytes())
        raw[-1] = 1
        output.write_bytes(raw)
        with self.assertRaises(media.MediaError):
            media.prepare(self.recording, self.folder, self.ffmpeg)

    def test_inspect_rejects_truncation_incorrect_format_and_incomplete_duration(self):
        output = self.folder / 'audio.wav'
        self.write_wav(output)
        output.write_bytes(output.read_bytes()[:-2])
        with self.assertRaisesRegex(media.MediaError, 'truncated'):
            media.inspect_wav(output, 1000)
        for kwargs in [{'channels': 2}, {'rate': 8000}, {'width': 1}]:
            self.write_wav(output, **kwargs)
            with self.subTest(kwargs=kwargs), self.assertRaises(media.MediaError):
                media.inspect_wav(output, 1000)
        self.write_wav(output)
        with self.assertRaisesRegex(media.MediaError, 'duration differs'):
            media.inspect_wav(output, 5000)

    def test_invalid_durations_are_rejected_without_decode(self):
        for value in [True, 0, -1, 1.2, float('nan'), '1000', media.MAX_DURATION_MS + 1]:
            recording = {**self.recording, 'duration_ms': value}
            with self.subTest(value=value), mock.patch.object(media.subprocess, 'run') as run, self.assertRaises(media.MediaError):
                media.prepare(recording, self.folder, self.ffmpeg)
            run.assert_not_called()

    def test_prune_only_verified_completed_regenerable_audio(self):
        audio = self.prepare_fake()
        completion = self.completion(audio)
        unrelated = self.folder / 'unrelated.txt'
        unrelated.write_text('keep')
        self.assertEqual(media.prune_completed(self.folder, completion), audio['byte_count'])
        self.assertFalse(Path(audio['path']).exists())
        for retained in ['audio.json', 'transcript.json', 'raw-result.json', 'unrelated.txt']:
            self.assertTrue((self.folder / retained).exists())
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(media.prune_completed(self.folder, completion), 0)

    def test_prune_rejects_unbound_completion_or_missing_verified_result(self):
        audio = self.prepare_fake()
        completion = self.completion(audio)
        changed = copy.deepcopy(completion)
        changed['audio']['sha256'] = '0' * 64
        with self.assertRaises(media.MediaError):
            media.prune_completed(self.folder, changed)
        changed = copy.deepcopy(completion)
        changed['transcript']['sha256'] = '0' * 64
        with self.assertRaises(io.Error):
            media.prune_completed(self.folder, changed)
        self.assertTrue(Path(audio['path']).exists())

    def test_prune_rejects_changed_wav_and_symlink_without_deleting_either(self):
        audio = self.prepare_fake()
        completion = self.completion(audio)
        output = Path(audio['path'])
        output.write_bytes(b'changed')
        with self.assertRaises(media.MediaError):
            media.prune_completed(self.folder, completion)
        output.unlink()
        output.symlink_to(self.source)
        with self.assertRaises(OSError):
            media.prune_completed(self.folder, completion)
        self.assertTrue(output.is_symlink())
        self.assertEqual(self.source.read_bytes(), self.original)


if __name__ == '__main__':
    unittest.main()
