"""Bounded whole-recording upload preparation; no network or source mutations."""
from __future__ import annotations

import math
import os
from pathlib import Path
import resource
import shutil
import subprocess
import time
import wave

from pipeline import transcript_summary as io

MAX_AUDIO_BYTES = 1_999_000_000
MAX_DURATION_MS = 17 * 3600 * 1000
FORMATS = 'mov,matroska,avi,mp3,wav,flac,aac,ogg,mpegts,mpeg,asf,aiff'


class MediaError(RuntimeError):
    pass


def tolerance_ms(duration_ms):
    if type(duration_ms) is not int or not 0 < duration_ms <= MAX_DURATION_MS:
        raise MediaError('whole recording duration must be a positive bounded integer')
    return max(2000, math.ceil(duration_ms / 1000))


def inspect_wav(path, expected_duration_ms):
    tolerance = tolerance_ms(expected_duration_ms)
    with io.safe.opened(path) as descriptor:
        before = io.safe.witness(descriptor)
        if not 44 < before['st_size'] <= MAX_AUDIO_BYTES:
            raise MediaError('prepared whole-recording audio exceeds its upload bound')
        with os.fdopen(os.dup(descriptor), 'rb') as stream, wave.open(stream, 'rb') as audio:
            if (audio.getnchannels(), audio.getsampwidth(), audio.getframerate(), audio.getcomptype()) != (1, 2, 16000, 'NONE'):
                raise MediaError('prepared audio format differs from mono 16 kHz PCM')
            frames = audio.getnframes()
            # Check that declared frames actually exist; do not trust a truncated header.
            count = 0
            while chunk := audio.readframes(65536):
                count += len(chunk)
            if count != frames * 2:
                raise MediaError('prepared WAV is truncated')
        duration_ms = (frames * 1000 + 8000) // 16000
        if not 0 < duration_ms <= MAX_DURATION_MS or abs(duration_ms - expected_duration_ms) > tolerance:
            raise MediaError('decoded audio duration differs from the whole recording; review required')
        sha = io.safe.hash_fd(descriptor, MAX_AUDIO_BYTES, time.monotonic() + 180)
        if io.safe.witness(descriptor) != before:
            raise MediaError('prepared audio changed while verified')
    return {'path': str(path), 'sha256': sha, 'byte_count': before['st_size'],
            'duration_ms': duration_ms, 'sample_rate_hz': 16000, 'channels': 1,
            'sample_width_bytes': 2, 'frames': frames}


def _limits(max_bytes):
    resource.setrlimit(resource.RLIMIT_FSIZE, (max_bytes, max_bytes))
    resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
    resource.setrlimit(resource.RLIMIT_CPU, (1800, 1800))
    os.nice(10)


def prepare(recording, folder, ffmpeg):
    """Verify this recording only, then decode once without cuts, VAD or prompts."""
    folder = Path(folder)
    tolerance_ms(recording['duration_ms'])
    output, receipt = folder / 'audio.wav', folder / 'audio.json'
    if io.safe.exists(receipt):
        saved = io.read(io.binding(receipt))
        if saved.get('source') != recording['media'] or saved.get('ffmpeg') != ffmpeg:
            raise MediaError('audio preparation receipt belongs to different inputs')
        current = inspect_wav(output, recording['duration_ms'])
        if current != saved['audio']:
            raise MediaError('prepared upload audio differs from its receipt')
        return saved['audio']
    if io.safe.exists(output):
        raise MediaError('unreceipted audio exists after interrupted preparation; review required')
    if not 0 < recording['duration_ms'] <= MAX_DURATION_MS:
        raise MediaError('whole recording exceeds supported provider duration')
    maximum = min(MAX_AUDIO_BYTES, math.ceil((recording['duration_ms'] + tolerance_ms(recording['duration_ms'])) * 32) + 65536)
    if shutil.disk_usage(folder).free < maximum + 256 * 1024**2:
        raise MediaError('insufficient private workspace space for one whole-recording upload')
    with io.paths.retained_directory(folder) as directory, io.safe.opened(recording['media']['path']) as source, io.safe.opened(ffmpeg['path'], executable=True) as tool:
        before = io.safe.witness(source)
        if before['st_size'] != recording['media']['byte_count'] or io.safe.hash_fd(source, before['st_size'], time.monotonic() + 900) != recording['media']['sha256']:
            raise MediaError('source media differs from its bound acquisition artifact')
        if io.safe.hash_fd(tool, 128 * 1024**2, time.monotonic() + 60) != ffmpeg['sha256']:
            raise MediaError('FFmpeg implementation changed')
        target = os.open(output.name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            command = [f'/proc/self/fd/{tool}', '-nostdin', '-hide_banner', '-loglevel', 'error', '-xerror',
                       '-protocol_whitelist', 'file,pipe', '-format_whitelist', FORMATS,
                       '-threads', '2', '-i', f'/proc/self/fd/{source}', '-map', '0:a:0',
                       '-vn', '-sn', '-dn', '-map_metadata', '-1', '-filter_threads', '1',
                       '-ac', '1', '-ar', '16000', '-c:a', 'pcm_s16le', '-f', 'wav',
                       '-y', f'/proc/self/fd/{target}']
            try:
                result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL, timeout=1800, pass_fds=(source, tool, target),
                                        env={'PATH': '/usr/bin:/bin', 'LANG': 'C'},
                                        preexec_fn=lambda: _limits(maximum))
            except (OSError, subprocess.SubprocessError):
                raise MediaError('whole-recording audio preparation interrupted or could not run; source preserved') from None
            os.fsync(target)
            if result.returncode or io.safe.witness(source) != before:
                raise MediaError('whole-recording audio preparation failed; source preserved')
        finally:
            os.close(target)
    audio = inspect_wav(output, recording['duration_ms'])
    io.put(receipt, {'kind': 'himr_cloud_prepared_audio', 'schema_version': 1,
                     'source': recording['media'], 'ffmpeg': ffmpeg, 'audio': audio,
                     'whole_recording': True, 'speech_filter': False, 'cuts': False})
    return audio


def prune_completed(folder, completion):
    """Remove only this lane's regenerable WAV after durable result collection."""
    folder = Path(folder)
    if not io.safe.exists(folder / 'audio.wav'):
        return 0
    saved = io.read(io.binding(folder / 'audio.json'))
    audio = saved['audio']
    if Path(audio['path']) != folder / 'audio.wav' or completion['audio'] != audio:
        raise MediaError('refusing to prune unbound upload audio')
    io.read(completion['transcript'])
    io.read(completion['raw_result'])
    with io.paths.retained_directory(folder) as directory, io.safe.opened(folder / 'audio.wav') as descriptor:
        info = io.safe.witness(descriptor)
        if info['st_size'] != audio['byte_count'] or io.safe.hash_fd(descriptor, MAX_AUDIO_BYTES, time.monotonic() + 180) != audio['sha256']:
            raise MediaError('refusing to prune changed upload audio')
        os.unlink('audio.wav', dir_fd=directory)
        os.fsync(directory)
    return audio['byte_count']
