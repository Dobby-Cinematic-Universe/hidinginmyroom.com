"""Lossless transport copies and durable per-record unbillable-upload backoff.

Original PCM preparation, paid intents and normalized transcript contracts stay
unchanged. A FLAC transport copy must decode to exactly the original PCM samples.
"""
import hashlib
import math
import os
from pathlib import Path
import subprocess
import time
import wave

from pipeline import transcript_summary as io
from pipeline import cloud_transcription_client as clients
from pipeline import cloud_transcription_media as media

MAX_FAILURES = 128


def transient(error):
    return (isinstance(error, clients.CloudClientError) and error.response is None and
            ((str(error) == 'cloud request transport failed' and (error.status_code is None or
              type(error.status_code) is int and (200 <= error.status_code <= 299 or error.status_code in {408,429} or 500 <= error.status_code <= 599))) or
             (str(error) in {'cloud request failed with an HTTP status', 'cloud request returned an unexpected HTTP status'}
              and type(error.status_code) is int and (error.status_code in {408,429} or 500 <= error.status_code <= 599))))


def failures(folder):
    root = Path(folder) / 'upload-backoff'
    if not io.safe.exists(root):
        return []
    with io.paths.retained_directory(root) as fd:
        names = sorted(os.listdir(fd))
    if len(names) > MAX_FAILURES or any(name != f'{index:04d}.json' for index,name in enumerate(names)):
        raise RuntimeError('upload backoff history is invalid')
    values = [io.read(io.binding(root/name)) for name in names]
    for index, value in enumerate(values):
        io.safe.exact(value, {'kind','attempt','retry_after_unix','status_code','paid_post_retried','job_folder','transport_error_type'}, 'upload backoff')
        if (value['kind'] != 'himr_unbillable_upload_backoff' or value['attempt'] != index+1
                or value['job_folder'] != str(Path(folder)) or value['paid_post_retried'] is not False
                or type(value['retry_after_unix']) not in {int,float} or not math.isfinite(value['retry_after_unix'])
                or value['retry_after_unix'] < 0):
            raise RuntimeError('upload backoff binding differs')
    return values


def ready(folder, now=None):
    history = failures(folder)
    return len(history) < MAX_FAILURES and (not history or history[-1]['retry_after_unix'] <= (time.time() if now is None else now))


def defer(folder, error, now=None):
    if not transient(error):
        raise error
    history = failures(folder)
    if len(history) >= MAX_FAILURES:
        raise RuntimeError('upload retry history exhausted; review required')
    delay = max(min(3600, 900 * 2 ** min(len(history), 2)), error.retry_after_seconds or 0)
    value = {'kind': 'himr_unbillable_upload_backoff', 'attempt': len(history)+1,
             'retry_after_unix': (time.time() if now is None else now) + delay,
             'status_code': error.status_code, 'paid_post_retried': False,
             'job_folder': str(Path(folder)),
             'transport_error_type': type(error.__context__).__name__ if error.__context__ else None}
    root = Path(folder) / 'upload-backoff'
    io.mkdir(root)
    io.put(root/f'{len(history):04d}.json', value)
    return value


def prepare(audio, folder, ffmpeg):
    folder = Path(folder)
    output, receipt = folder/'audio.flac', folder/'flac-transport.json'
    if io.safe.exists(receipt):
        saved = io.read(io.binding(receipt))
        if saved['audio'] != audio or saved['ffmpeg'] != ffmpeg:
            raise RuntimeError('FLAC transport belongs to different audio or encoder')
        with io.safe.opened(output) as fd:
            if io.safe.hash_fd(fd, clients.ASSEMBLYAI_MAX_UPLOAD_BYTES, time.monotonic()+180) != saved['transport']['sha256']:
                raise RuntimeError('FLAC transport bytes changed')
        return saved['transport']
    with io.safe.opened(audio['path']) as source, io.safe.opened(ffmpeg['path'], executable=True) as tool:
        before = io.safe.witness(source)
        if io.safe.hash_fd(source, media.MAX_AUDIO_BYTES, time.monotonic()+180) != audio['sha256']:
            raise RuntimeError('PCM input changed before FLAC encoding')
        if io.safe.hash_fd(tool, 128*1024**2, time.monotonic()+60) != ffmpeg['sha256']:
            raise RuntimeError('FLAC encoder changed')
        os.lseek(source, 0, os.SEEK_SET)
        with os.fdopen(os.dup(source), 'rb') as stream, wave.open(stream) as wav:
            if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate(), wav.getnframes()) != (1,2,16000,audio['frames']):
                raise RuntimeError('PCM format differs')
            pcm = hashlib.sha256()
            while block := wav.readframes(65536):
                pcm.update(block)
        os.lseek(source, 0, os.SEEK_SET)
        common = [f'/proc/self/fd/{tool}', '-nostdin', '-hide_banner', '-loglevel', 'error', '-xerror',
                  '-protocol_whitelist', 'file,pipe', '-threads', '1']
        if not io.safe.exists(output):
            with io.paths.retained_directory(folder) as directory:
                target = os.open('audio.flac', os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW, 0o600, dir_fd=directory)
                try:
                    subprocess.run(common + ['-i', f'/proc/self/fd/{source}', '-map', '0:a:0', '-vn',
                        '-map_metadata', '-1', '-c:a', 'flac', '-compression_level', '5', '-threads', '1',
                        '-f', 'flac', '-y', f'/proc/self/fd/{target}'], check=True, timeout=1800,
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        pass_fds=(source,tool,target), env={'PATH':'/usr/bin:/bin','LANG':'C'},
                        preexec_fn=lambda: media._limits(clients.ASSEMBLYAI_MAX_UPLOAD_BYTES))
                    os.fsync(target)
                finally:
                    os.close(target)
        # An interrupted encode is never overwritten. A complete orphan copy
        # may be adopted only after the same full lossless equivalence checks.
        with io.safe.opened(output) as encoded:
            info = io.safe.witness(encoded)
            header = os.read(encoded, 42)
            if len(header) != 42 or header[:4] != b'fLaC' or header[4] & 127 or header[5:8] != b'\x00\x00\x22':
                raise RuntimeError('FLAC stream info is invalid')
            packed = int.from_bytes(header[18:26], 'big')
            if (packed >> 44, ((packed >> 41)&7)+1, ((packed >> 36)&31)+1, packed & ((1<<36)-1)) != (16000,1,16,audio['frames']):
                raise RuntimeError('FLAC format or frame count differs from PCM')
            os.lseek(encoded, 0, os.SEEK_SET)
            decoded = subprocess.run(common + ['-i', f'/proc/self/fd/{encoded}', '-map', '0:a:0',
                '-c:a', 'pcm_s16le', '-f', 'hash', '-hash', 'sha256', 'pipe:1'], check=True, timeout=1800,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                pass_fds=(encoded,tool), env={'PATH':'/usr/bin:/bin','LANG':'C'},
                preexec_fn=lambda: media._limits(clients.ASSEMBLYAI_MAX_UPLOAD_BYTES))
            if decoded.stdout.decode().strip().lower() != 'sha256='+pcm.hexdigest():
                raise RuntimeError('FLAC did not preserve exact PCM samples')
            digest = io.safe.hash_fd(encoded, clients.ASSEMBLYAI_MAX_UPLOAD_BYTES, time.monotonic()+180)
            if info != io.safe.witness(encoded) or before != io.safe.witness(source):
                raise RuntimeError('audio changed during FLAC verification')
    transport = {'path': str(output), 'sha256': digest, 'byte_count': info['st_size']}
    io.put(receipt, {'audio': audio, 'ffmpeg': ffmpeg, 'transport': transport,
                     'pcm_sha256': pcm.hexdigest(), 'lossless_verified': True})
    return transport


def prune(folder, completion):
    folder = Path(folder)
    if not io.safe.exists(folder/'audio.flac'):
        return 0
    saved = io.read(io.binding(folder/'flac-transport.json'))
    if saved['audio'] != completion['audio'] or saved['transport']['path'] != str(folder/'audio.flac'):
        raise RuntimeError('refusing to prune unbound FLAC')
    io.read(completion['transcript'])
    io.read(completion['raw_result'])
    with io.paths.retained_directory(folder) as directory, io.safe.opened(folder/'audio.flac') as fd:
        if io.safe.hash_fd(fd, clients.ASSEMBLYAI_MAX_UPLOAD_BYTES, time.monotonic()+180) != saved['transport']['sha256']:
            raise RuntimeError('refusing to prune changed FLAC')
        size = os.fstat(fd).st_size
        os.unlink('audio.flac', dir_fd=directory)
        os.fsync(directory)
    return size
