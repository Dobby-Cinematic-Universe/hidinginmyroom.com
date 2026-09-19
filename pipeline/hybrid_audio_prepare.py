"""Opt-in, CPU-only admission of one finished, hash-bound local recording.

Nothing here discovers sources, reads campaign state, uploads media, or deletes
originals. Each result is an independent private FLAC and recording-input
manifest. Linux retained descriptors pin both input and executable identities.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
import ctypes
from decimal import Decimal, InvalidOperation
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import time

try:
    from .salad_transcription_contract import canonical_bytes, load_recording_input, CloudContractError
except ImportError:
    from salad_transcription_contract import canonical_bytes, load_recording_input, CloudContractError


MAX_SOURCE_BYTES = 64 * 1024**3
MAX_AUDIO_BYTES = 4 * 1024**3
MAX_DURATION_SECONDS = 24 * 3600
FREE_SPACE_FLOOR_BYTES = 128 * 1024**2
MAX_JSON_BYTES = 1024**2
SAMPLE_RATE = 16000
FORMATS = "mov,matroska,avi,mp3,wav,flac,aac,ogg,mpegts,mpeg,asf,aiff"
NORMALIZATION = {
    "version": 1, "codec": "flac", "sample_format": "s16", "sample_rate_hz": SAMPLE_RATE,
    "channels": 1, "audio_stream": "0:a:0", "max_duration_seconds": MAX_DURATION_SECONDS,
    "cpu_only": True, "protocol_whitelist": "file,pipe", "format_whitelist": FORMATS,
}
_SHA = re.compile(r"^[0-9a-f]{64}$")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")


class AudioPreparationError(RuntimeError):
    """A local preparation failed; no source mutation is authorized."""


def _path(value):
    if not isinstance(value, (str, Path)):
        raise AudioPreparationError("a normalized absolute local path is required")
    raw = str(value)
    path = Path(raw)
    if (not path.is_absolute() or raw != str(path) or raw == "/" or len(raw.encode()) > 4096
            or any(ord(char) < 32 for char in raw) or ".." in path.parts):
        raise AudioPreparationError("a normalized absolute local path is required")
    return path


def _witness(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns,
            info.st_uid, info.st_mode, info.st_nlink)


@contextmanager
def _file(path, *, executable=False):
    """Open all path components without following links; retain the leaf FD."""
    descriptor = directory = None
    try:
        directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                             dir_fd=directory)
        info = os.fstat(descriptor)
        owners = {0, os.getuid()} if executable else {os.getuid()}
        if (not stat.S_ISREG(info.st_mode) or info.st_uid not in owners or info.st_nlink != 1
                or info.st_mode & 0o022 or (executable and not info.st_mode & 0o111)):
            raise AudioPreparationError("input must be a safe owned regular file")
        yield descriptor
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise AudioPreparationError("audio preparation exceeded its total time limit")
    return remaining


def _hash_fd(descriptor, deadline, maximum):
    before = os.fstat(descriptor)
    if not 0 < before.st_size <= maximum:
        raise AudioPreparationError("file exceeds the bounded nonempty input size")
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest, count = hashlib.sha256(), 0
    while True:
        _remaining(deadline)
        body = os.read(descriptor, min(1024**2, maximum + 1 - count))
        if not body:
            break
        digest.update(body)
        count += len(body)
        if count > maximum:
            raise AudioPreparationError("file grew beyond its size limit")
    if _witness(before) != _witness(os.fstat(descriptor)) or count != before.st_size:
        raise AudioPreparationError("file changed while being verified")
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.hexdigest(), count, _witness(before)


def _private_root(path):
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            try:
                os.mkdir(part, 0o700, dir_fd=descriptor)
                os.fsync(descriptor)
            except FileExistsError:
                pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        info = os.fstat(descriptor)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise AudioPreparationError("output root must be an owned private directory")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _decode_json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise AudioPreparationError("JSON contains duplicate fields")
            result[key] = value
        return result

    def nonfinite(_value):
        raise AudioPreparationError("JSON contains nonfinite values")

    value = json.loads(body.decode("utf-8"), object_pairs_hook=pairs, parse_constant=nonfinite)
    if not isinstance(value, dict):
        raise AudioPreparationError("JSON document must be an object")
    return value


def _read_json(path, deadline):
    with _file(path) as descriptor:
        digest, count, witness = _hash_fd(descriptor, deadline, MAX_JSON_BYTES)
        body = os.read(descriptor, count + 1)
        if (len(body) != count or hashlib.sha256(body).hexdigest() != digest
                or _witness(os.fstat(descriptor)) != witness):
            raise AudioPreparationError("JSON changed while being read")
        return _decode_json(body), digest


def _write_json(path, value):
    body = canonical_bytes(value)
    with path.open("xb") as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(body)
        handle.flush()
        os.fchmod(handle.fileno(), 0o400)
        os.fsync(handle.fileno())
    return hashlib.sha256(body).hexdigest()


def _run(command, descriptors, deadline, *, capture=False):
    _remaining(deadline)
    result = subprocess.run(command, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, pass_fds=tuple(descriptors),
                            timeout=_remaining(deadline), check=False,
                            env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"})
    if result.returncode != 0:
        raise AudioPreparationError("pinned local media tool failed")
    return result.stdout if capture else None


def _probe(media_fd, probe_fd, deadline, *, lease_fds=()):
    raw = _run([
        f"/proc/self/fd/{probe_fd}", "-v", "error", "-protocol_whitelist", "file,pipe",
        "-format_whitelist", FORMATS, "-select_streams", "a:0", "-show_entries",
        "stream=codec_name,sample_fmt,sample_rate,channels,duration,duration_ts,time_base,bits_per_raw_sample:format=duration",
        "-of", "json", f"/proc/self/fd/{media_fd}",
    ], [media_fd, probe_fd, *lease_fds], deadline, capture=True)
    if not raw or len(raw) > MAX_JSON_BYTES:
        raise AudioPreparationError("media probe returned an invalid bounded result")
    result = _decode_json(raw)
    if (not isinstance(result.get("streams"), list) or len(result["streams"]) != 1
            or not isinstance(result["streams"][0], dict)):
        raise AudioPreparationError("media must contain a readable first audio stream")
    return result


def _duration(probe):
    stream = probe["streams"][0]
    raw = stream.get("duration", probe.get("format", {}).get("duration"))
    try:
        duration = Decimal(str(raw))
    except InvalidOperation as error:
        raise AudioPreparationError("source audio duration is unavailable") from error
    if not duration.is_finite() or not 0 < duration <= MAX_DURATION_SECONDS:
        raise AudioPreparationError("source audio duration exceeds the 24-hour bound")
    return duration


def _normalized_metadata(probe):
    stream = probe["streams"][0]
    samples = stream.get("duration_ts")
    if (stream.get("codec_name") != "flac" or stream.get("sample_fmt") != "s16"
            or stream.get("sample_rate") != "16000" or stream.get("channels") != 1
            or stream.get("time_base") != "1/16000" or stream.get("bits_per_raw_sample") not in (16, "16")
            or isinstance(samples, bool) or not isinstance(samples, int)
            or not 0 < samples <= MAX_DURATION_SECONDS * SAMPLE_RATE):
        raise AudioPreparationError("normalized FLAC lacks exact bounded 16 kHz mono s16 samples")
    return {"sample_rate_hz": SAMPLE_RATE, "channels": 1, "total_samples": samples,
            "duration_ms": (samples * 1000 + SAMPLE_RATE // 2) // SAMPLE_RATE}


def _publish(root_fd, staging_name, destination_name):
    """Atomic Linux publication that cannot replace even an empty directory."""
    library = ctypes.CDLL(None, use_errno=True)
    try:
        rename = library.renameat2
    except AttributeError as error:
        raise AudioPreparationError("atomic no-replace publication is unavailable") from error
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(root_fd, os.fsencode(staging_name), root_fd, os.fsencode(destination_name), 1) != 0:
        code = ctypes.get_errno()
        if code == errno.EEXIST:
            raise AudioPreparationError("preparation output appeared before publication")
        raise OSError(code, "atomic result publication failed")


def _result(receipt, receipt_path, *, reused):
    return {"recording_input": receipt["recording_input"]["path"],
            "sha256": receipt["recording_input"]["sha256"],
            "preparation_id": receipt["preparation_id"], "recording_id": receipt["recording_id"],
            "media_id": receipt["source"]["media_id"], "audio": receipt["audio"],
            "receipt": str(receipt_path),
            "receipt_sha256": hashlib.sha256(canonical_bytes(receipt)).hexdigest(), "reused": reused}


def _reuse(destination, binding, preparation_id, probe_fd, deadline, *, lease_fds=()):
    info = destination.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise AudioPreparationError("existing preparation is not an owned private directory")
    receipt, _ = _read_json(destination / "receipt.json", deadline)
    if (set(receipt) != {"kind", "schema_version", "preparation_id", "binding", "source", "recording_id", "audio", "recording_input"}
            or receipt["kind"] != "himr_hybrid_audio_preparation_receipt" or receipt["schema_version"] != 1
            or receipt["binding"] != binding or receipt["source"] != binding["source"]
            or receipt["preparation_id"] != preparation_id
            or receipt["recording_id"] != "hybridrec_" + preparation_id.removeprefix("hybridaudio_")):
        raise AudioPreparationError("existing preparation receipt has incompatible source or tools")
    with _file(destination / "audio.flac") as descriptor:
        digest, count, witness = _hash_fd(descriptor, deadline, MAX_AUDIO_BYTES - 1)
        metadata = _normalized_metadata(_probe(descriptor, probe_fd, deadline, lease_fds=lease_fds))
        if _witness(os.fstat(descriptor)) != witness:
            raise AudioPreparationError("existing normalized audio changed during verification")
    expected_audio = {"path": str(destination / "audio.flac"), "sha256": digest, "byte_count": count, **metadata}
    if receipt["audio"] != expected_audio:
        raise AudioPreparationError("existing normalized audio differs from its receipt")
    manifest_path = destination / "recording-input.json"
    _, digest = _read_json(manifest_path, deadline)
    if receipt["recording_input"] != {"path": str(manifest_path), "sha256": digest}:
        raise AudioPreparationError("existing recording manifest differs from its receipt")
    recording = load_recording_input(manifest_path, digest)
    if (recording["recording_id"] != receipt["recording_id"] or recording["media_id"] != binding["source"]["media_id"]
            or recording["audio"] != {key: value for key, value in expected_audio.items() if key != "channels"}):
        raise AudioPreparationError("existing recording manifest has incompatible audio or identity")
    return _result(receipt, destination / "receipt.json", reused=True)


def prepare_audio(source: dict, *, output_root: Path, ffmpeg: dict, ffprobe: dict,
                  timeout_seconds: int = 7200, lease_fds: tuple[int, ...] = ()) -> dict:
    """Normalize only the supplied local input and admit a sealed private result.

    Input limit: 64 GiB and 24 hours. Output limit: less than 4 GiB. Requires
    4 GiB plus 128 MiB free before encoding. The integer timeout (1..7200 s)
    covers hashing, source/output probing and encoding together. Existing
    incompatible results are never overwritten; orphan staging is never reused.
    """
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= 7200:
        raise AudioPreparationError("timeout must be an integer from 1 to 7200 seconds")
    if not isinstance(lease_fds, tuple) or any(isinstance(fd, bool) or not isinstance(fd, int) or fd < 0 for fd in lease_fds):
        raise AudioPreparationError("inherited leases must be a tuple of open descriptors")
    try:
        for descriptor in lease_fds:
            os.fstat(descriptor)
    except OSError as error:
        raise AudioPreparationError("inherited lease is not an open descriptor") from error
    deadline = time.monotonic() + timeout_seconds
    if (not isinstance(source, dict) or set(source) != {"path", "sha256", "byte_count", "media_id"}
            or not isinstance(source.get("sha256"), str) or not _SHA.fullmatch(source["sha256"])
            or not isinstance(source.get("media_id"), str) or not _ID.fullmatch(source["media_id"])
            or isinstance(source.get("byte_count"), bool) or not isinstance(source.get("byte_count"), int)
            or not 0 < source["byte_count"] <= MAX_SOURCE_BYTES):
        raise AudioPreparationError("source requires an exact bounded local file binding")
    source = dict(source)
    source_path = _path(source["path"])
    source["path"] = str(source_path)
    tool_bindings = {}
    for name, value in (("ffmpeg", ffmpeg), ("ffprobe", ffprobe)):
        if (not isinstance(value, dict) or set(value) != {"path", "sha256"}
                or not isinstance(value.get("sha256"), str) or not _SHA.fullmatch(value["sha256"])):
            raise AudioPreparationError("media tools require exact local path and SHA-256 bindings")
        tool_bindings[name] = {"path": str(_path(value["path"])), "sha256": value["sha256"]}
    root = _path(output_root)
    protected = (source_path, *(Path(value["path"]) for value in tool_bindings.values()))
    if (len(root.parts) < 4 or root in {Path.cwd(), Path.home(), Path("/mnt/archive/HIMR")}
            or any(root == path.parent or root == path or root in path.parents for path in protected)):
        raise AudioPreparationError("output root must be a separate dedicated private directory")
    binding = {"source": source, "tools": tool_bindings, "normalization": NORMALIZATION}
    preparation_id = "hybridaudio_" + hashlib.sha256(canonical_bytes(binding)).hexdigest()[:32]
    destination = root / preparation_id
    staging = None
    root_fd = lock_fd = output_fd = None
    try:
        with ExitStack() as stack:
            source_fd = stack.enter_context(_file(source_path))
            source_sha, source_size, source_witness = _hash_fd(source_fd, deadline, MAX_SOURCE_BYTES)
            if (source_sha, source_size) != (source["sha256"], source["byte_count"]):
                raise AudioPreparationError("source content does not match its supplied binding")
            descriptors, witnesses = {}, {source_fd: source_witness}
            for name, value in tool_bindings.items():
                descriptor = stack.enter_context(_file(Path(value["path"]), executable=True))
                digest, _, witness = _hash_fd(descriptor, deadline, 1024**3)
                if digest != value["sha256"]:
                    raise AudioPreparationError("media executable differs from its pinned SHA-256")
                descriptors[name] = descriptor
                witnesses[descriptor] = witness
            root_fd = _private_root(root)
            lock_fd = os.open(".prepare.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                              0o600, dir_fd=root_fd)
            lock_info = os.fstat(lock_fd)
            if (not stat.S_ISREG(lock_info.st_mode) or lock_info.st_uid != os.getuid()
                    or lock_info.st_nlink != 1 or lock_info.st_mode & 0o077):
                raise AudioPreparationError("audio preparation lock is unsafe")
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise AudioPreparationError("audio preparation output is already locked") from error
            os.fsync(root_fd)
            child_leases = (*lease_fds, lock_fd)
            if destination.exists() or destination.is_symlink():
                result = _reuse(destination, binding, preparation_id, descriptors["ffprobe"], deadline,
                                lease_fds=child_leases)
                if any(_witness(os.fstat(fd)) != witness for fd, witness in witnesses.items()):
                    raise AudioPreparationError("source or media tool changed during result verification")
                return result
            source_duration = _duration(_probe(source_fd, descriptors["ffprobe"], deadline, lease_fds=child_leases))
            if shutil.disk_usage(root).free < MAX_AUDIO_BYTES + FREE_SPACE_FLOOR_BYTES:
                raise AudioPreparationError("insufficient bounded free space for audio preparation")
            staging = Path(tempfile.mkdtemp(prefix="." + preparation_id + "-", dir=root))
            output_fd = os.open(staging / "audio.flac", os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            _run([
                f"/proc/self/fd/{descriptors['ffmpeg']}", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                "-xerror", "-err_detect", "explode", "-protocol_whitelist", "file,pipe", "-format_whitelist", FORMATS,
                "-hwaccel", "none", "-threads", "1", "-i", f"/proc/self/fd/{source_fd}", "-map", "0:a:0",
                "-vn", "-sn", "-dn", "-map_metadata", "-1", "-ar", "16000", "-ac", "1", "-sample_fmt", "s16",
                "-c:a", "flac", "-compression_level", "5", "-threads", "1", "-t", str(MAX_DURATION_SECONDS + 1),
                "-fs", str(MAX_AUDIO_BYTES), "-f", "flac", f"/proc/self/fd/{output_fd}",
            ], [descriptors["ffmpeg"], source_fd, output_fd, *child_leases], deadline)
            os.fsync(output_fd)
            digest, count, audio_witness = _hash_fd(output_fd, deadline, MAX_AUDIO_BYTES - 1)
            metadata = _normalized_metadata(_probe(output_fd, descriptors["ffprobe"], deadline, lease_fds=child_leases))
            # Container durations are estimates, but a substantial shortfall is
            # evidence of truncation, not a safely finished recording.
            if Decimal(metadata["total_samples"]) / SAMPLE_RATE + 2 < source_duration:
                raise AudioPreparationError("normalized audio is shorter than the source duration")
            if (_witness(os.fstat(output_fd)) != audio_witness
                    or any(_witness(os.fstat(fd)) != witness for fd, witness in witnesses.items())):
                raise AudioPreparationError("source, output, or media tool changed during normalization")
            os.fchmod(output_fd, 0o400)
            os.fsync(output_fd)
            audio = {"path": str(destination / "audio.flac"), "sha256": digest, "byte_count": count, **metadata}
            recording_id = "hybridrec_" + preparation_id.removeprefix("hybridaudio_")
            manifest = {"kind": "himr_longform_recording_input_manifest", "schema_version": 1,
                        "boundary_candidates": [], "recording": {"recording_id": recording_id,
                        "media_id": source["media_id"], "input": {**audio, "artifact_id": "hybrid_audio_" + digest[:32]}}}
            manifest_sha = _write_json(staging / "recording-input.json", manifest)
            load_recording_input(staging / "recording-input.json", manifest_sha)
            receipt = {"kind": "himr_hybrid_audio_preparation_receipt", "schema_version": 1,
                       "preparation_id": preparation_id, "binding": binding, "source": source,
                       "recording_id": recording_id, "audio": audio,
                       "recording_input": {"path": str(destination / "recording-input.json"), "sha256": manifest_sha}}
            _write_json(staging / "receipt.json", receipt)
            staging_fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(staging_fd)
            finally:
                os.close(staging_fd)
            _remaining(deadline)
            _publish(root_fd, staging.name, destination.name)
            staging = None
            os.fsync(root_fd)
            return _result(receipt, destination / "receipt.json", reused=False)
    except AudioPreparationError:
        raise
    except (OSError, ValueError, TypeError, KeyError, IndexError, RecursionError,
            CloudContractError, subprocess.SubprocessError) as error:
        raise AudioPreparationError("local audio preparation or receipt verification failed") from error
    finally:
        if output_fd is not None:
            os.close(output_fd)
        if staging is not None:
            for name in ("audio.flac", "recording-input.json", "receipt.json"):
                try:
                    (staging / name).unlink()
                except FileNotFoundError:
                    pass
            staging.rmdir()
        if lock_fd is not None:
            os.close(lock_fd)
        if root_fd is not None:
            os.close(root_fd)
