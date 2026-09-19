#!/usr/bin/env python3
"""Admit one full normalized FLAC into the isolated long-form ASR planner.

The adapter consumes an existing completed media-preprocess result.  It verifies the
bound audio bytes, retains both the audio and hash-pinned ``ffprobe`` descriptors
through probing, obtains the exact FLAC sample count from ``duration_ts`` at a
1/16000 time base, and optionally converts already-recorded silence intervals into
unscored boundary candidates.  It writes no media and never changes the active
autonomous controller or any producer artifact.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterator, Sequence


SCHEMA_VERSION = 1
MANIFEST_KIND = "himr_longform_recording_input_manifest"
SAMPLE_RATE_HZ = 16_000
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_BOUNDARIES = 1_000_000
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")


class LongformInputError(RuntimeError):
    pass


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    mode: int
    byte_count: int
    mtime_ns: int
    ctime_ns: int


@dataclass(frozen=True)
class _RetainedFile:
    path: Path
    label: str
    descriptor: int
    identity: _FileIdentity
    sha256: str

    @property
    def proc_path(self) -> str:
        return f"/proc/self/fd/{self.descriptor}"


def canonical_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise LongformInputError(f"value cannot be encoded as canonical JSON: {error}") from error


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LongformInputError(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def parse_json(body: bytes, label: str) -> Any:
    try:
        return json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(
                LongformInputError(f"{label} contains non-finite number {value}")
            ),
        )
    except LongformInputError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise LongformInputError(f"{label} is not strict UTF-8 JSON: {error}") from error


def stable_read(path: Path, label: str, maximum: int = MAX_JSON_BYTES) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise LongformInputError(f"cannot open {label}: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0 or before.st_size > maximum:
            raise LongformInputError(f"{label} is not a bounded regular file")
        body = bytearray()
        while len(body) <= maximum:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise LongformInputError(f"{label} changed while being read")
        if len(body) != before.st_size or len(body) > maximum:
            raise LongformInputError(f"{label} exceeds its read bound")
        return bytes(body)
    finally:
        os.close(descriptor)


def _file_identity(observed: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=observed.st_dev,
        inode=observed.st_ino,
        mode=observed.st_mode,
        byte_count=observed.st_size,
        mtime_ns=observed.st_mtime_ns,
        ctime_ns=observed.st_ctime_ns,
    )


def _descriptor_identity(descriptor: int, label: str) -> _FileIdentity:
    try:
        return _file_identity(os.fstat(descriptor))
    except OSError as error:
        raise LongformInputError(f"cannot inspect retained {label} descriptor: {error}") from error


def _path_identity(path: Path, label: str) -> _FileIdentity:
    try:
        observed = path.stat(follow_symlinks=False)
    except OSError as error:
        raise LongformInputError(f"cannot inspect retained {label} path: {error}") from error
    return _file_identity(observed)


def _hash_descriptor(
    descriptor: int,
    label: str,
    expected_identity: _FileIdentity,
) -> str:
    before = _descriptor_identity(descriptor, label)
    if before != expected_identity:
        raise LongformInputError(f"retained {label} descriptor identity changed")
    digest_value = hashlib.sha256()
    offset = 0
    while offset < expected_identity.byte_count:
        try:
            chunk = os.pread(
                descriptor,
                min(8 * 1024 * 1024, expected_identity.byte_count - offset),
                offset,
            )
        except InterruptedError:
            continue
        except OSError as error:
            raise LongformInputError(f"cannot hash retained {label}: {error}") from error
        if not chunk:
            break
        digest_value.update(chunk)
        offset += len(chunk)
    after = _descriptor_identity(descriptor, label)
    if after != expected_identity or offset != expected_identity.byte_count:
        raise LongformInputError(f"retained {label} changed while being hashed")
    return digest_value.hexdigest()


def _verify_path_binding(retained: _RetainedFile) -> None:
    if _path_identity(retained.path, retained.label) != retained.identity:
        raise LongformInputError(f"retained {retained.label} path identity changed")


def _verify_after_probe(retained: _RetainedFile) -> None:
    if _descriptor_identity(retained.descriptor, retained.label) != retained.identity:
        raise LongformInputError(f"retained {retained.label} descriptor identity changed after probe")
    _verify_path_binding(retained)
    if _hash_descriptor(retained.descriptor, retained.label, retained.identity) != retained.sha256:
        raise LongformInputError(f"retained {retained.label} content changed after probe")
    _verify_path_binding(retained)


@contextmanager
def _retain_file(
    path: Path,
    label: str,
    *,
    executable: bool = False,
) -> Iterator[_RetainedFile]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise LongformInputError(f"cannot open {label}: {error}") from error
    try:
        identity = _descriptor_identity(descriptor, label)
        if not stat.S_ISREG(identity.mode) or identity.byte_count <= 0:
            raise LongformInputError(f"{label} is not a non-empty regular file")
        if _path_identity(path, label) != identity:
            raise LongformInputError(f"{label} path does not bind its retained descriptor")
        proc_path = f"/proc/self/fd/{descriptor}"
        if not Path("/proc/self/fd").is_dir() or not Path(proc_path).exists():
            raise LongformInputError("/proc/self/fd is required for retained descriptor admission")
        if executable and not os.access(proc_path, os.X_OK, effective_ids=True):
            raise LongformInputError(f"{label} retained descriptor is not executable")
        retained = _RetainedFile(
            path=path,
            label=label,
            descriptor=descriptor,
            identity=identity,
            sha256=_hash_descriptor(descriptor, label, identity),
        )
        _verify_path_binding(retained)
        yield retained
    finally:
        os.close(descriptor)


def identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or IDENTIFIER_RE.fullmatch(value) is None:
        raise LongformInputError(f"{label} must be a bounded identifier")
    return value


def digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise LongformInputError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise LongformInputError(f"{label} must be an integer >= {minimum}")
    return value


def resolve_file(value: Any, base: Path, label: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value or "://" in value:
        raise LongformInputError(f"{label} must be a local path")
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = base / candidate
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise LongformInputError(f"{label} cannot be resolved: {error}") from error
    if resolved.is_symlink() or not resolved.is_file():
        raise LongformInputError(f"{label} must be a regular non-symlink file")
    return resolved


def _artifact(result: dict[str, Any], kind: str) -> dict[str, Any] | None:
    rows = result.get("artifacts")
    if not isinstance(rows, list):
        raise LongformInputError("preprocess result artifacts must be an array")
    matches = [row for row in rows if isinstance(row, dict) and row.get("artifact_kind") == kind]
    if len(matches) > 1:
        raise LongformInputError(f"preprocess result repeats {kind}")
    return matches[0] if matches else None


def _verify_probe_inputs(*retained_files: _RetainedFile) -> None:
    failures: list[str] = []
    for retained in retained_files:
        try:
            _verify_after_probe(retained)
        except LongformInputError as error:
            failures.append(str(error))
    if failures:
        raise LongformInputError("probe input verification failed: " + "; ".join(failures))


def exact_flac_samples(ffprobe: _RetainedFile, audio: _RetainedFile) -> int:
    command = [
        ffprobe.proc_path,
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_name,sample_rate,channels,time_base,duration_ts",
        "-of",
        "json",
        audio.proc_path,
    ]
    try:
        try:
            completed = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
                check=False,
                env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C", "TZ": "UTC"},
                pass_fds=(ffprobe.descriptor, audio.descriptor),
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise LongformInputError(f"ffprobe failed: {error}") from error
    finally:
        _verify_probe_inputs(ffprobe, audio)
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace")[-4096:]
        raise LongformInputError(f"ffprobe exited {completed.returncode}: {detail}")
    value = parse_json(completed.stdout, "ffprobe output")
    streams = value.get("streams") if isinstance(value, dict) else None
    if not isinstance(streams, list) or len(streams) != 1 or not isinstance(streams[0], dict):
        raise LongformInputError("normalized audio must expose exactly one selected audio stream")
    stream = streams[0]
    if (
        stream.get("codec_name") != "flac"
        or stream.get("sample_rate") != str(SAMPLE_RATE_HZ)
        or stream.get("channels") != 1
        or stream.get("time_base") != "1/16000"
    ):
        raise LongformInputError("normalized audio must be mono 16000 Hz FLAC with time_base 1/16000")
    return integer(stream.get("duration_ts"), "FLAC duration_ts", 1)


def silence_candidates(
    result: dict[str, Any],
    result_base: Path,
    total_samples: int,
) -> list[dict[str, Any]]:
    artifact = _artifact(result, "scene_silence_routing_json")
    if artifact is None:
        return []
    path = resolve_file(artifact.get("path"), result_base, "routing artifact")
    expected_hash = digest(artifact.get("sha256"), "routing artifact SHA-256")
    expected_bytes = integer(artifact.get("byte_count"), "routing artifact byte count", 1)
    body = stable_read(path, "routing artifact")
    if sha256_bytes(body) != expected_hash or len(body) != expected_bytes:
        raise LongformInputError("routing artifact bytes differ from the preprocess result")
    value = parse_json(body, "routing artifact")
    rows = value.get("silence_intervals") if isinstance(value, dict) else None
    if not isinstance(rows, list) or len(rows) > MAX_BOUNDARIES:
        raise LongformInputError("routing silence intervals are not a bounded array")
    candidates: dict[int, dict[str, Any]] = {}
    for ordinal, row in enumerate(rows):
        if not isinstance(row, dict):
            raise LongformInputError(f"silence interval {ordinal} is not an object")
        start_ms = integer(row.get("start_ms"), f"silence interval {ordinal} start")
        end_ms = integer(row.get("end_ms"), f"silence interval {ordinal} end")
        if end_ms <= start_ms:
            raise LongformInputError(f"silence interval {ordinal} is empty or inverted")
        # 16 samples/ms makes the half-millisecond midpoint exact as an integer sample.
        sample = (start_ms + end_ms) * 8
        if 0 < sample < total_samples:
            candidates.setdefault(
                sample,
                {
                    "confidence_millionths": 0,
                    "kind": "silence_midpoint",
                    "sample": sample,
                },
            )
    return [candidates[key] for key in sorted(candidates)]


def build_manifest(
    result_path: Path,
    *,
    recording_id: str,
    media_id: str | None,
    ffprobe: Path,
    expected_ffprobe_sha256: str,
    include_routing: bool,
) -> dict[str, Any]:
    expected_tool_hash = digest(expected_ffprobe_sha256, "ffprobe SHA-256")
    result_resolved = result_path.resolve(strict=True)
    result_body = stable_read(result_resolved, "preprocess result")
    result = parse_json(result_body, "preprocess result")
    if not isinstance(result, dict) or result.get("status") != "completed" or result.get("dry_run") is True:
        raise LongformInputError("preprocess result must be a completed non-dry-run object")
    artifact = _artifact(result, "audio_16khz_mono_flac")
    if artifact is None:
        raise LongformInputError("preprocess result has no normalized audio artifact")
    audio = resolve_file(artifact.get("path"), result_resolved.parent, "normalized audio")
    expected_audio_hash = digest(artifact.get("sha256"), "normalized audio SHA-256")
    expected_audio_bytes = integer(artifact.get("byte_count"), "normalized audio byte count", 1)
    try:
        ffprobe_resolved = ffprobe.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise LongformInputError(f"ffprobe cannot be resolved: {error}") from error

    with _retain_file(audio, "normalized audio") as retained_audio:
        if (
            retained_audio.sha256,
            retained_audio.identity.byte_count,
        ) != (expected_audio_hash, expected_audio_bytes):
            raise LongformInputError("normalized audio bytes differ from the preprocess result")
        with _retain_file(ffprobe_resolved, "ffprobe", executable=True) as retained_ffprobe:
            if retained_ffprobe.sha256 != expected_tool_hash:
                raise LongformInputError("ffprobe differs from its expected SHA-256")
            total_samples = exact_flac_samples(retained_ffprobe, retained_audio)
            observed_audio_hash = retained_audio.sha256
            observed_audio_bytes = retained_audio.identity.byte_count
    duration_ms = (total_samples * 1_000 + SAMPLE_RATE_HZ // 2) // SAMPLE_RATE_HZ

    source_input = result.get("input")
    inferred_media_id = source_input.get("media_id") if isinstance(source_input, dict) else None
    selected_media_id = media_id if media_id is not None else inferred_media_id
    selected_media_id = identifier(selected_media_id, "media ID")
    artifact_id = identifier(artifact.get("artifact_id"), "normalized audio artifact ID")
    manifest = {
        "boundary_candidates": (
            silence_candidates(result, result_resolved.parent, total_samples)
            if include_routing
            else []
        ),
        "kind": MANIFEST_KIND,
        "recording": {
            "input": {
                "artifact_id": artifact_id,
                "byte_count": observed_audio_bytes,
                "channels": 1,
                "duration_ms": duration_ms,
                "path": str(audio),
                "sample_rate_hz": SAMPLE_RATE_HZ,
                "sha256": observed_audio_hash,
                "total_samples": total_samples,
            },
            "media_id": selected_media_id,
            "recording_id": identifier(recording_id, "recording ID"),
        },
        "schema_version": SCHEMA_VERSION,
    }
    return manifest


def write_new(path: Path, body: bytes) -> None:
    if not path.is_absolute():
        raise LongformInputError("output path must be absolute")
    parent = path.parent.resolve(strict=True)
    target = parent / path.name
    try:
        descriptor = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
    except OSError as error:
        raise LongformInputError(f"cannot create output without replacement: {error}") from error
    try:
        view = memoryview(body)
        while view:
            count = os.write(descriptor, view)
            if count <= 0:
                raise LongformInputError("output write made no progress")
            view = view[count:]
        os.fsync(descriptor)
    except BaseException:
        try:
            target.unlink()
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a verified normalized-audio manifest for long-form ASR"
    )
    parser.add_argument("--preprocess-result", type=Path, required=True)
    parser.add_argument("--recording-id", required=True)
    parser.add_argument("--media-id")
    parser.add_argument("--ffprobe", type=Path, default=Path("/usr/bin/ffprobe"))
    parser.add_argument("--ffprobe-sha256", required=True)
    parser.add_argument("--without-routing-boundaries", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        manifest = build_manifest(
            arguments.preprocess_result,
            recording_id=arguments.recording_id,
            media_id=arguments.media_id,
            ffprobe=arguments.ffprobe,
            expected_ffprobe_sha256=arguments.ffprobe_sha256,
            include_routing=not arguments.without_routing_boundaries,
        )
        body = canonical_bytes(manifest)
        write_new(arguments.output, body)
        sys.stdout.buffer.write(
            canonical_bytes(
                {
                    "boundary_candidate_count": len(manifest["boundary_candidates"]),
                    "manifest_path": str(arguments.output),
                    "manifest_sha256": sha256_bytes(body),
                    "status": "completed",
                    "total_samples": manifest["recording"]["input"]["total_samples"],
                }
            )
        )
        return 0
    except (LongformInputError, OSError) as error:
        sys.stderr.buffer.write(
            canonical_bytes(
                {
                    "error": {"message": str(error), "type": type(error).__name__},
                    "status": "failed",
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
