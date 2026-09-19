"""Opt-in, bounded Salad transcription lane; no live campaign mutations.

Imports, planning, status and collection never access the network. Only the
explicitly authorized run-cycle/reconcile commands construct a cloud client.
Ambiguous POST outcomes are durable holds, never automatic paid retries.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.parse
import wave

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from .salad_transcription_client import CloudClientError, SaladClient
    from .salad_transcription_temp_upload import upload_temp_file
    from .salad_transcription_contract import (
        CloudContractError, assemble_recording, build_plan, canonical_bytes,
        load_recording_input, normalize_output, plan_chunks, validate_plan,
    )
except ImportError:
    from pipeline.salad_transcription_client import CloudClientError, SaladClient
    from pipeline.salad_transcription_temp_upload import upload_temp_file
    from pipeline.salad_transcription_contract import (
        CloudContractError, assemble_recording, build_plan, canonical_bytes,
        load_recording_input, normalize_output, plan_chunks, validate_plan,
    )


MAX_JSON_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 128 * 1024 * 1024
STATES = frozenset({"planned", "prepared", "uploaded", "submitting", "submission_unknown",
                    "pending", "running", "succeeded", "completed", "failed", "cancelled"})
ACTIVE = frozenset({"submitting", "submission_unknown", "pending", "running", "succeeded"})
PROVIDER_STATES = frozenset({"pending", "running", "succeeded", "failed", "cancelled"})


class CloudPipelineError(RuntimeError):
    pass


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _path(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute() or str(path) != str(value) or ".." in path.parts or str(path) == "/":
        raise CloudPipelineError("paths must be normalized absolute paths")
    return path


@contextmanager
def _open_file(path: Path, *, executable: bool = False):
    """Retain a no-follow file descriptor through verification and consumption."""
    path = _path(path)
    directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    descriptor = None
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
            raise CloudPipelineError("input must be a regular file without peer write access")
        if executable and (info.st_uid not in {0, os.getuid()} or not info.st_mode & 0o111):
            raise CloudPipelineError("FFmpeg must be an owned executable")
        yield descriptor
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory)


def _witness(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _hash_fd(descriptor: int) -> tuple[str, int]:
    before = os.fstat(descriptor)
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    count = 0
    while body := os.read(descriptor, 1024 * 1024):
        count += len(body)
        digest.update(body)
    if count != before.st_size or _witness(before) != _witness(os.fstat(descriptor)):
        raise CloudPipelineError("file changed while being verified")
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.hexdigest(), count


def file_sha256(path: Path) -> str:
    with _open_file(path) as descriptor:
        return _hash_fd(descriptor)[0]


def read_json(path: Path, expected_sha256: str | None = None) -> dict:
    with _open_file(path) as descriptor:
        before = os.fstat(descriptor)
        if not 0 < before.st_size <= MAX_JSON_BYTES:
            raise CloudPipelineError("JSON file exceeds the bounded size")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            body = handle.read(MAX_JSON_BYTES + 1)
        if len(body) != before.st_size or _witness(before) != _witness(os.fstat(descriptor)):
            raise CloudPipelineError("JSON file changed while being read")
    if expected_sha256 is not None and _sha(body) != expected_sha256:
        raise CloudPipelineError("JSON file SHA-256 differs from the supplied binding")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise CloudPipelineError("duplicate JSON field")
            result[key] = value
        return result

    def constant(_value):
        raise CloudPipelineError("nonfinite JSON value")

    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise CloudPipelineError("nonfinite JSON value")
        return result

    try:
        value = json.loads(body, object_pairs_hook=pairs, parse_constant=constant, parse_float=finite_float)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise CloudPipelineError("invalid JSON document") from error
    if not isinstance(value, dict):
        raise CloudPipelineError("expected a JSON object")
    return value


def _private_directory(path: Path, *, create: bool = True) -> Path:
    path = _path(path)
    # Validate each ancestor, including any newly created directory. Never chmod
    # an existing directory belonging to another workflow.
    current = Path("/")
    for part in path.parts[1:]:
        current /= part
        if create and not current.exists() and not current.is_symlink():
            current.mkdir(mode=0o700)
        info = current.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise CloudPipelineError("output path contains a symlink or non-directory")
    info = path.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise CloudPipelineError("cloud output directory must be owned and private (0700)")
    return path


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json(path: Path, value: dict, *, replace: bool = False) -> None:
    body = canonical_bytes(value)
    if len(body) > MAX_JSON_BYTES:
        raise CloudPipelineError("output JSON exceeds the bounded size")
    descriptor, temporary = tempfile.mkstemp(prefix=".salad-json-", dir=path.parent)
    staging = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        if replace:
            if path.is_symlink():
                raise CloudPipelineError("refusing to replace a symlink")
            os.replace(staging, path)
        else:
            try:
                os.link(staging, path, follow_symlinks=False)
            except FileExistsError:
                if canonical_bytes(read_json(path)) != body:
                    raise CloudPipelineError("immutable output differs from existing evidence") from None
        _sync_directory(path.parent)
    finally:
        staging.unlink(missing_ok=True)


def _validate_output_root(plan: dict) -> Path:
    root = _path(plan["output_root"])
    forbidden = {Path.cwd(), Path.home(), Path("/mnt/archive"), Path("/mnt/archive/HIMR")}
    if root in forbidden or len(root.parts) < 4:
        raise CloudPipelineError("choose a dedicated cloud output directory")
    for recording in plan["recordings"]:
        for source in (recording["manifest"]["path"], recording["audio"]["path"], plan["ffmpeg"]["path"]):
            parent = Path(source).parent
            if root == parent or root in parent.parents or parent in root.parents:
                raise CloudPipelineError("cloud output root must be separate from source directories")
    return root


def load_plan(path: Path, expected_sha256: str) -> dict:
    plan = validate_plan(read_json(path, expected_sha256))
    _validate_output_root(plan)
    for recording in plan["recordings"]:
        observed = load_recording_input(Path(recording["manifest"]["path"]), recording["manifest"]["sha256"])
        expected = {key: recording[key] for key in ("manifest", "recording_id", "media_id", "audio")}
        if canonical_bytes(observed) != canonical_bytes(expected):
            raise CloudPipelineError("plan recording differs from its manifest")
    return plan


class Workspace:
    def __init__(self, plan: dict, *, lease_fds: tuple[int, ...] = ()):
        # Caller-owned exclusion leases must stay open for this workspace's
        # lifetime. Children inherit them; this class never closes them.
        if not isinstance(lease_fds, tuple) or any(type(descriptor) is not int or descriptor < 0 for descriptor in lease_fds):
            raise CloudPipelineError("cloud lease descriptors must be a tuple of open regular files")
        self.lease_fds = tuple(dict.fromkeys(lease_fds))
        self._lease_witnesses = []
        try:
            for descriptor in self.lease_fds:
                info = os.fstat(descriptor)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_nlink != 1 or info.st_mode & 0o077):
                    raise CloudPipelineError("cloud lease descriptor is not an owned private regular file")
                self._lease_witnesses.append(_witness(info))
        except OSError as error:
            raise CloudPipelineError("cloud lease descriptor is not open") from error
        self.plan = validate_plan(plan)
        self.root = _validate_output_root(plan)
        self.chunks = {chunk["chunk_id"]: chunk for chunk in plan_chunks(plan)}
        self.recordings = {recording["recording_id"]: recording for recording in plan["recordings"]}
        self.state = None

    @contextmanager
    def locked(self, *, create: bool = True):
        _private_directory(self.root, create=create)
        descriptor = os.open(self.root / ".lock", (os.O_CREAT if create else 0) | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o077:
                raise CloudPipelineError("invalid cloud workspace lock")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CloudPipelineError("another command holds this cloud workspace") from None
            binding = self.root / "plan.json"
            if create:
                _write_json(binding, self.plan)
            elif canonical_bytes(read_json(binding)) != canonical_bytes(self.plan):
                raise CloudPipelineError("workspace belongs to a different plan")
            state_path = self.root / "state.json"
            if state_path.exists() or state_path.is_symlink():
                self.state = read_json(state_path)
            else:
                evidence = self.root / "chunks"
                if evidence.exists() and any(evidence.iterdir()):
                    raise CloudPipelineError("runtime state is missing but chunk evidence exists; restore state before continuing")
                self.state = {"kind": "himr_salad_runtime_state", "schema_version": 1,
                              "plan_id": self.plan["plan_id"], "chunks": {}}
            self._validate_state()
            if create and not state_path.exists():
                self.save()
            yield self
        finally:
            self.state = None
            os.close(descriptor)

    def _validate_state(self):
        state = self.state
        if (set(state) != {"kind", "schema_version", "plan_id", "chunks"}
                or state["kind"] != "himr_salad_runtime_state" or state["schema_version"] != 1
                or state["plan_id"] != self.plan["plan_id"] or not isinstance(state["chunks"], dict)
                or not set(state["chunks"]) <= set(self.chunks)):
            raise CloudPipelineError("runtime state differs from the sealed plan")
        seen = set()
        # Independent, immutable intent receipts prevent a rolled-back or
        # partially restored journal from authorizing a second paid POST.
        for chunk_id in self.chunks:
            intent_path = self.root / "chunks" / chunk_id / "submission-intent.json"
            if intent_path.exists() or intent_path.is_symlink():
                evidence = read_json(intent_path)
                row = state["chunks"].get(chunk_id, {})
                if row.get("status") not in ACTIVE | {"completed", "failed", "cancelled"}:
                    raise CloudPipelineError("submission evidence is ahead of runtime state; restore the journal before continuing")
                if evidence != {"plan_id": self.plan["plan_id"], "chunk_id": chunk_id,
                                "intent": row.get("intent"), "intent_sha256": row.get("intent_sha256")}:
                    raise CloudPipelineError("submission intent receipt differs from runtime state")
        for chunk_id, row in state["chunks"].items():
            if not isinstance(row, dict) or row.get("status") not in STATES:
                raise CloudPipelineError("invalid runtime chunk state")
            if row["status"] in ACTIVE | {"completed", "failed", "cancelled"}:
                self._validate_intent(chunk_id, row)
            if row.get("provider_job_id"):
                job_id = _job_id(row["provider_job_id"])
                if job_id in seen:
                    raise CloudPipelineError("provider job ID was assigned to multiple chunks")
                seen.add(job_id)
            elif row["status"] in {"pending", "running", "succeeded", "completed", "failed", "cancelled"}:
                raise CloudPipelineError("known provider state lacks its job ID")
            if row["status"] == "completed":
                for key in ("transcript_sha256", "provider_job_sha256", "provider_output_sha256"):
                    if not isinstance(row.get(key), str) or re.fullmatch(r"[0-9a-f]{64}", row[key]) is None:
                        raise CloudPipelineError("completed state lacks a valid evidence SHA-256")

    def row(self, chunk_id: str) -> dict:
        if chunk_id not in self.chunks:
            raise CloudPipelineError("chunk is not in the plan")
        return self.state["chunks"].setdefault(chunk_id, {"status": "planned"})

    def save(self):
        self._validate_state()
        _write_json(self.root / "state.json", self.state, replace=True)

    def directory(self, chunk_id: str) -> Path:
        self.row(chunk_id)
        return _private_directory(self.root / "chunks" / chunk_id)

    def _intent(self, chunk_id: str, url: str) -> dict:
        options = dict(self.plan["transcription_options"])
        if options["summarize"] == 0:
            del options["summarize"]  # Unsupported by Lite; omit when not requested.
        return {"input": {"url": url, "return_as_file": False, **options},
                "metadata": {"client_request_id": _sha(canonical_bytes({"plan": self.plan["plan_id"], "chunk": chunk_id}))}}

    def _validate_intent(self, chunk_id: str, row: dict):
        intent = row.get("intent")
        if not isinstance(intent, dict) or not isinstance(intent.get("input"), dict):
            raise CloudPipelineError("submission state lacks a durable request intent")
        url = intent["input"].get("url")
        if not isinstance(url, str) or canonical_bytes(intent) != canonical_bytes(self._intent(chunk_id, url)):
            raise CloudPipelineError("submission intent differs from the plan")
        self._validate_upload_url(chunk_id, url)
        if row.get("intent_sha256") != _sha(canonical_bytes(intent)):
            raise CloudPipelineError("submission intent hash differs")

    def _validate_upload_url(self, chunk_id: str, url: str) -> None:
        parsed = urllib.parse.urlsplit(url)
        origin = "storage-api.salad.com" if self.chunks[chunk_id]["upload_provider"] == "s4" else "temp.sh"
        if parsed.scheme != "https" or parsed.netloc != origin or parsed.fragment or not parsed.path.startswith("/"):
            raise CloudPipelineError("audio download URL differs from its planned upload destination")

    def summary(self) -> dict:
        counts = Counter(self.state["chunks"].get(chunk_id, {"status": "planned"})["status"] for chunk_id in self.chunks)
        held = [chunk_id for chunk_id in self.chunks if self.state["chunks"].get(chunk_id, {}).get("status") in {"submitting", "submission_unknown"}]
        return {"plan_id": self.plan["plan_id"], "recordings": len(self.recordings),
                "chunks": len(self.chunks), "states": dict(sorted(counts.items())),
                "held_for_reconciliation": held, "estimate": self.plan["estimate"],
                "transcription_options": self.plan["transcription_options"],
                "planned_upload_destinations": dict(Counter(chunk["upload_provider"] for chunk in self.chunks.values())),
                "local_campaign_changed": False}

    def prepare(self, chunk_id: str) -> dict:
        row = self.row(chunk_id)
        directory = self.directory(chunk_id)
        receipt_path = directory / "prepared.json"
        chunk = self.chunks[chunk_id]
        if receipt_path.exists() or receipt_path.is_symlink():
            receipt = read_json(receipt_path)
            if (receipt.get("plan_id") != self.plan["plan_id"] or receipt.get("chunk_id") != chunk_id
                    or receipt.get("path") != str(directory / "audio.wav")):
                raise CloudPipelineError("prepared audio receipt differs from plan")
            self._verify_wav(Path(receipt["path"]), chunk, receipt)
        else:
            if row["status"] not in {"planned", "prepared"}:
                raise CloudPipelineError("submitted chunk lost its prepared audio receipt")
            recording = self.recordings[chunk["recording_id"]]
            bound = load_recording_input(Path(recording["manifest"]["path"]), recording["manifest"]["sha256"])
            if bound != {key: recording[key] for key in ("manifest", "recording_id", "media_id", "audio")}:
                raise CloudPipelineError("source manifest no longer matches plan")
            if shutil.disk_usage(directory).free < MIN_FREE_BYTES + chunk["wav_max_bytes"]:
                raise CloudPipelineError("insufficient free space for bounded cloud staging")
            receipt = self._render_wav(directory, recording, chunk)
            _write_json(receipt_path, receipt)
        if row["status"] == "planned":
            row["status"] = "prepared"
            self.save()
        return receipt

    def _verify_wav(self, path: Path, chunk: dict, receipt: dict | None = None) -> dict:
        if path.name == "audio.wav":
            self._recover_wav_links(path)
        with _open_file(path) as descriptor:
            before = _witness(os.fstat(descriptor))
            digest, size = _hash_fd(descriptor)
            if size > chunk["wav_max_bytes"]:
                raise CloudPipelineError("prepared WAV exceeds the planned upload size")
            with os.fdopen(os.dup(descriptor), "rb") as handle:
                with wave.open(handle, "rb") as wav:
                    if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate(), wav.getnframes()) != (
                        1, 2, 16000, chunk["analysis"]["end_sample"] - chunk["analysis"]["start_sample"]
                    ):
                        raise CloudPipelineError("prepared WAV does not match exact planned samples")
                    expected_bytes = (chunk["analysis"]["end_sample"] - chunk["analysis"]["start_sample"]) * 2
                    observed_bytes = 0
                    while pcm := wav.readframes(512 * 1024):
                        observed_bytes += len(pcm)
                        if observed_bytes > expected_bytes:
                            raise CloudPipelineError("prepared WAV contains excess PCM samples")
                    if observed_bytes != expected_bytes:
                        raise CloudPipelineError("prepared WAV PCM data is truncated")
            if before != _witness(os.fstat(descriptor)):
                raise CloudPipelineError("prepared WAV changed during validation")
            if receipt is not None and (receipt.get("sha256"), receipt.get("byte_count")) != (digest, size):
                raise CloudPipelineError("prepared WAV digest differs from its receipt")
        return {"sha256": digest, "byte_count": size}

    def _recover_wav_links(self, path: Path) -> None:
        """Recover only our same-inode temporary publication links after a crash."""
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise CloudPipelineError("prepared WAV is not an owned regular file")
        if info.st_nlink > 1:
            for candidate in path.parent.iterdir():
                if not candidate.name.startswith(".salad-wav-"):
                    continue
                observed = candidate.lstat()
                if (stat.S_ISREG(observed.st_mode) and observed.st_uid == os.getuid()
                        and (observed.st_dev, observed.st_ino) == (info.st_dev, info.st_ino)):
                    candidate.unlink()
            _sync_directory(path.parent)
        if path.stat().st_nlink != 1:
            raise CloudPipelineError("prepared WAV has an unexpected external hardlink")

    def _render_wav(self, directory: Path, recording: dict, chunk: dict) -> dict:
        try:
            if self._lease_witnesses != [_witness(os.fstat(descriptor)) for descriptor in self.lease_fds]:
                raise CloudPipelineError("cloud exclusion lease changed before media preparation")
        except OSError as error:
            raise CloudPipelineError("cloud exclusion lease closed before media preparation") from error
        descriptor, temporary = tempfile.mkstemp(prefix=".salad-wav-", dir=directory)
        staging = Path(temporary)
        try:
            with _open_file(Path(recording["audio"]["path"])) as source, _open_file(Path(self.plan["ffmpeg"]["path"]), executable=True) as executable:
                if _hash_fd(source) != (recording["audio"]["sha256"], recording["audio"]["byte_count"]):
                    raise CloudPipelineError("selected source audio hash or size differs")
                if _hash_fd(executable)[0] != self.plan["ffmpeg"]["sha256"]:
                    raise CloudPipelineError("FFmpeg changed since planning")
                witnesses = (_witness(os.fstat(source)), _witness(os.fstat(executable)))
                analysis = chunk["analysis"]
                command = [f"/proc/self/fd/{executable}", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                           "-protocol_whitelist", "file,pipe", "-f", "flac", "-i", f"/proc/self/fd/{source}",
                           "-map", "0:a:0", "-vn", "-af",
                           f"atrim=start_sample={analysis['start_sample']}:end_sample={analysis['end_sample']},asetpts=PTS-STARTPTS",
                           "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", "-map_metadata", "-1", "-f", "wav",
                           f"/proc/self/fd/{descriptor}"]
                result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                        timeout=3600, check=False,
                                        pass_fds=tuple(dict.fromkeys((source, executable, descriptor, *self.lease_fds))))
                if result.returncode != 0:
                    raise CloudPipelineError("FFmpeg failed while preparing selected audio")
                if witnesses != (_witness(os.fstat(source)), _witness(os.fstat(executable))):
                    raise CloudPipelineError("source or FFmpeg changed during preparation")
            os.fsync(descriptor)
            witness = self._verify_wav(staging, chunk)
            destination = directory / "audio.wav"
            try:
                os.link(staging, destination, follow_symlinks=False)
            except FileExistsError:
                self._verify_wav(destination, chunk, witness)
            _sync_directory(directory)
            return {"kind": "himr_salad_prepared_audio", "schema_version": 1,
                    "plan_id": self.plan["plan_id"], "chunk_id": chunk["chunk_id"],
                    "path": str(destination), **witness}
        finally:
            os.close(descriptor)
            staging.unlink(missing_ok=True)

    def _validate_job(self, chunk_id: str, job: dict) -> None:
        row = self.row(chunk_id)
        self._validate_intent(chunk_id, row)
        job_id = _job_id(job.get("id"))
        if row.get("provider_job_id") not in {None, job_id}:
            raise CloudPipelineError("provider returned another job ID")
        for other_id, other in self.state["chunks"].items():
            if other_id != chunk_id and other.get("provider_job_id") == job_id:
                raise CloudPipelineError("provider job is already bound to another chunk")
        expected = {"organization_name": self.plan["provider"]["organization"],
                    "inference_endpoint_name": self.plan["provider"]["engine"],
                    "input": row["intent"]["input"], "metadata": row["intent"]["metadata"]}
        if any(canonical_bytes(job.get(key)) != canonical_bytes(value) for key, value in expected.items()):
            raise CloudPipelineError("provider job request or organization binding differs")
        if job.get("status") not in PROVIDER_STATES:
            raise CloudPipelineError("provider job has an unsupported status")

    def _accept_job(self, chunk_id: str, job: dict, client: SaladClient) -> None:
        self._validate_job(chunk_id, job)
        row = self.row(chunk_id)
        row.update(provider_job_id=job["id"], status=job["status"])
        self.save()  # Persist the provider ID before downloading/normalizing anything.
        if job["status"] != "succeeded":
            return
        directory = self.directory(chunk_id)
        raw_path = directory / "provider-job.json"
        output_path = directory / "provider-output.json"
        if raw_path.exists():
            job = read_json(raw_path)
            self._validate_job(chunk_id, job)
            if job["status"] != "succeeded":
                raise CloudPipelineError("cached terminal result is not successful")
        else:
            _write_json(raw_path, job)
        output = job.get("output")
        if isinstance(output, dict) and set(output) == {"url"}:
            # Re-download after an interrupted completion. A cached output alone
            # is not sufficient to establish its relationship to the provider job.
            output = client.download_output(output["url"])
        if not isinstance(output, dict):
            raise CloudPipelineError("successful job lacks a transcription result")
        _write_json(output_path, output)  # Existing bytes must match the provider.
        transcript = normalize_output(self.plan, self.chunks[chunk_id], job["id"], output)
        _write_json(directory / "transcript.json", transcript)
        row.update(status="completed", transcript_sha256=_sha(canonical_bytes(transcript)),
                   provider_job_sha256=file_sha256(raw_path), provider_output_sha256=file_sha256(output_path))
        self.save()

    def submit(self, chunk_id: str, client: SaladClient) -> None:
        row = self.row(chunk_id)
        if row["status"] not in {"planned", "prepared", "uploaded"}:
            raise CloudPipelineError("chunk cannot be resubmitted; reconcile existing intent")
        receipt = self.prepare(chunk_id)
        if row["status"] != "uploaded" or row.get("url_expires_at", 0) <= time.time() + 300:
            destination = self.chunks[chunk_id]["upload_provider"]
            if destination == "s4":
                url = client.upload_file(Path(receipt["path"]), f"himr/{self.plan['plan_id']}/{chunk_id}.wav", expires_seconds=259200,
                                         expected_sha256=receipt["sha256"], expected_byte_count=receipt["byte_count"])
            else:
                url = upload_temp_file(Path(receipt["path"]), expected_sha256=receipt["sha256"], expected_byte_count=receipt["byte_count"])
            self._validate_upload_url(chunk_id, url)
            row.update(status="uploaded", upload_provider=destination, upload_url=url, url_expires_at=time.time() + 259200)
            self.save()
        intent = self._intent(chunk_id, row["upload_url"])
        row.update(status="submitting", intent=intent, intent_sha256=_sha(canonical_bytes(intent)))
        _write_json(self.directory(chunk_id) / "submission-intent.json",
                    {"plan_id": self.plan["plan_id"], "chunk_id": chunk_id,
                     "intent": intent, "intent_sha256": row["intent_sha256"]})
        self.save()  # No POST may happen before this durable write.
        try:
            job = client.submit(self.plan["provider"]["engine"], intent)
            self._validate_job(chunk_id, job)
        except Exception:
            # Even definite rejection stays held until an operator inspects it.
            # Transport failures and process death can never justify another POST.
            row["status"] = "submission_unknown"
            self.save()
            raise
        self._accept_job(chunk_id, job, client)

    def reconcile(self, chunk_id: str, provider_job_id: str, client: SaladClient) -> None:
        row = self.row(chunk_id)
        if row["status"] not in {"submitting", "submission_unknown"}:
            raise CloudPipelineError("only ambiguous submission intents can be reconciled")
        job_id = _job_id(provider_job_id)
        job = client.get_job(self.plan["provider"]["engine"], job_id)
        if job.get("id") != job_id:
            raise CloudPipelineError("reconciliation response differs from requested job")
        self._accept_job(chunk_id, job, client)

    def collect(self) -> int:
        completed = 0
        for recording in self.plan["recordings"]:
            if not all(self.row(chunk["chunk_id"])["status"] == "completed" for chunk in recording["chunks"]):
                continue
            transcripts = []
            for chunk in recording["chunks"]:
                row = self.row(chunk["chunk_id"])
                directory = self.directory(chunk["chunk_id"])
                raw = read_json(directory / "provider-job.json", row["provider_job_sha256"])
                self._validate_job(chunk["chunk_id"], raw)
                output = read_json(directory / "provider-output.json", row["provider_output_sha256"])
                inline = raw.get("output")
                if raw["status"] != "succeeded" or (isinstance(inline, dict) and set(inline) != {"url"}
                                                     and canonical_bytes(inline) != canonical_bytes(output)):
                    raise CloudPipelineError("saved output differs from the successful provider job")
                value = read_json(directory / "transcript.json", row["transcript_sha256"])
                replay = normalize_output(self.plan, chunk, row["provider_job_id"], output)
                if canonical_bytes(value) != canonical_bytes(replay):
                    raise CloudPipelineError("transcript differs from its saved provider output")
                transcripts.append(value)
            result = assemble_recording(self.plan, recording["recording_id"], transcripts)
            directory = _private_directory(self.root / "recordings")
            # IDs may contain punctuation, so use a path derived only from SHA-256.
            _write_json(directory / (_sha(recording["recording_id"].encode()) + ".json"), result)
            completed += 1
        return completed

    def run_cycle(self, client: SaladClient, *, max_new_jobs: int = 1, max_inflight: int = 2, max_polls: int = 10) -> dict:
        if (any(isinstance(limit, bool) or not isinstance(limit, int) for limit in (max_new_jobs, max_inflight, max_polls))
                or not 0 <= max_new_jobs <= 100 or not 1 <= max_inflight <= 100 or not 1 <= max_polls <= 100):
            raise CloudPipelineError("cycle limits must be bounded (at most 100)")
        polled = 0
        for chunk_id in self.chunks:
            row = self.row(chunk_id)
            if row["status"] in {"pending", "running", "succeeded"} and polled < max_polls:
                raw_path = self.root / "chunks" / chunk_id / "provider-job.json"
                if row["status"] == "succeeded" and raw_path.exists():
                    job = read_json(raw_path)
                else:
                    job = client.get_job(self.plan["provider"]["engine"], row["provider_job_id"])
                self._accept_job(chunk_id, job, client)
                polled += 1
        submitted = 0
        held = any(self.row(chunk_id)["status"] in {"submitting", "submission_unknown"} for chunk_id in self.chunks)
        active = sum(self.row(chunk_id)["status"] in ACTIVE for chunk_id in self.chunks)
        if max_new_jobs and active < max_inflight and not held:
            endpoint = client.endpoint(self.plan["provider"]["engine"])
            # Retain the provider's current pricing information for review. The
            # sealed estimate remains operator-supplied, not an actual-price cap.
            _write_json(self.root / "last-endpoint-preflight.json", endpoint, replace=True)
            for chunk_id in self.chunks:
                if submitted >= max_new_jobs or active >= max_inflight:
                    break
                if self.row(chunk_id)["status"] in {"planned", "prepared", "uploaded"}:
                    self.submit(chunk_id, client)
                    submitted += 1
                    active += self.row(chunk_id)["status"] in ACTIVE
        assembled = self.collect()
        return {**self.summary(), "submitted_this_cycle": submitted, "polled_this_cycle": polled,
                "assembled_recordings": assembled}


def _job_id(value) -> str:
    try:
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise ValueError("UUID")
    except ValueError:
        raise CloudPipelineError("provider job ID must be a canonical UUID") from None
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="build a hash-bound, metadata-only cost/chunk plan")
    plan.add_argument("--selection", type=Path, required=True)
    plan.add_argument("--organization", required=True)
    plan.add_argument("--engine", choices=("transcribe", "transcription-lite"), default="transcribe")
    plan.add_argument("--output-root", type=Path, required=True)
    plan.add_argument("--ffmpeg", type=Path, required=True)
    plan.add_argument("--rate-usd-per-hour", required=True)
    plan.add_argument("--max-estimated-cost-usd", required=True)
    plan.add_argument("--chunk-seconds", type=int, default=9000,
                      help="prefer a whole-recording job; maximum 9000 seconds, with context allowance when splitting")
    plan.add_argument("--overlap-seconds", type=int, default=5)
    plan.add_argument("--diarization", choices=("none", "sentence", "word", "both"), default="none",
                      help="optional anonymous speaker labels, scoped separately to each cloud chunk")
    plan.add_argument("--summary-words", type=int, default=0,
                      help="per-chunk provider summary word limit, 0-2000; full transcribe engine only")
    plan.add_argument("--output", type=Path, required=True)
    for name in ("validate", "status", "prepare", "run-cycle", "reconcile", "collect"):
        command = commands.add_parser(name)
        command.add_argument("--plan", type=Path, required=True)
        command.add_argument("--plan-sha256", required=True)
        if name == "prepare":
            command.add_argument("--max-chunks", type=int, default=1)
        if name in {"run-cycle", "reconcile"}:
            command.add_argument("--allow-cloud", action="store_true", help="authorize external transfers and API use; run-cycle can spend money")
        if name == "run-cycle":
            command.add_argument("--max-new-jobs", type=int, default=1)
            command.add_argument("--max-inflight", type=int, default=2)
            command.add_argument("--max-polls", type=int, default=10)
        if name == "reconcile":
            command.add_argument("--chunk-id", required=True)
            command.add_argument("--provider-job-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            selection = read_json(_path(args.selection))
            if (selection.get("kind") != "himr_salad_input_selection" or selection.get("schema_version") != 1
                    or not isinstance(selection.get("recordings"), list)):
                raise CloudPipelineError("invalid cloud input selection")
            recordings = []
            for row in selection["recordings"]:
                if not isinstance(row, dict) or set(row) != {"recording_input", "sha256"}:
                    raise CloudPipelineError("selection rows need only recording_input and sha256")
                recordings.append(load_recording_input(_path(row["recording_input"]), row["sha256"]))
            plan = build_plan(recordings, organization=args.organization, engine=args.engine,
                              output_root=str(_path(args.output_root)),
                              ffmpeg={"path": str(_path(args.ffmpeg)), "sha256": file_sha256(args.ffmpeg)},
                              rate_usd_per_hour=args.rate_usd_per_hour,
                              max_estimated_cost_usd=args.max_estimated_cost_usd,
                              chunk_seconds=args.chunk_seconds, overlap_seconds=args.overlap_seconds,
                              diarization=args.diarization in {"word", "both"},
                              sentence_diarization=args.diarization in {"sentence", "both"},
                              summary_words=args.summary_words)
            _validate_output_root(plan)
            output = _path(args.output)
            if not output.parent.is_dir() or output.parent.resolve(strict=True) != output.parent:
                raise CloudPipelineError("plan output parent must already exist without symlinks")
            _write_json(output, plan)
            result = {"plan_id": plan["plan_id"], "plan_sha256": _sha(canonical_bytes(plan)),
                      "recordings": len(recordings), "chunks": len(plan_chunks(plan)), "estimate": plan["estimate"],
                      "transcription_options": plan["transcription_options"],
                      "planned_upload_destinations": dict(Counter(chunk["upload_provider"] for chunk in plan_chunks(plan)))}
        else:
            if args.command in {"run-cycle", "reconcile"} and not args.allow_cloud:
                raise CloudPipelineError("cloud access requires explicit --allow-cloud; no request was sent")
            plan = load_plan(_path(args.plan), args.plan_sha256)
            workspace = Workspace(plan)
            if args.command == "validate":
                result = {"valid": True, "plan_id": plan["plan_id"], "media_read": False, "cloud_access": False}
            elif args.command == "status" and not workspace.root.exists():
                result = {"plan_id": plan["plan_id"], "initialized": False, "chunks": len(workspace.chunks), "states": {"planned": len(workspace.chunks)}}
            else:
                with workspace.locked(create=args.command != "status"):
                    if args.command == "prepare":
                        if not 1 <= args.max_chunks <= 100:
                            raise CloudPipelineError("max-chunks must be between 1 and 100")
                        count = 0
                        for chunk_id in workspace.chunks:
                            if workspace.row(chunk_id)["status"] == "planned" and count < args.max_chunks:
                                workspace.prepare(chunk_id)
                                count += 1
                        result = {**workspace.summary(), "prepared_this_command": count}
                    elif args.command == "run-cycle":
                        client = SaladClient(plan["provider"]["organization"])
                        result = workspace.run_cycle(client, max_new_jobs=args.max_new_jobs,
                                                     max_inflight=args.max_inflight, max_polls=args.max_polls)
                    elif args.command == "reconcile":
                        client = SaladClient(plan["provider"]["organization"])
                        workspace.reconcile(args.chunk_id, args.provider_job_id, client)
                        result = workspace.summary()
                    elif args.command == "collect":
                        result = {**workspace.summary(), "assembled_recordings": workspace.collect()}
                    else:
                        result = workspace.summary()
        print(json.dumps(result, sort_keys=True))
        return 0
    except CloudClientError as error:
        details = {"http_status": error.status_code, "retry_after_seconds": error.retry_after_seconds,
                   "request_outcome_ambiguous": error.ambiguous}
        print(f"SaladPipelineError: {error}; {json.dumps(details, sort_keys=True)}", file=sys.stderr)
        return 2
    except (CloudPipelineError, CloudContractError) as error:
        print(f"SaladPipelineError: {error}", file=sys.stderr)
        return 2
    except (OSError, ValueError, TypeError, KeyError, wave.Error, EOFError, subprocess.SubprocessError):
        # Raw third-party responses and signed URLs must never leak into a log.
        print("SaladPipelineError: local I/O, preparation, or document validation failed", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
