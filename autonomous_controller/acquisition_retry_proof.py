"""Explicit, immutable recovery evidence for the SHA-pinned acquisition queue.

The legacy failure ledger is never changed.  A separately authorized successful
retry may replace its *active* quarantine projection, not its historical evidence.
Readers inspect metadata only; callers must earn completed states through the
existing exact result validator.  Only ``seal_completion`` writes a new proof.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
import weakref
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator
from urllib.parse import urlsplit


PLAN_KIND = "himr_acquisition_quarantine_retry_plan"
PROOF_KIND = "himr_acquisition_quarantine_recovery_completion"
DIRECTORY = ".queue-retry-recovery-v1"
MAX_PLAN_BYTES = 1024 * 1024
MAX_METADATA_BYTES = 256 * 1024
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
LEGACY_RUNNER_SHA256 = "99dc1311c7cbb960ede2db1fe5c12ee19944915d9cfc90f12f57ddbc1c2d832c"
LEGACY_ACQUIRE_SHA256 = "de7eaa811232aaeecc1bf53f94904aa21753dd8d7e2e35cc0b52033927732d47"
_BINDING_KEYS = {
    "bundle_id", "manifest_sha256", "ordinal", "job_id", "work_order_sha256",
    "quarantine_receipt_sha256",
}
_ENTRY_KEYS = _BINDING_KEYS | {"manifest_path", "result_path", "expected_bytes", "url"}
_PROOF_KEYS = _BINDING_KEYS | {
    "schema_version", "kind", "authorization_plan_path", "authorization_plan_sha256",
    "result_path", "result_sha256", "media_sha256", "media_byte_count", "completed_at",
    "receipt_sha256",
}
_INSTALL_LOCK = threading.Lock()
_INSTALLED: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


class RecoveryProofError(RuntimeError):
    """Retry authority or its completed-result binding could not be verified."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise RecoveryProofError("recovery metadata is not canonical JSON") from error


def pretty_bytes(value: Any) -> bytes:
    try:
        return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                           allow_nan=False) + "\n").encode("utf-8")
    except (TypeError, ValueError) as error:
        raise RecoveryProofError("recovery metadata is not JSON") from error


def _hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _sha(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise RecoveryProofError("recovery SHA-256 is invalid")
    return value


def _integer(value: Any, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise RecoveryProofError("recovery integer is outside its bound")
    return value


def _exact(value: Any, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise RecoveryProofError("recovery metadata has unexpected fields")
    return value


def _path(value: Any) -> Path:
    if isinstance(value, Path):
        value = str(value)
    if (not isinstance(value, str) or len(value) > 4096 or not value.startswith("/")
            or value == "/" or "//" in value or any(ord(c) < 32 or ord(c) == 127 for c in value)
            or any(part in {".", ".."} for part in value.split("/"))
            or str(Path(value)) != value):
        raise RecoveryProofError("recovery path is not a normalized absolute path")
    return Path(value)


def _identifier(value: Any, prefix: str) -> str:
    if not isinstance(value, str) or re.fullmatch(prefix + r"[0-9a-f]{32}", value) is None:
        raise RecoveryProofError("recovery identity is invalid")
    return value


def _timestamp(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", value) is None:
        raise RecoveryProofError("recovery timestamp is invalid")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise RecoveryProofError("recovery timestamp is invalid") from error
    return value


def _directory(info: os.stat_result, *, private: bool = False) -> None:
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in {0, os.geteuid()}:
        raise RecoveryProofError("recovery directory is unsafe")
    mode = stat.S_IMODE(info.st_mode)
    if private:
        if info.st_uid != os.geteuid() or mode & 0o077:
            raise RecoveryProofError("recovery metadata parent is not private")
    elif mode & 0o022 and not (info.st_uid == 0 and mode & stat.S_ISVTX):
        # Deployment homes may be owner-owned 0775. Retain each directory FD,
        # reject symlink traversal, and require an owner-private final parent
        # plus owner-only, single-link, hash-bound leaf. A peer cannot substitute
        # an acceptable leaf, even if it can rename an ancestor. Do not infer
        # group exclusivity from incomplete local/NSS membership enumeration.
        if mode & 0o002 or info.st_uid != os.geteuid():
            raise RecoveryProofError("recovery ancestor permits another writer")


@contextmanager
def _parent(path: Path, *, private: bool = True) -> Iterator[int]:
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        _directory(os.fstat(descriptor))
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            _directory(os.fstat(descriptor))
        _directory(os.fstat(descriptor), private=private)
        yield descriptor
    finally:
        os.close(descriptor)


def _fingerprint(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read_bytes(path: Path, maximum: int, *, immutable: bool = True) -> bytes:
    path = _path(path)
    try:
        with _parent(path, private=immutable) as parent:
            descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                                 dir_fd=parent)
            try:
                before = os.fstat(descriptor)
                mode = stat.S_IMODE(before.st_mode)
                if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                        or before.st_uid not in ({os.geteuid()} if immutable else {0, os.geteuid()})
                        or not 1 <= before.st_size <= maximum
                        or (mode != 0o400 if immutable else bool(mode & 0o022))):
                    raise RecoveryProofError("recovery file is not a safe bounded immutable file")
                chunks = []
                size = 0
                while size < before.st_size:
                    chunk = os.read(descriptor, min(65536, before.st_size - size))
                    if not chunk:
                        raise RecoveryProofError("recovery file ended during read")
                    chunks.append(chunk)
                    size += len(chunk)
                after = os.fstat(descriptor)
                named = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
                if _fingerprint(before) != _fingerprint(after) or _fingerprint(after) != _fingerprint(named):
                    raise RecoveryProofError("recovery file changed during read")
                return b"".join(chunks)
            finally:
                os.close(descriptor)
    except OSError as error:
        raise RecoveryProofError("recovery metadata cannot be opened safely") from error


def _decode(body: bytes) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise RecoveryProofError("recovery metadata repeats a JSON key")
            result[key] = value
        return result

    def constant(_value):
        raise RecoveryProofError("recovery metadata contains a nonfinite number")

    try:
        value = json.loads(body, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise RecoveryProofError("recovery metadata is malformed") from error
    if not isinstance(value, dict):
        raise RecoveryProofError("recovery metadata is not an object")
    return value


def _document(path: Path, maximum: int, expected_sha256: str | None = None,
              *, pretty: bool = False) -> tuple[dict[str, Any], bytes]:
    body = _read_bytes(path, maximum)
    if expected_sha256 is not None and _hash(body) != _sha(expected_sha256):
        raise RecoveryProofError("recovery metadata physical hash differs")
    value = _decode(body)
    if pretty and body != pretty_bytes(value):
        raise RecoveryProofError("recovery metadata serialization is not canonical")
    return value, body


def validate_plan(path: Path, expected_sha256: str) -> dict[str, Any]:
    """Read one sealed plan and its campaign/schedule metadata; never media."""
    plan, _ = _document(_path(path), MAX_PLAN_BYTES, expected_sha256, pretty=True)
    _exact(plan, {"schema_version", "kind", "config_path", "config_sha256", "config_id",
                  "created_at", "entries", "limits", "plan_sha256"})
    if type(plan["schema_version"]) is not int or plan["schema_version"] != 1 or plan["kind"] != PLAN_KIND:
        raise RecoveryProofError("retry plan schema is unsupported")
    _timestamp(plan["created_at"])
    _identifier(plan["config_id"], "himrautocfg_")
    core = {key: value for key, value in plan.items() if key != "plan_sha256"}
    if _sha(plan["plan_sha256"]) != _hash(canonical_bytes(core)):
        raise RecoveryProofError("retry plan identity differs")
    limits = _exact(plan["limits"], {"max_attempts_per_item", "retry_backoff_seconds", "max_run_seconds"})
    _integer(limits["max_attempts_per_item"], 1, 3)
    _integer(limits["retry_backoff_seconds"], 1, 86400)
    _integer(limits["max_run_seconds"], 1, 86400)
    entries = plan["entries"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= 1024:
        raise RecoveryProofError("retry plan entry count is invalid")
    keys = set()
    jobs = set()
    requested = set()
    for entry in entries:
        _exact(entry, _ENTRY_KEYS)
        _identifier(entry["bundle_id"], "acqbundle_")
        _integer(entry["ordinal"], 1, 999999)
        if not isinstance(entry["job_id"], str) or re.fullmatch(r"acq-[0-9a-f]{32}-[0-9]{6}", entry["job_id"]) is None:
            raise RecoveryProofError("retry plan job identity is invalid")
        for name in ("manifest_sha256", "work_order_sha256", "quarantine_receipt_sha256"):
            _sha(entry[name])
        _integer(entry["expected_bytes"], 1, 2**63 - 1)
        _path(entry["result_path"])
        manifest_path = str(_path(entry["manifest_path"]))
        if not isinstance(entry["url"], str) or len(entry["url"]) > 8192 or any(ord(c) < 32 for c in entry["url"]):
            raise RecoveryProofError("retry plan source URL is invalid")
        try:
            url = urlsplit(entry["url"])
            if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.fragment:
                raise RecoveryProofError("retry plan source URL is not public HTTP")
        except ValueError as error:
            raise RecoveryProofError("retry plan source URL is invalid") from error
        key = (entry["bundle_id"], entry["ordinal"])
        if key in keys or entry["job_id"] in jobs:
            raise RecoveryProofError("retry plan repeats an acquisition identity")
        keys.add(key)
        jobs.add(entry["job_id"])
        requested.add((manifest_path, entry["manifest_sha256"], entry["bundle_id"]))
    config, _ = _document(_path(plan["config_path"]), MAX_METADATA_BYTES, plan["config_sha256"])
    if config.get("config_id") != plan["config_id"] or not isinstance(config.get("campaign"), dict):
        raise RecoveryProofError("retry plan campaign binding differs")
    schedules = config["campaign"].get("schedules")
    if not isinstance(schedules, list) or not 1 <= len(schedules) <= 1024:
        raise RecoveryProofError("retry plan campaign schedules are invalid")
    enrolled = set()
    for reference in schedules:
        if not isinstance(reference, dict) or not {"path", "sha256", "schedule_id"} <= set(reference):
            raise RecoveryProofError("retry plan campaign schedule reference is invalid")
        schedule, _ = _document(_path(reference["path"]), MAX_METADATA_BYTES, reference["sha256"])
        queue = schedule.get("queue")
        if schedule.get("schedule_id") != reference["schedule_id"] or not isinstance(queue, dict):
            raise RecoveryProofError("retry plan schedule identity differs")
        enrolled.add((str(_path(queue.get("manifest_path"))), _sha(queue.get("manifest_sha256")),
                      _identifier(queue.get("bundle_id"), "acqbundle_")))
    if not requested <= enrolled:
        raise RecoveryProofError("retry plan contains a manifest outside its campaign")
    return plan


def _binding(bundle: dict[str, Any], entry: dict[str, Any], order: dict[str, Any],
             quarantine: dict[str, Any]) -> dict[str, Any]:
    try:
        manifest = bundle["manifest"]
        body = bundle["body"]
        if not isinstance(body, bytes) or len(body) > MAX_MANIFEST_BYTES or _decode(body) != manifest:
            raise RecoveryProofError("retry manifest body differs")
        ordinal = _integer(entry["queue_ordinal"], 1, 999999)
        if not isinstance(entry["job_id"], str) or re.fullmatch(r"acq-[0-9a-f]{32}-[0-9]{6}", entry["job_id"]) is None:
            raise RecoveryProofError("retry job identity is invalid")
        if manifest["work_orders"][ordinal - 1] != entry or order["job_id"] != entry["job_id"]:
            raise RecoveryProofError("retry work order differs from its manifest entry")
        order_body = pretty_bytes(order)
        if entry.get("sha256") != _hash(order_body) or type(entry.get("byte_count")) is not int or entry["byte_count"] != len(order_body):
            raise RecoveryProofError("retry work-order bytes differ from their sealed manifest pin")
        binding = {
            "bundle_id": _identifier(manifest["bundle_id"], "acqbundle_"),
            "manifest_sha256": _hash(body), "ordinal": ordinal, "job_id": entry["job_id"],
            "work_order_sha256": _hash(canonical_bytes(order)),
            "quarantine_receipt_sha256": _sha(quarantine["receipt_sha256"]),
        }
        old_core = {key: value for key, value in quarantine.items() if key != "receipt_sha256"}
        if (binding["quarantine_receipt_sha256"] != _hash(canonical_bytes(old_core))
                or quarantine.get("receipt_kind") != "public_acquisition_quarantine"
                or type(quarantine.get("schema_version")) is not int or quarantine["schema_version"] != 1
                or type(quarantine.get("failure_attempt_limit")) is not int or quarantine["failure_attempt_limit"] != 3
                or any(quarantine.get(key) != value for key, value in binding.items()
                       if key != "quarantine_receipt_sha256")
                or manifest["policy"]["media_output_root"] != order["output"]["root"]):
            raise RecoveryProofError("retry quarantine binding differs")
        return binding
    except (KeyError, IndexError, TypeError) as error:
        raise RecoveryProofError("retry source binding is malformed") from error


def _proof_path(entry: dict[str, Any], order: dict[str, Any], bundle_id: str) -> Path:
    return (_path(order["output"]["root"]) / DIRECTORY / _identifier(bundle_id, "acqbundle_")
            / "ordinals" / f"{_integer(entry['queue_ordinal'], 1, 999999):06d}" / "completion.json")


def completion_path(bundle: dict[str, Any], entry: dict[str, Any], order: dict[str, Any]) -> Path:
    return _proof_path(entry, order, bundle["manifest"]["bundle_id"])


def _result_binding(order: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    try:
        result_path = (_path(order["output"]["root"]) / "jobs" / order["job_id"]
                       / _hash(canonical_bytes(order)) / "result.json")
        value = {"result_path": str(_path(result_path)),
                 "result_sha256": _sha(state["result_sha256"]),
                 "media_sha256": _sha(state["media_sha256"]),
                 "media_byte_count": _integer(state["byte_count"], 1, 2**63 - 1)}
        result = state["result"]
        if (result["result_path"] != str(result_path) or result["job_id"] != order["job_id"]
                or result["status"] != "completed" or result["dry_run"] is not False
                or result["work_order_sha256"] != _hash(canonical_bytes(order))
                or result["admission"]["sha256"] != value["media_sha256"]
                or type(result["admission"]["byte_count"]) is not int
                or result["admission"]["byte_count"] != value["media_byte_count"]
                or _hash(pretty_bytes(result)) != value["result_sha256"]):
            raise RecoveryProofError("retry completed-result state differs")
        return value
    except (KeyError, TypeError) as error:
        raise RecoveryProofError("retry completed-result state is malformed") from error


def _authorization(plan: dict[str, Any], bundle: dict[str, Any], order: dict[str, Any],
                   binding: dict[str, Any], result: dict[str, Any]) -> None:
    expected = {**binding, "manifest_path": str(_path(bundle["path"])),
                "result_path": result["result_path"],
                "expected_bytes": order["adapter_config"].get("expected_byte_count"),
                "url": order["adapter_config"].get("url")}
    if (order.get("adapter") != "direct_http" or order.get("source", {}).get("access_state") != "public"
            or result["media_byte_count"] != expected["expected_bytes"]
            or sum(canonical_bytes(entry) == canonical_bytes(expected) for entry in plan["entries"]) != 1):
        raise RecoveryProofError("completed retry is not exactly authorized by its plan")


def _verify(proof: dict[str, Any], bundle: dict[str, Any], entry: dict[str, Any],
            order: dict[str, Any], quarantine: dict[str, Any], state: dict[str, Any],
            plan: dict[str, Any] | None = None) -> dict[str, Any]:
    _exact(proof, _PROOF_KEYS)
    if type(proof["schema_version"]) is not int or proof["schema_version"] != 1 or proof["kind"] != PROOF_KIND:
        raise RecoveryProofError("retry completion schema is unsupported")
    if _sha(proof["receipt_sha256"]) != _hash(canonical_bytes(
            {key: value for key, value in proof.items() if key != "receipt_sha256"})):
        raise RecoveryProofError("retry completion identity differs")
    binding = _binding(bundle, entry, order, quarantine)
    result = _result_binding(order, state)
    expected = {**binding, **result}
    if canonical_bytes({key: proof[key] for key in expected}) != canonical_bytes(expected):
        raise RecoveryProofError("retry completion proof differs from the exact result or quarantine")
    if _timestamp(proof["completed_at"]) != _timestamp(state["result"]["completed_at"]):
        raise RecoveryProofError("retry completion timestamp differs from its result")
    if plan is None:
        plan = validate_plan(_path(proof["authorization_plan_path"]), _sha(proof["authorization_plan_sha256"]))
    _authorization(plan, bundle, order, binding, result)
    return proof


def verify_completion(bundle: dict[str, Any], entry: dict[str, Any], order: dict[str, Any],
                      quarantine: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    proof, _ = _document(completion_path(bundle, entry, order), MAX_METADATA_BYTES, pretty=True)
    return _verify(proof, bundle, entry, order, quarantine, state)


def _publish(path: Path, body: bytes, output_root: Path) -> None:
    """Atomically add one proof beneath a verified existing private output root."""
    try:
        with _parent(output_root / "unused") as root_descriptor:
            descriptor = os.dup(root_descriptor)
            try:
                for component in path.parent.relative_to(output_root).parts:
                    try:
                        os.mkdir(component, 0o700, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                    child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                    dir_fd=descriptor)
                    try:
                        _directory(os.fstat(child), private=True)
                        os.fsync(descriptor)
                    except BaseException:
                        os.close(child)
                        raise
                    os.close(descriptor)
                    descriptor = child
                temporary = ".completion-" + os.urandom(16).hex()
                writer = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 0o400, dir_fd=descriptor)
                try:
                    try:
                        offset = 0
                        while offset < len(body):
                            written = os.write(writer, body[offset:])
                            if written <= 0:
                                raise RecoveryProofError("retry completion write made no progress")
                            offset += written
                        os.fsync(writer)
                    finally:
                        os.close(writer)
                    try:
                        os.link(temporary, path.name, src_dir_fd=descriptor, dst_dir_fd=descriptor,
                                follow_symlinks=False)
                    except FileExistsError:
                        if _read_bytes(path, MAX_METADATA_BYTES) != body:
                            raise RecoveryProofError("existing retry completion proof differs")
                finally:
                    os.unlink(temporary, dir_fd=descriptor)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except OSError as error:
        raise RecoveryProofError("retry completion proof could not be sealed") from error


def seal_completion(bundle: dict[str, Any], entry: dict[str, Any], order: dict[str, Any],
                    quarantine: dict[str, Any], authorization_plan_path: Path,
                    authorization_plan_sha256: str, state: dict[str, Any]) -> dict[str, Any]:
    plan_path = _path(authorization_plan_path)
    plan = validate_plan(plan_path, authorization_plan_sha256)
    binding = _binding(bundle, entry, order, quarantine)
    result = _result_binding(order, state)
    _authorization(plan, bundle, order, binding, result)
    core = {"schema_version": 1, "kind": PROOF_KIND, **binding,
            "authorization_plan_path": str(plan_path),
            "authorization_plan_sha256": _sha(authorization_plan_sha256), **result,
            "completed_at": _timestamp(state["result"]["completed_at"])}
    proof = {**core, "receipt_sha256": _hash(canonical_bytes(core))}
    _verify(proof, bundle, entry, order, quarantine, state, plan)
    _publish(completion_path(bundle, entry, order), pretty_bytes(proof), _path(order["output"]["root"]))
    return verify_completion(bundle, entry, order, quarantine, state)


def _verify_projected(entry, order, quarantine, state):
    path = _proof_path(entry, order, quarantine["bundle_id"])
    proof, _ = _document(path, MAX_METADATA_BYTES, pretty=True)
    _exact(proof, _PROOF_KEYS)
    plan = validate_plan(_path(proof["authorization_plan_path"]), _sha(proof["authorization_plan_sha256"]))
    matches = [candidate for candidate in plan["entries"]
               if candidate["bundle_id"] == quarantine["bundle_id"]
               and candidate["ordinal"] == entry["queue_ordinal"]]
    if len(matches) != 1:
        raise RecoveryProofError("retry projection lacks exactly one authorized entry")
    manifest_path = _path(matches[0]["manifest_path"])
    manifest, body = _document(manifest_path, MAX_MANIFEST_BYTES, matches[0]["manifest_sha256"])
    return _verify(proof, {"path": manifest_path, "body": body, "manifest": manifest},
                   entry, order, quarantine, state, plan)


def install_adapter(queue_runner: ModuleType) -> bool:
    """Adapt only the reviewed legacy completed-row projection, idempotently."""
    if not isinstance(queue_runner, ModuleType) or not isinstance(getattr(queue_runner, "acquire", None), ModuleType):
        raise RecoveryProofError("retry adapter requires imported legacy modules")
    acquire = queue_runner.acquire
    runner_path = _path(queue_runner.__file__)
    acquire_path = _path(acquire.__file__)
    if (queue_runner.IMPLEMENTATION_VERSION != "0.2.0" or acquire.IMPLEMENTATION_VERSION != "0.3.2"
            or _hash(_read_bytes(runner_path, MAX_MANIFEST_BYTES, immutable=False)) != LEGACY_RUNNER_SHA256
            or _hash(_read_bytes(acquire_path, MAX_MANIFEST_BYTES, immutable=False)) != LEGACY_ACQUIRE_SHA256):
        raise RecoveryProofError("retry adapter source binding is not the reviewed legacy implementation")
    with _INSTALL_LOCK:
        original = getattr(queue_runner, "_result_action", None)
        installed = _INSTALLED.get(queue_runner)
        if installed is not None:
            if original is not installed[1]:
                raise RecoveryProofError("retry adapter was replaced")
            return True
        if (not callable(original) or getattr(original, "__name__", None) != "_result_action"
                or Path(original.__code__.co_filename).resolve() != runner_path.resolve()):
            raise RecoveryProofError("retry projection primitive is not source-bound")

        def recovered_result_action(*, entry, order, state, failure_state, action, adapter_invoked):
            row = original(entry=entry, order=order, state=state, failure_state=failure_state,
                           action=action, adapter_invoked=adapter_invoked)
            if state is not None and failure_state["quarantine"] is not None:
                try:
                    _verify_projected(entry, order, failure_state["quarantine"], state)
                except RecoveryProofError as error:
                    raise queue_runner.QueueRunnerError(str(error)) from error
                row = dict(row)
                row["quarantine_receipt_sha256"] = None
            return row

        recovered_result_action.__wrapped__ = original
        queue_runner._result_action = recovered_result_action
        _INSTALLED[queue_runner] = (original, recovered_result_action)
        return True
