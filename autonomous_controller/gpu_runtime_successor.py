"""Narrow, additive GPU control succession; immutable historical receipts stay intact.

This is not admission authority for an old launcher.  The optional sidecar selects
fresh controls for future materialization, or the exact controls embedded in an
existing batch for journal identity.  Actual launches still pass the unmodified
trusted launcher.  Publication requires the caller's stopped-controller leases.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import secrets
import stat
import sys
import threading
import types
from pathlib import Path
from typing import Any, Callable

from .config import ControllerConfig, canonical_bytes


KIND = "himr_gpu_runtime_successor_authorization"
SIDECAR_NAME = "gpu-runtime-successor-v1.json"
MAX_JSON_BYTES = 8 * 1024 * 1024
OLD_RUNTIME_SHA256 = "129f5a7013ad8213a079ff2755778cba16ebbfae00d20eafe0b40d406244bed7"
OLD_BWRAP = {
    "name": "bubblewrap", "path": "/usr/bin/bwrap",
    "sha256": "139bf12775025adf5c8523d119c5ad2950281335573708fd839c60181a3886dc",
    "byte_count": 86552, "mode": "0755", "uid": 0,
}
NEW_BWRAP = {
    **OLD_BWRAP,
    "sha256": "6da06f152b0865172d73348c34cb88487c326ce2f21cd980fc25ff10c4dbcdfb",
    "byte_count": 90696,
}
CONTROL_FIELDS = frozenset({
    "runtime_admission", "runtime_admission_sha256", "launcher_profile",
    "launcher_profile_sha256", "local_readiness", "local_readiness_sha256",
    "local_launcher",
})
POLICY = {
    "scope": "future_local_private_gpu_materialization_only",
    "historical_receipts": "unchanged_exact_candidate_replay_only",
    "old_pending_batch_launch": "forbidden_rematerialize_explicitly",
    "image_model_profile_root_runtime_change": False,
    "host_library_closure_change": False,
    "admitted_runtime_authority": False,
    "launcher_checks_bypassed": False,
    "same_uid_mutation_resistance": False,
}
HELPER_HASHES = {
    "admit_runtime_v2.py": "84834a81aea2f7bc7e93e02e1c4915201bd6d297b4849baaea843bfa4b407b1b",
    "trusted_launcher_v2.py": "ba3eeba50128f20b26b86c9bd1635f4cc14d520e470e8f260e0bb83749531b04",
}
_HELPERS: dict[str, types.ModuleType] = {}
_HELPER_LOCK = threading.RLock()


class RuntimeSuccessorError(RuntimeError):
    """The reviewed runtime transition cannot be proved exactly."""


def _path(value: Any) -> Path:
    if (not isinstance(value, (str, Path)) or not str(value)
            or "\x00" in str(value) or "\\" in str(value)):
        raise RuntimeSuccessorError("control path is not a local absolute path")
    raw = str(value)
    path = Path(raw)
    if not path.is_absolute() or raw == "/" or str(path) != raw or os.path.normpath(raw) != raw:
        raise RuntimeSuccessorError("control path is not normalized and absolute")
    return path


def _digest(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise RuntimeSuccessorError("control SHA-256 is invalid")
    return value


def _parent_fd(path: Path) -> int:
    """Walk pinned directory descriptors, refusing every symlink component."""
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parent.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _witness(info: os.stat_result) -> tuple[Any, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read_file(path: Path, expected: str | None = None, *,
               modes: set[int] = frozenset({0o400}),
               maximum: int = MAX_JSON_BYTES, same_uid: bool = True) -> bytes:
    path = _path(path)
    parent = descriptor = None
    try:
        parent = _parent_fd(path)
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=parent)
        initial = os.fstat(descriptor)
        if (not stat.S_ISREG(initial.st_mode) or initial.st_nlink != 1
                or stat.S_IMODE(initial.st_mode) not in modes
                or initial.st_uid not in ({os.geteuid()} if same_uid else {0, os.geteuid()})
                or not 0 < initial.st_size <= maximum):
            raise RuntimeSuccessorError("control file ownership, mode, type, links, or size is unsafe")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        if (len(body) != initial.st_size or len(body) > maximum
                or _witness(initial) != _witness(os.fstat(descriptor))
                or _witness(initial) != _witness(os.stat(path.name, dir_fd=parent, follow_symlinks=False))):
            raise RuntimeSuccessorError("control file changed during its bounded read")
        if expected is not None and hashlib.sha256(body).hexdigest() != _digest(expected):
            raise RuntimeSuccessorError("control file SHA-256 differs")
        return body
    except OSError as error:
        raise RuntimeSuccessorError("cannot safely inspect runtime succession control") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if parent is not None:
            os.close(parent)


def _pairs(rows: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in rows:
        if key in result:
            raise RuntimeSuccessorError("control JSON contains duplicate keys")
        result[key] = value
    return result


def _bad_constant(_value: str) -> None:
    raise RuntimeSuccessorError("control JSON contains a nonfinite number")


def _read_json(path: Path, expected: str | None = None) -> dict[str, Any]:
    body = _read_file(path, expected)
    try:
        value = json.loads(body, object_pairs_hook=_pairs, parse_constant=_bad_constant)
        if not isinstance(value, dict) or canonical_bytes(value) != body:
            raise RuntimeSuccessorError("control JSON is not a canonical object")
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise RuntimeSuccessorError("control JSON is invalid") from error


def _helper(filename: str) -> types.ModuleType:
    """Execute the reviewed helper snapshot, not a second unbound path read."""
    with _HELPER_LOCK:
        if filename in _HELPERS:
            return _HELPERS[filename]
        path = Path(__file__).resolve().parents[1] / "pipeline" / "gpu" / filename
        body = _read_file(path, HELPER_HASHES[filename], modes={0o644, 0o444, 0o400, 0o755},
                          same_uid=False)
        name = "_himr_successor_" + filename.removesuffix(".py")
        module = types.ModuleType(name)
        module.__file__ = str(path)
        sys.modules[name] = module
        try:
            exec(compile(body, str(path), "exec"), module.__dict__)
        except BaseException:
            sys.modules.pop(name, None)
            raise
        _HELPERS[filename] = module
        return module


def _isolated_admission() -> types.ModuleType:
    """Rebind function globals to a private dictionary; shared modules stay intact."""
    original = _helper("admit_runtime_v2.py")
    private = types.ModuleType("_himr_successor_historical_admission")
    private.__dict__.update(original.__dict__)
    for name, value in original.__dict__.items():
        if isinstance(value, types.FunctionType) and value.__globals__ is original.__dict__:
            rebound = types.FunctionType(value.__code__, private.__dict__, name,
                                         value.__defaults__, value.__closure__)
            rebound.__kwdefaults__ = value.__kwdefaults__
            private.__dict__[name] = rebound
    return private


def _controls(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != CONTROL_FIELDS:
        raise RuntimeSuccessorError("successor must provide exactly the seven GPU control fields")
    return {key: _digest(value[key]) if key.endswith("_sha256") else str(_path(value[key]))
            for key in sorted(CONTROL_FIELDS)}


def _original_controls(config: ControllerConfig) -> dict[str, str]:
    gpu = config.section("gpu_readiness")
    try:
        return _controls({key: gpu[key] for key in CONTROL_FIELDS})
    except KeyError as error:
        raise RuntimeSuccessorError("controller GPU controls are incomplete") from error


def _reference(controls: dict[str, str], receipt: dict[str, Any]) -> dict[str, str]:
    return {
        "receipt_path": controls["runtime_admission"],
        "receipt_sha256": controls["runtime_admission_sha256"],
        "receipt_id": receipt["receipt_id"],
        "identity_sha256": receipt["identity_sha256"],
        "status": receipt["status"],
    }


def _without(value: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key not in keys}


def _require_equal(left: Any, right: Any, label: str) -> None:
    if left != right:
        raise RuntimeSuccessorError(f"successor changes unreviewed {label}")


def _validate_control_bundle(config: ControllerConfig, controls: dict[str, str]) -> dict[str, Any]:
    launcher = _helper("trusted_launcher_v2.py")
    profile = launcher.validate_launcher_profile(_read_json(
        Path(controls["launcher_profile"]), controls["launcher_profile_sha256"]))
    runtime = launcher.validate_runtime_receipt(
        _read_json(Path(controls["runtime_admission"]), controls["runtime_admission_sha256"]),
        mode=launcher.MODE_LOCAL_PRIVATE, launcher_profile=profile,
        launcher_profile_path=controls["launcher_profile"],
        profile_sha256=controls["launcher_profile_sha256"])
    readiness = launcher.validate_local_readiness(_read_json(
        Path(controls["local_readiness"]), controls["local_readiness_sha256"]))
    _require_equal(profile["runtime_admission_install_path"], controls["runtime_admission"], "runtime install path")
    _require_equal(profile["launcher"]["path"], controls["local_launcher"], "launcher path")
    _read_file(Path(controls["local_launcher"]), profile["launcher"]["sha256"], modes={0o500})
    gpu = config.section("gpu_readiness")
    for name in ("production_profile", "root_registration"):
        for key, expected in (("path", gpu[name]), ("sha256", gpu[name + "_sha256"])):
            _require_equal(profile[name][key], expected, name)
    bindings = {
        "launcher": {**profile["launcher"], "profile": {
            "path": controls["launcher_profile"], "sha256": controls["launcher_profile_sha256"],
            "identity_sha256": profile["identity_sha256"]}},
        "runtime_admission": {"path": controls["runtime_admission"],
            "sha256": controls["runtime_admission_sha256"],
            "identity_sha256": runtime["identity_sha256"], "status": runtime["status"]},
        "execution_image": profile["execution_image"],
        "production_profile": profile["production_profile"],
        "root_registration": _without(profile["root_registration"], "root_id"),
        "host_abi_identity_sha256": profile["host_abi"]["identity_sha256"],
    }
    for name, expected in bindings.items():
        _require_equal(readiness[name], expected, "readiness " + name)
    return {"profile": profile, "runtime": runtime, "readiness": readiness}


def _transition(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    old_profile, new_profile = old["profile"], new["profile"]
    _require_equal(_without(old_profile, "launcher", "runtime_admission_install_path",
                           "system_tools", "host_abi", "identity_sha256", "profile_id"),
                   _without(new_profile, "launcher", "runtime_admission_install_path",
                            "system_tools", "host_abi", "identity_sha256", "profile_id"),
                   "launcher policy, image, production profile, or root")
    _require_equal(_without(old_profile["launcher"], "path"),
                   _without(new_profile["launcher"], "path"), "launcher bytes")
    old_tools, new_tools = old_profile["system_tools"], new_profile["system_tools"]
    _require_equal(_without(old_tools, "bubblewrap"), _without(new_tools, "bubblewrap"), "system tools")
    _require_equal(old_tools.get("bubblewrap"), _without(OLD_BWRAP, "name"), "historical bubblewrap")
    _require_equal(new_tools.get("bubblewrap"), _without(NEW_BWRAP, "name"), "replacement bubblewrap")
    old_abi, new_abi = old_profile["host_abi"], new_profile["host_abi"]
    _require_equal(_without(old_abi, "platform", "identity_sha256", "manifest_id"),
                   _without(new_abi, "platform", "identity_sha256", "manifest_id"),
                   "host library or consumer closure")
    platform_updates = ("release", "version", "nvidia_kernel_module_report_sha256",
                        "nvidia_kernel_module_report_byte_count")
    _require_equal(_without(old_abi["platform"], *platform_updates),
                   _without(new_abi["platform"], *platform_updates), "platform architecture or NVIDIA version")
    old_runtime, new_runtime = old["runtime"], new["runtime"]
    _require_equal(_without(old_runtime, "trusted_install", "runtime_candidate_identity_sha256",
                           "identity_sha256", "receipt_id"),
                   _without(new_runtime, "trusted_install", "runtime_candidate_identity_sha256",
                            "identity_sha256", "receipt_id"), "runtime, model, root, profile, or gates")
    _require_equal(_without(old_runtime["trusted_install"], "launcher", "launcher_profile", "system_tools"),
                   _without(new_runtime["trusted_install"], "launcher", "launcher_profile", "system_tools"),
                   "installation ownership policy")
    _require_equal(_without(old_runtime["trusted_install"]["launcher"], "path"),
                   _without(new_runtime["trusted_install"]["launcher"], "path"), "installed launcher metadata")
    _require_equal(_without(old_runtime["trusted_install"]["launcher_profile"], "path", "sha256", "byte_count"),
                   _without(new_runtime["trusted_install"]["launcher_profile"], "path", "sha256", "byte_count"),
                   "installed profile ownership")
    if old_runtime["status"] != "candidate" or any(value is not None for value in old_runtime["gates"].values()):
        raise RuntimeSuccessorError("only the exact ungated historical candidate may be succeeded")
    return {"old_bubblewrap": dict(OLD_BWRAP), "new_bubblewrap": dict(NEW_BWRAP),
            "old_host_abi_identity_sha256": old_abi["identity_sha256"],
            "new_host_abi_identity_sha256": new_abi["identity_sha256"],
            "old_platform": old_abi["platform"], "new_platform": new_abi["platform"]}


def _replay_current(controls: dict[str, str], bundle: dict[str, Any]) -> None:
    admission = _helper("admit_runtime_v2.py")
    current = admission.load_receipt(controls["runtime_admission"],
        controls["runtime_admission_sha256"], require_admitted=False, deep_image=False)
    _require_equal(current, bundle["runtime"], "current runtime replay")
    launcher = _helper("trusted_launcher_v2.py")
    platform = bundle["profile"]["host_abi"]["platform"]
    _require_equal(launcher.observe_host_abi_platform(platform["nvidia_driver_version"]),
                   platform, "current booted platform")


def build_successor(config: ControllerConfig, new_controls: dict[str, str]) -> dict[str, Any]:
    """Validate fresh doctor controls without writing state or hashing the image."""
    try:
        _require_equal(_read_json(config.path, config.physical_sha256),
                       config.document, "controller configuration snapshot")
        old_controls = _original_controls(config)
        new_controls = _controls(new_controls)
        if old_controls["runtime_admission_sha256"] != OLD_RUNTIME_SHA256:
            raise RuntimeSuccessorError("controller does not bind the reviewed historical candidate")
        for field in ("runtime_admission", "launcher_profile", "local_readiness", "local_launcher"):
            if old_controls[field] == new_controls[field]:
                raise RuntimeSuccessorError("successor must use new, separate control paths")
        old = _validate_control_bundle(config, old_controls)
        new = _validate_control_bundle(config, new_controls)
        transition = _transition(old, new)
        _replay_current(new_controls, new)
        core = {"kind": KIND, "schema_version": 1, "implementation_version": "0.1.0",
            "config": {"path": str(config.path), "sha256": config.physical_sha256,
                       "config_id": config.config_id},
            "old_controls": old_controls, "new_controls": new_controls,
            "old_runtime": _reference(old_controls, old["runtime"]),
            "new_runtime": _reference(new_controls, new["runtime"]),
            "transition": transition, "policy": dict(POLICY)}
        identity = hashlib.sha256(canonical_bytes(core)).hexdigest()
        return {**core, "identity_sha256": identity,
                "authorization_id": "gpurtsuccessor_" + identity[:32]}
    except RuntimeSuccessorError:
        raise
    except Exception as error:
        raise RuntimeSuccessorError("GPU runtime succession failed exact control validation") from error


def load_successor(config: ControllerConfig) -> dict[str, Any] | None:
    path = _path(config.path).parent / SIDECAR_NAME
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    value = _read_json(path)
    if set(value) != {"kind", "schema_version", "implementation_version", "config",
                      "old_controls", "new_controls", "old_runtime", "new_runtime",
                      "transition", "policy", "identity_sha256", "authorization_id"}:
        raise RuntimeSuccessorError("runtime succession authorization has unexpected fields")
    expected = build_successor(config, value["new_controls"])
    _require_equal(value, expected, "runtime succession authorization or binding")
    return expected


def stage_successor(config: ControllerConfig, new_controls: dict[str, str], *,
                    assert_stopped: Callable[[], None]) -> dict[str, Any]:
    """Publish once, under caller-held controller/legacy exclusion leases.

    ``assert_stopped`` must check authoritative stopped control and process state;
    its caller must retain the exclusion locks for this entire call.  This helper
    never stops/restarts processes, changes control state, or replaces a sidecar.
    """
    if not callable(assert_stopped):
        raise RuntimeSuccessorError("publication requires an explicit stopped-state guard")
    assert_stopped()
    value = build_successor(config, new_controls)
    body = canonical_bytes(value)
    path = _path(config.path).parent / SIDECAR_NAME
    parent = _parent_fd(path)
    temporary = f".{SIDECAR_NAME}.tmp-{secrets.token_hex(16)}"
    descriptor = None
    created = False
    try:
        info = os.fstat(parent)
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise RuntimeSuccessorError("successor parent must be owned mode 0700")
        assert_stopped()
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o400, dir_fd=parent)
        created = True
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path.name, src_dir_fd=parent, dst_dir_fd=parent,
                follow_symlinks=False)
        os.unlink(temporary, dir_fd=parent)
        created = False
        os.fsync(parent)
        return value
    except OSError as error:
        raise RuntimeSuccessorError("cannot exclusively publish runtime succession authorization") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if created:
            os.unlink(temporary, dir_fd=parent)
        os.close(parent)


def effective_gpu(config: ControllerConfig, batch_runtime: dict[str, Any] | None = None) -> dict[str, Any]:
    """Select exact controls without rewriting a historical batch or its journal."""
    successor = load_successor(config)
    gpu = copy.deepcopy(config.section("gpu_readiness"))
    if successor is None:
        if batch_runtime is not None:
            old_controls = _original_controls(config)
            old_receipt = _read_json(Path(old_controls["runtime_admission"]), old_controls["runtime_admission_sha256"])
            _require_equal(batch_runtime, _reference(old_controls, old_receipt), "batch runtime")
        return gpu
    if batch_runtime is None or batch_runtime == successor["new_runtime"]:
        gpu.update(successor["new_controls"])
    elif batch_runtime != successor["old_runtime"]:
        raise RuntimeSuccessorError("batch runtime is not an authorized exact old or new reference")
    return gpu


def install_historical_replay(asr_v5_module: types.ModuleType, config: ControllerConfig) -> bool:
    """Adapt only historical receipt inspection, never real admission or launch."""
    if load_successor(config) is None:
        return False
    marker = getattr(asr_v5_module.runtime_admission_reference, "_gpu_successor_config", None)
    binding = (str(config.path), config.physical_sha256)
    if marker == binding:
        return True
    if marker is not None:
        raise RuntimeSuccessorError("ASR helper already binds a different runtime succession")
    original = asr_v5_module.runtime_admission_reference

    def replay(receipt_path_value: str | Path, receipt_sha256: str, *, require_admitted: bool):
        old_controls = _original_controls(config)
        if (require_admitted or str(receipt_path_value) != old_controls["runtime_admission"]
                or receipt_sha256 != OLD_RUNTIME_SHA256):
            return original(receipt_path_value, receipt_sha256, require_admitted=require_admitted)
        successor = load_successor(config)
        if successor is None:
            return original(receipt_path_value, receipt_sha256, require_admitted=require_admitted)
        admission = _isolated_admission()
        tool = admission._tool_reference
        old_tool_ref = {key: OLD_BWRAP[key] for key in ("name", "path", "sha256")}
        new_tool_ref = {key: NEW_BWRAP[key] for key in ("name", "path", "sha256")}

        def historical_tool(value: Any, label: str):
            if value != old_tool_ref:
                return tool(value, label)
            _require_equal(tool(new_tool_ref, label), NEW_BWRAP, "current reviewed bubblewrap")
            return dict(OLD_BWRAP)

        admission._tool_reference = historical_tool
        receipt = admission.load_receipt(receipt_path_value, receipt_sha256,
                                         require_admitted=False, deep_image=False)
        reference = _reference(old_controls, receipt)
        _require_equal(reference, successor["old_runtime"], "historical receipt reference")
        return reference, receipt

    replay._gpu_successor_config = binding
    asr_v5_module.runtime_admission_reference = replay
    return True
