"""Stage one exact autonomous successor profile set without activating it.

The operator UI intentionally accepts exactly one ``autonomy.run`` profile and
one ``autonomy.request_stop`` profile.  This module replaces neither a running
console's pinned profile set nor the predecessor profile file.  It verifies that
the predecessor and successor controller configurations are both stopped, then
materializes a separate, immutable candidate that can be selected only by
restarting the console with ``--profiles``.
"""

from __future__ import annotations

import fcntl
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from autonomous_controller.config import ControllerConfig, load_config
from autonomous_controller.public_status import read_public_status
from autonomous_controller.state import read_control_state

from . import registry


SCHEMA_VERSION = 1
START_PROFILE_ID = "autonomy.start-all-known-archive"
STOP_PROFILE_ID = "autonomy.stop-all-known-archive"


class SuccessorProfileError(RuntimeError):
    """The staged profile would not be an exact, safely retired successor."""


def _canonical_bytes(value: Any) -> bytes:
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
        raise SuccessorProfileError(
            f"successor profile set is not canonical JSON: {error}"
        ) from error


def _repository_path(path: Path, repo_root: Path, label: str) -> Path:
    absolute = path if path.is_absolute() else (Path.cwd() / path).absolute()
    normalized = Path(os.path.abspath(absolute))
    if normalized != absolute or str(normalized) != str(absolute):
        raise SuccessorProfileError(f"{label} must be one normalized absolute path")
    try:
        normalized.relative_to(repo_root)
    except ValueError as error:
        raise SuccessorProfileError(f"{label} must remain below the repository") from error
    return normalized


def _profile_path_value(path: Path, repo_root: Path) -> str:
    try:
        relative = path.relative_to(repo_root)
    except ValueError as error:  # pragma: no cover - fenced by _repository_path
        raise SuccessorProfileError(
            "successor config must remain below the repository"
        ) from error
    return f"$REPO/{relative.as_posix()}"


def build_successor_profile_document(
    *, successor_config: Path, successor_config_sha256: str, repo_root: Path
) -> dict[str, Any]:
    """Return the closed two-button profile document for one successor config."""

    config_value = _profile_path_value(successor_config, repo_root)
    return {
        "schema_version": SCHEMA_VERSION,
        "profiles": [
            {
                "id": START_PROFILE_ID,
                "label": "Start",
                "description": (
                    "Start or resume the sealed unified Archive.org campaign, "
                    "including collection 699993."
                ),
                "action": "autonomy.run",
                "parameters": {
                    "config": config_value,
                    "expected_config_sha256": successor_config_sha256,
                },
            },
            {
                "id": STOP_PROFILE_ID,
                "label": "Stop",
                "description": (
                    "Request a graceful stop after the current bounded stage finishes."
                ),
                "action": "autonomy.request_stop",
                "parameters": {
                    "config": config_value,
                    "expected_config_sha256": successor_config_sha256,
                },
            },
        ],
    }


def _prepared_autonomy_binding(
    profile_set: registry.ProfileSet, repo_root: Path
) -> tuple[Path, str]:
    if len(profile_set.profiles) != 2:
        raise SuccessorProfileError(
            "autonomous profile set must contain exactly Start and Stop"
        )
    by_action: dict[str, registry.PreparedCommand] = {}
    for profile in profile_set.profiles:
        if profile.action_id not in {"autonomy.run", "autonomy.request_stop"}:
            raise SuccessorProfileError(
                "autonomous profile set contains an action other than Start or Stop"
            )
        if profile.action_id in by_action:
            raise SuccessorProfileError(
                f"autonomous profile set contains duplicate {profile.action_id} bindings"
            )
        try:
            prepared = registry.prepare_profile(
                profile_set, profile.profile_id, repo_root
            )
        except registry.RegistryError as error:
            raise SuccessorProfileError(
                f"autonomous profile {profile.profile_id!r} failed replay: {error}"
            ) from error
        by_action[profile.action_id] = prepared
    if set(by_action) != {"autonomy.run", "autonomy.request_stop"}:
        raise SuccessorProfileError(
            "autonomous profile set must bind exactly one Start and one Stop"
        )
    start = by_action["autonomy.run"].argv
    stop = by_action["autonomy.request_stop"].argv
    if (
        len(start) != 6
        or len(stop) != 6
        or start[1:3] != ("run", "--config")
        or stop[1:3] != ("request-stop", "--config")
        or start[4] != "--expected-config-sha256"
        or stop[4] != "--expected-config-sha256"
        or start[3] != stop[3]
        or start[5] != stop[5]
    ):
        raise SuccessorProfileError(
            "autonomous Start and Stop do not bind one exact controller config"
        )
    return Path(start[3]), start[5]


def _controller_lock_available(config: ControllerConfig, label: str) -> None:
    """Reject a controller that presently owns its exact run lock."""

    path = config.state_root / "controller.lock"
    try:
        inspected = path.lstat()
    except FileNotFoundError:
        return
    except OSError as error:
        raise SuccessorProfileError(f"cannot inspect {label} run lock: {error}") from error
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise SuccessorProfileError(f"cannot open {label} run lock: {error}") from error
    try:
        opened = os.fstat(descriptor)
        linked = path.lstat()
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
        ):
            raise SuccessorProfileError(
                f"{label} run lock is not an owner-private single-link file"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SuccessorProfileError(f"{label} controller is still active") from error
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        os.close(descriptor)


def _assert_quiescent_controller(
    config: ControllerConfig,
    *,
    label: str,
    allowed_actual_states: set[str],
) -> None:
    try:
        control = read_control_state(config)
        status = read_public_status(config.path, config.physical_sha256)
    except Exception as error:
        raise SuccessorProfileError(
            f"{label} controller state replay failed: {type(error).__name__}: {error}"
        ) from error
    if control.get("desired_state") != "stopped":
        raise SuccessorProfileError(f"{label} controller does not have durable Stop intent")
    if status.get("desired_state") != "stopped":
        raise SuccessorProfileError(f"{label} public status does not report Stop intent")
    if status.get("actual_state") not in allowed_actual_states:
        raise SuccessorProfileError(
            f"{label} controller is not quiescent: actual_state={status.get('actual_state')!r}"
        )
    execution = status.get("execution")
    if (
        not isinstance(execution, dict)
        or execution.get("accepting_new_work") is not False
        or execution.get("draining") is not False
        or execution.get("inflight_total") != 0
    ):
        raise SuccessorProfileError(f"{label} controller still reports in-flight work")
    if status.get("current_stage") is not None or status.get("current_gpu_child") is not None:
        raise SuccessorProfileError(f"{label} controller still reports an active stage")
    lanes = status.get("lanes")
    if not isinstance(lanes, dict) or any(
        not isinstance(lane, dict) or lane.get("active") != 0
        for lane in lanes.values()
    ):
        raise SuccessorProfileError(f"{label} controller lanes are not quiescent")
    _controller_lock_available(config, label)


def _write_candidate_no_replace(
    output: Path,
    body: bytes,
    *,
    repo_root: Path,
) -> registry.ProfileSet:
    if output.exists() or output.is_symlink():
        raise SuccessorProfileError("refusing to overwrite successor profile output")
    try:
        parent = output.parent.resolve(strict=True)
        parent_info = parent.lstat()
    except OSError as error:
        raise SuccessorProfileError(
            f"cannot inspect successor profile output parent: {error}"
        ) from error
    if (
        parent != output.parent
        or not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or stat.S_IMODE(parent_info.st_mode) & 0o022
    ):
        raise SuccessorProfileError(
            "successor profile output parent must be current-user and not peer-writable"
        )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".profiles.successor.tmp-", dir=parent
    )
    temporary = Path(temporary_name)
    linked = False
    try:
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise SuccessorProfileError("successor profile write made no progress")
            offset += written
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        # Deep registry replay happens before the candidate acquires its public name.
        candidate = registry.load_profiles(temporary)
        _prepared_autonomy_binding(candidate, repo_root)
        try:
            os.link(temporary, output)
            linked = True
        except FileExistsError as error:
            raise SuccessorProfileError(
                "refusing to overwrite successor profile output"
            ) from error
        temporary.unlink()
        directory = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        linked = False
        return registry.load_profiles(output)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        if linked:
            output.unlink(missing_ok=True)


def materialize_successor_profile_set(
    *,
    repo_root: Path,
    current_profiles: Path,
    expected_current_profiles_sha256: str,
    successor_config: Path,
    expected_successor_config_sha256: str,
    output: Path,
) -> dict[str, Any]:
    """Validate both campaigns and seal an inactive successor profile candidate."""

    root = repo_root.resolve(strict=True)
    current = _repository_path(current_profiles, root, "current profiles")
    successor_path = _repository_path(successor_config, root, "successor config")
    target = _repository_path(output, root, "successor profile output")
    if target == current:
        raise SuccessorProfileError(
            "successor staging never overwrites the active profile set"
        )
    if target.parent != current.parent or target.suffix != ".json":
        raise SuccessorProfileError(
            "successor profile output must be a JSON sibling of the active profile set"
        )
    if registry.SHA256_RE.fullmatch(expected_current_profiles_sha256) is None:
        raise SuccessorProfileError("expected current profile-set SHA-256 is invalid")
    try:
        current_set = registry.load_profiles(current)
    except registry.RegistryError as error:
        raise SuccessorProfileError(f"current profile replay failed: {error}") from error
    if current_set.raw_sha256 != expected_current_profiles_sha256:
        raise SuccessorProfileError(
            "current profile set differs from its reviewed SHA-256"
        )
    predecessor_path, predecessor_sha256 = _prepared_autonomy_binding(
        current_set, root
    )
    try:
        predecessor = load_config(predecessor_path, predecessor_sha256)
        successor = load_config(successor_path, expected_successor_config_sha256)
    except Exception as error:
        raise SuccessorProfileError(
            f"controller config replay failed: {type(error).__name__}: {error}"
        ) from error
    if (
        predecessor.config_id == successor.config_id
        or predecessor.physical_sha256 == successor.physical_sha256
        or predecessor.state_root == successor.state_root
    ):
        raise SuccessorProfileError(
            "successor must have a distinct config ID, digest, and state root"
        )
    _assert_quiescent_controller(
        predecessor, label="predecessor", allowed_actual_states={"stopped"}
    )
    _assert_quiescent_controller(
        successor,
        label="successor",
        allowed_actual_states={"not_started", "stopped"},
    )
    body = _canonical_bytes(
        build_successor_profile_document(
            successor_config=successor_path,
            successor_config_sha256=successor.physical_sha256,
            repo_root=root,
        )
    )
    staged = _write_candidate_no_replace(target, body, repo_root=root)
    staged_path, staged_digest = _prepared_autonomy_binding(staged, root)
    if staged_path != successor_path or staged_digest != successor.physical_sha256:
        raise SuccessorProfileError(
            "sealed successor profile does not replay to the reviewed config"
        )
    return {
        "schema_version": 1,
        "status": "successor_profile_set_staged",
        "profile_set": str(target),
        "profile_set_sha256": staged.raw_sha256,
        "profile_count": len(staged.profiles),
        "successor_config": str(successor_path),
        "successor_config_sha256": successor.physical_sha256,
        "predecessor_config": str(predecessor_path),
        "predecessor_config_sha256": predecessor.physical_sha256,
        "active_profile_set_changed": False,
        "processing_started": False,
        "activation": "restart_console_with_staged_profile_set",
    }


__all__ = [
    "SuccessorProfileError",
    "build_successor_profile_document",
    "materialize_successor_profile_set",
]
