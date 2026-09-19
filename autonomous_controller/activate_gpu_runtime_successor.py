"""Explicit, stopped-only activation of an additive GPU runtime authorization.

No service control, checkpoint rewrite, media read, result replay, or restart is
performed. The fully anchored metadata checkpoint must already show drained GPU
work. Uncheckpointed activity or orphan materialization receipts fail closed.
"""

from __future__ import annotations

import argparse
import copy
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace
from typing import Any, Iterator

from pipeline import hybrid_legacy_guard as legacy

from .config import ControllerConfig, canonical_bytes, load_config
from . import gpu_runtime_successor as successor
from .quarantine_recovery import _guards
from .sealed_backend import SealedArchiveBackend
from .state import ControlStore, RecoveryView, read_control_state


class ActivationError(RuntimeError):
    """Stopped generation, checkpoint, or execution exclusion is not exact."""


INERT_BWRAP_FAILURE = (
    "GPU source materialization for packed batch failed: local-private-production "
    "runtime replay failed: runtime admission replay failed: system tool "
    "bubblewrap SHA-256 differs from its reference"
)


def _validate_activation_documents(control: dict, status: dict, companion: dict) -> None:
    """Allow only this recovery's inert fault; never mutate the status document."""
    lanes = status.get("lanes")
    lane = lanes.get("gpu_readiness") if isinstance(lanes, dict) else None
    if isinstance(lane, dict) and lane.get("state") == "faulted":
        stage = status.get("stages", {}).get("gpu_readiness")
        if (lane.get("wait_reason") != INERT_BWRAP_FAILURE
                or lane.get("dispatch_id") is not None or lane.get("started_at") is not None
                or lane.get("last_status") != "held" or not isinstance(stage, dict)
                or stage.get("stop_reconciled") is not True
                or any(type(stage.get(key)) is not int or stage[key] != 0 for key in
                       ("active_children", "pending_batches", "pending_items",
                        "parked_batches", "parked_items", "buffered_ready_items"))):
            raise ActivationError("faulted GPU lane is not the exact inert, reconciled bubblewrap failure")
        # Existing strict validation still checks all activity, controller PID,
        # companion state, schemas, counts and every other lane. Only this local
        # copy's inert display state is translated; raw bytes remain the witness.
        status = copy.deepcopy(status)
        status["lanes"]["gpu_readiness"]["state"] = "stopped"
    legacy._validate_documents(control, status, companion)


def _activation_snapshot(guards: list[dict[str, str]]) -> list[dict[str, Any]]:
    snapshots = []
    for guard in guards:
        control = legacy._document(Path(guard["control_path"]), 32 * 1024)
        status = legacy._document(Path(guard["status_path"]))
        companion = legacy._document(Path(guard["companion_status_path"]))
        _validate_activation_documents(control, status, companion)
        locks = {key: list(legacy._fingerprint(legacy._stat_lock(Path(guard[key]))))
                 for key in ("controller_lock", "companion_lock")}
        snapshots.append({"paths": guard, "control": control, "status": status,
                          "companion": companion, "locks": locks})
    return snapshots


@contextmanager
def hold_legacy(guards: list[dict[str, str]]) -> Iterator[Any]:
    """Activation-only dual exclusion; the general hybrid guard stays strict."""
    normalized = legacy._normalize(guards)
    initial = legacy._witness(_activation_snapshot(normalized))
    locks = []
    active = True
    try:
        for path in sorted(Path(row[key]) for row in normalized
                           for key in ("controller_lock", "companion_lock")):
            with legacy._parent(path) as parent:
                descriptor = os.open(path.name, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                                     dir_fd=parent)
                try:
                    info = os.fstat(descriptor)
                    legacy._file_safe(info, lock=True)
                    expected = legacy._fingerprint(info)
                    if expected != legacy._fingerprint(os.stat(path.name, dir_fd=parent, follow_symlinks=False)):
                        raise ActivationError("legacy execution lock changed")
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BaseException:
                    os.close(descriptor)
                    raise
            locks.append((path, descriptor, expected))

        def check_unchanged() -> None:
            if not active:
                raise ActivationError("activation execution leases are not held")
            for path, descriptor, expected in locks:
                if (legacy._fingerprint(os.fstat(descriptor)) != expected
                        or legacy._fingerprint(legacy._stat_lock(path)) != expected):
                    raise ActivationError("activation execution lease changed")
            if legacy._witness(_activation_snapshot(normalized)) != initial:
                raise ActivationError("raw legacy state changed during runtime activation")

        check_unchanged()
        yield SimpleNamespace(check_unchanged=check_unchanged)
    finally:
        active = False
        for _path, descriptor, _expected in reversed(locks):
            os.close(descriptor)


@contextmanager
def _existing_control_lock(config: ControllerConfig) -> Iterator[None]:
    """Prevent an operator start between the last check and sidecar publication."""
    path = config.state_root / "control.lock"
    parent = successor._parent_fd(path)
    descriptor = None
    try:
        descriptor = os.open(path.name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=parent)
        opened = os.fstat(descriptor)
        linked = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or opened.st_uid != os.geteuid() or stat.S_IMODE(opened.st_mode) != 0o600
                or successor._witness(opened) != successor._witness(linked)):
            raise ActivationError("existing controller control lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _read_only_store(config: ControllerConfig) -> ControlStore:
    # ControlStore normally creates these on first setup. Activation is never setup.
    for name in ("events", "gpu-children"):
        info = (config.state_root / name).lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            raise ActivationError("activation requires existing private controller journals")
    return ControlStore(config)


def _drained_gpu(config: ControllerConfig, view: RecoveryView) -> dict[str, Any]:
    if view.checkpoint is None or view.legacy_full_replay:
        raise ActivationError("activation requires a validated drained checkpoint; no full replay is permitted")
    if view.tail_events:
        raise ActivationError("checkpoint has a journal tail; establish a fresh drained checkpoint before activation")
    # This existing validator checks only the complete backend envelope/identity.
    # Do not construct a backend or invoke its queue/media replay machinery.
    backend, _queue = SealedArchiveBackend._validated_backend_checkpoint_envelope(
        SimpleNamespace(config=config), view.checkpoint["backend"])
    rows = backend["gpu"]["records"]
    records: dict[str, dict[str, Any]] = {}
    receipts: set[str] = set()
    completed = items = not_applicable = 0
    fields = {"record", "status", "item_count", "claims", "dispositions", "parked", "files"}
    for row in rows:
        if not isinstance(row, dict) or set(row) != fields:
            raise ActivationError("GPU checkpoint row is malformed")
        record = row["record"]
        if not isinstance(record, dict) or not isinstance(record.get("batch_key"), str) or not record["batch_key"]:
            raise ActivationError("GPU checkpoint record identity is malformed")
        if record["batch_key"] in records:
            raise ActivationError("GPU checkpoint contains duplicate record identities")
        records[record["batch_key"]] = record
        count = row["item_count"]
        if type(count) is not int or count < 0 or row["parked"] is not None:
            raise ActivationError("GPU checkpoint has malformed or parked work")
        if row["status"] == "not_applicable":
            if count != 0 or record.get("record_kind") != "no_ready_members" or record.get("batch") is not None:
                raise ActivationError("GPU not-applicable row contradicts its batch")
            not_applicable += 1
            continue
        if row["status"] != "completed" or count < 1 or record.get("record_kind") != "ready_batch":
            raise ActivationError("nonterminal historical GPU work remains; review/rematerialize it before activation")
        completed += 1
        items += count
        sources = record.get("sources")
        if sources is None:
            sources = [record]
        if not isinstance(sources, list) or not sources:
            raise ActivationError("completed GPU batch lacks materialization provenance")
        for source in sources:
            reference = source.get("materialization_receipt") if isinstance(source, dict) else None
            if not isinstance(reference, dict) or not isinstance(reference.get("path"), str):
                raise ActivationError("completed GPU batch lacks a materialization receipt")
            receipts.add(str(successor._path(reference["path"])))
    return {"checkpoint_sha256": view.checkpoint["checkpoint_sha256"],
            "anchor_sequence": view.anchor_sequence, "completed_batches": completed,
            "completed_items": items, "not_applicable_records": not_applicable,
            "materialization_receipts": receipts}


def _check_materialization_inventory(config: ControllerConfig, expected: set[str]) -> None:
    """Metadata-only check for durable materializations absent from checkpoint."""
    directory = successor._path(config.section("gpu_readiness")["receipt_root"]) / "materializations"
    if any(Path(value).parent != directory for value in expected):
        raise ActivationError("checkpoint materialization provenance leaves its configured root")
    # Anchor the directory through the same no-symlink traversal used for controls.
    descriptor = successor._parent_fd(directory / "inventory-placeholder")
    try:
        info = os.fstat(descriptor)
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ActivationError("materialization inventory root is not private")
        observed = set()
        with os.scandir(descriptor) as entries:
            for entry in entries:
                path = str(directory / entry.name)
                info = entry.stat(follow_symlinks=False)
                if (path not in expected or not stat.S_ISREG(info.st_mode)
                        or info.st_nlink != 1 or info.st_uid != os.geteuid()
                        or stat.S_IMODE(info.st_mode) != 0o400):
                    raise ActivationError("uncheckpointed or unsafe GPU materialization requires review")
                observed.add(path)
        if observed != expected:
            raise ActivationError("checkpoint GPU materialization receipt is missing")
    finally:
        os.close(descriptor)


def activate(config: ControllerConfig, new_controls: dict[str, str], *,
             expected_generation: int, check_only: bool = False) -> dict[str, Any]:
    if type(expected_generation) is not int or not 0 <= expected_generation < 2**63:
        raise ActivationError("expected control generation must be an exact nonnegative integer")
    with hold_legacy(_guards(config)) as held, _existing_control_lock(config):
        def assert_stopped() -> None:
            held.check_unchanged()
            control = read_control_state(config)
            if control["desired_state"] != "stopped" or control["generation"] != expected_generation:
                raise ActivationError("controller stopped generation differs from the expected value")

        assert_stopped()
        store = _read_only_store(config)
        summary = _drained_gpu(config, store.recovery_view())
        _check_materialization_inventory(config, summary.pop("materialization_receipts"))
        assert_stopped()
        authorization = (successor.build_successor(config, new_controls) if check_only else
                         successor.stage_successor(config, new_controls, assert_stopped=assert_stopped))
        assert_stopped()
        return {"status": "validated_only" if check_only else "staged",
                "config_id": config.config_id, "control_generation": expected_generation,
                "authorization_id": authorization["authorization_id"],
                "authorization_identity_sha256": authorization["identity_sha256"],
                "sidecar_sha256": hashlib.sha256(canonical_bytes(authorization)).hexdigest(),
                "sidecar_path": str(config.path.parent / successor.SIDECAR_NAME),
                "gpu_checkpoint": summary, "services_started": False}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--controls-directory", required=True, type=Path)
    parser.add_argument("--runtime-sha256", required=True)
    parser.add_argument("--launcher-profile-sha256", required=True)
    parser.add_argument("--readiness-sha256", required=True)
    parser.add_argument("--expected-control-generation", required=True, type=int)
    parser.add_argument("--check-only", action="store_true", help="hold guards and validate without publishing")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(args.config, args.config_sha256)
        directory = successor._path(args.controls_directory)
        controls = {"runtime_admission": str(directory / "runtime-candidate-v2.json"),
                    "runtime_admission_sha256": args.runtime_sha256,
                    "launcher_profile": str(directory / "launcher-profile-v2.json"),
                    "launcher_profile_sha256": args.launcher_profile_sha256,
                    "local_readiness": str(directory / "readiness-v1.json"),
                    "local_readiness_sha256": args.readiness_sha256,
                    "local_launcher": str(directory / "trusted-launcher-v2")}
        result = activate(config, controls, expected_generation=args.expected_control_generation,
                          check_only=args.check_only)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except Exception as error:
        print(f"GPU runtime activation refused: {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
