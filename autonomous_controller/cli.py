"""Command line boundary for the foreground autonomous controller."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Any

from .config import ConfigError, ControllerConfig, load_config
from .controller import AutonomousController, ControllerError
from .gpu_child import GpuChildError
from .public_status import PublicStatusError, read_public_status
from .sealed_backend import BackendError, SealedArchiveBackend
from .setup_archive_all_known import SetupError, setup_archive_all_known
from .setup_archive_successor import setup_archive_successor
from .state import (
    ControlStore,
    StateError,
    read_control_state,
    request_start,
    request_stop,
    utc_now,
)


def _emit(value: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def _run_exit_summary(
    *, config_id: str, returncode: int, public_status: dict[str, Any]
) -> dict[str, Any]:
    """Project the cached terminal state into one bounded console result object."""

    return {
        "status": "controller_exited" if returncode == 0 else "controller_faulted",
        "config_id": config_id,
        "returncode": returncode,
        "lifecycle": public_status.get("lifecycle"),
        "actual_state": public_status.get("actual_state"),
        "desired_state": public_status.get("desired_state"),
        "completion_reason": public_status.get("completion_reason"),
        "cycle": public_status.get("cycle"),
        "last_error": public_status.get("last_error"),
    }


def _deep_audit_checkpoint(
    config: ControllerConfig, *, incremental: bool = False, trust_rsync_copy: bool = False
) -> dict[str, Any]:
    """Deeply replay the stopped campaign and publish one exact-head checkpoint.

    The first store exists only to hold the same singleton run lock used by the
    foreground controller.  Reloading the store after lock acquisition prevents
    an exiting controller from leaving this command with a pre-lock journal view.
    A final reload detects any out-of-contract journal mutation during the audit.
    """

    if trust_rsync_copy and not incremental:
        raise StateError("trusted rsync copy requires checkpoint recovery")
    lock_store = ControlStore(config)
    with lock_store.run_lock():
        audit_store = ControlStore(config)
        initial_control = audit_store.read_control()
        if initial_control["desired_state"] != "stopped":
            raise StateError(
                "deep audit requires durable desired_state stopped"
            )
        events = audit_store.events
        if not events:
            raise StateError(
                "deep audit checkpoint requires a nonempty event journal"
            )
        anchor = events[-1]

        # Maintenance launches no acquisition, preprocess, GPU, or retention
        # stage. The trusted-copy option is explicit, never an automatic fallback.
        backend = SealedArchiveBackend(config)
        if incremental:
            view = audit_store.recovery_view()
            if view.checkpoint is None:
                raise StateError("recover-checkpoint requires an existing checkpoint")
            restore = (
                backend.restore_rsync_copy_checkpoint if trust_rsync_copy
                else backend.restore_checkpoint
            )
            recovery = restore(view.checkpoint["backend"], view.tail_events)
        else:
            recovery = backend.restore(events)
        backend_document = backend.export_checkpoint()
        if not isinstance(backend_document, dict):
            raise StateError(
                "deep audit backend checkpoint was unexpectedly deferred"
            )

        final_store = ControlStore(config)
        final_control = final_store.read_control()
        if final_control["desired_state"] != "stopped":
            raise StateError(
                "desired_state changed from stopped during deep audit"
            )
        final_events = final_store.events
        if (
            not final_events
            or final_events[-1]["sequence"] != anchor["sequence"]
            or final_events[-1]["event_sha256"] != anchor["event_sha256"]
        ):
            raise StateError("event journal changed during deep audit")
        checkpoint = final_store.write_checkpoint(
            backend_document,
            expected_anchor_sequence=anchor["sequence"],
            expected_anchor_sha256=anchor["event_sha256"],
        )

    restart = recovery.get("restart") if isinstance(recovery, dict) else None
    return {
        "status": "recovery_checkpointed" if incremental else "deep_audit_checkpointed",
        "config_id": config.config_id,
        "event_count": len(events),
        "anchor": checkpoint["anchor"],
        "checkpoint_sha256": checkpoint["checkpoint_sha256"],
        "checkpoint_created_at": checkpoint["created_at"],
        "recovery_mode": (
            restart.get("mode") if isinstance(restart, dict) else "deep_audit"
        ),
        "stages_launched": False,
        "recovery": recovery,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run or gracefully stop one sealed autonomous Archive campaign"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in (
        "run",
        "request-start",
        "request-stop",
        "status",
        "deep-audit-checkpoint",
        "recover-checkpoint",
    ):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--expected-config-sha256", required=True)
        if name == "recover-checkpoint":
            command.add_argument(
                "--trust-rsync-copy",
                action="store_true",
                help="trust completed copied media for the reviewed disk migration; still verify new results",
            )
    setup = commands.add_parser("setup-archive-all-known")
    setup.add_argument("--output", type=Path, required=True)
    successor = commands.add_parser("setup-archive-successor")
    successor.add_argument(
        "--composite-schedule-set-manifest", type=Path, required=True
    )
    successor.add_argument(
        "--expected-composite-schedule-set-manifest-sha256", required=True
    )
    successor.add_argument("--expected-inventory-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "setup-archive-all-known":
            _emit(setup_archive_all_known(args.output))
            return 0
        if args.command == "setup-archive-successor":
            _emit(
                setup_archive_successor(
                    composite_manifest_path=args.composite_schedule_set_manifest,
                    expected_composite_manifest_sha256=(
                        args.expected_composite_schedule_set_manifest_sha256
                    ),
                    expected_inventory_sha256=args.expected_inventory_sha256,
                )
            )
            return 0
        config = load_config(args.config, args.expected_config_sha256)
        if args.command == "request-start":
            control = request_start(config, requested_at=utc_now())
            _emit(
                {
                    "status": "start_requested",
                    "config_id": config.config_id,
                    "control_generation": control["generation"],
                    "semantics": "admit_one_outer_systemd_controller_after_this_commit",
                    "processing_started": False,
                }
            )
            return 0
        if args.command == "request-stop":
            control = request_stop(config, requested_at=utc_now())
            _emit(
                {
                    "status": "stop_requested",
                    "config_id": config.config_id,
                    "control_generation": control["generation"],
                    "semantics": "finish_current_finite_stage_then_exit",
                    "signal_or_cancellation_sent": False,
                }
            )
            return 0
        if args.command == "status":
            _emit(
                {
                    "status": "observed",
                    "config_id": config.config_id,
                    "control": read_control_state(config),
                    "controller": read_public_status(
                        args.config, args.expected_config_sha256
                    ),
                    "files_written": False,
                    "event_journal_replayed": False,
                }
            )
            return 0
        if args.command in {"deep-audit-checkpoint", "recover-checkpoint"}:
            _emit(
                _deep_audit_checkpoint(
                    config,
                    incremental=args.command == "recover-checkpoint",
                    trust_rsync_copy=getattr(args, "trust_rsync_copy", False),
                ),
                stream=sys.stdout,
            )
            return 0

        store = ControlStore(config)
        controller = AutonomousController(
            config, store, SealedArchiveBackend(config)
        )

        def request_graceful_stop(_signum: int, _frame: Any) -> None:
            store.set_desired_state("stopped", requested_at=utc_now())

        signal.signal(signal.SIGTERM, request_graceful_stop)
        signal.signal(signal.SIGINT, request_graceful_stop)
        returncode = controller.run()
        terminal_status = read_public_status(
            args.config, args.expected_config_sha256
        )
        _emit(
            _run_exit_summary(
                config_id=config.config_id,
                returncode=returncode,
                public_status=terminal_status,
            ),
            stream=sys.stdout if returncode == 0 else sys.stderr,
        )
        return returncode
    except (
        ConfigError,
        StateError,
        ControllerError,
        BackendError,
        GpuChildError,
        PublicStatusError,
        SetupError,
        OSError,
    ) as error:
        _emit(
            {
                "status": "failed",
                "error": {"type": type(error).__name__, "message": str(error)},
            },
            stream=sys.stderr,
        )
        return 2
    except Exception as error:
        # Runtime boundary failures can originate in SHA-pinned modules that are
        # loaded dynamically by the sealed backend, so their exception classes
        # cannot all be named in this module.  Keep the subprocess contract
        # strict even for those failures: one JSON object on stderr and no
        # interpreter traceback.
        _emit(
            {
                "status": "failed",
                "error": {"type": type(error).__name__, "message": str(error)},
            },
            stream=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
