"""Restart-safe orchestration for the bounded private Archive pipeline.

The package deliberately owns operational scheduling only.  Acquisition results,
preprocess receipts, GPU materialization receipts, and cold-transfer receipts remain
the completion authority for their respective stages.
"""

from .config import ControllerConfig, ConfigError, load_config
from .controller import AutonomousController, ControllerError, StageOutcome
from .public_status import PublicStatusError, read_public_status
from .setup_archive_all_known import SetupError, setup_archive_all_known
from .state import (
    ControlStore,
    StateError,
    read_control_state,
    request_start,
    request_stop,
    set_desired_state_lightweight,
)

__all__ = [
    "AutonomousController",
    "ConfigError",
    "ControlStore",
    "ControllerConfig",
    "ControllerError",
    "PublicStatusError",
    "StageOutcome",
    "StateError",
    "SetupError",
    "load_config",
    "read_public_status",
    "read_control_state",
    "request_start",
    "request_stop",
    "set_desired_state_lightweight",
    "setup_archive_all_known",
]
