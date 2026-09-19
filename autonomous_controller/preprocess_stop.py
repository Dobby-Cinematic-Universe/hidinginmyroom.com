"""Durable Stop checks at the sealed preprocess handoff's item boundary.

The reviewed handoff intentionally processes a bounded list of singleton files in
one invocation so its queue replay is amortized.  This adapter leaves that source
unchanged while making the private per-item function an explicit controller stop
boundary.  A file which has already started is allowed to finish and seal its
receipt; no following file starts after a durable Stop is observed.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator


class PreprocessStopError(RuntimeError):
    """The exact preprocess boundary could not be installed or reconciled."""


class PreprocessStopBoundary(Exception):
    """Private control-flow signal raised before the next preprocess item."""


@dataclass
class PreprocessStopObservation:
    """Evidence collected while the handoff owns its serialized writer lock."""

    completed: list[dict[str, Any]] = field(default_factory=list)
    attempted_queue_ordinals: list[int] = field(default_factory=list)
    boundary_queue_ordinal: int | None = None
    stop_requested_between_items: bool = False


_GATE_LOCK = threading.Lock()


def _queue_ordinal(row: Any, *, label: str) -> int:
    if not isinstance(row, dict):
        raise PreprocessStopError(f"{label} is not an object")
    ordinal = row.get("queue_ordinal")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
        raise PreprocessStopError(f"{label} has an invalid queue ordinal")
    return ordinal


@contextmanager
def preprocess_stop_gate(
    handoff: Any,
    *,
    desired_state: Callable[[], str],
) -> Iterator[PreprocessStopObservation]:
    """Wrap exactly one handoff module until its bounded invocation unwinds.

    The shared source module is process-global, so only one owner may patch the
    item function.  The wrapper validates both the selected input and returned
    receipt row before retaining a private copy for post-stop reconciliation.
    """

    original = getattr(handoff, "_materialize_and_run_one", None)
    if not callable(original):
        raise PreprocessStopError(
            "sealed preprocess handoff lacks its exact per-item boundary"
        )
    if not _GATE_LOCK.acquire(blocking=False):
        raise PreprocessStopError("a concurrent preprocess stop gate is forbidden")
    observed = PreprocessStopObservation()

    def gated(row: dict[str, Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
        ordinal = _queue_ordinal(row, label="selected preprocess row")
        state = desired_state()
        if state not in {"running", "stopped"}:
            raise PreprocessStopError(
                "durable controller state is neither running nor stopped"
            )
        if state == "stopped":
            observed.stop_requested_between_items = True
            observed.boundary_queue_ordinal = ordinal
            raise PreprocessStopBoundary(
                "durable Stop requested before the next preprocess item"
            )
        observed.attempted_queue_ordinals.append(ordinal)
        returned = original(row, *args, **kwargs)
        returned_ordinal = _queue_ordinal(
            returned, label="completed preprocess row"
        )
        if returned_ordinal != ordinal:
            raise PreprocessStopError(
                "preprocess item result changed its selected queue ordinal"
            )
        observed.completed.append(deepcopy(returned))
        return returned

    try:
        setattr(handoff, "_materialize_and_run_one", gated)
        if getattr(handoff, "_materialize_and_run_one", None) is not gated:
            raise PreprocessStopError("preprocess stop gate could not be installed")
    except Exception:
        try:
            setattr(handoff, "_materialize_and_run_one", original)
        finally:
            _GATE_LOCK.release()
        raise

    changed = False
    try:
        yield observed
    finally:
        changed = getattr(handoff, "_materialize_and_run_one", None) is not gated
        try:
            setattr(handoff, "_materialize_and_run_one", original)
        finally:
            _GATE_LOCK.release()
        if changed:
            raise PreprocessStopError(
                "sealed preprocess item boundary changed while the gate was active"
            )
