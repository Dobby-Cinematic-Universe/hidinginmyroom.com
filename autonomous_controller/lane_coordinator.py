"""Independent finite-stage lanes for the autonomous controller.

The coordinator deliberately leaves acquisition on its calling thread.  The
sealed acquisition adapter uses POSIX process alarms and therefore must run on
the process main thread in production.  The remaining finite stages run in one
worker each, so a long download cannot delay preprocessing or GPU supervision.

Only backends which explicitly implement the lane protocol are admitted here.
Legacy and simple test backends continue to use the controller's sequential
cycle scheduler.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from .controller import CheckpointAnchor, ControllerError, STAGES, StageOutcome


ITEM_LIMITS: dict[str, int | None] = {
    # The sealed producer amortizes an expensive completed-media replay across
    # its configured (currently eight-item) bounded call.  Its backend installs
    # a durable-control gate between individual queue dispatches instead of
    # reducing this to one item and multiplying physical reads.
    "acquisition": None,
    # The handoff implementation emits singleton bundles internally but scans the
    # acknowledged queue snapshot once per invocation.  Preserve the sealed
    # max-items batch so that scan is amortized across the batch.
    "preprocess": None,
    "gpu_readiness": None,
    "cold_retention": 1,
}

# A downstream completion is fresh only after that lane has observed every
# progressed upstream generation on which it depends.
DEPENDENCY_TARGETS: dict[str, tuple[str, ...]] = {
    "acquisition": ("preprocess", "gpu_readiness", "cold_retention"),
    "preprocess": ("gpu_readiness",),
    "gpu_readiness": (),
    "cold_retention": (),
}


class LaneBackend(Protocol):
    """One stage-confined backend fork.

    ``observe_peer_outcome`` is invoked only by this lane's own execution
    thread, immediately before its next finite stage call.  Implementations do
    not need to make a mutable lane cache safe for concurrent observation.
    """

    def run_stage(
        self, stage: str, *, item_limit: int | None = None
    ) -> StageOutcome: ...

    def observe_peer_outcome(self, outcome: StageOutcome) -> None: ...


class ForkingBackend(Protocol):
    """Backend capability which opts into independent scheduling."""

    def fork_lane(self, stage: str) -> LaneBackend: ...

    def observe_peer_outcome(self, outcome: StageOutcome) -> None: ...


@dataclass(frozen=True)
class LaneRunResult:
    reason: str
    dispatch_count: int
    terminal_disposition: str | None


def supports_independent_lanes(backend: Any) -> bool:
    """Return true only for an explicit complete opt-in surface."""

    return all(
        callable(getattr(backend, name, None))
        for name in ("fork_lane", "observe_peer_outcome")
    )


class IndependentLaneCoordinator:
    """Run four finite stage lanes without a global cycle barrier."""

    def __init__(
        self,
        backend: ForkingBackend,
        *,
        desired_state: Callable[[], str],
        on_outcome: Callable[[str, StageOutcome, int], CheckpointAnchor | None],
        on_lane_state: Callable[[str, dict[str, Any]], None],
        terminal_disposition: Callable[[], str | None],
        now: Callable[[], str],
        idle_seconds: float,
        maximum_dispatches: int | None = None,
        on_gpu_quiesce: Callable[[StageOutcome], CheckpointAnchor | None]
        | None = None,
        on_checkpoint: Callable[[CheckpointAnchor], bool | None] | None = None,
        on_checkpoint_poll: Callable[[], bool | None] | None = None,
        checkpoint_deferred: Callable[[], bool] | None = None,
        on_forced_checkpoint: Callable[[CheckpointAnchor], bool | None] | None = None,
        on_lane_fault: Callable[[str, BaseException], None] | None = None,
        on_lane_recovered: Callable[[str], None] | None = None,
    ):
        if maximum_dispatches is not None and (
            isinstance(maximum_dispatches, bool)
            or not isinstance(maximum_dispatches, int)
            or maximum_dispatches < 1
        ):
            raise ControllerError("maximum dispatches must be a positive integer")
        if idle_seconds <= 0:
            raise ControllerError("lane idle interval must be positive")
        self.backend = backend
        self.desired_state = desired_state
        self.on_outcome = on_outcome
        self.on_lane_state = on_lane_state
        self.terminal_disposition = terminal_disposition
        self.now = now
        self.idle_seconds = idle_seconds
        self.maximum_dispatches = maximum_dispatches
        self.on_gpu_quiesce = on_gpu_quiesce or (lambda _outcome: None)
        self.on_checkpoint = on_checkpoint or (lambda _anchor: None)
        self.on_checkpoint_poll = on_checkpoint_poll or (lambda: False)
        self.checkpoint_deferred = checkpoint_deferred or (lambda: False)
        self.on_forced_checkpoint = on_forced_checkpoint or self.on_checkpoint
        self.on_lane_fault = on_lane_fault or (lambda _stage, _error: None)
        self.on_lane_recovered = on_lane_recovered or (lambda _stage: None)
        self._condition = threading.Condition(threading.RLock())
        self._root_observer_lock = threading.Lock()
        # Once a journal, root or checkpoint boundary raises, the aggregate may have
        # been partially mutated.  In-flight peers may still return and must be
        # journaled for exact retry, but this root can never authorize another
        # checkpoint.
        self._root_checkpoint_poisoned = False
        # This is not a recurring global cycle barrier.  It is requested only
        # after a due export proves that logical shared queue authority is ahead
        # of the journal-observed root.  New work then pauses while already-
        # admitted finite calls and peer observations reach their complete
        # boundaries.
        self._checkpoint_drain_requested = self._checkpoint_deferred_state()
        self._checkpoint_drain_exporting = False
        self._lanes: dict[str, LaneBackend] = {}
        self._pending_peer: dict[str, list[StageOutcome]] = {
            stage: [] for stage in STAGES
        }
        self._active = {stage: False for stage in STAGES}
        self._peer_observer_active = {stage: False for stage in STAGES}
        self._last_status: dict[str, str | None] = {stage: None for stage in STAGES}
        self._required_generation = {stage: 0 for stage in STAGES}
        self._applied_generation = {stage: 0 for stage in STAGES}
        self._completion_generation = {stage: -1 for stage in STAGES}
        self._dispatch_count = 0
        self._fault: BaseException | None = None
        self._fault_stage: str | None = None
        self._secondary_faults: list[tuple[str, BaseException]] = []
        self._reason: str | None = None
        self._terminal: str | None = None
        self._threads: list[threading.Thread] = []
        for stage in STAGES:
            lane = backend.fork_lane(stage)
            if not callable(getattr(lane, "run_stage", None)) or not callable(
                getattr(lane, "observe_peer_outcome", None)
            ):
                raise ControllerError(
                    f"independent {stage} lane does not implement the exact lane protocol"
                )
            self._lanes[stage] = lane

    @property
    def gpu_backend(self) -> LaneBackend:
        """The fork which owns exact GPU child supervision state."""

        return self._lanes["gpu_readiness"]

    @property
    def secondary_failures(self) -> tuple[tuple[str, BaseException], ...]:
        """Failures observed while reporting or containing the primary fault."""

        with self._condition:
            return tuple(self._secondary_faults)

    @property
    def primary_fault_stage(self) -> str | None:
        """The lane which won the coordinator's first-fault linearization."""

        with self._condition:
            return self._fault_stage

    def _publish(
        self,
        stage: str,
        state: str,
        *,
        active: bool,
        dispatch_id: int | None = None,
        started_at: str | None = None,
        status: str | None = None,
        wait_reason: str | None = None,
    ) -> None:
        self.on_lane_state(
            stage,
            {
                "state": state,
                "active": int(active),
                "limit": 1,
                "dispatch_id": dispatch_id,
                "started_at": started_at,
                "last_status": status,
                "wait_reason": wait_reason,
                "last_transition_at": self.now(),
            },
        )

    def _stopping_locked(self) -> bool:
        return (
            self._reason is not None
            or self._fault is not None
            or self.desired_state() != "running"
        )

    def _checkpoint_deferred_state(self) -> bool:
        value = self.checkpoint_deferred()
        if type(value) is not bool:
            raise ControllerError(
                "checkpoint deferred callback must return a boolean"
            )
        return value

    def _refresh_checkpoint_drain_request(self) -> None:
        """Mirror the controller's exact due-export deferral into dispatch flow."""

        deferred = self._checkpoint_deferred_state()
        with self._condition:
            if deferred:
                self._checkpoint_drain_requested = True
            elif (
                self._checkpoint_drain_requested
                and not self._checkpoint_drain_exporting
            ):
                # A later serialized outcome may naturally bring root authority
                # level with the shared store and write the pending checkpoint.
                self._checkpoint_drain_requested = False
            self._condition.notify_all()

    def _retry_checkpoint_after_drain(self) -> None:
        """Publish one pending checkpoint after every mutator reaches a boundary."""

        with self._condition:
            if (
                not self._checkpoint_drain_requested
                or self._checkpoint_drain_exporting
                or self._stopping_locked()
                or any(self._active.values())
                or any(self._peer_observer_active.values())
            ):
                return
            self._checkpoint_drain_exporting = True
        try:
            # No stage call or peer observer is active and the requested drain
            # prevents another from starting.  Reuse the journal/root serializer
            # so the pending anchor cannot race a new durable outcome.
            with self._root_observer_lock:
                try:
                    if self._root_checkpoint_poisoned:
                        raise ControllerError(
                            "cannot checkpoint a poisoned journal-observed root"
                        )
                    written = self.on_checkpoint_poll()
                    still_deferred = self._checkpoint_deferred_state()
                    if written is not True or still_deferred:
                        raise ControllerError(
                            "checkpoint export remained deferred at a drained lane boundary"
                        )
                except BaseException:
                    # Publish poison before releasing the serializer.  A GPU
                    # quiesce observer can race a durable Stop/fault and uses the
                    # same lock; it must never see a failed checkpoint boundary
                    # as still checkpoint-authoritative.
                    self._root_checkpoint_poisoned = True
                    raise
        except BaseException:
            with self._condition:
                self._checkpoint_drain_exporting = False
                self._condition.notify_all()
            raise
        else:
            with self._condition:
                self._checkpoint_drain_exporting = False
                self._checkpoint_drain_requested = False
                self._condition.notify_all()

    def _wait_for_checkpoint_drain(self) -> bool:
        """Wait without holding the root serializer; Stop and faults escape."""

        while True:
            self._retry_checkpoint_after_drain()
            with self._condition:
                if self._stopping_locked():
                    return False
                if not self._checkpoint_drain_requested:
                    return True
                # Durable Stop is external to this condition.  A bounded wait
                # observes it even if no in-process lane transition notifies us.
                self._condition.wait(timeout=self.idle_seconds)

    def _begin(self, stage: str) -> tuple[int, str, int] | None:
        while True:
            if not self._wait_for_checkpoint_drain():
                return None
            with self._condition:
                # A peer can request the drain between the wait and this lock.
                # Retry rather than admitting work across that boundary.
                if self._checkpoint_drain_requested:
                    continue
                if self._stopping_locked():
                    return None
                if (
                    self.maximum_dispatches is not None
                    and self._dispatch_count >= self.maximum_dispatches
                ):
                    self._reason = "limit"
                    self._condition.notify_all()
                    return None
                self._dispatch_count += 1
                dispatch_id = self._dispatch_count
                self._active[stage] = True
                started_at = self.now()
                dependency_generation = self._applied_generation[stage]
                break
        self._publish(
            stage,
            "running",
            active=True,
            dispatch_id=dispatch_id,
            started_at=started_at,
            status=self._last_status[stage],
        )
        return dispatch_id, started_at, dependency_generation

    def _apply_peer_outcomes(self, stage: str) -> bool:
        while True:
            if not self._wait_for_checkpoint_drain():
                return False
            with self._condition:
                if self._checkpoint_drain_requested:
                    continue
                if self._stopping_locked():
                    return False
                pending = self._pending_peer[stage]
                self._pending_peer[stage] = []
                if pending:
                    self._peer_observer_active[stage] = True
                break
        if not pending:
            return True
        succeeded = False
        try:
            for outcome in pending:
                self._lanes[stage].observe_peer_outcome(outcome)
            applied = sum(
                outcome.progressed and stage in DEPENDENCY_TARGETS[outcome.stage]
                for outcome in pending
            )
            if applied:
                with self._condition:
                    self._applied_generation[stage] += applied
                    if (
                        self._applied_generation[stage]
                        > self._required_generation[stage]
                    ):
                        raise ControllerError(
                            f"{stage} applied a dependency generation that was never required"
                        )
            succeeded = True
        finally:
            with self._condition:
                self._peer_observer_active[stage] = False
                self._condition.notify_all()
        if succeeded:
            self._retry_checkpoint_after_drain()
        return True

    def _record_secondary_fault(self, context: str, error: BaseException) -> None:
        with self._condition:
            failure = (context, error)
            if failure not in self._secondary_faults:
                # Status is bounded, so retain only a small diagnostic tail.
                self._secondary_faults = [*self._secondary_faults[-14:], failure]
            self._condition.notify_all()

    def _notify_fault(self, stage: str, error: BaseException) -> None:
        # This notification is deliberately status-only.  Durable event
        # admission remains on the controller thread after every lane has
        # reached its finite boundary, preserving journal order if a slow
        # acquisition call is still in flight.
        try:
            self.on_lane_fault(stage, error)
        except BaseException as report_error:
            # Observability must never replace the triggering lane failure.
            self._record_secondary_fault(
                f"{stage}_fault_status", report_error
            )

    def _record_fault(
        self,
        stage: str,
        error: BaseException,
        *,
        notify: bool = True,
        secondary_context: str | None = None,
        clear_active: bool = False,
    ) -> bool:
        first_fault = False
        with self._condition:
            if clear_active:
                self._active[stage] = False
            if self._fault is None:
                self._fault = error
                self._fault_stage = stage
                first_fault = True
            self._condition.notify_all()
        if first_fault and notify:
            self._notify_fault(stage, error)
        elif not first_fault:
            self._record_secondary_fault(
                secondary_context or f"{stage}_lane", error
            )
        return first_fault

    def _publish_fault(
        self, stage: str, error: BaseException, *, active: bool
    ) -> None:
        self._publish(
            stage,
            "faulted",
            active=active,
            status=self._last_status[stage],
            wait_reason=str(error)[:512],
        )

    def _complete(
        self,
        stage: str,
        outcome: StageOutcome,
        dispatch_id: int,
        started_at: str,
        dependency_generation: int,
    ) -> None:
        if not isinstance(outcome, StageOutcome) or outcome.stage != stage:
            observed = outcome.stage if isinstance(outcome, StageOutcome) else type(outcome).__name__
            raise ControllerError(
                f"independent {stage} lane returned invalid outcome {observed!r}"
            )
        outcome.normalized()
        # Journal admission, root observation, and checkpoint publication are one
        # exact serial order.  Keeping only root observation under this lock would
        # allow lane A to append before lane B but observe after it, producing a
        # checkpoint whose journal anchor claims state the root has not applied.
        # Peer forks still receive observations on their own stage threads below.
        with self._root_observer_lock:
            try:
                checkpoint_anchor = self.on_outcome(stage, outcome, dispatch_id)
                if not self._root_checkpoint_poisoned:
                    # An elapsed checkpoint may only describe the previously
                    # admitted root. Poll before an unjournaled outcome is allowed
                    # to refresh root runtime caches.
                    if checkpoint_anchor is None:
                        self.on_checkpoint_poll()
                        self._refresh_checkpoint_drain_request()
                    self.backend.observe_peer_outcome(outcome)
                    if checkpoint_anchor is not None:
                        self.on_checkpoint(checkpoint_anchor)
                        self._refresh_checkpoint_drain_request()
            except BaseException:
                self._root_checkpoint_poisoned = True
                raise

        with self._condition:
            self._active[stage] = False
            self._last_status[stage] = outcome.status
            self._completion_generation[stage] = dependency_generation
            if outcome.progressed:
                for peer in STAGES:
                    if peer != stage:
                        self._pending_peer[peer].append(outcome)
                for dependent in DEPENDENCY_TARGETS[stage]:
                    self._required_generation[dependent] += 1
            if self.desired_state() != "running" and self._reason is None:
                self._reason = "desired_stop"
            if (
                self._reason is None
                and self._fault is None
                and not any(self._active.values())
                and all(value in {"complete", "skipped"} for value in self._last_status.values())
                and all(
                    self._completion_generation[value]
                    == self._required_generation[value]
                    for value in STAGES
                )
            ):
                terminal = self.terminal_disposition()
                if terminal is not None:
                    self._terminal = terminal
                    self._reason = "terminal"
            self._condition.notify_all()

        self._retry_checkpoint_after_drain()

        waiting = self._reason is None and not outcome.progressed
        self._publish(
            stage,
            "waiting" if waiting else "idle",
            active=False,
            dispatch_id=dispatch_id,
            started_at=started_at,
            status=outcome.status,
            wait_reason=(
                outcome.monitor.get("stop_reason")
                if waiting and isinstance(outcome.monitor, dict)
                else None
            ),
        )
        # A previous retry fault is recovered only after the same lane has
        # crossed every success boundary: stage return validation, durable
        # outcome admission, root observation, dependency publication, and the
        # final externally visible lane state.  Serialize this decision against
        # first-fault admission so an in-flight peer cannot erase a fault which
        # has already won the coordinator.
        with self._condition:
            if self._fault is None:
                self.on_lane_recovered(stage)

    def _wait(self, stage: str) -> None:
        with self._condition:
            if self._stopping_locked() or self._pending_peer[stage]:
                return
            self._condition.wait(timeout=self.idle_seconds)

    def _quiesce_gpu_owner(self) -> StageOutcome | None:
        """Stop an exact GPU child on its owner without invoking observers.

        The returned outcome can be admitted only after any already-returned
        finite stage outcome.  This keeps the durable quiesce event as the final
        GPU authority without allowing observer I/O to delay exact containment.
        """

        backend = self._lanes["gpu_readiness"]
        quiesce = getattr(backend, "quiesce", None)
        if not callable(quiesce):
            return
        outcome = quiesce()
        if outcome is not None and (
            not isinstance(outcome, StageOutcome)
            or outcome.stage != "gpu_readiness"
        ):
            raise ControllerError("GPU lane quiesce returned an invalid outcome")
        if isinstance(outcome, StageOutcome):
            outcome.normalized()
        return outcome

    def _observe_gpu_quiesce(self, outcome: StageOutcome | None) -> None:
        """Publish a proven owner quiesce after earlier GPU authority."""

        if isinstance(outcome, StageOutcome):
            with self._condition:
                self._last_status["gpu_readiness"] = outcome.status
            # A material quiesce is another GPU authority transition.  Admit it
            # through the same journal -> root -> checkpoint serialization as a
            # normal lane outcome; otherwise a later checkpoint could anchor past
            # a quiesce which exists only on the stage-confined GPU fork.
            with self._root_observer_lock:
                try:
                    checkpoint_anchor = self.on_gpu_quiesce(outcome)
                    if (
                        checkpoint_anchor is not None
                        and not self._root_checkpoint_poisoned
                    ):
                        self.backend.observe_peer_outcome(outcome)
                        self.on_forced_checkpoint(checkpoint_anchor)
                except BaseException:
                    self._root_checkpoint_poisoned = True
                    raise

    def _quiesce_gpu_lane(self) -> None:
        """Contain the GPU owner before publishing its quiesce outcome."""

        self._observe_gpu_quiesce(self._quiesce_gpu_owner())

    def _lane_loop(self, stage: str) -> None:
        lane_fault: BaseException | None = None
        notify_after_quiesce = False
        gpu_quiesced_before_complete = False
        gpu_quiesce_outcome: StageOutcome | None = None
        gpu_quiesce_observation_pending = False
        gpu_quiesce_observer_failed = False
        try:
            while True:
                with self._condition:
                    if self._stopping_locked():
                        return
                if not self._apply_peer_outcomes(stage):
                    return
                binding = self._begin(stage)
                if binding is None:
                    return
                dispatch_id, started_at, dependency_generation = binding
                outcome = self._lanes[stage].run_stage(
                    stage, item_limit=ITEM_LIMITS[stage]
                )
                if stage == "gpu_readiness":
                    with self._condition:
                        containment_required = self._stopping_locked()
                    if containment_required:
                        # A peer can fault or request stop while the finite GPU
                        # call is in flight.  Once that call returns, its owner
                        # must contain the exact child before outcome admission,
                        # root observation, or lane-status I/O can block.
                        gpu_quiesce_outcome = self._quiesce_gpu_owner()
                        gpu_quiesced_before_complete = True
                        gpu_quiesce_observation_pending = True
                self._complete(
                    stage,
                    outcome,
                    dispatch_id,
                    started_at,
                    dependency_generation,
                )
                if gpu_quiesce_observation_pending:
                    # The finite return is admitted first; quiescence then
                    # becomes the final durable/monitor authority.  Clear the
                    # pending bit before invoking the callback so a partial
                    # callback failure cannot duplicate an admitted event.
                    gpu_quiesce_observation_pending = False
                    try:
                        self._observe_gpu_quiesce(gpu_quiesce_outcome)
                    except BaseException:
                        gpu_quiesce_observer_failed = True
                        raise
                if not outcome.progressed:
                    self._wait(stage)
        except BaseException as error:
            lane_fault = error
            # A GPU-origin fault cannot perform status I/O before exact child
            # quiesce.  Other lanes may report immediately because the GPU lane
            # observes the shared fault independently and owns its own shutdown.
            notify_after_quiesce = self._record_fault(
                stage,
                error,
                notify=stage != "gpu_readiness",
                clear_active=True,
                secondary_context=(
                    "gpu_readiness_quiesce"
                    if gpu_quiesce_observer_failed
                    else None
                ),
            )
            if stage != "gpu_readiness":
                try:
                    self._publish_fault(stage, error, active=False)
                except BaseException as report_error:
                    self._record_secondary_fault(
                        f"{stage}_lane_status", report_error
                    )
        finally:
            if stage == "gpu_readiness":
                quiesce_error: BaseException | None = None
                quiesce_is_primary = False
                if gpu_quiesce_observation_pending:
                    # `_complete` failed after exact containment.  Still publish
                    # the quiesce result last so recovery never ends on the stale
                    # returned active-child monitor.
                    gpu_quiesce_observation_pending = False
                    try:
                        self._observe_gpu_quiesce(gpu_quiesce_outcome)
                    except BaseException as error:
                        quiesce_error = error
                        quiesce_is_primary = self._record_fault(
                            "gpu_readiness",
                            error,
                            notify=False,
                            secondary_context="gpu_readiness_quiesce",
                        )
                elif not gpu_quiesced_before_complete:
                    try:
                        # This is deliberately the first observer-capable action in
                        # the finally path: the backend's exact stop is invoked
                        # before any lane/status publication can block or fail.
                        self._quiesce_gpu_lane()
                    except BaseException as error:
                        quiesce_error = error
                        quiesce_is_primary = self._record_fault(
                            "gpu_readiness",
                            error,
                            notify=False,
                            secondary_context="gpu_readiness_quiesce",
                        )

                if notify_after_quiesce and lane_fault is not None:
                    self._notify_fault("gpu_readiness", lane_fault)
                elif quiesce_is_primary and quiesce_error is not None:
                    self._notify_fault("gpu_readiness", quiesce_error)

                visible_error = lane_fault or quiesce_error
                try:
                    if visible_error is None:
                        self._publish(
                            "gpu_readiness",
                            "idle",
                            active=False,
                            status=self._last_status["gpu_readiness"],
                            wait_reason="gpu_child_quiesced",
                        )
                    else:
                        self._publish_fault(
                            "gpu_readiness", visible_error, active=False
                        )
                except BaseException as report_error:
                    report_is_primary = self._record_fault(
                        "gpu_readiness",
                        report_error,
                        notify=False,
                        secondary_context="gpu_readiness_lane_status",
                    )
                    if report_is_primary:
                        self._notify_fault("gpu_readiness", report_error)

    def run(self) -> LaneRunResult:
        """Run until durable stop, coherent completion, a limit, or lane failure."""

        for stage in STAGES:
            self._publish(stage, "idle", active=False)
        for stage in STAGES[1:]:
            thread = threading.Thread(
                target=self._lane_loop,
                args=(stage,),
                name=f"himr-{stage.replace('_', '-')}-lane",
            )
            thread.start()
            self._threads.append(thread)

        # Acquisition intentionally occupies the calling/main thread.
        self._lane_loop("acquisition")
        with self._condition:
            if self._reason is None and self.desired_state() != "running":
                self._reason = "desired_stop"
            self._condition.notify_all()
        for thread in self._threads:
            thread.join()

        if self._fault is not None:
            if isinstance(self._fault, Exception):
                raise self._fault
            raise ControllerError(
                f"independent lane failed with {type(self._fault).__name__}"
            )
        reason = self._reason or "desired_stop"
        return LaneRunResult(reason, self._dispatch_count, self._terminal)
