"""Pure, conservative admission and adaptive scheduling for Gemini batches.

This module never reads credentials, changes a ledger, releases a reservation,
or sends a request. Callers must supply their validated, unique union of pending
waves and ambiguous/orphan paid reservations. Input allowances come from the
existing immutable job budget (a conservative UTF-8-byte allowance, not a fresh
token estimate). The token ceiling is an OPERATOR limit; it does not establish
the actual quota or other usage of the provider account.
"""
from collections.abc import Mapping


MAX_ACTIVE = 16
DEFAULT_START = 8
DEFAULT_TOKEN_LIMIT = 3_000_000
SUCCESS_CYCLES_PER_INCREASE = 2
INCREASE_STEP = 2
MIN_COOLDOWN_SECONDS = 60
ACTIVE_STATES = frozenset({"pending", "ambiguous", "orphan"})


class AdmissionError(ValueError):
    pass


def _integer(value, name, minimum=0, maximum=None):
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise AdmissionError(f"{name} must be an integer in the allowed range")
    return value


def _sequence(value, name):
    if not isinstance(value, (tuple, list)):
        raise AdmissionError(f"{name} must be a list or tuple")
    return value


def wave_input_tokens(jobs):
    """Sum ALL immutable job input allowances, never just the first job."""
    _sequence(jobs, "wave jobs")
    if not jobs:
        raise AdmissionError("a wave must contain at least one job")
    total = 0
    for job in jobs:
        if not isinstance(job, Mapping) or not isinstance(job.get("budget"), Mapping):
            raise AdmissionError("each wave job must have a budget")
        total += _integer(job["budget"].get("input_token_allowance"), "job input token allowance", 1)
    return total


def active_usage(active_waves):
    """Count pending and unknown paid waves equally, without releasing holds."""
    _sequence(active_waves, "active waves")
    seen = set()
    tokens = pending = unknown = 0
    for wave in active_waves:
        if not isinstance(wave, Mapping):
            raise AdmissionError("active waves must be objects")
        wave_id = wave.get("wave_id")
        if not isinstance(wave_id, str) or not wave_id or len(wave_id) > 1024:
            raise AdmissionError("active wave identity must be a bounded nonempty string")
        if wave_id in seen:
            raise AdmissionError("active wave identity is duplicated")
        seen.add(wave_id)
        state = wave.get("state", "pending")
        if not isinstance(state, str) or state not in ACTIVE_STATES:
            raise AdmissionError("active wave state must be pending, ambiguous or orphan")
        tokens += _integer(wave.get("input_tokens"), "active wave input tokens", 1)
        pending += state == "pending"
        unknown += state != "pending"
    return {"active_slots": len(seen), "enqueued_input_tokens": tokens,
            "pending_waves": pending, "unknown_held_waves": unknown}


def assess_admission(*, active_waves, candidate_input_tokens, candidate_cost_microusd,
                     settled_microusd, held_microusd, budget_limit_microusd,
                     target=MAX_ACTIVE, token_limit=DEFAULT_TOKEN_LIMIT,
                     now_seconds=0, cooldown_until_seconds=0):
    """Return a decision; do not reserve, submit, cancel, or discard anything.

    ``held_microusd`` includes every unresolved reservation, not only pending
    batches. A blocked candidate is not necessarily permanent budget exhaustion:
    settled + candidate may fit after outstanding reservations reconcile.
    Candidate costs are maxima, and the caller must reserve atomically before
    POST and account for each newly reserved wave before the next admission.
    """
    usage = active_usage(active_waves)
    candidate_input_tokens = _integer(candidate_input_tokens, "candidate input tokens", 1)
    candidate_cost_microusd = _integer(candidate_cost_microusd, "candidate maximum cost")
    settled_microusd = _integer(settled_microusd, "settled usage")
    held_microusd = _integer(held_microusd, "held reservation cost")
    budget_limit_microusd = _integer(budget_limit_microusd, "immutable worker budget", 1)
    target = _integer(target, "adaptive target", 1, MAX_ACTIVE)
    token_limit = _integer(token_limit, "operator token ceiling", 1, DEFAULT_TOKEN_LIMIT)
    now_seconds = _integer(now_seconds, "current monotonic seconds")
    cooldown_until_seconds = _integer(cooldown_until_seconds, "cooldown deadline")
    accounted = settled_microusd + held_microusd
    if accounted > budget_limit_microusd:
        raise AdmissionError("settled usage and reservations exceed the immutable worker budget")

    candidate_exceeds_token_limit = candidate_input_tokens > token_limit
    candidate_exceeds_budget_limit = candidate_cost_microusd > budget_limit_microusd
    candidate_too_large = candidate_exceeds_token_limit or candidate_exceeds_budget_limit
    reasons = ["candidate_too_large"] if candidate_too_large else []
    if now_seconds < cooldown_until_seconds:
        reasons.append("rate_limit_cooldown")
    if usage["active_slots"] >= target:
        reasons.append("slot_limit")
    if usage["enqueued_input_tokens"] + candidate_input_tokens > token_limit:
        reasons.append("token_limit")
    budget_state = "available"
    if accounted + candidate_cost_microusd > budget_limit_microusd:
        reasons.append("budget_limit")
        budget_state = ("completed_exhaustion" if settled_microusd + candidate_cost_microusd > budget_limit_microusd
                        else "temporary_reservation_pressure")
    permanently_blocked = candidate_too_large or budget_state == "completed_exhaustion"
    wait_for_paid_results = bool(reasons and not permanently_blocked and usage["pending_waves"])
    reconciliation_required = bool(reasons and not permanently_blocked
                                   and usage["unknown_held_waves"] and not usage["pending_waves"]
                                   and any(reason in reasons for reason in ("slot_limit", "token_limit", "budget_limit")))
    return {"allowed": not reasons, "reasons": reasons, **usage,
            "target_slots": target, "hard_max_slots": MAX_ACTIVE,
            "operator_token_limit": token_limit, "account_quota_verified": False,
            "candidate_input_tokens": candidate_input_tokens,
            "candidate_exceeds_token_limit": candidate_exceeds_token_limit,
            "candidate_exceeds_budget_limit": candidate_exceeds_budget_limit,
            "candidate_state": "too_large" if candidate_too_large else "blocked" if reasons else "ready",
            "candidate_cost_microusd": candidate_cost_microusd,
            "accounted_microusd": accounted,
            "budget_remaining_microusd": budget_limit_microusd - accounted,
            "budget_state": budget_state, "wait_for_paid_results": wait_for_paid_results,
            "reconciliation_required": reconciliation_required,
            "cooldown_remaining_seconds": max(0, cooldown_until_seconds - now_seconds)}


def initial_state(*, start=DEFAULT_START, max_active=MAX_ACTIVE):
    max_active = _integer(max_active, "maximum active batches", 1, MAX_ACTIVE)
    start = _integer(start, "initial adaptive target", 1, max_active)
    return {"target": start, "max_active": max_active, "success_streak": 0,
            "cooldown_until_seconds": 0, "rate_limit_events": 0}


def advance(state, *, now_seconds, successful_cycle=False, rate_limited=False,
            retry_after_seconds=None):
    """Additively grow after healthy cycles; halve on 429, without cancellation.

    Only actual successful provider operations should mark a cycle successful;
    idle cycles must not increase concurrency. A rate limit always wins over
    success in a partially successful cycle. The monotonic deadline is in-memory
    scheduling state, NOT a paid receipt or reservation that could be released.
    """
    fields = {"target", "max_active", "success_streak", "cooldown_until_seconds", "rate_limit_events"}
    if not isinstance(state, Mapping) or set(state) != fields:
        raise AdmissionError("adaptive state has unexpected fields")
    max_active = _integer(state["max_active"], "maximum active batches", 1, MAX_ACTIVE)
    target = _integer(state["target"], "adaptive target", 1, max_active)
    streak = _integer(state["success_streak"], "success streak", 0, SUCCESS_CYCLES_PER_INCREASE - 1)
    deadline = _integer(state["cooldown_until_seconds"], "cooldown deadline")
    events = _integer(state["rate_limit_events"], "rate limit events")
    now_seconds = _integer(now_seconds, "current monotonic seconds")
    if type(successful_cycle) is not bool or type(rate_limited) is not bool:
        raise AdmissionError("cycle success and rate limit flags must be booleans")
    if retry_after_seconds is not None:
        retry_after_seconds = _integer(retry_after_seconds, "retry-after seconds")
    if rate_limited:
        target = max(1, target // 2)
        deadline = max(deadline, now_seconds + max(MIN_COOLDOWN_SECONDS, retry_after_seconds or 0))
        events += 1
        streak = 0
    elif now_seconds < deadline:
        streak = 0
    elif successful_cycle:
        streak += 1
        if streak == SUCCESS_CYCLES_PER_INCREASE:
            target = min(max_active, target + INCREASE_STEP)
            streak = 0
    else:
        streak = 0
    return {"target": target, "max_active": max_active, "success_streak": streak,
            "cooldown_until_seconds": deadline, "rate_limit_events": events}
