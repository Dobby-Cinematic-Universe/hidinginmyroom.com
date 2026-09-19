"""Explicit same-path archive replacements, bound to immutable campaign configs.

Historical configs/receipts keep their original bytes and filesystem observation.
A reviewed transition selects exactly one replacement UUID, never whichever disk
happens to be mounted. Ordinary recovery still exactly revalidates changed file
witnesses. Explicit operator-trusted rsync maintenance may instead rebind already
checkpointed completed media, retaining their saved envelopes and content IDs.
"""

from __future__ import annotations

from .config import ControllerConfig


LEGACY_COLD_MOUNT_UUID = "5b5813ad-b1a4-4f52-9960-e762ceac5636"

# Operator requested recovery after moving the archive to this replacement on
# 2026-09-06. Scope is deliberately the exact active campaign, not all configs.
_REVIEWED_TRANSITIONS = {
    (
        "himrautocfg_cbf42c1ecf0e1c59c221e9711e6f6047",
        "5d0b549e91a870874e17724510723b33e557288f507ef41a35ab2849656dc913",
    ): {
        "from_uuid": LEGACY_COLD_MOUNT_UUID,
        "to_uuid": "af41b7da-a588-41cf-83f8-cd99ef425b74",
        "mount_point": "/mnt/archive",
        "filesystem": "xfs",
        "reviewed_on": "2026-09-06",
        "content_validation": "existing_exact_replay_on_changed_witness",
    },
}


def reviewed_cold_mount_transition(config: ControllerConfig) -> dict[str, str] | None:
    """Return an isolated reviewed transition for this exact config, if any."""

    transition = _REVIEWED_TRANSITIONS.get((config.config_id, config.physical_sha256))
    return None if transition is None else dict(transition)
