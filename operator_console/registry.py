"""Typed, closed command registry for the local HIMR operator console.

The browser never supplies argv, paths, environment variables, or executable names.
It selects an owner-reviewed profile, and this module expands that profile through a
tracked action specification into one exact argv array.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


IMPLEMENTATION_VERSION = "0.3.1"
PROFILE_SCHEMA_VERSION = 1
MAX_ACTION_TIMEOUT_SECONDS = 30 * 24 * 60 * 60
MAX_PROFILE_BYTES = 1024 * 1024
MAX_PROFILES = 128
MAX_PATH_BYTES = 4096
PROFILE_ID_RE = re.compile(r"[a-z][a-z0-9._-]{2,63}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
FORBIDDEN_PARAMETER_WORDS = {
    "authorization",
    "cookie",
    "credential",
    "discord",
    "password",
    "proxy",
    "secret",
    "token",
}
SYSTEMD_USER_TOOLS = (
    Path("/usr/bin/systemd-run"),
    Path("/usr/bin/systemctl"),
    Path("/usr/bin/env"),
)
TRUSTED_GPU_LAUNCHER = Path(
    "/usr/local/libexec/himr-gpu/trusted-launcher-v2"
)
TRUSTED_GPU_RUNTIME_ADMISSION = Path(
    "/etc/himr-gpu/runtime-admission-v2.json"
)
TRUSTED_GPU_LAUNCHER_PROFILE = Path(
    "/etc/himr-gpu/launcher-profile-v2.json"
)


class RegistryError(ValueError):
    """A profile or action falls outside the console's closed contract."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    flag: str
    kind: str
    required: bool = True
    minimum: int | None = None
    maximum: int | None = None


@dataclass(frozen=True)
class ActionSpec:
    action_id: str
    stage: str
    label: str
    description: str
    effect: str
    resource: str
    launcher: str
    entrypoint: str | None
    prefix: tuple[str, ...]
    fields: tuple[FieldSpec, ...]
    confirmation: str | None
    timeout_seconds: int
    enabled: bool = True
    blocked_reason: str | None = None
    # ``direct`` preserves the original in-process supervisor.  Production GPU
    # actions may opt into the closed systemd-user cgroup supervisor, but must
    # also bind an exact host-memory ceiling in this tracked registry.
    supervisor: str = "direct"
    memory_max_bytes: int | None = None
    # For the same-UID local-private lane only, the reviewed profile may name
    # the mode-0500 launcher copy that subsequently authenticates itself.
    entrypoint_parameter: str | None = None
    # Optional systemd-user envelope overrides.  ``None`` retains the console's
    # conservative GPU defaults; non-GPU autonomous supervisors bind every value
    # explicitly so large media files are not subjected to the GPU artifact cap.
    tasks_max: int | None = None
    file_size_max_bytes: int | None = None
    stop_timeout_seconds: int | None = None
    memory_swap_max_bytes: int | None = None


@dataclass(frozen=True)
class Profile:
    profile_id: str
    label: str
    description: str
    action_id: str
    parameters: Mapping[str, Any]


@dataclass(frozen=True)
class ProfileSet:
    path: Path
    raw_sha256: str
    byte_count: int
    profiles: tuple[Profile, ...]


@dataclass(frozen=True)
class PreparedCommand:
    profile: Profile
    action: ActionSpec
    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str]
    entrypoint_path: Path
    entrypoint_sha256: str
    entrypoint_byte_count: int
    profile_set_sha256: str


def _field(
    name: str,
    flag: str,
    kind: str,
    *,
    required: bool = True,
    minimum: int | None = None,
    maximum: int | None = None,
) -> FieldSpec:
    return FieldSpec(name, flag, kind, required, minimum, maximum)


ACTIONS: dict[str, ActionSpec] = {
    "autonomy.run": ActionSpec(
        "autonomy.run",
        "Autonomous campaign",
        "Start autonomous Archive campaign",
        "Run the sealed all-known Archive.org campaign until it drains, faults closed, or receives a durable stop request.",
        "execute",
        "autonomous_pipeline",
        "repo",
        "autonomous_controller/bin/himr-autonomous-controller",
        ("run",),
        (
            _field("config", "--config", "input_file"),
            _field("expected_config_sha256", "--expected-config-sha256", "sha256"),
        ),
        None,
        MAX_ACTION_TIMEOUT_SECONDS,
        True,
        None,
        "systemd_user",
        16 * 1024**3,
        None,
        256,
        64 * 1024**3,
        30,
        0,
    ),
    "autonomy.request_stop": ActionSpec(
        "autonomy.request_stop",
        "Autonomous campaign",
        "Stop autonomous Archive campaign",
        "Durably quiesce the exact sealed campaign; the controller reconciles any admitted GPU child before exiting.",
        "execute",
        "autonomous_control",
        "repo",
        "autonomous_controller/bin/himr-autonomous-controller",
        ("request-stop",),
        (
            _field("config", "--config", "input_file"),
            _field("expected_config_sha256", "--expected-config-sha256", "sha256"),
        ),
        None,
        60,
    ),
    "acquisition.validate": ActionSpec(
        "acquisition.validate",
        "Acquisition",
        "Validate acquisition producer",
        "Replay one sealed producer schedule and its completed results without network access.",
        "inspect",
        "network",
        "repo",
        "acquisition/bin/background-producer",
        ("validate",),
        (_field("schedule", "--schedule", "input_file"),),
        None,
        14_400,
    ),
    "acquisition.run": ActionSpec(
        "acquisition.run",
        "Acquisition",
        "Run one acquisition cycle",
        "Run one finite public, credential-free producer cycle within explicit profile caps.",
        "execute",
        "network",
        "repo",
        "acquisition/bin/background-producer",
        ("run",),
        (
            _field("schedule", "--schedule", "input_file"),
            _field("max_new_items", "--max-new-items", "integer", minimum=1, maximum=64),
            _field(
                "max_new_bytes",
                "--max-new-bytes",
                "integer",
                minimum=1,
                maximum=2**63 - 1,
            ),
            _field(
                "max_run_seconds",
                "--max-run-seconds",
                "integer",
                minimum=1,
                maximum=14_400,
            ),
            _field(
                "free_space_floor_bytes",
                "--free-space-floor-bytes",
                "integer",
                minimum=1,
                maximum=2**63 - 1,
            ),
        ),
        "RUN ACQUISITION",
        14_700,
    ),
    "retention.public_acquisition": ActionSpec(
        "retention.public_acquisition",
        "Cold retention",
        "Retain one completed public acquisition",
        "Replay one exact public acquisition result, seal a separate hot staging object, and retain it at the fixed /mnt/archive/HIMR content address without mutating the acquisition payload.",
        "execute",
        "cold_storage",
        "repo",
        "acquisition/bin/retain-public-acquisition",
        ("run",),
        (
            _field("work_order", "--work-order", "input_file"),
            _field("acquisition_root", "--acquisition-root", "private_dir"),
            _field("staging_root", "--staging-root", "private_dir"),
            _field("receipt_root", "--receipt-root", "private_dir"),
            _field(
                "expected_work_order_sha256",
                "--expected-work-order-sha256",
                "sha256",
            ),
            _field(
                "expected_result_sha256",
                "--expected-result-sha256",
                "sha256",
            ),
            _field("expected_sha256", "--expected-sha256", "sha256"),
            _field(
                "expected_byte_count",
                "--expected-byte-count",
                "integer",
                minimum=1,
                maximum=2**63 - 1,
            ),
            _field(
                "free_space_floor_bytes",
                "--free-space-floor-bytes",
                "integer",
                minimum=1,
                maximum=2**63 - 1,
            ),
        ),
        "RETAIN ONE PUBLIC ACQUISITION",
        86_400,
    ),
    "archive.preprocess_next": ActionSpec(
        "archive.preprocess_next",
        "Archive acquisition handoff",
        "Preprocess next Archive results",
        "Replay one sealed producer schedule and process a bounded prefix of its exact completed, unacknowledged results as ASR-ready media.",
        "execute",
        "preprocess",
        "repo",
        "acquisition/bin/archive-preprocess-next",
        (),
        (
            _field("schedule", "--schedule", "input_file"),
            _field("bundle_root", "--bundle-root", "private_dir"),
            _field(
                "processing_output_root",
                "--processing-output-root",
                "private_dir",
            ),
            _field("limit", "--limit", "integer", minimum=1, maximum=8),
        ),
        "RUN ARCHIVE PREPROCESS",
        43_200,
    ),
    "archive.rolling_pipeline": ActionSpec(
        "archive.rolling_pipeline",
        "Rolling Archive pipeline",
        "Run rolling Archive pipeline",
        "Overlap exactly one sealed Archive producer with one receipt-bound ASR-ready preprocessor under cumulative item, reservation-byte, time, and free-space bounds.",
        "execute",
        "archive_pipeline",
        "repo",
        "acquisition/bin/archive-rolling-pipeline",
        (),
        (
            _field("schedule", "--schedule", "input_file"),
            _field("bundle_root", "--bundle-root", "private_dir"),
            _field(
                "processing_output_root",
                "--processing-output-root",
                "private_dir",
            ),
            _field("max_new_items", "--max-new-items", "integer", minimum=1, maximum=8),
            _field(
                "max_new_bytes",
                "--max-new-bytes",
                "integer",
                minimum=1,
                maximum=2**63 - 1,
            ),
            _field(
                "max_run_seconds",
                "--max-run-seconds",
                "integer",
                minimum=1,
                maximum=14_400,
            ),
            _field(
                "free_space_floor_bytes",
                "--free-space-floor-bytes",
                "integer",
                minimum=1,
                maximum=2**63 - 1,
            ),
            _field(
                "max_preprocess_items",
                "--max-preprocess-items",
                "integer",
                minimum=1,
                maximum=8,
            ),
        ),
        "RUN ROLLING ARCHIVE PIPELINE",
        43_200,
    ),
    "preprocess.validate_bundle": ActionSpec(
        "preprocess.validate_bundle",
        "ASR-ready preprocessing",
        "Validate preprocessing bundle",
        "Replay the immutable bundle, software pins, and source bindings without output writes.",
        "inspect",
        "preprocess",
        "repo",
        "pipeline/bin/preprocess-batch",
        ("validate-bundle",),
        (_field("bundle", "--bundle", "input_dir"),),
        None,
        14_400,
    ),
    "preprocess.status": ActionSpec(
        "preprocess.status",
        "ASR-ready preprocessing",
        "Inspect preprocessing status",
        "Validate completion receipts and report completed and pending ordinals.",
        "inspect",
        "preprocess",
        "repo",
        "pipeline/bin/preprocess-batch",
        ("status",),
        (
            _field("bundle", "--bundle", "input_dir"),
            _field("state_root", "--state-root", "private_dir"),
        ),
        None,
        14_400,
    ),
    "preprocess.dry_run": ActionSpec(
        "preprocess.dry_run",
        "ASR-ready preprocessing",
        "Dry-run preprocessing",
        "Replay a finite pending prefix without creating stage output or receipts.",
        "inspect",
        "preprocess",
        "repo",
        "pipeline/bin/preprocess-batch",
        ("run",),
        (
            _field("bundle", "--bundle", "input_dir"),
            _field("state_root", "--state-root", "private_dir"),
            _field("limit", "--limit", "integer", minimum=1, maximum=32),
            _field("dry_run", "--dry-run", "true"),
        ),
        None,
        14_400,
    ),
    "preprocess.run": ActionSpec(
        "preprocess.run",
        "ASR-ready preprocessing",
        "Run bounded preprocessing",
        "Run or resume a finite preprocessing prefix; immutable receipts remain completion authority.",
        "execute",
        "preprocess",
        "repo",
        "pipeline/bin/preprocess-batch",
        ("run",),
        (
            _field("bundle", "--bundle", "input_dir"),
            _field("state_root", "--state-root", "private_dir"),
            _field("limit", "--limit", "integer", minimum=1, maximum=32),
        ),
        "RUN PREPROCESS",
        43_200,
    ),
    "asr_queue.validate": ActionSpec(
        "asr_queue.validate",
        "ASR queue",
        "Validate ASR queue",
        "Dispatch exact v0.2/v0.3 validation by sealed implementation version; never execute ASR.",
        "inspect",
        "cpu_asr",
        "repo",
        "pipeline/bin/preprocess-asr-queue-dispatch",
        ("validate",),
        (_field("manifest", "--manifest", "input_file"),),
        None,
        14_400,
    ),
    "asr_queue.runner_v03_validate": ActionSpec(
        "asr_queue.runner_v03_validate",
        "ASR queue candidate",
        "Deep-validate ASR runner v0.3",
        "Replay the complete sealed v0.3 queue, routing policy, and reusable result trees without ASR execution.",
        "inspect",
        "cpu_asr",
        "repo",
        "pipeline/bin/preprocess-asr-queue-runner-v03",
        ("validate",),
        (_field("manifest", "--manifest", "input_file"),),
        None,
        14_400,
    ),
    "asr_queue.runner_v03_dry_run": ActionSpec(
        "asr_queue.runner_v03_dry_run",
        "ASR queue candidate",
        "Dry-run whole ASR runner v0.3 queue",
        "Probe and plan every eligible member of the complete sealed queue without creating ASR results.",
        "inspect",
        "cpu_asr",
        "repo",
        "pipeline/bin/preprocess-asr-queue-runner-v03",
        ("run",),
        (
            _field("manifest", "--manifest", "input_file"),
            _field("dry_run", "--dry-run", "true"),
        ),
        "DRY RUN WHOLE ASR V03",
        43_200,
    ),
    "asr_queue.runner_v03_run": ActionSpec(
        "asr_queue.runner_v03_run",
        "ASR queue candidate",
        "Run ASR runner v0.3",
        "Real CPU ASR dispatch for the sealed v0.3 candidate queue.",
        "execute",
        "cpu_asr",
        "repo",
        "pipeline/bin/preprocess-asr-queue-runner-v03",
        ("run",),
        (_field("manifest", "--manifest", "input_file"),),
        "RUN ASR V03",
        86_400,
        False,
        "Blocked: the candidate runner has no bounded-prefix or dispatch-lock contract and still requires a reviewed v0.3-to-v0.5 sealing pilot.",
    ),
    "catalog.validate": ActionSpec(
        "catalog.validate",
        "Private catalogue",
        "Validate private catalogue",
        "Open a closed, checkpointed catalogue through its immutable read-only audit path.",
        "inspect",
        "catalog",
        "corpus",
        None,
        ("validate",),
        (_field("database", "--db", "database_file"),),
        None,
        14_400,
    ),
    "catalog.plan_gpu_v3": ActionSpec(
        "catalog.plan_gpu_v3",
        "Private catalogue",
        "Plan private GPU-v3 admission",
        "Print a text-free admission plan only; this action never installs migrations or imports text.",
        "inspect",
        "catalog",
        "corpus",
        None,
        ("plan-faster-whisper-gpu-v3-admission",),
        (
            _field("database", "--db", "database_file"),
            _field("result", "--result", "input_file"),
            _field("work_order", "--work-order", "input_file"),
            _field("batch_completion", "--batch-completion", "input_file", required=False),
        ),
        None,
        14_400,
    ),
    "legacy_asr.audit_v04": ActionSpec(
        "legacy_asr.audit_v04",
        "Compatibility",
        "Audit historical v0.4 receipt",
        "Run the narrow read-only restart-portable audit; stdout grants no downstream authority.",
        "inspect",
        "cpu_asr",
        "repo",
        "pipeline/bin/asr-whispercpp-v04-receipt-audit",
        (),
        (_field("receipt", "--receipt", "input_file"),),
        None,
        14_400,
    ),
    "gpu.materialize_v1": ActionSpec(
        "gpu.materialize_v1",
        "GPU ASR",
        "Materialize reviewed GPU batch",
        "Create exact v5 work orders and one finite batch from selected ready v0.3 queue ordinals.",
        "execute",
        "cpu_asr",
        "repo",
        "pipeline/bin/materialize-gpu-asr-batch-v1",
        ("materialize",),
        (
            _field("queue_manifest", "--queue-manifest", "input_file"),
            _field("expected_queue_sha256", "--expected-queue-sha256", "sha256"),
            _field("root_registration", "--root-registration", "input_file"),
            _field("expected_root_registration_sha256", "--expected-root-registration-sha256", "sha256"),
            _field("runtime_admission", "--runtime-admission", "input_file"),
            _field("expected_runtime_admission_sha256", "--expected-runtime-admission-sha256", "sha256"),
            _field("production_profile", "--production-profile", "input_file"),
            _field("expected_production_profile_sha256", "--expected-production-profile-sha256", "sha256"),
            _field("queue_ordinals", "--queue-ordinal", "ordinal_list"),
            _field("work_order_root", "--work-order-root", "private_dir"),
            _field("receipt_root", "--receipt-root", "private_dir"),
            _field("result_root", "--result-root", "private_dir"),
            _field("batch_root", "--batch-root", "private_dir"),
            _field("event_root", "--event-root", "private_dir"),
            _field("lock_root", "--lock-root", "private_dir"),
        ),
        "MATERIALIZE GPU BATCH",
        3_600,
    ),
    "gpu.preprocess_queue_v1": ActionSpec(
        "gpu.preprocess_queue_v1",
        "GPU ASR",
        "Seal preprocess receipts as a GPU queue",
        "Materialize one explicit preprocess bundle/state pair into a typed GPU queue without globs or media inference.",
        "execute",
        "cpu_asr",
        "repo",
        "pipeline/bin/preprocess-gpu-asr-queue",
        ("materialize",),
        (
            _field("preprocess_bundle", "--preprocess-bundle", "input_dir"),
            _field("preprocess_state_root", "--preprocess-state-root", "input_dir"),
            _field("queue_root", "--queue-root", "private_dir"),
            _field("production_profile", "--production-profile", "input_file"),
            _field("root_registration", "--root-registration", "input_file"),
            _field("root_registration_sha256", "--root-registration-sha256", "sha256"),
        ),
        "MATERIALIZE GPU QUEUE",
        3_600,
    ),
    "gpu.prepare_local_private": ActionSpec(
        "gpu.prepare_local_private",
        "GPU ASR",
        "Prepare local-private GPU controls",
        "Generate a private launcher/profile/candidate-runtime tuple from explicit image, profile, root, and host-ABI bindings.",
        "execute",
        "cpu_asr",
        "repo",
        "pipeline/bin/prepare-local-private-production-v1",
        ("prepare",),
        (
            _field("execution_image_receipt", "--execution-image-receipt", "input_file"),
            _field("expected_execution_image_receipt_sha256", "--expected-execution-image-receipt-sha256", "sha256"),
            _field("production_profile", "--production-profile", "input_file"),
            _field("expected_production_profile_sha256", "--expected-production-profile-sha256", "sha256"),
            _field("root_registration", "--root-registration", "input_file"),
            _field("expected_root_registration_sha256", "--expected-root-registration-sha256", "sha256"),
            _field("host_abi_manifest", "--host-abi-manifest", "input_file"),
            _field("expected_host_abi_manifest_sha256", "--expected-host-abi-manifest-sha256", "sha256"),
            _field("launcher_output", "--launcher-output", "output_file"),
            _field("launcher_profile_output", "--launcher-profile-output", "output_file"),
            _field("runtime_admission_output", "--runtime-admission-output", "output_file"),
        ),
        "PREPARE LOCAL GPU CONTROLS",
        14_400,
    ),
    "gpu.materialize_local_v1": ActionSpec(
        "gpu.materialize_local_v1",
        "GPU ASR",
        "Materialize local-private GPU batch",
        "Create exact production-lineage v5 work orders bound to a candidate runtime and the local-private execution class.",
        "execute",
        "cpu_asr",
        "repo",
        "pipeline/bin/materialize-gpu-asr-batch-v1",
        ("materialize", "--execution-mode", "local-private-production"),
        (
            _field("queue_manifest", "--queue-manifest", "input_file"),
            _field("expected_queue_sha256", "--expected-queue-sha256", "sha256"),
            _field("root_registration", "--root-registration", "input_file"),
            _field("expected_root_registration_sha256", "--expected-root-registration-sha256", "sha256"),
            _field("runtime_admission", "--runtime-admission", "input_file"),
            _field("expected_runtime_admission_sha256", "--expected-runtime-admission-sha256", "sha256"),
            _field("production_profile", "--production-profile", "input_file"),
            _field("expected_production_profile_sha256", "--expected-production-profile-sha256", "sha256"),
            _field("queue_ordinals", "--queue-ordinal", "ordinal_list"),
            _field("work_order_root", "--work-order-root", "private_dir"),
            _field("receipt_root", "--receipt-root", "private_dir"),
            _field("result_root", "--result-root", "private_dir"),
            _field("batch_root", "--batch-root", "private_dir"),
            _field("event_root", "--event-root", "private_dir"),
            _field("lock_root", "--lock-root", "private_dir"),
        ),
        "MATERIALIZE LOCAL GPU BATCH",
        3_600,
    ),
    "gpu.batch_v2": ActionSpec(
        "gpu.batch_v2",
        "GPU ASR",
        "Run admitted GPU batch v2",
        "Run one finite private batch through the root-owned, networkless trusted launcher.",
        "execute",
        "gpu",
        "trusted_host",
        str(TRUSTED_GPU_LAUNCHER),
        (
            "run",
            "--mode",
            "production",
            "--runtime-admission",
            str(TRUSTED_GPU_RUNTIME_ADMISSION),
            "--launcher-profile",
            str(TRUSTED_GPU_LAUNCHER_PROFILE),
        ),
        (
            _field("batch_manifest", "--batch-manifest", "input_file"),
            _field("expected_batch_sha256", "--expected-batch-sha256", "sha256"),
            _field(
                "expected_runtime_admission_sha256",
                "--expected-runtime-admission-sha256",
                "sha256",
            ),
            _field("production_profile", "--production-profile", "input_file"),
            _field(
                "expected_production_profile_sha256",
                "--expected-production-profile-sha256",
                "sha256",
            ),
            _field("root_registration", "--root-registration", "input_file"),
            _field(
                "expected_root_registration_sha256",
                "--expected-root-registration-sha256",
                "sha256",
            ),
            _field(
                "expected_launcher_profile_sha256",
                "--expected-launcher-profile-sha256",
                "sha256",
            ),
            _field("writable_result_root", "--writable-result-root", "private_dir"),
            _field("writable_event_root", "--writable-event-root", "private_dir"),
            _field("writable_lock_root", "--writable-lock-root", "private_dir"),
        ),
        "RUN GPU ASR",
        3_600,
        True,
        None,
        "systemd_user",
        12 * 1024**3,
    ),
    "gpu.doctor_local_private": ActionSpec(
        "gpu.doctor_local_private",
        "GPU ASR",
        "Check local-private GPU readiness",
        "Replay the current private controls, full image hash, host ABI, GPU binding, and enforced resource envelope without media or inference.",
        "execute",
        "gpu",
        "local_private_host",
        None,
        ("doctor-local-private",),
        (
            _field("runtime_admission", "--runtime-admission", "input_file"),
            _field("expected_runtime_admission_sha256", "--expected-runtime-admission-sha256", "sha256"),
            _field("production_profile", "--production-profile", "input_file"),
            _field("expected_production_profile_sha256", "--expected-production-profile-sha256", "sha256"),
            _field("root_registration", "--root-registration", "input_file"),
            _field("expected_root_registration_sha256", "--expected-root-registration-sha256", "sha256"),
            _field("launcher_profile", "--launcher-profile", "input_file"),
            _field("expected_launcher_profile_sha256", "--expected-launcher-profile-sha256", "sha256"),
            _field("readiness_output", "--readiness-output", "output_file"),
        ),
        "CHECK LOCAL GPU READINESS",
        14_400,
        True,
        None,
        "systemd_user",
        12 * 1024**3,
        "local_launcher",
    ),
    "gpu.batch_local_private": ActionSpec(
        "gpu.batch_local_private",
        "GPU ASR",
        "Run local-private GPU batch",
        "Run one candidate-runtime production-lineage batch after a same-binding readiness receipt; controls retain the weaker same-UID boundary.",
        "execute",
        "gpu",
        "local_private_host",
        None,
        ("run", "--mode", "local-private-production"),
        (
            _field("batch_manifest", "--batch-manifest", "input_file"),
            _field("expected_batch_sha256", "--expected-batch-sha256", "sha256"),
            _field("runtime_admission", "--runtime-admission", "input_file"),
            _field("expected_runtime_admission_sha256", "--expected-runtime-admission-sha256", "sha256"),
            _field("production_profile", "--production-profile", "input_file"),
            _field("expected_production_profile_sha256", "--expected-production-profile-sha256", "sha256"),
            _field("root_registration", "--root-registration", "input_file"),
            _field("expected_root_registration_sha256", "--expected-root-registration-sha256", "sha256"),
            _field("launcher_profile", "--launcher-profile", "input_file"),
            _field("expected_launcher_profile_sha256", "--expected-launcher-profile-sha256", "sha256"),
            _field("local_readiness", "--local-readiness", "input_file"),
            _field("expected_local_readiness_sha256", "--expected-local-readiness-sha256", "sha256"),
            _field("writable_result_root", "--writable-result-root", "private_dir"),
            _field("writable_event_root", "--writable-event-root", "private_dir"),
            _field("writable_lock_root", "--writable-lock-root", "private_dir"),
        ),
        "RUN LOCAL PRIVATE GPU ASR",
        3_600,
        True,
        None,
        "systemd_user",
        12 * 1024**3,
        "local_launcher",
    ),
    "gpu.batch": ActionSpec(
        "gpu.batch",
        "GPU ASR",
        "GPU batch execution",
        "Resident GPU batch validation and execution are unavailable after the reboot.",
        "execute",
        "gpu",
        "repo",
        "pipeline/bin/asr-faster-whisper-gpu-batch",
        (),
        (),
        None,
        86_400,
        False,
        "Blocked: persisted runtime.expected_device is reboot-unstable; v4 also lacks an external trust anchor.",
    ),
}


BLOCKED_CAPABILITIES: tuple[dict[str, str], ...] = (
    {
        "stage": "Long-audio GPU chunking",
        "reason": "Receipt-bound GPU work orders and batches are available, but normalized audio above the admitted 420-second per-item ceiling still needs a deterministic sample-index chunker.",
    },
    {
        "stage": "CPU ASR v0.3 execution",
        "reason": "Real dispatch remains a candidate: it lacks a bounded-prefix control and dispatch lock, and its v0.3-to-v0.5 sealing handoff has not completed a reviewed pilot. Deep validation and a confirmed whole-queue dry-run are available.",
    },
    {
        "stage": "Root-owned GPU trust boundary",
        "reason": "The admitted local-private GPU path is active with a same-UID boundary; installing the stronger root-owned launcher remains a separate privileged operation.",
    },
    {
        "stage": "Catalogue admission and migrations",
        "reason": "Database writes remain a separate digest-reviewed administrative operation; migration 0034 is unapplied.",
    },
    {
        "stage": "Deletion, publication, and identity",
        "reason": "These capabilities are intentionally absent and require separate authority or human review. The cold-retention action grants none of them.",
    },
)


def _strict_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise RegistryError(f"{label} has unexpected shape: {observed}")
    return value


def _parse_json(body: bytes, label: str) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RegistryError(f"{label} contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise RegistryError(f"{label} contains non-finite number {value!r}")

    try:
        return json.loads(
            body.decode("utf-8"), object_pairs_hook=unique, parse_constant=reject_constant
        )
    except RegistryError:
        raise
    except (UnicodeDecodeError, ValueError) as error:
        raise RegistryError(f"{label} is not strict UTF-8 JSON: {error}") from error


def _assert_no_symlink_components(path: Path, label: str) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except OSError as error:
            raise RegistryError(f"cannot inspect {label} component: {error}") from error
        if stat.S_ISLNK(info.st_mode):
            raise RegistryError(f"{label} contains a symlink component")


def _stable_file(
    path: Path,
    *,
    maximum: int,
    required_mode: int | None = None,
    require_owner: bool = True,
    require_single_link: bool = True,
) -> tuple[bytes, os.stat_result]:
    absolute = path.absolute()
    _assert_no_symlink_components(absolute, str(absolute))
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(absolute, flags)
    except OSError as error:
        raise RegistryError(f"cannot open {absolute}: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size < 1 or before.st_size > maximum:
            raise RegistryError(f"{absolute} must be a bounded regular file")
        if require_owner and before.st_uid != os.geteuid():
            raise RegistryError(f"{absolute} must be owned by the current user")
        if require_single_link and before.st_nlink != 1:
            raise RegistryError(f"{absolute} must be single-link")
        if required_mode is not None and stat.S_IMODE(before.st_mode) != required_mode:
            raise RegistryError(f"{absolute} must have exact mode {required_mode:04o}")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
    if len(body) != before.st_size or any(
        getattr(before, field) != getattr(after, field) for field in fields
    ):
        raise RegistryError(f"{absolute} changed while being read")
    current = absolute.lstat()
    if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
        raise RegistryError(f"{absolute} pathname changed while being read")
    return body, before


def load_profiles(path: Path) -> ProfileSet:
    body, info = _stable_file(path, maximum=MAX_PROFILE_BYTES, required_mode=0o400)
    root = _strict_object(_parse_json(body, "profile configuration"), "profile configuration", {"schema_version", "profiles"})
    if root["schema_version"] != PROFILE_SCHEMA_VERSION:
        raise RegistryError("profile configuration schema_version must be 1")
    rows = root["profiles"]
    if not isinstance(rows, list) or len(rows) > MAX_PROFILES:
        raise RegistryError(f"profiles must be an array of at most {MAX_PROFILES} entries")
    profiles: list[Profile] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        row = _strict_object(
            raw,
            f"profiles[{index}]",
            {"id", "label", "description", "action", "parameters"},
        )
        profile_id = row["id"]
        if not isinstance(profile_id, str) or not PROFILE_ID_RE.fullmatch(profile_id):
            raise RegistryError(f"profiles[{index}].id is invalid")
        if profile_id in seen:
            raise RegistryError(f"duplicate profile id {profile_id!r}")
        seen.add(profile_id)
        label = row["label"]
        description = row["description"]
        if (
            not isinstance(label, str)
            or not 1 <= len(label) <= 100
            or not isinstance(description, str)
            or len(description) > 500
        ):
            raise RegistryError(f"profiles[{index}] label/description is invalid")
        action_id = row["action"]
        if not isinstance(action_id, str) or action_id not in ACTIONS:
            raise RegistryError(f"profiles[{index}].action is not registered")
        parameters = row["parameters"]
        if not isinstance(parameters, dict):
            raise RegistryError(f"profiles[{index}].parameters must be an object")
        profiles.append(Profile(profile_id, label, description, action_id, parameters))
    return ProfileSet(
        path=path.absolute(),
        raw_sha256=hashlib.sha256(body).hexdigest(),
        byte_count=info.st_size,
        profiles=tuple(profiles),
    )


def _contains_forbidden_word(value: str) -> bool:
    lowered = value.lower()
    return any(word in lowered for word in FORBIDDEN_PARAMETER_WORDS)


def _lexical_path(value: Any, *, repo_root: Path, label: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value or len(value.encode()) > MAX_PATH_BYTES:
        raise RegistryError(f"{label} must be a bounded path string")
    if any(ord(character) < 32 for character in value):
        raise RegistryError(f"{label} contains a control character")
    if value == "$REPO":
        path = repo_root
    elif value.startswith("$REPO/"):
        path = repo_root / value[6:]
    else:
        path = Path(value)
    if not path.is_absolute():
        raise RegistryError(f"{label} must be absolute or begin with $REPO/")
    normalized = Path(os.path.abspath(path))
    try:
        normalized.relative_to(repo_root)
    except ValueError as error:
        raise RegistryError(f"{label} must stay beneath the repository root") from error
    cold = Path("/mnt/archive/HIMR")
    try:
        normalized.relative_to(cold)
    except ValueError:
        pass
    else:
        raise RegistryError(f"{label} cannot reference cold storage")
    forbidden_roots = (
        repo_root / ".git",
        repo_root / "dist",
        repo_root / "public",
        repo_root / "src",
    )
    for forbidden in forbidden_roots:
        try:
            normalized.relative_to(forbidden)
        except ValueError:
            continue
        raise RegistryError(f"{label} cannot reference public or repository-control output")
    current = Path(normalized.anchor)
    for component in normalized.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            break
        except OSError as error:
            raise RegistryError(f"cannot inspect {label} component: {error}") from error
        if stat.S_ISLNK(info.st_mode):
            raise RegistryError(f"{label} contains a symlink component")
    return normalized


def _validated_parameter(
    field: FieldSpec, value: Any, *, repo_root: Path
) -> str | bool | tuple[str, ...]:
    label = f"parameter {field.name}"
    if field.kind == "sha256":
        if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
            raise RegistryError(f"{label} must be a lowercase SHA-256 digest")
        return value
    if field.kind == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise RegistryError(f"{label} must be an integer")
        if field.minimum is not None and value < field.minimum:
            raise RegistryError(f"{label} is below {field.minimum}")
        if field.maximum is not None and value > field.maximum:
            raise RegistryError(f"{label} exceeds {field.maximum}")
        return str(value)
    if field.kind == "ordinal_list":
        if (
            not isinstance(value, list)
            or not 1 <= len(value) <= 32
            or any(
                isinstance(item, bool)
                or not isinstance(item, int)
                or not 1 <= item <= 128
                for item in value
            )
            or value != sorted(value)
            or len(set(value)) != len(value)
        ):
            raise RegistryError(
                f"{label} must be a sorted unique list of 1..32 ordinals in 1..128"
            )
        return tuple(str(item) for item in value)
    if field.kind == "true":
        if value is not True:
            raise RegistryError(f"{label} must be true")
        return True
    path = _lexical_path(value, repo_root=repo_root, label=label)
    try:
        info = path.lstat()
    except FileNotFoundError:
        if field.kind in {"private_dir", "output_file"}:
            research_root = repo_root / "research"
            try:
                path.relative_to(research_root)
            except ValueError as error:
                raise RegistryError(f"{label} must exist or be below $REPO/research") from error
            return str(path)
        raise RegistryError(f"{label} does not exist")
    except OSError as error:
        raise RegistryError(f"cannot inspect {label}: {error}") from error
    if field.kind in {"input_file", "database_file", "output_file"}:
        if not stat.S_ISREG(info.st_mode):
            raise RegistryError(f"{label} must be a regular file")
        if field.kind == "database_file":
            database_roots = (repo_root / "research", repo_root / "corpus" / "private")
            if not any(_is_beneath(path, root) for root in database_roots):
                raise RegistryError(f"{label} must be below a private database root")
    elif field.kind in {"input_dir", "private_dir"}:
        if not stat.S_ISDIR(info.st_mode):
            raise RegistryError(f"{label} must be a directory")
        if field.kind == "private_dir" and not _is_beneath(path, repo_root / "research"):
            raise RegistryError(f"{label} must be below $REPO/research")
    else:
        raise RegistryError(f"unsupported field kind {field.kind!r}")
    return str(path)


def _is_beneath(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _entrypoint(
    action: ActionSpec, repo_root: Path, profile: Profile
) -> tuple[Path, tuple[str, ...], dict[str, str]]:
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TZ": "UTC",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "HF_HUB_OFFLINE": "1",
        "HF_DATASETS_OFFLINE": "1",
        "HF_HUB_DISABLE_TELEMETRY": "1",
        "DO_NOT_TRACK": "1",
    }
    if action.launcher == "repo":
        assert action.entrypoint is not None
        path = repo_root / action.entrypoint
        return path, (str(path),), environment
    if action.launcher == "corpus":
        path = Path(sys.executable).resolve(strict=True)
        environment["PYTHONPATH"] = str(repo_root / "corpus" / "src")
        return path, (str(path), "-B", "-m", "himr_corpus"), environment
    if action.launcher == "trusted_host":
        if action.entrypoint != str(TRUSTED_GPU_LAUNCHER):
            raise RegistryError("trusted-host launcher path is not the exact allowlisted path")
        return TRUSTED_GPU_LAUNCHER, (str(TRUSTED_GPU_LAUNCHER),), environment
    if action.launcher == "local_private_host":
        if action.entrypoint is not None or action.entrypoint_parameter != "local_launcher":
            raise RegistryError("local-private action has an invalid entrypoint contract")
        path = _lexical_path(
            profile.parameters.get("local_launcher"),
            repo_root=repo_root,
            label="parameter local_launcher",
        )
        try:
            info = path.lstat()
        except OSError as error:
            raise RegistryError("local-private launcher is unavailable") from error
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o500
        ):
            raise RegistryError(
                "local-private launcher must be current-user, single-link mode 0500"
            )
        return path, (str(path),), environment
    raise RegistryError(f"unsupported launcher {action.launcher!r}")


def _validate_trusted_host_entrypoint(path: Path, info: os.stat_result) -> None:
    if (
        path != TRUSTED_GPU_LAUNCHER
        or info.st_uid != 0
        or info.st_gid != 0
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o555
    ):
        raise RegistryError(
            "trusted GPU launcher must be the exact root-owned, root-group, "
            "single-link mode-0555 install"
        )
    current = path.parent
    while True:
        try:
            ancestor = current.lstat()
        except OSError as error:
            raise RegistryError(
                f"cannot inspect trusted launcher ancestor {current}: {error}"
            ) from error
        if (
            not stat.S_ISDIR(ancestor.st_mode)
            or ancestor.st_uid != 0
            or stat.S_IMODE(ancestor.st_mode) & 0o022
        ):
            raise RegistryError(
                "trusted GPU launcher ancestors must be root-owned directories "
                "without group/other write access"
            )
        if current == Path("/"):
            break
        current = current.parent


def _validate_systemd_user_tools() -> None:
    for path in SYSTEMD_USER_TOOLS:
        try:
            _assert_no_symlink_components(path, str(path))
            info = path.lstat()
        except (OSError, RegistryError) as error:
            raise RegistryError(
                "systemd_user requires root-owned /usr/bin/systemd-run, "
                "/usr/bin/systemctl, and /usr/bin/env"
            ) from error
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 0
            or info.st_mode & 0o022
            or not os.access(path, os.X_OK)
        ):
            raise RegistryError(
                "systemd_user control tools must be root-owned, executable, and not "
                "group/world-writable"
            )


def prepare_profile(profile_set: ProfileSet, profile_id: str, repo_root: Path) -> PreparedCommand:
    repo_root = repo_root.resolve(strict=True)
    matches = [profile for profile in profile_set.profiles if profile.profile_id == profile_id]
    if len(matches) != 1:
        raise RegistryError(f"unknown profile {profile_id!r}")
    profile = matches[0]
    action = ACTIONS[profile.action_id]
    if not action.enabled:
        raise RegistryError(action.blocked_reason or "action is blocked")
    if (
        isinstance(action.timeout_seconds, bool)
        or not isinstance(action.timeout_seconds, int)
        or not 1 <= action.timeout_seconds <= MAX_ACTION_TIMEOUT_SECONDS
    ):
        raise RegistryError(f"action {action.action_id!r} has an invalid timeout")
    if action.supervisor not in {"direct", "systemd_user"}:
        raise RegistryError(f"action {action.action_id!r} has an unsupported supervisor")
    envelope_values = (
        action.memory_max_bytes,
        action.tasks_max,
        action.file_size_max_bytes,
        action.stop_timeout_seconds,
        action.memory_swap_max_bytes,
    )
    if action.supervisor == "direct" and any(value is not None for value in envelope_values):
        raise RegistryError(
            f"action {action.action_id!r} binds a cgroup envelope without the systemd-user supervisor"
        )
    if action.supervisor == "systemd_user" and (
        isinstance(action.memory_max_bytes, bool)
        or not isinstance(action.memory_max_bytes, int)
        or not 256 * 1024 * 1024 <= action.memory_max_bytes <= 2**53 - 1
    ):
        raise RegistryError(
            f"action {action.action_id!r} must bind a 256 MiB-or-greater MemoryMax"
        )
    if action.supervisor == "systemd_user" and action.resource not in {
        "gpu",
        "autonomous_pipeline",
    }:
        raise RegistryError(
            f"action {action.action_id!r} may use systemd_user only for a closed long-running resource"
        )
    if action.supervisor == "systemd_user":
        for label, value, minimum, maximum in (
            ("TasksMax", action.tasks_max, 1, 4096),
            ("LimitFSIZE", action.file_size_max_bytes, 16 * 1024 * 1024, 2**53 - 1),
            ("TimeoutStopSec", action.stop_timeout_seconds, 1, 300),
            ("MemorySwapMax", action.memory_swap_max_bytes, 0, 2**53 - 1),
        ):
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise RegistryError(
                    f"action {action.action_id!r} has an invalid {label} override"
                )
        _validate_systemd_user_tools()
    expected_names = {field.name for field in action.fields}
    if action.entrypoint_parameter is not None:
        expected_names.add(action.entrypoint_parameter)
    if set(profile.parameters) - expected_names:
        raise RegistryError(f"profile {profile_id!r} has unknown parameters")
    if any(_contains_forbidden_word(key) for key in profile.parameters):
        raise RegistryError(f"profile {profile_id!r} contains a forbidden parameter name")
    argv_path, argv_prefix, environment = _entrypoint(action, repo_root, profile)
    entrypoint_body, entrypoint_info = _stable_file(
        argv_path,
        maximum=4 * 1024 * 1024,
        require_owner=action.launcher in {"repo", "local_private_host"},
        require_single_link=action.launcher in {"repo", "trusted_host", "local_private_host"},
    )
    if action.launcher == "trusted_host":
        _validate_trusted_host_entrypoint(argv_path, entrypoint_info)
    if action.launcher == "local_private_host" and (
        entrypoint_info.st_uid != os.geteuid()
        or entrypoint_info.st_nlink != 1
        or stat.S_IMODE(entrypoint_info.st_mode) != 0o500
    ):
        raise RegistryError(
            "local-private launcher changed from its current-user mode-0500 binding"
        )
    if not os.access(argv_path, os.X_OK):
        raise RegistryError(f"entrypoint is not executable: {argv_path}")
    argv = [*argv_prefix, *action.prefix]
    for field in action.fields:
        if field.name not in profile.parameters:
            if field.required:
                raise RegistryError(f"profile {profile_id!r} is missing {field.name}")
            continue
        prepared = _validated_parameter(field, profile.parameters[field.name], repo_root=repo_root)
        if field.kind == "true":
            argv.append(field.flag)
        elif field.kind == "ordinal_list":
            assert isinstance(prepared, tuple)
            for item in prepared:
                argv.extend((field.flag, item))
        else:
            argv.extend((field.flag, str(prepared)))
    return PreparedCommand(
        profile=profile,
        action=action,
        argv=tuple(argv),
        cwd=repo_root,
        environment=environment,
        entrypoint_path=argv_path,
        entrypoint_sha256=hashlib.sha256(entrypoint_body).hexdigest(),
        entrypoint_byte_count=entrypoint_info.st_size,
        profile_set_sha256=profile_set.raw_sha256,
    )


def public_actions() -> list[dict[str, Any]]:
    return [
        {
            "action_id": action.action_id,
            "stage": action.stage,
            "label": action.label,
            "description": action.description,
            "effect": action.effect,
            "resource": action.resource,
            "supervisor": action.supervisor,
            "timeout_seconds": action.timeout_seconds,
            "memory_max_bytes": action.memory_max_bytes,
            "tasks_max": action.tasks_max,
            "file_size_max_bytes": action.file_size_max_bytes,
            "stop_timeout_seconds": action.stop_timeout_seconds,
            "memory_swap_max_bytes": action.memory_swap_max_bytes,
            "enabled": action.enabled,
            "blocked_reason": action.blocked_reason,
            "confirmation": action.confirmation,
        }
        for action in ACTIONS.values()
    ]


def public_profiles(profile_set: ProfileSet) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for profile in profile_set.profiles:
        action = ACTIONS[profile.action_id]
        result.append(
            {
                "profile_id": profile.profile_id,
                "label": profile.label,
                "description": profile.description,
                "action_id": profile.action_id,
                "stage": action.stage,
                "effect": action.effect,
                "resource": action.resource,
                "supervisor": action.supervisor,
                "timeout_seconds": action.timeout_seconds,
                "memory_max_bytes": action.memory_max_bytes,
                "tasks_max": action.tasks_max,
                "file_size_max_bytes": action.file_size_max_bytes,
                "stop_timeout_seconds": action.stop_timeout_seconds,
                "memory_swap_max_bytes": action.memory_swap_max_bytes,
                "enabled": action.enabled,
                "blocked_reason": action.blocked_reason,
                "confirmation": action.confirmation,
            }
        )
    return result
