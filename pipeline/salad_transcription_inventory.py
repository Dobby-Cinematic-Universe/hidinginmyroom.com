#!/usr/bin/env python3
"""Read-only selection of already-admitted local audio for a future cloud plan.

This is a metadata snapshot, not a reservation of local work. It never prepares
audio, reads media or transcript contents, creates discovery receipts, or changes
campaign state. Stop competing local work before acting on a cloud selection.
"""

from __future__ import annotations

import argparse
import os
import stat
import sys
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import longform_asr_campaign as campaign  # noqa: E402
from himr_corpus.longform_asr_planner import (  # noqa: E402
    LongformPlanningError,
    validate_recording_manifest,
)


class CloudInventoryError(RuntimeError):
    """The bounded metadata snapshot could not be obtained safely."""


def _load_config(path: Path, expected_sha256: str) -> campaign.CampaignConfig:
    """Replay the pinned config, without admission or executable/media replay."""
    path = campaign._absolute_path(str(path), "campaign config path")
    expected = campaign._digest(expected_sha256, "campaign config SHA-256")
    body = campaign._stable_file(
        path,
        "campaign config",
        allowed_modes=frozenset({0o400, 0o444}),
    )
    if campaign.sha256_bytes(body) != expected:
        raise CloudInventoryError("campaign config differs from its supplied SHA-256")
    document = campaign._normalize_config(campaign._strict_json(body, "campaign config"))
    if campaign.canonical_bytes(document) != body:
        raise CloudInventoryError("campaign config is not canonical")
    # The ordinary config loader additionally hashes execution tools. Inventory
    # needs only the externally pinned metadata, not execution readiness checks.
    return campaign.CampaignConfig(document, path, expected)


def _stat_optional(path: Path, label: str, *, directory: bool = False) -> os.stat_result | None:
    try:
        observed = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise CloudInventoryError(f"cannot inspect {label}: {error}") from error
    valid_type = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        not valid_type(observed.st_mode)
        or observed.st_uid not in {os.geteuid(), 0}
        or stat.S_IMODE(observed.st_mode) & 0o022
        or (not directory and observed.st_nlink != 1)
        or path.resolve(strict=True) != path
    ):
        raise CloudInventoryError(f"{label} has unsafe metadata")
    return observed


def _completion(config: campaign.CampaignConfig, job: dict[str, Any]) -> str | None:
    path = Path(job["paths"]["completion"])
    if _stat_optional(path, "local completion receipt") is None:
        return None
    body = campaign._stable_file(
        path,
        "local completion receipt",
        maximum=1024 * 1024,
        allowed_modes=frozenset({0o400}),
    )
    value = campaign._exact(
        campaign._strict_json(body, "local completion receipt"),
        "local completion receipt",
        {
            "kind", "schema_version", "job_id", "campaign_config_id",
            "candidate_identity_sha256", "transcript", "runner", "assembler", "policy",
        },
    )
    if (
        campaign.canonical_bytes(value) != body
        or value["kind"] != "himr_longform_asr_campaign_completion"
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["job_id"] != job["job_id"]
        or value["campaign_config_id"] != config.config_id
        or value["candidate_identity_sha256"] != job["source"]["identity_sha256"]
        or value["policy"] != {
            "machine_generated": True,
            "human_review_required": True,
            "publication_authority": "none",
            "catalogue_mutation_authority": "none",
        }
    ):
        raise CloudInventoryError("local completion receipt has a different job binding")
    transcript = campaign._exact(
        value["transcript"], "completion transcript binding", {"path", "sha256", "byte_count"}
    )
    campaign._digest(transcript["sha256"], "completion transcript SHA-256")
    campaign._integer(transcript["byte_count"], "completion transcript bytes", 1, 2**63 - 1)
    if transcript["path"] != job["paths"]["transcript"]:
        raise CloudInventoryError("completion transcript path differs from its job")
    runner, assembler = value["runner"], value["assembler"]
    if (
        not isinstance(runner, dict)
        or runner.get("status") != "completed"
        or runner.get("bindings_path") != job["paths"]["bindings"]
        or not isinstance(assembler, dict)
        or assembler.get("status") != "completed"
        or assembler.get("coverage_complete") is not True
        or assembler.get("output") != transcript
    ):
        raise CloudInventoryError("completion execution receipts are inconsistent")
    campaign._digest(runner.get("bindings_sha256"), "completion bindings SHA-256")
    observed = _stat_optional(Path(transcript["path"]), "completed local transcript")
    if observed is None or observed.st_size != transcript["byte_count"]:
        raise CloudInventoryError("completed local transcript is absent or has a different size")
    # Deliberately do not open transcript contents. This receipt is an exclusion
    # signal, not a fresh claim that the transcript's content hash was replayed.
    return campaign.sha256_bytes(body)


def _partial(job: dict[str, Any]) -> bool:
    for name in ("bindings", "transcript"):
        if _stat_optional(Path(job["paths"][name]), f"local {name}") is not None:
            return True
    results = Path(job["paths"]["results"])
    if _stat_optional(results, "local span results directory", directory=True) is not None:
        # Any entry, including an interrupted span's staging directory, is enough
        # to require explicit consent to whole-recording paid retranscription.
        with os.scandir(results) as entries:
            return next(entries, None) is not None
    return False


def _recording(job: dict[str, Any]) -> dict[str, str] | None:
    path = Path(job["paths"]["recording_input"])
    if _stat_optional(path, "recording input manifest") is None:
        return None
    job_path = Path(job["paths"]["root"]) / "job.json"
    if _stat_optional(job_path, "local job receipt") is None:
        raise CloudInventoryError("recording input has no committed local job receipt")
    job_body = campaign._stable_file(
        job_path, "local job receipt", maximum=1024 * 1024, allowed_modes=frozenset({0o400})
    )
    if job_body != campaign.canonical_bytes(job):
        raise CloudInventoryError("local job receipt differs from the discovered candidate")
    body = campaign._stable_file(
        path, "recording input manifest", maximum=64 * 1024 * 1024,
        allowed_modes=frozenset({0o400}),
    )
    value = campaign._strict_json(body, "recording input manifest")
    normalized = validate_recording_manifest(value)
    if campaign.canonical_bytes(normalized) != body:
        raise CloudInventoryError("recording input manifest is not canonical")
    if normalized["recording"]["recording_id"] != job["job_id"]:
        raise CloudInventoryError("recording input manifest belongs to a different local job")
    candidate = job["source"]
    if candidate["candidate_kind"] == "gpu_queue_requires_chunking":
        expected_audio = candidate["audio"]
        actual_audio = normalized["recording"]["input"]
        if (
            normalized["recording"]["media_id"] != expected_audio["media_id"]
            or any(actual_audio[key] != expected_audio[key] for key in (
                "artifact_id", "path", "sha256", "byte_count"
            ))
            or not campaign._duration_ms_matches_exact_samples(
                expected_audio["duration_ms"], actual_audio
            )
        ):
            raise CloudInventoryError("recording input differs from its admitted queue audio")
    return {"recording_input": str(path), "sha256": campaign.sha256_bytes(body)}


def inventory(
    config_path: Path, expected_sha256: str, *, include_partial: bool = False
) -> dict[str, Any]:
    """Return a metadata-only selection; never admit, prepare, or reserve work."""
    if type(include_partial) is not bool:
        raise CloudInventoryError("include_partial must be a boolean")
    try:
        config = _load_config(config_path, expected_sha256)
        candidates = campaign.discover_candidates(
            config, admit_new_cold=False, admit_new_queues=False
        )
        recordings: list[dict[str, str]] = []
        exclusions: list[dict[str, str]] = []
        partial_included: list[str] = []
        counts = {
            "admitted_candidates": len(candidates),
            "selected": 0,
            "completed": 0,
            "partial_local_work": 0,
            "needs_audio_preparation": 0,
            "included_partial_local_work": 0,
        }
        seen_jobs: set[str] = set()
        for candidate in candidates:
            job = campaign._job(config, candidate)
            if job["job_id"] in seen_jobs:
                raise CloudInventoryError("discovery repeated a local job")
            seen_jobs.add(job["job_id"])
            root = Path(job["paths"]["root"])
            if _stat_optional(root, "local job directory", directory=True) is None:
                reason = "needs_audio_preparation"
                exclusions.append({"job_id": job["job_id"], "reason": reason})
                counts[reason] += 1
                continue
            completion_sha256 = _completion(config, job)
            if completion_sha256 is not None:
                counts["completed"] += 1
                exclusions.append({
                    "job_id": job["job_id"], "reason": "completed",
                    "completion_sha256": completion_sha256,
                })
                continue
            partial = _partial(job)
            if partial and not include_partial:
                counts["partial_local_work"] += 1
                exclusions.append({"job_id": job["job_id"], "reason": "partial_local_work"})
                continue
            recording = _recording(job)
            if recording is None:
                counts["needs_audio_preparation"] += 1
                exclusions.append({"job_id": job["job_id"], "reason": "needs_audio_preparation"})
                continue
            # Check terminal/partial markers a second time after manifest reading.
            # This narrows races but deliberately does not claim a dispatch lease.
            completion_sha256 = _completion(config, job)
            if completion_sha256 is not None:
                counts["completed"] += 1
                exclusions.append({
                    "job_id": job["job_id"], "reason": "completed",
                    "completion_sha256": completion_sha256,
                })
                continue
            partial = partial or _partial(job)
            if partial and not include_partial:
                counts["partial_local_work"] += 1
                exclusions.append({"job_id": job["job_id"], "reason": "partial_local_work"})
                continue
            if partial:
                partial_included.append(job["job_id"])
                counts["included_partial_local_work"] += 1
            recordings.append(recording)
        counts["selected"] = len(recordings)
        return {
            "kind": "himr_salad_input_selection",
            "schema_version": 1,
            "recordings": recordings,
            "source_campaign": {
                "path": str(config.path), "sha256": config.physical_sha256,
                "config_id": config.config_id,
            },
            "counts": counts,
            "excluded": exclusions,
            "partial_local_job_ids": partial_included,
            "policy": {
                "metadata_only": True,
                "snapshot_only": True,
                "source_controller_mutated": False,
                "media_content_verified": False,
                "transcript_content_verified": False,
                "complete_jobs_excluded": True,
                "partial_local_work_allowed": include_partial,
                "whole_recording_paid_retranscription_warning": bool(partial_included),
                "cloud_resumes_local_spans": False,
                "stop_competing_local_work_before_cloud_dispatch": True,
            },
        }
    except CloudInventoryError:
        raise
    except (campaign.CampaignError, LongformPlanningError, OSError, KeyError, TypeError, ValueError) as error:
        raise CloudInventoryError(f"cloud inventory failed closed: {error}") from error


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-config", required=True, type=Path)
    parser.add_argument("--campaign-config-sha256", required=True)
    parser.add_argument(
        "--include-partial-local-work", action="store_true",
        help="include interrupted local jobs for whole-recording paid retranscription",
    )
    args = parser.parse_args(argv)
    try:
        result = inventory(
            args.campaign_config, args.campaign_config_sha256,
            include_partial=args.include_partial_local_work,
        )
    except CloudInventoryError as error:
        print(f"CloudInventoryError: {error}", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(campaign.canonical_bytes(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
