"""Read cached pipeline statistics without starting a console or any work."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from autonomous_controller.public_status import read_public_status


def read_longform_statistics(config: Path, expected_sha256: str) -> dict:
    # A broken optional reader must not prevent the ordinary cached report.
    from .longform_statistics import read_longform_statistics as read
    return read(config, expected_sha256)


def unavailable_statistics(_diagnostic: str) -> dict:
    return {"schema_version": 1, "state": "unavailable", "updated_at": None,
            "lifecycle": None, "counts": None, "completion_percent": None,
            "basis": "cached_companion_status_completed_recording_jobs",
            "last_error": None, "diagnostic": "statistics_reader_failure"}


def collect_statistics(config: Path, expected_sha256: str) -> dict:
    controller = {"state": "unavailable", "updated_at": None,
                  "actual_state": None, "desired_state": None,
                  "queued_items": None, "preprocessed_items": None,
                  "asr_completed_items": None, "diagnostic": "controller_status_unavailable"}
    try:
        status = read_public_status(config, expected_sha256)
        telemetry = status["pipeline_telemetry"]
        controller = {"state": "available", "updated_at": status.get("updated_at"),
                      "actual_state": status.get("actual_state"), "desired_state": status.get("desired_state"),
                      **{key: telemetry[key] for key in ("queued_items", "preprocessed_items", "asr_completed_items")},
                      "diagnostic": None}
    except Exception:
        pass
    try:
        longform = read_longform_statistics(config, expected_sha256)
    except Exception:
        longform = unavailable_statistics("reader_error")
    return {"kind": "himr_cached_pipeline_statistics", "schema_version": 1,
            "controller": controller, "longform": longform,
            "combined_unique_asr_complete": None,
            "combined_count_basis": "unavailable_cross_lane_deduplication_not_proven",
            "source": "independently_timestamped_cached_status_documents",
            "read_only": True, "media_scanned": False}


def format_statistics(report: dict) -> str:
    controller, longform = report["controller"], report["longform"]
    number = lambda value: "Not reported" if value is None else f"{value:,}"
    lines = [
        f"Standard-queue ASR complete: {number(controller['asr_completed_items'])}",
        f"Standard-queue preprocessed: {number(controller['preprocessed_items'])}",
        f"Standard queue: {number(controller['queued_items'])}",
        f"Controller: {controller['actual_state'] or 'Unavailable'}; snapshot {controller['updated_at'] or 'Not reported'}",
    ]
    if longform["state"] != "available":
        lines.append("Long-form ASR: " + ("Not registered" if longform["state"] == "not_registered" else "Unavailable"))
    else:
        counts = longform["counts"]
        lines.extend([
            f"Long-form ASR complete: {number(counts['completed_recordings'])} / {number(counts['discovered_recordings'])} discovered recordings",
            f"Long-form remaining discovered: {number(counts['remaining_discovered_recordings'])}",
            f"Expected cold candidates not yet discovered: {number(counts['cold_candidates_not_discovered'])}",
            f"Long-form stages: {number(counts['unprepared_recordings'])} unprepared; "
            f"{number(counts['preprocessed_recordings'])} preprocessed; "
            f"{number(counts['prepared_recordings'])} prepared; {number(counts['incomplete_recordings'])} incomplete",
            f"Long-form lifecycle: {longform['lifecycle']}; active recordings: {number(counts['active_recordings'])}",
            f"Long-form snapshot: {longform['updated_at']}",
        ])
        if longform["completion_percent"] is not None:
            lines.append(f"Long-form progress: {longform['completion_percent']:.1f}% of discovered recordings only")
    lines.append("Cached counts, not a new receipt verification. Lane totals are separate, not summed.")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--expected-config-sha256", required=True)
    parser.add_argument("--json", action="store_true", help="emit bounded machine-readable statistics")
    args = parser.parse_args(argv)
    report = collect_statistics(args.config, args.expected_config_sha256)
    print(json.dumps(report, sort_keys=True, allow_nan=False) if args.json else format_statistics(report))
    return 2 if report["controller"]["state"] == "unavailable" or report["longform"]["state"] == "unavailable" else 0


if __name__ == "__main__":
    raise SystemExit(main())
