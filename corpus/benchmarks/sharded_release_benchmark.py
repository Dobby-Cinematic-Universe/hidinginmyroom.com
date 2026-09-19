#!/usr/bin/env python3
"""Dependency-light scale benchmark for the v2 static release format.

The default run materially writes and validates 100,000 transcript segments.
The million-segment figure is explicitly a byte projection from that measured
run; pass ``--segments 1000000`` when a host has enough time and memory for an
empirical million-segment run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "corpus" / "src"))

from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.sharded_release import (  # noqa: E402
    export_release_v2_from_release,
    validate_sharded_release,
)


def identifier(prefix: str, value: int) -> str:
    return f"{prefix}_{value:032x}"


def synthetic_release(segment_count: int, recording_count: int) -> dict:
    if segment_count < recording_count:
        raise ValueError("segment count must be at least the recording count")
    base, remainder = divmod(segment_count, recording_count)
    recordings: list[dict] = []
    next_segment = 1
    for index in range(recording_count):
        local_count = base + (1 if index < remainder else 0)
        segments = []
        for ordinal in range(local_count):
            start_ms = ordinal * 2_000
            segments.append(
                {
                    "segment_id": identifier("seg", next_segment),
                    "start_ms": start_ms,
                    "end_ms": start_ms + 1_800,
                    "text": f"Synthetic reviewed transcript segment {next_segment} for scale measurement.",
                    "speaker_label": "Daniel",
                    "confidence_band": "human",
                    "calibrated_probability": None,
                }
            )
            next_segment += 1
        record_number = index + 1
        recordings.append(
            {
                "recording_id": identifier("rec", record_number),
                "slug": f"synthetic-recording-{record_number:06d}",
                "title": f"Synthetic recording {record_number:06d}",
                "date_label": "2026-08-26",
                "date_year": 2026,
                "date_basis": "benchmark_fixture",
                "duration_ms": local_count * 2_000,
                "recording_type": "video",
                "review_state": "reviewed",
                "sources": [
                    {
                        "source_id": identifier("src", record_number),
                        "platform": "archive.org",
                        "url": f"https://archive.org/details/synthetic-{record_number:06d}",
                        "native_id": f"synthetic-{record_number:06d}",
                        "access_state": "public",
                    }
                ],
                "transcript_revisions": [
                    {
                        "revision_id": identifier("rev", record_number),
                        "revision_kind": "human_verbatim",
                        "language": "en",
                        "review_state": "media_checked",
                        "machine_generated": False,
                        "unreviewed": False,
                        "verified_quotation": False,
                        "disclaimer_code": "reviewed_transcript_not_fact_checked_v1",
                        "lifecycle_state": "active",
                        "lifecycle_history": [],
                        "segments": segments,
                    }
                ],
            }
        )
    payload = {
        "schema_version": 1,
        "generated_at": "2026-08-26T20:00:00Z",
        "counts": {
            "recordings": recording_count,
            "sources": recording_count,
            "transcript_revisions": recording_count,
            "segments": segment_count,
        },
        "recordings": recordings,
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return {
        "schema_version": 1,
        "release_id": f"release_{digest[:24]}",
        "generated_at": payload["generated_at"],
        "counts": payload["counts"],
        "recordings": recordings,
    }


def directory_measurements(root: Path, release_id: str) -> dict:
    release_root = root / "releases" / release_id
    detail_sizes = [path.stat().st_size for path in (release_root / "recordings").glob("*.json")]
    catalog_sizes = [path.stat().st_size for path in (release_root / "catalog").glob("*.json")]
    return {
        "manifest_bytes": (root / "manifest.json").stat().st_size,
        "recording_shard_bytes": sum(detail_sizes),
        "largest_recording_shard_bytes": max(detail_sizes, default=0),
        "catalog_shard_bytes": sum(catalog_sizes),
        "largest_catalog_shard_bytes": max(catalog_sizes, default=0),
        "release_tree_bytes": sum(path.stat().st_size for path in release_root.rglob("*") if path.is_file()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segments", type=int, default=100_000)
    parser.add_argument("--recordings", type=int, default=100)
    parser.add_argument("--catalog-shard-size", type=int, default=100)
    parser.add_argument("--estimate-segments", type=int, default=1_000_000)
    parser.add_argument("--output", type=Path, help="retain artifacts in this explicit directory")
    args = parser.parse_args()
    if args.segments <= 0 or args.recordings <= 0 or args.estimate_segments <= 0:
        parser.error("counts must be positive")

    started = time.perf_counter()
    release = synthetic_release(args.segments, args.recordings)
    generated_seconds = time.perf_counter() - started

    temporary = None
    if args.output is None:
        temporary = tempfile.TemporaryDirectory(prefix="himr-v2-benchmark-")
        output = Path(temporary.name)
    else:
        output = args.output.resolve()
        if output == Path(output.anchor):
            parser.error("refusing to use a filesystem root as benchmark output")

    try:
        export_started = time.perf_counter()
        exported = export_release_v2_from_release(
            release,
            output,
            catalog_shard_size=args.catalog_shard_size,
        )
        export_seconds = time.perf_counter() - export_started
        validate_started = time.perf_counter()
        validated = validate_sharded_release(output / "manifest.json")
        validate_seconds = time.perf_counter() - validate_started
        sizes = directory_measurements(output, exported["release_id"])
        projection_ratio = args.estimate_segments / args.segments
        projection = {
            "kind": "linear byte projection from empirical recording shards; not a timed run",
            "segments": args.estimate_segments,
            "recording_shard_bytes": round(sizes["recording_shard_bytes"] * projection_ratio),
        }
        result = {
            "schema_version": 1,
            "empirical": {
                "segments": args.segments,
                "recordings": args.recordings,
                "generation_seconds": round(generated_seconds, 6),
                "export_seconds": round(export_seconds, 6),
                "validation_seconds": round(validate_seconds, 6),
                "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                **sizes,
                "catalog_shards": validated["catalog_shards"],
            },
            "projection": projection,
        }
        print(json.dumps(result, indent=2, sort_keys=True))
    finally:
        if temporary is not None:
            temporary.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
