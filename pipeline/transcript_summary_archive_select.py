"""Prepare an offline, metadata-bound selection of completed archive transcripts.

No model calls, credentials, raw-media reads, transcript-text reads, or campaign
manifest sealing. The campaign runner must validate selected transcript bytes.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import transcript_summary as runner

STATE = ROOT / "research/operator-state"
INVENTORY = ROOT / "research/corpus/speaker-screen-campaigns/archive-fast-20260912/inventory.json"
LONGFORM_ROOTS = tuple(STATE / name for name in (
    "longform-asr-archive-all-known-2026-08-30",
    "longform-asr-archive-699994-update-2026-09-12-v2"))
NORMALIZED_ROOTS = tuple(STATE / name for name in (
    "autonomous-archive-all-known-2026-08-29",
    "autonomous-archive-all-known-2026-08-30",
    "autonomous-archive-699994-update-2026-09-12"))
CANARY = "himrlongjob_e29c548056b03029e0c225bf23afc8e1"
KIND = "himr_private_transcript_archive_selection"


class SelectionError(RuntimeError):
    pass


def _ref(value):
    result = {key: value[key] for key in ("path", "sha256")}
    runner.safe.file_binding(result)
    return result


class Metadata:
    def __init__(self):
        self.documents = {}
        self.bindings = {}
        self.mtimes = {}

    def read(self, path, expected=None):
        path = str(runner.safe.path_value(path))
        if path not in self.documents:
            with runner.safe.opened(path) as fd:
                before = runner.safe.witness(fd)
                if not 0 < before["st_size"] <= runner.MAX_JSON:
                    raise SelectionError("metadata exceeds the bounded JSON size")
                raw = os.pread(fd, runner.MAX_JSON + 1, 0)
                if len(raw) != before["st_size"] or runner.safe.witness(fd) != before:
                    raise SelectionError("metadata changed during selection")
            value = runner.parse(raw)
            if not isinstance(value, dict):
                raise SelectionError("metadata must be a JSON object")
            self.documents[path] = value
            self.bindings[path] = {"path": path, "sha256": hashlib.sha256(raw).hexdigest()}
            self.mtimes[path] = before["st_mtime_ns"]
        if expected is not None and self.bindings[path] != _ref(expected):
            raise SelectionError("metadata binding differs from its parent receipt")
        return self.documents[path]

    def bound(self, value):
        ref = _ref(value)
        return self.read(ref["path"], ref)


def _sha(value):
    if not isinstance(value, str) or not runner.safe.SHA.fullmatch(value):
        raise SelectionError("invalid original-media identity")
    return value


def _original_from_preprocess(metadata, ref, audio):
    pre = metadata.bound(ref)
    if pre.get("status") != "completed":
        raise SelectionError("preprocessing lineage is not completed")
    matches = [item for item in pre.get("artifacts", [])
               if all(item.get(key) == audio[key] for key in ("artifact_id", "path", "sha256"))]
    if len(matches) != 1:
        raise SelectionError("preprocessing does not bind the selected ASR audio")
    return _sha(pre["input"]["sha256"])


def _transcript_artifact(value, expected_path):
    ref = _ref(value)
    if ref["path"] != str(expected_path):
        raise SelectionError("transcript artifact is outside its completed result folder")
    count = value.get("byte_count")
    if type(count) is not int or not 0 < count <= runner.sources_module.MAX_JSON_BYTES:
        raise SelectionError("transcript metadata exceeds the source JSON byte bound")
    # Stat the retained JSON only. Do not read text or any raw media here.
    with runner.safe.opened(ref["path"]) as fd:
        if os.fstat(fd).st_size != count:
            raise SelectionError("transcript byte count differs from completion receipt")
    return ref


def _candidate(spec, media_sha, receipt, byte_count, segment_count, duration_ms,
               *, preference, text_characters=None):
    runner.sources_module.validate_spec(spec)
    for number in (byte_count, segment_count, duration_ms):
        if type(number) is not int or number < 0:
            raise SelectionError("invalid completed transcript metadata count")
    if text_characters is not None and (type(text_characters) is not int or text_characters < 0):
        raise SelectionError("invalid completed transcript character count")
    return {"source": spec, "original_media_sha256": media_sha, "completion_receipt": receipt,
            "transcript_bytes": byte_count, "segment_count": segment_count,
            "duration_ms": duration_ms, "text_character_count": text_characters,
            "preference": preference}


def build_selection(*, inventory=INVENTORY, longform_roots=LONGFORM_ROOTS,
                    normalized_roots=NORMALIZED_ROOTS, canary_job_id=CANARY,
                    expected_audio_count=4061, shard_size=16):
    """Read bounded receipts, deduplicate original media, and return an unsealed selection."""
    if type(shard_size) is not int or not 1 <= shard_size <= 32:
        raise SelectionError("shard size must be 1..32")
    metadata = Metadata()
    inventory = str(runner.safe.path_value(inventory))
    catalogue = metadata.read(inventory)
    if catalogue.get("kind") != "himr_private_speaker_screen_archive_inventory" or catalogue.get("schema_version") != 1:
        raise SelectionError("unsupported archive inventory")
    records = catalogue.get("records")
    if not isinstance(records, list):
        raise SelectionError("inventory requires recording records")
    audio = set()
    for row in records:
        streams = row.get("audio_stream_count")
        if type(streams) is not int or streams < 0:
            raise SelectionError("inventory audio count is invalid")
        if streams:
            media_sha = _sha(row["recording"]["sha256"])
            if media_sha in audio:
                raise SelectionError("inventory repeats an original audio identity")
            audio.add(media_sha)
    if len(audio) != expected_audio_count or not 0 < len(audio) <= 4096:
        raise SelectionError("inventory audio count differs from the expected complete selection")
    candidates = {}
    receipt_counts = Counter()

    def admit(row):
        if row["original_media_sha256"] not in audio:
            raise SelectionError("completed transcript is outside the audio inventory")
        candidates.setdefault(row["original_media_sha256"], []).append(row)

    for rank, root in enumerate(longform_roots):
        root = runner.safe.path_value(root)
        for path in sorted((root / "jobs").glob("*/completion.json")):
            done = metadata.read(path)
            job = metadata.read(path.parent / "job.json")
            receipt_counts["longform"] += 1
            if (done.get("kind") != "himr_longform_asr_campaign_completion"
                    or job.get("kind") != "himr_longform_asr_campaign_job"
                    or done.get("job_id") != job.get("job_id")
                    or done.get("runner", {}).get("status") != "completed"
                    or done.get("assembler", {}).get("status") != "completed"
                    or done["assembler"].get("coverage_complete") is not True):
                raise SelectionError("longform completion/job metadata is inconsistent")
            source = job["source"]
            if "source_media" in source:
                media_sha = _sha(source["source_media"]["sha256"])
                duration = source["source_media"]["duration_ms"]
            else:
                media_sha = _original_from_preprocess(metadata, source["preprocess_result"], source["audio"])
                duration = source["audio"]["duration_ms"]
            transcript = _transcript_artifact(done["transcript"], path.parent / "recording-transcript.json")
            spec = {"transcript": transcript, "format": "longform", "recording_id": job["job_id"],
                    "title": None, "date": None, "completion": None}
            admit(_candidate(spec, media_sha, metadata.bindings[str(path)], done["transcript"]["byte_count"],
                done["assembler"]["segment_count"], duration,
                preference=(1, rank, metadata.mtimes[str(path)], str(path))))

    for rank, root in enumerate(normalized_roots):
        root = runner.safe.path_value(root)
        folder = root / "gpu-results/asr/faster-whisper-gpu-v5/sha256"
        for path in sorted(folder.glob("*/*/results/*/result.json")):
            done = metadata.read(path)
            receipt_counts["normalized"] += 1
            if done.get("kind") != "himr_faster_whisper_gpu_result" or done.get("status") != "completed":
                raise SelectionError("normalized ASR result is not completed")
            work_id = done["work_order"]["work_order_id"]
            if not isinstance(work_id, str) or not work_id.startswith("gpuasrwo5_") or "/" in work_id:
                raise SelectionError("invalid completed ASR work order ID")
            work = metadata.read(root / "gpu-work-orders/work-orders" / (work_id + ".json"))
            if (work.get("work_order_id") != work_id or
                    work.get("identity_sha256") != done["work_order"].get("identity_sha256")):
                raise SelectionError("ASR work order identity differs")
            audio_input = {"artifact_id": work["input"]["artifact_id"],
                           "path": work["input"]["path"], "sha256": work["input"]["expected_sha256"]}
            if (done["input"]["artifact_id"] != audio_input["artifact_id"] or
                    done["input"]["sha256"] != audio_input["sha256"] or
                    done["input"].get("timeline_offset_ms") != 0):
                raise SelectionError("completed ASR input differs or is not a whole-recording source")
            media_sha = _original_from_preprocess(metadata, work["source_lineage"]["preprocess_result"], audio_input)
            artifacts = [row for row in done.get("artifacts", [])
                         if row.get("artifact_kind") == "transcript_normalized_json"]
            if len(artifacts) != 1:
                raise SelectionError("ASR completion lacks a unique normalized transcript")
            artifact = artifacts[0]
            transcript = _transcript_artifact(artifact, path.parent / "transcript.normalized.json")
            spec = {"transcript": transcript, "format": "normalized", "recording_id": "media_sha256_" + media_sha,
                    "title": None, "date": None, "completion": metadata.bindings[str(path)]}
            completed_at = done["execution"]["completed_at"]
            if not isinstance(completed_at, str) or not completed_at.endswith("Z"):
                raise SelectionError("completed ASR requires a UTC completion timestamp")
            admit(_candidate(spec, media_sha, metadata.bindings[str(path)], artifact["byte_count"],
                done["transcript"]["segment_count"], done["input"]["duration_ms"],
                preference=(0, completed_at, rank, str(path)),
                text_characters=done["transcript"]["text_character_count"]))

    if set(candidates) != audio:
        raise SelectionError("completed transcript selection is missing " + str(len(audio - candidates.keys())) + " audio recordings")
    chosen = {key: max(rows, key=lambda row: row["preference"]) for key, rows in candidates.items()}
    canaries = [key for key, row in chosen.items() if row["source"]["recording_id"] == canary_job_id]
    if len(canaries) != 1:
        raise SelectionError("the exact completed pilot is not the selected canary")
    order = canaries + sorted(set(chosen) - set(canaries))
    sources = [deepcopy(chosen[key]["source"]) for key in order]
    shards = [sources[:1]] + [sources[start:start + shard_size] for start in range(1, len(sources), shard_size)]
    formats = Counter(row["source"]["format"] for row in chosen.values())
    return {"kind": KIND, "schema_version": 1, "inventory": metadata.bindings[inventory],
            "sources": sources, "shards": shards,
            "counts": {"audio_recordings": len(audio), "selected_sources": len(sources),
                "shards": len(shards), "formats": dict(formats), "completion_receipts": dict(receipt_counts),
                "duplicate_candidates": sum(len(rows) - 1 for rows in candidates.values()),
                "metadata_empty_normalized_sources": sum(row["text_character_count"] == 0 for row in chosen.values()),
                "transcript_bytes": sum(row["transcript_bytes"] for row in chosen.values()),
                "segment_count": sum(row["segment_count"] for row in chosen.values()),
                "duration_ms": sum(row["duration_ms"] for row in chosen.values())},
            "recordings": [{**{key: value for key, value in chosen[media_sha].items() if key != "source"},
                            "recording_id": chosen[media_sha]["source"]["recording_id"],
                            "candidate_count": len(candidates[media_sha])} for media_sha in order],
            "proof_bindings": [metadata.bindings[path] for path in sorted(metadata.bindings)],
            "dedup_policy": {"key": "original_media_sha256", "format": "longform_before_normalized",
                "longform": "later_listed_campaign_then_completion_mtime_ns_then_path",
                "normalized": "UTC_completed_at_then_later_listed_campaign_then_path",
                "longform_roots_oldest_first": [str(root) for root in longform_roots],
                "normalized_roots_oldest_first": [str(root) for root in normalized_roots],
                "canary_recording_id": canary_job_id, "max_sources_per_later_shard": shard_size},
            "semantics": {"private": True, "metadata_only": True, "transcript_content_verified": False,
                "raw_media_read": False, "raw_media_checksums_verified": False,
                "third_party_transcripts_included": False, "empty_sources_retained": True,
                "dates_inferred_from_filenames": False, "cloud_processing_approved": False,
                "paid_requests_started": False, "campaign_manifest_sealed": False,
                "publication_authority": False}}


def write_selection(output, **kwargs):
    output = runner.safe.path_value(output)
    if output.exists():
        raise SelectionError("selection output must be a new private leaf directory")
    value = build_selection(**kwargs)
    with runner.paths.retained_directory(output.parent) as directory:
        os.mkdir(output.name, 0o700, dir_fd=directory)
        os.fsync(directory)
    artifact = runner.put(output / "selection.json", value)
    return {"state": "prepared_offline_selection", "artifact": artifact, "counts": value["counts"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="new absolute private leaf directory")
    args = parser.parse_args(argv)
    try:
        print(json.dumps(write_selection(args.output), indent=2, sort_keys=True))
        return 0
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        detail = str(error).replace("\n", " ")[:500] if isinstance(error, SelectionError) else type(error).__name__
        print("TranscriptArchiveSelectionError: " + detail, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
