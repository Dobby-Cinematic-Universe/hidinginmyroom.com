"""Read-only exact campaign inventory for the separate private speaker screen.

Only explicit controller checkpoints/admission documents and their small result
JSONs are read. Media bytes are never opened or hashed here. Audio availability
and duration are acquisition metadata hints, not fresh decoding validation.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import unicodedata

from pipeline import speaker_screen as screen
from pipeline import speaker_screen_guidance as guidance

ScreenError = screen.ScreenError
MAX_ADMISSIONS = 10000
MAX_CHECKPOINT_BYTES = 128 * 1024**2
MAX_METADATA_BYTES = 16 * 1024**2
MAX_TOTAL_BYTES = 512 * 1024**2


class _Reader:
    def __init__(self):
        self.bindings = {}
        self.total_bytes = 0

    def read(self, reference, *, maximum=MAX_METADATA_BYTES, expected_required=True):
        if expected_required:
            screen.file_binding(reference)
        else:
            screen.exact(reference, {"path"}, "unbound result path")
        path = screen.path_value(reference["path"])
        with screen.opened(path) as descriptor:
            before = screen.witness(descriptor)
            size = before["st_size"]
            if not 0 < size <= maximum or self.total_bytes + size > MAX_TOTAL_BYTES:
                raise ScreenError("archive inventory metadata exceeds its bounded byte budget")
            chunks, count = [], 0
            while count < size:
                block = os.read(descriptor, min(1024**2, size - count))
                if not block:
                    raise ScreenError("inventory metadata was truncated")
                chunks.append(block)
                count += len(block)
            if screen.witness(descriptor) != before:
                raise ScreenError("inventory metadata changed while being read")
        body = b"".join(chunks)
        binding = {"path": str(path), "sha256": hashlib.sha256(body).hexdigest()}
        if expected_required and binding != reference:
            raise ScreenError("archive inventory metadata SHA-256 mismatch")
        if str(path) in self.bindings and self.bindings[str(path)] != binding:
            raise ScreenError("archive inventory path has conflicting hashes")
        self.bindings[str(path)] = binding
        self.total_bytes += len(body)
        def pairs(items):
            value = {}
            for key, item in items:
                if key in value:
                    raise ScreenError("duplicate inventory JSON key")
                value[key] = item
            return value
        try:
            value = json.loads(body, object_pairs_hook=pairs,
                parse_constant=lambda _: (_ for _ in ()).throw(ScreenError("nonfinite inventory JSON")))
            screen.canonical(value)
        except (ValueError, UnicodeError, RecursionError) as error:
            raise ScreenError("invalid inventory JSON") from error
        if not isinstance(value, dict):
            raise ScreenError("inventory JSON must be an object")
        return value, binding


def _sha(value):
    if not isinstance(value, str) or not screen.SHA.fullmatch(value):
        raise ScreenError("invalid inventory SHA-256")
    return value


def _title_priority(title):
    normalized = unicodedata.normalize("NFKC", title).casefold()
    reasons, score = [], 0
    for name, pattern, points in guidance.TITLE_RULES:
        if re.search(pattern, normalized):
            reasons.append("title:" + name)
            score += points
    return {"score": min(score, 60), "reasons": reasons}


def _filename_date(native_id):
    name = native_id.rsplit("/", 1)[-1]
    compact = re.match(r"^(\d{4})(\d{2})(\d{2})-", name)
    labeled = re.match(r"^\[(\d{4})-(\d{2})-(\d{2})\] ", name)
    match = compact or labeled
    if match:
        try:
            value = date.fromisoformat("-".join(match.groups())).isoformat()
            return {"value": value, "basis": "filename_date"}
        except ValueError:
            pass
    return {"value": None, "basis": "unknown"}


def _result(result, binding, *, expected=None):
    if (type(result.get("schema_version")) is not int or result["schema_version"] != 1
            or result.get("status") != "completed" or result.get("dry_run") is not False
            or result.get("errors") != [] or result.get("result_path") != binding["path"]):
        raise ScreenError("inventory requires an exact completed acquisition result")
    if not isinstance(result.get("job_id"), str) or not screen.IDENTIFIER.fullmatch(result["job_id"]):
        raise ScreenError("invalid acquisition job identity")
    _sha(result.get("work_order_sha256"))
    admission, catalog, source = result.get("admission"), result.get("catalog_records"), result.get("source")
    if not isinstance(admission, dict) or not isinstance(catalog, dict) or not isinstance(source, dict):
        raise ScreenError("acquisition result lacks admission/catalog/source metadata")
    media = catalog.get("media_objects")
    if not isinstance(media, list) or len(media) != 1 or not isinstance(media[0], dict):
        raise ScreenError("acquisition result must name one exact media object")
    media = media[0]
    sha = _sha(admission.get("sha256"))
    identifier = "media_sha256_" + sha
    size = admission.get("byte_count")
    screen.integer(size, 1, 64 * 1024**3, "media byte count")
    path = str(screen.path_value(admission.get("path")))
    if (admission.get("media_id") != identifier or media.get("media_id") != identifier
            or media.get("sha256") != sha or type(media.get("byte_count")) is not int or media["byte_count"] != size):
        raise ScreenError("acquisition admission/catalog media identity differs")
    native_id = guidance._text(source.get("native_id"), "source native id", 4096, empty=False)
    title = guidance._text(source.get("title") or "", "source title", 1000)
    probe = admission.get("normalized_probe")
    if not isinstance(probe, dict) or not isinstance(probe.get("format"), dict) or not isinstance(probe.get("streams"), list):
        raise ScreenError("acquisition result lacks bounded normalized probe metadata")
    streams = probe["streams"]
    if len(streams) > 128:
        raise ScreenError("acquisition probe stream count exceeds bound")
    seen = set()
    for stream in streams:
        if not isinstance(stream, dict) or not isinstance(stream.get("codec_type"), str):
            raise ScreenError("invalid normalized probe stream")
        screen.integer(stream.get("index"), 0, 1024, "probe stream index")
        if stream["index"] in seen:
            raise ScreenError("duplicate normalized probe stream index")
        seen.add(stream["index"])
    duration = probe["format"].get("duration_ms")
    if duration is not None:
        screen.integer(duration, 1, 86400000, "media duration hint")
    if media.get("duration_ms") != duration:
        raise ScreenError("media catalog and admission duration hints differ")
    audio = [row for row in streams if row["codec_type"] == "audio"]
    if expected is not None:
        for key, actual in (("job_id", result.get("job_id")), ("work_order_semantic_sha256", result.get("work_order_sha256")),
                            ("native_id", native_id), ("expected_byte_count", size)):
            if expected.get(key) != actual or (key == "expected_byte_count" and type(expected[key]) is not int):
                raise ScreenError("incremental admission and completed result differ")
        if expected.get("canonical_url") != source.get("canonical_url"):
            raise ScreenError("incremental admission source URL differs")
    return {"recording": {"media_id": identifier, "path": path, "sha256": sha,
                          "byte_count": size, "duration_hint_ms": duration},
            "audio_stream_count": len(audio), "cached_audio_state": "audio_present" if audio else "no_audio_stream",
            "container_hint": probe["format"].get("format_name"),
            "audio_codec_hints": [row.get("codec_name") for row in audio],
            "first_audio_duration_hint_ms": audio[0].get("duration_ms") if audio else None,
            "alias": {"result": binding, "title": title, "source_native_id": native_id,
                      "date": _filename_date(native_id), "priority": _title_priority(title),
                      "job_id": result.get("job_id")}}


def _checkpoint_orders(value):
    if (value.get("kind") != "himr_autonomous_controller_checkpoint"
            or type(value.get("schema_version")) is not int or value["schema_version"] != 1):
        raise ScreenError("unsupported controller inventory checkpoint")
    backend = value.get("backend")
    replay = backend.get("queue_replay") if isinstance(backend, dict) else None
    if (not isinstance(replay, dict) or replay.get("kind") != "himr_queue_operational_replay_restart_checkpoint"
            or type(replay.get("schema_version")) is not int or replay["schema_version"] != 2):
        raise ScreenError("unsupported acquisition replay checkpoint")
    schedules = replay.get("schedules")
    if not isinstance(schedules, list) or not 1 <= len(schedules) <= MAX_ADMISSIONS:
        raise ScreenError("checkpoint schedule count exceeds bound")
    orders = []
    for schedule in schedules:
        if not isinstance(schedule, dict) or not isinstance(schedule.get("orders"), list):
            raise ScreenError("malformed checkpoint schedule")
        if type(schedule.get("work_order_count")) is not int or schedule["work_order_count"] != len(schedule["orders"]):
            raise ScreenError("checkpoint schedule work order count differs")
        orders.extend(schedule["orders"])
        if len(orders) > MAX_ADMISSIONS:
            raise ScreenError("checkpoint admission count exceeds bound")
    totals = replay.get("totals")
    if (not isinstance(totals, dict) or type(totals.get("completed_count")) is not int
            or totals["completed_count"] != len(orders) or type(totals.get("pending_count")) is not int
            or totals["pending_count"] != 0 or type(totals.get("work_order_count")) is not int
            or totals["work_order_count"] != len(orders) or type(totals.get("schedule_count")) is not int
            or totals["schedule_count"] != len(schedules)):
        raise ScreenError("checkpoint is not an exact completed acquisition inventory")
    return orders


def inventory(checkpoints, admissions, *, verify_results=True):
    """Return content-deduplicated candidates with every exact source alias.

    verify_results=False is metadata preparation only: checkpoint result hashes
    are verified against their exact cached acquisition serialization, but their
    on-disk JSON is not reread. New incremental admissions always read results.
    Neither mode verifies or opens media; a separate source probe must do that.
    """
    if type(verify_results) is not bool:
        raise ScreenError("verify_results must be boolean")
    if (not isinstance(checkpoints, list) or not isinstance(admissions, list)
            or len(checkpoints) > 16 or len(admissions) > 16 or not checkpoints + admissions):
        raise ScreenError("inventory needs bounded explicit checkpoint/admission bindings")
    reader = _Reader()
    extracted, seen_sources, seen_results = [], set(), set()
    def add(result, binding, *, expected=None):
        if binding["path"] in seen_results:
            raise ScreenError("inventory repeats an acquisition result")
        if len(extracted) >= MAX_ADMISSIONS:
            raise ScreenError("archive inventory exceeds 10000 admitted sources")
        seen_results.add(binding["path"])
        item = _result(result, binding, expected=expected)
        source_key = item["alias"]["source_native_id"]
        if source_key in seen_sources:
            raise ScreenError("inventory repeats a source native identity")
        seen_sources.add(source_key)
        extracted.append(item)
    for reference in checkpoints:
        checkpoint, _ = reader.read(reference, maximum=MAX_CHECKPOINT_BYTES)
        for order in _checkpoint_orders(checkpoint):
            if not isinstance(order, dict) or not isinstance(order.get("state"), dict):
                raise ScreenError("checkpoint has an incomplete acquisition")
            state = order["state"]
            result = state.get("result")
            if not isinstance(result, dict):
                raise ScreenError("checkpoint result is missing")
            binding = {"path": str(screen.path_value(order.get("result_path"))), "sha256": _sha(state.get("result_sha256"))}
            # Acquisition's result SHA binds the physical pretty-JSON bytes,
            # not its compact semantic work-order identity serialization.
            cached = (json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
            if len(cached) > MAX_METADATA_BYTES or hashlib.sha256(cached).hexdigest() != binding["sha256"]:
                raise ScreenError("cached acquisition result differs from its physical SHA-256")
            if not isinstance(result.get("admission"), dict):
                raise ScreenError("checkpoint acquisition admission is malformed")
            if (result.get("job_id") != order.get("job_id")
                    or result.get("work_order_sha256") != order.get("work_order_identity_sha256")
                    or state.get("media_sha256") != result.get("admission", {}).get("sha256")
                    or type(state.get("byte_count")) is not int
                    or state["byte_count"] != result.get("admission", {}).get("byte_count")):
                raise ScreenError("checkpoint acquisition identity differs")
            if verify_results:
                result, _ = reader.read(binding)
            else:
                reader.bindings[binding["path"]] = binding
            add(result, binding)
    for reference in admissions:
        value, _ = reader.read(reference)
        if (value.get("kind") != "himr_exact_archive_incremental_admission"
                or type(value.get("schema_version")) is not int or value["schema_version"] != 1
                or not isinstance(value.get("records"), list) or len(value["records"]) > MAX_ADMISSIONS):
            raise ScreenError("unsupported bounded incremental admission")
        if (not isinstance(value.get("totals"), dict) or type(value["totals"].get("recordings")) is not int
                or value["totals"]["recordings"] != len(value["records"])):
            raise ScreenError("incremental admission count differs")
        for row in value["records"]:
            if not isinstance(row, dict):
                raise ScreenError("malformed incremental source admission")
            result, binding = reader.read({"path": str(screen.path_value(row.get("result_path")))}, expected_required=False)
            add(result, binding, expected=row)
    if not extracted:
        raise ScreenError("archive inventory contains no admitted sources")
    groups = {}
    for item in extracted:
        sha = item["recording"]["sha256"]
        if sha not in groups:
            groups[sha] = []
        for old in groups[sha][:1]:
            if any(old["recording"][key] != item["recording"][key] for key in ("media_id", "path", "byte_count")):
                raise ScreenError("identical content has conflicting media identity/path/bytes")
            if old["audio_stream_count"] != item["audio_stream_count"]:
                raise ScreenError("identical content has conflicting acquisition audio metadata")
        groups[sha].append(item)
    records = []
    for rows in groups.values():
        rows.sort(key=lambda row: (-row["alias"]["priority"]["score"], row["alias"]["source_native_id"], row["alias"]["result"]["path"]))
        selected = rows[0]
        records.append({key: selected[key] for key in ("recording", "audio_stream_count", "cached_audio_state",
                       "container_hint", "audio_codec_hints", "first_audio_duration_hint_ms")} | {
            "acquisition_result": selected["alias"]["result"], "aliases": [row["alias"] for row in rows],
            "priority_score": selected["alias"]["priority"]["score"],
            "duration_hint_disagreement": len({row["recording"]["duration_hint_ms"] for row in rows}) > 1})
    records.sort(key=lambda row: (-row["priority_score"], row["recording"]["media_id"]))
    no_audio = [row for row in records if row["cached_audio_state"] == "no_audio_stream"]
    counts = {"admitted_sources": len(extracted), "unique_media": len(records),
              "duplicate_source_admissions": len(extracted) - len(records),
              "unique_audio_present": len(records) - len(no_audio), "unique_no_audio": len(no_audio),
              "source_aliases_no_audio": sum(len(row["aliases"]) for row in no_audio),
              "unique_media_bytes": sum(row["recording"]["byte_count"] for row in records),
              "duration_hints_missing": sum(row["recording"]["duration_hint_ms"] is None for row in records)}
    return {"kind": "himr_private_speaker_screen_archive_inventory", "schema_version": 1,
            "records": records, "counts": counts, "source_bindings": [reader.bindings[key] for key in sorted(reader.bindings)],
            "metadata_bytes_read": reader.total_bytes,
            "semantics": {"media_bytes_opened": False, "media_hashes_reverified": False,
                "result_metadata_currently_verified": verify_results or not checkpoints,
                "acquisition_audio_metadata_is_fresh_decode_proof": False,
                "priority_is_speaker_evidence": False, "all_source_aliases_preserved": True,
                "publication_authority": False, "identity_authority": False}}
