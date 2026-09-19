"""Deny-by-default static release for the reviewed public entity/event graph.

The graph is intentionally separate from recording release schemas v1/v2.  Its
only database inputs are migration 0031's narrow ``public_graph_*`` views.  The
static format omits aliases, metadata, basis strings, observations, biometric
state, local paths, and every field that is not needed by the public graph UI.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from .importers import canonical_json
from .private_acquisition import assert_no_restricted_publication_state


GRAPH_SCHEMA_VERSION = 1
MAX_COLLECTION_ITEMS = 10_000
MAX_TOTAL_ITEMS = 50_000

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
_RELEASE_ID_RE = re.compile(r"^graph_release_[a-f0-9]{24}$")
_SHARD_PATH_RE = re.compile(r"^graph/graph-[a-f0-9]{16}\.json$")
_UTC_RE = re.compile(
    r"^(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])"
    r"T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?Z$"
)
_DATE_VALUE_RE = re.compile(r"^\d{4}(?:-\d{2}(?:-\d{2})?)?$")

_COLLECTIONS = (
    "entities",
    "events",
    "appearances",
    "event_participants",
    "event_dates",
    "event_relations",
    "event_evidence",
)
_COUNT_KEYS = set(_COLLECTIONS)
_GRAPH_KEYS = {
    "schema_version",
    "kind",
    "generated_at",
    "counts",
    *_COLLECTIONS,
}
_MANIFEST_KEYS = {
    "schema_version",
    "kind",
    "release_id",
    "generated_at",
    "counts",
    "graph_shard",
}
_REF_KEYS = {"path", "sha256", "bytes"}
_ENTITY_TYPES = {
    "person",
    "community_figure",
    "animal",
    "place",
    "organization",
    "platform",
    "term",
    "object",
    "unknown",
}
_DATE_PRECISIONS = {"day", "month", "year", "range", "circa", "unknown"}
_DATE_CERTAINTIES = {"certain", "probable", "uncertain", "disputed"}
_SUPPORT_KINDS = {"direct", "contextual", "corroborating", "contradicting"}
_GRAPH_OBJECT_TYPES = {
    "entity",
    "event",
    "appearance",
    "event_participant",
    "event_date",
    "event_relation",
    "event_evidence",
}


def _encoded(value: object) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: object, keys: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"Invalid {label} fields")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer >= {minimum}")
    return value


def _text(value: object, label: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{label} must be a bounded non-empty string")
    if "\x00" in value:
        raise ValueError(f"{label} contains a null character")
    return value


def _identifier(value: object, label: str) -> str:
    result = _text(value, label, maximum=256)
    if not _IDENTIFIER_RE.fullmatch(result):
        raise ValueError(f"{label} is not a safe identifier")
    return result


def _slug(value: object, label: str) -> str:
    result = _text(value, label, maximum=160)
    if not _SLUG_RE.fullmatch(result):
        raise ValueError(f"{label} is not a safe slug")
    return result


def _timestamp(value: object, label: str) -> str:
    result = _text(value, label, maximum=64)
    if not _UTC_RE.fullmatch(result):
        raise ValueError(f"{label} must be an explicit UTC timestamp")
    try:
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{label} is not a valid timestamp") from error
    if parsed.tzinfo != timezone.utc:
        raise ValueError(f"{label} must use the UTC Z designator")
    return result


def _canonical_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("Graph decision time is invalid") from error
    if parsed.tzinfo is None:
        raise ValueError("Graph decision time lacks a UTC offset")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _nullable_date(value: object, label: str) -> str | None:
    if value is None:
        return None
    result = _text(value, label, maximum=10)
    if not _DATE_VALUE_RE.fullmatch(result):
        raise ValueError(f"{label} is not a bounded date value")
    try:
        if len(result) == 10:
            date.fromisoformat(result)
        elif len(result) == 7:
            year, month = result.split("-")
            date(int(year), int(month), 1)
        else:
            date(int(result), 1, 1)
    except ValueError as error:
        raise ValueError(f"{label} is not a valid calendar value") from error
    return result


def _anchor(value: object, label: str) -> tuple[dict, dict, dict]:
    anchor = _exact(value, {"recording", "source", "rendition"}, label)
    recording = _exact(
        anchor["recording"], {"recording_id", "slug", "title"}, f"{label}.recording"
    )
    source = _exact(
        anchor["source"],
        {"source_id", "platform", "url", "native_id"},
        f"{label}.source",
    )
    rendition = _exact(
        anchor["rendition"],
        {"rendition_id", "time_basis"},
        f"{label}.rendition",
    )
    _identifier(recording["recording_id"], f"{label}.recording.recording_id")
    _slug(recording["slug"], f"{label}.recording.slug")
    _text(recording["title"], f"{label}.recording.title", maximum=1_000)
    _identifier(source["source_id"], f"{label}.source.source_id")
    _text(source["platform"], f"{label}.source.platform", maximum=200)
    _text(source["native_id"], f"{label}.source.native_id", maximum=1_000)
    url = _text(source["url"], f"{label}.source.url", maximum=4_096)
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{label}.source.url must be uncredentialed HTTP(S)")
    _identifier(rendition["rendition_id"], f"{label}.rendition.rendition_id")
    if rendition["time_basis"] != "rendition_media_ms":
        raise ValueError(f"{label}.rendition.time_basis is not allowed")
    return recording, source, rendition


def _ordered_unique_ids(values: list, key: str, label: str) -> set[str]:
    ids: list[str] = []
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            raise ValueError(f"{label}[{index}] must be an object")
        ids.append(_identifier(value.get(key), f"{label}[{index}].{key}"))
    if len(ids) != len(set(ids)):
        raise ValueError(f"{label} contains duplicate IDs")
    if ids != sorted(ids):
        raise ValueError(f"{label} is not deterministically ordered")
    return set(ids)


def validate_graph_release_shape(value: object) -> dict:
    """Strictly validate one graph shard and all cross-object references."""

    graph = _exact(value, _GRAPH_KEYS, "entity/event graph")
    if graph["schema_version"] != GRAPH_SCHEMA_VERSION:
        raise ValueError("Unsupported entity/event graph schema version")
    if graph["kind"] != "entity_event_graph":
        raise ValueError("Invalid entity/event graph kind")
    _timestamp(graph["generated_at"], "generated_at")
    counts = _exact(graph["counts"], _COUNT_KEYS, "graph counts")
    for key, count in counts.items():
        _integer(count, f"counts.{key}")

    collections: dict[str, list] = {}
    total = 0
    for name in _COLLECTIONS:
        items = graph[name]
        if not isinstance(items, list) or len(items) > MAX_COLLECTION_ITEMS:
            raise ValueError(f"{name} must be a bounded array")
        if counts[name] != len(items):
            raise ValueError(f"counts.{name} does not match {name}")
        collections[name] = items
        total += len(items)
    if total > MAX_TOTAL_ITEMS:
        raise ValueError("Entity/event graph exceeds the total item bound")

    entity_ids = _ordered_unique_ids(collections["entities"], "entity_id", "entities")
    event_ids = _ordered_unique_ids(collections["events"], "event_id", "events")
    appearance_ids = _ordered_unique_ids(
        collections["appearances"], "appearance_id", "appearances"
    )
    participant_ids = _ordered_unique_ids(
        collections["event_participants"],
        "event_participant_id",
        "event_participants",
    )
    date_ids = _ordered_unique_ids(
        collections["event_dates"], "event_date_id", "event_dates"
    )
    relation_ids = _ordered_unique_ids(
        collections["event_relations"], "event_relation_id", "event_relations"
    )
    evidence_ids = _ordered_unique_ids(
        collections["event_evidence"], "event_evidence_id", "event_evidence"
    )
    # Keep the locals live as an explicit audit of every ID-bearing collection.
    del appearance_ids, participant_ids, date_ids, relation_ids, evidence_ids

    entity_slugs: set[str] = set()
    for index, entity in enumerate(collections["entities"]):
        prefix = f"entities[{index}]"
        _exact(entity, {"entity_id", "slug", "label", "entity_type"}, prefix)
        slug = _slug(entity["slug"], f"{prefix}.slug")
        if slug in entity_slugs:
            raise ValueError("Entity graph contains duplicate entity slugs")
        entity_slugs.add(slug)
        _text(entity["label"], f"{prefix}.label", maximum=512)
        if entity["entity_type"] not in _ENTITY_TYPES:
            raise ValueError(f"{prefix}.entity_type is not allowed")

    event_slugs: set[str] = set()
    for index, event in enumerate(collections["events"]):
        prefix = f"events[{index}]"
        _exact(event, {"event_id", "slug", "label"}, prefix)
        slug = _slug(event["slug"], f"{prefix}.slug")
        if slug in event_slugs:
            raise ValueError("Entity graph contains duplicate event slugs")
        event_slugs.add(slug)
        _text(event["label"], f"{prefix}.label", maximum=512)

    recording_snapshots: dict[str, dict] = {}
    source_snapshots: dict[str, dict] = {}

    def validate_anchor_snapshot(anchor_value: object, prefix: str) -> None:
        recording, source, rendition = _anchor(anchor_value, f"{prefix}.anchor")
        recording_id = str(recording["recording_id"])
        source_id = str(source["source_id"])
        prior_recording = recording_snapshots.setdefault(recording_id, recording)
        prior_source = source_snapshots.setdefault(source_id, source)
        if prior_recording != recording:
            raise ValueError(f"{prefix} changes a recording anchor snapshot")
        if prior_source != source:
            raise ValueError(f"{prefix} changes a source anchor snapshot")
        _identifier(rendition["rendition_id"], f"{prefix}.anchor.rendition.rendition_id")

    referenced_entities: set[str] = set()
    participant_events: set[str] = set()
    evidence_events: set[str] = set()
    for index, appearance in enumerate(collections["appearances"]):
        prefix = f"appearances[{index}]"
        _exact(
            appearance,
            {"appearance_id", "entity_id", "label", "start_ms", "end_ms", "anchor"},
            prefix,
        )
        entity_id = _identifier(appearance["entity_id"], f"{prefix}.entity_id")
        if entity_id not in entity_ids:
            raise ValueError(f"{prefix} references a missing public entity")
        referenced_entities.add(entity_id)
        _text(appearance["label"], f"{prefix}.label", maximum=512)
        start = _integer(appearance["start_ms"], f"{prefix}.start_ms")
        end = _integer(appearance["end_ms"], f"{prefix}.end_ms", minimum=1)
        if end <= start:
            raise ValueError(f"{prefix} has an invalid half-open interval")
        validate_anchor_snapshot(appearance["anchor"], prefix)

    for index, participant in enumerate(collections["event_participants"]):
        prefix = f"event_participants[{index}]"
        _exact(
            participant,
            {"event_participant_id", "event_id", "entity_id", "label"},
            prefix,
        )
        event_id = _identifier(participant["event_id"], f"{prefix}.event_id")
        entity_id = _identifier(participant["entity_id"], f"{prefix}.entity_id")
        if event_id not in event_ids or entity_id not in entity_ids:
            raise ValueError(f"{prefix} references a missing public root")
        participant_events.add(event_id)
        referenced_entities.add(entity_id)
        _text(participant["label"], f"{prefix}.label", maximum=512)

    for index, item in enumerate(collections["event_dates"]):
        prefix = f"event_dates[{index}]"
        _exact(
            item,
            {
                "event_date_id",
                "event_id",
                "label",
                "value_start",
                "value_end",
                "precision",
                "certainty",
            },
            prefix,
        )
        if _identifier(item["event_id"], f"{prefix}.event_id") not in event_ids:
            raise ValueError(f"{prefix} references a missing public event")
        _text(item["label"], f"{prefix}.label", maximum=512)
        start = _nullable_date(item["value_start"], f"{prefix}.value_start")
        end = _nullable_date(item["value_end"], f"{prefix}.value_end")
        if item["precision"] not in _DATE_PRECISIONS:
            raise ValueError(f"{prefix}.precision is not allowed")
        if item["certainty"] not in _DATE_CERTAINTIES:
            raise ValueError(f"{prefix}.certainty is not allowed")
        if (item["precision"] == "unknown") != (start is None and end is None):
            raise ValueError(f"{prefix} has inconsistent unknown date precision")
        if end is not None and (start is None or len(start) != len(end) or end < start):
            raise ValueError(f"{prefix} has an invalid date range")

    for index, relation in enumerate(collections["event_relations"]):
        prefix = f"event_relations[{index}]"
        _exact(
            relation,
            {"event_relation_id", "from_event_id", "to_event_id", "label"},
            prefix,
        )
        source_event = _identifier(
            relation["from_event_id"], f"{prefix}.from_event_id"
        )
        target_event = _identifier(relation["to_event_id"], f"{prefix}.to_event_id")
        if source_event == target_event or source_event not in event_ids or target_event not in event_ids:
            raise ValueError(f"{prefix} has invalid public event endpoints")
        _text(relation["label"], f"{prefix}.label", maximum=512)

    relation_graph: dict[str, set[str]] = {event_id: set() for event_id in event_ids}
    for relation in collections["event_relations"]:
        relation_graph[str(relation["from_event_id"])].add(str(relation["to_event_id"]))
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(event_id: str) -> None:
        if event_id in visiting:
            raise ValueError("Public event relations contain a directed cycle")
        if event_id in visited:
            return
        visiting.add(event_id)
        for target_id in relation_graph[event_id]:
            visit(target_id)
        visiting.remove(event_id)
        visited.add(event_id)

    for event_id in sorted(event_ids):
        visit(event_id)

    for index, evidence in enumerate(collections["event_evidence"]):
        prefix = f"event_evidence[{index}]"
        _exact(
            evidence,
            {
                "event_evidence_id",
                "event_id",
                "label",
                "support_kind",
                "start_ms",
                "end_ms",
                "anchor",
            },
            prefix,
        )
        event_id = _identifier(evidence["event_id"], f"{prefix}.event_id")
        if event_id not in event_ids:
            raise ValueError(f"{prefix} references a missing public event")
        evidence_events.add(event_id)
        _text(evidence["label"], f"{prefix}.label", maximum=512)
        if evidence["support_kind"] not in _SUPPORT_KINDS:
            raise ValueError(f"{prefix}.support_kind is not allowed")
        start_raw = evidence["start_ms"]
        end_raw = evidence["end_ms"]
        if (start_raw is None) != (end_raw is None):
            raise ValueError(f"{prefix} must set both interval endpoints or neither")
        if start_raw is not None:
            start = _integer(start_raw, f"{prefix}.start_ms")
            end = _integer(end_raw, f"{prefix}.end_ms", minimum=1)
            if end <= start:
                raise ValueError(f"{prefix} has an invalid half-open interval")
        validate_anchor_snapshot(evidence["anchor"], prefix)

    if event_ids != participant_events or event_ids != evidence_events:
        raise ValueError("Every public event requires a public participant and evidence edge")
    if entity_ids != referenced_entities:
        raise ValueError("Every public entity must be referenced by a public graph edge")
    return graph


def _anchor_payload(row: sqlite3.Row) -> dict:
    return {
        "recording": {
            "recording_id": row["recording_id"],
            "slug": row["recording_slug"],
            "title": row["recording_title"],
        },
        "source": {
            "source_id": row["source_id"],
            "platform": row["source_platform"],
            "url": row["source_url"],
            "native_id": row["source_native_id"],
        },
        "rendition": {
            "rendition_id": row["rendition_id"],
            "time_basis": "rendition_media_ms",
        },
    }


def _release_generated_at(
    connection: sqlite3.Connection,
    graph_object_ids: set[tuple[str, str]],
    anchor_object_ids: set[tuple[str, str]],
) -> str:
    visible = graph_object_ids | anchor_object_ids
    decision_times: list[str] = []
    for row in connection.execute(
        """
        SELECT object_type, object_id, decided_at
        FROM current_publication_decisions
        UNION ALL
        SELECT object_type, object_id, decided_at
        FROM current_publication_gate_decisions
        """
    ):
        if (row["object_type"], row["object_id"]) in visible:
            decision_times.append(row["decided_at"])
    if not decision_times:
        return "1970-01-01T00:00:00Z"
    latest = max(
        decision_times,
        key=lambda item: datetime.fromisoformat(item.replace("Z", "+00:00")).timestamp(),
    )
    return _canonical_timestamp(latest)


def build_graph_release(connection: sqlite3.Connection) -> dict:
    """Build one deterministic graph shard from migration 0031's public views."""

    assert_no_restricted_publication_state(connection)

    raw_entities = {
        row["entity_id"]: {
            "entity_id": row["entity_id"],
            "slug": row["slug"],
            "label": row["public_label"],
            "entity_type": row["entity_type"],
        }
        for row in connection.execute(
            "SELECT * FROM public_graph_entities ORDER BY entity_id"
        )
    }
    raw_events = {
        row["event_id"]: {
            "event_id": row["event_id"],
            "slug": row["slug"],
            "label": row["public_label"],
        }
        for row in connection.execute("SELECT * FROM public_graph_events ORDER BY event_id")
    }
    appearances = [
        {
            "appearance_id": row["appearance_id"],
            "entity_id": row["entity_id"],
            "label": row["public_label"],
            "start_ms": row["start_ms"],
            "end_ms": row["end_ms"],
            "anchor": _anchor_payload(row),
        }
        for row in connection.execute(
            "SELECT * FROM public_graph_appearances ORDER BY appearance_id"
        )
    ]
    participants = [
        {
            "event_participant_id": row["event_participant_id"],
            "event_id": row["event_id"],
            "entity_id": row["entity_id"],
            "label": row["public_label"],
        }
        for row in connection.execute(
            "SELECT * FROM public_graph_event_participants ORDER BY event_participant_id"
        )
    ]
    evidence = [
        {
            "event_evidence_id": row["event_evidence_id"],
            "event_id": row["event_id"],
            "label": row["public_label"],
            "support_kind": row["support_kind"],
            "start_ms": row["start_ms"],
            "end_ms": row["end_ms"],
            "anchor": _anchor_payload(row),
        }
        for row in connection.execute(
            "SELECT * FROM public_graph_event_evidence ORDER BY event_evidence_id"
        )
    ]

    # A public event is source-bearing, not a free-floating label.  Suppress roots
    # until at least one independently gated participant and evidence edge survive.
    event_ids = (
        {item["event_id"] for item in participants}
        & {item["event_id"] for item in evidence}
        & set(raw_events)
    )
    participants = [item for item in participants if item["event_id"] in event_ids]
    evidence = [item for item in evidence if item["event_id"] in event_ids]
    events = [raw_events[event_id] for event_id in sorted(event_ids)]

    entity_ids = {
        item["entity_id"] for item in participants
    } | {item["entity_id"] for item in appearances}
    entity_ids &= set(raw_entities)
    appearances = [item for item in appearances if item["entity_id"] in entity_ids]
    participants = [item for item in participants if item["entity_id"] in entity_ids]
    # Participant pruning can only occur for malformed/mutating catalog state; if it
    # removes an event's last participant, drop that event and its dependent edges.
    event_ids &= {item["event_id"] for item in participants}
    events = [raw_events[event_id] for event_id in sorted(event_ids)]
    participants = [item for item in participants if item["event_id"] in event_ids]
    evidence = [item for item in evidence if item["event_id"] in event_ids]
    entity_ids = {
        item["entity_id"] for item in participants
    } | {item["entity_id"] for item in appearances}
    entities = [raw_entities[entity_id] for entity_id in sorted(entity_ids)]

    dates = [
        {
            "event_date_id": row["event_date_id"],
            "event_id": row["event_id"],
            "label": row["public_label"],
            "value_start": row["value_start"],
            "value_end": row["value_end"],
            "precision": row["precision"],
            "certainty": row["certainty"],
        }
        for row in connection.execute(
            "SELECT * FROM public_graph_event_dates ORDER BY event_date_id"
        )
        if row["event_id"] in event_ids
    ]
    relations = [
        {
            "event_relation_id": row["event_relation_id"],
            "from_event_id": row["from_event_id"],
            "to_event_id": row["to_event_id"],
            "label": row["public_label"],
        }
        for row in connection.execute(
            "SELECT * FROM public_graph_event_relations ORDER BY event_relation_id"
        )
        if row["from_event_id"] in event_ids and row["to_event_id"] in event_ids
    ]

    graph_object_ids: set[tuple[str, str]] = {
        *(('entity', item['entity_id']) for item in entities),
        *(('event', item['event_id']) for item in events),
        *(('appearance', item['appearance_id']) for item in appearances),
        *(('event_participant', item['event_participant_id']) for item in participants),
        *(('event_date', item['event_date_id']) for item in dates),
        *(('event_relation', item['event_relation_id']) for item in relations),
        *(('event_evidence', item['event_evidence_id']) for item in evidence),
    }
    anchor_object_ids: set[tuple[str, str]] = set()
    for item in [*appearances, *evidence]:
        anchor_object_ids.add(("recording", item["anchor"]["recording"]["recording_id"]))
        anchor_object_ids.add(("source", item["anchor"]["source"]["source_id"]))
    generated_at = _release_generated_at(
        connection, graph_object_ids, anchor_object_ids
    )
    collections = {
        "entities": entities,
        "events": events,
        "appearances": appearances,
        "event_participants": participants,
        "event_dates": dates,
        "event_relations": relations,
        "event_evidence": evidence,
    }
    graph = {
        "schema_version": GRAPH_SCHEMA_VERSION,
        "kind": "entity_event_graph",
        "generated_at": generated_at,
        "counts": {name: len(collections[name]) for name in _COLLECTIONS},
        **collections,
    }
    validate_graph_release_shape(graph)
    return graph


def _manifest_release_id(manifest: dict) -> str:
    payload = {key: manifest[key] for key in sorted(_MANIFEST_KEYS - {"release_id"})}
    return f"graph_release_{_sha256(canonical_json(payload).encode('utf-8'))[:24]}"


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _sync(path: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _same_tree(left: Path, right: Path) -> bool:
    left_files = sorted(item.relative_to(left) for item in left.rglob("*") if item.is_file())
    right_files = sorted(item.relative_to(right) for item in right.rglob("*") if item.is_file())
    return left_files == right_files and all(
        (left / item).read_bytes() == (right / item).read_bytes() for item in left_files
    )


def export_graph_release_from_graph(graph: dict, output_directory: str | Path) -> dict:
    """Install and atomically activate one validated content-addressed graph."""

    validate_graph_release_shape(graph)
    output_root = Path(output_directory).resolve()
    if output_root == Path(output_root.anchor):
        raise ValueError("Refusing to export a graph release at a filesystem root")
    output_root.mkdir(parents=True, exist_ok=True)
    releases_root = output_root / "releases"
    releases_root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".graph-release-", dir=output_root))
    candidate_manifest: Path | None = None
    try:
        graph_bytes = _encoded(graph)
        graph_digest = _sha256(graph_bytes)
        graph_path = f"graph/graph-{graph_digest[:16]}.json"
        _write(work / graph_path, graph_bytes)
        manifest = {
            "schema_version": GRAPH_SCHEMA_VERSION,
            "kind": "entity_event_graph_manifest",
            "release_id": "",
            "generated_at": graph["generated_at"],
            "counts": graph["counts"],
            "graph_shard": {
                "path": graph_path,
                "sha256": graph_digest,
                "bytes": len(graph_bytes),
            },
        }
        manifest["release_id"] = _manifest_release_id(manifest)
        target = releases_root / manifest["release_id"]
        _sync(work / "graph")
        _sync(work)
        if target.exists():
            if target.is_symlink() or not target.is_dir() or not _same_tree(work, target):
                raise RuntimeError(f"Immutable graph release already differs: {target}")
            shutil.rmtree(work)
        else:
            os.replace(work, target)
            _sync(releases_root)

        candidate_manifest = output_root / f".manifest-{os.getpid()}.json"
        manifest_bytes = _encoded(manifest)
        _write(candidate_manifest, manifest_bytes)
        validate_graph_release(candidate_manifest, release_root=target)
        os.replace(candidate_manifest, output_root / "manifest.json")
        _sync(output_root)
        return {
            "path": str(output_root / "manifest.json"),
            "release_directory": str(target),
            "release_id": manifest["release_id"],
            "sha256": _sha256(manifest_bytes),
            "bytes": len(manifest_bytes),
            **graph["counts"],
        }
    finally:
        if work.exists():
            shutil.rmtree(work)
        if candidate_manifest is not None and candidate_manifest.exists():
            candidate_manifest.unlink()


def export_graph_release(connection: sqlite3.Connection, output_directory: str | Path) -> dict:
    return export_graph_release_from_graph(build_graph_release(connection), output_directory)


def _pairs_without_duplicates(pairs: list[tuple[str, object]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key {key!r}")
        result[key] = value
    return result


def _load(payload: bytes, path: Path) -> object:
    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs_without_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid UTF-8 graph JSON: {path}") from error


def _validated_reference(value: object) -> dict:
    reference = _exact(value, _REF_KEYS, "graph shard reference")
    if (
        not isinstance(reference["path"], str)
        or not _SHARD_PATH_RE.fullmatch(reference["path"])
        or PurePosixPath(reference["path"]).is_absolute()
        or any(part in {"", ".", ".."} for part in PurePosixPath(reference["path"]).parts)
    ):
        raise ValueError("Unsafe graph shard path")
    if not isinstance(reference["sha256"], str) or not _SHA256_RE.fullmatch(
        reference["sha256"]
    ):
        raise ValueError("Invalid graph shard SHA-256")
    _integer(reference["bytes"], "graph_shard.bytes", minimum=1)
    return reference


def _read_verified(root: Path, reference: dict) -> tuple[bytes, Path]:
    root = root.resolve()
    candidate = root.joinpath(*PurePosixPath(reference["path"]).parts)
    parent = candidate.parent.resolve()
    if parent != root and root not in parent.parents:
        raise ValueError("Graph shard escapes its release directory")
    before = candidate.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("Graph shard is not a regular non-symlink file")
    payload = candidate.read_bytes()
    after = candidate.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError("Graph shard changed while being validated")
    if len(payload) != reference["bytes"] or _sha256(payload) != reference["sha256"]:
        raise ValueError("Graph shard failed its byte count or SHA-256 check")
    return payload, candidate


def validate_graph_release(
    manifest_path: str | Path, *, release_root: str | Path | None = None
) -> dict:
    """Validate the complete isolated graph manifest and immutable shard tree."""

    path = Path(manifest_path)
    manifest = _exact(_load(path.read_bytes(), path), _MANIFEST_KEYS, "graph manifest")
    if manifest["schema_version"] != GRAPH_SCHEMA_VERSION:
        raise ValueError("Unsupported graph manifest schema version")
    if manifest["kind"] != "entity_event_graph_manifest":
        raise ValueError("Invalid graph manifest kind")
    release_id = manifest["release_id"]
    if not isinstance(release_id, str) or not _RELEASE_ID_RE.fullmatch(release_id):
        raise ValueError("Invalid graph release_id")
    expected_id = _manifest_release_id(manifest)
    if release_id != expected_id:
        raise ValueError(
            f"Graph release identity mismatch: expected {expected_id}, received {release_id}"
        )
    generated_at = _timestamp(manifest["generated_at"], "manifest.generated_at")
    counts = _exact(manifest["counts"], _COUNT_KEYS, "manifest graph counts")
    for key, count in counts.items():
        _integer(count, f"manifest.counts.{key}")
    reference = _validated_reference(manifest["graph_shard"])
    root = (
        Path(release_root).resolve()
        if release_root is not None
        else (path.parent / "releases" / release_id).resolve()
    )
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"Graph release directory is missing or unsafe: {root}")
    payload, shard_path = _read_verified(root, reference)
    graph = validate_graph_release_shape(_load(payload, shard_path))
    if graph["generated_at"] != generated_at or graph["counts"] != counts:
        raise ValueError("Graph manifest metadata differs from its shard")

    expected_files = {reference["path"]}
    actual_files: set[str] = set()
    for candidate in root.rglob("*"):
        relative = candidate.relative_to(root).as_posix()
        if candidate.is_symlink():
            raise ValueError(f"Symlink exists in graph release tree: {relative}")
        if candidate.is_dir():
            if relative != "graph":
                raise ValueError(f"Unexpected graph release directory: {relative}")
        elif candidate.is_file():
            actual_files.add(relative)
        else:
            raise ValueError(f"Unexpected graph release object: {relative}")
    if actual_files != expected_files:
        raise ValueError("Graph release tree contains missing or unreferenced files")
    return {
        "path": str(path),
        "release_directory": str(root),
        "release_id": release_id,
        "schema_version": GRAPH_SCHEMA_VERSION,
        **counts,
    }
