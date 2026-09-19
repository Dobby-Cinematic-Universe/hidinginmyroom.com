#!/usr/bin/env python3
"""Build a private, non-authoritative wiki-citation/catalog crosswalk."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import unicodedata
from urllib.parse import parse_qs, unquote, urlsplit


PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRIVATE_INVENTORY_ROOT = PROJECT_ROOT / "research" / "corpus" / "graph-seed-inventories"
PRIVATE_OUTPUT_ROOT = (
    PROJECT_ROOT / "research" / "corpus" / "wiki-catalog-anchor-inventories"
)
PRIVATE_CATALOG_ROOT = PROJECT_ROOT / "research" / "corpus"
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
REDDIT_POST_RE = re.compile(r"^/r/[^/]+/comments/([A-Za-z0-9]+)/", re.I)


class AnchorInventoryError(ValueError):
    """The requested inventory cannot be produced safely or deterministically."""


def _fail(message: str) -> None:
    raise AnchorInventoryError(message)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def wiki_seed_inventory_id(identity_body: dict[str, object]) -> str:
    return "wgsi_" + _sha256_bytes(_canonical_json(identity_body).encode("utf-8"))[:32]


def _load_json_no_duplicates(value: bytes, label: str) -> object:
    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                _fail(f"{label} repeats JSON key {key!r}")
            result[key] = item
        return result

    try:
        return json.loads(value, object_pairs_hook=object_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail(f"{label} is not valid UTF-8 JSON: {exc}")


def _stable_regular_read(path: Path) -> bytes:
    try:
        before = path.lstat()
    except FileNotFoundError:
        _fail(f"Input does not exist: {path}")
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        _fail(f"Input is not a regular non-symlink file: {path}")
    data = path.read_bytes()
    after = path.lstat()
    identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(getattr(before, key) != getattr(after, key) for key in identity):
        _fail(f"Input changed while being read: {path}")
    return data


def _stable_file_sha256(path: Path) -> tuple[str, int]:
    try:
        before = path.lstat()
    except FileNotFoundError:
        _fail(f"Catalog does not exist: {path}")
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        _fail(f"Catalog is not a regular non-symlink file: {path}")
    digest = hashlib.sha256()
    byte_count = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            byte_count += len(chunk)
    after = path.lstat()
    identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(getattr(before, key) != getattr(after, key) for key in identity):
        _fail(f"Catalog changed while being hashed: {path}")
    if byte_count != before.st_size:
        _fail("Catalog byte count changed while being hashed")
    return digest.hexdigest(), byte_count


def _under(path: Path, root: Path, label: str) -> Path:
    resolved = path.resolve(strict=False)
    resolved_root = root.resolve(strict=False)
    try:
        resolved.relative_to(resolved_root)
    except ValueError:
        _fail(f"{label} must remain beneath {resolved_root}")
    return resolved


def _reject_symlink_components(path: Path, label: str) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            _fail(f"{label} contains a symlink component: {current}")


def extract_locator(href: object) -> dict[str, object] | None:
    """Extract an exact provider identifier without dereferencing the URL."""

    if href is None:
        return None
    if not isinstance(href, str) or not href.strip():
        _fail("Citation href must be a nonempty string or null")
    parsed = urlsplit(href)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return {"kind": "unsupported_href", "value": href, "lookups": []}
    host = parsed.hostname.lower().rstrip(".")
    path = parsed.path or "/"

    video_id: str | None = None
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        if path == "/watch":
            values = parse_qs(parsed.query, keep_blank_values=True).get("v", [])
            if len(values) == 1:
                video_id = values[0]
        else:
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) >= 2 and parts[0].lower() in {"shorts", "live", "embed"}:
                video_id = parts[1]
    elif host in {"youtu.be", "www.youtu.be"}:
        parts = [unquote(part) for part in path.split("/") if part]
        if parts:
            video_id = parts[0]
    if video_id is not None and VIDEO_ID_RE.fullmatch(video_id):
        return {
            "kind": "youtube_video_id",
            "value": video_id,
            "lookups": [
                {"namespace": "youtube_video_id", "tier": 0},
                {"namespace": "youtube_video_id_candidate", "tier": 1},
            ],
        }

    if host in {"archive.org", "www.archive.org"} and path.startswith("/download/"):
        raw_tail = path[len("/download/") :]
        raw_parts = raw_tail.split("/", 1)
        if len(raw_parts) == 2 and raw_parts[0] and raw_parts[1]:
            item = unicodedata.normalize("NFC", unquote(raw_parts[0]))
            filename = unicodedata.normalize("NFC", unquote(raw_parts[1]))
            return {
                "kind": "internet_archive_item_filename",
                "value": f"{item}/{filename}",
                "lookups": [
                    {"namespace": "internet_archive_item_filename", "tier": 0}
                ],
            }

    if host in {"reddit.com", "www.reddit.com", "old.reddit.com", "new.reddit.com"}:
        match = REDDIT_POST_RE.match(path)
        if match:
            return {
                "kind": "reddit_post_id",
                "value": match.group(1).lower(),
                "lookups": [{"namespace": "reddit_post_id", "tier": 0}],
            }
    if host in {"redd.it", "www.redd.it"}:
        parts = [part for part in path.split("/") if part]
        if len(parts) == 1 and re.fullmatch(r"[A-Za-z0-9]+", parts[0]):
            return {
                "kind": "reddit_post_id",
                "value": parts[0].lower(),
                "lookups": [{"namespace": "reddit_post_id", "tier": 0}],
            }
    if host == "v.redd.it":
        parts = [part for part in path.split("/") if part]
        if parts and re.fullmatch(r"[A-Za-z0-9]+", parts[0]):
            return {
                "kind": "v_reddit_media_id",
                "value": parts[0],
                "lookups": [{"namespace": "v_reddit_media_id", "tier": 0}],
            }

    return {"kind": "unsupported_href", "value": href, "lookups": []}


REQUIRED_TABLES = {
    "schema_migrations",
    "external_ids",
    "sources",
    "recordings",
    "recording_sources",
    "renditions",
    "media_objects",
}


def _open_catalog(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table'"
        ).fetchall()
    }
    missing = sorted(REQUIRED_TABLES - tables)
    if missing:
        connection.close()
        _fail("Catalog lacks required tables: " + ", ".join(missing))
    return connection


def _external_matches(
    connection: sqlite3.Connection, locator: dict[str, object]
) -> tuple[list[dict[str, object]], int, int | None]:
    selected: list[dict[str, object]] = []
    selected_tier: int | None = None
    lower_tier_match_count = 0
    lookups = locator["lookups"]
    assert isinstance(lookups, list)
    for lookup in lookups:
        assert isinstance(lookup, dict)
        rows = connection.execute(
            """
            SELECT external_id_id, object_type, object_id, namespace,
                   external_value, confidence_state, source_id
            FROM external_ids
            WHERE namespace = ? AND external_value = ?
              AND confidence_state <> 'rejected'
            ORDER BY external_id_id
            """,
            (lookup["namespace"], locator["value"]),
        ).fetchall()
        mapped = [dict(row) for row in rows]
        if mapped and selected_tier is None:
            selected = mapped
            selected_tier = int(lookup["tier"])
        elif mapped:
            lower_tier_match_count += len(mapped)
    return selected, lower_tier_match_count, selected_tier


def _source_row(connection: sqlite3.Connection, source_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT source_id, access_state, review_state FROM sources WHERE source_id = ?",
        (source_id,),
    ).fetchone()


def _recording_row(
    connection: sqlite3.Connection, recording_id: str
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT recording_id, review_state FROM recordings WHERE recording_id = ?",
        (recording_id,),
    ).fetchone()


def _mapping_rows(
    connection: sqlite3.Connection,
    *,
    source_id: str | None = None,
    recording_id: str | None = None,
) -> list[sqlite3.Row]:
    clauses = ["confidence_state <> 'rejected'"]
    values: list[str] = []
    if source_id is not None:
        clauses.append("source_id = ?")
        values.append(source_id)
    if recording_id is not None:
        clauses.append("recording_id = ?")
        values.append(recording_id)
    return connection.execute(
        """
        SELECT recording_source_id, source_id, recording_id, mapping_role,
               source_start_ms, source_end_ms, recording_start_ms,
               recording_end_ms, mapping_method, confidence_state
        FROM recording_sources
        WHERE """
        + " AND ".join(clauses)
        + " ORDER BY recording_source_id",
        values,
    ).fetchall()


def _resolve_anchors(
    connection: sqlite3.Connection, matches: list[dict[str, object]]
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    candidates: dict[tuple[str, ...], dict[str, object]] = {}
    unresolved: set[tuple[str, str, str | None, str | None]] = set()

    for match in matches:
        external_id_id = str(match["external_id_id"])
        object_type = str(match["object_type"])
        object_id = str(match["object_id"])
        declared_source_id = match["source_id"]
        mappings: list[sqlite3.Row] = []

        if object_type == "source":
            source_id = object_id
            if _source_row(connection, source_id) is None:
                unresolved.add((external_id_id, "missing_source", source_id, None))
                continue
            mappings = _mapping_rows(connection, source_id=source_id)
            if not mappings:
                unresolved.add(
                    (external_id_id, "source_without_recording", source_id, None)
                )
                continue
        elif object_type == "recording":
            recording_id = object_id
            if _recording_row(connection, recording_id) is None:
                unresolved.add(
                    (external_id_id, "missing_recording", None, recording_id)
                )
                continue
            if declared_source_id is not None:
                source_id = str(declared_source_id)
                if _source_row(connection, source_id) is None:
                    unresolved.add(
                        (external_id_id, "missing_source", source_id, recording_id)
                    )
                    continue
                mappings = _mapping_rows(
                    connection, source_id=source_id, recording_id=recording_id
                )
            else:
                mappings = _mapping_rows(connection, recording_id=recording_id)
            if not mappings:
                unresolved.add(
                    (
                        external_id_id,
                        "recording_without_source_mapping",
                        str(declared_source_id) if declared_source_id else None,
                        recording_id,
                    )
                )
                continue
        else:
            unresolved.add(
                (external_id_id, "unsupported_external_id_object", None, None)
            )
            continue

        for mapping in mappings:
            source_id = str(mapping["source_id"])
            recording_id = str(mapping["recording_id"])
            source = _source_row(connection, source_id)
            recording = _recording_row(connection, recording_id)
            if source is None or recording is None:
                unresolved.add(
                    (
                        external_id_id,
                        "broken_source_recording_mapping",
                        source_id,
                        recording_id,
                    )
                )
                continue
            if source["review_state"] == "rejected":
                unresolved.add(
                    (external_id_id, "rejected_source", source_id, recording_id)
                )
                continue
            if recording["review_state"] in {"rejected", "merged"}:
                unresolved.add(
                    (external_id_id, "inactive_recording", source_id, recording_id)
                )
                continue
            renditions = connection.execute(
                """
                SELECT r.rendition_id, r.review_state AS rendition_review_state,
                       r.rendition_kind, r.media_id, m.media_kind,
                       m.container, m.integrity_state, m.duration_ms
                FROM renditions AS r
                JOIN media_objects AS m ON m.media_id = r.media_id
                WHERE r.recording_id = ? AND r.review_state <> 'rejected'
                ORDER BY r.rendition_id
                """,
                (recording_id,),
            ).fetchall()
            if not renditions:
                unresolved.add(
                    (
                        external_id_id,
                        "recording_without_rendition",
                        source_id,
                        recording_id,
                    )
                )
                continue
            for rendition in renditions:
                key = (
                    source_id,
                    recording_id,
                    str(rendition["rendition_id"]),
                    str(mapping["recording_source_id"]),
                )
                candidate = candidates.setdefault(
                    key,
                    {
                        "source_id": source_id,
                        "source_access_state": source["access_state"],
                        "source_review_state": source["review_state"],
                        "recording_source_id": mapping["recording_source_id"],
                        "mapping_role": mapping["mapping_role"],
                        "source_start_ms": mapping["source_start_ms"],
                        "source_end_ms": mapping["source_end_ms"],
                        "recording_start_ms": mapping["recording_start_ms"],
                        "recording_end_ms": mapping["recording_end_ms"],
                        "mapping_method": mapping["mapping_method"],
                        "mapping_confidence_state": mapping["confidence_state"],
                        "recording_id": recording_id,
                        "recording_review_state": recording["review_state"],
                        "rendition_id": rendition["rendition_id"],
                        "rendition_kind": rendition["rendition_kind"],
                        "rendition_review_state": rendition[
                            "rendition_review_state"
                        ],
                        "media_id": rendition["media_id"],
                        "media_kind": rendition["media_kind"],
                        "container": rendition["container"],
                        "media_integrity_state": rendition["integrity_state"],
                        "duration_ms": rendition["duration_ms"],
                        "external_id_ids": [],
                    },
                )
                ids = candidate["external_id_ids"]
                assert isinstance(ids, list)
                if external_id_id not in ids:
                    ids.append(external_id_id)

    ordered_candidates = sorted(
        candidates.values(),
        key=lambda item: (
            item["source_id"],
            item["recording_id"],
            item["rendition_id"],
            item["recording_source_id"],
        ),
    )
    for candidate in ordered_candidates:
        candidate["external_id_ids"].sort()
    ordered_unresolved = [
        {
            "external_id_id": item[0],
            "reason": item[1],
            "source_id": item[2],
            "recording_id": item[3],
        }
        for item in sorted(unresolved, key=lambda value: tuple(part or "" for part in value))
    ]
    return ordered_candidates, ordered_unresolved


def _validate_wiki_inventory(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        _fail("Wiki seed inventory root must be an object")
    required = {
        "schema_version",
        "kind",
        "source_tree_sha256",
        "inventory_id",
        "pages",
    }
    missing = sorted(required - value.keys())
    if missing:
        _fail("Wiki seed inventory lacks keys: " + ", ".join(missing))
    if value["schema_version"] != 1 or value["kind"] != "wiki_graph_seed_inventory":
        _fail("Unsupported wiki seed inventory contract")
    if not isinstance(value["pages"], list):
        _fail("Wiki seed inventory pages must be an array")
    identity_body = {key: item for key, item in value.items() if key != "inventory_id"}
    expected_inventory_id = wiki_seed_inventory_id(identity_body)
    if value["inventory_id"] != expected_inventory_id:
        _fail("Wiki seed inventory ID does not match its canonical content")
    return value


def build_anchor_inventory(inventory_path: Path, database_path: Path) -> dict[str, object]:
    inventory_bytes = _stable_regular_read(inventory_path)
    inventory = _validate_wiki_inventory(
        _load_json_no_duplicates(inventory_bytes, "wiki seed inventory")
    )
    database_sha256, database_byte_count = _stable_file_sha256(database_path)
    connection = _open_catalog(database_path)
    try:
        schema_version_row = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()
        schema_version = schema_version_row[0] if schema_version_row else None
        if not isinstance(schema_version, int):
            _fail("Catalog has no integer migration version")

        citations: list[dict[str, object]] = []
        for page in inventory["pages"]:
            if not isinstance(page, dict):
                _fail("Wiki seed inventory page must be an object")
            source_citations = page.get("source_citations")
            if not isinstance(source_citations, list):
                _fail("Wiki seed inventory page lacks source_citations array")
            for citation in source_citations:
                if not isinstance(citation, dict):
                    _fail("Wiki seed citation must be an object")
                locator = extract_locator(citation.get("href"))
                matches: list[dict[str, object]] = []
                lower_tier_match_count = 0
                selected_tier: int | None = None
                anchors: list[dict[str, object]] = []
                unresolved: list[dict[str, object]] = []
                if locator is None:
                    resolution_state = "missing_href"
                elif locator["kind"] == "unsupported_href":
                    resolution_state = "unsupported_href"
                else:
                    matches, lower_tier_match_count, selected_tier = _external_matches(
                        connection, locator
                    )
                    if not matches:
                        resolution_state = "catalog_external_id_unmatched"
                    else:
                        anchors, unresolved = _resolve_anchors(connection, matches)
                        if len(anchors) == 1:
                            resolution_state = "single_anchor_candidate"
                        elif len(anchors) > 1:
                            resolution_state = "multiple_anchor_candidates"
                        else:
                            resolution_state = "matched_without_anchor_candidate"
                citations.append(
                    {
                        "page_domain": page.get("domain"),
                        "page_slug": page.get("slug"),
                        "source_path": page.get("source_path"),
                        "citation_ordinal": citation.get("ordinal"),
                        "citation_source_id": citation.get("source_id"),
                        "claim_id": citation.get("claim_id"),
                        "href": citation.get("href"),
                        "review_state": citation.get("review_state"),
                        "checked_attribute_present": citation.get(
                            "checked_attribute_present"
                        ),
                        "locator": locator,
                        "selected_external_id_tier": selected_tier,
                        "external_id_matches": matches,
                        "lower_tier_match_count": lower_tier_match_count,
                        "anchor_candidates": anchors,
                        "unresolved_lineages": unresolved,
                        "resolution_state": resolution_state,
                    }
                )
    finally:
        connection.close()
    final_database_sha256, final_database_byte_count = _stable_file_sha256(database_path)
    if (
        final_database_sha256 != database_sha256
        or final_database_byte_count != database_byte_count
    ):
        _fail("Catalog changed while the crosswalk was being built")

    state_counts: dict[str, int] = {}
    locator_counts: dict[str, int] = {}
    total_anchor_candidates = 0
    citations_with_lineage_warnings = 0
    for citation in citations:
        state = str(citation["resolution_state"])
        state_counts[state] = state_counts.get(state, 0) + 1
        locator = citation["locator"]
        locator_kind = "missing_href" if locator is None else str(locator["kind"])
        locator_counts[locator_kind] = locator_counts.get(locator_kind, 0) + 1
        total_anchor_candidates += len(citation["anchor_candidates"])
        if citation["unresolved_lineages"]:
            citations_with_lineage_warnings += 1

    identity_body: dict[str, object] = {
        "schema_version": 1,
        "kind": "wiki_catalog_anchor_inventory",
        "wiki_seed_inventory_id": inventory["inventory_id"],
        "wiki_seed_inventory_sha256": _sha256_bytes(inventory_bytes),
        "wiki_source_tree_sha256": inventory["source_tree_sha256"],
        "catalog_sha256": database_sha256,
        "catalog_byte_count": database_byte_count,
        "catalog_schema_version": schema_version,
        "coordinate_basis": "rendition_media_ms",
        "authority": {
            "semantic_mapping": False,
            "identity_assertion": False,
            "event_truth_assertion": False,
            "claim_catalog_link": False,
            "catalog_import": False,
            "publication": False,
        },
        "counts": {
            "citations": len(citations),
            "resolution_states": dict(sorted(state_counts.items())),
            "locator_kinds": dict(sorted(locator_counts.items())),
            "anchor_candidates": total_anchor_candidates,
            "citations_with_lineage_warnings": citations_with_lineage_warnings,
        },
        "citations": citations,
    }
    return {
        **identity_body,
        "inventory_id": "wcai_"
        + _sha256_bytes(_canonical_json(identity_body).encode("utf-8"))[:32],
    }


def write_owner_private_exact(
    output_path: Path, output_root: Path, content: bytes
) -> bool:
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(output_root, 0o700)
    resolved_root = output_root.resolve()
    output_path = _under(output_path, resolved_root, "Output")
    output_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(output_path.parent, 0o700)
    resolved_parent = output_path.parent.resolve()
    try:
        resolved_parent.relative_to(resolved_root)
    except ValueError:
        _fail("Resolved output parent escapes the private output root")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(output_path, flags, 0o600)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
        existing = _stable_regular_read(output_path)
        if existing != content:
            _fail("Refusing to overwrite a different existing anchor inventory")
        return True
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(output_path, 0o600)
    except BaseException:
        try:
            output_path.unlink()
        except FileNotFoundError:
            pass
        raise
    return False


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _reject_symlink_components(Path(args.inventory), "Wiki seed inventory")
    _reject_symlink_components(Path(args.db), "Catalog")
    _reject_symlink_components(Path(args.output), "Output")
    inventory_path = _under(
        Path(args.inventory), PRIVATE_INVENTORY_ROOT, "Wiki seed inventory"
    )
    database_path = _under(Path(args.db), PRIVATE_CATALOG_ROOT, "Catalog")
    output_path = _under(Path(args.output), PRIVATE_OUTPUT_ROOT, "Output")
    if output_path.suffix.lower() != ".json":
        _fail("Output must have a .json suffix")
    result = build_anchor_inventory(inventory_path, database_path)
    content = (json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    reused = write_owner_private_exact(output_path, PRIVATE_OUTPUT_ROOT, content)
    print(
        _canonical_json(
            {
                "inventory_id": result["inventory_id"],
                "output_sha256": _sha256_bytes(content),
                "byte_count": len(content),
                "counts": result["counts"],
                "reused": reused,
                "claim_catalog_link_authority": False,
                "publication_authority": False,
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AnchorInventoryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
